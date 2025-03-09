package xsync

// this package contains a lock-free concurrent map
//
// underneath, there's just a linked-list of (hash, key, value) entries,
// with start and end sentinels. for example, a list with
// two values looks like this, and we keep it in (hash, key) order:
//
// ```
//     start, a1, b1, end
// ```
//
// to insert new versions, we add them to the list, and for deletes
// we insert a tombstone. all new items come after the old ones:
//
// ```
//     start, a1, a2, b1, bX, c1, c2, c3, end
// ```
//
// once we've inserted the new versions, we can trim out the old
// versions from the list:
//
// ```
//     start, a2, c3, end
// ```
//
// this means that lookups must search for the last matching item
// in the list, rather than the first.
//
// it might seem a little weird, but we have good reasons for it.
//
// with a concurrent linked list, it's easy to add new items lock-free
// but deleting items from a list can be much harder:
//
// ```
//     start, a1, aX, end
//     start, a1, aX, b1, end // must not happen
//     start, end // what must happen before inserts
// ```
//
// when we delete items from the list, we copy over aX's next pointer
// into start, and if another thread tries to insert after aX, it
// might happen after aX has been removed from the list.
//
// to prevent this, we do not allow entries to be inserted after tombstones
// and require all new versions to be added after old ones, effectively
// freezing those next pointers from changes.
//
// more formally:
//
// - it's always safe to read an item once it's in the list,
//   as only the next pointer can change
// - inserting a new entry into the list does not require taking a lock,
//   and we always insert new entries after all other matching entries
//   (and so once an entry has been replaced, its next pointer is fixed)
// - if we need to insert after a tombstone, we remove tombstone first
//   (and so tombstone next pointers are never changed)
// - as old versions and tombstones have frozen next pointers, we can
//   patch them out of the list, without worrying about other threads
//
// alongside the linked-list, we keep a lookup table of special entries
// throughout the list:
//
// ```
//     start --> 0 --> 0xxx... ---> 1 -->  1xxx... ---> end
// ```
//
// when we search the list, we take the prefix of the hash we're looking
// for, and use the lookup table to jump further into the list.
//
// when the list gets too big, we create new dummy entries inside the list
// and create a new, larger lookup table, reusing the older entries:
//
// ```
// start -> 00         -> 01         -> 10         -> 11         -> end
// start -> 000 -> 001 -> 010 -> 011 -> 100 -> 101 -> 110 -> 111 -> end
// ```
//
// we grow the table when it takes more than some N entries to find
// an item. when we delete an entry, we check to see if there's any
// items before, or after the now removed element, and if we find
// two dummy items afterwards, we shrink the table in half.
//
// i.e if we delete X from `00 --> X -- > 01 --> 10...`  we
// can infer that the `00-->01` and `01 --> 10` stretches are empty
// and shrink the jump table.
//
// growing and shrinking a jump table can be done concurrently
// as readers can use earlier items in the table if the desired
// entry is missing,
//
// nb: this strucure is very similar to, and directly inspired by:
// "Split-Ordered Lists: Lock-Free Extensible Hash Tables"
//
// the paper also uses a lock-free linked list and a jump table
// to speed up searches
//
// the key differences are:
//
// the paper uses pointer tagging to freeze out next pointers, whereas
// we use a tombstone. they can stop at the first match, but we have to
// continue.
//
// tagging pointers might require a little bit of cooperation from the
// garbage collector, which is why we do not use it here
//
// the other major difference is that the paper uses a bithack to
// avoid reordering the jump table during resizes. instead of lexicographic
// order, they reverse the bits and so effectively sort it by trailing
// zero count, so '1', '10', '100', all map to the same slot.
//
// as we replace the jump table each time it grows, we don't need
// to worry about preserving the offsets into the table during resizes,
// and keeping things in lexicographic order means that we can
// handle missing jump entries with very little fuss.
//
// we also use different logic to manage splitting and growing.
//
// in some ways, this is a simplification (no pointer tagging, no bithacks)
// but in others, it's a complication (read until last match, tombstones)
//
// c'est la vie

import (
	"cmp"
	"fmt"
	"hash/maphash"
	"sync/atomic"
	"time"
)

const uint64w = 64
const hashBits = 56 // width of uint64 - flag nibble

const hash_mask = 0xFFFF_FFFF_FFFF_FFF0
const entry_mask = 2
const tombstone_mask = 3

// bit 0 is deleted, bit 1 is real/placeholder, bit 2 is sentinel
// so 000 = dummy item
//    010, 011, real item, deleted
//    1xx = end sentinel value

type entry struct {
	hash  uint64
	key   string
	value any
	next  atomic.Pointer[entry]
}

func (e *entry) isDeleted() bool {
	return e.hash&1 == 1
}

func (e *entry) isDummy() bool {
	return e.hash&2 == 0
}

func (e *entry) cursor() cursor {
	return cursor{start: e, prev: e}
}

func (e *entry) compare(o *entry) int {
	return cmp.Or(
		// deleted items compare the same
		cmp.Compare(e.hash>>1, o.hash>>1),
		cmp.Compare(e.key, o.key),
	)
}

func (e *entry) insert_after(value *entry, old *entry) bool {
	value.next.Store(old)
	return e.next.CompareAndSwap(old, value)
}

func (e *entry) replace_next(value *entry, old *entry) bool {
	return e.next.CompareAndSwap(old, value)
}

func (e *entry) compact(next *entry) (*entry, *entry) {
	if next == nil {
		next = e.next.Load()
	}

	if next == nil {
		return nil, nil
	}

	start := next
	end := start
	next = end.next.Load()

	// we search for  e ---> (start ---> .... ---> end) --> next
	// where start ... end are all entries for the same value

	for next != nil && start.compare(next) == 0 {
		end = next
		next = next.next.Load()
	}

	// if we've found a chain, then we can compact it.
	// if end is deleted, the whole thing can go
	// if end is fresh, the rest can go

	if start != end {
		if end.isDeleted() {
			if e.replace_next(next, start) {
				end = nil
			}
		} else {
			e.replace_next(end, start)
		}
	}
	return end, next
}

// a cursor represents an insertion point in the list
//
// it starts as (start, nil, nil, nil), then
// walks to find a point in the list for a given entry
//
// ending up as (start, predecessor, matching node, successor)
// or just      (start, predecessor, nil, successor)
//
// the cursor can then go on to insert after the matching node
// or affter the predecessor

type cursor struct {
	start *entry

	prev  *entry
	match *entry
	next  *entry

	count int
}

func (c *cursor) ready() bool {
	if c.prev == nil {
		c.prev = c.start
	}
	if c.prev == nil {
		return false
	}

	if c.next == nil {
		c.next = c.prev.next.Load()
	}

	if c.next == nil || c.next.isDeleted() {
		return false
	}
	return true
}

func (c *cursor) find(needle *entry) bool {
	// always called on a ready entry
	var prev_match, match, next *entry

	prev_match = c.prev
	next = c.next

	count := 0
	for next != nil {
		c := next.compare(needle)

		if c < 0 {
			prev_match = next
		} else if c == 0 {
			match = next
			// don't break
			// may be followed by newer version
		} else if c > 0 {
			break
		}

		next = next.next.Load()
		count += 1
	}
	c.prev = prev_match
	c.match = match
	c.next = next
	c.count = count

	return c.match != nil && !c.match.isDeleted()
}

func (c *cursor) findRepair(needle *entry) bool {
	// always called on a ready entry
	var prev_match, match, prev_next, next, after *entry

	prev_next = c.prev
	prev_match = c.prev
	next = c.next

	count := 0
	for next != nil {
		// if this set of values is ahead, we're done
		c := next.compare(needle)
		if c > 0 {
			break
		}

		next, after = prev_next.compact(next)

		if next != nil {
			count += 1
			prev_next = next

			if c < 0 {
				prev_match = next
			} else if c == 0 {
				match = next
			}
		}

		next = after

	}

	c.prev = prev_match
	c.match = match
	c.next = next
	c.count = count

	return c.match != nil && !c.match.isDeleted()
}

func (c *cursor) repair_from_start() bool {
	slow := c.start.cursor()

	if !slow.ready() {
		return false
	}

	slow.findRepair(c.next)
	return true
}

func (c *cursor) insert_after_prev(e *entry) bool {
	return !c.prev.isDeleted() && c.prev.insert_after(e, c.next)
}

func (c *cursor) insert_after_match(e *entry) bool {
	if c.match.isDeleted() {
		return false
	}

	return c.match.insert_after(e, c.next)
}

func (c *cursor) replace_after_prev(e *entry) bool {
	return !c.prev.isDeleted() && c.prev.replace_next(e, c.match)
}

func (c *cursor) count_empty() int {
	// check the next for dummy entries
	if !c.next.isDummy() {
		return 0
	}

	count := 1
	next := c.next.next.Load()
	for next != nil && next.isDummy() {
		next = next.next.Load()
		count += 1

	}
	return count
}

type table struct {
	seed  maphash.Seed
	start *entry
	end   *entry

	width   int
	entries []atomic.Pointer[entry]

	new atomic.Pointer[table]
	old atomic.Pointer[table]
}

func (t *table) hash(key string) uint64 {
	hash := maphash.String(t.seed, key)
	return (hash & hash_mask) | entry_mask
}

func (t *table) tombstone_hash(key string) uint64 {
	hash := maphash.String(t.seed, key)
	return (hash & hash_mask) | tombstone_mask
}

func (t *table) cursorFor(e *entry) cursor {
	index := e.hash >> (uint64w - t.width)

	start := t.entries[index].Load()
	if start == nil {
		start = t.insertDummy(index)
	}

	cursor := start.cursor()

	if !cursor.ready() {
		// if the dummy is marked for deletion
		// a shrink must have occurred
		new := t.new.Load()
		if new == nil {
			panic("bad: dummy marked for deletion but no new table")
		}
		return new.cursorFor(e)
	}

	if start.compare(e) > 0 {
		panic("bad")
	}

	return cursor
}

func (t *table) insertDummy(index uint64) *entry {
	if index == 0 {
		return t.start
	}

	start := t.entries[index].Load()
	if start != nil {
		return start
	}

	start = t.insertDummy(index - 1)

	if start.isDeleted() {
		return start
	}

	hash := index << (uint64w - t.width)

	e := &entry{
		hash: hash,
	}

	old := t.entries[index].Load()
	for old == nil {
		c := start.cursor()
		if c.ready() {
			if c.findRepair(e) {
				e = c.match
				break
			}

			if c.insert_after_prev(e) {
				break
			}
		}
		old = t.entries[index].Load()
	}

	for true {
		if old != nil && old.isDeleted() {
			// just in case

			t := &entry{hash: e.hash | 1}
			for true {
				c := e.cursor()
				if !c.ready() {
					return old
				}
				if c.insert_after_prev(t) {
					return old
				}
			}
		}
		if old != nil {
			return old
		}

		if t.entries[index].CompareAndSwap(nil, e) {
			break
		}
		old = t.entries[index].Load()
	}

	return e
}

func (t *table) lookup(e *entry) *entry {
	c := t.cursorFor(e)

	if c.find(e) {
		return c.match
	}

	return nil
}

func (t *table) store(e *entry) (bool, int) {
	c := t.cursorFor(e)

	if !c.find(e) {
		if !c.insert_after_prev(e) {
			return t.storeSlow(e)
		}
	} else {
		if !c.insert_after_match(e) {
			return t.storeSlow(e)
		}
		if !c.replace_after_prev(e) {
			c.repair_from_start()
		}
	}

	return true, c.count
}

func (t *table) storeSlow(e *entry) (bool, int) {
	for true {
		c := t.cursorFor(e)

		if !c.find(e) {
			if !c.insert_after_prev(e) {
				continue
			}
		} else {
			if !c.insert_after_match(e) {
				continue
			}
			if !c.replace_after_prev(e) {
				c.repair_from_start()
			}
		}

		return true, c.count

	}
	return false, -1
}

func (t *table) delete(e *entry) (bool, int) {
	count := 0
	for true {
		c := t.cursorFor(e)

		if !c.find(e) {
			return false, 0
		}

		if !c.insert_after_match(e) {
			continue
		}

		if !c.replace_after_prev(c.next) {
			c.repair_from_start()
		}

		if c.prev.isDummy() {
			// check to see if we're the last item
			count = c.count_empty()
		}

		break
	}

	return true, count
}

func (t *table) grow(w int) *table {
	if w < 0 || t.width >= hashBits {
		return t
	}
	if w == t.width {
		return t
	}
	nt := t.new.Load()
	if nt == nil {
		new_len := 1 << w
		new_table := make([]atomic.Pointer[entry], new_len)
		new_table[0].Store(t.start)

		nt = &table{
			start:   t.start,
			end:     t.end,
			seed:    t.seed,
			width:   w,
			entries: new_table,
		}

		nt.old.Store(t)

		gap := nt.width - t.width

		if gap > 0 {
			for i := range t.entries {
				j := i << gap
				old := t.entries[i].Load()
				if old != nil {
					nt.entries[j].Store(old)
				}
			}
		} else if gap < 0 {
			for i := range nt.entries {
				j := i << -gap
				old := t.entries[j].Load()
				if old != nil {
					nt.entries[i].Store(old)
				}
			}
		}

		if !t.new.CompareAndSwap(nil, nt) {
			nt = t.new.Load()
		}
	}
	return nt
}

func (t *table) sweepOld() {
	old := t.old.Load()
	if old == nil {
		return
	}

	sentinel := &entry{
		hash: 1,
	}

	gap := old.width - t.width
	if gap <= 0 {
		t.old.CompareAndSwap(old, nil)
		return
	}

	for i := range old.entries {
		// we mark out every old entry
		// not just the ones we left behind
		// as new entries could get added to the old table
		// and not the new table, and be missed on subsequent sweeps

		o := old.entries[i].Swap(sentinel)

		if o == nil || o.isDeleted() {
			continue
		}

		// if it's an entry we copied over
		// copy it over again, in case it's new

		j := (i >> gap)
		if i == (j << gap) {
			t.entries[j].Store(o)
			continue
		}

		// otherwise, time to delete it

		e := &entry{
			hash: o.hash | 1,
		}
		for true {
			c := o.cursor()
			if c.ready() {
				if !c.insert_after_prev(e) {
					continue
				}
			}
			break
		}
	}
	c := t.start.cursor()
	if c.ready() {
		c.findRepair(t.end)
	}

	t.old.CompareAndSwap(old, nil)
}

type Map struct {
	t atomic.Pointer[table]
}

func (m *Map) grow(w int) {
	t := m.t.Load()
	if t == nil {
		return
	}

	new := t.new.Load()
	if new != nil {
		return // already growing
	}

	old := t.old.Load()
	if old != nil {
		return // already growing
	}

	go m.tryGrow(w)
}

func (m *Map) tryGrow(w int) {
	t := m.t.Load()
	new := t.grow(w)
	if new != nil && new != t {
		m.t.CompareAndSwap(t, new)
	}
	new.sweepOld()
}

func (m *Map) waitGrow(w int) {
	for {
		t := m.t.Load()
		if t == nil {
			break
		}
		if t.old.Load() != nil {
			continue
		}
		if t.width == w {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (m *Map) fill() {
	t := m.table()

	for i := range t.entries {
		if t.entries[i].Load() == nil {
			t.insertDummy(uint64(i))
		}
	}
}

func (m *Map) Clear() {
	m.t.Store(nil)
}

func (m *Map) table() *table {
	t := m.t.Load()
	if t != nil {
		return t
	}

	start := &entry{
		hash: 0,
	}
	end := &entry{
		hash: ^uint64(0) - 1,
	}

	start.next.Store(end)

	entries := make([]atomic.Pointer[entry], 1)

	entries[0].Store(start)

	t = &table{
		seed:    maphash.MakeSeed(),
		start:   start,
		end:     end,
		width:   0,
		entries: entries,
	}

	if m.t.CompareAndSwap(nil, t) {
		return t
	} else {
		return m.t.Load()
	}
}

func (m *Map) Load(key string) (value any, ok bool) {
	t := m.table()
	e := entry{
		hash: t.hash(key),
		key:  key,
	}

	if match := t.lookup(&e); match != nil {
		if match.compare(&e) != 0 || match.isDeleted() {
			panic("no")
		}
		return match.value, true
	}
	return nil, false
}

func (m *Map) Store(key string, value any) {
	t := m.table()

	e := &entry{
		hash:  t.hash(key),
		key:   key,
		value: value,
	}

	ok, count := t.store(e)

	if !ok {
		panic("bad: failed to insert into map")
	}
	if count > 20 {
		m.grow(t.width + 1)
	}
}
func (m *Map) Delete(key string) {
	t := m.table()

	hash := t.tombstone_hash(key)

	e := &entry{
		hash: hash,
		key:  key,
	}

	_, count := t.delete(e)
	if count >= 4 {
		m.grow(t.width - 1)
	}
}
func (m *Map) print() {
	t := m.table()

	fmt.Println("table", t.width)

	next := t.start

	for next != nil {
		var v string
		if next.isDummy() {
			v = ""
		} else if next.isDeleted() {
			v = fmt.Sprintf("-%v\n", next.key)
		} else {
			v = fmt.Sprintf("+%v:%v", next.key, next.value)
		}
		fmt.Printf("%064b %v\n", next.hash, v)
		next = next.next.Load()
	}
}
