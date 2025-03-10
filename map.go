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
// this does mean handling some weird cases
//
// - a dummy entry is inserted, but must be deleted as the table is being evicted
// - the table has been shrunk then grown, and so another thread sees the dummy
//   and inserts it into the table
// - the older thread gasps and goes "uh oh" and drops the item
// - the newer thread now has a tombstone'd waypoint in the array
//
// if we used epochs, and waited for old readers before shrinking/growing
// we'd avoid this problem altogether, but it's not really that annoying
// to clear out and reinsert items upon errors
//
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
	"strings"
	"sync/atomic"
	"time"
)

const maxInsertCount = 12 // length of search before suggesting grow
const maxEmptyDummy = 6   // number of empty dummy sections found after delete before suggesting shrink

const uint64w = 64
const hashBits = 56 // width of uint64 - flag nibble

const hash_mask = 0xFFFF_FFFF_FFFF_FFF0
const entry_mask = 2
const tombstone_mask = 3

// we store some flags inside of the `hash` field of `entry`:
//
// bit 0 is deleted, bit 1 is real/placeholder, bit 2 is sentinel
// so 0000, dummy item
//    0001, dummy tombstone
//    0010, real item,
//    0011, real tombstone
//    1110, end sentinel value

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

func (e *entry) insert_after(old *entry, value *entry) bool {
	value.next.Store(old)
	return e.next.CompareAndSwap(old, value)
}

func (e *entry) replace_next(old *entry, value *entry) bool {
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
			if e.replace_next(start, next) {
				end = nil
			}
		} else {
			e.replace_next(start, end)
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
	// is there no tombstone following this dummy entry?

	if c.start == nil || c.start.isDeleted() {
		return false
	}
	if c.prev == nil {
		c.prev = c.start
	}
	if c.prev == nil || c.prev.isDeleted() {
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

func (c *cursor) findSlow(needle *entry) bool {
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

func (c *cursor) insert_after_prev(e *entry) bool {
	// called after ready()
	if !c.prev.isDeleted() && c.prev.insert_after(c.next, e) {
		return true
	}
	return false
}

func (c *cursor) insert_after_match(e *entry) bool {
	if !c.match.isDeleted() && c.match.insert_after(c.next, e) {
		// we do not update c.match as we use it
		// later
		return true
	}
	return false
}

func (c *cursor) replace_after_prev(old *entry, e *entry) bool {
	if !c.prev.isDeleted() && c.prev.replace_next(old, e) {
		return true
	}
	return false
}

func (c *cursor) repair_from_start() bool {
	// compact every entry from start to next

	slow := c.start.cursor()

	if !slow.ready() {
		return false
	}

	slow.findSlow(c.next)
	return true
}

func (c *cursor) count_empty_successors(n int) int {
	// check the next entries for dummy entries
	// which is used by delete to know when to shrink

	count := 0
	if !c.next.isDummy() {
		return 0
	}
	next := c.next.next.Load()
	for next != nil && next.isDummy() && count < n {
		next = next.next.Load()
		count += 1

	}
	return count
}

type table struct {
	version uint

	seed  maphash.Seed
	start *entry
	end   *entry

	width   int
	entries []atomic.Pointer[entry]

	new atomic.Pointer[table]
	old atomic.Pointer[table]

	expunged *entry
}

func (t *table) pause() {
	d := 128 - t.width - (t.width >> 1)
	time.Sleep(time.Duration(d) * time.Millisecond)
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
	var c cursor

	for true {
		start := t.entries[index].Load()
		if start == nil {
			start = t.insertDummy(index)
		}

		if start != nil && start != t.expunged {
			if start.compare(e) > 0 {
				panic("bad")
			}

			c = start.cursor()

			if c.ready() {
				break
			}
		}

		// we found a dummy, but there's a tombstone next to it(?!)
		// or when we tried to insert, a previous dummy had a tombstone

		// we could be being expunged, so we forward the lookup:

		nt := t.new.Load()
		if nt != nil {
			return nt.cursorFor(e)
		}

		// alas, if we're the most recent table, someone has decided to
		// evict the dummy entry, so we must clear the entry

		if start != nil && start != t.expunged {
			t.entries[index].CompareAndSwap(start, nil)
		}

		// and insertDummy will compact it and try again
		t.pause()
	}

	return c
}

func (t *table) insertDummy(index uint64) *entry {
	if index == 0 {
		return t.start
	}

	hash := index << (uint64w - t.width)

	e := &entry{
		hash: hash,
	}

	var c cursor
	var old *entry
	var start *entry
	var inserted *entry

	for true {
		// check before inserting that it's still empty
		old = t.entries[index].Load()

		if old != nil { // or expunged
			return old
		}

		start = t.insertDummy(index - 1)
		if start == t.expunged || start == nil {
			return start
		}

		c = start.cursor()

		if c.ready() {
			// someone else already put it in the list, so we use it
			// and try to insert it into the table (we're helping!)

			if c.find(e) {
				e = c.match
				break
			}

			if c.match != nil && c.match.isDeleted() {
				// we found a dummy tombstone, so compact
				// and retry
				c.repair_from_start()
			} else if c.insert_after_prev(e) {
				inserted = e
				break
			} else {
				c.repair_from_start()
			}
		} else {
			// we found a tombstone, so we clear it out
			// and try again

			t.entries[index].CompareAndSwap(start, nil)
		}
	}

	// insert/lookup succeded, table still empty

	if t.entries[index].CompareAndSwap(nil, e) {
		return e
	}

	old = t.entries[index].Load()

	if old == e {
		return e
	}

	if old != t.expunged {
		panic("what??")
	}

	// our table is being expunged

	if inserted == nil {
		return t.expunged
	}

	// we inserted, but our table is being evicted
	// so we must delete and evict our insert
	// or find a home for it in the new table

	nt := t.new.Load()
	if nt.repairDummyInsert(inserted) {
		// it's valid, so we just make progress
		return e
	}

	tombstone := &entry{hash: inserted.hash | 1}
	for true {
		c2 := inserted.cursor()
		if !c2.ready() {
			break
		}

		if !c2.insert_after_prev(tombstone) {
			t.pause()
			continue
		}

		// best effort eviction
		c.replace_after_prev(inserted, tombstone.next.Load())
		break
	}

	return t.expunged
}

func (t *table) repairDummyInsert(e *entry) bool {
	nt := t.new.Load()
	if nt != nil {
		return nt.repairDummyInsert(e)
	}

	index := e.hash >> (uint64w - t.width)
	hash := index << (uint64w - t.width)

	if e.hash != hash {
		return false
	}

	start := t.entries[index].Load()
	if start == e {
		return true
	} else if start == nil && t.entries[index].CompareAndSwap(nil, e) {
		return true
	}

	start = t.entries[index].Load()
	return start == e
}

func (t *table) lookup(e *entry) *entry {
	c := t.cursorFor(e)

	if c.find(e) {
		return c.match
	}

	return nil
}

func (t *table) store(e *entry) (*entry, bool) {
	c := t.cursorFor(e)

	if !c.find(e) {
		if !c.insert_after_prev(e) {
			return t.storeSlow(e)
		}
	} else {
		if !c.insert_after_match(e) {
			return t.storeSlow(e)
		}
		if !c.replace_after_prev(c.match, e) {
			c.repair_from_start()
		}
	}

	return e, c.count > maxInsertCount
}

func (t *table) storeSlow(e *entry) (*entry, bool) {
	var c cursor
	for true {
		c = t.cursorFor(e)

		if !c.findSlow(e) {
			if !c.insert_after_prev(e) {
				t.pause()
				continue
			}
		} else {
			if !c.insert_after_match(e) {
				t.pause()
				continue
			}
			if !c.replace_after_prev(c.match, e) {
				c.repair_from_start()
			}
		}
		break
	}

	return e, c.count > maxInsertCount
}

func (t *table) delete(e *entry) (*entry, bool) {
	count := 0
	var deleted *entry
	for true {
		c := t.cursorFor(e)

		if !c.find(e) {
			return nil, false
		}

		deleted = c.match

		if !c.insert_after_match(e) {
			t.pause()
			continue
		}

		if !c.replace_after_prev(c.match, c.next) {
			c.repair_from_start()
		}

		if c.prev.isDummy() {
			// check to see if we're the last item
			count = c.count_empty_successors(maxEmptyDummy-1) + 1
		}

		break
	}

	return deleted, count >= maxEmptyDummy
}

func (t *table) resize(from int, to int) *table {
	if to < 0 || to > hashBits {
		return nil
	}
	if to == t.width || from != t.width {
		return nil
	}

	old := t.old.Load()

	if old != nil {
		return nil
	}

	nt := t.new.Load()
	if nt == nil {
		new_len := 1 << to
		new_table := make([]atomic.Pointer[entry], new_len)
		new_table[0].Store(t.start)

		nt = &table{
			version:  t.version + 1,
			start:    t.start,
			end:      t.end,
			seed:     t.seed,
			width:    to,
			entries:  new_table,
			expunged: t.expunged,
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

	sentinel := t.expunged

	gap := old.width - t.width

	// fmt.Printf("sweeping v%d's old (v%d), from %d to %d\n", t.version, old.version, old.width, t.width)

	var compact *entry

	for i := len(old.entries) - 1; i >= 0; i-- {
		// we mark out every old entry, even ones we copied over
		// to stop new entries being added to the old table

		o := old.entries[i].Swap(sentinel)

		if o == nil || o.isDeleted() {
			continue
		}

		// if we're growing, we copy over
		// the old entry again, just in case

		if gap <= 0 {
			j := i << -gap
			t.entries[j].CompareAndSwap(nil, o)
			continue

		}

		// we're shrinking, and so we try and compact the tail

		if compact != nil {
			c2 := o.cursor()
			if c2.ready() {
				c2.findSlow(compact) // compact up to this target
				// if we found it, then it wasn't deleted
				if c2.match == nil {
					compact = nil
				}
			}
		}

		// if we're shrinking, then we copy
		// over the matching items

		j := (i >> gap)
		if i == (j << gap) {
			t.entries[j].CompareAndSwap(nil, o)
			continue
		}

		// ... and clear out old dummy entries

		e := &entry{
			hash: o.hash | 1,
		}
		for true {
			c := o.cursor()
			if c.ready() {
				if !c.insert_after_prev(e) {
					t.pause()
					continue
				}
				if compact == nil {
					compact = e
				}
			}
			break
		}

	}

	if compact != nil {
		c := t.start.cursor()
		if c.ready() {
			c.findSlow(compact)
		}
	}

	for !t.old.CompareAndSwap(old, nil) {
		if t.old.Load() == nil {
			break
		}
		t.pause()
	}

	// fmt.Printf("done sweeping v%d's old (v%d), clearing old\n", t.version, old.version)

}
func (t *table) print() string {

	var b strings.Builder

	s := fmt.Sprintln("table", t.width, "version", t.version)
	b.WriteString(s)

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
		s := fmt.Sprintf("%064b %v\n", next.hash, v)
		b.WriteString(s)
		next = next.next.Load()
	}
	return b.String()
}

type Map struct {
	t atomic.Pointer[table]
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

	expunged := &entry{
		hash: ^uint64(0),
	}

	t = &table{
		seed:     maphash.MakeSeed(),
		start:    start,
		end:      end,
		width:    0,
		entries:  entries,
		expunged: expunged,
	}

	if m.t.CompareAndSwap(nil, t) {
		return t
	} else {
		return m.t.Load()
	}
}

func (m *Map) print() string {
	t := m.table()
	return t.print()
}

func (m *Map) resize(from int, to int) {
	t := m.t.Load()
	if t == nil {
		return
	}

	old := t.old.Load()
	if old != nil {
		// we could run t.sweepOld()
		return // already shrinking
	}

	new := t.new.Load()
	if new != nil {
		// we don't sweep old until it has been replaced
		return // already growing
	}

	go m.tryResize(from, to)
}

func (m *Map) tryResize(from int, to int) {
	t := m.t.Load()

	old := t.old.Load()
	if old != nil {
		// could run t.sweepddOld()
		return
	}

	new := t.new.Load()
	if new != nil {
		return // already growing
	}

	nt := t.resize(from, to)

	if nt == nil || nt == t {
		return
	}
	if nt.version != t.version+1 {
		panic("what???")
	}
	if m.t.CompareAndSwap(t, nt) {
		// fmt.Printf("new table is v%d\n", nt.version)
		nt.sweepOld()
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

func (m *Map) Load(key string) (value any, ok bool) {
	t := m.table()
	e := entry{
		hash: t.hash(key),
		key:  key,
	}

	match := t.lookup(&e)

	if match != nil {
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

	inserted, shouldGrow := t.store(e)

	if inserted == nil {
		panic("bad: failed to insert into map")
	}
	if shouldGrow {
		m.resize(t.width, t.width+1)
	}
}
func (m *Map) Delete(key string) {
	t := m.table()
	hash := t.tombstone_hash(key)

	e := &entry{
		hash: hash,
		key:  key,
	}

	_, shouldShrink := t.delete(e)
	if shouldShrink {
		m.resize(t.width, t.width-1)
	}
}

func (m *Map) waitResize() {
	for true {
		t := m.t.Load()
		t.pause()
		if t == nil {
			break
		}
		if t.old.Load() != nil {
			continue
		}
		if t.new.Load() != nil {
			continue
		}
		break
	}
}

func (m *Map) waitGrow(w int) {
	for true {
		t := m.t.Load()
		t.pause()
		if t == nil {
			break
		}
		if t.new.Load() != nil {
			continue
		}
		if t.old.Load() != nil {
			continue
		}
		if t.width >= w {
			return
		}
	}
}

func (m *Map) waitShrink(w int) {
	for true {
		t := m.t.Load()
		t.pause()
		if t == nil {
			break
		}
		if t.new.Load() != nil {
			continue
		}
		if t.old.Load() != nil {
			continue
		}
		if t.width <= w {
			return
		}
	}
}
