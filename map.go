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
// the list is a bit mvcc: we always insert a new entry to make a change,
// either the newer version or a tombstone. once inserted, we go back
// and clean up the list. for example, we can have multiple versions of an
// entry, but a tombstone is always the last version if present:
//
// ```
//     start, a1, a2, b1, bX, c1, c2, c3, end
//     start, a2, c3, end
// ```
//
// when we search through the list, the first matching entry may
// not be the most recent version, and so we must continue searching
// through until the last matching entry.  this might seem a little
// excessive, but we have good reason.
//
// we can do all of the above in a lock free manner:
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
// the usual problem is that another thread updates the next pointer
// of the node you're deleting, but only after you update the predecessor to
// exclude it. this leaves a new item dangling without anyone knowing.
//
// freezing the next pointer prevents any lost updates. essentially it
// acts as both a lock on the key, and a lock over the gap between it
// and the next key.
//
// ... but wait, there's more!
//
// in order to speed up operations on the list, we keep a
// series of dummy entries in the list, and a lookup table
// pointing to them. this allows us to jump deeply into the
// linked list, avoiding substantial amounts of searching
//
// for example: a three bit jump table has eight dummy entries in the list
// for all possible three bit prefixes of a hash.
//
//     `000 -> 001 -> 010 -> 011 -> 100 -> 101 -> 110 -> 111`
//
// the jump table is all the dummy entries in order, and
// when we resize the array, we copy across the old dummy entries
// into their new positions.
//
//
// nb: this strucure is very similar to, and directly inspired by:
//
// "Split-Ordered Lists: Lock-Free Extensible Hash Tables"
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
// in some ways, this is a simplification (no pointer tagging, no bithacks)
// but in others, it's a complication (read until last match, tombstones)
//
// c'est la vie
//
//
// TODO: growing when inserting at end of long chain
//       or shrinking when buckets touch

import (
	"cmp"
	"fmt"
	"hash/maphash"
	"sync/atomic"
	"time"
)

const hashBits = 56 // width of uint64 - flag nibble
const mask = 0xFFFF_FFFF_FFFF_FFF0
const uint64w = 64

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

func (e *entry) deleted() bool {
	return e.hash&1 == 1
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

// a cursor represents an insertion point in the list
//
// it starts as (node, nil, nil), then
// walks to find a point in the list, returning
// either a triplet, (predecesor, matching node, successor)
// or a doublet (predecessor, nil, successor)
//
// and inserts into the gap, or after the match
// via compare and swap operations

type cursor struct {
	prev  *entry
	match *entry
	next  *entry
	count int
}

func (c *cursor) found(e *entry) *entry {
	if c.match == nil || c.match.deleted() {
		return nil
	}
	return c.match
}

func (c *cursor) insert_after_prev(e *entry) bool {
	return !c.prev.deleted() && c.prev.insert_after(e, c.next)
}

func (c *cursor) insert_after_match(e *entry) bool {
	if c.match.deleted() {
		return false
	}

	return c.match.insert_after(e, c.next)
}

func (c *cursor) replace_after_prev(e *entry) bool {
	return !c.prev.deleted() && c.prev.replace_next(e, c.match)
}

func (c *cursor) walk(needle *entry) bool {
	// always called on a dummy entry
	var prev_match, match, next *entry

	prev_match = c.prev
	if prev_match.compare(needle) > 0 {
		return false
	}

	// potentially first real element or
	// deletion marker for dummy
	next = prev_match.next.Load()

	if next == nil || next.deleted() {
		return false
	}

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
	return true
}

func (c *cursor) walkSlow(needle *entry) bool {
	// always called on a dummy entry
	var prev_match, match, prev_next, next *entry

	prev_next = c.prev
	if prev_next.compare(needle) > 0 {
		return false
	}

	prev_match = prev_next
	// potentially first real element or
	// deletion marker for dummy

	next = prev_next.next.Load()

	if next == nil || next.deleted() {
		return false
	}

	count := 0
	for next != nil {
		// if this set of values is ahead, we're done
		c := next.compare(needle)
		if c > 0 {
			break
		}

		cur := cursor{prev_next, nil, next, 0}
		cur.compact()

		next = cur.match

		if next != nil {
			count += 1
			prev_next = next

			if c < 0 {
				prev_match = next
			} else if c == 0 {
				match = next
			}
		}

		next = cur.next

	}

	c.prev = prev_match
	c.match = match
	c.next = next
	c.count = count
	return true
}

func (c *cursor) compact() {
	prev_next := c.prev

	start := c.next
	end := c.next

	next := end.next.Load()

	// find the end of the chain
	for next != nil && start.compare(next) == 0 {
		end = next
		next = next.next.Load()
	}

	// we know that prev_next ---> (start ---> end) --> next
	// and start and end are all entries for the same value

	// ... but! we cannot delete it all unless end is deleted
	// because end --> next can change under insert
	// but ---> end should be stable, so we can delete that

	if start != end {
		if end.deleted() {
			if prev_next.replace_next(next, start) {
				end = nil
			}
		} else {
			prev_next.replace_next(end, start)
		}
	}

	c.match = end
	c.next = next
}

func (c *cursor) repair_from(start *entry) {
	for {
		slow := cursor{prev: start}
		if slow.walkSlow(c.next) {
			return
		}
	}
}

func (c *cursor) count_empty() int {
	if c.prev.hash&15 != 0 || c.next.hash&15 != 0 {
		return 0
	}

	count := 1
	next := c.next.next.Load()
	for next != nil && next.hash&15 == 0 {
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
}

func (t *table) hash(key string) uint64 {
	hash := maphash.String(t.seed, key)
	return (hash & mask) | 2
}

func (t *table) tombstone_hash(key string) uint64 {
	hash := maphash.String(t.seed, key)
	return (hash & mask) | 3
}

func (t *table) jump(e *entry) *entry {
	index := e.hash >> (uint64w - t.width)

	start := t.entries[index].Load()
	if start == nil {
		return t.jumpSlow(index)
	}

	return start
}

func (t *table) jumpSlow(index uint64) *entry {
	if index == 0 {
		return t.start
	}

	start := t.entries[index].Load()
	if start != nil {
		return start
	}

	start = t.jumpSlow(index - 1)

	hash := index << (uint64w - t.width)

	e := &entry{
		hash: hash,
	}

	for true {
		cursor := cursor{prev: start}
		cursor.walkSlow(e)

		if match := cursor.found(e); match != nil {
			return match
		}
		if cursor.insert_after_prev(e) {
			break
		}
	}

	start = t.entries[index].Load()
	if start != nil {
		return start
	}

	if t.entries[index].CompareAndSwap(nil, e) {
		return e
	}

	return t.entries[index].Load()
}

func (t *table) lookup(e *entry) *entry {
	start := t.jump(e)
	cursor := cursor{prev: start}
	cursor.walk(e)

	if found := cursor.found(e); found != nil {
		return found
	}
	return nil

}

func (t *table) store(e *entry) (bool, int) {
	start := t.jump(e)
	c := cursor{prev: start}
	if !c.walk(e) {
		return t.storeSlow(e)
	}

	match := c.found(e)

	if match == nil {
		if !c.insert_after_prev(e) {
			return t.storeSlow(e)
		}
	} else {
		if !c.insert_after_match(e) {
			return t.storeSlow(e)
		}
		if !c.replace_after_prev(e) {
			c.repair_from(start)
		}
	}

	return true, c.count
}

func (t *table) storeSlow(e *entry) (bool, int) {
	for true {
		start := t.jump(e)
		c := cursor{prev: start}
		if !c.walk(e) {
			continue
		}

		match := c.found(e)

		if match == nil {
			if !c.insert_after_prev(e) {
				continue
			}
		} else {
			if !c.insert_after_match(e) {
				continue
			}
			if !c.replace_after_prev(e) {
				c.repair_from(start)
			}
		}

		return true, c.count

	}
	return false, -1
}

func (t *table) delete(e *entry) (bool, int) {
	count := 0
	for true {
		start := t.jump(e)
		c := cursor{prev: start}
		if !c.walk(e) {
			continue
		}

		match := c.found(e)

		if match == nil {
			return false, 0
		}

		if !c.insert_after_match(e) {
			continue
		}

		if !c.replace_after_prev(c.next) {
			c.repair_from(start)
		}

		count = c.count_empty()

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
		nt = &table{
			start:   t.start,
			end:     t.end,
			seed:    t.seed,
			width:   w,
			entries: new_table,
		}
		if !t.new.CompareAndSwap(nil, nt) {
			nt = t.new.Load()
		}
	}
	gap := nt.width - t.width
	if gap >= 0 {
		for i := range t.entries {
			j := i << gap
			nt.entries[j].CompareAndSwap(nil, t.entries[i].Load())
		}
	} else {
		for i := range nt.entries {
			j := i << (-gap)
			nt.entries[i].CompareAndSwap(nil, t.entries[j].Load())
		}
	}
	return nt
}

type Map struct {
	t atomic.Pointer[table]
}

func (m *Map) grow(w int) {
	t := m.t.Load()
	if t != nil {
		new := t.new.Load()
		if new == nil {
			go m.tryGrow(w)
		}
	}

}

func (m *Map) waitGrow(w int) {
	for {
		t := m.t.Load()
		if t == nil {
			break
		}
		if t.width == w {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (m *Map) tryGrow(w int) {
	t := m.t.Load()
	new := t.grow(w)
	if new != nil && new != t {
		m.t.CompareAndSwap(t, new)
	}
}

func (m *Map) fill() {
	t := m.table()

	for i := range t.entries {
		if t.entries[i].Load() == nil {
			t.jumpSlow(uint64(i))
		}
	}
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
		if match.compare(&e) != 0 || match.deleted() {
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
	if count >= 2 {
		//fmt.Println("empty bucket")
		//m.grow(t.width -1 )
	}
}
func (m *Map) print() {
	t := m.table()

	fmt.Println("table", t.width)

	next := t.start

	for next != nil {
		fmt.Printf("%064b %v:%v\n", next.hash, next.key, next.value)
		next = next.next.Load()
	}
}
