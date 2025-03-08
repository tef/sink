package xsync

// this package contains a lock-free concurrent map
//
// underneath, there's just a linked-list of (key, value) entries,
// with start and end sentinel values. for example, a list with
// two values looks like this, and we keep it in (hash, key) order:
//
// ```
//     start, a1, b1,, end
// ```
//
// effectively, this is a mvcc list: we insert new entries, before
// removing old entries, and we also insert a tombstone into the list
// before removing it and the previous entry.
//
// this means that when we look for an entry in the list, we must
// not stop at the first match, and only consider the last match
// to be valid, as we may encounter a tombstone, or a new version.
//
// this might seem a little excessive, but we have good reason: keeping
// new versions at the end allows us to do lock-free operations without
// pointer tagging, or indirection nodes.
//
// if we hold the following invariants:
//
// - we always search for the last matching item in the list
//    (and so we always check for tombstones or newer versions)
// - we insert new entries after all other matching entries, never inbetween
//   (and so once an entry has been replaced, its next pointer is fixed)
// - if we need to insert after a tombstone, we remove tombstone first
//   (and so tombstone next pointers are never changed)
//
// then after inserting a new entry or tombstone, it will always
// be safe to remove the old entries from the list.
//
// the usual problem is that another thread updates the next pointer
// of the node you're deleting, after you update the predecessor to
// exclude it. this leaves a new item dangling without anyone knowing.
//
// we prevent this, as the next pointer cannot be updated by other threads
// in these instances. tombstones's next pointers are never changed, and
// old entries next pointers never change once a new version is inserted.
//
// it's worth noting that pointer tagging is another means of freezing
// the next pointers. with tagged pointers, you don't need tombstones,
// and you can always return the first match.
//
// with pointer tagging, you do need the gc to know what you're up to, and
// you do need to use unsafe, unportable code to make it work. the other
// alternative, indirection nodes, involves doubling the linked list
// in size.
//
// the "return the last matching entry" method may not be as clever
// or as simple as the alternatives, but it works without unsafe, and
// works faster than indirection pointers
//
// ... but wait, there's more!
//
// in order to speed up operations on the list, we keep a
// series of dummy entries in the list, and a lookup table
// pointing to them. this allows us to jump deeply into the
// linked list, avoiding substantial amounts of searching
//
// a three bit jump table has eight dummy entries in the list
// for all possible three bit prefixes of a hash, e.g.
//
//     `000 -> 001 -> 010 -> 011 -> 100 -> 101 -> 110 -> 111`
//
// we just keep an array of dummy entries, where the prefix bits are
// the array index, so prefix 001 is entry 1 in the array, etc
//
// when we resize the array, we copy across the old dummy entries,
// then we insert new dummy elemenents into the list and new array.
//
// aside:
//
// there's a fancy way to store this jump table: reverse the hash
// and take the lower bits as the table index. this keeps the items
// ordered by their trailing zeros, so as we grow the table, a prefix
// always has the same location in the table
//
// as we replace the jump table each time it grows, we don't need
// to worry about preserving the offsets into the table during resizes,
// and keeping things in lexicographic order means that we can
// handle missing jump entries with very little fuss.

import (
	"cmp"
	"fmt"
	"hash/maphash"
	"sync/atomic"
)

const hashBits = 56 // width of uint64 - flag nibble
const mask = 0xFFFF_FFFF_FFFF_FFF0
const uint64w = 64

// bit 0 is deleted, bit 1 is real/placeholder, bit 2 is sentinel

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

type cursor struct {
	prev  *entry
	match *entry
	next  *entry
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
	}
	c.prev = prev_match
	c.match = match
	c.next = next
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

	for next != nil {
		// if this set of values is ahead, we're done
		c := next.compare(needle)
		if c > 0 {
			break
		}

		cur := cursor{prev_next, nil, next}
		cur.compact()

		next = cur.match

		if next != nil {
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

func (t *table) delete_hash(key string) uint64 {
	hash := maphash.String(t.seed, key)
	return (hash & mask) | 3
}

func (t *table) grow(w int) *table {
	if t.width >= hashBits {
		return t
	}
	if w <= t.width {
		return t
	}
	nt := t.new.Load()
	if nt == nil {
		new_len := 1 << w
		new_table := make([]atomic.Pointer[entry], new_len)
		gap := w - t.width
		for i := range t.entries {
			j := i << gap
			new_table[j].Store(t.entries[i].Load())
		}
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
	return nt
}

func (t *table) jump(e *entry) *entry {
	index := e.hash >> (64 - t.width)

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

	hash := index << (64 - t.width)

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

func (t *table) store(e *entry) bool {
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

	return true
}

func (t *table) storeSlow(e *entry) bool {
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

		return true

	}
	return false
}

func (t *table) delete(e *entry) bool {
	for true {
		start := t.jump(e)
		c := cursor{prev: start}
		if !c.walk(e) {
			continue
		}

		match := c.found(e)

		if match == nil {
			return false
		}

		if !c.insert_after_match(e) {
			continue
		}

		if !c.replace_after_prev(c.next) {
			c.repair_from(start)
		}

		break
	}

	return true
}

type Map struct {
	t atomic.Pointer[table]
}

func (m *Map) grow(w int) {
	t := m.t.Load()
	if t != nil {
		new := t.grow(w)
		if new != nil && new != t {
			m.t.CompareAndSwap(t, new)
		}
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

	if !t.store(e) {
		panic("bad: failed to insert into map")
	}
}
func (m *Map) Delete(key string) {
	t := m.table()

	hash := t.delete_hash(key)

	e := &entry{
		hash: hash,
		key:  key,
	}

	t.delete(e)
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
