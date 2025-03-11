package sink

// this package contains a lock-free, generic, concurrent map
// which provides the same methods as the built in sync.Map
//
// yep, it's all lock free: lookups, inserts, updates, deletes, as
// well as scanning all entries, shrinking and growing the map
//
// neat, huh?
//
// it's basically a lookup table and a linked list inside a trench coat.
//
//
//
// first, the linked list:
//
// we start with a singly linked-list of (hash, key, value) entries,
// with start and end sentinels. for example, a list with
// two values looks like this, and we keep it in (hash, key) order:
//
// ```
//     start, a1, b1, end
// ```
//
// to insert new entry, we insert it after any old versions, and
// similarly for deletes, we also insert a tombstone after any previous
// versions. for example, if we insert a new a2, we delete b1, and insert
// a new key c1, and two new versions, the list could look like this:
//
// ```
//     start, a1, a2, b1, bX, c1, c2, c3, end
// ```
//
// after we insert the new entries, we trim out the old versions
// from the list. for example, we would always end up with this
// list, after compaction.
//
// ```
//     start, a2, c3, end
// ```
//
// yes, this means that lookups must search for the last matching item
// in the list, rather than the first, which does involve a little
// more work, but there is a good reason for it.
//
// by adding new entries after the old, we can safely delete the earlier
// entries without any danger.
//
// danger? let me explain the problem:
//
// with a concurrent linked list it's easy to add new items lock-free
// but deleting items from a list can be much harder. another thread
// can append to the parts you're trying to delete
//
// ```
//     start, a1, aX, end        // we have a value and a tombstone
//     start, a1, aX, b1, end    // and if we insert a value after it
//     start, end 		 // another thread can accidentally delete it
//				 // as it never saw insert
// ```
//
// to stop this happening, we do not allow inserting entries after a tombstone
// and force threads to compact the list, if they want to insert there
//
// this is also why we add new versions after the old version, as that's
// where a tombstone must go to prevent changes
//
// more formally, the argument for lock-freedom works something like this:
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
// that's pretty much it for the linked list
//
// that said: if the go stdlib had tagged pointers, we could have lock-free
// deletes without tombstone values, but it doesn't, so we don't!
//
//
//
// next up: the lookup table
//
// in order to speed up searching through the list, we store a bunch of waypoint
// entries in amongst the regular key:value entries, and keep the waypoints
// inside a lookup table.
//
// for example, we can have a lookup table with two waypoints, 0, and 1. if a
// hash has a leading bit of 1, we use the 1 waypoint for searches, etc:
//
// ```
//     start (waypoint 0) --> 0xxx... ---> waypoint 1 -->  1xxx... ---> end
// ```
//
// when the list gets too big, we create new waypoint entries inside the list
// and create a new, larger waypoint table, reusing the older entries as we go:
//
// ```
// start (00)         -> 01         -> 10         -> 11         -> end
// start (000) -> 001 -> 010 -> 011 -> 100 -> 101 -> 110 -> 111 -> end
// ```
//
// growing and shrinking are done in the background, which means that waypoints
// can be inserted and deleted by other threads, and so the code has to handle
// compacting the list to remove old waypoints, or inserting a waypoint only to
// find that the table has been resized.
//
// it's a little gnarly, but it's manageable. we could avoid this by waiting
// on old threads to exit before clearing out old waypoints, but this would
// mean resizing can be blocked by other threads.
//
//
//
// finally, resizes:
//
// as for triggering resizes, instead of keeping accurate list sizes, we
// use two simple heruistics to gently nudge the map in the right direction.
//
// - if we search more than N entries to insert a new item into the list
//   we tell the map to double the waypoint table
//
// - if we delete an item, and it's the last item between waypoints, we
//   check to see how many empty sections come after the item, and if
//   it is above some threshold, we tell the map to halve the table
//

import (
	"cmp"
	"fmt"
	"hash/maphash"
	"math/bits" // don't get excited, we only use it for UintSize
	"strings"
	"sync/atomic" // and we only use atomic.Pointer
	"time"
)

// type aliases

type Key cmp.Ordered     // because we can't cmp.Compare comparable objects
type Value comparable    // ... and you know, in python, there's a total sort order

type conditionFunc[K Key, V Value] func(old *entry[K, V], new *entry[K, V]) bool

// tuning params:

const defaultInsertCount = 7 // length of search before suggesting grow, on insert new
const defaultEmptyCount = 3  // empty waypoints before suggesting shrink, on delete

// as mentioned above, we use simple heuristics for growing/shrinking
// the following defaults can be overriden per-map

// on inserting a new entry (rather than replacing an old version)
// we count how many steps from the waypoint entry it took
// and if it's above this threshold, we suggest doubling the table

// on deleting an item, we can see if there's no other item left in the section
// between waypoints, and then we can see how many sections after it are empty too
// and suggest a shrink

// in theory, as hashes are uniform, two empty buckets means there's a lot of empty
// space

// the hash:

type uintH uintptr

const uintHbits = bits.UintSize
const maxHashBits = uintHbits - 4

const hash_mask = ^uintH(15)
const entry_mask = 2
const tombstone_mask = 3

// we use a uintptr sized hash, and we use the lowest four bits for flags
//
//    0000, waypoint entry (and start value)
//    0001, waypoint tombstone
//    0010, key/value entry,
//    0011, key/value tombstone
//    1110, end sentinel value

type entry[K Key, V Value] struct {
	hash  uintH
	key   K
	value V
	next  atomic.Pointer[entry[K, V]]
}

func (e *entry[K, V]) cursor() cursor[K, V] {
	return cursor[K, V]{start: e, prev: e}
}

func (e *entry[K, V]) isDeleted() bool {
	return e.hash&1 == 1
}

func (e *entry[K, V]) isWaypoint() bool {
	return e.hash&2 == 0
}

func (e *entry[K, V]) compare(o *entry[K, V]) int {
	return cmp.Or(
		// deleted items compare the same
		cmp.Compare(e.hash>>1, o.hash>>1),
		cmp.Compare(e.key, o.key),
	)
}

func (e *entry[K, V]) insert_after(old *entry[K, V], value *entry[K, V]) bool {
	value.next.Store(old)
	return e.next.CompareAndSwap(old, value)
}

func (e *entry[K, V]) replace_next(old *entry[K, V], value *entry[K, V]) bool {
	return e.next.CompareAndSwap(old, value)
}

func (e *entry[K, V]) compact(next *entry[K, V]) (*entry[K, V], *entry[K, V]) {
	// we search for  e ---> (start ---> .... ---> end) --> next
	// where start ... end are all entries for the same value

	if next == nil {
		next = e.next.Load()
	}

	if next == nil {
		return nil, nil
	}

	start := next
	end := start
	next = end.next.Load()

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

// a cursor handles searching through the list for values
//
// we create one from a waypoint entry, with e.cursor()
//
// we then call cursor.ready(), to check that we have
// a valid waypoint, and then call .walk or walkSlow
// to walk to a position in the list
//
// when a match is found, we set c.match, and we store
// the predecessor node of the earliest matching entry
//
// start --> entries* ---> prev ---> old versions* --> last match --> next
//
// and if there's no match found, we stop at the point where
// that entry could be inserted, between prev and next:
//
// start --> entries* ---> prev --> next
//

type cursor[K Key, V Value] struct {
	start *entry[K, V]

	prev  *entry[K, V]
	match *entry[K, V]
	next  *entry[K, V]

	count int
}

func (c *cursor[K, V]) ready() bool {
	// is there no tombstone following this waypoint entry?

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

func (c *cursor[K, V]) walk(needle *entry[K, V]) *entry[K, V] {
	// always called on a ready entry
	var prev_match, match, next *entry[K, V]

	prev_match = c.prev
	next = c.next

	count := 0
	last := next

	for next != nil {
		if last.compare(next) != 0 && !last.isDeleted() && !last.isWaypoint() {
			count += 1
		}

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

		last = next
		next = next.next.Load()
	}
	c.prev = prev_match
	c.match = match
	c.next = next
	c.count = count

	if c.match != nil && !c.match.isDeleted() {
		return c.match
	} else {
		return nil
	}
}

func (c *cursor[K, V]) walkSlow(needle *entry[K, V]) *entry[K, V] {
	// always called on a ready entry
	var prev_match, match, prev_next, next, after *entry[K, V]

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

	if c.match != nil && !c.match.isDeleted() {
		return c.match
	} else {
		return nil
	}
}

func (c *cursor[K, V]) insert_after_prev(e *entry[K, V]) bool {
	// called after ready()
	if !c.prev.isDeleted() && c.prev.insert_after(c.next, e) {
		return true
	}
	return false
}

func (c *cursor[K, V]) insert_after_match(e *entry[K, V]) bool {
	if !c.match.isDeleted() && c.match.insert_after(c.next, e) {
		// we do not update c.match as we use it
		// later
		return true
	}
	return false
}

func (c *cursor[K, V]) replace_after_prev(old *entry[K, V], e *entry[K, V]) bool {
	if !c.prev.isDeleted() && c.prev.replace_next(old, e) {
		return true
	}
	return false
}

func (c *cursor[K, V]) repair_from_start() bool {
	// compact every entry from start to next

	slow := c.start.cursor()

	if !slow.ready() {
		return false
	}

	slow.walkSlow(c.next)
	return true
}

func (c *cursor[K, V]) count_adjacent_waypoints(n int) int {
	// check the next entries for adjacent waypoint entries
	// which is used by delete to know when to shrink

	count := 0
	if !c.next.isWaypoint() {
		return 0
	}
	next := c.next.next.Load()
	for next != nil && next.isWaypoint() && count < n {
		next = next.next.Load()
		count += 1

	}
	return count
}

// a table contains the waypoint lookup table
// and the start and end of the list, along
// with other shared values, like the maphash Seed
//
// when we grow, we copy over these values from
// one table to the next
//
// when we grow a table, we create a new table
// and new_table.old points to the old table
//
// we install it by setting old_table.new to point to it
// then updating the map
//
// new_table.old is cleared once the old table
// has been expunged, but old_table.new is never
// cleared, so that old readers can sneak ahead
// to the latest lookup table

type table[K Key, V Value] struct {
	version uint

	seed  maphash.Seed
	start *entry[K, V]
	end   *entry[K, V]

	width   int
	entries []atomic.Pointer[entry[K, V]]

	new atomic.Pointer[table[K, V]]
	old atomic.Pointer[table[K, V]]

	expunged *entry[K, V]

	growInsertCount  int
	shrinkEmptyCount int
}

func (t *table[K, V]) pause() {
	d := 128 - t.width - (t.width >> 1)
	time.Sleep(time.Duration(d) * time.Millisecond)
}

func (t *table[K, V]) hash(key K) uintH {
	hash := uintH(maphash.Comparable(t.seed, key))
	return (hash & hash_mask) | entry_mask
}

func (t *table[K, V]) tombstone_hash(key K) uintH {
	hash := uintH(maphash.Comparable(t.seed, key))
	return (hash & hash_mask) | tombstone_mask
}

func (t *table[K, V]) waypointFor(e *entry[K, V]) cursor[K, V] {
	index := e.hash >> (uintHbits - t.width)
	var c cursor[K, V]

	for true {
		start := t.entries[index].Load()

		if start == nil {
			start = t.createWaypoint(index)
		}

		if start != nil && start != t.expunged {
			c = start.cursor()

			if c.ready() {
				break
			}
		}

		// we found a waypoint, but there's a tombstone next to it(?!)
		// or maybe we bumped into a tombstone on a previous waypoint

		// we could be being expunged, so we forward the lookup:

		nt := t.new.Load()
		if nt != nil {
			return nt.waypointFor(e)
		}

		// alas, if we're the most recent table, someone has decided to
		// evict the waypoint entry, so we must clear the entry

		if start != nil && start != t.expunged {
			t.entries[index].CompareAndSwap(start, nil)
		}

		// and createWaypoint will compact it and try again
		t.pause()
	}

	return c
}

func (t *table[K, V]) createWaypoint(index uintH) *entry[K, V] {
	if index == 0 {
		return t.start
	}

	hash := index << (uintHbits - t.width)

	e := &entry[K, V]{
		hash: hash,
	}

	var c cursor[K, V]
	var old *entry[K, V]
	var start *entry[K, V]
	var inserted *entry[K, V]

	for true {
		// check before inserting that it's still empty
		old = t.entries[index].Load()

		if old != nil { // or expunged
			return old
		}

		start = t.createWaypoint(index - 1)
		if start == t.expunged || start == nil {
			return start
		}

		c = start.cursor()

		if c.ready() {
			// someone else already put it in the list, so we use it
			// and try to insert it into the table (we're helping!)

			if c.walk(e) != nil {
				e = c.match
				break
			}

			if c.match != nil && c.match.isDeleted() {
				// we found a waypoint tombstone, so compact
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
	if nt != nil && nt.insertWaypoint(inserted) {
		// it's valid, so we just make progress
		return e
	}

	tombstone := &entry[K, V]{hash: inserted.hash | 1}
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

func (t *table[K, V]) insertWaypoint(e *entry[K, V]) bool {
	// used when we created a waypoint
	// but our lookup table was expunged

	// and we forward the call if we've
	// been replaced, too
	nt := t.new.Load()
	if nt != nil {
		return nt.insertWaypoint(e)
	}

	index := e.hash >> (uintHbits - t.width)
	hash := index << (uintHbits - t.width)

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

func (t *table[K, V]) lookup(e *entry[K, V]) *entry[K, V] {
	c := t.waypointFor(e)

	if c.walk(e) != nil {
		return c.match
	}

	return nil
}

func (t *table[K, V]) store(e *entry[K, V], shouldGrow *bool, cond conditionFunc[K, V]) (old *entry[K, V], inserted bool) {
	c := t.waypointFor(e)

	found := c.walk(e)

	if cond != nil && !cond(found, e) {
		return found, false
	}

	if found == nil {
		if !c.insert_after_prev(e) {
			return t.storeSlow(e, shouldGrow, cond)
		}
		*shouldGrow = c.count > t.growInsertCount
		return nil, true
	} else {
		if !c.insert_after_match(e) {
			return t.storeSlow(e, shouldGrow, cond)
		}
		if !c.replace_after_prev(c.match, e) {
			c.repair_from_start()
		}
		return c.match, true
	}

}

func (t *table[K, V]) storeSlow(e *entry[K, V], shouldGrow *bool, cond conditionFunc[K, V]) (old *entry[K, V], inserted bool) {

	var c cursor[K, V]

	for true {
		c = t.waypointFor(e)

		found := c.walkSlow(e)

		if cond != nil && !cond(found, e) {
			return found, false
		}

		if found == nil {
			if !c.insert_after_prev(e) {
				t.pause()
				continue
			}
			*shouldGrow = c.count > t.growInsertCount
			return nil, true
		}

		// found an old version

		if !c.insert_after_match(e) {
			t.pause()
			continue
		}

		if !c.replace_after_prev(c.match, e) {
			c.repair_from_start()
		}
		break
	}
	return c.match, true

}

func (t *table[K, V]) delete(e *entry[K, V], shouldShrink *bool, cond conditionFunc[K, V]) (*entry[K, V], bool) {
	var deleted *entry[K, V]
	for true {
		c := t.waypointFor(e)

		deleted = c.walk(e)

		if deleted == nil {
			return nil, false
		}

		if cond != nil && !cond(deleted, e) {
			return deleted, false
		}

		if !c.insert_after_match(e) {
			t.pause()
			continue
		}

		if !c.replace_after_prev(c.match, c.next) {
			c.repair_from_start()
		}

		if c.prev.isWaypoint() {
			// check to see if we're the last item
			count := c.count_adjacent_waypoints(t.shrinkEmptyCount-1) + 1
			*shouldShrink = count >= t.shrinkEmptyCount
		}

		break
	}
	return deleted, true
}

func (t *table[K, V]) resize(from int, to int) *table[K, V] {
	if to < 0 || to > maxHashBits {
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
		new_table := make([]atomic.Pointer[entry[K, V]], new_len)
		new_table[0].Store(t.start)

		nt = &table[K, V]{
			version:          t.version + 1,
			start:            t.start,
			end:              t.end,
			seed:             t.seed,
			width:            to,
			entries:          new_table,
			expunged:         t.expunged,
			growInsertCount:  t.growInsertCount,
			shrinkEmptyCount: t.shrinkEmptyCount,
		}

		nt.old.Store(t)

		gap := nt.width - t.width

		// we tank performance if we drop in a mostly empty table
		// as everything rushes to fill out the entries

		if gap > 0 {
			for i := range t.entries {
				j := i << gap
				old := t.entries[i].Load()
				if old != nil {
					nt.entries[j].CompareAndSwap(nil, old)
				}
			}
		} else if gap < 0 {
			for i := range nt.entries {
				j := i << -gap
				old := t.entries[j].Load()
				if old != nil {
					nt.entries[i].CompareAndSwap(nil, old)
				}
			}
		}

		if !t.new.CompareAndSwap(nil, nt) {
			nt = t.new.Load()
		}

	}

	return nt
}

func (t *table[K, V]) deleteOldWaypoints() {
	old := t.old.Load()
	if old == nil {
		return
	}

	sentinel := t.expunged

	gap := old.width - t.width

	// fmt.Printf("sweeping v%d's old (v%d), from %d to %d\n", t.version, old.version, old.width, t.width)

	var compact *entry[K, V]

	for i := len(old.entries) - 1; i >= 0; i-- {
		// we mark out every old entry, even ones we copied over
		// to stop new entries being added to the old table

		o := old.entries[i].Load()
		if o == t.expunged {
			continue
		}

		o = old.entries[i].Swap(sentinel)

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

		// otherwise, we're shrinking, and so we try and compact the tail

		if compact != nil {
			c2 := o.cursor()
			if c2.ready() {
				c2.walkSlow(compact) // compact up to this target
				// if we found it, then it wasn't deleted
				if c2.match == nil {
					compact = nil
				}
			}
		}

		// some table items are copied over

		j := (i >> gap)
		if i == (j << gap) {
			t.entries[j].CompareAndSwap(nil, o)
			continue
		}

		// and the rest get cleared out

		e := &entry[K, V]{
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
			c.walkSlow(compact)
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

func (t *table[K, V]) print() string {

	var b strings.Builder

	s := fmt.Sprintln("table", t.width, "version", t.version)
	b.WriteString(s)

	next := t.start

	for next != nil {
		var v string
		if next.isWaypoint() {
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

// A Map is nothing more than a pointer to a table
// and when we grow and shrink the table, we replace
// the pointer inside the map

type Map[K Key, V Value] struct {
	t atomic.Pointer[table[K, V]]

	GrowInsertCount  int
	ShrinkEmptyCount int
}

func (m *Map[K, V]) table() *table[K, V] {
	t := m.t.Load()
	if t != nil {
		return t
	}

	expunged := &entry[K, V]{
		hash: ^uintH(0),
	}
	end := &entry[K, V]{
		hash: ^uintH(0) - 3,
	}
	start := &entry[K, V]{
		hash: uintH(0),
	}

	start.next.Store(end)

	grow := cmp.Or(m.GrowInsertCount, defaultInsertCount)
	shrink := cmp.Or(m.ShrinkEmptyCount, defaultEmptyCount)

	entries := make([]atomic.Pointer[entry[K, V]], 1)
	entries[0].Store(start)

	t = &table[K, V]{
		seed:             maphash.MakeSeed(),
		start:            start,
		end:              end,
		width:            0,
		entries:          entries,
		expunged:         expunged,
		growInsertCount:  grow,
		shrinkEmptyCount: shrink,
	}

	if m.t.CompareAndSwap(nil, t) {
		return t
	} else {
		return m.t.Load()
	}
}

func (m *Map[K, V]) print() string {
	t := m.table()
	return t.print()
}

func (m *Map[K, V]) resize(from int, to int) {
	t := m.t.Load()
	if t == nil {
		return
	}

	old := t.old.Load()
	if old != nil {
		// already grown, so help expunge
		go t.deleteOldWaypoints()
		return
	}

	go m.tryResize(from, to)
}

func (m *Map[K, V]) tryResize(from int, to int) {
	t := m.t.Load()

	old := t.old.Load()
	if old != nil {
		// clear out old before resizing
		go t.deleteOldWaypoints()
		return
	}

	nt := t.new.Load()

	if nt == nil {
		nt = t.resize(from, to)
	}

	if nt == nil || nt == t {
		return
	}

	if nt.version != t.version+1 {
		panic("bad: tried to resize with a wrong version")
	}

	for true {
		if m.t.CompareAndSwap(t, nt) {
			//fmt.Printf("new table is v%d\n", nt.version)
			break
		}
		if m.t.Load() == nt {
			break
		}
	}
	nt.deleteOldWaypoints()
}

func (m *Map[K, V]) Clear() {
	m.t.Store(nil)
}

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	t := m.table()
	e := entry[K, V]{
		hash: t.hash(key),
		key:  key,
	}

	match := t.lookup(&e)

	if match != nil {
		return match.value, true
	}
	var empty V
	return empty, false
}

func (m *Map[K, V]) Store(key K, value V) {
	t := m.table()

	e := &entry[K, V]{
		hash:  t.hash(key),
		key:   key,
		value: value,
	}

	var shouldGrow bool
	t.store(e, &shouldGrow, nil)

	if shouldGrow {
		m.resize(t.width, t.width+1)
	}
}

func (m *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	// aka Store and return old value, if any
	t := m.table()

	e := &entry[K, V]{
		hash:  t.hash(key),
		key:   key,
		value: value,
	}

	var shouldGrow bool

	old, _ := t.store(e, &shouldGrow, nil)

	if shouldGrow {
		m.resize(t.width, t.width+1)
	}

	if old == nil {
		var empty V
		return empty, false
	} else {
		return old.value, true
	}
}

func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	t := m.table()

	e := &entry[K, V]{
		hash:  t.hash(key),
		key:   key,
		value: value,
	}

	var shouldGrow bool

	loadOrStore := func(old *entry[K, V], new *entry[K, V]) bool {
		return old == nil
	}

	old, inserted := t.store(e, &shouldGrow, loadOrStore)

	if shouldGrow {
		m.resize(t.width, t.width+1)
	}

	if inserted {
		return value, false
	} else {
		return old.value, true
	}
}

func (m *Map[K, V]) CompareAndSwap(key K, oldValue V, value V) (swapped bool) {
	t := m.table()

	e := &entry[K, V]{
		hash:  t.hash(key),
		key:   key,
		value: value,
	}

	var shouldGrow bool

	compareAndSwap := func(old *entry[K, V], new *entry[K, V]) bool {
		return old != nil && old.value == oldValue
	}

	_, inserted := t.store(e, &shouldGrow, compareAndSwap)

	if shouldGrow {
		m.resize(t.width, t.width+1)
	}
	return inserted
}

func (m *Map[K, V]) CompareAndDelete(key K, oldValue V) (deleted bool) {
	t := m.table()

	e := &entry[K, V]{
		hash:  t.hash(key),
		key:   key,
		value: oldValue,
	}

	var shouldShrink bool

	compareAndDelete := func(old *entry[K, V], new *entry[K, V]) bool {
		return old != nil && old.value == oldValue
	}

	_, deleted = t.delete(e, &shouldShrink, compareAndDelete)

	if shouldShrink {
		m.resize(t.width, t.width-1)
	}

	return deleted
}

func (m *Map[K, V]) Delete(key K) {
	t := m.table()
	hash := t.tombstone_hash(key)

	e := &entry[K, V]{
		hash: hash,
		key:  key,
	}

	var shouldShrink bool

	t.delete(e, &shouldShrink, nil)
	if shouldShrink {
		m.resize(t.width, t.width-1)
	}
}

func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	t := m.table()
	hash := t.tombstone_hash(key)

	e := &entry[K, V]{
		hash: hash,
		key:  key,
	}

	var shouldShrink bool

	old, _ := t.delete(e, &shouldShrink, nil)

	if shouldShrink {
		m.resize(t.width, t.width-1)
	}
	if old != nil {
		return old.value, true
	}
	var empty V
	return empty, false
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	t := m.table()

	start := t.start
	next := start.next.Load()

	last := next

	for next != nil {
		if last.compare(next) != 0 {
			if !last.isDeleted() && !last.isWaypoint() {
				if !f(last.key, last.value) {
					break
				}

			}

		}
		last = next
		next = next.next.Load()
	}

	if last != t.end {
		panic("what")
	}
}

// --- test helpers

func (m *Map[K, V]) fill() {
	t := m.table()

	for i := range t.entries {
		if t.entries[i].Load() == nil {
			t.createWaypoint(uintH(i))
		}
	}
}
func (m *Map[K, V]) waitStable() uint {
	for true {
		t := m.t.Load()
		t.pause()
		if t == nil {
			break
		}
		if t.new.Load() != nil {
			t.pause()
			continue
		}
		if t.old.Load() != nil {
			t.pause()
			continue
		}
		return t.version
	}
	return 0
}

func (m *Map[K, V]) waitVersion(v uint) uint {
	for true {
		t := m.t.Load()
		if t == nil {
			if v == 0 {
				return 0
			}
			t.pause()
			continue
		}
		if t.new.Load() != nil {
			t.pause()
			continue
		}
		if t.old.Load() != nil {
			t.pause()
			continue
		}
		if t.version >= v {
			return t.version
		}
	}
	return 0
}
