# sink.Map, a generic, lock-free map for go 1.24

This package contains a lock-free, generic, concurrent map
which provides the same methods as the built in sync.Map.

Yes, it's all lock free: lookups, inserts, updates, deletes, as
well as scanning all entries, shrinking and growing the map.

Neat, huh?

It's basically a lookup table and a linked list inside a trench coat.

## A lock free linked list

We start with a singly linked-list of (hash, key, value) entries,
with start and end sentinels. for example, a list with
two values looks like this, and we keep it in (hash, key) order:

```
start, a1, b1, end
```

To insert new entry, we insert it after any old versions, and
similarly for deletes, we also insert a tombstone after any previous
versions. for example, if we insert a new a2, we delete b1, and insert
a new key c1, and two new versions, the list could look like this:

```
start, a1, a2, b1, bX, c1, c2, c3, end
```

After we insert the new entries, we trim out the old versions
from the list. for example, we would always end up with this
list, after compaction.

```
start, a2, c3, end
```

Yes, this means that lookups must search for the last matching item
in the list, rather than the first, which does involve a little
more work, but there is a good reason for it.

By adding new entries after the old, we can safely delete the earlier
entries without any danger.

## Danger? let me explain the problem with concurrency

With a concurrent linked list it's easy to add new items lock-free
but deleting items from a list can be much harder. another thread
can append to the parts you're trying to delete.

```
start, a1, aX, end       #  We have a value and a tombstone,
start, a1, aX, b1, end   #  and if we insert a value after it,
start, end 		         #  another thread can accidentally delete it
                         #  as it never saw insert.
```

To stop this happening, we do not allow inserting entries after a tombstone
and force threads to compact the list before inserting new items.

This is also why we add new versions after the old version, as that's
where a tombstone must go to prevent changes.

## lock-freedom

More formally, the argument for lock-freedom works something like this:

- It's always safe to read an item once it's in the list,
  As only the next pointer can change
- Inserting a new entry into the list does not require taking a lock,
  and we always insert new entries after all other matching entries
  (and so once an entry has been replaced, its next pointer is fixed).
- If we need to insert after a tombstone, we remove tombstone first
  (and so tombstone next pointers are never changed).
- As old versions and tombstones have frozen next pointers, we can
  patch them out of the list, without worrying about other threads.

That's pretty much it for the linked list.

That said: if the go stdlib had tagged pointers, we could have lock-free
deletes without tombstone values, but it doesn't, so we don't!


## The lookup table

In order to speed up searching through the list, we store a bunch of waypoint
entries in amongst the regular key:value entries, and keep the waypoints
inside a lookup table.

For example, we can have a lookup table with two waypoints, 0, and 1. if a
hash has a leading bit of 1, we use the 1 waypoint for searches, etc:

```
start (waypoint 0) --> 0xxx... ---> waypoint 1 -->  1xxx... ---> end
```

When the list gets too big, we create new waypoint entries inside the list
and create a new, larger waypoint table, reusing the older entries as we go:

```
start (00)         -> 01         -> 10         -> 11         -> end
start (000) -> 001 -> 010 -> 011 -> 100 -> 101 -> 110 -> 111 -> end
```

Growing and shrinking are done in the background, which means that waypoints
can be inserted and deleted by other threads, and so the code has to handle
compacting the list to remove old waypoints, or inserting a waypoint only to
find that the table has been resized.

It's a little gnarly, but it's manageable. we could avoid this by waiting
on old threads to exit before clearing out old waypoints, but this would
mean resizing can be blocked by other threads.


## Resizing the jump table

As for triggering resizes, instead of keeping accurate list sizes, we
use two simple heruistics to gently nudge the map in the right direction.

- If we search more than N entries to insert a new item into the list
  we tell the map to double the waypoint table.

- If we delete an item, and it's the last item between waypoints, we
  check to see how many empty sections come after the item, and if
  it is above some threshold, we tell the map to halve the table.

... and that's pretty much everything, except for one final detail.


## Related work

This structure is very similar to, and directly inspired by the
split-ordered list, another concurrent map described in:

> "Split-Ordered Lists: Lock-Free Extensible Hash Tables"

The paper also uses a lock-free linked list and a jump table
to speed up searches. the key differences are:

The paper uses pointer tagging to freeze out next pointers, whereas
we use a tombstone. they can stop at the first match, but we have to
continue.

Tagging pointers might require a little bit of cooperation from the
garbage collector, which is why we do not use it here.

The other major difference is that the paper uses a bithack to
avoid reordering the jump table during resizes. instead of lexicographic
order, they reverse the bits and so effectively sort it by trailing
zero count, so '1', '10', '100', all map to the same slot.

This means that resizing a table only involves appending to the jump table
which is kinda nice, but not nice enough to do the same here.

As we replace the jump table each time it grows, we don't need
to worry about preserving the offsets into the table during resizes,

Keeping things in lexicographic order means that we can
handle missing waypoint entries in the jump table with very little fuss.

We also use different logic to manage splitting and growing.
but that's a given, frankly.

In some ways, this is a simplification (no pointer tagging, no bithacks)
but in others, it's a complication (read until last match, tombstones).

C'est la vie.


