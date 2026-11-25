# Compaction

The ring has a fixed size, so a long-running program must regularly tell the
log which records it no longer needs. There are two ways.

## Compact: store a snapshot

```go
// state is your in-memory state after applying every record up to index upTo.
err := w.Compact(state, upTo)
```

`Compact` durably stores `state` as the snapshot for all records up to and
including `upTo`, and frees the ring space of those records. After a restart,
`Snapshot` returns `state` and `upTo`, and `Replay` starts at `upTo+1`.

`state` must really reflect all records up to `upTo`, and none after it: the
log does not check, and replay will apply the records after `upTo` on top of
it. `Compact` copies `state`, so you may reuse the slice once it returns.

A typical use is to compact up to the index you last applied:

```go
state, last := w.Snapshot()
// ... restore, replay, keep `last` = the index of the last record applied ...
if err := w.Compact(encode(myState), last); err != nil {
	return err
}
```

Appends can continue while the snapshot file is being written. `Compact`
returns once the new state is durable, at which point the freed space can be
reused. Calls to `Compact` and `Release` run one at a time.

## Release: free without a snapshot

```go
err := w.Release(upTo)
```

`Release` frees the records up to and including `upTo` and stores nothing. Use
it when your state is kept elsewhere (for example in a database) and is already
durable up to `upTo`; the log then only has to hold the records that state has
not absorbed yet. Releasing records that are already released does nothing.

`Snapshot` is not updated by `Release`. Pick one of the two styles for a log
and stick to it; mixing them leaves `Snapshot` returning an out-of-date state
(see [Replay and Snapshot](replay.md)).

## Valid indexes

`upTo` must refer to a record that is durable, meaning one whose `Append` has
returned. `Compact` accepts any index from the one just before the first live
record (this stores a new snapshot and frees nothing) to the last appended
record. `Release` accepts the same upper bound; anything at or below the last
released record is a no-op. Other values return an error that is not one of
the exported sentinels.

## A full ring

When the ring has no room for a batch, `Append` returns `ErrFull`. `Compact`
and `Release` need no ring space, so a full log can always be emptied. Call
them with the index of the last record to free everything:

```go
first, err := w.Append(batch)
if errors.Is(err, tinywal.ErrFull) {
	// Everything up to last has been applied and is durable elsewhere.
	if err := w.Release(last); err != nil {
		return err
	}
	first, err = w.Append(batch)
}
```

Space is a whole-ring budget: records are packed byte by byte, wrapping around
the end, so nothing is wasted at the wrap point. A batch needs room for all its
records at once.

## When Compact fails

If `Compact` cannot write the snapshot file, it returns the error and the log is
unchanged and still usable. If the failure happens in the write and sync that
makes the new state current, the WAL becomes unusable like after any failed
write; see [Appending](appending.md#after-a-failed-write).

A crash during `Compact` leaves either the old state or the new one, never a
mix: the previous snapshot is kept until the new one is complete and current.
