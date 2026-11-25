# Replay and Snapshot

## Rebuilding state

On start, rebuild your state from the snapshot and the records after it:

```go
state, last := w.Snapshot() // nil, 0 when there is no snapshot
restore(state)

err := w.Replay(func(index uint64, data []byte) error {
	return apply(index, data)
})
```

`Snapshot` returns the state stored by the latest `Compact` and the index of
the last record it covers. Treat the returned slice as read-only.

`Replay` calls your function for every live record, oldest first, with the
record's index. The live records are those after the last `Compact` or
`Release` (all of them if you never called either), including records you
appended since `Open`. After `Compact(state, upTo)` the first record replayed
is `upTo+1`.

If your function returns an error, `Replay` stops and returns that error
unchanged. The WAL stays usable.

## The data is borrowed

The `data` slice passed to your function points straight into the memory-mapped
log file (or, for a record that wraps around the end of the ring, into a
buffer that is reused for the next record). So:

- Do not modify it. For a record inside the mapping, which is read-only,
  writing crashes the process.
- Do not keep it after your function returns. Its contents are overwritten once
  the space is reused, and the mapping goes away on `Close`. Copy what you need
  (`string(data)` and `append([]byte(nil), data...)` both copy).

## Do not call back into the WAL from the callback

While `Replay` runs, it holds the WAL exclusively. Calling `Append`,
`Replay`, `Compact`, `Release` or `Close` from inside the callback, or waiting
in the callback for another goroutine that does, **deadlocks**. Only
`Snapshot` is safe there. Collect what you need and act after `Replay`
returns.

Other goroutines that call `Append`, `Compact`, `Release` or `Close` while a
`Replay` runs are not harmed, but they wait until it is done. A long replay
therefore stalls appenders. Two replays run one after the other.

## Snapshot after Release

`Release` does not store a snapshot, so `Snapshot` keeps returning the last
state given to `Compact`, with its old index. Records released since then are
in neither the snapshot nor `Replay`. If you use `Release`, your state lives
elsewhere and you should not restore it from `Snapshot`; see
[Compaction](compaction.md).
