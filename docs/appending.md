# Appending and group commit

```go
first, err := w.Append([][]byte{[]byte("a"), []byte("b"), []byte("c")})
// first == 1; the records have indexes 1, 2 and 3
next, err := w.Append([][]byte{[]byte("d")})
// next == 4
```

`Append` takes a batch of records and returns the index of the first one; the
others follow consecutively. An empty batch does nothing and returns
`0, nil`.

## Durability

When `Append` returns `nil`, all of its records have been written and synced
to stable storage. There is no separate flush call and nothing buffered in the
WAL that `Close` would have to write.

Appended records are visible to `Replay` right away.

## Group commit

`WAL` is safe for concurrent use. Appends from several goroutines are
collected into one batch that is written and synced once; the records of a
single `Append` call share that sync too. One of the waiting callers performs
the flush while the others wait, and appends that arrive during a flush form
the next batch. So with many concurrent writers each call is still durable
when it returns, but the sync cost is shared. For throughput, either call
`Append` from many goroutines or pass several records per call; see
[Performance](performance.md).

Records of concurrent calls are numbered in the order the calls got into the
log. A call's own records are always consecutive.

A batch being collected holds at most 1024 records. An `Append` that would go
over that waits until the current batch has been taken by a flush. A single
call with more than 1024 records is accepted: it waits for an empty batch and
then goes through on its own.

## Errors

`Append` can refuse a batch in these ways, and the batch is accepted or
refused as a whole: if it is refused, none of its records are written.

- `ErrTooLarge`: a record is over 64 MiB, or the whole batch is larger than
  the ring. It can never fit, whatever you release.
- `ErrFull`: the ring has no room for the whole batch right now. `Compact` or
  `Release` frees space.
- `ErrClosed`: the WAL was closed. Nothing was written.
- Any other error: the write or sync failed, see
  [After a failed write](#after-a-failed-write). Whether the records reached
  the disk is unknown.

```go
first, err := w.Append(batch)

switch {
case errors.Is(err, tinywal.ErrFull):
	// free space with Compact or Release, then retry
case errors.Is(err, tinywal.ErrTooLarge):
	// retrying cannot help: split the batch or the record
case err != nil:
	// closed, or the log failed: stop using this WAL
}
```

These checks are not made in one fixed order for every state of the WAL. An
empty batch returns `0, nil` and an oversized record or batch returns
`ErrTooLarge` right away, even on a closed WAL or one that failed earlier.
Otherwise a closed or failed WAL returns its error, before `ErrFull` is
considered.

Each record takes its data plus a small fixed header (currently 28 bytes) of
ring space.

## After a failed write

If writing or syncing the log fails, the WAL becomes unusable: the callers
whose records were in that flush get the underlying error, and later calls to
`Append` (with a non-empty batch that is not oversized), `Replay`, `Compact`
and `Release` return an error wrapping `tinywal: log unusable after failed
write`. It stays that way until you `Close` it and `Open` the directory again.
`Snapshot` has no error result and keeps returning the last snapshot it had.
After a `Close`, calls return `ErrClosed` instead, and a repeated `Close`
returns `nil`.

This applies to a failed write or sync of the log itself, which includes the
flush that makes a `Compact` or `Release` current. A failure to write the
snapshot file inside `Compact` is different: `Compact` returns that error, the
log is unchanged and stays usable. See [Compaction](compaction.md#when-compact-fails).

The error does not tell you whether the records of the failed flush reached
the disk. They may or may not be there after you reopen. Records whose
`Append` returned `nil` earlier are unaffected.

The same goes for a crash: records of an `Append` that did not return may or
may not be in the log after a restart. See [Recovery](recovery.md).
