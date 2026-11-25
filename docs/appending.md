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

| Error | Meaning | Records written? |
|---|---|---|
| `ErrTooLarge` | One record is over 64 MiB, or the whole batch is larger than the ring. It can never fit, whatever you release. | No |
| `ErrFull` | The ring has no room for the whole batch right now. `Compact` or `Release` frees space. | No |
| `ErrClosed` | The WAL was closed. | No |
| other | The write or sync failed. | Unknown |

The batch is accepted or refused as a whole: if it does not fit, none of its
records are written.

Each record takes its data plus a small fixed header (currently 28 bytes) of
ring space.

## After a failed write

If writing or syncing fails, the WAL becomes unusable: the callers whose
records were in that flush get the underlying error, and every later call
(`Append`, `Replay`, `Compact`, `Release`) returns an error wrapping
`tinywal: log unusable after failed write`. It stays that way until you
`Close` it and `Open` the directory again. `Snapshot` keeps returning the last
snapshot it had.

The error does not tell you whether the records of the failed flush reached
the disk. They may or may not be there after you reopen. Records whose
`Append` returned `nil` earlier are unaffected.

The same goes for a crash: records of an `Append` that did not return may or
may not be in the log after a restart. See [Recovery](recovery.md).
