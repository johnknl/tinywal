# Limits and caveats

!!! warning "On-disk format and upgrades"
    The on-disk format will gain a version marker in an upcoming release, and
    there is no automatic upgrade of existing logs yet (a known gap). Do not
    assume that a log written by this version can be opened by a later one.

## Limits

- **Fixed capacity.** The ring size is chosen at creation and cannot be
  changed. When it is full, `Append` returns `ErrFull` until you `Compact` or
  `Release`.
- **Record size.** At most 64 MiB per record, and a whole batch must fit in
  the ring. Both return `ErrTooLarge`.
- **Batch collection.** A batch being collected for group commit holds at most
  1024 records; a larger `Append` waits for an empty batch and then runs on its
  own. This is not a limit on the size of an `Append`.
- **Snapshots are held in memory.** `Open` reads the whole current snapshot
  into memory, and `Compact` makes a copy of the state it is given.
- **Unix only.** The ring is memory-mapped through `golang.org/x/sys/unix`.
  See [Platform notes](platforms.md).

## Caveats

- **One WAL per directory.** Opening the same directory twice is not prevented
  and not supported. See [Recovery](recovery.md#one-wal-per-directory).
- **Corruption is not reported.** Recovery stops at the first invalid record
  and cannot tell a torn tail from damage; records behind it are dropped
  without an error. See [Recovery](recovery.md).
- **Batches are not atomic across a crash.** A multi-record `Append` can come
  back as a prefix. Use a single record for all-or-nothing changes.
- **Failed log writes are sticky.** After a failed write or sync of the log,
  `Append`, `Replay`, `Compact` and `Release` fail until you reopen the WAL
  (empty and oversized `Append` calls are still answered as usual, and
  `Snapshot` still returns data). A failed snapshot-file write in `Compact` is
  not sticky. See [Appending](appending.md#after-a-failed-write).
- **Replay payloads are borrowed**, and the callback must not call back into
  the WAL, or it deadlocks. See [Replay and Snapshot](replay.md).
- **A corrupt current snapshot makes `Open` fail**, with no fallback to the
  older snapshot file. See [Recovery](recovery.md).
- **Do not use `Release` and `Compact` interchangeably.** `Release` leaves the
  last compacted snapshot in place, which then no longer matches the records.
- **Every `Open` costs a sync** and records a new epoch, even if you only
  read.
- **Close.** `Close` waits for a running flush or replay. After it, `Append`
  (non-empty, not oversized), `Replay`, `Compact` and `Release` return
  `ErrClosed`, `Snapshot` still returns its last data, and a repeated `Close`
  returns `nil`. Do not use replayed data after `Close`.
