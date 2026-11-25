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
- **Failed writes are sticky.** After a failed write or sync every call fails
  until you reopen the WAL. See [Appending](appending.md#after-a-failed-write).
- **Replay payloads are borrowed**, and the callback must not call back into
  the WAL, or it deadlocks. See [Replay and Snapshot](replay.md).
- **A corrupt current snapshot makes `Open` fail**, with no fallback to the
  older snapshot file.
- **Do not use `Release` and `Compact` interchangeably.** `Release` leaves the
  last compacted snapshot in place, which then no longer matches the records.
- **Every `Open` costs a sync** and records a new epoch, even if you only
  read.
- **Close.** `Close` waits for a running flush or replay, and later or pending
  calls fail with `ErrClosed`. A second `Close` returns `nil`. Do not use
  replayed data after `Close`.

## Errors

| Error | Returned by | When |
|---|---|---|
| `ErrClosed` | any call | The WAL is closed. |
| `ErrTooLarge` | `Append` | A record is over 64 MiB, or the batch is larger than the ring. |
| `ErrFull` | `Append` | No room for the whole batch. |
| `ErrCorruptHeader` | `Open` | The file is too short or neither header slot is valid. |
| `ErrCorruptSnapshot` | `Open` | The current snapshot is too short or fails its checksum. |

Everything else (capacity below the minimum, an `upTo` out of range, file
system errors, a failed write) is an ordinary error. Use `errors.Is` to test
for the ones above.
