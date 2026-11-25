# tinywal

tinywal is a minimal write-ahead log (WAL) for Go. You append records, and
each `Append` returns only once its records are on stable storage. After a
restart you read the records back, in order, and carry on.

It is built for a program that keeps its state in memory and wants to survive
restarts: log every change, rebuild the state from the log on start, and
every now and then fold the old records into a snapshot so the log does not
fill up.

```text
go get github.com/johnknl/tinywal
```

tinywal needs Go 1.27 and a Unix-like operating system. See
[Platform notes](platforms.md).

## Concepts

**Records and indexes.** A record is an opaque byte slice. Every record gets
an *index*: the first record ever appended is 1, and each next record is one
higher. Indexes never repeat and have no gaps while the log is healthy. Index
0 means "none": it is what `Snapshot` reports when there is no snapshot.

**The ring.** The log lives in one file, `wal.log`, that is created once at a
fixed size and never grows. Records are written into it as a circular buffer
(a *ring*). Writing wraps around to the start when it reaches the end, and
reuses the space of records you have released. When there is no free space
left, `Append` returns `ErrFull`. The capacity is chosen when the log is
created; see [Configuration](configuration.md).

**Compaction and snapshots.** To free ring space you tell the log that
everything up to some index is no longer needed:

- `Compact(state, upTo)` stores `state` as a *snapshot* that stands for every
  record up to `upTo`, and frees those records.
- `Release(upTo)` frees the records without storing anything, for programs
  whose state is already durable somewhere else.

Snapshots are kept in two files, `snap.0` and `snap.1`, that are written
alternately, so a crash during `Compact` always leaves the previous snapshot
intact. See [Compaction](compaction.md).

**Metadata slots.** The start of `wal.log` holds two small header slots that
say where the live records start and which snapshot is current. Each update
overwrites the older slot, so a torn write to a slot falls back to the other
one. You never touch the slots yourself; they explain why `Open` can always
find the log's state again.

**Epochs.** Every `Open` starts a new *epoch* and records it durably before
anything is appended. Records carry the epoch they were written in. Recovery
uses epochs, together with a checksum on every record, to tell valid records
from leftovers of earlier laps around the ring or of a write cut short by a
crash. See [Recovery](recovery.md).

**Group commit.** Concurrent `Append` calls share a single write and a
single sync, so throughput grows with the number of concurrent writers.
See [Appending](appending.md).

## What to read next

- [Quick start](quickstart.md): open, append, replay, close.
- [Appending and group commit](appending.md)
- [Replay and Snapshot](replay.md): including a rule your callback must follow.
- [Compaction](compaction.md)
- [Recovery and failure model](recovery.md): what survives a crash, and what
  does not.
- [Limits and caveats](limits.md): read this before relying on tinywal.
