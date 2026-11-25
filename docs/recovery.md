# Recovery and failure model

This page describes what `Open` does with an existing log and what you can
and cannot rely on after a crash, as the code behaves today.

## What `Open` does

1. Picks the current header slot: the valid one with the higher sequence
   number. If neither slot is valid, or the file is too short, `Open` fails
   with `ErrCorruptHeader`.
2. Loads the snapshot that slot refers to and checks its checksum. A snapshot
   that fails the check, or is too short, gives `ErrCorruptSnapshot`; a missing
   snapshot file gives the underlying file error. There is no fallback to
   the other snapshot file.
3. Walks the records from the first live one and finds where writing continues
   (see below).
4. Starts a new epoch and syncs it to a header slot. So every `Open` of an
   existing log costs one sync, and nothing in the ring is rewritten or
   zeroed.

Creating a new log is atomic: the file is written under a temporary name
(`wal.log.tmp`) and renamed once complete, so a crash during creation leaves
no half-made `wal.log`. A leftover `wal.log.tmp` is overwritten by the next
attempt.

## Recovery stops at the first invalid record

The walk accepts a record only if its checksum is correct and it has the
expected index, belongs to the current lap around the ring, and has an epoch
that is not older than the record before it. At the first record that fails
any of these, recovery stops: that is the end of the log, and new appends
continue there.

This has consequences you should know about:

- **A torn tail and corruption look the same.** A crash that cut off the last
  write and a damaged record in the middle of the log are both "an invalid
  record". tinywal does not distinguish them and reports neither.
- **Damage in the middle silently drops the rest.** Records after a corrupted
  record are not returned by `Replay`, `Open` returns no error, and later
  appends overwrite them.
- **Replay never shows a gap.** What you read is always a run of consecutive
  indexes.

Leftovers do not come back to life: records from an earlier lap around the
ring have an older generation, and records from a write that was cut short by
a crash have an older epoch than the next `Open` writes, so they are not
mistaken for valid records.

If the newest header slot is damaged, `Open` uses the other one, which
describes an earlier state. That is meant for a slot write that was torn by a
crash; it is not a general repair mechanism.

## What survives a crash

- A record whose `Append` returned `nil` is not lost, provided the storage
  honors syncs (see [Platform notes](platforms.md)).
- A record whose `Append` did not return (or returned an error) may or may not
  be in the log after restart. It can reappear, and it can be gone.
- **A multi-record `Append` is not atomic.** The records of a batch are written
  together, but after a crash you can get a *prefix* of them, never a gap and
  never a later record without the earlier ones. The same holds for a group
  commit across callers: a crash can keep the first part of the combined
  batch only. If a set of changes must be all-or-nothing, put it in a single
  record: one record is either fully recovered or not at all.
- `Compact` is all-or-nothing: after a crash you see the old snapshot with
  its records or the new one.

## One WAL per directory

Nothing stops you from opening the same directory twice, in one process or
in two: there is no lock. The instances do not know about each other and would
write the same ring positions and header slots independently, so this is
unsupported. Make sure only one `WAL` is open on a directory at a time.
