# tinywal

A minimal write-ahead log for Go: one fixed-size, preallocated log file used
as a ring, crash-safe snapshots, and group commit.

```
go get github.com/johnknl/tinywal
```

Requires Go 1.27 and a Unix-like OS (`mmap` through `golang.org/x/sys/unix`).

## Usage

```go
w, err := tinywal.Open(dir, 64<<20) // ring capacity, used when creating the log
if err != nil {
	return err
}
defer w.Close()

// Recover: the snapshot, then every record after it.
state, _ := w.Snapshot()
restore(state)

err = w.Replay(func(index uint64, data []byte) error {
	return apply(index, data) // data is only valid during the call
})

// Append a batch: consecutive indexes, durable when Append returns.
first, err := w.Append([][]byte{rec1, rec2}) // first is rec1's index

// Free ring space, storing the state that covers it...
err = w.Compact(stateBytes, upTo)
// ...or without a snapshot, when the state is durable elsewhere.
err = w.Release(upTo)
```

Runnable examples are in [`example_test.go`](example_test.go).

## Semantics

- `Append` returns once its records are on stable storage. Concurrent calls
  share one write and one sync (group commit), as do the records of a batch.
- A crash keeps a prefix of the appended records, never a gap. Records whose
  `Append` returned are never lost.
- The log has a fixed capacity. `Append` returns `ErrFull` when the ring has
  no room for the whole batch; `Compact` or `Release` frees space. Neither
  needs ring space, so a full log can always be emptied.
- `Compact(state, upTo)` stores `state` as the snapshot covering every record
  up to `upTo`. `Release(upTo)` frees the same records without a snapshot;
  `Snapshot` then still returns the last compacted state.
- After a failed write or sync, the WAL returns an error for every call until
  it is reopened.

## On disk

```
wal.log   slot 0 (4 KiB) | slot 1 (4 KiB) | ring (capacity bytes)
snap.0    snapshot state
snap.1    snapshot state
```

The log is created once, zero-filled, and never grows, shrinks or gets
replaced. Appends only overwrite written blocks, so a sync never changes the
file's metadata, and on Linux `fdatasync` is enough.

**Frames** are `crc32 | len | gen | index | epoch | data`, written
contiguously and wrapping byte by byte past the end of the ring. Positions are
absolute byte offsets that only grow: the ring offset is `pos % capacity`, the
generation `pos / capacity`.

**Slots** hold the read position, the index there, the snapshot's index,
length, checksum and file, and the current epoch. The valid slot with the
higher sequence number is current; updates overwrite the other slot.

**Compact** writes the state into the snapshot file the current slot does not
use, syncs it, and then writes the other slot with the next group commit. A
crash at any point leaves a current slot with an intact snapshot. Ring space is
reused only after the new slot is durable.

## Recovery

`Open` takes the current slot, checks its snapshot, and walks the frames from
the read position. It stops at the first frame with a bad checksum, the wrong
generation or index, or a lower epoch than the frame before. That is where
appends continue.

- Frames left over from an earlier trip around the ring have an older
  generation.
- Frames left over from a batch torn by a crash have an older epoch: every
  `Open` stores a new epoch in a slot before anything is appended.

So no part of the log is zeroed or rewritten during recovery. A torn slot write
falls back to the other slot. A corrupt current snapshot makes `Open` fail with
`ErrCorruptSnapshot`.

## Limits

- Capacity is fixed when the log is created; there is no resizing.
- A record is at most 64 MiB, and a batch must fit in the ring.
- `Replay` and the flush of a batch exclude each other; appends wait while a
  replay runs.
- Syncs use `fdatasync` on Linux and `fsync` elsewhere.

## Performance

AMD Ryzen 9 5950X, ext4 on NVMe, Linux. Every `Append` is durable when it
returns.

| Benchmark                     | 16 B records | 4 KiB records |
|-------------------------------|-------------:|--------------:|
| 1 writer, 1 record per call   |    2,400 /s  |     2,300 /s  |
| 1 writer, 100 per call        |    233 k/s   |      69 k/s   |
| 16 writers, 1 per call        |     21 k/s   |      18 k/s   |
| 16 writers, 200 per call      |    1.9 M/s   |     151 k/s   |
| 1024 writers, 1 per call      |    590 k/s   |     106 k/s   |

Replay reads about 47 M records/s without allocating. `Compact` with a 4 KiB
state takes about 0.9 ms, `Release` about 0.45 ms; compacting every 5 ms
under 16 concurrent writers costs about 1% of append throughput.

```
go test -run x -bench . ./...
```

## Testing

```
go test -race ./...                 # unit tests, examples, fuzz seed corpus
go test -short ./...                # skips the 64 MiB max-record test
go test -fuzz FuzzOps -fuzztime 1m  # also FuzzCorruption, FuzzSlot
gremlins unleash                    # mutation testing, see .gremlins.yaml
```
