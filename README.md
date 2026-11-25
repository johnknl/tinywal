# tinywal

A minimal write-ahead log for Go: one fixed-size, preallocated log file used
as a ring, crash-safe snapshots, and group commit. `Append` returns once its
records are on stable storage.

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

## Documentation

The full documentation (concepts, quick start, appending and group commit,
replay, compaction, the recovery and failure model, configuration, limits and
caveats, platform notes) is in [`docs/`](docs/index.md) and built with
[MkDocs](https://www.mkdocs.org/): `make docs` serves it locally with live reload.

Note that the on-disk format will gain a version marker in an upcoming
release, and there is no automatic upgrade of existing logs yet.

## Performance

Reported results, measured on an AMD Ryzen 9 5950X, ext4 on NVMe, Linux.
Every `Append` is durable when it returns.

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
