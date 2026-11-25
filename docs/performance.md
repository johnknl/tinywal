# Performance

!!! note "Reported results"
    The figures below are results reported by the author, not guarantees. They
    were measured on one machine (AMD Ryzen 9 5950X, ext4 on NVMe, Linux) and
    were not reproduced when writing this documentation. Run the benchmarks on
    your own hardware before drawing conclusions.

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

The pattern to take from this: throughput is limited by the sync, so it grows
with concurrent writers and with records per call; see
[Appending and group commit](appending.md).

To run the benchmarks yourself, from a checkout of the repository:

```text
go test -run x -bench . ./...
```
