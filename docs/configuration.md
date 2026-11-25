# Configuration

tinywal has a single entry point and no option structs:

```go
w, err := tinywal.Open(dir, capacity)
```

## `dir`

The directory that holds the log. `Open` creates it (and missing parents) if
needed, with mode `0750`; files are created with mode `0600`, subject to your
umask. It contains:

| File | Content |
|---|---|
| `wal.log` | The two header slots followed by the ring. |
| `snap.0`, `snap.1` | Snapshots, written alternately by `Compact`. Created when first needed. |
| `wal.log.tmp` | Only while a new log is being created. |

Do not modify these files, and do not put two WALs in one directory. See
[Recovery](recovery.md#one-wal-per-directory).

## `capacity`

The size of the ring in bytes. It is used only when the log is created:

- `capacity` must be at least `tinywal.MinCapacity` (4096). A smaller value
  makes `Open` fail with an ordinary error, and no file is created.
- For an existing log `capacity` is ignored, even if it differs from the real
  one; passing `0` is fine. There is no resizing.
- `wal.log` is zero-filled and synced at creation, so it takes `capacity`
  plus 8 KiB of disk space up front, and creating a large log takes time in
  proportion to its size. The file never grows or shrinks afterwards.

Choose `capacity` so the ring can hold all records written between two
compactions, with some headroom. Each record costs its length plus a small
fixed header (currently 28 bytes), and a batch must fit as a whole. A single
record can be at most 64 MiB; a record or batch larger than the ring
returns `ErrTooLarge`.

The snapshot files are separate from the ring and are not limited by
`capacity`. A snapshot file keeps the size of the largest state written to it
so far, even if later states are smaller. Open reads the current snapshot
fully into memory.

The ring is memory-mapped in full when the log is opened.
