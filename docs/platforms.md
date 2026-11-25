# Platform notes

## Supported systems

tinywal needs a Unix-like operating system: the log is memory-mapped with
`mmap` through `golang.org/x/sys/unix`. Windows is not supported. It needs Go
1.27.

## How syncs work

Every durable write ends in a sync of the file:

- **Linux:** `fdatasync`. The log file is preallocated and never changes size,
  so an append does not change the file's metadata and `fdatasync` is enough;
  it skips the extra inode update that `fsync` would do.
- **Other Unix-like systems (for example macOS and the BSDs):** a full
  `fsync` through Go's `os.File.Sync`. It can be slower than `fdatasync`.
  (On macOS, Go's `Sync` asks the drive to flush its cache with
  `F_FULLFSYNC`.)

The directory is also synced when `wal.log` is created and when a snapshot
file is created for the first time, so the new names are durable.

tinywal's durability promise ("durable when `Append` returns") is only as
strong as the storage's: if the file system or device acknowledges syncs
without writing to stable storage, records can still be lost.

## File systems

The log file is mapped and also written through ordinary file writes, so it
needs a file system that supports `mmap` of regular files. Performance figures
in this documentation were measured on ext4; see [Performance](performance.md).
