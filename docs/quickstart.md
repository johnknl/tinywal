# Quick start

```go
package main

import (
	"fmt"
	"log"

	"github.com/johnknl/tinywal"
)

func main() {
	// Create the log with a 64 MiB ring, or open it if it already exists.
	w, err := tinywal.Open("data/wal", 64<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	// Recover: first the snapshot (empty on a fresh log), then every record
	// after it.
	state, last := w.Snapshot()
	fmt.Printf("snapshot: %q covers records up to %d\n", state, last)

	err = w.Replay(func(index uint64, data []byte) error {
		fmt.Println("replayed", index, string(data)) // string() copies data
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Append a batch. It is on stable storage when Append returns.
	first, err := w.Append([][]byte{[]byte("set a=1"), []byte("set b=2")})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("appended from index", first)
}
```

Run it twice: the second run replays the records of the first.

What happens here:

- `Open` creates the directory and the log if needed. The capacity argument
  only matters when the log is created; for an existing log it is ignored.
- `Snapshot` returns the latest snapshot and the index of the last record it
  covers. `Replay` then visits the records after it. Together they rebuild
  your in-memory state.
- `Append` takes a batch of records, gives them consecutive indexes and
  returns the index of the first one.
- `Close` releases the file. Appended records are already durable, so
  nothing is lost if the process dies without calling it.

A log that is only appended to eventually fills up. Add `Compact` or
`Release` calls to your program: see [Compaction](compaction.md).

A complete, tested version of the usual cycle (recover, append, compact) is
in [`example_test.go`](https://github.com/johnknl/tinywal/blob/main/example_test.go).
