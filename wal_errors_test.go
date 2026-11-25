// MIT License
//
// Copyright (C) 2026 John Kleijn
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE

package tinywal

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(file, testCapacity); err == nil {
		t.Fatal("open on a regular file succeeded")
	}

	if _, err := Open(t.TempDir(), MinCapacity-1); err == nil {
		t.Fatal("capacity below the minimum was accepted")
	}
}

func TestOpenCorruptHeader(t *testing.T) {
	t.Run("short file", func(t *testing.T) {
		dir := t.TempDir()
		mustOpen(t, dir).Close()

		if err := os.Truncate(filepath.Join(dir, logName), headerSize); err != nil {
			t.Fatal(err)
		}

		if _, err := Open(dir, testCapacity); !errors.Is(err, ErrCorruptHeader) {
			t.Fatalf("err %v", err)
		}
	})

	t.Run("both slots", func(t *testing.T) {
		dir := t.TempDir()
		mustOpen(t, dir).Close()

		flipByte(t, filepath.Join(dir, logName), 8)
		flipByte(t, filepath.Join(dir, logName), slotSize+8)

		if _, err := Open(dir, testCapacity); !errors.Is(err, ErrCorruptHeader) {
			t.Fatalf("err %v", err)
		}
	})
}

func TestOpenBadSnapshotFile(t *testing.T) {
	setup := func(t *testing.T) (dir, snap string) {
		t.Helper()

		dir = t.TempDir()
		w := mustOpen(t, dir)
		mustAppend(t, w, "a")

		if err := w.Compact([]byte("state"), 1); err != nil {
			t.Fatal(err)
		}

		snap = filepath.Join(dir, snapNames[w.slot.snapFile])
		w.Close()

		return dir, snap
	}

	t.Run("missing", func(t *testing.T) {
		dir, snap := setup(t)
		if err := os.Remove(snap); err != nil {
			t.Fatal(err)
		}

		if _, err := Open(dir, testCapacity); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err %v", err)
		}
	})

	t.Run("short", func(t *testing.T) {
		dir, snap := setup(t)
		if err := os.Truncate(snap, 2); err != nil {
			t.Fatal(err)
		}

		if _, err := Open(dir, testCapacity); !errors.Is(err, ErrCorruptSnapshot) {
			t.Fatalf("err %v", err)
		}
	})
}

func TestAppendRecordTooLarge(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	defer w.Close()

	if _, err := w.Append([][]byte{make([]byte, maxRecord+1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err %v", err)
	}
}

func TestReplayError(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	defer w.Close()

	mustAppend(t, w, "a")
	mustAppend(t, w, "b")

	stop := errors.New("stop")
	calls := 0

	err := w.Replay(func(uint64, []byte) error {
		calls++
		return stop
	})
	if !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
}

func TestTruncateBounds(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	defer w.Close()

	for range 3 {
		mustAppend(t, w, "r")
	}

	if err := w.Release(2); err != nil {
		t.Fatal(err)
	}

	if err := w.Release(1); err != nil {
		t.Fatalf("releasing released records: %v", err)
	}

	if err := w.Release(4); err == nil {
		t.Fatal("release past the last record succeeded")
	}

	if err := w.Compact(nil, 1); err == nil {
		t.Fatal("compact before the first live record succeeded")
	}

	if got := collect(t, w); got != "[3:r]" {
		t.Fatalf("got %s", got)
	}
}

func TestClosedCalls(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	mustAppend(t, w, "a")

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	if err := w.Replay(func(uint64, []byte) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("replay: %v", err)
	}

	if err := w.Compact(nil, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("compact: %v", err)
	}
}

// After a failed write the WAL refuses everything until it is reopened, and
// the records that were durable before survive.
func TestWriteFailureIsSticky(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	mustAppend(t, w, "a")

	if err := w.f.Close(); err != nil { // every later write fails
		t.Fatal(err)
	}

	if _, err := w.Append([][]byte{[]byte("b")}); err == nil {
		t.Fatal("append after a failed write succeeded")
	}

	if _, err := w.Append([][]byte{[]byte("c")}); err == nil || errors.Is(err, ErrClosed) {
		t.Fatalf("second append: %v", err)
	}

	if err := w.Release(1); err == nil {
		t.Fatal("release after a failed write succeeded")
	}

	if err := w.Replay(func(uint64, []byte) error { return nil }); err == nil {
		t.Fatal("replay after a failed write succeeded")
	}

	_ = w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if got := collect(t, w); got != "[1:a]" {
		t.Fatalf("got %s", got)
	}
}

// A snapshot that cannot be written leaves the log as it was.
func TestCompactSnapshotWriteFails(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	defer w.Close()

	mustAppend(t, w, "a")

	// Compact writes the file the current slot does not use; block it.
	target := snapNames[1-w.slot.snapFile]
	if err := os.Mkdir(filepath.Join(dir, target), 0o750); err != nil {
		t.Fatal(err)
	}

	if err := w.Compact([]byte("state"), 1); err == nil {
		t.Fatal("compact succeeded")
	}

	if got := collect(t, w); got != "[1:a]" {
		t.Fatalf("got %s", got)
	}

	if idx := mustAppend(t, w, "b"); idx != 2 {
		t.Fatalf("next index %d", idx)
	}
}

// Batches that together exceed the batch limit wait for a leader to take the
// open batch, and all get consecutive, gap-free indexes.
func TestLargeConcurrentBatches(t *testing.T) {
	w, err := Open(t.TempDir(), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const writers, calls, per = 8, 4, 600

	batch := make([][]byte, per)
	for i := range batch {
		batch[i] = []byte("x")
	}

	var wg sync.WaitGroup

	for range writers {
		wg.Go(func() {
			for range calls {
				if _, err := w.Append(batch); err != nil {
					t.Error(err)
				}
			}
		})
	}

	wg.Wait()

	if last := w.lastDurable(); last != writers*calls*per {
		t.Fatalf("last index %d", last)
	}
}

// A batch that fills the ring exactly is accepted, and survives a reopen.
func TestExactFill(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, MinCapacity)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = w.Append([][]byte{make([]byte, MinCapacity-frameHdrLen)}); err != nil {
		t.Fatalf("exact fill: %v", err)
	}

	if _, err = w.Append([][]byte{nil}); !errors.Is(err, ErrFull) {
		t.Fatalf("append to a full ring: %v", err)
	}

	if _, err = w.Append([][]byte{make([]byte, MinCapacity-frameHdrLen+1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("one byte over the ring: %v", err)
	}

	w.Close()

	w, err = Open(dir, MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	n := 0

	if err = w.Replay(func(_ uint64, data []byte) error {
		n = len(data)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if n != MinCapacity-frameHdrLen {
		t.Fatalf("replayed %d bytes", n)
	}
}

// The largest record is accepted and survives a reopen.
func TestMaxRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 64 MiB log")
	}

	dir := t.TempDir()

	w, err := Open(dir, maxRecord+frameHdrLen)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = w.Append([][]byte{make([]byte, maxRecord)}); err != nil {
		t.Fatalf("max record: %v", err)
	}

	w.Close()

	w, err = Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if last := w.lastDurable(); last != 1 {
		t.Fatalf("last index after reopen %d", last)
	}
}

func TestReleaseFirstLive(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	defer w.Close()

	mustAppend(t, w, "a")
	mustAppend(t, w, "b")

	if err := w.Release(1); err != nil {
		t.Fatal(err)
	}

	if got := collect(t, w); got != "[2:b]" {
		t.Fatalf("after releasing the first record got %s", got)
	}

	if err := w.Release(2); err != nil {
		t.Fatal(err)
	}

	if got := collect(t, w); got != "[]" {
		t.Fatalf("got %s", got)
	}

	err := w.Release(3)
	if err == nil || err.Error() != "tinywal: index 3 outside [2, 2]" {
		t.Fatalf("err %v", err)
	}
}

func TestCreateSizeAndCapacityCheck(t *testing.T) {
	dir := t.TempDir()

	if _, err := Open(dir, MinCapacity-1); err == nil || errors.Is(err, ErrCorruptHeader) {
		t.Fatalf("err %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, logName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("log created for a rejected capacity: %v", err)
	}

	w, err := Open(dir, testCapacity+1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	fi, err := os.Stat(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}

	if fi.Size() != headerSize+testCapacity+1 {
		t.Fatalf("log size %d", fi.Size())
	}
}

// A slot with a valid checksum but an impossible snapshot file is ignored.
func TestSlotWithBadSnapFile(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	mustAppend(t, w, "a")

	bad := w.slot
	bad.seq += 10
	bad.snapFile = 2
	bad.snapLen = 5
	other := 1 - w.cur
	w.Close()

	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = f.WriteAt(encodeSlot(bad), int64(other)*slotSize); err != nil {
		t.Fatal(err)
	}

	f.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if got := collect(t, w); got != "[1:a]" {
		t.Fatalf("got %s", got)
	}
}

// A length field damaged into a huge value is a bad frame, not a panic.
func TestHugeLengthField(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	mustAppend(t, w, "a")
	mustAppend(t, w, "b")
	w.Close()

	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	// The length field of record 2, which starts after record 1's frame.
	if _, err = f.WriteAt([]byte{0xff, 0xff, 0xff, 0x7f}, headerSize+frameHdrLen+1+4); err != nil {
		t.Fatal(err)
	}

	f.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if got := collect(t, w); got != "[1:a]" {
		t.Fatalf("got %s", got)
	}
}
