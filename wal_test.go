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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const testCapacity = 64 << 10

func mustOpen(t testing.TB, dir string) *WAL {
	t.Helper()

	w, err := Open(dir, testCapacity)
	if err != nil {
		t.Fatal(err)
	}

	return w
}

// lastDurable returns the index of the last durable record.
func (w *WAL) lastDurable() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.durIdx
}

func mustAppend(t testing.TB, w *WAL, s string) uint64 {
	t.Helper()

	idx, err := w.Append([][]byte{[]byte(s)})
	if err != nil {
		t.Fatal(err)
	}

	return idx
}

func collect(t *testing.T, w *WAL) string {
	t.Helper()

	var out []string

	if err := w.Replay(func(idx uint64, data []byte) error {
		out = append(out, fmt.Sprintf("%d:%s", idx, data))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return fmt.Sprint(out)
}

// flipByte inverts one byte of a file in dir at off.
func flipByte(t *testing.T, path string, off int64) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	b := make([]byte, 1)
	if _, err = f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}

	b[0] ^= 0xff
	if _, err = f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func TestAppendReopenReplay(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	for _, s := range []string{"a", "b", "c"} {
		mustAppend(t, w, s)
	}

	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if got := collect(t, w); got != "[1:a 2:b 3:c]" {
		t.Fatalf("got %s", got)
	}

	if idx := mustAppend(t, w, "d"); idx != 4 {
		t.Fatalf("next index %d", idx)
	}
}

func TestTornBatchDoesNotComeBack(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	mustAppend(t, w, "ok")
	mustAppend(t, w, "torn")
	mustAppend(t, w, "stale")
	w.Close()

	// Corrupt the last data byte of record 2; record 3 stays intact behind it.
	flipByte(t, filepath.Join(dir, logName), headerSize+frameHdrLen+2+frameHdrLen+3)

	w = mustOpen(t, dir)
	if got := collect(t, w); got != "[1:ok]" {
		t.Fatalf("got %s", got)
	}

	// Same size as the torn record, so it ends right where stale record 3
	// starts, with the index the walk expects next. Its older epoch stops it.
	if idx := mustAppend(t, w, "agn!"); idx != 2 {
		t.Fatalf("next index %d", idx)
	}

	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if got := collect(t, w); got != "[1:ok 2:agn!]" {
		t.Fatalf("after reopen got %s", got)
	}
}

func TestWrapAround(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, MinCapacity)
	if err != nil {
		t.Fatal(err)
	}

	// 7 laps of a 4 KiB ring with frames that do not divide it evenly, so
	// frames, headers included, straddle the end.
	data := []byte(strings.Repeat("x", 300))

	var last uint64

	for range 100 {
		idx, appendErr := w.Append([][]byte{data})
		if errors.Is(appendErr, ErrFull) {
			if err = w.Release(last - 2); err != nil {
				t.Fatal(err)
			}

			idx, appendErr = w.Append([][]byte{data})
		}

		if appendErr != nil {
			t.Fatal(appendErr)
		}

		last = idx
	}

	want := collect(t, w)
	w.Close()

	w, err = Open(dir, MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if got := collect(t, w); got != want {
		t.Fatalf("after reopen got %.80s…, want %.80s…", got, want)
	}

	if !strings.HasSuffix(want, fmt.Sprintf("%d:%s]", last, data)) {
		t.Fatalf("last record missing: %.80s…", want)
	}
}

func TestFullAndRelease(t *testing.T) {
	w, err := Open(t.TempDir(), MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	data := make([]byte, 1000)

	var last uint64

	for {
		idx, appendErr := w.Append([][]byte{data})
		if errors.Is(appendErr, ErrFull) {
			break
		}

		if appendErr != nil {
			t.Fatal(appendErr)
		}

		last = idx
	}

	if err = w.Release(last); err != nil {
		t.Fatal(err)
	}

	if _, err = w.Append([][]byte{data}); err != nil {
		t.Fatalf("after release: %v", err)
	}

	if _, err = w.Append([][]byte{make([]byte, MinCapacity)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized record: %v", err)
	}
}

func TestCompact(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	for i := range 5 {
		mustAppend(t, w, fmt.Sprintf("r%d", i+1))
	}

	if err := w.Compact([]byte("state@3"), 3); err != nil {
		t.Fatal(err)
	}

	if got := collect(t, w); got != "[4:r4 5:r5]" {
		t.Fatalf("got %s", got)
	}

	mustAppend(t, w, "r6")

	if err := w.Compact([]byte("state@5"), 5); err != nil {
		t.Fatal(err)
	}

	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if state, idx := w.Snapshot(); string(state) != "state@5" || idx != 5 {
		t.Fatalf("snapshot %q@%d", state, idx)
	}

	if got := collect(t, w); got != "[6:r6]" {
		t.Fatalf("got %s", got)
	}

	if err := w.Compact(nil, 99); err == nil {
		t.Fatal("compact past the last record succeeded")
	}
}

// A torn slot write falls back to the previous slot, whose snapshot file and
// ring data are still intact.
func TestTornSlotFallsBack(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	mustAppend(t, w, "a")
	mustAppend(t, w, "b")

	if err := w.Compact([]byte("one"), 1); err != nil {
		t.Fatal(err)
	}

	if err := w.Compact([]byte("two"), 2); err != nil {
		t.Fatal(err)
	}

	cur := w.cur
	w.Close()

	flipByte(t, filepath.Join(dir, logName), int64(cur)*slotSize+8)

	w = mustOpen(t, dir)
	defer w.Close()

	if state, idx := w.Snapshot(); string(state) != "one" || idx != 1 {
		t.Fatalf("snapshot %q@%d", state, idx)
	}

	if got := collect(t, w); got != "[2:b]" {
		t.Fatalf("got %s", got)
	}
}

func TestCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	mustAppend(t, w, "a")

	if err := w.Compact([]byte("state"), 1); err != nil {
		t.Fatal(err)
	}

	file := w.slot.snapFile
	w.Close()

	flipByte(t, filepath.Join(dir, snapNames[file]), 0)

	if _, err := Open(dir, testCapacity); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err %v", err)
	}
}

func TestConcurrentGroupCommit(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)

	const n = 500

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, err := w.Append([][]byte{fmt.Appendf(nil, "%d", i)}); err != nil {
				t.Error(err)
			}

			if i == n/2 {
				if err := w.Compact(nil, 1); err != nil {
					t.Error(err)
				}
			}
		})
	}

	wg.Wait()
	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	count := 0
	prev := uint64(1)

	if err := w.Replay(func(idx uint64, _ []byte) error {
		if idx != prev+1 {
			return fmt.Errorf("gap: %d after %d", idx, prev)
		}

		prev = idx
		count++

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if count != n-1 {
		t.Fatalf("replayed %d", count)
	}
}

func TestClosed(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	w.Close()

	if _, err := w.Append([][]byte{[]byte("x")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err %v", err)
	}
}

func TestAppend(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	mustAppend(t, w, "a")

	first, err := w.Append([][]byte{[]byte("b"), []byte("c"), []byte("d")})
	if err != nil {
		t.Fatal(err)
	}

	if first != 2 {
		t.Fatalf("first index %d", first)
	}

	if idx := mustAppend(t, w, "e"); idx != 5 {
		t.Fatalf("next index %d", idx)
	}

	if _, err = w.Append(nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}

	// A batch that does not fit as a whole is refused as a whole.
	big := make([][]byte, 100)
	for i := range big {
		big[i] = make([]byte, 1000)
	}

	if _, err = w.Append(big); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("batch larger than the ring: %v", err)
	}

	small, err := Open(t.TempDir(), MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()

	if _, err = small.Append(big[:3]); err != nil {
		t.Fatal(err)
	}

	if _, err = small.Append(big[:2]); !errors.Is(err, ErrFull) {
		t.Fatalf("batch larger than the free space: %v", err)
	}

	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if got := collect(t, w); got != "[1:a 2:b 3:c 4:d 5:e]" {
		t.Fatalf("got %s", got)
	}
}

// Compactions joining in-flight group commits keep the log consistent: after
// a reopen, the records follow the snapshot without a gap.
func TestCompactDuringAppends(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)

	const writers, each = 8, 200

	var wg sync.WaitGroup

	for range writers {
		wg.Go(func() {
			for range each {
				if _, err := w.Append([][]byte{[]byte("rec")}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}

	wg.Go(func() {
		for range 20 {
			last := w.lastDurable()
			if err := w.Compact(fmt.Appendf(nil, "%d", last), last); err != nil {
				t.Error(err)
				return
			}
		}
	})

	wg.Wait()
	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	state, snapIdx := w.Snapshot()
	if string(state) != fmt.Sprint(snapIdx) {
		t.Fatalf("snapshot %q does not match index %d", state, snapIdx)
	}

	next := snapIdx + 1

	if err := w.Replay(func(idx uint64, _ []byte) error {
		if idx != next {
			return fmt.Errorf("got index %d, want %d", idx, next)
		}

		next++

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if next-1 != writers*each {
		t.Fatalf("last index %d, want %d", next-1, writers*each)
	}
}

// Snapshot files are overwritten without truncation; a shorter snapshot
// must not pick up the tail of an older, longer one.
func TestSnapshotShrinks(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)

	for i, state := range []string{"a long first state", "second state, also long", "x", "y"} {
		mustAppend(t, w, "r")

		if err := w.Compact([]byte(state), uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}

	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()

	if state, idx := w.Snapshot(); string(state) != "y" || idx != 4 {
		t.Fatalf("snapshot %q@%d", state, idx)
	}
}
