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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// model is what the WAL should contain: the live records and the snapshot.
type model struct {
	data    map[uint64][]byte
	state   []byte
	first   uint64 // index of the first live record
	next    uint64 // index of the next record
	snapIdx uint64
}

func (m *model) used() uint64 {
	var n uint64
	for i := m.first; i < m.next; i++ {
		n += frameHdrLen + uint64(len(m.data[i]))
	}

	return n
}

// check compares the WAL with the model.
func (m *model) check(t *testing.T, w *WAL) {
	t.Helper()

	if state, idx := w.Snapshot(); !bytes.Equal(state, m.state) || idx != m.snapIdx {
		t.Fatalf("snapshot %q@%d, want %q@%d", state, idx, m.state, m.snapIdx)
	}

	want := m.first

	if err := w.Replay(func(idx uint64, data []byte) error {
		if idx != want || !bytes.Equal(data, m.data[idx]) {
			return fmt.Errorf("record %d (%d bytes), want %d (%d bytes)", idx, len(data), want, len(m.data[want]))
		}

		want++

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if want != m.next {
		t.Fatalf("replay ended before %d, want %d", want, m.next)
	}
}

// FuzzOps runs random appends, releases, compactions and reopens on a small
// ring, and checks the WAL against a model after each.
func FuzzOps(f *testing.F) {
	f.Add([]byte{0, 3, 200, 100, 50, 7, 0, 1, 90, 1, 2, 2, 5, 3})
	f.Add([]byte{0, 0, 255, 0, 0, 255, 0, 0, 255, 0, 0, 255, 1, 3, 0, 0, 255, 3})
	f.Add(bytes.Repeat([]byte{0, 2, 150, 150, 150, 1, 4, 3}, 8))

	f.Fuzz(func(t *testing.T, ops []byte) {
		dir := t.TempDir()

		w, err := Open(dir, MinCapacity)
		if err != nil {
			t.Fatal(err)
		}

		defer func() { _ = w.Close() }()

		m := &model{data: map[uint64][]byte{}, first: 1, next: 1}
		pos := 0
		next := func() byte {
			if pos >= len(ops) {
				return 0
			}

			pos++

			return ops[pos-1]
		}

		for pos < len(ops) {
			switch next() % 4 {
			case 0: // append a batch
				records := make([][]byte, 1+int(next())%4)
				size := uint64(0)

				for i := range records {
					fill := next()
					records[i] = bytes.Repeat([]byte{fill}, int(fill)*7%1500)
					size += frameHdrLen + uint64(len(records[i]))
				}

				first, appendErr := w.Append(records)

				switch {
				case size > MinCapacity:
					if !errors.Is(appendErr, ErrTooLarge) {
						t.Fatalf("batch of %d bytes: %v, want ErrTooLarge", size, appendErr)
					}
				case m.used()+size > MinCapacity:
					if !errors.Is(appendErr, ErrFull) {
						t.Fatalf("batch of %d bytes with %d used: %v, want ErrFull", size, m.used(), appendErr)
					}
				default:
					if appendErr != nil || first != m.next {
						t.Fatalf("append: first %d, %v; want %d", first, appendErr, m.next)
					}

					for _, r := range records {
						m.data[m.next] = r
						m.next++
					}
				}
			case 1: // release
				upTo := m.first - 1 + uint64(next())%(m.next-m.first+1)
				if err = w.Release(upTo); err != nil {
					t.Fatalf("release %d: %v", upTo, err)
				}

				m.first = max(m.first, upTo+1)
			case 2: // compact
				upTo := m.first - 1 + uint64(next())%(m.next-m.first+1)
				state := fmt.Appendf(nil, "state@%d", upTo)

				if err = w.Compact(state, upTo); err != nil {
					t.Fatalf("compact %d: %v", upTo, err)
				}

				m.first, m.state, m.snapIdx = upTo+1, state, upTo
			case 3: // reopen
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}

				if w, err = Open(dir, MinCapacity); err != nil {
					t.Fatalf("reopen: %v", err)
				}
			}

			m.check(t, w)
		}
	})
}

// FuzzCorruption overwrites bytes of a log and checks that Open either fails
// cleanly or recovers only records that were written, in order, without gaps.
func FuzzCorruption(f *testing.F) {
	f.Add(uint32(0), []byte{0xff})
	f.Add(uint32(slotSize), []byte{1, 2, 3})
	f.Add(uint32(headerSize+frameHdrLen+4), []byte{0xff, 0xff, 0xff, 0x7f})
	f.Add(uint32(headerSize+200), bytes.Repeat([]byte{0}, 64))

	f.Fuzz(func(t *testing.T, offset uint32, patch []byte) {
		dir := t.TempDir()

		w, err := Open(dir, MinCapacity)
		if err != nil {
			t.Fatal(err)
		}

		// Records whose sizes do not divide the ring, wrapping a few times,
		// with snapshots along the way.
		written := map[uint64][]byte{}
		states := map[string]bool{"": true}

		for i := range 40 {
			r := bytes.Repeat([]byte{byte(i)}, 50+i*13%170)

			idx, appendErr := w.Append([][]byte{r})
			if errors.Is(appendErr, ErrFull) {
				upTo := w.lastDurable() - 2
				state := fmt.Appendf(nil, "state@%d", upTo)

				if err = w.Compact(state, upTo); err != nil {
					t.Fatal(err)
				}

				states[string(state)] = true

				idx, appendErr = w.Append([][]byte{r})
			}

			if appendErr != nil {
				t.Fatal(appendErr)
			}

			written[idx] = r
		}

		// Where the current slot and the live records are, to tell a patch
		// that must not lose anything from one that may.
		cur, live := w.cur, w.slot
		end, capacity, last := w.durAbs, w.ring.size, w.durIdx
		_ = w.Close()

		path := filepath.Join(dir, logName)

		file, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}

		off := int(offset % uint32(len(file)))
		n := copy(file[off:], patch)
		harmless := !overlaps(off, n, cur*slotSize, slotLen) &&
			!overlapsRing(off, n, live.readAbs, end, capacity)

		if err = os.WriteFile(path, file, 0o600); err != nil {
			t.Fatal(err)
		}

		w, err = Open(dir, MinCapacity)
		if err != nil && harmless {
			t.Fatalf("open failed after a patch outside the live data: %v", err)
		}

		if err != nil {
			if !errors.Is(err, ErrCorruptHeader) && !errors.Is(err, ErrCorruptSnapshot) {
				t.Fatalf("open failed with an unexpected error: %v", err)
			}

			return
		}

		defer func() { _ = w.Close() }()

		if state, _ := w.Snapshot(); !states[string(state)] {
			t.Fatalf("snapshot %q was never written", state)
		}

		var prev uint64

		replayed := 0

		if err = w.Replay(func(idx uint64, data []byte) error {
			if prev != 0 && idx != prev+1 {
				return fmt.Errorf("gap: %d after %d", idx, prev)
			}

			if !bytes.Equal(data, written[idx]) {
				return fmt.Errorf("record %d was never written like this", idx)
			}

			prev = idx
			replayed++

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if harmless {
			if want := int(last - live.readIdx + 1); replayed != want || prev != last {
				t.Fatalf("replayed %d records up to %d after a patch outside the live data", replayed, prev)
			}

			if state, idx := w.Snapshot(); idx != live.snapIdx || !states[string(state)] {
				t.Fatalf("snapshot %q@%d, want index %d", state, idx, live.snapIdx)
			}
		}

		// The recovered log must keep working.
		if _, err = w.Append([][]byte{[]byte("after")}); err != nil && !errors.Is(err, ErrFull) {
			t.Fatalf("append after recovery: %v", err)
		}

		if err = w.Close(); err != nil {
			t.Fatal(err)
		}

		if w, err = Open(dir, MinCapacity); err != nil {
			t.Fatalf("reopen after recovery: %v", err)
		}
	})
}

// overlaps reports whether [off, off+n) intersects [start, start+size).
func overlaps(off, n, start, size int) bool {
	return off < start+size && start < off+n
}

// overlapsRing reports whether the file range [off, off+n) touches the live
// ring bytes [from, to), given as absolute positions.
func overlapsRing(off, n int, from, to, capacity uint64) bool {
	for abs := from; abs < to; abs++ {
		if overlaps(off, n, headerSize+int(abs%capacity), 1) {
			return true
		}
	}

	return false
}

// FuzzSlot opens logs whose current slot has a valid checksum but arbitrary
// contents, over a ring of arbitrary bytes. Open and Replay must not panic,
// and appending must not corrupt the log.
func FuzzSlot(f *testing.F) {
	f.Add(uint64(0), uint64(1), uint64(0), uint32(0), byte(0), []byte{})
	f.Add(^uint64(0)-10, ^uint64(0), ^uint64(0), ^uint32(0), byte(1), []byte{1, 2, 3})
	f.Add(uint64(MinCapacity*3+5), uint64(7), uint64(3), uint32(9), byte(0), bytes.Repeat([]byte{0xab}, 300))

	f.Fuzz(func(t *testing.T, readAbs, readIdx, snapLen uint64, epoch uint32, snapFile byte, ring []byte) {
		dir := t.TempDir()

		w, err := Open(dir, MinCapacity)
		if err != nil {
			t.Fatal(err)
		}

		_ = w.Close()

		file := make([]byte, headerSize+MinCapacity)
		copy(file, encodeSlot(slot{
			seq: 1, readAbs: readAbs, readIdx: readIdx, snapLen: snapLen % 64,
			epoch: epoch, snapFile: snapFile,
		}))
		copy(file[headerSize:], ring)

		if err = os.WriteFile(filepath.Join(dir, logName), file, 0o600); err != nil {
			t.Fatal(err)
		}

		w, err = Open(dir, MinCapacity)
		if err != nil {
			return
		}

		defer func() { _ = w.Close() }()

		if err = w.Replay(func(uint64, []byte) error { return nil }); err != nil {
			t.Fatal(err)
		}

		first, err := w.Append([][]byte{[]byte("x")})
		if err != nil {
			return
		}

		var got []byte

		if err = w.Replay(func(idx uint64, data []byte) error {
			if idx == first {
				got = append(got, data...)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if string(got) != "x" {
			t.Fatalf("appended record %d replayed as %q", first, got)
		}
	})
}
