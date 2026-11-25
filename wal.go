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

// Package tinywal is a minimal write-ahead log: one fixed-size, preallocated
// log file used as a ring, two header slots that say where the live records
// start, snapshots in two files paired with the slots, and group commit so
// that concurrent appenders share a single sync.
//
// The log file is laid out as:
//
//	slot 0 (4 KiB) | slot 1 (4 KiB) | ring (capacity bytes)
//
// A slot holds:
//
//	crc32(4) | seq(8) | readAbs(8) | readIdx(8) | snapIdx(8) | snapLen(8) |
//	snapCRC(4) | epoch(4) | snapFile(1)
//
// The valid slot with the higher seq is current; updates always overwrite
// the other slot, so a torn slot write falls back to the previous one.
// Positions in the ring are absolute byte offsets that only grow; the ring
// position is abs % capacity and the generation, the number of times the
// ring has wrapped, is abs / capacity.
//
// A frame in the ring is:
//
//	crc32(4) | len(4) | gen(8) | index(8) | epoch(4) | data(len)
//
// Frames are contiguous and wrap byte by byte past the end of the ring. The
// crc covers everything after itself. Open walks from the current slot's
// readAbs and stops at the first frame with a bad crc, the wrong generation
// or index, or an epoch lower than the frame before it: that is the write
// position. Leftovers from earlier trips around the ring have an older
// generation; leftovers from a batch torn by a crash have an older epoch,
// because every Open starts a new epoch and stores it in a slot first.
//
// A snapshot lives in snap.0 or snap.1. Compact writes the new snapshot into
// the file the current slot does not use, then writes the other slot, so a
// crash at any point leaves a current slot whose snapshot is intact. Freeing
// ring space never needs ring space, so a full ring can always be emptied.
package tinywal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	logName      = "wal.log"
	slotSize     = 4096
	headerSize   = 2 * slotSize
	slotLen      = 4 + 8 + 8 + 8 + 8 + 8 + 4 + 4 + 1
	frameHdrLen  = 4 + 4 + 8 + 8 + 4
	maxRecord    = 64 << 20
	maxBatchSize = 1024
	dirPerm      = 0o750
	filePerm     = 0o600

	// MinCapacity is the smallest ring Open accepts.
	MinCapacity = 4096
)

var snapNames = [2]string{"snap.0", "snap.1"}

var (
	// ErrClosed is returned by calls on a closed WAL.
	ErrClosed = errors.New("tinywal: closed")
	// ErrTooLarge is returned by Append for a record, or a batch, that cannot fit in the ring.
	ErrTooLarge = errors.New("tinywal: record too large")
	// ErrFull is returned by Append when the ring has no room for the records.
	// Compact or Release frees space.
	ErrFull = errors.New("tinywal: log full")
	// ErrCorruptHeader is returned by Open when neither header slot is valid.
	ErrCorruptHeader = errors.New("tinywal: corrupt header")
	// ErrCorruptSnapshot is returned by Open when the current snapshot fails its checksum.
	ErrCorruptSnapshot = errors.New("tinywal: corrupt snapshot")

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// slot is the durable pointer to the live part of the log.
type slot struct {
	seq      uint64
	readAbs  uint64 // absolute position of the first live frame
	readIdx  uint64 // index of the first live frame
	snapIdx  uint64 // last index covered by the snapshot
	snapLen  uint64
	snapCRC  uint32
	epoch    uint32 // epoch of the session that wrote this slot
	snapFile uint8
}

type batch struct {
	err   error
	slot  *slot // a header slot to write with this batch, if any
	buf   []byte
	start uint64 // absolute position of buf[0]
	last  uint64 // index of the last record in buf
	n     int
	done  bool
}

// WAL is safe for concurrent use.
type WAL struct {
	err      error
	f        *os.File
	open     *batch
	root     *os.Root
	cond     sync.Cond
	scratch  []byte
	snapData []byte
	mapped   []byte
	ring     ring
	slot     slot
	cur      int
	endAbs   uint64
	durAbs   uint64
	durIdx   uint64
	next     uint64
	mu       sync.Mutex
	truncMu  sync.Mutex // serializes Compact and Release
	busy     bool
}

// Open opens the WAL in dir, creating it with a ring of capacity bytes if it
// does not exist. For an existing log, capacity is ignored.
func Open(dir string, capacity int64) (*WAL, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}

	w, err := open(root, capacity)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}

	return w, nil
}

func open(root *os.Root, capacity int64) (*WAL, error) {
	f, err := root.OpenFile(logName, os.O_RDWR, filePerm)
	if errors.Is(err, os.ErrNotExist) {
		f, err = create(root, capacity)
	}

	if err != nil {
		return nil, err
	}

	w := &WAL{root: root, f: f, open: &batch{}}
	w.cond.L = &w.mu

	if err = w.load(); err != nil {
		return nil, errors.Join(err, unmap(w.mapped), f.Close())
	}

	return w, nil
}

// create makes a zero-filled log with an initial slot, atomically.
func create(root *os.Root, capacity int64) (*os.File, error) {
	if capacity < MinCapacity || capacity > math.MaxInt64-headerSize {
		return nil, fmt.Errorf("tinywal: capacity %d below minimum %d", capacity, MinCapacity)
	}

	tmp := logName + ".tmp"

	t, err := root.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return nil, err
	}

	initial := encodeSlot(slot{seq: 1, readIdx: 1})

	err = zeroFill(t, 0, headerSize+capacity)
	if err == nil {
		_, err = t.WriteAt(initial, 0)
	}

	if err == nil {
		err = t.Sync()
	}

	if closeErr := t.Close(); err == nil {
		err = closeErr
	}

	if err == nil {
		err = root.Rename(tmp, logName)
	}

	if err == nil {
		err = syncDir(root)
	}

	if err != nil {
		return nil, errors.Join(err, root.Remove(tmp))
	}

	return root.OpenFile(logName, os.O_RDWR, filePerm)
}

// load maps the log, picks the current slot, loads its snapshot, finds the
// write position and starts a new epoch.
func (w *WAL) load() error {
	fi, err := w.f.Stat()
	if err != nil {
		return err
	}

	if fi.Size() < headerSize+MinCapacity {
		return ErrCorruptHeader
	}

	if w.mapped, err = mapFile(w.f, fi.Size()); err != nil {
		return err
	}

	w.ring = newRing(w.mapped[headerSize:])

	s0, ok0 := parseSlot(w.mapped[:slotSize])
	s1, ok1 := parseSlot(w.mapped[slotSize:headerSize])

	switch {
	case ok0 && (!ok1 || s0.seq > s1.seq):
		w.slot, w.cur = s0, 0
	case ok1:
		w.slot, w.cur = s1, 1
	default:
		return ErrCorruptHeader
	}

	if err = w.loadSnapshot(); err != nil {
		return err
	}

	w.recoverTail()

	// Persist the new epoch before any frame uses it, so every frame ever
	// written has an epoch no higher than a slot that is durable.
	s := w.slot
	s.seq++
	s.epoch++

	w.mu.Lock()
	defer w.mu.Unlock()

	w.open.slot = &s

	return w.commit(w.open)
}

func (w *WAL) loadSnapshot() error {
	if w.slot.snapLen == 0 {
		return nil
	}

	b, err := w.root.ReadFile(snapNames[w.slot.snapFile&1])
	if err != nil {
		return err
	}

	if uint64(len(b)) < w.slot.snapLen {
		return ErrCorruptSnapshot
	}

	b = b[:w.slot.snapLen]
	if crc32.Checksum(b, crcTable) != w.slot.snapCRC {
		return ErrCorruptSnapshot
	}

	w.snapData = b

	return nil
}

// recoverTail walks the frames from the read position and sets the write
// position after the last valid one.
func (w *WAL) recoverTail() {
	abs, idx := w.slot.readAbs, w.slot.readIdx
	limit := w.slot.readAbs + w.ring.size

	var epoch uint32

	for {
		h, ok := w.ring.header(abs)
		if !ok || abs+h.size() > limit || h.gen != abs/w.ring.size || h.idx != idx ||
			h.epoch < epoch || h.epoch > w.slot.epoch || !w.ring.checksumOK(abs, h) {
			break
		}

		epoch = h.epoch
		abs += h.size()
		idx++
	}

	w.endAbs, w.durAbs = abs, abs
	w.next, w.durIdx = idx, idx-1
}

// Snapshot returns the latest snapshot state and the last index it covers
// (nil and 0 when there is none). The state must not be modified.
func (w *WAL) Snapshot() (state []byte, index uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.snapData, w.slot.snapIdx
}

// Append durably writes records with consecutive indexes and returns the
// index of the first. All of them share one write and one sync, which they
// also share with concurrent calls (group commit); a crash can keep a prefix
// of the records, never a gap. It returns ErrFull when the ring has no room
// for all of them, and does nothing for an empty batch. Compact or Release
// frees space.
func (w *WAL) Append(records [][]byte) (uint64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	size := uint64(0)

	for _, data := range records {
		if len(data) > maxRecord {
			return 0, ErrTooLarge
		}

		size += frameHdrLen + uint64(len(data))
	}

	if size > w.ring.size {
		return 0, ErrTooLarge
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Wait for room in the open batch; a large batch waits for an empty one.
	for w.err == nil && w.open.n > 0 && w.open.n+len(records) > maxBatchSize {
		w.cond.Wait()
	}

	if w.err != nil {
		return 0, w.err
	}

	if w.endAbs+size > w.slot.readAbs+w.ring.size {
		return 0, ErrFull
	}

	b := w.open
	if b.n == 0 {
		b.start = w.endAbs
	}

	first := w.next

	for _, data := range records {
		b.buf = appendFrame(b.buf, w.endAbs/w.ring.size, w.next, w.slot.epoch, data)
		w.endAbs += frameHdrLen + uint64(len(data))
		w.next++
	}

	b.last = w.next - 1
	b.n += len(records)

	if err := w.commit(b); err != nil {
		return 0, err
	}

	return first, nil
}

// commit waits until b is durable, leading its flush when no flush is
// running. A leader takes the whole open batch, so batches are flushed in
// order. It is called, and returns, with mu held.
func (w *WAL) commit(b *batch) error {
	for w.busy && !b.done {
		w.cond.Wait()
	}

	if b.done {
		return b.err // another caller led this batch
	}

	if w.err != nil {
		return w.err
	}

	w.busy = true
	w.open = &batch{}
	target := 1 - w.cur
	w.mu.Unlock()

	err := w.flush(b, target)

	w.mu.Lock()

	b.err, b.done = err, true

	switch {
	case err != nil:
		if w.err == nil {
			w.err = fmt.Errorf("tinywal: log unusable after failed write: %w", err)
		}
	default:
		if len(b.buf) > 0 {
			w.durAbs = b.start + uint64(len(b.buf))
			w.durIdx = b.last
		}

		if b.slot != nil {
			w.slot, w.cur = *b.slot, target
		}
	}

	w.busy = false
	w.cond.Broadcast()

	return err
}

// flush writes the records of b to the ring and its slot, if any, over the
// target slot, and syncs both at once.
func (w *WAL) flush(b *batch, target int) error {
	if len(b.buf) > 0 {
		if err := w.writeRing(b.buf, b.start); err != nil {
			return err
		}
	}

	if b.slot != nil {
		if _, err := w.f.WriteAt(encodeSlot(*b.slot), int64(target)*slotSize); err != nil {
			return err
		}
	}

	return datasync(w.f)
}

// Replay calls fn for every live record, in order. data must not be
// modified, and must not be used after fn returns. Appends wait while
// Replay runs.
func (w *WAL) Replay(fn func(index uint64, data []byte) error) error {
	if err := w.acquire(); err != nil {
		return err
	}
	defer w.release()

	abs, idx := w.slot.readAbs, w.slot.readIdx
	for abs < w.durAbs {
		h, _ := w.ring.header(abs)

		a, b := w.ring.span(abs+frameHdrLen, int(h.length))

		data := a
		if len(b) > 0 {
			w.scratch = append(append(w.scratch[:0], a...), b...)
			data = w.scratch
		}

		if err := fn(idx, data); err != nil {
			return err
		}

		abs += h.size()
		idx++
	}

	return nil
}

// Compact durably stores state as the snapshot covering every record up to
// and including upTo, and frees the ring space of those records. upTo must
// lie between the first live record minus one and the last appended record.
func (w *WAL) Compact(state []byte, upTo uint64) error {
	return w.truncate(upTo, state, true)
}

// Release frees the ring space of every record up to and including upTo
// without storing a snapshot, for callers that keep their state elsewhere;
// that state must be durable first. Releasing records that are already
// released does nothing.
func (w *WAL) Release(upTo uint64) error {
	return w.truncate(upTo, nil, false)
}

func (w *WAL) truncate(upTo uint64, state []byte, snapshot bool) error {
	w.truncMu.Lock()
	defer w.truncMu.Unlock()

	w.mu.Lock()
	s, durIdx, err := w.slot, w.durIdx, w.err
	w.mu.Unlock()

	if err != nil {
		return err
	}

	first := s.readIdx
	if upTo > durIdx || (snapshot && upTo+1 < first) {
		return fmt.Errorf("tinywal: index %d outside [%d, %d]", upTo, first-1, durIdx)
	}

	if !snapshot && upTo < first {
		return nil
	}

	s.seq++

	// Skip the released frames; their sizes are in their headers. They are
	// durable, and nothing overwrites them until this slot is.
	for s.readIdx <= upTo {
		h, _ := w.ring.header(s.readAbs)
		s.readAbs += h.size()
		s.readIdx++
	}

	var stateCopy []byte

	if snapshot {
		s.snapFile = 1 - s.snapFile
		s.snapIdx = upTo
		s.snapLen = uint64(len(state))
		s.snapCRC = crc32.Checksum(state, crcTable)

		// The current slot does not use this file, so appends keep running.
		if err = w.writeSnapshot(snapNames[s.snapFile], state); err != nil {
			return err
		}

		stateCopy = append([]byte(nil), state...)
	}

	// The slot rides along with the next group commit.
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return w.err
	}

	b := w.open
	b.slot = &s

	if err = w.commit(b); err != nil {
		return err
	}

	if snapshot {
		w.snapData = stateCopy
	}

	return nil
}

// Close waits for running work and closes the log. Pending and later calls
// fail with ErrClosed.
func (w *WAL) Close() error {
	w.mu.Lock()
	for w.busy {
		w.cond.Wait()
	}

	if errors.Is(w.err, ErrClosed) {
		w.mu.Unlock()
		return nil
	}

	w.err = ErrClosed
	w.cond.Broadcast()
	w.mu.Unlock()

	return errors.Join(unmap(w.mapped), w.f.Close(), w.root.Close())
}

// acquire takes the busy slot, excluding flushes, for exclusive access.
func (w *WAL) acquire() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.busy && w.err == nil {
		w.cond.Wait()
	}

	if w.err != nil {
		return w.err
	}

	w.busy = true

	return nil
}

func (w *WAL) release() {
	w.mu.Lock()
	w.busy = false
	w.cond.Broadcast()
	w.mu.Unlock()
}

// writeSnapshot writes state at the start of the snapshot file name,
// durably. The current slot never refers to name. The file is not truncated:
// the slot records the snapshot's length, so stale bytes after it are
// ignored, and an overwrite that does not grow the file only needs
// fdatasync. The directory is synced only when the file is created.
func (w *WAL) writeSnapshot(name string, state []byte) error {
	f, err := w.root.OpenFile(name, os.O_WRONLY, filePerm)

	created := errors.Is(err, os.ErrNotExist)
	if created {
		f, err = w.root.OpenFile(name, os.O_WRONLY|os.O_CREATE, filePerm)
	}

	if err != nil {
		return err
	}

	_, err = f.WriteAt(state, 0)
	if err == nil {
		err = datasync(f)
	}

	if closeErr := f.Close(); err == nil {
		err = closeErr
	}

	if err != nil || !created {
		return err
	}

	return syncDir(w.root)
}

// writeRing writes buf to the ring at absolute position abs, wrapping.
func (w *WAL) writeRing(buf []byte, abs uint64) error {
	pos := w.ring.pos(abs)
	n := min(len(buf), len(w.ring.b)-pos)

	if _, err := w.f.WriteAt(buf[:n], int64(headerSize+pos)); err != nil {
		return err
	}

	if n < len(buf) {
		_, err := w.f.WriteAt(buf[n:], headerSize)
		return err
	}

	return nil
}

func encodeSlot(s slot) []byte {
	b := make([]byte, 4, slotLen)
	b = binary.LittleEndian.AppendUint64(b, s.seq)
	b = binary.LittleEndian.AppendUint64(b, s.readAbs)
	b = binary.LittleEndian.AppendUint64(b, s.readIdx)
	b = binary.LittleEndian.AppendUint64(b, s.snapIdx)
	b = binary.LittleEndian.AppendUint64(b, s.snapLen)
	b = binary.LittleEndian.AppendUint32(b, s.snapCRC)
	b = binary.LittleEndian.AppendUint32(b, s.epoch)
	b = append(b, s.snapFile)
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], crcTable))

	return b
}

func parseSlot(b []byte) (slot, bool) {
	if crc32.Checksum(b[4:slotLen], crcTable) != binary.LittleEndian.Uint32(b) {
		return slot{}, false
	}

	s := slot{
		seq:      binary.LittleEndian.Uint64(b[4:]),
		readAbs:  binary.LittleEndian.Uint64(b[12:]),
		readIdx:  binary.LittleEndian.Uint64(b[20:]),
		snapIdx:  binary.LittleEndian.Uint64(b[28:]),
		snapLen:  binary.LittleEndian.Uint64(b[36:]),
		snapCRC:  binary.LittleEndian.Uint32(b[44:]),
		epoch:    binary.LittleEndian.Uint32(b[48:]),
		snapFile: b[52],
	}

	return s, s.seq > 0 && s.snapFile < 2
}

func appendFrame(buf []byte, gen, idx uint64, epoch uint32, data []byte) []byte {
	start := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // crc, filled in below
	buf = binary.LittleEndian.AppendUint32(buf, recordLen(data))
	buf = binary.LittleEndian.AppendUint64(buf, gen)
	buf = binary.LittleEndian.AppendUint64(buf, idx)
	buf = binary.LittleEndian.AppendUint32(buf, epoch)
	buf = append(buf, data...)
	binary.LittleEndian.PutUint32(buf[start:], crc32.Checksum(buf[start+4:], crcTable))

	return buf
}

func recordLen(data []byte) uint32 {
	n := len(data)
	if n < 0 || n > maxRecord {
		panic("tinywal: record too large")
	}

	return uint32(n)
}

type frameHeader struct {
	gen    uint64
	idx    uint64
	length uint32
	crc    uint32
	epoch  uint32
}

func (h frameHeader) size() uint64 { return frameHdrLen + uint64(h.length) }

// ring is the mapped ring area, addressed by absolute positions.
type ring struct {
	b    []byte
	size uint64
}

func newRing(b []byte) ring { return ring{b: b, size: uint64(len(b))} }

func (r ring) pos(abs uint64) int {
	p := abs % r.size
	if p > math.MaxInt {
		panic("tinywal: ring larger than memory")
	}

	return int(p)
}

// span returns the n bytes at abs as one or, when they wrap, two slices.
func (r ring) span(abs uint64, n int) (a, b []byte) {
	p := r.pos(abs)
	if p+n <= len(r.b) {
		return r.b[p : p+n], nil
	}

	return r.b[p:], r.b[:n-(len(r.b)-p)]
}

// header decodes the frame header at abs, reporting false when its length
// cannot be a frame.
func (r ring) header(abs uint64) (frameHeader, bool) {
	var raw [frameHdrLen]byte

	a, b := r.span(abs, frameHdrLen)
	copy(raw[copy(raw[:], a):], b)

	n := binary.LittleEndian.Uint32(raw[4:])
	h := frameHeader{
		crc:    binary.LittleEndian.Uint32(raw[:]),
		length: n,
		gen:    binary.LittleEndian.Uint64(raw[8:]),
		idx:    binary.LittleEndian.Uint64(raw[16:]),
		epoch:  binary.LittleEndian.Uint32(raw[24:]),
	}

	return h, n <= maxRecord && h.size() <= r.size
}

func (r ring) checksumOK(abs uint64, h frameHeader) bool {
	a, b := r.span(abs+4, frameHdrLen-4+int(h.length))
	crc := crc32.Update(crc32.Checksum(a, crcTable), crcTable, b)

	return crc == h.crc
}

func syncDir(root *os.Root) (err error) {
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()

	return d.Sync()
}

// zeros is the source for zeroFill.
var zeros [1 << 20]byte

// zeroFill writes zeros to f in [from, to).
func zeroFill(f *os.File, from, to int64) error {
	for from < to {
		n, err := f.WriteAt(zeros[:min(to-from, int64(len(zeros)))], from)
		if err != nil {
			return err
		}

		from += int64(n)
	}

	return nil
}

// mapFile maps the first size bytes of f read-only. The caller must unmap
// the result.
func mapFile(f *os.File, size int64) ([]byte, error) {
	if size <= 0 || size > math.MaxInt {
		return nil, fmt.Errorf("tinywal: cannot map %d bytes", size)
	}

	var buf []byte

	err := control(f, func(fd int) (err error) {
		buf, err = unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
		return err
	})

	return buf, err
}

func unmap(buf []byte) error {
	if buf == nil {
		return nil
	}

	return unix.Munmap(buf)
}

// control runs fn with f's file descriptor.
func control(f *os.File, fn func(fd int) error) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}

	var fnErr error

	if err = rc.Control(func(fd uintptr) { fnErr = fn(int(fd)) }); err != nil {
		return err
	}

	return fnErr
}
