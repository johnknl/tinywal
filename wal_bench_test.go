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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const benchCapacity = 64 << 20

func benchOpen(b *testing.B) *WAL {
	b.Helper()

	w, err := Open(b.TempDir(), benchCapacity)
	if err != nil {
		b.Fatal(err)
	}

	return w
}

// appendOrRelease appends records, releasing everything durable when the
// ring is full, as a consumer that keeps up would.
func appendOrRelease(w *WAL, records [][]byte) error {
	for {
		_, err := w.Append(records)
		if !errors.Is(err, ErrFull) {
			return err
		}

		if err = w.Release(w.lastDurable()); err != nil {
			return err
		}
	}
}

// reportRecords adds a records/s metric for n records handled in the timed part of b.
func reportRecords(b *testing.B, n int) {
	b.Helper()

	if s := b.Elapsed().Seconds(); s > 0 {
		b.ReportMetric(float64(n)/s, "records/s")
	}
}

// appendN appends n records of size bytes from the given number of writers,
// up to batch records per Append call.
func appendN(b *testing.B, w *WAL, writers, batch, n, size int) {
	b.Helper()

	records := make([][]byte, batch)
	for i := range records {
		records[i] = make([]byte, size)
	}

	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)

	for range writers {
		wg.Go(func() {
			for {
				end := next.Add(int64(batch))
				start := end - int64(batch)

				if start >= int64(n) {
					return
				}

				if err := appendOrRelease(w, records[:min(end, int64(n))-start]); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}

	wg.Wait()
}

// BenchmarkAppend measures durable appends by concurrent writers, each
// appending batch records per call. One op is one record. Group commit
// shares each sync across writers, and a batch shares it across its
// records. The ring wraps, and is released whenever it fills up.
func BenchmarkAppend(b *testing.B) {
	cases := []struct{ writers, batch int }{
		{1, 1}, {16, 1}, {1024, 1}, {1, 10}, {1, 100},
		{16, 200}, // where throughput levels off for 16 writers
	}

	for _, c := range cases {
		for _, size := range []int{16, 4096} {
			name := fmt.Sprintf("writers=%d/batch=%d/size=%d", c.writers, c.batch, size)
			b.Run(name, func(b *testing.B) {
				w := benchOpen(b)
				defer w.Close()

				b.ResetTimer()
				appendN(b, w, c.writers, c.batch, b.N, size)
				b.StopTimer()
				reportRecords(b, b.N)
			})
		}
	}
}

// BenchmarkReplay measures reading back a log of 10,000 records. One op is
// one full replay.
func BenchmarkReplay(b *testing.B) {
	w := benchOpen(b)
	defer w.Close()

	appendN(b, w, 256, 1, 10_000, 128)

	seen := 0

	for b.Loop() {
		if err := w.Replay(func(uint64, []byte) error {
			seen++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}

	reportRecords(b, seen)
}

// BenchmarkTruncate measures freeing 1,000 records, with a 4 KiB snapshot
// (Compact) or without one (Release). Appending them is not timed;
// records/s counts the records freed.
func BenchmarkTruncate(b *testing.B) {
	const records = 1_000

	state := make([]byte, 4096)

	for _, mode := range []string{"compact", "release"} {
		b.Run(mode, func(b *testing.B) {
			w := benchOpen(b)
			defer w.Close()

			freed := 0

			for b.Loop() {
				b.StopTimer()
				appendN(b, w, 256, 1, records, 128)

				last := w.lastDurable()

				b.StartTimer()

				var err error
				if mode == "compact" {
					err = w.Compact(state, last)
				} else {
					err = w.Release(last)
				}

				if err != nil {
					b.Fatal(err)
				}

				freed += records
			}

			reportRecords(b, freed)
		})
	}
}

// BenchmarkAppendDuringCompact measures 16 writers appending while another
// goroutine compacts every 5 ms with a state of the given size. One op is one
// record; compare records/s with BenchmarkAppend/writers=16/batch=1/size=16
// for the dip.
func BenchmarkAppendDuringCompact(b *testing.B) {
	for _, stateSize := range []int{4 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("state=%d", stateSize), func(b *testing.B) {
			w := benchOpen(b)
			defer w.Close()

			state := make([]byte, stateSize)
			stop := make(chan struct{})
			compactions := 0

			var cwg sync.WaitGroup

			cwg.Go(func() {
				tick := time.NewTicker(5 * time.Millisecond)
				defer tick.Stop()

				for {
					select {
					case <-stop:
						return
					case <-tick.C:
					}

					if err := w.Compact(state, w.lastDurable()); err != nil {
						b.Error(err)
						return
					}

					compactions++
				}
			})

			b.ResetTimer()
			appendN(b, w, 16, 1, b.N, 16)
			b.StopTimer()

			close(stop)
			cwg.Wait()

			reportRecords(b, b.N)
			b.ReportMetric(float64(compactions)/b.Elapsed().Seconds(), "compactions/s")
		})
	}
}
