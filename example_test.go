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

package tinywal_test

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/johnknl/tinywal"
)

// tempDir returns a fresh directory and a function that removes it.
func tempDir() (dir string, cleanup func()) {
	dir, err := os.MkdirTemp("", "tinywal")
	if err != nil {
		log.Fatal(err)
	}

	return dir, func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Print(err)
		}
	}
}

// The usual cycle: recover the state from the snapshot and the records after
// it, append, and compact from time to time.
func Example() {
	dir, cleanup := tempDir()
	defer cleanup()

	// First run: append some records and stop without compacting.
	w, err := tinywal.Open(dir, 1<<20)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = w.Append([][]byte{[]byte("set a=1"), []byte("set b=2")}); err != nil {
		log.Fatal(err)
	}

	if err = w.Close(); err != nil {
		log.Fatal(err)
	}

	// Second run: rebuild the state, then fold it into a snapshot.
	w, err = tinywal.Open(dir, 1<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	state, last := w.Snapshot()
	commands := []string{}

	if len(state) > 0 {
		commands = strings.Split(string(state), ";")
	}

	if err = w.Replay(func(index uint64, data []byte) error {
		commands = append(commands, string(data)) // data is only valid here: copy it
		last = index

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("recovered:", commands)

	if err = w.Compact([]byte(strings.Join(commands, ";")), last); err != nil {
		log.Fatal(err)
	}

	state, index := w.Snapshot()
	fmt.Printf("snapshot at %d: %s\n", index, state)

	// Output:
	// recovered: [set a=1 set b=2]
	// snapshot at 2: set a=1;set b=2
}

// Append takes a batch; its records get consecutive indexes and share one
// sync.
func ExampleWAL_Append() {
	dir, cleanup := tempDir()
	defer cleanup()

	w, err := tinywal.Open(dir, 1<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	first, err := w.Append([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		log.Fatal(err)
	}

	next, err := w.Append([][]byte{[]byte("d")})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(first, next)

	// Output:
	// 1 4
}

// When the ring is full, Append returns ErrFull until Compact or Release
// frees space. Release suits callers that keep their state elsewhere.
func ExampleWAL_Release() {
	dir, cleanup := tempDir()
	defer cleanup()

	w, err := tinywal.Open(dir, tinywal.MinCapacity)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	record := [][]byte{make([]byte, 1000)}

	var last uint64

	for {
		index, appendErr := w.Append(record)
		if errors.Is(appendErr, tinywal.ErrFull) {
			break
		}

		if appendErr != nil {
			log.Fatal(appendErr)
		}

		last = index
	}

	fmt.Println("full after", last, "records")

	// The records up to last are applied and durable elsewhere: free them.
	if err = w.Release(last); err != nil {
		log.Fatal(err)
	}

	index, err := w.Append(record)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("appended", index)

	// Output:
	// full after 3 records
	// appended 4
}
