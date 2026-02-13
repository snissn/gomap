package db

import (
	"bytes"
	"runtime"
	"testing"
)

func TestResolveReadWorkers(t *testing.T) {
	maxWorkers := runtime.GOMAXPROCS(0) + 1

	cases := []struct {
		name string
		raw  int
		keys int
		want int
	}{
		{name: "explicit one", raw: 1, keys: 17, want: 1},
		{name: "auto by default", raw: 0, keys: 17, want: min(maxWorkers, 17)},
		{name: "negative values use auto", raw: -1, keys: 17, want: min(maxWorkers, 17)},
		{name: "clamp to key count", raw: 32, keys: 9, want: 9},
		{name: "empty key list", raw: -1, keys: 0, want: 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := resolveReadWorkers(tc.raw, tc.keys)
			if got != tc.want {
				t.Fatalf("resolveReadWorkers(%d, %d) = %d, want %d", tc.raw, tc.keys, got, tc.want)
			}
		})
	}
}

func TestBoundedArenaCap(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cases := []struct {
		name      string
		itemCount int
		guess     int
		maxBytes  int
		want      int
	}{
		{name: "normal", itemCount: 8, guess: 128, maxBytes: 4096, want: 1024},
		{name: "clamped", itemCount: 1000, guess: 128, maxBytes: 4096, want: 4096},
		{name: "overflow saturates to cap", itemCount: maxInt/128 + 1, guess: 128, maxBytes: 4096, want: 4096},
		{name: "empty count", itemCount: 0, guess: 128, maxBytes: 4096, want: 0},
		{name: "zero max", itemCount: 8, guess: 128, maxBytes: 0, want: 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := boundedArenaCap(tc.itemCount, tc.guess, tc.maxBytes); got != tc.want {
				t.Fatalf("boundedArenaCap(%d,%d,%d)=%d want %d", tc.itemCount, tc.guess, tc.maxBytes, got, tc.want)
			}
		})
	}
}

func TestGetMany_ParallelAndSerialSemanticsMatch(t *testing.T) {
	dir := t.TempDir()

	seed, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open base DB: %v", err)
	}
	if err := seed.Set([]byte("k1"), []byte("value-1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := seed.Set([]byte("k-empty"), []byte{}); err != nil {
		t.Fatalf("set k-empty: %v", err)
	}
	if err := seed.Set([]byte("k2"), []byte("value-2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close base DB: %v", err)
	}

	keys := [][]byte{
		[]byte("k1"),
		[]byte("missing-1"),
		[]byte("k-empty"),
		[]byte("k1"),
		[]byte("missing-2"),
		[]byte("k2"),
	}

	serial, err := Open(Options{Dir: dir, ReadWorkers: 1})
	if err != nil {
		t.Fatalf("open serial DB: %v", err)
	}
	serialOut, err := serial.GetMany(keys)
	if err != nil {
		t.Fatalf("serial GetMany: %v", err)
	}
	if err := serial.Close(); err != nil {
		t.Fatalf("close serial DB: %v", err)
	}

	parallel, err := Open(Options{Dir: dir, ReadWorkers: 4})
	if err != nil {
		t.Fatalf("open parallel DB: %v", err)
	}
	defer func() {
		if err := parallel.Close(); err != nil {
			t.Fatalf("close parallel DB: %v", err)
		}
	}()

	parallelOut, err := parallel.GetMany(keys)
	if err != nil {
		t.Fatalf("parallel GetMany: %v", err)
	}

	expected := [][]byte{
		[]byte("value-1"),
		nil,
		[]byte{},
		[]byte("value-1"),
		nil,
		[]byte("value-2"),
	}

	if len(serialOut) != len(expected) || len(parallelOut) != len(expected) {
		t.Fatalf("unexpected output lengths: serial=%d parallel=%d expected=%d", len(serialOut), len(parallelOut), len(expected))
	}

	for i := range keys {
		if !bytes.Equal(serialOut[i], expected[i]) {
			t.Fatalf("serial out[%d]=%q expected=%q", i, serialOut[i], expected[i])
		}
		if !bytes.Equal(parallelOut[i], expected[i]) {
			t.Fatalf("parallel out[%d]=%q expected=%q", i, parallelOut[i], expected[i])
		}
		if !bytes.Equal(serialOut[i], parallelOut[i]) {
			t.Fatalf("serial vs parallel mismatch at %d: serial=%q parallel=%q", i, serialOut[i], parallelOut[i])
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
