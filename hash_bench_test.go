package gomap

import (
	"math/rand"
	"os"
	"testing"
)

// Benchmark the raw hash-based store (no ordering) to gauge baseline performance.
func BenchmarkHashmap_SetGet(b *testing.B) {
	benchmarkHashmapSetGet(b, 1024, 8)
}

func benchmarkHashmapSetGet(b *testing.B, numKeys int, shards int) {
	dir, err := os.MkdirTemp("", "gomap-bench-*")
	if err != nil {
		b.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	h := &HashmapDistributed{}
	if err := h.NewWithShards(dir, shards); err != nil {
		b.Fatalf("init gomap: %v", err)
	}

	keys := make([][]byte, numKeys)
	vals := make([][]byte, numKeys)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 16)
		val := make([]byte, 128)
		for j := range key {
			key[j] = byte(rng.Int())
		}
		for j := range val {
			val[j] = byte(rng.Int())
		}
		keys[i] = key
		vals[i] = val
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := i % numKeys
		if err := h.Add(keys[idx], vals[idx]); err != nil {
			b.Fatalf("add: %v", err)
		}
		if _, err := h.Get(keys[idx]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
