package btree

import (
	"math/rand"
	"os"
	"testing"

	"github.com/snissn/gomap"
)

func BenchmarkBTree_PutGet(b *testing.B) {
	benchmarkBTreePutGet(b, 1024, 8)
}

func BenchmarkBTree_ScanAll(b *testing.B) {
	benchmarkBTreeScanAll(b, 2048, 8)
}

func benchmarkBTreePutGet(b *testing.B, numKeys int, shards int) {
	dir, err := os.MkdirTemp("", "btree-bench-*")
	if err != nil {
		b.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &gomap.HashmapDistributed{}
	if err := store.NewWithShards(dir, shards); err != nil {
		b.Fatalf("init gomap: %v", err)
	}
	tree, err := NewTreeOnGomap(store, "bench")
	if err != nil {
		b.Fatalf("init tree: %v", err)
	}

	keys := make([][]byte, numKeys)
	vals := make([][]byte, numKeys)
	rng := rand.New(rand.NewSource(2))
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
		if err := tree.Put(keys[idx], vals[idx]); err != nil {
			b.Fatalf("put: %v", err)
		}
		if _, err := tree.Get(keys[idx]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func benchmarkBTreeScanAll(b *testing.B, numKeys int, shards int) {
	dir, err := os.MkdirTemp("", "btree-scan-bench-*")
	if err != nil {
		b.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &gomap.HashmapDistributed{}
	if err := store.NewWithShards(dir, shards); err != nil {
		b.Fatalf("init gomap: %v", err)
	}
	tree, err := NewTreeOnGomap(store, "bench-scan")
	if err != nil {
		b.Fatalf("init tree: %v", err)
	}

	rng := rand.New(rand.NewSource(3))
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 16)
		val := make([]byte, 64)
		for j := range key {
			key[j] = byte(rng.Int())
		}
		for j := range val {
			val[j] = byte(rng.Int())
		}
		if err := tree.Put(key, val); err != nil {
			b.Fatalf("put preload: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := tree.ScanAll()
		if err != nil {
			b.Fatalf("scanall: %v", err)
		}
		for it.Valid() {
			it.Next()
		}
		it.Close()
	}
}
