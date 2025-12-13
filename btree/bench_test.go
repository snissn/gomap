package btree

import (
	"encoding/binary"
	"math/rand"
	"os"
	"testing"

	"github.com/snissn/gomap"
)

var benchSinkUint64 uint64

func BenchmarkBTree_PutGet(b *testing.B) {
	benchmarkBTreePutGet(b, 1024, 8)
}

func BenchmarkBTree_PutOnly(b *testing.B) {
	benchmarkBTreePutOnly(b, 2048, 8)
}

func BenchmarkBTree_GetOnly(b *testing.B) {
	benchmarkBTreeGetOnly(b, 2048, 8)
}

func BenchmarkBTree_ScanAll(b *testing.B) {
	benchmarkBTreeScanAll(b, 2048, 8)
}

func BenchmarkBTree_RangeSpan100(b *testing.B) {
	const prefill = 50000
	const span = 100

	dir, err := os.MkdirTemp("", "btree-range-bench-*")
	if err != nil {
		b.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &gomap.HashmapDistributed{}
	if err := store.NewWithShards(dir, 8); err != nil {
		b.Fatalf("init gomap: %v", err)
	}
	tree, err := NewTreeOnGomap(store, "bench-range")
	if err != nil {
		b.Fatalf("init tree: %v", err)
	}

	val := make([]byte, 150)
	for i := range val {
		val[i] = byte(i)
	}
	for i := 0; i < prefill; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := tree.Put(key[:], val); err != nil {
			b.Fatalf("preload put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startIdx := i % (prefill - span)
		endIdx := startIdx + span

		var startKey [8]byte
		var endKey [8]byte
		binary.BigEndian.PutUint64(startKey[:], uint64(startIdx))
		binary.BigEndian.PutUint64(endKey[:], uint64(endIdx))

		it, err := tree.Range(startKey[:], endKey[:])
		if err != nil {
			b.Fatalf("range: %v", err)
		}
		for it.Valid() {
			benchSinkUint64 += uint64(len(it.Key()))
			it.Next()
		}
		if it.Error() != nil {
			b.Fatalf("iterator error: %v", it.Error())
		}
		it.Close()
	}
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

	keys := make([][]byte, numKeys)
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
		keys[i] = key
		if err := tree.Put(key, val); err != nil {
			b.Fatalf("put preload: %v", err)
		}
	}

	b.ReportAllocs()
	// Warm cache
	for i := 0; i < numKeys; i++ {
		if _, err := tree.Get(keys[i]); err != nil {
			b.Fatalf("warm get: %v", err)
		}
	}
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

func benchmarkBTreePutOnly(b *testing.B, numKeys int, shards int) {
	dir, err := os.MkdirTemp("", "btree-putonly-bench-*")
	if err != nil {
		b.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &gomap.HashmapDistributed{}
	if err := store.NewWithShards(dir, shards); err != nil {
		b.Fatalf("init gomap: %v", err)
	}
	tree, err := NewTreeOnGomap(store, "bench-putonly")
	if err != nil {
		b.Fatalf("init tree: %v", err)
	}

	keys := make([][]byte, numKeys)
	vals := make([][]byte, numKeys)
	rng := rand.New(rand.NewSource(4))
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
		if err := tree.Put(key, val); err != nil {
			b.Fatalf("preload: %v", err)
		}
	}

	b.ReportAllocs()
	// Warm cache
	for i := 0; i < numKeys; i++ {
		if _, err := tree.Get(keys[i]); err != nil {
			b.Fatalf("warm get: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numKeys
		if err := tree.Put(keys[idx], vals[idx]); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func benchmarkBTreeGetOnly(b *testing.B, numKeys int, shards int) {
	dir, err := os.MkdirTemp("", "btree-getonly-bench-*")
	if err != nil {
		b.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &gomap.HashmapDistributed{}
	if err := store.NewWithShards(dir, shards); err != nil {
		b.Fatalf("init gomap: %v", err)
	}
	tree, err := NewTreeOnGomap(store, "bench-getonly")
	if err != nil {
		b.Fatalf("init tree: %v", err)
	}

	keys := make([][]byte, numKeys)
	vals := make([][]byte, numKeys)
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 16)
		val := make([]byte, 64)
		for j := range key {
			key[j] = byte(rng.Int())
		}
		for j := range val {
			val[j] = byte(rng.Int())
		}
		keys[i] = key
		vals[i] = val
		if err := tree.Put(key, val); err != nil {
			b.Fatalf("preload put: %v", err)
		}
	}

	// Warm cache
	for i := 0; i < numKeys; i++ {
		if _, err := tree.Get(keys[i]); err != nil {
			b.Fatalf("warm get: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numKeys
		if _, err := tree.Get(keys[idx]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
