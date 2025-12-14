package db

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// BenchmarkStress performs mixed Read/Write operations.
func BenchmarkStress(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		Dir:        tmpDir,
		KeepRecent: 10000,
	}
	d, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer d.Close()

	keyRange := 10000
	valSize := 100

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		valBuf := make([]byte, valSize)

		for pb.Next() {
			op := r.Intn(100)
			k := r.Intn(keyRange)
			key := []byte(fmt.Sprintf("key-%09d", k))

			if op < 50 { // 50% Write
				r.Read(valBuf)
				if err := d.Set(key, valBuf); err != nil {
					b.Errorf("Set failed: %v", err)
				}
			} else if op < 90 { // 40% Read
				if _, err := d.Get(key); err != nil {
				}
			} else { // 10% Delete
				if err := d.Delete(key); err != nil {
					b.Errorf("Delete failed: %v", err)
				}
			}
		}
	})
}

// BenchmarkGet performs 100% Read operations on a pre-filled DB.
func BenchmarkGet(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-get-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	// Pre-fill 10k items
	count := 10000
	val := make([]byte, 100)
	for i := 0; i < count; i += 1000 {
		batch := d.NewBatch()
		for j := 0; j < 1000 && i+j < count; j++ {
			k := []byte(fmt.Sprintf("key-%09d", i+j))
			if err := batch.Set(k, val); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := r.Intn(count)
			key := []byte(fmt.Sprintf("key-%09d", k))
			if _, err := d.Get(key); err != nil {
				// b.Errorf("Get failed: %v", err)
			}
		}
	})
}

// BenchmarkScan iterates over all keys.
func BenchmarkScan(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-scan-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	// Pre-fill 10k items
	count := 10000
	val := make([]byte, 100)
	for i := 0; i < count; i += 1000 {
		batch := d.NewBatch()
		for j := 0; j < 1000 && i+j < count; j++ {
			k := []byte(fmt.Sprintf("key-%09d", i+j))
			if err := batch.Set(k, val); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := d.Iterator(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		items := 0
		for ; iter.Valid(); iter.Next() {
			items++
		}
		iter.Close()
		if items != count {
			b.Fatalf("Expected %d items, got %d", count, items)
		}
	}
}

// BenchmarkBatch performs batched writes (1000 items/batch).
func BenchmarkBatch(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-batch-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		Dir:        tmpDir,
		KeepRecent: 10000,
	}
	d, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer d.Close()

	batchSize := 1000
	valSize := 100
	totalKeys := 100000

	keys := make([][]byte, totalKeys)
	for i := 0; i < totalKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	valBuf := make([]byte, valSize)
	rand.Read(valBuf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := d.NewBatch()
		startIdx := (i * batchSize) % (totalKeys - batchSize)
		for j := 0; j < batchSize; j++ {
			if j%10 == 0 {
				batch.Delete(keys[startIdx+j])
			} else {
				batch.Set(keys[startIdx+j], valBuf)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatalf("Batch write failed: %v", err)
		}
	}
}

// BenchmarkLargeVal writes larger values (4KB).
func BenchmarkLargeVal(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	valSize := 4096 // 4KB
	valBuf := make([]byte, valSize)
	rand.Read(valBuf)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := r.Intn(10000)
			key := []byte(fmt.Sprintf("key-%09d", k))
			if err := d.Set(key, valBuf); err != nil {
				b.Errorf("Set failed: %v", err)
			}
		}
	})
}
