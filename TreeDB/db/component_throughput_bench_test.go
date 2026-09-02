package db

import (
	"encoding/binary"
	"os"
	"sync/atomic"
	"testing"
)

// BenchmarkComponentBTreeSetParallel isolates backend index write throughput
// (TreeDB/db Set path) without caching-layer memtable or value-log lanes.
func BenchmarkComponentBTreeSetParallel(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-index-throughput-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer d.Close()

	const (
		keySpace  = 1 << 16
		valueSize = 256
	)
	keys := make([][]byte, keySpace)
	for i := 0; i < keySpace; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		keys[i] = k
	}
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i)
	}

	var opSeq atomic.Uint64
	var failed atomic.Bool
	errCh := make(chan error, 1)

	b.ReportAllocs()
	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if failed.Load() {
				return
			}
			n := opSeq.Add(1)
			key := keys[int(n%keySpace)]
			if err := d.Set(key, value); err != nil {
				if failed.CompareAndSwap(false, true) {
					errCh <- err
				}
				return
			}
		}
	})
	if failed.Load() {
		b.Fatalf("Set failed: %v", <-errCh)
	}
}
