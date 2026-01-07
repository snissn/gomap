package caching

import (
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkRepeatedIterator(b *testing.B) {
	dir, err := os.MkdirTemp("", "treedb-bench-iter-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	// Default flush threshold
	cached, err := Open(dir, backend, Options{FlushThreshold: 64 * 1024 * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer cached.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := cached.Iterator(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		it.Close()
	}
}

func BenchmarkBatchRandom(b *testing.B) {
	dir, err := os.MkdirTemp("", "treedb-bench-batch-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		FlushThreshold: 64 * 1024 * 1024,
		DisableWAL:     false,
		AllowUnsafe:    true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer cached.Close()

	batchSize := 1000
	val := make([]byte, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := cached.NewBatch()
		for j := 0; j < batchSize; j++ {
			batch.Set([]byte(fmt.Sprintf("k-%d", j)), val)
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "ops/sec")
}
