package nativewire

import (
	"context"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkNativewireCollectionInsertBatch(b *testing.B) {
	const batchSize = 32
	b.Run("direct_collection", func(b *testing.B) {
		_, col, cleanup := benchmarkCollection(b)
		defer cleanup()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			b.StartTimer()
			if _, err := col.InsertBatch(ids, docs); err != nil {
				b.Fatalf("InsertBatch: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_inproc", func(b *testing.B) {
		mgr, _, cleanup := benchmarkCollection(b)
		defer cleanup()
		server := NewServer(ServerOptions{Collections: mgr})
		client, clientCleanup, err := NewInProcessClient(context.Background(), server)
		if err != nil {
			b.Fatalf("NewInProcessClient: %v", err)
		}
		defer func() { _ = clientCleanup() }()
		handle, err := client.OpenCollection(context.Background(), "bench")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			ids, docs := benchmarkStoredBatch(i*batchSize, batchSize)
			b.StartTimer()
			if _, err := client.InsertBatchHandle(context.Background(), handle, collections.DocumentFormatJSON, ids, docs, AckVisible); err != nil {
				b.Fatalf("InsertBatch native: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
}

func BenchmarkNativewireCollectionGetMany(b *testing.B) {
	const (
		docs      = 4096
		batchSize = 64
	)
	b.Run("direct_collection", func(b *testing.B) {
		_, col, cleanup := benchmarkCollection(b)
		defer cleanup()
		seedBenchmarkCollection(b, col, docs)
		ids, _ := benchmarkStoredBatch(0, batchSize)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, id := range ids {
				if _, err := col.Get(id); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
	b.Run("native_wire_inproc", func(b *testing.B) {
		mgr, col, cleanup := benchmarkCollection(b)
		defer cleanup()
		seedBenchmarkCollection(b, col, docs)
		server := NewServer(ServerOptions{Collections: mgr})
		client, clientCleanup, err := NewInProcessClient(context.Background(), server)
		if err != nil {
			b.Fatalf("NewInProcessClient: %v", err)
		}
		defer func() { _ = clientCleanup() }()
		handle, err := client.OpenCollection(context.Background(), "bench")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		ids, _ := benchmarkStoredBatch(0, batchSize)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := client.GetManyHandle(context.Background(), handle, ids); err != nil {
				b.Fatalf("GetMany native: %v", err)
			}
		}
		b.ReportMetric(float64(b.N*batchSize), "docs_total")
	})
}

func benchmarkCollection(tb testing.TB) (*collections.CollectionManager, *collections.Collection, func()) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "bench",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
	}); err != nil {
		_ = db.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bench")
	if err != nil {
		_ = db.Close()
		tb.Fatalf("open collection: %v", err)
	}
	return mgr, col, func() { _ = db.Close() }
}

func seedBenchmarkCollection(tb testing.TB, col *collections.Collection, docs int) {
	tb.Helper()
	const batchSize = 256
	for start := 0; start < docs; start += batchSize {
		n := batchSize
		if docs-start < n {
			n = docs - start
		}
		ids, values := benchmarkStoredBatch(start, n)
		if _, err := col.InsertBatch(ids, values); err != nil {
			tb.Fatalf("seed InsertBatch: %v", err)
		}
	}
	if err := col.Flush(); err != nil {
		tb.Fatalf("seed Flush: %v", err)
	}
}

func benchmarkStoredBatch(start, count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("doc-%08d", start+i)
		ids[i] = []byte(id)
		docs[i] = []byte(fmt.Sprintf(`{"email":"%s@example.com","city":"hnl","age":%d}`, id, 18+((start+i)%67)))
	}
	return ids, docs
}
