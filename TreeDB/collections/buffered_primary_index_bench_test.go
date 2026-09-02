package collections

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkBufferedIndexedFirstGetAfterInsert(b *testing.B) {
	const entries = 4096
	const batchSize = 256
	b.ReportAllocs()
	b.ReportMetric(entries, "pending_docs")
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{
			Name: "users",
			Options: CollectionOptions{
				BufferedIndexedWrites:            true,
				BufferedIndexedWriteMaxDocuments: entries * 2,
				BufferedIndexedWriteMaxRootRuns:  entries * 2,
				DisableBufferedIndexedAsyncFlush: true,
			},
			Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
		}); err != nil {
			_ = d.Close()
			b.Fatalf("create collection: %v", err)
		}
		col, err := mgr.OpenCollection("users")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		for start := 0; start < entries; start += batchSize {
			ids := make([][]byte, 0, batchSize)
			docs := make([][]byte, 0, batchSize)
			for j := 0; j < batchSize && start+j < entries; j++ {
				id := fmt.Sprintf("u%06d", start+j)
				ids = append(ids, []byte(id))
				docs = append(docs, []byte(fmt.Sprintf(`{"email":"%s@example.com"}`, id)))
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				_ = d.Close()
				b.Fatalf("insert batch: %v", err)
			}
		}
		lookupID := fmt.Sprintf("u%06d", entries-1)
		b.StartTimer()
		_, found, err := col.GetInto([]byte(lookupID), nil)
		b.StopTimer()
		if err != nil {
			_ = d.Close()
			b.Fatalf("GetInto: %v", err)
		}
		if !found {
			_ = d.Close()
			b.Fatal("GetInto missing final inserted document")
		}
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}
