package documentservice

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkServiceUpsertDocumentsFreshBatch measures the service path for an
// all-new batch. Unique IDs force each iteration through the batch insert path.
func BenchmarkServiceUpsertDocumentsFreshBatch(b *testing.B) {
	svc, db := newTestService(b)
	defer db.Close()
	if _, err := svc.CreateIndex(context.Background(), CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		b.Fatal(err)
	}
	const batchSize = 1000
	docs := make([]Document, batchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := range docs {
			docs[i] = Document{ID: fmt.Sprintf("%08d-%04d", n, i), Embedding: []float32{1, 0}}
		}
		if _, err := svc.UpsertDocuments(context.Background(), "docs", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
			b.Fatal(err)
		}
	}
}
