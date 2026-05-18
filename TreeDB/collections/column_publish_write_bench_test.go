package collections

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkColumnStoreCommandWALRootPublicationM10B(b *testing.B) {
	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("insert/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			var totals columnStoreCommandWALBenchTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batch := makeColumnStoreCommandWALBenchBatch(b, i, commandWALBenchBatchSize, false)
				totals.add(batch, true)
				b.StartTimer()
				if _, err := collection.InsertBatch(batch.ids, batch.docs); err != nil {
					b.Fatalf("InsertBatch: %v", err)
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(b.N), uint64(b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, totals)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("update/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			seedColumnStoreCommandWALBenchBatches(b, collection, 0, b.N, commandWALBenchBatchSize)
			var totals columnStoreCommandWALBenchTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batch := makeColumnStoreCommandWALBenchBatch(b, i, commandWALBenchBatchSize, true)
				totals.add(batch, true)
				b.StartTimer()
				results, err := collection.UpdateBatch(batch.updates)
				if err != nil {
					b.Fatalf("UpdateBatch: %v", err)
				}
				if len(results) != len(batch.updates) {
					b.Fatalf("UpdateBatch results=%d, want %d", len(results), len(batch.updates))
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(2*b.N), uint64(2*b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, totals)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("delete/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			seedColumnStoreCommandWALBenchBatches(b, collection, 0, b.N, commandWALBenchBatchSize)
			var totals columnStoreCommandWALBenchTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batch := makeColumnStoreCommandWALBenchBatch(b, i, commandWALBenchBatchSize, false)
				totals.add(batch, false)
				b.StartTimer()
				deleted, err := collection.DeleteBatch(batch.ids)
				if err != nil {
					b.Fatalf("DeleteBatch: %v", err)
				}
				if deleted != len(batch.ids) {
					b.Fatalf("DeleteBatch deleted=%d, want %d", deleted, len(batch.ids))
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(2*b.N), uint64(2*b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, totals)
		})
	}
}

type columnStoreCommandWALBenchBatch struct {
	ids      [][]byte
	docs     [][]byte
	updates  []UpdateBatchItem
	idBytes  int
	docBytes int
}

type columnStoreCommandWALBenchTotals struct {
	docs  int
	bytes int
}

func (totals *columnStoreCommandWALBenchTotals) add(batch columnStoreCommandWALBenchBatch, includeDocs bool) {
	totals.docs += len(batch.ids)
	totals.bytes += batch.idBytes
	if includeDocs {
		totals.bytes += batch.docBytes
	}
}

func openColumnStoreCommandWALRootPublicationBenchmark(b *testing.B, columnStore bool) (*backenddb.DB, *Collection) {
	b.Helper()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    b.TempDir(),
		CommandWAL:             true,
		Durability:             backenddb.DurabilityDurable,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		b.Fatalf("open benchmark DB: %v", err)
	}
	b.Cleanup(func() {
		if err := backend.Close(); err != nil {
			b.Errorf("close benchmark DB: %v", err)
		}
	})
	meta := CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
		},
	}
	if columnStore {
		meta.Options.ColumnStore = testColumnStoreConfig(nil)
	}
	manager := NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&meta); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	return backend, collection
}

func makeColumnStoreCommandWALBenchBatch(b *testing.B, batchIndex, batchSize int, includeUpdates bool) columnStoreCommandWALBenchBatch {
	b.Helper()
	offset := batchIndex * batchSize
	ids, docs, idBytes, docBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 0)
	batch := columnStoreCommandWALBenchBatch{
		ids:      ids,
		docs:     docs,
		idBytes:  idBytes,
		docBytes: docBytes,
	}
	if includeUpdates {
		_, replacements, _, replacementBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 1_000_000)
		batch.updates = make([]UpdateBatchItem, batchSize)
		batch.docBytes = replacementBytes
		for j := 0; j < batchSize; j++ {
			replacement := replacements[j]
			batch.updates[j] = UpdateBatchItem{
				DocumentID: ids[j],
				Update: func([]byte) ([]byte, bool, error) {
					return replacement, true, nil
				},
			}
		}
	}
	return batch
}

func columnStoreCommandWALBenchDocuments(start, count, timeOffset int) ([][]byte, [][]byte, int, int) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	idBytes := 0
	docBytes := 0
	for i := 0; i < count; i++ {
		n := start + i
		ids[i] = []byte(fmt.Sprintf("e%09d", n))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_%d","did":"d%06d"}`, n+timeOffset, n%8, n%1024))
		idBytes += len(ids[i])
		docBytes += len(docs[i])
	}
	return ids, docs, idBytes, docBytes
}

func seedColumnStoreCommandWALBenchBatches(b *testing.B, collection *Collection, start, batches, batchSize int) {
	b.Helper()
	for i := 0; i < batches; i++ {
		batch := makeColumnStoreCommandWALBenchBatch(b, start+i, batchSize, false)
		if _, err := collection.InsertBatch(batch.ids, batch.docs); err != nil {
			b.Fatalf("seed InsertBatch: %v", err)
		}
	}
}

func assertColumnStoreCommandWALBenchState(b *testing.B, backend *backenddb.DB, collection *Collection, columnStore bool, wantGeneration, wantAppliedLSN uint64) {
	b.Helper()
	assertCommandWALBenchModeForMutations(b, backend, true, wantAppliedLSN)
	if columnStore {
		assertColumnManifestStateM10B(b, collection, wantGeneration, wantAppliedLSN)
	}
}

func reportColumnStoreCommandWALBenchMetrics(b *testing.B, totals columnStoreCommandWALBenchTotals) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(totals.docs)/elapsed, "docs/s")
	b.ReportMetric((float64(totals.bytes)/(1024*1024))/elapsed, "payload_MiB/s")
}
