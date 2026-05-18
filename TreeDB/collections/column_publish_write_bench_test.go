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
			batches := makeColumnStoreCommandWALBenchBatches(b, 0, b.N, commandWALBenchBatchSize, false)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := collection.InsertBatch(batches[i].ids, batches[i].docs); err != nil {
					b.Fatalf("InsertBatch: %v", err)
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(b.N), uint64(b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, batches, true)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("update/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			batches := makeColumnStoreCommandWALBenchBatches(b, 0, b.N, commandWALBenchBatchSize, true)
			seedColumnStoreCommandWALBenchBatches(b, collection, batches)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := collection.UpdateBatch(batches[i].updates)
				if err != nil {
					b.Fatalf("UpdateBatch: %v", err)
				}
				if len(results) != len(batches[i].updates) {
					b.Fatalf("UpdateBatch results=%d, want %d", len(results), len(batches[i].updates))
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(2*b.N), uint64(2*b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, batches, true)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("delete/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			batches := makeColumnStoreCommandWALBenchBatches(b, 0, b.N, commandWALBenchBatchSize, false)
			seedColumnStoreCommandWALBenchBatches(b, collection, batches)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				deleted, err := collection.DeleteBatch(batches[i].ids)
				if err != nil {
					b.Fatalf("DeleteBatch: %v", err)
				}
				if deleted != len(batches[i].ids) {
					b.Fatalf("DeleteBatch deleted=%d, want %d", deleted, len(batches[i].ids))
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(2*b.N), uint64(2*b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, batches, false)
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

func makeColumnStoreCommandWALBenchBatches(b *testing.B, start, batches, batchSize int, includeUpdates bool) []columnStoreCommandWALBenchBatch {
	b.Helper()
	out := make([]columnStoreCommandWALBenchBatch, batches)
	for i := 0; i < batches; i++ {
		offset := start + i*batchSize
		ids, docs, idBytes, docBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 0)
		out[i] = columnStoreCommandWALBenchBatch{
			ids:      ids,
			docs:     docs,
			idBytes:  idBytes,
			docBytes: docBytes,
		}
		if includeUpdates {
			_, replacements, _, replacementBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 1_000_000)
			out[i].updates = make([]UpdateBatchItem, batchSize)
			out[i].docBytes = replacementBytes
			for j := 0; j < batchSize; j++ {
				replacement := replacements[j]
				out[i].updates[j] = UpdateBatchItem{
					DocumentID: ids[j],
					Update: func([]byte) ([]byte, bool, error) {
						return replacement, true, nil
					},
				}
			}
		}
	}
	return out
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

func seedColumnStoreCommandWALBenchBatches(b *testing.B, collection *Collection, batches []columnStoreCommandWALBenchBatch) {
	b.Helper()
	for i := range batches {
		if _, err := collection.InsertBatch(batches[i].ids, batches[i].docs); err != nil {
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

func reportColumnStoreCommandWALBenchMetrics(b *testing.B, batches []columnStoreCommandWALBenchBatch, includeDocs bool) {
	b.Helper()
	docs := 0
	bytes := 0
	for i := range batches {
		docs += len(batches[i].ids)
		bytes += batches[i].idBytes
		if includeDocs {
			bytes += batches[i].docBytes
		}
	}
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(docs)/elapsed, "docs/s")
	b.ReportMetric((float64(bytes)/(1024*1024))/elapsed, "payload_MiB/s")
}
