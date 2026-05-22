package collections

import (
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const commandWALBenchBatchSize = 128

func BenchmarkCollectionCommandWALInsertBatchByID(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, commandWAL := range []bool{false, true} {
			name := fmt.Sprintf("indexed=%t/command_wal=%t", indexed, commandWAL)
			b.Run(name, func(b *testing.B) {
				backend, collection := openCollectionCommandWALBenchmark(b, indexed, commandWAL)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ids, docs := commandWALBenchDocuments(i*commandWALBenchBatchSize, commandWALBenchBatchSize)
					if _, err := collection.InsertBatch(ids, docs); err != nil {
						b.Fatalf("InsertBatch: %v", err)
					}
				}
				b.StopTimer()
				assertCommandWALBenchMode(b, backend, commandWAL)
				b.ReportMetric(float64(b.N*commandWALBenchBatchSize)/b.Elapsed().Seconds(), "docs/s")
			})
		}
	}
}

func BenchmarkCollectionCommandWALDeleteBatchByID(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, commandWAL := range []bool{false, true} {
			name := fmt.Sprintf("indexed=%t/command_wal=%t", indexed, commandWAL)
			b.Run(name, func(b *testing.B) {
				backend, collection := openCollectionCommandWALBenchmark(b, indexed, commandWAL)
				for i := 0; i < b.N; i++ {
					ids, docs := commandWALBenchDocuments(i*commandWALBenchBatchSize, commandWALBenchBatchSize)
					if _, err := collection.InsertBatch(ids, docs); err != nil {
						b.Fatalf("seed InsertBatch: %v", err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ids, _ := commandWALBenchDocuments(i*commandWALBenchBatchSize, commandWALBenchBatchSize)
					if deleted, err := collection.DeleteBatch(ids); err != nil {
						b.Fatalf("DeleteBatch: %v", err)
					} else if deleted != commandWALBenchBatchSize {
						b.Fatalf("DeleteBatch deleted=%d, want %d", deleted, commandWALBenchBatchSize)
					}
				}
				b.StopTimer()
				assertCommandWALBenchMode(b, backend, commandWAL)
				b.ReportMetric(float64(b.N*commandWALBenchBatchSize)/b.Elapsed().Seconds(), "docs/s")
			})
		}
	}
}

func BenchmarkCollectionCommandWALUpdateBatchByID(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, commandWAL := range []bool{false, true} {
			name := fmt.Sprintf("indexed=%t/command_wal=%t", indexed, commandWAL)
			b.Run(name, func(b *testing.B) {
				backend, collection := openCollectionCommandWALBenchmark(b, indexed, commandWAL)
				for i := 0; i < b.N; i++ {
					ids, docs := commandWALBenchDocuments(i*commandWALBenchBatchSize, commandWALBenchBatchSize)
					if _, err := collection.InsertBatch(ids, docs); err != nil {
						b.Fatalf("seed InsertBatch: %v", err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					items := make([]UpdateBatchItem, commandWALBenchBatchSize)
					for j := 0; j < commandWALBenchBatchSize; j++ {
						n := i*commandWALBenchBatchSize + j
						replacement := []byte(fmt.Sprintf(`{"email":"u%09d@example.com","city":"sea","age":%d}`, n, n%120))
						items[j] = UpdateBatchItem{
							DocumentID: []byte(fmt.Sprintf("u%09d", n)),
							Update: func([]byte) ([]byte, bool, error) {
								return replacement, true, nil
							},
						}
					}
					results, err := collection.UpdateBatch(items)
					if err != nil {
						b.Fatalf("UpdateBatch: %v", err)
					}
					if len(results) != commandWALBenchBatchSize {
						b.Fatalf("UpdateBatch results=%d, want %d", len(results), commandWALBenchBatchSize)
					}
				}
				b.StopTimer()
				assertCommandWALBenchMode(b, backend, commandWAL)
				b.ReportMetric(float64(b.N*commandWALBenchBatchSize)/b.Elapsed().Seconds(), "docs/s")
			})
		}
	}
}

func BenchmarkCollectionCommandWALCreateCollection(b *testing.B) {
	for _, commandWAL := range []bool{false, true} {
		b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
			backend, manager := openCollectionCatalogCommandWALBenchmark(b, commandWAL)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := manager.CreateCollection(&CollectionMeta{
					Name: fmt.Sprintf("bench_%09d", i),
					Options: CollectionOptions{
						DocumentFormat: DocumentFormatJSON,
					},
				}); err != nil {
					b.Fatalf("CreateCollection: %v", err)
				}
			}
			b.StopTimer()
			assertCommandWALBenchModeForMutations(b, backend, commandWAL, uint64(b.N))
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "collections/s")
		})
	}
}

func BenchmarkCollectionCommandWALRejectedIndexDDL(b *testing.B) {
	backend, collection := openCollectionCommandWALBenchmark(b, false, true)
	beforeLSN := backend.State().AppliedCommandLSN
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collection.CreateIndex(IndexDefinition{Name: fmt.Sprintf("idx_%09d", i), Field: "email", ValueType: IndexValueString})
		if !errors.Is(err, backenddb.ErrCommandWALRejected) {
			b.Fatalf("CreateIndex error=%v, want ErrCommandWALRejected", err)
		}
	}
	b.StopTimer()
	if got := backend.State().AppliedCommandLSN; got != beforeLSN {
		b.Fatalf("AppliedCommandLSN after rejected DDL=%d, want %d", got, beforeLSN)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rejects/s")
}

func openCollectionCommandWALBenchmark(b *testing.B, indexed bool, commandWAL bool) (*backenddb.DB, *Collection) {
	b.Helper()
	dir := b.TempDir()
	meta := CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
		},
	}
	if indexed {
		meta.Indexes = []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}}
	}
	if commandWAL {
		setup, err := backenddb.Open(backenddb.Options{
			Dir:                    dir,
			Durability:             backenddb.DurabilityWALOffRelaxed,
			DisableBackgroundPrune: true,
		})
		if err != nil {
			b.Fatalf("open setup DB: %v", err)
		}
		if _, err := NewCollectionManager(setup).CreateCollection(&meta); err != nil {
			_ = setup.Close()
			b.Fatalf("create setup collection: %v", err)
		}
		if err := setup.Close(); err != nil {
			b.Fatalf("close setup DB: %v", err)
		}
		if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
			b.Fatalf("save command WAL format: %v", err)
		}
	} else {
		// Baseline is the current WAL-off collection path.
	}
	openOpts := backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	}
	if commandWAL {
		openOpts.Durability = backenddb.DurabilityWALOnRelaxed
	}
	backend, err := backenddb.Open(openOpts)
	if err != nil {
		b.Fatalf("open benchmark DB: %v", err)
	}
	b.Cleanup(func() {
		if err := backend.Close(); err != nil {
			b.Errorf("close benchmark DB: %v", err)
		}
	})
	manager := NewCollectionManager(backend)
	if !commandWAL {
		if _, err := manager.CreateCollection(&meta); err != nil {
			b.Fatalf("create collection: %v", err)
		}
	}
	collection, err := manager.OpenCollection("bench")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	return backend, collection
}

func openCollectionCatalogCommandWALBenchmark(b *testing.B, commandWAL bool) (*backenddb.DB, *CollectionManager) {
	b.Helper()
	openOpts := backenddb.Options{
		Dir:                    b.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	}
	if commandWAL {
		openOpts.CommandWAL = true
		openOpts.Durability = backenddb.DurabilityWALOnRelaxed
	}
	backend, err := backenddb.Open(openOpts)
	if err != nil {
		b.Fatalf("open catalog benchmark DB: %v", err)
	}
	b.Cleanup(func() {
		if err := backend.Close(); err != nil {
			b.Errorf("close catalog benchmark DB: %v", err)
		}
	})
	return backend, NewCollectionManager(backend)
}

func commandWALBenchDocuments(start int, count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		n := start + i
		ids[i] = []byte(fmt.Sprintf("u%09d", n))
		docs[i] = []byte(fmt.Sprintf(`{"email":"u%09d@example.com","city":"hnl","age":%d}`, n, n%120))
	}
	return ids, docs
}

func assertCommandWALBenchMode(b *testing.B, backend *backenddb.DB, want bool) {
	b.Helper()
	assertCommandWALBenchModeForMutations(b, backend, want, 1)
}

func assertCommandWALBenchModeForMutations(b *testing.B, backend *backenddb.DB, want bool, minAppliedLSN uint64) {
	b.Helper()
	if got := backend.CommandWALEnabled(); got != want {
		b.Fatalf("CommandWALEnabled=%t, want %t", got, want)
	}
	if want && backend.State().AppliedCommandLSN < minAppliedLSN {
		b.Fatalf("command WAL benchmark AppliedCommandLSN=%d, want at least %d", backend.State().AppliedCommandLSN, minAppliedLSN)
	}
}
