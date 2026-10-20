package treedb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// New-feature measurements: the pre-feature native factory refuses this schema.
// Run with a fixed small iteration count (e.g. -benchtime=8x): each CRUD sample
// gets a fresh cohort so tombstones/high-water cannot grow across samples.
// Setup, Checkpoint, activation checks and Close are untimed. Search response
// path/result validation remains inside each timed search iteration.
func BenchmarkNativeDocumentVectorPublic(b *testing.B) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		for _, size := range []int{32, 128} {
			for _, operation := range []string{"Insert", "Update", "Delete", "SearchCurrent", "SearchHeld"} {
				b.Run(fmt.Sprintf("%s/N%d/%s", format, size, operation), func(b *testing.B) {
					b.StopTimer()
					b.ReportAllocs()
					if operation == "SearchCurrent" || operation == "SearchHeld" {
						database, _, col, closeDB := openNativeDocumentVectorBench(b, format, size)
						defer closeDB()
						var held *collections.VectorIndexSearcher
						if operation == "SearchHeld" {
							var err error
							held, err = col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: "embedding"})
							if err != nil {
								b.Fatal(err)
							}
							defer func() {
								if err := held.Close(); err != nil {
									b.Error(err)
								}
							}()
						}
						var buffer collections.VectorIndexSearchBuffer
						currentOpts := collections.VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, EfSearch: 16, StatsMode: collections.VectorIndexSearchStatsModeMinimal}
						heldOpts := collections.VectorIndexSearcherSearchOptions{Query: currentOpts.Query, TopK: 1, EfSearch: 16, StatsMode: currentOpts.StatsMode}
						search := func() (collections.VectorIndexSearchResponse, error) {
							if held != nil {
								return held.SearchWithBuffer(heldOpts, &buffer)
							}
							return col.SearchVectorIndexWithBuffer(currentOpts, &buffer)
						}
						// Warm caller-owned output backing, not an unrelated graph cache.
						if response, err := search(); err != nil {
							b.Fatal(err)
						} else {
							requireNativeDocumentBenchHit(b, response)
						}
						before := database.cached.COWMemoryStats()
						b.ResetTimer()
						b.StartTimer()
						for i := 0; i < b.N; i++ {
							response, err := search()
							if err != nil {
								b.Fatal(err)
							}
							requireNativeDocumentBenchHit(b, response)
						}
						b.StopTimer()
						after := database.cached.COWMemoryStats()
						b.ReportMetric(float64(after.TotalBytes)-float64(before.TotalBytes), "cow_retained_delta_B")
					} else {
						var retainedDelta float64
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							database, backend, col, closeDB := openNativeDocumentVectorBench(b, format, size)
							id := []byte(fmt.Sprintf("doc-%04d", size/2))
							if operation == "Insert" {
								id = []byte("new-document")
							}
							document := nativeDocumentBenchDocument(b, format, 0, 1)
							next := backend.CommandWALNextLSN()
							before := database.cached.COWMemoryStats()
							b.StartTimer()
							switch operation {
							case "Insert":
								if _, err := col.Insert(id, document); err != nil {
									b.Fatal(err)
								}
							case "Update":
								if _, changed, err := col.Update(id, func([]byte) ([]byte, bool, error) { return document, true, nil }); err != nil || !changed {
									b.Fatalf("update=%v: %v", changed, err)
								}
							case "Delete":
								if deleted, err := col.DeleteDocument(id); err != nil || !deleted {
									b.Fatalf("delete=%v: %v", deleted, err)
								}
							}
							b.StopTimer()
							retainedDelta += float64(database.cached.COWMemoryStats().TotalBytes) - float64(before.TotalBytes)
							if backend.CommandWALNextLSN() != next+1 {
								b.Fatal("durable CRUD did not assign exactly one original frame")
							}
							got, err := col.Get(id)
							if err != nil {
								b.Fatal(err)
							}
							if operation == "Delete" {
								if got != nil {
									b.Fatal("deleted document remains")
								}
							} else if !bytes.Equal(got, document) {
								b.Fatalf("CRUD stored %x want %x", got, document)
							}
							closeDB()
						}
						b.ReportMetric(retainedDelta/float64(b.N), "cow_retained_delta_B/op")
					}
					b.ReportMetric(float64(size), "seed_docs")
				})
			}
		}
	}
}

func nativeDocumentBenchDocument(b *testing.B, format collections.DocumentFormat, x, y float64) []byte {
	b.Helper()
	if format == collections.DocumentFormatJSON {
		return []byte(fmt.Sprintf(`{"embedding":[%g,%g]}`, x, y))
	}
	raw, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{x, y}}})
	if err != nil {
		b.Fatal(err)
	}
	return raw
}

func requireNativeDocumentBenchHit(b *testing.B, response collections.VectorIndexSearchResponse) {
	b.Helper()
	if response.Path != collections.VectorIndexSearchPathNativeRuntime || len(response.Results) != 1 || string(response.Results[0].ID) != "doc-0000" {
		b.Fatalf("actual native current/held search route/result=%+v", response)
	}
}

func openNativeDocumentVectorBench(b *testing.B, format collections.DocumentFormat, size int) (*DB, *backenddb.DB, *collections.Collection, func()) {
	b.Helper()
	opts := indexedSliceOptions(ProfileCommandWALDurable, b.TempDir())
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	return openNativeDocumentVectorBenchWithOptions(b, format, size, opts, true)
}

// Both measurements use the same actual native factory and final owner/pin
// oracles. seed=false reopens the already checkpointed cohort without writing.
func openNativeDocumentVectorBenchWithOptions(b *testing.B, format collections.DocumentFormat, size int, opts Options, seed bool) (*DB, *backenddb.DB, *collections.Collection, func()) {
	b.Helper()
	database, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		b.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	closed := false
	closeDB := func() {
		if closed {
			return
		}
		closed = true
		if err := manager.Close(); err != nil {
			b.Error(err)
		}
		cache, pins := database.cached, backend.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			b.Error(err)
		}
		if s := cache.COWMemoryStats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
			b.Errorf("final native owner resources=%+v", s)
		}
		if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
			b.Errorf("final native pins=%+v", s)
		}
	}
	b.Cleanup(closeDB)
	if database.ResolvedProfile() != ProfileCommandWALDurable {
		b.Fatal("benchmark is not CommandWAL durable")
	}
	def := collections.VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 4, Strategy: collections.VectorIndexStrategyNativeRuntime}
	// This actual native factory/schema selects metadata8 and coverage5. An
	// unmarked legacy manager cannot silently substitute for the native backend.
	if seed {
		if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: format}, VectorIndexes: []collections.VectorIndexDefinition{def}}); err != nil {
			b.Fatal(err)
		}
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		b.Fatal(err)
	}
	if seed {
		ids, docs := make([][]byte, size), make([][]byte, size)
		for i := 0; i < size; i++ {
			ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
			docs[i] = nativeDocumentBenchDocument(b, format, 1, float64(i)/float64(size))
		}
		if result, err := col.InsertBatch(ids, docs); err != nil || len(result) != size {
			b.Fatalf("seed=%d: %v", len(result), err)
		}
		if err := database.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}

	var buffer collections.VectorIndexSearchBuffer
	response, err := col.SearchVectorIndexWithBuffer(collections.VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, EfSearch: 16, StatsMode: collections.VectorIndexSearchStatsModeMinimal}, &buffer)
	if err != nil {
		b.Fatal(err)
	}
	requireNativeDocumentBenchHit(b, response)
	return database, backend, col, closeDB
}

// This read-only reopened path imports a separate private graph descriptor per
// one-shot capture. Opening a held searcher validates its own retained cut once;
// its open/Close are excluded, while per-iteration public result checks are timed.
func BenchmarkNativeDocumentVectorReopenedSearch(b *testing.B) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		for _, size := range []int{32, 128} {
			for _, held := range []bool{false, true} {
				operation := "SearchCurrent"
				if held {
					operation = "SearchHeld"
				}
				b.Run(fmt.Sprintf("%s/N%d/%s", format, size, operation), func(b *testing.B) {
					b.StopTimer()
					b.ReportAllocs()
					opts := indexedSliceOptions(ProfileCommandWALDurable, b.TempDir())
					opts.ValueLog.Generational.Policy = ValueLogGenerationOff
					_, _, _, closeSeed := openNativeDocumentVectorBenchWithOptions(b, format, size, opts, true)
					closeSeed()
					database, backend, col, closeDB := openNativeDocumentVectorBenchWithOptions(b, format, size, opts, false)
					defer closeDB()
					next, applied := backend.CommandWALNextLSN(), backend.State().AppliedCommandLSN
					var searcher *collections.VectorIndexSearcher
					if held {
						var err error
						searcher, err = col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: "embedding"})
						if err != nil {
							b.Fatal(err)
						}
						defer func() {
							if err := searcher.Close(); err != nil {
								b.Error(err)
							}
						}()
					}
					var buffer collections.VectorIndexSearchBuffer
					current := collections.VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, EfSearch: 16, StatsMode: collections.VectorIndexSearchStatsModeMinimal}
					search := func() (collections.VectorIndexSearchResponse, error) {
						if searcher != nil {
							return searcher.SearchWithBuffer(collections.VectorIndexSearcherSearchOptions{Query: current.Query, TopK: current.TopK, EfSearch: current.EfSearch, StatsMode: current.StatsMode}, &buffer)
						}
						return col.SearchVectorIndexWithBuffer(current, &buffer)
					}
					if result, err := search(); err != nil {
						b.Fatal(err)
					} else {
						requireNativeDocumentBenchHit(b, result)
					}
					before := database.cached.COWMemoryStats()
					b.ResetTimer()
					b.StartTimer()
					for i := 0; i < b.N; i++ {
						result, err := search()
						if err != nil {
							b.Fatal(err)
						}
						requireNativeDocumentBenchHit(b, result)
					}
					b.StopTimer()
					if backend.CommandWALNextLSN() != next || backend.State().AppliedCommandLSN != applied {
						b.Fatal("read-only search changed command frontier")
					}
					b.ReportMetric(float64(database.cached.COWMemoryStats().TotalBytes)-float64(before.TotalBytes), "cow_retained_delta_B")
					b.ReportMetric(float64(size), "seed_docs")
				})
			}
		}
	}
}
