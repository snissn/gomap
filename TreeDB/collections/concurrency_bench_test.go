package collections_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func benchmarkReportCollectionConcurrency(b *testing.B, kind string) {
	b.Helper()
	b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
	b.ReportMetric(2, "indexes/doc")
	if kind != "" {
		// Keep the benchmark name as the primary machine-readable label. The string
		// parameter exists so callers are explicit about the concurrency shape they
		// are reporting when this helper grows more counters.
		_ = kind
	}
}

func BenchmarkCollectionConcurrencyReadPrimaryParallel(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_concurrency_read_primary", collectionShapeIndexes(2)...)
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	benchmarkReportCollectionConcurrency(b, "primary_read_parallel")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		stride := runtime.GOMAXPROCS(0)
		if stride <= 0 {
			stride = 1
		}
		for pb.Next() {
			if _, err := collection.Get(ids[i%len(ids)]); err != nil {
				b.Errorf("concurrent primary read: %v", err)
			}
			i += stride
		}
	})
}

func BenchmarkCollectionConcurrencyReadPrimaryIntoParallel(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_concurrency_read_primary_into", collectionShapeIndexes(2)...)
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	benchmarkReportCollectionConcurrency(b, "primary_read_into_parallel")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		stride := runtime.GOMAXPROCS(0)
		if stride <= 0 {
			stride = 1
		}
		buf := make([]byte, 0, 8<<10)
		for pb.Next() {
			got, found, err := collection.GetInto(ids[i%len(ids)], buf)
			if err != nil {
				b.Errorf("concurrent primary read into: %v", err)
			}
			if !found {
				b.Errorf("concurrent primary read into: document not found")
			}
			buf = got
			i += stride
		}
	})
}

func BenchmarkCollectionConcurrencySecondaryLookupUniqueParallel(b *testing.B) {
	backend, collection := openBenchmarkCollection(
		b,
		"bench_concurrency_secondary_unique",
		collections.IndexDefinition{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
	)
	seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	benchmarkReportCollectionConcurrency(b, "secondary_unique_parallel")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		stride := runtime.GOMAXPROCS(0)
		if stride <= 0 {
			stride = 1
		}
		for pb.Next() {
			email := fmt.Sprintf("user-%09d@example.com", i%collectionBenchSeedDocs)
			if _, err := collection.FindByIndex("email_idx", email); err != nil {
				b.Errorf("concurrent unique secondary lookup: %v", err)
			}
			i += stride
		}
	})
}

func BenchmarkCollectionConcurrencySecondaryLookupNonUniqueParallel(b *testing.B) {
	backend, collection := openBenchmarkCollection(
		b,
		"bench_concurrency_secondary_nonunique",
		collections.IndexDefinition{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
	)
	seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	benchmarkReportCollectionConcurrency(b, "secondary_nonunique_parallel")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		stride := runtime.GOMAXPROCS(0)
		if stride <= 0 {
			stride = 1
		}
		for pb.Next() {
			city := fmt.Sprintf("city-%02d", i%collectionBenchCities)
			if _, err := collection.FindByIndex("city_idx", city); err != nil {
				b.Errorf("concurrent nonunique secondary lookup: %v", err)
			}
			i += stride
		}
	})
}
