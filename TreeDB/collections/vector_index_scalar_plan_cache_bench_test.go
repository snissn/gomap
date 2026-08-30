package collections

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// BenchmarkNativeScalarPlanCache4477 intentionally uses only predecessor APIs.
// The file can be copied unchanged to baeb442d8 for an identical comparison;
// reflection reports zero for cache counters that do not exist there.
func BenchmarkNativeScalarPlanCache4477(b *testing.B) {
	d, col, def := newNativeScalarTestCollection(b, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	const tenants = 128
	ids := make([][]byte, tenants)
	documents := make([][]byte, tenants)
	for i := range tenants {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		documents[i] = []byte(fmt.Sprintf(`{"embedding":[1,0],"tenant":"tenant-%03d"}`, i))
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		b.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatal(err)
	}
	warmFilter := &HybridScalarFilter{IndexName: "tenant_idx", Value: "tenant-000"}
	coldFilters := make([]*HybridScalarFilter, tenants)
	for i := range tenants {
		coldFilters[i] = &HybridScalarFilter{IndexName: "tenant_idx", Value: fmt.Sprintf("cold-miss-%03d", i)}
	}
	search := func(tb testing.TB, buffer *VectorIndexSearchBuffer, filter *HybridScalarFilter) VectorIndexSearchStats {
		tb.Helper()
		response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
			IndexName: def.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 64,
			StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: filter,
		}, buffer)
		if err != nil {
			tb.Fatal(err)
		}
		return response.Stats
	}

	b.Run("warm_repeated", func(b *testing.B) {
		var buffer VectorIndexSearchBuffer
		_ = search(b, &buffer, warmFilter)
		var hits, misses, count uint64
		var last VectorIndexSearchStats
		b.ReportAllocs()
		b.ResetTimer()
		started := time.Now()
		for b.Loop() {
			last = search(b, &buffer, warmFilter)
			hits += scalarPlanCacheBenchmarkField4477(last, "ScalarFilterPlanCacheHits")
			misses += scalarPlanCacheBenchmarkField4477(last, "ScalarFilterPlanCacheMisses")
			count++
		}
		elapsed := time.Since(started)
		b.StopTimer()
		reportNativeScalarPlanCacheBenchmark4477(b, count, elapsed, hits, misses, last)
	})

	b.Run("cold_unique_over_capacity", func(b *testing.B) {
		var buffer VectorIndexSearchBuffer
		var hits, misses, count uint64
		var last VectorIndexSearchStats
		b.ReportAllocs()
		b.ResetTimer()
		started := time.Now()
		for b.Loop() {
			last = search(b, &buffer, coldFilters[count%tenants])
			hits += scalarPlanCacheBenchmarkField4477(last, "ScalarFilterPlanCacheHits")
			misses += scalarPlanCacheBenchmarkField4477(last, "ScalarFilterPlanCacheMisses")
			count++
		}
		elapsed := time.Since(started)
		b.StopTimer()
		reportNativeScalarPlanCacheBenchmark4477(b, count, elapsed, hits, misses, last)
	})
}

func scalarPlanCacheBenchmarkField4477(stats VectorIndexSearchStats, name string) uint64 {
	field := reflect.ValueOf(stats).FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.Uint64 {
		return 0
	}
	return field.Uint()
}

func reportNativeScalarPlanCacheBenchmark4477(b *testing.B, count uint64, elapsed time.Duration, hits, misses uint64, last VectorIndexSearchStats) {
	b.Helper()
	if count != 0 {
		b.ReportMetric(100*float64(hits)/float64(count), "cache_hit_%")
		b.ReportMetric(100*float64(misses)/float64(count), "cache_miss_%")
	}
	if elapsed > 0 {
		b.ReportMetric(float64(count)/elapsed.Seconds(), "ops/s")
		b.ReportMetric(float64(count)/elapsed.Seconds(), "QPS")
	}
	b.ReportMetric(float64(scalarPlanCacheBenchmarkField4477(last, "ScalarFilterPlanCacheEntries")), "cache_entries")
	b.ReportMetric(float64(scalarPlanCacheBenchmarkField4477(last, "ScalarFilterPlanCacheRetainedBytes")), "cache_retained_B")
}
