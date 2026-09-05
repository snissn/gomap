package collections

import (
	"fmt"
	"runtime"
	"testing"
)

// Fixture creation, graph build/open and current-pin/overlay setup are excluded.
// Prepare and prepare_search explicitly include posting ownership + locator
// work on each operation; query reuses one immutable current-pin plan.
func BenchmarkTypedGraphPreparedFilterBoundaries(b *testing.B) {
	const n = 50000
	col, base, _, _, _, _ := openTypedGraphQualityFixture(b, n)
	current, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 32, Tombstones: 16, Bytes: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	query := vectorBenchmarkEmbedding(19, 8)
	limits := typedGraphFilterLimits{SourceIDs: n + 32, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 4 << 20, InspectedEntries: 2 * n}
	for _, count := range []int{4097, n} {
		filter := HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: fmt.Sprintf("%05d", count-1), Inclusive: true}}}
		for _, boundary := range []string{"prepare", "query", "prepare_search"} {
			b.Run(fmt.Sprintf("eligible%d/%s", count, boundary), func(b *testing.B) {
				plan, err := prepareTypedGraphFilter(overlay, filter, limits)
				if err != nil {
					b.Fatal(err)
				}
				var buffer VectorIndexSearchBuffer
				if _, _, err := overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if boundary != "query" {
						plan, err = prepareTypedGraphFilter(overlay, filter, limits)
						if err != nil {
							b.Fatal(err)
						}
					}
					if boundary != "prepare" {
						results, _, err := overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer)
						if err != nil || len(results) != 10 {
							b.Fatalf("results=%d err=%v", len(results), err)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(plan.sourceIDs), "plan-source-IDs")
				b.ReportMetric(float64(plan.sourceBytes), "plan-source-ID-payload-B")
				b.ReportMetric(float64(plan.retainedBytes), "retained-ordinals-B")
				b.ReportMetric(float64(plan.ordinalGrowthPeakBytes), "ordinal-growth-peak-B")
				runtime.KeepAlive(plan)
			})
		}
	}
}

// Deliberately exercises the unchanged complete-intersection path: a 50K user
// leaf and a five-row path leaf. Fixture/mutation/pins are outside the timer.
func BenchmarkTypedGraphPreparedFilterMixedBroadNarrow(b *testing.B) {
	const n = 50000
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(b, n)
	replacement := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:5]},
		{Name: "content", Strings: columns[1].Strings[:5]},
		{Name: "user", Strings: columns[2].Strings[:5]},
		{Name: "path", Strings: []string{"narrow", "narrow", "narrow", "narrow", "narrow"}},
	}
	if _, err := col.ReplaceTypedBatch(ids[:5], retained[:5], replacement); err != nil {
		b.Fatal(err)
	}
	current, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 32, Tombstones: 16, Bytes: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	filter := HybridScalarFilter{And: []HybridScalarFilter{
		{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "49999", Inclusive: true}}},
		{IndexName: "path", Value: "narrow"},
	}}
	limits := typedGraphFilterLimits{SourceIDs: n + 32, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 4 << 20, InspectedEntries: 2 * n}
	b.Run("prepare", func(b *testing.B) {
		plan, err := prepareTypedGraphFilter(overlay, filter, limits)
		if err != nil || plan.count != 5 || plan.sourceIDs != n+5 {
			b.Fatalf("plan=%+v err=%v", plan, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			plan, err = prepareTypedGraphFilter(overlay, filter, limits)
			if err != nil || plan.count != 5 {
				b.Fatalf("plan=%+v err=%v", plan, err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.sourceIDs), "plan-source-IDs")
		b.ReportMetric(float64(plan.retainedBytes), "retained-ordinals-B")
		runtime.KeepAlive(plan)
	})
}
