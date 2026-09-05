package collections

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
)

// A separate allocation/residency diagnostic: forced GC is deliberately not
// mixed into throughput measurements. Signed heap deltas include Go pool/GC
// effects; source/logical payload counters are not claimed as exact heap size.
func TestTypedGraphBaseFilterRepresentativeSuffixResidency(t *testing.T) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 5000)
	cold, err := prepareTypedGraphBaseFilter(base, HybridScalarFilter{IndexName: "path", Value: "source"}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 10000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 20000}, Clauses: 8, PredicateBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	overlayLimits := typedGraphOverlayLimits{Rows: 384, Tombstones: 128, Bytes: 16 << 20}
	bindLimits := typedGraphFilterBindLimits{Rows: 256, IDBytes: 1 << 20, ValueBytes: 1 << 20, MappingWork: 1 << 20, PredicateWork: 1 << 20, RetainedBytes: 1 << 20, ExactScanRows: 4352}
	for _, tc := range []struct {
		name                          string
		start, end, deleted, physical int
	}{{"empty", 0, 0, 0, 0}, {"typical", 0, 16, 0, 16}, {"threshold", 16, 256, 0, 256}, {"tombstone_heavy", 0, 0, 128, 384}} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.end > tc.start {
				content := make([]string, tc.end-tc.start)
				for i := range content {
					content[i] = "modified content"
				}
				row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[tc.start:tc.end]}, {Name: "content", Strings: content}, {Name: "user", Strings: columns[2].Strings[tc.start:tc.end]}, {Name: "path", Strings: columns[3].Strings[tc.start:tc.end]}}
				if _, err := col.ReplaceTypedBatch(ids[tc.start:tc.end], retained[tc.start:tc.end], row); err != nil {
					t.Fatal(err)
				}
			}
			if tc.deleted > 0 {
				if n, err := col.DeleteBatch(ids[:tc.deleted]); err != nil || n != tc.deleted {
					t.Fatalf("deleted=%d err=%v", n, err)
				}
			}
			current, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			prepare := func() (*typedGraphOverlaySearch, *typedGraphPreparedFilter) {
				overlay, err := prepareTypedGraphOverlaySearch(base, current, overlayLimits)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := bindTypedGraphBaseFilter(cold, overlay, bindLimits)
				if err != nil {
					t.Fatal(err)
				}
				return overlay, plan
			}
			overlay, plan := prepare()
			if overlay.sourceRows != tc.physical || overlay.sourceTombstones != tc.deleted || plan.count != 5000-tc.deleted || len(overlay.rows) > bindLimits.Rows {
				t.Fatalf("source rows=%d tombstones=%d logical=%d final=%d", overlay.sourceRows, overlay.sourceTombstones, len(overlay.rows), plan.count)
			}
			var payload int
			for _, row := range overlay.rows {
				payload += len(row.ID)
				for _, value := range row.Values {
					payload += len(value.String) + len(value.StringBytes) + len(value.Float32Vector)*4
				}
			}
			if int64(payload) > overlayLimits.Bytes || plan.retainedBytes > bindLimits.RetainedBytes {
				t.Fatal("retained payload exceeded limits")
			}
			var buffer VectorIndexSearchBuffer
			query := columns[0].Float32Vectors[4999]
			results, stats, err := overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer)
			if err != nil || len(results) != 10 || stats.FilteredExact || stats.Base.Candidates == 0 || stats.Base.Edges == 0 {
				t.Fatalf("search results=%d stats=%+v err=%v", len(results), stats, err)
			}
			queryAllocs := testing.AllocsPerRun(10, func() {
				if _, _, err := overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer); err != nil {
					t.Fatal(err)
				}
			})
			if tc.physical > 0 {
				limited := overlayLimits
				limited.Rows = tc.physical - 1
				if _, err := prepareTypedGraphOverlaySearch(base, current, limited); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
					t.Fatalf("source cap accepted: %v", err)
				}
			}
			if tc.deleted > 0 {
				limited := overlayLimits
				limited.Tombstones = tc.deleted - 1
				if _, err := prepareTypedGraphOverlaySearch(base, current, limited); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
					t.Fatalf("tombstone cap accepted: %v", err)
				}
			}
			const copies = 8
			kept := make([]*typedGraphPreparedFilter, copies)
			var before, allocated, live runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			for i := range kept {
				_, kept[i] = prepare()
			}
			runtime.ReadMemStats(&allocated)
			runtime.GC()
			runtime.ReadMemStats(&live)
			t.Logf("physical=%d tombstones=%d source_asset_B=%d logical_D=%d typed_logical_payload_B=%d bound_ordinal_capacity_B=%d query_allocs=%g candidates=%d edges=%d base_result_IDs=%d prepare_bind_B/copy=%d allocs/copy=%d signed_live_heap_delta_B/copy=%d copies=%d", overlay.sourceRows, overlay.sourceTombstones, overlay.sourceBytes, len(overlay.rows), payload, plan.retainedBytes, queryAllocs, stats.Base.Candidates, stats.Base.Edges, stats.BaseResultIDs, (allocated.TotalAlloc-before.TotalAlloc)/copies, (allocated.Mallocs-before.Mallocs)/copies, (int64(live.HeapAlloc)-int64(before.HeapAlloc))/copies, copies)
			runtime.KeepAlive(kept)
			runtime.KeepAlive(overlay)
			runtime.KeepAlive(plan)
			runtime.KeepAlive(buffer)
		})
	}
}

// This diagnostic keeps one immutable base and one logical changed ID. Cold
// compilation/selection, overlay, bind, query and actual new-pin reads are
// separate boundaries. The new-pin case changes content before every timed
// read; write/ack is EXCLUDED, while pin open/overlay/bind/query/close is included.
// Use -benchtime=10x (the turnover case is deliberately not an unbounded soak).
func BenchmarkTypedGraphBaseFilterBindingBoundaries(b *testing.B) {
	const n = 50000
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixtureWithNarrowPaths(b, n, 5)
	coldLimits := typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 2 * n, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 4 << 20, InspectedEntries: 3 * n}, Clauses: 8, PredicateBytes: 1024}
	bindLimits := typedGraphFilterBindLimits{Rows: 256, IDBytes: 1 << 20, ValueBytes: 1 << 20, MappingWork: 1 << 20, PredicateWork: 1 << 20, RetainedBytes: 1 << 20, ExactScanRows: 4096 + 256}
	overlayLimits := typedGraphOverlayLimits{Rows: 256, Tombstones: 128, Bytes: 16 << 20}
	rangeFilter := func(count int) HybridScalarFilter {
		return HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: fmt.Sprintf("%05d", count-1), Inclusive: true}}}
	}
	cases := []struct {
		name   string
		filter HybridScalarFilter
		count  int
	}{
		{"eligible4097", rangeFilter(4097), 4097},
		{"eligible50000", rangeFilter(n), n},
		{"mixed50000_5", HybridScalarFilter{And: []HybridScalarFilter{rangeFilter(n), {IndexName: "path", Value: "narrow"}}}, 5},
	}
	row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{""}}, {Name: "user", Strings: columns[2].Strings[:1]}, {Name: "path", Strings: columns[3].Strings[:1]}}
	mutations := 0
	mutate := func() {
		mutations++
		row[1].Strings[0] = fmt.Sprintf("turnover-%d", mutations)
		results, err := col.ReplaceTypedBatch(ids[:1], retained[:1], row)
		if err != nil || len(results) != 1 || !results[0].Modified {
			b.Fatalf("mutation=%d results=%+v err=%v", mutations, results, err)
		}
	}
	mutate()
	current, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, overlayLimits)
	if err != nil {
		b.Fatal(err)
	}
	query := vectorBenchmarkEmbedding(19, 8)
	for _, tc := range cases {
		cold, err := prepareTypedGraphBaseFilter(base, tc.filter, coldLimits)
		if err != nil {
			b.Fatal(err)
		}
		for _, boundary := range []string{"cold", "overlay", "bind", "query", "bind_query", "newpin_overlay_bind_query"} {
			b.Run(tc.name+"/"+boundary, func(b *testing.B) {
				b.StopTimer()
				if boundary == "newpin_overlay_bind_query" && b.N > 64 {
					b.Fatal("bounded turnover diagnostic: use -benchtime=10x")
				}
				plan, err := bindTypedGraphBaseFilter(cold, overlay, bindLimits)
				if err != nil {
					b.Fatal(err)
				}
				var buffer VectorIndexSearchBuffer
				results, _, err := overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer)
				if err != nil || len(results) != min(10, tc.count) {
					b.Fatalf("warm results=%d err=%v", len(results), err)
				}
				generations := make([]uint64, b.N)
				lsns := make([]uint64, b.N)
				maxPhysicalRows := 0
				lastOverlay := overlay
				b.ReportAllocs()
				b.ResetTimer()
				if boundary != "newpin_overlay_bind_query" {
					b.StartTimer()
				}
				for i := 0; i < b.N; i++ {
					switch boundary {
					case "cold":
						cold, err = prepareTypedGraphBaseFilter(base, tc.filter, coldLimits)
					case "overlay":
						lastOverlay, err = prepareTypedGraphOverlaySearch(base, current, overlayLimits)
					case "bind", "bind_query":
						plan, err = bindTypedGraphBaseFilter(cold, overlay, bindLimits)
					case "newpin_overlay_bind_query":
						mutate()
						b.StartTimer()
						pin, openErr := col.OpenCollectionReadView()
						if openErr != nil {
							b.Fatal(openErr)
						}
						lastOverlay, err = prepareTypedGraphOverlaySearch(base, pin, overlayLimits)
						if err == nil {
							plan, err = bindTypedGraphBaseFilter(cold, lastOverlay, bindLimits)
						}
						if err == nil {
							results, _, err = lastOverlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer)
						}
						if err == nil {
							generations[i] = lastOverlay.rows[0].Generation
							lsns[i] = lastOverlay.rows[0].AppliedCommandLSN
							maxPhysicalRows = max(maxPhysicalRows, lastOverlay.sourceRows)
						}
						closeErr := pin.Close()
						if err == nil {
							err = closeErr
						}
						b.StopTimer()
					}
					if err != nil {
						b.Fatal(err)
					}
					if boundary == "query" || boundary == "bind_query" {
						results, _, err = overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer)
					}
					if err != nil || plan.count != tc.count {
						b.Fatalf("count=%d expected=%d err=%v", plan.count, tc.count, err)
					}
				}
				b.StopTimer()
				if boundary == "newpin_overlay_bind_query" {
					for i := 1; i < len(generations); i++ {
						if generations[i] <= generations[i-1] || lsns[i] <= lsns[i-1] {
							b.Fatalf("current pin did not advance: generations=%v lsns=%v", generations, lsns)
						}
					}
					b.Logf("write/ack excluded; generations=%v appliedLSNs=%v cumulative_mutations=%d max_suffix_physical_rows=%d", generations, lsns, mutations, maxPhysicalRows)
					b.ReportMetric(float64(maxPhysicalRows), "max-suffix-physical-rows")
				}
				b.ReportMetric(float64(cold.plan.sourceIDs), "cold-source-IDs")
				b.ReportMetric(float64(plan.sourceIDs), "binding-source-IDs")
				b.ReportMetric(float64(plan.retainedBytes), "binding-retained-ordinal-B")
				b.ReportMetric(float64(cold.predicateBytes), "owned-predicate-B")
				runtime.KeepAlive(cold)
				runtime.KeepAlive(plan)
				runtime.KeepAlive(lastOverlay)
				runtime.KeepAlive(results)
			})
		}
	}
}
