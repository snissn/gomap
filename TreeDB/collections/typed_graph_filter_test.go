package collections

import (
	"errors"
	"fmt"
	"testing"
)

func TestTypedGraphPreparedFilterFinalIntersectionAndBounds(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	const n = 6000
	ids, retained := make([][]byte, n), make([][]byte, n)
	columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("row-%05d", i)
		ids[i], retained[i] = []byte(id), []byte(fmt.Sprintf(`{"id":%q}`, id))
		columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{1, float32(i%97) / 97, 0, 0, 0, 0, 0, 0})
		columns[1].Strings = append(columns[1].Strings, "content")
		for j := 2; j < 4; j++ {
			columns[j].Strings = append(columns[j].Strings, fmt.Sprintf("%05d", i))
		}
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	base, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	current, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 1, Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	rangeFilter := func(index string, lo, hi int) HybridScalarFilter {
		return HybridScalarFilter{IndexName: index, Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: fmt.Sprintf("%05d", lo), Inclusive: true}, Upper: IndexRangeBound{Value: fmt.Sprintf("%05d", hi), Inclusive: true}}}
	}
	limits := typedGraphFilterLimits{SourceIDs: 20000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 20000}
	for _, count := range []int{512, 513, 1000, 4096, 4097} {
		plan, err := prepareTypedGraphFilter(overlay, rangeFilter("user", 0, count-1), limits)
		if err != nil || plan.count != count || plan.base.Count() != count || len(plan.delta) != 0 || !plan.validFor(overlay) {
			t.Fatalf("count%d plan=%+v err=%v", count, plan, err)
		}
		other := *overlay
		if plan.validFor(&other) {
			t.Fatal("filter accepted different overlay identity")
		}
		var buffer VectorIndexSearchBuffer
		if count == 512 {
			tied, _, err := overlay.searchPreparedFilter(plan, []float32{0, 0, 1, 0, 0, 0, 0, 0}, 10, 128, n, &buffer)
			if err != nil || len(tied) != 10 {
				t.Fatalf("tied exact query: %+v %v", tied, err)
			}
			for i, result := range tied {
				if string(result.ID) != fmt.Sprintf("row-%05d", i) {
					t.Fatalf("exact cutoff tie used graph order: position=%d id=%s", i, result.ID)
				}
			}
		}
		results, searchStats, searchErr := overlay.searchPreparedFilter(plan, []float32{1, .5, 0, 0, 0, 0, 0, 0}, 10, 128, n, &buffer)
		if searchErr != nil || len(results) != 10 || searchStats.FilteredExact != (count <= 4096) {
			t.Fatalf("count%d route/results n=%d stats=%+v err=%v", count, len(results), searchStats, searchErr)
		}
		if count <= 4096 && (searchStats.ExactBaseScored != count || searchStats.Base.Candidates != 0) {
			t.Fatalf("exact route mislabeled graph work: %+v", searchStats)
		}
		if count > 4096 && (searchStats.ExactBaseScored != 0 || searchStats.Base.Candidates == 0 || searchStats.Base.Edges == 0) {
			t.Fatalf("ANN route lacked graph work: %+v", searchStats)
		}
		if _, _, err := overlay.searchPreparedFilter(plan, []float32{1}, 10, 128, n, &buffer); err == nil || len(buffer.results) != 0 {
			t.Fatalf("invalid query retained prior results: %v", err)
		}
		if _, _, err := other.searchPreparedFilter(plan, []float32{1, .5, 0, 0, 0, 0, 0, 0}, 10, 128, n, &buffer); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || len(buffer.results) != 0 {
			t.Fatalf("wrong overlay accepted plan: %v", err)
		}
		allocs := testing.AllocsPerRun(10, func() {
			got, _, err := overlay.searchPreparedFilter(plan, []float32{1, .5, 0, 0, 0, 0, 0, 0}, 10, 128, n, &buffer)
			if err != nil || len(got) != 10 {
				t.Fatalf("reused search: n=%d err=%v", len(got), err)
			}
		})
		t.Logf("prepared count=%d repeat query allocs=%g source_ids=%d source_bytes=%d retained_ordinal_bytes=%d inspected=%d mapping_bound=%d", count, allocs, plan.sourceIDs, plan.sourceBytes, plan.retainedBytes, plan.inspectedEntries, plan.mappingWork)
		if count == 4097 {
			var scratch columnVectorGraphNativeSearchScratch
			results, stats, err := overlay.pack.searchCosine([]float32{1, .5, 0, 0, 0, 0, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 128, CandidateLimit: n, CandidateRows: plan.base, HasCandidateRows: true}, &scratch)
			if err != nil || len(results) != 10 || stats.Candidates == 0 || stats.Edges == 0 {
				t.Fatalf("filtered ANN unavailable/no graph work: n=%d stats=%+v err=%v", len(results), stats, err)
			}
			for _, result := range results {
				if !plan.base.Contains(result.Ordinal) {
					t.Fatalf("ineligible ordinal %d", result.Ordinal)
				}
			}
			results, stats, err = overlay.pack.searchCosine([]float32{1, .5, 0, 0, 0, 0, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 128, CandidateLimit: 16, CandidateRows: plan.base, HasCandidateRows: true}, &scratch)
			if !errors.Is(err, errTypedGraphSearchBudget) || len(results) != 0 || stats.Candidates != 16 || stats.Edges == 0 {
				t.Fatalf("filtered cap lost work/returned partial results: n=%d stats=%+v err=%v", len(results), stats, err)
			}
		}
	}
	filter := HybridScalarFilter{And: []HybridScalarFilter{rangeFilter("user", 0, 4999), rangeFilter("path", 1000, 5999)}}
	empty, err := prepareTypedGraphFilter(overlay, HybridScalarFilter{And: []HybridScalarFilter{{IndexName: "user", Value: "absent"}, rangeFilter("path", 0, 5999)}}, limits)
	if err != nil || empty.count != 0 {
		t.Fatalf("empty first conjunct resurrected rows: %+v %v", empty, err)
	}
	plan, err := prepareTypedGraphFilter(overlay, filter, limits)
	if err != nil || plan.count != 4000 || plan.sourceIDs != 10000 || plan.inspectedEntries < plan.sourceIDs {
		t.Fatalf("large leaves/small final intersection: %+v %v", plan, err)
	}
	for _, bound := range []struct {
		name   string
		mutate func(*typedGraphFilterLimits)
	}{
		{"source_ids", func(l *typedGraphFilterLimits) { l.SourceIDs = 9999 }},
		{"source_bytes", func(l *typedGraphFilterLimits) { l.SourceBytes = 1 }},
		{"retained", func(l *typedGraphFilterLimits) { l.RetainedBytes = 1 }},
		{"mapping", func(l *typedGraphFilterLimits) { l.MappingWork = 1 }},
		{"physical_work", func(l *typedGraphFilterLimits) { l.InspectedEntries = 9999 }},
	} {
		t.Run(bound.name, func(t *testing.T) {
			limited := limits
			bound.mutate(&limited)
			if got, err := prepareTypedGraphFilter(overlay, filter, limited); !errors.Is(err, errTypedGraphSearchBudget) || got != nil {
				t.Fatalf("budget returned partial plan=%+v err=%v", got, err)
			}
		})
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if plan.validFor(overlay) {
		t.Fatal("filter survived closed current pin")
	}
	var closedBuffer VectorIndexSearchBuffer
	if _, _, err := overlay.searchPreparedFilter(plan, []float32{1, .5, 0, 0, 0, 0, 0, 0}, 10, 128, n, &closedBuffer); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("closed pin query: %v", err)
	}
}
