package collections

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestTypedGraphPreparedFilterDisconnectedSelectedSeeds(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.EntryOrdinal, input.MaxLayer = 1, 0
	input.Levels = []uint16{0, 0, 0}
	input.AdjacencyLayers = []columnHNSWSearchPackLayerInput{{Offsets: []uint64{0, 0, 0, 0}}}
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	pack, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer pack.Close()
	selection, err := typedcolumn.NewSparseRowSelectionNoCopy(3, []int{0, 2})
	if err != nil {
		t.Fatal(err)
	}
	accelerated, err := selection.WithBitmapMembership(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	for _, selection := range []typedcolumn.RowSelection{selection, accelerated} {
		for _, cap := range []int{3, 2, 3} {
			results, stats, err := pack.searchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: 2, StrictScoreBudget: true, CandidateLimit: cap, CandidateRows: selection, HasCandidateRows: true}, &scratch)
			if cap == 2 {
				if !errors.Is(err, errTypedGraphSearchBudget) || len(results) != 0 || stats.Candidates != 2 || stats.FilteredSeedInspections != 1 {
					t.Fatalf("cap results=%+v stats=%+v err=%v", results, stats, err)
				}
				continue
			}
			if err != nil || len(results) != 2 || results[0].Ordinal != 0 || results[1].Ordinal != 2 || stats.Candidates != 3 || stats.FilteredSeedInspections != 2 || stats.FilteredIneligibleScores != 1 || stats.Edges != 0 {
				t.Fatalf("selected seeds results=%+v stats=%+v err=%v", results, stats, err)
			}
		}
	}
}

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
	t.Run("single_leaf_allocation_growth", func(t *testing.T) {
		// Structural regression guard, not a latency/residency qualification:
		// a borrowed single-leaf scan must not allocate one string per ID.
		filter := rangeFilter("user", 0, 4096)
		allocs := testing.AllocsPerRun(3, func() {
			if _, err := prepareTypedGraphFilter(overlay, filter, limits); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("single-leaf 4097 IDs: allocations=%g", allocs)
		if allocs >= 4097/2 {
			t.Fatalf("single-leaf preparation retains per-ID allocation growth: %g", allocs)
		}
		limited := limits
		const word = bits.UintSize / 8
		limited.RetainedBytes = 4097 * word
		plan, err := prepareTypedGraphFilter(overlay, filter, limited)
		if err != nil || plan.retainedBytes > limited.RetainedBytes || plan.ordinalGrowthPeakBytes < plan.retainedBytes || plan.ordinalGrowthPeakBytes > 2*limited.RetainedBytes {
			t.Fatalf("checked capacity/growth accounting: plan=%+v err=%v", plan, err)
		}
		limited.RetainedBytes--
		if plan, err := prepareTypedGraphFilter(overlay, filter, limited); !errors.Is(err, errTypedGraphSearchBudget) || plan != nil {
			t.Fatalf("capacity limit returned partial plan=%+v err=%v", plan, err)
		}
		// One matching row canonicalizes to a range, but its old backing
		// capacity remains live while the exact ID-rank slice is copied.
		one, err := prepareTypedGraphFilter(overlay, HybridScalarFilter{IndexName: "user", Value: "00000"}, limits)
		if err != nil || one.count != 1 || one.ordinalGrowthPeakBytes < 65*word {
			t.Fatalf("exact rank omitted live capacity: plan=%+v err=%v", one, err)
		}
		// Failed preparation keeps nil plan while exposing the actually visited prefix.
		prefixLimits := limits
		prefixLimits.SourceIDs = 4096
		before := workstats.Read().Graph
		var work ColumnGraphFilterWork
		failed, failErr := prepareTypedGraphFilterWithWork(overlay, filter, prefixLimits, &work)
		after := workstats.Read().Graph
		if !errors.Is(failErr, errTypedGraphSearchBudget) || failed != nil || !work.Attempted || work.Completed || work.SourceIDs != 4096 || work.InspectedEntries < work.SourceIDs || after.Filters.Errors-before.Filters.Errors != 1 || after.FilterSourceIDs-before.FilterSourceIDs != work.SourceIDs || after.FilterInspectedEntries-before.FilterInspectedEntries != work.InspectedEntries {
			t.Fatalf("filter error prefix=%+v err=%v", work, failErr)
		}
		for _, bound := range []func(*typedGraphFilterLimits){
			func(l *typedGraphFilterLimits) { l.SourceIDs = 4096 },
			func(l *typedGraphFilterLimits) { l.SourceBytes = 1 },
			func(l *typedGraphFilterLimits) { l.MappingWork = 1 },
			func(l *typedGraphFilterLimits) { l.InspectedEntries = 4096 },
		} {
			limited := limits
			bound(&limited)
			if plan, err := prepareTypedGraphFilter(overlay, filter, limited); !errors.Is(err, errTypedGraphSearchBudget) || plan != nil {
				t.Fatalf("single-leaf budget returned partial plan=%+v err=%v", plan, err)
			}
		}
	})
	t.Run("sparse_membership_budget_and_search", func(t *testing.T) {
		filter := rangeFilter("user", 0, 4096)
		plan, err := prepareTypedGraphFilter(overlay, filter, limits)
		if err != nil {
			t.Fatal(err)
		}
		bitmapBytes := ((n + 63) / 64) * 8
		if shape := plan.base.Shape(); shape.Kind != "sparse" || shape.BitmapWords*8 != bitmapBytes {
			t.Fatalf("missing sparse membership accelerator: %+v", shape)
		}
		limited := limits
		limited.RetainedBytes = plan.retainedBytes - bitmapBytes
		plain, err := prepareTypedGraphFilter(overlay, filter, limited)
		if err != nil || plain.base.Shape().BitmapWords != 0 || plain.retainedBytes+bitmapBytes != plan.retainedBytes || plan.ordinalGrowthPeakBytes < plan.retainedBytes {
			t.Fatalf("bitmap budget/fallback: plain=%+v accelerated=%+v err=%v", plain, plan, err)
		}
		cold, err := prepareTypedGraphBaseFilter(base, filter, typedGraphBaseFilterLimits{typedGraphFilterLimits: limits, Clauses: 1, PredicateBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		bound, err := bindTypedGraphBaseFilter(cold, overlay, typedGraphFilterBindLimits{Rows: 1, IDBytes: 1024, ValueBytes: 1024, MappingWork: 1024, PredicateWork: 1024, RetainedBytes: 1024, ExactScanRows: n})
		if err != nil || bound.borrowedBaseFilter != cold || bound.retainedBytes != 0 || bound.base.Shape() != plan.base.Shape() {
			t.Fatalf("borrowed accelerator ownership: bound=%+v err=%v", bound, err)
		}
		query := []float32{1, .5, 0, 0, 0, 0, 0, 0}
		var a, b VectorIndexSearchBuffer
		want, wantStats, err := overlay.searchPreparedFilter(plain, query, 10, 128, n, &a)
		if err != nil {
			t.Fatal(err)
		}
		for _, accelerated := range []*typedGraphPreparedFilter{plan, bound} {
			got, stats, err := overlay.searchPreparedFilter(accelerated, query, 10, 128, n, &b)
			if err != nil || !reflect.DeepEqual(got, want) || stats.Base.PreparedScoreCalls != wantStats.Base.PreparedScoreCalls || stats.Base.FilteredIneligibleScores != wantStats.Base.FilteredIneligibleScores || stats.Base.FilteredSeedInspections != wantStats.Base.FilteredSeedInspections || stats.Base.Edges != wantStats.Base.Edges || stats.Base.Candidates != wantStats.Base.Candidates {
				t.Fatalf("membership changed results/work: got=%+v want=%+v stats=%+v wantStats=%+v err=%v", got, want, stats, wantStats, err)
			}
		}
		ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 6}
		ordinals := append([]int(nil), plan.base.SparseRows()...)
		building := &typedGraphPreparedFilter{overlay: overlay, count: len(ordinals), retainedBytes: len(ordinals) * (bits.UintSize / 8)}
		if partial, err := finishTypedGraphFilter(ctx, building, ordinals, nil, limits); !errors.Is(err, context.Canceled) || partial != nil {
			t.Fatalf("canceled bitmap construction: plan=%+v err=%v", partial, err)
		}
	})
	t.Run("filter_cancellation_prefix", func(t *testing.T) {
		leaf := rangeFilter("user", 0, 4095)
		for _, filter := range []HybridScalarFilter{leaf, {And: []HybridScalarFilter{leaf, rangeFilter("path", 0, 4095)}}} {
			ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
			var full ColumnGraphFilterWork
			if _, err := prepareTypedGraphFilterWithContext(ctx, overlay, filter, limits, &full); err != nil {
				t.Fatal(err)
			}
			partial := false
			for check := 1; check < ctx.calls; check++ {
				var work ColumnGraphFilterWork
				cancel := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
				before := workstats.Read().Graph
				got, err := prepareTypedGraphFilterWithContext(cancel, overlay, filter, limits, &work)
				after := workstats.Read().Graph
				if !errors.Is(err, context.Canceled) || got != nil || !work.Attempted || work.Completed || after.FilterSourceIDs-before.FilterSourceIDs != work.SourceIDs || after.FilterInspectedEntries-before.FilterInspectedEntries != work.InspectedEntries || after.Filters.Errors-before.Filters.Errors != 1 {
					t.Fatalf("filter canceled plan=%+v work=%+v err=%v", got, work, err)
				}
				if work.SourceIDs > 0 && work.SourceIDs < full.SourceIDs {
					if len(filter.And) == 0 && (work.ScratchRows != work.SourceIDs || work.ScratchIDBytes != work.SourceBytes) {
						t.Fatalf("lost pending chunk prefix=%+v", work)
					}
					partial = true
					break
				}
			}
			if !partial {
				t.Fatalf("no cancellable posting prefix in %d checks", ctx.calls)
			}
			if _, err := prepareTypedGraphFilterWithContext(nil, overlay, filter, limits, nil); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("single_leaf_pending_error_prefix", func(t *testing.T) {
		limited := limits
		const accepted = 3
		limited.SourceBytes = accepted * len(ids[0])
		var work ColumnGraphFilterWork
		got, err := prepareTypedGraphFilterWithWork(overlay, rangeFilter("user", 0, 4096), limited, &work)
		if !errors.Is(err, errTypedGraphSearchBudget) || got != nil || !work.Attempted || work.Completed || work.SourceIDs != accepted || work.SourceBytes != uint64(limited.SourceBytes) || work.ScratchRows != accepted || work.ScratchIDBytes != work.SourceBytes || work.RetainedBytes != 0 {
			t.Fatalf("pending chunk error prefix: plan=%+v work=%+v err=%v", got, work, err)
		}
	})
	for _, count := range []int{512, 513, 1000, 4096, 4097} {
		plan, err := prepareTypedGraphFilter(overlay, rangeFilter("user", 0, count-1), limits)
		if err != nil || plan.count != count || plan.base.Count() != count || len(plan.delta) != 0 || !plan.validFor(overlay) {
			t.Fatalf("count%d plan=%+v err=%v", count, plan, err)
		}
		if count <= typedGraphScalarExactLimit && plan.base.Shape().BitmapWords != 0 {
			t.Fatal("exact filter allocated a membership bitmap")
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
		beforeWork := workstats.Read().Graph
		results, searchStats, searchErr := overlay.searchPreparedFilter(plan, []float32{1, .5, 0, 0, 0, 0, 0, 0}, 10, 128, n, &buffer)
		afterWork := workstats.Read().Graph
		if afterWork.BaseANNScored-beforeWork.BaseANNScored != searchStats.Base.PreparedScoreCalls || afterWork.ExactBaseScored-beforeWork.ExactBaseScored != uint64(searchStats.ExactBaseScored) || afterWork.DeltaScored != beforeWork.DeltaScored {
			t.Fatalf("score producer stats=%+v before=%+v after=%+v", searchStats, beforeWork, afterWork)
		}
		if count <= 4096 && searchStats.Route != "typed_exact" || count > 4096 && (searchStats.Route != "typed_hnsw" || searchStats.Base.PreparedScoreCalls == 0) {
			t.Fatalf("executed route=%+v", searchStats)
		}
		if searchErr != nil || len(results) != 10 || searchStats.FilteredExact != (count <= 4096) {
			t.Fatalf("count%d route/results n=%d stats=%+v err=%v", count, len(results), searchStats, searchErr)
		}
		if count <= 4096 && (searchStats.ExactBaseScored != count || searchStats.Base.Candidates != 0) {
			t.Fatalf("exact route mislabeled graph work: %+v", searchStats)
		}
		if count > 4096 && (searchStats.ExactBaseScored != 0 || searchStats.Base.Candidates == 0 || searchStats.Base.Edges == 0) {
			t.Fatalf("ANN route lacked graph work: %+v", searchStats)
		}
		if count == 4096 || count == 4097 {
			// Prepared filtering must pass the same context to both exact scoring
			// and selected ANN. Measure checks on this warm fixture, then stop in
			// the first positive scoring prefix without relying on wall time.
			query := []float32{1, .5, 0, 0, 0, 0, 0, 0}
			ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
			_, full, err := overlay.searchPreparedFilterWithContext(ctx, plan, query, 10, 128, n, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			partial := false
			for check := 1; check < ctx.calls; check++ {
				cancel := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
				results, stats, err := overlay.searchPreparedFilterWithContext(cancel, plan, query, 10, 128, n, &buffer)
				if !errors.Is(err, context.Canceled) || len(results) != 0 || len(buffer.results) != 0 || len(buffer.baseResults) != 0 || len(buffer.deltaResults) != 0 {
					t.Fatalf("count=%d check=%d cancellation results=%v stats=%+v err=%v", count, check, results, stats, err)
				}
				if stats.ExactBaseScored > 0 && stats.ExactBaseScored < full.ExactBaseScored || stats.Base.PreparedScoreCalls > 0 && stats.Base.PreparedScoreCalls < full.Base.PreparedScoreCalls {
					partial = true
					break
				}
			}
			if !partial {
				t.Fatalf("no cancellable prepared scoring prefix count=%d checks=%d full=%+v", count, ctx.calls, full)
			}
			results, _, err := overlay.searchPreparedFilterWithContext(nil, plan, query, 10, 128, n, &buffer)
			if err != nil || len(results) != 10 {
				t.Fatalf("retry after scoring cancellation: %v %v", results, err)
			}
		}

		if count == 4096 {
			t.Run("exact_invalid_id_prefix", func(t *testing.T) {
				// A failed ID translation must retain earlier successful translations,
				// independently of cancellation. Corrupt only an owned test offset
				// table; the real prepared pack and mapped assets remain untouched.
				badRank := 1
				if plan.exactBaseByID[badRank] == 0 {
					badRank++
				}
				badOrdinal := plan.exactBaseByID[badRank]
				badOffsets := append([]uint64(nil), overlay.pack.DocumentIDOffsets...)
				badOffsets[badOrdinal+1] = badOffsets[badOrdinal] - 1
				badOverlay := *overlay
				badOverlay.pack = &columnHNSWSearchPackPreparedView{Header: overlay.pack.Header, DocumentIDOffsets: badOffsets, DocumentIDBytes: overlay.pack.DocumentIDBytes, handle: overlay.pack.handle, status: overlay.pack.status}
				badPlan := *plan
				badPlan.overlay = &badOverlay
				before := workstats.Read().Graph
				got, work, err := badOverlay.searchPreparedFilter(&badPlan, []float32{0, 0, 1, 0, 0, 0, 0, 0}, 3, 128, n, &buffer)
				after := workstats.Read().Graph
				if !errors.Is(err, ErrVectorIndexSnapshotMismatch) || len(got) != 0 || len(buffer.results) != 0 || len(buffer.baseResults) != 0 || work.BaseResultIDs != badRank || after.BaseResultIDs-before.BaseResultIDs != uint64(badRank) {
					t.Fatalf("invalid ID lost prefix: want=%d local=%d process=%d err=%v", badRank, work.BaseResultIDs, after.BaseResultIDs-before.BaseResultIDs, err)
				}
			})
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
			results, stats, err := overlay.pack.searchCosine([]float32{1, .5, 0, 0, 0, 0, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 128, StrictScoreBudget: true, CandidateLimit: n, CandidateRows: plan.base, HasCandidateRows: true}, &scratch)
			if err != nil || len(results) != 10 || stats.Candidates == 0 || stats.Edges == 0 {
				t.Fatalf("filtered ANN unavailable/no graph work: n=%d stats=%+v err=%v", len(results), stats, err)
			}
			for _, result := range results {
				if !plan.base.Contains(result.Ordinal) {
					t.Fatalf("ineligible ordinal %d", result.Ordinal)
				}
			}
			results, stats, err = overlay.pack.searchCosine([]float32{1, .5, 0, 0, 0, 0, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 128, StrictScoreBudget: true, CandidateLimit: 16, CandidateRows: plan.base, HasCandidateRows: true}, &scratch)
			if !errors.Is(err, errTypedGraphSearchBudget) || len(results) != 0 || stats.PreparedScoreCalls != 16 || stats.Candidates > stats.PreparedScoreCalls || stats.Edges == 0 {
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
			var work ColumnGraphFilterWork
			if got, err := prepareTypedGraphFilterWithWork(overlay, filter, limited, &work); !errors.Is(err, errTypedGraphSearchBudget) || got != nil || !work.Attempted || work.Completed || work.RetainedBytes != 0 {
				t.Fatalf("budget returned partial plan=%+v work=%+v err=%v", got, work, err)
			}
		})
	}

	t.Run("public_minimal_proof_and_error_prefix", func(t *testing.T) {
		requireTypedGraphPublicServingTest(t)
		opts := typedGraphPublicTestOptions()
		opts.Filter = limits
		opts.Filter.SourceIDs = 4096
		opts.SearchCandidates = 8192
		if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", opts); err != nil {
			t.Fatal(err)
		}
		var buffer VectorIndexSearchBuffer
		for _, count := range []int{0, 4096, 4097} {
			q := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, .5, 0, 0, 0, 0, 0, 0}, TopK: 10, EfSearch: 128, StatsMode: VectorIndexSearchStatsModeMinimal}
			if count != 0 {
				f := rangeFilter("user", 0, count-1)
				q.DeclaredScalarFilter = &f
			}
			before := workstats.Read().Graph
			response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
			after := workstats.Read().Graph
			work := response.Stats.ColumnGraphWork
			if after.BaseANNScored-before.BaseANNScored != work.BaseANNScored || after.BaseCandidates-before.BaseCandidates != work.BaseCandidates || work.BaseANNScored+work.DeltaScored > uint64(opts.SearchCandidates) {
				t.Fatalf("public total score allowance/local process proof mismatch: %+v", work)
			}
			if !work.Available || after.Requests.Attempts-before.Requests.Attempts != 1 {
				t.Fatalf("missing public proof=%+v err=%v", work, err)
			}
			if count == 4097 {
				if !errors.Is(err, errTypedGraphSearchBudget) || view != nil || len(response.Results) != 0 || len(buffer.results) != 0 || work.Filter.SourceIDs != 4096 || work.Filter.Completed || work.Route != "" || after.Requests.Errors-before.Requests.Errors != 1 || after.Requests.Completed != before.Requests.Completed || after.FilterSourceIDs-before.FilterSourceIDs != work.Filter.SourceIDs {
					t.Fatalf("public error prefix=%+v err=%v", work, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			if count == 0 && (work.BaseANNScored != response.Stats.PreparedScoreCalls || work.BaseCandidates != response.Stats.Candidates || work.BaseANNScored <= work.BaseCandidates) {
				t.Fatalf("public proof omitted upper scorer work: work=%+v scores=%d layer0=%d", work, response.Stats.PreparedScoreCalls, response.Stats.Candidates)
			}
			if count == 0 && (work.Route != "typed_hnsw" || work.BaseANNScored == 0 || work.BaseEdges == 0) || count == 4096 && (work.Route != "typed_exact" || work.ExactBaseScored != 4096 || work.BaseANNScored != 0) {
				t.Fatalf("public Minimal proof=%+v", work)
			}
		}
		// Reuse the existing counting context to cancel at the final post-search
		// check. Capture the same warm path's actual check count, not a fixed number.
		q := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, .5, 0, 0, 0, 0, 0, 0}, TopK: 10, EfSearch: 128, StatsMode: VectorIndexSearchStatsModeMinimal}
		ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
		q.Context = ctx
		_, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		checks := ctx.calls
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		ctx = &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: checks}
		q.Context = ctx
		before := workstats.Read().Graph
		response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		after := workstats.Read().Graph
		if !errors.Is(err, context.Canceled) || view != nil || len(response.Results) != 0 || len(buffer.results) != 0 || response.Stats.ColumnGraphWork.BaseANNScored == 0 || after.Requests.Errors-before.Requests.Errors != 1 || after.Requests.Completed != before.Requests.Completed || after.BaseANNScored-before.BaseANNScored != response.Stats.ColumnGraphWork.BaseANNScored {
			t.Fatalf("post-search cancellation err=%v work=%+v checks=%d", err, response.Stats.ColumnGraphWork, checks)
		}

		// Sweep the existing deterministic check-count context: cancellation
		// must be observable during graph work, not only after all scoring.
		q.Context = context.Background()
		full, fullView, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		if err := fullView.Close(); err != nil {
			t.Fatal(err)
		}
		partial := false
		for check := 1; check < checks; check++ {
			q.Context = &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
			before := workstats.Read().Graph
			response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
			after := workstats.Read().Graph
			if !errors.Is(err, context.Canceled) || view != nil || len(response.Results) != 0 || len(buffer.results) != 0 {
				t.Fatalf("mid-search cancellation check=%d err=%v view=%v results=%d", check, err, view, len(response.Results))
			}
			work := response.Stats.ColumnGraphWork
			if got := col.collectionSchemaCoordinator().typedGraphOwners.owners; got != 0 {
				t.Fatalf("canceled query leaked %d owners", got)
			}
			if work.Completed || after.BaseANNScored-before.BaseANNScored != work.BaseANNScored || after.Requests.Errors-before.Requests.Errors != 1 {
				t.Fatalf("canceled local/process prefix mismatch: %+v", work)
			}
			if work.BaseANNScored > 0 && work.BaseANNScored < full.Stats.ColumnGraphWork.BaseANNScored {
				partial = true
				break
			}
		}
		if !partial {
			t.Fatalf("no cancellable graph prefix in %d checks; completed work=%+v", checks, full.Stats.ColumnGraphWork)
		}

		// The public filtered route owns both preparation and exact-scoring
		// prefixes, and must release its owner for cancellation in either stage.
		filter := rangeFilter("user", 0, 4095)
		q.DeclaredScalarFilter = &filter
		counted := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
		q.Context = counted
		_, measuredView, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		if err := measuredView.Close(); err != nil {
			t.Fatal(err)
		}
		filterPrefix, exactPrefix := false, false
		for check := 1; check < counted.calls; check++ {
			q.Context = &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
			response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
			if !errors.Is(err, context.Canceled) || view != nil || len(response.Results) != 0 || len(buffer.results) != 0 {
				t.Fatalf("public filtered cancellation check=%d err=%v", check, err)
			}
			work := response.Stats.ColumnGraphWork
			if got := col.collectionSchemaCoordinator().typedGraphOwners.owners; got != 0 {
				t.Fatalf("filtered cancellation leaked %d owners", got)
			}
			if !work.Filter.Completed && work.Filter.SourceIDs > 0 && work.Filter.SourceIDs < 4096 {
				filterPrefix = true
			}
			if work.Filter.Completed && work.ExactBaseScored > 0 && work.ExactBaseScored < 4096 {
				exactPrefix = true
			}
			if filterPrefix && exactPrefix {
				break
			}
		}
		if !filterPrefix || !exactPrefix {
			t.Fatalf("missing public cancellation prefix: filter=%t exact=%t checks=%d", filterPrefix, exactPrefix, counted.calls)
		}

		// TopK=257 reaches a second periodic ID-translation check after 256
		// appends. Sweep the existing counting context to target that boundary
		// without assuming how many owner/preparation checks precede it.
		q.TopK, q.EfSearch = 257, 257
		translated := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: int(^uint(0) >> 1)}
		q.Context = translated
		complete, completeView, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		if err := completeView.Close(); err != nil {
			t.Fatal(err)
		}
		if complete.Stats.ColumnGraphWork.BaseResultIDs != 257 {
			t.Fatalf("complete translation=%+v", complete.Stats.ColumnGraphWork)
		}
		idPrefix := false
		for check := 1; check < translated.calls; check++ {
			q.Context = &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
			before := workstats.Read().Graph
			response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
			after := workstats.Read().Graph
			work := response.Stats.ColumnGraphWork
			if !errors.Is(err, context.Canceled) || view != nil || len(response.Results) != 0 || len(buffer.results) != 0 || len(buffer.baseResults) != 0 || len(buffer.deltaResults) != 0 || work.Completed || after.BaseResultIDs-before.BaseResultIDs != work.BaseResultIDs {
				t.Fatalf("ID translation cancellation check=%d work=%+v err=%v", check, work, err)
			}
			if got := col.collectionSchemaCoordinator().typedGraphOwners.owners; got != 0 {
				t.Fatalf("ID cancellation leaked %d owners", got)
			}
			if work.BaseResultIDs == 256 {
				if work.ExactBaseScored != 4096 || !work.Filter.Completed {
					t.Fatalf("ID prefix lacks completed scoring/filter work=%+v", work)
				}
				idPrefix = true
				break
			}
		}
		if !idPrefix {
			t.Fatalf("missing 256 translated-ID prefix in %d checks", translated.calls)
		}

		q.Context = nil
		retry, retryView, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil || len(retry.Results) != 257 || retry.Stats.ColumnGraphWork.BaseResultIDs != 257 {
			t.Fatalf("public retry after cancellation: results=%d err=%v", len(retry.Results), err)
		}
		if err := retryView.Close(); err != nil {
			t.Fatal(err)
		}
		if got := col.collectionSchemaCoordinator().typedGraphOwners.owners; got != 0 {
			t.Fatalf("retry leaked %d owners", got)
		}

	})
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
