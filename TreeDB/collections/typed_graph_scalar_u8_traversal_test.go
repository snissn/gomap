package collections

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// These tests use real scalar-u8 code assets but deliberately tiny synthetic
// graph views. That keeps Q1 coverage focused on the private typed adapter:
// code rows remain owned by the immutable reader while the graph shape makes
// budget, filtering, and local-to-base behavior deterministic.
type typedGraphScalarU8Fixture4684 struct {
	overlay          *typedGraphOverlaySearch
	indexName        string
	query            []float32
	rows             int
	dimensions       int
	baseVectorStride int
	docOrdinals      [5]int
}

func openTypedGraphScalarU8Fixture4684(tb testing.TB) typedGraphScalarU8Fixture4684 {
	tb.Helper()
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-0", vector: []float32{0, 1, 0}},
		{id: "doc-1", vector: []float32{-1, 0, 0}},
		{id: "doc-2", vector: []float32{1, 0, 0}},
		{id: "doc-3", vector: []float32{0, 0, 1}},
		{id: "doc-4", vector: []float32{1, 0, 0}},
	}
	_, db, collection, def := openColumnGraphQuantizedGuardrailTestCollection1926(tb, rows)
	tb.Cleanup(func() { _ = db.Close() })
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		tb.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	tb.Cleanup(func() { _ = searcher.Close() })
	current, err := collection.OpenCollectionReadView()
	if err != nil {
		tb.Fatalf("OpenCollectionReadView: %v", err)
	}
	tb.Cleanup(func() { _ = current.Close() })
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		tb.Fatal("fixture missing prepared reader or hnsw pack")
	}
	basePack := searcher.reader.hnswSearchPack
	docOrdinals := [5]int{-1, -1, -1, -1, -1}
	for ordinal := 0; ordinal < basePack.Header.Rows; ordinal++ {
		id, ok := basePack.documentIDForOrdinal(ordinal)
		if !ok {
			tb.Fatalf("documentIDForOrdinal(%d) failed", ordinal)
		}
		switch string(id) {
		case "doc-0":
			docOrdinals[0] = ordinal
		case "doc-1":
			docOrdinals[1] = ordinal
		case "doc-2":
			docOrdinals[2] = ordinal
		case "doc-3":
			docOrdinals[3] = ordinal
		case "doc-4":
			docOrdinals[4] = ordinal
		}
	}
	for document, ordinal := range docOrdinals {
		if ordinal < 0 {
			tb.Fatalf("rebuilt pack did not retain doc-%d", document)
		}
	}
	return typedGraphScalarU8Fixture4684{
		overlay:          &typedGraphOverlaySearch{base: searcher, pack: basePack, current: current},
		indexName:        def.QuantizedIndexes[0].Name,
		query:            []float32{1, 0, 0},
		rows:             basePack.Header.Rows,
		dimensions:       def.Dimensions,
		baseVectorStride: basePack.Header.VectorStride,
		docOrdinals:      docOrdinals,
	}
}

func typedGraphScalarU8Header4684(rows, dimensions, vectorStride, entryOrdinal int, levels []uint16, layers []columnHNSWSearchPackPreparedLayer) columnHNSWSearchPackHeader {
	maxLayer := 0
	for _, level := range levels {
		if int(level) > maxLayer {
			maxLayer = int(level)
		}
	}
	return columnHNSWSearchPackHeader{
		Rows: rows, Dimensions: dimensions, VectorStride: vectorStride,
		M: 1, EfConstruction: 1, EfSearch: rows, EntryOrdinal: entryOrdinal,
		MaxLayer: maxLayer, AdjacencyLayerCount: len(layers),
	}
}

func typedGraphScalarU8Pack4684(rows, dimensions, vectorStride, entryOrdinal int, levels []uint16, layers []columnHNSWSearchPackPreparedLayer) *columnHNSWSearchPackPreparedView {
	return &columnHNSWSearchPackPreparedView{
		Header: typedGraphScalarU8Header4684(rows, dimensions, vectorStride, entryOrdinal, levels, layers),
		Levels: levels, AdjacencyLayers: layers,
		status: columnHNSWSearchPackPreparedStatusHeap, ephemeralHeap: true,
	}
}

func typedGraphScalarU8Layer4684(rows int, neighbors [][]uint32) columnHNSWSearchPackPreparedLayer {
	offsets := make([]uint64, rows+1)
	var flattened []uint32
	for ordinal := 0; ordinal < rows; ordinal++ {
		offsets[ordinal] = uint64(len(flattened))
		flattened = append(flattened, neighbors[ordinal]...)
	}
	offsets[rows] = uint64(len(flattened))
	return columnHNSWSearchPackPreparedLayer{Offsets: offsets, Neighbors: flattened}
}

func typedGraphScalarU8ConnectedPack4684(f typedGraphScalarU8Fixture4684) *columnHNSWSearchPackPreparedView {
	doc := f.docOrdinals
	neighbors := make([][]uint32, f.rows)
	neighbors[doc[0]] = []uint32{uint32(doc[1]), uint32(doc[2])}
	neighbors[doc[1]] = []uint32{uint32(doc[0])}
	neighbors[doc[2]] = []uint32{uint32(doc[0]), uint32(doc[4])}
	neighbors[doc[4]] = []uint32{uint32(doc[2])}
	return typedGraphScalarU8Pack4684(f.rows, f.dimensions, f.baseVectorStride, doc[1], make([]uint16, f.rows), []columnHNSWSearchPackPreparedLayer{typedGraphScalarU8Layer4684(f.rows, neighbors)})
}

func typedGraphScalarU8DisconnectedPack4684(f typedGraphScalarU8Fixture4684) *columnHNSWSearchPackPreparedView {
	return typedGraphScalarU8Pack4684(f.rows, f.dimensions, f.baseVectorStride, f.docOrdinals[1], make([]uint16, f.rows), []columnHNSWSearchPackPreparedLayer{{Offsets: make([]uint64, f.rows+1)}})
}

func typedGraphScalarU8Navigation4684(f typedGraphScalarU8Fixture4684, basePack *columnHNSWSearchPackPreparedView, upper bool) *typedGraphFilterNavigation {
	levels := []uint16{0, 0, 0}
	layers := []columnHNSWSearchPackPreparedLayer{{
		// Entry local ordinal 0 has two unvisited layer-0 neighbors. Besides
		// exercising the fused mapped row-ID path, this gives cancellation
		// coverage a real multi-row scalar-u8 score batch.
		Offsets:   []uint64{0, 2, 3, 4},
		Neighbors: []uint32{1, 2, 0, 0},
	}}
	if upper {
		levels = []uint16{1, 1, 1}
		layers = append(layers, columnHNSWSearchPackPreparedLayer{
			Offsets:   []uint64{0, 2, 3, 4},
			Neighbors: []uint32{1, 2, 0, 0},
		})
	}
	// Construct the view in place: copying a prepared view would copy its
	// sync.Once/atomic state and fails go vet's copylocks check.
	return &typedGraphFilterNavigation{
		view: columnHNSWSearchPackPreparedView{
			Header: typedGraphScalarU8Header4684(3, f.dimensions, f.baseVectorStride, 0, levels, layers),
			Levels: levels, AdjacencyLayers: layers,
			status: columnHNSWSearchPackPreparedStatusHeap, ephemeralHeap: true,
		},
		baseOrdinals: []uint32{uint32(f.docOrdinals[0]), uint32(f.docOrdinals[4]), uint32(f.docOrdinals[2])},
		basePack:     basePack,
	}
}

func typedGraphScalarU8Selection4684(tb testing.TB, f typedGraphScalarU8Fixture4684, documents ...int) typedcolumn.RowSelection {
	tb.Helper()
	rows := make([]int, len(documents))
	for i, document := range documents {
		rows[i] = f.docOrdinals[document]
	}
	sort.Ints(rows)
	selection, err := typedcolumn.NewSparseRowSelection(f.rows, rows)
	if err != nil {
		tb.Fatal(err)
	}
	return selection
}

func typedGraphScalarU8DispersedRows4684(f typedGraphScalarU8Fixture4684) []int {
	rows := []int{f.docOrdinals[0], f.docOrdinals[2], f.docOrdinals[4]}
	sort.Ints(rows)
	return rows
}

func typedGraphScalarU8DirectPlan4684(overlay *typedGraphOverlaySearch, selection typedcolumn.RowSelection) *typedGraphPreparedFilter {
	return &typedGraphPreparedFilter{overlay: overlay, base: selection, count: selection.Count()}
}

func typedGraphScalarU8CachedPlan4684(tb testing.TB, overlay *typedGraphOverlaySearch, selection typedcolumn.RowSelection, navigation *typedGraphFilterNavigation) *typedGraphPreparedFilter {
	tb.Helper()
	if overlay == nil || overlay.base == nil || overlay.base.reader == nil || overlay.base.reader.sharedPreparedSearch == nil || overlay.base.reader.sharedPreparedSearch.holder == nil || overlay.base.catalog == nil || overlay.base.catalog.meta.Options.ColumnStore == nil {
		tb.Fatal("fixture missing shared prepared base required for cached navigation binding")
	}
	origin := &typedGraphPreparedFilter{base: selection, count: selection.Count()}
	base := &typedGraphBaseFilter{
		plan:       origin,
		navigation: navigation,
		holder:     overlay.base.reader.sharedPreparedSearch.holder,
		schemaHash: overlay.base.catalog.meta.Options.ColumnStore.SchemaHash,
	}
	return &typedGraphPreparedFilter{
		overlay:            overlay,
		base:               selection,
		count:              selection.Count(),
		borrowedBaseFilter: base,
		cachedBasePlan:     origin,
	}
}

func typedGraphScalarU8Options4684(f typedGraphScalarU8Fixture4684, plan *typedGraphPreparedFilter) typedGraphScalarU8TraversalOptions {
	return typedGraphScalarU8TraversalOptions{
		TopK:               2,
		EfSearch:           3,
		ScoreBudget:        32,
		QuantizedIndexName: f.indexName,
		PreparedFilter:     plan,
		StatsMode:          columnVectorGraphNativeSearchStatsModeFullDiagnostics,
	}
}

func typedGraphScalarU8ExpectedHigh4684(f typedGraphScalarU8Fixture4684) []int {
	got := []int{f.docOrdinals[2], f.docOrdinals[4]}
	sort.Ints(got)
	return got
}

func typedGraphScalarU8ExpectedDisconnectedSeeds4684(f typedGraphScalarU8Fixture4684) []int {
	seeds := typedGraphScalarU8DispersedRows4684(f)[:2]
	sort.Slice(seeds, func(i, j int) bool {
		class := func(ordinal int) int {
			if ordinal == f.docOrdinals[2] || ordinal == f.docOrdinals[4] {
				return 1
			}
			return 0
		}
		if class(seeds[i]) != class(seeds[j]) {
			return class(seeds[i]) > class(seeds[j])
		}
		return seeds[i] < seeds[j]
	})
	return seeds
}

func assertTypedGraphScalarU8CandidateOnly4684(t *testing.T, got []columnVectorGraphNativeSearchResult, stats columnVectorGraphNativeSearchStats) {
	t.Helper()
	if stats.QuantizedScoreCalls == 0 || stats.PreparedScoreCalls != 0 || stats.FP32ScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 || stats.ResultFetches != 0 {
		t.Fatalf("scalar candidate stats=%+v want quantized-only code work without fp32/vector/norm/result fetch", stats)
	}
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 0 {
		t.Fatalf("private candidate collector claimed a public quantized route: %+v", stats)
	}
	for i, result := range got {
		if len(result.ID) != 0 || result.HasRowRef {
			t.Fatalf("candidate[%d]=%+v materialized document identity", i, result)
		}
	}
}

func assertTypedGraphScalarU8ScratchCleared4684(t *testing.T, got []columnVectorGraphNativeSearchResult, scratch *columnVectorGraphNativeSearchScratch) {
	t.Helper()
	if len(got) != 0 || len(scratch.results) != 0 || len(scratch.top) != 0 || len(scratch.frontier) != 0 {
		t.Fatalf("partial shortlist escaped got=%+v scratch results/top/frontier=%d/%d/%d", got, len(scratch.results), len(scratch.top), len(scratch.frontier))
	}
	assertTypedGraphScalarU8ScratchReferencesCleared4684(t, scratch)
}

func assertTypedGraphScalarU8ScratchReferencesCleared4684(t *testing.T, scratch *columnVectorGraphNativeSearchScratch) {
	t.Helper()
	if scratch == nil {
		t.Fatal("nil scalar-u8 scratch")
	}
	admission := scratch.typedScalarU8Admission
	plane := scratch.typedScalarU8Plane
	if admission.hasCandidateRows || admission.candidateRows.Rows() != 0 || admission.candidateRows.Count() != 0 || len(admission.excludedBaseOrdinals) != 0 || len(admission.localToBase) != 0 || plane.ctx != nil || plane.base != nil || len(plane.ordinals) != 0 {
		t.Fatalf("typed scalar-u8 scratch retained borrowed state admission=%+v plane=%+v", admission, plane)
	}
}

func TestTypedGraphScalarU8PreparedCandidatesFilterAdmission4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	selection := typedGraphScalarU8Selection4684(t, fixture, 0, 2, 4)

	t.Run("unfiltered_base_fast_path", func(t *testing.T) {
		overlay := typedGraphScalarU8Overlay4684(fixture, typedGraphScalarU8ConnectedPack4684(fixture))
		var scratch columnVectorGraphNativeSearchScratch
		got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, typedGraphScalarU8Options4684(fixture, nil), &scratch)
		if err != nil {
			t.Fatal(err)
		}
		want := typedGraphScalarU8ExpectedHigh4684(fixture)
		if len(got) != len(want) || got[0].Ordinal != want[0] || got[1].Ordinal != want[1] {
			t.Fatalf("unfiltered results=%+v want base ordinals %v", got, want)
		}
		assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)
		assertTypedGraphScalarU8ScratchReferencesCleared4684(t, &scratch)
	})

	t.Run("dispersed_base_filter_and_exclusion", func(t *testing.T) {
		overlay := typedGraphScalarU8Overlay4684(fixture, typedGraphScalarU8ConnectedPack4684(fixture))
		plan := typedGraphScalarU8DirectPlan4684(overlay, selection)
		var scratch columnVectorGraphNativeSearchScratch
		opts := typedGraphScalarU8Options4684(fixture, plan)
		got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		want := typedGraphScalarU8ExpectedHigh4684(fixture)
		if len(got) != len(want) || got[0].Ordinal != want[0] || got[1].Ordinal != want[1] {
			t.Fatalf("dispersed filter results=%+v want base ordinals %v", got, want)
		}
		if stats.FilteredIneligibleScores == 0 || stats.FilteredFrontierPeak == 0 || stats.CandidateRows != 3 {
			t.Fatalf("filter admission stats=%+v", stats)
		}
		assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)
		assertTypedGraphScalarU8ScratchReferencesCleared4684(t, &scratch)

		// Reuse the same caller scratch for an unfiltered request. The private
		// collector must not retain the preceding typed filter/navigation state.
		got, stats, err = overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, typedGraphScalarU8Options4684(fixture, nil), &scratch)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) || got[0].Ordinal != want[0] || got[1].Ordinal != want[1] {
			t.Fatalf("filtered-to-unfiltered results=%+v want base ordinals %v", got, want)
		}
		assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)
		assertTypedGraphScalarU8ScratchReferencesCleared4684(t, &scratch)

		plan.excludedBase = []int{fixture.docOrdinals[2]}
		got, stats, err = overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Ordinal != fixture.docOrdinals[4] || got[1].Ordinal != fixture.docOrdinals[0] {
			t.Fatalf("exclusion results=%+v want base ordinals [%d %d]", got, fixture.docOrdinals[4], fixture.docOrdinals[0])
		}
		if stats.FilteredIneligibleScores < 2 || stats.CandidateRows != 2 {
			t.Fatalf("exclusion admission stats=%+v", stats)
		}
		assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)
		assertTypedGraphScalarU8ScratchReferencesCleared4684(t, &scratch)
	})

	t.Run("disconnected_selected_seeds", func(t *testing.T) {
		overlay := typedGraphScalarU8Overlay4684(fixture, typedGraphScalarU8DisconnectedPack4684(fixture))
		plan := typedGraphScalarU8DirectPlan4684(overlay, selection)
		var scratch columnVectorGraphNativeSearchScratch
		opts := typedGraphScalarU8Options4684(fixture, plan)
		opts.EfSearch = 2
		got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		assertTypedGraphScalarU8ScratchReferencesCleared4684(t, &scratch)
		want := typedGraphScalarU8ExpectedDisconnectedSeeds4684(fixture)
		if len(got) != len(want) || got[0].Ordinal != want[0] || got[1].Ordinal != want[1] || stats.FilteredSeedInspections != 2 {
			t.Fatalf("disconnected filtered seeds results=%+v stats=%+v want ordinals %v and two selected seed inspections", got, stats, want)
		}
		assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)
	})
}

func TestTypedGraphScalarU8PreparedCandidatesMappedNavigationBudget4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	selection := typedGraphScalarU8Selection4684(t, fixture, 0, 2, 4)
	overlay := fixture.overlay
	navigation := typedGraphScalarU8Navigation4684(fixture, overlay.pack, true)
	plan := typedGraphScalarU8CachedPlan4684(t, overlay, selection, navigation)
	var scratch columnVectorGraphNativeSearchScratch
	opts := typedGraphScalarU8Options4684(fixture, plan)
	warmOpts := opts
	warmOpts.ScoreBudget = 32
	if warm, _, warmErr := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, warmOpts, &scratch); warmErr != nil || len(warm) == 0 {
		t.Fatalf("warm mapped traversal results=%+v err=%v", warm, warmErr)
	}
	opts.ScoreBudget = 1 // entry score fits; the upper-layer tile of two cannot.

	got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
	if !errors.Is(err, errTypedGraphSearchBudget) {
		t.Fatalf("batch budget err=%v want %v", err, errTypedGraphSearchBudget)
	}
	assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
	if stats.QuantizedScoreCalls != 1 || stats.PreparedScoreCalls != 0 || stats.Candidates != 0 {
		t.Fatalf("batch budget stats=%+v want only the attempted upper entry scalar score", stats)
	}
	if warm, _, warmErr := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, warmOpts, &scratch); warmErr != nil || len(warm) == 0 {
		t.Fatalf("second warm mapped traversal results=%+v err=%v", warm, warmErr)
	}
	// This is an intentionally early failure: it returns before the collector
	// installs the next request's filter/plane. Reuse must still clear the prior
	// mapped request's borrowed state.
	preCanceled := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 1}
	got, _, err = overlay.searchScalarU8PreparedCandidatesWithContext(preCanceled, fixture.query, warmOpts, &scratch)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled traversal err=%v want cancellation", err)
	}
	assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
}

func TestTypedGraphScalarU8PreparedCandidatesBudgetBoundary4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	selection := typedGraphScalarU8Selection4684(t, fixture, 0, 2, 4)
	overlay := typedGraphScalarU8Overlay4684(fixture, typedGraphScalarU8DisconnectedPack4684(fixture))
	plan := typedGraphScalarU8DirectPlan4684(overlay, selection)
	opts := typedGraphScalarU8Options4684(fixture, plan)
	opts.EfSearch = 2
	opts.ScoreBudget = 3 // ineligible entry plus the two selected disconnected seeds.
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
	if !errors.Is(err, errTypedGraphSearchBudget) || stats.QuantizedScoreCalls != uint64(opts.ScoreBudget) {
		t.Fatalf("exact budget boundary err=%v stats=%+v want exhausted after %d scalar calls", err, stats, opts.ScoreBudget)
	}
	assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
}

func TestTypedGraphScalarU8PreparedCandidatesNavigationBinding4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	selection := typedGraphScalarU8Selection4684(t, fixture, 0, 2, 4)
	navigation := typedGraphScalarU8Navigation4684(fixture, fixture.overlay.pack, false)
	plan := typedGraphScalarU8CachedPlan4684(t, fixture.overlay, selection, navigation)
	opts := typedGraphScalarU8Options4684(fixture, plan)
	var scratch columnVectorGraphNativeSearchScratch
	if warm, _, err := fixture.overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch); err != nil || len(warm) == 0 {
		t.Fatalf("warm valid mapped traversal results=%+v err=%v", warm, err)
	}

	wrongSelection := typedGraphScalarU8Selection4684(t, fixture, 0, 1, 3)
	wrongPlan := &typedGraphPreparedFilter{
		overlay:            fixture.overlay,
		base:               wrongSelection,
		count:              wrongSelection.Count(),
		borrowedBaseFilter: plan.borrowedBaseFilter,
		cachedBasePlan:     &typedGraphPreparedFilter{base: wrongSelection, count: wrongSelection.Count()},
	}
	wrongOpts := opts
	wrongOpts.PreparedFilter = wrongPlan
	got, stats, err := fixture.overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, wrongOpts, &scratch)
	if !errors.Is(err, ErrVectorIndexSnapshotMismatch) || stats.QuantizedScoreCalls != 0 {
		t.Fatalf("same-cardinality mismatched selection err=%v stats=%+v", err, stats)
	}
	assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
	if warm, _, err := fixture.overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch); err != nil || len(warm) == 0 {
		t.Fatalf("valid bound wrapper did not recover/reuse scratch results=%+v err=%v", warm, err)
	}

	otherOverlay := typedGraphScalarU8Overlay4684(fixture, typedGraphScalarU8ConnectedPack4684(fixture))
	otherPlan := typedGraphScalarU8CachedPlan4684(t, otherOverlay, selection, navigation)
	otherOpts := typedGraphScalarU8Options4684(fixture, otherPlan)
	got, stats, err = otherOverlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, otherOpts, &scratch)
	if !errors.Is(err, ErrVectorIndexSnapshotMismatch) || stats.QuantizedScoreCalls != 0 {
		t.Fatalf("same-shape different base pin err=%v stats=%+v", err, stats)
	}
	assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
}

func TestTypedGraphScalarU8PreparedCandidatesMappedNavigation4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	selection := typedGraphScalarU8Selection4684(t, fixture, 0, 2, 4)
	overlay := fixture.overlay
	plan := typedGraphScalarU8CachedPlan4684(t, overlay, selection, typedGraphScalarU8Navigation4684(fixture, overlay.pack, false))
	var scratch columnVectorGraphNativeSearchScratch
	opts := typedGraphScalarU8Options4684(fixture, plan)

	got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
	if err != nil {
		t.Fatal(err)
	}
	// Local ordinal 1 maps to doc-4 and local ordinal 2 maps to doc-2. Their
	// scalar-u8 scores tie, so local-order tie handling must choose doc-4 first.
	if len(got) != 2 || got[0].Ordinal != fixture.docOrdinals[4] || got[1].Ordinal != fixture.docOrdinals[2] {
		t.Fatalf("mapped navigation results=%+v want base ordinals [%d %d]", got, fixture.docOrdinals[4], fixture.docOrdinals[2])
	}
	assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)

	plan.excludedBase = []int{fixture.docOrdinals[4]}
	got, stats, err = overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Ordinal != fixture.docOrdinals[2] || got[1].Ordinal != fixture.docOrdinals[0] || stats.FilteredIneligibleScores == 0 {
		t.Fatalf("mapped exclusion results=%+v stats=%+v want base ordinals [%d %d] with local exclusion admission", got, stats, fixture.docOrdinals[2], fixture.docOrdinals[0])
	}
	assertTypedGraphScalarU8CandidateOnly4684(t, got, stats)
}

func TestTypedGraphScalarU8PreparedCandidatesCancellationPhases4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	selection := typedGraphScalarU8Selection4684(t, fixture, 0, 2, 4)

	t.Run("upper_layer", func(t *testing.T) {
		navigation := typedGraphScalarU8Navigation4684(fixture, fixture.overlay.pack, true)
		plan := typedGraphScalarU8CachedPlan4684(t, fixture.overlay, selection, navigation)
		opts := typedGraphScalarU8Options4684(fixture, plan)
		canceled := false
		for check := 1; check < 96; check++ {
			ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
			var scratch columnVectorGraphNativeSearchScratch
			got, stats, err := fixture.overlay.searchScalarU8PreparedCandidatesWithContext(ctx, fixture.query, opts, &scratch)
			if !errors.Is(err, context.Canceled) || stats.QuantizedScoreCalls == 0 || stats.Candidates != 0 {
				continue
			}
			if stats.PreparedScoreCalls != 0 {
				t.Fatalf("upper cancellation used fp32 scores: %+v", stats)
			}
			assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
			canceled = true
			break
		}
		if !canceled {
			t.Fatal("no deterministic upper-layer cancellation point after scalar-u8 work")
		}
	})

	t.Run("mapped_layer0_batch", func(t *testing.T) {
		navigation := typedGraphScalarU8Navigation4684(fixture, fixture.overlay.pack, false)
		plan := typedGraphScalarU8CachedPlan4684(t, fixture.overlay, selection, navigation)
		opts := typedGraphScalarU8Options4684(fixture, plan)
		canceled := false
		for check := 1; check < 96; check++ {
			ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
			var scratch columnVectorGraphNativeSearchScratch
			got, stats, err := fixture.overlay.searchScalarU8PreparedCandidatesWithContext(ctx, fixture.query, opts, &scratch)
			if !errors.Is(err, context.Canceled) || stats.QuantizedScoreCalls < 3 || stats.Candidates < 3 || stats.ScoreBatchMaxTileSize < 2 {
				continue
			}
			if stats.PreparedScoreCalls != 0 {
				t.Fatalf("mapped batch cancellation used fp32 scores: %+v", stats)
			}
			assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
			canceled = true
			break
		}
		if !canceled {
			t.Fatal("no deterministic mapped layer-0 batch cancellation point after scalar-u8 work")
		}
	})
}

func TestTypedGraphScalarU8PreparedCandidatesRejectNonLegacyCalibration4684(t *testing.T) {
	fixture := openTypedGraphScalarU8Fixture4684(t)
	fixture.overlay.base.reader.def.QuantizedIndexes[0].ScalarU8Calibration = &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModePerGranuleAlpha}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := fixture.overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, typedGraphScalarU8Options4684(fixture, nil), &scratch)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || stats.QuantizedScoreCalls != 0 || stats.QuantizedScorerActive != 0 {
		t.Fatalf("nonlegacy scalar-u8 candidate collection err=%v stats=%+v", err, stats)
	}
	assertTypedGraphScalarU8ScratchCleared4684(t, got, &scratch)
}

func BenchmarkTypedGraphScalarU8PreparedCandidatesFilteredBudget4684(b *testing.B) {
	fixture := openTypedGraphScalarU8Fixture4684(b)
	selection := typedGraphScalarU8Selection4684(b, fixture, 0, 2, 4)
	overlay := typedGraphScalarU8Overlay4684(fixture, typedGraphScalarU8ConnectedPack4684(fixture))
	opts := typedGraphScalarU8Options4684(fixture, typedGraphScalarU8DirectPlan4684(overlay, selection))
	var scratch columnVectorGraphNativeSearchScratch
	if got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch); err != nil || len(got) != opts.TopK || stats.QuantizedScoreCalls == 0 || stats.QuantizedScoreCalls >= uint64(opts.ScoreBudget) {
		b.Fatalf("warm filtered strict-budget candidate collection results=%+v stats=%+v err=%v", got, stats, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, stats, err := overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), fixture.query, opts, &scratch)
		if err != nil || len(got) != opts.TopK || stats.QuantizedScoreCalls == 0 || stats.QuantizedScoreCalls >= uint64(opts.ScoreBudget) {
			b.Fatalf("filtered strict-budget candidate collection results=%+v stats=%+v err=%v", got, stats, err)
		}
	}
}

func typedGraphScalarU8Overlay4684(f typedGraphScalarU8Fixture4684, pack *columnHNSWSearchPackPreparedView) *typedGraphOverlaySearch {
	return &typedGraphOverlaySearch{base: f.overlay.base, pack: pack, current: f.overlay.current}
}
