package collections

import "testing"

func TestColumnVectorGraphPreparedExactIndexedScoringEquivalence2098(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("prepared indexed scoring equivalence requires mmap_direct prepared views")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	shape.efSearch = shape.rows
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()

	scalarResults, scalarStats := searchColumnVectorGraphPreparedScoreMode2098(t, reader, query, shape, columnVectorGraphScoreBatchModeScalar)
	indexedResults, indexedStats := searchColumnVectorGraphPreparedScoreMode2098(t, reader, query, shape, columnVectorGraphScoreBatchModeIndexed)
	if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(indexedResults, scalarResults); mismatch != "" {
		t.Fatal(mismatch)
	}
	assertColumnVectorGraphPreparedExactIndexedWorkStatsEqual2098(t, scalarStats, indexedStats)
	if indexedStats.ScoreBatchMaxTileSize < 2 || scalarStats.ScoreBatchMaxTileSize != indexedStats.ScoreBatchMaxTileSize || scalarStats.ScoreBatchCalls != indexedStats.ScoreBatchCalls {
		t.Fatalf("indexed stats=%+v scalar stats=%+v want exact prepared scoring modes to preserve neighbor-tile topology", indexedStats, scalarStats)
	}
	if scalarStats.ScoreBatchOptimizedCalls != 0 || scalarStats.ScoreBatchScalarFallbackCalls != scalarStats.ScoreBatchCalls {
		t.Fatalf("indexed stats=%+v scalar stats=%+v want scalar mode to use tile fallback backend", indexedStats, scalarStats)
	}
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(t, indexedStats.ScoreBatchOptimizedCalls, indexedStats.ScoreBatchScalarFallbackCalls, int(indexedStats.ScoreBatchMaxTileSize), shape.dims)
}

func TestColumnVectorGraphPreparedExactIndexedScoringTieOrder2098(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("prepared indexed scoring tie-order test requires mmap_direct prepared views")
	}
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1}, InvNorm: 1, Adjacency: []uint32{2, 1, 3, 4}},
		{ID: []byte("doc-tie-low-ordinal"), Vector: []float32{1, 0}, InvNorm: 1},
		{ID: []byte("doc-tie-high-ordinal"), Vector: []float32{1, 0}, InvNorm: 1},
		{ID: []byte("doc-opposite-y"), Vector: []float32{0, -1}, InvNorm: 1},
		{ID: []byte("doc-opposite-x"), Vector: []float32{-1, 0}, InvNorm: 1},
	}
	shape := columnVectorGraphSearchTopologyParityShape2091{rows: len(rows), dims: 2, degree: 4, topK: 2, efSearch: len(rows), queryOrdinal: 1}
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()

	scalarResults, scalarStats := searchColumnVectorGraphPreparedScoreMode2098(t, reader, query, shape, columnVectorGraphScoreBatchModeScalar)
	indexedResults, indexedStats := searchColumnVectorGraphPreparedScoreMode2098(t, reader, query, shape, columnVectorGraphScoreBatchModeIndexed)
	if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(indexedResults, scalarResults); mismatch != "" {
		t.Fatal(mismatch)
	}
	wantOrdinals := []int{1, 2}
	for i, want := range wantOrdinals {
		if i >= len(indexedResults) || indexedResults[i].Ordinal != want {
			t.Fatalf("indexed results=%+v want ordinal %d at rank %d", indexedResults, want, i)
		}
	}
	assertColumnVectorGraphPreparedExactIndexedWorkStatsEqual2098(t, scalarStats, indexedStats)
	if indexedStats.ScoreBatchMaxTileSize < 2 {
		t.Fatalf("indexed stats=%+v want tie fixture to exercise gathered score batch", indexedStats)
	}
	if indexedStats.ScoreBatchOptimizedCalls != 0 || indexedStats.ScoreBatchScalarFallbackCalls == 0 {
		t.Fatalf("indexed stats=%+v want tiny-dims prepared indexed batch to use observable scalar fallback", indexedStats)
	}
}

func searchColumnVectorGraphPreparedScoreMode2098(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, query []float32, shape columnVectorGraphSearchTopologyParityShape2091, mode columnVectorGraphScoreBatchMode) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats) {
	tb.Helper()
	opts := columnVectorGraphSearchTopologyParityOptions2091(shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091)
	opts.StatsMode = columnVectorGraphNativeSearchStatsModeBenchmarkDebug
	opts.ScoreBatchMode = mode
	var scratch columnVectorGraphNativeSearchScratch
	results, stats, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		tb.Fatalf("SearchCosine score mode=%s: %v", mode.String(), err)
	}
	if len(results) == 0 {
		tb.Fatalf("SearchCosine score mode=%s returned no results", mode.String())
	}
	assertColumnVectorGraphBenchmarkDebugStats1979(tb, stats, true)
	assertColumnVectorGraphSearchTopologyParityCurrentStats2091(tb, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, stats)
	return cloneColumnVectorGraphPreparedResults2045(results), stats
}

func assertColumnVectorGraphPreparedExactIndexedWorkStatsEqual2098(tb testing.TB, scalar, indexed columnVectorGraphNativeSearchStats) {
	tb.Helper()
	checks := []struct {
		name    string
		scalar  uint64
		indexed uint64
	}{
		{name: "candidate_rows", scalar: scalar.CandidateRows, indexed: indexed.CandidateRows},
		{name: "candidates", scalar: scalar.Candidates, indexed: indexed.Candidates},
		{name: "edges", scalar: scalar.Edges, indexed: indexed.Edges},
		{name: "visited_edges", scalar: scalar.VisitedEdges, indexed: indexed.VisitedEdges},
		{name: "visited_nodes", scalar: scalar.VisitedNodes, indexed: indexed.VisitedNodes},
		{name: "candidate_fetches", scalar: scalar.CandidateFetches, indexed: indexed.CandidateFetches},
		{name: "prepared_score_calls", scalar: scalar.PreparedScoreCalls, indexed: indexed.PreparedScoreCalls},
		{name: "score_batch_candidates", scalar: scalar.ScoreBatchCandidates, indexed: indexed.ScoreBatchCandidates},
		{name: "layer0_scores", scalar: scalar.Layer0Scores, indexed: indexed.Layer0Scores},
		{name: "layer0_scored_neighbors", scalar: scalar.Layer0ScoredNeighbors, indexed: indexed.Layer0ScoredNeighbors},
		{name: "frontier_pushes", scalar: scalar.FrontierPushes, indexed: indexed.FrontierPushes},
		{name: "top_k_insert_attempts", scalar: scalar.TopKInsertAttempts, indexed: indexed.TopKInsertAttempts},
		{name: "exact_order_observations", scalar: scalar.ExactCandidateOrderObservations, indexed: indexed.ExactCandidateOrderObservations},
		{name: "exact_order_transitions", scalar: scalar.ExactCandidateOrderTransitions, indexed: indexed.ExactCandidateOrderTransitions},
		{name: "exact_order_adjacent_forward", scalar: scalar.ExactCandidateOrderAdjacentForward, indexed: indexed.ExactCandidateOrderAdjacentForward},
		{name: "exact_order_non_adjacent_forward", scalar: scalar.ExactCandidateOrderNonAdjacentForward, indexed: indexed.ExactCandidateOrderNonAdjacentForward},
		{name: "exact_order_backward_jumps", scalar: scalar.ExactCandidateOrderBackwardJumps, indexed: indexed.ExactCandidateOrderBackwardJumps},
		{name: "exact_order_max_forward_run", scalar: scalar.ExactCandidateOrderMaxForwardRun, indexed: indexed.ExactCandidateOrderMaxForwardRun},
		{name: "vector_prepared_identity_mapping", scalar: scalar.VectorPreparedIdentityMappings, indexed: indexed.VectorPreparedIdentityMappings},
		{name: "vector_prepared_row_ref_mapping", scalar: scalar.VectorPreparedRowRefMappings, indexed: indexed.VectorPreparedRowRefMappings},
		{name: "graph_row_fallbacks", scalar: scalar.GraphRowFallbacks, indexed: indexed.GraphRowFallbacks},
		{name: "typed_column_fallbacks", scalar: scalar.TypedColumnFallbacks, indexed: indexed.TypedColumnFallbacks},
		{name: "norm_source_fallbacks", scalar: scalar.NormSourceFallbacks, indexed: indexed.NormSourceFallbacks},
	}
	for _, check := range checks {
		if check.scalar != check.indexed {
			tb.Fatalf("%s scalar=%d indexed=%d scalarStats=%+v indexedStats=%+v", check.name, check.scalar, check.indexed, scalar, indexed)
		}
	}
	if indexed.GraphRowFallbacks != 0 || indexed.TypedColumnFallbacks != 0 || indexed.VectorScratchDecodes != 0 || indexed.NormScratchDecodes != 0 || indexed.AdjacencySourceFallbacks != 0 {
		tb.Fatalf("indexed stats=%+v want healthy prepared sources with no graph-row/source fallback", indexed)
	}
}
