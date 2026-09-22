package collections

import (
	"bytes"
	"math"
	"slices"
	"testing"
)

func TestScalarU8RawDotCandidateOrderingMatchesFloatScores2658(t *testing.T) {
	candidates := []columnVectorGraphRawDotSearchCandidate{
		{ordinal: 6, dot: 9},
		{ordinal: 4, dot: 21},
		{ordinal: 2, dot: 21},
		{ordinal: 7, dot: -3},
		{ordinal: 1, dot: 65025},
		{ordinal: 3, dot: 65024},
		{ordinal: 0, dot: 21},
		{ordinal: 5, dot: 9},
	}
	const limit = 5
	var rawTop, floatTop columnVectorGraphNativeSearchScratch
	rawTop.prepareRawDotCandidateQueues(len(candidates), 1, limit, limit)
	for _, raw := range candidates {
		floatCandidate := columnVectorGraphSearchCandidate{ordinal: raw.ordinal, score: scalarU8QuantizedCosineScoreFromDot(raw.dot)}
		rawInserted := rawTop.insertRawDotTop(limit, raw)
		floatInserted := floatTop.insertTop(limit, floatCandidate)
		if rawInserted != floatInserted {
			t.Fatalf("insert raw=%+v inserted=%v floatInserted=%v", raw, rawInserted, floatInserted)
		}
	}
	for _, raw := range candidates {
		floatCandidate := columnVectorGraphSearchCandidate{ordinal: raw.ordinal, score: scalarU8QuantizedCosineScoreFromDot(raw.dot)}
		got := columnVectorGraphLayer0RawDotSearchShouldStop(raw, rawTop.rawDot.top, limit)
		want := columnVectorGraphLayer0SearchShouldStop(floatCandidate, floatTop.top, limit)
		if got != want {
			t.Fatalf("stop raw=%+v got=%v want float=%v", raw, got, want)
		}
	}
	rawTop.promoteRawDotTopToFloat(limit)
	floatTop.sortTopBestFirst()
	if len(rawTop.top) != len(floatTop.top) {
		t.Fatalf("raw top=%d float top=%d", len(rawTop.top), len(floatTop.top))
	}
	for i := range floatTop.top {
		got := rawTop.top[i]
		want := floatTop.top[i]
		if got.ordinal != want.ordinal || got.score != want.score {
			t.Fatalf("top[%d] raw-promoted=%+v want float=%+v", i, got, want)
		}
	}

	var rawHeap, floatHeap columnVectorGraphNativeSearchScratch
	rawHeap.prepareRawDotCandidateQueues(len(candidates), 1, limit, limit)
	for _, raw := range candidates {
		rawHeap.pushRawDotFrontier(raw)
		floatHeap.pushFrontier(columnVectorGraphSearchCandidate{ordinal: raw.ordinal, score: scalarU8QuantizedCosineScoreFromDot(raw.dot)})
	}
	for i := 0; ; i++ {
		raw, rawOK := rawHeap.popRawDotFrontier()
		floatCandidate, floatOK := floatHeap.popFrontier()
		if rawOK != floatOK {
			t.Fatalf("pop %d rawOK=%v floatOK=%v", i, rawOK, floatOK)
		}
		if !rawOK {
			break
		}
		if raw.ordinal != floatCandidate.ordinal || scalarU8QuantizedCosineScoreFromDot(raw.dot) != floatCandidate.score {
			t.Fatalf("pop[%d] raw=%+v float=%+v", i, raw, floatCandidate)
		}
	}
}

func TestScalarU8RawDotTopRetention4136(t *testing.T) {
	for _, limit := range []int{1, 10, 64, 256} {
		var scratch columnVectorGraphNativeSearchScratch
		scratch.prepareRawDotCandidateQueues(4096, 16, limit, limit)
		var want []columnVectorGraphRawDotSearchCandidate
		state := uint64(0x4136_2658_9e37_79b9)
		for i := 0; i < 4096; i++ {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			candidate := columnVectorGraphRawDotSearchCandidate{ordinal: int(state % 769), dot: int64(state>>32) % 31}
			scratch.insertRawDotTop(limit, candidate)
			want = append(want, candidate)
			slices.SortFunc(want, compareColumnVectorGraphRawDotSearchCandidates)
			if len(want) > limit {
				want = want[:limit]
			}
		}
		scratch.retainRawDotTopBestFirst(limit)
		if !slices.Equal(scratch.rawDot.top, want) {
			t.Fatalf("limit=%d retained=%+v want=%+v", limit, scratch.rawDot.top, want)
		}
	}
}

func TestScalarU8PreparedTraversalRawDotQuantizedOnlyParity2658(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{1000, 1, 0}}, // quantizes to the same scalar_u8 codes as doc-a for the query below.
		{id: "doc-c", vector: []float32{0.8, 0.6, 0}},
		{id: "doc-d", vector: []float32{4, 3, 0}}, // quantized tie with doc-c.
		{id: "doc-e", vector: []float32{0, 1, 0}},
		{id: "doc-f", vector: []float32{0, 0, 1}}, // quantized tie with doc-e; top-k boundary keeps lower ordinal.
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	packSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher pack: %v", err)
	}
	defer func() { _ = packSearcher.Close() }()
	fallbackSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher fallback: %v", err)
	}
	defer func() { _ = fallbackSearcher.Close() }()
	if packSearcher.reader == nil || packSearcher.reader.hnswSearchPack == nil || packSearcher.reader.adjacencyLayerSources == nil {
		t.Fatalf("pack searcher missing prepared pack or adjacency state")
	}
	packStatus := packSearcher.reader.hnswSearchPack.fastStatus(packSearcher.reader.hnswSearchPackStatus)
	if packStatus != columnHNSWSearchPackPreparedStatusDirect && packStatus != columnHNSWSearchPackPreparedStatusHeap {
		t.Fatalf("pack status=%s", packStatus)
	}
	packSearcher.reader.preparedSearch = nil
	fallbackSearcher.reader.hnswSearchPack = nil

	query := []float32{1, 0, 0}
	topK := 5
	opts := VectorIndexSearcherSearchOptions{
		Query:              query,
		QueryMode:          VectorIndexQueryModeQuantizedOnly,
		QuantizedIndexName: def.QuantizedIndexes[0].Name,
		TopK:               topK,
		EfSearch:           len(rows),
		StatsMode:          VectorIndexSearchStatsModeProduction,
	}
	var packBuffer, fallbackBuffer VectorIndexSearchBuffer
	packResults, err := packSearcher.SearchWithBuffer(opts, &packBuffer)
	if err != nil {
		t.Fatalf("pack raw-dot SearchWithBuffer: %v", err)
	}
	fallbackResults, err := fallbackSearcher.SearchWithBuffer(opts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("fallback float SearchWithBuffer: %v", err)
	}
	fixtureTop := scalarU8QuantizedTopKForTest1926(t, rows, query, topK)
	if len(fixtureTop) < 2 || fixtureTop[0].Score != fixtureTop[1].Score || !bytes.Equal(fixtureTop[0].ID, []byte("doc-a")) || !bytes.Equal(fixtureTop[1].ID, []byte("doc-b")) {
		t.Fatalf("test fixture top tie got=%+v want doc-a/doc-b equal-score tie", fixtureTop)
	}
	if len(packResults.Results) != len(fallbackResults.Results) {
		t.Fatalf("pack results=%d fallback results=%d", len(packResults.Results), len(fallbackResults.Results))
	}
	assertQuantizedOnlyGuardrailStats2416(t, packResults.Stats, def.Dimensions)
	assertScalarU8PreparedTraversalPackAdjacencyStats2586(t, packResults.Stats, packStatus, "raw-dot quantized_only")
	if packResults.Stats.Candidates != 0 || packResults.Stats.Edges != 0 || packResults.Stats.VisitedEdges != 0 {
		t.Fatalf("production pack stats=%+v want no traversal diagnostics", packResults.Stats)
	}
	fullOpts := opts
	fullOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	packFull, err := packSearcher.SearchWithBuffer(fullOpts, &packBuffer)
	if err != nil {
		t.Fatalf("full diagnostics pack quantized_only SearchWithBuffer: %v", err)
	}
	fallbackFull, err := fallbackSearcher.SearchWithBuffer(fullOpts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("full diagnostics fallback quantized_only SearchWithBuffer: %v", err)
	}
	if packFull.Stats.Candidates != fallbackFull.Stats.Candidates || packFull.Stats.VisitedEdges != fallbackFull.Stats.VisitedEdges || packFull.Stats.QuantizedScoreCalls != fallbackFull.Stats.QuantizedScoreCalls {
		t.Fatalf("full pack stats=%+v fallback stats=%+v want same quantized traversal counters", packFull.Stats, fallbackFull.Stats)
	}
	for i := range packResults.Results {
		got := packResults.Results[i]
		wantResult := fallbackResults.Results[i]
		if got.Ordinal != wantResult.Ordinal || !bytes.Equal(got.ID, wantResult.ID) || math.Abs(got.Score-wantResult.Score) > 0 {
			t.Fatalf("quantized_only result[%d] pack=%+v fallback=%+v", i, got, wantResult)
		}
	}
	rawOnlyResults := searchScalarU8PreparedRawDotResultsForTest2658(t, packSearcher.reader, query, columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, topK, len(rows), 0)
	assertScalarU8PreparedRawDotNativeResultsMatchPublic2658(t, rawOnlyResults, fallbackResults.Results, 0, "quantized_only raw-dot direct")

	rerankOpts := opts
	rerankOpts.QueryMode = VectorIndexQueryModeQuantizedRerank
	rerankOpts.TopK = 3
	rerankOpts.QuantizedRerankCandidates = 4
	packRerank, err := packSearcher.SearchWithBuffer(rerankOpts, &packBuffer)
	if err != nil {
		t.Fatalf("pack raw-dot quantized_rerank SearchWithBuffer: %v", err)
	}
	fallbackRerank, err := fallbackSearcher.SearchWithBuffer(rerankOpts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("fallback quantized_rerank SearchWithBuffer: %v", err)
	}
	assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(t, packRerank.Stats, rerankOpts.QuantizedRerankCandidates, def.Dimensions)
	assertQuantizedRerankNoDocumentGuardrailStats2416(t, fallbackRerank.Stats, rerankOpts.QuantizedRerankCandidates)
	if packRerank.Stats.Candidates != 0 || packRerank.Stats.Edges != 0 || packRerank.Stats.VisitedEdges != 0 {
		t.Fatalf("production rerank pack stats=%+v want no traversal diagnostics", packRerank.Stats)
	}
	fullRerankOpts := rerankOpts
	fullRerankOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	packRerankFull, err := packSearcher.SearchWithBuffer(fullRerankOpts, &packBuffer)
	if err != nil {
		t.Fatalf("full diagnostics pack quantized_rerank SearchWithBuffer: %v", err)
	}
	fallbackRerankFull, err := fallbackSearcher.SearchWithBuffer(fullRerankOpts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("full diagnostics fallback quantized_rerank SearchWithBuffer: %v", err)
	}
	if packRerankFull.Stats.Candidates != fallbackRerankFull.Stats.Candidates || packRerankFull.Stats.VisitedEdges != fallbackRerankFull.Stats.VisitedEdges || packRerankFull.Stats.QuantizedScoreCalls != fallbackRerankFull.Stats.QuantizedScoreCalls || packRerankFull.Stats.QuantizedRerankExactScoreCalls != fallbackRerankFull.Stats.QuantizedRerankExactScoreCalls {
		t.Fatalf("full rerank pack stats=%+v fallback stats=%+v want same raw-dot traversal and rerank counters", packRerankFull.Stats, fallbackRerankFull.Stats)
	}
	if len(packRerank.Results) != len(fallbackRerank.Results) {
		t.Fatalf("rerank pack results=%d fallback results=%d", len(packRerank.Results), len(fallbackRerank.Results))
	}
	for i := range packRerank.Results {
		got := packRerank.Results[i]
		wantResult := fallbackRerank.Results[i]
		if got.Ordinal != wantResult.Ordinal || !bytes.Equal(got.ID, wantResult.ID) || math.Abs(got.Score-wantResult.Score) > 1e-6 {
			t.Fatalf("quantized_rerank result[%d] pack=%+v fallback=%+v", i, got, wantResult)
		}
	}
	rawRerankResults := searchScalarU8PreparedRawDotResultsForTest2658(t, packSearcher.reader, query, columnVectorGraphNativeSearchQueryModeQuantizedRerank, def.QuantizedIndexes[0].Name, rerankOpts.TopK, len(rows), rerankOpts.QuantizedRerankCandidates)
	assertScalarU8PreparedRawDotNativeResultsMatchPublic2658(t, rawRerankResults, fallbackRerank.Results, 1e-6, "quantized_rerank raw-dot direct")
}

func searchScalarU8PreparedRawDotResultsForTest2658(t *testing.T, reader *columnVectorGraphPhysicalRowReader, query []float32, queryMode columnVectorGraphNativeSearchQueryMode, indexName string, topK, efSearch, rerankCandidates int) []columnVectorGraphNativeSearchResult {
	t.Helper()
	if reader == nil || reader.hnswSearchPack == nil {
		t.Fatalf("raw-dot direct test requires prepared pack reader")
	}
	var scratch columnVectorGraphNativeSearchScratch
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("query norm: %v", err)
	}
	scorer, err := reader.prepareScalarU8QuantizedScorer(queryMode, indexName, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("prepare scalar_u8 scorer: %v", err)
	}
	scratch.preparedScalarU8Plane.scorer = scorer
	scratch.preparedScalarU8Plane.ready = true
	packOpts := columnHNSWPreparedTraversalOptions{
		TopK:                                 topK,
		EfSearch:                             efSearch,
		StatsMode:                            columnVectorGraphNativeSearchStatsModeFullDiagnostics,
		OmitResultMaterialization:            queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		SuppressOmittedResultMaterialization: queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank,
	}
	results, stats, err := reader.hnswSearchPack.searchCosinePreparedRawDotTraversal(query, packOpts, &scratch, &scratch.preparedScalarU8Plane)
	if err != nil {
		t.Fatalf("raw-dot traversal: %v", err)
	}
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
		return append([]columnVectorGraphNativeSearchResult(nil), results...)
	}
	if rerankCandidates == 0 {
		rerankCandidates = efSearch
	}
	scratch.results = scratch.results[:0]
	if err := reader.hnswSearchPack.exactRerankPreparedTraversalRowIDCandidates(query, topK, rerankCandidates, columnVectorGraphScoreBatchModeDefault, &scratch, &stats); err != nil {
		t.Fatalf("raw-dot exact rerank: %v", err)
	}
	if err := reader.hnswSearchPack.fetchTopSearchResults(&scratch, &stats); err != nil {
		t.Fatalf("raw-dot fetch results: %v", err)
	}
	return append([]columnVectorGraphNativeSearchResult(nil), scratch.results...)
}

func assertScalarU8PreparedRawDotNativeResultsMatchPublic2658(t *testing.T, got []columnVectorGraphNativeSearchResult, want []VectorIndexSearchResult, tolerance float64, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s results=%d want %d", label, len(got), len(want))
	}
	for i := range got {
		if got[i].Ordinal != want[i].Ordinal || !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > tolerance {
			t.Fatalf("%s result[%d] got=%+v want=%+v", label, i, got[i], want[i])
		}
	}
}
