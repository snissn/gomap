package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/rabitq"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const columnGraphRabitQQuantizedIndexName2450 = "embedding.rabitq_1bit.fast"

func TestVectorIndexQuantizedDefinitionNormalizationRabitQ2450(t *testing.T) {
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 3)
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphRabitQQuantizedIndexName2450, Codec: rabitq.CodecName}}
	normalized, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition rabitq_1bit: %v", err)
	}
	if got := normalized.QuantizedIndexes[0]; got.Codec != rabitq.CodecName || got.Version != rabitq.CodecVersion {
		t.Fatalf("normalized rabitq quantized index=%+v", got)
	}

	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphRabitQQuantizedIndexName2450, Codec: rabitq.CodecName, Version: rabitq.CodecVersion + 1}}
	if _, err := normalizeVectorIndexDefinition(def); err == nil || !strings.Contains(err.Error(), "rabitq_1bit version") {
		t.Fatalf("normalize unsupported rabitq version err=%v want rabitq_1bit version failure", err)
	}
}

func TestColumnGraphRabitQQuantizedAssetRowBytesOverflow2450(t *testing.T) {
	plan, err := rabitq.NewPlan(128, rabitq.DefaultConfig())
	if err != nil {
		t.Fatalf("rabitq.NewPlan: %v", err)
	}
	if got, err := checkedColumnVectorGraphQuantizedRowBytes(3, plan.BytesPerCode(), "rabitq_1bit codes"); err != nil || got != 3*plan.BytesPerCode() {
		t.Fatalf("checked rabitq code bytes got=%d err=%v", got, err)
	}
	for _, tc := range []struct {
		name        string
		rowCount    int
		bytesPerRow int
	}{
		{name: "codes", rowCount: math.MaxInt/plan.BytesPerCode() + 1, bytesPerRow: plan.BytesPerCode()},
		{name: "primary_id", rowCount: math.MaxInt/8 + 1, bytesPerRow: 8},
		{name: "code_count", rowCount: math.MaxInt/4 + 1, bytesPerRow: 4},
		{name: "quantized_dot_product_inv", rowCount: math.MaxInt/4 + 1, bytesPerRow: 4},
	} {
		if _, err := checkedColumnVectorGraphQuantizedRowBytes(tc.rowCount, tc.bytesPerRow, "rabitq_1bit "+tc.name); err == nil || !strings.Contains(err.Error(), "bytes overflow") {
			t.Fatalf("checked rabitq %s overflow err=%v want bytes overflow", tc.name, err)
		}
	}
}

func TestColumnGraphRabitQQuantizedAssetRebuildPrepareReopen2450(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0.125}},
	}
	dir, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	cfg, graph, state, asset := loadColumnGraphRabitQQuantizedState2450(t, d, def)
	q := def.QuantizedIndexes[0]
	if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes || asset.AssetID != columnVectorGraphQuantizedAssetID(q) || asset.RowCount != len(rows) || asset.AssetBytes <= 0 {
		t.Fatalf("rabitq quantized asset snapshot=%+v", asset)
	}
	if asset.LogicalType != columnVectorIndexStateLogicalTypePackedBitVector || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawPackedBitVector {
		t.Fatalf("rabitq state type/encoding=(%q,%q)", asset.LogicalType, asset.PhysicalEncoding)
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, state, graph); err != nil {
		t.Fatalf("validate state assets with rabitq asset: %v", err)
	}

	prepared, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, asset)
	if err != nil {
		t.Fatalf("loadColumnVectorGraphQuantizedAsset rabitq: %v", err)
	}
	_, scannedRows := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	assertPreparedRabitQRows2450(t, prepared, def, scannedRows)
	if fp := prepared.Footprint(); fp.AssetBytes != asset.AssetBytes || fp.BytesPerVector <= 0 {
		t.Fatalf("rabitq footprint=%+v assetBytes=%d", fp, asset.AssetBytes)
	} else {
		plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
		if err != nil {
			t.Fatalf("rabitq.NewPlan: %v", err)
		}
		t.Logf("rabitq fixture asset_B/vector=%.2f logical_code_B/vector=%.2f", fp.BytesPerVector, float64(plan.BytesPerCode()))
	}

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	loaded := reader.quantizedAssetStatus[q.Name]
	if loaded.Prepared == nil || loaded.Err != nil {
		_ = reader.Close()
		t.Fatalf("reader rabitq quantized status=%+v", loaded)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}

	quantized, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("rabitq quantized_only SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantized.Results, rabitqQuantizedTopKForTest2451(t, rows, []float32{1, 0, 0}, 1), false)
	if quantized.Stats.QuantizedScoreCalls == 0 || quantized.Stats.PreparedScoreCalls != 0 || quantized.Stats.VectorBytesRead != 0 || quantized.Stats.NormBytesRead != 0 {
		t.Fatalf("rabitq quantized stats=%+v want code scoring without exact vector/norm scoring", quantized.Stats)
	}
	if exact, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}); err != nil || len(exact.Results) != 1 {
		t.Fatalf("exact SearchVectorIndex results=%d err=%v", len(exact.Results), err)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	reopenedReader, err := reopenedCol.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader reopen: %v", err)
	}
	defer func() { _ = reopenedReader.Close() }()
	reopenedStatus := reopenedReader.quantizedAssetStatus[q.Name]
	if reopenedStatus.Prepared == nil || reopenedStatus.Err != nil {
		t.Fatalf("reopened reader rabitq status=%+v", reopenedStatus)
	}
	assertPreparedRabitQRows2450(t, reopenedStatus.Prepared, def, scannedRows)
}

func TestVectorIndexSearcherRabitQQuantizedSearchWithBuffer2451(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: append([]float32{1, 0, 0}, make([]float32, 61)...)},
		{id: "doc-b", vector: append([]float32{0.5, 0.5, 0}, make([]float32, 61)...)},
		{id: "doc-c", vector: append([]float32{0, 1, 0}, make([]float32, 61)...)},
		{id: "doc-d", vector: append([]float32{0, 0, 1}, make([]float32, 61)...)},
		{id: "doc-e", vector: append([]float32{-0.25, 0.75, 0.125}, make([]float32, 61)...)},
	}
	query := append([]float32{0.2, 0.9, 0.1}, make([]float32, 61)...)
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	q := def.QuantizedIndexes[0]
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		t.Fatalf("rabitq.NewPlan: %v", err)
	}
	var buffer VectorIndexSearchBuffer

	if _, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows)}, &buffer); err == nil || !strings.Contains(err.Error(), "exact") {
		t.Fatalf("exact SearchWithBuffer with rabitq index err=%v want exact quantized-option rejection", err)
	}
	if _, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QuantizedRerankCandidates: 2, TopK: 1, EfSearch: len(rows)}, &buffer); err == nil || !strings.Contains(err.Error(), "exact") || !strings.Contains(err.Error(), "rerank") {
		t.Fatalf("default exact SearchWithBuffer with rerank candidates err=%v want exact quantized-option rejection", err)
	}

	quantizedOnly, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 3, EfSearch: len(rows)}, &buffer)
	if err != nil {
		t.Fatalf("rabitq quantized_only SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, rabitqQuantizedTopKForTest2451(t, rows, query, 3), false)
	assertRabitQQuantizedOnlyStats2451(t, quantizedOnly.Stats, plan.BytesPerCode())

	var collectionBuffer VectorIndexSearchBuffer
	collectionGot, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1}, &collectionBuffer)
	if err != nil {
		t.Fatalf("collection rabitq quantized_only SearchVectorIndexWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, collectionGot.Results, rabitqQuantizedTopKForTest2451(t, rows, query, 3), false)
	assertRabitQQuantizedOnlyStats2451(t, collectionGot.Stats, plan.BytesPerCode())

	rerankedAll, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: q.Name, QuantizedRerankCandidates: len(rows), TopK: 2, EfSearch: len(rows)}, &buffer)
	if err != nil {
		t.Fatalf("rabitq quantized_rerank all SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, rerankedAll.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	assertRabitQQuantizedRerankStats2451(t, rerankedAll.Stats, len(rows), def.Dimensions, plan.BytesPerCode())
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(t, rerankedAll.Stats.ScoreBatchOptimizedCalls, rerankedAll.Stats.ScoreBatchScalarFallbackCalls, int(rerankedAll.Stats.ScoreBatchMaxTileSize), def.Dimensions)

	const shortlist = 3
	rerankedShort, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: q.Name, QuantizedRerankCandidates: shortlist, TopK: 2, EfSearch: len(rows)}, &buffer)
	if err != nil {
		t.Fatalf("rabitq quantized_rerank short SearchWithBuffer: %v", err)
	}
	if len(rerankedShort.Results) != 2 {
		t.Fatalf("rabitq quantized_rerank short results=%d want 2", len(rerankedShort.Results))
	}
	assertRabitQQuantizedRerankStats2451(t, rerankedShort.Stats, shortlist, def.Dimensions, plan.BytesPerCode())
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(t, rerankedShort.Stats.ScoreBatchOptimizedCalls, rerankedShort.Stats.ScoreBatchScalarFallbackCalls, int(rerankedShort.Stats.ScoreBatchMaxTileSize), def.Dimensions)
}

func TestRabitQPreparedHNSWSearchPackRouteParity2587(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.7, 0.2, 0.1}},
		{id: "doc-c", vector: []float32{0.1, 0.8, 0.2}},
		{id: "doc-d", vector: []float32{0, 0.1, 0.9}},
		{id: "doc-e", vector: []float32{-0.2, 0.4, 0.7}},
		{id: "doc-f", vector: []float32{0.3, -0.4, 0.6}},
		{id: "doc-g", vector: []float32{-0.5, 0.1, 0.7}},
		{id: "doc-h", vector: []float32{0.2, 0.5, -0.4}},
	}
	query := []float32{0.25, 0.7, -0.1}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	qName := def.QuantizedIndexes[0].Name
	cases := []struct {
		name      string
		mode      VectorIndexQueryMode
		topK      int
		rerank    int
		wantExact bool
	}{
		{name: "quantized_only", mode: VectorIndexQueryModeQuantizedOnly, topK: 4},
		{name: "quantized_rerank_default", mode: VectorIndexQueryModeQuantizedRerank, topK: 3, wantExact: true},
		{name: "quantized_rerank_all", mode: VectorIndexQueryModeQuantizedRerank, topK: 3, rerank: len(rows), wantExact: true},
		{name: "quantized_rerank_shortlist", mode: VectorIndexQueryModeQuantizedRerank, topK: 3, rerank: 5, wantExact: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			legacyOpts := columnVectorGraphNativeSearchOptions{TopK: tc.topK, EfSearch: len(rows), QueryMode: columnVectorGraphNativeSearchQueryModeFromPublic2415(tc.mode), QuantizedIndexName: qName, QuantizedRerankCandidates: tc.rerank}
			var legacyScratch columnVectorGraphNativeSearchScratch
			legacy, legacyStats, err := searcher.reader.SearchCosine(query, legacyOpts, &legacyScratch)
			if err != nil {
				t.Fatalf("legacy SearchCosine: %v", err)
			}
			var buffer VectorIndexSearchBuffer
			got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: tc.mode, QuantizedIndexName: qName, QuantizedRerankCandidates: tc.rerank, TopK: tc.topK, EfSearch: len(rows)}, &buffer)
			if err != nil {
				t.Fatalf("SearchWithBuffer prepared pack route: %v", err)
			}
			assertRabitQPreparedPackResultsMatch2587(t, got.Results, legacy)
			if got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackActive != 1 || got.Stats.SearchRouteColumnGraphPrepared+got.Stats.SearchRouteColumnGraphFallback != 0 {
				t.Fatalf("prepared pack route stats=%+v", got.Stats)
			}
			if got.Stats.QuantizedScoreCalls != legacyStats.QuantizedScoreCalls || got.Stats.QuantizedCodeBytesRead != legacyStats.QuantizedCodeBytesRead {
				t.Fatalf("quantized scorer counters got=%+v legacy=%+v", got.Stats, legacyStats)
			}
			if tc.wantExact {
				wantRerank := tc.rerank
				if wantRerank == 0 {
					wantRerank = len(rows)
				}
				if got.Stats.QuantizedRerankCandidates != uint64(wantRerank) || got.Stats.QuantizedRerankExactScoreCalls != uint64(wantRerank) || got.Stats.PreparedScoreCalls != uint64(wantRerank) {
					t.Fatalf("rerank counters=%+v want shortlist=%d", got.Stats, wantRerank)
				}
			} else if got.Stats.PreparedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 {
				t.Fatalf("quantized_only exact counters=%+v want zero", got.Stats)
			}
		})
	}
}

func TestRabitQPreparedHNSWSearchPackRerankPreservesEfTraversal2587(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.95, 0.05, 0}},
		{id: "doc-c", vector: []float32{0.85, 0.15, 0}},
		{id: "doc-d", vector: []float32{0.75, 0.25, 0}},
		{id: "doc-e", vector: []float32{0.65, 0.35, 0}},
		{id: "doc-f", vector: []float32{0.55, 0.45, 0}},
		{id: "doc-g", vector: []float32{0.45, 0.55, 0}},
		{id: "doc-h", vector: []float32{0.35, 0.65, 0}},
	}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
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
	if fallbackSearcher.reader == nil || fallbackSearcher.reader.hnswSearchPack == nil {
		t.Fatalf("fallback searcher missing reader or prepared pack")
	}
	fallbackSearcher.reader.hnswSearchPack = nil

	opts := VectorIndexSearcherSearchOptions{
		Query:                     []float32{0.7, 0.3, 0},
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        def.QuantizedIndexes[0].Name,
		QuantizedRerankCandidates: 4,
		TopK:                      3,
		EfSearch:                  len(rows),
		StatsMode:                 VectorIndexSearchStatsModeFullDiagnostics,
	}
	var packBuffer, fallbackBuffer VectorIndexSearchBuffer
	packResults, err := packSearcher.SearchWithBuffer(opts, &packBuffer)
	if err != nil {
		t.Fatalf("pack SearchWithBuffer: %v", err)
	}
	fallbackResults, err := fallbackSearcher.SearchWithBuffer(opts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("fallback SearchWithBuffer: %v", err)
	}
	for name, stats := range map[string]VectorIndexSearchStats{"pack": packResults.Stats, "fallback": fallbackResults.Stats} {
		if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
			t.Fatalf("%s stats=%+v want no document/fallback guardrail counters", name, stats)
		}
		if stats.QuantizedRerankExactScoreCalls != uint64(opts.QuantizedRerankCandidates) {
			t.Fatalf("%s stats=%+v want rerank shortlist=%d", name, stats, opts.QuantizedRerankCandidates)
		}
		if name == "pack" && stats.PreparedScoreCalls != uint64(opts.QuantizedRerankCandidates) {
			t.Fatalf("%s stats=%+v want pack rerank prepared score calls=%d", name, stats, opts.QuantizedRerankCandidates)
		}
	}
	if packResults.Stats.Candidates == 0 || packResults.Stats.VisitedEdges == 0 {
		t.Fatalf("pack stats=%+v want production traversal counters", packResults.Stats)
	}
	if packResults.Stats.Candidates != fallbackResults.Stats.Candidates || packResults.Stats.VisitedEdges != fallbackResults.Stats.VisitedEdges || packResults.Stats.QuantizedScoreCalls != fallbackResults.Stats.QuantizedScoreCalls || packResults.Stats.QuantizedRerankExactScoreCalls != fallbackResults.Stats.QuantizedRerankExactScoreCalls {
		t.Fatalf("pack stats=%+v fallback stats=%+v want same efSearch traversal and rerank counters", packResults.Stats, fallbackResults.Stats)
	}
	if len(packResults.Results) != len(fallbackResults.Results) {
		t.Fatalf("pack results=%d fallback results=%d", len(packResults.Results), len(fallbackResults.Results))
	}
	for i := range packResults.Results {
		got := packResults.Results[i]
		want := fallbackResults.Results[i]
		if got.Ordinal != want.Ordinal || !bytes.Equal(got.ID, want.ID) || math.Abs(got.Score-want.Score) > 1e-6 {
			t.Fatalf("result[%d] pack=%+v fallback=%+v", i, got, want)
		}
	}
}

func TestCollectionSearchVectorIndexWithBufferRabitQConcurrentPreparedPack2587(t *testing.T) {
	rows := make([]columnGraphRebuildInputRowV2A, 32)
	for i := range rows {
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%02d", i), vector: rabitqTestVector2477(3, uint64(i+1))}
	}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	qName := def.QuantizedIndexes[0].Name
	warmOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: rabitqTestVector2477(3, 100), QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 4, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var warmBuffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(warmOpts, &warmBuffer); err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	const workers = 8
	const iterations = 20
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buffer VectorIndexSearchBuffer
			for iter := 0; iter < iterations; iter++ {
				query := rabitqTestVector2477(3, uint64(1000+worker*iterations+iter))
				opts := warmOpts
				opts.Query = query
				if iter%2 == 1 {
					opts.QueryMode = VectorIndexQueryModeQuantizedRerank
					opts.QuantizedRerankCandidates = 8
				} else {
					opts.QueryMode = VectorIndexQueryModeQuantizedOnly
					opts.QuantizedRerankCandidates = 0
				}
				got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
				if err != nil {
					errCh <- fmt.Errorf("worker=%d iter=%d: %w", worker, iter, err)
					return
				}
				if len(got.Results) != opts.TopK || got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackActive != 1 || got.Stats.QuantizedScorerActive != 1 || got.Stats.QuantizedScoreCalls == 0 {
					errCh <- fmt.Errorf("worker=%d iter=%d stats=%+v results=%d", worker, iter, got.Stats, len(got.Results))
					return
				}
				if opts.QueryMode == VectorIndexQueryModeQuantizedOnly {
					if got.Stats.PreparedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 {
						errCh <- fmt.Errorf("worker=%d iter=%d quantized_only exact counters=%+v", worker, iter, got.Stats)
						return
					}
				} else if got.Stats.QuantizedRerankExactScoreCalls != uint64(opts.QuantizedRerankCandidates) || got.Stats.PreparedScoreCalls != uint64(opts.QuantizedRerankCandidates) {
					errCh <- fmt.Errorf("worker=%d iter=%d rerank counters=%+v", worker, iter, got.Stats)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func assertRabitQPreparedPackResultsMatch2587(tb testing.TB, got []VectorIndexSearchResult, want []columnVectorGraphNativeSearchResult) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("results=%d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Ordinal != want[i].Ordinal || !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want ordinal=%d id=%q score=%v", i, got[i], want[i].Ordinal, want[i].ID, want[i].Score)
		}
	}
}

func TestCollectionSearchVectorIndexWithBufferRabitQQuantized2452(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.5, 0.5, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{-0.25, 0.75, 0.125}},
	}
	query := []float32{0.2, 0.9, 0.1}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	q := def.QuantizedIndexes[0]
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		_ = d.Close()
		t.Fatalf("rabitq.NewPlan: %v", err)
	}

	quantizedOnlyOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	quantizedOnly, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
	if err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndexWithBuffer rabitq quantized_only: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, quantizedOnly, def.Name, quantizedOnlyOpts.TopK)
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, rabitqQuantizedTopKForTest2451(t, rows, query, quantizedOnlyOpts.TopK), false)
	assertCollectionBufferedRabitQQuantizedRouteStats2452(t, quantizedOnly.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, quantizedOnlyOpts, def.Dimensions, plan.BytesPerCode())
	if len(buffer.results) != quantizedOnlyOpts.TopK || len(buffer.idBytes) == 0 || &quantizedOnly.Results[0] != &buffer.results[0] || &quantizedOnly.Results[0].ID[0] != &buffer.idBytes[0] {
		_ = d.Close()
		t.Fatalf("rabitq quantized_only response does not alias caller-owned buffer: results=%d idBytes=%d", len(buffer.results), len(buffer.idBytes))
	}
	warmSnap := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if warmSnap.Entries != 1 || warmSnap.CacheBuilds != 1 || warmSnap.CacheMisses != 1 {
		_ = d.Close()
		t.Fatalf("cache after rabitq quantized warm=%+v want one prepared entry", warmSnap)
	}

	for i := 0; i < 3; i++ {
		got, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
		if err != nil {
			_ = d.Close()
			t.Fatalf("cached rabitq quantized_only iteration %d: %v", i, err)
		}
		assertCollectionBufferedRabitQQuantizedRouteStats2452(t, got.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, quantizedOnlyOpts, def.Dimensions, plan.BytesPerCode())
	}
	afterOnly := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterOnly.Entries != 1 || afterOnly.CacheBuilds != warmSnap.CacheBuilds || afterOnly.CacheHits < warmSnap.CacheHits+3 {
		_ = d.Close()
		t.Fatalf("cache after rabitq quantized_only reuse=%+v warm=%+v want hits without rebuild", afterOnly, warmSnap)
	}

	rerankOpts := quantizedOnlyOpts
	rerankOpts.QueryMode = VectorIndexQueryModeQuantizedRerank
	rerankOpts.QuantizedRerankCandidates = len(rows)
	rerankOpts.TopK = 2
	reranked, err := col.SearchVectorIndexWithBuffer(rerankOpts, &buffer)
	if err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndexWithBuffer rabitq quantized_rerank: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, reranked, def.Name, rerankOpts.TopK)
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, query, rerankOpts.TopK), false)
	assertCollectionBufferedRabitQQuantizedRouteStats2452(t, reranked.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, rerankOpts, def.Dimensions, plan.BytesPerCode())
	afterRerank := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterRerank.Entries != 1 || afterRerank.CacheBuilds != afterOnly.CacheBuilds || afterRerank.CacheHits <= afterOnly.CacheHits {
		_ = d.Close()
		t.Fatalf("cache after rabitq quantized_rerank=%+v afterOnly=%+v want shared prepared entry", afterRerank, afterOnly)
	}

	if !collectionsRaceEnabled {
		quantizedOnlyAllocs := testing.AllocsPerRun(100, func() {
			got, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
			if err != nil || len(got.Results) != quantizedOnlyOpts.TopK {
				panic("unexpected rabitq quantized_only SearchVectorIndexWithBuffer allocation probe result")
			}
		})
		if quantizedOnlyAllocs != 0 {
			_ = d.Close()
			t.Fatalf("rabitq quantized_only SearchVectorIndexWithBuffer steady-state allocs/run=%v want 0", quantizedOnlyAllocs)
		}
		rerankAllocs := testing.AllocsPerRun(100, func() {
			got, err := col.SearchVectorIndexWithBuffer(rerankOpts, &buffer)
			if err != nil || len(got.Results) != rerankOpts.TopK {
				panic("unexpected rabitq quantized_rerank SearchVectorIndexWithBuffer allocation probe result")
			}
		})
		if rerankAllocs != 0 {
			_ = d.Close()
			t.Fatalf("rabitq quantized_rerank SearchVectorIndexWithBuffer steady-state allocs/run=%v want 0", rerankAllocs)
		}
	}

	beforeClose := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if beforeClose.Entries != 1 || beforeClose.ActiveHandles == 0 {
		_ = d.Close()
		t.Fatalf("cache before close=%+v want active rabitq quantized entry", beforeClose)
	}
	if err := col.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		_ = d.Close()
		t.Fatalf("close collection cache: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		_ = d.Close()
		t.Fatalf("cache after collection close=%+v want released", snap)
	}
	if _, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer); err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndexWithBuffer after rabitq cache close: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.ActiveHandles == 0 {
		_ = d.Close()
		t.Fatalf("cache after rabitq rebuild=%+v want active entry", snap)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("DB Close: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		t.Fatalf("cache after DB close=%+v want released by manager close hook", snap)
	}
}

func TestCollectionSearchVectorIndexWithBufferRabitQFailClosedAndQueryErrors2452(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	q := def.QuantizedIndexes[0]
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		t.Fatalf("rabitq.NewPlan: %v", err)
	}
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	beforeBadQuery := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if beforeBadQuery.Entries != 1 || beforeBadQuery.CacheBuilds != 1 || beforeBadQuery.ActiveHandles == 0 {
		t.Fatalf("cache before bad rabitq query=%+v want warmed entry", beforeBadQuery)
	}
	badQuery := base
	badQuery.Query = []float32{1, 0}
	bad, err := col.SearchVectorIndexWithBuffer(badQuery, &buffer)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("bad rabitq query response=%+v err=%v want dimension mismatch", bad, err)
	}
	if len(bad.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("bad rabitq query left results response=%d bufferResults=%d idBytes=%d", len(bad.Results), len(buffer.results), len(buffer.idBytes))
	}
	afterBadQuery := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterBadQuery.Entries != 1 || afterBadQuery.CacheBuilds != beforeBadQuery.CacheBuilds || afterBadQuery.Invalidations != beforeBadQuery.Invalidations || afterBadQuery.ActiveHandles == 0 {
		t.Fatalf("cache after bad rabitq query=%+v before=%+v want healthy prepared state retained", afterBadQuery, beforeBadQuery)
	}
	valid, err := col.SearchVectorIndexWithBuffer(base, &buffer)
	if err != nil {
		t.Fatalf("valid rabitq SearchVectorIndexWithBuffer after bad query: %v", err)
	}
	assertCollectionBufferedRabitQQuantizedRouteStats2452(t, valid.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, base, def.Dimensions, plan.BytesPerCode())

	for _, tc := range []struct {
		name   string
		mode   VectorIndexQueryMode
		health columnVectorGraphQuantizedAssetHealth
		mutate func(map[string]columnVectorGraphQuantizedAssetLoadStatus, string, columnVectorGraphQuantizedAssetLoadStatus)
	}{
		{
			name:   "missing",
			mode:   VectorIndexQueryModeQuantizedOnly,
			health: columnVectorGraphQuantizedAssetHealthMissing,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				delete(status, qName)
			},
		},
		{
			name:   "invalid",
			mode:   VectorIndexQueryModeQuantizedOnly,
			health: columnVectorGraphQuantizedAssetHealthInvalid,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				original.Prepared = nil
				original.RabitQPlan = nil
				original.Health = columnVectorGraphQuantizedAssetHealthInvalid
				original.Err = fmt.Errorf("%w: checksum mismatch", errColumnVectorGraphQuantizedAssetInvalid)
				status[qName] = original
			},
		},
		{
			name:   "stale",
			mode:   VectorIndexQueryModeQuantizedRerank,
			health: columnVectorGraphQuantizedAssetHealthStale,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				original.Prepared = nil
				original.RabitQPlan = nil
				original.Health = columnVectorGraphQuantizedAssetHealthStale
				original.Err = fmt.Errorf("%w: base graph identity mismatch", errColumnVectorGraphQuantizedAssetStale)
				status[qName] = original
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			opts.QueryMode = tc.mode
			if tc.mode == VectorIndexQueryModeQuantizedRerank {
				opts.QuantizedRerankCandidates = 1
			}
			if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
				t.Fatalf("warm %s SearchVectorIndexWithBuffer: %v", tc.name, err)
			}
			mutateCachedCollectionQuantizedAssetStatus2415(t, col, opts, tc.mutate)
			got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("SearchVectorIndexWithBuffer err=%v want ErrVectorIndexSearchUnavailable", err)
			}
			if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
				t.Fatalf("unavailable rabitq response results=%d buffer.results=%d idBytes=%d want fail-closed empty", len(got.Results), len(buffer.results), len(buffer.idBytes))
			}
			assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeFromPublic2415(tc.mode), tc.health)
			if got.Stats.SearchRouteHNSWSearchPack != 0 || got.Stats.HNSWSearchPackFallbacks != 0 || got.Stats.PreparedScoreCalls != 0 || got.Stats.QuantizedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 || got.Stats.OpenSearcherCalls != 0 || got.Stats.OpenSetupInTimedLoop != 0 {
				t.Fatalf("unavailable rabitq stats=%+v want fail-closed without exact fallback/open", got.Stats)
			}
			afterFailure := col.collectionVectorIndexPreparedSearchCacheSnapshot()
			if afterFailure.Entries != 0 || afterFailure.ActiveHandles != 0 {
				t.Fatalf("cache after rabitq fail-closed asset=%+v want invalidated closed entry", afterFailure)
			}
			recovered, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err != nil {
				t.Fatalf("SearchVectorIndexWithBuffer after rabitq fail-closed rebuild: %v", err)
			}
			assertCollectionBufferedRabitQQuantizedRouteStats2452(t, recovered.Stats, columnVectorGraphNativeSearchQueryModeFromPublic2415(tc.mode), opts, def.Dimensions, plan.BytesPerCode())
		})
	}
}

func TestCollectionVectorIndexPreparedQuantizedSearchCacheKeyRabitQIdentity2452(t *testing.T) {
	makeKey := func(t *testing.T, q QuantizedVectorIndexDefinition) string {
		t.Helper()
		def := columnGraphRebuildVectorIndexDefinitionV2A(3, 3)
		def.QuantizedIndexes = []QuantizedVectorIndexDefinition{q}
		def, err := normalizeVectorIndexDefinition(def)
		if err != nil {
			t.Fatalf("normalizeVectorIndexDefinition: %v", err)
		}
		q = def.QuantizedIndexes[0]
		graphRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "ns", Generation: 7, PartID: 11, FileID: 13, Offset: 17, Length: 19, Checksum: 23}
		graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: 29, BaseManifestChecksum: 31, BaseSchemaHash: 37, GraphSchemaHash: 41, RowCount: 5, AssetRef: graphRef, AssetBytes: graphRef.Length}
		logicalType, physicalEncoding := columnVectorGraphQuantizedAssetStateType(q)
		asset := columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedCodes, AssetID: columnVectorGraphQuantizedAssetID(q), LogicalType: logicalType, PhysicalEncoding: physicalEncoding, RowCount: graph.RowCount, SourceSchemaHash: 43, AssetBytes: 47, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "ns", Generation: graph.BaseManifestGeneration, PartID: 53, FileID: 59, Offset: 61, Length: 67, Checksum: 71}}
		state := columnVectorIndexStateSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, RowCount: graph.RowCount, BaseManifestGeneration: graph.BaseManifestGeneration, BaseManifestChecksum: graph.BaseManifestChecksum, BaseSchemaHash: graph.BaseSchemaHash, AdjacencyLayerCount: graph.AdjacencyLayerCount, Assets: []columnVectorIndexStateAssetSnapshot{asset}}
		key, err := collectionVectorIndexPreparedQuantizedSearchCacheKey("docs", "ns", def, graph, state, q.Name, 1)
		if err != nil {
			t.Fatalf("collectionVectorIndexPreparedQuantizedSearchCacheKey %s: %v", q.Codec, err)
		}
		return key
	}

	scalarKey := makeKey(t, QuantizedVectorIndexDefinition{Name: columnGraphScalarU8QuantizedBenchIndexName1926, Codec: QuantizedVectorCodecScalarU8})
	rabitqKey := makeKey(t, QuantizedVectorIndexDefinition{Name: columnGraphRabitQQuantizedIndexName2450, Codec: rabitq.CodecName})
	if scalarKey == rabitqKey {
		t.Fatalf("scalar_u8 and rabitq cache keys are equal: %q", scalarKey)
	}
	if !strings.Contains(scalarKey, "codec=scalar_u8|version=1|codec_config_hash=0|codec_config=|code_dimensions=3|code_width_bits=8|") || !strings.Contains(scalarKey, "asset_id=quantized/embedding.scalar_u8.fast/codes") {
		t.Fatalf("scalar cache key missing codec/version/config/asset identity: %s", scalarKey)
	}
	wantRabitQIdentity := fmt.Sprintf("codec=%s|version=%d|codec_config_hash=%d|codec_config=%x|code_dimensions=4|code_width_bits=%d|", rabitq.CodecName, rabitq.CodecVersion, rabitq.DefaultConfig().Hash64(), rabitq.DefaultConfig().CanonicalBytes(), rabitq.CodeWidthBits)
	if !strings.Contains(rabitqKey, wantRabitQIdentity) || !strings.Contains(rabitqKey, "asset_id=quantized/embedding.rabitq_1bit.fast/packed_codes") {
		t.Fatalf("rabitq cache key missing codec/version/config/asset identity: %s want identity %s", rabitqKey, wantRabitQIdentity)
	}
}

func assertCollectionBufferedRabitQQuantizedRouteStats2452(tb testing.TB, stats VectorIndexSearchStats, mode columnVectorGraphNativeSearchQueryMode, opts VectorIndexSearchOptions, dims int, bytesPerCode int) {
	tb.Helper()
	if bytesPerCode <= 0 {
		tb.Fatalf("rabitq bytes_per_code=%d", bytesPerCode)
	}
	if !vectorIndexSearchStatsAreBufferedNoDocumentQuantizedRoute(stats, mode, opts, dims) {
		tb.Fatalf("collection buffered rabitq stats=%+v want healthy no-document quantized route", stats)
	}
	if stats.OpenSearcherCalls != 0 || stats.OpenSetupInTimedLoop != 0 || stats.ResponseOwnedResultAllocs != 0 {
		tb.Fatalf("collection buffered rabitq boundary stats=%+v want no one-shot open/setup or response-owned allocation signal", stats)
	}
	switch mode {
	case columnVectorGraphNativeSearchQueryModeQuantizedOnly:
		assertRabitQQuantizedOnlyStats2451(tb, stats, bytesPerCode)
	case columnVectorGraphNativeSearchQueryModeQuantizedRerank:
		shortlist := opts.QuantizedRerankCandidates
		if shortlist == 0 {
			shortlist = opts.EfSearch
		}
		assertRabitQQuantizedRerankStats2451(tb, stats, shortlist, dims, bytesPerCode)
	default:
		tb.Fatalf("unexpected rabitq query mode %s", mode.String())
	}
}

func TestColumnGraphRabitQQuantizedScorerMatchesOracle2451(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0.125}},
		{id: "doc-d", vector: []float32{0.1, 0.2, 0.95}},
	}
	query := []float32{0.15, -0.35, 0.75}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	q := def.QuantizedIndexes[0]
	status := reader.quantizedAssetStatus[q.Name]
	if status.Prepared == nil || status.RabitQPlan == nil || status.Err != nil {
		t.Fatalf("rabitq prepared status=%+v", status)
	}
	var scratch columnVectorGraphNativeSearchScratch
	scorer, err := reader.prepareRabitQQuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, q.Name, query, &scratch)
	if err != nil {
		t.Fatalf("prepareRabitQQuantizedScorer: %v", err)
	}
	ordinals := []int{0, 1, 2, 3}
	var stats columnVectorGraphNativeSearchStats
	scores, err := scorer.scoreOrdinals(ordinals, nil, &scratch, &stats)
	if err != nil {
		t.Fatalf("scoreOrdinals: %v", err)
	}
	oracleQuery, err := status.RabitQPlan.EncodeQuery(query, &rabitq.Workspace{})
	if err != nil {
		t.Fatalf("oracle EncodeQuery: %v", err)
	}
	view, ok := status.Prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok {
		t.Fatal("prepared packed code view unavailable")
	}
	for i, ordinal := range ordinals {
		code, ok := view.RowBytes(ordinal)
		if !ok {
			t.Fatalf("row %d code unavailable", ordinal)
		}
		codeCount, ok := status.Prepared.Uint32(quantizedasset.RoleCodeCount, ordinal)
		if !ok {
			t.Fatalf("row %d code_count unavailable", ordinal)
		}
		qdpInv, ok := status.Prepared.Float32(quantizedasset.RoleQuantizedDotProductInv, ordinal)
		if !ok {
			t.Fatalf("row %d qdp unavailable", ordinal)
		}
		want, err := status.RabitQPlan.ScoreCosine(oracleQuery, code, codeCount, qdpInv)
		if err != nil {
			t.Fatalf("oracle ScoreCosine ordinal=%d: %v", ordinal, err)
		}
		if math.Abs(scores[i]-want) > 1e-9 {
			t.Fatalf("ordinal=%d score=%v want oracle=%v", ordinal, scores[i], want)
		}
	}
	if stats.QuantizedScoreCalls != uint64(len(ordinals)) || stats.QuantizedCodeBytesRead != uint64(len(ordinals)*status.RabitQPlan.BytesPerCode()) || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 {
		t.Fatalf("rabitq scorer stats=%+v want packed code reads only", stats)
	}
	_, err = scorer.scoreOrdinals([]int{0, len(rows)}, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "ordinal=4") {
		t.Fatalf("invalid ordinal err=%v want fail-closed unavailable", err)
	}
	if _, err = scorer.scoreOrdinals(ordinals, scores[:0], nil, &columnVectorGraphNativeSearchStats{}); !errors.Is(err, errColumnVectorGraphNativeSearchScratchRequired) {
		t.Fatalf("nil scratch err=%v want scratch required", err)
	}

	_, err = scorer.scoreOrdinals(ordinals, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
	if err != nil {
		t.Fatalf("warm scoreOrdinals: %v", err)
	}
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are unstable under race instrumentation")
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, err := scorer.scoreOrdinals(ordinals, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
		if err != nil {
			panic(err)
		}
		columnPhysicalScanBenchSum += int64(got[0] * 1_000_000)
	})
	if allocs != 0 {
		t.Fatalf("steady-state rabitq scoreOrdinals allocs/run=%v want 0", allocs)
	}
}

func TestVectorIndexSearcherRabitQQuantizedFailClosedStats2451(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	query := []float32{1, 0, 0}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	q := def.QuantizedIndexes[0]
	cases := []struct {
		name   string
		status *columnVectorGraphQuantizedAssetLoadStatus
		health columnVectorGraphQuantizedAssetHealth
	}{
		{name: "missing_status", health: columnVectorGraphQuantizedAssetHealthMissing},
		{name: "missing_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetMissing}, health: columnVectorGraphQuantizedAssetHealthMissing},
		{name: "invalid_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetInvalid}, health: columnVectorGraphQuantizedAssetHealthInvalid},
		{name: "stale_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetStale}, health: columnVectorGraphQuantizedAssetHealthStale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				t.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			if tc.status == nil {
				delete(searcher.reader.quantizedAssetStatus, q.Name)
			} else {
				searcher.reader.quantizedAssetStatus[q.Name] = *tc.status
			}
			var buffer VectorIndexSearchBuffer
			got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows)}, &buffer)
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) || len(got.Results) != 0 {
				t.Fatalf("SearchWithBuffer response=%+v err=%v want fail-closed unavailable", got, err)
			}
			assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, tc.health)
			if got.Stats.QuantizedScoreCalls != 0 || got.Stats.PreparedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 {
				t.Fatalf("fail-closed stats=%+v want no scoring or exact fallback", got.Stats)
			}
		})
	}
}

func assertRabitQQuantizedOnlyStats2451(tb testing.TB, stats VectorIndexSearchStats, bytesPerCode int) {
	tb.Helper()
	if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("rabitq quantized_only route stats=%+v", stats)
	}
	if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.SearchRouteColumnGraphPrepared != 0 || stats.SearchRouteColumnGraphFallback != 0 {
		tb.Fatalf("rabitq quantized_only route stats=%+v want prepared hnsw_search_pack_v1 score-plane traversal", stats)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(bytesPerCode) {
		tb.Fatalf("rabitq quantized_only code stats=%+v bytes_per_code=%d", stats, bytesPerCode)
	}
	if stats.PreparedScoreCalls != 0 || stats.QuantizedRerankCandidates != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 || stats.DocumentsFetched != 0 {
		tb.Fatalf("rabitq quantized_only exact/doc stats=%+v want none", stats)
	}
}

func assertRabitQQuantizedRerankStats2451(tb testing.TB, stats VectorIndexSearchStats, shortlist int, dims int, bytesPerCode int) {
	tb.Helper()
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("rabitq quantized_rerank route stats=%+v", stats)
	}
	if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.SearchRouteColumnGraphPrepared != 0 || stats.SearchRouteColumnGraphFallback != 0 {
		tb.Fatalf("rabitq quantized_rerank route stats=%+v want prepared hnsw_search_pack_v1 score-plane traversal", stats)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(bytesPerCode) {
		tb.Fatalf("rabitq quantized_rerank code stats=%+v bytes_per_code=%d", stats, bytesPerCode)
	}
	if stats.QuantizedRerankCandidates != uint64(shortlist) || stats.QuantizedRerankExactScoreCalls != uint64(shortlist) {
		tb.Fatalf("rabitq quantized_rerank exact calls stats=%+v shortlist=%d", stats, shortlist)
	}
	if stats.VectorBytesRead != uint64(shortlist*dims*4) || stats.NormBytesRead != 0 {
		tb.Fatalf("rabitq quantized_rerank exact bytes stats=%+v want prepared-pack vector=%d norm=0", stats, shortlist*dims*4)
	}
	if stats.DocumentsFetched != 0 {
		tb.Fatalf("rabitq quantized_rerank stats=%+v want no documents", stats)
	}
}

func rabitqQuantizedTopKForTest2451(tb testing.TB, rows []columnGraphRebuildInputRowV2A, query []float32, topK int) []columnVectorGraphNativeSearchResult {
	tb.Helper()
	if len(rows) == 0 || topK <= 0 {
		return nil
	}
	plan, err := rabitq.NewPlan(len(query), rabitq.DefaultConfig())
	if err != nil {
		tb.Fatalf("rabitq.NewPlan: %v", err)
	}
	var ws rabitq.Workspace
	encodedQuery, err := plan.EncodeQuery(query, &ws)
	if err != nil {
		tb.Fatalf("rabitq EncodeQuery: %v", err)
	}
	var top []columnVectorGraphSearchCandidate
	var codeScratch []byte
	for ordinal, row := range rows {
		encoded, err := plan.Encode(codeScratch, row.vector, &ws)
		if err != nil {
			tb.Fatalf("rabitq Encode row %d: %v", ordinal, err)
		}
		codeScratch = encoded.Code
		score, err := plan.ScoreEncoded(encodedQuery, encoded)
		if err != nil {
			tb.Fatalf("rabitq ScoreEncoded row %d: %v", ordinal, err)
		}
		top = insertColumnGraphTopForTest(top, topK, columnVectorGraphSearchCandidate{ordinal: ordinal, score: score})
	}
	out := make([]columnVectorGraphNativeSearchResult, len(top))
	for i, candidate := range top {
		out[i] = columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, ID: []byte(rows[candidate.ordinal].id), Score: candidate.score}
	}
	return out
}

func TestColumnGraphRabitQQuantizedAssetFailClosed2450(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0.125}},
	}
	_, d, col, def := openColumnGraphRabitQQuantizedTestCollection2450(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	cfg, graph, state, asset := loadColumnGraphRabitQQuantizedState2450(t, d, def)
	q := def.QuantizedIndexes[0]

	missing := state
	missing.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	for i := range missing.Assets {
		if missing.Assets[i].Role == columnVectorIndexStateAssetRoleQuantizedCodes {
			missing.Assets = append(missing.Assets[:i], missing.Assets[i+1:]...)
			break
		}
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, missing, graph); err == nil || !strings.Contains(err.Error(), "missing quantized asset") {
		t.Fatalf("missing rabitq asset err=%v want missing quantized asset", err)
	}

	staleSchema := asset
	staleSchema.SourceSchemaHash++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleSchema); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "schema_hash") {
		t.Fatalf("stale schema err=%v want stale schema_hash", err)
	}

	staleType := asset
	staleType.LogicalType = columnVectorIndexStateLogicalTypeByteVector
	staleType.PhysicalEncoding = columnVectorIndexStateEncodingRawFixedBytes
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleType); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "type/encoding") {
		t.Fatalf("stale type err=%v want stale type/encoding", err)
	}

	staleRowCount := asset
	staleRowCount.RowCount++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleRowCount); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "row_count") {
		t.Fatalf("stale row_count err=%v want stale row_count", err)
	}

	badVersion := q
	badVersion.Version++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, badVersion, asset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "version") {
		t.Fatalf("bad codec version err=%v want invalid version", err)
	}

	badChecksum := asset
	badChecksum.Ref.Checksum++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, badChecksum); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("bad checksum err=%v want invalid checksum", err)
	}

	paddingAsset := writeUncheckedRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+101, func(raw []byte, image typedcolumn.ColumnPartImage) {
		section := mustColumnDataSection2450(t, image, columnVectorGraphQuantizedPackedCodesColumnName)
		if section.Length <= 0 {
			t.Fatalf("packed section length=%d", section.Length)
		}
		raw[section.Offset+section.Length-1] |= 0xf0
	})
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, paddingAsset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || (!strings.Contains(err.Error(), "non-zero padding") && !strings.Contains(err.Error(), "checksum")) {
		t.Fatalf("non-zero padding err=%v want invalid padding/checksum fail-closed", err)
	}

	countAsset := writeUncheckedRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+102, func(raw []byte, image typedcolumn.ColumnPartImage) {
		section := mustColumnDataSection2450(t, image, columnVectorGraphQuantizedCodeCountColumnName)
		if section.Length < 4 {
			t.Fatalf("code_count section length=%d", section.Length)
		}
		raw[section.Offset]++
	})
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, countAsset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "code_count") {
		t.Fatalf("code_count mismatch err=%v want invalid code_count", err)
	}

	qdpRangeAsset := writeOutOfRangeQDPRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+103)
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, qdpRangeAsset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "quantized_dot_product_inv") || !strings.Contains(err.Error(), "outside valid range") {
		t.Fatalf("qdp range err=%v want invalid quantized_dot_product_inv range", err)
	}

	wrongShape := writeWrongShapeRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, asset.Ref.PartID+104, len(rows))
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, wrongShape); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "elements_per_row") {
		t.Fatalf("wrong packed shape err=%v want elements_per_row invalid", err)
	}

	missingSide := writeMissingSideRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+105)
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, missingSide); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || (!strings.Contains(err.Error(), "missing required role") && !strings.Contains(err.Error(), "missing typed-column column")) {
		t.Fatalf("missing side-array err=%v want missing role/column", err)
	}

	schema, err := columnVectorGraphQuantizedAssetSchema(def, graph, q, asset, columnVectorGraphQuantizedAssetRefIdentity(asset.Ref))
	if err != nil {
		t.Fatalf("columnVectorGraphQuantizedAssetSchema: %v", err)
	}
	expected := quantizedasset.ExpectedSchema{Metric: schema.Metric, VectorDimensions: schema.VectorDimensions, CodeDimensions: schema.CodeDimensions, CodeWidthBits: schema.CodeWidthBits, RowCount: schema.RowCount, OrdinalOrder: schema.OrdinalOrder, Codec: schema.Codec, BaseGraph: schema.BaseGraph, RequiredRoles: columnVectorGraphQuantizedRequiredRoles(q)}
	expected.Codec.ConfigHash++
	if err := quantizedasset.Validate(schema, expected); err == nil || !strings.Contains(err.Error(), "config_hash") {
		t.Fatalf("config hash mismatch err=%v want config_hash", err)
	}
	expected = quantizedasset.ExpectedSchema{Metric: schema.Metric, VectorDimensions: schema.VectorDimensions, CodeDimensions: schema.CodeDimensions, CodeWidthBits: schema.CodeWidthBits, RowCount: schema.RowCount, OrdinalOrder: schema.OrdinalOrder, Codec: schema.Codec, BaseGraph: schema.BaseGraph, RequiredRoles: columnVectorGraphQuantizedRequiredRoles(q)}
	expected.Codec.Config = append([]byte(nil), expected.Codec.Config...)
	expected.Codec.Config[len(expected.Codec.Config)-1] ^= 1
	if err := quantizedasset.Validate(schema, expected); err == nil || !strings.Contains(err.Error(), "config bytes") {
		t.Fatalf("config bytes mismatch err=%v want config bytes", err)
	}
	expected = quantizedasset.ExpectedSchema{Metric: schema.Metric, VectorDimensions: schema.VectorDimensions, CodeDimensions: schema.CodeDimensions, CodeWidthBits: schema.CodeWidthBits, RowCount: schema.RowCount, OrdinalOrder: schema.OrdinalOrder, Codec: schema.Codec, BaseGraph: schema.BaseGraph, RequiredRoles: columnVectorGraphQuantizedRequiredRoles(q)}
	expected.BaseGraph.BaseManifestChecksum++
	if err := quantizedasset.Validate(schema, expected); err == nil || !strings.Contains(err.Error(), "base graph identity mismatch") {
		t.Fatalf("base graph mismatch err=%v want identity mismatch", err)
	}
	mismatchedGraph := graph
	mismatchedGraph.BaseManifestChecksum++
	if columnVectorIndexStateMatchesGraph(state, mismatchedGraph) {
		t.Fatalf("state matched graph with mismatched base manifest checksum")
	}
}

func BenchmarkColumnGraphQuantizedAssetBuildPrepare2450(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}
	rows := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	assetRows := make([]columnVectorGraphAssetRow, len(rows))
	for i, row := range rows {
		assetRows[i] = columnGraphQuantizedAssetRow1926(b, row.id, row.vector)
	}
	base, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(shape.dims))
	if err != nil {
		b.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	baseGraph := columnGraphRebuildVectorIndexDefinitionV2A(shape.dims, shape.m)
	cases := []struct {
		name string
		q    QuantizedVectorIndexDefinition
	}{
		{name: "scalar_u8", q: QuantizedVectorIndexDefinition{Name: columnGraphScalarU8QuantizedBenchIndexName1926, Codec: QuantizedVectorCodecScalarU8}},
		{name: "rabitq_1bit", q: QuantizedVectorIndexDefinition{Name: columnGraphRabitQQuantizedIndexName2450, Codec: rabitq.CodecName}},
	}
	for _, tc := range cases {
		tc := tc
		def := baseGraph
		def.QuantizedIndexes = []QuantizedVectorIndexDefinition{tc.q}
		def, err = normalizeVectorIndexDefinition(def)
		if err != nil {
			b.Fatalf("normalizeVectorIndexDefinition %s: %v", tc.name, err)
		}
		q := def.QuantizedIndexes[0]
		payload, sourceCfg, err := prepareColumnVectorGraphQuantizedCodesPayload("docs", *base, def, q, 245000, assetRows)
		if err != nil {
			b.Fatalf("warm build %s: %v", tc.name, err)
		}
		graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: 1, BaseManifestChecksum: 2, BaseSchemaHash: base.SchemaHash, GraphSchemaHash: 3, RowCount: len(assetRows)}
		logicalCodeBytes := float64(def.Dimensions)
		if q.Codec == rabitq.CodecName {
			plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
			if err != nil {
				b.Fatalf("rabitq.NewPlan: %v", err)
			}
			logicalCodeBytes = float64(plan.BytesPerCode())
		}
		b.Run("build/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(payload))/float64(len(assetRows)), "asset_B/vector")
			b.ReportMetric(logicalCodeBytes, "logical_code_B/vector")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, gotCfg, err := prepareColumnVectorGraphQuantizedCodesPayload("docs", *base, def, q, uint64(245001+i), assetRows)
				if err != nil {
					b.Fatalf("build %s: %v", tc.name, err)
				}
				columnPhysicalScanBenchSum += int64(len(got)) + int64(gotCfg.SchemaHash&0xffff)
			}
		})
		b.Run("prepare/"+tc.name, func(b *testing.B) {
			asset := columnVectorIndexStateAssetSnapshot{SourceSchemaHash: sourceCfg.SchemaHash, AssetBytes: int64(len(payload))}
			asset.LogicalType, asset.PhysicalEncoding = columnVectorGraphQuantizedAssetStateType(q)
			b.ReportAllocs()
			b.ReportMetric(float64(len(payload))/float64(len(assetRows)), "asset_B/vector")
			b.ReportMetric(logicalCodeBytes, "logical_code_B/vector")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				image, err := typedcolumn.ParseColumnPartImage(payload)
				if err != nil {
					b.Fatalf("ParseColumnPartImage %s: %v", tc.name, err)
				}
				schema, err := columnVectorGraphQuantizedAssetSchema(def, graph, q, asset, quantizedasset.AssetRefIdentity{})
				if err != nil {
					b.Fatalf("schema %s: %v", tc.name, err)
				}
				prepared, err := quantizedasset.Prepare(quantizedasset.PrepareRequest{
					Schema: schema,
					Expected: quantizedasset.ExpectedSchema{
						Metric:           schema.Metric,
						VectorDimensions: schema.VectorDimensions,
						CodeDimensions:   schema.CodeDimensions,
						CodeWidthBits:    schema.CodeWidthBits,
						RowCount:         schema.RowCount,
						OrdinalOrder:     schema.OrdinalOrder,
						Codec:            schema.Codec,
						BaseGraph:        schema.BaseGraph,
						RequiredRoles:    columnVectorGraphQuantizedRequiredRoles(q),
					},
					Parts: []quantizedasset.PartImageSource{{Image: image, AssetBytes: asset.AssetBytes, SourceSchemaHash: asset.SourceSchemaHash}},
				})
				if err != nil {
					b.Fatalf("Prepare %s: %v", tc.name, err)
				}
				if err := validateColumnVectorGraphQuantizedPreparedAsset(def, q, prepared); err != nil {
					b.Fatalf("validate prepared %s: %v", tc.name, err)
				}
				columnPhysicalScanBenchSum += int64(prepared.Rows())
			}
		})
	}
}

func BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, efConstruction: 128, topK: 10, efSearch: 128, queryOrdinal: 37, queryCount: 1}
	_, d, col, def, _ := openColumnGraphRabitQQuantizedBenchCollection2450(b, shape)
	defer func() { _ = d.Close() }()
	b.ReportAllocs()
	reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, shape)
	b.ReportMetric(1, "quantized_indexes")
	plan, err := rabitq.NewPlan(shape.dims, rabitq.DefaultConfig())
	if err != nil {
		b.Fatalf("rabitq.NewPlan: %v", err)
	}
	b.ReportMetric(float64(plan.BytesPerCode()), "rabitq_logical_code_B/vector")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, err := col.RebuildVectorIndex(def.Name)
		if err != nil {
			b.Fatalf("RebuildVectorIndex: %v", err)
		}
		if !status.Loaded || status.RebuildNeeded {
			b.Fatalf("status=%+v, want loaded", status)
		}
		columnGraphRebuildBenchSinkV2A = status
	}
	b.StopTimer()
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	reportColumnGraphRabitQQuantizedStorageMetrics2450(b, d, def, shape)
}

func openColumnGraphRabitQQuantizedTestCollection2450(tb testing.TB, rows []columnGraphRebuildInputRowV2A) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return openColumnGraphQuantizedTestCollection1926(tb, rows, []QuantizedVectorIndexDefinition{{Name: columnGraphRabitQQuantizedIndexName2450, Codec: rabitq.CodecName}})
}

func loadColumnGraphRabitQQuantizedState2450(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition) (*ColumnStoreConfig, columnVectorGraphManifestSnapshot, columnVectorIndexStateSnapshot, columnVectorIndexStateAssetSnapshot) {
	tb.Helper()
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, "docs")
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		tb.Fatalf("missing graph record %q", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		tb.Fatalf("decode graph: %v", err)
	}
	stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		tb.Fatalf("missing vector-index state record %q", def.Name)
	}
	state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		tb.Fatalf("decode state: %v", err)
	}
	assets := columnVectorGraphQuantizedAssetByName(state, def)
	asset, ok := assets[def.QuantizedIndexes[0].Name]
	if !ok {
		tb.Fatalf("rabitq quantized asset missing from state assets: %+v", state.Assets)
	}
	return cfg, graph, state, asset
}

func assertPreparedRabitQRows2450(tb testing.TB, prepared *quantizedasset.Prepared, def VectorIndexDefinition, rows []columnGraphRebuildScannedRowV2A) {
	tb.Helper()
	if prepared == nil {
		tb.Fatal("prepared rabitq asset is nil")
	}
	if prepared.Rows() != len(rows) {
		tb.Fatalf("prepared rows=%d want %d", prepared.Rows(), len(rows))
	}
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		tb.Fatalf("rabitq.NewPlan: %v", err)
	}
	view, ok := prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok || view.Rows() != len(rows) || view.BytesPerRow() != plan.BytesPerCode() || view.ElementsPerRow() != plan.CodeDimensions() {
		tb.Fatalf("rabitq code view=%+v ok=%v", view, ok)
	}
	var ws rabitq.Workspace
	var codeScratch []byte
	for ordinal, row := range rows {
		encoded, err := plan.Encode(codeScratch, row.vector, &ws)
		if err != nil {
			tb.Fatalf("rabitq Encode row %d: %v", ordinal, err)
		}
		codeScratch = encoded.Code
		gotCode, ok := view.RowBytes(ordinal)
		if !ok || !bytes.Equal(gotCode, encoded.Code) {
			tb.Fatalf("row %d code=%v ok=%v want %v", ordinal, gotCode, ok, encoded.Code)
		}
		gotCount, ok := prepared.Uint32(quantizedasset.RoleCodeCount, ordinal)
		if !ok || gotCount != encoded.CodeCount {
			tb.Fatalf("row %d code_count=%d ok=%v want %d", ordinal, gotCount, ok, encoded.CodeCount)
		}
		gotQDP, ok := prepared.Float32(quantizedasset.RoleQuantizedDotProductInv, ordinal)
		if !ok || gotQDP != encoded.QuantizedDotProductInv {
			tb.Fatalf("row %d qdp_inv=%v ok=%v want %v", ordinal, gotQDP, ok, encoded.QuantizedDotProductInv)
		}
	}
}

func writeUncheckedRabitQQuantizedAsset2450(tb testing.TB, d *backenddb.DB, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, rows []columnGraphRebuildInputRowV2A, partID uint64, mutate func([]byte, typedcolumn.ColumnPartImage)) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	assetRows := make([]columnVectorGraphAssetRow, len(rows))
	for i, row := range rows {
		assetRows[i] = columnGraphQuantizedAssetRow1926(tb, string(row.id), row.vector)
	}
	payload, sourceCfg, err := prepareColumnVectorGraphQuantizedCodesPayload("docs", cfg, def, q, partID, assetRows)
	if err != nil {
		tb.Fatalf("prepare rabitq payload: %v", err)
	}
	raw := append([]byte(nil), payload...)
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage before mutate: %v", err)
	}
	if mutate != nil {
		mutate(raw, image)
	}
	return appendRabitQRawQuantizedAsset2450(tb, d, sourceCfg, q, graph, partID, raw)
}

func writeWrongShapeRabitQQuantizedAsset2450(tb testing.TB, d *backenddb.DB, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, partID uint64, rows int) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig("docs", cfg, def, q)
	if err != nil {
		tb.Fatalf("columnVectorGraphQuantizedCodesColumnStoreConfig: %v", err)
	}
	primaryIDs := make([]int64, rows)
	for i := range primaryIDs {
		primaryIDs[i] = int64(i)
	}
	packed, err := typedcolumn.NewPackedUintRows(rows, 2, rabitq.CodeWidthBits, make([]byte, rows))
	if err != nil {
		tb.Fatalf("NewPackedUintRows wrong shape: %v", err)
	}
	payload := buildRabitQTestPart2450(tb, partID, uint32(sourceCfg.SchemaHash), rows, []typedcolumn.ColumnDefinition{
		columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
		{Name: columnVectorGraphQuantizedPackedCodesColumnName, Type: typedcolumn.ColumnTypePackedBitVector, Encoding: typedcolumn.EncodingRawPackedBitVector, FixedWidthElements: 2, BitsPerElement: rabitq.CodeWidthBits, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: columnVectorGraphQuantizedCodeCountColumnName, Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: columnVectorGraphQuantizedDotProductInvColumnName, Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{
		Rows:              rows,
		Columns:           map[string][]int64{typedColumnAdapterPrimaryIDColumn: primaryIDs},
		PackedUintColumns: map[string]typedcolumn.PackedUintRows{columnVectorGraphQuantizedPackedCodesColumnName: packed},
		Uint32Columns:     map[string][]uint32{columnVectorGraphQuantizedCodeCountColumnName: make([]uint32, rows)},
		Float32Columns:    map[string][]float32{columnVectorGraphQuantizedDotProductInvColumnName: filledFloat32Slice2450(rows, 1)},
	})
	return appendRabitQRawQuantizedAsset2450(tb, d, sourceCfg, q, graph, partID, payload)
}

func writeOutOfRangeQDPRabitQQuantizedAsset2450(tb testing.TB, d *backenddb.DB, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, rowsIn []columnGraphRebuildInputRowV2A, partID uint64) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig("docs", cfg, def, q)
	if err != nil {
		tb.Fatalf("columnVectorGraphQuantizedCodesColumnStoreConfig: %v", err)
	}
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		tb.Fatalf("rabitq.NewPlan: %v", err)
	}
	primaryIDs := make([]int64, len(rowsIn))
	codes := make([]byte, len(rowsIn)*plan.BytesPerCode())
	codeCounts := make([]uint32, len(rowsIn))
	var ws rabitq.Workspace
	var scratch []byte
	for i, row := range rowsIn {
		primaryIDs[i] = int64(i)
		encoded, err := plan.Encode(scratch, row.vector, &ws)
		if err != nil {
			tb.Fatalf("rabitq Encode row %d: %v", i, err)
		}
		scratch = encoded.Code
		copy(codes[i*plan.BytesPerCode():(i+1)*plan.BytesPerCode()], encoded.Code)
		codeCounts[i] = encoded.CodeCount
	}
	packed, err := typedcolumn.NewPackedUintRows(len(rowsIn), plan.CodeDimensions(), rabitq.CodeWidthBits, codes)
	if err != nil {
		tb.Fatalf("NewPackedUintRows: %v", err)
	}
	payload := buildRabitQTestPart2450(tb, partID, uint32(sourceCfg.SchemaHash), len(rowsIn), []typedcolumn.ColumnDefinition{
		columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
		{Name: columnVectorGraphQuantizedPackedCodesColumnName, Type: typedcolumn.ColumnTypePackedBitVector, Encoding: typedcolumn.EncodingRawPackedBitVector, FixedWidthElements: plan.CodeDimensions(), BitsPerElement: rabitq.CodeWidthBits, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: columnVectorGraphQuantizedCodeCountColumnName, Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: columnVectorGraphQuantizedDotProductInvColumnName, Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{
		Rows:              len(rowsIn),
		Columns:           map[string][]int64{typedColumnAdapterPrimaryIDColumn: primaryIDs},
		PackedUintColumns: map[string]typedcolumn.PackedUintRows{columnVectorGraphQuantizedPackedCodesColumnName: packed},
		Uint32Columns:     map[string][]uint32{columnVectorGraphQuantizedCodeCountColumnName: codeCounts},
		Float32Columns:    map[string][]float32{columnVectorGraphQuantizedDotProductInvColumnName: filledFloat32Slice2450(len(rowsIn), 2)},
	})
	return appendRabitQRawQuantizedAsset2450(tb, d, sourceCfg, q, graph, partID, payload)
}

func writeMissingSideRabitQQuantizedAsset2450(tb testing.TB, d *backenddb.DB, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, rowsIn []columnGraphRebuildInputRowV2A, partID uint64) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig("docs", cfg, def, q)
	if err != nil {
		tb.Fatalf("columnVectorGraphQuantizedCodesColumnStoreConfig: %v", err)
	}
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		tb.Fatalf("rabitq.NewPlan: %v", err)
	}
	primaryIDs := make([]int64, len(rowsIn))
	codes := make([]byte, len(rowsIn)*plan.BytesPerCode())
	var ws rabitq.Workspace
	var scratch []byte
	for i, row := range rowsIn {
		primaryIDs[i] = int64(i)
		encoded, err := plan.Encode(scratch, row.vector, &ws)
		if err != nil {
			tb.Fatalf("rabitq Encode row %d: %v", i, err)
		}
		scratch = encoded.Code
		copy(codes[i*plan.BytesPerCode():(i+1)*plan.BytesPerCode()], encoded.Code)
	}
	packed, err := typedcolumn.NewPackedUintRows(len(rowsIn), plan.CodeDimensions(), rabitq.CodeWidthBits, codes)
	if err != nil {
		tb.Fatalf("NewPackedUintRows: %v", err)
	}
	payload := buildRabitQTestPart2450(tb, partID, uint32(sourceCfg.SchemaHash), len(rowsIn), []typedcolumn.ColumnDefinition{
		columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
		{Name: columnVectorGraphQuantizedPackedCodesColumnName, Type: typedcolumn.ColumnTypePackedBitVector, Encoding: typedcolumn.EncodingRawPackedBitVector, FixedWidthElements: plan.CodeDimensions(), BitsPerElement: rabitq.CodeWidthBits, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: columnVectorGraphQuantizedCodeCountColumnName, Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{
		Rows:              len(rowsIn),
		Columns:           map[string][]int64{typedColumnAdapterPrimaryIDColumn: primaryIDs},
		PackedUintColumns: map[string]typedcolumn.PackedUintRows{columnVectorGraphQuantizedPackedCodesColumnName: packed},
		Uint32Columns:     map[string][]uint32{columnVectorGraphQuantizedCodeCountColumnName: make([]uint32, len(rowsIn))},
	})
	return appendRabitQRawQuantizedAsset2450(tb, d, sourceCfg, q, graph, partID, payload)
}

func buildRabitQTestPart2450(tb testing.TB, partID uint64, schemaVersion uint32, rows int, columns []typedcolumn.ColumnDefinition, batch typedcolumn.Batch) []byte {
	tb.Helper()
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{SchemaVersion: schemaVersion, SchemaMode: typedcolumn.ColumnSchemaFixed, Columns: columns, LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}}, SortKey: typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}}, PartPolicy: typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule}, Compression: typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone}}, batch)
	if err != nil {
		tb.Fatalf("BuildColumnPart: %v", err)
	}
	if part.Descriptor.RowCount != rows {
		tb.Fatalf("part rows=%d want %d", part.Descriptor.RowCount, rows)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{columnVectorGraphQuantizedPackedCodesColumnName: columnVectorIndexStateLogicalTypePackedBitVector, columnVectorGraphQuantizedCodeCountColumnName: "uint32", columnVectorGraphQuantizedDotProductInvColumnName: columnVectorIndexStateLogicalTypeFloat32}})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage: %v", err)
	}
	return image.Bytes
}

func appendRabitQRawQuantizedAsset2450(tb testing.TB, d *backenddb.DB, sourceCfg ColumnStoreConfig, q QuantizedVectorIndexDefinition, graph columnVectorGraphManifestSnapshot, partID uint64, raw []byte) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	appender, err := newNextColumnPhysicalAssetSegmentAppender(d.ColumnAssetRootDir(), sourceCfg)
	if err != nil {
		tb.Fatalf("new appender: %v", err)
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, sourceCfg)
	ref, appendErr := appender.appendKindWithAlignment(raw, ColumnAssetKindTCS1TypedColumnPart, graph.BaseManifestGeneration, partID, alignment)
	closeErr := appender.close()
	if appendErr != nil || closeErr != nil {
		tb.Fatalf("append rabitq asset append=%v close=%v", appendErr, closeErr)
	}
	logicalType, physicalEncoding := columnVectorGraphQuantizedAssetStateType(q)
	return columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedCodes, AssetID: columnVectorGraphQuantizedAssetID(q), LogicalType: logicalType, PhysicalEncoding: physicalEncoding, RowCount: graph.RowCount, SourceSchemaHash: sourceCfg.SchemaHash, Ref: ref, AssetBytes: ref.Length}
}

func mustColumnDataSection2450(tb testing.TB, image typedcolumn.ColumnPartImage, column string) typedcolumn.ColumnPartImageSection {
	tb.Helper()
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			return section
		}
	}
	tb.Fatalf("missing data section %q in %+v", column, image.Sections)
	return typedcolumn.ColumnPartImageSection{}
}

func filledFloat32Slice2450(n int, value float32) []float32 {
	out := make([]float32, n)
	for i := range out {
		out[i] = value
	}
	return out
}

func openColumnGraphRabitQQuantizedBenchCollection2450(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	if shape.rows <= 0 || shape.dims <= 0 {
		tb.Fatalf("invalid benchmark shape rows=%d dims=%d", shape.rows, shape.dims)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphScalarU8QuantizedBenchVectorIndexDefinition1926(tb, shape)
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphRabitQQuantizedIndexName2450, Codec: rabitq.CodecName}}
	var err error
	def, err = normalizeVectorIndexDefinition(def)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(shape.dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	rows := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	insertColumnGraphScalarU8QuantizedBenchRows1926(tb, col, shape, rows)
	return dir, d, col, def, rows
}

func reportColumnGraphRabitQQuantizedStorageMetrics2450(b *testing.B, d *backenddb.DB, def VectorIndexDefinition, shape columnGraphScalarU8QuantizedBenchShape1926) {
	b.Helper()
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(b, d, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	b.ReportMetric(float64(graph.AssetBytes), "graph_asset_B/op")
	b.ReportMetric(float64(columnVectorIndexStateAssetsStorageBytes(state)), "state_assets_B/op")
	b.ReportMetric(float64(columnVectorGraphStorageBytesWithState(graph, state)), "graph_total_storage_B/op")
	plan, err := rabitq.NewPlan(shape.dims, rabitq.DefaultConfig())
	if err != nil {
		b.Fatalf("rabitq.NewPlan: %v", err)
	}
	b.ReportMetric(float64(plan.BytesPerCode()), "quantized_code_B/vector")
	b.ReportMetric(float64(shape.dims*4), "exact_vector_B/vector")
	b.ReportMetric(4, "exact_norm_B/vector")
	b.ReportMetric(float64(shape.dims*4+4), "exact_vector_norm_B/vector")
	var quantizedAssets int
	var quantizedBytes int64
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes {
			quantizedAssets++
			quantizedBytes += asset.AssetBytes
		}
	}
	b.ReportMetric(float64(quantizedAssets), "quantized_assets/op")
	b.ReportMetric(float64(quantizedBytes), "quantized_asset_B/op")
	if shape.rows > 0 {
		b.ReportMetric(float64(quantizedBytes)/float64(shape.rows), "quantized_asset_B/vector")
	}
	if got := math.Ceil(float64(plan.CodeDimensions()) / 8); got != float64(plan.BytesPerCode()) {
		b.Fatalf("logical code bytes mismatch got ceil=%v bytes=%d", got, plan.BytesPerCode())
	}
}
