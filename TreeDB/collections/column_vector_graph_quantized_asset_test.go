package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

func TestColumnGraphScalarU8QuantizedAssetRebuildPrepareReopen1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0}},
	}
	dir, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		t.Fatalf("missing graph record %q", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decode graph: %v", err)
	}
	stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		t.Fatalf("missing vector-index state record %q", def.Name)
	}
	state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		t.Fatalf("decode state: %v", err)
	}
	assets := columnVectorGraphQuantizedAssetByName(state, def)
	asset, ok := assets[def.QuantizedIndexes[0].Name]
	if !ok {
		t.Fatalf("quantized asset for %q missing in state assets: %+v", def.QuantizedIndexes[0].Name, state.Assets)
	}
	if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes || asset.AssetID != columnVectorGraphQuantizedCodesAssetID(def.QuantizedIndexes[0]) || asset.RowCount != len(rows) || asset.AssetBytes <= 0 {
		t.Fatalf("quantized asset snapshot=%+v", asset)
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, state, graph); err != nil {
		t.Fatalf("validate state assets with quantized asset: %v", err)
	}
	missingQuantized := state
	missingQuantized.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	for i := range missingQuantized.Assets {
		if missingQuantized.Assets[i].Role == columnVectorIndexStateAssetRoleQuantizedCodes {
			missingQuantized.Assets = append(missingQuantized.Assets[:i], missingQuantized.Assets[i+1:]...)
			break
		}
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, missingQuantized, graph); err == nil || !strings.Contains(err.Error(), "missing quantized asset") {
		t.Fatalf("validate missing quantized asset err=%v want missing quantized asset", err)
	}
	noQuantizedDef := def
	noQuantizedDef.QuantizedIndexes = nil
	if columnVectorIndexStateDefinitionParametersMatch(&state, &noQuantizedDef) {
		t.Fatalf("state with quantized asset matched definition without quantized declarations")
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, noQuantizedDef, state, graph); err == nil || !strings.Contains(err.Error(), "unexpected quantized asset") {
		t.Fatalf("validate unexpected quantized asset err=%v want unexpected quantized asset", err)
	}
	prepared, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, def.QuantizedIndexes[0], asset)
	if err != nil {
		t.Fatalf("loadColumnVectorGraphQuantizedAsset: %v", err)
	}
	if prepared.Rows() != len(rows) {
		t.Fatalf("prepared rows=%d want %d", prepared.Rows(), len(rows))
	}
	row0, ok := prepared.CodeRowBytes("codes", 0)
	if !ok || !bytes.Equal(row0, []byte{218, 128, 37}) {
		t.Fatalf("row0 codes=%v ok=%v", row0, ok)
	}
	row1, ok := prepared.CodeRowBytes("codes", 1)
	if !ok || !bytes.Equal(row1, []byte{87, 248, 128}) {
		t.Fatalf("row1 codes=%v ok=%v", row1, ok)
	}
	row2, ok := prepared.CodeRowBytes("codes", 2)
	if !ok || !bytes.Equal(row2, []byte{213, 42, 170}) {
		t.Fatalf("row2 codes=%v ok=%v", row2, ok)
	}

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	loaded := reader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]
	if loaded.Prepared == nil || loaded.Err != nil {
		_ = reader.Close()
		t.Fatalf("reader quantized status=%+v", loaded)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
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
	reopenedStatus := reopenedReader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]
	if reopenedStatus.Prepared == nil || reopenedStatus.Err != nil {
		t.Fatalf("reopened reader quantized status=%+v", reopenedStatus)
	}
	quantized, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("quantized SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantized.Results, scalarU8QuantizedTopKForTest1926(t, rows, []float32{1, 0, 0}, 1), false)
	if quantized.Stats.QuantizedScoreCalls == 0 || quantized.Stats.PreparedScoreCalls != 0 {
		t.Fatalf("quantized stats=%+v want scalar_u8 scorer and no exact prepared scoring", quantized.Stats)
	}
	reranked, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: def.QuantizedIndexes[0].Name, QuantizedRerankCandidates: len(rows), TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("quantized_rerank SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, []float32{1, 0, 0}, 1), false)
	if reranked.Stats.QuantizedScoreCalls == 0 || reranked.Stats.QuantizedRerankExactScoreCalls == 0 || reranked.Stats.VectorBytesRead == 0 || reranked.Stats.NormBytesRead == 0 {
		t.Fatalf("reranked stats=%+v want reopened scalar_u8 traversal plus exact rerank", reranked.Stats)
	}
	exact, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil || len(exact.Results) != 1 {
		t.Fatalf("exact SearchVectorIndex results=%d err=%v", len(exact.Results), err)
	}
}

func TestColumnGraphScalarU8QuantizedOnlyUsesPreparedCodes1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-exact", vector: []float32{0.40633525, -0.06700023, -0.027197814}},
		{id: "doc-quantized", vector: []float32{-0.22174846, 0.8332732, 0.28568664}},
	}
	query := []float32{-0.23968919, -0.60389674, 0.9352316}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	exact, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeExact, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("exact SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, exact.Results, exactColumnGraphTopKForTest(t, rows, query, 1), false)
	quantized, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("quantized_only SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantized.Results, scalarU8QuantizedTopKForTest1926(t, rows, query, 1), false)
	if string(exact.Results[0].ID) != "doc-exact" || string(quantized.Results[0].ID) != "doc-quantized" {
		t.Fatalf("exact top=%q quantized top=%q want quantized scoring to differ from exact on fixture", exact.Results[0].ID, quantized.Results[0].ID)
	}
	if quantized.Stats.QuantizedScoreCalls == 0 || quantized.Stats.PreparedScoreCalls != 0 || quantized.Stats.VectorBytesRead != 0 || quantized.Stats.NormBytesRead != 0 {
		t.Fatalf("quantized stats=%+v want prepared scalar_u8 code scoring without exact vector/norm scoring", quantized.Stats)
	}
}

func TestColumnGraphScalarU8CenteredQueryScratch2258(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.25, -0.5, 0.75}},
		{id: "doc-c", vector: []float32{-0.75, 0.25, 0.5}},
	}
	query := []float32{0.2, -0.4, 0.9}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}

	if _, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query[:2], queryInvNorm, &columnVectorGraphNativeSearchScratch{}); !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("prepare short query err=%v want dimension mismatch", err)
	}
	if _, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, nil); !errors.Is(err, errColumnVectorGraphNativeSearchScratchRequired) {
		t.Fatalf("prepare nil scratch err=%v want scratch required", err)
	}

	var scratch columnVectorGraphNativeSearchScratch
	scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("prepare scorer: %v", err)
	}
	if len(scorer.queryCode) != def.Dimensions || !scorer.centeredQuery.ValidForDims(def.Dimensions) {
		t.Fatalf("scorer query_code_len=%d centered=%+v dims=%d", len(scorer.queryCode), scorer.centeredQuery, def.Dimensions)
	}
	for i, code := range scorer.queryCode {
		want := vectorops.ScalarU8CenteredValue(code)
		if got := scorer.centeredQuery.Values[i]; got != want {
			t.Fatalf("centered query[%d]=%d want %d from code %d", i, got, want, code)
		}
	}
	row, ok := scorer.codeRows.RowBytes(1)
	if !ok {
		t.Fatal("row 1 code bytes unavailable")
	}
	got, err := scorer.scoreOrdinal(1, nil)
	if err != nil {
		t.Fatalf("scoreOrdinal: %v", err)
	}
	var legacyDot int64
	for i, qc := range scorer.queryCode {
		q := int64(2*int(qc) - 255)
		c := int64(2*int(row[i]) - 255)
		legacyDot += q * c
	}
	want := float64(legacyDot) / columnVectorGraphScalarU8CodeScale
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("centered score=%v want legacy=%v", got, want)
	}

	_, err = reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("warm prepare scorer: %v", err)
	}
	var stats columnVectorGraphNativeSearchStats
	allocs := testing.AllocsPerRun(1000, func() {
		scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
		if err != nil {
			panic(err)
		}
		score, err := scorer.scoreOrdinal(1, &stats)
		if err != nil {
			panic(err)
		}
		columnPhysicalScanBenchSum += int64(score * 1_000_000)
	})
	if allocs != 0 {
		t.Fatalf("steady-state centered scalar_u8 prepare+score allocs/run=%v want 0", allocs)
	}
}

func TestColumnGraphScalarU8QuantizedRerankExactRanksCandidateSet1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-exact", vector: []float32{0.40633525, -0.06700023, -0.027197814}},
		{id: "doc-quantized", vector: []float32{-0.22174846, 0.8332732, 0.28568664}},
	}
	query := []float32{-0.23968919, -0.60389674, 0.9352316}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	exactWant := exactColumnGraphTopKForTest(t, rows, query, 1)
	quantizedWant := scalarU8QuantizedTopKForTest1926(t, rows, query, 1)
	if string(exactWant[0].ID) != "doc-exact" || string(quantizedWant[0].ID) != "doc-quantized" {
		t.Fatalf("fixture exact=%q quantized=%q want differing top candidates", exactWant[0].ID, quantizedWant[0].ID)
	}

	limited, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: def.QuantizedIndexes[0].Name, QuantizedRerankCandidates: 1, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("limited quantized_rerank SearchVectorIndex: %v", err)
	}
	if len(limited.Results) != 1 || string(limited.Results[0].ID) != "doc-quantized" {
		t.Fatalf("limited quantized_rerank results=%+v want only quantized candidate doc-quantized", limited.Results)
	}
	wantLimitedScore := exactColumnGraphScoreForTest1926(t, rows[1].vector, query)
	if math.Abs(limited.Results[0].Score-wantLimitedScore) > 1e-6 || math.Abs(limited.Results[0].Score-quantizedWant[0].Score) < 1e-6 {
		t.Fatalf("limited quantized_rerank score=%v want exact candidate score=%v and not quantized score=%v", limited.Results[0].Score, wantLimitedScore, quantizedWant[0].Score)
	}
	if limited.Stats.QuantizedScoreCalls == 0 || limited.Stats.QuantizedRerankCandidates != 1 || limited.Stats.QuantizedRerankExactScoreCalls != 1 || limited.Stats.VectorBytesRead == 0 || limited.Stats.NormBytesRead == 0 {
		t.Fatalf("limited quantized_rerank stats=%+v want quantized traversal plus one exact rerank", limited.Stats)
	}

	wide, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: def.QuantizedIndexes[0].Name, QuantizedRerankCandidates: 2, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("wide quantized_rerank SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, wide.Results, exactWant, false)
	if wide.Stats.QuantizedRerankCandidates != 2 || wide.Stats.QuantizedRerankExactScoreCalls != 2 {
		t.Fatalf("wide quantized_rerank stats=%+v want two exact-reranked candidates", wide.Stats)
	}

	defaulted, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("default quantized_rerank SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, defaulted.Results, exactWant, false)
	if defaulted.Stats.QuantizedRerankCandidates != uint64(len(rows)) {
		t.Fatalf("default quantized_rerank stats=%+v want normalized ef_search candidates", defaulted.Stats)
	}

	_, err = col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: def.QuantizedIndexes[0].Name, QuantizedRerankCandidates: 1, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err == nil || !strings.Contains(err.Error(), "rerank candidates") || !strings.Contains(err.Error(), "top_k") {
		t.Fatalf("quantized_rerank candidates < top_k err=%v want validation failure", err)
	}
}

func TestColumnGraphScalarU8QuantizedRerankTraversesEfSearchBeforeTrim1926(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		columnGraphQuantizedAssetRow1926(t, "doc-a", []float32{-1, 0, 0}),
		columnGraphQuantizedAssetRow1926(t, "doc-b", []float32{1, 0, 0}),
		columnGraphQuantizedAssetRow1926(t, "doc-c", []float32{0, -1, 0}),
		columnGraphQuantizedAssetRow1926(t, "doc-target", []float32{0, 0, 1}),
	}
	query := []float32{0, 0, 1}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeAndAdjacencyState1989(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	q := attachScalarU8QuantizedAssetForReader1926(t, reader, rows)

	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{
		TopK:                      1,
		EfSearch:                  len(rows),
		QueryMode:                 columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		QuantizedIndexName:        q.Name,
		QuantizedRerankCandidates: 2,
		OmitResultMaterialization: true,
	}, &scratch)
	if err != nil {
		t.Fatalf("quantized_rerank SearchCosine: %v", err)
	}
	if len(got) != 1 || got[0].Ordinal != 3 {
		t.Fatalf("quantized_rerank results=%+v want later row 3 discovered by ef_search fallback seeding", got)
	}
	if stats.Candidates != uint64(len(rows)) || stats.QuantizedScoreCalls != uint64(len(rows)) {
		t.Fatalf("quantized_rerank stats=%+v want traversal to score normalized ef_search candidate pool", stats)
	}
	if stats.QuantizedRerankCandidates != 2 || stats.QuantizedRerankExactScoreCalls != 2 {
		t.Fatalf("quantized_rerank rerank stats=%+v want exact rerank of trimmed quantized shortlist only", stats)
	}
}

func TestColumnGraphScalarU8QuantizedRerankWavefrontKeepsEfSearchTraversal1926(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("quantized_rerank wavefront regression requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityShape2091{rows: 4, dims: 3, degree: 1, topK: 1, efSearch: 4, queryOrdinal: 0}
	rows := []columnVectorGraphAssetRow{
		columnGraphQuantizedAssetRow1926(t, "doc-a", []float32{1, 0, 0}),
		columnGraphQuantizedAssetRow1926(t, "doc-b", []float32{0, 1, 0}),
		columnGraphQuantizedAssetRow1926(t, "doc-c", []float32{0, 0, 1}),
		columnGraphQuantizedAssetRow1926(t, "doc-d", []float32{-1, 0, 0}),
	}
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	q := attachScalarU8QuantizedAssetForReader1926(t, reader, rows)

	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{
		TopK:                      shape.topK,
		EfSearch:                  shape.efSearch,
		ScoreBatchMode:            columnVectorGraphScoreBatchModeIndexed,
		QueryMode:                 columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		QuantizedIndexName:        q.Name,
		QuantizedRerankCandidates: 2,
		TraversalMode:             columnVectorGraphNativeSearchTraversalModeWavefront,
		WavefrontWidth:            3,
		OmitResultMaterialization: true,
	}, &scratch)
	if err != nil {
		t.Fatalf("quantized_rerank wavefront SearchCosine: %v", err)
	}
	if len(got) != 1 || got[0].Ordinal != 0 {
		t.Fatalf("quantized_rerank wavefront results=%+v want exact-reranked top ordinal 0", got)
	}
	if stats.Candidates != uint64(shape.efSearch) || stats.QuantizedScoreCalls != uint64(shape.efSearch) {
		t.Fatalf("quantized_rerank wavefront stats=%+v want traversal to keep normalized ef_search candidate pool", stats)
	}
	if stats.QuantizedRerankCandidates != 2 || stats.QuantizedRerankExactScoreCalls != 2 {
		t.Fatalf("quantized_rerank wavefront rerank stats=%+v want exact rerank of trimmed quantized shortlist only", stats)
	}
}

func TestColumnGraphScalarU8QuantizedOnlyMultipleIndexes1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	quantizedIndexes := []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.fast"}, {Name: "embedding.scalar_u8.recall"}}
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, quantizedIndexes)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{0, 1, 0}
	wantQuantized := scalarU8QuantizedTopKForTest1926(t, rows, query, 2)
	wantExact := exactColumnGraphTopKForTest(t, rows, query, 2)
	for _, q := range def.QuantizedIndexes {
		got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("quantized_only %q: %v", q.Name, err)
		}
		assertVectorIndexSearchResultsV4(t, got.Results, wantQuantized, false)
		if got.Stats.QuantizedScoreCalls == 0 {
			t.Fatalf("quantized stats for %q=%+v want scorer use", q.Name, got.Stats)
		}
		reranked, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: q.Name, QuantizedRerankCandidates: len(rows), TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("quantized_rerank %q: %v", q.Name, err)
		}
		assertVectorIndexSearchResultsV4(t, reranked.Results, wantExact, false)
		if reranked.Stats.QuantizedScoreCalls == 0 || reranked.Stats.QuantizedRerankExactScoreCalls == 0 {
			t.Fatalf("rerank stats for %q=%+v want quantized traversal and exact rerank", q.Name, reranked.Stats)
		}
	}
	if _, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: "embedding.scalar_u8.missing", TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}); !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "is not declared") {
		t.Fatalf("missing quantized index err=%v want declared-name fail-closed", err)
	}
}

func TestColumnGraphScalarU8QuantizedModesConcurrentSearch1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.5, 0.5, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{1, 0, 0}
	wantQuantized := scalarU8QuantizedTopKForTest1926(t, rows, query, 2)
	wantReranked := exactColumnGraphTopKForTest(t, rows, query, 2)
	const workers = 4
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				errCh <- fmt.Errorf("worker %d open searcher: %w", worker, err)
				return
			}
			defer func() { _ = searcher.Close() }()
			var buffer VectorIndexSearchBuffer
			for i := 0; i < 50; i++ {
				quantized, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 2, EfSearch: len(rows)}, &buffer)
				if err != nil {
					errCh <- fmt.Errorf("worker %d iteration %d quantized_only search: %w", worker, i, err)
					return
				}
				if mismatch := vectorIndexSearchResultsMismatch1926(quantized.Results, wantQuantized); mismatch != "" {
					errCh <- fmt.Errorf("worker %d iteration %d quantized_only: %s", worker, i, mismatch)
					return
				}
				reranked, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: def.QuantizedIndexes[0].Name, QuantizedRerankCandidates: len(rows), TopK: 2, EfSearch: len(rows)}, &buffer)
				if err != nil {
					errCh <- fmt.Errorf("worker %d iteration %d quantized_rerank search: %w", worker, i, err)
					return
				}
				if mismatch := vectorIndexSearchResultsMismatch1926(reranked.Results, wantReranked); mismatch != "" {
					errCh <- fmt.Errorf("worker %d iteration %d quantized_rerank: %s", worker, i, mismatch)
					return
				}
			}
			errCh <- nil
		}()
	}
	for worker := 0; worker < workers; worker++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func exactColumnGraphScoreForTest1926(tb testing.TB, vector []float32, query []float32) float64 {
	tb.Helper()
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	invNorm, err := columnVectorGraphInvNorm(vector)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm vector: %v", err)
	}
	var dot float64
	for i, value := range query {
		dot += float64(value) * float64(vector[i])
	}
	return dot * float64(queryInvNorm) * float64(invNorm)
}

func scalarU8QuantizedTopKForTest1926(tb testing.TB, rows []columnGraphRebuildInputRowV2A, query []float32, topK int) []columnVectorGraphNativeSearchResult {
	tb.Helper()
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	queryCodes := make([]byte, len(query))
	for i, value := range query {
		queryCodes[i] = columnVectorGraphScalarU8Code(value * queryInvNorm)
	}
	queryCentered, _, ok := vectorops.PrepareScalarU8CenteredQuery(make([]vectorops.ScalarU8CenteredCode, 0, len(queryCodes)), queryCodes, len(queryCodes))
	if !ok {
		tb.Fatalf("PrepareScalarU8CenteredQuery query dims=%d", len(queryCodes))
	}
	var top []columnVectorGraphSearchCandidate
	rowCodes := make([]byte, len(query))
	for ordinal, row := range rows {
		invNorm, err := columnVectorGraphInvNorm(row.vector)
		if err != nil {
			tb.Fatalf("columnVectorGraphInvNorm row %d: %v", ordinal, err)
		}
		for i, value := range row.vector {
			rowCodes[i] = columnVectorGraphScalarU8Code(value * invNorm)
		}
		score, ok := scalarU8CenteredQuantizedCosineScore(queryCentered, rowCodes)
		if !ok {
			tb.Fatalf("scalarU8CenteredQuantizedCosineScore row %d", ordinal)
		}
		top = insertColumnGraphTopForTest(top, topK, columnVectorGraphSearchCandidate{ordinal: ordinal, score: score})
	}
	out := make([]columnVectorGraphNativeSearchResult, len(top))
	for i, candidate := range top {
		out[i] = columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, ID: []byte(rows[candidate.ordinal].id), Score: candidate.score}
	}
	return out
}

func vectorIndexSearchResultsMismatch1926(got []VectorIndexSearchResult, want []columnVectorGraphNativeSearchResult) string {
	if len(got) != len(want) {
		return "result length mismatch"
	}
	for i := range want {
		if !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			return "result mismatch"
		}
	}
	return ""
}

func columnGraphQuantizedAssetRow1926(tb testing.TB, id string, vector []float32) columnVectorGraphAssetRow {
	tb.Helper()
	invNorm, err := columnVectorGraphInvNorm(vector)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm %s: %v", id, err)
	}
	return columnVectorGraphAssetRow{ID: []byte(id), Vector: append([]float32(nil), vector...), InvNorm: invNorm}
}

func attachScalarU8QuantizedAssetForReader1926(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, rows []columnVectorGraphAssetRow) QuantizedVectorIndexDefinition {
	tb.Helper()
	if reader == nil || reader.catalog == nil || reader.catalog.meta.Options.ColumnStore == nil {
		tb.Fatal("reader missing catalog typed-storage metadata")
	}
	def := reader.def
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.fast"}}
	def, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		tb.Fatalf("normalizeVectorIndexDefinition with quantized index: %v", err)
	}
	q := def.QuantizedIndexes[0]
	payload, sourceCfg, err := prepareColumnVectorGraphQuantizedCodesPayload(reader.catalog.meta.Name, *reader.catalog.meta.Options.ColumnStore, def, q, 1001, rows)
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphQuantizedCodesPayload: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(payload)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	asset := columnVectorIndexStateAssetSnapshot{SourceSchemaHash: sourceCfg.SchemaHash, AssetBytes: int64(len(payload))}
	schema := columnVectorGraphQuantizedAssetSchema(def, reader.graph, q, asset, quantizedasset.AssetRefIdentity{})
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
			RequiredRoles:    []quantizedasset.Role{quantizedasset.RoleCodes},
		},
		Parts: []quantizedasset.PartImageSource{{Image: image, AssetBytes: asset.AssetBytes, SourceSchemaHash: asset.SourceSchemaHash}},
	})
	if err != nil {
		tb.Fatalf("quantizedasset.Prepare: %v", err)
	}
	reader.def = def
	reader.quantizedAssetStatus = map[string]columnVectorGraphQuantizedAssetLoadStatus{
		q.Name: {Definition: q, Asset: asset, Prepared: prepared},
	}
	return q
}

func openColumnGraphQuantizedTestCollection1926(tb testing.TB, rows []columnGraphRebuildInputRowV2A, quantizedIndexes []QuantizedVectorIndexDefinition) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:             "embedding_graph",
		Field:            "embedding",
		Metric:           VectorMetricCosine,
		Dimensions:       3,
		M:                3,
		Strategy:         VectorIndexStrategyColumnGraph,
		QuantizedIndexes: quantizedIndexes,
	})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(3),
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
	if len(rows) != 0 {
		insertColumnGraphRebuildRowsV2A(tb, col, rows)
	}
	return dir, d, col, def
}

func TestColumnGraphScalarU8QuantizedAssetBuildRejectsDimensionOverflow1926(t *testing.T) {
	_, err := buildColumnVectorGraphScalarU8Codes(VectorIndexDefinition{Dimensions: math.MaxInt}, []columnVectorGraphAssetRow{{}, {}})
	if err == nil || !strings.Contains(err.Error(), "codes bytes overflow") {
		t.Fatalf("buildColumnVectorGraphScalarU8Codes err=%v want codes bytes overflow", err)
	}
}

func TestColumnGraphScalarU8QuantizedAssetBuildNormalizesCosineRows1926(t *testing.T) {
	codes, err := buildColumnVectorGraphScalarU8Codes(VectorIndexDefinition{Metric: VectorMetricCosine, Dimensions: 2}, []columnVectorGraphAssetRow{
		{Vector: []float32{1, 0}, InvNorm: 1},
		{Vector: []float32{2, 0}, InvNorm: 0.5},
	})
	if err != nil {
		t.Fatalf("buildColumnVectorGraphScalarU8Codes: %v", err)
	}
	if !bytes.Equal(codes[:2], codes[2:]) || !bytes.Equal(codes[:2], []byte{255, 128}) {
		t.Fatalf("codes=%v want equivalent cosine directions encoded identically as [255 128]", codes)
	}
}

func TestColumnGraphQuantizedAssetValidationFailClosedWhenUnavailable1926(t *testing.T) {
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		M:          3,
		Strategy:   VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{{
			Name: "embedding.scalar_u8.fast",
		}},
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	reader := &columnVectorGraphPhysicalRowReader{
		def: def,
		quantizedAssetStatus: map[string]columnVectorGraphQuantizedAssetLoadStatus{
			def.QuantizedIndexes[0].Name: {Definition: def.QuantizedIndexes[0], Err: errors.New("checksum mismatch")},
		},
	}
	err = reader.validateQuantizedNativeSearchOptions(columnVectorGraphNativeSearchQueryModeQuantizedOnly, columnVectorGraphNativeSearchOptions{TopK: 1, QuantizedIndexName: def.QuantizedIndexes[0].Name})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "score-plane asset unavailable") || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("validate err=%v want unavailable checksum fail-closed", err)
	}
	err = reader.validateQuantizedNativeSearchOptions(columnVectorGraphNativeSearchQueryModeQuantizedRerank, columnVectorGraphNativeSearchOptions{TopK: 1, QuantizedIndexName: def.QuantizedIndexes[0].Name})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "query_mode=quantized_rerank") || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("validate rerank err=%v want unavailable checksum fail-closed", err)
	}
	reader.quantizedAssetStatus = nil
	err = reader.validateQuantizedNativeSearchOptions(columnVectorGraphNativeSearchQueryModeQuantizedOnly, columnVectorGraphNativeSearchOptions{TopK: 1, QuantizedIndexName: def.QuantizedIndexes[0].Name})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "has no loaded quantized score-plane asset") {
		t.Fatalf("validate missing err=%v", err)
	}
	err = reader.validateQuantizedNativeSearchOptions(columnVectorGraphNativeSearchQueryModeQuantizedRerank, columnVectorGraphNativeSearchOptions{TopK: 1, QuantizedIndexName: def.QuantizedIndexes[0].Name})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "query_mode=quantized_rerank") || !strings.Contains(err.Error(), "has no loaded quantized score-plane asset") {
		t.Fatalf("validate missing rerank err=%v", err)
	}
}
