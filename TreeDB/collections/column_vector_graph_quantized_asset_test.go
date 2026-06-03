package collections

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	want := scalarU8QuantizedTopKForTest1926(t, rows, query, 2)
	for _, q := range def.QuantizedIndexes {
		got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("quantized_only %q: %v", q.Name, err)
		}
		assertVectorIndexSearchResultsV4(t, got.Results, want, false)
		if got.Stats.QuantizedScoreCalls == 0 {
			t.Fatalf("quantized stats for %q=%+v want scorer use", q.Name, got.Stats)
		}
	}
	if _, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: "embedding.scalar_u8.missing", TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}); !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "is not declared") {
		t.Fatalf("missing quantized index err=%v want declared-name fail-closed", err)
	}
}

func TestColumnGraphScalarU8QuantizedOnlyConcurrentSearch1926(t *testing.T) {
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
	want := scalarU8QuantizedTopKForTest1926(t, rows, query, 2)
	const workers = 4
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = searcher.Close() }()
			var buffer VectorIndexSearchBuffer
			for i := 0; i < 50; i++ {
				got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 2, EfSearch: len(rows)}, &buffer)
				if err != nil {
					errCh <- err
					return
				}
				if mismatch := vectorIndexSearchResultsMismatch1926(got.Results, want); mismatch != "" {
					errCh <- errors.New(mismatch)
					return
				}
			}
			errCh <- nil
		}()
	}
	for worker := 0; worker < workers; worker++ {
		if err := <-errCh; err != nil {
			t.Fatalf("worker %d: %v", worker, err)
		}
	}
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
		score := scalarU8QuantizedCosineScore(queryCodes, rowCodes)
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
	reader.quantizedAssetStatus = nil
	err = reader.validateQuantizedNativeSearchOptions(columnVectorGraphNativeSearchQueryModeQuantizedOnly, columnVectorGraphNativeSearchOptions{TopK: 1, QuantizedIndexName: def.QuantizedIndexes[0].Name})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "has no loaded quantized score-plane asset") {
		t.Fatalf("validate missing err=%v", err)
	}
}
