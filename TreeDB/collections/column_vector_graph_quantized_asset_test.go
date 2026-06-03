package collections

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
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
	if !ok || !bytes.Equal(row0, []byte{255, 128, 0}) {
		t.Fatalf("row0 codes=%v ok=%v", row0, ok)
	}
	row1, ok := prepared.CodeRowBytes("codes", 1)
	if !ok || !bytes.Equal(row1, []byte{96, 223, 128}) {
		t.Fatalf("row1 codes=%v ok=%v", row1, ok)
	}
	row2, ok := prepared.CodeRowBytes("codes", 2)
	if !ok || !bytes.Equal(row2, []byte{191, 64, 159}) {
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
	if _, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}); !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "prepared") || !strings.Contains(err.Error(), "scorer is unavailable") {
		t.Fatalf("quantized SearchVectorIndex err=%v want prepared scorer-unavailable", err)
	}
	exact, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil || len(exact.Results) != 1 {
		t.Fatalf("exact SearchVectorIndex results=%d err=%v", len(exact.Results), err)
	}
}

func TestColumnGraphScalarU8QuantizedAssetBuildRejectsDimensionOverflow1926(t *testing.T) {
	_, err := buildColumnVectorGraphScalarU8Codes(VectorIndexDefinition{Dimensions: math.MaxInt}, []columnVectorGraphAssetRow{{}, {}})
	if err == nil || !strings.Contains(err.Error(), "codes bytes overflow") {
		t.Fatalf("buildColumnVectorGraphScalarU8Codes err=%v want codes bytes overflow", err)
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
