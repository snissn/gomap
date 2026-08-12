package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
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
	for _, stateAsset := range state.Assets {
		if stateAsset.Role == columnVectorIndexStateAssetRoleQuantizedAlpha {
			t.Fatalf("legacy scalar_u8 unexpectedly published alpha asset: %+v", stateAsset)
		}
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

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1, UseResourceQuantizedAssets: true})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	loaded := reader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]
	if loaded.Prepared == nil || loaded.Err != nil {
		_ = reader.Close()
		t.Fatalf("reader quantized status=%+v", loaded)
	}
	if columnVectorGraphQuantizedAssetMmapExpectedForTest2621() {
		if loaded.Health != columnVectorGraphQuantizedAssetHealthMmapDirect || loaded.MappedBytes == 0 || loaded.HeapCopyBytes != 0 {
			_ = reader.Close()
			t.Fatalf("reader scalar_u8 status=%+v want mmap/direct without heap copy", loaded)
		}
	} else if loaded.Health != columnVectorGraphQuantizedAssetHealthHeapCopy || loaded.HeapCopyBytes == 0 {
		_ = reader.Close()
		t.Fatalf("reader scalar_u8 status=%+v want heap-copy fallback", loaded)
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
	reopenedReader, err := reopenedCol.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1, UseResourceQuantizedAssets: true})
	if err != nil {
		t.Fatalf("open reader reopen: %v", err)
	}
	defer func() { _ = reopenedReader.Close() }()
	reopenedStatus := reopenedReader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]
	if reopenedStatus.Prepared == nil || reopenedStatus.Err != nil {
		t.Fatalf("reopened reader quantized status=%+v", reopenedStatus)
	}
	if columnVectorGraphQuantizedAssetMmapExpectedForTest2621() {
		if reopenedStatus.Health != columnVectorGraphQuantizedAssetHealthMmapDirect || reopenedStatus.MappedBytes == 0 || reopenedStatus.HeapCopyBytes != 0 {
			t.Fatalf("reopened scalar_u8 status=%+v want mmap/direct without heap copy", reopenedStatus)
		}
	} else if reopenedStatus.Health != columnVectorGraphQuantizedAssetHealthHeapCopy || reopenedStatus.HeapCopyBytes == 0 {
		t.Fatalf("reopened scalar_u8 status=%+v want heap-copy fallback", reopenedStatus)
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

func TestColumnGraphScalarU8AlphaQuantizedAssetRebuildPrepareReopen2843(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{3, 4, 0}},
		{id: "doc-b", vector: []float32{1, 2, 2}},
		{id: "doc-c", vector: []float32{-2, 1, 2}},
	}
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")
	dir, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{q})
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	cfg, graph, state, assets := loadColumnGraphScalarU8AlphaState2843(t, d, def)
	if !assets.HasCodes || !assets.HasAlpha {
		t.Fatalf("alpha quantized assets=%+v state=%+v want codes and alpha", assets, state.Assets)
	}
	if assets.Codes.RowCount != len(rows) || assets.Alpha.RowCount != 1 || assets.Codes.AssetBytes <= 0 || assets.Alpha.AssetBytes <= 0 {
		t.Fatalf("asset rows/bytes codes=%+v alpha=%+v", assets.Codes, assets.Alpha)
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, state, graph); err != nil {
		t.Fatalf("validate state assets: %v", err)
	}
	prepared, err := loadColumnVectorGraphQuantizedAssetSet(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, def.QuantizedIndexes[0], assets)
	if err != nil {
		t.Fatalf("loadColumnVectorGraphQuantizedAssetSet: %v", err)
	}
	lookup, err := columnVectorGraphScalarU8AlphaLookupFromPrepared(def.QuantizedIndexes[0], prepared)
	if err != nil {
		t.Fatalf("alpha lookup: %v", err)
	}
	if lookup.Rows() != len(rows) || lookup.Granules() != 1 {
		t.Fatalf("lookup rows/granules=(%d,%d)", lookup.Rows(), lookup.Granules())
	}
	alpha, ok := lookup.AlphaForGranule(0)
	if !ok || math.Abs(float64(alpha-0.8)) > 1e-6 {
		t.Fatalf("alpha=%v ok=%v want 0.8", alpha, ok)
	}
	rowCount, ok := lookup.RowCountForGranule(0)
	if !ok || rowCount != uint32(len(rows)) {
		t.Fatalf("granule row_count=%d ok=%v want %d", rowCount, ok, len(rows))
	}
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	scannedRows := loadColumnGraphRebuildRowsFromStateV2A(t, d, "docs", cfg, def, graph, records)
	assetRows := columnGraphAssetRowsFromScanned2843(scannedRows)
	wantCodes := referenceScalarU8AlphaCodes2843(t, def, assetRows, []float32{0.8})
	codeView, ok := prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok {
		t.Fatal("code row view unavailable")
	}
	payload, ok := codeView.PayloadBytes()
	if !ok || !bytes.Equal(payload, wantCodes) {
		t.Fatalf("alpha codes=%v ok=%v want reference %v", payload, ok, wantCodes)
	}
	legacyCodes, err := buildColumnVectorGraphScalarU8Codes(def, assetRows)
	if err != nil {
		t.Fatalf("legacy codes: %v", err)
	}
	if bytes.Equal(payload, legacyCodes) {
		t.Fatalf("alpha codes unexpectedly match legacy codes=%v", payload)
	}
	preReopenPayload := append([]byte(nil), payload...)

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1, UseResourceQuantizedAssets: true})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	loaded := reader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]
	if loaded.Prepared == nil || loaded.Err != nil || loaded.ScalarU8Alpha == nil {
		_ = reader.Close()
		t.Fatalf("reader alpha quantized status=%+v", loaded)
	}
	if columnVectorGraphQuantizedAssetMmapExpectedForTest2621() {
		if loaded.Health != columnVectorGraphQuantizedAssetHealthMmapDirect || loaded.MappedBytes == 0 {
			_ = reader.Close()
			t.Fatalf("reader alpha scalar_u8 status=%+v want mmap/direct code asset", loaded)
		}
	} else if loaded.Health != columnVectorGraphQuantizedAssetHealthHeapCopy || loaded.HeapCopyBytes == 0 {
		_ = reader.Close()
		t.Fatalf("reader alpha scalar_u8 status=%+v want heap-copy fallback", loaded)
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
	reopenedReader, err := reopenedCol.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1, UseResourceQuantizedAssets: true})
	if err != nil {
		t.Fatalf("open reader reopen: %v", err)
	}
	defer func() { _ = reopenedReader.Close() }()
	reopenedStatus := reopenedReader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]
	if reopenedStatus.Prepared == nil || reopenedStatus.Err != nil || reopenedStatus.ScalarU8Alpha == nil {
		t.Fatalf("reopened reader alpha quantized status=%+v", reopenedStatus)
	}
	reopenedPayload, ok := reopenedStatus.Prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok {
		t.Fatal("reopened code row view unavailable")
	}
	reopenedBytes, ok := reopenedPayload.PayloadBytes()
	if !ok || !bytes.Equal(reopenedBytes, preReopenPayload) {
		t.Fatalf("reopened code payload identity ok=%v got=%v want=%v", ok, reopenedBytes, preReopenPayload)
	}
	reopenedAlpha, _, ok := reopenedStatus.ScalarU8Alpha.AlphaForRow(1)
	if !ok || math.Abs(float64(reopenedAlpha-0.8)) > 1e-6 {
		t.Fatalf("reopened alpha row lookup=%v ok=%v want 0.8", reopenedAlpha, ok)
	}
}

func TestColumnGraphScalarU8AlphaQuantileSparseGranulePositiveFallback2843(t *testing.T) {
	const dims = 1000
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha.quantile")
	q.ScalarU8Calibration.AlphaPolicy = ScalarU8AlphaPolicy{Name: ScalarU8AlphaPolicyAbsQuantile, QuantilePPM: ScalarU8AlphaPolicyAbsQuantilePPM999}
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:             "embedding_graph",
		Field:            "embedding",
		Metric:           VectorMetricCosine,
		Dimensions:       dims,
		M:                3,
		Strategy:         VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{q},
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	q = def.QuantizedIndexes[0]
	vector := make([]float32, dims)
	vector[dims-1] = 1
	rows := []columnVectorGraphAssetRow{{ID: []byte("doc-a"), Vector: vector, InvNorm: 1}}

	alpha, err := computeColumnVectorGraphScalarU8GranuleAlpha(def, q, rows)
	if err != nil {
		t.Fatalf("computeColumnVectorGraphScalarU8GranuleAlpha: %v", err)
	}
	if alpha != 1 {
		t.Fatalf("alpha=%v want deterministic positive fallback 1", alpha)
	}
	metadata, err := buildColumnVectorGraphScalarU8AlphaMetadata(def, q, rows)
	if err != nil {
		t.Fatalf("buildColumnVectorGraphScalarU8AlphaMetadata: %v", err)
	}
	if len(metadata.Alphas) != 1 || metadata.Alphas[0] != 1 {
		t.Fatalf("metadata alphas=%v want [1]", metadata.Alphas)
	}
	codes, err := buildColumnVectorGraphScalarU8CodesForDefinition(def, q, rows)
	if err != nil {
		t.Fatalf("buildColumnVectorGraphScalarU8CodesForDefinition: %v", err)
	}
	if len(codes) != dims {
		t.Fatalf("codes len=%d want %d", len(codes), dims)
	}
	if codes[0] != 128 || codes[dims-1] != 255 {
		t.Fatalf("codes edge=(%d,%d) want (128,255)", codes[0], codes[dims-1])
	}
}

func TestColumnGraphScalarU8AlphaQuantileSparseQueryPositiveFallback2844(t *testing.T) {
	const dims = 1000
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha.query_quantile")
	q.ScalarU8Calibration.AlphaPolicy = ScalarU8AlphaPolicy{Name: ScalarU8AlphaPolicyAbsQuantile, QuantilePPM: ScalarU8AlphaPolicyAbsQuantilePPM999}
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:             "embedding_graph",
		Field:            "embedding",
		Metric:           VectorMetricCosine,
		Dimensions:       dims,
		M:                3,
		Strategy:         VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{q},
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	q = def.QuantizedIndexes[0]
	query := make([]float32, dims)
	query[dims-1] = 1
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("query inv norm: %v", err)
	}
	buildAlpha, err := computeColumnVectorGraphScalarU8GranuleAlpha(def, q, []columnVectorGraphAssetRow{{ID: []byte("query"), Vector: query, InvNorm: queryInvNorm}})
	if err != nil {
		t.Fatalf("computeColumnVectorGraphScalarU8GranuleAlpha: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	queryAlpha, err := columnVectorGraphScalarU8QueryAlpha(q, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("columnVectorGraphScalarU8QueryAlpha: %v", err)
	}
	if queryAlpha != buildAlpha || queryAlpha != 1 {
		t.Fatalf("query alpha=%v build alpha=%v want mirrored positive fallback 1", queryAlpha, buildAlpha)
	}
}

func TestColumnGraphScalarU8AlphaStateStatusRejectsWrongGranuleCount2843(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{3, 4, 0}},
		{id: "doc-b", vector: []float32{1, 2, 2}},
		{id: "doc-c", vector: []float32{-2, 1, 2}},
	}
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	if cfg.ActiveManifest == nil {
		t.Fatal("active manifest is nil")
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	_, _, state, _ := loadColumnGraphScalarU8AlphaState2843(t, d, def)
	badState := state
	badState.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	mutated := false
	for i := range badState.Assets {
		if badState.Assets[i].Role == columnVectorIndexStateAssetRoleQuantizedAlpha {
			badState.Assets[i].RowCount++
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatalf("state assets=%+v missing alpha", state.Assets)
	}
	if got := columnVectorIndexStateMatchStatus(badState, def, *cfg, manifest, records); got != columnVectorIndexStateMatchMismatch {
		t.Fatalf("match status=%v want mismatch for wrong alpha granule count", got)
	}

	if _, err := encodeColumnVectorIndexStateRecord(badState); err != nil {
		t.Fatalf("encode bad state with relaxed alpha row_count: %v", err)
	}
}

func TestColumnGraphScalarU8AlphaQuantizedAssetFailsClosed2843(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{3, 4, 0}},
		{id: "doc-b", vector: []float32{1, 2, 2}},
		{id: "doc-c", vector: []float32{-2, 1, 2}},
	}
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	cfg, graph, state, assets := loadColumnGraphScalarU8AlphaState2843(t, d, def)
	q := def.QuantizedIndexes[0]

	missingAlpha := state
	missingAlpha.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	for i := range missingAlpha.Assets {
		if missingAlpha.Assets[i].Role == columnVectorIndexStateAssetRoleQuantizedAlpha {
			missingAlpha.Assets = append(missingAlpha.Assets[:i], missingAlpha.Assets[i+1:]...)
			break
		}
	}
	if err := validateColumnVectorGraphQuantizedStateAssets("docs", *cfg, def, missingAlpha); err == nil || !strings.Contains(err.Error(), "missing quantized asset") {
		t.Fatalf("missing alpha state err=%v want missing quantized asset", err)
	}
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, assets.Codes); !errors.Is(err, errColumnVectorGraphQuantizedAssetMissing) || !strings.Contains(err.Error(), "alpha") {
		t.Fatalf("load without alpha err=%v want missing alpha", err)
	}

	staleSchema := assets
	staleSchema.Alpha.SourceSchemaHash++
	if _, err := loadColumnVectorGraphQuantizedAssetSet(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleSchema); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "schema_hash") {
		t.Fatalf("stale alpha schema err=%v want stale schema_hash", err)
	}
	staleRows := assets
	staleRows.Alpha.RowCount++
	if _, err := loadColumnVectorGraphQuantizedAssetSet(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleRows); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "granule_count") {
		t.Fatalf("stale alpha row count err=%v want invalid granule_count", err)
	}
}

func TestColumnGraphScalarU8AlphaQuantizedAssetRejectsWrongGranuleCountIdentity2843(t *testing.T) {
	if _, err := prepareUncheckedScalarU8AlphaPrepared2843(t, 3, []uint32{1, 2}); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "granule_count") {
		t.Fatalf("wrong alpha granule count err=%v want invalid granule_count", err)
	}
}

func TestColumnGraphScalarU8AlphaQuantizedAssetRejectsWrongGranuleBoundaryIdentity2843(t *testing.T) {
	rowsPerGranule := typedColumnDefaultRowsPerGranule()
	if rowsPerGranule < 2 {
		t.Fatalf("rows_per_granule=%d want >=2", rowsPerGranule)
	}
	rows := rowsPerGranule + 2
	wrongRowCounts := []uint32{uint32(rowsPerGranule - 1), 3}
	if _, err := prepareUncheckedScalarU8AlphaPrepared2843(t, rows, wrongRowCounts); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "row_count") || !strings.Contains(err.Error(), "want") {
		t.Fatalf("wrong alpha granule boundary err=%v want invalid row_count identity", err)
	}
}

func TestColumnGraphScalarU8AlphaQuantizedAssetRejectsInvalidAlpha2843(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{3, 4, 0}},
		{id: "doc-b", vector: []float32{1, 2, 2}},
	}
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	cfg, graph, _, assets := loadColumnGraphScalarU8AlphaState2843(t, d, def)
	q := def.QuantizedIndexes[0]
	for _, tc := range []struct {
		name  string
		alpha float32
	}{
		{name: "zero", alpha: 0},
		{name: "negative", alpha: -0.25},
		{name: "nan", alpha: float32(math.NaN())},
		{name: "inf", alpha: float32(math.Inf(1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badAlpha := writeUncheckedScalarU8AlphaAsset2843(t, d, *cfg, def, graph, q, assets.Alpha.Ref.PartID+100, []float32{tc.alpha}, []uint32{uint32(len(rows))})
			badSet := assets
			badSet.Alpha = badAlpha
			if _, err := loadColumnVectorGraphQuantizedAssetSet(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, badSet); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "alpha") {
				t.Fatalf("invalid alpha=%v err=%v want invalid alpha", tc.alpha, err)
			}
		})
	}
}

func TestColumnGraphScalarU8AlphaScorerAdjustsRankingAndTies2844(t *testing.T) {
	rowsPerGranule := typedColumnDefaultRowsPerGranule()
	if rowsPerGranule < 2 {
		t.Fatalf("rows_per_granule=%d want >=2", rowsPerGranule)
	}
	const dims = 1
	const tailRows = 2
	rowCount := rowsPerGranule + tailRows
	tailStart := rowsPerGranule
	codes := bytes.Repeat([]byte{128}, rowCount*dims)
	codes[0] = 255         // unadjusted top, but alpha lowers it below the tail granule.
	codes[tailStart] = 200 // equal adjusted score with the next tail row; lower ordinal must win.
	codes[tailStart+1] = 200
	alphas := []float32{0.4, 1}
	rowCounts := []uint32{uint32(rowsPerGranule), tailRows}
	def, q, prepared, lookup, _ := prepareScalarU8AlphaPreparedForTest2844(t, dims, codes, alphas, rowCounts)
	codeRows, ok := prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok {
		t.Fatal("code row view unavailable")
	}
	payload, ok := codeRows.PayloadBytes()
	if !ok || !bytes.Equal(payload, codes) {
		t.Fatalf("code payload=%v ok=%v want %v", payload, ok, codes)
	}
	query := []float32{1}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("query inv norm: %v", err)
	}
	var queryScratch columnVectorGraphNativeSearchScratch
	queryAlpha, err := columnVectorGraphScalarU8QueryAlpha(q, query, queryInvNorm, &queryScratch)
	if err != nil {
		t.Fatalf("query alpha: %v", err)
	}
	centered := []vectorops.ScalarU8CenteredCode{vectorops.ScalarU8CenteredValue(columnVectorGraphScalarU8Code(query[0] * queryInvNorm / queryAlpha))}
	centeredQuery, centered, ok := vectorops.PrepareScalarU8CenteredQueryFromCentered(centered, def.Dimensions, int64(centered[0]))
	if !ok {
		t.Fatal("PrepareScalarU8CenteredQueryFromCentered")
	}
	_ = centered
	scorer := columnVectorGraphScalarU8QuantizedScorer{indexName: q.Name, dims: def.Dimensions, codeRows: codeRows, codePayload: payload, centeredQuery: centeredQuery, alphaLookup: lookup, queryAlpha: queryAlpha}
	ordinals := make([]int, rowCount)
	ids := make([]string, rowCount)
	for ordinal := range ordinals {
		ordinals[ordinal] = ordinal
		ids[ordinal] = fmt.Sprintf("row-%d", ordinal)
	}
	scores := make([]float64, 0, len(ordinals))
	var scratch columnVectorGraphNativeSearchScratch
	var stats columnVectorGraphNativeSearchStats
	got, err := scorer.scoreOrdinals(ordinals, scores, &scratch, &stats)
	if err != nil {
		t.Fatalf("scoreOrdinals: %v", err)
	}
	want := scalarU8AlphaQuantizedTopKForCodesForTest2844(t, ids, def.Dimensions, q, query, codes, alphas, rowCounts, 3)
	if len(want) != 3 {
		t.Fatalf("reference top len=%d want 3", len(want))
	}
	for _, result := range want {
		if math.Abs(got[result.Ordinal]-result.Score) > 1e-12 {
			t.Fatalf("score ordinal=%d got=%v want=%v", result.Ordinal, got[result.Ordinal], result.Score)
		}
	}
	if want[0].Ordinal != tailStart || want[1].Ordinal != tailStart+1 || want[2].Ordinal != 0 {
		t.Fatalf("alpha-adjusted top=%+v want tail rows then demoted row 0", want)
	}
	var top []columnVectorGraphSearchCandidate
	for ordinal, score := range got {
		top = insertColumnGraphTopForTest(top, 3, columnVectorGraphSearchCandidate{ordinal: ordinal, score: score})
	}
	if len(top) != 3 || top[0].ordinal != tailStart || top[1].ordinal != tailStart+1 || top[2].ordinal != 0 {
		t.Fatalf("alpha top=%+v want tail rows then demoted row 0", top)
	}
	if got[tailStart] != got[tailStart+1] {
		t.Fatalf("tie fixture scores tail0=%v tail1=%v want equal adjusted scores", got[tailStart], got[tailStart+1])
	}
	if got[0] >= got[tailStart] {
		t.Fatalf("alpha did not demote row0 below tail: row0=%v tail=%v", got[0], got[tailStart])
	}
	if stats.QuantizedScoreCalls != uint64(len(ordinals)) || stats.QuantizedScoreCodecScalarU8Alpha != 1 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 {
		t.Fatalf("alpha scorer stats=%+v", stats)
	}
	legacyScorer := scorer
	legacyScorer.alphaLookup = nil
	legacyScorer.alphaScoreScales = nil
	var legacyScratch columnVectorGraphNativeSearchScratch
	var legacyStats columnVectorGraphNativeSearchStats
	if _, err := legacyScorer.scoreOrdinals(ordinals, scores[:0], &legacyScratch, &legacyStats); err != nil {
		t.Fatalf("legacy scoreOrdinals: %v", err)
	}
	if legacyStats.QuantizedScoreCalls != uint64(len(ordinals)) || legacyStats.QuantizedScoreCodecScalarU8Alpha != 0 {
		t.Fatalf("legacy scorer stats=%+v want no scalar_u8 alpha counter", legacyStats)
	}
}

func TestColumnGraphScalarU8AlphaLookupUniformGranuleEdges2866(t *testing.T) {
	rowsPerGranule := typedColumnDefaultRowsPerGranule()
	if rowsPerGranule < 2 {
		t.Fatalf("rows_per_granule=%d want >=2", rowsPerGranule)
	}
	const dims = 1
	codes := bytes.Repeat([]byte{128}, rowsPerGranule*2*dims)
	_, q, _, lookup, _ := prepareScalarU8AlphaPreparedForTest2844(t, dims, codes, []float32{0.5, 0.25}, []uint32{uint32(rowsPerGranule), uint32(rowsPerGranule)})
	if lookup.uniformGranuleRows != rowsPerGranule {
		t.Fatalf("uniformGranuleRows=%d want %d", lookup.uniformGranuleRows, rowsPerGranule)
	}
	for _, tc := range []struct {
		row       int
		want      int
		wantOK    bool
		wantAlpha float32
	}{
		{row: -1, wantOK: false},
		{row: 0, want: 0, wantOK: true, wantAlpha: 0.5},
		{row: rowsPerGranule - 1, want: 0, wantOK: true, wantAlpha: 0.5},
		{row: rowsPerGranule, want: 1, wantOK: true, wantAlpha: 0.25},
		{row: rowsPerGranule*2 - 1, want: 1, wantOK: true, wantAlpha: 0.25},
		{row: rowsPerGranule * 2, wantOK: false},
	} {
		alpha, granule, ok := lookup.AlphaForRow(tc.row)
		if ok != tc.wantOK || granule != tc.want || (ok && alpha != tc.wantAlpha) {
			t.Fatalf("AlphaForRow(%d)=(alpha=%v granule=%d ok=%v) want (alpha=%v granule=%d ok=%v)", tc.row, alpha, granule, ok, tc.wantAlpha, tc.want, tc.wantOK)
		}
	}
	var scales [2]float64
	if !prepareColumnVectorGraphScalarU8AlphaScoreScales(scales[:], lookup, 0.75) {
		t.Fatal("prepare alpha score scales failed")
	}
	if got, want := scales[0], float64(0.75*0.5)/columnVectorGraphScalarU8CodeScale; got != want {
		t.Fatalf("scale[0]=%v want %v", got, want)
	}
	if got, want := scales[1], float64(0.75*0.25)/columnVectorGraphScalarU8CodeScale; got != want {
		t.Fatalf("scale[1]=%v want %v", got, want)
	}
	_ = q
}

func TestColumnGraphScalarU8AlphaLookupTailGranuleEdges2866(t *testing.T) {
	rowsPerGranule := typedColumnDefaultRowsPerGranule()
	if rowsPerGranule < 2 {
		t.Fatalf("rows_per_granule=%d want >=2", rowsPerGranule)
	}
	const dims = 1
	tailRows := 1
	rowCount := rowsPerGranule + tailRows
	codes := bytes.Repeat([]byte{128}, rowCount*dims)
	_, _, _, lookup, _ := prepareScalarU8AlphaPreparedForTest2844(t, dims, codes, []float32{0.5, 1}, []uint32{uint32(rowsPerGranule), uint32(tailRows)})
	if lookup.uniformGranuleRows != rowsPerGranule {
		t.Fatalf("uniformGranuleRows=%d want first full granule size %d", lookup.uniformGranuleRows, rowsPerGranule)
	}
	if granule, ok := lookup.granuleForRow(rowsPerGranule - 1); !ok || granule != 0 {
		t.Fatalf("last full row granule=%d ok=%v want granule 0", granule, ok)
	}
	if granule, ok := lookup.granuleForRow(rowsPerGranule); !ok || granule != 1 {
		t.Fatalf("tail row granule=%d ok=%v want granule 1", granule, ok)
	}
	if _, ok := lookup.granuleForRow(rowCount); ok {
		t.Fatalf("row %d unexpectedly resolved", rowCount)
	}
}

func TestColumnGraphScalarU8AlphaPreparedTraversalQuantizedOnlyAndRerankUseAlpha2844(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-exact", vector: []float32{1, 0, 0}},
		{id: "doc-alpha", vector: []float32{0, 1, 0}},
		{id: "doc-tail", vector: []float32{0, 0, 1}},
	}
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{q})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	idsByOrdinal := hnswSearchPackIDsByOrdinalForTest2844(t, searcher.reader.hnswSearchPack)
	codes := make([]byte, len(rows)*def.Dimensions)
	for ordinal, id := range idsByOrdinal {
		copy(codes[ordinal*def.Dimensions:(ordinal+1)*def.Dimensions], []byte{128, 128, 128})
		switch id {
		case "doc-exact":
			copy(codes[ordinal*def.Dimensions:(ordinal+1)*def.Dimensions], []byte{255, 128, 128})
		case "doc-alpha":
			copy(codes[ordinal*def.Dimensions:(ordinal+1)*def.Dimensions], []byte{200, 128, 128})
		}
	}
	alphas := []float32{0.5}
	rowCounts := []uint32{uint32(len(rows))}
	installScalarU8AlphaPreparedStatusForTest2844(t, searcher.reader, codes, alphas, rowCounts)

	query := []float32{1, 0, 0}
	qName := def.QuantizedIndexes[0].Name
	quantizedOnlyOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	quantizedOnly, err := searcher.SearchWithBuffer(quantizedOnlyOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer quantized_only: %v", err)
	}
	wantQuantized := scalarU8AlphaQuantizedTopKForCodesForTest2844(t, idsByOrdinal, def.Dimensions, def.QuantizedIndexes[0], query, codes, alphas, rowCounts, quantizedOnlyOpts.TopK)
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, wantQuantized, false)
	if string(quantizedOnly.Results[0].ID) != string(wantQuantized[0].ID) {
		t.Fatalf("quantized_only top=%q want reference %q", quantizedOnly.Results[0].ID, wantQuantized[0].ID)
	}
	if quantizedOnly.Stats.SearchRouteQuantizedOnly != 1 || quantizedOnly.Stats.QuantizedScorerActive != 1 || quantizedOnly.Stats.QuantizedScoreCodecScalarU8Alpha != 1 || quantizedOnly.Stats.QuantizedScoreCalls == 0 || quantizedOnly.Stats.VectorBytesRead != 0 || quantizedOnly.Stats.NormBytesRead != 0 || quantizedOnly.Stats.DocumentsFetched != 0 {
		t.Fatalf("quantized_only stats=%+v want alpha quantized scorer without exact/doc reads", quantizedOnly.Stats)
	}

	rerankOpts := quantizedOnlyOpts
	rerankOpts.QueryMode = VectorIndexQueryModeQuantizedRerank
	rerankOpts.TopK = 1
	rerankOpts.QuantizedRerankCandidates = 1
	reranked, err := searcher.SearchWithBuffer(rerankOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer quantized_rerank: %v", err)
	}
	wantRerankID := string(wantQuantized[0].ID)
	if len(reranked.Results) != 1 || string(reranked.Results[0].ID) != wantRerankID {
		t.Fatalf("quantized_rerank results=%+v want exact rerank of reference-selected %q only", reranked.Results, wantRerankID)
	}
	var wantRerankScore float64
	foundRerankVector := false
	for _, row := range rows {
		if row.id == wantRerankID {
			wantRerankScore = exactColumnGraphScoreForTest1926(t, row.vector, query)
			foundRerankVector = true
			break
		}
	}
	if !foundRerankVector {
		t.Fatalf("missing rerank vector for %q", wantRerankID)
	}
	if math.Abs(reranked.Results[0].Score-wantRerankScore) > 1e-6 {
		t.Fatalf("rerank score=%v want exact %q score=%v", reranked.Results[0].Score, wantRerankID, wantRerankScore)
	}
	if reranked.Stats.SearchRouteQuantizedRerank != 1 || reranked.Stats.QuantizedScorerActive != 1 || reranked.Stats.QuantizedScoreCodecScalarU8Alpha != 1 || reranked.Stats.QuantizedRerankCandidates != 1 || reranked.Stats.QuantizedRerankExactScoreCalls != 1 || reranked.Stats.VectorBytesRead == 0 || reranked.Stats.NormBytesRead == 0 || reranked.Stats.DocumentsFetched != 0 {
		t.Fatalf("quantized_rerank stats=%+v want alpha traversal plus one exact rerank", reranked.Stats)
	}

	for i := 0; i < 3; i++ {
		if got, err := searcher.SearchWithBuffer(quantizedOnlyOpts, &buffer); err != nil || len(got.Results) != quantizedOnlyOpts.TopK {
			t.Fatalf("warm quantized_only iteration %d results=%d err=%v", i, len(got.Results), err)
		}
		if got, err := searcher.SearchWithBuffer(rerankOpts, &buffer); err != nil || len(got.Results) != rerankOpts.TopK {
			t.Fatalf("warm quantized_rerank iteration %d results=%d err=%v", i, len(got.Results), err)
		}
	}
	quantizedOnlyAllocs := testing.AllocsPerRun(100, func() {
		got, err := searcher.SearchWithBuffer(quantizedOnlyOpts, &buffer)
		if err != nil || len(got.Results) != quantizedOnlyOpts.TopK {
			panic("unexpected scalar_u8 alpha quantized_only allocation probe result")
		}
	})
	if quantizedOnlyAllocs != 0 {
		t.Fatalf("scalar_u8 alpha quantized_only SearchWithBuffer steady-state allocs/run=%v want 0", quantizedOnlyAllocs)
	}
	rerankAllocs := testing.AllocsPerRun(100, func() {
		got, err := searcher.SearchWithBuffer(rerankOpts, &buffer)
		if err != nil || len(got.Results) != rerankOpts.TopK {
			panic("unexpected scalar_u8 alpha quantized_rerank allocation probe result")
		}
	})
	if rerankAllocs != 0 {
		t.Fatalf("scalar_u8 alpha quantized_rerank SearchWithBuffer steady-state allocs/run=%v want 0", rerankAllocs)
	}
}

func TestColumnGraphScalarU8AlphaMissingStaleFailsClosedNoExactFallback2844(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{q})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	codes := []byte{255, 128, 128, 200, 128, 128}
	alphas := []float32{0.5}
	rowCounts := []uint32{uint32(len(rows))}
	baseStatus := installScalarU8AlphaPreparedStatusForTest2844(t, searcher.reader, codes, alphas, rowCounts)
	qName := def.QuantizedIndexes[0].Name
	query := []float32{1, 0, 0}
	opts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 1, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer

	missing := baseStatus
	missing.ScalarU8Alpha = nil
	searcher.reader.quantizedAssetStatus[qName] = missing
	got, err := searcher.SearchWithBuffer(opts, &buffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || len(got.Results) != 0 {
		t.Fatalf("missing alpha results=%+v err=%v want unavailable", got.Results, err)
	}
	if got.Stats.SearchRouteQuantizedOnly != 1 || got.Stats.QuantizedScorerActive != 0 || got.Stats.QuantizedScoreCalls != 0 || got.Stats.PreparedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 || got.Stats.QuantizedAssetUnavailable != 1 || got.Stats.QuantizedAssetMissing != 1 {
		t.Fatalf("missing alpha stats=%+v want fail-closed without exact fallback", got.Stats)
	}

	stale := baseStatus
	staleLookup := *baseStatus.ScalarU8Alpha
	staleLookup.rows = 1
	stale.ScalarU8Alpha = &staleLookup
	searcher.reader.quantizedAssetStatus[qName] = stale
	got, err = searcher.SearchWithBuffer(opts, &buffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || len(got.Results) != 0 {
		t.Fatalf("stale alpha results=%+v err=%v want unavailable", got.Results, err)
	}
	if got.Stats.SearchRouteQuantizedOnly != 1 || got.Stats.QuantizedScorerActive != 0 || got.Stats.QuantizedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 || got.Stats.QuantizedAssetUnavailable != 1 || got.Stats.QuantizedAssetStale != 1 {
		t.Fatalf("stale alpha stats=%+v want stale fail-closed without exact fallback", got.Stats)
	}

	exactOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, TopK: 1, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	exact, err := searcher.SearchWithBuffer(exactOpts, &buffer)
	if err != nil || len(exact.Results) != 1 {
		t.Fatalf("exact with stale alpha results=%+v err=%v", exact.Results, err)
	}
	if exact.Stats.SearchRouteQuantizedOnly != 0 || exact.Stats.SearchRouteQuantizedRerank != 0 || exact.Stats.QuantizedScorerActive != 0 || exact.Stats.QuantizedScoreCalls != 0 || exact.Stats.QuantizedAssetUnavailable != 0 {
		t.Fatalf("exact stats=%+v want quantized alpha state ignored", exact.Stats)
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
	if !scorer.centeredQuery.ValidForDims(def.Dimensions) {
		t.Fatalf("scorer centered=%+v dims=%d", scorer.centeredQuery, def.Dimensions)
	}
	queryCodes := make([]byte, len(query))
	for i, value := range query {
		code := columnVectorGraphScalarU8Code(value * queryInvNorm)
		queryCodes[i] = code
		want := vectorops.ScalarU8CenteredValue(code)
		got, ok := scorer.centeredQuery.Value(i)
		if !ok || got != want {
			t.Fatalf("centered query[%d]=%d ok=%v want %d from code %d", i, got, ok, want, code)
		}
	}
	row, ok := scorer.codeRows.RowBytes(1)
	if !ok {
		t.Fatal("row 1 code bytes unavailable")
	}
	got, err := scorer.scoreOrdinal(1, &scratch, nil)
	if err != nil {
		t.Fatalf("scoreOrdinal: %v", err)
	}
	var legacyDot int64
	for i, qc := range queryCodes {
		q := int64(2*int(qc) - 255)
		c := int64(2*int(row[i]) - 255)
		legacyDot += q * c
	}
	want := float64(legacyDot) / columnVectorGraphScalarU8CodeScale
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("centered score=%v want legacy=%v", got, want)
	}

	scorer, err = reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("warm prepare scorer: %v", err)
	}
	if _, err := scorer.scoreOrdinal(1, &scratch, nil); err != nil {
		t.Fatalf("warm scoreOrdinal: %v", err)
	}
	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are unstable under race instrumentation")
	}
	if !enterIsolatedVectorAllocationGate(t, "scalar-u8-centered-query-scratch") {
		return
	}
	var stats columnVectorGraphNativeSearchStats
	allocs := testing.AllocsPerRun(1000, func() {
		scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
		if err != nil {
			panic(err)
		}
		score, err := scorer.scoreOrdinal(1, &scratch, &stats)
		if err != nil {
			panic(err)
		}
		columnPhysicalScanBenchSum += int64(score * 1_000_000)
	})
	if allocs != 0 {
		t.Fatalf("steady-state centered scalar_u8 prepare+score allocs/run=%v want 0", allocs)
	}
}

func TestColumnGraphScalarU8QuantizedScoreOrdinalsUsesScalarU8BatchKernel2260(t *testing.T) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 8, dims: 32, m: 4, topK: 2, efSearch: 8, queryOrdinal: 3}
	_, d, col, def, rows := openColumnGraphScalarU8QuantizedBenchCollection1926(t, shape, true)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	query := rows[shape.queryOrdinal].vector
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("prepare scorer: %v", err)
	}
	if payload, ok := scorer.codeRows.PayloadBytes(); !ok || !bytes.Equal(payload, scorer.codePayload) {
		t.Fatalf("scorer code payload ok=%v len=%d want direct CodeRowView payload", ok, len(payload))
	}

	ordinals := []int{0, 1, 2, 3, 4, 5, 6, 7}
	scores := make([]float64, 0, len(ordinals))
	var stats columnVectorGraphNativeSearchStats
	scores, err = scorer.scoreOrdinals(ordinals, scores, &scratch, &stats)
	if err != nil {
		t.Fatalf("scoreOrdinals batch: %v", err)
	}
	if len(scores) != len(ordinals) {
		t.Fatalf("scoreOrdinals len=%d want %d", len(scores), len(ordinals))
	}
	for i, ordinal := range ordinals {
		row, ok := scorer.codeRows.RowBytes(ordinal)
		if !ok {
			t.Fatalf("row %d unavailable", ordinal)
		}
		want, ok := scalarU8CenteredQuantizedCosineScore(scorer.centeredQuery, row)
		if !ok {
			t.Fatalf("scalar score row %d unavailable", ordinal)
		}
		if math.Abs(scores[i]-want) > 1e-12 {
			t.Fatalf("batch score ordinal=%d got=%v want scalar=%v", ordinal, scores[i], want)
		}
	}
	if stats.QuantizedScoreCalls != uint64(len(ordinals)) || stats.QuantizedCodeBytesRead != uint64(len(ordinals)*shape.dims) || stats.ScoreBatchCalls != 1 || stats.ScoreBatchCandidates != uint64(len(ordinals)) || stats.ScoreBatchMaxTileSize != uint64(len(ordinals)) {
		t.Fatalf("batch stats=%+v want one quantized batch over all ordinals", stats)
	}
	if vectorops.DotScalarU8CenteredIndexedOptimizedEligible(len(ordinals), shape.dims) {
		if stats.ScoreBatchOptimizedCalls != 1 || stats.ScoreBatchScalarFallbackCalls != 0 {
			t.Fatalf("batch stats=%+v want optimized scalar_u8 batch status", stats)
		}
	} else if stats.ScoreBatchOptimizedCalls != 0 || stats.ScoreBatchScalarFallbackCalls != 1 {
		t.Fatalf("batch stats=%+v want fallback scalar_u8 batch status", stats)
	}

	singleStats := columnVectorGraphNativeSearchStats{}
	single, err := scorer.scoreOrdinals(ordinals[:1], scores[:0], &scratch, &singleStats)
	if err != nil {
		t.Fatalf("scoreOrdinals single fallback: %v", err)
	}
	wantSingle, ok := scalarU8CenteredQuantizedCosineScore(scorer.centeredQuery, mustColumnGraphQuantizedCodeRowForTest2260(t, scorer, ordinals[0]))
	if !ok || len(single) != 1 || math.Abs(single[0]-wantSingle) > 1e-12 {
		t.Fatalf("single score=%v ok=%v want %v", single, ok, wantSingle)
	}
	if vectorops.DotScalarU8CenteredIndexedOptimizedEligible(1, shape.dims) {
		if singleStats.ScoreBatchOptimizedCalls != 1 || singleStats.ScoreBatchScalarFallbackCalls != 0 || singleStats.QuantizedScoreCalls != 1 {
			t.Fatalf("single stats=%+v want optimized scalar_u8 single-row status", singleStats)
		}
	} else if singleStats.ScoreBatchOptimizedCalls != 0 || singleStats.ScoreBatchScalarFallbackCalls != 1 || singleStats.QuantizedScoreCalls != 1 {
		t.Fatalf("single stats=%+v want scalar_u8 fallback status", singleStats)
	}

	_, err = scorer.scoreOrdinals([]int{0, len(ordinals)}, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "ordinal=8") {
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
	var allocStats columnVectorGraphNativeSearchStats
	allocs := testing.AllocsPerRun(1000, func() {
		allocStats = columnVectorGraphNativeSearchStats{}
		got, err := scorer.scoreOrdinals(ordinals, scores[:0], &scratch, &allocStats)
		if err != nil {
			panic(err)
		}
		columnPhysicalScanBenchSum += int64(got[0] * 1_000_000)
	})
	if allocs != 0 {
		t.Fatalf("steady-state scalar_u8 batch scoreOrdinals allocs/run=%v want 0", allocs)
	}
}

func mustColumnGraphQuantizedCodeRowForTest2260(tb testing.TB, scorer columnVectorGraphScalarU8QuantizedScorer, ordinal int) []byte {
	tb.Helper()
	row, ok := scorer.codeRows.RowBytes(ordinal)
	if !ok {
		tb.Fatalf("row %d unavailable", ordinal)
	}
	return row
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

func prepareScalarU8AlphaPreparedForTest2844(tb testing.TB, dims int, codes []byte, alphas []float32, rowCounts []uint32) (VectorIndexDefinition, QuantizedVectorIndexDefinition, *quantizedasset.Prepared, *columnVectorGraphScalarU8AlphaLookup, int64) {
	tb.Helper()
	if dims <= 0 {
		tb.Fatalf("dims=%d", dims)
	}
	rowCount := 0
	for _, count := range rowCounts {
		rowCount += int(count)
	}
	if len(alphas) != len(rowCounts) || len(codes) != rowCount*dims {
		tb.Fatalf("codes=%d alphas=%d rowCounts=%d rowCount=%d dims=%d", len(codes), len(alphas), len(rowCounts), rowCount, dims)
	}
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: VectorMetricCosine, Dimensions: dims, M: 3, Strategy: VectorIndexStrategyColumnGraph, QuantizedIndexes: []QuantizedVectorIndexDefinition{q}})
	if err != nil {
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	q = def.QuantizedIndexes[0]
	base := columnGraphRebuildColumnStoreConfigV2A(dims)
	base.AssetManager = &ColumnAssetManagerConfig{Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "docs/column-assets"}
	codeCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig("docs", *base, def, q)
	if err != nil {
		tb.Fatalf("columnVectorGraphQuantizedCodesColumnStoreConfig: %v", err)
	}
	primaryIDs := make([]int64, rowCount)
	for i := range primaryIDs {
		primaryIDs[i] = int64(i)
	}
	fixedRows, err := typedcolumn.NewFixedBytesRows(rowCount, dims, codes)
	if err != nil {
		tb.Fatalf("NewFixedBytesRows: %v", err)
	}
	codePart, err := typedcolumn.BuildColumnPart(284401, typedcolumn.Options{
		SchemaVersion: uint32(codeCfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
			{Name: columnVectorGraphQuantizedCodesColumnName, Type: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, FixedWidthElements: dims, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows:              rowCount,
		Columns:           map[string][]int64{typedColumnAdapterPrimaryIDColumn: primaryIDs},
		FixedBytesColumns: map[string]typedcolumn.FixedBytesRows{columnVectorGraphQuantizedCodesColumnName: fixedRows},
	})
	if err != nil {
		tb.Fatalf("BuildColumnPart codes: %v", err)
	}
	codeImage, err := typedcolumn.BuildColumnPartImage(codePart, typedcolumn.ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{columnVectorGraphQuantizedCodesColumnName: string(columnsemantics.LogicalByteVector)}})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage codes: %v", err)
	}
	metadata := columnVectorGraphScalarU8AlphaMetadata{Alphas: append([]float32(nil), alphas...), RowCounts: append([]uint32(nil), rowCounts...)}
	alphaPayload, alphaCfg, err := prepareColumnVectorGraphScalarU8AlphaPayload("docs", *base, def, q, 284402, metadata)
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphScalarU8AlphaPayload: %v", err)
	}
	alphaImage, err := typedcolumn.ParseColumnPartImage(alphaPayload)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage alpha: %v", err)
	}
	codeBytes := int64(len(codeImage.Bytes))
	alphaBytes := int64(len(alphaImage.Bytes))
	assets := columnVectorGraphQuantizedAssetSet{
		Codes:    columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedCodes, AssetID: columnVectorGraphQuantizedCodesAssetID(q), LogicalType: columnVectorIndexStateLogicalTypeByteVector, PhysicalEncoding: columnVectorIndexStateEncodingRawFixedBytes, RowCount: rowCount, SourceSchemaHash: codeCfg.SchemaHash, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "docs/column-assets", Generation: 1, PartID: 284401, FileID: 1, Length: codeBytes}, AssetBytes: codeBytes},
		Alpha:    columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedAlpha, AssetID: columnVectorGraphScalarU8AlphaAssetID(q), LogicalType: columnVectorIndexStateLogicalTypeScalarU8Alpha, PhysicalEncoding: columnVectorIndexStateEncodingRawFloat32Uint32, RowCount: len(alphas), SourceSchemaHash: alphaCfg.SchemaHash, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "docs/column-assets", Generation: 1, PartID: 284402, FileID: 1, Length: alphaBytes}, AssetBytes: alphaBytes},
		HasCodes: true,
		HasAlpha: true,
	}
	graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Dimensions: dims, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: 1, BaseManifestChecksum: 2, BaseSchemaHash: 3, GraphSchemaHash: 4, RowCount: rowCount}
	prepared, err := prepareColumnVectorGraphQuantizedAssetFromImages(def, graph, q, assets, codeImage, alphaImage)
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphQuantizedAssetFromImages: %v", err)
	}
	lookup, err := columnVectorGraphScalarU8AlphaLookupFromPrepared(q, prepared)
	if err != nil {
		tb.Fatalf("columnVectorGraphScalarU8AlphaLookupFromPrepared: %v", err)
	}
	return def, q, prepared, lookup, assets.Codes.AssetBytes + assets.Alpha.AssetBytes
}

func hnswSearchPackIDsByOrdinalForTest2844(tb testing.TB, pack *columnHNSWSearchPackPreparedView) []string {
	tb.Helper()
	if pack == nil {
		tb.Fatal("nil hnsw search pack")
	}
	ids := make([]string, pack.Header.Rows)
	for ordinal := range ids {
		id, ok := pack.documentIDForOrdinal(ordinal)
		if !ok {
			tb.Fatalf("document ID unavailable for ordinal=%d", ordinal)
		}
		ids[ordinal] = string(id)
	}
	return ids
}

func installScalarU8AlphaPreparedStatusForTest2844(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, codes []byte, alphas []float32, rowCounts []uint32) columnVectorGraphQuantizedAssetLoadStatus {
	tb.Helper()
	if reader == nil {
		tb.Fatal("nil reader")
	}
	def, q, prepared, lookup, assetBytes := prepareScalarU8AlphaPreparedForTest2844(tb, reader.def.Dimensions, codes, alphas, rowCounts)
	// Keep the live graph identity from the reader while installing the custom
	// small-granule score-plane payload.
	q = reader.def.QuantizedIndexes[0]
	if !quantizedVectorIndexDefinitionValuesEqual(def.QuantizedIndexes[0], q) {
		tb.Fatalf("test quantized definition mismatch custom=%+v reader=%+v", def.QuantizedIndexes[0], q)
	}
	status := columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Prepared: prepared, ScalarU8Alpha: lookup, Health: columnVectorGraphQuantizedAssetHealthHeapCopy, HeapCopyBytes: uint64(assetBytes)}
	if reader.quantizedAssetStatus == nil {
		reader.quantizedAssetStatus = make(map[string]columnVectorGraphQuantizedAssetLoadStatus, 1)
	}
	reader.quantizedAssetStatus[q.Name] = status
	return status
}

func scalarU8AlphaQuantizedTopKForCodesForTest2844(tb testing.TB, ids []string, dims int, q QuantizedVectorIndexDefinition, query []float32, codes []byte, alphas []float32, rowCounts []uint32, topK int) []columnVectorGraphNativeSearchResult {
	tb.Helper()
	if dims <= 0 || len(query) != dims {
		tb.Fatalf("dims/query=(%d,%d)", dims, len(query))
	}
	rowCount := 0
	for _, count := range rowCounts {
		rowCount += int(count)
	}
	if len(ids) != rowCount || len(codes) != rowCount*dims || len(alphas) != len(rowCounts) {
		tb.Fatalf("ids=%d codes=%d alphas=%d rowCounts=%d rowCount=%d dims=%d", len(ids), len(codes), len(alphas), len(rowCounts), rowCount, dims)
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	queryAlpha, err := columnVectorGraphScalarU8QueryAlpha(q, query, queryInvNorm, &scratch)
	if err != nil {
		tb.Fatalf("query alpha: %v", err)
	}
	queryCodes := make([]byte, dims)
	for i, value := range query {
		queryCodes[i] = columnVectorGraphScalarU8Code(value * queryInvNorm / queryAlpha)
	}
	queryCentered, _, ok := vectorops.PrepareScalarU8CenteredQuery(make([]vectorops.ScalarU8CenteredCode, 0, dims), queryCodes, dims)
	if !ok {
		tb.Fatalf("PrepareScalarU8CenteredQuery dims=%d", dims)
	}
	var top []columnVectorGraphSearchCandidate
	rowBase := 0
	for granule, count := range rowCounts {
		alpha := alphas[granule]
		for row := rowBase; row < rowBase+int(count); row++ {
			rowCodes := codes[row*dims : (row+1)*dims]
			score, ok := scalarU8CenteredQuantizedCosineScore(queryCentered, rowCodes)
			if !ok {
				tb.Fatalf("scalar_u8 score row=%d", row)
			}
			score *= float64(queryAlpha) * float64(alpha)
			top = insertColumnGraphTopForTest(top, topK, columnVectorGraphSearchCandidate{ordinal: row, score: score})
		}
		rowBase += int(count)
	}
	out := make([]columnVectorGraphNativeSearchResult, len(top))
	for i, candidate := range top {
		out[i] = columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, ID: []byte(ids[candidate.ordinal]), Score: candidate.score}
	}
	return out
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

func scalarU8AlphaQuantizedIndex2843(name string) QuantizedVectorIndexDefinition {
	return QuantizedVectorIndexDefinition{
		Name:  name,
		Codec: QuantizedVectorCodecScalarU8,
		ScalarU8Calibration: &ScalarU8CalibrationConfig{
			Mode:     ScalarU8CalibrationModePerGranuleAlpha,
			Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
			AlphaPolicy: ScalarU8AlphaPolicy{
				Name: ScalarU8AlphaPolicyMaxAbs,
			},
		},
	}
}

func loadColumnGraphScalarU8AlphaState2843(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition) (*ColumnStoreConfig, columnVectorGraphManifestSnapshot, columnVectorIndexStateSnapshot, columnVectorGraphQuantizedAssetSet) {
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
	sets := columnVectorGraphQuantizedAssetSetsByName(state, def)
	assets, ok := sets[def.QuantizedIndexes[0].Name]
	if !ok {
		tb.Fatalf("alpha quantized assets missing from state assets: %+v", state.Assets)
	}
	return cfg, graph, state, assets
}

func columnGraphAssetRowsFromInput2843(tb testing.TB, rows []columnGraphRebuildInputRowV2A) []columnVectorGraphAssetRow {
	tb.Helper()
	out := make([]columnVectorGraphAssetRow, len(rows))
	for i, row := range rows {
		out[i] = columnGraphQuantizedAssetRow1926(tb, row.id, row.vector)
	}
	return out
}

func columnGraphAssetRowsFromScanned2843(rows []columnGraphRebuildScannedRowV2A) []columnVectorGraphAssetRow {
	out := make([]columnVectorGraphAssetRow, len(rows))
	for i, row := range rows {
		out[i] = columnVectorGraphAssetRow{ID: []byte(row.id), Vector: append([]float32(nil), row.vector...), InvNorm: row.invNorm}
	}
	return out
}

func referenceScalarU8AlphaCodes2843(tb testing.TB, def VectorIndexDefinition, rows []columnVectorGraphAssetRow, alphas []float32) []byte {
	tb.Helper()
	if len(alphas) == 0 {
		tb.Fatal("reference alphas are empty")
	}
	codes := make([]byte, 0, len(rows)*def.Dimensions)
	for rowIdx, row := range rows {
		alpha := alphas[0]
		if len(alphas) == len(rows) {
			alpha = alphas[rowIdx]
		}
		if !validColumnVectorGraphScalarU8Alpha(alpha) {
			tb.Fatalf("invalid reference alpha %v", alpha)
		}
		for _, value := range row.Vector {
			codes = append(codes, columnVectorGraphScalarU8Code(value*row.InvNorm/alpha))
		}
	}
	return codes
}

func prepareUncheckedScalarU8AlphaPrepared2843(tb testing.TB, rows int, rowCounts []uint32) (*quantizedasset.Prepared, error) {
	tb.Helper()
	if rows < 0 {
		tb.Fatalf("rows=%d", rows)
	}
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:             "embedding_graph",
		Field:            "embedding",
		Metric:           VectorMetricCosine,
		Dimensions:       3,
		M:                3,
		Strategy:         VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{q},
	})
	if err != nil {
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	q = def.QuantizedIndexes[0]
	base, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(def.Dimensions))
	if err != nil {
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	assetRows := make([]columnVectorGraphAssetRow, rows)
	for i := range assetRows {
		assetRows[i] = columnVectorGraphAssetRow{ID: []byte(fmt.Sprintf("doc-%06d", i)), Vector: []float32{1, 0, 0}, InvNorm: 1}
	}
	codePayload, codeCfg, err := prepareColumnVectorGraphQuantizedCodesPayload("docs", *base, def, q, 2843001, assetRows)
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphQuantizedCodesPayload: %v", err)
	}
	codeImage, err := typedcolumn.ParseColumnPartImage(codePayload)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage codes: %v", err)
	}
	alphas := make([]float32, len(rowCounts))
	for i := range alphas {
		alphas[i] = 1
	}
	alphaPayload, alphaCfg, err := prepareColumnVectorGraphScalarU8AlphaPayload("docs", *base, def, q, 2843002, columnVectorGraphScalarU8AlphaMetadata{Alphas: alphas, RowCounts: append([]uint32(nil), rowCounts...)})
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphScalarU8AlphaPayload: %v", err)
	}
	alphaImage, err := typedcolumn.ParseColumnPartImage(alphaPayload)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage alpha: %v", err)
	}
	graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: 1, BaseManifestChecksum: 2, BaseSchemaHash: 3, GraphSchemaHash: 4, RowCount: rows}
	codeRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: base.AssetManager.Namespace, Generation: graph.BaseManifestGeneration, PartID: codeImage.PartID, FileID: 1, Length: int64(len(codePayload))}
	alphaRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: base.AssetManager.Namespace, Generation: graph.BaseManifestGeneration, PartID: alphaImage.PartID, FileID: 2, Length: int64(len(alphaPayload))}
	assets := columnVectorGraphQuantizedAssetSet{
		Codes:    columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedCodes, AssetID: columnVectorGraphQuantizedCodesAssetID(q), LogicalType: columnVectorIndexStateLogicalTypeByteVector, PhysicalEncoding: columnVectorIndexStateEncodingRawFixedBytes, RowCount: rows, SourceSchemaHash: codeCfg.SchemaHash, Ref: codeRef, AssetBytes: int64(len(codePayload))},
		Alpha:    columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedAlpha, AssetID: columnVectorGraphScalarU8AlphaAssetID(q), LogicalType: columnVectorIndexStateLogicalTypeScalarU8Alpha, PhysicalEncoding: columnVectorIndexStateEncodingRawFloat32Uint32, RowCount: len(rowCounts), SourceSchemaHash: alphaCfg.SchemaHash, Ref: alphaRef, AssetBytes: int64(len(alphaPayload))},
		HasCodes: true,
		HasAlpha: true,
	}
	return prepareColumnVectorGraphQuantizedAssetFromImages(def, graph, q, assets, codeImage, alphaImage)
}

func writeUncheckedScalarU8AlphaAsset2843(tb testing.TB, d *backenddb.DB, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, partID uint64, alphas []float32, rowCounts []uint32) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	sourceCfg, err := columnVectorGraphScalarU8AlphaColumnStoreConfig("docs", cfg, def, q)
	if err != nil {
		tb.Fatalf("columnVectorGraphScalarU8AlphaColumnStoreConfig: %v", err)
	}
	if len(alphas) != len(rowCounts) {
		tb.Fatalf("alphas=%d rowCounts=%d", len(alphas), len(rowCounts))
	}
	primaryIDs := make([]int64, len(alphas))
	for i := range primaryIDs {
		primaryIDs[i] = int64(i)
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(sourceCfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
			{Name: columnVectorGraphQuantizedScalarU8AlphaColumnName, Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: columnVectorGraphQuantizedGranuleRowCountColumnName, Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows:           len(alphas),
		Columns:        map[string][]int64{typedColumnAdapterPrimaryIDColumn: primaryIDs},
		Float32Columns: map[string][]float32{columnVectorGraphQuantizedScalarU8AlphaColumnName: alphas},
		Uint32Columns:  map[string][]uint32{columnVectorGraphQuantizedGranuleRowCountColumnName: rowCounts},
	})
	if err != nil {
		tb.Fatalf("BuildColumnPart invalid alpha fixture: %v", err)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{
		columnVectorGraphQuantizedScalarU8AlphaColumnName:   string(columnsemantics.LogicalFloat32),
		columnVectorGraphQuantizedGranuleRowCountColumnName: string(columnsemantics.LogicalUint32),
	}})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage invalid alpha fixture: %v", err)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(d.ColumnAssetRootDir(), sourceCfg)
	if err != nil {
		tb.Fatalf("new appender: %v", err)
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, sourceCfg)
	ref, appendErr := appender.appendKindWithAlignment(image.Bytes, ColumnAssetKindTCS1TypedColumnPart, graph.BaseManifestGeneration, partID, alignment)
	closeErr := appender.close()
	if appendErr != nil || closeErr != nil {
		tb.Fatalf("append invalid alpha asset append=%v close=%v", appendErr, closeErr)
	}
	return columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedAlpha, AssetID: columnVectorGraphScalarU8AlphaAssetID(q), LogicalType: columnVectorIndexStateLogicalTypeScalarU8Alpha, PhysicalEncoding: columnVectorIndexStateEncodingRawFloat32Uint32, RowCount: len(alphas), SourceSchemaHash: sourceCfg.SchemaHash, Ref: ref, AssetBytes: ref.Length}
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
	asset := columnVectorIndexStateAssetSnapshot{SourceSchemaHash: sourceCfg.SchemaHash, AssetBytes: int64(len(payload)), LogicalType: columnVectorIndexStateLogicalTypeByteVector, PhysicalEncoding: columnVectorIndexStateEncodingRawFixedBytes}
	schema, err := columnVectorGraphQuantizedAssetSchema(def, reader.graph, q, asset, quantizedasset.AssetRefIdentity{})
	if err != nil {
		tb.Fatalf("columnVectorGraphQuantizedAssetSchema: %v", err)
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
	dims := 3
	if len(rows) > 0 {
		dims = len(rows[0].vector)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:             "embedding_graph",
		Field:            "embedding",
		Metric:           VectorMetricCosine,
		Dimensions:       dims,
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
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(dims),
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

func TestColumnGraphScalarU8CodeMatchesRoundReference2642(t *testing.T) {
	cases := []float32{
		float32(math.NaN()), float32(math.Inf(-1)), -10, -1.01, -1, -0.999, -0.99607843,
		-0.5, -0.003921569, 0, 0.003921569, 0.5, 0.99607843, 0.999, 1, 1.01, 10, float32(math.Inf(1)),
	}
	for i := -512; i <= 512; i++ {
		cases = append(cases, float32(i)/255)
	}
	for _, value := range cases {
		got := columnVectorGraphScalarU8Code(value)
		want := columnVectorGraphScalarU8CodeRoundReference2642(value)
		if got != want {
			t.Fatalf("columnVectorGraphScalarU8Code(%g)=%d want round reference %d", value, got, want)
		}
	}
}

func columnVectorGraphScalarU8CodeRoundReference2642(value float32) byte {
	if math.IsNaN(float64(value)) {
		return 0
	}
	scaled := math.Round((float64(value) + 1.0) * 127.5)
	if scaled < 0 {
		return 0
	}
	if scaled > 255 {
		return 255
	}
	return byte(scaled)
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
