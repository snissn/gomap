package collections

import (
	"bytes"
	"errors"
	"math"
	"strings"
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

	if _, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}); !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "not scalar_u8") {
		t.Fatalf("rabitq quantized_only err=%v want fail-closed unavailable without exact fallback", err)
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
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
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
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
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
	def := columnGraphRebuildVectorIndexDefinitionV2A(shape.dims, shape.m)
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
	insertColumnGraphRebuildRowsV2A(tb, col, rows)
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
