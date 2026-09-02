package collections

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnVectorIndexStateManifestRecordRoundTrip1986(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	if !columnVectorIndexStateTypedColumnEncodingNamesMatchConstants() {
		t.Fatal("vector-index state typed-column encoding names drifted from typedcolumn constants")
	}
	state := testColumnVectorIndexStateSnapshot1986(*baseCfg, def, 11, 3)
	encoded, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	decoded, err := decodeColumnVectorIndexStateRecord(encoded)
	if err != nil {
		t.Fatalf("decodeColumnVectorIndexStateRecord: %v", err)
	}
	if !reflect.DeepEqual(decoded, state) {
		t.Fatalf("decoded=%+v want %+v", decoded, state)
	}
	key := columnVectorIndexStateRecordKey(def.Name)
	if !bytes.HasPrefix(key, columnVectorIndexStateRecordPrefixBytes) {
		t.Fatalf("state record key %q missing vector-index state prefix", key)
	}
	if bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes) {
		t.Fatalf("state record key %q must not use legacy graph record prefix", key)
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt = append(corrupt, 0)
	if _, err := decodeColumnVectorIndexStateRecord(corrupt); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("decode corrupt err=%v want trailing-bytes failure", err)
	}

	duplicate := state
	duplicate.Assets = append(append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...), state.Assets[0])
	if _, err := encodeColumnVectorIndexStateRecord(duplicate); err == nil || !strings.Contains(err.Error(), "duplicate asset") {
		t.Fatalf("encode duplicate err=%v want duplicate asset failure", err)
	}
}

func TestColumnVectorIndexStateManifestOversizedControlRecordUsesBoundedCompression3914(t *testing.T) {
	ctx := makeColumnVectorIndexStateStatusContext1986(t)
	def := ctx.def
	state := testColumnVectorIndexStateSnapshot1986(*ctx.cfg, def, 11, 1_000_000)
	layer0 := state.Assets[0]
	const layers = 32
	for layer := 1; layer < layers; layer++ {
		asset := layer0
		asset.AssetID = fmt.Sprintf("hnsw/layer/%d", layer)
		asset.Ref.PartID = uint64(100 + layer)
		asset.Ref.FileID = uint32(1_000 + layer)
		asset.Ref.Offset = int64(layer) * 4_096
		asset.Ref.Checksum = uint32(10_000 + layer)
		state.Assets = append(state.Assets, asset)
	}
	state.AdjacencyLayerCount = layers

	rawV2 := encodeColumnVectorIndexStateRecordV2(state)
	if len(rawV2) <= columnVectorIndexStateMaxInlineRecordBytes {
		t.Fatalf("v2 bytes=%d want > inline limit=%d", len(rawV2), columnVectorIndexStateMaxInlineRecordBytes)
	}
	encoded, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	if len(encoded) > columnVectorIndexStateMaxInlineRecordBytes {
		t.Fatalf("encoded bytes=%d exceed inline limit=%d", len(encoded), columnVectorIndexStateMaxInlineRecordBytes)
	}
	cur := manifestCursor{raw: encoded}
	if magic, version := cur.u32(), cur.u16(); magic != columnVectorIndexStateMagic || version != columnVectorIndexStateVersion {
		t.Fatalf("compressed envelope magic/version=(0x%08x,%d) want (0x%08x,%d)", magic, version, columnVectorIndexStateMagic, columnVectorIndexStateVersion)
	}
	decoded, err := decodeColumnVectorIndexStateRecord(encoded)
	if err != nil {
		t.Fatalf("decodeColumnVectorIndexStateRecord: %v", err)
	}
	if !reflect.DeepEqual(decoded, state) {
		t.Fatalf("decoded oversized state differs from input")
	}

	builder := node.NewBuilder(make([]byte, page.PageSize), page.PageTypeLeaf)
	if err := builder.AddLeafEntry(columnVectorIndexStateRecordKey(def.Name), encoded, 0, page.ValuePtr{}); err != nil {
		t.Fatalf("compressed state does not fit an empty manifest leaf: %v", err)
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt = append(corrupt, 0)
	if _, err := decodeColumnVectorIndexStateRecord(corrupt); err == nil {
		t.Fatal("decode compressed state with trailing corruption err=nil")
	}
}

func TestColumnVectorIndexStateManifestEncodeInfersAdjacencyLayerCount1986(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	state := testColumnVectorIndexStateSnapshot1986(*baseCfg, def, 11, 3)
	layer1 := state.Assets[0]
	layer1.AssetID = "hnsw/layer/1"
	layer1.Ref.PartID = 31
	layer1.Ref.FileID = 131
	layer1.Ref.Checksum = 31
	layer2 := state.Assets[0]
	layer2.AssetID = "hnsw/layer/2"
	layer2.Ref.PartID = 32
	layer2.Ref.FileID = 132
	layer2.Ref.Checksum = 32
	state.Assets = append(state.Assets, layer1, layer2)
	state.AdjacencyLayerCount = 0
	wantLayers, err := columnVectorIndexStateAdjacencyLayerCountFromAssets(state.Assets)
	if err != nil {
		t.Fatalf("columnVectorIndexStateAdjacencyLayerCountFromAssets: %v", err)
	}

	encoded, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	decoded, err := decodeColumnVectorIndexStateRecord(encoded)
	if err != nil {
		t.Fatalf("decodeColumnVectorIndexStateRecord: %v", err)
	}
	if got := decoded.AdjacencyLayerCount; got != wantLayers {
		t.Fatalf("decoded adjacency layer count=%d want inferred %d", got, wantLayers)
	}
}

func TestColumnVectorIndexStateManifestRecordV1Compatibility1986(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	state := testColumnVectorIndexStateSnapshot1986(*baseCfg, def, 11, 3)
	var b bytes.Buffer
	writeManifestUint32(&b, columnVectorIndexStateMagic)
	writeManifestUint16(&b, columnVectorIndexStateVersionV1)
	writeManifestString(&b, state.IndexName)
	writeManifestString(&b, state.Field)
	writeManifestString(&b, state.Metric.String())
	writeManifestString(&b, state.Encoding.String())
	writeManifestUint64(&b, uint64(state.Dimensions))
	writeManifestUint64(&b, uint64(state.M))
	writeManifestUint64(&b, uint64(state.EfConstruction))
	writeManifestUint64(&b, uint64(state.EfSearch))
	writeManifestUint64(&b, uint64(state.RowCount))
	writeManifestUint64(&b, state.BaseManifestGeneration)
	writeManifestUint64(&b, state.BaseManifestChecksum)
	writeManifestUint64(&b, state.BaseSchemaHash)
	writeManifestUint64(&b, uint64(len(state.Assets)))
	for _, asset := range state.Assets {
		writeManifestString(&b, asset.Role)
		writeManifestString(&b, asset.AssetID)
		writeManifestString(&b, asset.LogicalType)
		writeManifestString(&b, asset.PhysicalEncoding)
		writeManifestUint64(&b, uint64(asset.RowCount))
		writeManifestUint64(&b, asset.SourceSchemaHash)
		writeManifestString(&b, string(asset.Ref.Kind))
		writeManifestString(&b, asset.Ref.Namespace)
		writeManifestUint64(&b, asset.Ref.Generation)
		writeManifestUint64(&b, asset.Ref.PartID)
		writeManifestUint64(&b, uint64(asset.Ref.FileID))
		writeManifestUint64(&b, uint64(asset.Ref.Offset))
		writeManifestUint64(&b, uint64(asset.Ref.Length))
		writeManifestUint64(&b, uint64(asset.Ref.Checksum))
		writeManifestUint64(&b, uint64(asset.AssetBytes))
	}
	decoded, err := decodeColumnVectorIndexStateRecord(b.Bytes())
	if err != nil {
		t.Fatalf("decodeColumnVectorIndexStateRecord v1: %v", err)
	}
	if decoded.AdjacencyLayerCount != 0 {
		t.Fatalf("decoded adjacency layer count=%d want 0 for v1 record", decoded.AdjacencyLayerCount)
	}
	state.AdjacencyLayerCount = 0
	if !reflect.DeepEqual(decoded, state) {
		t.Fatalf("decoded=%+v want %+v", decoded, state)
	}
}

func TestColumnVectorIndexStateValidationRejectsTypedAssetMismatches1986(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	base := testColumnVectorIndexStateSnapshot1986(*baseCfg, def, 11, 3)
	base.Assets = base.Assets[:1]

	cases := []struct {
		name    string
		mutate  func(*columnVectorIndexStateSnapshot)
		message string
	}{
		{
			name: "generation_mismatch",
			mutate: func(state *columnVectorIndexStateSnapshot) {
				state.Assets[0].Ref.Generation++
			},
			message: "ref generation",
		},
		{
			name: "row_count_mismatch",
			mutate: func(state *columnVectorIndexStateSnapshot) {
				state.Assets[0].RowCount++
			},
			message: "row_count",
		},
		{
			name: "schema_missing",
			mutate: func(state *columnVectorIndexStateSnapshot) {
				state.Assets[0].SourceSchemaHash = 0
			},
			message: "schema",
		},
		{
			name: "type_mismatch",
			mutate: func(state *columnVectorIndexStateSnapshot) {
				state.Assets[0].LogicalType = "adjacency_list"
			},
			message: "type/encoding",
		},
		{
			name: "encoding_mismatch",
			mutate: func(state *columnVectorIndexStateSnapshot) {
				state.Assets[0].PhysicalEncoding = "raw_uint32_dense"
			},
			message: "type/encoding",
		},
		{
			name: "wrong_kind",
			mutate: func(state *columnVectorIndexStateSnapshot) {
				state.Assets[0].Ref.Kind = ColumnAssetKindTCS1PartImage
			},
			message: "ref kind",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := base
			state.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), base.Assets...)
			tc.mutate(&state)
			if _, err := encodeColumnVectorIndexStateRecord(state); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("encode err=%v want %q", err, tc.message)
			}
		})
	}
}

func TestColumnVectorIndexStateMatchRejectsIdentityMismatches1986(t *testing.T) {
	ctx := makeColumnVectorIndexStateStatusContext1986(t)
	state := columnVectorIndexStateSnapshotFromGraph(ctx.graph)
	if got := columnVectorIndexStateMatchStatus(state, ctx.def, *ctx.cfg, ctx.manifest, ctx.records); got != columnVectorIndexStateMatchLoaded {
		t.Fatalf("match status=%v want loaded", got)
	}

	cases := []struct {
		name string
		want columnVectorIndexStateMatch
		edit func(*columnVectorIndexStateSnapshot)
	}{
		{
			name: "base_generation_mismatch",
			want: columnVectorIndexStateMatchUnsupportedVisibility,
			edit: func(state *columnVectorIndexStateSnapshot) { state.BaseManifestGeneration++ },
		},
		{
			name: "base_checksum_mismatch",
			want: columnVectorIndexStateMatchMismatch,
			edit: func(state *columnVectorIndexStateSnapshot) { state.BaseManifestChecksum++ },
		},
		{
			name: "base_schema_mismatch",
			want: columnVectorIndexStateMatchMismatch,
			edit: func(state *columnVectorIndexStateSnapshot) { state.BaseSchemaHash++ },
		},
		{
			name: "index_mismatch",
			want: columnVectorIndexStateMatchMismatch,
			edit: func(state *columnVectorIndexStateSnapshot) { state.IndexName = "other_embedding_graph" },
		},
		{
			name: "dimension_mismatch",
			want: columnVectorIndexStateMatchMismatch,
			edit: func(state *columnVectorIndexStateSnapshot) { state.Dimensions++ },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := state
			tc.edit(&candidate)
			if got := columnVectorIndexStateMatchStatus(candidate, ctx.def, *ctx.cfg, ctx.manifest, ctx.records); got != tc.want {
				t.Fatalf("match status=%v want %v", got, tc.want)
			}
		})
	}

	rowMismatch := state
	rowMismatch.RowCount++
	if !columnVectorIndexStateDefinitionParametersMatch(&rowMismatch, &ctx.def) {
		t.Fatal("row-count mismatch should not masquerade as definition mismatch")
	}
	if columnVectorIndexStateMatchesGraph(rowMismatch, ctx.graph) {
		t.Fatal("row-count mismatch matched graph identity")
	}
}

func TestColumnVectorIndexStatePublishedReopenAndFailClosed1986(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		_ = d.Close()
		t.Fatal("rebuilt manifest missing vector-index state record")
	}
	state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		_ = d.Close()
		t.Fatalf("decode vector-index state: %v", err)
	}
	graph := graphManifestFromRecords1918(t, records, def)
	if !columnVectorIndexStateMatchesGraph(state, graph) {
		_ = d.Close()
		t.Fatalf("state=%+v does not match graph=%+v", state, graph)
	}
	assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(t, d, "docs", cfg, def, graph, state, loadColumnGraphRebuildScannedRowsOnly1987(t, d, "docs", def))
	asset, ok := findColumnVectorGraphInvNormStateAsset(state)
	if !ok || asset.RowCount != len(rows) || asset.LogicalType != columnVectorIndexStateLogicalTypeFloat32 || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawFloat32 {
		_ = d.Close()
		t.Fatalf("rebuilt vector-index state assets=%+v want raw_float32 inverse-norm state asset", state.Assets)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
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
	reopenedStatus, err := reopenedCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus reopen: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, reopenedStatus, def.Name)
}

func TestColumnGraphVectorIndexStatusValidatesVectorIndexState1986(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
	baseState := ctx.state
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{ctx.def},
	}

	t.Run("loaded_with_state_record", func(t *testing.T) {
		records, identity := appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, baseState)
		publishColumnGraphCatalogForTestV2A(t, d, meta, identity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(ctx.def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		assertColumnGraphRebuildLoadedStatusV2A(t, status, ctx.def.Name)
	})

	t.Run("state_row_count_mismatch", func(t *testing.T) {
		state := baseState
		state.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), baseState.Assets...)
		state.RowCount++
		for i := range state.Assets {
			state.Assets[i].RowCount = state.RowCount
		}
		records, identity := appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, state)
		publishColumnGraphCatalogForTestV2A(t, d, meta, identity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(ctx.def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded {
			t.Fatalf("status=%+v want state mismatch rebuild-needed", status)
		}
	})

	t.Run("state_corrupt", func(t *testing.T) {
		records, identity := appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, baseState)
		for i := range records {
			if bytes.Equal(records[i].key, columnVectorIndexStateRecordKey(ctx.def.Name)) {
				records[i].value = append(records[i].value, 0)
			}
		}
		identity.Checksum = checksumColumnManifestRecords(columnVectorIndexStateChecksumInput1986(*ctx.cfg), identity.Generation, records)
		publishColumnGraphCatalogForTestV2A(t, d, meta, identity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(ctx.def.Name)
		if err == nil {
			t.Fatal("VectorIndexStatus err=nil want corrupt state failure")
		}
		if status.State != VectorIndexStateColumnGraphUnavailable || status.Reason != VectorIndexReasonColumnGraphCorrupt || !status.RebuildNeeded {
			t.Fatalf("status=%+v want corrupt unavailable", status)
		}
	})

	t.Run("state_asset_missing", func(t *testing.T) {
		state := baseState
		missingRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: ctx.cfg.AssetManager.Namespace, Generation: state.BaseManifestGeneration, PartID: 99, FileID: 199, Offset: 0, Length: 64, Checksum: 7}
		state.Assets = []columnVectorIndexStateAssetSnapshot{columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleAdjacency, "hnsw/layer/0", missingRef, state.RowCount, ctx.cfg.SchemaHash+1)}
		records, identity := appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, state)
		publishColumnGraphCatalogForTestV2A(t, d, meta, identity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(ctx.def.Name)
		if err == nil {
			t.Fatal("VectorIndexStatus err=nil want missing state asset failure")
		}
		if status.State != VectorIndexStateColumnGraphUnavailable || status.Reason != VectorIndexReasonColumnGraphCorrupt || !status.RebuildNeeded {
			t.Fatalf("status=%+v want missing state asset corrupt", status)
		}
	})
}

type columnVectorIndexStateStatusContext1986 struct {
	cfg      *ColumnStoreConfig
	def      VectorIndexDefinition
	identity ColumnManifestIdentity
	records  []columnManifestRecord
	manifest columnManifestSnapshot
	graph    columnVectorGraphManifestSnapshot
}

func makeColumnVectorIndexStateStatusContext1986(tb testing.TB) columnVectorIndexStateStatusContext1986 {
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	return makeColumnVectorIndexStateStatusContextWithDB1986(tb, d)
}

func makeColumnVectorIndexStateStatusContextWithDB1986(tb testing.TB, d *backenddb.DB) columnVectorIndexStateStatusContext1986 {
	tb.Helper()
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		tb.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsV2A(tb, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	baseCfg.ActiveManifest = &identity
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		tb.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	graph := graphManifestFromRecords1918(tb, records, def)
	return columnVectorIndexStateStatusContext1986{cfg: baseCfg, def: def, identity: identity, records: records, manifest: manifest, graph: graph}
}

func columnVectorIndexStateAssetSnapshotForTest(role, assetID string, ref ColumnAssetRef, rows int, schemaHash uint64) columnVectorIndexStateAssetSnapshot {
	logical, encoding, strict := columnVectorIndexStateAssetTypeContract(role)
	if !strict {
		logical = "row_ref"
		encoding = "native_row_ref"
	}
	return columnVectorIndexStateAssetSnapshot{
		Role:             role,
		AssetID:          assetID,
		LogicalType:      logical,
		PhysicalEncoding: encoding,
		RowCount:         rows,
		SourceSchemaHash: schemaHash,
		Ref:              ref,
		AssetBytes:       ref.Length,
	}
}

func columnVectorIndexStateTypedColumnEncodingNamesMatchConstants() bool {
	return columnVectorIndexStateEncodingRawUint32List == typedcolumn.EncodingRawUint32OffsetsList.String() &&
		columnVectorIndexStateEncodingRawInt64 == typedcolumn.EncodingRawInt64.String() &&
		columnVectorIndexStateEncodingRawFloat32 == typedcolumn.EncodingRawFloat32.String() &&
		columnVectorIndexStateEncodingRawFloat32Vector == typedcolumn.EncodingRawFloat32Vector.String() &&
		columnVectorIndexStateEncodingRawBytesOffsets == typedcolumn.EncodingRawBytesOffsets.String()
}

func testColumnVectorIndexStateSnapshot1986(cfg ColumnStoreConfig, def VectorIndexDefinition, generation uint64, rows int) columnVectorIndexStateSnapshot {
	mkRef := func(partID uint64) ColumnAssetRef {
		return ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: cfg.AssetManager.Namespace, Generation: generation, PartID: partID, FileID: uint32(100 + partID), Offset: int64(partID) * 128, Length: 1024 + int64(partID), Checksum: uint32(partID)}
	}
	return columnVectorIndexStateSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		RowCount:               rows,
		BaseManifestGeneration: generation,
		BaseManifestChecksum:   0xabcddcba,
		BaseSchemaHash:         cfg.SchemaHash,
		AdjacencyLayerCount:    1,
		Assets: []columnVectorIndexStateAssetSnapshot{
			columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleAdjacency, "hnsw/layer/0", mkRef(21), rows, cfg.SchemaHash+1),
			columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleInverseNorm, "inv_norm_by_ordinal", mkRef(22), rows, cfg.SchemaHash+2),
			columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleNormalizedVectors, "normalized_vectors", mkRef(23), rows, cfg.SchemaHash+3),
			{Role: columnVectorIndexStateAssetRoleRowRefs, AssetID: "row_refs", LogicalType: "int64", PhysicalEncoding: "raw_int64", RowCount: rows, SourceSchemaHash: cfg.SchemaHash + 4, Ref: mkRef(24), AssetBytes: mkRef(24).Length},
			{Role: columnVectorIndexStateAssetRoleDocumentIDs, AssetID: "document_ids", LogicalType: "bytes", PhysicalEncoding: "raw_bytes_offsets", RowCount: rows, SourceSchemaHash: cfg.SchemaHash + 5, Ref: mkRef(25), AssetBytes: mkRef(25).Length},
		},
	}
}

func appendVectorIndexStateRecordForTest1986(tb testing.TB, cfg ColumnStoreConfig, records []columnManifestRecord, identity ColumnManifestIdentity, state columnVectorIndexStateSnapshot) ([]columnManifestRecord, ColumnManifestIdentity) {
	tb.Helper()
	raw, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		tb.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	out, err := replaceColumnVectorGraphManifestRecord(records, identity.Generation, columnManifestRecord{key: columnVectorIndexStateRecordKey(state.IndexName), value: raw})
	if err != nil {
		tb.Fatalf("replace state record: %v", err)
	}
	identity.Checksum = checksumColumnManifestRecords(columnVectorIndexStateChecksumInput1986(cfg), identity.Generation, out)
	return out, identity
}

func columnVectorIndexStateChecksumInput1986(cfg ColumnStoreConfig) ColumnPublishManifestEncodeInput {
	return ColumnPublishManifestEncodeInput{
		Collection:        "docs",
		ColumnStore:       cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
	}
}
