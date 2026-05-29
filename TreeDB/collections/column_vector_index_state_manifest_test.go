package collections

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
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
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
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
	asset, ok := findColumnVectorGraphInvNormStateAsset(state)
	if !ok || len(state.Assets) != 1 || asset.RowCount != len(rows) || asset.LogicalType != columnVectorIndexStateLogicalTypeFloat32 || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawFloat32 {
		_ = d.Close()
		t.Fatalf("rebuilt vector-index state assets=%+v want one raw_float32 inverse-norm state asset", state.Assets)
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
	ctx := makeColumnVectorIndexStateStatusContextWithDB1986(t, d)
	baseState := columnVectorIndexStateSnapshotFromGraph(ctx.graph)
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
		state.RowCount++
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
		columnVectorIndexStateEncodingRawFloat32 == typedcolumn.EncodingRawFloat32.String() &&
		columnVectorIndexStateEncodingRawFloat32Vector == typedcolumn.EncodingRawFloat32Vector.String()
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
		Assets: []columnVectorIndexStateAssetSnapshot{
			columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleAdjacency, "hnsw/layer/0", mkRef(21), rows, cfg.SchemaHash+1),
			columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleInverseNorm, "inv_norm_by_ordinal", mkRef(22), rows, cfg.SchemaHash+2),
			columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleNormalizedVectors, "normalized_vectors", mkRef(23), rows, cfg.SchemaHash+3),
			{Role: columnVectorIndexStateAssetRoleRowRefs, AssetID: "row_refs", LogicalType: "int64", PhysicalEncoding: "raw_int64", RowCount: rows, SourceSchemaHash: cfg.SchemaHash + 4, Ref: mkRef(24), AssetBytes: mkRef(24).Length},
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
