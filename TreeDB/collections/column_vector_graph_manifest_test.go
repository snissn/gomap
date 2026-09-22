package collections

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestColumnVectorGraphManifestRecordRoundTripV2A(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", *baseCfg, def)
	if err != nil {
		t.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	record := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: 11,
		BaseManifestChecksum:   0xabcddcba,
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               3,
		AssetRef: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  graphCfg.AssetManager.Namespace,
			Generation: 11,
			PartID:     7,
			FileID:     1,
			Offset:     128,
			Length:     4096,
			Checksum:   0xfeedbeef,
		},
		AssetBytes: 4096,
	}
	encoded, err := encodeColumnVectorGraphManifestRecord(record)
	if err != nil {
		t.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	decoded, err := decodeColumnVectorGraphManifestRecord(encoded)
	if err != nil {
		t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded=%+v want %+v", decoded, record)
	}
	if key := columnVectorGraphManifestRecordKey(def.Name); !bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes) {
		t.Fatalf("graph record key %q missing graph prefix", key)
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt = append(corrupt, 0)
	if _, err := decodeColumnVectorGraphManifestRecord(corrupt); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("decode corrupt err=%v want trailing-bytes failure", err)
	}
	shortSourceHeader := append([]byte(nil), encoded...)
	shortSourceHeader = append(shortSourceHeader, 0, 0, 0, 0, 0)
	if _, err := decodeColumnVectorGraphManifestRecord(shortSourceHeader); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("decode short source header err=%v want trailing-bytes failure", err)
	}
	unknownTrailer := append([]byte(nil), encoded...)
	unknownTrailer = append(unknownTrailer, 0, 0, 0, 1, 0, 0)
	if _, err := decodeColumnVectorGraphManifestRecord(unknownTrailer); err == nil || !strings.Contains(err.Error(), "unrecognized trailing bytes") {
		t.Fatalf("decode unknown trailer err=%v want unrecognized trailing bytes failure", err)
	}
}

func TestColumnVectorGraphManifestLayer0AdjacencySourceRoundTrip1918(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", *baseCfg, def)
	if err != nil {
		t.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	sourceCfg, _, err := columnVectorGraphLayer0AdjacencySourceColumnStoreConfig("docs", *baseCfg, def)
	if err != nil {
		t.Fatalf("columnVectorGraphLayer0AdjacencySourceColumnStoreConfig: %v", err)
	}
	// CRC32/checksum identity values can legitimately be zero; the
	// generation/part/file/offset/length tuple still carries asset identity.
	graphRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: graphCfg.AssetManager.Namespace, Generation: 9, PartID: 4, FileID: 1, Offset: 128, Length: 2048, Checksum: 0}
	sourceRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: graphCfg.AssetManager.Namespace, Generation: 9, PartID: 5, FileID: 1009, Offset: 0, Length: 1024, Checksum: 0}
	snapshot := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: 9,
		BaseManifestChecksum:   0xabcddcba,
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               3,
		AssetRef:               graphRef,
		AssetBytes:             graphRef.Length,
		Layer0AdjacencySource: columnVectorGraphLayer0AdjacencySourceSnapshot{
			Present:                true,
			Schema:                 columnVectorGraphLayer0AdjacencySourceSchema,
			ColumnName:             columnVectorGraphLayer0AdjacencySourceColumnName,
			ValueType:              string(ColumnStoreValueAdjacencyList),
			Encoding:               "raw_uint32_offsets_list",
			Layer:                  0,
			SourceSchemaHash:       sourceCfg.SchemaHash,
			RowCount:               3,
			ValuesCount:            4,
			OffsetsBytes:           32,
			ValuesBytes:            16,
			PaddingBytes:           7,
			Ref:                    sourceRef,
			AssetBytes:             sourceRef.Length,
			BaseManifestGeneration: 9,
			BaseManifestChecksum:   0xabcddcba,
			BaseSchemaHash:         baseCfg.SchemaHash,
			GraphSchemaHash:        graphCfg.SchemaHash,
			GraphAssetGeneration:   graphRef.Generation,
			GraphAssetPartID:       graphRef.PartID,
			GraphAssetFileID:       graphRef.FileID,
			GraphAssetOffset:       graphRef.Offset,
			GraphAssetLength:       graphRef.Length,
			GraphAssetChecksum:     graphRef.Checksum,
		},
	}
	encoded, err := encodeColumnVectorGraphManifestRecord(snapshot)
	if err != nil {
		t.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	decoded, err := decodeColumnVectorGraphManifestRecord(encoded)
	if err != nil {
		t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("decoded=%+v want %+v", decoded, snapshot)
	}
	for _, cut := range []int{1, 8} {
		truncated := append([]byte(nil), encoded[:len(encoded)-cut]...)
		decoded, err := decodeColumnVectorGraphManifestRecord(truncated)
		if err != nil {
			t.Fatalf("decode source truncated by %d: %v", cut, err)
		}
		want := snapshot
		want.Layer0AdjacencySource = columnVectorGraphLayer0AdjacencySourceSnapshot{}
		if !reflect.DeepEqual(decoded, want) {
			t.Fatalf("decode source truncated by %d decoded=%+v want optional source ignored", cut, decoded)
		}
	}
}

func TestColumnVectorGraphPhysicalConfigUsesLittleEndianVectorOnlyM1C(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", *baseCfg, testColumnGraphVectorIndexDefinitionV2A())
	if err != nil {
		t.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	if len(graphCfg.Columns) != 3 {
		t.Fatalf("graph columns=%d want 3", len(graphCfg.Columns))
	}
	vectorCol := columnVectorGraphPhysicalColumnForTestM1C(t, graphCfg.Columns, columnVectorGraphVectorColumnName)
	if got := vectorCol.FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
		t.Fatalf("vector fixed_width_encoding=%q want %q", got, ColumnFixedWidthEncodingLittleEndian)
	}
	invNormCol := columnVectorGraphPhysicalColumnForTestM1C(t, graphCfg.Columns, columnVectorGraphInvNormColumnName)
	if got := invNormCol.FixedWidthEncoding; got != ColumnFixedWidthEncodingDefault {
		t.Fatalf("inv_norm fixed_width_encoding=%q want default", got)
	}
	adjacencyCol := columnVectorGraphPhysicalColumnForTestM1C(t, graphCfg.Columns, columnVectorGraphAdjacencyColumnName)
	if got := adjacencyCol.FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
		t.Fatalf("adjacency fixed_width_encoding=%q want %q", got, ColumnFixedWidthEncodingLittleEndian)
	}
}

func TestColumnVectorGraphPhysicalAssetWriteRejectsInvalidInputBeforeSegmentAllocationV2A(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", *baseCfg, testColumnGraphVectorIndexDefinitionV2A())
	if err != nil {
		t.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())

	if _, err := writeColumnVectorGraphPhysicalAssetToManager(root, graphCfg, nil, 7, 1); err == nil || !strings.Contains(err.Error(), "payload is empty") {
		t.Fatalf("writeColumnVectorGraphPhysicalAssetToManager err=%v want empty-payload failure", err)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(root, graphCfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	entries, err := os.ReadDir(namespace.SegmentDir)
	if err == nil {
		if len(entries) != 0 {
			t.Fatalf("segment files after rejected write=%d want 0", len(entries))
		}
		return
	}
	if !os.IsNotExist(err) {
		t.Fatalf("ReadDir segment dir err=%v want missing or empty", err)
	}
}

func TestColumnVectorGraphManifestRecordsParticipateInIdentityWithoutRowLaneCountsV2A(t *testing.T) {
	cfg, input, manifest, asset := makeColumnManifestWithPartForGraphTestV2A(t)
	graphRecord := testColumnVectorGraphManifestRecordV2A(t, cfg, testColumnGraphVectorIndexDefinitionV2A(), manifest.Identity, asset.Ref)
	withGraph := cloneColumnManifestRecords(manifest.Records)
	withGraph = append(withGraph, graphRecord)
	sortColumnManifestRecords(withGraph)
	withGraphIdentity := manifest.Identity
	withGraphIdentity.Checksum = checksumColumnManifestRecords(input, manifest.Identity.Generation, withGraph)

	withoutGraphChecksum := checksumColumnManifestRecords(input, manifest.Identity.Generation, manifest.Records)
	if withGraphIdentity.Checksum == withoutGraphChecksum {
		t.Fatalf("graph manifest record did not change identity checksum=%d", withGraphIdentity.Checksum)
	}

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, withGraphIdentity, withGraph)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer func() { _ = snap.Close() }()
	caps, err := loadColumnManifestPlannerCapabilitiesForScan(snap, rootID, *cfg, withGraphIdentity, "events")
	if err != nil {
		t.Fatalf("loadColumnManifestPlannerCapabilitiesForScan: %v", err)
	}
	if caps.PhysicalAssetCount != 1 || caps.MutationParts != 0 {
		t.Fatalf("planner caps=%+v want only the row-lane part counted", caps)
	}
}

func makeColumnManifestWithPartForGraphTestV2A(tb testing.TB) (*ColumnStoreConfig, ColumnPublishManifestEncodeInput, ColumnPublishManifestEncodeResult, ColumnPreparedAsset) {
	tb.Helper()
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.Ref.PartID = 1
	asset.GenerationID = 1
	asset.Reason = string(ColumnPublishOperationInsert)
	input := ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{asset},
			RowCount:           1,
			ColumnPayloadBytes: asset.Bytes,
		},
	}
	manifest, err := encodeColumnManifestForWrite(input)
	if err != nil {
		tb.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	return cfg, input, manifest, asset
}

func TestColumnVectorGraphPhysicalAssetRoundTripV2A(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(t.TempDir(), "docs", *baseCfg, def, 12, 4, 88, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1, 2}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0, 2}},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1, Adjacency: []uint32{0, 1}},
	})
	if err != nil {
		t.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	raw, err := readColumnPhysicalAssetFromManager(prepared.AssetRootDir, prepared.Ref)
	if err != nil {
		t.Fatalf("read graph asset: %v", err)
	}
	if err := validateColumnPhysicalAssetForManifest(raw, prepared.Ref, prepared.Config); err != nil {
		t.Fatalf("validate graph asset: %v", err)
	}
	if got := columnVectorGraphPhysicalAssetVersionForTestM1C(t, raw); got != columnPhysicalAssetVersionV5 {
		t.Fatalf("graph physical asset version=%d want %d", got, columnPhysicalAssetVersionV5)
	}
	decoded, err := decodeColumnPhysicalAsset(raw)
	if err != nil {
		t.Fatalf("decode graph asset: %v", err)
	}
	decodedVectorCol := columnVectorGraphPhysicalColumnForTestM1C(t, decoded.Columns, columnVectorGraphVectorColumnName)
	if got := decodedVectorCol.FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
		t.Fatalf("decoded vector fixed_width_encoding=%q want %q", got, ColumnFixedWidthEncodingLittleEndian)
	}
	decodedInvNormCol := columnVectorGraphPhysicalColumnForTestM1C(t, decoded.Columns, columnVectorGraphInvNormColumnName)
	if got := decodedInvNormCol.FixedWidthEncoding; got != ColumnFixedWidthEncodingDefault {
		t.Fatalf("decoded inv_norm fixed_width_encoding=%q want default", got)
	}
	decodedAdjacencyCol := columnVectorGraphPhysicalColumnForTestM1C(t, decoded.Columns, columnVectorGraphAdjacencyColumnName)
	if got := decodedAdjacencyCol.FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
		t.Fatalf("decoded adjacency fixed_width_encoding=%q want %q", got, ColumnFixedWidthEncodingLittleEndian)
	}
	projection, err := newColumnPhysicalScanProjection(prepared.Config, []string{
		columnVectorGraphVectorColumnName,
		columnVectorGraphInvNormColumnName,
		columnVectorGraphAdjacencyColumnName,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	var ids []string
	var vectors [][]float32
	var invNorms []float32
	var neighbors [][]uint32
	summary, err := scanColumnPhysicalAssetRows(raw, prepared.Ref, "docs", &prepared.Config, projection, func(row columnPhysicalScanRowView) error {
		ids = append(ids, string(row.ID))
		vectors = append(vectors, append([]float32(nil), row.Values[0].Float32Vector...))
		invNorms = append(invNorms, row.Values[1].Float32)
		neighbors = append(neighbors, append([]uint32(nil), row.Values[2].AdjacencyList...))
		return nil
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalAssetRows: %v", err)
	}
	if summary.rows != 3 || strings.Join(ids, ",") != "doc-a,doc-b,doc-c" {
		t.Fatalf("summary=%+v ids=%v", summary, ids)
	}
	if got := neighbors[1]; len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("row1 neighbors=%v", got)
	}
	if got := vectors[0]; len(got) != 3 || got[0] != 1 || got[1] != 0 || got[2] != 0 {
		t.Fatalf("row0 vector=%v", got)
	}
	if got := vectors[1]; len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 0 {
		t.Fatalf("row1 vector=%v", got)
	}
	if got := invNorms[2]; got != 1 {
		t.Fatalf("row2 inv_norm=%v want 1", got)
	}
}

func columnVectorGraphPhysicalColumnForTestM1C(tb testing.TB, columns []ColumnStoreColumn, name string) ColumnStoreColumn {
	tb.Helper()
	for _, col := range columns {
		if col.Name == name {
			return col
		}
	}
	tb.Fatalf("missing graph physical column %q in %+v", name, columns)
	return ColumnStoreColumn{}
}

func columnVectorGraphPhysicalAssetVersionForTestM1C(tb testing.TB, raw []byte) uint16 {
	tb.Helper()
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		tb.Fatalf("graph physical asset magic=0x%08x want 0x%08x", magic, columnPhysicalAssetMagic)
	}
	version := cur.u16()
	if cur.err != nil {
		tb.Fatalf("read graph physical asset header: %v", cur.err)
	}
	return version
}

func TestColumnGraphVectorIndexStatusRequiresVectorIndexStateRecord1987(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	if err != nil {
		t.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	meta := CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{
			def,
		},
	}
	manifestRecords, identity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	publishColumnGraphCatalogForTestV2A(t, d, meta, identity, manifestRecords)
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopenedDB, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopenedDB.Close() }()
	col, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded || status.Loaded {
		t.Fatalf("status=%+v want missing vector-index state to be rebuild-needed", status)
	}
	if _, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{}); err == nil || !strings.Contains(err.Error(), "missing vector-index state record") {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want missing state record failure", err)
	}
}

func TestColumnGraphVectorIndexStatusRefreshesNativeRuntimeStrategyV2A(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	staleColumnGraphDef := testColumnGraphVectorIndexDefinitionV2A()
	nativeDef := staleColumnGraphDef
	nativeDef.Strategy = VectorIndexStrategyNativeRuntime
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsV2A(t, *baseCfg, staleColumnGraphDef, identity, ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  baseCfg.AssetManager.Namespace,
		Generation: 2,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     128,
		Checksum:   7,
	}, 128, 2)
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{nativeDef},
	}
	publishColumnGraphCatalogForTestV2A(t, d, meta, identity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	status, err := col.columnGraphVectorIndexStatus(staleColumnGraphDef.Name)
	if err != nil {
		t.Fatalf("columnGraphVectorIndexStatus: %v", err)
	}
	if status.Strategy != VectorIndexStrategyNativeRuntime || status.State != VectorIndexStateNativeRuntime || status.Reason != VectorIndexReasonNativeRuntime {
		t.Fatalf("status=%+v want native_runtime after catalog refresh", status)
	}
}

func TestColumnGraphVectorIndexStatusClosedDBReturnsErrClosedV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
	})
	if err != nil {
		t.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	records, identity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	publishColumnGraphCatalogForTestV2A(t, d, meta, identity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if _, err := col.VectorIndexStatus(def.Name); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("VectorIndexStatus closed err=%v want ErrClosed", err)
	}
}

func TestColumnGraphVectorIndexStatusClosedDBBeforeEarlyStatusV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: &ColumnStoreConfig{}},
		VectorIndexes: []VectorIndexDefinition{testColumnGraphVectorIndexDefinitionV2A()},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if _, err := col.columnGraphVectorIndexStatus(meta.VectorIndexes[0].Name); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("columnGraphVectorIndexStatus closed unsupported err=%v want ErrClosed", err)
	}
}

func TestColumnGraphVectorIndexStatusFailsClosedOnMissingOrMismatchedAssetV2A(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	if err != nil {
		t.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{def},
	}

	t.Run("mismatched_base_manifest", func(t *testing.T) {
		mismatchDef := def
		mismatchDef.Dimensions = 4
		mismatchMeta := meta
		mismatchMeta.VectorIndexes = []VectorIndexDefinition{mismatchDef}
		records, goodIdentity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
		publishColumnGraphCatalogForTestV2A(t, d, mismatchMeta, goodIdentity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded {
			t.Fatalf("status=%+v want mismatch rebuild-needed", status)
		}
	})

	t.Run("active_recovery_manifest_mismatch", func(t *testing.T) {
		records, goodIdentity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
		publishColumnGraphCatalogForTestV2A(t, d, meta, goodIdentity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		currentMeta := copyCollectionMeta(col.Meta())
		badCfg := *currentMeta.Options.ColumnStore
		recoveryIdentity := *badCfg.RecoveryAuthoritativeManifest
		recoveryIdentity.Checksum++
		badCfg.RecoveryAuthoritativeManifest = &recoveryIdentity
		currentMeta.Options.ColumnStore = &badCfg
		if _, err := normalizeCollectionMeta(currentMeta); err == nil || !strings.Contains(err.Error(), "recovery-authoritative column manifest must match active column manifest") {
			t.Fatalf("normalizeCollectionMeta err=%v want active/recovery mismatch failure", err)
		}
	})

	t.Run("asset_generation_mismatch", func(t *testing.T) {
		badRef := prepared.Ref
		badRef.Generation++
		records, badIdentity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, badRef, prepared.Bytes, prepared.RowCount)
		publishColumnGraphCatalogForTestV2A(t, d, meta, badIdentity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded {
			t.Fatalf("status=%+v want asset-generation mismatch rebuild-needed", status)
		}
	})

	t.Run("base_manifest_checksum_mismatch", func(t *testing.T) {
		records, goodIdentity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
		for i := range records {
			if !bytes.HasPrefix(records[i].key, columnManifestVectorGraphRecordPrefixBytes) {
				continue
			}
			graph, err := decodeColumnVectorGraphManifestRecord(records[i].value)
			if err != nil {
				t.Fatalf("decode graph record: %v", err)
			}
			graph.BaseManifestChecksum++
			records[i].value, err = encodeColumnVectorGraphManifestRecord(graph)
			if err != nil {
				t.Fatalf("encode graph record: %v", err)
			}
		}
		sortColumnManifestRecords(records)
		badIdentity := goodIdentity
		badIdentity.Checksum = checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
			Collection:        "docs",
			ColumnStore:       *baseCfg,
			Operation:         ColumnPublishOperationInsert,
			AppliedCommandLSN: 1,
		}, badIdentity.Generation, records)
		publishColumnGraphCatalogForTestV2A(t, d, meta, badIdentity, records)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded {
			t.Fatalf("status=%+v want checksum mismatch rebuild-needed", status)
		}
	})

	t.Run("missing_asset", func(t *testing.T) {
		records, goodIdentity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
		publishColumnGraphCatalogForTestV2A(t, d, meta, goodIdentity, records)
		path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), prepared.Ref)
		if err != nil {
			t.Fatalf("columnAssetSegmentPath: %v", err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("remove graph asset: %v", err)
		}
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("open collection: %v", err)
		}
		status, err := col.VectorIndexStatus(def.Name)
		if err == nil {
			t.Fatalf("VectorIndexStatus err=nil want missing asset error")
		}
		if status.State != VectorIndexStateColumnGraphUnavailable || status.Reason != VectorIndexReasonColumnGraphCorrupt || !status.RebuildNeeded || status.Loaded {
			t.Fatalf("status=%+v want corrupt unavailable", status)
		}
	})
}

func TestColumnGraphManifestMatchesDefinitionRejectsNonPartImageRefV2A(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  baseCfg.AssetManager.Namespace,
		Generation: 2,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     128,
		Checksum:   7,
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, ref, ref.Length, 2)
	manifest, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords: %v", err)
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		t.Fatal("graph record missing")
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	graph.AssetRef.Kind = ColumnAssetKind("other_part_image")
	baseCfg.ActiveManifest = &identity
	if columnVectorGraphManifestMatchesDefinition("docs", graph, def, *baseCfg, manifest, records) {
		t.Fatal("manifest matched graph ref with non-TCS1 part-image kind")
	}
}

func TestColumnVectorGraphReachabilityRejectsBaseAssetGenerationMismatchV2A(t *testing.T) {
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  baseCfg.AssetManager.Namespace,
		Generation: identity.Generation - 1,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     128,
		Checksum:   7,
	}
	records, identity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, ref, ref.Length, 2)
	_, err = columnVectorGraphAssetRefsFromManifestRecordsForReachability(records, identity.Generation, baseCfg.AssetManager.Namespace, true, []VectorIndexDefinition{def})
	if err == nil || !strings.Contains(err.Error(), "base manifest generation=7 does not match asset generation=6") {
		t.Fatalf("reachability err=%v want base/asset generation mismatch", err)
	}
}

func BenchmarkColumnGraphVectorIndexStatusLoadedV2A(b *testing.B) {
	dir := b.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(b, d)
	publishColumnVectorIndexStateAdjacencyContext1987(b, d, ctx)
	def := ctx.def
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(ctx.graph.RowCount), "graph_rows")
	if ctx.graph.RowCount > 0 {
		b.ReportMetric(float64(columnVectorGraphStorageBytesWithState(ctx.graph, ctx.state))/float64(ctx.graph.RowCount), "asset_B/row")
	}
	for i := 0; i < b.N; i++ {
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			b.Fatalf("VectorIndexStatus: %v", err)
		}
		if status.State != VectorIndexStateColumnGraphLoaded || !status.Loaded || status.RebuildNeeded {
			b.Fatalf("status=%+v want loaded", status)
		}
		vectorIndexStatusBenchSink = status
	}
}

func testColumnGraphBaseColumnStoreConfigV2A() *ColumnStoreConfig {
	return &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
		},
	}
}

func testColumnGraphVectorIndexDefinitionV2A() VectorIndexDefinition {
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		panic(err)
	}
	return def
}

func testColumnVectorGraphManifestRecordV2A(tb testing.TB, baseCfg *ColumnStoreConfig, def VectorIndexDefinition, identity ColumnManifestIdentity, ref ColumnAssetRef) columnManifestRecord {
	tb.Helper()
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("events", *baseCfg, def)
	if err != nil {
		tb.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	record := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: identity.Generation,
		// Callers must pass the base-only identity; manifest identities that
		// already include graph records have a different checksum.
		BaseManifestChecksum: identity.Checksum,
		BaseSchemaHash:       baseCfg.SchemaHash,
		GraphSchemaHash:      graphCfg.SchemaHash,
		RowCount:             1,
		AssetRef:             ref,
		AssetBytes:           ref.Length,
	}
	encoded, err := encodeColumnVectorGraphManifestRecord(record)
	if err != nil {
		tb.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	return columnManifestRecord{key: columnVectorGraphManifestRecordKey(def.Name), value: encoded}
}

func testColumnGraphManifestRecordsV2A(tb testing.TB, baseCfg ColumnStoreConfig, def VectorIndexDefinition, identity ColumnManifestIdentity, ref ColumnAssetRef, assetBytes int64, rows int) ([]columnManifestRecord, ColumnManifestIdentity) {
	tb.Helper()
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", baseCfg, def)
	if err != nil {
		tb.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	input := ColumnPublishManifestEncodeInput{
		Collection:        "docs",
		ColumnStore:       baseCfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			RowCount:           0,
			ColumnPayloadBytes: 0,
		},
	}
	header, err := encodeColumnManifestHeaderRecord(input, identity.Generation)
	if err != nil {
		tb.Fatalf("encodeColumnManifestHeaderRecord: %v", err)
	}
	baseRecords := []columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: header},
	}
	sortColumnManifestRecords(baseRecords)
	baseChecksum := checksumColumnManifestRecords(input, identity.Generation, baseRecords)
	record := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: identity.Generation,
		BaseManifestChecksum:   baseChecksum,
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               rows,
		AssetRef:               ref,
		AssetBytes:             assetBytes,
	}
	encoded, err := encodeColumnVectorGraphManifestRecord(record)
	if err != nil {
		tb.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	records := []columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: header},
		{key: columnVectorGraphManifestRecordKey(def.Name), value: encoded},
	}
	sortColumnManifestRecords(records)
	identity.Checksum = checksumColumnManifestRecords(input, identity.Generation, records)
	return records, identity
}

func publishColumnGraphCatalogForTestV2A(tb testing.TB, d *backenddb.DB, meta CollectionMeta, identity ColumnManifestIdentity, records []columnManifestRecord) uint64 {
	tb.Helper()
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		tb.Fatalf("normalize meta: %v", err)
	}
	cfg := normalized.Options.ColumnStore
	cfg.ActiveManifest = &identity
	cfg.RecoveryAuthoritativeManifest = &identity
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	normalized.Options.ColumnStore = cfg
	normalized, err = normalizeCollectionMeta(normalized)
	if err != nil {
		tb.Fatalf("normalize active meta: %v", err)
	}
	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		tb.Fatalf("encode meta: %v", err)
	}
	rootName := collectionColumnManifestRootName(normalized.Name)
	manifestIter := columnManifestRootRecordIterator(encodeColumnManifestIdentityRecordArray(identity), records)
	defer func() { _ = manifestIter.Close() }()
	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{{
		Iter: manifestIter,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{
			systemCollectionMetaKey(normalized.Name): encoded,
			systemCollectionRootKey(rootName):        encodeRootID(rootIDs[0]),
		})
	})
	if err != nil {
		tb.Fatalf("publish column graph manifest root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		tb.Fatalf("unexpected root IDs: %v", rootIDs)
	}
	return rootIDs[0]
}
