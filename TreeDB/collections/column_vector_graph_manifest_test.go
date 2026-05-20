package collections

import (
	"bytes"
	"os"
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
	if decoded != record {
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
	projection, err := newColumnPhysicalScanProjection(prepared.Config, []string{
		columnVectorGraphVectorColumnName,
		columnVectorGraphInvNormColumnName,
		columnVectorGraphAdjacencyColumnName,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	var ids []string
	var neighbors [][]uint32
	summary, err := scanColumnPhysicalAssetRows(raw, prepared.Ref, "docs", &prepared.Config, projection, func(row columnPhysicalScanRowView) error {
		ids = append(ids, string(row.ID))
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
}

func TestColumnGraphVectorIndexStatusUsesPublishedPhysicalManifestV2A(t *testing.T) {
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
	if status.State != VectorIndexStateColumnGraphLoaded || !status.Loaded || status.RebuildNeeded || status.Reason != "" {
		t.Fatalf("status=%+v want loaded column graph", status)
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
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		if status.State != VectorIndexStateColumnGraphUnavailable || status.Reason != VectorIndexReasonColumnGraphCorrupt || !status.RebuildNeeded || status.Loaded {
			t.Fatalf("status=%+v want corrupt unavailable", status)
		}
	})
}

func BenchmarkColumnGraphVectorIndexStatusLoadedV2A(b *testing.B) {
	dir := b.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		b.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	if err != nil {
		b.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	manifestRecords, identity := testColumnGraphManifestRecordsV2A(b, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	publishColumnGraphCatalogForTestV2A(b, d, meta, identity, manifestRecords)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(prepared.RowCount), "graph_rows")
	b.ReportMetric(float64(prepared.Bytes)/float64(prepared.RowCount), "asset_B/row")
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
	records, _ := testColumnGraphManifestRecordsV2A(tb, *baseCfg, def, identity, ref, ref.Length, 1)
	for _, record := range records {
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			return record
		}
	}
	tb.Fatal("missing graph manifest record")
	return columnManifestRecord{}
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
		BaseManifestChecksum:   identity.Checksum,
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
