package collections

import (
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestColumnStoreMetadataRoundTripsReopenAndCacheIdentity(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("create column-enabled collection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("open column-enabled collection: %v", err)
	}
	meta := col.Meta()
	assertNormalizedColumnStoreMeta(t, meta)
	meta.Options.ColumnStore.Columns[0].Name = "mutated"
	if got := col.Meta().Options.ColumnStore.Columns[0].Name; got != "time_us" {
		t.Fatalf("Meta leaked column mutation: got %q", got)
	}
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok {
		t.Fatal("ColumnStoreCacheIdentity returned ok=false")
	}
	if id.Collection != "events" || id.ManifestGeneration != 0 || id.ManifestRoot != 0 || id.SchemaHash == 0 {
		t.Fatalf("unexpected cache identity: %+v", id)
	}
	if key := id.BlockKey(ColumnCacheEntryDecodedBlock, 17, ColumnAssetCacheIdentity{Segment: 3, Offset: 99, Length: 4096, Checksum: 7}, 4); key.Identity != id || key.PartID != 17 || key.Block != 4 {
		t.Fatalf("unexpected block cache key: %+v", key)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("open reopened column-enabled collection: %v", err)
	}
	assertNormalizedColumnStoreMeta(t, reopenedCol.Meta())
	reopenedID, ok := reopenedCol.ColumnStoreCacheIdentity()
	if !ok {
		t.Fatal("reopened ColumnStoreCacheIdentity returned ok=false")
	}
	if reopenedID.SchemaHash != id.SchemaHash {
		t.Fatalf("schema hash changed across reopen: got %d want %d", reopenedID.SchemaHash, id.SchemaHash)
	}
}

func TestColumnStoreActiveManifestRootValidates(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(identity)}}
	rootID := publishColumnStoreCatalogForTest(t, d, meta, *identity)
	col, err := NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatalf("open active column manifest collection: %v", err)
	}
	cacheID, ok := col.ColumnStoreCacheIdentity()
	if !ok {
		t.Fatal("ColumnStoreCacheIdentity returned ok=false")
	}
	if cacheID.ManifestGeneration != identity.Generation || cacheID.ManifestRoot != rootID {
		t.Fatalf("unexpected active manifest identity: %+v rootID=%d", cacheID, rootID)
	}
}

func TestColumnStoreDisabledCollectionHasNoCacheIdentity(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if id, ok := col.ColumnStoreCacheIdentity(); ok {
		t.Fatalf("column-disabled collection returned cache identity: %+v", id)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if got := catalog.rootID(collectionColumnManifestRootName("users")); got != 0 {
		t.Fatalf("column-disabled collection has column manifest root descriptor %d", got)
	}
}

func TestColumnStoreActiveManifestFailsClosedWithoutRootDescriptor(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	if _, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(identity)},
	}); err != nil {
		t.Fatalf("create active column manifest metadata: %v", err)
	}
	_, err = NewCollectionManager(d).OpenCollection("events")
	if err == nil || !strings.Contains(err.Error(), "missing root descriptor") {
		t.Fatalf("OpenCollection err=%v want missing root descriptor", err)
	}
}

func TestColumnStoreActiveManifestFailsClosedOnIdentityMismatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(identity)}}
	publishColumnStoreCatalogForTest(t, d, meta, ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0x11111111})
	_, err = NewCollectionManager(d).OpenCollection("events")
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("OpenCollection err=%v want identity mismatch", err)
	}
}

func TestColumnStoreMetadataValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ColumnStoreConfig
		want string
	}{
		{
			name: "disabled with fields",
			cfg: &ColumnStoreConfig{
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
			},
			want: "enabled=true",
		},
		{
			name: "duplicate column",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
					{Name: "time_us", Path: "commit.time_us", ValueType: ColumnStoreValueInt64},
				},
			},
			want: "duplicate column",
		},
		{
			name: "unknown sort column",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				SortKey: []ColumnSortKey{{Column: "missing"}},
			},
			want: "unknown column",
		},
		{
			name: "vector column requires dims",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector}},
			},
			want: "vector_dims",
		},
		{
			name: "adjacency column rejects dims",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, VectorDims: 16}},
			},
			want: "only float32_vector columns may set vector_dims",
		},
		{
			name: "float32 column rejects dims",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32, VectorDims: 1}},
			},
			want: "only float32_vector columns may set vector_dims",
		},
		{
			name: "vector sort key rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 128}},
				SortKey: []ColumnSortKey{{Column: "embedding"}},
			},
			want: "not orderable",
		},
		{
			name: "vector aggregate rejected",
			cfg: &ColumnStoreConfig{
				Enabled:           true,
				Columns:           []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 128}},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "min_embedding", Column: "embedding", Kind: ColumnAggregateMin}},
			},
			want: "does not support",
		},
		{
			name: "vector dictionary rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 128, Dictionary: true}},
			},
			want: "dictionary",
		},
		{
			name: "float32 dictionary rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32, Dictionary: true}},
			},
			want: "dictionary",
		},
		{
			name: "unsupported locator",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				Locator: &ColumnLocatorConfig{Strategy: "per-column-root"},
			},
			want: "unsupported column locator strategy",
		},
		{
			name: "unsupported active manifest version",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				ActiveManifest: &ColumnManifestIdentity{
					Generation: 7,
					Version:    99,
					Checksum:   123,
				},
			},
			want: "unsupported active column manifest version",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: tt.cfg}})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("normalizeCollectionMeta err=%v want %q", err, tt.want)
			}
		})
	}
}

func TestColumnStoreVectorMetadataNormalizes(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = append(cfg.Columns,
		ColumnStoreColumn{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 128},
		ColumnStoreColumn{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
		ColumnStoreColumn{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList},
	)
	cfg.SortKey = append(cfg.SortKey, ColumnSortKey{Column: "embedding_inv_norm"})
	cfg.AggregateMetadata = append(cfg.AggregateMetadata,
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", Kind: ColumnAggregateMax},
	)
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	if meta.Options.ColumnStore.SchemaHash == 0 {
		t.Fatal("schema hash was not populated")
	}

	changed := testColumnStoreConfig(nil)
	changed.Columns = append(changed.Columns,
		ColumnStoreColumn{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 256},
		ColumnStoreColumn{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
		ColumnStoreColumn{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList},
	)
	changed.SortKey = append(changed.SortKey, ColumnSortKey{Column: "embedding_inv_norm"})
	changed.AggregateMetadata = append(changed.AggregateMetadata,
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", Kind: ColumnAggregateMax},
	)
	changedMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: changed}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta changed: %v", err)
	}
	if changedMeta.Options.ColumnStore.SchemaHash == meta.Options.ColumnStore.SchemaHash {
		t.Fatalf("schema hash did not include vector dims: %x", meta.Options.ColumnStore.SchemaHash)
	}

	changed = testColumnStoreConfig(nil)
	changed.Columns = append(changed.Columns,
		ColumnStoreColumn{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 128},
		ColumnStoreColumn{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueDouble},
		ColumnStoreColumn{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList},
	)
	changed.SortKey = append(changed.SortKey, ColumnSortKey{Column: "embedding_inv_norm"})
	changed.AggregateMetadata = append(changed.AggregateMetadata,
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", Kind: ColumnAggregateMax},
	)
	changedMeta, err = normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: changed}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta changed scalar type: %v", err)
	}
	if changedMeta.Options.ColumnStore.SchemaHash == meta.Options.ColumnStore.SchemaHash {
		t.Fatalf("schema hash did not include scalar value type: %x", meta.Options.ColumnStore.SchemaHash)
	}
}

func BenchmarkColumnStoreControlPlane(b *testing.B) {
	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(identity)}})
	if err != nil {
		b.Fatalf("normalize meta: %v", err)
	}
	encoded, err := encodeCollectionMeta(meta)
	if err != nil {
		b.Fatalf("encode meta: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	rootID := publishColumnStoreCatalogForTest(b, d, meta, *identity)
	snap := d.AcquireSnapshot()
	if snap == nil {
		b.Fatal("missing snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "events")
	if err != nil {
		b.Fatalf("load catalog: %v", err)
	}
	_ = snap.Close()

	b.Run("encode_decode_meta", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			encoded, err := encodeCollectionMeta(meta)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := decodeCollectionMeta(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode_meta", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := decodeCollectionMeta(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("load_catalog_active_manifest", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			snap := d.AcquireSnapshot()
			if snap == nil {
				b.Fatal("missing snapshot")
			}
			if _, err := loadCollectionCatalog(snap, "events"); err != nil {
				b.Fatal(err)
			}
			_ = snap.Close()
		}
	})
	b.Run("cache_identity_and_block_key", func(b *testing.B) {
		b.ReportAllocs()
		asset := ColumnAssetCacheIdentity{Segment: 3, Offset: 99, Length: 4096, Checksum: 7}
		var key ColumnBlockCacheKey
		for i := 0; i < b.N; i++ {
			id, ok := columnStoreCacheIdentity(catalog, 123, 456)
			if !ok || id.ManifestRoot != rootID {
				b.Fatalf("bad identity: %+v ok=%v", id, ok)
			}
			key = id.BlockKey(ColumnCacheEntryDecodedBlock, uint64(i), asset, uint32(i))
		}
		_ = key
	})
	b.Run("decode_manifest_identity_record", func(b *testing.B) {
		b.ReportAllocs()
		record := encodeColumnManifestIdentityRecord(*identity)
		for i := 0; i < b.N; i++ {
			decoded, err := decodeColumnManifestIdentityRecord(record)
			if err != nil {
				b.Fatal(err)
			}
			if decoded.Generation != identity.Generation {
				b.Fatal(decoded)
			}
		}
	})
}

func testColumnStoreConfig(active *ColumnManifestIdentity) *ColumnStoreConfig {
	return &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "rows", Kind: ColumnAggregateCount},
			{Name: "min_time_us", Column: "time_us", Kind: ColumnAggregateMin},
			{Name: "max_time_us", Column: "time_us", Kind: ColumnAggregateMax},
		},
		ActiveManifest: active,
	}
}

func assertNormalizedColumnStoreMeta(t *testing.T, meta CollectionMeta) {
	t.Helper()
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled {
		t.Fatalf("missing enabled column_store metadata: %+v", meta.Options.ColumnStore)
	}
	if got := cfg.RetainedPayload; got != ColumnRetainedPayloadNonColumn {
		t.Fatalf("retained payload=%q want %q", got, ColumnRetainedPayloadNonColumn)
	}
	if got := cfg.Reconstruction; got != ColumnReconstructionRetainedPayloadAndColumns {
		t.Fatalf("reconstruction=%q want %q", got, ColumnReconstructionRetainedPayloadAndColumns)
	}
	if cfg.AssetManager == nil || cfg.AssetManager.Kind != ColumnAssetManagerValueLog || !cfg.AssetManager.IsolatedNamespace || cfg.AssetManager.Namespace != "events/column-assets" {
		t.Fatalf("unexpected asset manager metadata: %+v", cfg.AssetManager)
	}
	if cfg.Locator == nil || cfg.Locator.Strategy != ColumnLocatorStrategySideIndex {
		t.Fatalf("unexpected locator metadata: %+v", cfg.Locator)
	}
}

func publishColumnStoreCatalogForTest(tb testing.TB, d *backenddb.DB, meta CollectionMeta, rootIdentity ColumnManifestIdentity) uint64 {
	tb.Helper()
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		tb.Fatalf("normalize meta: %v", err)
	}
	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		tb.Fatalf("encode meta: %v", err)
	}
	rootName := collectionColumnManifestRootName(normalized.Name)
	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{{
		Iter: columnManifestIdentityIterator(rootIdentity),
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
		tb.Fatalf("publish column manifest root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		tb.Fatalf("unexpected root IDs: %v", rootIDs)
	}
	return rootIDs[0]
}
