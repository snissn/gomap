package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestColumnManifestIdentityRecordKeyBytesReturnsFreshSliceM10A(t *testing.T) {
	first := newColumnManifestIdentityRecordKey()
	second := newColumnManifestIdentityRecordKey()
	if string(first) != columnManifestIdentityRecordKey || string(second) != columnManifestIdentityRecordKey {
		t.Fatalf("unexpected identity key copies: first=%q second=%q", string(first), string(second))
	}
	if len(first) == 0 {
		t.Fatal("identity record key must be non-empty")
	}
	first[0] ^= 0xff
	if string(second) != columnManifestIdentityRecordKey {
		t.Fatalf("identity key copy was mutated through another caller: %q", string(second))
	}
	if fresh := newColumnManifestIdentityRecordKey(); string(fresh) != columnManifestIdentityRecordKey {
		t.Fatalf("fresh identity key copy was mutated: %q", string(fresh))
	}
}

func TestColumnManifestIdentityRecordIteratorUsesPrivateKeySliceM10A(t *testing.T) {
	identity := ColumnManifestIdentity{Generation: 42, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	first := columnManifestIdentityIterator(identity)
	second := columnManifestIdentityIterator(identity)
	defer func() { _ = first.Close() }()
	defer func() { _ = second.Close() }()
	firstKey := first.UnsafeKey()
	secondKey := second.UnsafeKey()
	if !bytes.Equal(firstKey, []byte(columnManifestIdentityRecordKey)) || !bytes.Equal(secondKey, []byte(columnManifestIdentityRecordKey)) {
		t.Fatalf("unexpected iterator keys: first=%q second=%q", string(firstKey), string(secondKey))
	}
	firstKey[0] ^= 0xff
	if string(second.UnsafeKey()) != columnManifestIdentityRecordKey {
		t.Fatalf("iterator key slice was shared across iterators: %q", string(second.UnsafeKey()))
	}
}

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

func TestColumnStoreNormalizesLegacyValueLogAssetManagerNameM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.AssetManager = &ColumnAssetManagerConfig{
		Kind:              columnAssetManagerLegacyValueLog,
		IsolatedNamespace: true,
		Namespace:         "events/column-assets",
	}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig legacy value-log-shaped manager: %v", err)
	}
	if normalized.AssetManager == nil || normalized.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		t.Fatalf("asset manager kind=%+v want %q", normalized.AssetManager, ColumnAssetManagerValueLogShaped)
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
	if cacheID.RecoveryAuthoritativeGeneration != identity.Generation {
		t.Fatalf("unexpected recovery-authoritative generation: %+v", cacheID)
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

func TestColumnStoreActiveManifestRequiresRecoveryAuthoritativeMetadata(t *testing.T) {
	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	cfg := testColumnStoreConfig(identity)
	cfg.RecoveryAuthoritativeManifest = nil
	_, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err == nil || !strings.Contains(err.Error(), "recovery-authoritative") {
		t.Fatalf("normalizeCollectionMeta err=%v want recovery-authoritative metadata failure", err)
	}
}

func TestColumnStoreActiveManifestRequiresRecoveryAuthoritativeAppliedLSN(t *testing.T) {
	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	cfg := testColumnStoreConfig(identity)
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 0
	_, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err == nil || !strings.Contains(err.Error(), "AppliedCommandLSN") {
		t.Fatalf("normalizeCollectionMeta err=%v want recovery-authoritative AppliedCommandLSN failure", err)
	}
}

func TestColumnStoreActiveManifestFailsClosedOnRecoveryAuthoritativeMismatch(t *testing.T) {
	identity := &ColumnManifestIdentity{Generation: 42, Version: 1, Checksum: 0xfeedbeef}
	cfg := testColumnStoreConfig(identity)
	cfg.RecoveryAuthoritativeManifest = &ColumnManifestIdentity{Generation: 41, Version: 1, Checksum: 0xfeedbeef}
	_, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err == nil || !strings.Contains(err.Error(), "must match active") {
		t.Fatalf("normalizeCollectionMeta err=%v want active/recovery-authoritative mismatch", err)
	}
}

func TestColumnStoreActiveManifestFailsClosedOnIdentityMismatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	identity := &ColumnManifestIdentity{Generation: 42, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(identity)}}
	publishColumnStoreCatalogForTest(t, d, meta, ColumnManifestIdentity{Generation: 42, Version: columnManifestIdentityVersion, Checksum: 0x11111111})
	_, err = NewCollectionManager(d).OpenCollection("events")
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("OpenCollection err=%v want identity mismatch", err)
	}
}

func TestColumnStoreActiveManifestFailsClosedOnInvalidRootRecordM10C(t *testing.T) {
	identity := &ColumnManifestIdentity{Generation: 42, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	valid := encodeColumnManifestIdentityRecord(*identity)
	if len(valid) < columnManifestIdentityRecordSize {
		t.Fatalf("encoded identity record length=%d, want at least %d bytes for corruption cases", len(valid), columnManifestIdentityRecordSize)
	}
	shortRecord := valid[:len(valid)-1]
	badMagic := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badMagic[columnManifestIdentityMagicOffset:columnManifestIdentityEncodingVersionOffset], 0xdeadbeef)
	unsupportedRecordVersion := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(unsupportedRecordVersion[columnManifestIdentityEncodingVersionOffset:columnManifestIdentityManifestVersionOffset], columnManifestIdentityVersion+1)

	tests := []struct {
		name          string
		includeRecord bool
		record        []byte
		want          error
		wantContains  string
	}{
		{name: "missing identity record", want: ErrColumnManifestIdentityMissing},
		{name: "short identity record", includeRecord: true, record: shortRecord, want: ErrColumnManifestIdentityMalformed},
		{name: "bad identity magic", includeRecord: true, record: badMagic, want: ErrColumnManifestIdentityBadMagic, wantContains: "magic=0xdeadbeef"},
		{name: "unsupported identity record version", includeRecord: true, record: unsupportedRecordVersion, want: ErrColumnManifestIdentityUnsupportedVersion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			meta := CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(identity)}}
			publishColumnStoreCatalogRawManifestRootForTest(t, d, meta, tt.includeRecord, tt.record)
			_, err = NewCollectionManager(d).OpenCollection("events")
			if !errors.Is(err, tt.want) {
				t.Fatalf("OpenCollection err=%v want errors.Is %v", err, tt.want)
			}
			if tt.wantContains != "" && !strings.Contains(err.Error(), tt.wantContains) {
				t.Fatalf("OpenCollection err=%v want substring %q", err, tt.wantContains)
			}
		})
	}
}

func TestColumnManifestIdentityRecordRejectsNonZeroReserved(t *testing.T) {
	identity := ColumnManifestIdentity{Generation: 42, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	record := encodeColumnManifestIdentityRecord(identity)
	record[columnManifestIdentityReservedOffset+columnManifestIdentityReservedSize-1] = 1
	decoded, err := decodeColumnManifestIdentityRecord(record)
	if !errors.Is(err, ErrColumnManifestIdentityNonZeroReserved) || !strings.Contains(err.Error(), "0x00000001") {
		t.Fatalf("decodeColumnManifestIdentityRecord decoded=%+v err=%v want hex reserved-field rejection", decoded, err)
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
			name: "vector column rejects negative elements per row with dims",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3, ElementsPerRow: -1}},
			},
			want: "elements_per_row: must be non-negative",
		},
		{
			name: "vector column rejects negative dims with elements per row",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: -1, ElementsPerRow: 128}},
			},
			want: "vector_dims: must be non-negative",
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
			name: "uint32 list rejects nullable",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "tags", Path: "tags", ValueType: ColumnStoreValueUint32List, Owner: TypedStorageOwnerColumnPart, Nullable: true}},
			},
			want: "nullable uint32_list",
		},
		{
			name: "uint32 list rejects adjacency degree",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "tags", Path: "tags", ValueType: ColumnStoreValueUint32List, Owner: TypedStorageOwnerColumnPart, AdjacencyDegree: 3}},
			},
			want: "only adjacency_list columns may set adjacency_degree",
		},
		{
			name: "uint32 list rejects adjacency layout selector",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "tags", Path: "tags", ValueType: ColumnStoreValueUint32List, Owner: TypedStorageOwnerColumnPart, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}},
			},
			want: "only adjacency_list columns may set adjacency_layout",
		},
		{
			name: "bytes requires typed column owner",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "opaque_id", Path: "opaque_id", ValueType: ColumnStoreValueBytes}},
			},
			want: "bytes requires owner",
		},
		{
			name: "typed column adjacency requires degree",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, Owner: TypedStorageOwnerColumnPart}},
			},
			want: "adjacency_degree",
		},
		{
			name: "typed row adjacency rejects degree",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, AdjacencyDegree: 16}},
			},
			want: "only adjacency_list typed_column_part columns may set adjacency_degree",
		},
		{
			name: "typed column adjacency rejects nullable",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, Owner: TypedStorageOwnerColumnPart, Nullable: true, AdjacencyDegree: 16}},
			},
			want: "nullable adjacency_list",
		},
		{
			name: "offsets list selector requires typed column owner",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}},
			},
			want: "uint32_offsets_list requires owner",
		},
		{
			name: "offsets list selector rejects dense degree",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, Owner: TypedStorageOwnerColumnPart, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList, AdjacencyDegree: 16}},
			},
			want: "must be zero for adjacency_layout",
		},
		{
			name: "offsets list selector rejects fixed width encoding",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, Owner: TypedStorageOwnerColumnPart, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}},
			},
			want: "fixed_width_encoding",
		},
		{
			name: "adjacency layout selector rejects non adjacency",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "count", Path: "count", ValueType: ColumnStoreValueInt64, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}},
			},
			want: "only adjacency_list columns may set adjacency_layout",
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
			name: "dense numeric vector min aggregate rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
					{Name: "codes", Path: "codes", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueUint16Vector, ElementsPerRow: 3},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "min_codes", Column: "codes", GroupColumn: "kind", Kind: ColumnAggregateMin}},
			},
			want: "does not support",
		},
		{
			name: "dense numeric vector count distinct aggregate rejected",
			cfg: &ColumnStoreConfig{
				Enabled:           true,
				Columns:           []ColumnStoreColumn{{Name: "codes", Path: "codes", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueUint16Vector, ElementsPerRow: 3}},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "distinct_codes", Column: "codes", Kind: ColumnAggregateCountDistinct}},
			},
			want: "does not support",
		},
		{
			name: "string sum aggregate rejected",
			cfg: &ColumnStoreConfig{
				Enabled:           true,
				Columns:           []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString}},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "sum_kind", Column: "kind", Kind: ColumnAggregateSum}},
			},
			want: "does not support",
		},
		{
			name: "adjacency count distinct aggregate rejected",
			cfg: &ColumnStoreConfig{
				Enabled:           true,
				Columns:           []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList}},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "distinct_neighbors", Column: "neighbors", Kind: ColumnAggregateCountDistinct}},
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
			name: "unknown fixed width encoding rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 128, FixedWidthEncoding: "native"}},
			},
			want: "fixed_width_encoding",
		},
		{
			name: "string fixed width encoding rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}},
			},
			want: "fixed_width_encoding",
		},
		{
			name: "float32 fixed width encoding rejected",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}},
			},
			want: "fixed_width_encoding",
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
		{
			name: "missing active manifest version",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				ActiveManifest: &ColumnManifestIdentity{
					Generation: 7,
					Checksum:   123,
				},
			},
			want: "active column manifest version is required",
		},
		{
			name: "missing recovery-authoritative manifest version",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				ActiveManifest: &ColumnManifestIdentity{
					Generation: 7,
					Version:    1,
					Checksum:   123,
				},
				RecoveryAuthoritativeManifest: &ColumnManifestIdentity{
					Generation: 7,
					Checksum:   123,
				},
			},
			want: "recovery-authoritative column manifest version is required",
		},
		{
			name: "recovery without active manifest",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				RecoveryAuthoritativeManifest: &ColumnManifestIdentity{
					Generation: 7,
					Version:    1,
					Checksum:   123,
				},
			},
			want: "without active",
		},
		{
			name: "unsupported profile support",
			cfg: &ColumnStoreConfig{
				Enabled:        true,
				Columns:        []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				ProfileSupport: "relaxed-production",
			},
			want: "unsupported column profile support",
		},
		{
			name: "manifest root descriptor mismatch",
			cfg: &ColumnStoreConfig{
				Enabled:      true,
				Columns:      []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				ManifestRoot: &ColumnManifestRootDescriptor{Name: "events/wrong-root"},
			},
			want: "column manifest root descriptor",
		},
		{
			name: "aggregate min missing group column",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "min_time_us", Column: "time_us", Kind: ColumnAggregateMin}},
			},
			want: "requires a group column",
		},
		{
			name: "aggregate min group column wrong type",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
					{Name: "did", Path: "did", ValueType: ColumnStoreValueInt64},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "min_time_us", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin}},
			},
			want: "group column",
		},
		{
			name: "aggregate count group column wrong type",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "did", Path: "did", ValueType: ColumnStoreValueInt64},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "count_did", GroupColumn: "did", Kind: ColumnAggregateCount}},
			},
			want: "group column",
		},
		{
			name: "aggregate count rejects value column",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "count_did", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateCount}},
			},
			want: "does not support a value column",
		},
		{
			name: "aggregate min value column wrong type",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueString},
					{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "min_time_us", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin}},
			},
			want: "value column",
		},
		{
			name: "aggregate group hour missing value column",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
					{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "feed_event_hour_count", GroupColumn: "collection", Kind: ColumnAggregateGroupHourCount}},
			},
			want: "requires a column",
		},
		{
			name: "aggregate group hour missing group column",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
					{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "feed_event_hour_count", Column: "time_us", Kind: ColumnAggregateGroupHourCount}},
			},
			want: "requires a group column",
		},
		{
			name: "aggregate group hour group column wrong type",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
					{Name: "collection", Path: "collection", ValueType: ColumnStoreValueInt64},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "feed_event_hour_count", Column: "time_us", GroupColumn: "collection", Kind: ColumnAggregateGroupHourCount}},
			},
			want: "group column",
		},
		{
			name: "aggregate group hour value column wrong type",
			cfg: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueString},
					{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString},
				},
				AggregateMetadata: []ColumnAggregateMetadata{{Name: "feed_event_hour_count", Column: "time_us", GroupColumn: "collection", Kind: ColumnAggregateGroupHourCount}},
			},
			want: "value column",
		},
		{
			name: "invalid asset namespace traversal",
			cfg: &ColumnStoreConfig{
				Enabled:      true,
				Columns:      []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				AssetManager: &ColumnAssetManagerConfig{Namespace: "events/../escape"},
			},
			want: "invalid column asset namespace",
		},
		{
			name: "invalid asset namespace windows volume",
			cfg: &ColumnStoreConfig{
				Enabled:      true,
				Columns:      []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				AssetManager: &ColumnAssetManagerConfig{Namespace: "C:events/column-assets"},
			},
			want: "invalid column asset namespace",
		},
		{
			name: "invalid asset namespace whitespace",
			cfg: &ColumnStoreConfig{
				Enabled:      true,
				Columns:      []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				AssetManager: &ColumnAssetManagerConfig{Namespace: " events/column-assets "},
			},
			want: "invalid column asset namespace",
		},
		{
			name: "invalid asset namespace nul",
			cfg: &ColumnStoreConfig{
				Enabled:      true,
				Columns:      []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64}},
				AssetManager: &ColumnAssetManagerConfig{Namespace: "events/\x00/column-assets"},
			},
			want: "invalid column asset namespace",
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

func TestColumnStoreAdjacencyFixedWidthEncodingNormalizes(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	if got := meta.Options.ColumnStore.Columns[0].FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
		t.Fatalf("adjacency fixed_width_encoding=%q want %q", got, ColumnFixedWidthEncodingLittleEndian)
	}
}

func TestColumnStoreAdjacencyOffsetsListLayoutNormalizesSpecOnly(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList, Owner: TypedStorageOwnerColumnPart, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	col := meta.Options.ColumnStore.Columns[0]
	if col.AdjacencyLayout != ColumnAdjacencyListLayoutUint32OffsetsList || col.AdjacencyDegree != 0 || col.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
		t.Fatalf("normalized adjacency offsets-list column=%+v", col)
	}

	layoutToggled := *meta.Options.ColumnStore
	layoutToggled.Columns = append([]ColumnStoreColumn(nil), meta.Options.ColumnStore.Columns...)
	layoutToggled.Columns[0].AdjacencyLayout = ColumnAdjacencyListLayoutFixedDense
	if toggledHash := hashColumnStoreSchema(&layoutToggled); meta.Options.ColumnStore.SchemaHash == toggledHash {
		t.Fatalf("schema hash did not include adjacency_layout selector: offsets=%x toggled=%x", meta.Options.ColumnStore.SchemaHash, toggledHash)
	}
}

func TestColumnStoreScalarFixedWidthEncodingRequiresTypedColumnPart(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "score_i64", Path: "score_i64", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		{Name: "score32", Path: "score32", ValueType: ColumnStoreValueFloat32, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		{Name: "score64", Path: "score64", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "scores", Options: CollectionOptions{ColumnStore: cfg}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	for i, col := range meta.Options.ColumnStore.Columns {
		if got := col.FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
			t.Fatalf("column[%d] fixed_width_encoding=%q want %q", i, got, ColumnFixedWidthEncodingLittleEndian)
		}
	}

	invalid := testColumnStoreConfig(nil)
	invalid.Columns = []ColumnStoreColumn{{Name: "score32", Path: "score32", ValueType: ColumnStoreValueFloat32, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}}
	invalid.SortKey = nil
	invalid.AggregateMetadata = nil
	if _, err := normalizeCollectionMeta(CollectionMeta{Name: "scores_invalid", Options: CollectionOptions{ColumnStore: invalid}}); err == nil || !strings.Contains(err.Error(), "requires owner") {
		t.Fatalf("normalizeCollectionMeta row-asset scalar fixed_width err=%v want owner rejection", err)
	}
}

func TestColumnStoreTypedColumnCompressionPolicyNormalizes2297(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta default: %v", err)
	}
	defaultCfg := meta.Options.ColumnStore
	if got := defaultCfg.TypedColumnCompression; got != ColumnStoreTypedColumnCompressionLZ4 {
		t.Fatalf("typed_column_compression=%q want %q", got, ColumnStoreTypedColumnCompressionLZ4)
	}
	if got := defaultCfg.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionZSTD {
		t.Fatalf("typed_column_section_compression=%q want %q", got, ColumnStoreTypedColumnCompressionZSTD)
	}

	defaultAlias := testColumnStoreConfig(nil)
	defaultAlias.TypedColumnCompression = ColumnStoreTypedColumnCompression(" default ")
	defaultAlias.TypedColumnSectionCompression = ColumnStoreTypedColumnCompression("default")
	defaultAliasMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: defaultAlias}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta default alias: %v", err)
	}
	if got := defaultAliasMeta.Options.ColumnStore.TypedColumnCompression; got != ColumnStoreTypedColumnCompressionLZ4 {
		t.Fatalf("alias typed_column_compression=%q want %q", got, ColumnStoreTypedColumnCompressionLZ4)
	}
	if got := defaultAliasMeta.Options.ColumnStore.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionZSTD {
		t.Fatalf("alias typed_column_section_compression=%q want %q", got, ColumnStoreTypedColumnCompressionZSTD)
	}
	if defaultAliasMeta.Options.ColumnStore.SchemaHash != defaultCfg.SchemaHash {
		t.Fatalf("schema hash should canonicalize default alias: alias=%x default=%x", defaultAliasMeta.Options.ColumnStore.SchemaHash, defaultCfg.SchemaHash)
	}

	snappy := testColumnStoreConfig(nil)
	snappy.TypedColumnCompression = ColumnStoreTypedColumnCompressionSnappy
	snappyMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: snappy}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta snappy: %v", err)
	}
	if got := snappyMeta.Options.ColumnStore.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionSnappy {
		t.Fatalf("section compression default=%q want typed compression %q", got, ColumnStoreTypedColumnCompressionSnappy)
	}

	none := testColumnStoreConfig(nil)
	none.TypedColumnCompression = ColumnStoreTypedColumnCompressionNone
	none.TypedColumnSectionCompression = ColumnStoreTypedColumnCompressionNone
	noneMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: none}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta none: %v", err)
	}
	if noneMeta.Options.ColumnStore.SchemaHash == defaultCfg.SchemaHash {
		t.Fatalf("schema hash did not include typed-column compression policy: default=%x none=%x", defaultCfg.SchemaHash, noneMeta.Options.ColumnStore.SchemaHash)
	}

	off := testColumnStoreConfig(nil)
	off.TypedColumnCompression = ColumnStoreTypedColumnCompression("off")
	off.TypedColumnSectionCompression = ColumnStoreTypedColumnCompression("compression_off")
	offMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: off}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta off alias: %v", err)
	}
	if got := offMeta.Options.ColumnStore.TypedColumnCompression; got != ColumnStoreTypedColumnCompressionNone {
		t.Fatalf("off alias typed_column_compression=%q want %q", got, ColumnStoreTypedColumnCompressionNone)
	}
	if got := offMeta.Options.ColumnStore.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionNone {
		t.Fatalf("off alias typed_column_section_compression=%q want %q", got, ColumnStoreTypedColumnCompressionNone)
	}
	if offMeta.Options.ColumnStore.SchemaHash != noneMeta.Options.ColumnStore.SchemaHash {
		t.Fatalf("schema hash should canonicalize none aliases: off=%x none=%x", offMeta.Options.ColumnStore.SchemaHash, noneMeta.Options.ColumnStore.SchemaHash)
	}

	zstd := testColumnStoreConfig(nil)
	zstd.TypedColumnCompression = ColumnStoreTypedColumnCompression("zstd")
	if _, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: zstd}}); !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "typed_column_compression zstd") {
		t.Fatalf("normalizeCollectionMeta zstd err=%v want benchmark-only unsupported typed_column_compression", err)
	}

	zstdSection := testColumnStoreConfig(nil)
	zstdSection.TypedColumnSectionCompression = ColumnStoreTypedColumnCompression("zstd")
	zstdSectionMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: zstdSection}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta zstd section: %v", err)
	}
	if got := zstdSectionMeta.Options.ColumnStore.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionZSTD {
		t.Fatalf("zstd typed_column_section_compression=%q want %q", got, ColumnStoreTypedColumnCompressionZSTD)
	}

	invalid := testColumnStoreConfig(nil)
	invalid.TypedColumnSectionCompression = ColumnStoreTypedColumnCompression("zstd_dict")
	if _, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: invalid}}); !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "typed_column_section_compression") {
		t.Fatalf("normalizeCollectionMeta invalid section compression err=%v want typed_column_section_compression unsupported", err)
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
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMax},
	)
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	if meta.Options.ColumnStore.SchemaHash == 0 {
		t.Fatal("schema hash was not populated")
	}

	alias := testColumnStoreConfig(nil)
	alias.Columns = append(alias.Columns,
		ColumnStoreColumn{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, ElementsPerRow: 128},
		ColumnStoreColumn{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
		ColumnStoreColumn{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList},
	)
	alias.SortKey = append(alias.SortKey, ColumnSortKey{Column: "embedding_inv_norm"})
	alias.AggregateMetadata = append(alias.AggregateMetadata,
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMax},
	)
	aliasMeta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: alias}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta alias: %v", err)
	}
	if aliasMeta.Options.ColumnStore.SchemaHash != meta.Options.ColumnStore.SchemaHash {
		t.Fatalf("schema hash should treat float32_vector elements_per_row as vector_dims alias: dims=%x alias=%x", meta.Options.ColumnStore.SchemaHash, aliasMeta.Options.ColumnStore.SchemaHash)
	}

	changed := testColumnStoreConfig(nil)
	changed.Columns = append(changed.Columns,
		ColumnStoreColumn{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 256},
		ColumnStoreColumn{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
		ColumnStoreColumn{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList},
	)
	changed.SortKey = append(changed.SortKey, ColumnSortKey{Column: "embedding_inv_norm"})
	changed.AggregateMetadata = append(changed.AggregateMetadata,
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMax},
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
		ColumnAggregateMetadata{Name: "min_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMin},
		ColumnAggregateMetadata{Name: "max_embedding_inv_norm", Column: "embedding_inv_norm", GroupColumn: "kind", Kind: ColumnAggregateMax},
	)
	changedMeta, err = normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: changed}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta changed scalar type: %v", err)
	}
	if changedMeta.Options.ColumnStore.SchemaHash == meta.Options.ColumnStore.SchemaHash {
		t.Fatalf("schema hash did not include scalar value type: %x", meta.Options.ColumnStore.SchemaHash)
	}
}

func TestColumnStoreProfileSupportMatrix(t *testing.T) {
	tests := []struct {
		name       string
		durability backenddb.DurabilityMode
		profile    ColumnStoreProfileSupport
		wantCreate string
		wantOpen   string
	}{
		{name: "durable default", durability: backenddb.DurabilityDurable},
		{name: "wal on relaxed default rejected", durability: backenddb.DurabilityWALOnRelaxed, wantCreate: "durable-only", wantOpen: "durable-only"},
		{name: "wal off relaxed default rejected", durability: backenddb.DurabilityWALOffRelaxed, wantCreate: "durable-only", wantOpen: "durable-only"},
		{name: "wal on relaxed benchmark allowed", durability: backenddb.DurabilityWALOnRelaxed, profile: ColumnStoreProfileBenchmarkRelaxed},
		{name: "wal off relaxed benchmark allowed", durability: backenddb.DurabilityWALOffRelaxed, profile: ColumnStoreProfileBenchmarkRelaxed},
	}
	for _, tt := range tests {
		wantProfile := tt.profile
		if wantProfile == "" {
			wantProfile = ColumnStoreProfileDurableOnly
		}
		t.Run(tt.name+"/create", func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: tt.durability})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()
			cfg := testColumnStoreConfig(nil)
			cfg.ProfileSupport = tt.profile
			_, err = NewCollectionManager(d).CreateCollection(&CollectionMeta{
				Name:    "events",
				Options: CollectionOptions{ColumnStore: cfg},
			})
			if tt.wantCreate != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantCreate) {
					t.Fatalf("CreateCollection err=%v want %q", err, tt.wantCreate)
				}
				return
			}
			if err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := NewCollectionManager(d).OpenCollection("events")
			if tt.wantOpen != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantOpen) {
					t.Fatalf("OpenCollection err=%v want %q", err, tt.wantOpen)
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			if got := col.Meta().Options.ColumnStore.ProfileSupport; got != wantProfile {
				t.Fatalf("ProfileSupport=%q want %q", got, wantProfile)
			}
		})
		t.Run(tt.name+"/open", func(t *testing.T) {
			dir := t.TempDir()
			createDB, err := backenddb.Open(backenddb.Options{Dir: dir})
			if err != nil {
				t.Fatalf("open durable create db: %v", err)
			}
			cfg := testColumnStoreConfig(nil)
			cfg.ProfileSupport = tt.profile
			if _, err := NewCollectionManager(createDB).CreateCollection(&CollectionMeta{
				Name:    "events",
				Options: CollectionOptions{ColumnStore: cfg},
			}); err != nil {
				t.Fatalf("durable CreateCollection: %v", err)
			}
			if err := createDB.Close(); err != nil {
				t.Fatalf("close durable create db: %v", err)
			}
			openDB, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: tt.durability})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = openDB.Close() }()
			col, err := NewCollectionManager(openDB).OpenCollection("events")
			if tt.wantOpen != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantOpen) {
					t.Fatalf("OpenCollection err=%v want %q", err, tt.wantOpen)
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			if got := col.Meta().Options.ColumnStore.ProfileSupport; got != wantProfile {
				t.Fatalf("ProfileSupport=%q want %q", got, wantProfile)
			}
		})
	}
}

func TestColumnStoreProfileSupportRejectsWriteDomainCacheOpen(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOnRelaxed})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "seed"}); err != nil {
		t.Fatalf("create seed collection: %v", err)
	}
	state := d.State()
	if state == nil || state.SystemRootPageID == 0 {
		t.Fatalf("unexpected db state: %+v", state)
	}
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)}})
	if err != nil {
		t.Fatalf("normalize column meta: %v", err)
	}
	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           meta,
		catalog:        &collectionCatalog{meta: meta, pager: d.Pager()},
		baseSystemRoot: state.SystemRootPageID,
		baseCommitSeq:  state.CommitSeq,
	}
	mgr.domainMu.Lock()
	mgr.domains = map[string]*collectionWriteDomain{"events": domain}
	mgr.domainMu.Unlock()

	_, err = mgr.OpenCollection("events")
	if err == nil || !strings.Contains(err.Error(), "durable-only") {
		t.Fatalf("OpenCollection cached err=%v want durable-only rejection", err)
	}
}

func TestColumnStoreDisabledCacheIdentityAllocatesZero(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{Name: "users"}}
	allocs := testing.AllocsPerRun(1000, func() {
		if id, ok := columnStoreCacheIdentity(catalog, 123, 456); ok {
			t.Fatalf("column-disabled collection returned cache identity: %+v", id)
		}
	})
	if allocs != 0 {
		t.Fatalf("disabled ColumnStoreCacheIdentity allocated %f times, want 0", allocs)
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
	b.Run("disabled_cache_identity", func(b *testing.B) {
		b.ReportAllocs()
		disabledCatalog := &collectionCatalog{meta: CollectionMeta{Name: "users"}}
		for i := 0; i < b.N; i++ {
			if id, ok := columnStoreCacheIdentity(disabledCatalog, 123, 456); ok {
				b.Fatalf("bad disabled identity: %+v", id)
			}
		}
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
	var recovery *ColumnManifestIdentity
	if active != nil {
		copied := *active
		recovery = &copied
	}
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "rows", Kind: ColumnAggregateCount},
			{Name: "min_time_us", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
			{Name: "max_time_us", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMax},
		},
		ActiveManifest:                active,
		RecoveryAuthoritativeManifest: recovery,
	}
	if active != nil {
		cfg.RecoveryAuthoritativeAppliedCommandLSN = 99
	}
	return cfg
}

func TestColumnStoreConfigEmptyIncludesPhysicalMutationPartsM13C(t *testing.T) {
	if columnStoreConfigEmpty(ColumnStoreConfig{PhysicalMutationParts: 1}) {
		t.Fatal("columnStoreConfigEmpty ignored physical mutation parts")
	}
	if _, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{PhysicalMutationParts: 1}); err == nil || !strings.Contains(err.Error(), "enabled=true") {
		t.Fatalf("normalize disabled physical mutation metadata err=%v want enabled=true rejection", err)
	}
	if parts, err := columnPublishPhysicalMutationParts(&ColumnStoreConfig{PhysicalMutationParts: ^uint64(0)}, ColumnPublishPlan{
		Operation:      ColumnPublishOperationUpdate,
		PreparedAssets: []ColumnPreparedAsset{{}},
	}); err == nil || parts != 0 || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("columnPublishPhysicalMutationParts parts=%d err=%v want overflow", parts, err)
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
	if got := cfg.RetainedPayloadEncoding; got != ColumnRetainedPayloadEncodingSemanticStreamV1 {
		t.Fatalf("retained payload encoding=%q want %q", got, ColumnRetainedPayloadEncodingSemanticStreamV1)
	}
	if got := cfg.Reconstruction; got != ColumnReconstructionRetainedPayloadAndColumns {
		t.Fatalf("reconstruction=%q want %q", got, ColumnReconstructionRetainedPayloadAndColumns)
	}
	if cfg.AssetManager == nil || cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped || !cfg.AssetManager.IsolatedNamespace || cfg.AssetManager.Namespace != "events/column-assets" {
		t.Fatalf("unexpected asset manager metadata: %+v", cfg.AssetManager)
	}
	if cfg.Locator == nil || cfg.Locator.Strategy != ColumnLocatorStrategySideIndex {
		t.Fatalf("unexpected locator metadata: %+v", cfg.Locator)
	}
	if cfg.ManifestRoot == nil || cfg.ManifestRoot.Name != "events/column/manifest" {
		t.Fatalf("unexpected manifest root descriptor: %+v", cfg.ManifestRoot)
	}
	if cfg.ProfileSupport != ColumnStoreProfileDurableOnly {
		t.Fatalf("unexpected profile support: %q", cfg.ProfileSupport)
	}
	if got := cfg.TypedColumnCompression; got != ColumnStoreTypedColumnCompressionLZ4 {
		t.Fatalf("unexpected typed_column_compression: %q", got)
	}
	if got := cfg.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionZSTD {
		t.Fatalf("unexpected typed_column_section_compression: %q", got)
	}
}

func publishColumnStoreCatalogForTest(tb testing.TB, d *backenddb.DB, meta CollectionMeta, rootIdentity ColumnManifestIdentity) uint64 {
	tb.Helper()
	return publishColumnStoreCatalogRawManifestRootForTest(tb, d, meta, true, encodeColumnManifestIdentityRecord(rootIdentity))
}

func publishColumnStoreCatalogRawManifestRootForTest(tb testing.TB, d *backenddb.DB, meta CollectionMeta, includeIdentityRecord bool, identityRecord []byte) uint64 {
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
	manifestIter := &systemTargetIterator{}
	if includeIdentityRecord {
		manifestIter.entries = []systemTargetEntry{{
			key:   []byte(columnManifestIdentityRecordKey),
			value: append([]byte(nil), identityRecord...),
		}}
	}
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
		tb.Fatalf("publish column manifest root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		tb.Fatalf("unexpected root IDs: %v", rootIDs)
	}
	return rootIDs[0]
}
