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
	first := columnManifestIdentityRecordKeyBytes()
	second := columnManifestIdentityRecordKeyBytes()
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
	if fresh := columnManifestIdentityRecordKeyBytes(); string(fresh) != columnManifestIdentityRecordKey {
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
	}{
		{name: "missing identity record", want: errColumnManifestIdentityMissing},
		{name: "short identity record", includeRecord: true, record: shortRecord, want: errColumnManifestIdentityMalformed},
		{name: "bad identity magic", includeRecord: true, record: badMagic, want: errColumnManifestIdentityBadMagic},
		{name: "unsupported identity record version", includeRecord: true, record: unsupportedRecordVersion, want: errColumnManifestIdentityUnsupportedVersion},
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
		})
	}
}

func TestColumnManifestIdentityRecordRejectsNonZeroReserved(t *testing.T) {
	identity := ColumnManifestIdentity{Generation: 42, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	record := encodeColumnManifestIdentityRecord(identity)
	record[columnManifestIdentityReservedOffset+columnManifestIdentityReservedSize-1] = 1
	decoded, err := decodeColumnManifestIdentityRecord(record)
	if !errors.Is(err, errColumnManifestIdentityNonZeroReserved) || !strings.Contains(err.Error(), "0x00000001") {
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
		catalog:        &collectionCatalog{meta: meta},
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
			{Name: "min_time_us", Column: "time_us", Kind: ColumnAggregateMin},
			{Name: "max_time_us", Column: "time_us", Kind: ColumnAggregateMax},
		},
		ActiveManifest:                active,
		RecoveryAuthoritativeManifest: recovery,
	}
	if active != nil {
		cfg.RecoveryAuthoritativeAppliedCommandLSN = 99
	}
	return cfg
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
	if cfg.ManifestRoot == nil || cfg.ManifestRoot.Name != "events/column/manifest" {
		t.Fatalf("unexpected manifest root descriptor: %+v", cfg.ManifestRoot)
	}
	if cfg.ProfileSupport != ColumnStoreProfileDurableOnly {
		t.Fatalf("unexpected profile support: %q", cfg.ProfileSupport)
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
