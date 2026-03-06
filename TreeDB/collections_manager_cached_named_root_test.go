package treedb

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCachedCollectionsInsert_PrimaryNamedRootBufferedUntilCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint collection create: %v", err)
	}

	rootName, err := collections.CollectionPrimaryRootName(meta.Name)
	if err != nil {
		t.Fatalf("primary root name: %v", err)
	}
	beforeInsert := loadBackendRootDescriptor(t, d, rootName)
	if beforeInsert.RootPageID != 0 {
		t.Fatalf("expected empty primary root before insert, got %d", beforeInsert.RootPageID)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get from cached handle: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"name":"ada"}`)) {
		t.Fatalf("cached get = %q", got)
	}

	reopened, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("reopen collection through cached view: %v", err)
	}
	reopenedDoc, err := reopened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reopened get: %v", err)
	}
	if !bytes.Equal(reopenedDoc, []byte(`{"name":"ada"}`)) {
		t.Fatalf("reopened get = %q", reopenedDoc)
	}

	stillPersisted := loadBackendRootDescriptor(t, d, rootName)
	if stillPersisted.RootPageID != beforeInsert.RootPageID {
		t.Fatalf("backend primary root changed before checkpoint: got %d want %d", stillPersisted.RootPageID, beforeInsert.RootPageID)
	}
	rawBackendDoc, err := d.backend.GetAtRoot(stillPersisted.RootPageID, []byte("u1"))
	if err != nil {
		t.Fatalf("backend get before checkpoint: %v", err)
	}
	if len(rawBackendDoc) != 0 {
		t.Fatalf("expected backend primary root to remain unchanged before checkpoint")
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}

	afterInsert := loadBackendRootDescriptor(t, d, rootName)
	if afterInsert.RootPageID == 0 {
		t.Fatalf("expected primary root publish after checkpoint")
	}
	persistedDoc, err := d.backend.GetAtRoot(afterInsert.RootPageID, []byte("u1"))
	if err != nil {
		t.Fatalf("backend get after checkpoint: %v", err)
	}
	if !bytes.Equal(persistedDoc, []byte(`{"name":"ada"}`)) {
		t.Fatalf("persisted doc = %q", persistedDoc)
	}
}

func TestCachedCollectionsInsert_SecondaryRootsBufferedUntilCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	primaryRootName, err := collections.CollectionPrimaryRootName(meta.Name)
	if err != nil {
		t.Fatalf("primary root name: %v", err)
	}
	stateRootName, err := collections.CollectionIndexStateRootName(meta.Name)
	if err != nil {
		t.Fatalf("state root name: %v", err)
	}
	indexRootName, err := collections.CollectionIndexRootName(meta.Name, "email_idx")
	if err != nil {
		t.Fatalf("index root name: %v", err)
	}
	primaryBefore := loadBackendRootDescriptor(t, d, primaryRootName)
	stateBefore := loadBackendRootDescriptor(t, d, stateRootName)
	indexBefore := loadBackendRootDescriptor(t, d, indexRootName)

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ids, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index from cached handle: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("cached ids = %#v", ids)
	}

	reopened, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("reopen collection through cached view: %v", err)
	}
	reopenedIDs, err := reopened.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("reopened find by index: %v", err)
	}
	if len(reopenedIDs) != 1 || !bytes.Equal(reopenedIDs[0], []byte("u1")) {
		t.Fatalf("reopened ids = %#v", reopenedIDs)
	}

	assertBackendDocMissingAtRoot(t, d, primaryBefore.RootPageID, []byte("u1"))
	assertBackendDocMissingAtRoot(t, d, stateBefore.RootPageID, []byte("u1"))
	if got := countBackendRootEntries(t, d, indexBefore.RootPageID); got != 0 {
		t.Fatalf("expected backend secondary root to remain unchanged before checkpoint, got %d entries", got)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}

	primaryAfter := loadBackendRootDescriptor(t, d, primaryRootName)
	stateAfter := loadBackendRootDescriptor(t, d, stateRootName)
	indexAfter := loadBackendRootDescriptor(t, d, indexRootName)
	assertBackendDocAtRoot(t, d, primaryAfter.RootPageID, []byte("u1"), []byte(`{"email":"ada@example.com"}`))
	stateValue, err := d.backend.GetAtRoot(stateAfter.RootPageID, []byte("u1"))
	if err != nil {
		t.Fatalf("backend state root get after checkpoint: %v", err)
	}
	if len(stateValue) == 0 {
		t.Fatalf("expected backend index-state entry after checkpoint")
	}
	if got := countBackendRootEntries(t, d, indexAfter.RootPageID); got != 1 {
		t.Fatalf("expected one backend secondary entry after checkpoint, got %d", got)
	}
}

func TestCachedCollectionsUniqueConflict_BufferedWithoutOverlayIterator(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	bufferedIteratorCalls := 0
	restore := setNamedRootBufferedIteratorTestHook(func(rootID uint64, start, end []byte) {
		bufferedIteratorCalls++
	})
	defer restore()

	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil {
		t.Fatalf("expected duplicate unique-key conflict")
	}
	if bufferedIteratorCalls != 0 {
		t.Fatalf("expected cached duplicate unique conflict to avoid bufferedIteratorAtRoot, got %d calls", bufferedIteratorCalls)
	}
}

func TestCachedCollectionsUniqueConflict_BufferedWithoutOverlayPrefixScan(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	prefixScans := 0
	restore := setNamedRootOverlayPrefixScanTestHook(func(rootID uint64, prefix []byte) {
		prefixScans++
	})
	defer restore()

	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil {
		t.Fatalf("expected duplicate unique-key conflict")
	}
	if prefixScans != 0 {
		t.Fatalf("expected cached duplicate unique conflict to avoid overlay prefix scans, got %d calls", prefixScans)
	}
}

func TestCachedCollectionsPrimaryGet_BufferedWithoutOverlayEntryPointLookup(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint collection create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	overlayEntryReads := 0
	restore := setNamedRootOverlayEntryReadTestHook(func(op string, rootID uint64, key []byte) {
		overlayEntryReads++
	})
	defer restore()

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get before checkpoint: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"name":"ada"}`)) {
		t.Fatalf("get before checkpoint = %q", got)
	}
	if overlayEntryReads != 0 {
		t.Fatalf("expected buffered primary get to avoid overlay entry lookups, got %d", overlayEntryReads)
	}
}

func TestCachedCollectionsExistingInsert_BufferedWithoutOverlayEntryPointLookup(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	overlayEntryReads := 0
	restore := setNamedRootOverlayEntryReadTestHook(func(op string, rootID uint64, key []byte) {
		overlayEntryReads++
	})
	defer restore()

	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"sea"}`)); err != nil {
		t.Fatalf("upsert before checkpoint: %v", err)
	}
	if overlayEntryReads != 0 {
		t.Fatalf("expected buffered existing insert to avoid overlay entry lookups, got %d", overlayEntryReads)
	}
}

func TestCachedCollectionsUniqueUpsertSameDocument_SameValueNoConflictBeforeCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"sea"}`)); err != nil {
		t.Fatalf("same-document upsert before checkpoint: %v", err)
	}
}

func TestCachedCollectionsDeleteThenReuseUniqueValue_BeforeCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete before checkpoint: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("reuse unique value before checkpoint: %v", err)
	}
}

func TestCachedCollectionsDeleteThenReuseUniqueValue_WithoutOverlayPrefixScan(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete before checkpoint: %v", err)
	}

	prefixScans := 0
	restore := setNamedRootOverlayPrefixScanTestHook(func(rootID uint64, prefix []byte) {
		prefixScans++
	})
	defer restore()

	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("reuse unique value before checkpoint: %v", err)
	}
	if prefixScans != 0 {
		t.Fatalf("expected cached delete/reuse unique path to avoid overlay prefix scans, got %d calls", prefixScans)
	}
}

func TestCachedCollectionsBufferedNamedRoots_KeepMemtableOnlyState(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	primaryState := requireBufferedNamedRootStateByName(t, d, mustCollectionPrimaryRootName(t, meta.Name))
	if !primaryState.HasDomain {
		t.Fatalf("expected primary buffered root to use root-domain state")
	}
	if primaryState.LegacyEntryCount != 0 {
		t.Fatalf("expected primary buffered root to avoid legacy entry map, got %d entries", primaryState.LegacyEntryCount)
	}

	indexState := requireBufferedNamedRootStateByName(t, d, mustCollectionIndexStateRootName(t, meta.Name))
	if !indexState.HasDomain {
		t.Fatalf("expected index-state buffered root to use root-domain state")
	}
	if indexState.LegacyEntryCount != 0 {
		t.Fatalf("expected index-state buffered root to avoid legacy entry map, got %d entries", indexState.LegacyEntryCount)
	}

	secondaryState := requireBufferedNamedRootStateByName(t, d, mustCollectionIndexRootName(t, meta.Name, "email_idx"))
	if !secondaryState.HasDomain {
		t.Fatalf("expected secondary buffered root to use root-domain state")
	}
	if secondaryState.LegacyEntryCount != 0 {
		t.Fatalf("expected secondary buffered root to avoid legacy entry map, got %d entries", secondaryState.LegacyEntryCount)
	}
}

func TestCachedCollectionsBufferedNamedRoots_LiveInCachingLayer(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if d.cached == nil {
		t.Fatalf("expected cached db")
	}
	if !d.cached.PendingNamedRoots() {
		t.Fatalf("expected cached layer to hold pending named roots before checkpoint")
	}
	if len(d.namedRootsByID) != 0 || len(d.namedRootsByKey) != 0 {
		t.Fatalf("expected public db named-root overlay state to stay empty once cached-native buffering is active")
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if d.cached.PendingNamedRoots() {
		t.Fatalf("expected cached named-root buffers to flush on checkpoint")
	}
}

func TestCachedCollectionsBufferedNamedRoots_InsertUsesSingleMemtableWritePerRoot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	writes := 0
	restore := setNamedRootMemtableWriteTestHook(func(rootID uint64, kind string, key []byte) {
		writes++
	})
	defer restore()

	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if writes != 3 {
		t.Fatalf("expected one memtable write per buffered named root, got %d", writes)
	}
}

func TestCachedCollectionsBufferedNamedRoots_DeleteUsesSingleMemtableWritePerRoot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	writes := 0
	restore := setNamedRootMemtableWriteTestHook(func(rootID uint64, kind string, key []byte) {
		writes++
	})
	defer restore()

	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if writes != 3 {
		t.Fatalf("expected one memtable write per buffered named root delete, got %d", writes)
	}
}

func TestCachedCollectionsInsert_UsesOwnedStagingForDerivedWrites(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	ownedWrites := 0
	restore := setNamedRootOwnedWriteTestHook(func(kind string, key []byte) {
		ownedWrites++
	})
	defer restore()

	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if ownedWrites == 0 {
		t.Fatalf("expected cached insert to use owned staging writes for derived bytes")
	}
}

func TestCachedCollectionsInsert_UsesRootBulkMutationOps(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	bulkCalls := 0
	restore := setRootBulkMutationOpsTestHook(func(rootCount int) {
		bulkCalls++
	})
	defer restore()

	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if bulkCalls == 0 {
		t.Fatalf("expected cached insert to use root bulk mutation ops")
	}
}

func TestCachedCollectionsFindByIndex_BufferedWithoutLegacyIteratorMaterialization(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	legacyMaterializations := 0
	restore := setNamedRootLegacyIteratorMaterializeTestHook(func(rootID uint64, start, end []byte) {
		legacyMaterializations++
	})
	defer restore()

	ids, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index before checkpoint: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids before checkpoint = %#v", ids)
	}
	if legacyMaterializations != 0 {
		t.Fatalf("expected buffered find-by-index to avoid legacy iterator materialization, got %d calls", legacyMaterializations)
	}
}

func TestCachedCollectionsCheckpoint_BufferedWithoutLegacyFlushSnapshot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	legacySnapshots := 0
	restore := setNamedRootLegacyFlushSnapshotTestHook(func(rootID uint64) {
		legacySnapshots++
	})
	defer restore()

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}
	if legacySnapshots != 0 {
		t.Fatalf("expected checkpoint flush to avoid legacy entry snapshots, got %d calls", legacySnapshots)
	}
}

func TestCachedCollectionsCheckpoint_UsesRootBulkMutationOps(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	bulkPublishCalls := 0
	restore := setNamedRootBulkPublishOpsTestHook(func(rootCount int) {
		bulkPublishCalls++
	})
	defer restore()

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}
	if bulkPublishCalls == 0 {
		t.Fatalf("expected checkpoint to publish named roots through bulk ops")
	}
}

func TestCachedCollectionsCheckpoint_UsesRootMutationTables(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tablePublishCalls := 0
	restore := setNamedRootBulkPublishTablesTestHook(func(rootCount int) {
		tablePublishCalls++
		if rootCount != 3 {
			t.Fatalf("root count=%d want 3", rootCount)
		}
	})
	defer restore()

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}
	if tablePublishCalls == 0 {
		t.Fatalf("expected checkpoint to publish named roots through tables")
	}
}

func TestCachedCollectionsCheckpoint_BulkPublishRefreshesValueLogState(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := []byte(`{"email":"ada@example.com","payload":"` + strings.Repeat("x", 2048) + `"}`)
	if _, err := col.Insert([]byte("u1"), doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after bulk checkpoint publish: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("doc after checkpoint = %q", got)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete after bulk checkpoint publish: %v", err)
	}
}

func TestCachedCollectionsDelete_NamedRootTombstonesBufferedUntilCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed insert: %v", err)
	}

	primaryRootName, err := collections.CollectionPrimaryRootName(meta.Name)
	if err != nil {
		t.Fatalf("primary root name: %v", err)
	}
	stateRootName, err := collections.CollectionIndexStateRootName(meta.Name)
	if err != nil {
		t.Fatalf("state root name: %v", err)
	}
	indexRootName, err := collections.CollectionIndexRootName(meta.Name, "email_idx")
	if err != nil {
		t.Fatalf("index root name: %v", err)
	}
	primaryBeforeDelete := loadBackendRootDescriptor(t, d, primaryRootName)
	stateBeforeDelete := loadBackendRootDescriptor(t, d, stateRootName)
	indexBeforeDelete := loadBackendRootDescriptor(t, d, indexRootName)

	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after delete from cached handle: %v", err)
	}
	if got != nil {
		t.Fatalf("expected cached delete to hide document, got %q", got)
	}
	ids, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index after delete from cached handle: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected cached delete to hide secondary entry, got %#v", ids)
	}

	reopened, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("reopen collection through cached view: %v", err)
	}
	reopenedDoc, err := reopened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reopened get after delete: %v", err)
	}
	if reopenedDoc != nil {
		t.Fatalf("expected reopened cached view to hide deleted doc, got %q", reopenedDoc)
	}

	assertBackendDocAtRoot(t, d, primaryBeforeDelete.RootPageID, []byte("u1"), []byte(`{"email":"ada@example.com"}`))
	stateValue, err := d.backend.GetAtRoot(stateBeforeDelete.RootPageID, []byte("u1"))
	if err != nil {
		t.Fatalf("backend state root before checkpoint: %v", err)
	}
	if len(stateValue) == 0 {
		t.Fatalf("expected backend index-state entry to remain before checkpoint")
	}
	if got := countBackendRootEntries(t, d, indexBeforeDelete.RootPageID); got != 1 {
		t.Fatalf("expected backend secondary entry to remain before checkpoint, got %d", got)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint delete: %v", err)
	}

	primaryAfterDelete := loadBackendRootDescriptor(t, d, primaryRootName)
	stateAfterDelete := loadBackendRootDescriptor(t, d, stateRootName)
	indexAfterDelete := loadBackendRootDescriptor(t, d, indexRootName)
	assertBackendDocMissingAtRoot(t, d, primaryAfterDelete.RootPageID, []byte("u1"))
	assertBackendDocMissingAtRoot(t, d, stateAfterDelete.RootPageID, []byte("u1"))
	if got := countBackendRootEntries(t, d, indexAfterDelete.RootPageID); got != 0 {
		t.Fatalf("expected backend secondary entry to be removed after checkpoint, got %d", got)
	}
}

func TestCachedCollectionsCheckpoint_SameHandleReadsPublishedNamedRoots(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	const docCount = 1024
	emails := make([]string, docCount)
	for i := 0; i < docCount; i++ {
		emails[i] = fmt.Sprintf("user-%d@example.com", i)
		if _, err := col.Insert([]byte(fmt.Sprintf("u-%d", i)), []byte(fmt.Sprintf(`{"email":"%s"}`, emails[i]))); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint insert: %v", err)
	}

	got, err := col.Get([]byte("u-0"))
	if err != nil {
		t.Fatalf("get after checkpoint on same handle: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"email":"user-0@example.com"}`)) {
		t.Fatalf("doc after checkpoint on same handle = %q", got)
	}

	for i := 0; i < 100000; i++ {
		ids, err := col.FindByIndex("email_idx", emails[i%docCount])
		if err != nil {
			t.Fatalf("find by index after checkpoint on same handle (iteration %d): %v", i, err)
		}
		wantID := []byte(fmt.Sprintf("u-%d", i%docCount))
		if len(ids) != 1 || !bytes.Equal(ids[0], wantID) {
			t.Fatalf("ids after checkpoint on same handle (iteration %d) = %#v", i, ids)
		}
	}
}

func TestCachedCollectionsTriggerFlush_PublishesBufferedNamedRootsWithoutCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	primaryRootName := mustCollectionPrimaryRootName(t, meta.Name)
	primaryBefore := loadBackendRootDescriptor(t, d, primaryRootName)

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if !d.cached.PendingNamedRoots() {
		t.Fatalf("expected pending named roots before trigger flush")
	}

	d.cached.TriggerFlush()
	waitForCachedFlushCondition(t, "named roots trigger flush", func() bool {
		return !d.cached.PendingNamedRoots()
	})

	primaryAfter := loadBackendRootDescriptor(t, d, primaryRootName)
	if primaryAfter.RootPageID == 0 || primaryAfter.RootPageID == primaryBefore.RootPageID {
		t.Fatalf("expected trigger flush to publish new primary root, before=%d after=%d", primaryBefore.RootPageID, primaryAfter.RootPageID)
	}
	assertBackendDocAtRoot(t, d, primaryAfter.RootPageID, []byte("u1"), []byte(`{"email":"ada@example.com"}`))
}

func TestCachedCollectionsBufferedNamedRoots_AutoFlushOnThreshold(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 128})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := []byte(`{"email":"ada@example.com","payload":"` + strings.Repeat("x", 1024) + `"}`)
	if _, err := col.Insert([]byte("u1"), doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	waitForCachedFlushCondition(t, "named roots auto flush", func() bool {
		return !d.cached.PendingNamedRoots()
	})

	primaryRootName := mustCollectionPrimaryRootName(t, meta.Name)
	primaryAfter := loadBackendRootDescriptor(t, d, primaryRootName)
	if primaryAfter.RootPageID == 0 {
		t.Fatalf("expected auto flush to publish primary root")
	}
	assertBackendDocAtRoot(t, d, primaryAfter.RootPageID, []byte("u1"), doc)
}

func TestCachedCollectionsGetAtRoot_ResolvesFlushedVirtualRootID(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	primaryRootKey, err := collections.SystemCollectionRootKey(meta.PrimaryRoot)
	if err != nil {
		t.Fatalf("primary root key: %v", err)
	}
	rawDesc, err := d.GetSystem(primaryRootKey)
	if err != nil {
		t.Fatalf("cached get primary descriptor: %v", err)
	}
	var desc collections.CollectionRootDescriptor
	if err := desc.Decode(rawDesc); err != nil {
		t.Fatalf("decode primary descriptor: %v", err)
	}
	virtualRootID := desc.RootPageID

	d.cached.TriggerFlush()
	waitForCachedFlushCondition(t, "named roots trigger flush", func() bool {
		return !d.cached.PendingNamedRoots()
	})

	got, err := d.GetAtRoot(virtualRootID, []byte("u1"))
	if err != nil {
		t.Fatalf("get at flushed virtual root: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"name":"ada"}`)) {
		t.Fatalf("get at flushed virtual root = %q", got)
	}
}

func loadBackendRootDescriptor(t *testing.T, d *DB, rootName string) collections.CollectionRootDescriptor {
	t.Helper()
	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key for %s: %v", rootName, err)
	}
	raw, err := d.backend.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("backend get system %s: %v", rootName, err)
	}
	if len(raw) == 0 {
		t.Fatalf("missing backend root descriptor for %s", rootName)
	}
	var desc collections.CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor %s: %v", rootName, err)
	}
	return desc
}

func assertBackendDocMissingAtRoot(t *testing.T, d *DB, rootID uint64, key []byte) {
	t.Helper()
	got, err := d.backend.GetAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("backend get root %d: %v", rootID, err)
	}
	if len(got) != 0 {
		t.Fatalf("expected backend root %d key %q to be absent, got %q", rootID, key, got)
	}
}

func assertBackendDocAtRoot(t *testing.T, d *DB, rootID uint64, key, want []byte) {
	t.Helper()
	got, err := d.backend.GetAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("backend get root %d: %v", rootID, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("backend root %d key %q = %q, want %q", rootID, key, got, want)
	}
}

func countBackendRootEntries(t *testing.T, d *DB, rootID uint64) int {
	t.Helper()
	it, err := d.backend.IteratorAtRoot(rootID, nil, nil)
	if err != nil {
		t.Fatalf("backend iterator root %d: %v", rootID, err)
	}
	defer it.Close()
	count := 0
	for it.Valid() {
		if !it.IsDeleted() {
			count++
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("backend iterator root %d error: %v", rootID, err)
	}
	return count
}

func requireBufferedNamedRootStateByName(t *testing.T, d *DB, rootName string) caching.NamedRootDebugState {
	t.Helper()
	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key for %s: %v", rootName, err)
	}
	raw, err := d.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("cached get system %s: %v", rootName, err)
	}
	if len(raw) == 0 {
		t.Fatalf("missing cached root descriptor for %s", rootName)
	}
	var desc collections.CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode cached root descriptor %s: %v", rootName, err)
	}
	if d.cached == nil {
		t.Fatalf("cached db missing for root %s", rootName)
	}
	state, ok := d.cached.DebugNamedRootStateByID(desc.RootPageID)
	if !ok {
		t.Fatalf("named root state %s: missing", rootName)
	}
	return state
}

func mustCollectionPrimaryRootName(t *testing.T, collection string) string {
	t.Helper()
	rootName, err := collections.CollectionPrimaryRootName(collection)
	if err != nil {
		t.Fatalf("primary root name for %s: %v", collection, err)
	}
	return rootName
}

func mustCollectionIndexStateRootName(t *testing.T, collection string) string {
	t.Helper()
	rootName, err := collections.CollectionIndexStateRootName(collection)
	if err != nil {
		t.Fatalf("index-state root name for %s: %v", collection, err)
	}
	return rootName
}

func mustCollectionIndexRootName(t *testing.T, collection, indexName string) string {
	t.Helper()
	rootName, err := collections.CollectionIndexRootName(collection, indexName)
	if err != nil {
		t.Fatalf("index root name for %s/%s: %v", collection, indexName, err)
	}
	return rootName
}

func waitForCachedFlushCondition(t *testing.T, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
