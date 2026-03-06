package treedb

import (
	"bytes"
	"fmt"
	"testing"

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
