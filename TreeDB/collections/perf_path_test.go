package collections

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

type perfMockDB struct {
	*atomicMockDB
	failGetAtRoot        bool
	failGetAtRootAppend  bool
	failHasAtRoot        bool
	failIteratorAtRoot   bool
	getAtRootCalls       int
	getAtRootRootIDs     []uint64
	getAtRootAppendCalls int
	getAtRootAppendRoots []uint64
	hasAtRootCalls       int
	hasPrefixAtRootCalls int
	iteratorAtRootCalls  int
	rootIteratorCalls    int
	rootBulkOpsCalls     int
	getSystemCalls       map[string]int
}

func newPerfMockDB() *perfMockDB {
	return &perfMockDB{
		atomicMockDB:   newAtomicMockDB(),
		getSystemCalls: make(map[string]int),
	}
}

func (d *perfMockDB) resetCounters() {
	d.getAtRootCalls = 0
	d.getAtRootRootIDs = d.getAtRootRootIDs[:0]
	d.getAtRootAppendCalls = 0
	d.getAtRootAppendRoots = d.getAtRootAppendRoots[:0]
	d.hasAtRootCalls = 0
	d.hasPrefixAtRootCalls = 0
	d.iteratorAtRootCalls = 0
	d.rootIteratorCalls = 0
	d.rootBulkOpsCalls = 0
	clear(d.getSystemCalls)
}

func (d *perfMockDB) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	d.getAtRootCalls++
	d.getAtRootRootIDs = append(d.getAtRootRootIDs, rootID)
	if d.failGetAtRoot {
		return nil, errors.New("unexpected GetAtRoot call")
	}
	return d.atomicMockDB.GetAtRoot(rootID, key)
}

func (d *perfMockDB) GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	d.getAtRootAppendCalls++
	d.getAtRootAppendRoots = append(d.getAtRootAppendRoots, rootID)
	if d.failGetAtRootAppend {
		return nil, errors.New("unexpected GetAtRootAppend call")
	}
	return d.atomicMockDB.GetAtRootAppend(rootID, key, dst)
}

func (d *perfMockDB) GetSystem(key []byte) ([]byte, error) {
	d.getSystemCalls[string(key)]++
	return d.atomicMockDB.GetSystem(key)
}

func (d *perfMockDB) HasAtRoot(rootID uint64, key []byte) (bool, error) {
	d.hasAtRootCalls++
	if d.failHasAtRoot {
		return false, errors.New("unexpected HasAtRoot call")
	}
	return d.atomicMockDB.HasAtRoot(rootID, key)
}

func (d *perfMockDB) HasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	d.hasPrefixAtRootCalls++
	return d.atomicMockDB.HasPrefixAtRoot(rootID, prefix)
}

func (d *perfMockDB) IteratorAtRoot(rootID uint64, start, end []byte) (systemIterator, error) {
	d.iteratorAtRootCalls++
	if d.failIteratorAtRoot {
		return nil, errors.New("unexpected IteratorAtRoot call")
	}
	return d.atomicMockDB.IteratorAtRoot(rootID, start, end)
}

func (d *perfMockDB) MutateRootsWithFormatOps(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootOps [][]batch.Entry, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	d.rootBulkOpsCalls++
	return d.atomicMockDB.MutateRootsWithFormatOps(sync, rootIDs, formats, rootOps, buildSystemOps)
}

func (d *perfMockDB) MutateRootsWithFormatIterators(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootIters []iterator.UnsafeIterator, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	d.rootIteratorCalls++
	return d.atomicMockDB.MutateRootsWithFormatIterators(sync, rootIDs, formats, rootIters, buildSystemOps)
}

func TestInsertWithoutIndexes_SkipsExistingDocumentRead(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	d.resetCounters()
	d.failGetAtRoot = true
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert without indexes: %v", err)
	}
	if d.getAtRootCalls != 0 {
		t.Fatalf("expected insert without indexes to skip GetAtRoot, got %d calls", d.getAtRootCalls)
	}
}

func TestDeleteWithoutIndexes_SkipsExistingDocumentRead(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	d.resetCounters()
	d.failGetAtRoot = true
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete without indexes: %v", err)
	}
	if d.getAtRootCalls != 0 {
		t.Fatalf("expected delete without indexes to skip GetAtRoot, got %d calls", d.getAtRootCalls)
	}
}

func TestPrimaryRootDescriptor_IsCachedWithinEpoch(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	rootKey, err := SystemCollectionRootKey(meta.PrimaryRoot)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}

	d.resetCounters()
	if _, err := col.Get([]byte("u1")); err != nil {
		t.Fatalf("first get: %v", err)
	}
	if _, err := col.Get([]byte("u1")); err != nil {
		t.Fatalf("second get: %v", err)
	}
	if got := d.getSystemCalls[string(rootKey)]; got != 1 {
		t.Fatalf("expected one primary descriptor load within epoch, got %d", got)
	}
}

func TestCreateIndex_OpenCollectionHandleStartsIndexing(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert after create index: %v", err)
	}
	ids, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index: %v", err)
	}
	if len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("unexpected ids: %#v", ids)
	}
}

func TestInsertWithIndexes_UsesHasBeforeLoadingExistingDocument(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	d.resetCounters()
	d.failGetAtRoot = true
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert with indexes: %v", err)
	}
	if d.hasAtRootCalls == 0 {
		t.Fatalf("expected indexed insert to probe existence with HasAtRoot")
	}
	if d.getAtRootCalls != 0 {
		t.Fatalf("expected indexed insert to skip GetAtRoot when document is absent, got %d calls", d.getAtRootCalls)
	}
}

func TestInsertWithUniqueIndexes_UsesPrefixProbeInsteadOfIteratorConflictScan(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	d.resetCounters()
	d.failIteratorAtRoot = true
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil {
		t.Fatalf("expected duplicate unique-key conflict")
	}
	if d.hasPrefixAtRootCalls == 0 {
		t.Fatalf("expected unique indexed insert to probe with HasPrefixAtRoot")
	}
	if d.iteratorAtRootCalls != 0 {
		t.Fatalf("expected unique indexed insert to avoid IteratorAtRoot, got %d calls", d.iteratorAtRootCalls)
	}
}

func TestInsertBatchWithIndexes_UsesIteratorPublishForWarmLargeBatches(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	initialIDs := make([][]byte, 8)
	initialDocs := make([][]byte, 8)
	for i := range initialIDs {
		initialIDs[i] = []byte("seed-" + string(rune('a'+i)))
		initialDocs[i] = []byte(`{"email":"seed-` + string(rune('a'+i)) + `@example.com","city":"seed"}`)
	}
	if _, err := col.InsertBatch(initialIDs, initialDocs); err != nil {
		t.Fatalf("seed insert batch: %v", err)
	}

	d.resetCounters()
	ids := make([][]byte, batchIteratorPublishMinDocuments)
	docs := make([][]byte, batchIteratorPublishMinDocuments)
	for i := range ids {
		ids[i] = []byte("warm-" + string(rune('a'+(i%26))) + string(rune('A'+((i/26)%26))))
		docs[i] = []byte(`{"email":"warm-` + string(rune('a'+(i%26))) + string(rune('A'+((i/26)%26))) + `@example.com","city":"hnl"}`)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("warm insert batch: %v", err)
	}
	if d.rootIteratorCalls != 1 {
		t.Fatalf("iterator publish calls=%d want 1", d.rootIteratorCalls)
	}
	if d.rootBulkOpsCalls != 0 {
		t.Fatalf("bulk ops calls=%d want 0", d.rootBulkOpsCalls)
	}
}

func TestInsertWithIndexes_LoadsExistingDocumentWhenPresent(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada-v1@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada-v2@example.com"}`)); err != nil {
		t.Fatalf("replace insert: %v", err)
	}
	oldIDs, err := col.FindByIndex("email_idx", "ada-v1@example.com")
	if err != nil {
		t.Fatalf("find old index: %v", err)
	}
	if len(oldIDs) != 0 {
		t.Fatalf("expected old index entry removed, got %#v", oldIDs)
	}
	newIDs, err := col.FindByIndex("email_idx", "ada-v2@example.com")
	if err != nil {
		t.Fatalf("find new index: %v", err)
	}
	if len(newIDs) != 1 || string(newIDs[0]) != "u1" {
		t.Fatalf("unexpected new ids: %#v", newIDs)
	}
}

func TestDeleteWithIndexes_UsesIndexStateInsteadOfPrimaryRead(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	primaryDesc, _, err := col.primaryRootDescriptor()
	if err != nil {
		t.Fatalf("primary root descriptor: %v", err)
	}
	stateDesc, _, err := col.indexStateRootDescriptor()
	if err != nil {
		t.Fatalf("state root descriptor: %v", err)
	}

	d.resetCounters()
	d.failGetAtRootAppend = true
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete with indexes: %v", err)
	}
	if d.getAtRootAppendCalls != 0 {
		t.Fatalf("expected indexed delete to avoid GetAtRootAppend, got %d calls", d.getAtRootAppendCalls)
	}
	for _, rootID := range d.getAtRootRootIDs {
		if rootID == primaryDesc.RootPageID {
			t.Fatalf("expected indexed delete to avoid primary root reads, got GetAtRoot on root %d", rootID)
		}
	}
	if len(d.getAtRootRootIDs) == 0 {
		t.Fatalf("expected indexed delete to read compact index state")
	}
	if d.getAtRootRootIDs[0] != stateDesc.RootPageID {
		t.Fatalf("expected indexed delete to read state root %d, got %d", stateDesc.RootPageID, d.getAtRootRootIDs[0])
	}
}

func TestCreateIndex_BackfillsStateForIndexedDeleteFastPath(t *testing.T) {
	d := newPerfMockDB()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	stateDesc, _, err := col.indexStateRootDescriptor()
	if err != nil {
		t.Fatalf("state root descriptor: %v", err)
	}

	d.resetCounters()
	d.failGetAtRootAppend = true
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete after backfill: %v", err)
	}
	if d.getAtRootAppendCalls != 0 {
		t.Fatalf("expected delete after backfill to avoid GetAtRootAppend, got %d calls", d.getAtRootAppendCalls)
	}
	foundStateRead := false
	for _, rootID := range d.getAtRootRootIDs {
		if rootID == stateDesc.RootPageID {
			foundStateRead = true
		}
	}
	if !foundStateRead {
		t.Fatalf("expected delete after backfill to read state root %d, got %v", stateDesc.RootPageID, d.getAtRootRootIDs)
	}
}
