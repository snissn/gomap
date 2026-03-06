package collections

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

type perfMockDB struct {
	*atomicMockDB
	failGetAtRoot  bool
	failHasAtRoot  bool
	getAtRootCalls int
	hasAtRootCalls int
	getSystemCalls map[string]int
}

func newPerfMockDB() *perfMockDB {
	return &perfMockDB{
		atomicMockDB:   newAtomicMockDB(),
		getSystemCalls: make(map[string]int),
	}
}

func (d *perfMockDB) resetCounters() {
	d.getAtRootCalls = 0
	d.hasAtRootCalls = 0
	clear(d.getSystemCalls)
}

func (d *perfMockDB) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	d.getAtRootCalls++
	if d.failGetAtRoot {
		return nil, errors.New("unexpected GetAtRoot call")
	}
	return d.atomicMockDB.GetAtRoot(rootID, key)
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
