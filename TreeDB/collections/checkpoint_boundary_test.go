package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionCheckpointDoesNotFlushPendingNoIndexInsert(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	beforeRoot := collectionCheckpointBoundaryPrimaryRootIDForTest(t, d, "users")
	if got := collectionCheckpointBoundaryPendingCountForTest(t, col); got != 1 {
		t.Fatalf("pending count before checkpoint=%d want 1", got)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	afterCheckpointRoot := collectionCheckpointBoundaryPrimaryRootIDForTest(t, d, "users")
	if afterCheckpointRoot != beforeRoot {
		t.Fatalf("checkpoint changed primary root from %d to %d for pending collection-local insert", beforeRoot, afterCheckpointRoot)
	}
	if got := collectionCheckpointBoundaryPendingCountForTest(t, col); got != 1 {
		t.Fatalf("pending count after checkpoint=%d want 1", got)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get pending document after checkpoint: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("pending document after checkpoint=%q want %q", got, want)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	afterFlushRoot := collectionCheckpointBoundaryPrimaryRootIDForTest(t, d, "users")
	if afterFlushRoot == beforeRoot {
		t.Fatalf("flush left primary root at %d, want published root descriptor", afterFlushRoot)
	}
	if got := collectionCheckpointBoundaryPendingCountForTest(t, col); got != 0 {
		t.Fatalf("pending count after flush=%d want 0", got)
	}
}

func collectionCheckpointBoundaryPrimaryRootIDForTest(t *testing.T, d *backenddb.DB, collectionName string) uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	return catalog.rootID(collectionPrimaryRootName(collectionName))
}

func collectionCheckpointBoundaryPendingCountForTest(t *testing.T, col *Collection) int {
	t.Helper()
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	return col.writeDomain.count
}
