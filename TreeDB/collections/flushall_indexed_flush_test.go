package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionManagerFlushAllDrainsQueuedIndexedFlushUnit(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert buffered indexed batch: %v", err)
	}

	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
		col.writeDomain.mu.Unlock()
		t.Fatal("rotate indexed mutable state returned false")
	}
	col.writeDomain.mu.Unlock()
	if got := mgr.StatsSnapshot().PendingIndexedFlushUnits; got != 1 {
		t.Fatalf("pending indexed flush units before FlushAll=%d want 1", got)
	}

	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	stats := mgr.StatsSnapshot()
	if got := stats.PendingIndexedFlushUnits; got != 0 {
		t.Fatalf("pending indexed flush units after FlushAll=%d want 0", got)
	}
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("pending documents after FlushAll=%d want 0", got)
	}
	ids, err := col.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find flushed email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("flushed email ids=%q want [u1]", ids)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err = reopenedCol.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find reopened email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopened email ids=%q want [u1]", ids)
	}
}
