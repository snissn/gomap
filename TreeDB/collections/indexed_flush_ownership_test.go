package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionIndexedAsyncPublishLostOwnershipDoesNotRemoveCurrentPublishingUnit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

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
		t.Fatalf("insert buffered batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	if work.pin != nil {
		defer func() { _ = work.pin.Close() }()
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com"}`)},
	); err != nil {
		t.Fatalf("insert replacement publishing batch: %v", err)
	}

	col.writeDomain.mu.Lock()
	rotateIndexedMutableToFlushUnitLocked(col.writeDomain)
	if len(col.writeDomain.indexedFlushUnits) != 1 {
		t.Fatalf("replacement queued units=%d want 1", len(col.writeDomain.indexedFlushUnits))
	}
	currentPublishing := col.writeDomain.indexedFlushUnits[0]
	col.writeDomain.indexedPublishingUnits = []indexedFlushUnit{currentPublishing}
	col.writeDomain.indexedFlushUnits = nil
	rebuildBufferedPendingIndexesLocked(col.writeDomain, "users", true)
	col.writeDomain.mu.Unlock()
	t.Cleanup(func() {
		col.writeDomain.mu.Lock()
		publishingUnits := col.writeDomain.indexedPublishingUnits
		col.writeDomain.indexedPublishingUnits = nil
		col.writeDomain.mu.Unlock()
		resetIndexedFlushUnits(publishingUnits)
	})

	rootIDs := make([]uint64, len(work.rootNames))
	for i := range rootIDs {
		rootIDs[i] = uint64(1000 + i)
	}
	err = col.completePreparedIndexedFlush(work, 999, rootIDs, nil, 0, 0, 0)
	if !errors.Is(err, errIndexedFlushLostOwnership) {
		t.Fatalf("complete lost ownership err=%v want lost ownership", err)
	}

	col.writeDomain.mu.RLock()
	publishingUnits := append([]indexedFlushUnit(nil), col.writeDomain.indexedPublishingUnits...)
	col.writeDomain.mu.RUnlock()
	if len(publishingUnits) != 1 || !sameIndexedFlushUnitTables(publishingUnits[0], currentPublishing) {
		t.Fatalf("publishing units after lost ownership=%+v want original current unit retained", publishingUnits)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get retained publishing document: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("retained publishing document=%q want %q", got, want)
	}
	ids, err := col.FindByIndexValue("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find retained publishing email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("retained publishing email ids=%q want [u2]", ids)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"grace@example.com"}`)},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate retained publishing unique insert err=%v want ErrUniqueIndexConflict", err)
	}
	stats := mgr.StatsSnapshot()
	if got := stats.IndexedFlushErrors; got != 1 {
		t.Fatalf("indexed flush errors=%d want 1", got)
	}
	if got := stats.IndexedFlushCalls; got != 1 {
		t.Fatalf("indexed flush calls=%d want 1", got)
	}
	if got := stats.IndexedFlushLostOwnership; got != 1 {
		t.Fatalf("indexed flush lost ownership=%d want 1", got)
	}
}
