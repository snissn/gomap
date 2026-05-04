package collections

import (
	"strings"
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

	col.writeDomain.mu.Lock()
	col.writeDomain.indexedPublishingUnits = []indexedFlushUnit{{}}
	col.writeDomain.mu.Unlock()

	rootIDs := make([]uint64, len(work.rootNames))
	for i := range rootIDs {
		rootIDs[i] = uint64(1000 + i)
	}
	err = col.completePreparedIndexedFlush(work, 999, rootIDs, nil, 0, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "lost ownership") {
		t.Fatalf("complete lost ownership err=%v want lost ownership", err)
	}

	col.writeDomain.mu.RLock()
	publishingUnits := append([]indexedFlushUnit(nil), col.writeDomain.indexedPublishingUnits...)
	col.writeDomain.mu.RUnlock()
	if len(publishingUnits) != 1 || !sameIndexedFlushUnitTables(publishingUnits[0], indexedFlushUnit{}) {
		t.Fatalf("publishing units after lost ownership=%+v want original current unit retained", publishingUnits)
	}
	stats := mgr.StatsSnapshot()
	if got := stats.IndexedFlushErrors; got != 1 {
		t.Fatalf("indexed flush errors=%d want 1", got)
	}
	if got := stats.IndexedFlushCalls; got != 1 {
		t.Fatalf("indexed flush calls=%d want 1", got)
	}
}
