package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionIndexedFlushGuardCounters(t *testing.T) {
	t.Run("publish_error_requeue", func(t *testing.T) {
		d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		mgr, col := collectionTestIndexedFlushCounterCollection(t, d)
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
		defer collectionTestCloseIndexedFlushWork(work)

		injectedErr := errors.New("injected publish failure")
		if err := col.completePreparedIndexedFlush(work, 0, nil, injectedErr, 0, 0, 0); !errors.Is(err, injectedErr) {
			t.Fatalf("complete failure err=%v want injected error", err)
		}
		stats := mgr.StatsSnapshot()
		if got := stats.IndexedFlushRequeues; got != 1 {
			t.Fatalf("indexed flush requeues=%d want 1", got)
		}
		if got := stats.IndexedFlushRequeuedUnits; got != 1 {
			t.Fatalf("indexed flush requeued units=%d want 1", got)
		}
		if got := mgr.Stats()["treedb.collections.write_domain.indexed_flush.requeues_total"]; got != "1" {
			t.Fatalf("requeue stat=%q want 1", got)
		}
	})

	t.Run("publish_error_lost_ownership", func(t *testing.T) {
		d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		mgr, col := collectionTestIndexedFlushCounterCollection(t, d)
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
		defer collectionTestCloseIndexedFlushWork(work)

		col.writeDomain.mu.Lock()
		currentPublishing := indexedFlushUnit{}
		col.writeDomain.indexedPublishingUnits = []indexedFlushUnit{currentPublishing}
		col.writeDomain.mu.Unlock()

		injectedErr := errors.New("injected publish failure")
		err = col.completePreparedIndexedFlush(work, 0, nil, injectedErr, 0, 0, 0)
		if !errors.Is(err, errIndexedFlushLostOwnership) {
			t.Fatalf("complete failure err=%v want lost ownership", err)
		}
		if !errors.Is(err, injectedErr) {
			t.Fatalf("complete failure err=%v want injected error", err)
		}
		col.writeDomain.mu.RLock()
		publishingUnits := append([]indexedFlushUnit(nil), col.writeDomain.indexedPublishingUnits...)
		queuedUnits := len(col.writeDomain.indexedFlushUnits)
		col.writeDomain.mu.RUnlock()
		if len(publishingUnits) != 1 || !sameIndexedFlushUnitTables(publishingUnits[0], currentPublishing) {
			t.Fatalf("publishing units after lost ownership publish error=%+v want current unit retained", publishingUnits)
		}
		if queuedUnits != 0 {
			t.Fatalf("queued units after lost ownership publish error=%d want 0", queuedUnits)
		}
		stats := mgr.StatsSnapshot()
		if got := stats.IndexedFlushLostOwnership; got != 1 {
			t.Fatalf("indexed flush lost ownership=%d want 1", got)
		}
		if got := stats.IndexedFlushRequeues; got != 0 {
			t.Fatalf("indexed flush requeues=%d want 0", got)
		}
		if got := mgr.Stats()["treedb.collections.write_domain.indexed_flush.lost_ownership_total"]; got != "1" {
			t.Fatalf("lost ownership stat=%q want 1", got)
		}
	})

	t.Run("lost_ownership", func(t *testing.T) {
		d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		mgr, col := collectionTestIndexedFlushCounterCollection(t, d)
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
		defer collectionTestCloseIndexedFlushWork(work)

		col.writeDomain.mu.Lock()
		col.writeDomain.indexedPublishingUnits = []indexedFlushUnit{{}}
		col.writeDomain.mu.Unlock()
		rootIDs := make([]uint64, len(work.rootNames))
		for i := range rootIDs {
			rootIDs[i] = uint64(1000 + i)
		}
		err = col.completePreparedIndexedFlush(work, 999, rootIDs, nil, 0, 0, 0)
		if !errors.Is(err, errIndexedFlushLostOwnership) {
			t.Fatalf("complete err=%v want lost ownership", err)
		}
		stats := mgr.StatsSnapshot()
		if got := stats.IndexedFlushLostOwnership; got != 1 {
			t.Fatalf("indexed flush lost ownership=%d want 1", got)
		}
		if got := mgr.Stats()["treedb.collections.write_domain.indexed_flush.lost_ownership_total"]; got != "1" {
			t.Fatalf("lost ownership stat=%q want 1", got)
		}
	})

	t.Run("root_base_mismatch", func(t *testing.T) {
		d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		leftMgr, left := collectionTestIndexedFlushCounterCollection(t, d)
		if _, err := left.InsertBatch(
			[][]byte{[]byte("u1")},
			[][]byte{[]byte(`{"email":"ada@example.com"}`)},
		); err != nil {
			t.Fatalf("stage left pending insert: %v", err)
		}

		right, err := NewCollectionManager(d).OpenCollection("users")
		if err != nil {
			t.Fatalf("open right collection: %v", err)
		}
		if _, err := right.InsertBatch(
			[][]byte{[]byte("u2")},
			[][]byte{[]byte(`{"email":"grace@example.com"}`)},
		); err != nil {
			t.Fatalf("insert right durable row: %v", err)
		}
		if err := right.Flush(); err != nil {
			t.Fatalf("flush right row: %v", err)
		}

		work, err := left.prepareIndexedAsyncPublish()
		if work != nil {
			collectionTestCloseIndexedFlushWork(work)
			t.Fatal("prepare returned work despite root-base mismatch")
		}
		if !errors.Is(err, ErrConcurrentMutation) {
			t.Fatalf("prepare err=%v want ErrConcurrentMutation", err)
		}
		stats := leftMgr.StatsSnapshot()
		if got := stats.IndexedFlushRootBaseMismatches; got != 1 {
			t.Fatalf("indexed flush root-base mismatches=%d want 1", got)
		}
		if got := leftMgr.Stats()["treedb.collections.write_domain.indexed_flush.root_base_mismatch_total"]; got != "1" {
			t.Fatalf("root-base mismatch stat=%q want 1", got)
		}
	})
}

func collectionTestIndexedFlushCounterCollection(tb testing.TB, d *backenddb.DB) (*CollectionManager, *Collection) {
	tb.Helper()
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
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		tb.Fatalf("open collection: %v", err)
	}
	return mgr, col
}

func collectionTestCloseIndexedFlushWork(work *indexedFlushPublishWork) {
	if work != nil && work.pin != nil {
		_ = work.pin.Close()
		work.pin = nil
	}
}
