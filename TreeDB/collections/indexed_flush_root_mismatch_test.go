package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionIndexedAsyncPrepareRejectsRootBaseMismatchAndPreservesPendingVisibility(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	leftMgr := NewCollectionManager(d)
	if _, err := leftMgr.CreateCollection(&CollectionMeta{
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
	left, err := leftMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left collection: %v", err)
	}
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
		if work.pin != nil {
			_ = work.pin.Close()
		}
		t.Fatal("prepare returned work despite root-base mismatch")
	}
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("prepare err=%v want ErrConcurrentMutation", err)
	}

	got, err := left.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get pending left row after mismatch: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("pending left row=%q want %q", got, want)
	}
	ids, err := left.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find pending email after mismatch: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("pending email ids=%q want [u1]", ids)
	}
}

func TestCollectionIndexedAsyncPublishRootBaseMismatchRequeuesFIFOAndCounts(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	leftMgr := NewCollectionManager(d)
	if _, err := leftMgr.CreateCollection(&CollectionMeta{
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
	left, err := leftMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left collection: %v", err)
	}
	if _, err := left.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com"}`)},
	); err != nil {
		t.Fatalf("stage left active pending insert: %v", err)
	}
	work, err := left.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare left async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare left async publish returned nil work")
	}
	defer collectionTestCloseIndexedFlushWork(work)

	if _, err := left.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"b@example.com"}`)},
	); err != nil {
		t.Fatalf("stage later queued insert: %v", err)
	}
	left.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(left.writeDomain) {
		left.writeDomain.mu.Unlock()
		t.Fatal("rotate later queued insert returned false")
	}
	if got := len(left.writeDomain.indexedPublishingUnits); got != 1 {
		left.writeDomain.mu.Unlock()
		t.Fatalf("publishing units before mismatch=%d want 1", got)
	}
	if got := len(left.writeDomain.indexedFlushUnits); got != 1 {
		left.writeDomain.mu.Unlock()
		t.Fatalf("queued units before mismatch=%d want 1", got)
	}
	left.writeDomain.mu.Unlock()

	right, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open right collection: %v", err)
	}
	if _, err := right.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"c@example.com"}`)},
	); err != nil {
		t.Fatalf("insert right durable row: %v", err)
	}
	if err := right.Flush(); err != nil {
		t.Fatalf("flush right row: %v", err)
	}

	if err := left.publishPreparedIndexedFlush(work); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("publish prepared left err=%v want ErrConcurrentMutation", err)
	}

	prefixA := indexedFlushRequeueEmailPrefix(t, "a@example.com")
	prefixB := indexedFlushRequeueEmailPrefix(t, "b@example.com")
	left.writeDomain.mu.RLock()
	publishingUnits := len(left.writeDomain.indexedPublishingUnits)
	queuedUnits := append([]indexedFlushUnit(nil), left.writeDomain.indexedFlushUnits...)
	pending := left.writeDomain.uniqueValueIndex["email"]
	containsA := pending != nil && pending.contains(prefixA)
	containsB := pending != nil && pending.contains(prefixB)
	left.writeDomain.mu.RUnlock()
	if publishingUnits != 0 {
		t.Fatalf("publishing units after mismatch=%d want 0", publishingUnits)
	}
	if got, want := len(queuedUnits), 2; got != want {
		t.Fatalf("queued units after mismatch=%d want %d", got, want)
	}
	if !indexedFlushRequeueUnitHasUniquePrefix(queuedUnits[0], "email", prefixA) {
		t.Fatal("first requeued unit does not contain original active unique reservation")
	}
	if !indexedFlushRequeueUnitHasUniquePrefix(queuedUnits[1], "email", prefixB) {
		t.Fatal("second queued unit does not contain later queued unique reservation")
	}
	if !containsA || !containsB {
		t.Fatalf("pending unique reservations after mismatch contain a=%v b=%v want true/true", containsA, containsB)
	}
	stats := leftMgr.StatsSnapshot()
	if got := stats.IndexedFlushRootBaseMismatches; got != 1 {
		t.Fatalf("indexed flush root-base mismatches=%d want 1", got)
	}
	if got := stats.IndexedFlushRequeues; got != 1 {
		t.Fatalf("indexed flush requeues=%d want 1", got)
	}
	if got := stats.IndexedFlushRequeuedUnits; got != 1 {
		t.Fatalf("indexed flush requeued units=%d want 1", got)
	}
}
