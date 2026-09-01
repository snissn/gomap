package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionIndexedAsyncPublishFailureRequeuesUnitsAndPreservesUniqueReservations(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
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
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert first buffered batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	defer collectionTestCloseIndexedFlushWork(work)
	const transientRoot = "test:transient"
	work.flushUnit.rootRuns[transientRoot] = nil
	work.flushUnit.rootPolicies[transientRoot] = 0
	work.flushUnit.rootBaseIDs[transientRoot] = 1
	work.flushUnit.uniqueValueRuns[transientRoot] = nil
	col.writeDomain.mu.RLock()
	claimed := col.writeDomain.indexedPublishingUnits[0]
	_, rootRunsShared := claimed.rootRuns[transientRoot]
	_, rootPoliciesShared := claimed.rootPolicies[transientRoot]
	_, rootBaseIDsShared := claimed.rootBaseIDs[transientRoot]
	_, uniqueRunsShared := claimed.uniqueValueRuns[transientRoot]
	col.writeDomain.mu.RUnlock()
	if rootRunsShared || rootPoliciesShared || rootBaseIDsShared || uniqueRunsShared {
		t.Fatal("prepared publication shares mutable maps with claimed FIFO unit")
	}
	delete(work.flushUnit.rootRuns, transientRoot)
	delete(work.flushUnit.rootPolicies, transientRoot)
	delete(work.flushUnit.rootBaseIDs, transientRoot)
	delete(work.flushUnit.uniqueValueRuns, transientRoot)

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"b@example.com","city":"sea"}`)},
	); err != nil {
		t.Fatalf("insert second buffered batch while first publishes: %v", err)
	}
	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
		col.writeDomain.mu.Unlock()
		t.Fatal("rotate second buffered batch returned false")
	}
	if got := len(col.writeDomain.indexedPublishingUnits); got != 1 {
		col.writeDomain.mu.Unlock()
		t.Fatalf("publishing units before failure=%d want 1", got)
	}
	if got := len(col.writeDomain.indexedFlushUnits); got != 1 {
		col.writeDomain.mu.Unlock()
		t.Fatalf("queued units before failure=%d want 1", got)
	}
	col.writeDomain.mu.Unlock()

	prefixA := indexedFlushRequeueEmailPrefix(t, "a@example.com")
	prefixB := indexedFlushRequeueEmailPrefix(t, "b@example.com")
	injectedErr := errors.New("injected publish failure")
	if err := col.completePreparedIndexedFlush(work, 0, nil, injectedErr, 0, 0, 0); !errors.Is(err, injectedErr) {
		t.Fatalf("complete failure err=%v want injected publish failure", err)
	}

	col.writeDomain.mu.RLock()
	publishingUnits := len(col.writeDomain.indexedPublishingUnits)
	queuedUnits := append([]indexedFlushUnit(nil), col.writeDomain.indexedFlushUnits...)
	pending := col.writeDomain.uniqueValueIndex["email"]
	containsA := pending != nil && pending.contains(prefixA)
	containsB := pending != nil && pending.contains(prefixB)
	col.writeDomain.mu.RUnlock()
	if publishingUnits != 0 {
		t.Fatalf("publishing units after failure=%d want 0", publishingUnits)
	}
	if got, want := len(queuedUnits), 2; got != want {
		t.Fatalf("queued units after failure=%d want %d", got, want)
	}
	if !indexedFlushRequeueUnitHasUniquePrefix(queuedUnits[0], "email", prefixA) {
		t.Fatal("first requeued unit does not contain original active unique reservation")
	}
	if !indexedFlushRequeueUnitHasUniquePrefix(queuedUnits[1], "email", prefixB) {
		t.Fatal("second queued unit does not contain later queued unique reservation")
	}
	if !containsA || !containsB {
		t.Fatalf("pending unique reservations after failure contain a=%v b=%v want true/true", containsA, containsB)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("dupe")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"duplicate"}`)},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate active/requeued email err=%v want ErrUniqueIndexConflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("dupe2")},
		[][]byte{[]byte(`{"email":"b@example.com","city":"duplicate"}`)},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate queued email err=%v want ErrUniqueIndexConflict", err)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush requeued units: %v", err)
	}
	indexedFlushRequeueRequireIndexIDs(t, col, "email", "a@example.com", "u1")
	indexedFlushRequeueRequireIndexIDs(t, col, "email", "b@example.com", "u2")
}

func indexedFlushRequeueEmailPrefix(tb testing.TB, email string) []byte {
	tb.Helper()
	encoded, err := encodeIndexScalar(IndexValueString, email)
	if err != nil {
		tb.Fatalf("encode email %q: %v", email, err)
	}
	_, prefix, err := appendIndexValuePrefixSlice(nil, encoded)
	if err != nil {
		tb.Fatalf("email prefix %q: %v", email, err)
	}
	return prefix
}

func indexedFlushRequeueUnitHasUniquePrefix(unit indexedFlushUnit, indexName string, prefix []byte) bool {
	pending := rebuildBufferedUniqueValueIndexes(unit.uniqueValueRuns)[indexName]
	return pending != nil && pending.contains(prefix)
}

func indexedFlushRequeueRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range want {
		if !bytes.Equal(ids[i], []byte(want[i])) {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
