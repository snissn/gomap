package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionNoIndexUpdatePublishesSynchronouslyNotQueued(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada","city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeState := d.State()
	beforePrimaryRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")

	callbackErr := errors.New("unexpected current document")
	var unexpectedCurrent []byte
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Contains(current, []byte(`"city":"hnl"`)) {
			unexpectedCurrent = bytes.Clone(current)
			return nil, false, callbackErr
		}
		return []byte(`{"name":"ada","city":"sea"}`), true, nil
	})
	if unexpectedCurrent != nil {
		t.Fatalf("current document=%s missing city hnl", unexpectedCurrent)
	}
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched/modified=%v/%v want true/true", matched, modified)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq+1 {
		t.Fatalf("no-index update advanced commit seq by %d want 1", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterPrimaryRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	if afterPrimaryRoot == beforePrimaryRoot {
		t.Fatalf("primary root stayed at %d after synchronous no-index update", afterPrimaryRoot)
	}

	col.writeDomain.mu.RLock()
	queuedIndexedUnits := len(col.writeDomain.indexedFlushUnits)
	publishingIndexedUnits := len(col.writeDomain.indexedPublishingUnits)
	indexedRootRuns := len(col.writeDomain.rootRuns)
	noIndexBufferedCount := col.writeDomain.count
	noIndexTable := col.writeDomain.table
	noIndexTableLen := 0
	if noIndexTable != nil {
		noIndexTableLen = noIndexTable.Len()
	}
	col.writeDomain.mu.RUnlock()
	if queuedIndexedUnits != 0 || publishingIndexedUnits != 0 || indexedRootRuns != 0 {
		t.Fatalf("no-index update queued indexed state queued=%d publishing=%d rootRuns=%d, want all zero", queuedIndexedUnits, publishingIndexedUnits, indexedRootRuns)
	}
	if noIndexBufferedCount != 0 || noIndexTableLen != 0 {
		t.Fatalf("no-index update left collection-local buffer count=%d tableLen=%d, want drained", noIndexBufferedCount, noIndexTableLen)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated document: %v", err)
	}
	if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("updated document=%q want %q", got, want)
	}
}

func collectionNoIndexPrimaryRootIDForTest(t *testing.T, d *backenddb.DB, collectionName string) uint64 {
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
	rootID := catalog.rootID(collectionPrimaryRootName(collectionName))
	if rootID == 0 {
		t.Fatalf("primary root for %q was not persisted", collectionName)
	}
	return rootID
}
