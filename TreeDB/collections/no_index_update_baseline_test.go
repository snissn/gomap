package collections

import (
	"bytes"
	"errors"
	"fmt"
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
	beforeStats := mgr.StatsSnapshot()

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
	afterStats := mgr.StatsSnapshot()
	if got, want := afterStats.PrimaryOnlyUpdateCalls-beforeStats.PrimaryOnlyUpdateCalls, uint64(1); got != want {
		t.Fatalf("primary-only update calls delta=%d want %d", got, want)
	}
	if got, want := afterStats.PrimaryOnlyMatched-beforeStats.PrimaryOnlyMatched, uint64(1); got != want {
		t.Fatalf("primary-only matched delta=%d want %d", got, want)
	}
	if got, want := afterStats.PrimaryOnlyModified-beforeStats.PrimaryOnlyModified, uint64(1); got != want {
		t.Fatalf("primary-only modified delta=%d want %d", got, want)
	}
	if got, want := afterStats.PrimaryOnlyRootPublishes-beforeStats.PrimaryOnlyRootPublishes, uint64(1); got != want {
		t.Fatalf("primary-only root publishes delta=%d want %d", got, want)
	}
	if got, want := afterStats.PrimaryOnlyCoalescedDocs-beforeStats.PrimaryOnlyCoalescedDocs, uint64(1); got != want {
		t.Fatalf("primary-only coalesced docs delta=%d want %d", got, want)
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

func TestCollectionNoIndexUpdateFlushesPendingInsertBeforeSynchronousPublish(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada","city":"hnl"}`)); err != nil {
		t.Fatalf("buffered insert: %v", err)
	}
	if got := collectionNoIndexPendingStateForTest(t, col).count; got != 1 {
		t.Fatalf("pending insert count=%d want 1", got)
	}
	beforeState := d.State()

	callbackErr := errors.New("unexpected current document")
	var unexpectedCurrent []byte
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"name":"ada","city":"hnl"}`)) {
			unexpectedCurrent = bytes.Clone(current)
			return nil, false, callbackErr
		}
		return []byte(`{"name":"ada","city":"sea"}`), true, nil
	})
	if unexpectedCurrent != nil {
		t.Fatalf("update callback current=%q want pending insert document", unexpectedCurrent)
	}
	if err != nil {
		t.Fatalf("update pending insert: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update pending insert matched/modified=%v/%v want true/true", matched, modified)
	}
	afterState := d.State()
	if got, want := afterState.CommitSeq-beforeState.CommitSeq, uint64(2); got != want {
		t.Fatalf("commit seq delta after insert flush + update publish=%d want %d", got, want)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after synchronous update=%+v want empty", state)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get reopened updated doc: %v", err)
	}
	if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopened updated doc=%q want %q", got, want)
	}
}

func TestCollectionNoIndexUpdateMissingAndUnchangedDoNotPublishOrQueue(t *testing.T) {
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

	beforeRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return current, false, nil
	})
	if err != nil {
		t.Fatalf("unchanged update: %v", err)
	}
	if !matched || modified {
		t.Fatalf("unchanged update matched/modified=%v/%v want true/false", matched, modified)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("unchanged update commit seq=%d want %d", got, beforeState.CommitSeq)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != beforeRoot {
		t.Fatalf("unchanged update primary root=%d want %d", got, beforeRoot)
	}
	afterUnchangedStats := mgr.StatsSnapshot()
	if got := afterUnchangedStats.PrimaryOnlyRootPublishes - beforeStats.PrimaryOnlyRootPublishes; got != 0 {
		t.Fatalf("unchanged update primary root publishes delta=%d want 0", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after unchanged update=%+v want empty", state)
	}

	callbackRan := false
	matched, modified, err = col.Update([]byte("missing"), func(current []byte) ([]byte, bool, error) {
		callbackRan = true
		return nil, false, fmt.Errorf("callback should not run, current=%q", current)
	})
	if err != nil {
		t.Fatalf("missing update: %v", err)
	}
	if callbackRan {
		t.Fatal("missing update callback ran")
	}
	if matched || modified {
		t.Fatalf("missing update matched/modified=%v/%v want false/false", matched, modified)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("missing update commit seq=%d want %d", got, beforeState.CommitSeq)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != beforeRoot {
		t.Fatalf("missing update primary root=%d want %d", got, beforeRoot)
	}
	afterMissingStats := mgr.StatsSnapshot()
	if got := afterMissingStats.PrimaryOnlyRootPublishes - beforeStats.PrimaryOnlyRootPublishes; got != 0 {
		t.Fatalf("missing update primary root publishes delta=%d want 0", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after missing update=%+v want empty", state)
	}
}

func TestCollectionNoIndexUpdateFlushAfterSyncPublishDoesNotRepublish(t *testing.T) {
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
	if _, _, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"name":"ada","city":"sea"}`), true, nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	beforeFlushState := d.State()
	beforeFlushRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	beforeFlushStats := mgr.StatsSnapshot()

	if err := col.Flush(); err != nil {
		t.Fatalf("flush after synchronous update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeFlushState.CommitSeq {
		t.Fatalf("flush after synchronous update commit seq=%d want %d", got, beforeFlushState.CommitSeq)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != beforeFlushRoot {
		t.Fatalf("flush after synchronous update primary root=%d want %d", got, beforeFlushRoot)
	}
	afterFlushStats := mgr.StatsSnapshot()
	if got := afterFlushStats.PrimaryOnlyRootPublishes - beforeFlushStats.PrimaryOnlyRootPublishes; got != 0 {
		t.Fatalf("flush after synchronous update primary root publishes delta=%d want 0", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after flush=%+v want empty", state)
	}
}

func TestCollectionNoIndexUpdateSynchronousVisibilityAcrossManagers(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	writerMgr := NewCollectionManager(d)
	if _, err := writerMgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := writerMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer collection: %v", err)
	}
	sameManagerReader, err := writerMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open same-manager reader collection: %v", err)
	}
	otherManagerReader, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open other-manager reader collection: %v", err)
	}
	if _, err := writer.Insert([]byte("u1"), []byte(`{"name":"ada","city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	if _, _, err := writer.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"name":"ada","city":"sea"}`), true, nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	for name, reader := range map[string]*Collection{
		"writer":               writer,
		"same-manager-reader":  sameManagerReader,
		"other-manager-reader": otherManagerReader,
	} {
		got, err := reader.Get([]byte("u1"))
		if err != nil {
			t.Fatalf("%s get updated doc: %v", name, err)
		}
		if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
			t.Fatalf("%s updated doc=%q want %q", name, got, want)
		}
	}
}

type collectionNoIndexPendingState struct {
	count             int
	tableLen          int
	indexedQueued     int
	indexedPublishing int
	indexedRootRuns   int
}

func collectionNoIndexPendingStateForTest(t *testing.T, col *Collection) collectionNoIndexPendingState {
	t.Helper()
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	state := collectionNoIndexPendingState{
		count:             col.writeDomain.count,
		indexedQueued:     len(col.writeDomain.indexedFlushUnits),
		indexedPublishing: len(col.writeDomain.indexedPublishingUnits),
		indexedRootRuns:   len(col.writeDomain.rootRuns),
	}
	if col.writeDomain.table != nil {
		state.tableLen = col.writeDomain.table.Len()
	}
	return state
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
