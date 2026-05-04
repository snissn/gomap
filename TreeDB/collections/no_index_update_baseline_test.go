package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionNoIndexUpdateStagesPrimaryOnlyAndFlushPublishes(t *testing.T) {
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
	afterUpdateState := d.State()
	if afterUpdateState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("staged no-index update advanced commit seq by %d want 0", afterUpdateState.CommitSeq-beforeState.CommitSeq)
	}
	afterUpdatePrimaryRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	if afterUpdatePrimaryRoot != beforePrimaryRoot {
		t.Fatalf("staged no-index update changed primary root from %d to %d before flush", beforePrimaryRoot, afterUpdatePrimaryRoot)
	}
	afterUpdateStats := mgr.StatsSnapshot()
	if got, want := afterUpdateStats.PrimaryOnlyUpdateCalls-beforeStats.PrimaryOnlyUpdateCalls, uint64(1); got != want {
		t.Fatalf("primary-only update calls delta=%d want %d", got, want)
	}
	if got, want := afterUpdateStats.PrimaryOnlyMatched-beforeStats.PrimaryOnlyMatched, uint64(1); got != want {
		t.Fatalf("primary-only matched delta=%d want %d", got, want)
	}
	if got, want := afterUpdateStats.PrimaryOnlyModified-beforeStats.PrimaryOnlyModified, uint64(1); got != want {
		t.Fatalf("primary-only modified delta=%d want %d", got, want)
	}
	if got, want := afterUpdateStats.PrimaryOnlyBufferedCalls-beforeStats.PrimaryOnlyBufferedCalls, uint64(1); got != want {
		t.Fatalf("primary-only buffered calls delta=%d want %d", got, want)
	}
	if got := afterUpdateStats.PrimaryOnlyRootPublishes - beforeStats.PrimaryOnlyRootPublishes; got != 0 {
		t.Fatalf("primary-only root publishes before flush delta=%d want 0", got)
	}
	if got := afterUpdateStats.PrimaryOnlyCoalescedDocs - beforeStats.PrimaryOnlyCoalescedDocs; got != 0 {
		t.Fatalf("primary-only coalesced docs before flush delta=%d want 0", got)
	}

	state := collectionNoIndexPendingStateForTest(t, col)
	if state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("no-index update queued indexed state=%+v, want indexed state zero", state)
	}
	if state.count != 1 || state.tableLen != 1 {
		t.Fatalf("no-index staged update pending state=%+v want count/tableLen 1", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get staged document: %v", err)
	}
	if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("staged document=%q want %q", got, want)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush staged update: %v", err)
	}
	afterFlushState := d.State()
	if afterFlushState.CommitSeq != beforeState.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d want 1", afterFlushState.CommitSeq-beforeState.CommitSeq)
	}
	afterFlushPrimaryRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	if afterFlushPrimaryRoot == beforePrimaryRoot {
		t.Fatalf("primary root stayed at %d after flushing staged update", afterFlushPrimaryRoot)
	}
	afterFlushStats := mgr.StatsSnapshot()
	if got, want := afterFlushStats.PrimaryOnlyRootPublishes-beforeStats.PrimaryOnlyRootPublishes, uint64(1); got != want {
		t.Fatalf("primary-only root publishes after flush delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyRootDeltaEntries-beforeStats.PrimaryOnlyRootDeltaEntries, uint64(1); got != want {
		t.Fatalf("primary-only root delta entries after flush delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyCoalescedDocs-beforeStats.PrimaryOnlyCoalescedDocs, uint64(1); got != want {
		t.Fatalf("primary-only coalesced docs delta=%d want %d", got, want)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after flush=%+v want empty", state)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated document: %v", err)
	}
	if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("updated document=%q want %q", got, want)
	}
}

func TestCollectionNoIndexUpdateReadsAndOverwritesPendingInsert(t *testing.T) {
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
	if got := afterState.CommitSeq - beforeState.CommitSeq; got != 0 {
		t.Fatalf("commit seq delta after staged insert update=%d want 0", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen < 1 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after staged insert update=%+v want one unique no-index row", state)
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
	if got, want := afterUnchangedStats.PrimaryOnlyUpdateCalls-beforeStats.PrimaryOnlyUpdateCalls, uint64(1); got != want {
		t.Fatalf("unchanged update primary-only update calls delta=%d want %d", got, want)
	}
	if got, want := afterUnchangedStats.PrimaryOnlyMatched-beforeStats.PrimaryOnlyMatched, uint64(1); got != want {
		t.Fatalf("unchanged update primary-only matched delta=%d want %d", got, want)
	}
	if got := afterUnchangedStats.PrimaryOnlyModified - beforeStats.PrimaryOnlyModified; got != 0 {
		t.Fatalf("unchanged update primary-only modified delta=%d want 0", got)
	}
	if got := afterUnchangedStats.PrimaryOnlyBufferedCalls - beforeStats.PrimaryOnlyBufferedCalls; got != 0 {
		t.Fatalf("unchanged update primary-only buffered calls delta=%d want 0", got)
	}
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
	if got, want := afterMissingStats.PrimaryOnlyUpdateCalls-afterUnchangedStats.PrimaryOnlyUpdateCalls, uint64(1); got != want {
		t.Fatalf("missing update primary-only update calls delta=%d want %d", got, want)
	}
	if got := afterMissingStats.PrimaryOnlyMatched - afterUnchangedStats.PrimaryOnlyMatched; got != 0 {
		t.Fatalf("missing update primary-only matched delta=%d want 0", got)
	}
	if got := afterMissingStats.PrimaryOnlyModified - afterUnchangedStats.PrimaryOnlyModified; got != 0 {
		t.Fatalf("missing update primary-only modified delta=%d want 0", got)
	}
	if got := afterMissingStats.PrimaryOnlyBufferedCalls - afterUnchangedStats.PrimaryOnlyBufferedCalls; got != 0 {
		t.Fatalf("missing update primary-only buffered calls delta=%d want 0", got)
	}
	if got := afterMissingStats.PrimaryOnlyRootPublishes - beforeStats.PrimaryOnlyRootPublishes; got != 0 {
		t.Fatalf("missing update primary root publishes delta=%d want 0", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after missing update=%+v want empty", state)
	}
}

func TestCollectionNoIndexUpdateFlushPublishesOnceThenNoOps(t *testing.T) {
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
		t.Fatalf("flush staged update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeFlushState.CommitSeq+1 {
		t.Fatalf("flush staged update commit seq=%d want %d", got, beforeFlushState.CommitSeq+1)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got == beforeFlushRoot {
		t.Fatalf("flush staged update primary root stayed at %d", got)
	}
	afterFlushStats := mgr.StatsSnapshot()
	if got, want := afterFlushStats.PrimaryOnlyRootPublishes-beforeFlushStats.PrimaryOnlyRootPublishes, uint64(1); got != want {
		t.Fatalf("flush staged update primary root publishes delta=%d want %d", got, want)
	}
	beforeSecondFlushState := d.State()
	beforeSecondFlushStats := mgr.StatsSnapshot()
	if err := col.Flush(); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeSecondFlushState.CommitSeq {
		t.Fatalf("second flush commit seq=%d want %d", got, beforeSecondFlushState.CommitSeq)
	}
	afterSecondFlushStats := mgr.StatsSnapshot()
	if got := afterSecondFlushStats.PrimaryOnlyRootPublishes - beforeSecondFlushStats.PrimaryOnlyRootPublishes; got != 0 {
		t.Fatalf("second flush primary root publishes delta=%d want 0", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after flush=%+v want empty", state)
	}
}

func TestCollectionNoIndexUpdateWriteBackVisibilityAcrossManagers(t *testing.T) {
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
		"writer":              writer,
		"same-manager-reader": sameManagerReader,
	} {
		got, err := reader.Get([]byte("u1"))
		if err != nil {
			t.Fatalf("%s get staged doc: %v", name, err)
		}
		if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
			t.Fatalf("%s staged doc=%q want %q", name, got, want)
		}
	}
	got, err := otherManagerReader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("other-manager get old doc before flush: %v", err)
	}
	if want := []byte(`{"name":"ada","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("other-manager doc before flush=%q want %q", got, want)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush writer: %v", err)
	}
	got, err = otherManagerReader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("other-manager get flushed doc: %v", err)
	}
	if want := []byte(`{"name":"ada","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("other-manager doc after flush=%q want %q", got, want)
	}
}

func TestCollectionNoIndexRepeatedSameIDUpdatesCoalesceAndPreserveOrder(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"count":0}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeStats := mgr.StatsSnapshot()

	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":0}`)) {
			return nil, false, fmt.Errorf("first callback current=%q want count 0", current)
		}
		return []byte(`{"count":1}`), true, nil
	})
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("first update matched/modified=%v/%v want true/true", matched, modified)
	}
	afterFirstStats := mgr.StatsSnapshot()
	if got, want := afterFirstStats.PendingDocuments, 1; got != want {
		t.Fatalf("pending docs after first same-id update=%d want %d", got, want)
	}
	if afterFirstStats.PendingBytes <= 0 {
		t.Fatalf("pending bytes after first same-id update=%d want positive", afterFirstStats.PendingBytes)
	}
	matched, modified, err = col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":1}`)) {
			return nil, false, fmt.Errorf("second callback current=%q want count 1", current)
		}
		return []byte(`{"count":2}`), true, nil
	})
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("second update matched/modified=%v/%v want true/true", matched, modified)
	}
	afterSecondStats := mgr.StatsSnapshot()
	if got, want := afterSecondStats.PendingDocuments, 1; got != want {
		t.Fatalf("pending docs after second same-id update=%d want %d", got, want)
	}
	if got, want := afterSecondStats.PendingBytes, afterFirstStats.PendingBytes; got != want {
		t.Fatalf("pending bytes after same-length overwrite=%d want current-table bytes %d", got, want)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen < 1 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after repeated updates=%+v want one unique no-index row", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get staged repeated update: %v", err)
	}
	if want := []byte(`{"count":2}`); !bytes.Equal(got, want) {
		t.Fatalf("staged repeated update=%q want %q", got, want)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush repeated updates: %v", err)
	}
	afterFlushStats := mgr.StatsSnapshot()
	if got, want := afterFlushStats.PrimaryOnlyBufferedCalls-beforeStats.PrimaryOnlyBufferedCalls, uint64(2); got != want {
		t.Fatalf("primary-only buffered calls delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyRootPublishes-beforeStats.PrimaryOnlyRootPublishes, uint64(1); got != want {
		t.Fatalf("primary-only root publishes delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyCoalescedDocs-beforeStats.PrimaryOnlyCoalescedDocs, uint64(2); got != want {
		t.Fatalf("primary-only coalesced docs delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyRootDeltaEntries-beforeStats.PrimaryOnlyRootDeltaEntries, uint64(1); got != want {
		t.Fatalf("primary-only root delta entries delta=%d want %d", got, want)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flushed repeated update: %v", err)
	}
	if want := []byte(`{"count":2}`); !bytes.Equal(got, want) {
		t.Fatalf("flushed repeated update=%q want %q", got, want)
	}
}

func TestCollectionNoIndexUpdateBatchReadsStagedPrimaryOnlyValue(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"count":0}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()
	if _, _, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":0}`)) {
			return nil, false, fmt.Errorf("direct update current=%q want count 0", current)
		}
		return []byte(`{"count":1}`), true, nil
	}); err != nil {
		t.Fatalf("stage direct update: %v", err)
	}

	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
			if !bytes.Equal(current, []byte(`{"count":1}`)) {
				return nil, false, fmt.Errorf("batch update current=%q want staged count 1", current)
			}
			return []byte(`{"count":2}`), true, nil
		}},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v want one matched/modified result", results)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("staged direct+batch updates commit seq=%d want %d", got, beforeState.CommitSeq)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen < 1 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after direct+batch updates=%+v want one unique no-index row", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get staged batch update: %v", err)
	}
	if want := []byte(`{"count":2}`); !bytes.Equal(got, want) {
		t.Fatalf("staged batch update=%q want %q", got, want)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush staged batch update: %v", err)
	}
	afterFlushStats := mgr.StatsSnapshot()
	if got, want := afterFlushStats.PrimaryOnlyUpdateCalls-beforeStats.PrimaryOnlyUpdateCalls, uint64(2); got != want {
		t.Fatalf("primary-only update calls delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyBufferedCalls-beforeStats.PrimaryOnlyBufferedCalls, uint64(2); got != want {
		t.Fatalf("primary-only buffered calls delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyRootPublishes-beforeStats.PrimaryOnlyRootPublishes, uint64(1); got != want {
		t.Fatalf("primary-only root publishes delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyRootDeltaEntries-beforeStats.PrimaryOnlyRootDeltaEntries, uint64(1); got != want {
		t.Fatalf("primary-only root delta entries delta=%d want %d", got, want)
	}
	if got, want := afterFlushStats.PrimaryOnlyCoalescedDocs-beforeStats.PrimaryOnlyCoalescedDocs, uint64(2); got != want {
		t.Fatalf("primary-only coalesced docs delta=%d want %d", got, want)
	}
}

func TestCollectionNoIndexUpdateBatchAfterUnrelatedCatalogChangePreservesStagedWrite(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"count":0}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	if _, _, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":0}`)) {
			return nil, false, fmt.Errorf("stage current=%q want count 0", current)
		}
		return []byte(`{"count":1}`), true, nil
	}); err != nil {
		t.Fatalf("stage update: %v", err)
	}
	stagedRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen != 1 {
		t.Fatalf("pending state after staged update=%+v want one no-index row", state)
	}

	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "other"}); err != nil {
		t.Fatalf("create unrelated collection: %v", err)
	}
	beforeBatchState := d.State()
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != stagedRoot {
		t.Fatalf("unrelated catalog change changed users primary root from %d to %d", stagedRoot, got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen != 1 {
		t.Fatalf("pending state after unrelated catalog change=%+v want one no-index row", state)
	}

	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
			if !bytes.Equal(current, []byte(`{"count":1}`)) {
				return nil, false, fmt.Errorf("batch current=%q want staged count 1", current)
			}
			return []byte(`{"count":2}`), true, nil
		}},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v want one matched/modified result", results)
	}
	if got := d.State().CommitSeq; got != beforeBatchState.CommitSeq {
		t.Fatalf("UpdateBatch after unrelated catalog change advanced commit seq from %d to %d", beforeBatchState.CommitSeq, got)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != stagedRoot {
		t.Fatalf("UpdateBatch after unrelated catalog change published primary root from %d to %d", stagedRoot, got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen != 1 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after UpdateBatch replan=%+v want one no-index row", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get staged batch update: %v", err)
	}
	if want := []byte(`{"count":2}`); !bytes.Equal(got, want) {
		t.Fatalf("staged batch document=%q want %q", got, want)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush staged batch update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeBatchState.CommitSeq+1 {
		t.Fatalf("flush after staged batch advanced commit seq to %d want %d", got, beforeBatchState.CommitSeq+1)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 {
		t.Fatalf("pending state after flush=%+v want empty", state)
	}
}

func TestCollectionNoIndexUpdateBatchGenerationConflictReplansWithoutFlush(t *testing.T) {
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
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"count":0}`), []byte(`{"count":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeState := d.State()
	beforeRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	if _, _, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":0}`)) {
			return nil, false, fmt.Errorf("stage u1 current=%q want count 0", current)
		}
		return []byte(`{"count":1}`), true, nil
	}); err != nil {
		t.Fatalf("stage u1: %v", err)
	}

	injected := false
	callbackCalls := 0
	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
			callbackCalls++
			if !bytes.Equal(current, []byte(`{"count":1}`)) {
				return nil, false, fmt.Errorf("batch current=%q want staged count 1", current)
			}
			if !injected {
				injected = true
				matched, modified, err := col.Update([]byte("u2"), func(current []byte) ([]byte, bool, error) {
					if !bytes.Equal(current, []byte(`{"count":0}`)) {
						return nil, false, fmt.Errorf("nested u2 current=%q want count 0", current)
					}
					return []byte(`{"count":1}`), true, nil
				})
				if err != nil {
					return nil, false, err
				}
				if !matched || !modified {
					return nil, false, fmt.Errorf("nested u2 matched/modified=%v/%v want true/true", matched, modified)
				}
			}
			return []byte(`{"count":2}`), true, nil
		}},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v want one matched/modified result", results)
	}
	if callbackCalls < 2 {
		t.Fatalf("batch callback calls=%d want retry after generation conflict", callbackCalls)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("generation-conflict retry advanced commit seq from %d to %d", beforeState.CommitSeq, got)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != beforeRoot {
		t.Fatalf("generation-conflict retry published primary root from %d to %d", beforeRoot, got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 2 || state.tableLen != 2 || state.indexedQueued != 0 || state.indexedPublishing != 0 || state.indexedRootRuns != 0 {
		t.Fatalf("pending state after generation-conflict retry=%+v want two no-index rows", state)
	}
	for _, tc := range []struct {
		id   []byte
		want []byte
	}{
		{id: []byte("u1"), want: []byte(`{"count":2}`)},
		{id: []byte("u2"), want: []byte(`{"count":1}`)},
	} {
		got, err := col.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s: %v", tc.id, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("document %s=%q want %q", tc.id, got, tc.want)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush staged generation-conflict updates: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq+1 {
		t.Fatalf("flush after generation-conflict retry advanced commit seq to %d want %d", got, beforeState.CommitSeq+1)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 {
		t.Fatalf("pending state after flush=%+v want empty", state)
	}
}

func TestCollectionNoIndexUpdateCreateIndexDrainsStagedUpdateBeforeBackfill(t *testing.T) {
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
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"city":"hnl","score":0}`), []byte(`{"city":"hnl","score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeState := d.State()
	beforeRoot := collectionNoIndexPrimaryRootIDForTest(t, d, "users")
	if _, _, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Contains(current, []byte(`"city":"hnl"`)) {
			return nil, false, fmt.Errorf("stage current=%q missing hnl", current)
		}
		return []byte(`{"city":"sea","score":1}`), true, nil
	}); err != nil {
		t.Fatalf("stage update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("staged update before CreateIndex advanced commit seq from %d to %d", beforeState.CommitSeq, got)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got != beforeRoot {
		t.Fatalf("staged update before CreateIndex changed primary root from %d to %d", beforeRoot, got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen != 1 {
		t.Fatalf("pending state before CreateIndex=%+v want one no-index row", state)
	}

	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if got := d.State().CommitSeq; got <= beforeState.CommitSeq {
		t.Fatalf("CreateIndex commit seq=%d want greater than %d", got, beforeState.CommitSeq)
	}
	if got := collectionNoIndexPrimaryRootIDForTest(t, d, "users"); got == beforeRoot {
		t.Fatalf("CreateIndex left primary root at %d after draining staged update", got)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 0 || state.tableLen != 0 {
		t.Fatalf("pending state after CreateIndex=%+v want empty", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1 after CreateIndex: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"city":"sea","score":1}`)) {
		t.Fatalf("u1 after CreateIndex=%q want staged value", got)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("sea city ids=%q want [u1]", seaIDs)
	}
	hnlIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find hnl city: %v", err)
	}
	if len(hnlIDs) != 1 || !bytes.Equal(hnlIDs[0], []byte("u2")) {
		t.Fatalf("hnl city ids=%q want [u2]", hnlIDs)
	}
}

func TestCollectionNoIndexUpdateCallbackErrorDoesNotDropPriorStagedUpdate(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"count":0}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	if _, _, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"count":1}`), true, nil
	}); err != nil {
		t.Fatalf("stage first update: %v", err)
	}
	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()
	sentinel := errors.New("callback failed")
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":1}`)) {
			return nil, false, fmt.Errorf("error callback current=%q want staged count 1", current)
		}
		return nil, false, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error callback err=%v want sentinel", err)
	}
	if matched || modified {
		t.Fatalf("error callback matched/modified=%v/%v want false/false", matched, modified)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("failed callback commit seq=%d want %d", got, beforeState.CommitSeq)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen < 1 {
		t.Fatalf("pending state after failed callback=%+v want prior staged row", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after failed callback: %v", err)
	}
	if want := []byte(`{"count":1}`); !bytes.Equal(got, want) {
		t.Fatalf("document after failed callback=%q want %q", got, want)
	}
	afterFailedStats := mgr.StatsSnapshot()
	if got := afterFailedStats.PrimaryOnlyUpdateCalls - beforeStats.PrimaryOnlyUpdateCalls; got != 0 {
		t.Fatalf("failed callback update calls delta=%d want 0", got)
	}
	if got := afterFailedStats.PrimaryOnlyBufferedCalls - beforeStats.PrimaryOnlyBufferedCalls; got != 0 {
		t.Fatalf("failed callback buffered calls delta=%d want 0", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush prior staged update: %v", err)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after flush: %v", err)
	}
	if want := []byte(`{"count":1}`); !bytes.Equal(got, want) {
		t.Fatalf("document after flush=%q want %q", got, want)
	}
}

func TestCollectionNoIndexUpdateCallbackPanicDoesNotDropPriorStagedUpdate(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"count":0}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	if _, _, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"count":1}`), true, nil
	}); err != nil {
		t.Fatalf("stage first update: %v", err)
	}
	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, []byte(`{"count":1}`)) {
			return nil, false, fmt.Errorf("panic callback current=%q want staged count 1", current)
		}
		panic("bad callback")
	})
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("bad callback")) {
		t.Fatalf("panic callback err=%v want bad callback error", err)
	}
	if matched || modified {
		t.Fatalf("panic callback matched/modified=%v/%v want false/false", matched, modified)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("panic callback commit seq=%d want %d", got, beforeState.CommitSeq)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen < 1 {
		t.Fatalf("pending state after panic callback=%+v want prior staged row", state)
	}
	afterPanicStats := mgr.StatsSnapshot()
	if got := afterPanicStats.PrimaryOnlyUpdateCalls - beforeStats.PrimaryOnlyUpdateCalls; got != 0 {
		t.Fatalf("panic callback update calls delta=%d want 0", got)
	}
	if got := afterPanicStats.PrimaryOnlyBufferedCalls - beforeStats.PrimaryOnlyBufferedCalls; got != 0 {
		t.Fatalf("panic callback buffered calls delta=%d want 0", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush prior staged update: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after flush: %v", err)
	}
	if want := []byte(`{"count":1}`); !bytes.Equal(got, want) {
		t.Fatalf("document after flush=%q want %q", got, want)
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
