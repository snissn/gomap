package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionErrorsAreClassifiable(t *testing.T) {
	if !errors.Is(ErrCollectionNotFound, ErrCollectionNotFound) {
		t.Fatal("ErrCollectionNotFound should be errors.Is-compatible")
	}
	for _, err := range []error{
		ErrDocumentExists,
		ErrDuplicateDocumentID,
		fmt.Errorf("wrapped: %w", ErrUniqueIndexConflict),
	} {
		if !IsDuplicateKeyError(err) {
			t.Fatalf("IsDuplicateKeyError(%v)=false want true", err)
		}
	}
	if IsDuplicateKeyError(ErrCollectionNotFound) {
		t.Fatal("ErrCollectionNotFound classified as duplicate key")
	}
}

func TestCollectionGetNilDBReturnsError(t *testing.T) {
	col := &Collection{}
	_, err := col.Get([]byte("u1"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("Get nil db error=%v", err)
	}
}

func TestCollectionInsertBatchBridge_RoundTripWithSecondaryIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if got, want := meta.Name, "users"; got != want {
		t.Fatalf("collection name=%q want %q", got, want)
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if len(ids) != 2 || !bytes.Equal(ids[0], []byte("u2")) || !bytes.Equal(ids[1], []byte("u1")) {
		t.Fatalf("result ids=%q", ids)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}

	emailIDs, err := col.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u2")) {
		t.Fatalf("email ids=%q want u2", emailIDs)
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 2 || !bytes.Equal(cityIDs[0], []byte("u1")) || !bytes.Equal(cityIDs[1], []byte("u2")) {
		t.Fatalf("city ids=%q want [u1 u2]", cityIDs)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := catalog.rootID(rootName); got == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
	}
}

func TestCollectionInsertBatchStatsExposeIndexRunShape(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	stats := col.LastInsertStats()
	if got, want := stats.Documents, 2; got != want {
		t.Fatalf("stats documents=%d want %d", got, want)
	}
	if got, want := stats.Indexes, 2; got != want {
		t.Fatalf("stats indexes=%d want %d", got, want)
	}
	if got, minRuns, maxRuns := stats.Runs, 2+len(stats.SecondaryRuns), 3+len(stats.SecondaryRuns); got < minRuns || got > maxRuns {
		t.Fatalf("stats runs=%d want between %d and %d", got, minRuns, maxRuns)
	}
	if got, want := len(stats.SecondaryRuns), 2; got != want {
		t.Fatalf("stats secondary runs=%d want %d", got, want)
	}
	if got, want := stats.SecondaryEntries, 4; got != want {
		t.Fatalf("stats secondary entries=%d want %d", got, want)
	}
	if stats.SecondaryKeyBytes == 0 {
		t.Fatal("stats secondary key bytes=0 want positive")
	}
	stats.SecondaryRuns[0].IndexName = "mutated"
	again := col.LastInsertStats()
	if got := again.SecondaryRuns[0].IndexName; got == "mutated" {
		t.Fatal("LastInsertStats did not return an owned secondary-run slice")
	}
	if ids, err := col.InsertBatch(nil, nil); err != nil {
		t.Fatalf("empty insert batch: %v", err)
	} else if ids != nil {
		t.Fatalf("empty insert ids=%v want nil", ids)
	}
	empty := col.LastInsertStats()
	if got, want := empty.Documents, 0; got != want {
		t.Fatalf("empty stats documents=%d want %d", got, want)
	}
	if got, want := empty.Indexes, 2; got != want {
		t.Fatalf("empty stats indexes=%d want %d", got, want)
	}
	if got := empty.Runs; got != 0 {
		t.Fatalf("empty stats runs=%d want 0", got)
	}
	if got := len(empty.SecondaryRuns); got != 0 {
		t.Fatalf("empty stats secondary runs=%d want 0", got)
	}
}

func TestCollectionInsertBatchStatsExposeNoIndexFastPath(t *testing.T) {
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
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	stats := col.LastInsertStats()
	if got, want := stats.Documents, 2; got != want {
		t.Fatalf("stats documents=%d want %d", got, want)
	}
	if got, want := stats.Indexes, 0; got != want {
		t.Fatalf("stats indexes=%d want %d", got, want)
	}
	if got, want := stats.Runs, 1; got != want {
		t.Fatalf("stats runs=%d want %d", got, want)
	}
	if got := len(stats.SecondaryRuns); got != 0 {
		t.Fatalf("stats secondary runs=%d want 0", got)
	}
}

func TestCollectionInsertBatchBridge_ReturnedIDsAndDocumentsAreOwned(t *testing.T) {
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

	inputID := []byte("u1")
	inputDocument := []byte(`{"name":"ada"}`)
	ids, err := col.InsertBatch(
		[][]byte{inputID},
		[][]byte{inputDocument},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	inputID[0] = 'x'
	inputDocument[9] = 'x'
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("returned ids=%q want owned u1", ids)
	}

	ids[0][0] = 'z'
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original id after mutating returned id: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("original id value=%q want %q", got, want)
	}
	if got, err := col.Get([]byte("z1")); err != nil || got != nil {
		t.Fatalf("mutated returned id lookup got=%q err=%v want missing", got, err)
	}
}

func TestCollectionInsertBatchBridge_IndexedReturnedIDsAreOwned(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	inputID := []byte("u1")
	inputDocument := []byte(`{"email":"ada@example.com","city":"hnl"}`)
	ids, err := col.InsertBatch(
		[][]byte{inputID},
		[][]byte{inputDocument},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	inputID[0] = 'x'
	inputDocument[10] = 'x'
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("returned ids=%q want owned u1", ids)
	}

	ids[0][0] = 'z'
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original id after mutating returned id: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("original id value=%q want %q", got, want)
	}
	if got, err := col.Get([]byte("z1")); err != nil || got != nil {
		t.Fatalf("mutated returned id lookup got=%q err=%v want missing", got, err)
	}
}

func TestCollectionSingleInsertBufferedNoIndexReadsBeforeFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	id := []byte("u1")
	doc := []byte(`{"name":"ada"}`)
	gotID, err := writer.Insert(id, doc)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id[0] = 'x'
	doc[9] = 'x'
	if !bytes.Equal(gotID, []byte("u1")) {
		t.Fatalf("returned id=%q want u1", gotID)
	}
	gotID[0] = 'z'

	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get buffered doc: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("buffered doc=%q want %q", got, want)
	}
	got[9] = 'x'
	again, err := writer.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("writer get buffered doc again: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(again, want) {
		t.Fatalf("buffered doc after caller mutation=%q want %q", again, want)
	}
}

func TestCollectionSingleInsertBufferedNoIndexFlushPersistsAfterReopen(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
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
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopened doc=%q want %q", got, want)
	}
}

func TestCollectionSingleInsertBufferedNoIndexCloseFlushes(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
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
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopened close-flushed doc=%q want %q", got, want)
	}
}

func TestCollectionSingleInsertBufferedNoIndexRejectsBufferedDuplicate(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"grace"}`)); err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("duplicate err=%v want document already exists", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}
}

func TestCollectionSingleInsertBufferedNoIndexRejectsConcurrentSchemaChange(t *testing.T) {
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
		t.Fatalf("open writer: %v", err)
	}
	if _, err := writer.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("buffer insert: %v", err)
	}

	indexMgr := NewCollectionManager(d)
	indexer, err := indexMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open indexer: %v", err)
	}
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	if _, err := writer.Insert([]byte("u2"), []byte(`{"email":"grace@example.com"}`)); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("insert after schema change err=%v want concurrent schema modification", err)
	}
	if err := writer.Flush(); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("flush after schema change err=%v want concurrent schema modification", err)
	}
}

func TestCollectionSingleInsertBufferedNoIndexRejectsConcurrentPrimaryRootChange(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	leftMgr := NewCollectionManager(d)
	if _, err := leftMgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	left, err := leftMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left writer: %v", err)
	}
	if _, err := left.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("left buffer insert: %v", err)
	}

	rightMgr := NewCollectionManager(d)
	right, err := rightMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open right writer: %v", err)
	}
	if _, err := right.Insert([]byte("u2"), []byte(`{"name":"grace"}`)); err != nil {
		t.Fatalf("right insert: %v", err)
	}
	if err := right.Flush(); err != nil {
		t.Fatalf("right flush: %v", err)
	}

	if _, err := left.Insert([]byte("u3"), []byte(`{"name":"katherine"}`)); err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("insert after root change err=%v want concurrent root modification", err)
	}
	if err := left.Flush(); err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("flush after root change err=%v want concurrent root modification", err)
	}
}

func TestRootDescriptorDeltaRejectsConcurrentSchemaChange(t *testing.T) {
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

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	baseSystemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	baseMeta := catalog.meta
	col.meta = baseMeta
	rootName := collectionPrimaryRootName("users")
	baseRoot := catalog.rootID(rootName)

	indexer, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open indexer: %v", err)
	}
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email"}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	iter, err := col.buildRootDescriptorSystemDeltaIterator(
		baseSystemRoot,
		[]string{rootName},
		map[string]uint64{rootName: baseRoot},
		[]uint64{baseRoot + 1},
	)
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("system delta err=%v want concurrent schema modification", err)
	}
}

func TestCreateIndexSystemIteratorRejectsConcurrentPrimaryRootChange(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush u1: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	baseMeta := catalog.meta
	newMeta, normalizedDef, err := addIndexToCollectionMeta(baseMeta, IndexDefinition{Name: "email", Field: "email"})
	if err != nil {
		t.Fatalf("add index metadata: %v", err)
	}
	primaryRootName := collectionPrimaryRootName("users")
	stateRootName := collectionIndexStateRootName("users")
	secondaryRootName := collectionSecondaryRootName("users", normalizedDef.Name)
	baseRootIDs := map[string]uint64{
		primaryRootName:   catalog.rootID(primaryRootName),
		stateRootName:     catalog.rootID(stateRootName),
		secondaryRootName: catalog.rootID(secondaryRootName),
	}

	other, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open other writer: %v", err)
	}
	if _, err := other.Insert([]byte("u2"), []byte(`{"email":"grace@example.com"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if err := other.Flush(); err != nil {
		t.Fatalf("flush u2: %v", err)
	}

	rootNames := []string{stateRootName, secondaryRootName}
	iter, err := col.buildSchemaAndRootDescriptorSystemIterator(
		baseMeta,
		newMeta,
		rootNames,
		baseRootIDs,
		[]uint64{baseRootIDs[primaryRootName] + 1, baseRootIDs[primaryRootName] + 2},
	)
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("schema publish err=%v want concurrent root modification", err)
	}
	if err != nil && !strings.Contains(err.Error(), primaryRootName) {
		t.Fatalf("schema publish err=%v want primary root name %q", err, primaryRootName)
	}
}

func TestCollectionInsertBatchBridge_ReopenUsesPersistedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
		t.Fatalf("insert batch: %v", err)
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
		t.Fatalf("open collection after reopen: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopen u1=%q want %q", got, want)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopen email ids=%q want u1", ids)
	}
}

func TestCollectionCachedCatalogRefreshesAfterCrossHandleWrite(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer collection: %v", err)
	}

	if got, err := reader.Get([]byte("u1")); err != nil || got != nil {
		t.Fatalf("initial reader get got=%q err=%v want missing", got, err)
	}
	if _, err := writer.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("writer insert: %v", err)
	}
	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get after writer insert: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("reader saw stale catalog value=%q want %q", got, want)
	}
}

func TestCollectionSingleInsertMatchesSingleItemBatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		indexes []IndexDefinition
	}{
		{name: "no_index"},
		{name: "indexed", indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			insertMeta := &CollectionMeta{Name: "insert_users", Indexes: tc.indexes}
			batchMeta := &CollectionMeta{Name: "batch_users", Indexes: tc.indexes}
			if _, err := mgr.CreateCollection(insertMeta); err != nil {
				t.Fatalf("create insert collection: %v", err)
			}
			if _, err := mgr.CreateCollection(batchMeta); err != nil {
				t.Fatalf("create batch collection: %v", err)
			}
			insertCol, err := mgr.OpenCollection("insert_users")
			if err != nil {
				t.Fatalf("open insert collection: %v", err)
			}
			batchCol, err := mgr.OpenCollection("batch_users")
			if err != nil {
				t.Fatalf("open batch collection: %v", err)
			}

			doc := []byte(`{"email":"ada@example.com","city":"hnl"}`)
			insertID, err := insertCol.Insert([]byte("u1"), doc)
			if err != nil {
				t.Fatalf("single insert: %v", err)
			}
			batchIDs, err := batchCol.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc})
			if err != nil {
				t.Fatalf("single-item batch insert: %v", err)
			}
			if len(batchIDs) != 1 || !bytes.Equal(insertID, batchIDs[0]) {
				t.Fatalf("ids insert=%q batch=%q", insertID, batchIDs)
			}

			insertDoc, err := insertCol.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("single get: %v", err)
			}
			batchDoc, err := batchCol.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("batch get: %v", err)
			}
			if !bytes.Equal(insertDoc, batchDoc) {
				t.Fatalf("documents differ: insert=%q batch=%q", insertDoc, batchDoc)
			}
			if len(tc.indexes) == 0 {
				return
			}
			insertIDs, err := insertCol.FindByIndex("email", "ada@example.com")
			if err != nil {
				t.Fatalf("single find email: %v", err)
			}
			batchIndexIDs, err := batchCol.FindByIndex("email", "ada@example.com")
			if err != nil {
				t.Fatalf("batch find email: %v", err)
			}
			if !reflect.DeepEqual(insertIDs, batchIndexIDs) {
				t.Fatalf("email ids differ: insert=%q batch=%q", insertIDs, batchIndexIDs)
			}
		})
	}
}

func TestCollectionSingleInsertRejectsUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate insert err=%v want unique index conflict", err)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get failed insert: %v", err)
	}
	if got != nil {
		t.Fatalf("failed insert left primary doc=%q", got)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
}

func TestCollectionSingleDocumentReopenUsesPersistedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"grace@example.com"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
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
		t.Fatalf("get deleted u1 after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 visible after reopen: %q", got)
	}
	got, err = reopenedCol.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2 after reopen: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("u2 after reopen=%q want %q", got, want)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email after reopen: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("deleted email ids after reopen=%q want none", ids)
	}
	ids, err = reopenedCol.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find remaining email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("remaining email ids after reopen=%q want [u2]", ids)
	}
}

func TestCollectionInsertBatchBridge_AppendsWithoutDroppingExistingRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert first batch: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert second batch: %v", err)
	}

	for id, want := range map[string][]byte{
		"u1": []byte(`{"email":"ada@example.com","city":"hnl"}`),
		"u2": []byte(`{"email":"grace@example.com","city":"hnl"}`),
	} {
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s=%q want %q", id, got, want)
		}
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 2 || !bytes.Equal(cityIDs[0], []byte("u1")) || !bytes.Equal(cityIDs[1], []byte("u2")) {
		t.Fatalf("city ids=%q want [u1 u2]", cityIDs)
	}
}

func TestCollectionCreateIndexBackfill_BuildsSecondaryAndIndexState(t *testing.T) {
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
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	meta, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, ok := findIndex(meta.Indexes, "city"); !ok {
		t.Fatalf("created meta missing city index: %+v", meta.Indexes)
	}
	if _, ok := findIndex(col.Meta().Indexes, "city"); !ok {
		t.Fatalf("collection meta missing city index after create: %+v", col.Meta().Indexes)
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 2 || !bytes.Equal(cityIDs[0], []byte("u1")) || !bytes.Equal(cityIDs[1], []byte("u2")) {
		t.Fatalf("city ids=%q want [u1 u2]", cityIDs)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := catalog.rootID(rootName); got == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
	}

	if _, err := col.Insert([]byte("u3"), []byte(`{"email":"katherine@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert after create index: %v", err)
	}
	cityIDs, err = col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after insert: %v", err)
	}
	if len(cityIDs) != 3 ||
		!bytes.Equal(cityIDs[0], []byte("u1")) ||
		!bytes.Equal(cityIDs[1], []byte("u2")) ||
		!bytes.Equal(cityIDs[2], []byte("u3")) {
		t.Fatalf("city ids after insert=%q want [u1 u2 u3]", cityIDs)
	}
}

func TestCollectionCreateIndexBackfill_EmptyCollectionUpdatesSchema(t *testing.T) {
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
		t.Fatalf("create index on empty collection: %v", err)
	}
	if _, ok := findIndex(col.Meta().Indexes, "city"); !ok {
		t.Fatalf("collection meta missing city index after empty create: %+v", col.Meta().Indexes)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert after empty create index: %v", err)
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after empty create: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city ids after empty create=%q want [u1]", ids)
	}
}

func TestCollectionCreateIndexBackfill_PreservesExistingIndexState(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
		t.Fatalf("create city index: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}

	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("deleted email ids=%q want none", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find deleted city: %v", err)
	}
	if len(cityIDs) != 0 {
		t.Fatalf("deleted city ids=%q want none", cityIDs)
	}
	if _, err := col.Insert([]byte("u3"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("reuse unique email after delete: %v", err)
	}
}

func TestCollectionCreateIndexBackfill_ReopenUsesPersistedSchemaAndRoots(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
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
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids after reopen=%q want [u1]", ids)
	}
	if _, err := reopenedCol.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate insert err=%v want unique index conflict", err)
	}
}

func TestCollectionCreateIndexBackfill_RejectsUniqueConflictAtomically(t *testing.T) {
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
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert duplicate documents before unique index: %v", err)
	}

	_, err = col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true})
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("create unique index err=%v want unique index conflict", err)
	}
	if _, ok := findIndex(col.Meta().Indexes, "email"); ok {
		t.Fatalf("collection meta gained failed email index: %+v", col.Meta().Indexes)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find failed index: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("failed index visible ids=%q want none", ids)
	}
}

func TestCollectionReplaceRejectsUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	replaced, err := col.Replace([]byte("u1"), []byte(`{"email":"grace@example.com","city":"hnl"}`))
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("replace err=%v want ErrUniqueIndexConflict", err)
	}
	if replaced {
		t.Fatal("replace reported success on unique conflict")
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1 after failed replace: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1 after failed replace=%q want %q", got, want)
	}

	replaced, err = col.Replace([]byte("u1"), []byte(`{"email":"ada2@example.com","city":"hnl"}`))
	if err != nil {
		t.Fatalf("replace valid: %v", err)
	}
	if !replaced {
		t.Fatal("replace valid reported false")
	}
	ids, err := col.FindByIndex("email", "ada2@example.com")
	if err != nil {
		t.Fatalf("find replacement email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("replacement email ids=%q want u1", ids)
	}
	ids, err = col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find old email: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("old email ids=%q want none", ids)
	}
}

func TestCollectionUpdateConcurrentCounterNoLostUpdates(t *testing.T) {
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
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"count":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	const (
		workers    = 8
		increments = 5
	)
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerCol, err := mgr.OpenCollection("users")
			if err != nil {
				errs <- err
				return
			}
			<-start
			for j := 0; j < increments; j++ {
				if _, _, err := workerCol.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
					var doc struct {
						Count int `json:"count"`
					}
					if err := json.Unmarshal(current, &doc); err != nil {
						return nil, false, err
					}
					doc.Count++
					next, err := json.Marshal(doc)
					if err != nil {
						return nil, false, err
					}
					return next, true, nil
				}); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	var doc struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("unmarshal counter: %v", err)
	}
	if want := workers * increments; doc.Count != want {
		t.Fatalf("count=%d want %d", doc.Count, want)
	}
}

func TestCollectionInsertBatchBridge_RejectsPersistedUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("seed"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want unique index conflict", err)
	}
	for _, id := range [][]byte{[]byte("u1"), []byte("u2")} {
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if got != nil {
			t.Fatalf("unexpected doc %q=%q", id, got)
		}
	}
}

func TestCollectionInsertBatchBridge_RejectsPersistedDocumentIDAtomically(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"seed"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"dup"}`), []byte(`{"name":"new"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("err=%v want persisted document id conflict", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"name":"seed"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("unexpected u2 doc=%q", got)
	}
}

func TestCollectionInsertBatchBridge_RejectsDuplicateIDBeforePublish(t *testing.T) {
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
	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u1")},
		[][]byte{[]byte(`{"name":"first"}`), []byte(`{"name":"second"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate document id") {
		t.Fatalf("err=%v want duplicate document id", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got != nil {
		t.Fatalf("unexpected u1 doc=%q", got)
	}
}

func TestCollectionDeleteBridge_RemovesPrimaryAndSecondaryEntries(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	deleted, err := col.DeleteDocument([]byte("missing"))
	if err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if deleted {
		t.Fatal("delete missing reported deleted")
	}
	deleted, err = col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if !deleted {
		t.Fatal("delete u1 reported not deleted")
	}
	deleted, err = col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete u1 again: %v", err)
	}
	if deleted {
		t.Fatal("delete u1 again reported deleted")
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted u1: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 still visible: %q", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u2=%q want %q", got, want)
	}

	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("deleted email ids=%q want none", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 1 || !bytes.Equal(cityIDs[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", cityIDs)
	}
}

func TestCollectionDeleteBridge_AllowsUniqueValueReuse(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("reuse unique email: %v", err)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("email ids=%q want [u2]", ids)
	}
}

func TestCollectionDeleteBridge_ReopenUsesDeletedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
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
		t.Fatalf("get deleted u1 after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 visible after reopen: %q", got)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email after reopen: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("deleted email ids after reopen=%q want none", ids)
	}
	ids, err = reopenedCol.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find remaining email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("remaining email ids=%q want [u2]", ids)
	}
}
