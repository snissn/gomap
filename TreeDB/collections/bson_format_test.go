package collections

import (
	"bytes"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONFormatStoresNativeBSONAndIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
			{Name: "age", Field: "age"},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc1 := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "email", Value: "ada@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "age", Value: int64(37)},
	})
	doc2 := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u2"},
		{Key: "email", Value: "grace@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "age", Value: int32(42)},
	})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{doc1, doc2},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed memtables: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc1) {
		t.Fatalf("stored BSON changed\n got=%x\nwant=%x", got, doc1)
	}
	if err := bson.Raw(got).Validate(); err != nil {
		t.Fatalf("stored BSON did not validate: %v", err)
	}

	emailIDs, err := col.FindByIndexValue("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u2")) {
		t.Fatalf("email ids=%q want u2", emailIDs)
	}
	cityIDs, err := col.FindByIndexValue("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))
	ageIDs, err := col.FindByIndexValue("age", int64(37))
	if err != nil {
		t.Fatalf("find age: %v", err)
	}
	if len(ageIDs) != 1 || !bytes.Equal(ageIDs[0], []byte("u1")) {
		t.Fatalf("age ids=%q want u1", ageIDs)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	requireNoCollectionRootDescriptor(t, snap, collectionIndexStateRootName("users"))
	_ = snap.Close()
	if got := catalog.rootID(collectionSecondaryRootName("users", "email")); got == 0 {
		t.Fatal("email secondary root was not persisted")
	}
}

func TestCollectionBSONFormatRejectsInvalidBSON(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("bad")}, [][]byte{[]byte{1, 2, 3}}); err == nil {
		t.Fatal("insert invalid BSON err=nil want error")
	}
}

func TestInsertBatchValidatedBSONRequiresBSONCollection(t *testing.T) {
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
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	_, err = col.InsertBatchValidatedBSON([][]byte{[]byte("u1")}, [][]byte{doc})
	if err == nil || !strings.Contains(err.Error(), "trusted BSON insert requires BSON document format") {
		t.Fatalf("InsertBatchValidatedBSON err=%v want BSON format error", err)
	}
}

func TestInsertBatchPlannerBSONSkipsIndexStateRun(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", unique: true},
			{name: "city", field: "city"},
		},
		options: collectionOptions{documentFormat: DocumentFormatBSON},
	}

	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "ada@example.com"}, {Key: "city", Value: "hnl"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "grace@example.com"}, {Key: "city", Value: "hnl"}}),
		},
	)
	if err != nil {
		t.Fatalf("plan BSON insert batch: %v", err)
	}
	if idx := findRunIndex(plan, collectionRootIndexState, ""); idx >= 0 {
		t.Fatalf("BSON plan unexpectedly emitted index-state run at %d", idx)
	}
	if got := plan.stats.IndexStateRunBuild; got != 0 {
		t.Fatalf("BSON index-state run build=%s want 0", got)
	}
	_ = mustFindRun(t, plan, collectionRootPrimary, "")
	_ = mustFindRun(t, plan, collectionRootSecondary, "email")
	_ = mustFindRun(t, plan, collectionRootSecondary, "city")
}

func TestOrderedIndexStateForDocumentBSONHandlesScalarsAndArrays(t *testing.T) {
	runtimes := []indexRuntime{
		{def: indexDefinition{name: "email", field: "email"}, path: []string{"email"}},
		{def: indexDefinition{name: "age", field: "age"}, path: []string{"age"}},
		{def: indexDefinition{name: "tag", field: "tags", multiKey: true}, path: []string{"tags"}},
		{def: indexDefinition{name: "deleted_at", field: "deleted_at"}, path: []string{"deleted_at"}},
	}
	doc := mustBSONCollectionDocument(t, bson.D{
		{Key: "email", Value: "ada@example.com"},
		{Key: "age", Value: int64(37)},
		{Key: "tags", Value: bson.A{"b", "a", "a"}},
		{Key: "deleted_at", Value: nil},
	})

	state, err := orderedIndexStateForDocument(doc, runtimes, collectionOptions{documentFormat: DocumentFormatBSON})
	if err != nil {
		t.Fatalf("ordered BSON index state: %v", err)
	}
	requireOrderedIndexValues(t, state, 0, "s:ada@example.com")
	requireOrderedIndexValues(t, state, 1, "n:37")
	requireOrderedIndexValues(t, state, 2, "s:a", "s:b")
	requireOrderedIndexValues(t, state, 3)
}

func TestCollectionBSONUniqueIndexSkipsNullValues(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc1 := mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: nil}, {Key: "city", Value: "hnl"}})
	doc2 := mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: nil}, {Key: "city", Value: "sea"}})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{doc1, doc2},
	); err != nil {
		t.Fatalf("insert null unique values: %v", err)
	}
}

func requireOrderedIndexValues(tb testing.TB, state orderedDocumentIndexState, runtimeIdx int, want ...string) {
	tb.Helper()
	got := state.valuesAt(runtimeIdx)
	if len(got) != len(want) {
		tb.Fatalf("runtime %d values=%q want %q", runtimeIdx, got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			tb.Fatalf("runtime %d value %d=%q want %q", runtimeIdx, i, got[i], want[i])
		}
	}
}

func mustBSONCollectionDocument(tb testing.TB, doc bson.D) []byte {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal BSON document: %v", err)
	}
	return raw
}
