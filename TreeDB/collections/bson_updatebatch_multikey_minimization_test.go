package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionUpdateBatchBSONSameEffectiveMultikeySkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatBSON,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "tags", Field: "tags", ValueType: IndexValueString, MultiKey: true},
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
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "tags", Value: bson.A{"b", "a", "a"}},
			{Key: "note", Value: "before"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "tags"),
	}
	before := mustBSONUpdateBatchMultikeyRootIDs(t, d, "users", rootNames...)
	assertBSONUpdateBatchMultikeyIDs(t, col, "a", "u1")
	assertBSONUpdateBatchMultikeyIDs(t, col, "b", "u1")

	sameTagsDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "tags", Value: bson.A{"a", "b"}},
		{Key: "note", Value: "same-effective-tags"},
	})
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return sameTagsDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update same effective BSON tags: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("same effective update results=%+v want one matched modified row", results)
	}

	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same effective update secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same effective update index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("same effective update index unchanged=%d want %d", got, want)
	}

	afterSame := mustBSONUpdateBatchMultikeyRootIDs(t, d, "users", rootNames...)
	if afterSame[collectionPrimaryRootName("users")] == before[collectionPrimaryRootName("users")] {
		t.Fatalf("primary root did not change for BSON replacement")
	}
	tagsRoot := collectionSecondaryRootName("users", "tags")
	if afterSame[tagsRoot] != before[tagsRoot] {
		t.Fatalf("tags secondary root changed from %d to %d for same effective multikey values", before[tagsRoot], afterSame[tagsRoot])
	}
	assertBSONUpdateBatchMultikeyIDs(t, col, "a", "u1")
	assertBSONUpdateBatchMultikeyIDs(t, col, "b", "u1")
	assertBSONUpdateBatchMultikeyIDs(t, col, "c")

	changedTagsDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "tags", Value: bson.A{"a", "c"}},
		{Key: "note", Value: "changed-tags"},
	})
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return changedTagsDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update changed BSON tags: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("changed-tag update results=%+v want one matched modified row", results)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.SecondaryDeleteEntries, 2; got != want {
		t.Fatalf("changed-tag update secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 2; got != want {
		t.Fatalf("changed-tag update secondary sets=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("changed-tag update index changes=%d want %d", got, want)
	}
	afterChanged := mustBSONUpdateBatchMultikeyRootIDs(t, d, "users", rootNames...)
	if afterChanged[tagsRoot] == afterSame[tagsRoot] {
		t.Fatalf("tags secondary root did not change for changed BSON multikey values")
	}
	assertBSONUpdateBatchMultikeyIDs(t, col, "a", "u1")
	assertBSONUpdateBatchMultikeyIDs(t, col, "b")
	assertBSONUpdateBatchMultikeyIDs(t, col, "c", "u1")
}

func mustBSONUpdateBatchMultikeyRootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
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
		t.Fatalf("missing catalog for collection %q", collectionName)
	}
	out := make(map[string]uint64, len(rootNames))
	for _, name := range rootNames {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			t.Fatalf("root %q was not persisted", name)
		}
		out[name] = rootID
	}
	return out
}

func assertBSONUpdateBatchMultikeyIDs(t *testing.T, col *Collection, tag string, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue("tags", tag)
	if err != nil {
		t.Fatalf("find tag %q: %v", tag, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		t.Fatalf("tag %q ids=%q want %q", tag, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("tag %q ids=%q want %q", tag, ids, want)
		}
	}
}
