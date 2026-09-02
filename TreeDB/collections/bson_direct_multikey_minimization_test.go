package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionDirectUpdateBSONSameEffectiveMultikeySkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
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
	before := mustCollectionRootIDsForBSONMultikeyTest(t, d, "users", rootNames...)
	assertBSONMultikeyIDs(t, col, "a", "u1")
	assertBSONMultikeyIDs(t, col, "b", "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "tags", Value: bson.A{"a", "b"}},
			{Key: "note", Value: "after"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct BSON update matched=%v modified=%v want true/true", matched, modified)
	}

	after := mustCollectionRootIDsForBSONMultikeyTest(t, d, "users", rootNames...)
	if after[collectionPrimaryRootName("users")] == before[collectionPrimaryRootName("users")] {
		t.Fatalf("primary root did not change for BSON replacement")
	}
	tagsRoot := collectionSecondaryRootName("users", "tags")
	if after[tagsRoot] != before[tagsRoot] {
		t.Fatalf("tags secondary root changed from %d to %d for same effective multikey values", before[tagsRoot], after[tagsRoot])
	}
	assertBSONMultikeyIDs(t, col, "a", "u1")
	assertBSONMultikeyIDs(t, col, "b", "u1")
	assertBSONMultikeyIDs(t, col, "c")

	matched, modified, err = col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "tags", Value: bson.A{"a", "c"}},
			{Key: "note", Value: "changed-tags"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON changed-tag update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct BSON changed-tag update matched=%v modified=%v want true/true", matched, modified)
	}
	afterChanged := mustCollectionRootIDsForBSONMultikeyTest(t, d, "users", rootNames...)
	if afterChanged[tagsRoot] == after[tagsRoot] {
		t.Fatalf("tags secondary root did not change for changed BSON multikey values")
	}
	assertBSONMultikeyIDs(t, col, "a", "u1")
	assertBSONMultikeyIDs(t, col, "b")
	assertBSONMultikeyIDs(t, col, "c", "u1")
}

func mustCollectionRootIDsForBSONMultikeyTest(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
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

func assertBSONMultikeyIDs(t *testing.T, col *Collection, tag string, want ...string) {
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
