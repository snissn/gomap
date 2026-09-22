package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionDirectUpdateBSONInt64SameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "scores",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatBSON,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "score", Field: "score", ValueType: IndexValueInt64},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("scores")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const (
		oldScore int64 = 9007199254740993
		newScore int64 = 9007199254740995
	)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "score", Value: oldScore},
			{Key: "note", Value: "before"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	}
	before := mustBSONDirectInt64RootIDs(t, d, "scores", rootNames...)
	assertBSONDirectInt64IDs(t, col, oldScore, "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "score", Value: oldScore},
			{Key: "note", Value: "after"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON same-score update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct BSON same-score update matched=%v modified=%v want true/true", matched, modified)
	}

	afterSame := mustBSONDirectInt64RootIDs(t, d, "scores", rootNames...)
	if afterSame[collectionPrimaryRootName("scores")] == before[collectionPrimaryRootName("scores")] {
		t.Fatalf("primary root did not change for BSON replacement")
	}
	scoreRoot := collectionSecondaryRootName("scores", "score")
	if afterSame[scoreRoot] != before[scoreRoot] {
		t.Fatalf("score secondary root changed from %d to %d for same BSON int64 value", before[scoreRoot], afterSame[scoreRoot])
	}
	assertBSONDirectInt64IDs(t, col, oldScore, "u1")

	matched, modified, err = col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "score", Value: newScore},
			{Key: "note", Value: "changed-score"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON changed-score update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct BSON changed-score update matched=%v modified=%v want true/true", matched, modified)
	}
	afterChanged := mustBSONDirectInt64RootIDs(t, d, "scores", rootNames...)
	if afterChanged[scoreRoot] == afterSame[scoreRoot] {
		t.Fatalf("score secondary root did not change for changed BSON int64 value")
	}
	assertBSONDirectInt64IDs(t, col, oldScore)
	assertBSONDirectInt64IDs(t, col, newScore, "u1")
}

func mustBSONDirectInt64RootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
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

func assertBSONDirectInt64IDs(t *testing.T, col *Collection, score int64, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue("score", score)
	if err != nil {
		t.Fatalf("find score %d: %v", score, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		t.Fatalf("score %d ids=%q want %q", score, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("score %d ids=%q want %q", score, ids, want)
		}
	}
}
