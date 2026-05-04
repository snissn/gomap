package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateJSONDoubleSameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "scores",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "score", Field: "score", ValueType: IndexValueDouble},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("scores")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{
		[]byte(`{"score":{"$numberDouble":"2.5"},"note":"before"}`),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonDirectDoubleRootIDs(t, d)
	assertJSONDirectDoubleIDs(t, col, 2.5, "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"score":{"$numberDouble":"2.5"},"note":"same-score"}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct JSON same-score update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct JSON same-score update matched=%v modified=%v want true/true", matched, modified)
	}

	afterSame := jsonDirectDoubleRootIDs(t, d)
	if rootName := collectionPrimaryRootName("scores"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for unchanged JSON double index value", rootName, before[rootName], afterSame[rootName])
		}
	}
	assertJSONDirectDoubleIDs(t, col, 2.5, "u1")

	matched, modified, err = col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"score":{"$numberDouble":"3.5"},"note":"changed-score"}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct JSON changed-score update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct JSON changed-score update matched=%v modified=%v want true/true", matched, modified)
	}

	afterChanged := jsonDirectDoubleRootIDs(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	} {
		if afterChanged[rootName] == afterSame[rootName] {
			t.Fatalf("root %q did not change for changed JSON double index value", rootName)
		}
	}
	assertJSONDirectDoubleIDs(t, col, 2.5)
	assertJSONDirectDoubleIDs(t, col, 3.5, "u1")
}

func jsonDirectDoubleRootIDs(t *testing.T, d *backenddb.DB) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	catalog, err := loadCollectionCatalog(snap, "scores")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("scores"),
		collectionIndexStateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	}
	out := make(map[string]uint64, len(names))
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			t.Fatalf("root %q was not persisted", name)
		}
		out[name] = rootID
	}
	return out
}

func assertJSONDirectDoubleIDs(t *testing.T, col *Collection, score float64, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue("score", score)
	if err != nil {
		t.Fatalf("find score %v: %v", score, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		t.Fatalf("score %v ids=%q want %q", score, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("score %v ids=%q want %q", score, ids, want)
		}
	}
}
