package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateJSONBoolSameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "flags",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "active", Field: "active", ValueType: IndexValueBool},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("flags")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{
		[]byte(`{"active":true,"note":"before"}`),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonDirectBoolRootIDs(t, d)
	assertJSONDirectBoolIDs(t, col, true, "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"active":true,"note":"same-active"}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct JSON same-active update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct JSON same-active update matched=%v modified=%v want true/true", matched, modified)
	}

	afterSame := jsonDirectBoolRootIDs(t, d)
	if rootName := collectionPrimaryRootName("flags"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for unchanged JSON bool index value", rootName, before[rootName], afterSame[rootName])
		}
	}
	assertJSONDirectBoolIDs(t, col, true, "u1")

	matched, modified, err = col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"active":false,"note":"changed-active"}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct JSON changed-active update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct JSON changed-active update matched=%v modified=%v want true/true", matched, modified)
	}

	afterChanged := jsonDirectBoolRootIDs(t, d)
	if rootName := collectionPrimaryRootName("flags"); afterChanged[rootName] == afterSame[rootName] {
		t.Fatalf("primary root %q did not change for second modified JSON replacement", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
	} {
		if afterChanged[rootName] == afterSame[rootName] {
			t.Fatalf("root %q did not change for changed JSON bool index value", rootName)
		}
	}
	assertJSONDirectBoolIDs(t, col, true)
	assertJSONDirectBoolIDs(t, col, false, "u1")
}

func jsonDirectBoolRootIDs(t *testing.T, d *backenddb.DB) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	catalog, err := loadCollectionCatalog(snap, "flags")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("flags"),
		collectionIndexStateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
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

func assertJSONDirectBoolIDs(t *testing.T, col *Collection, active bool, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue("active", active)
	if err != nil {
		t.Fatalf("find active %v: %v", active, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		t.Fatalf("active %v ids=%q want %q", active, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("active %v ids=%q want %q", active, ids, want)
		}
	}
}
