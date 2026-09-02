package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionDirectUpdateBSONBoolSameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "flags",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatBSON,
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

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "active", Value: true},
			{Key: "note", Value: "before"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
	}
	before := mustBSONDirectBoolRootIDs(t, d, "flags", rootNames...)
	assertBSONDirectBoolIDs(t, col, true, "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "active", Value: true},
			{Key: "note", Value: "same-active"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON same-active update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct BSON same-active update matched=%v modified=%v want true/true", matched, modified)
	}

	afterSame := mustBSONDirectBoolRootIDs(t, d, "flags", rootNames...)
	if afterSame[collectionPrimaryRootName("flags")] == before[collectionPrimaryRootName("flags")] {
		t.Fatalf("primary root did not change for BSON replacement")
	}
	activeRoot := collectionSecondaryRootName("flags", "active")
	if afterSame[activeRoot] != before[activeRoot] {
		t.Fatalf("active secondary root changed from %d to %d for same BSON bool value", before[activeRoot], afterSame[activeRoot])
	}
	assertBSONDirectBoolIDs(t, col, true, "u1")

	matched, modified, err = col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "active", Value: false},
			{Key: "note", Value: "changed-active"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON changed-active update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct BSON changed-active update matched=%v modified=%v want true/true", matched, modified)
	}
	afterChanged := mustBSONDirectBoolRootIDs(t, d, "flags", rootNames...)
	if afterChanged[activeRoot] == afterSame[activeRoot] {
		t.Fatalf("active secondary root did not change for changed BSON bool value")
	}
	assertBSONDirectBoolIDs(t, col, true)
	assertBSONDirectBoolIDs(t, col, false, "u1")
}

func mustBSONDirectBoolRootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
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

func assertBSONDirectBoolIDs(t *testing.T, col *Collection, active bool, want ...string) {
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
