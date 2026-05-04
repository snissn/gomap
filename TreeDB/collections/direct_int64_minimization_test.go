package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateSkipsUnchangedInt64IndexRoot(t *testing.T) {
	const score = int64(9007199254740993)

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "score", Field: "score", ValueType: IndexValueInt64},
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
		[][]byte{[]byte(`{"score":9007199254740993,"note":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	before := directInt64RootIDs(t, d)

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"score":9007199254740993,"note":1}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}

	after := directInt64RootIDs(t, d)
	if rootName := collectionPrimaryRootName("users"); after[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified document", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "score"),
	} {
		if after[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for unchanged int64 index value", rootName, before[rootName], after[rootName])
		}
	}
	ids, err := col.FindByIndexValue("score", score)
	if err != nil {
		t.Fatalf("find score: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("score ids=%q want [u1]", ids)
	}
}

func directInt64RootIDs(t *testing.T, d *backenddb.DB) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "score"),
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
