package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateSkipsUnchangedMultikeySecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1")},
		[][]byte{templateV1MultikeyDoc(t, []any{"b", nil, "a", "a"}, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1MultikeyRoots(t, d)
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "b", "d1")

	sameTagsDoc := templateV1MultikeyDoc(t, []any{"a", "b", "a", nil}, "same-effective-tags")
	matched, modified, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return sameTagsDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update same effective tags: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("same-effective tags update matched=%v modified=%v want true/true", matched, modified)
	}
	afterSameEffective := templateV1MultikeyRoots(t, d)
	for _, rootName := range []string{
		collectionTemplateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	} {
		if afterSameEffective[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for template-v1 multikey replacement with unchanged effective values", rootName, before[rootName], afterSameEffective[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("docs"); afterSameEffective[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified template-v1 replacement", rootName)
	}
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "b", "d1")

	changedTagsDoc := templateV1MultikeyDoc(t, []any{"a", "c"}, "tag-changed")
	matched, modified, err = col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return changedTagsDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update changed tags: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("changed tags update matched=%v modified=%v want true/true", matched, modified)
	}
	afterChanged := templateV1MultikeyRoots(t, d)
	if rootName := collectionSecondaryRootName("docs", "tag"); afterChanged[rootName] == afterSameEffective[rootName] {
		t.Fatalf("tag root %q did not change for changed template-v1 multikey values", rootName)
	}
	if rootName := collectionTemplateRootName("docs"); afterChanged[rootName] != afterSameEffective[rootName] {
		t.Fatalf("template root %q changed from %d to %d for same-shape tag update", rootName, afterSameEffective[rootName], afterChanged[rootName])
	}
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "b")
	templateV1MultikeyRequireIndexIDs(t, col, "tag", "c", "d1")
}

func templateV1MultikeyDoc(tb *testing.T, tags []any, note string) []byte {
	tb.Helper()
	return mustTemplateV1Document(tb, []string{"tags", "note"}, []any{tags, note})
}

func templateV1MultikeyRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("docs"),
		collectionTemplateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	}
	roots := make(map[string]uint64, len(names))
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
		roots[name] = rootID
	}
	return roots
}

func templateV1MultikeyRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range want {
		if !bytes.Equal(ids[i], []byte(want[i])) {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
