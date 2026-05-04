package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateTreatsMissingAndNullIndexValuesAsUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "deleted_at", Field: "deleted_at", ValueType: IndexValueString},
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
		[][]byte{templateV1NullMinDoc(t, `{"city":"hnl","note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1NullMinRoots(t, d)
	templateV1NullMinRequireIndexIDs(t, col, "city", "hnl", "u1")
	templateV1NullMinRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	nullDoc := templateV1NullMinDoc(t, `{"city":"hnl","deleted_at":null,"note":"explicit-null"}`)
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return nullDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update missing deleted_at to null: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("missing-to-null update matched=%v modified=%v want true/true", matched, modified)
	}
	afterNull := templateV1NullMinRoots(t, d)
	if rootName := collectionTemplateRootName("users"); afterNull[rootName] == before[rootName] {
		t.Fatalf("template root %q did not change when template-v1 shape added deleted_at", rootName)
	}
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	} {
		if afterNull[rootName] != before[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for template-v1 missing-to-null index value update", rootName, before[rootName], afterNull[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterNull[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified template-v1 replacement", rootName)
	}
	templateV1NullMinRequireIndexIDs(t, col, "city", "hnl", "u1")
	templateV1NullMinRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	valueDoc := templateV1NullMinDoc(t, `{"city":"hnl","deleted_at":"2026-05-03","note":"deleted-at-set"}`)
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return valueDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update deleted_at value: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("deleted_at value update matched=%v modified=%v want true/true", matched, modified)
	}
	afterValue := templateV1NullMinRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "deleted_at"); afterValue[rootName] == afterNull[rootName] {
		t.Fatalf("deleted_at root %q did not change when template-v1 null became an indexed value", rootName)
	}
	for _, rootName := range []string{
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterValue[rootName] != afterNull[rootName] {
			t.Fatalf("root %q changed from %d to %d for deleted_at-only template-v1 update", rootName, afterNull[rootName], afterValue[rootName])
		}
	}
	templateV1NullMinRequireIndexIDs(t, col, "city", "hnl", "u1")
	templateV1NullMinRequireIndexIDs(t, col, "deleted_at", "2026-05-03", "u1")
}

func templateV1NullMinDoc(tb testing.TB, rawJSON string) []byte {
	tb.Helper()
	doc, err := EncodeTemplateV1DocumentJSON([]byte(rawJSON))
	if err != nil {
		tb.Fatalf("encode template-v1 document %s: %v", rawJSON, err)
	}
	return doc
}

func templateV1NullMinRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("users"),
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	}
	roots := make(map[string]uint64, len(names))
	for _, name := range names {
		roots[name] = catalog.rootID(name)
	}
	for _, name := range []string{
		collectionPrimaryRootName("users"),
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if roots[name] == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
	}
	return roots
}

func templateV1NullMinRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
