package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateSeparatesTemplateAndSecondaryRootWork(t *testing.T) {
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
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
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
		[][]byte{templateV1MinimizationDoc(t, `{"email":"ada@example.com","city":"hnl","note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1MinimizationRoots(t, d)
	templateV1MinimizationRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1MinimizationRequireIndexIDs(t, col, "city", "hnl", "u1")

	sameTemplateDoc := templateV1MinimizationDoc(t, `{"email":"ada@example.com","city":"hnl","note":"same-template"}`)
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return sameTemplateDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update same template/index values: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("same-template update matched=%v modified=%v want true/true", matched, modified)
	}
	afterSameTemplate := templateV1MinimizationRoots(t, d)
	for _, rootName := range []string{
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterSameTemplate[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for template-v1 replacement with unchanged template/index values", rootName, before[rootName], afterSameTemplate[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterSameTemplate[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified template-v1 replacement", rootName)
	}

	newTemplateDoc := templateV1MinimizationDoc(t, `{"email":"ada@example.com","city":"hnl","note":"new-template","updated":true}`)
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return newTemplateDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update new template shape: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("new-template update matched=%v modified=%v want true/true", matched, modified)
	}
	afterNewTemplate := templateV1MinimizationRoots(t, d)
	if rootName := collectionTemplateRootName("users"); afterNewTemplate[rootName] == afterSameTemplate[rootName] {
		t.Fatalf("template root %q did not change for new template-v1 shape", rootName)
	}
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterNewTemplate[rootName] != afterSameTemplate[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for template-only shape update", rootName, afterSameTemplate[rootName], afterNewTemplate[rootName])
		}
	}
	templateV1MinimizationRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1MinimizationRequireIndexIDs(t, col, "city", "hnl", "u1")

	cityDoc := templateV1MinimizationDoc(t, `{"email":"ada@example.com","city":"sea","note":"city-changed","updated":true}`)
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return cityDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update city value: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("city update matched=%v modified=%v want true/true", matched, modified)
	}
	afterCity := templateV1MinimizationRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "city"); afterCity[rootName] == afterNewTemplate[rootName] {
		t.Fatalf("city root %q did not change for indexed city update", rootName)
	}
	for _, rootName := range []string{
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
	} {
		if afterCity[rootName] != afterNewTemplate[rootName] {
			t.Fatalf("root %q changed from %d to %d for city-only template-v1 update", rootName, afterNewTemplate[rootName], afterCity[rootName])
		}
	}
	templateV1MinimizationRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1MinimizationRequireIndexIDs(t, col, "city", "hnl")
	templateV1MinimizationRequireIndexIDs(t, col, "city", "sea", "u1")
}

func templateV1MinimizationDoc(tb testing.TB, rawJSON string) []byte {
	tb.Helper()
	doc, err := EncodeTemplateV1DocumentJSON([]byte(rawJSON))
	if err != nil {
		tb.Fatalf("encode template-v1 document %s: %v", rawJSON, err)
	}
	return doc
}

func templateV1MinimizationRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
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

func templateV1MinimizationRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
