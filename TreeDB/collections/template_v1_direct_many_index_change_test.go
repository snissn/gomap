package collections

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1DirectUpdateChangesOneIndexAmongMany(t *testing.T) {
	const indexCount = 24
	const changedIndex = 13

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	indexes := make([]IndexDefinition, indexCount)
	for i := range indexes {
		field := templateV1DirectManyIndexField(i)
		indexes[i] = IndexDefinition{Name: field, Field: field, ValueType: IndexValueString}
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: indexes,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1")},
		[][]byte{templateV1DirectManyIndexDoc(t, indexCount, -1, "", "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1DirectManyIndexRoots(t, d, indexCount)
	for i := range indexes {
		field := templateV1DirectManyIndexField(i)
		templateV1DirectManyIndexRequireIDs(t, col, field, templateV1DirectManyIndexValue(i), "d1")
	}

	changedField := templateV1DirectManyIndexField(changedIndex)
	changedValue := "changed-13"
	matched, modified, err := col.updateDirect([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return templateV1DirectManyIndexDoc(t, indexCount, changedIndex, changedValue, "one-index-changed"), true, nil
	})
	if err != nil {
		t.Fatalf("direct template-v1 one-index update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct template-v1 one-index update matched=%v modified=%v want true/true", matched, modified)
	}

	after := templateV1DirectManyIndexRoots(t, d, indexCount)
	for _, rootName := range []string{
		collectionPrimaryRootName("docs"),
		collectionSecondaryRootName("docs", changedField),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for one-index template-v1 direct update", rootName)
		}
	}
	templateRoot := collectionTemplateRootName("docs")
	if after[templateRoot] != before[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same-shape template-v1 direct update", before[templateRoot], after[templateRoot])
	}
	for i := range indexes {
		if i == changedIndex {
			continue
		}
		rootName := collectionSecondaryRootName("docs", templateV1DirectManyIndexField(i))
		if after[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d when only %s changed", rootName, before[rootName], after[rootName], changedField)
		}
	}
	templateV1DirectManyIndexRequireIDs(t, col, changedField, templateV1DirectManyIndexValue(changedIndex))
	templateV1DirectManyIndexRequireIDs(t, col, changedField, changedValue, "d1")
	for i := range indexes {
		if i == changedIndex {
			continue
		}
		field := templateV1DirectManyIndexField(i)
		templateV1DirectManyIndexRequireIDs(t, col, field, templateV1DirectManyIndexValue(i), "d1")
	}
}

func templateV1DirectManyIndexDoc(tb testing.TB, indexCount, changedIndex int, changedValue, note string) []byte {
	tb.Helper()
	doc := map[string]string{"note": note}
	for i := 0; i < indexCount; i++ {
		value := templateV1DirectManyIndexValue(i)
		if i == changedIndex {
			value = changedValue
		}
		doc[templateV1DirectManyIndexField(i)] = value
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal template-v1 document: %v", err)
	}
	out, err := EncodeTemplateV1DocumentJSON(raw)
	if err != nil {
		tb.Fatalf("encode template-v1 document: %v", err)
	}
	return out
}

func templateV1DirectManyIndexRoots(tb testing.TB, d *backenddb.DB, indexCount int) map[string]uint64 {
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
	}
	for i := 0; i < indexCount; i++ {
		names = append(names, collectionSecondaryRootName("docs", templateV1DirectManyIndexField(i)))
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

func templateV1DirectManyIndexRequireIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}

func templateV1DirectManyIndexField(i int) string {
	return fmt.Sprintf("f%02d", i)
}

func templateV1DirectManyIndexValue(i int) string {
	return fmt.Sprintf("v%02d", i)
}
