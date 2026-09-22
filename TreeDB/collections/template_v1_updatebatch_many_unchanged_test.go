package collections

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateBatchManyIndexesSkipsAllSecondaryWorkForNonIndexedUpdate(t *testing.T) {
	const indexCount = 24

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	indexes := make([]IndexDefinition, indexCount)
	for i := range indexes {
		name := templateV1UpdateBatchManyUnchangedField(i)
		indexes[i] = IndexDefinition{Name: name, Field: name, ValueType: IndexValueString}
	}
	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
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
		[][]byte{templateV1UpdateBatchManyUnchangedDoc(t, indexCount, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1UpdateBatchManyUnchangedRoots(t, d, indexCount)
	replacement := templateV1UpdateBatchManyUnchangedDoc(t, indexCount, "primary-changed")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch template-v1 non-indexed update: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", results)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("changed indexes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, indexCount; got != want {
		t.Fatalf("unchanged indexes=%d want %d", got, want)
	}
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("secondary work deletes=%d sets=%d bytes=%d runs=%+v, want none",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}

	after := templateV1UpdateBatchManyUnchangedRoots(t, d, indexCount)
	if after[collectionPrimaryRootName("docs")] == before[collectionPrimaryRootName("docs")] {
		t.Fatal("primary root did not change for template-v1 document replacement")
	}
	templateRoot := collectionTemplateRootName("docs")
	if after[templateRoot] != before[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same-shape template-v1 update", before[templateRoot], after[templateRoot])
	}
	for i := 0; i < indexCount; i++ {
		rootName := collectionSecondaryRootName("docs", templateV1UpdateBatchManyUnchangedField(i))
		if after[rootName] != before[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for non-indexed template-v1 update", rootName, before[rootName], after[rootName])
		}
		templateV1UpdateBatchManyUnchangedRequireIDs(t, col, templateV1UpdateBatchManyUnchangedField(i), templateV1UpdateBatchManyUnchangedValue(i), "d1")
	}
}

func templateV1UpdateBatchManyUnchangedDoc(tb testing.TB, indexCount int, note string) []byte {
	tb.Helper()
	doc := map[string]string{"note": note}
	for i := 0; i < indexCount; i++ {
		doc[templateV1UpdateBatchManyUnchangedField(i)] = templateV1UpdateBatchManyUnchangedValue(i)
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

func templateV1UpdateBatchManyUnchangedRoots(tb testing.TB, d *backenddb.DB, indexCount int) map[string]uint64 {
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
		names = append(names, collectionSecondaryRootName("docs", templateV1UpdateBatchManyUnchangedField(i)))
	}
	out := make(map[string]uint64, len(names))
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
		out[name] = rootID
	}
	return out
}

func templateV1UpdateBatchManyUnchangedRequireIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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

func templateV1UpdateBatchManyUnchangedField(i int) string {
	return fmt.Sprintf("f%02d", i)
}

func templateV1UpdateBatchManyUnchangedValue(i int) string {
	return fmt.Sprintf("v%02d", i)
}
