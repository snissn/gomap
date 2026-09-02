package collections

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateBatchChangesOneIndexAmongMany(t *testing.T) {
	const indexCount = 65
	const changedIndex = 64

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	indexes := make([]IndexDefinition, indexCount)
	for i := range indexes {
		field := templateV1UpdateBatchManyIndexField(i)
		indexes[i] = IndexDefinition{Name: field, Field: field, ValueType: IndexValueString}
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
		[][]byte{templateV1UpdateBatchManyIndexDoc(t, indexCount, -1, "", "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1UpdateBatchManyIndexRoots(t, d, indexCount)
	for i := range indexes {
		field := templateV1UpdateBatchManyIndexField(i)
		templateV1UpdateBatchManyIndexRequireIDs(t, col, field, templateV1UpdateBatchManyIndexValue(i), "d1")
	}

	changedField := templateV1UpdateBatchManyIndexField(changedIndex)
	changedValue := "changed-13"
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return templateV1UpdateBatchManyIndexDoc(t, indexCount, changedIndex, changedValue, "one-index-changed"), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch one template-v1 indexed field: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, indexCount-1; got != want {
		t.Fatalf("index unchanged=%d want %d", got, want)
	}
	if got := stats.MaskFallbacks; got == 0 {
		t.Fatalf("fast-mask fallbacks=%d want positive for changed index ordinal %d", got, changedIndex)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != changedField || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("changed index secondary run=%+v want %s delete+set with key bytes", run, changedField)
	}
	if stats.SecondaryDeleteEntries != 1 || stats.SecondarySetEntries != 1 || stats.SecondaryKeyBytes == 0 {
		t.Fatalf("secondary aggregate stats deletes=%d sets=%d bytes=%d want one delete+set with key bytes",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes)
	}

	after := templateV1UpdateBatchManyIndexRoots(t, d, indexCount)
	for _, rootName := range []string{
		collectionPrimaryRootName("docs"),
		collectionSecondaryRootName("docs", changedField),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for one-index template-v1 UpdateBatch", rootName)
		}
	}
	templateRoot := collectionTemplateRootName("docs")
	if after[templateRoot] != before[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same-shape template-v1 update", before[templateRoot], after[templateRoot])
	}
	for i := range indexes {
		if i == changedIndex {
			continue
		}
		rootName := collectionSecondaryRootName("docs", templateV1UpdateBatchManyIndexField(i))
		if after[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d when only %s changed", rootName, before[rootName], after[rootName], changedField)
		}
	}
	templateV1UpdateBatchManyIndexRequireIDs(t, col, changedField, templateV1UpdateBatchManyIndexValue(changedIndex))
	templateV1UpdateBatchManyIndexRequireIDs(t, col, changedField, changedValue, "d1")
	for i := range indexes {
		if i == changedIndex {
			continue
		}
		field := templateV1UpdateBatchManyIndexField(i)
		templateV1UpdateBatchManyIndexRequireIDs(t, col, field, templateV1UpdateBatchManyIndexValue(i), "d1")
	}
}

func templateV1UpdateBatchManyIndexDoc(tb testing.TB, indexCount, changedIndex int, changedValue, note string) []byte {
	tb.Helper()
	doc := map[string]string{"note": note}
	for i := 0; i < indexCount; i++ {
		value := templateV1UpdateBatchManyIndexValue(i)
		if i == changedIndex {
			value = changedValue
		}
		doc[templateV1UpdateBatchManyIndexField(i)] = value
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

func templateV1UpdateBatchManyIndexRoots(tb testing.TB, d *backenddb.DB, indexCount int) map[string]uint64 {
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
		names = append(names, collectionSecondaryRootName("docs", templateV1UpdateBatchManyIndexField(i)))
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

func templateV1UpdateBatchManyIndexRequireIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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

func templateV1UpdateBatchManyIndexField(i int) string {
	return fmt.Sprintf("f%02d", i)
}

func templateV1UpdateBatchManyIndexValue(i int) string {
	return fmt.Sprintf("v%02d", i)
}
