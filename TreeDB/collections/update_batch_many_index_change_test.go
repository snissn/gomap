package collections

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateBatchChangesOneIndexAmongMany(t *testing.T) {
	const indexCount = 32
	const changedIndex = 17

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	indexes := make([]IndexDefinition, indexCount)
	for i := range indexes {
		field := updateBatchManyIndexField(i)
		indexes[i] = IndexDefinition{Name: field, Field: field, ValueType: IndexValueString}
	}
	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
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
		[][]byte{updateBatchManyIndexDoc(t, indexCount, -1, "", "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := updateBatchManyIndexRoots(t, d, indexCount)
	for i := range indexes {
		field := updateBatchManyIndexField(i)
		updateBatchManyIndexRequireIndexIDs(t, col, field, updateBatchManyIndexValue(i), "d1")
	}

	changedField := updateBatchManyIndexField(changedIndex)
	changedValue := "changed-17"
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return updateBatchManyIndexDoc(t, indexCount, changedIndex, changedValue, "one-index-changed"), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch one indexed field: %v", err)
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

	after := updateBatchManyIndexRoots(t, d, indexCount)
	for _, rootName := range []string{
		collectionPrimaryRootName("docs"),
		collectionIndexStateRootName("docs"),
		collectionSecondaryRootName("docs", changedField),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for one-index UpdateBatch", rootName)
		}
	}
	for i := range indexes {
		if i == changedIndex {
			continue
		}
		rootName := collectionSecondaryRootName("docs", updateBatchManyIndexField(i))
		if after[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d when only %s changed", rootName, before[rootName], after[rootName], changedField)
		}
	}
	updateBatchManyIndexRequireIndexIDs(t, col, changedField, updateBatchManyIndexValue(changedIndex))
	updateBatchManyIndexRequireIndexIDs(t, col, changedField, changedValue, "d1")
	for i := range indexes {
		if i == changedIndex {
			continue
		}
		field := updateBatchManyIndexField(i)
		updateBatchManyIndexRequireIndexIDs(t, col, field, updateBatchManyIndexValue(i), "d1")
	}
}

func updateBatchManyIndexDoc(tb testing.TB, indexCount, changedIndex int, changedValue, note string) []byte {
	tb.Helper()
	var b strings.Builder
	b.WriteString(`{`)
	for i := 0; i < indexCount; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		value := updateBatchManyIndexValue(i)
		if i == changedIndex {
			value = changedValue
		}
		fmt.Fprintf(&b, "%q:%q", updateBatchManyIndexField(i), value)
	}
	fmt.Fprintf(&b, `,"note":%q}`, note)
	return []byte(b.String())
}

func updateBatchManyIndexRoots(tb testing.TB, d *backenddb.DB, indexCount int) map[string]uint64 {
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
	roots := make(map[string]uint64, indexCount+2)
	names := []string{
		collectionPrimaryRootName("docs"),
		collectionIndexStateRootName("docs"),
	}
	for i := 0; i < indexCount; i++ {
		names = append(names, collectionSecondaryRootName("docs", updateBatchManyIndexField(i)))
	}
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
		roots[name] = rootID
	}
	return roots
}

func updateBatchManyIndexRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	wantSorted := append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(wantSorted)
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}

func updateBatchManyIndexField(i int) string {
	return fmt.Sprintf("f%02d", i)
}

func updateBatchManyIndexValue(i int) string {
	return fmt.Sprintf("v%02d", i)
}
