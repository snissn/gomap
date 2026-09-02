package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateBatchTemplateV1BoolSameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "flags",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
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
		mustTemplateV1UpdateBatchBoolDocument(t, true, "before"),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("flags"),
		collectionTemplateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
	}
	before := mustTemplateV1UpdateBatchBoolRootIDs(t, d, "flags", rootNames...)
	assertTemplateV1UpdateBatchBoolIDs(t, col, true, "u1")

	sameActiveDoc := mustTemplateV1UpdateBatchBoolDocument(t, true, "same-active")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return sameActiveDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update same template-v1 active: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("same-active update results=%+v want one matched modified row", results)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same-active update secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same-active update index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("same-active update index unchanged=%d want %d", got, want)
	}

	afterSame := mustTemplateV1UpdateBatchBoolRootIDs(t, d, "flags", rootNames...)
	if afterSame[collectionPrimaryRootName("flags")] == before[collectionPrimaryRootName("flags")] {
		t.Fatalf("primary root did not change for template-v1 replacement")
	}
	templateRoot := collectionTemplateRootName("flags")
	if afterSame[templateRoot] != before[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same template shape", before[templateRoot], afterSame[templateRoot])
	}
	activeRoot := collectionSecondaryRootName("flags", "active")
	if afterSame[activeRoot] != before[activeRoot] {
		t.Fatalf("active secondary root changed from %d to %d for same template-v1 bool value", before[activeRoot], afterSame[activeRoot])
	}
	assertTemplateV1UpdateBatchBoolIDs(t, col, true, "u1")

	changedActiveDoc := mustTemplateV1UpdateBatchBoolDocument(t, false, "changed-active")
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return changedActiveDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update changed template-v1 active: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("changed-active update results=%+v want one matched modified row", results)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.SecondaryDeleteEntries, 1; got != want {
		t.Fatalf("changed-active update secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 1; got != want {
		t.Fatalf("changed-active update secondary sets=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("changed-active update index changes=%d want %d", got, want)
	}
	afterChanged := mustTemplateV1UpdateBatchBoolRootIDs(t, d, "flags", rootNames...)
	if afterChanged[activeRoot] == afterSame[activeRoot] {
		t.Fatalf("active secondary root did not change for changed template-v1 bool value")
	}
	if afterChanged[templateRoot] != afterSame[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same shape active update", afterSame[templateRoot], afterChanged[templateRoot])
	}
	assertTemplateV1UpdateBatchBoolIDs(t, col, true)
	assertTemplateV1UpdateBatchBoolIDs(t, col, false, "u1")
}

func mustTemplateV1UpdateBatchBoolDocument(t *testing.T, active bool, note string) []byte {
	t.Helper()
	return mustTemplateV1Document(t, []string{"active", "note"}, []any{active, note})
}

func mustTemplateV1UpdateBatchBoolRootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
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

func assertTemplateV1UpdateBatchBoolIDs(t *testing.T, col *Collection, active bool, want ...string) {
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
