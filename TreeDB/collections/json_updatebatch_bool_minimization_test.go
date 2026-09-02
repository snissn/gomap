package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateBatchJSONBoolSameEffectiveSkipsSecondaryRoot(t *testing.T) {
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
		[]byte(`{"active":true,"note":"before"}`),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonUpdateBatchBoolRootIDs(t, d)
	assertJSONUpdateBatchBoolIDs(t, col, true, "u1")

	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"active":true,"note":"same-active"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch JSON same-active update: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("same-active results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same-active secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same-active index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("same-active index unchanged=%d want %d", got, want)
	}

	afterSame := jsonUpdateBatchBoolRootIDs(t, d)
	if rootName := collectionPrimaryRootName("flags"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for unchanged JSON bool index value", rootName, before[rootName], afterSame[rootName])
		}
	}
	assertJSONUpdateBatchBoolIDs(t, col, true, "u1")

	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"active":false,"note":"changed-active"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch JSON changed-active update: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("changed-active results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.SecondaryDeleteEntries, 1; got != want {
		t.Fatalf("changed-active secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 1; got != want {
		t.Fatalf("changed-active secondary sets=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("changed-active index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 0; got != want {
		t.Fatalf("changed-active index unchanged=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("changed-active secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "active" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("active secondary run stats=%+v want active delete/set work with key bytes", run)
	}

	afterChanged := jsonUpdateBatchBoolRootIDs(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
	} {
		if afterChanged[rootName] == afterSame[rootName] {
			t.Fatalf("root %q did not change for changed JSON bool index value", rootName)
		}
	}
	assertJSONUpdateBatchBoolIDs(t, col, true)
	assertJSONUpdateBatchBoolIDs(t, col, false, "u1")
}

func jsonUpdateBatchBoolRootIDs(t *testing.T, d *backenddb.DB) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	catalog, err := loadCollectionCatalog(snap, "flags")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("flags"),
		collectionIndexStateRootName("flags"),
		collectionSecondaryRootName("flags", "active"),
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

func assertJSONUpdateBatchBoolIDs(t *testing.T, col *Collection, active bool, want ...string) {
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
