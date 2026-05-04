package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateBatchJSONDoubleSameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "scores",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "score", Field: "score", ValueType: IndexValueDouble},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("scores")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{
		[]byte(`{"score":{"$numberDouble":"2.5"},"note":"before"}`),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonUpdateBatchDoubleRootIDs(t, d)
	assertJSONUpdateBatchDoubleIDs(t, col, 2.5, "u1")

	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"score":{"$numberDouble":"2.5"},"note":"same-score"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch JSON same-score update: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("same-score results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same-score secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same-score index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("same-score index unchanged=%d want %d", got, want)
	}

	afterSame := jsonUpdateBatchDoubleRootIDs(t, d)
	if rootName := collectionPrimaryRootName("scores"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for unchanged JSON double index value", rootName, before[rootName], afterSame[rootName])
		}
	}
	assertJSONUpdateBatchDoubleIDs(t, col, 2.5, "u1")

	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"score":{"$numberDouble":"3.5"},"note":"changed-score"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch JSON changed-score update: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("changed-score results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.SecondaryDeleteEntries, 1; got != want {
		t.Fatalf("changed-score secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 1; got != want {
		t.Fatalf("changed-score secondary sets=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("changed-score index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 0; got != want {
		t.Fatalf("changed-score index unchanged=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("changed-score secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "score" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("score secondary run stats=%+v want score delete/set work with key bytes", run)
	}

	afterChanged := jsonUpdateBatchDoubleRootIDs(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	} {
		if afterChanged[rootName] == afterSame[rootName] {
			t.Fatalf("root %q did not change for changed JSON double index value", rootName)
		}
	}
	assertJSONUpdateBatchDoubleIDs(t, col, 2.5)
	assertJSONUpdateBatchDoubleIDs(t, col, 3.5, "u1")
}

func jsonUpdateBatchDoubleRootIDs(t *testing.T, d *backenddb.DB) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	catalog, err := loadCollectionCatalog(snap, "scores")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("scores"),
		collectionIndexStateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
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

func assertJSONUpdateBatchDoubleIDs(t *testing.T, col *Collection, score float64, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue("score", score)
	if err != nil {
		t.Fatalf("find score %v: %v", score, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		t.Fatalf("score %v ids=%q want %q", score, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("score %v ids=%q want %q", score, ids, want)
		}
	}
}
