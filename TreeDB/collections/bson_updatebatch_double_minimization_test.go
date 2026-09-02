package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionUpdateBatchBSONDoubleSameEffectiveSkipsSecondaryRoot(t *testing.T) {
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
			DocumentFormat:               DocumentFormatBSON,
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

	const (
		oldScore = 2.5
		newScore = 3.5
	)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "score", Value: oldScore},
			{Key: "note", Value: "before"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	}
	before := mustBSONUpdateBatchDoubleRootIDs(t, d, "scores", rootNames...)
	assertBSONUpdateBatchDoubleIDs(t, col, oldScore, "u1")

	sameScoreDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "score", Value: oldScore},
		{Key: "note", Value: "same-score"},
	})
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return sameScoreDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update same BSON score: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("same-score update results=%+v want one matched modified row", results)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same-score update secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same-score update index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("same-score update index unchanged=%d want %d", got, want)
	}

	afterSame := mustBSONUpdateBatchDoubleRootIDs(t, d, "scores", rootNames...)
	if afterSame[collectionPrimaryRootName("scores")] == before[collectionPrimaryRootName("scores")] {
		t.Fatalf("primary root did not change for BSON replacement")
	}
	scoreRoot := collectionSecondaryRootName("scores", "score")
	if afterSame[scoreRoot] != before[scoreRoot] {
		t.Fatalf("score secondary root changed from %d to %d for same BSON double value", before[scoreRoot], afterSame[scoreRoot])
	}
	assertBSONUpdateBatchDoubleIDs(t, col, oldScore, "u1")

	changedScoreDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "score", Value: newScore},
		{Key: "note", Value: "changed-score"},
	})
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return changedScoreDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update changed BSON score: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("changed-score update results=%+v want one matched modified row", results)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.SecondaryDeleteEntries, 1; got != want {
		t.Fatalf("changed-score update secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 1; got != want {
		t.Fatalf("changed-score update secondary sets=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("changed-score update index changes=%d want %d", got, want)
	}
	afterChanged := mustBSONUpdateBatchDoubleRootIDs(t, d, "scores", rootNames...)
	if afterChanged[scoreRoot] == afterSame[scoreRoot] {
		t.Fatalf("score secondary root did not change for changed BSON double value")
	}
	assertBSONUpdateBatchDoubleIDs(t, col, oldScore)
	assertBSONUpdateBatchDoubleIDs(t, col, newScore, "u1")
}

func mustBSONUpdateBatchDoubleRootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
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

func assertBSONUpdateBatchDoubleIDs(t *testing.T, col *Collection, score float64, want ...string) {
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
