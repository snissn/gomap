package collections

import (
	"encoding/json"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateBatchSkipsUnchangedUniqueWhenNonUniqueChanges(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
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
		[][]byte{templateV1UpdateBatchUniqueSkipDoc(t, "ada@example.com", "hnl", "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1UpdateBatchUniqueSkipRoots(t, d)
	templateV1UpdateBatchUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1UpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "hnl", "u1")

	replacement := templateV1UpdateBatchUniqueSkipDoc(t, "ada@example.com", "sea", "city-changed")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch city-only template-v1 update: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("unique skips=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "city" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("city secondary run stats=%+v want city delete+set with key bytes", run)
	}

	after := templateV1UpdateBatchUniqueSkipRoots(t, d)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for template-v1 city-only update", rootName)
		}
	}
	for _, rootName := range []string{
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
	} {
		if after[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for unchanged template-v1 unique email", rootName, before[rootName], after[rootName])
		}
	}
	templateV1UpdateBatchUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1UpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "hnl")
	templateV1UpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "sea", "u1")
}

func templateV1UpdateBatchUniqueSkipDoc(tb testing.TB, email, city, note string) []byte {
	tb.Helper()
	raw, err := json.Marshal(struct {
		Email string `json:"email"`
		City  string `json:"city"`
		Note  string `json:"note"`
	}{
		Email: email,
		City:  city,
		Note:  note,
	})
	if err != nil {
		tb.Fatalf("marshal template-v1 document JSON: %v", err)
	}
	doc, err := EncodeTemplateV1DocumentJSON(raw)
	if err != nil {
		tb.Fatalf("encode template-v1 document: %v", err)
	}
	return doc
}

func templateV1UpdateBatchUniqueSkipRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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

func templateV1UpdateBatchUniqueSkipRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
