package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateChangesMultipleSecondaryRoots(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, col *Collection, replacement []byte) CollectionUpdateStats
	}{
		{
			name: "direct",
			run: func(t *testing.T, col *Collection, replacement []byte) CollectionUpdateStats {
				t.Helper()
				matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
					return replacement, true, nil
				})
				if err != nil {
					t.Fatalf("direct update: %v", err)
				}
				if !matched || !modified {
					t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
				}
				return col.LastUpdateStats()
			},
		},
		{
			name: "batch",
			run: func(t *testing.T, col *Collection, replacement []byte) CollectionUpdateStats {
				t.Helper()
				results, err := col.UpdateBatch([]UpdateBatchItem{{
					DocumentID: []byte("u1"),
					Update: func([]byte) ([]byte, bool, error) {
						return replacement, true, nil
					},
				}})
				if err != nil {
					t.Fatalf("UpdateBatch: %v", err)
				}
				if len(results) != 1 || !results[0].Matched || !results[0].Modified {
					t.Fatalf("UpdateBatch results=%+v want one matched modified result", results)
				}
				return col.LastUpdateStats()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "users",
				Options: CollectionOptions{
					DisableIndexedWriteMemtables: true,
				},
				Indexes: []IndexDefinition{
					{Name: "email", Field: "email", ValueType: IndexValueString},
					{Name: "city", Field: "city", ValueType: IndexValueString},
					{Name: "active", Field: "active", ValueType: IndexValueBool},
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
				[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","active":true,"note":"old"}`)},
			); err != nil {
				t.Fatalf("insert: %v", err)
			}

			rootNames := []string{
				collectionPrimaryRootName("users"),
				collectionIndexStateRootName("users"),
				collectionSecondaryRootName("users", "email"),
				collectionSecondaryRootName("users", "city"),
				collectionSecondaryRootName("users", "active"),
			}
			before := multiSecondaryRootIDs(t, d, "users", rootNames...)
			stats := tc.run(t, col, []byte(`{"email":"grace@example.com","city":"sea","active":true,"note":"new"}`))
			if got, want := stats.IndexValueChanges, 2; got != want {
				t.Fatalf("changed indexes=%d want %d", got, want)
			}
			if got, want := stats.IndexValueUnchanged, 1; got != want {
				t.Fatalf("unchanged indexes=%d want %d", got, want)
			}
			if got, want := len(stats.SecondaryRuns), 2; got != want {
				t.Fatalf("secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
			}
			if stats.SecondaryRuns[0].IndexName != "city" || stats.SecondaryRuns[1].IndexName != "email" {
				t.Fatalf("secondary run order=%+v want deterministic root order city, email", stats.SecondaryRuns)
			}
			multiSecondaryRequireRun(t, stats.SecondaryRuns, "email")
			multiSecondaryRequireRun(t, stats.SecondaryRuns, "city")

			after := multiSecondaryRootIDs(t, d, "users", rootNames...)
			for _, rootName := range []string{
				collectionPrimaryRootName("users"),
				collectionIndexStateRootName("users"),
				collectionSecondaryRootName("users", "email"),
				collectionSecondaryRootName("users", "city"),
			} {
				if after[rootName] == before[rootName] {
					t.Fatalf("root %q did not change for multi-secondary update", rootName)
				}
			}
			activeRoot := collectionSecondaryRootName("users", "active")
			if after[activeRoot] != before[activeRoot] {
				t.Fatalf("unchanged active root changed from %d to %d", before[activeRoot], after[activeRoot])
			}

			multiSecondaryRequireIndexIDs(t, col, "email", "ada@example.com")
			multiSecondaryRequireIndexIDs(t, col, "email", "grace@example.com", "u1")
			multiSecondaryRequireIndexIDs(t, col, "city", "hnl")
			multiSecondaryRequireIndexIDs(t, col, "city", "sea", "u1")
			multiSecondaryRequireIndexIDs(t, col, "active", true, "u1")
		})
	}
}

func multiSecondaryRootIDs(tb testing.TB, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatalf("missing catalog for collection %q", collectionName)
	}
	roots := make(map[string]uint64, len(rootNames))
	for _, rootName := range rootNames {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", rootName)
		}
		roots[rootName] = rootID
	}
	return roots
}

func multiSecondaryRequireRun(tb testing.TB, runs []CollectionUpdateSecondaryRunStats, indexName string) {
	tb.Helper()
	for _, run := range runs {
		if run.IndexName == indexName {
			if run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
				tb.Fatalf("secondary run %q=%+v want one delete, one set, and key bytes", indexName, run)
			}
			return
		}
	}
	tb.Fatalf("missing secondary run %q in %+v", indexName, runs)
}

func multiSecondaryRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
