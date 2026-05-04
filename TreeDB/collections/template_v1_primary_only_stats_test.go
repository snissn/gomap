package collections

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1PrimaryOnlyDeltasExcludeTemplateRootWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, col *Collection, replacement []byte)
	}{
		{
			name: "direct",
			run: func(t *testing.T, col *Collection, replacement []byte) {
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
			},
		},
		{
			name: "batch",
			run: func(t *testing.T, col *Collection, replacement []byte) {
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
					DocumentFormat:               DocumentFormatTemplateV1,
					DisableIndexedWriteMemtables: true,
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
				[][]byte{templateV1PrimaryOnlyStatsDoc(t, `{"name":"ada"}`)},
			); err != nil {
				t.Fatalf("insert: %v", err)
			}

			before := mgr.StatsSnapshot()
			tc.run(t, col, templateV1PrimaryOnlyStatsDoc(t, `{"name":"ada","score":1}`))
			after := mgr.StatsSnapshot()

			if got, want := after.RootDeltaPlanPrimaryRoots-before.RootDeltaPlanPrimaryRoots, uint64(1); got != want {
				t.Fatalf("root-delta primary roots delta=%d want %d", got, want)
			}
			if got, want := after.RootDeltaPlanTemplateRoots-before.RootDeltaPlanTemplateRoots, uint64(1); got != want {
				t.Fatalf("root-delta template roots delta=%d want %d", got, want)
			}
			if got, want := after.PrimaryOnlyRootPublishes-before.PrimaryOnlyRootPublishes, uint64(1); got != want {
				t.Fatalf("primary-only publishes delta=%d want %d", got, want)
			}
			if got, want := after.PrimaryOnlyRootDeltaEntries-before.PrimaryOnlyRootDeltaEntries, uint64(1); got != want {
				t.Fatalf("primary-only root delta entries delta=%d want %d", got, want)
			}
			if got := after.RootDeltaPlanEntries - before.RootDeltaPlanEntries; got <= after.PrimaryOnlyRootDeltaEntries-before.PrimaryOnlyRootDeltaEntries {
				t.Fatalf("aggregate root-delta entries delta=%d should include template work beyond primary-only entries", got)
			}
			if got, want := after.PrimaryOnlyRootDeltaKeyBytes-before.PrimaryOnlyRootDeltaKeyBytes, uint64(len("u1")); got != want {
				t.Fatalf("primary-only root delta key bytes delta=%d want %d", got, want)
			}
			if got := after.PrimaryOnlyRootDeltaValueBytes - before.PrimaryOnlyRootDeltaValueBytes; got == 0 {
				t.Fatal("primary-only root delta value bytes delta=0 want positive")
			}
		})
	}
}

func templateV1PrimaryOnlyStatsDoc(tb testing.TB, rawJSON string) []byte {
	tb.Helper()
	doc, err := EncodeTemplateV1DocumentJSON([]byte(rawJSON))
	if err != nil {
		tb.Fatalf("encode template-v1 document %s: %v", rawJSON, err)
	}
	return doc
}
