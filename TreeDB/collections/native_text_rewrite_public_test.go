package collections

import (
	"context"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Identical public debt, held-cut, policy and reopen lifecycle on both owners.
func TestNativeTextRewritePublicLifecycle(t *testing.T) {
	for _, native := range []bool{false, true} {
		name := "legacy"
		if native {
			name = "native"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var manager *CollectionManager
			var closeOwner, checkpoint func() error
			var budget nativeHNSWAdmissionBudget
			closeAll := func() {
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
					manager = nil
				}
				if closeOwner != nil {
					if err := closeOwner(); err != nil {
						t.Error(err)
					}
					closeOwner = nil
				}
				if budget != nil {
					if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
						t.Errorf("full Close retained %+v", s)
					}
					budget = nil
				}
			}
			t.Cleanup(closeAll)
			open := func() {
				opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
				opts.MemtableMode = "cow_btree"
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				if native {
					d, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					closeOwner, checkpoint = d.Close, d.Checkpoint
					b, err := d.NativeSemanticBackend()
					if err != nil {
						t.Fatal(err)
					}
					manager = NewCollectionManager(b)
					cut, err := b.CaptureNativeSemanticState()
					if err != nil {
						t.Fatal(err)
					}
					budget = cut.NativeAllocator().(nativeHNSWAdmissionBudget)
					if err = cut.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					d, close, err := treedb.OpenBackendWithCachedLeafLog(opts)
					if err != nil {
						t.Fatal(err)
					}
					manager, closeOwner, checkpoint = NewCollectionManager(d), close, d.Checkpoint
				}
			}
			open()
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.InsertBatch([][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{[]byte(`{"body":"refund old keep"}`), []byte(`{"body":"refund delete keep"}`), []byte(`{"body":"shipping keep"}`)}); err != nil {
				t.Fatal(err)
			}
			if _, _, err = c.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
				t.Fatal(err)
			}
			view, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			if _, _, err = c.Update([]byte("d1"), func([]byte) ([]byte, bool, error) { return []byte(`{"body":"updated keep"}`), true, nil }); err != nil {
				t.Fatal(err)
			}
			if err = c.Delete([]byte("d2")); err != nil {
				t.Fatal(err)
			}
			if _, err = c.Insert([]byte("d4"), []byte(`{"body":"refund refund live"}`)); err != nil {
				t.Fatal(err)
			}
			before, err := c.TextIndexStorageStats("lexical")
			if err != nil {
				t.Fatal(err)
			}
			if before.V2DeletedDocs != 1 || before.V2MicroPostingBlocks == 0 {
				t.Fatalf("debt=%+v", before)
			}
			current, err := c.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
			if err != nil {
				t.Fatal(err)
			}
			var generation uint64
			if native {
				r, e := c.nativeJSON.Capture("docs")
				if e != nil {
					t.Fatal(e)
				}
				generation = r.collection.documentGeneration
				if e = r.Close(); e != nil {
					t.Fatal(e)
				}
			}
			if native {
				for _, call := range []func() error{
					func() error {
						_, e := c.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Force: true, RunStorageCompaction: true})
						return e
					},
					func() error {
						_, e := c.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{Force: true, RunStorageCompaction: true})
						return e
					},
					func() error {
						_, e := manager.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{Force: true, RunStorageCompaction: true})
						return e
					},
				} {
					if e := call(); !errors.Is(e, backenddb.ErrNativeSemanticUnsupported) {
						t.Fatalf("native full compaction=%v", e)
					}
				}
				if _, _, e := c.CreateTextIndex(TextIndexDefinition{Name: "v1", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); e != nil {
					t.Fatal(e)
				}
				if _, e := c.RewriteTextIndex("v1", TextIndexRewriteOptions{}); !errors.Is(e, ErrTextIndexUnavailable) {
					t.Fatalf("V1 rewrite=%v", e)
				}
				if e := func() error {
					r, e := c.nativeJSON.Capture("docs")
					if e != nil {
						return e
					}
					defer r.Close()
					blocker, e := r.capture.NativeAllocator().AdmitNativeBytes(0)
					if e != nil {
						return e
					}
					defer blocker.Close()
					available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
					if e = blocker.Resize(available); e != nil {
						return e
					}
					_, e = c.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
					if !errors.Is(e, memtable.ErrCOWCapacity) {
						t.Fatalf("finite public rewrite=%v", e)
					}
					return nil
				}(); e != nil {
					t.Fatal(e)
				}
			}
			// Bounded/canceled/dry-run planning must leave the original status intact.
			bounded, err := c.RewriteTextIndex("lexical", TextIndexRewriteOptions{MaxTerms: 1})
			if err != nil || !bounded.Noop || !bounded.BudgetExhausted {
				t.Fatalf("bounded=%+v err=%v", bounded, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = c.MaintainTextIndex(ctx, "lexical", TextIndexMaintenanceOptions{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
			dry, err := c.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Force: true, DryRun: true})
			if err != nil || dry.IndexesRewritten != 0 {
				t.Fatalf("dry=%+v %v", dry, err)
			}
			unchanged, err := c.TextIndexStorageStats("lexical")
			if err != nil || unchanged.V2RootGeneration != before.V2RootGeneration {
				t.Fatalf("planning changed storage=%+v %v", unchanged, err)
			}
			stats, err := c.RewriteTextIndex("lexical", TextIndexRewriteOptions{TargetPostingsPerBlock: 8})
			if err != nil {
				t.Fatalf("public RewriteTextIndex: %v", err)
			}
			if stats.Noop || stats.StalePostingsPurged == 0 || stats.TombstoneDocIDEntriesPurged != 1 || stats.RootGenerationAfter != stats.RootGenerationBefore+1 {
				t.Fatalf("rewrite=%+v", stats)
			}
			after, err := c.TextIndexStorageStats("lexical")
			if err != nil || after.V2DeletedDocs != 0 || after.V2MicroPostingBlocks != 0 || after.V2PostingBlocks >= before.V2PostingBlocks {
				t.Fatalf("after=%+v %v", after, err)
			}
			got, err := c.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Results, current.Results) {
				t.Fatalf("results/scores changed: before=%+v after=%+v", current.Results, got.Results)
			}
			old, err := c.searchTextAtReadView(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10}, textSearchResultFull, view)
			if err != nil || !slicesEqualStrings(textSearchResultIDs2630(old), []string{"d1", "d2"}) {
				t.Fatalf("held cut=%+v %v", old, err)
			}
			if err = view.Close(); err != nil {
				t.Fatal(err)
			}
			if native {
				r, e := c.nativeJSON.Capture("docs")
				if e != nil {
					t.Fatal(e)
				}
				if r.collection.documentGeneration != generation {
					t.Fatal("rewrite advanced document generation")
				}
				if e = r.Close(); e != nil {
					t.Fatal(e)
				}
			}
			maintained, err := manager.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{})
			if err != nil || maintained.IndexesRewritten != 0 || maintained.IndexesScanned != 1 {
				t.Fatalf("Maintain=%+v %v", maintained, err)
			}
			if err = checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeAll()
			open()
			c, err = manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := c.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
			if err != nil || !reflect.DeepEqual(reopened.Results, current.Results) {
				t.Fatalf("reopen=%+v err=%v", reopened.Results, err)
			}
		})
	}
}

func TestNativeTextRewriteCommandWALGuard(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			b, err := d.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			m := NewCollectionManager(b)
			defer func() {
				if err := m.Close(); err != nil {
					t.Error(err)
				}
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()
			if _, err = m.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
				t.Fatal(err)
			}
			c, err := m.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			seq, lsn := r.capture.LogicalSequence(), b.CommandWALNextLSN()
			if _, err = c.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
				t.Fatalf("rewrite=%v", err)
			}
			if _, err = c.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
				t.Fatalf("Maintain=%v", err)
			}
			after, err := c.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			defer after.Close()
			if after.capture.LogicalSequence() != seq || b.CommandWALNextLSN() != lsn || after.collection != r.collection {
				t.Fatal("CWAL refusal changed original identity/WAL")
			}
		})
	}
}
