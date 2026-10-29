package collections

import (
	"context"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

// Exercise the real transaction finalizer, including its admitted output. The
// history representation should depend on changed domains, not text key count.
func TestNativeTextDomainEffects(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) {
			_, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true, version)
			meta := nativeTextPublicMeta(version)
			meta.Name = "pair"
			meta.TextIndexes = append(meta.TextIndexes, TextIndexDefinition{Name: "topic", Version: version, Fields: []TextIndexField{{Field: "topic"}}, Analyzer: TextAnalyzerSimple, StorePositions: true})
			if _, err := manager.CreateCollection(meta); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			txn, err := c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer txn.Close()
			var ids, docs [][]byte
			for i := 0; i < 8; i++ {
				ids = append(ids, []byte(fmt.Sprintf("d%02d", i)))
				docs = append(docs, []byte(`{"tenant":"a","content":"alpha beta gamma delta epsilon policy","topic":"one two three four five"}`))
			}
			if err = txn.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if err = txn.finalizeNativeRowsWithStats(false, nil); err != nil {
				t.Fatal(err)
			}
			g := txn.binding(c.name)
			domains := make(map[uint64]int)
			entries := 0
			for _, change := range g.plan.additional {
				domain := nativeTextDomainNumber(g.probe.meta, change.name)
				if domain != 0 && len(change.entries) != 0 {
					domains[domain] = 0
					entries += len(change.entries)
				}
			}
			if len(domains) < 6 || entries <= len(domains) {
				t.Fatalf("fixture did not exercise both multi-key text indexes: domains=%d entries=%d", len(domains), entries)
			}
			primaryPoints := 0
			for _, effect := range g.effects {
				if _, text := domains[effect.Namespace.Domain]; !text {
					if effect.Kind == txnhistory.PointEffect {
						primaryPoints++
					}
					continue
				}
				if effect.Namespace.Collection != g.incarnation || effect.Namespace.Interpretation != 1 || effect.Kind != txnhistory.RangeEffect || effect.Key != nil || effect.End != nil {
					t.Fatalf("text effect must cover its complete original domain: %+v", effect)
				}
				domains[effect.Namespace.Domain]++
			}
			for domain, count := range domains {
				if count != 1 {
					t.Errorf("domain %d has %d effects, want one", domain, count)
				}
			}
			if primaryPoints < len(ids) {
				t.Fatalf("ordinary non-text point effects were lost: %d", primaryPoints)
			}
		})
	}
}

// Decide is the maintained rewrite planner's existing decision boundary: it
// lets the test publish a row after rewrite captures/plans, before publication.
func TestNativeTextDomainEffectsRewriteConflict(t *testing.T) {
	for _, rowFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("row_first_%t", rowFirst), func(t *testing.T) {
			_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			doc := []byte(`{"tenant":"a","content":"shared policy"}`)
			for _, id := range []string{"a", "b"} {
				if _, err := c.Insert([]byte(id), doc); err != nil {
					t.Fatal(err)
				}
			}
			row, err := c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer row.Close()
			if err = row.InsertBatch([][]byte{[]byte("c")}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			decided := false
			run := textV2RewriteRunOptions{Rewrite: TextIndexRewriteOptions{TargetPostingsPerBlock: 8}, Decide: func(_ TextIndexStorageStats, _ TextIndexRewriteStats) textV2RewriteDecision {
				decided = true
				if rowFirst {
					if err := row.Commit(); err != nil {
						t.Fatalf("row before captured rewrite: %v", err)
					}
				}
				return textV2RewriteDecision{Apply: true}
			}}
			_, _, _, rewriteErr := c.nativeRewriteTextIndex(context.Background(), "content", run)
			if !decided {
				t.Fatal("rewrite did not reach its captured publication decision")
			}
			if rowFirst {
				if !errors.Is(rewriteErr, backenddb.ErrConcurrentModification) {
					t.Fatalf("stale rewrite not fenced by row: %v", rewriteErr)
				}
				requireNativeTextIDs(t, c, "shared", "a", "b", "c")
			} else {
				if rewriteErr != nil {
					t.Fatal(rewriteErr)
				}
				if err := row.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
					t.Fatalf("stale row not fenced by rewrite: %v", err)
				}
				requireNativeTextIDs(t, c, "shared", "a", "b")
				if _, err := c.Insert([]byte("c"), doc); err != nil {
					t.Fatalf("fresh row after refusal: %v", err)
				}
			}
		})
	}
}

func TestNativeTextDomainEffectsCollectionIsolation(t *testing.T) {
	_, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	meta := nativeTextPublicMeta()
	meta.Name = "other"
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	a, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := c.nativeJSON.Begin(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	doc := []byte(`{"tenant":"a","content":"shared policy"}`)
	for _, txn := range []*NativeJSONTxn{a, b} {
		if err := txn.InsertBatch([][]byte{[]byte("same-id")}, [][]byte{doc}); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("different collection's text domain conflicted: %v", err)
		}
	}
}
