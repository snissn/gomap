package collections

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"

	treedb "github.com/snissn/gomap/TreeDB"
)

// The same ordinary APIs must backfill captured physical plus pending primary
// rows and keep old text cuts usable after the current declaration is dropped.
func TestNativeTextPublicDDL(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		for _, route := range []string{"legacy", "native"} {
			t.Run(string(version)+"/"+route, func(t *testing.T) {
				dir := t.TempDir()
				var manager *CollectionManager
				var closeOwner func() error
				var checkpoint func() error
				open := func() {
					t.Helper()
					opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
					opts.MemtableMode = "cow_btree"
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					if route == "native" {
						owner, err := treedb.Open(opts)
						if err != nil {
							t.Fatal(err)
						}
						closeOwner, checkpoint = owner.Close, owner.Checkpoint
						backend, err := owner.NativeSemanticBackend()
						if err != nil {
							t.Fatal(err)
						}
						manager = NewCollectionManager(backend)
					} else {
						backend, close, err := treedb.OpenBackendWithCachedLeafLog(opts)
						if err != nil {
							t.Fatal(err)
						}
						closeOwner, checkpoint = close, backend.Checkpoint
						manager = NewCollectionManager(backend)
					}
				}
				close := func() {
					t.Helper()
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
				}
				defer close()
				open()
				vector := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
				if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}, VectorIndexes: []VectorIndexDefinition{vector}}); err != nil {
					t.Fatal(err)
				}
				c, err := manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = c.Insert([]byte("a"), []byte(`{"content":"alpha beta","tenant":"one","embedding":[1,0]}`)); err != nil {
					t.Fatal(err)
				}
				if err = checkpoint(); err != nil {
					t.Fatal(err)
				}
				if _, err = c.Insert([]byte("b"), []byte(`{"content":"alpha gamma","tenant":"two","embedding":[0,1]}`)); err != nil {
					t.Fatal(err)
				}
				idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(vector))
				if err != nil {
					t.Fatal(err)
				}
				def := TextIndexDefinition{Name: "content", Version: version, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: version == TextIndexVersionV2}
				meta, stats, err := c.CreateTextIndex(def)
				if err != nil {
					t.Fatalf("ordinary CreateTextIndex with seeded primary: %v", err)
				}
				if len(meta.TextIndexes) != 1 || stats.DocumentsScanned != 2 {
					t.Fatalf("backfill metadata=%+v stats=%+v", meta, stats)
				}
				requireNativeTextIDs(t, c, "alpha", "a", "b")
				if route == "native" {
					// Exercise the existing shared mutation owner: an unrelated text
					// family changes schema while the ordinary facade reconciles HNSW.
					done := make(chan error, 2)
					go func() {
						other := def
						other.Name = "secondary"
						for i := 0; i < 3; i++ {
							if _, _, e := c.CreateTextIndex(other); e != nil {
								done <- fmt.Errorf("concurrent text create round=%d: %w", i, e)
								return
							}
							if _, e := c.DropTextIndex(other.Name); e != nil {
								done <- fmt.Errorf("concurrent text drop round=%d: %w", i, e)
								return
							}
						}
						done <- nil
					}()
					go func() {
						for i := 0; i < 3; i++ {
							_, _, e := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
								return []byte(`{"content":"alpha beta","tenant":"one","embedding":[1,0]}`), true, nil
							})
							if e != nil {
								done <- fmt.Errorf("concurrent document update round=%d: %w", i, e)
								return
							}
						}
						done <- nil
					}()
					var concurrentErr error
					for i := 0; i < 2; i++ {
						concurrentErr = errors.Join(concurrentErr, <-done)
					}
					if concurrentErr != nil {
						t.Fatal(concurrentErr)
					}
				}
				if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2}); err != nil {
					t.Fatalf("held HNSW after text create: %v", err)
				}
				view, err := c.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				defer view.Close()
				if _, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
					return []byte(`{"content":"delta beta","tenant":"one","embedding":[1,0]}`), true, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err = c.Delete([]byte("b")); err != nil {
					t.Fatal(err)
				}
				requireNativeTextIDs(t, c, "delta", "a")
				requireNativeTextIDs(t, c, "alpha")
				if _, err = c.DropTextIndex("content"); err != nil {
					t.Fatal(err)
				}
				if _, err = c.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 8}); !errors.Is(err, ErrIndexNotFound) {
					t.Fatalf("current dropped text query: %v", err)
				}
				held, err := c.searchTextAtReadView(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 8}, textSearchResultFull, view)
				if err != nil {
					t.Fatal(err)
				}
				var ids []string
				for _, row := range held.Results {
					ids = append(ids, string(row.DocumentID))
				}
				sort.Strings(ids)
				if !reflect.DeepEqual(ids, []string{"a", "b"}) {
					t.Fatalf("old text cut after mutation/drop: %v", ids)
				}
				if err = view.Close(); err != nil {
					t.Fatal(err)
				}
				if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); err != nil {
					t.Fatalf("held HNSW after text drop: %v", err)
				}
				if _, err = idx.SaveSnapshot(); err != nil {
					if c.nativeJSON == nil {
						t.Fatal(err)
					}
					cut, captureErr := c.nativeJSON.db.CaptureNativeSemanticState()
					if captureErr == nil {
						if budget, ok := cut.NativeAllocator().(nativeHNSWAdmissionBudget); ok {
							t.Logf("save refusal budget: %+v limits=%+v", budget.Stats(), budget.Limits())
						}
						cut.Close()
					}
					t.Fatalf("held HNSW save after text DDL: %v (capture diagnostic=%v)", err, captureErr)
				}
				if err = checkpoint(); err != nil {
					t.Fatal(err)
				}
				close()
				open()
				c, err = manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				if len(c.Meta().TextIndexes) != 0 {
					t.Fatal("text drop did not survive reopen")
				}
				if _, stats, err = c.CreateTextIndex(def); err != nil || stats.DocumentsScanned != 1 {
					t.Fatalf("recreated backfill: %+v %v", stats, err)
				}
				requireNativeTextIDs(t, c, "delta", "a")
			})
		}
	}
}

// Pin each accepted setup cut, so unrelated prior-cut teardown cannot hide a
// refused operation's added receipts. Both create and drop refuse before any
// assignment; releasing only the blocker makes the identical operation succeed.
func TestNativeTextPublicDDLAdmissionCleanup(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			var pins []backenddb.NativeSemanticCapture
			var budget nativeHNSWAdmissionBudget
			defer func() {
				for i := len(pins) - 1; i >= 0; i-- {
					if e := pins[i].Close(); e != nil {
						t.Error(e)
					}
				}
				if e := manager.Close(); e != nil {
					t.Error(e)
				}
				if e := owner.Close(); e != nil {
					t.Error(e)
				}
				if budget != nil {
					stats := budget.Stats()
					if stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
						t.Errorf("full Close retained DDL receipts: %+v", stats)
					}
				}
			}()
			pin := func() {
				t.Helper()
				cut, e := backend.CaptureNativeSemanticState()
				if e != nil {
					t.Fatal(e)
				}
				pins = append(pins, cut)
			}
			pin()
			budget = pins[0].NativeAllocator().(nativeHNSWAdmissionBudget)
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
				t.Fatal(err)
			}
			pin()
			c, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Insert([]byte("a"), []byte(`{"content":"alpha beta"}`)); err != nil {
				t.Fatal(err)
			}
			pin()
			def := TextIndexDefinition{Name: "content", Version: version, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: version == TextIndexVersionV2}
			for _, drop := range []bool{false, true} {
				func() {
					read, e := c.nativeJSON.Capture(c.name)
					if e != nil {
						t.Fatal(e)
					}
					defer read.Close()
					beforeMeta := read.meta
					beforeSequence := read.capture.LogicalSequence()
					beforeLSN := backend.CommandWALNextLSN()
					captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
					blocker, e := read.capture.NativeAllocator().AdmitNativeBytes(0)
					if e != nil {
						t.Fatal(e)
					}
					defer blocker.Close()
					available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
					leave := captureBytes + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					if available < leave {
						t.Fatalf("insufficient setup budget %d < %d", available, leave)
					}
					if e = blocker.Resize(available - leave); e != nil {
						t.Fatal(e)
					}
					for attempt := 0; attempt < 3; attempt++ {
						before := budget.Stats()
						if drop {
							_, e = c.DropTextIndex(def.Name)
						} else {
							_, _, e = c.CreateTextIndex(def)
						}
						if !errors.Is(e, memtable.ErrCOWCapacity) || backenddb.CommitPublicationAccepted(e) {
							t.Fatalf("drop=%v attempt%d expected preassignment capacity refusal: %v", drop, attempt, e)
						}
						after := budget.Stats()
						// Producer preflight can retire checkpoint-owned receipts,
						// independently of pinned semantic cuts. Refusal must add
						// no retained private reservation or lease on any attempt.
						if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
							t.Fatalf("refused DDL added receipts: before=%+v after=%+v", before, after)
						}
					}
					blocker.Close()
					current, e := c.nativeJSON.Capture(c.name)
					if e != nil {
						t.Fatal(e)
					}
					defer current.Close()
					if current.capture.LogicalSequence() != beforeSequence || backend.CommandWALNextLSN() != beforeLSN ||
						current.collection.incarnation != read.collection.incarnation ||
						current.collection.documentGeneration != read.collection.documentGeneration ||
						current.collection.schema != read.collection.schema || !sameCollectionMeta(current.meta, beforeMeta) {
						t.Fatal("refused DDL changed logical/schema/incarnation/document/command identity")
					}
					for _, captured := range []*NativeJSONRead{read, current} {
						if got, e := captured.Get([]byte("a")); e != nil || string(got) != `{"content":"alpha beta"}` {
							t.Fatalf("refused DDL changed held/current primary: %s %v", got, e)
						}
					}
				}()
				if drop {
					if _, err = c.DropTextIndex(def.Name); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, _, err = c.CreateTextIndex(def); err != nil {
						t.Fatal(err)
					}
					requireNativeTextIDs(t, c, "alpha", "a")
				}
				pin()
			}
		})
	}
}

func TestNativeTextPublicDDLCommandWALGuard(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
			t.Run(string(version)+"/"+string(profile), func(t *testing.T) {
				owner, _, c := openNativeTextPublic(t, t.TempDir(), profile, true, version)
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				read, err := c.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()
				beforeLSN := backend.CommandWALNextLSN()
				def := read.meta.TextIndexes[0]
				def.Name = "other"
				if _, _, err = c.CreateTextIndex(def); !errors.Is(err, backenddb.ErrCommandWALRejected) {
					t.Fatalf("create guard: %v", err)
				}
				if _, err = c.DropTextIndex("content"); !errors.Is(err, backenddb.ErrCommandWALRejected) {
					t.Fatalf("drop guard: %v", err)
				}
				current, err := c.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				defer current.Close()
				if backend.CommandWALNextLSN() != beforeLSN || current.capture.LogicalSequence() != read.capture.LogicalSequence() || !sameCollectionMeta(current.meta, read.meta) {
					t.Fatal("profile refusal changed logical/schema/command identity")
				}
			})
		}
	}
}
