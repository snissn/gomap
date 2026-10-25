package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func nativeHybridRequest() HybridSearchOptions {
	return HybridSearchOptions{TopK: 3, Text: &HybridTextQuery{IndexName: "content", Query: "alpha", CandidateLimit: 8, IncludeTextMatches: true}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "a"}, IncludeDocuments: true, DocumentFetchOptions: DocumentFetchOptions{IncludePaths: []string{"content", "tenant"}}, Debug: HybridSearchDebugOptions{IncludeCandidates: true}}
}

func requireNativeHybridResult(t *testing.T, c *Collection, opts HybridSearchOptions, want ...string) HybridSearchResponse {
	t.Helper()
	response, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(response.Results))
	for _, r := range response.Results {
		ids = append(ids, string(r.ID))
		if opts.IncludeDocuments {
			var document map[string]any
			if !r.DocumentFound || json.Unmarshal(r.Document, &document) != nil || document["content"] == nil || document["embedding"] != nil {
				t.Fatalf("projection/materialization: %+v", r)
			}
		}
	}
	if want == nil {
		want = []string{}
	}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("ids=%v want=%v", ids, want)
	}
	if !response.Snapshot.Native || response.Snapshot.CollectionIncarnation == 0 || response.Snapshot.CollectionGeneration != 0 || response.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("native identity/scan: %+v %+v", response.Snapshot, response.Stats)
	}
	return response
}

// Real public JSON owners preserve V1/V2 lexical scoring/filtering and one-cut
// final projection, including a cold physical import with logical sequence zero.
func TestNativeHybridPublicLifecycle(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
			t.Run(string(version)+"/"+string(profile), func(t *testing.T) {
				dir := t.TempDir()
				owner, manager, c := openNativeTextPublic(t, dir, profile, true, version)
				if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha beta","embedding":[1,0]}`), []byte(`{"tenant":"b","content":"alpha beta","embedding":[0,1]}`), []byte(`{"tenant":"c","content":"gamma","embedding":[1,0]}`)}); err != nil {
					t.Fatal(err)
				}
				old, err := c.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				opts := nativeHybridRequest()
				opening := requireNativeHybridResult(t, c, opts, "a")
				ownedDocument := bytes.Clone(opening.Results[0].Document)
				if cap(opening.Results[0].Document) != len(opening.Results[0].Document) {
					t.Fatal("document capacity not limited")
				}
				appended := append(opening.Results[0].Document, '!')
				appended[0] = '!'
				if !bytes.Equal(opening.Results[0].Document, ownedDocument) {
					t.Fatal("append mutated transferred document")
				}
				opts.ScalarFilter = &HybridScalarFilter{IndexName: "tenant", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "a", Inclusive: true}, Upper: IndexRangeBound{Value: "b", Inclusive: true}}}
				requireNativeHybridResult(t, c, opts, "a", "b")
				opts.ScalarFilter = &HybridScalarFilter{And: []HybridScalarFilter{*opts.ScalarFilter, {IndexName: "tenant", Value: "a"}}}
				requireNativeHybridResult(t, c, opts, "a")
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				after := requireNativeHybridResult(t, c, opts, "a")
				if after.Snapshot.NativeLogicalSequence != opening.Snapshot.NativeLogicalSequence || after.Snapshot.CollectionIncarnation != opening.Snapshot.CollectionIncarnation || after.Snapshot.DocumentGeneration != opening.Snapshot.DocumentGeneration {
					t.Fatal("physical checkpoint changed logical identity")
				}
				if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
					return []byte(`{"tenant":"a","content":"delta","embedding":[1,0]}`), true, nil
				}); err != nil {
					t.Fatal(err)
				}
				requireNativeHybridResult(t, c, opts)
				held, err := old.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
				if err != nil || len(held.Results) != 1 || !strings.Contains(string(held.Results[0].Document), "alpha") {
					t.Fatalf("held old view: %+v %v", held, err)
				}
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(opening.Results[0].Document, ownedDocument) {
					t.Fatal("owned document changed after owner Close")
				}
				owner, manager, c = openNativeTextPublic(t, dir, profile, false, version)
				cut, err := c.nativeJSON.db.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				if cut.State() != nil || cut.LogicalSequence() != 0 {
					t.Fatalf("cold capture state=%T seq=%d", cut.State(), cut.LogicalSequence())
				}
				if err := cut.Close(); err != nil {
					t.Fatal(err)
				}
				opts = nativeHybridRequest()
				opts.Text.Query = "delta"
				cold := requireNativeHybridResult(t, c, opts, "a")
				if cold.Snapshot.NativeLogicalSequence != 0 || cold.Snapshot.CollectionIncarnation != opening.Snapshot.CollectionIncarnation || cold.Snapshot.DocumentGeneration <= opening.Snapshot.DocumentGeneration {
					t.Fatalf("cold logical identity: %+v", cold.Snapshot)
				}
				// The private cold import is not publication. A later ordinary
				// mutation must install a complete image and survive another reopen.
				if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
					return []byte(`{"tenant":"a","content":"epsilon","embedding":[1,0]}`), true, nil
				}); err != nil {
					t.Fatal(err)
				}
				opts.Text.Query = "epsilon"
				mutated := requireNativeHybridResult(t, c, opts, "a")
				if mutated.Snapshot.NativeLogicalSequence == 0 {
					t.Fatal("cold mutation was not published")
				}
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				_, _, c = openNativeTextPublic(t, dir, profile, false, version)
				requireNativeHybridResult(t, c, opts, "a")
			})
		}
	}
}

func setNativeHybridHook(t *testing.T, fn func(*Collection, *CollectionReadView)) {
	t.Helper()
	hybridSearchAfterReadOwnerAcquiredHookForTest.Lock()
	hybridSearchAfterReadOwnerAcquiredHookForTest.fn = fn
	hybridSearchAfterReadOwnerAcquiredHookForTest.Unlock()
	t.Cleanup(func() {
		hybridSearchAfterReadOwnerAcquiredHookForTest.Lock()
		hybridSearchAfterReadOwnerAcquiredHookForTest.fn = nil
		hybridSearchAfterReadOwnerAcquiredHookForTest.Unlock()
	})
}

func TestNativeHybridPublicSnapshotFence(t *testing.T) {
	for _, change := range []string{"document", "unrelated", "schema", "text-drop-recreate", "incarnation", "checkpoint", "cancel"} {
		t.Run(change, func(t *testing.T) {
			owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			if _, err := c.Insert([]byte("a"), []byte(`{"tenant":"a","content":"alpha beta","embedding":[1,0]}`)); err != nil {
				t.Fatal(err)
			}
			held, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := nativeHybridRequest()
			opts.Context = ctx
			setNativeHybridHook(t, func(got *Collection, view *CollectionReadView) {
				if got != c {
					return
				}
				var err error
				switch change {
				case "document":
					_, err = c.Insert([]byte("b"), []byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`))
				case "unrelated":
					_, err = manager.CreateCollection(&CollectionMeta{Name: "other"})
				case "schema":
					_, err = c.CreateIndex(IndexDefinition{Name: "other", Field: "other", ValueType: IndexValueString})
				case "text-drop-recreate":
					def := view.nativeRead.meta.TextIndexes[0]
					if _, err = c.DropTextIndex(def.Name); err == nil {
						_, _, err = c.CreateTextIndex(def)
					}
				case "incarnation":
					// CollectionManager has no public collection-drop API. This
					// internal publication uses the real catalog owner/commit seam;
					// the query and held view under test remain ordinary public APIs.
					err = replaceNativeHybridIncarnation(c)
					if err == nil {
						_, err = manager.CreateCollection(nativeTextPublicMeta())
					}
				case "checkpoint":
					err = owner.Checkpoint()
				case "cancel":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			response, err := c.SearchHybrid(opts)
			if change == "checkpoint" {
				if err != nil || len(response.Results) != 1 {
					t.Fatalf("physical handoff false stale: %+v %v", response, err)
				}
			} else {
				if change == "cancel" {
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				} else if !errors.Is(err, ErrHybridSearchStaleIndex) {
					t.Fatalf("expected logical stale: %v", err)
				}
				if len(response.Results) != 0 || len(response.Candidates) != 0 || response.Stats.FailClosed != 1 {
					t.Fatalf("not fail closed: %+v", response)
				}
			}
			old, err := held.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
			if err != nil || len(old.Results) != 1 || !old.Results[0].Found {
				t.Fatalf("held view lost: %+v %v", old, err)
			}
		})
	}
}

func TestNativeHybridPublicBoundsAndCleanup(t *testing.T) {
	owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	var pins []backenddb.NativeSemanticCapture
	pin := func() {
		cut, e := backend.CaptureNativeSemanticState()
		if e != nil {
			t.Fatal(e)
		}
		pins = append(pins, cut)
	}
	pin()
	budget := pins[0].NativeAllocator().(nativeHNSWAdmissionBudget)
	t.Cleanup(func() {
		for _, p := range pins {
			_ = p.Close()
		}
		_ = manager.Close()
		_ = owner.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("full Close: %+v", stats)
		}
	})
	if _, err = c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha beta","embedding":[1,0]}`), []byte(`{"tenant":"a","content":"alpha beta","embedding":[0,1]}`)}); err != nil {
		t.Fatal(err)
	}
	pin()
	// A range can encounter the same multivalue document more than once. Both
	// the maintained merger's seen map and the outer allow-set are admitted.
	arrayMeta := nativeTextPublicMeta()
	arrayMeta.Name = "arrays"
	arrayMeta.VectorIndexes = nil // Dedupe is independent of inherited HNSW multivalue-parser eligibility.
	arrayMeta.Options.AllowArrayValuesInIndex = true
	if _, err := manager.CreateCollection(arrayMeta); err != nil {
		t.Fatal(err)
	}
	pin()
	arrayCollection, err := manager.OpenCollection("arrays")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arrayCollection.Insert([]byte("array"), []byte(`{"tenant":["a","b"],"content":"alpha beta","embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	pin()
	arrayOpts := nativeHybridRequest()
	arrayOpts.ScalarFilter = &HybridScalarFilter{IndexName: "tenant", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "a", Inclusive: true}, Upper: IndexRangeBound{Value: "b", Inclusive: true}}}
	requireNativeHybridResult(t, arrayCollection, arrayOpts, "array")
	opts := nativeHybridRequest()
	requireNativeHybridResult(t, c, opts, "a", "b")
	// Public scalar limits are derived, not caller-settable. Exercise the
	// unchanged small-limit planner/owner seam without adding an API option or
	// claiming a 4,097-row public-limit corpus.
	for _, aggregate := range []bool{false, true} {
		view, e := c.OpenCollectionReadView()
		if e != nil {
			t.Fatal(e)
		}
		plan, e := planHybridSearch(opts)
		if e != nil {
			t.Fatal(e)
		}
		plan.readView = view
		plan.nativeScratch, e = newHybridNativeScratch(view)
		if e != nil {
			t.Fatal(e)
		}
		view.catalog, e = newNativeTextCatalog(view.nativeRead, plan.nativeScratch.admit)
		if e != nil {
			t.Fatal(e)
		}
		if aggregate {
			plan.scalarAggregateLimit = 1
		} else {
			plan.scalarLookupLimit = 1
		}
		_, _, e = c.hybridScalarAllowSetAtReadView(plan, view)
		if e == nil {
			t.Fatalf("aggregate=%t expected finite scalar refusal", aggregate)
		}
		if e = view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	opts = nativeHybridRequest()
	opts.Text.MaxPostingsScanned = 1
	failed, err := c.SearchHybrid(opts)
	if err == nil || failed.Stats.FailClosed != 1 {
		t.Fatalf("postings cap: %+v %v", failed, err)
	}
	// Empty indexed sets still run the declared analyzer, not just syntax checks.
	def := TextIndexDefinition{Name: "stopwords", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"why"}}}
	if _, _, err = c.CreateTextIndex(def); err != nil {
		t.Fatal(err)
	}
	pin()
	opts = nativeHybridRequest()
	opts.ScalarFilter.Value = "missing"
	opts.Text.IndexName = "stopwords"
	opts.Text.Query = "why AND alpha"
	failed, err = c.SearchHybrid(opts)
	if !errors.Is(err, ErrHybridSearchUnsupported) || failed.Stats.TextPostingsScanned != 0 || len(failed.Results) != 0 {
		t.Fatalf("empty invalid analyzer: %+v %v", failed, err)
	}
	opts.Text.Query = "alpha AND why alpha"
	requireNativeHybridResult(t, c, opts)
	opts = nativeHybridRequest()
	opts.Vector = &HybridVectorQuery{IndexName: "embedding", Query: []float32{1, 0}, CandidateLimit: 3}
	opts.Vector.QueryMode = VectorIndexQueryModeQuantizedOnly // ordinary adapter does not reinterpret typed/quantized modes
	before := budget.Stats()
	failed, err = c.SearchHybrid(opts)
	after := budget.Stats()
	if !errors.Is(err, ErrHybridSearchUnsupported) || failed.Stats.FailClosed != 1 || after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
		t.Fatalf("vector refusal: %v before=%+v after=%+v", err, before, after)
	}
	// Warm capture/data caches before the allocation refusal. Every attempt must
	// be refused, add no debt and preserve the original logical/WAL identity.
	opts = nativeHybridRequest()
	requireNativeHybridResult(t, c, opts, "a", "b")
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	sequence, lsn := read.capture.LogicalSequence(), backend.CommandWALNextLSN()
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err = blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		before := budget.Stats()
		failed, err = c.SearchHybrid(opts)
		after := budget.Stats()
		if !errors.Is(err, memtable.ErrCOWCapacity) || len(failed.Results) != 0 || len(failed.Candidates) != 0 || after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases || backend.CommandWALNextLSN() != lsn {
			t.Fatalf("attempt%d refusal: %v before=%+v after=%+v", attempt, err, before, after)
		}
	}
	if err := blocker.Resize(0); err != nil {
		t.Fatal(err)
	}
	// A second designated refusal occurs only after the complete view/catalog
	// is open. It exercises growth of that SAME receipt before scalar sets,
	// rather than merely refusing the initial capture allocation.
	setNativeHybridHook(t, func(got *Collection, _ *CollectionReadView) {
		if got != c {
			return
		}
		if err := blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
			t.Fatal(err)
		}
	})
	for attempt := 0; attempt < 3; attempt++ {
		before := budget.Stats()
		failed, err = c.SearchHybrid(opts)
		if errReset := blocker.Resize(0); errReset != nil {
			t.Fatal(errReset)
		}
		after := budget.Stats()
		if !errors.Is(err, memtable.ErrCOWCapacity) || len(failed.Results) != 0 || len(failed.Candidates) != 0 || after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases || backend.CommandWALNextLSN() != lsn {
			t.Fatalf("view-growth attempt%d refusal: %v before=%+v after=%+v", attempt, err, before, after)
		}
	}
	hybridSearchAfterReadOwnerAcquiredHookForTest.Lock()
	hybridSearchAfterReadOwnerAcquiredHookForTest.fn = nil
	hybridSearchAfterReadOwnerAcquiredHookForTest.Unlock()
	blocker.Close()
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if current.capture.LogicalSequence() != sequence || current.collection.incarnation != read.collection.incarnation || current.collection.documentGeneration != read.collection.documentGeneration || !sameCollectionMeta(current.meta, read.meta) {
		t.Fatal("query changed logical identity")
	}
	current.Close()
	requireNativeHybridResult(t, c, opts, "a", "b")
}

// No public drop API is invented: publish one genuine catalog removal using
// the existing native replacement owner, then let the public constructor create
// a new incarnation. This helper is only the deterministic stale-query control.
func replaceNativeHybridIncarnation(c *Collection) error {
	tx, err := c.nativeJSON.db.BeginNativeConditionalTxn()
	if err != nil {
		return err
	}
	defer tx.Close()
	base := tx.State().(*nativeEngineVersion)
	old, ok := base.names.Get(c.name)
	if !ok {
		return ErrIndexNotFound
	}
	counter, err := nativeCatalogIdentityRecord(base.nextIncarnation, true)
	if err != nil {
		return err
	}
	effects := []memtable.COWMutation{
		{Key: []byte(systemCollectionMetaKey(c.name)), Flags: node.FlagTombstone},
		{Key: []byte(nativeCatalogIncarnationPrefix + c.name), Flags: node.FlagTombstone},
		{Key: []byte(nativeCatalogNextIncarnationKey), Value: counter[:], Flags: node.FlagInline},
	}
	next, err := prepareNativeCatalogReplacement(base, old, nil, effects)
	if err != nil {
		if next != nil {
			err = errors.Join(err, finishPrivateNative(next.Release()))
		}
		return err
	}
	_, err = tx.CommitNative(&backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}, nil, false)
	return err
}

// The cap counts older shadowed sources as work even if the result would fit
// the live-ID limit. The iterator and merger remain the maintained implementations.
func TestNativeHybridPublicScalarPhysicalWork(t *testing.T) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	var pins []backenddb.NativeSemanticCapture
	pin := func() {
		cut, err := c.nativeJSON.db.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, cut)
	}
	defer func() {
		for _, cut := range pins {
			if err := cut.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	pin()
	if _, err := c.Insert([]byte("a"), []byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	pin()
	// Preserve both sources for the same scalar key by publishing two primary
	// mutations while the earlier cuts remain live.
	for _, tenant := range []string{"b", "a"} {
		if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
			return []byte(`{"tenant":"` + tenant + `","content":"alpha beta","embedding":[1,0]}`), true, nil
		}); err != nil {
			t.Fatal(err)
		}
		pin()
	}
	view, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	scratch, err := newHybridNativeScratch(view)
	if err != nil {
		t.Fatal(err)
	}
	view.catalog, err = newNativeTextCatalog(view.nativeRead, scratch.admit)
	if err != nil {
		t.Fatal(err)
	}
	lookup := hybridScalarLookupView{context: context.Background(), snapshot: view.snapshot, catalog: view.catalog}
	_, _, err = lookup.visitLeafIDs(HybridScalarFilter{IndexName: "tenant", Value: "a"}, 8, 1, nil, func([]byte) error { return nil })
	if !errors.Is(err, errCollectionIndexScanWorkCap) {
		t.Fatalf("shadow-source cap not enforced: %v", err)
	}
	requireNativeHybridResult(t, c, nativeHybridRequest(), "a")
}
