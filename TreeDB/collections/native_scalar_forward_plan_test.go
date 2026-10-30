package collections

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func openNativeScalarForwardPublic(t *testing.T) (*treedb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() { _ = manager.Close() })
	meta := nativeTextPublicMeta()
	meta.VectorIndexes = nil // Ordinary text cannot open a hidden HNSW runtime.
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	return owner, manager, c
}

// This regression uses existing public counters and ordinary native mutations;
// it compiles against the frozen predecessor and fails at the warm-hit oracle.
func TestNativeScalarForwardPublicLifecycle(t *testing.T) {
	owner, manager, c := openNativeScalarForwardPublic(t)
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{
		[]byte(`{"tenant":"a","content":"alpha beta"}`),
		[]byte(`{"tenant":"b","content":"alpha beta"}`),
	}); err != nil {
		t.Fatal(err)
	}
	held, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	budget := held.nativeRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	var begins, ends atomic.Int64
	unregister := c.nativeJSON.db.RegisterForegroundReadObserver(func() {}, func() func() {
		begins.Add(1)
		return func() { ends.Add(1) }
	})
	defer unregister()
	opts := nativeHybridRequest()
	cold := requireNativeHybridResult(t, c, opts, "a")
	warm := requireNativeHybridResult(t, c, opts, "a")
	if cold.Stats.ScalarFilterPlanCacheMisses != 1 || cold.Stats.ScalarFilterInputIDs == 0 || warm.Stats.ScalarFilterPlanCacheHits != 1 || warm.Stats.ScalarFilterInputIDs != 0 || warm.Stats.ScalarFilterPlanCacheEntries != 1 || warm.Stats.ScalarFilterPlanCacheRetainedBytes == 0 {
		t.Fatalf("ordinary no-vector cold=%+v warm=%+v", cold.Stats, warm.Stats)
	}
	if c.registeredColumnGraphPartitionLiveCarrier("embedding") != nil || len(c.meta.VectorIndexes) != 0 {
		t.Fatal("text reuse opened or declared a vector runtime")
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.SearchHybrid(opts)
			if err != nil || len(r.Results) != 1 || string(r.Results[0].ID) != "a" || r.Stats.ScalarFilterPlanCacheHits != 1 {
				failures <- fmt.Errorf("concurrent ordinary reuse: %+v %v", r, err)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := opts
	cancelled.Context = ctx
	if _, err := c.SearchHybrid(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("warm query ignored cancellation", err)
	}
	if begins.Load() == 0 || begins.Load() != ends.Load() {
		t.Fatal("warm foreground observation unbalanced", begins.Load(), ends.Load())
	}
	if replaced, err := c.Replace([]byte("a"), []byte(`{"tenant":"b","content":"alpha beta"}`)); err != nil || !replaced {
		t.Fatal("replace", replaced, err)
	}
	current := requireNativeHybridResult(t, c, opts)
	if current.Stats.ScalarFilterPlanCacheHits != 0 || current.Snapshot.DocumentGeneration == warm.Snapshot.DocumentGeneration {
		t.Fatal("new state reused old scalar selection", current)
	}
	old, err := held.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
	if err != nil || len(old.Results) != 1 || !old.Results[0].Found {
		t.Fatal("held old capture", old, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("Close retained scalar/forward backing: %+v", stats)
	}
}

func TestNativeScalarForwardForegroundReadScope(t *testing.T) {
	_, _, c := openNativeScalarForwardPublic(t)
	view, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	var begins, ends atomic.Int64
	unregister := c.nativeJSON.db.RegisterForegroundReadObserver(func() {}, func() func() {
		begins.Add(1)
		return func() { ends.Add(1) }
	})
	defer unregister()
	end := view.beginForegroundRead()
	if begins.Load() != 1 || ends.Load() != 0 {
		t.Fatalf("native read did not enter its existing backend observer: begins=%d ends=%d", begins.Load(), ends.Load())
	}
	end()
	if ends.Load() != 1 {
		t.Fatal("native foreground scope did not end", ends.Load())
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	view.beginForegroundRead()()
	if begins.Load() != 1 || ends.Load() != 1 {
		t.Fatal("closed native view entered foreground observation", begins.Load(), ends.Load())
	}
}

// Count real captured storage calls, preserving every optional reader capability.
// The hook changes only observation of the public request's already acquired cut.
type scalarForwardCountedCapture struct {
	backenddb.NativeSemanticCapture
	reads *atomic.Int64
}

func (c *scalarForwardCountedCapture) NativeLeafPage(p page.LeafLogPtr, dst []byte) ([]byte, error) {
	c.reads.Add(1)
	return c.NativeSemanticCapture.(backenddb.NativePhysicalLeafReader).NativeLeafPage(p, dst)
}
func (c *scalarForwardCountedCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	c.reads.Add(1)
	return c.NativeSemanticCapture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(root, key, copyValue)
}
func (c *scalarForwardCountedCapture) NativeValueAtPointer(p page.ValuePtr, admit func(uint64) error) ([]byte, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(p, admit)
}
func TestNativeScalarForwardPublicPhysicalReuse(t *testing.T) {
	owner, _, c := openNativeScalarForwardPublic(t)
	var ids, docs [][]byte
	for i := 0; i < 32; i++ {
		ids = append(ids, []byte(fmt.Sprintf("%04d", i)))
		docs = append(docs, []byte(`{"tenant":"a","content":"alpha beta"}`))
	}
	if _, err := c.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	setNativeHybridHook(t, func(_ *Collection, view *CollectionReadView) {
		view.nativeRead.capture = &scalarForwardCountedCapture{view.nativeRead.capture, &reads}
	})
	opts := nativeHybridRequest()
	opts.TopK = 1
	opts.Text.CandidateLimit = 64
	opts.IncludeDocuments = false
	opts.DocumentFetchOptions = DocumentFetchOptions{}
	opts.Text.IncludeTextMatches = false
	cold, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	coldReads := reads.Swap(0)
	warm, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	warmReads := reads.Load()
	if len(cold.Results) != 1 || len(warm.Results) != 1 || string(cold.Results[0].ID) != string(warm.Results[0].ID) || cold.Stats.ScalarFilterInputIDs != 32 || warm.Stats.ScalarFilterInputIDs != 0 || warm.Stats.ScalarFilterPlanCacheHits != 1 || coldReads <= warmReads {
		t.Fatalf("physical reuse cold=%+v warm=%+v reads=%d/%d", cold, warm, coldReads, warmReads)
	}
	// A different complete predicate cannot reuse the broad selection.
	opts.ScalarFilter.Value = "missing"
	changed, err := c.SearchHybrid(opts)
	if err != nil || len(changed.Results) != 0 || changed.Stats.ScalarFilterPlanCacheHits != 0 {
		t.Fatalf("predicate alias: %+v %v", changed, err)
	}
}

// Build the real finite conjunction and validated forward identities under an
// actual read receipt, then exhaust that SAME parent only for optional retention.
func TestNativeScalarForwardOptionalRefusalAndBound(t *testing.T) {
	_, _, c := openNativeScalarForwardPublic(t)
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha"}`), []byte(`{"tenant":"b","content":"alpha"}`)}); err != nil {
		t.Fatal(err)
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
	view.catalog, err = newNativeTextCatalog(view.nativeRead, scratch.admitFn)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planHybridSearch(nativeHybridRequest())
	if err != nil {
		t.Fatal(err)
	}
	plan.readView, plan.nativeScratch = view, scratch
	ids, _, err := c.hybridScalarAllowSetAtReadView(plan, view)
	if err != nil || ids.forwardPlan == nil {
		t.Fatalf("eligible finite source: %+v %v", ids, err)
	}
	build := ids.forwardPlan
	q, keyLease := nativeScalarForwardCompile(view.nativeRead, build.textIndex, build.rawFilters, build.lookupLimit, build.aggregateLimit)
	if q == nil {
		t.Fatal("optional key unexpectedly refused")
	}
	defer keyLease.Close()
	q.ids = build.ids
	// End construction provenance only on this query-local value; the original
	// operation's full scalar and forward work/admission still happens here.
	ids.forwardPlan = nil
	def, _ := findTextIndex(view.nativeRead.meta.TextIndexes, "content")
	ctx, err := newTextV2SearchContext(view.nativeRead.capture.Snapshot(), view.catalog, def, []string{"alpha"})
	if err != nil {
		t.Fatal(err)
	}
	forward, err := newTextV2SearchOrdinalAllowSet(view.nativeRead.capture.Snapshot(), view.catalog, ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	q.status, q.forward = ctx.status, *forward
	allocator := view.nativeRead.engine.allocator
	budget := allocator.(nativeHNSWAdmissionBudget)
	blocker, err := allocator.AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err = blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	nativeScalarForwardPublish(view.nativeRead, q)
	after := budget.Stats()
	state := view.nativeRead.collection
	if state.scalarForwardCount != 0 || state.scalarForwardPlans != nil || before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases || !ids.contains("a") || forward.count != 1 {
		t.Fatalf("optional refusal altered source/parent: %+v %+v", before, after)
	}
	blocker.Close()
	nativeScalarForwardPublish(view.nativeRead, q)
	var stats HybridSearchStats
	cached := state.scalarForwardGet(q, &stats)
	if cached == nil || stats.ScalarFilterPlanCacheHits != 1 || state.scalarForwardBytes == 0 {
		t.Fatal("retention missing", stats)
	}
	charge, ok := nativeScalarForwardCharge(q, true)
	if !ok || charge != state.scalarForwardBytes {
		t.Fatal("full independently owned backing not charged", charge, state.scalarForwardBytes)
	}
	// Source mutation cannot change either half of the retained composite.
	localIDs := q.ids
	localIDs.forwardPlan = nil
	localIDs.delete("a")
	ordinal := q.forward.sorted[0]
	q.forward.sorted[0] = 999
	delete(q.forward.ordinals, ordinal)
	if !cached.ids.contains("a") || cached.forward.sorted[0] == 999 || len(cached.forward.ordinals) != 1 {
		t.Fatal("retained composite aliases operation backing")
	}
	// Observe the original admission without changing its result or replacing
	// real parent pressure. Full ReservedBytes is only a snapshot: the original
	// worker can refund cleanup and admission can reclaim idle read scratch.
	// The retained macOS RED did not capture these discriminators.
	var admissionCalls, admissionBytes uint64
	var admissionErr error
	originalAdmit := view.catalog.nativeText.admit
	view.catalog.nativeText.admit = func(n uint64) error {
		admissionCalls++
		admissionBytes += n
		admissionErr = originalAdmit(n)
		return admissionErr
	}
	forwardCharge, chargeErr := nativeTextForwardIdentityCharge(cached.ids, 1)
	if chargeErr != nil || forwardCharge == 0 {
		t.Fatalf("expected nonzero original forward charge: %d %v", forwardCharge, chargeErr)
	}
	// Under exhausted real parent, the borrowed forward plan succeeds while the
	// original uncached constructor still refuses its full original allocation.
	blocker, err = allocator.AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err = blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatal(err)
	}
	originalCharged := scratch.charged
	parentBefore := budget.Stats()
	var refusedStats HybridSearchStats
	if p := nativeScalarForwardLookup(view.nativeRead, "content", []HybridScalarFilter{{IndexName: "tenant", Value: "b"}}, plan.scalarLookupLimit, plan.scalarAggregateLimit, &refusedStats); p != nil {
		t.Fatal("exhausted optional key unexpectedly hit")
	}
	parentAfter := budget.Stats()
	if scratch.charged != originalCharged || parentBefore.ReservedBytes != parentAfter.ReservedBytes || parentBefore.ExternalLeases != parentAfter.ExternalLeases {
		t.Fatal("optional key refusal consumed original miss headroom", originalCharged, scratch.charged, parentBefore, parentAfter)
	}
	borrowed := cached.ids
	borrowed.forwardPlan = cached
	borrowBefore := budget.Stats()
	borrowCharged := scratch.charged
	borrowCalls := admissionCalls
	if got, err := newTextV2SearchOrdinalAllowSet(view.nativeRead.capture.Snapshot(), view.catalog, ctx, borrowed); err != nil || got != &cached.forward {
		t.Fatalf("validated forward reuse missed: err=%v parent=%+v charged=%d admission_calls=%d admission_bytes=%d admission_err=%v", err, budget.Stats(), scratch.charged, admissionCalls, admissionBytes, admissionErr)
	}
	if admissionCalls != borrowCalls || scratch.charged != borrowCharged {
		t.Fatalf("validated forward reuse attempted original admission: calls=%d->%d charged=%d->%d", borrowCalls, admissionCalls, borrowCharged, scratch.charged)
	}
	uncachedBefore := budget.Stats()
	uncachedCharged := scratch.charged
	uncachedCalls, uncachedBytes := admissionCalls, admissionBytes
	if got, err := newTextV2SearchOrdinalAllowSet(view.nativeRead.capture.Snapshot(), view.catalog, ctx, cached.ids); err == nil {
		t.Fatalf("uncached forward constructor bypassed original parent refusal: got_nil=%t ids=%d provenance=%t single_term_fields=%d forward_charge=%d borrow_parent=%+v before_parent=%+v after_parent=%+v charged=%d->%d admission_calls=%d->%d admission_bytes=%d->%d admission_err=%v", got == nil, cached.ids.len(), cached.ids.forwardPlan != nil, view.catalog.nativeText.singleTermFields, forwardCharge, borrowBefore, uncachedBefore, budget.Stats(), uncachedCharged, scratch.charged, uncachedCalls, admissionCalls, uncachedBytes, admissionBytes, admissionErr)
	}
	blocker.Close()
	// The same original scalar miss remains available after optional key refusal.
	lookup := hybridScalarLookupView{context: context.Background(), catalog: view.catalog}
	fallback, input, truncated, err := lookup.leafProbeBeforeCopy(HybridScalarFilter{IndexName: "tenant", Value: "b"}, 8, 9, nil, nil)
	if err != nil || truncated || input != 1 || !fallback.contains("b") {
		t.Fatal("original fallback changed", input, truncated, err)
	}
	// Saturation never evicts a pointer borrowed by an earlier read. All new
	// keys use the same validated immutable selection but different exact limits.
	for i := 1; i < nativeScalarPlanCacheMaxEntries; i++ {
		next := *cached
		next.published = false
		next.lookupLimit += i
		nativeScalarForwardPublish(view.nativeRead, &next)
	}
	retained := state.scalarForwardBytes
	extra := *cached
	extra.lookupLimit += nativeScalarPlanCacheMaxEntries
	nativeScalarForwardPublish(view.nativeRead, &extra)
	if state.scalarForwardCount != nativeScalarPlanCacheMaxEntries || state.scalarForwardBytes != retained || retained > nativeScalarPlanCacheMaxBytes {
		t.Fatal("slot/byte bound changed", state.scalarForwardCount, retained, state.scalarForwardBytes)
	}
	var again HybridSearchStats
	if state.scalarForwardGet(cached, &again) != cached {
		t.Fatal("saturation retired a live borrower")
	}
}

func TestNativeScalarForwardPublicSchemaAndPhysicalRefresh(t *testing.T) {
	owner, _, c := openNativeScalarForwardPublic(t)
	if _, err := c.Insert([]byte("a"), []byte(`{"tenant":"a","content":"alpha"}`)); err != nil {
		t.Fatal(err)
	}
	opts := nativeHybridRequest()
	requireNativeHybridResult(t, c, opts, "a")
	warm := requireNativeHybridResult(t, c, opts, "a")
	if warm.Stats.ScalarFilterPlanCacheHits != 1 {
		t.Fatal("not warm", warm.Stats)
	}
	held, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	old := held.nativeRead.collection
	if _, err = c.CreateIndex(IndexDefinition{Name: "content_scalar", Field: "content", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	current := requireNativeHybridResult(t, c, opts, "a")
	if current.Stats.ScalarFilterPlanCacheHits != 0 {
		t.Fatal("schema inherited old entry", current.Stats)
	}
	if old.scalarForwardCount != 1 || !old.scalarForwardPlans[0].ids.contains("a") {
		t.Fatal("held old capture lost selection")
	}
	before, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if err = owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	after, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if before.nativeRead.collection != after.nativeRead.collection && after.nativeRead.collection.scalarForwardCount != 0 {
		t.Fatal("physical replacement inherited derived entries")
	}
	requireNativeHybridResult(t, c, opts, "a")
	if _, err = c.DropIndex("tenant"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SearchHybrid(opts); !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatal("dropped scalar declaration reused", err)
	}
}

// This deterministic control isolates provenance validation and the original
// admission boundary. It supplements, rather than replaces, the public shared
// budget/pressure/lifecycle fixture above. A nil snapshot is deliberate: every
// refusal must be returned before any storage lookup or forward allocation.
func TestNativeScalarForwardOriginalAdmissionRefusal(t *testing.T) {
	refusal := fmt.Errorf("deterministic forward admission: %w", memtable.ErrCOWCapacity)
	local, err := newLocalIDIndex(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	local.insert("a", 0)
	ids := nativeScalarIDs(local)
	status := textV2IndexStatusValue{FormatVersion: 2, RootGeneration: 7}
	owner := &nativeCollectionState{scalarForwardPlans: new([nativeScalarPlanCacheMaxEntries]nativeScalarForwardPlan), scalarForwardCount: 1}
	published := &owner.scalarForwardPlans[0]
	*published = nativeScalarForwardPlan{textIndex: "content", status: status, ids: ids, published: true}
	ctx := &textV2SearchContext{indexName: "content", status: status}
	var calls uint64
	var charged uint64
	catalog := &collectionCatalog{nativeText: &nativeTextCatalog{
		read: &NativeJSONRead{collection: owner},
		admit: func(n uint64) error {
			calls++
			charged = n
			return refusal
		},
	}}
	borrowed := ids
	borrowed.forwardPlan = published
	if got, err := newTextV2SearchOrdinalAllowSet(nil, catalog, ctx, borrowed); err != nil || got != &published.forward || calls != 0 {
		t.Fatalf("validated owned provenance must borrow without admission: got=%p want=%p err=%v calls=%d", got, &published.forward, err, calls)
	}
	foreign := *published
	for _, name := range []string{"no-provenance", "unpublished", "foreign-owner", "wrong-index", "wrong-status"} {
		t.Run(name, func(t *testing.T) {
			calls, charged = 0, 0
			original := ids
			saved := *published
			defer func() { *published = saved }()
			// Mutate the actual owned slot so wrong index/status/publication
			// cannot pass merely because a copied pointer has a foreign owner.
			original.forwardPlan = published
			switch name {
			case "no-provenance":
				original.forwardPlan = nil
			case "unpublished":
				published.published = false
			case "foreign-owner":
				original.forwardPlan = &foreign
			case "wrong-index":
				published.textIndex = "other"
			case "wrong-status":
				published.status.RootGeneration++
			}
			originalPlan := original.forwardPlan
			originalPublished := *published
			want, err := nativeTextForwardIdentityCharge(original, 1)
			if err != nil || want == 0 {
				t.Fatalf("invalid original admission charge: %d %v", want, err)
			}
			got, err := newTextV2SearchOrdinalAllowSet(nil, catalog, ctx, original)
			if got != nil || err != refusal || !errors.Is(err, memtable.ErrCOWCapacity) || calls != 1 || charged != want {
				t.Fatalf("original refusal must precede lookup: got=%p err=%v calls=%d charged=%d want=%d", got, err, calls, charged, want)
			}
			if !original.contains("a") || original.len() != 1 || original.forwardPlan != originalPlan || published.published != originalPublished.published || published.status != originalPublished.status || published.textIndex != originalPublished.textIndex {
				t.Fatal("refusal changed caller identities, provenance or retained entry")
			}
		})
	}
}
