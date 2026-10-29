package collections

import (
	"fmt"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The wrapper preserves the real capture's physical capabilities and blocks
// before a real leaf read, without holding a shared storage/publication lock.
// Only this read's own NativeJSONRead mutex is held by its synchronous caller.
type scalarForwardPausedCapture struct {
	backenddb.NativeSemanticCapture
	once   sync.Once
	arrive func()
}

func (c *scalarForwardPausedCapture) NativeLeafPage(p page.LeafLogPtr, dst []byte) ([]byte, error) {
	c.once.Do(c.arrive)
	return c.NativeSemanticCapture.(backenddb.NativePhysicalLeafReader).NativeLeafPage(p, dst)
}
func (c *scalarForwardPausedCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(root, key, copyValue)
}
func (c *scalarForwardPausedCapture) NativeValueAtPointer(p page.ValuePtr, admit func(uint64) error) ([]byte, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(p, admit)
}

func scalarForwardWait(t *testing.T, ch <-chan struct{}, why string) {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal("barrier not reached: ", why)
	}
}

func scalarForwardCompactRequest() HybridSearchOptions {
	return HybridSearchOptions{TopK: 8, Text: &HybridTextQuery{IndexName: "content", Query: "alpha", CandidateLimit: 8}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "a"}}
}
func scalarForwardRequireIDs(t *testing.T, r HybridSearchResponse, want ...string) {
	t.Helper()
	if len(r.Results) != len(want) {
		t.Fatalf("results=%+v want=%v", r.Results, want)
	}
	for i, id := range want {
		if string(r.Results[i].ID) != id {
			t.Fatalf("results=%+v want=%v", r.Results, want)
		}
	}
	if !r.Snapshot.Native || r.Stats.FullDocumentScanFallbacks != 0 || r.Stats.FailClosed != 0 {
		t.Fatalf("route=%+v stats=%+v", r.Snapshot, r.Stats)
	}
}

// Every worker reaches its first physical scalar leaf AFTER observing a cold
// cache. No worker can publish until all eight are in flight on the same state.
// Consequently eight recorded misses, rather than eight merely simultaneous
// goroutine starts, prove that duplicate cold constructions meet at publication.
func TestNativeScalarForwardConcurrentColdPublication(t *testing.T) {
	owner, manager, c := openNativeScalarForwardPublic(t)
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha beta"}`), []byte(`{"tenant":"b","content":"alpha beta"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Prime ordinary physical metadata without a scalar filter/cache entry.
	if _, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 8, CandidateLimit: 8}); err != nil {
		t.Fatal(err)
	}
	pin, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	state := pin.nativeRead.collection
	budget := pin.nativeRead.engine.allocator.(nativeHNSWAdmissionBudget)
	if state.scalarForwardCount != 0 {
		t.Fatal("fixture was not cold")
	}
	before := budget.Stats()
	const workers = 8
	arrived := make(chan struct{}, workers)
	captured := make(chan *nativeCollectionState, workers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var wg sync.WaitGroup
	// Registered before worker launch: every failure releases the barrier and
	// drains workers before the fixture/read owners can close.
	defer func() { unblock(); wg.Wait() }()
	setNativeHybridHook(t, func(got *Collection, view *CollectionReadView) {
		if got != c {
			return
		}
		captured <- view.nativeRead.collection
		view.nativeRead.capture = &scalarForwardPausedCapture{NativeSemanticCapture: view.nativeRead.capture, arrive: func() { arrived <- struct{}{}; <-release }}
	})
	type outcome struct {
		r   HybridSearchResponse
		err error
	}
	results := make(chan outcome, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.SearchHybrid(scalarForwardCompactRequest())
			results <- outcome{r: r, err: err}
		}()
	}
	for i := 0; i < workers; i++ {
		scalarForwardWait(t, arrived, fmt.Sprintf("cold scalar leaf %d", i))
	}
	for i := 0; i < workers; i++ {
		if got := <-captured; got != state {
			t.Fatal("cold worker acquired a different state")
		}
	}
	state.scalarForwardMu.Lock()
	count := state.scalarForwardCount
	state.scalarForwardMu.Unlock()
	if count != 0 {
		t.Fatal("publication escaped cold barrier", count)
	}
	unblock()
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		scalarForwardRequireIDs(t, result.r, "a")
		if result.r.Stats.ScalarFilterPlanCacheMisses != 1 || result.r.Stats.ScalarFilterPlanCacheHits != 0 || result.r.Stats.ScalarFilterInputIDs != 1 {
			t.Fatalf("worker did not execute an original cold construction: %+v", result.r.Stats)
		}
	}
	state.scalarForwardMu.Lock()
	count, bytes, lease := state.scalarForwardCount, state.scalarForwardBytes, state.scalarForwardLease
	state.scalarForwardMu.Unlock()
	after := budget.Stats()
	if count != 1 || bytes == 0 || lease == nil || after.ExternalLeases != before.ExternalLeases+1 {
		t.Fatalf("duplicate publication/lease: count=%d bytes=%d before=%+v after=%+v", count, bytes, before, after)
	}
	current, err := c.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	if current.nativeRead.collection != state {
		t.Fatal("workers did not share the selected state")
	}
	if err = current.Close(); err != nil {
		t.Fatal(err)
	}
	if err = pin.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if final := budget.Stats(); final.TotalBytes != 0 || final.ExternalLeases != 0 {
		t.Fatalf("cold publication Close: %+v", final)
	}
}

// A captured executor may continue on its pinned old cut. Public SearchHybrid
// remains current-snapshot-bound (the separate snapshot-fence tests require its
// original stale-index error after logical mutation); this test does not change
// that contract or attempt to accept a stale public response.
func TestNativeScalarForwardBorrowDuringSuccessorPublication(t *testing.T) {
	for _, change := range []string{"writer", "ddl", "physical"} {
		t.Run(change, func(t *testing.T) {
			owner, manager, c := openNativeScalarForwardPublic(t)
			if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha beta"}`), []byte(`{"tenant":"b","content":"alpha beta"}`)}); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// Keep an actual pending physical change so Checkpoint has a successor to
			// publish; this is not a conditional assertion on a no-op checkpoint.
			if _, err := c.Insert([]byte("c"), []byte(`{"tenant":"b","content":"gamma"}`)); err != nil {
				t.Fatal(err)
			}
			opts := scalarForwardCompactRequest()
			cold, err := c.SearchHybrid(opts)
			if err != nil {
				t.Fatal(err)
			}
			scalarForwardRequireIDs(t, cold, "a")
			warm, err := c.SearchHybrid(opts)
			if err != nil || warm.Stats.ScalarFilterPlanCacheHits != 1 {
				t.Fatalf("not warm: %+v %v", warm.Stats, err)
			}
			old, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			budget := old.nativeRead.engine.allocator.(nativeHNSWAdmissionBudget)
			state := old.nativeRead.collection
			scratch, err := newHybridNativeScratch(old)
			if err != nil {
				t.Fatal(err)
			}
			old.catalog, err = newNativeTextCatalog(old.nativeRead, scratch.admitFn)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := planHybridSearch(opts)
			if err != nil {
				t.Fatal(err)
			}
			plan.readView, plan.nativeScratch = old, scratch
			allowed, stats, err := c.hybridScalarAllowSetAtReadView(plan, old)
			if err != nil || stats.ScalarFilterPlanCacheHits != 1 || allowed.forwardPlan == nil || !allowed.forwardPlan.published {
				t.Fatalf("captured borrower did not obtain immutable entry: %+v %v", stats, err)
			}
			state.scalarForwardMu.Lock()
			retained, lease := state.scalarForwardBytes, state.scalarForwardLease
			state.scalarForwardMu.Unlock()
			arrived, release, finished := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			old.nativeRead.capture = &scalarForwardPausedCapture{NativeSemanticCapture: old.nativeRead.capture, arrive: func() { arrived <- struct{}{}; <-release }}
			textOpts := TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 8, CandidateLimit: 8, textV2AllowedIDs: allowed}
			var response TextSearchResponse
			var queryErr error
			go func() {
				defer close(finished)
				response, queryErr = c.searchTextAtReadView(textOpts, textSearchResultScoreOnly, old)
			}()
			// Always unblock and join before deferred old.Close or fixture cleanup.
			defer func() { unblock(); <-finished }()
			scalarForwardWait(t, arrived, "warm captured text leaf")
			switch change {
			case "writer":
				replaced, err := c.Replace([]byte("a"), []byte(`{"tenant":"b","content":"alpha beta"}`))
				if err != nil || !replaced {
					t.Fatal("ordinary replacement", replaced, err)
				}
			case "ddl":
				if _, err := c.CreateIndex(IndexDefinition{Name: "content_scalar", Field: "content", ValueType: IndexValueString}); err != nil {
					t.Fatal(err)
				}
			case "physical":
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			current, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			if current.nativeRead.collection == state {
				t.Fatal("operation did not publish a successor", change)
			}
			if err = current.Close(); err != nil {
				t.Fatal(err)
			}
			currentResult, err := c.SearchHybrid(opts)
			if err != nil {
				t.Fatal(err)
			}
			if change == "writer" {
				scalarForwardRequireIDs(t, currentResult)
			} else {
				scalarForwardRequireIDs(t, currentResult, "a")
			}
			if currentResult.Stats.ScalarFilterPlanCacheHits != 0 || currentResult.Stats.ScalarFilterPlanCacheMisses != 1 {
				t.Fatal("successor inherited old composite", currentResult.Stats)
			}
			state.scalarForwardMu.Lock()
			stillPinned := state.scalarForwardCount == 1 && state.scalarForwardBytes == retained && state.scalarForwardLease == lease && lease != nil
			state.scalarForwardMu.Unlock()
			if !stillPinned || retained == 0 || budget.Stats().ReservedBytes < retained {
				t.Fatal("active old borrower lost retained backing", budget.Stats())
			}
			unblock()
			scalarForwardWait(t, finished, "captured old text completion")
			if queryErr != nil || len(response.Results) != 1 || string(response.Results[0].DocumentID) != "a" {
				t.Fatalf("captured cut changed: %+v %v", response, queryErr)
			}
			// End adapter/catalog/scratch aliases before releasing the original read.
			allowed = hybridScalarIDs{}
			textOpts.textV2AllowedIDs = hybridScalarIDs{}
			scratch.scalarForwardBuild = nativeScalarForwardPlan{}
			plan.nativeScratch, plan.readView = nil, nil
			scratch = nil
			if err = old.Close(); err != nil {
				t.Fatal(err)
			}
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err = owner.Close(); err != nil {
				t.Fatal(err)
			}
			if final := budget.Stats(); final.TotalBytes != 0 || final.ExternalLeases != 0 {
				t.Fatalf("borrowed successor Close: %+v", final)
			}
		})
	}
}

func TestNativeScalarForwardRangeAndWarmPredicateChange(t *testing.T) {
	_, _, c := openNativeScalarForwardPublic(t)
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte(`{"tenant":"a","content":"alpha beta"}`), []byte(`{"tenant":"b","content":"alpha beta"}`), []byte(`{"tenant":"c","content":"alpha beta"}`)}); err != nil {
		t.Fatal(err)
	}
	opts := scalarForwardCompactRequest()
	opts.ScalarFilter = &HybridScalarFilter{And: []HybridScalarFilter{
		{IndexName: "tenant", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "a", Inclusive: true}, Upper: IndexRangeBound{Value: "c", Inclusive: true}}},
		{IndexName: "tenant", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "b", Inclusive: true}, Upper: IndexRangeBound{Value: "c", Inclusive: true}}},
	}}
	cold, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	scalarForwardRequireIDs(t, cold, "b", "c")
	warm, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	scalarForwardRequireIDs(t, warm, "b", "c")
	if cold.Stats.ScalarFilterPlanCacheMisses != 1 || cold.Stats.ScalarFilterLookups != 2 || warm.Stats.ScalarFilterPlanCacheHits != 1 || warm.Stats.ScalarFilterInputIDs != 0 {
		t.Fatalf("range/AND was not actually reused: cold=%+v warm=%+v", cold.Stats, warm.Stats)
	}
	// Equal encoded bound bytes with changed inclusivity must be a different key.
	opts.ScalarFilter.And[1].Range.Lower.Inclusive = false
	changed, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	scalarForwardRequireIDs(t, changed, "c")
	if changed.Stats.ScalarFilterPlanCacheHits != 0 || changed.Stats.ScalarFilterPlanCacheMisses != 1 || changed.Stats.ScalarFilterInputIDs == 0 {
		t.Fatal("predicate inclusivity reused prior conjunction", changed.Stats)
	}
	changedWarm, err := c.SearchHybrid(opts)
	if err != nil {
		t.Fatal(err)
	}
	scalarForwardRequireIDs(t, changedWarm, "c")
	if changedWarm.Stats.ScalarFilterPlanCacheHits != 1 || changedWarm.Stats.ScalarFilterInputIDs != 0 || changedWarm.Stats.ScalarFilterPlanCacheEntries != 2 {
		t.Fatal("changed conjunction did not warm independently", changedWarm.Stats)
	}
}
