package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Freeze before production changes: this exercises the ordinary public native
// read route, same-search document fetch, concurrent readers and complete Close.
func TestNativeScalarPlanReusePublicLifecycle(t *testing.T) {
	dir := t.TempDir()
	owner, manager, c := openNativeTextPublic(t, dir, treedb.ProfileCommandWALDurable, true)
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{
		[]byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`),
		[]byte(`{"tenant":"b","content":"alpha","embedding":[0,1]}`),
	}); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	search := func(value string) (VectorIndexSearchResponse, error) {
		var buffer VectorIndexSearchBuffer
		defer buffer.Reset()
		response, view, err := c.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName: "embedding", Query: []float32{1, 0}, TopK: 2,
			StatsMode:            VectorIndexSearchStatsModeProduction,
			DeclaredScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: value},
		}, &buffer)
		if err != nil {
			return response, err
		}
		defer view.Close()
		if len(response.Results) != 1 || string(response.Results[0].ID) != value || response.Stats.SearchRouteNativeRuntime != 1 || !response.Diagnostics().NoDocumentGuardrailsOK {
			return response, fmt.Errorf("native route/results for %q: %+v", value, response)
		}
		docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
		if err != nil || len(docs.Results) != 1 || !docs.Results[0].Found {
			return response, fmt.Errorf("same-cut fetch: %+v %v", docs, err)
		}
		if err = view.Close(); err != nil {
			return response, err
		}
		return response, nil
	}
	var cold, warm [2]VectorIndexSearchStats
	for i, value := range []string{"a", "b"} {
		response, err := search(value)
		if err != nil {
			t.Fatal(err)
		}
		cold[i] = response.Stats
		response, err = search(value)
		if err != nil {
			t.Fatal(err)
		}
		warm[i] = response.Stats
	}
	if err = owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Checkpoint may publish a new graph version. Warm that exact identity before
	// testing concurrent immutable borrowing; no old-version hit is required.
	for _, value := range []string{"a", "b"} {
		if _, err := search(value); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := []string{"a", "b"}[i%2]
			response, err := search(value)
			if err != nil {
				failures <- err
				return
			}
			if response.Stats.ScalarFilterPlanCacheHits != 1 {
				failures <- fmt.Errorf("concurrent cache hit=%d", response.Stats.ScalarFilterPlanCacheHits)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("Close retained admission: %+v", stats)
	}
	for i := range cold {
		if cold[i].ScalarFilterPlanCacheMisses != 1 || cold[i].ScalarFilterProbeIDs == 0 || warm[i].ScalarFilterProbeIDs != 0 || warm[i].ScalarFilterPlanCacheHits != 1 || warm[i].ScalarFilterPlanCacheEntries != uint64(i+1) || warm[i].ScalarFilterPlanCacheRetainedBytes == 0 {
			t.Errorf("filter %d cold=%+v warm=%+v", i, cold[i], warm[i])
		}
	}
}

func TestNativeScalarPlanReuseHeldGenerationAndSchema(t *testing.T) {
	owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{
		[]byte(`{"tenant":"a","content":"alpha","embedding":[1,0]}`),
		[]byte(`{"tenant":"b","content":"alpha","embedding":[0,1]}`),
	}); err != nil {
		t.Fatal(err)
	}
	opts := VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 2, StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "a"}}
	var buffer VectorIndexSearchBuffer
	defer buffer.Reset()
	response, held, err := c.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	budget := held.nativeRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	if len(response.Results) != 1 {
		t.Fatalf("held result: %+v", response)
	}
	heldID := bytes.Clone(response.Results[0].ID)
	buffer.Reset()
	response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil || response.Stats.ScalarFilterPlanCacheHits != 1 {
		t.Fatalf("warm: %+v %v", response, err)
	}
	buffer.Reset()
	if replaced, err := c.Replace([]byte("a"), []byte(`{"tenant":"b","content":"new","embedding":[1,0]}`)); err != nil || !replaced {
		t.Fatalf("replace=%v %v", replaced, err)
	}
	opts.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "tenant", Value: "b"}
	response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil || len(response.Results) != 2 || response.Stats.ScalarFilterPlanCacheHits != 0 || response.Stats.ScalarFilterPlanCacheMisses != 1 {
		t.Fatalf("current generation: %+v %v", response, err)
	}
	buffer.Reset()
	if _, err = c.searchNativeHNSWAtReadView(opts, &buffer, held, hybridScalarIDs{}); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("held runtime mismatch: %v", err)
	}
	docs, err := held.FetchDocumentsByID([][]byte{heldID}, DocumentFetchOptions{})
	if err != nil || len(docs.Results) != 1 || !docs.Results[0].Found || !bytes.Contains(docs.Results[0].Document, []byte(`"tenant":"a"`)) {
		t.Fatalf("held document: %+v %v", docs, err)
	}
	if _, err = c.CreateIndex(IndexDefinition{Name: "content_scalar", Field: "content", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil || len(response.Results) != 2 || response.Stats.ScalarFilterPlanCacheHits != 0 || response.Stats.ScalarFilterPlanCacheMisses != 1 {
		t.Fatalf("new schema: %+v %v", response, err)
	}
	buffer.Reset()
	response, err = c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil || response.Stats.ScalarFilterPlanCacheHits != 1 {
		t.Fatalf("new schema warm: %+v %v", response, err)
	}
	buffer.Reset()
	if _, err = c.DropIndex("tenant"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SearchVectorIndexWithBuffer(opts, &buffer); !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("dropped declaration reused: %v", err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("schema/swap Close retained: %+v", stats)
	}
}

// This tests the optional clone AFTER a successful, actually admitted local
// plan, while the original runtime/read lifetimes remain live. Filling the real
// parent's remaining headroom forces its original runtime lease Resize to fail.
func TestNativeScalarPlanReuseOptionalRefusalAndBound(t *testing.T) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	if _, err := c.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"tenant":"a","embedding":[1,0]}`)}); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	if _, err := c.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer); err != nil {
		t.Fatal(err)
	}
	buffer.Reset()
	idx := c.registeredVectorIndex("embedding")
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
	idx.nativeHNSWUseMu.RLock()
	defer idx.nativeHNSWUseMu.RUnlock()
	def, _ := findVectorIndex(view.nativeRead.meta.VectorIndexes, "embedding")
	filter := HybridScalarFilter{IndexName: "tenant", Value: "a"}
	lookup := hybridScalarLookupView{catalog: view.catalog}
	// Caller-provided complete sets remain local; they must not enter the cache.
	local, stats, err := c.planNativeScalarFilterAtLookup(&filter, idx, def, &lookup, view.nativeRead.collection.documentGeneration, legacyScalarIDs(hybridScalarAllowSet{"a": {}}))
	if err != nil || stats.entries != 0 || idx.nativeScalarPlans != nil {
		t.Fatalf("local plan: %+v %v", stats, err)
	}
	key, eligible := idx.nativeRuntimeScalarPlanIdentity(view.nativeRead, local.sourceGeneration)
	if !eligible {
		t.Fatal("identity is not eligible")
	}
	allocator := view.nativeRead.capture.NativeAllocator()
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
	originalCharge := idx.nativeHNSWCharge
	stats = idx.nativeRuntimeScalarPlanPut(key, view.nativeRead, local, nativeScalarPlanCacheStats{misses: 1})
	after := budget.Stats()
	if stats.entries != 0 || idx.nativeScalarPlans != nil || idx.nativeHNSWCharge != originalCharge || before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases || local.finiteIDs.len() != 1 {
		t.Fatalf("optional refusal: stats=%+v before=%+v after=%+v", stats, before, after)
	}
	blocker.Close()
	stats = idx.nativeRuntimeScalarPlanPut(key, view.nativeRead, local, nativeScalarPlanCacheStats{misses: 1})
	if stats.entries != 1 || stats.retainedBytes == 0 {
		t.Fatalf("retention: %+v", stats)
	}
	cached, hit := idx.nativeRuntimeScalarPlanGet(key, view.nativeRead, local.clauses)
	if hit.hits != 1 || cached == local || cached.probeIDs != 0 {
		t.Fatalf("immutable hit: %+v", hit)
	}
	// A later optional copy must also leave existing slots and charge untouched.
	blocker, err = allocator.AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err = blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatal(err)
	}
	existingArray, existingCharge := idx.nativeScalarPlans, idx.nativeHNSWCharge
	deniedKey := key
	deniedKey.plan.vectorMutationSeq++
	before = budget.Stats()
	deniedStats := idx.nativeRuntimeScalarPlanPut(deniedKey, view.nativeRead, local, stats)
	after = budget.Stats()
	if deniedStats.entries != 1 || idx.nativeScalarPlans != existingArray || idx.nativeHNSWCharge != existingCharge || before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
		t.Fatalf("partial optional refusal: %+v before=%+v after=%+v", deniedStats, before, after)
	}
	blocker.Close()
	originalLower := bytes.Clone(local.clauses[0].lower)
	if len(local.clauses[0].lower) > 0 {
		local.clauses[0].lower[0] ^= 1
	}
	local.finiteIDs.delete("a")
	if cached.finiteIDs.len() != 1 || !bytes.Equal(cached.clauses[0].lower, originalLower) {
		t.Fatal("retained plan aliases local plan")
	}
	local.clauses[0].lower = originalLower
	local.finiteIDs = legacyScalarIDs(hybridScalarAllowSet{"a": {}})
	changedKey := key
	changedKey.plan.sourceGeneration++
	if got, _ := idx.nativeRuntimeScalarPlanGet(changedKey, view.nativeRead, local.clauses); got != nil {
		t.Fatal("generation aliased")
	}
	mixed := *local
	mixed.identity = NativeScalarFilterPlanMixed
	changedKey = key
	changedKey.plan.vectorMutationSeq++
	if got := idx.nativeRuntimeScalarPlanPut(changedKey, view.nativeRead, &mixed, stats); got.entries != 1 {
		t.Fatal("mixed plan retained")
	}
	// Different filters/generations can fill the established finite bound. Full
	// storage remains usable by existing readers; the next plan stays local.
	for i := 1; i < nativeScalarPlanCacheMaxEntries; i++ {
		k := key
		k.plan.vectorMutationSeq += uint64(i)
		stats = idx.nativeRuntimeScalarPlanPut(k, view.nativeRead, local, stats)
	}
	retained := idx.nativeHNSWCharge
	changedKey = key
	changedKey.plan.vectorMutationSeq += uint64(nativeScalarPlanCacheMaxEntries)
	stats = idx.nativeRuntimeScalarPlanPut(changedKey, view.nativeRead, local, stats)
	if stats.entries != nativeScalarPlanCacheMaxEntries || stats.retainedBytes > nativeScalarPlanCacheMaxBytes || idx.nativeHNSWCharge != retained {
		t.Fatalf("finite bound: %+v", stats)
	}
	if got, _ := idx.nativeRuntimeScalarPlanGet(key, view.nativeRead, local.clauses); got != cached {
		t.Fatal("full cache retired an active borrower")
	}
	// Existing runtime validity refuses unrelated read authority, even if numeric
	// generations happen to match. This is not a new visibility fallback.
	other := &NativeJSONRead{meta: view.nativeRead.meta, collection: &nativeCollectionState{incarnation: view.nativeRead.collection.incarnation + 1}}
	if _, ok := idx.nativeRuntimeScalarPlanIdentity(other, local.sourceGeneration); ok {
		t.Fatal("unrelated cut admitted")
	}
}

// Flat backing has a deterministic charge above the former Swiss-map boundary.
// Exercise warm reuse through the original full finite-probe capability while
// retaining the same runtime lease, byte/slot limits and public read lifecycle.
func TestNativeScalarPlanReuseRetainedCardinalityBoundary(t *testing.T) {
	for _, count := range []int{896, 897, 4096} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			ids, docs := make([][]byte, count), make([][]byte, count)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("d%04d", i))
				docs[i] = []byte(`{"tenant":"a","embedding":[1,0]}`)
			}
			for first := 0; first < count; first += 128 {
				last := min(first+128, count)
				if _, err := c.InsertBatch(ids[first:last], docs[first:last]); err != nil {
					t.Fatal(err)
				}
			}
			// Isolate cardinality from the independently bounded physical work
			// of probing several still-pending sources for every document.
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			var buffer VectorIndexSearchBuffer
			defer buffer.Reset()
			opts := VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 10, StatsMode: VectorIndexSearchStatsModeProduction}
			if _, err := c.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
				t.Fatal(err)
			}
			buffer.Reset()
			idx := c.registeredVectorIndex("embedding")
			view, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			budget := view.nativeRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			filter := HybridScalarFilter{IndexName: "tenant", Value: "a"}
			func() {
				defer view.Close()
				scratch, err := newHybridNativeScratch(view)
				if err != nil {
					t.Fatal(err)
				}
				view.catalog, err = newNativeTextCatalog(view.nativeRead, scratch.admitFn)
				if err != nil {
					t.Fatal(err)
				}
				idx.nativeHNSWUseMu.RLock()
				defer idx.nativeHNSWUseMu.RUnlock()
				def, _ := findVectorIndex(view.nativeRead.meta.VectorIndexes, "embedding")
				lookup := hybridScalarLookupView{context: t.Context(), catalog: view.catalog}
				beforeCharge := idx.nativeHNSWCharge
				plan, stats, err := c.planNativeScalarFilterAtLookup(&filter, idx, def, &lookup, view.nativeRead.collection.documentGeneration, hybridScalarIDs{})
				if err != nil || plan == nil {
					t.Fatalf("original local probe: %v", err)
				}
				if plan.identity != NativeScalarFilterPlanCompleteFinite || plan.finiteIDs.len() != count || plan.probeIDs != uint64(count) || stats.misses != 1 {
					t.Fatalf("local probe: plan=%+v stats=%+v", plan, stats)
				}
				for _, id := range ids {
					if ok := plan.finiteIDs.contains(string(id)); !ok {
						t.Fatalf("local plan lost %q", id)
					}
				}
				charge, eligible := nativeRuntimeScalarPlanCharge(plan, true)
				if !eligible || charge == 0 || charge > nativeScalarPlanCacheMaxBytes || stats.entries != 1 || stats.retainedBytes != charge || idx.nativeHNSWCharge != beforeCharge+charge {
					t.Fatalf("%d retention: charge=%d eligible=%v stats=%+v", count, charge, eligible, stats)
				}
				warm, warmStats, err := c.planNativeScalarFilterAtLookup(&filter, idx, def, &lookup, view.nativeRead.collection.documentGeneration, hybridScalarIDs{})
				if err != nil || warm == nil || warm.finiteIDs.len() != count {
					t.Fatalf("repeat local plan: %v", err)
				}
				if warmStats.hits != 1 || warm.probeIDs != 0 {
					t.Fatalf("boundary reuse: stats=%+v probe=%d", warmStats, warm.probeIDs)
				}
			}()
			opts.DeclaredScalarFilter = &filter
			response, resultView, err := c.SearchVectorIndexWithBufferReadView(opts, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Results) != opts.TopK || response.Stats.SearchRouteNativeRuntime != 1 || !response.Diagnostics().NoDocumentGuardrailsOK {
				resultView.Close()
				t.Fatalf("boundary public search: %+v", response)
			}
			fetched, err := resultView.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(fetched.Results) != opts.TopK {
				resultView.Close()
				t.Fatalf("boundary same-cut fetch: %+v %v", fetched, err)
			}
			for _, row := range fetched.Results {
				if !row.Found || !bytes.Contains(row.Document, []byte(`"tenant":"a"`)) {
					resultView.Close()
					t.Fatalf("boundary document: %+v", row)
				}
			}
			if err = resultView.Close(); err != nil {
				t.Fatal(err)
			}
			buffer.Reset()
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err = owner.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("boundary Close retained: %+v", stats)
			}
		})
	}
}
