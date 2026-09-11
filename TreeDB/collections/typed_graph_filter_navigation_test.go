package collections

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestTypedGraphFilterNavigationReducesDispersedTraversal(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	const n = 20000
	col, base, ids, _, columns, ranks := openTypedGraphQualityFixture(t, n)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	owners := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, owners.Cold); err != nil {
		t.Fatal(err)
	}
	keeper, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", owners)
	if err != nil {
		t.Fatal(err)
	}
	defer col.CloseVectorIndexPreparedSearchCache()
	owner, err := col.openTypedGraphReadOwner(owners)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	borrowed := col.borrowTypedGraphFilterKeeper(owner, "embedding_graph")
	if borrowed == nil || borrowed != keeper {
		t.Fatal("missing captured-base keeper")
	}
	defer borrowed.mu.RUnlock()
	filter := HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "04096", Inclusive: true}}}
	limits := typedGraphFilterLimits{SourceIDs: n + 32, SourceBytes: 4 << 20, RetainedBytes: 16 << 20, MappingWork: 4 << 20, InspectedEntries: 2 * n}
	started := time.Now()
	accounting := borrowed.capturedBase.accounting
	accounting.Lock()
	beforeBacking := accounting.baseBackingBytes
	accounting.Unlock()
	var work ColumnGraphFilterWork
	plan, err := prepareTypedGraphServingFilter(t.Context(), borrowed, owner.overlay, filter, limits, &work)
	if err != nil {
		t.Fatal(err)
	}
	navigation := plan.borrowedBaseFilter.navigation
	if navigation == nil || work.RetainedBytes < uint64(navigation.retainedBytes) {
		t.Fatalf("missing or uncharged navigation: work=%+v", work)
	}
	accounting.Lock()
	baseBytes := accounting.baseBackingBytes - beforeBacking - int64(navigation.retainedBytes)
	remaining := accounting.limits.StateBytes - accounting.stateBytes - accounting.baseDescriptorBytes - accounting.baseBackingBytes
	reserve := remaining - baseBytes
	if baseBytes <= 0 || reserve <= 0 {
		accounting.Unlock()
		t.Fatalf("invalid admission fixture base=%d remaining=%d", baseBytes, remaining)
	}
	accounting.stateBytes += reserve
	accounting.Unlock()
	defer func() {
		accounting.Lock()
		accounting.stateBytes -= reserve
		accounting.Unlock()
	}()
	withoutNavigation := HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00001", Inclusive: true}, Upper: IndexRangeBound{Value: "04097", Inclusive: true}}}
	var admitted ColumnGraphFilterWork
	baseOnly, err := prepareTypedGraphServingFilter(t.Context(), borrowed, owner.overlay, withoutNavigation, limits, &admitted)
	if err != nil || baseOnly.borrowedBaseFilter.navigation != nil {
		t.Fatalf("base-only admission work=%+v err=%v", admitted, err)
	}
	var cached ColumnGraphFilterWork
	baseOnly, err = prepareTypedGraphServingFilter(t.Context(), borrowed, owner.overlay, withoutNavigation, limits, &cached)
	if err != nil || cached.SourceIDs != 0 || baseOnly.borrowedBaseFilter.navigation != nil {
		t.Fatalf("base-only cache hit work=%+v err=%v", cached, err)
	}
	fresh, err := prepareTypedGraphFilter(owner.overlay, filter, limits)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := buildTypedGraphFilterNavigation(t.Context(), owner.overlay, fresh, 1); got != nil || !errors.Is(err, errTypedGraphFilterNavigationDeclined) {
		t.Fatalf("one-byte retained budget navigation=%v err=%v", got, err)
	}
	queryRow := slices.Index(ranks, 1000)
	if queryRow < 0 {
		t.Fatal("missing selected query row")
	}
	query := columns[0].Float32Vectors[queryRow]
	oracle := make([]VectorIndexSearchResult, 0, plan.count)
	for i, vector := range columns[0].Float32Vectors {
		if ranks[i] > 4096 {
			continue
		}
		distance, err := exactVectorDistance(query, vector, VectorMetricCosine)
		if err != nil {
			t.Fatal(err)
		}
		score := 1 - float64(distance)
		oracle = append(oracle, VectorIndexSearchResult{ID: ids[i], Score: score})
	}
	slices.SortFunc(oracle, func(a, b VectorIndexSearchResult) int {
		if vectorIndexSearchResultBefore(a, b) {
			return -1
		}
		if vectorIndexSearchResultBefore(b, a) {
			return 1
		}
		return 0
	})
	oracle = oracle[:10]
	var buffer VectorIndexSearchBuffer
	if results, _, err := navigation.search(&cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 2}, query, 10, 2048, 1<<20, owner.overlay.pack, &buffer.searchScratch); len(results) != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled navigation results=%d err=%v", len(results), err)
	}
	if results, stats, err := navigation.search(t.Context(), query, 10, 2048, 1, owner.overlay.pack, &buffer.searchScratch); len(results) != 0 || !errors.Is(err, errTypedGraphSearchBudget) || stats.PreparedScoreCalls > 1 {
		t.Fatalf("exhausted navigation results=%d scores=%d err=%v", len(results), stats.PreparedScoreCalls, err)
	}
	if results, budgetStats, err := navigation.search(t.Context(), query, 10, 2048, plan.count-1, owner.overlay.pack, &buffer.searchScratch); err != nil || len(results) != 10 || budgetStats.PreparedScoreCalls > uint64(plan.count-1) {
		t.Fatalf("budgeted navigation results=%d scores=%d err=%v", len(results), budgetStats.PreparedScoreCalls, err)
	}
	legacy, legacyStats, err := owner.overlay.searchPreparedFilter(fresh, query, 10, 2048, 1<<20, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	assertNativeScalarANNOracleContract(t, legacy, oracle)
	got, stats, err := owner.overlay.searchPreparedFilter(plan, query, 10, 2048, 1<<20, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	assertNativeScalarANNOracleContract(t, got, oracle)
	if stats.Base.PreparedScoreCalls >= legacyStats.Base.PreparedScoreCalls*3/4 {
		t.Fatalf("filtered navigation scores=%d, legacy=%d", stats.Base.PreparedScoreCalls, legacyStats.Base.PreparedScoreCalls)
	}
	if navigation.retainedBytes <= 0 {
		t.Fatal("invalid retained bytes")
	}
	all, err := prepareTypedGraphServingFilter(t.Context(), borrowed, owner.overlay, HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "19999", Inclusive: true}}}, limits, &work)
	if err != nil {
		t.Fatal(err)
	}
	_, allStats, err := owner.overlay.searchPreparedFilter(all, query, 10, 2048, 1<<20, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	if allStats.Base.PreparedScoreCalls == 0 || allStats.Base.QuantizedScoreCalls != 0 {
		t.Fatalf("all-match filter left FP32 traversal: %+v", allStats.Base)
	}
	t.Logf("build=%s retained=%d scores=%d legacy_scores=%d edges=%d legacy_edges=%d", time.Since(started), navigation.retainedBytes, stats.Base.PreparedScoreCalls, legacyStats.Base.PreparedScoreCalls, stats.Base.Edges, legacyStats.Base.Edges)
}
