package collections

import (
	"slices"
	"testing"
	"time"
)

func TestTypedGraphFilterNavigationReducesDispersedTraversal(t *testing.T) {
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
	var work ColumnGraphFilterWork
	plan, err := prepareTypedGraphServingFilter(t.Context(), borrowed, owner.overlay, filter, limits, &work)
	if err != nil {
		t.Fatal(err)
	}
	navigation := plan.borrowedBaseFilter.navigation
	if navigation == nil || work.RetainedBytes < uint64(navigation.retainedBytes) {
		t.Fatalf("missing or uncharged navigation: work=%+v", work)
	}
	fresh, err := prepareTypedGraphFilter(owner.overlay, filter, limits)
	if err != nil {
		t.Fatal(err)
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
	t.Logf("build=%s retained=%d max_scores=%d scores=%d legacy_scores=%d edges=%d legacy_edges=%d", time.Since(started), navigation.retainedBytes, navigation.maxScoreCalls, stats.Base.PreparedScoreCalls, legacyStats.Base.PreparedScoreCalls, stats.Base.Edges, legacyStats.Base.Edges)
}
