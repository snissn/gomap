package collections

import "testing"

func TestColumnPhysicalTypedColumnOneShotCacheQ1Q3NoMetadata3088(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	q1Req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	q1Want := rowScanCollectionCounts1950(t, col, len(events))

	q1First, err := col.RunColumnPhysicalQuery(q1Req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q1): %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "q1 first one-shot", q1First, q1Want, len(events), len(events))
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q1 first", 1, 0, 1, 1, 0)

	q1Second, err := col.RunColumnPhysicalQuery(q1Req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q1): %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "q1 second one-shot", q1Second, q1Want, len(events), len(events))
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q1 second", 1, 1, 1, 1, 0)

	q3Req := columnPhysicalQ3DenseRequest1950()
	q3Want := columnPhysicalQ3DenseReferenceGroups1950(scanned)
	q3MatchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", scanned)

	q3First, err := col.RunColumnPhysicalQuery(q3Req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q3): %v", err)
	}
	assertColumnPhysicalQ3DenseResult1950(t, "q3 first one-shot", q3First, q3Want, len(events), q3MatchedRows, q3MatchedRows)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q3 first", 2, 1, 2, 2, 0)

	q3Second, err := col.RunColumnPhysicalQuery(q3Req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q3): %v", err)
	}
	assertColumnPhysicalQ3DenseResult1950(t, "q3 second one-shot", q3Second, q3Want, len(events), q3MatchedRows, q3MatchedRows)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q3 second", 2, 2, 2, 2, 0)

	q1Third, err := col.RunColumnPhysicalQuery(q1Req)
	if err != nil {
		t.Fatalf("third RunColumnPhysicalQuery(q1): %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "q1 third one-shot", q1Third, q1Want, len(events), len(events))
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q1 third", 2, 3, 2, 2, 0)
}

func TestColumnPhysicalTypedColumnOneShotCacheQ5NoMetadata3088(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	req := columnPhysicalQ5DenseRequest1950()
	want := columnPhysicalQ5DenseReferenceGroups1950(scanned, req.TopK)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned)

	first, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q5): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "q5 first one-shot", first, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q5 first", 1, 0, 1, 1, 0)

	second, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q5): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "q5 second one-shot", second, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q5 second", 1, 1, 1, 1, 0)
}

func TestColumnPhysicalTypedColumnOneShotCacheQ2NoMetadata3123(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2LocalDictBatchA1950(), typedColumnQ2LocalDictBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := typedColumnQ2Request1950()
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	want := columnPhysicalJSONBenchQ2ReferenceCountsP0(events)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events)

	first, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q2): %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "q2 first one-shot", first, rowHash, want)
	assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(t, "q2 first one-shot", first.Diagnostics, len(events), matchedRows, columnTypedColumnDenseGroupCountDistinctReducerPairBitset)
	assertTypedColumnOneShotCacheDiagnostics3158(t, "q2 first one-shot", first.Diagnostics, false, true, true)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q2 first", 1, 0, 1, 1, 0)
	assertTypedColumnQ2OneShotRankMapsNoGlobalCodes3158(t, col, req)

	q1Req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	q1, err := col.RunColumnPhysicalQuery(q1Req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q1 between q2 runs): %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "q1 between q2 runs", q1, rowScanCollectionCounts1950(t, col, len(events)), len(events), len(events))
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q1 between q2 runs", 2, 0, 2, 2, 0)

	second, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q2): %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "q2 second one-shot", second, rowHash, want)
	assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(t, "q2 second one-shot", second.Diagnostics, len(events), matchedRows, columnTypedColumnDenseGroupCountDistinctReducerPairBitset)
	assertTypedColumnOneShotCacheDiagnostics3158(t, "q2 second one-shot", second.Diagnostics, true, false, false)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q2 second", 2, 1, 2, 2, 0)
	assertTypedColumnQ2OneShotRankMapsNoGlobalCodes3158(t, col, req)
}

func TestColumnPhysicalTypedColumnParallelPartDecodeShapes3158(t *testing.T) {
	readCache := &columnPhysicalAssetReadCache{}
	baseReq := func(kind ColumnPhysicalQueryKind) ColumnPhysicalQueryRequest {
		return ColumnPhysicalQueryRequest{
			Kind:                     kind,
			GroupColumn:              "collection",
			ValueColumn:              "time_us",
			TopK:                     3,
			TopKOrder:                ColumnPhysicalQueryTopKInt64Asc,
			ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify,
		}
	}
	tests := []struct {
		name                                      string
		plan                                      columnTypedColumnPhysicalQueryPlan
		req                                       ColumnPhysicalQueryRequest
		allowDenseGroupCountDistinct              bool
		prepareDenseInt64SpanGlobalCodes          bool
		prepareDenseGroupCountDistinctGlobalCodes bool
		prepareDenseGroupCountDistinctGlobalRanks bool
		includePhysicalRows                       bool
		want                                      bool
	}{
		{
			name: "q1 dense group-count",
			plan: columnTypedColumnPhysicalQueryPlan{DenseGroupCount: true, GroupColumn: "collection"},
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			want: true,
		},
		{
			name: "q3 dense group-hour-count",
			plan: columnTypedColumnPhysicalQueryPlan{DenseGroupHourCount: true, GroupColumn: "collection", ValueColumn: "time_us"},
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, GroupColumn: "collection", ValueColumn: "time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			want: true,
		},
		{
			name: "q5 dense int64 span",
			plan: columnTypedColumnPhysicalQueryPlan{DenseInt64Span: true, GroupColumn: "did", ValueColumn: "time_us"},
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			want: true,
		},
		{
			name:                         "q2 dense group-count-distinct rank maps",
			plan:                         columnTypedColumnPhysicalQueryPlan{DenseGroupCountDistinct: true, GroupColumn: "collection", DistinctColumn: "did"},
			req:                          ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "collection", DistinctColumn: "did", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			allowDenseGroupCountDistinct: true,
			prepareDenseGroupCountDistinctGlobalRanks: true,
			want: true,
		},
		{
			name: "q2 rank maps require insert-only allowance",
			plan: columnTypedColumnPhysicalQueryPlan{DenseGroupCountDistinct: true, GroupColumn: "collection", DistinctColumn: "did"},
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "collection", DistinctColumn: "did", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			prepareDenseGroupCountDistinctGlobalRanks: true,
		},
		{
			name: "q4 time-order TopK uses parallel eager payloads",
			plan: columnTypedColumnPhysicalQueryPlan{TimeOrderTopK: true, GroupColumn: "collection", ValueColumn: "time_us"},
			req:  baseReq(ColumnPhysicalQueryGroupMinInt64),
			want: true,
		},
		{
			name:                             "global int64 codes stay serial",
			plan:                             columnTypedColumnPhysicalQueryPlan{DenseInt64Span: true, GroupColumn: "did", ValueColumn: "time_us"},
			req:                              ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			prepareDenseInt64SpanGlobalCodes: true,
		},
		{
			name:                         "global count-distinct codes stay serial",
			plan:                         columnTypedColumnPhysicalQueryPlan{DenseGroupCountDistinct: true, GroupColumn: "collection", DistinctColumn: "did"},
			req:                          ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountAndDistinct, GroupColumn: "collection", DistinctColumn: "did", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			allowDenseGroupCountDistinct: true,
			prepareDenseGroupCountDistinctGlobalCodes: true,
		},
		{
			name:                "physical rows stay serial",
			plan:                columnTypedColumnPhysicalQueryPlan{DenseGroupCount: true, GroupColumn: "collection"},
			req:                 ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify},
			includePhysicalRows: true,
		},
		{
			name: "verify full reads stay serial",
			plan: columnTypedColumnPhysicalQueryPlan{DenseGroupCount: true, GroupColumn: "collection"},
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := columnTypedColumnPhysicalQueryUseParallelPartDecode(
				tc.plan,
				tc.req,
				readCache,
				4,
				tc.includePhysicalRows,
				tc.allowDenseGroupCountDistinct,
				tc.prepareDenseInt64SpanGlobalCodes,
				tc.prepareDenseGroupCountDistinctGlobalCodes,
				tc.prepareDenseGroupCountDistinctGlobalRanks,
			)
			if got != tc.want {
				t.Fatalf("parallel part decode=%t want %t", got, tc.want)
			}
		})
	}
}

func assertTypedColumnQ2OneShotRankMapsNoGlobalCodes3158(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest) {
	tb.Helper()
	if col == nil {
		tb.Fatalf("nil collection")
	}
	wantPredicates := collectionTypedColumnOneShotPredicateKey(req)
	var entry *collectionTypedColumnOneShotCacheEntry
	col.typedColumnOneShotMu.Lock()
	for slot, current := range col.typedColumnOneShot {
		if slot.kind == req.Kind &&
			slot.groupColumn == req.GroupColumn &&
			slot.distinctColumn == req.DistinctColumn &&
			slot.predicates == wantPredicates {
			entry = current
			break
		}
	}
	col.typedColumnOneShotMu.Unlock()
	if entry == nil {
		tb.Fatalf("q2 typed-column one-shot cache entry not found")
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	runner := entry.runner
	if runner == nil {
		tb.Fatalf("q2 typed-column one-shot cache entry has nil runner")
	}
	if len(runner.parts) < 2 {
		tb.Fatalf("q2 typed-column one-shot runner parts=%d want multiple local dictionaries", len(runner.parts))
	}
	for partIdx := range runner.parts {
		dense := runner.parts[partIdx].DenseGroupCountDistinct
		if dense == nil {
			tb.Fatalf("q2 typed-column one-shot part %d missing dense grouped count-distinct state", partIdx)
		}
		assertTypedColumnQ2OneShotColumnRankMapsNoGlobalCodes3158(tb, partIdx, "group", &dense.Group)
		assertTypedColumnQ2OneShotColumnRankMapsNoGlobalCodes3158(tb, partIdx, "distinct", &dense.Distinct)
	}
}

func assertTypedColumnQ2OneShotColumnRankMapsNoGlobalCodes3158(tb testing.TB, partIdx int, label string, column *columnTypedColumnDenseStringCodeColumn) {
	tb.Helper()
	if column == nil {
		tb.Fatalf("q2 typed-column one-shot part %d %s column missing", partIdx, label)
	}
	if len(column.GlobalCodes) != 0 {
		tb.Fatalf("q2 typed-column one-shot part %d %s global codes=%d want 0", partIdx, label, len(column.GlobalCodes))
	}
	if label == "group" && column.GlobalDictionary == nil {
		tb.Fatalf("q2 typed-column one-shot part %d %s global dictionary is nil", partIdx, label)
	}
	if label == "distinct" && column.GlobalDictionary != nil {
		tb.Fatalf("q2 typed-column one-shot part %d %s global dictionary allocated=%d want nil", partIdx, label, len(column.GlobalDictionary))
	}
	if !column.GlobalCardinalityOK {
		tb.Fatalf("q2 typed-column one-shot part %d %s global cardinality not prepared", partIdx, label)
	}
	if len(column.GlobalLocalRanks) != len(column.Dictionary) {
		tb.Fatalf("q2 typed-column one-shot part %d %s local rank entries=%d want dictionary=%d", partIdx, label, len(column.GlobalLocalRanks), len(column.Dictionary))
	}
}

func assertTypedColumnOneShotCacheDiagnostics3158(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, wantHit, wantMiss, wantBuild bool) {
	tb.Helper()
	if diag.TypedColumnOneShotCacheHit != wantHit ||
		diag.TypedColumnOneShotCacheMiss != wantMiss ||
		diag.TypedColumnOneShotCacheBuild != wantBuild {
		tb.Fatalf("%s typed-column one-shot cache hit/miss/build=%t/%t/%t want %t/%t/%t diagnostics=%+v",
			label,
			diag.TypedColumnOneShotCacheHit, diag.TypedColumnOneShotCacheMiss, diag.TypedColumnOneShotCacheBuild,
			wantHit, wantMiss, wantBuild, diag)
	}
	if !wantBuild && diag.TypedColumnOneShotBuildNanos != 0 {
		tb.Fatalf("%s typed-column one-shot build nanos=%d want 0 diagnostics=%+v", label, diag.TypedColumnOneShotBuildNanos, diag)
	}
	prepareNanos := diag.TypedColumnPreparePlanNanos +
		diag.TypedColumnPrepareRefsNanos +
		diag.TypedColumnPreparePairingNanos +
		diag.TypedColumnPreparePartDecodeNanos +
		diag.TypedColumnPreparePostPrepareNanos +
		diag.TypedColumnPrepareSummaryNanos +
		diag.TypedColumnOneShotCacheStoreNanos +
		diag.TypedColumnPrepareReadImageNanos +
		diag.TypedColumnPrepareStateBuildNanos +
		diag.TypedColumnPrepareDictionaryNanos +
		diag.TypedColumnPreparePruningNanos +
		diag.TypedColumnPrepareSortKeyNanos +
		diag.TypedColumnPrepareStatsNanos +
		diag.TypedColumnPrepareRangeReadNanos +
		diag.TypedColumnPrepareAdapterNanos +
		diag.TypedColumnPrepareDenseGroupNanos +
		diag.TypedColumnPrepareDenseValueNanos +
		diag.TypedColumnPrepareDensePredicateNanos +
		diag.TypedColumnPrepareDensePreapplyNanos
	if wantBuild {
		if prepareNanos == 0 {
			return
		}
		if diag.TypedColumnPreparePartDecodeNanos <= 0 {
			tb.Fatalf("%s typed-column one-shot setup nanos build=%d prepare_sum=%d part_decode=%d diagnostics=%+v",
				label,
				diag.TypedColumnOneShotBuildNanos,
				prepareNanos,
				diag.TypedColumnPreparePartDecodeNanos,
				diag)
		}
		if diag.DenseInt64SpanUsed {
			fineCoreNanos := diag.TypedColumnPrepareReadImageNanos +
				diag.TypedColumnPrepareStateBuildNanos +
				diag.TypedColumnPrepareDictionaryNanos +
				diag.TypedColumnPrepareRangeReadNanos +
				diag.TypedColumnPrepareAdapterNanos
			denseNanos := diag.TypedColumnPrepareDenseGroupNanos +
				diag.TypedColumnPrepareDenseValueNanos +
				diag.TypedColumnPrepareDensePredicateNanos +
				diag.TypedColumnPrepareDensePreapplyNanos
			if fineCoreNanos <= 0 ||
				diag.TypedColumnPrepareRangeReadBytes <= 0 ||
				denseNanos <= 0 ||
				diag.TypedColumnPrepareDenseGroupNanos <= 0 ||
				diag.TypedColumnPrepareDenseValueNanos <= 0 ||
				diag.TypedColumnPrepareDensePredicateNanos <= 0 {
				tb.Fatalf("%s typed-column dense setup nanos fine_core=%d range_read_bytes=%d total=%d group=%d value=%d predicate=%d preapply=%d diagnostics=%+v",
					label,
					fineCoreNanos,
					diag.TypedColumnPrepareRangeReadBytes,
					denseNanos,
					diag.TypedColumnPrepareDenseGroupNanos,
					diag.TypedColumnPrepareDenseValueNanos,
					diag.TypedColumnPrepareDensePredicateNanos,
					diag.TypedColumnPrepareDensePreapplyNanos,
					diag)
			}
		}
		return
	}
	if prepareNanos != 0 {
		tb.Fatalf("%s typed-column one-shot prepare nanos sum=%d want 0 diagnostics=%+v", label, prepareNanos, diag)
	}
}

func assertTypedColumnOneShotCacheSnapshot3088(tb testing.TB, col *Collection, label string, entries int, hits uint64, misses uint64, builds uint64, invalidations uint64) {
	tb.Helper()
	snapshot := col.typedColumnOneShotCacheSnapshotForTest()
	if snapshot.Entries != entries ||
		snapshot.CacheHits != hits ||
		snapshot.CacheMisses != misses ||
		snapshot.CacheBuilds != builds ||
		snapshot.Invalidations != invalidations {
		tb.Fatalf("%s cache snapshot=%+v want entries=%d hits=%d misses=%d builds=%d invalidations=%d", label, snapshot, entries, hits, misses, builds, invalidations)
	}
}
