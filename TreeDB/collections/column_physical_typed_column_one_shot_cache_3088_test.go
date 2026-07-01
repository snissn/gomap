package collections

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

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
	q3Req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	q3Want := columnPhysicalQ3DenseReferenceGroups1950(scanned)
	q3MatchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", scanned)

	q3First, err := col.RunColumnPhysicalQuery(q3Req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q3): %v", err)
	}
	assertColumnPhysicalQ3DenseResult1950(t, "q3 first one-shot", q3First, q3Want, len(events), q3MatchedRows, q3MatchedRows)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q3 first", 2, 1, 2, 2, 0)
	assertTypedColumnQ3OneShotDenseDictionaryValuesByCode3175(t, col, q3Req)

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
	oldGOMAXPROCS := runtime.GOMAXPROCS(2)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(oldGOMAXPROCS)
	})

	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	req := columnPhysicalQ5DenseRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	want := columnPhysicalQ5DenseReferenceGroups1950(scanned, req.TopK)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned)
	wantPrepareWorkers := columnTypedColumnPhysicalQueryPartDecodeWorkers(len(batches))
	if wantPrepareWorkers < 2 {
		t.Fatalf("q5 test setup prepare workers=%d want parallel decode workers", wantPrepareWorkers)
	}

	first, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q5): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "q5 first one-shot", first, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q5 first", 1, 0, 1, 1, 0)
	if got := first.Diagnostics.TypedColumnPrepareWorkerCount; got != wantPrepareWorkers {
		t.Fatalf("q5 first one-shot typed-column prepare workers=%d want %d diagnostics=%+v", got, wantPrepareWorkers, first.Diagnostics)
	}
	assertTypedColumnQ5OneShotDenseDictionaryValuesByCode3175(t, col, req, true)

	second, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q5): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "q5 second one-shot", second, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q5 second", 1, 1, 1, 1, 0)
	if got := second.Diagnostics.TypedColumnPrepareWorkerCount; got != 0 {
		t.Fatalf("q5 second cache-hit typed-column prepare workers=%d want 0 diagnostics=%+v", got, second.Diagnostics)
	}
}

func TestColumnPhysicalTypedColumnQ5PredicateBlockMinMaxSkips3088(t *testing.T) {
	const rowsPerGranule = 2
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ5PredicateBlockMaskBatch3088()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnRowsPerGranuleFixtureBatches3088(t, nil, rowsPerGranule, batches)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	req := columnPhysicalQ5DenseRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	want := columnPhysicalQ5DenseReferenceGroups1950(scanned, req.TopK)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned)

	first, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q5 predicate mask): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "q5 predicate mask first", first, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
	if got := first.Diagnostics.DenseInt64SpanPredicateBlocksSkipped; got < 3 {
		t.Fatalf("q5 predicate mask skipped predicate blocks=%d want at least one full row block skipped across three predicates diagnostics=%+v", got, first.Diagnostics)
	}
	if got, fullPredicateBlocks := first.Diagnostics.DecodedBlocks, 3*(len(events)/rowsPerGranule)+2; got >= fullPredicateBlocks {
		t.Fatalf("q5 predicate mask decoded blocks=%d want below unfiltered predicate decode budget %d diagnostics=%+v", got, fullPredicateBlocks, first.Diagnostics)
	}
	assertTypedColumnQ5OneShotDenseDictionaryValuesByCode3175(t, col, req, true)

	second, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q5 predicate mask): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "q5 predicate mask second", second, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
	if got := second.Diagnostics.DenseInt64SpanPredicateBlocksSkipped; got != first.Diagnostics.DenseInt64SpanPredicateBlocksSkipped {
		t.Fatalf("q5 predicate mask cache-hit skipped predicate blocks=%d want %d diagnostics=%+v", got, first.Diagnostics.DenseInt64SpanPredicateBlocksSkipped, second.Diagnostics)
	}
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
	assertTypedColumnQ2PostPrepareSubphaseDiagnostics3158(t, "q2 first one-shot", first.Diagnostics, true)
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
	assertTypedColumnQ2PostPrepareSubphaseDiagnostics3158(t, "q2 second one-shot", second.Diagnostics, false)
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

func assertTypedColumnQ5OneShotDenseDictionaryValuesByCode3175(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest, wantCompact bool) {
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
			slot.valueColumn == req.ValueColumn &&
			slot.predicates == wantPredicates {
			entry = current
			break
		}
	}
	col.typedColumnOneShotMu.Unlock()
	if entry == nil {
		tb.Fatalf("q5 typed-column one-shot cache entry not found")
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	runner := entry.runner
	if runner == nil {
		tb.Fatalf("q5 typed-column one-shot cache entry has nil runner")
	}
	sawCompact := false
	for partIdx := range runner.parts {
		dense := runner.parts[partIdx].DenseInt64Span
		if dense == nil {
			tb.Fatalf("q5 typed-column one-shot part %d missing dense int64-span state", partIdx)
		}
		if len(dense.Dictionary) != dense.Cardinality {
			tb.Fatalf("q5 typed-column one-shot part %d dictionary values-by-code=%d want cardinality=%d", partIdx, len(dense.Dictionary), dense.Cardinality)
		}
		if len(dense.DictionaryByCode) != 0 {
			tb.Fatalf("q5 typed-column one-shot part %d reverse dictionary retained=%d want 0", partIdx, len(dense.DictionaryByCode))
		}
		if !dense.PredicatesPreApplied {
			tb.Fatalf("q5 typed-column one-shot part %d predicates were not preapplied", partIdx)
		}
		if dense.PreAppliedRowsScanned < len(dense.GroupCodes) {
			tb.Fatalf("q5 typed-column one-shot part %d preapplied rows scanned=%d below compact rows=%d", partIdx, dense.PreAppliedRowsScanned, len(dense.GroupCodes))
		}
		if len(dense.PredicateRows) == 0 {
			tb.Fatalf("q5 typed-column one-shot part %d predicate rows empty", partIdx)
		}
		if len(dense.PredicateRows) > len(dense.GroupCodes) {
			tb.Fatalf("q5 typed-column one-shot part %d predicate rows=%d group rows=%d", partIdx, len(dense.PredicateRows), len(dense.GroupCodes))
		}
		compact := dense.PreAppliedRowsScanned > len(dense.GroupCodes)
		if compact {
			if len(dense.PredicateRows) != len(dense.GroupCodes) {
				tb.Fatalf("q5 typed-column one-shot part %d compact predicate rows=%d compact rows=%d", partIdx, len(dense.PredicateRows), len(dense.GroupCodes))
			}
			sawCompact = true
		}
		previousPredicateRow := -1
		for rowIdx, predicateRow := range dense.PredicateRows {
			row := int(predicateRow)
			if row <= previousPredicateRow {
				tb.Fatalf("q5 typed-column one-shot part %d predicate row[%d]=%d is not strictly ascending", partIdx, rowIdx, predicateRow)
			}
			if row >= len(dense.GroupCodes) {
				tb.Fatalf("q5 typed-column one-shot part %d predicate row[%d]=%d outside group rows=%d", partIdx, rowIdx, predicateRow, len(dense.GroupCodes))
			}
			if compact && row != rowIdx {
				tb.Fatalf("q5 typed-column one-shot part %d compact predicate row[%d]=%d", partIdx, rowIdx, predicateRow)
			}
			previousPredicateRow = row
		}
		if len(dense.Predicates) != 3 {
			tb.Fatalf("q5 typed-column one-shot part %d predicates=%d want 3", partIdx, len(dense.Predicates))
		}
		for predicateIdx, predicate := range dense.Predicates {
			if len(predicate.Codes) != 0 || len(predicate.Valid) != 0 {
				tb.Fatalf("q5 typed-column one-shot part %d predicate %d retained decoded rows codes/valid=%d/%d want 0/0", partIdx, predicateIdx, len(predicate.Codes), len(predicate.Valid))
			}
			if !predicate.SingleCodeAllowed || predicate.MissingMatchesEmpty || predicate.RejectsAll {
				tb.Fatalf("q5 typed-column one-shot part %d predicate %d metadata=%+v want single-code non-missing match", partIdx, predicateIdx, predicate)
			}
		}
	}
	if wantCompact && !sawCompact {
		tb.Fatalf("q5 typed-column one-shot cache did not contain a compact predicate-first part")
	}
}

func assertTypedColumnQ3OneShotDenseDictionaryValuesByCode3175(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest) {
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
			slot.valueColumn == req.ValueColumn &&
			slot.predicates == wantPredicates {
			entry = current
			break
		}
	}
	col.typedColumnOneShotMu.Unlock()
	if entry == nil {
		tb.Fatalf("q3 typed-column one-shot cache entry not found")
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	runner := entry.runner
	if runner == nil {
		tb.Fatalf("q3 typed-column one-shot cache entry has nil runner")
	}
	sawCompact := false
	for partIdx := range runner.parts {
		dense := runner.parts[partIdx].DenseGroupHourCount
		if dense == nil {
			tb.Fatalf("q3 typed-column one-shot part %d missing dense group-hour-count state", partIdx)
		}
		if len(dense.Dictionary) != dense.Cardinality {
			tb.Fatalf("q3 typed-column one-shot part %d dictionary values-by-code=%d want cardinality=%d", partIdx, len(dense.Dictionary), dense.Cardinality)
		}
		if len(dense.DictionaryByCode) != 0 {
			tb.Fatalf("q3 typed-column one-shot part %d reverse dictionary retained=%d want 0", partIdx, len(dense.DictionaryByCode))
		}
		if !dense.PredicatesPreApplied {
			tb.Fatalf("q3 typed-column one-shot part %d predicates were not preapplied", partIdx)
		}
		if dense.PreAppliedRowsScanned != runner.parts[partIdx].Rows {
			tb.Fatalf("q3 typed-column one-shot part %d preapplied rows scanned=%d want part rows=%d", partIdx, dense.PreAppliedRowsScanned, runner.parts[partIdx].Rows)
		}
		if len(dense.Values) != len(dense.GroupCodes) {
			tb.Fatalf("q3 typed-column one-shot part %d values=%d want group rows=%d", partIdx, len(dense.Values), len(dense.GroupCodes))
		}
		if len(dense.PredicateRows) != 0 {
			tb.Fatalf("q3 typed-column one-shot part %d retained identity predicate rows=%d want 0", partIdx, len(dense.PredicateRows))
		}
		if dense.PreAppliedRowsScanned > len(dense.GroupCodes) {
			sawCompact = true
		}
		if len(dense.Predicates) != 3 {
			tb.Fatalf("q3 typed-column one-shot part %d predicates=%d want 3", partIdx, len(dense.Predicates))
		}
		for predicateIdx, predicate := range dense.Predicates {
			if len(predicate.Codes) != 0 || len(predicate.Valid) != 0 {
				tb.Fatalf("q3 typed-column one-shot part %d predicate %d retained decoded rows codes/valid=%d/%d want 0/0", partIdx, predicateIdx, len(predicate.Codes), len(predicate.Valid))
			}
			if predicate.RejectsAll {
				tb.Fatalf("q3 typed-column one-shot part %d predicate %d rejects all", partIdx, predicateIdx)
			}
			if predicateIdx < 2 {
				if !predicate.SingleCodeAllowed || predicate.MissingMatchesEmpty {
					tb.Fatalf("q3 typed-column one-shot part %d predicate %d metadata=%+v want single-code non-missing match", partIdx, predicateIdx, predicate)
				}
				continue
			}
			if len(predicate.Allowed) == 0 || predicate.MissingMatchesEmpty {
				tb.Fatalf("q3 typed-column one-shot part %d group predicate metadata=%+v want allowed-code bitset without missing match", partIdx, predicate)
			}
		}
	}
	if !sawCompact {
		tb.Fatalf("q3 typed-column one-shot cache did not contain a compact predicate-first part")
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
	if wantBuild && diag.TypedColumnPreparePlanNanos != 0 {
		tb.Fatalf("%s typed-column one-shot prepare plan nanos=%d want 0 for reused query plan diagnostics=%+v", label, diag.TypedColumnPreparePlanNanos, diag)
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
		if diag.TypedColumnOneShotBuildNanos <= 0 {
			tb.Fatalf("%s typed-column one-shot build nanos=%d want >0 prepare_sum=%d diagnostics=%+v",
				label,
				diag.TypedColumnOneShotBuildNanos,
				prepareNanos,
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

func assertTypedColumnQ2PostPrepareSubphaseDiagnostics3158(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, want bool) {
	tb.Helper()
	rankTotal := diag.TypedColumnPrepareQ2GroupRankNanos +
		diag.TypedColumnPrepareQ2DistinctRankNanos +
		diag.TypedColumnPrepareQ2LocalRankNanos
	denseRankTotal := diag.TypedColumnPrepareQ2DenseGroupGlobalRankNanos +
		diag.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos +
		diag.TypedColumnPrepareQ2DensePartLocalRankNanos
	globalCodeTotal := diag.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos +
		diag.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos +
		diag.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos +
		diag.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos
	if !want {
		if rankTotal+denseRankTotal+globalCodeTotal != 0 {
			tb.Fatalf("%s q2 post-prepare subphase nanos=%d want 0 diagnostics=%+v", label, rankTotal+denseRankTotal+globalCodeTotal, diag)
		}
		return
	}
	if rankTotal+denseRankTotal+globalCodeTotal == 0 {
		return
	}
	if denseRankTotal != 0 {
		if diag.TypedColumnPrepareQ2DenseGroupGlobalRankNanos != diag.TypedColumnPrepareQ2GroupRankNanos ||
			diag.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos != diag.TypedColumnPrepareQ2DistinctRankNanos ||
			diag.TypedColumnPrepareQ2DensePartLocalRankNanos != diag.TypedColumnPrepareQ2LocalRankNanos {
			tb.Fatalf("%s dense q2 rank split diagnostics=%+v want explicit dense phases to mirror legacy rank phases", label, diag)
		}
	}
	if globalCodeTotal != 0 {
		assertTypedColumnQ2SortedGroupedDistinctPostPrepareDiagnostics3324(tb, label, diag, true)
	}
}

func typedColumnQ5PredicateBlockMaskBatch3088() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_900_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "mask-m-1", TimeUS: base + 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
		{ID: "mask-m-2", TimeUS: base + 90, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
		{ID: "mask-kind-1", TimeUS: base + 110, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:guard-kind-1"},
		{ID: "mask-kind-2", TimeUS: base + 120, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:guard-kind-2"},
		{ID: "mask-operation-1", TimeUS: base + 130, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.post", Did: "did:guard-operation-1"},
		{ID: "mask-operation-2", TimeUS: base + 140, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.post", Did: "did:guard-operation-2"},
		{ID: "mask-collection-1", TimeUS: base + 150, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:guard-collection-1"},
		{ID: "mask-collection-2", TimeUS: base + 160, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:guard-collection-2"},
	}
}

func openTypedColumnRowsPerGranuleFixtureBatches3088(tb testing.TB, sortKey []ColumnSortKey, rowsPerGranule int, batches [][]columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func()) {
	tb.Helper()
	tb.Setenv(typedColumnBenchmarkRowsPerGranuleEnv, strconv.Itoa(rowsPerGranule))
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	cfg := typedColumnSortKeyConfig1948(sortKey)
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	for batchIdx, batch := range batches {
		ids := make([][]byte, len(batch))
		docs := make([][]byte, len(batch))
		for i, event := range batch {
			ids[i] = []byte(event.ID)
			docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch[%d]: %v", batchIdx, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint before reopen: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("Close before reopen: %v", err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open reopened DB: %v", err)
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		tb.Fatalf("OpenCollection reopened: %v", err)
	}
	return reopen, reopened, func() { _ = reopen.Close() }
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
