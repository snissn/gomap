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
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q3 first", 1, 1, 2, 2, 1)

	q3Second, err := col.RunColumnPhysicalQuery(q3Req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q3): %v", err)
	}
	assertColumnPhysicalQ3DenseResult1950(t, "q3 second one-shot", q3Second, q3Want, len(events), q3MatchedRows, q3MatchedRows)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q3 second", 1, 2, 2, 2, 1)
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
