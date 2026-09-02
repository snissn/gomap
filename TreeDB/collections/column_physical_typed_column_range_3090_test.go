package collections

import "testing"

func TestColumnPhysicalDenseTypedColumnTargetedRangeReads3090(t *testing.T) {
	t.Run("q1_group_count", func(t *testing.T) {
		batches := columnPhysicalQ1DenseEventBatches1950([][]string{
			{"app.m", "app.z", "app.feed", "app.m", "app.graph", "app.m"},
			{"app.a", "app.m", "app.chat", "app.a", "app.video", "app.m"},
		})
		_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
		defer closeFn()

		totalRows := totalQ1DenseRows1950(batches)
		want := rowScanCollectionCounts1950(t, col, totalRows)
		req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify}
		full, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q1 verify): %v", err)
		}
		assertColumnPhysicalQ1DenseResult1950(t, "q1 verify", full, want, totalRows, totalRows)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		ranged, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q1 skip checksums): %v", err)
		}
		assertColumnPhysicalQ1DenseResult1950(t, "q1 targeted ranges", ranged, want, totalRows, totalRows)
		assertColumnPhysicalTargetedRangeBytes3090(t, "q1", full.Diagnostics, ranged.Diagnostics)
	})

	t.Run("q2_group_count_distinct", func(t *testing.T) {
		batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2LocalDictBatchA1950(), typedColumnQ2LocalDictBatchB1950()}
		events := flattenColumnPhysicalEvents1950(batches)
		_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
		defer closeFn()

		req := typedColumnQ2Request1950()
		rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
		want := columnPhysicalJSONBenchQ2ReferenceCountsP0(events)
		matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityVerify
		full, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q2 verify): %v", err)
		}
		assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "q2 verify", full, rowHash, want)
		assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(t, "q2 verify", full.Diagnostics, len(events), matchedRows, columnTypedColumnDenseGroupCountDistinctReducerPairBitset)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		ranged, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q2 skip checksums): %v", err)
		}
		assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "q2 targeted ranges", ranged, rowHash, want)
		assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(t, "q2 targeted ranges", ranged.Diagnostics, len(events), matchedRows, columnTypedColumnDenseGroupCountDistinctReducerPairBitset)
		assertColumnPhysicalTargetedRangeBytes3090(t, "q2", full.Diagnostics, ranged.Diagnostics)
	})

	t.Run("q3_group_hour_count", func(t *testing.T) {
		batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
		events := flattenColumnPhysicalEvents1950(batches)
		_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
		defer closeFn()

		scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
		req := columnPhysicalQ3DenseRequest1950()
		want := columnPhysicalQ3DenseReferenceGroups1950(scanned)
		matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", scanned)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityVerify
		full, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q3 verify): %v", err)
		}
		assertColumnPhysicalQ3DenseResult1950(t, "q3 verify", full, want, len(events), matchedRows, matchedRows)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		ranged, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q3 skip checksums): %v", err)
		}
		assertColumnPhysicalQ3DenseResult1950(t, "q3 targeted ranges", ranged, want, len(events), matchedRows, matchedRows)
		assertColumnPhysicalTargetedRangeBytes3090(t, "q3", full.Diagnostics, ranged.Diagnostics)
	})

	t.Run("q4_time_order_topk", func(t *testing.T) {
		batches := columnPhysicalQ4ATimeOrderBatches1950(9_000)
		events := flattenColumnPhysicalEvents1950(batches)
		_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, []ColumnSortKey{{Column: "time_us"}}, batches)
		defer closeFn()

		scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
		req := columnPhysicalQ4ATimeOrderRequest1950()
		want := columnPhysicalQ4ATimeOrderReferenceGroups1950(scanned, req.TopK)
		wantCandidates := columnPhysicalQ4ATimeOrderReferenceEarlyCandidates1950(scanned, req.TopK)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityVerify
		full, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q4 verify): %v", err)
		}
		assertColumnPhysicalQ4ATimeOrderResult1950(t, "q4 verify", full, want, len(events), wantCandidates)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		ranged, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q4 skip checksums): %v", err)
		}
		assertColumnPhysicalQ4ATimeOrderResult1950(t, "q4 targeted ranges", ranged, want, len(events), wantCandidates)
		assertColumnPhysicalTargetedRangeBytes3090(t, "q4", full.Diagnostics, ranged.Diagnostics)
	})

	t.Run("q5_group_int64_span", func(t *testing.T) {
		batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
		events := flattenColumnPhysicalEvents1950(batches)
		_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
		defer closeFn()

		scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
		req := columnPhysicalQ5DenseRequest1950()
		want := columnPhysicalQ5DenseReferenceGroups1950(scanned, req.TopK)
		matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityVerify
		full, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q5 verify): %v", err)
		}
		assertColumnPhysicalQ5DenseResult1950(t, "q5 verify", full, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)

		req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		ranged, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q5 skip checksums): %v", err)
		}
		assertColumnPhysicalQ5DenseResult1950(t, "q5 targeted ranges", ranged, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)
		assertColumnPhysicalTargetedRangeBytes3090(t, "q5", full.Diagnostics, ranged.Diagnostics)
	})
}

func assertColumnPhysicalTargetedRangeBytes3090(tb testing.TB, label string, full, ranged ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if full.ColumnAssetReadIntegrity != string(ColumnAssetReadIntegrityVerify) {
		tb.Fatalf("%s full-read integrity=%q want %q diagnostics=%+v", label, full.ColumnAssetReadIntegrity, ColumnAssetReadIntegrityVerify, full)
	}
	if ranged.ColumnAssetReadIntegrity != string(ColumnAssetReadIntegritySkipChecksums) {
		tb.Fatalf("%s ranged integrity=%q want %q diagnostics=%+v", label, ranged.ColumnAssetReadIntegrity, ColumnAssetReadIntegritySkipChecksums, ranged)
	}
	if full.PhysicalBytesScanned <= 0 || ranged.PhysicalBytesScanned <= 0 {
		tb.Fatalf("%s physical bytes full=%d ranged=%d want positive diagnostics full=%+v ranged=%+v", label, full.PhysicalBytesScanned, ranged.PhysicalBytesScanned, full, ranged)
	}
	if ranged.PhysicalBytesScanned >= full.PhysicalBytesScanned {
		tb.Fatalf("%s targeted-range physical bytes=%d want below verify/full bytes=%d diagnostics full=%+v ranged=%+v", label, ranged.PhysicalBytesScanned, full.PhysicalBytesScanned, full, ranged)
	}
	if ranged.RowMaterializations != 0 || ranged.DocumentMaterializations != 0 {
		tb.Fatalf("%s targeted ranges materialized rows/documents: %+v", label, ranged)
	}
}
