package collections

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
)

func TestTypedColumnBoolPreparedCountsAndDiagnostics(t *testing.T) {
	d, col := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d.Close() }()
	insertTypedColumnBoolScanRows(t, col, []bool{true, false, true, true, false})

	tests := []struct {
		name       string
		req        TypedColumnBoolPredicateAggregateRequest
		rows       int64
		trueCount  int64
		falseCount int64
	}{
		{name: "all", req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}, rows: 5, trueCount: 3, falseCount: 2},
		{name: "equal_true", req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: true, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}, rows: 3, trueCount: 3, falseCount: 0},
		{name: "equal_false", req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: false, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}, rows: 2, trueCount: 0, falseCount: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			session, err := col.PrepareTypedColumnBoolPredicateAggregate(tc.req)
			if err != nil {
				t.Fatalf("PrepareTypedColumnBoolPredicateAggregate: %v", err)
			}
			defer func() { _ = session.Close() }()
			result, err := session.Run()
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			assertTypedColumnBoolAggregate(t, result, tc.rows, tc.trueCount, tc.falseCount)
			if result.Diagnostics.Fallback || result.Diagnostics.KernelBlocks == 0 || result.Diagnostics.BlocksDecoded == 0 || result.Diagnostics.RowMaterializations != 0 {
				t.Fatalf("diagnostics=%+v want prepared kernel path without document materialization", result.Diagnostics)
			}
			if result.Diagnostics.StatsFallbackBlocks == 0 || result.Diagnostics.StatsFallbackReason != "stats_payload_unsupported" {
				t.Fatalf("diagnostics=%+v want explicit bool stats fallback", result.Diagnostics)
			}
			if tc.req.Kind == TypedColumnBoolPredicateEqual && (result.Diagnostics.PruningFallbackBlocks == 0 || !strings.Contains(result.Diagnostics.PruningFallbackReason, string(columnsemantics.ReasonPruningPayloadUnsupported))) {
				t.Fatalf("diagnostics=%+v want explicit bool pruning fallback", result.Diagnostics)
			}
		})
	}
}

func TestTypedColumnBoolPreparedForegroundLifetimeIdleAndRun(t *testing.T) {
	d, col := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d.Close() }()
	insertTypedColumnBoolScanRows(t, col, []bool{true, false, true})

	begins, ends, active := 0, 0, 0
	unregister := d.RegisterForegroundReadObserver(func() {}, func() func() {
		begins++
		active++
		return func() {
			ends++
			active--
		}
	})
	defer unregister()

	session, err := col.PrepareTypedColumnBoolPredicateAggregate(TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnBoolPredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	if session.view.snapshot != nil || active != 0 || begins != ends {
		t.Fatalf("prepared bool snapshot=%p foreground begin/end/active=%d/%d/%d want nil balanced idle", session.view.snapshot, begins, ends, active)
	}
	before := begins
	if _, err := session.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if active != 0 || begins != ends || begins != before+1 {
		t.Fatalf("foreground begin/end/active after bool Run=%d/%d/%d want one balanced operation", begins, ends, active)
	}
}

func TestTypedColumnBoolOneShotVerifyUsesBoolKernel(t *testing.T) {
	d, col := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d.Close() }()
	insertTypedColumnBoolScanRows(t, col, []bool{true, false, true, false})

	result, err := col.RunTypedColumnBoolPredicateAggregate(TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnBoolPredicateAggregate: %v", err)
	}
	assertTypedColumnBoolAggregate(t, result, 4, 2, 2)
	if result.Diagnostics.Fallback || result.Diagnostics.KernelBlocks == 0 || result.Diagnostics.ColumnAssetReadIntegrity != string(ColumnAssetReadIntegrityVerify) {
		t.Fatalf("diagnostics=%+v want one-shot verify bool kernel path", result.Diagnostics)
	}
}

func TestTypedColumnBoolPreparedAllPrunedRLE(t *testing.T) {
	d, col := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d.Close() }()
	insertTypedColumnBoolScanRows(t, col, []bool{true, true, true, true, true, true})

	session, err := col.PrepareTypedColumnBoolPredicateAggregate(TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: false, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnBoolPredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertTypedColumnBoolAggregate(t, result, 0, 0, 0)
	if result.Diagnostics.BlocksPruned == 0 || result.Diagnostics.BlocksDecoded != 0 || result.Diagnostics.RowsScanned != 0 {
		t.Fatalf("diagnostics=%+v want min/max all-pruned bool block", result.Diagnostics)
	}
}

func TestTypedColumnBoolVisibilityDeleteComposition(t *testing.T) {
	d, col := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d.Close() }()
	ids := insertTypedColumnBoolScanRows(t, col, []bool{true, true, false, true})
	if err := col.Delete(ids[1]); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	session, err := col.PrepareTypedColumnBoolPredicateAggregate(TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: true, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnBoolPredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertTypedColumnBoolAggregate(t, result, 2, 2, 0)
	if result.Diagnostics.SelectionCompositions == 0 || result.Diagnostics.MutationParts == 0 {
		t.Fatalf("diagnostics=%+v want visibility/delete composition", result.Diagnostics)
	}
}

func TestTypedColumnBoolNullableMissingAndUnsupportedOperationsFailClosed(t *testing.T) {
	d, col := setupTypedColumnBoolScanCollection(t, true)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("b-null-1"), []byte("b-null-2"), []byte("b-null-3")}
	docs := [][]byte{[]byte(`{"flag":true}`), []byte(`{"flag":false}`), []byte(`{"flag":null}`)}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch nullable: %v", err)
	}
	_, err := col.PrepareTypedColumnBoolPredicateAggregate(TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: false, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "nullable") {
		t.Fatalf("nullable prepare err=%v want fail-closed nullable diagnostic", err)
	}

	d2, col2 := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d2.Close() }()
	_, err = col2.InsertBatch([][]byte{[]byte("b-missing-1"), []byte("b-missing-2")}, [][]byte{[]byte(`{"flag":true}`), []byte(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "missing non-null declared value") {
		t.Fatalf("InsertBatch missing non-null bool err=%v want fail-closed missing diagnostic", err)
	}

	d3, col3 := setupTypedColumnBoolScanCollection(t, false)
	defer func() { _ = d3.Close() }()
	insertTypedColumnBoolScanRows(t, col3, []bool{true, false})
	_, err = col3.PrepareTypedColumnBoolPredicateAggregate(TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateRange, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), string(columnsemantics.ReasonBoolRangeUnsupported)) {
		t.Fatalf("range prepare err=%v want bool range unsupported", err)
	}
}

func BenchmarkTypedColumnBoolPredicateAggregate(b *testing.B) {
	rows := 65536
	if raw := strings.TrimSpace(os.Getenv("TREEDB_TYPED_COLUMN_BOOL_BENCH_ROWS")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			b.Fatalf("TREEDB_TYPED_COLUMN_BOOL_BENCH_ROWS=%q", raw)
		}
		rows = parsed
	}
	shapes := []typedColumnBoolBenchShape{
		{name: "all_mixed_counts", valueAt: func(row, rows int) bool { return row%3 == 0 || row%17 == 0 }, req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateAll}},
		{name: "equal_true_selective", valueAt: func(row, rows int) bool { return row%100 == 0 }, req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: true}},
		{name: "equal_true_all_match_rle", valueAt: func(row, rows int) bool { return true }, req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: true}},
		{name: "equal_true_all_pruned_rle", valueAt: func(row, rows int) bool { return false }, req: TypedColumnBoolPredicateAggregateRequest{Column: "flag", Kind: TypedColumnBoolPredicateEqual, Value: true}},
	}
	for _, shape := range shapes {
		shape := shape
		b.Run(fmt.Sprintf("rows_%d/shape_%s/timed_prepared_session_hot_scan/read_integrity_cached_verify", rows, shape.name), func(b *testing.B) {
			d, col := setupTypedColumnBoolScanCollection(b, false)
			defer func() { _ = d.Close() }()
			values := make([]bool, rows)
			for i := range values {
				values[i] = shape.valueAt(i, rows)
			}
			insertTypedColumnBoolScanRowsBatched(b, col, values)
			expectedRows, expectedTrue, expectedFalse := expectedTypedColumnBoolBenchCounts(values, shape.req)
			req := shape.req
			req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityCachedVerify
			session, err := col.PrepareTypedColumnBoolPredicateAggregate(req)
			if err != nil {
				b.Fatalf("PrepareTypedColumnBoolPredicateAggregate: %v", err)
			}
			warmup, err := session.Run()
			if err != nil {
				_ = session.Close()
				b.Fatalf("warmup Run: %v", err)
			}
			if err := validateTypedColumnBoolBenchResult(warmup, expectedRows, expectedTrue, expectedFalse); err != nil {
				_ = session.Close()
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			stopHotCPUProfile := startTypedColumnInt64AggregateBenchHotCPUProfile(b)
			start := time.Now()
			var result TypedColumnBoolPredicateAggregateResult
			for i := 0; i < b.N; i++ {
				result, err = session.Run()
				if err != nil {
					b.Fatalf("Run: %v", err)
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()
			if stopHotCPUProfile != nil {
				stopHotCPUProfile()
			}
			if err := session.Close(); err != nil {
				b.Fatalf("Close: %v", err)
			}
			if err := validateTypedColumnBoolBenchResult(result, expectedRows, expectedTrue, expectedFalse); err != nil {
				b.Fatal(err)
			}
			reportTypedColumnBoolBenchMetrics(b, rows, result, elapsed)
		})
	}
}

type typedColumnBoolBenchShape struct {
	name    string
	valueAt func(row, rows int) bool
	req     TypedColumnBoolPredicateAggregateRequest
}

func setupTypedColumnBoolScanCollection(tb testing.TB, nullable bool) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "flag", Path: "flag", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart, Nullable: nullable}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "bools", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("bools")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col
}

func insertTypedColumnBoolScanRows(tb testing.TB, col *Collection, values []bool) [][]byte {
	tb.Helper()
	ids := make([][]byte, len(values))
	docs := make([][]byte, len(values))
	for i, value := range values {
		ids[i] = []byte(fmt.Sprintf("b%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"flag":%t}`, value))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
	return ids
}

func insertTypedColumnBoolScanRowsBatched(tb testing.TB, col *Collection, values []bool) {
	tb.Helper()
	const batchRows = 32768
	for start := 0; start < len(values); start += batchRows {
		end := start + batchRows
		if end > len(values) {
			end = len(values)
		}
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i, value := range values[start:end] {
			row := start + i
			ids[i] = []byte(fmt.Sprintf("bench_b%012d", row))
			docs[i] = []byte(fmt.Sprintf(`{"flag":%t}`, value))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			tb.Fatalf("InsertBatch batch %d:%d: %v", start, end, err)
		}
	}
}

func assertTypedColumnBoolAggregate(t *testing.T, result TypedColumnBoolPredicateAggregateResult, rows int64, trueCount int64, falseCount int64) {
	t.Helper()
	if result.Rows != rows || result.NonNulls != rows || result.TrueCount != trueCount || result.FalseCount != falseCount {
		t.Fatalf("bool aggregate rows=%d non_nulls=%d true=%d false=%d want rows=%d true=%d false=%d diagnostics=%+v", result.Rows, result.NonNulls, result.TrueCount, result.FalseCount, rows, trueCount, falseCount, result.Diagnostics)
	}
}

func expectedTypedColumnBoolBenchCounts(values []bool, req TypedColumnBoolPredicateAggregateRequest) (int64, int64, int64) {
	var rows, trueCount, falseCount int64
	for _, value := range values {
		if req.Kind == TypedColumnBoolPredicateEqual && value != req.Value {
			continue
		}
		rows++
		if value {
			trueCount++
		} else {
			falseCount++
		}
	}
	return rows, trueCount, falseCount
}

func validateTypedColumnBoolBenchResult(result TypedColumnBoolPredicateAggregateResult, rows int64, trueCount int64, falseCount int64) error {
	if result.Rows != rows || result.NonNulls != rows || result.TrueCount != trueCount || result.FalseCount != falseCount {
		return fmt.Errorf("bool aggregate rows=%d non_nulls=%d true=%d false=%d want rows=%d true=%d false=%d diagnostics=%+v", result.Rows, result.NonNulls, result.TrueCount, result.FalseCount, rows, trueCount, falseCount, result.Diagnostics)
	}
	return nil
}

func reportTypedColumnBoolBenchMetrics(b *testing.B, rows int, result TypedColumnBoolPredicateAggregateResult, elapsed time.Duration) {
	b.Helper()
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		return
	}
	ops := float64(b.N) / seconds
	b.ReportMetric(ops, "ops/sec")
	b.ReportMetric(float64(rows*b.N)/seconds, "rows/sec")
	b.ReportMetric(float64(result.Rows*int64(b.N))/seconds, "matches/sec")
	diag := result.Diagnostics
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.DecodedHeapCopyBytes+diag.MaterializedBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.SelectionAllBlocks), "selection_all_blocks/op")
	b.ReportMetric(float64(diag.SelectionEmptyBlocks), "selection_empty_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangeBlocks+diag.SelectionRangesBlocks), "selection_range_blocks/op")
	b.ReportMetric(float64(diag.SelectionBitmapBlocks), "selection_bitmap_blocks/op")
	b.ReportMetric(float64(diag.SelectionSparseBlocks), "selection_sparse_blocks/op")
	b.ReportMetric(float64(diag.KernelBlocks), "kernel_blocks/op")
	b.ReportMetric(float64(diag.KernelFullCoveredBlocks), "kernel_full_blocks/op")
	b.ReportMetric(float64(diag.KernelSelectedBlocks), "kernel_selected_blocks/op")
	b.ReportMetric(float64(diag.StatsFallbackBlocks), "stats_fallback_blocks/op")
	b.ReportMetric(float64(diag.PruningFallbackBlocks), "pruning_fallback_blocks/op")
	b.ReportMetric(float64(diag.BlocksPruned), "blocks_pruned/op")
}
