package collections

import (
	"fmt"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedColumnInt64ScanEqualityPredicate(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{20, 20})
	if result.Diagnostics.Fallback || result.Diagnostics.DirectTypedColumnAssetReads == 0 || result.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("diagnostics=%+v want direct typed-column path without reconstruction", result.Diagnostics)
	}
	if result.Diagnostics.RowsScanned != 4 || result.Diagnostics.RowsMatched != 2 {
		t.Fatalf("diagnostics=%+v want rows scanned=4 matched=2", result.Diagnostics)
	}
}

func TestTypedColumnInt64ScanRangePredicate(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{5, 10, 15, 20, 25})

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 10, High: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{10, 15, 20})
	if result.Diagnostics.BlocksDecoded == 0 || result.Diagnostics.DecodedHeapCopyBytes == 0 || result.Diagnostics.DecodedMetadataBytes == 0 {
		t.Fatalf("diagnostics=%+v want decoded block and metadata bytes", result.Diagnostics)
	}
}

func TestTypedColumnInt64ScanPrunesWithMinMaxMetadata(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	insertTypedColumnInt64ScanRows(t, col, []int64{100, 101, 102})

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 100, High: 102})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{100, 101, 102})
	if result.Diagnostics.PartsPruned == 0 || result.Diagnostics.BlocksPruned == 0 || result.Diagnostics.PartsDecoded == 0 {
		t.Fatalf("diagnostics=%+v want min/max pruning and decoded matching part", result.Diagnostics)
	}
	if result.Diagnostics.RowsScanned >= 6 {
		t.Fatalf("rows_scanned=%d want pruned below full row count diagnostics=%+v", result.Diagnostics.RowsScanned, result.Diagnostics)
	}
}

func TestTypedColumnInt64ScanAllPrunedNoMatch(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 11, 12})

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 99})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	if len(result.Rows) != 0 || result.Diagnostics.RowsMatched != 0 || result.Diagnostics.RowsScanned != 0 {
		t.Fatalf("result=%+v diagnostics=%+v want all pruned no match", result.Rows, result.Diagnostics)
	}
	if result.Diagnostics.PartsPruned != 2 || result.Diagnostics.BlocksPruned == 0 || result.Diagnostics.BlocksDecoded != 0 {
		t.Fatalf("diagnostics=%+v want all parts/blocks pruned and no decoded blocks", result.Diagnostics)
	}
}

func TestTypedColumnInt64ScanFallbackWhenTypedColumnUnsupported(t *testing.T) {
	d := openTypedColumnInt64ScanDB(t)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	insertTypedColumnInt64ScanRows(t, col, []int64{7, 8, 9})

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 8, High: 9})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan fallback: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{8, 9})
	if !result.Diagnostics.Fallback || result.Diagnostics.FallbackReason != "typed_column_not_selected" || result.Diagnostics.DirectTypedColumnAssetReads != 0 {
		t.Fatalf("diagnostics=%+v want typed-row fallback", result.Diagnostics)
	}
}

func TestTypedColumnInt64ScanStaleMetadataFailsClosed(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	corruptTypedColumnAssetPayload1755(t, d, typedRefs[0])

	if _, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2}); err == nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan err=nil want fail-closed corrupt ref/checksum metadata")
	}
}

func TestTypedColumnInt64ScanReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnInt64ScanCollection(t, d)
	insertTypedColumnInt64ScanRows(t, col, []int64{11, 12, 13})
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	result, err := reopenedCol.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 12, High: 13})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan reopened: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{12, 13})
	if result.Diagnostics.DirectTypedColumnAssetReads == 0 || result.Diagnostics.MappedBytes+result.Diagnostics.HeapCopyBytes == 0 {
		t.Fatalf("diagnostics=%+v want durable typed_column_part asset reads after reopen", result.Diagnostics)
	}
}

func BenchmarkTypedColumnInt64PredicateScan(b *testing.B) {
	const rows = 4096
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i % 1024)
	}
	b.Run("typed_column_part", func(b *testing.B) {
		d, col := setupTypedColumnInt64ScanCollection(b)
		defer func() { _ = d.Close() }()
		insertTypedColumnInt64ScanRows(b, col, values)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 100, High: 199, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
			if err != nil {
				b.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
			}
			if i == b.N-1 {
				reportTypedColumnInt64ScanBenchMetrics(b, result.Diagnostics)
			}
		}
	})
	b.Run("document_full_scan_fallback", func(b *testing.B) {
		d := openTypedColumnInt64ScanDB(b)
		defer func() { _ = d.Close() }()
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events"}); err != nil {
			b.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("events")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		insertTypedColumnInt64ScanRows(b, col, values)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 100, High: 199})
			if err != nil {
				b.Fatalf("RunTypedColumnInt64PredicateScan fallback: %v", err)
			}
			if i == b.N-1 {
				reportTypedColumnInt64ScanBenchMetrics(b, result.Diagnostics)
			}
		}
	})
}

func reportTypedColumnInt64ScanBenchMetrics(b *testing.B, diag TypedColumnInt64PredicateScanDiagnostics) {
	b.Helper()
	if diag.ScanNanos > 0 {
		b.ReportMetric(float64(1_000_000_000)/float64(diag.ScanNanos), "ops/sec")
	}
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.DecodedHeapCopyBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.PartsPruned), "parts_pruned/op")
	b.ReportMetric(float64(diag.BlocksPruned), "blocks_pruned/op")
}

func setupTypedColumnInt64ScanCollection(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	return d, createTypedColumnInt64ScanCollection(tb, d)
}

func openTypedColumnInt64ScanDB(tb testing.TB) *backenddb.DB {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	return openCollectionCommandWALDB(tb, dir)
}

func createTypedColumnInt64ScanCollection(tb testing.TB, d *backenddb.DB) *Collection {
	tb.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset, Dictionary: true},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func insertTypedColumnInt64ScanRows(tb testing.TB, col *Collection, values []int64) {
	tb.Helper()
	ids := make([][]byte, len(values))
	docs := make([][]byte, len(values))
	for i, value := range values {
		ids[i] = []byte(fmt.Sprintf("e%06d_%d", value, i))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"k%d"}`, value, value%8))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
}

func assertTypedColumnInt64ScanValues(t *testing.T, result TypedColumnInt64PredicateScanResult, want []int64) {
	t.Helper()
	got := make([]int64, len(result.Rows))
	for i, row := range result.Rows {
		got[i] = row.Value
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want = append([]int64(nil), want...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("values=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("values=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
		}
	}
}
