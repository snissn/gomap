package collections

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedColumnStringScanEqualityPredicate1785(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	// Two generations exercise per-part dictionary resolution: "like" is code 0
	// in the first part (like/share) and code 1 in the second (alpha/like).
	insertTypedColumnStringScanRows1785(t, col, []string{"like", "share"})
	insertTypedColumnStringScanRows1785(t, col, []string{"alpha", "like"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "like"})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, result, []string{"like", "like"})
	if result.Diagnostics.Fallback || result.Diagnostics.DirectTypedColumnAssetReads == 0 || result.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("diagnostics=%+v want direct typed-column path without reconstruction", result.Diagnostics)
	}
	if result.Diagnostics.RowsScanned != 4 || result.Diagnostics.RowsMatched != 2 || result.Diagnostics.CodesMatched != 2 {
		t.Fatalf("diagnostics=%+v want rows scanned=4 matched=2 codes=2", result.Diagnostics)
	}
	if result.Diagnostics.DictionaryBytesDecoded == 0 || result.Diagnostics.DecodedHeapCopyBytes == 0 {
		t.Fatalf("diagnostics=%+v want dictionary and code/id block decodes", result.Diagnostics)
	}
	for _, row := range result.Rows {
		if len(row.DocumentID) == 0 || row.PartID != columnPhysicalRowAssetPartID {
			t.Fatalf("row=%+v want physical row asset identity and document id", row)
		}
	}
}

func TestTypedColumnStringScanNoMatchAllPruned1785(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"alpha", "beta"})
	insertTypedColumnStringScanRows1785(t, col, []string{"gamma", "delta"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "missing"})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan: %v", err)
	}
	if len(result.Rows) != 0 || result.Diagnostics.RowsMatched != 0 || result.Diagnostics.RowsScanned != 0 {
		t.Fatalf("result=%+v diagnostics=%+v want all pruned no match", result.Rows, result.Diagnostics)
	}
	if result.Diagnostics.PartsPruned != 2 || result.Diagnostics.BlocksDecoded != 0 || result.Diagnostics.DirectTypedColumnAssetReads != 2 {
		t.Fatalf("diagnostics=%+v want dictionary-absent part pruning", result.Diagnostics)
	}
	if result.Diagnostics.DictionaryBytesDecoded == 0 {
		t.Fatalf("diagnostics=%+v want per-part dictionary decode accounting", result.Diagnostics)
	}
}

func TestTypedColumnStringScanNullableTypedColumnUnsupportedFailsClosed1785(t *testing.T) {
	d := openTypedColumnInt64ScanDB(t)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Nullable: true, Owner: TypedStorageOwnerColumnPart}}
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

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "like"})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "nullable=true") {
		t.Fatalf("RunTypedColumnStringPredicateScan err=%v want unsupported nullable typed-column string", err)
	}
	if result.Diagnostics.Fallback || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.FallbackReads != 0 || len(result.Rows) != 0 {
		t.Fatalf("result=%+v want fail-closed without document fallback/materialization", result)
	}
}

func TestTypedColumnStringScanNoDocumentReconstructionDiagnostics1785(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"alpha", "beta", "beta"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "beta"})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, result, []string{"beta", "beta"})
	diag := result.Diagnostics
	if diag.Fallback || diag.FallbackReads != 0 || diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.DocumentReconstructions != 0 {
		t.Fatalf("diagnostics=%+v want no fallback or row/document reconstruction", diag)
	}
	if diag.PhysicalRowIDLookups == 0 || diag.PhysicalRowAssetReads == 0 {
		t.Fatalf("diagnostics=%+v want physical row id lookup only after matches", diag)
	}
}

func TestTypedColumnStringScanReopen1785(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart},
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	insertTypedColumnStringScanRows1785(t, col, []string{"alpha", "beta", "gamma", "beta"})
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
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	result, err := reopenedCol.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "beta"})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan reopened: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, result, []string{"beta", "beta"})
	if result.Diagnostics.DirectTypedColumnAssetReads == 0 || result.Diagnostics.DictionaryBytesDecoded == 0 || result.Diagnostics.MappedBytes+result.Diagnostics.HeapCopyBytes == 0 {
		t.Fatalf("diagnostics=%+v want durable typed_column_part dictionary reads after reopen", result.Diagnostics)
	}
}

func setupTypedColumnStringScanCollection1785(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart},
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
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
	return d, col
}

func insertTypedColumnStringScanRows1785(tb testing.TB, col *Collection, values []string) {
	tb.Helper()
	ids := make([][]byte, len(values))
	docs := make([][]byte, len(values))
	for i, value := range values {
		ids[i] = []byte(fmt.Sprintf("e%06d_%s", i, value))
		docs[i] = []byte(fmt.Sprintf(`{"kind":%q,"time_us":%d}`, value, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
}

func assertTypedColumnStringScanValues1785(t *testing.T, result TypedColumnStringPredicateScanResult, want []string) {
	t.Helper()
	got := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		got[i] = row.Value
	}
	sort.Strings(got)
	want = append([]string(nil), want...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("values=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("values=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
		}
	}
}
