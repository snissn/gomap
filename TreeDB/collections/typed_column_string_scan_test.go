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

func TestTypedColumnStringScanVerifyIntegrityUsesFullTypedAsset1846(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"like", "share", "like"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "like", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityVerify})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, result, []string{"like", "like"})
	if result.Diagnostics.FullAssetReads == 0 || result.Diagnostics.FullAssetReads != result.Diagnostics.DirectTypedColumnAssetReads {
		t.Fatalf("diagnostics=%+v want verify mode to read/checksum full typed-column assets", result.Diagnostics)
	}
	if result.Diagnostics.KernelBlocks == 0 || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.DocumentMaterializations != 0 {
		t.Fatalf("diagnostics=%+v want prepared dictionary kernel without row/document materialization", result.Diagnostics)
	}
}

func TestTypedColumnStringScanInListCategoryPreparedDictionary1846(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"like", "share", "comment"})
	insertTypedColumnStringScanRows1785(t, col, []string{"alpha", "comment", "like"})

	inList, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: TypedColumnStringPredicateInList, Values: []string{"like", "comment"}})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan in-list: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, inList, []string{"like", "comment", "comment", "like"})
	if inList.Diagnostics.KernelBlocks == 0 || inList.Diagnostics.CodesMatched != 4 || inList.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("in-list diagnostics=%+v want prepared dictionary-code kernels without string materialization", inList.Diagnostics)
	}

	category, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: TypedColumnStringPredicateCategory, Values: []string{"share", "alpha"}})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan category: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, category, []string{"share", "alpha"})
	if category.Diagnostics.KernelBlocks == 0 || category.Diagnostics.CodesMatched != 2 {
		t.Fatalf("category diagnostics=%+v want dictionary-code kernel path", category.Diagnostics)
	}
}

func TestTypedColumnStringScanInListEmptyStringUsesValues1846(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"", "like", "comment"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: TypedColumnStringPredicateInList, Values: []string{"", "comment"}})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan empty-string in-list: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, result, []string{"", "comment"})

	_, err = col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: TypedColumnStringPredicateInList, Value: "like"})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "uses Values") {
		t.Fatalf("Value-form in-list err=%v want explicit Values-only rejection", err)
	}
}

func TestTypedColumnStringScanVisibilityDeleteComposition1846(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"like", "share", "like"})
	if deleted, err := col.DeleteDocument([]byte("e000002_like")); err != nil || !deleted {
		t.Fatalf("DeleteDocument deleted=%v err=%v", deleted, err)
	}

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "like"})
	if err != nil {
		t.Fatalf("RunTypedColumnStringPredicateScan: %v", err)
	}
	assertTypedColumnStringScanValues1785(t, result, []string{"like"})
	if result.Diagnostics.MutationParts == 0 || result.Diagnostics.SelectionCompositions == 0 {
		t.Fatalf("diagnostics=%+v want latest-row visibility/delete composition", result.Diagnostics)
	}
	if len(result.Rows) != 1 || string(result.Rows[0].DocumentID) != "e000000_like" {
		t.Fatalf("rows=%+v want only non-deleted like", result.Rows)
	}
}

func TestTypedColumnStringScanLexicalPrefixUnsupported1846(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"apple", "banana"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: TypedColumnStringPredicatePrefix, Prefix: "a"})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "dictionary_order_unproven") {
		t.Fatalf("prefix err=%v want dictionary-order fallback", err)
	}
	if !result.Diagnostics.Fallback || result.Diagnostics.DirectTypedColumnAssetReads != 0 || result.Diagnostics.KernelBlocks != 0 || len(result.Rows) != 0 {
		t.Fatalf("result=%+v want unsafe lexical prefix rejected before numeric code scan", result)
	}
}

func TestTypedColumnStringScanUnknownKindFailsFast1846(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"apple", "banana"})

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: TypedColumnStringPredicateScanKind("contains"), Value: "a"})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "unsupported kind") {
		t.Fatalf("unknown kind err=%v want explicit unsupported kind", err)
	}
	if len(result.Rows) != 0 || result.Diagnostics.DirectTypedColumnAssetReads != 0 || result.Diagnostics.KernelBlocks != 0 {
		t.Fatalf("result=%+v want fail-fast before prepared scan", result)
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
	if diag.Fallback || diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.DocumentReconstructions != 0 {
		t.Fatalf("diagnostics=%+v want no document fallback or row/document reconstruction", diag)
	}
	if diag.PhysicalRowIDLookups == 0 || diag.PhysicalRowAssetReads == 0 {
		t.Fatalf("diagnostics=%+v want physical row id lookup only after matches", diag)
	}
}

func TestTypedColumnStringScanCorruptTypedColumnAssetFailsClosed1785(t *testing.T) {
	d, col := setupTypedColumnStringScanCollection1785(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringScanRows1785(t, col, []string{"alpha", "beta", "beta"})
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed refs=%+v want one", refs)
	}
	corruptTypedColumnAssetPayload1755(t, d, refs[0])

	result, err := col.RunTypedColumnStringPredicateScan(TypedColumnStringPredicateScanRequest{Column: "kind", Value: "beta"})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("RunTypedColumnStringPredicateScan corrupt err=%v want checksum failure", err)
	}
	if result.Diagnostics.Fallback || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.DocumentMaterializations != 0 || result.Diagnostics.DocumentReconstructions != 0 || len(result.Rows) != 0 {
		t.Fatalf("result=%+v want fail-closed without document fallback/materialization", result)
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
