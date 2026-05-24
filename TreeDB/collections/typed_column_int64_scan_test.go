package collections

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestTypedColumnInt64ScanEqualityPredicateSkipsRowLocators(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	values := []int64{10, 20, 30, 20}
	insertTypedColumnInt64ScanRows(t, col, values)

	runResult, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, runResult, []int64{20, 20})
	if runResult.Diagnostics.Fallback {
		t.Fatalf("diagnostics=%+v want direct typed-column path", runResult.Diagnostics)
	}

	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), typedRefs[0])
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	fields := []TypedStorageField{{Name: "time_us", Path: "time_us", ValueType: "int64", Owner: TypedStorageOwnerColumnPart}}
	generic, err := typedColumnAdapterPartFromBytes(typedColumnAdapterOptions{Fields: fields}, raw)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytes: %v", err)
	}
	if got := len(generic.Part.Locators); got != len(values) {
		t.Fatalf("generic locator count=%d want %d", got, len(values))
	}
	prepared, adapterColumn, _, err := typedColumnAdapterPrepareInt64PredicateScanPart(fields, raw, typedRefs[0].PartID, len(values), len(values), uint64(generic.Part.Descriptor.SchemaVersion), "time_us")
	if err != nil {
		t.Fatalf("typedColumnAdapterPrepareInt64PredicateScanPart: %v", err)
	}
	if prepared.Part.Locators != nil {
		t.Fatalf("int64 predicate scan locators loaded: len=%d want nil", len(prepared.Part.Locators))
	}
	var scanResult TypedColumnInt64PredicateScanResult
	partPruned, err := scanTypedColumnInt64PredicatePart(prepared.Part, adapterColumn.Definition.Name, TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20}, typedRefs[0].Generation, typedRefs[0].PartID, &scanResult)
	if err != nil {
		t.Fatalf("scanTypedColumnInt64PredicatePart: %v", err)
	}
	if partPruned {
		t.Fatalf("partPruned=true want decoded matches")
	}
	assertTypedColumnInt64ScanValues(t, scanResult, []int64{20, 20})
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

func TestTypedColumnInt64ScanAllPredicate(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{5, 10, 15})
	insertTypedColumnInt64ScanRows(t, col, []int64{20, 25})

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan all: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{5, 10, 15, 20, 25})
	if result.Diagnostics.RowsScanned != 5 || result.Diagnostics.RowsMatched != 5 || result.Diagnostics.PartsPruned != 0 {
		t.Fatalf("diagnostics=%+v want full scan over all rows", result.Diagnostics)
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

func TestTypedColumnInt64ScanDirectIdentityMatchesPhysicalFallback(t *testing.T) {
	directDB, directCol := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = directDB.Close() }()
	insertTypedColumnInt64ScanRows(t, directCol, []int64{10, 20, 30, 20})
	direct, err := directCol.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("direct scan: %v", err)
	}

	fallbackDB := openTypedColumnInt64ScanDB(t)
	defer func() { _ = fallbackDB.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(fallbackDB)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection fallback: %v", err)
	}
	fallbackCol, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection fallback: %v", err)
	}
	insertTypedColumnInt64ScanRows(t, fallbackCol, []int64{10, 20, 30, 20})
	fallback, err := fallbackCol.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("fallback scan: %v", err)
	}
	if !fallback.Diagnostics.Fallback || direct.Diagnostics.Fallback {
		t.Fatalf("direct fallback=%v fallback fallback=%v", direct.Diagnostics.Fallback, fallback.Diagnostics.Fallback)
	}
	got := typedColumnInt64ScanIdentityStrings(direct)
	want := typedColumnInt64ScanIdentityStrings(fallback)
	if len(got) != len(want) {
		t.Fatalf("direct identities=%v fallback identities=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("direct identities=%v fallback identities=%v", got, want)
		}
	}
	for _, row := range direct.Rows {
		if len(row.DocumentID) == 0 || row.PartID != columnPhysicalRowAssetPartID {
			t.Fatalf("direct row=%+v want physical row asset identity and document id", row)
		}
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

func TestTypedColumnInt64ScanNullableTypedColumnUnsupportedFailsClosed(t *testing.T) {
	d := openTypedColumnInt64ScanDB(t)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Nullable: true, Owner: TypedStorageOwnerColumnPart}}
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

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 0})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "nullable=true") {
		t.Fatalf("RunTypedColumnInt64PredicateScan err=%v want unsupported nullable typed-column int64", err)
	}
	if result.Diagnostics.Fallback || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.FallbackReads != 0 || len(result.Rows) != 0 {
		t.Fatalf("result=%+v want fail-closed without document fallback/materialization", result)
	}
}

func TestTypedColumnInt64AggregateNullableTypedColumnUnsupportedFailsClosed(t *testing.T) {
	d := openTypedColumnInt64ScanDB(t)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Nullable: true, Owner: TypedStorageOwnerColumnPart}}
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

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 0})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "nullable=true") {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate err=%v want unsupported nullable typed-column int64", err)
	}
	if result.Diagnostics.Fallback || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.FallbackReads != 0 || result.Count != 0 || result.Sum != 0 {
		t.Fatalf("result=%+v want fail-closed without document fallback/materialization", result)
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

func TestTypedColumnInt64ScanManifestSetupMismatchFailsClosed(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	col.catalogMu.Lock()
	if col.catalog == nil || col.catalog.meta.Options.ColumnStore == nil || col.catalog.meta.Options.ColumnStore.RecoveryAuthoritativeManifest == nil {
		col.catalogMu.Unlock()
		t.Fatalf("missing cached recovery-authoritative manifest")
	}
	col.catalog.meta.Options.ColumnStore.RecoveryAuthoritativeManifest.Checksum++
	col.catalogMu.Unlock()
	if _, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2}); err == nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan err=nil want fail-closed manifest setup mismatch")
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

func TestTypedColumnInt64AggregateEqualityPredicate(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 2, 40)
}

func TestTypedColumnInt64AggregateRangePredicate(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{5, 10, 15, 20, 25})

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 10, High: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 3, 45)
	if result.Diagnostics.BlocksDecoded == 0 || result.Diagnostics.DecodedHeapCopyBytes == 0 || result.Diagnostics.DecodedMetadataBytes == 0 {
		t.Fatalf("diagnostics=%+v want decoded block and metadata bytes", result.Diagnostics)
	}
}

func TestTypedColumnInt64AggregateAllPredicate(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{5, 10, 15})
	insertTypedColumnInt64ScanRows(t, col, []int64{20, 25})

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate all: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 5, 75)
	if result.Diagnostics.RowsScanned != 5 || result.Diagnostics.RowsMatched != 5 || result.Diagnostics.PartsPruned != 0 {
		t.Fatalf("diagnostics=%+v want full aggregate over all rows", result.Diagnostics)
	}
}

func TestTypedColumnInt64AggregateAllPrunedZero(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 11, 12})

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 99})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 0, 0)
	if result.Diagnostics.RowsScanned != 0 || result.Diagnostics.RowsMatched != 0 || result.Diagnostics.PartsPruned != 2 || result.Diagnostics.BlocksDecoded != 0 {
		t.Fatalf("diagnostics=%+v want all pruned zero aggregate", result.Diagnostics)
	}
}

func TestTypedColumnInt64AggregateReopenDurable(t *testing.T) {
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
	result, err := reopenedCol.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 12, High: 13})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate reopened: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 2, 25)
	if result.Diagnostics.DirectTypedColumnAssetReads == 0 || result.Diagnostics.MappedBytes+result.Diagnostics.HeapCopyBytes == 0 {
		t.Fatalf("diagnostics=%+v want durable typed_column_part asset reads after reopen", result.Diagnostics)
	}
}

func TestTypedColumnInt64AggregateCorruptStaleMismatchFailClosed(t *testing.T) {
	t.Run("corrupt_asset", func(t *testing.T) {
		d, col := setupTypedColumnInt64ScanCollection(t)
		defer func() { _ = d.Close() }()
		insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
		typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
		if len(typedRefs) != 1 {
			t.Fatalf("typed refs=%+v want one", typedRefs)
		}
		corruptTypedColumnAssetPayload1755(t, d, typedRefs[0])
		if _, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2}); err == nil {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate err=nil want fail-closed corrupt asset")
		}
	})
	t.Run("manifest_setup_mismatch", func(t *testing.T) {
		d, col := setupTypedColumnInt64ScanCollection(t)
		defer func() { _ = d.Close() }()
		insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
		col.catalogMu.Lock()
		if col.catalog == nil || col.catalog.meta.Options.ColumnStore == nil || col.catalog.meta.Options.ColumnStore.RecoveryAuthoritativeManifest == nil {
			col.catalogMu.Unlock()
			t.Fatalf("missing cached recovery-authoritative manifest")
		}
		col.catalog.meta.Options.ColumnStore.RecoveryAuthoritativeManifest.Checksum++
		col.catalogMu.Unlock()
		if _, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2}); err == nil {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate err=nil want fail-closed manifest mismatch")
		}
	})
	t.Run("schema_mismatch", func(t *testing.T) {
		d, col := setupTypedColumnInt64ScanCollection(t)
		defer func() { _ = d.Close() }()
		insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
		col.catalogMu.Lock()
		if col.catalog == nil || col.catalog.meta.Options.ColumnStore == nil {
			col.catalogMu.Unlock()
			t.Fatalf("missing cached column store config")
		}
		col.catalog.meta.Options.ColumnStore.SchemaHash++
		col.catalogMu.Unlock()
		if _, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2}); err == nil {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate err=nil want fail-closed schema mismatch")
		}
	})
}

func TestValidateTypedColumnPhysicalAssetPairing(t *testing.T) {
	typedRefs := func(generations ...uint64) map[uint64]columnManifestAssetRefForScan {
		refs := make(map[uint64]columnManifestAssetRefForScan, len(generations))
		for _, generation := range generations {
			refs[generation] = columnManifestAssetRefForScan{
				Ref:    ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Generation: generation, PartID: typedColumnPartAssetPartID},
				Reason: ColumnPublishOperationInsert,
			}
		}
		return refs
	}
	physicalRef := func(generation uint64, reason ColumnPublishOperation) columnManifestAssetRefForScan {
		return columnManifestAssetRefForScan{
			Ref:    ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Generation: generation, PartID: columnPhysicalRowAssetPartID},
			Reason: reason,
		}
	}

	for _, tc := range []struct {
		name      string
		typed     map[uint64]columnManifestAssetRefForScan
		physical  []columnManifestAssetRefForScan
		want      []uint64
		wantError string
	}{
		{
			name:     "paired_generations",
			typed:    typedRefs(1, 2),
			physical: []columnManifestAssetRefForScan{physicalRef(1, ColumnPublishOperationInsert), physicalRef(2, ColumnPublishOperationInsert)},
			want:     []uint64{1, 2},
		},
		{
			name:      "rejects_non_insert_physical_ref",
			typed:     typedRefs(1),
			physical:  []columnManifestAssetRefForScan{physicalRef(1, ColumnPublishOperationUpdate)},
			wantError: "insert-only physical refs, got update",
		},
		{
			name:      "rejects_missing_typed_ref",
			typed:     typedRefs(1),
			physical:  []columnManifestAssetRefForScan{physicalRef(1, ColumnPublishOperationInsert), physicalRef(2, ColumnPublishOperationInsert)},
			wantError: "missing typed_column_part asset for generation=2",
		},
		{
			name:      "rejects_duplicate_physical_ref",
			typed:     typedRefs(1),
			physical:  []columnManifestAssetRefForScan{physicalRef(1, ColumnPublishOperationInsert), physicalRef(1, ColumnPublishOperationInsert)},
			wantError: "duplicate physical row asset ref for generation=1",
		},
		{
			name:      "rejects_missing_physical_ref",
			typed:     typedRefs(1, 2),
			physical:  []columnManifestAssetRefForScan{physicalRef(1, ColumnPublishOperationInsert)},
			wantError: "missing physical row asset for typed_column_part generation=2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateTypedColumnPhysicalAssetPairing(tc.typed, tc.physical)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("validateTypedColumnPhysicalAssetPairing err=%v want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateTypedColumnPhysicalAssetPairing: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("physical refs by generation=%v want %v", got, tc.want)
			}
			for _, generation := range tc.want {
				if _, ok := got[generation]; !ok {
					t.Fatalf("physical refs by generation=%v missing %d", got, generation)
				}
			}
		})
	}
}

func TestTypedColumnInt64AggregateMissingPhysicalRefFailsClosed(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotViewWithSidecars: %v", err)
	}
	if len(view.AssetRefs) == 0 || len(view.TypedColumnPartRefs) == 0 {
		t.Fatalf("view refs asset=%d typed=%d want both", len(view.AssetRefs), len(view.TypedColumnPartRefs))
	}
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	view.AssetRefs = nil
	_, err = col.runTypedColumnInt64PredicateAggregateDirect(view, TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 1, High: 3}, cfg, time.Now())
	if err == nil || !strings.Contains(err.Error(), "missing physical row asset") {
		t.Fatalf("runTypedColumnInt64PredicateAggregateDirect err=%v want missing physical row asset", err)
	}
}

func TestTypedColumnInt64AggregateSumOverflowFails(t *testing.T) {
	values := []int64{typedColumnInt64PredicateAggregateMaxSum, 1}
	req := TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 1, High: typedColumnInt64PredicateAggregateMaxSum}

	t.Run("typed_column_part", func(t *testing.T) {
		d, col := setupTypedColumnInt64ScanCollection(t)
		defer func() { _ = d.Close() }()
		insertTypedColumnInt64ScanRows(t, col, values)
		if _, err := col.RunTypedColumnInt64PredicateAggregate(req); err == nil || !strings.Contains(err.Error(), "sum overflow") {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate err=%v want sum overflow", err)
		}
	})

	t.Run("document_full_scan_fallback", func(t *testing.T) {
		d := openTypedColumnInt64ScanDB(t)
		defer func() { _ = d.Close() }()
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events"}); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("events")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		insertTypedColumnInt64ScanRows(t, col, values)
		if _, err := col.RunTypedColumnInt64PredicateAggregate(req); err == nil || !strings.Contains(err.Error(), "sum overflow") {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate fallback err=%v want sum overflow", err)
		}
	})
}

func TestTypedColumnInt64AggregateDirectDiagnosticsNoMaterialization(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 2, 40)
	diag := result.Diagnostics
	if diag.Fallback || diag.DirectTypedColumnAssetReads == 0 || diag.RowLocatorDecodes != 0 || diag.PhysicalRowIDLookups != 0 || diag.PhysicalRowAssetReads != 0 || diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.DocumentReconstructions != 0 {
		t.Fatalf("diagnostics=%+v want direct aggregate with zero row/document materialization", diag)
	}
}

func TestTypedColumnInt64AggregateFallbackDiagnostics(t *testing.T) {
	t.Run("row_asset_owner", func(t *testing.T) {
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

		result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 8, High: 9})
		if err != nil {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate fallback: %v", err)
		}
		assertTypedColumnInt64Aggregate(t, result, 2, 17)
		if !result.Diagnostics.Fallback || result.Diagnostics.FallbackReason != "typed_column_not_selected" || result.Diagnostics.DirectTypedColumnAssetReads != 0 || result.Diagnostics.DocumentMaterializations == 0 {
			t.Fatalf("diagnostics=%+v want document fallback diagnostics", result.Diagnostics)
		}
	})
	t.Run("no_column_store", func(t *testing.T) {
		d := openTypedColumnInt64ScanDB(t)
		defer func() { _ = d.Close() }()
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events"}); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("events")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		insertTypedColumnInt64ScanRows(t, col, []int64{7, 8, 9})

		result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 8, High: 9})
		if err != nil {
			t.Fatalf("RunTypedColumnInt64PredicateAggregate fallback: %v", err)
		}
		assertTypedColumnInt64Aggregate(t, result, 2, 17)
		if !result.Diagnostics.Fallback || result.Diagnostics.FallbackReason != "column_store_unavailable" || result.Diagnostics.DirectTypedColumnAssetReads != 0 || result.Diagnostics.DocumentMaterializations == 0 {
			t.Fatalf("diagnostics=%+v want no-column-store document fallback diagnostics", result.Diagnostics)
		}
	})
}

func TestTypedColumnInt64AggregateBenchParsers(t *testing.T) {
	t.Run("rows", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			input   string
			large   bool
			want    []int
			wantErr string
		}{
			{name: "empty_defaults", input: "", want: []int{4096, 65536}},
			{name: "list", input: "16, 32,64", want: []int{16, 32, 64}},
			{name: "bad", input: "16,nope", wantErr: "invalid row count"},
			{name: "non_positive", input: "0", wantErr: "must be positive"},
			{name: "large_blocked", input: "10000000", wantErr: "TREEDB_TYPED_COLUMN_BENCH_LARGE=true"},
			{name: "large_allowed", input: "10000000,50000000", large: true, want: []int{10000000, 50000000}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := parseTypedColumnInt64AggregateBenchRows(tc.input, tc.large)
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("parseTypedColumnInt64AggregateBenchRows(%q) err=%v want %q", tc.input, err, tc.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("parseTypedColumnInt64AggregateBenchRows(%q): %v", tc.input, err)
				}
				if fmt.Sprint(got) != fmt.Sprint(tc.want) {
					t.Fatalf("rows=%v want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("shapes", func(t *testing.T) {
		got, err := parseTypedColumnInt64AggregateBenchShapes("")
		if err != nil {
			t.Fatalf("default shapes: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchShapeNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"selective_range_1pct", "all_pruned_no_match", "all_match"}) {
			t.Fatalf("default shapes=%v", gotNames)
		}
		got, err = parseTypedColumnInt64AggregateBenchShapes("no_filter, exact,range_1pct,range_10pct,all_pruned,tail")
		if err != nil {
			t.Fatalf("explicit shapes: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchShapeNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"no_filter_full_aggregate", "exact_value", "selective_range_1pct", "wide_range_10pct", "all_pruned_no_match", "tail_range"}) {
			t.Fatalf("explicit shapes=%v", gotNames)
		}
		if _, err := parseTypedColumnInt64AggregateBenchShapes("selective_range_1pct,nope"); err == nil || !strings.Contains(err.Error(), "invalid shape") {
			t.Fatalf("invalid shape err=%v", err)
		}
	})

	t.Run("distributions", func(t *testing.T) {
		got, err := parseTypedColumnInt64AggregateBenchDistributions("")
		if err != nil {
			t.Fatalf("default dists: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchDistributionNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"clustered_monotonic"}) {
			t.Fatalf("default dists=%v", gotNames)
		}
		got, err = parseTypedColumnInt64AggregateBenchDistributions("clustered,reverse,partial_random,random,hotspot")
		if err != nil {
			t.Fatalf("explicit dists: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchDistributionNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"clustered_monotonic", "reverse_monotonic", "partially_clustered", "random_uniform", "hotspot_skewed"}) {
			t.Fatalf("explicit dists=%v", gotNames)
		}
		if _, err := parseTypedColumnInt64AggregateBenchDistributions("clustered_monotonic,nope"); err == nil || !strings.Contains(err.Error(), "invalid distribution") {
			t.Fatalf("invalid dist err=%v", err)
		}
	})

	t.Run("booleans_and_fallback_default", func(t *testing.T) {
		for _, tc := range []struct {
			input   string
			def     bool
			want    bool
			wantErr bool
		}{
			{input: "", def: true, want: true},
			{input: "false", def: true, want: false},
			{input: "true", want: true},
			{input: "not-bool", wantErr: true},
		} {
			got, err := parseTypedColumnInt64AggregateBenchBool("TEST_BOOL", tc.input, tc.def)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "TEST_BOOL") {
					t.Fatalf("parse bool input=%q err=%v", tc.input, err)
				}
				continue
			}
			if err != nil || got != tc.want {
				t.Fatalf("parse bool input=%q got=%v err=%v want=%v", tc.input, got, err, tc.want)
			}
		}
		if !typedColumnInt64AggregateBenchIncludeFallback("", 4096) || !typedColumnInt64AggregateBenchIncludeFallback("", 65536) || typedColumnInt64AggregateBenchIncludeFallback("", 1048576) {
			t.Fatalf("fallback default should include only default rows")
		}
		if !typedColumnInt64AggregateBenchIncludeFallback("true", 1048576) || typedColumnInt64AggregateBenchIncludeFallback("false", 4096) {
			t.Fatalf("fallback explicit override failed")
		}
	})
}

func TestTypedColumnInt64AggregateBenchDistributionDeterminism(t *testing.T) {
	const rows = 257
	for _, dist := range typedColumnInt64AggregateBenchAllDistributions() {
		t.Run(dist.name, func(t *testing.T) {
			first := typedColumnInt64AggregateBenchValues(rows, dist)
			second := typedColumnInt64AggregateBenchValues(rows, dist)
			if fmt.Sprint(first) != fmt.Sprint(second) {
				t.Fatalf("distribution is not deterministic")
			}
			for i, value := range first {
				if value < 0 || value >= rows {
					t.Fatalf("value[%d]=%d outside [0,%d)", i, value, rows)
				}
			}
		})
	}
}

func TestTypedColumnInt64AggregateBenchRandomUniformScattersPowerOfTwoRows(t *testing.T) {
	const rows = 1024
	values := typedColumnInt64AggregateBenchValues(rows, typedColumnInt64AggregateBenchDistributionByName("random_uniform"))
	seen := make(map[int64]struct{}, rows)
	for _, value := range values {
		seen[value] = struct{}{}
	}
	if len(seen) != rows {
		t.Fatalf("random_uniform produced %d unique values, want permutation of %d rows", len(seen), rows)
	}
	minValue, maxValue := values[0], values[0]
	for _, value := range values[:32] {
		if value < minValue {
			minValue = value
		}
		if value > maxValue {
			maxValue = value
		}
	}
	if maxValue-minValue < int64(rows/2) {
		t.Fatalf("first random_uniform window min=%d max=%d does not scatter enough for pruning benchmark", minValue, maxValue)
	}
}

func TestTypedColumnInt64AggregateBenchShapeExpectedAggregates(t *testing.T) {
	const rows = 1000
	values := typedColumnInt64AggregateBenchValues(rows, typedColumnInt64AggregateBenchDistributionByName("clustered_monotonic"))
	for _, tc := range []struct {
		shape     string
		wantCount int64
		wantSum   int64
	}{
		{shape: "no_filter_full_aggregate", wantCount: 1000, wantSum: 499500},
		{shape: "exact_value", wantCount: 1, wantSum: 250},
		{shape: "tiny_range", wantCount: 10, wantSum: 2545},
		{shape: "selective_range_1pct", wantCount: 10, wantSum: 2545},
		{shape: "wide_range_10pct", wantCount: 100, wantSum: 29950},
		{shape: "all_pruned_no_match", wantCount: 0, wantSum: 0},
		{shape: "all_match", wantCount: 1000, wantSum: 499500},
		{shape: "tail_range", wantCount: 10, wantSum: 9945},
	} {
		t.Run(tc.shape, func(t *testing.T) {
			shape := typedColumnInt64AggregateBenchShapeByName(tc.shape)
			req := shape.request(rows)
			got := expectedTypedColumnInt64AggregateBenchResult(values, req)
			if got.count != tc.wantCount || got.sum != tc.wantSum {
				t.Fatalf("expected count=%d sum=%d want count=%d sum=%d req=%+v", got.count, got.sum, tc.wantCount, tc.wantSum, req)
			}
		})
	}

	for _, dist := range typedColumnInt64AggregateBenchAllDistributions() {
		values := typedColumnInt64AggregateBenchValues(rows, dist)
		for _, shape := range typedColumnInt64AggregateBenchAllShapes() {
			req := shape.request(rows)
			expected := expectedTypedColumnInt64AggregateBenchResult(values, req)
			var manual typedColumnInt64AggregateBenchExpected
			for _, value := range values {
				if typedColumnInt64PredicateMatches(typedColumnInt64PredicateAggregateScanRequest(req), value) {
					manual.count++
					manual.sum += value
				}
			}
			manual.finish()
			if expected != manual {
				t.Fatalf("dist=%s shape=%s expected=%+v manual=%+v", dist.name, shape.name, expected, manual)
			}
		}
	}
}

func BenchmarkTypedColumnInt64PredicateAggregate(b *testing.B) {
	large, err := parseTypedColumnInt64AggregateBenchBool("TREEDB_TYPED_COLUMN_BENCH_LARGE", os.Getenv("TREEDB_TYPED_COLUMN_BENCH_LARGE"), false)
	if err != nil {
		b.Fatal(err)
	}
	rowCounts, err := parseTypedColumnInt64AggregateBenchRows(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_ROWS"), large)
	if err != nil {
		b.Fatalf("TREEDB_TYPED_COLUMN_BENCH_ROWS: %v", err)
	}
	shapes, err := parseTypedColumnInt64AggregateBenchShapes(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_SHAPES"))
	if err != nil {
		b.Fatalf("TREEDB_TYPED_COLUMN_BENCH_SHAPES: %v", err)
	}
	dists, err := parseTypedColumnInt64AggregateBenchDistributions(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_DISTS"))
	if err != nil {
		b.Fatalf("TREEDB_TYPED_COLUMN_BENCH_DISTS: %v", err)
	}
	includeFallbackEnv := os.Getenv("TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK")
	if strings.TrimSpace(includeFallbackEnv) != "" {
		if _, err := parseTypedColumnInt64AggregateBenchBool("TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK", includeFallbackEnv, false); err != nil {
			b.Fatal(err)
		}
	}

	for _, rows := range rowCounts {
		rows := rows
		for _, dist := range dists {
			dist := dist
			for _, shape := range shapes {
				shape := shape
				req := shape.request(rows)
				expected := expectedTypedColumnInt64AggregateBenchResultForDistribution(rows, dist, req)
				runTypedColumnInt64AggregateBenchPath(b, rows, dist, shape, req, expected, true)
				if typedColumnInt64AggregateBenchIncludeFallback(includeFallbackEnv, rows) {
					runTypedColumnInt64AggregateBenchPath(b, rows, dist, shape, req, expected, false)
				}
			}
		}
	}
}

type typedColumnInt64AggregateBenchDistribution struct {
	name    string
	valueAt func(row, rows int) int64
}

type typedColumnInt64AggregateBenchShape struct {
	name    string
	request func(rows int) TypedColumnInt64PredicateAggregateRequest
}

type typedColumnInt64AggregateBenchExpected struct {
	count int64
	sum   int64
	avg   float64
}

const typedColumnInt64AggregateBenchBatchRows = 32768

func runTypedColumnInt64AggregateBenchPath(b *testing.B, rows int, dist typedColumnInt64AggregateBenchDistribution, shape typedColumnInt64AggregateBenchShape, req TypedColumnInt64PredicateAggregateRequest, expected typedColumnInt64AggregateBenchExpected, typedPath bool) {
	pathName := "document_full_scan_fallback"
	if typedPath {
		pathName = "typed_column_part"
	}
	b.Run(fmt.Sprintf("rows_%d/dist_%s/path_%s/shape_%s/predicate_count_sum_avg", rows, dist.name, pathName, shape.name), func(b *testing.B) {
		setupStart := time.Now()
		d, col := setupTypedColumnInt64AggregateBenchCollection(b, typedPath)
		defer func() { _ = d.Close() }()
		batches := insertTypedColumnInt64AggregateBenchRows(b, col, rows, dist)
		setupDuration := time.Since(setupStart)
		setupMetrics := collectTypedColumnInt64AggregateBenchSetupMetrics(rows, batches, d, typedPath, setupDuration)

		req.Column = "time_us"
		if typedPath {
			req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityCachedVerify
		}
		b.StopTimer()
		b.ReportAllocs()
		b.ResetTimer()
		reportTypedColumnInt64AggregateBenchSetupMetrics(b, setupMetrics)
		b.StartTimer()
		var result TypedColumnInt64PredicateAggregateResult
		var runErr error
		for i := 0; i < b.N; i++ {
			result, runErr = col.RunTypedColumnInt64PredicateAggregate(req)
		}
		b.StopTimer()
		if runErr != nil {
			b.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", runErr)
		}
		if result.Count != expected.count || result.Sum != expected.sum || result.Avg != expected.avg {
			b.Fatalf("aggregate count=%d sum=%d avg=%f want count=%d sum=%d avg=%f diagnostics=%+v", result.Count, result.Sum, result.Avg, expected.count, expected.sum, expected.avg, result.Diagnostics)
		}
		reportTypedColumnInt64AggregateBenchMetrics(b, rows, result.Diagnostics, b.Elapsed(), b.N)
	})
}

func setupTypedColumnInt64AggregateBenchCollection(tb testing.TB, typedPath bool) (*backenddb.DB, *Collection) {
	tb.Helper()
	if typedPath {
		return setupTypedColumnInt64ScanCollection(tb)
	}
	d := openTypedColumnInt64ScanDB(tb)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events"}); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col
}

func insertTypedColumnInt64AggregateBenchRows(tb testing.TB, col *Collection, rows int, dist typedColumnInt64AggregateBenchDistribution) int {
	tb.Helper()
	batches := 0
	for start := 0; start < rows; start += typedColumnInt64AggregateBenchBatchRows {
		end := start + typedColumnInt64AggregateBenchBatchRows
		if end > rows {
			end = rows
		}
		values := make([]int64, end-start)
		for i := range values {
			values[i] = dist.valueAt(start+i, rows)
		}
		insertTypedColumnInt64AggregateBenchRowsBatch(tb, col, start, values)
		batches++
	}
	return batches
}

func parseTypedColumnInt64AggregateBenchRows(env string, large bool) ([]int, error) {
	if strings.TrimSpace(env) == "" {
		return []int{4096, 65536}, nil
	}
	parts := strings.Split(env, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty row count")
		}
		rows, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid row count %q: %w", part, err)
		}
		if rows <= 0 {
			return nil, fmt.Errorf("row count %d must be positive", rows)
		}
		if rows >= 10000000 && !large {
			return nil, fmt.Errorf("row count %d requires TREEDB_TYPED_COLUMN_BENCH_LARGE=true", rows)
		}
		out = append(out, rows)
	}
	return out, nil
}

func parseTypedColumnInt64AggregateBenchShapes(env string) ([]typedColumnInt64AggregateBenchShape, error) {
	if strings.TrimSpace(env) == "" {
		return []typedColumnInt64AggregateBenchShape{
			typedColumnInt64AggregateBenchShapeByName("selective_range_1pct"),
			typedColumnInt64AggregateBenchShapeByName("all_pruned_no_match"),
			typedColumnInt64AggregateBenchShapeByName("all_match"),
		}, nil
	}
	parts := strings.Split(env, ",")
	out := make([]typedColumnInt64AggregateBenchShape, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("empty shape")
		}
		shape := typedColumnInt64AggregateBenchShapeByName(name)
		if shape.name == "" {
			return nil, fmt.Errorf("invalid shape %q (available: %s)", name, strings.Join(typedColumnInt64AggregateBenchShapeNames(typedColumnInt64AggregateBenchAllShapes()), ","))
		}
		out = append(out, shape)
	}
	return out, nil
}

func parseTypedColumnInt64AggregateBenchDistributions(env string) ([]typedColumnInt64AggregateBenchDistribution, error) {
	if strings.TrimSpace(env) == "" {
		return []typedColumnInt64AggregateBenchDistribution{typedColumnInt64AggregateBenchDistributionByName("clustered_monotonic")}, nil
	}
	parts := strings.Split(env, ",")
	out := make([]typedColumnInt64AggregateBenchDistribution, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("empty distribution")
		}
		dist := typedColumnInt64AggregateBenchDistributionByName(name)
		if dist.name == "" {
			return nil, fmt.Errorf("invalid distribution %q (available: %s)", name, strings.Join(typedColumnInt64AggregateBenchDistributionNames(typedColumnInt64AggregateBenchAllDistributions()), ","))
		}
		out = append(out, dist)
	}
	return out, nil
}

func parseTypedColumnInt64AggregateBenchBool(name, env string, def bool) (bool, error) {
	if strings.TrimSpace(env) == "" {
		return def, nil
	}
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "1", "t", "true", "yes", "y", "on":
		return true, nil
	case "0", "f", "false", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s: invalid boolean %q", name, env)
	}
}

func typedColumnInt64AggregateBenchIncludeFallback(env string, rows int) bool {
	include, err := parseTypedColumnInt64AggregateBenchBool("TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK", env, rows == 4096 || rows == 65536)
	return err == nil && include
}

func typedColumnInt64AggregateBenchAllShapes() []typedColumnInt64AggregateBenchShape {
	return []typedColumnInt64AggregateBenchShape{
		{name: "no_filter_full_aggregate", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll}
		}},
		{name: "exact_value", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: int64(rows / 4)}
		}},
		{name: "tiny_range", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			low := rows / 4
			width := clampIntForTypedColumnInt64AggregateBench(rows/1000, 10, 100)
			if width > rows-low {
				width = rows - low
			}
			if width < 1 {
				width = 1
			}
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(low), High: int64(low + width - 1)}
		}},
		{name: "selective_range_1pct", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			low := rows / 4
			width := maxIntForTypedColumnInt64AggregateBench(1, rows/100)
			if width > rows-low {
				width = rows - low
			}
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(low), High: int64(low + width - 1)}
		}},
		{name: "wide_range_10pct", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			low := rows / 4
			width := maxIntForTypedColumnInt64AggregateBench(1, rows/10)
			if width > rows-low {
				width = rows - low
			}
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(low), High: int64(low + width - 1)}
		}},
		{name: "all_pruned_no_match", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: int64(rows)}
		}},
		{name: "all_match", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 0, High: int64(rows - 1)}
		}},
		{name: "tail_range", request: func(rows int) TypedColumnInt64PredicateAggregateRequest {
			width := maxIntForTypedColumnInt64AggregateBench(1, rows/100)
			if width > rows {
				width = rows
			}
			return TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(rows - width), High: int64(rows - 1)}
		}},
	}
}

func typedColumnInt64AggregateBenchShapeByName(name string) typedColumnInt64AggregateBenchShape {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "no_filter":
		name = "no_filter_full_aggregate"
	case "exact", "equality", "equal":
		name = "exact_value"
	case "tiny":
		name = "tiny_range"
	case "range_1pct", "selective_1pct":
		name = "selective_range_1pct"
	case "range_10pct", "wide_10pct":
		name = "wide_range_10pct"
	case "all_pruned", "no_match":
		name = "all_pruned_no_match"
	case "tail", "latest_window":
		name = "tail_range"
	}
	for _, shape := range typedColumnInt64AggregateBenchAllShapes() {
		if shape.name == name {
			return shape
		}
	}
	return typedColumnInt64AggregateBenchShape{}
}

func typedColumnInt64AggregateBenchAllDistributions() []typedColumnInt64AggregateBenchDistribution {
	return []typedColumnInt64AggregateBenchDistribution{
		{name: "clustered_monotonic", valueAt: func(row, rows int) int64 { return int64(row) }},
		{name: "reverse_monotonic", valueAt: func(row, rows int) int64 { return int64(rows - row - 1) }},
		{name: "partially_clustered", valueAt: func(row, rows int) int64 {
			const chunk = 256
			start := row / chunk * chunk
			end := start + chunk
			if end > rows {
				end = rows
			}
			return int64(start + (end - start - 1 - (row - start)))
		}},
		{name: "random_uniform", valueAt: func(row, rows int) int64 {
			if rows <= 1 {
				return 0
			}
			mul := randomUniformMultiplierForTypedColumnInt64AggregateBench(rows)
			offset := rows/3 + 17
			return int64((row*mul + offset) % rows)
		}},
		{name: "hotspot_skewed", valueAt: func(row, rows int) int64 {
			if rows <= 1 {
				return 0
			}
			span := minIntForTypedColumnInt64AggregateBench(8, rows)
			base := rows/4 - span/2
			if base < 0 {
				base = 0
			}
			if base+span > rows {
				base = rows - span
			}
			if row%5 == 0 {
				return int64(row)
			}
			return int64(base + row%span)
		}},
	}
}

func typedColumnInt64AggregateBenchDistributionByName(name string) typedColumnInt64AggregateBenchDistribution {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "clustered":
		name = "clustered_monotonic"
	case "reverse":
		name = "reverse_monotonic"
	case "partial_random", "partially_random", "partial_clustered":
		name = "partially_clustered"
	case "random":
		name = "random_uniform"
	case "hotspot", "skew", "skewed":
		name = "hotspot_skewed"
	}
	for _, dist := range typedColumnInt64AggregateBenchAllDistributions() {
		if dist.name == name {
			return dist
		}
	}
	return typedColumnInt64AggregateBenchDistribution{}
}

func typedColumnInt64AggregateBenchShapeNames(shapes []typedColumnInt64AggregateBenchShape) []string {
	out := make([]string, len(shapes))
	for i, shape := range shapes {
		out[i] = shape.name
	}
	return out
}

func typedColumnInt64AggregateBenchDistributionNames(dists []typedColumnInt64AggregateBenchDistribution) []string {
	out := make([]string, len(dists))
	for i, dist := range dists {
		out[i] = dist.name
	}
	return out
}

func typedColumnInt64AggregateBenchValues(rows int, dist typedColumnInt64AggregateBenchDistribution) []int64 {
	values := make([]int64, rows)
	for i := range values {
		values[i] = dist.valueAt(i, rows)
	}
	return values
}

func expectedTypedColumnInt64AggregateBenchResult(values []int64, req TypedColumnInt64PredicateAggregateRequest) typedColumnInt64AggregateBenchExpected {
	var out typedColumnInt64AggregateBenchExpected
	scanReq := typedColumnInt64PredicateAggregateScanRequest(req)
	for _, value := range values {
		if typedColumnInt64PredicateMatches(scanReq, value) {
			out.count++
			out.sum += value
		}
	}
	out.finish()
	return out
}

func expectedTypedColumnInt64AggregateBenchResultForDistribution(rows int, dist typedColumnInt64AggregateBenchDistribution, req TypedColumnInt64PredicateAggregateRequest) typedColumnInt64AggregateBenchExpected {
	var out typedColumnInt64AggregateBenchExpected
	scanReq := typedColumnInt64PredicateAggregateScanRequest(req)
	for row := 0; row < rows; row++ {
		value := dist.valueAt(row, rows)
		if typedColumnInt64PredicateMatches(scanReq, value) {
			out.count++
			out.sum += value
		}
	}
	out.finish()
	return out
}

func (e *typedColumnInt64AggregateBenchExpected) finish() {
	if e.count != 0 {
		e.avg = float64(e.sum) / float64(e.count)
	}
}

func randomUniformMultiplierForTypedColumnInt64AggregateBench(rows int) int {
	if rows <= 2 {
		return 1
	}
	mul := int(uint64(6364136223846793005) % uint64(rows))
	if mul == 0 {
		mul = 1
	}
	if mul%2 == 0 {
		mul++
	}
	for gcdIntForTypedColumnInt64AggregateBench(mul, rows) != 1 {
		mul += 2
		if mul >= rows {
			mul = 1
		}
	}
	return mul
}

func gcdIntForTypedColumnInt64AggregateBench(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func maxIntForTypedColumnInt64AggregateBench(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minIntForTypedColumnInt64AggregateBench(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clampIntForTypedColumnInt64AggregateBench(v, low, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

type typedColumnInt64AggregateBenchSetupMetrics struct {
	rows                  int
	batches               int
	setupNanos            int64
	typedColumnAssetBytes int64
	dbDirBytes            int64
}

func collectTypedColumnInt64AggregateBenchSetupMetrics(rows, batches int, d *backenddb.DB, typedPath bool, setupDuration time.Duration) typedColumnInt64AggregateBenchSetupMetrics {
	metrics := typedColumnInt64AggregateBenchSetupMetrics{
		rows:       rows,
		batches:    batches,
		setupNanos: setupDuration.Nanoseconds(),
		dbDirBytes: directorySizeForTypedColumnInt64AggregateBench(d.Dir()),
	}
	if typedPath {
		metrics.typedColumnAssetBytes = directorySizeForTypedColumnInt64AggregateBench(d.ColumnAssetRootDir())
	}
	return metrics
}

func reportTypedColumnInt64AggregateBenchSetupMetrics(b *testing.B, metrics typedColumnInt64AggregateBenchSetupMetrics) {
	b.Helper()
	b.ReportMetric(float64(metrics.rows), "dataset_rows")
	b.ReportMetric(float64(metrics.batches), "setup_batches")
	b.ReportMetric(float64(metrics.setupNanos), "setup_ns")
	if metrics.typedColumnAssetBytes != 0 {
		b.ReportMetric(float64(metrics.typedColumnAssetBytes), "typed_column_asset_bytes")
	}
	b.ReportMetric(float64(metrics.dbDirBytes), "db_dir_bytes")
}

func directorySizeForTypedColumnInt64AggregateBench(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

func BenchmarkTypedColumnInt64PredicateScan(b *testing.B) {
	const rows = 4096
	valuesHot := make([]int64, rows)
	valuesCold := make([]int64, rows)
	for i := range valuesHot {
		valuesHot[i] = int64(i)
		valuesCold[i] = int64(10000 + i)
	}
	b.Run("typed_column_part", func(b *testing.B) {
		d, col := setupTypedColumnInt64ScanCollection(b)
		defer func() { _ = d.Close() }()
		insertTypedColumnInt64ScanRows(b, col, valuesHot)
		insertTypedColumnInt64ScanRows(b, col, valuesCold)
		b.ReportAllocs()
		b.ResetTimer()
		benchStart := time.Now()
		for i := 0; i < b.N; i++ {
			result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 100, High: 199, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
			if err != nil {
				b.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
			}
			if i == b.N-1 {
				reportTypedColumnInt64ScanBenchMetrics(b, result.Diagnostics, time.Since(benchStart), b.N)
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
		insertTypedColumnInt64ScanRows(b, col, valuesHot)
		insertTypedColumnInt64ScanRows(b, col, valuesCold)
		b.ReportAllocs()
		b.ResetTimer()
		benchStart := time.Now()
		for i := 0; i < b.N; i++ {
			result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 100, High: 199})
			if err != nil {
				b.Fatalf("RunTypedColumnInt64PredicateScan fallback: %v", err)
			}
			if i == b.N-1 {
				reportTypedColumnInt64ScanBenchMetrics(b, result.Diagnostics, time.Since(benchStart), b.N)
			}
		}
	})
}

func reportTypedColumnInt64AggregateBenchMetrics(b *testing.B, totalRows int, diag TypedColumnInt64PredicateScanDiagnostics, elapsed time.Duration, iterations int) {
	b.Helper()
	if elapsed > 0 && iterations > 0 {
		b.ReportMetric(float64(iterations)/elapsed.Seconds(), "ops/sec")
		b.ReportMetric(float64(totalRows*iterations)/elapsed.Seconds(), "rows/sec")
		b.ReportMetric(float64(diag.RowsMatched*iterations)/elapsed.Seconds(), "matches/sec")
	}
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(diag.PartsConsidered), "parts_considered/op")
	b.ReportMetric(float64(diag.PartsPruned), "parts_pruned/op")
	b.ReportMetric(float64(diag.PartsDecoded), "parts_decoded/op")
	b.ReportMetric(float64(diag.BlocksConsidered), "blocks_considered/op")
	b.ReportMetric(float64(diag.BlocksPruned), "blocks_pruned/op")
	b.ReportMetric(float64(diag.BlocksDecoded), "blocks_decoded/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.DecodedHeapCopyBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.RowLocatorDecodes), "row_locator_decodes/op")
	b.ReportMetric(float64(diag.PhysicalRowIDLookups), "physical_row_id_lookups/op")
	b.ReportMetric(float64(diag.PhysicalRowAssetReads), "physical_row_asset_reads/op")
	b.ReportMetric(float64(diag.RowMaterializations), "row_materializations/op")
	b.ReportMetric(float64(diag.DocumentMaterializations), "document_materializations/op")
	b.ReportMetric(float64(diag.DocumentReconstructions), "document_reconstructions/op")
}

func reportTypedColumnInt64ScanBenchMetrics(b *testing.B, diag TypedColumnInt64PredicateScanDiagnostics, elapsed time.Duration, iterations int) {
	b.Helper()
	if elapsed > 0 && iterations > 0 {
		b.ReportMetric(float64(iterations)/elapsed.Seconds(), "ops/sec")
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

func insertTypedColumnInt64AggregateBenchRowsBatch(tb testing.TB, col *Collection, globalStart int, values []int64) {
	tb.Helper()
	ids := make([][]byte, len(values))
	docs := make([][]byte, len(values))
	for i, value := range values {
		row := globalStart + i
		ids[i] = []byte(fmt.Sprintf("bench_e%012d_%d", row, value))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"k%d"}`, value, value%8))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
}

func typedColumnInt64ScanIdentityStrings(result TypedColumnInt64PredicateScanResult) []string {
	out := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		out[i] = fmt.Sprintf("%s/%d/%d/%d/%d", string(row.DocumentID), row.Generation, row.PartID, row.RowIndex, row.Value)
	}
	sort.Strings(out)
	return out
}

func assertTypedColumnInt64Aggregate(t *testing.T, result TypedColumnInt64PredicateAggregateResult, wantCount int64, wantSum int64) {
	t.Helper()
	if result.Count != wantCount || result.Sum != wantSum {
		t.Fatalf("aggregate count=%d sum=%d want count=%d sum=%d diagnostics=%+v", result.Count, result.Sum, wantCount, wantSum, result.Diagnostics)
	}
	if wantCount == 0 {
		if result.Avg != 0 {
			t.Fatalf("aggregate avg=%f want zero diagnostics=%+v", result.Avg, result.Diagnostics)
		}
		return
	}
	wantAvg := float64(wantSum) / float64(wantCount)
	if result.Avg != wantAvg {
		t.Fatalf("aggregate avg=%f want %f diagnostics=%+v", result.Avg, wantAvg, result.Diagnostics)
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
