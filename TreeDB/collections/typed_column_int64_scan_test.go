package collections

import (
	"errors"
	"fmt"
	"os"
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

func TestTypedColumnInt64AggregateBenchRowCountParser(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		want    []int
		wantErr bool
	}{
		{name: "empty_defaults", input: "", want: []int{4096, 65536}},
		{name: "list", input: "16, 32,64", want: []int{16, 32, 64}},
		{name: "bad", input: "16,nope", wantErr: true},
		{name: "non_positive", input: "0", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTypedColumnInt64AggregateBenchRows(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseTypedColumnInt64AggregateBenchRows(%q) err=nil", tc.input)
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
}

func BenchmarkTypedColumnInt64PredicateAggregate(b *testing.B) {
	rowCounts, err := parseTypedColumnInt64AggregateBenchRows(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_ROWS"))
	if err != nil {
		b.Fatalf("TREEDB_TYPED_COLUMN_BENCH_ROWS: %v", err)
	}
	for _, rows := range rowCounts {
		rows := rows
		values := make([]int64, rows)
		for i := range values {
			values[i] = int64(i)
		}
		low := rows / 4
		matchCount := maxIntForTypedColumnInt64AggregateBench(1, rows/100)
		high := low + matchCount - 1
		if high >= rows {
			high = rows - 1
			matchCount = high - low + 1
		}
		wantCount := int64(matchCount)
		wantSum := int64(low+high) * wantCount / 2
		b.Run(fmt.Sprintf("rows_%d/typed_column_part/predicate_count_sum_avg", rows), func(b *testing.B) {
			d, col := setupTypedColumnInt64ScanCollection(b)
			defer func() { _ = d.Close() }()
			mid := rows / 2
			insertTypedColumnInt64ScanRows(b, col, values[:mid])
			insertTypedColumnInt64ScanRows(b, col, values[mid:])
			b.ReportAllocs()
			b.ResetTimer()
			benchStart := time.Now()
			for i := 0; i < b.N; i++ {
				result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(low), High: int64(high), ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
				if err != nil {
					b.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
				}
				if result.Count != wantCount || result.Sum != wantSum {
					b.Fatalf("aggregate count=%d sum=%d want count=%d sum=%d diagnostics=%+v", result.Count, result.Sum, wantCount, wantSum, result.Diagnostics)
				}
				if i == b.N-1 {
					reportTypedColumnInt64AggregateBenchMetrics(b, rows, result.Diagnostics, time.Since(benchStart), b.N)
				}
			}
			b.StopTimer()
		})
		b.Run(fmt.Sprintf("rows_%d/document_full_scan_fallback/predicate_count_sum_avg", rows), func(b *testing.B) {
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
			mid := rows / 2
			insertTypedColumnInt64ScanRows(b, col, values[:mid])
			insertTypedColumnInt64ScanRows(b, col, values[mid:])
			b.ReportAllocs()
			b.ResetTimer()
			benchStart := time.Now()
			for i := 0; i < b.N; i++ {
				result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(low), High: int64(high)})
				if err != nil {
					b.Fatalf("RunTypedColumnInt64PredicateAggregate fallback: %v", err)
				}
				if result.Count != wantCount || result.Sum != wantSum {
					b.Fatalf("aggregate count=%d sum=%d want count=%d sum=%d diagnostics=%+v", result.Count, result.Sum, wantCount, wantSum, result.Diagnostics)
				}
				if i == b.N-1 {
					reportTypedColumnInt64AggregateBenchMetrics(b, rows, result.Diagnostics, time.Since(benchStart), b.N)
				}
			}
			b.StopTimer()
		})
	}
}

func parseTypedColumnInt64AggregateBenchRows(env string) ([]int, error) {
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
		out = append(out, rows)
	}
	return out, nil
}

func maxIntForTypedColumnInt64AggregateBench(a, b int) int {
	if a > b {
		return a
	}
	return b
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
