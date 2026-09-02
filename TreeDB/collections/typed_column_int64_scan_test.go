package collections

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

func typedColumnInt64DirectViewSupportedForTest() bool {
	if !typedColumnInt64HostLittleEndianForTest() {
		return false
	}
	switch runtime.GOOS {
	case "darwin", "linux", "freebsd", "netbsd", "openbsd":
		return true
	default:
		return false
	}
}

func typedColumnInt64HostLittleEndianForTest() bool {
	var value uint16 = 1
	return *(*byte)(unsafe.Pointer(&value)) == 1
}

func TestTypedColumnInt64PreparedForegroundLifetimeIdleAndRun(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30})

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

	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	if session.view.snapshot != nil || active != 0 || begins != ends {
		t.Fatalf("prepared int64 snapshot=%p foreground begin/end/active=%d/%d/%d want nil balanced idle", session.view.snapshot, begins, ends, active)
	}
	before := begins
	if _, err := session.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if active != 0 || begins != ends || begins != before+1 {
		t.Fatalf("foreground begin/end/active after int64 Run=%d/%d/%d want one balanced operation", begins, ends, active)
	}
}

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
	insertTypedColumnInt64ScanRows(t, col, []int64{0})
	if session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}); err == nil {
		_ = session.Close()
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate nullable err=nil want fail-closed nullable typed-column int64")
	} else if !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate nullable err=%v want unsupported nullable typed-column int64", err)
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

func TestTypedColumnInt64ScanMultipartLatestVisibleUpdatesDeletes(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	col := createTypedColumnInt64ScanCollection(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2"), []byte("e3")}, [][]byte{
		[]byte(`{"time_us":10,"kind":"k1"}`),
		[]byte(`{"time_us":20,"kind":"k2"}`),
		[]byte(`{"time_us":30,"kind":"k3"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":25,"kind":"k1b"}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update e1 changed=%v err=%v", changed, err)
	}
	if _, changed, err := col.Update([]byte("e2"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":40,"kind":"k2b"}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update e2 changed=%v err=%v", changed, err)
	}
	if deleted, err := col.DeleteDocument([]byte("e3")); err != nil || !deleted {
		t.Fatalf("DeleteDocument e3 deleted=%v err=%v", deleted, err)
	}

	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 1, High: 100})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{25, 40})
	if result.Diagnostics.Fallback || result.Diagnostics.MutationParts == 0 || result.Diagnostics.PhysicalRowAssetReads == 0 {
		t.Fatalf("diagnostics=%+v want direct multipart latest-visible path", result.Diagnostics)
	}
	gotIDs := map[string]int64{}
	for _, row := range result.Rows {
		gotIDs[string(row.DocumentID)] = row.Value
		if row.PartID != columnPhysicalRowAssetPartID {
			t.Fatalf("row=%+v want physical row asset identity", row)
		}
	}
	if gotIDs["e1"] != 25 || gotIDs["e2"] != 40 || gotIDs["e3"] != 0 || len(gotIDs) != 2 {
		t.Fatalf("document IDs to values=%v want e1=25 e2=40 only", gotIDs)
	}

	agg, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 1, High: 100})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, agg, 2, 65)
	if agg.Diagnostics.Fallback || agg.Diagnostics.MutationParts == 0 || agg.Diagnostics.PhysicalRowAssetReads == 0 {
		t.Fatalf("aggregate diagnostics=%+v want direct multipart latest-visible path", agg.Diagnostics)
	}

	roles := typedColumnManifestPartRolesByGenerationForTest(t, d, col)
	if roles[1] != ColumnManifestPartRoleBase || roles[2] != ColumnManifestPartRoleDelta || roles[3] != ColumnManifestPartRoleDelta || roles[4] != ColumnManifestPartRoleTombstone {
		t.Fatalf("manifest roles=%v want base/delta/delta/tombstone", roles)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = openCollectionCommandWALDB(t, dir)
	col, err = NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopened, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 1, High: 100})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan reopened: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, reopened, []int64{25, 40})
	if reopened.Diagnostics.Fallback || reopened.Diagnostics.MutationParts == 0 || reopened.Diagnostics.PhysicalRowAssetReads == 0 {
		t.Fatalf("reopened diagnostics=%+v want direct multipart latest-visible path", reopened.Diagnostics)
	}
}

func TestTypedColumnInt64ScanIgnoresAuxiliaryMultipartTypedRefs(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{11, 12, 13})
	extraRef := publishTypedColumnMultipartPartRef1787(t, d, col, 3)
	if extraRef.PartID != 3 {
		t.Fatalf("extra ref=%+v want part_id=3", extraRef)
	}
	result, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 12})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, result, []int64{12})
	if result.Diagnostics.Fallback {
		t.Fatalf("diagnostics=%+v want direct typed-column scan", result.Diagnostics)
	}
	agg, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 11, High: 13})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, agg, 3, 36)
	if agg.Diagnostics.Fallback {
		t.Fatalf("aggregate diagnostics=%+v want direct typed-column aggregate", agg.Diagnostics)
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

func TestTypedColumnInt64AggregateSecondOfDaySquareExpressionFloorsNegativeMicros(t *testing.T) {
	tests := []struct {
		value int64
		want  int64
	}{
		{value: -1, want: 86_399 * 86_399},
		{value: -999_999, want: 86_399 * 86_399},
		{value: -1_000_000, want: 86_399 * 86_399},
		{value: -1_000_001, want: 86_398 * 86_398},
		{value: 0, want: 0},
		{value: 1_000_000, want: 1},
	}
	for _, tt := range tests {
		got, err := typedColumnInt64AggregateExpressionValue(TypedColumnInt64AggregateSecondOfDaySquare, tt.value)
		if err != nil {
			t.Fatalf("typedColumnInt64AggregateExpressionValue(%d): %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("typedColumnInt64AggregateExpressionValue(%d)=%d want %d", tt.value, got, tt.want)
		}
	}
}

func TestTypedColumnInt64AggregateSecondOfDaySquareExpression(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{-1, -1_000_001, 1_000_000})

	req := TypedColumnInt64PredicateAggregateRequest{
		Column:     "time_us",
		Kind:       TypedColumnInt64PredicateAll,
		Expression: TypedColumnInt64AggregateSecondOfDaySquare,
	}
	result, err := col.RunTypedColumnInt64PredicateAggregate(req)
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate expression: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 3, 86_399*86_399+86_398*86_398+1)
	if result.Diagnostics.RowsScanned != 3 || result.Diagnostics.RowsMatched != 3 || result.Diagnostics.StatsBlocks != 0 {
		t.Fatalf("diagnostics=%+v want expression to scan typed-column rows without aggregate stats", result.Diagnostics)
	}
	assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, result.Diagnostics, "second-of-day-square expression aggregate")
}

func TestTypedColumnInt64AggregateSecondOfDaySquareExpressionRange(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{-1, 0, 1_000_000, 2_000_000})

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{
		Column:     "time_us",
		Kind:       TypedColumnInt64PredicateRange,
		Low:        0,
		High:       2_000_000,
		Expression: TypedColumnInt64AggregateSecondOfDaySquare,
	})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate range expression: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 3, 0+1+4)
	if result.Diagnostics.RowsScanned != 4 || result.Diagnostics.RowsMatched != 3 || result.Diagnostics.StatsBlocks != 0 {
		t.Fatalf("diagnostics=%+v want range expression to scan typed-column rows without aggregate stats", result.Diagnostics)
	}
	assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, result.Diagnostics, "second-of-day-square range expression aggregate")
}

func TestTypedColumnInt64PreparedAggregateSecondOfDaySquareExpression(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{-1, -1_000_001, 1_000_000})

	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{
		Column:                   "time_us",
		Kind:                     TypedColumnInt64PredicateAll,
		Expression:               TypedColumnInt64AggregateSecondOfDaySquare,
		ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify,
	})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate expression: %v", err)
	}
	defer func() { _ = session.Close() }()

	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run expression: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 3, 86_399*86_399+86_398*86_398+1)
	if result.Diagnostics.RowsScanned != 3 || result.Diagnostics.RowsMatched != 3 || result.Diagnostics.StatsBlocks != 0 || result.Diagnostics.KernelBlocks != 0 {
		t.Fatalf("diagnostics=%+v want expression hot run to scan typed-column rows without stats/kernel sum", result.Diagnostics)
	}
	assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, result.Diagnostics, "prepared second-of-day-square expression aggregate")
}

func TestTypedColumnInt64AggregateRejectsUnsupportedExpression(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1_000_000})

	_, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{
		Column:     "time_us",
		Kind:       TypedColumnInt64PredicateAll,
		Expression: TypedColumnInt64AggregateExpression("bogus"),
	})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate err=%v want ErrColumnQueryPlanUnsupported", err)
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
	if result.Diagnostics.SelectionEmptyBlocks == 0 || result.Diagnostics.SelectionAllBlocks != 0 || result.Diagnostics.SelectionSparseBlocks != 0 {
		t.Fatalf("diagnostics=%+v want shared empty selection diagnostics for pruned blocks", result.Diagnostics)
	}
}

func TestTypedColumnInt64AggregateSelectionShapes(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})

	all, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate all: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, all, 4, 80)
	if all.Diagnostics.SelectionAllBlocks == 0 || all.Diagnostics.SelectionSparseBlocks != 0 || all.Diagnostics.SelectionEmptyBlocks != 0 {
		t.Fatalf("all diagnostics=%+v want all-row selection shape", all.Diagnostics)
	}

	exact, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate exact: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, exact, 2, 40)
	if exact.Diagnostics.SelectionSparseBlocks+exact.Diagnostics.SelectionRangeBlocks == 0 {
		t.Fatalf("exact diagnostics=%+v want sparse/range predicate selection shape", exact.Diagnostics)
	}
	assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, exact.Diagnostics, "selection-shaped typed-column aggregate")
}

func TestTypedColumnInt64PreparedUsesDurablePruningMetadata(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	if session.prepareDiagnostics.DecodedMetadataBytes == 0 || session.prepareDiagnostics.PruningCertified == 0 {
		t.Fatalf("prepare diagnostics=%+v want pruning metadata decoded/certified", session.prepareDiagnostics)
	}
	if session.prepareDiagnostics.PruningBlocks != 1 || session.prepareDiagnostics.PruningRows != 2 {
		t.Fatalf("prepare diagnostics=%+v want one pruned candidate block with two rows", session.prepareDiagnostics)
	}
	var preparedColumn *typedColumnPreparedColumnState
	for _, part := range session.preparedState.partsByRef {
		preparedColumn = part.Columns[session.aggregateColumn.Definition.Name]
		break
	}
	if preparedColumn == nil {
		t.Fatalf("prepared pruning column missing")
	}
	if !preparedColumn.Int64PruningReady || preparedColumn.PruningFallbackReason != "" {
		t.Fatalf("prepared pruning state=%+v fallback=%q", preparedColumn, preparedColumn.PruningFallbackReason)
	}
	matched := 0
	for _, block := range preparedColumn.BlockPlans {
		if block.CandidateSelection.IsEmpty() {
			continue
		}
		if !block.NeedsPredicate {
			t.Fatalf("block %+v must keep predicate verification after pruning metadata narrows rows", block)
		}
		matched += block.CandidateSelection.Count()
	}
	if matched != 2 {
		t.Fatalf("pruning candidate rows=%d want 2", matched)
	}
	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 2, 40)
	if result.Diagnostics.PruningBlocks != 1 || result.Diagnostics.PruningRows != 2 {
		t.Fatalf("run diagnostics=%+v want one pruned candidate block with two rows", result.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedAllPredicateSkipsPruningMetadata(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate all: %v", err)
	}
	defer func() { _ = session.Close() }()
	var preparedColumn *typedColumnPreparedColumnState
	for _, part := range session.preparedState.partsByRef {
		preparedColumn = part.Columns[session.aggregateColumn.Definition.Name]
		break
	}
	if preparedColumn == nil {
		t.Fatalf("prepared pruning column missing")
	}
	if preparedColumn.Int64PruningReady || preparedColumn.PruningFallbackReason != "" {
		t.Fatalf("prepared all pruning state=%+v fallback=%q want pruning skipped", preparedColumn, preparedColumn.PruningFallbackReason)
	}
	if session.prepareDiagnostics.PruningBlocks != 0 || session.prepareDiagnostics.PruningRows != 0 || session.prepareDiagnostics.PruningFallbackBlocks != 0 {
		t.Fatalf("prepare diagnostics=%+v want no all-predicate pruning work", session.prepareDiagnostics)
	}
}

func TestTypedColumnInt64RawLayoutRoundTripReopenQuery(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnInt64ScanCollectionWithFixedWidthEncoding(t, d, ColumnFixedWidthEncodingLittleEndian)
	insertTypedColumnInt64ScanRows(t, col, []int64{11, 12, 13, 20})
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed-column refs=%+v want one", refs)
	}
	if refs[0].Offset != 0 || refs[0].FileID == columnAssetM12ASegmentFileID {
		t.Fatalf("raw direct-view typed-column ref=%+v want deterministic direct-view segment at offset 0", refs[0])
	}
	reachability, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil || !reachability.Complete || reachability.Segments.Unknown != 0 || reachability.Segments.Missing != 0 {
		t.Fatalf("raw direct-view reachability plan=%+v err=%v want complete with no unknown/missing segments", reachability, err)
	}
	if got := typedColumnInt64ColumnEncodingForTest(t, d, refs[0], "time_us"); got != typedcolumn.EncodingRawInt64 {
		t.Fatalf("time_us encoding=%s want %s", got, typedcolumn.EncodingRawInt64)
	}
	scan, err := col.RunTypedColumnInt64PredicateScan(TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 12, High: 20})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateScan raw: %v", err)
	}
	assertTypedColumnInt64ScanValues(t, scan, []int64{12, 13, 20})
	agg, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate raw: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, agg, 4, 56)
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 12, High: 20, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate raw: %v", err)
	}
	prepared, err := session.Run()
	if err != nil {
		t.Fatalf("prepared raw Run: %v", err)
	}
	if diag := session.Diagnostics(); diag.ActiveResourceHandles == 0 || diag.ActiveMappedBytes+diag.ActiveHeapCopyBytes == 0 {
		t.Fatalf("session diagnostics before close=%+v want pinned direct-view resources", diag)
	}
	if closeErr := session.Close(); closeErr != nil {
		t.Fatalf("Close prepared raw: %v", closeErr)
	}
	if _, err := session.Run(); !errors.Is(err, errTypedColumnInt64PredicateAggregateSessionClosed) {
		t.Fatalf("Run after Close err=%v want session closed before exposing stale direct view", err)
	}
	assertTypedColumnInt64Aggregate(t, prepared, 3, 45)
	if prepared.Diagnostics.DecodedHeapCopyBytes != 0 || prepared.Diagnostics.FastDecodeDirectViewPlans == 0 {
		t.Fatalf("prepared raw diagnostics=%+v want fast raw reducer without decoded heap copy", prepared.Diagnostics)
	}
	if typedColumnInt64DirectViewSupportedForTest() {
		if prepared.Diagnostics.DirectViewSuccesses == 0 || prepared.Diagnostics.FastDecodeMmapDirectViews == 0 || prepared.Diagnostics.FastDecodeHeapCopyTypedViews != 0 || prepared.Diagnostics.FastDecodeStreamingFallbacks != 0 || prepared.Diagnostics.HeapCopyBytes != 0 {
			t.Fatalf("prepared raw diagnostics=%+v want mmap direct-view reducer with source counters", prepared.Diagnostics)
		}
	} else if prepared.Diagnostics.FastDecodeHeapCopyTypedViews > 0 {
		if prepared.Diagnostics.DirectViewSuccesses == 0 || prepared.Diagnostics.FastDecodeScratchDecodes != 0 || prepared.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonHandleSourceUnsupported) {
			t.Fatalf("prepared raw diagnostics=%+v want platform heap typed-view fallback without scratch decode", prepared.Diagnostics)
		}
	} else if prepared.Diagnostics.DirectViewFailures == 0 || prepared.Diagnostics.FastDecodeStreamingFallbacks == 0 || prepared.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonHandleSourceUnsupported) {
		t.Fatalf("prepared raw diagnostics=%+v want platform streaming fallback without materialization", prepared.Diagnostics)
	}
	if session.prepareDiagnostics.DirectViewCertified == 0 || session.prepareDiagnostics.CertificationFailures != 0 {
		t.Fatalf("prepare diagnostics=%+v want certified direct-view layout with no failures", session.prepareDiagnostics)
	}
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
	reopenedAgg, err := reopenedCol.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 12, High: 13, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate reopened raw: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, reopenedAgg, 2, 25)
	if reopenedAgg.Diagnostics.DirectViewCertified == 0 || reopenedAgg.Diagnostics.DecodedHeapCopyBytes != 0 {
		t.Fatalf("reopened diagnostics=%+v want certified hot raw plan without heap decode", reopenedAgg.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedCertificationDiagnosticsAndClose(t *testing.T) {
	d, col := setupTypedColumnInt64RawScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3, 4, 5})
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 2, High: 4, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	if session.prepareDiagnostics.DirectViewCertified == 0 || session.prepareDiagnostics.CertificationFailures != 0 {
		t.Fatalf("prepare diagnostics=%+v want direct-view certification without failures", session.prepareDiagnostics)
	}
	first, err := session.Run()
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	second, err := session.Run()
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, first, 3, 9)
	assertTypedColumnInt64Aggregate(t, second, 3, 9)
	if first.Diagnostics.DecodedMetadataBytes != 0 || second.Diagnostics.DecodedMetadataBytes != 0 || first.Diagnostics.DecodedHeapCopyBytes != 0 || second.Diagnostics.DecodedHeapCopyBytes != 0 {
		t.Fatalf("hot diagnostics first=%+v second=%+v want no per-run metadata or heap decode", first.Diagnostics, second.Diagnostics)
	}
	if typedColumnInt64DirectViewSupportedForTest() {
		if first.Diagnostics.DirectViewSuccesses == 0 || second.Diagnostics.DirectViewSuccesses == 0 || first.Diagnostics.FastDecodeMmapDirectViews == 0 || second.Diagnostics.FastDecodeMmapDirectViews == 0 || first.Diagnostics.FastDecodeStreamingFallbacks != 0 || second.Diagnostics.FastDecodeStreamingFallbacks != 0 || first.Diagnostics.HeapCopyBytes != 0 || second.Diagnostics.HeapCopyBytes != 0 {
			t.Fatalf("hot diagnostics first=%+v second=%+v want mmap direct-view runs with source counters", first.Diagnostics, second.Diagnostics)
		}
	} else if first.Diagnostics.FastDecodeHeapCopyTypedViews > 0 || second.Diagnostics.FastDecodeHeapCopyTypedViews > 0 {
		if first.Diagnostics.DirectViewSuccesses == 0 || second.Diagnostics.DirectViewSuccesses == 0 || first.Diagnostics.FastDecodeScratchDecodes != 0 || second.Diagnostics.FastDecodeScratchDecodes != 0 || first.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonHandleSourceUnsupported) || second.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonHandleSourceUnsupported) {
			t.Fatalf("hot diagnostics first=%+v second=%+v want platform heap typed-view fallback without scratch decode", first.Diagnostics, second.Diagnostics)
		}
	} else if first.Diagnostics.DirectViewFailures == 0 || second.Diagnostics.DirectViewFailures == 0 || first.Diagnostics.FastDecodeStreamingFallbacks == 0 || second.Diagnostics.FastDecodeStreamingFallbacks == 0 || first.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonHandleSourceUnsupported) || second.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonHandleSourceUnsupported) {
		t.Fatalf("hot diagnostics first=%+v second=%+v want platform streaming fallback without materialization", first.Diagnostics, second.Diagnostics)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if diag := session.Diagnostics(); !diag.Closed || diag.ActiveResourceHandles != 0 || diag.ActiveMappedBytes != 0 || diag.ActiveHeapCopyBytes != 0 {
		t.Fatalf("session diagnostics after close=%+v want resources released", diag)
	}
}

func TestTypedColumnInt64PreparedHeapCopyTypedViewCounters(t *testing.T) {
	d, col := setupTypedColumnInt64RawScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	if err := session.readCache.close(); err != nil {
		t.Fatalf("close prepared read cache before forced heap run: %v", err)
	}
	session.readCache.forceReadAtFallback = true
	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run forced heap-copy typed view: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 1, 2)
	if result.Diagnostics.FastDecodeHeapCopyTypedViews == 0 || result.Diagnostics.FastDecodeMmapDirectViews != 0 || result.Diagnostics.FastDecodeScratchDecodes != 0 || result.Diagnostics.FastDecodeStreamingFallbacks != 0 || result.Diagnostics.HeapCopyBytes == 0 {
		t.Fatalf("diagnostics=%+v want heap-copy typed view without mmap or scratch decode fallback", result.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedAbsoluteOffsetUnalignedFallsBack(t *testing.T) {
	d, col := setupTypedColumnInt64RawScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	insertTypedColumnInt64ScanRows(t, col, []int64{4, 5, 6})

	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if err != nil {
		if closeView != nil {
			closeView()
		}
		t.Fatalf("prepareColumnPhysicalScanSnapshotViewWithSidecars: %v", err)
	}
	if len(view.TypedColumnPartRefs) < 2 {
		if closeView != nil {
			closeView()
		}
		t.Fatalf("typed refs=%+v want multi-asset typed-column parts", view.TypedColumnPartRefs)
	}
	unalignedRef := appendUnalignedTypedColumnAssetCopyForTest(t, d.ColumnAssetRootDir(), view.TypedColumnPartRefs[0].Ref)
	for i := range view.TypedColumnPartRefs {
		if view.TypedColumnPartRefs[i].Ref.Generation == unalignedRef.Generation {
			view.TypedColumnPartRefs[i].Ref = unalignedRef
		}
	}

	req := TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
	session, _, err := col.prepareTypedColumnInt64PredicateAggregateSessionFromView(view, closeView, req)
	if err != nil {
		if closeView != nil {
			closeView()
		}
		t.Fatalf("prepareTypedColumnInt64PredicateAggregateSessionFromView: %v", err)
	}
	defer func() { _ = session.Close() }()
	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run with unaligned absolute offset: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 1, 2)
	if result.Diagnostics.FastDecodeAbsoluteUnaligned == 0 || result.Diagnostics.DirectViewFailures == 0 || result.Diagnostics.FastDecodeScratchDecodes == 0 || result.Diagnostics.FastDecodeStreamingFallbacks == 0 || result.Diagnostics.FastDecodeFallbackReason != string(typeddecode.ReasonAbsoluteOffsetUnaligned) {
		t.Fatalf("diagnostics=%+v want absolute-offset unaligned direct-view fallback counters", result.Diagnostics)
	}
}

func appendUnalignedTypedColumnAssetCopyForTest(t *testing.T, rootDir string, ref ColumnAssetRef) ColumnAssetRef {
	t.Helper()
	raw, err := readColumnPhysicalAssetFromManager(rootDir, ref)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open segment append: %v", err)
	}
	defer func() { _ = file.Close() }()
	offset, err := file.Seek(0, 2)
	if err != nil {
		t.Fatalf("seek segment end: %v", err)
	}
	if offset%8 == 0 {
		if _, err := file.Write([]byte{0}); err != nil {
			t.Fatalf("write unalignment padding: %v", err)
		}
		offset++
	}
	if offset%8 == 0 {
		t.Fatalf("failed to choose unaligned offset=%d", offset)
	}
	if _, err := file.Write(raw); err != nil {
		t.Fatalf("write duplicate typed-column asset: %v", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("sync duplicate typed-column asset: %v", err)
	}
	dup := ref
	dup.Offset = offset
	dup.Length = int64(len(raw))
	return dup
}

func TestTypedColumnInt64PreparedStatsRoundTripReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnInt64ScanCollection(t, d)
	insertTypedColumnInt64ScanRows(t, col, []int64{11, 12, 13, 14})
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
	session, err := reopenedCol.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare all stats: %v", err)
	}
	all, err := session.Run()
	if closeErr := session.Close(); closeErr != nil {
		t.Fatalf("Close all stats: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run all stats: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, all, 4, 50)
	if all.Diagnostics.StatsBlocks == 0 || all.Diagnostics.BlocksDecoded != 0 || all.Diagnostics.RowsScanned != 0 || all.Diagnostics.RangeBytesRead != 0 || all.Diagnostics.KernelBlocks != 0 {
		t.Fatalf("all diagnostics=%+v want durable stats with no payload decode", all.Diagnostics)
	}

	rangeSession, err := reopenedCol.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 10, High: 20, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare full-covered range stats: %v", err)
	}
	covered, err := rangeSession.Run()
	if closeErr := rangeSession.Close(); closeErr != nil {
		t.Fatalf("Close range stats: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run full-covered range stats: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, covered, 4, 50)
	if covered.Diagnostics.StatsBlocks == 0 || covered.Diagnostics.BlocksDecoded != 0 || covered.Diagnostics.StatsFallbackBlocks != 0 {
		t.Fatalf("range diagnostics=%+v want full-covered durable stats", covered.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedStatsCorruptionIntegrityPolicy(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed-column refs=%+v want one", refs)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatalf("read typed-column asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	statsSection, ok, err := image.ColumnStatsSection()
	if err != nil || !ok {
		t.Fatalf("ColumnStatsSection ok=%v err=%v", ok, err)
	}
	corruptAt := int64(statsSection.Offset + statsSection.Length - 1)
	var corrupt [1]byte
	corrupt[0] = raw[corruptAt] ^ 0xff
	writeColumnAssetBytesAtRelativeOffset(t, d, refs[0], corruptAt, corrupt[:])
	_, err = col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Prepare corrupt stats err=%v want fail-closed checksum validation", err)
	}

	skipSession, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums})
	if err != nil {
		t.Fatalf("Prepare corrupt stats with skip-checksums: %v", err)
	}
	skipResult, err := skipSession.Run()
	if closeErr := skipSession.Close(); closeErr != nil {
		t.Fatalf("Close skip-checksums session: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run corrupt stats with skip-checksums: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, skipResult, 3, 6)
	if skipResult.Diagnostics.StatsBlocks != 0 || skipResult.Diagnostics.BlocksDecoded == 0 {
		t.Fatalf("skip-checksums diagnostics=%+v want stats ignored and decode fallback", skipResult.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedDeltaUsesStreamingCursor(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 3, 6, 10, 15, 21})
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 3, High: 15, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	if session.prepareDiagnostics.StreamingCertified == 0 {
		t.Fatalf("prepare diagnostics=%+v want streaming-certified delta layout", session.prepareDiagnostics)
	}
	result, err := session.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 4, 34)
	if result.Diagnostics.FastDecodeStreamingPlans == 0 || result.Diagnostics.FastDecodeDirectViewPlans != 0 || result.Diagnostics.DirectViewSuccesses != 0 || result.Diagnostics.FastDecodeStreamingFallbacks != 0 || result.Diagnostics.DecodedHeapCopyBytes != 0 {
		t.Fatalf("diagnostics=%+v want planned delta-varint streaming cursor without direct view/fallback or []int64 materialization", result.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedKernelOverflowAndNegativeValues(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{-10, -20, 5})
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare negative values: %v", err)
	}
	negative, err := session.Run()
	if closeErr := session.Close(); closeErr != nil {
		t.Fatalf("Close negative session: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run negative values: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, negative, 3, -25)
	if negative.Diagnostics.StatsBlocks == 0 || negative.Diagnostics.BlocksDecoded != 0 || negative.Diagnostics.KernelBlocks != 0 {
		t.Fatalf("negative diagnostics=%+v want prepared durable stats path", negative.Diagnostics)
	}

	overflowDB, overflowCol := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = overflowDB.Close() }()
	insertTypedColumnInt64ScanRows(t, overflowCol, []int64{typedColumnInt64PredicateAggregateMaxSum, 1})
	overflowSession, err := overflowCol.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare overflow values: %v", err)
	}
	_, err = overflowSession.Run()
	if closeErr := overflowSession.Close(); closeErr != nil {
		t.Fatalf("Close overflow session: %v", closeErr)
	}
	if err == nil || !strings.Contains(err.Error(), "sum overflow") {
		t.Fatalf("Run overflow err=%v want sum overflow through prepared kernel path", err)
	}
}

func TestTypedColumnInt64PreparedDeltaKernelDiagnosticsFullPartialPruned(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3, 4, 5, 6})

	allSession, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare all: %v", err)
	}
	all, err := allSession.Run()
	if closeErr := allSession.Close(); closeErr != nil {
		t.Fatalf("Close all: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run all: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, all, 6, 21)
	if all.Diagnostics.StatsBlocks == 0 || all.Diagnostics.StatsFullCoveredBlocks == 0 || all.Diagnostics.BlocksDecoded != 0 || all.Diagnostics.KernelBlocks != 0 || all.Diagnostics.KernelFallbackBlocks != 0 {
		t.Fatalf("all diagnostics=%+v want full-covered durable stats fast path", all.Diagnostics)
	}

	partialSession, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 3, High: 4, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare partial: %v", err)
	}
	partial, err := partialSession.Run()
	if closeErr := partialSession.Close(); closeErr != nil {
		t.Fatalf("Close partial: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run partial: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, partial, 2, 7)
	if partial.Diagnostics.StatsBlocks != 0 || partial.Diagnostics.StatsFallbackBlocks == 0 || partial.Diagnostics.KernelFallbackBlocks == 0 || partial.Diagnostics.KernelCursorBlocks != 0 {
		t.Fatalf("partial diagnostics=%+v want stats fallback plus explicit partial streaming fallback classification", partial.Diagnostics)
	}

	prunedSession, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 99, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("Prepare pruned: %v", err)
	}
	pruned, err := prunedSession.Run()
	if closeErr := prunedSession.Close(); closeErr != nil {
		t.Fatalf("Close pruned: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run pruned: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, pruned, 0, 0)
	if pruned.Diagnostics.BlocksPruned == 0 || pruned.Diagnostics.RangeBytesRead != 0 || pruned.Diagnostics.KernelBlocks != 0 || pruned.Diagnostics.KernelFallbackBlocks != 0 {
		t.Fatalf("pruned diagnostics=%+v want payload-free prune without kernel/fallback blocks", pruned.Diagnostics)
	}
}

func TestTypedColumnInt64PreparedRawKernelSelectionShapes(t *testing.T) {
	if !typedColumnInt64DirectViewSupportedForTest() {
		t.Skip("raw selected-value kernel path requires direct-view support")
	}
	tests := []struct {
		name      string
		values    []int64
		req       TypedColumnInt64PredicateAggregateRequest
		wantCount int64
		wantSum   int64
		wantShape string
	}{
		{name: "all", values: []int64{1, 2, 3, 4}, req: TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll}, wantCount: 4, wantSum: 10, wantShape: "all"},
		{name: "range", values: []int64{0, 1, 2, 3, 4, 5}, req: TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 2, High: 4}, wantCount: 3, wantSum: 9, wantShape: "range"},
		{name: "sparse_parity", values: repeatedInt64SelectionValuesForTest(40, func(i int) bool { return i%2 == 0 }, 7), req: TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 7}, wantCount: 20, wantSum: 140},
		{name: "bitmap_parity", values: repeatedInt64SelectionValuesForTest(128, func(i int) bool { return i%4 != 0 }, 7), req: TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 7}, wantCount: 96, wantSum: 672},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, col := setupTypedColumnInt64RawScanCollection(t)
			defer func() { _ = d.Close() }()
			insertTypedColumnInt64ScanRows(t, col, tc.values)
			direct, err := col.RunTypedColumnInt64PredicateAggregate(tc.req)
			if err != nil {
				t.Fatalf("direct aggregate: %v", err)
			}
			assertTypedColumnInt64Aggregate(t, direct, tc.wantCount, tc.wantSum)
			req := tc.req
			req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityCachedVerify
			session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
			if err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			prepared, err := session.Run()
			if closeErr := session.Close(); closeErr != nil {
				t.Fatalf("Close: %v", closeErr)
			}
			if err != nil {
				t.Fatalf("prepared Run: %v", err)
			}
			assertTypedColumnInt64Aggregate(t, prepared, direct.Count, direct.Sum)
			if tc.wantShape == "all" {
				if prepared.Diagnostics.StatsBlocks == 0 || prepared.Diagnostics.BlocksDecoded != 0 || prepared.Diagnostics.KernelBlocks != 0 {
					t.Fatalf("prepared diagnostics=%+v want raw all-selection durable stats reducer", prepared.Diagnostics)
				}
			} else if prepared.Diagnostics.KernelBlocks == 0 || prepared.Diagnostics.KernelSelectedBlocks+prepared.Diagnostics.KernelFullCoveredBlocks == 0 || prepared.Diagnostics.KernelFallbackBlocks != 0 {
				t.Fatalf("prepared diagnostics=%+v want raw direct-view typedkernel reducer", prepared.Diagnostics)
			}
			switch tc.wantShape {
			case "all":
				if prepared.Diagnostics.SelectionAllBlocks == 0 {
					t.Fatalf("diagnostics=%+v want all selection", prepared.Diagnostics)
				}
			case "range":
				if prepared.Diagnostics.SelectionRangeBlocks == 0 {
					t.Fatalf("diagnostics=%+v want range selection", prepared.Diagnostics)
				}
			}
		})
	}
}

func repeatedInt64SelectionValuesForTest(rows int, selected func(int) bool, selectedValue int64) []int64 {
	values := make([]int64, rows)
	for i := range values {
		if selected(i) {
			values[i] = selectedValue
		} else {
			values[i] = selectedValue + 1
		}
	}
	return values
}

func TestTypedColumnInt64PreparedLayoutContractCorruptionFailsClosed(t *testing.T) {
	d, col := setupTypedColumnInt64RawScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed refs=%+v want one", refs)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	contractSection, err := image.LayoutContractSection()
	if err != nil {
		t.Fatalf("LayoutContractSection: %v", err)
	}
	var corruptRows [8]byte
	binary.LittleEndian.PutUint64(corruptRows[:], uint64(image.Rows+1))
	// Contract row count sits after version/reserved/part_id in the contract section.
	writeColumnAssetBytesAtRelativeOffset(t, d, refs[0], int64(contractSection.Offset+12), corruptRows[:])
	_, err = col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums})
	if err == nil || !strings.Contains(err.Error(), "layout certification") || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("Prepare corrupt layout contract err=%v want fail-closed certification row mismatch", err)
	}
}

func TestTypedColumnInt64RawLayoutMatchesDeltaForQueryShapes(t *testing.T) {
	values := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	shapes := []TypedColumnInt64PredicateAggregateRequest{
		{Column: "time_us", Kind: TypedColumnInt64PredicateAll},
		{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 4},
		{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 3, High: 6},
		{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 99},
		{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: 0, High: 9},
	}
	for _, req := range shapes {
		req := req
		t.Run(string(req.Kind), func(t *testing.T) {
			deltaDB, deltaCol := setupTypedColumnInt64ScanCollection(t)
			defer func() { _ = deltaDB.Close() }()
			rawDB, rawCol := setupTypedColumnInt64RawScanCollection(t)
			defer func() { _ = rawDB.Close() }()
			insertTypedColumnInt64ScanRows(t, deltaCol, values)
			insertTypedColumnInt64ScanRows(t, rawCol, values)
			delta, err := deltaCol.RunTypedColumnInt64PredicateAggregate(req)
			if err != nil {
				t.Fatalf("delta aggregate: %v", err)
			}
			raw, err := rawCol.RunTypedColumnInt64PredicateAggregate(req)
			if err != nil {
				t.Fatalf("raw aggregate: %v", err)
			}
			if raw.Count != delta.Count || raw.Sum != delta.Sum || raw.Avg != delta.Avg {
				t.Fatalf("raw result count=%d sum=%d avg=%f want delta count=%d sum=%d avg=%f raw_diag=%+v delta_diag=%+v", raw.Count, raw.Sum, raw.Avg, delta.Count, delta.Sum, delta.Avg, raw.Diagnostics, delta.Diagnostics)
			}
			rawSession, err := rawCol.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: req.Column, Kind: req.Kind, Value: req.Value, Low: req.Low, High: req.High, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
			if err != nil {
				t.Fatalf("prepare raw: %v", err)
			}
			preparedRaw, err := rawSession.Run()
			if closeErr := rawSession.Close(); closeErr != nil {
				t.Fatalf("close raw session: %v", closeErr)
			}
			if err != nil {
				t.Fatalf("prepared raw: %v", err)
			}
			if preparedRaw.Count != delta.Count || preparedRaw.Sum != delta.Sum || preparedRaw.Avg != delta.Avg {
				t.Fatalf("prepared raw result=%+v want delta=%+v", preparedRaw, delta)
			}
		})
	}
}

func TestTypedColumnInt64RawTruncatedPayloadFailsClosed(t *testing.T) {
	d, col := setupTypedColumnInt64RawScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed-column refs=%+v want one", refs)
	}
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := os.Truncate(assetPath, refs[0].Offset+refs[0].Length-1); err != nil {
		t.Fatalf("Truncate raw typed-column asset: %v", err)
	}
	if _, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}); err == nil || !strings.Contains(err.Error(), "raw layout") {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate truncated raw err=%v want raw-layout fail closed", err)
	}
}

func TestTypedColumnInt64PredicateSelectionShapes(t *testing.T) {
	var scratch typedColumnInt64PredicateAggregateScanScratch

	scratch.predicateRows = append(scratch.predicateRows[:0], 2, 3, 4, 5)
	sel, err := typedColumnInt64PredicateRowsSelection(16, &scratch)
	if err != nil {
		t.Fatalf("range selection: %v", err)
	}
	if sel.Kind() != typedcolumn.RowSelectionRange || sel.Count() != 4 {
		t.Fatalf("range selection=%+v shape=%+v", sel, sel.Shape())
	}

	scratch.predicateRows = append(scratch.predicateRows[:0], 1, 2, 8, 9, 14, 15)
	sel, err = typedColumnInt64PredicateRowsSelection(20, &scratch)
	if err != nil {
		t.Fatalf("ranges selection: %v", err)
	}
	if sel.Kind() != typedcolumn.RowSelectionRanges || sel.Count() != 6 {
		t.Fatalf("ranges selection=%+v shape=%+v", sel, sel.Shape())
	}

	scratch.predicateRows = scratch.predicateRows[:0]
	for row := 0; row < 128; row++ {
		if row%4 != 0 {
			scratch.predicateRows = append(scratch.predicateRows, row)
		}
	}
	sel, err = typedColumnInt64PredicateRowsSelection(128, &scratch)
	if err != nil {
		t.Fatalf("bitmap selection: %v", err)
	}
	if sel.Kind() != typedcolumn.RowSelectionBitmap || sel.Count() != 96 {
		t.Fatalf("bitmap selection=%+v shape=%+v", sel, sel.Shape())
	}
}

func TestTypedColumnInt64RawSchemaRejectsLegacyDeltaAsset(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
	col.catalogMu.Lock()
	if col.catalog == nil || col.catalog.meta.Options.ColumnStore == nil || len(col.catalog.meta.Options.ColumnStore.Columns) == 0 {
		col.catalogMu.Unlock()
		t.Fatal("missing cached column store config")
	}
	col.catalog.meta.Options.ColumnStore.Columns[0].FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	col.catalogMu.Unlock()
	_, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err == nil || !strings.Contains(err.Error(), "schema mismatch") {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate mixed raw/delta err=%v want explicit schema mismatch", err)
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
	if diag.Fallback {
		t.Fatalf("diagnostics=%+v want direct aggregate path", diag)
	}
	if diag.DirectTypedColumnAssetReads == 0 {
		t.Fatalf("diagnostics=%+v want typed-column asset reads", diag)
	}
	assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, diag, "insert-only typed-column aggregate path")
}

func assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t *testing.T, diag TypedColumnInt64PredicateScanDiagnostics, context string) {
	t.Helper()
	zeroDiagnostics := map[string]int{
		"document_materializations": diag.DocumentMaterializations,
		"document_reconstructions":  diag.DocumentReconstructions,
		"row_materializations":      diag.RowMaterializations,
		"row_locator_decodes":       diag.RowLocatorDecodes,
		"physical_row_asset_reads":  diag.PhysicalRowAssetReads,
		"physical_row_id_lookups":   diag.PhysicalRowIDLookups,
	}
	for name, got := range zeroDiagnostics {
		if got != 0 {
			t.Fatalf("diagnostics=%+v %s=%d want 0 for %s", diag, name, got, context)
		}
	}
}

func TestTypedColumnInt64AggregatePreparedSessionLifecycle(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30, 20})

	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 20, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	first, err := session.Run()
	if err != nil {
		_ = session.Close()
		t.Fatalf("session first Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, first, 2, 40)
	if first.Diagnostics.Fallback || first.Diagnostics.DirectTypedColumnAssetReads == 0 || first.Diagnostics.SegmentFileCacheHits == 0 || first.Diagnostics.SegmentFileCacheMisses != 0 || first.Diagnostics.FullAssetReads != 0 || first.Diagnostics.SectionBytesRead != 0 || first.Diagnostics.DecodedMetadataBytes != 0 || first.Diagnostics.ColumnAssetReadIntegrity != string(ColumnAssetReadIntegrityCachedVerify) {
		_ = session.Close()
		t.Fatalf("first diagnostics=%+v want prepared hot direct cached-verify read with prepare-time validation", first.Diagnostics)
	}
	firstSessionDiag := session.Diagnostics()
	if firstSessionDiag.ActiveResourceHandles == 0 || firstSessionDiag.ActiveMappedBytes+firstSessionDiag.ActiveHeapCopyBytes == 0 || firstSessionDiag.TotalResourceAcquires == 0 {
		_ = session.Close()
		t.Fatalf("session diagnostics after first run=%+v want active resource handles", firstSessionDiag)
	}

	second, err := session.Run()
	if err != nil {
		_ = session.Close()
		t.Fatalf("session second Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, second, 2, 40)
	if second.Diagnostics.SegmentFileCacheHits == 0 || second.Diagnostics.SegmentFileCacheMisses != 0 {
		_ = session.Close()
		t.Fatalf("second diagnostics=%+v want hot cache hits and no misses", second.Diagnostics)
	}
	secondSessionDiag := session.Diagnostics()
	if firstSessionDiag.ActiveMappedBytes > 0 && secondSessionDiag.TotalResourceAcquires != firstSessionDiag.TotalResourceAcquires {
		_ = session.Close()
		t.Fatalf("session diagnostics after second run=%+v first=%+v want mapped handle reuse", secondSessionDiag, firstSessionDiag)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("session Close: %v", err)
	}
	closedDiag := session.Diagnostics()
	if !closedDiag.Closed || closedDiag.ActiveResourceHandles != 0 || closedDiag.ActiveMappedBytes != 0 || closedDiag.ActiveHeapCopyBytes != 0 {
		t.Fatalf("session diagnostics after close=%+v want no active handles or bytes", closedDiag)
	}
	if closedDiag.TotalResourceReleases != closedDiag.TotalResourceAcquires {
		t.Fatalf("session diagnostics after close=%+v want releases to match acquires", closedDiag)
	}
	if len(session.refsByGeneration) != 0 || len(session.validatedRefs) != 0 || len(session.view.AssetRefs) != 0 || len(session.view.TypedColumnPartRefs) != 0 {
		t.Fatalf("session retained stale refs after close: refs=%d validated=%d view_assets=%d typed_refs=%d", len(session.refsByGeneration), len(session.validatedRefs), len(session.view.AssetRefs), len(session.view.TypedColumnPartRefs))
	}
	if session.preparedState != nil && (!session.preparedState.closed || len(session.preparedState.partsByRef) != 0) {
		t.Fatalf("prepared state after close=%+v want closed with no part refs", session.preparedState)
	}
	if _, err := session.Run(); !errors.Is(err, errTypedColumnInt64PredicateAggregateSessionClosed) {
		t.Fatalf("session Run after Close err=%v want closed-session error", err)
	}
}

func TestTypedColumnInt64AggregatePreparedSessionHotScanUsesTargetedRanges(t *testing.T) {
	const rows = 65536
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	dist := typedColumnInt64AggregateBenchDistributionByName("clustered")
	insertTypedColumnInt64AggregateBenchRows(t, col, rows, dist)
	req := typedColumnInt64AggregateBenchShapeByName("range_1pct").request(rows)
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityCachedVerify
	expected := expectedTypedColumnInt64AggregateBenchResultForDistribution(rows, dist, req)

	session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	warmup, err := session.Run()
	if err != nil {
		t.Fatalf("warmup Run: %v", err)
	}
	if err := validateTypedColumnInt64AggregateBenchResult(warmup, expected); err != nil {
		t.Fatal(err)
	}
	if session.prepareDiagnostics.FullAssetBytes == 0 || session.prepareDiagnostics.SectionBytesRead == 0 || session.prepareDiagnostics.DecodedMetadataBytes == 0 {
		t.Fatalf("prepare diagnostics=%+v want one-time full validation plus prepared section metadata", session.prepareDiagnostics)
	}
	if warmup.Diagnostics.FullAssetBytes != 0 || warmup.Diagnostics.SectionBytesRead != 0 || warmup.Diagnostics.DecodedMetadataBytes != 0 || warmup.Diagnostics.RangeBytesRead == 0 {
		t.Fatalf("warmup diagnostics=%+v want prepared hot range reads only", warmup.Diagnostics)
	}

	hot, err := session.Run()
	if err != nil {
		t.Fatalf("hot Run: %v", err)
	}
	if err := validateTypedColumnInt64AggregateBenchResult(hot, expected); err != nil {
		t.Fatal(err)
	}
	diag := hot.Diagnostics
	assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, diag, "prepared typed-column aggregate hot scan")
	if diag.FullAssetBytes != 0 || diag.FullAssetReads != 0 {
		t.Fatalf("hot diagnostics=%+v want no per-run full-asset validation bytes after cached-verify session boundary", diag)
	}
	if diag.SectionBytesRead != 0 || diag.DecodedMetadataBytes != 0 || diag.RangeBytesRead == 0 {
		t.Fatalf("hot diagnostics=%+v want cached section metadata and candidate range bytes only", diag)
	}
	if diag.PhysicalBytesScanned != int64(diag.RangeBytesRead) {
		t.Fatalf("hot diagnostics=%+v want physical bytes to equal candidate range bytes without full asset bytes", diag)
	}
	if diag.BlocksPruned == 0 || diag.BlocksDecoded == 0 || diag.RowsScanned >= rows {
		t.Fatalf("hot diagnostics=%+v want pruned selective scan", diag)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	var totalTypedBytes int64
	for _, ref := range typedRefs {
		totalTypedBytes += ref.Length
	}
	if diag.PhysicalBytesScanned >= totalTypedBytes {
		t.Fatalf("hot diagnostics=%+v total_typed_bytes=%d want touched bytes below full typed-column assets", diag, totalTypedBytes)
	}
}

func TestTypedColumnInt64AggregatePreparedSessionShapeParity(t *testing.T) {
	const rows = 65536
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	dist := typedColumnInt64AggregateBenchDistributionByName("clustered")
	insertTypedColumnInt64AggregateBenchRows(t, col, rows, dist)
	for _, shapeName := range []string{"all_pruned_no_match", "exact_value", "tiny_range", "selective_range_1pct", "wide_range_10pct", "no_filter_full_aggregate", "all_match"} {
		t.Run(shapeName, func(t *testing.T) {
			shape := typedColumnInt64AggregateBenchShapeByName(shapeName)
			req := shape.request(rows)
			req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityCachedVerify
			expected := expectedTypedColumnInt64AggregateBenchResultForDistribution(rows, dist, req)
			session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
			if err != nil {
				t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
			}
			defer func() { _ = session.Close() }()
			result, err := session.Run()
			if err != nil {
				t.Fatalf("session Run: %v", err)
			}
			if err := validateTypedColumnInt64AggregateBenchResult(result, expected); err != nil {
				t.Fatal(err)
			}
			assertTypedColumnInt64AggregateNoMaterializationDiagnostics(t, result.Diagnostics, "prepared shape parity")
			if result.Diagnostics.FullAssetReads != 0 || result.Diagnostics.SectionBytesRead != 0 || result.Diagnostics.DecodedMetadataBytes != 0 {
				t.Fatalf("hot diagnostics=%+v want no full/section/metadata reads", result.Diagnostics)
			}
			if shapeName == "all_pruned_no_match" {
				if result.Diagnostics.DirectTypedColumnAssetReads != 0 || result.Diagnostics.RangeBytesRead != 0 || result.Diagnostics.PhysicalBytesScanned != 0 || result.Diagnostics.BlocksDecoded != 0 || result.Diagnostics.RowsScanned != 0 {
					t.Fatalf("all-pruned hot diagnostics=%+v want metadata-only pruning with no payload reads", result.Diagnostics)
				}
			}
		})
	}
}

func TestTypedColumnInt64AggregatePreparedSessionAllPrunedAllocsPerRunGuardrail(t *testing.T) {
	const rows = 65536
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	dist := typedColumnInt64AggregateBenchDistributionByName("clustered")
	insertTypedColumnInt64AggregateBenchRows(t, col, rows, dist)
	req := typedColumnInt64AggregateBenchShapeByName("all_pruned_no_match").request(rows)
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	for i := 0; i < 3; i++ {
		result, err := session.Run()
		if err != nil {
			t.Fatalf("warm Run %d: %v", i, err)
		}
		if result.Count != 0 || result.Diagnostics.RangeBytesRead != 0 || result.Diagnostics.BlocksDecoded != 0 {
			t.Fatalf("warm Run %d result=%+v diagnostics=%+v want all-pruned zero/payload-free", i, result, result.Diagnostics)
		}
	}
	var runErr error
	allocs := testing.AllocsPerRun(20, func() {
		result, err := session.Run()
		if err != nil && runErr == nil {
			runErr = err
		}
		if result.Count != 0 && runErr == nil {
			runErr = fmt.Errorf("count=%d want 0", result.Count)
		}
	})
	if runErr != nil {
		t.Fatalf("session Run during AllocsPerRun: %v", runErr)
	}
	if allocs > 16 {
		t.Fatalf("all-pruned prepared hot Run allocs/run=%.2f want <=16 fixed session overhead allocations", allocs)
	}
}

func TestTypedColumnInt64AggregatePreparedSessionSnapshotPinnedAcrossMutation(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnInt64ScanRows(t, col, []int64{10, 20, 30})
	req := TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
	session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	before, err := session.Run()
	if err != nil {
		_ = session.Close()
		t.Fatalf("session before mutation Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, before, 3, 60)
	if _, err := col.InsertBatch([][]byte{[]byte("e-new")}, [][]byte{[]byte(`{"time_us":100,"kind":"k"}`)}); err != nil {
		_ = session.Close()
		t.Fatalf("InsertBatch mutation: %v", err)
	}
	afterOldSnapshot, err := session.Run()
	if err != nil {
		_ = session.Close()
		t.Fatalf("session after mutation Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, afterOldSnapshot, 3, 60)
	if err := session.Close(); err != nil {
		t.Fatalf("Close old session: %v", err)
	}
	newSession, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
	if err != nil {
		t.Fatalf("Prepare new session: %v", err)
	}
	defer func() { _ = newSession.Close() }()
	fresh, err := newSession.Run()
	if err != nil {
		t.Fatalf("new session Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, fresh, 4, 160)
}

func TestTypedColumnInt64PreparedSessionCachedVerifyVisibilityUsesKernelPath(t *testing.T) {
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2"), []byte("e3")}, [][]byte{
		[]byte(`{"time_us":10,"kind":"k1"}`),
		[]byte(`{"time_us":20,"kind":"k2"}`),
		[]byte(`{"time_us":30,"kind":"k3"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":25,"kind":"k1b"}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update e1 changed=%v err=%v", changed, err)
	}
	if _, changed, err := col.Update([]byte("e2"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":40,"kind":"k2b"}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update e2 changed=%v err=%v", changed, err)
	}
	if deleted, err := col.DeleteDocument([]byte("e3")); err != nil || !deleted {
		t.Fatalf("DeleteDocument e3 deleted=%v err=%v", deleted, err)
	}

	session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll, ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify})
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	result, err := session.Run()
	if closeErr := session.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertTypedColumnInt64Aggregate(t, result, 2, 65)
	if result.Diagnostics.MutationParts == 0 || result.Diagnostics.SelectionCompositions == 0 || result.Diagnostics.KernelBlocks == 0 || result.Diagnostics.KernelCursorBlocks == 0 || result.Diagnostics.StatsBlocks != 0 || result.Diagnostics.StatsFallbackBlocks == 0 {
		t.Fatalf("diagnostics=%+v want cached-verify targeted kernel+visibility path with stats fallback", result.Diagnostics)
	}
	if result.Diagnostics.FullAssetReads != 0 || result.Diagnostics.SectionBytesRead != 0 || result.Diagnostics.DecodedMetadataBytes != 0 || result.Diagnostics.DecodedHeapCopyBytes != 0 || result.Diagnostics.KernelFallbackBlocks != 0 {
		t.Fatalf("diagnostics=%+v want hot targeted visibility run without full asset/materialized fallback", result.Diagnostics)
	}
}

func TestTypedColumnInt64AggregatePreparedSessionStreamsDeltaWithoutDecodeScratch(t *testing.T) {
	const rows = 65536
	d, col := setupTypedColumnInt64ScanCollection(t)
	defer func() { _ = d.Close() }()
	dist := typedColumnInt64AggregateBenchDistributionByName("clustered")
	insertTypedColumnInt64AggregateBenchRows(t, col, rows, dist)
	req := typedColumnInt64AggregateBenchShapeByName("range_1pct").request(rows)
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	expected := expectedTypedColumnInt64AggregateBenchResultForDistribution(rows, dist, req)

	session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
	if err != nil {
		t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
	}
	defer func() { _ = session.Close() }()
	first, err := session.Run()
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := validateTypedColumnInt64AggregateBenchResult(first, expected); err != nil {
		t.Fatal(err)
	}
	if first.Diagnostics.FastDecodeStreamingPlans == 0 || first.Diagnostics.DecodedHeapCopyBytes != 0 || len(session.aggregateScratch.values) != 0 {
		t.Fatalf("first diagnostics=%+v scratch_values=%d want delta streaming cursor without decode scratch", first.Diagnostics, len(session.aggregateScratch.values))
	}

	second, err := session.Run()
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if err := validateTypedColumnInt64AggregateBenchResult(second, expected); err != nil {
		t.Fatal(err)
	}
	if second.Diagnostics.FastDecodeStreamingPlans == 0 || second.Diagnostics.DecodedHeapCopyBytes != 0 || len(session.aggregateScratch.values) != 0 {
		t.Fatalf("second diagnostics=%+v scratch_values=%d want delta streaming cursor without decode scratch", second.Diagnostics, len(session.aggregateScratch.values))
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if session.aggregateScratch.values != nil {
		t.Fatalf("Close retained aggregate decode scratch len=%d cap=%d", len(session.aggregateScratch.values), cap(session.aggregateScratch.values))
	}
}

func TestTypedColumnInt64AggregatePreparedSessionIntegrityFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		integrity ColumnAssetReadIntegrity
	}{
		{name: "verify", integrity: ColumnAssetReadIntegrityVerify},
		{name: "cached_verify", integrity: ColumnAssetReadIntegrityCachedVerify},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.integrity == ColumnAssetReadIntegrityCachedVerify {
				resetColumnAssetVerifiedChecksumCacheForTest(t)
			}
			d, col := setupTypedColumnInt64ScanCollection(t)
			defer func() { _ = d.Close() }()
			insertTypedColumnInt64ScanRows(t, col, []int64{1, 2, 3})
			typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
			if len(typedRefs) != 1 {
				t.Fatalf("typed refs=%+v want one", typedRefs)
			}

			session, err := col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2, ColumnAssetReadIntegrity: tc.integrity})
			if err != nil {
				t.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
			}
			if _, err := session.Run(); err != nil {
				_ = session.Close()
				t.Fatalf("session warm Run: %v", err)
			}
			if tc.integrity == ColumnAssetReadIntegrityCachedVerify {
				if err := session.Close(); err != nil {
					t.Fatalf("cached-verify warm session Close: %v", err)
				}
				session = nil
			}
			corruptTypedColumnAssetPayload1755(t, d, typedRefs[0])
			assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), typedRefs[0])
			if err != nil {
				if session != nil {
					_ = session.Close()
				}
				t.Fatalf("columnAssetSegmentPath: %v", err)
			}
			changedModTime := time.Now().Add(2 * time.Hour).Round(0)
			if err := os.Chtimes(assetPath, changedModTime, changedModTime); err != nil {
				if session != nil {
					_ = session.Close()
				}
				t.Fatalf("Chtimes corrupt typed-column asset: %v", err)
			}
			if tc.integrity == ColumnAssetReadIntegrityCachedVerify {
				session, err = col.PrepareTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateEqual, Value: 2, ColumnAssetReadIntegrity: tc.integrity})
				if err == nil || !strings.Contains(err.Error(), "checksum") {
					if session != nil {
						_ = session.Close()
					}
					t.Fatalf("PrepareTypedColumnInt64PredicateAggregate after corruption err=%v want checksum failure", err)
				}
				return
			}
			_, err = session.Run()
			_ = session.Close()
			if err == nil || !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("session Run after corruption err=%v want checksum failure", err)
			}
		})
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

	t.Run("read_integrity", func(t *testing.T) {
		got, err := parseTypedColumnInt64AggregateBenchReadIntegrities("")
		if err != nil {
			t.Fatalf("default read integrity: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchReadIntegrityNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"cached_verify"}) {
			t.Fatalf("default read integrity=%v", gotNames)
		}
		got, err = parseTypedColumnInt64AggregateBenchReadIntegrities("cached_verify,skip_checksums")
		if err != nil {
			t.Fatalf("explicit read integrity: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchReadIntegrityNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"cached_verify", "unsafe_skip_checksums_ceiling"}) {
			t.Fatalf("explicit read integrity=%v", gotNames)
		}
		if _, err := parseTypedColumnInt64AggregateBenchReadIntegrities("verify"); err == nil || !strings.Contains(err.Error(), "invalid read integrity") {
			t.Fatalf("invalid read integrity err=%v", err)
		}
		if _, err := parseTypedColumnInt64AggregateBenchReadIntegrities("not_applicable"); err == nil || !strings.Contains(err.Error(), "invalid read integrity") {
			t.Fatalf("not_applicable should be reserved for fallback labels, err=%v", err)
		}
	})

	t.Run("layouts", func(t *testing.T) {
		got, err := parseTypedColumnInt64AggregateBenchLayouts("")
		if err != nil {
			t.Fatalf("default layouts: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchLayoutNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"delta_varint"}) {
			t.Fatalf("default layouts=%v", gotNames)
		}
		got, err = parseTypedColumnInt64AggregateBenchLayouts("delta,raw")
		if err != nil {
			t.Fatalf("explicit layouts: %v", err)
		}
		if gotNames := typedColumnInt64AggregateBenchLayoutNames(got); fmt.Sprint(gotNames) != fmt.Sprint([]string{"delta_varint", "raw_int64"}) {
			t.Fatalf("explicit layouts=%v", gotNames)
		}
		if _, err := parseTypedColumnInt64AggregateBenchLayouts("nope"); err == nil || !strings.Contains(err.Error(), "invalid layout") {
			t.Fatalf("invalid layout err=%v", err)
		}
	})

	t.Run("fallback_reason_metric_tokens", func(t *testing.T) {
		for _, tc := range []struct {
			input string
			want  string
		}{
			{input: "typed_column_not_selected", want: "typed_column_not_selected"},
			{input: "Capability: nullable carrier aggregate semantics", want: "capability_nullable_carrier_aggregate_semantics"},
			{input: "!!!", want: "unknown"},
		} {
			if got := typedColumnInt64AggregateBenchMetricToken(tc.input); got != tc.want {
				t.Fatalf("metric token %q=%q want %q", tc.input, got, tc.want)
			}
		}
	})

	t.Run("sub_benchmark_names", func(t *testing.T) {
		name := typedColumnInt64AggregateBenchSubBenchmarkName(256, typedColumnInt64AggregateBenchDistributionByName("clustered_monotonic"), "typed_column_part", typedColumnInt64AggregateBenchShapeByName("range_1pct"), typedColumnInt64AggregateBenchReadIntegrityByName("cached_verify"), typedColumnInt64AggregateBenchExecutionModeByName("serial"))
		want := "rows_256/dist_clustered_monotonic/path_typed_column_part/shape_selective_range_1pct/timed_one_shot_api/read_integrity_cached_verify/execution_serial/predicate_count_sum_avg"
		if name != want {
			t.Fatalf("serial sub-benchmark name=%q want %q", name, want)
		}
		parallelUnsafe := typedColumnInt64AggregateBenchSubBenchmarkName(256, typedColumnInt64AggregateBenchDistributionByName("clustered_monotonic"), "typed_column_part", typedColumnInt64AggregateBenchShapeByName("range_1pct"), typedColumnInt64AggregateBenchReadIntegrityByName("skip_checksums"), typedColumnInt64AggregateBenchExecutionModeByName("parallel_contention"))
		if !strings.Contains(parallelUnsafe, "/timed_one_shot_api/read_integrity_unsafe_skip_checksums_ceiling/execution_parallel_contention/") {
			t.Fatalf("parallel unsafe sub-benchmark name=%q missing timed boundary/read integrity/execution labels", parallelUnsafe)
		}
		prepared := typedColumnInt64AggregateBenchSubBenchmarkName(256, typedColumnInt64AggregateBenchDistributionByName("clustered_monotonic"), "typed_column_part", typedColumnInt64AggregateBenchShapeByName("range_1pct"), typedColumnInt64AggregateBenchReadIntegrityByName("cached_verify"), typedColumnInt64AggregateBenchExecutionModeByName("prepared_session_serial"))
		if !strings.Contains(prepared, "/timed_prepared_session_hot_scan/read_integrity_cached_verify/execution_serial/") {
			t.Fatalf("prepared sub-benchmark name=%q missing prepared timed boundary", prepared)
		}
		fallback := typedColumnInt64AggregateBenchSubBenchmarkName(256, typedColumnInt64AggregateBenchDistributionByName("clustered_monotonic"), "document_full_scan_fallback", typedColumnInt64AggregateBenchShapeByName("range_1pct"), typedColumnInt64AggregateBenchReadIntegrityNotApplicable(), typedColumnInt64AggregateBenchExecutionModeByName("serial"))
		if !strings.Contains(fallback, "/path_document_full_scan_fallback/") || !strings.Contains(fallback, "/read_integrity_not_applicable/") {
			t.Fatalf("fallback sub-benchmark name=%q missing fallback/not-applicable labels", fallback)
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

func BenchmarkTypedColumnMultipartLatestVisibleLookup(b *testing.B) {
	d, col := setupTypedColumnInt64ScanCollection(b)
	defer func() { _ = d.Close() }()
	const rows = 1024
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		ids[i] = []byte(fmt.Sprintf("e%04d", i))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"k%d"}`, i, i%8))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch: %v", err)
	}
	for i := 0; i < rows; i += 16 {
		id := append([]byte(nil), ids[i]...)
		value := i + rows
		if _, changed, err := col.Update(id, func(current []byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"time_us":%d,"kind":"ku"}`, value)), true, nil
		}); err != nil || !changed {
			b.Fatalf("Update %s changed=%v err=%v", id, changed, err)
		}
	}
	for i := 8; i < rows; i += 32 {
		if deleted, err := col.DeleteDocument(ids[i]); err != nil || !deleted {
			b.Fatalf("DeleteDocument %s deleted=%v err=%v", ids[i], deleted, err)
		}
	}
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		b.Fatalf("prepareColumnPhysicalScanSnapshotViewWithSidecars: %v", err)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegrityCachedVerify)
	if err != nil {
		b.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity: %v", err)
	}
	defer func() { _ = readCache.close() }()
	resolver, err := buildTypedColumnLatestRowResolver(view, &readCache, nil)
	if err != nil {
		b.Fatalf("buildTypedColumnLatestRowResolver: %v", err)
	}
	visibleRows := 0
	for i := range resolver.parts {
		part := &resolver.parts[i]
		for row := 0; row < part.Rows; row++ {
			if part.rowVisible(row) {
				visibleRows++
			}
		}
	}
	if visibleRows == 0 {
		b.Fatal("visible rows is zero")
	}
	b.ReportAllocs()
	b.ReportMetric(float64(visibleRows), "visible_rows/op")
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		count := 0
		for partIdx := range resolver.parts {
			part := &resolver.parts[partIdx]
			for row := 0; row < part.Rows; row++ {
				if !part.rowVisible(row) {
					continue
				}
				if len(part.documentID(row)) == 0 {
					b.Fatalf("visible row without document id part=%d row=%d", partIdx, row)
				}
				count++
			}
		}
		total += count
	}
	if total == 0 {
		b.Fatal("total is zero")
	}
}

func BenchmarkTypedColumnInt64PrepareCertification(b *testing.B) {
	const rows = 65536
	for _, layout := range []typedColumnInt64AggregateBenchLayout{typedColumnInt64AggregateBenchLayoutByName("delta_varint"), typedColumnInt64AggregateBenchLayoutByName("raw_int64")} {
		layout := layout
		b.Run("layout_"+layout.name, func(b *testing.B) {
			d, col := setupTypedColumnInt64AggregateBenchCollection(b, true, layout)
			defer func() { _ = d.Close() }()
			insertTypedColumnInt64AggregateBenchRows(b, col, rows, typedColumnInt64AggregateBenchDistributionByName("clustered"))
			req := TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateRange, Low: int64(rows / 4), High: int64(rows / 2), ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
			var diag TypedColumnInt64PredicateScanDiagnostics
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				session, err := col.PrepareTypedColumnInt64PredicateAggregate(req)
				if err != nil {
					b.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
				}
				diag = session.prepareDiagnostics
				if err := session.Close(); err != nil {
					b.Fatalf("Close prepared session: %v", err)
				}
			}
			reportTypedColumnInt64AggregateBenchMetrics(b, rows, diag, b.Elapsed(), b.N)
		})
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
	readIntegrities, err := parseTypedColumnInt64AggregateBenchReadIntegrities(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY"))
	if err != nil {
		b.Fatalf("TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY: %v", err)
	}
	layouts, err := parseTypedColumnInt64AggregateBenchLayouts(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_LAYOUTS"))
	if err != nil {
		b.Fatalf("TREEDB_TYPED_COLUMN_BENCH_LAYOUTS: %v", err)
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
				for _, layout := range layouts {
					layout := layout
					for _, readIntegrity := range readIntegrities {
						readIntegrity := readIntegrity
						runTypedColumnInt64AggregateBenchPath(b, rows, dist, shape, req, expected, true, readIntegrity, layout)
					}
				}
				if typedColumnInt64AggregateBenchIncludeFallback(includeFallbackEnv, rows) {
					runTypedColumnInt64AggregateBenchPath(b, rows, dist, shape, req, expected, false, typedColumnInt64AggregateBenchReadIntegrityNotApplicable(), typedColumnInt64AggregateBenchLayoutDefault())
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

type typedColumnInt64AggregateBenchReadIntegrity struct {
	name      string
	integrity ColumnAssetReadIntegrity
}

type typedColumnInt64AggregateBenchLayout struct {
	name               string
	fixedWidthEncoding ColumnFixedWidthEncoding
}

type typedColumnInt64AggregateBenchExecutionMode struct {
	name          string
	timedBoundary string
	typedPathOnly bool
	run           func(*testing.B, *Collection, *TypedColumnInt64PredicateAggregateSession, TypedColumnInt64PredicateAggregateRequest, typedColumnInt64AggregateBenchExpected) (TypedColumnInt64PredicateAggregateResult, error)
}

const typedColumnInt64AggregateBenchBatchRows = 32768

func runTypedColumnInt64AggregateBenchPath(b *testing.B, rows int, dist typedColumnInt64AggregateBenchDistribution, shape typedColumnInt64AggregateBenchShape, req TypedColumnInt64PredicateAggregateRequest, expected typedColumnInt64AggregateBenchExpected, typedPath bool, readIntegrity typedColumnInt64AggregateBenchReadIntegrity, layout typedColumnInt64AggregateBenchLayout) {
	pathName := "document_full_scan_fallback"
	if typedPath {
		pathName = "typed_column_part"
		if layout.name != "" && layout.name != "delta_varint" {
			pathName = "typed_column_part_" + layout.name
		}
	}
	for _, execution := range typedColumnInt64AggregateBenchExecutionModes() {
		execution := execution
		if execution.typedPathOnly && !typedPath {
			continue
		}
		b.Run(typedColumnInt64AggregateBenchSubBenchmarkName(rows, dist, pathName, shape, readIntegrity, execution), func(b *testing.B) {
			setupStart := time.Now()
			d, col := setupTypedColumnInt64AggregateBenchCollection(b, typedPath, layout)
			defer func() { _ = d.Close() }()
			batches := insertTypedColumnInt64AggregateBenchRows(b, col, rows, dist)
			setupDuration := time.Since(setupStart)
			setupMetrics := collectTypedColumnInt64AggregateBenchSetupMetrics(rows, batches, d, typedPath, setupDuration)

			req.Column = "time_us"
			req.ColumnAssetReadIntegrity = readIntegrity.integrity
			b.StopTimer()
			var session *TypedColumnInt64PredicateAggregateSession
			if execution.timedBoundary == "prepared_session_hot_scan" {
				var err error
				session, err = col.PrepareTypedColumnInt64PredicateAggregate(req)
				if err != nil {
					b.Fatalf("PrepareTypedColumnInt64PredicateAggregate: %v", err)
				}
				warmup, err := session.Run()
				if err != nil {
					_ = session.Close()
					b.Fatalf("prepared warmup Run: %v", err)
				}
				if err := validateTypedColumnInt64AggregateBenchResult(warmup, expected); err != nil {
					_ = session.Close()
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			reportTypedColumnInt64AggregateBenchSetupMetrics(b, setupMetrics)
			stopHotCPUProfile := startTypedColumnInt64AggregateBenchHotCPUProfile(b)
			b.StartTimer()
			result, runErr := execution.run(b, col, session, req, expected)
			b.StopTimer()
			if stopHotCPUProfile != nil {
				stopHotCPUProfile()
			}
			if session != nil {
				if err := session.Close(); err != nil {
					b.Fatalf("prepared session Close: %v", err)
				}
			}
			if runErr != nil {
				b.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", runErr)
			}
			if err := validateTypedColumnInt64AggregateBenchResult(result, expected); err != nil {
				b.Fatal(err)
			}
			reportTypedColumnInt64AggregateBenchMetrics(b, rows, result.Diagnostics, b.Elapsed(), b.N)
			if session != nil {
				b.ReportMetric(float64(session.prepareDiagnostics.DirectViewCertified), "prepare_direct_view_certified/op")
				b.ReportMetric(float64(session.prepareDiagnostics.StreamingCertified), "prepare_streaming_certified/op")
				b.ReportMetric(float64(session.prepareDiagnostics.PruningCertified), "prepare_pruning_certified/op")
				b.ReportMetric(float64(session.prepareDiagnostics.PruningBlocks), "prepare_pruning_blocks/op")
				b.ReportMetric(float64(session.prepareDiagnostics.PruningRows), "prepare_pruning_rows/op")
				b.ReportMetric(float64(session.prepareDiagnostics.CertificationFailures), "prepare_certification_failures/op")
				b.ReportMetric(float64(session.prepareDiagnostics.PruningValidationFailures), "prepare_pruning_validation_failures/op")
			}
		})
	}
}

func typedColumnInt64AggregateBenchSubBenchmarkName(rows int, dist typedColumnInt64AggregateBenchDistribution, pathName string, shape typedColumnInt64AggregateBenchShape, readIntegrity typedColumnInt64AggregateBenchReadIntegrity, execution typedColumnInt64AggregateBenchExecutionMode) string {
	timedBoundary := execution.timedBoundary
	if timedBoundary == "" {
		timedBoundary = "one_shot_api"
	}
	return fmt.Sprintf("rows_%d/dist_%s/path_%s/shape_%s/timed_%s/read_integrity_%s/execution_%s/predicate_count_sum_avg", rows, dist.name, pathName, shape.name, timedBoundary, readIntegrity.name, execution.name)
}

func runTypedColumnInt64AggregateBenchOneShotSerial(b *testing.B, col *Collection, _ *TypedColumnInt64PredicateAggregateSession, req TypedColumnInt64PredicateAggregateRequest, expected typedColumnInt64AggregateBenchExpected) (TypedColumnInt64PredicateAggregateResult, error) {
	b.Helper()
	var result TypedColumnInt64PredicateAggregateResult
	for i := 0; i < b.N; i++ {
		var err error
		result, err = col.RunTypedColumnInt64PredicateAggregate(req)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func runTypedColumnInt64AggregateBenchOneShotParallel(b *testing.B, col *Collection, _ *TypedColumnInt64PredicateAggregateSession, req TypedColumnInt64PredicateAggregateRequest, expected typedColumnInt64AggregateBenchExpected) (TypedColumnInt64PredicateAggregateResult, error) {
	b.Helper()
	var firstResult TypedColumnInt64PredicateAggregateResult
	var firstResultOnce sync.Once
	var firstErr atomic.Value
	var stop atomic.Bool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if stop.Load() {
				continue
			}
			result, err := col.RunTypedColumnInt64PredicateAggregate(req)
			if err == nil {
				err = validateTypedColumnInt64AggregateBenchResult(result, expected)
			}
			if err != nil {
				if stop.CompareAndSwap(false, true) {
					firstErr.Store(err)
				}
				continue
			}
			firstResultOnce.Do(func() { firstResult = result })
		}
	})
	if errValue := firstErr.Load(); errValue != nil {
		return firstResult, errValue.(error)
	}
	return firstResult, nil
}

func runTypedColumnInt64AggregateBenchPreparedSessionSerial(b *testing.B, _ *Collection, session *TypedColumnInt64PredicateAggregateSession, _ TypedColumnInt64PredicateAggregateRequest, _ typedColumnInt64AggregateBenchExpected) (TypedColumnInt64PredicateAggregateResult, error) {
	b.Helper()
	if session == nil {
		return TypedColumnInt64PredicateAggregateResult{}, errors.New("missing prepared typed-column int64 aggregate session")
	}
	var result TypedColumnInt64PredicateAggregateResult
	for i := 0; i < b.N; i++ {
		var err error
		result, err = session.Run()
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func validateTypedColumnInt64AggregateBenchResult(result TypedColumnInt64PredicateAggregateResult, expected typedColumnInt64AggregateBenchExpected) error {
	if result.Count != expected.count || result.Sum != expected.sum || result.Avg != expected.avg {
		return fmt.Errorf("aggregate count=%d sum=%d avg=%f want count=%d sum=%d avg=%f diagnostics=%+v", result.Count, result.Sum, result.Avg, expected.count, expected.sum, expected.avg, result.Diagnostics)
	}
	return nil
}

func setupTypedColumnInt64AggregateBenchCollection(tb testing.TB, typedPath bool, layout typedColumnInt64AggregateBenchLayout) (*backenddb.DB, *Collection) {
	tb.Helper()
	if typedPath {
		d := openTypedColumnInt64ScanDB(tb)
		return d, createTypedColumnInt64ScanCollectionWithFixedWidthEncoding(tb, d, layout.fixedWidthEncoding)
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

func parseTypedColumnInt64AggregateBenchReadIntegrities(env string) ([]typedColumnInt64AggregateBenchReadIntegrity, error) {
	if strings.TrimSpace(env) == "" {
		return []typedColumnInt64AggregateBenchReadIntegrity{typedColumnInt64AggregateBenchReadIntegrityByName("cached_verify")}, nil
	}
	parts := strings.Split(env, ",")
	out := make([]typedColumnInt64AggregateBenchReadIntegrity, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("empty read integrity")
		}
		if strings.EqualFold(name, "all") {
			out = append(out, typedColumnInt64AggregateBenchReadIntegrityByName("cached_verify"), typedColumnInt64AggregateBenchReadIntegrityByName("skip_checksums"))
			continue
		}
		readIntegrity := typedColumnInt64AggregateBenchReadIntegrityByName(name)
		if readIntegrity.name == "" {
			return nil, fmt.Errorf("invalid read integrity %q (available: cached_verify,skip_checksums,all)", name)
		}
		out = append(out, readIntegrity)
	}
	return out, nil
}

func typedColumnInt64AggregateBenchReadIntegrityByName(name string) typedColumnInt64AggregateBenchReadIntegrity {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "cached", "cached_verify":
		return typedColumnInt64AggregateBenchReadIntegrity{name: "cached_verify", integrity: ColumnAssetReadIntegrityCachedVerify}
	case "skip", "skip_checksums", "unsafe_skip_checksums", "unsafe_skip_checksums_ceiling":
		// Unsafe checksum-skipping is benchmark-only ceiling coverage. It is not
		// the default and must be explicitly requested with
		// TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY.
		return typedColumnInt64AggregateBenchReadIntegrity{name: "unsafe_skip_checksums_ceiling", integrity: ColumnAssetReadIntegritySkipChecksums}
	default:
		return typedColumnInt64AggregateBenchReadIntegrity{}
	}
}

func typedColumnInt64AggregateBenchReadIntegrityNotApplicable() typedColumnInt64AggregateBenchReadIntegrity {
	return typedColumnInt64AggregateBenchReadIntegrity{name: "not_applicable"}
}

func typedColumnInt64AggregateBenchReadIntegrityNames(readIntegrities []typedColumnInt64AggregateBenchReadIntegrity) []string {
	names := make([]string, len(readIntegrities))
	for i, readIntegrity := range readIntegrities {
		names[i] = readIntegrity.name
	}
	return names
}

func parseTypedColumnInt64AggregateBenchLayouts(env string) ([]typedColumnInt64AggregateBenchLayout, error) {
	if strings.TrimSpace(env) == "" {
		return []typedColumnInt64AggregateBenchLayout{typedColumnInt64AggregateBenchLayoutDefault()}, nil
	}
	parts := strings.Split(env, ",")
	out := make([]typedColumnInt64AggregateBenchLayout, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("empty layout")
		}
		if strings.EqualFold(name, "all") {
			out = append(out, typedColumnInt64AggregateBenchLayoutByName("delta_varint"), typedColumnInt64AggregateBenchLayoutByName("raw_int64"))
			continue
		}
		layout := typedColumnInt64AggregateBenchLayoutByName(name)
		if layout.name == "" {
			return nil, fmt.Errorf("invalid layout %q (available: delta_varint,raw_int64,all)", name)
		}
		out = append(out, layout)
	}
	return out, nil
}

func typedColumnInt64AggregateBenchLayoutDefault() typedColumnInt64AggregateBenchLayout {
	return typedColumnInt64AggregateBenchLayoutByName("delta_varint")
}

func typedColumnInt64AggregateBenchLayoutByName(name string) typedColumnInt64AggregateBenchLayout {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "delta", "delta_varint", "legacy":
		return typedColumnInt64AggregateBenchLayout{name: "delta_varint", fixedWidthEncoding: ColumnFixedWidthEncodingDefault}
	case "raw", "raw_int64", "fixed", "fixed_width":
		return typedColumnInt64AggregateBenchLayout{name: "raw_int64", fixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}
	default:
		return typedColumnInt64AggregateBenchLayout{}
	}
}

func typedColumnInt64AggregateBenchLayoutNames(layouts []typedColumnInt64AggregateBenchLayout) []string {
	names := make([]string, len(layouts))
	for i, layout := range layouts {
		names[i] = layout.name
	}
	return names
}

func typedColumnInt64AggregateBenchExecutionModes() []typedColumnInt64AggregateBenchExecutionMode {
	return []typedColumnInt64AggregateBenchExecutionMode{
		typedColumnInt64AggregateBenchExecutionModeByName("serial"),
		typedColumnInt64AggregateBenchExecutionModeByName("parallel_contention"),
		typedColumnInt64AggregateBenchExecutionModeByName("prepared_session_serial"),
	}
}

func typedColumnInt64AggregateBenchExecutionModeByName(name string) typedColumnInt64AggregateBenchExecutionMode {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "serial":
		return typedColumnInt64AggregateBenchExecutionMode{name: "serial", timedBoundary: "one_shot_api", run: runTypedColumnInt64AggregateBenchOneShotSerial}
	case "parallel", "parallel_contention", "runparallel":
		return typedColumnInt64AggregateBenchExecutionMode{name: "parallel_contention", timedBoundary: "one_shot_api", run: runTypedColumnInt64AggregateBenchOneShotParallel}
	case "prepared", "prepared_session", "prepared_session_serial", "session_serial":
		return typedColumnInt64AggregateBenchExecutionMode{name: "serial", timedBoundary: "prepared_session_hot_scan", typedPathOnly: true, run: runTypedColumnInt64AggregateBenchPreparedSessionSerial}
	default:
		return typedColumnInt64AggregateBenchExecutionMode{}
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
	b.ReportMetric(float64(diag.SelectionEmptyBlocks), "selection_empty_blocks/op")
	b.ReportMetric(float64(diag.SelectionAllBlocks), "selection_all_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangeBlocks), "selection_range_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangesBlocks), "selection_ranges_blocks/op")
	b.ReportMetric(float64(diag.SelectionBitmapBlocks), "selection_bitmap_blocks/op")
	b.ReportMetric(float64(diag.SelectionSparseBlocks), "selection_sparse_blocks/op")
	b.ReportMetric(float64(diag.SelectionCompositions), "selection_compositions/op")
	b.ReportMetric(float64(diag.FullAssetReads), "full_asset_reads/op")
	b.ReportMetric(float64(diag.FullAssetBytes), "full_asset_bytes/op")
	b.ReportMetric(float64(diag.SectionBytesRead), "section_bytes_read/op")
	b.ReportMetric(float64(diag.RangeBytesRead), "range_bytes_read/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.DecodedHeapCopyBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.MaterializedBytes), "materialized_bytes/op")
	b.ReportMetric(float64(diag.FastDecodeDirectViewPlans), "fast_decode_direct_view_plans/op")
	b.ReportMetric(float64(diag.FastDecodeStreamingPlans), "fast_decode_streaming_plans/op")
	b.ReportMetric(float64(diag.FastDecodeMaterializePlans), "fast_decode_materialize_plans/op")
	b.ReportMetric(float64(diag.FastDecodeUnsupportedPlans), "fast_decode_unsupported_plans/op")
	b.ReportMetric(float64(diag.FastDecodeMmapDirectViews), "mmap_direct_view/op")
	b.ReportMetric(float64(diag.FastDecodeHeapCopyTypedViews), "heap_copy_typed_view/op")
	b.ReportMetric(float64(diag.FastDecodeScratchDecodes), "scratch_decode/op")
	b.ReportMetric(float64(diag.FastDecodeStreamingFallbacks), "streaming_fallback/op")
	b.ReportMetric(float64(diag.FastDecodeCertificationFailure), "certification_failure/op")
	b.ReportMetric(float64(diag.FastDecodeAbsoluteUnaligned), "absolute_offset_unaligned/op")
	b.ReportMetric(float64(diag.FastDecodeActualUnaligned), "actual_pointer_unaligned/op")
	b.ReportMetric(float64(diag.FastDecodeStaleHandles), "stale_handle/op")
	b.ReportMetric(float64(diag.DirectViewSuccesses), "direct_view_successes/op")
	b.ReportMetric(float64(diag.DirectViewFailures), "direct_view_failures/op")
	b.ReportMetric(float64(diag.KernelBlocks), "kernel_blocks/op")
	b.ReportMetric(float64(diag.KernelFullCoveredBlocks), "kernel_full_covered_blocks/op")
	b.ReportMetric(float64(diag.KernelSelectedBlocks), "kernel_selected_blocks/op")
	b.ReportMetric(float64(diag.KernelCursorBlocks), "kernel_cursor_blocks/op")
	b.ReportMetric(float64(diag.KernelFallbackBlocks), "kernel_fallback_blocks/op")
	b.ReportMetric(float64(diag.StatsBlocks), "stats_blocks/op")
	b.ReportMetric(float64(diag.StatsFullCoveredBlocks), "stats_full_covered_blocks/op")
	b.ReportMetric(float64(diag.StatsFallbackBlocks), "stats_fallback_blocks/op")
	b.ReportMetric(float64(diag.StatsRows), "stats_rows/op")
	b.ReportMetric(float64(diag.StatsValidationFailures), "stats_validation_failures/op")
	b.ReportMetric(float64(diag.PruningBlocks), "pruning_blocks/op")
	b.ReportMetric(float64(diag.PruningRows), "pruning_rows/op")
	b.ReportMetric(float64(diag.PruningFallbackBlocks), "pruning_fallback_blocks/op")
	b.ReportMetric(float64(diag.PruningValidationFailures), "pruning_validation_failures/op")
	b.ReportMetric(float64(diag.DirectViewCertified), "direct_view_certified/op")
	b.ReportMetric(float64(diag.StreamingCertified), "streaming_certified/op")
	b.ReportMetric(float64(diag.PruningCertified), "pruning_certified/op")
	b.ReportMetric(float64(diag.CertificationFailures), "certification_failures/op")
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.RowLocatorDecodes), "row_locator_decodes/op")
	b.ReportMetric(float64(diag.PhysicalRowIDLookups), "physical_row_id_lookups/op")
	b.ReportMetric(float64(diag.PhysicalRowAssetReads), "physical_row_asset_reads/op")
	b.ReportMetric(float64(diag.RowMaterializations), "row_materializations/op")
	b.ReportMetric(float64(diag.DocumentMaterializations), "document_materializations/op")
	b.ReportMetric(float64(diag.DocumentReconstructions), "document_reconstructions/op")
	fallbackCount := 0
	if diag.Fallback {
		fallbackCount = 1
	}
	b.ReportMetric(float64(fallbackCount), "fallback_count")
	if diag.FallbackReason != "" {
		b.ReportMetric(1, "fallback_reason_"+typedColumnInt64AggregateBenchMetricToken(diag.FallbackReason)+"_count")
	}
	if diag.FastDecodeFallbackReason != "" {
		b.ReportMetric(1, "fast_decode_fallback_reason_"+typedColumnInt64AggregateBenchMetricToken(diag.FastDecodeFallbackReason)+"_count")
	}
	if diag.PruningFallbackReason != "" {
		b.ReportMetric(1, "pruning_fallback_reason_"+typedColumnInt64AggregateBenchMetricToken(diag.PruningFallbackReason)+"_count")
	}
}

func startTypedColumnInt64AggregateBenchHotCPUProfile(b *testing.B) func() {
	b.Helper()
	profilePath := strings.TrimSpace(os.Getenv("TREEDB_TYPED_COLUMN_BENCH_HOT_CPU_PROFILE"))
	if profilePath == "" {
		return nil
	}
	if dir := filepath.Dir(profilePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatalf("create hot CPU profile dir %q: %v", dir, err)
		}
	}
	file, err := os.Create(profilePath)
	if err != nil {
		b.Fatalf("create hot CPU profile %q: %v", profilePath, err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		b.Fatalf("start hot CPU profile %q: %v", profilePath, err)
	}
	return func() {
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			b.Fatalf("close hot CPU profile %q: %v", profilePath, err)
		}
	}
}

func typedColumnInt64AggregateBenchMetricToken(reason string) string {
	var builder strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToLower(reason) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore && builder.Len() != 0 {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(builder.String(), "_")
	if out == "" {
		return "unknown"
	}
	return out
}

func reportTypedColumnInt64ScanBenchMetrics(b *testing.B, diag TypedColumnInt64PredicateScanDiagnostics, elapsed time.Duration, iterations int) {
	b.Helper()
	if elapsed > 0 && iterations > 0 {
		b.ReportMetric(float64(iterations)/elapsed.Seconds(), "ops/sec")
	}
	b.ReportMetric(float64(diag.FullAssetReads), "full_asset_reads/op")
	b.ReportMetric(float64(diag.FullAssetBytes), "full_asset_bytes/op")
	b.ReportMetric(float64(diag.SectionBytesRead), "section_bytes_read/op")
	b.ReportMetric(float64(diag.RangeBytesRead), "range_bytes_read/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.DecodedHeapCopyBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.MaterializedBytes), "materialized_bytes/op")
	b.ReportMetric(float64(diag.DirectViewCertified), "direct_view_certified/op")
	b.ReportMetric(float64(diag.StreamingCertified), "streaming_certified/op")
	b.ReportMetric(float64(diag.CertificationFailures), "certification_failures/op")
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.PartsPruned), "parts_pruned/op")
	b.ReportMetric(float64(diag.BlocksPruned), "blocks_pruned/op")
	b.ReportMetric(float64(diag.SelectionEmptyBlocks), "selection_empty_blocks/op")
	b.ReportMetric(float64(diag.SelectionAllBlocks), "selection_all_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangeBlocks), "selection_range_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangesBlocks), "selection_ranges_blocks/op")
	b.ReportMetric(float64(diag.SelectionBitmapBlocks), "selection_bitmap_blocks/op")
	b.ReportMetric(float64(diag.SelectionSparseBlocks), "selection_sparse_blocks/op")
	b.ReportMetric(float64(diag.SelectionCompositions), "selection_compositions/op")
}

type typedColumnInt64AggregateTestBlockRange struct {
	offset              int
	length              int
	columnSectionLength int
	min                 int64
	max                 int64
	hasMinMax           bool
}

func typedColumnInt64AggregateFindPrunedBlockRange(tb testing.TB, d *backenddb.DB, refs []ColumnAssetRef, req TypedColumnInt64PredicateAggregateRequest) (ColumnAssetRef, typedColumnInt64AggregateTestBlockRange) {
	tb.Helper()
	scanReq := typedColumnInt64PredicateAggregateScanRequest(req)
	for _, ref := range refs {
		blocks := typedColumnInt64AggregateTestBlockRanges(tb, d, ref, req.Column)
		for _, block := range blocks {
			if block.hasMinMax && !typedColumnInt64PredicateMayMatch(scanReq, block.min, block.max) {
				return ref, block
			}
		}
	}
	tb.Fatalf("no pruned block found for req=%+v refs=%+v", req, refs)
	return ColumnAssetRef{}, typedColumnInt64AggregateTestBlockRange{}
}

func typedColumnInt64ColumnEncodingForTest(tb testing.TB, d *backenddb.DB, ref ColumnAssetRef, column string) typedcolumn.Encoding {
	tb.Helper()
	_, columns := typedColumnInt64DescriptorColumnsForTest(tb, d, ref)
	valueCol, ok := columns[column]
	if !ok {
		tb.Fatalf("missing column %q in descriptor", column)
	}
	return valueCol.Definition.Encoding
}

func typedColumnInt64DescriptorColumnsForTest(tb testing.TB, d *backenddb.DB, ref ColumnAssetRef) (typedcolumn.ColumnPartImage, map[string]typedcolumn.ColumnPartColumn) {
	tb.Helper()
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		tb.Fatalf("read typed-column part: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	descriptor, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDescriptor)
	if err != nil {
		tb.Fatalf("descriptor section: %v", err)
	}
	_, columns, err := typedcolumn.DecodeColumnPartDescriptorSection(image.Bytes[descriptor.Offset : descriptor.Offset+descriptor.Length])
	if err != nil {
		tb.Fatalf("DecodeColumnPartDescriptorSection: %v", err)
	}
	return image, columns
}

func typedColumnInt64AggregateTestBlockRanges(tb testing.TB, d *backenddb.DB, ref ColumnAssetRef, column string) []typedColumnInt64AggregateTestBlockRange {
	tb.Helper()
	image, columns := typedColumnInt64DescriptorColumnsForTest(tb, d, ref)
	valueCol, ok := columns[column]
	if !ok {
		tb.Fatalf("missing column %q in descriptor", column)
	}
	section, ok := typedColumnAdapterColumnDataSection(image, column)
	if !ok {
		tb.Fatalf("missing column data section %q", column)
	}
	out := make([]typedColumnInt64AggregateTestBlockRange, 0, len(valueCol.Blocks))
	offset := section.Offset
	for _, block := range valueCol.Blocks {
		length := block.Descriptor.StoredBytes
		if length <= 0 || offset > section.Offset+section.Length || length > section.Offset+section.Length-offset {
			tb.Fatalf("block length=%d offset=%d outside section=%+v", length, offset, section)
		}
		out = append(out, typedColumnInt64AggregateTestBlockRange{offset: offset, length: length, columnSectionLength: section.Length, min: block.Granule.Min, max: block.Granule.Max, hasMinMax: block.Granule.HasMinMax})
		offset += length
	}
	if offset != section.Offset+section.Length {
		tb.Fatalf("column data consumed=%d section=%d", offset-section.Offset, section.Length)
	}
	return out
}

func corruptColumnAssetByteAtRelativeOffset(tb testing.TB, d *backenddb.DB, ref ColumnAssetRef, relativeOffset int64) {
	tb.Helper()
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		tb.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		tb.Fatalf("OpenFile asset: %v", err)
	}
	defer func() { _ = file.Close() }()
	absoluteOffset := ref.Offset + relativeOffset
	var one [1]byte
	if _, err := file.ReadAt(one[:], absoluteOffset); err != nil {
		tb.Fatalf("ReadAt corrupt byte: %v", err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one[:], absoluteOffset); err != nil {
		tb.Fatalf("WriteAt corrupt byte: %v", err)
	}
}

func writeColumnAssetBytesAtRelativeOffset(tb testing.TB, d *backenddb.DB, ref ColumnAssetRef, relativeOffset int64, data []byte) {
	tb.Helper()
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		tb.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		tb.Fatalf("OpenFile asset: %v", err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteAt(data, ref.Offset+relativeOffset); err != nil {
		tb.Fatalf("WriteAt asset bytes: %v", err)
	}
}

func setupTypedColumnInt64ScanCollection(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	return d, createTypedColumnInt64ScanCollection(tb, d)
}

func setupTypedColumnInt64RawScanCollection(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	return d, createTypedColumnInt64ScanCollectionWithFixedWidthEncoding(tb, d, ColumnFixedWidthEncodingLittleEndian)
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
	return createTypedColumnInt64ScanCollectionWithFixedWidthEncoding(tb, d, ColumnFixedWidthEncodingDefault)
}

func createTypedColumnInt64ScanCollectionWithFixedWidthEncoding(tb testing.TB, d *backenddb.DB, fixedWidthEncoding ColumnFixedWidthEncoding) *Collection {
	tb.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: fixedWidthEncoding},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset, Dictionary: true},
	}
	if fixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian {
		cfg.TypedColumnCompression = ColumnStoreTypedColumnCompressionNone
		cfg.TypedColumnSectionCompression = ColumnStoreTypedColumnCompressionNone
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

func typedColumnManifestPartRolesByGenerationForTest(tb testing.TB, d *backenddb.DB, col *Collection) map[uint64]ColumnManifestPartRole {
	tb.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil {
		tb.Fatal("missing column manifest")
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, col.Meta().Name)
	if err != nil {
		tb.Fatalf("loadCollectionCatalog: %v", err)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, catalog.rootID(collectionColumnManifestRootName(col.Meta().Name)))
	if err != nil {
		tb.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	roles := make(map[uint64]ColumnManifestPartRole)
	for _, record := range records {
		if !strings.HasPrefix(string(record.key), columnManifestPartRecordPrefix) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			tb.Fatalf("decodeColumnManifestPartRecord: %v", err)
		}
		if part.AssetRef.Kind != ColumnAssetKindTCS1PartImage || part.AssetRef.PartID != columnPhysicalRowAssetPartID {
			continue
		}
		roles[part.AssetRef.Generation] = part.PartRole
	}
	return roles
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
