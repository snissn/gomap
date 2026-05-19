package collections

import (
	"errors"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnPhysicalSerialScannerReadsReopenedAssetsM13A(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)

	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"ignored"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","payload":"ignored"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	insertLSN := d.State().AppliedCommandLSN
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)

	var rows []columnPhysicalScanRowForTest
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{
		ProjectedColumns: []string{"time_us", "kind"},
		Visitor: func(row columnPhysicalScanRowView) error {
			rows = append(rows, copyColumnPhysicalScanRowForTest(row))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows: %v", err)
	}
	if diag.ManifestGeneration != 1 || diag.RecoveryManifestGeneration != 1 || diag.AppliedCommandLSN != insertLSN {
		t.Fatalf("unexpected scan manifest diagnostics: %+v want generation=1 appliedLSN=%d", diag, insertLSN)
	}
	if diag.AssetRefs != 1 || diag.DecodedBlocks != 1 || diag.ScheduledGranules != 1 || diag.RowsScanned != 2 || diag.ProjectedColumns != 2 {
		t.Fatalf("unexpected scan diagnostics: %+v", diag)
	}
	if diag.RowMaterializations != 0 {
		t.Fatalf("row materializations=%d want 0 for declared-column scan", diag.RowMaterializations)
	}
	if diag.PhysicalBytesScanned <= 0 {
		t.Fatalf("physical bytes scanned=%d want positive", diag.PhysicalBytesScanned)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d want 2", len(rows))
	}
	assertColumnPhysicalScanRowM13A(t, rows[0], ColumnPublishOperationInsert, "e1", false, int64(1), "like")
	assertColumnPhysicalScanRowM13A(t, rows[1], ColumnPublishOperationInsert, "e2", false, int64(2), "post")
}

func TestColumnPhysicalSerialScannerExposesMutationRowsM13A(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)

	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":3,"kind":"like","did":"d1"}`), true, nil
	}); err != nil {
		_ = d.Close()
		t.Fatalf("Update: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e2")}); err != nil || deleted != 1 {
		_ = d.Close()
		t.Fatalf("DeleteBatch deleted=%d err=%v, want one delete", deleted, err)
	}
	deleteLSN := d.State().AppliedCommandLSN
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)

	var rows []columnPhysicalScanRowForTest
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{
		ProjectedColumns: []string{"time_us"},
		Visitor: func(row columnPhysicalScanRowView) error {
			rows = append(rows, copyColumnPhysicalScanRowForTest(row))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows: %v", err)
	}
	if diag.ManifestGeneration != 3 || diag.AssetRefs != 3 || diag.DecodedBlocks != 3 || diag.RowsScanned != 4 || diag.DeletedRows != 1 {
		t.Fatalf("unexpected scan diagnostics: %+v", diag)
	}
	if diag.AppliedCommandLSN != deleteLSN {
		t.Fatalf("applied LSN=%d want delete LSN %d", diag.AppliedCommandLSN, deleteLSN)
	}
	if len(rows) != 4 {
		t.Fatalf("rows=%d want insert/update/delete rows", len(rows))
	}
	assertColumnPhysicalScanRowM13A(t, rows[0], ColumnPublishOperationInsert, "e1", false, int64(1), "")
	assertColumnPhysicalScanRowM13A(t, rows[1], ColumnPublishOperationInsert, "e2", false, int64(2), "")
	assertColumnPhysicalScanRowM13A(t, rows[2], ColumnPublishOperationUpdate, "e1", false, int64(3), "")
	assertColumnPhysicalScanRowM13A(t, rows[3], ColumnPublishOperationDelete, "e2", true, 0, "")
}

func TestColumnPhysicalSerialScannerFailsClosedMissingAssetM13A(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := os.Remove(assetPath); err != nil {
		t.Fatalf("Remove asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	_, err = reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scan missing asset err=%v want os.ErrNotExist", err)
	}
}

func TestColumnPhysicalSerialScannerFailsClosedCorruptAssetM13A(t *testing.T) {
	dir, ref := prepareColumnPhysicalScannerCorruptionFixtureM13A(t)
	assetPath, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile asset: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, ref.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt asset: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt asset: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	_, err = reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("scan corrupt asset err=%v want checksum failure", err)
	}
}

func TestColumnPhysicalSerialScannerDoesNotEnablePlannerRoutingM13A(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	plan, err := col.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "q1",
		ProjectedColumns: []string{"time_us"},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery: %v", err)
	}
	if plan.Supported {
		t.Fatalf("forced serial plan supported before M14 routing: %+v", plan)
	}
	if got := plan.Diagnostics.UnsupportedPlanReason; got != "physical column scanner is not implemented yet" {
		t.Fatalf("unsupported reason=%q want physical scanner disabled", got)
	}
}

func TestColumnPhysicalSerialScannerRejectsInvalidProjectionM13A(t *testing.T) {
	cfg := *testColumnStoreConfig(nil)
	for _, tc := range []struct {
		name       string
		projection []string
		want       string
	}{
		{name: "empty column", projection: []string{""}, want: "empty column"},
		{name: "duplicate", projection: []string{"time_us", "time_us"}, want: "duplicate projected column"},
		{name: "undeclared", projection: []string{"missing"}, want: "undeclared column"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newColumnPhysicalScanProjection(cfg, tc.projection)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("projection err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestColumnPhysicalAssetSerialScanRejectsWrongCollectionM13A(t *testing.T) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(t, 4)
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 1,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"time_us"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "other_events", *normalized, projection, nil)
	if err == nil || !strings.Contains(err.Error(), "collection") {
		t.Fatalf("scan err=%v want collection mismatch", err)
	}
}

func TestColumnPhysicalAssetSerialScanRejectsV1DeleteOperationM13A(t *testing.T) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(t, 1)
	encoded := encodeColumnPhysicalAssetV1ForTest(t, columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationDelete,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 1,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"time_us"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "events", *normalized, projection, nil)
	if err == nil || !strings.Contains(err.Error(), "legacy v1 column physical asset delete operation unsupported") {
		t.Fatalf("scan err=%v want legacy v1 delete unsupported", err)
	}
}

func TestColumnPhysicalAssetSerialScanNumericProjectionHasZeroAllocsM13A(t *testing.T) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(t, 1024)
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 1,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"time_us"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	var scanErr error
	var sum int64
	allocs := testing.AllocsPerRun(100, func() {
		if scanErr != nil {
			return
		}
		summary, err := scanColumnPhysicalAssetRows(encoded, ref, "events", *normalized, projection, func(row columnPhysicalScanRowView) error {
			sum += row.Values[0].Int64
			return nil
		})
		if err != nil {
			scanErr = err
			return
		}
		if summary.rows != len(rows) {
			scanErr = errors.New("unexpected scanned row count")
		}
	})
	if scanErr != nil {
		t.Fatalf("scanColumnPhysicalAssetRows: %v", scanErr)
	}
	if allocs != 0 {
		t.Fatalf("allocs/run=%v want zero for numeric physical asset scan", allocs)
	}
	if sum == 0 {
		t.Fatal("scan sum stayed zero")
	}
}

type columnPhysicalScanRowForTest struct {
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	ID                string
	Deleted           bool
	Values            []columnDeclaredValue
}

func copyColumnPhysicalScanRowForTest(row columnPhysicalScanRowView) columnPhysicalScanRowForTest {
	values := append([]columnDeclaredValue(nil), row.Values...)
	return columnPhysicalScanRowForTest{
		Generation:        row.Generation,
		PartID:            row.PartID,
		AppliedCommandLSN: row.AppliedCommandLSN,
		Operation:         row.Operation,
		ID:                string(row.ID),
		Deleted:           row.Deleted,
		Values:            values,
	}
}

func assertColumnPhysicalScanRowM13A(t testing.TB, row columnPhysicalScanRowForTest, operation ColumnPublishOperation, id string, deleted bool, timeUS int64, kind string) {
	t.Helper()
	if row.Operation != operation || row.ID != id || row.Deleted != deleted {
		t.Fatalf("row=%+v want operation=%s id=%q deleted=%v", row, operation, id, deleted)
	}
	if deleted {
		if len(row.Values) != 0 {
			t.Fatalf("deleted row values=%+v want none", row.Values)
		}
		return
	}
	if len(row.Values) == 0 {
		t.Fatalf("row values empty for %+v", row)
	}
	if row.Values[0].Type != ColumnStoreValueInt64 || row.Values[0].Null || row.Values[0].Int64 != timeUS {
		t.Fatalf("time_us value=%+v want %d", row.Values[0], timeUS)
	}
	if kind != "" {
		if len(row.Values) < 2 {
			t.Fatalf("kind projection missing from values=%+v", row.Values)
		}
		if row.Values[1].Type != ColumnStoreValueString || row.Values[1].Null || row.Values[1].String != kind {
			t.Fatalf("kind value=%+v want %q", row.Values[1], kind)
		}
	}
}

func prepareColumnPhysicalScannerCorruptionFixtureM13A(t *testing.T) (string, ColumnAssetRef) {
	t.Helper()
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(refs) != 1 {
		_ = d.Close()
		t.Fatalf("refs=%+v want one", refs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir, refs[0]
}
