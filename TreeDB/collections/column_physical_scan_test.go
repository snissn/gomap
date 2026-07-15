package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnManifestAssetRefsFromRecordsRejectsFutureGenerationM13C(t *testing.T) {
	oldAsset := columnManifestAssetRefFilterTestAssetM13C(1, 1, ColumnPublishOperationInsert)
	activeAsset := columnManifestAssetRefFilterTestAssetM13C(2, 1, ColumnPublishOperationUpdate)
	futureAsset := columnManifestAssetRefFilterTestAssetM13C(3, 1, ColumnPublishOperationDelete)
	oldRecord, err := encodeColumnManifestPartRecord(oldAsset)
	if err != nil {
		t.Fatalf("encode old part: %v", err)
	}
	activeRecord, err := encodeColumnManifestPartRecord(activeAsset)
	if err != nil {
		t.Fatalf("encode active part: %v", err)
	}
	futureRecord, err := encodeColumnManifestPartRecord(futureAsset)
	if err != nil {
		t.Fatalf("encode future part: %v", err)
	}
	records := []columnManifestRecord{
		{key: columnManifestPartRecordKey(oldAsset.Ref.Generation, oldAsset.Ref.PartID), value: oldRecord},
		{key: columnManifestPartRecordKey(activeAsset.Ref.Generation, activeAsset.Ref.PartID), value: activeRecord},
	}

	refs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, activeAsset.Ref.Generation, activeAsset.Ref.Namespace)
	if err != nil {
		t.Fatalf("columnManifestAssetRefsFromRecordsForScan reachable lineage: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("refs=%d want old base plus active delta", len(refs))
	}
	if refs[0].Ref.Generation != oldAsset.Ref.Generation || refs[0].Reason != ColumnPublishOperationInsert {
		t.Fatalf("unexpected base ref: %+v", refs[0])
	}
	if refs[1].Ref.Generation != activeAsset.Ref.Generation || refs[1].Reason != ColumnPublishOperationUpdate {
		t.Fatalf("unexpected active delta ref: %+v", refs[1])
	}
	if mutationParts != 1 {
		t.Fatalf("mutation parts=%d want one active update part", mutationParts)
	}

	records = append(records, columnManifestRecord{key: columnManifestPartRecordKey(futureAsset.Ref.Generation, futureAsset.Ref.PartID), value: futureRecord})
	if _, _, err := columnManifestAssetRefsFromRecordsForScan(records, activeAsset.Ref.Generation, activeAsset.Ref.Namespace); err == nil {
		t.Fatal("columnManifestAssetRefsFromRecordsForScan accepted future-generation part")
	}
}

func TestColumnManifestAssetRefsRejectPartIDKeyMismatchM13C(t *testing.T) {
	asset := columnManifestAssetRefFilterTestAssetM13C(2, 7, ColumnPublishOperationInsert)
	record, err := encodeColumnManifestPartRecord(asset)
	if err != nil {
		t.Fatalf("encode part: %v", err)
	}
	records := []columnManifestRecord{{
		key:   columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID+1),
		value: record,
	}}
	_, _, err = columnManifestAssetRefsFromRecordsForScan(records, asset.Ref.Generation, asset.Ref.Namespace)
	if err == nil || !strings.Contains(err.Error(), "key part_id") {
		t.Fatalf("columnManifestAssetRefsFromRecordsForScan err=%v want key part_id mismatch", err)
	}
}

func TestColumnManifestScanViewRejectsPreparedAssetByteMismatchM1634(t *testing.T) {
	_, _, manifest, asset := columnManifestPlannerOnePartFixtureM14A(t)
	tampered := cloneColumnManifestRecords(manifest.Records)
	replaced := false
	for i := range tampered {
		if bytes.Equal(tampered[i].key, columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID)) {
			badRecord := bytes.Clone(tampered[i].value)
			pos := columnManifestPartRecordBytesOffsetForScanTestM1634(t, badRecord)
			binary.BigEndian.PutUint64(badRecord[pos:], uint64(asset.Ref.Length+1))
			tampered[i].value = badRecord
			replaced = true
			break
		}
	}
	if !replaced {
		t.Fatal("manifest part record not found")
	}
	if _, _, _, err := decodeColumnManifestSnapshotViewForScan(tampered, asset.Ref.Namespace); err == nil || !strings.Contains(err.Error(), "does not match ref length") {
		t.Fatalf("decodeColumnManifestSnapshotViewForScan err=%v want byte/ref length mismatch", err)
	}
}

func TestColumnManifestScanLoaderRejectsChecksumMismatchM1634(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, _, manifest, _ := columnManifestPlannerOnePartFixtureM14A(t)
	active := manifest.Identity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &active
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1

	tampered := cloneColumnManifestRecords(manifest.Records)
	tamperedCount := 0
	for i := range tampered {
		if bytes.HasPrefix(tampered[i].key, columnManifestPartRecordPrefixBytes) {
			badRecord := bytes.Clone(tampered[i].value)
			bytesOffset := columnManifestPartRecordBytesOffsetForScanTestM1634(t, badRecord)
			publishIDOffset := bytesOffset + 8
			publishID := binary.BigEndian.Uint64(badRecord[publishIDOffset:])
			binary.BigEndian.PutUint64(badRecord[publishIDOffset:], publishID+1)
			tampered[i].value = badRecord
			tamperedCount++
			break
		}
	}
	if tamperedCount != 1 {
		t.Fatalf("tampered manifest part records=%d want 1", tamperedCount)
	}
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, manifest.Identity, tampered)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	_, _, _, _, _, err = loadColumnManifestSnapshotViewForScanFromRoot(snap, rootID, *cfg, manifest.Identity, "events", false, nil)
	if err == nil || !strings.Contains(err.Error(), "physical column scan manifest checksum") {
		t.Fatalf("loadColumnManifestSnapshotViewForScanFromRoot err=%v want checksum mismatch", err)
	}
}

func columnManifestPartRecordBytesOffsetForScanTestM1634(t testing.TB, raw []byte) int {
	t.Helper()
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		t.Fatalf("bad part magic=0x%08x", magic)
	}
	version := cur.u16()
	cur.skipStringBytes() // kind
	cur.skipStringBytes() // namespace
	_ = cur.u64()         // generation
	_ = cur.u64()         // part_id
	_ = cur.u64()         // file_id
	_ = cur.u64()         // offset
	_ = cur.u64()         // length
	_ = cur.u64()         // checksum
	if version >= columnManifestRecordVersionV2 {
		_ = cur.u64() // rows
	}
	if cur.err != nil {
		t.Fatalf("decode part prefix: %v", cur.err)
	}
	return cur.pos
}

func columnManifestAssetRefFilterTestAssetM13C(generation, partID uint64, reason ColumnPublishOperation) ColumnPreparedAsset {
	return ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  "events/column-assets",
			Generation: generation,
			PartID:     partID,
			FileID:     1,
			Offset:     int64((generation - 1) * 128),
			Length:     64,
			Checksum:   uint32(1000 + generation),
		},
		Bytes:        64,
		PublishID:    generation,
		GenerationID: generation,
		Reason:       string(reason),
	}
}

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

func TestColumnPhysicalSerialScannerUsesSnapshotCatalogM13A(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("first Insert: %v", err)
	}

	staleSnap := d.AcquireSnapshot()
	if staleSnap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	staleCatalog, err := loadCollectionCatalog(staleSnap, "events")
	if err != nil {
		_ = staleSnap.Close()
		t.Fatalf("load stale catalog: %v", err)
	}
	staleSystemRoot := snapshotSystemRoot(staleSnap)
	staleCommitSeq := snapshotCommitSeq(staleSnap)
	if err := staleSnap.Close(); err != nil {
		t.Fatalf("close stale snapshot: %v", err)
	}

	if _, err := col.Insert([]byte("e2"), []byte(`{"time_us":2,"kind":"post","did":"d2"}`)); err != nil {
		t.Fatalf("second Insert: %v", err)
	}
	col.catalogMu.Lock()
	col.catalog = staleCatalog
	col.catalogSystemRoot = staleSystemRoot
	col.catalogCommitSeq = staleCommitSeq
	col.catalogMu.Unlock()

	var rows []columnPhysicalScanRowForTest
	diag, err := col.scanColumnPhysicalRows(columnPhysicalScanRequest{
		ProjectedColumns: []string{"time_us"},
		Visitor: func(row columnPhysicalScanRowView) error {
			rows = append(rows, copyColumnPhysicalScanRowForTest(row))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows: %v", err)
	}
	if diag.ManifestGeneration != 2 || diag.AssetRefs != 2 || diag.RowsScanned != 2 {
		t.Fatalf("scan diagnostics=%+v want latest snapshot generation=2 with two rows", diag)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d want latest snapshot rows", len(rows))
	}
	assertColumnPhysicalScanRowM13A(t, rows[0], ColumnPublishOperationInsert, "e1", false, int64(1), "")
	assertColumnPhysicalScanRowM13A(t, rows[1], ColumnPublishOperationInsert, "e2", false, int64(2), "")
}

func TestColumnPhysicalSerialScannerUsesSelectedClosureWhenOtherAssetMissingM13A(t *testing.T) {
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
	var rows []columnPhysicalScanRowForTest
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{Visitor: func(row columnPhysicalScanRowView) error {
		rows = append(rows, copyColumnPhysicalScanRowForTest(row))
		return nil
	}})
	if err != nil || diag.RowsScanned != 1 || len(rows) != 1 {
		t.Fatalf("scan after missing non-selected asset rows=%+v diagnostics=%+v err=%v want selected valid closure", rows, diag, err)
	}
	assertColumnPhysicalScanRowM13A(t, rows[0], ColumnPublishOperationInsert, "e1", false, int64(1), "like")
}

func TestColumnPhysicalSerialScannerUsesSelectedClosureWhenOtherAssetCorruptM13A(t *testing.T) {
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
	var rows []columnPhysicalScanRowForTest
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{Visitor: func(row columnPhysicalScanRowView) error {
		rows = append(rows, copyColumnPhysicalScanRowForTest(row))
		return nil
	}})
	if err != nil || diag.RowsScanned != 1 || len(rows) != 1 {
		t.Fatalf("scan after corrupt non-selected asset rows=%+v diagnostics=%+v err=%v want selected valid closure", rows, diag, err)
	}
	assertColumnPhysicalScanRowM13A(t, rows[0], ColumnPublishOperationInsert, "e1", false, int64(1), "like")
}

func TestColumnPhysicalSerialScannerDoesNotEnableAutomaticPlannerRoutingM14B(t *testing.T) {
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
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery: %v", err)
	}
	if !plan.Supported || plan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("automatic physical routing should remain disabled without a forced physical label: %+v", plan)
	}
}

func TestColumnPhysicalScanSnapshotViewPinsManifestRefsM14B(t *testing.T) {
	reopened, closeFn := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 4)
	defer closeFn()

	view, closeView, err := reopened.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	defer closeView()
	if got, want := len(view.AssetRefs), 4; got != want {
		t.Fatalf("prepared refs=%d want %d", got, want)
	}
	preparedGeneration := view.Diagnostics.ManifestGeneration
	if _, err := reopened.InsertBatch([][]byte{[]byte("new")}, [][]byte{
		[]byte(`{"time_us":99,"kind":"kind_new","did":"did_new","payload":"after_view"}`),
	}); err != nil {
		t.Fatalf("InsertBatch after prepared view: %v", err)
	}
	if got := reopened.meta.Options.ColumnStore.ActiveManifest.Generation; got <= preparedGeneration {
		t.Fatalf("active generation=%d did not advance past prepared generation %d", got, preparedGeneration)
	}

	var scanned int
	diag, err := reopened.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ProjectedColumns: []string{"time_us", "kind"},
		Visitor: func(row columnPhysicalScanRowView) error {
			scanned++
			if string(row.ID) == "new" {
				t.Fatalf("prepared scan view observed row inserted after view preparation")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRowsInSnapshotView: %v", err)
	}
	if scanned != 4 || diag.RowsScanned != 4 || diag.ScheduledGranules != 4 || diag.AssetRefs != 4 {
		t.Fatalf("scan diagnostics=%+v scanned=%d want only the four prepared refs", diag, scanned)
	}
	if diag.ManifestGeneration != preparedGeneration {
		t.Fatalf("scan generation=%d want prepared generation %d", diag.ManifestGeneration, preparedGeneration)
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
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "other_events", normalized, projection, nil)
	if err == nil || !strings.Contains(err.Error(), "collection") {
		t.Fatalf("scan err=%v want collection mismatch", err)
	}
}

func TestColumnPhysicalSerialScannerRejectsManifestReasonOperationMismatchM13C(t *testing.T) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(t, 4)
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationUpdate,
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
	visited := false
	_, err = scanColumnPhysicalAssetRowsWithManifestOperation(encoded, ref, "events", normalized, projection, ColumnPublishOperationInsert, func(row columnPhysicalScanRowView) error {
		visited = true
		return nil
	})
	if !errors.Is(err, errColumnPhysicalAssetManifestOperationMismatch) {
		t.Fatalf("scan mismatched manifest operation err=%v want mismatch", err)
	}
	if visited {
		t.Fatal("scanner visited rows before rejecting manifest operation mismatch")
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
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "events", normalized, projection, nil)
	if err == nil || !strings.Contains(err.Error(), "legacy v1 column physical asset delete operation unsupported") {
		t.Fatalf("scan err=%v want legacy v1 delete unsupported", err)
	}
}

func TestColumnManifestAssetRefsForScanRetainsChecksumCoveredGenerationsM13A(t *testing.T) {
	stale := testColumnPublishPreparedAssetM10A()
	stale.Ref.Generation = 2
	stale.Ref.PartID = 1
	stale.GenerationID = stale.Ref.Generation
	stale.Reason = string(ColumnPublishOperationInsert)
	staleValue, err := encodeColumnManifestPartRecord(stale)
	if err != nil {
		t.Fatalf("encode stale part: %v", err)
	}
	active := testColumnPublishPreparedAssetM10A()
	active.Ref.Generation = 3
	active.Ref.PartID = 2
	active.GenerationID = active.Ref.Generation
	active.Reason = string(ColumnPublishOperationInsert)
	activeValue, err := encodeColumnManifestPartRecord(active)
	if err != nil {
		t.Fatalf("encode active part: %v", err)
	}

	refs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan([]columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: []byte("header ignored by ref extraction")},
		{key: columnManifestPartRecordKey(stale.Ref.Generation, stale.Ref.PartID), value: staleValue},
		{key: columnManifestPartRecordKey(active.Ref.Generation, active.Ref.PartID), value: activeValue},
	}, active.Ref.Generation, active.Ref.Namespace)
	if err != nil {
		t.Fatalf("columnManifestAssetRefsFromRecordsForScan: %v", err)
	}
	if mutationParts != 0 {
		t.Fatalf("mutation parts=%d want zero retained insert parts", mutationParts)
	}
	if len(refs) != 2 ||
		refs[0].Ref.Generation != stale.Ref.Generation || refs[0].Ref.PartID != stale.Ref.PartID ||
		refs[1].Ref.Generation != active.Ref.Generation || refs[1].Ref.PartID != active.Ref.PartID {
		t.Fatalf("refs=%+v want retained stale ref %+v and active ref %+v", refs, stale.Ref, active.Ref)
	}
}

func TestColumnManifestEncodeChecksumsRetainedPartRecordsM13A(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	retained := testColumnPublishPreparedAssetM10A()
	retained.Ref.Generation = 1
	retained.Ref.PartID = 1
	retained.GenerationID = retained.Ref.Generation
	retained.Reason = string(ColumnPublishOperationInsert)
	retainedValue, err := encodeColumnManifestPartRecord(retained)
	if err != nil {
		t.Fatalf("encode retained part: %v", err)
	}
	next := testColumnPublishPreparedAssetM10A()
	next.Ref.Generation = 2
	next.Ref.PartID = 2
	next.GenerationID = next.Ref.Generation
	next.Reason = string(ColumnPublishOperationUpdate)
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationUpdate,
		AppliedCommandLSN: 2,
		CurrentManifest:   &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1},
		CurrentManifestRecords: []columnManifestRecord{{
			key:   columnManifestPartRecordKey(retained.Ref.Generation, retained.Ref.PartID),
			value: retainedValue,
		}},
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{next},
			RowCount:           1,
			ColumnPayloadBytes: next.Bytes,
		},
	})
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	if len(manifest.Records) != 3 {
		t.Fatalf("manifest records=%d want header + retained + next", len(manifest.Records))
	}
	active, err := activeColumnManifestRecordsForScan(manifest.Records, manifest.Identity.Generation)
	if err != nil {
		t.Fatalf("activeColumnManifestRecordsForScan: %v", err)
	}
	checksum := checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       ColumnStoreConfig{SchemaHash: cfg.SchemaHash},
		Operation:         ColumnPublishOperationUpdate,
		AppliedCommandLSN: 2,
	}, manifest.Identity.Generation, active)
	if checksum != manifest.Identity.Checksum {
		t.Fatalf("checksum=%d want manifest identity checksum=%d", checksum, manifest.Identity.Checksum)
	}
	tampered := cloneColumnManifestRecords(active)
	tamperedCount := 0
	for i := range tampered {
		if bytes.HasPrefix(tampered[i].key, []byte(columnManifestPartRecordPrefix)) && bytes.Contains(tampered[i].value, []byte(retained.Reason)) {
			tampered[i].value[len(tampered[i].value)-1] ^= 0x01
			tamperedCount++
			break
		}
	}
	if tamperedCount != 1 {
		t.Fatalf("tampered retained manifest records=%d want 1", tamperedCount)
	}
	tamperedChecksum := checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       ColumnStoreConfig{SchemaHash: cfg.SchemaHash},
		Operation:         ColumnPublishOperationUpdate,
		AppliedCommandLSN: 2,
	}, manifest.Identity.Generation, tampered)
	if tamperedChecksum == manifest.Identity.Checksum {
		t.Fatal("tampered retained manifest part did not change checksum")
	}
}

func TestColumnPublishRejectsTamperedExistingManifestBeforeCarryM13A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.Ref.PartID = 1
	asset.GenerationID = 1
	asset.Reason = string(ColumnPublishOperationInsert)
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{asset},
			RowCount:           1,
			ColumnPayloadBytes: asset.Bytes,
		},
	})
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	active := manifest.Identity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &active
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1

	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, manifest.Identity, manifest.Records)
	col := &Collection{db: d}
	if _, err := col.loadColumnManifestRecordsForPublish(rootID, "events", *cfg); err != nil {
		t.Fatalf("load untampered manifest for publish: %v", err)
	}
	if _, err := col.loadColumnManifestRecordsForPublish(0, "events", *cfg); err == nil || !strings.Contains(err.Error(), "missing manifest root") {
		t.Fatalf("load active manifest with missing root err=%v want missing root rejection", err)
	}

	tampered := cloneColumnManifestRecords(manifest.Records)
	tamperedCount := 0
	for i := range tampered {
		if bytes.HasPrefix(tampered[i].key, []byte(columnManifestPartRecordPrefix)) {
			badRecord := bytes.Clone(tampered[i].value)
			bytesOffset := columnManifestPartRecordBytesOffsetForScanTestM1634(t, badRecord)
			publishIDOffset := bytesOffset + 8
			publishID := binary.BigEndian.Uint64(badRecord[publishIDOffset:])
			binary.BigEndian.PutUint64(badRecord[publishIDOffset:], publishID+1)
			tampered[i].value = badRecord
			tamperedCount++
			break
		}
	}
	if tamperedCount != 1 {
		t.Fatalf("tampered manifest part records=%d want 1", tamperedCount)
	}
	corruptRootID := publishColumnManifestRecordsForScanTestM13A(t, d, manifest.Identity, tampered)
	_, err = col.loadColumnManifestRecordsForPublish(corruptRootID, "events", *cfg)
	if err == nil || !strings.Contains(err.Error(), "column publish manifest checksum") {
		t.Fatalf("load tampered manifest for publish err=%v want checksum rejection", err)
	}
}

func TestColumnManifestPlannerCapabilitiesRejectPartIDKeyMismatchM14A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.Ref.PartID = 1
	asset.GenerationID = 1
	asset.Reason = string(ColumnPublishOperationInsert)
	input := ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{asset},
			RowCount:           1,
			ColumnPayloadBytes: asset.Bytes,
		},
	}
	manifest, err := encodeColumnManifestForWrite(input)
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}

	tampered := cloneColumnManifestRecords(manifest.Records)
	tamperedCount := 0
	for i := range tampered {
		if bytes.HasPrefix(tampered[i].key, columnManifestPartRecordPrefixBytes) {
			tampered[i].key = columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID+1)
			tamperedCount++
			break
		}
	}
	if tamperedCount != 1 {
		t.Fatalf("tampered manifest part keys=%d want 1", tamperedCount)
	}
	tamperedIdentity := manifest.Identity
	tamperedIdentity.Checksum = checksumColumnManifestRecords(input, tamperedIdentity.Generation, tampered)
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, tamperedIdentity, tampered)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	_, err = loadColumnManifestPlannerCapabilitiesForScan(snap, rootID, *cfg, tamperedIdentity, "events")
	if err == nil || !strings.Contains(err.Error(), "key part_id") {
		t.Fatalf("loadColumnManifestPlannerCapabilitiesForScan err=%v want key part_id mismatch", err)
	}
}

func TestColumnManifestPlannerCapabilitiesRejectOrphanAggregateMetadataM1634(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, input, _, asset := columnManifestPlannerOnePartFixtureM14A(t)
	metadata := asset
	metadata.Ref.Kind = ColumnAssetKindTCS1AggregateMetadata
	metadata.Ref.PartID = asset.Ref.PartID + 1
	metadata.Ref.Offset += metadata.Ref.Length
	metadata.Ref.Length = 256
	metadata.Bytes = metadata.Ref.Length
	metadata.Reason = "min_time_us"
	input.Prepared.Assets = []ColumnPreparedAsset{asset, metadata}
	manifest, err := encodeColumnManifestForWrite(input)
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, manifest.Identity, manifest.Records)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	_, err = loadColumnManifestPlannerCapabilitiesForScan(snap, rootID, *cfg, manifest.Identity, "events")
	if err == nil || !strings.Contains(err.Error(), "matching live part") {
		t.Fatalf("loadColumnManifestPlannerCapabilitiesForScan err=%v want matching live part failure", err)
	}
}

func TestColumnManifestSnapshotSidecarFilterStillValidatesSkippedRecordsM1634(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, input, _, asset := columnManifestPlannerOnePartFixtureM14A(t)
	dictionary := asset
	dictionary.Ref.Kind = ColumnAssetKindTCS1DictionaryCodes
	dictionary.Ref.PartID = asset.Ref.PartID + 1
	dictionary.Ref.Offset += dictionary.Ref.Length
	dictionary.Ref.Length = 256
	dictionary.Bytes = dictionary.Ref.Length
	dictionary.Reason = "kind"
	input.Prepared.Assets = []ColumnPreparedAsset{asset, dictionary}
	manifest, err := encodeColumnManifestForWrite(input)
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, manifest.Identity, manifest.Records)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	_, _, _, _, _, _, err = loadColumnManifestSnapshotViewForScanFromRootWithSidecars(
		snap,
		rootID,
		*cfg,
		manifest.Identity,
		"events",
		columnManifestScanSidecarFilter{Int64Values: true},
		false,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "matching live part") {
		t.Fatalf("loadColumnManifestSnapshotViewForScanFromRootWithSidecars err=%v want skipped sidecar validation failure", err)
	}
}

func TestColumnManifestPlannerCapabilitiesRejectActivePartCountMismatchM14A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, input, manifest, _ := columnManifestPlannerOnePartFixtureM14A(t)
	headerInput := input
	extra := input.Prepared.Assets[0]
	extra.Ref.PartID++
	headerInput.Prepared.Assets = append(append([]ColumnPreparedAsset(nil), input.Prepared.Assets...), extra)
	header, err := encodeColumnManifestHeaderRecord(headerInput, manifest.Identity.Generation)
	if err != nil {
		t.Fatalf("encode tampered header: %v", err)
	}
	tampered := cloneColumnManifestRecords(manifest.Records)
	replaced := false
	for i := range tampered {
		if bytes.Equal(tampered[i].key, columnManifestHeaderRecordKeyBytes) {
			tampered[i].value = header
			replaced = true
			break
		}
	}
	if !replaced {
		t.Fatal("manifest header record not found")
	}
	tamperedIdentity := manifest.Identity
	tamperedIdentity.Checksum = checksumColumnManifestRecords(headerInput, tamperedIdentity.Generation, tampered)
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, tamperedIdentity, tampered)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	_, err = loadColumnManifestPlannerCapabilitiesForScan(snap, rootID, *cfg, tamperedIdentity, "events")
	if err == nil || !strings.Contains(err.Error(), "invalid column manifest part count=1 want 2") {
		t.Fatalf("loadColumnManifestPlannerCapabilitiesForScan err=%v want active part-count mismatch", err)
	}
}

func TestColumnManifestScanRejectsHugeExpectedPartsWithoutPreallocM1634(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, input, manifest, _ := columnManifestPlannerOnePartFixtureM14A(t)
	tampered := cloneColumnManifestRecords(manifest.Records)
	replaced := false
	for i := range tampered {
		if bytes.Equal(tampered[i].key, columnManifestHeaderRecordKeyBytes) {
			header := bytes.Clone(tampered[i].value)
			binary.BigEndian.PutUint64(header[len(header)-8:], ^uint64(0))
			tampered[i].value = header
			replaced = true
			break
		}
	}
	if !replaced {
		t.Fatal("manifest header record not found")
	}
	tamperedIdentity := manifest.Identity
	tamperedIdentity.Checksum = checksumColumnManifestRecords(input, tamperedIdentity.Generation, tampered)
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, tamperedIdentity, tampered)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	_, err = loadColumnManifestPlannerCapabilitiesForScan(snap, rootID, *cfg, tamperedIdentity, "events")
	if err == nil || !strings.Contains(err.Error(), "invalid column manifest part count=1 want 18446744073709551615") {
		t.Fatalf("loadColumnManifestPlannerCapabilitiesForScan err=%v want huge part-count mismatch", err)
	}
	_, _, _, _, _, _, err = loadColumnManifestSnapshotViewForScanFromRootWithSidecars(
		snap,
		rootID,
		*cfg,
		tamperedIdentity,
		"events",
		columnManifestScanAllSidecars(),
		false,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid column manifest part count=1 want 18446744073709551615") {
		t.Fatalf("loadColumnManifestSnapshotViewForScanFromRootWithSidecars err=%v want huge active part-count mismatch", err)
	}
}

func TestColumnManifestPlannerCapabilitiesRejectChecksumMismatchM14A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg, _, manifest, asset := columnManifestPlannerOnePartFixtureM14A(t)
	tampered := cloneColumnManifestRecords(manifest.Records)
	tamperedAsset := asset
	tamperedAsset.Ref.Checksum++
	tamperedValue, err := encodeColumnManifestPartRecord(tamperedAsset)
	if err != nil {
		t.Fatalf("encode tampered part: %v", err)
	}
	tamperedCount := 0
	for i := range tampered {
		if bytes.HasPrefix(tampered[i].key, columnManifestPartRecordPrefixBytes) {
			tampered[i].value = tamperedValue
			tamperedCount++
			break
		}
	}
	if tamperedCount != 1 {
		t.Fatalf("tampered part records=%d want 1", tamperedCount)
	}
	rootID := publishColumnManifestRecordsForScanTestM13A(t, d, manifest.Identity, tampered)
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	_, err = loadColumnManifestPlannerCapabilitiesForScan(snap, rootID, *cfg, manifest.Identity, "events")
	if err == nil || !strings.Contains(err.Error(), "physical column planner manifest checksum") {
		t.Fatalf("loadColumnManifestPlannerCapabilitiesForScan err=%v want checksum mismatch", err)
	}
}

func TestColumnManifestPlannerCapabilitiesRejectHeaderOperationM14A(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	header, err := decodeColumnManifestHeaderRecordForScan(mustEncodeColumnManifestHeaderRecordForScanTestM14A(t, ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperation("rewrite"),
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			RowCount: 1,
		},
	}))
	if err != nil {
		t.Fatalf("decodeColumnManifestHeaderRecordForScan: %v", err)
	}

	err = validateColumnManifestHeaderRecordForScan(header, *cfg, ColumnManifestIdentity{
		Generation: 1,
	}, "events")
	if err == nil || !strings.Contains(err.Error(), "unsupported column manifest header operation") {
		t.Fatalf("validateColumnManifestHeaderRecordForScan err=%v want unsupported operation", err)
	}
}

func columnManifestPlannerOnePartFixtureM14A(t testing.TB) (*ColumnStoreConfig, ColumnPublishManifestEncodeInput, ColumnPublishManifestEncodeResult, ColumnPreparedAsset) {
	t.Helper()
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.Ref.PartID = 1
	asset.GenerationID = 1
	asset.Reason = string(ColumnPublishOperationInsert)
	input := ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{asset},
			RowCount:           1,
			ColumnPayloadBytes: asset.Bytes,
		},
	}
	manifest, err := encodeColumnManifestForWrite(input)
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	return cfg, input, manifest, asset
}

func mustEncodeColumnManifestHeaderRecordForScanTestM14A(t testing.TB, input ColumnPublishManifestEncodeInput) []byte {
	t.Helper()
	header, err := encodeColumnManifestHeaderRecord(input, 1)
	if err != nil {
		t.Fatalf("encodeColumnManifestHeaderRecord: %v", err)
	}
	return header
}

func publishColumnManifestRecordsForScanTestM13A(t testing.TB, d *backenddb.DB, identity ColumnManifestIdentity, records []columnManifestRecord) uint64 {
	t.Helper()
	iter := columnManifestRootRecordIterator(encodeColumnManifestIdentityRecordArray(identity), records)
	defer func() { _ = iter.Close() }()
	rootID, err := d.PublishOrderedRootIterator(0, iter)
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if rootID == 0 {
		t.Fatal("PublishOrderedRootIterator returned root 0")
	}
	return rootID
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
		summary, err := scanColumnPhysicalAssetRows(encoded, ref, "events", normalized, projection, func(row columnPhysicalScanRowView) error {
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
		if row.Values[1].Type != ColumnStoreValueString || row.Values[1].Null || columnPhysicalScanStringForTest(row.Values[1]) != kind {
			t.Fatalf("kind value=%+v want %q", row.Values[1], kind)
		}
		if row.Values[1].StringBytes != nil && row.Values[1].String != "" {
			t.Fatalf("kind value kept stale String=%q beside StringBytes=%q", row.Values[1].String, row.Values[1].StringBytes)
		}
	}
}

func columnPhysicalScanStringForTest(value columnDeclaredValue) string {
	if value.StringBytes != nil {
		return string(value.StringBytes)
	}
	return value.String
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
	physicalRefs := columnManifestPhysicalAssetRefsForTestM1634(refs)
	if len(physicalRefs) != 1 {
		_ = d.Close()
		t.Fatalf("refs=%+v want one", refs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir, physicalRefs[0]
}
