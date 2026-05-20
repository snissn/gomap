package collections

import (
	"bytes"
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
	if got := plan.Diagnostics.UnsupportedPlanReason; got != ColumnQueryUnsupportedSerialPhysicalDisabledReason {
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

	tampered := cloneColumnManifestRecords(manifest.Records)
	tamperedCount := 0
	for i := range tampered {
		if bytes.HasPrefix(tampered[i].key, []byte(columnManifestPartRecordPrefix)) {
			tampered[i].value[len(tampered[i].value)-1] ^= 0x01
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
	if len(refs) != 1 {
		_ = d.Close()
		t.Fatalf("refs=%+v want one", refs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir, refs[0]
}
