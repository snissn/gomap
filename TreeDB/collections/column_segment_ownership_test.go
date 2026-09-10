package collections

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func insertColumnOwnedSegmentBatch(t *testing.T, col *Collection, prefix string) {
	t.Helper()
	ids, retained, columns := typedUpsertRecoveryBatch("owned", 2)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("%s%d", prefix, i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
	}
	if _, err := col.UpsertTypedBatch(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
}

func TestColumnSegmentOwnershipReuseSuffixAndReopen(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir, db, col := openTypedMinimaCollection(t)
	defer func() { _ = db.Close() }()
	insertColumnOwnedSegmentBatch(t, col, "a")
	first := columnManifestAllPartsForCollectionM12C(t, db, col)
	if len(first) < 2 {
		t.Fatalf("first parts=%d", len(first))
	}
	fileID := first[0].AssetRef.FileID
	var firstEnd int64
	for _, part := range first {
		firstEnd = max(firstEnd, part.AssetRef.Offset+part.AssetRef.Length)
	}
	prefixPath, err := columnAssetSegmentPath(db.ColumnAssetRootDir(), first[0].AssetRef)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := os.ReadFile(prefixPath)
	if err != nil {
		t.Fatal(err)
	}
	// A retained prefix stays readable across an append to the same inode.
	old := db.AcquireSnapshot()
	defer old.Close()
	insertColumnOwnedSegmentBatch(t, col, "b")
	appended, err := os.ReadFile(prefixPath)
	if err != nil || len(appended) < len(prefix) || !bytes.Equal(appended[:len(prefix)], prefix) {
		t.Fatal("append changed retained prefix")
	}
	all := columnManifestAllPartsForCollectionM12C(t, db, col)
	if len(all) <= len(first) {
		t.Fatalf("parts first=%d all=%d", len(first), len(all))
	}
	for _, part := range all {
		if part.AssetRef.FileID != fileID {
			t.Fatalf("ref=%+v did not reuse file %d", part.AssetRef, fileID)
		}
		if part.AssetRef.Generation > first[0].AssetRef.Generation && part.AssetRef.Offset < firstEnd {
			t.Fatal("append overlaps retained prefix")
		}
	}
	path, err := columnAssetSegmentPath(db.ColumnAssetRootDir(), first[0].AssetRef)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate bytes written before an interrupted publication, with no manifest ref.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("unacknowledged suffix")); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	assertProtected := func() {
		t.Helper()
		plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
		if err != nil || !plan.Complete || plan.Segments.Unknown != 0 || plan.Segments.Missing != 0 || plan.Segments.OutOfBoundsRefs != 0 {
			t.Fatalf("plan=%+v err=%v", plan, err)
		}
		for _, segment := range plan.SegmentEntries {
			if segment.FileID == fileID && (segment.ProtectedBytes != segment.Bytes || segment.UnknownBytes != 0 || segment.ReclaimableBytes != 0) {
				t.Fatalf("owned segment=%+v", segment)
			}
		}
	}
	assertProtected()
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	assertProtected()
	insertColumnOwnedSegmentBatch(t, col, "c")
	for _, part := range columnManifestAllPartsForCollectionM12C(t, db, col) {
		if part.AssetRef.FileID != fileID {
			t.Fatal("reopen lost segment ownership")
		}
	}
	for _, id := range []string{"a0", "b1", "c0"} {
		if doc, err := col.Get([]byte(id)); err != nil || doc == nil {
			t.Fatalf("Get %s: %s, %v", id, doc, err)
		}
	}
	assertProtected()
}

func TestColumnSegmentOwnershipAppendRejectsChangedFile(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	for _, change := range []string{"missing", "rebound", "short"} {
		t.Run(change, func(t *testing.T) {
			_, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			insertColumnOwnedSegmentBatch(t, col, "a")
			parts := columnManifestAllPartsForCollectionM12C(t, db, col)
			path, err := columnAssetSegmentPath(db.ColumnAssetRootDir(), parts[0].AssetRef)
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "missing":
				err = os.Remove(path)
			case "rebound":
				if err = os.Rename(path, path+".original"); err == nil {
					err = os.WriteFile(path, original, 0600)
				}
			case "short":
				err = os.Truncate(path, 1)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := db.StateToken()
			ids, retained, columns := typedUpsertRecoveryBatch("owned", 1)
			ids[0], retained[0] = []byte("new"), []byte(`{"id":"new"}`)
			if _, err := col.UpsertTypedBatch(ids, retained, columns); err == nil {
				t.Fatal("changed append target accepted")
			}
			after, _ := db.StateToken()
			if after != before {
				t.Fatal("failed append published a root")
			}
			got, readErr := os.ReadFile(path)
			switch change {
			case "missing":
				if !os.IsNotExist(readErr) {
					t.Fatalf("missing target was recreated: %v", readErr)
				}
			case "short":
				if readErr != nil || !bytes.Equal(got, original[:1]) {
					t.Fatal("short target was modified")
				}
			case "rebound":
				if readErr != nil || !bytes.Equal(got, original) {
					t.Fatal("rebound target was modified")
				}
			}
		})
	}
}

func TestColumnSegmentOwnershipRotation(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	insertColumnOwnedSegmentBatch(t, col, "a")
	first := columnManifestAllPartsForCollectionM12C(t, db, col)
	path, err := columnAssetSegmentPath(db.ColumnAssetRootDir(), first[0].AssetRef)
	if err != nil {
		t.Fatal(err)
	}
	// An unacknowledged physical suffix also counts against the append target.
	if err = os.Truncate(path, columnPhysicalAssetSegmentTargetBytes); err != nil {
		t.Fatal(err)
	}
	insertColumnOwnedSegmentBatch(t, col, "b")
	all := columnManifestAllPartsForCollectionM12C(t, db, col)
	for _, part := range all {
		if part.AssetRef.Generation > first[0].AssetRef.Generation && part.AssetRef.FileID == first[0].AssetRef.FileID {
			t.Fatal("full segment reused")
		}
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != columnPhysicalAssetSegmentTargetBytes {
		t.Fatalf("rotation modified old file: %v %v", info, err)
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{})
	if err != nil || !plan.Complete || plan.Segments.Unknown != 0 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
}

func TestColumnSegmentOwnershipNormalizesFinalLiveRefs(t *testing.T) {
	row := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "owned", Generation: 1, PartID: 1, FileID: 2, Offset: 0, Length: 32, Checksum: 1}
	typed := row
	typed.Kind, typed.PartID, typed.Offset = ColumnAssetKindTCS1TypedColumnPart, 2, 128
	record := func(ref ColumnAssetRef) columnManifestRecord {
		t.Helper()
		raw, err := encodeColumnManifestPartRecord(ColumnPreparedAsset{Ref: ref, Rows: 1, Bytes: ref.Length, PublishID: 1, GenerationID: ref.Generation, Reason: string(ColumnPublishOperationInsert)})
		if err != nil {
			t.Fatal(err)
		}
		return columnManifestRecord{key: columnManifestPartRecordKey(ref.Generation, ref.PartID), value: raw}
	}
	records := []columnManifestRecord{record(row), record(typed)}
	unmarked, err := normalizeColumnManifestSegmentOwnership(append([]columnManifestRecord(nil), records...), nil, 1, "owned")
	if err != nil || len(unmarked) != 2 {
		t.Fatalf("unmarked assets gained ownership: %v", err)
	}
	marked, err := normalizeColumnManifestSegmentOwnership(records, map[uint32]struct{}{2: {}}, 1, "owned")
	if err != nil {
		t.Fatal(err)
	}
	markerRecord := marked[len(marked)-1]
	marker, err := decodeColumnManifestSegmentOwnership(markerRecord.key, markerRecord.value)
	if err != nil || marker.Ref != typed || marker.Frontier != 160 || marker.Graph {
		t.Fatalf("marker=%+v err=%v", marker, err)
	}
	selector, err := marker.selector()
	if err != nil || selector.Kind != rootpublication.ResourceTypedColumnAsset {
		t.Fatalf("selector=%+v err=%v", selector, err)
	}
	lower, err := normalizeColumnManifestSegmentOwnership([]columnManifestRecord{record(row), markerRecord}, nil, 1, "owned")
	if err != nil {
		t.Fatal(err)
	}
	low, err := decodeColumnManifestSegmentOwnership(lower[1].key, lower[1].value)
	if err != nil || low.Ref != row || low.Frontier != 32 {
		t.Fatalf("retired maximum retained: %+v %v", low, err)
	}
	selector, err = low.selector()
	if err != nil || selector.Kind != rootpublication.ResourceColumnAsset {
		t.Fatalf("selector=%+v err=%v", selector, err)
	}
	empty, err := normalizeColumnManifestSegmentOwnership([]columnManifestRecord{lower[1]}, nil, 1, "owned")
	if err != nil || len(empty) != 0 {
		t.Fatalf("last-ref marker retained: %v %v", empty, err)
	}
	for _, bad := range [][]byte{markerRecord.value[:len(markerRecord.value)-1], append(bytes.Clone(markerRecord.value), 0)} {
		if _, err := decodeColumnManifestSegmentOwnership(markerRecord.key, bad); err == nil {
			t.Fatal("malformed marker accepted")
		}
	}
	if _, err := decodeColumnManifestSegmentOwnership(columnManifestSegmentOwnershipRecordKey(3), markerRecord.value); err == nil {
		t.Fatal("wrong file key accepted")
	}
	marker.Graph = true
	selector, err = marker.selector()
	if err != nil || selector.Kind != rootpublication.ResourceVectorGraphPack {
		t.Fatalf("graph witness selector=%+v err=%v", selector, err)
	}
}

func TestColumnSegmentOwnershipBindingRejectsForgedWitness(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	insertColumnOwnedSegmentBatch(t, col, "a")
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecarsAndBudget(context.Background(), columnManifestScanAllSidecars(), 0, 0)
	if closeView != nil {
		defer closeView()
	}
	if err != nil || len(view.SegmentOwnership) != 1 {
		t.Fatalf("markers=%v err=%v", view.SegmentOwnership, err)
	}
	input := columnAssetReachabilityInputFromSnapshotView(view, columnAssetReachabilityOptionsInternal{})
	release, err := col.bindColumnSegmentOwnership(context.Background(), view, &input)
	if err != nil {
		t.Fatal(err)
	}
	release()
	for _, change := range []string{"checksum", "frontier", "graph", "duplicate"} {
		t.Run(change, func(t *testing.T) {
			bad := view
			bad.SegmentOwnership = append([]columnManifestSegmentOwnership(nil), view.SegmentOwnership...)
			switch change {
			case "checksum":
				bad.SegmentOwnership[0].Ref.Checksum++
			case "frontier":
				bad.SegmentOwnership[0].Frontier++
			case "graph":
				bad.SegmentOwnership[0].Graph = !bad.SegmentOwnership[0].Graph
			case "duplicate":
				bad.SegmentOwnership = append(bad.SegmentOwnership, bad.SegmentOwnership[0])
			}
			release, err := col.bindColumnSegmentOwnership(context.Background(), bad, &input)
			if release != nil {
				release()
			}
			if err == nil {
				t.Fatal("forged ownership accepted")
			}
		})
	}
}
