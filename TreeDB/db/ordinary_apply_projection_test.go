package db

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOrdinaryExactClosureDeletesRangesAndInlineReplacement(t *testing.T) {
	db, _, old, fresh := setupExactRewritePairWithValueLogOptions(t, ValueLogOptions{})
	scans := 0
	db.testScanCandidateExternalReferencesHook = func() { scans++ }
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("a"), old); err != nil {
		t.Fatal(err)
	} // net zero, shared count remains two
	if err := b.Delete([]byte("missing")); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	closeNoErr(t, b)
	if snapshotCandidateTracker(db).counts[old.FileID] != 2 {
		t.Fatal("shared net-zero replacement lost multiplicity")
	}
	b = db.NewBatch().(*Batch)
	if err := b.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := b.SetPointer([]byte("b"), fresh); err != nil {
		t.Fatal(err)
	}
	if err := b.SetPointer([]byte("b"), old); err != nil {
		t.Fatal(err)
	} // normalized last put
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	closeNoErr(t, b)
	if got, err := db.Get([]byte("a")); err != nil || got != nil {
		t.Fatalf("range deleted a=%q %v", got, err)
	}
	if snapshotCandidateTracker(db).counts[old.FileID] != 1 {
		t.Fatal("range/put multiplicity mismatch")
	}
	b = db.NewBatch().(*Batch)
	if err := b.Set([]byte("b"), []byte("inline")); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	closeNoErr(t, b)
	if got, err := db.Get([]byte("b")); err != nil || !bytes.Equal(got, []byte("inline")) {
		t.Fatalf("inline=%q %v", got, err)
	}
	db.testScanCandidateExternalReferencesHook = nil
	if scans != 0 {
		t.Fatalf("supported destructive publications used %d full scans", scans)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	if len(snapshotCandidateTracker(db).counts) != 0 {
		t.Fatalf("old logical references retained: %v", snapshotCandidateTracker(db).counts)
	}
}

func TestOrdinaryExactClosurePrimaryDescriptorAliasFallback(t *testing.T) {
	db, _, old, fresh := setupExactRewritePair(t)
	beforeRoot := db.State().RootPageID
	if _, err := db.PublishSystemRootIterator(mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(beforeRoot)).NewIterator(nil, nil)); err != nil {
		t.Fatal(err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	scans := 0
	db.testScanCandidateExternalReferencesHook = func() { scans++ }
	if err := applyExactClosureFixtureReplacement(db, old, fresh, true); err != nil {
		t.Fatal(err)
	}
	db.testScanCandidateExternalReferencesHook = nil
	if scans != 1 {
		t.Fatalf("aliased predecessor fallback scans=%d want 1", scans)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	counts := snapshotCandidateTracker(db).counts
	if counts[old.FileID] != 3 || counts[fresh.FileID] != 1 {
		t.Fatalf("aliased old root lost logical references: %v", counts)
	}
}

func TestOrdinaryGroupIncompleteProvenanceUsesFullScan(t *testing.T) {
	db, _, _, fresh := setupExactRewritePair(t)
	group, err := db.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	for i, key := range []string{"a", "b"} {
		b := db.NewPhysicalBatch().(*Batch)
		if err := b.SetPointer([]byte(key), fresh); err != nil {
			t.Fatal(err)
		}
		if err := b.SetRootPublicationBuildGroup(group, i == 1); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := b.Write(); err != nil {
				t.Fatal(err)
			}
			// Simulate a preceding contribution whose Apply provenance was lost.
			// The complete final chunk must not promote that incomplete chain.
			group.projectionComplete = false
		} else {
			before := db.durableRootCandidateFullScans.Load()
			if err := b.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if db.durableRootCandidateFullScans.Load() != before+1 {
				t.Fatal("final chunk certified an incomplete group")
			}
		}
		closeNoErr(t, b)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
}

func TestOrdinaryGroupDeltaOverflowAbortsBeforeMerge(t *testing.T) {
	fileID := page.ValueLogFileID(1)
	for _, positive := range []bool{false, true} {
		group := &RootPublicationBuildGroup{vlogRefDelta: newValueLogRefDelta()}
		src := newValueLogRefDelta()
		if positive {
			group.vlogRefDelta.addPositive(fileID, math.MaxInt64)
			src.addPositive(fileID, 1)
		} else {
			group.vlogRefDelta.addChange(fileID, math.MinInt64)
			src.addChange(fileID, -1)
		}
		if err := group.mergeValueLogRefDeltaLocked(src); err == nil {
			t.Fatal("overflow accepted")
		}
		if !positive && group.vlogRefDelta.changeFor(fileID) != math.MinInt64 {
			t.Fatal("overflow partially mutated aggregate")
		}
		releaseValueLogRefDelta(src)
		releaseValueLogRefDelta(group.vlogRefDelta)
	}
}

func TestOrdinaryGroupDropsUnreachableIntermediateRaw(t *testing.T) {
	db, writer, old, fresh := setupExactRewritePair(t)
	if err := writer.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	group, err := db.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	beforeScans := db.durableRootCandidateFullScans.Load()
	var intermediateID uint32
	for i, ptr := range []page.ValuePtr{fresh, old} {
		b := db.NewPhysicalBatch().(*Batch)
		if err := b.SetPointer([]byte("a"), ptr); err != nil {
			t.Fatal(err)
		}
		if err := b.SetRootPublicationBuildGroup(group, i == 1); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := b.Write(); err != nil {
				t.Fatal(err)
			}
			segments, err := writer.CurrentLeafPageLogSegmentsSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			if len(segments) != 1 {
				t.Fatalf("current segments=%v", segments)
			}
			intermediateID = segments[0].FileID
			if err := writer.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
		} else if err := b.WriteSync(); err != nil {
			t.Fatal(err)
		}
		closeNoErr(t, b)
	}
	if db.durableRootCandidateFullScans.Load() != beforeScans {
		t.Fatal("complete net-zero group used full candidate scan")
	}
	for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
		if uint32(descriptor.Generation) == intermediateID && (descriptor.Kind == rootpublication.ResourceOuterLeafLog || descriptor.Kind == rootpublication.ResourceValueLog) {
			t.Fatalf("unreachable private raw segment %d retained", intermediateID)
		}
	}
	assertCandidateTrackerMatchesFullScan(t, db)
}

func TestOrdinaryGroupFinalFailureRejectsReuseAndFreshGroupRetries(t *testing.T) {
	db, _, _, fresh := setupExactRewritePair(t)
	before := snapshotCandidateTracker(db)
	beforeSeq := db.currentCommitSeq()
	beforeResources := db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors()
	group, err := db.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	b := db.NewPhysicalBatch().(*Batch)
	defer b.Close()
	if err := b.SetPointer([]byte("a"), fresh); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	db.testFailDurableRootVisibleInstall.Store(true)
	err = b.WriteSync()
	db.testFailDurableRootVisibleInstall.Store(false)
	if !errors.Is(err, errTestDurableRootVisibleInstallFailpoint) {
		t.Fatalf("final failure=%v", err)
	}
	if !group.closed || !group.failed || group.leafCapture.capture.builder != nil {
		t.Fatal("failed final group retained build ownership")
	}
	if err := b.WriteSync(); err == nil {
		t.Fatal("failed group allowed reuse of consumed capture")
	}
	if db.currentCommitSeq() != beforeSeq || !reflect.DeepEqual(snapshotCandidateTracker(db), before) || !reflect.DeepEqual(db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors(), beforeResources) {
		t.Fatal("failed group changed selected root or closure")
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	retry, err := db.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Close()
	next := db.NewPhysicalBatch().(*Batch)
	defer next.Close()
	if err := next.SetPointer([]byte("a"), fresh); err != nil {
		t.Fatal(err)
	}
	if err := next.SetRootPublicationBuildGroup(retry, true); err != nil {
		t.Fatal(err)
	}
	if err := next.WriteSync(); err != nil {
		t.Fatal(err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
}
