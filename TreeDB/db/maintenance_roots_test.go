package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const maintenanceTestCollectionRootKey = "collections/root/users/primary"

func TestMaintenanceRoots_NoCollectionsIncludesUserAndSystem(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}
	if _, err := d.PublishSystemRootIterator(mustFrozenSystemMemtable(t, "sys/a", "sv").NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system root: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	roots, err := maintenanceRootsForSnapshot(snap)
	if err != nil {
		t.Fatalf("maintenanceRootsForSnapshot: %v", err)
	}
	requireMaintenanceRoot(t, roots, maintenanceRootUser, snap.state.RootPageID)
	requireMaintenanceRoot(t, roots, maintenanceRootSystem, snap.state.SystemRootPageID)
	requireMaintenanceRootCount(t, roots, maintenanceRootCollection, 0)
}

func TestMaintenanceRoots_IncludesPointerBackedCollectionDescriptor(t *testing.T) {
	opts := vacuumPointerDescriptorOptions(t.TempDir())
	d := openVacuumPointerDescriptorFixture(t, opts)
	defer func() { _ = d.Close() }()

	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	descriptorValue, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(maintenanceTestCollectionRootKey))
	if err != nil {
		t.Fatalf("read descriptor: %v", err)
	}
	rootID := binary.BigEndian.Uint64(descriptorValue)
	roots, err := maintenanceRootsForSnapshot(snap)
	if err != nil {
		t.Fatalf("maintenanceRootsForSnapshot: %v", err)
	}
	root := requireMaintenanceRoot(t, roots, maintenanceRootCollection, rootID)
	if !bytes.Equal(root.descriptorKey, []byte(maintenanceTestCollectionRootKey)) {
		t.Fatalf("descriptor key=%q want %q", string(root.descriptorKey), maintenanceTestCollectionRootKey)
	}
}

func TestMaintenanceRoots_DeduplicatesCollectionRootDescriptors(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemMemtable(t, "doc/u1", "document").NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			"collections/root/users/by-email", encodeMaintenanceRootID(rootIDs[0]),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish roots: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}

	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	roots, err := maintenanceRootsForSnapshot(snap)
	if err != nil {
		t.Fatalf("maintenanceRootsForSnapshot: %v", err)
	}
	requireMaintenanceRoot(t, roots, maintenanceRootCollection, rootIDs[0])
	requireMaintenanceRootCount(t, roots, maintenanceRootCollection, 1)
}

func TestMaintenanceRoots_PreservesDistinctRolesForSameRootID(t *testing.T) {
	var acc maintenanceRootAccumulator
	rootID := uint64(42)
	acc.add(maintenanceRootUser, rootID, nil)
	acc.add(maintenanceRootCollection, rootID, []byte(maintenanceTestCollectionRootKey))
	acc.add(maintenanceRootCollection, rootID, []byte("collections/root/users/alias"))
	acc.add(maintenanceRootSystem, rootID, nil)

	requireMaintenanceRoot(t, acc.roots, maintenanceRootUser, rootID)
	collectionRoot := requireMaintenanceRoot(t, acc.roots, maintenanceRootCollection, rootID)
	if !bytes.Equal(collectionRoot.descriptorKey, []byte(maintenanceTestCollectionRootKey)) {
		t.Fatalf("collection descriptor key=%q want %q", collectionRoot.descriptorKey, maintenanceTestCollectionRootKey)
	}
	requireMaintenanceRoot(t, acc.roots, maintenanceRootSystem, rootID)
	requireMaintenanceRootCount(t, acc.roots, maintenanceRootUser, 1)
	requireMaintenanceRootCount(t, acc.roots, maintenanceRootCollection, 1)
	requireMaintenanceRootCount(t, acc.roots, maintenanceRootSystem, 1)
}

func TestMaintenanceRootIDsDedupesByRootID(t *testing.T) {
	roots := []maintenanceRoot{
		{kind: maintenanceRootUser, rootID: 7},
		{kind: maintenanceRootCollection, rootID: 7, descriptorKey: []byte(maintenanceTestCollectionRootKey)},
		{kind: maintenanceRootSystem, rootID: 8},
		{kind: maintenanceRootCollection, rootID: 8, descriptorKey: []byte("collections/root/users/alias")},
		{kind: maintenanceRootCollection, rootID: 0},
	}
	got := maintenanceRootIDs(roots)
	want := []uint64{7, 8}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("maintenanceRootIDs=%v want %v", got, want)
	}
}

func TestMaintenanceRoots_IncludesCollectionOverlayRootDescriptors(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{
		{BaseRoot: 0, Iter: mustFrozenSystemMemtable(t, "doc/u1", "base").NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
		{BaseRoot: 0, Iter: mustFrozenSystemMemtable(t, "doc/u1", "overlay-1").NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
		{BaseRoot: 0, Iter: mustFrozenSystemMemtable(t, "doc/u2", "overlay-2").NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			"collections/root-overlay/users/primary", encodeCollectionRootDescriptorRootIDs([]uint64{rootIDs[2], rootIDs[1]}),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish roots: %v", err)
	}
	if len(rootIDs) != 3 {
		t.Fatalf("rootIDs=%d want 3", len(rootIDs))
	}

	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	roots, err := maintenanceRootsForSnapshot(snap)
	if err != nil {
		t.Fatalf("maintenanceRootsForSnapshot: %v", err)
	}
	requireMaintenanceRoot(t, roots, maintenanceRootCollection, rootIDs[0])
	overlayNewest := requireMaintenanceRoot(t, roots, maintenanceRootCollection, rootIDs[2])
	overlayOlder := requireMaintenanceRoot(t, roots, maintenanceRootCollection, rootIDs[1])
	if want := []byte("collections/root-overlay/users/primary"); !bytes.Equal(overlayNewest.descriptorKey, want) || !bytes.Equal(overlayOlder.descriptorKey, want) {
		t.Fatalf("overlay descriptor keys newest=%q older=%q want %q", overlayNewest.descriptorKey, overlayOlder.descriptorKey, want)
	}
	requireMaintenanceRootCount(t, roots, maintenanceRootCollection, 3)
}

func TestPublishSystemRootIterator_RejectsMalformedCollectionDescriptor(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	beforeSeq := d.currentCommitSeq()
	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, []byte("bad")).NewIterator(nil, nil)); err == nil {
		t.Fatal("published malformed collection descriptor")
	}

	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	if snap.state.CommitSeq != beforeSeq {
		t.Fatalf("commit sequence advanced after rejected descriptor: got=%d want=%d", snap.state.CommitSeq, beforeSeq)
	}
	if _, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(maintenanceTestCollectionRootKey)); err == nil {
		t.Fatal("rejected malformed collection descriptor became visible")
	}
}

func TestValueLogGC_KeepsSegmentReferencedOnlyByCollectionRoot(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	unreferenced := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("unreferenced|"), 64)
	})[0]
	referenced := appendPointersInNewSegment(t, dir, 0, 2, 20_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 3, 30_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-head|"), 64)
	})
	referencedID := referenced.FileID
	unreferencedPath := valueLogSegmentPath(t, dir, unreferenced.FileID)
	referencedPath := valueLogSegmentPath(t, dir, referencedID)

	publishCollectionPointerRoot(t, d, maintenanceTestCollectionRootKey, referenced)

	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete an unreferenced segment, stats=%+v", stats)
	}
	if _, err := os.Stat(unreferencedPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected unreferenced segment to be removed, err=%v", err)
	}
	if _, err := os.Stat(referencedPath); err != nil {
		t.Fatalf("expected collection-referenced segment %d to remain: %v", referencedID, err)
	}
}

func TestValueLogGC_KeepsSegmentMadeReachableBySystemDescriptorOnly(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	unreferenced := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("unreferenced|"), 64)
	})[0]
	referenced := appendPointersInNewSegment(t, dir, 0, 2, 20_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("descriptor-live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 3, 30_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-head|"), 64)
	})
	unreferencedPath := valueLogSegmentPath(t, dir, unreferenced.FileID)
	referencedPath := valueLogSegmentPath(t, dir, referenced.FileID)

	collectionRoot, err := d.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "doc/p", referenced).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish unattached collection root: %v", err)
	}
	if _, err := d.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}
	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRoot)).NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system descriptor: %v", err)
	}

	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete an unreferenced segment, stats=%+v", stats)
	}
	if _, err := os.Stat(unreferencedPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected unreferenced segment to be removed, err=%v", err)
	}
	if _, err := os.Stat(referencedPath); err != nil {
		t.Fatalf("expected descriptor-referenced segment %d to remain: %v", referenced.FileID, err)
	}
}

func TestValueLogRefTracker_PreservesFastPathForUnchangedCollectionDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 35_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("unchanged-descriptor-live|"), 64)
	})[0]
	collectionRoot := publishCollectionPointerRoot(t, d, maintenanceTestCollectionRootKey, referenced)
	if _, err := d.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}

	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t,
		maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRoot),
		"sys/unchanged", "value",
	).NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish unchanged collection descriptor: %v", err)
	}
	seq := d.currentCommitSeq()
	incRefs, ok := d.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatal("expected unchanged descriptor publish to preserve value-log ref tracker")
	}
	if _, ok := incRefs[referenced.FileID]; !ok {
		t.Fatalf("expected incremental refs to include descriptor-referenced segment %d", referenced.FileID)
	}
	fullCounts, fullSeq, err := d.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
}

func TestValueLogRefTracker_PreservesFastPathForCollectionDescriptorRepresentationChange(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 45_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("representation-change-live|"), 64)
	})[0]
	collectionRoot, err := d.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "doc/p", referenced).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish collection root: %v", err)
	}
	descriptorPtr := appendPointersInNewSegment(t, dir, 0, 2, 46_000, 1, func(int) []byte {
		return encodeMaintenanceRootID(collectionRoot)
	})[0]
	if _, err := d.PublishSystemRootIterator(mustFrozenSystemPointerMemtable(t, maintenanceTestCollectionRootKey, descriptorPtr).NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish pointer-backed system descriptor: %v", err)
	}
	if _, err := d.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}

	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t,
		maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRoot),
		"sys/changed", "value",
	).NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish inline system descriptor: %v", err)
	}
	seq := d.currentCommitSeq()
	incRefs, ok := d.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatal("expected descriptor representation change to preserve value-log ref tracker")
	}
	fullCounts, fullSeq, err := d.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
	if _, ok := incRefs[referenced.FileID]; !ok {
		t.Fatalf("expected collection-referenced segment %d to remain tracked", referenced.FileID)
	}
	if _, ok := incRefs[descriptorPtr.FileID]; ok {
		t.Fatalf("descriptor pointer segment %d remained tracked after inline rewrite", descriptorPtr.FileID)
	}
}

func TestValueLogGC_KeepsSegmentMadeReachableByDeltaGroupSystemDescriptorOnly(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	unreferenced := appendPointersInNewSegment(t, dir, 0, 1, 80_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("delta-group-unreferenced|"), 64)
	})[0]
	referenced := appendPointersInNewSegment(t, dir, 0, 2, 90_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("delta-group-descriptor-live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 3, 100_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-head|"), 64)
	})
	unreferencedPath := valueLogSegmentPath(t, dir, unreferenced.FileID)
	referencedPath := valueLogSegmentPath(t, dir, referenced.FileID)

	collectionRoot, err := d.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "doc/p", referenced).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish unattached collection root: %v", err)
	}
	if _, err := d.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}
	if _, _, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder(nil, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 0 {
			return nil, fmt.Errorf("rootIDs=%d want 0", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRoot)).NewIterator(nil, nil), nil
	}); err != nil {
		t.Fatalf("publish system descriptor via delta group builder: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); ok {
		t.Fatal("expected delta group descriptor publish to invalidate value-log ref tracker")
	}

	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete an unreferenced segment, stats=%+v", stats)
	}
	if _, err := os.Stat(unreferencedPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected unreferenced segment to be removed, err=%v", err)
	}
	if _, err := os.Stat(referencedPath); err != nil {
		t.Fatalf("expected delta-group descriptor-referenced segment %d to remain: %v", referenced.FileID, err)
	}
}

func TestValueLogGC_RemovesSegmentAfterSystemDescriptorRemoval(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 40_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("removed-descriptor-live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 2, 50_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-head|"), 64)
	})
	referencedPath := valueLogSegmentPath(t, dir, referenced.FileID)

	collectionRoot := publishCollectionPointerRoot(t, d, maintenanceTestCollectionRootKey, referenced)
	refs, err := d.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := refs[referenced.FileID]; !ok {
		t.Fatalf("expected primed refs to include descriptor-referenced segment %d", referenced.FileID)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}
	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t, "sys/after", encodeMaintenanceRootID(collectionRoot)).NewIterator(nil, nil)); err != nil {
		t.Fatalf("remove collection descriptor: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); ok {
		t.Fatal("expected descriptor removal to invalidate value-log ref tracker")
	}

	advancePastRetainedDurableSlotForTest(t, d)
	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete removed-descriptor segment, stats=%+v", stats)
	}
	if _, err := os.Stat(referencedPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected removed-descriptor segment to be deleted, err=%v", err)
	}
}

func TestValueLogGC_RemovesSegmentAfterGroupedSystemDescriptorRemoval(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 60_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("grouped-removed-descriptor-live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 2, 70_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-head|"), 64)
	})
	referencedPath := valueLogSegmentPath(t, dir, referenced.FileID)

	collectionRoot := publishCollectionPointerRoot(t, d, maintenanceTestCollectionRootKey, referenced)
	refs, err := d.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := refs[referenced.FileID]; !ok {
		t.Fatalf("expected primed refs to include descriptor-referenced segment %d", referenced.FileID)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}
	if _, _, err := d.PublishOrderedRootGroup(mustFrozenRawMemtable(t, "sys/after", encodeMaintenanceRootID(collectionRoot)).NewIterator(nil, nil), nil); err != nil {
		t.Fatalf("remove collection descriptor via grouped system publish: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); ok {
		t.Fatal("expected grouped descriptor removal to invalidate value-log ref tracker")
	}

	advancePastRetainedDurableSlotForTest(t, d)
	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete grouped removed-descriptor segment, stats=%+v", stats)
	}
	if _, err := os.Stat(referencedPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected grouped removed-descriptor segment to be deleted, err=%v", err)
	}
}

func TestValueLogGC_RemovesSegmentAfterDeltaGroupSystemDescriptorRemoval(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 110_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("delta-group-removed-descriptor-live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 2, 120_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-head|"), 64)
	})
	referencedPath := valueLogSegmentPath(t, dir, referenced.FileID)

	collectionRoot := publishCollectionPointerRoot(t, d, maintenanceTestCollectionRootKey, referenced)
	refs, err := d.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if _, ok := refs[referenced.FileID]; !ok {
		t.Fatalf("expected primed refs to include descriptor-referenced segment %d", referenced.FileID)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); !ok {
		t.Fatal("expected primed value-log ref tracker")
	}
	if _, _, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder(nil, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 0 {
			return nil, fmt.Errorf("rootIDs=%d want 0", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, "sys/after", encodeMaintenanceRootID(collectionRoot)).NewIterator(nil, nil), nil
	}); err != nil {
		t.Fatalf("remove collection descriptor via delta group builder: %v", err)
	}
	if _, ok := d.valueLogRefTracker.referencedSet(d.currentCommitSeq()); ok {
		t.Fatal("expected delta group descriptor removal to invalidate value-log ref tracker")
	}

	advancePastRetainedDurableSlotForTest(t, d)
	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete delta group removed-descriptor segment, stats=%+v", stats)
	}
	if _, err := os.Stat(referencedPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected delta group removed-descriptor segment to be deleted, err=%v", err)
	}
}

func TestValueLogRewritePlanningCountsCollectionRootPointers(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-live|"), 64)
	})[0]
	publishCollectionPointerRoot(t, d, maintenanceTestCollectionRootKey, referenced)

	liveByID, err := d.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesBySegment: %v", err)
	}
	if got := liveByID[referenced.FileID]; got <= 0 {
		t.Fatalf("live bytes for collection-referenced file %d = %d, want > 0", referenced.FileID, got)
	}

	liveByChunk, err := d.estimateValueLogLiveBytesByChunk(context.Background(), 16<<20)
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesByChunk: %v", err)
	}
	found := false
	for key, bytesLive := range liveByChunk {
		if key.fileID == referenced.FileID && bytesLive > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("collection-referenced file %d missing from chunk live-byte estimate: %+v", referenced.FileID, liveByChunk)
	}
}

func TestMaintenanceRootScansDeduplicateRoleAliasesByRootID(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	referenced := appendPointersInNewSegment(t, dir, 0, 1, 130_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("role-alias-live|"), 64)
	})[0]
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	batch := d.NewBatch()
	ptrBatch, ok := batch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch %T", batch)
	}
	if err := ptrBatch.SetPointer([]byte("doc/p"), referenced); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}
	aliasedUserRoot := d.State().RootPageID
	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t,
		maintenanceTestCollectionRootKey, encodeMaintenanceRootID(aliasedUserRoot),
	).NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish collection alias descriptor: %v", err)
	}

	counts, _, err := d.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if got := counts[referenced.FileID]; got != 1 {
		t.Fatalf("ref count for file %d = %d, want 1", referenced.FileID, got)
	}

	recordLen, err := d.valueLogRecordLengthForRewrite(referenced)
	if err != nil {
		t.Fatalf("valueLogRecordLengthForRewrite: %v", err)
	}
	liveByID, err := d.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesBySegment: %v", err)
	}
	if got, want := liveByID[referenced.FileID], int64(recordLen); got != want {
		t.Fatalf("segment live bytes for file %d = %d, want %d", referenced.FileID, got, want)
	}

	chunkBytes := int64(16 << 20)
	chunkOffset, err := valueLogChunkOffsetForPtr(referenced, chunkBytes)
	if err != nil {
		t.Fatalf("valueLogChunkOffsetForPtr: %v", err)
	}
	liveByChunk, err := d.estimateValueLogLiveBytesByChunk(context.Background(), chunkBytes)
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesByChunk: %v", err)
	}
	chunkKey := valueLogChunkKey{fileID: referenced.FileID, chunkOffset: chunkOffset}
	if got, want := liveByChunk[chunkKey], int64(recordLen); got != want {
		t.Fatalf("chunk live bytes for %+v = %d, want %d", chunkKey, got, want)
	}
}

func TestLeafGenerationLiveStatsCountsCollectionLeafRefRoot(t *testing.T) {
	d, _ := openLeafGenerationGCTestDB(t)

	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemMemtable(t, "doc/u1", "document").NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
	requireLeafLogRootChildren(t, d, rootIDs[0])

	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	if snap.state.LeafGenerations == nil {
		t.Fatal("expected leaf-generation view")
	}

	stats, err := d.scanLeafGenerationLiveStats(context.Background(), snap)
	if err != nil {
		t.Fatalf("scanLeafGenerationLiveStats: %v", err)
	}
	livePages := 0
	for _, totals := range stats.Generations {
		livePages += totals.LivePages
	}
	if livePages == 0 {
		t.Fatalf("collection leaf-ref root was not counted in live stats: %+v", stats.Generations)
	}
}

func mustFrozenRawMemtable(tb testing.TB, kvs ...any) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	for i := 0; i+1 < len(kvs); i += 2 {
		key, ok := kvs[i].(string)
		if !ok {
			tb.Fatalf("key %d has type %T, want string", i, kvs[i])
		}
		var value []byte
		switch v := kvs[i+1].(type) {
		case string:
			value = []byte(v)
		case []byte:
			value = v
		default:
			tb.Fatalf("value %d has type %T, want string or []byte", i+1, kvs[i+1])
		}
		mt.Set([]byte(key), value)
	}
	mt.Freeze()
	return mt
}

func encodeMaintenanceRootID(rootID uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, rootID)
	return out
}

func publishCollectionPointerRoot(tb testing.TB, d *DB, descriptorKey string, ptr page.ValuePtr) uint64 {
	tb.Helper()
	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(tb, "doc/p", ptr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenRawMemtable(tb, descriptorKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		tb.Fatalf("publish collection pointer root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		tb.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}
	return rootIDs[0]
}

func requireMaintenanceRoot(tb testing.TB, roots []maintenanceRoot, kind maintenanceRootKind, rootID uint64) maintenanceRoot {
	tb.Helper()
	for _, root := range roots {
		if root.kind == kind && root.rootID == rootID {
			return root
		}
	}
	tb.Fatalf("missing maintenance root kind=%d rootID=%d in %+v", kind, rootID, roots)
	return maintenanceRoot{}
}

func requireMaintenanceRootCount(tb testing.TB, roots []maintenanceRoot, kind maintenanceRootKind, want int) {
	tb.Helper()
	got := 0
	for _, root := range roots {
		if root.kind == kind {
			got++
		}
	}
	if got != want {
		tb.Fatalf("maintenance root count kind=%d got=%d want=%d roots=%+v", kind, got, want, roots)
	}
}

func valueLogSegmentPath(tb testing.TB, dir string, fileID uint32) string {
	tb.Helper()
	lane, seq := valuelog.DecodeFileID(fileID)
	return filepath.Join(dir, "value_vlog", fmt.Sprintf("value-l%d-%06d.log", lane, seq))
}
