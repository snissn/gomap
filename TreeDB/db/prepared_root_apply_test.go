package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPreparedRootDeltaPlanSummaryFromBatch(t *testing.T) {
	delta := batch.New(nil, 1<<20)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte("b"), []byte("value-b")); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if err := delta.Delete([]byte("a")); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if err := delta.SetPointer([]byte("c"), page.ValuePtr{
		FileID: page.ValueLogFileID(3),
		Offset: 17,
		Length: 41,
	}); err != nil {
		t.Fatalf("set pointer c: %v", err)
	}

	summary := preparedRootDeltaPlanSummaryFromBatch(delta, true)
	if summary.entries != 3 {
		t.Fatalf("entries=%d want 3", summary.entries)
	}
	if summary.tombstones != 1 {
		t.Fatalf("tombstones=%d want 1", summary.tombstones)
	}
	if summary.keyBytes != 3 {
		t.Fatalf("key bytes=%d want 3", summary.keyBytes)
	}
	wantValueBytes := uint64(len("value-b")) + uint64(page.ValuePtrSize)
	if summary.valueBytes != wantValueBytes {
		t.Fatalf("value bytes=%d want %d", summary.valueBytes, wantValueBytes)
	}
	if summary.pointerValues != 1 {
		t.Fatalf("pointer values=%d want 1", summary.pointerValues)
	}
	if !bytes.Equal(summary.firstKey, []byte("a")) || !bytes.Equal(summary.lastKey, []byte("c")) {
		t.Fatalf("key span=%q..%q want a..c", summary.firstKey, summary.lastKey)
	}
	if summary.checksum == 0 || summary.checksum == preparedRootPlanChecksumOffset {
		t.Fatalf("checksum=%d looks uninitialized", summary.checksum)
	}
	noSnapshotSummary := preparedRootDeltaPlanSummaryFromBatch(delta, false)
	if noSnapshotSummary.firstKey != nil || noSnapshotSummary.lastKey != nil {
		t.Fatalf("non-hook summary retained key span=%q..%q", noSnapshotSummary.firstKey, noSnapshotSummary.lastKey)
	}
	if noSnapshotSummary.checksum != 0 {
		t.Fatalf("non-hook checksum=%d want 0", noSnapshotSummary.checksum)
	}
}

func TestPreparedRootDeltaPlanSummaryChecksumDistinguishesPointerAndInline(t *testing.T) {
	ptr := page.ValuePtr{
		FileID: page.ValueLogFileID(3),
		Offset: 17,
		Length: 41,
	}
	ptrDelta := batch.New(nil, 1<<20)
	defer func() { _ = ptrDelta.Close() }()
	if err := ptrDelta.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}

	inlineValue := make([]byte, 24)
	binary.LittleEndian.PutUint64(inlineValue[0:8], uint64(ptr.FileID))
	binary.LittleEndian.PutUint64(inlineValue[8:16], ptr.Offset)
	binary.LittleEndian.PutUint64(inlineValue[16:24], uint64(ptr.Length))
	inlineDelta := batch.New(nil, 1<<20)
	defer func() { _ = inlineDelta.Close() }()
	if err := inlineDelta.Set([]byte("k"), inlineValue); err != nil {
		t.Fatalf("Set inline: %v", err)
	}

	ptrSummary := preparedRootDeltaPlanSummaryFromBatch(ptrDelta, true)
	inlineSummary := preparedRootDeltaPlanSummaryFromBatch(inlineDelta, true)
	if ptrSummary.checksum == inlineSummary.checksum {
		t.Fatalf("pointer and inline checksum both %d", ptrSummary.checksum)
	}
}

func TestPreparedRootApplyStatsCountsPreparedZeroRoot(t *testing.T) {
	group := preparedRootApplyGroup{}
	group.appendApply(preparedRootApply{
		identity: preparedRootIdentity{kind: preparedRootIdentityData},
		plan: preparedRootDeltaPlanSummary{
			entries:    1,
			keyBytes:   3,
			valueBytes: 5,
		},
		state: preparedRootApplyStatePlanned,
	})
	group.markPrepared(0, 0, 1)
	group.markInstalled()

	var stats preparedRootApplyStats
	stats.observeGroup(&group)
	if stats.groups != 1 || stats.roots != 1 || stats.installed != 1 || stats.abandoned != 0 {
		t.Fatalf("stats groups=%d roots=%d installed=%d abandoned=%d want 1,1,1,0", stats.groups, stats.roots, stats.installed, stats.abandoned)
	}
	if stats.entries != 1 || stats.keyBytes != 3 || stats.valueBytes != 5 {
		t.Fatalf("stats entries/key/value=%d/%d/%d want 1/3/5", stats.entries, stats.keyBytes, stats.valueBytes)
	}
}

func TestPreparedRootApplyRecordsLaterSuccessBeforeEarlierApplyError(t *testing.T) {
	sentinel := errors.New("root apply failed")
	group := preparedRootApplyGroup{}
	group.appendApply(preparedRootApply{
		identity: preparedRootIdentity{kind: preparedRootIdentityData, ordinal: 0},
		plan:     preparedRootDeltaPlanSummary{entries: 1},
		state:    preparedRootApplyStatePlanned,
	})
	group.appendApply(preparedRootApply{
		identity: preparedRootIdentity{kind: preparedRootIdentityData, ordinal: 1},
		plan:     preparedRootDeltaPlanSummary{entries: 2},
		state:    preparedRootApplyStatePlanned,
	})
	rootIDs := make([]uint64, 2)
	var pendingRetired []uint64
	var phaseStats orderedRootDeltaGroupPublishPhaseStats
	rootsObserved := 0

	err := recordOrderedRootDeltaBatchGroupApplyResults(
		&group,
		rootIDs,
		[]orderedRootDeltaBatchGroupApplyResult{
			{idx: 0, err: sentinel, attempted: true},
			{idx: 1, rootID: 42, pendingRetiredPages: []uint64{7}, attempted: true},
			{idx: 2},
		},
		&pendingRetired,
		nil,
		&phaseStats,
		&rootsObserved,
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error=%v want sentinel", err)
	}
	if rootIDs[0] != 0 || rootIDs[1] != 42 {
		t.Fatalf("root IDs=%v want [0 42]", rootIDs)
	}
	if rootsObserved != 1 || phaseStats.rootApplyCalls != 1 {
		t.Fatalf("roots observed=%d root apply calls=%d want 1/1", rootsObserved, phaseStats.rootApplyCalls)
	}
	if len(pendingRetired) != 1 || pendingRetired[0] != 7 {
		t.Fatalf("pending retired=%v want [7]", pendingRetired)
	}
	if failed := group.applyAt(0); failed == nil || failed.prepared || failed.state != preparedRootApplyStatePlanned {
		t.Fatalf("failed apply=%+v want unprepared planned", failed)
	}
	later := group.applyAt(1)
	if later == nil || !later.prepared || later.preparedRoot != 42 {
		t.Fatalf("later apply=%+v want prepared root 42", later)
	}
	group.markAbandoned()
	var stats preparedRootApplyStats
	stats.observeGroup(&group)
	if stats.roots != 1 || stats.abandoned != 1 || stats.entries != 2 {
		t.Fatalf("stats roots=%d abandoned=%d entries=%d want 1/1/2", stats.roots, stats.abandoned, stats.entries)
	}
}

func TestPreparedRootSetSystemRootSupersedesLatestActiveSystemApply(t *testing.T) {
	first := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := first.Set([]byte("sys/a"), []byte("1")); err != nil {
		t.Fatalf("first set: %v", err)
	}
	defer func() { _ = first.Close() }()
	second := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := second.Set([]byte("sys/b"), []byte("2")); err != nil {
		t.Fatalf("second set: %v", err)
	}
	defer func() { _ = second.Close() }()
	third := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := third.Set([]byte("sys/c"), []byte("3")); err != nil {
		t.Fatalf("third set: %v", err)
	}
	defer func() { _ = third.Close() }()

	group := preparedRootApplyGroup{
		baseSystemRootID: 7,
		state:            preparedRootApplyStatePlanned,
	}
	firstIdx := group.setSystemRoot(10, first, false)
	group.markPrepared(firstIdx, 100)
	secondIdx := group.setSystemRoot(20, second, false)
	group.markPrepared(secondIdx, 200)
	thirdIdx := group.setSystemRoot(30, third, false)

	if firstIdx == secondIdx || secondIdx == thirdIdx || firstIdx == thirdIdx {
		t.Fatalf("system indexes should be distinct, got %d/%d/%d", firstIdx, secondIdx, thirdIdx)
	}
	if got := group.baseSystemRootID; got != 30 {
		t.Fatalf("group base system root=%d want latest 30", got)
	}
	if firstApply := group.applyAt(firstIdx); firstApply == nil || firstApply.state != preparedRootApplyStateAbandoned {
		t.Fatalf("first system apply=%+v want abandoned", firstApply)
	}
	if secondApply := group.applyAt(secondIdx); secondApply == nil || secondApply.state != preparedRootApplyStateAbandoned {
		t.Fatalf("second system apply=%+v want abandoned", secondApply)
	}
	latest := group.applyAt(thirdIdx)
	if latest == nil {
		t.Fatal("missing latest system apply")
	}
	if latest.prepared {
		t.Fatalf("latest system apply is already prepared: %+v", latest)
	}
	if latest.baseRootID != 30 || latest.state != preparedRootApplyStatePlanned {
		t.Fatalf("latest system apply=%+v want base 30 planned", latest)
	}
}

func TestPreparedRootPrepareNsRecordedWithoutPreparedRoots(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	db.observeOrderedRootDeltaGroupPreparedRootApply(123, preparedRootApplyStats{})
	after := db.Stats()
	beforePrepareNs := installGuardStatUint(t, before, "treedb.publish.ordered_root_delta_group.prepared_root.prepare_ns_total")
	afterPrepareNs := installGuardStatUint(t, after, "treedb.publish.ordered_root_delta_group.prepared_root.prepare_ns_total")
	if afterPrepareNs-beforePrepareNs != 123 {
		t.Fatalf("prepare ns delta=%d want 123", afterPrepareNs-beforePrepareNs)
	}
	if got := installGuardStatUint(t, after, "treedb.publish.ordered_root_delta_group.prepared_root.groups_total"); got != installGuardStatUint(t, before, "treedb.publish.ordered_root_delta_group.prepared_root.groups_total") {
		t.Fatalf("prepared groups changed: before=%s after=%d", before["treedb.publish.ordered_root_delta_group.prepared_root.groups_total"], got)
	}
}

func TestOrderedRootDeltaBatchGroupPreparedRootMetadataRecordsInstall(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("missing state before ordered root group publish")
	}

	deltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	var captured []preparedRootApplyGroup
	db.testPreparedRootApplyHook = func(group preparedRootApplyGroup) {
		captured = append(captured, group)
	}
	newSystemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      baseRoot,
		Delta:         delta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	db.testPreparedRootApplyHook = nil
	if err != nil {
		t.Fatalf("publish ordered root group: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("root IDs=%v want one nonzero root", rootIDs)
	}
	if newSystemRoot == 0 {
		t.Fatal("new system root is zero")
	}
	if len(captured) != 1 {
		t.Fatalf("captured groups=%d want 1", len(captured))
	}

	group := captured[0]
	if group.state != preparedRootApplyStateInstalled {
		t.Fatalf("group state=%v want installed", group.state)
	}
	if group.baseUserRootID != before.RootPageID {
		t.Fatalf("base user root=%d want %d", group.baseUserRootID, before.RootPageID)
	}
	if group.baseSystemRootID != before.SystemRootPageID {
		t.Fatalf("base system root=%d want %d", group.baseSystemRootID, before.SystemRootPageID)
	}
	if group.applyLen() != 2 {
		t.Fatalf("applies=%d want data+system", group.applyLen())
	}

	data := *group.applyAt(0)
	if data.identity.kind != preparedRootIdentityData || data.identity.ordinal != 0 {
		t.Fatalf("data identity=%#v", data.identity)
	}
	if data.baseRootID != baseRoot {
		t.Fatalf("data base root=%d want %d", data.baseRootID, baseRoot)
	}
	if data.preparedRoot != rootIDs[0] {
		t.Fatalf("data prepared root=%d want %d", data.preparedRoot, rootIDs[0])
	}
	if data.outputID == 0 {
		t.Fatal("data prepared output ID is zero")
	}
	if data.storage != OrderedRootStoragePagerLeaves {
		t.Fatalf("data storage=%d want pager leaves", data.storage)
	}
	if data.state != preparedRootApplyStateInstalled {
		t.Fatalf("data state=%v want installed", data.state)
	}
	if data.plan.entries != 1 || data.plan.tombstones != 0 {
		t.Fatalf("data plan entries/tombstones=%d/%d want 1/0", data.plan.entries, data.plan.tombstones)
	}
	if !bytes.Equal(data.plan.firstKey, []byte("root/b")) || !bytes.Equal(data.plan.lastKey, []byte("root/b")) {
		t.Fatalf("data key span=%q..%q", data.plan.firstKey, data.plan.lastKey)
	}

	system := *group.applyAt(1)
	if system.identity.kind != preparedRootIdentitySystem {
		t.Fatalf("system identity=%#v", system.identity)
	}
	if system.baseRootID != before.SystemRootPageID {
		t.Fatalf("system base root=%d want %d", system.baseRootID, before.SystemRootPageID)
	}
	if system.preparedRoot != newSystemRoot {
		t.Fatalf("system prepared root=%d want %d", system.preparedRoot, newSystemRoot)
	}
	if system.outputID == 0 {
		t.Fatal("system prepared output ID is zero")
	}
	if system.outputID == data.outputID {
		t.Fatalf("system/data prepared output IDs both %d, want distinct owners", system.outputID)
	}
	if system.state != preparedRootApplyStateInstalled {
		t.Fatalf("system state=%v want installed", system.state)
	}
	if system.plan.entries == 0 {
		t.Fatal("system plan entries=0 want descriptor delta")
	}

	stats := db.Stats()
	if got := installGuardStatUint(t, stats, "treedb.publish.ordered_root_delta_group.prepared_root.groups_total"); got != 1 {
		t.Fatalf("prepared groups=%d want 1", got)
	}
	if got := installGuardStatUint(t, stats, "treedb.publish.ordered_root_delta_group.prepared_root.roots_total"); got != 2 {
		t.Fatalf("prepared roots=%d want 2", got)
	}
	if got := installGuardStatUint(t, stats, "treedb.publish.ordered_root_delta_group.prepared_root.installed_total"); got != 2 {
		t.Fatalf("prepared installed=%d want 2", got)
	}
	if got := installGuardStatUint(t, stats, "treedb.publish.ordered_root_delta_group.prepared_root.abandoned_total"); got != 0 {
		t.Fatalf("prepared abandoned=%d want 0", got)
	}
}

func TestOrderedRootDeltaBatchGroupPreparedRootMetadataRecordsOptimisticBuilderError(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}

	seedDeltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	seedIter := seedDeltaTable.NewIterator(nil, nil)
	seedDelta, err := OrderedRootDeltaBatchFromIterator(seedIter)
	_ = seedIter.Close()
	if err != nil {
		t.Fatalf("seed OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = seedDelta.Close() }()
	_, seedRootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      baseRoot,
		Delta:         seedDelta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("seed publish ordered root group: %v", err)
	}
	if len(seedRootIDs) != 1 || seedRootIDs[0] == 0 {
		t.Fatalf("seed root IDs=%v want one nonzero root", seedRootIDs)
	}
	beforeStats := db.Stats()

	deltaTable := mustFrozenSystemMemtable(t, "root/c", "vc")
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	sentinel := errors.New("injected system builder failure")
	var captured []preparedRootApplyGroup
	db.testPreparedRootApplyHook = func(group preparedRootApplyGroup) {
		captured = append(captured, group)
	}
	_, _, err = db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      seedRootIDs[0],
		Delta:         delta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return nil, sentinel
	})
	db.testPreparedRootApplyHook = nil
	if !errors.Is(err, sentinel) {
		t.Fatalf("publish err=%v want %v", err, sentinel)
	}
	if len(captured) != 1 {
		t.Fatalf("captured groups=%d want 1", len(captured))
	}
	group := captured[0]
	if group.state != preparedRootApplyStateAbandoned {
		t.Fatalf("group state=%v want abandoned", group.state)
	}
	if group.applyLen() != 1 {
		t.Fatalf("applies=%d want data-only group", group.applyLen())
	}
	data := group.applyAt(0)
	if data == nil {
		t.Fatal("missing data apply")
	}
	if data.identity.kind != preparedRootIdentityData || data.identity.ordinal != 0 {
		t.Fatalf("data identity=%#v", data.identity)
	}
	if data.state != preparedRootApplyStateAbandoned {
		t.Fatalf("data state=%v want abandoned", data.state)
	}
	if data.preparedRoot == 0 {
		t.Fatal("data prepared root is zero")
	}
	if data.plan.entries != 1 {
		t.Fatalf("data plan entries=%d want 1", data.plan.entries)
	}

	afterStats := db.Stats()
	statDelta := func(name string) uint64 {
		after := installGuardStatUint(t, afterStats, name)
		before := installGuardStatUint(t, beforeStats, name)
		if after < before {
			t.Fatalf("%s decreased: before=%d after=%d", name, before, after)
		}
		return after - before
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.groups_total"); got != 1 {
		t.Fatalf("prepared groups delta=%d want 1", got)
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.roots_total"); got != 1 {
		t.Fatalf("prepared roots delta=%d want 1", got)
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.entries_total"); got != 1 {
		t.Fatalf("prepared entries delta=%d want 1", got)
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.installed_total"); got != 0 {
		t.Fatalf("prepared installed delta=%d want 0", got)
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.abandoned_total"); got != 1 {
		t.Fatalf("prepared abandoned delta=%d want 1", got)
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.calls_total"); got != 0 {
		t.Fatalf("ordered root publish calls delta=%d want 0 before write-lock publish", got)
	}
	if got := statDelta("treedb.publish.ordered_root_delta_group.errors_total"); got != 0 {
		t.Fatalf("ordered root publish errors delta=%d want 0 before write-lock publish", got)
	}
}

func TestOrderedRootDeltaBatchGroupPreparedRootMetadataRecordsOptimisticFallbackToSerialized(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	seedDeltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	seedIter := seedDeltaTable.NewIterator(nil, nil)
	seedDelta, err := OrderedRootDeltaBatchFromIterator(seedIter)
	_ = seedIter.Close()
	if err != nil {
		t.Fatalf("seed OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = seedDelta.Close() }()
	_, seedRootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      baseRoot,
		Delta:         seedDelta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("seed publish ordered root group: %v", err)
	}
	if len(seedRootIDs) != 1 || seedRootIDs[0] == 0 {
		t.Fatalf("seed root IDs=%v want one nonzero root", seedRootIDs)
	}

	beforeStats := db.Stats()
	targetDeltaTable := mustFrozenSystemMemtable(t, "root/c", "vc")
	targetIter := targetDeltaTable.NewIterator(nil, nil)
	targetDelta, err := OrderedRootDeltaBatchFromIterator(targetIter)
	_ = targetIter.Close()
	if err != nil {
		t.Fatalf("target OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = targetDelta.Close() }()

	builderCalls := 0
	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      seedRootIDs[0],
		Delta:         targetDelta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			return nil, errors.New("unexpected target root IDs")
		}
		builderCalls++
		if builderCalls <= orderedRootOptimisticSystemDeltaRebaseMaxAttempts {
			if err := publishPreparedRootMetadataSystemRootChange(t, db, builderCalls); err != nil {
				return nil, err
			}
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/primary",
			strconv.FormatUint(rootIDs[0], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish ordered root group after optimistic fallback: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v want nonzero roots", systemRoot, rootIDs)
	}
	if builderCalls != orderedRootOptimisticSystemDeltaRebaseMaxAttempts+1 {
		t.Fatalf("builder calls=%d want %d", builderCalls, orderedRootOptimisticSystemDeltaRebaseMaxAttempts+1)
	}

	afterStats := db.Stats()
	statDelta := func(name string) uint64 {
		after := installGuardStatUint(t, afterStats, name)
		before := installGuardStatUint(t, beforeStats, name)
		if after < before {
			t.Fatalf("%s decreased: before=%d after=%d", name, before, after)
		}
		return after - before
	}

	attempts := uint64(orderedRootOptimisticSystemDeltaRebaseMaxAttempts)
	wantCalls := attempts + 1
	if got := statDelta("treedb.publish.ordered_root_delta_group.calls_total"); got != wantCalls {
		t.Fatalf("ordered root publish calls delta=%d want %d", got, wantCalls)
	}
	wantPreparedGroups := wantCalls + 1
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.groups_total"); got != wantPreparedGroups {
		t.Fatalf("prepared groups delta=%d want %d", got, wantPreparedGroups)
	}
	wantAbandonedRoots := attempts + 1
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.abandoned_total"); got != wantAbandonedRoots {
		t.Fatalf("prepared abandoned delta=%d want %d", got, wantAbandonedRoots)
	}
	wantInstalledRoots := attempts*2 + 2
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.installed_total"); got != wantInstalledRoots {
		t.Fatalf("prepared installed delta=%d want %d", got, wantInstalledRoots)
	}
	wantPreparedRoots := wantAbandonedRoots + wantInstalledRoots
	if got := statDelta("treedb.publish.ordered_root_delta_group.prepared_root.roots_total"); got != wantPreparedRoots {
		t.Fatalf("prepared roots delta=%d want %d", got, wantPreparedRoots)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/c"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(root/c): %v", err)
	}
	if got := string(entry.Value); got != "vc" {
		t.Fatalf("root/c=%q want vc", got)
	}
}

func publishPreparedRootMetadataSystemRootChange(t *testing.T, db *DB, ordinal int) error {
	t.Helper()
	deltaTable := mustFrozenSystemMemtable(t,
		"root/concurrent/"+strconv.Itoa(ordinal),
		"value-"+strconv.Itoa(ordinal),
	)
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		return err
	}
	defer func() { _ = delta.Close() }()
	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      0,
		Delta:         delta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			return nil, errors.New("unexpected concurrent root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/concurrent/"+strconv.Itoa(ordinal),
			strconv.FormatUint(rootIDs[0], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		return err
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return errors.New("concurrent publish returned invalid root IDs")
	}
	return nil
}
