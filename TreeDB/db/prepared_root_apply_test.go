package db

import (
	"bytes"
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
}
