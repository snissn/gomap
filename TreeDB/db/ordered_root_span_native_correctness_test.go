package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func openOrderedRootSpanNativeTestDB(t *testing.T, dir string, outerLeavesInValueLog bool, valueLogPointerThreshold int) *DB {
	t.Helper()
	db, err := Open(Options{
		Dir:                        dir,
		ChunkSize:                  64 * 1024,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: outerLeavesInValueLog,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      2,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		ValueLog: ValueLogOptions{
			Compression:      ValueLogCompressionBlock,
			BlockCodec:       ValueLogBlockLZ4,
			PointerThreshold: valueLogPointerThreshold,
		},
	})
	if err != nil {
		t.Fatalf("Open ordered-root span-native DB: %v", err)
	}
	return db
}

func newOrderedRootSpanNativeBatch(t *testing.T, entries int, prefix string) *batch.Batch {
	t.Helper()
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("%s-%06d", prefix, i))
		if err := delta.Set(key, val); err != nil {
			_ = delta.Close()
			t.Fatalf("Set seed %d: %v", i, err)
		}
	}
	return delta
}

func publishOrderedRootSpanNativeBatch(t *testing.T, db *DB, baseRoot uint64, delta *batch.Batch, storage OrderedRootStoragePolicy) uint64 {
	t.Helper()
	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot:               baseRoot,
			Delta:                  delta,
			StoragePolicy:          storage,
			PrepareReadOnly:        true,
			ReadOnlyPrepareWorkers: 2,
		}},
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 {
				t.Fatalf("system builder rootIDs=%v want one root", rootIDs)
			}
			return mustFrozenRawMemtable(t, collectionRootDescriptorPrefix+"ordered-root-span-native", encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}
	return rootIDs[0]
}

func requireOrderedRootValue(t *testing.T, db *DB, root uint64, key string, want []byte) {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	got, err := snap.GetAtRoot(root, []byte(key))
	if err != nil {
		t.Fatalf("GetAtRoot(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("GetAtRoot(%q)=%q want %q", key, got, want)
	}
}

func TestOrderedRootSpanNativeAppliesEdgeSemanticsAndSnapshotVisibility(t *testing.T) {
	db := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
	defer func() { _ = db.Close() }()

	baseDelta := newOrderedRootSpanNativeBatch(t, 2048, "base")
	defer func() { _ = baseDelta.Close() }()
	baseRoot := publishOrderedRootSpanNativeBatch(t, db, 0, baseDelta, OrderedRootStoragePagerLeaves)

	oldSnap := db.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatal("AcquireSnapshot old returned nil")
	}
	defer func() { _ = oldSnap.Close() }()

	update := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer func() { _ = update.Close() }()
	if err := update.Set([]byte("key-000010"), []byte("first-overwrite")); err != nil {
		t.Fatalf("Set first overwrite: %v", err)
	}
	if err := update.Set([]byte("key-000010"), []byte("second-overwrite")); err != nil {
		t.Fatalf("Set second overwrite: %v", err)
	}
	for _, idx := range []int{511, 1024, 1533} {
		if err := update.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte(fmt.Sprintf("updated-%06d", idx))); err != nil {
			t.Fatalf("Set update %d: %v", idx, err)
		}
	}
	for _, idx := range []int{12, 13, 1025, 1700} {
		if err := update.Delete([]byte(fmt.Sprintf("key-%06d", idx))); err != nil {
			t.Fatalf("Delete %d: %v", idx, err)
		}
	}

	usedBefore := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.used_ops_total")
	notImplBefore := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	newRoot := publishOrderedRootSpanNativeBatch(t, db, baseRoot, update, OrderedRootStoragePagerLeaves)
	if newRoot == baseRoot {
		t.Fatalf("newRoot=%d want changed root after update/delete mix", newRoot)
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.used_ops_total"); got <= usedBefore {
		t.Fatalf("ordered-root span-native used ops delta=%d want >0", got-usedBefore)
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total"); got != notImplBefore {
		t.Fatalf("span_native_not_implemented ops delta=%d want 0", got-notImplBefore)
	}

	requireOrderedRootValue(t, db, newRoot, "key-000010", []byte("second-overwrite"))
	requireOrderedRootValue(t, db, newRoot, "key-000511", []byte("updated-000511"))
	requireOrderedRootValue(t, db, newRoot, "key-000900", []byte("base-000900"))
	if _, err := oldSnap.GetEntryAtRoot(baseRoot, []byte("key-000012")); err != nil {
		t.Fatalf("old snapshot lost base value before delete: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot new returned nil")
	}
	defer func() { _ = snap.Close() }()
	if _, err := snap.GetEntryAtRoot(newRoot, []byte("key-000012")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("deleted key err=%v want ErrKeyNotFound", err)
	}
}

func TestOrderedRootSpanNativeNoopAndColdMaintenanceFallbacks(t *testing.T) {
	dir := t.TempDir()
	db := openOrderedRootSpanNativeTestDB(t, dir, false, 0)
	defer func() { _ = db.Close() }()

	baseDelta := newOrderedRootSpanNativeBatch(t, 256, "base")
	defer func() { _ = baseDelta.Close() }()
	coldBefore := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".count_total")
	baseRoot := publishOrderedRootSpanNativeBatch(t, db, 0, baseDelta, OrderedRootStoragePagerLeaves)
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".count_total"); got <= coldBefore {
		t.Fatalf("cold-build fallback count delta=%d want >0", got-coldBefore)
	}

	empty := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer func() { _ = empty.Close() }()
	noopBefore := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackBelowThreshold.String()+".count_total")
	noopRoot := publishOrderedRootSpanNativeBatch(t, db, baseRoot, empty, OrderedRootStoragePagerLeaves)
	if noopRoot != baseRoot {
		t.Fatalf("noop root=%d want base root %d", noopRoot, baseRoot)
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackBelowThreshold.String()+".count_total"); got <= noopBefore {
		t.Fatalf("below-threshold fallback count delta=%d want >0", got-noopBefore)
	}

	maintenanceBefore := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackMaintenance.String()+".count_total")
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: baseRoot,
			Iter:     mustFrozenSystemMemtable(t, "key-000001", "maintenance-rewrite").NewIterator(nil, nil),
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/maintenance", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("maintenance ordered-root publish: %v", err)
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackMaintenance.String()+".count_total"); got <= maintenanceBefore {
		t.Fatalf("maintenance fallback count delta=%d want >0", got-maintenanceBefore)
	}
}

func TestOrderedRootSpanNativeValueLogLeavesCheckpointReopenAndGC(t *testing.T) {
	dir := t.TempDir()
	db := openOrderedRootSpanNativeTestDB(t, dir, true, 1)
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionBlock,
		BlockCodec:  ValueLogBlockLZ4,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	db.SetLeafPageLog(leafLog)

	baseDelta := newOrderedRootSpanNativeBatch(t, 4096, "base")
	baseRoot := publishOrderedRootSpanNativeBatch(t, db, 0, baseDelta, OrderedRootStorageValueLogLeaves)
	_ = baseDelta.Close()

	update := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	type valueLogUpdate struct {
		key   []byte
		value []byte
	}
	var pointerUpdates []valueLogUpdate
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			value := bytes.Repeat([]byte{byte(1 + (anchor+j)%251)}, 180)
			pointerUpdates = append(pointerUpdates, valueLogUpdate{key: key, value: value})
		}
	}
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 10_000, len(pointerUpdates), func(i int) []byte {
		return pointerUpdates[i].value
	})
	_ = appendPointersInNewSegment(t, dir, 0, 2, 20_000, 1, func(i int) []byte {
		return []byte("active-value-log-segment")
	})
	if err := db.RefreshValueLogSet(); err != nil {
		_ = update.Close()
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	for i, upd := range pointerUpdates {
		if err := update.SetPointer(upd.key, ptrs[i]); err != nil {
			_ = update.Close()
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("SetPointer value-log update %q: %v", upd.key, err)
		}
	}
	rootID := publishOrderedRootSpanNativeBatch(t, db, baseRoot, update, OrderedRootStorageValueLogLeaves)
	_ = update.Close()
	if got := requireDBStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("span-native used ops=0 want ordered-root value-log prepared output")
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total"); got == 0 {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("root apply leaf-log pages written=0 want ordered-root span-native value-log output")
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.root_apply_internal_leaf_log_refs_total"); got == 0 {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("root apply internal leaf-log refs=0 want ordered-root span-native value-log pointers")
	}
	requireOrderedRootValue(t, db, rootID, "key-000017-000", bytes.Repeat([]byte{18}, 180))
	snap := db.AcquireSnapshot()
	if snap == nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatal("AcquireSnapshot returned nil")
	}
	entry, err := snap.GetEntryAtRoot(rootID, []byte("key-000017-000"))
	_ = snap.Close()
	if err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("GetEntryAtRoot raw value-log pointer: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("raw entry flags=%02x ptr=%+v want value-log pointer", entry.Flags, entry.ValuePtr)
	}
	gcStats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("ValueLogGC: %v", err)
	}
	if gcStats.SegmentsReferenced == 0 {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("ValueLogGC referenced segments=0 want reachable ordered-root value pointers; stats=%+v", gcStats)
	}
	requireOrderedRootValue(t, db, rootID, "key-000017-000", bytes.Repeat([]byte{18}, 180))
	if err := db.Checkpoint(); err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		_ = leafLog.Close()
		t.Fatalf("Close db: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	requireOrderedRootValue(t, reopened, rootID, "key-000017-000", bytes.Repeat([]byte{18}, 180))
}

func TestOrderedRootSpanNativeCommandWALRouteUsesAssignedLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openOrderedRootSpanNativeTestDB(t, dir, false, 0)
	defer func() { _ = db.Close() }()

	initial := newOrderedRootSpanNativeBatch(t, 1024, "base")
	defer func() { _ = initial.Close() }()
	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{BaseRoot: 0, Delta: initial}},
		mustRawKVCommandWALIntent(t, db, "cmd/initial", "1"),
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("initial command-WAL publish: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("initial rootIDs=%v want one root", rootIDs)
	}

	update := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer func() { _ = update.Close() }()
	if err := update.Set([]byte("key-000111"), []byte("command-wal-update")); err != nil {
		t.Fatalf("Set command-WAL update: %v", err)
	}
	lsnBefore := db.State().AppliedCommandLSN
	usedBefore := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.used_ops_total")
	var builderLSN uint64
	_, updatedRoots, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{BaseRoot: rootIDs[0], Delta: update}},
		mustRawKVCommandWALIntent(t, db, "cmd/update", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			builderLSN = ctx.AppliedCommandLSN
			return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("warm command-WAL publish: %v", err)
	}
	if builderLSN <= lsnBefore {
		t.Fatalf("builder LSN=%d want > before %d", builderLSN, lsnBefore)
	}
	if got := db.State().AppliedCommandLSN; got != builderLSN {
		t.Fatalf("AppliedCommandLSN=%d want builder LSN %d", got, builderLSN)
	}
	if got := requireDBStatUint64(t, db, "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.used_ops_total"); got <= usedBefore {
		t.Fatalf("command-WAL span-native used ops delta=%d want >0", got-usedBefore)
	}
	requireOrderedRootValue(t, db, updatedRoots[0], "key-000111", []byte("command-wal-update"))
}
