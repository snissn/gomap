package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogGC_EmptySet_NoValueLogSegments(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats != (ValueLogGCStats{}) {
		t.Fatalf("expected zero stats for empty value-log set, got %+v", stats)
	}
}

func TestValueLogGC_PostRefreshNilSetReturnsEmptyStats(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	origPostRefreshCurrentSet := valueLogGCPostRefreshCurrentSetNoRefresh
	calls := 0
	valueLogGCPostRefreshCurrentSetNoRefresh = func(vm *valuelog.Manager) *valuelog.Set {
		calls++
		return nil
	}
	defer func() { valueLogGCPostRefreshCurrentSetNoRefresh = origPostRefreshCurrentSet }()

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats != (ValueLogGCStats{}) {
		t.Fatalf("expected zero stats for nil post-refresh value-log set, got %+v", stats)
	}
	if calls != 1 {
		t.Fatalf("post-refresh current-set hook calls=%d want 1", calls)
	}

	segments, err := filepath.Glob(filepath.Join(ValueLogDirPath(dir), "*.log"))
	if err != nil {
		t.Fatalf("glob value-log segments: %v", err)
	}
	if len(segments) != 0 {
		t.Fatalf("nil-set GC path created or retained unexpected value-log segments: %v", segments)
	}
}

func TestValueLogRefTracker_OuterLeavesRebuildsStaleMetadata(t *testing.T) {
	dir := t.TempDir()
	stale := []byte("stale ref-count metadata from an older build")
	metaPath := filepath.Join(dir, valueLogRefCountsFileName)
	if err := os.WriteFile(metaPath, stale, 0o644); err != nil {
		t.Fatalf("write stale metadata: %v", err)
	}

	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if db.valueLogRefTracker == nil {
		t.Fatal("outer-leaf value-log mode did not initialize the logical ref tracker")
	}
	requireValueLogRefTrackerValid(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	after, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read stale metadata: %v", err)
	}
	if bytes.Equal(after, stale) {
		t.Fatal("stale metadata was not rebuilt on open")
	}
	if _, err := decodeValueLogRefCounts(after); err != nil {
		t.Fatalf("decode rebuilt metadata: %v", err)
	}
}

func TestValueOnlyValueLogFiles_KeepsReservedLaneInPrimaryValueLog(t *testing.T) {
	dir := t.TempDir()
	id, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("fileid: %v", err)
	}
	db := &DB{dir: dir, leafPageLog: replayInlineLeafPageLog{}}

	valuePath := filepath.Join(ValueLogDirPath(dir), "value-l255-000001.log")
	valueFiles := db.valueOnlyValueLogFiles(map[uint32]*valuelog.File{
		id: {Path: valuePath},
	})
	if _, ok := valueFiles[id]; !ok {
		t.Fatalf("primary value_vlog reserved-lane segment was filtered")
	}

	leafPath := filepath.Join(LeafLogDirPath(dir), "value-l255-000001.log")
	leafFiles := db.valueOnlyValueLogFiles(map[uint32]*valuelog.File{
		id: {Path: leafPath},
	})
	if _, ok := leafFiles[id]; ok {
		t.Fatalf("leaf_vlog reserved-lane segment was not filtered")
	}
}

func TestValueLogGC_IgnoresInlineLeafLogWhenDBDefaultUsesPagerLeaves(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.indexOuterLeavesInValueLog {
		t.Fatalf("indexOuterLeavesInValueLog=true, want false for this regression")
	}
	inlineAppender, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("new replay inline appender: %v", err)
	}
	defer func() { _ = inlineAppender.close() }()
	db.SetValueLogAppender(inlineAppender)
	db.SetLeafPageLog(replayInlineLeafPageLog{appender: inlineAppender})
	defer db.SetValueLogAppender(nil)
	defer db.SetLeafPageLog(nil)
	if db.leafPageLog == nil {
		t.Fatalf("inline appender did not install a leaf page log")
	}

	if _, err := db.AppendValueLogValues([][]byte{bytes.Repeat([]byte("ordinary-value|"), 32)}); err != nil {
		t.Fatalf("append ordinary value-log value: %v", err)
	}
	appender := db.currentValueLogAppender()
	if appender == nil {
		t.Fatalf("missing value-log appender")
	}
	primaryPath, primaryFileID, ok := appender.CurrentValueLogSegment()
	if !ok || primaryPath == "" || primaryFileID == 0 {
		t.Fatalf("ordinary value-log segment not opened: path=%q id=%d ok=%t", primaryPath, primaryFileID, ok)
	}

	table := mustFrozenSystemMemtable(t, "doc/u1", "document")
	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          table.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}})
	if err != nil {
		t.Fatalf("publish value-log leaf root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
	leafPtrs := requireLeafLogRootChildren(t, db, rootIDs[0])
	leafFileID := page.ValueLogFileID(leafPtrs[0].FileID)

	preSet := db.valueLogManager.CurrentSetNoRefresh()
	if preSet == nil {
		t.Fatalf("missing value-log set before GC")
	}
	if _, ok := preSet.Files[primaryFileID]; !ok {
		_ = db.valueLogManager.Release(preSet)
		t.Fatalf("ordinary value-log segment %d missing before GC", primaryFileID)
	}
	if _, ok := preSet.Files[leafFileID]; !ok {
		_ = db.valueLogManager.Release(preSet)
		t.Fatalf("leaf-log segment %d missing before GC", leafFileID)
	}
	_ = db.valueLogManager.Release(preSet)

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{ProtectedPaths: []string{primaryPath}})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("leaf-log segment was considered ordinary GC work: %+v", stats)
	}

	postSet := db.valueLogManager.CurrentSetNoRefresh()
	if postSet == nil {
		t.Fatalf("missing value-log set after GC")
	}
	if _, ok := postSet.Files[leafFileID]; !ok {
		_ = db.valueLogManager.Release(postSet)
		t.Fatalf("leaf-log segment %d missing from current set after GC", leafFileID)
	}
	_ = db.valueLogManager.Release(postSet)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("doc/u1"))
	if err != nil {
		t.Fatalf("read value-log leaf root after GC: %v", err)
	}
	if got := string(entry.Value); got != "document" {
		t.Fatalf("value after GC=%q want document", got)
	}
}

type valueLogGCDurableSlotState struct {
	visibleCommit uint64
	durableCommit uint64
	activeSlot    uint64
	slotCommits   [2]uint64
}

func captureValueLogGCDurableSlotState(t *testing.T, database *DB, phase string) valueLogGCDurableSlotState {
	t.Helper()
	visible, ok := database.StateToken()
	if !ok {
		t.Fatalf("%s: visible state unavailable", phase)
	}
	database.durablePublishMu.Lock()
	database.rootReuseMu.RLock()
	state := valueLogGCDurableSlotState{
		visibleCommit: visible.CommitSeq,
		durableCommit: database.durableRoot.record.CommitSeq,
		activeSlot:    database.durableRoot.slot,
		slotCommits:   database.durableRoot.slotCommit,
	}
	slotRecords := database.durableRoot.slotRecord
	database.rootReuseMu.RUnlock()
	database.durablePublishMu.Unlock()
	t.Logf("%s: visible=%d durable=%d active_slot=%d slot_commits=%v slot_records=[{commit:%d user:%d system:%d lsn:%d} {commit:%d user:%d system:%d lsn:%d}]",
		phase, state.visibleCommit, state.durableCommit, state.activeSlot, state.slotCommits,
		slotRecords[0].CommitSeq, slotRecords[0].UserRootPageID,
		slotRecords[0].SystemRootPageID, slotRecords[0].AppliedCommandLSN,
		slotRecords[1].CommitSeq, slotRecords[1].UserRootPageID,
		slotRecords[1].SystemRootPageID, slotRecords[1].AppliedCommandLSN)
	return state
}

func recoverableValueLogSegmentsForTest(t *testing.T, database *DB) map[uint32]struct{} {
	t.Helper()
	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatalf("capture recoverable roots: %v", err)
	}
	defer set.Release()
	referenced, err := database.referencedValueLogSegmentsForRecoverableRootSet(context.Background(), set)
	if err != nil {
		t.Fatalf("scan recoverable value-log references: %v", err)
	}
	return referenced
}

func TestValueLogGC_RemovesUnreferencedSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	path1 := filepath.Join(walDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	w1.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	value1 := bytes.Repeat([]byte("value-1|"), 128)
	ptr1, err := w1.Append(0, nil, 1, value1)
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}
	registerTestValueLogProducer(t, dir, path1, id1)

	path2 := filepath.Join(walDir, "value-l0-000002.log")
	id2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	value2 := bytes.Repeat([]byte("value-2|"), 64)
	ptr2, err := w2.Append(0, nil, 2, value2)
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}
	registerTestValueLogProducer(t, dir, path2, id2)

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	// Make the pointer-bearing root recovery-selectable so this fixture cannot
	// accidentally pass only because background publication skipped that state.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint pointer-bearing root: %v", err)
	}
	pointerState := captureValueLogGCDurableSlotState(t, db, "pointer-bearing root")
	if pointerState.durableCommit < pointerState.visibleCommit {
		t.Fatalf("pointer durable commit=%d want at least visible commit=%d", pointerState.durableCommit, pointerState.visibleCommit)
	}
	if _, ok := recoverableValueLogSegmentsForTest(t, db)[id1]; !ok {
		t.Fatalf("pointer-bearing segment %d is not recovery-selectable", id1)
	}

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}
	deleteVisible := captureValueLogGCDurableSlotState(t, db, "delete visible")
	if deleteVisible.visibleCommit <= pointerState.visibleCommit {
		t.Fatalf("delete visible commit=%d want greater than pointer commit=%d", deleteVisible.visibleCommit, pointerState.visibleCommit)
	}
	// Publish the deletion into the alternate durable slot before the successor
	// below overwrites the older pointer-bearing slot. Without this handoff, one
	// recovery-selectable root can still retain k1 and GC must delete nothing.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint deleted pointer: %v", err)
	}
	deleteDurable := captureValueLogGCDurableSlotState(t, db, "delete durable")
	if deleteDurable.durableCommit < deleteVisible.visibleCommit {
		t.Fatalf("delete durable commit=%d want at least visible commit=%d", deleteDurable.durableCommit, deleteVisible.visibleCommit)
	}
	advancePastRetainedDurableSlotForTest(t, db)
	settled := captureValueLogGCDurableSlotState(t, db, "both slots settled")
	for slot, commit := range settled.slotCommits {
		if commit < deleteVisible.visibleCommit {
			t.Fatalf("recoverable slot %d commit=%d want at least delete commit=%d", slot, commit, deleteVisible.visibleCommit)
		}
	}
	if _, ok := recoverableValueLogSegmentsForTest(t, db)[id1]; ok {
		t.Fatalf("deleted pointer segment %d remains in recoverable roots", id1)
	}
	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected at least one segment deleted, got %+v", stats)
	}

	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected segment1 to be removed, err=%v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected segment2 to remain, err=%v", err)
	}
	got, err := db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("read retained segment value: %v", err)
	}
	if !bytes.Equal(got, value2) {
		t.Fatalf("retained segment value=%q want %q", got, value2)
	}
}

func TestMarkValueLogZombieTreatsStaleRecoverableRootSetAsDeferred(t *testing.T) {
	dir := t.TempDir()

	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	path := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	ptr, err := writer.Append(0, nil, 1, bytes.Repeat([]byte("stale-zombie|"), 128))
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)

	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("stale-zombie"), ptr); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Delete([]byte("stale-zombie")); err != nil {
		t.Fatal(err)
	}
	advancePastRetainedDurableSlotForTest(t, database)

	originalEpoch := database.systemRootPublishEpoch.Load()
	t.Cleanup(func() {
		database.testValueLogGCBeforeRevalidateHook = nil
		database.systemRootPublishEpoch.Store(originalEpoch)
	})
	var once sync.Once
	database.testValueLogGCBeforeRevalidateHook = func() {
		once.Do(func() { database.systemRootPublishEpoch.Add(1) })
	}
	err = database.MarkValueLogZombie(fileID)
	database.testValueLogGCBeforeRevalidateHook = nil
	database.systemRootPublishEpoch.Store(originalEpoch)
	if !errors.Is(err, ErrValueLogZombieDeferred) || !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatalf("MarkValueLogZombie error=%v, want deferred stale capability", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale zombie attempt removed segment: %v", err)
	}
}

func TestValueLogGC_StableIdentityPinDefersUnreferencedSegmentDelete(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("unreferenced stable value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogManager.RegisterSegment(path, fileID); err != nil {
		t.Fatal(err)
	}
	activeID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(ValueLogDirPath(dir), "value-l0-000002.log")
	activeWriter, err := valuelog.NewWriter(activePath, activeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := activeWriter.Append(0, nil, 2, []byte("active value")); err != nil {
		t.Fatal(err)
	}
	if err := activeWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogManager.RegisterSegment(activePath, activeID); err != nil {
		t.Fatal(err)
	}
	token, err := database.valueLogManager.StableResourceToken(fileID, valuelog.StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "0", Generation: 1,
		DiagnosticPath: "value_vlog/" + filepath.Base(path), Digest: [32]byte{1},
		Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsEligible == 0 {
		t.Fatalf("target segment was not eligible: %+v", stats)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("GC removed stable-pinned segment: %v", err)
	}
	token.Release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("GC zombie was not removed after stable token release")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestValueLogGC_VisibleDeleteCannotUnlinkOlderDurableRootSegment(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir:                       dir,
		Durability:                DurabilityWALOffRelaxed,
		DisableBackgroundPrune:    true,
		rootPublicationFixedDelay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	oldValue := bytes.Repeat([]byte("older-durable-root-value|"), 32)
	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 300_000, 1, func(int) []byte {
		return oldValue
	})[0]
	appendPointersInNewSegment(t, dir, 0, 2, 400_000, 1, func(int) []byte {
		return []byte("active-segment")
	})
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}

	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("durable"), oldPtr); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	durableBefore := database.durableRoot.record.CommitSeq
	if durableBefore == 0 || database.rootPublication.coordinator.Stats().VisibleCommitSeq != durableBefore {
		t.Fatalf("initial durable/visible frontier=(%d,%d), want equal non-zero frontiers", durableBefore, database.rootPublication.coordinator.Stats().VisibleCommitSeq)
	}

	// Hold publication of the delete candidate before any durable-root record
	// write. This models a crash while the delete is visible but the older root
	// remains recovery-selectable.
	releasePublisher := make(chan struct{})
	releasedPublisher := false
	release := func() {
		if !releasedPublisher {
			close(releasePublisher)
			releasedPublisher = true
		}
	}
	restoreCuts := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == dir && event.Point == durabilitycut.BeforePublicationSealWrite {
			<-releasePublisher
		}
		return nil
	})
	t.Cleanup(func() {
		release()
		restoreCuts()
	})

	hookRan := false
	database.testStorageMaintenanceBeforeLockHook = func(operation string) {
		if operation != "value-log-gc" || hookRan {
			return
		}
		hookRan = true
		database.testStorageMaintenanceBeforeLockHook = nil
		if err := database.Delete([]byte("durable")); err != nil {
			t.Errorf("delete after GC preflight: %v", err)
		}
	}

	dryRun, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if !hookRan {
		t.Fatal("delete did not run between GC preflight and maintenance lock")
	}
	publication := database.rootPublication.coordinator.Stats()
	if publication.VisibleCommitSeq <= durableBefore || publication.DurableCommitSeq != durableBefore {
		t.Fatalf("post-delete visible/durable frontier=(%d,%d), want visible > durable=%d", publication.VisibleCommitSeq, publication.DurableCommitSeq, durableBefore)
	}
	if dryRun.SegmentsReferenced == 0 || dryRun.BytesReferenced == 0 || dryRun.SegmentsEligible != 0 || dryRun.SegmentsPending != 0 {
		t.Fatalf("dry-run GC did not classify the older-root segment as recoverably referenced: %+v", dryRun)
	}

	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsReferenced == 0 || stats.BytesReferenced == 0 || stats.SegmentsEligible != 0 || stats.SegmentsPending != 0 {
		t.Fatalf("GC did not classify the older-root segment as recoverably referenced: %+v", stats)
	}
	oldPath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("GC unlinked segment retained by older durable root: %v", err)
	}

	// A concurrent read-only open selects the still-durable root, providing the
	// same dependency check an abrupt reopen would perform without allowing the
	// live DB's clean Close to stabilize the visible delete first.
	recovered, err := openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("open recovery-selectable root after GC: %v", err)
	}
	got, err := recovered.Get([]byte("durable"))
	if err != nil {
		_ = recovered.Close()
		t.Fatalf("read older durable root after GC: %v", err)
	}
	if !bytes.Equal(got, oldValue) {
		_ = recovered.Close()
		t.Fatalf("older durable root value mismatch: got %d bytes want %d", len(got), len(oldValue))
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}

	release()
	restoreCuts()
	restoreCuts = func() {}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	advancePastRetainedDurableSlotForTest(t, database)
	converged, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC after durable-root advance: %v", err)
	}
	if converged.SegmentsDeleted == 0 || converged.BytesDeleted == 0 {
		t.Fatalf("older-root value-log debt did not converge after both slots advanced: %+v", converged)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("older-root segment still present after convergence GC: %v", err)
	}
}

func TestValueLogGC_ObservedSourceCannotZombieOlderRecoverableRootSegment(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir:                       dir,
		Durability:                DurabilityWALOffRelaxed,
		DisableBackgroundPrune:    true,
		rootPublicationFixedDelay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	oldValue := bytes.Repeat([]byte("observed-older-root-value|"), 32)
	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 410_000, 1, func(int) []byte {
		return oldValue
	})[0]
	appendPointersInNewSegment(t, dir, 0, 2, 420_000, 1, func(int) []byte {
		return []byte("active-segment")
	})
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}

	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("durable"), oldPtr); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	durableBefore := database.durableRoot.record.CommitSeq

	releasePublisher := make(chan struct{})
	releasedPublisher := false
	release := func() {
		if !releasedPublisher {
			close(releasePublisher)
			releasedPublisher = true
		}
	}
	restoreCuts := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == dir && event.Point == durabilitycut.BeforePublicationSealWrite {
			<-releasePublisher
		}
		return nil
	})
	t.Cleanup(func() {
		release()
		restoreCuts()
	})

	if err := database.Delete([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	publication := database.rootPublication.coordinator.Stats()
	if publication.VisibleCommitSeq <= durableBefore || publication.DurableCommitSeq != durableBefore {
		t.Fatalf("post-delete visible/durable frontier=(%d,%d), want visible > durable=%d", publication.VisibleCommitSeq, publication.DurableCommitSeq, durableBefore)
	}

	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{oldPtr.FileID},
		ObservedSourceAssumeUnreferenced: true,
		ObservedSourceReclaimActive:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObservedSourceSegmentsReferenced != 1 || stats.ObservedSourceSegmentsEligible != 0 || stats.ObservedSourceSegmentsDeleted != 0 {
		t.Fatalf("observed-only GC did not retain older-root reference: %+v", stats)
	}
	if err := database.MarkValueLogZombie(oldPtr.FileID); !errors.Is(err, ErrValueLogZombieDeferred) {
		t.Fatalf("MarkValueLogZombie older-root error=%v want ErrValueLogZombieDeferred", err)
	}
	oldPath := valueLogSegmentPath(t, dir, oldPtr.FileID)
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("observed-only GC removed older-root segment: %v", err)
	}

	recovered, err := openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("open older recoverable root: %v", err)
	}
	got, err := recovered.Get([]byte("durable"))
	if err != nil {
		_ = recovered.Close()
		t.Fatal(err)
	}
	if !bytes.Equal(got, oldValue) {
		_ = recovered.Close()
		t.Fatalf("older recoverable value mismatch: got %d bytes want %d", len(got), len(oldValue))
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValueLogGC_KeepsPendingValueLogAppenderSegments(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, dir, 0, 1, 500, 1, func(int) []byte {
		return bytes.Repeat([]byte("old-unreferenced|"), 32)
	})
	id1 := appendPointersInNewSegment(t, dir, 0, 2, 1_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("pending-seq2|"), 32)
	})[0].FileID
	appendPointersInNewSegment(t, dir, 0, 3, 2_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-seq3|"), 32)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	db.pendingValueLogAppendMu.Lock()
	db.pendingValueLogAppendFileIDRefs = map[uint32]int{id1: 1}
	db.pendingValueLogAppendMu.Unlock()

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted != 1 {
		t.Fatalf("expected one unreferenced segment deleted while pending remained, got %+v", stats)
	}
	oldPath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old unreferenced segment should have been deleted: %v", err)
	}
	pendingPath := filepath.Join(dir, "value_vlog", "value-l0-000002.log")
	if _, err := os.Stat(pendingPath); err != nil {
		t.Fatalf("pending segment missing after GC: %v", err)
	}
}

func TestValueLogGC_FullScanDoesNotBlockPublishBeforeLockedPhase(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, db.dir, 0, 1, 1_000, 1, func(int) []byte { return []byte("old") })
	appendPointersInNewSegment(t, db.dir, 0, 2, 2_000, 1, func(int) []byte { return []byte("active") })
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value-log set: %v", err)
	}
	db.valueLogRefTracker.invalidate()

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseScan) }) }
	defer release()
	var once sync.Once
	restore := registerScanValueLogRefCountsHook(func() {
		once.Do(func() {
			close(scanStarted)
			<-releaseScan
		})
	})
	defer restore()

	gcDone := make(chan error, 1)
	go func() {
		_, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
		gcDone <- err
	}()
	<-scanStarted

	publishDone := make(chan error, 1)
	go func() { publishDone <- db.Set([]byte("publish-during-gc-scan"), []byte("ok")) }()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish during scan: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish blocked while value-log GC full scan was paused")
	}
	release()
	if err := <-gcDone; err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
}

func TestValueLogGC_PostScanFirstReferenceToOldSegmentIsSafe(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldPtr := appendPointersInNewSegment(t, db.dir, 0, 1, 3_000, 1, func(int) []byte { return []byte("old") })[0]
	appendPointersInNewSegment(t, db.dir, 0, 2, 4_000, 1, func(int) []byte { return []byte("active") })
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value-log set: %v", err)
	}
	db.valueLogRefTracker.invalidate() // Exercise the full-scan fallback split.
	oldPath := filepath.Join(ValueLogDirPath(db.dir), "value-l0-000001.log")

	postScan := make(chan struct{})
	releaseGC := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseGC) }) }
	defer release()
	var once sync.Once
	restore := registerValueLogGCPostScanHook(func() {
		once.Do(func() {
			close(postScan)
			<-releaseGC
		})
	})
	defer restore()
	gcDone := make(chan error, 1)
	go func() {
		_, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
		gcDone <- err
	}()
	<-postScan
	setValueLogPointer(t, db, []byte("old-first-reference"), oldPtr)
	release()
	if err := <-gcDone; err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old segment first referenced after scan was removed: %v", err)
	}
}

func TestValueLogGC_PostScanNewSegmentIsSafe(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, db.dir, 0, 1, 5_000, 1, func(int) []byte { return []byte("old") })
	appendPointersInNewSegment(t, db.dir, 0, 2, 6_000, 1, func(int) []byte { return []byte("active") })
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value-log set: %v", err)
	}
	db.valueLogRefTracker.invalidate() // Exercise the full-scan fallback split.

	postScan := make(chan struct{})
	releaseGC := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseGC) }) }
	defer release()
	var hookOnce sync.Once
	restore := registerValueLogGCPostScanHook(func() {
		hookOnce.Do(func() {
			close(postScan)
			<-releaseGC
		})
	})
	defer restore()
	gcDone := make(chan error, 1)
	go func() {
		_, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
		gcDone <- err
	}()
	<-postScan

	newPtr := appendPointersInNewSegment(t, db.dir, 0, 3, 7_000, 1, func(int) []byte { return []byte("new") })[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh post-scan value-log set: %v", err)
	}
	key := []byte("post-scan-new-segment")
	setValueLogPointer(t, db, key, newPtr)
	release()
	if err := <-gcDone; err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	newPath := filepath.Join(ValueLogDirPath(db.dir), "value-l0-000003.log")
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("segment created and referenced after scan was removed: %v", err)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get post-scan pointer: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("get post-scan pointer = %q, want %q", got, "new")
	}
}

func TestValueLogGC_RepeatedPostScanPublicationAbortsWithoutDelete(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, db.dir, 0, 1, 8_000, 1, func(int) []byte { return []byte("old") })
	appendPointersInNewSegment(t, db.dir, 0, 2, 9_000, 1, func(int) []byte { return []byte("active") })
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value-log set: %v", err)
	}
	db.valueLogRefTracker.invalidate()
	oldPath := filepath.Join(ValueLogDirPath(db.dir), "value-l0-000001.log")

	var attempts int
	var publishErr error
	restore := registerValueLogGCPostScanHook(func() {
		attempts++
		publishErr = db.Set([]byte(fmt.Sprintf("publish-after-scan-%d", attempts)), []byte("ok"))
	})
	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	restore()
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if publishErr != nil {
		t.Fatalf("publish after scan: %v", publishErr)
	}
	if attempts != 2 {
		t.Fatalf("scan attempts = %d, want bounded initial attempt plus one retry", attempts)
	}
	if stats.SegmentsDeleted != 0 || stats.BytesDeleted != 0 {
		t.Fatalf("stale retry deleted segments: %+v", stats)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("bounded stale retry removed an unvalidated segment: %v", err)
	}
}

func setValueLogPointer(t *testing.T, db *DB, key []byte, ptr page.ValuePtr) {
	t.Helper()
	b := db.NewBatch().(*Batch)
	defer b.Close()
	if err := b.SetPointer(key, ptr); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write pointer: %v", err)
	}
}

func TestValueLogRefTracker_GCReplaceDoesNotRegressConcurrentAdvance(t *testing.T) {
	tracker := newValueLogRefTracker()
	tracker.replace(map[uint32]uint64{12: 1}, 6, true)
	observedRevision := tracker.revisionSnapshot()
	advanced := make(chan struct{})
	go func() {
		tracker.replace(map[uint32]uint64{11: 1}, 7, true)
		close(advanced)
	}()
	<-advanced
	if tracker.replaceUnlessAdvanced(map[uint32]uint64{12: 1}, 6, true, observedRevision) {
		t.Fatal("GC fallback replaced tracker after concurrent sequence advance")
	}
	refs, ok := tracker.referencedSet(7)
	if !ok {
		t.Fatal("GC fallback regressed tracker commit sequence")
	}
	if _, ok := refs[11]; !ok {
		t.Fatalf("concurrently advanced counts were replaced by stale scan: %v", refs)
	}
}

func TestValueLogRefTracker_GCReplaceRepairsUnchangedStaleAheadState(t *testing.T) {
	tracker := newValueLogRefTracker()
	tracker.replace(map[uint32]uint64{11: 1}, 7, true)
	observedRevision := tracker.revisionSnapshot()
	if !tracker.replaceUnlessAdvanced(map[uint32]uint64{12: 1}, 6, true, observedRevision) {
		t.Fatal("unchanged stale-ahead tracker was not repaired")
	}
	refs, ok := tracker.referencedSet(6)
	if !ok {
		t.Fatal("repaired tracker does not match scanned sequence")
	}
	if _, ok := refs[12]; !ok {
		t.Fatalf("repaired tracker counts = %v, want file 12", refs)
	}
}

func TestValueLogGC_ProtectedPathsDoNotKeepHistoricalRewriteLanes(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, dir, 0, 1, 1_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq1|"), 32)
	})
	appendPointersInNewSegment(t, dir, 0, 2, 2_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq2|"), 32)
	})
	appendPointersInNewSegment(t, dir, 250, 1, 3_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane250-seq1|"), 32)
	})
	appendPointersInNewSegment(t, dir, 250, 2, 4_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane250-seq2|"), 32)
	})

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	protected := []string{
		filepath.Join(dir, "value_vlog", "value-l0-000002.log"),
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{ProtectedPaths: protected})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted < 2 {
		t.Fatalf("expected GC to delete historical unprotected rewrite-lane segments, got %+v", stats)
	}

	for _, path := range []string{
		filepath.Join(dir, "value_vlog", "value-l250-000001.log"),
		filepath.Join(dir, "value_vlog", "value-l250-000002.log"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be deleted, err=%v", filepath.Base(path), err)
		}
	}

	for _, path := range []string{
		filepath.Join(dir, "value_vlog", "value-l0-000001.log"),
		filepath.Join(dir, "value_vlog", "value-l0-000002.log"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected protected-lane window to retain %s, err=%v", filepath.Base(path), err)
		}
	}
}

func TestValueLogGC_ObservedSourceReclaimActiveRequiresExplicitOption(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, dir, 0, 1, 1_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq1|"), 32)
	})
	appendPointersInNewSegment(t, dir, 0, 2, 2_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq2|"), 32)
	})

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	activeID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("active fileid: %v", err)
	}
	activePath := filepath.Join(dir, "value_vlog", "value-l0-000002.log")

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{activeID},
		ObservedSourceAssumeUnreferenced: true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC without active reclaim: %v", err)
	}
	if stats.ObservedSourceSegmentsActive != 1 || stats.ObservedSourceSegmentsDeleted != 0 {
		t.Fatalf("observed active source was not protected by default: %+v", stats)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("expected active source to remain without explicit reclaim: %v", err)
	}

	stats, err = db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{activeID},
		ObservedSourceAssumeUnreferenced: true,
		ObservedSourceReclaimActive:      true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC with active reclaim: %v", err)
	}
	if stats.ObservedSourceSegmentsEligible != 1 || stats.ObservedSourceSegmentsDeleted != 1 {
		t.Fatalf("observed active source was not reclaimed with explicit option: %+v", stats)
	}
	if _, err := os.Stat(activePath); !os.IsNotExist(err) {
		t.Fatalf("expected active source to be removed with explicit reclaim, err=%v", err)
	}
}

func TestValueLogGC_KeepsEveryCurrentWritableInMultiCurrentLane(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value log: %v", err)
	}
	type currentWriter struct {
		writer *valuelog.Writer
		path   string
		id     uint32
	}
	writers := make([]currentWriter, 0, 2)
	for seq := uint32(1); seq <= 2; seq++ {
		id, err := valuelog.EncodeFileID(0, seq)
		if err != nil {
			t.Fatalf("encode file id: %v", err)
		}
		path := valuelog.SegmentPath(valueLogDir, id)
		writer, err := valuelog.NewWriter(path, id)
		if err != nil {
			t.Fatalf("new writer: %v", err)
		}
		if _, err := writer.Append(0, nil, uint64(seq), []byte("unreferenced")); err != nil {
			t.Fatalf("seed writer %d: %v", seq, err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("flush writer %d: %v", seq, err)
		}
		if err := database.valueLogManager.RegisterSegment(path, id); err != nil {
			t.Fatalf("register writer %d: %v", seq, err)
		}
		writers = append(writers, currentWriter{writer: writer, path: path, id: id})
	}
	database.valueLogManager.SetMultiCurrentWritableLane(0, true)
	for _, current := range writers {
		if err := database.valueLogManager.PromoteCurrentWritable(current.id); err != nil {
			t.Fatalf("promote current writer %d: %v", current.id, err)
		}
	}

	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsActive != 2 || stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("multi-current writers were not all protected: %+v", stats)
	}
	for _, current := range writers {
		if _, err := os.Stat(current.path); err != nil {
			t.Fatalf("current writer %d removed by GC: %v", current.id, err)
		}
	}

	lateValue := []byte("appended after GC")
	latePtr, err := writers[0].writer.Append(0, nil, 3, lateValue)
	if err != nil {
		t.Fatalf("append after GC: %v", err)
	}
	if err := writers[0].writer.Flush(); err != nil {
		t.Fatalf("flush after GC: %v", err)
	}
	got, err := database.valueLogManager.Read(latePtr)
	if err != nil {
		t.Fatalf("read after GC: %v", err)
	}
	if !bytes.Equal(got, lateValue) {
		t.Fatalf("read after GC=%q want %q", got, lateValue)
	}

	for _, current := range writers {
		if err := current.writer.Close(); err != nil {
			t.Fatalf("close writer %d: %v", current.id, err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	database, err = Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = database.Close() }()
	got, err = database.valueLogManager.Read(latePtr)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(got, lateValue) {
		t.Fatalf("read after reopen=%q want %q", got, lateValue)
	}
}

func TestMarkValueLogZombie_PreservesMissingSegmentSignal(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, dir, 0, 1, 1_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("tracked|"), 32)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	missingID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("missing fileid: %v", err)
	}
	if err := db.MarkValueLogZombie(missingID); !errors.Is(err, valuelog.ErrFileNotFound) {
		t.Fatalf("MarkValueLogZombie missing error=%v, want ErrFileNotFound", err)
	}
}

func TestMarkValueLogZombie_PreservesMissingSegmentSignalWhenValueLogIsEmpty(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	missingID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("missing fileid: %v", err)
	}
	if err := db.MarkValueLogZombie(missingID); !errors.Is(err, valuelog.ErrFileNotFound) {
		t.Fatalf("MarkValueLogZombie missing error=%v, want ErrFileNotFound", err)
	}
}

func TestValueLogGC_ProtectedPathBreakdownStats(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for seq := 1; seq <= 5; seq++ {
		seq := seq
		appendPointersInNewSegment(t, dir, 0, uint32(seq), uint64(seq)*1_000, 1, func(int) []byte {
			return bytes.Repeat([]byte(fmt.Sprintf("lane0-seq%d|", seq)), 32)
		})
	}

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	inUseOnlyPath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	retainedOnlyPath := filepath.Join(dir, "value_vlog", "value-l0-000002.log")
	overlapPath := filepath.Join(dir, "value_vlog", "value-l0-000003.log")
	observedInUseID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("observed in-use fileid: %v", err)
	}
	observedRetainedID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("observed retained fileid: %v", err)
	}
	observedOverlapID, err := valuelog.EncodeFileID(0, 3)
	if err != nil {
		t.Fatalf("observed overlap fileid: %v", err)
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		DryRun:                 true,
		ProtectedInUsePaths:    []string{inUseOnlyPath, overlapPath},
		ProtectedRetainedPaths: []string{retainedOnlyPath, overlapPath},
		ObservedSourceFileIDs:  []uint32{observedInUseID, observedRetainedID, observedOverlapID},
	})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if stats.SegmentsTotal != 5 {
		t.Fatalf("segments total=%d want 5", stats.SegmentsTotal)
	}
	if stats.SegmentsActive != 2 {
		t.Fatalf("segments active=%d want 2", stats.SegmentsActive)
	}
	if stats.SegmentsProtected != 3 {
		t.Fatalf("segments protected=%d want 3", stats.SegmentsProtected)
	}
	if stats.SegmentsProtectedInUse != 1 {
		t.Fatalf("segments protected in-use=%d want 1", stats.SegmentsProtectedInUse)
	}
	if stats.SegmentsProtectedRetained != 1 {
		t.Fatalf("segments protected retained=%d want 1", stats.SegmentsProtectedRetained)
	}
	if stats.SegmentsProtectedOverlap != 1 {
		t.Fatalf("segments protected overlap=%d want 1", stats.SegmentsProtectedOverlap)
	}
	if stats.SegmentsProtectedOther != 0 {
		t.Fatalf("segments protected other=%d want 0", stats.SegmentsProtectedOther)
	}
	if stats.SegmentsEligible != 0 {
		t.Fatalf("segments eligible=%d want 0", stats.SegmentsEligible)
	}
	if stats.SegmentsDeleted != 0 {
		t.Fatalf("segments deleted=%d want 0", stats.SegmentsDeleted)
	}
	if stats.BytesProtected <= 0 {
		t.Fatalf("bytes protected=%d want >0", stats.BytesProtected)
	}
	if stats.BytesProtectedInUse <= 0 || stats.BytesProtectedRetained <= 0 || stats.BytesProtectedOverlap <= 0 {
		t.Fatalf("expected non-zero protected byte buckets, got %+v", stats)
	}
	if stats.BytesProtectedOther != 0 {
		t.Fatalf("bytes protected other=%d want 0", stats.BytesProtectedOther)
	}
	if stats.ObservedSourceSegments != 3 {
		t.Fatalf("observed source segments=%d want 3", stats.ObservedSourceSegments)
	}
	if stats.ObservedSourceSegmentsReferenced != 0 {
		t.Fatalf("observed source segments referenced=%d want 0", stats.ObservedSourceSegmentsReferenced)
	}
	if stats.ObservedSourceSegmentsActive != 0 {
		t.Fatalf("observed source segments active=%d want 0", stats.ObservedSourceSegmentsActive)
	}
	if stats.ObservedSourceSegmentsProtected != 3 {
		t.Fatalf("observed source segments protected=%d want 3", stats.ObservedSourceSegmentsProtected)
	}
	if stats.ObservedSourceSegmentsProtectedInUse != 1 {
		t.Fatalf("observed source segments protected in-use=%d want 1", stats.ObservedSourceSegmentsProtectedInUse)
	}
	if stats.ObservedSourceSegmentsProtectedRetained != 1 {
		t.Fatalf("observed source segments protected retained=%d want 1", stats.ObservedSourceSegmentsProtectedRetained)
	}
	if stats.ObservedSourceSegmentsProtectedOverlap != 1 {
		t.Fatalf("observed source segments protected overlap=%d want 1", stats.ObservedSourceSegmentsProtectedOverlap)
	}
	if stats.ObservedSourceSegmentsProtectedOther != 0 {
		t.Fatalf("observed source segments protected other=%d want 0", stats.ObservedSourceSegmentsProtectedOther)
	}
	if stats.ObservedSourceSegmentsEligible != 0 {
		t.Fatalf("observed source segments eligible=%d want 0", stats.ObservedSourceSegmentsEligible)
	}
	if stats.ObservedSourceSegmentsDeleted != 0 {
		t.Fatalf("observed source segments deleted=%d want 0", stats.ObservedSourceSegmentsDeleted)
	}
	if stats.ObservedSourceSegmentsPending != 0 {
		t.Fatalf("observed source segments pending=%d want 0", stats.ObservedSourceSegmentsPending)
	}
	if stats.ObservedSourceBytes <= 0 {
		t.Fatalf("observed source bytes=%d want >0", stats.ObservedSourceBytes)
	}
	if stats.ObservedSourceBytesProtected <= 0 {
		t.Fatalf("observed source bytes protected=%d want >0", stats.ObservedSourceBytesProtected)
	}
	if stats.ObservedSourceBytesProtectedInUse <= 0 ||
		stats.ObservedSourceBytesProtectedRetained <= 0 ||
		stats.ObservedSourceBytesProtectedOverlap <= 0 {
		t.Fatalf("expected non-zero observed source protected byte buckets, got %+v", stats)
	}
	if stats.ObservedSourceBytesProtectedOther != 0 {
		t.Fatalf("observed source bytes protected other=%d want 0", stats.ObservedSourceBytesProtectedOther)
	}
	if stats.ObservedSourceBytesEligible != 0 {
		t.Fatalf("observed source bytes eligible=%d want 0", stats.ObservedSourceBytesEligible)
	}
	if stats.ObservedSourceBytesDeleted != 0 {
		t.Fatalf("observed source bytes deleted=%d want 0", stats.ObservedSourceBytesDeleted)
	}
	if stats.ObservedSourceBytesPending != 0 {
		t.Fatalf("observed source bytes pending=%d want 0", stats.ObservedSourceBytesPending)
	}
}

func TestValueLogGC_KeepsReferencedPointerSegments_WithOuterLeavesInValueLog(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	// Use an otherwise-unused lane so TreeDB can create its own lane0 segments
	// for outer leaves without colliding with this test's explicit segments.
	const lane uint32 = 100

	path1 := filepath.Join(walDir, "value-l100-000001.log")
	id1, err := valuelog.EncodeFileID(lane, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	ptr1, err := w1.Append(0, nil, 1, bytes.Repeat([]byte("value-1|"), 64))
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}
	registerTestValueLogProducer(t, dir, path1, id1)

	path2 := filepath.Join(walDir, "value-l100-000002.log")
	id2, err := valuelog.EncodeFileID(lane, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	ptr2, err := w2.Append(0, nil, 2, bytes.Repeat([]byte("value-2|"), 64))
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}
	registerTestValueLogProducer(t, dir, path2, id2)

	db, err := Open(Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		LeafPrefixCompression:      true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := newRewriteWriter(filepath.Join(dir, "value_vlog"), 0, 0, 0)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)
	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	// Lane100 seq2 is active; the GC must still treat the older referenced
	// segment (seq1) as referenced and keep it.
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected referenced non-active segment to remain, err=%v", err)
	}

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("value-1|"), 64)) {
		t.Fatalf("k1 mismatch after GC")
	}
}

func TestValueLogGC_IncrementalParityWithFullScan(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	seedValueLogRefIncrementalParityFixture(t, db, dir, false)

	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}

	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}

	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%d full=%d", len(incRefs), len(fullRefs))
	}
}

type valueLogRefIncrementalParityFixture struct {
	SegmentAFileID     uint32
	SegmentBFileID     uint32
	UnreferencedFileID uint32
}

func seedValueLogRefIncrementalParityFixture(t *testing.T, db *DB, dir string, includeUnreferenced bool) valueLogRefIncrementalParityFixture {
	t.Helper()

	key := func(i int) []byte { return []byte(fmt.Sprintf("k%06d", i)) }
	valueA := func(i int) []byte { return bytes.Repeat([]byte{byte(i % 251)}, 256) }
	valueB := func(i int) []byte { return bytes.Repeat([]byte{byte((i + 17) % 251)}, 512) }

	ptrsA := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 320, valueA)
	ptrsB := appendPointersInNewSegment(t, dir, 0, 2, 20_000, 120, valueB)
	var unreferencedFileID uint32
	if includeUnreferenced {
		unreferenced := appendPointersInNewSegment(t, dir, 0, 3, 30_000, 1, func(int) []byte {
			return bytes.Repeat([]byte("unreferenced|"), 32)
		})
		unreferencedFileID = unreferenced[0].FileID
	}

	b := db.NewBatch().(*Batch)
	for i := 0; i < 320; i++ {
		if err := b.SetPointer(key(i), ptrsA[i]); err != nil {
			t.Fatalf("set initial pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write initial: %v", err)
	}
	_ = b.Close()

	b = db.NewBatch().(*Batch)
	if err := b.DeleteRange(key(90), key(120)); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	for i := 0; i < 90; i++ {
		if err := b.Delete(key(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := b.Delete(key(100)); err != nil {
		t.Fatalf("delete inside range: %v", err)
	}
	for i := 160; i < 280; i++ {
		if err := b.SetPointer(key(i), ptrsB[i-160]); err != nil {
			t.Fatalf("set overwrite pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write churn: %v", err)
	}
	_ = b.Close()

	return valueLogRefIncrementalParityFixture{
		SegmentAFileID:     ptrsA[0].FileID,
		SegmentBFileID:     ptrsB[0].FileID,
		UnreferencedFileID: unreferencedFileID,
	}
}

func TestValueLogGC_IncrementalTrackerPersistsOnClose(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 25_000, 3, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("k%02d", i)), ptrs[i]); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	seq := db.currentCommitSeq()
	if _, ok := db.valueLogRefTracker.referencedSet(seq); !ok {
		t.Fatalf("expected dirty incremental tracker for seq=%d", seq)
	}
	metaPath := db.valueLogRefCountsPath()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	disk, err := decodeValueLogRefCounts(data)
	if err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if disk.commitSeq != seq {
		t.Fatalf("metadata seq mismatch after close: got=%d want=%d", disk.commitSeq, seq)
	}
	if got := disk.counts[ptrs[0].FileID]; got != uint64(len(ptrs)) {
		t.Fatalf("metadata file refcount mismatch: got=%d want=%d", got, len(ptrs))
	}

	db, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); !ok {
		t.Fatalf("expected persisted tracker to load on reopen")
	}
}

func TestValueLogGC_IncrementalCounterRollbackOnFailedCommit(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldValue := bytes.Repeat([]byte("old"), 128)
	newValue := bytes.Repeat([]byte("new"), 128)
	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 30_000, 1, func(int) []byte { return oldValue })[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 40_000, 1, func(int) []byte { return newValue })[0]

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), oldPtr); err != nil {
		t.Fatalf("seed set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	beforeSeq := db.currentCommitSeq()
	beforeRefs, ok := db.valueLogRefTracker.referencedSet(beforeSeq)
	if !ok {
		t.Fatalf("expected incremental ref set before failpoint seq=%d", beforeSeq)
	}

	db.testFailFinalizeCommit.Store(true)
	b = db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), newPtr); err != nil {
		t.Fatalf("failpoint set pointer: %v", err)
	}
	err = b.Write()
	_ = b.Close()
	db.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("expected failpoint error, got %v", err)
	}

	afterSeq := db.currentCommitSeq()
	if afterSeq != beforeSeq {
		t.Fatalf("commit seq changed on failed commit: before=%d after=%d", beforeSeq, afterSeq)
	}
	afterRefs, ok := db.valueLogRefTracker.referencedSet(afterSeq)
	if !ok {
		t.Fatalf("expected incremental ref set after failpoint seq=%d", afterSeq)
	}
	if !reflect.DeepEqual(beforeRefs, afterRefs) {
		t.Fatalf("ref set changed on failed commit")
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get after failed commit: %v", err)
	}
	if !bytes.Equal(got, oldValue) {
		t.Fatalf("value changed on failed commit")
	}
}

func TestValueLogGC_IncrementalMode_RebuildOnCorruption(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 50_000, 200, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i % 251)}, 256)
	})

	b := db.NewBatch().(*Batch)
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		if err := b.SetPointer(key, ptrs[i]); err != nil {
			t.Fatalf("seed set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC dry-run: %v", err)
	}

	metaPath := db.valueLogRefCountsPath()
	orig, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if len(orig) == 0 {
		t.Fatalf("expected non-empty metadata")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	corrupt := []byte("not-a-valid-metadata-file")
	if err := os.WriteFile(metaPath, corrupt, 0o644); err != nil {
		t.Fatalf("write corrupt metadata: %v", err)
	}

	db, err = Open(Options{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	seq := db.currentCommitSeq()
	if _, ok := db.valueLogRefTracker.referencedSet(seq); !ok {
		t.Fatalf("expected ref tracker rebuilt for seq=%d", seq)
	}

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC after rebuild: %v", err)
	}

	after, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read rebuilt metadata: %v", err)
	}
	if bytes.Equal(after, corrupt) {
		t.Fatalf("expected metadata rewrite after corruption")
	}
	if _, err := decodeValueLogRefCounts(after); err != nil {
		t.Fatalf("rebuilt metadata decode: %v", err)
	}
}

func TestValueLogGC_HealthMetadata_UpdatesAfterDeleteAndRewrite(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	ptrsA := appendPointersInNewSegment(t, dir, 0, 1, 70_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 512)
	})
	ptrsB := appendPointersInNewSegment(t, dir, 0, 2, 80_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 512)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrsA[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrsB[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}
	advancePastRetainedDurableSlotForTest(t, db)
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	healthPath := valueLogHealthPath(dir)
	health, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after GC: %v", err)
	}
	if _, ok := health[ptrsA[0].FileID]; ok {
		t.Fatalf("expected deleted segment %d removed from health metadata", ptrsA[0].FileID)
	}
	h2, ok := health[ptrsB[0].FileID]
	if !ok {
		t.Fatalf("expected referenced segment %d in health metadata", ptrsB[0].FileID)
	}
	if h2.SegmentBytes <= 0 {
		t.Fatalf("expected positive segment bytes for segment %d, got %+v", ptrsB[0].FileID, h2)
	}
	if h2.LiveBytes < 0 || h2.LiveBytes > h2.SegmentBytes {
		t.Fatalf("expected bounded live bytes for segment %d, got %+v", ptrsB[0].FileID, h2)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = nil
	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.SegmentsAfter == 0 {
		t.Fatalf("expected rewritten segments, got %+v", stats)
	}

	healthAfterRewrite, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after rewrite: %v", err)
	}
	if len(healthAfterRewrite) == 0 {
		t.Fatalf("expected health metadata entries after rewrite")
	}
	foundRewrite := false
	for _, h := range healthAfterRewrite {
		if h.RewriteCount > 0 {
			foundRewrite = true
			break
		}
	}
	if !foundRewrite {
		t.Fatalf("expected rewrite_count > 0 after rewrite, got %+v", healthAfterRewrite)
	}
}

func TestValueLogGC_HealthMetadata_PreservesPinnedZombieSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ptrsA := appendPointersInNewSegment(t, dir, 0, 1, 81_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrsB := appendPointersInNewSegment(t, dir, 0, 2, 82_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrsA[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrsB[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	pinned := db.valueLogManager.CurrentSetNoRefresh()
	if pinned == nil {
		t.Fatalf("expected pinned value-log set")
	}
	defer func() { _ = db.valueLogManager.Release(pinned) }()

	zombieFile, ok := pinned.Files[ptrsA[0].FileID]
	if !ok || zombieFile == nil || zombieFile.Path == "" {
		t.Fatalf("missing pinned segment for %d", ptrsA[0].FileID)
	}

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if _, err := os.Stat(zombieFile.Path); err != nil {
		t.Fatalf("expected pinned zombie segment to remain on disk: %v", err)
	}

	health, err := loadValueLogHealth(valueLogHealthPath(dir))
	if err != nil {
		t.Fatalf("load health after gc: %v", err)
	}
	h, ok := health[ptrsA[0].FileID]
	if !ok {
		t.Fatalf("expected health entry for pinned zombie segment %d", ptrsA[0].FileID)
	}
	if h.SegmentBytes <= 0 {
		t.Fatalf("expected positive segment bytes for pinned zombie segment: %+v", h)
	}
	if h.LiveBytes != 0 {
		t.Fatalf("expected zero live bytes for unreferenced pinned zombie segment: %+v", h)
	}
}

func valueLogRefSetFromCounts(counts map[uint32]uint64) map[uint32]struct{} {
	out := make(map[uint32]struct{}, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		out[fileID] = struct{}{}
	}
	return out
}

// testValueLogProducers models the producer side of tests that construct raw
// value-log segments. #3679 deliberately removed publisher path discovery, so
// the fixture that owns the writer must register its exact segment before a
// pointer can become reachable. Segments created before Open are handed to the
// first matching DB handle; registrations are never carried across reopen.
var testValueLogProducers = struct {
	sync.Mutex
	databases map[string]*DB
	pending   map[string]map[uint32]string
}{
	databases: make(map[string]*DB),
	pending:   make(map[string]map[uint32]string),
}

func init() {
	testDBOpenHook = func(database *DB) error {
		dir, err := filepath.Abs(database.dir)
		if err != nil {
			return err
		}
		dir = filepath.Clean(dir)
		testValueLogProducers.Lock()
		testValueLogProducers.databases[dir] = database
		pending := testValueLogProducers.pending[dir]
		delete(testValueLogProducers.pending, dir)
		testValueLogProducers.Unlock()
		for fileID, path := range pending {
			if err := database.RegisterValueLogSegment(path, fileID); err != nil {
				return fmt.Errorf("register test value-log producer file %d: %w", fileID, err)
			}
		}
		return nil
	}
	testDBCloseHook = func(database *DB) {
		dir, err := filepath.Abs(database.dir)
		if err != nil {
			return
		}
		dir = filepath.Clean(dir)
		testValueLogProducers.Lock()
		if testValueLogProducers.databases[dir] == database {
			delete(testValueLogProducers.databases, dir)
		}
		testValueLogProducers.Unlock()
	}
}

func registerTestValueLogProducer(tb testing.TB, dir, path string, fileID uint32) {
	tb.Helper()
	absDir, err := filepath.Abs(dir)
	if err != nil {
		tb.Fatalf("absolute test value-log producer directory: %v", err)
	}
	absDir = filepath.Clean(absDir)
	testValueLogProducers.Lock()
	database := testValueLogProducers.databases[absDir]
	if database == nil {
		pending := testValueLogProducers.pending[absDir]
		if pending == nil {
			pending = make(map[uint32]string)
			testValueLogProducers.pending[absDir] = pending
		}
		pending[fileID] = path
		testValueLogProducers.Unlock()
		return
	}
	testValueLogProducers.Unlock()
	if err := database.RegisterValueLogSegment(path, fileID); err != nil {
		tb.Fatalf("register test value-log producer file %d: %v", fileID, err)
	}
}

func appendPointersInNewSegment(t *testing.T, dir string, lane, seq uint32, ridBase uint64, n int, valueAt func(i int) []byte) []page.ValuePtr {
	t.Helper()
	ptrs, path, fileID := appendPointersInUnregisteredNewSegment(t, dir, lane, seq, ridBase, n, valueAt)
	registerTestValueLogProducer(t, dir, path, fileID)
	return ptrs
}

func appendPointersInUnregisteredNewSegment(t *testing.T, dir string, lane, seq uint32, ridBase uint64, n int, valueAt func(i int) []byte) ([]page.ValuePtr, string, uint32) {
	t.Helper()
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("encode file id lane=%d seq=%d: %v", lane, seq, err)
	}
	path := filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrs := make([]page.ValuePtr, 0, n)
	for i := 0; i < n; i++ {
		ptr, err := w.Append(0, nil, ridBase+uint64(i), valueAt(i))
		if err != nil {
			t.Fatalf("append rid=%d: %v", ridBase+uint64(i), err)
		}
		ptrs = append(ptrs, ptr)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return ptrs, path, fileID
}
