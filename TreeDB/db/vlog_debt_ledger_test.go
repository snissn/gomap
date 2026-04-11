package db

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestValueLogDebtLedger_RebuildAndLoad(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 310_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 320_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	liveByID, ok, err := db.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected debt ledger live bytes to be available")
	}
	if liveByID[ptrs1[0].FileID] <= 0 || liveByID[ptrs2[0].FileID] <= 0 {
		t.Fatalf("unexpected live bytes map: %+v", liveByID)
	}

	path := filepath.Join(dir, valueLogDebtLedgerFileName)
	if disk, ok, err := loadValueLogDebtLedgerFromPath(path, db.currentCommitSeq()); err != nil {
		t.Fatalf("load persisted debt ledger: %v", err)
	} else if !ok {
		t.Fatalf("expected persisted debt ledger to load")
	} else if len(disk.Records) == 0 {
		t.Fatalf("expected persisted debt ledger to include record refs")
	}
}

func TestReferencedValueLogSegments_UsesPersistedDebtLedgerAcrossReopen(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 350_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 350_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	closeNoErr(t, db)

	var legacyCalls atomic.Uint64
	unregister := registerReferencedValueLogSegmentsLegacyHook(func() {
		legacyCalls.Add(1)
	})
	t.Cleanup(unregister)

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, db2)
	refs, err := db2.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if _, ok := refs[ptrs1[0].FileID]; !ok {
		t.Fatalf("missing referenced file %d in %+v", ptrs1[0].FileID, refs)
	}
	if _, ok := refs[ptrs2[0].FileID]; !ok {
		t.Fatalf("missing referenced file %d in %+v", ptrs2[0].FileID, refs)
	}
	if got := legacyCalls.Load(); got != 0 {
		t.Fatalf("legacy referenced-segment scan calls=%d want 0", got)
	}
}

func TestValueLogDebtLedger_TracksCommitDeltaAcrossPointerWrites(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 10, 360_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 11, 361_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	if !db.valueLogDebtLedger.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected debt ledger to become trackable after rebuild")
	}

	ptrs3 := appendPointersInNewSegment(t, dir, 0, 12, 362_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("c"), 256)
	})
	b = db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs3[0]); err != nil {
		t.Fatalf("update k1: %v", err)
	}
	if err := b.Delete([]byte("k2")); err != nil {
		t.Fatalf("delete k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("delta write: %v", err)
	}
	closeNoErr(t, b)

	if !db.valueLogDebtLedger.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected debt ledger to stay trackable after pointer delta commit")
	}
	ledgerLive, ok, err := db.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected live bytes from debt ledger after delta commit")
	}
	scanLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimate live bytes: %v", err)
	}
	if !sameValueLogLiveBytesBySegment(ledgerLive, scanLive) {
		t.Fatalf("debt ledger live bytes mismatch after pointer delta: ledger=%+v scan=%+v", ledgerLive, scanLive)
	}
}

func TestValueLogDebtLedger_PersistsTrackableStateAcrossReopen(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 20, 363_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 21, 364_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	ptrs3 := appendPointersInNewSegment(t, dir, 0, 22, 365_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("c"), 256)
	})
	b = db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs3[0]); err != nil {
		t.Fatalf("update k1: %v", err)
	}
	if err := b.Delete([]byte("k2")); err != nil {
		t.Fatalf("delete k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("delta write: %v", err)
	}
	closeNoErr(t, b)
	closeNoErr(t, db)

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, db2)

	if !db2.valueLogDebtLedger.canTrack(db2.currentCommitSeq()) {
		t.Fatalf("expected reopened debt ledger to stay trackable without rebuild")
	}
	ledgerLive, ok, err := db2.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected reopened debt ledger live bytes")
	}
	scanLive, err := db2.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimate live bytes: %v", err)
	}
	if !sameValueLogLiveBytesBySegment(ledgerLive, scanLive) {
		t.Fatalf("reopened debt ledger live bytes mismatch: ledger=%+v scan=%+v", ledgerLive, scanLive)
	}

	ptrs4 := appendPointersInNewSegment(t, dir, 0, 23, 366_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("d"), 256)
	})
	b = db2.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs4[0]); err != nil {
		t.Fatalf("second update k1: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("second delta write: %v", err)
	}
	closeNoErr(t, b)

	if !db2.valueLogDebtLedger.canTrack(db2.currentCommitSeq()) {
		t.Fatalf("expected reopened debt ledger to remain trackable after new commit")
	}
	ledgerLive, ok, err = db2.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("post-reopen live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected post-reopen debt ledger live bytes")
	}
	scanLive, err = db2.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("post-reopen estimate live bytes: %v", err)
	}
	if !sameValueLogLiveBytesBySegment(ledgerLive, scanLive) {
		t.Fatalf("post-reopen debt ledger live bytes mismatch: ledger=%+v scan=%+v", ledgerLive, scanLive)
	}
}

func TestValueLogDebtLedger_TracksOuterLeafCommitDeltaAcrossWrites(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	leafLog := &registeredLeafPageLog{db: db, dir: dir}
	db.SetLeafPageLog(leafLog)
	db.idx.Load().zipper.SetLeafPageReader(db.valueLogManager)

	value := bytes.Repeat([]byte("outer-leaf-value-"), 4)
	b := db.NewBatch().(*Batch)
	for i := 0; i < 512; i++ {
		if err := b.Set([]byte(fmt.Sprintf("k%04d", i)), value); err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value-log set: %v", err)
	}

	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	if !db.valueLogDebtLedger.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected debt ledger to become trackable after outer-leaf rebuild")
	}
	beforeLeafCounts := make(map[uint32]int, 16)
	stateBefore := db.State()
	collectLeafRefFileCountsBench(t, db.Pager(), stateBefore.RootPageID, beforeLeafCounts)
	collectLeafRefFileCountsBench(t, db.Pager(), stateBefore.SystemRootPageID, beforeLeafCounts)
	db.valueLogDebtLedger.mu.RLock()
	for fileID := range beforeLeafCounts {
		if db.valueLogDebtLedger.segments[fileID].OuterLeafLiveBytes == 0 {
			db.valueLogDebtLedger.mu.RUnlock()
			t.Fatalf("expected tracked outer-leaf live bytes for file %d after rebuild", fileID)
		}
	}
	db.valueLogDebtLedger.mu.RUnlock()

	b = db.NewBatch().(*Batch)
	for i := 0; i < 64; i++ {
		if err := b.Set([]byte(fmt.Sprintf("k%04d", i)), append(value, byte(i))); err != nil {
			t.Fatalf("delta set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("delta write: %v", err)
	}
	closeNoErr(t, b)

	if !db.valueLogDebtLedger.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected debt ledger to stay trackable after outer-leaf delta commit")
	}
	ledgerLive, ok, err := db.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected live bytes from debt ledger after outer-leaf delta commit")
	}
	scanLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimate live bytes: %v", err)
	}
	if !sameValueLogLiveBytesBySegment(ledgerLive, scanLive) {
		afterLeafCounts := make(map[uint32]int, 16)
		stateAfter := db.State()
		collectLeafRefFileCountsBench(t, db.Pager(), stateAfter.RootPageID, afterLeafCounts)
		collectLeafRefFileCountsBench(t, db.Pager(), stateAfter.SystemRootPageID, afterLeafCounts)
		ledgerRecordCounts := make(map[uint32]int, 16)
		for key, rec := range db.valueLogDebtLedger.records {
			if rec.RefCount == 0 {
				continue
			}
			ledgerRecordCounts[key.FileID]++
		}
		t.Fatalf("debt ledger live bytes mismatch after outer-leaf delta: ledger=%+v scan=%+v before_leaf_counts=%+v after_leaf_counts=%+v ledger_record_counts=%+v", ledgerLive, scanLive, beforeLeafCounts, afterLeafCounts, ledgerRecordCounts)
	}
	db.valueLogDebtLedger.mu.RLock()
	for fileID, n := range beforeLeafCounts {
		if n == 0 {
			continue
		}
		if db.valueLogDebtLedger.segments[fileID].OuterLeafLiveBytes == 0 {
			db.valueLogDebtLedger.mu.RUnlock()
			t.Fatalf("expected tracked outer-leaf live bytes for file %d after delta commit", fileID)
		}
	}
	db.valueLogDebtLedger.mu.RUnlock()
}

func TestValueLogDebtLedger_TracksLeafRefRewriteCommits(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	db, sourceIDs, cleanup := setupLeafRefRewriteBench(t, 768)
	defer cleanup()

	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	if !db.valueLogDebtLedger.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected debt ledger to become trackable before leafref rewrite")
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs:     sourceIDs,
		MaxSourceSegments: len(sourceIDs),
		BatchSize:         64,
		SyncEachBatch:     true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.LeafRefRecordsCopied == 0 {
		t.Fatalf("expected leafref rewrite to copy leaf pages, stats=%+v", stats)
	}
	if stats.LeafRefTreeNodesVisited == 0 || stats.LeafRefInternalNodesVisited == 0 {
		t.Fatalf("expected leafref rewrite traversal stats, stats=%+v", stats)
	}
	if stats.LeafRefRefsVisited == 0 || stats.LeafRefRefsSelected == 0 {
		t.Fatalf("expected leafref rewrite to report visited and selected refs, stats=%+v", stats)
	}
	if !db.valueLogDebtLedger.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected debt ledger to stay trackable after leafref rewrite")
	}
	ledgerLive, ok, err := db.liveBytesBySegmentFromDebtLedger(context.Background())
	if err != nil {
		t.Fatalf("live bytes from debt ledger: %v", err)
	}
	if !ok {
		t.Fatalf("expected live bytes from debt ledger after leafref rewrite")
	}
	scanLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimate live bytes: %v", err)
	}
	if !sameValueLogLiveBytesBySegment(ledgerLive, scanLive) {
		t.Fatalf("debt ledger live bytes mismatch after leafref rewrite: ledger=%+v scan=%+v", ledgerLive, scanLive)
	}
}

func TestValueLogOuterLeafChangeCollector_CapturesLeafSplit(t *testing.T) {
	t.Setenv(envEnableVlogDebtLedger, "1")
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	leafLog := &registeredLeafPageLog{db: db, dir: dir}
	db.SetLeafPageLog(leafLog)
	db.idx.Load().zipper.SetLeafPageReader(db.valueLogManager)

	value := bytes.Repeat([]byte("outer-leaf-value-"), 4)
	b := db.NewBatch().(*Batch)
	for i := 0; i < 512; i++ {
		if err := b.Set([]byte(fmt.Sprintf("k%04d", i)), value); err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	idx := db.idx.Load()
	db.mu.RLock()
	rootID := db.meta.UserRootPageID
	db.mu.RUnlock()

	b = db.NewBatch().(*Batch)
	for i := 0; i < 64; i++ {
		if err := b.Set([]byte(fmt.Sprintf("k%04d", i)), append(value, byte(i))); err != nil {
			t.Fatalf("delta set %d: %v", i, err)
		}
	}
	collector := &valueLogOuterLeafChangeCollector{}
	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	z.SetOuterLeafRecordObserver(collector.Observe)
	_, _, _, err = z.Apply(rootID, b.batch)
	if err != nil {
		closeNoErr(t, b)
		t.Fatalf("apply: %v", err)
	}
	if len(collector.oldPtrs) == 0 || len(collector.newPtrs) <= len(collector.oldPtrs) {
		closeNoErr(t, b)
		t.Fatalf("unexpected outer-leaf observer capture old=%d new=%d", len(collector.oldPtrs), len(collector.newPtrs))
	}
	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		closeNoErr(t, b)
		t.Fatalf("rebuild debt ledger: %v", err)
	}
	delta, err := db.buildValueLogDebtDelta(idx.pager, rootID, db.currentCommitSeq(), b.batch.SortedEntries(), collector)
	closeNoErr(t, b)
	if err != nil {
		t.Fatalf("build debt delta: %v", err)
	}
	if delta == nil || len(delta.changes) == 0 {
		t.Fatalf("expected non-empty debt delta from outer-leaf split")
	}
}
