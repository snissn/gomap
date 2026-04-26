package caching

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func newAdaptiveStatsDB(mode memtable.Mode) *DB {
	db := &DB{
		memtableAdaptive: true,
		memtableStats:    memtableStats{},
	}
	db.storeMemtableMode(mode)
	db.memtableAdaptiveObserve.Store(true)
	return db
}

func TestAtomicStats(t *testing.T) {
	// Setup a minimal DB instance with adaptive mode enabled
	db := newAdaptiveStatsDB(0)

	// 1. Verify basic increments
	db.noteWriteKey([]byte("a"))
	if got := db.memtableStats.writes.Load(); got != 1 {
		t.Errorf("writes = %d, want 1", got)
	}

	// 2. Concurrent updates
	var wg sync.WaitGroup
	workers := 10
	iterations := 1000

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Mix of writes, seq writes, iterators
				db.noteWriteKey([]byte("key"))
				db.noteIterator(nil, nil)
			}
		}()
	}
	wg.Wait()

	expectedWrites := uint64(1 + workers*iterations) // +1 from initial write
	if got := db.memtableStats.writes.Load(); got != expectedWrites {
		t.Errorf("concurrent writes = %d, want %d", got, expectedWrites)
	}
	if got := db.memtableStats.overwriteWrites.Load(); got != expectedWrites-2 {
		t.Errorf("overwrite writes = %d, want %d", got, expectedWrites-2)
	}

	expectedIters := uint64(workers * iterations)
	if got := db.memtableStats.iterators.Load(); got != expectedIters {
		t.Errorf("concurrent iterators = %d, want %d", got, expectedIters)
	}

	db.noteDeleteKey([]byte("delete-key"))
	if got := db.memtableStats.deleteWrites.Load(); got != 1 {
		t.Errorf("delete writes = %d, want 1", got)
	}
}

func TestNoteWriteKeyDeadlock(t *testing.T) {
	// This test simulates high contention on noteWriteKey to check for
	// deadlocks or race conditions in the new fine-grained locking.
	db := newAdaptiveStatsDB(0)

	done := make(chan struct{})
	go func() {
		time.Sleep(2 * time.Second)
		close(done)
	}()

	var wg sync.WaitGroup
	workers := 20 // High enough to force contention

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			key := make([]byte, 16)

			for {
				select {
				case <-done:
					return
				default:
					rng.Read(key)
					db.noteWriteKey(key)
					// Simulate occasionally resetting stats (like a rotation would)
					if rng.Intn(100) == 0 {
						db.memtableStats.writes.Store(0)
						db.memtableStats.overwriteWrites.Store(0)
						db.memtableStats.lastKeyMu.Lock()
						db.memtableStats.hasLastKey = false
						db.memtableStats.lastKeyMu.Unlock()
					}
				}
			}
		}(i)
	}

	// If this hangs, the test framework will time out
	wg.Wait()
}

func TestChooseAdaptiveMode(t *testing.T) {
	db := newAdaptiveStatsDB(memtable.ModeSkiplist)

	// 1. Not enough data -> default
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeSkiplist {
		t.Errorf("low data mode = %v, want %v", got, memtable.ModeSkiplist)
	}

	// 2. High sequential with low overwrites -> AppendOnly
	// adaptiveMinWrites writes, 90% sequential
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites * 9 / 10)
	db.memtableStats.overwriteWrites.Store(0)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeAppendOnly {
		t.Errorf("sequential mode = %v, want %v", got, memtable.ModeAppendOnly)
	}

	// 3. High range scans -> BTree
	// Reset
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10) // Low seq
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(100)
	db.memtableStats.rangeIters.Store(80) // 80% range
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeBTree {
		t.Errorf("range scan mode = %v, want %v", got, memtable.ModeBTree)
	}

	// 4. High overwrites -> HashSorted
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites)
	db.memtableStats.overwriteWrites.Store(adaptiveMinWrites / 2)
	db.memtableStats.iterators.Store(0)
	db.memtableStats.rangeIters.Store(0)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeHashSorted {
		t.Errorf("overwrite mode = %v, want %v", got, memtable.ModeHashSorted)
	}

	// 5. Delete-heavy traffic -> AppendOnly. Tombstones are cheap to append and
	// can be coalesced during flush; hash-sorted insertion is wasted churn here.
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(0)
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.deleteWrites.Store(adaptiveMinWrites)
	db.memtableStats.iterators.Store(0)
	db.memtableStats.rangeIters.Store(0)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeAppendOnly {
		t.Errorf("delete-heavy mode = %v, want %v", got, memtable.ModeAppendOnly)
	}
	if got := adaptiveDecisionReasonString(db.memtableAdaptiveDecisionReason.Load()); got != "append_deletes" {
		t.Errorf("delete-heavy reason=%q want append_deletes", got)
	}

	// 6. Low-overwrite write-heavy traffic -> AppendOnly even when not sequential.
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(0)
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.deleteWrites.Store(0)
	db.memtableStats.iterators.Store(0)
	db.memtableStats.rangeIters.Store(0)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeAppendOnly {
		t.Errorf("write-heavy mode = %v, want %v", got, memtable.ModeAppendOnly)
	}
	if got := adaptiveDecisionReasonString(db.memtableAdaptiveDecisionReason.Load()); got != "append_write_heavy" {
		t.Errorf("write-heavy reason=%q want append_write_heavy", got)
	}
}

func TestChooseAdaptiveMode_BTreeMinIteratorSamplesOverride(t *testing.T) {
	db := newAdaptiveStatsDB(memtable.ModeSkiplist)
	db.memtableAdaptiveBTreeMinIters = 64

	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10)
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(32)
	db.memtableStats.rangeIters.Store(32)

	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeHashSorted {
		t.Fatalf("blocked-btree mode = %v, want %v", got, memtable.ModeHashSorted)
	}
	if got := db.memtableAdaptiveDecisionBTreeBlockedMinItersTotal.Load(); got != 1 {
		t.Fatalf("blocked-min-iters count=%d want 1", got)
	}
	if got := adaptiveDecisionReasonString(db.memtableAdaptiveDecisionReason.Load()); got != "btree_blocked_min_iters" {
		t.Fatalf("last reason=%q want btree_blocked_min_iters", got)
	}

	db.memtableStats.iterators.Store(128)
	db.memtableStats.rangeIters.Store(96)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeBTree {
		t.Fatalf("range-heavy mode = %v, want %v", got, memtable.ModeBTree)
	}
	if got := db.memtableAdaptiveDecisionBTreeTotal.Load(); got != 1 {
		t.Fatalf("btree decision count=%d want 1", got)
	}
	if got := adaptiveDecisionReasonString(db.memtableAdaptiveDecisionReason.Load()); got != "btree_range" {
		t.Fatalf("last reason=%q want btree_range", got)
	}
}

func TestAdaptiveObservationContinuesInAppendOnly(t *testing.T) {
	db := newAdaptiveStatsDB(memtable.ModeAppendOnly)
	db.memtableWarmupActive = false

	db.updateAdaptiveObservationLocked()
	if !db.memtableAdaptiveObserve.Load() {
		t.Fatalf("expected adaptive observation to remain enabled in append_only mode")
	}

	db.noteWriteKey([]byte("a"))
	db.noteWriteKey([]byte("b"))
	db.noteIterator([]byte("a"), []byte("c"))

	if got := db.memtableStats.writes.Load(); got != 2 {
		t.Fatalf("writes=%d want 2", got)
	}
	if got := db.memtableStats.seqWrites.Load(); got != 1 {
		t.Fatalf("seqWrites=%d want 1", got)
	}
	if got := db.memtableStats.iterators.Load(); got != 1 {
		t.Fatalf("iterators=%d want 1", got)
	}
	if got := db.memtableStats.rangeIters.Load(); got != 1 {
		t.Fatalf("rangeIters=%d want 1", got)
	}
}

func TestChooseAdaptiveMode_LowOverwriteMixedWritesPreferAppendOnly(t *testing.T) {
	db := newAdaptiveStatsDB(memtable.ModeAppendOnly)

	for i := adaptiveTailSequentialWriteMin - 1; i >= 0; i-- {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		db.noteWriteKey(key[:])
	}
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeAppendOnly {
		t.Fatalf("descending low-overwrite phase mode=%v want %v", got, memtable.ModeAppendOnly)
	}
}

func TestShouldRotateForDeleteHeavyRecovery(t *testing.T) {
	db := newAdaptiveStatsDB(memtable.ModeHashSorted)
	for i := 0; i < adaptiveMinWrites; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		db.noteDeleteKey(key[:])
	}

	if !db.shouldRotateForDeleteHeavyRecovery() {
		t.Fatal("expected delete-heavy adaptive stats to request append-only recovery from hash_sorted")
	}

	db.storeMemtableMode(memtable.ModeAppendOnly)
	if db.shouldRotateForDeleteHeavyRecovery() {
		t.Fatal("did not expect delete-heavy recovery when already in append_only mode")
	}
}

func TestNoteWriteSortedRunMatchesPerKey_NoPrior(t *testing.T) {
	perKey := newAdaptiveStatsDB(0)
	perKey.noteWriteKey([]byte("a"))
	perKey.noteWriteKey([]byte("b"))
	perKey.noteWriteKey([]byte("c"))

	run := newAdaptiveStatsDB(0)
	run.noteWriteSortedRun([]byte("a"), []byte("c"), 3)

	assertStatsEquivalent(t, perKey, run)
}

func TestNoteDeleteSortedRunMatchesPerKey(t *testing.T) {
	perKey := newAdaptiveStatsDB(0)
	perKey.noteDeleteKey([]byte("a"))
	perKey.noteDeleteKey([]byte("b"))
	perKey.noteDeleteKey([]byte("c"))

	run := newAdaptiveStatsDB(0)
	run.noteDeleteSortedRun([]byte("a"), []byte("c"), 3)

	assertStatsEquivalent(t, perKey, run)
}

func TestNoteDeleteBatchRecordsDeleteCountWithoutOrderTracking(t *testing.T) {
	db := newAdaptiveStatsDB(0)
	db.noteWriteKey([]byte("prior"))
	db.noteDeleteBatch(7)

	if got := db.memtableStats.writes.Load(); got != 8 {
		t.Fatalf("writes=%d want 8", got)
	}
	if got := db.memtableStats.deleteWrites.Load(); got != 7 {
		t.Fatalf("deleteWrites=%d want 7", got)
	}
	if got := db.memtableStats.currentSeqRun.Load(); got != 0 {
		t.Fatalf("currentSeqRun=%d want 0", got)
	}
	db.memtableStats.lastKeyMu.Lock()
	hasLast := db.memtableStats.hasLastKey
	db.memtableStats.lastKeyMu.Unlock()
	if hasLast {
		t.Fatal("expected aggregate delete batch to clear last-key tracking")
	}
}

func TestNoteWriteSortedRunWithDeletesRecordsMixedDeleteCount(t *testing.T) {
	db := newAdaptiveStatsDB(0)
	db.noteWriteSortedRunWithDeletes([]byte("a"), []byte("d"), 4, 2)

	if got := db.memtableStats.writes.Load(); got != 4 {
		t.Fatalf("writes=%d want 4", got)
	}
	if got := db.memtableStats.deleteWrites.Load(); got != 2 {
		t.Fatalf("deleteWrites=%d want 2", got)
	}
	if got := db.memtableStats.seqWrites.Load(); got != 3 {
		t.Fatalf("seqWrites=%d want 3", got)
	}
}

func TestNoteWriteSortedRunMatchesPerKey_WithPriorSmaller(t *testing.T) {
	perKey := newAdaptiveStatsDB(0)
	perKey.noteWriteKey([]byte("c"))
	perKey.noteWriteKey([]byte("d"))
	perKey.noteWriteKey([]byte("e"))
	perKey.noteWriteKey([]byte("f"))

	run := newAdaptiveStatsDB(0)
	run.noteWriteKey([]byte("c"))
	run.noteWriteSortedRun([]byte("d"), []byte("f"), 3)

	assertStatsEquivalent(t, perKey, run)
}

func TestNoteWriteSortedRunMatchesPerKey_WithPriorGreater(t *testing.T) {
	perKey := newAdaptiveStatsDB(0)
	perKey.noteWriteKey([]byte("z"))
	perKey.noteWriteKey([]byte("a"))
	perKey.noteWriteKey([]byte("b"))

	run := newAdaptiveStatsDB(0)
	run.noteWriteKey([]byte("z"))
	run.noteWriteSortedRun([]byte("a"), []byte("b"), 2)

	assertStatsEquivalent(t, perKey, run)
}

func assertStatsEquivalent(t *testing.T, want, got *DB) {
	t.Helper()

	if w, g := want.memtableStats.writes.Load(), got.memtableStats.writes.Load(); w != g {
		t.Fatalf("writes mismatch: want=%d got=%d", w, g)
	}
	if w, g := want.memtableStats.seqWrites.Load(), got.memtableStats.seqWrites.Load(); w != g {
		t.Fatalf("seqWrites mismatch: want=%d got=%d", w, g)
	}
	if w, g := want.memtableStats.overwriteWrites.Load(), got.memtableStats.overwriteWrites.Load(); w != g {
		t.Fatalf("overwriteWrites mismatch: want=%d got=%d", w, g)
	}
	if w, g := want.memtableStats.deleteWrites.Load(), got.memtableStats.deleteWrites.Load(); w != g {
		t.Fatalf("deleteWrites mismatch: want=%d got=%d", w, g)
	}
	if w, g := want.memtableStats.currentSeqRun.Load(), got.memtableStats.currentSeqRun.Load(); w != g {
		t.Fatalf("currentSeqRun mismatch: want=%d got=%d", w, g)
	}

	want.memtableStats.lastKeyMu.Lock()
	wantLastKey := append([]byte(nil), want.memtableStats.lastKey...)
	wantHasLastKey := want.memtableStats.hasLastKey
	want.memtableStats.lastKeyMu.Unlock()

	got.memtableStats.lastKeyMu.Lock()
	gotLastKey := append([]byte(nil), got.memtableStats.lastKey...)
	gotHasLastKey := got.memtableStats.hasLastKey
	got.memtableStats.lastKeyMu.Unlock()

	if wantHasLastKey != gotHasLastKey {
		t.Fatalf("hasLastKey mismatch: want=%v got=%v", wantHasLastKey, gotHasLastKey)
	}
	if string(wantLastKey) != string(gotLastKey) {
		t.Fatalf("lastKey mismatch: want=%q got=%q", string(wantLastKey), string(gotLastKey))
	}
}
