package caching

import (
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

	// 3. Shallow range scans should not force BTree or hash-sorted on a small
	// absolute sample when overwrite pressure is low.
	// Reset
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10) // Low seq
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(100)
	db.memtableStats.rangeIters.Store(80) // 80% range
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeAppendOnly {
		t.Errorf("shallow range scan mode = %v, want %v", got, memtable.ModeAppendOnly)
	}

	// 4. High range scans with enough absolute samples -> BTree
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10) // Low seq
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(adaptiveBTreeMinIteratorSamplesDefault * 2)
	db.memtableStats.rangeIters.Store(adaptiveBTreeMinIteratorSamplesDefault)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeBTree {
		t.Errorf("range scan mode = %v, want %v", got, memtable.ModeBTree)
	}

	// 5. Blocked BTree with high overwrites -> HashSorted
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10)
	db.memtableStats.overwriteWrites.Store(adaptiveMinWrites / 2)
	db.memtableStats.iterators.Store(100)
	db.memtableStats.rangeIters.Store(80)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeHashSorted {
		t.Errorf("blocked range overwrite mode = %v, want %v", got, memtable.ModeHashSorted)
	}

	// 6. High overwrites -> HashSorted
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites)
	db.memtableStats.overwriteWrites.Store(adaptiveMinWrites / 2)
	db.memtableStats.iterators.Store(0)
	db.memtableStats.rangeIters.Store(0)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeHashSorted {
		t.Errorf("overwrite mode = %v, want %v", got, memtable.ModeHashSorted)
	}
}

func TestChooseAdaptiveMode_BTreeMinIteratorSamplesExplicitZero(t *testing.T) {
	db := newAdaptiveStatsDB(memtable.ModeSkiplist)
	db.memtableAdaptiveBTreeMinItersSet = true

	if got := db.adaptiveBTreeMinIteratorSamples(); got != 0 {
		t.Fatalf("effective min iterator samples=%d want 0", got)
	}

	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10)
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(100)
	db.memtableStats.rangeIters.Store(80)

	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeBTree {
		t.Fatalf("explicit-zero range-heavy mode = %v, want %v", got, memtable.ModeBTree)
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

	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeAppendOnly {
		t.Fatalf("blocked-btree mode = %v, want %v", got, memtable.ModeAppendOnly)
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

func TestMutableMemtableCapacityForMode_CapsHashSortedPrealloc(t *testing.T) {
	oversized := hashSortedMutablePreallocCap * 32
	if got := mutableMemtableCapacityForMode(oversized, memtable.ModeHashSorted); got != hashSortedMutablePreallocCap {
		t.Fatalf("hash_sorted capacity hint=%d want %d", got, hashSortedMutablePreallocCap)
	}
	if got := mutableMemtableCapacityForMode(oversized, memtable.ModeAppendOnly); got != oversized {
		t.Fatalf("append_only capacity hint=%d want %d", got, oversized)
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
