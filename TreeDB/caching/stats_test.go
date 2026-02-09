package caching

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestAtomicStats(t *testing.T) {
	// Setup a minimal DB instance with adaptive mode enabled
	db := &DB{
		memtableAdaptive: true,
		memtableStats:    memtableStats{},
	}

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

	expectedIters := uint64(workers * iterations)
	if got := db.memtableStats.iterators.Load(); got != expectedIters {
		t.Errorf("concurrent iterators = %d, want %d", got, expectedIters)
	}
}

func TestNoteWriteKeyDeadlock(t *testing.T) {
	// This test simulates high contention on noteWriteKey to check for
	// deadlocks or race conditions in the new fine-grained locking.
	db := &DB{
		memtableAdaptive: true,
		memtableStats:    memtableStats{},
	}

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
	db := &DB{
		memtableAdaptive: true,
		memtableStats:    memtableStats{},
		memtableMode:     memtable.ModeSkiplist,
	}

	// 1. Not enough data -> default
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeSkiplist {
		t.Errorf("low data mode = %v, want %v", got, memtable.ModeSkiplist)
	}

	// 2. High sequential -> HashSorted
	// adaptiveMinWrites writes, 90% sequential
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites * 9 / 10)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeHashSorted {
		t.Errorf("sequential mode = %v, want %v", got, memtable.ModeHashSorted)
	}

	// 3. High range scans -> BTree
	// Reset
	db.memtableStats.writes.Store(adaptiveMinWrites)
	db.memtableStats.seqWrites.Store(adaptiveMinWrites / 10) // Low seq
	db.memtableStats.iterators.Store(100)
	db.memtableStats.rangeIters.Store(80) // 80% range
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeBTree {
		t.Errorf("range scan mode = %v, want %v", got, memtable.ModeBTree)
	}
}

func TestNoteWriteSortedRunMatchesPerKey_NoPrior(t *testing.T) {
	perKey := &DB{memtableAdaptive: true}
	perKey.noteWriteKey([]byte("a"))
	perKey.noteWriteKey([]byte("b"))
	perKey.noteWriteKey([]byte("c"))

	run := &DB{memtableAdaptive: true}
	run.noteWriteSortedRun([]byte("a"), []byte("c"), 3)

	assertStatsEquivalent(t, perKey, run)
}

func TestNoteWriteSortedRunMatchesPerKey_WithPriorSmaller(t *testing.T) {
	perKey := &DB{memtableAdaptive: true}
	perKey.noteWriteKey([]byte("c"))
	perKey.noteWriteKey([]byte("d"))
	perKey.noteWriteKey([]byte("e"))
	perKey.noteWriteKey([]byte("f"))

	run := &DB{memtableAdaptive: true}
	run.noteWriteKey([]byte("c"))
	run.noteWriteSortedRun([]byte("d"), []byte("f"), 3)

	assertStatsEquivalent(t, perKey, run)
}

func TestNoteWriteSortedRunMatchesPerKey_WithPriorGreater(t *testing.T) {
	perKey := &DB{memtableAdaptive: true}
	perKey.noteWriteKey([]byte("z"))
	perKey.noteWriteKey([]byte("a"))
	perKey.noteWriteKey([]byte("b"))

	run := &DB{memtableAdaptive: true}
	run.noteWriteKey([]byte("z"))
	run.noteWriteSortedRun([]byte("a"), []byte("b"), 2)

	assertStatsEquivalent(t, perKey, run)
}

func TestNoteWriteKeyForShardTracksSequentialRunsIndependently(t *testing.T) {
	db := &DB{
		memtableAdaptive:     true,
		adaptiveShardedStats: true,
		mutableShards:        make([]memShard, 2),
	}
	db.resetMemtableStatsLocked()

	db.noteWriteKeyForShard(0, []byte("a"))
	db.noteWriteKeyForShard(1, []byte("m"))
	db.noteWriteKeyForShard(0, []byte("b"))
	db.noteWriteKeyForShard(1, []byte("n"))

	if got := db.memtableStats.writes.Load(); got != 4 {
		t.Fatalf("writes=%d want 4", got)
	}
	if got := db.memtableStats.seqWrites.Load(); got != 2 {
		t.Fatalf("seqWrites=%d want 2", got)
	}

	if len(db.memtableStats.shardSeq) != 2 {
		t.Fatalf("shard trackers=%d want 2", len(db.memtableStats.shardSeq))
	}
	for i, wantLast := range []string{"b", "n"} {
		tracker := &db.memtableStats.shardSeq[i]
		tracker.mu.Lock()
		last := string(tracker.lastKey)
		has := tracker.hasLastKey
		tracker.mu.Unlock()
		if !has {
			t.Fatalf("shard %d hasLastKey=false want true", i)
		}
		if last != wantLast {
			t.Fatalf("shard %d lastKey=%q want %q", i, last, wantLast)
		}
	}
}

func TestNoteWriteSortedRunForShardMatchesPerKey(t *testing.T) {
	perKey := &DB{
		memtableAdaptive:     true,
		adaptiveShardedStats: true,
		mutableShards:        make([]memShard, 1),
	}
	perKey.resetMemtableStatsLocked()
	perKey.noteWriteKeyForShard(0, []byte("c"))
	perKey.noteWriteKeyForShard(0, []byte("d"))
	perKey.noteWriteKeyForShard(0, []byte("e"))
	perKey.noteWriteKeyForShard(0, []byte("f"))

	run := &DB{
		memtableAdaptive:     true,
		adaptiveShardedStats: true,
		mutableShards:        make([]memShard, 1),
	}
	run.resetMemtableStatsLocked()
	run.noteWriteKeyForShard(0, []byte("c"))
	run.noteWriteSortedRunForShard(0, []byte("d"), []byte("f"), 3)

	if w, g := perKey.memtableStats.writes.Load(), run.memtableStats.writes.Load(); w != g {
		t.Fatalf("writes mismatch: want=%d got=%d", w, g)
	}
	if w, g := perKey.memtableStats.seqWrites.Load(), run.memtableStats.seqWrites.Load(); w != g {
		t.Fatalf("seqWrites mismatch: want=%d got=%d", w, g)
	}

	perTracker := &perKey.memtableStats.shardSeq[0]
	runTracker := &run.memtableStats.shardSeq[0]

	perTracker.mu.Lock()
	perLast := string(perTracker.lastKey)
	perHas := perTracker.hasLastKey
	perTracker.mu.Unlock()

	runTracker.mu.Lock()
	runLast := string(runTracker.lastKey)
	runHas := runTracker.hasLastKey
	runTracker.mu.Unlock()

	if perHas != runHas {
		t.Fatalf("shard hasLastKey mismatch: want=%v got=%v", perHas, runHas)
	}
	if perLast != runLast {
		t.Fatalf("shard lastKey mismatch: want=%q got=%q", perLast, runLast)
	}
}

func assertStatsEquivalent(t *testing.T, want, got *DB) {
	t.Helper()

	if w, g := want.memtableStats.writes.Load(), got.memtableStats.writes.Load(); w != g {
		t.Fatalf("writes mismatch: want=%d got=%d", w, g)
	}
	if w, g := want.memtableStats.seqWrites.Load(), got.memtableStats.seqWrites.Load(); w != g {
		t.Fatalf("seqWrites mismatch: want=%d got=%d", w, g)
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
