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
	// 1000 writes, 900 sequential
	db.memtableStats.writes.Store(1000)
	db.memtableStats.seqWrites.Store(900)
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeHashSorted {
		t.Errorf("sequential mode = %v, want %v", got, memtable.ModeHashSorted)
	}

	// 3. High range scans -> BTree
	// Reset
	db.memtableStats.writes.Store(1000)
	db.memtableStats.seqWrites.Store(100) // Low seq
	db.memtableStats.iterators.Store(100)
	db.memtableStats.rangeIters.Store(80) // 80% range
	if got := db.chooseAdaptiveMemtableModeLocked(); got != memtable.ModeBTree {
		t.Errorf("range scan mode = %v, want %v", got, memtable.ModeBTree)
	}
}
