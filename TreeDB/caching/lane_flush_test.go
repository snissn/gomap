package caching

import (
	"fmt"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestLaneAwareFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Use 4 lanes
	opts := Options{
		JournalLanes:             4,
		MemtableShards:           4,
		ValueLogPointerThreshold: 1, // Always use vlog pointers
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
		AllowUnsafe:              true,
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var mu sync.Mutex
	flushedLanes := make(map[int]int)
	syncedLanes := make(map[int]int)

	db.testOnVlogFlush = func(laneID int) {
		mu.Lock()
		flushedLanes[laneID]++
		mu.Unlock()
	}
	db.testOnVlogSync = func(laneID int) {
		mu.Lock()
		syncedLanes[laneID]++
		mu.Unlock()
	}

	// Write data that will land in lane 1
	var targetKey []byte
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		shardIdx := db.shardIndex(key)
		laneID := db.laneForShardIndex(shardIdx)
		if laneID == 1 {
			targetKey = key
			break
		}
	}
	if targetKey == nil {
		t.Fatal("could not find key for lane 1")
	}

	if err := db.Set(targetKey, []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Rotate to move it to queue
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	// Flush lane 1 specifically
	if !db.flushLaneOnce(true, 1) {
		t.Fatal("flushLaneOnce failed")
	}

	mu.Lock()
	defer mu.Unlock()

	for laneID, count := range flushedLanes {
		if laneID != 1 && count > 0 {
			t.Errorf("lane %d was flushed %d times, expected 0", laneID, count)
		}
	}
	if flushedLanes[1] == 0 {
		t.Error("lane 1 was not flushed")
	}

	for laneID, count := range syncedLanes {
		if laneID != 1 && count > 0 {
			t.Errorf("lane %d was synced %d times, expected 0", laneID, count)
		}
	}
	if syncedLanes[1] == 0 {
		t.Error("lane 1 was not synced")
	}
}

func TestLaneAwareFlushAll(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Use 4 lanes
	opts := Options{
		JournalLanes:             4,
		MemtableShards:           4,
		ValueLogPointerThreshold: 1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
		AllowUnsafe:              true,
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var mu sync.Mutex
	flushedLanes := make(map[int]int)

	db.testOnVlogFlush = func(laneID int) {
		mu.Lock()
		flushedLanes[laneID]++
		mu.Unlock()
	}

	// Write data to all lanes
	for laneID := 0; laneID < 4; laneID++ {
		var key []byte
		for i := 0; i < 10000; i++ {
			k := []byte(fmt.Sprintf("lane%d-key%d", laneID, i))
			if db.laneForShardIndex(db.shardIndex(k)) == laneID {
				key = k
				break
			}
		}
		if key == nil {
			t.Fatalf("failed to find key for lane %d within iteration limit", laneID)
		}
		if err := db.Set(key, []byte("value")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Rotate
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	// Flush all
	db.flushAll(true)

	mu.Lock()
	defer mu.Unlock()

	for laneID := 0; laneID < 4; laneID++ {
		// Each lane should be flushed exactly once.
		// Without the fix, each of the 4 goroutines would flush ALL 4 lanes,
		// resulting in 4 flushes per lane.
		if flushedLanes[laneID] != 1 {
			t.Errorf("lane %d was flushed %d times, expected 1", laneID, flushedLanes[laneID])
		}
	}
}
