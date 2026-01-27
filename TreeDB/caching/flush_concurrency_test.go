package caching

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCachingDB_ConcurrentFlushDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Low threshold to create queued memtables quickly.
	db, err := Open(dir, backend, Options{FlushThreshold: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Create work for the flusher.
	for i := 0; i < 200; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Ensure the current mutable memtable is rotated into the flush queue.
	db.mu.Lock()
	_ = db.rotateMemtableLocked(true)
	db.mu.Unlock()

	// In older versions, concurrent flushers could race and slice an empty queue.
	// This test ensures flush operations remain safe under concurrent calls.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.flushAll(true)
		}()
	}
	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_FlushAllEmptyQueueCompletes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	done := make(chan struct{})
	go func() {
		db.flushAll(false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("flushAll hung on empty queue")
	}
}

func TestCachingDB_MultiLaneFlushDrainsQueue(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 20,
		MemtableShards:     4,
		MemtableMode:       "hash_sorted",
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
		JournalLanes:       2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	written := make([]string, 0, 4000)
	writeKVBatch := func(prefix byte, n int) {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("%c%08d", prefix, i)
			if err := db.Set([]byte(k), []byte(k)); err != nil {
				t.Fatalf("Set: %v", err)
			}
			written = append(written, k)
		}
		db.mu.Lock()
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
		db.mu.Unlock()
	}

	writeKVBatch('A', 2000)
	writeKVBatch('B', 2000)
	db.mu.RLock()
	if len(db.queue) == 0 {
		db.mu.RUnlock()
		t.Fatalf("expected queued memtables before flush")
	}
	db.mu.RUnlock()

	db.flushAll(false)

	db.mu.RLock()
	if len(db.queue) != 0 {
		db.mu.RUnlock()
		t.Fatalf("expected queue drained after flush, got %d", len(db.queue))
	}
	db.mu.RUnlock()

	for _, key := range written {
		if _, err := backend.Get([]byte(key)); err != nil {
			t.Fatalf("backend.Get %s: %v", key, err)
		}
	}
}

func TestCachingDB_FlushLaneOnlyMatchingMemtables(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 20,
		MemtableShards:     2,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
		JournalLanes:       2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	findKey := func(targetLane int) []byte {
		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("lane%d-%04d", targetLane, i))
			if db.laneForShardIndex(db.shardIndex(key)) == targetLane {
				return key
			}
		}
		t.Fatalf("could not find key for lane %d", targetLane)
		return nil
	}

	key0 := findKey(0)
	key1 := findKey(1)

	if err := db.Set(key0, []byte("v0")); err != nil {
		t.Fatalf("Set lane0: %v", err)
	}
	if err := db.Set(key1, []byte("v1")); err != nil {
		t.Fatalf("Set lane1: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(true); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	if len(db.queue) != 2 {
		t.Fatalf("expected 2 queued memtables, got %d", len(db.queue))
	}

	if !db.flushLaneOnce(false, 0) {
		t.Fatalf("flushLaneOnce returned false for lane0")
	}
	if len(db.queue) != 1 {
		t.Fatalf("expected queue len 1 after lane0 flush, got %d", len(db.queue))
	}

	if !db.flushLaneOnce(false, 1) {
		t.Fatalf("flushLaneOnce returned false for lane1")
	}
	if len(db.queue) != 0 {
		t.Fatalf("expected queue empty after both lanes flushed, got %d", len(db.queue))
	}

	if db.flushLaneOnce(false, 0) {
		t.Fatalf("flushLaneOnce should return false when no memtables remain")
	}
}

func TestCachingDB_FlushAllHandlesConcurrentCalls(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 16,
		MemtableMode:       "hash_sorted",
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	fillQueue := func(id byte) {
		for i := 0; i < 500; i++ {
			if err := db.Set([]byte(fmt.Sprintf("%c%04d", id, i)), []byte("value")); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		db.mu.Lock()
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
		db.mu.Unlock()
	}

	fillQueue('X')
	fillQueue('Y')
	fillQueue('Z')

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.flushAll(false)
		}()
	}
	wg.Wait()

	db.mu.RLock()
	if len(db.queue) != 0 {
		db.mu.RUnlock()
		t.Fatalf("expected queue drained after concurrent flushAll, got %d", len(db.queue))
	}
	db.mu.RUnlock()
}

func TestCachingDB_TriggerFlushWakesLoop(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 16,
		MemtableMode:       "hash_sorted",
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for i := 0; i < 1000; i++ {
		if err := db.Set([]byte(fmt.Sprintf("trigger-%04d", i)), []byte("val")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(true); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.TriggerFlush()

	deadline := time.Now().Add(3 * time.Second)
	for {
		db.mu.RLock()
		queueLen := len(db.queue)
		db.mu.RUnlock()
		if queueLen == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue did not drain after TriggerFlush, len=%d", queueLen)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFlushAllBlocksWhileFlushMuHeld(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 16,
		MemtableMode:       "hash_sorted",
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.flushMu.Lock()

	done := make(chan struct{})
	go func() {
		db.flushAll(false)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("flushAll finished while flushMu held")
	default:
	}

	db.flushMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flushAll did not finish after releasing flushMu")
	}
}

func TestFlushOneBlocksWhileFlushMuHeld(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 20,
		MemtableMode:       "hash_sorted",
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
		JournalLanes:       2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.flushMu.Lock()

	done := make(chan struct{})
	go func() {
		db.flushOne()
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("flushOne finished while flushMu held")
	default:
	}

	db.flushMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flushOne did not finish after releasing flushMu")
	}
}

func TestAdaptiveChunkCapDefaultAllowsAdaptive(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:             1 << 20,
		MemtableMode:               "hash_sorted",
		MaxQueuedMemtables:         -1,
		AllowUnsafe:                true,
		FlushBuildChunkCap:         0,
		FlushBuildChunkTargetBytes: 16 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.flushBuildChunkCap != 0 {
		t.Fatalf("expected chunk cap 0 to enable adaptive sizing, got %d", db.flushBuildChunkCap)
	}
}
