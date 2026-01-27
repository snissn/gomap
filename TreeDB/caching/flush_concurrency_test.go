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
