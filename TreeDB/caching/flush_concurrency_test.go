package caching

import (
	"fmt"
	"sync"
	"testing"
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
