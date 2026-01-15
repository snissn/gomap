package db

import (
	"errors"
	"sync"
	"testing"
)

// This test deterministically reproduces a CI-only panic where an in-flight write
// hits waitForSlabDurability while another goroutine clears db.slabManager (as
// DB.Close does). The pre-fix behavior panics with a nil receiver in
// SlabManager.WaitForOffset.
func TestWaitForSlabDurability_CloseNilsSlabManager_Regression(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, BackgroundCompactionIndexSwap: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	originalSM := db.slabManager

	var once sync.Once
	called := make(chan struct{})
	testHookWaitForSlabDurabilityAfterFlush = func(hdb *DB) {
		once.Do(func() {
			hdb.mu.Lock()
			hdb.slabManager = nil
			hdb.mu.Unlock()
			close(called)
		})
	}
	t.Cleanup(func() {
		testHookWaitForSlabDurabilityAfterFlush = nil
		db.mu.Lock()
		db.slabManager = originalSM
		db.mu.Unlock()
		_ = db.Close()
	})

	// Force a slab-backed write (large value).
	large := make([]byte, 1<<20) // 1MB
	if err := db.Set([]byte("big"), large); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("Set: %v", err)
	}

	select {
	case <-called:
	default:
		t.Fatalf("test hook was not invoked; regression not exercised")
	}
}
