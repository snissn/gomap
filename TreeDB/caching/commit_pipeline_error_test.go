package caching

import (
	"errors"
	"testing"
	"time"
)

func TestCommitPipelineErrorPropagation(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		FlushThreshold: 1 << 20,
		AllowUnsafe:    true,
		JournalLanes:   1,
		MemtableShards: 1,
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() {
			_ = db.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Logf("timeout closing db after commit failure")
		}
	})

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if db.mutableBytes.Load() == 0 {
		t.Fatalf("expected mutable bytes after Set")
	}

	// Rotate mutable to queue.
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	queueLen := len(db.queue)
	queuedLen := 0
	for i := range db.queue {
		queuedLen += db.queue[i].Len()
	}
	db.mu.Unlock()
	if queueLen == 0 || queuedLen == 0 {
		t.Fatalf("expected queued memtable entries, queueLen=%d queuedLen=%d", queueLen, queuedLen)
	}

	backend.SetWriteErr(errors.New("commit failure"))
	if err := backend.NewBatch().WriteSync(); err == nil {
		t.Fatalf("expected backend WriteSync to fail after SetWriteErr")
	}
	db.mu.Lock()
	_, _, _, totalLen := db.collectFlushUnitsLocked(0, 1, 0)
	db.mu.Unlock()
	if totalLen == 0 {
		t.Fatalf("expected queued memtable entries for flush (queuedLen=%d)", queuedLen)
	}
	if ok := db.flushLaneOnce(true, 0); ok {
		t.Fatalf("expected flushLaneOnce to fail")
	}

	// Background error should be set; allow for async worker scheduling.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := db.backgroundError(); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected background error to be set")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// New writes should be rejected.
	if err := db.Set([]byte("k2"), []byte("v2")); err == nil {
		t.Fatalf("expected Set to fail after commit error")
	}

	// Ensure any pending commits are drained before cleanup.
	drain := make(chan struct{})
	go func() {
		db.commitWg.Wait()
		close(drain)
	}()
	select {
	case <-drain:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for commit workers to drain")
	}
}
