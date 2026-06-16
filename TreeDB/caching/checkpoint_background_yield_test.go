package caching

import (
	"testing"
	"time"
)

func TestBackgroundFlushYieldsToWaitingCheckpointRequest(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing: true,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 2, "checkpoint-yield")

	db.checkpointRequests.Add(1)
	defer db.checkpointRequests.Add(-1)

	db.flushAllBackground(false)

	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.background_flush_yields_total"); got != 1 {
		t.Fatalf("background_flush_yields_total=%d want 1", got)
	}
	db.mu.RLock()
	queueLen := len(db.queue)
	db.mu.RUnlock()
	if queueLen != 2 {
		t.Fatalf("queue_len=%d want background flush to leave queued memtables for checkpoint", queueLen)
	}
}

func TestNonBackgroundDrainIgnoresCheckpointRequestCounter(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing: true,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 2, "checkpoint-nonyield")

	db.checkpointRequests.Add(1)
	defer db.checkpointRequests.Add(-1)

	db.flushAll(false)

	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.background_flush_yields_total"); got != 0 {
		t.Fatalf("background_flush_yields_total=%d want 0 for non-background drain", got)
	}
	db.mu.RLock()
	queueLen := len(db.queue)
	db.mu.RUnlock()
	if queueLen != 0 {
		t.Fatalf("queue_len=%d want non-background drain to flush despite request counter", queueLen)
	}
}

func TestCheckpointPublishesRequestBeforeFlushMuWait(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{}, highSingleOpCoalescingSnapshot())

	db.flushMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- db.Checkpoint()
	}()

	deadline := time.After(2 * time.Second)
	for db.checkpointRequests.Load() == 0 {
		select {
		case <-deadline:
			db.flushMu.Unlock()
			t.Fatal("checkpoint request was not published before waiting on flushMu")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	db.flushMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Checkpoint did not finish after flushMu was released")
	}
	if got := db.checkpointRequests.Load(); got != 0 {
		t.Fatalf("checkpointRequests=%d want 0 after checkpoint returns", got)
	}
}
