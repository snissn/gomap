package caching

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func newTestFlushCoordinatorDB(backlog, inFlight, stopBytes int64) *DB {
	db := &DB{
		flushThreshold:          1,
		maxBacklogBytes:         stopBytes,
		writerFlushMaxMemtables: 1,
		flushApplyConcurrency:   4,
		flushCoordinatorWaitCh:  make(chan struct{}),
	}
	db.memtables.Store(&memtableView{queue: make([]memtable.Table, 1)})
	db.queueBacklogBytes.Store(backlog)
	db.flushCoordinatorActive.Store(1)
	db.flushCoordinatorInFlightBytes.Store(inFlight)
	return db
}

func TestFlushCoordinatorActiveFlushCreditsSkipForegroundAssist(t *testing.T) {
	db := newTestFlushCoordinatorDB(1500, 800, 1000)

	db.maybeAssistFlush()

	if got := db.flushApplyCoordinatorActiveAssistSkips.Load(); got != 1 {
		t.Fatalf("active assist skips=%d want 1", got)
	}
	if got := db.flushApplyForegroundAssistCalls.Load(); got != 0 {
		t.Fatalf("foreground assist calls=%d want 0", got)
	}
	if got := db.flushApplyCoordinatorBlockingFallbacks.Load(); got != 0 {
		t.Fatalf("blocking fallbacks=%d want 0", got)
	}
}

func TestFlushCoordinatorHardOverloadFallsBackToForegroundAssist(t *testing.T) {
	db := newTestFlushCoordinatorDB(1500, 200, 1000)

	db.maybeAssistFlush()

	if got := db.flushApplyCoordinatorHardOverloadFallbacks.Load(); got != 1 {
		t.Fatalf("hard overload fallbacks=%d want 1", got)
	}
	if got := db.flushApplyCoordinatorBlockingFallbacks.Load(); got != 1 {
		t.Fatalf("blocking fallbacks=%d want 1 for direct hard-overload assist", got)
	}
	if got := db.flushApplyForegroundAssistCalls.Load(); got != 1 {
		t.Fatalf("foreground assist calls=%d want 1", got)
	}
}

func TestFlushCoordinatorStopBackpressureHardOverloadSkipsProgressWait(t *testing.T) {
	db := newTestFlushCoordinatorDB(1500, 200, 1000)

	db.waitForStop()

	if got := db.flushApplyCoordinatorHardOverloadFallbacks.Load(); got == 0 {
		t.Fatalf("hard overload fallbacks=%d want >0", got)
	}
	if got := db.flushApplyCoordinatorProgressWaits.Load(); got != 0 {
		t.Fatalf("progress waits=%d want 0 when active credits cannot cover stop backlog", got)
	}
	if got := db.flushApplyCoordinatorBlockingFallbacks.Load(); got == 0 {
		t.Fatalf("blocking fallbacks=%d want >0", got)
	}
	if got := db.flushApplyForegroundAssistCalls.Load(); got == 0 {
		t.Fatalf("foreground assist calls=%d want >0", got)
	}
}

func TestFlushCoordinatorStopAssistBlocksWhenFlushMuHeld(t *testing.T) {
	db := newTestFlushCoordinatorDB(1500, 200, 1000)
	db.flushMu.Lock()
	done := make(chan struct{})
	go func() {
		db.maybeAssistFlush()
		close(done)
	}()
	select {
	case <-done:
		db.flushMu.Unlock()
		t.Fatal("maybeAssistFlush returned while stop-backpressure assist flushMu was held")
	case <-time.After(20 * time.Millisecond):
	}
	db.flushMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maybeAssistFlush did not complete after flushMu was released")
	}
	if got := db.flushApplyCoordinatorBlockingFallbacks.Load(); got != 1 {
		t.Fatalf("blocking fallbacks=%d want 1", got)
	}
}

func TestFlushCoordinatorFlushErrorRemovesActiveCredit(t *testing.T) {
	db := newTestFlushCoordinatorDB(1500, 0, 1000)
	db.beginFlushCoordinatorWork(800)
	db.finishFlushCoordinatorWork(800, false)

	db.maybeAssistFlush()

	if got := db.flushCoordinatorErrors.Load(); got != 1 {
		t.Fatalf("coordinator errors=%d want 1", got)
	}
	if got := db.flushCoordinatorInFlightBytes.Load(); got != 0 {
		t.Fatalf("in-flight bytes=%d want 0", got)
	}
	if got := db.flushApplyCoordinatorActiveAssistSkips.Load(); got != 0 {
		t.Fatalf("active assist skips=%d want 0 after failed credit", got)
	}
	if got := db.flushApplyForegroundAssistCalls.Load(); got != 1 {
		t.Fatalf("foreground assist calls=%d want 1", got)
	}
	if got := db.flushApplyCoordinatorBlockingFallbacks.Load(); got != 1 {
		t.Fatalf("blocking fallbacks=%d want 1", got)
	}
}

func TestFlushCoordinatorStalledActiveFlushRecordsWait(t *testing.T) {
	db := newTestFlushCoordinatorDB(1500, 800, 1000)

	if ok := db.waitForActiveFlushProgress(1500, 1000, time.Millisecond); ok {
		t.Fatal("waitForActiveFlushProgress returned progress for a stalled active flush")
	}
	if got := db.flushApplyCoordinatorStallWaits.Load(); got != 1 {
		t.Fatalf("stall waits=%d want 1", got)
	}
	if got := db.flushApplyCoordinatorProgressWaits.Load(); got != 1 {
		t.Fatalf("progress waits=%d want 1", got)
	}
}

func TestFlushCoordinatorCheckpointDrainsQueuedWork(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:        1024,
		MemtableShards:        1,
		DisableWAL:            true,
		AllowUnsafe:           true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	payload := bytes.Repeat([]byte("c"), 2048)
	db.mu.Lock()
	setMutable(db, []byte("checkpoint-key"), payload)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := mustStatInt64(t, db.Stats(), "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want 0", got)
	}
	if got := db.flushCoordinatorActive.Load(); got != 0 {
		t.Fatalf("active coordinator passes=%d want 0", got)
	}
	if got := db.flushCoordinatorInFlightBytes.Load(); got != 0 {
		t.Fatalf("in-flight bytes=%d want 0", got)
	}
	got, err := backend.Get([]byte("checkpoint-key"))
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("backend value mismatch after checkpoint")
	}
}

func TestFlushCoordinatorCloseDrainsQueuedWork(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:        1024,
		MemtableShards:        1,
		DisableWAL:            true,
		AllowUnsafe:           true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	payload := bytes.Repeat([]byte("d"), 2048)
	db.mu.Lock()
	setMutable(db, []byte("close-key"), payload)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := db.flushCoordinatorActive.Load(); got != 0 {
		t.Fatalf("active coordinator passes=%d want 0", got)
	}
	if got := db.flushCoordinatorInFlightBytes.Load(); got != 0 {
		t.Fatalf("in-flight bytes=%d want 0", got)
	}
	got, err := backend.Get([]byte("close-key"))
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("backend value mismatch after close")
	}
}
