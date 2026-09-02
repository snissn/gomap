package caching

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
)

type recordingBackend struct {
	*MockBackend
	maxNewBatchWithSize atomic.Int64
	calls               atomic.Int64
}

func (b *recordingBackend) NewBatchWithSize(size int) batch.Interface {
	b.calls.Add(1)
	for {
		cur := b.maxNewBatchWithSize.Load()
		if int64(size) <= cur {
			break
		}
		if b.maxNewBatchWithSize.CompareAndSwap(cur, int64(size)) {
			break
		}
	}
	// Use the default batch implementation so this test remains lightweight.
	return b.MockBackend.NewBatch()
}

func TestFlushCapsBackendBatchSizeHint(t *testing.T) {
	dir := t.TempDir()
	backend := &recordingBackend{MockBackend: NewMockBackend()}

	db, err := Open(dir, backend, Options{
		// Keep everything queued so we can force a single large flush unit.
		FlushThreshold: 1 << 60,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create a large immutable memtable. This historically could cause very large
	// reserve hints to bubble into backend.NewBatchWithSize -> batch.Reserve,
	// resulting in long CPU stalls and page-fault storms.
	const entries = 100_000
	payload := bytes.Repeat([]byte("x"), 128)

	db.mu.Lock()
	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		setMutable(db, key, payload)
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	done := make(chan struct{})
	go func() {
		db.flushAll(false)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("flushAll did not complete; likely stuck allocating an oversized backend batch")
	}

	if backend.calls.Load() == 0 {
		t.Fatalf("expected NewBatchWithSize to be used at least once")
	}
	if got := backend.maxNewBatchWithSize.Load(); got > int64(flushBackendBatchInitEntries) {
		t.Fatalf("NewBatchWithSize sizeHint=%d want <= %d", got, flushBackendBatchInitEntries)
	}
}

func TestWaitForStopSelfHealsStaleBacklogBytes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1024,
		MemtableShards: 1,
		// Enable adaptive backpressure so waitForStop runs.
		MaxBacklogBytes: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Simulate a stale backlog counter: queue is empty, but backlog bytes is non-zero.
	db.queueBacklogBytes.Store(1234)

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("k"), []byte("v"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Set blocked due to stale backlog bytes")
	}

	if got := db.QueueBacklogBytes(); got != 0 {
		t.Fatalf("QueueBacklogBytes=%d want 0", got)
	}
}
