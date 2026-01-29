package caching

import (
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// TestCommitPipelineBackpressure verifies that per-lane backpressure blocks
// concurrent flushes when a commit is already in flight.
func TestCommitPipelineBackpressure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		FlushThreshold: 1,
		AllowUnsafe:    true,
		JournalLanes:   1,
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Populate the current mutable memtable without triggering background flush.
	db.mu.Lock()
	if len(db.mutableShards) == 0 {
		db.mu.Unlock()
		t.Fatalf("no mutable shards")
	}
	shard := &db.mutableShards[0]
	shard.mu.Lock()
	before := shard.mem.Size()
	shard.mem.SetEntry([]byte("k1"), []byte("v1"), page.ValuePtr{}, node.FlagInline)
	shard.rng.add([]byte("k1"))
	after := shard.mem.Size()
	delta := after - before
	shard.bytes = after
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()

	// Rotate mutable to queue (without triggering background flush).
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	// Simulate an in-flight commit for lane 0 to force backpressure.
	lane := &db.lanes[0]
	lane.commitMu.Lock()
	lane.commitsInFlight.Store(1)
	lane.commitMu.Unlock()

	// Kick off a flush in another goroutine; it should block until we release the lane.
	done := make(chan bool, 1)
	go func() {
		ok := db.flushLaneOnce(false, 0)
		done <- ok
	}()

	// Ensure the flush is blocked while the commit is "in flight".
	select {
	case <-done:
		lane.commitMu.Lock()
		lane.commitsInFlight.Store(0)
		lane.commitCond.Broadcast()
		lane.commitMu.Unlock()
		t.Fatalf("flushLaneOnce returned while commit was in flight")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked.
	}

	// Release the lane and ensure the flush completes.
	lane.commitMu.Lock()
	lane.commitsInFlight.Store(0)
	lane.commitCond.Broadcast()
	lane.commitMu.Unlock()

	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("flushLaneOnce returned false after release")
		}
	case <-time.After(2 * time.Second):
		// Cleanup to avoid leaking goroutine if it did block unexpectedly.
		lane.commitMu.Lock()
		lane.commitsInFlight.Store(0)
		lane.commitCond.Broadcast()
		lane.commitMu.Unlock()
		t.Fatalf("flushLaneOnce did not return after releasing commit")
	}

	// Ensure queue was drained (commit job should have been enqueued and processed).
	// Wait for any background commits to finish.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		db.commitWg.Wait()
	}()
	wg.Wait()
}
