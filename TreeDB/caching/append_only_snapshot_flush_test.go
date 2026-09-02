package caching

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestAppendOnlyMutable_RepeatedGetsHitMutableMemtable(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Wrap the live mutable memtable so we can count point-read probes.
	db.mu.Lock()
	if len(db.mutableShards) != 1 {
		db.mu.Unlock()
		t.Fatalf("mutable shard count=%d want=1", len(db.mutableShards))
	}
	wrapped := &countingTable{inner: db.mutableShards[0].mem}
	db.mutableShards[0].mem = wrapped
	db.publishMemtablesLocked()
	db.mu.Unlock()

	key := []byte("hot-key")
	val := []byte("hot-value")
	if err := db.Set(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}

	const reads = 2000
	for i := 0; i < reads; i++ {
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != string(val) {
			t.Fatalf("get %d value=%q want=%q", i, got, val)
		}
	}

	if wrapped.getEntryCalls != reads {
		t.Fatalf("mutable getEntry calls=%d want=%d", wrapped.getEntryCalls, reads)
	}
	if wrapped.getCalls != 0 {
		t.Fatalf("mutable get calls=%d want=0 (Get uses GetEntry fast-path)", wrapped.getCalls)
	}
}

func TestAppendOnlyIteratorSnapshot_PinsOldViewAcrossFlush_CurrentBehavior(t *testing.T) {
	// This test intentionally captures today's implementation detail:
	// iterator snapshots pin an old memtable view until iterator close.
	//
	// That behavior is useful for simple snapshot isolation, but it can retain
	// append-only memtables longer than desired. If a future implementation keeps
	// snapshot correctness while releasing old views earlier (or switching iterator
	// data sources safely), this test may fail and that could be an improvement,
	// not necessarily a regression.
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = it.Close()
		}
	}()

	viewBeforeFlush := db.memtables.Load()
	if viewBeforeFlush == nil || len(viewBeforeFlush.queue) != 1 {
		t.Fatalf("expected one queued memtable after iterator snapshot, got=%v", viewBeforeFlush)
	}
	queued, ok := viewBeforeFlush.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", viewBeforeFlush.queue[0])
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued len before flush=%d want=2", got)
	}

	done := make(chan struct{})
	go func() {
		db.flushAll(false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("flushAll blocked while iterator snapshot was open")
	}

	db.mu.RLock()
	queueLen := len(db.queue)
	db.mu.RUnlock()
	if queueLen != 0 {
		t.Fatalf("queue len after flush=%d want=0", queueLen)
	}
	viewAfterFlush := db.memtables.Load()
	if viewAfterFlush == nil {
		t.Fatalf("view after flush=nil")
	}
	if viewAfterFlush == viewBeforeFlush {
		t.Fatalf("expected flush publish to swap memtable view pointers")
	}
	if len(viewAfterFlush.queue) != 0 {
		t.Fatalf("expected current view queue empty after flush, got=%d", len(viewAfterFlush.queue))
	}

	got, err := backend.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("backend get k1: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("backend k1=%q want=v1", got)
	}

	// The old queued memtable remains alive while the iterator holds a lease to
	// the pre-flush view.
	if refs := viewBeforeFlush.refs.Load(); refs != 1 {
		t.Fatalf("old view refs while iterator open=%d want=1", refs)
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued len while iterator open=%d want=2", got)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	closed = true
	if refs := viewBeforeFlush.refs.Load(); refs != 0 {
		t.Fatalf("old view refs after iterator close=%d want=0", refs)
	}

	if got := queued.Len(); got != 0 {
		t.Fatalf("queued len after iterator close=%d want=0", got)
	}
}
