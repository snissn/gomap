package caching

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

func mustStatInt64(t *testing.T, stats map[string]string, key string) int64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("stat %q parse: %v", key, err)
	}
	return val
}

func TestBackpressureStatsQueueMetrics(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:          1024,
		MemtableShards:          1,
		MaxBacklogBytes:         2048,
		SlowdownBacklogSeconds:  1,
		StopBacklogSeconds:      2,
		WriterFlushMaxMemtables: 1,
		DisableWAL:              true,
		AllowUnsafe:             true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	payload := bytes.Repeat([]byte("x"), 2048)

	db.mu.Lock()
	setMutable(db, []byte("k1"), payload)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 1 {
		t.Fatalf("queue_len=%d want 1", got)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got <= 0 {
		t.Fatalf("queue_backlog_bytes=%d want >0", got)
	}

	db.flushAll(false)

	stats = db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want 0", got)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got != 0 {
		t.Fatalf("queue_backlog_bytes=%d want 0", got)
	}
}

func TestStopBackpressureFlushesAndReturns(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:          1024,
		MemtableShards:          1,
		MaxBacklogBytes:         4096,
		SlowdownBacklogSeconds:  0,
		StopBacklogSeconds:      0,
		WriterFlushMaxMemtables: 1,
		DisableWAL:              true,
		AllowUnsafe:             true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	payload := bytes.Repeat([]byte("y"), 2048)

	db.mu.Lock()
	for i := 0; i < 3; i++ {
		setMutable(db, []byte{byte('a' + i)}, payload)
		if err := db.rotateMemtableLocked(false); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
	}
	db.mu.Unlock()

	before := db.QueueBacklogBytes()
	if before == 0 {
		t.Fatalf("expected backlog >0 before waitForStop")
	}

	done := make(chan struct{})
	go func() {
		db.waitForStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForStop did not return")
	}

	after := db.QueueBacklogBytes()
	if after >= before {
		t.Fatalf("backlog did not decrease: before=%d after=%d", before, after)
	}

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got != after {
		t.Fatalf("stats backlog=%d want %d", got, after)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.flush_bps_ewma"); got <= 0 {
		t.Fatalf("flush_bps_ewma=%d want >0", got)
	}
}

func TestFlushRemovesEmptyUnits(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1024,
		MemtableShards: 1,
		DisableWAL:     true,
		AllowUnsafe:    true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 1 {
		t.Fatalf("queue_len=%d want 1", got)
	}

	db.flushAll(false)

	stats = db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want 0", got)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got != 0 {
		t.Fatalf("queue_backlog_bytes=%d want 0", got)
	}
}
