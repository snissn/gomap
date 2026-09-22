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

func mustStatString(t *testing.T, stats map[string]string, key string) string {
	t.Helper()
	val, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
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
	ewma := mustStatInt64(t, stats, "treedb.cache.flush_bps_ewma")
	if after > 0 && ewma == 0 {
		t.Fatalf("flush_bps_ewma=%d want >0 when backlog remains", ewma)
	}
}

func TestFlushSkipsEmptyUnitsAndRemovesNonEmptyUnits(t *testing.T) {
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
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want 0 after empty rotate", got)
	}

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked(non-empty): %v", err)
	}
	db.mu.Unlock()
	stats = db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 1 {
		t.Fatalf("queue_len=%d want 1 after non-empty rotate", got)
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

func TestBackpressureModeStatsLegacyVsAdaptive(t *testing.T) {
	legacy, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold:     1024,
		MemtableShards:     1,
		MaxQueuedMemtables: 2,
	})
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	t.Cleanup(func() { _ = legacy.Close() })

	stats := legacy.Stats()
	if got := mustStatString(t, stats, "treedb.cache.backpressure_mode"); got != "queue_len" {
		t.Fatalf("legacy backpressure_mode=%q want queue_len", got)
	}

	adaptive, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold:         1024,
		MemtableShards:         1,
		MaxBacklogBytes:        2048,
		SlowdownBacklogSeconds: 1,
		StopBacklogSeconds:     2,
	})
	if err != nil {
		t.Fatalf("Open adaptive: %v", err)
	}
	t.Cleanup(func() { _ = adaptive.Close() })

	stats = adaptive.Stats()
	if got := mustStatString(t, stats, "treedb.cache.backpressure_mode"); got != "adaptive" {
		t.Fatalf("adaptive backpressure_mode=%q want adaptive", got)
	}
}

func TestWaitForStopWithConcurrentFlushTrigger(t *testing.T) {
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

	payload := bytes.Repeat([]byte("z"), 2048)
	db.mu.Lock()
	for i := 0; i < 4; i++ {
		setMutable(db, []byte{byte('k' + i)}, payload)
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

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				db.TriggerFlush()
			case <-stop:
				return
			}
		}
	}()

	go func() {
		db.waitForStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		close(stop)
		t.Fatal("waitForStop did not return")
	}
	close(stop)

	after := db.QueueBacklogBytes()
	if after >= before {
		t.Fatalf("backlog did not decrease: before=%d after=%d", before, after)
	}
}

func TestCheckpointFlushStats(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:         1024,
		MemtableShards:         1,
		MaxBacklogBytes:        2048,
		SlowdownBacklogSeconds: 1,
		StopBacklogSeconds:     2,
		DisableWAL:             true,
		AllowUnsafe:            true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	payload := bytes.Repeat([]byte("q"), 2048)
	db.mu.Lock()
	setMutable(db, []byte("k1"), payload)
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got == 0 {
		t.Fatalf("expected queue_len >0 before checkpoint")
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats = db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want 0 after checkpoint", got)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got != 0 {
		t.Fatalf("queue_backlog_bytes=%d want 0 after checkpoint", got)
	}
}

func TestAdaptiveBackpressureSelfHealsStaleBacklogStats(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:         1024,
		MemtableShards:         1,
		MaxBacklogBytes:        2048,
		SlowdownBacklogSeconds: 1,
		StopBacklogSeconds:     2,
		DisableWAL:             true,
		AllowUnsafe:            true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.queueBacklogBytes.Store(1234)

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("heal"), []byte("ok"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Set blocked despite stale backlog bytes")
	}

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got != 0 {
		t.Fatalf("queue_backlog_bytes=%d want 0 after heal", got)
	}
}
