package caching

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openCheckpointPreDrainTestDB(t *testing.T, enablePreDrain bool) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                   dir,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
		FlushApplySpanNative:  true,
	})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:           64 << 20,
		MemtableShards:           1,
		JournalLanes:             1,
		FlushApplyConcurrency:    4,
		FlushApplyMinEntries:     1,
		FlushApplyMinSpans:       1,
		FlushApplyMinBytes:       1,
		FlushApplySpanNative:     true,
		FlushBacklogCoalescing:   enablePreDrain,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func writeCheckpointPreDrainValues(t *testing.T, db *DB, prefix string) {
	t.Helper()
	payload := bytes.Repeat([]byte(prefix[:1]), 256)
	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("%s-key-%04d", prefix, i))
		if err := db.Set(key, payload); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
}

func enqueueCheckpointPreDrainMemtable(t *testing.T, db *DB, prefix string) {
	t.Helper()
	writeCheckpointPreDrainValues(t, db, prefix)
	db.mu.Lock()
	if err := db.rotateMutableShardsLocked(db.checkpointRotateCapacity(), false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMutableShardsLocked: %v", err)
	}
	db.mu.Unlock()
	if got := mustStatInt64(t, db.Stats(), "treedb.cache.queue_len"); got == 0 {
		t.Fatalf("queue_len=%d want queued work before checkpoint", got)
	}
}

func TestCheckpointPreDrainRotatesMutableBeforeCheckpointFallback(t *testing.T) {
	db := openCheckpointPreDrainTestDB(t, true)
	writeCheckpointPreDrainValues(t, db, "mutable")
	if got := mustStatInt64(t, db.Stats(), "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want no queued work before checkpoint", got)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.pre_drain_flushes_total"); got == 0 {
		t.Fatalf("pre_drain_flushes_total=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"); got != 0 {
		t.Fatalf("close_or_checkpoint fallback ops=%d want 0 for pre-drained mutable work", got)
	}
}

func TestCheckpointPreDrainUsesBackgroundModeForOptInQueuedWork(t *testing.T) {
	db := openCheckpointPreDrainTestDB(t, true)
	enqueueCheckpointPreDrainMemtable(t, db, "predrain")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got != 0 {
		t.Fatalf("queue_len=%d want 0 after checkpoint", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.pre_drain_flushes_total"); got == 0 {
		t.Fatalf("pre_drain_flushes_total=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.stage.pre_drain.samples"); got == 0 {
		t.Fatalf("checkpoint pre_drain samples=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"); got != 0 {
		t.Fatalf("close_or_checkpoint fallback ops=%d want 0 for pre-drained queued work", got)
	}
}

func TestCheckpointPreDrainDisabledWithoutBacklogOptInKeepsCheckpointFallback(t *testing.T) {
	db := openCheckpointPreDrainTestDB(t, false)
	enqueueCheckpointPreDrainMemtable(t, db, "fallback")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.pre_drain_flushes_total"); got != 0 {
		t.Fatalf("pre_drain_flushes_total=%d want 0 without backlog opt-in", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.stage.pre_drain.samples"); got != 0 {
		t.Fatalf("checkpoint pre_drain samples=%d want 0 without backlog opt-in", got)
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"); got == 0 {
		t.Fatalf("close_or_checkpoint fallback ops=%d want >0 when pre-drain is disabled", got)
	}
}

func TestCheckpointPreDrainSkipsCommandWALCheckpointPublisher(t *testing.T) {
	db := openCheckpointPreDrainTestDB(t, true)
	enqueueCheckpointPreDrainMemtable(t, db, "cmdhook")

	var calls atomic.Uint64
	db.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		calls.Add(1)
		return 0, nil, nil
	})
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := calls.Load(); got == 0 {
		t.Fatalf("command WAL checkpoint hook calls=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.pre_drain_flushes_total"); got != 0 {
		t.Fatalf("pre_drain_flushes_total=%d want 0 with command WAL checkpoint publisher", got)
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"); got == 0 {
		t.Fatalf("close_or_checkpoint fallback ops=%d want >0 with command WAL checkpoint publisher", got)
	}
}

func TestCheckpointPreDrainSkipsWALDisabledCheckpoint(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                   dir,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
		FlushApplySpanNative:  true,
	})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:           64 << 20,
		MemtableShards:           1,
		JournalLanes:             1,
		DisableWAL:               true,
		AllowUnsafe:              true,
		FlushApplyConcurrency:    4,
		FlushApplyMinEntries:     1,
		FlushApplyMinSpans:       1,
		FlushApplyMinBytes:       1,
		FlushApplySpanNative:     true,
		FlushBacklogCoalescing:   true,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	enqueueCheckpointPreDrainMemtable(t, db, "waloff")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.pre_drain_flushes_total"); got != 0 {
		t.Fatalf("pre_drain_flushes_total=%d want 0 with WAL disabled", got)
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"); got == 0 {
		t.Fatalf("close_or_checkpoint fallback ops=%d want >0 with WAL disabled", got)
	}
}
