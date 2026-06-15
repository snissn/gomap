package caching

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func requireStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return v
}

func TestFlushApplyStatsExposeStageCounters(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:             1 << 20,
		MemtableShards:             1,
		IndexOuterLeavesInValueLog: true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	value := bytes.Repeat([]byte("v"), 96)
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	stats := db.Stats()
	for _, key := range []string{
		"treedb.cache.flush_apply.planning_ns_total",
		"treedb.cache.flush_apply.build_ns_total",
		"treedb.cache.flush_apply.leaf_log_encode_compress_ns_total",
		"treedb.flush_apply.apply_ns_total",
		"treedb.cache.checkpoint.active_background_flush_wait_ns_total",
		"treedb.cache.checkpoint.stage.command_wal_publish.samples",
		"treedb.cache.checkpoint.stage.backend_boundary.samples",
		"treedb.cache.checkpoint.stage.leaf_value_log_sync.samples",
		"treedb.cache.checkpoint.stage.reducer_publish.samples",
	} {
		// Tiny stage timers can round to zero on low-resolution platforms; the
		// plumbing requirement is that these counters are present in DB.Stats().
		_ = requireStatUint64(t, stats, key)
	}
	if got := stats["treedb.cache.flush_apply.leaf_log_append_frames_per_op"]; got == "" {
		t.Fatalf("missing leaf_log_append_frames_per_op")
	}
	for _, key := range []string{
		"treedb.cache.flush_apply.batches_total",
		"treedb.cache.flush_apply.bytes_total",
		"treedb.cache.flush_apply.backend_write_ns_total",
		"treedb.cache.flush_span_run.runs_total",
		"treedb.cache.flush_span_run.planned_ops_total",
		"treedb.cache.flush_span_run.source_memtables_total",
		"treedb.cache.flush_span_run.backend_chunks_total",
		"treedb.flush_apply.apply_calls_total",
		"treedb.flush_apply.old_leaf_read_decode.node_loads_total",
		"treedb.flush_apply.merge_build.leaf_merges_total",
		"treedb.flush_apply.prepared_output.leaf_log_pages_installed_total",
		"treedb.flush_apply.guarded_publish.calls_total",
		"treedb.cache.checkpoint.stage.cutover.samples",
		"treedb.cache.checkpoint.stage.wal_rotate.samples",
		"treedb.cache.checkpoint.stage.value_log_flush.samples",
		"treedb.cache.checkpoint.stage.flush_all.samples",
		"treedb.cache.checkpoint.stage.flush_all.total_ns",
		"treedb.cache.checkpoint.stage.wal_cleanup.samples",
		"treedb.cache.checkpoint.stage.post_maintenance.samples",
	} {
		if got := requireStatUint64(t, stats, key); got == 0 {
			t.Fatalf("%s=%d want >0", key, got)
		}
	}
	// Outer leaves are persisted through the persistent leaf log when enabled;
	// the M0 counters expose that append path separately from backend apply.
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.leaf_log_append_records_total"); got == 0 {
		t.Fatalf("leaf log append records = 0, want >0")
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.leaf_log_append_frames_total"); got == 0 {
		t.Fatalf("leaf log append frames = 0, want >0")
	}
}

func TestFlushApplyStatsHelpersExposeForegroundAssist(t *testing.T) {
	db := &DB{}
	db.observeForegroundFlushAssist(2*time.Millisecond, 3)
	stats := map[string]string{}
	db.appendCacheFlushApplyStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.foreground_assist_calls_total"); got != 1 {
		t.Fatalf("foreground assist calls=%d want 1", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.foreground_assist_flushes_total"); got != 3 {
		t.Fatalf("foreground assist flushes=%d want 3", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.foreground_assist_wait_ns_total"); got == 0 {
		t.Fatalf("foreground assist wait ns=%d want >0", got)
	}
}
