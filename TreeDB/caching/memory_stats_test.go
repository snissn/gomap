package caching

import (
	"strconv"
	"testing"
)

func TestProcessMemoryStatsIncludeRuntimeBreakdown(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableWAL:  true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	stats := db.Stats()

	requiredInt := []string{
		"treedb.process.memory.heap_idle_bytes",
		"treedb.process.memory.heap_released_bytes",
		"treedb.process.memory.stack_inuse_bytes",
		"treedb.process.memory.stack_sys_bytes",
		"treedb.process.memory.total_sys_bytes",
		"treedb.process.memory.non_heap_sys_bytes",
		"treedb.process.memory.next_gc_bytes",
		"treedb.process.memory.num_gc",
		"treedb.process.memory.mutable_bytes",
		"treedb.process.memory.queue_backlog_bytes",
		"treedb.process.memory.memtable_view_deferred_bytes_current",
		"treedb.process.memory.memtable_view_deferred_bytes_max",
		"treedb.process.memory.vlog_retained_bytes_estimate",
	}
	for _, key := range requiredInt {
		if got := mustStatInt64(t, stats, key); got < 0 {
			t.Fatalf("%s=%d want >=0", key, got)
		}
	}

	if got := mustStatInt64(t, stats, "treedb.process.memory.queue_backlog_bytes"); got != mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes") {
		t.Fatalf("queue_backlog mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"))
	}

	rawGCFraction, ok := stats["treedb.process.memory.gc_cpu_fraction"]
	if !ok {
		t.Fatalf("missing stat %q", "treedb.process.memory.gc_cpu_fraction")
	}
	if _, err := strconv.ParseFloat(rawGCFraction, 64); err != nil {
		t.Fatalf("gc_cpu_fraction parse: %v", err)
	}
}
