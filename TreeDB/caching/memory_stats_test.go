package caching

import (
	"strconv"
	"testing"
)

type mockBackendWithStats struct {
	*MockBackend
	stats map[string]string
}

func (m *mockBackendWithStats) Stats() map[string]string {
	if m.stats == nil {
		return m.MockBackend.Stats()
	}
	out := make(map[string]string, len(m.stats))
	for k, v := range m.stats {
		out[k] = v
	}
	return out
}

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
		"treedb.process.memory.rss_bytes",
		"treedb.process.memory.rss_hwm_bytes",
		"treedb.process.memory.rss_minus_heap_inuse_bytes",
		"treedb.process.memory.rss_minus_total_sys_bytes",
		"treedb.process.memory.peak_rss_bytes",
		"treedb.process.memory.peak_heap_alloc_bytes",
		"treedb.process.memory.peak_heap_inuse_bytes",
		"treedb.process.memory.peak_total_sys_bytes",
		"treedb.process.memory.peak_vlog_mmap_active_bytes",
		"treedb.process.memory.peak_vlog_mmap_current_bytes",
		"treedb.process.memory.peak_vlog_mmap_sealed_bytes",
		"treedb.process.memory.peak_vlog_mmap_active_segments",
		"treedb.process.memory.peak_vlog_mmap_current_segments",
		"treedb.process.memory.peak_vlog_mmap_sealed_segments",
		"treedb.process.memory.vlog_mmap_backend_active_bytes",
		"treedb.process.memory.vlog_mmap_backend_current_bytes",
		"treedb.process.memory.vlog_mmap_backend_sealed_bytes",
		"treedb.process.memory.vlog_mmap_backend_dead_bytes",
		"treedb.process.memory.vlog_mmap_backend_active_segments",
		"treedb.process.memory.vlog_mmap_backend_current_segments",
		"treedb.process.memory.vlog_mmap_backend_sealed_segments",
		"treedb.process.memory.vlog_mmap_cache_active_bytes",
		"treedb.process.memory.vlog_mmap_cache_current_bytes",
		"treedb.process.memory.vlog_mmap_cache_sealed_bytes",
		"treedb.process.memory.vlog_mmap_cache_dead_bytes",
		"treedb.process.memory.vlog_mmap_cache_active_segments",
		"treedb.process.memory.vlog_mmap_cache_current_segments",
		"treedb.process.memory.vlog_mmap_cache_sealed_segments",
		"treedb.cache.append_only.mutable_from_lease_total",
		"treedb.cache.append_only.mutable_from_pool_total",
		"treedb.cache.append_only.mutable_new_alloc_total",
		"treedb.process.append_only.mutable_from_lease_total",
		"treedb.process.append_only.mutable_from_pool_total",
		"treedb.process.append_only.mutable_new_alloc_total",
	}
	for _, key := range requiredInt {
		if got := mustStatInt64(t, stats, key); got < 0 {
			t.Fatalf("%s=%d want >=0", key, got)
		}
	}

	if got := mustStatInt64(t, stats, "treedb.process.memory.queue_backlog_bytes"); got != mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes") {
		t.Fatalf("queue_backlog mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_from_lease_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_from_lease_total") {
		t.Fatalf("append_only lease source mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_from_lease_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_from_pool_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_from_pool_total") {
		t.Fatalf("append_only pool source mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_from_pool_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_new_alloc_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_new_alloc_total") {
		t.Fatalf("append_only new source mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_new_alloc_total"))
	}

	rawGCFraction, ok := stats["treedb.process.memory.gc_cpu_fraction"]
	if !ok {
		t.Fatalf("missing stat %q", "treedb.process.memory.gc_cpu_fraction")
	}
	if _, err := strconv.ParseFloat(rawGCFraction, 64); err != nil {
		t.Fatalf("gc_cpu_fraction parse: %v", err)
	}
}

func TestProcessMemoryStatsIncludeBackendVlogMmap(t *testing.T) {
	dir := t.TempDir()
	backend := &mockBackendWithStats{
		MockBackend: NewMockBackend(),
		stats: map[string]string{
			"treedb.vlog.mmap_active_bytes":     "111",
			"treedb.vlog.mmap_current_bytes":    "22",
			"treedb.vlog.mmap_sealed_bytes":     "89",
			"treedb.vlog.mmap_dead_bytes":       "7",
			"treedb.vlog.mmap_active_segments":  "5",
			"treedb.vlog.mmap_current_segments": "2",
			"treedb.vlog.mmap_sealed_segments":  "3",
		},
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:  true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_active_bytes"); got != 111 {
		t.Fatalf("process active_bytes=%d want 111", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_current_bytes"); got != 22 {
		t.Fatalf("process current_bytes=%d want 22", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_sealed_bytes"); got != 89 {
		t.Fatalf("process sealed_bytes=%d want 89", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_dead_bytes"); got != 7 {
		t.Fatalf("process dead_bytes=%d want 7", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_backend_active_bytes"); got != 111 {
		t.Fatalf("backend active_bytes=%d want 111", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_cache_active_bytes"); got != 0 {
		t.Fatalf("cache active_bytes=%d want 0", got)
	}
}
