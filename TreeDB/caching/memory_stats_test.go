package caching

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
		"treedb.process.memtable_residency.mutable.total.count",
		"treedb.process.memtable_residency.mutable.total.entries",
		"treedb.process.memtable_residency.mutable.total.size_bytes",
		"treedb.process.memtable_residency.mutable.total.entry_capacity",
		"treedb.process.memtable_residency.mutable.total.entry_backing_bytes",
		"treedb.process.memtable_residency.mutable.total.value_arena_active_bytes",
		"treedb.process.memtable_residency.mutable.total.value_arena_retained_bytes",
		"treedb.process.memtable_residency.mutable.append_only.entry_capacity",
		"treedb.process.memtable_residency.mutable.append_only.entry_backing_bytes",
		"treedb.process.memtable_residency.mutable.append_only.value_arena_active_chunks",
		"treedb.process.memtable_residency.mutable.append_only.value_arena_active_bytes",
		"treedb.process.memtable_residency.mutable.append_only.value_arena_retained_chunks",
		"treedb.process.memtable_residency.mutable.append_only.value_arena_retained_bytes",
		"treedb.process.memtable_residency.queue.total.count",
		"treedb.process.memtable_residency.queue.total.entries",
		"treedb.process.memtable_residency.queue.total.size_bytes",
		"treedb.process.memtable_residency.queue.total.entry_capacity",
		"treedb.process.memtable_residency.queue.total.entry_backing_bytes",
		"treedb.process.memtable_residency.queue.total.value_arena_active_bytes",
		"treedb.process.memtable_residency.queue.total.value_arena_retained_bytes",
		"treedb.process.memtable_residency.queue.append_only.entry_capacity",
		"treedb.process.memtable_residency.queue.append_only.entry_backing_bytes",
		"treedb.process.memtable_residency.queue.append_only.value_arena_active_chunks",
		"treedb.process.memtable_residency.queue.append_only.value_arena_active_bytes",
		"treedb.process.memtable_residency.queue.append_only.value_arena_retained_chunks",
		"treedb.process.memtable_residency.queue.append_only.value_arena_retained_bytes",
		"treedb.process.read_path.snapshot.queue_inline_hits_total",
		"treedb.process.read_path.snapshot.queue_inline_bytes_total",
		"treedb.process.read_path.snapshot.queue_pointer_hits_total",
		"treedb.process.read_path.snapshot.queue_pointer_bytes_total",
		"treedb.process.read_path.snapshot.backend_hits_total",
		"treedb.process.read_path.snapshot.backend_bytes_total",
		"treedb.process.read_path.backend_tree.getmany.calls_total",
		"treedb.process.read_path.backend_tree.getmany.grouped_calls_total",
		"treedb.process.read_path.backend_tree.getmany.fallback_calls_total",
		"treedb.process.read_path.backend_tree.getmany.leaf_groups_total",
		"treedb.process.read_path.backend_tree.getmany.leaf_group_items_total",
		"treedb.process.read_path.backend_tree.getmany.leaf_loads_saved_total",
		"treedb.process.read_path.outer_leaf.loads_total",
		"treedb.process.read_path.outer_leaf.point_loads_total",
		"treedb.process.read_path.outer_leaf.iterator_loads_total",
		"treedb.process.read_path.outer_leaf.bytes_total",
		"treedb.process.read_path.outer_leaf.sample_mod",
		"treedb.process.read_path.outer_leaf.samples_total",
		"treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hits_total",
		"treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hits_total",
		"treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hits_total",
		"treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hits_total",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores",
		"treedb.process.read_path.outer_leaf.cache.write_admission_attempts",
		"treedb.process.read_path.outer_leaf.cache.write_admission_stores",
		"treedb.process.read_path.outer_leaf.cache.write_admission_skips",
		"treedb.process.read_path.outer_leaf.cache.write_admission_lock_skips",
		"treedb.process.batch.set.calls_total",
		"treedb.process.batch.set.bytes_total",
		"treedb.process.batch.set_view.calls_total",
		"treedb.process.batch.set_view.bytes_total",
		"treedb.process.batch.set_caller.sample_mod",
		"treedb.process.batch.set_caller.samples_total",
		"treedb.process.read_path.db_get_caller.sample_mod",
		"treedb.process.read_path.db_get_caller.samples_total",
		"treedb.process.read_path.snapshot_get_caller.sample_mod",
		"treedb.process.read_path.snapshot_get_caller.samples_total",
		"treedb.cache.append_only.mutable_from_lease_total",
		"treedb.cache.append_only.mutable_from_pool_total",
		"treedb.cache.append_only.mutable_pool_puts_total",
		"treedb.cache.append_only.mutable_pool_entry_backing_dropped_bytes_total",
		"treedb.cache.append_only.mutable_new_alloc_total",
		"treedb.cache.append_only.mem_lease_value_arena_active_bytes",
		"treedb.cache.append_only.mem_lease_value_arena_retained_bytes",
		"treedb.cache.append_only.mutable_value_arena_active_bytes",
		"treedb.cache.append_only.mutable_value_arena_retained_bytes",
		"treedb.cache.append_only.entry_pool_retained_bytes_estimate",
		"treedb.cache.append_only.entry_pool_retained_bytes_max_estimate",
		"treedb.cache.append_only.entry_pool_gets_total",
		"treedb.cache.append_only.entry_pool_puts_total",
		"treedb.cache.append_only.entry_pool_drops_total",
		"treedb.cache.append_only.entry_pool_drop_bytes_total",
		"treedb.cache.append_only.entry_pool_admission_drops_total",
		"treedb.cache.append_only.entry_pool_admission_drop_bytes_total",
		"treedb.cache.append_only.value_arena_pool_retained_bytes_estimate",
		"treedb.cache.append_only.value_arena_pool_retained_bytes_max_estimate",
		"treedb.cache.append_only.value_arena_pool_gets_total",
		"treedb.cache.append_only.value_arena_pool_puts_total",
		"treedb.cache.append_only.value_arena_pool_drops_total",
		"treedb.cache.append_only.value_arena_pool_drop_bytes_total",
		"treedb.cache.append_only.value_arena_pool_admission_drops_total",
		"treedb.cache.append_only.value_arena_pool_admission_drop_bytes_total",
		"treedb.cache.append_only.reserve.calls_total",
		"treedb.cache.append_only.reserve.entries_total",
		"treedb.cache.append_only.reserve.grow_calls_total",
		"treedb.cache.append_only.reserve.grow_bytes_total",
		"treedb.cache.append_only.reserve.skipped_growth_allocs_total",
		"treedb.cache.append_only.reserve.skipped_growth_bytes_total",
		"treedb.process.append_only.mutable_from_lease_total",
		"treedb.process.append_only.mutable_from_pool_total",
		"treedb.process.append_only.mutable_pool_puts_total",
		"treedb.process.append_only.mutable_pool_entry_backing_dropped_bytes_total",
		"treedb.process.append_only.mutable_new_alloc_total",
		"treedb.process.append_only.mem_lease_value_arena_active_bytes",
		"treedb.process.append_only.mem_lease_value_arena_retained_bytes",
		"treedb.process.append_only.mutable_value_arena_active_bytes",
		"treedb.process.append_only.mutable_value_arena_retained_bytes",
		"treedb.process.append_only.entry_pool_retained_bytes_estimate",
		"treedb.process.append_only.entry_pool_retained_bytes_max_estimate",
		"treedb.process.append_only.entry_pool_gets_total",
		"treedb.process.append_only.entry_pool_puts_total",
		"treedb.process.append_only.entry_pool_drops_total",
		"treedb.process.append_only.entry_pool_drop_bytes_total",
		"treedb.process.append_only.entry_pool_admission_drops_total",
		"treedb.process.append_only.entry_pool_admission_drop_bytes_total",
		"treedb.process.append_only.value_arena_pool_retained_bytes_estimate",
		"treedb.process.append_only.value_arena_pool_retained_bytes_max_estimate",
		"treedb.process.append_only.value_arena_pool_gets_total",
		"treedb.process.append_only.value_arena_pool_puts_total",
		"treedb.process.append_only.value_arena_pool_drops_total",
		"treedb.process.append_only.value_arena_pool_drop_bytes_total",
		"treedb.process.append_only.value_arena_pool_admission_drops_total",
		"treedb.process.append_only.value_arena_pool_admission_drop_bytes_total",
		"treedb.process.append_only.reserve.calls_total",
		"treedb.process.append_only.reserve.entries_total",
		"treedb.process.append_only.reserve.grow_calls_total",
		"treedb.process.append_only.reserve.grow_bytes_total",
		"treedb.process.append_only.reserve.skipped_growth_allocs_total",
		"treedb.process.append_only.reserve.skipped_growth_bytes_total",
	}
	for _, key := range requiredInt {
		if got := mustStatInt64(t, stats, key); got < 0 {
			t.Fatalf("%s=%d want >=0", key, got)
		}
	}
	directArenaSuffixes := []string{
		"active_chunks",
		"active_bytes",
		"active_used_bytes",
		"retained_chunks",
		"retained_bytes",
		"lease_count",
		"lease_chunks",
		"lease_bytes",
	}
	for _, suffix := range directArenaSuffixes {
		cacheKey := "treedb.cache.append_only_direct_arena." + suffix
		processKey := "treedb.process.append_only_direct_arena." + suffix
		if got := mustStatInt64(t, stats, cacheKey); got < 0 {
			t.Fatalf("%s=%d want >=0", cacheKey, got)
		}
		if got := mustStatInt64(t, stats, processKey); got != mustStatInt64(t, stats, cacheKey) {
			t.Fatalf("append_only_direct_arena %s mismatch process=%d cache=%d", suffix, got, mustStatInt64(t, stats, cacheKey))
		}
	}
	if got := stats["treedb.process.read_path.outer_leaf.cache.write_admission_policy"]; got == "" {
		t.Fatalf("missing non-empty write admission policy stat")
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
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_pool_puts_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_pool_puts_total") {
		t.Fatalf("append_only pool puts mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_pool_puts_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_pool_entry_backing_dropped_bytes_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_pool_entry_backing_dropped_bytes_total") {
		t.Fatalf("append_only pool entry backing dropped bytes mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_pool_entry_backing_dropped_bytes_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_new_alloc_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_new_alloc_total") {
		t.Fatalf("append_only new source mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_new_alloc_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mem_lease_value_arena_retained_bytes"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mem_lease_value_arena_retained_bytes") {
		t.Fatalf("append_only lease value arena retained mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mem_lease_value_arena_retained_bytes"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.mutable_value_arena_active_bytes"); got != mustStatInt64(t, stats, "treedb.cache.append_only.mutable_value_arena_active_bytes") {
		t.Fatalf("append_only mutable value arena active mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.mutable_value_arena_active_bytes"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.entry_pool_retained_bytes_estimate"); got != mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_retained_bytes_estimate") {
		t.Fatalf("append_only entry pool retained mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_retained_bytes_estimate"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.entry_pool_drop_bytes_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_drop_bytes_total") {
		t.Fatalf("append_only entry pool drop bytes mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_drop_bytes_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.entry_pool_admission_drops_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_admission_drops_total") {
		t.Fatalf("append_only entry pool admission drops mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_admission_drops_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.entry_pool_admission_drop_bytes_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_admission_drop_bytes_total") {
		t.Fatalf("append_only entry pool admission drop bytes mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.entry_pool_admission_drop_bytes_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.value_arena_pool_retained_bytes_estimate"); got != mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_retained_bytes_estimate") {
		t.Fatalf("append_only value arena pool retained mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_retained_bytes_estimate"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.value_arena_pool_drop_bytes_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_drop_bytes_total") {
		t.Fatalf("append_only value arena pool drop bytes mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_drop_bytes_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.value_arena_pool_admission_drops_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_admission_drops_total") {
		t.Fatalf("append_only value arena pool admission drops mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_admission_drops_total"))
	}
	if got := mustStatInt64(t, stats, "treedb.process.append_only.value_arena_pool_admission_drop_bytes_total"); got != mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_admission_drop_bytes_total") {
		t.Fatalf("append_only value arena pool admission drop bytes mismatch process=%d cache=%d", got, mustStatInt64(t, stats, "treedb.cache.append_only.value_arena_pool_admission_drop_bytes_total"))
	}
	reserveMirrors := []string{
		"calls_total",
		"entries_total",
		"grow_calls_total",
		"grow_bytes_total",
		"skipped_growth_allocs_total",
		"skipped_growth_bytes_total",
	}
	for _, suffix := range reserveMirrors {
		processKey := "treedb.process.append_only.reserve." + suffix
		cacheKey := "treedb.cache.append_only.reserve." + suffix
		if got := mustStatInt64(t, stats, processKey); got != mustStatInt64(t, stats, cacheKey) {
			t.Fatalf("append_only reserve %s mismatch process=%d cache=%d", suffix, got, mustStatInt64(t, stats, cacheKey))
		}
	}

	rawGCFraction, ok := stats["treedb.process.memory.gc_cpu_fraction"]
	if !ok {
		t.Fatalf("missing stat %q", "treedb.process.memory.gc_cpu_fraction")
	}
	if _, err := strconv.ParseFloat(rawGCFraction, 64); err != nil {
		t.Fatalf("gc_cpu_fraction parse: %v", err)
	}
}

func resetBatchSetCallerSamplingForTest() {
	batchSetCallerSamplesTotal.Store(0)
	batchSetCallerSampleSeq.Store(0)
	batchSetCallerStatsMap = sync.Map{}
}

func resetReadCallerSamplingForTest() {
	dbGetCallerSamplesTotal.Store(0)
	dbGetCallerSampleSeq.Store(0)
	dbGetCallerStatsMap = sync.Map{}
	snapshotGetCallerSamplesTotal.Store(0)
	snapshotGetCallerSampleSeq.Store(0)
	snapshotGetCallerStatsMap = sync.Map{}
}

func batchSetCallerStatsTestWrite(t *testing.T, b *Batch) {
	t.Helper()
	if err := b.Set([]byte("caller-key"), []byte("caller-value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestBatchSetCallerStatsSampled(t *testing.T) {
	prevMod := batchSetCallerSampleMod
	batchSetCallerSampleMod = 1
	t.Cleanup(func() { batchSetCallerSampleMod = prevMod })
	resetBatchSetCallerSamplingForTest()

	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		DisableWAL:  true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	batchSetCallerStatsTestWrite(t, b)

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.process.batch.set_caller.samples_total"); got != 1 {
		t.Fatalf("samples_total=%d want 1", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.batch.set_caller.top.0.calls_total"); got != 1 {
		t.Fatalf("top.0.calls_total=%d want 1", got)
	}
	frame := stats["treedb.process.batch.set_caller.top.0.frame"]
	if !strings.Contains(frame, "batchSetCallerStatsTestWrite") {
		t.Fatalf("top.0.frame=%q want caller helper", frame)
	}
}

func dbGetCallerStatsTestRead(t *testing.T, db *DB, key []byte) {
	t.Helper()
	if _, err := db.Get(key); err != nil {
		t.Fatalf("Get: %v", err)
	}
}

func snapshotGetCallerStatsTestRead(t *testing.T, snap *Snapshot, key []byte) {
	t.Helper()
	if _, err := snap.Get(key); err != nil {
		t.Fatalf("Snapshot.Get: %v", err)
	}
}

func TestReadCallerStatsSampled(t *testing.T) {
	prevDBMod := dbGetCallerSampleMod
	prevSnapshotMod := snapshotGetCallerSampleMod
	dbGetCallerSampleMod = 1
	snapshotGetCallerSampleMod = 1
	t.Cleanup(func() {
		dbGetCallerSampleMod = prevDBMod
		snapshotGetCallerSampleMod = prevSnapshotMod
	})
	resetReadCallerSamplingForTest()

	dir := t.TempDir()
	backendDir := dir + "/backend"
	if err := os.MkdirAll(backendDir, 0o755); err != nil {
		t.Fatalf("mkdir backend dir: %v", err)
	}
	backend, err := backenddb.Open(backenddb.Options{Dir: backendDir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:  true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("read-caller-key"), []byte("read-caller-value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	dbGetCallerStatsTestRead(t, db, []byte("read-caller-key"))
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot: nil")
	}
	defer func() { _ = snap.Close() }()
	snapshotGetCallerStatsTestRead(t, snap, []byte("read-caller-key"))

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.process.read_path.db_get_caller.samples_total"); got != 1 {
		t.Fatalf("db_get samples_total=%d want 1", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.read_path.snapshot_get_caller.samples_total"); got != 1 {
		t.Fatalf("snapshot_get samples_total=%d want 1", got)
	}
	dbFrame := stats["treedb.process.read_path.db_get_caller.top.0.frame"]
	if !strings.Contains(dbFrame, "dbGetCallerStatsTestRead") {
		t.Fatalf("db_get top.0.frame=%q want caller helper", dbFrame)
	}
	snapshotFrame := stats["treedb.process.read_path.snapshot_get_caller.top.0.frame"]
	if !strings.Contains(snapshotFrame, "snapshotGetCallerStatsTestRead") {
		t.Fatalf("snapshot_get top.0.frame=%q want caller helper", snapshotFrame)
	}
}

func TestProcessMemoryStatsIncludeBackendVlogMmap(t *testing.T) {
	dir := t.TempDir()
	backend := &mockBackendWithStats{
		MockBackend: NewMockBackend(),
		stats: map[string]string{
			"treedb.vlog.mmap_active_bytes":                    "111",
			"treedb.vlog.mmap_current_bytes":                   "22",
			"treedb.vlog.mmap_sealed_bytes":                    "89",
			"treedb.vlog.mmap_dead_bytes":                      "7",
			"treedb.vlog.mmap_active_segments":                 "5",
			"treedb.vlog.mmap_current_segments":                "2",
			"treedb.vlog.mmap_sealed_segments":                 "3",
			"treedb.vlog.mmap_max_mapped_leaf_sealed_segments": "512",
			"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes":    "1610612736",
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
	if got := mustStatInt64(t, stats, "treedb.vlog.mmap_max_mapped_leaf_sealed_segments"); got != 512 {
		t.Fatalf("backend leaf sealed segment cap=%d want 512", got)
	}
	if got := mustStatInt64(t, stats, "treedb.vlog.mmap_max_mapped_leaf_sealed_bytes"); got != 1610612736 {
		t.Fatalf("backend leaf sealed byte cap=%d want 1610612736", got)
	}
	if got := mustStatInt64(t, stats, "treedb.process.memory.vlog_mmap_cache_active_bytes"); got != 0 {
		t.Fatalf("cache active_bytes=%d want 0", got)
	}
}
