package caching

import (
	"strings"
	"testing"
)

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

func TestSelectTreeDBExpvarStatsFiltersAndCoerces(t *testing.T) {
	stats := map[string]string{
		"treedb.process.identity.wal_dir":                               "/tmp/app.db/wal",
		"treedb.vlog.mmap_active_bytes":                                 "22222",
		"treedb.cache.vlog_mmap.active_bytes":                           "12345",
		"treedb.cache.vlog_mmap.read.hit_ratio":                         "0.625000",
		"treedb.cache.vlog_mmap.enabled":                                "true",
		"treedb.cache.vlog_decode_buffer_grow.calls_total":              "42",
		"treedb.cache.vlog_write_mode.raw_bytes.dict":                   "40960",
		"treedb.cache.vlog_payload_split.raw_bytes.outer_leaf":          "1024",
		"treedb.cache.vlog_auto.bytes.dict":                             "8192",
		"treedb.cache.vlog_dict.current_k":                              "32",
		"treedb.cache.vlog_payload_kind.raw_bytes.single_value":         "2048",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4":              "512",
		"treedb.cache.vlog_generation.rewrite.reclaimed_bytes":          "1234",
		"treedb.process.memory.heap_inuse_bytes":                        "4096",
		"treedb.process.memory.pool_pressure_level":                     "critical",
		"treedb.cache.batch_arena.pool_bytes_estimate":                  "65536",
		"treedb.process.batch_arena.retained_bytes_global_max_estimate": "1048576",
		"treedb.process.memtable_residency.queue.total.size_bytes":      "2048",
		"treedb.process.read_path.snapshot.backend_bytes_total":         "8192",
		"treedb.process.batch.set.bytes_total":                          "4096",
		"treedb.process.batch.set_caller.top.0.frame":                   "snissn/iavl.batchSetOwned <= cosmossdk.io/store/rootmulti.(*Store).Restore",
		"treedb.process.flush_merge.applied_ops_total":                  "12",
		"treedb.cache.backpressure_mode":                                "adaptive",
		"treedb.cache.entry_slice.trim_runs_total":                      "77",
		"treedb.process.memory.pool_pressure_high_pct":                  "85.5",
	}

	got := selectTreeDBExpvarStats(stats)
	if len(got) < 10 {
		t.Fatalf("selectTreeDBExpvarStats len=%d want at least 10", len(got))
	}

	if v, ok := got["treedb.vlog.mmap_active_bytes"].(int64); !ok || v != 22222 {
		t.Fatalf("backend active_bytes=%T(%v) want int64(22222)", got["treedb.vlog.mmap_active_bytes"], got["treedb.vlog.mmap_active_bytes"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.active_bytes"].(int64); !ok || v != 12345 {
		t.Fatalf("active_bytes=%T(%v) want int64(12345)", got["treedb.cache.vlog_mmap.active_bytes"], got["treedb.cache.vlog_mmap.active_bytes"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.read.hit_ratio"].(float64); !ok || v != 0.625 {
		t.Fatalf("hit_ratio=%T(%v) want float64(0.625)", got["treedb.cache.vlog_mmap.read.hit_ratio"], got["treedb.cache.vlog_mmap.read.hit_ratio"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.enabled"].(bool); !ok || !v {
		t.Fatalf("enabled=%T(%v) want bool(true)", got["treedb.cache.vlog_mmap.enabled"], got["treedb.cache.vlog_mmap.enabled"])
	}
	if v, ok := got["treedb.cache.vlog_decode_buffer_grow.calls_total"].(int64); !ok || v != 42 {
		t.Fatalf("decode_buffer_grow.calls_total=%T(%v) want int64(42)", got["treedb.cache.vlog_decode_buffer_grow.calls_total"], got["treedb.cache.vlog_decode_buffer_grow.calls_total"])
	}
	if v, ok := got["treedb.cache.vlog_write_mode.raw_bytes.dict"].(int64); !ok || v != 40960 {
		t.Fatalf("vlog_write_mode.raw_bytes.dict=%T(%v) want int64(40960)", got["treedb.cache.vlog_write_mode.raw_bytes.dict"], got["treedb.cache.vlog_write_mode.raw_bytes.dict"])
	}
	if v, ok := got["treedb.cache.vlog_payload_split.raw_bytes.outer_leaf"].(int64); !ok || v != 1024 {
		t.Fatalf("vlog_payload_split.raw_bytes.outer_leaf=%T(%v) want int64(1024)", got["treedb.cache.vlog_payload_split.raw_bytes.outer_leaf"], got["treedb.cache.vlog_payload_split.raw_bytes.outer_leaf"])
	}
	if v, ok := got["treedb.cache.vlog_auto.bytes.dict"].(int64); !ok || v != 8192 {
		t.Fatalf("vlog_auto.bytes.dict=%T(%v) want int64(8192)", got["treedb.cache.vlog_auto.bytes.dict"], got["treedb.cache.vlog_auto.bytes.dict"])
	}
	if v, ok := got["treedb.cache.vlog_dict.current_k"].(int64); !ok || v != 32 {
		t.Fatalf("vlog_dict.current_k=%T(%v) want int64(32)", got["treedb.cache.vlog_dict.current_k"], got["treedb.cache.vlog_dict.current_k"])
	}
	if v, ok := got["treedb.cache.vlog_payload_kind.raw_bytes.single_value"].(int64); !ok || v != 2048 {
		t.Fatalf("vlog_payload_kind.raw_bytes.single_value=%T(%v) want int64(2048)", got["treedb.cache.vlog_payload_kind.raw_bytes.single_value"], got["treedb.cache.vlog_payload_kind.raw_bytes.single_value"])
	}
	if v, ok := got["treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4"].(int64); !ok || v != 512 {
		t.Fatalf("vlog_outer_leaf_codec.raw_bytes.lz4=%T(%v) want int64(512)", got["treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4"], got["treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4"])
	}
	if v, ok := got["treedb.cache.vlog_generation.rewrite.reclaimed_bytes"].(int64); !ok || v != 1234 {
		t.Fatalf("vlog_generation.rewrite.reclaimed_bytes=%T(%v) want int64(1234)", got["treedb.cache.vlog_generation.rewrite.reclaimed_bytes"], got["treedb.cache.vlog_generation.rewrite.reclaimed_bytes"])
	}
	if v, ok := got["treedb.process.memory.heap_inuse_bytes"].(int64); !ok || v != 4096 {
		t.Fatalf("heap_inuse_bytes=%T(%v) want int64(4096)", got["treedb.process.memory.heap_inuse_bytes"], got["treedb.process.memory.heap_inuse_bytes"])
	}
	if v, ok := got["treedb.process.identity.wal_dir"].(string); !ok || v != "/tmp/app.db/wal" {
		t.Fatalf("identity.wal_dir=%T(%v) want /tmp/app.db/wal", got["treedb.process.identity.wal_dir"], got["treedb.process.identity.wal_dir"])
	}
	if v, ok := got["treedb.process.memory.pool_pressure_level"].(string); !ok || v != "critical" {
		t.Fatalf("pool_pressure_level=%T(%v) want string(critical)", got["treedb.process.memory.pool_pressure_level"], got["treedb.process.memory.pool_pressure_level"])
	}
	if v, ok := got["treedb.cache.batch_arena.pool_bytes_estimate"].(int64); !ok || v != 65536 {
		t.Fatalf("batch_arena.pool_bytes_estimate=%T(%v) want int64(65536)", got["treedb.cache.batch_arena.pool_bytes_estimate"], got["treedb.cache.batch_arena.pool_bytes_estimate"])
	}
	if v, ok := got["treedb.process.batch_arena.retained_bytes_global_max_estimate"].(int64); !ok || v != 1048576 {
		t.Fatalf("batch_arena.retained_bytes_global_max_estimate=%T(%v) want int64(1048576)", got["treedb.process.batch_arena.retained_bytes_global_max_estimate"], got["treedb.process.batch_arena.retained_bytes_global_max_estimate"])
	}
	if v, ok := got["treedb.process.memtable_residency.queue.total.size_bytes"].(int64); !ok || v != 2048 {
		t.Fatalf("memtable_residency.queue.total.size_bytes=%T(%v) want int64(2048)", got["treedb.process.memtable_residency.queue.total.size_bytes"], got["treedb.process.memtable_residency.queue.total.size_bytes"])
	}
	if v, ok := got["treedb.process.read_path.snapshot.backend_bytes_total"].(int64); !ok || v != 8192 {
		t.Fatalf("read_path.snapshot.backend_bytes_total=%T(%v) want int64(8192)", got["treedb.process.read_path.snapshot.backend_bytes_total"], got["treedb.process.read_path.snapshot.backend_bytes_total"])
	}
	if v, ok := got["treedb.process.batch.set.bytes_total"].(int64); !ok || v != 4096 {
		t.Fatalf("batch.set.bytes_total=%T(%v) want int64(4096)", got["treedb.process.batch.set.bytes_total"], got["treedb.process.batch.set.bytes_total"])
	}
	if v, ok := got["treedb.process.batch.set_caller.top.0.frame"].(string); !ok || !containsAll(v, "iavl.batchSetOwned", "rootmulti") {
		t.Fatalf("batch.set_caller.top.0.frame=%T(%v) want caller string", got["treedb.process.batch.set_caller.top.0.frame"], got["treedb.process.batch.set_caller.top.0.frame"])
	}
	if v, ok := got["treedb.process.flush_merge.applied_ops_total"].(int64); !ok || v != 12 {
		t.Fatalf("flush_merge.applied_ops_total=%T(%v) want int64(12)", got["treedb.process.flush_merge.applied_ops_total"], got["treedb.process.flush_merge.applied_ops_total"])
	}
	if _, ok := got["treedb.cache.backpressure_mode"]; ok {
		t.Fatalf("unexpected backpressure_mode key in expvar selection")
	}
}

func TestSelectTreeDBExpvarStatsEmpty(t *testing.T) {
	got := selectTreeDBExpvarStats(nil)
	if len(got) != 0 {
		t.Fatalf("selectTreeDBExpvarStats(nil) len=%d want 0", len(got))
	}
}

func resetTreeDBExpvarRegistryForTest(t *testing.T) {
	t.Helper()
	treedbExpvarDBsMu.Lock()
	oldDBs := treedbExpvarDBs
	treedbExpvarDBs = make(map[*DB]struct{})
	treedbExpvarDBsMu.Unlock()
	oldCurrent := treedbExpvarCurrentDB.Load()
	treedbExpvarCurrentDB.Store(nil)
	t.Cleanup(func() {
		treedbExpvarCurrentDB.Store(oldCurrent)
		treedbExpvarDBsMu.Lock()
		treedbExpvarDBs = oldDBs
		treedbExpvarDBsMu.Unlock()
	})
}

func findExpvarInstanceByWalDir(instances map[string]any, walDir string) (map[string]any, bool) {
	for _, raw := range instances {
		inst, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if inst["treedb.expvar.wal_dir"] == walDir {
			return inst, true
		}
	}
	return nil, false
}

func TestCurrentTreeDBExpvarStatsIncludesInstances(t *testing.T) {
	resetTreeDBExpvarRegistryForTest(t)

	dbA := &DB{
		dir: "/tmp/a/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir":        "/tmp/a/wal",
				"treedb.process.memory.heap_inuse_bytes": "111",
				"treedb.vlog.mmap_active_bytes":          "7",
			},
		},
	}
	dbB := &DB{
		dir: "/tmp/b/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir":        "/tmp/b/wal",
				"treedb.process.memory.heap_inuse_bytes": "222",
				"treedb.vlog.mmap_active_bytes":          "9",
			},
		},
	}

	registerTreeDBExpvarStatsDB(dbA)
	registerTreeDBExpvarStatsDB(dbB)

	got := currentTreeDBExpvarStats()
	if got["treedb.expvar.current_wal_dir"] != "/tmp/b/wal" {
		t.Fatalf("current_wal_dir=%v want /tmp/b/wal", got["treedb.expvar.current_wal_dir"])
	}
	if got["treedb.expvar.instances_count"] != 2 {
		t.Fatalf("instances_count=%v want 2", got["treedb.expvar.instances_count"])
	}
	instances, ok := got["instances"].(map[string]any)
	if !ok {
		t.Fatalf("instances=%T want map[string]any", got["instances"])
	}
	if len(instances) != 2 {
		t.Fatalf("len(instances)=%d want 2", len(instances))
	}
	instA, ok := findExpvarInstanceByWalDir(instances, "/tmp/a/wal")
	if !ok {
		t.Fatalf("instance /tmp/a/wal missing")
	}
	instB, ok := findExpvarInstanceByWalDir(instances, "/tmp/b/wal")
	if !ok {
		t.Fatalf("instance /tmp/b/wal missing")
	}
	if instA["treedb.expvar.is_current"] != false {
		t.Fatalf("instance a is_current=%v want false", instA["treedb.expvar.is_current"])
	}
	if instB["treedb.expvar.is_current"] != true {
		t.Fatalf("instance b is_current=%v want true", instB["treedb.expvar.is_current"])
	}
	if instA["treedb.expvar.wal_dir"] != "/tmp/a/wal" || instB["treedb.expvar.wal_dir"] != "/tmp/b/wal" {
		t.Fatalf("unexpected instance wal dirs: a=%v b=%v", instA["treedb.expvar.wal_dir"], instB["treedb.expvar.wal_dir"])
	}
}

func TestCurrentTreeDBExpvarStatsDuplicateWalDirKeepsDistinctInstances(t *testing.T) {
	resetTreeDBExpvarRegistryForTest(t)

	dbA := &DB{
		dir: "/tmp/shared/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir":        "/tmp/shared/wal",
				"treedb.process.memory.heap_inuse_bytes": "111",
			},
		},
	}
	dbB := &DB{
		dir: "/tmp/shared/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir":        "/tmp/shared/wal",
				"treedb.process.memory.heap_inuse_bytes": "222",
			},
		},
	}

	registerTreeDBExpvarStatsDB(dbA)
	registerTreeDBExpvarStatsDB(dbB)

	got := currentTreeDBExpvarStats()
	instances, ok := got["instances"].(map[string]any)
	if !ok {
		t.Fatalf("instances=%T want map[string]any", got["instances"])
	}
	if got["treedb.expvar.instances_count"] != 2 {
		t.Fatalf("instances_count=%v want 2", got["treedb.expvar.instances_count"])
	}
	if len(instances) != 2 {
		t.Fatalf("len(instances)=%d want 2", len(instances))
	}
	seenSharedWalDir := 0
	for key, raw := range instances {
		inst, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("instance %q type=%T want map[string]any", key, raw)
		}
		if inst["treedb.expvar.wal_dir"] == "/tmp/shared/wal" {
			seenSharedWalDir++
		}
	}
	if seenSharedWalDir != 2 {
		t.Fatalf("instances with shared wal dir=%d want 2", seenSharedWalDir)
	}
}

func TestUnregisterTreeDBExpvarStatsDBKeepsCurrentWhenOthersRemain(t *testing.T) {
	resetTreeDBExpvarRegistryForTest(t)

	dbA := &DB{
		dir: "/tmp/a/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir": "/tmp/a/wal",
			},
		},
	}
	dbB := &DB{
		dir: "/tmp/b/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir": "/tmp/b/wal",
			},
		},
	}

	registerTreeDBExpvarStatsDB(dbA)
	registerTreeDBExpvarStatsDB(dbB)
	unregisterTreeDBExpvarStatsDB(dbB)

	got := currentTreeDBExpvarStats()
	if got["treedb.expvar.current_wal_dir"] != "/tmp/a/wal" {
		t.Fatalf("current_wal_dir=%v want /tmp/a/wal", got["treedb.expvar.current_wal_dir"])
	}
	if got["treedb.expvar.instances_count"] != 1 {
		t.Fatalf("instances_count=%v want 1", got["treedb.expvar.instances_count"])
	}
	instances, ok := got["instances"].(map[string]any)
	if !ok {
		t.Fatalf("instances=%T want map[string]any", got["instances"])
	}
	if len(instances) != 1 {
		t.Fatalf("len(instances)=%d want 1", len(instances))
	}
	if _, ok := findExpvarInstanceByWalDir(instances, "/tmp/a/wal"); !ok {
		t.Fatalf("instance /tmp/a/wal missing after unregister")
	}
}
