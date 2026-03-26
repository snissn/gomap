package caching

import "testing"

func TestSelectTreeDBExpvarStatsFiltersAndCoerces(t *testing.T) {
	stats := map[string]string{
		"treedb.process.identity.wal_dir":                               "/tmp/app.db/wal",
		"treedb.vlog.mmap_active_bytes":                                 "22222",
		"treedb.cache.vlog_mmap.active_bytes":                           "12345",
		"treedb.cache.vlog_mmap.read.hit_ratio":                         "0.625000",
		"treedb.cache.vlog_mmap.enabled":                                "true",
		"treedb.cache.vlog_decode_buffer_grow.calls_total":              "42",
		"treedb.cache.vlog_write_mode.raw_bytes.dict":                   "40960",
		"treedb.cache.vlog_auto.bytes.dict":                             "8192",
		"treedb.cache.vlog_dict.current_k":                              "32",
		"treedb.process.memory.heap_inuse_bytes":                        "4096",
		"treedb.process.memory.pool_pressure_level":                     "critical",
		"treedb.cache.batch_arena.pool_bytes_estimate":                  "65536",
		"treedb.process.batch_arena.retained_bytes_global_max_estimate": "1048576",
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
	if v, ok := got["treedb.cache.vlog_auto.bytes.dict"].(int64); !ok || v != 8192 {
		t.Fatalf("vlog_auto.bytes.dict=%T(%v) want int64(8192)", got["treedb.cache.vlog_auto.bytes.dict"], got["treedb.cache.vlog_auto.bytes.dict"])
	}
	if v, ok := got["treedb.cache.vlog_dict.current_k"].(int64); !ok || v != 32 {
		t.Fatalf("vlog_dict.current_k=%T(%v) want int64(32)", got["treedb.cache.vlog_dict.current_k"], got["treedb.cache.vlog_dict.current_k"])
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

func TestCurrentTreeDBExpvarStatsIncludesInstances(t *testing.T) {
	treedbExpvarDBsMu.Lock()
	oldDBs := treedbExpvarDBs
	treedbExpvarDBs = make(map[*DB]struct{})
	treedbExpvarDBsMu.Unlock()
	oldCurrent := treedbExpvarCurrentDB.Load()
	treedbExpvarCurrentDB.Store(nil)
	defer func() {
		treedbExpvarCurrentDB.Store(oldCurrent)
		treedbExpvarDBsMu.Lock()
		treedbExpvarDBs = oldDBs
		treedbExpvarDBsMu.Unlock()
	}()

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
	instA, ok := instances["/tmp/a/wal"].(map[string]any)
	if !ok {
		t.Fatalf("instance /tmp/a/wal missing or wrong type: %T", instances["/tmp/a/wal"])
	}
	instB, ok := instances["/tmp/b/wal"].(map[string]any)
	if !ok {
		t.Fatalf("instance /tmp/b/wal missing or wrong type: %T", instances["/tmp/b/wal"])
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
