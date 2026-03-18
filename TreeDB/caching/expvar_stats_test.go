package caching

import "testing"

func TestSelectTreeDBExpvarStatsFiltersAndCoerces(t *testing.T) {
	stats := map[string]string{
		"treedb.cache.vlog_mmap.active_bytes":                           "12345",
		"treedb.cache.vlog_mmap.read.hit_ratio":                         "0.625000",
		"treedb.cache.vlog_mmap.enabled":                                "true",
		"treedb.cache.vlog_decode_buffer_grow.calls_total":              "42",
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
	if len(got) != 10 {
		t.Fatalf("selectTreeDBExpvarStats len=%d want 10", len(got))
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
	if v, ok := got["treedb.process.memory.heap_inuse_bytes"].(int64); !ok || v != 4096 {
		t.Fatalf("heap_inuse_bytes=%T(%v) want int64(4096)", got["treedb.process.memory.heap_inuse_bytes"], got["treedb.process.memory.heap_inuse_bytes"])
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
