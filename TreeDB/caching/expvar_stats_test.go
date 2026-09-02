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
		"treedb.process.identity.wal_dir":                                                             "/tmp/app.db/wal",
		"treedb.command_wal.enabled":                                                                  "true",
		"treedb.command_wal.writer.command_buffer.capacity_bytes":                                     "4194304",
		"treedb.command_wal.writer.command_buffer.retain_limit_bytes":                                 "4194304",
		"treedb.command_wal.writer.command_buffer.trim_count":                                         "7",
		"treedb.command_wal.writer.command_buffer.dropped_bytes_total":                                "67108864",
		"treedb.command_wal.writer.pending_batch.capacity_bytes":                                      "32768",
		"treedb.command_wal.public_batch.set_view.calls_total":                                        "11",
		"treedb.public.batch.write_sync.ns_total":                                                     "123456789",
		"treedb.public.checkpoint.calls_total":                                                        "4",
		"treedb.maintenance.full_scan.gc_runs":                                                        "2",
		"treedb.bg_vacuum.vacuums":                                                                    "5",
		"treedb.vlog.mmap_active_bytes":                                                               "22222",
		"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes":                                               "1610612736",
		"treedb.vlog.decode_buffer_grow.read_append_decoded_payload.calls_total":                      "17",
		"treedb.vlog.decode_scratch.small_pool.retained_bytes":                                        "16384",
		"treedb.vlog.writer_append_buf.pool.retained_bytes":                                           "4194304",
		"treedb.cache.vlog_mmap.active_bytes":                                                         "12345",
		"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments":                                      "512",
		"treedb.cache.vlog_mmap.read.hit_ratio":                                                       "0.625000",
		"treedb.cache.vlog_mmap.enabled":                                                              "true",
		"treedb.cache.vlog_grouped_frame_cache.retained_bytes":                                        "65536",
		"treedb.cache.vlog_grouped_frame_cache.allocated_slots":                                       "64",
		"treedb.cache.vlog_grouped_frame_cache.hit_ratio":                                             "0.750000",
		"treedb.cache.vlog_decode_buffer_grow.calls_total":                                            "42",
		"treedb.cache.vlog_decode_scratch.small_pool.retained_bytes":                                  "32768",
		"treedb.cache.vlog_writer_append_buf.drops_total":                                             "3",
		"treedb.cache.vlog_write_mode.raw_bytes.dict":                                                 "40960",
		"treedb.cache.vlog_payload_split.raw_bytes.outer_leaf":                                        "1024",
		"treedb.cache.vlog_auto.bytes.dict":                                                           "8192",
		"treedb.cache.vlog_dict.current_k":                                                            "32",
		"treedb.cache.vlog_payload_kind.raw_bytes.single_value":                                       "2048",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4":                                            "512",
		"treedb.cache.vlog_generation.rewrite.reclaimed_bytes":                                        "1234",
		"treedb.cache.vlog_retained_segments":                                                         "9",
		"treedb.cache.vlog_retained_bytes_estimate":                                                   "987654",
		"treedb.cache.vlog_retained_prune.runs":                                                       "3",
		"treedb.cache.vlog_zombie.pinned_bytes":                                                       "4096",
		"treedb.process.memory.heap_inuse_bytes":                                                      "4096",
		"treedb.process.memory.pool_pressure_level":                                                   "critical",
		"treedb.cache.batch_arena.pool_bytes_estimate":                                                "65536",
		"treedb.process.batch_arena.retained_bytes_global_max_estimate":                               "1048576",
		"treedb.process.memtable_residency.queue.total.size_bytes":                                    "2048",
		"treedb.process.memtable_residency.queue.append_only.entry_backing_bytes":                     "7340032",
		"treedb.process.memtable_residency.queue.append_only.value_arena_retained_bytes":              "32768",
		"treedb.process.append_only.mem_lease_value_arena_retained_bytes":                             "65536",
		"treedb.process.append_only.value_arena_pool_retained_bytes_estimate":                         "33554432",
		"treedb.cache.append_only_direct_arena.active_bytes":                                          "262144",
		"treedb.cache.append_only_direct_arena.lease_bytes":                                           "131072",
		"treedb.process.append_only_direct_arena.retained_bytes":                                      "65536",
		"treedb.process.read_path.snapshot.backend_bytes_total":                                       "8192",
		"treedb.process.batch.set.bytes_total":                                                        "4096",
		"treedb.process.batch.delete_view.calls_total":                                                "5",
		"treedb.process.batch.delete_view.bytes_total":                                                "256",
		"treedb.process.batch.set_caller.top.0.frame":                                                 "snissn/iavl.batchSetOwned <= cosmossdk.io/store/rootmulti.(*Store).Restore",
		"treedb.process.flush_merge.applied_ops_total":                                                "12",
		"treedb.flush_admission.flush_apply_span_native":                                              "true",
		"treedb.flush_apply.span_native.used_ops_total":                                               "123",
		"treedb.flush_apply.span_native.fallback.reason.disabled.ops_total":                           "4",
		"treedb.raw.span_native.route.point_put.used_ops_total":                                       "99",
		"treedb.raw.span_native.route.mixed_range_delete.fallbacks_total":                             "2",
		"treedb.publish.ordered_root_delta_group.span_native.used_ops_total":                          "77",
		"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.status": "eligible",
		"treedb.leaf_generation.pack.runs":                                                            "5",
		"treedb.cache.flush_apply.foreground_assist_wait_ns_total":                                    "12345",
		"treedb.cache.flush_span_run.ops_per_span":                                                    "8.5",
		"treedb.cache.flush_backlog_coalescing.skip.reason.not_enough_backlog_total":                  "6",
		"treedb.cache.checkpoint.runs":                                                                "3",
		"treedb.cache.checkpoint.total_ms":                                                            "123.5",
		"treedb.cache.checkpoint.stage.backend_boundary.total_ns":                                     "987654321",
		"treedb.cache.auto_checkpoint.count":                                                          "8",
		"treedb.cache.auto_checkpoint.last_reason":                                                    "size",
		"treedb.cache.command_wal.checkpoint_publish.piggybacked":                                     "2",
		"treedb.cache.command_wal.checkpoint_publish.separate":                                        "1",
		"treedb.cache.leaf_log_lanes.append_bytes_total":                                              "4096",
		"treedb.cache.vlog_queue.depth_max":                                                           "12",
		"treedb.cache.vlog_shape.bytes_total":                                                         "777",
		"treedb.cache.write.wait_for_checkpoint.count_total":                                          "4",
		"treedb.cache.backpressure_mode":                                                              "adaptive",
		"treedb.cache.entry_slice.trim_runs_total":                                                    "77",
		"treedb.process.memory.pool_pressure_high_pct":                                                "85.5",
	}

	got := selectTreeDBExpvarStats(stats)
	if len(got) < 10 {
		t.Fatalf("selectTreeDBExpvarStats len=%d want at least 10", len(got))
	}

	for key, want := range map[string]any{
		"treedb.maintenance.full_scan.gc_runs":                    int64(2),
		"treedb.bg_vacuum.vacuums":                                int64(5),
		"treedb.cache.checkpoint.runs":                            int64(3),
		"treedb.cache.checkpoint.total_ms":                        123.5,
		"treedb.cache.checkpoint.stage.backend_boundary.total_ns": int64(987654321),
		"treedb.cache.auto_checkpoint.count":                      int64(8),
		"treedb.cache.auto_checkpoint.last_reason":                "size",
		"treedb.public.batch.write_sync.ns_total":                 int64(123456789),
		"treedb.public.checkpoint.calls_total":                    int64(4),
		"treedb.cache.leaf_log_lanes.append_bytes_total":          int64(4096),
		"treedb.cache.vlog_queue.depth_max":                       int64(12),
		"treedb.cache.vlog_shape.bytes_total":                     int64(777),
	} {
		if got[key] != want {
			t.Fatalf("%s=%T(%v) want %T(%v)", key, got[key], got[key], want, want)
		}
	}

	if v, ok := got["treedb.vlog.mmap_active_bytes"].(int64); !ok || v != 22222 {
		t.Fatalf("backend active_bytes=%T(%v) want int64(22222)", got["treedb.vlog.mmap_active_bytes"], got["treedb.vlog.mmap_active_bytes"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.active_bytes"].(int64); !ok || v != 12345 {
		t.Fatalf("active_bytes=%T(%v) want int64(12345)", got["treedb.cache.vlog_mmap.active_bytes"], got["treedb.cache.vlog_mmap.active_bytes"])
	}
	if v, ok := got["treedb.vlog.mmap_max_mapped_leaf_sealed_bytes"].(int64); !ok || v != 1610612736 {
		t.Fatalf("backend leaf mmap byte cap=%T(%v) want int64(1610612736)", got["treedb.vlog.mmap_max_mapped_leaf_sealed_bytes"], got["treedb.vlog.mmap_max_mapped_leaf_sealed_bytes"])
	}
	if v, ok := got["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.calls_total"].(int64); !ok || v != 17 {
		t.Fatalf("backend decoded payload calls=%T(%v) want int64(17)", got["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.calls_total"], got["treedb.vlog.decode_buffer_grow.read_append_decoded_payload.calls_total"])
	}
	if v, ok := got["treedb.vlog.decode_scratch.small_pool.retained_bytes"].(int64); !ok || v != 16384 {
		t.Fatalf("backend decode_scratch.small_pool.retained_bytes=%T(%v) want int64(16384)", got["treedb.vlog.decode_scratch.small_pool.retained_bytes"], got["treedb.vlog.decode_scratch.small_pool.retained_bytes"])
	}
	if v, ok := got["treedb.vlog.writer_append_buf.pool.retained_bytes"].(int64); !ok || v != 4194304 {
		t.Fatalf("backend writer append pool retained_bytes=%T(%v) want int64(4194304)", got["treedb.vlog.writer_append_buf.pool.retained_bytes"], got["treedb.vlog.writer_append_buf.pool.retained_bytes"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments"].(int64); !ok || v != 512 {
		t.Fatalf("cache leaf mmap segment cap=%T(%v) want int64(512)", got["treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments"], got["treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.read.hit_ratio"].(float64); !ok || v != 0.625 {
		t.Fatalf("hit_ratio=%T(%v) want float64(0.625)", got["treedb.cache.vlog_mmap.read.hit_ratio"], got["treedb.cache.vlog_mmap.read.hit_ratio"])
	}
	if v, ok := got["treedb.cache.vlog_mmap.enabled"].(bool); !ok || !v {
		t.Fatalf("enabled=%T(%v) want bool(true)", got["treedb.cache.vlog_mmap.enabled"], got["treedb.cache.vlog_mmap.enabled"])
	}
	if v, ok := got["treedb.cache.vlog_grouped_frame_cache.retained_bytes"].(int64); !ok || v != 65536 {
		t.Fatalf("grouped cache retained_bytes=%T(%v) want int64(65536)", got["treedb.cache.vlog_grouped_frame_cache.retained_bytes"], got["treedb.cache.vlog_grouped_frame_cache.retained_bytes"])
	}
	if v, ok := got["treedb.cache.vlog_grouped_frame_cache.allocated_slots"].(int64); !ok || v != 64 {
		t.Fatalf("grouped cache allocated_slots=%T(%v) want int64(64)", got["treedb.cache.vlog_grouped_frame_cache.allocated_slots"], got["treedb.cache.vlog_grouped_frame_cache.allocated_slots"])
	}
	if v, ok := got["treedb.cache.vlog_grouped_frame_cache.hit_ratio"].(float64); !ok || v != 0.75 {
		t.Fatalf("grouped cache hit_ratio=%T(%v) want float64(0.75)", got["treedb.cache.vlog_grouped_frame_cache.hit_ratio"], got["treedb.cache.vlog_grouped_frame_cache.hit_ratio"])
	}
	if v, ok := got["treedb.cache.vlog_decode_buffer_grow.calls_total"].(int64); !ok || v != 42 {
		t.Fatalf("decode_buffer_grow.calls_total=%T(%v) want int64(42)", got["treedb.cache.vlog_decode_buffer_grow.calls_total"], got["treedb.cache.vlog_decode_buffer_grow.calls_total"])
	}
	if v, ok := got["treedb.cache.vlog_decode_scratch.small_pool.retained_bytes"].(int64); !ok || v != 32768 {
		t.Fatalf("decode_scratch.small_pool.retained_bytes=%T(%v) want int64(32768)", got["treedb.cache.vlog_decode_scratch.small_pool.retained_bytes"], got["treedb.cache.vlog_decode_scratch.small_pool.retained_bytes"])
	}
	if v, ok := got["treedb.cache.vlog_writer_append_buf.drops_total"].(int64); !ok || v != 3 {
		t.Fatalf("writer append buffer drops_total=%T(%v) want int64(3)", got["treedb.cache.vlog_writer_append_buf.drops_total"], got["treedb.cache.vlog_writer_append_buf.drops_total"])
	}
	if v, ok := got["treedb.cache.vlog_write_mode.raw_bytes.dict"].(int64); !ok || v != 40960 {
		t.Fatalf("vlog_write_mode.raw_bytes.dict=%T(%v) want int64(40960)", got["treedb.cache.vlog_write_mode.raw_bytes.dict"], got["treedb.cache.vlog_write_mode.raw_bytes.dict"])
	}
	if v, ok := got["treedb.process.append_only.value_arena_pool_retained_bytes_estimate"].(int64); !ok || v != 33554432 {
		t.Fatalf("append_only value arena pool retained=%T(%v) want int64(33554432)", got["treedb.process.append_only.value_arena_pool_retained_bytes_estimate"], got["treedb.process.append_only.value_arena_pool_retained_bytes_estimate"])
	}
	if v, ok := got["treedb.cache.append_only_direct_arena.active_bytes"].(int64); !ok || v != 262144 {
		t.Fatalf("append_only_direct_arena active bytes=%T(%v) want int64(262144)", got["treedb.cache.append_only_direct_arena.active_bytes"], got["treedb.cache.append_only_direct_arena.active_bytes"])
	}
	if v, ok := got["treedb.cache.append_only_direct_arena.lease_bytes"].(int64); !ok || v != 131072 {
		t.Fatalf("append_only_direct_arena lease bytes=%T(%v) want int64(131072)", got["treedb.cache.append_only_direct_arena.lease_bytes"], got["treedb.cache.append_only_direct_arena.lease_bytes"])
	}
	if v, ok := got["treedb.process.append_only_direct_arena.retained_bytes"].(int64); !ok || v != 65536 {
		t.Fatalf("process append_only_direct_arena retained bytes=%T(%v) want int64(65536)", got["treedb.process.append_only_direct_arena.retained_bytes"], got["treedb.process.append_only_direct_arena.retained_bytes"])
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
	if v, ok := got["treedb.cache.vlog_retained_segments"].(int64); !ok || v != 9 {
		t.Fatalf("vlog_retained_segments=%T(%v) want int64(9)", got["treedb.cache.vlog_retained_segments"], got["treedb.cache.vlog_retained_segments"])
	}
	if v, ok := got["treedb.cache.vlog_retained_bytes_estimate"].(int64); !ok || v != 987654 {
		t.Fatalf("vlog_retained_bytes_estimate=%T(%v) want int64(987654)", got["treedb.cache.vlog_retained_bytes_estimate"], got["treedb.cache.vlog_retained_bytes_estimate"])
	}
	if v, ok := got["treedb.cache.vlog_retained_prune.runs"].(int64); !ok || v != 3 {
		t.Fatalf("vlog_retained_prune.runs=%T(%v) want int64(3)", got["treedb.cache.vlog_retained_prune.runs"], got["treedb.cache.vlog_retained_prune.runs"])
	}
	if v, ok := got["treedb.cache.vlog_zombie.pinned_bytes"].(int64); !ok || v != 4096 {
		t.Fatalf("vlog_zombie.pinned_bytes=%T(%v) want int64(4096)", got["treedb.cache.vlog_zombie.pinned_bytes"], got["treedb.cache.vlog_zombie.pinned_bytes"])
	}
	if v, ok := got["treedb.process.memory.heap_inuse_bytes"].(int64); !ok || v != 4096 {
		t.Fatalf("heap_inuse_bytes=%T(%v) want int64(4096)", got["treedb.process.memory.heap_inuse_bytes"], got["treedb.process.memory.heap_inuse_bytes"])
	}
	if v, ok := got["treedb.process.identity.wal_dir"].(string); !ok || v != "/tmp/app.db/wal" {
		t.Fatalf("identity.wal_dir=%T(%v) want /tmp/app.db/wal", got["treedb.process.identity.wal_dir"], got["treedb.process.identity.wal_dir"])
	}
	if v, ok := got["treedb.command_wal.enabled"].(bool); !ok || !v {
		t.Fatalf("command_wal.enabled=%T(%v) want bool(true)", got["treedb.command_wal.enabled"], got["treedb.command_wal.enabled"])
	}
	if v, ok := got["treedb.command_wal.writer.command_buffer.capacity_bytes"].(int64); !ok || v != 4194304 {
		t.Fatalf("command_buffer.capacity_bytes=%T(%v) want int64(4194304)", got["treedb.command_wal.writer.command_buffer.capacity_bytes"], got["treedb.command_wal.writer.command_buffer.capacity_bytes"])
	}
	if v, ok := got["treedb.command_wal.writer.command_buffer.retain_limit_bytes"].(int64); !ok || v != 4194304 {
		t.Fatalf("command_buffer.retain_limit_bytes=%T(%v) want int64(4194304)", got["treedb.command_wal.writer.command_buffer.retain_limit_bytes"], got["treedb.command_wal.writer.command_buffer.retain_limit_bytes"])
	}
	if v, ok := got["treedb.command_wal.writer.command_buffer.trim_count"].(int64); !ok || v != 7 {
		t.Fatalf("command_buffer.trim_count=%T(%v) want int64(7)", got["treedb.command_wal.writer.command_buffer.trim_count"], got["treedb.command_wal.writer.command_buffer.trim_count"])
	}
	if v, ok := got["treedb.command_wal.writer.command_buffer.dropped_bytes_total"].(int64); !ok || v != 67108864 {
		t.Fatalf("command_buffer.dropped_bytes_total=%T(%v) want int64(67108864)", got["treedb.command_wal.writer.command_buffer.dropped_bytes_total"], got["treedb.command_wal.writer.command_buffer.dropped_bytes_total"])
	}
	if v, ok := got["treedb.command_wal.writer.pending_batch.capacity_bytes"].(int64); !ok || v != 32768 {
		t.Fatalf("pending_batch.capacity_bytes=%T(%v) want int64(32768)", got["treedb.command_wal.writer.pending_batch.capacity_bytes"], got["treedb.command_wal.writer.pending_batch.capacity_bytes"])
	}
	if v, ok := got["treedb.command_wal.public_batch.set_view.calls_total"].(int64); !ok || v != 11 {
		t.Fatalf("public_batch.set_view.calls_total=%T(%v) want int64(11)", got["treedb.command_wal.public_batch.set_view.calls_total"], got["treedb.command_wal.public_batch.set_view.calls_total"])
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
	if v, ok := got["treedb.process.memtable_residency.queue.append_only.entry_backing_bytes"].(int64); !ok || v != 7340032 {
		t.Fatalf("memtable_residency.queue.append_only.entry_backing_bytes=%T(%v) want int64(7340032)", got["treedb.process.memtable_residency.queue.append_only.entry_backing_bytes"], got["treedb.process.memtable_residency.queue.append_only.entry_backing_bytes"])
	}
	if v, ok := got["treedb.process.memtable_residency.queue.append_only.value_arena_retained_bytes"].(int64); !ok || v != 32768 {
		t.Fatalf("memtable_residency.queue.append_only.value_arena_retained_bytes=%T(%v) want int64(32768)", got["treedb.process.memtable_residency.queue.append_only.value_arena_retained_bytes"], got["treedb.process.memtable_residency.queue.append_only.value_arena_retained_bytes"])
	}
	if v, ok := got["treedb.process.append_only.mem_lease_value_arena_retained_bytes"].(int64); !ok || v != 65536 {
		t.Fatalf("append_only.mem_lease_value_arena_retained_bytes=%T(%v) want int64(65536)", got["treedb.process.append_only.mem_lease_value_arena_retained_bytes"], got["treedb.process.append_only.mem_lease_value_arena_retained_bytes"])
	}
	if v, ok := got["treedb.process.read_path.snapshot.backend_bytes_total"].(int64); !ok || v != 8192 {
		t.Fatalf("read_path.snapshot.backend_bytes_total=%T(%v) want int64(8192)", got["treedb.process.read_path.snapshot.backend_bytes_total"], got["treedb.process.read_path.snapshot.backend_bytes_total"])
	}
	if v, ok := got["treedb.process.batch.set.bytes_total"].(int64); !ok || v != 4096 {
		t.Fatalf("batch.set.bytes_total=%T(%v) want int64(4096)", got["treedb.process.batch.set.bytes_total"], got["treedb.process.batch.set.bytes_total"])
	}
	if v, ok := got["treedb.process.batch.delete_view.calls_total"].(int64); !ok || v != 5 {
		t.Fatalf("batch.delete_view.calls_total=%T(%v) want int64(5)", got["treedb.process.batch.delete_view.calls_total"], got["treedb.process.batch.delete_view.calls_total"])
	}
	if v, ok := got["treedb.process.batch.delete_view.bytes_total"].(int64); !ok || v != 256 {
		t.Fatalf("batch.delete_view.bytes_total=%T(%v) want int64(256)", got["treedb.process.batch.delete_view.bytes_total"], got["treedb.process.batch.delete_view.bytes_total"])
	}
	if v, ok := got["treedb.process.batch.set_caller.top.0.frame"].(string); !ok || !containsAll(v, "iavl.batchSetOwned", "rootmulti") {
		t.Fatalf("batch.set_caller.top.0.frame=%T(%v) want caller string", got["treedb.process.batch.set_caller.top.0.frame"], got["treedb.process.batch.set_caller.top.0.frame"])
	}
	if v, ok := got["treedb.process.flush_merge.applied_ops_total"].(int64); !ok || v != 12 {
		t.Fatalf("flush_merge.applied_ops_total=%T(%v) want int64(12)", got["treedb.process.flush_merge.applied_ops_total"], got["treedb.process.flush_merge.applied_ops_total"])
	}
	if v, ok := got["treedb.flush_admission.flush_apply_span_native"].(bool); !ok || !v {
		t.Fatalf("flush_admission.flush_apply_span_native=%T(%v) want bool(true)", got["treedb.flush_admission.flush_apply_span_native"], got["treedb.flush_admission.flush_apply_span_native"])
	}
	if v, ok := got["treedb.flush_apply.span_native.used_ops_total"].(int64); !ok || v != 123 {
		t.Fatalf("flush_apply.span_native.used_ops_total=%T(%v) want int64(123)", got["treedb.flush_apply.span_native.used_ops_total"], got["treedb.flush_apply.span_native.used_ops_total"])
	}
	if v, ok := got["treedb.flush_apply.span_native.fallback.reason.disabled.ops_total"].(int64); !ok || v != 4 {
		t.Fatalf("flush_apply fallback ops=%T(%v) want int64(4)", got["treedb.flush_apply.span_native.fallback.reason.disabled.ops_total"], got["treedb.flush_apply.span_native.fallback.reason.disabled.ops_total"])
	}
	if v, ok := got["treedb.raw.span_native.route.point_put.used_ops_total"].(int64); !ok || v != 99 {
		t.Fatalf("raw span-native point_put used_ops_total=%T(%v) want int64(99)", got["treedb.raw.span_native.route.point_put.used_ops_total"], got["treedb.raw.span_native.route.point_put.used_ops_total"])
	}
	if v, ok := got["treedb.raw.span_native.route.mixed_range_delete.fallbacks_total"].(int64); !ok || v != 2 {
		t.Fatalf("raw span-native mixed_range_delete fallbacks_total=%T(%v) want int64(2)", got["treedb.raw.span_native.route.mixed_range_delete.fallbacks_total"], got["treedb.raw.span_native.route.mixed_range_delete.fallbacks_total"])
	}
	if v, ok := got["treedb.publish.ordered_root_delta_group.span_native.used_ops_total"].(int64); !ok || v != 77 {
		t.Fatalf("ordered-root span-native used_ops_total=%T(%v) want int64(77)", got["treedb.publish.ordered_root_delta_group.span_native.used_ops_total"], got["treedb.publish.ordered_root_delta_group.span_native.used_ops_total"])
	}
	if v, ok := got["treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.status"].(string); !ok || v != "eligible" {
		t.Fatalf("ordered-root triage status=%T(%v) want eligible", got["treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.status"], got["treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.status"])
	}
	if v, ok := got["treedb.leaf_generation.pack.runs"].(int64); !ok || v != 5 {
		t.Fatalf("leaf_generation pack runs=%T(%v) want int64(5)", got["treedb.leaf_generation.pack.runs"], got["treedb.leaf_generation.pack.runs"])
	}
	if v, ok := got["treedb.cache.flush_apply.foreground_assist_wait_ns_total"].(int64); !ok || v != 12345 {
		t.Fatalf("cache flush_apply foreground assist wait=%T(%v) want int64(12345)", got["treedb.cache.flush_apply.foreground_assist_wait_ns_total"], got["treedb.cache.flush_apply.foreground_assist_wait_ns_total"])
	}
	if v, ok := got["treedb.cache.flush_span_run.ops_per_span"].(float64); !ok || v != 8.5 {
		t.Fatalf("cache flush_span_run ops_per_span=%T(%v) want float64(8.5)", got["treedb.cache.flush_span_run.ops_per_span"], got["treedb.cache.flush_span_run.ops_per_span"])
	}
	if v, ok := got["treedb.cache.flush_backlog_coalescing.skip.reason.not_enough_backlog_total"].(int64); !ok || v != 6 {
		t.Fatalf("cache flush_backlog_coalescing skip=%T(%v) want int64(6)", got["treedb.cache.flush_backlog_coalescing.skip.reason.not_enough_backlog_total"], got["treedb.cache.flush_backlog_coalescing.skip.reason.not_enough_backlog_total"])
	}
	if v, ok := got["treedb.cache.command_wal.checkpoint_publish.piggybacked"].(int64); !ok || v != 2 {
		t.Fatalf("command WAL checkpoint piggybacked=%T(%v) want int64(2)", got["treedb.cache.command_wal.checkpoint_publish.piggybacked"], got["treedb.cache.command_wal.checkpoint_publish.piggybacked"])
	}
	if v, ok := got["treedb.cache.command_wal.checkpoint_publish.separate"].(int64); !ok || v != 1 {
		t.Fatalf("command WAL checkpoint separate=%T(%v) want int64(1)", got["treedb.cache.command_wal.checkpoint_publish.separate"], got["treedb.cache.command_wal.checkpoint_publish.separate"])
	}
	if v, ok := got["treedb.cache.write.wait_for_checkpoint.count_total"].(int64); !ok || v != 4 {
		t.Fatalf("write wait-for-checkpoint count=%T(%v) want int64(4)", got["treedb.cache.write.wait_for_checkpoint.count_total"], got["treedb.cache.write.wait_for_checkpoint.count_total"])
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
				"treedb.process.identity.wal_dir":                              "/tmp/b/wal",
				"treedb.process.memory.heap_inuse_bytes":                       "222",
				"treedb.command_wal.writer.command_buffer.capacity_bytes":      "4194304",
				"treedb.command_wal.writer.command_buffer.dropped_bytes_total": "67108864",
				"treedb.vlog.mmap_active_bytes":                                "9",
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
	if instB["treedb.command_wal.writer.command_buffer.capacity_bytes"] != int64(4194304) {
		t.Fatalf("instance b command buffer capacity=%T(%v) want int64(4194304)", instB["treedb.command_wal.writer.command_buffer.capacity_bytes"], instB["treedb.command_wal.writer.command_buffer.capacity_bytes"])
	}
	if instB["treedb.command_wal.writer.command_buffer.dropped_bytes_total"] != int64(67108864) {
		t.Fatalf("instance b command buffer dropped bytes=%T(%v) want int64(67108864)", instB["treedb.command_wal.writer.command_buffer.dropped_bytes_total"], instB["treedb.command_wal.writer.command_buffer.dropped_bytes_total"])
	}
}

func TestCurrentTreeDBExpvarStatsIncludesStatsHook(t *testing.T) {
	resetTreeDBExpvarRegistryForTest(t)

	db := &DB{
		dir: "/tmp/hook/wal",
		backend: &mockBackendWithStats{
			MockBackend: NewMockBackend(),
			stats: map[string]string{
				"treedb.process.identity.wal_dir":        "/tmp/hook/wal",
				"treedb.process.memory.heap_inuse_bytes": "333",
			},
		},
	}
	db.SetStatsHook(func(stats map[string]string) {
		stats["treedb.command_wal.public_batch.set_view.calls_total"] = "13"
		stats["treedb.command_wal.public_batch.delete_view.calls_total"] = "5"
	})

	registerTreeDBExpvarStatsDB(db)

	got := currentTreeDBExpvarStats()
	if got["treedb.command_wal.public_batch.set_view.calls_total"] != int64(13) {
		t.Fatalf("current set_view calls=%T(%v) want int64(13)", got["treedb.command_wal.public_batch.set_view.calls_total"], got["treedb.command_wal.public_batch.set_view.calls_total"])
	}
	instances, ok := got["instances"].(map[string]any)
	if !ok {
		t.Fatalf("instances=%T want map[string]any", got["instances"])
	}
	inst, ok := findExpvarInstanceByWalDir(instances, "/tmp/hook/wal")
	if !ok {
		t.Fatalf("instance /tmp/hook/wal missing")
	}
	if inst["treedb.command_wal.public_batch.delete_view.calls_total"] != int64(5) {
		t.Fatalf("instance delete_view calls=%T(%v) want int64(5)", inst["treedb.command_wal.public_batch.delete_view.calls_total"], inst["treedb.command_wal.public_batch.delete_view.calls_total"])
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
