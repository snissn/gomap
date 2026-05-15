# unified_bench

- keys: 2,000
- key-shape: be8
- valsize: 64
- batchsize: 1000
- range-queries: 200
- range-span: 100
- val-pattern: zero
- val-pool-size: 0
- profile: durable
- dbs: treedb_backend,treedb_backend_command_wal
- tests: batch_write
- seed: 1

```text
       Test  TreeDB (backend)  TreeDB (backend command_wal_v1)
-----------  ----------------  -------------------------------
Batch Write           120,003                           88,714
```

## Checkpoint Time (Between Tests + Post-run)

```text
Before Test  TreeDB (backend)  TreeDB (backend command_wal_v1)
-----------  ----------------  -------------------------------
Batch Write           21.84ms                          19.60ms
  After Run           38.44ms                          39.72ms
```

## Disk Usage (End of Run)

```text
TreeDB (backend command_wal_v1): total=444 KiB files=8

TreeDB (backend): total=286 KiB files=6
```

## TreeDB Selected Stats (End of Run)

```text
TreeDB (backend):
  vlog_mmap.read.hits: 0
  vlog_mmap.read.miss_out_of_range: 0
  vlog_mmap.read.miss_no_mapping: 0
  vlog_mmap.read.miss_dead_mapping_cap: 0
  vlog_mmap.read.fallback_readat: 0
  applied_command_lsn: 0
  command_wal.enabled: false
  command_wal.required_feature: false
  command_wal.frames: 0
  command_wal.typed_segments: 0
  command_wal.max_lsn: 0
  leaf_generation.generations.pinned: 0
  leaf_generation.pins.total: 0
  publish.ordered_root_delta_group.calls_total: 0
  publish.ordered_root_delta_group.roots_total: 0
  publish.ordered_root_delta_group.avg_roots_per_call: 0.000
  publish.ordered_root_delta_group.root_apply_calls_total: 0
  publish.ordered_root_delta_group.root_apply_ns_total: 0
  publish.ordered_root_delta_group.root_apply_ops_total: 0
  publish.ordered_root_delta_group.root_apply_node_loads_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total: 0
  publish.ordered_root_delta_group.write_lock_wait_ns_total: 0
  publish.ordered_root_delta_group.write_lock_hold_ns_total: 0
TreeDB (backend command_wal_v1):
  vlog_mmap.read.hits: 0
  vlog_mmap.read.miss_out_of_range: 0
  vlog_mmap.read.miss_no_mapping: 0
  vlog_mmap.read.miss_dead_mapping_cap: 0
  vlog_mmap.read.fallback_readat: 0
  applied_command_lsn: 2
  command_wal.enabled: true
  command_wal.required_feature: true
  command_wal.frames: 2
  command_wal.typed_segments: 1
  command_wal.max_lsn: 2
  leaf_generation.generations.pinned: 0
  leaf_generation.pins.total: 0
  publish.ordered_root_delta_group.calls_total: 0
  publish.ordered_root_delta_group.roots_total: 0
  publish.ordered_root_delta_group.avg_roots_per_call: 0.000
  publish.ordered_root_delta_group.root_apply_calls_total: 0
  publish.ordered_root_delta_group.root_apply_ns_total: 0
  publish.ordered_root_delta_group.root_apply_ops_total: 0
  publish.ordered_root_delta_group.root_apply_node_loads_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total: 0
  publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total: 0
  publish.ordered_root_delta_group.write_lock_wait_ns_total: 0
  publish.ordered_root_delta_group.write_lock_hold_ns_total: 0
```
