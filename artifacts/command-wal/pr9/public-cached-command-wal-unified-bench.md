# unified_bench

- keys: 200,000
- key-shape: be8
- valsize: 32
- batchsize: 1024
- range-queries: 200
- range-span: 100
- val-pattern: zero
- val-pool-size: 0
- profile: wal_on_fast
- dbs: treedb,treedb_public_command_wal
- tests: batch_write
- seed: 1

## Resolved TreeDB Options

```text
durability=wal_on_relaxed
read_integrity=skip_checksums
maintenance_mode=bench
index_optimizations=false
leaf_prefix_compression=true
index_columnar_leaves=true
index_packed_valueptr=true
index_internal_base_delta=false
index_outer_leaves_in_vlog=true
outer_leaf_read_cache_entries=default/env (effective=32768)
cached.domain_ingress_workers=0
cached.domain_ingress_queue_size=0
vlog.force_pointers=false
vlog.pointer_threshold=default (effective=512B)
vlog.compression=auto
vlog.block_codec=snappy
vlog.auto_policy=balanced
vlog.compression_autotune=medium
vlog.dict_class_mode=single
vlog.generation_policy=off
vlog.generation_hot_segment_bytes=0
vlog.generation_warm_segment_bytes=0
vlog.generation_cold_segment_bytes=0
vlog.rewrite_budget_bytes_per_sec=0
vlog.rewrite_budget_records_per_sec=0
vlog.rewrite_trigger_stale_ratio_ppm=0
vlog.rewrite_trigger_total_bytes=0
vlog.rewrite_trigger_churn_per_sec=0
vlog.rewrite_min_segment_age_ms=default (effective=30000)
vlog.block_target_bytes=default (effective=4096B)
vlog.incompressible_hold_bytes=default (effective=67108864B)
vlog.incompressible_probe_bytes=default (effective=8388608B)
notes:
  - index_internal_base_delta disabled: leaf-log child pages use explicit LogRecordRef entries
  - index_optimizations enables leaf prefix compression + columnar leaves + packed value pointers + internal base-delta
  - index_packed_valueptr uses a packed 12B leaf ValuePtr encoding (u32 offset cap; cached mode rotates value-log segments automatically)
  - leaf_prefix_compression + index_columnar_leaves: enabling combined columnar+prefix leaf encoding for new pages
  - leaf_prefix_compression uses front-coding with restart points (compact v2 leaf entry header for new pages)
  - maintenance_mode=bench defaults vlog.generation_policy=off
```

```text
       Test         TreeDB  TreeDB (public cached command_wal_v1)
-----------  -------------  -------------------------------------
Batch Write      5,851,645                              7,133,165
```

## Disk Usage (End of Run)

```text
TreeDB:
  maindb/index.db: 512 KiB
  maindb/wal: total=12 B files=1 other=12 B
  maindb/value_vlog: total=0 B files=3
  maindb/leaf_vlog: total=5 MiB files=2 value=5 MiB other=244 B
  dictdb/index.db: 64 KiB

TreeDB (public cached command_wal_v1):
  maindb/index.db: 512 KiB
  maindb/wal: total=12 B files=1 other=12 B
  maindb/leaf_vlog: total=5 MiB files=2 value=5 MiB other=244 B
  dictdb/index.db: 64 KiB
```

## TreeDB Selected Stats (End of Run)

```text
TreeDB:
  write_path.mode: cached
  write_path.redo_log: on
  vlog_mmap.read.hits: 0
  vlog_mmap.read.miss_out_of_range: 0
  vlog_mmap.read.miss_no_mapping: 0
  vlog_mmap.read.miss_dead_mapping_cap: 0
  vlog_mmap.read.fallback_readat: 0
  vlog_mmap.read.hit_ratio: 1.000000
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
TreeDB (public cached command_wal_v1):
  write_path.mode: command_wal_cached
  write_path.redo_log: command_wal
  vlog_mmap.read.hits: 0
  vlog_mmap.read.miss_out_of_range: 0
  vlog_mmap.read.miss_no_mapping: 0
  vlog_mmap.read.miss_dead_mapping_cap: 0
  vlog_mmap.read.fallback_readat: 0
  vlog_mmap.read.hit_ratio: 1.000000
  applied_command_lsn: 196
  command_wal.enabled: true
  command_wal.required_feature: true
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
```
