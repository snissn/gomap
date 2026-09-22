# TreeDB collection/document storage wiring M1 report (#2951)

Status: classification-only investigation. No production storage behavior changes are made here.

## Executive summary

`cmd/unified_bench -suite collection_storage ... document_only insert_batch` is a **direct backend** benchmark today. It opens `TreeDB/db` with `backenddb.Open` and does not use the normal unified-bench TreeDB option-resolution path or the cached-layer leaf-log wiring. The suite-level "TreeDB options (resolved)" block can therefore say `index_outer_leaves_in_vlog=true` and `flush_admission_admitted=true`, while the actual collection-storage database has `format.json:index_outer_leaves_in_vlog=false`, no `treedb.cache.leaf_log_lanes.*` stats, and zero flush-apply leaf-log output work.

The in-package collection insert-shape benchmarks are different. `TreeDB/collections` benchmarks use `treedb.OptionsFor(...)` and, when the collection storage policy enables outer leaves, use `treedb.OpenBackendWithCachedLeafLog(...)`. A short template-v1 indexed insert-shape run exercised that path, but the benchmark report itself does not preserve cached-layer lane counters. An equivalent diagnostic open with the same production-mainline storage policy showed only one configured/used leaf-log append lane, so it is not a multi-lane proof row.

Recommendation for #2943 tracking: do **not** treat `collection_storage document_only insert_batch` as evidence for cached outer-leaf lane behavior. Keep it labelled as backend-direct/native collection storage, or split future rows into explicit `backend_direct` and `cached_leaf_log` labels before using the suite as a leaf-log-lane regression gate.

## Code-path classification

| Surface | Open path today | Uses normal unified-bench TreeDB options? | Cached leaf-log installed? | Lane-counter proof? | Classification |
|---|---|---:|---:|---:|---|
| `cmd/unified_bench -suite collection_storage -collection-storage-modes document_only -collection-storage-workloads insert_batch` | `cmd/unified_bench/suite_collection_storage.go` writes a minimal format config, then calls `openCollectionStorageDB`, which directly calls `backenddb.Open(backenddb.Options{CommandWAL:true, Durability:Durable, ...})`. | No | No | No; cache lane counters are absent and flush-apply counters are zero. | Backend-direct row; label/split before using as cached-lane evidence. |
| `TreeDB/collections` package shape benchmarks, e.g. `BenchmarkCollectionShapeInsertBatch/indexes_1` with `TREEDB_COLLECTION_DOCUMENT_FORMAT=template-v1` | `openBenchmarkBackend` calls `treedb.OptionsFor(benchmarkTreeDBProfile, dir)` and chooses `treedb.OpenBackendWithCachedLeafLog` when data/index outer leaves are enabled. | Yes, for package benchmarks | Yes when `data_outer || index_outer` | Not in the normal report. A diagnostic equivalent showed one configured/used lane, not multi-lane. | Normal TreeDB option-resolution row, but not a multi-lane proof unless stats are exported and show `append_lanes_used > 1`. |

Relevant source anchors:

- `cmd/unified_bench/suite_collection_storage.go`: `prepareCollectionStorageMode` saves only `RequiredFeatureCommandWALV1`, then calls `openCollectionStorageDB`.
- `cmd/unified_bench/suite_collection_storage.go`: `openCollectionStorageDB` calls `backenddb.Open(...)` directly.
- `TreeDB/collections/bench_test.go`: `benchmarkCollectionStoragePolicy` defaults `data_outer=true,index_outer=false`.
- `TreeDB/collections/bench_test.go`: `openBenchmarkBackend` calls `treedb.OptionsFor(...)` and selects `treedb.OpenBackendWithCachedLeafLog` when `IndexOuterLeavesInValueLog` is true.
- `TreeDB/open_backend.go`: `OpenBackendWithCachedLeafLog` opens the public/cached DB and returns the underlying backend.
- `TreeDB/caching/flush_apply_stats.go`: cached leaf-log lane counters are emitted as `treedb.cache.leaf_log_lanes.*` only when the cached DB owns outer leaves.

## Evidence rows

### 1. `collection_storage document_only insert_batch`

Command shape:

```sh
OUT=/tmp/gomap_2951_collection_storage_keep_20260623_120621
flock /tmp/gomap_diag_bench.lock -c \
  "GOWORK=off go run ./cmd/unified_bench \
    -suite collection_storage \
    -profile durable \
    -dbs treedb \
    -keys 64 \
    -batchsize 32 \
    -collection-storage-modes document_only \
    -collection-storage-workloads insert_batch \
    -profile-dir '$OUT' \
    -path-label native-fastpath \
    -progress=false \
    -keep"
```

Artifacts:

- `/tmp/gomap_2951_collection_storage_keep_20260623_120621/benchprof_results.md`
- `/tmp/gomap_2951_collection_storage_keep_20260623_120621/collection_storage_results.json`
- kept DB dir: `/mnt/fast4tb/tmp/unified-bench-collection-storage-1498007480`

Observed storage format:

```json
{
  "required_features": ["command_wal_v1"],
  "index_outer_leaves_in_vlog": false,
  "vlog_compression": "auto",
  "vlog_block_codec": "snappy",
  "vlog_auto_policy": "balanced"
}
```

Selected stats from `benchprof_results.md`:

| Counter / proof label | Observed value | Interpretation |
|---|---:|---|
| `flush_admission.admitted` | `true` | Present because backend stats normalize/report the flush-admission decision from options; this is not cached-lane evidence. |
| `flush_admission.flush_apply_span_native` | `true` | Configuration/reporting only for this row; no flush-apply work occurred. |
| `treedb.cache.leaf_log_lanes.configured` | missing | Cached leaf-log lane stats are not installed/exported by this backend-direct path. |
| `treedb.cache.leaf_log_lanes.append_lanes_used` | missing | No lane proof. |
| `flush_apply.leaf_log_output.append_pages_total` | `0` | Flush apply did not append leaf-log pages. |
| `flush_apply.leaf_log_output.lane.tasks_lanes_used` | `0` | No flush-apply lane fan-out. |
| `flush_apply.apply_ns_total` | `0` | No backend flush-apply path ran. |
| `command_wal.enabled` | `true` | The row is command-WAL durable backend-direct. |

This row should therefore be reported as **backend-direct / no cached leaf-log-lane proof**, despite the top-level unified-bench option block printing `index_outer_leaves_in_vlog=true` for the normal TreeDB adapter configuration.

### 2. Indexed/template-v1 collection insert shape

Command shape:

```sh
OUT=/tmp/gomap_2951_collection_shape_20260623_120434
flock /tmp/gomap_diag_bench.lock -c \
  "OUT_DIR='$OUT' \
   COUNT=1 \
   BENCHTIME=64x \
   BENCH_REGEX='^BenchmarkCollectionShapeInsertBatch$/^indexes_1$' \
   TREEDB_COLLECTION_PATH_LABEL=native-fastpath \
   TREEDB_COLLECTION_DOCUMENT_FORMAT=template-v1 \
   TREEDB_COLLECTION_BENCH_ENGINE=bench_unsafe \
   TREEDB_COLLECTION_BENCH_BATCH_SIZE=64 \
   TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=true \
   TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=false \
   scripts/bench_collections_report.sh"
```

Artifact:

- `/tmp/gomap_2951_collection_shape_20260623_120434/collections_report.md`

Observed row:

```text
BenchmarkCollectionShapeInsertBatch/indexes_1-12  64  6552 ns/op ...
  document format: template-v1
  storage policy: data_outer=true,index_outer=false
  execution path: native-fastpath
  indexes/doc=1
  buffered_indexed_writes=1
  stored_docs=64
```

Classification:

- Static path uses `treedb.OptionsFor(...)` and selects `OpenBackendWithCachedLeafLog` for the default storage policy (`data_outer=true,index_outer=false`).
- The published collection report does not include `treedb.cache.leaf_log_lanes.*`; it reports benchmark metrics and backend-native fallback counters instead.
- A small equivalent diagnostic open of the same policy after a 64-doc template-v1/indexes_1 insert reported:

```text
treedb.cache.leaf_log_lanes.configured=1
treedb.cache.leaf_log_lanes.active=1
treedb.cache.leaf_log_lanes.append_lanes_used=1
treedb.cache.leaf_log_lanes.append_pages_total=5
treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used=0
```

That confirms cached leaf-log wiring can be present for package shape benches, but this shape is still **not** a multi-lane proof row.

## Recommendation for follow-up

For #2943, track these as separate benchmark labels instead of silently rewiring the production suite in this report PR:

1. Keep current `collection_storage` semantics as `backend_direct_command_wal` unless/until a deliberate behavior change is accepted.
2. Add an explicit cached/public-TreeDB collection-storage row if the suite must prove cached outer-leaf/lane behavior.
3. Preserve/report the exact proof counters for any cached row: `treedb.cache.leaf_log_lanes.configured`, `treedb.cache.leaf_log_lanes.append_lanes_used`, per-lane `treedb.cache.leaf_log_lanes.lane.NN.append_pages_total`, and `treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used`.
4. Treat `flush_admission.admitted=true` only as an admission/configuration signal. It is insufficient without non-zero lane or flush-apply counters.
