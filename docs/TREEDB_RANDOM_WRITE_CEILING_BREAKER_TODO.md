# TreeDB random-write ceiling breaker: span-run contract

Issue: #2768 (M8). Parent tracker: #2743.

This document is the post-M7 contract for the M9-M14 span-run / span-native
flush/apply stack. M8 is intentionally an architecture and observability PR:
it does **not** implement span-native apply, adaptive backlog coalescing, a
read-visible durable delta layer, or permanent keyspace sharding.

## Invariants

- TreeDB keeps one logical B-tree root. Span partitioning is ephemeral per flush
  run and must reduce deterministically into the same root publish path.
- Value-log and leaf-log output is persistent storage. Prepared output abandoned
  after a retry/root mismatch is durable-but-unreachable and must be accounted;
  it is not rollbackable WAL output and must not be reclaimed by truncating old
  durable pointer targets.
- Range-delete, lane, command-WAL, checkpoint, close, and memory/backpressure
  barriers are correctness boundaries. Later milestones may skip or fall back at
  these boundaries, but must not silently reorder across them.
- Success cannot be claimed from throughput alone. M9-M14 must prove movement in
  old-leaf decode bytes/op, leaf merges/op, replacement pages/op, append
  frames/op, ops/span, fallback reasons, reducer/publish timing, checkpoint wait
  splits, stalls, storage footprint, and correctness gates.

## Canonical flush-run contract (M9 input)

A canonical point-write flush run is built from one or more sealed immutable
memtables that are eligible to flush together:

1. Capture the base root identity before planning.
2. Select source memtables without crossing lane, range-delete, command-WAL,
   checkpoint, close, or memory/backpressure barriers.
3. Produce a globally sorted point-op run.
4. Apply same-key shadowing across all selected memtables before target-leaf
   span planning: the newest op for a key wins, older duplicate-key ops are
   counted as `shadowed_ops_total` and are not assigned to leaf jobs.
5. Preserve range deletes as explicit barriers/ranges. A range that overlaps
   multiple target leaves is represented in each touched target span for future
   workers, but it remains a barrier for run selection/coalescing.
6. Plan exact target leaf spans against the captured base root. Each span owns a
   half-open point-op slice and range slice, not durable output.

The Go contract types live in `TreeDB/db/span_run_contract.go`:

- `FlushSpanRunMetadata`
- `FlushSpanRunTargetLeafSpan`
- `FlushSpanRunBackendChunk`
- `FlushSpanRunSpanJobInput`
- `FlushSpanRunSpanJobOutput`
- `FlushSpanRunReducerInput`
- `FlushSpanRunPreparedOutputOwnership`
- `FlushSpanRunFallbackReason`

`ValidateFlushSpanRunMetadata` checks the M8 metadata invariants. The
`SummarizeFlushSpanRunChunkSplits` fixture proves the specific bug class where
entry-count backend chunks split one target leaf across multiple chunks.

## Span job and reducer contract (M10 input)

A future span-native job receives:

- captured base root ID;
- one exact `FlushSpanRunTargetLeafSpan`;
- canonical point-op and range slices for that span;
- no ownership of global root publish state.

A span job emits deterministic reducer input:

- replacement child refs in key order;
- split boundaries matching replacement refs;
- retired refs/pages for ownership transfer only after guarded publish;
- prepared leaf/value-log output ownership counters.

The reducer must sort/validate span outputs by `SpanIndex`, validate the captured
base root before publish, then publish or retry/fallback. On retry or failure,
prepared output is recorded as abandoned durable-but-unreachable output.

## Fallback matrix

Fallback reasons are stable stat strings and should be appended, not renamed:

| reason | meaning |
|---|---|
| `disabled` | span-native path is disabled by option/default |
| `below_threshold` | planned run is too small for configured gates |
| `span_native_not_implemented` | M8/M9 metadata exists but M10 engine is not present yet |
| `prepare_error` | read-only prepare failed |
| `validation_failed` | span metadata failed validation |
| `root_mismatch` | guarded publish saw a different base root |
| `range_delete_barrier` | range deletes require fallback or a future supported path |
| `lane_barrier` | lane ordering prevents coalescing/execution |
| `command_wal_barrier` | command-WAL publish ordering prevents coalescing/execution |
| `inexact_leaf_spans` | spans are hints rather than exact leaf ownership |
| `cold_build` | no old leaf exists for a cold build path |
| `maintenance` | maintenance/rebalance path prevents span-native apply |
| `backend_chunk_split` | entry-count backend chunks split target leaves |
| `close_or_checkpoint` | close/checkpoint drain requires serial safe fallback |
| `unknown` | fail-closed catch-all; should trend to zero |

M8 intentionally records current exact point-span opportunities as
`span_native_not_implemented`; M10 should move supported work from ineligible
fallback counters into eligible/executed counters.

## Required proof counters

TreeDB stats, unified-bench selected stats, and benchprof metadata preserve these
additive counters.

### Cache/run-level counters

- `treedb.cache.flush_span_run.runs_total`
- `treedb.cache.flush_span_run.source_point_ops_total` (pre-shadow point ops)
- `treedb.cache.flush_span_run.planned_ops_total` (post-shadow planned point ops plus range barriers)
- `treedb.cache.flush_span_run.planned_point_ops_total` (post-shadow point ops)
- `treedb.cache.flush_span_run.source_memtables_total`
- `treedb.cache.flush_span_run.source_memtables_max`
- `treedb.cache.flush_span_run.shadowed_ops_total`
- `treedb.cache.flush_span_run.range_barriers_total`
- `treedb.cache.flush_span_run.range_delete_ops_total`
- `treedb.cache.flush_span_run.backend_chunks_total`
- `treedb.cache.flush_span_run.target_leaf_spans_total`
- `treedb.cache.flush_span_run.single_op_spans_total`
- `treedb.cache.flush_span_run.span_ops_total`
- `treedb.cache.flush_span_run.span_bytes_total`
- `treedb.cache.flush_span_run.ops_per_span`
- `treedb.cache.flush_span_run.bytes_per_span`
- `treedb.cache.flush_span_run.single_op_span_ratio`
- `treedb.cache.flush_span_run.target_leaves_split_across_chunks_total`
- `treedb.cache.flush_span_run.max_chunks_per_target_leaf`
- `treedb.cache.flush_span_run.ops_per_run`

M8 included the deterministic `SummarizeFlushSpanRunChunkSplits` fixture for
entry-count chunks that split target leaves. M9 promotes this to runtime cache
flush evidence: canonical point runs are shadowed before target-leaf planning,
leaf-aware backend chunks keep normal target leaves intact, and any emergency
single-leaf split is reported by
`treedb.cache.flush_span_run.target_leaves_split_across_chunks_total` rather than
hidden as a false zero.

### Target span and span-native fallback counters

- `treedb.flush_apply.span_run.target_leaf_spans_total`
- `treedb.flush_apply.span_run.single_op_spans_total`
- `treedb.flush_apply.span_run.span_ops_total`
- `treedb.flush_apply.span_run.span_bytes_total`
- `treedb.flush_apply.span_run.ops_per_span`
- `treedb.flush_apply.span_run.bytes_per_span`
- `treedb.flush_apply.span_native.candidate_ops_total`
- `treedb.flush_apply.span_native.candidate_spans_total`
- `treedb.flush_apply.span_native.eligible_ops_total`
- `treedb.flush_apply.span_native.eligible_spans_total`
- `treedb.flush_apply.span_native.ineligible_ops_total`
- `treedb.flush_apply.span_native.ineligible_spans_total`
- `treedb.flush_apply.span_native.fallbacks_total`
- `treedb.flush_apply.span_native.fallback.reason.<reason>.ops_total`
- `treedb.flush_apply.span_native.fallback.reason.<reason>.spans_total`

### Rewrite-amplification proof counters

- `treedb.flush_apply.old_leaf_read_decode.bytes_total`
- `treedb.flush_apply.old_leaf_read_decode.bytes_per_op`
- `treedb.flush_apply.merge_build.leaf_merges_total`
- `treedb.flush_apply.merge_build.leaf_merges_per_op`
- `treedb.flush_apply.merge_build.replacement_leaf_pages_total`
- `treedb.flush_apply.merge_build.replacement_leaf_pages_per_op`
- `treedb.cache.flush_apply.leaf_log_append_frames_total`
- `treedb.cache.flush_apply.leaf_log_append_frames_per_op`

### Reducer/publish and checkpoint split counters

- `treedb.flush_apply.root_reduce.ns_total`
- `treedb.flush_apply.root_reduce.ns_per_op`
- `treedb.flush_apply.guarded_publish.ns_total`
- `treedb.flush_apply.guarded_publish.ns_per_op`
- `treedb.flush_apply.retry_total`
- `treedb.flush_apply.mismatch_total`
- `treedb.cache.checkpoint.active_background_flush_wait_ns_total`
- `treedb.cache.checkpoint.stage.value_log_flush.total_ns`
- `treedb.cache.checkpoint.stage.leaf_value_log_sync.total_ns`
- `treedb.cache.checkpoint.stage.reducer_publish.total_ns`
- existing `treedb.cache.checkpoint.flushmu_wait_*` and stage counters

## M8 acceptance notes

- The current apply path remains recursive/serial or M2 worker-pool COW apply;
  M8 only exposes metadata/fallback contracts and proof counters.
- A 10MM-key unified-bench close-phase gate is required by #2768 unless the
  coordinator explicitly waives it. Smaller local runs are smoke/support only.
