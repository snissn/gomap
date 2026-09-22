# VLOG Rewrite Min-Segment-Age Sweep (2026-03-28)

## Goal

Evaluate whether lowering online rewrite min-segment-age improves short-loop
signal without harming sync-time or end-of-run app-dir size.

## Workload

- Command core:
  - `./bin/unified-bench`
  - `-profile fast`
  - `-dbs treedb`
  - `-keys 900000`
  - `-valsize 256`
  - `-batchsize 4000`
  - `-test batch_write_steady,random_write`
  - `-val-pattern celestia_height_prefix_fill`
  - `-checkpoint-every-bytes 4194304`
  - `-treedb-force-value-pointers=true`
  - `-treedb-vlog-compression dict`
  - `-treedb-vlog-compression-autotune aggressive`
  - `-treedb-vlog-generation-policy hot_warm_cold`
  - `-treedb-vlog-rewrite-trigger-total-bytes 1`
  - `-treedb-vlog-rewrite-trigger-stale-ratio-ppm 1`
  - `-treedb-vlog-rewrite-trigger-churn-per-sec 1`
  - `-treedb-vlog-rewrite-budget-bytes-per-sec 134217728`
  - `-treedb-cache-stats-after-tests=true`

- Swept:
  - default (effective 30000ms)
  - `-treedb-vlog-rewrite-min-segment-age-ms 1000`
  - `-treedb-vlog-rewrite-min-segment-age-ms 5000`
  - `-treedb-vlog-rewrite-min-segment-age-ms 10000`

## Results

| min age | rewrite activity | dir bytes | wal bytes | note |
|---|---:|---:|---:|---|
| default (30000ms) | rewrite_runs=0, plan_empty.age_blocked=1 | 567,439,668 | 553,889,306 | baseline behavior |
| 1000ms | rewrite_runs=1, plan_selected=1, gc_runs=1 | 702,734,421 | 685,611,243 | clear regression |
| 5000ms | rewrite_runs=0, plan_empty.age_blocked=1 | 567,406,884 | 553,889,290 | effectively baseline |
| 10000ms | rewrite_runs=0, plan_empty.age_blocked=1 | 567,439,650 | 553,889,288 | effectively baseline |

Observed for the regressing 1000ms run:

- `rewrite.bytes_in` ~= 64MB
- `rewrite.bytes_out` ~= 528MB
- `rewrite.reclaim_ratio` = `0.000000`
- `gc.deleted_segments` = `0`

Interpretation: rewrite executes too early and amplifies bytes without reclaim,
so this setting is not suitable for production-like loops.

## Interleaved A/B confirmation

Using `scripts/celestia_fast_gate.sh` with same binaries and only this flag as
candidate delta (`CANDIDATE_EXTRA_FLAGS='-treedb-vlog-rewrite-min-segment-age-ms 1000'`):

- Output: `/tmp/gomap_minage_gate_ctr4Ji/gate`
- Decision: `clear_regression`
- Completed pairs: 2
- Median delta (`candidate - control`):
  - `s_sync_app_bytes`: +135,580,501.5
  - `t_sync_seconds`: +13

## Conclusion

- Keep default min-segment-age for normal runs.
- Keep the flag as an explicit lab-only override for controlled scheduler
  experiments.
- Do not enable low values (1ms/1000ms) in gate/default configs.
