# TreeDB Generational Value-Log Runbook

## Goals
- Keep steady-state `maindb/wal` bounded under churn.
- Avoid full-stop rewrite passes; use incremental rewrite + frequent GC.
- Keep index maintenance policy-linked but independent from value-log GC.

## Recommended Defaults
- `-treedb-vlog-generation-policy hot_warm_cold`
- `-treedb-vlog-rewrite-trigger-total-bytes` set for your dataset size
- `-treedb-vlog-rewrite-budget-bytes-per-sec` and/or `-treedb-vlog-rewrite-budget-records-per-sec`
- `-treedb-index-outer-leaf-mode v1_leaflog_route`
- `-treedb-v1-leaflog-route-payload-profile default` (8KiB payload blocks)

Size-oriented profile:
- `-treedb-v1-leaflog-route-payload-profile size16k`

## Maintenance Model
- Rewrite: threshold-triggered and budget-bounded.
- GC: cheap/frequent passes; heuristic-triggered when reclaimable bytes or churn pressure is high.
- Vacuum: only after major rewrite windows with cooldown; never on every GC pass.

## Observability Keys
Use `-treedb-cache-stats-after-tests` in unified-bench.

Primary keys:
- `treedb.cache.vlog_generation.scheduler_state`
- `treedb.cache.vlog_generation.scheduler_last_reason`
- `treedb.cache.vlog_generation.churn_bytes_per_sec`
- `treedb.cache.vlog_generation.rewrite.runs`
- `treedb.cache.vlog_generation.rewrite.bytes_in`
- `treedb.cache.vlog_generation.rewrite.bytes_out`
- `treedb.cache.vlog_generation.gc.runs`
- `treedb.cache.vlog_generation.gc.deleted_segments`
- `treedb.cache.vlog_generation.gc.deleted_bytes`
- `treedb.cache.vlog_generation.vacuum.runs`
- `treedb.cache.vlog_generation.vacuum.failures`

## Bench Commands
### Churn sanity (TreeDB)
```bash
GOWORK=off make unified-bench
./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -progress=false \
  -format markdown \
  -checkpoint-between-tests \
  -treedb-index-outer-leaf-mode v1_leaflog_route \
  -treedb-vlog-generation-policy hot_warm_cold \
  -treedb-vlog-rewrite-trigger-total-bytes 67108864 \
  -treedb-vlog-rewrite-budget-bytes-per-sec 8388608 \
  -treedb-v1-leaflog-route-payload-profile default \
  -treedb-cache-stats-after-tests
```

### Size-oriented variant (16KiB payload blocks)
```bash
./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -progress=false \
  -format markdown \
  -checkpoint-between-tests \
  -treedb-index-outer-leaf-mode v1_leaflog_route \
  -treedb-vlog-generation-policy hot_warm_cold \
  -treedb-v1-leaflog-route-payload-profile size16k
```

## Guardrail Expectations
- No correctness regressions vs existing modes.
- No unbounded maintenance loops.
- No repeated vacuum executions without cooldown.
- Throughput regressions should be investigated if read p95 spikes after 16KiB payload profile.
