# TreeDB Generational Value-Log Runbook

## Goals
- Keep steady-state `maindb/wal` bounded under churn.
- Avoid full-stop rewrite passes; use incremental rewrite + frequent GC.
- Keep index maintenance policy-linked but independent from value-log GC.

## Recommended Defaults
- `-treedb-maintenance-mode normal` (default)
- `-treedb-vlog-generation-policy hot_warm_cold`
- `-treedb-vlog-rewrite-trigger-total-bytes` set for your dataset size
- `-treedb-vlog-rewrite-budget-bytes-per-sec` and/or `-treedb-vlog-rewrite-budget-records-per-sec`

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

## Live Run Capacity Report
For `run_celestia`-style runs, analyze the latest diagnostics snapshot with:

```bash
./scripts/analyze_vlog_maintenance_capacity.py
```

Optional explicit input:

```bash
./scripts/analyze_vlog_maintenance_capacity.py ~/.celestia-app-mainnet-treedb-<timestamp>
./scripts/analyze_vlog_maintenance_capacity.py ~/.celestia-app-mainnet-treedb-<timestamp>/sync/diagnostics/<file>.debug_vars.json
```

The report highlights:
- maintenance lane pressure (attempt/acquire/collision + skip mix)
- rewrite plan-to-exec realization
- rewrite source outcomes (requested vs still-referenced vs unreferenced)
- stale-bytes processed vs immediate reclaim
- observed-source replay drain
- observed-source retained-prune outcomes (candidate/live-skipped/zombie-marked/removed)
- zombie inventory (pinned vs unpinned bytes)
- GC eligibility/protection signals

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
  -treedb-vlog-generation-policy hot_warm_cold \
  -treedb-vlog-rewrite-trigger-total-bytes 67108864 \
  -treedb-vlog-rewrite-budget-bytes-per-sec 8388608 \
  -treedb-cache-stats-after-tests
```

### Size-oriented variant (larger outer-leaf blocks)
```bash
./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -progress=false \
  -format markdown \
  -checkpoint-between-tests \
  -treedb-vlog-generation-policy hot_warm_cold \
  -treedb-outer-leaf-block-target-bytes 16384
```

## Guardrail Expectations
- No correctness regressions vs supported configurations.
- No unbounded maintenance loops.
- No repeated vacuum executions without cooldown.
- Throughput regressions should be investigated if read p95 spikes after larger block targets.
