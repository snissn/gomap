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

## Interleaved A/B Harness
For sync+rewrite tradeoff validation, use the interleaved harness:

```bash
cat >/tmp/cel_control.env <<'EOF'
LOCAL_GOMAP_DIR=/path/to/control/gomap
TREEDB_OPEN_PROFILE=fast
EOF

cat >/tmp/cel_candidate.env <<'EOF'
LOCAL_GOMAP_DIR=/path/to/candidate/gomap
TREEDB_OPEN_PROFILE=fast
EOF

CONTROL_ENV_FILE=/tmp/cel_control.env \
CANDIDATE_ENV_FILE=/tmp/cel_candidate.env \
MAX_PAIRS=10 \
MIN_PAIRS=4 \
CLEAR_WIN_PAIRS=3 \
CLEAR_LOSS_PAIRS=3 \
./scripts/run_celestia_ab.sh
```

Default pair metric focus:
- `T_sync`: sync duration (seconds)
- `S_sync_app`: app dir bytes at sync end
- `S_sync_wal`: `application.db/maindb/wal` bytes at sync end
- `T_rw`: offline `vlog-rewrite` wall time
- `S_post_wal`: WAL bytes after offline rewrite
- `T_total = T_sync + T_rw`
- `max_rss_kb` (memory guardrail)

Outputs:
- `artifacts/celestia_ab/<ts>/runs.csv`
- `artifacts/celestia_ab/<ts>/pairs.csv`
- `artifacts/celestia_ab/<ts>/summary.md`
- per-run JSON under `artifacts/celestia_ab/<ts>/runs/*/run.json`

The harness alternates run order per pair (`control->candidate`, then
`candidate->control`) and can stop early on clear win/loss signals.

## Experimental Knob
- `TREEDB_ENABLE_VLOG_GENERATION_PRECHECKPOINT_REWRITE=1`
  - WAL-off only.
  - Allows rewrite planning/execution before the first explicit checkpoint.
  - Default is disabled to avoid adding early restore contention.
  - Use for controlled `run_celestia` experiments when `maintenance.skip.before_first_checkpoint` dominates and live rewrite never starts.
- `TREEDB_ENABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1`
  - WAL-off only.
  - During checkpoint-kick maintenance, skips starting a fresh rewrite plan while foreground activity is hot and rewrite queue debt is empty.
  - Still allows queued rewrite debt (and deferred-due passes) to run.
  - Default is disabled.

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
