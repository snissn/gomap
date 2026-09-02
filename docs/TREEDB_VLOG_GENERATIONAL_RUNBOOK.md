# TreeDB Generational Value-Log Runbook

## Goals
- Keep steady-state `maindb/value_vlog`, `maindb/leaf_vlog`, and `maindb/wal` bounded under churn.
- Avoid full-stop rewrite passes; use incremental rewrite + frequent GC.
- Keep index maintenance policy-linked but independent from value-log GC.

For manual final-footprint maintenance, prefer:

```sh
treemap compact <db-dir> -rw
```

Use `treemap compact-plan <db-dir>` for a read-only preview. The individual
`vlog-*` and `leafgen-*` commands below are advanced diagnostics and scheduler
building blocks, not the recommended path for benchmark storage accounting.
When online index vacuum is unsupported on a platform, `treemap compact` reports
that phase as skipped and still performs the value-log, leaf-log, and cleanup
phases.

## Recommended Defaults
- `-treedb-maintenance-mode normal` (default)
- `-treedb-vlog-generation-policy hot_warm_cold`
- `-treedb-vlog-rewrite-trigger-total-bytes` set for your dataset size
- `-treedb-vlog-rewrite-budget-bytes-per-sec` and/or `-treedb-vlog-rewrite-budget-records-per-sec`
- `-treedb-vlog-rewrite-min-segment-age-ms` keep default for production; lower only for short-loop experiments

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
- `treedb.cache.vlog_generation.rewrite.min_segment_age_ms`
- `treedb.cache.vlog_generation.rewrite.plan_runs`
- `treedb.cache.vlog_generation.rewrite.plan_empty`
- `treedb.cache.vlog_generation.rewrite.plan_empty.age_blocked`
- `treedb.cache.vlog_generation.rewrite.plan_selected`
- `treedb.cache.vlog_generation.rewrite.ledger_bytes_stale`
- `treedb.cache.vlog_generation.rewrite.runs.source.*`
- `treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.*`
- `treedb.cache.vlog_generation.rewrite.exec.source_bytes_{requested,unreferenced}_total.source.*`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.checkpoint_kick`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.checkpoint_kick`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.checkpoint_kick`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.fresh_plan`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.fresh_plan`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.fresh_plan`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions.fresh_plan`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.*`
- `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_burst`
- `treedb.cache.vlog_generation.rewrite.queue_config.resume_max_segments`
- `treedb.cache.vlog_generation.rewrite.queue_config.debt_drain_max_segments`
- `treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_min_segments`
- `treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_max_segments`
- `treedb.cache.vlog_generation.rewrite.runs`
- `treedb.cache.vlog_generation.rewrite.bytes_in`
- `treedb.cache.vlog_generation.rewrite.bytes_out`
- `treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total`
- `treedb.cache.vlog_generation.rewrite.exec.source_bytes_still_referenced_total`
- `treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total`
- `treedb.cache.vlog_generation.gc.runs`
- `treedb.cache.vlog_generation.gc.deleted_segments`
- `treedb.cache.vlog_generation.gc.deleted_bytes`
- `treedb.cache.vlog_generation.vacuum.runs`
- `treedb.cache.vlog_generation.vacuum.failures`

Note: `rewrite.queue_run_segment_cap.per_segment_budget_bytes*` uses queue live-byte
hints when available (average live bytes per queued segment), and falls back to
the warm segment target when live-byte hints are unavailable.

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
# Legacy compatibility benchmark ceiling, not a public TreeDB server profile.
TREEDB_OPEN_PROFILE=fast
EOF

cat >/tmp/cel_candidate.env <<'EOF'
LOCAL_GOMAP_DIR=/path/to/candidate/gomap
# Legacy compatibility benchmark ceiling, not a public TreeDB server profile.
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

Recommended for probe loops (faster fail on low-signal state-sync stalls):
- Set `FREEZE_REMOTE_HEIGHT_AT_START=1` in both env files so pair targets are stable.
- Set `ZERO_LOCAL_FAIL_SECONDS=<n>` (for example `120` to `300`) to abort runs that
  stay at `local=0` too long even if restore I/O is active.
- Keep `NO_PROGRESS_FAIL_SECONDS`/`NO_PROGRESS_HARD_FAIL_SECONDS` as a secondary
  backstop for non-zero-local stalls.

Optional engagement gating (use when optimizing queued rewrite-debt behavior):
- `AB_REQUIRE_MAINTENANCE_WITH_REWRITE=1` rejects attempts that never execute the
  maintenance rewrite lane (common in bootstrap/restore/catch-up dominated runs).
- `AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC=1` rejects attempts where the queued
  rewrite-debt executor never runs (more specific than `*_WITH_REWRITE`).
- When enabled, each variant will retry up to `RUN_MAX_ATTEMPTS_PER_VARIANT` and
  log attempt outcomes to `artifacts/celestia_ab/<ts>/runs/<pair>_<variant>/attempts.log`.

Default pair metric focus:
- `T_sync`: sync duration (seconds)
- `S_sync_app`: app dir bytes at sync end
- `S_sync_wal`: `application.db/maindb/wal` bytes at sync end
- `T_rw`: offline `vlog-rewrite` wall time
- `S_post_wal`: WAL bytes after offline rewrite
- `T_total = T_sync + T_rw`
- `max_rss_kb` (memory guardrail)
- `blocks_synced` and normalized metrics (`*_per_block`) to de-noise moving-target runs
- rewrite efficiency/capacity (`rewrite_exec_reclaimed_vs_churn_ratio`, `rewrite_reclaimed_share_of_budget_pct`, `rewrite_budget_consumed_share_of_budget_pct`, `rewrite_ineffective_runs`, `observed_gc_pending_ids`)
- rewrite queue + checkpoint-kick pressure (`rewrite_queue_len`, `rewrite_queue_live_bytes_after_tokens`, `rewrite_queue_eta_seconds.budget`, `checkpoint_kick_skipped_hot_no_debt`)

Outputs:
- `artifacts/celestia_ab/<ts>/runs.csv`
- `artifacts/celestia_ab/<ts>/pairs.csv`
- `artifacts/celestia_ab/<ts>/summary.md`
- `artifacts/celestia_ab/<ts>/decision.json`
- per-run JSON under `artifacts/celestia_ab/<ts>/runs/*/run.json`

Light stats snapshots in each run directory are captured via `treemap stats -rw`
with automatic fallback to `treemap vlog-gc -rw -dry-run`.

`runs.csv` includes per-run rewrite-capacity KPIs (including `rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst`) and `pairs.csv` includes pair deltas for those KPIs (including `delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst`), so candidate/control maintenance behavior can be evaluated directly from CSV outputs.
This includes queue-pressure and checkpoint-kick fields to diagnose whether rewrite is keeping up incrementally during hot sync windows.
It also includes checkpoint-like (`bypass` + `checkpoint_pending`) vs non-checkpoint rewrite split metrics:
`rewrite_checkpoint_like_budget_share_pct`, `rewrite_checkpoint_like_source_unreferenced_bytes_pct`, and
`rewrite_non_checkpoint_source_unreferenced_bytes_pct`.

For segment-size candidate sweeps that preserve the same A/B stop policy, use:

```bash
CONTROL_ENV_FILE=/tmp/cel_control.env \
CANDIDATE_BASE_ENV_FILE=/tmp/cel_candidate.env \
SEGMENT_SCOPE=hot_warm_cold \
SEGMENT_BYTES_LIST="4194304 8388608 16777216" \
./scripts/celestia_segment_sweep.sh
```

The harness alternates run order per pair (`control->candidate`, then
`candidate->control`) and can stop early on clear win/loss signals.
For stable pair scoring, prefer `FREEZE_REMOTE_HEIGHT_AT_START=1` and validate
`delta_blocks_synced` stays near zero across pairs.

Scoring policy defaults:
- `AB_POLICY=low_noise` (default)
  - `SCORING_MODE=per_block`
  - `BLOCK_DRIFT_TOLERANCE=50`
  - `ALLOW_DRIFT_SCORING=0`
- `AB_POLICY=legacy` to restore historical behavior (`absolute` scoring, drift gate disabled by default).

Optional composite stop policy:
- `COMPOSITE_STOP_ON_CLEAR=1` enables weighted time+size stop logic (disabled by default).
- `COMPOSITE_WEIGHT_TIME` / `COMPOSITE_WEIGHT_SIZE` set relative weight (defaults `0.5` / `0.5`).
- `COMPOSITE_MIN_PAIRS` controls minimum comparable pairs before composite stop (default `4`).
- `COMPOSITE_CLEAR_WIN_PCT` / `COMPOSITE_CLEAR_LOSS_PCT` set median composite-score thresholds in percent (defaults `1.0` / `1.0`; lower is better).

Additional reporting:
- `pairs.csv` includes absolute per-pair control/candidate values and `composite_score_pct`.
- `summary.md` includes an `Absolute Medians` section for direct control vs candidate totals.
- `decision.json` includes `absolute_aggregates` and `composite` config/summary fields.

## Experimental Knob
- `TREEDB_ENABLE_VLOG_GENERATION_PRECHECKPOINT_REWRITE=1`
  - Legacy WAL-off compatibility profile only.
  - Allows rewrite planning/execution before the first explicit checkpoint.
  - Default is disabled to avoid adding early restore contention.
  - Use for controlled `run_celestia` experiments when `maintenance.skip.before_first_checkpoint` dominates and live rewrite never starts.
- `TREEDB_DISABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1`
  - Legacy WAL-off compatibility profile only.
  - By default, checkpoint-kick maintenance skips starting a fresh rewrite plan while foreground activity is hot and rewrite queue debt is empty.
  - Queued rewrite debt and deferred-due passes still run.
  - Set this disable knob only for controlled rollback experiments that need the legacy checkpoint-kick fresh-plan behavior.
- Periodic rewrite-plan scan budget:
  - By default, periodic/background fresh rewrite planning is bounded by a short internal deadline so scheduled live-byte scans cannot monopolize foreground-heavy restore/sync windows.
  - Explicit/checkpoint/deferred rewrite debt paths keep their wider planning contexts.
  - `TREEDB_DISABLE_VLOG_GENERATION_PERIODIC_REWRITE_PLAN_BUDGET=1` disables the budget for controlled rollback experiments.
  - `TREEDB_VLOG_GENERATION_PERIODIC_REWRITE_PLAN_BUDGET_MS` overrides the internal budget in milliseconds for diagnostics.
- Optional debt-drain cap overrides (for controlled experiments):
  - `TREEDB_VLOG_GENERATION_REWRITE_RESUME_MAX_SEGMENTS`
  - `TREEDB_VLOG_GENERATION_REWRITE_DEBT_DRAIN_MAX_SEGMENTS`
  - `TREEDB_VLOG_GENERATION_REWRITE_FRESH_PLAN_DEBT_DRAIN_MIN_SEGMENTS`
  - `TREEDB_VLOG_GENERATION_REWRITE_FRESH_PLAN_DEBT_DRAIN_MAX_SEGMENTS`
  - Effective values are exported in stats under:
    - `treedb.cache.vlog_generation.rewrite.queue_config.*`
  - Runtime cap decisions are exported in stats under:
    - `treedb.cache.vlog_generation.rewrite.queue_run_segment_cap*`

## Bench Commands
### Churn sanity (TreeDB)
```bash
GOWORK=off make unified-bench
LEGACY_BENCH_PROFILE=fast # legacy benchmark-runner ceiling preset, not a public TreeDB server profile
./bin/unified-bench \
  -dbs treedb \
  -profile "$LEGACY_BENCH_PROFILE" \
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
LEGACY_BENCH_PROFILE=fast # legacy benchmark-runner ceiling preset, not a public TreeDB server profile
./bin/unified-bench \
  -dbs treedb \
  -profile "$LEGACY_BENCH_PROFILE" \
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
