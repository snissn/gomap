# Celestia Compression Iteration Loop

This loop exists to avoid slow, low-signal experimentation.

Primary objective:
- Reduce on-disk `application.db` bytes.

Secondary objectives:
- Keep combined wall time (`sync + rewrite`) bounded.
- Avoid memory regressions (`max_rss`).
- Keep gzip as a sanity check, not the primary objective.

## Stage 0: Hypothesis Contract (Required)

Before running anything expensive, define:
- hypothesis: what changed and why it should help
- expected effect size: minimum size delta worth promoting
- time budget: max acceptable wall-time regression
- rollback condition: what result means we stop and redesign

If expected effect size is below threshold, do not run full `run_celestia` yet.

## Stage 1: Fast Gate (Default Iteration Loop)

Use `scripts/celestia_fast_gate.sh` for fast interleaved control/candidate A/B.

What it measures per run:
- pre-rewrite size: `sync_app`, `sync_wal`, optional `sync_gzip`
- post-rewrite size: `post_app`, `post_wal`, optional `post_gzip`
- timing: benchmark duration + rewrite duration + total
- throughput: batch-write ops/sec from unified-bench output

Defaults chosen for celestia-like pressure:
- `-profile fast`
- `-val-pattern celestia_height_prefix_fill`
- dict compression enabled
- dict defaults passed explicitly:
  - `-treedb-vlog-dict-train-bytes=1048576`
  - `-treedb-vlog-dict-dict-bytes=32768`

Fast-gate anti-loop safeguards:
- interleaved order alternates each pair (bias reduction)
- early clear stop (improvement/regression)
- futility stop when remaining pairs cannot reach a clear decision
- low-signal stop on neutral-streak threshold
- per-run process review artifact (`process_review.md`)

Example:

```bash
MAX_PAIRS=6 \
MIN_PAIRS=3 \
CLEAR_WIN_PAIRS=2 \
CLEAR_LOSS_PAIRS=2 \
LOW_SIGNAL_MIN_PAIRS=3 \
LOW_SIGNAL_NEUTRAL_STREAK=3 \
SIZE_FIELD=s_post_app_bytes \
SIZE_TOLERANCE_BYTES=$((64<<20)) \
TIME_TOLERANCE_SECONDS=30 \
./scripts/celestia_fast_gate.sh
```

Outputs:
- `summary.md`
- `process_review.md`
- `runs.csv`
- `pairs.csv`
- per-run `run.json`

Signal hygiene additions in `run_celestia` A/B artifacts:
- `runs.csv` now includes `blocks_synced` plus normalized metrics:
  - `s_sync_app_bytes_per_block`
  - `s_post_app_bytes_per_block`
  - `t_sync_seconds_per_block`
  - `t_total_seconds_per_block`
- `pairs.csv` now includes:
  - `delta_blocks_synced`
  - `delta_s_sync_app_bytes_per_block`
  - `delta_t_total_seconds_per_block`
- `summary.md` includes `pairs with block-count drift` so moving-target runs
  are visible before making a promote/reject decision.

## Stage 2: Pprof/Implementation Efficiency Pass

Run this stage before full `run_celestia` if fast gate shows:
- promising size gains with time regression, or
- ambiguous neutral outcomes near threshold.

Goal:
- remove avoidable implementation overhead (copying/alloc/lock contention)
- preserve size gains while pulling time back inside budget

## Incremental Rewrite Lab Gate (Post-864)

When testing incremental rewrite/scheduler changes, require this order:
1. microbench gate
2. optional pprof efficiency pass (if microbench regresses)
3. then `run_celestia` A/B

Required microbench gate command:

```bash
./scripts/rewrite_micro_gate.sh
```

Gate policy:
- Baseline and candidate must both report:
  - `BenchmarkValueLogRewriteOnline_LeafRefs`
  - `BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs`
- Promote to `run_celestia` only when:
  - allocs/op is non-regressing (or improved), and
  - ns/op regression is either absent or explained by a concrete, fixable implementation issue.

For full `run_celestia` proof runs, set `TREEMAP_BIN` explicitly so rewrite is never silently skipped:

```bash
TREEMAP_BIN=/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local
```

## Stage 3: Full `run_celestia` A/B Confirmation

Only promote candidates that pass Stage 1 and Stage 2.

Use `scripts/run_celestia_ab.sh` with interleaved pairs and stop rules.

Now includes anti-loop safeguards:
- clear stop (improvement/regression)
- futility stop (`futile_remaining_pairs`)
- low-signal neutral-streak stop (`low_signal_neutral_streak`)
- optional composite stop (`COMPOSITE_STOP_ON_CLEAR=1`) using weighted time+size score
- strict new-run-home detection (no fallback to old run dirs)
- per-variant timeout/retry for stuck syncs
- invalid-pair streak stop (`invalid_pair_streak`)
- low-noise scoring policy defaults (`AB_POLICY=low_noise`):
  - `SCORING_MODE=per_block`
  - `BLOCK_DRIFT_TOLERANCE=50`
  - `ALLOW_DRIFT_SCORING=0`

Example:

```bash
MAX_PAIRS=4 \
MIN_PAIRS=3 \
CLEAR_WIN_PAIRS=2 \
CLEAR_LOSS_PAIRS=2 \
LOW_SIGNAL_MIN_PAIRS=3 \
LOW_SIGNAL_NEUTRAL_STREAK=3 \
RUN_TIMEOUT_SECONDS=1800 \
RUN_MAX_ATTEMPTS_PER_VARIANT=2 \
RUN_RETRY_SLEEP_SECONDS=20 \
INVALID_PAIR_STREAK_STOP=2 \
REWRITE_ENABLED=1 \
./scripts/run_celestia_ab.sh
```

Notes:
- Pair execution remains strictly single-run at a time and interleaved by pair order.
- Invalid runs (timeout, launcher failure, missing new run home, rewrite failure) are recorded but excluded from pair scoring.
- Per-run `run.json` now includes `status.sync_probe` (last snapshot chunk, last and max snapshot totals, fetch event count, state-sync-complete flag) for timeout forensics.
- `run_celestia_ab` now captures lightweight pre/post rewrite TreeDB stats snapshots when possible (without requiring heavy diagnostics JSON):
  - enable/disable: `AB_CAPTURE_LIGHT_VLOG_STATS=1|0` (default `1`)
  - command timeout: `AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS` (default `20`)
  - capture path uses `treemap stats -rw` (falls back to `treemap vlog-gc -rw -dry-run` if needed)
  - outputs land in each run directory as `light_stats_pre.txt` / `light_stats_post.txt`, and summarized values are included in `run.json` + `runs.csv`.
- `run_celestia_ab` also performs a lightweight periodic `/debug/vars` sample during each live run:
  - enable/disable: `AB_CAPTURE_LIVE_DEBUG_VARS=1|0` (default `1`)
  - sample interval: `AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS` (default `30`)
  - per-request timeout: `AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS` (default `5`)
  - source URL: `AB_LIVE_DEBUG_VARS_URL` (default `http://127.0.0.1:6062/debug/vars`)
  - latest successful sample is stored as `live_debug_vars_latest.json` in each run directory
  - if a successful run lacks on-disk diagnostics JSON, the harness retries maintenance analysis against that sampled payload
  - `run.json.maintenance_summary_source` distinguishes `diagnostics_json`, `live_debug_vars`, and `light_stats_post`
  - `run.json.maintenance_summary_is_live_runtime` is `true` for `diagnostics_json` and `live_debug_vars`, `false` for offline light-stats fallback

- Optional engagement gating (use when optimizing queued rewrite-debt behavior):
  - `AB_REQUIRE_MAINTENANCE_WITH_REWRITE=1` rejects attempts that never execute the maintenance rewrite lane.
  - `AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC=1` rejects attempts where the queued rewrite-debt executor never runs.
  - Each variant retries up to `RUN_MAX_ATTEMPTS_PER_VARIANT`; see `attempts.log` under the run dir.

- `runs.csv` now also includes rewrite-capacity counters from diagnostics summaries (plan runs/selection, source segment+byte outcomes, ledger bytes, budget utilization, observed-GC drain pct) so candidate/control behavior can be compared without opening each `run.json`.
- `pairs.csv` now includes absolute per-pair control/candidate values (`t_sync`, `t_total`, `s_sync_app`, `s_post_wal`, `max_rss_kb`) plus `composite_score_pct` (negative is better).
- `summary.md` now includes `Absolute Medians` (control vs candidate) in addition to pair deltas.
- `decision.json` now includes:
  - `absolute_aggregates` (control/candidate median+mean totals for time, size, and RSS)
  - `composite` configuration echo and aggregate composite score fields
- `runs.csv` also includes reclaimed-vs-churn and capacity pressure KPIs (for example `rewrite_exec_reclaimed_vs_churn_ratio`, `rewrite_reclaimed_share_of_budget_pct`, `rewrite_budget_consumed_share_of_budget_pct`, `rewrite_ineffective_runs`, `observed_gc_pending_ids`) for faster triage of low-signal loops.
- `runs.csv` also includes rewrite queue pressure and checkpoint-kick behavior KPIs (for example `rewrite_queue_len`, `rewrite_queue_live_bytes_after_tokens`, `rewrite_queue_eta_seconds_budget`, `checkpoint_kick_skipped_hot_no_debt`) so incremental rewrite headroom is visible without log spelunking.
- `runs.csv` includes checkpoint-kick burst limiter usage (`rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst`) so queued-debt burst behavior is visible directly in control/candidate comparisons.
- `pairs.csv` includes per-pair deltas for those KPIs so control/candidate capacity behavior can be compared without post-processing scripts.
- `pairs.csv` includes `delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst` so checkpoint-kick burst usage changes are visible at pair level.
- `runs.csv` now includes checkpoint-like (`bypass` + `checkpoint_pending`) vs non-checkpoint rewrite split metrics, and `pairs.csv` includes deltas for source-efficiency fields such as `delta_rewrite_checkpoint_like_budget_share_pct`.
- To use pre-policy behavior, set `AB_POLICY=legacy`.

### Segment Granularity Sweep Harness

Use `scripts/celestia_segment_sweep.sh` to run repeatable segment-size sweeps while keeping the same A/B stop policy:

```bash
CONTROL_ENV_FILE=/tmp/cel_control.env \
CANDIDATE_BASE_ENV_FILE=/tmp/cel_candidate.env \
SEGMENT_SCOPE=hot_warm_cold \
SEGMENT_BYTES_LIST="4194304 8388608 16777216" \
MAX_PAIRS=4 \
MIN_PAIRS=3 \
CLEAR_WIN_PAIRS=2 \
CLEAR_LOSS_PAIRS=2 \
./scripts/celestia_segment_sweep.sh
```

Outputs:
- `artifacts/celestia_segment_sweep/<ts>/summary.json`
- `artifacts/celestia_segment_sweep/<ts>/summary.md`
- per-candidate run outputs under `artifacts/celestia_segment_sweep/<ts>/runs/seg_*`

## Process Review Cadence

Review and revise the loop after every decision event:
- `clear_improvement`
- `clear_regression`
- `futile_remaining_pairs`
- `low_signal_neutral_streak`

Required review questions:
- Was the fast gate predictive of full-run direction?
- Were thresholds too strict or too loose for current goals?
- Did we spend time validating changes below meaningful effect size?
- Is the next candidate large enough to justify promotion?

If two consecutive campaigns end in low-signal/futility, tighten promotion gates and bundle larger candidate deltas before next full run.
