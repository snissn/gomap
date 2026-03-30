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
  - open mode for stats capture: `AB_LIGHT_VLOG_STATS_RW_OPEN=1|0` (default `1`)
  - outputs land in each run directory as `light_stats_pre.txt` / `light_stats_post.txt`, and summarized values are included in `run.json` + `runs.csv`.
- To use pre-policy behavior, set `AB_POLICY=legacy`.

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
