# Celestia: Checkpoint-Kick Hot-Debt-Only Gate (2026-03-28)

## Goal
Reduce `run_celestia` sync wall-time regression from live value-log maintenance while preserving on-disk size gains.

## Change Under Test
Candidate enables:

- `TREEDB_ENABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1`

Behavior:

- In WAL-off checkpoint-kick path, if foreground is hot and rewrite queue is empty, skip starting a fresh rewrite plan.
- Queued rewrite debt and deferred-due maintenance still run.
- Default behavior remains unchanged unless this env flag is set.

## Commands
Both campaigns used fixed trust/target and a single interleaved pair (`MAX_PAIRS=1`) with offline rewrite enabled.

Common env (both variants):

- `TREEDB_OPEN_PROFILE=fast`
- `POLL_INTERVAL_SECONDS=1`
- `FREEZE_REMOTE_HEIGHT_AT_START=1`
- `ALLOW_CLAMPED_TARGET_EARLY_EXIT=1`
- `STOP_AT_LOCAL_HEIGHT=<captured at campaign start>`
- `TRUST_HEIGHT=<captured at campaign start>`
- `TRUST_HASH=<captured at campaign start>`

Variant-specific env:

- `main`: `LOCAL_GOMAP_DIR=/tmp/gomap_ab_base_20260328162444`
- `hot_debt_only`: `LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active` + `TREEDB_ENABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1`

Harness:

```bash
OUT_DIR=<out> \
CONTROL_ENV_FILE=<control_env> \
CANDIDATE_ENV_FILE=<candidate_env> \
MAX_PAIRS=1 MIN_PAIRS=1 CLEAR_WIN_PAIRS=1 CLEAR_LOSS_PAIRS=1 \
LOW_SIGNAL_MIN_PAIRS=1 LOW_SIGNAL_NEUTRAL_STREAK=1 \
SIZE_TOLERANCE_BYTES=$((64<<20)) TIME_TOLERANCE_SECONDS=120 \
REWRITE_ENABLED=1 \
./scripts/run_celestia_ab.sh
```

## Runs
- control=main, candidate=hot_debt_only:
  - `/tmp/celestia_ab_hotdebt_20260328171204`
- control=hot_debt_only, candidate=main (swapped to counter order bias):
  - `/tmp/celestia_ab_hotdebt_swapped_20260328172453`

## Normalized Results (hot_debt_only - main)
- Run A (hot_debt_only as candidate):
  - `delta_t_sync_seconds = -16`
  - `delta_t_total_seconds = -17`
  - `delta_s_sync_app_bytes = -694,418,294`
  - `delta_s_post_wal_bytes = +3,315,722`
- Run B (hot_debt_only as control, normalized):
  - `delta_t_sync_seconds = +3`
  - `delta_t_total_seconds = +2`
  - `delta_s_sync_app_bytes = -98,696,592`
  - `delta_s_post_wal_bytes = -3,665,002`

Two-run median/average (same with n=2):

- `delta_t_sync_seconds = -6.5s`
- `delta_t_total_seconds = -7.5s`
- `delta_s_sync_app_bytes = -396,557,443B` (~`-378.2 MiB`)
- `delta_s_post_wal_bytes = -174,640B` (~`-170.5 KiB`, effectively neutral)

## Maintenance Counters
Across both runs, both variants showed:

- `rewrite_runs=0`
- `checkpoint_kick_runs=0`

Candidate (`hot_debt_only`) showed one lightweight GC pass in each run (`gc_runs=1`), with no rewrite execution.

## Takeaway
The hot-debt-only gate removed checkpoint-kick rewrite pressure during hot sync windows and improved sync+rewrite wall time in this small sample, while keeping pre-rewrite app size better than main and post-rewrite WAL roughly neutral.

## Next Step
Run an interleaved sequence with more pairs (stop-on-significance) and include the new stat key:

- `treedb.cache.vlog_generation.checkpoint_kick.skipped_hot_no_debt`

to confirm skip path activation frequency under full mainnet sync pressure.
