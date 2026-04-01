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
- pre-rewrite size:
  - `du_sync_app` as the primary end-of-run directory metric
  - `sync_app` as the immediate launcher end-of-sync snapshot
  - `sync_wal`, optional `sync_gzip`
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
  - `s_sync_app_bytes_per_block` for the launcher end snapshot
  - `s_du_sync_app_bytes_per_block` for the scored post-run `du` snapshot
  - `s_post_app_bytes_per_block`
  - `t_sync_seconds_per_block`
  - `t_total_seconds_per_block`
- `pairs.csv` now includes:
  - `delta_blocks_synced`
  - `delta_s_sync_app_bytes_per_block` for the scored post-run `du` snapshot
  - `delta_s_launcher_sync_app_bytes_per_block` for the launcher end snapshot
  - `delta_t_total_seconds_per_block`
- `summary.md` includes `pairs with block-count drift` so moving-target runs
  are visible before making a promote/reject decision.

`run_celestia` A/B scoring now uses `du_sync_app_bytes` rather than the
launcher's immediate `end_app_bytes`. This matches the actual end-of-run
`application.db` directory size that we optimize for. The launcher-end
`sync_app_bytes` snapshot is still reported because it can reveal short
post-sync settling effects, but it is no longer the primary score input.

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

To attribute the finished `application.db/maindb/wal` live bytes by source
class after a run, use:

```bash
GOWORK=off go run ./TreeDB/cmd/treemap vlog-attribution \
  ~/.celestia-app-mainnet-treedb-<run-id>/data/application.db \
  -rw -by-file-top 8
```

Interpretation notes:
- `outer_leaf` reports outer-leaf payload records kept live by leafrefs
- `nested_outer_leaf_pointer` reports value-log payloads reachable through
  outer-leaf blob refs
- `leaf_pages`, `leaf_avg_entries`, `leaf_avg_free_bytes`, and
  `leaf_avg_fill_ppm` summarize the referenced outer-leaf pages themselves, so
  the command can distinguish "too many bytes because pages are underfilled"
  from "too many bytes despite dense pages"
- grouped frame `stored_bytes` are apportioned across live classes by payload
  share within that frame, so the command is intended for directional source
  attribution rather than exact reclaim accounting

To run the fast codec loop on the extracted outer-leaf page corpus from a
finished TreeDB home:

```bash
GOWORK=off go run ./TreeDB/cmd/template_corpus_extract \
  -app-dir ~/.celestia-app-mainnet-treedb-<run-id>/data/application.db \
  -out-dir /tmp/outer_leaf_codec_corpus_<run-id> \
  -skip-pointer \
  -outer-leaf-limit 20000 \
  -outer-leaf-stride 1 \
  -overwrite

GOWORK=off go run ./TreeDB/cmd/vlog_codec_shape_sweep \
  -input /tmp/outer_leaf_codec_corpus_<run-id>/outer_leaf_pages.bin \
  -input-format auto \
  -train 15000 \
  -eval 5000 \
  -k 1,2,4,8,16,32 \
  -level fastest \
  -out /tmp/outer_leaf_codec_shape_sweep_<run-id>.json
```

The codec sweep accepts both the older JSONL `{val}` dataset format and the
binary corpus format emitted by `template_corpus_extract`
(`u32le length + raw payload`).

Recent outer-leaf corpus result (20k pages from runs `20260331231403` and
`20260331230641`):
- best block path: `snappy k=32`, total ratio about `0.535`
- best dict path: `dict_h64k_s64k_noent k=32`, total ratio about `0.481`
- relative stored-byte improvement vs best block path: about `10%`
- the attribution pass on those same homes showed outer-leaf bytes were already
  about `99.18%` of referenced live value-log bytes and leaf pages were already
  about `986,765 ppm` full, so this result points at value-log compression of
  dense 4 KiB leaf pages rather than page-density tuning

To exercise the actual TreeDB write path on that same extracted outer-leaf
corpus, first convert the binary corpus to the JSONL `{val}` format expected by
`vlog_dict_realdata`:

```bash
python3 - <<'PY'
import base64, json, struct
src = "/tmp/outer_leaf_codec_corpus_<run-id>/outer_leaf_pages.bin"
dst = "/tmp/outer_leaf_pages_<run-id>.jsonl"
with open(src, "rb") as f, open(dst, "w", encoding="utf-8") as out:
    while True:
        hdr = f.read(4)
        if not hdr:
            break
        if len(hdr) != 4:
            raise SystemExit("short length header")
        n = struct.unpack("<I", hdr)[0]
        payload = f.read(n)
        if len(payload) != n:
            raise SystemExit("short payload")
        out.write(json.dumps({
            "val": base64.b64encode(payload).decode("ascii"),
            "encoding": "base64",
        }))
        out.write("\n")
PY
```

Then run the write-path bench with outer leaves forced into the value log:

```bash
GOWORK=off go run ./TreeDB/cmd/vlog_dict_realdata \
  -input /tmp/outer_leaf_pages_<run-id>.jsonl \
  -input-encoding auto \
  -train 15000 \
  -eval 5000 \
  -cap 0 \
  -bench-kv \
  -bench-mode wal_off \
  -bench-index-outer-leaves-in-vlog \
  -bench-compression-mode block \
  -bench-block-codec snappy \
  -bench-raw-mib 64 \
  -bench-batch 1024 \
  -bench-workers 1 \
  -bench-key-mode sequential \
  -bench-pointer-threshold 1
```

Equivalent explicit dict and auto-mode probes:

```bash
GOWORK=off go run ./TreeDB/cmd/vlog_dict_realdata \
  -input /tmp/outer_leaf_pages_<run-id>.jsonl \
  -input-encoding auto \
  -train 15000 \
  -eval 5000 \
  -cap 0 \
  -bench-kv \
  -bench-mode wal_off \
  -bench-index-outer-leaves-in-vlog \
  -bench-compression-mode dict \
  -bench-auto-policy size \
  -bench-dict-class-mode split_outer_leaf \
  -bench-block-codec snappy \
  -bench-raw-mib 64 \
  -bench-batch 1024 \
  -bench-workers 1 \
  -bench-key-mode sequential \
  -bench-pointer-threshold 1 \
  -bench-dict-train-mib 1 \
  -bench-dict-bytes 65536 \
  -bench-dict-sample-stride 1

GOWORK=off go run ./TreeDB/cmd/vlog_dict_realdata \
  -input /tmp/outer_leaf_pages_<run-id>.jsonl \
  -input-encoding auto \
  -train 15000 \
  -eval 5000 \
  -cap 0 \
  -bench-kv \
  -bench-mode wal_off \
  -bench-index-outer-leaves-in-vlog \
  -bench-compression-mode auto \
  -bench-auto-policy size \
  -bench-dict-class-mode split_outer_leaf \
  -bench-block-codec snappy \
  -bench-raw-mib 64 \
  -bench-batch 1024 \
  -bench-workers 1 \
  -bench-key-mode sequential \
  -bench-pointer-threshold 1 \
  -bench-dict-train-mib 1 \
  -bench-dict-bytes 65536 \
  -bench-dict-sample-stride 1
```

Recent write-path result on extracted outer-leaf pages from run `20260331231403`
(steady-state ratio is the relevant metric because the dict runs pay warmup and
training overhead before steady begins):
- block `snappy`:
  - `steady_raw_MBps=662.084`
  - `steady_vlog_ratio=0.604185`
- explicit dict `size + split_outer_leaf + 64 KiB dict`:
  - `steady_raw_MBps=578.303`
  - `steady_vlog_ratio=0.498880`
- auto `size + split_outer_leaf + 64 KiB dict`:
  - `steady_raw_MBps=599.183`
  - `steady_vlog_ratio=0.602922`

Interpretation:
- explicit outer-leaf dicting is now visibly effective on the actual TreeDB
  write path once legacy 4 KiB leaf pages are classified correctly
- current auto mode does publish an outer-leaf dict in this setup, but it still
  lands near the block-compression ratio rather than the explicit dict ratio
- the next product lever is therefore auto-mode selection/write-mode behavior
  for outer-leaf streams, not more classification work

Follow-up replay sweeps on the mixed `vlog-replay` corpus showed the competing
block path still had meaningful headroom: increasing grouped block target from
the default `4 KiB` to `32-64 KiB` improved both throughput and stored-byte
ratio on `auto + size + split_outer_leaf` without changing dict/block selection
mix. The current candidate branch therefore defaults the unset block target to
`32 KiB` only for that explicit outer-leaf auto-size path.

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
  - these snapshots are offline `treemap` opens, not in-process runtime counters from the live node; `run.json.maintenance_summary_is_live_runtime` marks this (`true` only when diagnostics JSON is present).
- when `PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST=1`, the second leg also forces a tighter stop poll:
  - `PAIR_ALIGN_STOP_POLL_INTERVAL_SECONDS` (default `0.2`)
  - this is written into the overlay as `POLL_INTERVAL_SECONDS` alongside `STOP_AT_LOCAL_HEIGHT`
  - fractional seconds are allowed
  - use this to reduce avoidable block overshoot and invalid low-noise pairs on stop-aligned runs
- `run_celestia_ab` also performs a lightweight periodic `/debug/vars` sample during each live run:
  - enable/disable: `AB_CAPTURE_LIVE_DEBUG_VARS=1|0` (default `1`)
  - sample interval: `AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS` (default `30`)
  - per-request timeout: `AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS` (default `5`)
  - source URL: `AB_LIVE_DEBUG_VARS_URL` (default `http://127.0.0.1:6062/debug/vars`)
  - latest successful sample is stored as `live_debug_vars_latest.json` in each run directory
  - if a successful run lacks on-disk diagnostics JSON, the harness retries maintenance analysis against that sampled payload so `run.json.maintenance_summary` can still reflect live runtime counters
- maintenance summaries also export exact-source counters for `rewrite_stage_confirm` vs `rewrite_stage_confirm_exit`, so restart loops can be separated from the initial stage-confirm pass in `run.json`, `runs.csv`, and `analyze_vlog_maintenance_capacity.py`
- retained-prune accounting needs two views:
  - `retained_prune_zombie_marked_bytes` / `retained_prune_removed_bytes` report work done inline by the prune pass itself
  - `vlog_zombie_bytes` / `vlog_zombie_pinned_bytes` report end-of-capture zombie state in the value-log manager
  - a live run can therefore show `retained_prune_removed_bytes=0` even when zombie-marked segments have already drained by the final summary capture; use the end-of-capture `vlog_zombie_*` fields to tell whether zombie bytes are still present
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

### Rejected Path: `hot_only 1MiB`

Do not continue tuning `TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES=1048576`
inside the live scheduler path without a new, materially different hypothesis.

Why:
- It consistently regressed `run_celestia` sync time and pre-rewrite
  `application.db` size.
- Removing the checkpoint-like bypass rewrite did not rescue the result.
- Offline rewrite collapses the candidate back near control, which means the
  loss is primarily a pre-rewrite fragmentation / segment-count problem rather
  than a better steady-state layout.

Representative artifacts:
- baseline bad config:
  - `artifacts/celestia_ab/hot_only_1m_smoke_20260331135014/summary.md`
- follow-up with checkpoint-like bypass removed:
  - `artifacts/celestia_ab/hot_only_1m_skip_low_live_smoke_20260331143719/summary.md`

Key evidence from the follow-up run:
- control:
  - `t_sync=379s`
  - `s_sync_app=3,553,186,986`
  - offline rewrite: `segments_before=528`, `segments_after=17`,
    `bytes_before=3,441,325,752`, `bytes_after=2,197,110,371`
- candidate (`hot_only 1MiB`):
  - `t_sync=434s`
  - `s_sync_app=5,066,049,606`
  - checkpoint-like bypass removed:
    - `rewrite_runs_source_bypass=0`
    - `rewrite_checkpoint_like_runs=0`
  - offline rewrite: `segments_before=5102`, `segments_after=17`,
    `bytes_before=4,949,728,217`, `bytes_after=2,209,496,347`

Interpretation:
- The candidate creates far too many tiny hot segments during sync.
- Live rewrite only drains a tiny fraction of that debt before the run ends.
- The extra pre-rewrite bytes are dominated by `application.db/maindb/wal`
  fragmentation, not by a better on-disk final state.

Implication:
- Prefer a different lever next:
  - rewrite source selection / quality, or
  - a separate segment-layout experiment outside the live scheduler loop
    (for example moderate segment sizes, not `1MiB`).

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
