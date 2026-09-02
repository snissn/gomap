# Celestia: Checkpoint-Kick Hot-Debt-Only Gate (2026-03-28)

## Goal
Reduce `run_celestia` sync wall-time regression from live value-log maintenance while preserving on-disk size gains.

## Change Under Test
Candidate enabled the then-experimental hot-debt-only checkpoint-kick guard:

- historical env: `TREEDB_ENABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1`
- current default: the guard is enabled by default; use `TREEDB_DISABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1` only for rollback experiments.

Behavior:

- In WAL-off checkpoint-kick path, if foreground is hot and rewrite queue is empty, skip starting a fresh rewrite plan.
- Queued rewrite debt and deferred-due maintenance still run.

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
- `hot_debt_only`: `LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active` + historical `TREEDB_ENABLE_VLOG_GENERATION_CHECKPOINT_KICK_HOT_DEBT_ONLY=1`

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

## 2026-03-29 Follow-Up (Hybrid Wake Path)
Code under test additionally keeps hot-debt-only skip behavior, but adds a bounded
deferred wake that retries checkpoint-kick after the maintenance quiet window.

### Repro Command
```bash
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active \
USE_LOCAL_TREE_STACK=1 \
TREEDB_OPEN_PROFILE=fast \
~/run_celestia.sh
```

Run home:

- `/home/mikers/.celestia-app-mainnet-treedb-20260329044918`

### Sync Result (pre offline rewrite)
- `duration_seconds=402`
- `end_app_bytes=4,762,541,410`
- `max_rss_kb=11,078,464`

### Offline Rewrite Result
Command:

```bash
/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local \
  vlog-rewrite /home/mikers/.celestia-app-mainnet-treedb-20260329044918/data/application.db -rw
```

Result:

- `elapsed_seconds=46.23`
- `max_rss_kb=201,432`
- `bytes_before=4,633,867,260`
- `bytes_after=2,232,674,890`
- `segments_before=21`
- `segments_after=17`
- `records=1,030,810`
- `post_rewrite_app_bytes=2,273,530,132`
- `post_rewrite_gzip_bytes=1,861,954,157` (`gzip -1` over application.db)

### Context vs Recent Fast Runs
Recent same-day fast-profile runs (no offline rewrite in `duration_seconds`) were:

- `20260329034043`: `duration_seconds=303`, `end_app_bytes=5,339,008,353`
- `20260329035348`: `duration_seconds=317`, `end_app_bytes=5,259,222,322`
- `20260329040218`: `duration_seconds=315`, `end_app_bytes=5,247,290,570`
- `20260329040840`: `duration_seconds=360`, `end_app_bytes=5,261,253,378`
- `20260329041947`: `duration_seconds=327`, `end_app_bytes=5,339,742,186`
- `20260329042621`: `duration_seconds=303`, `end_app_bytes=5,293,282,308`

Observed tradeoff in this follow-up sample:

- Better pre-rewrite size (`4.76G` vs recent `~5.25G-5.34G`)
- Slower sync wall-time (`402s` vs recent `~303s-360s`)
- Sync + offline rewrite total wall-time `~448s`

Note: pre-rewrite gzip for this exact run was not captured before rewrite; include
that in the next controlled interleaved A/B pass.

## 2026-03-29 Rewrite No-Rescan Follow-Up
Patch under test:

- Online `ValueLogRewriteOnline` now reuses the manager's post-publish set for:
  - rewrite health metadata update
  - `SegmentsAfter`/`BytesAfter` stats
- This avoids extra WAL directory scans on the online rewrite hot path.

### Single `run_celestia` sanity run
Command:

```bash
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active \
USE_LOCAL_TREE_STACK=1 \
TREEDB_OPEN_PROFILE=fast \
TRUST_HEIGHT=10432048 \
TRUST_HASH=11DFB276B9941D4A5C071641A9F4166F4A7FE2FA2AFC6B5106715D5BAB0EAAB1 \
STOP_AT_LOCAL_HEIGHT=10434185 \
~/run_celestia.sh
```

Run home:

- `/home/mikers/.celestia-app-mainnet-treedb-20260329060509`

Sync result:

- `duration_seconds=280`
- `end_app_bytes=4,343,298,864` (from `sync-time.log`)
- `sync_app_bytes=4,344,366,789` (post-run `du -sb`)
- `max_rss_kb=7,419,948`

Offline rewrite:

```bash
/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local \
  vlog-rewrite /home/mikers/.celestia-app-mainnet-treedb-20260329060509/data/application.db -rw
```

- `rewrite_seconds=45`
- `segments_before=19`
- `segments_after=16`
- `bytes_before=4,191,577,486`
- `bytes_after=2,015,614,944`
- `records=966,376`
- `post_app_bytes=2,054,110,809`
- `post_wal_bytes=2,015,619,040`
- `post_gzip_bytes=1,701,847,087` (`gzip -1`)

### Microbench signal (rewrite path)
Benchmark:

```bash
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench 'BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs$' \
  -benchmem -benchtime=3x -count=1
```

Before no-rescan patch:

- `26630695 ns/op`
- `4807149 B/op`
- `1845 allocs/op`

After no-rescan patch:

- `26645666 ns/op`
- `4768021 B/op`
- `1430 allocs/op`

Interpretation:

- Runtime stayed effectively flat in this microbench.
- Allocation pressure improved materially (`~22.5%` fewer allocs/op).
- End-to-end significance still needs interleaved control/candidate runs.

### Patch-Isolated A/B (same commit, local diff only)
Purpose: isolate this uncommitted no-rescan patch from all other branch deltas.

Control/candidate setup:

- `control`: clean detached worktree at current `HEAD` (`/tmp/gomap_patch_base_20260329061302`)
- `candidate`: working tree (`/home/mikers/dev/snissn/gomap-phasehook-active`)
- shared env: `TREEDB_OPEN_PROFILE=fast`, fixed trust/stop-height, offline rewrite enabled

Harness:

```bash
OUT_DIR=/tmp/celestia_ab_patch_20260329061302 \
CONTROL_ENV_FILE=/tmp/celestia_ab_patch_ctrl_20260329061302.env \
CANDIDATE_ENV_FILE=/tmp/celestia_ab_patch_cand_20260329061302.env \
MAX_PAIRS=1 MIN_PAIRS=1 CLEAR_WIN_PAIRS=1 CLEAR_LOSS_PAIRS=1 \
LOW_SIGNAL_MIN_PAIRS=1 LOW_SIGNAL_NEUTRAL_STREAK=1 \
SIZE_TOLERANCE_BYTES=$((64<<20)) TIME_TOLERANCE_SECONDS=120 \
REWRITE_ENABLED=1 RUN_TIMEOUT_SECONDS=1800 RUN_MAX_ATTEMPTS_PER_VARIANT=2 \
./scripts/run_celestia_ab.sh
```

Result (`candidate - control`):

- `delta_t_sync_seconds = -9`
- `delta_t_total_seconds = -12`
- `delta_s_sync_app_bytes = -61,351,635`
- `delta_s_post_wal_bytes = -1,186,655`
- outcome: `neutral` (below `64 MiB` size tolerance and `120s` time tolerance)

Pair notes:

- This pair was valid (`control_valid=true`, `candidate_valid=true`) but noisy
  (both runs entered extended state-sync discover/apply phases).

### Fast-Loop Follow-Up: Cached Segment Size Reuse
Additional patch:

- Exported best-effort segment size from value-log file metadata and reused it
  in DB-side `fileSize(...)` callers to reduce repeated `stat(2)` calls on the
  rewrite/health stats path.

Primary microbench (`-benchtime=3x`, same host):

- previous (after no-rescan patch): `26645666 ns/op`, `4768021 B/op`, `1430 allocs/op`
- with cached-size reuse: `25392517 ns/op`, `4641410 B/op`, `855 allocs/op`

Secondary check:

- `BenchmarkValueLogRewriteOnline_ValuePointers`: `20536220 ns/op`, `4608546 B/op`, `710 allocs/op`

Interpretation:

- This is a meaningful additional allocation drop on the rewrite hot path in the
  fast loop.
- A long multi-pair end-to-end A/B attempt was started but aborted due another
  high-noise near-target crawl in control (slow/no-signal loop).

### Single-Run Follow-Up With Cached-Size Reuse
Command (same profile/trust/stop-height as prior sanity runs):

```bash
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active \
USE_LOCAL_TREE_STACK=1 \
TREEDB_OPEN_PROFILE=fast \
TRUST_HEIGHT=10432048 \
TRUST_HASH=11DFB276B9941D4A5C071641A9F4166F4A7FE2FA2AFC6B5106715D5BAB0EAAB1 \
STOP_AT_LOCAL_HEIGHT=10434185 \
~/run_celestia.sh
```

Run home:

- `/home/mikers/.celestia-app-mainnet-treedb-20260329063701`

Sync result:

- `duration_seconds=225`
- `end_app_bytes=4,767,004,994`
- `max_rss_kb=8,024,660`

Offline rewrite:

```bash
/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local \
  vlog-rewrite /home/mikers/.celestia-app-mainnet-treedb-20260329063701/data/application.db -rw
```

- `rewrite_seconds=42`
- `segments_before=20`
- `segments_after=15`
- `bytes_before=4,645,416,218`
- `bytes_after=1,955,509,927`
- `post_app_bytes=1,990,068,009`
- `post_wal_bytes=1,955,509,927`
- `post_gzip_bytes=1,668,555,899` (`gzip -1`)

Delta vs prior sanity run (`/home/mikers/.celestia-app-mainnet-treedb-20260329060509`):

- `sync_duration_seconds`: `280 -> 225` (`-55s`, `-19.64%`)
- `sync_end_app_bytes`: `4,343,298,864 -> 4,767,004,994` (`+423,706,130`, `+9.76%`)
- `sync_max_rss_kb`: `7,419,948 -> 8,024,660` (`+604,712`, `+8.15%`)
- `postrewrite_wal_bytes`: `2,015,619,040 -> 1,955,509,927` (`-60,109,113`, `-2.98%`)
- `postrewrite_app_bytes`: `2,054,110,809 -> 1,990,068,009` (`-64,042,800`, `-3.12%`)
- `postrewrite_gzip_bytes`: `1,701,847,087 -> 1,668,555,899` (`-33,291,188`, `-1.96%`)

Notes:

- This is still a single-run comparison and remains susceptible to state-sync
  phase variance.
- It confirms the new patch does not obviously hurt post-rewrite size and
  remains consistent with the microbench allocation improvements.

## 2026-03-29 Follow-Up: Remove Forced Commit-Time Value-Log Refresh
Patch under test:

- Commit publish path now relies on touched-segment checks and registered
  value-log segments instead of forcing `valueLogManager.Refresh()` whenever
  `indexOuterLeavesInValueLog` is enabled.
- Added refresh-scan guard coverage for outer-leaf write loops.

### Targeted correctness checks
Command:

```bash
GOWORK=off go test ./TreeDB/db -run \
'Test(InlineCommitSkipsValueLogRefresh|PointerCommitRefreshesValueLogSet|PointerCommitSkipsValueLogRefreshWhenSegmentAlreadyRegistered|OuterLeafCommitPublishesRegisteredSegmentWithoutExplicitRefresh|OuterLeafWriteLoopSkipsForcedValueLogRefreshScans|ValueLogGC_DoesNotRescanWhenSetAlreadyPopulated|ValueLogRewritePlan_DoesNotRescanWhenSetAlreadyPopulated|ValueLogRewriteOnline_LeafRefsReserveRIDs_DoesNotRefreshManager)$' \
-count=1
```

Result:

- `ok github.com/snissn/gomap/TreeDB/db`

### Fast microbench signal (outer-leaf write loop)
Command:

```bash
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench 'BenchmarkOuterLeafWriteLoop_(NoRefresh|ForcedRefresh)$' \
  -benchmem -benchtime=1x -count=1
```

Result:

- `NoRefresh`: `2.081s/op`, `0 refresh_scans/iter`, `33,565,000 B/op`, `63,734 allocs/op`
- `ForcedRefresh`: `2.098s/op`, `2000 refresh_scans/iter`, `35,315,952 B/op`, `95,740 allocs/op`

Interpretation:

- In this local synthetic loop, forced refresh has modest wall-time impact but
  materially higher allocation churn.
- This supports keeping forced refresh off when segment registration is already
  in place.

### `run_celestia` probe
Command:

```bash
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active \
USE_LOCAL_TREE_STACK=1 \
TREEDB_OPEN_PROFILE=fast \
TRUST_HEIGHT=10432048 \
TRUST_HASH=11DFB276B9941D4A5C071641A9F4166F4A7FE2FA2AFC6B5106715D5BAB0EAAB1 \
STOP_AT_LOCAL_HEIGHT=10434185 \
~/run_celestia.sh
```

Attempt A (`/home/mikers/.celestia-app-mainnet-treedb-20260329065615`):

- Aborted by node panic at block `10434001`:
  - `collections: not found: key 'no_key' ... distribution.v1beta1.Params`
- Treated as invalid datapoint (no completed sync metrics).

Attempt B (`/home/mikers/.celestia-app-mainnet-treedb-20260329070113`):

- `duration_seconds=302`
- `end_app_bytes=3,188,229,248` (from `sync-time.log`)
- `max_rss_kb=8,784,824`
- `rewrite_seconds=43`
- `segments_before=14`, `segments_after=16`
- `bytes_before=2,972,289,592`, `bytes_after=2,016,638,090`
- `post_app_bytes=2,055,063,050`
- `post_wal_bytes=2,016,638,090`
- `post_gzip_bytes=1,701,260,711`

Comparison vs prior cached-size sanity run (`/home/mikers/.celestia-app-mainnet-treedb-20260329063701`):

- `sync_seconds`: `225 -> 302` (`+77s`)
- `sync_end_app_bytes`: `4,767,004,994 -> 3,188,229,248` (`-1,578,775,746`)
- `post_app_bytes`: `1,990,068,009 -> 2,055,063,050` (`+64,995,041`)
- `post_gzip_bytes`: `1,668,555,899 -> 1,701,260,711` (`+32,704,812`)

Interpretation:

- Full-run outcome remains noisy; this sample does not show a clear wall-time +
  post-rewrite size win despite better pre-rewrite app bytes.
- Keep this change as “candidate” pending interleaved control/candidate pairs.

## 2026-03-29 Follow-Up: Rewrite Swap Match Sort Fast-Path
Patch under test:

- `collectRewriteSwapPointerMatches` now skips `sort.Slice` when swap keys are
  already in non-decreasing order (the common default-locality rewrite path),
  and only sorts when needed.

### Targeted correctness checks
Command:

```bash
GOWORK=off go test ./TreeDB/db -run \
'TestRewriteSwapsKeySorted|Test(InlineCommitSkipsValueLogRefresh|PointerCommitRefreshesValueLogSet|PointerCommitSkipsValueLogRefreshWhenSegmentAlreadyRegistered|OuterLeafCommitPublishesRegisteredSegmentWithoutExplicitRefresh|OuterLeafWriteLoopSkipsForcedValueLogRefreshScans|ValueLogGC_DoesNotRescanWhenSetAlreadyPopulated|ValueLogRewritePlan_DoesNotRescanWhenSetAlreadyPopulated|ValueLogRewriteOnline_LeafRefsReserveRIDs_DoesNotRefreshManager)$' \
-count=1
```

Result:

- `ok github.com/snissn/gomap/TreeDB/db`

### Microbench A/B (same host load, HEAD~1 vs candidate)
Commands:

```bash
# Baseline (HEAD~1) in temporary worktree
git worktree add /tmp/gomap_base_rewrite HEAD~1
GOWORK=off go test -C /tmp/gomap_base_rewrite ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers$' \
  -benchmem -benchtime=20x -count=5 \
  > /tmp/vlog_rewrite_base_headminus1.txt
git worktree remove /tmp/gomap_base_rewrite --force

# Candidate (current branch)
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers$' \
  -benchmem -benchtime=20x -count=5 \
  > /tmp/vlog_rewrite_candidate_head.txt

benchstat /tmp/vlog_rewrite_base_headminus1.txt /tmp/vlog_rewrite_candidate_head.txt
```

Result summary:

- `sec/op`: statistically unchanged (`~`, `p=0.841`, `n=5`)
- `rewrite_allocs/op`: `189.7 -> 186.6` (`-1.63%`, `p=0.008`, `n=5`)
- `allocs/op`: `189 -> 186` (`-1.59%`, `p=0.008`, `n=5`)

Interpretation:

- This is a small but consistent allocation reduction in the rewrite hot path
  without measured throughput regression in this sample size.

### Follow-on microbench: leafref rewrite map bootstrap sizing
Change:

- Reduce initial `leafMap` and `internalMap` capacities in leafref online rewrite
  from `1024` to `128` so small rewrites avoid over-allocating map buckets.

Commands:

```bash
# Baseline (HEAD~1) in temporary worktree
git worktree add /tmp/gomap_base_leafrefs HEAD~1
GOWORK=off go test -C /tmp/gomap_base_leafrefs ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs$' \
  -benchmem -benchtime=20x -count=12 -cpu=1 \
  > /tmp/leafrefs_base_cpu1.txt
git worktree remove /tmp/gomap_base_leafrefs --force

# Candidate (current branch)
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs$' \
  -benchmem -benchtime=20x -count=12 -cpu=1 \
  > /tmp/leafrefs_cand_cpu1.txt

benchstat /tmp/leafrefs_base_cpu1.txt /tmp/leafrefs_cand_cpu1.txt
```

Result summary:

- `sec/op`: `19.34ms -> 19.03ms` (`-1.57%`, `p=0.045`, `n=12`)
- `rewrite_allocs/op`: `230.2 -> 226.2` (`-1.74%`, `p<0.001`, `n=12`)
- `B/op`: `4.152MiB -> 4.091MiB` (`-1.47%`, `p<0.001`, `n=12`)

Interpretation:

- Small but consistent allocation reduction on leafref rewrite with no latency
  regression in the controlled (`-cpu=1`) sample.

### Follow-on microbench: inline remap cache before map promotion
Change:

- Add small inline remap caches for leaf/internal id rewrites and promote to maps
  only after the inline cache fills.

Commands:

```bash
# Baseline (HEAD~1) in temporary worktree
git worktree add /tmp/gomap_base_leafrefs_remap HEAD~1
GOWORK=off go test -C /tmp/gomap_base_leafrefs_remap ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs$' \
  -benchmem -benchtime=20x -count=8 \
  > /tmp/leafrefs_remap_base_defcpu.txt
git worktree remove /tmp/gomap_base_leafrefs_remap --force

# Candidate (current branch)
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs$' \
  -benchmem -benchtime=20x -count=8 \
  > /tmp/leafrefs_remap_cand_defcpu.txt

benchstat /tmp/leafrefs_remap_base_defcpu.txt /tmp/leafrefs_remap_cand_defcpu.txt
```

Result summary:

- `sec/op`: statistically unchanged (`p=0.505`, `n=8`)
- `rewrite_allocs/op`: `235.0 -> 224.4` (`-4.49%`, `p=0.001`, `n=8`)
- `allocs/op`: `234.5 -> 224.0` (`-4.48%`, `p=0.001`, `n=8`)
- `B/op`: `4.125MiB -> 4.110MiB` (`-0.37%`, `p=0.015`, `n=8`)

Interpretation:

- This provides a meaningful allocation reduction on the leafref rewrite path
  with no measured throughput regression in this sample.

### Follow-on microbench: inline value-log ref delta before map promotion
Change:

- Use inline storage for `valueLogRefDelta` changes and promote to a map only
  after inline capacity is exceeded.

Commands:

```bash
# Baseline (HEAD~1) in temporary worktree
git worktree add /tmp/gomap_base_valueptr_delta HEAD~1
GOWORK=off go test -C /tmp/gomap_base_valueptr_delta ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers$' \
  -benchmem -benchtime=20x -count=8 \
  > /tmp/valueptr_delta_base_defcpu.txt
git worktree remove /tmp/gomap_base_valueptr_delta --force

# Candidate (current branch)
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers$' \
  -benchmem -benchtime=20x -count=8 \
  > /tmp/valueptr_delta_cand_defcpu.txt

benchstat /tmp/valueptr_delta_base_defcpu.txt /tmp/valueptr_delta_cand_defcpu.txt
```

Result summary:

- `sec/op`: statistically unchanged (`p=0.130`, `n=8`)
- `rewrite_allocs/op`: `187.1 -> 185.2` (`-1.02%`, `p=0.007`, `n=8`)
- `allocs/op`: `186.5 -> 185.0` (`-0.80%`, `p=0.030`, `n=8`)

Interpretation:

- Small but consistent allocation reduction in the value-pointer rewrite path
  with no measured throughput regression in this sample.

### Follow-on microbench: no-copy small touched segment view
Change:

- In `Batch.TouchedValueLogSegments()`, return a sorted view over the existing
  small touched-segment buffer when the small-set fast path is active (no map),
  instead of allocating a copied slice.

Commands:

```bash
# Baseline (HEAD~1) in temporary worktree
git worktree add /tmp/gomap_base_touchseg HEAD~1
GOWORK=off go test -C /tmp/gomap_base_touchseg ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers$' \
  -benchmem -benchtime=20x -count=10 -cpu=1 \
  > /tmp/vp_touch_base.txt
git worktree remove /tmp/gomap_base_touchseg --force

# Candidate (current branch)
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers$' \
  -benchmem -benchtime=20x -count=10 -cpu=1 \
  > /tmp/vp_touch_cand.txt

benchstat /tmp/vp_touch_base.txt /tmp/vp_touch_cand.txt
```

Result summary:

- `sec/op`: statistically unchanged (`p=0.631`, `n=10`)
- `rewrite_allocs/op`: `183.0 -> 182.0` (`-0.55%`, `p<0.001`, `n=10`)
- `allocs/op`: `183.0 -> 182.0` (`-0.55%`, `p<0.001`, `n=10`)

Interpretation:

- Small incremental allocation reduction on value-pointer rewrite with no
  measured throughput regression in this sample.
