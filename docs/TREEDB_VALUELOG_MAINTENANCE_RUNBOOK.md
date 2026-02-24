# TreeDB Value-Log Maintenance Runbook

## Scope

This runbook covers **online value-log maintenance** in TreeDB cached mode:

- Background GC
- Background incremental rewrite
- Score/cooldown/budget tuning

## Contract

- Value log is persistent append-only storage.
- GC deletes only fully unreachable segments.
- Rewrite is the only compaction path for mixed live/stale segments.
- Pointer remap publish must be atomic.
- No early segment deletion before unreachable proof.

## Core Controls

### Loop intervals

- `BackgroundValueLogGCInterval`
- `BackgroundValueLogRewriteInterval`
- `BackgroundValueLogRewriteCooldown`

### Score gating

- `BackgroundValueLogRewriteScoreTargetTotalBytes`
- `BackgroundValueLogRewriteScoreTargetStaleBytes`
- `BackgroundValueLogRewriteScoreTargetChurnBytes`
- `BackgroundValueLogRewriteScoreTrigger`
- `BackgroundValueLogRewriteScoreCooldownBypass`

Rewrite score:

`score = max(total_bytes/target_total, stale_bytes/target_stale, churn_bytes/target_churn)`

Rewrite runs when `score >= trigger`. Cooldown can be bypassed when score is high.

### Rewrite budget

- `BackgroundValueLogRewriteBudgetBytesPerSec`

This bounds rewrite source intake per interval and keeps rewrite incremental.

### Generation targets

- `ValueLog.RewriteHotSegmentTargetBytes`
- `ValueLog.RewriteWarmSegmentTargetBytes`
- `ValueLog.RewriteColdSegmentTargetBytes`

Default cold target: `256 MiB`.

## Operational Profiles

### Balanced default

- Enable GC and rewrite intervals.
- Keep score trigger at default (`1.0`).
- Keep cooldown bypass at default (`1.5`).
- Set moderate rewrite budget to avoid foreground latency cliffs.

### Size-biased

- Lower score targets and/or trigger rewrite sooner.
- Raise rewrite budget and cold segment size target.
- Monitor read p95 before increasing block/segment sizes aggressively.

### Throughput-biased

- Increase score targets and reduce rewrite budget.
- Keep GC frequent to drop dead segments cheaply.
- Run periodic explicit rewrite during low-traffic windows.

## Metrics To Watch

From `db.Stats()`:

- `treedb.bg_vlog_maintenance.last_rewrite_score`
- `treedb.bg_vlog_maintenance.last_gc_bytes_total`
- `treedb.bg_vlog_maintenance.last_gc_bytes_eligible`
- `treedb.bg_vlog_maintenance.last_churn_bytes`
- `treedb.vlog.generation.hot.*`
- `treedb.vlog.generation.warm.*`
- `treedb.vlog.generation.cold.*`

Alert on:

- monotonic WAL growth without reclaim,
- rewrite score persistently above trigger without rewrite runs,
- repeated maintenance errors.

## Validation Checklist

1. Reopen parity after mixed set/update/delete churn.
2. Crash windows:
   - after copy before publish,
   - after publish before cleanup.
3. No orphan pointers and no deleted-live segment regressions.
4. Cross-mode parity:
   - `v1`
   - `v2_fenceptr`
   - `v2_blockptr`
   - `v1_leaflog_route`

## Notes For Bench Runs

- `unified-bench` `fast` profile may disable background maintenance defaults for throughput measurement.
- For maintenance validation, run with an options profile that enables background GC/rewrite and inspect `db.Stats()` counters.
