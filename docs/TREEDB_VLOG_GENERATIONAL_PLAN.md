# TreeDB Generational Value-Log Plan

Status: Draft implementation roadmap for generational segmented value-log management.

## Current Implementation Snapshot

Implemented now:

- Score-gated background rewrite trigger:
  - `score=max(total/target_total, stale/target_stale, churn/target_churn)`.
- Soft cooldown:
  - cooldown can be bypassed when score crosses high-pressure threshold.
- Budgeted rewrite intake:
  - optional bytes/sec budget caps rewrite source bytes per interval.
- Generation-aware rewrite sizing:
  - explicit hot/warm/cold rewrite output segment targets.
- Candidate ranking improvements:
  - reclaim-efficiency bias plus hot-first preference.
- Generation-aware GC preference:
  - dead hot segments are prioritized first.

Key options now available:

- `BackgroundValueLogRewriteScoreTargetTotalBytes`
- `BackgroundValueLogRewriteScoreTargetStaleBytes`
- `BackgroundValueLogRewriteScoreTargetChurnBytes`
- `BackgroundValueLogRewriteScoreTrigger`
- `BackgroundValueLogRewriteScoreCooldownBypass`
- `BackgroundValueLogRewriteBudgetBytesPerSec`
- `ValueLog.RewriteSegmentTargetBytes`
- `ValueLog.RewriteHotSegmentTargetBytes`
- `ValueLog.RewriteWarmSegmentTargetBytes`
- `ValueLog.RewriteColdSegmentTargetBytes`

Current defaults:

- rewrite score trigger: `1.0`
- rewrite cooldown bypass score: `1.5`
- cold rewrite target: `256 MiB`

## 1. Contract And Success Criteria

### Generations

- `hot`: new writes and high-churn updates.
- `warm`: stable data promoted from hot.
- `cold`: long-lived data with lowest rewrite cadence.

### Targets

- Steady-state value-log bytes lower than current churn baseline.
- Rewrite write amplification bounded by explicit ceiling (report and gate in CI/bench).
- Read latency guardrail: no major p95 regression vs current baseline.
- Crash/reopen parity: no data loss, no stale-pointer reads.

### Invariants

- Pointer remap publish is atomic from reader perspective.
- No segment deletion before unreachable proof from a consistent snapshot.
- No correctness regressions across durable vs relaxed durability profiles.

### WAL Mode Parity (Required)

- All value-log behavior changes must preserve correctness under both:
  - WAL on (`DurabilityDurable`, `DurabilityWALOnRelaxed`)
  - WAL off (`DurabilityWALOffRelaxed`)
- WAL on: large/huge values must avoid inline-WAL payload overflow paths and use
  RID/pointer publication semantics.
- WAL off: large/huge values must use the same value-log object layout and
  reopen behavior without relying on WAL replay.
- Maintenance parity:
  - GC and rewrite must account for nested value-log references identically in
    WAL on/off modes.
- Validation parity:
  - Add reopen/recovery and maintenance tests for both WAL on and WAL off
    profiles for any new large-value object format.

## 2. Observability First

Add counters and reports before behavior changes:

- Per-generation bytes: live/stale/total.
- Per-generation segment count and size histograms.
- Rewrite bytes in/out and promotion counts (`hot->warm`, `warm->cold`).
- GC candidates/deletions by generation (segments and bytes).
- Pointer remap successes/failures and publish retries.

Bench-facing additions (`unified-bench` + `treemap stats`):

- Churn efficiency summary.
- Reclaim efficiency summary.
- Rewrite budget utilization.

## 3. Generation-Aware Segment Allocator

- Keep append-only semantics.
- Place new and updated records in `hot` segments.
- Size classes:
  - `hot`: smaller segments for quick turnover/reclaim.
  - `warm/cold`: larger segments for density and scan locality.
- No in-segment hole reuse.

## 4. Incremental Rewrite Scheduler

- Continuous small-budget rewrite loop (bytes/sec and/or records/sec).
- Trigger conditions:
  - stale/live ratio threshold,
  - total WAL/value-log bytes threshold,
  - observed churn threshold.
- Candidate ordering: highest waste first, with generation-aware priority.
- Promote surviving records by policy (`hot->warm->cold`).

## 5. Frequent Cheap GC, Less Frequent Heavy Rewrite

- Run lightweight GC often for fully-unreferenced segments.
- Prefer dead `hot` segments first where possible.
- Enforce active-segment protection and snapshot-pinned reachability.

## 6. Coordinate Rewrite And Index Maintenance

Policy coupling without hard coupling of operations:

- GC frequent.
- Incremental rewrite periodic/threshold-driven.
- Index vacuum only on index fragmentation/space thresholds or post-major rewrite windows.
- Do not force index vacuum for every GC pass.

## 7. Leaf-Block Density Tuning

- Default leaf-block block target to `8KiB`.
- Offer `16KiB` size-oriented profile option.
- Add latency guardrails to avoid oversized-block read cliffs.
- Keep codec behavior explicit and measurable.

## 8. Correctness And Recovery Hardening

Targeted tests:

- Crash between rewrite-copy and remap publish.
- Crash after publish before old-segment cleanup.
- Reopen/verify after mixed set/delete/update churn.
- No orphan pointers, no early deletes.
- Generation transition invariants and reachability accounting checks.

## 9. Rollout PR Sequence

1. PR1: metrics + config surface + no-op scheduler scaffolding.
2. PR2: generation-aware allocator + segment size classes.
3. PR3: incremental rewrite engine + atomic remap publish safety.
4. PR4: frequent GC policy + candidate heuristics.
5. PR5: coordinated maintenance policy (GC/rewrite/vacuum thresholds).
6. PR6: perf tuning (8KiB/16KiB) + docs + operator runbook.

## 10. Merge Gates

- CI green across Linux/macOS/Windows.
- No correctness regressions.
- Bench evidence includes:
  - reduced steady-state wal bytes under churn,
  - bounded rewrite amplification,
  - no major throughput/latency regressions,
  - predictable runtime versus periodic full rewrite workflow.

## Implementation Notes

- Keep all changes behind explicit defaults and feature gates until parity tests are complete.
