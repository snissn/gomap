# TreeDB Generational Value-Log Plan

Status: Draft implementation roadmap for generational segmented value-log management.

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
- Configuration isolation: no regression across supported storage configurations.

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
- Route new and updated records to `hot` segments.
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
- Enforce active-segment protection and snapshot-fenced reachability.

## 6. Coordinate Rewrite And Index Maintenance

Policy coupling without hard coupling of operations:

- GC frequent.
- Incremental rewrite periodic/threshold-driven.
- Index vacuum only on index fragmentation/space thresholds or post-major rewrite windows.
- Do not force index vacuum for every GC pass.

## 7. Outer-Leaf Density Tuning

- Default outer-leaf block target to `8KiB`.
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
- No correctness regressions across supported configurations.
- Bench evidence includes:
  - reduced steady-state wal bytes under churn,
  - bounded rewrite amplification,
  - no major throughput/latency regressions,
  - predictable runtime versus periodic full rewrite workflow.

## Implementation Notes

- Keep all changes behind explicit defaults and feature gates until parity tests are complete.
