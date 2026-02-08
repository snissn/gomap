# TreeDB #406 Revised Memo: GC/Rewrite Co-Tenancy and Memtable Sequencing

Date: 2026-02-08
Author: Codex (performance review follow-up)
Related issues: #406, #393, #394, #395, #396, #398, #399, #401, #402

## Canonical Status
- Superseded by issue body: `https://github.com/snissn/gomap/issues/406`.
- The issue description is the single source of truth for gates, scope, and promotion decisions.
- This memo is a historical/reference mirror only.

## Purpose
This memo proposes targeted revisions to issue #406 so that:
1. value-log GC does not require global write-path convoys,
2. maintenance contention with B-tree vacuum is explicitly bounded,
3. memtable unlock risk from late Wave 3 dependency is reduced.

## Current Constraint Summary
The current design can still stall writes during GC because cached-mode `ValueLogGC` forces a checkpoint first.

Relevant code:
- `TreeDB/vlog_gc.go` calls `db.cached.Checkpoint()` before backend GC.
- `TreeDB/caching/db.go` checkpoint path takes global barriers (`flushMu`, then `writeMu`) and blocks writers in the checkpoint critical window.

This means value-log append/flush/sync ownership improvements alone do not eliminate maintenance-time write interference.

## Proposed Revision A: Online Value-Log GC Fence (No Mandatory Checkpoint)

### A1. Replace GC precondition
Change cached-mode GC precondition from "global checkpoint first" to "lane durability fence + protected-set snapshot".

Behavior:
1. Request a fence token from WAL/value-log lane actors.
2. Wait until `durable_seq >= fence_seq` for all lanes.
3. Snapshot protected segment sets.
4. Run pointer reachability scan from backend snapshot state.
5. Mark eligible segments zombie.
6. Perform deferred physical deletion when refcount reaches zero.

### A2. Protected segment sets
Compute a conservative keep-set without global stop-the-world:
1. Keep all lane active heads.
2. Keep all cached retained paths tracked by queue/retention metadata.
3. Keep all pointer-referenced segments from user/system trees.

Conservative keep is acceptable. It can reclaim less in one pass but must never over-delete.

### A3. Manager lifecycle stays compatible
Use existing manager semantics:
1. `MarkZombie(id)` for logical retirement.
2. `Release(set)` for refcount-based delayed removal.

No change to pointer durability model is required.

### A4. Safety invariants and rollout guard
Online GC must ship with explicit safety invariants and a rollback switch:
1. **Protected-set soundness invariant:** no segment may be deleted if reachable from:
   - current backend snapshot trees,
   - retained path metadata,
   - lane active heads,
   - in-flight unflushed memtable/WAL references tracked by retention metadata.
2. **Rotation/churn stress proof:** add a blocking stress gate with concurrent segment rotation + heavy overwrite churn + GC, then reopen/verify all keys.
3. **Strict fallback switch:** define a concrete mode control (for example `Options.ValueLogGCMode = strict|online`) where:
   - `strict` = checkpoint-first GC (current behavior),
   - `online` = fence-based GC,
   - runtime invariant violation in `online` mode fails closed to `strict` and emits an explicit error/metric.
4. **Rollout requirement:** default may switch to online fence GC only after 3 consecutive green runs of the safety gate.

## Proposed Revision B: Maintenance Coordination with B-tree Vacuum

### B1. Add maintenance coordinator policy
Introduce a single coordinator policy that distinguishes:
1. full-scan tasks (value-log GC pointer scan, vacuum build scan),
2. cutover tasks (short writer-pause windows).

Policy:
1. Run at most one full-scan task at a time.
2. Coalesce cutover windows where possible.
3. Enforce explicit CPU/IO budgets per maintenance task.

### B2. Co-scheduling rules
Rules:
1. Do not run full `ValueLogGC` scan concurrently with full index vacuum scan.
2. Permit overlap only for short metadata operations that do not hold write barriers.
3. Defer non-urgent GC when vacuum cutover is pending.
4. If backlog exceeds safety threshold, GC can preempt vacuum but only up to a bounded duty cycle.

### B3. Starvation and freshness bounds (required)
To avoid permanent deferral under sustained load, add hard bounds:
1. **Max deferral:** each full-scan task must begin within a bounded time window (for example 10 minutes) after becoming eligible.
2. **Freshness SLO:** GC staleness and vacuum staleness metrics must stay under configured thresholds (p99 + max).
3. **Forced service rule:** if a task exceeds max deferral, coordinator must reserve the next available full-scan token for that task.
4. **Budget overrun behavior:** if emergency preemption repeats, shed low-priority maintenance first, not writer throughput.

### B4. Rewrite interaction
`ValueLogRewriteOffline` remains offline and exclusive. No online co-tenancy claim should be made for rewrite.
Rewrite is explicitly out-of-scope for maintenance coordinator arbitration.

## Proposed Revision C: Reduce Memtable Late-Wave Risk

### C1. Add Wave 1.75 (new mandatory stage)
Before Wave 2 closeout, land a minimal memtable admission de-coupling slice:
1. Remove routine `waitForStop` from default per-write admission path.
2. Move routine slowdown/assist logic behind domain-local or shard-local counters where possible.
3. Keep only a global emergency brake for pathological backlog.
4. Add explicit backlog/memory ceilings:
   - queue backlog bytes hard cap,
   - queue depth hard cap,
   - write admission timeout/assist bound,
   - emergency brake behavior (`throttle -> assist -> block`) with deterministic order.
5. Add emergency-brake safety gate:
   - no unbounded queue growth,
   - RSS stays within configured ceiling,
   - no OOM under sustained overload stress.

Scope boundary for Wave 1.75 (explicit):
1. This stage is **admission de-coupling only**.
2. It does **not** include full domain routing, cross-domain publish fairness, or full memtable ownership model migration.
3. Those remain Wave 3 deliverables.

This keeps Wave 1.75 small and lowers schedule risk while delivering first-order memtable contention relief earlier.

### C2. Keep Wave 3 for full unlock
Wave 3 still delivers full domain-local ingress and publish-sequence fairness, but #406 should no longer depend on Wave 3 for first-order memtable contention relief.

## Proposed New Gates for #406

Add the following blocking gates:

1. **GC non-convoy gate**
`ValueLogGC` on cached mode under sustained writes must satisfy:
- throughput during GC >= 0.90x same workload without GC,
- writer stalled duty cycle attributable to GC <= 5%,
- p99 single-write stall from GC windows <= 10ms.

2. **No mandatory checkpoint gate for GC**
Cached `ValueLogGC` must not call global checkpoint in steady state.

3. **Maintenance co-tenancy gate**
Under concurrent maintenance demand:
- at most one full-scan maintenance task active,
- no overlapping full-scan GC and full-scan vacuum,
- combined maintenance-induced writer stall duty cycle <= 10%.
- max deferral and freshness SLO gates pass for both GC and vacuum.

4. **GC reclaim-effectiveness gate**
When no long-lived snapshots are pinned:
- bytes deleted / bytes eligible >= configured floor (for example 0.60) over rolling maintenance windows,
- if below floor for N consecutive windows, emit degraded-reclaim alert and force diagnostic mode.

5. **Wave 1.75 memtable gate**
Before Wave 2 completion:
- routine write admission does not require global stop-wait in normal cadence,
- cached write p99 under sustained mixed load improves >= 10% vs baseline,
- no ordering/visibility regressions,
- backlog/memory hard caps are enforced and pass overload stress.

6. **Online GC safety gate**
Before enabling online GC by default:
- protected-set soundness stress passes under rotation/churn,
- reopen verification passes after repeated GC cycles,
- strict fallback mode remains available and tested.

## Canonical Gate Commands (for these new gates)

Use the same hardware/protocol rules as #406.

Current runnable proxy commands (available now):

1. **Baseline write pressure (no explicit GC harness):**
`GOMAXPROCS=8 go run ./cmd/unified_bench -dbs treedb -profile durable -test random_write_parallel -write-workers 8 -keys 1500000 -valsize 1024 -progress=false -format markdown`

2. **Checkpoint-convoy proxy under write load:**
`GOMAXPROCS=8 go run ./cmd/unified_bench -dbs treedb -profile durable -test random_write_parallel -write-workers 8 -keys 1500000 -valsize 1024 -checkpoint-every-ops 50000 -progress=false -format markdown`

3. **Existing maintenance sweep proxy:**
`go run ./cmd/unified_bench -suite maintenance_budget -dbs treedb -profile durable -keys 1000000 -valsize 1024 -batchsize 1000 -progress=false -format markdown`

4. **Wave 1.75 overload safety (proxy run):**
`GOMAXPROCS=8 go run ./cmd/unified_bench -dbs treedb -profile durable -test random_write_parallel -write-workers 32 -keys 2000000 -valsize 1024 -progress=false -format markdown`

Required harnesses/tests to add (not available yet):

5. **GC non-convoy under active online GC (new harness):**
`GOMAXPROCS=8 go run ./cmd/unified_bench -suite maintenance_gc -dbs treedb -profile durable -keys 1500000 -valsize 1024 -batchsize 1000 -gc-mode online -gc-period 30s -progress=false -format markdown`

6. **Maintenance co-tenancy arbitration (new harness):**
`GOMAXPROCS=8 go run ./cmd/unified_bench -suite maintenance_coordination -dbs treedb -profile durable -keys 1500000 -valsize 1024 -batchsize 1000 -maintenance gc,vacuum -progress=false -format markdown`

7. **Online GC safety stress test (new test):**
`go test ./TreeDB -run '^TestValueLogGCOnline_ProtectedSetSafety$' -count=5`

## Gate Freshness and Value-Size Coverage Addendum (2026-02-08)

The following should be treated as blocking execution hygiene for #406:

1. **Gate freshness requirement**
- Stage evidence is stale after 48 hours, or immediately stale after any merge/rebase from `main`, or after performance-relevant changes under `TreeDB/*` or `cmd/unified_bench/*`.
- No stage promotion may use stale artifacts.
- Before stage promotion, rerun that stage's checklist and sentinel pack on the current candidate SHA.

2. **Branch-vs-main net throughput matrix (mandatory)**
- For each promoted candidate, run the same matrix on:
  - candidate branch SHA,
  - pinned `main` comparison SHA used for the decision.
- Report both `ops/sec` and wall time in a single table with deltas (`candidate/main`).
- Matrix dimensions:
  - `GOMAXPROCS` and `write-workers`: `1,2,4,8,16` (or max-equivalent),
  - values: `128`, `256`, `1024`, `1025`,
  - test: `random_write_parallel` with durable profile.

3. **Scaling interpretation rule**
- Do not require linear scaling.
- Require monotonic scaling with explicit efficiency reporting:
  - throughput gain from `1->8`,
  - incremental gain from `8->16`,
  - wall-time improvement trend.
- Any flat or negative step in worker ladder must be called out as a blocker/risk note.

4. **Value-size acceptance expansion**
- Add blocking gate coverage for `valsize=128`, `256`, and `1025` in addition to current `1024`.
- Keep `1025` as the boundary-condition sentinel (off-1 page/packing pressure case).
- Stage closure must include at least one refreshed matrix table containing all required value sizes.

## Suggested #406 Text Deltas

Apply these scope edits in issue #406:

1. In Wave 1.5, add "online GC fence precondition" and "maintenance coordinator policy" tasks.
2. Insert Wave 1.75 (mandatory) between Wave 1.5 and Wave 2 for minimal memtable admission de-coupling.
3. Add GC non-convoy and maintenance co-tenancy acceptance gates.
4. Add explicit online-GC safety gate + strict fallback switch during rollout.
5. Add starvation/freshness bounds for maintenance coordinator (max deferral + forced service).
6. Add backlog/memory hard-cap and overload-safety gates for Wave 1.75.
7. Add reclaim-effectiveness gate for online GC (with pinned-snapshot exceptions).
8. Clarify rewrite status as offline-exclusive and out of online unlock claims and coordinator scope.
9. Keep Wave 3 as full memtable unlock and fairness completion, not first relief point.
10. Add a blocking "freshness" rule: no stale artifacts older than 48h for stage promotion.
11. Add a mandatory branch-vs-main throughput + wall-time matrix across workers `1,2,4,8,16` and value sizes `128,256,1024,1025`.
12. Add explicit scaling-report requirements (monotonicity + 1->8 and 8->16 efficiency deltas) instead of assuming linearity.

## Risks and Trade-offs

1. Conservative protected sets can delay space reclamation in heavy churn.
2. Coordinator complexity increases scheduler logic.
3. Early memtable de-coupling without full domain routing requires careful invariants to avoid ordering regressions.
4. Adding hard caps can surface overload earlier as explicit backpressure; this is preferable to hidden latency cliffs or OOM.

These are acceptable trade-offs because they reduce writer convoy risk without requiring immediate format changes.

## Implementation Sketch

1. Add lane fence API shared by WAL and value-log actors.
2. Add `ValueLogGCOnline` path using fence + protected sets.
3. Add protected-set soundness stress tests and reopen-verify checks under rotation/churn.
4. Keep existing `ValueLogGC` as fallback strict mode behind option flag during rollout.
5. Add maintenance coordinator with scan-token arbitration and max-deferral forced service rules.
6. Add Wave 1.75 memtable admission de-coupling with backlog/memory hard caps and emergency-brake policy.
7. Add benchmark and lock-delay gates to CI in informational mode, then blocking.

## Compatibility Notes

1. No on-disk format change is required for this memo’s core revisions.
2. Pointer durability and snapshot safety semantics remain unchanged.
3. Offline rewrite behavior remains unchanged.
