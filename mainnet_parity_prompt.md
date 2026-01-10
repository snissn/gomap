You are a Codex agent working in the `gomap` repo. Your mission is to optimize TreeDB’s **pointer-values / slab-heavy** mode for Celestia workloads.

Follow `AGENTS_SLAB_OPTIMIZATIONS.md` as the authoritative issue tracker, checklist, and work log. Treat it as your TODO list and update it continuously.

## Compatibility stance (Alpha)
- Backward compatibility is NOT required.
- It is acceptable to bump slab/index formats and invalidate old on-disk data.
- “Needs a migration plan” is not a valid blocker reason; treat format bumps as acceptable.

## Linux server (for true workload + OS-specific tuning)
- You may read `celestia_testing_info.md` for SSH details to a Linux server used for Celestia runs and profiling.
- Prefer validating OS-specific changes (e.g., huge pages, madvise) on that Linux server via `run_celestia_trace.sh`.

## Hard rules
1) **Branch-per-feature, stacked workflow**
   - Use the current RC base branch (default: `treedb-tracing-capture` unless the repo indicates otherwise).
   - Create/maintain a moving RC head branch (recommended: `slab-opt-rc`) that represents “accepted + reverted attempts”.
   - For each high-level item in `AGENTS_SLAB_OPTIMIZATIONS.md`, create a new branch `slab-opt-NN-<shortname>` off the current `slab-opt-rc` head.
   - If the feature is accepted: fast-forward merge into `slab-opt-rc`.
   - If rejected: **do not delete the work**. Keep the implementation commits, then `git revert` them so the code path returns to baseline. Merge that into `slab-opt-rc` so the attempt is recorded but inactive. The next feature branch must be created off this updated RC head.

2) **Benchmark before/after**
   - For every change, run a local pointer-values replay benchmark (timeline) and capture a CPU profile at least once per feature.
   - For any promising change, confirm via server `run_celestia_trace.sh` (true workload) if you have access.
   - Record results (numbers + paths) in `AGENTS_SLAB_OPTIMIZATIONS.md` and optionally deeper notes in `invalid_value_debug.md`.

3) **Keep a clean audit trail**
   - Commit frequently with small, atomic commits.
   - Update `AGENTS_SLAB_OPTIMIZATIONS.md` work log on every major action (branch created, bench run, accept/reject decision).

4) **Iterate toward convergence (MANDATORY)**
   - Optimize via tight loops: baseline → 1 small change → measure → keep/adjust → repeat.
   - Prefer **idempotent iterations**: each run should be safe to re-run and should make forward progress (code, measurement, or clarity).
   - Land enabling work when it helps convergence, even if perf-neutral:
     - instrumentation, counters, trace/pprof hooks, benchmarks, small refactors, and guarded feature flags.
   - Only “reject for now” after at least one iteration to salvage the approach (tune a threshold, gate behind opt-in, reduce scope).
   - Keep the tree green: avoid leaving broken builds/tests between iterations.

5) **No deferrals (MANDATORY)**
   - Do not use “deferred” as an outcome. This repo is alpha; format bumps are allowed; a “migration plan” is never a blocker.
   - If an item is too large, start with the item’s **MVA** (minimum viable attempt) to get real measurements.
     - It is valid (and preferred) to land **enabling sub-steps** into `slab-opt-rc` (instrumentation/bench/flags/scaffolding) while keeping the item itself “planned/in_progress”.
     - Only “reject for now” when the approach is clearly a dead end after at least one salvage iteration; keep the attempt commits and revert the behavior change so the RC stays healthy.
   - If you hit a true external blocker (e.g. you cannot access the Linux server at all), still create a branch, do the smallest safe spike you can locally, record the blocker, then reject-for-now via a revert and move on.

6) **Stop condition**
   - Only stop (create `SLAB_WORK_HALT`) when every punch-list item is either:
     - **accepted**, or
     - **rejected for now** with a recorded attempt (commits) and a revert merged into `slab-opt-rc`.
   - `SLAB_WORK_HALT` must contain:
     - final RC branch name + head SHA
     - summary of accepted optimizations + benchmark deltas
     - what remains and why it was rejected
   - Once `SLAB_WORK_HALT` exists, do not continue work.

## Execution loop (what to do now)
1) Open `AGENTS_SLAB_OPTIMIZATIONS.md` and identify the first unchecked item.
2) Ensure `slab-opt-rc` exists and is up to date:
   - If it doesn’t exist, create it from the current RC base branch.
3) Create the feature branch for the next item and implement it.
4) Run the baseline + after benchmarks:
   - Local: use the “trusted pointer-values replay (timeline)” command in `AGENTS_SLAB_OPTIMIZATIONS.md`.
   - Capture `-cpuprofile` at least once.
5) Decide accept vs reject:
   - Accept: merge into `slab-opt-rc`.
   - Reject: revert, re-benchmark to confirm baseline restored, then merge revert history into `slab-opt-rc`.
6) Update `AGENTS_SLAB_OPTIMIZATIONS.md`:
   - Mark status, write results, list branch names/SHAs, and next step.

Proceed until the list is done, then create `SLAB_WORK_HALT`.
