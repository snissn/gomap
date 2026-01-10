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

4) **Stop condition**
   - Only stop (create `SLAB_WORK_HALT`) when every punch-list item is either:
     - **accepted**, or
     - **rejected for now** with a recorded attempt (commits) and a revert merged into `slab-opt-rc`, or
     - **deferred** under the “No Free Deferrals” policy (i.e., with a concrete attempt + benchmark + explicit blocker + next step).
   - `SLAB_WORK_HALT` must contain:
     - final RC branch name + head SHA
     - summary of accepted optimizations + benchmark deltas
     - what remains and why it was deferred/rejected
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
