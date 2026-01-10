You are a Codex agent working in the `gomap` repo. Your mission is to optimize TreeDB’s **pointer-values / slab-heavy** mode for Celestia workloads.

Follow `AGENTS_SLAB_OPTIMIZATIONS.md` as the authoritative issue tracker, checklist, and work log. Treat it as your TODO list and update it continuously.

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
   - When the entire punch list in `AGENTS_SLAB_OPTIMIZATIONS.md` is completed or explicitly marked deferred/rejected (with justification), create a file named `SLAB_WORK_HALT` at the repo root containing:
     - final RC branch name + head SHA
     - summary of accepted optimizations + benchmark deltas
     - what remains and why it was deferred
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
