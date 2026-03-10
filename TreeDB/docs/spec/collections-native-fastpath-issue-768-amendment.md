# Issue `#768` Amendment Draft

Status: draft issue-sync text, non-normative.

Use this draft to update GitHub issue `#768` so the remote issue matches the
current main-based rewrite plan.

## Required issue edits

1. Keep the issue title at:
   - `Collections Native Fast-Path Rewrite from main: benchmark-gated 82-step execution plan`
2. Add a top-of-issue status block:
   - execution base is `pr/native-fastpath-prep-main-sync`
   - oracle branch is external-only and not a merge target
   - all collection baselines must be recaptured from:
     - the oracle worktree, and
     - the main-based execution branch
3. Replace any lingering `72-step` wording with `82-step`.
4. Clarify that `R0` does **not** assume an in-tree legacy/native selector on
   `main`.
5. Clarify that if the collection benchmark harness is absent on `main`, `R0`
   must add it before native collection baseline capture.
6. Rename the first rewrite branch in the issue from
   `pr/native-fastpath-r0-switch-baseline` to
   `pr/native-fastpath-r0-oracle-baseline`.
7. State that every relevant phase is benchmark-gated against:
   - raw TreeDB `unified-bench` anchors,
   - focused collection benchmarks,
   - deferred-work `flushdrain` results when work can move across checkpoint.

## Suggested status block

```md
## Execution base status

- Main-based prep branch: `pr/native-fastpath-prep-main-sync`
- Oracle comparison model: external oracle branch/worktree only
- Oracle branch is not a merge target for the rewrite stack
- All baseline artifacts must be recaptured on the main-based execution branch
  before implementation starts
- If the native collection benchmark harness is not present on the main-based
  branch, `R0` must land that harness before native collection baseline capture
```

## Suggested `R0` wording replacement

Replace the current selector-oriented `R0` wording with:

```md
### R0: oracle freeze + native harness bring-up

- freeze the oracle branch name and current main execution base
- freeze the exact `origin/main` baseline SHA
- freeze the canonical benchmark command set
- introduce the native-path collection benchmark/report harness on the
  main-based branch if it is not already present
- plumb benchmark labels so every run says `oracle` or `native-fastpath` when
  comparison output is emitted
- document the baseline capture block that must exist before `R1`
```

## Suggested checklist clarifications

Add explicit checklist text that:

- native collection baseline rows are `N/A before R0 harness bring-up` if the
  harness does not yet exist on `main`
- `R0` exits only after the oracle branch, main execution base, exact main SHA,
  and frozen benchmark command set are recorded
- every benchmark artifact records:
  - worktree path
  - branch name
  - HEAD SHA
  - durability profile
  - whether the result is mixed-under-debt or settled-after-checkpoint
