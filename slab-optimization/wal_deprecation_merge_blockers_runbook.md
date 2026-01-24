# Merge-Blocker Runbook: WAL Deprecation (PR #176)

This runbook is for finishing the **merge-blocking work** required to land PR #176 (“remove legacy slab/WAL-v1 paths; WAL on/off only”) without regressions in maintenance and long-lived disk behavior.

## Scope (hard)

This runbook covers **only** the blockers:
- #178 Restore swap-based index vacuum semantics (online/offline).
- #177 Add value-log GC CLI (`treemap vlog-gc`) for deleting fully-unreferenced segments.
- #179 Add value-log rewrite compaction (`treemap vlog-rewrite`) to reclaim **mixed** live/dead segments (merge-blocking).

Explicitly out of scope:
- Background value-log recompression on rotation (#180). Track only; do not implement here.

## Operating Rules

- Keep PRs small and reviewable: one blocker per PR, stacked on top of PR #176’s branch.
- No backwards compatibility required (legacy “mode1” stores do not need to open post-merge).
- Do not change unrelated defaults in this runbook unless required for correctness.
- For any GitHub text output, use `--body-file` (avoid literal `\\n` in PR/issue bodies).

## Repo / Branching Model

This runbook assumes PR #176 is based on branch:
- `refactor/wal-on-off-only`

Create stacked branches/PRs (names are suggestions; keep the stack order):
1) `fix/vacuum-swap` (closes #178)
2) `feat/vlog-gc` (closes #177) — base on `fix/vacuum-swap`
3) `feat/vlog-rewrite` (closes #179) — base on `feat/vlog-gc`

Each PR should target the previous branch in the stack (not `main` directly).

## Step 0 — Resume / Detect Existing Work

Before coding:
1) `gh pr list --state open` and confirm whether any PRs already exist for #177/#178/#179.
2) If a PR already exists for a blocker:
   - check it out,
   - run the validation commands listed for that blocker,
   - and either approve/merge it or supersede it with a new PR (do not half-duplicate).

## Step 1 — Blocker #178: Restore Swap-Based Vacuum

### Goal
Restore **swap-based** vacuum semantics:
- Online vacuum: rebuild into `index.db.new` while writers continue, then short cutover pause to swap.
- Offline vacuum: rebuild into `index.db.new` and swap, reclaiming disk space.

This must be **index-only** and must not require slabs.

### Code Checklist

1) Backend implementations exist and are used:
   - `TreeDB/db/vacuum_online.go`
   - `TreeDB/db/vacuum_offline.go`
   - shared swap helpers (if needed) in `TreeDB/db/index_swap.go` or similar.

2) Public API is correctly wired:
   - `TreeDB/public.go`:
     - `(*DB).VacuumIndexOnline(ctx)` must call backend online vacuum (not alias `CompactIndex`).
     - `VacuumIndexOffline(opts)` must perform swap-based offline vacuum (not “open + in-place CompactIndex”).

3) CLI semantics are correct (index-only maintenance):
   - `TreeDB/cmd/treemap/main.go`:
     - `compact`: in-place rebuild (`db.CompactIndex()`), requires `-rw`.
     - `vacuum`: swap-based shrink (call `treedb.VacuumIndexOffline(...)`), requires `-rw`.

### Tests (deterministic; no timing flakes)

- Offline vacuum shrinks `index.db` after churn:
  - create DB
  - churn writes so `index.db` grows
  - run offline vacuum
  - assert `index.db` size decreases (or at least meaningfully changes downward).

- Online vacuum bounded stall:
  - skip on Windows
  - run continuous writer goroutine while vacuum runs
  - assert writer makes progress during build (cutover may pause briefly).

### Validate
```bash
GOWORK=off go test ./TreeDB/... -count=1
```

### PR
- Create description file `.pr/PR_vacuum_swap.md`
- Open PR via:
```bash
gh pr create --title "treedb: restore swap-based vacuum semantics" --body-file .pr/PR_vacuum_swap.md --head fix/vacuum-swap --base refactor/wal-on-off-only
gh pr checks --watch
```

## Step 2 — Blocker #177: Value-Log GC CLI (`vlog-gc`)

### Goal
Provide a safe CLI to delete **fully unreferenced** value-log segments.

This is not “rewrite compaction”. It only deletes segments that have *zero* live pointers.

### Behavior Requirements
- Scan both:
  - user tree, and
  - system tree
  for `page.ValuePtr` references.
- Compute referenced `FileID`s.
- Delete segments that are:
  - not referenced,
  - not the current/open writer segment,
  - not pinned by snapshots (use existing valuelog refcounts/zombie handling).
- Provide:
  - `-dry-run`
  - and summary output (segments total/referenced/deleted, bytes reclaimed).

### Implementation Notes
- Prefer implementing as a backend entrypoint so cached mode can `Drain()`/`Checkpoint()` first.
- File location: value-log segments live under `<dir>/wal/value-l*` (lane-encoded ids).

### Tests
- Create DB with forced pointers + multiple value-log segments.
- Delete keys so at least one whole segment becomes unreferenced.
- Run GC.
- Assert:
  - segment file removed
  - remaining keys still readable
  - Windows: no open handles prevent TempDir cleanup.

### Validate
```bash
GOWORK=off go test ./TreeDB/... -count=1
```

### PR
- Create description file `.pr/PR_vlog_gc.md`
- Open PR stacked on the previous:
```bash
gh pr create --title "treemap: add vlog-gc (delete unreferenced value-log segments)" --body-file .pr/PR_vlog_gc.md --head feat/vlog-gc --base fix/vacuum-swap
gh pr checks --watch
```

## Step 3 — Blocker #179: Value-Log Rewrite Compaction (`vlog-rewrite`)

### Goal (merge-blocking)
Add rewrite compaction so we can reclaim disk space when segments contain **mixed** live+dead values.

### Safety Model (v1: offline)
To keep v1 reviewable and safe:
- Require exclusive access (DB opened with the open lock; no concurrent writers).
- Require a “clean” durable boundary for cached mode before rewriting:
  - caller should `Checkpoint()`/`Drain()` before rewrite, or `vlog-rewrite` should enforce it.
- Crash safety:
  - old pointers must remain valid until the new index is swapped in.

### Implementation Path (conservative)
- Rebuild+swap the index (reuse the swap infrastructure from #178):
  1) Enumerate all key/value entries in user tree and system tree.
  2) For each value pointer:
     - read the value bytes from the existing value-log set,
     - append to a new value-log segment set (use a distinct lane to avoid id collisions),
     - write the new pointer into the rebuilt index.
  3) Swap `index.db.new` into place atomically (using `.ready` marker).
  4) Mark old value-log segments zombie and delete when unpinned (or leave for `vlog-gc` to delete).

### Required CLI
- `treemap vlog-rewrite -rw`:
  - prints: segments before/after, bytes before/after, bytes reclaimed, wall time.
  - supports `-dry-run` (optional but strongly recommended).

### Tests (must prove reclaim + correctness)
- Integration test with churn:
  - force frequent segment rotation (small max segment size) and forced pointers.
  - overwrite keys multiple times so segments become mixed live/dead.
  - record `du` (or stat-summed bytes) of value-log before.
  - run `vlog-rewrite`.
  - assert:
    - all keys readable and correct
    - old segments removed/zombied
    - bytes decreased materially (e.g., >50% reclaim in test scenario).

### Validate
```bash
GOWORK=off go test ./TreeDB/... -count=1
```

### PR
- Create description file `.pr/PR_vlog_rewrite.md`
- Open PR stacked on the previous:
```bash
gh pr create --title "treemap: add vlog-rewrite (value-log rewrite compaction)" --body-file .pr/PR_vlog_rewrite.md --head feat/vlog-rewrite --base feat/vlog-gc
gh pr checks --watch
```

## Step 4 — Close The Loop

- Update issues #177/#178/#179 with:
  - reproduction commands
  - PR links
  - any remaining limitations (e.g., Windows online vacuum unsupported)
- Comment on PR #176 summarizing:
  - which blocker PRs landed
  - and that value-log rewrite compaction now exists (no slab feature regression).

