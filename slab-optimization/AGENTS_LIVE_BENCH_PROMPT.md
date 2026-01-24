# Prompt: Execute `AGENTS_LIVE_BENCH.md` (One Long Autonomous Session)

You are a Codex-style coding agent working in the repo:

- `/Users/michaelseiler/dev/snissn/gomap`

Your task is to **execute the runbook** in:

- `slab-optimization/AGENTS_LIVE_BENCH.md`

…and carry it through to completion in **one long autonomous work session**: implement missing code, run tests/validation, run the runbook’s example commands (when feasible), and open PR(s) for review.


base PRs on sprint/autotuner_7 and have as many PRs as appropriate and use branches named sprint/live_bench_1 and increment _1

use the github cli for opening PRs and ensure that the github CLI confirms the ci passes for each PR

## 0) First: resume / detect if already done

This work may be resumed mid-stream.

1. Open and read `slab-optimization/AGENTS_LIVE_BENCH.md`.
2. Treat that file as the **work log** for this project:
   - If it already contains a work-log section, continue from the most recent entry.
   - If it does not, append a `## Work Log (append-only)` section at the end and start logging there.
3. If the runbook’s implementation tasks already appear completed, **verify and halt**:
   - Confirm the new CLI flags exist and `go run ./TreeDB/cmd/vlog_dict_realdata -h` shows `-bench-kv` and related flags.
   - Run targeted tests (at minimum `go test ./TreeDB/... -count=1`).
   - Run one representative example command from the runbook to confirm it produces `steady_raw_MBps` output and prints `treedb.write_path.*` stats keys.
   - Log verification results in `slab-optimization/AGENTS_LIVE_BENCH.md` and stop.

## 1) Scope + priorities

Follow the runbook exactly. Key constraints:

- Use the **public TreeDB KV store** API for throughput measurement (open DB via `treedb.Open`, write via `Batch.Set` + `Batch.Write`).
- Focus on **relaxed/no-fsync** behavior for now (do not prioritize `WriteSync`/fsync).
- Compare write-path modes (wal_on/wal_off) and **compression on/off**, as spelled out in the runbook.
- The benchmark must **force values through the value log** (dataset average values may otherwise be inlined).

Do not expand scope beyond what is needed to:
- implement the KV throughput bench,
- make it safe/reliable,
- make it easy to run and interpret,
- and keep it mergeable.

## 2) Implementation deliverable (minimum)

Implement the runbook’s “Implementation task: extend `cmd/vlog_dict_realdata` with KV throughput bench”.

Concretely:
- Edit: `TreeDB/cmd/vlog_dict_realdata/main.go`
- Add a new mode (e.g. `-bench-kv`) that:
  - opens a fresh DB with options matching the selected mode,
  - writes a warmup/training phase (time it),
  - then writes a steady-state phase (headline metric),
  - prints a single “headline” line including `steady_raw_MBps`,
  - prints/validates `db.Stats()` write-path keys for the selected mode,
  - prints compression stats (`attempted_frac`, `kept_frac`, `current_k`, `dict_id`) when available.

Keep it deterministic where possible, but this is a *live bench*; tests should validate correctness and wiring, not absolute performance.

## 3) Engineering process requirements

Work autonomously and continuously; do not stop mid-way unless blocked.

Before edits:
- Use `rg -n` to locate anchors/symbols.
- Keep patches small and focused.

Commits / PRs:
- Create new branch(es) for the work (don’t commit directly to the current branch).
- If multiple PRs are needed, open each successive PR **to the previous sprint branch** (stacked PRs).
- PR base:
  - Prefer `fix/treedb-index-columnar-leaves` if it exists and is the sprint’s integration base.
  - Otherwise, use `main`.
- Use GitHub CLI: `gh pr create ...`.
- Include in the PR body:
  - what was implemented,
  - how to run it,
  - at least one representative log snippet from running the tool on a real JSONL input (or a small synthetic dataset if needed),
  - and what the output means.

Validation:
- Run `go test ./... -count=1` if feasible; otherwise run the narrowest relevant packages and document why broader tests were skipped.
- Ensure the new flags are discoverable in `-h` output and errors are fail-closed (no panics / OOM from malformed inputs).

## 4) Work logging (required)

After every meaningful action, append a dated entry to `slab-optimization/AGENTS_LIVE_BENCH.md` under `## Work Log (append-only)`:
- timestamp (local time)
- what changed (files + summary)
- commands run (tests/benches) and outcomes
- PR links + branch names
- any follow-ups or known limitations

## 5) Definition of done

You are done when:
- The KV throughput bench exists and matches the runbook’s matrix needs.
- Compression off/on behavior is correctly forced and measurable.
- At least one end-to-end run produces the expected headline metrics and stats.
- Tests pass (or are explicitly scoped with justification).
- PR(s) are opened and work log is updated.
