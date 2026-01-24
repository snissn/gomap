# Prompt: Execute `slab_optimization_merge_runbook.md` (Merge Gate, Post–Live Bench)

You are a Codex-style coding agent working in the repo:

- `/Users/michaelseiler/dev/snissn/gomap`

Your task is to **execute the merge-gate runbook**:

- `slab-optimization/slab_optimization_merge_runbook.md`

This prompt assumes the live bench work from:
- `slab-optimization/AGENTS_LIVE_BENCH.md`
- `slab-optimization/AGENTS_LIVE_BENCH_PROMPT.md`

…has already been completed, and that the KV live bench exists via:
- `go run ./TreeDB/cmd/vlog_dict_realdata ... -bench-kv ...`

Run this as **one long autonomous session**: verify prerequisites, run the required validations/benches, and open PR(s) or follow-up issues as needed to make the work mergeable into `main` (or the sprint integration base if your repo uses one).

## 0) Resume / verify current state first

This work may be resumed mid-stream.

1. Read `slab-optimization/slab_optimization_merge_runbook.md` end-to-end.
2. Check whether a prior agent already completed the runbook:
   - Look for existing PRs/branches already addressing the runbook sections.
   - Look for `out/` artifacts already produced (if your repo keeps them).
3. If the required tools/flags are missing (live bench not implemented), stop and run `slab-optimization/AGENTS_LIVE_BENCH_PROMPT.md` first.

## 1) Hard scope boundaries

- Follow `slab-optimization/slab_optimization_merge_runbook.md` as the source of truth.
- Do not expand scope beyond what is needed to:
  - validate correctness,
  - validate deterministic autotune marks (`TreeDB/cmd/unified_bench -suite vlog_autotune -validate`),
  - validate live wall-clock throughput (`TreeDB/cmd/vlog_dict_realdata -bench-kv`),
  - and clean up docs/flags so the work is reviewable and merge-safe.
- Current priority is **relaxed/no-fsync** performance. Do not prioritize `WriteSync` benchmarking unless the runbook explicitly requires it.

## 2) Execution order (recommended)

Execute in this order unless the runbook says otherwise:

1. **Sanity**: confirm the TreeDB-local deterministic autotune suite runs:
   - `go run ./TreeDB/cmd/unified_bench -suite vlog_autotune -validate`
2. **Live bench gate**: run the mode matrix and capture logs:
   - `go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv ... | tee out/...`
3. **Repo-root unified_bench**: run the synthetic mode comparisons and scan regressions:
   - `go run ./cmd/unified_bench ... > out/...` (unified_bench writes results to stdout)
4. **Tests**: run `go test` (and `-race` where the runbook requires).
5. **Docs + defaults**: apply the runbook’s doc/deprecation/defaults guidance.

## 3) Required artifacts (capture + attach to PRs)

For every performance-meaningful change (or for merge gate verification), capture outputs:

- Deterministic marks:
  - `go run ./TreeDB/cmd/unified_bench -suite vlog_autotune -validate -json > out/vlog_autotune.json`
- Live KV throughput logs:
  - `out/live_mode*_*.log` (via `tee`)
- Synthetic mode comparison markdown (capture via stdout redirection):
  - `out/mode_compare_large_values.md`
  - `out/scan_compare.md`

Do **not** commit large `out/` artifacts unless repo policy explicitly says to. Attach them to PRs or store them as CI artifacts.

## 4) PR + issue workflow

- Use small, reviewable PRs that map cleanly to the runbook sections.
- If multiple PRs are needed, stack them (each PR targets the previous sprint branch).
- Prefer using `gh`:
  - `gh pr create ...`
  - `gh pr checks ... --watch`
- If you discover gaps in CI (missing bench/test coverage), open a GitHub issue with:
  - exact commands to reproduce,
  - suggested fixes,
  - and acceptance criteria so another agent can implement and verify.

## 5) If everything already passes

If all runbook gates already pass:

1. Re-run the deterministic suite and one live bench config to confirm nothing regressed.
2. Summarize results (what passed, what commands, where logs are).
3. Stop (no further changes).
