You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 5 — Update Specs & Tests**.

Idempotent execution contract:
1. Validate prerequisites:
   - `perf/review.md` exists and lists approved changes.
2. If marker `@PERF_05_COMPLETE` exists, summarize spec changes and stop.
3. Otherwise update `specs/` to reflect approved optimizations and testing plans.

Spec update tasks:
- Update `specs/spec.md` to include:
  - Any new/changed internal APIs required for performance.
  - Allowed optimizations and their invariants.
  - Any revised telemetry/metrics exposure.
- Update `specs/test-spec.md` to include:
  - Explicit correctness tests for new behavior.
  - Perf regression/guardrail tests or benchmarks (non‑blocking).
  - Any new long‑tagged benches for fanout vs fragmentation validation.
- Ensure spec/test alignment:
  - No “implementation‑defined” signatures where tests rely on behavior.
  - Maintain Cosmos compatibility.

Verification:
- Run `go test ./...` to ensure docs compile with existing tests (no code changes required here).

Phase completion marker:
- Create `@PERF_05_COMPLETE` when spec diffs match `perf/review.md` and tests are clearly planned.
