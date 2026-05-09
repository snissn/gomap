# Minimal Implementation Slice and Scope-Control Review

## Role / persona

You are a technical lead responsible for landing the smallest correct
implementation without leaving traps for column-store, native-wire, or Raft
follow-up work.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- especially sections `11. Test Plan`, `12. Benchmark Plan`, `13. Roadmap and
  Gates`, `15. Open Questions`, and `16. Implementation Notes`
- `TreeDB/docs/spec/verification.md`
- `TreeDB/docs/spec/collections-write-domain.md`
- column-store specs/RFCs when present; this repository snapshot does not
  include a dedicated column-store RFC file
- `TreeDB/docs/spec/native-query-raft-roadmap.md`
- `TreeDB/collections/api.go`
- `TreeDB/db/ordered_root_publish.go`
- `TreeDB/db/system_root_publish.go`
- `TreeDB/internal/commitlog`
- current collection tests and WAL/recovery tests

## Task

Review the proposed roadmap and identify the minimal implementation slice that
preserves correctness while reducing scope. This is not a code feasibility
review. The goal is to separate must-have correctness from optional performance,
ergonomics, and future extensibility.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Which features are essential for PR1 correctness versus deferred optimization.
- Whether no-index row collections can be made durable-at-ack before indexed
  collections, or whether that creates incompatible semantics.
- Whether PR1 should use inline-only root deltas, side payloads, or an enforced
  size cap.
- Whether replay-side accumulation is required in the first slice or can be
  avoided by logging merged flush-unit transactions.
- Whether async indexed flush can be disabled or forced synchronous in the first
  durable implementation.
- Whether column-store gating can be stated as "blocked until X exact invariants
  pass" without implementing column files.
- Whether native-wire/Raft hooks should be stubbed, explicitly blocked, or
  ignored until later.
- Whether cleanup can be conservative in PR1 by retaining more WAL/side files
  until checkpoint.
- Whether benchmarks can be advisory in PR1 but gating before default
  enablement.
- Whether open questions must be resolved before coding or can be deferred
  safely.

## Focus questions

1. What is the smallest set of transaction fields that must be stable in PR1?
2. Can PR1 forbid large side-payload root deltas and still be useful?
3. Can PR1 serialize all collection WAL publication through one global publisher
   to avoid watermark complexity?
4. Which concurrency or async paths can be disabled without breaking existing
   tests?
5. Which cleanup can be conservative to avoid unsafe deletion?
6. Which APIs should keep old flush-boundary semantics until a later gate?
7. What exact milestone unblocks column-store format tests versus persistent
   writes?
8. What open questions are actually blockers?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Scope item that must be included for correctness
- Finding:
- Evidence:
- Why deferring is unsafe:
- Required remediation:

## P1 - Scope item that should be deferred to reduce risk

## P2 - Roadmap/gate ambiguity

## P3 - Nice-to-have sequencing cleanup

# Solution phase

## Minimal slice proposal
Include:
- In scope:
- Explicitly out of scope:
- Conservative defaults:
- Forbidden modes/paths:
- Required invariants:

## Exact spec edits
- Roadmap edits:
- Non-goal edits:
- Gate edits:
- Open-question edits:

## Implementation constraints
- Required feature flags:
- Required hard errors:
- Required fallback behavior:
- Required cleanup conservatism:

## Tests
- Minimal-slice correctness tests:
- Tests proving disabled paths fail safely:
- Tests for conservative cleanup:
- Regression tests for old behavior where retained:

## Benchmarks
- Advisory PR1 benchmarks:
- Gating benchmarks before default enablement:

## Sequencing
- PR1:
- PR2:
- Before column-store persistent writes:
- Before native-wire/Raft exposure:

## Open questions
```

## Required solution phase

Produce a concrete minimal-slice plan. For every proposed deferral, state why
deferral is safe, what guard prevents accidental use, and what test proves the
guard.
