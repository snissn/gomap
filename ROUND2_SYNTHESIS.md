# #1242 Round 2 Synthesis

This summarizes the second review round used to narrow issue #1242 and split
implementation into smaller PRs.

## What The Prompts Established

- Raw TreeDB gates are mandatory for DB publish, zipper, leaf-log, value-log,
  cache, prepared output, and root-apply parallelism changes. Collection
  benchmark wins do not justify raw engine regressions.
- PR 1a should be a source-confirmed ownership and visibility guardrail PR.
  The first functional bug is non-exact secondary range reads missing queued
  and active indexed flush units. No-index updates and checkpoint semantics
  should stay unchanged in PR 1a.
- PR 1b should mostly expose existing counters through harnesses and reports.
  It adds only narrow missing observability for batch effectiveness, overlay
  queued/active depth, root-delta sizing, primary-only/no-index behavior, and
  per-phase Mongo gateway TreeDB deltas.
- PR 2 should be an audit/proof and small observability tightening PR, not a
  changed-mask rewrite. Current affected-index minimization mostly exists, but
  needs path/format parity tests, direct-update stats, and >64-index fallback
  proof.
- Unique reservations are currently inferred from pending secondary unique
  runs. Insert-style reservations cover mutable, queued, and active units, but
  changed-unique update semantics remain a later PR 3b coalescing problem.
- Deterministic PR 1a hooks should live primarily in collections, with at most
  one narrow DB ordered-root hook. There is no safe zipper prepared-output hook
  until prepared output exists.

## Ticket Changes Made

- Added a raw TreeDB / engine reuse boundary section to #1242.
- Replaced broad PR 1a language with a narrower source-verified version:
  range visibility, pending-layer enumeration, current ownership transitions,
  no-index update baseline, and checkpoint boundary.
- Reframed PR 1b around existing metrics and report surfaces instead of a broad
  new metrics subsystem.
- Kept prepared output, semantic coalescing, leaf-span parallelism, and no-index
  async write-back out of PR 1a/1b.
- Added acceptance language that raw-engine changes require raw TreeDB gates and
  collection-only wins are insufficient for shared engine code.

## Resulting PR Plan

- PR 1a: fix non-exact secondary range pending enumeration and add focused
  visibility/ownership tests for queued, active, requeued, close/flush, and
  checkpoint boundaries.
- PR 1b: wire phase-local TreeDB deltas, writer-sweep counters, selected raw
  root-apply stats, and benchprof metadata.
- PR 2: add direct/batch/BSON/template-v1/JSON/default/>64-index proof tests and
  small direct-update stats parity.
- Later PRs: semantic coalescing, prepared root apply, prepared pager/leaf-log
  output, leaf-span parallelism, and no-index async write-back.

## Next Round Guidance

The next GPT-5 Pro review should be a code-review round over concrete PR diffs,
not another architecture round. It should check merge semantics, pending-run
ordering, tombstone behavior, publish-work cleanup, counter denominators, and
whether any missing tests or counters are truly required before merge.
