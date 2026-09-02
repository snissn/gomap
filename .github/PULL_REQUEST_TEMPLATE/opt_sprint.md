## Objective

Describe the single objective in 1–3 sentences.

## Context

Why this change is needed now (and what it unlocks).

## Non-goals

Explicitly list what is intentionally NOT being done in this PR.

## Design

- Formats/flags/APIs changed (include diagrams if applicable)
- Invariants that must hold
- Fail-closed behavior (caps before alloc; CRC/error handling)

## Scope

- Includes (files/symbols):
- Excludes (files/symbols):

## Correctness

- Tests run (exact commands):
- Corruption/edge cases covered:
- Concurrency/race coverage (if applicable):

## Performance

- Benchmarks run (exact commands):
- Results table (before/after; median-of-5 per G1 in `docs/OPT_SPRINT_NEXT.md`):
- Regressions (if any) and why acceptable:

## Rollout / Toggle Plan

- Default setting:
- How to disable quickly:

## Follow-ups

- Issues/PRs to do next:

## Column Graph Native Workstream (#1646, if applicable)

### Copied/Adapted From Old Stack

- Copied:
- Adapted:
- Oracle/comparator only:
- Quarantined/not copied:

### Path Identity

- Native reader:
- Decoded comparator:
- Legacy/native vector index:
- Unsupported/fail-closed:

### Base And Dependency State

- Base branch:
- Base commit:
- Base PR/head:
- #1621/#1634 assumptions:
- Missing generic column-store primitives:
- Parent review fixes propagated:

### Evidence

- Tests:
- Benchmarks:
- Status/fallback proof:
- Performance attribution:
- Local regressions found/fixed:

### Test Plan Start

- Milestone tasks targeted:
- Tests to add before or with implementation:
- Existing tests to preserve:
- #1646 test-list changes made before implementation:

### Performance Plan Start

- Relevant benchmarks/metrics for this PR:
- Baseline/comparator command or reason not applicable:
- Expected setup/search/materialization boundary:
- Upstream costs explicitly out of scope:
- Local performance risks to watch:

### Test Plan Close

- Additional tests found during implementation/review:
- Tests added or changed:
- Failing tests fixed:
- #1646 test-list changes made at close:
- Residual untested gaps, if any:

### Performance Plan Close

- Benchmark commands run:
- Results summary with throughput/latency/allocations:
- Before/after or baseline comparison:
- Local slow/heavy code found and fixed:
- Remaining upstream or deferred costs:

### AI Review Loop

- Codex latest-head review:
- Copilot latest-head review:
- CodeRabbit latest-head review:
- Review comments/threads resolved or explicitly dismissed:
- Re-requested after final meaningful push:

---

## Optimization PR Checklist (TreeDB)

### Scope
- [ ] Single, clear objective for this PR (not a bundle)
- [ ] Explicit include list (files/symbols touched):
- [ ] Explicit exclude list (files/symbols NOT touched):
- [ ] No unwanted feature reintroduction (e.g. async slab writer, hard zones, ValueIndex) unless explicitly stated in the PR objective

### Safety / Correctness
- [ ] Clear written-vs-durable semantics for any `*Sync` path touched
- [ ] No new mmap usage on mutable/truncating files
- [ ] All new length fields are capped before allocation (OOM-safe)
- [ ] CRC/checksum behavior is fail-closed (clean error, no panic)
- [ ] Any concurrency change has a defined state machine and invariants

### Tests
- [ ] Added deterministic regression tests for the targeted failure mode
- [ ] Tests avoid sleeps where possible (use latches/hooks)
- [ ] Added corruption/edge-case tests (truncation, bad headers, invalid lengths) when format/parsing changes
- [ ] Ran locally:
  - [ ] `go test ./... -count=1`
  - [ ] Any targeted packages/benchmarks:

### Benchmarks / Measurements
- [ ] Added or updated a benchmark relevant to the change
- [ ] Reported results in PR description (before/after, command, machine)
- [ ] Included “incompressible” (worst-case) results if compression is involved

### Docs
- [ ] Updated `docs/OPT_SPRINT_NEXT.md` milestone status (if applicable)
- [ ] Documented any new config knobs + defaults
- [ ] Called out any format change in PR description (pre-alpha OK)

### Rollout / Toggle Plan (if behavior-affecting)
- [ ] Safe defaults (feature off or conservative threshold)
- [ ] Clear knobs to disable/revert behavior quickly
- [ ] Observability: counters/logs to verify effectiveness
