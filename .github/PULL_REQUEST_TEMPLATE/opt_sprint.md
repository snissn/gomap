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

