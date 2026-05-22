# Deferred Experiment Issues

This log is for non-blocking `experiments/colgranule` issues found while
making the stacked PRs mergeable. It exists to avoid spending production-track
time polishing experiment-only code while still preserving review context.

## Deferral Rule

Defer only issues that are clearly limited to the experiment harness or its
diagnostics. Do not defer:

- Fail-closed correctness issues in manifest, lifecycle, reachability, or
  recovery paths.
- Storage-contract issues that would mislead production column-store design.
- Bugs that can corrupt benchmark parity, asset refs, checksums, or GC inputs.
- Issues that block CI, active AI review resolution, or clean propagation.

## Deferred Items

- Experiment-only byte-accounting integer widths still use `int` in several
  reporting structs. This is acceptable for the current JSONBench-scale harness
  and should be revisited only if the experiment report format is reused as a
  production/public API.
- Additional JSON/report polish for `jsonbench_compare` should stay deferred
  unless it affects q1-q5 parity, benchmark gate interpretation, or PR evidence.
- Local setup-allocation cleanup in experiment lifecycle/view benchmarks should
  not get standalone work unless it changes a production-facing design decision.
