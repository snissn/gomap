# Changelog

This project is a dev playground; the public surface and on-disk formats may evolve.
See `docs/API_STABILITY.md` for what is intended to be stable for downstream use.

## Unreleased

- (Add notable changes here.)

## v0.5.0 - 2026-04-17

This release continues the current TreeDB pre-alpha push around live maintenance,
outer-leaf storage efficiency, and profiling-guided runtime work.

### Highlights

- Landed the immutable leaf-generation maintenance stack and follow-on efficiency
  work for online leaf-pack planning, selection, and execution.
- Added TreeDB-specific `unified-bench` perf instrumentation plus live
  `run_celestia` profiling support so synthetic and real-workload analysis now
  share a common telemetry path.
- Reduced several hot-path costs in TreeDB, especially snapshot acquire/close
  overhead, `leaf_vlog` mmap coverage, and steady flush copy churn.
- Added compact split-`leaf_vlog` payload encoding for sparse leaf pages and
  improved leaf-page compression defaults.
- Split outer-leaf dictionary plumbing from generic value-log dictionary state
  so rewrite and maintenance paths can carry leaf-specific dict behavior
  cleanly.

### Compatibility

- TreeDB remains pre-alpha.
- On-disk formats and behavior may still change without backward-compatibility
  guarantees.
- Rebuilding DB directories between versions is still the expected path for
  experiments and benchmarks.
