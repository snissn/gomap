# Trace Bench Changelog

This changelog summarizes the trace-based benchmarking work on `treedb-tracing-capture`.

## Unreleased

- Added trace capture hooks via `kvstore/adapters/treedbtrace` (opt-in wrapper).
- Added trace summary tool (`cmd/trace_bench`) and trace replay tool (`cmd/trace_replay`).
- Added summary-based benchmarks:
  - `BenchmarkTraceReplay`
  - `BenchmarkTraceReplayMemtableModes`
- Added timeline replay benchmarks with iterator overlap:
  - `BenchmarkTraceReplayTimeline`
  - `BenchmarkTraceReplayTimelineMemtableModes`
- Added helper scripts to capture and pull traces from the server:
  - `scripts/capture_celestia_trace.sh`
  - `scripts/pull_celestia_trace.sh`
- Added memtable reuse + pooling in cached mode with read tracking (default enabled).
- Added iterator rotation reduction option (`IteratorMutableMaxBytes`) (opt-in).
- Added `docs/TREEDB_BENCHMARKING.md` and updated tuning/plan notes.
