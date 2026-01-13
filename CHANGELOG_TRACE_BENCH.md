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

## Benchmark Plan (Working)

1) **Write benchmark we can trust**
   - Use timeline replay with `TREEDB_TRACE_TIMELINE_NO_SLEEP=1` and `TREEDB_TRACE_TIMELINE_INLINE_ITERS=1` to reduce scheduler noise.
   - Add `TREEDB_TRACE_SKIP_ITERS=1` to focus on write/flush hot paths when validating write bottlenecks.
   - Compare the resulting CPU profile against a Celestia run; ensure TreeDB zipper/batch paths dominate.

2) **Use benchmark to resolve priority optimizations**
   - Target hotspots from profile: zipper merge/write, leaf key decoding, batch sorting/building, GC pressure.
   - Measure improvements in `BenchmarkTraceReplayTimeline` (no-sleep mode).

3) **Confirm optimizations using Celestia run**
   - Re-run `/home/mikers/run_celestia_trace.sh` and compare CPU profiles (zipper + leaf key paths).
   - Validate wall-clock improvements in `sync-time.log`.

4) **Commit and iterate**
   - Keep each optimization as a small commit with before/after benchmark numbers.
   - Update `invalid_value_debug.md` with trace run IDs and profile diffs.
