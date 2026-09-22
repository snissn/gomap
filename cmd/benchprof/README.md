# benchprof

`benchprof` analyzes `unified-bench` profile artifacts (CPU + allocation sections) and emits:

- `insights.md` (human-readable summary)
- `insights.json` (machine-friendly summary)
- `insights.html` (browser view rendered from markdown)

It emits concrete investigation targets (function + `file:line`) for each section using
symbol/theme inference (iterator/seek, decode/read I/O, write/delete/flush, locking, alloc/copy, etc.),
so it can adapt as implementations evolve without hardcoding specific function names.

## Build

```bash
make benchprof
```

## Typical flow

1. Run `unified-bench` with profile outputs into one directory. `-profile-dir` defaults artifact metadata to `-path-label native-fastpath`; pass `-path-label oracle` for explicit oracle/comparator captures, `-path-label m8-m14-10mm-gate` for the #2768+ mandatory span-run gate shape, `-path-label span-native-default-gate` for the default span-native production closeout matrix, or `-path-label span-native-read-scan-guardrail` for the related settled read/scan guardrail matrix.
2. `unified-bench` auto-runs `benchprof` in-process when `-profile-dir` is enabled. You can still run `benchprof` manually if needed.

Example:

This example uses unified-bench's legacy `fast` benchmark-runner preset for a
no-WAL profiling ceiling; it is not a TreeDB server profile recommendation.

```bash
mkdir -p /tmp/scan-profiles

./bin/unified-bench \
  -dbs treedb \
  -keys 800000 \
  -profile fast \
  -checkpoint-between-tests \
  -treedb-vlog-compression-variant off \
  -test full_scan,prefix_scan \
  -profile-dir /tmp/scan-profiles \
  -path-label native-fastpath \
  -progress=false

./bin/benchprof \
  -profiles-dir /tmp/scan-profiles
```

Outputs:

- `/tmp/scan-profiles/insights.md`
- `/tmp/scan-profiles/insights.json`
- `/tmp/scan-profiles/insights.html`

`insights.html` is always generated. For `unified-bench -suite collection_storage`,
the same command also renders the suite's `runs[].collection_workloads` metadata
(mode/workload names, correctness and semantic-equivalence flags, asset-byte
splits, and per-workload counters) into a "Collection Workload Metadata" table.

## Notes

- `benchprof` currently reads:
  - `benchprof_results.json` (preferred; auto-written by `unified-bench -profile-dir`)
  - `benchprof_results.md` (fallback; auto-written by `unified-bench -profile-dir`)
  - `cpu_<test>_<db>.pprof` (all test sections)
  - `allocs_<test>_<db>.pprof` (allocation delta profiles by section; auto-written by `unified-bench -profile-dir`)
  - `block_<test>_<db>.pprof` (per-test block contention delta profiles; present when non-empty)
  - `mutex_<test>_<db>.pprof` (per-test mutex contention delta profiles; present when non-empty)
  - `checkpoint_cpu_checkpoint_<test>_<db>.pprof` (checkpoint CPU sections)
  - `block.pprof` / `mutex.pprof` (global run-level fallback/supplement)
  - `trace.out` (detected, but not deeply analyzed yet)
- `benchprof_results.json` preserves selected TreeDB stats under
  `runs[].treedb_stats` when the benchmark exposes them. Checkpoint-enabled
  runs also expose `runs[].checkpoint_durations_seconds`, optional
  `runs[].checkpoint_settle_seconds`, and checkpoint-local selected stats under
  `runs[].checkpoint_treedb_stats`. This is the raw
  counter metadata used for TreeDB root-apply/cache review artifacts. For
  value-log mmap reads, unified-bench selected displays prefer backend
  `treedb.vlog.mmap_read.*` counters over cache-prefixed aliases, and the
  metadata includes generic plus leaf-specific sealed mmap budget caps when
  TreeDB exposes them. Parallel-flush M0/M8 artifacts preserve
  `treedb.cache.flush_apply.*`, `treedb.cache.leaf_log_lanes.*`,
  `treedb.cache.flush_span_run.*`, `treedb.flush_apply.*`,
  `treedb.flush_apply.span_run.*`, and `treedb.flush_apply.span_native.*`
  counters so planning, leaf-log lane distribution, canonical run shape,
  old-leaf read/decode bytes/op, leaf merges/op, replacement pages/op,
  append frames/op, publish prepare/final-install, guarded publish,
  reducer/publish, checkpoint wait splits, fallback reasons, retry, and foreground-assist stages appear beside
  CPU/allocation/contention profiles. Value-log codec policy artifacts also preserve
  `treedb.cache.vlog_auto.*`, `treedb.cache.vlog_write_mode.*`,
  `treedb.cache.vlog_payload_kind.*`, `treedb.cache.vlog_payload_split.*`,
  `treedb.cache.vlog_outer_leaf_codec.*`, and `treedb.cache.vlog_block.*`
  counters so actual auto codec selection, outer-leaf codec distribution, and
  frame-K distribution remain available in benchprof output.
- For command-WAL adapter evidence, keep `treedb.command_wal.*`,
  `treedb.applied_command_lsn`, and `treedb.cache.checkpoint.*` counters
  together. The first group proves accepted/covered command frames; the
  checkpoint group measures explicit backend publication work and should not be
  conflated with `WriteSync`/`Batch.WriteSync` command-WAL sync cost.
- `benchprof_results.json` also preserves collection-storage suite metadata
  under `runs[].collection_workloads` when `unified-bench -suite
  collection_storage` is used. `benchprof` keeps those stable mode/workload names
  and semantic comparability fields in `insights.{md,json,html}` alongside the
  CPU/allocation profile summaries.
- Optional flags:
  - `-bin` if you want explicit symbolization target (otherwise profile-only mode is used)
  - `-run-md` to force a specific markdown log file for ops/sec parsing
  - `-out-html` to override the default HTML output path

## Maintenance Expectations

If `unified-bench` profile naming, profile-dir defaults, or benchmark test names
change, update `benchprof` in the same PR:

- filename parsing in `internal/benchprof/main.go`
- parser tests in `internal/benchprof/main_test.go`
- `cmd/unified_bench/profile_artifact_dir_test.go` expectations
- this README and `cmd/unified_bench/README.md`
