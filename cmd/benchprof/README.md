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

1. Run `unified-bench` with profile outputs into one directory.
2. Run `benchprof` on that directory.

Example:

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
  -progress=false

./bin/benchprof \
  -profiles-dir /tmp/scan-profiles
```

Outputs:

- `/tmp/scan-profiles/insights.md`
- `/tmp/scan-profiles/insights.json`
- `/tmp/scan-profiles/insights.html`

`insights.html` is always generated.

## Notes

- `benchprof` currently reads:
  - `benchprof_results.json` (preferred; auto-written by `unified-bench -profile-dir`)
  - `benchprof_results.md` (fallback; auto-written by `unified-bench -profile-dir`)
  - `cpu_<test>_<db>.pprof` (all test sections)
  - `allocs_<test>_<db>.pprof` (allocation delta profiles by section; auto-written by `unified-bench -profile-dir`)
  - `checkpoint_cpu_checkpoint_<test>_<db>.pprof` (checkpoint CPU sections)
  - `block.pprof`
  - `mutex.pprof`
  - `trace.out` (detected, but not deeply analyzed yet)
- Optional flags:
  - `-bin` if you want explicit symbolization target (otherwise profile-only mode is used)
  - `-run-md` to force a specific markdown log file for ops/sec parsing
  - `-out-html` to override the default HTML output path

## Maintenance Expectations

If `unified-bench` profile naming, profile-dir defaults, or benchmark test names
change, update `benchprof` in the same PR:

- filename parsing in `cmd/benchprof/main.go`
- parser tests in `cmd/benchprof/main_test.go`
- `cmd/unified_bench/profile_artifact_dir_test.go` expectations
- this README and `cmd/unified_bench/README.md`
