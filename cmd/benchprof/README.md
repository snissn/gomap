# benchprof

`benchprof` analyzes `unified-bench` profile artifacts and emits:

- `insights.md` (human-readable summary)
- `insights.json` (machine-friendly summary)
- `insights.html` (optional browser view rendered from markdown)

It now also emits concrete investigation targets (function + `file:line`) when it detects
iterator/seek-heavy prefix scans (for example: "iterator setup/seek overhead, not value decoding").

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
  -profiles-dir /tmp/scan-profiles \
  -html
```

Outputs:

- `/tmp/scan-profiles/insights.md`
- `/tmp/scan-profiles/insights.json`
- `/tmp/scan-profiles/insights.html`

## Notes

- `benchprof` currently reads:
  - `benchprof_results.json` (preferred; auto-written by `unified-bench -profile-dir`)
  - `benchprof_results.md` (fallback; auto-written by `unified-bench -profile-dir`)
  - `cpu_full_scan_*.pprof`
  - `cpu_prefix_scan_*.pprof`
  - `block.pprof`
  - `mutex.pprof`
  - `trace.out` (detected, but not deeply analyzed yet)
- Optional flags:
  - `-bin` if you want explicit symbolization target (otherwise profile-only mode is used)
  - `-run-md` to force a specific markdown log file for ops/sec parsing
