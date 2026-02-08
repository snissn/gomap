# benchprof

`benchprof` analyzes `unified-bench` profile artifacts and emits:

- `insights.md` (human-readable summary)
- `insights.json` (machine-friendly summary)
- `insights.html` (optional browser view rendered from markdown)

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
  -cpuprofile /tmp/scan-profiles/cpu \
  -cpuprofile-tests full_scan,prefix_scan \
  -blockprofile /tmp/scan-profiles/block.pprof \
  -mutexprofile /tmp/scan-profiles/mutex.pprof \
  -trace /tmp/scan-profiles/trace.out \
  -progress=false \
  > /tmp/scan-profiles/run.md 2>&1

./bin/benchprof \
  -profiles-dir /tmp/scan-profiles \
  -bin ./bin/unified-bench \
  -run-md /tmp/scan-profiles/run.md \
  -html
```

Outputs:

- `/tmp/scan-profiles/insights.md`
- `/tmp/scan-profiles/insights.json`
- `/tmp/scan-profiles/insights.html`

## Notes

- `benchprof` currently reads:
  - `cpu_full_scan_*.pprof`
  - `cpu_prefix_scan_*.pprof`
  - `block.pprof`
  - `mutex.pprof`
  - `trace.out` (detected, but not deeply analyzed yet)
- For ops/sec parsing from run logs, capture **combined stdout+stderr** (`> run.md 2>&1`).
