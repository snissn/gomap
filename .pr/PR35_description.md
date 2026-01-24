## PR35: CI perf gate for ValueLog dict compressibility bench

### Summary
- Fix the CI perf bench selector + baselines so we actually gate the current benchmark names:
  - Bench names now include `valsize=...` and pattern names (e.g. `highly_compressible_tail64`).
- Promote the checker from warning-only to a **hard gate** (`-strict=true`) with conservative thresholds.

### Local validation
```
go test ./TreeDB/internal/valuelog -run '^$' \
  -bench 'BenchmarkValueLogDictCompressibilitySweep/valsize=1024/(highly_compressible_tail64|medium_compressible|incompressible)/dict_(off|on)/k=4$' \
  -benchmem -count=5 | tee /tmp/vlog_dict_bench.txt

go run .github/scripts/check_vlog_dict_bench.go \
  -bench-output /tmp/vlog_dict_bench.txt \
  -baseline .github/perf_baselines/vlog_dict_defaults.json \
  -strict=true
```

### Sample checker output
```
perf: BenchmarkValueLogDictCompressibilitySweep/valsize=1024/highly_compressible_tail64/dict_off/k=4 runs=5 mbps=1042.10 ratio=1.00000 fallback_max=0
perf: BenchmarkValueLogDictCompressibilitySweep/valsize=1024/highly_compressible_tail64/dict_on/k=4 runs=5 mbps=554.20 ratio=0.06978 fallback_max=0
perf: BenchmarkValueLogDictCompressibilitySweep/valsize=1024/medium_compressible/dict_off/k=4 runs=5 mbps=1659.27 ratio=1.00000 fallback_max=0
perf: BenchmarkValueLogDictCompressibilitySweep/valsize=1024/medium_compressible/dict_on/k=4 runs=5 mbps=505.73 ratio=0.08682 fallback_max=0
perf: BenchmarkValueLogDictCompressibilitySweep/valsize=1024/incompressible/dict_off/k=4 runs=5 mbps=1687.26 ratio=1.00000 fallback_max=0
perf: BenchmarkValueLogDictCompressibilitySweep/valsize=1024/incompressible/dict_on/k=4 runs=5 mbps=1295.73 ratio=1.00000 fallback_max=0
```

