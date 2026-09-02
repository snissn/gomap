# BenchProf Maintenance Runbook

Scope: keeping benchmark profile capture and analysis maintainable as benchmark
sections and internals evolve.

This is an implementation runbook for contributors/agents. Canonical CLI
semantics live in:

- `cmd/unified_bench/README.md`
- `cmd/benchprof/README.md`

## Standard Workflow (recommended)

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -keys 800000 \
  -profile fast \
  -checkpoint-between-tests \
  -test random_write,random_delete,random_read,full_scan,prefix_scan \
  -profile-dir "$OUT" \
  -progress=false

./bin/benchprof -profiles-dir "$OUT"
```

Expected artifacts in `$OUT`:

- `benchprof_results.json` and `benchprof_results.md`
- `cpu_<test>_<db>.pprof`
- `allocs_<test>_<db>.pprof`
- `checkpoint_cpu_checkpoint_<test>_<db>.pprof`
- `block.pprof`, `mutex.pprof`, `trace.out`
- `insights.md`, `insights.json`, `insights.html`
- selected TreeDB metadata in `benchprof_results.json`, including current
  `treedb.cache.flush_apply.*` / `treedb.flush_apply.*` stage counters when
  TreeDB emits them.

## Maintenance Contract

When profile file naming, benchmark section names, or profile-dir defaults
change, update all of these in the same PR:

- Parsing and section inference in `cmd/benchprof/main.go`
- Tests in `cmd/benchprof/main_test.go`
- Profile-dir artifact expectations in
  `cmd/unified_bench/profile_artifact_dir_test.go`
- User-facing docs:
  - `cmd/unified_bench/README.md`
  - `cmd/benchprof/README.md`
  - `docs/BENCHMARK_SPEC.md` (if user workflow changes)

## Keep It Future-Proof

- Prefer discovery-by-pattern over hardcoded test-name lists.
- Keep insight rules theme-based (iterator/seek, decode/read I/O,
  write/delete/flush, lock/contention, allocation/copy), not function-name
  exact matches.
- If new profile sections appear, ensure they pass through the same insight
  engine and render in markdown/json/html without special-casing.

## Validation Checklist

```bash
go test ./cmd/unified_bench -short -count=1
go test ./cmd/benchprof -count=1
make unified-bench benchprof
```

Optional local smoke run:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)
./bin/unified-bench -dbs treedb -keys 200000 -profile fast -profile-dir "$OUT" -progress=false
./bin/benchprof -profiles-dir "$OUT"
```
