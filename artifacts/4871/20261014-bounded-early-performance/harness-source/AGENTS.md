# Agent Plan: Persistent Value Log (No Slabs)

This document reflects the current TreeDB design: the value log is **persistent**
storage and is **not** an ephemeral WAL. The legacy "slab" storage path is
removed; large values are stored in the value log and referenced by pointers in
the index.

## Project status (pre-alpha)

- TreeDB is **pre-alpha**. On-disk formats and public APIs may change without backward-compatibility guarantees.
- It is acceptable for new binaries to fail to open old DB directories (and vice versa). Prefer rebuilding DB dirs for benchmarks/experiments.
- If you change on-disk formats, update docs and add/adjust relevant tests (but do not add complex migration scaffolding yet).

## Current Architecture

- **WAL (journal):** Redo log for durability/recovery. Can be disabled in
  cached mode. WAL and value log are decoupled.
- **Value log (vlog):** Persistent append-only store for large values. Value
  pointers are stored in the index. The value log has:
  - **GC** based on reachability (scans index for pointers).
  - **Rewrite/compaction** tooling (vlog rewrite) to reclaim space.
  - **Read integrity options** (checksum verification controls).
- **Index (B-Tree):** Stores inline values or value-log pointers (ValuePtr).

## Implications

- Value-log pointers are **valid long-term**; segments are not treated as
  ephemeral and must not be truncated just because they’re old.
- Pointer thresholds are safe **as long as** the value log is managed as
  persistent storage (GC/rewrite) and segments are only deleted when
  unreachable.

## Testing Strategy

Focus on pointer durability and GC correctness:

### Pointer durability after reopen
- **Setup:** `Options.ValueLog.PointerThreshold=1` (force value-log pointers).
- **Action:** Write values, `Checkpoint()` (or `WriteSync`), close, reopen.
- **Assert:** Values remain readable and pointers resolve after reopen.
- **Existing coverage:** `TreeDB/reopen_verify_test.go` (e.g. `TestReopenVerify_WALOn_Checkpoint`, `TestReopenVerify_WALOn_WriteSync`).

### GC deletes unreferenced segments
- **Setup:** Write values to the value log, delete keys, checkpoint.
- **Action:** Run `DB.ValueLogGC`.
- **Assert:** Fully-unreferenced segments are removed; referenced segments remain.
- **Existing coverage:** `TreeDB/db/vlog_gc_test.go` (`TestValueLogGC_RemovesUnreferencedSegment`).

### Leaf key compression density
- **Harness:** `TreeDB/node/leaf_density_test.go` (`BenchmarkLeafPageDensity`) measures keys/page with prefix compression on/off and enforces minimum effectiveness.

## Notes

- Any documentation describing TreeDB values being stored in "slabs" is legacy and should be updated or removed (HashDB still uses slab segments).
- If WAL is disabled, value-log writes can still be deferred to flush boundaries,
  but the value log remains persistent storage.

## Benchmark Profiling Workflow (Keep Updated)

- Standard capture flow:
  - `OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)`
  - `./bin/unified-bench ... -profile-dir "$OUT"`
  - `./bin/benchprof -profiles-dir "$OUT"`
- `-profile-dir` is expected to emit:
  - `benchprof_results.json`, `benchprof_results.md`
  - `cpu_<test>_<db>.pprof`
  - `allocs_<test>_<db>.pprof`
  - `checkpoint_cpu_checkpoint_<test>_<db>.pprof`
  - `block.pprof`, `mutex.pprof`, `trace.out`
- If benchmark test names, profile filenames, or profile-dir defaults change:
  - update parsers in `cmd/benchprof/main.go`
  - update tests in `cmd/benchprof/main_test.go` and `cmd/unified_bench/profile_artifact_dir_test.go`
  - update `cmd/unified_bench/README.md` and `cmd/benchprof/README.md`

### Focused collection insert compression profile

Use this when investigating TreeDB collection `InsertBatch` throughput and
value-log compression allocation costs:

```sh
RUN_DIR=/tmp/treedb_insert_compression_profile_$(date +%Y%m%d_%H%M%S) \
  scripts/treedb_insert_compression_profile.sh
```

The harness runs the short-lived `BenchmarkCollectionShapeInsertBatch` insert
shape for template-v1 collections on the native fast path. It captures the
default value-log compression path under `auto/`, optionally captures a
compression-off ceiling under `off/`, and writes `benchstat_auto_vs_off.txt`
when `benchstat` is installed.

Primary artifacts:

- `auto/collections_report.md`
- `auto/collections_cpu.pprof`, `auto/collections_cpu_top.txt`
- `auto/collections_mem.pprof`, `auto/collections_mem_top.txt`
- `benchstat_auto_vs_off.txt`

Useful overrides:

- `COUNT=1 BENCHTIME=1000x RUN_COMPRESSION_OFF=false` for a smoke run.
- `INDEXES_REGEX='0|1|2|3'` to include all current shape insert index counts.
- `RUN_TIMED_CPU=true` to also run the timed CPU-only insert profile.
