# TreeDB Benchmarking Guide

This guide focuses on how to run TreeDB benchmarks consistently and how to use
trace-based replays that approximate Celestia workloads.

## Quick Map

- **Micro/macro synthetic:** `cmd/unified_bench`
- **Trace summary replay:** `BenchmarkTraceReplay`
- **Trace timeline replay (overlap-aware):** `BenchmarkTraceReplayTimeline`
- **Memtable mode matrices:** `BenchmarkTraceReplayMemtableModes`, `BenchmarkTraceReplayTimelineMemtableModes`
- **External YCSB comparison:** `scripts/ycsb_compare_mongodb_treedb.sh`, with the current report index in `docs/benchmarks/ycsb_mongodb_treedb_current.md`

## Common Principles

- Use the same hardware when comparing results.
- Keep env and flags identical (journal, value log, compression, memtable settings).
- Prefer multiple runs or `-count` to reduce noise.
- Record inputs (trace file, summary, scaling, memtable mode) with results.

## External YCSB Comparison

Use this when comparing MongoDB, `treedb-native`, and the TreeDB Mongo gateway
with the external `go-ycsb` client:

```bash
scripts/ycsb_compare_mongodb_treedb.sh
```

The script writes host metadata, raw `go-ycsb` output, exact commands,
`summary.tsv`, and `summary.md` under `OUT_DIR`. Set `RUN_REPEATS=3` when
investigating first-run versus steady-state behavior after a load phase.

Any nonzero YCSB operation error counter such as `INSERT_ERROR`, `READ_ERROR`,
or `UPDATE_ERROR` marks that phase invalid and makes the script exit nonzero by
default. For exploratory parsing of known-bad artifacts, set
`ALLOW_YCSB_ERRORS=true`; the generated summaries still show the invalid rows.
Use `PARSE_ONLY=true OUT_DIR=/path/to/run` to regenerate summaries from saved
raw `*.out` files without rerunning servers.

The current report index is
`docs/benchmarks/ycsb_mongodb_treedb_current.md`. It points at the latest
checked-in command-WAL profile evidence, labels older reports as historical or
superseded, and records the next full rerun matrix.

## 1) Unified Bench (Synthetic)

Baseline synthetic workload across engines.

Example:
```bash
./bin/unified-bench -keys 1000000 -tests write_seq,read_rand -dbs treedb
```

More details: `docs/BENCHMARK_SPEC.md`.

Key-shape override for non-dataset 8-byte workloads:
```bash
./bin/unified-bench -dbs treedb -test batch_write,random_read,prefix_scan -key-shape be8_prefix4
```

### Profile Analysis with benchprof (recommended)

Capture profiles and analyze them in one pass:

This example uses unified-bench's `fast` benchmark-runner preset, which enters
TreeDB's explicit `bench_unsafe` no-WAL profiling boundary. Public TreeDB server runs should use
`command_wal_durable`, `command_wal_relaxed`, `no_wal_fast`, or explicit
benchmark/test-only `bench_unsafe` through the benchmark constructor boundary.

```bash
OUT=$(mktemp -d /tmp/treedb_profiles_XXXXXX)

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

Outputs:
- `$OUT/insights.md`
- `$OUT/insights.json`
- `$OUT/insights.html`

This includes section-wise CPU hotspots, allocation hotspots
(`alloc_space`/`alloc_objects`), contention hotspots, selected TreeDB stats
(including `treedb.cache.flush_apply.*` / `treedb.flush_apply.*` when emitted),
and investigation targets with source references when resolvable.

### Force-pointers perf matrix (script)

For repeatable perf work on the TreeDB "forced value pointers" mode (value log
hot path), use:

```bash
scripts/treedb_forceptr_matrix.sh
```

Defaults:
- `PROFILES=fast`
- `TESTS=batch_write,random_write,batch_delete`
- `VARIANTS=base,prefix,columnar,columnar_prefix`
- writes artifacts under `artifacts/perf/treedb_forceptr_matrix_<timestamp>/`

Common overrides:
```bash
KEYS=4000000 PROFILES=fast scripts/treedb_forceptr_matrix.sh
KEYS=2000000 PROFILES=fast,balanced PPROF_TESTS=batch_write,random_write scripts/treedb_forceptr_matrix.sh
```

### All-flags gate (script)

To validate the stacked index flags as a bundle, run:

```bash
scripts/treedb_allflags_gate.sh
```

Defaults:
- `RUNS=7`
- `PROFILE=fast`
- `KEYS=4000000`
- `TESTS=batch_write,batch_random,random_read,prefix_scan`
- both variants include `-checkpoint-between-tests`

Compared variants:
- `base`: `-treedb-force-value-pointers -treedb-index-optimizations=false`
- `allflags`: `-treedb-index-optimizations=true` (enables force pointers + prefix compression + columnar leaves + packed value pointers + internal base-delta)

Pass conditions (median-of-runs):
- `batch_write`, `batch_random`, `random_read`, `prefix_scan`: all-flags median must be strictly greater than base median.
- `maindb/index.db`: all-flags median must be strictly smaller than base median.

Outputs:
- `artifacts/perf/treedb_allflags_gate_<timestamp>/summary.md`
- `artifacts/perf/treedb_allflags_gate_<timestamp>/summary.json`
- per-run markdown logs under `.../runs/`

PR benchmark template:

```markdown
## All-flags gate

- command: `scripts/treedb_allflags_gate.sh`
- artifact path: `artifacts/perf/treedb_allflags_gate_<timestamp>/`

| metric | base median | all-flags median | delta |
|---|---:|---:|---:|
| batch_write | ... | ... | ... |
| batch_random | ... | ... | ... |
| random_read | ... | ... | ... |
| prefix_scan | ... | ... | ... |
| index_db_bytes | ... | ... | ... |

- gate result: PASS/FAIL
```

### Leaf page density harness (prefix compression)

When working on leaf key encodings, it’s useful to measure **keys per leaf page**
directly (and the alloc/memory churn of the encoder) without running a full DB:

```bash
go test ./TreeDB/node -run '^$' -bench BenchmarkLeafPageDensity -benchmem -count=1
```

Regression guardrails:
- `TreeDB/node/leaf_density_test.go` enforces a minimum density improvement for
  prefix-heavy key workloads when leaf prefix compression is enabled (and when
  the combined columnar+prefix mode is enabled).

### Template encode profiling (steady-only)

For template-compression profiling, prefer the steady-state-only CPU profile
capture in `TreeDB/cmd/vlog_dict_realdata`:

```bash
go run ./TreeDB/cmd/vlog_dict_realdata \
  -input /tmp/treedb_template_smoke.jsonl -input-encoding string \
  -train 10000 -eval 5000 -cap 0 \
  -bench-kv -bench-mode wal_off -bench-pointer-threshold 1 \
  -bench-compression off -bench-template on \
  -bench-raw-mib 512 -bench-batch 1024 -bench-workers 8 -bench-key-mode dataset \
  -bench-cpu-profile /tmp/treedb_template_steady.pprof

go tool pprof -http=:0 /tmp/treedb_template_steady.pprof
```

Notes:
- `-bench-cpu-profile` focuses on the steady write loop (best for template encode hot paths).
- `-cpu-profile` profiles the whole program (load + warmup + steady + reporting).

## 2) Trace Summary Replay (Counts Only)

Uses the JSON summary from trace capture and replays batch sizes + op counts.
This does **not** model timing overlap.

Benchmark:
```bash
TREEDB_TRACE_SUMMARY=/path/to/trace.summary.json \
go test -bench BenchmarkTraceReplay -run '^$' ./TreeDB
```

Memtable mode matrix (summary-based):
```bash
TREEDB_TRACE_SUMMARY=/path/to/trace.summary.json \
go test -bench BenchmarkTraceReplayMemtableModes -run '^$' ./TreeDB
```

## 3) Trace Timeline Replay (Overlap-Aware)

Uses the JSONL trace to model iterator lifetimes and write overlap. This is the
closest approximation to the Celestia workload that does not require a server.

Required inputs:
- JSONL trace: `treedb_trace_*.jsonl`
- Summary JSON: `treedb_trace_*.summary.json`

Benchmark:
```bash
TREEDB_TRACE_SUMMARY=/path/to/trace.summary.json \
TREEDB_TRACE_JSONL=/path/to/trace.jsonl \
TREEDB_TRACE_TIMELINE_DURATION_MS=3000 \
go test -bench BenchmarkTraceReplayTimeline -run '^$' ./TreeDB
```

Memtable mode matrix (timeline-based):
```bash
TREEDB_TRACE_SUMMARY=/path/to/trace.summary.json \
TREEDB_TRACE_JSONL=/path/to/trace.jsonl \
TREEDB_TRACE_TIMELINE_DURATION_MS=3000 \
go test -bench BenchmarkTraceReplayTimelineMemtableModes -run '^$' ./TreeDB
```

### Timeline Duration Scaling

`TREEDB_TRACE_TIMELINE_DURATION_MS` compresses each phase’s timeline. Lower
values run faster but can exaggerate contention. Higher values are more faithful
but slower.

Guidance (Apple M3, trace `20260109071235`):
- `1000ms` ~3.2–3.5s/op (very compressed)
- `3000ms` ~9s/op (balanced)
- `5000ms` ~15s/op (more realistic)
- `10000ms` ~30s/op (high fidelity)

A good default for local work is **3000–5000ms**.

## 4) Capturing a Trace from the Server

### Capture (server run)
```bash
scripts/capture_celestia_trace.sh mikers@192.168.0.132 /home/mikers/run_celestia.sh
```

This prints:
- trace JSONL path (`/home/mikers/treedb_trace_*.jsonl`)
- summary JSON path (`/home/mikers/treedb_trace_*.summary.json`)

### Pull to local
```bash
scripts/pull_celestia_trace.sh mikers@192.168.0.132 /home/mikers/treedb_trace_YYYYMMDDHHMMSS.jsonl ./tmp_traces
```

If summary is missing (older trace), generate it on server:
```bash
ssh mikers@192.168.0.132 'cd /home/mikers/dev/snissn/gomap-tracing && \
  go run ./cmd/trace_bench -trace /home/mikers/treedb_trace_YYYY.jsonl \
  -out /home/mikers/treedb_trace_YYYY.summary.json'
```

## 5) Memtable Mode Comparisons

Modes:
- `adaptive` (default)
- `skiplist`
- `hash_sorted`
- `btree`

When using trace benchmarks, always run a mode matrix to compare behavior.
Note that results can converge as timeline duration increases.

## 6) Common Environment Knobs

These apply to trace replay benchmarks:

- `TREEDB_TRACE_MODE`: `cached` (default) or `backend`
- `TREEDB_TRACE_DISABLE_JOURNAL`: `1/0` (preferred; disables the journal; takes precedence when set)
- `TREEDB_TRACE_DISABLE_WAL`: `1/0` (legacy alias; disables journal + value log)
- `TREEDB_TRACE_DISABLE_VLOG`: `1/0`
- `TREEDB_TRACE_FLUSH_THRESHOLD`: bytes
- `TREEDB_TRACE_MEMTABLE_SHARDS`: int
- `TREEDB_TRACE_ITERATOR_MUTABLE_MAX_BYTES`: bytes
- `TREEDB_TRACE_SCALE`: scale factor for counts in summary (default 1.0)
- `TREEDB_TRACE_TIMELINE_DURATION_MS`: per-phase timeline duration (timeline replay)

## 7) Reporting Results

When you share results, always include:
- Trace ID or JSONL path
- Timeline duration (if timeline replay)
- Hardware / CPU model
- Memtable mode and key options (journal/value log/thresholds)

This makes comparisons reproducible.
