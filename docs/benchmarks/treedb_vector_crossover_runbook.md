# TreeDB Vector Crossover Benchmark Runbook

This runbook defines the command contract for issue #2491 and the downstream
#2490 crossover campaign. It is documentation-only: do not change benchmark
names, counters, runtime search semantics, or route boundaries to satisfy this
matrix without a separate harness issue/PR.

## Tracker and inventory summary

Issue facts used for this contract:

- #2490 is the parent crossover campaign. It asks where TreeDB exact FP32,
  scalar_u8, and RaBitQ `rabitq_1bit` search cross over as dataset size and
  dimension grow, with USearch as an in-memory comparator and pgvector as a
  separated PostgreSQL system-context comparator.
- #2491 owns this runbook/matrix foundation: fixture presets, c=1/c=8 rows,
  commands, metrics, artifact layout, quiet-host rules, and setup caveats.
- #2483 is the final docs lane. It must not publish final performance tables
  until accepted evidence is available and must keep exact FP32, scalar_u8,
  RaBitQ, USearch, and pgvector boundaries distinct.
- #2487 produced a current-main small-fixture snapshot. Its host was moderately
  loaded, so treat it as contextual current-main evidence, not speedup proof.
  It explicitly excludes #2457, #2460-#2464, #2478, and #2479 no-promote rows.
- #2445 closed no-promote for the exact c=8 heap candidate; PR #2457 was closed
  unmerged and provides no accepted speedup claim.
- #2446 accepted only the scalar_u8 edge-accounting aggregation (#2456) and the
  per-row profiling workflow (#2466). #2460-#2464 are no-promote.
- #2475 keeps RaBitQ lanes separate. Accepted Sublane A artifacts include the
  `rabitq_1bit` profile gate and scorer work; #2478/#2479 are no-promote, and
  future RaBitQ/BRQ codecs require new codec names/versions.

Existing harnesses inventoried:

- `scripts/bench_vector_search_compare.sh`: Go benchmark wrapper for exact FP32
  TreeDB production rows and USearch in-memory controls. It accepts
  `TREEDB_VECTOR_BENCH_DOCS`, `TREEDB_VECTOR_BENCH_DIMS`, M/ef/topK/query env
  vars, `CPU_LIST`, `BENCHTIME`, and `COUNT`, and writes `README.md` plus
  `bench.txt` under `RUN_DIR`.
- `scripts/treedb_quantized_buffered_row_profiles.sh`: fixed 1024x128 scalar_u8
  per-row profiler for lower-level and collection `SearchWithBuffer` rows. It
  is an optimization/profile gate, not a scale-matrix harness.
- `scripts/treedb_rabitq_1bit_profile_gate.sh`: fixed 1024x128 RaBitQ/scalar_u8
  gate with row selectors, summaries, pprof artifacts, and no-promote rules. It
  is the authoritative RaBitQ guardrail/profile workflow today.
- `scripts/bench_vector_db_compare.sh` and `benchmarks/vector_db_compare/`:
  persistent database-tier comparison for TreeDB exact column_graph, scalar_u8
  quantized modes, Vectorlite, pgvector, and MongoDB. It supports arbitrary
  `DOCS`/`DIMS`; pgvector is PostgreSQL system-level context, not in-process
  `SearchWithBuffer`.

## Fixture matrix

Run the same HNSW/search shape unless an issue explicitly records why a cell is
unavailable or too expensive:

| fixture id | docs | dims | M | efConstruction | efSearch | topK | query stream |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1k_x_128` | 1000 | 128 | 16 | 128 | 128 | 10 | 16 for Go benches; 10000 for DB harness unless reduced |
| `10k_x_128` | 10000 | 128 | 16 | 128 | 128 | 10 | same |
| `100k_x_128` | 100000 | 128 | 16 | 128 | 128 | 10 | same |
| `10k_x_384` | 10000 | 384 | 16 | 128 | 128 | 10 | same |
| `10k_x_768` | 10000 | 768 | 16 | 128 | 128 | 10 | same |
| `10k_x_1536` | 10000 | 1536 | 16 | 128 | 128 | 10 | same |

Collect c=1 and c=8 rows. For Go benchmarks use `CPU_LIST=1,8`; for
`bench_vector_db_compare.sh` use `SEARCH_CONCURRENCY=8` because the harness
always emits c=1 plus the requested concurrency list.

Recommended run-root layout:

```text
/tmp/gomap_2490_vector_crossover_<date>/
  manifest.md
  exact_usearch/<fixture>/README.md bench.txt
  db_compare/<fixture>/README.md comparison.md *.json dataset/
  rabitq_gate/<optional fixed-gate run>/context.txt summary.md summary.tsv ...
```

## Exact FP32 + USearch command contract

Use this script where possible for same-process Go benchmark rows and USearch
controls. It bootstraps USearch headers/libs when `USEARCH_ROOT` is unset.

```sh
RUN_ROOT=${RUN_ROOT:-/tmp/gomap_2490_vector_crossover_$(date +%Y%m%d_%H%M%S)}

run_exact_usearch_fixture() {
  local id=$1 docs=$2 dims=$3
  RUN_DIR="$RUN_ROOT/exact_usearch/$id" \
  TREEDB_VECTOR_BENCH_DOCS="$docs" \
  TREEDB_VECTOR_BENCH_DIMS="$dims" \
  TREEDB_VECTOR_BENCH_M=16 \
  TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 \
  TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 \
  CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare/(TreeDB_SearchWithBuffer|TreeDB_SearchWithBufferParallel|TreeDB_CollectionSearchVectorIndexWithBuffer|TreeDB_CollectionSearchVectorIndexNoDocsOneShot|USearch_Search|USearch_SearchParallel)$' \
  scripts/bench_vector_search_compare.sh
}

run_exact_usearch_fixture 1k_x_128 1000 128
run_exact_usearch_fixture 10k_x_128 10000 128
run_exact_usearch_fixture 100k_x_128 100000 128
run_exact_usearch_fixture 10k_x_384 10000 384
run_exact_usearch_fixture 10k_x_768 10000 768
run_exact_usearch_fixture 10k_x_1536 10000 1536
```

Accepted exact rows should include `TreeDB_SearchWithBuffer`,
`TreeDB_SearchWithBufferParallel`, and, when reporting collection API behavior,
`TreeDB_CollectionSearchVectorIndexWithBuffer` and/or
`TreeDB_CollectionSearchVectorIndexNoDocsOneShot`. Keep with-document rows out
of no-document high-QPS claims. USearch rows are `USearch_Search` and
`USearch_SearchParallel`, a pure in-memory external ANN baseline using the same
synthetic vectors and HNSW parameters.

## scalar_u8 command contract

For scale fixtures, use the persistent DB comparison harness and keep it labeled
as the `cmd/treedb_vector_search_demo` / database-tier boundary, not the fixed
per-row `SearchWithBuffer` profiling gate.

```sh
RUN_ROOT=${RUN_ROOT:-/tmp/gomap_2490_vector_crossover_$(date +%Y%m%d_%H%M%S)}

run_scalar_db_fixture() {
  local id=$1 docs=$2 dims=$3
  RUN_DIR="$RUN_ROOT/db_compare/$id" \
  BACKENDS=treedb_column_graph,treedb_column_graph_scalar_u8_quantized_only,treedb_column_graph_scalar_u8_quantized_rerank \
  DOCS="$docs" DIMS="$dims" QUERIES=10000 VALIDATE_QUERIES=64 \
  TOP_K=10 M=16 EF_CONSTRUCTION=128 EF_SEARCH=128 \
  SEARCH_CONCURRENCY=8 \
  TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME=embedding.scalar_u8.fast \
  TREEDB_QUANTIZED_RERANK_CANDIDATES=32 \
  TREEDB_QUANTIZED_MIN_RECALL=0 \
  scripts/bench_vector_db_compare.sh
}

run_scalar_db_fixture 1k_x_128 1000 128
run_scalar_db_fixture 10k_x_128 10000 128
run_scalar_db_fixture 100k_x_128 100000 128
run_scalar_db_fixture 10k_x_384 10000 384
run_scalar_db_fixture 10k_x_768 10000 768
run_scalar_db_fixture 10k_x_1536 10000 1536
```

For scalar_u8 optimization/profile attribution only, use the existing fixed
1024x128 workflow:

```sh
RUN_ROOT=${RUN_ROOT:-/tmp/gomap_2490_vector_crossover_$(date +%Y%m%d_%H%M%S)}
RUN_DIR="$RUN_ROOT/scalar_u8_fixed_profile" ROWS=all \
  BENCHTIME=100000x TIMING_COUNT=5 PROFILE_COUNT=1 GOMAXPROCS=8 GOWORK=off \
  scripts/treedb_quantized_buffered_row_profiles.sh
```

## RaBitQ `rabitq_1bit` command contract

For database-tier scale fixtures, use the persistent DB comparison harness with
explicit RaBitQ aliases so these rows stay separate from scalar_u8 evidence:

```sh
RUN_ROOT=${RUN_ROOT:-/tmp/gomap_2490_vector_crossover_$(date +%Y%m%d_%H%M%S)}

run_rabitq_db_fixture() {
  local id=$1 docs=$2 dims=$3
  RUN_DIR="$RUN_ROOT/db_compare_rabitq/$id" \
  BACKENDS=treedb_column_graph,treedb_column_graph_rabitq_1bit_quantized_only,treedb_column_graph_rabitq_1bit_quantized_rerank \
  DOCS="$docs" DIMS="$dims" QUERIES=10000 VALIDATE_QUERIES=64 \
  TOP_K=10 M=16 EF_CONSTRUCTION=128 EF_SEARCH=128 \
  SEARCH_CONCURRENCY=8 \
  TREEDB_RABITQ_QUANTIZED_INDEX_NAME=embedding.rabitq_1bit.fast \
  TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES=32 \
  TREEDB_RABITQ_QUANTIZED_MIN_RECALL=0 \
  scripts/bench_vector_db_compare.sh
}
```

For RaBitQ scorer/profile attribution only, use the fixed profile gate. Never
substitute future-codec or no-promote rows for current `rabitq_1bit` v1:

```sh
RUN_ROOT=${RUN_ROOT:-/tmp/gomap_2490_vector_crossover_$(date +%Y%m%d_%H%M%S)}
RUN_DIR="$RUN_ROOT/rabitq_gate/current_main_fixed_1024x128" \
  ROWS=claim_rerank \
  PROFILE_ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8,rabitq_collection_quantized_rerank32_c1,rabitq_collection_quantized_rerank32_c8 \
  BENCHTIME=100000x TIMING_COUNT=5 PROFILE_COUNT=1 \
  GOMAXPROCS=8 GOWORK=off \
  scripts/treedb_rabitq_1bit_profile_gate.sh
```

## pgvector/PostgreSQL context command contract

pgvector is a system-level PostgreSQL row. It is not the same boundary as
TreeDB's in-process `SearchWithBuffer`, and its setup/build/client/server costs
must be described exactly as emitted by the harness.

```sh
RUN_ROOT=${RUN_ROOT:-/tmp/gomap_2490_vector_crossover_$(date +%Y%m%d_%H%M%S)}

run_pgvector_fixture() {
  local id=$1 docs=$2 dims=$3
  RUN_DIR="$RUN_ROOT/db_compare_pgvector/$id" \
  BACKENDS=treedb_column_graph,treedb_column_graph_scalar_u8_quantized_only,treedb_column_graph_scalar_u8_quantized_rerank,pgvector \
  DOCS="$docs" DIMS="$dims" QUERIES=10000 VALIDATE_QUERIES=64 \
  TOP_K=10 M=16 EF_CONSTRUCTION=128 EF_SEARCH=128 \
  SEARCH_CONCURRENCY=8 \
  TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME=embedding.scalar_u8.fast \
  TREEDB_QUANTIZED_RERANK_CANDIDATES=32 \
  TREEDB_QUANTIZED_MIN_RECALL=0 \
  scripts/bench_vector_db_compare.sh
}

# #2493 should run at least these context rows when the environment supports it.
run_pgvector_fixture 1k_x_128 1000 128
run_pgvector_fixture 10k_x_128 10000 128
run_pgvector_fixture 10k_x_768 10000 768
# Add 100k_x_128, 10k_x_384, and/or 10k_x_1536 if feasible on the same host.
```

If `PGVECTOR_DSN` is unset, the script starts a temporary
`pgvector/pgvector:pg16` Docker container. For external Postgres, set
`PGVECTOR_DSN`, use a disposable `PGVECTOR_SCHEMA`, and only set
`PGVECTOR_ALLOW_DROP_SCHEMA=true`/`PGVECTOR_DROP_SCHEMA_AFTER=true` for schemas
that can be safely removed. The harness uses pgvector full-vector HNSW; it does
not benchmark pgvector `halfvec`, `binary_quantize`, SQL rerank, custom byte
scoring, or custom operator classes.

## Required metrics and counters

Every accepted table must include:

- commit (`origin/main` SHA), branch, Go version, OS/arch, hardware, run path,
  command, fixture, M, efConstruction, efSearch, topK, query count/stream, and
  concurrency (`c=1`, `c=8` separately);
- `ns/op`, `ops/sec`, `B/op`, `allocs/op` for Go benchmark rows;
- DB harness latency/throughput fields from each backend JSON plus
  `comparison.md`;
- recall@K versus exact where relevant;
- exact route counters: `search_route_hnsw_search_pack/search=1`,
  `hnsw_search_pack_active/search=1`, docs/fallback/scratch/quantized counters
  zero, exact vector/norm bytes as reported;
- scalar_u8/RaBitQ quantized counters: `search_route_quantized_only/search` or
  `search_route_quantized_rerank/search`, `quantized_code_B/search`,
  `quantized_code_B/vector`, `quantized_asset_B/vector`,
  `quantized_asset_unavailable/search`, invalid/stale/closed/missing counters,
  fallback counters, and recall;
- `quantized_only`: exact vector/norm bytes and exact rerank score calls must be
  zero;
- `quantized_rerank`: rerank candidates and exact score calls must be bounded by
  the configured shortlist, normally 32;
- collection buffered rows: prepared-cache hits/builds/misses/waits/errors,
  `open_searcher_calls/op=0`, `open_setup_in_timed_loop=0`, and caller-owned
  result buffer allocation status;
- USearch setup path/version and whether automatic bootstrap or `USEARCH_ROOT`
  was used;
- pgvector DSN mode (Docker temporary vs external), image/server version when
  available, schema/table, and explicit system-boundary caveat.

## Quiet-host and evidence rejection rules

Before running, capture `git status --short`, `git rev-parse HEAD`, `go version`,
`uptime`, and top CPU processes in `manifest.md` or rely on script-generated
`README.md`/`context.txt` where available. Reject evidence for claims when:

- load averages are high relative to core count or rise materially during the
  run;
- other benchmark/test/compiler/database jobs are visible;
- thermal throttling, laptop power changes, indexing, backups, Docker churn, or
  VM contention are suspected;
- c=1/c=8 variance is unexplained or rows contradict guardrail counters;
- baseline/candidate or comparator rows are not same-host for relative claims.

Contaminated runs may be archived as smoke/context only, but must be labeled
`no-claim` and excluded from accepted speedup/crossover conclusions.

## USearch setup caveats

`scripts/bench_vector_search_compare.sh` downloads USearch release artifacts when
possible. On Linux it needs `dpkg-deb` unless `USEARCH_ROOT` points to an install
containing `usearch.h` and `libusearch_c.so`. On macOS it expects
`usearch.h` and `libusearch_c.dylib`; the script sets `CGO_CFLAGS`,
`CGO_LDFLAGS` with `-Wl,-rpath,<libdir>`, and `DYLD_LIBRARY_PATH`. If headers or
libraries are supplied manually, record `USEARCH_ROOT`, include dir, lib dir,
version, OS, and arch from the generated `README.md`.

## Reporting template

For each fixture, report one table per boundary:

1. TreeDB exact FP32 + USearch Go benchmark rows.
2. TreeDB scalar_u8 database-tier rows (`exact`, `quantized_only`,
   `quantized_rerank`) from `bench_vector_db_compare.sh`.
3. RaBitQ fixed-gate status or approved scale-harness rows, explicitly labeled.
4. pgvector/PostgreSQL context rows.

Do not combine exact FP32, scalar_u8, RaBitQ, USearch, and pgvector into a single
undifferentiated winner table. Any missing cell should state `unavailable`,
`too expensive`, or `contaminated/no-claim` with the artifact path and reason.
