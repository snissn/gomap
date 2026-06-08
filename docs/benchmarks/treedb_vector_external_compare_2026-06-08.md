# TreeDB Vector External Comparison Snapshot (10k x 1536)

This is a scoped local benchmark snapshot comparing TreeDB exact FP32,
TreeDB scalar_u8 rerank32, USearch f32 HNSW, and PostgreSQL+pgvector HNSW on
one synthetic `10k x 1536` vector-search shape.

Treat these rows as same-host comparison evidence, not isolated public latency
claims. The run used a local developer laptop with non-zero background load.

## Context

- Artifact root: `/tmp/gomap_exact_scalar_usearch_pgvector_20260607_160605`
- Commit: `840bff52062e3b2c1c2818cde7d94efe3b9a45ce`
- Host: Apple M3, 8 CPUs, macOS/Darwin arm64
- Go: `go version go1.26.0 darwin/arm64`
- Shape: `docs=10000`, `dims=1536`, `queries=10000`, `validate_queries=64`, `topK=10`
- HNSW knobs: `M=16`, `efConstruction=128`, `efSearch=128`
- TreeDB scalar rerank: `scalar_u8`, `quantized_rerank`, `rerank_candidates=32`
- Benchmark lock: `/tmp/gomap_2556_benchmark.lock`
- Load averages:
  - DB/pgvector start: `3.38 2.87 2.37`
  - DB/pgvector end: `5.90 3.82 2.81`
  - USearch start: `3.72 3.58 2.80`
  - USearch end: `4.23 3.72 2.88`

## Search Results

| system | recall@10 | c=1 avg / p95 / QPS | c=8 avg / p95 / QPS |
| --- | ---: | ---: | ---: |
| TreeDB exact FP32 | `0.9859` (`631/640`) | `418µs` / `562µs` / `2,391.1` | `852µs` / `1.77ms` / `9,386.0` |
| TreeDB scalar_u8 rerank32 | `0.9828` (`629/640`) | `165µs` / `235µs` / `6,071.9` | `511µs` / `833µs` / `15,571.3` |
| USearch f32 HNSW | `0.8938` (`572/640`) | `725µs` / `1.38ms`* / `1,380.0` | `160µs` / n/a / `6,258.8` |
| PostgreSQL+pgvector HNSW | `0.9859` (`631/640`) | `2.67ms` / `5.08ms` / `373.9` | `4.29ms` / `8.25ms` / `1,864.2` |

`*` USearch p95 is from per-query Python calls. USearch c=1/c=8 QPS and
avg/query are from batch searches with `threads=1/8`, so the p95 row is not
fully comparable to the batch QPS rows. USearch is an in-memory library HNSW
comparator, not a persistent DB/server row.

## Build / Storage Context

| system | insert/load | index build | storage/index |
| --- | ---: | ---: | ---: |
| TreeDB exact FP32 | `5.106s` | `16.362s` | `126.87MiB` |
| TreeDB scalar_u8 rerank32 | `5.025s` | `16.687s` | `141.91MiB` |
| USearch f32 HNSW | n/a | `4.055s` | `60.01MiB` |
| PostgreSQL+pgvector HNSW | `4.092s` | `12.298s` | `158.31MiB` |

## TreeDB Guardrails

TreeDB DB-demo rows used the no-document search boundary:

- documents fetched: `0`
- response-owned result allocations: `0`
- route/fallback/scratch counters: `0`
- scalar_u8 rerank exact score calls/search: `32`
- scalar_u8 rerank vector bytes/search: `196,608`
- scalar_u8 rerank norm bytes/search: `128`

pgvector and USearch are full-vector HNSW comparators and do not expose TreeDB's
quantized-route guardrail counters.

## Commands

TreeDB exact/scalar and pgvector used the DB comparator harness:

```sh
RUN_DIR=/tmp/gomap_exact_scalar_usearch_pgvector_20260607_160605 \
BACKENDS=treedb_column_graph,treedb_column_graph_scalar_u8_quantized_rerank,pgvector \
DOCS=10000 DIMS=1536 QUERIES=10000 VALIDATE_QUERIES=64 VALIDATE_DOCS=16 TOP_K=10 \
SEARCH_CONCURRENCY=1,8 M=16 EF_CONSTRUCTION=128 EF_SEARCH=128 TREEDB_COLUMN_GRAPH_EF_SEARCH=128 \
MIN_RECALL=0 TREEDB_QUANTIZED_MIN_RECALL=0 TREEDB_QUANTIZED_RERANK_MIN_RECALL=0 \
TREEDB_QUANTIZED_RERANK_CANDIDATES=32 TREEDB_VALIDATION_EXACT_SOURCE=dataset \
PGVECTOR_DOCKER=auto PGVECTOR_DROP_SCHEMA_AFTER=true PGVECTOR_ALLOW_DROP_SCHEMA=true \
NUMPY_PACKAGE=numpy==2.3.5 \
lockf /tmp/gomap_2556_benchmark.lock scripts/bench_vector_db_compare.sh
```

USearch used the same exported dataset from the DB comparator artifact, the
Python `usearch` package, `metric=cos`, `dtype=f32`, `connectivity=16`,
`expansion_add=128`, and `expansion_search=128`. Raw files in the artifact root:

- `selected_summary.md`
- `comparison.md`
- `treedb_column_graph.json`
- `treedb_column_graph_scalar_u8_quantized_rerank.json`
- `pgvector.json`
- `usearch.json`
- `usearch_batch.json`
- `usearch_dataset_bench.py`
- `usearch_batch_probe.py`
