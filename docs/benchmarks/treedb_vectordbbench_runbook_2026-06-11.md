# TreeDB VectorDBBench Runbook and Smoke Evidence (2026-06-11)

This note documents the TreeDB VectorDBBench integration after the service and
adapter stack merged. It is a reproducibility/runbook document plus a functional
smoke artifact. It is **not** claim-quality latency/QPS evidence.

For the repeatable artifact harness and route-proof sidecar contract used by
follow-up benchmark issues, see
[`treedb_vectordbbench_artifact_harness.md`](treedb_vectordbbench_artifact_harness.md).

## Canonical Published Benchmark

Use the [August 21 Cohere 1M report](treedb_vectordbbench_cohere1m_c6i_dense_curve_2026-08-21.md)
for current claim-quality VDBBench results. Its two canonical TreeDB curves are:

- FP32 HNSW graph traversal.
- Scalar-u8 graph traversal with FP32 reranking, limited to the non-dominated
  recall/QPS points.

Scalar-u8-only and dominated rerank screening cells remain supporting evidence,
not headline curves. This June 11 document remains the integration and smoke
runbook.

## Merged Stack

| issue | PR | merge commit | scope |
| --- | --- | --- | --- |
| #2570 | `snissn/gomap#2583` | `28fa5edda27b52dadf7cff060683f71666dd470a` | TreeDB document-service benchmark lifecycle, no-document vector-index route, and `treedb-client` support |
| #2571 | `snissn/vectordbbench#1` | `b22d106df04b8cf7f903fafa49c99cea01a27848` | VectorDBBench TreeDB client/config/CLI seam |
| #2572 | `snissn/vectordbbench#3` | `c1c763c0dcd97c489c2b107fd7060374f76d09b8` | named exact/scalar_u8/RaBitQ variants and route/counter guardrails |

`snissn/vectordbbench#2` was the original stacked #2572 PR. It was auto-closed
when its base branch merged and is superseded by `snissn/vectordbbench#3`.

## Measurement Boundaries

Keep these lanes separate:

- **VDBBench TreeDB rows** measure Python + `treedb-client` + HTTP/JSON +
  TreeDB document service + TreeDB vector-index work.
- **Haystack route** remains exact dense document scoring through
  `/v1/indexes/{index}/search/vector`; it is not ANN and should not be used for
  TreeDB ANN claims.
- **Native Go no-document rows** (`SearchWithBuffer` / reusable searcher) remain
  the high-QPS `0 B/op`, `0 allocs/op` evidence lane when Go benchmarks emit
  allocation metrics.
- **USearch rows** are in-memory/library comparators.
- **pgvector rows** are PostgreSQL/server comparators.
- **RaBitQ v1 rows** are experimental/compact evidence. Do not present them as
  exact-like recall.

The current exact-like quantized TreeDB lane is scalar_u8 + exact rerank:
`query_mode="quantized_rerank"`, an explicit `quantized_index_name`, and
`quantized_rerank_candidates=32` for the rerank32 baseline.

## Local Service Setup

Use a fresh TreeDB data directory for managed VDBBench runs. Existing
`column_graph` benchmark indexes fail closed for in-place `drop_old` reset, so
shared services should use a unique `--index-name` per run.

```sh
RUN_DIR=/tmp/treedb_vdbbench_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_DIR"

go run ./cmd/treedb-document-service \
  -dir "$RUN_DIR/treedb-data" \
  -addr 127.0.0.1:7120 \
  -profile command_wal_durable
```

Health check:

```sh
curl -fsS http://127.0.0.1:7120/v1/health
```

## Local Python Setup

For local development from checkouts:

```sh
# gomap checkout provides treedb-client
export TREEDB_CLIENT_SRC=/path/to/gomap/clients/python/treedb_client/src

# vectordbbench checkout at or after c1c763c0dcd97c489c2b107fd7060374f76d09b8
cd /path/to/vectordbbench
export PYTHONPATH="$PWD:$TREEDB_CLIENT_SRC"
```

The packaged VDBBench extra also pins `treedb-client` to an immutable gomap
commit containing the merged service/client contract.

## VDBBench Commands

List TreeDB commands:

```sh
python -m vectordb_bench.cli.vectordbbench --help | grep -i treedb
```

Dry-run exact FP32 no-document command:

```sh
PYTHONPATH="$PWD:$TREEDB_CLIENT_SRC" \
python -m vectordb_bench.cli.vectordbbench treedbcolumngraphexact \
  --base-url http://127.0.0.1:7120 \
  --index-name treedb_exact_$(date +%s) \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --skip-load \
  --skip-search-serial \
  --skip-search-concurrent \
  --dry-run
```

Dry-run scalar_u8 rerank32 command:

```sh
PYTHONPATH="$PWD:$TREEDB_CLIENT_SRC" \
python -m vectordb_bench.cli.vectordbbench treedbscalaru8rerank \
  --base-url http://127.0.0.1:7120 \
  --index-name treedb_scalar_u8_$(date +%s) \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --quantized-index-name embedding.scalar_u8.fast \
  --quantized-rerank-candidates 32 \
  --skip-load \
  --skip-search-serial \
  --skip-search-concurrent \
  --dry-run
```

For a full VDBBench run, remove the `--skip-*`/`--dry-run` flags and select the
same dataset/case settings that you use for comparator databases. Record the
case ID, dimensions, topK, metric, load concurrency, search concurrency list,
duration, and service profile next to results. Do not compare full-run results
to native Go allocation rows.

Optional experimental RaBitQ v1 command:

```sh
PYTHONPATH="$PWD:$TREEDB_CLIENT_SRC" \
python -m vectordb_bench.cli.vectordbbench treedbrabitq1bitexperimental \
  --base-url http://127.0.0.1:7120 \
  --index-name treedb_rabitq_v1_experimental_$(date +%s) \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --query-mode quantized_only
```

Keep this row labeled experimental and recall-limited unless a future versioned
codec/design issue changes that contract.

## Smoke Evidence

Artifact root: `/tmp/gomap_2573_vdbbench_smoke_20260610_202907`

Context:

- gomap commit: `e30ac7384a579d301e8a384fab6977e8e8367879`
- vectordbbench commit: `c1c763c0dcd97c489c2b107fd7060374f76d09b8`
- Go: `go version go1.26.0 darwin/arm64`
- Python: `Python 3.14.0`
- OS/arch: `Darwin arm64`
- Service profile: `command_wal_durable`
- Service command used a fresh artifact-owned data directory.

Validation commands captured in the artifact:

```sh
PYTHONPATH=. uv run --no-sync --with pytest --with click --with pydantic --with pyyaml --with environs --with pandas --with polars --with pyarrow --with psutil --with pytz --with tqdm --with plotly --with ujson --with hdrhistogram --with scikit-learn --with s3fs --with oss2 python -m pytest tests/test_treedb_cli.py tests/test_db_client_resolution.py -q
```

Result: `56 passed` (`vectordbbench_tests.txt`).

The smoke script instantiated the merged VDBBench TreeDB client classes against
a local TreeDB document service, loaded four 2-D vectors into fresh exact and
scalar indexes, optimized, searched through the VDBBench client API, and then
queried the service response counters directly.

| row | result IDs | route | no docs | docs fetched | quantized scorer | quantized score calls | rerank exact calls |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| exact FP32 smoke | `101,103` | `exact_hnsw_search_pack_v1` | true | 0 | 0 | 0 | 0 |
| scalar_u8 rerank32 smoke | `101,103` | `quantized_rerank` | true | 0 | 1 | 7 | 4 |

The scalar smoke loaded only four documents, so the route correctly reranked the
available shortlist (`4`) rather than the configured upper bound (`32`). This
proves the shortlist-bounded exact-read guardrail, not a production recall or
throughput number.

Additional artifacts:

- `README.md`
- `context.txt`
- `health.json`
- `service.log`
- `vdbbench_treedb_smoke.py`
- `vdbbench_treedb_smoke.json`
- `vectordbbench_tests.txt`
- `cli_exact_dry_run.txt`
- `cli_scalar_dry_run.txt`

## Result Reporting Template

When publishing a full VDBBench run, include a table like this and keep exact,
scalar_u8, and RaBitQ separate:

| row | case/dims/docs | topK | metric | concurrency | recall/NDCG | avg | p95 | p99 | QPS | load | optimize | route proof |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| TreeDB exact FP32 |  |  | cosine |  |  |  |  |  |  |  |  | `route=exact_hnsw_search_pack_v1`, `documents_fetched=0` |
| TreeDB scalar_u8 rerank32 |  |  | cosine |  |  |  |  |  |  |  |  | `route=quantized_rerank`, `quantized_scorer_active=1`, exact reads `<=32` |
| TreeDB RaBitQ v1 experimental |  |  | cosine |  |  |  |  |  |  |  |  | experimental; do not combine with scalar_u8 |

Also state:

- whether the service used a fresh data directory or a unique index name;
- TreeDB service profile (`command_wal_durable`, `command_wal_relaxed`, or
  benchmark-only `bench`);
- Python, Go, OS/CPU context, and host-load caveats;
- that VDBBench rows include Python/client/service overhead and do not emit Go
  `B/op`/`allocs/op` metrics.

## Relationship To Existing Evidence

The June 8 external comparison remains the current same-host comparison snapshot
for TreeDB exact FP32, scalar_u8 rerank32, USearch, and pgvector on `10k x 1536`:

- [TreeDB Vector External Comparison Snapshot](treedb_vector_external_compare_2026-06-08.md)

Use that report for current exact/scalar_u8 vs USearch/pgvector comparison
context. Use this runbook to reproduce the new VDBBench service/client boundary.
