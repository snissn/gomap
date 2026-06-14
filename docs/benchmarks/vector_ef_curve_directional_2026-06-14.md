# Directional Vector EF Curve - 2026-06-14

This is a directional, noisy-host EF curve run to inspect whether equal
`ef_search` values map to equal recall/throughput across engines. It is not a
publication-quality result.

Artifact root:

```text
/mnt/fast4tb/tmp/gomap_vector_ef_curve_20260614_072859
```

Primary files:

- `curve.csv`
- `curve.md`
- `treedb_column_graph.json`
- `treedb_column_graph_scalar_u8_quantized_rerank.json`
- `pgvector.json`

Run shape:

| Field | Value |
| --- | --- |
| Dataset | synthetic TreeDB-exported vectors |
| Documents | 10,000 |
| Dimensions | 768 |
| Queries | 2,000 |
| Validation queries | 20 |
| TopK | 10 |
| Search concurrency | 32 plus serial baseline |
| HNSW M / efConstruction | 16 / 128 |
| Host | noisy local workstation |

Backends:

- TreeDB `column_graph` exact/default: `ef=8,12,16,24,32,48,64,96,128`
- TreeDB `column_graph` scalar_u8 `quantized_rerank`, rerank32:
  `ef=8,12,16,24,32,48,64,96,128`
- PostgreSQL+pgvector HNSW: `ef=10,12,16,24,32,48,64,96,128`

Pgvector `ef=8` was not included because pgvector returned fewer than `top_k=10`
results at `ef_search=8`. Treat pgvector `ef=10` as the lowest comparable point
in this run.

## Concurrency 32

| Backend | Mode | ef | Recall@10 | QPS | P95 us | P99 us | Avg candidates |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB | scalar_u8 rerank32 | 8 | 0.8950 | 116219.0 | 154 | 275 | 212.6 |
| TreeDB | scalar_u8 rerank32 | 12 | 0.9150 | 105289.4 | 162 | 547 | 234.3 |
| TreeDB | scalar_u8 rerank32 | 16 | 0.9200 | 90055.8 | 194 | 384 | 272.8 |
| TreeDB | scalar_u8 rerank32 | 24 | 0.9250 | 75962.0 | 208 | 1030 | 336.1 |
| TreeDB | scalar_u8 rerank32 | 32 | 0.9250 | 63272.6 | 277 | 1171 | 387.3 |
| TreeDB | scalar_u8 rerank32 | 48 | 0.9250 | 54089.8 | 301 | 5427 | 461.5 |
| TreeDB | scalar_u8 rerank32 | 64 | 0.9250 | 49793.0 | 324 | 7939 | 513.5 |
| TreeDB | scalar_u8 rerank32 | 96 | 0.9400 | 42367.4 | 387 | 4320 | 593.4 |
| TreeDB | scalar_u8 rerank32 | 128 | 0.9400 | 37594.3 | 429 | 3731 | 666.0 |
| TreeDB | exact/default | 8 | 0.9250 | 88907.3 | 213 | 351 | 210.8 |
| TreeDB | exact/default | 12 | 0.9250 | 79858.0 | 256 | 678 | 232.6 |
| TreeDB | exact/default | 16 | 0.9250 | 68166.2 | 281 | 583 | 270.8 |
| TreeDB | exact/default | 24 | 0.9250 | 56949.8 | 322 | 1139 | 333.5 |
| TreeDB | exact/default | 32 | 0.9250 | 51056.0 | 364 | 808 | 384.1 |
| TreeDB | exact/default | 48 | 0.9250 | 40164.1 | 487 | 3378 | 458.9 |
| TreeDB | exact/default | 64 | 0.9250 | 33612.8 | 619 | 14431 | 512.0 |
| TreeDB | exact/default | 96 | 0.9250 | 29827.6 | 567 | 7026 | 592.5 |
| TreeDB | exact/default | 128 | 0.9250 | 24330.9 | 921 | 20868 | 662.0 |
| pgvector | full-vector HNSW | 10 | 0.9300 | 6746.6 | 9280 | 13257 | n/a |
| pgvector | full-vector HNSW | 12 | 0.9300 | 8106.0 | 7438 | 10375 | n/a |
| pgvector | full-vector HNSW | 16 | 0.9300 | 7295.8 | 9056 | 12928 | n/a |
| pgvector | full-vector HNSW | 24 | 0.9300 | 7428.6 | 8095 | 10927 | n/a |
| pgvector | full-vector HNSW | 32 | 0.9300 | 6757.6 | 8876 | 12659 | n/a |
| pgvector | full-vector HNSW | 48 | 0.9300 | 6166.0 | 10060 | 15233 | n/a |
| pgvector | full-vector HNSW | 64 | 0.9300 | 6082.7 | 10287 | 13616 | n/a |
| pgvector | full-vector HNSW | 96 | 0.9300 | 5049.6 | 12116 | 17524 | n/a |
| pgvector | full-vector HNSW | 128 | 0.9300 | 4881.3 | 13239 | 18026 | n/a |

## Interpretation

- The harness now produces the right artifact shape: one build per backend, then
  multiple `ef_search` points during search.
- Equal `ef_search` is not the right comparison axis for claims. The useful view
  is QPS versus recall.
- TreeDB scalar_u8 rerank32 shows the expected tradeoff: lower ef gives lower
  recall and higher QPS; higher ef raises recall and reduces QPS.
- On this small/noisy run, TreeDB scalar_u8 reaches about `0.895` recall at
  `ef8`, `0.920` at `ef16`, `0.925` around `ef24-64`, and `0.940` at
  `ef96+`.
- Pgvector recall is flat at `0.930` across sampled ef values in this run. With
  only 20 validation queries, treat that as coarse directional evidence, not a
  precise recall curve.
- The host was noisy during the run, so absolute QPS and tail latency should be
  treated directionally. The curve shape and the repeatable artifact are the
  useful outputs.

## Reproduction

TreeDB rows were produced with:

```sh
RUN_DIR=/mnt/fast4tb/tmp/gomap_vector_ef_curve_20260614_072859 \
EF_VALUES=8,12,16,24,32,48,64,96,128 \
BACKENDS=treedb_column_graph,treedb_column_graph_scalar_u8_quantized_rerank,pgvector \
DOCS=10000 \
DIMS=768 \
QUERIES=2000 \
VALIDATE_QUERIES=20 \
VALIDATE_DOCS=8 \
SEARCH_CONCURRENCY=32 \
TOP_K=10 \
TREEDB_QUANTIZED_RERANK_CANDIDATES=32 \
TREEDB_QUANTIZED_MIN_RECALL=0 \
MIN_RECALL=0 \
scripts/bench_vector_ef_curve.sh
```

The initial combined run failed at pgvector `ef8`, so pgvector was rerun into
`pgvector_run/` with:

```sh
RUN_DIR=/mnt/fast4tb/tmp/gomap_vector_ef_curve_20260614_072859/pgvector_run \
EF_SEARCH_VALUES=10,12,16,24,32,48,64,96,128 \
BACKENDS=pgvector \
DOCS=10000 \
DIMS=768 \
QUERIES=2000 \
VALIDATE_QUERIES=20 \
SEARCH_CONCURRENCY=32 \
TOP_K=10 \
MIN_RECALL=0 \
scripts/bench_vector_db_compare.sh
```

The resulting `pgvector.json` was copied into the primary artifact root and the
combined curve was regenerated with:

```sh
python3 benchmarks/vector_db_compare/collect_ef_curve.py \
  --run-dir /mnt/fast4tb/tmp/gomap_vector_ef_curve_20260614_072859 \
  --csv /mnt/fast4tb/tmp/gomap_vector_ef_curve_20260614_072859/curve.csv \
  --markdown /mnt/fast4tb/tmp/gomap_vector_ef_curve_20260614_072859/curve.md
```
