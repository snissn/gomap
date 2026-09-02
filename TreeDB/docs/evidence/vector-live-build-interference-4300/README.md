# Native live-build interference decision (#4300)

This local diagnostic isolates HNSW construction and immutable search-view publication from HTTP and storage. It explains why the controlled H-B R9 candidate cannot satisfy both the insert and concurrent-search gates by tuning construction workers alone. AWS H-B remains authoritative.

## Reproduce

```sh
GOMAXPROCS=32 \
TREEDB_VECTOR_MIXED_MODE=current \
TREEDB_VECTOR_MIXED_WORKERS=1 \
go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkVectorIndexMixedSearchInsert4300$' \
  -benchtime=1x -count=3
```

Omit `TREEDB_VECTOR_MIXED_WORKERS` for the production worker choice. A worker limit of one serializes the frozen-prefix planner without changing its topology. Supported diagnostic modes are `current`, `serial-reciprocal`, `no-publish`, `live-delta`, and `live-delta-cutover`. The last two route inserted rows through the native live-delta graph; `live-delta-cutover` inserts enough rows to cross the production fold bound once.

The default 200 ms pace per 100-row batch models the 500 rows/s H-B stage and caps offered throughput near 500 rows/s. Set `TREEDB_VECTOR_MIXED_BATCH_PACE=0s` for an unpaced control. The local throughput result is directional under this pacing; AWS H-B is authoritative.

Fixture: deterministic 10,000-row 768D FP32 cosine base, 2,000 live inserts in 100-row batches, HNSW M16/efConstruction128/efSearch64, concurrency 10, topK100. Override the inserted-row count with `TREEDB_VECTOR_MIXED_INSERT_ROWS`; for example, `10000` grows the live delta to 50% of the final 20,000-row graph and approximates the R21 growth ratio. Host: Intel i5-11400F (6 cores/12 threads), Linux, with `GOMAXPROCS=32` to preserve the EC2 scheduler shape.

Set `TREEDB_VECTOR_MIXED_ACCOUNT_SEARCH_WORK=1` only for diagnostic attribution. It reports the percentage of searches that use the delta, initial and terminal topK/efSearch budgets, retry incidence and depth, resumed-retry percentage, delta visits per query, and whether retrying changes merged topK. This mode performs extra accounting and an initial-result fingerprint; do not use its QPS as production-mode evidence.

Example R21-ratio attribution run:

```sh
TREEDB_VECTOR_MIXED_MODE=live-delta \
TREEDB_VECTOR_MIXED_INSERT_ROWS=10000 \
TREEDB_VECTOR_MIXED_ACCOUNT_SEARCH_WORK=1 \
go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkVectorIndexMixedSearchInsert4300$' \
  -benchtime=1x -count=1
```

## Three-repeat medians

| Construction budget | Retained QPS | Insert rows/s | Mixed p99 |
| --- | ---: | ---: | ---: |
| current | 79.73% | 246.5 | 6.803 ms |
| 1 | 85.89% | 174.2 | 5.921 ms |
| 2 | 83.31% | 199.5 | 6.008 ms |

Single-run controls: worker 4 retained 77.64% at 240.0 rows/s; worker 8 retained 81.84% at 258.4 rows/s; skipping publication retained 85.64% at 262.3 rows/s. These controls are directional because the host was busy.

## Attribution and decision

In the current mixed-only profile, AVX-512 FP32 dot product is 71.31% flat CPU; concurrent search is 86.84% cumulative, frozen planning 6.67%, reciprocal linking 4.65%, and view publication 0.58%. Publication allocates about 38 MiB over 2,000 inserts, mostly dirty adjacency clones, but skipping it recovers only about four retained-QPS points. Mutex delay is negligible; block delay is the expected construction `WaitGroup`.

The only bounded rule that reaches the local 85% retained-QPS floor loses 29.3% of insert throughput under the paced harness. The unbounded AWS R9 candidate already achieved only 398.68 rows/s against the 475 rows/s floor. No construction-worker cap can plausibly meet both frozen gates. Follow-up #4301 owns one immutable searchable base plus one bounded live delta and atomic reconciliation; #4248 remains the final controlled AWS gate.
