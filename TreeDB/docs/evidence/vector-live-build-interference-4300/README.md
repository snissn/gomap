# Native live-build interference decision (#4300)

This local diagnostic isolates HNSW construction and immutable search-view publication from HTTP and storage. It explains why the controlled H-B R9 candidate cannot satisfy both the insert and concurrent-search gates by tuning construction workers alone. AWS H-B remains authoritative.

## Reproduce

```sh
GOMAXPROCS=32 \
TREEDB_VECTOR_MIXED_MODE=current \
TREEDB_VECTOR_MIXED_WORKERS=1 \
TREEDB_VECTOR_MIXED_PROFILE_DIR=/path/to/profiles \
go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkVectorIndexMixedSearchInsert4300$' \
  -benchtime=1x -count=3
```

Omit `TREEDB_VECTOR_MIXED_WORKERS` for the production worker choice. Supported diagnostic modes are `current`, `serial-plan`, `serial-reciprocal`, `all-serial`, and `no-publish`. A profile directory captures mixed-interval CPU plus before/after allocation, heap, mutex, and block profiles.

Fixture: deterministic 10,000-row 768D FP32 cosine base, 2,000 live inserts in 100-row batches, HNSW M16/efConstruction128/efSearch64, concurrency 10, topK100. Host: Intel i5-11400F (6 cores/12 threads), Linux, with `GOMAXPROCS=32` to preserve the EC2 scheduler shape.

## Three-repeat medians

| Construction budget | Retained QPS | Insert rows/s | Mixed p99 |
| --- | ---: | ---: | ---: |
| current | 79.73% | 246.5 | 6.803 ms |
| 1 | 85.89% | 174.2 | 5.921 ms |
| 2 | 83.31% | 199.5 | 6.008 ms |

Single-run controls: worker 4 retained 77.64% at 240.0 rows/s; worker 8 retained 81.84% at 258.4 rows/s; skipping publication retained 85.64% at 262.3 rows/s. These controls are directional because the host was busy.

## Attribution and decision

In the current mixed-only profile, AVX-512 FP32 dot product is 71.31% flat CPU; concurrent search is 86.84% cumulative, frozen planning 6.67%, reciprocal linking 4.65%, and view publication 0.58%. Publication allocates about 38 MiB over 2,000 inserts, mostly dirty adjacency clones, but skipping it recovers only about four retained-QPS points. Mutex delay is negligible; block delay is the expected construction `WaitGroup`.

The only bounded rule that reaches the local 85% retained-QPS floor loses 29.3% of insert throughput. The unbounded AWS R9 candidate already achieved only 398.68 rows/s against the 475 rows/s floor. No construction-worker cap can plausibly meet both frozen gates. Follow-up #4301 owns one immutable searchable base plus one bounded live delta and atomic reconciliation; #4248 remains the final controlled AWS gate.
