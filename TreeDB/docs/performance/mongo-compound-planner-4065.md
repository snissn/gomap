# #4065 compound planner cursor microbenchmark

This is a narrow, local microbenchmark for the standalone gateway's compound
cursor stream. It is not a MongoDB-comparison or production-throughput claim.

## Provenance

- Base: `3eea650b65261267b412560955bc6f4c19f7ef18`
- Source capture: `45769651102040554fb953675b689ed5a70830e6`
- Benchmark source SHA-256: `f3dc03543557f397c59707eeca03c5ed065ea0fcc5f8c7a1bdc13f562198c9d4` (`TreeDB/mongo_gateway/compound_planner_test.go`)
- Host: Apple M3, darwin/arm64, Go test default `GOMAXPROCS=8`
- Command: `GOWORK=off go test ./TreeDB/mongo_gateway -run '^$' -bench BenchmarkMongoCompoundPlannerCursor -benchtime=25x -count=10 -benchmem`

Each variant builds the same 128 BSON documents outside its timer and runs the
same `tenant=t`, `score:-1`, `skip=16`, `limit=32`, `batchSize=8` command,
draining every `getMore`. Preflight asserts every batch succeeds, all 32
result IDs are the same descending `111` through `080` sequence in every
variant, and executionStats is positive/non-negative as appropriate before
timing. The variants are compound V2 stream, no-secondary-index bounded scan
plus sort, and a legacy `tenant:1` single-field alternative plus sort. Index
creation and seed insertion are excluded.

| variant | p50 ns/op | p95/p99 ns/op | p50 ops/sec | B/op range | allocs/op range |
| --- | ---: | ---: | ---: | ---: | ---: |
| compound stream | 121,544 | 126,077 / 126,077 | 8,228 | 68,128–68,768 | 1,080–1,081 |
| bounded scan + sort | 620,884 | 660,555 / 660,555 | 1,611 | 443,488–444,751 | 17,941–17,944 |
| single-field + sort | 622,908 | 683,867 / 683,867 | 1,605 | 479,050–479,868 | 18,349–18,351 |

Percentiles use the 10 observed per-variant samples: p50 is the midpoint of
the fifth and sixth sorted samples; p95/p99 use the conservative nearest-rank
maximum at this sample size. These are local microbenchmark observations, not
release SLOs.

| variant | candidates examined | documents materialized | materialized bytes |
| --- | ---: | ---: | ---: |
| compound stream | 48 | 32 | 2,112 |
| bounded scan + sort | 128 | 128 | 0 |
| single-field + sort | 128 | 128 | 0 |

These are exact `executionStats` counters from the preflight command. The
zero byte values are truthful current gateway accounting for the non-compound
paths, rather than an assertion that those paths decode no BSON.

An operation-heavy exact-source compound-stream capture used `-benchtime=10000x`:
`64,228 ns/op`, `68,341 B/op`, `1,086 allocs/op`, `48` candidates examined,
`32` documents materialized, and `2,112` materialized bytes; the 25x/10-run
text capture SHA-256 is `a809e8ec207e07bf8c49bd565431285c20f4a26698c93353320af6d0004b2d2f`.
CPU SHA-256 `00cf4f0deb16a280ceba7adb06ced309b719cf6f49e83edd471311849543f28d`,
heap SHA-256 `d32267a67ddbbb7de2960ac62003a4f321224c569016da7a4196f23654a8a454`.
The profiles are local `/tmp` diagnostic artifacts, not repository content;
their tops include scheduler/runtime activity; the largest benchmark-specific
allocation path is `compoundIndexPlanIDs`, so they are diagnostic only.

The direct scan records a maximum of 32 returned IDs after skip/limit and does
not retain decoded BSON in the cursor. The table reports gateway-owned
candidate/fetch/materialized counters directly rather than inferring them from
elapsed time.
