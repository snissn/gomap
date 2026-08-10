# #4065 compound planner cursor microbenchmark

This is a narrow, local microbenchmark for the standalone gateway's compound
cursor stream. It is not a MongoDB-comparison or production-throughput claim.

## Provenance

- Base: `3eea650b65261267b412560955bc6f4c19f7ef18`
- Source capture: `f4af2fede5afa25bc3e141bd63407b1a81db3bf9`
- Benchmark source SHA-256: `5f194ea9e0d1f7c47fee2f8289d157276963566cf78b3fd9a1b394335e4d286d` (`TreeDB/mongo_gateway/compound_planner_test.go`)
- Host: Apple M3, darwin/arm64, Go test default `GOMAXPROCS=8`
- Command: `GOWORK=off go test ./TreeDB/mongo_gateway -run '^$' -bench BenchmarkMongoCompoundPlannerCursor -benchtime=25x -count=10 -benchmem`

Each variant builds the same 128 BSON documents outside its timer and runs the
same `tenant=t`, `score:-1`, `skip=16`, `limit=32`, `batchSize=8` command,
draining every `getMore`. Preflight asserts the first 8-result batch and drains
the full 32-result cursor before timing. The variants are compound V2 stream,
no-secondary-index bounded scan plus sort, and a legacy `tenant:1` single-field
alternative plus sort. Index creation and seed insertion are excluded.

| variant | p50 ns/op | p95/p99 ns/op | p50 ops/sec | B/op range | allocs/op range |
| --- | ---: | ---: | ---: | ---: |
| compound stream | 95,056 | 101,957 / 101,957 | 10,520 | 321,648–322,465 | 1,035–1,038 |
| bounded scan + sort | 493,954 | 555,443 / 555,443 | 2,024 | 438,512–440,022 | 17,856–17,859 |
| single-field + sort | 515,924 | 534,742 / 534,742 | 1,938 | 474,150–474,820 | 18,263–18,265 |

Percentiles use the 10 observed per-variant samples: p50 is the midpoint of
the fifth and sixth sorted samples; p95/p99 use the conservative nearest-rank
maximum at this sample size. These are local microbenchmark observations, not
release SLOs.

An operation-heavy exact-source compound-stream capture used `-benchtime=10000x`:
`82,520 ns/op`, `322,008 B/op`, `1,042 allocs/op`; CPU SHA-256
`843b69a2f02f26aca4f2b3357426bc19bc4c1a3e542de594d671533c96430580`, heap
SHA-256 `8640467767a6b5997308b66bc6c7f7f2d12ea3e280e9586d32521c9c65221355`.
The profiles are local `/tmp` diagnostic artifacts, not repository content;
their top includes scheduler/runtime activity, so they are diagnostic only.

The direct scan records a maximum of 32 returned IDs after skip/limit and does
not retain decoded BSON in the cursor. Candidate/fetch/materialized counters
are exposed through `explain`; this benchmark intentionally does not infer them
from elapsed time.
