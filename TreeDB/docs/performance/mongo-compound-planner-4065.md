# #4065 compound planner cursor microbenchmark

This is a narrow, local microbenchmark for the standalone gateway's compound
cursor stream. It is not a MongoDB-comparison or production-throughput claim.

## Provenance

- Base: `3eea650b65261267b412560955bc6f4c19f7ef18`
- Source capture: `e6c01a05b8d738ae1eddb4ff9920e7b3596c58af`
- Host: Apple M3, darwin/arm64, Go test default `GOMAXPROCS=8`
- Command: `GOWORK=off go test ./TreeDB/mongo_gateway -run '^$' -bench BenchmarkMongoCompoundPlannerCursor -benchtime=25x -count=3 -benchmem`

The benchmark builds 128 BSON documents and a `{tenant:1,score:-1}` index
outside its timer. Each timed operation is a `tenant=t` compound scan ordered
by `score:-1`, with `skip=16`, `limit=32`, `batchSize=8`, and drains every
`getMore`. It therefore measures bounded planning plus cursor batch decoding,
not index creation or seed insertion.

| sample | ns/op | ops/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 1 | 156,803 | 6,378 | 320,664 | 972 |
| 2 | 142,637 | 7,011 | 320,568 | 971 |
| 3 | 125,640 | 7,959 | 320,463 | 971 |

With only three samples this table does **not** claim p50/p95/p99. The harness
is retained so a controlled runner can collect a sufficient sample distribution
and an equivalent bounded scan-plus-sort and single-field comparator before any
performance decision.

An operation-heavy profile capture used `-benchtime=10000x` at the same source:
`75,430 ns/op`, `319,744 B/op`, `977 allocs/op`; CPU SHA-256
`560c75dd9648949fafce859131504095ff9509318dc61704765d6bb41f7c36df`,
heap SHA-256 `7657086de8a46e188b1cea885073a063793dbf5018e685bccfc710f5cb37b8c3`.
The profile is a local `/tmp` diagnostic artifact, not repository content; its
top includes scheduler/runtime activity, so it is diagnostic only.

The direct scan records a maximum of 32 returned IDs after skip/limit and does
not retain decoded BSON in the cursor. Candidate/fetch/materialized counters
are exposed through `explain`; this benchmark intentionally does not infer them
from elapsed time.
