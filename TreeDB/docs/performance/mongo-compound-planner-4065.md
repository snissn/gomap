# #4065 compound planner cursor microbenchmark

This is a narrow, local microbenchmark for the standalone gateway's compound
cursor stream. It is not a MongoDB-comparison or production-throughput claim.

## Provenance

- Base: `3eea650b65261267b412560955bc6f4c19f7ef18`
- Source capture: `93d91136e44b99fef3ea4b68007f7b9f2a9af4b5`
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
| compound stream | 60,889 | 70,915 / 70,915 | 16,423 | 59,072–59,426 | 795–796 |
| bounded scan + sort | 489,237 | 544,085 / 544,085 | 2,044 | 438,507–440,234 | 17,856–17,859 |
| single-field + sort | 498,719 | 543,428 / 543,428 | 2,005 | 474,043–474,741 | 18,263–18,265 |

Percentiles use the 10 observed per-variant samples: p50 is the midpoint of
the fifth and sixth sorted samples; p95/p99 use the conservative nearest-rank
maximum at this sample size. These are local microbenchmark observations, not
release SLOs.

An operation-heavy exact-source compound-stream capture used `-benchtime=10000x`:
`54,225 ns/op`, `59,245 B/op`, `801 allocs/op`; CPU SHA-256
`1b330e729a5d37a9129479cbc239965b116ad61ba776dbccbf591b0f505b4d61`, heap
SHA-256 `6116bbade55e68ec448b0775198c70cc06229194a30204fe593e678311b4a5eb`.
The profiles are local `/tmp` diagnostic artifacts, not repository content;
their top includes scheduler/runtime activity, so they are diagnostic only.

The direct scan records a maximum of 32 returned IDs after skip/limit and does
not retain decoded BSON in the cursor. Candidate/fetch/materialized counters
are exposed through `explain`; this benchmark intentionally does not infer them
from elapsed time.
