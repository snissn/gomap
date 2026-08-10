# #4065 compound planner cursor microbenchmark

This is a narrow, local microbenchmark for the standalone gateway's compound
cursor stream. It is not a MongoDB-comparison or production-throughput claim.

## Provenance

- Base: `3eea650b65261267b412560955bc6f4c19f7ef18`
- Source capture: `d4a2373c14d132c88cab2c08569ed11a1e3c76c8`
- Benchmark source SHA-256: `518c18fca9925b7b4e10478da047a1c327636c8d5472f9fe46cedf65c2b5b6ce` (`TreeDB/mongo_gateway/compound_planner_test.go`)
- Host: Apple M3, darwin/arm64, Go test default `GOMAXPROCS=8`
- Command: `GOWORK=off go test ./TreeDB/mongo_gateway -run '^$' -bench BenchmarkMongoCompoundPlannerCursor -benchtime=25x -count=10 -benchmem`

Each variant builds the same 128 BSON documents outside its timer and runs the
same `tenant=t`, `score:-1`, `skip=16`, `limit=32`, `batchSize=8` command,
draining every `getMore`. Preflight asserts the first 8-result batch and drains
the full 32-result cursor before timing. The variants are compound V2 stream,
no-secondary-index bounded scan plus sort, and a legacy `tenant:1` single-field
alternative plus sort. Index creation and seed insertion are excluded.

| variant | p50 ns/op | p95/p99 ns/op | p50 ops/sec | B/op range | allocs/op range |
| --- | ---: | ---: | ---: | ---: | ---: |
| compound stream | 68,649 | 82,768 / 82,768 | 14,567 | 59,072–59,542 | 795–796 |
| bounded scan + sort | 485,810 | 509,502 / 509,502 | 2,058 | 438,588–439,774 | 17,856–17,858 |
| single-field + sort | 608,648 | 640,978 / 640,978 | 1,643 | 474,046–475,044 | 18,263–18,266 |

Percentiles use the 10 observed per-variant samples: p50 is the midpoint of
the fifth and sixth sorted samples; p95/p99 use the conservative nearest-rank
maximum at this sample size. These are local microbenchmark observations, not
release SLOs.

An operation-heavy exact-source compound-stream capture used `-benchtime=10000x`:
`55,480 ns/op`, `59,243 B/op`, `801 allocs/op`; CPU SHA-256
`30910a619864e3cdab46082d0e2b558fa5915bb9e316a9552eb7bc91db7eb1a0`, heap
SHA-256 `0b393053e5b9d4dcf6cec9bb3a42b421c5d0cb96352059956fcb3afc0b26f5ca`.
The profiles are local `/tmp` diagnostic artifacts, not repository content;
their top includes scheduler/runtime activity, so they are diagnostic only.

The direct scan records a maximum of 32 returned IDs after skip/limit and does
not retain decoded BSON in the cursor. Candidate/fetch/materialized counters
are exposed through `explain`; this benchmark intentionally does not infer them
from elapsed time.
