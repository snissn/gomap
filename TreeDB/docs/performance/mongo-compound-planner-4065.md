# #4065 compound planner cursor microbenchmark

This is a narrow, local microbenchmark for the standalone gateway's compound
cursor stream. It is not a MongoDB-comparison or production-throughput claim.

## Provenance

- Base: `3eea650b65261267b412560955bc6f4c19f7ef18`
- Source capture: `5aaab7aae666a7fa8a2e7bad5aa72f5c876cb471`
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
| compound stream | 123,822 | 149,503 / 149,503 | 8,076 | 319,547–319,868 | 970–972 |
| bounded scan + sort | 549,886 | 622,377 / 622,377 | 1,818 | 438,514–440,025 | 17,856–17,859 |
| single-field + sort | 542,249 | 560,682 / 560,682 | 1,844 | 474,232–474,850 | 18,263–18,266 |

Percentiles use the 10 observed per-variant samples: p50 is the midpoint of
the fifth and sixth sorted samples; p95/p99 use the conservative nearest-rank
maximum at this sample size. These are local microbenchmark observations, not
release SLOs.

An operation-heavy compound-stream profile capture used `-benchtime=10000x` at
the committed-content source: `76,949 ns/op`, `319,746 B/op`, `977 allocs/op`;
CPU SHA-256 `e58b325931e54a2aa827691692529b955f2e7994f21b6cc69a30ac8030d9219a`,
heap SHA-256 `fab817b29f9522c18fe1736818748f95cc85c426f836ac30b9eb485d6d720a33`.
The profile is a local `/tmp` diagnostic artifact, not repository content; its
top includes scheduler/runtime activity, so it is diagnostic only.

The direct scan records a maximum of 32 returned IDs after skip/limit and does
not retain decoded BSON in the cursor. Candidate/fetch/materialized counters
are exposed through `explain`; this benchmark intentionally does not infer them
from elapsed time.
