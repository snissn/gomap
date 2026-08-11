# #4066 query benchmark

- source: `5a60b96f49d9bd02c1f024102a6f7dadb6a53e7b`
- host: Darwin Michaels-Laptop.local, arm64 (Apple M3)
- Go: `go version go1.26.0 darwin/arm64`
- command: `GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run '^$' -bench '^BenchmarkMongoNegativeDottedQueryShapes$' -benchtime=100x -count=10`
- script: `scripts/mongo_gateway_query_4066_bench.sh`
- percentile method: nearest rank, rank `ceil(p*n)`, `n=10`.
- validation: the script requires ten non-empty raw samples for each shape and
  rejects a counter/cardinality mismatch before emitting this report. Result
  equality/cardinality is preflighted by each `executionStats` explain.

| Shape | p50 ns/op | p95 ns/op | p99 ns/op | ops/s | B/op | allocs/op | explain returned/candidates/materialized/materialized bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| indexed positive + residual | 279616 | 378461 | 378461 | 3576.3 | 390808-392196 | 5128-5131 | 20 / 256 / 256 / 16640 |
| bounded negative scan | 128103 | 131816 | 131816 | 7806.2 | 123765-123833 | 4067-4067 | 86 / 256 / 86 / 0 |
| dotted projection | 338870 | 463086 | 463086 | 2951.0 | 457412-460709 | 9086-9088 | 256 / 256 / 256 / 0 |
| dotted sort, materialized | 688240 | 706039 | 706039 | 1453.0 | 337892-339774 | 10269-10322 | 256 / 256 / 256 / 0 |

`candidateMaterializedBytes` is nonzero only when the indexed-positive
candidate path copies raw candidate documents; the other three snapshots
truthfully report zero for that counter while their separate materialized-doc
counts remain bounded at the 256-document scan cap.
