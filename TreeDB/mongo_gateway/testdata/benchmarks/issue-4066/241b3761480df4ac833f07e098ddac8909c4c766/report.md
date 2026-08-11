# #4066 query benchmark

- source: 241b3761480df4ac833f07e098ddac8909c4c766
- platform: os=Darwin arch=arm64
- go: go version go1.26.0 darwin/arm64
- command: `GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run ^$ -bench ^BenchmarkMongoNegativeDottedQueryShapes$ -benchtime=100x -count=10`
- percentile method: nearest rank, rank=ceil(p*n), n=10
- dotted_sort_materialized-8: p50=674521ns/op p95=679313ns/op p99=679313ns/op ops/s=1482.5 B/op=337968-339849 allocs/op=10269-10322 explain={returned:256.0,candidates:256.0,materialized:256.0,materializedBytes:0}
- indexed_positive_residual-8: p50=192655ns/op p95=219602ns/op p99=219602ns/op ops/s=5190.6 B/op=295751-297240 allocs/op=3866-3868 explain={returned:20.00,candidates:192.0,materialized:192.0,materializedBytes:12480}
- bounded_negative_scan-8: p50=127910ns/op p95=130428ns/op p99=130428ns/op ops/s=7818.0 B/op=124301-124382 allocs/op=4065-4065 explain={returned:86.00,candidates:256.0,materialized:86.00,materializedBytes:0}
- dotted_projection-8: p50=313288ns/op p95=320040ns/op p99=320040ns/op ops/s=3192.0 B/op=457365-460408 allocs/op=9086-9088 explain={returned:256.0,candidates:256.0,materialized:256.0,materializedBytes:0}
