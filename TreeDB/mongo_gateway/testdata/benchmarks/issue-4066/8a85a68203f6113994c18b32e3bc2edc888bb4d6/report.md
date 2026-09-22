# #4066 query benchmark

- source: 8a85a68203f6113994c18b32e3bc2edc888bb4d6
- platform: os=Darwin arch=arm64
- go: go version go1.26.0 darwin/arm64
- command: `GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run ^$ -bench ^BenchmarkMongoNegativeDottedQueryShapes$ -benchtime=100x -count=10`
- percentile method: nearest rank, rank=ceil(p*n), n=10
- dotted_sort_materialized-8: p50=673862ns/op p95=680658ns/op p99=680658ns/op ops/s=1484.0 B/op=337968-339774 allocs/op=10269-10322 explain={returned:256.0,candidates:256.0,materialized:256.0,materializedBytes:0}
- indexed_positive_residual-8: p50=199010ns/op p95=255535ns/op p99=255535ns/op ops/s=5024.9 B/op=295581-297497 allocs/op=3865-3868 explain={returned:20.00,candidates:192.0,materialized:192.0,materializedBytes:12480}
- bounded_negative_scan-8: p50=125957ns/op p95=128404ns/op p99=128404ns/op ops/s=7939.2 B/op=124296-124447 allocs/op=4065-4065 explain={returned:86.00,candidates:256.0,materialized:86.00,materializedBytes:0}
- dotted_projection-8: p50=310552ns/op p95=317941ns/op p99=317941ns/op ops/s=3220.1 B/op=457205-460621 allocs/op=9086-9088 explain={returned:256.0,candidates:256.0,materialized:256.0,materializedBytes:0}
