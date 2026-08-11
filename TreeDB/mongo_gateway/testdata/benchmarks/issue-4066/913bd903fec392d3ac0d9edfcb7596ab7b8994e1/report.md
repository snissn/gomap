# #4066 query benchmark
- source: 913bd903fec392d3ac0d9edfcb7596ab7b8994e1
- host: Darwin Michaels-Laptop.local 25.2.0 Darwin Kernel Version 25.2.0: Tue Nov 18 21:03:25 PST 2025; root:xnu-12377.61.12~1/RELEASE_ARM64_T8122 arm64
- go: go version go1.26.0 darwin/arm64
- command: `GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run ^$ -bench ^BenchmarkMongoNegativeDottedQueryShapes$ -benchtime=100x -count=10`
- percentile method: nearest rank, rank=ceil(p*n), n=10
- dotted_sort_materialized-8: p50=680114ns/op p95=844514ns/op p99=844514ns/op ops/s=1470.3 B/op=350060-354060 allocs/op=10676-10679 explain={returned:256.0,candidates:256.0,materialized:256.0,materializedBytes:0}
- indexed_positive_residual-8: p50=248569ns/op p95=250215ns/op p99=250215ns/op ops/s=4023.0 B/op=389935-390072 allocs/op=5128-5128 explain={returned:20.00,candidates:256.0,materialized:256.0,materializedBytes:16640}
- bounded_negative_scan-8: p50=120904ns/op p95=123193ns/op p99=123193ns/op ops/s=8271.0 B/op=123816-123816 allocs/op=4065-4065 explain={returned:86.00,candidates:256.0,materialized:86.00,materializedBytes:0}
- dotted_projection-8: p50=293146ns/op p95=297709ns/op p99=297709ns/op ops/s=3411.3 B/op=450061-450973 allocs/op=8881-8908 explain={returned:256.0,candidates:256.0,materialized:256.0,materializedBytes:0}
