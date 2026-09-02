## TreeDB Dgraph MVCC closeout matrix

- candidate: `103f9c5af85d8d6a5801119fc2247be3b9c87fad`
- samples: 5
- measured benchmark invocations: 10
- maximum benchmark-process RSS: 611264 KiB
- aggregate process CPU: user 102.36s, system 6.97s
- durability classes are separate rows; relaxed rows are not durability-equivalent to durable sync

| Benchmark | ns/op | Throughput | B/op | allocs/op | storage bytes/op | durable footprint bytes/op | delete write amp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkDgraphMVCCCloseout/AllVersions/durable_sync/keys=64/depth=1` | 18364.000 | 3485151.000 versions/s | 4195.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/durable_sync/keys=64/depth=32` | 497176.000 | 4119271.000 versions/s | 51915.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_off_relaxed/keys=64/depth=1` | 18879.000 | 3390107.000 versions/s | 4192.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_off_relaxed/keys=64/depth=32` | 501606.000 | 4082893.000 versions/s | 51833.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_on_relaxed/keys=64/depth=1` | 18669.000 | 3428068.000 versions/s | 4195.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_on_relaxed/keys=64/depth=32` | 510273.000 | 4013546.000 versions/s | 51918.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=1` | 6688579.000 | 149.500 mutations/s | 4652.000 | 14.000 | 1960.000 | 1960.000 | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=32` | 6071126.000 | 5271.000 mutations/s | 16925.000 | 112.000 | 4496.000 | 4496.000 | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_off_relaxed/batch=1` | 3993.000 | 250407.000 mutations/s | 7494.000 | 9.000 | 76.840 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_off_relaxed/batch=32` | 38143.000 | 838950.000 mutations/s | 26144.000 | 106.000 | 2054.000 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_on_relaxed/batch=1` | 4593.000 | 217744.000 mutations/s | 4252.000 | 13.000 | 83.410 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_on_relaxed/batch=32` | 42543.000 | 752181.000 mutations/s | 20131.000 | 110.000 | 2085.000 | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/durable_sync/depth=1` | 1067.000 | 937621.000 lookups/s | 629.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/durable_sync/depth=64` | 1492.000 | 670037.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_off_relaxed/depth=1` | 1057.000 | 946058.000 lookups/s | 628.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_off_relaxed/depth=64` | 1473.000 | 678949.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_on_relaxed/depth=1` | 1039.000 | 962350.000 lookups/s | 629.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_on_relaxed/depth=64` | 1454.000 | 687875.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/Prune/durable_sync/keys=64/depth=16/floor=12` | 27923914.000 | 25211.000 pruned_versions/s | 245568.000 | 182.000 | 524727.000 | 524727.000 | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/durable_sync/keys=64/depth=16/floor=4` | 14216940.000 | 13505.000 pruned_versions/s | 269456.000 | 142.000 | 524727.000 | 524727.000 | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_off_relaxed/keys=64/depth=16/floor=12` | 415359.000 | 1694919.000 pruned_versions/s | 167120.000 | 154.000 | 524664.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_off_relaxed/keys=64/depth=16/floor=4` | 353285.000 | 543471.000 pruned_versions/s | 130184.000 | 122.000 | 524664.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_on_relaxed/keys=64/depth=16/floor=12` | 568753.000 | 1237796.000 pruned_versions/s | 240336.000 | 166.000 | 524727.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_on_relaxed/keys=64/depth=16/floor=4` | 769070.000 | 249652.000 pruned_versions/s | 133152.000 | 124.000 | 524727.000 | - | 0.829 |
