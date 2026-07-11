## TreeDB Dgraph MVCC closeout matrix

- candidate: `2f0a687f048ece277ab039303f6e28a1a7906bcb`
- samples: 5
- measured benchmark invocations: 10
- maximum benchmark-process RSS: 622068 KiB
- aggregate process CPU: user 100.64s, system 6.85s
- durability classes are separate rows; relaxed rows are not durability-equivalent to durable sync

| Benchmark | ns/op | Throughput | B/op | allocs/op | storage bytes/op | durable footprint bytes/op | delete write amp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkDgraphMVCCCloseout/AllVersions/durable_sync/keys=64/depth=1` | 19127.000 | 3346027.000 versions/s | 4195.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/durable_sync/keys=64/depth=32` | 510050.000 | 4015297.000 versions/s | 51918.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_off_relaxed/keys=64/depth=1` | 18778.000 | 3408185.000 versions/s | 4192.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_off_relaxed/keys=64/depth=32` | 509352.000 | 4020796.000 versions/s | 51832.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_on_relaxed/keys=64/depth=1` | 18661.000 | 3429627.000 versions/s | 4195.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_on_relaxed/keys=64/depth=32` | 510853.000 | 4008987.000 versions/s | 51908.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=1` | 6550372.000 | 152.700 mutations/s | 4689.000 | 14.000 | 1931.000 | 1931.000 | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=32` | 5034211.000 | 6357.000 mutations/s | 16917.000 | 112.000 | 5177.000 | 5177.000 | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_off_relaxed/batch=1` | 4056.000 | 246564.000 mutations/s | 7503.000 | 9.000 | 77.520 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_off_relaxed/batch=32` | 40236.000 | 795301.000 mutations/s | 26843.000 | 106.000 | 2081.000 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_on_relaxed/batch=1` | 4837.000 | 206731.000 mutations/s | 4259.000 | 13.000 | 81.350 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_on_relaxed/batch=32` | 43463.000 | 736250.000 mutations/s | 20868.000 | 110.000 | 2065.000 | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/durable_sync/depth=1` | 1133.000 | 882798.000 lookups/s | 629.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/durable_sync/depth=64` | 1504.000 | 664880.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_off_relaxed/depth=1` | 1060.000 | 943589.000 lookups/s | 628.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_off_relaxed/depth=64` | 1472.000 | 679436.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_on_relaxed/depth=1` | 1069.000 | 935395.000 lookups/s | 629.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_on_relaxed/depth=64` | 1477.000 | 676921.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/Prune/durable_sync/keys=64/depth=16/floor=12` | 26114625.000 | 26958.000 pruned_versions/s | 246960.000 | 198.000 | 524727.000 | 524727.000 | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/durable_sync/keys=64/depth=16/floor=4` | 12983205.000 | 14788.000 pruned_versions/s | 271024.000 | 160.000 | 524727.000 | 524727.000 | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_off_relaxed/keys=64/depth=16/floor=12` | 486220.000 | 1447904.000 pruned_versions/s | 168512.000 | 170.000 | 524664.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_off_relaxed/keys=64/depth=16/floor=4` | 389759.000 | 492612.000 pruned_versions/s | 131576.000 | 138.000 | 524664.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_on_relaxed/keys=64/depth=16/floor=12` | 612027.000 | 1150276.000 pruned_versions/s | 241728.000 | 182.000 | 524727.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_on_relaxed/keys=64/depth=16/floor=4` | 841761.000 | 228093.000 pruned_versions/s | 134544.000 | 140.000 | 524727.000 | - | 0.829 |
