## TreeDB Dgraph MVCC closeout matrix

- candidate: `dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a`
- samples: 5
- measured benchmark invocations: 10
- maximum benchmark-process RSS: 600640 KiB
- aggregate process CPU: user 109.07s, system 7.49s
- durability classes are separate rows; relaxed rows are not durability-equivalent to durable sync

| Benchmark | ns/op | Throughput | B/op | allocs/op | storage bytes/op | durable footprint bytes/op | delete write amp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkDgraphMVCCCloseout/AllVersions/durable_sync/keys=64/depth=1` | 21356.000 | 2996840.000 versions/s | 4195.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/durable_sync/keys=64/depth=32` | 571677.000 | 3582446.000 versions/s | 51918.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_off_relaxed/keys=64/depth=1` | 22236.000 | 2878164.000 versions/s | 4192.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_off_relaxed/keys=64/depth=32` | 564869.000 | 3625628.000 versions/s | 51834.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_on_relaxed/keys=64/depth=1` | 21558.000 | 2968764.000 versions/s | 4195.000 | 150.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/AllVersions/wal_on_relaxed/keys=64/depth=32` | 585307.000 | 3499021.000 versions/s | 51920.000 | 4118.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=1` | 6559316.000 | 152.500 mutations/s | 4689.000 | 14.000 | 1876.000 | 1876.000 | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=32` | 6621481.000 | 4833.000 mutations/s | 16911.000 | 112.000 | 4522.000 | 4522.000 | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_off_relaxed/batch=1` | 4126.000 | 242359.000 mutations/s | 7485.000 | 9.000 | 74.010 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_off_relaxed/batch=32` | 44931.000 | 712204.000 mutations/s | 27035.000 | 106.000 | 2050.000 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_on_relaxed/batch=1` | 5497.000 | 181928.000 mutations/s | 4267.000 | 13.000 | 79.630 | - | - |
| `BenchmarkDgraphMVCCCloseout/CommitAt/wal_on_relaxed/batch=32` | 44432.000 | 720202.000 mutations/s | 20647.000 | 110.000 | 2079.000 | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/durable_sync/depth=1` | 1149.000 | 870369.000 lookups/s | 628.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/durable_sync/depth=64` | 1761.000 | 567989.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_off_relaxed/depth=1` | 1229.000 | 813744.000 lookups/s | 628.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_off_relaxed/depth=64` | 1690.000 | 591775.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_on_relaxed/depth=1` | 1205.000 | 829597.000 lookups/s | 629.000 | 12.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/GetAt/wal_on_relaxed/depth=64` | 1659.000 | 602691.000 lookups/s | 1008.000 | 16.000 | - | - | - |
| `BenchmarkDgraphMVCCCloseout/Prune/durable_sync/keys=64/depth=16/floor=12` | 26745805.000 | 26322.000 pruned_versions/s | 247008.000 | 199.000 | 524727.000 | 524727.000 | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/durable_sync/keys=64/depth=16/floor=4` | 13333546.000 | 14400.000 pruned_versions/s | 270976.000 | 159.000 | 524727.000 | 524727.000 | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_off_relaxed/keys=64/depth=16/floor=12` | 510644.000 | 1378651.000 pruned_versions/s | 168512.000 | 170.000 | 524664.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_off_relaxed/keys=64/depth=16/floor=4` | 528876.000 | 363034.000 pruned_versions/s | 131576.000 | 138.000 | 524664.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_on_relaxed/keys=64/depth=16/floor=12` | 633169.000 | 1111867.000 pruned_versions/s | 241728.000 | 182.000 | 524727.000 | - | 0.829 |
| `BenchmarkDgraphMVCCCloseout/Prune/wal_on_relaxed/keys=64/depth=16/floor=4` | 814680.000 | 235675.000 pruned_versions/s | 134544.000 | 140.000 | 524727.000 | - | 0.829 |
