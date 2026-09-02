# TreeDB MVCC adapter-overhead gate

- result: **FAIL**
- baseline: `f9c9b2a37838909d0e669818cfa2840c0a8d5f85`
- candidate: `dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- base/head timing threshold: +5%
- candidate MVCC/direct ratio threshold: 2x

| Benchmark | Base ns/op | Head ns/op | Delta | Base B/op | Head B/op | Base allocs/op | Head allocs/op | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| BenchmarkCommitAt/DirectTreeDB/1 | 7026.500 | 7713.000 | +9.77% | 7477.000 | 7467.500 | 8.500 | 8.500 | FAIL |
| BenchmarkCommitAt/MVCC/1 | 6743.500 | 7217.500 | +7.03% | 7558.500 | 7559.000 | 9.000 | 9.000 | FAIL |
| BenchmarkGetAt/DirectSeek/64 | 2679.500 | 2750.000 | +2.63% | 992.000 | 992.000 | 15.000 | 15.000 | PASS |
| BenchmarkGetAt/MVCC/64 | 2657.500 | 2773.000 | +4.35% | 1008.000 | 1008.000 | 16.000 | 16.000 | PASS |
| BenchmarkVersionIteration/Physical/keys=64/depth=32/reverse=false | 828734.500 | 985224.000 | +18.88% | 50114.000 | 50119.000 | 4108.000 | 4108.000 | FAIL |
| BenchmarkVersionIteration/MVCC/keys=64/depth=32/reverse=false | 973824.000 | 1104381.500 | +13.41% | 51846.500 | 51853.000 | 4118.000 | 4118.000 | FAIL |

| Pair | Base ratio | Head ratio | Result |
| --- | ---: | ---: | --- |
| CommitAt | 0.960x | 0.936x | PASS |
| GetAt | 0.992x | 1.008x | PASS |
| VersionIteration | 1.175x | 1.121x | PASS |
