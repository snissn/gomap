## TreeDB MVCC adapter-overhead gate

- result: **PASS**
- baseline: `f9c9b2a37838909d0e669818cfa2840c0a8d5f85`
- candidate: `2f0a687f048ece277ab039303f6e28a1a7906bcb`
- samples: 7 per revision, alternating sequential order
- base/head timing threshold: +5%
- candidate MVCC/direct ratio threshold: 2x

| Benchmark | Base ns/op | Head ns/op | Delta | Base B/op | Head B/op | Base allocs/op | Head allocs/op | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| BenchmarkCommitAt/DirectTreeDB/1 | 4749.000 | 4741.000 | -0.17% | 7556.000 | 7555.000 | 8.000 | 8.000 | PASS |
| BenchmarkCommitAt/MVCC/1 | 4972.000 | 4939.000 | -0.66% | 7567.000 | 7560.000 | 9.000 | 9.000 | PASS |
| BenchmarkGetAt/DirectSeek/64 | 1421.000 | 1415.000 | -0.42% | 992.000 | 992.000 | 15.000 | 15.000 | PASS |
| BenchmarkGetAt/MVCC/64 | 1573.000 | 1584.000 | +0.70% | 1008.000 | 1008.000 | 16.000 | 16.000 | PASS |
| BenchmarkVersionIteration/Physical/keys=64/depth=32/reverse=false | 508946.000 | 473346.000 | -6.99% | 50102.000 | 50100.000 | 4108.000 | 4108.000 | PASS |
| BenchmarkVersionIteration/MVCC/keys=64/depth=32/reverse=false | 538873.000 | 524927.000 | -2.59% | 51831.000 | 51831.000 | 4118.000 | 4118.000 | PASS |

| Pair | Base ratio | Head ratio | Result |
| --- | ---: | ---: | --- |
| CommitAt | 1.047x | 1.042x | PASS |
| GetAt | 1.107x | 1.119x | PASS |
| VersionIteration | 1.059x | 1.109x | PASS |
