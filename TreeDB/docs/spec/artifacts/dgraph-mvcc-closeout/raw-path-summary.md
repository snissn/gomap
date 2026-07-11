## TreeDB MVCC raw-path gate

- result: **PASS**
- baseline: `f9c9b2a37838909d0e669818cfa2840c0a8d5f85`
- candidate: `2f0a687f048ece277ab039303f6e28a1a7906bcb`
- samples: 7 per revision, alternating sequential order
- timing threshold: candidate median <= baseline median + 5%
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Benchmark | Base ns/op | Head ns/op | Delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| BenchmarkGetVersioned | 803.000 | 763.800 | -4.88% | 480.000 | 480.000 | 4.800 | 1.000 | 1.000 | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | 18515.000 | 18486.000 | -0.16% | 13240.000 | 13238.000 | 64.000 | 30.000 | 30.000 | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | 291.600 | 293.400 | +0.62% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS |
| BenchmarkRepeatedIterator | 313.600 | 303.000 | -3.38% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | 5454877.000 | 5592783.000 | +2.53% | 4720.000 | 4731.000 | 47.200 | 17.000 | 17.000 | PASS |
