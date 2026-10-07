## TreeDB MVCC raw-path gate

- verdict: **FAIL**
- measured threshold observation: **FAIL**
- baseline: `edbd68d34b0fdf7152a498b87d373576de029721`
- candidate: `cb09ffe33b10a8a623cd335a43780d4997524f82`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `254417d6df230fc850c17aaeed912859f9c8c387498434837b1fa4e2f60e994e` | `cc83869bec750f0ca3469f5399e2fbd38efd404a8bbec2cff154fd1b5b94388c` | DIFFERENT |
| caching | `dcb73a53abc60fe622f8f48d694611bce661a2ff56cbecb1f8ee3b31aff7ebf1` | `9cdc109305b032d4eca4bc0b7bcd8248068afabe039511279e52cede13e27667` | DIFFERENT |
| treedb | `4121b4e7a650757d741ea6b39b11c54ce9efcdfe50e8bea3091f8e7f9352d995` | `0aa9f871c5220fd81857a5d650bb0e917594eff3b844d1eec4869412c51901b3` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 511.500 | 521.750 | +2.00% | +1.63% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 108638.500 | 110715.000 | +1.91% | +2.83% | 152689.000 | 152689.000 | 64.000 | 281.000 | 281.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 315.500 | 309.600 | -1.87% | -0.86% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 235.850 | 235.450 | -0.17% | +1.25% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 744192.500 | 436736.000 | -41.31% | +11.19% | 4578.000 | 4580.500 | 45.780 | 17.000 | 17.000 | FAIL | CANDIDATE | FAIL |
