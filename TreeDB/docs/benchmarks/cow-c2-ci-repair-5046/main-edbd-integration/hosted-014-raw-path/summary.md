## TreeDB MVCC raw-path gate

- verdict: **PASS**
- measured threshold observation: **PASS**
- baseline: `40dc31f44e7bb14a4f85dab254131af029ef820b`
- candidate: `0142519b2cc4c097f50b4e7a64a5b6993ff0d9c6`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `6494a881b48ad0b7cb3eef9ed211dbb18dbf8d05c375367d3e805bcf3e809415` | `52ec71b99af2036b3d254b95866d7116f0808af6656c2e19864a582814e87dea` | DIFFERENT |
| caching | `dcb73a53abc60fe622f8f48d694611bce661a2ff56cbecb1f8ee3b31aff7ebf1` | `137c020d27a62c234b3847dc5bdba916e9602b96dc4ace515a5c97b4b12d58cc` | DIFFERENT |
| treedb | `1fd618162699eb12a2841a1f930879f76106a51d078b89391c0ffc20c7c46af7` | `c547a7121a1a6b6d61c510780926e86ad93c7cfe82e1d49f6b88b159241202c3` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 643.800 | 628.500 | -2.38% | -2.35% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 124723.000 | 126312.500 | +1.27% | +2.11% | 152688.000 | 152745.000 | 64.000 | 281.000 | 281.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 395.800 | 381.100 | -3.71% | -3.83% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 284.850 | 292.250 | +2.60% | +2.50% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 254663.500 | 253876.000 | -0.31% | -0.70% | 4578.500 | 4578.000 | 45.785 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
