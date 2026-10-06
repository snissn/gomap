## TreeDB MVCC raw-path gate

- verdict: **FAIL**
- measured threshold observation: **FAIL**
- baseline: `2f626cb4f40e44736257537e38359ad003939ab9`
- candidate: `8a159dc1a2aeea02611421ecc3216b8ee84a1516`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `6494a881b48ad0b7cb3eef9ed211dbb18dbf8d05c375367d3e805bcf3e809415` | `d4cf3dd8249f7d9d072b2e70a6bfb7c57f0fa4470e2c7a7c0a6bfa5e6856ea73` | DIFFERENT |
| caching | `dcb73a53abc60fe622f8f48d694611bce661a2ff56cbecb1f8ee3b31aff7ebf1` | `b148ac9faf8fd05dcb8ffb912a6cea68bb092d9c250013ae1e663ad894bd53b5` | DIFFERENT |
| treedb | `1fd618162699eb12a2841a1f930879f76106a51d078b89391c0ffc20c7c46af7` | `d8eb4ac49a0c6e94462a25613a324781f829505066c64b00bad3419e0de5e354` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 648.350 | 700.000 | +7.97% | +7.96% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 125315.000 | 124627.500 | -0.55% | -0.58% | 152689.000 | 152745.000 | 64.000 | 281.000 | 281.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 399.600 | 384.850 | -3.69% | -3.27% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 283.700 | 283.200 | -0.18% | +0.21% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 250671.500 | 250278.500 | -0.16% | +0.89% | 4570.000 | 4699.500 | 45.700 | 17.000 | 17.000 | FAIL | CANDIDATE | FAIL |
