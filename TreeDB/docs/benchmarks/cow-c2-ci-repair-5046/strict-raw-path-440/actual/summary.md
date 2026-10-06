## TreeDB MVCC raw-path gate

- verdict: **PASS**
- measured threshold observation: **PASS**
- baseline: `edbd68d34b0fdf7152a498b87d373576de029721`
- candidate: `4403776d10b6ac82eb9e1839f437f59a78058816`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `254417d6df230fc850c17aaeed912859f9c8c387498434837b1fa4e2f60e994e` | `eb25b812c44c76cd3f3249824744bf7035081ae71b8ced1683e1473e43fd495d` | DIFFERENT |
| caching | `dcb73a53abc60fe622f8f48d694611bce661a2ff56cbecb1f8ee3b31aff7ebf1` | `137c020d27a62c234b3847dc5bdba916e9602b96dc4ace515a5c97b4b12d58cc` | DIFFERENT |
| treedb | `4121b4e7a650757d741ea6b39b11c54ce9efcdfe50e8bea3091f8e7f9352d995` | `f7ad4a1fd651942f945f59946805eaa2bff1ceeaac5c3b5ef808d24188c87350` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 434.750 | 425.000 | -2.24% | -1.02% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 79586.000 | 81382.500 | +2.26% | -1.68% | 152689.000 | 152744.000 | 64.000 | 281.000 | 281.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 204.500 | 212.000 | +3.67% | +3.15% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 276.650 | 272.450 | -1.52% | -1.43% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 217596.500 | 220151.500 | +1.17% | +1.23% | 4577.500 | 4577.000 | 45.775 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
