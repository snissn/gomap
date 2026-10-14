## TreeDB MVCC raw-path gate

- verdict: **FAIL**
- measured threshold observation: **FAIL**
- baseline: `3f87d74b98e8b1e809d151611be03785f85e2283`
- candidate: `c3ba82a5ef6d6590f6c70734045c8d640c4c1255`
- samples: 2 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `22231a13e2dc4cbc93b6954c5b2fd935bd02568fc81a6663c502d95d7062a6d8` | `928824da1ce6515e11f2f46153b9713842e37c049b348406c3718b84ae0319f2` | DIFFERENT |
| caching | `5f6758c5eae6168cd3089b12a700198e90228e58166792c510b2a9b2e9e19fa4` | `a04291d8211b74f4dc2a3a2aa568cf6e1979f8dc70c28604c91f905c8413349a` | DIFFERENT |
| treedb | `d2d98999b97d9338b299cec571fe87ad976bcc7ce1b3d7c4bf3552e34c54c763` | `fb36299f907fad7c853c09d7a2ee675d73d71d8ff5cd100558f0b15ef1ab048d` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 593.500 | 864.600 | +45.68% | +46.14% | 0.000 | 112.000 | 0.000 | 0.000 | 1.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 89926.500 | 92713.000 | +3.10% | +4.40% | 152790.500 | 155006.000 | 64.000 | 281.000 | 286.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 301.600 | 299.300 | -0.76% | -0.67% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 320.450 | 316.250 | -1.31% | -1.30% | 113.000 | 113.000 | 1.130 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 5651533.500 | 5565633.000 | -1.52% | -1.38% | 4753.000 | 4717.000 | 47.530 | 18.000 | 18.000 | PASS | CANDIDATE | PASS |
