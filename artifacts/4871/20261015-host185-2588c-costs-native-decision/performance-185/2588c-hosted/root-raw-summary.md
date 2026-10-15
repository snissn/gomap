## TreeDB MVCC raw-path gate

- verdict: **PASS**
- measured threshold observation: **PASS**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `4ded26c902590d1202496e386ad8214affd0eb49ffcf03e4ca3ba8bb62369942` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `8109dccf62e611075743851e13beccbf774227a21dc123485ad7f2db78341974` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `f46e163cfad00e34ca93401adfeb28d94e6aa0a51a9c2640c6bfafa645823779` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 424.650 | 411.600 | -3.07% | -3.26% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 36302.500 | 36266.500 | -0.10% | +3.28% | 31945.000 | 31502.000 | 64.000 | 127.000 | 104.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 223.900 | 226.700 | +1.25% | +1.64% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 294.600 | 291.300 | -1.12% | +0.14% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 225446.500 | 227681.000 | +0.99% | +0.79% | 4578.000 | 4577.500 | 45.780 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
