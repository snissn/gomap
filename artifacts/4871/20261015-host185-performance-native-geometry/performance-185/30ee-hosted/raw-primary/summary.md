## TreeDB MVCC raw-path gate

- verdict: **PASS**
- measured threshold observation: **PASS**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `30ee954b974077b8968cdfb836e03058d9934ed1`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `22f60c81652b9f4589ca0935b3ec0f77c49392a3699a783b6422e9db6e1380ee` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `6fe6f34eb76de25086db0565b304221aef0c2cda717a2f2d9d6eec8f52be32c7` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `2a2cd75bf3a3eb067529db72cf0b77eb6ffa197c6a3c037697c3c7637d9ca071` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 401.750 | 393.500 | -2.05% | -0.42% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 35150.500 | 32866.500 | -6.50% | -5.07% | 31944.500 | 31502.000 | 64.000 | 127.000 | 104.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 211.450 | 213.150 | +0.80% | +1.83% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 274.800 | 274.300 | -0.18% | -1.12% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 225925.000 | 219727.000 | -2.74% | -0.43% | 4577.500 | 4578.000 | 45.775 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
