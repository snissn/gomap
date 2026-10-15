## TreeDB MVCC raw-path gate

- verdict: **FAIL**
- measured threshold observation: **FAIL**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `f0415773f13a522dea5c1debdaecba09d15585dd`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `2dbb31dd8d2979d21ea6896a5104852e5ae76c9ca9c82820b144d35ca683d952` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `307472a560b1e46a2e1edda48bdac5b20cdd4cb101c92e9c9baf99e27f323e43` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `e517ed2b379a61d75577a5a4f572a3ff551d26036822829dd84e15710887b5ba` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 664.600 | 751.150 | +13.02% | +13.09% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 54106.000 | 51721.000 | -4.41% | -4.92% | 31945.000 | 31502.000 | 64.000 | 127.000 | 104.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 347.150 | 369.800 | +6.52% | +6.54% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkRepeatedIterator | caching DIFFERENT | 378.700 | 378.350 | -0.09% | +0.04% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 179397.500 | 180442.000 | +0.58% | +0.89% | 4581.000 | 4582.000 | 45.810 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
