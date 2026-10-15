## TreeDB MVCC raw-path gate

- verdict: **PASS**
- measured threshold observation: **PASS**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `a8c52ed66fc587d4823f020cf15ea1c3ee8c1ebc`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `5e182ceb21aa29ad3a271b380ab3afce4a93db1a09d0cc6f6b4c3b50e60e50f7` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `797acda748da99a0867b5bd8d6f606919848bd680bbd9c827eca035e606231ea` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `edef866f664c149faf9b285c7c05c2f814cef0339363ad24210ab749c8dc29dd` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 418.500 | 410.350 | -1.95% | -1.72% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 35944.000 | 34763.000 | -3.29% | -3.83% | 31944.500 | 31502.000 | 64.000 | 127.000 | 104.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 214.600 | 218.550 | +1.84% | +1.84% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 284.150 | 282.000 | -0.76% | -0.39% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 231392.000 | 233658.000 | +0.98% | +0.28% | 4578.000 | 4578.000 | 45.780 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
