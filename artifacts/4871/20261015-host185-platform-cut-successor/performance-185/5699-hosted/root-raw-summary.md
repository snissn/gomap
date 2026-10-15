## TreeDB MVCC raw-path gate

- verdict: **PASS**
- measured threshold observation: **PASS**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `5699f8e791c753f204cc297416e0b742d415afe3`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `6ec1c9db444d065992808eee2b5c482263832aa4f71a2940d1ed3a3168ecda13` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `a59c0a08ca0c27ea0cdd04fd78aacabb81c569caf4895202eed375288e615d47` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `2a2cd75bf3a3eb067529db72cf0b77eb6ffa197c6a3c037697c3c7637d9ca071` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 408.850 | 408.450 | -0.10% | -1.52% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 34429.000 | 32792.000 | -4.75% | -5.60% | 31945.000 | 31502.000 | 64.000 | 127.000 | 103.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 211.900 | 217.600 | +2.69% | +2.90% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 278.050 | 281.250 | +1.15% | +0.62% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 217101.000 | 219232.000 | +0.98% | +1.79% | 4577.500 | 4578.000 | 45.775 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
