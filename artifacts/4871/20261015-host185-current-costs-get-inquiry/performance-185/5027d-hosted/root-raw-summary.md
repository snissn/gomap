## TreeDB MVCC raw-path gate

- verdict: **FAIL**
- measured threshold observation: **FAIL**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `5027dc559f83dee04317910169c48d277fcce72f`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `f31f04240cd874be28fb1fa22db5449217dd578dddbdbc5507d72af4fc53416d` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `a9493d0cba19951d8ca0df04c391b539482a7c09f4ad4f0b01ec5f23cbbd5cb5` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `151f04e88801d4a5f63e8393a344e8c0be41b0015c294a510a1caf91bf17a59b` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 443.100 | 485.350 | +9.54% | +10.34% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 47650.500 | 48014.500 | +0.76% | +0.35% | 31944.500 | 31502.000 | 64.000 | 127.000 | 104.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 303.200 | 304.850 | +0.54% | +0.33% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 224.800 | 222.000 | -1.25% | -1.13% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 188651.500 | 190109.500 | +0.77% | +0.39% | 4577.000 | 4555.500 | 45.770 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
