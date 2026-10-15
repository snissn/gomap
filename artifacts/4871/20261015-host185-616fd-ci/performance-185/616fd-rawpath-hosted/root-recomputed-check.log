## TreeDB MVCC raw-path gate

- verdict: **FAIL**
- measured threshold observation: **FAIL**
- baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`
- candidate: `616fd1497b3e483f40bfbf3cefe4538d250bb33a`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing acceptance: median paired candidate/base relative delta <= 5% (base/head medians remain reported for context)
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe` | `596355fc27f19ff2bd2c036edcb30a758434cb9a7b0d0fae0ade053bcf59316e` | DIFFERENT |
| caching | `09a243b41aa0486cf78b4e98493204d3620c74bece6780c6c55ac44a2382ab12` | `06b1416f82088ccccc01f5723a241c322db361f4ddf07fa7184ea96997026beb` | DIFFERENT |
| treedb | `4519a5d48cfabf9d68dead1ef4fdb0bd9ca673a99fdbe8451b181ec043824950` | `481ca6fd9f44a08819c10109c2c323402c457277eb99c728cd5d9c20aaa21259` | DIFFERENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| BenchmarkGetVersioned | db DIFFERENT | 560.200 | 635.400 | +13.42% | +13.59% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | FAIL | CANDIDATE | FAIL |
| BenchmarkConditionalTxnBaselineBatchWrite | db DIFFERENT | 61904.500 | 60060.000 | -2.98% | -3.33% | 31944.500 | 31502.000 | 64.000 | 127.000 | 103.000 | PASS | CANDIDATE | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb DIFFERENT | 391.350 | 393.850 | +0.64% | +0.63% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS | CANDIDATE | PASS |
| BenchmarkRepeatedIterator | caching DIFFERENT | 284.750 | 295.050 | +3.62% | +3.84% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS | CANDIDATE | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb DIFFERENT | 179609.000 | 183224.500 | +2.01% | +1.33% | 4580.500 | 4581.000 | 45.805 | 17.000 | 17.000 | PASS | CANDIDATE | PASS |
