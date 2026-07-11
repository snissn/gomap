## TreeDB MVCC raw-path gate

- verdict: **EQUIVALENT**
- measured threshold observation: **FAIL**
- baseline: `f9c9b2a37838909d0e669818cfa2840c0a8d5f85`
- candidate: `dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a`
- samples: 8 per revision, benchmark-group-paired alternating AB/BA order
- timing threshold: candidate median <= baseline median + 5%
- allocs/op threshold: candidate median must not increase
- B/op jitter threshold: candidate median may increase by at most the smaller of 1% or 64 B; zero-B baselines remain strict
- equivalence acceptance: every row-producing base/head benchmark binary is byte-identical, so the measured delta is retained but is not attributable to the candidate revision

| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |
| --- | --- | --- | --- |
| db | `42e584b3fb32dec8c901966a2921fcd8fdd03c44da58881b20b1d5e5982651c3` | `42e584b3fb32dec8c901966a2921fcd8fdd03c44da58881b20b1d5e5982651c3` | EQUIVALENT |
| caching | `3b4e906530b05cad58b68abd326f08946425c975664dbe40252339a26ba33b2f` | `3b4e906530b05cad58b68abd326f08946425c975664dbe40252339a26ba33b2f` | EQUIVALENT |
| treedb | `1e9a6b4f060be007fa025a74b839b5a7e574268b2bf7ecf62efd1a3f5ed1548f` | `1e9a6b4f060be007fa025a74b839b5a7e574268b2bf7ecf62efd1a3f5ed1548f` | EQUIVALENT |

| Benchmark | Binary | Base ns/op | Head ns/op | Delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| BenchmarkGetVersioned | db EQUIVALENT | 615.950 | 622.850 | +1.12% | 480.000 | 480.000 | 4.800 | 1.000 | 1.000 | PASS |
| BenchmarkConditionalTxnBaselineBatchWrite | db EQUIVALENT | 18772.500 | 18840.500 | +0.36% | 13242.000 | 13241.000 | 64.000 | 30.000 | 30.000 | PASS |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | treedb EQUIVALENT | 279.200 | 277.900 | -0.47% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | PASS |
| BenchmarkRepeatedIterator | caching EQUIVALENT | 227.150 | 223.150 | -1.76% | 112.000 | 112.000 | 1.120 | 2.000 | 2.000 | PASS |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | treedb EQUIVALENT | 383380.500 | 490497.000 | +27.94% | 4560.000 | 4557.500 | 45.600 | 17.000 | 17.000 | FAIL |
