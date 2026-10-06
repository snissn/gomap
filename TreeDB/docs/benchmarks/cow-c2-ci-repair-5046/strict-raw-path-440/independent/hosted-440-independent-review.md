# Exact440 hosted raw-path independent review

**ACCEPT — all five measurement rows pass the original unwaived thresholds.**

Candidate `4403776d10b6ac82eb9e1839f437f59a78058816` versus base `edbd68d34b0fdf7152a498b87d373576de029721`; hosted job112196498298 completed successfully at 2026-10-06T09:20:43Z. Artifact11401980581 contains40 baseline and40 candidate rows; all40 AB/BA benchmark-group pairs match collector order, eight samples/revision/group.

Thresholds remain paired-median timing ≤5%, median allocation count cannot increase, median bytes increase ≤min(1% baseline,64B), strict zero-byte baseline. Current collector/analyzer bytes match the base. Their thresholds match the original failed8a159 gate. All six reported binary digests differ; equivalence acceptance was neither needed nor used.

| Benchmark | Base/head ns/op medians | Paired timing delta | Base/head B/op | Base/head allocs/op |
|---|---:|---:|---:|---:|
| BenchmarkGetVersioned | 434.75 / 425 | -1.020641% | 0 / 0 | 0 / 0 |
| BenchmarkConditionalTxnBaselineBatchWrite | 79586 / 81382.5 | -1.675387% | 152689 / 152744 | 281 / 281 |
| BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek | 204.5 / 212 | +3.148270% | 0 / 0 | 0 / 0 |
| BenchmarkRepeatedIterator | 276.65 / 272.45 | -1.429011% | 112 / 112 | 2 / 2 |
| BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1 | 217596 / 220152 | +1.229491% | 4577.5 / 4577 | 17 / 17 |

Independent parser/statistics reconstruction matches every original numeric/pass field and current analyzer functions. JSON retains all actual sample values, spread through paired sample deltas, private input hashes, source bindings and sanitized order markers. Go1.26.8 linux/amd64, CPU0/GOMAXPROCS1, Ubuntu24/EPYC9V45 are retained by environment.txt; batch baseline-write1000x, remaining groups2s.

Limitation: six executable digest **references** agree between binary-sha256.txt and summary.json. Actual executables were not transferred and cannot be independently rehashed here. No binaries were invented or rebuilt. Unrelated process argv remains private: processes.txt was read only to verify order and hash-bound; no process rows are copied into this review. Historical974 experimental COW cost evidence remains separately scoped and is not substituted for this current gate. No source edit, new Go validation, workflow rerun, or publication action occurred.
