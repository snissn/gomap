# PR #5071 MVCC raw-path failure diagnosis

Exact comparison: baseline `edbd68d34b0fdf7152a498b87d373576de029721`, candidate `cb09ffe33b10a8a623cd335a43780d4997524f82`. Read-only diagnosis of retained artifacts and Git objects; no Go build, test, benchmark, GitHub write, or polling was performed.

**Original observation remains FAIL / accepted=false.** The failing inline dirty one-key WriteSync cell reports paired median **+11.1942906566171%** against the unchanged 5% limit. Its independent medians are 744192.5 to 436736 ns/op (**-41.31410891671173%**), which the gate reports as context. Allocations are unchanged at 17/op; bytes rise 2.5 B/op within the 45.78 B/op allowance. Other four cells pass.

Pairing is valid: the script invokes adjacent baseline/candidate pairs, odd AB and even BA, and appends each revision in sample order; the checker zips those ordered rows. Raw rows reproduce the exact failed paired statistic. The difference between the two statistics follows from three very large candidate wins and five smaller losses; it is not a parser or pair-index defect.

| Sample | Order | Base ns/op | Head ns/op | Paired delta | Total delta ns/op | File-sync delta ns/op | Delta outside file sync |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | AB | 616869 | 656066 | +6.354% | +39197 | +40413 | -1216 |
| 2 | BA | 1692455 | 944275 | -44.207% | -748180 | -746591 | -1589 |
| 3 | AB | 871516 | 1043732 | +19.761% | +172216 | +172305 | -89 |
| 4 | BA | 901331 | 353772 | -60.750% | -547559 | -548767 | +1208 |
| 5 | AB | 1038199 | 357307 | -65.584% | -680892 | -680044 | -848 |
| 6 | BA | 436177 | 516165 | +18.338% | +79988 | +82768 | -2780 |
| 7 | AB | 285848 | 331682 | +16.034% | +45834 | +45591 | +243 |
| 8 | BA | 202016 | 248304 | +22.913% | +46288 | +43999 | +2289 |

File sync occupies 91.83% to 98.83% of every invocation's ns/op. Across all pairs, the total change differs from the file-sync change by at most 2780 ns/op. The baseline max/min ratio is 8.38x and the candidate ratio is 4.20x. Every sample has one file sync, zero checkpoint runs, zero vlog syncs and zero vlog materialization syncs. Large changes therefore track actual `os.File.Sync` duration, not additional barriers or maintenance work. Subtracting that duration is a diagnostic decomposition, not a new acceptance metric.

AB median is +11.194%; BA median is -12.934%. Both orders contain gains and losses, so the data do not establish a universal candidate-second/order penalty. Process snapshots show no competing benchmark process, but cannot exclude host/device I/O contention. The broad decline over time is consistent with a settling or varying I/O environment; that remains an inference.

Relevant source comparison: the public benchmark, public batch wrapper, caching package, command-log writer, gate checker and gate script are unchanged. The new appender metadata retirement acts only after value-log pointer production; this inline cell records no value-log materialization. New immutable-manifest GC and packed-resource projection act during maintenance/rebuild; this cell disables background maintenance and records zero checkpoints. Value-log retry-worker shutdown is in DB close after StopTimer. No changed timed-path operation was identified.

**No binary-equivalence acceptance is available.** All three package executable SHA-256 pairs differ. Unchanged relevant source is useful attribution evidence but does not prove equal generated machine code or eliminate code-layout effects. No retained durable-cell executables/objdump/profiles exist in this packet; the script only retains those when the repeated-iterator cell fails.

**Judgment:** variable file-sync latency is the leading supported explanation. This packet cannot conclusively distinguish runner noise from candidate regression. Preserve the red observation and its exact artifact; do not alter the paired statistic, threshold, samples, or equivalence policy.

**Targeted next action:** One exact-head/base rerun of the completed MVCC raw-path CI job with the existing eight paired samples and unchanged 5%/allocation/bytes contract; retain and report the original failed observation. If the durable cell fails again, stop retrying and collect one focused durable-cell diagnostic with retained exact executables, identical fixed iteration count and balanced AB/BA order, per-stage and fsync/host-I/O evidence. That diagnostic investigates attribution and does not replace the gate result.

Source evidence:

- [paired statistic](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/.github/scripts/check_mvcc_raw_path_gate.py#L135-L170)
- [binary equivalence](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/.github/scripts/check_mvcc_raw_path_gate.py#L206-L235)
- [sample order](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/scripts/mvcc_raw_path_gate.sh#L130-L171)
- [benchmark options](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/command_wal_public_test.go#L616-L631)
- [benchmark timed loop](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/command_wal_public_test.go#L4183-L4251)
- [file sync instrumentation](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/internal/commitlog/writer.go#L302-L320)
- [changed appender metadata](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/db/wal_recovery.go#L1244-L1314)
- [changed manager shutdown](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/internal/valuelog/manager.go#L1758)
- [changed manifest gc](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/db/leaf_manifest_revision_gc.go#L20)
- [changed rebuild capture](https://github.com/snissn/gomap/blob/cb09ffe33b10a8a623cd335a43780d4997524f82/TreeDB/db/durable_root_runtime.go#L1208)

Original files: `/tmp/gomap-r1-execution-20261005/cb09-mvcc-artifact/summary.json`, `/tmp/gomap-r1-execution-20261005/cb09-mvcc-artifact/baseline.txt`, `/tmp/gomap-r1-execution-20261005/cb09-mvcc-artifact/candidate.txt`, `/tmp/gomap-r1-execution-20261005/cb09-mvcc-artifact/processes.txt`, `/tmp/gomap-r1-execution-20261005/cb09-mvcc-failed.log`. JSON companion includes exact pair metrics and original digest/result values.
