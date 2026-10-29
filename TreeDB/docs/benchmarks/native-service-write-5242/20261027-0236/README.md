# Final physical insert-span checkpoint

Draft #5242 / #5238 / #5225: materialize one physical plan per eligible ordinary FullJSON, graph-free, homogeneous insert span. Preserve every original command, accepted-prefix cut, namespace-member LSN, durable ACK, old captured view and original finite construction ownership. Other operation/graph/import paths retain the maintained member loop. One common constructor suffix serves both inputs; no synthetic command, new publisher or cap change.

The public regression fails original production only for four content Syncs instead of one, after all semantic/WAL/held-view/reopen/Close-zero assertions pass. Changed production passes that test and repeated nonempty physical-basis controls. All58 maintained risk checks and24 race executions pass, including8/8 original public4096 concurrent lifecycles. Independent source review found no material defect.

The first full collection run hit its six-minute package timeout;332 documentservice checks passed. The adequate-limit collection run completed595.994s with6976 pass events,10 skips and one naming-inventory failure. Its two additional uses of existing API names were classified in the existing inventory entry; the focused guard then passed39.182s. No runtime production changed for that repair. Staging the new public fixture then exposed seven lines/fourteen uses of the same maintained schema API; those are now classified too. All failed runs are retained. Final staged-artifact inventory passes38.080s, log6a28eefff930e0565b906dcfe39586f406798e322b92bfcc5fb60052494f30e8; its source-bound receipt/log/plan are archived. Full-suite test events include nested cases.

## Public write evidence

Diagnostic4 uses the unchanged harness and runner: four fresh legacy/native/native/legacy processes, each one/four writers,4096 records,32 once-only durable requests of128, required Optimize/query/document/Close/reopen. All eight rows are valid, source unchanged, no retry. Legacy resolves requested COW to mutable btree; this compares publication architectures, not isolated memtable overhead.

| Writers | Time vs prior native | Bytes vs prior native | Objects vs prior native | Current native/legacy time |
| --- | ---: | ---: | ---: | ---: |
| 1 | -16.77% | -31.53% | -30.48% | 8.2494x |
| 4 | -17.48% | -33.85% | -30.47% | 9.3060x |

This is a bounded partial improvement. Numeric gate and final qualification remain null; the material remaining regression prevents default acceptance. Earlier failed/null results remain failed/null in prior committed packets.

Trace2 uses the exact diagnostic4 ELF. Both native/legacy four-writer cells pass original ACK/lifecycle checks. Attributed preparation syscall delay is462.85ms versus41.39ms direct completion. Nested/cross-goroutine durations overlap, are not additive wall time and do not qualify speed. The next selected inquiry is pressure-only durable accepted-prefix materialization, reusing maintained drain/seal/custody/refund joins with full-checkpoint fallback where WAL maintenance capability is absent. No pressure implementation is part of this source delta. Active WAL recovery and sustained/natural rollover cleanup remain required; default2GiB cleanup policy is soft asynchronous, not a hard total bound.

## Remaining delivery gates

Current040 hosted CI completed36 successful jobs, one strict raw-path PRIMARY failure (+2B/op;97359 to97361,358 objects flat, paired time-0.279% pass), and its dependent required aggregate failure. All16 original samples and complete non-ELF artifact contents are archived here;226127039-byte artifact ZIP and its original SHA remain locally/hosted retained. M0, metadata and snapshot workflows pass. No waiver or unchanged retry is selected;040 CI does not certify this later source delta.

Native/default/parent promotion is unmet. Required owning/race/compatibility, same-cut/ACK/recovery, sustained resources, public performance and mature exact-head CI/review/resolved-thread gates remain. Raw #5243/#5244 strict failure remains owned by #5191. Command #5241 is provisional and must complete real ordinary ColumnGraph wire support and its unchanged8D command corpus, actual ACK/recovery and public performance after accepted #5238. No HNSW fixture substitution. The rolling parent checkpoint records current routing and all historical failures.

## Integrity and reproduction

selected-source-binding.json binds the five staged paths against040; four measured files are unchanged and the fifth is the inventory-only correction. Apply selected-source.patch.gz on that base. sha256.json authenticates each original and deterministic gzip stream; decompression recovers exact plans, runner/harness, source/review packets, all raw logs/rows/receipts and profiles. Excluded binaries/ZIP retain authenticated identity in archived manifests. Original task paths are provenance; adapt paths when reconstructing while keeping the recorded Go1.26.0, GOMAXPROCS4, workload/profile/ACK/limits. No final-five campaign or hosted review has been requested for this immature candidate.
