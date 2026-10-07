# Final E closure audit — preparation only

This audit supplies report fields and already-selected commands, not acceptance. Five live issue bodies were read once with `gh issue view NUMBER --repo snissn/gomap --json number,title,body,state,url,updatedAt`; all returned exit0. They retain 37 criteria:8 checked/29 open. The current closeout map has the same checklist. No CI polling, capture, Go execution, existing helper edit or publication occurred. Root owns the exact a645 required gate and any successor decision; this note does not assert that it passed or landed.

The compact template covers all37 criteria. Its 3182/90fa state rows and the readiness note's selected5072 source are historical snapshots: replace them with the **actual selected5071 landing**, root's corresponding review/required-CI/thread record and original retained receipts when those exist. Preserve historical numerical rows and original identities. Do not mechanically substitute a645 or any premerge head as a measured/landed source. The existing one-E-PR publication sequence supersedes older land-first wording.

## Concrete report tables

| Table / keys | Required fields and calculation / source |
| --- | --- |
| Delivery / source authority; A,C,D separate rows | Actual source and verified landing commit; review/CI record; clean source before/after; runtime, distinct harness, fixture/config, actual build argv/toolchain/compiler/library/runtime environment/host/device; independently observed executable SHA before run; exact completed packet SHA; independent validation argv/exit/log; root acceptance decision. Publish literal values from original authenticated observations. D compiled-test harness is not A/C harness. |
| A all-six-engine × phase | Engine, phase, supported/skipped reason, five repetitions, operations/call and logical rows/call, median ns/call, calls/s, rows/s, Go B/call, objects/call, medians of per-repetition p50/p95/p99, min/max/spread, baseline and candidate, comparison classification, throughput ratio and allocation deltas. Keep ordinary point/public range, prepared point/batch, quiescent range decomposition, read-state transition, view setup, first fetch, mutation and checkpoint rows distinct. |
| A comparison decisions | Per-group stable matched/inconclusive/newly-enabled/removed plus reason. Original helper uses `(max throughput-min throughput)/median throughput`; **either** side >0.15 makes a matched timing inconclusive. Ratio=candidate median calls/s ÷ baseline median calls/s; delta B or objects=candidate median−baseline median. Optional percentage allocation delta=100×delta/baseline median only for nonzero baseline. No ratio for unsupported complete-row baseline. Do not label throughput and latency ratios interchangeably. |
| A storage/capability | Cell `storage_boundary`, `persistent_bytes`, `wal_bytes`, `transient_bytes`, declared stats/components and verified SQLite settings; median/range by engine across repetitions. This is the declared boundary census, not a new whole-run peak. WAL and SQLite transient SHM remain separate; Go allocations omit SQLite C allocations. Preserve all `capabilities`, `oracle_verified`, unsupported and skipped reasons. |
| C sixteen dimension groups | bio96/4096 × request rows1/32 × indexed/unindexed × bio/email+city; five repetitions/group and80 original cells. Median ns/request, requests/s, rows/s, Go B/request/objects/request, medians of per-repetition p50/p95/p99 and heap-after; ACK spread and separately Flush ns/spread. The reviewed summary derives requests/s as `1e9/ns_per_op`, rows/s=requests/s×request_rows. It is a current generic UpdateBatch cost sweep, not before/after optimization evidence. |
| C actual work | ACK and Flush counter deltas separately: command-WAL append/write bytes/sync/file-sync count+duration, UpdateBatch calls/items/current-read/prepare/publication and indexed-flush publication. Preserve raw counters and all repetitions. If normalizing counters, divide each raw delta by that cell's actual requests or rows **before** group summary and label units. Every flush/reopen full-row and current/historical posting oracle must pass. Metadata/reference-only guarantees remain separately linked correctness proof. |
| D process/epoch metrics | Five fresh OS processes × five final epochs;4096 live rows/1024 calls/epoch,512 repeated IDs; distinct/new/revisited/cumulative counts+hash. Total calls=epochs×calls/epoch; mean loop ns/call=`call_ns/total_calls`; mixed calls/s=`total_calls×1e9/call_ns`; Go B/call=`loop_bytes/total_calls`; objects/call=`loop_allocs/total_calls`. Keep epoch ns/op and process elapsed distinct; calibration raw logs excluded from final results. Median/min/max per-process metrics; report actual load and descriptive spread rather than imposing a new threshold. |
| D per-phase component trajectory | Every process and named ingest/churn/checkpoint/fold/vacuum/exhaustive/final-GC/maintenance/release/reopen census: index,persistent vlog,persistent leaf log,typed assets,redo WAL,dictionary/template stores,immutable manifest metadata,other; bytes and regular files. Total per observation=sum components; persistent payload total=all−redo WAL. Growth=phase−ingest and adjacent-epoch delta, **computed within each process then summarized**, not difference of component medians. Keep full trajectories/min/max and explain remaining active/partial/recovery/pin retention. |
| D memory/maintenance/reclamation | Phase HeapAlloc/HeapInuse/objects, sampled high and post-GC retained heap; full API timer sum and per-operation times (fold,rewrite,checkpoint,exhaustive compaction,direct vacuum,final same-LSN refresh,typed/leaf/revision/value GC). Report actual eligible/protected/debt and source/ref/segment/mapped/handle stats, both slots/commit/LSN/NextLSN before/after. Typed actual deleted bytes/files and leaf logical marking vs physical unlink remain distinct. Revision16-handle batching/full scans/whole-call locks have actual finite maintenance cost to interpret. |
| Correctness/example/limits | Current/held/prepared full-row/index and release/reopen oracles; meaningful ACK/process-cut and alternate-meta recovery source applicability; actual final example run/vet command+exit/log; supported required strings+residual JSON; durable ambiguity/reconcile rule; ordinary/prepared ownership; direct-backend D omits cached-wrapper cost. Process cuts are not power-loss proof. No five-epoch/infinite/full-population/whole-DB bound or production4GiB crossing inference. |
| Public evidence/replay | Immutable text evidence commit/path URLs, unique overlay tag/asset identity/hash/size, core+supplemental ledgers, source receipts/certificate/frozen validator identities, fresh public download/restore/original-binary semantic validation argv/env/host/exit/log and root acceptance, descendant report proof SHA and actual E review/CI/merge. Private preparations and replay from private copies are not public download proof. |

A row throughput uses each raw phase's `ops_per_sec × rows/operations`; setup phases with rows0 have no useful row throughput. Percentiles above are medians of per-repetition percentiles, never pooled quantiles. SQLite total memory, RSS and unsampled heap peaks are unavailable. D samples include oracle/latency bookkeeping. Source classes may overlap, so summed class bytes are not a unique retained-byte total.

## Commands after actual root selection/qualification

These are templates for future root actions, not executed here. `SOURCE/LANDING/RUNTIME/HARNESS/BINARY_SHA/PACKET_SHA` and paths must come from the separate observed receipt; all outputs are NEW. Use the already reviewed observers and actual canonical host lock, not direct capture commands.

1. Independently validate original bytes before deriving tables:

```sh
A_OUT/collection_workload_bench r1-validate -source-manifest A_RECEIPTS/independent-expected-source.json A_OUT/packet.json
C_OUT/collection_workload_bench r1-mutation-sweep-validate -source-manifest C_RECEIPTS/independent-expected-source.json -expected-commit SOURCE -expected-runtime RUNTIME -expected-harness C_HARNESS -expected-landed-tooling-commit LANDING -expected-binary-sha256 BINARY_SHA -expected-packet-sha256 PACKET_SHA C_OUT/packet.json
python3 SELECTED_REPO/scripts/r1_lifecycle_validate.py D_OUT/packet.json --expected-commit SOURCE --expected-runtime RUNTIME --expected-harness D_HARNESS --expected-landed-tooling-commit LANDING --expected-binary-sha256 BINARY_SHA --expected-packet-sha256 PACKET_SHA > NEW_D_VALIDATION_AND_SUMMARY.log
```

A has no six-pin flags: its observer independently checks actual executable/packet/source-manifest bytes before/after original CLI validation. Producer selfchecks, especially C `validation.txt`, remain unqualified. D CLI both validates and emits the reviewed summary; do not recompute/rebind original `summary.md`.

2. Certified final A comparison uses the original math and explicit approved semantic exception for unused C harness additions:

```sh
python3 compare-certified.py --certificate ROOT_APPROVED_CERTIFICATE.json --expected-certificate-sha256 INDEPENDENT_APPROVED_SHA --repo REPO_WITH_BASELINE_AND_SELECTED_OBJECTS --before ORIGINAL_BASELINE/packet.json --after A_OUT/packet.json --receipts A_RECEIPTS --accepted-freeze ROOT_ACCEPTED_FREEZE.json --original-compare ORIGINAL_compare_r1_packets.py > NEW_A_COMPARISON.json
python3 /tmp/gomap-r1-execution-20261005/mutation-receipt-preparation/summarize.py C_OUT/packet.json > NEW_C_SUMMARY.json
```

No Python optimization. Preserve distinct baseline/final runtime and full harness hashes. Certificate covers A workload semantics, not product binary/performance equality. Freeze its actual selected Git blobs, reviewed diffs and12 original receipt-file pins; match actual baseline host111/device/compiler/SQLite environment from observations, never copying expectations into observed data. Baseline0216 remains unchanged.

3. Final example at the actual landed source (root-owned future Go execution):

```sh
GOWORK=off go vet ./examples/typed_rows
GOWORK=off go run ./examples/typed_rows
```

Record actual pinned toolchain, complete command/exit/output and retained new DB path. Existing example README confirms this route; sparse absence is not absence from Git. No command ran in this audit.

4. After **separate** A/C/D acceptance, append their actual records to unchanged13-record selection, preserve original independent logs in new publication-input mirrors, and use existing helper:

```sh
python3 prepare-evidence-publication-final.py --captures NEW_ACCEPTED_16_RECORD_SELECTION.json --out NEW_STAGE
sha256sum -c PUBLICATION_SHA256SUMS
python3 validation/verify-publication-replay.py --stage PUBLIC_DOWNLOADED_STAGE --out NEW_RESTORED_TREE
```

The ledger check runs in actual downloaded stage after root publishes its immutable text commit and exact overlay. Use approved nine-group assembler only on actual selected stage; no duplicate support collection needed. Replay each original binary/validator with authenticated external pins and replay the certified A comparison. Stage construction/restoration alone is not semantic replay or acceptance. The existing root-publication-sequence.md keeps one E PR: artifact text commit/tag/upload→fresh replay→descendant report/proof→E review/CI/merge.

## Concrete remaining acceptance gaps

1. **Landing/gates:** root must finish actual mature required CI/review/threads and record real5071 landing, then reconcile incorporated5072. This audit intentionally does not observe current CI; new failure diagnosis may change selected source. An earlier hosted clean or focused pass cannot fill this field.
2. **Independent current measurements:** none of the inspected preparation artifacts is the future actual selected-source C80-cell sweep, A30-cell six-engine packet or D5-process/5-final-epoch retained packet. Root must supply clean-build/run/executable/packet receipts, external semantic exits/logs, actual child/host/device observations and actual approved A certificate. Full runtime inequality excludes wholesale reuse of historical measurements.
3. **C6:** all16 width/request/index/change groups and actual sync/publication/Flush work require cost/noise interpretation; metadata reference-only correctness is not their cost measurement. Existing serial deferral of concurrency/larger populations remains explicit, not an invented new test gate.
4. **D/5066 finite guard:** full component and memory trajectories, real unlink and held/both-recovery protections plus maintenance pause/allocation costs remain acceptance evidence. Tiny metadata plateau/pack cleanup/scaled rotation/descriptor safety are useful scoped support, not retained guard acceptance. Active vlog/partial segment history requires lawful measured explanation; no logical debt0/soft-cap waiver. Any actual breached invariant remains a root-owned blocker with its original failure evidence.
5. **Current supported application/integrated report:** execute final example/vet and preserve row/posting/reopen/lifetime outputs; replace historical template source/gate placeholders only with actual facts. Preserve accepted B/current scoped source-applicability, old numeric identities, capability deferrals and1954/5037 boundaries. Verify native links and canonical integrated docs as part of E review.
6. **Public E:** actual accepted originals, independent receipt authority, immutable URLs/overlay hash, fresh public restore+original semantic+certificate replay, and final E merge remain absent. Nine support groups are frozen private preparation, not public acceptance. Root must decide C,D,E,5066 and then reconcile the parent; parent stays open until its criteria are satisfied.

This is a completeness check against current ticket requirements, not a new policy or hardening request. There is no duplicate lifecycle authority, hypothetical gate, automatic numeric capacity decision or permission request in this note.
