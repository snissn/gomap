# Pressure publication and command integration checkpoint

Draft #5242 jointly delivers the remaining #5238 shared-consumer work and #5241 command defaults. Root retains acceptance and merge authority. This is a provisional implementation checkpoint; native/default/parent promotion remains unmet.

## Resulting behavior

Producer pressure reuses the existing captured cut, native build, physical handoff and semantic retirement. With the actually-started positive-threshold size worker and original cleanup hooks, it waits for the original physical seal and accepted-prefix completion without forcing WAL rotation, a whole-backend checkpoint or covered-file cleanup. Other configurations retain full Checkpoint. Explicit Checkpoint, Close, Drain, automatic maintenance, original commands/LSNs/ACKs and finite limits retain their contracts. The existing 2 GiB WAL policy is soft and asynchronous; no hard retention bound or autonomous cleanup deadline is asserted.

Both real commands use one owning public shared-COW DB, its semantic backend, manager and existing deferred-build maintenance. Explicit legacy construction remains available. Native zero-scalar ordinary ColumnGraph retains full JSON. ColumnGraph is durable-only for both owners; unsupported profiles refuse before effects while plain indexed CRUD remains supported. After raw wire mutation, keyword/exact is immediate; pending ANN/Hybrid refuses until existing explicit Optimize. Standalone uses stop/offline public Optimize/restart. Command64 version5 transports ordinary dense results and same-search documents with truthful counters; original versions1–4 remain. No hidden query rebuild or new wire maintenance command is introduced.

## Validation and evidence boundaries

The original pressure causal RED is preserved: durability, LSNs, documents, held view, refund and final zero ownership passed; only active-WAL rotation failed. Frozen pressure source passes32 focused and40 race/maintenance executions, including same-active-file hard-exit/recovery and natural rollover/eventual explicit cleanup. Independent source review accepts the change.

Complete changed command documentservice/nativewire packages pass with unchanged source:48.631s and701.489s,332/1721 pass events including package events,1/3 skips. Earlier two fixture failures and the six-minute progressing-tail timeout remain retained. The migrated default-strategy test preserves unsupported/no-effect controls; the wire test retains its original v1 metadata invalidation rejection, then renegotiates v5 and compares the same filtered request with public exact results.

Combined canonical public checks pass45 test/subtest executions across4 packages, including pressure, command lifecycle and maintained naming inventory. The complete caching owner passes118.408s with2003 pass events including package event and1 skip. Combined command/dense/default race passes67 test/subtest executions across5 packages. All source-bound runners report unchanged source and empty owned process groups at completion. Independent combined source review finds no material interaction issue. Source-derived naming counts and the advisory CI discovery binding are refreshed without changing policy. Final staged-artifact naming inventory also passes; log e8b3013ae6bd295bb3559e005663ec26148645e8dd8cf4bb2eae735575296053.

The pushed predecessor00bd has all4 workflows and38 vet+test jobs passing, including authenticated strict PRIMARY evidence. That CI does not qualify this later combined source. Mature exact-head CI/review/resolved threads and public product/performance/recovery/resource gates remain required.

## Public write diagnostic

Unchanged diagnostic5 uses4096 rows,32 once-only durable requests of128,1/4 writers, fixed legacy/native/native/legacy fresh processes and original Optimize/Close/reopen validation. All8 rows are valid/source-bound, without retry.

| Writers | Time vs preceding native | Bytes | Objects | Native/legacy time |
| --- | ---: | ---: | ---: | ---: |
| 1 | -29.20% | -5.59% | -0.42% | 5.68x |
| 4 | -27.40% | -6.69% | -0.39% | 6.54x |

Four-writer reopen increases6.06ms/12.22%; its cause is unproved and the result is retained. The sum of separately reported write/lifecycle medians falls22.01%/21.00%; this is not a measured end-to-end observation and excludes fixture/read validation. Numeric and default qualification remain null. These measurements bind the pressure+insert-span source before command projection; this unchanged2D diagnostic does not replace the verbatim original8D/4096/fixed128/explicit-native_runtime command corpus. Required native-runtime capacity still depends on external raw#5243; strict#5244 failure remains owned by#5191.

Trace3 confirms pressure rotation/cleanup stacks are gone, while physical publication/preparation remains costly. Source inquiry establishes no safe fsync omission. CPU/allocation sampling finds distributed work. Generic TCIM row allowances cannot be reduced from absent-work observations alone. A provisional successor may extend the maintained direct image builder for FP32 while preserving DeltaVarint/default compression, with a complete constructor/caller ledger and actual public activation/fewer-drain proof before consumer selection or a new native promise. No such successor production change is part of this packet.

## Integrity and reproduction

selected-source-binding.json binds all34 source/test/doc/inventory paths against00bd, including the exact staged tree exercised by combined checks. selected-source.patch.gz reconstructs that delta. sha256.json authenticates every original artifact and deterministic gzip stream. The archive preserves source packets, original failures, raw diagnostic rows, lifecycle costs, traces/profiles, runners, reviews and before/after source receipts. Excluded ELF identity remains in its source-bound manifests and the original local evidence directory.

Use Go1.26.0, GOMAXPROCS4 and the original workload/profile/ACK/finite limits. Task paths in archived scripts are provenance and may be adapted when reconstructing. No final-five qualification or hosted mature review was requested for this still-incomplete performance candidate. Current routing and complete failed/null history remain in the parent rolling checkpoint.
