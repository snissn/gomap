# Absent physical closure repair and PRIMARY performance diagnosis

Artifact-only recovery packet for gomap #5111 / PR #5127 at product parent b3c7ff702538e552e685ded554a79eb256a3380c. Product HEAD is unchanged. The exact 20-path source overlay is included; source-manifest SHA256 is b8e825578a7edb865c76220b33c1726165f80c4fc981c864382e4440f98b880b. This is diagnostic evidence, not merge acceptance.

The repair propagates the existing nil physical-closure representation through internal publication capture, lazy assembly and exact generic empty clones. It preserves selected metadata, independent directories, pins and exported snapshot/builder contracts. The original DATA terminal receipt now completes even when its physical resource Set is absent. Meaningful public-path and actual single/coalesced coordinator RED controls are retained, followed by 13 focused normal controls, three changed-custody race controls and the real PRIMARY generation/materialization/reopen control passing.

## Short uninstrumented comparison

Baseline main 5bca87ffe6e1bfba2d693264d2dc82936adc234f; candidate b3c7 plus exact overlay. Host 192.168.0.111, pinned Go 1.26.3, CPU0, GOMAXPROCS=1. Three AB/BA/AB pairs per operation, all 18 samples retained. GetVersioned uses 500ms; ConditionalTxnBaselineBatchWrite 1000x; CommitAtPrimaryOnlyAfterDataGeneration 128x. Frozen input/HEAD/index/ELF identities are recorded before and after.

| Operation | Main ns/op | Candidate ns/op | Main B/op | Candidate B/op | Main allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GetVersioned | 319.1 | 331.8 | 0 | 0 | 0 | 0 |
| DATA batch | 21244 | 20356 | 31944 | 31448 | 127 | 105 |
| PRIMARY public CommitAt | 143190 | 499802 | 279177 | 368445 | 474 | 473 |

Get +3.98% and DATA -4.18% fit the bounded timing guard; DATA bytes fall 496 and objects fall 22. PRIMARY remains RED: +249.05% time and +89268 bytes (+31.98%). No threshold is waived. The benchmark file is identical on both binaries (SHA256 ec137a4fbc8f4c7b5038e53f16e5b7cd8ff922cb9dfa989a214082b55e36ee11); baseline uses only this released fixture overlay. ProfileNoWALFast, DisableSideStores=true, checkpoint interval=-1, pointer threshold4096, CommitDurable, initial33 then64 actual DATA materialization, repeated equal physical-key CommitAt(100). Full CommitAt and physical ACK remain inside timing; setup/Stats/Close are outside. Generation advances/op 2 -> 0 is the intended PRIMARY routing, separately checked against later true DATA materialization and reopen.

## Direct causal profiles

One matched set: separate CPU4096x baseline/candidate and allocation512x baseline/candidate (rate1 only on allocation invocation). Same frozen binaries/configuration, no overlapping runtime. Four captures exited0 within the fixed120s outer bounds. Profiles cover the whole process, including setup and teardown; instrumented timings are not mixed into the uninstrumented scorecard.

CPU samples: baseline710ms, candidate2.14s. Candidate SHA256 block flat1.18s (55.14%); candidate external-reference/maintenance scan cumulative1.30s (60.75%), versus about70ms baseline. VerifyPrimaryOperand and ValidatePrimaryComponent overlap that scan. CPU work explains the measured wall regression; this evidence does not indicate durability waits as the dominant cause. The next performance priority is the existing exact dependency/immutable producer evidence seam, eliminating redundant full scans or validation while preserving changed-operand, corruption, identity, lifetime and durable ACK contracts. No replacement route is yet accepted by this packet.

Allocation512x invocation reports about279208 ->368409 B/op. Whole-process allocation tops include append-only memtable entries, PrimaryBankMetadataEdgesOwnedV5, maintenanceReachabilityScan and iterator construction. Cumulative and flat totals overlap and include setup; do not add them as independent operation costs. Source overlays, all failed and passing controls, raw samples, commands, four profiles and 12 top reports are retained in the archive. ELF binaries alone are excluded; manifests bind their identities.

All remaining CI, real retention/recovery/platform defects, exact-candidate independent Sol review, native integration and final public Dgraph/performance outcome gates remain open. The same source owner continues; root owns Git/GitHub/merge. This packet is recoverable evidence, not completion of #5111 or graph #4871.
