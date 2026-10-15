# Actual33 pre-effect geometry result

The one authorized correctness run PASSed on the existing actual33 fixture. Source is FROZEN; no further source/runtime work is running. This is a scalar pre-effect probe and existing refusal/custody controls, not a split-success diagnostic. No builder, enqueue, activation, pager count installation, candidate physical write, visibility, fence, Consume or Finish was driven by the probe. Original Q32/1MiB/input32/resource16/file17/two-index/SAMERequest33/80/4096 guards remain unchanged; **Q32 RED / ACK0**. No allowance was chosen.

## Exact source and command binding

Base HEAD `2bb79593fd5b5a6a1dc919bae32cd6158345daac` plus the seven paths below. Both `native-build/pre-effect-geometry/before-all-source.json` and `after-all-source.json` contain all 10,308 tracked/untracked source paths and are byte-identical, sha256 `16c532a90a41b494fe346b83060d86edec1cb9b936db6923acefd5a467d280c4`. Full test log sha256 `35392bc921d912e05146dc750cb203bff2e293b3a012fa76ce643f56f233247b`; receipt.json contains exact command, exit0, before/after equality and changed inventory. Source hashes were captured BEFORE compilation and AFTER the run; no later test hash was substituted.

Command (cwd `native`):

```sh
env GOROOT=/mnt/fast4tb/gomap-mvcc-4871-20261015/toolchains/go1.26.3 GOTOOLCHAIN=local GOWORK=off GOPROXY=off GOMAXPROCS=2 TMPDIR=/mnt/fast4tb/gomap-mvcc-4871-20261015/native-build/tmp GOCACHE=/mnt/fast4tb/gomap-mvcc-4871-20261015/native-build/cache /mnt/fast4tb/gomap-mvcc-4871-20261015/toolchains/go1.26.3/bin/go test -p 1 ./TreeDB/db ./TreeDB/mvcc -run 'TestNativeStablePagerPartialTokenCancellationV6|TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6|TestNativeMVCCCopiedParentAcrossBothDurableSlotsV6' -count=1 -v -timeout=3m
```

PASS: partial-token cancellation at before-token/before-retain/after-retain; actual route refusal/custody/newer real durable writer/stale source/old reader/reopen; copied-parent cancellation and stale parent after both durable slots move. This is correctness-only; no performance claim, benchmark, larger diagnostic or new public qualification.

## Actual retained geometry

* Same actual33 Request floor32, one selected deletion and retained anchor; actual route refuses at call147/phase26 with `native MVCC transaction requires its original work-bearing caller`. Original candidate stage8, original seal stage13, promotion present, both original index operands retained.
* Exact basis booleans all true: current index, expected sequence, full current parent Ref/promotion, full record/meta/slot, both activation headers ready. Leaf-generation view nil, active conditional oracle empty. Runtime prefix1, debt len/cap0/4, seals0/0, visible0/4. Forecast after original activation is P=D=S=V=1.
* Ledger owners0/candidates1/selected IDs0/private DATA1/new distinct IDs1/burned0/coverage0. Append placement tail13/count3; current live high-water13, candidate required16; actual metadata images13..15, all4096 bytes. Activated0/cap0. Projected actual reservation IDs after late claim N=1 (not a guessed tail count). `TailWritten=true` is the existing ledger's `tailWriteAttempted` flag: original emit marks it conservatively before recording the private candidate page image; it is NOT evidence that the candidate has been physically installed. The probe never changes it.
* Coordinator pending/cap0/0, builders/dispatch/native dispatch/receipts/waiters0, pin/high-water map cardinalities0/0, publishing/preparing false, timer absent, real clock true. Actual typed builder headers total64 bytes, handoff receipt48, pending entry16; these are raw object sizes, not a complete enqueue allocation bound.
* Both Sets have entries/kinds/physical directories0. Same immutable kind backing, refs3; original Set metadata refs1 and same metadata owner as visible clone. **Empty directory absent on BOTH Sets** (`Empty=false`, `SameEmpty=true` because both nil), so e=0 and release is6 records, not the previously hypothetical7. No independent directory was invented.
* Both original token namespaces stable/native/linked, tokens unreleased/owned/not imported, exact-RID lengths0. SAME Snapshot/captured native callback identities true, original operations refs1 each. Token duplicated-file name lengths150/158; the actual saved sync paths are139/147 bytes and those are used in the sync formulas.
* DATA pager logical13/target16, one4MiB chunk, verified chunks1/prefetch words1/dirty words1. PRIMARY logical11, one1MiB chunk, same capacities1. Both file-backed, not closing, Linux no mapped-flush requirement, populate false, grow policy target0. Existing backing capacity covers the required DATA count, but the original logical13→16 count installation and all materialization validation are still required. Do not remove GrowTo merely because no extra mapping is forecast.
* Both dirty count1/span1. DATA concurrency4, PRIMARY1; min workers1 each. Candidate metadata13..15 occupies the same already dirty DATA chunk, so dirty union forecasts are exact1/1, not upper estimates. No production verification concurrency theorem follows.

## Evaluated existing terms

These are source-exact CURRENT reservation forecasts on that geometry, not executed publication work and not a complete bound. Metadata/capsule retry counts assume the proposed no-concurrent-verifier diagnostic schedule; the current production path still has retry loops. Go-map byte charges remain their existing conservative work-traffic estimates, not exact transitive heap admission. Each term and sourceSymbol is also in `native-build/pre-effect-geometry/evaluated-terms.json`.

| Term | Records | Bytes | Source symbol / actual geometry |
|---|---:|---:|---|
| Activation dispatch | 1 | 916 | `DurableRootTransaction.activateFromCoordinatorStepWithWorkV6 / reserveDurableRootCallbackDispatchV6`; original typed callback dispatch |
| Activation member | 2 | 1008 | `rootPublicationRuntimeV1.activateNativeVisibleMemberV6`; one SAME member; headers ready; leaf generations nil |
| Activation install | 4 | 272 | `rootPublicationVisibleInstallV1.activateWithWorkV6`; actual preborn headers; no leaf-generation pin work |
| Late DATA claim including allocator activation | 13 | 3480 | `COWPreparationWorkV6.claimDataAdmissionLockedV6 / AdoptAndActivateV6`; O=0,C=1,I=0,n=1,B=0; activation4/2952 and MarkVisible1/48 INCLUDED |
| Begin turn existing reservation | 1 | 304 | `Coordinator.BeginNativePublicationTurnV6`; excludes actual clock and notify |
| Publisher lookup | 3 | 1752 | `rootPublicationRuntimeV1.publishWithWorkV6`; postactivation V=1; excludes path/closure extras |
| Metadata page writes existing reservations | 15 | 27648 | `Pager.WriteViewWithWork`; three 4096-byte images, 5/9216 each; zero retries requires the no-concurrent-verifier diagnostic schedule |
| DATA dirty sync | 15 | 8664 | `Pager.syncDirtyChunksWithFileWork`; span1,dirty1,workers1,path139; exact dirty union forecast |
| PRIMARY dirty sync | 15 | 8728 | `Pager.syncDirtyChunksWithFileWork`; span1,dirty1,workers1,path147; exact dirty union forecast |
| Runtime authority scope | 22 | 14976 | `nativeAuthorityAdmissionV6 / nativeAuthorityScopeV6.begin`; projected P=D=S=V=1 |
| Allocator authority scope total | 15 | 776 | `Allocator.BeginCOWPublicationScopeV6 / cowPublicationBaseAdmissionV6`; P=A=N=1,B=K=0; first2 plus second13 records |
| Original capsule write | 7 | 29696 | `Pager.writeOwnedSpanWithEffectWorkV6`; Linux file-backed three-page branch; zero retries under no-concurrent-verifier schedule |
| Shared Set consume | 6 | 1848 | `StableResourceSetReleaseWorkV6.consumeSharedWithVisibleV6 / Coordinator.ConsumeNativePublicationResourcesV6`; h=d=e=0,v=1; both empty directories NIL; same kind backing refs3 |
| Transaction consume | 4 | 1908 | `DurableRootTransaction.consumeFromCoordinatorStepWithWorkV6 / consumeNativeVisibleMemberV6`; dispatch1 plus original member3 |
| Release turn existing reservation | 1 | 304 | `Coordinator.ReleaseNativePublicationTurnV6`; excludes waiter/timer/clock/notify/signal |

Known partial composition = **124 records / 102,280 bytes**. This omits all missing rows below and grants nothing. The largest existing individual Reserve forecast among these evaluated rows is runtime scope22records/14976bytes; capsule write has largest bytes29696. The original nonyielding authority scope alone composes runtime22 + allocator15 = **37records/15752bytes**, already above Q32; no work charge was lowered to fit. These are not a complete maximum indivisible publication-turn bound.

The shared read-only calculator factoring preserves original charges: claimDataAdmissionLockedV6, cowPublicationBaseAdmissionV6 and nativeAuthorityAdmissionV6 are used by BOTH original operation and probe. Claim13/3480 already includes original allocator activation4/2952 and MarkVisible1/48; do not add those a second time. Formula inputs and exact sizeof operands are in the frozen source. Allocator scope15/776 consists original first admission2 plus original second13; its projected N1 is obtained through individually admitted owner/duplicate reads under the SAME allocator/ledger locks.

Probe's OWN work is separate: basis6/141456, allocator11/111600, coordinator3/69728, Sets4/69328, DATA7/75360, PRIMARY7/75360, final repeated basis6/141456; each uses an independent unchangedQ32/1MiB grant, returns only by-value scalars and keeps no observer continuation. Sum44/684288 is probe traffic over seven calls, not a publication grant or public stats. Variable traversal is admitted before each next/read; no count-then-Reserve loop was added. All original short locks release on every return. Final basis equals initial basis and cursor stays26.

## Explicit incomplete / opaque terms

| Term | Status | Source symbols / remaining geometry |
|---|---|---|
| Final basis / SAME Cut | unimplemented | `Request.Validate / BeginCut / ObservePrune / Commit / Abort`; exact proof/current parent/slot/base/sequence operands remain mandatory |
| Builder / enqueue / handoff receipt | unimplemented | `AcquireBuilder / EnqueueBuiltWithWorkV6 / enqueueLockedWithWorkV6`; pending len/cap0/0, builder0, pin maps0/0; headers64 bytes, receipt48, pendingEntry16; actual backing/map/lineage work not fully priced |
| Seal installation | unimplemented | `rootPublicationRuntimeV1 / original activeSeal and seals`; seal len/cap0/0; append needs its real admitted backing; no slot/visibility effect performed |
| Empty dependency flush/sync | unimplemented | `syncStableResourceDependenciesV1 / StableResourceSet.FlushThrough,SyncThrough`; entries/kinds/directories0; no token callback loop, header/access/empty path work remains |
| Both token wrappers / stable namespaces | unimplemented | `StableResourceToken.syncThrough / StableNamespaceToken.Stabilize / stablePagerOwnedOperations.SyncThrough`; original namespace stable/native/linked; imported false, exact RID lengths0; original saved operations refs1 each |
| Physical materialization / tail / GrowTo | unimplemented | `materializePrimarySealV5 / GrowToWithWork / ValidateCOWPhysicalTailV1`; DATA logical13 to16; mapped chunk/verified/prefetch/dirty capacity already covers; three page visits plus validation/count-install still real work |
| Notify backing | opaque | `Coordinator.notifyLocked / runtime.makechan`; original channel close/create/select operations must be counted; no runtime layout mirror introduced |
| Timer backing / timing branch | opaque | `Coordinator.installTimerLocked / realClock.NewTimer / time.NewTimer`; real clock, no timer at snapshot; future enqueue delay branch not frozen; all actual clocks/timer/stop operations remain |
| Release turn additional operations | unimplemented | `removeCompletedPendingPrefixV6 / completePublicationLockedV6 / satisfyWaiters / recomputePublishRequestLocked / notifyLocked`; initial waiter0 and receipt0; SAME receipt and pending member will exist later; no assumption of zero post-enqueue work |
| Bind / physical Finish / terminal completion | unimplemented-separate-phases | `BindNativePublicationV6 / original transaction Finish / CompleteNativePublicationV6`; existing Bind reservation1/416 plus notify; SAME physical owner/resource/frame lifetime and terminal receipt traversal remain outside F |

Thus the report is **BOUND INCOMPLETE**. Missing values are null in the machine report, never zero. Public Q32 remains RED, no larger allowance or product timing/concurrency policy was chosen, and no broad publication seam was implemented. Actual close/create/select and timer/new/stop/clock calls must be counted at their original source sites. Channel/timer backing remains opaque in this run; runtime scheduler/allocator global internals remain ordinary service under the accepted diagnostic boundary. No risky runtime struct mirror, guessed per-timer constant or DWARF investigation was added.

## Frozen changed paths / ownership

Only these seven native paths changed; root retains sole Git/GH ownership and later ordinary reconciliation. First two allocator calculators and authority calculator were factored narrowly; all other additions are scalar ForTest observation and the existing fixture call.

* `TreeDB/db/native_mvcc_seal_preparation_v6.go`
* `TreeDB/db/primary_authority_scope_v6.go`
* `TreeDB/freelist/allocator_preparation_work_v6.go`
* `TreeDB/freelist/allocator_publication_work_v6.go`
* `TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go`
* `TreeDB/mvcc/native_prune_backend_route_v6_test.go`
* `TreeDB/pager/write_work.go`

The next decision belongs to root: whether to complete the finite missing diagnostic operation plans and separately resolve opaque runtime-owned sizes under the accepted boundary, or stop at this negative/incomplete feasibility evidence. This packet does not authorize a split diagnostic or a larger publication allowance. Native source remains frozen for recovery/review.
