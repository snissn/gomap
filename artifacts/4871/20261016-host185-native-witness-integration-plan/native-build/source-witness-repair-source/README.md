# Original source-witness repair: frozen source and primitive controls

The native producer now retains its SAME already acquired current-root edge and canonical certificate through prepared phase7. Successful scratch cleanup no longer destroys the witness required by the final gate. Both final-basis and observer use full arena + generation-tagged Ref + canonical-certificate identity. The original authority scope and complete parent/record/slot predicates are unchanged. **Public Q32 RED/ACK0 remains unchanged.**

Selection SHA `c83ce87ffcf7ff1bdf1b5d9838c52a59838fc4cebfcf3d54b429410616c9045a`. The root's additional receipt-control direction is applied: actual original unfinished-receipt refusal is tested; a synthetic phase9 position tests source cleanup; positive completed-receipt-to-source ordering is SOURCE-REVIEWED ONLY. No successful receipt authority is fabricated.

## Exact immutable source

Directory: `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-build/source-witness-repair-source`.

- Full 10,349-file manifest SHA `4b230aa27b6a51d2394d2cd237403f956ac4897afb4b76e9e2e47bfb39947fd2`.
- Full verified archive SHA `44682995595a636fe99ab3febcebca1802f8992574eb4fab8dbef49f355ea03c`.
- Baseline is canonical-parent c654/10,348 files. Its consumed failed actual33 and diagnosis remain intact.
- `source.delta.patch`, `changed-files.json`, and complete `delta-files/` preserve every changed byte, including the new test. `receipt.json` binds their hashes to selection and baseline. No Git/GH mutation or child agent.

Exactly seven changed paths (one new):

1. `TreeDB/db/native_mvcc_root_preparation_v6.go`: separate success scratch cleanup and terminal source disposal; shared identity predicate and fixed pricing.
2. `TreeDB/db/native_mvcc_published_terminal_v6.go`: preserve source frame across receipt; drain it before allocator scratch in phase9.
3. `TreeDB/db/native_mvcc_final_basis_v6.go`: preadmit and use the shared complete source predicate.
4. `TreeDB/db/native_mvcc_seal_preparation_v6.go`: matching preadmitted SameCurrentRoot/FullBasisReady observation.
5. `TreeDB/db/native_prebuilder_pricing_for_test_v6.go`: actual role2 uses same source comparison price, F291 label.
6. `TreeDB/mvcc/native_prune_backend_route_v6_test.go`: F289→F291 expression label ONLY; fixture values, guards, flow and caps unchanged.
7. `TreeDB/db/native_mvcc_root_preparation_v6_test.go` (new): finite original arena/source/queue primitives, three focused controls.

## Original custody and caller disposition

`StepNativeMVCCBackendRootV6` phase0 still performs the original protected canonical borrow, identity check and Acquire exactly once. It stores the original arena/source/certificate; no extra Acquire, owner, certificate, pointer authority, keeper allocation or containing field is introduced.

Successful phases5/6 now call `closeScratchStep`: admitted alias scrub, temporary group Drop, then SAME release queue until genuinely empty. This clears directory/key/component/image/encoder/sealer/group scratch while preserving the source certificate and source Ref. Phase7 means prepared. Original source-bank observer role0 already reads `rootPreparation.source` and `.arena`; because these now remain live, its existing scalar observation/dedup route needs no alternate handle or source edit.

The shared `matchesCurrentRoot` requires prepared phase7, nonnil arena, nonzero full Ref, nonnil original canonical certificate, and nonnil current root whose arena/Ref/certificate are all identical. It does not borrow/create/copy a certificate or validate scalar CommitSeq alone. Existing exact sequence, saved/current full record/meta/parent/slot/promotion, transaction, prefix, storage/capacity, Set and index predicates remain intact. `primary_authority_scope_v6.go` is byte-identical to baseline.

Terminal root states reuse the existing phase byte:

| Position | Actual custody and effect |
| --- | --- |
| phase0 / phase10 | No acquired source / genuinely closed; idempotent ready. |
| partial phase1..6 | Finish original scratch/group cleanup first; refusal retains its exact group/queue. |
| phase7→8 | Admit scrub; clear borrowed source certificate/directory BEFORE a possible last source Drop. Source edge still live. |
| phase8→9 | Admit control and original Drop+protected lookup; on successful Drop clear only consumed Ref. Refusal retains the original edge; no replay after phase9. |
| phase9 | Drain SAME ReleaseQueue, including descendants and original queue RemovalWork; refusal/error retains arena/queue/refund frame. |
| phase9→10 | After genuine no-progress empty result, admit final clear, end arena/queue aliases, mark closed. Never clear while queue/refund remains live. |

Production terminal callers are exactly:

- Private `CloseNativeMVCCCurrentDeleteStepV6` at its existing `rootPreparation.closeStep(w)` site. Original seal/candidate/allocator error handling remains. No newly adopted output is privately aborted.
- Adopted `closePublishedNativeStepV6`: phase7 retains root preparation while ending member-borrowed aliases; phase8 calls actual `release.ContinueFinishWorkV6`; only ready true advances to phase9. Phase9 drives original root terminal to completion BEFORE allocator/output completion. A source-refund yield leaves outer phase9 and its original root cursor intact; a later allocator yield sees root phase10 and cannot repeat Drop/refund.

The source root is independent of member-owned resources, so it survives genuine physical receipt Finish. Existing snapshot/index custody stays alive until later producer terminal. Existing recovery-error guard retains the linked original closure before private/published disposal; this patch changes no sticky error authority. Newer writer edges are never substituted or consumed.

## Applicable operation and retained accounting

Let S/G/B/Q denote `sizeof(nativeMVCCRootPreparationV6/primaryarena.Group/bank/ReleaseQueue)`, r=`sizeof(Ref)`, p=`sizeof(phase)`; types are the actual existing containing types, not guessed backing constants. Every term below is an actual Reserve or original typed continuation.

Shared source comparison price is **2 records / 145 bytes** on pinned linux/amd64 Go1.26.3, tested from the actual calculator:

`2*sizeof(primaryStateRootV5) + 2*sizeof(Ref) + 2*sizeof(*Arena) + 2*sizeof(*ReadRootCertificateV6) + sizeof(phase)`.

The existing coarse gate reserve remains; no old traffic credit is subtracted. The new price is reserved before the actual comparison. Observer basis observation also reserves that same price before observing, separately from product F arithmetic. It is not a second F contribution.

| Applicable term | c654 predecessor | Witness successor |
| --- | --- | --- |
| Final invocation F records | `289+D+2K+23P+8M+3S+N+Z+V+A` | `291+D+2K+23P+8M+3S+N+Z+V+A` |
| F traffic bytes | prior exact expression | prior expression +145 |
| Included post-adoption R | `145+21P+7M+3S+N` | unchanged |
| Included R traffic | prior exact expression | unchanged |
| New allocated bank/certificate/header/control storage | none | none |
| Containing struct size/charge delta | — | zero; existing phase byte reused |

Variables retain the prior complete source packet's definitions; no actual33 values, total allowance, complete retained union or historical bound is inferred. `NativePublicationBound`/authority calculators are unchanged because source comparison is before adoption; R145 is not added again to F291.

Successful prebuilder cleanup now costs its existing `1/2S` alias scrub, optional original group Drop `2/(2G+2Q)`, and every actual scratch queue step. It no longer consumes source Drop/descendants; that real work moves to terminal. Scratch queue emptiness remains a real 10-record call.

Additional root terminal, **after** existing receipt Finish in the published route, is:

`1/(2S)` source alias scrub + `1/(2r+2p)` source-release control + original source Drop + SUM(actual terminal queue steps) + `1/(2S)` final frame clear.

Original source Drop reserves `2/(2B+2Q)`, then the existing protected pager lookup header and each visited link. Each queue invocation, including empty/refused/refund invocations, reserves `10/(18B+2G+2Q)` before actual work, then any applicable protected bank lookup/removal and typed accounting work. Original `CompleteRemovalWork` includes its real Bind, Remove start, owner/enrollment/binding traversal, concrete ResizeWork, link retirement and Reset reservations; it is not a fixed anonymous refund. Mutable enrollment/header traversal is revalidated on each actual continuation. Refusal retains that SAME receipt/cursor and does not silently use nil-work. Owner byte withdrawal can precede completion of external lease shrink only after genuine child alias/physical detach, with exact accounting debt held by the original queue receipt until completion.

A full source Finish expression must therefore include this terminal sum, rather than reuse the predecessor's successful-prebuilder source cleanup as an already completed Finish. Each split has a separate real empty-queue check. Retries can repeat already admitted header/attempt work; these are charged at invocation, never erased from a total. A universal numeric Finish bound is NOT supplied: last-owner descendants, registry collisions and live enrollment/concurrency geometry remain actual dynamic original terms and can refuse under Q32. The two-phase queue sum above names all moved work and its extra control/alias traffic; it is not a progress qualification.

Disjoint retention/custody:

- The existing original source edge is retained longer. It keeps the canonical source bank/image/certificate/group/descendant closure alive through source consumption; this is REAL retention, not a zero-cost lifetime exemption.
- Storage remains charged by original bank metadata/pager-node/arena mechanisms. Dedup storage once by SAME arena+full generation-tagged Ref and its canonical bank/image/certificate allocation identity; independently acquired snapshot/current/slot/source edges are separate custody, not separate storage allocations. A last-owner source can retain an entire closure until cancel/Finish; no refund at fence is claimed.
- The native source frame, SAME queue and embedded RemovalWork were already included in original admitted producer sizeof. No additional backing allocation/owner/enrollment is created by this patch.
- Before last source Drop, certificate/directory aliases are detached. Before original bank charge transfer to queue refund, original arena code detaches pager membership/image/certificate. Before final source frame reset, physical queue and exact pending accounting completion are genuinely empty. Independent held roots keep their own edges and charges.
- Private captured clone/sink/planner map provenance, shard Z observation, dynamic Finish geometry and final disjoint-retention completeness remain OPEN. None are relabeled inherited canonical output or resolved by this lifetime repair.

## Frozen verification

Pinned Go1.26.3 binary SHA `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`; compiler SHA `3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`. Same offline verified modcache, task cache/tmp, GOMAXPROCS2, GOMEMLIMIT8GiB, count1, Go timeout3m/outer300s. Exact argv/env retained in `proposed-commands.json` and executed `verification/*/command.json`.

| Command | Result | Seconds | Test RUN entries | PID/PGID |
| --- | --- | --- | --- | --- |
| compile-only db/mvcc/primaryarena | PASS | 54.572 | 0 | 902642/902642 |
| primitive-normal db | PASS | 1.826 | 7 | 903473/903473 |
| primitive-race db | PASS | 57.644 | 7 | 903595/903595 |

Normal/race each ran exactly three top-level tests, four subtests, no failures/skips:

- `TestNativeRootPreparationSourceWitnessV6`: successful scratch preparation preserves SAME source and charge; all nil/arena/Ref/page/incarnation/certificate mismatches; actual final-gate under-admission; independent original held edge remains after native disposal; actual fixed145byte price.
- `TestNativeRootPreparationSourceTerminalV6`: before-acquire, last-owner, independent-held-owner, partial-scratch-cancel-and-refund. Record/byte refusals before alias scrub, before original Drop, after Drop and at original pending refund; ordinary queue cannot steal native release; one Drop; no early physical/charge release; actual enrolled last-owner refund and idempotent terminal; stale bank cannot revive after release.
- `TestNativePublishedRootPreparationDrainV6`: constructor directly uses original root frame in actual producer, SAME initialized queue (never copied/replaced). Real zero/unfinished original receipt refuses at actual phase8 with same root/frame/counters/charge. Explicit synthetic phase9 tests resumable root cleanup. Successful real receipt→source release is SOURCE-REVIEWED ONLY, not runtime proven.

All source before/after manifests equal the full freeze. Archive members verified against every manifest row before runtime. Runner verifies Linux child-subreaper, records actual command/PID/PGID and observed group processes, joins leader/adopted children and checks empty groups/children; all three receipts show empty. No timeout, kill, failure, retry or source repair after freeze. All processes are joined.

## Handoff and limits

Source/runtime lease RELEASED with this packet. Source remains frozen. No actual33, DB workload, stale-parent DB control, final driver, native enqueue/adoption/activation/fence/Consume/Finish, ACK or benchmark was executed. The prior generic actual33 failure remains failure and consumed; these primitive controls do not prove route recovery. A full completed-receipt/newer-real-writer Finish control remains separately required.

Root will first perform custody-preserving integration with ordinary0149 in its NEW isolated worktree, review/reprice the actual integrated Snapshot/token/namespace/layout and22shared semantic divergences, then decide any separate observation grant. No old-base rerun is proposed or authorized by this handoff. Public Q32/1MiB/33/80/4096 and all resource guards are unchanged; no numeric split allowance or final qualification claim.
