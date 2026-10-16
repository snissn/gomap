# N1/N2/N3 connected successor — source repair, awaiting verification

This successor implements the three accepted findings from `atomic-completion-independent-review.md/json` under `root-atomic-completion-repair-selection.md` (SHA256 `cd43a01ad975e8fa64d9c810e733cb650eb003cdececb20de97b26ed021e12de`). The rejected 10338-file image and its tests remain historical. Its decision packet is not relabelled accepted. Source identity, complete reconstructable archive, exact repair delta, prior-packet union and proposed commands are in `native-build/atomic-completion-repair-source/receipt.json`. Root owns Git. No source in ordinary or rejected packets was edited.

**No builds, tests, DB/publication runs or benchmarks were executed for this successor.** Pinned gofmt parsed/formatted the edited source; that is not type checking. There is no new runtime evidence, numeric allowance, observed F, retained total, publication or ACK. Original Q32/1MiB/input32/resource16/file17/two-index/SAMERequest33/80/4096 remain unchanged RED/ACK0. The broad scalar observer is not implemented. Source is frozen for root's verification/review disposition.

## N1: original published outputs, no private headers

`rootPublicationVisibleInstallV1` no longer contains activationState/activationSnapshot. `prepareActivationHeadersV6` and candidate phase5's private allocation step are removed. Private candidate preparation, refusals, yields, stale cancellation and abort therefore cannot retain either newly created publication header. Abort no longer pretends that clearing such fields ends external readers.

`activateWithWorkV6` first calls `admitActivationHeadersV6`: two actual allocation units and `S(DBState)+S(snapshotView)` traffic on SAME work. The existing four install/init units remain. All sequence/conditional/Set/leaf/snapshot-pin/Cut/allocator checks stay before adoption. Only after successful SAME allocator adoption does the original noyield install create `&DBState{...}`, store it, create `&snapshotView{}`, populate and immediately store the original snapshot view. No new admission, refusal, returned continuation or error branch occurs between header creation and their original stores. Conditional point-map work remains ordinary-only as before.

Snapshot Z receives `install.leafGenerationView` itself. `snapshotPublicationScopeV6` retains only that same existing leaf operand on stack, not a temporary DBState. `publishSnapshotView` passes its original state's LeafGenerations. Old-view stale/pin and system-root/negative coverage behavior remains in the original helpers. The existing phase26 test scalar is renamed PublicationOperandsReady and checks the real root/certificate/index operands; it cannot assert removed preborn headers. Its existing combined install-byte scalar now includes both allocation and init traffic.

This uses root's explicit decision only for the original published DBState/snapshotView GC outputs. State()/StateToken() may retain old published headers outside root-bank last-ref lifetime. No per-header refund, deterministic GC, transitive heap or whole-output residency bound is claimed. Native-private callbacks, receipt, array and containing storage remain admitted. The two removed pointers reduce install raw size by16 on this 64bit target; actual charge is always Q(S(current install)), not a hand-adjusted number. Actual target layout/rounding still requires the proposed pinned compiler control.

## N2: native join is a teardown barrier

`Coordinator.HasOutstandingNativePublicationsV6` takes original c.mu and returns only `nativePublicationsV6!=nil || nativeDispatchV6!=0`. It traverses no graph and creates no owner/registry. `DB.closeAfterHooks`:

1. Sets closing and closes admission, finishes the original writer barrier.
2. Reads runtime under teardownMu.RLock and releases it before original Join. Failed Join with protected native debt returns immediately, before recovery handoff, runtime detachment, internal/capture teardown, index/pager/Set closure or original resource release.
3. Takes teardownMu exclusively and rechecks. If a prior admitted Bind registered in the interval, releases gate and joins outside it, returning on unresolved debt or recrossing after settlement.
4. Only with no native debt performs existing Drain/Stop. Rechecks protected disposition after Stop, before TakeRecoveryHandoff or runtime=nil. Ordinary errors with no native debt retain existing error collection/teardown behavior.

`closeTeardownOnce` preserves an incomplete error and SAME runtime graph on repeated Close. This is intentionally incomplete physical closure, never successful teardown/recovery. Genuine sticky recovery-required Join already returns the embedded first cause without calling native owner.Close, so the DB barrier keeps its original pending candidate/transaction/frame/dispatch/lease in place. No private Abort/Cancel is introduced after adoption.

`BindNativeMVCCPublicationV6` now crosses teardownMu.RLock and rechecks closing and runtime before coordinator registration. The existing bound early return adds no edge. The gate's closing recheck seals late registration; no native owner join occurs under teardownMu. Bind's actual entry reservation changes1->2 and adds original gate/closing traffic. No containing storage is added. Lock order remains original caller/cursor -> teardown read gate -> coordinator. Close releases writeMu before Join, Join releases c.mu before owner callbacks, and exclusive gate never covers a native owner.Close callback: outstanding debt forces unlock/join/return first. The post-Stop recheck is under the same gate. Nil runtime is checked under that gate.

## N3: same original staged callback/custody constructor

Phase4 no longer reserves a fixed6 and calls ordinary `newStablePagerOwnedOperations`. It calls `constructOwnedOperationsV6` on the existing admitted native capture:

| Stage | Accepted original storage/edge and next owner |
|---|---|
| 0 | Admit actual callback/path traffic and exact independently rounded Q(callback)+Q(path). Original metadata AddPendingWithOrdinalWork admits current bindings/active enrollment and both forward/rollback resize capacity before effects. Allocate SAME callback/path using shared `makeStablePagerOwnedOperations`; immediately assign native capture, refs1 and stage1. |
| 1 | `NewOriginalCleanupCustodyWithWorkV6` uses SAME custody constructor. Before inspecting/advancing it admits header/allocation traffic, locks original registry, checks closing/edge overflow, then typed AddPending under registry->Owner order. Only successful growth creates/backlinks original backing and increments edge count. Assign to original operations immediately, then stage2. |
| 2 | Original primaryArenaOwner.retain receives SAME work. Successful nonfallible increment is immediately installed in operations.owner, then stage3. A refusal/error leaves callback and accepted custody in the same capture. |
| 3 | Original parent sets spec.OwnedOperations and advances capture phase5. Token construction never sees partial operations. |

No lock survives a yield. Repeated protected basis validation still occurs in capture.step before accepting more effects. A later stale/capability/error result retains partial original edges for cancellation. There is no ordinary RemovePending, custody.Complete(nil work), owner.release(nil work), helper owner or error-time keeper on this native construction path. The ordinary constructor still performs the identical AddPending/custody/retain/unwind sequence and uses the same output initializer.

Partial states are already consumable by original `closeStep -> ReleaseWithWorkV6`: original Snapshot closes with its counter exactly once; unaccepted physical-owner/custody pointers are nil; accepted edges use original primaryArenaReleaseWork and typed custody Complete. Custody completion returns accounting-only value after detaching its owner/charge. The parent clears actual custody/operations/Snapshot aliases before original RemovalWork completes their charge. Refusal keeps the same receipt/delta and charges, including after partial shrink. Spec is cleared before releasing callbacks; constructor Detach and real final-owner token release remain unchanged. Existing current-generation/nonterminal eligibility is revalidated at actual release under idxMu/wrapperMu; late retirement/closing-registry limitations remain explicit, with no early consume/refund.

One `operationsPhase uint8` is added inside each existing capture, inside CurrentDelete and PruneCursor. All actual containing admissions already derive from unsafe.Sizeof of those concrete containing types. Do not add standalone rounded capture charge to the outer cursor. No new independent storage is added except the SAME callback/path and original custody which already existed and now receive actual typed admission. Their physical retained cost remains `Q(S(operations))+Q(path length)+Q(S(OriginalCleanupCustody))` once each; the Snapshot wrapper is separately charged once. Registry originalCleanupEdges is a scalar guard, not an independent allocation or second owner.

### Exact N3 per-invocation pricing

Let G be the actual tuple admitted by `Owner.addWithWork`, under SAME Owner.mu, for each growth independently:

```
G.records = 1 + activePending*(2+2*rho_active)
              + sum_bindings(3+2*rho_binding)
G.bytes = 2*S(Owner)+2*S(EnrollmentWork)
        + activePending*4*S(EnrollmentWork)
        + sum_bindings 6*S(binding)
        + 2*sum_actual_resize_admissions(concrete Resize bytes)
```

For supported pinned COWExternalLease, each rho=1; each resize byte term is `2*S(COWExternalLease)+2*S(COWBudget)`. Forward AND possible rollback are reserved before first resize; actual rollback is the unchanged original resize transaction. Unsupported capability is hard ErrWorkRequired; insufficient vector is ready=false,nil; actual capacity failure is original error with original rollback. No allocation/custody effect occurs for any such failed growth.

Successful fresh constructor without an intervening return costs entry1 + output2 + G_output + custody2 + G_custody + physicalRetain1 (when nonnil owner). Thus with zero bindings/no active enrollment and nonnil owner it is8 records, not old6. Actual states may have different G on each call. A resumed stage pays its new entry and every actually attempted header/admission again; completed stages are not replayed. Entry bytes are `2*S(capture)+S(Snapshot)+S(indexGen)+S(os.File)+S(operations)`; output traffic is `2*S(operations)+2*pathBytes`; custody traffic `2*S(IdentityPinRegistry)+2*S(OriginalCleanupCustody)`; retain traffic `2*S(primaryArenaOwner)`, plus actual G terms. Unsupported large atomic growth may refuse forever under Q32: safety, not progress qualification. Both DATA/PRIMARY captures traverse this same path.

Partial cleanup's exact conditional price is the reached Reserve sum in existing closeStep/ReleaseWithWorkV6, Snapshot close, primaryArenaReleaseWork, custody Complete and RemovalWork. Zero-work leaves accepted edges and accounting intact. No new synchronous failure defer exists. Original Owner atomic rollback pure controls are included in the proposed regression batch; this packet does not substitute them for actual partial-capture cancellation.

## Whole final invocation F and physical Finish

Definitions remain P prefix, D debt before activation, K selected keys (and actual key lengths for bytes), M recorded metadata pages, S seals after install, N immutable claim-ID upper bound, Z original snapshot pin/prune work, V installed AppendWork validation, A actual mutable claim+activation admission. Capture construction and Bind occur before F; their new work above must not be hidden in F or omitted from preparation. Before source selection, no numeric actual33 instantiation exists.

| Applicable original F component | Records |
|---|---:|
| Driver entry/local Cut/plan/result | 6 |
| Linked recovery observation | 2 |
| Exact final basis/Request/two capture+pager capabilities/seal install | 15+D |
| SAME Cut/key observation | 2+2K |
| Original bound/capacity observation | 15+2P+M |
| Original completion plan incl timer/clock/notify/error | 28 |
| Original builder-release contingency | 8 |
| SAME work floor/restore | 4 |
| Original builder acquire | 2 |
| Atomic original enqueue+attempt/emptySet/timer/notifications | 46 |
| Original activation dispatch | 1 |
| Member/post validation + original visible install/init + TWO published allocations | **11** |
| Snapshot pin/prune | Z |
| Installed-page validation | V |
| Original claim+visible activation | A |
| Immutable postactivation storage/fence/Consume remainder | R |

```
F.records = 140 + D + 2K + 2P + M + Z + V + A + R.records
R.records = 141 + 21P + 7M + 3S + N
F.records = 281 + D + 2K + 23P + 8M + 3S + N + Z + V + A
```

F.bytes is the ordered sum of the concrete Reserve byte operands in final driver, linked observer, final-basis/Cut, remainder observation, original completion/builder plans, floor, acquire/enqueue/activation, snapshot scope, installed validation and claim, then remainder. **Add `S(DBState)+S(snapshotView)` for N1 final allocation traffic**, use current sizeof for every changed containing operand, and retain original install init traffic. This is a complete source expression, not an evaluated number or allowance. Previous F279 is superseded by F281. Synthetic postactivation-only R235 remains R only and is not recomputed/misrepresented as actual F. No R operation or arithmetic constant changes in this repair.

The source-evaluable expansion of V/A/Z/R and each byte term remains in `atomic-completion-frozen-packet.md`'s operation tables, interpreted against this successor archive. The accepted N1 move is the only final F row change. The original scope includes fixed ambiguous-disposition allowance before irreversible authority, canonical zero unused native no-prune capability, exact current parent/slot/prefix checks and allocator locks. Original SAME-work temporary limits restore on every success/refusal/error and overflow paths. Mutable actual claim/activation must fit remaining composite work before first claim effect. No new max grant, scope exemption, retry policy or changed caps is introduced.

Physical Finish remains the original conditional sum, separate from F:

1. Published phase7 drains detached candidate/seal backing RemovalWork, reserves header, binds real Finish, then clears original borrowed candidate/root/seal aliases.
2. Original transaction ContinueFinish dispatch serializes SAME Retire callback; native retirement transfers the original post.oldState root/Set edges once into actual member/queue. Retired visible Set/runtime/seals and real vectors dispose through original owners; each alias clears before accounting.
3. Original arena release queue persists in the surviving caller through callback return; every real node/dependency/lastref and pager table removal is admitted by actual geometry.
4. Genuine Coordinator-owner Set release traverses actual token/directory/provider/identity/namespace/index callback/lastowner branches. Empty fence pins do not imply nonlast physical Set. Both index callbacks retain their containing capture frames until final consumers end; N3 partial outputs use this same branch.
5. Original terminal constructor returns only by-value accounting after physical completion; disposing parent detaches real aliases before original RemovalWork and external terminal lease close.
6. Published phase9 closes published allocator scratch (no Cancel); phase10 completes original AppendWork/output and typed deltas; phase11 closes both captures/input/projection and actual Snapshot/counter/namespace/proof/owner/registry edges. Refund refusal retains original delta. Late generation/registry limitations remain explicit.
7. Linked receipt detaches only after actual physical completion: DB wrapper1 + original CompleteNativePublication (supported success14+L, actual original link visits L), clearing builder/inline/callback aliases before counters. Outer cursor final accounting/clear precedes once-only ACK.
8. Genuine hard-I/O failure retains whole original graph/lease/pending/receipt/dispatch and first cause. N2 now makes DB Close match original Stop: unresolved custody returns sticky incomplete error before recovery or teardown. No successful error Close or physical recovery is claimed.

Finish byte/record terms are exactly the current source Reserve sum for reached states, including repeated entry on retries and actual later-writer geometry. Native construction records do not get charged again as physical allocations during release; only actual release/alias/refund traffic is charged. Future lastowner/retirement geometry is unknown until genuine consumption and may cause an oversized indivisible unit to refuse indefinitely. No universal Q32/C3 progress, cancellation eligibility for every history, or successful error teardown is implied.

## Disjoint retained ledger and remaining observation gap

Use Q(actual independent allocation) and original allocation identities, not summed standalone sizeof samples. All prior packet domains remain, with the following corrected explicit canonical union:

- **Outer PruneCursor:** one actual Q(S(current cursor)), including embedded CurrentDelete, two captures, NativePublication/inline pending/builder/attempt, all queues/refunds/control scratch. Added stage byte is embedded; never separately charge Q(capture). Size/rounding samples from rejected source are stale until pinned layout test. Same original cursor lease lasts until callback consumers, receipt detachment and final scrub.
- **Original member/install/environments:** current containing expression in candidate phase1 counts member, install and actual callback/nested environments once. Removed header pointers are not independent allocations. finalizeCommitPost, queues/terminal receipts are embedded. Any Q class change follows actual sizeof, not subtraction16 from rounded quota.
- **Original seal/primary seal/prefix:** original P-capacity admission once, no duplicate outer capture charge; same consumer retirement lifetime.
- **Independent runtime descriptors/current full backing:** actual current sizes/capacities; debtBackingCapacity remains authoritative across subslicing/ordinary growth. New+old overlap admitted before replacement; original detached old charge is counted separately only after transfer out of runtime metadataCharge, and remains in Owner.Bytes until real RemovalWork. No per-runtime native continuation added.
- **Selected reader and two original index-capture Snapshots:** each actual independently allocated/pool-supplied wrapper's original charge once. Capture state pointers borrow original published outputs; do not invent/credit DBState copies which the constructor does not allocate. If any other source constructor actually creates a state copy it needs its own actual allocation entry. Every nonnil Set remains pinned by the existing bounded capture contract.
- **Two original callback/path/custody outputs:** each actual callback Q(S(operations)), cloned path Q(length), original custody Q(S(backing)) independently once, plus exact accepted physical-owner edge. No second allocation for its native backpointer. Stage0/1/2 partial states retain actual accepted charges until original typed cleanup. This corrects the old fixed6 traffic claim; it does not add an ownership domain.
- **Remaining PRIMARY/native graph:** real constructor/arena/directory/token/namespace/identity/pager table metadata, bindings/enrollments, private arrays/images/key/retirement/sort scratch at actual independent capacities/owners; actual rounded bank+node separate. Unsupported protected growth refuses before effects. Count shared original allocation once by identity even when two owners' quota policies or independent enrolled leases charge overlapping governor totals.
- **Original canonical DATA output:** same #5111 inherited DATA domain; no new transitive enrollment or accumulating per-generation framework. Native copies/scratch/control added around it remain included.
- **Original published DBState/snapshotView outputs:** explicit N1 root decision, original GC lifetime ONLY after adoption. Their real allocation/init traffic is in F. They are neither private retained objects nor refundable member storage. No residency/transitive heap bound or credit from bank last-ref. No extension of this boundary to other native storage.
- **Original coordinator runtime services:** actual changed-channel/timer/clock work/traffic remains in F under the accepted original GC-service boundary; no registry, per-channel refund or transitive runtime heap claim. Notification aliases may outlive replacement. Pinned Go ELF layout evidence remains diagnostic-only.

Owner.Bytes is a policy sum, not a second physical allocation union. Enrollment control/header and external concrete lease header are actual independent allocations where applicable; overlapping governor byte promises must be reported separately from physical union. No actual complete retained total has been observed. Current source sizeof/capacity/charge and live enrollment identities must instantiate all rows; refuse an incomplete observation rather than report a subtotal as total. Published-output GC residency is explicitly outside that selected retained union, with allocation traffic disclosed.

## Observer disposition and proposed verification

`atomic-completion-observer-plan.md` remains a read-only plan, not implementation/runtime authorization. Successor correction: expose **no native preborn header row** because none exists; report published-output boundary and final two allocation terms explicitly. Observe actual current containing sizes (including new stage byte/removed pointers), each callback/path/custody accepted stage/charge and same registry/metadata/physical-owner equality, with no alias escape. Keep five groups, per-visit Q32/1MiB admission, exact protected basis, no count-before-reserve, original actual33 fixture and phase26/candidate8/seal13 stop before builder/adoption/enqueue/grow/write/fence/Consume/Finish. Its new observed fields still require root authorization. Actual33 coverage nonzero/unsupported geometry must be reported, never retried away. Future Finish remains conditional.

The executable argv/environment proposals are in `proposed-commands.json` beside source receipt. They are **not executed**:

1. Pinned `go test -p=1` compile-only db/rootpublication/mvcc/retainedalloc, run regex `^$`, timeout3m/package, outer230s.
2. Normal focused N1 pre-adoption traffic/same-leaf operand; N2 incomplete/repeated Close, prior close-hook Bind crossing gate, denied late Bind, actual completed join and ordinary no-debt hook error; N3 four stage boundaries, same-output resumption, original owner-retain overflow error/partial cancellation, zero-work refusal and counter/ref/charge restoration; typed custody unsupported/closing/overflow/ordinary/refund, plus original atomic growth rollback and unchanged partial-token cancellation. All proposed previsibility only, no native activation/publication.
3. Same focused controls under race, outer300s; no broad package campaign/performance run.
4. Separately proposed existing exact actual33 real-route/copied-parent normal+race safety rerun, unchanged guards. Its phase26 call enters final driver's pre-effect Q32 refusal; it is not part of an implicit runtime grant. Root must explicitly select that scope. It cannot qualify publication/progress/ACK.

Before any authorized run: save full current source archive INCLUDING new files, all-source before/after, exact compiler/argv/env/PID/PGID, bounded outer timeout, kill entire group on timeout and join leader; inspect actual remaining group membership, not leader-only claim. Preserve first failures. Any subsequent edit invalidates binding and needs a successor freeze. No repeated or broadened run without a concrete unresolved defect/root disposition. Source/runtime lease released for root's review decision after this packet.
