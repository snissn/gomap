# Connected original native publication packet, frozen 2026-10-15

The source is frozen at `native-build/atomic-completion-freeze/receipt.json`: **10,338 tracked/new files, 42 changed paths**, byte-identical to archived `connected-pricing-normal-01`. Manifest SHA256 `ccaa15b2381b87336937688cdc1e672c875564261da53fd6ba5c1f5e814107e6`. Full reconstructable source archive SHA256 `761722ad4d5a3b41a19934d00b45f810c229727eb0d9c682170da9a2f5eab929`. The archive includes untracked source; the tracked binary patch is supplemental. Root remains sole Git owner. No source mutation follows this freeze.

This packet connects the original selected+anchor producer to original enqueue/attempt, visibility, physical storage, fence, Consume, turn release, physical Finish and terminal ACK **as source**. No native activation, enqueue, fence, final-driver or Finish runtime was executed. No numeric diagnostic allowance is proposed. Original Q32/1MiB/input32/resource16/file17/two-index/SAMERequest33/80/4096 guards remain unchanged **RED / ACK0**. Earlier previsibility evidence remains bound to its own source image, not upgraded to this freeze.

## Connected source and state transitions

`mvcc/prune_cursor_v6.go` phase26 retains the SAME candidate8/seal13 and calls `NativeMVCCCurrentDeleteV6.FenceNativeMVCCPreparedV6`; phase27 closes the published producer through its real receipt. Only after phase27 succeeds does original phase22 perform its once-only ACK. The current phase26 Q32 refusal is retained. `native_mvcc_final_publication_v6.go` is the sole new final invocation, using original `OrdinalScanWork`, `PreparedRootCandidate`, transaction, builder handle/lease, allocator preparation, publisher, two captured index operations and physical receipt.

1. The driver admits local traffic and observes the original linked recovery error. Under `writeMu -> durablePublishMu -> rootReuseMu`, final preparation checks exact expected sequence, selected Request, current root Ref/certificate, original durable slot/meta/full record and generation-tagged parent Ref, promoted parent/current record, exact prepared prefix, preborn seal capacity/storage, and both actual captured pager callbacks/capabilities. It observes the exact obsolete deletion keys through a stack SAME `mvccadmission.Cut`. No private cursor retains a Cut or a lock across return.
2. Before acquiring a builder, it computes the immutable storage/Consume remainder from original operands, admits original completion and builder-release plans, then temporarily withholds that remainder from the SAME work limits. Every actual operation still reserves on that SAME work. All success/refusal/error exits restore both original limits; the immediately deferred restore also covers early acquisition/enqueue failure. Overflow refuses before an effect. This is not a replacement grant or scalar-precharged callback.
3. `EnqueueNativeBuiltWithWorkV6` takes original handle -> lease -> coordinator locks and performs original enqueue/activation plus original attempt start before unlocking. The sole native entry borrows inline pending storage embedded in the SAME admitted `NativePublicationV6`. It proves the original pending empty before attaching. No new queue, keeper, independent handle pool, owner goroutine or admission gate exists. Original below-hard ordinary Enqueue remains available during unlocked native I/O.
4. The last fallible activation operation is original `COWPreparationWorkV6.AdoptAndActivateV6`, under allocator -> ledger. It validates every original installed `AppendWork`, reserves actual mutable claim and activation geometry before the first claim effect, extends SAME candidate claim, transitions original ledger visible, transfers actual output rollback edges and installs the SAME candidate. It then performs nonfallible visible install/Cut commit. No Abort or private Cancel is legal after this point.
5. The SAME preadmitted seal is installed; fixed supported ordered post work completes before releasing build exclusion. All build locks release before dependency, metadata materialization, namespaces and index sync. Physical DATA growth is only the existing capacity-covered count install: original target13 ->16 uses original three recorded page writes; there is no pre-growth across a yield, changed physical-tail guard, file remap or alternate ledger.
6. The original publisher executes materialization -> PRIMARY index sync and namespace -> DATA index sync -> typed authority capsule. The short native authority scope alone takes `durablePublishMu -> rootReuseMu -> runtime.mu -> allocator -> ledger` over authority write and original commit. All scope locks unwind before disposition, Consume, turn release or physical Finish. No owner.Close under the scope.
7. Original Set logical handoff and member transaction Consume occur after the successful fence. Preadmitted completion clears only original pending[0], subtracts its exact saved bytes, then either nils singleton pending or advances to the existing tail without copy. Original receipt remains linked through physical Finish; its inline candidate alias is not an early frame-refund authorization. The shared scalar durable frontier advances once, timer stops, logical turn releases and original changed/wake are signalled. A newer writer never runs native callbacks.
8. `native_mvcc_published_terminal_v6.go` phases7..11 drain original backing accounting, bind genuine Finish, clear borrowed candidate/root/seal aliases, drive original member/resource Finish, close published allocator scratch, complete materialized output, close both original captures/input/projection, clear outer aliases and detach original linked receipt. It does not call constructor Abort or allocator Cancel after adoption. The original physical completion token reaches the terminal accounting barrier rather than being discarded. Outer PruneCursor remains charged until its consumers are detached.

## Whole final-invocation formula F

This is a source-evaluable operation/byte expression, not an observed actual33 total. It starts at entry to `FenceNativeMVCCPreparedV6` with candidate8/seal13 already privately prepared; earlier retained preparation calls keep their own actual reservations. Finish is separately bounded below. A byte term denotes actual source work/backing traffic, not a transitive Go heap claim. Let `Phi(f)` be the ordered sum of the actual `Reserve(records, bytes)` expressions reached in function f on the stated branch. Each expression uses the pinned compiler's `unsafe.Sizeof`; source archive is its exact specification. Record coefficients below expand those expressions; byte formula is the same component-wise sum, never rounded scalar constants substituted for source expressions.

Geometry, captured/revalidated at original locks:

- P: seal prefix length; D: debt length before activation (required D=P-1).
- K: exact selected deletion-key count; each key's byte length is retained as an input to Cut byte cost.
- M: total original recorded metadata pages across prefix; every image must be 4096 bytes.
- S: seal count after the preadmitted SAME seal install.
- N: immutable upper bound on final prefix claimed IDs, sum original claim IDs plus private DATA allocation count for its final member. Actual claim appends only unowned IDs, so actual publication IDs <=N. Empty `abandonedCoverage` is required by this first diagnostic forecast, not a public cap or history-progress claim.
- L/capL: original activated vector length/capacity at actual activation; n: final private allocated DATA count; C/O/I/B: original candidate/owner/current-claim-ID/burned-tail counts at actual claim.
- J: linked materialized installed frames. Per frame j, A_live and A_private are whole canonical allocated-vector lengths; T_j is zero for oldID0, otherwise retirement-record count plus total retirement PageIDs visited. Z is actual snapshot publication pin work below.

| Operation (actual source) | Records in F | Byte expression source |
|---|---:|---|
| Driver local frame/Cut/plan/result traffic | 6 | `FenceNativeMVCCPreparedV6` first Reserve |
| DB+Coordinator recovery observation | 2 | `NativeMVCCPublicationRecoveryErrorV6` and original receipt observer |
| Final basis, Request, both capture headers+pager capabilities, final seal install | 15+D | `prepareNativeSealInstallationV6`: 4+Request1+2*(capture3+pager1)+D+2 |
| SAME Cut and selected key observations | 2+2K | `observeNativePublicationCutV6` plus `Cut.ObservePrune`; includes each key byte length |
| Effect-free original bound and capacity geometry | 15+2P+M | remainder helper6 + allocator bound3+2P+M + capacity predicate6 |
| Preadmitted original completion, incl timer/notify/clock/error traffic | 28 | `AdmitNativePublicationCompletionV6` |
| Original builder refusal-release contingency | 8 | `AdmitNativeBuilderReleaseV6` including notify+wake; already reserved even on success |
| SAME-work floor/restore control | 4 | `nativePublicationRemainderV6.withhold` |
| Original builder acquire | 2 | `AcquireNativeBuilderWithWorkV6` |
| Atomic original enqueue+attempt start, empty Set, timer, both actual notifications | 46 | `EnqueueNativeBuiltWithWorkV6` plus receipt branch of `enqueueLockedWithNativeReceiptV6`; wrapper12+inner34 |
| Original activation dispatch | 1 | transaction activation dispatch |
| DB member/post validation and nonfallible install | 9 | post-validation3 + member callback2 + visible install4 |
| Snapshot-pin publication work | Z | `beginSnapshotPublicationV6` and `beginStaleTransitionV6` |
| Original installed-page validation/adoption | V | `ValidateCOWInstalledPagesV6`, `ValidateInstalledPageV6` |
| Original claim+visible activation | A | `claimDataAdmissionLockedV6(activationAdmission+1, activationBytes+48)` |
| Immutable postactivation storage/fence/Consume remainder | R | exact pure `nativePublicationRemainderForGeometryV6` |

Therefore, component-wise for records/bytes:

```
F = all table components exactly once
F.records = 138 + D + 2K + 2P + M + Z + V + A + R.records
R.records = 141 + 21P + 7M + 3S + N
F.records = 279 + D + 2K + 23P + 8M + 3S + N + Z + V + A
```

No numeric value for F.bytes or F.records on the real fixture is claimed. The formula includes preactivation and failure contingencies; **235 records /122,784B is only a synthetic R**, using P1/M3/S1/N-upper49 with synthetic byte operands, not actual33 geometry or F.

Installed-page expansion is exact:

```
V.records = 1 + sum_j [3 + A_live + A_private + T_j]
V.bytes = 2*S(MaterializedWriteWork)
 + sum_j [4*S(AppendWork)+2*S(frame.result)+32
           +2*S(AppendWork)+64
           +(A_live+A_private)*2*S(allocatedPage)
           +if oldID!=0: retirement_count*2*S(COWRetirementV1)
                            +total_retirement_PageIDs*16]
```

The frame loop admits its original disposing-edge clear before validation. All traversals use original allocator+ledger cut. Claim expansion with coverage0, saturating checked original bound arithmetic:

```
activation.records = 3 + if L==capL: (1+L)
activation.bytes = 96 + S(candidateBeginWorkV1)
                 + if L==capL: S(candidate pointer)*(3L+1)
A.records = 1+C + n*(1+O+n) + n*(1+I+n) + n*(1+B+C)
            + activation.records + 1
A.bytes = S(reservation)+128*(1+C+O+n)+8*n*(1+I+n)
          + activation.bytes +48
```

All actual mutable claim/activation work must fit the reduced SAME remaining grant before claim inserts or ledger transition. An observed earlier value is not authority; failure to fit refuses while private custody is still intact. Snapshot work Z is also entirely before adoption: stale transition is `1+pins`, bytes `S(pinSet)+pins*(S(ref pointer)+16)` when an old nonstale PinSet differs, with pins equal its refs only if holders>0 and not already pinned. New nonnil LeafGenerations with no snapshot acquire in flight adds `len(tracker.refs)*(1+len(GenerationOrder))` records, bytes `len(refs)*S(ref pointer)+records*8`, under original tracker lock. If both original state views have nil LeafGenerations, Z=0. The observer must establish that actual fact; the final code does not silently impose a nil-only public capability. Snapshot changes are not committed on failed allocator activation.

R's complete expansion (each byte row is its actual helper expression in `native_publication_remainder_v6.go`):

| Postactivation operation | Records |
|---|---:|
| Supported original ordered post | 11 |
| Native publisher locals, seal lookup, exact visible head | 6 |
| Executor/header/observation/materialize dispatch plus BOTH empty dependency operations | 10 |
| Materialization headers6 +canonical validation7+3P +capacity count6 +DB prefix P +candidate P+7M | 19+5P+7M |
| BOTH captured token syncs, BOTH concrete callback+retained-FD barriers, PRIMARY namespace, DB dispatch | 48 |
| Authority wrapper2 +runtime13+6P+3S +allocator4+10P+N +poison3 +allocator-failure3 +typed capsule14 | 39+16P+3S+N |
| Set logical handoff, transaction dispatch, concrete member Consume | 8 |

The SAME runtime scope uses exact-head proof and prefix-only clearing, not whole current tail compaction. Native `scope.Commit` calls original policy with `eagerPrune=false`; it neither uses nor stores its `nextCapability` argument. It receives canonical zero-value `ReuseCapability{}`. Ordinary scope==nil still computes its real `durableRootReuseCapabilityV1`, including MinPinned and current floors; separate oldest-recoverable-slot observation is retained for return/retirement semantics. No minPinned=0 scalar is fabricated. Next original preparation obtains its real capability and performs deferred conservative reuse. Pure controls prove native does not invoke MinPinned hook, ordinary does once, native retains retired bits and next genuine preparation reclaims them.

## Native physical primitive contract and ordinary preservation

Native mode is an explicit selector on the SAME page/typed capsule cores, not work!=nil. Ordinary and generic work-bearing writes retain existing masked verified-bit CAS and dirty bookkeeping. Native writes conservatively `atomic.StoreUint64(word,0)` on the checksum-cache word; this can discard up to63 neighboring verified bits per write. Later readers may perform extra actual checksum verification. It never validates bad content or changes reachable data/visibility. Three-page capsule clearing may affect other bits; it is cache loss, not hidden bounded reader work inside F.

Both native captured index callbacks and typed capsule use one Linux retained-FD `unix.Fdatasync`, KeepAlive, one stack `unix.Fstat`/`Stat_t`, KeepAlive, and original durable-extent observations under pager RLock. No FileInfo heap object, EINTR loop, ENOSYS/EINVAL fallback, path reopen, p.file substitution after retirement, or native MS_SYNC traversal. Every errno is a genuine physical error, with stricter availability than ordinary retry/fallback; no kernel wall-time bound is claimed. All original dirty membership is preserved. Later ordinary sync can redundantly scan/detach/sync that bookkeeping. This deferred work is disclosed and is not proof of aggregate bounded maintenance.

The original capacity count install is 6 records and page write is 5 plus two concrete page/image dispatch records; typed capsule is14. Linux unsupported/observer/mapped-flush/closing/read-only/current-file shape is rejected before activation by final basis capabilities. Resource capture owns exact FD through actual consumers. Primitive tests exercise retired pager/path replacement against SAME old inode and preserve dirty bits/count/hints. No portability qualification is claimed.

## Completion, retry/error, demand and locks

Successful completion uses shared scalar frontier/service/lineage helper without eager whole-waiter settlement. WaitThrough observes original changed under coordinator mutex, preserves buffered original attempt outcomes and original cancel/poison precedence. Native completion marks original demand dirty; original service/caller dispatch reconciles before sleep/eligibility. Its actual O(waiters+drains) membership/recompute, own waiter removal/cancellation, timer rearm and notifications remain ordinary service/caller work. They have deliberately moved outside F and are not a universal aggregate latency/progress bound. Ordinary completion retains its old eager helper. First diagnostic admission requires zero initial waiters/pending/pin maps, single native receipt and original counts; later ordinary entries/waiters may register during physical I/O, and native completion must leave those entries intact. No external controlled schedule substitutes for that source protocol.

Before authority, insufficient work, unsupported platform/shape, stale exact base, active conditional transactions and cancellation retain refusal/resumption/private cancellation. They must not be converted to I/O errors. The supported final context is canonical Background; arbitrary cancellable contexts remain unsupported diagnostic geometry, not a cancellation-success claim.

For genuine postactivation physical hard-I/O error, phase classification remains known-unfenced versus potentially-mutated ambiguity. Fixed failure/poison operations were admitted before possible authority mutation; candidate's exact activationOwner backlink avoids mutable vector rescan after unlock. Original Cond.Broadcast is counted as original runtime notification service, not an alleged bounded user-level waiter traversal. The first cause is latched in admitted embedded error storage. Logical attempt/turn clears only after actual call returns; SAME whole native frame, transaction, pending candidate, external admission lease and both dispatch counts stay linked/charged. No ACK, private Abort, output rollback/refund, scheduler adoption or new physical retry occurs. Repeated Prune/Close/Join/Stop reports the same cause. This is explicitly **errored physically incomplete Close**, not successful error teardown. Successful recovery/capture handoff after that state remains unsupported.

A review obligation is explicit: after adoption, all nonphysical `false`/protocol returns in ordered post/publisher/Consume/Complete must be proven unreachable for supported retained operands and the preadmitted R. Those branches are not silently reclassified sticky I/O. If independent review finds a valid after-adoption stale/shape/budget path, that is a remaining closure defect, not permission to execute or increase an allowance. In particular review the capability-to-use interval, active seal/prefix stability while ordinary Activate is allowed, pending attempt identity, exact two callback lifetimes, original lineage/preflight formatted-error branches and no-yield Cut/DB lock inversion. No runtime evidence currently proves these concurrency invariants.

## Genuine Finish: full conditional operation tree

Finish has its own SAME work per continuation, can yield, and cannot replay durable frontier. Its source-level total is the sum over actual visited states below; retries include their real repeated headers. There is no single fixture scalar that prices a graph changed by a newer writer, and no successful universal Q32/C3 claim. Every dynamic primitive checks its actual geometry under its original lock before effects; an oversized indivisible unit may refuse indefinitely, retaining original receipt/charges.

| Original Finish caller | Applicable actual work and last-alias obligation |
|---|---|
| Published phase7 | Candidate and seal `primaryBackingRefundV6.complete` each drive their original RemovalWork; then header1 and `BindFinishV6`1. Only after binding clear candidate/root/seal borrowed aliases. |
| `ContinueFinishWorkV6` -> `finishPrimaryStepWithWorkV6` | Original transaction descriptor admission each attempt; invoke SAME Retire callback without holding t.mu across callback; original finishBusy serialization remains. |
| `continueNativeRetirementV6` | Header1 each attempt; retired primary root queue-init/drop/control units; actual arena queue nodes; post.oldState transfer3 into SAME member fields/queue; post root and VLogSet release; retired visible Set typed graph; original retired runtime disposal; each original retired seal and its vectors; admitted vector alias clearing and accounting. No immutable DBState mutation or extra retained edge. |
| Constructor arena cleanup | Original root DropToReleaseQueue; actual `Arena.ReleaseQueueStep` over each real node/dependency/lastref and pager registration removal; original queue persists in actual parent across return and final physical callback. |
| Coordinator-owner Set release | Original typed release callback2 plus original `StableResourceSetReleaseWork`: actual last/nonlast, token/dependency-directory/identity/namespace/provider branches, counted original pin consumption, resource-specific physical callbacks and real lastowner operations. Empty logical pins at fence never imply a nonlast physical Set. |
| Terminal constructor accounting | Pre-admit original Set completion transfer; clear callback/root/transaction/constructor aliases before accounting; original `primaryTerminalCompletionV6` carries only by-value owner/charge/next, then bind to original `RemovalWork`. Actual physical return precedes containing refund; no final-owner charge discarded. |
| Published phase9 | Original allocator published-close2 headers plus both normalizer scratch completions; release SAME finite-only scratch. `Cancel` invalid after adoption. |
| Published phase10 | Original pending accounting; output `CompleteWithWorkV6` over actual frames, AppendWork/leaf state and typed refunds; resulting owner/charge returned into actual parent and aliases cleared. |
| Published phase11 | Pending refund; original seal close including both capture state machines; exact token/index namespace/identity/FD/Snapshot/registry edges and current-generation revalidation at actual consumption; input materialization and projection-frame cancellation; outer alias-clear1. Late generation retirement may refuse while retaining all original custody. |
| Linked receipt detach | DB wrapper1 plus original CompleteNativePublication work. Current supported no-error success path has fixed14+L records, L actual original link-list visits; errors/observer branches add their actual admitted rows. Exact builder/handoff/inline aliases cleared before link/counter teardown, never c.mu-only access to mutex-protected initialized builder state. |
| Outer cursor terminal | Original external lease close, real typed accounting removal and final containing cursor scrub; original phase22 ACK only after all above completes. Close/Stop must join this actual path; unlimited legacy Close/Stop is not itself a bounded callback. |

Exact byte terms and branch costs are the source Reserve expressions in these original implementations: `primary_member_retirement_work_v6.go`, `native_primary_finish_work_v6.go`, `primary_transaction_construction_v6.go`, `resource_set_release_work_v6.go`, `primary_runtime_metadata.go`, `native_mvcc_published_terminal_v6.go`, `native_mvcc_seal_preparation_v6.go`, `native_stable_pager_capture_v6.go`, `allocator_preparation_work_v6.go`, zipper materialized completion, original arena release queue and retainedalloc `RemovalWork`. The owner/enrollment scalar geometry proposal below is required to instantiate these conditional terms; count actual graph nodes/lastref states, not assumed future fixture sizes.

## Canonical disjoint retained ledger

Let `Q(x)` be original `AllocationCharge(actual bytes)` for a physically independent allocation x. The retained total is the sum of Q(x) for the union of actual native-admitted allocations reachable from the original cursor/member/candidate/seal/receipts, counting each backing once by its original allocation identity, plus already-owned retained accounting deltas not yet consumed by original RemovalWork. Newly added storage is not exempt merely because a callback aliases it. The union is not a pool/registry implementation; the observer only emits scalars/dedup booleans.

| Canonical allocation/lifetime domain | Disjoint rule |
|---|---|
| Outer `PruneCursor` | Actual containing charge794624, size794392. Includes CurrentDelete68816, NativePublication336+inline16, allocator prep24264, capture/authority/refund/queue/control frames. **Do not add their standalone rounded layout samples.** Lifetime reaches final callback, detached receipt and terminal cursor clear. |
| Actual member + install + ten callback environments + nested allocator environment | Existing containing-admission expression3744, once. Install includes finalizeCommitPost1280. Release queues/terminal receipts embedded in those frames are not separate rounded allocations. |
| Actual seal + primary seal + original prefix backing | Existing P-dependent admission; P1 sample3088. No duplicate charge for embedded capture frames. Includes preborn descriptors until original member retirement/consumer detachment. |
| Independent runtime descriptors/backing | Primary durable runtime Q=4096; publication runtime Q=512; original full capacities for visible/debt/seals and retired vectors charged once. `debtBackingCapacity` preserves full original charge across native prefix advances/empty suffix/ordinary append/compaction. Replacement preadmits new+old overlap, clears actual aliases, resets exact capacity only after success. |
| Detached old backing accounting | Exact old charge moves out of runtime.metadataCharge into SAME candidate/seal `primaryBackingRefundV6`, while Owner.Bytes retains old+new until original RemovalWork completes. No runtime-owned native continuation, no next-Prune obligation; cancel/Finish drains. Do not count old charge both as current runtime backing and transferred debt. |
| Independent snapshots / operations / state headers | Each actual Snapshot Q2048, operations Q256, DBState Q128, independent snapshotView header and token/construction storage at actual constructor admission. Two real capture snapshots remain independently pinned, including every nonnil empty Set. Borrowed aliases do not create a second allocation or permit early refund. |
| Original PRIMARY/new native graph | Actual constructor/environment/header/directory/token/namespace/identity-pin/pager metadata and enrollment/binding allocations, frame/image/key/retirement vectors, private preparation copies and finite-only scratch use original owner/charge fields and independent capacities. Include ownership-table nodes/banks at independently rounded actual bank+node sizes and original Add/resize forward+rollback admission. Unsupported protected rehash/growth refuses before Add. |
| Canonical DATA output | SAME ordinary allocator output graph stays under accepted #5111 inherited DATA custody; not re-enrolled transitively into selected PRIMARY quota. Native-only retained controls, private copies/sort scratch and work-bearing wrappers remain included. No accumulating generation owner framework. |
| Original coordinator runtime services | Actual native-invoked channel/timer/clock operations and allocation traffic counted in F. Existing GC-managed changed/timer outputs have original GC lifetime; observers may retain old closed channel. No per-channel native-owner refund, new borrow registry or transitive runtime heap claim. Pinned ELF evidence: hchan112/timeTimer112/time.Time24/publicTimer16, realTimer no separate wrapper allocation. |

The observer must instantiate the complete union, not add sampled layout rows. **No actual aggregate retained number is available at this freeze.** The known layout samples are not the retained total. Current source prices actual containing sizes, while the freeze and observer expose unknown live capacities/enrollments rather than asserting a fabricated subtotal. Peak overlap includes both replacement backing and pending old accounting; physical bytes survive until actual last alias and original accounting completion.

## Proposed single bounded prebuilder observer — NOT executed

Use the SAME unchanged actual33 test/Store/Request/selection+anchor and original Q32/1MiB route. Stop at original phase26/candidate8/seal13 **before builder acquisition, activation, enqueue, GrowTo/count install, page write, fence, Consume or Finish**. No new fixture, producer, publisher, authority or production observation API. Add only same-package test support with by-value scalar result; no pointer/root/pager/owner/FD/closure/slice escapes. Return explicit unsupported/opaque rows.

Minimal output, each read/visit admitted before inspecting under original short locks:

1. Boolean exact basis equality: original Request/current seq/root Ref/certificate/durable slot/meta/fullrecord/parent Ref, actual two constructor index identities and retained FD identity, preborn storage/strict Linux capability, original graph owner/enrollment identity. Emit booleans and counts, no authority operands.
2. Original P,D,K,key-byte sum,M,per-image size validity,S,N-upper/highwater/capacity-covered13->16. Use SAME canonical bound and scope formulas (not old historical probe aggregate). Coverage count must be reported. Any nonzero coverage on actual33 is an obstruction; no retry until empty or altered fixture.
3. Original activation L/capL/n/C/O/I/B, installed J plus per-frame allocation/retirement sums needed for V, all captured while original allocator+ledger protect them. Also original snapshot-pin Z geometry under original pin locks. Aggregate scalar sums with checked overflow; no count-first/reserve-after traversal.
4. Actual independent allocation charge/capacity scalars for each ledger row, owner-binding/active-enrollment counts and supported concrete lease booleans. Dedup equality booleans establish embedded/independent domains; return charge sums only after each visit is admitted. Report old detached accounting separately. Existing original Owner may expose concrete protected inspection through same-package pure test support, not a new permit or registry.
5. Emit each F term `(sourceSymbol, records, bytes, geometry, ready)` and the union retained total only if every term is observed under a compatible basis. Original short lock order only, no all-graph lock or retained lock across yield. If observation spans calls, retain only admitted scalar accumulators on existing test state and validate unchanged basis before final report; on change report invalidated observation, not authority/repricing. Dynamic later-writer Finish is a conditional formula and remains uninstantiated; this observer cannot certify future lastowner state or universal progress.

The separately named observer requires root review/runtime authorization before implementation/execution. F is source-evaluable but its actual geometry and complete disjoint retained total are presently unobserved. Neither this proposal nor an eventual observer is a diagnostic publication grant.

## Exact checks, first errors, and review handoff

All current compiler/pure controls used pinned Go1.26.3 executable SHA256 `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`, GOMAXPROCS2, -p1, GOTOOLCHAIN=local/GOWORK=off, task caches/tmp, 3m package timeout, outer230s and process-group join. Full commands/environment/PID/PGID/source-before/source-after/archive/result are in `native-build/atomic-completion-source/<run>/`.

- `connected-driver-compile-01`: FAIL1.2425s, exact10337 archive; mechanical undefined `w` after factoring original pager capacity predicate. Fixed by explicit bounded bool, ordinary predicate policy unchanged; failure retained.
- `connected-driver-compile-02`: PASS68.855455s, exact10337 archive, pager/freelist/rootpublication/db/mvcc compile, no tests (`-run ^$`).
- `connected-pricing-normal-01`: PASS55.481725s, exact10338 unchanged before/after, archive verified, leader joined and process group absent. Twelve named pure/pricing/primitive controls in command.json, actual runtime native final driver absent. Includes R arithmetic/overflow/floor restore on success/refusal/error, native zero-capability no-MinPinned and ordinary hook control, conservative native reuse plus next preparation, full-backing accounting transfer, bitmap explicit policy and same retired-FD/dirty controls.
- Prior `pager-fixed-fence-normal` and `pager-fixed-fence-race` both PASS on their exact10334 archived images, with group receipts. They cover A/B primitives but are not a race run on final10338. No final-source race or actual previsibility rerun is claimed; root directed no more component runs at this checkpoint.
- Older `previsibility-prefix-growth` is manifest/leader-only evidence for its transient source, not fully reconstructed final-source evidence or group verification. The baseline separately recovered growth-yield repair remains independently accepted by root. No promotion of any historical evidence binding.

Review requested on this full connected packet: applicable F/bytes and same-work floor, exact preactivation failure atomics, supported after-adoption nonrefusal proof, runtime/full-backing charge representation and accounting transfer, ordinary waiter/demand semantics and later-tail preservation, Linux explicit primitive selection, Set lastowner terminal completion, all original Finish/Close error paths and true last aliases. Missing full actual geometry/retained total and C1/C3/error successful teardown/platform/progress gates remain explicit. Source/runtime lease released; worker idle pending root disposition.

### Exact observer implementation attachment (proposal only)

The existing original test attachment is `TreeDB/mvcc/native_prune_backend_route_v6_test.go:testNativePreEffectGeometryV6`, invoked by `TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6` only after phase26/candidate8/seal13 assertions. Its existing `MaintenanceNative*GeometryForTestV6` methods are the extension points; do not add another test route. The old logged partial124/scope37 report remains historical and must not be silently relabelled the new complete report.

| Proposed changed path/symbol | Minimal scalar-only change |
|---|---|
| `mvcc/native_prune_backend_route_v6_test.go:testNativePreEffectGeometryV6` | Preserve original fixture and original guards; gather extended fields through work-bearing existing helpers, compare original before/after basis, emit individual terms, refuse report completion if any field missing. No call to Fence/Enqueue. |
| `db/native_mvcc_seal_preparation_v6.go:MaintenanceNativeBasisGeometryForTestV6` and existing allocator/resources/index geometry wrappers | Extend existing by-value observations with full-capacity/charge and operand-equality booleans, exact P,D,S/K bytes, original prepared state and current/promotion identity equality. Stack/result traffic admitted first. Delegate original remainder pricing without builder or authority effects. |
| `freelist/allocator_publication_work_v6.go:MaintenanceNativeGeometryForTestV6` | Under original allocator->ledger locks emit exact n/C/O/I/B, activated capacity and empty-coverage fact, invoke SAME claim/activation calculators; include native bound's canonical metadata/materialization inputs. Fixed admission before headers, per-item admission before traversals. |
| `zipper/materialized_write_work.go:MaterializedWriteWork` test-support observation | Add only by-value installed-frame counts/byte totals and equality booleans; admit header and each frame before reading. No page/result/AppendWork pointer escapes. Original producer remains sole source of actual installed-frame count. |
| Existing `rootpublication` coordinator/empty-Set/index geometry helpers | Extend original scalar support with actual constructor/token/Set header charges, equality/lastref eligibility booleans and current original link geometry. No new diagnostics registry. Native/public modes unchanged. |
| Existing `retainedalloc.Owner` same-package test support, only if current scalar access is insufficient | Emit original allocation/binding/enrollment counts and bytes under Owner.mu, with concrete lease-support and identity-equality booleans. Admit before each binding read; refuse unsupported concrete capability. This does not inspect arbitrary transitive DATA/runtime storage. No owner/lease handle exported. |

Observer's own scalar accumulator/backing must be born/admitted on existing test state before use and included in diagnostic traffic separately from production retained union. Each helper's report records its own measured work separately, so observation traffic is not confused with F. A graph changed between short lock scopes invalidates the aggregate. Future Finish graph after a newer writer remains a conditional observation at its actual execution boundary, not an invented current snapshot bound. Root may reject the extra test-support exposure or choose a smaller first observation; in that case retain incomplete rows rather than claim the full total.
