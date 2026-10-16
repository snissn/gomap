# Independent frozen native connected-driver review

**REJECT** — three source blockers (N1–N3). This is an independent source/evidence review of the complete frozen connected native packet, not a publication, runtime, numeric allowance, performance, merge, or native qualification grant. The original actual 33/80/4096 Q32/1MiB result remains **RED / ACK0**.

Reviewer: `/root/ordinary_review`, explicitly routed **gpt-6.1-sol / high**, `INTERNAL_SUBAGENT_REVIEW`. `candidate_edits_by_reviewer: false`. No children, source changes, Git/GitHub mutations, builds, Go tests, benchmarks, profiles or publication runs were performed. Hosted manual review was not requested because it cannot pin the required reviewer model; this is not a claim of a hosted review failure or quota response.

## Exact candidate and independent binding

The reviewed candidate is the frozen working-tree image over HEAD `3dd8d2d11d129ea93fc6277215d06d7a4ba65650` (HEAD tree `5d91d2a93fc4df488002a1ba4e4e54a5ea8aca04`), **not that commit alone**. All 42 changed paths, including eight untracked source additions, are included.

| Object | SHA256 / result |
| --- | --- |
| [10338-file manifest](../native-build/atomic-completion-freeze/source-frozen.json) | `ccaa15b2381b87336937688cdc1e672c875564261da53fd6ba5c1f5e814107e6` |
| [Full reconstructable archive](../native-build/atomic-completion-source/connected-pricing-normal-01/source-before.tar.gz) | `761722ad4d5a3b41a19934d00b45f810c229727eb0d9c682170da9a2f5eab929` |
| [Supplemental tracked patch](../native-build/atomic-completion-freeze/tracked-source.patch) | `1d6dc794125e9ba128dd661b506731b0b3e27e33ee50a6dcca0d402f318c075b` |
| [Decision packet](atomic-completion-frozen-packet.md) | `4d004a97faf71b4e0eb2194fb4b73dd80be34652ce6c80d4b5d82a7e948dad0c` |
| Pinned Go1.26.3 executable | `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65` |

I independently hashed every current manifest path and every archived file: 10338 current files and 10338 archive files match exactly, with no archive missing/extra path. Pricing run before/after manifests equal this same manifest. The final read-only Git inventory still shows exactly the frozen 34 modified/eight new paths. Compiler hash matches its command receipt. The source archive includes new files; the tracked patch alone is insufficient to reconstruct the candidate.

## Blocking finding inventory

### N1: Native preparation retains two independent headers without retained admission

Primary anchor: [TreeDB/db/snapshot_publication_work_v6.go:13](../native/TreeDB/db/snapshot_publication_work_v6.go:13). Related: `TreeDB/db/native_mvcc_candidate_preparation_v6.go:160`, `TreeDB/db/native_mvcc_candidate_preparation_v6.go:174`, `TreeDB/db/root_publication_activation.go:657`.

**Trigger.** Phase5 allocates activationState, then the independent activationSnapshot allocation or a later preparation step refuses/yields before adoption.

**Causal path.** The two heap headers receive only OrdinalScanWork Reserve. Candidate/member admission and phase4 root metadata allowance do not cover these independent allocations. Both can survive a return in private preparation; successful outputs subsequently escape through DB state/snapshot publication.

**Effect.** The claimed native retained union omits newly retained private storage. Refunding with the containing candidate would also be invalid after publication.

**Correction.** Remove private preborn headers and price their allocation/init before adoption, allocating and immediately publishing only in the nonyielding accepted original install under an explicit original-GC-output accounting decision; alternatively prove a genuine admitted lifetime for every escaped alias. Do not attach a containing-member refund to published headers.

### N2: DB.Close proceeds with ordinary teardown after native join refuses settlement

Primary anchor: [TreeDB/db/db.go:3497](../native/TreeDB/db/db.go:3497). Related: `TreeDB/db/db.go:3522`, `TreeDB/internal/rootpublication/native_publication_v6.go:255`, `TreeDB/internal/rootpublication/native_publication_v6.go:607`, `TreeDB/internal/rootpublication/coordinator.go:788`, `TreeDB/internal/rootpublication/coordinator.go:1612`.

**Trigger.** A genuine selected native physical I/O failure sets recoveryRequired while the original receipt, pending candidate and native dispatch remain linked; DB.Close is called.

**Causal path.** JoinNativePublications returns the sticky error and Stop preserves active native dispatch, but DB.Close merely appends both errors. TakeRecoveryHandoff then accepts poison, captures/clears pending and transfers the original transaction to ordinary recovery custody; DB.rootPublication is cleared and later teardown runs.

**Effect.** The native still-linked completion graph loses its required coordinator-owned pending/transaction state and DB discovery while its physical settlement is explicitly incomplete. This is not a valid successful Close and is not the selected error-state Close that preserves the same receipt/frame graph.

**Correction.** Make failed native join a real DB teardown boundary, preserving the original coordinator/receipt/transaction/callback graph and sticky error across repeated Close/Stop observation; do not invoke ordinary recovery transfer or resource teardown while native join is unsettled. Any alternate terminal handoff needs its own proved same-original representation.

### N3: Native capture invokes ordinary owner-binding fan-out and synchronous unwind behind fixed work reservation

Primary anchor: [TreeDB/db/native_stable_pager_capture_v6.go:363](../native/TreeDB/db/native_stable_pager_capture_v6.go:363). Related: `TreeDB/db/stable_pager_owned_operations.go:42`, `TreeDB/internal/rootpublication/resource_original_cleanup.go:91`, `TreeDB/internal/retainedalloc/owner.go:88`, `TreeDB/internal/retainedalloc/owner_growth_work.go:14`.

**Trigger.** Native capture phase4 constructs original stable pager operations while the metadata Owner has reader/enrollment bindings; success or a subsequent constructor refusal exercises owner growth/rollback.

**Causal path.** Reserve6 precedes ordinary newStablePagerOwnedOperations. Its AddPending and NewOriginalCleanupCustody AddPending use ordinary resizeLocked, which visits all bindings and may roll back them. Failure invokes ordinary RemovePending and possibly custody.Complete synchronously. No actual binding discovery/forward/rollback work is admitted through the native work object.

**Effect.** The actual pre-effect native Q32 route performs geometry-dependent synchronous work hidden behind a fixed six-record reservation, on successful construction and on unwind. The existence of a work-aware Owner growth seam elsewhere does not price this caller.

**Correction.** Make this SAME original constructor a native work-bearing continuation: admit protected discovery and real forward/rollback costs before effects, preserve partial custody and pending accounting on the original capture across refusal, and complete genuine refund after last aliases. Preserve ordinary nil-work behavior; do not substitute a larger fixed precharge or a helper owner/registry.

N1 concerns newly retained **private native preparation**. It does not silently enroll the inherited ordinary published DBState/snapshotView GC domain into a new lifetime scheme. The existing root-metadata admission covers `primaryStateRootV5` only. The packet's ledger row claiming all state/header storage is admitted at construction is therefore inaccurate for this freeze. Two captured Snapshot/DBState objects are separate allocations with their own existing admission; they do not cover these two publication headers.

Root's proposed N1 repair direction is source-feasible subject to review of its actual implementation: eliminate private preborn-header storage, reserve both actual allocation/initialization operations before adoption, allocate only after adoption in the same nonyielding install, and immediately publish the original outputs under an explicit original-GC-output scope decision. Price Z from the same leaf-generation operand later installed under original exclusion. Introduce no post-adoption admission/refusal branch or retained wrapper. Count operation/allocation traffic without inventing a heap/quota total or per-header refund. This is a conditional direction, not acceptance of unreviewed source or permission to exempt other native controls.

N2 does **not** assert that the whole failed graph must become garbage: the embedded sticky error can retain its containing frame and backpointers. The concrete violation is the ordinary recovery ownership transfer/pending withdrawal and teardown of a graph that the native receipt still requires to remain linked and unsettled. The valid scoped error policy is to return the original sticky error while preserving that graph; successful Close requires genuine settlement/join. `Coordinator.Stop` already respects this distinction, but its DB caller does not.

N3 includes both successful construction and failure paths: the first metadata addition, cleanup-custody addition, forward resize of bindings and rollback, followed by ordinary synchronous shrinking on constructor failure. Its phase is before final F, but it remains part of the actual native preparation continuation and its Q32 contract. Canonical inherited DATA allocation scope does not exempt this newly invoked control constructor.

## Connected-driver and lifetime coverage

I read the complete tracked diff, all eight new files, all 42 changed-path implementations/tests, and the actual caller paths needed to assess the complete connected driver rather than isolated seam tests. The JSON companion lists the changed paths. Adjacent review covered:

- `PruneCursorV6` phase26/27, candidate/seal preparation, final basis and binding, the real current-delete entry, final native publication, published terminal close and ACK boundary.
- Allocator canonical validation, claim/activation pricing and rollback, ledger activation, materialization and page-count capacity, ordered post-activation work, root activation/snapshot pins, authority install and durable-root commit.
- Coordinator Enqueue/WaitThrough/Drain/service/Stop, native builder lease and dispatch, pending-head completion, original attempt/frontier identity, failure completion, Join and ordinary recovery handoff.
- Both original captured index callbacks, pager write/invalidation/grow/file barriers and typed capsule, original cleanup custody and identity registry, Set logical handoff and actual last/nonlast physical Finish.
- Actual retirement of old DBState/root/Set/runtime/seals, PRIMARY arena release queues, original transaction terminal/removal work, detached visible/debt backing accounting and cursor external completion/refund; DB.Close and resource/index/runtime teardown.
- Applicable root/TreeDB AGENTS and contribution policy, task-local Go performance skill and internal-review fallback. Persistent vlog and physical-last-consumer semantics remain required.

The source has several coherent improvements, which do not cancel N1–N3:

1. The final driver uses the original work object and locks build admission in `writeMu -> durablePublishMu -> rootReuseMu` order. It withholds the actual publication remainder from the same grant before claim/activation, and releases the private Cut/builder on pre-effect refusal. The source expression is `F.records = 138 + D + 2K + 2P + M + Z + V + A + R`, with `R.records = 141 + 21P + 7M + 3S + N`; equivalently `279 + D + 2K + 23P + 8M + 3S + N + Z + V + A`. Byte terms are the reached Reserve expressions, not a transitive heap total. This is a conditional expression, not an instantiated actual33 bound.
2. Native publication captures the original prefix; completion clears/subtracts that exact head and advances the slice rather than copying or destroying a later ordinary tail. Ordinary below-hard enqueue remains allowed during unlocked I/O. `c.publishing` serializes durable publication, and the supported scope checks original candidate/seal/slot/record identities. I found no additional concrete supported post-adoption budget/stale refusal in the examined success path under those retained preconditions. This is source reasoning, not a concurrency runtime proof; the complete route is rejected for the concrete gaps above.
3. The original constructor Set edge is handed logically to the same genuine Finish instead of relying on a last-ref assumption at the physical fence. Actual typed Set release handles late sharing; phase7 clears borrowed aliases after binding Finish. Both capture callback frames remain in the original outer frame through seal physical completion. Their consumed tokens/operations are cleared before the containing owner is refunded.
4. Old/new visible and debt backing overlap remains charged. Detached old accounting follows the same completion edge; debt backing remembers its full original capacity despite prefix subslicing. The source does not refund a live alias by assuming current logical length is physical capacity.
5. Native verified-word invalidation uses a zero store, conservatively losing adjacent cache hits while preserving ordinary masked policy. Linux retained-file barriers use the original retained FD and original pager locks, one `Fdatasync` plus stack `Fstat` and KeepAlive; no repo-owned EINTR retry/fallback is hidden. Any errno is genuine I/O failure under the explicit stricter availability policy. Native dirty membership remains intact. Unsupported platforms/mapped-range/observer shapes refuse before activation. Kernel duration is not bounded by this source-count reasoning.
6. Finish remains a genuine work-bearing continuation over the actual current graph, not a fabricated universal scalar. Later writers can change last-owner/retirement geometry. Oversized indivisible units can refuse without refunding the original receipt. No universal Q32 progress or successful error teardown follows merely from the conditional formulas.

## Evidence actually checked

The exact `connected-pricing-normal-01` command/result/raw log contains twelve passing top-level controls across pager, freelist, internal/rootpublication, db and mvcc, zero top-level failures; exit0, elapsed55.481725s, source equal, leader joined and process group empty. I read these retained receipts/logs rather than rerunning them. Its command uses pinned Go1.26.3, `-p=1`, `-count=1`, `-timeout=3m`, outer230s, GOMAXPROCS2 and GOMEMLIMIT8GiB. The component controls cover native/ordinary invalidation and retained-FD barriers, ordinary dirty retry, reuse policy, pricing refusal, synthetic remainder/floor, supported post-fence capability, containing-size admissions, and detached backing accounting. They do not execute the actual publication route.

The earlier undefined-`w` compile failure remains recorded; its corrected compile/pricing receipts are not relabeled as actual publication evidence. Earlier pager race controls bind older manifests and are historical. I do not claim a final-image race run. The pricing `go test` executable is not retained as a standalone ELF in that packet; binding here is source/archive/compiler/command/raw receipt, not an invented ELF digest. No fresh hosted correctness/performance, actual aggregate F, complete retained total, portability qualification, actual native publication, ACK, final Dgraph, C1/C3, or mergeability evidence is supplied by this review.

## Prebuilder scalar observer decision and smallest next batch

The proposed observer's **design boundary is reasonable**, but it is not permission to execute the rejected connected route or to increase Q32/1MiB. It must use the same actual fixture/Request/selected+anchor producer, stop before builder/adoption/enqueue/grow/write/fence/Consume/Finish, and emit only admitted by-value scalars and identity-equality booleans. Geometry must come from the original protected operands, with per-visit work admission and basis invalidation across short lock scopes. It must separately account diagnostic traffic. No owner/lease/file/authority operand may escape, and no partial layout subtotal may be labeled the full retained total. Future later-writer Finish geometry remains conditional.

Recommended coherent source batch: fix N1 private-header storage/admission, close N2 at the actual DB teardown caller, and make N3 the same original work-bearing constructor/unwind. Update the packet's ledger and caller obligations accordingly. A narrowly scoped scalar observer can be constructed in that batch and independently frozen/reviewed; root must separately authorize any bounded execution. First expose missing/unsupported rows rather than force a number. Reuse existing primitive controls plus targeted negative controls for these real boundaries; this review requests no broad runtime campaign or arbitrary allowance.

**Disposition: REJECT exact frozen manifest `ccaa15b2381b87336937688cdc1e672c875564261da53fd6ba5c1f5e814107e6`. Source/runtime review lease released after this artifact.**
