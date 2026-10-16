# Selected resource terminal unit bound

Read-only advisory on immutable source manifest **5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d**, archive **0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4**. Reviewer: gpt-6.1-sol/high, internal independent advisory; no construction, children, Go execution or runtime grant. This supplements terminal-unit report 4e170295e7cb844c85fef3569899dd7991b6ebeb64c1b55c651af5ea78700e4d. DATA AppendWork/Growth, coordinator wrappers and empty-vlog-Set/manager remain their assigned lanes.

## Result and precise remaining seam

Resource graph, token, namespace and owned-pager local reservation rows are reducible below. **The last-idle IdentityPin branch is not already a work-bearing owner-removal continuation.** It synchronously calls the original embedded nonnil registry metadata Owner.Remove after table deletion (pin_registry.go:328-330). Native capture explicitly removes its temporary observation at native_stable_pager_capture_v6.go:383-388; a designated old reader does not by itself prove an identity observation. Do not discard this branch.

A whole synchronous removal has the finite conditional envelope below, but the current source does not debit its nested work from the supplied cursor. The implementation must explicitly disposition/pre-admit the whole indivisible unit with the exact original owner/capacity/type proof, or supply a same-parent bounded continuation. Merely adding its symbolic value to a report does not admit it. This is one concrete source-work seam, not a request for another registry or global graph traversal.

## Notation and scope

`(r,b)` means records and bytes; add vectors componentwise and check every multiplication/addition for overflow. `s(T)` is the real target-package unsafe.Sizeof(T), never a containing charge. `Uremove`, `Erelease`, Drop, Queue and SnapshotFamily are the explicit typed formulas in the preceding report, with the corrections below. A next-progress envelope includes repeated ancestors before the next stored phase/index/current/refund advance. These are bounds on repository work, not kernel latency or total Finish.

Selected scope is one ready frame, two real index captures, four selected empty resource wrappers and the actual empty manifest. Preserve exact empty/callback/provider predicates. Other logical/physical resource graph shapes are not inferred from logical count zero. DirectoryRelease is the original DependencyDirectoryReleaseWork already assigned to the writer's empty-Set lane; its actual constructor Add and provider step must be composed, not replaced with zero.

## Identity pin: both branches

Sources pin_registry.go:254-342, pin_registry_capture_work_v6.go:12-35,67-98; registry_capacity.go:179-184.

```
P0 = (1, 2s(IdentityPin)+2s(identityPinState)+128)
PH = (1, len(identity.Platform)+len(identity.ObjectID))
PC = (1, 2s(registryCell[StableIdentity,*identityPinState])+2s(identityPinState))
PinObservedOrNonlast = P0
PinLastIdle(C,B) = P0+PH+C*PC+Rwhole(B)
```

`C` is a proven upper bound on the **actual states.cells length**, including empty/tombstone cells, through this call. The search starts from scratch each retry under registry.mu; at most C probes for a present unchanged state. Preserve physical identity generation normalization. String hashing is paid by PH; equality operands have those same bounded identity strings. `B` counts **all** intrusive owner bindings, including cleanup-held/zero-ref entries, not just live enrollments. Registry metadata is an embedded Owner initialized at pin_registry.go:134-151, never nil.

Using the prior report's exact I,H,V,J,Kbinding,Dbinding definitions:

```
Rwhole(B) = I + (B+1)*(H+V+J+B*(Kbinding+V+Dbinding+V))
```

This overcounts a full nil-work Owner.Remove (owner.go:190-200; owner_work.go:250-288,68-199): one debit; each pass includes possible active pending resize, list start, every binding resize/disposal/Close. A deletion requests a restart, so <=B deletions, <=B+1 passes, <=B(B+1) visits. Owner.mu spans the whole synchronous removal, preventing concurrent list insertion during it. V requires actual original COWExternalLease/COWBudget implementations for **every binding and active pending lease**; arbitrary lease implementations have no proved bound. Include stack RemovalWork initialization/teardown conservatively via the prior B/Z rows if instrumentation prices it separately. No new allocation is implied by this stack receipt.

Pre-cursor/ready scalar geometry must transform C and B through both native construction and the designated successor, or reject unsupported geometry before Fence. Existing registry closing must remain false until the cursor joins: last-idle closing returns ErrWorkRequired before consumption; no numeric grant makes that path succeed. An actual earlier pin release may use P0 only when state.pins>1 or observers>0 or deleting is revalidated under the original mutex. Last-idle is required in the envelope even if Finish happens nonlast while the designated old reader lives; that reader closes before owning DB.Close.

## Namespace, original custody and strict token

```
NS = (4, 4s(StableNamespaceToken)+256)
Custody = (1, 2s(resourcePinnedBacking)+128)
OSupport = (1, 2s(stablePagerOwnedOperations))
Tclaim = (1, 2s(StableResourceToken))
Tbacking = (1, 2s(resourcePinnedBacking)+64)
TobligationClaim = (1, 2s(resourceAllocation))
TobligationSlot = (1, 2s(StableLogicalObligation))
TobligationEnd = (1, 2s(resourceObligationBacking))
Tfinal = (1, 2s(StableResourceToken))
TokenUnit = Tclaim+OSupport+Tbacking+NS+PinLastIdle(C,B)
          +OwnedPagerUnit+TobligationClaim+TobligationSlot+TobligationEnd+Tfinal
```

Sources namespace_token_release_work_v6.go:34,49,61,73; resource_original_cleanup.go:285-324; resource_token_release_work_v6.go:88,126,207,220,226,239. NS conservatively includes both retained FD closes. Custody completes only refs==1/file==nil/sourceRoot==nil/!registry.closing; it consumes the original edge and returns accounting-only owner/charge. TokenUnit sums alternatives rather than assuming zero-cost phases. Obligation capacity affects total slot count, but persisted obligationIndex makes one slot a next-progress unit. Native actual selected empty obligation backing must be checked. Callbacks/imported source/non-supported providers remain original pre-effect refusals. Parent clears token/namespace/operations aliases before applying Uremove to returned charge. Failing actual closes retain first-error custody; they are not completion.

## Owned pager: exact local rows and selected owner branch

Sources native_stable_pager_capture_v6.go:47-202; native_mvcc_snapshot_close_v6.go:12-211; primary_arena_release_work.go:41-81; namespace_proof_close_work.go:192-207.

```
Oconsume = (1,2s(stablePagerOwnedOperations)+2s(nativeStablePagerCaptureV6))
OclaimSnapshot = (1,2s(Snapshot))
Oend = (1,2s(stablePagerOwnedOperations)+len(syncPath))
ArenaBind = (1,2s(primaryArenaOwnerV5)+2s(primaryArenaReleaseWork)+64)
ProofSupport = (1,2s(StableNamespaceCreationProof)+64)
OwnedPagerUnit = 2Uremove + Oconsume + OclaimSnapshot
               + SnapshotFamily + ArenaBind+ProofSupport
               + Custody+Uremove + Oend
```

The two initial Uremove overcount continueRefund's completion and direct pair. The consumed bit prevents duplicate callback ref drop. Release phases0..5 persist: claim SAME Snapshot; close it; bind/release original primary owner; complete SAME original custody; refund; scrub pager/native/path/metadata before returning final accounting receipt. Actual callback counter decrements only after Snapshot close completes. Completion's returned operations+wrapper charge is refunded only after parent detaches its final operations alias.

**Selected primary owner condition:** original current index/runtime owner remains live while the cursor's captured Snapshot/callback releases; both operation supportsNativeReleaseLockedV6 and Snapshot phase7 revalidate current idx, live wrappers and no physical completion. Native cursor join must precede owning DB.Close withdrawing that DB/index owner. Under this original lifetime the callback owner release and Snapshot primary-owner release are nonlast. Bind still calls ProofSupport before decrement, even for a nonlast edge. Its ContinueWork phase6 is scalar/no further child. An explicit selected ownership/count predicate should establish this rather than infer nonlast from the designated old reader. If index retirement/last primary owner is allowed before join, replace this nonlast row by the real arena.CloseWork+namespace proof/FD/refund continuation; this report does not silently price that larger route.

SnapshotFamily's child primary-owner entry must include **ArenaBind+ProofSupport**, not only releaseNativeMVCCNonlast: actual phase6 calls primaryArenaReleaseWork.Bind/ContinueWork. It also includes actual registry cancel, root Drop/Queue, empty vlog Set, leaf-pin, manager wrapper and Erelease with its existing ancestors. Those actual assigned rows cannot be dropped from OwnedPagerUnit.

## Selected graph: explicit typed frame/provider rows

Sources resource_set_graph_release_work_v6.go; resource_graph_release_work_v6.go; resource_graph_physical_release_work_v6.go. For the selected empty wrappers/manifest and independent token-backed captures:

```
SetClaim = (1,2s(StableResourceSet)+2s(resourceAllocation))
GraphEntry = (1,s(resourceReleaseFrameV6))
GraphMetadata = (1,2s(resourceReleaseFrameV6))
GraphDetach = (1,2s(resourceReleaseFrameV6))
ChildClaim = (1,2s(resourceAllocation))
SetScrub = (1,2s(StableResourceSet))
ManifestSlot = (1,2s(pointer))
ManifestLoadedSlot = (1,2s(dependencyManifestEncodedEntryV1))
ManifestPayload = (1,4096)
BackingClaim = (1,2s(resourcePinnedBacking))
BackingClose = (1,2s(resourcePinnedBacking)+128)
RopeClaim = (1,2s(stableResourceEntryNode)+2s(resourceAllocation))
TokenGraphClaim = (1,2s(StableResourceToken))
FailureAncestor = (1,2s(resourceReleaseFrameV6)+2s(resourceAllocation)
 +2s(Owner)+2s(StableResourceSet)+2s(StableResourceToken)
 +2s(resourcePinnedBacking)+2s(IdentityPinRegistry))
```

A conservative selected graph next-progress superbound is `SetClaim+GraphEntry+2Uremove+GraphMetadata+ChildClaim+GraphDetach + max(SetScrub, ManifestSlot,ManifestLoadedSlot,ManifestPayload, BackingClaim+BackingClose, RopeClaim, TokenGraphClaim+TokenUnit, FailureAncestor, DirectoryReleaseUnit)`; add the actual transaction/seal/capture ancestor prefix from the preceding report. DirectoryReleaseUnit is specifically the writer-assigned original typed directory receipt row, not an opaque generic graph walk. For empty manifest, actual entries/loadedEntries/payload lengths must be zero before using that specialization. For empty selected wrappers, entries/pinHighWater/logicalMembershipEvidence must satisfy releaseSelectedGraph's native guards; kind roots/extra physical directories require their actual original rows. Never assume kindViews nil from equal byte charges.

Failure propagation persists failureCursor one ancestor at a time; original graph current/phase/index/offset/refund persist. Parent child aliases are detached before accounting-only completion. root.running is protected under set.mu, callbacks execute after unlocking set.mu, and original root actor is retained across refusal.

Resource rope refs use a CAS loop with one (1,16) reservation per failed CAS (physical release file:27-36). For the selected ordered schedule, require no simultaneous ref mutations during this final physical release segment (designated successor commits before resumed drain; no additional writer/read capture). Otherwise there is no deterministic bound on retries from cardinality alone. This is separate from the designated reader remaining live before its ordered close. No generalized contended CAS guarantee is claimed.

## Minimum enforcement choice for last-idle removal

Prefer the **whole-removal admission under the existing locks** for this selected diagnostic if its concrete owner envelope is supported. After table lookup, keep registry.mu and acquire the original metadata Owner.mu; inspect/count every original binding and validate concrete COW lease/active-pending identity, admit the complete indivisible removal vector, then perform the original removal and pin bookkeeping without a yield or owner unlock. Admission failure leaves table, pin, state and owner charge unchanged. A bound read before acquiring Owner.mu is not a consumption permit: ordinary enrollment can change the list. Do not call public Owner.Remove recursively while already holding Owner.mu; factor its original locked core, preserving same owner/shrink policy, or use a narrowly typed original-owner critical section. Any callback needed to keep both original locks must have its own source/escape/allocation proof; no unpriced closure is assumed.

The admission census itself must be paid. Reuse the existing original geometry traversal prices (owner_work.go:637-693), with active enrollment refused for this smaller selected envelope:

```
LG=(1,2s(COWExternalLease)+2s(COWBudget)+2s(NativeRetentionLeaseGeometryForTestV6))
Census(B)=(2,2s(NativeOwnerGeometryForTestV6)+2s(Owner)+2s(EnrollmentWork))
         +B*((2,2s(binding)+2s(NativeOwnerGeometryForTestV6))+LG)
PinLastIdleEnforced=P0+PH+C*PC+Census(B)+Rwhole(B)
```

LG is the actual original COW lease observer at native_retention_work.go:78-95. The original geometry helper locks Owner.mu itself, so it cannot be nested recursively; use its source-equivalent priced traversal inside the combined original critical section. An unsupported active enrollment/type must be refused before pin consumption; alternatively explicitly extend census for its real pending lease. The existing selected geometry reports UnsupportedEnrollment rather than proving it away. This is checked work over actual bindings, not an arbitrary scalar precharge.

Complete bookkeeping which still dereferences identityPinState (pins/zero notification) and end the pin/state/table aliases before its metadata charge is physically withdrawn. Keep registry.mu held until this indivisible action returns; it prevents concurrent registry teardown. Preserve ordinary behavior and all unsupported capability/error distinctions. If supported original shrink cannot be guaranteed after pre-admission, retain original pin/custody and use a real continuation instead of pretending the operation completed.

The narrow infallible original shrink proof also requires: Owner is open, not retired/cleanup-failed, `enrolling==0`, no active enrollment, amount<=bytes, and each original binding has rollback0 plus a concrete open COWExternalLease with its original binding-budget identity and payload admission at least current Owner.bytes. Then all requested targets are nonincreasing; cow_budget.go:134-166 has no capacity-refusal path on shrink. Bindings may have different original COW budgets; validate each against **its own b.budget**, not one globally substituted budget. A WorkLease interface assertion by itself proves neither implementation complexity nor infallible shrinking. The implementation must establish the concrete original type/provenance (or a source-audited closed equivalent), and exclude an independent direct Close of that lease between validation and consumption. Existing Owner-controlled lease ownership supplies that exclusion for supported original bindings. Reject unsupported state before the teardown callback. A failed/rollback/under-admitted owner must not be converted into a successful physical release.

Root subsequently selected this whole atomic original-owner route, with a fixed infallible teardown callback (no lock acquisition, allocation, failure or reentry). **That selection is not frozen implementation evidence.** Immutable 5cde43 still contains the original unadmitted nested Owner.Remove. Added helper/callback/price/layout and negative no-effect controls need their own exact successor review; this advice grants no source acceptance.

A preborn pin RemovalWork would preserve bounded per-step owner cleanup but needs a real lifetime proof beyond simply retaining pin.registry. **closeIfIdleLocked at pin_registry.go:855-863 does not test activePins**; it tests state count, originalCleanupEdges, failure lists and namespace counts. Tombstoning the last state then yielding can allow registry metadata Close while pin cleanup still uses it. Keeping a state pointer until refund would also retain the exact state allocation being debited. For the actual token route an existing original callback custody may keep originalCleanupEdges live, but generic pin Release does not establish that premise. A continuation must preserve a genuine existing authority edge or add an explicit same-pin outstanding cleanup obligation consumed by closeIfIdleLocked, with admitted layout/cancel/error/close work. That is a larger representation change than the locked whole-removal seam. Do not add a keeper registry or silently treat activePins as an existing guard.

## Required pure forwards and disposition

Expose actual package sizeof values only: rootpublication resourceReleaseFrameV6/resourceAllocation/resourcePinnedBacking/resourceTokenMetadataReleaseV6/identityPinState/registryCell[StableIdentity,*identityPinState]; DB stablePagerOwnedOperations/nativeStablePagerCaptureV6/primaryArenaOwnerV5/primaryArenaReleaseWork and existing Snapshot/native Snapshot state; retainedalloc binding/EnrollmentWork/RemovalWork and original memtable COWExternalLease/COWBudget. Reuse existing typed owner/queue/enrollment rows. Pure forwards need not instantiate fake owners, arrays or collaborators.

**Advisory disposition: CONDITIONAL FORMULAS; no finaldriver/source/runtime approval.** First concrete unresolved seam is last-idle synchronous registry Owner.Remove work admission/type/cardinality proof. Prove/pre-admit that exact indivisible unit or explicitly repair the original continuation; retain it unavailable otherwise. Directory/empty Set+manager and coordinator rows stay assigned, not waived. No publication, Q32/1MiB fit, Finish total, complete union or numeric allowance follows.

## Inspected immutable bytes
- `TreeDB/db/native_mvcc_snapshot_close_v6.go` `56d2268c196d5624f95f78207cdd657e494c227fa467c4bc0c3f93f419c7d4c1`
- `TreeDB/db/native_stable_pager_capture_v6.go` `a5b9f6023ccb7460005b49ba086a901f436a4b1ce43d51f7c800afc05c9184a6`
- `TreeDB/db/primary_arena_release_work.go` `d3a617c32599768e604e818599d130c0d6ed1abb96b31a56f6a73b7eb87ee302`
- `TreeDB/db/stable_pager_owned_operations.go` `033f88c049979a5c218ec7c24634fae4c72d6b510247da3941937fd87bef3b3b`
- `TreeDB/internal/retainedalloc/owner.go` `aa2725812345880c152932466ee45dfa2d31b69562309595afb98d354e725c3d`
- `TreeDB/internal/retainedalloc/owner_work.go` `d4727141cffdc31205bc99854746a91fe10e19bdc628f643206d9ca94fd8aabe`
- `TreeDB/internal/rootpublication/namespace_proof_close_work.go` `df6f00386a2c3d5523c5a5e99135990b13dc98eaf6e396de54ebc1db5188b1f7`
- `TreeDB/internal/rootpublication/namespace_token_release_work_v6.go` `e1c129ae775189b4489b3e4c5de49a3fd5eb819494a625164257be9bcfbbe2d5`
- `TreeDB/internal/rootpublication/pin_registry.go` `770993e8d91fb63b2f47e9a8b757fe28140cc66520e96be5e5df0ccd06b35653`
- `TreeDB/internal/rootpublication/pin_registry_capture_work_v6.go` `3200ad2c59d83cfbfe037189bd5967028b6167e343f794d39822c7bf995ab09d`
- `TreeDB/internal/rootpublication/registry_capacity.go` `0c35c4c6a0194474ac2134ad8aa1574795f48e0b77ef82e95eb791a52b709b7d`
- `TreeDB/internal/rootpublication/resource_graph_physical_release_work_v6.go` `9460134cb1476fa0f097d069ca6b5a1711da19ddb1976fd24902cc5b2efad79c`
- `TreeDB/internal/rootpublication/resource_graph_release_work_v6.go` `49855cb3646a6cebe9e5869a0f2e3df287d6c17d14e3792357b10d6883440308`
- `TreeDB/internal/rootpublication/resource_original_cleanup.go` `be01b344603485b43b21d89bd37a9acab5edd23a3f392a0dbc513c6722a490fb`
- `TreeDB/internal/rootpublication/resource_set_graph_release_work_v6.go` `d86f5f8b78a3ca41834c19619917ec625c4ff10b92b02f7ccd51050af9c6b1ff`
- `TreeDB/internal/rootpublication/resource_token_release_work_v6.go` `a9e659cfcea57b775c81920ea8fd8bff36f6554dab88ce8405555593601a1c70`
- `TreeDB/internal/memtable/cow_budget.go` `f3102bd2d83c3009ff5cf470634ce9046ae35d95a59f2a106f9e147a8615a823`
- `TreeDB/internal/memtable/native_retention_work.go` `0863e195a9720e1490ddb1e0c6599533510de1523fb3f09a4ca1d7debb541fba`
