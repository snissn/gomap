# Selected terminal progress bounds

Read-only immutable-source advice, gpt-6.1-sol/high, no children/source/Git/Go/runtime operations. Input manifest **5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d**, archive **0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4**, staged tree **e523999e97e0af545da0072359cd1ac2fc57b5eb**. Exact inspected-source hashes are in JSON. Preparation remains writer-owned. No mutable worktree was read.

The shared Owner, arena, published-private, capture and projection formulas below are concrete source-derived bounds. The final composed G remains conditional on the explicitly named typed child rows at the end; no complete numeric G, general Finish total, arbitrary allowance, native publication acceptance or Q32 fit is asserted.

## Notation and supported schedule

Vectors are `(records,bytes)`. `+`, multiplication and max are checked componentwise operations; `s(T)` means actual target-compiler `unsafe.Sizeof(T)`. A progress unit includes every repeated ancestor reservation before the next persisted phase/index/cursor/ownership update. It is insufficient that each individual Reserve fit separately.

Carry **L=Npre+6**, including terminal-null lookup, from the accepted SAME-pager bootstrap proof. Carry allocator K/S/M/capacity bounds from b151 and retirement count `R=D+I`. Do not substitute observed metadata3, historical F or retained-byte charges.

The designated independently held old reader remains through the required publication/successor assertions and closes in the selected order before owning DB.Close. Both nonlast and last root branches are covered. Exclude only additional unmodeled readers/writers/enrollments. During ordered leaf-pin teardown no competing reference mutation may indefinitely defeat CAS. The native private Snapshot has no active read/iterator/rebind operation waiting on this same thread.

## Owner, removal, enrollment and lease

Sources: retainedalloc/owner_work.go:68-199,250-288,333-427,464-633; owner_enroll_work.go:294-410,572-638; internal/memtable/native_retention_work.go:34-35,68-72.

```
V = (1,2s(COWExternalLease)+2s(COWBudget))
B = (1,2s(RemovalWork)+128)
I = (1,2s(Owner)+2s(RemovalWork)+128)
H = (1,2s(Owner)+2s(EnrollmentWork))
J = (1,2s(Owner)+128)
Kbinding = (1,2s(binding)+2s(Owner)+128)
Dbinding = (3,6s(binding)+128)
Z = (1,2s(RemovalWork))
Uremove = B+I+H+V+J+Kbinding+V+Dbinding+V+Z
```

V covers each concrete original Resize or admitted Close. Uremove deliberately includes optional active-enrollment resize, current binding resize, disposal and last binding Close together, plus bind/debit/reset. Amount withdrawal occurs once; active pending-admitted update, binding phase/cursor and removal.started persist. CompletePendingWork resets its own RemovalWork. No additional helper reset is needed.

Binding count b affects total finite turns, not this next-progress bound. With no new binding, a restart follows deletion; at most b restarts and b(b+1) visits conservatively bound the whole list. Count cleanup-held and zero-ref bindings too. The selected original owners/leases must support WorkLease; larger G cannot repair unsupported capability.

```
Ebind = 2*(1,2s(EnrollmentReleaseWork)+2s(Enrollment)+128)
Eedge = (1,2s(Enrollment)+2s(binding)+2s(RemovalWork)+128)+Uremove
Erelease = Ebind+2*Eedge
Ecancel = (1,2s(EnrollmentWork))
        +(1,2s(EnrollmentWork)+2s(Owner)+4s(binding))+Uremove
        +(1,2s(EnrollmentWork)+2s(Owner)+2s(binding))+V+Uremove
        +(1,2s(EnrollmentWork)+2s(Owner))+V
PairCancel = (1,2s(EnrollmentPairWork))+2*(Ecancel+Erelease)
```

This overcounts the two primary/companion alternatives but bounds actual partial cancellation, detached binding refund and pending lease Close. It adds no owner.

Pure terminal Remove has no Add. If a reachable graph child constructs directory-release scratch, include that real constructor plus atomic Add: `(1,2s(Owner)+2s(EnrollmentWork)) + active*((2,4s(EnrollmentWork))+2V) + sum_bindings((3,6s(binding))+2V)`. Each pair of V is the original forward/rollback admission. Do not silently omit this exception or enroll unrelated canonical storage.

## Arena queue, root lookup and source witness

Sources primaryarena/arena.go:150-166,766-1032; pager/primary_read_root_v6.go; db/native_mvcc_root_preparation_v6.go:222-270.

```
T(L) = (1,s(primaryReadRootTableV6))
     + L*(1,s(pointer)+s(primaryReadRootEntryV6))
Dtable(L) = (1,128)+L*(1,s(pointer)+2s(primaryReadRootEntryV6))
Drop = (2,2s(bank)+2s(ReleaseQueue))+T(L)
Queue = (10,18s(bank)+2s(Group)+2s(ReleaseQueue))
      +9*T(L)+Dtable(L)+Uremove
Witness = (1,2s(nativeMVCCRootPreparationV6))
        +(1,2s(primaryarena.Ref)+2s(phase))+Drop
        +Queue+(1,2s(nativeMVCCRootPreparationV6))
```

releaseCells is exactly8. Nine lookups overbound the current bank plus eight outgoing edges. Groups need no more. File-bank paths cost less. Queue edge/component indices persist; RAM root removal transfers its charge into the SAME queue accounting receipt before bank aliases end. Native node removal and bank metadata use the same Owner. Source witness Drop remains after genuine receipt Finish.

Queue is a next-progress bound, not a total-bank count. It includes all branch alternatives conservatively; no entire root graph traversal must fit one invocation.

## Finish/callback/terminal ancestors

Sources native_primary_finish_work_v6.go:20-154; primary_transaction_construction_v6.go:435-553; primary_member_retirement_work_v6.go; primary_seal_release_work_v6.go; primary_runtime_disposal_work_v6.go.

```
Htxn=(1,2s(DurableRootTransaction)+2s(primaryTransactionConstructionV6))
Hmember=(1,s(rootPublicationVisibleMemberV1))
HprimarySeal=(3,2s(primaryPublicationSealV5)+2s(disposalFailure)+2s(Owner))
HrootSeal=(3,2s(rootPublicationSealV1)+2s(disposalFailure)+2s(Owner))
Hruntime=(3,2s(primaryDurableRuntimeV5)+2s(disposalFailure)+2s(Owner))
BindFinish=(1,2s(PrimaryConstructionReleaseWork)+2s(DurableRootTransaction))
Detach7=(1,2s(nativeMVCCCandidatePreparationV6)
           +2s(nativeMVCCRootPreparationV6)+2s(sealPointer))
```

Lift every child unit by all its actual repeated ancestors, e.g cursorEntry+DeleteEntry+Htxn+Hmember+HrootSeal+HprimarySeal+child. It is safe to sum all four callback headers on every callback path. Add actual completed-sibling completion checks (Uremove bounds each) and descriptor reservations before child calls. A completed physical flag is not reset when the next child refuses.

Important remaining local units, directly evaluable in package db:

* Post-state transfer: `(3,s(install.post)+s(DBState)+s(primaryStateRootV5)+s(retiredPostRoot)+s(retiredPostSet)+s(retiredPostManager))`.
* Queue init `(1,2s(ReleaseQueue))`; Ref prefix `(1,2s(Ref))+Drop`; queue flag clear `(1,2s(bool))`.
* Retirement seal dispatch `(1,2s(sealPointer))`; final indivisible vector clear `(1,2*cap(retiredSeals)*s(sealPointer))`.
* Token/Set/manifest alias detach and actual typed graph child units remain real even when logical resource count is0.

The native member's retiredSeals is **independent** preborn commitRetired backing: durable_primary_runtime_v5.go:767 checks its capacity;857-864 transfers that exact array;939-953 fills it and moves it to the member. A later successor cannot grow this private array. Bind its constructor capacity through the native seal creation before Fence; a ready-only observation is not its pre-cursor bound. The later successor's own retirement array/runtime vector is separate and needs its own route bound if included. Never reuse this private capacity as a bound on shared runtime.seals after successor growth or optimistic attempts.

Finish phase3 binds the terminal accounting receipt, detaches the original transaction and makes the mandatory false return before refund. Subsequent phase1/2 uses Uremove. Genuine physical errors preserve first-error custody; an error-state Close is not successful physical teardown.

## Published private allocator and output/input

Sources freelist/allocator_preparation_work_v6.go:641-756; private_control_capacity_v6.go:402-441; generation_v1_reservation_inventory_work.go:338-355; db/native_mvcc_allocator_preparation_v6.go:269-281.

```
Hpub=(1,2s(COWPreparationWorkV6)+2s(PreparedCOWCandidateV1))
Norm=(1,s(reservationNormalizeWorkV1))+Uremove+Z
HashClose=2*(1,2s(privateUint64SetV6))+2*Uremove
PublishedPrivate=Hpub+2*Norm+(1,2s(COWPreparationWorkV6))
                +4*HashClose+Uremove+(1,2s(COWPreparationWorkV6))
                +(1,2s(nativeMVCCAllocatorPreparationV6)+8*(D+I))
```

This is a whole finite-stage superbound. The two normalizers and four hashes are fixed. HashClose includes old debt before detach and new debt afterward. No cap-times-slot clearing exists at private table Close. Clone aliases end before refund. Add actual outer prefix and containing completion.

Output Complete: each frame `(1,s(materializedWriteFrame))`, final `(1,s(MaterializedWriteWork))`. Input Close: decoder `(1,s(node.ProjectionDecoder))`, each component `(1,len(Key)+len(Value)+2s(batch.Entry))`, each entry `(1,2s(batch.Entry))`, final `(1,2s(MaterializedInputWorkV6))`. Heads/indices persist, so use maximum payload unit or sum the finite original payloads for a whole-stage bound.

Actualready cancellation first closes the candidate before cancelling installed AppendWork pages. append_work.go:144-167 invokes the true persistent appendRetirementWorkV6 trie engine. The stale current_delete:449 comment is not proof of unsupported cancellation. Its fixed-depth14 mutation uses the preparation mutation upper bound (writer-owned), one per installed output frame. Uninstalled growth uses real growth.Cancel. Selected ordered cancellation excludes concurrent allocator generation changes; otherwise repeated restart is not bounded by a finite grant.

## Capture/Snapshot/projection/outer Close

Sources db/native_mvcc_snapshot_close_v6.go; leaf_generation_pin_work.go; caching/native_mvcc_prune_v6.go:302-347; lifecycle/native_mvcc_registry.go:98-112; tree/charged_projection.go:637-712; mvcc/prune_cursor_v6.go:756-850.

```
CursorEntry=(1,2s(closePhase)+2s(closed)+2s(preparationPointer))
DeleteEntry=(1,s(publicationCoordinator)+s(publicationBound))+CoordinatorRecoveryView
SnapStart=(2,2s(Snapshot)+64)
SnapLoop=(1,2s(nativeSnapshotState)+64)
RegistryCancel=(1,2s(NativeMVCCRegistration)+8)
PinStart=(1,2s(leafGenerationPinSet)+2s(leafGenerationPinReleaseWork)+64)
PinIter=(1,2s(leafGenerationPinSet)+s(leafGenerationPinReleaseWork))
       +(2,2s(leafGenerationPinRef)+16)
PinEnd=(1,2s(leafGenerationPinSet)+s(leafGenerationPinReleaseWork))
SnapDetach=(2,4s(Snapshot)+64)
SnapFinal=(1,2s(Snapshot)+64)
SnapshotFamily=SnapStart+SnapLoop+
 max(RegistryCancel,Drop,Queue,EmptyVlogSetRelease,
     PinStart+PinIter+PinEnd,PrimaryNonlastRelease,
     ManagerWrapperRelease,Erelease,SnapDetach,SnapFinal)
Capture=(1,2s(NativeMVCCCaptureV6))+PairCancel+SnapshotFamily+V
```

Every Snapshot child unit also needs Capture/Cursor ancestors. The held designated reader is allowed. Leaf pin CAS needs no concurrent mutator during the ordered final segment. Manager/primary-owner native support and current idx are rechecked, not inferred from an old snapshot. Empty resource dependency entries alone do not establish empty vlog files; require actual FileCount0 or include genuine physical File callbacks.

```
Projection=(1,32)+maxTraversalDepth*(1,s(CursorItem))
 +2*(1,s(node.ProjectionDecoder))+2*(1,s(node.ProjectionComparison))
 +(1,2s(node.LeafEntry)+s(node.PrimaryDirectoryEntry)
      +s(node.PrimaryDirectoryView)+s(node.Node)+256)
 +ReaderCancel+(1,s(tree.Tree)+96)
ReaderCancel=(1,s(NativeMVCCFrameReaderV6))+ExternalLeafReaderCancel
```

Decoder/comparison Cancel is scalar sizeof. ExternalLeafReaderCancel=0 needs actual external=false. Otherwise COWLeaf cancel contains `(1,s(COWTemplateContinuation))+(1,s(node.ProjectionDecoder))+(1,s(COWLeafContinuation))` plus actual ZSTD cancel. With decoder=nil and hasher=nil, ZSTD cancel is `(1,1)+(1,s(COWZSTDContinuation))`; otherwise original bounded reader CloseStep is required. A nonnil codec holder does not itself establish active decoder, and outerLeavesfalse must be traced or guarded rather than assumed to suppress it.

Outer Close adds prepared pointer `(1,2s(prepared))`, record/reader `(1,2s(record)+2s(reader))`, output frame `(1,2s(outputReader)+2s(outputLeaf))`, each batch slot `(1,s(node.LeafEntry))`, and the exact final field scrub at prune_cursor:838. BatchSize1 bounds that loop. CursorEntry repeats at every invocation.

Cancel parent detail: nativeMVCCSealPreparation.closeStep retries both indexCaptures from0, so include completed capture0's entry prefix before capture1's next progress; fixed2, not unbounded. Candidate close repeats backingRefund/resourceCompletion before its `(1,2s(candidatePrep))` header. Add the original child cleanup and completion checks on that actual path.

## Exact remaining typed rows and limits

Minimal private size forwards: retainedalloc binding/EnrollmentWork/EnrollmentReleaseWork/EnrollmentPairWork/RemovalWork; primaryarena bank/Group/ReleaseQueue; pager primaryReadRootTableV6/primaryReadRootEntryV6; rootpublication primaryTransactionConstructionV6/primaryTerminalCompletionV6/resourceReleaseFrameV6/resourceAllocation/resourceTokenMetadataReleaseV6/resourcePinnedBacking plus public receipt/Set/token types; valuelog nativeMVCCSetReleaseState/ManagerWrapperReleaseWork/Manager/Set/COWLeafProjectionReader and public leaf/template/ZSTD continuation types; tree CursorItem/maxTraversalDepth. DB-local containing/leafPin/seal types use direct sizeof. No fake constructor/owner is needed to obtain sizes.

The resource graph's actual selected Set/token/manifest, owned pager-operation and identity-pin next-progress child vector, coordinator recovery/completion wrapper vector, empty vlog Set/manager-wrapper vector, and active ZSTD reader CloseStep were **not fully reduced to a closed expression in this bounded pass**. Their real source was inspected where listed; naming a child is not proof its cost is0. These are fixed typed row forwards in the same coherent packet, not a request for another owner/registry/graph traversal or a general price framework. Installed-page Cancel carries the writer's original trie mutation bound. Complete U_cancel/U_close cannot be asserted until these rows are included with the ancestor sums above.

No source defect is inferred merely because these expressions are unevaluated. The next frozen review must check the writer's added caller/layout/price deltas, real capabilities and evaluated rows. Ordinary DB teardown outside the native cursor has its original synchronous policy and process deadline; no finite repo work vector bounds kernel time. No numeric allowance or runtime grant is made here.
