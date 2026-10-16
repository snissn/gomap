# Frozen15f0 private capacity and constructor-ledger account

This describes source manifest15f0548d03b03923cc8785816b21ee957df3a4f114a2578cba0ef3bbbdd142ec, not an executed workload or numeric allowance. The original Q32/1MiB/33/80/4096 guards remain RED/ACK0. Only the eight-package zero-body compile ran on this source. Full source and30-path delta are recoverable in this directory. Earlier compile-source01/ad721 is preserved as a separate historical compile pass.

## Actual newly governed allocations

Let Q be retainedalloc.AllocationCharge. Every independent backing is rounded independently. Same owner means the existing bound PRIMARY metadata Owner passed into COWPreparationWorkV6.BindRetainedOwnerV6; no owner, lease, registry or framework was introduced.

| Allocation | Exact retained admission | Last actual aliases and disposal |
|---|---|---|
| Captured FreelistTxn heap header | Q(sizeof(FreelistTxn)) | s.private; privateTxnView; prune/retire/materialize/planner/inventory borrowers. Temporary allocator installation ends when the SAME activation installs its distinct successor. Detach all original private borrowers before original privateRemoval. |
| Captured allocated vector | Q(cap(private.allocated)*sizeof(allocatedPage)) | Private txn and inline planned header/inventory borrowers. Fixed/read-only after exact capture. No canonical live-vector refund. |
| Captured abandoned vector | Q(cap(private.abandonedAppends)*sizeof(ReservationExtentV1)) | Same private borrow graph. Partial capture owns every allocation already made, including before subsequent allocation refusal/stale source. |
| Captured changed chunks set | Q(cap(slots)*8), Q(cap(next)*8), Q(cap(snapshot)*8) independently | Embedded changedPrivate; original markers/typed view. Current/new/capture arrays overlap and all remain admitted. |
| Captured replaced metadata set | Same three independent array terms | Embedded replacedPrivate and private inventory/output borrowers. |
| Independent planning replacement set | Same terms, though no map-key snapshot is needed for the typed clone | Embedded materialize.reuse.plannedReplaced. Its backing is independent of the captured replacement table; other planned txn fields remain borrowed. |
| Private candidate sink validation set | Same terms | Embedded sinkPrivate; recordingSink borrows its pointer. CandidatePageSink.pageIDs stays nil in selected private mode. Canonical recorded pages are independent original outputs. |
| Two normalization scratch vectors | Original normalize.scratchCharge per actual second vector | Existing normalizers release scratch using their original RemovalWork before their containing aliases reset. The original inventory/output vector remains in its existing canonical domain. |

The hash headers, private view, continuation positions, RemovalWork receipts and planner header are embedded in already governed preparation/cursor objects; they are not independent heap-header rows. Enlarged containing admissions use actual unsafe.Sizeof and therefore grow naturally. Arrays are admitted by original AddPendingWithOrdinalWork before make in the same invocation. No unused admission token or map iterator survives a yield. A detached refund-only debt has no surviving array alias; it is reported separately from physical current/new/capture backing. Owner.Bytes may already have been debited while original RemovalWork still retains completion state, so a pending scalar amount is not an exact copy of current Owner.Bytes.

Source files are private_control_capacity_v6.go, allocator_preparation_work_v6.go and the original generation_v1_*_work.go parents. Canonical live/successor maps, shared live allocated DATA, original state tree/generation/page output, ledger reservations and published DBState/snapshotView outputs retain their existing contracts. This packet neither re-enrolls that transitive DATA graph nor refunds those surviving canonical outputs at private close.

## Actual work units and temporary capacity

All following are source Reserve operands, with underlying original AddPending/Removal work additionally required and consumed on the SAME work object. They are not precharged synchronous callbacks. `H=sizeof(privateUint64SetV6)`, `C=sizeof(COWPreparationWorkV6)`, `T=sizeof(FreelistTxn)`. Work refusal remains false,nil; unsupported work-bearing lease remains the original error. Bounds use checked raw/rounded multiplication and addition.

| Operation | Records/bytes debited before its effect |
|---|---|
| insert entry/header validation | 1 / 2H on each call |
| phase0 key/version/zero dispatch | 1 / 2H |
| hash start / rehash hash / final new-table key hash | 1 / 64 each |
| current table lookup cell | 1 / 32 per probe, at most actual capacity |
| new table allocation+zero | 1 / (8*Cnew+2H), then SAME AddPending(Q(8*Cnew),w) before make |
| rehash source cell | 1 / 32 per visited old cell, including empty cells |
| rehash destination probe+possible write | 1 / 40 per actual destination probe |
| no-growth insertion commit | 2 / 64 |
| growth-key destination probe | 1 / 32 per probe |
| growth insertion+swap+old-alias detach | 2 / (2H+8); old backing charge transfers to scalar original removal debt |
| successful operation terminal field reset | 1 / 2H after actual debt completion |
| locked canonical map snapshot, n entries | 1+n / (2H+32n), then SAME AddPending(Q(8n),w), then one exact vector allocation and complete locked copy; no resumable Go map iterator |
| snapshot build entry | 1 / 2H per invocation, 1 / 32 per loaded key, plus actual insert state machine, then 1 / 2H detach + original snapshot refund |
| independent planner clone | 1 / 2H per call plus 1 / 2H binding, 1 / 32 zero bit, 1 / 32 per source cell, actual inserts, 1 / 2H final source detach; source key/version/capacity remain checked |
| captured private header | 1 / (3T+2C), then SAME AddPending(Q(T),w), allocation/copy and original source binding |
| captured allocated/abandoned vector allocation | each 1 / (raw+48), then SAME AddPending(Q(raw),w), then exact make; subsequent copies use existing stateEncodeEntriesV1 admission per copied element |
| private geometry read | 1 / (2H+2sizeof(returned scalar geometry)) per control |
| private cleanup global alias detach | 1 / 2C before clearing borrowers, before any table or clone refund |
| each set close | 1 / 2H per invocation, original prior debt finish; if live state exists, 1 / 2H before all arrays/clone/source aliases detach, then original combined same-owner debt removal |
| private clone final removal | actual original privateRemoval.CompletePendingWork after all private header/vector borrowers detach |
| containing final reset | 1 / 2C after every normalizer/set/clone removal completes |

Power-of-two capacity starts16 and doubles. Nonzero occupancy stays at most half. Zero is an embedded presence bit; max uint64 is a valid nonzero key. A probe is bounded by capacity and detects a corrupt full table. Growth is not worst-case constant time: every real old cell, destination probe and final key probe is separately consumed and resumes at saved positions. Expected hash performance is a rationale, not a measurement or hard collision bound.

The whole locked key snapshot and each array allocation remain indivisible original units. Oversized source cardinality, array extent, active enrollment fan-out or unsupported WorkLease can refuse indefinitely under Q32/1MiB; this is a disclosed progress limit. No caps were raised and no segmentation was introduced. Old+new+capture overlap must fit the original governing owner/lease limits; eventual new capacity fitting alone does not permit overlap.

## Original cleanup and transfer order

Cancel keeps original rollbackPreVisibleWithWorkV6 first; adopted candidates refuse Cancel. Published close validates the exact original published+activated candidate and detached installed-page producer under original allocator lock, then uses the same finite private cleanup. Both parents release the two normalization scratch receipts first, detach every typed view, marker, recorded sink, planner, private txn and vector alias, then close four sets and finish clone scalar removal. No active RemovalWork is copied/reset, and no new runtime-owned continuation can be assisted by an ordinary caller. Parent db.nativeMVCCAllocatorPreparationV6.cancel/closePublished remain the original callers. Original canonical candidate pages, generation/root and distinct successor survive private published cleanup. The new positive published-cleanup test is allocator-subsystem-only and is written but unexecuted; it does not prove real DB publication/Finish.

## Whole-operation price applicability

This packet changes prebuilder private representation and its cleanup; it does not add a new final-driver/ready-bit transition. The original final driver, enqueue/start, authority/storage/Consume, coordinator completion and physical receipt semantics are unchanged. The fixed record expression remains source-derived by the unchanged DB seven F rows, original coordinator rows/dispatch and allocator prefix/page rows:

`F_records = 293 + D + 2K + 23P + 8M + 3S + N + Z + V + A`.

This is a symbolic expression, not the earlier observed367-record total and not a grant. New private insertion/snapshot/rehash work occurs before this final operation and is independently charged as above. The typed private count/source guard is fixed header/count work inside the existing admitted allocator validation; no final-turn hash traversal is introduced. The observer's actual allocator preparation header traffic and original containing admission operands use enlarged actual unsafe.Sizeof. Byte prices are the checked sums emitted by MaintenanceNativeFRowForTestV6, NativePublicationObservationPriceForTestV6, MaintenanceNativeRemainderForTestV6, native frame validation and the original coordinator price helpers on this source. Historical871273 bytes, R145 and any earlier concrete retained total are NOT reused. No current actual33 geometry or numeric byte total has run on15f0.

The whole invocation is the actual sum of repeated finite prebuilder steps (including their repeated headers/refusal work), this conditional F, and the original stateful Finish/retirement steps including the new private cleanup sequence. A complete numerical invocation/Finish bound still needs genuine current geometry and the future reviewed caller/ready boundary; it is not inferred from one fixed price or a synthetic unit. Post-newer-writer retirement geometry remains conditional. C1/C3/nonempty resources/append/burned-tail/sustained-memory qualification remain open.

## Selected P ledger and known follow-up

The checked evaluator in mvcc/native_selected_ledger_test.go emits P01-P21 as a specifically named *selected-empty constructor admission envelope*, and optional P_named_roots includes held source=parent and distinct target bank envelopes. H02/H03 remain separate local shared-service observations. There is no Owner.Bytes/Controls sum, lease payload sum, exact allocator/RSS count, atomic multi-lock snapshot or transitive heap claim.

P01 cursor retained aggregate includes batch payload and embedded larger preparation/controls. P02 concrete outer/capture lease headers only. P03 application Snapshot wrapper with no overflow backing. P04 original two enrollment/binding/concrete-binding-lease headers. P05 input aggregate. P06 output aggregate already contains linked frames. P07 allocator retired vector. P08 independently rounded clone header/two vectors. P09 four distinct table controls' current/new/capture backing. P10 two normalizer scratch charges. P11 original completed transaction aggregate including transferred member/install/callback/promotion allowances (slack is preserved). P12 seal headers/prefix PLUS the independent original empty manifest aggregate once. P13 commit runtime+commit-retired vector. P14 Set wrapper charges, P15 kind backing, P16 two index wrapper/operations/custody aggregates, P17 token metadata, P18 separate empty obligation backing, P19 physical duplicate backing, P20 namespace aggregate, P21 canonical selected pin-state header. Two-token equality is actual allocation-pointer equality evaluated locally, never physical identity or equal charge. IdentityPin header remains in P17. Source/parent bank equality suppresses one H01 duplicate. Token pin Generation0 is compared to physicalStableIdentity(full token identity), while full token identity is still independently validated.

Each original accessor preadmits its fixed read and variable visit before inspecting the protected object, returns scalar copies only, and preserves existing lock order. The evaluator uses fresh unchanged Q32/1MiB observer work per fixed role/checked arithmetic group; it does not replace or enlarge the cursor's original work/limits or create an owner. It verifies completed construction phases, exact expected owners/budget and actual transfer/alias facts. Missing/unknown/unsupported/pending overlap/refund/extra map/root overflow/ancillary graph/zero-channel cases stay incomplete. Checked arithmetic preserves prior subtotal on overflow and marks incomplete. P12 observes seal.manifest after real binder transfer, requires phase13 and preparation.manifest==nil; absent prep alias is not a zero manifest row. Its independent constructor aggregate Q(DependencyManifestV1)+Q(resourceAllocation) is not hidden in seal metadata.

**Root follow-up on this exact frozen source:** evaluator currently requires MemberSealSameSet. The actual seal uses CloneAdmitted(member.resources), producing a distinct admitted wrapper. Thus the current selected P14 predicate is not satisfied by the real route. In addition, proofResources transfers to seal.primary.proofResources while the existing role3 reads the cleared preparation pointer. Root/independent review is tracing the coherent actual-holder/allocation correction, including wrapper/kind multiplicity. The frozen evaluator must NOT be represented as a complete ledger. Its current P subtotal is a diagnostic incomplete subtotal; source guard failure cannot justify dropping the actual holder. No correction is silently applied to15f0 and no runtime is authorized.

**Test-only follow-up on this exact source:** SnapshotIndependentPlanAndPeak's intermediate snapshot-build assertion still requires Owner.Bytes equal live charges+scalar debt. Original removeUnserialized may debit the owner before receipt ResetCompletedWork can finish, so that equality can be too strong on a partial refund. EveryTurn already checks the truthful liveBacking <= Bytes <= liveBacking+debt interval. This is a source-supported fixture concern, not a runtime failure or permission to weaken retained lifetime accounting. Root will select its disposition before bodies run.
