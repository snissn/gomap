# Original public cursor progress composition

Read-only advice by explicitly routed gpt-6.1-sol/high. No child agents, source/Git/GitHub mutations, Go execution or runtime collection. Immutable input manifest **5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d**, archive **0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4**, tree e523999e97e0af545da0072359cd1ac2fc57b5eb. Exact inspected file hashes are in the companion JSON. This is a conditional symbolic composition, not source acceptance of the writer's mutable successor or a numeric allowance.

## Conventions and domain

Vectors are (records,bytes), addition/max/size arithmetic checked componentwise. s(T) is actual target compiler unsafe.Sizeof(T), not a guessed architecture size. B=1 for this selected fixture; K is the maximum physical key length reached by its real scan, k/l/v/a/r denote current physical/logical/value/anchor/resume lengths. K<=mvcckey.MaxEncodedKeySize=65535 follows the codec; the actual original fixture may establish a smaller bound. Inline ordinary values and the floor are required, outer leaves off, no value-log pointer or codec-frame path. Preserve the designated idle old reader; no extra writer before ready. Nidx includes every actual db.idxAll map entry; the existing exclusion and producer schedule must bound it, not the number of MVCC timestamps.

The accepted projection, caching capture, PRIMARY construction, stable capture, allocator, zipper and terminal reports are named children Uprojection/Ucapture/Uconstructor/Uallocator/Uoutput/Uterminal. Their own repeated prefixes remain included. Never add another fictitious generic Step row: PruneCursor.step has none. A bound for a child reached after completed siblings is their completed-entry costs plus that child's unit, not its unit alone. Persisted phase/index/child progress lets separate units run on later fresh calls; whole key operations and parent preludes cannot be split this way.

## Birth before capture

mvcc/prune_cursor_v6.go:81-152 creates a fresh work object for each public call. Existing cursors require exact same Store, mode, B and both frozen work limits. Zero limits are refused; default B=256 is irrelevant to an explicit B1 fixture. Public options/identity checks, locks, stats/defer and pointer assignments have no literal Reserve; they are fixed caller/control traffic, not an unbounded loop.

Define Q=(1,2s(COWBudget)+s(COWExternalLease)) and V=(1,2s(COWExternalLease)+2s(COWBudget)). Birth is

    BudgetChild + (1,s(PruneCursor)) + Q

where BudgetChild includes TreeDB/native_mvcc_prune.go's (1,64), followed by the caching budget observation/lazy construction from e4c33057. The cursor allocation includes its original inline buffers and embedded preparation/capture/reader controls; do not independently count those containing fields as additional allocations. The acquired lease and cursor are created together after admission. The returned stats retains c before c.step; a later refusal does not authorize recreating it. A lazily created cache budget may survive a refused outer cursor birth in its original cache domain.

Before capture, first c.step additionally performs

    BatchBirth=(1,B*s(node.LeafEntry))+V

before one admitted make([]LeafEntry,0,B). This is a single next-progress compound: the slice is not saved until ResizeWork succeeds. Size/charge overflow and source ByteLimit checks precede allocation. Phase0 then invokes the original public StepNativeMVCCOwnedCaptureV6 (public (1,64) plus Ucapture). captureBegun is saved before the call, so partial snapshot cancellation remains reachable. The fixed cursor birth and initial batch birth may share the first public call, but each successful storage/phase transition is retained. A conservative G_birth may sum all of these; no claim that every child must complete in one call follows.

## Scan/group/selection parent rows

The following table gives literal outer rows and children. The projection report supplies Begin/Step/Record internals. A row which contains multiple children requires completed siblings on every retry until the indicated outer phase is stored.

| Phase | Prefix and child | Stored progress / replay |
|---|---|---|
|1|one-time (1,64)+V projection admission; Snapshot.NewNativeMVCCProjectionV6 adds (1,s(Snapshot))+projection constructor|admission flag persists before ctor; a remaining-work precheck is not an additional Reserve. Certificate accessor is a fixed tuple borrow under beginRead, not a RAM root-table lookup.|
|2| (1,64)+Projection.Begin(SeekGE floor)|phase3 only on ready|
|3|Projection.Step|phase4 on completed step|
|4|(1,256)+Projection.Record|record/floor equality retained; floor is exact 9-byte inline envelope; pointer branch excluded|
|19|(1,len(floor value)+128)+RequestCapture|phase5 only after request capture|
|5|(1,64)+Projection.Begin(SeekGE namespace)|phase6|
|6|Projection.Step|phase7|
|7|(1,256)+Projection.Record; valid namespace record then (1,3k)+DecodeAppend|record/logical/timestamp persisted before phase10; exhausted namespace routes flush/anchor/complete|
|10|(1,2l+v+3k+a+128)|envelope, group equality, anchor/resume/selected copies; each outcome sets later phase or stores the new group. No group-wide restart scan.|
|11|(1,64)+Projection.Begin(Next)|phase6|
|12|(1,selectedSize+64)+Projection.Begin(SeekGE selected)|phase13|
|13|Projection.Step|phase14|
|14|Wselection+(1,selectedSize+64); optional one-time Buffer below; BindCurrentDelete|selection witness/equality repeat on Bind refusal; selectionBuffered suppresses duplicate allocation|
|15|StepCurrentDelete+BatchBind|both must finish before phase16; first completed child is called again if BatchBind refuses. A genuine nil return follows phase16 assignment.|
|16|fixed BindOutputReader pointer assignment + Uoutput|phase24|
|24|root-preparation child|phase25|
|25|allocator child, including oldest-reader row below|phase26|
|26|CandidateDoneOrStep+SealDoneOrStep+BindPublication+Fence|all completed siblings replay before Fence until next phase is set|
|27|CloseCurrentDelete (genuine published Finish)|phase22 only after true/err=nil|
|22|fixed stats ACK of exact batch; phase20, public return|ACK is after genuine Finish, never merely Fence/preparation|
|20|(1,2s(prepared)+128)+optional completed CloseCurrentDelete; each batch clear (1,s(LeafEntry)); final (1,64), payload refund and reset|batchClear persists; final reset after every entry scrub; original prepared pointer is not cleared before child completion|
|21|(1,resumeSize+64)+Projection.Begin(SeekGE resume)|phase6|

RequestCapture=RequestValidate=(1,s(Request)+s(Capability)+s(Authority)); CaptureRequest/Validate take the SAME authority lock and reject closed/foreign epochs. Wselection=(1,2s(tree.NativeMVCCSelectionV6)+k). It hashes the stationary actual projection key, copies the original physical selection tuple and retains no new root. Match=(1,2s(Tree)+2s(LeafEntry)+2k), preceded by Snapshot validation (1,128). Both key equality and SHA256 are included in those source byte expressions. Advancing/closing/rebinding the projection invalidates the witness.

Buffer=(1,2*(k+v)+s(LeafEntry))+V, followed by the original separately admitted key/value clones and one append within B-sized capacity. Pointer values are not cloned here. checked retained-charge arithmetic must use the constructor limits; no unlimited B or output envelope is inferred from actual33. DecodeAppend inspects and decodes the same k-byte bounded key into the cursor's fixed inline logical array. Its two scans plus output copy are covered by 3k; malformed/oversized keys fail rather than allocate a bigger logical buffer. `validateRecord` is a fixed header/tombstone-length check; it does not scan all value bytes.

Group semantics: previous logical key, haveAnchor, anchorTombstone, selected/resume keys and flags live on this SAME cursor. Above-floor versions are retained; the first at/below-floor version is the anchor, later ones are selected. Tombstone-anchor decisions can defer the anchor until buffered prior versions flush. BatchSize1 is not a bound on visited physical records, comparisons or logical groups: the independently finite physical projection cardinality supplies a total-turn bound. Each Next, projection step and saved group decision is persistent progress, so do not multiply the whole namespace into one indivisible U_scan.

## Selected constructor/callback and oldest observation

The actual immutable names are BindNativeMVCCCurrentDeleteV6 / StepNativeMVCCCurrentDeleteV6, not a separate method literally named PrepareNativeCurrentDelete. Bind adds (1,2s(NativeMVCCCurrentDeleteV6)+s(Selection)). Step phase1 repeats SnapshotMatch+(RequestValidate) until both succeed. Step phase2 adds (2,2s(currentDelete)+128), then the original writeMu/durablePublishMu TryLock and original constructor child. A nonnil partial transaction is saved even when constructor ready=false, preventing duplicate construction/refund. phase3 calls promotion storage; completed phase3 is still a child prefix on retry. BatchBind adds (1,lastKeyLen+64), verifies exact key/revision/flags/ValuePtr of the original selection, and borrows the SAME B1 entries.

Composition of the current-root cut adds (2,2s(currentDelete)+2s(PrimaryDirectoryEntry)+128), original BorrowReadRootCertificate child, then (1,2s(Cut)+2s(Authority)) before Authority.Begin; ObservePrune adds (1,k+s(Request)+s(Capability)+s(Authority)). The cut stays under the caller's original exclusion, validates timestamp/floor and SAME request, and aborts/unlocks on every unsuccessful return. Cert.Directory().Base and fixed entry construction are borrowed fixed fields. Any changed lookup-admission implementation belongs to the successor source review, not this immutable proof.

The allocator's nativeMVCCSnapshotMinimumV6.step at db/native_mvcc_allocator_preparation_v6.go:46 repeats

    (1,s(minimumContinuation)+32)+(1+Nidx,32*Nidx)+RegistryMinNext

before saving the next generation ID/currentSeen. The full map search is nonpersistent and must fit together with the selected registry next-progress child. A registry refusal repeats the map walk. Acquisition epoch changes reset the index/minimum observation, not authority; the supported schedule has no unmodeled continuously acquiring reader. After minimum ready, the allocator repeats the completed minimum entry and then (1,256) for the original fixed two slots and fixed primary.dataFloor array before reuse capability/generation setup. Do not confuse this observed minimum with MVCC discard-floor request authority or infer Nidx=33.

## Ready/Fence and caller tail

Immutable phase26 has NO ready-only public return after BindPublication. Fence ready=false returns ErrNativeMVCCRootWorkRequiredV6, then public defer latches c.failure; exact options freeze limits forever. No diagnostic may clear failure, modify phase, increase the existing cursor grant, or claim this baseline reached ready/Fence success. The writer's selected publicationReady seam needs a fresh source-backed cursor and independent successor review. Its caller-parent rows and new flag sizeof traffic must be added to these immutable expressions.

For a future ready-to-Fence call, the maximal candidate is the sum of the actual completed Candidate/Seal/Bind siblings plus actual Fence F and fixed public stats/control tail. For paused genuine Finish, lift Uterminal by the actual current-delete recovery/refund/close prefixes and phase27 caller; completed callback siblings must be included. The public ACK scalar tail adds no variable scan and happens only after ready=true Finish. No budget value is selected here.

## Outer cancellation and Close composition

Define C=(1,2s(closePhase)+2s(closed)+2s(preparation)). Every closeStep starts with C, including already-closed checks. The maximum must include:

* phase0: C+CloseCurrentDeleteNext+(1,2s(prepared)). A completed child repeats if final clear refuses. All real seal/candidate/allocator/root/input/scope/refund/receipt siblings inside that child remain the trusted terminal rows; no private abort after adoption.
* phase1: C+ProjectionCloseNext.
* phase2: C+ValueCancelNext+(1,2s(record)+2s(reader))+OutputFrameCancelNext+(1,2s(outputReader)+2s(outputLeaf)). Earlier completed children and clear row repeat if a later child refuses. Empty value-log support does not eliminate the fixed cancellation checks.
* phase3: C+OwnedCaptureCloseNext.
* phase4: C+(1,s(LeafEntry)) for a persisted entry clear; and C+FinalClear. Conservatively adding both is valid for the last B1 entry. FinalClear=(1,2s(batchEntries slice)+2s(store pointer)+2s(db pointer)+2s(Request)+2s(codec)+2s(retention pointer)). Only then all borrowed fields are scrubbed and the SAME cursor lease is returned.

Close()/CloseNativePublicationV6 drive closeStep with freshly reset frozen records/bytes until ready or error. Source-proven G must dominate every compound above; otherwise a permanently too-small remaining unit can cause a synchronous loop. A total finite-turn proof additionally needs the finite child and entry counts and no conflicting active reader/iterator on the private Snapshot. CloseNativePublicationStep uses caller work without inventing a second owner. Public Close ends the SAME lease only after all final alias scrubs; coordinator closure owns that returned lease in its own original path. Sticky real physical errors retain original custody and do not imply successful physical teardown.

## Fixed source boundaries requiring explicit successor disposition

No new hidden unbounded variable operation was found in this selected cursor parent. Three fixed/bounded traffic boundaries must not be silently called literal work reservations:

1. beginBatchFlushV6 copies the last key without its own Reserve. For the namespace-exhausted phase7 branch, the current record can be invalid/empty; its key length does not bound the last buffered key. Include the buffered selected key copy (bounded by K) in the source-shaped parent bound; do not charge it using invalid current-key zero. The path sets phase12 immediately and does not allocate.
2. ReleaseCodecBytes uses synchronous COWExternalLease.Resize after scalar charge subtraction checks. Phase20's literal row is only (1,64), not the V helper; include the concrete V traffic if this path is included in the finite caller guarantee, and explicitly preadmit it in the selected successor if admission enforcement is required. Its same budget operation is fixed, contains no binding traversal, and is not a new owner domain.
3. Public Cursor.Close calls the returned lease.Close outside closeStep/work. Its fixed concrete V traffic belongs to the outer caller's guaranteed final tail. The baseline contains no AdmitRetentionClose at that point. Ownership/alias order is sound; including V in a test bound alone does not retroactively add production admission.

The conditional caching Snapshot-birth correction 04c371a9, PRIMARY Growth, namespace/capture and lookup repairs remain independent successor source obligations. This advice neither accepts their mutable bytes nor reopens accepted ordinary GC/output domain boundaries.

## Composition result and limits

Use checked componentwise max over Birth; BatchBirth; every scan/table alternative lifted by its actual parent; the constructor/request/oldest compounds above; actual phase26 completed siblings+F+caller tail; genuine Finish wrapper; every close alternative and concrete lease tail. Call these G_birth/G_prepare/G_finish/G_cancel/G_close. A single fresh diagnostic grant must dominate their max and the writer/root-owned children. This report supplies the cursor-only additive terms, not a complete numeric G or retained union, future Finish total, Q32 fit, publication allowance, successful final-driver result or qualification. Original Q32 refusal/ACK0 remains unchanged.
