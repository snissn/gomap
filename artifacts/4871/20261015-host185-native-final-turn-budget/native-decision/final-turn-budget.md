# Actual native final-turn budget and next prerequisite

Status: public Q32/1MiB RED, ACK0. No larger-quantum success, native fence, physical Finish, or performance qualification exists. This is a source-bound construction decision packet, not permission to weaken the public fixture.

## Exact tested source checkpoint

Private original seal descriptor and original blank commit storage now complete at seal phase13. They own the SAME original two index tokens and prepared promotion. The containing capture/callback frames remain live. The descriptor takes only disposing-parent aliases. Its finite cancellation enters the original seal disposer before clearing the original transaction's four promotion edges.

`native-build/logs/private-seal-control-rerun1-all-source-{before,after}.json`: 10,308 paths, identical; corresponding full log PASS. The canonical capsule encoder zeros ParentRecordPageID/ParentCommitSeq/ParentRecordDigest because it embeds its parent by value, and rewrites the current user directory to its fixed slot directory. Descriptor validation compares that exact encoded result.

`native-build/logs/private-seal-basis-all-source-{before,after}.json`: 10,308 paths, identical; corresponding full log PASS after two authorized audit fixes: fixed two-record basis reservation BEFORE copying/comparing current base/capsule; final authority scope binds captured slot/meta/record and the saved generation-tagged promotion.Parent plus full embedded parent record, never the zero canonical ParentCommitSeq.

Both commands: pinned Go1.26.3, GOMAXPROCS2, -p1, db+mvcc focused partial-token cancellation / actual selected+anchor refusal / both-slot parent advancement tests, -count=1 -v -timeout=3m. Full commands are recorded by their log/before-source task receipt. Actual route remains phase26 missing publication, original ACK0; newer real durable foreground publication, stale/private cancellation, old-reader preservation and reopen pass.

## Current minimal scope lower bound: 36 + N, not an impossibility theorem

Let P=seal.prefix length, D=runtime.debt length, S=runtime.seals length, V=unconsumed visible members, A=allocator activated length, N=sum actual selected reservation ids, B=burned tails, K_i=abandoned coverage length of selected reservation i.

`db/primary_authority_scope_v6.go:begin` reserves

    R_runtime = 11 + 2P + 4D + 3S + 2V
    B_runtime = 2*sizeof(nativeAuthorityScopeV6)
      + 4*sizeof(primaryDurableRuntimeV5) + 16P + 32D + 32S + 32V.

`freelist/allocator_publication_work_v6.go:BeginCOWPublicationScopeV6` reserves BOTH

    R_allocator_first = 1 + P
    B_allocator_first = 2*sizeof(COWPublicationScopeV6)
      + P*(sizeof(PreparedCOWCandidateV1)+32)
    R_allocator_second = 3 + 8P + A + N + sum(B*(2+K_i), K_i != 0)
    B_allocator_second = sizeof(cow allocator state) + 256
      + P*(sizeof(CandidateIDV1)+128) + 16A + 32N
      + sum(B*(2+K_i)*sizeof(reservationInterval), K_i != 0).

For the smallest single native post-enqueue geometry P=D=S=V=A=1, B=0: 22+2+(12+N)=36+N records BEFORE physical capsule work. The selected append claim owns its tail separately; N is not the metadata tail page count. The bound already exceeds32 at N=0. Current source really repeats validation, constructs ids, allocates a duplicate seen map, scans/copies/clears activated/debt/seal arrays, and deletes actual owner keys. These are actual costs to remove, not charges to relabel.

## Full current final-turn formula

Additional terms cannot be omitted merely because the scope itself refuses:

* SAME Request.Validate:1 and Cut.ObservePrune:1 per exact observed selected/anchor key, plus exact key bytes and Request/Capability/Authority header bytes (mvccadmission/request.go).
* Atomic enqueue: actual original single-member preflight, physical Set count, temporary sets vector, Set and transaction ownership transfers, pending backing growth, builder handoff receipt, timer and notify-channel creation. These are currently unadmitted in enqueueLockedWithWorkV6/EnqueueBuiltWithWorkV6; a complete finite driver must admit this original fixed empty geometry, not call it nil or ignore its allocations.
* Activation dispatch:1; activateNativeVisibleMemberV6:2; activateWithWorkV6:4; actual leaf-generation transition/prune geometry if present; late DATA claim below.
* In claimDataLockedV6 let C=ledger candidates,O=ledger owner keys,I=selected reservation ids,n=private allocated DATA entries, and B=burned intervals. Its exact current record formula is

      L = 1+C + n*(1+O+n) + n*(1+I+n)
          + n*(1+B+C) + activationAdmissionV6.records + 1.

  activationAdmissionV6 starts at3; actual activated-vector growth adds1+A_old. Byte formula is claimStorageBoundLockedV1 plus original activation bytes+48. Even n=0,C=1,spare vector gives L=6, so activation minimum is13 records.
* Original seal installation/enqueue needs actual prefix/debt/current-base validation and original seals-vector growth before its first effect.
* BeginNativePublicationTurnV6:1; publisher preparedSealFor reservation:1; visible lookup/storage validation:1+V (3 combined at V=1).
* Actual materializePrimarySealV5: physical GrowTo/ValidateCOWPhysicalTail and visit+write every immutable candidate metadata page. Current method is synchronous/nil-work and must be converted to original work-bearing pager/candidate primitives. It cannot be charged as a constant1.
* Each original index sync uses pager.syncDirtyChunksWithFileWork:

      R_sync = 5 + 4*actualDirtyWordSpan + 5*actualDirtyChunks + min(max(1,concurrency),actualDirtyChunks)
      B_sync = 4096 + 128*span + 256*chunks + 1024*workers
             + 8*len(actualPinnedPath) + 2048.

  TWO actual index syncs, >=10 records even with no dirty chunks; namespace stabilization and empty dependency validation remain additional actual work.
* Actual durable three-page capsule write:7 records,29696 bytes on the file-backed non-mapped-flush branch, plus platform mapped flush traffic/reservation where required. Its verified-word CAS retry reserves1/32 bytes for EACH actual retry; a universal finite pre-effect upper bound must address concurrent verification rather than assume zero retries.
* ConsumeNativePublicationResourcesV6:outer1 plus original Set protected-edge consumption. Even empty selected directory is actual Set/metadata/empty-directory/backing work, with exact shared clone protection; it is not zero.
* ConsumeNativePublicationTransactionV6 dispatch:1 plus consumeNativeVisibleMemberV6:3. ReleaseNativePublicationTurnV6:1. Finish remains separately bounded on original receipt after this release.

Even hypothetically deleting ALL authority-scope work leaves >=39 records: activation13 + publisher3 + BeginTurn1 + ReleaseTurn1 + capsule7 + both minimal syncs10 + transaction Consume4. This excludes enqueue/Cut/materialization/namespaces/Set consumption, so a canonical-scope-only rewrite is rejected as sufficient for Q32.

## Narrow canonical-scope preparation proposal (not implemented)

Retain a plan on the SAME prepared candidate/original seal, with prebuilt candidate-id vector, unique-id evidence, exact selected reservation identities, and prepared output vectors for activated/debt/seal/burned-tail compactions. Perform each traversal/copy under its original mutex in admitted finite steps. Every mutable canonical ledger/runtime mutation must advance an original epoch (including independent rollback/Fail burning tails). A changed epoch restarts/refuses before enqueue; a final comparison under durablePublish->rootReuse->runtime->allocator->ledger validates exact current idx/base/slot/meta/record/Parent Ref, expected sequence, selected reservation identities/state, prefix identity and captured epochs. Own activation's known epoch transitions are included exactly. No mutable iterator or lock survives a yield.

Final original commit uses the prebuilt canonical operands: remove the actual N owner keys and exact candidates once, install prepared burned-tail/activated/debt/seal vectors, mark the original candidates published, transfer original descriptor/resource edges. It must NOT call publishBatchLockedV1's seen-map allocation or repeat complete prefix/compaction scans. Ordinary APIs/policy/nil-work behavior stay unchanged by sharing the prepared core, not a second ledger/publisher. Newly introduced backing is admitted while private, transferred as canonical output with truthful custody, and obsolete controls scrub before refund. This removes actual work but does not by itself satisfy Q32; the >=39 residual above is the honest forecast before further preparation changes.

## Physical append preparation across safe yields: existing primitive and obligations

Preferred source direction for further decision: split SAME Pager.Growth at existing growthReady before its original final install. Current Growth already privately owns new mappings/backings; captures weak pager/chunk-list/file/verified/prefetch identities, logical page count and dirty revision; revalidates before each step; installs logical numPages ONLY in install(). A prepare-only stop must never run installLogicalCountLocked or alter the ordinary allocator high-water guard.

Physical candidate writes would use only the SAME ledger's already-owned metadata ranges. Before the FIRST file/mapping write call original markTailWriteAttempted on that exact reservation. Stale/cancel then uses original RollbackPreVisible/Fail burning, never truncation, unreserve or deletion of newer pages. Prepared private mapping cleanup already unmaps only newly owned mappings and never source mappings; keep its charge and actual Snapshot/token alive until real disposal.

PageCount unchanged is necessary but insufficient. File preallocation/Truncate to chunk capacity occurs in Growth phase6 BEFORE logical install and is grow-only after current file.Stat under pager serialization. Ordinary GrowTo also uses Growth; it does not shrink a larger actual file. p.Truncate rejects shrinking logical count. Therefore capacity extension can be compatible with newer append IF private mappings are not installed, physical writes cannot hit unowned/newer ranges, all competing growth/compaction/Close paths retain exact file and mapping lifetime, and final source check rejects stale private mapping/backing. Existing source invalidation rejects even a newer ordinary count/bitset/chunk change; it does NOT provide progress under a writer after every yielded phase. Such an adversarial writer schedule can restart forever. Do not claim history-independent progress from this proposal.

Missing shared seam is an original prepared-physical-candidate sink/barrier over reserved pages with no logical-count install, and an original final Growth installation fused with allocator activation under the existing pager/count guards. Existing materializePrimarySealV5 cannot simply be called before activation because it GrowTo-publishes logical PageCount above live txn.highWater. Raw WriteAt around the guard or an alternate tail ledger is rejected.

Moving materialization/sync before visibility can remove those actual costs from the critical turn, but leaves >=29 fixed records even with zero scope (39 minus10 minimal sync), BEFORE Cut/enqueue/Set consumption and a nonzero canonical commit. Thus it also needs a concrete original transition simplification or an explicitly accepted same-transaction visibility boundary change. No <=32 total forecast is currently established.

## Separately named real split diagnostic: available decision boundary, not an invented bound

A finite diagnostic can preserve public Q32 RED and exercise the actual original producer/fence/Consume/Finish on separately named work accounting. It must use an admitted actual geometry envelope and SAME work-bearing shared primitives, with declared finite records/bytes and before/after exact source. It must pause physical Finish after real fence/turn release, complete a newer real durable foreground write, prove old receipt/ACK/resources/charges remain, resume and prove no frontier replay. It is not permission to edit Q32 fixture/caps or call nil-work/synchronous ordinary adapters.

A COMPLETE numeric bound is not yet source-proven: original enqueue's fixed empty geometry (including receipt/timer/notify controls), native empty-dependency/namespace calls, and authority verified-word retries are not fully admitted. Existing sync/growth/canonical formulas above make these remaining source obligations explicit. Quoting an arbitrary512/4096 record allowance would be a scalar grant, not the requested complete actual-operation bound. A controlled no-concurrent-verifier diagnostic can state/prove zero CAS retries from its schedule, but production bound cannot assume that. Construction should first bind these remaining actual original operations to a finite pre-effect report; then choose the separately named diagnostic allowance from its computed sum, preserving1MiB if the real bytes fit. No diagnostic runtime or broad refactor has been launched for this proposal.
