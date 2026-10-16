# Independent index-observer diagnosis and plan review

**ACCEPT the diagnosis and selected observer-only repair plan.** This is not acceptance of a repaired implementation or permission to execute it. The frozen 041b0 observer has the confirmed defect below; its actual33 attempt remains failed.

Reviewer: `/root/ordinary_review`, explicitly routed `gpt-6.1-sol`, high, internal independent subagent review. No construction role, children, source edits, Git/GitHub operations, Go execution or runtime were performed. Hosted manual review cannot pin the required exact model; none was requested. Only immutable source and retained evidence were inspected. Mutable `native-integrated` was not read.

## Exact bindings and retained failure

- Source manifest `041b0ddfca3581c2b773ceb857a27b2dd4c718775b45a9cb71d0c7cc3107744d`; archive `963f9deb573465a5e56a9e2d94841058851bbc2ae28e01cc7533e7cee7106182`; recorded tree `2fc987a8d295af50286fa81d103537e569466eda`. Independently rehashed all **10,375** archive members: exact manifest/path/hash equality, no omissions or extras.
- Diagnosis `378e53e227244ab9bca70319f1f2243b90e5078744c429570ab9c89d37589bc7`; selection `7c9941e990ba64cfd413ec66ffd2bf649d6fe829c599f20052d33d940f07fdfe`.
- Actual33 raw `b8e3a82ea642c953abf82b221960ca6cc41ebfce1f9b6fdd192217612f4879cb`; evidence manifest `7b436f31954838bd68864988309f4f977994932941b9626221de71db6d1832dd`; archive `1cb3c15e7d5844ba2ccecf62ae345fcc97d59e8413db8820d18d53ff2842bc1e`. All 17 retained payload hashes and all 18 evidence archive members, including manifest, independently match.
- Root verifier `bcebbe1d48fc9287730decc1ba8601bea5b9108748db3fe0e6990b7055ba2651` corroborates source/process binding, without diagnosing the failure.

One actual command exited 1. It reached phase26/candidate8/seal13, with first original route refusal after157 calls. The group4 index0 observation was ready and failed its identity assertion. Process/result receipts show the leader joined, subreaper verified, all group/direct/adopted children absent, no timeout or signals. The command template's inherited `executed:false` is not the execution receipt: RUN/FAIL, actual PID/PGID and result prove the single execution. Old bytes remain unchanged.

## Confirmed predecessor defect and sibling scope

**O1 — deterministic false retained-file identity predicate.** `TreeDB/pager/write_work.go:449` requires `token.pinned == pager.file`. The caller is `rootpublication/primary_empty_candidate_work_v6.go:329`, reached by `db/native_mvcc_seal_preparation_v6.go:140` and asserted at `mvcc/native_prune_backend_route_v6_test.go:349`.

`pager.WithStableResourceFile` explicitly lends the original handle only for its callback (`pager.go:105–117`). Native token construction deliberately duplicates that handle (`index_token_construction_work_v6.go:186–191`); Unix `Dup` followed by `os.NewFile` creates a distinct live File object (`resource_identity_unix.go:102–111`). Thus the equality rejects the intended successful retained capture. Archive-wide call inventory found only this observer caller for the pager equality helper. DATA and PRIMARY traverse the same helper and constructor: fixing only index0 would deterministically fail index1.

The selected replacement preserves stronger, relevant checks: derive the expected DATA `snapshot.idx.pager` or PRIMARY `snapshot.idx.primary.Pager()` from the SAME retained snapshot; compare the actual callback pager with that selection; compare exact original operations; verify the unreleased token's nonnil retained file is the physical backing's file, its ref pointer is the same backing ref counter, refs remain positive, and metadata/physical owner/registry match original operands. It neither reopens nor stats a pathname, creates an owner/reference, nor returns a physical alias. The incorrect pager helper can be removed without production-barrier changes.

**Generation must follow the original constructor.** Capture phase2 records immutable physical `capture.identity`. Token constructor phase5 derives identity from the duplicate and assigns `identity.Generation = spec.Generation`, where spec.Generation is the original snapshot's index-generation ID. Compare full identities after assigning only that expected Generation to a local copy of capture.identity. Do not derive expected identity from the token under test, mutate the capture witness, compare paths/fd integers, or omit Generation. The namespace's existing `sameStableObject` deliberately checks only physical Platform/VolumeID/ObjectID (`resource_token.go:2632–2651`); that is compatible with a separate full logical-generation check.

The existing other predicates were traced to actual constructor assignments: original Snapshot and transferred completion, embedded native capture, metadata owner, physical primary owner, original registry, callback reference and charged wrappers. The selection retains them. It also adds the missing independent selected-pager and physical-backing checks. No additional deterministic sibling mismatch was found in this bounded chain review.

## Pending custody and actual lifetime

`CustodyComplete=false` is correct before cleanup, not evidence that construction forgot custody. `newOriginalCleanupCustody` admits the backing, initializes refs1/owner/registry, increments the registry's original cleanup edge and leaves custodyCompleted false (`resource_original_cleanup.go:106–127`). Actual33 separately reports nonzero CustodyCharge512 and matching owner/registry. Its fatal predicate does not assert CustodyComplete.

Construction retains the SAME callback. Acceptance installs it on the retained Snapshot; final capture preparation drops only independent builder edges and detaches the constructor (`native_stable_pager_capture_v6.go:375–420`). Seal preparation transfers the two token aliases into original DATA/PRIMARY seal slots and retains embedded capture frames for callbacks (`native_mvcc_seal_preparation_v6.go:550–582`). There is no reason to complete/refund custody during observation.

The original last-consumer release closes the SAME Snapshot, releases the physical owner, then invokes `OriginalCleanupCustody.CompleteWithWorkV6`, detaches its alias and drains existing accounting completion before final callback/frame scrub and remaining refund. The completion primitive requires the actual original registry edge and refs1 (`resource_original_cleanup.go:285–324`). The observer may strengthen presence/nonzero charge/expected pending state and owner/registry checks, but must not call Complete, acquire a new edge or change cleanup sequencing. This source trace is not proof of an unexecuted actual native Finish.

Production sync already dispatches the token's retained file (`resource_index_sync_work_v6.go:63`) through the original callback to `SyncNativeIndexDataWithStableFileWithWorkV6`. That pager primitive explicitly preserves the supplied retained file even after the original pager file retires; Linux fdatasync/fstat use it directly. Production does not require the rejected File-pointer equality. No constructor, fence or release change is justified by this observer failure.

## Minimum coherent batch and verification boundary

The selected four observer/assertion paths plus at most the two narrow control files are sufficient scope. Preserve constructors, publishers, final driver, cleanup owners, representations/layouts, F293/R145 and work caps. Reserve actual newly read scalar operands/comparison traffic before access; this prices observation, not the final-turn product expression.

The proposed controls are meaningful if they exercise both actual original DATA and PRIMARY capture step/close paths under unchanged32records/1MiB, use the same factored observer core, and avoid manufacturing a seal/member/receipt. Cover wrong original operations, selected pager, physical identity, logical generation, missing/mismatched backing and zero/short-work no effects. Restore any isolated temporary input substitution before the SAME original cleanup; compare actual charges, capture/borrow counters and no replay to the warmed baseline. Existing partial construction/cancellation, original custody typed-growth/refund/unsupported/closing controls and the retained-file fence control cover adjacent boundaries without authorizing final publication.

A final frozen source review must inspect the actual implementation, observer pricing and precise commands. The selected compile-only run is a root authorization, not a grant from this report. No further fixture or actual33 run was performed or authorized here.

Detailed index1 geometry, remaining group4 retention rows, group5 and later newer-writer/stale/cancel/old-reader/reopen assertions were not reached. The emitted367records/871273bytes are a partial non-atomic diagnostic expression, not complete retained/Finish geometry or an allowance. Original Q32/1MiB/33/80/4096 remain RED/ACK0; private backing, retention/transfer dedup, nonempty routes, full Finish and C1/C3 qualification remain open.

All exact input and inspected-source hashes are retained in the sibling JSON. Plan review lease released.
