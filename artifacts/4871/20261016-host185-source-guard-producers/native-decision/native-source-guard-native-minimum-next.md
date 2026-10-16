# Guard-authorized native allocator minimum: next implementation

Read-only proposal against immutable producer03. No source change, execution, cap revision, or continuing-writer/Q32 qualification. Producer03 manifest and exact inspected source hashes are in the sibling JSON. Root owns the next source and observation selections.

## Selected boundary

Change only `StepNativeMVCCAllocatorV6` phase 4's choice of minimum provider. For an actual held PRIMARY V6 Snapshot with a completed original Directory guard, query its **same DATA indexGen registry** through that guard. DATA-only and V5 Snapshots keep `nativeMVCCSnapshotMinimumV6.step`, including both saved epoch/shard passes and its current-index map work. Keep public `DB.MinPinnedSnapshotCommitSeq` and ordinary rootReuse semantics unchanged. A malformed/mismatched V6 guard is an ownership error, not permission to take the legacy branch.

The query is neither a global format switch nor an unprotected minimum cache. Add a narrow Snapshot-to-arena-to-registry operation:

1. The actual current-delete Snapshot retains its original `primaryRoot`, certificate, DATA `idx`, pager/allocator and wrapper/physical edges. Validate that this is the preparation's allocator; compare the original root/certificate tuple, not a copied format flag. Its effective floor is `primaryRoot.dataCommit`.
2. Under the same arena mutex, validate the original certificate source: bound, born and birth-ACKed; exact generation participant/registry, `{Generation: idx.id, Source: ref.PageID}`, exact DATA floor; no source failure, busy generation handoff, withdrawal or drop ACK. A retained Snapshot bank cannot concurrently be the bank's last consumer. Use the retained certificate directly; do not rewalk a root-table bucket, acquire another bank, or allocate a new guard.
3. While retaining that exclusion, enter the same registry mutex, apply `guardRefLocked` (including its self-cell test), validate the leaf sequence, then call `minLocked` before unlocking. This rejects copied refs, wrong generation/floor, and detached/withdrawing cells. Arena-to-registry is the existing source order; no Owner, generation-close or parent callback runs under these locks.

The result is conservative during a structural mutation (`pendingFloor`), and exact when the mutation completes. It preserves floor zero as data. No Patricia descent is needed: the original counted ref already identifies its live leaf, while the minimum reads sixteen cohorts and one canonical summary.

Repeat this validation/query on every phase-4 entry that has not yet committed its capability. In particular, refusal of the later fixed horizon row must not turn an earlier sampled scalar into authority on retry. After phase 4 saves the capability, its validity follows from source overlap below, not from pretending the numeric registry minimum is monotone.

## Why acquisition and renewal cannot open an older DATA hole

For this route, ordinary Snapshot acquisition holds `rootReuseMu.RLock` from selected view through Register and actual bank/leaf-holder acquisition. Native partial birth holds that exclusion through its original bank/physical references and leaf holder, before a yielded registry registration. A partial failed birth is cancellation-only, not a readable Snapshot. Thus the source guard already covers the registration gap; the registration is not the first protection of its pages.

Current, both slots, both one-hop proofs, candidate/ambiguous coordinator members and captured RecoverableRootSet roots retain exact Directory banks. Coordinator captures now acquire counted roots while transaction/Finish exclusion is valid and retain the physical union before partial return. Writers hold their original current/basis source until their own registration/borrow is established. Last-bank withdrawal and source-generation wrapper ACK end those edges; scalar runtime/view copies do not create protection. The SAME-generation condition matters: vacuum may share PRIMARY while replacing DATA pager/allocator/registry.

A later lower relevant Register must derive readable pages from one of those already held sources, or an already registered Snapshot. It therefore overlaps a guard at or below its effective DATA floor before the sampled capability. A newly built source cannot manufacture old page authority: it derives from a held source or a newly committed DATA generation. Rebuilt historical PRIMARY records all use the one rebuilt DATA generation's `generation.CommitSeq()`, even when their record sequences are larger. Constructors bind guards before exposing roots, and a moving mutation retains a conservative old/new minimum. Do not claim arbitrary calls to Register make arbitrary pages readable, or that numeric minima cannot decrease.

Initial V6 birth and recovery bind their guards before exposure. Rebuild/vacuum create replacement index generations and bind all replacement roles before cutover. Legacy format is immutable for the opened arena; legacy capture support stays on the old scan path. The implementation review must preserve this transition proof: a guarded V6 allocator must not still have an unguarded reachable legacy source for that same DATA generation. The query itself cannot prove missing producers by inspecting one certificate. If an actual counterexample is found, keep that route scan-dependent rather than licensing it from CapsuleFormat alone.

## Genuine zero and the capability sentinel

`NewReuseCapability(oldest, 0, 0)` means no minimum restriction: `permits` explicitly ignores a zero `minPinnedSnapshotCommitSeq`. That is incompatible with a genuine guard/reader floor of zero. Never substitute 1 or pass genuine zero through this constructor as unrestricted reuse.

Existing `ReuseCapability{}` is a useful safe alternative. `permits` is false when its oldest horizon is zero; ordinary `PruneWithCapability` returns immediately and native `txnPruneWorkV1.advance` returns complete before promoting retired pages. `COWPreparationWorkV6` retains and accepts this capability. For a genuine sampled minimum zero, retain/validate the ordinary durable-slot horizon checks, then select this existing **no-new-retirement-reuse** capability. Existing reusable free pages are distinct from newly promoted retired pages; materialized DATA can still use its existing append path (`AllocateAppend`), and metadata may use already safe free storage. This is not a promise that every allocation appends, nor permanent reclamation progress. Add a named opaque factory only if needed to express this existing state clearly, without changing permits or public limits. If another concrete consumer rejects this state, refuse before reuse with original custody instead of inventing a floor.

## Price and implementation partition

- `lifecycle/source_guard.go` and registry price helper: original-cell guarded minimum under one mutex; reuse the actual 17-record `NativeMinimumPriceForTestV6` cohort/summary row. Add separate real ref/leaf/identity authority operands before touching them.
- `internal/primaryarena/source_guard_v6.go`: certificate/source/generation/floor validation and forwarding under arena exclusion; charge the actual certificate, source control, mutex and ref operands. No child creation or lifecycle-policy conversion.
- `db/primary_read_certificate_v6.go` (or adjacent narrow helper), `native_mvcc_allocator_preparation_v6.go`: Snapshot/index/allocator validation, qualified/fallback selection, phase-4 retry behavior and genuine-zero capability choice. Preserve fixed slot/proof horizon validation.
- Affected DB/lifecycle/arena pure-price helpers and focused tests: derive the sum of actual DB/arena/lifecycle entry rows plus 17, and include every repeated public/current-delete prefix. These added authority row sizes are an implementation obligation, not an already closed numeric allowance. Distinguish next-query price from whole-success traffic; no epoch scans are silently priced as one record. Do not revise the benchmark meter or original limits.

## Separate leaf publication prerequisite

Allocator DATA guards do not justify deleting `snapshotAcquireInFlight()` in `beginSnapshotPublicationV6` or its second call in `finish`. Those are two actual 256-load scans, currently outside explicit scan admission in this publication scope. The scope also reserves and performs a whole tracker walk: `T * (1 + A)` records for T tracker entries and A active generation IDs. Old-pin stale transition is another atomic `1 + pins` row. These are separate from the allocator minimum and may exceed Q32.

There is a concrete narrower candidate for a later selected change: native final publication already owns exclusive rootReuse. Ordinary captures obtain their actual pin-set holder before releasing rootReuse read exclusion; native partial Snapshot birth likewise owns its holder before first yield. The old PinSet mutex/stale transition pins every old generation whenever holders exist; later release uses its saved unpin continuation. A helper callable only from that original exclusive publication scope could use these actual holder/stale invariants instead of the global acquisition scans, after auditing historical Snapshot/Set and all holder paths. Do not expose a caller-supplied boolean certificate or reuse this argument for arbitrary ordinary publication.

Even if that proof is accepted, the real stale-transition and tracker comparison/deletion work remains. Keep exact pre-effect admission/refusal for shapes that do not fit. No existing persistent admitted native tracker-pruning continuation was found; arbitrary T/A progress would require a separately selected same-original-parent retirement mechanism preserving pin-ref identity and eventual deletion. Skipping pruning indefinitely is not a complete memory claim. Fence/caller atomic-cut work also remains separate.

## Discriminating controls and next observation

Proposed, unexecuted controls:

- Original live guard/certificate query versus copies, wrong DATA generation/registry/floor, unACKed birth, withdrawal and failure; record and byte deficits have no capability/reuse effects.
- Deep Patricia source tree and many governors: query cost stays cohort/summary plus real authority rows; intermediate minimum conservative, completed minimum exact.
- Hold a real old Snapshot/Set/slot/proof while ordinary registration pauses at an actual admission callback; query during the registration gap, then finish/close and check page readability and exact withdrawal. Exercise lower floor renewal and distinct DATA generations sharing PRIMARY.
- Rebuilt record-sequence versus DATA-floor case; current/newer root must not hide an older reachable floor. Source guard birth precedes role exposure, including partial cancellation and coordinator capture.
- Genuine floor zero: no retired-page promotion/reuse, actual append preparation can finish when resources permit, same original failures/custody retained. Empty registry sentinel is a separate case.
- DATA-only and V5 public acquisitions retain their legacy algorithm; public global minimum unchanged.
- Leaf publication races: paused ordinary capture and native partial birth, old holder/readability through successor, real last release; over-Q32 stale/table work refuses before effects, without expanded caps.

Root should first run its selected six-arm diagnostic on the accepted producer source. A later implementation should then observe the protected original Q32 writer-after-every-yield driver without changing workloads, limits, ACK/Finish, old-reader/slot/failure oracles. Removing the allocator's epoch-starvation dependence is not a claim that leaf publication, owner/hash/claim units or the atomic Fence now complete under Q32. No broad continuing-writer or performance acceptance follows from this proposal.
