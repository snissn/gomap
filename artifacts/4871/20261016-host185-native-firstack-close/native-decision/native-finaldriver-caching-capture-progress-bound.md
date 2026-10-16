# Caching native capture: preparation progress envelope

Read-only advice on immutable manifest **5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d**, archive **0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4**. JSON binds 22 inspected files. No mutable source, Go/runtime, numerical G or publication permission. Actual method names are `StepNativeMVCCCaptureV6` / `StepNativeMVCCOwnedCaptureV6`; this is the caching Snapshot constructor, not either stable-index file capture child.

Notation: vector `(records,bytes)`; vectors add componentwise; `s(X)=unsafe.Sizeof(X)`. S is original maintained mutable shard count (same lengths as view.mutables/rootPointShards); q is remaining shards, 0<=q<=S. k is actual metadata producer count (registry alone or distinct PRIMARY+registry, at most2). B_i is actual original linked binding count of producer i, including idle binding links, not only live reader counts. Use checked arithmetic. No counts are inferred from 33/80/4096.

## Actual caching prefix and saved progress

Source `caching/native_mvcc_prune_v6.go:16–43,89–188,196–256`:

```
BudgetRead = (1,2s(db.nativeMVCCBudget)+2s(COWLimits))
BudgetBirth = (1,2s(COWBudget)+s(COWLimits))
CaptureEntry = (1,2s(NativeMVCCCaptureV6)+128)
Frontier = (1,256)+(1,2s(rootDomainSnapshot))
Shard = (1,s(memShard)+2s(rootDomainSnapshot))
CacheBeforeBegin(q) = CaptureEntry+Frontier+q*Shard
```

The budget pointer birth is the original cache-lifetime governor: Validate finite original limits, charge its control into its own stats, then one COWBudget allocation. `c.budget` and db.nativeMVCCBudget persist, so BudgetRead+optional BudgetBirth occur only until c.budget is installed, not on every later capture return. db.cow must be nil for this native empty-overlay route; NativeMVCCBudget can return cow.budget, but StepCapture subsequently rejects that mode.

Capture TryLocks flushMu then writeMu, takes db.mu.RLock, and releases all on every return. No work is held across return. Failure to acquire TryLock is an effect-free retry, not proof of elapsed-time progress; supported selected schedule has no competing foreground producer before ready. This pricing does not bound scheduler/lock wait duration.

Before Snapshot birth each retry repeats CaptureEntry and both Frontier rows, even when all shards were already probed. Actual system/iterator roots are checked by four constant helpers (two HasInMemoryState, two HasPublishedState); production aggregate mutable must be nil. `root_domain.go:873–879` is fixed Len/length/pointer checks, not an immutable-vector traversal. Each remaining shard has one admitted row, checks exact table pointer equality, scalar Len, immutable-vector length and published pointer/root. BTree/HashSorted/AppendOnly/Memtable Len implementations use original scalar counts under their mutex; no entry scan is reachable for those original types. An injected arbitrary memtable implementation with a nonconstant Len is not proved by this envelope.

`c.probe++` persists after each accepted shard. Only maintained rootVersion change resets probe to0; there is no bound on repeated externally induced invalidations. Exact selected no-new-producer-before-ready schedule excludes such change. Cache must have mutableBytes0, both queue lengths0, no range spans, no pending/retry root group, no conditional active transactions, matching maintained rootVersion and shard vector lengths. A nonempty/unbound shape returns the original error, never drains/checkpoints/copies it.

After backend returns nonnil Snapshot, cache stores it and phase2 before returning even on error. Owned wrapper no longer re-enters the frontier scan; it resumes metadata enrollment then Snapshot registry capture. Unowned Step's phase2 directly resumes Snapshot.Step. Completed phase3 still charges CaptureEntry in unowned Step; owned Step has no independent generic Reserve and calls completed Snapshot.Step, which returns ready directly. NativeMVCCCapture itself is embedded in admitted outer pruning storage: no new separate allocation here. Outer public cursor/owner constructor rows must be added by that caller's report.

## Backend Snapshot birth is a compound unit

Source `db/native_mvcc_snapshot_v6.go:39–185`:

```
BackendInspect = (1,s(snapshotView)+128)
FixedRefs = (6,4s(Snapshot)+256)
 + managerPresent*(1,2s(valuelog.Manager)+64)
 + leafPinSetPresent*(1,2s(leafGenerationPinSet))
 + nonnilSet*(1,0)
 + primaryPresent*(2, primaryarena.RefAcquireBytesV6()+2s(primaryArenaOwnerV5))
Q = (1,2s(COWBudget)+s(COWExternalLease))
H = (1,2s(EnrollmentWork)+2s(retainedalloc.Owner))
AdmitBirth(k) = Q+k*(H+Q)
```

Original locks are backend maintenanceMu→valueLogPublicationMu.RLock→rootReuseMu.RLock→db.mu.RLock. View/current state+idx must match, registry and bounded-close pager must exist, current leaf PinSet must not be stale, PRIMARY root arena/physical owner must agree, and known immutable Set retention shape must have FileCount<=17. Selected empty-vlog scope uses FileCount0 (nil and admitted nonnil-empty are distinct flags), not merely the17 upper limit. `Set.RetentionSizes` returns a frozen constructor scalar: no file enumeration. Leaf pin sizes use original len/cap scalars; no refs walk during current holder acquire. SnapshotPool.Get actually allocates one fresh unpooled Snapshot and initializes its private mutex/generation; there is no pool search/reset loop or escaped-handle reuse.

Before any Snapshot/epoch/ref effect, FixedRefs is reserved, then `admitOwnedSnapshotV6` checks **remaining** records `1+2k` and bytes `Q.bytes+k*(H.bytes+Q.bytes)`. That check is NOT a Reserve/effect credit. Its body performs literal Q for wrapper lease and H+Q for each original producer beginning. A sufficient single birth call is:

```
CacheBeforeBegin(q)+BackendInspect+FixedRefs+AdmitBirth(k)
```

with BudgetRead+optional BudgetBirth if this is the first owned call, plus the caller's genuine outer prefix and separately repaired actual root lookup row below. This whole sum suffices to pass constructor preflight, not a claim that all stages require a single call. Budget creation itself is saved progress. Partial producer beginnings persist if a genuine lease/budget error interrupts callback; original c.retention/enrollment fields own them and native Cancel must dispose them. Callback success is followed by nonyielding Snapshot birth; do not introduce a new work refusal between admission success and attachment without revalidating retained size/producer provenance on retry.

The wrapper lease payload is existing `cowSnapshotRetentionCharge(sizes)+2*ptr`: independently rounded Snapshot/PrimaryRoot/PinSet/ref-vector/ID-vector/ref objects/Set envelope/file/path sizes, not byte traffic and not a new union claim. Actual lease carries its own separately admitted header. No changed quota or numeric allowance is needed for this formula. Positive validated finite COWLimits are a prerequisite; invalid-limit formatting/error construction is not a successful-route work unit.

## Required root lookup/custody repair (not implemented in input)

Backend birth currently performs `root.arena.Acquire(root.ref,nil)` after Snapshot birth and physical owner retain. Actual arena Acquire uses plain a.valid→RAM lookup. FixedRefs includes fixed bank/owner operands but not per-link lookup admission. This is a genuine inherited admission gap, separately raised to root.

**Simply passing w there is unsafe:** the current Snapshot starts capturePhase2, so a post-birth ready=false,nil leaves a Snapshot whose next Step skips missing borrows and starts registry. Returning a synthetic hard error on work exhaustion is also wrong. A partial phase cannot rely on the unowned snapshotView/current bank surviving future publication after locks release.

Minimal coherent source option selected in discussion: under SAME original DB locks, hold original primaryArenaOwner.mu and prevalidate refs>0 and <max and SAME arena. A narrow `arena.AcquireAfterAdmissionV6(r,admit,w)` factors original Acquire core: under arena.mu reserve fixed+actual lookup, retain SAME discovered bank, validate exact root/sealed/ref-overflow state, then invoke **original admission only**. On callback success increment that bank/counters nonfallibly before arena unlock. There is no second lookup or escaping bank/permit. After helper returns true, original physical owner mutex is still held; retainLockedV6 must be guaranteed nonfallible by its protected precheck, then allocate/init fresh Snapshot and immediately attach BOTH s.primaryOwner and s.primaryRoot and initialize s.primaryRelease before owner mutex releases. No work admission, error return or other fallible operation is permitted between bank acquisition and this attachment. Fixed fresh Snapshot allocation/private-mutex initialization and original atomic counters are nonfallible operations here. Move manager/index/leaf borrow operations after that attachment, outside both callback and owner/arena mutexes, still under original DB publication exclusions; record each flag immediately and return nonnil Snapshot on genuine subsequent error. Refusal of lookup admission occurs before callback/Snapshot/epoch/borrow effects. Callback refusal acquires no root edge; original c owns any already saved lease/enrollment. FixedRefs may conservatively include its original bank fixed row plus helper fixed row; label that actual duplication rather than dropping one from the source vector.

This is narrower than invoking Snapshot birth inside arena callback: only original admission runs under arena.mu. Physical-owner mutex excludes final close through the small bank-acquired→Snapshot-attachment interval, and acquired bank edge protects root while arena mutex is released. Lock-order requirement: physicalOwner→arena→retainedOwner/EnrollmentWork→concreteBudget. Existing arena constructors already take arena→metadata. Known callback BeginEnrollment takes only original retained Owner/EnrollmentWork and concrete COW budget locks, never calls physical owner/arena; arbitrary user callbacks cannot inherit this proof. The helper callback must not reenter arena/physical owner, invert those locks or store a permit. Original `BeginNativeMVCCSnapshotV6` exposes a generic func argument: either document the new native no-reentry/no-inversion contract and inventory every original callback, or use a named typed-owned seam so unknown generic callback behavior remains outside the new locked route. Existing ordinary snapshot-admission APIs and ordinary Acquire must remain unchanged. No manager/idx/leaf or existing Snapshot locks belong inside the callback. Any added escaping callback environment must be admitted/repriced; no new retained registry/owner is justified.

Finalizer `native_mvcc_snapshot_close_v6.go` phases2/3 consumes root+initialized queue before phase6 physical-owner release; nil primaryOwner is structurally skipped. However the bank edge alone does not keep the physical Arena owner alive. The combined prevalidated owner+root attachment avoids needing a new proof of partial root/no-owner physical lifetime. Root lookup envelope uses prior L=Npre+6 bound; actual patched admission, not merely adding L to test G, is required.

## Metadata pair: exact persistent-stage rows

`owner_enroll_work.go` uses original embedded EnrollmentPairWork; no map or replacement owner. BeginPair has no additional literal pair Reserve: each producer beginning uses H+Q and saves original pending governing lease before physical capture. After Snapshot birth `EnrollPairWithWork` normally already has firstOwner/budget from Begin, so its one-time generic pair initializer `(1,2s(EnrollmentPairWork))` is skipped; include it only for a separately valid unbegun call, never double-count.

For each original producer i:

```
Visit = (1,2s(binding)+s(EnrollmentWork))
Phase2 = H
Phase4 = (1,s(Enrollment)+2s(binding)+2s(Owner)+2s(EnrollmentWork))
R = (1,2s(COWExternalLease)+2s(COWBudget))
```

Phase1 searches B_i bindings with Visit per node, saves cursor after every visit and holds original cleanupRefs, then phase2 reserves header/control delta and changes phase before later budget work. Phase3 walks B_i original binding links, Visit+R when that binding requires Resize; cursor saves next only after genuine Resize. A refused Resize replays Visit on the SAME node, not the preceding B_i-node prefix. Phase4 new-binding route needs Phase4+R indivisibly for pending lease Resize then original binding+Enrollment allocation; found-binding route needs Phase4+R for temporary lease Close before detachment. Both preserve original charged control delta until complete. A conservative whole no-concurrent-owner-mutation stage bound is:

```
EnrollStage_i <= B_i*Visit + H + B_i*(Visit+R) + Phase4+R
```

This is a sufficient finite source-shaped sum, not a minimum indivisible unit. It includes already completed stages conservatively. Most stages persist before a later refusal. A legal concurrent ordinary enrollment can require actual adjustment RemovalWork; selected no-unmodeled-reader/producer mutation excludes this branch. Otherwise add original adjustment/unwind children from terminal reports, do not invent constant zero. Own new binding can raise later producer binding census by at most1; no external enrollment producer is included.

Pair completion uses actual `(1,2s(Enrollment)+s(EnrollmentPairWork))` to attach companion after second producer succeeds. If that row refuses, completed second Enrollment result remains saved. Snapshot.Adopt then validates actual pair under original iteratorMu and transfers it; DetachResult clears source aliases. Those fixed scalar/lock operations have **no separate literal Reserve** in this immutable source; they are not hidden loops/allocations, but an exact whole-parent inspection envelope must label them as fixed parent traffic rather than invent an existing row. Owned path sets metadataAdopted only after transfer, then skips the entire pair in future calls. Pair error/unwind remains original finite receipts, not synchronous Close.

## Snapshot registry readiness and final store

Let T=fastReaderShardCount=16, N=NativeMVCCRegistration:

```
RegistryBegin = (1,2s(N)+8)
RegistryProbe = (5,2s(N)+40)
SnapshotReady = (2,2s(Snapshot)+64)
```

Begin stores original registry/sequence/hint and Snapshot capturePhase advances2→3. Registry loops join pass then claim pass (at most2T=32 admitted probes without competing atomic mutators). Probe next/phase persist; failed CAS repeats SAME probe with its own Reserve. The required held old reader remains open and idle through this preparation: it does not imply contention or no-reader shape. With arbitrary concurrent registry changes there is no finite bound on successful progress, though each retry is admitted. Empty registry or joinable designated reader supplies genuine slot authority, not a mocked completion. Full occupied cells yield original capacity error, never fallback allocation.

Registry success stores original id in embedded registration before SnapshotReady. If SnapshotReady refuses, that id survives and next Step returns it without replaying count increment/claim. Final row performs reader/tree reconfigure, sets readable state/phase4, clears borrowed view, and closes original in-flight epoch. This repeats no tree walk or file decode. Unowned caching phase2 adds CaptureEntry per call; owned wrapper after Snapshot has no generic literal capture prefix, but retains actual caller prefix. A sufficient final registry call envelope is `RegistryBegin + 32*RegistryProbe + SnapshotReady` (plus its genuine caller prefix), with first begin omitted if already saved; this is conservative, not an instruction to force all32 into one production Q32 call.

## Supported census and limits

Use exact existing cache shard count and table/domain pointers, original finite COWLimits/budget presence, primary/registry distinctness, known immutable Set shape flags and capacities, actual PinSet current/stale scalar, actual owner linked binding counts and original registry fixed16 shards. Header/slot Len inspections do not traverse those vectors. No extra cache writer/flush, owner enrollment producer or registry mutator before ready; designated old reader idle is allowed. Capacity/lease/overflow errors remain actual errors; budget/work/refusal never becomes physical I/O poison. TryLock/OS scheduling cannot be turned into a progress guarantee by increasing G.

No other variable unadmitted loop was found in the selected caching frontier/lazy-budget/empty Snapshot constructor besides the separately identified PRIMARY lookup. Unknown interface memtable Len, arbitrary admission callback lock behavior, mutable version churn, nonempty vlog physical cleanup and foreign WorkBudget are outside this proof. Actual implementation/control acceptance must bind the later root repair and its new layout/traffic; this advice does not credit mutable source changes.
