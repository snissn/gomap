# TreeDB Verification Matrix

This document maps specification invariants to existing tests and harnesses.

Immutable memtable foundation: `TestCOWOwnedHeadersAndBytes` covers old header
identity, caller/output alias attempts and legacy arena poison/reset isolation;
`TestCOWPrivatePreparationCancelAndResourceOwnership` covers private cancellation,
preallocated publication and independently retained exact-once resource owners.
`TestCOWSplitDeleteBatchHeightAndMetadata`, `TestCOWDependencyLayoutContract`,
`TestCOWDependencyReserveWitness`, `TestCOWBatchReserveWitness` and
`TestCOWRebalanceCapacityHistoryWitness` bind allocation reserves to dependency
layout, split/rebalance/capacity and batch-height boundaries.
`TestCOWConcurrentTraversalAndClose`, `TestCOWBudgetCloseKeepsExistingViews` and
`TestCOWSuccessorAndChargedCursorAdapter` cover concurrent traversal/lifetime and
allocation-free successor dispatch. `TestCOWLimitsAndRefusal`,
`TestCOWFiniteReplacementHistoryAndResume` and `TestCOWRetainedGenerationPlateau`
cover finite admission, cumulative replacement history, pinned residency and
release/resumption. `BenchmarkCOWPrepareReplace`, `BenchmarkCOWCapture` and
`BenchmarkCOWScan` witness internal N/2N preparation/capture/output costs; the
[capture commands and scope](cow-memtable-ownership.md) keep these distinct from
future integrated DB lifecycle qualification.
`TestCOWRetainCloseRace` covers shutdown refusing fresh root ownership;
`TestCOWCursorGeometricStackWitness` and `TestCOWPointerAllocationCharge` bind
discarded stack backings and GC allocation headers to the reservation proof.
`TestCOWExternalAdmissionLifetimeAndRefusal` and
`TestCOWExternalAdmissionSharesPrepareAndRetirementBounds` cover external storage
admission before allocation, finite shared capacity, overflow, concurrent Close
and shutdown control lifetime. `BenchmarkCOWExternalLease` reports its wrapper
allocation/admission cost. `TestCOWDeferredCleanupRemainsChargedAndCopiesDrainOnce`
keeps deferred callbacks/storage charged through copy-safe cleanup.
`TestCOWLargeKeyAdmissionAndReadAllocations` and
`TestCOWByteLookupAcrossLevels` cover conversion-free pre-admission estimation,
refusal, owned-key replacement/removal, reads/ranges/cursor seeks and three-level
ordering in both default and `treedb_safe` builds. `BenchmarkCOWLargeKeyLookup`
reports the safe-build rank-search cost and zero key-conversion allocations;
[build-tag commands and scope](cow-memtable-ownership.md) bind the comparator.

Captured R1 readers: `TestR1CapturedReaderSharesValidatedMetadata` checks public
reuse without shared snapshot pins. `TestR1CapturedReaderBoundsBorrowedBlocks`
checks complete rows, independent block/handle/descriptor caps, backing credit,
eviction and owned output after close. `TestR1CapturedReaderFailedLoadReleasesAdmission`
checks corruption refusal, reservation/descriptor cleanup and same-view retry.
`TestR1CapturedReaderOversizeOwnedFallback` checks explicit owned fallback and
stale-reference refusal. `TestR1GetIntoDoesNotAllocateWholeIntermediateDocument`
and `TestR1GetIntoAliasesReallocationAndErrorOwnership` cover public final-buffer
emission, aliases, growth and missing/error ownership. Existing `TestR1TypedRow*`,
`TestCollectionReadView*` and serving-materializer suites remain the held-cut,
reopen, projection, integrity and concurrent-publication oracle.
`TestColumnSharedJSONEmitterParity1887`, `TestColumnSharedDeclaredScalarJSONParity1887`
and `TestColumnRetainedEmissionCursorParity1887`
compare JSON byte/decoder parity. `TestAcquireOpenFileRangeDescriptorLease` binds
range mapping/read-at lifetime to the stable open descriptor through path replacement.

## Immutable COW cache qualification

The [COW publication contract](cow-cache-publication.md) requires evidence from
actual public `MemtableMode="cow_btree"` dispatch in all three production
profiles. Internal foundation/read-owner checks alone do not qualify the mode.
`cow_cache_contract_test.go` keeps the cache dirty and checks capture rotation
counters and pinned visibility. `cow_public_contract_test.go` exercises public
owned reads/successors/iterators, early capability refusals, point batches,
explicit sync and checkpoint/close/reopen. Acceptance must bind their results
to the tested source; a gated Open is a failing route, not a passing skip.

Additional integration gates are:

- Deterministic preparation/preappend/preswap barriers across two shards, final
  staged/live/frame/replayed RID/revision identity and concurrent accepted writers.
- Reader progress during private preparation and durability pauses; `no_wal_fast` sync
  and checkpoint/admitted-writer/flush lock progression.
- Captured-prefix chunking, one final backend publication, late history/same-ts
  writes and physical tombstones; accepted-plus-error and handoff-admission
  refusal keep the exact receipt and retry only handoff.
- Actual forced-pointer/dictionary producers, old read cuts across GC/rewrite,
  partial preparation cancellation and deferred exact-once unlocked cleanup.
- Snapshot/iterator Next/Seek/Close and DB-close races, finite pending batches,
  read workspace/codec backing, retention plateau/refusal/drain/resume.
- Direct COW process-crash cuts and separately labeled modeled stable-image
  power-loss cuts around dependency sync, index/seal/meta publication and reopen.

Construction tests in `caching/cow_*_test.go`,
`internal/valuelog/cow_*_test.go`, `internal/dictdb/read_definition_test.go`,
`db/bounded_read_owner_test.go` and `tree/owned_iterator*_test.go` provide focused
mechanism witnesses. Reconcile their hashes with each candidate's changed
inputs; earlier packets do not certify later checkpoint/public/recovery edits.

```sh
GOWORK=off go test ./TreeDB -run '^TestCOW' -count=1
GOWORK=off go test -race ./TreeDB -run '^TestCOW' -count=1
```

Cost evidence compares identical N/2N dirty fixtures at fixed shard/source limits
in append_only, btree and COW, with the same production profile and instrumentation.
Include acquisition/read/release, changed-operation preparation, returned-record
scans, B/op/allocs, tails, checkpoint/sync and pinned/retired/drained memory.
Retain source/toolchain/module/binary/host/seed identities and anomalous runs.
Report no RSS quota, whole-process zero-allocation or production speedup from
internal reserve/lookup witnesses. C4 owns sustained public qualification and
any recommendation to promote the mode.

## Additional implementation witnesses

`TestOuterLeafOrdinaryAdditiveProducerInventory` covers ordinary optimistic,
forced serialized, and physical build-group publication with multiple real
within-apply rotations and an empty current lane. It requires zero fresh-load
candidate scans, inclusion and stable frontiers for created/current identities
before registration consumes them, unchanged logical pointer counts, exact
projection for overwrite/delete/range-delete, retained snapshot bytes, and
checkpoint/clean-close/reopen pointer bytes.
`TestBuildValueLogRefDeltaProducerInventoryErrors` rejects created/current
snapshot errors while preserving pager/untrackable eligibility. Existing
`TestOuterLeafCommitFailsClosedWhenReportedSegmentCannotRegister`,
`TestOuterLeafPointerCommitFailsClosedForUnreportedSegmentWithoutRefreshScan`,
`TestDurableRootPublicationRejectsUnregisteredCanonicalValueLogPath`,
`TestDurableRootRecoveryRetainsAndReplacesBothSlotDependencyClosures`,
`TestLeafGenerationGC_DryRunRetainsOlderRecoverableRootGeneration`,
`TestLeafGenerationGC_RetiresPinnedGenerationUntilSnapshotCloses`, and
`TestValueLogGC_IncrementalParityWithFullScan` retain ownership of registration,
recovery-slot, pin, and GC safety checks. The tests establish correctness and
path selection; same-harness native sync-load and ordinary guardrails remain
required for performance acceptance.

Prepared no-index JSON semantic-stream insertion is covered by
`TestPreparedInsertOverlapsOrderedCommit` (batch N+1 prepares while N is held
before publication, with no early acknowledgment),
`TestPreparedInsertSortedValuesAndReopen` (caller-order IDs, sorted retained
and typed values, caller-buffer reuse, one-shot commit, durable reopen),
`TestPreparedInsertAbandonBoundsAndLateConflict` (oversized admission,
abandonment, duplicate precedence, and authoritative late conflict),
`TestPreparedInsertCheckpointBeforeCommitAndReopen` (private prepare across a
sibling ordinary write, checkpoint, and durable reopen),
`TestPreparedInsertValueLogBlockPointerSurvivesReopenAndGC` (persistent block
pointer and GC reachability), `TestPreparedInsertCrashRecoveryCuts` (observed
WAL sync before durable acknowledgment and WAL/asset/applied-LSN cuts during
commit, plus queued root-installation cuts under checkpoint),
`TestPreparedInsertFallsBackBeforeUnboundedDeclaredRowExtraction` (unsupported
scalar type uses ordinary insertion), and
`TestPreparedInsertRejectsMismatchedCapturedSchema` (commit-time catalog
validation), and `TestPreparedInsertThreeAggregateSpecsRemainEligible`
(the JSONBench five-column, three-metadata-spec target stays on the prepared
path without assigning a part identity).
`TestPreparedInsertNearRowLimitHighEntropy`,
`TestPreparedInsertRejectsTypedPrebuildWithoutCredit`, and
`TestPreparedInsertLongIDsRejectCommitReserveBeforeWAL` exercise bounded
admission at real batch cardinality and assert resource rejection before LSN
assignment. The stream quota tests cover rare-path growth and compressed/raw
block capacity. These focused tests establish the checked limits; the strict
incremental-byte envelope still requires the encoder and typed/aggregate
allocation-site audit described in the prepared-insert memory gate.
`BenchmarkPreparedInsertPublicPath` compares ordinary, prepared serial, and
one-ahead public insertion with the real WAL/publication path. These tests do
not replace the JSONBench real-data load and query comparison.

Within-domain physical home packing: `TestDomainHomePackingKeepsCommunitiesV1`
is the interleaved-community regression; `TestHomePackingBoundResponseAndRetainedReuseV1`
checks bound requests, hostile responses, exact capacity and persisted homes
across overlap variants. `TestHomePackingPinnedKaHIPV1` optionally executes the
pinned native solver, including repeatability and empty/single-pack cases.
`TestM0MaterializeByteBoundedMembershipReopensDisposableClone` retains non-striped homes
through materialization/reopen. `BenchmarkDomainHomePackingV1` compares the
100K-row packing/validation allocation boundary, excluding the separately
measured solver and serving. These are construction checks, not qualification
of the real-data serving improvement required by #4775/#4753.

The global all-level spherical router is distinguished from the historical
leaf-only model by `TestRouterGlobalBudgetRetainsInternalCenters`,
`TestRouterGlobalBudgetDegenerateUnderfill`,
`TestRouterGlobalBudgetApportionment` and
`TestRouterGlobalBudgetCanonicalSiblingQuotaIdentity`. These cover retained
parents, repeated provenance, global conservation, structural underfill and
canonical sibling/quota identity across clustering seeds.
`TestVectorPartitionRouterV3HierarchyBudgetAndDurableNodeIdentity` exercises
actual published records/reopen, roots-first scoring, atomic child-group budget
stops, typed pre-score exhaustion with no partial routes, and charged exact
scans. `TestVectorPartitionRouterV3ConcurrentPinnedSearch` checks shared-owner
search.
Existing manifest/lifecycle, live-index and production public-backend tests
continue to own checkpoint/reopen, source changes, pin/deletion and stable-ID
merge behavior. These are correctness tests, not scaling qualification.

`TestCollectionVectorIndexCloseCosineRerankIsStableWithFilterAndLiveDelta`
proves that materialized FP32 cosine reranking preserves close-vector order and
filtered/unfiltered distance parity, with zero distance for an identical live
delta.
The vectorops cosine tests and `TestVectorIndexCosineCloseVectorsRetainDistanceAndDiversity`
cover normalized-difference arithmetic, scale, close-vector ordering and graph
construction diversity.

Point FP32 reconstruction: `TestTypedColumnPointFetchDoesNotExpandFP32Part`
checks four public results against 128/1024-row parts without whole-column FP32
union expansion. `TestTypedColumnPointFP32OwnershipAndBounds` checks constant
requested-row decoder allocations, owned results, overwritten source bytes, and
invalid row/payload/locator rejection. `TestTypedColumnPointReadAtRetainsVectorsAcrossGenerations`
forces real read-at scratch reuse across base/replacement generations and checks
deletion. Descriptor/primary-ID setup is still part-sized; these are not whole
public-request constant-allocation claims.

Complete current source-vector diagnostics: `TestColocatedAuditPopulationOnlyDecodeV1`
is a capability-absence semantic red against the predecessor decoder.
`TestFixedPeerColocatedAuditCurrentAuthorityV1` exercises initial and final
population attachments through authenticated diagnostics on all four real Raft
voters, retains legacy six-witness compatibility, and refuses actual extra,
missing and same-count stale vectors outside the witness IDs. Its concurrent
follower-apply test refuses changed-state receipts without an FSM/admission lock
cycle. `TestVectorSourcePopulationCurrentProjectionV1` covers current inserts,
deletes, replacements, fixed-D column projection, signed zero, exact inspection
exhaustion, bounds/overflow and cancellation; `TestVectorSourcePopulationRetainedJSONV1`
covers inline retained JSON and invalid/missing/nonfinite/zero-cosine vectors,
including exact source-record and total-byte bounds.
`TestVectorSourcePopulationDirectoryV2` requires real pointer-backed current
primary entries and exact descriptor-plus-vector bytes, with one-byte and
physical-work refusal controls. `TestVectorSourcePopulationJSONPointerRefusesV1`
refuses non-column JSON pointers before value-log payload decoding.
`TestVectorSourcePopulationEmptyV1` proves an empty typed collection without
requiring an unused manifest or asset.
`TestVectorPopulationPhysicalIteratorCancellationV1` cancels skipped tombstone
work before a visible callback exists. `TestPeerPopulationAuditPreMarshalBoundV1`
checks conservative encoded accounting and oversized/partial-plan refusal.
Existing `TestBufferedRootRunsIterator*`, `TestTypedColumnPoint*` and
`TestVectorFromJSONFieldMissingAndInvalid` remain affected helper controls.
`BenchmarkPopulationLegacyColocatedPlanGuardV1/{validate,decode}` uses the
retained six-original-outcome/four-ID plan with no population attachment; it
guards only existing admission/decoding work, while the real-Raft legacy audit
control above covers authenticated current-FSM behavior.

`BenchmarkVectorSourcePopulationProofV1/{512x128,10000x128}` reports scan time,
B/op, allocs/op, charged source/asset bytes and physical inspection units.
This is explicitly untimed diagnostic overhead, not serving speedup or capacity
qualification; #4250 owns actual all-voter resource/lifecycle acceptance.

`TestTypedGraphPublicFoldControlRootPolicy` uses the public command-WAL durable
opening helper with native leaf generation enabled. Default, fast, and compressed
control policies cover fold followed by an ordinary typed replacement, exact
held/current full fetch, and ordinary wrapper close/reopen/re-Ensure.

`TestTypedGraphPublicEmptyLifecycle` exercises empty Ensure, filtered/unfiltered
search, insert, delete-all/fold, held-owner fetch, ordinary reopen/re-ensure and
reinsert with identical M2/M16 schemas and zero canonical-row reconstruction.
The internal empty prepared-cache seam still rejects without retaining a holder;
public empty cutover releases its obsolete keeper without closing held owners.
`TestTypedGraphPublicEmptyFoldLateKeeper` pauses a nonempty fold's captured-cache
build across a second empty fold and checks the late keeper is also released.
`TestPreparedSearchInvalidationReplacementBuild` checks exact-old invalidation
neither waits for nor removes a newer building entry, while broad invalidation
still waits for installation and closes the replacement.

`TestColumnAssetLifecycleIdentityUsesImmutableCatalog` verifies that pin and
registry admission and snapshots use one immutable catalog identity rather than
mutable handle metadata. Concurrent typed upsert and Dense reads after vacuum
are covered by `TestServiceTypedFreshUpsertConcurrentDenseAfterColumnGraphBuildAndVacuum`.

`TestColumnAssetLifecycleSharedCopyBudgetCountsRecords` checks the common
pre-copy budget across input refs, pin records, and registry records, including
exact fit and exhausted-budget rejection. `TestTypedGraphPublicFinalWarmBarrierCancellation`
checks cancellation while the final Ensure/Fold warmer waits on the actual
storage barrier. `TestTypedGraphCapturedCacheCanceledWaiterPreservesBuilder`
checks that a canceled coalesced waiter leaves the other builder's keeper valid.
`TestTypedGraphColdManifestScanCancellation` cancels on the second periodic
context check in a real multi-interval manifest preflight and preserves the
existing over-budget error translation.
`TestPreparedSearchBuilderBarrierCancellation` holds the actual storage barrier
while public warm/buffered callers build exact and quantized prepared state,
then checks cancellation, cache cleanup, and healthy nil-context retry.

Explicit typed graph serving: `TestTypedGraphPublicSameOwnerServing` covers public
ensure, typed mutation, same-owner filtered search/full fetch, fold, independent
manager and normal reopen. `TestTypedGraphPublicServingPressureAndOptions` covers
owner lifetime, unsupported controls and fail-closed missing metadata;
`TestTypedGraphPublicEnsureCancellationAndFailedAdmission` covers held-barrier
cancellation and rejected writes after partial setup. `BenchmarkTypedGraphPublicServing`
includes public acquisition/filter/search/full fetch/Close with a live suffix and
tombstone at 128/1024 rows. It is bounded diagnostic evidence, not final Minima
throughput qualification. `TestTypedGraphLifecyclePublicMutationAndReopen`'s
`serving_crash_reopen` cases enable public Ensure before acknowledged
insert/replace/delete/reinsert, exit without Close/Flush, then ordinary-open and
re-ensure for filtered/unfiltered same-owner search/full fetch. Indexed canonical
JSON extraction stays zero; the old unconfigured compatibility cases remain.
`TestTypedGraphPublicEnsureStaleCapturedKeeper` pauses after captured resources
exist but before cache installation, interleaves another handle's public fold,
and checks conflict, exact keeper accounting/pin release, accepted suffix and
held old-view readability. These do not prove power-loss or foreground fold
availability. See [serving admission](typed-asset-maintenance-1788.md#explicit-typed-column_graph-serving-admission).

`TestTypedGraphPublicFoldPublicationAvailability` exercises public filtered
search/full fetch and replacement after physical install but before checkpoint,
including an old held view. `TestTypedGraphPublicFoldPostInstallAckCrash` blocks
publication seal writes, acknowledges a public replacement after fold install,
exits without Close, and proves ordinary replay from the pre-fold base plus latest
same-owner filtered full fetch with zero indexed JSON extraction.
`TestTypedGraphPublicFoldPostCaptureSuffixAndDebt` covers replacement, delete,
reinsert and a growing-base insert during construction, exact logical/pending
costs and unchanged attempted-output debt at install.
`TestTypedGraphPublicHealthyEnsureConcurrentOwner` forces acquisition between
drain and storage capture while unchanged Ensure holds schema admission.
`TestTypedGraphPublicFoldStateInstallConflictFencesWrites` injects a derived-state
CAS conflict after physical apply, asserts public write rejection, then explicit
Ensure recovery. These are deterministic correctness gates, not latency targets
or power-loss qualification.

`TestTypedGraphFoldRetiresSiblingKeepers` exercises warmed idle handles from the
same and separate managers, keeper release after fold, independent old-view
fetch, and refreshed public search. `TestLeafGenerationPackRelocationFollowsAcceptance`
forces actual collection-root packing through success, a pre-acceptance error,
and an accepted meta-wait error; it checks exactly-once relocation completion,
the visible root coordinates, and value preservation before fail-closed error
handling. These are lifetime/publication tests, not memory-peak evidence.
`TestTypedGraphPublicEnsureLateSiblingKeeper` pauses the first, still-unregistered
sibling warm across a nonempty fold, checks exact keeper-accounting release and
held-reader preservation, and keeps a same-base suffix-only control warmed.
`TestTypedGraphPublicFoldReclaimsWholeGenerationsWithoutPack` observes actual
whole-dead generation files and a no-op pack, requires public Fold to remove
those files, and checks current filtered search/materialization plus reopen.
The `TestLeafGenerationMaintenanceLimits*` cases reject oversized directory,
pager, manifest, and recoverable-snapshot inputs before indexing/GC work, inject
growth between plan and actual pack snapshot, and retain successful admitted
packing/GC plus reopen coverage. They verify per-phase footprint admission, not
a cumulative I/O quota.

`TestLeafGenerationPackAllocatorsUseInstalledSequenceAuthority` records the
floor-39 split-allocation regression and requires pack and live reservations to
produce 40 then 41 from one authority. The repeated db and caching allocator
tests require unique, monotonic, in-range reservations under concurrency and
retain exhaustion behavior. `TestCommandWALLeafOwnerSharesPackSequenceAuthority`
and `TestCommandWALLeafOwnerStablePrepareSharesSequenceAuthority` require the
production replay-inline owner to share the same authority with ordinary and
stable pack preparation, including the existing CommandWAL RID namespace.
`TestLeafGenerationPackSharedAuthorityInterleavesLiveChildAndReopens`
pauses at deterministic copy completion, creates a live lane-255 child, then
checks distinct IDs, unchanged live-child identity, successful no-replace
promotion, value readability, checkpoint, and clean reopen. The unsupported
owner tests install through the normal record-length/lane wrappers and require
both ordinary copy and stable preparation to fail before their staging-directory
creation seams are called. Existing promotion-authority tests continue to own
collision identity and no-replace cleanup coverage.

`TestRecoverableColumnAssetReplayStrictFloor` checks exact excluded identities,
strict equality retention, namespace mismatch, and disabled-floor behavior.
`TestRecoverableColumnAssetReplayFloorUnknownAuthority` checks missing/zero and
incompatible authority plus WAL-off rejection. `TestTypedGraphWorkEpochRepeatedMaintenance`
exercises eight real typed write/fold/GC cycles, reads every captured fallback
asset afterward, and reopens the same native directory with an unapplied typed
replacement command. These are bounded maintenance/replay gates, not a claim of
whole-database storage plateau or public mutable Minima activation.

`TestPrepareColumnPhysicalAssetRowsTypedGenerationPlacement` checks isolated
same-generation insert/update/delete/retry outputs, typed-image alignment, one
close/sync epoch, and rejection of an out-of-range deletion generation.
`TestPrepareTypedAssetFailedPrefixRetry` retains failed sync bytes unchanged while
retrying into a fresh file. `TestTypedSourceSecondStageOutputFailure` checks
second-stage failure preserves the old row/root and normal replay atomically
installs a separate output with complete reachability. `TestTypedGraphFoldProcessCut`
also requires complete bounded cleanup after each real process cut and reopen.
`TestTypedGraphWorkEpochEmptyDeniedOutput` proves actual fold byte denial before
first write leaves unchanged authority and can renew after zero-byte cleanup.
`TestColumnAssetGCEmptyConstructionPin` protects a live creator then reclaims
after release; `TestColumnAssetGCEmptyReferencedOrChanged` rejects referenced
emptiness and post-plan growth. `TestColumnAssetGCEmptyNonregularAndQuarantine`
preserves directories, symlinks, unknown names, and explicit empty quarantine.

Allocator tests `TestColumnAssetAllocatorReusesExhaustedHint` and
`TestColumnAssetAllocatorSmallCompleteBoundary` cover cold/exhausted hints and
fully occupied bounded ID selection. `TestColumnAssetAllocatorReusesOnlyAfterReaderAndExactGC`
holds a mapped reader, performs exact cleanup after release, then reuses the ID
at the same generation with checksum rejection of a stale ref.
`TestColumnAssetAllocatorHoleCollisionAndConcurrent` covers occupied nonregular
entries and concurrent exclusive creation. These are not public serving qualification.

Existing generic
producer tests keep their prior file placement and sync expectations. The eight
real maintenance cycles also assert equal-width retained column bytes stay below
the warm-cycle ceiling (further reclamation may shrink them) and cross-generation
mixed row debt stays bounded; pager/WAL growth is separate.

## Minima native-path contract (#4615)

`TestTypedGraphContiguousProjectionWorkspace` checks the internal synchronous
borrow, capped slice capacity, unchanged input, fixed allocation count, and
unchanged noncontiguous/reordered/error behavior. Allocation-only checks follow
the existing non-race convention; semantic checks still run under race.
`TestTypedGraphFoldConstructionWorkspaceAdmission` rejects overflow/degree,
planning and reciprocal logical workspace excess before graph allocation, while
admitting the existing 16K/M16 limits without treating EF as an allocation count.
These checks do not certify a process-wide heap bound or activate public serving.

`minima-native-execution.md` defines the target, not current mutable graph
support. `TreeDB/cmd/treedb_rag_benchmark/minima_bounded_test.go` and the existing
`TestMinima` suite check versioned bounded fixtures, frozen full-manifest
compatibility, rejection of measured native proof, and diagnostic-only
qualification boundaries, completed lifecycle evidence, and process-lifetime
high-water consistency. Native semantic counter validation belongs with its
actual producers in #4619; M0 cannot qualify any native proof. The Python `test_minima*runner.py` suites and
`scripts/bench_minima_qualification_test.sh` cover actual runner and bounded
process execution. The runbook records their commands.

Existing collection command-WAL indexed staging, typed-column replay without
checkpoint, and mutation-asset tests establish reuse boundaries. They do not
certify the future typed Minima overlay; #4616–#4619 must add typed admission,
replay, snapshot, fold/crash and public-route tests as those features land.

`TestStableLogicalObligationNamespace*` checks empty namespace replacement on a
shared physical token, unrelated-namespace retention, scope normalization and
copy ownership, malformed/overlapping scopes, missing/stale obligations, and
declining the whole-field completeness certificate. The DB
`TestCaptureDurableRootNamespaceScopeCannotBypassAppendFallback` covers empty
namespace registration alongside append evidence. The service
`TestServiceColumnGraphCrossCollectionClosure` exercises A→B→A rebuilds with a
held read view, current full fetch and ordinary reopen. The existing deferred
maintenance lifecycle/manager/crash tests remain regression gates.

`TestVectorIndexRebuildDrainsRawBarriersBeforeMutation` covers ordinary and
normalized, empty and populated rebuild capture/publication without barriers
under mutation ownership. `TestVectorIndexRebuildCheckpointPublicationHandoff`
forces checkpoint raw ownership during construction, proves unrelated raw
writes are not blocked by construction, and checks unchanged-source publication,
changed-source rejection and ordinary reopen. Normalized public search after
reopen additionally requires supported prepared-holder/namespace authority.
`TestVectorIndexRebuildReleasesRawWhileMutationIsBusy` forces mutation contention
at capture and final publication; unrelated raw commands must still progress.

### Internal mutable graph consumer (#4617)

`TestTypedGraphOverlay*` covers checked base/current lineage, insert/replacement/
delete visibility, cumulative physical bounds and the still-gated public route.
`TestTypedGraphInverse*` covers the optional mapped inverse, coordinate/LSN
validation, corruption and handle lifetime. `TestTypedGraphLocatorVisitorOwnership`
checks the shared borrowed lookup boundary and unchanged owning public results.
`TestTypedGraphReadOwnerDoesNotWaitForImmediatePublication` checks admission and
public search while an immediate writer is paused before publication, including
cold/stale local entries and a cold sibling handle.
`TestTypedGraphReadOwnerReusesExactLocalCatalog` proves warm owner and public
normalized SQ8/full-fetch calls do not persistently reload the catalog.
`TestTypedGraphReadOwnerRejectsStaleOrIncompleteLocalCatalog` covers pager,
system-root, commit, missing-base/roots and base-schema invalidation with a
single cold repair.
`TestTypedGraphReadOwnerCatalogMatchesPublishers` separately compares current
roots and captured base metadata/root maps to direct persistent loading after
initial build, upsert, delete, fold and rebuild.
`TestTypedGraphReadOwnerRetriesPublicationChangedDuringCapture`,
`TestTypedGraphReadOwnerInstallationGapWaiters`, and
`TestTypedGraphReadOwnerCloseWakesPublicationWaiter` cover coherent capture,
publication-gap notification, cancellation, and Close without polling or spins.
`TestTypedGraphPreparedFilterFinalIntersectionAndBounds` preserves complete
512/513/1,000/4,096/4,097 classification and large-leaf/small-intersection behavior;
`TestTypedGraphPreparedFilterDispersedQuality` supplies a separate 50,000-row
exact-oracle/ANN-recall engine diagnostic, not final Minima qualification.

`TestTypedGraphBaseFilter*` covers immutable predicate ownership, bounded current
binding, eligibility transitions, shadow overfetch, new IDs, output from the
current pin, closure and independent readers during publication. The separate
representative suffix residency test reports logical payload and signed GC heap
measurements without equating them. `BenchmarkTypedGraphBaseFilterBindingBoundaries`
separates cold setup, bounded binding, warm query and actual new-pin reads;
write/ack and final materialization are explicitly excluded from that benchmark.
Existing `TestNativeScalarPlanCache*` remains an affected guardrail for shared
scalar scanning. The maintained admission tests keep the suffix experimental
and distinguish the already-supported base-only prepared-pack route.

No public mutable serving/fold/crash readiness follows from these tests. M3 owns
installation and bounded lifecycle; M4/M5 own the final application evidence.

### Typed indexed-write substrate (#4616)

The collection `TestTypedMinima*` tests exercise an 8-D FP32 typed column,
three typed-row strings, two scalar indexes, and text postings:

- `TestTypedMinimaAdmissionValidation`: mismatched carriers/counts, invalid
  strings/vectors, duplicate IDs, and retained-field overlap reject.
- `TestTypedMinimaInsertAndReplay`: unsorted batch IDs, reconstructed output,
  scalar/text parity, and zero column-publication document extraction through
  command-WAL replay.
- `TestTypedMinimaGenericMutations`: existing update and delete APIs maintain
  indexes immediately after typed admission, without an explicit caller flush.
- `TestTypedMinimaReplacementReplayAndNoop`: replacement/no-op semantics,
  reusable caller buffers, changed scalar/text values, and typed update replay.
- `TestTypedMinimaInFlightPublicationMutation`: replace/delete while an asset
  publication is paused; old text postings must not survive the mutation.
- `TestTypedMinimaUniqueAdmission`: duplicate unique values and conflicting
  replacements reject without changing the prior indexed row.
- `TestTypedMinimaAdHocRuntimeAdmission`: separately registered document-based
  vector runtimes reject typed admission rather than introducing a JSON path.
- `TestTypedMinimaUnsupportedScalarSibling`: the new retained-payload/scalar
  capability does not broaden unsupported sibling storage layouts.
- `TestTypedMinimaReplaySemanticValidation`: schema and typed-value mismatches
  fail replay projection validation.
- `TestTypedMinimaVectorOnlyReplay`: vector-only schemas with no index, multiple
  indexes, or a column name distinct from its field path survive replay rather
  than being misclassified as the legacy single-index projection API.
- `TestTypedMinimaCrashAndPublicationCuts`: explicit `command_wal_durable`
  admission, subprocess exit after acknowledgement, pre-append failure, and
  post-sync ambiguity followed by normal reopen. Assigned command LSNs must be
  accounted for even when the append call returns an error.
- `TestTypedMinimaCatalogPostSyncAmbiguity`: collection creation classifies a
  post-sync failure using the actual published intent and recovers on reopen.
- `TestTypedMinimaAmbiguousMutationNotRetried`: commit ambiguity takes precedence
  over a wrapped retryable mutation error, preventing duplicate execution.

The frame-copy replay fixtures preserve a checkpointed catalog and replay subsequent
command frames through normal open. They prove command replay, not modeled or
physical power-loss survival. The subprocess exit case is `process-crash`
evidence; neither it nor an injected sync cut is physical power-loss proof.
Typed vector authority and asset readback do not
by themselves prove mutable prepared-graph serving or the Minima service route;
those remain #4617–#4619 gates.

## 0. Durability Evidence Taxonomy

Durability evidence uses exactly one of these labels:

| Label | What it proves | What it does not prove |
|---|---|---|
| `clean-reopen` | data is readable after an orderly close and normal public reopen | crash or power-loss survival |
| `process-crash` | data is readable after abrupt process termination while the OS/kernel remains alive | loss of volatile kernel/device state |
| `modeled-power-loss` | a deterministic stable-only image, with volatile bytes and unsynced directory mutations discarded, is accepted or rejected through normal public `Open` as specified | physical block-device behavior outside the model |
| `block-device-power-loss` | the guarded device fault harness removed volatile device writes and recovered through normal public open | behavior on devices/configurations outside the recorded harness |

`os.Exit`, SIGKILL, subprocess termination, and failure to call `Close` are
`process-crash` evidence. They are never described as power-loss evidence.

### 0.1 Deterministic stable/volatile oracle

`TreeDB/internal/powerlossoracle` is shared test infrastructure. It maintains
separate process-visible and stable bytes, inode-aware names, file-sync
promotion, and directory-sync promotion for create/rename/unlink. `Crash`
discards volatile state. `MaterializeStable` writes only stable bytes reachable
through stable directory entries. Materialization necessarily creates new host
file IDs, so the model carries the captured file and directory identities into
that image through a scoped, test-only physical-object adapter. The adapter is
keyed by the recreated object's native identity, not by pathname: retained
handles and aliases follow the object, while a later path replacement does not
inherit the old identity. `internal/powerlossreopen.Stable` installs that view,
passes the directory to normal public read-write or read-only `treedb.Open`,
then releases it after close; it does not invoke recovery internals.

The canonical cut-point enumeration command is:

```sh
GOWORK=off go test ./TreeDB -run '^TestPowerLossOracleEnumerateCutPoints$' -v -count=1
```

Failure output includes `seed=3674` and the stable cut-point name. The command
enumerates cuts before/after dependency append, userspace flush, dependency
file sync, new-file directory sync, index-data sync, durable-root-record write,
meta write/sync, applied-LSN advancement, WAL/asset unlink, and deletion
directory sync. The stable identifiers `before-publication-seal-write` and
`after-publication-seal-write` bracket the exact `DurableRootRecordV1` page
write. The following index sync makes that record and its COW closure stable;
only the later alternate-meta write and sync make it recovery-selectable.

The full-index mapped-write barrier uses the pager's existing platform policy:
Linux requires one retained-file data barrier rather than a preceding `MS_SYNC`
per dirty chunk; flush-only and non-Linux mapped-range fences remain required.
`pager.TestDirtyChunkSyncRetainsFlushAndRetriesStableFileBarrier` checks selective
flush bookkeeping without a file barrier, exact retained-file identity, one file
barrier per durable attempt, restoration after a failed barrier, and byte readback
after retry. Existing `TestSyncIndexDataWithStableFile*`,
`TestSyncPagesWithStableFileUsesPinnedIdentityAfterPathReplacement` and
`TestFlushDirtyChunksFromRetainsLowerChunksForFinalSync` cover retained handles
after close, nil-target cut ordering, path replacement and final dirty drainage.
These guards do not count mapped syscalls or prove physical power-loss survival.
An external Linux syscall comparison must show durable dirty-chunk `msync` calls
on the prior source and none on the revised source, while flush-only calls still
issue them. The crash-image oracle, platform CI and matched unprofiled load and
checkpoint campaigns remain separate acceptance gates.

`db.TestRebindDurableRootSnapshotV1PreservesBothSlotsAndExactTargetIdentity`
proves that ordinary copied dependencies fail before explicit snapshot rebind,
a cut immediately before the first rebound-meta write leaves the installed
index byte-for-byte unchanged, both distinct slots recover afterward, the older
slot remains usable when the newest meta is corrupt, and a later byte-identical
dependency replacement is still rejected.
`db.TestDurableRootPublicLayoutDictionaryDependencyReopenAndNewestSlotFallback`
proves that a main DB resolves a transitive dictionary identity against the
public sibling `dictdb` layout and falls back when only the newest slot's exact
dependency identity is replaced. `raftfsm.TestRaftSnapshotV1*` exercises rebind
through the production archive installer, including value-log pointers and side
stores.

### 0.2 Bounded adversarial crash-image generation

`powerlossoracle.GenerateVariants` expands a registered cut into a bounded,
deterministic set of legal partial writebacks. The committed limit is 256
images per cut. Generation fails closed when a requested family is unknown,
inapplicable, silently omitted, or would exceed that limit. Logical resource
IDs, cut occurrence, family, and format boundary determine stable variant IDs
and seeds; host paths and input/map iteration order do not. Stable ID ordering
also gives balanced deterministic sharding through `ShardVariants`. Every
generated variant carries one of the ledger result categories, and generation
fails when an applicable family has no declared expected result.

The registered families cover the synced baseline, target-meta-only and
one-missing-dependency ordering controls, file-data/directory-namespace
mismatches for newly created names, complete writeback, old-live-page reuse,
and torn format-aware ranges. The generator recognizes meta, root-record,
freelist, and index-page labels, rejects unknown labels, duplicate selectors,
and ranges outside actual declared changes. The current public-Open integration
witness exercises a real changed meta checksum boundary. Freelist and
index-page labels have generator coverage only unless a production cut
registers their corresponding changed bytes. The #3679 publication-seal event
now exposes the root record's exact index-file page range as such a production
boundary. A witness must register that range in its `CutSpec` before claiming
public-reopen coverage for a torn root-record image.

Every generated image used by the integration witnesses is materialized and
passed to normal public `Open`. A single image can be replayed without a host
path by copying its exact `TREEDB_POWERLOSS_CUT_ID`,
`TREEDB_POWERLOSS_VARIANT_ID`, and `TREEDB_POWERLOSS_SEED` values into the
recorded test command. `TreeDB/testdata/power_loss_counterexamples.json` is the
machine-readable invariant ledger. Schema v2 separately records the declared
contract result, the observed public-Open result, and any allowed exact named
invariant or typed `errors.Is`/`errors.As` sentinel. Replay package, test, cut,
variant, and seed are structured fields from which the shell command is
generated. A code-owned real-witness registry is checked bidirectionally, so an
entry cannot disappear or be added only in JSON. Validation also fails when
requested family coverage or the maximum changes. A resolved entry remains in
the retained inventory with observed equal to expected and no known violation.

The focused generator and ledger command is:

```sh
GOWORK=off go test ./TreeDB/internal/powerlossoracle ./TreeDB -run 'GenerateVariants|ReplaySelector|CounterexampleLedger|AdversarialNewFileNamespaceMismatch|CounterexampleNewMetaMissingClosure' -v -count=1
```

Verbose output reports generated image count, generation runtime, peak
materialized temporary-image bytes, family coverage, and committed-fixture
shard balance. These measurements bound test-harness cost; they are not
production throughput evidence.

`TestProductionWitnessRegistryExactlyMatchesCanonicalPolicy` is the independent
producer-side coverage gate for DUR-03 #3677. Its handwritten 16-row registry
points to package-local tests that execute the real producer-owned capture
paths; a separate literal 20-row registry keeps adjacent, rebuildable, legacy,
and separate-durability fields negative. `TestAllKindAuthorityGeneratesStableTargetAndAllButOneVariants`
maps the same 16 fields independently into the #3717 generator and proves the
deterministic 19-image shape: synced baseline, target-meta-only, sixteen
one-missing-dependency images, and full writeback. That generator test does not
materialize or reopen those 19 images through public `Open` and does not by
itself prove that a root candidate consumes the captured resource sets; the
#3679 publication and public-open integration witnesses own that proof.

### 0.3 Counterexample-to-conformance map

Counterexamples use real production calls and bytes. Every generated image is
classified through normal public open/read as an exact old root, new root,
suffix discard, typed sentinel, or successful-open named corruption. A model
invariant such as missing namespace durability may coexist with an old-root
public outcome; the ledger keeps those facts orthogonal. Known deviations pass
only when the exact registered witness reproduces its structured violation, so
they do not keep the branch red while the production graph is in progress.
Later children update these stable test names rather than duplicating them.

| Stable scenario | Current counterexample | Positive-conformance owner |
|---|---|---|
| `TestPowerLossOracleCounterexampleNewMetaMissingClosure` | resolved: the target meta is written only after the durable-root record, manifest, index, value-log, and outer-leaf closure is stable; incomplete candidates fall back | DUR-09 #3679 |
| `TestPowerLossOracleCounterexampleRecoverablePageReuse` | resolved for synchronous publication: the root-reuse admission fence prevents reuse from racing older-root capture, and both durable slots retain their exact COW generations | DUR-04 #3678 and DUR-09 #3679; maintenance horizon remains DUR-08 #3681 |
| `TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID` | relaxed command-WAL external-RID replay applies a checksum-valid frame with a missing RID | DUR-05 #3718 |
| `TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage` | resolved for activation: cleanup retains the command-WAL source until AppliedCommandLSN coverage is stable; complete cleanup convergence remains downstream | DUR-06 #3680; convergence remains DUR-07 #3682 and DUR-08 #3681 |
| `TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot` | resolved: cached Checkpoint and sync boundaries publish only the complete final root, never an intermediate chunk | DUR-06 #3680 |

`TestPowerLossOracleFixtureInventoryReopensStableOnly` covers inline values,
`ValueLog.PointerThreshold=1` forced pointers, forced outer leaves, combined
value pointers plus outer leaves, multi-lane value-log configuration, and
segment rotation. Every fixture is checkpointed, captured, materialized from
stable state only, and reopened through normal public read-only `Open`.

Publication metadata reuse (#4627) has additional production DB witnesses:

- `TestDependencyManifestV1DeterministicMultiPageRoundTrip` checks that reference
  preparation allocates no pages and matches actual materialization, including
  the last legal page interval, rejected overflow, and a partial sink failure.
  `TestDependencyManifestV1ReferenceEmptyAndNil` distinguishes a valid manifest
  with no entries from an uninitialized manifest and preserves nil-sink errors.
- `TestStableResourceSetDependencyManifestEncodingReusesRetainedEntries` bounds
  cached assembly allocations to payload/reference storage, and
  `TestStableResourceSetDependencyManifestSurvivesCoalescingAndRelease` checks
  immutable metadata after coalescing and last-pin release. The multi-page
  round trip also checks constructor/accessor ownership and normalization errors.
- `TestDurableRootMetadataReuseLowPlacementAndFallback4627` holds an old
  snapshot through publication, verifies bounded-fixture high-water stability
  and low metadata/auxiliary placement, decodes both slots, and reopens the
  exact older slot after corrupting the newest meta.
- `TestDurableRootMetadataReuseMultipageManifest4627` reuses a two-page,
  24-dependency manifest and verifies all values after ordinary and fallback
  reopen.
- `TestDurableRootMetadataReuseProcessCut4627` exits without `Close` before a
  reused seal write and after acknowledged publication, then validates the
  old/new tuple and both retained slots. This is process death, not a substitute
  for the power-loss oracle's stable-writeback permutations.
- Freelist-level `4627` tests cover capability-derived free capacity, observed
  auxiliary claim races, arbitrary-sink rollback, real pager failed-tail gaps,
  low placement sizing and bounded search starvation. They do not prove every
  workload plateaus: requests larger than a fitting chunk retain tail fallback.
- Metadata sizing rejects impossible runs without allocating and leaves a
  losing claimant's staged state intact. Emission retains the freshly copied
  selected path but isolates dirty siblings; success and partial-write failure
  tests verify retained branches' identities, checksums and contents unchanged.
- The vacuum M0 fixture manufactures explicit retired user-tree debt with
  fixed append-only compactions, recorded in its parameters. Its minimum 50%
  reclaimable-page and 40% shrink checks must not depend on metadata leakage.

Pure scenario-validator unit tests under
`TreeDB/internal/powerlossoracle/scenario_test.go` cover durable acknowledgement
loss, relaxed non-suffix loss, invalid selected roots, and key-state mismatch.

The oracle asserts complete old-or-new roots, full dependency/pointer closure,
freelist/live-page disjointness, contiguous command replay, durable
acknowledgement survival, relaxed suffix-only loss, and no early source
deletion. Production packages do not import the oracle model. They emit only
coarse durability-boundary events through `internal/durabilitycut`; when no test
observer is installed the seam is an atomic load and nil branch, with path
collection skipped.

## 1. Pointer Durability and Reopen

Invariant:
- Value pointers survive close/reopen and still resolve correctly.

Coverage:
- `TreeDB/reopen_verify_test.go`:
  - `TestReopenVerify_WALOn_Checkpoint`
  - `TestReopenVerify_WALOn_WriteSync`
  - `TestReopenVerify_WALOn_Checkpoint_CompressionModes`
  - `TestReopenVerify_WALOff_NoJournal`
  - `TestReopenVerify_IndexColumnarLeaves`
  - `TestReopenVerify_AdaptiveLeafEncoding_MixedEncodingPages`
  - `TestReopenVerify_InternalBaseDelta_WALOn_Checkpoint`
  - `TestValuePlacement_PerDomainThreshold_ReopenDurability`

- `TreeDB/caching/vlog_raw_read_amplification_test.go`:
  - `TestValueLogRawFrames_DurableSizingAndFallback` checks persisted raw grouping
    for ordinary and queued planners, small/mixed/oversized values, configured
    byte targets, explicit off and auto raw selection, missing-dictionary and
    writer-capability fallback, exact reads after writer close/reopen, and
    checksum rejection of changed persisted payloads. It also characterizes the
    separate auto block-bootstrap rejection policy, which can persist raw frames
    exceeding the chooser-selected raw byte target.
- `TreeDB/caching/vlog_compression_selector_test.go`:
  - `TestChooseValueLogRawWriteK_ExternalCommandWAL` applies the byte bound when
    external command-WAL durability disables the cached journal.
  - `TestChooseValueLogRawWriteK_WALOffRawPolicyUnchanged` preserves ingest K.
  - `TestChooseValueLogRawWriteK_LiveLeafLogCapsGroupedFramesForColdReads`
    preserves the dedicated leaf-lane cap.

- `TreeDB/raw_frame_command_wal_test.go`:
  - `TestPublicCommandWALRawFrames_DurableGrouping` uses the public command-WAL
    durable profile, syncs a forced-pointer 4 KiB raw batch, checks persisted
    records hold one value per frame, and verifies exact values after reopen.
  - `TestPublicCompressedFrames_OrdinaryGroupingReopen` checks mixed-size
    auto/balanced frames through ordinary public writes, checkpoint, close and
    reopen in command-WAL durable and no-WAL fast profiles, including owned
    rereads and oversized singletons.
- `TreeDB/caching/vlog_block_read_amplification_test.go`:
  - `TestValueLogBlockFrames_BoundedOrdinaryPayload` checks actual decoded-byte
    bounds, pointer order and emitted-K counters through direct, worker-prepared
    and queued ordinary block paths, including raw compression rejection.
  - `TestValueLogBlockFrameBoundary` covers exact/empty/mixed/oversized payloads
    and K ceilings; `TestValueLogBlockRawLimitEligibility` preserves excluded
    leaf/template/retained and explicit compression-policy paths.
  - `TestAppendValueLog_RestoresWriterPolicyAfterBoundedHandoff` injects a
    competing same-lane policy change at a variable-span handoff and checks the
    resumed batch's mode/codec/keep policy and pointer order. The fake writer
    controls the interleaving; this is not a physical scheduler timing proof.

## 2. Recovery Coherence

Invariant:
- Open-time recovery replays commit logs coherently and cleans replayed logs.

Coverage:
- `TreeDB/recovery_spec_test.go`:
  - `TestCrashRecovery_WALReplayIsCoherent`
  - `TestCrashRecovery_DeleteRangeReplaysCorrectKeys`
  - `TestCrashRecovery_DurabilityTiers`
  - `TestRecovery_RIDJoinReplaysValueLog`
  - `TestRecovery_MultiLaneOrdering`
  - `TestRecovery_PartialCommitBatchIgnored`
  - `TestRecovery_TruncatedCommitLogRecord`
  - `TestRecovery_TruncatedValueLogRecord`
  - `TestRecovery_MissingDictFails`

Evidence label: `process-crash` for subprocess/`os.Exit` cases and
`clean-reopen` for orderly reopen cases. These tests are not
`modeled-power-loss` evidence.

## 2.1 Active Command-Frame V2 Recovery Boundary

Invariant:

- V2 durability class and canonical RID-fence bytes decode strictly; V1,
  unknown classes/versions, malformed fences, and complete dependency defects
  fail closed.
- Complete and terminal compressed V2 segment records fail closed with the
  typed unsupported-compression error; a torn compressed payload is never
  interpreted as an uncompressed identity header.
- The highest complete durable frame or barrier establishes the horizon.
  Defects through it cause no mutation; only one relaxed suffix above it is
  discardable.
- Physical repair is reverse-LSN, directory durable before anchor truncation,
  retryable at every registered cut, and read-only inspection is non-mutating.
- Production command-WAL append and reopen use only V2 when
  `command_wal_v2` is active; V1 requires a pre-alpha rebuild.

Coverage:

- `TreeDB/internal/commitlog/command_frame_v2_test.go`
- `TreeDB/internal/commitlog/testdata/command_wal_v2_*.hex`
- `TreeDB/db/command_wal_v2_classification_test.go`
- `TreeDB/db/command_wal_v2_physical_test.go`

The physical suffix-repair test captures real stable models immediately before
and after the first non-anchor dependency-file sync, then materializes both
through #3717 `powerlossoracle.CutSpec` stable variant IDs and retries from a
fresh directory scan. Separate deterministic cuts cover non-anchor sync,
unlink, deletion-directory sync, and final anchor sync. This is crash-model
unit evidence for the physical repair boundary. Public activation coverage is
provided by the command-WAL durable-prefix tests.

## 2.2 Publication Readability

Invariant:
- Published commit state exposes roots, system-root collection catalog metadata,
  value-log pointers, snapshots, and `AppliedCommandLSN` as a readable state
  tuple. Bounded pre-commit catalog EOF is retriable; post-commit readability
  failure is commit-ambiguous.
- The #2026 local closeout state is final after #3382: collection catalog
  publication/readability, forced-pointer value-log readability, current-writable
  value-log read barriers, nativewire forced-pointer publication, and external
  nativewire YCSB diagnostic evidence are covered locally. Distributed HA,
  read-index/snapshot/rejoin, and routing/fanout remain owned by #3044, #3045,
  and #3046.

Coverage:
- `TreeDB/docs/spec/publication-readability-3245.md`
- `TreeDB/collections/publication_readability_test.go`:
  - `TestCollectionCatalogEOFInsertBatchPreCommitRetryUsesCatalogLoadFaults`
  - `TestCollectionCatalogEOFInsertBatchRetryExhaustionIncludesCatalogContext`
  - `TestCollectionCatalogEOFInsertBatchPostCommitReturnsCommitAmbiguous`
  - `TestCollectionCatalogRootEOFInsertBatchPostCommitReturnsCommitAmbiguous`
  - `TestCollectionCatalogCachedForcedPointersReadableFromFreshSnapshot`
  - `TestCollectionCatalogCurrentWritableValueLogReadBarrier`
- `TreeDB/caching/vlog_current_segment_readbarrier_test.go`:
  - `TestCachedModeValueLogPointerReadBarrierResolvesBackendRootRead`
- `TreeDB/nativewire/forced_pointer_readability_test.go`:
  - `TestNativewireYCSBForcedPointerPublicationReadability`
  - `TestNativewireYCSBCurrentWritableValueLogReadBarrier`
- `docs/benchmarks/nativewire_ycsb_closeout_2026-06-30.md`:
  - current-head external 100k and 1M nativewire load evidence with zero
    `INSERT_ERROR`.
- `docs/benchmarks/nativewire_ycsb_insert_error_classification_2026-06-30.md`:
  - diagnostic gate with `TREEDB_YCSB_LOG_ERRORS=1`, `-p silence=false`, empty
    stderr, zero raw matches for `INSERT_ERROR`, `EOF`, `ambiguous`, `panic`,
    `fatal`, `ERROR`, and `Failed`, and classification of the invalid
    intermediate artifact as non-current TreeDB publication evidence.
- `scripts/nativewire_ycsb_diagnostic.sh`:
  - reusable diagnostic gate that exits nonzero on detected load failures,
    `INSERT_ERROR`, or raw error-scan matches.

## 3. Value-Log Reachability GC

Invariant:
- Fully unreferenced segments are removable; referenced segments are preserved.

Coverage:
- `TreeDB/db/vlog_gc_test.go`:
  - `TestValueLogGC_RemovesUnreferencedSegment`
- `TreeDB/db/durable_root_tracker_repair_test.go`:
  - `TestDurableRootCandidateScanRepairsReferenceTracker`
  - `TestDurableRootCandidateScanColdCollectionAttachmentRepairsTracker`
  - `TestDurableRootCandidateScanAbortPreservesReferenceTracker`
  - `TestDurableRootCandidateScanRejectsMismatchedEvidence`
  - `TestDurableRootCandidateScanDoesNotApplyDeltaTwice`
  - Covers exact empty/nonempty counts, candidate identity, activation ordering,
    reopen, and avoiding repeated fallback scans on subsequent ordinary writes.

## 4. Value-Log Rewrite Correctness

Invariant:
- Offline rewrite preserves values while reducing/replacing old segments.
- Physical segment/chunk live bytes include each live record's CRC exactly
  once. All-live sealed outputs have no CRC-only rewrite debt; real dead-record
  spans and recoverable-root retention remain distinguishable.

Coverage:
- `TreeDB/db/vlog_physical_live_bytes_test.go`:
  - `TestValueLogPhysicalLiveBytes_FileSpanOracle`: actual file spans for
    ordinary/grouped, hinted/header-fallback and mixed live/dead records,
    direct maintenance projection, chunk accounting and exhaustive admission.
  - `TestValueLogPhysicalLiveBytes_MemoizedProtectedRoots`: one physical frame
    shared across roots and repeated memoized scans, preserving dedupe counts.
  - `TestValueLogPhysicalLiveBytes_ExhaustiveRolloverReopen`: forced small
    output rollover, retained-root debt, durable-horizon settling, exhaustive
    convergence and checksum-verified reopen.
- `TreeDB/db/vlog_rewrite_test.go`:
  - `TestValueLogRewriteOffline_RewritesAndShrinks`

## 5. Leaf Encoding Density and Regressions

Invariant:
- Prefix/packed/columnar optimizations do not silently regress key density beyond guardrails.

Coverage:
- `TreeDB/node/leaf_density_test.go`:
  - `TestLeafPrefixCompression_IncreasesPageDensity_PointerEntries`
  - `TestLeafPackedValuePtr_IncreasesPageDensity_PointerEntries`
  - `TestLeafPrefixCompression_IncreasesPageDensity_InlineEntries`
  - `TestLeafColumnarPrefixCompression_IncreasesPageDensity_PointerEntries`
  - `TestLeafColumnar_DoesNotReducePageDensity_PointerEntries`
  - `TestLeafColumnarPrefixPacked_PointerDensityWithinTolerance`
  - `TestLeafAdaptiveEncoding_DensityFixture_HighPrefixInline`
  - `TestLeafAdaptiveEncoding_DensityFixture_PointerLowPrefix`
  - `TestLeafBuilder_AdaptiveEncoding_HeuristicDeterminism`
  - `BenchmarkLeafPageDensity`

## 6. Durability/Profile Defaults

Authoritative production `no_wal_fast` boundaries (#4980) are covered by
`TestNoWALFastBoundaryDrainsAcknowledgedCollectionBuffers` (separate managers,
indexed/unindexed buffers, checkpoint/maintenance, point/batch/conditional/no-op
sync and clean close reopened from the existing power-loss oracle),
`TestNoWALCheckpointWaitsForIndexedAsyncPublisher`,
`TestCollectionVectorIndexNativeRootCheckpointPersistsDirtyGraph`,
`TestNoWALSyncBarrierDrainAndFailure` (observer/drain outside teardown and failed
drain prevents the later mutation), and
`TestNoWALFastPublicNoopSyncSealsEarlierWrites`.
`TestNoWALReadOnlyConditionalSyncValidatesAfterDrain` preserves read conflicts.
`TestNoWALSyncBarrierDoesNotChasePostDrainWrites` and
`TestNoWALCheckpointCoversPriorCollectionWritesWithoutChasingRefill` bound the
registered-hook frontier. `TestNoWALCheckpointBoundsActiveIndexedAsyncFrontier`
checks prepared-worker completion and durable pre-entry ACKs;
`TestNoWALIndexedAsyncDrainDefersSiblingWorkAndResumesAfterLastWaiter` checks
sibling exclusion, overlapping drains, and deferred work resumption.
`TestNoWALVolatileCollectionAckDoesNotImplyGlobalOrdinaryPrefix` documents the
autonomous-seal limitation across independently buffered ordinary ACKs.
`TestApplyProfile_FastAndExplicitUnsafeCeiling` verifies the benchmark production
fast/explicit unsafe mapping and integrity/mmap resolution.
See [the no-WAL audit](no-wal-fast-audit.md) for source boundaries and existing
reopen/GC/closure-oracle qualification. These checks make no throughput claim.


Invariant:
- Durability modes and profile bundles map to expected policy knobs.

Coverage:
- `TreeDB/vlog_default_threshold_test.go`
- `TreeDB/profiles_test.go`
- `TreeDB/unsafe_options_test.go`
- `TreeDB/force_value_log_test.go`:
  - `TestValuePlacement_PerDomainThreshold_Respected`
- `TreeDB/db/api_test.go`:
  - `TestValuePlacement_PerDomainThreshold_DefaultFallback`

### 6.1 Command-WAL durable-write ordering and syscall ledger

Invariant:
- external value-log bytes pass their required durability boundary before the
  command frame; the complete command frame passes the command-WAL file-sync
  boundary before cached publication; and an empty `WriteSync` covers pending
  value-log lanes before its command-WAL barrier.
- logical append/flush/sync counters remain distinguishable from actual writer
  `write`/`writev`, file-sync, and directory-sync hook counts.

Coverage:
- `TreeDB/db/command_wal_raw_test.go`:
  - `TestFlushCommandWALBarrierOrdersExternalRefsBeforeCommandWAL`
- `TreeDB/db/command_wal_recovery_test.go`:
  - `TestCommandWALCrashAfterFrameBeforeRootPublishRecovers`
- `TreeDB/command_wal_public_test.go`:
  - `TestPublicCommandWALStateShapedDurabilityLedger`
  - `TestPublicCommandWALBatchWriteSyncExternalRefOrderingPhaseStats`
  - `TestPublicCommandWALPointerEmptyWriteSyncSweepsPriorUnsyncedWrite`
  - `TestPublicCommandWALWriteThenDirtyWriteSyncDurabilityLedger`
  - `BenchmarkPublicCommandWALDurableTinyBatchWriteSync`
- `TreeDB/caching/value_log_appender_test.go`:
  - `TestCachingValueLogExternalRefFlusherSyncsRotatedSegments`
  - `TestCachingValueLogExternalRefFlusherAccountsForRotatedCommandFrameSegment`
- `TreeDB/internal/commitlog/commitlog_test.go` and
  `TreeDB/internal/valuelog/valuelog_test.go`:
  deterministic file/directory sync-hook count and rotation tests.

## 7. Maintenance/Compaction Behavior

Invariant:
- Index rewrite/vacuum paths preserve data and handle pinned snapshots safely.
- Online/offline vacuum preserves configured system-leaf placement and binds
  appended leaf resources and their manifest authority in each replacement slot.
- Full storage compaction preserves value visibility, removes reachable debt only
  through the documented lifecycle, serializes backend maintenance phases, and
  keeps cached-mode value-log writers from reusing backend-created segments.

Coverage:
- `TreeDB/db/compact_index_test.go`
- `TreeDB/db/compact_index_sequential_alloc_test.go`
- `TreeDB/db/vacuum_online_swap_test.go`
- `TreeDB/db/vacuum_system_leaf_policy_test.go`: system catalog warm writes,
  exact manifest membership, and offline swap recovery.
- `TreeDB/db/compact_storage_test.go`
  - `TestCompactStorageHoldsMaintenanceLockAcrossPhases`
- `TreeDB/db/compact_storage_audit_test.go`
  - shared-walk call counts, snapshot-basis revalidation, structural reuse,
    grouped pointer accounting, and legacy planner parity
- `TreeDB/compact_storage_test.go`
  - `TestCompactStorageFullPacksLeafGenerationDebtOffline`
  - `TestCompactStorageCachedDeletesZeroByteValueLogFiles`
- `TreeDB/compact_storage_persistence_test.go`
  - `TestCompactStorageExhaustiveCommandWALRandom4KOffline`: dependency-free
    public durable command-WAL fixture with 32-byte keys, random 4-KiB values,
    synced spread updates, cached-owner close/reopen, and exclusive backend
    Exhaustive compaction with synced phases. Both the 20,000-key characterization
    and original 100,000-key size run two maintenance/GC passes and verify every
    final value plus 10,000 missing keys through independent read-only opens.
    The smaller case also checks partial-phase failure, cleanup, and retry when
    packed dictionary authority is unavailable. `-short` skips the original size.
- `TreeDB/side_store_lookups_test.go`
  - read-only dictionary owners retain lookups without writes; stable capture
    succeeds on supported owners and returns typed namespace refusal without
    authority on reopened read-only Windows owners
- `TreeDB/db/leaf_generation_pack_authority_test.go`
  - dictionary closure lifetime, rollback after partial install, and post-install
    failure cleanup
- `TreeDB/compact_storage_cached_internal_test.go`
  - `TestCompactStorageCachedAdvancesWritersPastBackendSegments`

## 8. Required Checks for Format/Behavior Changes

When changing on-disk format, replay behavior, or pointer lifecycle:

1. update `TreeDB/docs/spec/storage-format.md` and any affected spec sections,
2. update/add tests in the corresponding invariant section above,
3. run relevant package tests, minimum:
   - `go test ./TreeDB/...`
4. for leaf-encoding work, run:
   - `go test ./TreeDB/node -run '^$' -bench BenchmarkLeafPageDensity -benchmem -count=1`

## 9. Documentation Terminology Integrity

Invariant:
- TreeDB docs consistently describe persistent value-log storage and avoid
  legacy alternate value-store terminology.

Coverage:
- `TreeDB/docs/docs_lint_test.go`:
  - docs terminology lint test

## 10. Cached Reads Include Cached Writes

Invariant:
- In cached mode, snapshots and iterators include writes buffered in memtables and are snapshot-isolated (writes after acquisition are not visible).

Coverage:
- `TreeDB/snapshot_cached_writes_test.go`:
  - `TestAcquireSnapshot_IncludesCachedWrites`
  - `TestAcquireSnapshot_IncludesCachedWrites_ValuePointers`
- `TreeDB/caching/snapshot_test.go`:
  - `TestIteratorSnapshotIsolation`
  - `TestSnapshotGet_PublishedAppendOwnsResultWithoutEntryProbe`: published
    owned reads use one append lookup without an entry pre-read; empty, small,
    and larger-than-scratch results survive caller mutation, later reads and close.
  - `TestSnapshotGet_BackendPublishedMissSkipsEntryProbe`: published backend
    misses use one append lookup without materializing a leaf entry.
  - `TestSnapshotGet_NilReceiverReturnsClosed`: owned nil-receiver reads retain
    `ErrClosed`, distinct from append's nil-receiver miss behavior.
  - `TestSnapshotGetAppend_RootBoundPublishedMissDoesNotFallbackToDefaultRoot`:
    both owned and append reads preserve the pinned root on a miss.
- `TreeDB/caching/snapshot_pool_test.go`:
  - `TestAcquireSnapshot_CachedPathConcurrentAcquireCloseWithWrites`: alternating
    owned and append reads with concurrent acquisition, closure and queued writes.
- `TreeDB/caching/snapshot_owned_read_bench_test.go`:
  - `BenchmarkSnapshotPublishedOwnedRead`: warmed published outer-leaf reads
    through a cached snapshot with an unrelated queued write; owned `Get` and
    reused-destination `GetAppend` allocation costs exclude setup/checkpoint.
    This microbenchmark does not qualify the concurrent generic workload.
- `TreeDB/caching/iterator_cached_writes_test.go`:
  - `TestIterator_IncludesCachedWrites_SnapshotIsolated`
  - `TestIterator_IncludesCachedWrites_ValuePointers`
- `TreeDB/caching/reverse_iterator_cached_writes_test.go`:
  - `TestReverseIterator_IncludesCachedWrites_SnapshotIsolated`
- `kvstore/adapters/treedb/read_snapshot_cached_writes_test.go`
- Unified-bench correctness guardrail: `cmd/unified_bench/read_snapshot_guardrail_test.go` and `BenchConfig.ReadRequireHit`
- `TreeDB/caching/memtable_adaptive_test.go`:
  - `TestAdaptiveMemtableMode_EarlyRotationKeepsSampling`: snapshot, iterator,
    and ordinary rotations retain low-data observation, reach mixed/sequential
    selection, and stop sampling after a sufficient append-only choice.
  - `TestAdaptiveMemtableMode_ExplicitModesDoNotObserve`: fixed modes never
    enable adaptive observation or change mode during rotation.
  - `TestAdaptiveMemtableMode_StartsSamplingWithoutByteWarmup`: small flush
    thresholds and adaptive aliases start observation before any decision.
- `TreeDB/caching/memtable_adaptive_public_bench_test.go`:
  - `BenchmarkAdaptiveMVCCSnapshotCommandWAL`: public MVCC commits and early
    snapshots under relaxed/durable command-WAL profiles, with explicit-mode
    controls, allocation counts, selection proof, and WAL counters.

## 10.1 Target Conditional Raw KV Revisions And Transactions

This section owns the planned verification gates for the target native
conditional raw-KV feature tracked by
https://github.com/snissn/gomap/issues/3420. The feature is not complete until
entry revisions, command-WAL recovery, and conditional transactions are all
implemented on the native raw write/read path.

Invariant:
- Raw KV entry revisions are first-class metadata on the visible entry path.
  They are carried through memtables, batch entries, leaf construction, command
  WAL replay, recovery, and future Raft apply semantics with the value or
  tombstone.
- All write paths share one persisted raw-KV revision domain. Tests must prove
  cached, backend-only, command-WAL, reopen/replay, and future Raft authorities
  seed above the durable revision floor or fail closed before versioned
  visibility.
- A sidecar-per-write metadata tree is rejected for this feature because it
  creates a second hot-path lookup/write and an independent durability boundary.
- Conditional transactions validate recorded point-read preconditions against
  committed intervening writes and commit disjoint writes without serializing
  whole transaction bodies behind a coarse global lock.
- Unsupported range reads and `DeleteRange` inside conditional transactions fail
  closed until deterministic range guards are implemented.

Required coverage:
- `TestRawKVEntryRevisionMonotonicSetOverwriteDeleteReinsert`
- `TestRawKVVersionedSnapshotRevisionStable`
- `TestRawKVEntryRevisionVisibleThroughCachedMemtable`
- `TestRawKVEntryRevisionSurvivesReopenAndCommandWALReplay`
- `TestRawKVEntryRevisionPreservedByLeafRebuildAndCompaction`
- `TestConditionalTxnConflictsOnExistingReadOverwrite`
- `TestConditionalTxnConflictsOnExistingReadDelete`
- `TestConditionalTxnConflictsOnAbsentReadInsert`
- `TestConditionalTxnConflictsOnAbsentReadInsertDeleteCycle`
- `TestConditionalTxnAllowsDisjointConcurrentCommit`
- `TestConditionalTxnRejectsUnsupportedRangeGuard`
- `TestConditionalTxnCommandWALReplayMatchesLiveRevisionContract`
- `TestAdapterReadinessUsesNativeRevisionsAndConditionalConflicts`
- `TestAdapterReadinessCommandWALReopenPreservesRevisionAndFailsClosedConditional`
- `TestResolveOptionsRejectsUnsupportedAdapterFeatures`
- `TestOpenRejectsUnsupportedAdapterFeaturesBeforeCreatingDB`

Required performance evidence:
- `BenchmarkGetVersioned`
- `BenchmarkConditionalTxnReadSet1`
- `BenchmarkConditionalTxnReadSet10`
- `BenchmarkConditionalTxnReadSet100`
- `BenchmarkConditionalTxnReadSet10000`
- Raw write baseline comparison showing entry revision metadata does not add a
  second ordered-root write or lookup per operation.
- M0 placeholder benchmarks live in
  `TreeDB/db/conditional_kv_contract_bench_test.go`; #3424/#3425 must replace
  them with non-skipped benchmarks and allocation evidence before closing the
  feature stack.

## 10.2 External-Version MVCC Key Codec

Invariant:

- The opt-in external-version codec round-trips arbitrary logical bytes and
  orders physical keys by logical key ascending, timestamp descending.
- Timestamp zero, wrong namespace versions, malformed escapes,
  missing/truncated/extra suffixes, and encoded keys above the uint16 envelope
  fail explicitly before the caller can persist an ambiguous key.
- Namespace, logical-prefix, and exact-key/all-version half-open bounds contain
  exactly their intended version-1 physical keys.
- The codec remains separate from raw TreeDB operations and from
  `EntryRevision`; adding it does not alter existing raw key encoding or the
  on-page entry-revision domain.

Coverage:

- `TreeDB/internal/mvcckey/codec_test.go`:
  - arbitrary-byte and timestamp-extreme round trips;
  - randomized order equivalence against the `(key asc, timestamp desc)`
    oracle;
  - namespace, logical-prefix, and all-version bound membership;
  - malformed/truncated/wrong-version rejection;
  - exact maximum-size acceptance and one-byte-over rejection.
- `TreeDB/internal/mvcckey/codec_fuzz_test.go`:
  - `FuzzRoundTrip`;
  - `FuzzDecodeNeverPanics`.
- `TreeDB/internal/mvcckey/codec_bench_test.go`:
  - `BenchmarkEncode` and `BenchmarkDecode` for allocating and reusable-buffer
    paths;
  - `BenchmarkLogicalPrefixBounds`.

The codec is a new opt-in path, so its performance gate is absolute cost and
allocation behavior rather than a before/after raw-TreeDB result. Existing raw
benchmarks must remain unchanged because no current raw API calls this package.

## 10.3 External-Version MVCC Commit and Point Read

Invariants:

- one `CommitAt` call atomically publishes records at one nonzero caller
  timestamp, with deterministic pre-write duplicate rejection; a singleton
  can use `Set`/`SetSync`, while larger publications use one TreeDB batch;
- puts, empty values, and historical tombstones remain distinguishable;
- `GetAt` returns the newest retained version at or below the read timestamp by
  a direct bounded seek, without collecting version history;
- durable commits use the profile's explicit sync opt-up and survive a process
  crash; relaxed command-WAL ACKs include kernel drain without forced fsync,
  and checkpoint/safe close establishes the profile's later reopen boundary;
- storage failures may leave the whole batch present or absent but never leave
  a visible prefix; malformed records fail with `ErrMalformedRecord`, while
  storage errors wrap `ErrStorage` and their underlying cause;
- raw TreeDB and `EntryRevision` paths do not invoke this opt-in layer.

Coverage:

- `TreeDB/mvcc/mvcc_test.go`:
  - golden overwrite/tombstone/read-before/exact/between/max histories and
    repeated commits at one physical timestamp;
  - multi-key atomicity, duplicate/nil-empty-key rejection, and validation
    before storage write;
  - injected pre-commit, post-commit, and staging failures proving all-or-none
    visibility;
  - malformed envelope and underlying closed-storage errors;
  - durable/relaxed mode gates, checkpoint plus close/reopen, and durable child
    process crash recovery without `Close`;
  - deterministic randomized comparison with an in-memory MVCC oracle;
  - concurrent readers under the race detector.
- `TreeDB/mvcc/mvcc_bench_test.go`:
  - single and 32-key `CommitAt` versus an equivalent direct TreeDB batch;
  - `GetAt` versus an equivalent direct bounded seek at version depths 1, 8,
    and 64, including allocation counts.
- `TreeDB/caching/point_successor_mvcc_test.go` deterministically pauses backend
  snapshot acquisition to check concurrent readers/point writers, retained
  exclusive fences, generic/fallback admission, same-timestamp replacement,
  rotation and partial one-record backend publication. A WAL-admitted writer
  paused before shard apply cannot insert into the rotated generation. A
  physical-delete retry must acquire a fresh view. The raw-delete schedule
  demonstrates why codec shape alone cannot qualify the shared capability.
- `TreeDB/internal/memtable/mvcc_successor_alias_test.go` checks minimum and long
  canonical key aliases across replacement/growth for every table mode; run it
  under the race detector. The minimum key length excludes append-only inline
  entry-slot key backing, which can be pooled during growth.
- `TreeDB/mvcc/point_read_concurrency_test.go` checks Store group admission at
  relaxed and durable public batch publication boundaries. Existing pruning,
  floor, forced-pointer reopen and snapshot suites also exercise the capability.

For lock attribution, observe `treedb.cache.point_successor.mvcc_shared_total`
and the `mvcc_{noncanonical,range_span,physical_delete}_fallbacks_total` counters
alongside existing mutable/queue/backend probe and hit counters. Mutex holder
delay and block waiter delay require matched timed windows and sampling rates;
neither is CPU time or a production latency acceptance measurement. Keep
profile-option changes outside production benchmark acceptance binaries.

The raw regression gate uses unchanged existing point/batch benchmarks from the
same base and head. Because `TreeDB/mvcc` is opt-in and not called by raw APIs,
any repeatable raw-path regression or allocation increase attributable to a
changed row-owning binary blocks closeout.

## 10.4 Retained-Version Iteration and Safe Pruning

Invariants:

- forward scans order `(logical asc, timestamp desc)` and reverse scans order
  `(logical desc, timestamp asc)` while honoring prefix, logical bounds, and a
  read-timestamp ceiling;
- iterators pin one snapshot, copy options and returned bytes, exclude the
  discard metadata key, and surface tombstones plus exact
  visited/skipped/retained accounting;
- the persisted floor is the greatest discardable timestamp: reads at or below
  it and commits at or below it fail, while the floor never moves backward;
- pruning is streaming/bounded, retains the newest value anchor at/below the
  floor, and removes a tombstone only after older versions cannot resurrect;
- durable floor-first interruption can be reopened and resumed idempotently;
  pinned pre-prune snapshots remain readable under the race detector;
- a qualified prune paused after snapshot capture blocks foreground point
  reads and snapshot acquisition until physical deletion completes; batch-only
  adapters retain nonblocking reads/commits/snapshot capture. Floor advancement
  stays serialized, and previously pinned snapshots stay readable.

Coverage:

- `TreeDB/mvcc/versions_test.go` covers directional golden histories, binary
  keys, seek, copied ownership, prefix/bound/read-time filters, tombstones,
  floor rejection/regression, value and tombstone anchors, reopen,
  interrupted-batch restart, idempotence, and concurrent snapshot readers.
- `TreeDB/mvcc/ownership_test.go` checks borrowed inspection without additional
  Value calls or allocations, owned Entry independence through movement/Close,
  owned payload transfer on both successor capabilities, borrowed iterator
  poisoning at Close, eager storage/malformed-envelope errors, and public
  ordinary/forced-pointer output survival after mutation, replacement,
  checkpoint and DB Close. Close clears the decoded borrowed current/key buffer,
  preserving raw Error forwarding, statistics and cleanup-error behavior.
- `TreeDB/mvcc/exact_key_test.go` compares exact-key reads with an unchanged
  generic scan filtered after consuming owned entries: both directions, nonexact
  ceilings, seek, empty/binary keys, logical tombstones, empty values, bounds,
  pinned commit/checkpoint/floor behavior and storage/malformed-record errors.
  It checks codec-maximum bounds without claiming oversized physical keys fit
  a published page. Canonical snapshot sources deterministically drop from nine
  to two on the eight-shard fixture. Malformed/incomplete/oversized timestamp
  suffixes retain `VersionAffinityPrefix`, so stored malformed records cannot
  escape fail-closed decoding through source selection.
- `TreeDB/caching/snapshot_exact_key_test.go` checks newest physical-duplicate
  precedence, physical tombstones, and full-source fallbacks for missing point
  roots, missing queue IDs, retained positional spans and global-root upgrade.
  The public MVCC range-delete test verifies the real full-queue barrier path.
- The same suite pauses prune iterator creation after snapshot capture and
  separately checks qualified foreground fencing and batch-only nonblocking
  reads/commits/iterator acquisition. It verifies old pinned views, completed
  pruning and subsequent durable commits. The cached successor suite reproduces
  retained queue/live40 plus a later backend snapshot after logical tomb80
  deletion, demonstrating why the Store prune fence is necessary.
- `TreeDB/mvcc/mvcc_bench_test.go` compares all-version scans with physical
  encoded-key scans with the same owned key/value output across key counts
  `{64,256}`, version depths `{1,8,32}`, and both directions. Filtered scans use
  depth 16/read timestamp 8 and cover all 128 keys plus all, 100, 10, and 1 of
  512 keys (100%, 19.5%, 1.95%, and 0.195% selectivity). Pruning uses depth 16:
  64-key cases cover 3/16 and 11/16 discard densities, while 256-key cases
  cover 3/16, 7/16, and 11/16. The matrix reports useful/skipped rates,
  bytes/op, allocs/op,
  prune throughput, physical bytes per pruned version, delete-batch write
  amplification, and retained physical bytes/versions per operation. Retained
  bytes are physical records still reachable in the pinned prune snapshot, not
  immediate filesystem reclamation.

`BenchmarkVersionIterationExactKey` is the bounded M0 diagnostic for the
public all-version posting-read path, activated through `ExactKey` after M1:
128 populated logical keys over eight
shards, a binary/NUL target, and depths 1/8/64 with non-exact read timestamps.
Its FrozenQueue and PublishedRoot rows exclude fixture creation and checkpoint;
Interleaved includes eight singleton replacements (one per populated shard)
per completed read and holds physical history cardinality constant. All rows
time and charge iterator open, explicit seek, owned `Entry` consumption,
validation/accounting and close. Useful versions are consumed once; the current
physical visited/retained counters include the first version examined at both
open and seek. The benchmark reports these work counters without asserting
their values, allowing equivalent optimized reads to reduce work. Observed and
unobserved rows separately report B/op and allocs/op.
`TestVersionIterationExactKeyFixture` checks the result oracle and verifies cached
versus backend-only public routing. Codec maximum-length coverage remains in
`internal/mvcckey/codec_test.go`; this fixture does not claim every codec-sized
key fits a published TreeDB page.

`SetIteratorDebug` is disabled by default. `TestAcquireSnapshotDebugAccounting`
proves direct cached snapshot calls/cuts are separate from DB.Iterator cuts,
counts all replaced shards plus the newly enqueued frozen records and memtable
`Size()` byte estimates, and checks disabled accounting. Snapshot iterators reuse
`iterator.sources_total` and process-lifetime source/queue maxima; these now
aggregate DB.Iterator and cached Snapshot.Iterator sources. Their separate
`snapshot.iterator_calls_total` denominator counts successful cached iterator
construction. A checkpointed public backend-only snapshot bypasses both cached
entry points, so it correctly records zero cached sources and zero cuts; the
benchmark separately records one public iterator open per completed read.
Source-open counts include empty sources, not only winning sources. Debug cuts
perform O(shards) Len/Size reads of newly frozen tables and atomic additions
under the existing lock, without payload copies, clock sampling or stack walks.
Process heap end gauges and sampled process-lifetime maxima include setup and
unrelated process state; they are not retained-heap deltas or per-operation peaks.
Flush units, apply batches and entries use distinct denominators and stop before
cleanup/checkpoint drain, so outstanding debt remains visible in the end queue.
These diagnostic rows and optional Go profiles cannot establish matched Dgraph
performance acceptance; M0 consumes the accepted #4862 matrix after exact
runtime/harness/workload closure checks and publishes lane decisions separately.

Measurement boundary:

- `BenchmarkMVCCOwnership` measures public GetAt and exact-key iterator
  inspection versus owned collection at widths 8/16/4096/32768 and depths
  1/8/64, with ordinary and forced-pointer routing. Owned collection charges
  its output slice and every Entry copy; borrowed inspection validates the same
  metadata and payload endpoints before movement. Baselines without EntryView
  use Entry, with dispatch once per opened iterator. Both iterator rows include
  identical clock samples for first-entry latency; total ns/op includes complete
  consumption and Close. Existing Filtered rows never call Entry and serve only
  as traversal guards. Ordinary large values may use automatic value pointers.
- Opt-in `TestMVCCRetainedOwnedOutputs` retains 4096 public GetAt results of
  each width after store closure and two GCs, validates every output, and holds
  them through heap measurement with KeepAlive. Use one selected subtest per
  fresh process, identical fixture source on both binaries, and report raw
  HeapAlloc/HeapObjects plus equal payload bytes. Size-class/header retention
  can increase despite lower allocation traffic; no cap-based GC claim or noisy
  CI heap threshold is made. This diagnostic does not establish throughput.

- scan fixtures are built before `ResetTimer`; scan time and allocations cover
  iterator open, owned decode/copy, traversal, accounting, and close;
- filtered fixtures are also outside the measurement and cover the same MVCC
  iterator lifecycle with prefix/read-time rejection included;
- pruning rebuilds its fixture and advances the floor with the timer stopped on
  every iteration because pruning is destructive. Timed bytes, allocations,
  and throughput cover only `PruneVersions`; DB close is also outside the
  measurement. The floor and prune use relaxed mode so the benchmark measures
  version selection and bounded delete publication rather than fsync latency.

The bounded matrix smoke is `GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench
'BenchmarkVersionIteration|BenchmarkPruneVersions' -benchmem -benchtime=1x
-count=1`. Omit `-benchtime=1x` for measurements. Existing raw `BenchmarkScan`
is compared on the same base/head with ten samples; because the MVCC path is
opt-in, its acceptable raw-path regression is at most 5% with no allocation
increase.

## 10.5 Dgraph MVCC Public Conformance and Closeout

Invariants:

- downstream conformance uses only exported `TreeDB`, `mvcc`, and `mvcctest`
  APIs; Dgraph-specific envelopes do not become generic TreeDB contracts;
- one committed golden public trace covers binary-key codec effects, atomic
  commit/read-at, empty values, tombstones, all-version order, durable reopen,
  discard floor, and pruning;
- deterministic randomized and concurrent traces remain reproducible from
  fixed seeds and bounded operation counts;
- the Dgraph module pin is an exact merged-main gomap commit containing the
  closeout, never a worker branch or floating main;
- raw-path and MVCC evidence name the tested commit, host, Go toolchain,
  commands, sample count, and artifact location; relaxed and durable rows are
  never compared as equivalent acknowledgement classes.

Coverage:

- `TreeDB/mvcc/mvcctest` provides the closure-based downstream harness and
  TreeDB adapter helper; `TreeDB/mvcc/conformance_external_test.go` proves the
  suite from outside the implementation package and the harness example
  compiles against the public surface.
- `TreeDB/internal/mvcckey/testdata/codec_v1_golden.json` pins the internal
  pre-alpha codec bytes and order independently from the public behavioral
  trace; existing codec property/fuzz tests continue to cover larger domains.
- Existing MVCC tests retain direct fault-injection and abrupt-child-exit
  coverage that a generic in-process adapter factory cannot express.
- `scripts/mvcc_raw_path_gate.sh` is the <=5% raw-path base/head gate. Its
  machine-readable report distinguishes raw measured `PASS`/`FAIL` from
  per-row attribution and aggregate acceptance. The checker attributes every
  row to the SHA-256 relation of its owning `db`, `caching`, or `treedb`
  benchmark binary. A failed row with byte-identical base/head owner remains
  reported but is non-attributable; a failed changed-owner row remains
  threshold-enforced. Mixed evidence can receive aggregate `EQUIVALENT` only
  if every changed-owner row passes. Missing, malformed, duplicated, or
  mismatched binary evidence fails closed and cannot override a failed
  measurement.
  Raw and adapter gates require balanced even AB/BA sample counts and default
  to eight samples per revision. The raw-gate timing verdict uses the median
  per-pair candidate/base relative delta; base/head timing medians remain
  reported as context. Its raw batch-write row pins 1,000 iterations per
  sample and measures bounded eight-write foreground groups under a fixed
  100 ms coordinator delay. Publisher execution and checkpoint drains remain
  outside the timed/allocation interval, and an unexpected publisher call
  during a group fails the benchmark instead of contaminating the sample.
- The `performance-observation-only` PR label is the narrow exception for a
  ticket whose frozen performance class explicitly replaces the raw-path
  percentage budget with matched observational fixtures. CI still runs the
  exact base/head gate, uploads its artifacts, and reports its measured verdict;
  only threshold enforcement becomes non-blocking. The linked ticket and PR
  must document the accepted performance class and replacement evidence.
  `scripts/mvcc_closeout_matrix.sh` runs the pinned CommitAt, GetAt,
  all-version, and pruning depth/durability matrix and captures CPU, peak RSS,
  normalized storage footprint, `B/op`, and `allocs/op`.
- `TreeDB/docs/spec/dgraph-mvcc-readiness-3673.md` is the supported/unsupported
  capability and measurement-boundary closeout; its artifact index records the
  exact measured commit and compact results.

## 11. Collections Native Fast Path

Invariant:
- Collections use the native ordered-root publish path by default.
- The runtime collections package has no oracle-path selector and no detached
  replay or overlay translation hook.
- Collection benchmark defaults measure the production-mainline storage cell:
  data roots with outer leaves in the value log and secondary indexes in pager
  leaves unless explicitly overridden.
- Indexed collection writes use collection-local write memtables by default.
  Buffered writes are visible through the owning manager before root publish,
  but the async indexed flush path is flush-boundary durable, not
  durable-at-ack.
- Flush barriers and schema/index changes drain pending indexed write-domain
  state and wait for in-flight async publishing units before planning from
  persisted roots.

Coverage:
- `TreeDB/collections/native_default_test.go`:
  - `TestCollectionsRuntimeHasNoOracleOrTranslationSelectors`
- `TreeDB/collections/bench_test.go`:
  - `TestBenchmarkCollectionStoragePolicyDefaultsProductionMainline`
- `TreeDB/db/ordered_root_publish_test.go`:
  - `TestPublishOrderedRootGroup_UsesPerRootStoragePolicy`
  - `TestPublishOrderedRootDeltaGroupWithSystemBuilder_UsesPerRootStoragePolicy`
- `TreeDB/collections/api_test.go`:
  - `TestCollectionSingleInsertMatchesSingleItemBatch`
  - `TestCollectionSingleInsertRejectsUniqueConflictAtomically`
  - `TestCollectionSingleDocumentReopenUsesPersistedRootDescriptors`
  - `TestCollectionIndexedWriteMemtablesReadUniqueAndFlush`
  - `TestCollectionIndexedFlushUnitCloseFlushesRotatedState`
  - `TestCollectionIndexedWriteMemtablesAsyncAutoFlushDrainsOnFlush`
  - `TestCollectionIndexedWriteMemtablesAsyncPublishingUnitsParticipateInReadsAndUniqueChecks`
  - `TestCollectionIndexedWriteMemtablesAsyncBackpressureWaitsForPublishingUnit`
  - `TestCollectionIndexedWriteMemtablesFlushWaitsForPublishingUnits`
  - `TestCollectionIndexedWriteMemtablesCreateIndexWaitsForPublishingUnits`
  - `TestCollectionIndexedWriteMemtablesAsyncPublishRetargetsMutableRuns`
  - `TestCollectionIndexedWriteMemtablesAsyncUpdateAndDeleteDrainCorrectly`

Benchmark verification for collection cutover must include the full storage
matrix:

- `data_outer=true,index_outer=false` (production-mainline priority),
- `data_outer=true,index_outer=true` (fully compressed),
- `data_outer=false,index_outer=false` (fast/control),
- `data_outer=false,index_outer=true` (low-priority compatibility cell).

## 11.1 Collection Text Search Metadata, Storage, And Analyzer

Invariant:
- Collection text index metadata persists through reopen, root names/policies are
  stable, versioned postings/text-state/text-stats encodings fail closed on
  malformed data, CreateTextIndex drains buffered writes across collection
  managers before taking its backfill snapshot, insert/delete/update/batch paths
  maintain postings/text-state/text-stats across flush/checkpoint/reopen,
  DropTextIndex clears metadata/root descriptors, the simple analyzer is
  deterministic, and SearchText uses bounded postings scans plus BM25F-style
  ranking with top-K-bounded document fetch and fail-closed truncation/storage
  counters.

Coverage:
- `TreeDB/collections/text_index_test.go`:
  - `TestTextAnalyzerSimpleFixtures`
  - `TestCollectionTextIndexMetadataValidateAndReopen`
  - `TestCollectionTextIndexMetadataRejectsInvalidDefinitions`
  - `TestCollectionTextRootNames`
  - `TestCollectionTextRootStoragePolicies`
  - `TestCollectionTextIndexedWritesMaintainStorageAndSearchRanks`
- `TreeDB/collections/text_storage_test.go`:
  - `TestTextStorageCodecsRoundTripAndFailClosed`
  - `TestCollectionCreateTextIndexBackfillsReopensAndReportsStorage`
  - `TestCollectionDropTextIndexClearsMetadataRootsAndWriteGuard`
  - `TestCreateTextIndexFlushesBufferedWritesFromOtherManagers`
  - `TestCreateTextIndexMaintainsWritesFromStaleHandles`
  - `TestTextIndexStorageStatsFailsClosedOnMalformedRoot`
  - `BenchmarkCreateTextIndexBackfill`
- `TreeDB/collections/text_maintenance_test.go`:
  - `TestTextIndexMaintenanceInsertMaintainsPostingsStateStats`
  - `TestTextIndexMaintenanceDeleteRemovesPostingsAfterFlushReopen`
  - `TestTextIndexMaintenanceUpdateRemovesOldAndAddsNewTerms`
  - `TestTextIndexMaintenanceBatchInsertUpdateDelete`
  - `TestTextIndexMaintenanceBufferedCreateFlushCheckpointReopen`
- `TreeDB/collections/text_search_m4_test.go`:
  - `TestSearchTextSingleTermRankedSearchM4`
  - `TestSearchTextANDOROperatorsM4`
  - `TestSearchTextLiteralModeM4`
  - `TestSearchTextFieldWeightAffectsRankingM4`
  - `TestSearchTextMissingIndexUnsupportedSyntaxAndTruncationM4`
  - `TestSearchTextSeesUnflushedTextIndexedInsertM4`
  - `TestSearchTextTombstonedPostingsConsumeScanBudgetM4`
  - `TestSearchTextFailClosedWrapsStorageCorruptionM4`
  - `TestSearchTextTopKBoundsDocumentFetchM4`
  - `TestSearchTextReopenParityM4`
  - `BenchmarkSearchTextM4`
- `TreeDB/collections/hybrid_text_candidates_test.go`:
  - `TestSearchHybridTextCandidatesLexicalOptions4766`
- `TreeDB/collections/text_v2_blockmax_test.go` and
  `text_v2_position_validation_4558_test.go`:
  - block-max fallback and final-attribution posting budgets remain monotonic
    and fail closed without exceeding the explicit cap
- `TreeDB/documentservice/rag_parity_test.go`:
  - `TestHTTPLiteralAndBoundedFilteredLexicalSearch4766`
  - `TestHTTPBooleanOperatorConflictParity4766`
  - `BenchmarkHTTPFilteredLexicalSearch4766`

## 11.2 Raft Placement Route Preflight

Invariant:
- Catalog-backed route preflight converts validated placement decisions to
  request-only cluster route metadata, supports collection and single-token
  token/ring targets, classifies token batches, and fails closed before submit
  for token/ring multi-ID writes until split/fanout exists.
- Once route metadata is present, the single-group submitter treats the target
  group as binding and rejects group mismatches before command preflight,
  commit-source invocation, or local apply. Route metadata remains excluded
  from deterministic entry bytes and command digests.

Coverage:
- `TreeDB/internal/raftplacement/route_test.go`:
  - `TestRouteCollectionDecisionIncludesGroupMetadata`
  - `TestRouteTokenDecisionIncludesPartitionAndGroupMetadata`
  - `TestRouteDocumentTokenPreservesCollectionModeAndRoutesTokenModes`
  - `TestRouteTokenBatchClassifiesDocumentTokens`
  - `TestRouteTokenBatchFailsClosed`
- `TreeDB/nativewire/cluster_submitter_test.go`:
  - `TestCatalogRouteResolverRoutesResolvedCatalog`
  - `TestClusterRoutePreflightTokenPlacementSingleIDMutationCommands`
  - `TestClusterRoutePreflightTokenPlacementRejectsMultiID`
  - `TestRaftClusterSubmitterRouteGroupMismatchRejectsBeforeLocalMutation`
  - `TestClusterSubmitterRequestOnlyFieldsDoNotAlterDeterministicEntry`
- `TreeDB/mongo_gateway/cluster_submitter_test.go`:
  - `TestClusterRoutePreflightMongoTokenPlacementSingleIDWrites`
  - `TestClusterRoutePreflightMongoTokenPlacementRejectsMultiIDWrites`
  - `TestClusterSubmitterConcreteBridgeRouteGroupMismatchNotWritablePrimary`
- `TreeDB/internal/raftcluster/submit_test.go`:
  - `TestSingleGroupSubmitterRejectsRouteGroupMismatchBeforePreflightCommitApply`
  - `TestSingleGroupSubmitterAllowsMatchingRouteGroup`
  - `TestSingleGroupSubmitterPreservesNoRouteMetadataBehavior`
- `TreeDB/internal/raftentry/contract_test.go`:
  - `TestDigestV1StabilityAcrossMetadataAndApplyEntryID`

## 11.5 Planned User-Command WAL Durability Gate

This section owns the canonical planned test matrix for the user-command WAL
implementation track. Design documents may list invariants, but named tests,
fault classes, fuzz targets, benchmark artifact fields, and acceptance evidence
are maintained here.

The detailed TDD execution plan is tracked in
https://github.com/snissn/gomap/issues/1529. That issue owns implementation
sequencing and PR task breakdown. This section owns the durable verification
requirements that the ticket and implementation PRs must satisfy.

### 11.5.1 Normative Coverage Matrix

In this matrix, `AppliedLSN` names the logical command stream boundary.
`AppliedCommandLSN` is used only when the test or statement specifically refers
to the V1 in-page-marked meta-page storage field.

| Normative statement | Owner section | Required test/evidence | Status |
|---|---|---|---|
| WAL-supported command visibility implies process-crash recoverability. | `user-command-wal.md` normal write path | `TestCommandWALInsertAckRecoverableBeforeCheckpoint`, `TestCommandWALDeleteAckRecoverableBeforeCheckpoint`, `TestCommandWALReplaceAckRecoverableBeforeCheckpoint` | planned |
| V1 has no durable pending overlay; process visibility requires recoverable WAL plus normal-executor install. | `collections-write-domain.md` durability boundary | `TestCommandWALFrameDurableBeforeExecutorInstallNotVisible`, `TestCommandWALVisibleAckRecoverableBeforeCheckpoint` | planned |
| Pre-frame failures are ordinary not-committed failures and leave no visible state. | `user-command-wal.md` normal write path | `TestCommandWALPreFrameValidationFailureLeavesNoMutation`, `TestCommandWALExternalRefPrepareFailureRejectsBeforeVisibility` | planned |
| Post-recoverable-frame failures are commit-ambiguous or recovery-required, not retryable not-committed errors. | `user-command-wal.md` replay idempotency, `native-wire-protocol.md` ack policy | `TestCommandWALPostFramePublishFailureCommitAmbiguous`, `TestNativeWireCommandWALPublishFailureCommitAmbiguous` | planned |
| Logical `AppliedLSN` is selected atomically with roots and stored in V1 as `AppliedCommandLSN`. | `user-command-wal.md` checkpoint and cleanup, `storage-format.md` command WAL target | `TestCommandWALRootsAndAppliedCommandLSNPublishAtomically`, model split-state rejection proof | planned |
| `AppliedLSN` advances only over a contiguous command LSN prefix. | `user-command-wal.md` publish boundary | `TestCommandWALAppliedLSNContiguousPrefixOnly`, `TestCommandWALOutOfOrderPublishRejected` | planned |
| Complete frames with missing required external refs fail closed. | `user-command-wal.md` recovery | `TestCommandWALMissingExternalRefFailsRecovery`, `TestCommandWALCorruptExternalRefFailsRecovery` | planned |
| Terminal incomplete tails are ignored only when no complete commit marker exists. | `recovery.md` decoder outcomes | `TestCommandWALTerminalShortHeaderIgnored`, `TestCommandWALTruncatedCompleteFrameFailsClosed` | planned |
| Unknown required versions, command kinds, and critical flags fail closed. | `storage-format.md` command WAL target | `TestCommandWALUnknownRequiredVersionFailsClosed`, `TestCommandWALUnknownRequiredKindFailsClosed`, `TestCommandWALUnknownCriticalFlagFailsClosed` | planned |
| Old raw `commitlog.Record` payloads are unsupported after command WAL activation. | `storage-format.md` command WAL target | `TestCommandWALFeatureGateRejectsLegacyRawPayload` | planned |
| Batch commands are one command frame, one LSN, and all-or-nothing. | `user-command-wal.md` batch atomicity | `TestCommandWALRawKVBatchOneLSNAtomic`, `TestCommandWALCollectionInsertBatchOneLSNAtomic`, `TestCommandWALOversizedBatchRejectsBeforeLSN` | planned |
| Callback update APIs never replay Go callback code. | `user-command-wal.md` update API categories | `TestCommandWALCallbackUpdateLogsFinalReplacement`, `TestCommandWALRecoveryDoesNotInvokeCallback` | planned |
| Resolver helpers are resolved before WAL append. | `user-command-wal.md` update API categories | `TestCommandWALSetNowStoresResolvedLiteral`, `TestCommandWALRecoveryDoesNotInvokeResolver` | planned |
| Catalog/schema barriers cannot race lower unapplied commands. | `collections-write-domain.md` barrier semantics | `TestCollectionCommandWALCreateCollectionDrainsRecoveredLowerLSN`, `TestCollectionCommandWALRejectsCatalogIndexMutations` | PR6 coverage for create collection and rejected index DDL; index command support remains future |
| Read-only open fails when mutating command replay would be required. | `recovery.md`, `contracts.md` read-only open | `TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired` | planned |
| Backup/restore either includes needed WAL/external refs or has durable cleanup proof. | `backup-restore.md` restore validation | `TestCommandWALBackupManifestRestoresUnappliedCommands`, `TestCommandWALRestoreMissingRequiredFrameFailsWithoutCleanupProof` | planned |
| Native-wire deterministic command schemas align with local command WAL payload schemas. | `native-wire-protocol.md`, `user-command-wal.md` Raft/native-wire relationship | `TestCommandWALNativeWireAlignmentManifestCoverage`, `TestNativeWireAndLocalCommandDigestStable` | planned |
| Raft/local recoverability is not reported before local command WAL publish and `AppliedLSN`. | `native-query-raft-roadmap.md` local apply layering | `TestRaftApplyDoesNotReportRecoverableBeforeCommandWALAppliedLSN` | future |
| R3a durable apply metadata preserves applied progress, idempotency/result replay, and logical digests across reopen and fails closed on corrupt metadata. | `storage-format.md` R3a apply metadata logs, `native-query-raft-roadmap.md` local apply layering | `TestDurableApplyStoresCloseReopenPreservesApplyProgressIdempotencyAndResult`, `TestDurableApplyStoresIdempotencyDuplicateSameDigestAndDifferentDigest`, `TestDurableApplyStoresFailClosedOnTruncatedAndCorruptMetadata`, `TestDurableApplyStoresLogicalDigestIndependentOfMetadataFiles` | implemented |
| Collection-level Raft placement resolves each placed `{database,catalog,collection}` to exactly one group and fails closed for unplaced collections, duplicate placements, unknown groups/features, and invalid leader hints. Token/ring catalog placements validate partition coverage, exactly-one-ID token routes resolve one partition/group, token batches classify single-token, same-partition, same-group, and fanout-required cases, adapters reject multi-ID token/ring writes before submit until explicit split/fanout execution exists, and single-group submit admission rejects mismatched route group metadata before preflight/commit/apply. | `raftplacement.md` #3046 first slice, token-partition catalog slice, token-batch preflight slice, and route-binding admission slice | `TestValidateResolvesCollectionLevelPlacements`, `TestValidateRejectsInvalidCatalog`, `TestValidateFeatureFloorFailsClosed`, `TestResolveFailsClosedForUnplacedCollection`, `TestValidateAcceptsTokenRingCatalogPlacements`, `TestValidateRejectsInvalidTokenRingCatalogPlacements`, `TestValidateRejectsDuplicateCollectionAcrossTokenAndCollectionPlacement`, `TestRouteTokenDecisionIncludesPartitionAndGroupMetadata`, `TestRouteTokenBatchClassifiesDocumentTokens`, `TestRouteTokenBatchFailsClosed`, `TestClusterRoutePreflightTokenPlacementRejectsMultiID`, `TestClusterRoutePreflightMongoTokenPlacementRejectsMultiIDWrites`, `TestSingleGroupSubmitterRejectsRouteGroupMismatchBeforePreflightCommitApply`, `TestRaftClusterSubmitterRouteGroupMismatchRejectsBeforeLocalMutation`, `TestClusterSubmitterConcreteBridgeRouteGroupMismatchNotWritablePrimary` | implemented |
| Simulation-only token-ring plans cover the uint64 token space exactly once, assign each virtual partition to a known catalog group, and fail closed for empty plans, invalid or duplicate partition IDs, unknown groups, invalid ranges, gaps, overlaps, and incomplete coverage. | `raftplacement.md` #3046 token-ring simulation slice | `TestPlanTokenRingDistributesVirtualPartitions`, `TestPlanTokenRingRejectsInvalidInputs`, `TestValidateTokenRingPlanRejectsInvalidPlans`, `TestValidateTokenRingPlanSortsAndProtectsResolvedPlan` | implemented |

### 11.5.2 Milestone Test Slices

PR 1: typed commit-log frames and feature gate:

- `TestCommandWALFormatGoldenV1EmptySegment`;
- `TestCommandWALFormatGoldenV1RawKVBatch`;
- `TestCommandWALFormatGoldenV1CollectionInsertBatchByID`;
- `TestCommandWALFormatGoldenV1CatalogCreateCollection`;
- `TestCommandWALFormatRejectsUnsupportedRequiredVersion`;
- `TestCommandWALFormatRejectsUnknownRequiredKind`;
- `TestCommandWALFormatRejectsUnknownCriticalFlag`;
- `TestCommandWALFormatSkipsUnknownNonCriticalExtensionOnlyWhenAllowed`;
- `TestCommandWALFormatRoundTripExternalRefs`;
- `TestCommandWALFormatRejectsMalformedLengthBeforeAllocation`;
- `TestCommandWALFormatRejectsFrameCRCMismatch`;
- `TestCommandWALFeatureGateRejectsLegacyRawPayload`;
- `TestCommandWALFeatureGateRequiresCleanLegacyWALBeforeActivation`;
- `TestCommandWALRequiredFeatureFailsClosedUntilExecutionEnabled`;
- `TestCommandWALNoCollectionSegmentFamilyCreated`;
- `TestCommandWALTerminalShortHeaderIgnored`;
- `TestCommandWALDuplicateLSNFailsClosed`;
- `TestCommandWALDuplicateLSNAcrossSegmentsFailsClosed`;
- `TestCommandWALRawKVBatchOneLSNAtomic`;
- `TestCommandWALRawKVBatchPreservesEmptySetValue`;
- `TestCommandWALExistingCoverageInventoryMapsLegacyWALTests`;
- `TestCommandWALLegacyRawEncodingTestsHaveTypedFrameEquivalents`.

PR 2: shared journal ownership and `AppliedCommandLSN` plumbing:

- `TestCommandJournalAllocatesContiguousLSNs`;
- `TestCommandJournalSeedsLSNFromExistingFrames`;
- `TestCommandJournalSeedsLSNFromExistingSegmentFamily`;
- `TestCommandJournalSeedsLSNFromExistingLanes`;
- `TestCommandJournalTruncatesTerminalTailBeforeAppend`;
- `TestCommandJournalTruncatesActiveTerminalTailPerLane`;
- `TestCommandJournalRejectsNonActiveTerminalTail`;
- `TestCommandJournalConcurrentAppendsSerializeFrameOrder`;
- `TestCommandJournalRejectsIndependentMutableOwner`;
- `TestJournalOwnerRollbackMaxLSNClearsExhausted`;
- `TestCommandJournalUsesCommitSegmentFamily`;
- `TestCommandJournalValidationFailureDoesNotConsumeLSN`;
- `TestCommandJournalUnsupportedVersionDoesNotConsumeLSN`;
- `TestCommandJournalAppendFailureRollsBackLSN`;
- `TestCommandJournalOversizedFrameDoesNotConsumeLSN`;
- `TestCommandJournalSegmentTargetRotatesBeforeLSNReservation`;
- `TestCommandJournalDeterministicStressReopenAcrossLanesAndTails`;
- `FuzzCommandWALDecodeFrame`;
- `FuzzCommandWALRawKVBatchPayload`;
- `TestMetaPageBodyAppliedCommandLSNRoundTrip`;
- `TestMetaPageBodyFullLegacyDecodeIgnoresReservedAppliedCommandLSNBytes`;
- `TestMetaPageBodyLegacyDecodeDefaultsAppliedCommandLSN`;
- `TestCommandWALAppliedCommandLSNMetaFieldRoundTrip`;
- `TestCommandWALAppliedCommandLSNAlternatingMetaPages`;
- `TestCommandWALLegacyMetaDecodeIgnoresReservedAppliedLSNBytes`;
- `TestCommandWALRootsAndAppliedCommandLSNPublishAtomically`;
- `TestCommandWALPublishHelperRejectsRootsWithoutAppliedLSN`;
- `TestCommandWALAppliedLSNContiguousPrefixOnly`;
- `TestCommandWALAppliedLSNContiguousPrefixMatchesModelStress`;
- `TestCommandWALCheckpointCleanupDeletesOnlyCoveredSegments`;
- `TestCommandWALCheckpointCleanupRetainsActiveCoveredSegment`;
- `TestCommandWALSegmentMaxLSNStreamsFrames`;
- `TestCommandWALSegmentMaxLSNFailsClosedOnNonIncreasingLSN`;
- `TestCommandWALOpenFailsClosedOnCorruptTypedSegmentEvenWhenCovered`;
- `TestCommandWALOpenFailsClosedOnNonActiveTerminalTailEvenWhenCovered`;
- `TestCommandWALOpenFailsClosedOnTypedTailWithHigherLegacyRawSegment`;
- `TestCommandWALOpenAllowsActiveTypedTailWithHigherPartialLegacyAliasSegment`;
- `TestCommandWALOpenAllowsActivePartialFirstFrameTail`;
- `TestCommandWALOpenFailsClosedOnNonActivePartialFirstFrameTail`;
- `TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired`;
- `TestCommandWALReadOnlyOpenAllowsFramesCoveredByAppliedLSN`;
- `TestCommandWALWriteOpenSkipsCoveredFramesBeforeLegacyReplay`;
- `TestCommandWALWriteOpenRejectsUnappliedFramesUntilDispatcher`;
- `TestCommandWALWriteOpenRejectsFirstUnappliedFrameUntilDispatcher`;
- `TestCommandWALWALOffOpenRejectsUnappliedFramesUntilDispatcher`;
- `TestCommandWALBackupManifestShapeIncludesAppliedLSNAndRanges`.

PR 3: recovery dispatcher and raw KV command conversion:

- `TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor`;
- `TestCommandWALRIDFencePreservedForRawKVBatch`;
- `TestCommandWALCrashAfterFrameBeforeRootPublishRecovers`;
- `TestCommandWALCrashDuringRootPublishSelectsOldTupleOrNewTuple`;
- `TestCommandWALCrashAfterRootAppliedLSNBeforeCleanupSkipsFrame`;
- `TestCommandWALRawSetReplayRePointersWhenThresholdDrops`;
- `TestCommandWALRawEmptyBatchAdvancesAppliedLSNAsNoop`;
- `TestCommandWALRecoveryCrashDuringReplayResumesFromAppliedLSN`;
- `TestCommandWALStrictCommandEffectWithoutAppliedLSNFailsClosed`;
- `TestCommandWALIdempotentSkipRequiresDigestProof`;
- `TestCommandWALExistingRawReplayTestsMappedToRawKVBatch`;
- `TestCommandWALExistingRIDFenceTestsMappedToExternalRefFence`.

PR3 implementation evidence:

- `RawKVBatch` is the first replayable command kind for direct backend
  command-WAL mode.
- Read-write recovery dispatches typed frames, replays raw KV commands through
  the normal backend batch executor, and publishes roots plus
  `AppliedCommandLSN` in one finalize boundary.
- Clean read-write reopens still run covered-segment cleanup, so a prior crash
  after root plus `AppliedCommandLSN` publication but before cleanup converges
  on the next open even when no frames need replay.
- Explicit `CommandWAL` activation first fails closed on dirty legacy WAL,
  then persists `command_wal_v2` after replay preconditions are clear and
  before opening the command journal, so a process cannot acknowledge typed
  frames without a durable required-feature gate.
- Raw KV `SetRID` command entries preserve the existing value-log RID fence by
  requiring the referenced RID to be present in scanned value-log segments
  before recovery can publish the command.
- `RawKVBatchV2` materialized-RID entries carry exact RID plus value bytes.
  Codec tests reject them under V1; recovery tests prove exact creation,
  crash/retry reuse, matching existing-RID reuse, conflict failure, and
  checkpoint/reopen readability. Public reopen tests also prove that a lower
  exact RID repaired into a newer segment cannot lower the cached foreground
  allocator below an older segment's high-water. Bounded forced-pointer `Batch.WriteSync`
  proves one command-WAL file sync and zero value-log file syncs, while the
  frame-cap and 257-total-operation cases prove whole-batch `SetRID` fallback
  and its dependency fence; the 256-operation boundary remains eligible.
- Pointer-backed raw KV command writes resolve the source RID directly from
  value-log pointer metadata instead of scanning whole value-log segments.
- Inline-only raw KV replay does not depend on value-log RID scanning. Recovery
  builds the RID map and replay value-log appender only when a pending frame
  contains `SetRID`, the current value-placement policy requires
  re-pointerizing a logged `set`, or value-log-backed leaf pages require a
  replay appender.
- Raw KV `set` replay that exceeds the current inline threshold is
  re-pointerized through the existing replay value-log appender, and the
  appended value-log bytes are synced before roots plus `AppliedCommandLSN` are
  published.
- Empty `RawKVBatch` frames are explicit no-op command frames: they publish the
  current roots with the frame LSN so command-stream contiguity remains exact.
- Command WAL with benchmark/compatibility WAL-off durability fails closed,
  including after `command_wal_v2` is persisted, because command-WAL mode requires a
  recoverable command frame before root visibility.
- Command journal flush/sync failures and post-append root publication failures
  poison the open handle so no later write can create a durable LSN gap before
  reopen recovery.
- Once a command frame has been appended, later flush/sync failures are
  commit-ambiguous rather than definitely-not-committed: recovery may replay
  the frame after close and read-write reopen.
- `RawKVBatch` frames that reference value-log RIDs require the external ref to
  reach the same fresh-process recovery boundary before the frame is appended;
  non-sync writes do not add a power-loss fsync guarantee, while sync writes
  sync external refs before the command frame.
- Operators and callers must treat a poisoned command-WAL handle as
  recovery-required: close the handle and reopen read-write before issuing more
  writes. The poisoned state is intentionally not cleared by an in-process
  retry.
- Public cached-mode command WAL writes remain fail-closed until the cached
  writer is converted to the shared typed command journal. This prevents mixed
  legacy raw records in `command_wal_v2` directories.
- Strict split-state detection for non-idempotent command kinds remains a
  required gate before collection/catalog commands can be marked
  `WAL-supported`; raw KV `set`/`delete` replay uses absolute deterministic
  assignments and never skips over missing LSNs without contiguous proof.

PR 4: collection insert/delete by explicit ID:

- `TestCollectionCommandWALInsertBatchByIDStagesAppliedLSNUntilFlush`;
- `TestCollectionCommandWALInsertBatchByIDReplayRecoversUnappliedFrame`;
- `TestCollectionCommandWALInsertBatchByIDReplayTemplateV1StoredDocument`;
- `TestCollectionCommandWALInsertBatchByIDReplayAdvancesEmptyFrame`;
- `TestCollectionCommandWALDeleteBatchByIDReplayIgnoresMissingIDs`;
- `TestCollectionCommandWALDeleteBatchByIDReplayAdvancesMissingOnlyFrame`;
- `BenchmarkCollectionCommandWALInsertBatchByID`;
- `BenchmarkCollectionCommandWALDeleteBatchByID`;
- acceptance artifact:
  `artifacts/command-wal/pr4/acceptance.json`.

PR 5: collection update by explicit ID:

- `TestCommandWALFormatGoldenV1CollectionUpdateBatchByID`;
- `TestCommandWALCollectionPayloadDecodeBoundsCountBeforeAllocation`;
- `TestCollectionCommandWALUpdateByIDPublishesAppliedLSN`;
- `TestCollectionCommandWALUpdateByIDReplayRecoversUnappliedFrame`;
- `TestCollectionCommandWALUpdateByIDIndexedPublishesSecondaryRoots`;
- `BenchmarkCollectionCommandWALUpdateBatchByID`;
- acceptance artifact:
  `artifacts/command-wal/pr5/acceptance.json`.

PR 6: catalog mutation commands:

- `TestCommandWALFormatGoldenV1CatalogCreateCollection`;
- `TestCollectionCommandWALCreateCollectionPublishesAppliedLSN`;
- `TestCollectionCommandWALCreateCollectionReplayRecoversUnappliedFrame`;
- `TestCollectionCommandWALCreateCollectionReplaySameMetadataIdempotent`;
- `TestCollectionCommandWALCreateCollectionReplayIncompatibleMetadataFailsClosed`;
- `TestCollectionCommandWALCreateCollectionDrainsRecoveredLowerLSN`;
- `TestCollectionCommandWALRejectsCatalogIndexMutations`;
- `BenchmarkCollectionCommandWALCreateCollection`;
- `BenchmarkCollectionCommandWALRejectedIndexDDL`;
- acceptance artifact:
  `artifacts/command-wal/pr6/acceptance.json`.

PR 6.5: collection/catalog command-WAL performance polish:

- consolidated benchmark evidence:
  `artifacts/command-wal/pr6_5/collection-catalog-performance-summary.md`;
- acceptance artifact:
  `artifacts/command-wal/pr6_5/acceptance.json`;
- default-ready collection throughput follow-up:
  `https://github.com/snissn/gomap/issues/1584`;
- PR9 raw KV default cutover evidence must not be used to claim collection
  command-WAL default readiness until every supported collection lane clears
  strict `>1.01x` command-WAL versus benchmark WAL-off throughput.

PR 7: matrix enforcement and drift tests:

- `TestCommandWALSupportMatrixIsWellFormed`;
- `TestCommandWALSupportMatrixCoversCollectionMutators`;
- `TestCommandWALSupportMatrixCoversMongoMutationHandlers`;
- `TestCommandWALSupportMatrixCoversNativeWireMutationCommands`;
- `TestCommandWALSupportMatrixDocumentsRejectedCommandsWithPublicError`;
- `TestCommandWALRejectedErrorDistinctFromUnsupported`;
- `TestMetadataUnsupportedCatalogCommandsReturnUnsupportedFeature`;
- `TestCommandWALNoActiveCollectionWALImplementationDrift`;
- `TestCommandWALDocsRejectActiveCollectionWALReferencesOutsideDeprecatedDoc`;
- `TestCommandWALDocsRequireAppliedCommandLSNAsV1Target`;
- `TestCommandWALDocsRequireBatchAtomicityText`.

PR 8: native-wire/Raft alignment closeout:

- `TestCommandWALNativeWireAlignmentManifestCoverage`;
- `TestNativeWireAndLocalCommandDigestStable`;
- `TestNativeWireAckFlushedRequiresRootPublishAndAppliedLSN`;
- `TestNativeWirePostFramePublishFailureCommitAmbiguous`;
- `TestRaftApplyDoesNotReportRecoverableBeforeCommandWALAppliedLSN`;
- `TestRaftCommandEntryAndLocalCommandPayloadUseSharedCanonicalSchema`.

### 11.5.3 Crash and Fault-Injection Matrix

Every WAL-supported command kind must run through a shared fault-injection
harness. The harness must be deterministic and must record the injected point,
expected public error, expected reopen behavior, and expected cleanup debt.

Required cut points:

| Cut point | Expected result |
|---|---|
| before validation completes | ordinary user error; no frame, no visibility |
| after external-ref prepare starts but before protection | no frame; orphan prepare classified after recovery |
| after external-ref protection but before frame append | no frame; protected ref released or quarantined by recovery artifact |
| after partial frame header | terminal tail ignored only for active tail; sealed/nonterminal segment fails |
| after partial first frame in newest command segment | active tail is ignored/truncated; older partial first-frame tails fail closed |
| after active command segment tail with higher canonical legacy raw WAL file present | legacy raw WAL files do not affect typed command active-tail selection |
| after active command segment tail with higher partial legacy alias WAL file present | legacy alias WAL files do not affect typed command active-tail selection |
| after complete frame before WAL sync boundary | relaxed modes follow their advertised boundary; durable mode must not acknowledge |
| after complete recoverable frame before command apply | read-write recovery replays; read-only open fails recovery-required |
| during command apply before root publish | copy-on-write partial pages are unreachable; recovery replays |
| after root publish attempt before meta selection | recovery selects old tuple or fails closed; no split state is served |
| after roots plus logical `AppliedLSN` selected before response | command is committed; API returns commit-ambiguous if response cannot be built |
| after roots plus logical `AppliedLSN` before WAL cleanup | recovery skips covered frames and cleanup resumes idempotently |
| during cleanup metadata write | cleanup is retried or leaked; missing frames are never tolerated without proof |
| during recovery replay before publish | next open resumes from previous `AppliedLSN` |
| during recovery replay after publish before cleanup | next open skips covered frames and resumes cleanup |

Required command/data combinations:

- raw set/delete/batch with inline values;
- raw set batch with value-log RID/external-ref fence;
- collection insert/delete single item;
- collection insert/delete batch with duplicate/conflict/no-op items;
- collection replacement update from callback output;
- declarative update with resolver literals;
- catalog create collection and create index once supported;
- command payload external refs for oversized logical payload bytes;
- generated external files for future column-store apply outputs.

### 11.5.4 Model, Property, and Fuzz Testing

Required state-machine models:

| Model | Required properties |
|---|---|
| command LSN prefix model | `AppliedLSN` is contiguous; no higher LSN can publish while a lower LSN is uncovered unless in the same publish boundary |
| root/meta tuple model | selected state is old roots plus old `AppliedLSN` or new roots plus new `AppliedLSN`; split states fail closed |
| strict replay idempotency model | strict commands never skip on generic already-exists evidence; idempotent skip requires declared proof |
| batch atomicity model | one user batch maps to one command LSN; item-level failure before frame leaves no visible item; post-frame failure is whole-command ambiguous |
| external-ref retention model | prepared/protected refs cannot be GCed before frame abort or root reachability handoff |
| read-only open model | complete unapplied command frames require recovery; stale modes are explicit and rejected by maintenance/backup |

Required fuzz targets:

- `FuzzCommandWALDecodeFrame`;
- `FuzzCommandWALDecodeNoPreChecksumAlloc`;
- `FuzzCommandWALDecodeExternalRefs`;
- `FuzzCommandWALDecodePayloadByKind`;
- `FuzzCommandWALRecoveryOrdering`;
- `FuzzCommandWALUnknownFieldsAndCriticalFlags`;
- `FuzzCommandWALPathCanonicalizeExternalRefs`;
- `FuzzCommandWALValuePtrExternalRefs`;
- `FuzzNativeWireCommandToLocalCommandPayload`;
- `FuzzCommandWALBatchAtomicityModel`.

Required fuzz properties: no panic, bounded allocation, no root publish on
invalid bytes, no file deletion/quarantine from invalid bytes, deterministic
error class for identical input, no skip of complete corrupt frames, no
advancement of `AppliedLSN` without command effects, and no command effects
without matching `AppliedLSN`.

### 11.5.5 Hardening Fixtures and Negative Tests

Required decoder and bounds tests:

- `TestCommandWALMaxEncodedFrameRejectsBeforeAlloc`;
- `TestCommandWALFrameLengthOverflowRejects`;
- `TestCommandWALVarintOverflowRejects`;
- `TestCommandWALExternalRefCountLimitRejects`;
- `TestCommandWALDuplicateExternalRefConflictingChecksumRejects`;
- `TestCommandWALUnknownRequiredExternalRefClassFatal`;
- `TestCommandWALUnknownOptionalExternalRefClassCannotCleanup`;
- `TestCommandWALBadFrameCRCCompleteRecordFailsOpen`;
- `TestCommandWALTerminalShortHeaderIgnored`;
- `TestCommandWALTruncatedActiveTailIgnoredOnlyWithoutCommitMarker`;
- `TestCommandWALTruncatedSealedSegmentFailsOpen`;
- `TestCommandWALMiddleCorruptionBlocksLaterLSN`;
- `TestCommandWALMissingLSNBlocksHigherLSN`;
- `TestCommandWALNoAllocBeforeChecksumForHugeExternalRefCount`;
- `TestCommandWALNoAllocBeforeChecksumForHugeStringLength`;
- `TestCommandWALCompressedRawLenBombRejectsBeforeDecode`;
- `TestCommandWALOffsetSizeOverflowRejects`;
- `TestCommandWALUint64ToInt64OffsetOverflowRejects`;
- `TestCommandWALLimitsMaxRecordSizeDisabledDoesNotDisableCommandCap`.

Required identity and catalog tests:

- `TestCommandWALDropRecreateSameNameDifferentUIDRejectsOldCommand`;
- `TestCommandWALRootUIDKindGenerationMismatchRejects`;
- `TestCommandWALSchemaEpochMismatchRejects`;
- `TestCommandWALCatalogEpochMismatchRejects`;
- `TestCommandWALIndexDefinitionDigestMismatchRejects`;
- `TestCommandWALCollectionNameNeverUsedAsReplayIdentity`;
- `TestNativeWireCatalogGuardV1CanonicalizesStableIDs`;
- `TestNativeWireNameDropRecreateRaceDeterministicGuardFailure`;
- `TestRaftMetadataIDsDeterministicAcrossReplicas`.

Required local-file safety tests for external refs and recovery artifacts:

- `TestCommandWALOpenRejectsSymlinkDBRoot`;
- `TestCommandWALOpenRejectsSymlinkWALDir`;
- `TestCommandWALOpenRejectsWorldWritableWALDir`;
- `TestCommandWALOpenRejectsGroupWritableClassRoot`;
- `TestCommandWALExternalRefOpenRejectsSymlinkComponent`;
- `TestCommandWALExternalRefOpenRejectsSymlinkFinalFile`;
- `TestCommandWALExternalRefCleanupDoesNotFollowSymlink`;
- `TestCommandWALExternalRefCleanupRejectsHardlink`;
- `TestCommandWALPreparedRenameRejectsCrossDevice`;
- `TestCommandWALLockfileRejectsSymlink`;
- `TestCommandWALCorruptErrorRedactsCollectionName`;
- `TestCommandWALMissingExternalRefErrorRedactsRelativePathUnlessAdmin`;
- `TestCommandWALDuplicateKeyErrorRedactsDocumentID`;
- `TestNativeWireErrorRedactsDocuments`;
- `TestCommandWALMetricsUseUIDAndHashesNotNames`;
- `TestCommandWALForensicToolRawOutputRequiresExplicitFlag`.

### 11.5.6 Maintenance, Backup, Restore, and Offline Preconditions

Required tests:

- `TestCommandWALValueLogGCSkipsProtectedExternalRef`;
- `TestCommandWALValueLogRewriteSkipsProtectedExternalRef`;
- `TestCommandWALCompactStorageAbortsOnCommandWALDebt`;
- `TestCommandWALExternalRefPrepareGuardBlocksGCWindow`;
- `TestCommandWALLeafGenerationGCKeepsPendingLeafRef`;
- `TestCommandWALVacuumOnlinePublishesOrRejectsDirtyWAL`;
- `TestCommandWALBackupBarrierCleanCheckpointRestoresWithoutWALDebt`;
- `TestCommandWALBackupBarrierWALSnapshotRestoresUnappliedCommand`;
- `TestCommandWALFilesystemBackupWithoutBarrierUnsupported`;
- `TestCommandWALBackupIncludesValueLeafDictTemplateAndColumnExternalRefs`;
- `TestCommandWALRestoreFailsWhenCommandMissingExternalPayload`;
- `TestCommandWALRestoreAcceptsMissingCleanedSegmentOnlyWithCleanupManifest`;
- `TestReadOnlyOpenRejectsUnappliedCommandWAL`;
- `TestReadOnlyOpenAllowsCleanCommandWAL`;
- `TestReadOnlyStaleModeReportsDebtAndIsRejectedByMaintenance`;
- `TestOpenReadOnlyNoLockRejectsDirtyCommandWALForOfflineRewrite`;
- `TestValueLogRewriteOfflineRejectsDirtyCommandWAL`;
- `TestVacuumIndexOfflineRejectsDirtyCommandWAL`;
- `TestOfflineMaintenanceRejectsUnclassifiedPreparedExternalRefs`;
- `TestOfflineMaintenanceAllowsCleanedCommandWALWithManifest`;
- `TestCommandWALCheckpointPublishesAppliedLSNBeforeCleanup`;
- `TestCommandWALCleanupRequiresDurableCheckpointBoundary`;
- `TestCommandWALSegmentCleanupDecodesEveryFrame`;
- `TestCommandWALCleanupDoesNotReleaseProtectionBeforeReachabilityHandoff`;
- `TestCommandWALPreparedUncommittedExternalFilesQuarantinedAfterRestore`;
- `TestCommandWALQuarantinePurgeRequiresCheckpoint`.

### 11.5.7 Observability, Tooling, and Acceptance Artifacts

Every completed milestone must write
`artifacts/command-wal/<milestone>/acceptance.json`. The artifact must include:

- branch, commit, Go version, platform, durability mode, sync policy, and command
  support matrix version;
- list of passed unit tests, fuzz targets, race tests, model tests, and crash
  harness scenarios;
- golden fixture digests and re-encode-identical proof;
- benchmark commands, inputs, and pass/fail thresholds;
- metrics schema version and emitted metric names;
- known unsupported command kinds and their public error behavior;
- cleanup debt, oldest unapplied LSN, and external-ref retained-byte summaries.

Required golden fixture families:

- empty command WAL segment;
- `RawKVBatch` with inline set/delete;
- `RawKVBatch` with value-log external refs;
- `CollectionInsertBatchByID`;
- `CollectionDeleteBatchByID`;
- `CollectionReplaceBatchByID`;
- `CollectionUpdateByIDOps` with resolved literals;
- catalog mutation placeholder or explicit WAL-on rejection fixture;
- command-payload external ref;
- cleanup record and segment metadata;
- unknown critical extension rejection;
- unknown noncritical extension skip;
- unsupported version fail-closed;
- malformed length, frame CRC, and commit marker corruption.

Required non-mutating CLI tooling:

- `treemap command-wal health --dir <db> --json` reports `db_dir_hash`,
  `format_version`, `generated_at_unix_nano`, `overall_state`,
  `safe_to_restart`, `safe_to_backup`, `safe_to_compact`,
  `requires_recovery`, `requires_operator_action`, `metrics`, `segments`,
  `pending_commands`, `protected_external_refs`, `cleanup_debt`, `gc_blockers`,
  `last_recovery`, and `errors`.
- `treemap command-wal safe-delete --dir <db> --json --dry-run` classifies files
  without mutation. Per-file fields are `file_id`, `relative_path`, `path_hash`,
  `class`, `bytes`, `status`, `safe_to_delete`, `delete_reason`,
  `blocking_reason`, `blocking_command_ids`, `blocking_lsn_ranges`,
  `blocking_external_ref_ids`, `blocking_snapshot_ids`, `requires_checkpoint`,
  `requires_recovery`, and `requires_quarantine`.
- `treemap command-wal command --dir <db> --command-id <id> --json` and
  `treemap command-wal command --dir <db> --lsn <n> --json` map one command to
  `command_id`, `lsn`, `kind`, `scope`, `segment_id`, `segment_offset`,
  `catalog_epoch`, `schema_epoch`, `payload_digest`, `external_refs`,
  `result_assertions`, `applied_lsn_state`, `replay_state`, and `cleanup_state`.
- `verify --dir <db> --read-only --command-wal --external-refs --json` verifies
  command WAL external-ref closure without mutation. JSON fields include
  `command_wal_checked`, `roots_checked`, `external_refs_declared`,
  `external_refs_canonical`, `external_refs_present`, `external_refs_missing`,
  `external_refs_corrupt`, `external_ref_closure_errors`, `applied_lsn_errors`,
  `cleanup_manifest_errors`, and `result`.
- `treemap command-wal classify --dir <db> --json` parses command WAL segment
  headers, frames, decoder outcomes, error categories, external-ref summaries,
  and redacted command summaries. The existing `wal_classify` value-log-oriented
  command must either be renamed to `vlog_classify` or kept explicitly
  documented as value-log-only to avoid operator confusion.

Required safe-delete statuses are `safe_cleaned_segment`, `pending_command_wal`,
`protected_external_ref`, `orphan_prepared_external_ref`,
`missing_required_external_ref`, `corrupt_required_external_ref`,
`cleanup_manifest_required`, `snapshot_pinned`, and `unknown_unclassified`.

The command WAL verification mode is read-only by default. Any repair, vacuum,
cleanup, or quarantine mutation must require an explicit mutating flag such as
`--repair`, `--vacuum-index`, or a future `--mutate`.

Required metric prefix: `treedb.command_wal.`. Required metrics include:
`append_ns/doc`, `bytes/doc`, `commands/sec`, `external_refs/doc`,
`pending_bytes`, `applied_lsn_lag`, `gc_protected_external_ref_bytes`,
`cleanup_ns/segment`, `recovery_commands/sec`, `recovery_payload_bytes/sec`,
`recovery_external_refs/sec`, `recovery_peak_heap_bytes`, `allocs/doc`, and
`bytes_allocated/doc`.

Resource-budget benchmark artifacts must include `durability_mode`,
`sync_policy`, `segment_size_bytes`, `command_kind`, `batch_docs`,
`doc_size_bytes`, `payload_format`, `external_ref_classes`, `collection_count`,
`backend`, `go_version`, and `commit`. Phase timings must include validate,
resolve helpers, callback execution, external-ref prepare, WAL encode, WAL
append, WAL sync, executor apply, root publish, `AppliedLSN` publish, checkpoint,
cleanup, and recovery replay.

The benchmark gate fails when required columns are missing, when formula-derived
bytes/doc is exceeded by more than 10 percent, or when an absolute ceiling from
resource accounting is exceeded, even when relative `benchstat` regression
thresholds pass. Required harnesses may use `cmd/collection_workload_bench`,
`cmd/collection_bench_matrix`, `cmd/collection_bench_report`, or an equivalent
`cmd/unified_bench` command WAL suite that emits the same schema.

Docs lint should enforce the observability contract once command WAL
implementation starts. Required lint checks:

- `user-command-wal.md` owns the active command support matrix;
- active specs outside `collection-wal-durability-plan.md` do not describe
  `wal/collection-l*.log`, `internal/collectionwal`, `CollectionSeq`, `WALLSN`,
  or collection applied watermarks as active implementation targets;
- `storage-format.md` names `AppliedCommandLSN` as the V1 storage target;
- `recovery.md` contains the stable recovery error category table;
- `verification.md` contains the required `treemap command-wal health`,
  `safe-delete`, `command`, `classify`, and `verify --command-wal` command names;
- the operator runbook states include `clean`, `pending`, `recovery_required`,
  `corrupt`, and `cleanup_debt`.

Production persistent column-store writes may start only after this command WAL
verification gate links to green typed-frame, `AppliedCommandLSN`, collection
command, catalog barrier, external-ref, backup/restore, and read-only-open
evidence.

### Public raw KV command-WAL cutover evidence

The first PR9 public cutover gate is:

- `TestPublicCommandWALRawKVWritesUseTypedFrames`

The historical strict PR9 performance gate is parity-plus. Its recorded point
`Set`, focused `Batch.Write`, `unified_bench` batch-write, and incompressible
value-log auto/off acceptance lanes must each report candidate throughput
strictly greater than `1.01x` of the relevant baseline. Any required lane in
that immutable acceptance artifact at or below `1.01x` is a failing gate,
including sub-parity results such as `0.80x`; those results may be recorded only
as failing evidence, not accepted evidence.

The current hosted incompressible value-log gate supersedes that lane's live
methodology under #3861/#3863 without rewriting the historical PR9 artifact.
`batch_write` measures front-end ingest before deferred value-log and leaf-log
publication, so the live gate uses `batch_write_steady` and profiles every exact
timed row. It publishes every raw wall-throughput ratio for diagnosis, but the
blocking pair ratio is off CPU sample seconds divided by auto CPU sample
seconds. The geometric mean of every fixed, order-balanced CPU-efficiency pair
must be strictly greater than `0.93x` on AMD EPYC 7763 runners or AMD EPYC
9V74 runners. Every other or unknown CPU model retains a threshold
strictly greater than `0.95x`. The evidence records the CPU model and selected
threshold, plus wall ratios, CPU sample seconds, and CPU-efficiency ratios.
Missing, ambiguous, or shorter-than-`0.25s` CPU profiles fail closed; rows are
never retried, selected, or discarded. One
favorable sample cannot override a mostly failing sample set. Each pair must
also keep the sum of the `total=` fields reported for `maindb/value_vlog` and
`maindb/leaf_vlog` less than or equal to `1.02x`. The checker separately
requires raw user values in both rows, block-compressed leaves in auto, and
uncompressed leaves in off; CPU-efficiency headroom cannot hide a broken
compression mode.

The same strict parity-plus rule applies to every historical command-WAL acceptance artifact
with a required performance gate: a passing status must have `>` throughput-gate
semantics, explicit `1.01x` minimum ratio thresholds, and recorded comparative
throughput ratios above that bar. Historical or diagnostic results below that
bar must be labeled as failing evidence.

This test must prove public `treedb.Open` can open a read-write
`command_wal_v2` handle, route raw KV writes through typed `RawKVBatch` command
frames, expose mode proof through stats, reopen without explicit backend-only
APIs, and recover final set/delete state. Mode proof must include cheap live
accepted/covered command-frame counters so benchmark artifacts do not require
diagnostic WAL segment scans. It is intentionally narrower than the future
cached typed-frame path: while this gate is active,
`treedb.write_path.mode=command_wal_cached` is the expected proof that public
command-WAL writes did not use the cached legacy redo journal.

Bounded-growth command-WAL evidence includes
`TestPublicCommandWALCheckpointCleansCoveredCommandJournalSegment`, which proves
checkpoint rotation and covered-segment cleanup, and
`TestPublicCommandWALAutoCheckpointUsesCommandWALBytes`, which proves
command-WAL cached mode feeds total command-WAL segment bytes into the
size-triggered auto-checkpoint loop while the legacy cached redo journal is
disabled.

Post-frontier admission evidence includes
`TestCachingDB_CheckpointExternalCommandWALAdmitsAfterFrontierCut`, which latches
the pre-cut, post-cut, and pre-drain phases and proves that only the captured
frontier reaches the first backend boundary;
`TestCachingDB_CheckpointDrainRetainsWriterGateWithoutCommandWALCutover`, which
keeps cached redo-WAL, unsafe WAL-off, and hookless external-WAL modes blocked
through the drain; and
`TestPublicCommandWALAutoCheckpointOverlapAdmitsPostFrontierWrites`, which
admits concurrent public `Write` and `WriteSync` calls while checkpoint publish
remains latched. `TestPublicCommandWALCheckpointPostFrontierRangeWritesWaitForDrain`
proves that DB-level and pure-batch range spans instead wait for the full drain,
append no command frame while checkpoint publish is latched, record one
`checkpoint_drain` wait, and do not increment point-write admission.
Publish-error retry and crash/reopen cleanup coverage live in
`TestPublicCommandWALCheckpointPostFrontierAdmissionPropagatesPublishError` and
`TestPublicCommandWALCheckpointPostFrontierGenerationSurvivesCrashReopen`; the
latter forces value-log pointers, crashes after covered command-WAL cleanup,
replays the fresh post-cut segment, and verifies both values again after
`ValueLogGC`.

Deferred document-vector finalization is covered by
`TestServiceDeferredVectorBuildOptimizeCheckpointCrashReopen`, which exits
without close after successful Optimize and verifies the document and clean
query-ready vector generation on reopen.

## 12. Collections Document Formats

Invariant:
- Template-v1 collections persist their hash-to-numeric-ID template map in the
  collection-local `<collection>/templates` TreeDB ordered root.
- Template-v1 primary documents store compact `TD1D` bytes with numeric
  template IDs and resolve templates from the current batch or from the
  persisted template root.
- Secondary indexes, deletes, reopens, and index backfills use the template root
  instead of JSON parsing.

Coverage:
- `TreeDB/collections/template_v1_test.go`:
  - `TestTemplateV1CollectionInsertBatchIndexesAndTemplateRoot`
  - `TestTemplateV1CollectionReopenFindAndDelete`
  - `TestTemplateV1EncoderLearnsIDsAfterInsertBatch`
  - `TestTemplateV1EncoderLearnsExistingTemplateIDFromHashInsert`
  - `TestTemplateV1EncoderRejectsLearnedIDsAcrossCollections`
  - `TestTemplateV1StoredDocsRequireScopedEncoderInsert`
  - `TestTemplateV1EncoderAllowsSameCollectionHandleReuse`
  - `TestTemplateV1EncoderResetClearsLearnedIDs`
  - `TestTemplateV1EncoderConvertsNestedRootShapeObjectsWithLearnedIDs`
  - `TestTemplateV1EncoderLearnsBufferedTemplateIDs`
  - `TestTemplateV1EncoderReusesPersistedTemplateRoot`
  - `TestTemplateV1EncoderResetEmitsTemplateAgain`
  - `TestTemplateV1CreateIndexBackfillsFromTemplateRoot`
  - `TestTemplateV1MultiKeyIndex`
  - `TestTemplateV1NestedIndexExtraction`
- `TreeDB/collections/overhead_bench_test.go`:
  - `BenchmarkCollectionOverheadPlanIndexedTemplateV1`
  - `BenchmarkCollectionOverheadIndexStateTemplateV1Extraction`

## 12.5 Typed-Column Schema Evolution and Migration Policy

Invariant:
- During pre-alpha, typed-column image, descriptor, manifest, and schema changes
  may reject existing DB directories rather than migrate them.
- Unsupported typed-column versions and schema/layout mismatches fail closed from
  headers, descriptors, manifest identities, or refs where possible before full
  payload decode or per-row allocation.
- Hot typed-column format/schema changes report baseline-versus-final `B/op` and
  `allocs/op` evidence or explicitly document a benchmarked fallback.

Coverage:
- Policy owner: `TreeDB/docs/spec/typed-column-schema-evolution.md`.
- Existing fail-closed coverage is distributed across typed-column adapter,
  publication, vector dense-section, int64 scan, and typed-asset maintenance
  tests in `TreeDB/collections` and `TreeDB/internal/typedcolumn`.
- Future format-version or schema-semantic changes must add targeted negative
  tests for unsupported image/descriptor versions, schema-hash mismatch,
  owner/value-type/vector-dim/fixed-width metadata mismatch, and manifest
  identity/ref mismatch in the package that owns the changed decoder.

## 12.6 Typed-Column Optimized-Consumer Capability Matrix

Invariant:
- Every current collection logical value type constant, `typedcolumn.ColumnType`,
  and `typedcolumn.Encoding` has an optimized-consumer tier entry or explicit
  compatibility/experimental classification.
- Graph-search-relevant typed-column state points to the generic tier matrix,
  with healthy current-format graph search requiring `mmap_direct` unless #2044
  admits a weaker tier with benchmark, allocation, and memory evidence.

Coverage:
- Policy owner: `TreeDB/docs/spec/typed-column-optimized-consumer-capabilities.md`.
- Docs lint: `TreeDB/docs/column_store_capability_matrix_test.go` fails when a
  new logical type, physical type, or encoding is added without matrix coverage,
  or when graph-search-relevant rows lose #2044/#2046 links.

## 12.7 Prepared Typed-Column Graph-Search Runtime Views

Invariant:
- Every current graph-search state role has a documented canonical persisted
  typed-column format, owner/state role, certification boundary, prepared runtime
  shape, hot-loop boundary, fallback/fail-closed rule, graph-row fallback
  prohibition, counters, tests, and benchmark evidence plan.
- Future typed-column graph-search dependencies cannot enter the healthy
  current-format path until their optimized runtime state is documented in the
  #2044 admission table, runtime enforcement fails closed, counters/tests exist,
  and #2037-style benchmarks prove no unaccepted material regression.

Coverage:
- Policy owner: `TreeDB/docs/spec/typed-column-graph-search-prepared-views.md`.
- Docs lint: `TreeDB/docs/graph_search_prepared_views_test.go` verifies the
  current base-vector, adjacency, inverse-norm, row-ref, and document-ID rows,
  the future type admission gate, graph-row fallback prohibition, and owner-doc
  links.
- The #2044 readiness/admission table is verified separately below; reusable
  certifier implementation remains owned by #2046; benchmark matrix evidence
  remains owned by #2037.

## 12.8 Graph-Search Typed-Column Optimized-State Admission Gate

Invariant:
- Every current graph-search optimized-state role has a readiness/admission row
  with status, #2047 tier, owner or manifest role, prepared runtime shape,
  hot-loop boundary, fallback/fail-closed rule, counters/tests, and benchmark or
  admission evidence fields.
- Current healthy base-vector, HNSW adjacency, inverse-norm, row-ref, and
  document-ID roles require `mmap_direct` unless the admission table explicitly
  admits a weaker tier with benchmark, allocation, memory, and wall-time
  evidence.
- Vector-index state roles added to `column_vector_index_state_manifest.go`
  cannot bypass the table; legacy graph-row and adjacency compatibility rows
  must remain fallback-only or fail closed.

Coverage:
- Policy owner: `TreeDB/docs/spec/typed-column-graph-search-admission.md`.
- Docs lint: `TreeDB/docs/graph_search_admission_test.go`:
  - `TestDocs_GraphSearchTypedColumnAdmissionGate`
  - `TestDocs_GraphSearchAdmissionCoversVectorIndexStateRoles`
  - `TestDocs_GraphSearchAdmissionHealthyRowsRequireMmapDirect`
  - `TestDocs_GraphSearchAdmissionLinkedFromOwners`
- Runtime prepared-view implementation remains owned by #2038/#2040/#2041 and
  combined routing remains owned by #2045; this section enforces the documented
  admission gate and fail-closed readiness fields.

## 12.9 Graph-Search Benchmark Truth Matrix

Invariant:
- Benchmark rows that compare graph-search source paths must carry stable labels
  for mode, timing boundary, concurrency, and fixture so legacy/direct graph-row
  controls, current TVIS/base typed-column routing rows, and combined prepared
  typed-column rows are not confused.
- After #2045/#2043, supported prepared typed-column rows are admission and
  fallback-readiness evidence, not final performance promotion by themselves.
  The `current_tvis_base_typed_column` label is retained for continuity and
  proves current-format routing selects the combined prepared view; it is not an
  unprepared hot-loop source route in healthy current-format readers. The matrix
  must call out non-apples-to-apples topology/search-work differences such as
  the #2043 612-versus-3340 visited_edges/search finding.
- Supported rows report `ns/op`, `ops/sec`, `B/op`, `allocs/op`, graph rows,
  candidates/search, edges/search, result/document counters, and direct/fallback
  typed-column source counters.
- The #2091 topology-parity benchmark must keep vectors, synthetic adjacency,
  query ordinal/order, `topK`, `efSearch`, filters, and timing boundary identical
  across the legacy graph-row/direct compatibility reader and the no-physical-row
  current prepared typed-column reader; parity tests must assert equal
  search-work counters and equivalent results before the benchmark evidence is
  used for #2035 promotion decisions.
- The #1979 batchability benchmark must be opt-in (`benchmark_debug`) and must
  report neighbor tile distribution, score-batch histograms, scored-versus-skipped
  neighbors, already-visited skips, layer-0 versus upper-layer work,
  frontier/top-k operation counts, visited-mark hits/misses, exact-mode candidate
  order summaries, `ns/op`, `ops/sec`, `B/op`, and `allocs/op` without changing
  traversal semantics or fixture topology.
- The #2103 promotion gate must prove that default gathered/indexed scoring is
  selected only for healthy eligible prepared typed-column search, that explicit
  scalar/default/indexed prepared results match, and that legacy graph-row/direct
  or non-prepared fallback routes keep default scalar scoring and existing
  fallback/source counters.

Coverage:
- Policy owner: `TreeDB/docs/spec/typed-column-graph-search-benchmark-matrix.md`.
- Code/test owners:
  - `TreeDB/collections/vector_graph_search_truth_matrix_2037_test.go`:
    `TestVectorGraphSearchTruthMatrixRows2037` freezes row labels and supported-row
    semantics; `TestVectorGraphSearchTruthMatrixMetricContract2037` freezes the
    required report-counter vocabulary.
  - `TreeDB/collections/column_vector_graph_topology_parity_2091_test.go`:
    `TestColumnVectorGraphSearchTopologyParity2091` freezes the #2091
    topology/search-work/result parity gate, and
    `BenchmarkColumnVectorGraphSearchTopologyParity2091` emits the equal-work
    graph-only and result-ID rows.
  - `TreeDB/collections/column_vector_graph_batchability_1979_test.go`:
    `TestColumnVectorGraphNativeSearchBenchmarkDebugCounters1979` checks #1979
    counter reconciliation, skip buckets, layer work, frontier/top-k counts, and
    exact-mode candidate-order summaries; `BenchmarkColumnVectorGraphSearchBatchability1979`
    emits the opt-in batchability/control-flow rows.
  - `TreeDB/collections/column_vector_graph_promotion_2103_test.go`:
    `TestColumnVectorGraphPreparedDefaultIndexedScoring2103`,
    `TestColumnVectorGraphNonPreparedDefaultScoringRemainsScalar2103`, and
    `TestColumnVectorGraphLegacyDefaultScoringRemainsScalar2103` freeze the
    default-gating decision; `BenchmarkColumnVectorGraphSearchPromotion2103`
    emits the default/scalar/indexed promotion rows.

## 12.10 Quantized Vector Score Planes

Invariant:
- Exact/default `column_graph` search remains authoritative float32-vector
  scoring unless callers explicitly select a named quantized score plane.
- `quantized_only` returns estimated legacy scalar_u8, explicit
  per-granule-alpha scalar_u8, pure-Go `rabitq_1bit`, or prototype `brq_1bit`
  scores from the selected named score plane and must not read exact vectors or
  norms during scoring. Omitted `scalar_u8_calibration` remains legacy after the
  #2845 no-promote gate; calibrated alpha is explicit opt-in.
- `quantized_rerank` uses the selected quantized traversal over the normalized
  `ef_search` candidate pool, trims to `QuantizedRerankCandidates`, exact-reranks
  only that shortlist by graph ordinal, and returns exact cosine scores.
- Missing, stale, mismatched, unsupported, or unprepared quantized assets fail
  closed with no hidden exact fallback.
- Public hybrid `quantized_rerank` uses the same admitted scalar-u8 traversal
  and authoritative FP32 rerank under one captured owner. Exact remains the
  default; selected requests use fixed source budgets, truthful typed-exact or
  typed-HNSW receipts, bounded final fetch, and zero output-vector bytes unless
  embeddings are explicitly requested.

Coverage:
- Policy owner: `TreeDB/docs/spec/quantized-vector-index.md`.
- Runtime tests:
  - `TreeDB/collections/column_vector_graph_quantized_asset_test.go` covers
    scalar_u8 asset build/prepare/reopen, per-granule-alpha metadata
    build/persist/reopen/reference-code validation, quantized_only score
    semantics, quantized_rerank exact shortlist ranking, normalized `ef_search`
    traversal before trim, multiple quantized indexes, concurrency, and
    fail-closed asset validation.
  - `TreeDB/collections/column_vector_graph_rabitq_quantized_asset_test.go`
    covers `rabitq_1bit` asset build/prepare/reopen, pure-Go scorer parity,
    lower-level and collection buffered quantized search, exact-read guardrails,
    cache/lifecycle behavior, allocation guardrails, and fail-closed asset
    validation.
  - `TreeDB/collections/column_vector_graph_brq_quantized_asset_test.go` covers
    `brq_1bit` definition normalization, asset build/prepare/reopen, oracle
    parity, lower-level buffered quantized search, exact-read guardrails,
    allocation guardrails, BRQ counters, and fail-closed asset validation.
  - `TreeDB/collections/vector_index_search_test.go` covers public exact,
    quantized_only, quantized_rerank, searcher buffer, and missing-name behavior.
  - `TreeDB/collections/typed_graph_hybrid_test.go` covers coherent selected
    hybrid ownership across concurrent publication, response ownership, scalar
    strategies, and independent filter budgets.
  - `TreeDB/documentservice/typed_hybrid_test.go` covers public service/HTTP
    selection, truthful selective/empty routing, cancellation, omitted
    embeddings, and invalid field/asset combinations. The Python client model,
    HTTP, and integration tests cover request serialization and receipt decode.
  - `TreeDB/internal/quantizedasset/quantized_asset_test.go` covers prepared
    ordinal readers, mixed row-count granule metadata roles, role/schema
    validation, footprint metrics, and scorer-shaped allocation benchmarks.
- Benchmarks:
  - `BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926` reports exact vs
    `quantized_only` vs `quantized_rerank` `ns/op`, `ops/sec`, `B/op`,
    `allocs/op`, recall@K, candidate/rerank counts, code bytes, exact vector/norm
    bytes, fallback counters, and asset bytes/vector on one fixture.
  - `BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414`
    and `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415`
    report explicit per-granule-alpha scalar_u8 lower-level/collection rows and
    `quantized_score_codec_scalar_u8_alpha/search` counters for the #2845 gate.
  - `BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926` reports rebuild
    cost and storage/asset bytes for exact assets versus legacy and
    per-granule-alpha scalar_u8 assets, including alpha metadata bytes,
    alpha distribution, and code-boundary rate for calibrated rows.
  - `BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451`,
    `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452`,
    and `BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450` report pure-Go
    RaBitQ lower-level/collection buffered search, c=1/c=8 concurrency rows,
    logical code bytes/vector, actual asset bytes/vector, exact-read counters,
    recall@K, and storage overhead for #2454 closeout.
  - `BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481`
    and `BenchmarkColumnGraphBRQQuantizedRebuildStorage2481` report prototype
    BRQ lower-level buffered search, BRQ-specific counters, exact-read
    guardrails, logical code bytes/vector, asset bytes/vector, recall@K, and
    rebuild/storage overhead.
  - `BenchmarkTypedGraphHybridPublicRoutes4767` compares exact and selected
    SQ8+rerank on the same unfiltered admitted hybrid fixture and reports route,
    quantized/rerank/packed work, fused candidates, final fetches, embedding
    output bytes, `ns/op`, `B/op`, and `allocs/op`.

## 13. Native Wire Protocol

Invariant:
- Native-wire v1 code that advertises protocol support must enforce frame,
  section, command-schema, feature-negotiation, and deterministic command-entry
  rules from `TreeDB/docs/spec/native-wire-protocol.md`.
- Protocol implementation work must keep schema IDs, codec constants, golden
  fixtures, fuzz targets, parity tests, deterministic-entry tests, benchmark
  labels, and observability counters aligned with
  `TreeDB/docs/spec/native-wire-implementation-guidelines.md`.

Coverage:
- `TreeDB/internal/nativewire/schema_test.go`:
  - command-header golden fixture,
  - command-schema validation for required sections, duplicate singleton
    sections, unknown critical sections, and unsupported command versions.
- `TreeDB/internal/nativewire/codec_test.go`:
  - frame-header golden fixture and malformed/unsupported header rejection,
  - section and byte-vector round trips,
  - byte-vector length-mismatch rejection.
- `TreeDB/internal/nativewire/fuzz_test.go`:
  - fuzz targets for frame-header, section-envelope, byte-vector decoding, and
    command-schema validation.
- `TreeDB/internal/nativewire/deterministic_test.go`:
  - deterministic-entry golden fixture and transport-field independence,
  - deterministic-entry rejection for missing distributed guards.
- `TreeDB/internal/nativewire/bench_test.go`:
  - nativewire benchmark cases for every command schema marked
    `BenchmarkRequired`,
  - reusable section and byte-vector decode scratch tests,
  - allocation guard tests for warmed frame, command-header, section,
    byte-vector, schema-validation, and deterministic-entry paths,
  - `BenchmarkNativewire...` coverage for frame headers, command headers, byte
    vectors, request body section encoding/decoding, decode+validate, and
    deterministic-entry encoding.

The native-wire server does not exist yet. R0 follow-up work must add
broader negative conformance fixtures, drift tests, and direct collection parity
tests before claiming native-wire v1 server support.

# Vector partition M1 verification

M1 verification covers canonical bounded codec round-trip, typed asset
verification, active/retired lifecycle, fail-closed reachability, Raft snapshot
archive inclusion, and column-asset GC eligibility after durable retirement.
It does not claim ANN query serving. Reader pins cover only this generation's
local cleanup lifecycle; they do not imply a query-serving or cluster-cutover
contract.

# Vector partition owner-scoped search-plan verification

The schema-7 path uses a bounded root and immutable owner/domain directory
pages. `TestVectorPartitionDirectoryPageV2OwnerSelectionAndRefusals` checks
owner selection and missing, corrupt, misbound or incorrectly ordered pages;
`TestVectorPartitionDirectoryPageV2StreamingWriterTails` covers bounded writer
tails. The `TestVectorPartitionPagedRoot*V2` cases cover the 64 KiB root codec,
mixed inline/paged refusal, identity/digest bindings, copied root lifetime,
legacy runtime/reclaim refusal and count-independent root decode allocation.

`TestVectorPartitionPagedSourceSessionV2VerifiesCompletedOwner` exercises
completed durable source imports, owner-bound source verification, source-only
Stage, reopen and session lifetime. Its current-chunk and concurrent-traversal
subtests check verified chunk reuse and replacement, complete identity matching,
cancellation, independently owned returned rows and traversal-local state under
the race detector. `TestANNOwnerCommitmentV2BindsExactCanonicalIntent`
checks the semantic domain/member commitment independently of physical page
layout. `TestVectorPartitionPagedGraphV2BuildStageReopen` exercises the combined
source/ANN producer, exact retry, one colocated graph per domain, reopened local
search and source provenance. `TestPrepareVectorPartitionSourcesV2UsesCompletedDurableOwnerImports`
reaches these producers from the applied Raft BUILD identity and actual local
hosting. Unsupported stable lifecycle namespace platforms must refuse before
creating the store or retaining producer pins.

The source-only producer refuses nodes that also require local ANN output.
The combined producer consumes source and ANN input once, verifies complete
owner commitments, builds one bounded domain through the existing Vamana core,
and stages the complete local closure. Local domain readers retain an
independent generation pin after their parent source session closes. These
checks do not admit distributed activation/search; those schema-7 paths remain
refused pending the P3 contract. Destructive schema-7 generation retirement
also remains refused before tombstone or lifecycle mutation.

The older `TestVectorPartitionOwnerSearchOpenPlan*V2` and
`TestOwnerGenerationSource*V2` cases retain intermediate owner-plan and legacy
public-open coverage. Their constructor allocation benchmark excludes V1
manifest acquisition/decoding and cannot establish bounded paged public open.
Full #4808 acceptance requires complete public build/open growth and resource
measurements, partial-build failure cleanup, snapshot/GC protection and
current-head platform/race checks. The producer uses separate private segments
for temporary intent and final output, with exact-identity cleanup for ordinary
failures. It requires an active recovery-authoritative source directory before
writing pages, so existing explicit column-asset GC can reclaim crash orphans.
Repeated crashes without that maintenance can still accumulate disk usage.

The `TestVectorPartitionPagedPrivateSegmentV2*` cases exercise process-crash
orphan GC, rebound-child refusal, retained parent-sync debt, constructor failure
recovery, changed frontiers and colliding write-lock stripes during shutdown.
The combined graph case checks selected snapshot closure and active-reader GC
protection, including corrupt transitive graph sections. Focused cleanup,
race, native preparation and storage-name checks passed on the retained
cleanup6 checkpoint. The legacy ConditionalTxn guard passed eight alternating
matched pairs after optional resource metadata became lazy: 152662.5 to 152662
bytes/op, with 281 allocations/op unchanged. Enabled DPM2 imports retain a
measured incremental 218 allocations for continuous imports and 310 after
reopen at the small32 boundary; these costs are separate from the passing
canonical-directory growth bound and remain subject to allocation review.

The call-local chunk repair at `f7edce49b` passed focused and race checks plus
three independent matched processes for each of five public build/open cases.
The 1024-row build median fell from 239.09 to 50.64 ms and 193.08 to 42.67 MB;
cold open with 256 local rows fell from 27.69 to 2.61 ms and 23.80 to 2.88 MB.
All five measured cases improved. `BenchmarkVectorPartitionPagedProjectionV2`
also separates fixture setup, global-map admission, build/Stage, cold open,
reused-session domain open and warmed local search. Remote fixture entries
represent admitted semantic metadata, not distributed storage preparation.
The complete matched matrix uses 32/256/1024 local rows, 0/1024/8192 remote
metadata entries and three independent alternating processes per arm/case.
All 216 build/open/search processes and 18 separate map-admission processes
pass. All 36 stage time medians and all 27 build/open allocation medians
improve; warmed local search remains 160 bytes and two allocations per call
in every cell. Global map admission still grows with remote metadata; its
8192-entry candidate median is 2.68 ms, 4.28 MB and 81788 allocations.
Fixture setup metrics exclude retained session/domain preparation in warm
cases; whole-process RSS and heap observations include it.

Process RSS improves in 34 of 36 cells, but warmed search with 32 local rows
and 1024 remote entries consistently rises from a 50344 KiB median to 62392
KiB (+24%). This real process-peak increase remains in the original matrix.
Three paired phase diagnostics locate the excess during the second fixture's
import, before its changed build/open paths: live heap and heap-in-use are
essentially equal, while anonymous resident memory differs. File-backed
residency is similar, and queries add approximately 127 KiB in both arms.
The candidate has lower RSS in all three single-fixture runs. These observations
support sensitivity to preceding fixture allocation/GC history; they do not
isolate the complete page-residency mechanism or establish a long-running RSS
plateau. No forced GC or fixture-lifetime adjustment replaces the original
measurement.

Component performance review accepts the build/open gains with this measured
RSS limitation; it makes no universal resident-memory improvement claim. The
unchanged-production phase packet retains 74 authenticated receipts and 18
successful exits at `growth-rss2` under the P2 evidence root, with manifest
`4317f88aef8cd4d8f63f19a7c4c669cb05fde701350c8480861e035dc189ef0b`.
Current-head platform checks and mature review remain required. These
small-fixture results do not establish P2 readiness or distributed capacity.

`TestSourceShardMapDocumentTokenIdentityV2` pins exact-byte token vectors;
`TestSourceShardMapBoundImmutableLookupV2` and
`TestSourceShardMapRefusesIdentityCoverageDriftV2` exercise immutable lookup,
wrong-shard refusal, epoch/collection/digest drift and complete range coverage.
`TestSourceShardMapSameTokenRangeDoesNotAliasIDsV2` checks exact-ID duplicate
semantics and caller input lifetime. `BenchmarkSourceShardMapResolveDocumentIDV2`
measures the enabled token/lookup cost; map validation does not grant catalog
authority or persist import progress.

`TestVectorPartitionLegacyByteCompatibilityProbeV2` uses only schema-6 APIs
and the same fixture on the candidate and exact D0 base; the scoped owner-local
workflow compares binary, JSON, integrity and ready digests. The workflow also
records exact-head Go version, focused/race checks and constructor benchmarks.

# Vector partition V1 correctness and approximation verification

The snapshot-bound V1 admission contract has disjoint exact and ANN gates. The
production-path exact-union gate opens every generation-pinned persistent pack
and requires canonical FP32 stable-ID and score-bit parity with the canonical
source oracle; it does not use the historical float64/modulo fixture oracle as
the V1 production gate. Partition-local HNSW remains recall-qualified
approximate even when every partition is probed. Documentation admission pins
the public identity/route/asset error taxonomy and the IDs/scores-only,
all-or-error, mutation-invalidation boundary.

Coverage and commands:

- `cmd/treedb_vector_partition_bench/m8_production_assets_test.go`:
  `TestM8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1` proves the
  generation-pinned exact union against the canonical source oracle, including
  FP32 score bits, stable-ID ties, and dedupe.
- `cmd/treedb_vector_partition_bench/main_test.go`:
  `TestPartitionLocalHNSWStageIsRecallQualifiedNotExact` prevents the
  all-partition HNSW result label from drifting into an exact claim.
- `TreeDB/docs/vector_partition_raft_v1_test.go`:
  `TestDocsVectorPartitionV1CorrectnessAndApproximationContract` admits the
  frozen V1 specification and this verification entry.

```sh
GOWORK=off go test -count=1 ./cmd/treedb_vector_partition_bench -run 'Test(M8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1|PartitionLocalHNSWStageIsRecallQualifiedNotExact)'
GOWORK=off go test -count=1 ./TreeDB/docs -run TestDocsVectorPartitionV1CorrectnessAndApproximationContract
```

The #4775 domain-graph storage gate is
`TestVectorPartitionDomainPackSectionsOpenWithoutReassemblyV1`,
`TestVectorPartitionDomainPackMaterializesOneChunkedSearcherV1`, and
`TestVectorPartitionDomainPackRejectsOneByteOversizeRecordV1`. Together they
compare an unchunked and forced-split V6 graph through the same traversal and
canonical top-10 path, require an in-frontier cross-chunk edge, prove one
searcher plus truthful chunk receipts for a multi-pack domain, and reject
missing, duplicate, mixed-generation, canceled and indivisible inputs without
mapped-handle leaks. Manifest, coordinator and M8 tests separately require
canonical gap-free IDs, co-location and one domain anchor/search/partial.

```sh
GOWORK=off go test ./TreeDB/collections ./TreeDB/nativewire ./cmd/treedb_vector_partition_bench \
  -run 'Test(VectorPartitionDomainPack|VectorPartitionCoordinator|M8LocalSearchFanoutIsOneGraphPerDomain)' -count=1
```

# Vector partition standalone live-delta verification

The #4324 extension keeps one immutable partition generation bound to the
registered collection `VectorIndex` while acknowledged standalone mutations
publish one atomic live revision and exact source coverage. Verification must
prove mutation visibility and shadowing, one delta search per logical domain,
fail-closed persistence/recovery, bounded reconciliation, request-wide proof
identity, and unchanged replicated-lifecycle behavior. A passing route reports
zero request-path full rebuilds and zero exact fallbacks; base/delta candidate
work and returned-result contribution remain separately attributable.

Coverage:

- `TreeDB/collections/vector_index_partition_live_v1_test.go` covers insert,
  replacement, delete, A-to-B-to-A movement, stale-base exclusion before HNSW
  top-k admission, atomic pinned revisions, repeated-update capacity cutover,
  byte-cap reclaim for single and batch replacements, true unreclaimable-cap
  rejection, pinned-view retirement, publication-barrier immutable-generation
  rebind, monotonic retired-domain epochs across native-root reopen, missing-
  tombstone rejection, V1-to-V3 complete materialization, public-mutation/
  rollback exclusion, snapshot corruption, first-binding durability,
  checkpoint/close/reopen, and command-WAL replay.
  The focused `TestVectorIndexPartitionLive*` family is the canonical local
  lifecycle gate.
- `TestVectorPartitionLiveColdPreparedMutationLoadsDurableOverlayV1` verifies
  an ordinary cold manager restores an acknowledged durable overlay before the
  next actual prepared Append and preserves both owners and the exact durable
  preparation completion through warm publication, cold publication and reopen,
  with zero graph rebuilds. `TestFixedPeerVectorInitializationLiveOverlaySnapshotTailRecoveryV1`
  acknowledges an actual live insert before a real provider snapshot and retains
  the fixture's tail, exact-retry, close/reopen, visibility and strict-search
  checks. It also checks each voter retains its exact local durable completion
  before the snapshot and after tail recovery, and activates serving under the
  existing current-DB/catalog guards. It does not add snapshot persistence commands.
- The V2-focused structural and concurrency gates
  `TestVectorIndexPartitionLiveNativeDeltaTouchesOnlyChangedRecordsV2`,
  `TestVectorIndexPartitionLiveReplayCandidateSynchronizesSharedOwnersV2`,
  `TestVectorIndexPartitionLiveReplayDomainTransactionRollbackRetryV2`,
  `TestVectorIndexPartitionLiveReplayHandoffBlocksOrdinaryPinV2`, and
  `TestVectorIndexPartitionLiveReplayAcceptedFailureInvalidatesV2` prove that a
  mutation emits only compact metadata, one changed owner, and dirty records
  from touched domains; speculative graph edits remain invisible; rollback is
  byte-for-byte deterministic; handoff blocks new pins until the new view is
  complete; and accepted handoff failure invalidates the carrier.
- `TreeDB/collections/vector_partition_persistent_searcher_v1_test.go` proves
  only standalone live generation opens build and charge the stable-ID
  eligibility map; immutable and replicated generation opens retain their
  prior heap/load behavior.
- `TreeDB/nativewire/vector_partition_live_production_v1_test.go` composes a
  real collection generation source, two production shard services, and the
  public coordinator. One logical domain spans two packs and two groups but is
  assigned and searched once. The fixture covers immediate insert/update/
  delete/domain movement, stale-nearest exclusion, warm pack reuse, cold
  post-mutation authority reload, unrelated DB publication, live-identity
  mismatch, checkpoint/close/reopen recovery, and zero exact-fallback/request-
  rebuild counters. The focused production gates are
  `TestVectorPartitionLiveProductionCoordinatorMutationAndColdReloadV1` and
  `TestVectorPartitionLiveProductionCheckpointCloseReopenV1`.

The namespace-backed persistence fixtures are platform-gated; Linux CI is the
authoritative runtime gate. Darwin still compiles them and reports a skip.

`BenchmarkVectorPartitionLiveProductionCoordinatorV1` is a bounded enabling
fixture, not final H-C, paid, or service-capacity evidence. It compares the
same production topology at one and 1,024 live owners with the identical fixed
one-update/one-search sequence. The old snapshot-invalidation path is a
correctness baseline, not a throughput comparison cell. The benchmark measures
the expected first result as recall@1 rather than requiring approximate HNSW to
be exact; lower recall remains visible capacity evidence while execution errors,
fallbacks, and request-path rebuilds fail the run. It also reports `ns/op`,
`B/op`, `allocs/op`, p99 search latency,
achieved writes/searches, base/delta candidate work and result contribution,
logical domains, selected packs, live owner/delta size, cutovers, observed
storage bytes, response-reported pack-heap footprint per operation, reachable
process heap while the live fixture remains open, and exact-fallback,
request-rebuild, and error counts. Use the fixed iteration count and three
repetitions for comparable local results; Linux is required for execution.
The profiling commands record CPU, allocation, and mutex-contention profiles;
the in-benchmark reachable-process-heap metric is the live-fixture retention
observation, while `/usr/bin/time -v` records peak RSS. Neither the allocation
profile nor response pack footprint is presented as retained overlay growth.
These bounded local profiles do not replace the H-C qualification owned by
#4249.

`TestM8ScalingComparisonCompleteBlocksV1`,
`TestM8ScalingComparisonIdentityAndPackingV1` and
`TestM8ScalingLogicalUnionV1` check five complete comparison blocks, failed-row
retention, frozen identity/settings and unchanged logical membership across
physical packing. `TestM8WholeCollectionPopulationV1` and
`TestM8WholeCollectionAdmissionV1` cover complete ordinary-reference outcomes
and refusal before output creation. On Linux,
`TestM8WholeCollectionReadOnlyPublicPathV1` verifies persisted-source reopen,
ordinary public search with worker-owned buffers at c1/c32, path counters,
truth-ID recall, error retention and unchanged source identity. These are
measurement-apparatus checks, not final qualification; the ordinary HNSW
in-process reference is not a same-algorithm/native-TCP comparator. See the
[comparison runbook](../performance/vector-partition-m8.md#fixed-cross-report-comparisons-and-ordinary-reference-4753).

`TestVectorPartitionLiveSelectedLifecycleV1` adds a bounded selected-product
component gate:512 procedural768D rows, approximate routing, selected Vamana
immutable packs and the native HNSW delta, TCP mutations/shard requests, and
independent concurrent readers/writer. It validates canonical truth against
the actual revision/coverage, acknowledged-write freshness, exact physical
pack expansion and every terminal attempt. Insert/delete/domain movement,
metadata-only updates, cold sources, checkpoint-backed durable-ack crash,
reopen and active-generation GC are checked separately, including native
document visibility. A quiesced full-source/partition/router replacement absorbs
the overlay; a held old-generation reader pin fences deletion, and repeated
reclamation plus physical absence and reopened new-generation searches are
checked. Deletion must be observable in the initial ANN result, and cross-domain
movement uses verified live owners, not inferred approximate-router winners.
`TestVectorPartitionLiveLifecycleReceiptRejectsV1` provides hostile population,
freshness, route and current-vector/tombstone score controls.
`TestVectorPartitionLiveBoundedTruthV1` compares the unchanged-top-K plus touched
delta oracle with a complete scan, including ties, replacements and deletions.
See the [component gate runbook](../performance/vector-partition-m8.md#selected-product-standalone-lifecycle-component-gate-4753)
for receipts and the opt-in pinned100K/768D real fixture. The default procedural
corpus and test read proof do not establish real-data scale or replicated/public
live serving. The maintenance-window replacement is not automatic online fold;
adding the real-fixture path does not by itself earn a retained real-data pass.

```sh
GOWORK=off go test -count=1 ./TreeDB/collections -run 'TestVectorIndexPartitionLive|TestVectorPartitionHNSWExcludesMoreThanTopKBeforeAdmission|TestVectorPartitionSearcherExcludesStaleBeforeTopK'
GOWORK=off go test -count=1 ./TreeDB/nativewire -run 'Test(VectorPartitionLiveProduction|VectorPartitionCoordinator|VectorPartitionShardSearch|CollectionVectorPartitionGenerationSource)'
GOWORK=off go test -race -count=1 ./TreeDB/collections -run 'Test(VectorIndexPartitionLive|VectorPartitionHNSWExcludesMoreThanTopKBeforeAdmission)'
GOWORK=off go test -race -count=1 ./TreeDB/nativewire -run 'Test(VectorPartitionLiveProduction|VectorPartitionCoordinator|VectorPartitionShardSearch)'
GOWORK=off go test ./TreeDB/nativewire -run '^$' -bench '^BenchmarkVectorPartitionLiveProductionCoordinatorV1$' -benchmem -benchtime=20x -count=3
PROFILE_DIR=$(mktemp -d /tmp/gomap_4324_profiles_XXXXXX)
GOWORK=off go test -c -o "$PROFILE_DIR/nativewire.test" ./TreeDB/nativewire
for CELL in live_overlay_1_id_update_search_1_to_1 live_overlay_1024_ids_update_search_1_to_1; do
  /usr/bin/time -v "$PROFILE_DIR/nativewire.test" -test.run '^$' -test.bench "^BenchmarkVectorPartitionLiveProductionCoordinatorV1/$CELL$" -test.benchmem -test.benchtime=20x -test.count=1 -test.cpuprofile "$PROFILE_DIR/$CELL.cpu.pprof" -test.memprofile "$PROFILE_DIR/$CELL.alloc.pprof" -test.mutexprofile "$PROFILE_DIR/$CELL.mutex.pprof"
done
```

`BenchmarkVectorIndexPartitionLiveIncrementalPublicationV2` is the bounded
publication microbenchmark for #4720. Both cells replace one stable ID in one
logical domain; only the pre-existing owner count differs (one versus 1,024).
It reports the root-delta record count and bytes in addition to `ns/op`, `B/op`,
and `allocs/op`. The timed path includes persistence acknowledgement through
the maintained O(1) byte/mutation state, then rolls the speculative transaction
back so all operations start from the identical durable base. The expected
result is dirty-neighborhood scaling: the 1,024-owner cell must not serialize,
allocate, or rescan the full owner table or domain graph. It is enabling local
evidence, not service capacity.

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' -bench '^BenchmarkVectorIndexPartitionLiveIncrementalPublicationV2$' -benchmem -benchtime=20x -count=3
```

# Vector partition M6 coordinator verification

M6 verification requires one exact M1/M4 generation per request, deterministic
grouping and chunking, a fixed-size fanout pool, bounded not-leader retry,
effective wall-clock deadline propagation, strict M5 response/read-proof
validation, stable-ID dedupe, deterministic top-k, and all-or-error
cancellation with every started worker joined. Caller, coordinator, and M5
byte/count ceilings must all pass before a successful response is published.

Coverage:

- `TreeDB/nativewire/vector_partition_coordinator_v1_test.go` covers
  deterministic grouping/chunking/dedupe/merge, all-partition parity and stable
  ties, short live corpora, mixed-generation rejection before dispatch,
  corrupt proofs/partials, bounded redirects, terminal sibling cancellation
  with no partial response, actual response-byte/candidate enforcement,
  deadline propagation/clamping, wall-clock cancellation/join, and the
  concurrent-request cap.
- `cmd/treedb_vector_partition_bench/main_test.go` covers the genuine M4/M6
  composition and local-simulation labels, bounded 1M-vector M6 preflight, and
  explicit source-HNSW-degree control with its legacy default.
- `TreeDB/docs/vector_partition_raft_v1_test.go` pins the M6 spec, evidence
  boundary, exact measured-head provenance, and retained-record hashes.

The accepted 1M-vector row is an all-partition correctness row using an
in-process M5-contract simulation and synthetic read proof. It is not network,
production Raft, or M8 evidence. See
`TreeDB/docs/performance/vector-partition-m6.md`.

## M8 opt-in graph-quality attribution (#4744)

`cmd/treedb_vector_partition_bench/m8_coverage_cost_test.go` checks the top-10
mask DP against exhaustive subsets, additive physical costs, simultaneous-budget
counterexamples, checked bounds, cancellation and owned scratch/results.
`m8_no_coarsening_test.go` checks all-pack nearest-member reduction, eligibility
empties, ties, duplicate route rejection and nested actual truth masks.

`m8_quality_integration_test.go` exercises real persisted local packs and fresh
prepared owners, exact canonical-union parity, DP/legacy-oracle parity across
probe counts, trace/ordinary result-and-work parity, replay identity, physical
pack expansion, CLI/child propagation, selected/missing/forged evidence, and
work/memory rejection. These tests preserve the existing public serving policy;
they are not a 100K/250K scaling result, fresh holdout, or Raft qualification.

`m8_quality_replay_review_test.go` verifies that schema-valid forged static
quality evidence in failed-coverage rows is rejected after independent asset
reopen, and that canceled trace preparation does not publish a cache.
`TreeDB/collections/vector_partition_trace_ids_test.go` covers cancellation
before and during the offline ID copy, reader-pin release, retry, and ownership
of returned IDs after close. The shared `BenchmarkM8RouterOrdinaryPathV1` measures
unchanged router-only work as the stacked policy diagnostic's control; it is not
full partition-search or service QPS.

`cmd/treedb_vector_partition_bench/fixture_query_offset_test.go` verifies CLI and
manifest query-range admission, zero-offset manifest bytes/checksum/cache
compatibility, unchanged corpus generation, fresh query ordinals and identities,
and negative/overflow/legacy rejection. The calibration builder test also checks
offset-aware selected queries and retained-source truth parity, with invalid
ranges rejected before corpus allocation. These are provenance/correctness
checks, not an observed fresh-holdout or baseline performance result.

`m8_report_replay_test.go` checks required independent pins before I/O, frozen
fixture/query-offset and argv identities before expensive work, dirty or mixed
source/executable/variant rejection, canonical root containment, escaping
symlinks, report digest and bounded/trailing-JSON rejection. The command reuses
the real production profile, command/executable, truth-anchor, transcript,
resource and retained-asset/attribution verifiers; these boundary tests do not
substitute for a complete successful replay of real retained artifacts. Its
`REPLAY_ACCEPTED_NOT_QUALIFICATION` result does not assert a campaign or baseline
acceptance and does not modify historical `validate-qualification`.

## R reference-topology correction (#4773)

`TreeDB/internal/vectorpartition/router_all_levels_test.go` verifies that a
logical domain is an unrepresented container whose depth-zero nodes are genuine
bucket centroids, and that residual quota is assigned only to splittable
buckets. `router_test.go` covers the deterministic sampled initializer, the
bounded forest-work proof, v3 defaults, and forged build-metadata rejection.
`TreeDB/collections/vector_partition_router_v1_test.go` preserves build,
publication, reopen, pin, and strict old-record rejection across the corrected
multi-root topology. These are product and format gates, not retained
qualification evidence.

## L canonical partition-local HNSW (#4774)

`TestVectorIndexCanonicalInsertPreservesUpperSearchSetForDescent` and
`TestVectorIndexConstructionSearchDescendsWithFullSeedSet` cover full bounded
SEARCH-LAYER descent across construction levels. The local-searcher exact/HNSW
tests cover deterministic public rescoring, non-excluded exact work, and strict
combined score budgets. The version-5 pack test rejects disconnected ordinal
reseeding. Run:

```sh
GOWORK=off go test -count=1 ./TreeDB/collections -run 'Test(VectorIndexCanonicalInsertPreservesUpperSearchSetForDescent|VectorIndexConstructionSearchDescendsWithFullSeedSet|VectorPartitionLocalSearcherV1(ExactStableIDsAndPins|HNSWCanonicalizesFP32TieOrder|PinnedExactPackScanV1)|ColumnHNSWCanonicalPartitionPackDoesNotReseedDisconnectedRowsV5)'
```

Manifest mutation, READY-promotion round trip, explicit graph-variant identity,
and persistent-searcher reopen/corruption tests own VPM1 v6, VRP1 v4, canonical
pack v5, and recovery fail-closed behavior. Live-delta stale-ID exclusion stays
under the standalone live-delta commands above. Shard/coordinator tests cover
score-budget transport, per-query accounting even without optional statistics,
cross-partition exhaustion, and corrupt or over-budget responses. Run:

```sh
GOWORK=off go test -count=1 ./TreeDB/collections -run 'Test(VectorPartitionManifestV1BinaryMutationAndResealMatrix|VectorPartitionReadyPromotionV1CanonicalRoundTripAndReconstruction|VectorPartitionLocalGraphVariantIdentityFailsClosedV1|VectorPartitionPersistentLocalSearcherReopenCorruptionAndPinsV1)'
GOWORK=off go test -count=1 ./TreeDB/nativewire -run '^TestVectorPartition'
```

The M8 schema-7 producer/replay tests bind the manifest graph variant, full
configured population, local score calls and caps, retained identity pins, and
the `REPLAY_ACCEPTED_NOT_QUALIFICATION` boundary. They are harness-readiness
gates only; the preregistered structured-250K retained run and explicit issue
receipt remain the scaling qualification.

```sh
GOWORK=off go test -count=1 ./cmd/treedb_vector_partition_bench -run 'Test(M3ConfiguredPartitionLocalHNSWBuildsCanonicalPacks|M8RetainedGraphVariantUsesManifestIdentityV1|M8ProductionReportRejectsUnexercisedDataGroupV1|ReplayM8Report)'
```

## V connectivity-preserving partition-local Vamana (#4787)

`vector_partition_vamana_v1_test.go` covers both frozen passes, construction
search, outgoing replacement, reciprocal insertion, overflow pruning, alpha
boundary, deterministic ties, candidate cap, current-neighbor union, and the
degree-preserving entry-reachability pass, including an alternate-source case
where the preferred reverse boundary would disconnect a reachable row.
Version-6 pack tests pin that exact one-layer R64/L256 profile identity and
corruption rejection. The ordinary materialize/publish/search/checkpoint/reopen,
recovery, pin, GC, score-budget, and stable-ID tests run through this Vamana
profile because it is the default; M18 HNSW remains an explicit offline
historical variant.

```sh
GOWORK=off go test -count=1 ./TreeDB/collections -run 'Test(VectorPartitionVamana|ColumnVamanaConnectivityPreservingPartitionPackV6|VectorPartitionLocalLayer0ReachabilityRepair|VectorPartitionLocalDefaultMaterializationVariantV1|VectorPartitionPersistentLocalSearcherReopenCorruptionAndPinsV1)'
GOWORK=off go test -count=1 ./cmd/treedb_vector_partition_bench -run 'Test(M0MaterializeVariantV1OnlyAcceptsProductionVariants|M8RetainedGraphVariantUsesManifestIdentityV1|ReplayM8Report)'
```

Acceptance still requires the immutable exact-head structured250K P2/EF96
concurrency-1 and concurrency-32 retained cells. Unit and CI coverage are not
qualification evidence.

## M bounded graph, pack bytes, and retained membership feasibility (#4775)

`TreeDB/internal/vectorpartition` tests bind recursive current-bucket pivot
sampling to the seed, repetition, and canonical bucket membership while
preserving deterministic artifacts and input-order invariance. M3 shard tests
reject zero, duplicate, incomplete, or over-envelope materialized packs before
publication. The M8 feasibility tests compare the existing joint DP with an
independent `P<=2` enumerator, charge complete expanded physical-pack ownership,
bind the whole query population into the work plan, publish immutable evidence,
and replay from the exact retained truth/membership/pack assets. Stale truth or
shard-generation identity fails closed.

```sh
GOWORK=off go test -count=1 ./TreeDB/internal/vectorpartition -run 'Test(RecursivePivotSampling|BuildCanonicalizesInput)'
GOWORK=off go test -count=1 ./cmd/treedb_vector_partition_bench -run 'Test(M3ActualShardPackBytes|M8Membership|M8RetainedMembershipFeasibility)'
```

## M8 same-candidate router policy diagnostics (#4745)

`TreeDB/collections/vector_partition_router_policy_reduce_test.go` checks the
hybrid golden across all 720 permutations and all probe prefixes, unique
representative voting, conflicting duplicate rejection, nearest-width exact
voting, deterministic ties, set/sequence identity, refusal receipts and owned
prefix buffers. The digest-byte golden protects the documented encoding during
allocation minimization.

`vector_partition_router_policy_diagnostic_test.go` covers the real persisted
router's shared candidate path, ordinary-result/work parity, unchanged ordinary
counters, independent owner reopening, invalid selection, cancellation, close,
concurrent readers and result ownership. The ordinary collector is shared, not
reimplemented in a benchmark-only approximate search.

`cmd/treedb_vector_partition_bench/m8_router_policy_experiment_test.go` covers
explicit CLI/config/child selection, full-population coverage refusals, cached
probe versus EF identity, actual physical pack cost, flags/source/query/candidate
replay and independently reopened retained report verification. Work and byte
preflight include actual retained model sizes. Failure receipts cannot be dropped
or replaced by successful-only averages. These tests do not select a production
policy, establish 100K/250K scaling, or release the graph-before-Raft gate.

- Router-policy cache-hit work, dimension-sized scratch and overflow admission:
  `TestM8RouterPolicyResourcePlanChargesEveryPopulationRecheck`,
  `TestM8RouterPolicyResourcePlanChargesQueryScratchAndRejectsOverflow`.
- Cancellation within nearest-width sorting and without partial policy results:
  `TestVectorPartitionRouterPolicyNearestWidthSortCancellation`,
  `TestVectorPartitionRouterPolicyReductionCancellationNoPartial`.
- Combined representative admission and typed full-512-query receipt bytes:
  `TestM8RouterPolicyRepresentativeCombinedAdmissionV1`,
  `TestM8PlannedRouterPolicyReceiptSizeV1`. These source checks preserve the
  original 200M work and 64MiB diagnostic caps; they are not policy outcomes.

- Optional complete identity-neutral pack digest admission and split parity:
  `TestM0ReadCaptureRequiresCleanBuildIdentity`,
  `TestM0CaptureSplitPairRejectsLeakage`. Missing historical hashes do not prove
  full geometry. Empty-ordinal geometry controls do not qualify as locality traces.

## Atomic typed source replacement (#4768)

`TestTypedSourceEmptyReplacementIsAdmittedNoop` proves that the collection
primitive performs normal typed/WAL admission while leaving WAL, root, and
sequence unchanged for an empty scope. Existing `TestTypedSource*` coverage
continues to own atomic publication, failure, replay, delete-only, and graph
visibility semantics.

`TestServiceReplaceSourceByIDLifecycle` exercises the public HTTP/service 5→2→0
lifecycle across lexical, scalar, dense, and hybrid visibility, including an
explicit graph build/ensure boundary. `TestTypedSourceReplaceEmptyCarrierAndRegistry`
pins command 67/v1, the canonical zero-live carrier, required sections, and
LocalOnly deterministic-entry rejection. Python codec/client tests pin one
HTTP/native request, fail-closed capability use, response counts, and structured
ambiguous/recovery errors. `BenchmarkTypedSourceReplacement` and
`BenchmarkTypedUpsertDecode` remain the bounded core/decoder performance gates;
no separate application harness is introduced.

## Typed metadata-only update (#4769)

`TestCollectionTypedMetadataPayloadGoldenRoundTrip` and its corruption suite pin
format 13's canonical metadata after-images, owned decoding, truncation/bounds,
and absence of vector-dimension-dependent bytes. Collection tests own atomic
old-or-new replay/publication, no-op, missing-ID, repeated-update, reopen/fold,
and preserved scoring/vector authority.

`TestTypedMetadataNormalizedColdFoldRaceAndGC4769` covers normalized SQ8 scoring
across base/suffix metadata updates, cold reopen, racing fold rejection/retry,
column/value-log GC, pinned old metadata, and subsequent replacement/deletion.
`TestTypedMetadataWALRecovery4769` injects a durable-intent failure and verifies
the recovery fence and replay. Invalid-batch and protected-afterimage tests
reject partial/prohibited updates. `TestTypedMetadataDimensionIndependentPayload4769`
compares batches 1/32/128 at 8/768 dimensions without vector-bearing plan values.

`BenchmarkTypedMinimaMetadataMutation` and `BenchmarkServiceTypedMetadataMutation`
measure core and service admission/publication separately on the existing typed
fixtures, including WAL payload, row assets and request JSON bytes. These are
bounded local diagnostics (`-benchtime=10x -count=3`), not serving-throughput
qualification. Setup/ingestion is untimed; no new benchmark driver is required.

`TestTypedMetadataUpdatePublicLifecycle` exercises the public service shape and
exact matched/modified counts without vector input.
`TestTypedMetadataUpdateGoldenBoundsAndRegistry` pins command 68/v1, the bounded
strict JSON request, sections 142/143, independent capability advertisement,
and LocalOnly deterministic-entry rejection. Python unit/integration tests pin
one HTTP or negotiated native request, no fallback, no vector carrier,
structured ambiguous/recovery errors, and unchanged content/vector retrieval.

`TestProductionRetrievalSourceAndACLFilterLifecycle4765` is the parent graph's
small integrated service/HTTP acceptance fixture: literal filtered AND BM25,
selected SQ8 plus packed canonical reranking, default vector-free responses,
atomic source shrink, ACL-filter visibility changes without vector input, and
reopen. BM25, dense and hybrid agree on the caller-filtered live set; stale chunks
do not return. This proves metadata eligibility filtering, not an independent
server-side authorization policy. Small selective allow-sets retain their
truthful typed-exact route.
This fixture lives with the final metadata child, not in a separate harness PR.


Draft source-snapshot V2 checks in `internal/vectorpartition/source_snapshot*_v2_test.go` compare the bounded streaming Merkle accumulator with a separate small-fixture tree, exercise checkpoint resume at every chunk, reject changed identities, missing/reordered/duplicate rows, corrupt/truncated codec bytes and excessive row/ID lengths, preserve original-source ordinal provenance, and decode a bounded local chunk when the declared global source row count reaches uint64 maximum. The scoped owner-local workflow runs these tests repeatedly and under race instrumentation, and records chunk verification/decode allocations. These checks establish the source codec, not whole-path boundedness. Public import and paged producer coverage is mapped separately; whole-path memory, failure cleanup and performance acceptance remain required by #4808.
## Sparse catalog runtime

| Invariant | Test / harness |
| --- | --- |
| Nonvoting, storage-free ingress reaches production durable remote ownership without wrong-group mutation | `TestSparseCatalogNonVoterIngressRoutesWithVerifiedProofV1` |
| More than 32 inventory nodes do not enlarge catalog voting or local hosting | `TestSparseCatalogConfigAcceptsMoreThan32NodesWithThreeVotersV1` |
| Oversized inventory/metadata/local hosting refuses before stores open | `TestSparseCatalogResourceBoundsRefuseBeforeOpeningV1` |
| Stable cluster identity never waives exact persisted topology | `TestSparseCatalogExplicitIdentityRequiresExactReopenV1` |
| Missing, conflicting, future and stale metadata refuse; consumer publication/read authority refuses; epoch refresh reacquires authority | `TestSparseCatalogConsumerRejectsTamperedRouteV1` |
| Nonvoting data owner survives exact restart/catalog leader failover, then refuses fresh writes after authority loss | `TestSparseCatalogNonVoterDataOwnerFailoverAndAuthorityLossV1` |
| Global client admission is independent and bounded | `TestSparseCatalogClientAdmissionIsBoundedAndIndependentV1` |
| Saturated ingress/forward capacity cannot force nested authoritative read RPCs | `TestSparseCatalogSaturationPreservesAuthoritativeReadProgressV1` |
| Closing a runtime interrupts an already accepted idle Raft connection without waiting for a remote node to close | `TestSparseCatalogRuntimeCloseInterruptsIdleRaftConnectionV1` and sparse catalog race tests |
| Matched all-voter baseline/candidate and enabled consumer cost with process resources | `BenchmarkSparseCatalogRemoteOwnerCreateV1`, `.github/workflows/sparse-catalog-qualification.yml` |

The 40-node inventory case is configuration/control-plane evidence, not 40 live
machines. These local cases do not close distributed ANN, authentication, EC2
failure-domain, or horizontal-scaling qualification in #4805/#4250/#3983.

Source-map V2 pure token/coverage/codec checks live in `internal/sourcepartition`; `raftplacement` separately checks every referenced group against its resolved catalog. `TestSourceShardMapCanonicalCodecAndPriorDigestV2` freezes the pre-extraction V2 digest and refuses unknown, duplicate, changed or noncanonical encoded content. Collection-side validation of a map is not publication or source-root authority.


Draft `TestVectorPartitionSourceImportAtomicResumeAndReplayV2` exercises public typed source import, exact and changed retry, source ordinal order across sorted typed WAL payloads, checkpoint/reopen and source-root equality. `TestVectorPartitionSourceImportRejectsGapDuplicateAndWrongOwnerV2` covers nonowner/group refusal and durable cross-range exact-ID uniqueness. `TestVectorPartitionSourceImportPublicationBoundaryV2` injects before/after-publication failure to require rows/progress/receipt atomicity and unambiguous exact retry. `TestCollectionSourceImportPayloadV2` covers bounded payload sections, truncation, frame registry and allocation-free envelope validation. The immutable source seal and paged source session consume this durable progress. Full acceptance still requires current-head hosted success and measured public owner-only build/open bounds.


Draft incremental source-directory coverage adds `TestVectorPartitionSourceImportUsesIncrementalDirectoryV2` (actual public-path semantic red before implementation), `TestVectorPartitionSourceImportRetainsAuthenticatedBytesAfterCheckpointV2`, `TestVectorPartitionSourceImportBoundsBeforeWALV2`, `TestVectorPartitionSourceImportRefusesLegacyMutationBeforeWALV2`, and `TestCollectionSourceImportCompleteCommandBudgetV2`. Persistent proof nodes and SCL2 rows are compared with an independent complete test tree across partial/power-of-two shapes. `BenchmarkVectorPartitionSourceImportDirectoryV2` uses the same public import with 32 versus 1024 prior chunks, reports allocation and actual directory/fallback record reads, and must not be interpreted as EC2 qualification. Directory reads alone do not establish whole-call resource bounds; public owner-load/build, source/graph lifetime and failure cleanup have separate gates.


`TestVectorPartitionSourceImportRetainedSealAndGCV2` exercises a completed source reader held across another source revision, checkpoint, immutable seal reopen and destructive asset GC. The V2 lifecycle reader verifies the retained source directory before asset reclamation; platforms without stable relative namespaces refuse destructive GC while retaining reader/reopen coverage. `BenchmarkVectorPartitionSourceImportDirectoryV2` now reports source leaf bytes/rows, directory and fallback reads, selected DPM stream bytes/items, encoded dependency bytes and whole-call allocations. Existing logarithmic COW work and full retained dependency serialization must be analyzed separately; no constant whole-call or distributed readiness claim follows from constant directory reads.


The opt-in `dependency_directory_v2` required format feature activates a third
COW B-tree for physical descriptors and globally keyed logical obligations on
new stores. `TestDependencyDirectoryV2RequiredFeature*` covers populated-V1 and
dirty-WAL refusal, feature removal refusal, and read-write/read-only/no-lock
reopen. `TestDependencyDirectoryV2UnknownRequiredFeaturePrecedesStorageDecode`
requires unsupported-feature refusal before malformed root or WAL decoding,
including `IgnoreFormatConfig`. Selected-root page bounds and ordinary page
checksums protect directory traversal; the directory is not a Merkle commitment.

`TestVectorPartitionSourceImportDependencyEncodingDoesNotScaleWithHistoryV2`
requires persisted DPM2 and nonzero changed-record-byte and COW-page counters
across public import plus checkpoint at 32 and 1024 prior chunks, including
reopen and coalesced imports. Changed-record bytes include changed keys/values
and deletion keys, not all descriptor comparison or binding work. No-op
publication leaves mutation/page counters unchanged. Allocation, CPU and
retained-memory comparisons remain separate performance gates.

`TestDependencyDirectoryV2SealRecoveryBothSlotsAndCorruptFallback`,
`TestDependencyDirectoryV2OldReaderSlotsAndSharedSubtreeReclamation` and
`TestDependencyDirectoryV2RebuildPreservesBothSlots` cover streaming recovery,
both fallback slots, retained readers and shared-page reachability.
`TestRebindDurableRootSnapshotV1PreservesBothSlotsAndExactTargetIdentity` covers
V1 and DPM2 staged snapshot rebinding: only fixed-width physical identity values
change in the validated private copy; logical keys, page layout, both slots and
lineage remain intact and affected page checksums are recomputed.

## Authenticated fixed-peer transport and operations (#4813)

Listener ownership through bootstrap (#4929):

- `TestFixedPeerFixtureRetainsBootstrapListenersV1` checks every advertised
  control, catalog/data Raft, public-vector and hosted-shard role across the
  shared fixture inventory. Its unchanged witness fails against the released
  allocator and passes with retained reservations.
- Linux-only `TestFixedPeerListenerOutboundCollisionControlV1` records the actual
  outbound local/remote addresses, owning PID and Linux socket inode for the released
  control, then verifies its bind failure and retained-reservation rejection.
  Darwin permits this source-port/listener pairing; the common all-role competing
  bind witness remains the portable ownership control.
- `TestFixedPeerListenerOwnershipFailureCleanupV1` covers invalid configuration,
  persisted manifest refusal, a nil supplied listener, partial bind failure and
  drain refusal. Existing subprocess, restart, replacement, standby, security,
  sparse-catalog and multi-owner lifecycle tests exercise the same ownership seam.
  Unix fixtures transfer held sockets; Windows subprocess builders select sockets
  in their eventual child before publishing each address. Neither introduces a
  release/rebind gap between fixture selection and runtime ownership.
  Reserved cold shard sockets do not authenticate or serve until backend
  authority checks succeed; observation still cannot warm the backend or READY.
- `TestFixedPeerDormantListenerTemporaryAcceptRefusalV1` injects a wrapped
  temporary Accept error then requires real TCP refusal to recover; its unchanged
  witness fails against the exited baseline pump with a bound, blackholed socket.
  `TestFixedPeerDormantListenerInterruptsAcceptBackoffV1` reaches the 1s backoff
  cap, then checks prompt exact-socket takeover/Close, pump exit and one owned
  socket close.
- `TestFixedPeerDormantListenerRefusalTakeoverV1` repeats prompt cold refusal,
  exact-socket takeover, pump termination and cleared-deadline serving.
  `TestFixedPeerDormantListenerFailureCleanupV1` checks close and interrupted
  takeover close the socket exactly once and release the address.
  `TestFixedPeerConsumedListenerCleanupV1` and
  `TestFixedPeerListenerChildStartFailureCleanupV1` check consumed-transport
  rejection/close and failed child startup cleanup. The Windows-only
  `TestFixedPeerWindowsStagedListenerOwnershipV1`,
  `TestFixedPeerWindowsUnactivatedListenerCleanupV1`,
  `TestFixedPeerWindowsStagedConfigFailureCleanupV1` and
  `TestFixedPeerWindowsStagedProtocolErrorCleanupV1` cover continuous child
  ownership, activation, exact-address restart and cancellation/config failure.
  Cross-compilation alone does not establish Windows runtime coverage.

Windows staged reply sharing (#4929):

- `TestFixedPeerWindowsReplyReadSharesRenameHandleV1` holds a DELETE-access handle
  on each published allocation/activation reply. It requires the former standard
  reader to fail with `ERROR_SHARING_VIOLATION` and the actual shared-delete
  `fixedPeerWindowsWaitReplyV1` reader to decode the same reply while the handle
  remains held, repeated 20 times. This control requires native Windows execution;
  a Linux copy of the staging protocol provides protocol coverage only.

Reproduce focused controls with
`GOWORK=off go test ./TreeDB/nativewire -run '^Test(FixedPeerFixtureRetainsBootstrapListeners|FixedPeerListener.*|FixedPeerDormantListener.*)V1$' -count=3 -v`,
and repeat with `-race`. `BenchmarkFixedPeerTCPRuntimeStartupV1` measures the
public opener with identical base/head harnesses; the existing
`BenchmarkFixedPeerTCPRemoteOwnerCreateV1` covers the ordinary steady-state path.
Port/directory allocation and close are outside the startup timer, and the
remote-owner benchmark reports parent/client allocations rather than child
process allocations.
`TestFixedPeerDormantListenerResourceBoundsV1` reports temporary goroutine,
descriptor and allocation costs for 32 dormant wrappers with raw socket setup
excluded (descriptor deltas are unknown when the platform cannot measure them).
Production cold roles additionally hold one socket per configured dormant role
from bootstrap instead of allocating it only on activation. The identical
base/head `TestFixedPeerColdRoleBootstrapResourceV1` audit measures the public
opener for one cold immutable owner after fixture setup has released its sockets;
it reports actual cold socket occupancy and total startup FD/goroutine/heap/allocation
counts. These Go process-global allocation figures include background startup
work, and syscall descriptor counts are platform dependent. Consumption leaves
the ordinary serving path unchanged. The copied sparse benchmark keeps three
private config/open/start callbacks with standalone base defaults,
installed only by candidate test helpers so held fixture sockets reach children.
This setup bridge runs outside the unchanged timed measurement body.

- `TreeDB/nativewire/peer_*_test.go`, `fixed_peer_security_v1_test.go` and
  `vector_partition_global_connection_budget_v1_test.go`: actual TLS/control,
  Raft, snapshot, native/shard boundaries; identity/group denial; bounded sockets,
  bytes and proposal/snapshot lifetimes; hot-group/cold-group progress; cancellation;
  quorum-backed readiness, drain, immutable configuration and paired-root loss.
- `TreeDB/nativewire/vector_partition_fixed_peer_authenticated_shard_v1_test.go`:
  private authenticated immutable shard topology, wrong certificate/group and
  plaintext refusal, plus an environment-gated two-host asset/search proof.
- `cmd/treedb-fixed-peer/operations_test.go`: plaintext refuses by default and
  executable identity mismatch refuses before stores/network work.
- `scripts/treedb_peer_ec2_test.py`: failure-domain/capacity/cost refusal,
  provider inventory checks, exact plan/change-set execution, wrong-tag refusal,
  partial-provision cleanup and idempotence. Fake-provider contract tests do not
  establish live AWS service acceptance.
- `.github/workflows/peer-security-qualification.yml`: exact-head focused/race
  gates, existing retained M8 resources, replicated TLS writes/shutdown, and
  equivalent public plaintext/TLS allocation/process-resource measurements.
- [Fixed-peer operations](../operations/fixed-peer-ec2.md) and
  [evidence](../evidence/peer-security-4813/README.md) distinguish generic substrate
  conformance from #4250 multi-host performance and #3983 fault evidence.

## Immutable ACTIVE replacement authority continuity (#4811)

`TestCatalogReplicaReplacementActiveImmutableRebindAndRestoreV1` checks
catalog epoch/digest rebinding of an immutable ACTIVE record, unchanged READY
receipts, a new ready-set digest, exact retry, and pending/final snapshot replay.
`TestCatalogReplicaReplacementRefusesPendingMutationAndBuildingV1` checks the
pending-mutation, BUILDING, and source-group guards. The existing
`TestCatalogReplicaReplacementSerialCompletionSnapshotAndNextV1` checks that
ordinary feature activation over older completed replacement evidence refuses
both as a command and a forward snapshot. These are authority-only checks;
ordinary fixed-peer vector replacement remains unavailable.

`TestCatalogOwnerPreparationIdentityAndPhaseCapV1` covers unmarked owner,
changed/incomplete identity, source-group refusal, allowed preparation phases,
exact retry, accessor alias isolation, cold restore, both marker/ACTIVE evidence
erasure refusal (empty snapshot without pending replacement stays valid), and direct/forward-import
promotion/removal refusals with retained or erased markers.
`TestCatalogCompletedReplacementHistoryAllowsLaterOwnerBindingSnapshotV1`
completes an unrelated ordinary replacement with lifecycle support already
enabled, then commits a new immutable ACTIVE index that uses that group and
checks exact cold/known snapshot replay. Completed ordinary history grants no
owner preparation authority; marked history retains its permanent phase cap.
`TestCatalogPendingReplacementSnapshotRequiresCompleteBeginAdmissionV1` uses
real pre-BEGIN lifecycle/mutation commands and verifies atomic cold/known refusal
for a pending ordinary replacement combined with BUILDING or mutable ACTIVE
state, a pending fence/barrier, or incompatible canonical/token placement.
The genuine ordinary pending control remains admissible; completed histories
remain covered separately.
`TestImmutableOwnerReplacementPreparationCapabilityV1` is the baseline-compatible
real-Raft capability producer: the baseline refuses at public replacement BEGIN
before seed/install, whereas the candidate must install and enroll only a nonvoter.
`TestImmutableOwnerReplacementInstalledAssetsTailAndRestartV1` checks actual current-DB
hosted scope/assets, post-enrollment semantic tail, restart/exact BEGIN binding,
unchanged static configuration, old voter retention, and no topology, public listener,
READY, or public hits. `TestImmutableOwnerReplacementValidTailWithoutHostedAssetRefusesV1`
retains native semantic tail progress while removing a declared hosted asset and
requires cutoff/tail refusal; promotion remains closed independently. These
preparation checks do not prove completed replacement or serving cutover. The
installed-assets fixture logs BEGIN/native-install/nonvoter preparation time,
native snapshot and hosted segment bytes, then three warmed semantic-tail calls.
Its process-global allocation counters include the caller, all five in-process
Raft servers, and background work; they provide no per-node allocation or RSS
attribution. There is no working old owner baseline, and race instrumentation
is diagnostic only. Healthy status/search implementation files are unchanged.
No stable performance or scaling claim is made.


`TestImmutableOwnerReplacementPrivateSourceWarmV1` checks explicit private
initialization after the genuine installed-seed DB swap, generation/searcher
cache reuse, fresh semantic-tail admission, nonvoter/old-voter membership,
typed public/phase/identity/authentication refusals, exact authenticated target
tail-read success with wrong-node/unmarked refusal, cold restart, and retirement
after a hosted file disappears between actual worker completion and polling,
or after an actual native snapshot DB replacement. The existing source authority
test seam blocks an actual cached worker; runtime Close cancels it, waits for its
return, and closes the private cache.
Status/readiness and public listeners remain cold. The pending operation's
existing lifecycle freeze prevents a legal ACTIVE replacement; this test does
not fabricate one. Cold/cache timing and process-global allocations are bounded
diagnostics including the caller, five Raft runtimes, background work and control
polls. Source Stats count cached searchers: exactly one cold partition miss
per hosted domain and one cached hit per domain, with generation/partition misses
unchanged on reuse. This fixture's one hosted domain covers two physical packs;
its first pack ID opens the domain root and all physical sections. Hosted physical
pack count is reported separately. Verified declared hosted artifact bytes deduplicate
identical namespace/file/offset/length references; they are not retained memory.
These diagnostics are neither per-node overhead nor a throughput/comparative claim.
`TestCatalogReplicaReplacementSnapshotRejectsExcessInvalidationRevisionsV1`
requires exact reducer revision distance across a compacted snapshot.
`TestCatalogReplicaReplacementSnapshotRejectsForgedNewActiveReadyV1` and
`TestCatalogReplicaReplacementSnapshotRejectsNewActiveBeforeConfirmedFenceV1`
reject direct ACTIVE READY/source forgeries while accepting genuine cutover
and confirmed-fence controls. `TestCatalogReplicaReplacementSnapshotAcceptsCleanedIntermediateCutoverV1`
preserves legitimate A-to-B-to-C catch-up with cleaned B; it does not establish
durable activation provenance for arbitrary intermediate histories.
The one-step forward snapshot test also rejects a self-canonical extra
terminal record and confirmed fence. Same-epoch preparation tests accept
committed STAGED-to-PREPARED progress but reject a rewrite of locally known
READY evidence. The genuine compacted PREPARED-to-cleanup control remains
accepted with the inherited terminal-provenance limit described in the
protocol spec.
The collection-barrier snapshot tests accept valid post-completion mutations,
reject unwitnessed epoch jumps and stale receipts without mutating authority,
and cover empty authority maps and the bounded receipt window.
`TestCatalogReplicaReplacementPendingRefusesNewCollectionMutationV1` refuses a
new BEGIN before barrier debt while retaining exact BEGIN/CONFIRM retries.
`TestCatalogReplicaReplacementSnapshotReservesEveryPhaseV1` accepts genuine
known/unknown same-epoch phase catch-up and anchored completion, while refusing
one missing applied entry across every mandatory phase and post-completion
BEGIN/CONFIRM. These refusals preserve the local exported snapshot.
`TestCatalogReplicaReplacementSnapshotAcceptsLegacyAdmittedBarrierConfirmationV1`
models already-owned old-producer barrier debt explicitly and retains exact
BEGIN retries, owned CONFIRM, completed catch-up, and applied-entry budgets.
Cold and known snapshots combining pending replacement and barrier debt refuse
without authority mutation. The current reducer refuses the old
BEGIN-after-replacement sequence; these controls establish neither migration
nor an actual data outcome.
Known-lifecycle budget tests accept the genuine six-entry ACTIVE-to-ABSENT
history and reject its one/five-entry snapshots without authority mutation.
Known records require the sum of their revision advances, including independent
commands within the same Index. A pure-reducer-proved two-record atomic cutover
shares one entry; both compacted suffixes must validate even when ACTIVATE's
digest has been overwritten. Accounting is conditioned on retained overlap
proofs, not an unconditional mathematical lower bound. Discounts start from
locally ACTIVE predecessors; tight budgets can refuse multiple compacted
cutovers through already locally PREPARED candidates whose further overlaps
cannot be proved. Incoming-only and erased history retain provenance limits.
Existing ordinary cleanup, incoming-only A-to-B-to-C cutover, and mixed
65-barrier catch-up remain positive controls.
`TestCatalogSnapshotKnownPreparationCleanupReachabilityV1` uses real BUILDING,
STAGED, and PREPARED abort/cleanup producers and refuses skipped terminal
revisions or forged final command digests without authority mutation.
`TestCatalogSnapshotKnownPreparationServingCatchupV1` retains genuine preparation
through activation, invalidation, confirmation, retirement, and completed cleanup.
`TestCatalogSnapshotIndependentSameIndexLifecycleBudgetV1` refuses two independent
same-index commands in one applied entry.
`TestCatalogSnapshotCompactedCutoverIndependentSuffixBudgetV1` preserves atomic
cutover sharing while charging independent commands on both resulting records.
`TestCatalogSnapshotMixedLifecycleBarrierProgressV1` accepts genuine mutation and
direct-invalidation epoch-jump producers, and refuses short mixed budgets and
forged confirmed maximum-epoch barriers. ABSENT's erased READY receipts are not
authenticated by these checks. These are snapshot-admission correctness tests;
they make no runtime replacement or quantitative performance claim.
The compacted-cutover test also refuses activation without predecessor
retirement even when the forged snapshot offers spare applied entries; its
predecessor is independently invalidated and confirmed, so canonical decoding
alone does not refuse the forgery.
`TestCatalogSnapshotLegacyAmbiguousCutoverBudgetV1` uses an admitted legacy
source-alias producer: unchanged history remains valid, but ambiguous
Index+generation pairs deterministically receive no atomic-entry discount.
Repeated genuine terminal INSTALL controls retain an unchanged PREPARED source
alias and a genuine incoming-only BEGIN that supplies the spare applied entry.
Exact candidate identity permits catch-up; eligible serving suffix selection
ignores preparation aliases and refuses competing terminal aliases. Canonical
source-epoch, final-digest, missing atomic retirement and ambiguous predecessor
negatives preserve the admission trust checks.
`TestCatalogSnapshotInitialActivationServingNameGuardV1` refuses a canonical
confirmed terminal candidate below an unchanged ACTIVE source watermark, while
accepting genuine invalidation/confirmation/retirement followed by initial
activation and a later confirmed mutation. It also refuses a genuinely produced
older unconfirmed predecessor substituted into that final confirmed snapshot,
with spare applied entries and a fresh canonical-decoding precondition.
Known-predecessor supersession chains retain bounded provenance limits; these
checks do not authenticate all erased activation ordering.
`TestCatalogSnapshotMixedBarrierInvalidationOrderingV1` uses real producers
before each canonical short-budget refusal. It covers late invalidations above
the final barrier or between its first retained BEGIN and final epoch, a jump
below the retained window that still leaves an evicted entry to charge,
multiple earlier/later known invalidations, new pending BEGINs, and locally
pending confirmation displaced by later receipts. Genuine invalidation before
many barriers and a pending-only barrier retain positive controls. Only
invalidation epochs strictly below the earliest new retained BEGIN are
compatible jump floors; the maximum qualifying candidate is used rather than
the final maximum. This bounds a possible reducer history and does not
authenticate actual erased ordering or command counts.

## Fixed-peer immutable multi-owner serving, bounded profile (#4809)

`TestMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1` starts real separate
source-holder, ingress-router, and two owner-leader processes with the catalog
and source-data leaders co-located. `TestMultiOwnerTCPDomainSearchWithSeparateCatalogAndSourceLeadersV1`
uses verified peer credentials and puts the catalog leader on ingress while
the source-data leader remains on the full-source holder. These tests check a
multi-chunk domain, disjoint hosted-only files and zero source rows at serving
nodes, real catalog BUILD/owner Stage/READY/PREPARE/ACTIVE, public strict-search
IDs and scores against the existing local live coordinator on the same prepared
generation with no intervening writes, one HNSW traversal per selected domain,
no partial result when a selected owner is unavailable, and owner restart on
the same generation. The separate-leader test also rejects a credentialed
non-catalog caller before BUILD; source-holder focused tests cover current FSM
source binding, stale identity/catalog proof, cancellation, and bounded
attestation size. `TestFixedPeerImmutableDefinitionAndMutationRefusalV1`
checks the durable index epoch/incarnation admission and mutation refusal.

The owner-only catalog-consumer prerequisite for #4811 has the following
additional verification mapping. Source-holder BUILD and the public ingress
router remain catalog voters; only the configured immutable owner `owner-b`
is excluded from `Catalog.Peers` before genesis.

| Test | Covered boundary |
| --- | --- |
| `TestMultiOwnerTCPDomainSearchWithCatalogConsumerOwnerV1` | Four real processes complete BUILD/Stage/READY/PREPARE/ACTIVE and public strict-search parity with hosted-only owner assets, zero serving-source rows, no owner-local catalog files, and no local catalog applied index or raft group. After a positive direct-owner request, cached and cold quorum loss refuse candidates and READY; cold status/readiness observation does not authenticate or serve the reserved shard endpoint. Explicit lifecycle recovery restores serving after restart. |
| `TestMultiOwnerTCPDomainSearchWithCatalogConsumerInvalidationV1` | The same initial positive consumer setup uses the existing in-flight invalidation control: public strict search refuses invalidated results, and the already-warm consumer refuses candidates and READY under the previous ready digest. |
| `TestFixedPeerImmutableVectorCatalogDecisionBindsIdentityAndReadyV1` | Fresh decisions bind configured catalog/index/source/manifest/placement/owner identity and complete READY evidence. Negative controls retain strict voter requirements for mutable, router and non-owner configurations, and reject absent or mismatched credentials before local stores are opened. |
| `TestFixedPeerImmutableOwnerReadCostV1/voter` and `/consumer` | The same bounded sampler observes owner status and public two-owner search: ten measured operations per loop, parent allocations/bytes/wall time, and existing child process resource logs. Consumer capability has no working old baseline. Parent allocations do not measure child/server allocations; child CPU/RSS cover the whole correctness/recovery case, not isolated steady reads. These samples do not establish a stable latency, throughput, or recall qualification. |

`TestFixedPeerTCPSnapshotRestoreTracksCurrentCatalogVersionV1` also checks
`authority_unavailable` when snapshot restore replaces the owner-bound FSM DB;
the cold consumer status control requires `topology_unavailable` before Warm.

The retained tests-only baseline consumer control fails at the runtime
constructor with `fixed-peer vector runtime requires local catalog authority`,
before genesis or Ensure/Stage; this is capability refusal, not a lifecycle
Stage failure. Its unchanged voter control passes. Consumer admission uses a
fresh authenticated quorum-backed catalog read without a local vote or cached
authority grant. Public strict search retains its final ACTIVE/current-DB
check after remote results; private owner requests use fresh admission and the
current-DB boundary without an additional post-search catalog barrier. This
prerequisite does not complete #4811 snapshot, replacement, or transfer gates.

`TestMultiOwnerTCPAcceptedModelFreshIndexEpochV1` reuses the four-process,
hosted-only public flow with a fresh deterministic 64-row, 768-dimension source,
three packs, two domains and two owners. Before collection creation it declares
index epoch 1 and the accepted source definition parameters: cosine, float32,
column_graph, M16, EfConstruction128 and EfSearch128. It checks the persisted
epoch and exact definition digest, the distinct historical epoch-zero digest,
and the canonical connectivity-preserving Vamana R64/L256/alpha1.2 graph marker.
Its query uses TopK4, Probes2 and EfSearch8. This is fresh-fixture public TCP
parity coverage; it does not qualify the retained epoch-zero 100K/D16/P64
fixture, historical P5/EfSearch96/TopK10 recall, a 95% quality threshold, or
performance. Historical fixture builders and accepted bytes remain unchanged.

`TestMultiOwnerTCPAcceptedModelScaledCorrectnessV1` retains that explicit
index-epoch-1 source definition and canonical model, with a fresh deterministic
1,024-row, 768-dimension source, three packs, two domains and two owners in four
real processes. The two domain populations are 683 and 341, both above L256.
Its 32 declared source-document queries use ordinals `(37 + 29*q) % 1024` for
`q=0..31`, TopK10, Probes2, EfSearch96 and MergeEntries32. Every query checks
public IDs/scores against the same-generation native reference and exact
selected-domain/group/RPC counters with no exact scan. One representative query
covers nonrouter refusal, owner loss without partial results, and owner reopen
parity. Hosted-only inventory and zero serving-source rows remain required.
This is public correctness parity, not held-out recall, retained 100K/D16/P64 or
historical P5 qualification, a 95% quality threshold, or performance/latency
qualification. Historical source builders and accepted assets remain unchanged.

These real TCP fixtures also require nonzero native router scoring and exact
public/native router score-call, candidate and edge counter parity on every
query. `TestVectorPartitionWireV1RoundTrip` checks distinct multibyte router
counters, all existing stage timings, and refusal of every truncated response
prefix through the shared strict/fast/pinned response codec.

Run the 4-row baseline, 64-row fixture and scaled fixture together in normal and
race modes on the reviewed candidate:

```sh
GOWORK=off go test -count=1 ./TreeDB/nativewire -run '^TestMultiOwnerTCP(DomainSearchUsesOnlyHostedAssetsV1|AcceptedModelFreshIndexEpochV1|AcceptedModelScaledCorrectnessV1)$'
GOWORK=off go test -race -count=1 ./TreeDB/nativewire -run '^TestMultiOwnerTCP(DomainSearchUsesOnlyHostedAssetsV1|AcceptedModelFreshIndexEpochV1|AcceptedModelScaledCorrectnessV1)$'
```

Current-FSM-DB restore and stale shard-response tests cover bound-handle
invalidation separately. Run the focused nativewire selectors on the exact
candidate with:

```sh
GOWORK=off go test -count=1 ./TreeDB/nativewire -run 'Test(MultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1|MultiOwnerTCPAcceptedModelFreshIndexEpochV1|MultiOwnerTCPDomainSearchWithSeparateCatalogAndSourceLeadersV1|MultiOwnerTCPDomainSearchWithCatalogLeaderOnSourceFollowerV1|FixedPeerImmutableDefinitionAndMutationRefusalV1|FixedPeerTCPSnapshotRestoreTracksCurrentCatalogVersionV1|VectorPartitionShardSearchRejectsDBReplacementBeforeResponseV1)$'
```

This bounded profile uses loopback vector listeners with peer credentials;
cross-host authenticated vector sockets are not qualified. A stale-ACTIVE and
corrupt-owner fault matrix, scoped
reclaim/reopen breadth, and matched enabled-path latency/CPU/alloc/RSS and
catalog-RPC measurements remain #4809 acceptance work. Local-vs-TCP ID/score
parity is not a holdout-recall or throughput result.

## Accepted-disjoint public qualification harness (#4809)

`TestMultiOwnerTCPAcceptedDisjointGeometryV1` reuses the existing fixture and
four-process public helpers with 64 rows, 768 dimensions, 64 placements and
16 whole domains across two owners. Two queries use P5, EfSearch96, TopK10,
MergeEntries256 and exact centroid routing with ScoreBudget256, while exact
vector-row scans remain forbidden. Native/public parity, selected counters,
hosted-only assets, nonrouter refusal, selected-owner loss without partial
results and same-generation owner reopen exercise the larger geometry. The
existing three-pack/two-domain wrappers retain P2, two groups/two RPCs and their
1e-5 score tolerance.

`TestVectorPartitionLiveRetainedGeometryV1` preserves the strict selected
20-percent-overlap default and its negatives, and separately checks explicit
`graph-disjoint-v1` admission at 100K/D16/P64 with zero overlap. The mutable
lifecycle caller binds the default recipe before opening a DB; fixture JSON
cannot widen it. `TestFixedPeerTCPRuntimeConfigFileV1` checks private 0600 child
configuration transport above the inline environment limit, at the existing
8 MiB cap, and refusal of malformed, trailing or oversized JSON.

`TestMultiOwnerTCPAcceptedDisjointRetained100KV1` is an opt-in harness requiring
a root-approved fresh epoch-one M3 copy and pinned descriptor, queries and
truth through `GOMAP_SELECTED_LIVE_FIXTURE`. It binds the actual definition and
manifest integrity to the accepted source/assignment/graph recipe, then uses
the existing owner-separated materializer once to produce a new global
generation and router. Source M16/128/128 is distinct from local compatibility
32/256 and canonical Vamana R64/L256/alpha1.2. Its 512 previously examined
queries use the parameters above, with at most 16 public warm queries, exact
ID/order/float32-score and algorithm-counter parity, unique IDs and pinned
recall >=0.95. Original source/assignment identities and fresh owner-separated
asset/READY/router identities are recorded separately. Historical epoch-zero
bytes remain checksum-only. Adding this harness establishes no 100K pass,
held-out recall or performance claim; expensive collection requires its own
reviewed recipe, resource limits and allocation.

### Retained-copy snapshot installation

`TestVectorPartitionRetainedSnapshotInstallV1` creates two forced-pointer
values, checkpoints and closes the source, proves that ordinary recovery
rejects a plain copy, then exercises the retained caller's explicit staged-copy
install and reads both values across reopen. It checks invalid pins, an original
source path, changed pathsets, symlinks and unsafe file-list paths, and verifies
that the original bytes remain unchanged. An actual hardlinked source tree is
refused before mutation, preserving both original and staged bytes; regular
file/hash equality alone is insufficient. Real dictionary/template side-store
copies are refused before mutation, with every copied byte unchanged. The existing
`db.TestRebindDurableRootSnapshotV1PreservesBothSlotsAndExactTargetIdentity`
retains the two-slot, interrupted-install, fallback and later-replacement proof.

The accepted retained caller requires `SnapshotFiles` (the pristine copied
file-path/SHA-256 JSON map), `SnapshotFilesSHA256`, and the root-selected input
pin `GOMAP_SELECTED_LIVE_FIXTURE_SHA256`. Input, descriptor, tools, queries and
truth are pinned before the shared opener verifies every copied file and
explicitly rebinds the staged snapshot. Shared input admission and installation
require every opened file to be regular with exactly one hardlink on the existing
namespace-supported platforms; other platforms fail closed. This input is flat-only;
`dictdb` and `templatedb` entries are refused before rebind or open. Public
root/`maindb` side-store layouts use their existing layout-aware opener elsewhere.
Ordinary Open remains strict; readers and replacement-build callers never
install a snapshot. The legacy retained caller keeps its existing behavior
unless installation is explicitly requested.

The enclosing driver proves the closed original and copy are byte-identical
before installation; rebind intentionally changes the staged index. Later
checks bind collection/source/definition/manifest semantics and the fresh
owner-separated generation, rather than demanding that the installed index
remain identical to the original. A reused build retains its original builder
head, executable, recipe and receipts, separately from the current caller
head. No prior failed caller is reclassified as passing by this installation.

The operation-owned endpoint checkpoint is mapped to
`TestImmutableOwnerReplacementPrivateEndpointV1`: preauthorized Nodes-only
address, fresh exact marked preparation, shared cold/cached domain source,
credentialed probe, NONVOTER strong-search NOT_LEADER/no hits, unchanged old-voter
endpoint, wrong-node/unmarked refusal, restart cold, hosted-asset refusal and
genuine current-DB swap retirement. It grants no public readiness or promotion.
Normal cold/cache diagnostics include all five Raft nodes, caller allocations,
background work and control polling; they do not measure per-node allocations
or establish a throughput comparison.

The historical qualification receipt checkpoint maps to
TestImmutableOwnerReplacementHistoricalQualificationReceiptV1 using the same
five-node authenticated native-snapshot/nonvoter/private-ANN fixture: genuine
private execution before the first receipt commit, two overlapping identical
commands returning the same receipt with one ANN cache hit and one catalog entry,
cancellation of an actual blocked waiter, nil/empty live-domain owned-byte retry,
conflicting query, external raw catalog-publish and generic advance refusal,
permanent nonvoter/unready behavior and cold restart requiring fresh explicit
preparation/qualification. TestCatalogOwnerQualificationReceiptIsHistoricalAndAtomicV1
covers dedicated-command idempotency, generic FSM injection refusal, known
receipt conflict/erasure atomicity, bounded cold restore, invalid ACTIVE/issuer
and phase-cap refusals, accessor ownership and one extra snapshot-entry budget,
including byte-atomic known restore refusal at unchanged AppliedIndex and
successful install with exactly one additional entry. The real fixture refuses
retained-result byte exhaustion before ANN/cache hits or commit and checks
release after successful submission.
TestReplacementOwnerQualificationReceiptRequestAdmissionV1 covers caller
capacity refusal before catalog work, later discovery refusal and cancellation
with no request/byte lease leak; it is semantic admission evidence, not ANN
or quorum evidence.
TestReplacementOwnerQualificationResultDigestIgnoresTelemetryV1 binds only
ordered partitions and neighbor IDs/scores; counters, timing and memory changes
leave the digest unchanged. TestReplacementOwnerQualificationReceiptGateCancellationV1
checks canceled admission cleanup and reuse of the bounded producer gate.
The fixture logs one enabled overlapping-qualification-plus-commit elapsed/ops-per-second
and aggregate MemStats sample across the caller, five Raft nodes and background
work. It is diagnostic, has no before/after throughput claim, and does not measure
isolated server allocations or retained ANN residency. Existing ordinary
healthy search/status implementation remains unchanged.

The ordinary immutable-owner recovery checkpoint maps to
`TestImmutableOwnerOrdinaryServingRecoversCurrentFSMDBV1`. The genuine pre-change
red is explicit lifecycle Warm refusing the original owner's stale startup DB
after native provider recovery. The candidate obtains a new ordinary M5 ANN
response with local ReadIndex after Warm, then performs another actual native
snapshot installation: cold health/status/ensure refuse, an old response paused
at its captured final guard returns no partials after a second successful Warm,
and the retired source cannot reopen. Missing hosted assets refuse Warm;
startup-only Stage remains stale. With the actual root barrier held, registered
construction is canceled or joined by concurrent Close without installing
serving state; the test does not assert entry into the barrier wait.
`TestImmutableOwnerWarmCatalogReadCloseV1` blocks a real mTLS catalog read on an
already warm owner and proves Close cancels and joins that initial authority
request, releases request/byte admission and leaves no serving state or tracker.
`TestImmutableVectorBackendAuthorityReadCloseV1` blocks the router's subsequent
ACTIVE read after the topology initializer's first fence;
`TestMutableVectorBackendLifecycleReadCloseV1` blocks a genuine authenticated
lifecycle request before initial activation. Both require authority I/O outside
`initMu`, cancellation and join by Close, no late backend installation, and
request/byte lease release. A waiter observed entering the occupied-slot select
can cancel without clearing the builder's ownership. The same initialization
slot covers topology and backend construction; mutable cached backend returns
retain their existing path. Already committed catalog work is not rolled back
by cancellation.
Existing private replacement quorum, operation and asset controls remain in the
shared fixture, with recovery disabled in all existing wrappers. This is
ordinary owner recovery only; replacement NONVOTER/add-intent remains unready.
Healthy-path cost comparison uses the unchanged voter/consumer
`TestFixedPeerImmutableOwnerReadCostV1` sampler on baseline and candidate,
including public strict search's captured-backend guard lock. Its fixed ten
samples report caller/global allocation and wall-time diagnostics; child CPU/RSS
cover the whole fixture and cannot establish server allocations, ANN residency
or stable throughput. No measurements are claimed before root execution.

The private ANN qualification checkpoint maps to
`TestImmutableOwnerReplacementPrivateANNQualificationV1`: genuine authenticated
leader-issued ReadIndex plus separate target FSM/Raft applied progress, cold/cache
parity with the original owner's unchanged leader-bound M5 shard route, one logical
two-pack domain graph traversal and zero exact fallback, ordinary NONVOTER
NOT_LEADER/no hits, exact BEGIN/wrong-node/unmarked refusal, warm-cache leader
quorum loss, restart, missing hosted assets and genuine current-DB swap refusal.
The legal ACTIVE invalidation producer remains blocked by pending preparation;
no artificial ACTIVE transition is used as evidence.
`TestReplacementPrivateANNFrameDiscriminatorsV1` checks distinct framing and
ordinary dispatcher refusal; the real fixture also checks public coordinator
proof refusal. `TestReplacementPrivateANNLateAdmissionClearsResponseV1` uses the
existing deterministic service fixture to prove final admission discards all
partials and releases its generation pin. `TestReplacementPrivateANNRequestAdmissionV1`
checks pre-dial byte-exhaustion refusal, request-lease release after pre-catalog authentication refusal,
and canceled admission through the existing shared transport fixture.
`TestReplacementPrivateANNServiceRejectsMutableShapeV1` rejects non-basic or
live/strict private requests before authority, proof or generation access.
`TestReplacementPrivateANNReceiveValidationV1` sends authenticated direct frames
without the exported client: shape and BEGIN-inclusive caller-budget refusals,
exact augmented-budget handoff and configured frame bounds leave no result or
transport lease. Its accepted boundary uses a refusal-only callback and claims
no operation authority or ANN success.
`TestReplacementPrivateANNResponseValidationV1` rejects missing or malformed
partitions, exact fallback, incoherent counters/chunks, invalid neighbors,
wrong proof identity and exceeded byte/work budgets through the shared coordinator
payload validator. Its authenticated controlled-catalog cases accept a previous
replacement issuer from the committed roster and reject removed startup peers,
stale BEGIN, wrong phase, missing seed, missing authority and unanchored peers. These are transport
and semantic controls, not fabricated owner promotion or additional quorum
evidence. The admission test also rejects a private BEGIN frame that exceeds
the caller's ordinary-body byte limit before authority lookup or dial.
Coherent empty partitions remain legal; private requests
require immutable identities and basic statistics. These checks grant no public
readiness or promotion. Per-request time/MemStats diagnostics include the caller, all five
Raft nodes, TLS, authority checks, ANN and background work; they are individual
observations, not server allocations, isolated ANN cost or a throughput comparison.

## Bounded split-source insert checkpoint

The semantic envelope and raw-route refusal are covered by
TestSplitVectorInsertSemanticEnvelopeV1 and
TestFixedPeerEntryRouteRejectsRawSplitCommandV1. The two authenticated
split-source tests cover canonical source storage, target projection without
canonical bytes, exact duplicates, the 64-outcome lifetime ceiling, strict
visibility fences, wrong producer identity, automatic pending replay after
reopen, idle retry without network work, and shutdown while foreground work
owns the mutation lock. Coupled catalog/target quorum loss refuses cached
receipts and watermark hits; the source fixture has one voter.
TestFixedPeerCloseRetiresUnusedAuthenticatedSelfDialV1 deterministically races
an authenticated speculative self-dial with a returned HTTP connection, then
checks graceful Close and complete resource release. TestFixedPeerAuthenticatedControlSocketCleanupOrderV1
characterizes the caller-first fixture ownership requirement. Existing
TestPeerSecurityDrainNativeVectorSelfFanoutV1 and
TestPeerSecurityShutdownTimeoutAndForcedCloseV1 retain admitted-work and bounded
shutdown-error coverage.
TestVectorPartitionSplitSourceInsertLegacyRefusedBeforeMutationV1 and
TestSplitInsertVisibilityWireOwnedAndBoundedV1 cover unsupported legacy
routing and bounded owned wire extensions.

TestSplitVectorInsertApplyRecoveryV1 covers source/project/clear after local
WAL append and after publication. TestSplitVectorInsertTargetStoredResultRecoveryV1
covers result-before-progress replay without another frame.
TestSplitVectorInsertSourcePublicationProcessExitV1 bypasses Close at the
existing accepted-source publication hook. TestSplitVectorInsertTerminalTailRecoveryV1
preserves a complete source intent before an incomplete next frame and refuses
a corrupt applied durable prefix. These checks do not establish power-loss
safety, independent quorum-loss qualification, or indefinite writes. Target
and clear crash cuts remain apply-boundary/reopen controls.

`TestDependencyStableRequiresExactPrefixAndNamespace` distinguishes unrelated
volatile suffixes from unstable/corrupt/short required prefixes, absent names,
changed physical identities and missing/rebound parent namespaces. It refuses
unsupported LSN/RID manifest frontiers. The actual-cut enumerator derives each
sealed generation's closure from its checksummed V1 manifest, while preserving
newest-complete-root, command-frame replay, ACK and RO/RW key-state checks.


### Fixed-peer vector initialization prerequisite (#4250)

`TestFixedPeerVectorInitializationJSONIdentityV1` is the retained semantic red
contract: on ef22 it compiles and fails because JSON discards the intent.
`TestFixedPeerVectorInitializationCloneDigestAndValidationV1` checks canonical
identity, caller-owned map isolation, malformed/coexisting mode refusal, and
canonical membership. `TestFixedPeerVectorInitializationRootIdentityV1` checks
paired-root binding, unchanged reopen, mutation/removal/retrofit refusal, and
unmarked nonempty root refusal. `TestFixedPeerVectorInitializationSixNodeLayoutV1`
checks refusal of unsupported two-group initialization while retaining ordinary
six-node config validation. `TestFixedPeerVectorInitializationRefusesReplicaReplacementV1`
checks the shared BEGIN and authority guards, including direct preparation,
removal, completion, and reconciliation before replacement publication.
`TestFixedPeerVectorInitializationRealRaftCreateIngestReopenV1` starts a tiny
fresh authenticated RF3 cluster, commits a real catalog and indexed collection
create plus a fresh document insert, observes the document and actual applied
progress on all three replicas, and reopens matching roots. It also checks
initializing status, non-readiness, bound reserved listeners that refuse traffic, and refused
vector operations before and after reopen. This is not cluster qualification
or proof of serving activation. Existing nil-intent runtime/security/readiness
regressions and `BenchmarkSparseCatalogConfigV1/Nodes4Groups2` cover the ordinary
configuration path; startup overhead should be compared at the exact base/head.


The Prepare convergence/resume witness is
`TestFixedPeerVectorInitializationRealRaftPrepareSourceConvergenceV1`.
It cancels after a genuine committed-applied source acknowledgment, observes
that actual prefix on all voters with no preparation completion, then resumes
the identical request. It retains the transient source-mismatch reread,
partial completion resume, changed-source refusal before append, completed
command/asset/source disagreement refusals, cancellation, and actual durable
completion controls. Private reply interception controls observations; it
does not establish scheduler-induced follower lag.
`TestSingleGroupSubmitterStalePrepareRequiresKnownReplayV1` separately checks
the shared stale-guard gate with exact source/prepare entries, unknown keys,
changed source/operation, malformed entries, missing idempotency authority,
and ordinary stale mutations. Its preflight is a deterministic stand-in;
the native fixture supplies the real FSM/Raft witness. These tests do not
establish performance, RF4 qualification, or broader readiness.

### Bounded RF4 initialization (#4944)

`TestFixedPeerVectorInitializationRF4LayoutV1` first validates the ordinary
four-voter authenticated single-group configuration, then checks initialization
admission and identical normalized identity from all four node configs. On the
original RF3-only source it fails at initialization admission after the ordinary
control passes. `TestFixedPeerVectorInitializationRF4RosterBoundsV1` keeps valid
ordinary one/two/five/six-node controls while refusing those initialization
sizes, and refuses incomplete RF4 catalog/data/address rosters or missing
authentication. Existing RF3 identity, scope, source-bound, endpoint, two-group
six-node and replacement refusal tests remain required.

`TestFixedPeerVectorInitializationRF4RealRaftPrepareServingSnapshotTailV1`
reuses the RF3 real fixture with four actual catalog/data voters: production
create/ingest, all-voter source rebuild/prepare, restart and strict search,
fresh insert/exact retry, provider snapshot-plus-tail reopen and retained
current-DB guard. Its terminal lower-FSM replacement remains quiescent after
all-voter prefix observation and provider shutdown. Unsupported publication
platforms retain pre-Append refusal assertions; they do not prove RF4 serving.
`TestFixedPeerVectorFixtureRF4RealRaftV1` runs the public initialize/qualify
client across four real peers, requiring readiness from all four after reopen.

The offline command `python3 scripts/treedb_fixed_cluster_2host_test.py` checks
RF3 2+1 and RF4 2+2 orchestration, immutable per-host images, exact roster checks,
container caps, graceful exit checks and fail-stop behavior. It launches no
SSH/Docker operation and is not two-host runtime evidence. Actual RF4 evidence
requires four resident SERVER containers, two on each host, pinned source,
binary/image/config identities, fresh roots, cross-host private shards, full
initialize/reopen/search/insert/retry/readiness receipts and owned teardown.

Use existing `BenchmarkSparseCatalogConfigV1/Nodes4Groups2` for a matched
baseline/candidate ordinary config/client guardrail. The additional
`BenchmarkFixedPeerVectorInitializationClientV1/RF3` and `/RF4` measure only
public config normalization, credential loading and NewClient/Close; certificate
and socket/root fixture setup are untimed. Compare RF3 on the same base/head
harness, then candidate RF3 versus RF4 for incremental B/op and allocs/op.
Neither benchmark measures election, preparation or four-daemon RSS. Retain
actual container footprint separately. These are proposed checks until executed
on the final integrated source; no 100K, sustained mutation, throughput or
physical-host failure acceptance follows. That original packet qualified only
three prepared rows and a bounded ordinary insert population; #4956 extends
preparation admission and the optional public probe as documented below. RF4
quorum3 cannot survive either host loss in 2+2.


## Dataset checkpoint (#4956)

The existing initializer/qualifier optionally consumes a frozen dataset; empty
`-dataset` preserves the three-row fixture. The shared preparation envelope is
16,384 rows / 32 MiB actual FP32+IDs / 1,024-byte individual IDs, enforced before
owned source materialization. The initial packet counts 10,000 unchanged128D
rows plus three separated oracle anchors. See
[the preparation contract](fixed-cluster-vector-prepare-v1.md#dataset-preparation-admission-4956)
for exact input eligibility, stream/chunk identities, UNKNOWN/fail-stop behavior,
restart proofs and allocation evidence gates. The real Raft dataset tests cover
unchanged RF4 reopen, lost committed second-chunk reply and eligible same-count
corpus replacement refusal before public search/write. Manifest origin boundary
coverage admits16,384, refuses16,385 and preserves the other origin constraints.
These are proposed checks until root executes the exact final candidate; the
failed staged packet did not establish a corpus-identity causal red.
The optional65-new-ID public
probe plans reconciliation of historical64 ordinary-write qualification claims.
Its retained trial08 corpus run failed after10 attempts; actual unpaced >64
completion remains pending #4958. It changes no split identity capacity and
establishes no sustained throughput or broad mutation/recall guarantee.
#4250/#4810 remain open.


## Ordinary owner admission progress (#4958)

`TestFixedPeerVectorWaitingSearchPrecedesNextWriterRealRaftV1` retains a real
prepared owner, observes a contended reader through a test-only context's Done
boundary, then submits the next actual public ordinary insert. On the unchanged
source it failed because that writer acknowledged commit8 before the waiting
reader acquired admission. The fix registers intent only after read contention
and retires it on admission or cancellation; later writers wait before their
existing RWMutex exclusive queue. The existing
`TestFixedPeerVectorOwnerSearchAdmissionRealRaftV1` retains genuine native shard
plans, concurrent reader pins and committed/applied publication exclusion.
Existing ordinary-owner recovery tests retain cold-initialization safety.

Focused admission checks also cover canceled reader intent cleanup, canceled
intent-waiting writer progress, cancellation after a queued writer acquires the
mutex, and a later reader not preempting an already queued writer. Uncontended
search and mutation admission must allocate zero times. The identical overlay
`fixed_peer_vector_admission_bench_v1_test.go` compares old/candidate admission
cost using `BenchmarkFixedPeerVectorAdmissionUncontendedV1/{search,mutation}`;
setup is excluded and its results do not measure Raft or whole public latency.
Root executes the exact formatted candidate in normal/race modes before these
checks can be treated as passing evidence.

Driver deadline tests admit explicit600s total/60s RPC limits without changing
logical request bytes, stable hashes or tiny default population, and reject
invalid bounds before input/network work. The larger limits do not themselves
prove admission fairness. Retained trial08's unpaced198-operation plan failed
with8 successes/1 failed search/1 UNKNOWN mutation/188 unissued, leaving actual
>64 ordinary growth pending a reviewed frozen fresh packet at that point.
Fresh trial10 subsequently passed all 198 attempts on the unchanged
10,000-row/128D corpus: 65 distinct ordinary inserts, one identical retry and
132 native searches, with verified client-call overlap and all four voters
applied through commit 154. No failed, UNKNOWN or unissued operations occurred;
all four servers stopped exit 0 without OOM and their stores remain preserved.
The independently reviewed sealed archive is
`91333f496872113c8b1942c173a43218810eaf0322921677e6cd53086814aa17`.
Its daemon retains source `47aa6ab` / ELF `1a9c7061` and its phase-renewal driver retains
source `f231933` / ELF `940752c0`; later ancestry-only integration does not relabel them.
This qualifies bounded ordinary growth and native client-call overlap, not
sustained throughput, representative recall, broad mutation support, whole-host
failure tolerance or unobserved whole-lifetime resource peaks. Write-phase cost
attribution and broader resource qualification remain open with #4959/#4250.


### Changing-top10 mixed harness (#4997)

`TestMixedChangingTop10ConservativeCompatibleRecallV2` is a retained semantic
regression: a whole response valid against two populations has recall1 at the
baseline and recall0.9 after a tail replacement; validation must report0.9.
The unchanged exported admission still refuses the changed corpus. Profile
planning independently checks all seven full-population canonical oracles,
immutable exports, deterministic six-original hashes, actual membership changes,
future/stale/mixed postimage refusals and strict fallback rejection.
`TestMixedChangingTop10PostJoinRecallV2` tightens the online prefix range using
actual invocation/ACK intervals, checks compatible masks, reuses retained recall
scalars and verifies recomputed aggregate mean/query counts, including cancellation.
Existing UNKNOWN, retry identity, token visibility, retention and invariant-profile
controls remain required. These are authored controls until root normal/race
execution is attached; no runtime recall or full-source population result follows
from source inspection. See command README for validation/retention benchmarks,
allocation boundaries and the optional external population attachment distinction.


### Bounded sustained mixed harness and complete outcome audit (#5021)

`TestMixedSustainedAdmissionV1` rejects the superseded58/300s/5s/3s proposal
and admits the revised48/300s/6s/3s declaration and feasible maximum63 count.
`TestMixedCumulativeRPCBudgetV1` checks every serial write-plus-visibility
budget, strict equality, both spacing-dominant and RPC-dominant schedules, and
refusal before input/network access. Six/60s with3s RPCs remains feasible;
omitted or explicit six with the shared10s RPC default correctly refuses.
The ordinary60s read-window ceiling remains a separate control.
`TestMixedSustainedMaximumPlanV1` independently reconstructs all64 native
canonical prefixes, checks distinct original keys and alternating changed
replacements of surviving A, and preserves the original six operations.
`TestMixedSustainedPrefixBit63V1` checks bit63, all64 compatibility bits and
range/overflow/duplicate/missing-oracle refusals.
`TestMixedSustainedCLIAndBoundsV1` refuses explicit zero/non-mixed options and
identical extended vector directions, and verifies the exact uint64 encoding
fits the unchanged per-prefix retention allowance. Existing conservative
recall, exact postjoin recomputation, canceled calls, retry identity, visibility
and final-slot controls remain required. The UNKNOWN control exercises six and
58 originals with one and four independent readers and retains every unissued
original after the first ambiguous result.

`TestFixedPeerColocatedAuditVariableLengthCurrentAuthorityV2` uses the existing
real four-voter fixture with63 committed originals and complete all-voter
witness/population proofs. A first-witness omission keeps the final source and
floor unchanged so rejection isolates the exact retained-count requirement.
The six-outcome current-authority control remains separate.
`TestColocatedAuditSustainedBoundsV1` checks matching pre-marshal count/byte
bounds, repeated original refusal and cancellation using explicitly synthetic
local admission inputs, which are not runtime authority. The attachment registry
is unchanged: no opcode, binary section or WAL-format extension is introduced.

Compare identical base/candidate `BenchmarkMixedPrefixValidationGuardV1`,
`BenchmarkMixedRetainedCallV1`, `BenchmarkWindowRetainedCallGuardV1` and
`BenchmarkPopulationLegacyColocatedPlanGuardV1/{validate,decode}` with B/op and
allocs/op. Candidate `BenchmarkMixedSustained58V1/{plan,validate-all-compatible}`
measures bounded native-oracle setup and worst compatible-prefix validation on
the documented small128D fixture; it is not a10K serving benchmark.
`BenchmarkColocatedAuditSustained58V1/{validate,decode}` times complete synthetic
58-outcome encoded ledger/token validation, excluding authenticated transport,
current-FSM witness lookup and source scans. Exact tooling, fixture/source
identity, commands, isolation and raw results must accompany measured claims.
These58-outcome synthetic costs remain conservative source guardrails, not
relabelled48-write runtime evidence. Actual48-outcome audit/resource costs and
all49 native truths remain fresh qualification obligations.
See command README for allocation ownership and planned operational bounds;
source controls alone establish no sustained service result.
