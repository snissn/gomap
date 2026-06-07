# TreeDB Verification Matrix

This document maps specification invariants to existing tests and harnesses.

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

## 3. Value-Log Reachability GC

Invariant:
- Fully unreferenced segments are removable; referenced segments are preserved.

Coverage:
- `TreeDB/db/vlog_gc_test.go`:
  - `TestValueLogGC_RemovesUnreferencedSegment`

## 4. Value-Log Rewrite Correctness

Invariant:
- Offline rewrite preserves values while reducing/replacing old segments.

Coverage:
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

## 7. Maintenance/Compaction Behavior

Invariant:
- Index rewrite/vacuum paths preserve data and handle pinned snapshots safely.
- Full storage compaction preserves value visibility, removes reachable debt only
  through the documented lifecycle, serializes backend maintenance phases, and
  keeps cached-mode value-log writers from reusing backend-created segments.

Coverage:
- `TreeDB/db/compact_index_test.go`
- `TreeDB/db/compact_index_sequential_alloc_test.go`
- `TreeDB/db/vacuum_online_swap_test.go`
- `TreeDB/db/compact_storage_test.go`
  - `TestCompactStorageHoldsMaintenanceLockAcrossPhases`
- `TreeDB/compact_storage_test.go`
  - `TestCompactStorageFullPacksLeafGenerationDebtOffline`
  - `TestCompactStorageCachedDeletesZeroByteValueLogFiles`
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
- `TreeDB/caching/iterator_cached_writes_test.go`:
  - `TestIterator_IncludesCachedWrites_SnapshotIsolated`
  - `TestIterator_IncludesCachedWrites_ValuePointers`
- `TreeDB/caching/reverse_iterator_cached_writes_test.go`:
  - `TestReverseIterator_IncludesCachedWrites_SnapshotIsolated`
- `kvstore/adapters/treedb/read_snapshot_cached_writes_test.go`
- Unified-bench correctness guardrail: `cmd/unified_bench/read_snapshot_guardrail_test.go` and `BenchConfig.ReadRequireHit`

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
  - `TestSearchTextFieldWeightAffectsRankingM4`
  - `TestSearchTextMissingIndexUnsupportedSyntaxAndTruncationM4`
  - `TestSearchTextTopKBoundsDocumentFetchM4`
  - `TestSearchTextReopenParityM4`
  - `BenchmarkSearchTextM4`

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
  then persists `command_wal_v1` after replay preconditions are clear and
  before opening the command journal, so a process cannot acknowledge typed
  frames without a durable required-feature gate.
- Raw KV `SetRID` command entries preserve the existing value-log RID fence by
  requiring the referenced RID to be present in scanned value-log segments
  before recovery can publish the command.
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
- Command WAL with WAL-off durability fails closed, including after
  `command_wal_v1` is persisted, because PR3 requires a recoverable command
  frame before root visibility.
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
  legacy raw records in `command_wal_v1` directories.
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
  strict `>1.01x` command-WAL/WAL-off throughput.

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

The PR9 public cutover performance gate is strict parity-plus. Required point
`Set`, focused `Batch.Write`, `unified_bench` batch-write, and incompressible
value-log auto/off lanes must each report candidate throughput strictly greater
than `1.01x` of the relevant baseline. Any required lane at or below `1.01x` is
a failing gate, including sub-parity results such as `0.80x`; those results may
be recorded only as failing evidence, not accepted evidence.

The same strict parity-plus rule applies to every command-WAL acceptance artifact
with a required performance gate: a passing status must have `>` throughput-gate
semantics, explicit `1.01x` minimum ratio thresholds, and recorded comparative
throughput ratios above that bar. Historical or diagnostic results below that
bar must be labeled as failing evidence.

This test must prove public `treedb.Open` can open a read-write
`command_wal_v1` handle, route raw KV writes through typed `RawKVBatch` command
frames, expose mode proof through stats, reopen without explicit backend-only
APIs, and recover final set/delete state. Mode proof must include cheap live
accepted/covered command-frame counters so benchmark artifacts do not require
diagnostic WAL segment scans. It is intentionally narrower than the future
cached typed-frame path: while this gate is active,
`treedb.write_path.mode=command_wal_cached` is the expected proof that public
command-WAL writes did not use the cached legacy redo journal.

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
- `quantized_only` returns estimated scalar_u8, pure-Go `rabitq_1bit`, or
  prototype `brq_1bit` scores from the selected named score plane and must not
  read exact vectors or norms during scoring.
- `quantized_rerank` uses the selected quantized traversal over the normalized
  `ef_search` candidate pool, trims to `QuantizedRerankCandidates`, exact-reranks
  only that shortlist by graph ordinal, and returns exact cosine scores.
- Missing, stale, mismatched, unsupported, or unprepared quantized assets fail
  closed with no hidden exact fallback.

Coverage:
- Policy owner: `TreeDB/docs/spec/quantized-vector-index.md`.
- Runtime tests:
  - `TreeDB/collections/column_vector_graph_quantized_asset_test.go` covers
    scalar_u8 asset build/prepare/reopen, quantized_only score semantics,
    quantized_rerank exact shortlist ranking, normalized `ef_search` traversal
    before trim, multiple quantized indexes, concurrency, and fail-closed asset
    validation.
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
  - `TreeDB/internal/quantizedasset/quantized_asset_test.go` covers prepared
    ordinal readers, role/schema validation, footprint metrics, and scorer-shaped
    allocation benchmarks.
- Benchmarks:
  - `BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926` reports exact vs
    `quantized_only` vs `quantized_rerank` `ns/op`, `ops/sec`, `B/op`,
    `allocs/op`, recall@K, candidate/rerank counts, code bytes, exact vector/norm
    bytes, fallback counters, and asset bytes/vector on one fixture.
  - `BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926` reports rebuild
    cost and storage/asset bytes for exact assets versus scalar_u8 assets.
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
