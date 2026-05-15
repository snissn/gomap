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

| Normative statement | Owner section | Required test/evidence | Status |
|---|---|---|---|
| WAL-supported command visibility implies process-crash recoverability. | `user-command-wal.md` normal write path | `TestCommandWALInsertAckPublishesRootsAndAppliedLSN`, `TestCommandWALDeleteAckPublishesRootsAndAppliedLSN`, `TestCommandWALReplaceAckPublishesRootsAndAppliedLSN` | planned |
| No V1 WAL-backed pending overlay is visible. | `collections-write-domain.md` durability boundary | `TestCommandWALFrameDurableButUnpublishedNotVisible`, `TestCommandWALNoPendingOverlayBeforeRootPublish` | planned |
| Pre-frame failures are ordinary not-committed failures and leave no visible state. | `user-command-wal.md` normal write path | `TestCommandWALPreFrameValidationFailureLeavesNoMutation`, `TestCommandWALExternalRefPrepareFailureRejectsBeforeVisibility` | planned |
| Post-recoverable-frame failures are commit-ambiguous or recovery-required, not retryable not-committed errors. | `user-command-wal.md` replay idempotency, `native-wire-protocol.md` ack policy | `TestCommandWALPostFramePublishFailureCommitAmbiguous`, `TestNativeWireCommandWALPublishFailureCommitAmbiguous` | planned |
| `AppliedCommandLSN` is selected atomically with roots. | `user-command-wal.md` checkpoint and cleanup, `storage-format.md` command WAL target | `TestCommandWALRootsAndAppliedCommandLSNPublishAtomically`, model split-state rejection proof | planned |
| `AppliedLSN` advances only over a contiguous command LSN prefix. | `user-command-wal.md` publish boundary | `TestCommandWALAppliedLSNContiguousPrefixOnly`, `TestCommandWALOutOfOrderPublishRejected` | planned |
| Complete frames with missing required external refs fail closed. | `user-command-wal.md` recovery | `TestCommandWALMissingExternalRefFailsRecovery`, `TestCommandWALCorruptExternalRefFailsRecovery` | planned |
| Terminal incomplete tails are ignored only when no complete commit marker exists. | `recovery.md` decoder outcomes | `TestCommandWALTerminalShortHeaderIgnored`, `TestCommandWALTruncatedCompleteFrameFailsClosed` | planned |
| Unknown required versions, command kinds, and critical flags fail closed. | `storage-format.md` command WAL target | `TestCommandWALUnknownRequiredVersionFailsClosed`, `TestCommandWALUnknownRequiredKindFailsClosed`, `TestCommandWALUnknownCriticalFlagFailsClosed` | planned |
| Old raw `commitlog.Record` payloads are unsupported after command WAL activation. | `storage-format.md` command WAL target | `TestCommandWALFeatureGateRejectsLegacyRawPayload` | planned |
| Batch commands are one command frame, one LSN, and all-or-nothing. | `user-command-wal.md` batch atomicity | `TestCommandWALRawKVBatchOneLSNAtomic`, `TestCommandWALCollectionInsertBatchOneLSNAtomic`, `TestCommandWALOversizedBatchRejectsBeforeLSN` | planned |
| Callback update APIs never replay Go callback code. | `user-command-wal.md` update API categories | `TestCommandWALCallbackUpdateLogsFinalReplacement`, `TestCommandWALRecoveryDoesNotInvokeCallback` | planned |
| Resolver helpers are resolved before WAL append. | `user-command-wal.md` update API categories | `TestCommandWALSetNowStoresResolvedLiteral`, `TestCommandWALRecoveryDoesNotInvokeResolver` | planned |
| Catalog/schema barriers cannot race lower unapplied commands. | `collections-write-domain.md` barrier semantics | `TestCommandWALCatalogMutationDrainsLowerLSNs`, `TestCommandWALCreateIndexRejectsUntilCatalogCommandSupported` | planned |
| Read-only open fails when mutating command replay would be required. | `recovery.md`, `contracts.md` read-only open | `TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired` | planned |
| Backup/restore either includes needed WAL/external refs or has durable cleanup proof. | `backup-restore.md` restore validation | `TestCommandWALBackupManifestRestoresUnappliedCommands`, `TestCommandWALRestoreMissingRequiredFrameFailsWithoutCleanupProof` | planned |
| Native-wire deterministic command schemas align with local command WAL payload schemas. | `native-wire-protocol.md`, `user-command-wal.md` Raft/native-wire relationship | `TestCommandWALPayloadMatchesNativeWireDeterministicFixture`, `TestNativeWireAndLocalCommandDigestStable` | planned |
| Raft/local recoverability is not reported before local command WAL publish and `AppliedLSN`. | `native-query-raft-roadmap.md` local apply layering | `TestRaftApplyDoesNotReportRecoverableBeforeCommandWALAppliedLSN` | future |

### 11.5.2 Milestone Test Slices

PR 1: typed commit-log frames and feature gate:

- `TestCommandWALFormatGoldenV1EmptySegment`;
- `TestCommandWALFormatGoldenV1RawKVBatch`;
- `TestCommandWALFormatGoldenV1CollectionInsertBatchByID`;
- `TestCommandWALFormatGoldenV1CatalogMutationPlaceholder`;
- `TestCommandWALFormatRejectsUnsupportedRequiredVersion`;
- `TestCommandWALFormatRejectsUnknownCriticalFlag`;
- `TestCommandWALFormatSkipsUnknownNonCriticalExtensionOnlyWhenAllowed`;
- `TestCommandWALFormatRejectsMalformedLengthBeforeAllocation`;
- `TestCommandWALFormatRejectsHeaderPayloadDigestAndTrailerMismatch`;
- `TestCommandWALFeatureGateRejectsLegacyRawPayload`;
- `TestCommandWALFeatureGateRequiresCleanLegacyWALBeforeActivation`;
- `TestCommandWALNoCollectionSegmentFamilyCreated`;
- `TestCommandWALSingleJournalOwnerRejectsSecondMutableWriter`.

PR 2: shared journal ownership and `AppliedCommandLSN` plumbing:

- `TestCommandWALAppliedCommandLSNMetaFieldRoundTrip`;
- `TestCommandWALMetaGateRejectsOldBinaryServingCommandWALDir`;
- `TestCommandWALRootsAndAppliedCommandLSNPublishAtomically`;
- `TestCommandWALPublishHelperRejectsRootsWithoutAppliedLSN`;
- `TestCommandWALAppliedLSNContiguousPrefixOnly`;
- `TestCommandWALCheckpointCleansOnlyCoveredSegments`;
- `TestCommandWALCheckpointCrashBeforeCleanupReplaysIdempotently`;
- `TestCommandWALCleanupManifestMissingBlocksSegmentDeletion`;
- `TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired`.

PR 3: recovery dispatcher and raw KV command conversion:

- `TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor`;
- `TestCommandWALRIDFencePreservedForRawKVBatch`;
- `TestCommandWALCrashAfterFrameBeforeRootPublishRecovers`;
- `TestCommandWALCrashDuringRootPublishSelectsOldTupleOrNewTuple`;
- `TestCommandWALCrashAfterRootAppliedLSNBeforeCleanupSkipsFrame`;
- `TestCommandWALRecoveryCrashDuringReplayResumesFromAppliedLSN`;
- `TestCommandWALStrictCommandEffectWithoutAppliedLSNFailsClosed`;
- `TestCommandWALIdempotentSkipRequiresDigestProof`.

PR 4: collection insert/delete by explicit ID:

- `TestCommandWALCollectionInsertAckBeforeCheckpointRecovers`;
- `TestCommandWALCollectionInsertBatchOneLSNAtomic`;
- `TestCommandWALCollectionDeleteAckBeforeCheckpointRecovers`;
- `TestCommandWALCollectionDeleteBatchOneLSNAtomic`;
- `TestCommandWALInsertUniqueConflictBeforeFrameLeavesNoPartialItems`;
- `TestCommandWALInsertDuplicateReplayUsesConfiguredIdempotencyRule`;
- `TestCommandWALUnsupportedIndexedInsertModeFailsBeforeStagingUntilSupported`;
- `TestCommandWALWALOffDoesNotCreateCommandFrameForFlushBoundaryWrites`.

PR 5: collection update by explicit ID:

- `TestCommandWALCallbackUpdateLogsFinalReplacement`;
- `TestCommandWALRecoveryDoesNotInvokeCallback`;
- `TestCommandWALDeclarativeSetStoresCanonicalOps`;
- `TestCommandWALSetNowStoresResolvedLiteral`;
- `TestCommandWALRecoveryDoesNotInvokeResolver`;
- `TestCommandWALUpdateChangedSecondaryRecoversAtomically`;
- `TestCommandWALUpdateUnchangedSecondaryPreservesIndexState`;
- `TestCommandWALUpdateBatchOneLSNAtomic`;
- `TestCommandWALUnsupportedUpdateOperatorFailsBeforeFrame`;
- `TestCommandWALQueryWideUpdateRejectedInWALOnMode`.

PR 6: catalog mutation commands:

- `TestCommandWALCreateCollectionCommandReopens`;
- `TestCommandWALCreateCollectionIdempotentRetrySameMetadata`;
- `TestCommandWALCreateCollectionIncompatibleRetryFailsNoMutation`;
- `TestCommandWALCreateIndexCommandDrainsLowerLSNs`;
- `TestCommandWALDropIndexCommandDoesNotResurrectSameNameOldUID`;
- `TestCommandWALCatalogEpochGuardRejectsStaleReplay`;
- `TestCommandWALSchemaEpochGuardRejectsStaleReplay`.

PR 7: matrix enforcement and drift tests:

- `TestCommandWALSupportMatrixCoversMutatingCollectionRegistry`;
- `TestCommandWALSupportMatrixCoversMongoGatewayMutations`;
- `TestCommandWALSupportMatrixCoversNativeWireMutations`;
- `TestCommandWALDocsRejectActiveCollectionWALReferencesOutsideDeprecatedDoc`;
- `TestCommandWALDocsRequireAppliedCommandLSNAsV1Target`;
- `TestCommandWALDocsRequireBatchAtomicityText`;
- `TestCommandWALUnsupportedCommandReturnsStablePublicError`.

PR 8: native-wire/Raft alignment closeout:

- `TestCommandWALPayloadMatchesNativeWireDeterministicFixture`;
- `TestNativeWireAndLocalCommandDigestStable`;
- `TestNativeWireAckVisibleRequiresRootPublishAndAppliedLSN`;
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
| after complete frame before WAL sync boundary | relaxed modes follow their advertised boundary; durable mode must not acknowledge |
| after complete recoverable frame before command apply | read-write recovery replays; read-only open fails recovery-required |
| during command apply before root publish | copy-on-write partial pages are unreachable; recovery replays |
| after root publish attempt before meta selection | recovery selects old tuple or fails closed; no split state is served |
| after roots plus `AppliedCommandLSN` selected before response | command is committed; API returns commit-ambiguous if response cannot be built |
| after roots plus `AppliedCommandLSN` before WAL cleanup | recovery skips covered frames and cleanup resumes idempotently |
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
- `TestCommandWALBadPayloadDigestCompleteRecordFailsOpen`;
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
- malformed length, frame CRC, payload digest, and commit marker corruption.

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
