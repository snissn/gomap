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

## 11.5 Planned Collection WAL Durability Gate

This section owns the canonical names of collection WAL acceptance tests. Design
documents may list fault classes and invariants, but named tests and acceptance
evidence are maintained here.

Normative coverage matrix:

| Normative statement | Owner section | Required test/evidence | Status |
|---|---|---|---|
| WAL-on visibility implies process-crash recoverability for enabled collection WAL capabilities. | `collection-wal-durability-plan.md` target durability contract | `TestCollectionWALNoIndexInsertAckBeforeFlushRecovers`, `TestCollectionWALNoIndexInsertBatchAckBeforeFlushRecovers`, full-contract indexed recovery tests | planned |
| WAL append failure before commit marker is not visible. | `collection-wal-durability-plan.md` failure contract | `TestCollectionWALAppendFailureRejectsBeforeVisibility` | planned |
| Post-commit failure returns commit-ambiguous/fatal, not an ordinary retryable mutation error. | `collection-wal-durability-plan.md` failure contract | `TestCollectionWALPostCommitVisibleInstallFailureCommitAmbiguous`, native-wire post-commit ack-failure tests | planned |
| Missing required side ref for a complete WAL transaction is a recovery error. | `collection-wal-durability-plan.md` side-ref recovery | `TestCollectionWALPartialFrameAndMissingSideRefNoPhantomRoots`, `TestRecoveryMissingRequiredSideRefFailsHard` | planned |
| Applied watermark advances only across contiguous applied collection sequence. | `collection-wal-durability-plan.md` watermark contract | `TestCollectionWALWatermarkOutOfOrderTxnDoesNotSkipLowerUnapplied` | planned |
| Root descriptors and applied watermark publish atomically. | `collection-wal-durability-plan.md` recovery states | `TestCollectionWALDescriptorAndWatermarkPublishAtomically`, `TestCollectionWALModelDescriptorWatermarkSplitRejected` | planned |
| Drop/recreate with the same collection name does not replay old transactions. | `collection-wal-durability-plan.md` identity guards | `TestCollectionWALCollectionUIDDropRecreateDoesNotReplayByName` | planned |
| Root, index, and catalog replay identity use stable UIDs/generations/digests, not names. | `collection-wal-durability-plan.md` identity guards, `storage-format.md` descriptor records | `TestCollectionWALCatalogDescriptorPersistsUIDGenerationEpochDigests`, `TestCollectionWALRootRefRejectsNameOnlyIdentity`, `TestCollectionWALIndexSameNameRecreateUsesNewUID` | planned |
| Direct publish, disabled-memtable, and large-batch paths cannot bypass WAL-on guards. | `collection-wal-durability-plan.md` write-path coverage | `TestCollectionWALDirectPublishAndDisabledMemtablePathsCannotBypassWAL` | planned |
| Read views pin pending value-log, leaf-log, and future column side refs until released. | `collection-wal-durability-plan.md`, `value-log-lifecycle.md` | `TestCollectionWALReadViewPinsLeafAndColumnSideRefs` | planned |
| Native-wire replicated mutations carry `CatalogGuardV1` with resolved stable identities. | `native-wire-protocol.md` catalog guard section | `TestNativeWireCatalogGuardV1CanonicalizesStableIDs`, `TestNativeWireNameDropRecreateRaceDeterministicGuardFailure` | future |
| Raft/local recoverability is not reported before local collection WAL durability. | `collection-wal-durability-plan.md`, `native-query-raft-roadmap.md` | `TestRaftApplyDoesNotAdvanceLocalRecoverableBeforeCollectionWAL` | future |

Invariant:
- Current indexed collection writes remain flush-boundary durable until the
  full indexed collection WAL implementation lands.
- Under the PR1-min guarded `NoIndexRowInsertOnly` capability, WAL-on
  no-index row insert visibility implies crash recoverability from either
  backend roots or a committed collection WAL transaction.
- Under the full collection WAL contract, the same visibility-implies-
  recoverability rule extends to indexed writes, update/delete, schema/index
  barriers, async publishing, and future column roots.
- Under `DurabilityWALOffRelaxed`, acknowledged writes before flush are not
  promised after process crash, and collection WAL files must not be created for
  unflushed writes.
- Collection WAL recovery must publish root groups and applied watermarks
  atomically, validate declared and embedded side refs, and clean only after a
  safe watermark plus checkpoint boundary.

PR1-min required coverage:

- `TestCollectionWALFormatGoldenV1EmptySegment`
- `TestCollectionWALFormatGoldenV1NoIndexInlineRootDelta`
- `TestCollectionWALFormatGoldenV1DescriptorOpAndWatermark`
- `TestCollectionWALFormatRejectsUnsupportedRequiredVersion`
- `TestCollectionWALFormatRejectsUnknownCriticalSection`
- `TestCollectionWALFormatSkipsUnknownNonCriticalSectionOnlyWhenAllowed`
- `TestCollectionWALFormatRejectsMalformedLengthBeforeAllocation`
- `TestCollectionWALFormatRejectsHeaderPayloadReplayAndTrailerCRCMismatch`
- `TestCollectionWALNoIndexInsertAckBeforeFlushRecovers`
- `TestCollectionWALNoIndexInsertBatchAckBeforeFlushRecovers`
- `TestCollectionWALOffRelaxedNoIndexAckBeforeFlushDoesNotClaimRecovery`
- `TestCollectionWALAppendFailureRejectsBeforeVisibility`
- `TestCollectionWALCrashAfterCommitBeforePublishRecovers`
- `TestCollectionWALCrashAfterPublishBeforeResponseIsIdempotent`
- `TestCollectionWALDescriptorAndWatermarkPublishAtomically`
- `TestCollectionWALCatalogDescriptorPersistsUIDGenerationEpochDigests`
- `TestCollectionWALRootRefRejectsNameOnlyIdentity`
- `TestCollectionWALCollectionUIDDropRecreateDoesNotReplayByName`
- `TestCollectionWALReadOnlyOpenWithPendingWALFails`
- `TestCollectionWALInlineCapRejectsBeforeVisibility`
- `TestCollectionWALMissingUncleanedSegmentFailsOpen`
- `TestCollectionWALIndexedSchemaUnsupportedBeforeStaging`
- `TestCollectionWALIndexedAsyncUnsupported`
- `TestCollectionWALUpdateUnsupportedBeforeMutation`
- `TestCollectionWALDeleteUnsupportedBeforeMutation`
- `TestCollectionWALValueLogPointerizationUnsupportedBeforeVisibility`
- `TestCollectionWALColumnRootKindUnsupported`
- `TestCollectionWALRootDeltaPayloadUnsupported`
- `TestCollectionWALWALOffDoesNotCreateCollectionWAL`
- `TestCollectionWALCheckpointRetainsSegments`
- `TestCollectionWALCloseRetainsSegments`
- `TestCollectionWALCleanupDisabledInPR1`
- current flush-boundary regression tests remain green with the feature off.

Full-contract required coverage:

- `TestCollectionWALFormatGoldenV1ValueLeafAndRootDeltaSideRefs`
- `TestCollectionWALFormatGoldenV1LargeRootDeltaSidePayload`
- `TestCollectionWALFormatGoldenV1TombstoneDelete`
- `TestCollectionWALFormatGoldenV1CleanupRecordAndSegmentMetadata`
- `TestCollectionWALOnRelaxedUpdateBatchAckBeforeFlushRecovers`
- `TestCollectionWALOnRelaxedDeleteBatchAckBeforeFlushRecovers`
- `TestCollectionWALOnRelaxedCreateCollectionAckReopens`
- `TestCollectionWALOnRelaxedCreateIndexBackfillAckReopens`
- `TestCollectionWALOnRelaxedCreateIndexUniqueConflictNoSchemaAfterReopen`
- `TestCollectionWALOffRelaxedBufferedInsertLostWithoutFlush`
- `TestCollectionWALOffRelaxedFlushEstablishesReopenBoundary`
- `TestCollectionWALOffRelaxedCheckpointDrainsKnownDomains`
- `TestCollectionWALOffRelaxedCloseDrainsSuccessfulWrites`
- `TestCollectionWALSideRefFailureRejectsWriteBeforeVisibility`
- `TestCollectionWALPostCommitVisibleInstallFailureCommitAmbiguous`
- `TestCollectionWALPublishFailureReportedByFlushCheckpointClose`
- `TestInsertBatchUniqueConflictBeforeWALLeavesNoPartialItems`
- `TestUpdateBatchItemErrorBeforeWALLeavesNoPartialItems`
- `TestDeleteBatchWALAppendFailureLeavesAllDocuments`
- `TestCollectionWALIndexedInsertRecoverAtomically`
- `TestCollectionWALIndexedUpdateChangedSecondaryRecoverAtomically`
- `TestCollectionWALIndexedUpdateUnchangedSecondarySkipsSecondaryRootsAfterRecovery`
- `TestCollectionWALIndexedDeleteRecoverAtomically`
- `TestCollectionWALUniqueReuseAfterDeleteRecovery`
- `TestCollectionWALIndexSameNameRecreateUsesNewUID`
- `TestCollectionWALSchemaChangeDrainsLowerSeqBeforeVisible`
- `TestCollectionWALDropIndexWithPendingOldIndexUIDCannotResurrectRoot`
- `TestCollectionWALTemplateRootGuardRequired`
- `TestCollectionWALTemplatePrimaryAndTemplateRootRecoverAtomically`
- `TestColumnPartDescriptorGenerationDigestGuards`
- `TestColumnCompactionPublishesSupersessionAndTargetDescriptorsAtomically`
- `TestCollectionWALBufferedSameBaseRootTransactionsReplayByAccumulator`
- `TestCollectionWALPartialFrameAndMissingSideRefNoPhantomRoots`
- `TestCollectionWALWatermarkOutOfOrderTxnDoesNotSkipLowerUnapplied`
- `TestCollectionWALRecoveryCrashAndCleanupAreIdempotent`
- `TestCollectionWALGCRewriteCompactionSnapshotsProtectPendingSideRefs`
- `TestCollectionWALCheckpointChosenRule`
- `TestCollectionWALFlushPublishesAndAdvancesWatermark`
- `TestCollectionWALFlushAllWaitsForAsyncPublishAndReopens`
- `TestDBCheckpointPublishesPreCutCollectionWALAndReopens`
- `TestDBCheckpointDoesNotReturnCleanWithUnpublishedPreCutDebt`
- `TestDBCheckpointRacingWriteCut`
- `TestDBCloseRacingInsertUpdateDeleteIndex`
- `TestDBCloseSafeCleanupLeak`
- `TestCollectionWALCloseSuccessfulWritesReopenVisible`
- `TestCollectionWALReadOnlyOpenWithPendingWAL`
- `TestCollectionWALStatsAppendSuccess`
- `TestCollectionWALStatsAppendFailureBeforeCommit`
- `TestCollectionWALStatsExpvarWhitelist`
- `TestCollectionWALStatsNativeWire`
- `TestCollectionWALErrorCategoriesStable`
- `TestCollectionWALRedaction`
- `TestCollectionWALMetricMonotonicity`
- `TestRecoveryMissingRequiredSideRefFailsHard`
- `TestRecoveryCorruptRequiredSideRefFailsHard`
- `TestRecoveryUnsupportedVersionFailsWithCategory`
- `TestRecoveryCleanupFailureSafeLeak`
- `TestCollectionWALHealthCleanGolden`
- `TestCollectionWALHealthPendingGolden`
- `TestCollectionWALSafeDeleteDryRunGolden`
- `TestCollectionWALTxnLookupGolden`
- `TestVerifyCollectionWALSideRefsGolden`
- `TestCollectionWALValueLogGCBlockedByPendingSideRef`
- `TestCollectionWALValueLogGCReleasedAfterWatermarkCheckpoint`
- `TestCollectionWALArtifactRedaction`
- `TestCollectionWALModelDescriptorWatermarkSplitRejected`
- `TestCollectionWALModelVisibleImpliesRecoverable`
- `TestCollectionWALModelSideRefProtectHappensBeforeGuardRelease`
- `TestCollectionWALModelDeterministicReplayDigest`
- `TestCollectionWALModelStateClassifierExclusive`
- `TestCollectionWALModelSkipUsesCollectionSeqOnly`
- `TestCollectionWALModelMaintenanceGuardBlocksRewrite`
- `TestCollectionWALModelRaftAppliedIndexRequiresLocalRecoverability`
- `TestCollectionWALTypedPublishWrapperRejectsFreeFormSystemDelta`
- `TestCollectionWALCostEstimatorMatchesEncoder`
- `TestCollectionWALRootDeltaSpillThresholds`
- `TestCollectionWALOversizedTransactionFailsBeforeAck`
- `TestCollectionWALPendingDebtSoftStopHardLimits`
- `TestCollectionWALBackpressureResumeWatermark`
- `TestCollectionWALReplayAccumulatorSoftCapChunksOrSpills`
- `TestCollectionWALReplayAccumulatorHardCapStopsRecovery`
- `TestCollectionWALProtectedSideRefRetainedSegmentDebt`
- `TestCollectionWALCleanupDebtBackpressure`
- `TestCollectionWALSegmentRotationAndCheckpointRotation`
- `TestCollectionWALDurableSyncBatchCaps`
- `TestColumnWALSideRefCapacityLimits`
- `TestNativewireInsertBatchWALAppendFailureNotCommitted`
- `TestNativewireInsertBatchAckFlushedPublishFailureCommitAmbiguous`
- `TestNativewireInsertBatchAckSyncedCheckpointFailureCommitAmbiguous`
- `TestNativewireAckSyncedRejectedInWALOnRelaxed`
- `TestNativewireResponseMetaActualAckAndCommitState`
- `TestMongoGatewayInsertDocumentsDurabilityModeDocumented`
- `TestMongoGatewayUpdateOrderedPartialSemantics`
- `TestMongoGatewayDeleteOrderedPartialSemantics`
- `TestMongoGatewayCreateIndexesUniqueConflictNoPartialSchema`

Formal invariant mapping:

| Invariant | Required evidence |
|---|---|
| `I1` gap-free sequence | `TestCollectionWALWatermarkOutOfOrderTxnDoesNotSkipLowerUnapplied`; model transition test for missing lower same-collection sequence. |
| `I2` WAL-before-visible | `TestCollectionWALModelVisibleImpliesRecoverable`; fault injection before/after side-ref prepare, WAL marker, and visible install. |
| `I3` side refs before WAL | `TestCollectionWALPartialFrameAndMissingSideRefNoPhantomRoots`; side-ref closure fuzz target. |
| `I4` WAL side-ref protection | `TestCollectionWALModelSideRefProtectHappensBeforeGuardRelease`; GC/rewrite interleaving model test. |
| `I5` descriptor/watermark atomicity | `TestCollectionWALModelDescriptorWatermarkSplitRejected`; `TestCollectionWALTypedPublishWrapperRejectsFreeFormSystemDelta`. |
| `I6` per-collection skip | `TestCollectionWALModelSkipUsesCollectionSeqOnly`; mixed-collection segment cleanup model test. |
| `I7` whole-transaction publish | Indexed insert/update/delete recovery tests and root-group publish fault tests. |
| `I8` checkpoint before cleanup | `TestCollectionWALRecoveryCrashAndCleanupAreIdempotent`; cleanup manifest corruption fixtures. |
| `I9` deterministic replay | `TestCollectionWALModelDeterministicReplayDigest`; replay accumulator property test comparing live and recovery digests. |
| `I10` no double apply | Repeated recovery after `S4 Applied` watermark and crash-after-publish tests. |
| `I11` maintenance serialized | `TestCollectionWALModelMaintenanceGuardBlocksRewrite`; value-log GC/rewrite blocked-by-pending-side-ref tests. |
| `I12` Raft local metadata | `TestCollectionWALModelRaftAppliedIndexRequiresLocalRecoverability`; future Raft apply crash matrix. |
| `I13` WAL-off exception | `TestCollectionWALOffRelaxedNoIndexAckBeforeFlushDoesNotClaimRecovery`; WAL-off model branch. |

The invariant table is evidence only when each entry points to an executable Go
test, model-checkable transition test, fuzz target, or generated proof artifact
recorded in `artifacts/collection-wal/<milestone>/acceptance.json`.

Acceptance artifacts:
- The collection WAL gate requires exact byte fixtures, not only round-trip
  tests. Each fixture must include raw bytes, decoded JSON or Go struct
  expectation, replay digest expectation, checksum expectation, and a
  re-encode-identical assertion.
- Required fixture families:
  - segment header v1 empty segment;
  - minimal no-index transaction with one inline root delta;
  - transaction with value-log, leaf-log, and root-delta side refs;
  - transaction with descriptor op and watermark system-delta template;
  - large root delta using side-payload ref;
  - transaction with tombstone/delete entry;
  - cleanup record and segment metadata;
  - future unknown critical section rejection;
  - future unknown noncritical section skip;
  - unsupported version fail-closed;
  - malformed length, header CRC, payload CRC, replay digest, and trailer CRC.
- Every completed milestone must write
  `artifacts/collection-wal/<milestone>/acceptance.json`.
- Benchmark artifacts must include the storage matrix above, baseline/new
  `benchstat` output, required metric rows from
  `collection-wal-durability-plan.md`, and the pass/fail decision.
- Benchmark artifacts must include a JSON record with `durability_mode`,
  `sync_policy`, `segment_size_bytes`, `batch_docs`, `doc_size_bytes`,
  `side_ref_classes`, `collection_count`, `backend`, `go_version`, and
  `commit`. Required metric names include `collection_wal_append_ns/doc`,
  `collection_wal_bytes/doc`, `collection_wal_docs/sec`,
  `collection_wal_side_refs/doc`, `pending_collection_wal_bytes`,
  `applied_watermark_lag_txns`, `gc_protected_side_ref_bytes`,
  `cleanup_ns/segment`, `recovery_docs/sec`,
  `recovery_root_delta_entries/sec`, `recovery_peak_heap_bytes`, `allocs/doc`,
  and `bytes_allocated/doc`.
- Resource-budget benchmark artifacts must include the phase-isolated columns
  from `collection-wal-durability-plan.md`: mutation class, storage cell,
  document format, doc bytes, key/value bytes, root deltas/transaction,
  root-delta entries/doc, root-delta payload bytes/doc, collection WAL frame and
  metadata bytes/doc, compressed bytes/doc, root-delta side-payload bytes/doc,
  side-ref metadata bytes/doc, side-ref payload bytes/doc, protected side-ref
  retained segment bytes, pending WAL bytes, pending side-payload bytes,
  cleanable WAL bytes, cleanup debt bytes, applied watermark lag, oldest
  unapplied age, ack p50/p95/p99, encode/append/prepare/publish/checkpoint
  phase timings, sync batch txns/bytes, fsyncs/sec, recovery scan MB/sec,
  recovery side refs/sec, peak heap/RSS, replay spill bytes, cleanup bytes/sec,
  blocked writes, backpressure wait p99, and error count.
- The benchmark gate fails when required columns are missing, when
  formula-derived bytes/doc is exceeded by more than 10 percent, or when an
  absolute ceiling from the resource accounting section is exceeded, even when
  relative `benchstat` regression thresholds pass.
- Required resource benchmark commands include
  `cmd/collection_workload_bench`, `cmd/collection_bench_matrix`, and
  `cmd/collection_bench_report`, or an equivalent `cmd/unified_bench`
  collection WAL suite that emits the same schema.
- Collection WAL observability tests must prove every required metric is emitted
  on successful append, append failure before commit marker, replay success,
  incomplete-tail skip, missing side-ref hard failure, and cleanup failure after
  watermark/checkpoint.
- Production persistent column-store writes may start only after the M7
  column-store sign-off artifact links to green M1-M6 collection WAL evidence.

Required collection WAL hardening fuzz targets:

- `FuzzCollectionWALDecodeTransaction`;
- `FuzzCollectionWALDecodeTransactionNoPreChecksumAlloc`;
- `FuzzCollectionWALDecodeSideRefs`;
- `FuzzCollectionWALDecodeRootDelta`;
- `FuzzCollectionWALRootDeltaPayloadStreaming`;
- `FuzzCollectionWALRecoveryOrdering`;
- `FuzzCollectionWALUnknownFieldsAndRefClasses`;
- `FuzzCollectionWALPathCanonicalize`;
- `FuzzCollectionWALValuePtrSideRefs`;
- `FuzzNativeWireCollectionCommandToWALPlan`.

Required fuzz properties: no panic, bounded allocation, no root publish on
invalid bytes, no file deletion/quarantine from invalid bytes, deterministic
error class for identical input, and no skip of a complete corrupt transaction
in favor of a later same-collection transaction.

Required collection WAL hardening fixtures and tests:

- `TestCollectionWALMaxEncodedTxnRejectsBeforeAlloc`;
- `TestCollectionWALFrameLengthOverflowRejects`;
- `TestCollectionWALVarintOverflowRejects`;
- `TestCollectionWALRootDeltaCountLimitRejects`;
- `TestCollectionWALSideRefCountLimitRejects`;
- `TestCollectionWALDuplicateSideRefConflictingChecksumRejects`;
- `TestCollectionWALUnknownRequiredRefClassFatal`;
- `TestCollectionWALUnknownOptionalRefClassCannotCleanup`;
- `TestCollectionWALBadFrameCRCCompleteRecordFailsOpen`;
- `TestCollectionWALBadTxnCRCCompleteRecordFailsOpen`;
- `TestCollectionWALTruncatedActiveTailIgnoredOnlyWithoutCommitMarker`;
- `TestCollectionWALTruncatedSealedSegmentFailsOpen`;
- `TestCollectionWALMiddleCorruptionBlocksLaterSeq`;
- `TestCollectionWALMissingSeqNBlocksSeqNPlusOne`;
- `TestCollectionWALDropRecreateSameNameDifferentUIDRejectsOldTxn`;
- `TestCollectionWALRootRefOutsideCatalogSetRejects`;
- `TestCollectionWALRootUIDKindGenerationMismatchRejects`;
- `TestCollectionWALSchemaEpochMismatchRejects`;
- `TestCollectionWALIndexDefinitionDigestMismatchRejects`;
- `TestCollectionWALPublishRequiresAllChecks`;
- `TestCollectionWALSideRefRelativePathDotDotRejects`;
- `TestCollectionWALSideRefRelativePathAbsoluteRejects`;
- `TestCollectionWALSideRefRelativePathWindowsDriveRejects`;
- `TestCollectionWALSideRefRelativePathUNCRejects`;
- `TestCollectionWALSideRefRelativePathBackslashRejects`;
- `TestCollectionWALSideRefRelativePathNULRejects`;
- `TestCollectionWALSideRefRelativePathEmptyComponentRejects`;
- `TestCollectionWALSideRefPathFileIDMismatchRejects`;
- `TestCollectionWALCollectionNameNeverUsedAsPath`;
- `TestNativeWireCatalogGuardV1CanonicalizesStableIDs`;
- `TestNativeWireNameDropRecreateRaceDeterministicGuardFailure`;
- `TestRaftMetadataIDsDeterministicAcrossReplicas`;
- `TestCollectionWALOpenRejectsSymlinkDBRoot`;
- `TestCollectionWALOpenRejectsSymlinkWALDir`;
- `TestCollectionWALOpenRejectsWorldWritableWALDir`;
- `TestCollectionWALOpenRejectsGroupWritableClassRoot`;
- `TestCollectionWALSideRefOpenRejectsSymlinkComponent`;
- `TestCollectionWALSideRefOpenRejectsSymlinkFinalFile`;
- `TestCollectionWALSideRefCleanupDoesNotFollowSymlink`;
- `TestCollectionWALSideRefCleanupRejectsHardlink`;
- `TestCollectionWALPreparedRenameRejectsCrossDevice`;
- `TestCollectionWALLockfileRejectsSymlink`;
- `TestCollectionWALNoAllocBeforeChecksumForHugeRootCount`;
- `TestCollectionWALNoAllocBeforeChecksumForHugeSideRefCount`;
- `TestCollectionWALNoAllocBeforeChecksumForHugeStringLength`;
- `TestCollectionWALCompressedRawLenBombRejectsBeforeDecode`;
- `TestCollectionWALOffsetSizeOverflowRejects`;
- `TestCollectionWALUint64ToInt64OffsetOverflowRejects`;
- `TestCollectionWALLimitsMaxRecordSizeDisabledDoesNotDisableWALCap`;
- `TestCollectionWALNativeWire64MiBCommandSpillsOrRejectsBeforeSideEffects`;
- `TestCollectionWALCorruptErrorRedactsCollectionName`;
- `TestCollectionWALMissingSideRefErrorRedactsRelativePathUnlessAdmin`;
- `TestCollectionWALDuplicateKeyErrorRedactsDocumentID`;
- `TestNativeWireErrorRedactsDocuments`;
- `TestCollectionWALMetricsUseUIDAndHashesNotNames`;
- `TestForensicToolRawOutputRequiresExplicitFlag`.

Required collection WAL maintenance, backup, restore, and offline precondition
tests:

- `TestCollectionWALValueLogGCSkipsWALOnlySideRef`;
- `TestCollectionWALValueLogRewriteSkipsProtectedWALOnlySource`;
- `TestCollectionWALCompactStorageAbortsOnCheckpointDebt`;
- `TestCollectionWALSideRefPrepareGuardBlocksGCWindow`;
- `TestCollectionWALLeafGenerationGCKeepsPendingLeafRef`;
- `TestCollectionWALVacuumOnlinePublishesOrRejectsDirtyWAL`;
- `TestCollectionWALBackupBarrierCleanCheckpointRestoresWithoutWALDebt`;
- `TestCollectionWALBackupBarrierWALSnapshotRestoresPendingTxn`;
- `TestCollectionWALFilesystemBackupWithoutBarrierUnsupported`;
- `TestCollectionWALBackupIncludesValueLeafDictTemplateAndColumnSideRefs`;
- `TestCollectionWALRestoreFailsWhenWALTxnMissingSidePayload`;
- `TestCollectionWALRestoreAcceptsMissingCleanedSegmentOnlyWithCleanupManifest`;
- `TestReadOnlyOpenRejectsUnappliedCollectionWAL`;
- `TestReadOnlyOpenAllowsCleanCollectionWAL`;
- `TestReadOnlyStaleModeReportsDebtAndIsRejectedByMaintenance`;
- `TestOpenReadOnlyNoLockRejectsDirtyCollectionWALForOfflineRewrite`;
- `TestValueLogRewriteOfflineRejectsDirtyCollectionWAL`;
- `TestVacuumIndexOfflineRejectsDirtyCollectionWAL`;
- `TestOfflineMaintenanceRejectsUnclassifiedPreparedSideRefs`;
- `TestOfflineMaintenanceAllowsCleanedCollectionWALWithManifest`;
- `TestCollectionWALCheckpointPublishesBeforeCleanup`;
- `TestCollectionWALCleanupRequiresDurableCheckpointBoundary`;
- `TestCollectionWALSegmentCleanupDecodesEveryFrame`;
- `TestCollectionWALCleanupDoesNotReleaseProtectionBeforeReachabilityHandoff`;
- `TestCollectionWALPreparedUncommittedSideFilesQuarantinedAfterRestore`;
- `TestCollectionWALQuarantinePurgeRequiresCheckpoint`.

Required non-mutating collection WAL CLI tooling:

- `treemap collection-wal health --dir <db> --json` reports `db_dir_hash`,
  `format_version`, `generated_at_unix_nano`, `overall_state`,
  `safe_to_restart`, `safe_to_backup`, `safe_to_compact`,
  `requires_recovery`, `requires_operator_action`, `metrics`, `collections`,
  `segments`, `pending_transactions`, `protected_side_refs`, `cleanup_debt`,
  `gc_blockers`, `last_recovery`, and `errors`.
- `treemap collection-wal safe-delete --dir <db> --json --dry-run` classifies
  files without mutation. Per-file fields are `file_id`, `relative_path`,
  `path_hash`, `class`, `bytes`, `status`, `safe_to_delete`, `delete_reason`,
  `blocking_reason`, `blocking_txn_ids`, `blocking_collection_uid_hashes`,
  `blocking_side_ref_ids`, `blocking_snapshot_ids`, `requires_checkpoint`,
  `requires_recovery`, and `requires_quarantine`.
- `treemap collection-wal txn --dir <db> --txn-id <id> --json` and
  `treemap collection-wal txn --dir <db> --wallsn <n> --json` map one
  transaction to `txn_id`, `wallsn`, `segment_id`, `segment_offset`,
  `collection_uid`, `collection_uid_hash`, `collection_generation`,
  `collection_seq`, `depends_on_collection_seq`, `schema_epoch`,
  `catalog_epoch`, `base_root_id`, `base_root_digest`, `root_name_hashes`,
  `root_delta_count`, `side_refs`, `record_checksum_crc32c`, `replay_digest`,
  `applied_watermark_seq`, `watermark_state`, `replay_state`, and
  `cleanup_state`.
- `verify --dir <db> --read-only --collection-wal --side-refs --json` verifies
  collection WAL side-ref closure without mutation. JSON fields include
  `collection_wal_checked`, `roots_checked`, `root_deltas_checked`,
  `side_refs_declared`, `side_refs_canonical`, `side_refs_present`,
  `side_refs_missing`, `side_refs_corrupt`, `side_ref_closure_errors`,
  `watermark_errors`, `cleanup_manifest_errors`, and `result`.
  It must detect declared/canonical side-ref mismatches, missing side-ref files,
  checksum/digest mismatches, roots that point past the applied watermark, and
  cleanup manifests that claim safe deletion while a root or pending WAL
  transaction still references the side file.
- `treemap collection-wal classify --dir <db> --json` parses collection WAL
  segment headers, frames, decoder outcomes, error categories, side-ref
  summaries, and redacted transaction summaries. The existing `wal_classify`
  value-log-oriented command must either be renamed to `vlog_classify` or kept
  explicitly documented as value-log-only to avoid operator confusion.

Required safe-delete statuses are `safe_cleaned_segment`,
`pending_collection_wal`, `protected_side_ref`, `orphan_prepared_side_ref`,
`missing_required_side_ref`, `corrupt_required_side_ref`,
`cleanup_manifest_required`, `snapshot_pinned`, and `unknown_unclassified`.

The collection WAL verification mode is read-only by default. Any repair,
vacuum, cleanup, or quarantine mutation must require an explicit mutating flag
such as `--repair`, `--vacuum-index`, or a future `--mutate`.

Docs lint should enforce the observability contract once the collection WAL
implementation starts. Required lint checks:

- `collection-wal-durability-plan.md` contains the stable
  `treedb.collection_wal.` metric prefix table;
- `recovery.md` contains the stable recovery error category table;
- `verification.md` contains the required `treemap collection-wal health`,
  `safe-delete`, `txn`, `classify`, and `verify --collection-wal` command names;
- the operator runbook states include `clean`, `pending`, `recovery_required`,
  `corrupt`, and `cleanup_debt`.

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
