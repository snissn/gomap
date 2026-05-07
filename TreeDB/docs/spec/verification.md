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

Coverage:
- `TreeDB/db/compact_index_test.go`
- `TreeDB/db/compact_index_sequential_alloc_test.go`
- `TreeDB/db/vacuum_online_swap_test.go`

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

## 12. Collections Document Formats

Invariant:
- Template-v1 collections persist their template ID map in the collection-local
  `<collection>/templates` TreeDB ordered root.
- Template-v1 primary documents store compact `TD1D` bytes and resolve template
  IDs from the current batch or from the persisted template root.
- Secondary indexes, deletes, reopens, and index backfills use the template root
  instead of JSON parsing.

Coverage:
- `TreeDB/collections/template_v1_test.go`:
  - `TestTemplateV1CollectionInsertBatchIndexesAndTemplateRoot`
  - `TestTemplateV1CollectionReopenFindAndDelete`
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
  vectors, fuzz targets, parity tests, deterministic-entry tests, benchmark
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

The native-wire server does not exist yet. R0 follow-up work must add
deterministic-entry coverage, broader negative conformance fixtures, drift
tests, and direct collection parity tests before claiming native-wire v1 server
support.
