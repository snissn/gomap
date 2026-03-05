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

## 11. Collection Document and Index Coherence

Invariant:
- Collection metadata round-trips cleanly.
- Caller-provided and auto-generated ids remain stable across reopen.
- Primary document writes live in dedicated named roots keyed by `_id`, not in
  the legacy shared `col:d:` keyspace.
- Secondary-index entries live in dedicated named roots, not in the legacy
  shared `col:i:` keyspace.
- Dedicated-root format bits override DB-global outer-leaf placement in both
  directions: primary roots can use leafrefs while the user root stays
  pager-backed, and secondary roots stay pager-backed while the user root uses
  outer-leaf-vlog mode.
- Primary document writes and derived secondary-index entries stay mutually
  coherent for normal collection mutations.
- DB-wide maintenance (`ValueLogGC`, `ValueLogRewriteOnline`,
  `ValueLogRewriteOffline`, `VacuumIndexOnline`, `VacuumIndexOffline`) keeps
  named-root collection data and secondary indexes reachable across reopen.
- Collection diagnostics report consistent counts on healthy state and detect
  missing/orphan secondary-index entries after deliberate corruption.

Coverage:
- `TreeDB/collections/collection_lifecycle_test.go`
- `TreeDB/collections/id_generation_test.go`
- `TreeDB/collections/root_catalog_test.go`
- `TreeDB/collections/document_primary_test.go`
- `TreeDB/collections/root_format_live_write_test.go`
- `TreeDB/collections/secondary_index_lifecycle_test.go`
- `TreeDB/collections/secondary_index_query_test.go`
- `TreeDB/collections/secondary_index_update_test.go`
- `TreeDB/collections/secondary_index_conflict_test.go`
- `TreeDB/db/named_root_maintenance_test.go`
- `TreeDB/db/named_root_format_closeout_test.go`
- `TreeDB/collections/collection_tx_atomic_test.go`
- `TreeDB/collections/recovery_matrix_test.go`
- `TreeDB/collections/maintenance_api_test.go`
- `TreeDB/collections/api_doc_test.go`
- `TreeDB/collections/edge_case_test.go`
- `TreeDB/collections/consistency_fuzz_test.go`
- `TreeDB/db/named_root_catalog_test.go`
- `TreeDB/db/named_root_api_test.go`
- `TreeDB/db/system_root_catalog_internals_test.go`

Benchmark coverage:
- `TreeDB/collections/collection_bench_test.go`:
  - `BenchmarkCollectionCreate`
  - `BenchmarkCollectionInsertProvidedID`
  - `BenchmarkCollectionInsertAutoID`
  - `BenchmarkCollectionGetByID`
  - `BenchmarkCollectionDeleteByID`
  - `BenchmarkSecondaryLookupUnique`
  - `BenchmarkSecondaryUpsertFieldChange`
  - `BenchmarkCollectionStats`
  - `BenchmarkCollectionCheckConsistency`
  - `BenchmarkCollectionMeta_EncodeLarge`
  - `BenchmarkCollectionRootDescriptorEncode`
