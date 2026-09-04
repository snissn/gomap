# TreeDB Verification Matrix

This document maps specification invariants to existing tests and harnesses.

## Minima native-path contract (#4615)

`minima-native-execution.md` defines the target, not current mutable graph
support. `TreeDB/cmd/treedb_rag_benchmark/minima_bounded_test.go` and the existing
`TestMinima` suite check versioned bounded fixtures, frozen full-manifest
compatibility, unavailable-versus-zero native proof, and diagnostic-only
qualification boundaries. The Python `test_minima*runner.py` suites and
`scripts/bench_minima_qualification_test.sh` cover actual runner and bounded
process execution. The runbook records their commands.

Existing collection command-WAL indexed staging, typed-column replay without
checkpoint, and mutation-asset tests establish reuse boundaries. They do not
certify the future typed Minima overlay; #4616–#4619 must add typed admission,
replay, snapshot, fold/crash and public-route tests as those features land.

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
- Full storage compaction preserves value visibility, removes reachable debt only
  through the documented lifecycle, serializes backend maintenance phases, and
  keeps cached-mode value-log writers from reusing backend-created segments.

Coverage:
- `TreeDB/db/compact_index_test.go`
- `TreeDB/db/compact_index_sequential_alloc_test.go`
- `TreeDB/db/vacuum_online_swap_test.go`
- `TreeDB/db/compact_storage_test.go`
  - `TestCompactStorageHoldsMaintenanceLockAcrossPhases`
- `TreeDB/db/compact_storage_audit_test.go`
  - shared-walk call counts, snapshot-basis revalidation, structural reuse,
    grouped pointer accounting, and legacy planner parity
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

- one `CommitAt` call is one atomic TreeDB batch at one nonzero caller
  timestamp, with deterministic pre-write duplicate rejection;
- puts, empty values, and historical tombstones remain distinguishable;
- `GetAt` returns the newest retained version at or below the read timestamp by
  a direct bounded seek, without collecting version history;
- durable commits require durable TreeDB mode and survive a process crash;
  relaxed commits require a later checkpoint/close boundary for reopen proof;
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
- a prune paused after snapshot capture does not block foreground `GetAt`,
  `CommitAt`, or `IterateVersions` acquisition, while a concurrent floor
  advance remains serialized behind the prune.

Coverage:

- `TreeDB/mvcc/versions_test.go` covers directional golden histories, binary
  keys, seek, copied ownership, prefix/bound/read-time filters, tombstones,
  floor rejection/regression, value and tombstone anchors, reopen,
  interrupted-batch restart, idempotence, and concurrent snapshot readers.
- The same suite deterministically pauses prune iterator creation after its
  snapshot is pinned, proves foreground point reads, commits, and retained-
  version iterator acquisition complete, and then verifies both the old pinned
  view and the newly committed version after pruning resumes.
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

Measurement boundary:

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
  - `TestSearchTextFieldWeightAffectsRankingM4`
  - `TestSearchTextMissingIndexUnsupportedSyntaxAndTruncationM4`
  - `TestSearchTextSeesUnflushedTextIndexedInsertM4`
  - `TestSearchTextTombstonedPostingsConsumeScanBudgetM4`
  - `TestSearchTextFailClosedWrapsStorageCorruptionM4`
  - `TestSearchTextTopKBoundsDocumentFetchM4`
  - `TestSearchTextReopenParityM4`
  - `BenchmarkSearchTextM4`

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
