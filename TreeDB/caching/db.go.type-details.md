# Type Details: TreeDB/caching/db.go

## routePersistedGapScanner (469-473)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `it` | `iterator.UnsafeIterator` |
| `cur` | `[]byte` |
| `valid` | `bool` |

## vlogPreparedFrameBody (554-556)

- kind: struct
- fields: 1

| Field | Type |
|---|---|
| `buf` | `[]byte` |

## batchEntrySliceRef (558-560)

- kind: struct
- fields: 1

| Field | Type |
|---|---|
| `entries` | `[]batch.Entry` |

## outerLeafRecordGroup (1750-1753)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `start` | `int` |
| `end` | `int` |

## deferredFenceCollapseState (2843-2847)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `lastPtr` | `page.ValuePtr` |
| `havePtr` | `bool` |
| `prevKey` | `[]byte` |

## fencePendingMutationKind (2849-2849)

- kind: alias/basic
- type: `uint8`

## fencePendingMutation (2858-2861)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `kind` | `fencePendingMutationKind` |
| `ptr` | `page.ValuePtr` |

## fenceMutationLookupFn (2912-2912)

- kind: alias/basic
- type: `func(key []byte) (fencePendingMutation, bool)`

## fenceAnchorPromoter (3018-3057)

- kind: struct
- fields: 28

| Field | Type |
|---|---|
| `db` | `*DB` |
| `lookup` | `fenceMutationLookupFn` |
| `snapshot` | `*backenddb.Snapshot` |
| `snapshotReady` | `bool` |
| `keysByPtr` | `map[page.ValuePtr][][]byte` |
| `promoted` | `map[string]page.ValuePtr` |
| `rangeLoaded` | `bool` |
| `rangeKnown` | `bool` |
| `backendRange` | `keyRange` |
| `backendEmptyChecked` | `bool` |
| `backendEmpty` | `bool` |
| `deleteRematerializedBySource` | `map[page.ValuePtr]struct{}` |
| `overlapRewriteBySource` | `map[page.ValuePtr]*fenceSourceRewritePlan` |
| `overlapRewriteOrder` | `[]page.ValuePtr` |
| `rewrittenSourceAlias` | `map[page.ValuePtr][]fenceSourceRewriteAlias` |
| `lastOverlapPlan` | `*fenceSourceRewritePlan` |
| `lastOverlapSourcePtr` | `page.ValuePtr` |
| `routeCursorReady` | `bool` |
| `routeCursor` | `iterator.UnsafeIterator` |
| `routeCursorCurrent` | `[]byte` |
| `routeCursorPtr` | `page.ValuePtr` |
| `routeCursorHaveCur` | `bool` |
| `routeCursorNext` | `[]byte` |
| `routeCursorNextPtr` | `page.ValuePtr` |
| `routeCursorHaveNext` | `bool` |
| `routeCursorLastKey` | `[]byte` |
| `routeDeleteDecision` | `map[uint64][]routeDeleteDecisionEntry` |
| `rewriteEntries` | `[]outerleaf.TypedEntry` |

## routeDeleteDecisionEntry (3059-3062)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `key` | `[]byte` |
| `emit` | `bool` |

## overlapRewritePending (3064-3069)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `plans` | `[]*fenceSourceRewritePlan` |
| `anchorKey` | `[]byte` |
| `maxKey` | `[]byte` |
| `payload` | `[]byte` |

## fenceSourcePendingSet (3071-3075)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `key` | `[]byte` |
| `value` | `[]byte` |
| `hash` | `uint64` |

## fenceSourceRewritePlan (3077-3087)

- kind: struct
- fields: 6

| Field | Type |
|---|---|
| `sourceKey` | `[]byte` |
| `sourcePtr` | `page.ValuePtr` |
| `sets` | `[]fenceSourcePendingSet` |
| `setIndex` | `map[uint64]int` |
| `sourceKeys` | `[][]byte` |
| `sourceKeyCount` | `int` |

## fenceSourceRewriteAlias (3091-3095)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `sourceKey` | `[]byte` |
| `maxKey` | `[]byte` |
| `sourcePtr` | `page.ValuePtr` |

## overlapRewriteBlock (3904-3908)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `anchorKey` | `[]byte` |
| `maxKey` | `[]byte` |
| `payload` | `[]byte` |

## fenceProjectionIteratorProvider (4851-4853)

- kind: interface
- methods: 1

| Method/Embed | Signature |
|---|---|
| `IteratorWithOptions` | `func(start, end []byte, opts backenddb.IteratorOptions) (iterator.UnsafeIterator, error)` |

## backendPointerProjectionIterator (7372-7374)

- kind: interface
- methods: 1

| Method/Embed | Signature |
|---|---|
| `IteratorWithOptions` | `func(start, end []byte, opts tree.IteratorOptions) (iterator.UnsafeIterator, error)` |

## valueLogZombieMarker (7491-7493)

- kind: interface
- methods: 1

| Method/Embed | Signature |
|---|---|
| `MarkValueLogZombie` | `func(id uint32) error` |

## valueLogSetRefresher (7495-7497)

- kind: interface
- methods: 1

| Method/Embed | Signature |
|---|---|
| `RefreshValueLogSet` | `func() error` |

## BackendDB (7666-7678)

- kind: interface
- methods: 11

| Method/Embed | Signature |
|---|---|
| `Get` | `func(key []byte) ([]byte, error)` |
| `GetUnsafe` | `func(key []byte) ([]byte, error)` |
| `GetAppend` | `func(key, dst []byte) ([]byte, error)` |
| `Has` | `func(key []byte) (bool, error)` |
| `Iterator` | `func(start, end []byte) (iterator.UnsafeIterator, error)` |
| `ReverseIterator` | `func(start, end []byte) (iterator.UnsafeIterator, error)` |
| `AcquireSnapshot` | `func() *backenddb.Snapshot` |
| `NewBatch` | `func() batch.Interface` |
| `Close` | `func() error` |
| `Print` | `func() error` |
| `Stats` | `func() map[string]string` |

## Options (7680-7906)

- kind: struct
- fields: 64

| Field | Type |
|---|---|
| `FlushThreshold` | `int64` |
| `MemtableMode` | `string` |
| `MemtableShards` | `int` |
| `DomainIngressWorkers` | `int` |
| `DomainIngressQueueSize` | `int` |
| `MaxQueuedMemtables` | `int` |
| `SlowdownBacklogSeconds` | `float64` |
| `StopBacklogSeconds` | `float64` |
| `MaxBacklogBytes` | `int64` |
| `WriterFlushMaxMemtables` | `int` |
| `WriterFlushMaxDuration` | `time.Duration` |
| `FlushBuildConcurrency` | `int` |
| `FlushBuildMinEntries` | `int` |
| `FlushBuildMinUnits` | `int` |
| `FlushBuildChunkCap` | `int` |
| `FlushBuildChunkTargetBytes` | `int` |
| `FlushBuildChunkMinBytes` | `int` |
| `FlushBuildChunkMaxBytes` | `int` |
| `FlushBuildPrefetchUnits` | `int` |
| `FlushBackendMaxEntries` | `int` |
| `FlushBackendMaxBatches` | `int` |
| `DisableWAL` | `bool` |
| `JournalLanes` | `int` |
| `WALMaxSegmentBytes` | `int64` |
| `JournalCompression` | `bool` |
| `RelaxedSync` | `bool` |
| `ValueLogPointerThreshold` | `int` |
| `IndexOuterLeafMode` | `string` |
| `ValueLogWALFenceMode` | `string` |
| `ValueLogDomainInlineThresholds` | `[]backenddb.ValueLogDomainThreshold` |
| `ValueLogRawWritevMinAvgBytes` | `int` |
| `ValueLogRawWritevMinBatchRecords` | `int` |
| `ValueLogCompression` | `uint8` |
| `ValueLogBlockCodec` | `uint8` |
| `ValueLogBlockTargetCompressedBytes` | `int` |
| `ValueLogOuterLeafBlockTargetBytes` | `int` |
| `ValueLogOuterLeafBlockCodec` | `uint8` |
| `ValueLogOuterLeafBlockRestartInterval` | `int` |
| `ValueLogOuterLeafBlobThresholdBytes` | `int` |
| `ValueLogIncompressibleHoldBytes` | `int` |
| `ValueLogIncompressibleProbeBytes` | `int` |
| `ValueLogAutoPolicy` | `uint8` |
| `ValueLogMaxSegmentBytes` | `int64` |
| `ForceValueLogPointers` | `bool` |
| `DisableReadChecksum` | `bool` |
| `AllowUnsafe` | `bool` |
| `MaxValueLogRetainedBytes` | `int64` |
| `MaxValueLogRetainedBytesHard` | `int64` |
| `ValueLogDictTrain` | `compression.TrainConfig` |
| `ValueLogDictMaxK` | `int` |
| `ValueLogDictFrameEncodeLevel` | `zstd.EncoderLevel` |
| `ValueLogDictFrameEnableEntropy` | `bool` |
| `ValueLogDictAdaptiveRatio` | `float64` |
| `ValueLogDictMetricsWindowBytes` | `int` |
| `ValueLogDictMetricsMinRecords` | `int` |
| `ValueLogDictMetricsPauseBytes` | `int` |
| `ValueLogDictIncompressibleHoldBytes` | `int` |
| `ValueLogDictProbeIntervalBytes` | `int` |
| `ValueLogDictMinPayloadSavingsRatio` | `float64` |
| `ValueLogCompressionAutotune` | `valuelog.AutotuneOptions` |
| `ValueLogTemplateMode` | `template.Mode` |
| `ValueLogTemplateConfig` | `template.Config` |
| `ValueLogTemplateReadStrict` | `bool` |
| `NotifyError` | `func(error)` |

## DictStore (7909-7912)

- kind: interface
- methods: 2

| Method/Embed | Signature |
|---|---|
| `GetCurrent` | `func(ctx context.Context) (uint64, error)` |
| `GetDictBytes` | `func(ctx context.Context, dictID uint64) ([]byte, error)` |

## batchArenaLease (7914-7917)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `refs` | `int` |
| `chunks` | `[][]byte` |

## memtableViewDeferredInfo (7919-7922)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `memtables` | `int64` |
| `sinceUnixNano` | `int64` |

## memtableViewLifecycleTelemetry (7924-7942)

- kind: struct
- fields: 13

| Field | Type |
|---|---|
| `retainTotal` | `atomic.Uint64` |
| `releaseTotal` | `atomic.Uint64` |
| `leasesInFlight` | `atomic.Int64` |
| `leasesInFlightMax` | `atomic.Int64` |
| `deferredViewsCurrent` | `atomic.Int64` |
| `deferredViewsMax` | `atomic.Int64` |
| `deferredViewsTotal` | `atomic.Uint64` |
| `deferredMemtablesCurrent` | `atomic.Int64` |
| `deferredMemtablesMax` | `atomic.Int64` |
| `deferredMemtablesTotal` | `atomic.Uint64` |
| `deferredMu` | `sync.Mutex` |
| `deferred` | `map[*memtableView]memtableViewDeferredInfo` |
| `oldestDeferredUnixNano` | `atomic.Int64` |

## DB (7944-8236)

- kind: struct
- fields: 244

| Field | Type |
|---|---|
| `mu` | `sync.RWMutex` |
| `flushMu` | `sync.Mutex` |
| `writeMu` | `sync.RWMutex` |
| `statsMu` | `sync.Mutex` |
| `bpMu` | `sync.Mutex` |
| `bpCond` | `*sync.Cond` |
| `checkpointMu` | `sync.Mutex` |
| `checkpointCond` | `*sync.Cond` |
| `checkpointing` | `atomic.Bool` |
| `mutableShards` | `[]memShard` |
| `mutableShardMask` | `uint64` |
| `mutableBytes` | `atomic.Int64` |
| `mutableThreshold` | `atomic.Int64` |
| `rotatePending` | `atomic.Bool` |
| `queue` | `[]memtable.Table` |
| `queueShardIDs` | `[]uint16` |
| `queueLaneIDs` | `[]uint16` |
| `queueIDs` | `[]uint64` |
| `queueEnqueueNS` | `[]int64` |
| `nextQueueID` | `atomic.Uint64` |
| `batchEntryHint` | `atomic.Int32` |
| `batchCopyBytesHint` | `atomic.Int32` |
| `batchArenaLeaseMu` | `sync.Mutex` |
| `batchArenaLeasesByMem` | `map[memtable.Table][]*batchArenaLease` |
| `batchEntriesPool` | `sync.Pool` |
| `batchShardEntriesPool` | `sync.Pool` |
| `batchIntPool` | `sync.Pool` |
| `memtables` | `atomic.Pointer[memtableView]` |
| `hashSortedIndexer` | `*memtable.HashSortedIndexer` |
| `appendOnlyMemPool` | `sync.Pool` |
| `appendOnlyMemLeaseMu` | `sync.Mutex` |
| `appendOnlyMemLeases` | `[]*memtable.AppendOnly` |
| `pendingRetiredMems` | `[]memtable.Table` |
| `memtableViewTelemetry` | `memtableViewLifecycleTelemetry` |
| `queueRanges` | `[]keyRange` |
| `queueWALPaths` | `[][]string` |
| `queueValueLogPaths` | `[][]string` |
| `backendRange` | `keyRange` |
| `backendRangeKnown` | `bool` |
| `backendRangeInit` | `sync.Once` |
| `backendRangeErr` | `error` |
| `lanes` | `[]lane` |
| `laneMu` | `sync.Mutex` |
| `laneCond` | `*sync.Cond` |
| `nextLane` | `int` |
| `flushLaneMu` | `[]sync.Mutex` |
| `nextCommitSeq` | `atomic.Uint64` |
| `walAckMu` | `sync.Mutex` |
| `walErr` | `error` |
| `nextRID` | `atomic.Uint64` |
| `ridBootstrapMaxRID` | `uint64` |
| `ridBootstrapInitialNextRID` | `uint64` |
| `ridBootstrapSegmentsScanned` | `uint64` |
| `ridBootstrapRecordsScanned` | `uint64` |
| `disableValueLog` | `bool` |
| `splitValueLog` | `bool` |
| `memtableValueLogPointers` | `bool` |
| `inlineThreshold` | `int` |
| `valueLogThreshold` | `int` |
| `valueLogDomainThresholds` | `[]backenddb.ValueLogDomainThreshold` |
| `indexOuterLeafMode` | `string` |
| `valueLogWALFenceMode` | `string` |
| `outerLeafBlockTargetBytes` | `int` |
| `outerLeafBlockCodec` | `uint8` |
| `outerLeafBlockRestart` | `int` |
| `outerLeafBlobThresholdBytes` | `int` |
| `forceValueLogPointers` | `bool` |
| `valueLogRawWritevMinAvgBytes` | `int` |
| `valueLogRawWritevMinRecords` | `int` |
| `valueLogCompressionMode` | `uint8` |
| `valueLogBlockCodec` | `valuelog.BlockCodec` |
| `valueLogBlockTargetBytes` | `int` |
| `valueLogIncompressibleHold` | `uint64` |
| `valueLogIncompressibleProbe` | `uint64` |
| `valueLogAutoPolicy` | `uint8` |
| `valueLogReader` | `*valuelog.Manager` |
| `valueLogMu` | `sync.Mutex` |
| `valueLogRetain` | `map[string]struct{}` |
| `backendReadVlogDirtySeq` | `atomic.Uint64` |
| `backendReadVlogFlushedSeq` | `atomic.Uint64` |
| `deferredFenceCandidateKeys` | `atomic.Uint64` |
| `deferredFenceEmittedGroups` | `atomic.Uint64` |
| `deferredFenceEnqueuedKeys` | `atomic.Uint64` |
| `deferredFenceEnqueuedBytes` | `atomic.Uint64` |
| `deferredFenceMaterializedKeys` | `atomic.Uint64` |
| `deferredFenceMaterializedBytes` | `atomic.Uint64` |
| `valueLogWarned` | `atomic.Bool` |
| `valueLogHardCapWarned` | `atomic.Bool` |
| `valueLogRetainedClosedBytes` | `atomic.Int64` |
| `valueLogRetainDirty` | `atomic.Bool` |
| `valueLogRetainLastPruneUnixNano` | `atomic.Int64` |
| `maxValueLogRetainedBytes` | `int64` |
| `maxValueLogRetainedBytesHard` | `int64` |
| `backend` | `BackendDB` |
| `dictStore` | `DictStore` |
| `templateStore` | `template.Store` |
| `valueLogDictTrain` | `compression.TrainConfig` |
| `valueLogDictMaxK` | `int` |
| `valueLogDictFrameEncodeLevel` | `zstd.EncoderLevel` |
| `valueLogDictFrameEnableEntropy` | `bool` |
| `valueLogDictSampleStride` | `uint64` |
| `valueLogDictSampleStrideCount` | `atomic.Uint64` |
| `valueLogDictClassifySampled` | `atomic.Uint64` |
| `valueLogDictClassifySkipped` | `atomic.Uint64` |
| `valueLogDictAdaptiveRatio` | `float64` |
| `valueLogDictMinPayloadSavings` | `float64` |
| `valueLogDictMetricsWindow` | `int` |
| `valueLogDictMetricsMinRecords` | `int` |
| `valueLogDictMetricsPauseBytes` | `int` |
| `valueLogDictTrainerMu` | `sync.Mutex` |
| `valueLogDictTrainer` | `*compression.Trainer` |
| `valueLogDictKickCh` | `chan struct{}` |
| `valueLogDictMetrics` | `compression.Metrics` |
| `valueLogDictFrames` | `struct {
	total		atomic.Uint64
	attempted	atomic.Uint64
	kept		atomic.Uint64
}` |
| `valueLogAutotuneMetrics` | `vlogAutotuneMetrics` |
| `valueLogAutotuneOptions` | `valuelog.AutotuneOptions` |
| `valueLogAutotuneCandidateKSet` | `bool` |
| `valueLogAutotuneLastProfile` | `atomic.Value` |
| `valueLogAutotuneLastSwitchFrames` | `atomic.Uint64` |
| `valueLogDictPauseRemaining` | `atomic.Uint64` |
| `valueLogDictProbeBytes` | `uint64` |
| `valueLogDictProbeRemaining` | `atomic.Uint64` |
| `valueLogDictIncompressibleHoldBytes` | `uint64` |
| `valueLogDictIncompressibleHoldRemaining` | `atomic.Uint64` |
| `valueLogDictIncompressibleProbeBytes` | `uint64` |
| `valueLogDictIncompressibleProbeRemaining` | `atomic.Uint64` |
| `valueLogDictIncompressibleHitStreak` | `atomic.Uint32` |
| `valueLogDictIncompressibleHits` | `atomic.Uint64` |
| `valueLogDictIncompressibleHolds` | `atomic.Uint64` |
| `valueLogDictIncompressibleBypassBytes` | `atomic.Uint64` |
| `valueLogDictPausedSampleStride` | `uint64` |
| `valueLogDictPausedSampleCounter` | `atomic.Uint64` |
| `valueLogDictLastAppliedDictHash` | `atomic.Uint64` |
| `valueLogDictLastAppliedDictID` | `atomic.Uint64` |
| `valueLogDictLastPublishUnixNano` | `atomic.Int64` |
| `valueLogDictLastKUpdateUnixNano` | `atomic.Int64` |
| `valueLogDictCurrentK` | `atomic.Uint32` |
| `valueLogDictKMu` | `sync.RWMutex` |
| `valueLogDictKCache` | `map[uint64]int` |
| `valueLogDictBytesMu` | `sync.Mutex` |
| `valueLogDictBytesID` | `uint64` |
| `valueLogDictBytes` | `[]byte` |
| `valueLogTemplateEnabled` | `bool` |
| `valueLogTemplateMode` | `template.Mode` |
| `valueLogTemplateEngine` | `*template.Engine` |
| `valueLogTemplateReadStrict` | `bool` |
| `valueLogTemplateDecodeOpts` | `template.DecodeOptions` |
| `dictCurrentCached` | `atomic.Uint64` |
| `dictCurrentOps` | `atomic.Uint64` |
| `dir` | `string` |
| `flushThreshold` | `int64` |
| `memtableCap` | `int` |
| `memtableMode` | `memtable.Mode` |
| `memtableStats` | `memtableStats` |
| `memtableAdaptive` | `bool` |
| `memtableAdaptiveObserve` | `atomic.Bool` |
| `adaptiveShardedStats` | `bool` |
| `memtableWarmupActive` | `bool` |
| `memtableWarmupThreshold` | `int64` |
| `domainIngressWorkers` | `int` |
| `domainIngressQueueSize` | `int` |
| `maxQueuedMemtables` | `int` |
| `slowdownBacklogSeconds` | `float64` |
| `stopBacklogSeconds` | `float64` |
| `maxBacklogBytes` | `int64` |
| `writerFlushMaxMemtables` | `int` |
| `writerFlushMaxDuration` | `time.Duration` |
| `flushBuildConcurrency` | `int` |
| `flushBuildAutoConcurrency` | `bool` |
| `flushBuildMinEntries` | `int` |
| `flushBuildMinUnits` | `int` |
| `flushBuildChunkCap` | `int` |
| `flushBuildChunkTarget` | `int` |
| `flushBuildChunkMinBytes` | `int` |
| `flushBuildChunkMaxBytes` | `int` |
| `flushBuildPrefetchUnits` | `int` |
| `flushBackendMaxEntries` | `int` |
| `flushBackendInitEntries` | `int` |
| `flushBackendMaxBatches` | `int` |
| `walMaxSegmentBytes` | `int64` |
| `valueLogMaxSegmentBytes` | `int64` |
| `journalCompression` | `bool` |
| `disableJournal` | `bool` |
| `relaxedSync` | `bool` |
| `notifyError` | `func(error)` |
| `debugFlushPointers` | `bool` |
| `debugFlushTiming` | `bool` |
| `debugV1LeafLogInvariants` | `bool` |
| `debugV1LeafLogInvariantsPanic` | `bool` |
| `debugPtrEligible` | `atomic.Int64` |
| `debugPtrUsed` | `atomic.Int64` |
| `debugPtrNoPtr` | `atomic.Int64` |
| `debugPtrDenied` | `atomic.Int64` |
| `debugPtrDisabled` | `atomic.Int64` |
| `routeLeaflogFallbackDirectAttempts` | `atomic.Uint64` |
| `routeLeaflogFallbackDirectRewrites` | `atomic.Uint64` |
| `bgErrMu` | `sync.Mutex` |
| `bgErr` | `error` |
| `queueBacklogBytes` | `atomic.Int64` |
| `flushBpsEWMA` | `float64` |
| `queueLaneIDMisses` | `atomic.Int64` |
| `backendWriteBatchesTotal` | `atomic.Int64` |
| `deferredFenceAssistCalls` | `atomic.Uint64` |
| `deferredFenceAssistFlushedMemtables` | `atomic.Uint64` |
| `deferredFenceAssistEarlyTriggers` | `atomic.Uint64` |
| `deferredFenceAssistTicker` | `atomic.Uint64` |
| `deferredFenceCheckpointPoolSkips` | `atomic.Uint64` |
| `domainIngressMu` | `sync.Mutex` |
| `domainIngressCh` | `[]chan domainIngressRequest` |
| `domainIngressEnqueued` | `atomic.Uint64` |
| `domainIngressProcessed` | `atomic.Uint64` |
| `domainIngressFallback` | `atomic.Uint64` |
| `domainIngressDepthMax` | `atomic.Uint64` |
| `closeCh` | `chan struct{}` |
| `closing` | `atomic.Bool` |
| `flushCh` | `chan struct{}` |
| `wg` | `sync.WaitGroup` |
| `autoCheckpointOnceCh` | `chan struct{}` |
| `autoCheckpointWriteCh` | `chan struct{}` |
| `autoCheckpointOn` | `atomic.Bool` |
| `autoCheckpointSizeArmed` | `atomic.Bool` |
| `autoCheckpointCount` | `atomic.Uint64` |
| `autoCheckpointLastReason` | `atomic.Uint32` |
| `autoCheckpointLastUnixNano` | `atomic.Int64` |
| `autoCheckpointLastDurNanos` | `atomic.Int64` |
| `autoCheckpointLastWALBefore` | `atomic.Int64` |
| `autoCheckpointLastWALAfter` | `atomic.Int64` |
| `autoCheckpointLastWALReclaimableBefore` | `atomic.Int64` |
| `autoCheckpointLastWALReclaimableAfter` | `atomic.Int64` |
| `autoCheckpointLastWALTrimmed` | `atomic.Int64` |
| `autoCheckpointLastWALBytes` | `atomic.Int64` |
| `autoCheckpointMaxWALBytes` | `atomic.Int64` |
| `checkpointCutoverLastNanos` | `atomic.Int64` |
| `checkpointCutoverMaxNanos` | `atomic.Int64` |
| `checkpointCutoverTotalNanos` | `atomic.Int64` |
| `checkpointCutoverSamples` | `atomic.Uint64` |
| `checkpointCutoverLastUnixNano` | `atomic.Int64` |
| `materializationLastDrainUnixNano` | `atomic.Int64` |
| `publishWatermarkLagMu` | `sync.Mutex` |
| `publishWatermarkLastBacklogBytes` | `int64` |
| `publishWatermarkLastUnixNano` | `int64` |
| `testOnVlogFlush` | `func(laneID int)` |
| `testOnVlogSync` | `func(laneID int)` |
| `testBeforeVlogUnlock` | `func(laneID int)` |

## keyRange (8310-8314)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `valid` | `bool` |
| `min` | `[]byte` |
| `max` | `[]byte` |

## memShard (8316-8322)

- kind: struct
- fields: 5

| Field | Type |
|---|---|
| `mu` | `sync.Mutex` |
| `mem` | `memtable.Table` |
| `rng` | `keyRange` |
| `bytes` | `int64` |
| `stats` | `memtableStats` |

## memtableView (8326-8334)

- kind: struct
- fields: 7

| Field | Type |
|---|---|
| `mutables` | `[]memtable.Table` |
| `queue` | `[]memtable.Table` |
| `queueShardIDs` | `[]uint16` |
| `queueRanges` | `[]keyRange` |
| `refs` | `atomic.Int64` |
| `retiredMems` | `[]memtable.Table` |
| `deferredRetiredMemtables` | `atomic.Int64` |

## snapshotMemtableReader (8336-8339)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `db` | `*DB` |
| `view` | `*memtableView` |

## memtableStats (8761-8770)

- kind: struct
- fields: 8

| Field | Type |
|---|---|
| `writes` | `atomic.Uint64` |
| `seqWrites` | `atomic.Uint64` |
| `overwriteWrites` | `atomic.Uint64` |
| `iterators` | `atomic.Uint64` |
| `rangeIters` | `atomic.Uint64` |
| `lastKeyMu` | `sync.Mutex` |
| `lastKey` | `[]byte` |
| `hasLastKey` | `bool` |

## flushBuildJob (9777-9783)

- kind: struct
- fields: 5

| Field | Type |
|---|---|
| `mem` | `memtable.Table` |
| `out` | `chan<- []batch.Entry` |
| `cancel` | `<-chan struct{}` |
| `chunkCap` | `int` |
| `errCh` | `chan<- error` |

## autoCheckpointMode (9999-9999)

- kind: alias/basic
- type: `uint8`

## domainIngressOp (10120-10120)

- kind: alias/basic
- type: `uint8`

## domainIngressRequest (10127-10133)

- kind: struct
- fields: 5

| Field | Type |
|---|---|
| `op` | `domainIngressOp` |
| `key` | `[]byte` |
| `value` | `[]byte` |
| `sync` | `bool` |
| `done` | `chan error` |

## walWriteRequest (10135-10139)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `records` | `[]logRecord` |
| `sync` | `bool` |
| `ack` | `*walAck` |

## walAck (10141-10144)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `wg` | `sync.WaitGroup` |
| `err` | `error` |

## vlogWriteRequest (10150-10160)

- kind: struct
- fields: 9

| Field | Type |
|---|---|
| `rid` | `uint64` |
| `value` | `[]byte` |
| `dictID` | `uint64` |
| `writeMode` | `vlogCompressionWriteMode` |
| `blockCodec` | `valuelog.BlockCodec` |
| `probeCompression` | `bool` |
| `durability` | `journalDurability` |
| `enqueuedAt` | `time.Time` |
| `ack` | `*vlogAck` |

## vlogAck (10162-10167)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `wg` | `sync.WaitGroup` |
| `ptr` | `page.ValuePtr` |
| `retainPath` | `string` |
| `err` | `error` |

## vlogDictPrepareTask (10173-10185)

- kind: struct
- fields: 11

| Field | Type |
|---|---|
| `fi` | `int` |
| `dictID` | `uint64` |
| `dict` | `[]byte` |
| `records` | `[]valuelog.Record` |
| `level` | `zstd.EncoderLevel` |
| `enableEntropy` | `bool` |
| `ioNsPerStored` | `float64` |
| `encodeNsPerRaw` | `float64` |
| `safetyMargin` | `float64` |
| `measureEncode` | `bool` |
| `out` | `chan<- vlogDictPrepareResult` |

## vlogDictPrepareResult (10187-10193)

- kind: struct
- fields: 5

| Field | Type |
|---|---|
| `fi` | `int` |
| `body` | `[]byte` |
| `bodyBuf` | `*vlogPreparedFrameBody` |
| `stats` | `valuelog.FrameStats` |
| `err` | `error` |

## opRunIter (10496-10501)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `runs` | `[][]batch.Entry` |
| `runIdx` | `int` |
| `idx` | `int` |
| `valid` | `bool` |

## opMergeItem (10547-10551)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `iter` | `*opRunIter` |
| `priority` | `int` |
| `key` | `[]byte` |

## opMergeHeap (10553-10553)

- kind: alias/basic
- type: `[]opMergeItem`

## opUnsafeMergeItem (10625-10630)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `iter` | `iterator.UnsafeIterator` |
| `priority` | `int` |
| `key` | `[]byte` |
| `stable` | `bool` |

## opUnsafeMergeHeap (10632-10632)

- kind: alias/basic
- type: `[]opUnsafeMergeItem`

## walFastItem (10766-10769)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `record` | `logRecord` |
| `ack` | `*walAck` |

## journalDurability (12204-12204)

- kind: alias/basic
- type: `uint8`

## preparedDictFrame (12285-12291)

- kind: struct
- fields: 5

| Field | Type |
|---|---|
| `start` | `int` |
| `end` | `int` |
| `body` | `[]byte` |
| `bodyBuf` | `*vlogPreparedFrameBody` |
| `stats` | `valuelog.FrameStats` |

## flushUnit (16366-16374)

- kind: struct
- fields: 7

| Field | Type |
|---|---|
| `mem` | `memtable.Table` |
| `memBytes` | `int64` |
| `memLen` | `int` |
| `memRange` | `keyRange` |
| `walPaths` | `[]string` |
| `id` | `uint64` |
| `laneID` | `int` |

## backendManyGetter (18149-18151)

- kind: interface
- methods: 1

| Method/Embed | Signature |
|---|---|
| `GetMany` | `func(keys [][]byte) ([][]byte, error)` |

## backendManyPlanner (18153-18155)

- kind: interface
- methods: 1

| Method/Embed | Signature |
|---|---|
| `GetManyParallelPlan` | `func(keyCount int) (workers int, parallel bool)` |

## debugIterator (19304-19308)

- kind: struct
- fields: 3

| Field | Type |
|---|---|
| `(embedded)` | `merging.Iterator` |
| `queueLen` | `int` |
| `sourcesUsed` | `int` |

## leasedMergingIterator (19314-19319)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `(embedded)` | `merging.Iterator` |
| `closeOnce` | `sync.Once` |
| `closeErr` | `error` |
| `release` | `func()` |

## concatUnsafeIterator (19331-19339)

- kind: struct
- fields: 6

| Field | Type |
|---|---|
| `first` | `iterator.UnsafeIterator` |
| `second` | `iterator.UnsafeIterator` |
| `cur` | `iterator.UnsafeIterator` |
| `usingFirst` | `bool` |
| `valid` | `bool` |
| `err` | `error` |

## reverseRangeFilterIterator (19448-19453)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `base` | `iterator.UnsafeIterator` |
| `start` | `[]byte` |
| `end` | `[]byte` |
| `valid` | `bool` |

## singletonReverseEntryIterator (19546-19555)

- kind: struct
- fields: 8

| Field | Type |
|---|---|
| `start` | `[]byte` |
| `end` | `[]byte` |
| `key` | `[]byte` |
| `val` | `[]byte` |
| `ptr` | `page.ValuePtr` |
| `flags` | `byte` |
| `valid` | `bool` |
| `err` | `error` |

## Batch (19790-19818)

- kind: struct
- fields: 26

| Field | Type |
|---|---|
| `db` | `*DB` |
| `entries` | `[]batch.Entry` |
| `backend` | `batch.Interface` |
| `size` | `int` |
| `copyArena` | `[]byte` |
| `copyArenaChunks` | `[][]byte` |
| `copyArenaCap` | `int` |
| `copyBytes` | `int` |
| `walBuf` | `[]logRecord` |
| `shardIdxs` | `[]int` |
| `eligibleIdxs` | `[]int` |
| `shardAdds` | `[]int64` |
| `shardCnts` | `[]int` |
| `shardEntries` | `[][]batch.Entry` |
| `shardIdxSets` | `[][]int` |
| `maxEntries` | `int` |
| `closed` | `bool` |
| `streamEligible` | `bool` |
| `streamTried` | `bool` |
| `firstKey` | `[]byte` |
| `lastKey` | `[]byte` |
| `batchRange` | `keyRange` |
| `dictID` | `uint64` |
| `dictIDValid` | `bool` |
| `dictBytes` | `[]byte` |
| `dictBytesValid` | `bool` |

## logSegmentInfo (21581-21587)

- kind: struct
- fields: 5

| Field | Type |
|---|---|
| `path` | `string` |
| `size` | `int64` |
| `seq` | `int` |
| `lane` | `int` |
| `valueLog` | `bool` |

## singleSourceIterator (21960-21965)

- kind: struct
- fields: 4

| Field | Type |
|---|---|
| `iter` | `iterator.UnsafeIterator` |
| `valid` | `bool` |
| `start` | `[]byte` |
| `end` | `[]byte` |

## emptyIterator (22027-22029)

- kind: struct
- fields: 2

| Field | Type |
|---|---|
| `start` | `[]byte` |
| `end` | `[]byte` |

