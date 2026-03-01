# Outline: TreeDB/caching/db.go

- Total declarations: 777

| Start | End | Kind | Name | Signature/Type |
|---:|---:|---|---|---|
| 41 | 41 | var | `errDBClosing` | `` |
| 43 | 43 | var | `ErrKeyEmpty` | `` |
| 44 | 44 | var | `ErrValueNil` | `` |
| 45 | 45 | var | `ErrBatchClosed` | `` |
| 46 | 46 | var | `ErrUnsafeOptions` | `` |
| 47 | 47 | var | `ErrMemtableFull` | `` |
| 48 | 48 | var | `errWALClosed` | `` |
| 49 | 49 | var | `errWALUnavailable` | `` |
| 51 | 51 | var | `iteratorDebugEnabled` | `atomic.Bool` |
| 52 | 52 | var | `visibilityDebugOnce` | `sync.Once` |
| 53 | 53 | var | `visibilityDebugEnabled` | `atomic.Bool` |
| 54 | 54 | var | `visibilityDebugMu` | `sync.Mutex` |
| 55 | 55 | var | `visibilityDebugLogFile` | `*os.File` |
| 56 | 56 | var | `getMissDebugOnce` | `sync.Once` |
| 57 | 57 | var | `getMissDebugEnabled` | `atomic.Bool` |
| 58 | 58 | var | `getMissDebugPrefix` | `[]byte` |
| 59 | 59 | var | `getMissDebugLimit` | `int64` |
| 60 | 60 | var | `getMissDebugCount` | `atomic.Int64` |
| 61 | 61 | var | `keyTraceOnce` | `sync.Once` |
| 62 | 62 | var | `keyTraceEnabled` | `atomic.Bool` |
| 63 | 63 | var | `keyTracePrefix` | `[]byte` |
| 64 | 64 | var | `keyTraceMatchKey` | `[]byte` |
| 65 | 65 | var | `keyTraceLimit` | `int64` |
| 66 | 66 | var | `keyTraceCount` | `atomic.Int64` |
| 67 | 67 | var | `writeSyncDumpOnce` | `sync.Once` |
| 68 | 68 | var | `writeSyncDumpPath` | `string` |
| 69 | 69 | var | `writeSyncDumpValuesOnce` | `sync.Once` |
| 70 | 70 | var | `writeSyncDumpValues` | `bool` |
| 71 | 71 | var | `writeSyncDumpPrefixOnce` | `sync.Once` |
| 72 | 72 | var | `writeSyncDumpPrefix` | `[]byte` |
| 73 | 73 | var | `writeSyncDumpMatchKeyOnce` | `sync.Once` |
| 74 | 74 | var | `writeSyncDumpMatchKey` | `[]byte` |
| 75 | 75 | var | `writeSyncDumpMatchWindowOnce` | `sync.Once` |
| 76 | 76 | var | `writeSyncDumpMatchWindow` | `int64` |
| 77 | 77 | var | `writeSyncDumpMatchWindowRemaining` | `atomic.Int64` |
| 79 | 79 | var | `valueLogEligiblePool` | `sync.Pool` |
| 80 | 80 | var | `valueLogRecordPool` | `sync.Pool` |
| 81 | 81 | var | `valueLogKeyPool` | `sync.Pool` |
| 82 | 82 | var | `valueLogPtrPool` | `sync.Pool` |
| 83 | 83 | var | `outerLeafEntryPool` | `sync.Pool` |
| 84 | 84 | var | `batchArenaPools` | `[batchArenaClassCount]sync.Pool` |
| 85 | 85 | var | `batchArenaLeasePool` | `sync.Pool` |
| 86 | 86 | var | `batchEntrySliceRefPool` | `sync.Pool` |
| 87 | 87 | var | `outerLeafArenaPools` | `[outerLeafArenaClassCount]sync.Pool` |
| 88 | 88 | var | `outerLeafArenaLeaseMu` | `sync.Mutex` |
| 89 | 89 | var | `outerLeafArenaLeases` | `[outerLeafArenaClassCount][][]byte` |
| 90 | 90 | var | `outerLeafBlobRefScratchPool` | `sync.Pool` |
| 91 | 91 | var | `outerLeafEncoderPool` | `sync.Pool` |
| 92 | 92 | var | `valueLogPreparedBodyPool` | `sync.Pool` |
| 93 | 93 | var | `valueLogPreparedFramesPool` | `sync.Pool` |
| 94 | 94 | var | `valueLogDictPrepareResultsPool` | `sync.Pool` |
| 95 | 95 | var | `valueLogKeyLeaseMu` | `sync.Mutex` |
| 96 | 96 | var | `valueLogKeyLeases` | `[][][]byte` |
| 99 | 99 | const | `maxValueLogKeyLeaseCount` | `` |
| 100 | 100 | const | `maxValueLogKeyLeaseCap` | `` |
| 101 | 101 | const | `batchArenaMinShift` | `` |
| 102 | 102 | const | `batchArenaMaxShift` | `` |
| 103 | 103 | const | `batchArenaClassCount` | `` |
| 104 | 104 | const | `outerLeafArenaMinShift` | `` |
| 105 | 105 | const | `outerLeafArenaMaxShift` | `` |
| 106 | 106 | const | `outerLeafArenaClassCount` | `` |
| 107 | 107 | const | `maxOuterLeafArenaLeases` | `` |
| 110 | 129 | func | `visibilityDebugOn` | `() (bool)` |
| 131 | 143 | func | `visibilityDebugf` | `(format string, args ...any) ()` |
| 145 | 173 | func | `getMissDebugOn` | `() (bool)` |
| 175 | 192 | func | `logGetMissDebug` | `(key []byte, source string, err error, queueLen int, mutableBytes int64, checkpointing bool) ()` |
| 194 | 235 | func | `keyTraceOn` | `() (bool)` |
| 237 | 260 | func | `logKeyTrace` | `(op string, key []byte, valueLen int, extra string) ()` |
| 262 | 267 | func | `writeSyncDumpLogPath` | `() (string)` |
| 269 | 278 | func | `writeSyncDumpIncludeValues` | `() (bool)` |
| 280 | 298 | func | `writeSyncDumpFilterPrefix` | `() ([]byte)` |
| 300 | 318 | func | `writeSyncDumpFilterMatchKey` | `() ([]byte)` |
| 320 | 337 | func | `writeSyncDumpMatchWindowBatches` | `() (int64)` |
| 339 | 417 | func | `writeSyncDumpEntries` | `(entries []batch.Entry) ()` |
| 419 | 435 | func | `looksLikeIAVLRootNodeKey` | `(key []byte) ((version uint64, ok bool))` |
| 437 | 446 | func | `looksLikeIAVLNodeVersionRange` | `(start, end []byte) (bool)` |
| 448 | 459 | func | `iavlNodeVersionFromNodeKey` | `(key []byte) ((version uint64, ok bool))` |
| 461 | 467 | func | `routeKeyRequiresSingletonOuterLeaf` | `(key []byte) (bool)` |
| 469 | 473 | type | `routePersistedGapScanner` | `struct{3 fields}` |
| 475 | 503 | func | `newRoutePersistedGapScanner` | `(backend BackendDB, start []byte) ((*routePersistedGapScanner, error))` |
| 505 | 516 | func | `(s *routePersistedGapScanner).refresh` | `() (error)` |
| 518 | 524 | func | `(s *routePersistedGapScanner).advance` | `() (error)` |
| 526 | 542 | func | `(s *routePersistedGapScanner).hasPersistedBetween` | `(lower, upper []byte) ((bool, error))` |
| 544 | 552 | func | `(s *routePersistedGapScanner).close` | `() ()` |
| 554 | 556 | type | `vlogPreparedFrameBody` | `struct{1 fields}` |
| 558 | 560 | type | `batchEntrySliceRef` | `struct{1 fields}` |
| 562 | 570 | func | `getBatchEntrySliceRef` | `(entries []batch.Entry) (*batchEntrySliceRef)` |
| 572 | 578 | func | `putBatchEntrySliceRef` | `(ref *batchEntrySliceRef) ()` |
| 580 | 592 | func | `getValueLogEligible` | `(capacity int) ([]int)` |
| 594 | 603 | func | `putValueLogEligible` | `(s []int) ()` |
| 605 | 617 | func | `getValueLogRecords` | `(n int) ([]valuelog.Record)` |
| 619 | 631 | func | `getValueLogRecordsCap` | `(capacity int) ([]valuelog.Record)` |
| 633 | 645 | func | `putValueLogRecords` | `(s []valuelog.Record) ()` |
| 647 | 653 | func | `clearValueLogRecordValues` | `(s []valuelog.Record) ()` |
| 655 | 670 | func | `putValueLogRecordsNoClear` | `(s []valuelog.Record) ()` |
| 672 | 683 | func | `putValueLogRecordsCheckpointAware` | `(db *DB, s []valuelog.Record) ()` |
| 685 | 695 | func | `getOuterLeafEntriesCap` | `(capacity int) ([]outerleaf.Entry)` |
| 697 | 712 | func | `putOuterLeafEntries` | `(s []outerleaf.Entry) ()` |
| 714 | 738 | func | `outerLeafArenaClassForLen` | `(capacity int) ((idx int, classCap int, ok bool))` |
| 740 | 754 | func | `outerLeafArenaClassForCap` | `(capacity int) ((idx int, ok bool))` |
| 756 | 772 | func | `outerLeafArenaMaxReuseCap` | `(capacity int) (int)` |
| 774 | 798 | func | `batchArenaClassForLen` | `(capacity int) ((idx int, classCap int, ok bool))` |
| 800 | 814 | func | `batchArenaClassForCap` | `(capacity int) ((idx int, ok bool))` |
| 816 | 832 | func | `getBatchArena` | `(capacity int) ([]byte)` |
| 834 | 846 | func | `putBatchArena` | `(buf []byte) ()` |
| 848 | 855 | func | `putBatchArenas` | `(chunks [][]byte) ()` |
| 857 | 897 | func | `getOuterLeafArena` | `(capacity int) ([]byte)` |
| 899 | 919 | func | `putOuterLeafArena` | `(buf []byte) ()` |
| 921 | 928 | func | `getOuterLeafBlobRefScratch` | `() (*[outerLeafBlobRefStackScratchCap]byte)` |
| 930 | 935 | func | `putOuterLeafBlobRefScratch` | `(buf *[outerLeafBlobRefStackScratchCap]byte) ()` |
| 937 | 944 | func | `getOuterLeafEncoder` | `() (*outerleaf.Encoder)` |
| 946 | 952 | func | `putOuterLeafEncoder` | `(e *outerleaf.Encoder) ()` |
| 954 | 966 | func | `getValueLogPtrs` | `(n int) ([]page.ValuePtr)` |
| 968 | 984 | func | `getValueLogPtrsCap` | `(capacity int) ([]page.ValuePtr)` |
| 986 | 996 | func | `putValueLogPtrs` | `(s []page.ValuePtr) ()` |
| 998 | 1008 | func | `putValueLogPtrsNoClear` | `(s []page.ValuePtr) ()` |
| 1010 | 1036 | func | `getValueLogKeys` | `(capacity int) ([][]byte)` |
| 1038 | 1055 | func | `putValueLogKeys` | `(s [][]byte) ()` |
| 1057 | 1064 | func | `getVlogPreparedFrameBody` | `() (*vlogPreparedFrameBody)` |
| 1066 | 1076 | func | `putVlogPreparedFrameBody` | `(body *vlogPreparedFrameBody) ()` |
| 1078 | 1090 | func | `getVlogPreparedFrames` | `(n int) ([]preparedDictFrame)` |
| 1092 | 1101 | func | `putVlogPreparedFrames` | `(frames []preparedDictFrame) ()` |
| 1103 | 1119 | func | `getVlogDictPrepareResults` | `(capacity int) (chan vlogDictPrepareResult)` |
| 1121 | 1136 | func | `putVlogDictPrepareResults` | `(ch chan vlogDictPrepareResult) ()` |
| 1139 | 1139 | const | `envDebugFlushPointers` | `` |
| 1140 | 1140 | const | `envDebugFlushTiming` | `` |
| 1143 | 1143 | const | `envDebugV1LeafLogInvariants` | `` |
| 1146 | 1146 | const | `envDebugV1LeafLogInvariantsPanic` | `` |
| 1148 | 1148 | const | `minMemtablePrealloc` | `` |
| 1149 | 1149 | const | `maxMemtablePrealloc` | `` |
| 1150 | 1150 | const | `adaptiveMinWrites` | `` |
| 1151 | 1151 | const | `adaptiveSequentialWritePct` | `` |
| 1152 | 1152 | const | `adaptiveRangeIteratorPct` | `` |
| 1153 | 1153 | const | `adaptiveOverwriteWritePct` | `` |
| 1154 | 1154 | const | `adaptiveWarmupBytes` | `` |
| 1155 | 1155 | const | `maxMemtableBytesPerShard` | `` |
| 1156 | 1156 | const | `maxOuterLeafArenaPoolCap` | `` |
| 1157 | 1157 | const | `outerLeafBlobRefStackScratchCap` | `` |
| 1158 | 1158 | const | `maxOuterLeafEncoderRawScratchCap` | `` |
| 1159 | 1159 | const | `maxOuterLeafEncoderEncScratchCap` | `` |
| 1160 | 1160 | const | `maxOuterLeafEncoderRestartsCap` | `` |
| 1161 | 1161 | const | `maxVlogPreparedBodyPoolCap` | `` |
| 1162 | 1162 | const | `maxVlogPreparedFramesPoolCap` | `` |
| 1163 | 1163 | const | `maxVlogDictPrepareResultsPoolCap` | `` |
| 1168 | 1170 | func | `SetIteratorDebug` | `(enabled bool) ()` |
| 1172 | 1191 | func | `envBool` | `(name string) (bool)` |
| 1193 | 1206 | func | `(db *DB).maybeValidateV1LeafLogInvariants` | `() (error)` |
| 1208 | 1317 | func | `(db *DB).validateV1LeafLogDirectoryInvariants` | `() (error)` |
| 1319 | 1334 | func | `(db *DB).syncDirBestEffort` | `(dir string) ()` |
| 1336 | 1350 | func | `(db *DB).removeFileRetry` | `(path string) (error)` |
| 1352 | 1382 | func | `warnInsecureDir` | `(dir string, notify func(error)) ()` |
| 1384 | 1400 | func | `memtableCapacity` | `(flushThreshold int64) (int)` |
| 1402 | 1412 | func | `normalizeShardCount` | `(n int) (int)` |
| 1414 | 1424 | func | `defaultMemtableShards` | `() (int)` |
| 1426 | 1438 | func | `shardCapacity` | `(totalCap, shards int) (int)` |
| 1440 | 1442 | func | `(db *DB).valueLogEnabled` | `() (bool)` |
| 1444 | 1446 | func | `(db *DB).splitValueLogEnabled` | `() (bool)` |
| 1448 | 1453 | func | `(db *DB).valueLogThresholdForKey` | `(key []byte) (int)` |
| 1455 | 1467 | func | `(db *DB).shouldWriteViaValueLogForKeyValue` | `(key, value []byte) (bool)` |
| 1473 | 1475 | func | `(db *DB).outerLeafFenceV2Enabled` | `() (bool)` |
| 1477 | 1479 | func | `(db *DB).outerLeafRouteModeEnabled` | `() (bool)` |
| 1481 | 1491 | func | `(db *DB).outerLeafAnchorModeEnabled` | `() (bool)` |
| 1493 | 1506 | func | `(db *DB).mutableHasEntriesLocked` | `() (bool)` |
| 1508 | 1523 | func | `(db *DB).normalizeRouteAnchorPointer` | `(ptr page.ValuePtr, context string) ((page.ValuePtr, error))` |
| 1525 | 1566 | func | `(db *DB).validateRouteAnchorPointers` | `(context string) (error)` |
| 1568 | 1577 | func | `normalizeValueLogWALFenceMode` | `(mode string) (string)` |
| 1579 | 1579 | const | `defaultOuterLeafBlobThresholdMin` | `` |
| 1580 | 1580 | const | `defaultOuterLeafFenceBlockTargetBytes` | `` |
| 1581 | 1581 | const | `outerLeafFenceAdaptiveLZ4MaxAvgValueBytes` | `` |
| 1582 | 1582 | const | `defaultLargeValueChunkBytes` | `` |
| 1583 | 1583 | const | `minLargeValueChunkBytes` | `` |
| 1585 | 1601 | func | `valueLogSingleRecordPayloadCap` | `() (int)` |
| 1603 | 1608 | func | `(db *DB).largeValueNeedsChunking` | `(valueLen int) (bool)` |
| 1610 | 1623 | func | `(db *DB).largeValueChunkBytes` | `() (int)` |
| 1625 | 1633 | func | `(db *DB).effectiveOuterLeafBlobThresholdBytes` | `() (int)` |
| 1635 | 1640 | func | `(db *DB).oversizedOuterLeafValue` | `(value []byte) (bool)` |
| 1642 | 1647 | func | `(db *DB).fenceRIDJoinHybridEnabled` | `() (bool)` |
| 1649 | 1662 | func | `(db *DB).selectOuterLeafBlockCodec` | `(totalValueBytes, entryCount int) (uint8)` |
| 1664 | 1670 | func | `(db *DB).encodeOuterLeafValue` | `(key, value []byte) (([]byte, error))` |
| 1672 | 1684 | func | `(db *DB).encodeOuterLeafBlobRef` | `(dst, key []byte, ptr page.ValuePtr) (([]byte, error))` |
| 1686 | 1736 | func | `(db *DB).decodeOuterLeafValue` | `(key, value []byte) (([]byte, error))` |
| 1738 | 1748 | func | `allOuterLeafRecordValues` | `(records []valuelog.Record) (bool)` |
| 1750 | 1753 | type | `outerLeafRecordGroup` | `struct{2 fields}` |
| 1755 | 1755 | const | `outerLeafEncodedHeaderBytes` | `` |
| 1757 | 1769 | func | `ensureOuterLeafArenaCap` | `(buf []byte, need int) ([]byte)` |
| 1771 | 1804 | func | `appendOuterLeafRecordGroup` | `(db *DB, encoder *outerleaf.Encoder, entries []outerleaf.Entry, groupStart int, records []valuelog.Record, groups []outerLeafRecordGroup, arena []byte, maxEncodedHint int) (([]valuelog.Record, []outerLeafRecordGroup, []byte, error))` |
| 1806 | 1841 | func | `appendOuterLeafTypedRecordGroup` | `(db *DB, encoder *outerleaf.Encoder, entries []outerleaf.TypedEntry, groupStart int, records []valuelog.Record, groups []outerLeafRecordGroup, arena []byte, maxEncodedHint int) (([]valuelog.Record, []outerLeafRecordGroup, []byte, error))` |
| 1843 | 1910 | func | `(db *DB).appendLargeValueManifest` | `(lane *lane, dictID uint64, value []byte, durability journalDurability) ((page.ValuePtr, error))` |
| 1912 | 2184 | func | `(db *DB).writeRouteOuterLeafValueRecords` | `(lane *lane, dictID uint64, keys [][]byte, values [][]byte, durability journalDurability) (([]page.ValuePtr, []outerLeafRecordGroup, []int, error))` |
| 2189 | 2322 | func | `(db *DB).buildOuterLeafValueRecords` | `(keys [][]byte, values [][]byte) (([]valuelog.Record, []outerLeafRecordGroup, []byte, error))` |
| 2324 | 2329 | func | `fallbackAutoVlogWriteMode` | `(mode vlogCompressionMode, writeMode vlogCompressionWriteMode) (vlogCompressionWriteMode)` |
| 2331 | 2342 | func | `lookupVlogDictBytes` | `(dictID uint64, singleDictID uint64, singleDict []byte, dictByID map[uint64][]byte) ([]byte)` |
| 2344 | 2360 | func | `(db *DB).deferredValueLogEnabled` | `() (bool)` |
| 2362 | 2366 | func | `(db *DB).deferredValueLogNeedsSingleLaneRegroup` | `() (bool)` |
| 2368 | 2372 | func | `(db *DB).allowFencePointerCollapse` | `() (bool)` |
| 2374 | 2378 | func | `(db *DB).allowMutableFencePointerCollapse` | `() (bool)` |
| 2380 | 2389 | func | `(db *DB).supportsFenceAnchorPromotion` | `() (bool)` |
| 2391 | 2423 | func | `backendSupportsRouteMode` | `(backend BackendDB) (bool)` |
| 2427 | 2441 | func | `sumKeyValueBytes` | `(keys, values [][]byte) (uint64)` |
| 2443 | 2451 | func | `(db *DB).recordDeferredFenceEnqueued` | `(keys int, bytes uint64) ()` |
| 2453 | 2463 | func | `(db *DB).recordDeferredFenceMaterialized` | `(keys int, bytes uint64) ()` |
| 2465 | 2483 | func | `(db *DB).shouldFlushDeferredValueLog` | `(writeMode vlogCompressionWriteMode, records []valuelog.Record) (bool)` |
| 2485 | 2498 | func | `(db *DB).shouldFlushDeferredValueLogValue` | `(writeMode vlogCompressionWriteMode, value []byte) (bool)` |
| 2500 | 2502 | func | `(db *DB).walUsesValueLog` | `() (bool)` |
| 2504 | 2512 | func | `(db *DB).needsVlogAutotuneTiming` | `() (bool)` |
| 2514 | 2563 | func | `(db *DB).pickLane` | `(sync bool, preferred int) ((*lane, error))` |
| 2565 | 2575 | func | `(db *DB).releaseLaneSync` | `(l *lane) ()` |
| 2577 | 2591 | func | `(db *DB).currentValueLogPath` | `(l *lane) (string)` |
| 2593 | 2607 | func | `(db *DB).currentValueLogSeq` | `(l *lane) (int)` |
| 2609 | 2623 | func | `(db *DB).currentWALPaths` | `() ([]string)` |
| 2625 | 2647 | func | `(db *DB).currentValueLogPaths` | `() ([]string)` |
| 2649 | 2841 | func | `(db *DB).deferValueLogOps` | `(ops []batch.Entry, sync bool) (([]batch.Entry, error))` |
| 2843 | 2847 | type | `deferredFenceCollapseState` | `struct{3 fields}` |
| 2849 | 2849 | type | `fencePendingMutationKind` | `uint8` |
| 2852 | 2852 | const | `fencePendingMutationNone` | `fencePendingMutationKind` |
| 2853 | 2853 | const | `fencePendingMutationDelete` | `` |
| 2854 | 2854 | const | `fencePendingMutationSetInline` | `` |
| 2855 | 2855 | const | `fencePendingMutationSetPointer` | `` |
| 2858 | 2861 | type | `fencePendingMutation` | `struct{2 fields}` |
| 2863 | 2879 | func | `valuePtrSameRecord` | `(a, b page.ValuePtr) (bool)` |
| 2881 | 2893 | func | `(m fencePendingMutation).replacesFenceAnchor` | `(oldPtr page.ValuePtr) (bool)` |
| 2895 | 2897 | func | `(m fencePendingMutation).keepsFencePointer` | `(oldPtr page.ValuePtr) (bool)` |
| 2899 | 2910 | func | `fencePendingMutationFromMemEntry` | `(ptr page.ValuePtr, flags byte, found bool) ((fencePendingMutation, bool))` |
| 2912 | 2912 | type | `fenceMutationLookupFn` | `func(key []byte) (fencePendingMutation, bool)` |
| 2914 | 2922 | func | `fenceMutationLookupForMem` | `(mem memtable.Table) (fenceMutationLookupFn)` |
| 2924 | 3016 | func | `fenceMutationLookupForUnits` | `(units []flushUnit) (fenceMutationLookupFn)` |
| 3018 | 3057 | type | `fenceAnchorPromoter` | `struct{28 fields}` |
| 3059 | 3062 | type | `routeDeleteDecisionEntry` | `struct{2 fields}` |
| 3064 | 3069 | type | `overlapRewritePending` | `struct{4 fields}` |
| 3071 | 3075 | type | `fenceSourcePendingSet` | `struct{3 fields}` |
| 3077 | 3087 | type | `fenceSourceRewritePlan` | `struct{6 fields}` |
| 3089 | 3089 | const | `fenceRewritePlanIndexThreshold` | `` |
| 3091 | 3095 | type | `fenceSourceRewriteAlias` | `struct{3 fields}` |
| 3097 | 3105 | func | `newFenceAnchorPromoter` | `(db *DB, lookup fenceMutationLookupFn) (*fenceAnchorPromoter)` |
| 3107 | 3122 | func | `(p *fenceAnchorPromoter).close` | `() ()` |
| 3124 | 3140 | func | `(p *fenceAnchorPromoter).resetRouteCursor` | `() ()` |
| 3142 | 3159 | func | `(p *fenceAnchorPromoter).snapshotView` | `() (*backenddb.Snapshot)` |
| 3161 | 3167 | func | `(p *fenceAnchorPromoter).v1LeafLogMode` | `() (bool)` |
| 3169 | 3174 | func | `(p *fenceAnchorPromoter).v1LeafLogLegacyMode` | `() (bool)` |
| 3176 | 3181 | func | `(p *fenceAnchorPromoter).v1LeafLogRouteMode` | `() (bool)` |
| 3183 | 3191 | func | `(p *fenceAnchorPromoter).encodeAnchorPointer` | `(basePtr page.ValuePtr) (page.ValuePtr)` |
| 3193 | 3202 | func | `(p *fenceAnchorPromoter).pointerActsAsAnchor` | `(ptr page.ValuePtr) (bool)` |
| 3204 | 3215 | func | `(p *fenceAnchorPromoter).getRewriteEntries` | `(capHint int) ([]outerleaf.TypedEntry)` |
| 3217 | 3222 | func | `(p *fenceAnchorPromoter).putRewriteEntries` | `(entries []outerleaf.TypedEntry) ()` |
| 3224 | 3226 | func | `(plan *fenceSourceRewritePlan).setValue` | `(key, value []byte) ()` |
| 3228 | 3230 | func | `(plan *fenceSourceRewritePlan).setValueOwned` | `(key, value []byte) ()` |
| 3232 | 3268 | func | `(plan *fenceSourceRewritePlan).setValueWithOwnership` | `(key, value []byte, owned bool) ()` |
| 3270 | 3280 | func | `(plan *fenceSourceRewritePlan).lookupValue` | `(key []byte) (([]byte, bool))` |
| 3282 | 3302 | func | `(plan *fenceSourceRewritePlan).ensureSetIndex` | `() ()` |
| 3304 | 3342 | func | `(plan *fenceSourceRewritePlan).lookupSetIndex` | `(keyHash uint64, key []byte) ((int, bool))` |
| 3344 | 3356 | func | `fenceRewriteKeyHash` | `(key []byte) (uint64)` |
| 3358 | 3384 | func | `(plan *fenceSourceRewritePlan).sourceFullyCovered` | `() (bool)` |
| 3386 | 3401 | func | `(plan *fenceSourceRewritePlan).containsKey` | `(key []byte) (bool)` |
| 3403 | 3410 | func | `(plan *fenceSourceRewritePlan).withinSourceRange` | `(key []byte) (bool)` |
| 3412 | 3420 | func | `(p *fenceAnchorPromoter).clearOverlapRewritePlans` | `() ()` |
| 3422 | 3458 | func | `(p *fenceAnchorPromoter).routeCursorLoadNext` | `() (error)` |
| 3460 | 3506 | func | `(p *fenceAnchorPromoter).lookupRouteSourceMonotonic` | `(key []byte) (([]byte, page.ValuePtr, bool, error))` |
| 3508 | 3517 | func | `(p *fenceAnchorPromoter).overlapRewriteSourceFullyCovered` | `(sourcePtr page.ValuePtr) (bool)` |
| 3519 | 3521 | func | `(p *fenceAnchorPromoter).remapRewrittenSource` | `(sourceKey []byte, sourcePtr page.ValuePtr) (([]byte, page.ValuePtr))` |
| 3523 | 3580 | func | `(p *fenceAnchorPromoter).remapRewrittenSourceForKey` | `(sourceKey []byte, sourcePtr page.ValuePtr, key []byte) (([]byte, page.ValuePtr))` |
| 3582 | 3605 | func | `(p *fenceAnchorPromoter).recordRewrittenSourceAlias` | `(oldSourcePtr page.ValuePtr, newSourceKey, newMaxKey []byte, newSourcePtr page.ValuePtr) ()` |
| 3607 | 3612 | func | `(p *fenceAnchorPromoter).resetRewrittenSourceAliasesFor` | `(sourcePtr page.ValuePtr) ()` |
| 3614 | 3677 | func | `(p *fenceAnchorPromoter).lookupPredecessorFenceAnchor` | `(key []byte) (([]byte, page.ValuePtr, bool, error))` |
| 3679 | 3728 | func | `(p *fenceAnchorPromoter).lookupFenceSourceForMutation` | `(key []byte) (([]byte, page.ValuePtr, bool, error))` |
| 3730 | 3732 | func | `(p *fenceAnchorPromoter).queueV1LeafLogOverlapRewrite` | `(key, value []byte) ((queued bool, sourcePtr page.ValuePtr, foundSource bool, err error))` |
| 3734 | 3736 | func | `(p *fenceAnchorPromoter).queueV1LeafLogRouteOverlapRewrite` | `(key, value []byte) ((queued bool, sourcePtr page.ValuePtr, foundSource bool, err error))` |
| 3738 | 3798 | func | `(p *fenceAnchorPromoter).queueLeafLogOverlapRewrite` | `(key, value []byte, routeMode bool) ((queued bool, sourcePtr page.ValuePtr, foundSource bool, err error))` |
| 3800 | 3802 | func | `(p *fenceAnchorPromoter).queueV1LeafLogOverlapRewriteWithSource` | `(sourceKey []byte, sourcePtr page.ValuePtr, key, value []byte) ((bool, error))` |
| 3804 | 3806 | func | `(p *fenceAnchorPromoter).queueV1LeafLogRouteOverlapRewriteWithSource` | `(sourceKey []byte, sourcePtr page.ValuePtr, key, value []byte) ((bool, error))` |
| 3808 | 3860 | func | `(p *fenceAnchorPromoter).queueLeafLogOverlapRewriteWithSource` | `(sourceKey []byte, sourcePtr page.ValuePtr, key, value []byte, routeMode bool) ((bool, error))` |
| 3862 | 3902 | func | `(p *fenceAnchorPromoter).materializeRewriteBlobRefs` | `(entries []outerleaf.TypedEntry, lane *lane, dictID uint64) (error)` |
| 3904 | 3908 | type | `overlapRewriteBlock` | `struct{3 fields}` |
| 3910 | 4007 | func | `(p *fenceAnchorPromoter).encodeRewriteBlocks` | `(out []outerleaf.TypedEntry) (([]overlapRewriteBlock, error))` |
| 4009 | 4136 | func | `(p *fenceAnchorPromoter).rewriteSourceFromPlan` | `(plan *fenceSourceRewritePlan, lane *lane, dictID uint64) (([]overlapRewriteBlock, bool, error))` |
| 4138 | 4140 | func | `(p *fenceAnchorPromoter).flushQueuedV1LeafLogOverlapRewrites` | `(emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error) (error)` |
| 4142 | 4144 | func | `(p *fenceAnchorPromoter).flushQueuedV1LeafLogRouteOverlapRewrites` | `(emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error) (error)` |
| 4146 | 4391 | func | `(p *fenceAnchorPromoter).flushQueuedLeafLogOverlapRewrites` | `(routeMode bool, emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error) (error)` |
| 4393 | 4399 | func | `overlapRewritePendingSortKey` | `(pending overlapRewritePending) ([]byte)` |
| 4401 | 4408 | func | `overlapRewritePendingPrimaryPlan` | `(pending overlapRewritePending) (*fenceSourceRewritePlan)` |
| 4410 | 4415 | func | `overlapRewritePendingSourceKey` | `(plan *fenceSourceRewritePlan) ([]byte)` |
| 4417 | 4422 | func | `overlapRewritePendingSourcePtr` | `(plan *fenceSourceRewritePlan) (page.ValuePtr)` |
| 4424 | 4443 | func | `dedupeOverlapRewritePlans` | `(plans []*fenceSourceRewritePlan) ([]*fenceSourceRewritePlan)` |
| 4445 | 4593 | func | `(p *fenceAnchorPromoter).coalesceRouteOverlapPending` | `(pending []overlapRewritePending) (([]overlapRewritePending, error))` |
| 4595 | 4623 | func | `(p *fenceAnchorPromoter).decodeFenceKeys` | `(ptr page.ValuePtr) (([][]byte, error))` |
| 4625 | 4684 | func | `(p *fenceAnchorPromoter).findPromotion` | `(ptr page.ValuePtr, anchorKey []byte) (([]byte, bool, error))` |
| 4686 | 4702 | func | `(p *fenceAnchorPromoter).emitUniquePointer` | `(key []byte, ptr page.ValuePtr, emitPointer func(key []byte, ptr page.ValuePtr) error) (error)` |
| 4704 | 4726 | func | `(p *fenceAnchorPromoter).keyResolvesFromSource` | `(snap *backenddb.Snapshot, key []byte, sourcePtr page.ValuePtr) ((exact bool, marked bool, resolved bool, err error))` |
| 4728 | 4849 | func | `(p *fenceAnchorPromoter).rewriteFallbackSourceBlock` | `(sourceKey, mutationKey []byte, sourcePtr page.ValuePtr) (([]byte, page.ValuePtr, bool, error))` |
| 4851 | 4853 | type | `fenceProjectionIteratorProvider` | `interface{1 methods}` |
| 4858 | 5000 | func | `(p *fenceAnchorPromoter).rematerializeFallbackSingleCandidate` | `(sourceKey, mutationKey []byte, sourcePtr page.ValuePtr, keys [][]byte, emitPointer func(key []byte, ptr page.ValuePtr) error) ((handled bool, err error))` |
| 5002 | 5147 | func | `(p *fenceAnchorPromoter).maybeRematerializeFallbackMutation` | `(mutationKey []byte, snap *backenddb.Snapshot, emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error, allowExactFallback bool) (error)` |
| 5149 | 5202 | func | `(p *fenceAnchorPromoter).maybePromoteAnchor` | `(key []byte, pending fencePendingMutation, emitPointer func(key []byte, ptr page.ValuePtr) error, emitDelete func(key []byte) error) (error)` |
| 5204 | 5214 | func | `(p *fenceAnchorPromoter).loadBackendRange` | `() ()` |
| 5216 | 5228 | func | `(p *fenceAnchorPromoter).keyCouldExistInBackend` | `(key []byte) (bool)` |
| 5230 | 5270 | func | `(p *fenceAnchorPromoter).shouldEmitRouteDelete` | `(key []byte) (bool)` |
| 5272 | 5283 | func | `(p *fenceAnchorPromoter).cachedRouteDeleteDecision` | `(key []byte) ((bool, bool))` |
| 5285 | 5303 | func | `(p *fenceAnchorPromoter).storeRouteDeleteDecision` | `(key []byte, emit bool) ()` |
| 5305 | 5326 | func | `(p *fenceAnchorPromoter).backendDefinitelyEmpty` | `() (bool)` |
| 5328 | 5338 | func | `reserveBackendBatchOps` | `(backendBatch batch.Interface, n int) ()` |
| 5340 | 5352 | func | `scaledReserveHint` | `(n, capHint int) (int)` |
| 5354 | 5369 | func | `scaledRouteDeferredReserveHint` | `(n, maxEntries int) (int)` |
| 5371 | 5385 | func | `mapRouteUnresolvedSourcePos` | `(unresolved []int, normToSource []int, pos int) ((int, error))` |
| 5387 | 6039 | func | `(db *DB).flushDeferredValueLogMemtable` | `(iter iterator.UnsafeIterator, backendBatch batch.Interface, memLen int, sync bool, laneID int, mutationLookup fenceMutationLookupFn, fenceState *deferredFenceCollapseState) ((int, error))` |
| 6041 | 6833 | func | `(db *DB).flushDeferredValueLogUnits` | `(units []flushUnit, backendBatch batch.Interface, sync bool, laneID int) ((int, error))` |
| 6836 | 6864 | func | `(db *DB).SetDictStore` | `(store DictStore) ()` |
| 6867 | 6877 | func | `(db *DB).SetTemplateStore` | `(store template.Store) ()` |
| 6879 | 6891 | func | `(db *DB).templateLookup` | `(ctx context.Context, templateID uint64) (([]byte, error))` |
| 6893 | 6910 | func | `(db *DB).currentDictID` | `(ctx context.Context) ((uint64, error))` |
| 6912 | 6936 | func | `(db *DB).dictBytes` | `(ctx context.Context, dictID uint64) (([]byte, error))` |
| 6938 | 6971 | func | `(db *DB).dictBytesForLane` | `(ctx context.Context, l *lane, dictID uint64) (([]byte, error))` |
| 6973 | 6975 | func | `(db *DB).templateCompressionEnabled` | `() (bool)` |
| 6977 | 6997 | func | `(db *DB).valueLogTemplateEncodeRecords` | `(records []valuelog.Record) (([]valuelog.Record, bool))` |
| 6999 | 7016 | func | `(db *DB).readValueLog` | `(key []byte, ptr page.ValuePtr) (([]byte, error))` |
| 7018 | 7047 | func | `(db *DB).readValueLogAppend` | `(key []byte, ptr page.ValuePtr, dst []byte) (([]byte, error))` |
| 7049 | 7063 | func | `(db *DB).flushValueLogForPtr` | `(ptr page.ValuePtr) (error)` |
| 7065 | 7092 | func | `(db *DB).flushValueLog` | `(laneIDs ...int) (error)` |
| 7094 | 7107 | func | `(db *DB).flushDeferredValueLogForBackendRead` | `() (error)` |
| 7109 | 7136 | func | `(db *DB).syncValueLog` | `(laneIDs ...int) (error)` |
| 7138 | 7184 | func | `(db *DB).flushValueLogLane` | `(l *lane) (error)` |
| 7186 | 7227 | func | `(db *DB).syncValueLogLane` | `(l *lane) (error)` |
| 7229 | 7231 | func | `(db *DB).logSegmentPrefix` | `(laneID int) (string)` |
| 7233 | 7250 | func | `(db *DB).markValueLogRetain` | `(path string) ()` |
| 7252 | 7261 | func | `(db *DB).forgetValueLogRetain` | `(path string) ()` |
| 7263 | 7279 | func | `(db *DB).dropValueLogSegment` | `(path string) ()` |
| 7281 | 7289 | func | `(db *DB).valueLogRetained` | `(path string) (bool)` |
| 7291 | 7342 | func | `(db *DB).valueLogRetainedStats` | `() ((segments int, bytes int64))` |
| 7344 | 7356 | func | `(db *DB).valueLogRetainedPaths` | `() ([]string)` |
| 7360 | 7362 | func | `(db *DB).ValueLogRetainedPaths` | `() ([]string)` |
| 7365 | 7370 | func | `(db *DB).ReadValueLogRecord` | `(ptr page.ValuePtr) (([]byte, error))` |
| 7372 | 7374 | type | `backendPointerProjectionIterator` | `interface{1 methods}` |
| 7376 | 7412 | func | `(db *DB).collectValueLogLiveIDs` | `() ((map[uint32]struct{}, error))` |
| 7414 | 7447 | func | `(db *DB).collectNestedValueLogLiveIDsFromOuterLeaf` | `(ptr page.ValuePtr, live map[uint32]struct{}) (error)` |
| 7449 | 7462 | func | `(db *DB).checkValueLogRetention` | `() ()` |
| 7464 | 7489 | func | `(db *DB).allowValueLogPointers` | `() (bool)` |
| 7491 | 7493 | type | `valueLogZombieMarker` | `interface{1 methods}` |
| 7495 | 7497 | type | `valueLogSetRefresher` | `interface{1 methods}` |
| 7499 | 7586 | func | `(db *DB).pruneRetainedValueLogs` | `() ()` |
| 7588 | 7588 | const | `retainedValueLogPruneMinInterval` | `` |
| 7590 | 7608 | func | `(db *DB).maybePruneRetainedValueLogs` | `() ()` |
| 7610 | 7612 | func | `hashKey` | `(key []byte) (uint64)` |
| 7614 | 7619 | func | `(db *DB).shardIndex` | `(key []byte) (int)` |
| 7621 | 7623 | func | `(db *DB).shardForKey` | `(key []byte) (*memShard)` |
| 7625 | 7633 | func | `(db *DB).laneForShardIndex` | `(shardID int) (int)` |
| 7635 | 7640 | func | `(db *DB).shardExceedsLimit` | `(shard *memShard, addBytes int64) (bool)` |
| 7642 | 7663 | func | `(db *DB).newBackendBatchWithSize` | `(size int) (batch.Interface)` |
| 7666 | 7678 | type | `BackendDB` | `interface{11 methods}` |
| 7680 | 7906 | type | `Options` | `struct{64 fields}` |
| 7909 | 7912 | type | `DictStore` | `interface{2 methods}` |
| 7914 | 7917 | type | `batchArenaLease` | `struct{2 fields}` |
| 7919 | 7922 | type | `memtableViewDeferredInfo` | `struct{2 fields}` |
| 7924 | 7942 | type | `memtableViewLifecycleTelemetry` | `struct{13 fields}` |
| 7944 | 8236 | type | `DB` | `struct{244 fields}` |
| 8238 | 8267 | func | `(db *DB).flushBackendEntriesCap` | `(totalOps int, sync bool) (int)` |
| 8269 | 8308 | func | `(db *DB).flushBackendEntriesCapForOps` | `(totalOps int, deleteOps int, sync bool) (int)` |
| 8310 | 8314 | type | `keyRange` | `struct{3 fields}` |
| 8316 | 8322 | type | `memShard` | `struct{5 fields}` |
| 8326 | 8334 | type | `memtableView` | `struct{7 fields}` |
| 8336 | 8339 | type | `snapshotMemtableReader` | `struct{2 fields}` |
| 8341 | 8363 | func | `(r *snapshotMemtableReader).GetEntry` | `(key []byte) ((node.LeafEntry, bool, error))` |
| 8365 | 8365 | const | `maxAppendOnlyMemLeases` | `` |
| 8366 | 8366 | const | `appendOnlyEstimatedBytesPerEntryDeferredFence` | `` |
| 8368 | 8378 | func | `updateInt64Max` | `(dst *atomic.Int64, value int64) ()` |
| 8380 | 8388 | func | `(db *DB).noteMemtableViewRetain` | `() ()` |
| 8390 | 8397 | func | `(db *DB).noteMemtableViewRelease` | `() ()` |
| 8399 | 8437 | func | `(db *DB).noteMemtableViewDeferredEnter` | `(view *memtableView, memtables int64) ()` |
| 8439 | 8468 | func | `(db *DB).noteMemtableViewDeferredExit` | `(view *memtableView) ()` |
| 8470 | 8496 | func | `(db *DB).retainMemtableView` | `() (*memtableView)` |
| 8498 | 8500 | func | `(db *DB).releaseMemtableView` | `(view *memtableView) ()` |
| 8502 | 8504 | func | `(db *DB).releasePublishedMemtableView` | `(view *memtableView) ()` |
| 8506 | 8522 | func | `(db *DB).releaseMemtableViewRef` | `(view *memtableView, leaseRelease bool) ()` |
| 8524 | 8529 | func | `memtableNeedsBatchArenaRetention` | `(mt memtable.Table) (bool)` |
| 8531 | 8539 | func | `getBatchArenaLease` | `(refs int, chunks [][]byte) (*batchArenaLease)` |
| 8541 | 8548 | func | `putBatchArenaLease` | `(lease *batchArenaLease) ()` |
| 8550 | 8578 | func | `(db *DB).retainBatchArenaChunksForMemtables` | `(chunks [][]byte, mems []memtable.Table) ()` |
| 8580 | 8605 | func | `(db *DB).releaseBatchArenaLeasesForMemtable` | `(mt memtable.Table) ()` |
| 8607 | 8613 | func | `(db *DB).queueRetiredMemtableLocked` | `(mem memtable.Table) ()` |
| 8615 | 8622 | func | `(db *DB).queueRetiredMemtablesLocked` | `(mems []memtable.Table) ()` |
| 8624 | 8638 | func | `(db *DB).popAppendOnlyMemLease` | `() (*memtable.AppendOnly)` |
| 8640 | 8651 | func | `(db *DB).putAppendOnlyMemLease` | `(mt *memtable.AppendOnly) (bool)` |
| 8653 | 8666 | func | `(db *DB).recycleMemtables` | `(mems []memtable.Table) ()` |
| 8668 | 8704 | func | `(db *DB).newMutableMemtableWithCapacityMode` | `(capacity int, mode memtable.Mode) ((memtable.Table, error))` |
| 8708 | 8745 | func | `(db *DB).publishMemtablesLocked` | `() ()` |
| 8749 | 8759 | func | `(db *DB).ensureQueueLaneIDsLocked` | `() ()` |
| 8761 | 8770 | type | `memtableStats` | `struct{8 fields}` |
| 8772 | 8788 | func | `(r *keyRange).add` | `(key []byte) ()` |
| 8790 | 8802 | func | `rangesOverlap` | `(a, b keyRange) (bool)` |
| 8806 | 8825 | func | `overlapsQuery` | `(start, end []byte, r keyRange) (bool)` |
| 8832 | 8843 | func | `queryCoversRange` | `(start, end []byte, r keyRange) (bool)` |
| 8845 | 8852 | func | `cloneRange` | `(r keyRange) (keyRange)` |
| 8854 | 8876 | func | `(db *DB).snapshotMutableRange` | `() (keyRange)` |
| 8878 | 8915 | func | `(db *DB).resetMutableShardsLocked` | `(nextMode memtable.Mode, reuse bool) (error)` |
| 8917 | 8940 | func | `(db *DB).noteWriteKey` | `(key []byte) ()` |
| 8943 | 8969 | func | `(db *DB).noteWriteSortedRun` | `(first, last []byte, count int) ()` |
| 8971 | 8980 | func | `(db *DB).noteIterator` | `(start, end []byte) ()` |
| 8982 | 8988 | func | `(db *DB).updateMutableThresholdLocked` | `() ()` |
| 8990 | 8992 | func | `(db *DB).mutableFlushThreshold` | `() (int64)` |
| 8994 | 9026 | func | `(db *DB).chooseAdaptiveMemtableModeLocked` | `() (memtable.Mode)` |
| 9028 | 9034 | func | `(db *DB).updateAdaptiveObservationLocked` | `() ()` |
| 9036 | 9041 | func | `(db *DB).applyAdaptiveMemtableModeLocked` | `() (memtable.Mode)` |
| 9043 | 9060 | func | `validateValueLogDomainThresholds` | `(domains []backenddb.ValueLogDomainThreshold) (error)` |
| 9062 | 9775 | func | `Open` | `(dir string, backend BackendDB, opts Options) ((*DB, error))` |
| 9777 | 9783 | type | `flushBuildJob` | `struct{5 fields}` |
| 9785 | 9793 | func | `(job flushBuildJob).report` | `(err error) ()` |
| 9795 | 9880 | func | `(job flushBuildJob).run` | `(closeCh <-chan struct{}) ()` |
| 9882 | 9900 | func | `(db *DB).reportError` | `(err error) ()` |
| 9902 | 9906 | func | `(db *DB).backgroundError` | `() (error)` |
| 9918 | 9934 | func | `(db *DB).StartAutoCheckpoint` | `(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) ()` |
| 9937 | 9945 | func | `(db *DB).TriggerAutoCheckpoint` | `() ()` |
| 9947 | 9997 | func | `(db *DB).noteWrite` | `() ()` |
| 9999 | 9999 | type | `autoCheckpointMode` | `uint8` |
| 10002 | 10002 | const | `autoCheckpointModeInterval` | `autoCheckpointMode` |
| 10003 | 10003 | const | `autoCheckpointModeIdle` | `` |
| 10004 | 10004 | const | `autoCheckpointModeSize` | `` |
| 10005 | 10005 | const | `autoCheckpointModeForce` | `` |
| 10009 | 10009 | const | `autoCheckpointMinIdleWALBytesMin` | `int64` |
| 10010 | 10010 | const | `autoCheckpointMinIdleWALBytesMax` | `int64` |
| 10011 | 10011 | const | `autoCheckpointMinIdleInterval` | `` |
| 10014 | 10027 | func | `autoCheckpointReasonString` | `(v uint32) (string)` |
| 10029 | 10040 | func | `resetTimer` | `(t *time.Timer, d time.Duration) ()` |
| 10042 | 10052 | func | `(db *DB).effectiveWALBytes` | `() (int64)` |
| 10054 | 10070 | func | `(db *DB).reclaimableWALBytes` | `() (int64)` |
| 10072 | 10087 | func | `(db *DB).minIdleCheckpointWALBytes` | `() (int64)` |
| 10090 | 10090 | const | `commitLogSegmentHeaderBytes` | `` |
| 10091 | 10091 | const | `commitLogBatchHeaderBytes` | `` |
| 10092 | 10092 | const | `commitLogRecordHeaderBytes` | `` |
| 10095 | 10097 | func | `(db *DB).logRecordSize` | `(key, value []byte) (int64)` |
| 10099 | 10108 | func | `(db *DB).logBatchSize` | `(records []logRecord) (int64)` |
| 10110 | 10118 | func | `(db *DB).assignCommitSeq` | `(records []logRecord) ()` |
| 10120 | 10120 | type | `domainIngressOp` | `uint8` |
| 10123 | 10123 | const | `domainIngressOpSet` | `domainIngressOp` |
| 10124 | 10124 | const | `domainIngressOpDelete` | `` |
| 10127 | 10133 | type | `domainIngressRequest` | `struct{5 fields}` |
| 10135 | 10139 | type | `walWriteRequest` | `struct{3 fields}` |
| 10141 | 10144 | type | `walAck` | `struct{2 fields}` |
| 10146 | 10148 | var | `walAckPool` | `` |
| 10150 | 10160 | type | `vlogWriteRequest` | `struct{9 fields}` |
| 10162 | 10167 | type | `vlogAck` | `struct{4 fields}` |
| 10169 | 10171 | var | `vlogAckPool` | `` |
| 10173 | 10185 | type | `vlogDictPrepareTask` | `struct{11 fields}` |
| 10187 | 10193 | type | `vlogDictPrepareResult` | `struct{5 fields}` |
| 10195 | 10216 | func | `(db *DB).publishVlogDictPrepareResult` | `(task vlogDictPrepareTask, res vlogDictPrepareResult) ()` |
| 10219 | 10219 | const | `maxEntryPoolCap` | `` |
| 10220 | 10220 | const | `maxEntryRunsPoolCap` | `` |
| 10221 | 10221 | const | `maxUnitRunsPoolCap` | `` |
| 10222 | 10222 | const | `maxOpMergeHeapCap` | `` |
| 10223 | 10223 | const | `entrySliceLeaseMinShift` | `` |
| 10224 | 10224 | const | `entrySliceLeaseMaxShift` | `` |
| 10225 | 10225 | const | `entrySliceLeaseClassCount` | `` |
| 10226 | 10226 | const | `maxEntrySliceLeasesPerBucket` | `` |
| 10229 | 10229 | var | `entrySlicePools` | `[entrySliceLeaseClassCount]sync.Pool` |
| 10230 | 10230 | var | `entryRunsPool` | `sync.Pool` |
| 10231 | 10231 | var | `unitRunsPool` | `sync.Pool` |
| 10232 | 10232 | var | `opMergeHeapPool` | `sync.Pool` |
| 10233 | 10233 | var | `entrySliceLeaseMu` | `sync.Mutex` |
| 10234 | 10234 | var | `entrySliceLeases` | `[entrySliceLeaseClassCount][][]batch.Entry` |
| 10236 | 10260 | func | `entrySliceLeaseClassForLen` | `(capacity int) ((idx int, classCap int, ok bool))` |
| 10262 | 10276 | func | `entrySliceLeaseClassForCap` | `(capacity int) ((idx int, ok bool))` |
| 10278 | 10294 | func | `entrySliceMaxReuseCap` | `(capacity int) (int)` |
| 10296 | 10339 | func | `getEntrySlice` | `(capacity int) ([]batch.Entry)` |
| 10341 | 10363 | func | `putEntrySlice` | `(entries []batch.Entry) ()` |
| 10365 | 10378 | func | `getEntryRuns` | `(capacity int) ([][]batch.Entry)` |
| 10380 | 10386 | func | `putEntryRuns` | `(runs [][]batch.Entry) ()` |
| 10388 | 10401 | func | `getUnitRuns` | `(length int) ([][][]batch.Entry)` |
| 10403 | 10409 | func | `putUnitRuns` | `(runs [][][]batch.Entry) ()` |
| 10411 | 10424 | func | `getOpMergeHeap` | `(capacity int) (opMergeHeap)` |
| 10426 | 10433 | func | `putOpMergeHeap` | `(h opMergeHeap) ()` |
| 10435 | 10453 | func | `estimateUnitRunEntries` | `(unitRuns [][][]batch.Entry, floor int) (int)` |
| 10455 | 10494 | func | `collectOpsInto` | `(mem memtable.Table, dst []batch.Entry) ((int, error))` |
| 10496 | 10501 | type | `opRunIter` | `struct{4 fields}` |
| 10503 | 10507 | func | `newOpRunIter` | `(runs [][]batch.Entry) (*opRunIter)` |
| 10509 | 10519 | func | `(it *opRunIter).advanceToValid` | `() ()` |
| 10521 | 10523 | func | `(it *opRunIter).Valid` | `() (bool)` |
| 10525 | 10531 | func | `(it *opRunIter).Next` | `() ()` |
| 10533 | 10538 | func | `(it *opRunIter).Entry` | `() (batch.Entry)` |
| 10540 | 10545 | func | `(it *opRunIter).Key` | `() ([]byte)` |
| 10547 | 10551 | type | `opMergeItem` | `struct{3 fields}` |
| 10553 | 10553 | type | `opMergeHeap` | `[]opMergeItem` |
| 10555 | 10555 | func | `(h opMergeHeap).Len` | `() (int)` |
| 10557 | 10563 | func | `(h opMergeHeap).Less` | `(i, j int) (bool)` |
| 10565 | 10567 | func | `(h opMergeHeap).Swap` | `(i, j int) ()` |
| 10569 | 10572 | func | `(h *opMergeHeap).push` | `(x opMergeItem) ()` |
| 10574 | 10585 | func | `(h *opMergeHeap).pop` | `() (opMergeItem)` |
| 10587 | 10592 | func | `(h opMergeHeap).peek` | `() (*opMergeItem)` |
| 10594 | 10603 | func | `(h *opMergeHeap).up` | `(j int) ()` |
| 10605 | 10623 | func | `(h *opMergeHeap).down` | `(i0, n int) (bool)` |
| 10625 | 10630 | type | `opUnsafeMergeItem` | `struct{4 fields}` |
| 10632 | 10632 | type | `opUnsafeMergeHeap` | `[]opUnsafeMergeItem` |
| 10634 | 10634 | func | `(h opUnsafeMergeHeap).Len` | `() (int)` |
| 10636 | 10642 | func | `(h opUnsafeMergeHeap).Less` | `(i, j int) (bool)` |
| 10644 | 10646 | func | `(h opUnsafeMergeHeap).Swap` | `(i, j int) ()` |
| 10648 | 10651 | func | `(h *opUnsafeMergeHeap).push` | `(x opUnsafeMergeItem) ()` |
| 10653 | 10664 | func | `(h *opUnsafeMergeHeap).pop` | `() (opUnsafeMergeItem)` |
| 10666 | 10671 | func | `(h opUnsafeMergeHeap).peek` | `() (*opUnsafeMergeItem)` |
| 10673 | 10682 | func | `(h *opUnsafeMergeHeap).up` | `(j int) ()` |
| 10684 | 10702 | func | `(h *opUnsafeMergeHeap).down` | `(i0, n int) (bool)` |
| 10704 | 10764 | func | `buildOpRuns` | `(mem memtable.Table, chunkCap int) (([][]batch.Entry, int, error))` |
| 10766 | 10769 | type | `walFastItem` | `struct{2 fields}` |
| 10772 | 10772 | const | `walWriteBuffer` | `` |
| 10773 | 10773 | const | `walWriteBatchMax` | `` |
| 10774 | 10774 | const | `walFastBatchMax` | `` |
| 10775 | 10775 | const | `walFastQueueMax` | `` |
| 10776 | 10776 | const | `defaultDomainIngressQueueSize` | `` |
| 10777 | 10777 | const | `domainIngressBatchMax` | `` |
| 10781 | 10781 | const | `vlogWriteBuffer` | `` |
| 10782 | 10782 | const | `vlogWriteBatchMax` | `` |
| 10783 | 10783 | const | `vlogDictPrepBuffer` | `` |
| 10788 | 10788 | const | `vlogQueueMinValueSize` | `` |
| 10791 | 10791 | const | `vlogWriteLinger` | `` |
| 10793 | 10814 | func | `defaultJournalLaneCount` | `(procs int) (int)` |
| 10816 | 10841 | func | `(db *DB).startDomainIngressWorkers` | `() ()` |
| 10843 | 10854 | func | `(db *DB).stopDomainIngressWorkers` | `() ()` |
| 10856 | 10880 | func | `(db *DB).domainIngressLoop` | `(ch <-chan domainIngressRequest) ()` |
| 10882 | 10935 | func | `(db *DB).processDomainIngressBatch` | `(reqs []domainIngressRequest) ()` |
| 10937 | 10951 | func | `(db *DB).observeDomainIngressDepth` | `(depth int) ()` |
| 10953 | 11000 | func | `(db *DB).enqueueDomainIngress` | `(op domainIngressOp, key, value []byte, sync bool) ((bool, error))` |
| 11002 | 11012 | func | `(db *DB).startWALWriter` | `(l *lane) ()` |
| 11014 | 11027 | func | `(db *DB).startVlogWriter` | `(l *lane) ()` |
| 11029 | 11046 | func | `(db *DB).vlogWriteWorkerCount` | `() (int)` |
| 11048 | 11068 | func | `(db *DB).vlogDictPrepWorkerCount` | `() (int)` |
| 11070 | 11081 | func | `(db *DB).startVlogDictPreparer` | `(l *lane) ()` |
| 11083 | 11105 | func | `(db *DB).ensureVlogDictPrepWorkers` | `(l *lane, wanted int) ()` |
| 11107 | 11164 | func | `(db *DB).vlogDictPrepareLoop` | `(l *lane) ()` |
| 11166 | 11214 | func | `(db *DB).walWriteLoop` | `(l *lane) ()` |
| 11216 | 11286 | func | `(db *DB).vlogWriteLoop` | `(l *lane) ()` |
| 11288 | 11344 | func | `(db *DB).walFastLoop` | `(l *lane) ()` |
| 11346 | 11384 | func | `(db *DB).drainWALWriter` | `(l *lane, batch []walWriteRequest) ()` |
| 11386 | 11406 | func | `(db *DB).drainVlogWriter` | `(l *lane, batch []vlogWriteRequest) ()` |
| 11408 | 11417 | func | `(db *DB).finishWALRequests` | `(requests []walWriteRequest, err error) ()` |
| 11419 | 11472 | func | `(db *DB).flushWALRequests` | `(l *lane, requests []walWriteRequest) (error)` |
| 11474 | 12199 | func | `(db *DB).flushVlogRequests` | `(l *lane, requests []vlogWriteRequest) ()` |
| 12204 | 12204 | type | `journalDurability` | `uint8` |
| 12207 | 12207 | const | `journalDurabilityNone` | `journalDurability` |
| 12208 | 12208 | const | `journalDurabilityFlush` | `` |
| 12209 | 12209 | const | `journalDurabilitySync` | `` |
| 12212 | 12249 | func | `(db *DB).appendWAL` | `(l *lane, records []logRecord, durability journalDurability) (error)` |
| 12251 | 12271 | func | `(db *DB).appendWALOne` | `(l *lane, record logRecord, durability journalDurability) (error)` |
| 12273 | 12283 | func | `(db *DB).appendWALOneChecked` | `(l *lane, record logRecord, durability journalDurability) (error)` |
| 12285 | 12291 | type | `preparedDictFrame` | `struct{5 fields}` |
| 12293 | 12300 | func | `releasePreparedDictFrame` | `(frame *preparedDictFrame) ()` |
| 12302 | 12306 | func | `releasePreparedDictFrames` | `(frames []preparedDictFrame) ()` |
| 12308 | 12315 | func | `(db *DB).valueLogKeepPolicy` | `() (ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64)` |
| 12317 | 12334 | func | `(db *DB).shouldUseVlogDictPrepWorkers` | `(l *lane, frameCount, rawPayloadBytes int) (bool)` |
| 12336 | 12373 | func | `(db *DB).shouldQueueValueLogOne` | `(l *lane, dictID uint64, valueLen int, durability journalDurability, writeMode vlogCompressionWriteMode, wallStart time.Time) (bool)` |
| 12375 | 12512 | func | `(db *DB).prepareAppendDictFrames` | `(l *lane, dictID uint64, dict []byte, records []valuelog.Record, k int, rawPayloadBytes int, ioNsPerStoredByte float64, encodeNsPerRawByte float64, safetyMargin float64, wallStart time.Time) (([]preparedDictFrame, int64, error))` |
| 12514 | 13186 | func | `(db *DB).appendValueLog` | `(l *lane, dictID uint64, dict []byte, records []valuelog.Record, durability journalDurability) (([]page.ValuePtr, error))` |
| 13188 | 13190 | func | `(db *DB).appendValueLogOne` | `(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) ((page.ValuePtr, string, error))` |
| 13192 | 13194 | func | `(db *DB).appendValueLogOneRaw` | `(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) ((page.ValuePtr, string, error))` |
| 13198 | 13214 | func | `(db *DB).appendFenceRIDJoinOversizedOne` | `(l *lane, dictID uint64, key, value []byte, durability journalDurability) ((ptr page.ValuePtr, rid uint64, retainPath string, err error))` |
| 13216 | 13218 | func | `(db *DB).appendValueLogOneWithKey` | `(l *lane, dictID uint64, dict []byte, rid uint64, key []byte, value []byte, durability journalDurability) ((page.ValuePtr, string, error))` |
| 13220 | 13686 | func | `(db *DB).appendValueLogOneInternal` | `(l *lane, dictID uint64, dict []byte, rid uint64, key []byte, value []byte, durability journalDurability, encodeOuterLeaf bool) ((page.ValuePtr, string, error))` |
| 13688 | 13733 | func | `(db *DB).appendWALInline` | `(l *lane, records []logRecord, flush bool) (error)` |
| 13735 | 13766 | func | `(db *DB).appendWALInlineOne` | `(l *lane, record logRecord, flush bool) (error)` |
| 13768 | 13794 | func | `(db *DB).flushWALLane` | `(l *lane) (error)` |
| 13796 | 13819 | func | `(db *DB).appendWALDirect` | `(l *lane, records []logRecord, sync bool) (error)` |
| 13821 | 13854 | func | `(db *DB).appendWALFast` | `(l *lane, record logRecord) (error)` |
| 13856 | 13896 | func | `(db *DB).autoCheckpointLoop` | `(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) ()` |
| 13898 | 13968 | func | `(db *DB).maybeAutoCheckpoint` | `(maxWALBytes int64, mode autoCheckpointMode) ()` |
| 13970 | 13994 | func | `(db *DB).ensureBackendRange` | `() (error)` |
| 13996 | 14021 | func | `(db *DB).computeBackendRange` | `() ((keyRange, bool, error))` |
| 14023 | 14023 | const | `stopResumeFraction` | `` |
| 14024 | 14024 | const | `stopBackpressureStallLimit` | `` |
| 14031 | 14031 | const | `v2DeferredAssistEarlyTriggerDiv` | `` |
| 14033 | 14033 | const | `v2DeferredAssistTickMask` | `` |
| 14034 | 14034 | const | `v2DeferredAssistSlowdownMinMemtables` | `` |
| 14035 | 14035 | const | `v2DeferredAssistStopMinMemtables` | `` |
| 14038 | 14040 | func | `(db *DB).adaptiveBackpressureEnabled` | `() (bool)` |
| 14042 | 14056 | func | `(db *DB).thresholdsLocked` | `() (slowdownBytes, stopBytes, resumeBytes int64)` |
| 14058 | 14067 | func | `(db *DB).waitForCheckpoint` | `() ()` |
| 14069 | 14087 | func | `(db *DB).recordCheckpointCutover` | `(d time.Duration) ()` |
| 14093 | 14106 | func | `(db *DB).checkpointRotateCapacity` | `() (int)` |
| 14108 | 14127 | func | `(db *DB).observePublishWatermarkLagDrift` | `(backlogBytes int64, now time.Time) (float64)` |
| 14138 | 14312 | func | `(db *DB).Checkpoint` | `() (error)` |
| 14314 | 14388 | func | `(db *DB).waitForStop` | `() ()` |
| 14390 | 14410 | func | `(db *DB).shouldWaitForStop` | `() (bool)` |
| 14412 | 14416 | func | `(db *DB).maybeWaitForStop` | `() ()` |
| 14418 | 14439 | func | `(db *DB).deferredAssistTargetMemtables` | `(backlogBytes, slowdownBytes, stopBytes int64) ((int, bool))` |
| 14441 | 14461 | func | `(db *DB).deferredAssistEarlyTriggerBytes` | `(slowdownBytes, stopBytes int64) (int64)` |
| 14463 | 14471 | func | `(db *DB).recordDeferredAssist` | `(flushed int) ()` |
| 14473 | 14478 | func | `(db *DB).routeLeafLogWriterAssistEnabled` | `() (bool)` |
| 14480 | 14542 | func | `(db *DB).maybeAssistFlush` | `() ()` |
| 14544 | 14579 | func | `(db *DB).flushSome` | `(sync bool, maxMemtables int, maxDuration time.Duration) (int)` |
| 14583 | 14610 | func | `(db *DB).flushSomeBlocking` | `(sync bool, maxMemtables int, maxDuration time.Duration) ()` |
| 14612 | 14770 | func | `(db *DB).Close` | `() (error)` |
| 14771 | 14780 | func | `(db *DB).Set` | `(key, value []byte) (error)` |
| 14782 | 14791 | func | `(db *DB).SetSync` | `(key, value []byte) (error)` |
| 14793 | 14812 | func | `(db *DB).flushAllMemtablesForSync` | `(sync bool) (error)` |
| 14814 | 14830 | func | `(db *DB).syncBarrierAfterWrite` | `(sync bool) (error)` |
| 14832 | 14837 | func | `(db *DB).set` | `(key, value []byte, sync bool) (error)` |
| 14839 | 15040 | func | `(db *DB).setDirect` | `(key, value []byte, sync bool) (error)` |
| 15042 | 15048 | func | `(db *DB).Delete` | `(key []byte) (error)` |
| 15054 | 15651 | func | `(db *DB).DeleteRange` | `(start, end []byte) (error)` |
| 15653 | 15659 | func | `(db *DB).DeleteSync` | `(key []byte) (error)` |
| 15661 | 15666 | func | `(db *DB).delete` | `(key []byte, sync bool) (error)` |
| 15668 | 15740 | func | `(db *DB).deleteDirect` | `(key []byte, sync bool) (error)` |
| 15742 | 15757 | func | `(db *DB).canReuseWALSegments` | `() (bool)` |
| 15759 | 15843 | func | `(db *DB).rotateMemtableLockedWithCapacity` | `(triggerFlush bool, newCapacity int) (error)` |
| 15852 | 15854 | func | `(db *DB).rotateMemtableLockedForIterator` | `(newCapacity int) (error)` |
| 15856 | 15858 | func | `(db *DB).rotateMemtableLocked` | `(triggerFlush bool) (error)` |
| 15860 | 15868 | func | `(db *DB).rotateMemtableIfNeeded` | `(triggerFlush bool) (error)` |
| 15870 | 15879 | func | `(db *DB).maybeRotateMemtable` | `(triggerFlush bool) (error)` |
| 15887 | 15967 | func | `(db *DB).rotateMutableShardsLocked` | `(newCapacity int, triggerFlush bool) (error)` |
| 15969 | 15988 | func | `(db *DB).cleanupLaneWALWriters` | `(l *lane) ()` |
| 15990 | 16002 | func | `(db *DB).defaultVlogWriteMode` | `() (vlogCompressionWriteMode)` |
| 16004 | 16022 | func | `(db *DB).setVlogWriterMode` | `(l *lane, w valueWriter, mode vlogCompressionWriteMode, codec valuelog.BlockCodec) ()` |
| 16024 | 16026 | func | `(db *DB).rotateWALLocked` | `(l *lane) (error)` |
| 16028 | 16030 | func | `(db *DB).rotateWALCheckpointLocked` | `(l *lane) (error)` |
| 16032 | 16076 | func | `(db *DB).rotateWALLockedWithOptions` | `(l *lane, rotateValueLog bool) (error)` |
| 16078 | 16088 | func | `(db *DB).rotateValueLogLocked` | `(l *lane) (error)` |
| 16090 | 16141 | func | `(db *DB).rotateValueLogMuHeld` | `(l *lane) (error)` |
| 16143 | 16155 | func | `(db *DB).rotateValueLogForMaxSegmentMuHeld` | `(l *lane, w valueWriter) (error)` |
| 16157 | 16183 | func | `(db *DB).untrackWALSegmentLocked` | `(path string) ()` |
| 16185 | 16212 | func | `(db *DB).untrackValueLogSegmentLocked` | `(path string) ()` |
| 16214 | 16229 | func | `(db *DB).flushLoop` | `() ()` |
| 16231 | 16233 | func | `(db *DB).flushSyncRequested` | `(sync bool) (bool)` |
| 16235 | 16266 | func | `(db *DB).pickFlushLane` | `() ((int, bool))` |
| 16268 | 16272 | func | `(db *DB).flushAll` | `(reqSync bool) ()` |
| 16274 | 16331 | func | `(db *DB).flushAllLocked` | `(reqSync bool) ()` |
| 16333 | 16346 | func | `(db *DB).flushOne` | `() (bool)` |
| 16349 | 16349 | const | `flushCombineTargetBytes` | `int64` |
| 16350 | 16350 | const | `flushCombineTargetBytesMax` | `` |
| 16351 | 16351 | const | `flushCombineMaxMemtables` | `` |
| 16358 | 16358 | const | `flushBackendBatchMaxEntries` | `` |
| 16363 | 16363 | const | `flushBackendBatchInitEntries` | `` |
| 16366 | 16374 | type | `flushUnit` | `struct{7 fields}` |
| 16376 | 16436 | func | `(db *DB).collectFlushUnitsLocked` | `(laneID int, maxMemtables int, targetBytes int64) (([]flushUnit, []uint64, int64, int))` |
| 16438 | 16506 | func | `(db *DB).removeQueuedUnitsLocked` | `(removeIDs map[uint64]struct{}, units []flushUnit, totalBytes int64) ()` |
| 16508 | 17387 | func | `(db *DB).flushLaneOnce` | `(sync bool, laneID int) (bool)` |
| 17389 | 17391 | func | `(db *DB).finalizeFlushStats` | `(totalLen int, totalBytes int64, flushDur, durPreVlog, durBuild, durSet, durPostVlog, durPostVlogSync, durBackendWrite time.Duration) (error)` |
| 17393 | 17898 | func | `(db *DB).flushOneLocked` | `(sync bool) (bool)` |
| 17908 | 17924 | func | `(db *DB).canBypassMemtableRead` | `(view *memtableView, key []byte) (bool)` |
| 17929 | 17975 | func | `(db *DB).canBypassMemtableReadMany` | `(view *memtableView, keys [][]byte) (bool)` |
| 17977 | 18062 | func | `(db *DB).getMemtable` | `(key []byte) (([]byte, bool, error))` |
| 18064 | 18147 | func | `(db *DB).getMemtableAppend` | `(key, dst []byte) (([]byte, bool, error))` |
| 18149 | 18151 | type | `backendManyGetter` | `interface{1 methods}` |
| 18153 | 18155 | type | `backendManyPlanner` | `interface{1 methods}` |
| 18162 | 18177 | func | `(db *DB).GetManyParallelPlan` | `(keyCount int) ((workers int, parallel bool))` |
| 18179 | 18195 | func | `(db *DB).backendGetMany` | `(keys [][]byte) (([][]byte, error))` |
| 18198 | 18200 | func | `(db *DB).GetUnsafe` | `(key []byte) (([]byte, error))` |
| 18203 | 18261 | func | `(db *DB).Get` | `(key []byte) (([]byte, error))` |
| 18263 | 18268 | func | `(db *DB).snapshotQueueLen` | `() (int)` |
| 18273 | 18324 | func | `(db *DB).GetMany` | `(keys [][]byte) (([][]byte, error))` |
| 18328 | 18343 | func | `(db *DB).GetAppend` | `(key, dst []byte) (([]byte, error))` |
| 18346 | 18384 | func | `(db *DB).AcquireSnapshot` | `() (*backenddb.Snapshot)` |
| 18386 | 18449 | func | `(db *DB).Has` | `(key []byte) ((bool, error))` |
| 18451 | 18977 | func | `(db *DB).Stats` | `() (map[string]string)` |
| 18980 | 18985 | func | `(db *DB).TriggerFlush` | `() ()` |
| 18988 | 18990 | func | `(db *DB).QueueBacklogBytes` | `() (int64)` |
| 18995 | 19038 | func | `(db *DB).CompactionAssist` | `() ()` |
| 19040 | 19042 | func | `(db *DB).Print` | `() (error)` |
| 19051 | 19066 | func | `(db *DB).Drain` | `() (error)` |
| 19074 | 19080 | func | `(db *DB).Flush` | `() (error)` |
| 19083 | 19302 | func | `(db *DB).Iterator` | `(start, end []byte) ((merging.Iterator, error))` |
| 19304 | 19308 | type | `debugIterator` | `struct{3 fields}` |
| 19310 | 19312 | func | `(it *debugIterator).DebugStats` | `() ((queueLen int, sourcesUsed int))` |
| 19314 | 19319 | type | `leasedMergingIterator` | `struct{4 fields}` |
| 19321 | 19329 | func | `(it *leasedMergingIterator).Close` | `() (error)` |
| 19331 | 19339 | type | `concatUnsafeIterator` | `struct{6 fields}` |
| 19341 | 19350 | func | `newConcatUnsafeIterator` | `(first, second iterator.UnsafeIterator) (merging.Iterator)` |
| 19352 | 19378 | func | `(it *concatUnsafeIterator).advance` | `() ()` |
| 19380 | 19386 | func | `(it *concatUnsafeIterator).Next` | `() ()` |
| 19388 | 19388 | func | `(it *concatUnsafeIterator).Valid` | `() (bool)` |
| 19390 | 19395 | func | `(it *concatUnsafeIterator).Key` | `() ([]byte)` |
| 19397 | 19402 | func | `(it *concatUnsafeIterator).Value` | `() ([]byte)` |
| 19404 | 19409 | func | `(it *concatUnsafeIterator).KeyCopy` | `(dst []byte) ([]byte)` |
| 19411 | 19416 | func | `(it *concatUnsafeIterator).ValueCopy` | `(dst []byte) ([]byte)` |
| 19418 | 19431 | func | `(it *concatUnsafeIterator).Close` | `() (error)` |
| 19433 | 19444 | func | `(it *concatUnsafeIterator).Error` | `() (error)` |
| 19446 | 19446 | func | `(it *concatUnsafeIterator).Domain` | `() (start, end []byte)` |
| 19448 | 19453 | type | `reverseRangeFilterIterator` | `struct{4 fields}` |
| 19455 | 19463 | func | `newReverseRangeFilterIterator` | `(base iterator.UnsafeIterator, start, end []byte) (*reverseRangeFilterIterator)` |
| 19465 | 19483 | func | `(it *reverseRangeFilterIterator).advance` | `() ()` |
| 19485 | 19491 | func | `(it *reverseRangeFilterIterator).Next` | `() ()` |
| 19493 | 19495 | func | `(it *reverseRangeFilterIterator).Valid` | `() (bool)` |
| 19497 | 19502 | func | `(it *reverseRangeFilterIterator).Key` | `() ([]byte)` |
| 19504 | 19509 | func | `(it *reverseRangeFilterIterator).Value` | `() ([]byte)` |
| 19511 | 19516 | func | `(it *reverseRangeFilterIterator).KeyCopy` | `(dst []byte) ([]byte)` |
| 19518 | 19523 | func | `(it *reverseRangeFilterIterator).ValueCopy` | `(dst []byte) ([]byte)` |
| 19525 | 19530 | func | `(it *reverseRangeFilterIterator).Close` | `() (error)` |
| 19532 | 19537 | func | `(it *reverseRangeFilterIterator).Error` | `() (error)` |
| 19539 | 19544 | func | `(it *reverseRangeFilterIterator).Domain` | `() (start, end []byte)` |
| 19546 | 19555 | type | `singletonReverseEntryIterator` | `struct{8 fields}` |
| 19557 | 19562 | func | `(it *singletonReverseEntryIterator).Next` | `() ()` |
| 19564 | 19564 | func | `(it *singletonReverseEntryIterator).Valid` | `() (bool)` |
| 19566 | 19571 | func | `(it *singletonReverseEntryIterator).Key` | `() ([]byte)` |
| 19573 | 19578 | func | `(it *singletonReverseEntryIterator).Value` | `() ([]byte)` |
| 19580 | 19585 | func | `(it *singletonReverseEntryIterator).KeyCopy` | `(dst []byte) ([]byte)` |
| 19587 | 19595 | func | `(it *singletonReverseEntryIterator).ValueCopy` | `(dst []byte) ([]byte)` |
| 19597 | 19597 | func | `(it *singletonReverseEntryIterator).Close` | `() (error)` |
| 19599 | 19599 | func | `(it *singletonReverseEntryIterator).Error` | `() (error)` |
| 19601 | 19601 | func | `(it *singletonReverseEntryIterator).Domain` | `() (start, end []byte)` |
| 19603 | 19608 | func | `(it *singletonReverseEntryIterator).UnsafeKey` | `() ([]byte)` |
| 19610 | 19615 | func | `(it *singletonReverseEntryIterator).UnsafeValue` | `() ([]byte)` |
| 19617 | 19622 | func | `(it *singletonReverseEntryIterator).UnsafeEntry` | `() (([]byte, page.ValuePtr, byte))` |
| 19624 | 19629 | func | `(it *singletonReverseEntryIterator).IsDeleted` | `() (bool)` |
| 19631 | 19785 | func | `(db *DB).ReverseIterator` | `(start, end []byte) ((merging.Iterator, error))` |
| 19790 | 19818 | type | `Batch` | `struct{26 fields}` |
| 19820 | 19831 | func | `(db *DB).NewBatch` | `() (*Batch)` |
| 19833 | 19843 | func | `(db *DB).NewBatchWithSize` | `(size int) (*Batch)` |
| 19845 | 19860 | func | `(db *DB).batchEntriesCapHint` | `(minCap int) (int)` |
| 19862 | 19893 | func | `(db *DB).observeBatchEntries` | `(n int) ()` |
| 19895 | 19910 | func | `(db *DB).batchCopyArenaInitCap` | `(sizeHint int) (int)` |
| 19912 | 19947 | func | `(db *DB).observeBatchCopyBytes` | `(n int) ()` |
| 19949 | 19977 | func | `(db *DB).getBatchEntries` | `(minCap int) ([]batch.Entry)` |
| 19979 | 19989 | func | `(db *DB).putBatchEntries` | `(entries []batch.Entry) ()` |
| 19991 | 20013 | func | `(db *DB).getBatchShardEntries` | `(minCap int) ([]batch.Entry)` |
| 20015 | 20025 | func | `(db *DB).putBatchShardEntries` | `(entries []batch.Entry) ()` |
| 20027 | 20044 | func | `(db *DB).getBatchIntSlice` | `(minCap int) ([]int)` |
| 20046 | 20054 | func | `(db *DB).putBatchIntSlice` | `(idxs []int) ()` |
| 20060 | 20105 | func | `(b *Batch).Reset` | `() ()` |
| 20107 | 20114 | func | `(b *Batch).noteEntryAppend` | `() ()` |
| 20116 | 20127 | func | `(b *Batch).updateBatchEntryHint` | `() ()` |
| 20129 | 20138 | func | `(b *Batch).drainCopyArenaChunks` | `() ([][]byte)` |
| 20140 | 20149 | func | `(b *Batch).recycleCopyArenaChunks` | `() ()` |
| 20151 | 20158 | func | `(b *Batch).updateBatchCopyHint` | `() ()` |
| 20160 | 20189 | func | `(b *Batch).arenaCopy` | `(n int) ([]byte)` |
| 20191 | 20195 | func | `(b *Batch).cloneKey` | `(key []byte) ([]byte)` |
| 20197 | 20201 | func | `(b *Batch).cloneValue` | `(value []byte) ([]byte)` |
| 20203 | 20210 | func | `(b *Batch).cloneKeyValue` | `(key, value []byte) (([]byte, []byte))` |
| 20212 | 20259 | func | `(b *Batch).Set` | `(key, value []byte) (error)` |
| 20263 | 20305 | func | `(b *Batch).SetView` | `(key, value []byte) (error)` |
| 20307 | 20346 | func | `(b *Batch).Delete` | `(key []byte) (error)` |
| 20350 | 20388 | func | `(b *Batch).DeleteView` | `(key []byte) (error)` |
| 20390 | 20432 | func | `(b *Batch).SetOps` | `(ops []batch.Entry) (error)` |
| 20435 | 20435 | const | `batchDefaultEntriesCap` | `` |
| 20438 | 20438 | const | `batchHintEntriesMax` | `` |
| 20439 | 20439 | const | `streamSwitchMinEntries` | `` |
| 20440 | 20440 | const | `streamSwitchMinBytes` | `` |
| 20443 | 20443 | const | `multiLaneValueLogMinRecords` | `` |
| 20444 | 20444 | const | `batchCopyArenaMinChunk` | `` |
| 20445 | 20445 | const | `batchCopyArenaUnsizedInit` | `` |
| 20446 | 20446 | const | `batchCopyArenaBytesPerEntry` | `` |
| 20447 | 20447 | const | `batchCopyArenaInitMax` | `` |
| 20448 | 20448 | const | `batchCopyArenaMaxRetain` | `` |
| 20449 | 20449 | const | `batchEntriesPoolMaxRetain` | `` |
| 20450 | 20450 | const | `batchIntSlicePoolMaxRetain` | `` |
| 20453 | 20468 | func | `batchCopyArenaInitCapForEntries` | `(entries int) (int)` |
| 20470 | 20544 | func | `(b *Batch).maybeSwitchToStreaming` | `() ()` |
| 20546 | 20551 | func | `(b *Batch).Write` | `() (error)` |
| 20553 | 20613 | func | `(b *Batch).WriteSync` | `() (error)` |
| 20615 | 20629 | func | `(b *Batch).freezeDictID` | `(ctx context.Context) (error)` |
| 20631 | 20645 | func | `(b *Batch).ensureDictBytes` | `(ctx context.Context) (([]byte, error))` |
| 20647 | 20698 | func | `(b *Batch).write` | `(sync bool) (error)` |
| 20700 | 20809 | func | `(b *Batch).tryWriteWALOffStreamBypass` | `(sync bool) ((bool, error))` |
| 20811 | 21579 | func | `(b *Batch).writeRegular` | `(syncWrite bool) (error)` |
| 21581 | 21587 | type | `logSegmentInfo` | `struct{5 fields}` |
| 21589 | 21615 | func | `listNonEmptyLogSegments` | `(walDir string) ((segments []logSegmentInfo, nonEmptyBytes int64))` |
| 21617 | 21671 | func | `bootstrapNextRIDFromValueLogSegments` | `(segments []logSegmentInfo) ((maxRID uint64, scannedSegments uint64, scannedRecords uint64, err error))` |
| 21673 | 21754 | func | `parseLogSeq` | `(name string) ((int, int, bool, bool))` |
| 21756 | 21886 | func | `(b *Batch).writeBypass` | `(sync bool) (error)` |
| 21888 | 21933 | func | `(b *Batch).Close` | `() (error)` |
| 21935 | 21949 | func | `(b *Batch).Replay` | `(fn func(batch.Entry) error) (error)` |
| 21951 | 21956 | func | `(b *Batch).GetByteSize` | `() ((int, error))` |
| 21960 | 21965 | type | `singleSourceIterator` | `struct{4 fields}` |
| 21967 | 21976 | func | `newSingleSourceIterator` | `(iter iterator.UnsafeIterator, start, end []byte) (merging.Iterator)` |
| 21978 | 21991 | func | `(it *singleSourceIterator).advance` | `() ()` |
| 21993 | 21999 | func | `(it *singleSourceIterator).Next` | `() ()` |
| 22001 | 22006 | func | `(it *singleSourceIterator).UnsafeKey` | `() ([]byte)` |
| 22008 | 22013 | func | `(it *singleSourceIterator).UnsafeValue` | `() ([]byte)` |
| 22015 | 22015 | func | `(it *singleSourceIterator).Valid` | `() (bool)` |
| 22016 | 22016 | func | `(it *singleSourceIterator).Key` | `() ([]byte)` |
| 22017 | 22017 | func | `(it *singleSourceIterator).Value` | `() ([]byte)` |
| 22018 | 22018 | func | `(it *singleSourceIterator).KeyCopy` | `(dst []byte) ([]byte)` |
| 22019 | 22021 | func | `(it *singleSourceIterator).ValueCopy` | `(dst []byte) ([]byte)` |
| 22022 | 22022 | func | `(it *singleSourceIterator).Close` | `() (error)` |
| 22023 | 22023 | func | `(it *singleSourceIterator).Error` | `() (error)` |
| 22024 | 22024 | func | `(it *singleSourceIterator).Domain` | `() (([]byte, []byte))` |
| 22027 | 22029 | type | `emptyIterator` | `struct{2 fields}` |
| 22031 | 22031 | func | `(it *emptyIterator).Next` | `() ()` |
| 22032 | 22032 | func | `(it *emptyIterator).Valid` | `() (bool)` |
| 22033 | 22033 | func | `(it *emptyIterator).Key` | `() ([]byte)` |
| 22034 | 22034 | func | `(it *emptyIterator).Value` | `() ([]byte)` |
| 22035 | 22035 | func | `(it *emptyIterator).KeyCopy` | `(_ []byte) ([]byte)` |
| 22036 | 22036 | func | `(it *emptyIterator).ValueCopy` | `(_ []byte) ([]byte)` |
| 22037 | 22037 | func | `(it *emptyIterator).Close` | `() (error)` |
| 22038 | 22038 | func | `(it *emptyIterator).Error` | `() (error)` |
| 22039 | 22039 | func | `(it *emptyIterator).Domain` | `() (([]byte, []byte))` |
