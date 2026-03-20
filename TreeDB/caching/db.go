package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var errDBClosing = errors.New("cachingdb: db closing")

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")
var ErrUnsafeOptions = fmt.Errorf("unsafe options require AllowUnsafe")
var ErrMemtableFull = fmt.Errorf("memtable full")
var errWALClosed = errors.New("cachingdb: wal writer closed")
var errWALUnavailable = errors.New("cachingdb: wal unavailable")

var iteratorDebugEnabled atomic.Bool

var valueLogEligiblePool sync.Pool                  // stores []int
var valueLogRecordPool sync.Pool                    // stores []valuelog.Record
var valueLogKeyPool sync.Pool                       // stores [][]byte
var valueLogPtrPool sync.Pool                       // stores []page.ValuePtr
var batchArenaPools [batchArenaClassCount]sync.Pool // stores []byte
var batchArenaLeasePool sync.Pool                   // stores *batchArenaLease
var appendOnlyDirectValueArenaPools [appendOnlyDirectValueArenaClassCount]sync.Pool
var batchEntrySliceRefPool sync.Pool                        // stores *batchEntrySliceRef
var outerLeafArenaPools [outerLeafArenaClassCount]sync.Pool // stores []byte
var outerLeafArenaLeaseMu sync.Mutex
var outerLeafArenaLeases [outerLeafArenaClassCount][][]byte
var outerLeafBlobRefScratchPool sync.Pool
var outerLeafEncoderPool sync.Pool // stores *outerleaf.Encoder
var valueLogPreparedBodyPool sync.Pool
var valueLogPreparedFramesPool sync.Pool     // stores []preparedDictFrame
var valueLogDictPrepareResultsPool sync.Pool // stores chan vlogDictPrepareResult
var valueLogKeyLeaseMu sync.Mutex
var valueLogKeyLeases [][][]byte

// Batch arena pooling can retain substantial heap across restore spikes. Track
// pooled bytes and enforce a byte-budget to cap retention.
var batchArenaPoolBytes atomic.Int64
var batchArenaPoolBytesMaxGlobal atomic.Int64
var batchArenaPoolLastGC atomic.Uint64
var batchArenaPoolBudgetState atomic.Value
var batchArenaLeasedBytesGlobal atomic.Int64
var batchArenaLeasedBytesMaxGlobal atomic.Int64
var batchArenaInFlightBytes atomic.Int64
var batchArenaInFlightBytesMaxGlobal atomic.Int64
var batchArenaRetainedBytesMaxGlobal atomic.Int64
var batchArenaRetainedHardCapOverride atomic.Int64
var batchArenaPoolSkipZeroBudgetTotal atomic.Uint64
var batchArenaPoolDropBytesTotal atomic.Uint64
var batchArenaPoolDropHardCapBytesTotal atomic.Uint64
var batchArenaBorrowBlockedTotal atomic.Uint64
var batchArenaBorrowPreflightBlockedTotal atomic.Uint64
var batchArenaBorrowPreflightBlockedBytesTotal atomic.Uint64
var batchArenaStealSuppressedDeferredTotal atomic.Uint64
var batchArenaStealSuppressedDeferredEntriesTotal atomic.Uint64
var poolPressureState atomic.Value
var poolPressureMu sync.Mutex
var poolPressureLastLeaseTrimUnixNano atomic.Int64
var poolPressureNormalSamplesTotal atomic.Uint64
var poolPressureHighSamplesTotal atomic.Uint64
var poolPressureCriticalSamplesTotal atomic.Uint64
var entrySlicePoolTrimRunsTotal atomic.Uint64
var entrySlicePoolTrimDropBytesTotal atomic.Uint64
var entrySliceLeaseHitTotal atomic.Uint64
var entrySliceLeaseHitBytesTotal atomic.Uint64
var entrySlicePoolHitTotal atomic.Uint64
var entrySlicePoolHitBytesTotal atomic.Uint64
var entrySliceFreshAllocTotal atomic.Uint64
var entrySliceFreshAllocBytesTotal atomic.Uint64
var entrySlicePutLeaseTotal atomic.Uint64
var entrySlicePutLeaseBytesTotal atomic.Uint64
var entrySlicePutPoolTotal atomic.Uint64
var entrySlicePutPoolBytesTotal atomic.Uint64
var entrySlicePutDropBudgetTotal atomic.Uint64
var entrySlicePutDropBudgetBytesTotal atomic.Uint64
var flushMergeShadowedOpsTotal atomic.Uint64
var flushMergeAppliedOpsTotal atomic.Uint64
var flushMergeDeferredShadowedOpsTotal atomic.Uint64
var flushMergeDeferredAppliedOpsTotal atomic.Uint64
var flushMergeParallelShadowedOpsTotal atomic.Uint64
var flushMergeParallelAppliedOpsTotal atomic.Uint64
var batchEntriesPoolDropUnderPressureTotal atomic.Uint64
var batchShardEntriesPoolDropUnderPressureTotal atomic.Uint64
var batchIntPoolDropUnderPressureTotal atomic.Uint64
var appendOnlyDirectArenaPoolHitChunksTotal atomic.Uint64
var appendOnlyDirectArenaPoolHitBytesTotal atomic.Uint64
var appendOnlyDirectArenaRetainedHitChunksTotal atomic.Uint64
var appendOnlyDirectArenaRetainedHitBytesTotal atomic.Uint64
var appendOnlyDirectArenaFreshAllocChunksTotal atomic.Uint64
var appendOnlyDirectArenaFreshAllocBytesTotal atomic.Uint64
var appendOnlyMemNewAllocWithQueueTotal atomic.Uint64
var appendOnlyMemNewAllocQueueBytesSum atomic.Uint64

var poolPressureNow = time.Now
var poolPressureReadMemStats = runtime.ReadMemStats
var poolPressureMemoryLimit = func() int64 {
	return debug.SetMemoryLimit(-1)
}

var runtimeNumGC = func() uint64 {
	samples := []metrics.Sample{{Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		return samples[0].Value.Uint64()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return uint64(ms.NumGC)
}
var batchArenaPoolNumGC = runtimeNumGC
var entrySlicePoolNumGC = runtimeNumGC

type poolPressureLevel uint8

const (
	poolPressureNormal poolPressureLevel = iota
	poolPressureHigh
	poolPressureCritical
)

type poolPressureSnapshot struct {
	sampledUnixNano    int64
	level              poolPressureLevel
	usedBytes          uint64
	heapAllocBytes     uint64
	heapInuseBytes     uint64
	heapSysBytes       uint64
	heapIdleBytes      uint64
	heapReleasedBytes  uint64
	heapIdleUnreleased uint64
	stackInuseBytes    uint64
	stackSysBytes      uint64
	nextGCBytes        uint64
	numGC              uint32
	gcCPUFraction      float64
	totalSysBytes      uint64
	nonHeapSysBytes    uint64
	memoryLimitBytes   int64
}

const (
	poolPressureRefreshInterval = 250 * time.Millisecond
	// Treat high/critical pressure as heap residency (not alloc churn). These
	// thresholds target restore-like peaks where pool retention can inflate RSS.
	poolPressureHighHeapBytes     = uint64(4 << 30)
	poolPressureCriticalHeapBytes = uint64(8 << 30)
	poolPressureTrimInterval      = 2 * time.Second
	poolPressureHighBudgetDivisor = int64(2)
	// Under heap pressure, reduce mutable flush threshold so we rotate/flush
	// earlier and cap memtable residency growth during restore-like ingest.
	mutableFlushThresholdHighPressureDivisor     = int64(2)
	mutableFlushThresholdCriticalPressureDivisor = int64(4)
	mutableFlushThresholdPressureFloorBytes      = int64(32 << 20)
	// Under heap pressure, aggressively cap fresh batch-copy chunk sizing. Keep
	// large single-copy writes functional by allowing request-size override.
	batchCopyArenaHighPressureMaxChunk     = 512 << 10
	batchCopyArenaCriticalPressureMaxChunk = 128 << 10
	// Direct append-only arena retention is best-effort reuse. Shrink retained
	// headroom materially under pressure so reuse does not dominate inuse peaks.
	appendOnlyDirectArenaHighPressureDivisor = int64(4)
	// Keep total live batch-arena retention (pool + active memtable leases) bounded.
	// We derive this from the pool budget so it scales with machine parallelism.
	batchArenaRetainedHardCapMultiplier = int64(2)
	// Regular flushes use a soft trim cadence; checkpoints perform an immediate
	// stricter trim pass because they are already explicit maintenance boundaries.
	flushRetainedArenaTrimMinInterval = 2 * time.Second
	// Keep post-flush pool footprints bounded without forcing full cold-start.
	postFlushBatchArenaTargetBytes = int64(64 << 20)
	postFlushEntrySliceTargetBytes = int64(64 << 20)
	// Checkpoint trim is intentionally stricter.
	postCheckpointBatchArenaTargetBytes        = int64(32 << 20)
	postCheckpointEntrySliceTargetBytes        = int64(32 << 20)
	postFlushEntrySliceLeaseKeepPerBucket      = 16
	postCheckpointEntrySliceLeaseKeepPerBucket = 8
	postFlushAppendOnlyMemLeaseKeep            = 24
	postCheckpointAppendOnlyMemLeaseKeep       = 8
)

func computeBatchArenaPoolBudgetBytes() int64 {
	return computeBatchArenaPoolBudgetBytesForProcs(runtime.GOMAXPROCS(0))
}

func computeBatchArenaPoolBudgetBytesForProcs(procs int) int64 {
	// Keep a few max-size chunks per P to avoid thrash while preventing runaway
	// retention during restore workloads.
	const maxChunksPerP = 4
	const maxBudgetBytes = int64(128 << 20)
	const perPBytes = int64(batchCopyArenaMaxRetain) * maxChunksPerP
	if procs < 1 {
		procs = 1
	}
	maxProcs := maxBudgetBytes / perPBytes
	if maxProcs < 1 {
		maxProcs = 1
	}
	if int64(procs) > maxProcs {
		procs = int(maxProcs)
	}
	budget := int64(procs) * perPBytes
	// Ensure we can pool at least a few chunks even on single-core runs.
	minBudget := perPBytes
	if budget < minBudget {
		budget = minBudget
	}
	if budget > maxBudgetBytes {
		budget = maxBudgetBytes
	}
	return budget
}

func classifyPoolPressureLevel(usedBytes uint64, memoryLimitBytes int64) poolPressureLevel {
	high := poolPressureHighHeapBytes
	critical := poolPressureCriticalHeapBytes

	if memoryLimitBytes > 0 {
		limit := uint64(memoryLimitBytes)
		if limit > 0 {
			limitHigh := (limit * 70) / 100
			limitCritical := (limit * 85) / 100
			if limitHigh > 0 && limitHigh < high {
				high = limitHigh
			}
			if limitCritical > 0 && limitCritical < critical {
				critical = limitCritical
			}
			if critical < high {
				critical = high
			}
		}
	}

	if usedBytes >= critical {
		return poolPressureCritical
	}
	if usedBytes >= high {
		return poolPressureHigh
	}
	return poolPressureNormal
}

func poolPressureUsedBytes(ms runtime.MemStats, heapIdleUnreleased uint64) uint64 {
	used := ms.HeapInuse
	if ms.HeapAlloc > used {
		used = ms.HeapAlloc
	}
	if heapIdleUnreleased > 0 {
		if used > math.MaxUint64-heapIdleUnreleased {
			return math.MaxUint64
		}
		used += heapIdleUnreleased
	}
	return used
}

func samplePoolPressureSnapshot(sampledAt time.Time) poolPressureSnapshot {
	var ms runtime.MemStats
	poolPressureReadMemStats(&ms)
	heapIdleUnreleased := uint64(0)
	if ms.HeapIdle > ms.HeapReleased {
		heapIdleUnreleased = ms.HeapIdle - ms.HeapReleased
	}
	used := poolPressureUsedBytes(ms, heapIdleUnreleased)
	memLimit := poolPressureMemoryLimit()
	level := classifyPoolPressureLevel(used, memLimit)
	nonHeapSysBytes := uint64(0)
	if ms.Sys > ms.HeapSys {
		nonHeapSysBytes = ms.Sys - ms.HeapSys
	}
	return poolPressureSnapshot{
		sampledUnixNano:    sampledAt.UnixNano(),
		level:              level,
		usedBytes:          used,
		heapAllocBytes:     ms.HeapAlloc,
		heapInuseBytes:     ms.HeapInuse,
		heapSysBytes:       ms.HeapSys,
		heapIdleBytes:      ms.HeapIdle,
		heapReleasedBytes:  ms.HeapReleased,
		heapIdleUnreleased: heapIdleUnreleased,
		stackInuseBytes:    ms.StackInuse,
		stackSysBytes:      ms.StackSys,
		nextGCBytes:        ms.NextGC,
		numGC:              ms.NumGC,
		gcCPUFraction:      ms.GCCPUFraction,
		totalSysBytes:      ms.Sys,
		nonHeapSysBytes:    nonHeapSysBytes,
		memoryLimitBytes:   memLimit,
	}
}

func trimEntrySliceLeasesToKeep(keepPerBucket int) int64 {
	if keepPerBucket < 0 {
		keepPerBucket = 0
	}
	var droppedBytes int64
	entrySliceLeaseMu.Lock()
	for i := range entrySliceLeases {
		leases := entrySliceLeases[i]
		if len(leases) > keepPerBucket {
			drop := len(leases) - keepPerBucket
			for j := 0; j < drop; j++ {
				if entries := leases[j]; entries != nil {
					droppedBytes += int64(cap(entries)) * entrySliceEntrySizeBytes
				}
				leases[j] = nil
			}
			if keepPerBucket == 0 {
				entrySliceLeases[i] = nil
			} else {
				entrySliceLeases[i] = leases[drop:]
			}
		}
	}
	entrySliceLeaseMu.Unlock()
	if droppedBytes > 0 {
		releaseEntrySlicePoolBytes(droppedBytes)
	}
	return droppedBytes
}

func maybeTrimEntrySliceLeasesUnderPressure(level poolPressureLevel, sampledAt time.Time) {
	if level == poolPressureNormal {
		return
	}
	nowUnix := sampledAt.UnixNano()
	last := poolPressureLastLeaseTrimUnixNano.Load()
	if last != 0 && nowUnix-last < int64(poolPressureTrimInterval) {
		return
	}
	if !poolPressureLastLeaseTrimUnixNano.CompareAndSwap(last, nowUnix) {
		return
	}

	keepPerBucket := maxEntrySliceLeasesPerBucket
	switch level {
	case poolPressureCritical:
		keepPerBucket = 0
	case poolPressureHigh:
		keepPerBucket = maxEntrySliceLeasesPerBucket / 8
		if keepPerBucket < 2 {
			keepPerBucket = 2
		}
	}
	droppedBytes := trimEntrySliceLeasesToKeep(keepPerBucket)
	entrySlicePoolTrimRunsTotal.Add(1)
	if droppedBytes > 0 {
		entrySlicePoolTrimDropBytesTotal.Add(uint64(droppedBytes))
	}
}

func currentPoolPressureSnapshot() poolPressureSnapshot {
	now := poolPressureNow()
	if cached, ok := poolPressureState.Load().(poolPressureSnapshot); ok {
		if now.UnixNano()-cached.sampledUnixNano <= int64(poolPressureRefreshInterval) {
			return cached
		}
	}

	poolPressureMu.Lock()
	defer poolPressureMu.Unlock()

	now = poolPressureNow()
	if cached, ok := poolPressureState.Load().(poolPressureSnapshot); ok {
		if now.UnixNano()-cached.sampledUnixNano <= int64(poolPressureRefreshInterval) {
			return cached
		}
	}

	snap := samplePoolPressureSnapshot(now)
	poolPressureState.Store(snap)
	switch snap.level {
	case poolPressureCritical:
		poolPressureCriticalSamplesTotal.Add(1)
	case poolPressureHigh:
		poolPressureHighSamplesTotal.Add(1)
	default:
		poolPressureNormalSamplesTotal.Add(1)
	}
	maybeTrimEntrySliceLeasesUnderPressure(snap.level, now)
	return snap
}

func currentZipperParallelMergePressure() zipper.ParallelMergePressureLevel {
	switch currentPoolPressureSnapshot().level {
	case poolPressureCritical:
		return zipper.ParallelMergePressureCritical
	case poolPressureHigh:
		return zipper.ParallelMergePressureHigh
	default:
		return zipper.ParallelMergePressureNormal
	}
}

func scalePoolBudgetForPressure(base int64, level poolPressureLevel) int64 {
	if base <= 0 {
		return 0
	}
	switch level {
	case poolPressureCritical:
		return 0
	case poolPressureHigh:
		return base / poolPressureHighBudgetDivisor
	default:
		return base
	}
}

func currentBatchArenaRetentionBudgetBytes() int64 {
	base := currentBatchArenaPoolBudgetBytes()
	level := currentPoolPressureSnapshot().level
	return scalePoolBudgetForPressure(base, level)
}

func currentBatchArenaRetainedHardCapBytes() int64 {
	if override := batchArenaRetainedHardCapOverride.Load(); override > 0 {
		return override
	}
	base := currentBatchArenaPoolBudgetBytes()
	if base <= 0 {
		return 0
	}
	hardCap := base * batchArenaRetainedHardCapMultiplier
	if hardCap < int64(batchCopyArenaMinChunk) {
		hardCap = int64(batchCopyArenaMinChunk)
	}
	level := currentPoolPressureSnapshot().level
	hardCap = scalePoolBudgetForPressure(hardCap, level)
	if hardCap < 0 {
		hardCap = 0
	}
	return hardCap
}

func (db *DB) batchArenaDeferredPressureActive() bool {
	return db != nil && db.memtableViewTelemetry.deferredBytesCurrent.Load() >= batchArenaDeferredPressureThresholdBytes
}

func (db *DB) currentBatchArenaRetainedHardCapEffectiveBytes() int64 {
	hardCap := currentBatchArenaRetainedHardCapBytes()
	if hardCap <= 0 || !db.batchArenaDeferredPressureActive() {
		return hardCap
	}
	reduced := hardCap / batchArenaDeferredPressureHardCapDivisor
	minCap := int64(batchCopyArenaMinChunk)
	if reduced < minCap {
		reduced = minCap
	}
	if reduced > hardCap {
		return hardCap
	}
	return reduced
}

func currentBatchArenaRetainedBytesEstimate() int64 {
	poolBytes := batchArenaPoolBytes.Load()
	if poolBytes < 0 {
		poolBytes = 0
	}
	leasedBytes := batchArenaLeasedBytesGlobal.Load()
	if leasedBytes < 0 {
		leasedBytes = 0
	}
	return poolBytes + leasedBytes
}

func noteBatchArenaRetainedBytesMax() {
	total := currentBatchArenaRetainedBytesEstimate()
	if total <= 0 {
		return
	}
	updateInt64Max(&batchArenaRetainedBytesMaxGlobal, total)
}

func noteBatchArenaPoolBytesMax(value int64) {
	if value <= 0 {
		return
	}
	updateInt64Max(&batchArenaPoolBytesMaxGlobal, value)
}

func noteBatchArenaLeasedBytesGlobalMax(value int64) {
	if value <= 0 {
		return
	}
	updateInt64Max(&batchArenaLeasedBytesMaxGlobal, value)
}

func noteBatchArenaInFlightBytesMax(value int64) {
	if value <= 0 {
		return
	}
	updateInt64Max(&batchArenaInFlightBytesMaxGlobal, value)
}

func shouldBorrowBatchArenaBytes() bool {
	hardCap := currentBatchArenaRetainedHardCapBytes()
	if hardCap <= 0 {
		return false
	}
	return currentBatchArenaRetainedBytesEstimate() < hardCap
}

func shouldBorrowBatchArenaBytesForWrite(prospectiveRetainBytes int64) (allow bool, preflightBlocked bool) {
	return shouldBorrowBatchArenaBytesForWriteWithHardCap(prospectiveRetainBytes, currentBatchArenaRetainedHardCapBytes())
}

func shouldBorrowBatchArenaBytesForWriteWithHardCap(prospectiveRetainBytes int64, hardCap int64) (allow bool, preflightBlocked bool) {
	if hardCap <= 0 {
		return false, false
	}
	currentRetained := currentBatchArenaRetainedBytesEstimate()
	if currentRetained >= hardCap {
		return false, false
	}
	if prospectiveRetainBytes <= 0 {
		return true, false
	}
	if prospectiveRetainBytes >= hardCap || currentRetained > math.MaxInt64-prospectiveRetainBytes {
		return false, true
	}
	if currentRetained+prospectiveRetainBytes > hardCap {
		return false, true
	}
	return true, false
}

func batchArenaChunksCapBytes(chunks [][]byte) int64 {
	var total int64
	for i := range chunks {
		c := cap(chunks[i])
		if c <= 0 {
			continue
		}
		if total > math.MaxInt64-int64(c) {
			return math.MaxInt64
		}
		total += int64(c)
	}
	return total
}

func currentEntrySlicePoolBudgetBytes() int64 {
	base := entrySlicePoolBudgetBytes
	level := currentPoolPressureSnapshot().level
	return scalePoolBudgetForPressure(base, level)
}

func scaleMutableFlushThresholdForPressure(base int64, level poolPressureLevel) int64 {
	if base <= 0 {
		return base
	}
	switch level {
	case poolPressureCritical:
		scaled := base / mutableFlushThresholdCriticalPressureDivisor
		if scaled <= 0 {
			scaled = 1
		}
		if base > mutableFlushThresholdPressureFloorBytes && scaled < mutableFlushThresholdPressureFloorBytes {
			scaled = mutableFlushThresholdPressureFloorBytes
		}
		if scaled > base {
			return base
		}
		return scaled
	case poolPressureHigh:
		scaled := base / mutableFlushThresholdHighPressureDivisor
		if scaled <= 0 {
			scaled = 1
		}
		if base > mutableFlushThresholdPressureFloorBytes && scaled < mutableFlushThresholdPressureFloorBytes {
			scaled = mutableFlushThresholdPressureFloorBytes
		}
		if scaled > base {
			return base
		}
		return scaled
	default:
		return base
	}
}

func batchCopyArenaMaxChunkForPressure(level poolPressureLevel) int {
	switch level {
	case poolPressureCritical:
		return batchCopyArenaCriticalPressureMaxChunk
	case poolPressureHigh:
		return batchCopyArenaHighPressureMaxChunk
	default:
		return batchCopyArenaMaxRetain
	}
}

func currentBatchCopyArenaMaxChunk() int {
	return batchCopyArenaMaxChunkForPressure(currentPoolPressureSnapshot().level)
}

func shouldRetainBatchAuxPoolEntries(level poolPressureLevel) bool {
	return level == poolPressureNormal
}

func appendOnlyDirectArenaRetentionLimitsForPressure(level poolPressureLevel) (maxChunks int, maxBytes int64) {
	switch level {
	case poolPressureCritical:
		return 0, 0
	case poolPressureHigh:
		maxBytes = int64(appendOnlyDirectValueArenaRetainMaxBytes) / appendOnlyDirectArenaHighPressureDivisor
		if maxBytes < appendOnlyDirectValueArenaDefaultChunk {
			maxBytes = appendOnlyDirectValueArenaDefaultChunk
		}
	default:
		maxBytes = int64(appendOnlyDirectValueArenaRetainMaxBytes)
	}
	maxChunks = int(maxBytes) / appendOnlyDirectValueArenaDefaultChunk
	if maxChunks < 1 {
		maxChunks = 1
	}
	if maxChunks > appendOnlyDirectValueArenaRetainMaxChunks {
		maxChunks = appendOnlyDirectValueArenaRetainMaxChunks
	}
	return maxChunks, maxBytes
}

func poolPressureLevelString(level poolPressureLevel) string {
	switch level {
	case poolPressureCritical:
		return "critical"
	case poolPressureHigh:
		return "high"
	default:
		return "normal"
	}
}

func currentBatchArenaPoolBudgetBytes() int64 {
	procs := runtime.GOMAXPROCS(0)
	if procs < 1 {
		procs = 1
	}
	if cached, _ := batchArenaPoolBudgetState.Load().(batchArenaPoolBudgetCache); cached.procs == int32(procs) {
		if budget := cached.budget; budget > 0 {
			return budget
		}
	}
	budget := computeBatchArenaPoolBudgetBytesForProcs(procs)
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{procs: int32(procs), budget: budget})
	return budget
}

func maybeResetBatchArenaPoolBytesAfterGC() {
	if batchArenaPoolBytes.Load() <= 0 {
		return
	}
	numGC := batchArenaPoolNumGC()
	last := batchArenaPoolLastGC.Load()
	if last == numGC {
		return
	}
	if last == 0 {
		if batchArenaPoolLastGC.CompareAndSwap(0, numGC) {
			batchArenaPoolBytes.Store(0)
		}
		return
	}
	if batchArenaPoolLastGC.CompareAndSwap(last, numGC) {
		batchArenaPoolBytes.Store(0)
	}
}

type batchArenaPoolBudgetCache struct {
	procs  int32
	budget int64
}

func init() {
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	poolPressureState.Store(poolPressureSnapshot{})
}

func noteBatchArenaPoolGC(numGC uint64) {
	if numGC == 0 {
		return
	}
	for {
		last := batchArenaPoolLastGC.Load()
		if last >= numGC {
			return
		}
		if batchArenaPoolLastGC.CompareAndSwap(last, numGC) {
			return
		}
	}
}

const (
	maxValueLogKeyLeaseCount = 64
	maxValueLogKeyLeaseCap   = 1 << 20
	batchArenaMinShift       = 12
	// batch arenas are chunked key/value copy buffers that may be leased to
	// memtables. Keep the max pooled chunk size modest to avoid retaining large
	// mostly-empty tail chunks during big restore batches.
	batchArenaMaxShift                       = 21
	batchArenaClassCount                     = batchArenaMaxShift - batchArenaMinShift + 1
	appendOnlyDirectValueArenaMinShift       = 15
	appendOnlyDirectValueArenaMaxShift       = 20
	appendOnlyDirectValueArenaClassCount     = appendOnlyDirectValueArenaMaxShift - appendOnlyDirectValueArenaMinShift + 1
	appendOnlyDirectValueArenaDefaultChunk   = 32 << 10
	appendOnlyDirectValueArenaPoolMaxCap     = 1 << appendOnlyDirectValueArenaMaxShift
	appendOnlyDirectValueArenaRetainMaxBytes = 8 << 20
	// Keep chunk count aligned with the byte cap for the common default-chunk
	// case; otherwise a low chunk cap can silently reduce effective retention to
	// ~1MB and force unnecessary regrowth allocations each memtable cycle.
	appendOnlyDirectValueArenaRetainMaxChunks = appendOnlyDirectValueArenaRetainMaxBytes / appendOnlyDirectValueArenaDefaultChunk
	outerLeafArenaMinShift                    = 12
	outerLeafArenaMaxShift                    = 24
	outerLeafArenaClassCount                  = outerLeafArenaMaxShift - outerLeafArenaMinShift + 1
	maxOuterLeafArenaLeases                   = 64
)

type vlogPreparedFrameBody struct {
	buf []byte
}

type batchEntrySliceRef struct {
	entries []batch.Entry
}

func getBatchEntrySliceRef(entries []batch.Entry) *batchEntrySliceRef {
	if v := batchEntrySliceRefPool.Get(); v != nil {
		if ref, ok := v.(*batchEntrySliceRef); ok {
			ref.entries = entries
			return ref
		}
	}
	return &batchEntrySliceRef{entries: entries}
}

func putBatchEntrySliceRef(ref *batchEntrySliceRef) {
	if ref == nil {
		return
	}
	ref.entries = nil
	batchEntrySliceRefPool.Put(ref)
}

func getValueLogEligible(capacity int) []int {
	if capacity < 0 {
		capacity = 0
	}
	if v := valueLogEligiblePool.Get(); v != nil {
		if s, ok := v.([]int); ok {
			if cap(s) >= capacity {
				return s[:0]
			}
		}
	}
	return make([]int, 0, capacity)
}

func putValueLogEligible(s []int) {
	if s == nil {
		return
	}
	// Avoid retaining huge slices in the pool.
	if cap(s) > 1<<20 {
		return
	}
	valueLogEligiblePool.Put(s[:0])
}

func getValueLogRecords(n int) []valuelog.Record {
	if n < 0 {
		n = 0
	}
	if v := valueLogRecordPool.Get(); v != nil {
		if s, ok := v.([]valuelog.Record); ok {
			if cap(s) >= n {
				return s[:n]
			}
		}
	}
	return make([]valuelog.Record, n)
}

func getValueLogRecordsCap(capacity int) []valuelog.Record {
	if capacity < 0 {
		capacity = 0
	}
	if v := valueLogRecordPool.Get(); v != nil {
		if s, ok := v.([]valuelog.Record); ok {
			if cap(s) >= capacity {
				return s[:0]
			}
		}
	}
	return make([]valuelog.Record, 0, capacity)
}

func putValueLogRecords(s []valuelog.Record) {
	if s == nil {
		return
	}
	for i := range s {
		s[i] = valuelog.Record{}
	}
	// Avoid retaining huge slices in the pool.
	if cap(s) > 1<<20 {
		return
	}
	valueLogRecordPool.Put(s[:0])
}

func clearValueLogRecordValues(s []valuelog.Record) {
	for i := range s {
		// Drop value references before pooling to avoid retaining large backing
		// arrays when callers provide subslices/views.
		s[i].Value = nil
	}
}

func putValueLogRecordsNoClear(s []valuelog.Record) {
	if s == nil {
		return
	}
	// Avoid O(cap) clearing work for oversized slices that we intentionally
	// drop instead of returning to the pool.
	if cap(s) > 1<<20 {
		return
	}
	records := s
	if cap(records) > len(records) {
		records = records[:cap(records)]
	}
	clearValueLogRecordValues(records)
	valueLogRecordPool.Put(s[:0])
}

func outerLeafArenaClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxOuterLeafArenaPoolCap {
		return 0, 0, false
	}
	minCap := 1 << outerLeafArenaMinShift
	if capacity <= minCap {
		return 0, minCap, true
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > maxOuterLeafArenaPoolCap {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - outerLeafArenaMinShift
	if idx < 0 || idx >= outerLeafArenaClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func outerLeafArenaClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << outerLeafArenaMinShift
	if capacity < minCap || capacity > maxOuterLeafArenaPoolCap {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - outerLeafArenaMinShift
	if idx < 0 || idx >= outerLeafArenaClassCount {
		return 0, false
	}
	return idx, true
}

func outerLeafArenaMaxReuseCap(capacity int) int {
	if capacity <= 0 {
		return 1 << outerLeafArenaMinShift
	}
	// Clamp before multiplication to avoid potential integer overflow.
	if capacity > maxOuterLeafArenaPoolCap/8 {
		return maxOuterLeafArenaPoolCap
	}
	maxCap := capacity * 8
	if maxCap < 1<<20 {
		maxCap = 1 << 20
	}
	if maxCap > maxOuterLeafArenaPoolCap {
		maxCap = maxOuterLeafArenaPoolCap
	}
	return maxCap
}

func batchArenaClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > batchCopyArenaMaxRetain {
		return 0, 0, false
	}
	minCap := 1 << batchArenaMinShift
	if capacity <= minCap {
		return 0, minCap, true
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > batchCopyArenaMaxRetain {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - batchArenaMinShift
	if idx < 0 || idx >= batchArenaClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func batchArenaClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << batchArenaMinShift
	if capacity < minCap || capacity > batchCopyArenaMaxRetain {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - batchArenaMinShift
	if idx < 0 || idx >= batchArenaClassCount {
		return 0, false
	}
	return idx, true
}

func getBatchArena(capacity int) []byte {
	if capacity < 0 {
		capacity = 0
	}
	idx, classCap, ok := batchArenaClassForLen(capacity)
	if !ok {
		return make([]byte, 0, capacity)
	}
	if v := batchArenaPools[idx].Get(); v != nil {
		if buf, ok := v.([]byte); ok {
			size := int64(cap(buf))
			next := batchArenaPoolBytes.Add(-size)
			if next < 0 {
				// Counter can drift if sync.Pool drops objects at GC or a caller
				// inserts an unexpected buffer.
				batchArenaPoolBytes.Store(0)
			}
			if cap(buf) == classCap {
				return buf[:0]
			}
		}
	}
	maybeResetBatchArenaPoolBytesAfterGC()
	return make([]byte, 0, classCap)
}

func putBatchArena(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) > batchCopyArenaMaxRetain {
		return
	}
	idx, ok := batchArenaClassForCap(cap(buf))
	if !ok {
		return
	}
	budget := currentBatchArenaRetentionBudgetBytes()
	if budget <= 0 {
		batchArenaPoolSkipZeroBudgetTotal.Add(1)
		return
	}
	hardCap := currentBatchArenaRetainedHardCapBytes()
	size := int64(cap(buf))
	noteEpoch := false
	for {
		held := batchArenaPoolBytes.Load()
		if held+size > budget {
			before := held
			maybeResetBatchArenaPoolBytesAfterGC()
			held = batchArenaPoolBytes.Load()
			if held == before || held+size > budget {
				batchArenaPoolDropBytesTotal.Add(uint64(size))
				return
			}
			continue
		}
		if hardCap > 0 {
			leased := batchArenaLeasedBytesGlobal.Load()
			if leased < 0 {
				leased = 0
			}
			if held+leased+size > hardCap {
				batchArenaPoolDropBytesTotal.Add(uint64(size))
				batchArenaPoolDropHardCapBytesTotal.Add(uint64(size))
				return
			}
		}
		if batchArenaPoolBytes.CompareAndSwap(held, held+size) {
			noteEpoch = held == 0
			noteBatchArenaPoolBytesMax(held + size)
			noteBatchArenaRetainedBytesMax()
			break
		}
	}
	if noteEpoch {
		noteBatchArenaPoolGC(batchArenaPoolNumGC())
	}
	batchArenaPools[idx].Put(buf[:0])
}

func putBatchArenas(chunks [][]byte) {
	for i := range chunks {
		if chunks[i] != nil {
			putBatchArena(chunks[i])
			chunks[i] = nil
		}
	}
}

func drainBatchArenaPoolToTargetBytes(target int64) int64 {
	if target < 0 {
		target = 0
	}
	if batchArenaPoolBytes.Load() <= target {
		return 0
	}
	var dropped int64
	for classIdx := len(batchArenaPools) - 1; classIdx >= 0 && batchArenaPoolBytes.Load() > target; classIdx-- {
		for batchArenaPoolBytes.Load() > target {
			v := batchArenaPools[classIdx].Get()
			if v == nil {
				break
			}
			buf, ok := v.([]byte)
			if !ok {
				continue
			}
			size := int64(cap(buf))
			if size <= 0 {
				continue
			}
			dropped += size
			next := batchArenaPoolBytes.Add(-size)
			if next < 0 {
				batchArenaPoolBytes.Store(0)
			}
		}
	}
	if dropped > 0 {
		batchArenaPoolDropBytesTotal.Add(uint64(dropped))
	}
	return dropped
}

func appendOnlyDirectValueArenaClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity <= 0 || capacity > appendOnlyDirectValueArenaPoolMaxCap {
		return 0, 0, false
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	minCap := 1 << appendOnlyDirectValueArenaMinShift
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > appendOnlyDirectValueArenaPoolMaxCap {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - appendOnlyDirectValueArenaMinShift
	if idx < 0 || idx >= appendOnlyDirectValueArenaClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func appendOnlyDirectValueArenaClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << appendOnlyDirectValueArenaMinShift
	if capacity < minCap || capacity > appendOnlyDirectValueArenaPoolMaxCap {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - appendOnlyDirectValueArenaMinShift
	if idx < 0 || idx >= appendOnlyDirectValueArenaClassCount {
		return 0, false
	}
	return idx, true
}

func getAppendOnlyDirectValueArenaChunk(capacity int) []byte {
	if capacity <= 0 {
		capacity = appendOnlyDirectValueArenaDefaultChunk
	}
	if capacity < appendOnlyDirectValueArenaDefaultChunk {
		capacity = appendOnlyDirectValueArenaDefaultChunk
	}
	if idx, classCap, ok := appendOnlyDirectValueArenaClassForLen(capacity); ok {
		if v := appendOnlyDirectValueArenaPools[idx].Get(); v != nil {
			if b, ok := v.([]byte); ok && cap(b) >= classCap {
				appendOnlyDirectArenaPoolHitChunksTotal.Add(1)
				appendOnlyDirectArenaPoolHitBytesTotal.Add(uint64(cap(b)))
				return b[:0]
			}
		}
		appendOnlyDirectArenaFreshAllocChunksTotal.Add(1)
		appendOnlyDirectArenaFreshAllocBytesTotal.Add(uint64(classCap))
		return make([]byte, 0, classCap)
	}
	appendOnlyDirectArenaFreshAllocChunksTotal.Add(1)
	appendOnlyDirectArenaFreshAllocBytesTotal.Add(uint64(capacity))
	return make([]byte, 0, capacity)
}

func putAppendOnlyDirectValueArenaChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	if idx, ok := appendOnlyDirectValueArenaClassForCap(cap(chunk)); ok {
		appendOnlyDirectValueArenaPools[idx].Put(chunk[:0])
	}
}

func putAppendOnlyDirectValueArenaChunks(chunks [][]byte) {
	for i := range chunks {
		if chunks[i] != nil {
			putAppendOnlyDirectValueArenaChunk(chunks[i])
			chunks[i] = nil
		}
	}
}

func appendOnlyDirectArenaChunksBytes(chunks [][]byte) int64 {
	var total int64
	for i := range chunks {
		if chunks[i] != nil {
			total += int64(cap(chunks[i]))
		}
	}
	return total
}

type appendOnlyDirectValueArena struct {
	retained      [][]byte
	retainedBytes int64
	active        [][]byte
	cur           []byte
	curPos        int
}

func (a *appendOnlyDirectValueArena) takeRetainedChunk(length int) []byte {
	want := length
	if want < appendOnlyDirectValueArenaDefaultChunk {
		want = appendOnlyDirectValueArenaDefaultChunk
	}
	for i := len(a.retained) - 1; i >= 0; i-- {
		chunk := a.retained[i]
		if cap(chunk) < want {
			continue
		}
		last := len(a.retained) - 1
		a.retained[i] = a.retained[last]
		a.retained[last] = nil
		a.retained = a.retained[:last]
		a.retainedBytes -= int64(cap(chunk))
		appendOnlyDirectArenaRetainedHitChunksTotal.Add(1)
		appendOnlyDirectArenaRetainedHitBytesTotal.Add(uint64(cap(chunk)))
		return chunk[:0]
	}
	return getAppendOnlyDirectValueArenaChunk(want)
}

func (a *appendOnlyDirectValueArena) alloc(length int) []byte {
	if length <= 0 {
		return nil
	}
	if a.cur == nil || cap(a.cur)-a.curPos < length {
		chunk := a.takeRetainedChunk(length)
		a.active = append(a.active, chunk)
		a.cur = chunk[:cap(chunk)]
		a.curPos = 0
	}
	out := a.cur[a.curPos : a.curPos+length : a.curPos+length]
	a.curPos += length
	return out
}

func (a *appendOnlyDirectValueArena) drainActiveChunks() [][]byte {
	chunks := a.active
	a.active = nil
	a.cur = nil
	a.curPos = 0
	return chunks
}

func (a *appendOnlyDirectValueArena) trimRetained(maxChunks int, maxBytes int64, maxChunkCap int) (droppedBytes int64) {
	if maxChunks < 0 {
		maxChunks = 0
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	if maxChunkCap <= 0 {
		maxChunkCap = appendOnlyDirectValueArenaPoolMaxCap
	}
	trimmed := a.retained[:0]
	var keptBytes int64
	for i := range a.retained {
		chunk := a.retained[i]
		if chunk == nil {
			continue
		}
		size := int64(cap(chunk))
		if cap(chunk) > maxChunkCap {
			putAppendOnlyDirectValueArenaChunk(chunk)
			droppedBytes += size
			continue
		}
		trimmed = append(trimmed, chunk[:0])
		keptBytes += size
	}
	for i := len(trimmed); i < len(a.retained); i++ {
		a.retained[i] = nil
	}
	a.retained = trimmed
	a.retainedBytes = keptBytes

	for len(a.retained) > maxChunks || a.retainedBytes > maxBytes {
		if len(a.retained) == 0 {
			break
		}
		evict := a.retained[0]
		copy(a.retained, a.retained[1:])
		last := len(a.retained) - 1
		a.retained[last] = nil
		a.retained = a.retained[:last]
		size := int64(cap(evict))
		a.retainedBytes -= size
		if a.retainedBytes < 0 {
			a.retainedBytes = 0
		}
		putAppendOnlyDirectValueArenaChunk(evict)
		droppedBytes += size
	}
	return droppedBytes
}

func (a *appendOnlyDirectValueArena) evictOldestRetainedToPool() bool {
	if len(a.retained) == 0 {
		return false
	}
	evict := a.retained[0]
	copy(a.retained, a.retained[1:])
	last := len(a.retained) - 1
	a.retained[last] = nil
	a.retained = a.retained[:last]
	a.retainedBytes -= int64(cap(evict))
	if a.retainedBytes < 0 {
		a.retainedBytes = 0
	}
	putAppendOnlyDirectValueArenaChunk(evict)
	return true
}

func (a *appendOnlyDirectValueArena) retainChunks(chunks [][]byte) {
	level := currentPoolPressureSnapshot().level
	maxRetainedChunks, maxRetainedBytes := appendOnlyDirectArenaRetentionLimitsForPressure(level)
	for len(a.retained) > 0 && (len(a.retained) > maxRetainedChunks || a.retainedBytes > maxRetainedBytes) {
		if !a.evictOldestRetainedToPool() {
			break
		}
	}
	if maxRetainedChunks <= 0 || maxRetainedBytes <= 0 {
		putAppendOnlyDirectValueArenaChunks(chunks)
		return
	}
	for i := range chunks {
		chunk := chunks[i]
		if chunk == nil {
			continue
		}
		chunks[i] = nil
		if cap(chunk) > appendOnlyDirectValueArenaPoolMaxCap {
			// Never retain oversize one-off chunks; they cannot be pooled by class
			// and would otherwise pin large backing arrays across rotations.
			putAppendOnlyDirectValueArenaChunk(chunk)
			continue
		}
		chunk = chunk[:0]
		size := int64(cap(chunk))
		for len(a.retained) > 0 && (len(a.retained) >= maxRetainedChunks || a.retainedBytes+size > maxRetainedBytes) {
			if !a.evictOldestRetainedToPool() {
				break
			}
		}
		if len(a.retained) >= maxRetainedChunks || a.retainedBytes+size > maxRetainedBytes {
			putAppendOnlyDirectValueArenaChunk(chunk)
			continue
		}
		a.retained = append(a.retained, chunk)
		a.retainedBytes += size
	}
}

func (a *appendOnlyDirectValueArena) recycleActive() {
	a.retainChunks(a.drainActiveChunks())
}

func (a *appendOnlyDirectValueArena) recycleAll() {
	putAppendOnlyDirectValueArenaChunks(a.drainActiveChunks())
	putAppendOnlyDirectValueArenaChunks(a.retained)
	a.retained = nil
	a.retainedBytes = 0
}

func getOuterLeafArena(capacity int) []byte {
	if capacity < 0 {
		capacity = 0
	}
	idx, classCap, ok := outerLeafArenaClassForLen(capacity)
	if !ok {
		return make([]byte, 0, capacity)
	}
	maxReuseCap := outerLeafArenaMaxReuseCap(capacity)
	maxIdx, _, maxOK := outerLeafArenaClassForLen(maxReuseCap)
	if !maxOK {
		maxIdx = idx
	}

	outerLeafArenaLeaseMu.Lock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		leases := outerLeafArenaLeases[bucket]
		if n := len(leases); n > 0 {
			buf := leases[n-1]
			outerLeafArenaLeases[bucket][n-1] = nil
			outerLeafArenaLeases[bucket] = leases[:n-1]
			outerLeafArenaLeaseMu.Unlock()
			if cap(buf) >= capacity {
				return buf[:0]
			}
			if capIdx, ok := outerLeafArenaClassForCap(cap(buf)); ok {
				outerLeafArenaPools[capIdx].Put(buf[:0])
			}
			return make([]byte, 0, classCap)
		}
	}
	outerLeafArenaLeaseMu.Unlock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		if v := outerLeafArenaPools[bucket].Get(); v != nil {
			if s, ok := v.([]byte); ok && cap(s) >= capacity && cap(s) <= maxReuseCap {
				return s[:0]
			}
		}
	}
	return make([]byte, 0, classCap)
}

func putOuterLeafArena(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) > maxOuterLeafArenaPoolCap {
		return
	}
	idx, ok := outerLeafArenaClassForCap(cap(buf))
	if !ok {
		return
	}
	buf = buf[:0]
	outerLeafArenaLeaseMu.Lock()
	if len(outerLeafArenaLeases[idx]) < maxOuterLeafArenaLeases {
		outerLeafArenaLeases[idx] = append(outerLeafArenaLeases[idx], buf)
		outerLeafArenaLeaseMu.Unlock()
		return
	}
	outerLeafArenaLeaseMu.Unlock()
	outerLeafArenaPools[idx].Put(buf)
}

func getOuterLeafBlobRefScratch() *[outerLeafBlobRefStackScratchCap]byte {
	if v := outerLeafBlobRefScratchPool.Get(); v != nil {
		if s, ok := v.(*[outerLeafBlobRefStackScratchCap]byte); ok && s != nil {
			return s
		}
	}
	return new([outerLeafBlobRefStackScratchCap]byte)
}

func putOuterLeafBlobRefScratch(buf *[outerLeafBlobRefStackScratchCap]byte) {
	if buf == nil {
		return
	}
	outerLeafBlobRefScratchPool.Put(buf)
}

func getOuterLeafEncoder() *outerleaf.Encoder {
	if v := outerLeafEncoderPool.Get(); v != nil {
		if e, ok := v.(*outerleaf.Encoder); ok && e != nil {
			return e
		}
	}
	return &outerleaf.Encoder{}
}

func putOuterLeafEncoder(e *outerleaf.Encoder) {
	if e == nil {
		return
	}
	e.Trim(maxOuterLeafEncoderRawScratchCap, maxOuterLeafEncoderEncScratchCap, maxOuterLeafEncoderRestartsCap)
	outerLeafEncoderPool.Put(e)
}

func getValueLogPtrs(n int) []page.ValuePtr {
	if n < 0 {
		n = 0
	}
	if v := valueLogPtrPool.Get(); v != nil {
		if s, ok := v.([]page.ValuePtr); ok {
			if cap(s) >= n {
				return s[:n]
			}
		}
	}
	return make([]page.ValuePtr, n)
}

func getValueLogPtrsCap(capacity int) []page.ValuePtr {
	if capacity < 0 {
		capacity = 0
	}
	if v := valueLogPtrPool.Get(); v != nil {
		if s, ok := v.([]page.ValuePtr); ok {
			maxCap := capacity * 2
			if maxCap < 256 {
				maxCap = 256
			}
			if cap(s) >= capacity && cap(s) <= maxCap {
				return s[:0]
			}
		}
	}
	return make([]page.ValuePtr, 0, capacity)
}

func putValueLogPtrs(s []page.ValuePtr) {
	if s == nil {
		return
	}
	clear(s)
	// Avoid retaining huge slices in the pool.
	if cap(s) > 1<<20 {
		return
	}
	valueLogPtrPool.Put(s[:0])
}

func putValueLogPtrsNoClear(s []page.ValuePtr) {
	if s == nil {
		return
	}
	// page.ValuePtr contains no pointer fields, so we can safely skip element
	// clearing in hot paths to reduce memclr overhead.
	if cap(s) > 1<<20 {
		return
	}
	valueLogPtrPool.Put(s[:0])
}

func getValueLogKeys(capacity int) [][]byte {
	if capacity < 0 {
		capacity = 0
	}
	valueLogKeyLeaseMu.Lock()
	for i := len(valueLogKeyLeases) - 1; i >= 0; i-- {
		s := valueLogKeyLeases[i]
		if cap(s) < capacity {
			continue
		}
		last := len(valueLogKeyLeases) - 1
		valueLogKeyLeases[i] = valueLogKeyLeases[last]
		valueLogKeyLeases[last] = nil
		valueLogKeyLeases = valueLogKeyLeases[:last]
		valueLogKeyLeaseMu.Unlock()
		return s[:0]
	}
	valueLogKeyLeaseMu.Unlock()
	if v := valueLogKeyPool.Get(); v != nil {
		if s, ok := v.([][]byte); ok {
			if cap(s) >= capacity {
				return s[:0]
			}
		}
	}
	return make([][]byte, 0, capacity)
}

func putValueLogKeys(s [][]byte) {
	if s == nil {
		return
	}
	clear(s)
	// Avoid retaining huge slices in the pool.
	if cap(s) > maxValueLogKeyLeaseCap {
		return
	}
	valueLogKeyLeaseMu.Lock()
	if len(valueLogKeyLeases) < maxValueLogKeyLeaseCount {
		valueLogKeyLeases = append(valueLogKeyLeases, s[:0])
		valueLogKeyLeaseMu.Unlock()
		return
	}
	valueLogKeyLeaseMu.Unlock()
	valueLogKeyPool.Put(s[:0])
}

func getVlogPreparedFrameBody() *vlogPreparedFrameBody {
	if v := valueLogPreparedBodyPool.Get(); v != nil {
		if body, ok := v.(*vlogPreparedFrameBody); ok {
			return body
		}
	}
	return &vlogPreparedFrameBody{}
}

func putVlogPreparedFrameBody(body *vlogPreparedFrameBody) {
	if body == nil {
		return
	}
	if cap(body.buf) > maxVlogPreparedBodyPoolCap {
		body.buf = nil
		return
	}
	body.buf = body.buf[:0]
	valueLogPreparedBodyPool.Put(body)
}

func getVlogPreparedFrames(n int) []preparedDictFrame {
	if n < 0 {
		n = 0
	}
	if v := valueLogPreparedFramesPool.Get(); v != nil {
		if s, ok := v.([]preparedDictFrame); ok {
			if cap(s) >= n {
				return s[:n]
			}
		}
	}
	return make([]preparedDictFrame, n)
}

func putVlogPreparedFrames(frames []preparedDictFrame) {
	if frames == nil {
		return
	}
	clear(frames)
	if cap(frames) > maxVlogPreparedFramesPoolCap {
		return
	}
	valueLogPreparedFramesPool.Put(frames[:0])
}

func getVlogDictPrepareResults(capacity int) chan vlogDictPrepareResult {
	if capacity < 1 {
		capacity = 1
	}
	if v := valueLogDictPrepareResultsPool.Get(); v != nil {
		if ch, ok := v.(chan vlogDictPrepareResult); ok {
			maxCap := capacity * 2
			if maxCap < 256 {
				maxCap = 256
			}
			if len(ch) == 0 && cap(ch) >= capacity && cap(ch) <= maxCap {
				return ch
			}
		}
	}
	return make(chan vlogDictPrepareResult, capacity)
}

func putVlogDictPrepareResults(ch chan vlogDictPrepareResult) {
	if ch == nil {
		return
	}
	for {
		select {
		case <-ch:
		default:
			if cap(ch) > maxVlogDictPrepareResultsPoolCap {
				return
			}
			valueLogDictPrepareResultsPool.Put(ch)
			return
		}
	}
}

const (
	envDebugFlushPointers = "TREEDB_DEBUG_FLUSH_PTRS"
	envDebugFlushTiming   = "TREEDB_DEBUG_FLUSH_TIMING"
	// Optional adaptive-memtable tuning knob for BTree selection safety.
	// 0 keeps legacy behavior (no iterator-sample gate).
	envAdaptiveBTreeMinIteratorSamples = "TREEDB_ADAPTIVE_BTREE_MIN_ITERATOR_SAMPLES"
	// Generational maintenance toggles (forensics / isolation).
	envDisableVlogGenerationRewrite        = "TREEDB_DISABLE_VLOG_GENERATION_REWRITE"
	envDisableVlogGenerationGC             = "TREEDB_DISABLE_VLOG_GENERATION_GC"
	envDisableVlogGenerationVacuum         = "TREEDB_DISABLE_VLOG_GENERATION_VACUUM"
	envDisableVlogGenerationLoop           = "TREEDB_DISABLE_VLOG_GENERATION_LOOP"
	envDisableVlogGenerationCheckpointKick = "TREEDB_DISABLE_VLOG_GENERATION_CHECKPOINT_KICK"
	envDisableVlogGenerationDeferred       = "TREEDB_DISABLE_VLOG_GENERATION_DEFERRED"
	envDebugVlogGenerationPlanTimeoutMS    = "TREEDB_DEBUG_VLOG_GENERATION_PLAN_TIMEOUT_MS"
	// Diagnostic toggle for WAL-off checkpoint-time sparse-index vacuum.
	envDisableCheckpointAutoVacuum         = "TREEDB_DISABLE_CHECKPOINT_AUTO_VACUUM"
	minMemtablePrealloc                    = 64 * 1024
	maxMemtablePrealloc                    = 256 << 20
	appendOnlyEntryHintMinEntries          = 128
	appendOnlyEntryHintMaxEntries          = 1 << 20
	adaptiveMinWrites                      = 1024
	adaptiveSequentialWritePct             = 0.85
	adaptiveRangeIteratorPct               = 0.40
	adaptiveOverwriteWritePct              = 0.25
	adaptiveBTreeMinIteratorSamplesDefault = 0
	adaptiveWarmupBytes                    = 16 * 1024 * 1024
	maxMemtableBytesPerShard               = int64(3 << 30)
	maxOuterLeafArenaPoolCap               = 16 << 20
	outerLeafBlobRefStackScratchCap        = 256
	maxOuterLeafEncoderRawScratchCap       = 2 << 20
	maxOuterLeafEncoderEncScratchCap       = 2 << 20
	maxOuterLeafEncoderRestartsCap         = 1 << 15
	maxVlogPreparedBodyPoolCap             = 8 << 20
	maxVlogPreparedFramesPoolCap           = 1 << 14
	maxVlogDictPrepareResultsPoolCap       = 1 << 14
)

// SetIteratorDebug toggles attaching debug metadata to iterators returned by
// CachingDB.Iterator. It is intended for benchmarking/diagnostics.
func SetIteratorDebug(enabled bool) {
	iteratorDebugEnabled.Store(enabled)
}

func envBool(name string) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return false
}

func envUint64(name string, fallback uint64) uint64 {
	v, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func updateAtomicMaxUint64(dst *atomic.Uint64, value uint64) {
	for {
		cur := dst.Load()
		if value <= cur {
			return
		}
		if dst.CompareAndSwap(cur, value) {
			return
		}
	}
}

func (db *DB) hasDirtyValueLogLanes() bool {
	if db == nil || !db.valueLogEnabled() {
		return false
	}
	for i := range db.lanes {
		if db.lanes[i].vlogDirty.Load() {
			return true
		}
	}
	return false
}

func (db *DB) checkpointFlushValueLogLanes() error {
	if db == nil || !db.splitValueLogEnabled() {
		return nil
	}
	// Checkpoint is a durability boundary for cached mode. Ensure any buffered
	// value-log bytes are visible before publishing pointers durably to the backend.
	flushOnly := db.relaxedSync
	for i := range db.lanes {
		l := &db.lanes[i]
		if !l.vlogDirty.Load() {
			continue
		}
		l.vlogMu.Lock()
		w := l.vlog
		var err error
		if w != nil {
			if flushOnly {
				err = w.Flush()
			} else {
				err = w.Sync()
			}
		}
		if err == nil {
			l.vlogDirty.Store(false)
		}
		l.vlogMu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) syncDirBestEffort(dir string) {
	if dir == "" || runtime.GOOS == "windows" {
		return
	}
	f, err := os.Open(dir)
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to open dir %q for sync: %w", dir, err))
		return
	}
	if err := f.Sync(); err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to sync dir %q: %w", dir, err))
	}
	if err := f.Close(); err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to close dir %q after sync: %w", dir, err))
	}
}

func (db *DB) removeFileRetry(path string) error {
	var err error
	backoff := 25 * time.Millisecond
	for i := 0; i < 40; i++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		if runtime.GOOS != "windows" {
			return err
		}
		if !isWindowsSharingViolationError(err) {
			return err
		}
		time.Sleep(backoff)
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
	return err
}

func isWindowsSharingViolationError(err error) bool {
	if runtime.GOOS != "windows" || err == nil {
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Err != nil {
		err = pathErr.Err
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "sharing violation") || strings.Contains(msg, "used by another process")
}

func warnInsecureDir(dir string, notify func(error)) {
	if dir == "" || notify == nil || runtime.GOOS == "windows" {
		return
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		notify(fmt.Errorf("cachingdb: failed to stat dir %q: %w", dir, err))
		return
	}
	if info.Mode()&os.ModeSymlink != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is a symlink; verify target permissions", dir))
		info, err = os.Stat(dir)
		if err != nil {
			notify(fmt.Errorf("cachingdb: failed to stat symlink target %q: %w", dir, err))
			return
		}
	}
	if !info.IsDir() {
		notify(fmt.Errorf("cachingdb: path %q is not a directory", dir))
		return
	}
	perms := info.Mode().Perm()
	if perms&0o002 != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is world-writable (mode %o)", dir, perms))
	} else if perms&0o020 != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is group-writable (mode %o)", dir, perms))
	}
}

func memtableCapacity(flushThreshold int64) int {
	if flushThreshold <= 0 {
		return 0
	}
	capBytes := flushThreshold + flushThreshold/4 // +25% to cover skiplist overhead.
	if capBytes < minMemtablePrealloc {
		capBytes = minMemtablePrealloc
	}
	if capBytes > maxMemtablePrealloc {
		capBytes = maxMemtablePrealloc
	}
	maxInt := int64(int(^uint(0) >> 1))
	if capBytes > maxInt {
		capBytes = maxInt
	}
	return int(capBytes)
}

func normalizeShardCount(n int) int {
	if n < 1 {
		return 1
	}
	// Round down to a power of two.
	v := 1
	for v<<1 <= n {
		v <<= 1
	}
	return v
}

func defaultMemtableShards() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	n *= 2
	if n > 16 {
		n = 16
	}
	return normalizeShardCount(n)
}

func shardCapacity(totalCap, shards int) int {
	if shards <= 1 {
		return totalCap
	}
	if totalCap <= 0 {
		return 1
	}
	cap := totalCap / shards
	if cap <= 0 {
		cap = 1
	}
	return cap
}

func (db *DB) valueLogEnabled() bool {
	return true
}

func (db *DB) splitValueLogEnabled() bool {
	return true
}

func (db *DB) valueLogThresholdForKey(key []byte) int {
	if db == nil {
		return page.DefaultInlineThreshold
	}
	return backenddb.ResolveInlineThresholdForKey(db.valueLogThreshold, key, db.valueLogDomainThresholds)
}

func (db *DB) shouldWriteViaValueLogForKeyValue(key, value []byte) bool {
	if db == nil {
		return false
	}
	return db.forceValueLogPointers || len(value) > db.valueLogThresholdForKey(key)
}

type outerLeafRecordGroup struct {
	start int
	end   int
}

// buildOuterLeafValueRecords encodes key/value pairs into value-log records.
// The returned groups map each encoded record back to a contiguous source range
// [start,end).
func (db *DB) buildOuterLeafValueRecords(keys [][]byte, values [][]byte) ([]valuelog.Record, []outerLeafRecordGroup, []byte, error) {
	if len(keys) != len(values) {
		return nil, nil, nil, fmt.Errorf("cachingdb: outer-leaf key/value length mismatch %d/%d", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, nil, nil, nil
	}

	records := getValueLogRecordsCap(len(keys))
	groups := make([]outerLeafRecordGroup, 0, len(keys))
	for i := range keys {
		records = append(records, valuelog.Record{Value: values[i]})
		groups = append(groups, outerLeafRecordGroup{start: i, end: i + 1})
	}
	return records, groups, nil, nil
}

func fallbackAutoVlogWriteMode(mode vlogCompressionMode, writeMode vlogCompressionWriteMode) vlogCompressionWriteMode {
	if mode == vlogCompressionAuto && writeMode == vlogWriteDict {
		return vlogWriteBlock
	}
	return writeMode
}

func lookupVlogDictBytes(dictID uint64, singleDictID uint64, singleDict []byte, dictByID map[uint64][]byte) []byte {
	if dictID == 0 {
		return nil
	}
	if dictByID == nil {
		if dictID == singleDictID {
			return singleDict
		}
		return nil
	}
	return dictByID[dictID]
}

func (db *DB) deferredValueLogEnabled() bool {
	if db == nil {
		return false
	}
	return false
}

// sumKeyValueBytes returns the total bytes across paired key/value entries.
// If keys and values differ in length, only the shared prefix is counted.
func sumKeyValueBytes(keys, values [][]byte) uint64 {
	if len(keys) == 0 || len(values) == 0 {
		return 0
	}
	if len(keys) > len(values) {
		keys = keys[:len(values)]
	} else if len(values) > len(keys) {
		values = values[:len(keys)]
	}
	var total uint64
	for i := 0; i < len(keys); i++ {
		total += uint64(len(keys[i]) + len(values[i]))
	}
	return total
}

func (db *DB) shouldFlushDeferredValueLog(writeMode vlogCompressionWriteMode, records []valuelog.Record) bool {
	return db != nil && db.deferredValueLogEnabled()
}

func (db *DB) shouldFlushDeferredValueLogValue(writeMode vlogCompressionWriteMode, value []byte) bool {
	return db != nil && db.deferredValueLogEnabled()
}

func (db *DB) walUsesValueLog() bool {
	return false
}

func (db *DB) needsVlogAutotuneTiming() bool {
	if db == nil {
		return false
	}
	if db.valueLogAutotuneOptions.Mode != valuelog.AutotuneOff {
		return true
	}
	return vlogAutotuneMetricsEnabled.Load()
}

func (db *DB) pickLane(sync bool, preferred int) (*lane, error) {
	if db == nil || len(db.lanes) == 0 {
		return nil, errWALUnavailable
	}
	if !sync && preferred >= 0 && preferred < len(db.lanes) {
		l := &db.lanes[preferred]
		if !l.syncing.Load() {
			return l, nil
		}
	}

	db.laneMu.Lock()
	defer db.laneMu.Unlock()
	for {
		select {
		case <-db.closeCh:
			return nil, errWALClosed
		default:
		}

		if preferred >= 0 && preferred < len(db.lanes) {
			l := &db.lanes[preferred]
			if !l.syncing.Load() {
				if sync {
					l.syncing.Store(true)
				}
				return l, nil
			}
			// If preferred lane is busy, we could wait or fallback.
			// To maintain strict lane-affinity, we wait.
			db.laneCond.Wait()
			continue
		}

		start := db.nextLane
		for i := 0; i < len(db.lanes); i++ {
			idx := (start + i) % len(db.lanes)
			l := &db.lanes[idx]
			if l.syncing.Load() {
				continue
			}
			db.nextLane = (idx + 1) % len(db.lanes)
			if sync {
				l.syncing.Store(true)
			}
			return l, nil
		}
		db.laneCond.Wait()
	}
}

func (db *DB) releaseLaneSync(l *lane) {
	if l == nil {
		return
	}
	if !l.syncing.CompareAndSwap(true, false) {
		return
	}
	db.laneMu.Lock()
	db.laneCond.Broadcast()
	db.laneMu.Unlock()
}

func (db *DB) currentValueLogPath(l *lane) string {
	if l == nil {
		return ""
	}
	if db.splitValueLogEnabled() {
		l.vlogMu.Lock()
		path := l.vlogPath
		l.vlogMu.Unlock()
		return path
	}
	l.walMu.Lock()
	path := l.walPath
	l.walMu.Unlock()
	return path
}

func (db *DB) currentValueLogSeq(l *lane) int {
	if l == nil {
		return 0
	}
	if db.splitValueLogEnabled() {
		l.vlogMu.Lock()
		seq := l.vlogSeq
		l.vlogMu.Unlock()
		return seq
	}
	l.walMu.Lock()
	seq := l.walSeq
	l.walMu.Unlock()
	return seq
}

func (db *DB) currentWALPaths() []string {
	if db == nil || db.disableJournal {
		return nil
	}
	paths := make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		if l.walPath != "" {
			paths = append(paths, l.walPath)
		}
		l.walMu.Unlock()
	}
	return paths
}

func (db *DB) currentValueLogPaths() []string {
	if db == nil || !db.valueLogEnabled() {
		return nil
	}
	paths := make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		if db.splitValueLogEnabled() {
			l.vlogMu.Lock()
			if l.vlogPath != "" {
				paths = append(paths, l.vlogPath)
			}
			l.vlogMu.Unlock()
			continue
		}
		l.walMu.Lock()
		if l.walPath != "" {
			paths = append(paths, l.walPath)
		}
		l.walMu.Unlock()
	}
	return paths
}

func valueLogIDsFromPaths(paths []string) map[uint32]struct{} {
	if len(paths) == 0 {
		return nil
	}
	ids := make(map[uint32]struct{}, len(paths))
	for _, path := range paths {
		laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if !ok || !valueLog || laneID < 0 {
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
		if err != nil {
			continue
		}
		ids[id] = struct{}{}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func valueLogPathByID(paths []string) map[uint32]string {
	if len(paths) == 0 {
		return nil
	}
	out := make(map[uint32]string, len(paths))
	for _, path := range paths {
		laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if !ok || !valueLog || laneID < 0 {
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
		if err != nil {
			continue
		}
		if _, exists := out[id]; !exists {
			out[id] = path
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (db *DB) queuedValueLogPathsSnapshot() []string {
	if db == nil || !db.valueLogEnabled() {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(db.queueValueLogPaths) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, paths := range db.queueValueLogPaths {
		for _, path := range paths {
			if path == "" {
				continue
			}
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			out = append(out, path)
		}
	}
	return out
}

func (db *DB) deferValueLogOps(ops []batch.Entry, sync bool) ([]batch.Entry, error) {
	if db == nil || len(ops) == 0 || !db.deferredValueLogEnabled() {
		return ops, nil
	}
	durability := journalDurabilityNone
	if sync {
		if db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	}
	return db.rewriteValueLogOpsForBackend(ops, durability)
}

func (db *DB) prepareBypassValueLogOps(ops []batch.Entry, sync bool) ([]batch.Entry, error) {
	if db == nil || len(ops) == 0 || !db.allowValueLogPointers() {
		return ops, nil
	}
	durability := journalDurabilityFlush
	if sync && !db.relaxedSync {
		durability = journalDurabilitySync
	}
	return db.rewriteValueLogOpsForBackend(ops, durability)
}

func (db *DB) rewriteValueLogOpsForBackend(ops []batch.Entry, durability journalDurability) ([]batch.Entry, error) {
	if db == nil || len(ops) == 0 {
		return ops, nil
	}
	if !db.allowValueLogPointers() {
		return ops, nil
	}

	eligible := getValueLogEligible(len(ops))
	defer putValueLogEligible(eligible)
	for i := range ops {
		op := &ops[i]
		if op.Type != batch.OpPut || op.IsPtr {
			continue
		}
		if !db.shouldWriteViaValueLogForKeyValue(op.Key, op.Value) {
			continue
		}
		eligible = append(eligible, i)
	}
	if len(eligible) == 0 {
		return ops, nil
	}

	lane, err := db.pickLane(false, -1)
	if err != nil {
		return nil, err
	}

	// Best-effort: use the current dict when available.
	dictID := uint64(0)
	if db.dictStore != nil {
		if id, err := db.currentDictID(context.Background()); err == nil {
			dictID = id
		}
	}

	keys := getValueLogKeys(len(eligible))
	defer putValueLogKeys(keys)
	values := getValueLogKeys(len(eligible))
	defer putValueLogKeys(values)
	for _, idx := range eligible {
		op := &ops[idx]
		keys = append(keys, op.Key)
		values = append(values, op.Value)
	}
	records, groups, outerArena, err := db.buildOuterLeafValueRecords(keys, values)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return ops, nil
	}
	startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
	for i := range records {
		records[i].RID = startRID + uint64(i)
	}
	ptrs, err := db.appendValueLog(lane, dictID, nil, records, durability)
	if err != nil {
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
		return nil, err
	}
	if len(ptrs) != len(records) {
		putValueLogPtrs(ptrs)
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
		return nil, fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(ptrs), len(records))
	}
	defer func() {
		putValueLogPtrs(ptrs)
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
	}()

	for i := range groups {
		ptr := ptrs[i]
		group := groups[i]
		if group.start < 0 || group.end < group.start || group.end > len(eligible) {
			return nil, errors.New("cachingdb: deferred value-log group out of range")
		}
		for srcPos := group.start; srcPos < group.end; srcPos++ {
			idx := eligible[srcPos]
			op := &ops[idx]
			op.ValuePtr = ptr
			op.IsPtr = true
			op.Value = nil
		}
	}
	retainPath := db.currentValueLogPath(lane)
	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}
	if durability == journalDurabilityNone {
		db.backendReadVlogDirtySeq.Add(1)
	}
	return ops, nil
}

func reserveBackendBatchOps(backendBatch batch.Interface, n int) {
	if backendBatch == nil || n <= 0 {
		return
	}
	type reserveHint interface {
		Reserve(int)
	}
	if r, ok := backendBatch.(reserveHint); ok {
		r.Reserve(n)
	}
}

func (db *DB) flushDeferredValueLogMemtable(
	iter iterator.UnsafeIterator,
	backendBatch batch.Interface,
	memLen int,
	sync bool,
	laneID int,
) (int, error) {
	if db == nil {
		return 0, nil
	}
	if iter == nil {
		return 0, nil
	}
	if backendBatch == nil {
		return 0, errors.New("cachingdb: missing backend batch")
	}

	allowPointers := db.allowValueLogPointers()

	type (
		setViewer interface {
			SetView(key, value []byte) error
		}
		deleteViewer interface {
			DeleteView(key []byte) error
		}
		ptrSetter interface {
			SetPointer(key []byte, ptr page.ValuePtr) error
		}
		ptrSetterView interface {
			SetPointerView(key []byte, ptr page.ValuePtr) error
		}
	)
	sv, _ := backendBatch.(setViewer)
	dv, _ := backendBatch.(deleteViewer)
	psv, _ := backendBatch.(ptrSetterView)
	ps, _ := backendBatch.(ptrSetter)
	reserveHint := memLen
	if db.flushBackendMaxEntries > 0 && reserveHint > db.flushBackendMaxEntries {
		reserveHint = db.flushBackendMaxEntries
	}
	reserveBackendBatchOps(backendBatch, reserveHint)

	var ptrKeys [][]byte
	var ptrVals [][]byte
	defer func() {
		putValueLogKeys(ptrKeys)
		putValueLogKeys(ptrVals)
	}()

	emittedOps := 0
	durability := journalDurabilityNone
	if sync {
		durability = journalDurabilitySync
	}

	vlogLane := (*lane)(nil)
	var (
		dictID      uint64
		dictIDReady bool
	)
	ensureVlogLane := func() error {
		if vlogLane != nil {
			return nil
		}
		l, err := db.pickLane(false, laneID)
		if err != nil {
			return err
		}
		vlogLane = l
		if !dictIDReady {
			if db.dictStore != nil {
				if id, err := db.currentDictID(context.Background()); err == nil {
					dictID = id
				}
			}
			dictIDReady = true
		}
		return nil
	}

	for iter.Valid() {
		key := iter.UnsafeKey()
		if iter.IsDeleted() {
			var err error
			if dv != nil {
				err = dv.DeleteView(key)
			} else {
				err = backendBatch.Delete(key)
			}
			if err != nil {
				return 0, err
			}
			emittedOps++
			iter.Next()
			continue
		}

		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			var err error
			if psv != nil {
				err = psv.SetPointerView(key, ptr)
			} else if ps != nil {
				err = ps.SetPointer(key, ptr)
			} else {
				return 0, errors.New("cachingdb: backend batch missing SetPointer")
			}
			if err != nil {
				return 0, err
			}
			emittedOps++
			iter.Next()
			continue
		}

		if val == nil && db.memtableValueLogPointers {
			return 0, errors.New("cachingdb: flush missing value-log ptr for key")
		}

		if allowPointers && db.shouldWriteViaValueLogForKeyValue(key, val) {
			if ptrKeys == nil {
				hint := memLen
				if hint <= 0 {
					hint = 16
				}
				if hint > db.flushBackendInitEntries {
					hint = db.flushBackendInitEntries
				}
				ptrKeys = getValueLogKeys(hint)
				ptrVals = getValueLogKeys(hint)
			}
			keyCopy := append([]byte(nil), key...)
			valCopy := append([]byte(nil), val...)
			ptrKeys = append(ptrKeys, keyCopy)
			ptrVals = append(ptrVals, valCopy)
			iter.Next()
			continue
		}

		var err error
		if sv != nil {
			err = sv.SetView(key, val)
		} else {
			err = backendBatch.Set(key, val)
		}
		if err != nil {
			return 0, err
		}
		emittedOps++
		iter.Next()
	}

	if len(ptrKeys) == 0 {
		return emittedOps, nil
	}
	if !allowPointers {
		return emittedOps, nil
	}
	if len(ptrKeys) != len(ptrVals) {
		return 0, errors.New("cachingdb: internal deferred value-log mismatch")
	}
	if err := ensureVlogLane(); err != nil {
		return 0, err
	}

	records, groups, outerArena, err := db.buildOuterLeafValueRecords(ptrKeys, ptrVals)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
		return emittedOps, nil
	}
	defer putValueLogRecordsNoClear(records)
	defer putOuterLeafArena(outerArena)

	startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
	for i := range records {
		records[i].RID = startRID + uint64(i)
	}

	ptrs, err := db.appendValueLog(vlogLane, dictID, nil, records, durability)
	if err != nil {
		return 0, err
	}
	if len(ptrs) != len(records) {
		putValueLogPtrs(ptrs)
		return 0, fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(ptrs), len(records))
	}
	defer putValueLogPtrs(ptrs)

	if len(ptrs) != len(groups) {
		return 0, errors.New("cachingdb: deferred value-log group/pointer count mismatch")
	}
	for i := range groups {
		ptr := ptrs[i]
		group := groups[i]
		if group.start < 0 || group.end < group.start || group.end > len(ptrKeys) {
			return 0, errors.New("cachingdb: deferred value-log group out of range")
		}
		for srcPos := group.start; srcPos < group.end; srcPos++ {
			key := ptrKeys[srcPos]
			if psv != nil {
				if err := psv.SetPointerView(key, ptr); err != nil {
					return 0, err
				}
			} else if ps != nil {
				if err := ps.SetPointer(key, ptr); err != nil {
					return 0, err
				}
			} else {
				return 0, errors.New("cachingdb: backend batch missing SetPointer")
			}
			emittedOps++
		}
	}

	retainPath := db.currentValueLogPath(vlogLane)
	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}
	if durability == journalDurabilityNone {
		db.backendReadVlogDirtySeq.Add(1)
	}
	return emittedOps, nil
}

func (db *DB) flushDeferredValueLogUnits(units []flushUnit, backendBatch batch.Interface, sync bool, laneID int) (int, error) {
	if db == nil || len(units) == 0 {
		return 0, nil
	}
	if backendBatch == nil {
		return 0, errors.New("cachingdb: missing backend batch")
	}

	type (
		setViewer interface {
			SetView(key, value []byte) error
		}
		deleteViewer interface {
			DeleteView(key []byte) error
		}
		ptrSetter interface {
			SetPointer(key []byte, ptr page.ValuePtr) error
		}
		ptrSetterView interface {
			SetPointerView(key []byte, ptr page.ValuePtr) error
		}
	)
	sv, _ := backendBatch.(setViewer)
	dv, _ := backendBatch.(deleteViewer)
	psv, _ := backendBatch.(ptrSetterView)
	ps, _ := backendBatch.(ptrSetter)

	chunkCap := db.flushBuildChunkCap
	if chunkCap <= 0 {
		chunkCap = 8192
	}
	const maxDeferredInlineGroupKeys = 32768

	unitRuns := getUnitRuns(len(units))
	defer func() {
		for i := range unitRuns {
			for _, run := range unitRuns[i] {
				putEntrySlice(run)
			}
			putEntryRuns(unitRuns[i])
		}
		putUnitRuns(unitRuns)
	}()
	for i := range units {
		runs, _, err := buildOpRuns(units[i].mem, chunkCap)
		if err != nil {
			return 0, err
		}
		unitRuns[i] = runs
	}

	heap := getOpMergeHeap(len(unitRuns))
	defer func() { putOpMergeHeap(heap) }()
	for i := range unitRuns {
		if len(unitRuns[i]) == 0 {
			continue
		}
		it := newOpRunIter(unitRuns[i])
		if !it.Valid() {
			continue
		}
		priority := len(unitRuns) - 1 - i
		heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	allowPointers := db.allowValueLogPointers()
	durability := journalDurabilityNone
	if sync {
		durability = journalDurabilitySync
	}

	ptrCap := estimateUnitRunEntries(unitRuns, chunkCap)
	reserveHint := ptrCap
	if db.flushBackendMaxEntries > 0 && reserveHint > db.flushBackendMaxEntries {
		reserveHint = db.flushBackendMaxEntries
	}
	reserveBackendBatchOps(backendBatch, reserveHint)
	if ptrCap > maxDeferredInlineGroupKeys {
		ptrCap = maxDeferredInlineGroupKeys
	}
	ptrKeys := getValueLogKeys(ptrCap)
	ptrVals := getValueLogKeys(ptrCap)
	defer func() {
		putValueLogKeys(ptrKeys)
		putValueLogKeys(ptrVals)
	}()

	backendPendingOps := 0
	vlogLane := (*lane)(nil)
	var (
		dictID      uint64
		dictIDReady bool
	)
	ensureVlogLane := func() error {
		if vlogLane != nil {
			return nil
		}
		l, err := db.pickLane(false, laneID)
		if err != nil {
			return err
		}
		vlogLane = l
		if !dictIDReady {
			if db.dictStore != nil {
				if id, err := db.currentDictID(context.Background()); err == nil {
					dictID = id
				}
			}
			dictIDReady = true
		}
		return nil
	}

	emitPointer := func(key []byte, ptr page.ValuePtr) error {
		if psv != nil {
			if err := psv.SetPointerView(key, ptr); err != nil {
				return err
			}
		} else if ps != nil {
			if err := ps.SetPointer(key, ptr); err != nil {
				return err
			}
		} else {
			return errors.New("cachingdb: backend batch missing SetPointer")
		}
		backendPendingOps++
		return nil
	}

	flushInlinePointerGroup := func() error {
		if len(ptrKeys) == 0 {
			return nil
		}
		if !allowPointers {
			ptrKeys = ptrKeys[:0]
			ptrVals = ptrVals[:0]
			return nil
		}
		if len(ptrKeys) != len(ptrVals) {
			return errors.New("cachingdb: deferred inline pointer grouping mismatch")
		}
		if err := ensureVlogLane(); err != nil {
			return err
		}
		totalKeys := len(ptrKeys)
		for chunkStart := 0; chunkStart < totalKeys; chunkStart += maxDeferredInlineGroupKeys {
			chunkEnd := chunkStart + maxDeferredInlineGroupKeys
			if chunkEnd > totalKeys {
				chunkEnd = totalKeys
			}
			chunkKeys := ptrKeys[chunkStart:chunkEnd]
			chunkVals := ptrVals[chunkStart:chunkEnd]

			records, groups, outerArena, err := db.buildOuterLeafValueRecords(chunkKeys, chunkVals)
			if err != nil {
				return err
			}
			if len(records) == 0 {
				putValueLogRecordsNoClear(records)
				putOuterLeafArena(outerArena)
				continue
			}

			startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
			for i := range records {
				records[i].RID = startRID + uint64(i)
			}

			ptrs, err := db.appendValueLog(vlogLane, dictID, nil, records, durability)
			putValueLogRecordsNoClear(records)
			putOuterLeafArena(outerArena)
			if err != nil {
				return err
			}
			if len(ptrs) != len(records) {
				putValueLogPtrs(ptrs)
				return fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(ptrs), len(records))
			}
			if len(ptrs) != len(groups) {
				putValueLogPtrs(ptrs)
				return errors.New("cachingdb: deferred inline pointer group/pointer count mismatch")
			}
			for i := range groups {
				ptr := ptrs[i]
				group := groups[i]
				if group.start < 0 || group.end < group.start || group.end > len(chunkKeys) {
					putValueLogPtrs(ptrs)
					return errors.New("cachingdb: deferred inline pointer source group out of range")
				}
				for srcPos := group.start; srcPos < group.end; srcPos++ {
					if err := emitPointer(chunkKeys[srcPos], ptr); err != nil {
						putValueLogPtrs(ptrs)
						return err
					}
				}
			}
			putValueLogPtrs(ptrs)
			if durability == journalDurabilityNone {
				db.backendReadVlogDirtySeq.Add(1)
			}
		}

		ptrKeys = ptrKeys[:0]
		ptrVals = ptrVals[:0]
		return nil
	}

	shadowedOps := 0
	appliedOps := 0
	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key

		for len(heap) > 0 {
			next := heap.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := heap.pop()
				shadowedOps++
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.Key()
					heap.push(shadowed)
				}
				continue
			}
			break
		}

		entry := top.iter.Entry()
		switch {
		case entry.Type == batch.OpDelete:
			if err := flushInlinePointerGroup(); err != nil {
				return 0, err
			}
			var err error
			if dv != nil {
				err = dv.DeleteView(entry.Key)
			} else {
				err = backendBatch.Delete(entry.Key)
			}
			if err != nil {
				return 0, err
			}
			backendPendingOps++
			appliedOps++
		case entry.IsPtr:
			if err := flushInlinePointerGroup(); err != nil {
				return 0, err
			}
			if err := emitPointer(entry.Key, entry.ValuePtr); err != nil {
				return 0, err
			}
		default:
			if allowPointers && db.shouldWriteViaValueLogForKeyValue(entry.Key, entry.Value) {
				if len(ptrKeys) >= maxDeferredInlineGroupKeys {
					if err := flushInlinePointerGroup(); err != nil {
						return 0, err
					}
				}
				keyCopy := append([]byte(nil), entry.Key...)
				valCopy := append([]byte(nil), entry.Value...)
				ptrKeys = append(ptrKeys, keyCopy)
				ptrVals = append(ptrVals, valCopy)
			} else {
				if err := flushInlinePointerGroup(); err != nil {
					return 0, err
				}
				var err error
				if sv != nil {
					err = sv.SetView(entry.Key, entry.Value)
				} else {
					err = backendBatch.Set(entry.Key, entry.Value)
				}
				if err != nil {
					return 0, err
				}
				backendPendingOps++
				appliedOps++
			}
		}

		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}

	if err := flushInlinePointerGroup(); err != nil {
		return 0, err
	}
	if vlogLane != nil {
		// Do not flush/sync the value-log lane here. flushLaneOnce performs one
		// flush boundary after value-log appends so we avoid redundant syscalls.
		retainPath := db.currentValueLogPath(vlogLane)
		if retainPath != "" {
			db.markValueLogRetain(retainPath)
		}
	}
	if shadowedOps > 0 {
		flushMergeShadowedOpsTotal.Add(uint64(shadowedOps))
		flushMergeDeferredShadowedOpsTotal.Add(uint64(shadowedOps))
	}
	if appliedOps > 0 {
		flushMergeAppliedOpsTotal.Add(uint64(appliedOps))
		flushMergeDeferredAppliedOpsTotal.Add(uint64(appliedOps))
	}
	return backendPendingOps, nil
}

// SetDictStore installs the dictionary store for current-ID freezing.
func (db *DB) SetDictStore(store DictStore) {
	if db == nil {
		return
	}
	db.dictStore = store
	db.dictCurrentCached.Store(0)
	db.dictCurrentOps.Store(0)
	if store != nil {
		if dictID, err := store.GetCurrent(context.Background()); err == nil {
			db.dictCurrentCached.Store(dictID)
		}
	}
	db.valueLogDictBytesMu.Lock()
	db.valueLogDictBytesID = 0
	db.valueLogDictBytes = nil
	db.valueLogDictBytesMu.Unlock()
	for i := range db.lanes {
		l := &db.lanes[i]
		l.vlogDictBytesMu.Lock()
		l.vlogDictBytes = nil
		l.vlogDictBytesMu.Unlock()
	}
	if db.valueLogReader != nil && store != nil {
		db.valueLogReader.SetDictLookup(func(dictID uint64) ([]byte, error) {
			return store.GetDictBytes(context.Background(), dictID)
		})
	}
	db.ensureValueLogDictTrainer()
}

// SetTemplateStore installs the template store used for template compression.
func (db *DB) SetTemplateStore(store template.Store) {
	if db == nil {
		return
	}
	db.templateStore = store
	if db.valueLogReader != nil && store != nil {
		db.valueLogReader.SetTemplateLookup(func(templateID uint64) ([]byte, error) {
			return db.templateLookup(context.Background(), templateID)
		}, db.valueLogTemplateDecodeOpts)
	}
}

func (db *DB) templateLookup(ctx context.Context, templateID uint64) ([]byte, error) {
	if templateID == 0 {
		return nil, valuelog.ErrMissingTemplate
	}
	if db == nil || db.templateStore == nil {
		return nil, valuelog.ErrMissingTemplate
	}
	defBytes, err := db.templateStore.GetTemplateDef(ctx, templateID)
	if err != nil {
		return nil, err
	}
	return defBytes, nil
}

func (db *DB) currentDictID(ctx context.Context) (uint64, error) {
	if db == nil || db.dictStore == nil {
		return 0, nil
	}
	// Avoid per-write dictdb reads on the hot path; refresh every N uses.
	const refreshEvery = uint64(1 << 16)
	seq := db.dictCurrentOps.Add(1)
	if seq&(refreshEvery-1) != 0 {
		return db.dictCurrentCached.Load(), nil
	}
	dictID, err := db.dictStore.GetCurrent(ctx)
	if err != nil {
		// Fall back to cached value on transient errors (best-effort).
		return db.dictCurrentCached.Load(), nil
	}
	db.dictCurrentCached.Store(dictID)
	return dictID, nil
}

func (db *DB) dictBytes(ctx context.Context, dictID uint64) ([]byte, error) {
	if dictID == 0 {
		return nil, nil
	}
	if db == nil || db.dictStore == nil {
		return nil, valuelog.ErrMissingDict
	}
	db.valueLogDictBytesMu.Lock()
	if db.valueLogDictBytesID == dictID && len(db.valueLogDictBytes) > 0 {
		out := db.valueLogDictBytes
		db.valueLogDictBytesMu.Unlock()
		return out, nil
	}
	db.valueLogDictBytesMu.Unlock()

	out, err := db.dictStore.GetDictBytes(ctx, dictID)
	if err != nil {
		return nil, err
	}
	db.valueLogDictBytesMu.Lock()
	db.valueLogDictBytesID = dictID
	db.valueLogDictBytes = out
	db.valueLogDictBytesMu.Unlock()
	return out, nil
}

func (db *DB) dictBytesForLane(ctx context.Context, l *lane, dictID uint64) ([]byte, error) {
	if dictID == 0 {
		return nil, nil
	}
	if l == nil {
		return db.dictBytes(ctx, dictID)
	}
	l.vlogDictBytesMu.RLock()
	if l.vlogDictBytes != nil {
		if b, ok := l.vlogDictBytes[dictID]; ok && len(b) > 0 {
			l.vlogDictBytesMu.RUnlock()
			return b, nil
		}
	}
	l.vlogDictBytesMu.RUnlock()

	out, err := db.dictBytes(ctx, dictID)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	l.vlogDictBytesMu.Lock()
	if l.vlogDictBytes == nil {
		l.vlogDictBytes = make(map[uint64][]byte)
	}
	if len(l.vlogDictBytes) >= 32 {
		clear(l.vlogDictBytes)
	}
	l.vlogDictBytes[dictID] = out
	l.vlogDictBytesMu.Unlock()
	return out, nil
}

func (db *DB) templateCompressionEnabled() bool {
	return db != nil && db.valueLogTemplateEnabled && db.valueLogTemplateEngine != nil && db.templateStore != nil
}

func (db *DB) valueLogTemplateEncodeRecords(records []valuelog.Record) ([]valuelog.Record, bool) {
	if !db.templateCompressionEnabled() || len(records) == 0 {
		return records, false
	}
	engine := db.valueLogTemplateEngine
	store := db.templateStore
	encoded := records
	used := false
	for i := range records {
		payload, ok := engine.Encode(nil, records[i].Value, store)
		if ok {
			if !used {
				encoded = make([]valuelog.Record, len(records))
				copy(encoded, records)
				used = true
			}
			encoded[i].Value = payload
		}
	}
	return encoded, used
}

func (db *DB) readValueLog(key []byte, ptr page.ValuePtr) ([]byte, error) {
	if db.valueLogReader == nil {
		return nil, errors.New("cachingdb: value-log reader unavailable")
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("cachingdb: non value-log pointer %#x", ptr.FileID)
	}
	if db.valueLogEnabled() {
		if err := db.flushValueLogForPtr(ptr); err != nil {
			return nil, err
		}
	}
	return db.valueLogReader.Read(ptr)
}

func (db *DB) readValueLogAppend(key []byte, ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if db.valueLogReader == nil {
		return nil, errors.New("cachingdb: value-log reader unavailable")
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("cachingdb: non value-log pointer %#x", ptr.FileID)
	}
	if db.valueLogEnabled() {
		if err := db.flushValueLogForPtr(ptr); err != nil {
			return nil, err
		}
	}
	return db.valueLogReader.ReadAppend(ptr, dst)
}

func (db *DB) flushValueLogForPtr(ptr page.ValuePtr) error {
	if !db.valueLogEnabled() {
		return nil
	}
	laneID, seq := valuelog.DecodeFileID(ptr.FileID)
	if laneID >= uint32(len(db.lanes)) {
		return nil
	}
	l := &db.lanes[laneID]
	currentSeq := db.currentValueLogSeq(l)
	if currentSeq == int(seq) {
		return db.flushValueLogLane(l)
	}
	return nil
}

func (db *DB) flushValueLog(laneIDs ...int) error {
	if !db.valueLogEnabled() {
		return nil
	}
	if len(laneIDs) == 0 {
		for i := range db.lanes {
			if err := db.flushValueLogLane(&db.lanes[i]); err != nil {
				return err
			}
		}
		return nil
	}

	seen := make(map[int]struct{}, len(laneIDs))
	for _, id := range laneIDs {
		if id < 0 || id >= len(db.lanes) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if err := db.flushValueLogLane(&db.lanes[id]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) flushDeferredValueLogForBackendRead() error {
	if db == nil || !db.deferredValueLogEnabled() || !db.valueLogEnabled() {
		return nil
	}
	dirtySeq := db.backendReadVlogDirtySeq.Load()
	if dirtySeq == 0 || dirtySeq == db.backendReadVlogFlushedSeq.Load() {
		return nil
	}
	if err := db.flushValueLog(); err != nil {
		return err
	}
	db.backendReadVlogFlushedSeq.Store(dirtySeq)
	return nil
}

func (db *DB) syncValueLog(laneIDs ...int) error {
	if !db.valueLogEnabled() {
		return nil
	}
	if len(laneIDs) == 0 {
		for i := range db.lanes {
			if err := db.syncValueLogLane(&db.lanes[i]); err != nil {
				return err
			}
		}
		return nil
	}

	seen := make(map[int]struct{}, len(laneIDs))
	for _, id := range laneIDs {
		if id < 0 || id >= len(db.lanes) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if err := db.syncValueLogLane(&db.lanes[id]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) flushValueLogLane(l *lane) error {
	if l == nil {
		return errWALUnavailable
	}
	if db.splitValueLogEnabled() {
		waitStart := time.Now()
		l.vlogMu.Lock()
		waited := time.Since(waitStart)
		w := l.vlog
		if w == nil {
			l.vlogMu.Unlock()
			return errWALUnavailable
		}
		// Always take vlogMu first so flush acts as a write barrier for in-flight appends.
		if !l.vlogDirty.Load() {
			l.vlogMu.Unlock()
			return nil
		}
		start := time.Now()
		err := w.Flush()
		if db.testOnVlogFlush != nil {
			db.testOnVlogFlush(int(l.id))
		}
		db.debugVlogTiming("vlog_flush", int(l.id), "vlogMu", waited, time.Since(start))
		if err == nil {
			l.vlogDirty.Store(false)
		}
		l.vlogMu.Unlock()
		return err
	}
	waitStart := time.Now()
	l.walMu.Lock()
	waited := time.Since(waitStart)
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	start := time.Now()
	err := w.Flush()
	if db.testOnVlogFlush != nil {
		db.testOnVlogFlush(int(l.id))
	}
	db.debugVlogTiming("wal_flush", int(l.id), "walMu", waited, time.Since(start))
	l.walMu.Unlock()
	return err
}

func (db *DB) syncValueLogLane(l *lane) error {
	if l == nil {
		return errWALUnavailable
	}
	if db.splitValueLogEnabled() {
		waitStart := time.Now()
		l.vlogMu.Lock()
		waited := time.Since(waitStart)
		w := l.vlog
		if w == nil {
			l.vlogMu.Unlock()
			return errWALUnavailable
		}
		start := time.Now()
		err := w.Sync()
		if db.testOnVlogSync != nil {
			db.testOnVlogSync(int(l.id))
		}
		db.debugVlogTiming("vlog_sync", int(l.id), "vlogMu", waited, time.Since(start))
		if err == nil {
			l.vlogDirty.Store(false)
		}
		l.vlogMu.Unlock()
		return err
	}
	waitStart := time.Now()
	l.walMu.Lock()
	waited := time.Since(waitStart)
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	start := time.Now()
	err := w.Sync()
	if db.testOnVlogSync != nil {
		db.testOnVlogSync(int(l.id))
	}
	db.debugVlogTiming("wal_sync", int(l.id), "walMu", waited, time.Since(start))
	l.walMu.Unlock()
	return err
}

func (db *DB) logSegmentPrefix(laneID int) string {
	return fmt.Sprintf("commit-l%d-", laneID)
}

func (db *DB) markValueLogRetain(path string) {
	if path == "" {
		return
	}
	db.valueLogMu.Lock()
	if db.valueLogRetain == nil {
		db.valueLogRetain = make(map[string]struct{})
	}
	db.valueLogRetain[path] = struct{}{}
	db.valueLogMu.Unlock()
}

func (db *DB) forgetValueLogRetain(path string) {
	if path == "" {
		return
	}
	db.valueLogMu.Lock()
	if db.valueLogRetain != nil {
		delete(db.valueLogRetain, path)
	}
	db.valueLogMu.Unlock()
}

func (db *DB) cleanupMissingRetainedValueLog(path string) bool {
	if path == "" {
		return false
	}
	if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
		return false
	}
	db.dropValueLogSegment(path)
	db.mu.Lock()
	db.untrackValueLogSegmentLocked(path)
	db.mu.Unlock()
	db.forgetValueLogRetain(path)
	return true
}

func (db *DB) cleanupOrphanedRetainedValueLog(path string) bool {
	if path == "" {
		return false
	}
	if db.valueLogReader != nil {
		laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if ok && valueLog && laneID >= 0 {
			if id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq)); err == nil {
				var removeErr error
				backoff := 25 * time.Millisecond
				for i := 0; i < 40; i++ {
					removeErr = db.valueLogReader.RemoveSegment(id)
					if removeErr == nil || errors.Is(removeErr, valuelog.ErrFileNotFound) {
						break
					}
					if runtime.GOOS != "windows" {
						return false
					}
					if !isWindowsSharingViolationError(removeErr) {
						return false
					}
					time.Sleep(backoff)
					if backoff < 200*time.Millisecond {
						backoff *= 2
					}
				}
				if removeErr != nil && !errors.Is(removeErr, valuelog.ErrFileNotFound) {
					return false
				}
			}
		}
	}
	if err := db.removeFileRetry(path); err != nil {
		return false
	}
	db.mu.Lock()
	db.untrackValueLogSegmentLocked(path)
	db.mu.Unlock()
	db.forgetValueLogRetain(path)
	db.syncDirBestEffort(db.dir)
	return true
}

func (db *DB) dropValueLogSegment(path string) {
	if db.valueLogReader == nil || path == "" {
		return
	}
	laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
	if !ok || !valueLog {
		return
	}
	if laneID < 0 {
		return
	}
	id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
	if err != nil {
		return
	}
	_ = db.valueLogReader.RemoveSegment(id)
}

func (db *DB) valueLogRetained(path string) bool {
	if path == "" {
		return false
	}
	db.valueLogMu.Lock()
	defer db.valueLogMu.Unlock()
	_, retained := db.valueLogRetain[path]
	return retained
}

func (db *DB) valueLogRetainedStats() (segments int, bytes int64) {
	stats := db.valueLogRetainedStatsDetailed()
	return stats.SegmentsTotal, stats.BytesTotal
}

type valueLogRetainedGenerationStats struct {
	SegmentsTotal int
	SegmentsHot   int
	SegmentsWarm  int
	SegmentsCold  int
	BytesTotal    int64
	BytesHot      int64
	BytesWarm     int64
	BytesCold     int64
}

func (db *DB) valueLogRetainedStatsDetailed() valueLogRetainedGenerationStats {
	var out valueLogRetainedGenerationStats
	db.valueLogMu.Lock()
	if len(db.valueLogRetain) == 0 {
		db.valueLogMu.Unlock()
		return out
	}
	paths := make([]string, 0, len(db.valueLogRetain))
	for path := range db.valueLogRetain {
		paths = append(paths, path)
	}
	db.valueLogMu.Unlock()

	pathSizes := make(map[string]int64)
	currentSizes := make(map[string]int64)
	pathClasses := make(map[string]uint8)
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			l := &db.lanes[i]
			l.vlogMu.Lock()
			for path, size := range l.vlogClosedSizes {
				pathSizes[path] = size
				pathClasses[path] = l.vlogGenerationClass
			}
			if l.vlogPath != "" {
				currentSizes[l.vlogPath] = l.vlogLiveBytes.Load()
				pathClasses[l.vlogPath] = l.vlogGenerationClass
			}
			l.vlogMu.Unlock()
		}
	} else {
		for i := range db.lanes {
			l := &db.lanes[i]
			l.walMu.Lock()
			for path, size := range l.walClosedSizes {
				pathSizes[path] = size
			}
			if l.walPath != "" {
				currentSizes[l.walPath] = l.walLiveBytes.Load()
			}
			l.walMu.Unlock()
		}
	}

	for _, path := range paths {
		out.SegmentsTotal++
		class := pathClasses[path]
		switch class {
		case vlogGenerationClassWarm:
			out.SegmentsWarm++
		case vlogGenerationClassCold:
			out.SegmentsCold++
		default:
			out.SegmentsHot++
		}
		size := int64(0)
		if v, ok := currentSizes[path]; ok {
			size = v
		} else if v, ok := pathSizes[path]; ok {
			size = v
		}
		out.BytesTotal += size
		switch class {
		case vlogGenerationClassWarm:
			out.BytesWarm += size
		case vlogGenerationClassCold:
			out.BytesCold += size
		default:
			out.BytesHot += size
		}
	}
	return out
}

func (db *DB) valueLogRetainedPaths() []string {
	db.valueLogMu.Lock()
	if len(db.valueLogRetain) == 0 {
		db.valueLogMu.Unlock()
		return nil
	}
	paths := make([]string, 0, len(db.valueLogRetain))
	for path := range db.valueLogRetain {
		paths = append(paths, path)
	}
	db.valueLogMu.Unlock()
	return paths
}

// ValueLogRetainedPaths returns a best-effort snapshot of retained value-log
// segment paths currently pinned by cached-mode pointer lifecycle tracking.
func (db *DB) ValueLogRetainedPaths() []string {
	return db.valueLogRetainedPaths()
}

func (db *DB) valueLogProtectedPaths() []string {
	retained := db.valueLogRetainedPaths()
	inUse := db.valueLogInUsePaths()
	if len(retained) == 0 {
		return inUse
	}
	if len(inUse) == 0 {
		return retained
	}
	seen := make(map[string]struct{}, len(retained)+len(inUse))
	paths := make([]string, 0, len(retained)+len(inUse))
	for _, path := range retained {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	for _, path := range inUse {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return paths
}

// valueLogInUsePaths returns a best-effort snapshot of value-log segment paths
// that may be referenced by in-memory state (mutable + queued memtables).
//
// This is intentionally narrower than valueLogRetainedPaths: retained paths can
// include segments that are still referenced in the backend index (and thus
// candidates for rewrite), while in-use paths are only about protecting against
// concurrent writers while online maintenance is running.
func (db *DB) valueLogInUsePaths() []string {
	if db == nil || !db.valueLogEnabled() {
		return nil
	}
	inUse := make(map[string]struct{})
	if db.splitValueLogEnabled() {
		for _, path := range db.currentValueLogPaths() {
			if path == "" {
				continue
			}
			inUse[path] = struct{}{}
		}
		db.mu.RLock()
		for _, paths := range db.queueValueLogPaths {
			for _, path := range paths {
				if path == "" {
					continue
				}
				inUse[path] = struct{}{}
			}
		}
		db.mu.RUnlock()
	} else {
		for _, path := range db.currentWALPaths() {
			if path == "" {
				continue
			}
			inUse[path] = struct{}{}
		}
		db.mu.RLock()
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				if path == "" {
					continue
				}
				inUse[path] = struct{}{}
			}
		}
		db.mu.RUnlock()
	}
	if len(inUse) == 0 {
		return nil
	}
	out := make([]string, 0, len(inUse))
	for path := range inUse {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

// ReadValueLogRecord reads a raw value-log record for diagnostics and tooling.
func (db *DB) ReadValueLogRecord(ptr page.ValuePtr) ([]byte, error) {
	if db == nil || db.valueLogReader == nil {
		return nil, fmt.Errorf("cachingdb: value-log reader unavailable")
	}
	return db.valueLogReader.Read(ptr)
}

type backendPointerProjectionIterator interface {
	IteratorWithOptions(start, end []byte, opts tree.IteratorOptions) (iterator.UnsafeIterator, error)
}

var errForegroundWritesResumed = errors.New("cachingdb: foreground writes resumed")

func (db *DB) foregroundWritesResumedSince(lastWrite int64) bool {
	if db == nil {
		return false
	}
	if lastWrite <= 0 {
		return false
	}
	current := db.lastForegroundWriteUnixNano.Load()
	return current > lastWrite
}

func (db *DB) collectValueLogLiveIDs() (map[uint32]struct{}, error) {
	return db.collectValueLogLiveIDsUntil(0)
}

func (db *DB) collectValueLogLiveIDsUntil(lastWrite int64) (map[uint32]struct{}, error) {
	if db == nil || !db.valueLogEnabled() {
		return make(map[uint32]struct{}), nil
	}
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return nil, err
		}
	}
	live := make(map[uint32]struct{})
	if snapper, ok := db.backend.(interface{ AcquireSnapshot() *backenddb.Snapshot }); ok {
		snap := snapper.AcquireSnapshot()
		if snap != nil {
			state := snap.State()
			p := snap.Pager()
			if state != nil && p != nil {
				reader := newCachedLiveScanReader(valueReaderForBackendState(state), db.valueLogReader)
				leafCtx, leafCancel := db.foregroundWriteResumeContext(lastWrite, 0)
				defer leafCancel()
				if state.RootPageID != 0 {
					if err := db.collectIteratorValueLogLiveIDsUntil(tree.New(p, reader, state.RootPageID).IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection}), live, lastWrite); err != nil {
						_ = snap.Close()
						return nil, err
					}
				}
				if state.SystemRootPageID != 0 {
					if err := db.collectIteratorValueLogLiveIDsUntil(tree.New(p, reader, state.SystemRootPageID).IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection}), live, lastWrite); err != nil {
						_ = snap.Close()
						return nil, err
					}
				}
				if db.foregroundWritesResumedSince(lastWrite) {
					_ = snap.Close()
					return nil, errForegroundWritesResumed
				}
				if err := collectLeafRefValueLogLiveIDs(leafCtx, p, state.RootPageID, reader, live); err != nil {
					if errors.Is(err, context.Canceled) {
						err = errForegroundWritesResumed
					}
					_ = snap.Close()
					return nil, err
				}
				if db.foregroundWritesResumedSince(lastWrite) {
					_ = snap.Close()
					return nil, errForegroundWritesResumed
				}
				if err := collectLeafRefValueLogLiveIDs(leafCtx, p, state.SystemRootPageID, reader, live); err != nil {
					if errors.Is(err, context.Canceled) {
						err = errForegroundWritesResumed
					}
					_ = snap.Close()
					return nil, err
				}
			}
			if err := snap.Close(); err != nil {
				return nil, err
			}
			return live, nil
		}
	}

	var (
		it  iterator.UnsafeIterator
		err error
	)
	if proj, ok := db.backend.(backendPointerProjectionIterator); ok {
		it, err = proj.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	} else {
		it, err = db.backend.Iterator(nil, nil)
	}
	if err != nil {
		return nil, err
	}
	if err := db.collectIteratorValueLogLiveIDsUntil(it, live, lastWrite); err != nil {
		return nil, err
	}

	return live, nil
}

func (db *DB) collectIteratorValueLogLiveIDs(it iterator.UnsafeIterator, live map[uint32]struct{}) error {
	return db.collectIteratorValueLogLiveIDsUntil(it, live, 0)
}

func (db *DB) collectIteratorValueLogLiveIDsUntil(it iterator.UnsafeIterator, live map[uint32]struct{}, lastWrite int64) error {
	if it == nil {
		return nil
	}
	defer it.Close()
	proj, _ := it.(iterator.PointerProjection)
	seen := 0
	for it.Valid() {
		if seen > 0 && seen&foregroundWriteResumeCheckMask == 0 && db.foregroundWritesResumedSince(lastWrite) {
			return errForegroundWritesResumed
		}
		var (
			ptr   page.ValuePtr
			flags byte
		)
		if proj != nil {
			ptr, flags = proj.UnsafePointerProjection()
		} else {
			_, ptr, flags = it.UnsafeEntry()
		}
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			live[ptr.FileID] = struct{}{}
			if err := db.collectNestedValueLogLiveIDsFromOuterLeaf(ptr, live); err != nil {
				return err
			}
		}
		it.Next()
		seen++
	}
	if err := it.Error(); err != nil {
		return err
	}
	if db.foregroundWritesResumedSince(lastWrite) {
		return errForegroundWritesResumed
	}
	return nil
}

func (db *DB) collectNestedValueLogLiveIDsFromOuterLeaf(ptr page.ValuePtr, live map[uint32]struct{}) error {
	if db == nil || len(live) == 0 || db.valueLogReader == nil {
		return nil
	}
	raw, err := db.valueLogReader.Read(ptr)
	if err != nil {
		return err
	}
	if !outerleaf.HasMagic(raw) {
		return nil
	}
	block, err := outerleaf.DecodeBlockLeaseWithVerify(raw, false)
	if err != nil {
		return nil
	}
	defer block.Release()

	typed, err := block.TypedEntries(nil)
	if err != nil {
		return nil
	}
	for i := range typed {
		if typed[i].Kind != outerleaf.EntryKindBlobRef {
			continue
		}
		if typed[i].BlobPtr.FileID == 0 {
			continue
		}
		if !page.IsValueLogFileID(typed[i].BlobPtr.FileID) {
			return fmt.Errorf("cachingdb: invalid nested blob pointer file %d", typed[i].BlobPtr.FileID)
		}
		live[typed[i].BlobPtr.FileID] = struct{}{}
	}
	return nil
}

func (db *DB) checkValueLogRetention() {
	limit := db.maxValueLogRetainedBytes
	if limit <= 0 || !db.valueLogEnabled() {
		return
	}
	_, bytes := db.valueLogRetainedStats()
	if bytes <= limit {
		db.valueLogWarned.Store(false)
		return
	}
	if db.valueLogWarned.CompareAndSwap(false, true) {
		db.reportError(fmt.Errorf("cachingdb: retained value-log bytes %d exceed limit %d", bytes, limit))
	}
}

func (db *DB) allowValueLogPointers() bool {
	if !db.valueLogEnabled() {
		return false
	}
	limit := db.maxValueLogRetainedBytesHard
	if limit <= 0 {
		return true
	}
	bytes := db.valueLogRetainedClosedBytes.Load()
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			l := &db.lanes[i]
			if l.vlogPath != "" && l.vlogPath == l.vlogRetainedPath {
				bytes += l.vlogLiveBytes.Load()
			}
		}
	}
	if bytes >= limit {
		if db.valueLogHardCapWarned.CompareAndSwap(false, true) {
			db.reportError(fmt.Errorf("cachingdb: retained value-log bytes %d exceed hard cap %d; disabling new value-log pointers", bytes, limit))
		}
		return false
	}
	db.valueLogHardCapWarned.Store(false)
	return true
}

type valueLogZombieMarker interface {
	MarkValueLogZombie(id uint32) error
}

type valueLogSetRefresher interface {
	RefreshValueLogSet() error
}

func (db *DB) pruneRetainedValueLogs() {
	if !db.valueLogEnabled() {
		return
	}
	paths := db.valueLogRetainedPaths()
	if len(paths) == 0 {
		return
	}

	inUse := make(map[string]struct{})
	for _, path := range db.valueLogInUsePaths() {
		inUse[path] = struct{}{}
	}

	candidatePaths := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, ok := inUse[path]; ok {
			continue
		}
		if db.cleanupMissingRetainedValueLog(path) {
			continue
		}
		candidatePaths = append(candidatePaths, path)
	}
	if len(candidatePaths) == 0 {
		return
	}

	live, err := db.collectValueLogLiveIDsUntil(db.lastForegroundWriteUnixNano.Load())
	if err != nil {
		if errors.Is(err, errForegroundWritesResumed) {
			return
		}
		db.reportError(fmt.Errorf("cachingdb: failed to scan value-log pointers: %w", err))
		return
	}

	removed := false
	marked := false
	for _, path := range candidatePaths {
		laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if !ok || !valueLog {
			continue
		}
		if laneID < 0 {
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
		if err != nil {
			continue
		}
		if _, ok := live[id]; ok {
			continue
		}

		if marker, ok := db.backend.(valueLogZombieMarker); ok {
			if db.valueLogReader != nil {
				_ = db.valueLogReader.EvictSegment(id)
			}
			if err := marker.MarkValueLogZombie(id); err != nil {
				if errors.Is(err, valuelog.ErrFileNotFound) && db.cleanupOrphanedRetainedValueLog(path) {
					removed = true
					continue
				}
				if db.cleanupMissingRetainedValueLog(path) {
					continue
				}
				db.reportError(fmt.Errorf("cachingdb: failed to mark value-log %d zombie: %w", id, err))
				continue
			}
			marked = true
		} else {
			db.dropValueLogSegment(path)
			_ = db.removeFileRetry(path)
			db.mu.Lock()
			db.untrackValueLogSegmentLocked(path)
			db.mu.Unlock()
			removed = true
		}
		db.forgetValueLogRetain(path)
	}

	if marked {
		if refresher, ok := db.backend.(valueLogSetRefresher); ok {
			if err := refresher.RefreshValueLogSet(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: failed to refresh value-log set: %w", err))
			}
		}
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
}

func (db *DB) cleanupProcessedRetainedRewriteSources(reason uint32, processedRewriteIDs []uint32) {
	if db == nil || len(processedRewriteIDs) == 0 || !db.valueLogEnabled() {
		return
	}
	liveIDs, err := db.collectValueLogLiveIDsUntil(db.lastForegroundWriteUnixNano.Load())
	if err != nil {
		db.debugVlogMaintf(
			"rewrite_retained_cleanup_scan_err reason=%s source_ids=%d err=%v",
			vlogGenerationReasonString(reason),
			len(processedRewriteIDs),
			err,
		)
		return
	}
	inUseIDs := valueLogIDsFromPaths(db.valueLogInUsePaths())
	currentIDs := valueLogIDsFromPaths(db.currentValueLogPaths())
	queuedIDs := valueLogIDsFromPaths(db.queuedValueLogPathsSnapshot())
	removed := 0
	for _, id := range processedRewriteIDs {
		if _, ok := liveIDs[id]; ok {
			continue
		}
		if _, ok := inUseIDs[id]; ok {
			continue
		}
		if _, ok := currentIDs[id]; ok {
			continue
		}
		if _, ok := queuedIDs[id]; ok {
			continue
		}
		if db.valueLogReader == nil {
			continue
		}
		path := db.valueLogReader.SegmentPath(id)
		if path == "" {
			continue
		}
		if db.cleanupMissingRetainedValueLog(path) {
			removed++
			continue
		}
		if !db.valueLogRetained(path) {
			continue
		}
		if db.cleanupOrphanedRetainedValueLog(path) {
			removed++
		}
	}
	if removed > 0 {
		db.debugVlogMaintf(
			"rewrite_retained_cleanup_done reason=%s source_ids=%d removed=%d",
			vlogGenerationReasonString(reason),
			len(processedRewriteIDs),
			removed,
		)
	}
}

func (db *DB) debugVlogGenerationProcessedSourceState(reason uint32, processedRewriteIDs []uint32) {
	if db == nil || len(processedRewriteIDs) == 0 || !debugVlogMaintOn() {
		return
	}
	liveIDs, err := db.collectValueLogLiveIDsUntil(db.lastForegroundWriteUnixNano.Load())
	if err != nil {
		db.debugVlogMaintf(
			"rewrite_source_state_scan_err reason=%s source_ids=%d err=%v",
			vlogGenerationReasonString(reason),
			len(processedRewriteIDs),
			err,
		)
		return
	}
	retainedPaths := db.valueLogRetainedPaths()
	inUsePaths := db.valueLogInUsePaths()
	currentPaths := db.currentValueLogPaths()
	queuedPaths := db.queuedValueLogPathsSnapshot()
	retainedIDs := valueLogIDsFromPaths(retainedPaths)
	inUseIDs := valueLogIDsFromPaths(inUsePaths)
	currentIDs := valueLogIDsFromPaths(currentPaths)
	queuedIDs := valueLogIDsFromPaths(queuedPaths)
	pathByID := valueLogPathByID(retainedPaths)
	for id, path := range valueLogPathByID(inUsePaths) {
		if _, ok := pathByID[id]; !ok {
			pathByID[id] = path
		}
	}
	for id, path := range valueLogPathByID(currentPaths) {
		if _, ok := pathByID[id]; !ok {
			pathByID[id] = path
		}
	}
	for id, path := range valueLogPathByID(queuedPaths) {
		if _, ok := pathByID[id]; !ok {
			pathByID[id] = path
		}
	}
	for _, id := range processedRewriteIDs {
		lane, seq := valuelog.DecodeFileID(id)
		_, live := liveIDs[id]
		_, retained := retainedIDs[id]
		_, inUse := inUseIDs[id]
		_, current := currentIDs[id]
		_, queued := queuedIDs[id]
		path := pathByID[id]
		exists := false
		if path != "" {
			if _, statErr := os.Stat(path); statErr == nil {
				exists = true
			}
		}
		db.debugVlogMaintf(
			"rewrite_source_state reason=%s file_id=%d lane=%d seq=%d live_backend=%t retained=%t in_use=%t current=%t queued=%t exists=%t path=%s",
			vlogGenerationReasonString(reason),
			id,
			lane,
			seq,
			live,
			retained,
			inUse,
			current,
			queued,
			exists,
			path,
		)
	}
}

func (db *DB) retainedPrunePressureBytes() int64 {
	if db == nil || !db.valueLogEnabled() {
		return 0
	}
	if limit := db.maxValueLogRetainedBytes; limit > 0 {
		return limit
	}
	if limit := db.maxValueLogRetainedBytesHard; limit > 0 {
		if limit <= 1 {
			return 1
		}
		return limit / 2
	}
	const (
		retainedPruneSegmentPressureMultiplier = 4
		// Keep retained-prune pressure comfortably above tiny test/default
		// segment sizes so prune cadence stays tied to substantial backlog.
		retainedPrunePressureFloorBytes = 1 << 30
	)
	pressure := db.valueLogMaxSegmentBytes * retainedPruneSegmentPressureMultiplier
	if pressure <= 0 {
		pressure = retainedPrunePressureFloorBytes
	}
	if db.flushThreshold > 0 {
		ft := db.flushThreshold
		if ft > math.MaxInt64/8 {
			ft = math.MaxInt64
		} else {
			ft *= 8
		}
		if ft > pressure {
			pressure = ft
		}
	}
	if pressure < retainedPrunePressureFloorBytes {
		pressure = retainedPrunePressureFloorBytes
	}
	return pressure
}

func (db *DB) shouldScheduleRetainedValueLogPrune() bool {
	if db == nil || !db.valueLogEnabled() {
		return false
	}
	closed := db.valueLogRetainedClosedBytes.Load()
	if closed <= 0 {
		return false
	}
	return closed >= db.retainedPrunePressureBytes()
}

func (db *DB) scheduleRetainedValueLogPrune() {
	if db == nil || !db.valueLogEnabled() {
		return
	}
	if db.testSkipRetainedPrune {
		return
	}
	db.retainedPruneMu.Lock()
	if db.closing.Load() {
		db.retainedPruneMu.Unlock()
		return
	}
	if db.retainedPruneDone != nil {
		db.retainedPruneMu.Unlock()
		return
	}
	done := make(chan struct{})
	db.retainedPruneDone = done
	db.retainedPruneMu.Unlock()
	go func() {
		defer func() {
			db.retainedPruneMu.Lock()
			close(done)
			db.retainedPruneDone = nil
			db.retainedPruneMu.Unlock()
		}()
		db.waitForForegroundMaintenanceQuietWindow(retainedPruneQuietWindow)
		if !db.shouldScheduleRetainedValueLogPrune() {
			return
		}
		// Retained prune is opportunistic reclaim; do not compete with checkpoint
		// cutovers or other backend maintenance windows.
		db.checkpointMu.Lock()
		for db.checkpointing.Load() || db.maintenanceActive.Load() {
			if db.closing.Load() {
				db.checkpointMu.Unlock()
				return
			}
			db.checkpointCond.Wait()
		}
		db.checkpointMu.Unlock()
		now := time.Now()
		last := db.retainedPruneLastStartUnixNano.Load()
		if last > 0 && now.Sub(time.Unix(0, last)) < retainedPruneMinInterval {
			return
		}
		db.retainedPruneLastStartUnixNano.Store(now.UnixNano())
		db.pruneRetainedValueLogs()
	}()
}

func (db *DB) retainedPruneActive() bool {
	if db == nil || !db.valueLogEnabled() {
		return false
	}
	db.retainedPruneMu.Lock()
	defer db.retainedPruneMu.Unlock()
	return db.retainedPruneDone != nil
}

func (db *DB) waitForRetainedValueLogPrune() {
	if db == nil || !db.valueLogEnabled() {
		return
	}
	db.retainedPruneMu.Lock()
	done := db.retainedPruneDone
	db.retainedPruneMu.Unlock()
	if done != nil {
		<-done
	}
}

func (db *DB) checkpointForBackendMaintenance() error {
	return db.runWithBackendMaintenance(nil)
}

type backendMaintenanceOptions struct {
	skipCheckpoint        bool
	skipRetainedPruneWait bool
}

func (db *DB) runWithBackendMaintenance(fn func() error) error {
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{}, fn)
}

func (db *DB) runWithBackendMaintenanceOptions(opts backendMaintenanceOptions, fn func() error) error {
	if db == nil {
		if fn != nil {
			return fn()
		}
		return nil
	}
	db.checkpointMu.Lock()
	for db.checkpointing.Load() || db.maintenanceActive.Load() {
		db.checkpointCond.Wait()
	}
	// Publish the maintenance-active flag while checkpointMu is held so Checkpoint
	// and background maintenance serialize through the same mutex/cond state.
	db.maintenanceActive.Store(true)
	db.checkpointMu.Unlock()
	defer func() {
		db.checkpointMu.Lock()
		db.maintenanceActive.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}()
	if !opts.skipCheckpoint {
		if err := db.Checkpoint(); err != nil {
			return err
		}
	}
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return err
		}
	}
	if fn != nil {
		return fn()
	}
	return nil
}

func hashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func (db *DB) shardIndex(key []byte) int {
	if len(db.mutableShards) <= 1 {
		return 0
	}
	return int(hashKey(key) & db.mutableShardMask)
}

func (db *DB) shardForKey(key []byte) *memShard {
	return &db.mutableShards[db.shardIndex(key)]
}

func (db *DB) laneForShardIndex(shardID int) int {
	if len(db.lanes) == 0 {
		return 0
	}
	if shardID < 0 {
		shardID = 0
	}
	if db.valueLogGenerationPolicy == uint8(backenddb.ValueLogGenerationHotWarmCold) && len(db.valueLogHotLanes) > 0 {
		return db.valueLogHotLanes[shardID%len(db.valueLogHotLanes)]
	}
	return shardID % len(db.lanes)
}

func (db *DB) rebuildGenerationLaneSets() {
	if db == nil {
		return
	}
	db.valueLogHotLanes = db.valueLogHotLanes[:0]
	db.valueLogWarmLanes = db.valueLogWarmLanes[:0]
	db.valueLogColdLanes = db.valueLogColdLanes[:0]
	for i := range db.lanes {
		switch db.lanes[i].vlogGenerationClass {
		case vlogGenerationClassWarm:
			db.valueLogWarmLanes = append(db.valueLogWarmLanes, i)
		case vlogGenerationClassCold:
			db.valueLogColdLanes = append(db.valueLogColdLanes, i)
		default:
			db.valueLogHotLanes = append(db.valueLogHotLanes, i)
		}
	}
	if len(db.valueLogHotLanes) == 0 && len(db.lanes) > 0 {
		db.valueLogHotLanes = append(db.valueLogHotLanes, 0)
	}
}

func (db *DB) valueLogMaxSegmentBytesForLane(l *lane) int64 {
	if db == nil {
		return 0
	}
	// Legacy cap still applies when generation class targets are unset.
	base := db.valueLogMaxSegmentBytes
	if db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) || l == nil {
		return base
	}
	target := int64(0)
	switch l.vlogGenerationClass {
	case vlogGenerationClassWarm:
		target = db.valueLogGenerationWarmTarget
	case vlogGenerationClassCold:
		target = db.valueLogGenerationColdTarget
	default:
		target = db.valueLogGenerationHotTarget
	}
	if target <= 0 {
		return base
	}
	if base > 0 && base < target {
		return base
	}
	return target
}

func (db *DB) shardExceedsLimit(shard *memShard, addBytes int64) bool {
	if maxMemtableBytesPerShard <= 0 {
		return false
	}
	return shard.bytes+addBytes > maxMemtableBytesPerShard
}

func (db *DB) newBackendBatchWithSize(size int) batch.Interface {
	if db == nil || db.backend == nil {
		return nil
	}
	type batchSizer interface {
		NewBatchWithSize(size int) batch.Interface
	}
	if size < 0 {
		size = 0
	}
	maxSize := flushBackendBatchInitEntries
	if db != nil && db.flushBackendInitEntries > 0 {
		maxSize = db.flushBackendInitEntries
	}
	if maxSize > 0 && size > maxSize {
		size = maxSize
	}
	if sizer, ok := db.backend.(batchSizer); ok {
		return sizer.NewBatchWithSize(size)
	}
	return db.backend.NewBatch()
}

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
	GetUnsafe(key []byte) ([]byte, error)
	GetAppend(key, dst []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
	ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error)
	NewBatch() batch.Interface
	Close() error
	Print() error
	Stats() map[string]string
}

type backendValueLogRewriter interface {
	ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error)
}

type backendValueLogRewritePlanner interface {
	ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error)
}

type backendValueLogSegmentPresence interface {
	ValueLogHasSegment(id uint32) bool
}

type backendValueLogGCer interface {
	ValueLogGC(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error)
}

type backendIndexVacuumer interface {
	VacuumIndexOnline(ctx context.Context) error
}

type backendIndexFragmenter interface {
	FragmentationReport() (map[string]string, error)
}

type Options struct {
	FlushThreshold int64

	// MemtableMode selects the in-memory write buffer implementation.
	// Supported: "skiplist", "hash_sorted", "btree", "append_only", "adaptive".
	// Use "adaptive" or "adaptive:<mode>" to switch per-rotation based on workload.
	MemtableMode string

	// MemtableShards controls the number of mutable memtable shards. Values <= 0
	// use a default derived from GOMAXPROCS. The count is rounded down to a power
	// of two.
	MemtableShards int
	// DomainIngressWorkers enables experimental domain-local write ingress queues.
	// Values <= 0 keep the legacy direct caller write path.
	DomainIngressWorkers int
	// DomainIngressQueueSize configures the per-worker ingress queue length.
	// Values <= 0 use a default.
	DomainIngressQueueSize int

	// Legacy backpressure knob: queue length limit.
	// 0 uses the default (4). <0 disables writer backpressure entirely.
	MaxQueuedMemtables int

	// Adaptive backpressure knobs (seconds/bytes). If any of these are non-zero,
	// the caching layer uses backlog-bytes thresholds instead of queue length.
	SlowdownBacklogSeconds float64
	StopBacklogSeconds     float64
	MaxBacklogBytes        int64

	// Writer flush assist limits when backpressure triggers.
	WriterFlushMaxMemtables int
	WriterFlushMaxDuration  time.Duration

	// FlushBuildConcurrency controls how many goroutines may be used to build a
	// combined flush batch from multiple immutable memtables. Values <= 1 disable
	// parallelism.
	FlushBuildConcurrency int
	// FlushBuildMinEntries gates the parallel build path by total entries.
	// Values <= 0 use a default of 16k.
	FlushBuildMinEntries int
	// FlushBuildMinUnits gates the parallel build path by number of queued units.
	// Values <= 0 use a default of 2.
	FlushBuildMinUnits int
	// FlushBuildChunkCap controls the maximum entries per build chunk.
	// Values < 0 use the fixed default of 8192, 0 enables adaptive chunk sizing,
	// and values > 0 set a fixed cap.
	FlushBuildChunkCap int
	// FlushBuildChunkTargetBytes controls adaptive chunk sizing (bytes per chunk).
	// Values <= 0 use a default of 2MiB.
	FlushBuildChunkTargetBytes int
	// FlushBuildChunkMinBytes clamps adaptive chunk sizes (minimum bytes).
	// Values <= 0 use a default of 1MiB.
	FlushBuildChunkMinBytes int
	// FlushBuildChunkMaxBytes clamps adaptive chunk sizes (maximum bytes).
	// Values <= 0 use a default of 4MiB.
	FlushBuildChunkMaxBytes int
	// FlushBuildPrefetchUnits controls how many memtables to start building ahead
	// of the consumer. Values <= 0 use FlushBuildConcurrency.
	FlushBuildPrefetchUnits int

	// FlushBackendMaxEntries caps how many operations are buffered into a single
	// backend batch before committing it and continuing with a fresh batch.
	//
	// This increases backend commit cadence during very large flushes, which can
	// reduce index.db high-watermark growth under small KeepRecent windows by
	// making retired pages eligible for reuse sooner.
	//
	// 0 uses the internal default. Negative disables chunking (single backend
	// commit per flush).
	FlushBackendMaxEntries int
	// FlushBackendMaxBatches caps how many intermediate backend commits a single
	// flush may emit. This bounds zipper/apply overhead when FlushBackendMaxEntries
	// is very small relative to the flush size.
	//
	// 0 uses the internal default. Negative disables the cap.
	FlushBackendMaxBatches int

	// DisableWAL disables the redo/journal log while keeping the value log enabled.
	DisableWAL bool
	// JournalLanes controls the number of active commit/value log lanes
	// (0=GOMAXPROCS-aware default).
	// Max supported lanes is 255; value-log segment sequence per lane is capped at 8,388,607.
	JournalLanes int
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// JournalCompression enables best-effort zstd compression for journal/commitlog
	// segments (metadata only). The writer only keeps compressed bytes when they
	// are smaller than the raw payload, so compression never causes size
	// amplification.
	JournalCompression bool
	// RelaxedSync disables fsync on Sync operations.
	RelaxedSync bool
	// ValueLogPointerThreshold controls when WAL/vlog pointers are used.
	// Values <= 0 use a default threshold. In relaxed durability modes, the
	// default is smaller to avoid catastrophic update-heavy cliffs at large key
	// counts by pushing moderate values into the value log.
	ValueLogPointerThreshold int
	// IndexOuterLeavesInValueLog stores B+Tree leaf pages (the pages containing
	// key/value entries) in the persistent value log instead of index.db.
	IndexOuterLeavesInValueLog bool
	// ValueLogDomainInlineThresholds configures optional per-domain overrides
	// for inline-vs-pointer placement. Longest-prefix match wins.
	ValueLogDomainInlineThresholds []backenddb.ValueLogDomainThreshold
	// ValueLogRawWritevMinAvgBytes controls raw grouped-frame writev usage for
	// the value log.
	//
	// 0 uses adaptive mode (no average-bytes floor); values >0 require average
	// payload bytes/record to meet this floor before raw writev is considered.
	ValueLogRawWritevMinAvgBytes int
	// ValueLogRawWritevMinBatchRecords controls the minimum grouped records before
	// raw writev is considered for value-log appends.
	//
	// Values <=0 use a default of 8.
	ValueLogRawWritevMinBatchRecords int
	// ValueLogCompression selects value-log compression behavior:
	// 0=default(unset; normalized to auto by TreeDB Open), 1=off, 2=block,
	// 3=dict, 4=auto.
	ValueLogCompression uint8
	// ValueLogBlockCodec selects block codec when block compression is enabled:
	// 0=snappy, 1=lz4.
	ValueLogBlockCodec uint8
	// ValueLogBlockTargetCompressedBytes controls block-mode grouped frame K
	// adaptation target (0=default).
	ValueLogBlockTargetCompressedBytes int
	// ValueLogIncompressibleHoldBytes configures auto-mode incompressible hold
	// window bytes (0=default).
	ValueLogIncompressibleHoldBytes int
	// ValueLogIncompressibleProbeBytes configures auto-mode hold probe interval
	// bytes (0=default).
	ValueLogIncompressibleProbeBytes int
	// ValueLogAutoPolicy controls auto-mode dict-vs-block bias:
	// 0=balanced, 1=throughput, 2=size.
	ValueLogAutoPolicy uint8
	// ValueLogMaxSegmentBytes caps the size of a single value-log segment file.
	// 0 disables the cap.
	//
	// This is an internal safety knob used by experimental index encodings
	// (e.g. packed on-disk ValuePtr) that require value-log offsets stay within a
	// smaller representable range.
	ValueLogMaxSegmentBytes int64
	// ValueLogGenerationPolicy selects generational mode:
	// 0=default(unset; normalized to hot_warm_cold by Open),
	// 1=off, 2=hot_warm_cold.
	ValueLogGenerationPolicy uint8
	// ValueLogGenerationHotSegmentTargetBytes configures hot segment target size.
	ValueLogGenerationHotSegmentTargetBytes int64
	// ValueLogGenerationWarmSegmentTargetBytes configures warm segment target size.
	ValueLogGenerationWarmSegmentTargetBytes int64
	// ValueLogGenerationColdSegmentTargetBytes configures cold segment target size.
	ValueLogGenerationColdSegmentTargetBytes int64
	// ValueLogRewriteBudgetBytesPerSec configures incremental rewrite byte budget.
	ValueLogRewriteBudgetBytesPerSec int64
	// ValueLogRewriteBudgetRecordsPerSec configures incremental rewrite record budget.
	ValueLogRewriteBudgetRecordsPerSec int
	// ValueLogRewriteTriggerStaleRatioPPM triggers rewrite by stale/live ratio.
	ValueLogRewriteTriggerStaleRatioPPM uint32
	// ValueLogRewriteTriggerTotalBytes triggers rewrite by retained bytes.
	ValueLogRewriteTriggerTotalBytes int64
	// ValueLogRewriteTriggerChurnPerSec triggers rewrite by churn rate.
	ValueLogRewriteTriggerChurnPerSec int64
	// ForceValueLogPointers stores all values out-of-line in the value log.
	ForceValueLogPointers bool
	// DisableReadChecksum skips CRC verification on value-log reads.
	DisableReadChecksum bool
	// AllowUnsafe acknowledges unsafe durability options.
	// When false, Open will reject DisableWAL or RelaxedSync.
	AllowUnsafe bool
	// MaxValueLogRetainedBytes emits a warning once retained value-log bytes
	// reach or exceed this threshold and also acts as the soft retained-byte
	// trigger for background prune scheduling (0 disables both).
	MaxValueLogRetainedBytes int64
	// MaxValueLogRetainedBytesHard disables value-log pointers for new large
	// values once retained bytes exceed this threshold (0 disables the cap).
	MaxValueLogRetainedBytesHard int64

	// ValueLogDictTrain configures background dictionary training for value-log frame compression.
	// TrainBytes <= 0 disables training.
	ValueLogDictTrain compression.TrainConfig
	// ValueLogDictMaxK clamps the maximum group size (K) used for dict-compressed
	// value-log frames. Values <= 0 use the default (32).
	ValueLogDictMaxK int
	// ValueLogDictFrameEncodeLevel controls the zstd encoder level used for
	// dict-compressed value-log frames. Values <= 0 use SpeedFastest.
	ValueLogDictFrameEncodeLevel zstd.EncoderLevel
	// ValueLogDictFrameEnableEntropy enables entropy coding for dict-compressed
	// frames (higher ratio, lower throughput).
	ValueLogDictFrameEnableEntropy bool
	// ValueLogDictAdaptiveRatio enables adaptive pause of dict compression when payload ratios degrade.
	// 0 disables.
	ValueLogDictAdaptiveRatio float64
	// ValueLogDictMetricsWindowBytes controls the metrics window size (0=default).
	ValueLogDictMetricsWindowBytes int
	// ValueLogDictMetricsMinRecords is a minimum record count before pausing (0=default).
	ValueLogDictMetricsMinRecords int
	// ValueLogDictMetricsPauseBytes controls pause duration in bytes (0=default).
	ValueLogDictMetricsPauseBytes int
	// ValueLogDictIncompressibleHoldBytes enables classifier-driven hold mode for
	// high-entropy streams. While hold mode is active, dict compression attempts
	// and trainer collection are bypassed until hold bytes are consumed.
	//
	// 0 uses profile/default hold configuration; <0 explicitly disables hold
	// mode and opts out of profile defaults.
	ValueLogDictIncompressibleHoldBytes int
	// ValueLogDictProbeIntervalBytes controls periodic probe attempts while
	// incompressible hold mode is active.
	//
	// Values <=0 use a default derived from hold bytes.
	ValueLogDictProbeIntervalBytes int
	// ValueLogDictMinPayloadSavingsRatio rejects newly trained dictionaries whose
	// payload ratio does not improve by at least this fraction (0 uses a
	// throughput-oriented default: 0.02 normally, 0.05 with force pointers or
	// WAL disabled).
	ValueLogDictMinPayloadSavingsRatio float64

	// ValueLogCompressionAutotune configures the wall-time value-log compression autotuner.
	// Cached mode only (value log enabled by default).
	ValueLogCompressionAutotune valuelog.AutotuneOptions

	// ValueLogTemplateMode controls template-based compression for value-log values.
	ValueLogTemplateMode template.Mode
	// ValueLogTemplateConfig controls template creation and encoding behavior.
	ValueLogTemplateConfig template.Config
	// ValueLogTemplateReadStrict controls strict template decode behavior.
	ValueLogTemplateReadStrict bool

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)
}

// DictStore provides access to the current dictionary ID for write freezing.
type DictStore interface {
	GetCurrent(ctx context.Context) (uint64, error)
	GetDictBytes(ctx context.Context, dictID uint64) ([]byte, error)
}

type batchArenaLease struct {
	refs   int
	chunks [][]byte
	bytes  int64
}

type appendOnlyDirectArenaLease struct {
	shardID uint16
	chunks  [][]byte
	bytes   int64
}

type memtableViewDeferredInfo struct {
	memtables     int64
	bytes         int64
	sinceUnixNano int64
}

type memtableViewLifecycleTelemetry struct {
	retainTotal  atomic.Uint64
	releaseTotal atomic.Uint64

	leasesInFlight    atomic.Int64
	leasesInFlightMax atomic.Int64

	deferredViewsCurrent     atomic.Int64
	deferredViewsMax         atomic.Int64
	deferredViewsTotal       atomic.Uint64
	deferredMemtablesCurrent atomic.Int64
	deferredMemtablesMax     atomic.Int64
	deferredMemtablesTotal   atomic.Uint64
	deferredBytesCurrent     atomic.Int64
	deferredBytesMax         atomic.Int64
	deferredBytesTotal       atomic.Uint64

	deferredMu sync.Mutex
	deferred   map[*memtableView]memtableViewDeferredInfo

	oldestDeferredUnixNano atomic.Int64
}

type DB struct {
	mu      sync.RWMutex
	flushMu sync.Mutex
	writeMu sync.RWMutex
	statsMu sync.Mutex // Re-introduce global statsMu for isolation
	bpMu    sync.Mutex
	bpCond  *sync.Cond

	// Commit workers removed; backend commits are synchronous.

	checkpointMu      sync.Mutex
	checkpointCond    *sync.Cond
	checkpointing     atomic.Bool
	maintenanceActive atomic.Bool

	// Level 0 (Memory)
	mutableShards                 []memShard
	mutableShardMask              uint64
	mutableBytes                  atomic.Int64
	mutableThreshold              atomic.Int64
	rotatePending                 atomic.Bool
	queue                         []memtable.Table
	queueShardIDs                 []uint16
	queueLaneIDs                  []uint16
	queueIDs                      []uint64
	queueEnqueueNS                []int64
	nextQueueID                   atomic.Uint64
	appendOnlyEntryHint           atomic.Int32
	batchEntryHint                atomic.Int32
	batchCopyBytesHint            atomic.Int32
	batchArenaLeaseMu             sync.Mutex
	batchArenaLeasesByMem         map[memtable.Table][]*batchArenaLease
	batchArenaLeaseBytes          atomic.Int64
	batchArenaLeaseBytesMax       atomic.Int64
	batchArenaAllocRequestedBytes atomic.Uint64
	batchArenaAllocClassBytes     atomic.Uint64
	batchArenaUsedBytes           atomic.Uint64
	batchArenaTailWasteBytes      atomic.Uint64
	batchArenaTailCompactRuns     atomic.Uint64
	batchArenaTailCompactCopied   atomic.Uint64
	batchArenaTailCompactSaved    atomic.Uint64
	batchEntriesPool              sync.Pool
	batchShardEntriesPool         sync.Pool
	batchIntPool                  sync.Pool

	// memtables is an RCU-style snapshot of (mutable, queue, queueRanges).
	// Readers load it atomically to avoid holding db.mu around memtable access.
	memtables                        atomic.Pointer[memtableView]
	hashSortedIndexer                *memtable.HashSortedIndexer
	appendOnlyMemPool                sync.Pool
	appendOnlyMemLeaseMu             sync.Mutex
	appendOnlyMemLeases              []*memtable.AppendOnly
	appendOnlyMemLeaseHitTotal       atomic.Uint64
	appendOnlyMemPoolHitTotal        atomic.Uint64
	appendOnlyMemNewAllocTotal       atomic.Uint64
	appendOnlyMemNewAllocWithQueue   atomic.Uint64
	appendOnlyMemNewAllocQueueBytes  atomic.Uint64
	appendOnlyDirectArenaLeaseMu     sync.Mutex
	appendOnlyDirectArenaLeasesByMem map[memtable.Table]*appendOnlyDirectArenaLease
	pendingRetiredMems               []memtable.Table
	memtableViewTelemetry            memtableViewLifecycleTelemetry
	retainedArenaTrimLastUnixNano    atomic.Int64
	queueRanges                      []keyRange
	queueWALPaths                    [][]string
	queueValueLogPaths               [][]string
	backendRange                     keyRange
	backendRangeKnown                bool
	backendRangeInit                 sync.Once
	backendRangeErr                  error

	// Durability
	lanes         []lane
	laneMu        sync.Mutex
	laneCond      *sync.Cond
	nextLane      int
	flushLaneMu   []sync.Mutex
	nextCommitSeq atomic.Uint64
	walAckMu      sync.Mutex
	walErr        error
	nextRID       atomic.Uint64

	// Legacy flags removed from public options; retained internally for code paths.
	disableValueLog            bool
	splitValueLog              bool
	memtableValueLogPointers   bool
	indexOuterLeavesInValueLog bool

	inlineThreshold                int
	valueLogThreshold              int
	valueLogDomainThresholds       []backenddb.ValueLogDomainThreshold
	outerLeafBlockTargetBytes      int
	outerLeafBlockCodec            uint8
	outerLeafBlockRestart          int
	outerLeafBlobThresholdBytes    int
	forceValueLogPointers          bool
	valueLogRawWritevMinAvgBytes   int
	valueLogRawWritevMinRecords    int
	valueLogCompressionMode        uint8
	valueLogBlockCodec             valuelog.BlockCodec
	valueLogBlockTargetBytes       int
	valueLogIncompressibleHold     uint64
	valueLogIncompressibleProbe    uint64
	valueLogAutoPolicy             uint8
	valueLogGenerationPolicy       uint8
	valueLogGenerationHotTarget    int64
	valueLogGenerationWarmTarget   int64
	valueLogGenerationColdTarget   int64
	valueLogRewriteBudgetBytes     int64
	valueLogRewriteBudgetRecords   int
	valueLogRewriteTriggerRatioPPM uint32
	valueLogRewriteTriggerBytes    int64
	valueLogRewriteTriggerChurn    int64
	valueLogReader                 *valuelog.Manager
	valueLogHotLanes               []int
	valueLogWarmLanes              []int
	valueLogColdLanes              []int
	valueLogMu                     sync.Mutex
	valueLogRetain                 map[string]struct{}
	backendReadVlogDirtySeq        atomic.Uint64
	backendReadVlogFlushedSeq      atomic.Uint64
	valueLogWarned                 atomic.Bool
	valueLogHardCapWarned          atomic.Bool
	valueLogRetainedClosedBytes    atomic.Int64
	maxValueLogRetainedBytes       int64
	maxValueLogRetainedBytesHard   int64

	// Level 1 (Disk)
	backend       BackendDB
	dictStore     DictStore
	templateStore template.Store

	// Value-log dictionary compression (cached mode).
	valueLogDictTrain              compression.TrainConfig
	valueLogDictMaxK               int
	valueLogDictFrameEncodeLevel   zstd.EncoderLevel
	valueLogDictFrameEnableEntropy bool
	valueLogDictSampleStride       uint64
	valueLogDictSampleStrideCount  atomic.Uint64
	valueLogDictClassifySampled    atomic.Uint64
	valueLogDictClassifySkipped    atomic.Uint64
	valueLogDictAdaptiveRatio      float64
	valueLogDictMinPayloadSavings  float64
	valueLogDictMetricsWindow      int
	valueLogDictMetricsMinRecords  int
	valueLogDictMetricsPauseBytes  int

	valueLogDictTrainerMu sync.Mutex
	valueLogDictTrainer   *compression.Trainer
	valueLogDictKickCh    chan struct{}
	valueLogDictMetrics   compression.Metrics
	valueLogDictFrames    struct {
		total     atomic.Uint64
		attempted atomic.Uint64
		kept      atomic.Uint64
	}
	valueLogAutotuneMetrics          vlogAutotuneMetrics
	valueLogAutotuneOptions          valuelog.AutotuneOptions
	valueLogAutotuneCandidateKSet    bool
	valueLogAutotuneLastProfile      atomic.Value // *vlogAutotuneProfile
	valueLogAutotuneLastSwitchFrames atomic.Uint64

	valueLogDictPauseRemaining               atomic.Uint64
	valueLogDictProbeBytes                   uint64
	valueLogDictProbeRemaining               atomic.Uint64
	valueLogDictIncompressibleHoldBytes      uint64
	valueLogDictIncompressibleHoldRemaining  atomic.Uint64
	valueLogDictIncompressibleProbeBytes     uint64
	valueLogDictIncompressibleProbeRemaining atomic.Uint64
	valueLogDictIncompressibleHitStreak      atomic.Uint32
	valueLogDictIncompressibleHits           atomic.Uint64
	valueLogDictIncompressibleHolds          atomic.Uint64
	valueLogDictIncompressibleBypassBytes    atomic.Uint64
	valueLogDictPausedSampleStride           uint64
	valueLogDictPausedSampleCounter          atomic.Uint64
	valueLogDictLastAppliedDictHash          atomic.Uint64
	valueLogDictLastAppliedDictID            atomic.Uint64
	valueLogDictLastPublishUnixNano          atomic.Int64
	valueLogDictLastKUpdateUnixNano          atomic.Int64
	valueLogDictCurrentK                     atomic.Uint32
	valueLogDictKMu                          sync.RWMutex
	valueLogDictKCache                       map[uint64]int
	valueLogDictBytesMu                      sync.Mutex
	valueLogDictBytesID                      uint64
	valueLogDictBytes                        []byte

	// Value-log template compression (cached mode).
	valueLogTemplateEnabled    bool
	valueLogTemplateMode       template.Mode
	valueLogTemplateEngine     *template.Engine
	valueLogTemplateReadStrict bool
	valueLogTemplateDecodeOpts template.DecodeOptions

	// Cached dictdb current pointer to avoid per-write lookups on the hot path.
	// A stale dictID is safe (it always points to a durable dict); at worst we
	// lag adoption of a newly trained dictionary.
	dictCurrentCached atomic.Uint64
	dictCurrentOps    atomic.Uint64

	// Config
	dir                                               string
	flushThreshold                                    int64
	memtableCap                                       int
	memtableMode                                      atomic.Uint32
	memtableStats                                     memtableStats
	memtableAdaptive                                  bool
	memtableAdaptiveObserve                           atomic.Bool
	memtableAdaptiveBTreeMinIters                     uint64
	memtableAdaptiveDecisionTotal                     atomic.Uint64
	memtableAdaptiveDecisionReason                    atomic.Uint32
	memtableAdaptiveDecisionMode                      atomic.Uint32
	memtableAdaptiveDecisionWrites                    atomic.Uint64
	memtableAdaptiveDecisionSeqWrites                 atomic.Uint64
	memtableAdaptiveDecisionOverwriteWrites           atomic.Uint64
	memtableAdaptiveDecisionIters                     atomic.Uint64
	memtableAdaptiveDecisionRangeIters                atomic.Uint64
	memtableAdaptiveDecisionRangePctPPM               atomic.Uint32
	memtableAdaptiveDecisionLowDataTotal              atomic.Uint64
	memtableAdaptiveDecisionBTreeTotal                atomic.Uint64
	memtableAdaptiveDecisionBTreeBlockedMinItersTotal atomic.Uint64
	memtableAdaptiveDecisionAppendTotal               atomic.Uint64
	memtableAdaptiveDecisionHashTotal                 atomic.Uint64
	adaptiveShardedStats                              bool
	memtableWarmupActive                              bool
	memtableWarmupThreshold                           int64
	domainIngressWorkers                              int
	domainIngressQueueSize                            int
	maxQueuedMemtables                                int
	slowdownBacklogSeconds                            float64
	stopBacklogSeconds                                float64
	maxBacklogBytes                                   int64
	writerFlushMaxMemtables                           int
	writerFlushMaxDuration                            time.Duration
	flushBuildConcurrency                             int
	flushBuildAutoConcurrency                         bool
	flushBuildMinEntries                              int
	flushBuildMinUnits                                int
	flushBuildChunkCap                                int
	flushBuildChunkTarget                             int
	flushBuildChunkMinBytes                           int
	flushBuildChunkMaxBytes                           int
	flushBuildPrefetchUnits                           int
	flushBackendMaxEntries                            int
	flushBackendInitEntries                           int
	flushBackendMaxBatches                            int
	walMaxSegmentBytes                                int64
	valueLogMaxSegmentBytes                           int64
	journalCompression                                bool

	disableJournal                             bool
	relaxedSync                                bool
	notifyError                                func(error)
	debugFlushPointers                         bool
	debugFlushTiming                           bool
	debugPtrEligible                           atomic.Int64
	debugPtrUsed                               atomic.Int64
	debugPtrNoPtr                              atomic.Int64
	debugPtrDenied                             atomic.Int64
	debugPtrDisabled                           atomic.Int64
	checkpointRuns                             atomic.Uint64
	checkpointTotalNs                          atomic.Uint64
	checkpointMaxNs                            atomic.Uint64
	checkpointNoopSkips                        atomic.Uint64
	checkpointFlushMuWaitNs                    atomic.Uint64
	checkpointFlushMuWaitMaxNs                 atomic.Uint64
	checkpointAutoVacuumRuns                   atomic.Uint64
	checkpointAutoVacuumLastCheckRun           atomic.Uint64
	checkpointAutoVacuumLastPages              atomic.Uint64
	checkpointAutoVacuumLastInternalP50        atomic.Uint64
	checkpointAutoVacuumLastInternalAvg        atomic.Uint64
	lastForegroundWriteUnixNano                atomic.Int64
	lastForegroundReadUnixNano                 atomic.Int64
	foregroundReadStampCounter                 atomic.Uint32
	activeForegroundIterators                  atomic.Int64
	retainedPruneLastStartUnixNano             atomic.Int64
	retainedPruneMu                            sync.Mutex
	retainedPruneDone                          chan struct{}
	vlogGenerationRemapSuccesses               atomic.Uint64
	vlogGenerationRemapFailures                atomic.Uint64
	vlogGenerationRewriteBytesIn               atomic.Uint64
	vlogGenerationRewriteBytesOut              atomic.Uint64
	vlogGenerationRewriteRuns                  atomic.Uint64
	vlogGenerationRewritePlanRuns              atomic.Uint64
	vlogGenerationRewritePlanCanceled          atomic.Uint64
	vlogGenerationRewritePlanErrors            atomic.Uint64
	vlogGenerationRewritePlanEmpty             atomic.Uint64
	vlogGenerationRewritePlanSelected          atomic.Uint64
	vlogGenerationRewritePlanCanceledLastNS    atomic.Int64
	vlogGenerationRewriteAgeBlockedUntilNS     atomic.Int64
	vlogGenerationRewriteIneffectiveLastNS     atomic.Int64
	vlogGenerationRewriteIneffectiveRuns       atomic.Uint64
	vlogGenerationRewriteIneffectiveBytesIn    atomic.Uint64
	vlogGenerationRewriteIneffectiveBytesOut   atomic.Uint64
	vlogGenerationRewriteCanceledRuns          atomic.Uint64
	vlogGenerationRewriteCanceledLastNS        atomic.Int64
	vlogGenerationRewriteQueuePruneRuns        atomic.Uint64
	vlogGenerationRewriteQueuePruneIDs         atomic.Uint64
	vlogGenerationGCSegmentsDeleted            atomic.Uint64
	vlogGenerationGCBytesDeleted               atomic.Uint64
	vlogGenerationGCRuns                       atomic.Uint64
	vlogGenerationVacuumRuns                   atomic.Uint64
	vlogGenerationVacuumFailures               atomic.Uint64
	vlogGenerationLastVacuumUnixNano           atomic.Int64
	vlogGenerationLastRewritePlanUnixNano      atomic.Int64
	vlogGenerationLastRewriteUnixNano          atomic.Int64
	vlogGenerationLastGCUnixNano               atomic.Int64
	vlogGenerationLastCheckpointKickUnixNano   atomic.Int64
	vlogGenerationLastGCDryRunUnixNano         atomic.Int64
	vlogGenerationLastGCDryRunBytesEligible    atomic.Int64
	vlogGenerationLastGCDryRunSegsEligible     atomic.Int64
	vlogGenerationChurnBytes                   atomic.Uint64
	vlogGenerationSchedulerState               atomic.Uint32
	vlogGenerationMaintenanceActive            atomic.Bool
	vlogGenerationLastReason                   atomic.Uint32
	vlogGenerationCheckpointKickRuns           atomic.Uint64
	vlogGenerationCheckpointKickRewriteRuns    atomic.Uint64
	vlogGenerationCheckpointKickGCRuns         atomic.Uint64
	vlogGenerationCheckpointKickPending        atomic.Bool
	vlogGenerationDeferredMaintenancePending   atomic.Bool
	vlogGenerationDeferredMaintenanceRunning   atomic.Bool
	vlogGenerationRewriteStageWakeObservedNS   atomic.Int64
	vlogGenerationRewriteQueueMu               sync.Mutex
	vlogGenerationCheckpointKickActive         atomic.Bool
	vlogGenerationRewriteQueue                 []uint32
	vlogGenerationRewriteLedger                []backenddb.ValueLogRewritePlanSegment
	vlogGenerationRewritePenalties             map[uint32]valueLogGenerationRewritePenalty
	vlogGenerationRewriteStagePending          bool
	vlogGenerationRewriteStageObservedUnixNano int64
	vlogGenerationRewriteQueueLoaded           bool
	vlogGenerationLastChurnBps                 atomic.Int64
	vlogGenerationLastChurnSampleBytes         atomic.Uint64
	vlogGenerationLastChurnSampleNS            atomic.Int64
	// Rewrite budget token bucket (bytes) for online maintenance. This lets us
	// interpret ValueLogRewriteBudgetBytesPerSec as a true per-second bandwidth
	// budget while still running maintenance at coarse intervals.
	vlogGenerationRewriteBudgetLastUnixNano atomic.Int64
	vlogGenerationRewriteBudgetTokensBytes  atomic.Int64
	bgErrMu                                 sync.Mutex
	bgErr                                   error

	// Backpressure state
	queueBacklogBytes        atomic.Int64
	flushBpsEWMA             float64
	queueLaneIDMisses        atomic.Int64
	backendWriteBatchesTotal atomic.Int64
	domainIngressMu          sync.Mutex
	domainIngressCh          []chan domainIngressRequest
	domainIngressEnqueued    atomic.Uint64
	domainIngressProcessed   atomic.Uint64
	domainIngressFallback    atomic.Uint64
	domainIngressDepthMax    atomic.Uint64

	// Lifecycle
	closeCh chan struct{}
	closing atomic.Bool
	flushCh chan struct{}
	wg      sync.WaitGroup

	autoCheckpointOnceCh  chan struct{}
	autoCheckpointWriteCh chan struct{}
	autoCheckpointOn      atomic.Bool
	// autoCheckpointSizeArmed gates the maxWALBytes size-triggered checkpoint.
	// It is disarmed after the first size-triggered checkpoint and re-armed only
	// after reclaimable WAL bytes fall below maxWALBytes/2.
	autoCheckpointSizeArmed atomic.Bool

	autoCheckpointCount                    atomic.Uint64
	autoCheckpointLastReason               atomic.Uint32
	autoCheckpointLastUnixNano             atomic.Int64
	autoCheckpointLastDurNanos             atomic.Int64
	autoCheckpointLastWALBefore            atomic.Int64
	autoCheckpointLastWALAfter             atomic.Int64
	autoCheckpointLastWALReclaimableBefore atomic.Int64
	autoCheckpointLastWALReclaimableAfter  atomic.Int64
	autoCheckpointLastWALTrimmed           atomic.Int64
	autoCheckpointLastWALBytes             atomic.Int64
	autoCheckpointMaxWALBytes              atomic.Int64

	checkpointCutoverLastNanos    atomic.Int64
	checkpointCutoverMaxNanos     atomic.Int64
	checkpointCutoverTotalNanos   atomic.Int64
	checkpointCutoverSamples      atomic.Uint64
	checkpointCutoverLastUnixNano atomic.Int64

	materializationLastDrainUnixNano atomic.Int64

	publishWatermarkLagMu            sync.Mutex
	publishWatermarkLastBacklogBytes int64
	publishWatermarkLastUnixNano     int64
	// testing hooks
	testOnVlogFlush              func(laneID int)
	testOnVlogSync               func(laneID int)
	testBeforeVlogUnlock         func(laneID int)
	testSkipRetainedPrune        bool
	testSkipVlogCheckpointKick   bool
	testSkipCheckpointAutoVacuum bool
}

const (
	vlogGenerationSchedulerDisabled uint32 = iota
	vlogGenerationSchedulerIdle
	vlogGenerationSchedulerRunning
	vlogGenerationSchedulerError
)

const (
	vlogGenerationReasonNone uint32 = iota
	vlogGenerationReasonTotalBytes
	vlogGenerationReasonStaleRatio
	vlogGenerationReasonChurn
	vlogGenerationReasonPeriodicGC
	vlogGenerationReasonPostRewriteVacuum
	vlogGenerationReasonRewriteResume
)

const (
	vlogGenerationLoopInterval              = 1 * time.Second
	vlogGenerationGCEvery                   = 5
	vlogGenerationGCMinBytes                = int64(1 << 20)
	vlogGenerationRewriteMinInterval        = 30 * time.Second
	vlogGenerationGCMinInterval             = 45 * time.Second
	vlogGenerationCheckpointKickMinInterval = 5 * time.Second
	vlogGenerationCheckpointKickRetryWindow = 5 * time.Second
	vlogGenerationDeferredRetryWindow       = 30 * time.Second
	vlogGenerationRewritePlanCancelBackoff  = 5 * time.Second
	vlogGenerationRewriteCancelBackoff      = 20 * time.Second
	vlogGenerationRewriteIneffectiveBackoff = 2 * time.Minute
	// Keep rewrite planning cancelable, but allow a short grace window so
	// sub-second plan scans can complete under light foreground jitter.
	vlogGenerationRewritePlanResumeGrace = 350 * time.Millisecond
	// Best-effort background maintenance should not immediately compete with
	// a just-active foreground write stream.
	vlogForegroundQuietWindow = 2 * time.Second
	// Foreground reads/iterators are latency-sensitive; do not start heavy
	// value-log maintenance while they are active or have just resumed.
	vlogForegroundReadQuietWindow = 2 * time.Second
	// Generational rewrite planning / rewrite / GC can scan a large live tree
	// and should only run when the foreground has been quiet for a meaningfully
	// longer window than lightweight maintenance.
	vlogGenerationMaintenanceQuietWindow = 15 * time.Second
	// Retained-path prune performs a full live-ID scan and is pure best-effort
	// reclaim. Require a meaningfully idle window before starting it so hot
	// write workloads do not pay for a large scan between active phases.
	retainedPruneQuietWindow = 15 * time.Second
	// Retained-path prune is opportunistic reclaim. Do not restart a full live-ID
	// scan on every periodic checkpoint during a hot workload.
	retainedPruneMinInterval = 30 * time.Second
	// Coordinate index vacuum with major rewrite windows; do not run on every GC.
	vlogGenerationVacuumTriggerRewriteBytes = int64(64 << 20)
	vlogGenerationVacuumMinInterval         = 5 * time.Minute
)

const (
	defaultVlogGenerationHotTargetBytes  int64  = 256 << 20
	defaultVlogGenerationWarmTargetBytes int64  = 256 << 20
	defaultVlogGenerationColdTargetBytes int64  = 512 << 20
	defaultVlogRewriteBudgetBytesPerSec  int64  = 128 << 20
	defaultVlogRewriteTriggerTotalBytes  int64  = 4 << 30
	defaultVlogRewriteTriggerStalePPM    uint32 = 200000
	// Staged stale-ratio debt can begin with a moderate floor, then confirm on a
	// later opportunity. Generic total-bytes/churn rewrites do not get that
	// safety net, so they must target only nearly-cold segments.
	vlogGenerationRewriteMinSegmentStaleRatio        = 0.50
	vlogGenerationRewriteGenericMinSegmentStaleRatio = 0.85
	vlogGenerationRewriteMinSegmentStaleBytes        = int64(1)
	vlogGenerationRewriteMinSegmentAge               = 30 * time.Second
	vlogGenerationRewriteEfficacyMinTotalBytes       = int64(512 << 20)
	// Treat rewrites that materially increase bytes as harmful and stop resuming
	// the remaining queued plan in the same debt cycle.
	vlogGenerationRewriteIneffectiveGrowthMinBytes = int64(4 << 20)
	vlogGenerationRewriteIneffectiveCooldown       = 10 * time.Minute
	// Resumable queued rewrites are already segment-limited; let them finish
	// under foreground activity with a bounded timeout instead of immediate
	// foreground-cancel semantics.
	vlogGenerationRewriteBoundedExecTimeout = 30 * time.Second
	// During checkpoint-kick debt drain, allow a bounded multi-segment rewrite
	// selection so debt can converge faster than one-segment-per-pass.
	vlogGenerationRewriteDebtDrainMaxSegments = 8
)

func (db *DB) flushBackendEntriesCap(totalOps int, sync bool) int {
	capEntries := db.flushBackendMaxEntries
	if capEntries < 1 {
		capEntries = 1
	}
	maxBatches := db.flushBackendMaxBatches
	if maxBatches == 0 {
		maxBatches = 16
	}
	// Sync-triggered flushes (checkpoint/close) should remain fast; cap the number
	// of intermediate commits in that path even if the steady-state micro-batch
	// policy is aggressive.
	if sync && maxBatches > 8 {
		maxBatches = 8
	}
	if maxBatches > 0 {
		// Avoid overflow when capEntries is very large (e.g., chunking disabled).
		maxInt := int(^uint(0) >> 1)
		if capEntries <= maxInt/maxBatches && totalOps > capEntries*maxBatches {
			// Increase the chunk size so we emit at most maxBatches intermediate
			// commits. This preserves the high-watermark benefits of micro-batching
			// while bounding zipper/apply overhead.
			capEntries = (totalOps + maxBatches - 1) / maxBatches
			if capEntries < 1 {
				capEntries = 1
			}
		}
	}
	return capEntries
}

func (db *DB) flushBackendEntriesCapForOps(totalOps int, deleteOps int, sync bool) int {
	capEntries := db.flushBackendMaxEntries
	if capEntries < 1 {
		capEntries = 1
	}
	maxBatches := db.flushBackendMaxBatches
	if maxBatches == 0 {
		maxBatches = 16
	}
	// Sync-triggered flushes (checkpoint/close) should remain fast; cap the number
	// of intermediate commits in that path even if the steady-state micro-batch
	// policy is aggressive.
	if sync && maxBatches > 8 {
		maxBatches = 8
	}
	// Delete-heavy flushes are expensive to apply in many intermediate commits.
	// Each commit re-writes leaf pages (copying surviving values), so repeated
	// commits amplify work dramatically when deletes touch a large fraction of the
	// keyspace. Favor fewer commits in that case.
	if maxBatches > 0 && deleteOps > 0 && totalOps > 0 {
		// Deterministic "delete-heavy" trigger: deletes are at least 25% of ops.
		if deleteOps*4 >= totalOps && maxBatches > 4 {
			maxBatches = 4
		}
	}
	if maxBatches > 0 {
		// Avoid overflow when capEntries is very large (e.g., chunking disabled).
		maxInt := int(^uint(0) >> 1)
		if capEntries <= maxInt/maxBatches && totalOps > capEntries*maxBatches {
			// Increase the chunk size so we emit at most maxBatches intermediate
			// commits. This preserves the high-watermark benefits of micro-batching
			// while bounding zipper/apply overhead.
			capEntries = (totalOps + maxBatches - 1) / maxBatches
			if capEntries < 1 {
				capEntries = 1
			}
		}
	}
	return capEntries
}

type keyRange struct {
	valid bool
	min   []byte
	max   []byte
}

type memShard struct {
	mu                         sync.Mutex
	mem                        memtable.Table
	rng                        keyRange
	bytes                      int64
	stats                      memtableStats
	appendOnlyDirectValueArena appendOnlyDirectValueArena
}

// memtableView is an immutable snapshot of the in-memory layers.
// It is published via atomic.Pointer and treated as read-only by readers.
type memtableView struct {
	mutables                 []memtable.Table
	queue                    []memtable.Table
	queueShardIDs            []uint16
	queueRanges              []keyRange
	refs                     atomic.Int64
	retiredMems              []memtable.Table
	deferredRetiredMemtables atomic.Int64
	deferredRetiredBytes     atomic.Int64
}

// appendOnlyEstimatedBytesPerEntryDefault tunes initial append-only memtable
// entry slice sizing. The historical pointer-heavy estimate (24B) can cause
// large over-allocation in real workloads where key bytes dominate.
const appendOnlyEstimatedBytesPerEntryDefault = 96

// maxAppendOnlyMemLeases bounds strong references to recycled append-only
// memtables. A very small bound forces frequent fallback to sync.Pool under
// rotate/checkpoint-heavy write workloads, which regresses entry-slice reuse
// and increases allocation churn.
const maxAppendOnlyMemLeases = 32

func updateInt64Max(dst *atomic.Int64, value int64) {
	for {
		cur := dst.Load()
		if value <= cur {
			return
		}
		if dst.CompareAndSwap(cur, value) {
			return
		}
	}
}

func memtableBytesTotal(mems []memtable.Table) int64 {
	var total int64
	for _, mt := range mems {
		if mt != nil {
			total += mt.Size()
		}
	}
	return total
}

func (db *DB) noteMemtableViewRetain() {
	if db == nil {
		return
	}
	tel := &db.memtableViewTelemetry
	tel.retainTotal.Add(1)
	inFlight := tel.leasesInFlight.Add(1)
	updateInt64Max(&tel.leasesInFlightMax, inFlight)
}

func (db *DB) noteMemtableViewRelease() {
	if db == nil {
		return
	}
	tel := &db.memtableViewTelemetry
	tel.releaseTotal.Add(1)
	tel.leasesInFlight.Add(-1)
}

func (db *DB) noteMemtableViewDeferredEnter(view *memtableView, memtables int64, bytes int64) {
	if db == nil || view == nil || memtables <= 0 {
		return
	}
	if bytes < 0 {
		bytes = 0
	}
	tel := &db.memtableViewTelemetry
	since := time.Now().UnixNano()

	tel.deferredMu.Lock()
	defer tel.deferredMu.Unlock()
	if tel.deferred == nil {
		tel.deferred = make(map[*memtableView]memtableViewDeferredInfo)
	}
	if _, exists := tel.deferred[view]; exists {
		return
	}
	// Avoid recording deferred telemetry after final release. This prevents
	// stale entries if a concurrent releaser already dropped refs to zero.
	if view.refs.Load() == 0 {
		return
	}
	tel.deferred[view] = memtableViewDeferredInfo{
		memtables:     memtables,
		bytes:         bytes,
		sinceUnixNano: since,
	}
	oldest := int64(0)
	for _, deferred := range tel.deferred {
		if oldest == 0 || deferred.sinceUnixNano < oldest {
			oldest = deferred.sinceUnixNano
		}
	}
	tel.oldestDeferredUnixNano.Store(oldest)

	tel.deferredViewsTotal.Add(1)
	deferredViewsCurrent := tel.deferredViewsCurrent.Add(1)
	updateInt64Max(&tel.deferredViewsMax, deferredViewsCurrent)
	tel.deferredMemtablesTotal.Add(uint64(memtables))
	deferredMemtablesCurrent := tel.deferredMemtablesCurrent.Add(memtables)
	updateInt64Max(&tel.deferredMemtablesMax, deferredMemtablesCurrent)
	tel.deferredBytesTotal.Add(uint64(bytes))
	deferredBytesCurrent := tel.deferredBytesCurrent.Add(bytes)
	updateInt64Max(&tel.deferredBytesMax, deferredBytesCurrent)
}

func (db *DB) noteMemtableViewDeferredExit(view *memtableView) {
	if db == nil || view == nil {
		return
	}
	tel := &db.memtableViewTelemetry
	var (
		info  memtableViewDeferredInfo
		found bool
	)
	oldest := int64(0)
	tel.deferredMu.Lock()
	defer tel.deferredMu.Unlock()
	if tel.deferred != nil {
		info, found = tel.deferred[view]
		if found {
			delete(tel.deferred, view)
		}
		for _, deferred := range tel.deferred {
			if oldest == 0 || deferred.sinceUnixNano < oldest {
				oldest = deferred.sinceUnixNano
			}
		}
	}
	tel.oldestDeferredUnixNano.Store(oldest)
	if !found {
		return
	}
	tel.deferredViewsCurrent.Add(-1)
	tel.deferredMemtablesCurrent.Add(-info.memtables)
	tel.deferredBytesCurrent.Add(-info.bytes)
}

func (db *DB) retainMemtableView() *memtableView {
	if db == nil {
		return nil
	}
	for {
		view := db.memtables.Load()
		if view == nil {
			return nil
		}
		// Fast path: published views keep a baseline ref, so this is usually one
		// atomic add and return.
		if n := view.refs.Add(1); n > 1 {
			db.noteMemtableViewRetain()
			return view
		}
		// We observed a zero-ref view. Undo and retry unless this is still the
		// published view, in which case self-heal by restoring baseline+caller refs.
		view.refs.Add(-1)
		if db.memtables.Load() != view {
			continue
		}
		if view.refs.CompareAndSwap(0, 2) {
			db.noteMemtableViewRetain()
			return view
		}
	}
}

func (db *DB) releaseMemtableView(view *memtableView) {
	db.releaseMemtableViewRef(view, true)
}

func (db *DB) releasePublishedMemtableView(view *memtableView) {
	db.releaseMemtableViewRef(view, false)
}

func (db *DB) releaseMemtableViewRef(view *memtableView, leaseRelease bool) {
	if db == nil || view == nil {
		return
	}
	if leaseRelease {
		db.noteMemtableViewRelease()
	}
	if refs := view.refs.Add(-1); refs != 0 {
		if refs > 0 && view.deferredRetiredMemtables.Load() > 0 {
			db.noteMemtableViewDeferredEnter(view, view.deferredRetiredMemtables.Load(), view.deferredRetiredBytes.Load())
		}
		return
	}
	db.noteMemtableViewDeferredExit(view)
	view.deferredRetiredMemtables.Store(0)
	view.deferredRetiredBytes.Store(0)
	db.recycleMemtables(view.retiredMems)
	view.retiredMems = nil
}

func cachedBatchWriteNeedsBatchArenaRetention(mt memtable.Table) bool {
	// This helper is only used for the cached batch write path, where
	// cachedBatchWriteUsesSteal governs whether a memtable receives borrowed
	// batch slices or copied writes. append_only and hash_sorted are forced onto
	// copy writes there, so they do not need batch-arena leases. Skiplist is the
	// other non-retained case: it accepts Steal calls, but still copies key/value
	// bytes into its own arena, so batch-owned buffers can be released
	// immediately after the write.
	if !cachedBatchWriteUsesSteal(mt) {
		return false
	}
	_, skiplist := mt.(*memtable.Memtable)
	return !skiplist
}

func cachedBatchWriteUsesSteal(mt memtable.Table) bool {
	// append_only and hash_sorted borrow key/value slices on Steal paths, so the
	// cached batch writer feeds them copied writes instead. Keep the Steal path
	// on an explicit allowlist so new memtable implementations do not silently
	// reintroduce borrowed-slice lifetime bugs. Mutable shard memtables are
	// constructed internally from concrete types, so wrappers should opt in here
	// explicitly instead of inheriting Steal behavior accidentally.
	switch mt.(type) {
	case *memtable.BTree, *memtable.Memtable:
		return true
	default:
		return false
	}
}

func cachedBatchWriteUseSteal(db *DB, mt memtable.Table) (useSteal bool, suppressedDeferred bool) {
	if !cachedBatchWriteUsesSteal(mt) {
		return false, false
	}
	// Under deferred memtable-view pressure, BTree Steal extends the lifetime
	// of batch-owned buffers until retired memtables are released. Prefer the
	// copy path in this mode to cap retained lease growth.
	if db != nil && db.batchArenaDeferredPressureActive() {
		if _, isBTree := mt.(*memtable.BTree); isBTree {
			return false, true
		}
	}
	return true, false
}

func memtableBatchDelete(mt memtable.Table, useSteal bool, key []byte) {
	if useSteal {
		mt.DeleteSteal(key)
		return
	}
	mt.Delete(key)
}

// memtableBatchSet applies a cached batch write while preserving the ownership
// rules selected by cachedBatchWriteUsesSteal. Pointer entries may still keep
// inline bytes in memtables that do not use value-log pointers internally.
func memtableBatchSet(mt memtable.Table, useSteal bool, allowBorrow bool, storeInlinePtrValues bool, op batch.Entry) {
	if op.IsPtr {
		memVal := []byte(nil)
		if storeInlinePtrValues {
			memVal = op.Value
		}
		if useSteal {
			mt.SetEntrySteal(op.Key, memVal, op.ValuePtr, node.FlagPointer)
			return
		}
		if allowBorrow {
			if borrower, ok := mt.(memtable.ValueBorrower); ok && len(memVal) > 0 {
				borrower.SetEntryBorrowValue(op.Key, memVal, op.ValuePtr, node.FlagPointer)
				return
			}
		}
		mt.SetEntry(op.Key, memVal, op.ValuePtr, node.FlagPointer)
		return
	}
	if useSteal {
		mt.SetSteal(op.Key, op.Value)
		return
	}
	if allowBorrow {
		if borrower, ok := mt.(memtable.ValueBorrower); ok && len(op.Value) > 0 {
			borrower.SetEntryBorrowValue(op.Key, op.Value, page.ValuePtr{}, node.FlagInline)
			return
		}
	}
	mt.Set(op.Key, op.Value)
}

func getBatchArenaLease(refs int, chunks [][]byte) *batchArenaLease {
	lease, _ := batchArenaLeasePool.Get().(*batchArenaLease)
	if lease == nil {
		lease = &batchArenaLease{}
	}
	lease.refs = refs
	lease.chunks = chunks
	var bytes int64
	for i := range chunks {
		if chunks[i] != nil {
			bytes += int64(cap(chunks[i]))
		}
	}
	lease.bytes = bytes
	return lease
}

func putBatchArenaLease(lease *batchArenaLease) {
	if lease == nil {
		return
	}
	lease.refs = 0
	lease.chunks = nil
	lease.bytes = 0
	batchArenaLeasePool.Put(lease)
}

func (db *DB) retainBatchArenaChunksForMemtables(chunks [][]byte, mems []memtable.Table) {
	if len(chunks) == 0 {
		return
	}
	if db == nil || len(mems) == 0 {
		putBatchArenas(chunks)
		return
	}
	lease := getBatchArenaLease(len(mems), chunks)
	if lease.bytes > 0 {
		cur := db.batchArenaLeaseBytes.Add(lease.bytes)
		updateInt64Max(&db.batchArenaLeaseBytesMax, cur)
		globalLeased := batchArenaLeasedBytesGlobal.Add(lease.bytes)
		noteBatchArenaLeasedBytesGlobalMax(globalLeased)
		noteBatchArenaRetainedBytesMax()
	}
	db.batchArenaLeaseMu.Lock()
	if db.batchArenaLeasesByMem == nil {
		db.batchArenaLeasesByMem = make(map[memtable.Table][]*batchArenaLease, len(mems))
	}
	for _, mt := range mems {
		db.batchArenaLeasesByMem[mt] = append(db.batchArenaLeasesByMem[mt], lease)
	}
	db.batchArenaLeaseMu.Unlock()
}

func (db *DB) releaseBatchArenaLeasesForMemtable(mt memtable.Table) {
	if db == nil || mt == nil {
		return
	}
	var release [][]byte
	db.batchArenaLeaseMu.Lock()
	leases := db.batchArenaLeasesByMem[mt]
	if len(leases) > 0 {
		delete(db.batchArenaLeasesByMem, mt)
		for _, lease := range leases {
			if lease == nil || lease.refs <= 0 {
				continue
			}
			lease.refs--
			if lease.refs == 0 && len(lease.chunks) > 0 {
				if lease.bytes > 0 {
					db.batchArenaLeaseBytes.Add(-lease.bytes)
					if next := batchArenaLeasedBytesGlobal.Add(-lease.bytes); next < 0 {
						batchArenaLeasedBytesGlobal.Store(0)
					}
					lease.bytes = 0
				}
				release = append(release, lease.chunks...)
				lease.chunks = nil
				putBatchArenaLease(lease)
			}
		}
	}
	db.batchArenaLeaseMu.Unlock()
	if len(release) > 0 {
		putBatchArenas(release)
	}
}

func (db *DB) retainAppendOnlyDirectArenaChunksForMemtable(shardID int, mt memtable.Table, chunks [][]byte) {
	if len(chunks) == 0 {
		return
	}
	if db == nil || mt == nil || shardID < 0 || shardID >= len(db.mutableShards) {
		putAppendOnlyDirectValueArenaChunks(chunks)
		return
	}
	db.appendOnlyDirectArenaLeaseMu.Lock()
	if db.appendOnlyDirectArenaLeasesByMem == nil {
		db.appendOnlyDirectArenaLeasesByMem = make(map[memtable.Table]*appendOnlyDirectArenaLease, 1)
	}
	if lease := db.appendOnlyDirectArenaLeasesByMem[mt]; lease != nil {
		lease.chunks = append(lease.chunks, chunks...)
		lease.bytes += appendOnlyDirectArenaChunksBytes(chunks)
		db.appendOnlyDirectArenaLeaseMu.Unlock()
		return
	}
	db.appendOnlyDirectArenaLeasesByMem[mt] = &appendOnlyDirectArenaLease{
		shardID: uint16(shardID),
		chunks:  chunks,
		bytes:   appendOnlyDirectArenaChunksBytes(chunks),
	}
	db.appendOnlyDirectArenaLeaseMu.Unlock()
}

func (db *DB) releaseAppendOnlyDirectArenaLeaseForMemtable(mt memtable.Table) {
	if db == nil || mt == nil {
		return
	}
	var lease *appendOnlyDirectArenaLease
	db.appendOnlyDirectArenaLeaseMu.Lock()
	if db.appendOnlyDirectArenaLeasesByMem != nil {
		lease = db.appendOnlyDirectArenaLeasesByMem[mt]
		delete(db.appendOnlyDirectArenaLeasesByMem, mt)
	}
	db.appendOnlyDirectArenaLeaseMu.Unlock()
	if lease == nil || len(lease.chunks) == 0 {
		return
	}
	if int(lease.shardID) >= len(db.mutableShards) {
		putAppendOnlyDirectValueArenaChunks(lease.chunks)
		return
	}
	shard := &db.mutableShards[lease.shardID]
	shard.mu.Lock()
	shard.appendOnlyDirectValueArena.retainChunks(lease.chunks)
	shard.mu.Unlock()
}

func (db *DB) retainMutableShardAppendOnlyArenaLocked(shardID int, shard *memShard) {
	if db == nil || shard == nil {
		return
	}
	if _, ok := shard.mem.(memtable.ValueBorrower); !ok {
		return
	}
	db.retainAppendOnlyDirectArenaChunksForMemtable(shardID, shard.mem, shard.appendOnlyDirectValueArena.drainActiveChunks())
}

func (db *DB) queueRetiredMemtableLocked(mem memtable.Table) {
	if db == nil || mem == nil {
		return
	}
	db.releaseBatchArenaLeasesForMemtable(mem)
	db.pendingRetiredMems = append(db.pendingRetiredMems, mem)
}

func (db *DB) queueRetiredMemtablesLocked(mems []memtable.Table) {
	if db == nil || len(mems) == 0 {
		return
	}
	for _, mem := range mems {
		db.queueRetiredMemtableLocked(mem)
	}
}

func (db *DB) popAppendOnlyMemLease() *memtable.AppendOnly {
	if db == nil {
		return nil
	}
	db.appendOnlyMemLeaseMu.Lock()
	defer db.appendOnlyMemLeaseMu.Unlock()
	n := len(db.appendOnlyMemLeases)
	if n == 0 {
		return nil
	}
	mt := db.appendOnlyMemLeases[n-1]
	db.appendOnlyMemLeases[n-1] = nil
	db.appendOnlyMemLeases = db.appendOnlyMemLeases[:n-1]
	return mt
}

func (db *DB) putAppendOnlyMemLease(mt *memtable.AppendOnly) bool {
	if db == nil || mt == nil {
		return false
	}
	db.appendOnlyMemLeaseMu.Lock()
	defer db.appendOnlyMemLeaseMu.Unlock()
	if len(db.appendOnlyMemLeases) >= maxAppendOnlyMemLeases {
		return false
	}
	db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	return true
}

func (db *DB) recycleMemtables(mems []memtable.Table) {
	if db == nil || len(mems) == 0 {
		return
	}
	resetCapacity := db.checkpointRotateCapacity()
	estimate := appendOnlyEstimatedBytesPerEntryDefault
	appendOnlyResetCapacity := db.appendOnlyMemtableCapacityHint(resetCapacity, estimate)
	for _, mt := range mems {
		switch typed := mt.(type) {
		case *memtable.AppendOnly:
			typed.ResetWithCapacityHard(appendOnlyResetCapacity, estimate)
			db.releaseAppendOnlyDirectArenaLeaseForMemtable(typed)
			if !db.putAppendOnlyMemLease(typed) {
				db.appendOnlyMemPool.Put(typed)
			}
		}
	}
}

func (db *DB) trimAppendOnlyMemLeases(maxLeases int, resetCapacity int) {
	if db == nil {
		return
	}
	if maxLeases < 0 {
		maxLeases = 0
	}
	var dropped []*memtable.AppendOnly
	db.appendOnlyMemLeaseMu.Lock()
	if n := len(db.appendOnlyMemLeases); n > maxLeases {
		drop := n - maxLeases
		dropped = append(dropped, db.appendOnlyMemLeases[:drop]...)
		copy(db.appendOnlyMemLeases, db.appendOnlyMemLeases[drop:])
		for i := n - drop; i < n; i++ {
			db.appendOnlyMemLeases[i] = nil
		}
		db.appendOnlyMemLeases = db.appendOnlyMemLeases[:n-drop]
	}
	db.appendOnlyMemLeaseMu.Unlock()

	if len(dropped) == 0 {
		return
	}
	effectiveResetCapacity := db.appendOnlyMemtableCapacityHint(resetCapacity, appendOnlyEstimatedBytesPerEntryDefault)
	for i := range dropped {
		if dropped[i] != nil {
			dropped[i].ResetWithCapacityHard(effectiveResetCapacity, appendOnlyEstimatedBytesPerEntryDefault)
			db.releaseAppendOnlyDirectArenaLeaseForMemtable(dropped[i])
			db.appendOnlyMemPool.Put(dropped[i])
		}
	}
}

func (db *DB) trimMutableShardAppendOnlyDirectArenas(checkpoint bool) {
	if db == nil || len(db.mutableShards) == 0 {
		return
	}
	level := currentPoolPressureSnapshot().level
	maxChunks, maxBytes := appendOnlyDirectArenaRetentionLimitsForPressure(level)
	if checkpoint {
		checkpointBytes := int64(appendOnlyDirectValueArenaRetainMaxBytes / 4)
		if maxBytes > checkpointBytes {
			maxBytes = checkpointBytes
		}
	} else {
		flushBytes := int64(appendOnlyDirectValueArenaRetainMaxBytes / 2)
		if maxBytes > flushBytes {
			maxBytes = flushBytes
		}
	}
	if maxBytes <= 0 || maxChunks <= 0 {
		maxBytes = 0
		maxChunks = 0
	} else {
		derivedChunks := int(maxBytes) / appendOnlyDirectValueArenaDefaultChunk
		if derivedChunks < 1 {
			derivedChunks = 1
		}
		if derivedChunks < maxChunks {
			maxChunks = derivedChunks
		}
	}
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		shard.appendOnlyDirectValueArena.trimRetained(maxChunks, maxBytes, appendOnlyDirectValueArenaPoolMaxCap)
		shard.mu.Unlock()
	}
}

func (db *DB) trimRetainedArenasAfterFlush(checkpoint bool) {
	if db == nil {
		return
	}
	now := time.Now()
	if checkpoint {
		db.retainedArenaTrimLastUnixNano.Store(now.UnixNano())
	} else {
		nowUnix := now.UnixNano()
		last := db.retainedArenaTrimLastUnixNano.Load()
		if last != 0 && nowUnix-last < int64(flushRetainedArenaTrimMinInterval) {
			return
		}
		if !db.retainedArenaTrimLastUnixNano.CompareAndSwap(last, nowUnix) {
			return
		}
	}

	batchTarget := postFlushBatchArenaTargetBytes
	entryTarget := postFlushEntrySliceTargetBytes
	leaseKeepPerBucket := postFlushEntrySliceLeaseKeepPerBucket
	appendOnlyLeaseKeep := postFlushAppendOnlyMemLeaseKeep
	if checkpoint {
		batchTarget = postCheckpointBatchArenaTargetBytes
		entryTarget = postCheckpointEntrySliceTargetBytes
		leaseKeepPerBucket = postCheckpointEntrySliceLeaseKeepPerBucket
		appendOnlyLeaseKeep = postCheckpointAppendOnlyMemLeaseKeep
	}
	if budget := currentBatchArenaRetentionBudgetBytes(); budget >= 0 && budget < batchTarget {
		batchTarget = budget
	}
	if budget := currentEntrySlicePoolBudgetBytes(); budget >= 0 && budget < entryTarget {
		entryTarget = budget
	}
	if batchTarget < 0 {
		batchTarget = 0
	}
	if entryTarget < 0 {
		entryTarget = 0
	}

	drainBatchArenaPoolToTargetBytes(batchTarget)
	entryDropped := trimEntrySliceLeasesToKeep(leaseKeepPerBucket)
	entryDropped += drainEntrySlicePoolsToTargetBytes(entryTarget)
	entrySlicePoolTrimRunsTotal.Add(1)
	if entryDropped > 0 {
		entrySlicePoolTrimDropBytesTotal.Add(uint64(entryDropped))
	}

	db.trimMutableShardAppendOnlyDirectArenas(checkpoint)
	db.trimAppendOnlyMemLeases(appendOnlyLeaseKeep, db.checkpointRotateCapacity())
}

func (db *DB) newMutableMemtableWithCapacityMode(capacity int, mode memtable.Mode) (memtable.Table, error) {
	if db != nil && mode == memtable.ModeAppendOnly {
		estimate := appendOnlyEstimatedBytesPerEntryDefault
		if mt := db.popAppendOnlyMemLease(); mt != nil {
			db.appendOnlyMemLeaseHitTotal.Add(1)
			mt.ResetWithCapacity(capacity, estimate)
			return mt, nil
		}
		if v := db.appendOnlyMemPool.Get(); v != nil {
			if mt, ok := v.(*memtable.AppendOnly); ok && mt != nil {
				db.appendOnlyMemPoolHitTotal.Add(1)
				mt.ResetWithCapacity(capacity, estimate)
				return mt, nil
			}
		}
		if backlog := db.queueBacklogBytes.Load(); backlog > 0 {
			appendOnlyMemNewAllocWithQueueTotal.Add(1)
			appendOnlyMemNewAllocQueueBytesSum.Add(uint64(backlog))
			db.appendOnlyMemNewAllocWithQueue.Add(1)
			db.appendOnlyMemNewAllocQueueBytes.Add(uint64(backlog))
		}
		db.appendOnlyMemNewAllocTotal.Add(1)
		return memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(capacity, estimate), nil
	}
	return memtable.NewWithCapacityModeAndIndexer(capacity, mode, db.hashSortedIndexer)
}

// publishMemtablesLocked publishes a new memtable snapshot.
// Caller must hold db.mu with a writer lock.
func (db *DB) publishMemtablesLocked() {
	view := &memtableView{}
	if len(db.mutableShards) > 0 {
		mutables := make([]memtable.Table, len(db.mutableShards))
		for i := range db.mutableShards {
			mutables[i] = db.mutableShards[i].mem
		}
		view.mutables = mutables
	}
	if len(db.queue) > 0 {
		q := make([]memtable.Table, len(db.queue))
		copy(q, db.queue)
		view.queue = q
	}
	if len(db.queueShardIDs) > 0 {
		qs := make([]uint16, len(db.queueShardIDs))
		copy(qs, db.queueShardIDs)
		view.queueShardIDs = qs
	}
	if len(db.queueRanges) > 0 {
		qr := make([]keyRange, len(db.queueRanges))
		copy(qr, db.queueRanges)
		view.queueRanges = qr
	}
	view.refs.Store(1)
	retired := db.pendingRetiredMems
	db.pendingRetiredMems = nil
	old := db.memtables.Swap(view)
	if old != nil {
		if len(retired) > 0 {
			old.retiredMems = append(old.retiredMems, retired...)
			old.deferredRetiredMemtables.Store(int64(len(old.retiredMems)))
			old.deferredRetiredBytes.Add(memtableBytesTotal(retired))
		}
		db.releasePublishedMemtableView(old)
		return
	}
	db.recycleMemtables(retired)
}

// ensureQueueLaneIDsLocked keeps queueLaneIDs aligned with queue length.
// Caller must hold db.mu.
func (db *DB) ensureQueueLaneIDsLocked() {
	if len(db.queueLaneIDs) >= len(db.queue) {
		return
	}
	missing := len(db.queue) - len(db.queueLaneIDs)
	if missing <= 0 {
		return
	}
	db.queueLaneIDMisses.Add(int64(missing))
	db.queueLaneIDs = append(db.queueLaneIDs, make([]uint16, missing)...)
}

type memtableStats struct {
	writes          atomic.Uint64
	seqWrites       atomic.Uint64
	overwriteWrites atomic.Uint64
	iterators       atomic.Uint64
	rangeIters      atomic.Uint64
	lastKeyMu       sync.Mutex
	lastKey         []byte
	hasLastKey      bool
}

type adaptiveMemtableDecisionReason uint32

const (
	adaptiveDecisionLowData adaptiveMemtableDecisionReason = iota + 1
	adaptiveDecisionBTreeRange
	adaptiveDecisionBTreeBlockedMinIters
	adaptiveDecisionAppendSequential
	adaptiveDecisionHashMixed
)

func adaptiveDecisionReasonString(v uint32) string {
	switch adaptiveMemtableDecisionReason(v) {
	case adaptiveDecisionLowData:
		return "low_data"
	case adaptiveDecisionBTreeRange:
		return "btree_range"
	case adaptiveDecisionBTreeBlockedMinIters:
		return "btree_blocked_min_iters"
	case adaptiveDecisionAppendSequential:
		return "append_sequential"
	case adaptiveDecisionHashMixed:
		return "hash_mixed"
	default:
		return "unknown"
	}
}

func (r *keyRange) add(key []byte) {
	if key == nil {
		return
	}
	if !r.valid {
		r.valid = true
		r.min = append([]byte(nil), key...)
		r.max = append([]byte(nil), key...)
		return
	}
	if bytes.Compare(key, r.min) < 0 {
		r.min = append(r.min[:0], key...)
	}
	if bytes.Compare(key, r.max) > 0 {
		r.max = append(r.max[:0], key...)
	}
}

func rangesOverlap(a, b keyRange) bool {
	if !a.valid || !b.valid {
		return false
	}
	// [a.min, a.max] overlaps [b.min, b.max] iff neither is strictly before the other.
	if bytes.Compare(a.max, b.min) < 0 {
		return false
	}
	if bytes.Compare(a.min, b.max) > 0 {
		return false
	}
	return true
}

// overlapsQuery checks if the query range [start, end) overlaps with the keyRange [r.min, r.max].
// nil start means -inf, nil end means +inf.
func overlapsQuery(start, end []byte, r keyRange) bool {
	if !r.valid {
		return false
	}
	// Range is [r.min, r.max]
	// Query is [start, end)

	// Check if Range is strictly before Query: r.max < start
	if start != nil && bytes.Compare(r.max, start) < 0 {
		return false
	}

	// Check if Range is strictly after Query: r.min >= end
	// Note: end is exclusive, so if r.min == end, it's outside.
	if end != nil && bytes.Compare(r.min, end) >= 0 {
		return false
	}

	return true
}

// queryCoversRange reports whether the query domain [start,end) fully covers the
// inclusive key range [r.min,r.max]. A nil bound is treated as -/+ infinity.
//
// Note: since end is exclusive, it must be strictly greater than r.max to cover
// the max key.
func queryCoversRange(start, end []byte, r keyRange) bool {
	if !r.valid {
		return true
	}
	if start != nil && bytes.Compare(start, r.min) > 0 {
		return false
	}
	if end != nil && bytes.Compare(end, r.max) <= 0 {
		return false
	}
	return true
}

func cloneRange(r keyRange) keyRange {
	if !r.valid {
		return r
	}
	r.min = append([]byte(nil), r.min...)
	r.max = append([]byte(nil), r.max...)
	return r
}

func (db *DB) snapshotMutableRange() keyRange {
	var out keyRange
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		r := cloneRange(shard.rng)
		shard.mu.Unlock()
		if !r.valid {
			continue
		}
		if !out.valid {
			out = r
			continue
		}
		if bytes.Compare(r.min, out.min) < 0 {
			out.min = append(out.min[:0], r.min...)
		}
		if bytes.Compare(r.max, out.max) > 0 {
			out.max = append(out.max[:0], r.max...)
		}
	}
	return out
}

func (db *DB) resetMutableShardsLocked(nextMode memtable.Mode, reuse bool) error {
	db.mutableBytes.Store(0)
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		reused := false
		if reuse {
			if r, ok := any(shard.mem).(interface{ Reset() }); ok {
				r.Reset()
				shard.appendOnlyDirectValueArena.recycleActive()
				reused = true
			}
		}
		if !reused {
			db.retainMutableShardAppendOnlyArenaLocked(i, shard)
			db.queueRetiredMemtableLocked(shard.mem)
			mt, err := db.newMutableMemtableWithCapacityMode(0, nextMode)
			if err != nil {
				shard.mu.Unlock()
				return err
			}
			shard.mem = mt
		}
		shard.rng = keyRange{}
		shard.bytes = 0
		shard.mu.Unlock()
	}
	db.memtableStats.writes.Store(0)
	db.memtableStats.seqWrites.Store(0)
	db.memtableStats.overwriteWrites.Store(0)
	db.memtableStats.iterators.Store(0)
	db.memtableStats.rangeIters.Store(0)
	db.memtableStats.lastKeyMu.Lock()
	db.memtableStats.hasLastKey = false
	db.memtableStats.lastKeyMu.Unlock()
	db.updateAdaptiveObservationLocked()
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	return nil
}

func (db *DB) noteWriteKey(key []byte) {
	if !db.memtableAdaptive || !db.memtableAdaptiveObserve.Load() {
		return
	}
	stats := &db.memtableStats
	stats.writes.Add(1)
	if len(key) == 0 {
		stats.lastKeyMu.Lock()
		stats.hasLastKey = false
		stats.lastKeyMu.Unlock()
		return
	}
	stats.lastKeyMu.Lock()
	defer stats.lastKeyMu.Unlock()
	if stats.hasLastKey {
		if bytes.Equal(stats.lastKey, key) {
			stats.overwriteWrites.Add(1)
		} else if bytes.Compare(stats.lastKey, key) < 0 {
			stats.seqWrites.Add(1)
		}
	}
	stats.lastKey = append(stats.lastKey[:0], key...)
	stats.hasLastKey = true
}

// noteWriteSortedRun records a strictly increasing key run in one shot.
func (db *DB) noteWriteSortedRun(first, last []byte, count int) {
	if !db.memtableAdaptive || !db.memtableAdaptiveObserve.Load() || count <= 0 {
		return
	}
	stats := &db.memtableStats
	stats.writes.Add(uint64(count))
	if len(last) == 0 {
		stats.lastKeyMu.Lock()
		stats.hasLastKey = false
		stats.lastKeyMu.Unlock()
		return
	}
	seqAdds := uint64(0)
	if count > 1 {
		seqAdds += uint64(count - 1)
	}
	stats.lastKeyMu.Lock()
	if len(first) > 0 && stats.hasLastKey && bytes.Compare(stats.lastKey, first) < 0 {
		seqAdds++
	}
	stats.lastKey = append(stats.lastKey[:0], last...)
	stats.hasLastKey = true
	stats.lastKeyMu.Unlock()
	if seqAdds > 0 {
		stats.seqWrites.Add(seqAdds)
	}
}

func (db *DB) noteIterator(start, end []byte) {
	if !db.memtableAdaptive || !db.memtableAdaptiveObserve.Load() {
		return
	}
	stats := &db.memtableStats
	stats.iterators.Add(1)
	if start != nil || end != nil {
		stats.rangeIters.Add(1)
	}
}

func (db *DB) updateMutableThresholdLocked() {
	threshold := db.flushThreshold
	if db.memtableWarmupActive && db.memtableWarmupThreshold > 0 && db.memtableWarmupThreshold < db.flushThreshold {
		threshold = db.memtableWarmupThreshold
	}
	db.mutableThreshold.Store(threshold)
}

func (db *DB) mutableFlushThreshold() int64 {
	base := db.mutableThreshold.Load()
	if base <= 0 {
		return base
	}
	level := currentPoolPressureSnapshot().level
	return scaleMutableFlushThresholdForPressure(base, level)
}

func (db *DB) adaptiveBTreeMinIteratorSamples() uint64 {
	if db == nil {
		return adaptiveBTreeMinIteratorSamplesDefault
	}
	if db.memtableAdaptiveBTreeMinIters > 0 {
		return db.memtableAdaptiveBTreeMinIters
	}
	return adaptiveBTreeMinIteratorSamplesDefault
}

func (db *DB) noteAdaptiveMemtableDecision(
	mode memtable.Mode,
	reason adaptiveMemtableDecisionReason,
	writes, seqWrites, overwriteWrites, iters, rangeIters uint64,
	rangeIterPct float64,
) {
	if db == nil {
		return
	}
	if rangeIterPct < 0 {
		rangeIterPct = 0
	}
	if rangeIterPct > 1 {
		rangeIterPct = 1
	}
	rangePPM := uint32(rangeIterPct * 1_000_000)
	db.memtableAdaptiveDecisionTotal.Add(1)
	db.memtableAdaptiveDecisionReason.Store(uint32(reason))
	db.memtableAdaptiveDecisionMode.Store(uint32(mode))
	db.memtableAdaptiveDecisionWrites.Store(writes)
	db.memtableAdaptiveDecisionSeqWrites.Store(seqWrites)
	db.memtableAdaptiveDecisionOverwriteWrites.Store(overwriteWrites)
	db.memtableAdaptiveDecisionIters.Store(iters)
	db.memtableAdaptiveDecisionRangeIters.Store(rangeIters)
	db.memtableAdaptiveDecisionRangePctPPM.Store(rangePPM)

	switch reason {
	case adaptiveDecisionLowData:
		db.memtableAdaptiveDecisionLowDataTotal.Add(1)
	case adaptiveDecisionBTreeRange:
		db.memtableAdaptiveDecisionBTreeTotal.Add(1)
	case adaptiveDecisionBTreeBlockedMinIters:
		db.memtableAdaptiveDecisionBTreeBlockedMinItersTotal.Add(1)
	case adaptiveDecisionAppendSequential:
		db.memtableAdaptiveDecisionAppendTotal.Add(1)
	case adaptiveDecisionHashMixed:
		db.memtableAdaptiveDecisionHashTotal.Add(1)
	}
}

func (db *DB) chooseAdaptiveMemtableModeLocked() memtable.Mode {
	// Read stats atomially (no global lock needed for counts)
	writes := db.memtableStats.writes.Load()
	seqWrites := db.memtableStats.seqWrites.Load()
	overwriteWrites := db.memtableStats.overwriteWrites.Load()
	iters := db.memtableStats.iterators.Load()
	rangeIters := db.memtableStats.rangeIters.Load()

	// Default to configured mode if not enough data
	if writes < adaptiveMinWrites {
		mode := db.currentMemtableMode()
		db.noteAdaptiveMemtableDecision(mode, adaptiveDecisionLowData, writes, seqWrites, overwriteWrites, iters, rangeIters, 0)
		return mode
	}

	seqWritePct := float64(seqWrites) / float64(writes)
	overwriteWritePct := float64(overwriteWrites) / float64(writes)
	rangeIterPct := 0.0
	if iters > 0 {
		rangeIterPct = float64(rangeIters) / float64(iters)
	}

	btreeBlockedByMinIters := false
	// 1) Range-heavy read paths benefit most from BTree order stability.
	if rangeIterPct >= adaptiveRangeIteratorPct {
		minIters := db.adaptiveBTreeMinIteratorSamples()
		if minIters > 0 && (iters < minIters || rangeIters < minIters) {
			btreeBlockedByMinIters = true
		} else {
			db.noteAdaptiveMemtableDecision(memtable.ModeBTree, adaptiveDecisionBTreeRange, writes, seqWrites, overwriteWrites, iters, rangeIters, rangeIterPct)
			return memtable.ModeBTree
		}
	}

	// 2) Mostly increasing writes with low overwrite pressure favor append-only.
	if seqWritePct >= adaptiveSequentialWritePct && overwriteWritePct < adaptiveOverwriteWritePct {
		db.noteAdaptiveMemtableDecision(memtable.ModeAppendOnly, adaptiveDecisionAppendSequential, writes, seqWrites, overwriteWrites, iters, rangeIters, rangeIterPct)
		return memtable.ModeAppendOnly
	}

	// 3) Overwrite-heavy or mixed-write traffic defaults to hash-sorted.
	if btreeBlockedByMinIters {
		db.noteAdaptiveMemtableDecision(memtable.ModeHashSorted, adaptiveDecisionBTreeBlockedMinIters, writes, seqWrites, overwriteWrites, iters, rangeIters, rangeIterPct)
		return memtable.ModeHashSorted
	}
	db.noteAdaptiveMemtableDecision(memtable.ModeHashSorted, adaptiveDecisionHashMixed, writes, seqWrites, overwriteWrites, iters, rangeIters, rangeIterPct)
	return memtable.ModeHashSorted
}

func (db *DB) updateAdaptiveObservationLocked() {
	observe := db.memtableAdaptive
	if observe && !db.memtableWarmupActive && db.currentMemtableMode() == memtable.ModeAppendOnly {
		observe = false
	}
	db.memtableAdaptiveObserve.Store(observe)
}

func (db *DB) applyAdaptiveMemtableModeLocked() memtable.Mode {
	desired := db.chooseAdaptiveMemtableModeLocked()
	db.storeMemtableMode(desired)
	db.updateAdaptiveObservationLocked()
	return desired
}

func (db *DB) currentMemtableMode() memtable.Mode {
	if db == nil {
		return memtable.ModeSkiplist
	}
	return memtable.Mode(db.memtableMode.Load())
}

func (db *DB) storeMemtableMode(mode memtable.Mode) {
	if db == nil {
		return
	}
	db.memtableMode.Store(uint32(mode))
}

func validateValueLogDomainThresholds(domains []backenddb.ValueLogDomainThreshold) error {
	seen := make(map[string]struct{}, len(domains))
	for i := range domains {
		d := domains[i]
		if len(d.Prefix) == 0 {
			return fmt.Errorf("cachingdb: value-log domain threshold[%d] has empty prefix", i)
		}
		if d.InlineThreshold < 0 {
			return fmt.Errorf("cachingdb: value-log domain threshold[%d] has negative inline threshold %d", i, d.InlineThreshold)
		}
		key := string(d.Prefix)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("cachingdb: duplicate value-log domain threshold prefix %q", d.Prefix)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func Open(dir string, backend BackendDB, opts Options) (*DB, error) {
	if !opts.AllowUnsafe && (opts.DisableWAL || opts.RelaxedSync || opts.DisableReadChecksum) {
		return nil, ErrUnsafeOptions
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = 256 * 1024 * 1024 // 256MB default
	}
	memCap := memtableCapacity(opts.FlushThreshold)
	modeStr := opts.MemtableMode
	if modeStr == "" {
		modeStr = "adaptive"
	}
	adaptive := false
	if modeStr == "adaptive" || modeStr == "auto" {
		adaptive = true
		modeStr = "append_only"
	} else if strings.HasPrefix(modeStr, "adaptive:") {
		adaptive = true
		modeStr = strings.TrimPrefix(modeStr, "adaptive:")
	}
	mode, err := memtable.ModeFromString(modeStr)
	if err != nil {
		return nil, err
	}
	shardCount := opts.MemtableShards
	if shardCount <= 0 {
		shardCount = defaultMemtableShards()
	}
	shardCount = normalizeShardCount(shardCount)
	if shardCount < 1 {
		shardCount = 1
	}
	domainIngressWorkers := opts.DomainIngressWorkers
	if domainIngressWorkers < 0 {
		domainIngressWorkers = 0
	}
	if domainIngressWorkers > shardCount {
		domainIngressWorkers = shardCount
	}
	domainIngressQueueSize := opts.DomainIngressQueueSize
	if domainIngressWorkers > 0 && domainIngressQueueSize <= 0 {
		domainIngressQueueSize = defaultDomainIngressQueueSize
	}
	if opts.MaxQueuedMemtables == 0 {
		// Keep the default queued backlog roughly stable in bytes when callers
		// tune FlushThreshold. Historically: 64MB flush threshold with a queue
		// length of 4 => ~256MB backlog.
		opts.MaxQueuedMemtables = defaultMaxQueuedMemtables(opts.FlushThreshold) * shardCount
	}
	if opts.WriterFlushMaxMemtables == 0 {
		opts.WriterFlushMaxMemtables = 1
	}
	flushBuildAutoConcurrency := opts.FlushBuildConcurrency <= 0
	if opts.FlushBuildConcurrency <= 0 {
		opts.FlushBuildConcurrency = runtime.GOMAXPROCS(0)
		if opts.FlushBuildConcurrency < 1 {
			opts.FlushBuildConcurrency = 1
		}
	}
	if opts.FlushBuildMinEntries <= 0 {
		opts.FlushBuildMinEntries = 16 * 1024
	}
	if opts.FlushBuildMinUnits <= 0 {
		opts.FlushBuildMinUnits = 2
	}
	if opts.FlushBuildChunkCap < 0 {
		opts.FlushBuildChunkCap = 8192
	}
	if opts.FlushBuildChunkTargetBytes <= 0 {
		opts.FlushBuildChunkTargetBytes = 2 << 20
	}
	if opts.FlushBuildChunkMinBytes <= 0 {
		opts.FlushBuildChunkMinBytes = 1 << 20
	}
	if opts.FlushBuildChunkMaxBytes <= 0 {
		opts.FlushBuildChunkMaxBytes = 4 << 20
	}
	if opts.FlushBuildPrefetchUnits <= 0 {
		opts.FlushBuildPrefetchUnits = opts.FlushBuildConcurrency
	}
	if opts.FlushBackendMaxEntries == 0 {
		// In relaxed durability modes, additional commit boundaries are cheap and
		// can reduce index.db high-watermark growth under small KeepRecent windows
		// by making retired pages eligible for reuse sooner. Default to a smaller
		// chunk size in that case.
		if opts.DisableWAL || opts.RelaxedSync {
			opts.FlushBackendMaxEntries = 2 * flushBackendBatchInitEntries
		} else {
			opts.FlushBackendMaxEntries = flushBackendBatchMaxEntries
		}
	} else if opts.FlushBackendMaxEntries < 0 {
		// Negative disables chunking; use a very large cap so the hot path
		// never triggers intermediate commits.
		opts.FlushBackendMaxEntries = int(^uint(0) >> 1)
		// Disable the max-batch cap when chunking is explicitly disabled.
		// This preserves the documented "<0 disables chunking" behavior without
		// accidentally re-enabling it via the cap adjustment logic.
		if opts.FlushBackendMaxBatches == 0 {
			opts.FlushBackendMaxBatches = -1
		}
	}
	if opts.FlushBackendMaxBatches == 0 {
		// In relaxed durability modes, additional commit boundaries are much
		// cheaper and can substantially reduce index.db high-watermark growth
		// under small KeepRecent windows by making retired pages eligible for
		// reuse sooner. Use a slightly higher default budget in that case.
		if opts.DisableWAL || opts.RelaxedSync {
			opts.FlushBackendMaxBatches = 32
		} else {
			opts.FlushBackendMaxBatches = 16
		}
	}
	flushBackendInitEntries := flushBackendBatchInitEntries
	if flushBackendInitEntries > opts.FlushBackendMaxEntries {
		flushBackendInitEntries = opts.FlushBackendMaxEntries
	}
	if flushBackendInitEntries < 1 {
		flushBackendInitEntries = 1
	}
	if err := validateValueLogDomainThresholds(opts.ValueLogDomainInlineThresholds); err != nil {
		return nil, err
	}

	// Ensure wal dir exists
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0700); err != nil {
		return nil, err
	}
	warnInsecureDir(walDir, opts.NotifyError)
	segments, _ := listNonEmptyLogSegments(walDir)
	// Cached value-log RIDs remain globally unique across reopen/rewrite cycles.
	// Until we persist nextRID separately, opening must recover the max on-disk
	// RID here rather than risk reusing low RIDs after a clean reopen.
	maxExistingRID, err := maxValueLogRIDFromSegments(tailValueLogSegmentsByLane(segments))
	if err != nil {
		return nil, err
	}
	maxLaneID := -1
	maxWALSeq := make(map[int]int)
	maxVlogSeq := make(map[int]int)
	for _, seg := range segments {
		if seg.lane > maxLaneID {
			maxLaneID = seg.lane
		}
		if seg.valueLog {
			if seg.seq > maxVlogSeq[seg.lane] {
				maxVlogSeq[seg.lane] = seg.seq
			}
		} else if seg.seq > maxWALSeq[seg.lane] {
			maxWALSeq[seg.lane] = seg.seq
		}
	}
	laneCount := opts.JournalLanes
	laneCountDefaulted := laneCount <= 0
	if laneCountDefaulted {
		laneCount = defaultJournalLaneCount(runtime.GOMAXPROCS(0))
	}
	valueLogGenerationPolicy := backenddb.ValueLogGenerationPolicy(opts.ValueLogGenerationPolicy)
	switch valueLogGenerationPolicy {
	case backenddb.ValueLogGenerationDefault:
		// In cached mode, default generation policy enables background
		// maintenance. Bench profiles explicitly set ValueLogGenerationOff.
		valueLogGenerationPolicy = backenddb.ValueLogGenerationHotWarmCold
	case backenddb.ValueLogGenerationOff, backenddb.ValueLogGenerationHotWarmCold:
	default:
		return nil, fmt.Errorf("cachingdb: invalid value-log generation policy %d", opts.ValueLogGenerationPolicy)
	}
	valueLogGenerationPolicyUint8 := uint8(valueLogGenerationPolicy)
	// Hot/warm/cold generation reserves lanes 1 and 2 for non-hot classes when
	// available. When the lane count is caller-provided (e.g., tests pinning
	// JournalLanes=1), allow fewer lanes and treat all lanes as hot.
	if laneCountDefaulted && valueLogGenerationPolicy == backenddb.ValueLogGenerationHotWarmCold && laneCount < 3 {
		laneCount = 3
	}
	// Temporarily remove the logic that increases laneCount based on maxLaneID
	if maxLaneID+1 > laneCount {
		laneCount = maxLaneID + 1
	}

	inlineThreshold := page.DefaultInlineThreshold
	if provider, ok := backend.(interface{ InlineThreshold() int }); ok {
		if v := provider.InlineThreshold(); v >= 0 {
			inlineThreshold = v
		}
	}
	valueLogThreshold := opts.ValueLogPointerThreshold
	if valueLogThreshold <= 0 {
		valueLogThreshold = page.DefaultInlineThreshold
	}
	valueLogDomainThresholds := backenddb.NormalizeValueLogDomainThresholds(opts.ValueLogDomainInlineThresholds)
	valueLogMaxSegmentBytes := opts.ValueLogMaxSegmentBytes
	if valueLogMaxSegmentBytes < 0 {
		valueLogMaxSegmentBytes = 0
	}
	valueLogGenerationHotTarget := opts.ValueLogGenerationHotSegmentTargetBytes
	valueLogGenerationWarmTarget := opts.ValueLogGenerationWarmSegmentTargetBytes
	valueLogGenerationColdTarget := opts.ValueLogGenerationColdSegmentTargetBytes
	valueLogRewriteBudgetBytes := opts.ValueLogRewriteBudgetBytesPerSec
	valueLogRewriteBudgetRecords := opts.ValueLogRewriteBudgetRecordsPerSec
	valueLogRewriteTriggerRatioPPM := opts.ValueLogRewriteTriggerStaleRatioPPM
	valueLogRewriteTriggerBytes := opts.ValueLogRewriteTriggerTotalBytes
	valueLogRewriteTriggerChurn := opts.ValueLogRewriteTriggerChurnPerSec
	if valueLogGenerationHotTarget < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational hot segment target bytes %d", valueLogGenerationHotTarget)
	}
	if valueLogGenerationWarmTarget < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational warm segment target bytes %d", valueLogGenerationWarmTarget)
	}
	if valueLogGenerationColdTarget < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational cold segment target bytes %d", valueLogGenerationColdTarget)
	}
	if valueLogRewriteBudgetBytes < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite budget bytes/sec %d", valueLogRewriteBudgetBytes)
	}
	if valueLogRewriteBudgetRecords < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite budget records/sec %d", valueLogRewriteBudgetRecords)
	}
	if valueLogRewriteTriggerBytes < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite trigger total bytes %d", valueLogRewriteTriggerBytes)
	}
	if valueLogRewriteTriggerChurn < 0 {
		return nil, fmt.Errorf("cachingdb: invalid value-log generational rewrite trigger churn/sec %d", valueLogRewriteTriggerChurn)
	}
	if valueLogGenerationPolicyUint8 == uint8(backenddb.ValueLogGenerationHotWarmCold) {
		if valueLogGenerationHotTarget == 0 {
			valueLogGenerationHotTarget = defaultVlogGenerationHotTargetBytes
		}
		if valueLogGenerationWarmTarget == 0 {
			valueLogGenerationWarmTarget = defaultVlogGenerationWarmTargetBytes
		}
		if valueLogGenerationColdTarget == 0 {
			valueLogGenerationColdTarget = defaultVlogGenerationColdTargetBytes
		}
		if valueLogRewriteBudgetBytes == 0 {
			valueLogRewriteBudgetBytes = defaultVlogRewriteBudgetBytesPerSec
		}
		if valueLogRewriteTriggerBytes == 0 {
			valueLogRewriteTriggerBytes = defaultVlogRewriteTriggerTotalBytes
		}
		if valueLogRewriteTriggerRatioPPM == 0 {
			valueLogRewriteTriggerRatioPPM = defaultVlogRewriteTriggerStalePPM
		}
	}
	valueLogRawWritevMinAvgBytes := opts.ValueLogRawWritevMinAvgBytes
	if valueLogRawWritevMinAvgBytes < 0 {
		valueLogRawWritevMinAvgBytes = 0
	}
	valueLogRawWritevMinRecords := opts.ValueLogRawWritevMinBatchRecords
	if valueLogRawWritevMinRecords <= 0 {
		valueLogRawWritevMinRecords = 8
	}
	disableJournal := opts.DisableWAL
	var retained map[string]struct{}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		if retained == nil {
			retained = make(map[string]struct{})
		}
		retained[seg.path] = struct{}{}
	}

	warmupThreshold := opts.FlushThreshold
	if adaptive && adaptiveWarmupBytes > 0 && int64(adaptiveWarmupBytes) < opts.FlushThreshold {
		warmupThreshold = int64(adaptiveWarmupBytes)
	}
	memCap = shardCapacity(memCap, shardCount)
	warmupCap := shardCapacity(memtableCapacity(warmupThreshold), shardCount)
	indexer := memtable.NewHashSortedIndexer()
	mutableShards := make([]memShard, shardCount)
	appendOnlyEstimate := appendOnlyEstimatedBytesPerEntryDefault
	for i := range mutableShards {
		if mode == memtable.ModeAppendOnly {
			mutableShards[i] = memShard{mem: memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(warmupCap, appendOnlyEstimate)}
			continue
		}
		mt, err := memtable.NewWithCapacityModeAndIndexer(warmupCap, mode, indexer)
		if err != nil {
			return nil, err
		}
		mutableShards[i] = memShard{mem: mt}
	}
	reader, err := valuelog.NewManager(walDir)
	if err != nil {
		return nil, err
	}
	reader.SetDisableReadChecksum(opts.DisableReadChecksum)
	valueLogReader := reader
	debugFlushPointers := envBool(envDebugFlushPointers)
	debugFlushTiming := envBool(envDebugFlushTiming)
	adaptiveBTreeMinIters := envUint64(envAdaptiveBTreeMinIteratorSamples, adaptiveBTreeMinIteratorSamplesDefault)

	valueLogCompressionMode := normalizeVlogCompressionMode(opts.ValueLogCompression)
	if valueLogCompressionMode == vlogCompressionDefault {
		// ValueLogCompression=0 is "unset/default" and resolves to auto mode.
		valueLogCompressionMode = vlogCompressionAuto
	}
	valueLogBlockCodec := normalizeVlogBlockCodec(opts.ValueLogBlockCodec)
	valueLogBlockTargetBytes := valuelog.NormalizeBlockTargetCompressedBytes(opts.ValueLogBlockTargetCompressedBytes)
	valueLogIncompressibleHold := opts.ValueLogIncompressibleHoldBytes
	if valueLogIncompressibleHold <= 0 {
		valueLogIncompressibleHold = defaultVlogHoldBytes
	}
	if valueLogIncompressibleHold < 64<<10 {
		valueLogIncompressibleHold = 64 << 10
	}
	valueLogIncompressibleProbe := opts.ValueLogIncompressibleProbeBytes
	if valueLogIncompressibleProbe <= 0 {
		valueLogIncompressibleProbe = defaultVlogProbeBytes
	}
	if valueLogIncompressibleProbe < 64<<10 {
		valueLogIncompressibleProbe = 64 << 10
	}
	if valueLogIncompressibleProbe > valueLogIncompressibleHold {
		valueLogIncompressibleProbe = valueLogIncompressibleHold
	}
	valueLogAutoPolicy := normalizeVlogAutoPolicy(opts.ValueLogAutoPolicy)

	valueLogDictTrain := opts.ValueLogDictTrain
	if valueLogCompressionMode == vlogCompressionOff || valueLogCompressionMode == vlogCompressionBlock {
		// Explicit off/block mode bypasses dictionary writes and training.
		valueLogDictTrain.TrainBytes = -1
	} else if valueLogDictTrain.TrainBytes == 0 {
		// "auto" and explicit "dict" modes are expected to train dictionaries by
		// default so dict compression can become active without additional config.
		// Callers may explicitly disable training via TrainBytes < 0.
		valueLogDictTrain.TrainBytes = compression.DefaultTrainBytes
	}
	valueLogDictMaxK := opts.ValueLogDictMaxK
	if valueLogDictMaxK <= 0 {
		valueLogDictMaxK = 32
	}
	if valueLogDictMaxK < 1 {
		valueLogDictMaxK = 1
	}
	if valueLogDictMaxK > valuelog.MaxFrameK {
		valueLogDictMaxK = valuelog.MaxFrameK
	}

	valueLogDictFrameEncodeLevel := opts.ValueLogDictFrameEncodeLevel
	if valueLogDictFrameEncodeLevel <= 0 {
		valueLogDictFrameEncodeLevel = zstd.SpeedFastest
	}
	if valueLogDictFrameEncodeLevel < zstd.SpeedFastest || valueLogDictFrameEncodeLevel > zstd.SpeedBestCompression {
		valueLogDictFrameEncodeLevel = zstd.SpeedFastest
	}
	valueLogDictFrameEnableEntropy := opts.ValueLogDictFrameEnableEntropy

	valueLogDictAdaptiveRatio := opts.ValueLogDictAdaptiveRatio
	valueLogDictMetricsWindow := opts.ValueLogDictMetricsWindowBytes
	valueLogDictMetricsMinRecords := opts.ValueLogDictMetricsMinRecords
	valueLogDictMetricsPauseBytes := opts.ValueLogDictMetricsPauseBytes

	minPayloadSavings := opts.ValueLogDictMinPayloadSavingsRatio
	if minPayloadSavings <= 0 {
		// Throughput-oriented default: avoid publishing dictionaries unless they
		// deliver clear payload reduction. This keeps dict-enabled mode close to
		// raw mode on incompressible streams.
		minPayloadSavings = 0.02
		if opts.ForceValueLogPointers || opts.DisableWAL {
			minPayloadSavings = 0.05
		}
	}

	valueLogAutotuneCandidateKSet := len(opts.ValueLogCompressionAutotune.CandidateK) > 0
	valueLogAutotune := valuelog.NormalizeAutotuneOptions(opts.ValueLogCompressionAutotune, true)
	valueLogTemplateEnabled := opts.ValueLogTemplateMode != template.TemplateOff
	valueLogTemplateCfg := template.NormalizeConfig(opts.ValueLogTemplateConfig)
	if opts.ValueLogTemplateMode == template.TemplatePrepass {
		// TemplatePrepass can be CPU-heavy (template match + dict/zstd encode). If
		// templates have not been kept recently, enter cold mode sooner so we don't
		// pay candidate lookup/matching cost on every value.
		if opts.ValueLogTemplateConfig.ColdSearchAfter <= 0 {
			valueLogTemplateCfg.ColdSearchAfter = 64
		}
		if opts.ValueLogTemplateConfig.ColdSearchProbeEvery <= 0 {
			valueLogTemplateCfg.ColdSearchProbeEvery = 64
		}
	}
	valueLogTemplateDecodeOpts := template.DecodeOptions{MaxGaps: valueLogTemplateCfg.MaxGaps, MaxDecodedBytes: valueLogTemplateCfg.MaxDecodedBytes, DefCacheSize: valueLogTemplateCfg.DefCacheSize}
	if valueLogTemplateDecodeOpts.MaxDecodedBytes <= 0 && limits.MaxRecordSize > 0 {
		valueLogTemplateDecodeOpts.MaxDecodedBytes = int(limits.MaxRecordSize)
	}

	// Favor aggressive sampling so the first dict arrives quickly. The trainer
	// still caps total work via TrainBytes and queue backpressure.
	if valueLogDictTrain.TrainBytes > 0 && valueLogDictTrain.SampleStride == 0 {
		valueLogDictTrain.SampleStride = 1
	}

	// If dict training is enabled but no adaptive ratio is specified, default to
	// a conservative pause threshold to avoid wasting CPU on incompressible
	// payload streams.
	if valueLogDictTrain.TrainBytes > 0 && valueLogDictAdaptiveRatio == 0 {
		// Require meaningful savings before staying in "dict mode". Payload ratios
		// close to 1.0 can be slower than raw frames due to additional framing and
		// encode/decode overhead, especially for small values and write-heavy batch
		// workloads.
		valueLogDictAdaptiveRatio = 0.98
	}
	if valueLogDictAdaptiveRatio > 0 {
		if valueLogDictMetricsWindow <= 0 {
			// Smaller windows let us detect "no-op" dict streams quickly and avoid
			// spending long stretches in the slower dict framing path on
			// incompressible payloads.
			valueLogDictMetricsWindow = 256 << 10
		}
		if valueLogDictMetricsPauseBytes <= 0 && valueLogAutotune.Mode != valuelog.AutotuneOff && valueLogAutotune.PauseBytes > 0 {
			valueLogDictMetricsPauseBytes = int(valueLogAutotune.PauseBytes)
		}
		if valueLogDictMetricsPauseBytes <= 0 {
			// Degraded streams should stay paused long enough that "dict enabled"
			// is effectively free on incompressible data, while still allowing
			// occasional probes to detect when compressibility returns.
			valueLogDictMetricsPauseBytes = 64 << 20
		}
	}

	// When dict compression is paused, periodically probe compression to recover
	// quickly if the payload stream becomes compressible again.
	probeBytes := valueLogDictMetricsPauseBytes
	if probeBytes <= 0 && valueLogAutotune.Mode != valuelog.AutotuneOff && valueLogAutotune.ProbeBytes > 0 {
		probeBytes = int(valueLogAutotune.ProbeBytes)
	}
	if probeBytes <= 0 {
		probeBytes = 64 << 20
	}
	probeBytes /= 4
	if probeBytes < 64<<10 {
		probeBytes = 64 << 10
	}
	incompressibleHoldBytes := opts.ValueLogDictIncompressibleHoldBytes
	if incompressibleHoldBytes < 0 {
		incompressibleHoldBytes = 0
	}
	if incompressibleHoldBytes > 0 && incompressibleHoldBytes < 8<<20 {
		incompressibleHoldBytes = 8 << 20
	}
	incompressibleProbeBytes := opts.ValueLogDictProbeIntervalBytes
	if incompressibleHoldBytes > 0 {
		if incompressibleProbeBytes <= 0 {
			incompressibleProbeBytes = incompressibleHoldBytes / 8
		}
		if incompressibleProbeBytes < 64<<10 {
			incompressibleProbeBytes = 64 << 10
		}
		if incompressibleProbeBytes > incompressibleHoldBytes {
			incompressibleProbeBytes = incompressibleHoldBytes
		}
	} else {
		incompressibleProbeBytes = 0
	}
	// While paused on degraded/incompressible streams, keep sampling sparse to
	// minimize hot-path CPU overhead.
	pausedSampleStride := uint64(256)
	selectorSeedCodec := valueLogBlockCodec
	if valueLogCompressionMode == vlogCompressionAuto && valueLogAutoPolicy != vlogAutoThroughput {
		selectorSeedCodec = valuelog.BlockCodecLZ4
	}

	lanes := make([]lane, laneCount)
	for i := range lanes {
		lanes[i].id = i
		lanes[i].walSeq = maxWALSeq[i]
		lanes[i].vlogSeq = maxVlogSeq[i]
		lanes[i].vlogGenerationClass = vlogGenerationClassHot
		lanes[i].vlogCompressionSelector = newVlogCompressionSelectorWithSeed(
			valueLogAutoPolicy,
			uint64(valueLogIncompressibleHold),
			uint64(valueLogIncompressibleProbe),
			selectorSeedCodec,
		)
	}
	if valueLogGenerationPolicyUint8 == uint8(backenddb.ValueLogGenerationHotWarmCold) && len(lanes) >= 3 {
		// Reserve one lane for warm and one for cold; remaining lanes serve hot.
		lanes[1].vlogGenerationClass = vlogGenerationClassWarm
		lanes[2].vlogGenerationClass = vlogGenerationClassCold
	}
	db := &DB{
		dir:                                  walDir,
		backend:                              backend,
		flushThreshold:                       opts.FlushThreshold,
		memtableCap:                          memCap,
		memtableAdaptive:                     adaptive,
		memtableAdaptiveBTreeMinIters:        adaptiveBTreeMinIters,
		memtableWarmupActive:                 adaptive && warmupThreshold < opts.FlushThreshold,
		memtableWarmupThreshold:              warmupThreshold,
		domainIngressWorkers:                 domainIngressWorkers,
		domainIngressQueueSize:               domainIngressQueueSize,
		maxQueuedMemtables:                   opts.MaxQueuedMemtables,
		slowdownBacklogSeconds:               opts.SlowdownBacklogSeconds,
		stopBacklogSeconds:                   opts.StopBacklogSeconds,
		maxBacklogBytes:                      opts.MaxBacklogBytes,
		writerFlushMaxMemtables:              opts.WriterFlushMaxMemtables,
		writerFlushMaxDuration:               opts.WriterFlushMaxDuration,
		flushBuildConcurrency:                opts.FlushBuildConcurrency,
		flushBuildAutoConcurrency:            flushBuildAutoConcurrency,
		flushBuildMinEntries:                 opts.FlushBuildMinEntries,
		flushBuildMinUnits:                   opts.FlushBuildMinUnits,
		flushBuildChunkCap:                   opts.FlushBuildChunkCap,
		flushBuildChunkTarget:                opts.FlushBuildChunkTargetBytes,
		flushBuildChunkMinBytes:              opts.FlushBuildChunkMinBytes,
		flushBuildChunkMaxBytes:              opts.FlushBuildChunkMaxBytes,
		flushBuildPrefetchUnits:              opts.FlushBuildPrefetchUnits,
		flushBackendMaxEntries:               opts.FlushBackendMaxEntries,
		flushBackendInitEntries:              flushBackendInitEntries,
		flushBackendMaxBatches:               opts.FlushBackendMaxBatches,
		walMaxSegmentBytes:                   opts.WALMaxSegmentBytes,
		valueLogMaxSegmentBytes:              valueLogMaxSegmentBytes,
		journalCompression:                   opts.JournalCompression,
		disableJournal:                       disableJournal,
		disableValueLog:                      false,
		splitValueLog:                        true,
		relaxedSync:                          opts.RelaxedSync,
		notifyError:                          opts.NotifyError,
		inlineThreshold:                      inlineThreshold,
		valueLogThreshold:                    valueLogThreshold,
		valueLogDomainThresholds:             valueLogDomainThresholds,
		forceValueLogPointers:                opts.ForceValueLogPointers,
		valueLogRawWritevMinAvgBytes:         valueLogRawWritevMinAvgBytes,
		valueLogRawWritevMinRecords:          valueLogRawWritevMinRecords,
		valueLogCompressionMode:              uint8(valueLogCompressionMode),
		valueLogBlockCodec:                   valueLogBlockCodec,
		valueLogBlockTargetBytes:             valueLogBlockTargetBytes,
		valueLogIncompressibleHold:           uint64(valueLogIncompressibleHold),
		valueLogIncompressibleProbe:          uint64(valueLogIncompressibleProbe),
		valueLogAutoPolicy:                   uint8(valueLogAutoPolicy),
		valueLogGenerationPolicy:             valueLogGenerationPolicyUint8,
		valueLogGenerationHotTarget:          valueLogGenerationHotTarget,
		valueLogGenerationWarmTarget:         valueLogGenerationWarmTarget,
		valueLogGenerationColdTarget:         valueLogGenerationColdTarget,
		valueLogRewriteBudgetBytes:           valueLogRewriteBudgetBytes,
		valueLogRewriteBudgetRecords:         valueLogRewriteBudgetRecords,
		valueLogRewriteTriggerRatioPPM:       valueLogRewriteTriggerRatioPPM,
		valueLogRewriteTriggerBytes:          valueLogRewriteTriggerBytes,
		valueLogRewriteTriggerChurn:          valueLogRewriteTriggerChurn,
		memtableValueLogPointers:             true,
		indexOuterLeavesInValueLog:           opts.IndexOuterLeavesInValueLog,
		valueLogReader:                       valueLogReader,
		valueLogRetain:                       retained,
		debugFlushPointers:                   debugFlushPointers,
		debugFlushTiming:                     debugFlushTiming,
		maxValueLogRetainedBytes:             opts.MaxValueLogRetainedBytes,
		maxValueLogRetainedBytesHard:         opts.MaxValueLogRetainedBytesHard,
		valueLogDictTrain:                    valueLogDictTrain,
		valueLogDictMaxK:                     valueLogDictMaxK,
		valueLogDictFrameEncodeLevel:         valueLogDictFrameEncodeLevel,
		valueLogDictFrameEnableEntropy:       valueLogDictFrameEnableEntropy,
		valueLogDictAdaptiveRatio:            valueLogDictAdaptiveRatio,
		valueLogDictMinPayloadSavings:        minPayloadSavings,
		valueLogDictMetricsWindow:            valueLogDictMetricsWindow,
		valueLogDictMetricsMinRecords:        valueLogDictMetricsMinRecords,
		valueLogDictMetricsPauseBytes:        valueLogDictMetricsPauseBytes,
		valueLogDictProbeBytes:               uint64(probeBytes),
		valueLogDictIncompressibleHoldBytes:  uint64(incompressibleHoldBytes),
		valueLogDictIncompressibleProbeBytes: uint64(incompressibleProbeBytes),
		valueLogDictPausedSampleStride:       pausedSampleStride,
		valueLogAutotuneOptions:              valueLogAutotune,
		valueLogAutotuneCandidateKSet:        valueLogAutotuneCandidateKSet,
		valueLogTemplateEnabled:              valueLogTemplateEnabled,
		valueLogTemplateMode:                 opts.ValueLogTemplateMode,
		valueLogTemplateReadStrict:           opts.ValueLogTemplateReadStrict,
		valueLogTemplateDecodeOpts:           valueLogTemplateDecodeOpts,
		valueLogTemplateEngine: func() *template.Engine {
			if !valueLogTemplateEnabled {
				return nil
			}
			return template.NewEngine(valueLogTemplateCfg)
		}(),
		mutableShards:         mutableShards,
		mutableShardMask:      uint64(shardCount - 1),
		hashSortedIndexer:     indexer,
		closeCh:               make(chan struct{}),
		flushCh:               make(chan struct{}, 1),
		autoCheckpointOnceCh:  make(chan struct{}, 1),
		autoCheckpointWriteCh: make(chan struct{}, 1),
		lanes:                 lanes,
		flushLaneMu:           make([]sync.Mutex, len(lanes)),
	}
	db.storeMemtableMode(mode)
	if maxExistingRID > 0 {
		db.nextRID.Store(maxExistingRID)
	}
	db.rebuildGenerationLaneSets()
	db.valueLogAutotuneMetrics.init(valuelog.RealClock{})
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
	db.bpCond = sync.NewCond(&db.bpMu)
	db.laneCond = sync.NewCond(&db.laneMu)
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	db.updateAdaptiveObservationLocked()
	nowNS := time.Now().UnixNano()
	db.materializationLastDrainUnixNano.Store(nowNS)
	db.publishWatermarkLastUnixNano = nowNS

	// Open initial value-log segments (if enabled) and journal/commit log
	// segments (if enabled). Journal and value log are decoupled.
	if db.valueLogEnabled() {
		for i := range db.lanes {
			if err := db.rotateValueLogLocked(&db.lanes[i]); err != nil {
				if db.valueLogReader != nil {
					_ = db.valueLogReader.Close()
					db.valueLogReader = nil
				}
				for j := 0; j <= i && j < len(db.lanes); j++ {
					db.cleanupLaneWALWriters(&db.lanes[j])
				}
				return nil, err
			}
		}
	}
	if !db.disableJournal {
		for i := range db.lanes {
			if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
				if db.valueLogReader != nil {
					_ = db.valueLogReader.Close()
					db.valueLogReader = nil
				}
				for j := 0; j <= i && j < len(db.lanes); j++ {
					db.cleanupLaneWALWriters(&db.lanes[j])
				}
				return nil, err
			}
		}
	}
	if len(segments) > 0 {
		for _, seg := range segments {
			if seg.lane < 0 || seg.lane >= len(db.lanes) {
				continue
			}
			l := &db.lanes[seg.lane]
			if db.splitValueLog {
				if seg.valueLog {
					if seg.path == l.vlogPath {
						continue
					}
					if l.vlogClosedSizes == nil {
						l.vlogClosedSizes = make(map[string]int64)
					}
					l.vlogClosedSizes[seg.path] = seg.size
					l.vlogClosedBytes.Add(seg.size)
				} else {
					if seg.path == l.walPath {
						continue
					}
					if l.walClosedSizes == nil {
						l.walClosedSizes = make(map[string]int64)
					}
					l.walClosedSizes[seg.path] = seg.size
					l.walClosedBytes.Add(seg.size)
				}
				continue
			}
			if seg.valueLog != db.walUsesValueLog() {
				continue
			}
			if seg.path == l.walPath {
				continue
			}
			if l.walClosedSizes == nil {
				l.walClosedSizes = make(map[string]int64)
			}
			l.walClosedSizes[seg.path] = seg.size
			l.walClosedBytes.Add(seg.size)
		}
	}
	if !db.disableJournal {
		for i := range db.lanes {
			db.startWALWriter(&db.lanes[i])
		}
	}
	if db.valueLogEnabled() {
		for i := range db.lanes {
			db.startVlogWriter(&db.lanes[i])
		}
	}

	if opts.IndexOuterLeavesInValueLog {
		setter, ok := backend.(interface{ SetLeafPageLog(backenddb.LeafPageLog) })
		if !ok {
			return nil, errors.New("cachingdb: backend does not support value-log leaf pages")
		}
		setter.SetLeafPageLog(newCachingLeafPageLog(db, 0))
	}
	if opts.DisableWAL {
		// Stage the adaptive gate on the fastest cached profile first. WAL-on
		// modes keep the legacy zipper fan-out policy until benchmark data shows
		// this pressure signal is neutral there as well.
		if setter, ok := backend.(interface {
			SetZipperParallelMergePressureSource(src zipper.ParallelMergePressureSource)
		}); ok {
			setter.SetZipperParallelMergePressureSource(func() zipper.ParallelMergePressureLevel {
				return currentZipperParallelMergePressure()
			})
		}
	}

	// Publish initial memtable snapshot for lock-free reads.
	db.mu.Lock()
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	db.mu.Unlock()

	db.startDomainIngressWorkers()

	// Start background flusher
	db.wg.Add(1)
	go db.flushLoop()
	db.startVlogGenerationLoop()
	db.startVlogShapeLoop()
	registerTreeDBExpvarStatsDB(db)

	return db, nil
}

type flushBuildJob struct {
	mem      memtable.Table
	out      chan<- []batch.Entry
	cancel   <-chan struct{}
	chunkCap int
	errCh    chan<- error
}

func (job flushBuildJob) report(err error) {
	if err == nil || job.errCh == nil {
		return
	}
	select {
	case job.errCh <- err:
	default:
	}
}

func (job flushBuildJob) run(closeCh <-chan struct{}) {
	if job.mem == nil || job.out == nil {
		if job.out != nil {
			close(job.out)
		}
		job.report(errors.New("cachingdb: flush build job missing memtable/out"))
		return
	}
	chunkCap := job.chunkCap
	if chunkCap <= 0 {
		chunkCap = 8192
	}

	iter := job.mem.NewIterator(nil, nil)

	send := func(ops []batch.Entry) bool {
		for {
			select {
			case job.out <- ops:
				return true
			case <-job.cancel:
				return false
			case <-closeCh:
				return false
			}
		}
	}

	ops := getEntrySlice(chunkCap)
	ops = ops[:0]
	for iter.Valid() {
		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			ops = append(ops, batch.Entry{
				Type: batch.OpDelete,
				Key:  iter.UnsafeKey(),
			})
		} else if flags&node.FlagPointer != 0 {
			ops = append(ops, batch.Entry{
				Type:     batch.OpPut,
				Key:      iter.UnsafeKey(),
				ValuePtr: ptr,
				IsPtr:    true,
			})
		} else {
			ops = append(ops, batch.Entry{
				Type:  batch.OpPut,
				Key:   iter.UnsafeKey(),
				Value: val,
			})
		}
		iter.Next()

		if len(ops) >= cap(ops) {
			if !send(ops) {
				putEntrySlice(ops)
				close(job.out)
				return
			}
			ops = getEntrySlice(chunkCap)
			ops = ops[:0]
		}
	}

	err := iter.Error()
	cerr := iter.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		putEntrySlice(ops)
		close(job.out)
		job.report(err)
		return
	}
	if len(ops) > 0 {
		if !send(ops) {
			putEntrySlice(ops)
			close(job.out)
			return
		}
	} else {
		putEntrySlice(ops)
	}
	close(job.out)
}

func (db *DB) reportError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, errDBClosing) {
		return
	}
	if db != nil && db.closing.Load() {
		return
	}
	if db.notifyError != nil {
		db.notifyError(err)
	}
	db.bgErrMu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.bgErrMu.Unlock()
}

func (db *DB) backgroundError() error {
	db.bgErrMu.Lock()
	defer db.bgErrMu.Unlock()
	return db.bgErr
}

// StartAutoCheckpoint enables a background loop that periodically forces a
// durable boundary and trims cached-mode WAL segments. When idleInterval > 0,
// it also triggers an opportunistic checkpoint after a period of write-idleness.
//
// interval > 0 enables periodic checkpoints. maxWALBytes is a safety cap: if > 0,
// the loop will attempt to checkpoint when the effective WAL bytes exceed this
// cap. maxWALBytes <= 0 disables the size trigger.
//
// This does not make each individual write durable; it bounds the window of
// unsynced writes for long-running workloads.
func (db *DB) StartAutoCheckpoint(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) {
	if db == nil {
		return
	}
	db.autoCheckpointMaxWALBytes.Store(maxWALBytes)
	if maxWALBytes > 0 {
		db.autoCheckpointSizeArmed.Store(true)
	}
	if interval <= 0 && idleInterval <= 0 && maxWALBytes <= 0 {
		return
	}
	if !db.autoCheckpointOn.CompareAndSwap(false, true) {
		return
	}
	db.wg.Add(1)
	go db.autoCheckpointLoop(interval, maxWALBytes, idleInterval)
}

// TriggerAutoCheckpoint schedules a best-effort immediate auto-checkpoint pass.
func (db *DB) TriggerAutoCheckpoint() {
	if db == nil || !db.autoCheckpointOn.Load() {
		return
	}
	select {
	case db.autoCheckpointOnceCh <- struct{}{}:
	default:
	}
}

func (db *DB) foregroundWriteQuietFor(now time.Time, quietWindow time.Duration) bool {
	if db == nil {
		return true
	}
	last := db.lastForegroundWriteUnixNano.Load()
	if last <= 0 {
		return true
	}
	return now.Sub(time.Unix(0, last)) >= quietWindow
}

func (db *DB) foregroundReadQuietFor(now time.Time, quietWindow time.Duration) bool {
	if db == nil {
		return true
	}
	if db.activeForegroundIterators.Load() > 0 {
		return false
	}
	last := db.lastForegroundReadUnixNano.Load()
	if last <= 0 {
		return true
	}
	return now.Sub(time.Unix(0, last)) >= quietWindow
}

func (db *DB) lastForegroundActivityUnixNano() int64 {
	if db == nil {
		return 0
	}
	lastWrite := db.lastForegroundWriteUnixNano.Load()
	lastRead := db.lastForegroundReadUnixNano.Load()
	if lastRead > lastWrite {
		return lastRead
	}
	return lastWrite
}

func (db *DB) foregroundActivityQuietFor(now time.Time, writeQuietWindow, readQuietWindow time.Duration) bool {
	if db == nil {
		return true
	}
	return db.foregroundWriteQuietFor(now, writeQuietWindow) && db.foregroundReadQuietFor(now, readQuietWindow)
}

func (db *DB) foregroundWriteQuiet(now time.Time) bool {
	return db.foregroundWriteQuietFor(now, vlogForegroundQuietWindow)
}

func (db *DB) waitForForegroundMaintenanceQuietWindow(quietWindow time.Duration) {
	if db == nil || quietWindow <= 0 {
		return
	}
	ticker := time.NewTicker(foregroundMaintenancePollInterval())
	defer ticker.Stop()
	for {
		if db.closing.Load() || db.foregroundActivityQuietFor(time.Now(), quietWindow, vlogForegroundReadQuietWindow) {
			return
		}
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
		}
	}
}

const (
	foregroundWriteResumeCheckInterval = 256
	foregroundWriteResumeCheckMask     = foregroundWriteResumeCheckInterval - 1
)

func foregroundMaintenancePollInterval() time.Duration {
	if interval := vlogGenerationLoopInterval / 10; interval > 0 {
		return interval
	}
	return time.Millisecond
}

func (db *DB) waitForForegroundMaintenanceQuiet() {
	db.waitForForegroundMaintenanceQuietWindow(vlogForegroundQuietWindow)
}

func (db *DB) foregroundActivityResumedSince(lastActivity int64) bool {
	if db == nil {
		return false
	}
	if db.activeForegroundIterators.Load() > 0 {
		return true
	}
	return db.lastForegroundActivityUnixNano() > lastActivity
}

func (db *DB) foregroundWriteResumeContext(lastWrite int64, timeout time.Duration) (context.Context, context.CancelFunc) {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	if db == nil {
		return ctx, cancel
	}
	if lastWrite <= 0 {
		return ctx, cancel
	}
	go func(lastWrite int64) {
		ticker := time.NewTicker(foregroundMaintenancePollInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-db.closeCh:
				cancel()
				return
			case <-ticker.C:
				if db.foregroundWritesResumedSince(lastWrite) {
					cancel()
					return
				}
			}
		}
	}(lastWrite)
	return ctx, cancel
}

func (db *DB) foregroundMaintenanceContextWithResumeGrace(timeout, resumeGrace time.Duration) (context.Context, context.CancelFunc) {
	if db == nil {
		if timeout > 0 {
			return context.WithTimeout(context.Background(), timeout)
		}
		return context.WithCancel(context.Background())
	}
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	lastActivity := db.lastForegroundActivityUnixNano()
	startedAt := time.Now()
	go func(lastActivity int64) {
		ticker := time.NewTicker(foregroundMaintenancePollInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-db.closeCh:
				cancel()
				return
			case <-ticker.C:
				if resumeGrace > 0 && time.Since(startedAt) < resumeGrace {
					continue
				}
				if db.foregroundActivityResumedSince(lastActivity) {
					cancel()
					return
				}
			}
		}
	}(lastActivity)
	return ctx, cancel
}

func (db *DB) foregroundMaintenanceContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return db.foregroundMaintenanceContextWithResumeGrace(timeout, 0)
}

func (db *DB) vlogGenerationMaintenanceContext(timeout time.Duration, opts vlogGenerationMaintenanceOptions) (context.Context, context.CancelFunc) {
	// Checkpoint-kick maintenance is an explicit caller opt-in to run outside the
	// quiet-window gate. Keep this context timeout-bounded, but do not
	// self-cancel on immediate foreground activity resumes.
	if opts.bypassQuiet {
		if timeout > 0 {
			return context.WithTimeout(context.Background(), timeout)
		}
		return context.WithCancel(context.Background())
	}
	return db.foregroundMaintenanceContext(timeout)
}

func (db *DB) vlogGenerationRewritePlanContext(timeout time.Duration, opts vlogGenerationMaintenanceOptions) (context.Context, context.CancelFunc) {
	if s := os.Getenv(envDebugVlogGenerationPlanTimeoutMS); s != "" {
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil && ms > 0 {
			timeout = time.Duration(ms) * time.Millisecond
		}
	}
	// Keep planner calls quiet-window-gated, but tolerate short foreground
	// activity resumes so sub-second plan scans can complete.
	if opts.bypassQuiet {
		if timeout > 0 {
			return context.WithTimeout(context.Background(), timeout)
		}
		return context.WithCancel(context.Background())
	}
	return db.foregroundMaintenanceContextWithResumeGrace(timeout, vlogGenerationRewritePlanResumeGrace)
}

func (db *DB) noteWrite() {
	if db == nil {
		return
	}
	db.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
	if !db.autoCheckpointOn.Load() {
		return
	}
	if db.disableJournal {
		return
	}
	const autoCheckpointWriteEveryBytes int64 = 1 << 20
	current := db.effectiveWALBytes()
	if current <= 0 {
		return
	}
	threshold := autoCheckpointWriteEveryBytes
	if max := db.autoCheckpointMaxWALBytes.Load(); max > 0 {
		// Rearm size-triggered checkpoints once WAL bytes drop substantially.
		if current < max/2 {
			db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
		} else if !db.autoCheckpointSizeArmed.Load() && db.walUsesValueLog() {
			reclaimable := db.reclaimableWALBytes()
			if reclaimable < max/2 {
				db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
			}
		}
		scaled := max / 4
		if scaled < 4*1024 {
			scaled = 4 * 1024
		}
		if scaled < threshold {
			threshold = scaled
		}
	}
	for {
		last := db.autoCheckpointLastWALBytes.Load()
		if current < last {
			if db.autoCheckpointLastWALBytes.CompareAndSwap(last, current) {
				return
			}
			continue
		}
		if current-last < threshold {
			return
		}
		if db.autoCheckpointLastWALBytes.CompareAndSwap(last, current) {
			break
		}
	}
	select {
	case db.autoCheckpointWriteCh <- struct{}{}:
	default:
	}
}

func (db *DB) noteRead() {
	if db == nil {
		return
	}
	now := time.Now().UnixNano()
	last := db.lastForegroundReadUnixNano.Load()
	if last > 0 && now-last < int64(foregroundReadStampMaxAge) {
		n := db.foregroundReadStampCounter.Add(1)
		if n != 1 && n%foregroundReadStampStride != 0 {
			return
		}
	}
	db.lastForegroundReadUnixNano.Store(now)
}

type autoCheckpointMode uint8

const (
	autoCheckpointModeInterval autoCheckpointMode = iota
	autoCheckpointModeIdle
	autoCheckpointModeSize
	autoCheckpointModeForce
)

const (
	autoCheckpointMinIdleWALBytesMin int64 = 1 << 20  // 1MiB
	autoCheckpointMinIdleWALBytesMax int64 = 32 << 20 // 32MiB
	autoCheckpointMinIdleInterval          = 10 * time.Second
	// Sample every 64 foreground reads to amortize time.Now() overhead while
	// keeping maintenance idle detection responsive during scan-heavy phases.
	foregroundReadStampStride = 64
	// Low-but-steady reads should still refresh the foreground timestamp often
	// enough to suppress background maintenance.
	foregroundReadStampMaxAge = 250 * time.Millisecond
)

func autoCheckpointReasonString(v uint32) string {
	switch autoCheckpointMode(v) {
	case autoCheckpointModeInterval:
		return "interval"
	case autoCheckpointModeIdle:
		return "idle"
	case autoCheckpointModeSize:
		return "size"
	case autoCheckpointModeForce:
		return "force"
	default:
		return "unknown"
	}
}

func vlogGenerationSchedulerStateString(v uint32) string {
	switch v {
	case vlogGenerationSchedulerDisabled:
		return "disabled"
	case vlogGenerationSchedulerIdle:
		return "idle"
	case vlogGenerationSchedulerRunning:
		return "running"
	case vlogGenerationSchedulerError:
		return "error"
	default:
		return "unknown"
	}
}

func vlogGenerationReasonString(v uint32) string {
	switch v {
	case vlogGenerationReasonNone:
		return "none"
	case vlogGenerationReasonTotalBytes:
		return "total_bytes"
	case vlogGenerationReasonStaleRatio:
		return "stale_ratio"
	case vlogGenerationReasonChurn:
		return "churn"
	case vlogGenerationReasonPeriodicGC:
		return "periodic_gc"
	case vlogGenerationReasonPostRewriteVacuum:
		return "post_rewrite_vacuum"
	case vlogGenerationReasonRewriteResume:
		return "rewrite_resume"
	default:
		return "unknown"
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func (db *DB) effectiveWALBytes() int64 {
	if db == nil {
		return 0
	}
	var total int64
	for i := range db.lanes {
		l := &db.lanes[i]
		total += l.walClosedBytes.Load() + l.walLiveBytes.Load()
	}
	return total
}

func (db *DB) reclaimableWALBytes() int64 {
	if db == nil {
		return 0
	}
	total := db.effectiveWALBytes()
	if total <= 0 {
		return 0
	}
	if !db.walUsesValueLog() {
		return total
	}
	_, retained := db.valueLogRetainedStats()
	if retained >= total {
		return 0
	}
	return total - retained
}

func (db *DB) minIdleCheckpointWALBytes() int64 {
	if db == nil {
		return autoCheckpointMinIdleWALBytesMin
	}
	db.mu.RLock()
	ft := db.flushThreshold
	db.mu.RUnlock()
	min := ft / 16
	if min < autoCheckpointMinIdleWALBytesMin {
		min = autoCheckpointMinIdleWALBytesMin
	}
	if min > autoCheckpointMinIdleWALBytesMax {
		min = autoCheckpointMinIdleWALBytesMax
	}
	return min
}

const (
	commitLogSegmentHeaderBytes = 8
	commitLogBatchHeaderBytes   = 1 + 4
	commitLogRecordHeaderBytes  = 1 + 2 + 4 + 8 + 8
)

func (db *DB) logRecordSize(key, value []byte) int64 {
	return int64(commitLogSegmentHeaderBytes + commitLogBatchHeaderBytes + commitLogRecordHeaderBytes + len(key) + len(value))
}

func (db *DB) logBatchSize(records []logRecord) int64 {
	if len(records) == 0 {
		return 0
	}
	total := commitLogSegmentHeaderBytes + commitLogBatchHeaderBytes
	for _, r := range records {
		total += commitLogRecordHeaderBytes + len(r.Key) + len(r.Value)
	}
	return int64(total)
}

func (db *DB) assignCommitSeq(records []logRecord) {
	if len(records) == 0 {
		return
	}
	seq := db.nextCommitSeq.Add(1)
	for i := range records {
		records[i].Seq = seq
	}
}

type domainIngressOp uint8

const (
	domainIngressOpSet domainIngressOp = iota + 1
	domainIngressOpDelete
)

type domainIngressRequest struct {
	op    domainIngressOp
	key   []byte
	value []byte
	sync  bool
	done  chan error
}

type walWriteRequest struct {
	records []logRecord
	sync    bool
	ack     *walAck
}

type walAck struct {
	wg  sync.WaitGroup
	err error
}

var walAckPool = sync.Pool{
	New: func() any { return &walAck{} },
}

type vlogWriteRequest struct {
	rid              uint64
	value            []byte
	dictID           uint64
	writeMode        vlogCompressionWriteMode
	blockCodec       valuelog.BlockCodec
	probeCompression bool
	durability       journalDurability
	enqueuedAt       time.Time
	ack              *vlogAck
}

type vlogAck struct {
	wg         sync.WaitGroup
	ptr        page.ValuePtr
	retainPath string
	err        error
}

var vlogAckPool = sync.Pool{
	New: func() any { return &vlogAck{} },
}

type vlogDictPrepareTask struct {
	fi             int
	dictID         uint64
	dict           []byte
	records        []valuelog.Record
	level          zstd.EncoderLevel
	enableEntropy  bool
	ioNsPerStored  float64
	encodeNsPerRaw float64
	safetyMargin   float64
	measureEncode  bool
	out            chan<- vlogDictPrepareResult
}

type vlogDictPrepareResult struct {
	fi      int
	body    []byte
	bodyBuf *vlogPreparedFrameBody
	stats   valuelog.FrameStats
	err     error
}

func (db *DB) publishVlogDictPrepareResult(task vlogDictPrepareTask, res vlogDictPrepareResult) {
	if task.out == nil {
		if res.bodyBuf != nil {
			putVlogPreparedFrameBody(res.bodyBuf)
		}
		return
	}
	select {
	case task.out <- res:
		return
	case <-db.closeCh:
		// During shutdown callers may stop receiving. Avoid blocking workers and
		// leaking pooled frame buffers in that case.
		select {
		case task.out <- res:
		default:
			if res.bodyBuf != nil {
				putVlogPreparedFrameBody(res.bodyBuf)
			}
		}
	}
}

const (
	maxEntryPoolCap              = 1 << 20
	maxEntryRunsPoolCap          = 1 << 14
	maxUnitRunsPoolCap           = 1 << 8
	maxOpMergeHeapCap            = 1 << 8
	entrySliceLeaseMinShift      = 4
	entrySliceLeaseMaxShift      = 20
	entrySliceLeaseClassCount    = entrySliceLeaseMaxShift - entrySliceLeaseMinShift + 1
	maxEntrySliceLeasesPerBucket = 128
)

var entrySlicePools [entrySliceLeaseClassCount]sync.Pool
var entryRunsPool sync.Pool
var unitRunsPool sync.Pool
var opMergeHeapPool sync.Pool
var entrySliceLeaseMu sync.Mutex
var entrySliceLeases [entrySliceLeaseClassCount][][]batch.Entry

// Entry slice pooling can retain very large backing arrays (up to maxEntryPoolCap).
// Track pooled bytes and enforce a byte-budget to cap retention after spikes.
var entrySlicePoolBytes atomic.Int64
var entrySlicePoolLastGC atomic.Uint64
var entrySlicePoolBudgetBytes int64 = computeEntrySlicePoolBudgetBytes()
var entrySliceEntrySizeBytes int64 = int64(unsafe.Sizeof(batch.Entry{}))

func computeEntrySlicePoolBudgetBytes() int64 {
	// Keep enough slices to amortize allocations without letting `sync.Pool` and
	// lease buckets accumulate unbounded heap during restore spikes.
	const perPBudgetBytes = int64(16 << 20) // 16MiB per P
	const minBudgetBytes = int64(64 << 20)
	const maxBudgetBytes = int64(512 << 20)
	procs := runtime.GOMAXPROCS(0)
	if procs < 1 {
		procs = 1
	}
	budget := int64(procs) * perPBudgetBytes
	if budget < minBudgetBytes {
		budget = minBudgetBytes
	}
	if budget > maxBudgetBytes {
		budget = maxBudgetBytes
	}
	return budget
}

func reserveEntrySlicePoolBytes(bytes int64) (ok, transitionedFromZero bool) {
	if bytes <= 0 {
		return true, false
	}
	budget := currentEntrySlicePoolBudgetBytes()
	if budget <= 0 {
		return false, false
	}
	for {
		held := entrySlicePoolBytes.Load()
		if held+bytes > budget {
			return false, false
		}
		if entrySlicePoolBytes.CompareAndSwap(held, held+bytes) {
			return true, held == 0
		}
	}
}

func releaseEntrySlicePoolBytes(bytes int64) {
	if bytes <= 0 {
		return
	}
	next := entrySlicePoolBytes.Add(-bytes)
	if next < 0 {
		// Counter can drift if sync.Pool drops objects at GC. Preserve any live
		// lease-accounted bytes instead of clamping all the way to zero.
		entrySlicePoolBytes.Store(entrySliceLeaseBytes())
	}
}

func noteEntrySlicePoolGC(numGC uint64) {
	if numGC == 0 {
		return
	}
	for {
		last := entrySlicePoolLastGC.Load()
		if last >= numGC {
			return
		}
		if entrySlicePoolLastGC.CompareAndSwap(last, numGC) {
			return
		}
	}
}

func entrySliceLeaseBytes() int64 {
	entrySliceLeaseMu.Lock()
	defer entrySliceLeaseMu.Unlock()

	var held int64
	for bucket := range entrySliceLeases {
		for _, entries := range entrySliceLeases[bucket] {
			if entries == nil {
				continue
			}
			held += int64(cap(entries)) * entrySliceEntrySizeBytes
		}
	}
	return held
}

func maybeResetEntrySlicePoolBytesAfterGC() {
	if entrySlicePoolBytes.Load() <= 0 {
		return
	}
	numGC := entrySlicePoolNumGC()
	last := entrySlicePoolLastGC.Load()
	if last == numGC {
		return
	}
	if last == 0 {
		entrySlicePoolLastGC.CompareAndSwap(0, numGC)
		return
	}
	if entrySlicePoolLastGC.CompareAndSwap(last, numGC) {
		entrySlicePoolBytes.Store(entrySliceLeaseBytes())
	}
}

func entrySliceLeaseClassForLen(capacity int) (idx int, classCap int, ok bool) {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxEntryPoolCap {
		return 0, 0, false
	}
	minCap := 1 << entrySliceLeaseMinShift
	if capacity <= minCap {
		return 0, minCap, true
	}
	classCap = 1 << uint(bits.Len(uint(capacity-1)))
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > maxEntryPoolCap {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - entrySliceLeaseMinShift
	if idx < 0 || idx >= entrySliceLeaseClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func entrySliceLeaseClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << entrySliceLeaseMinShift
	if capacity < minCap || capacity > maxEntryPoolCap {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - entrySliceLeaseMinShift
	if idx < 0 || idx >= entrySliceLeaseClassCount {
		return 0, false
	}
	return idx, true
}

func entrySliceMaxReuseCap(capacity int) int {
	if capacity <= 0 {
		return 1 << entrySliceLeaseMinShift
	}
	// Clamp before multiplication to avoid potential integer overflow.
	if capacity > maxEntryPoolCap/8 {
		return maxEntryPoolCap
	}
	maxCap := capacity * 8
	if maxCap < 1<<12 {
		maxCap = 1 << 12
	}
	if maxCap > maxEntryPoolCap {
		maxCap = maxEntryPoolCap
	}
	return maxCap
}

func getEntrySlice(capacity int) []batch.Entry {
	if capacity < 0 {
		capacity = 0
	}
	idx, classCap, ok := entrySliceLeaseClassForLen(capacity)
	if !ok {
		return make([]batch.Entry, 0, capacity)
	}
	maxReuseCap := entrySliceMaxReuseCap(capacity)
	maxIdx, _, maxOK := entrySliceLeaseClassForLen(maxReuseCap)
	if !maxOK {
		maxIdx = idx
	}
	entrySliceLeaseMu.Lock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		leases := entrySliceLeases[bucket]
		if n := len(leases); n > 0 {
			s := leases[n-1]
			entrySliceLeases[bucket][n-1] = nil
			entrySliceLeases[bucket] = leases[:n-1]
			entrySliceLeaseMu.Unlock()
			leaseBytes := int64(cap(s)) * entrySliceEntrySizeBytes
			releaseEntrySlicePoolBytes(leaseBytes)
			if cap(s) >= capacity {
				entrySliceLeaseHitTotal.Add(1)
				entrySliceLeaseHitBytesTotal.Add(uint64(leaseBytes))
				return s[:0]
			}
			if capIdx, ok := entrySliceLeaseClassForCap(cap(s)); ok {
				// The slice is too small for this request; return it to the pool if
				// we're within budget so smaller requests can reuse it.
				if ok, transitioned := reserveEntrySlicePoolBytes(leaseBytes); ok {
					if transitioned {
						noteEntrySlicePoolGC(entrySlicePoolNumGC())
					}
					entrySlicePools[capIdx].Put(s[:0])
				}
			}
			entrySliceFreshAllocTotal.Add(1)
			entrySliceFreshAllocBytesTotal.Add(uint64(classCap) * uint64(entrySliceEntrySizeBytes))
			return make([]batch.Entry, 0, classCap)
		}
	}
	entrySliceLeaseMu.Unlock()
	for bucket := idx; bucket <= maxIdx; bucket++ {
		if v := entrySlicePools[bucket].Get(); v != nil {
			s, ok := v.([]batch.Entry)
			if !ok {
				continue
			}
			poolBytes := int64(cap(s)) * entrySliceEntrySizeBytes
			releaseEntrySlicePoolBytes(poolBytes)
			if cap(s) >= capacity && cap(s) <= maxReuseCap {
				entrySlicePoolHitTotal.Add(1)
				entrySlicePoolHitBytesTotal.Add(uint64(poolBytes))
				return s[:0]
			}
		}
	}
	maybeResetEntrySlicePoolBytesAfterGC()
	entrySliceFreshAllocTotal.Add(1)
	entrySliceFreshAllocBytesTotal.Add(uint64(classCap) * uint64(entrySliceEntrySizeBytes))
	return make([]batch.Entry, 0, classCap)
}

func putEntrySlice(entries []batch.Entry) {
	if entries == nil {
		return
	}
	// Clear the full backing array on every path, including early returns. Batch
	// callers can hand us slices with len==0 but non-nil elements beyond len, and
	// leaving those hidden references intact can pin large heaps even when we do
	// not retain the slice in the pool.
	full := entries[:cap(entries)]
	clear(full)
	if cap(entries) > maxEntryPoolCap {
		return
	}
	idx, ok := entrySliceLeaseClassForCap(cap(entries))
	if !ok {
		return
	}
	leaseBytes := int64(cap(entries)) * entrySliceEntrySizeBytes
	ok, transitioned := reserveEntrySlicePoolBytes(leaseBytes)
	if !ok {
		entrySlicePutDropBudgetTotal.Add(1)
		entrySlicePutDropBudgetBytesTotal.Add(uint64(leaseBytes))
		return
	}
	if transitioned {
		noteEntrySlicePoolGC(entrySlicePoolNumGC())
	}
	entries = entries[:0]
	entrySliceLeaseMu.Lock()
	if len(entrySliceLeases[idx]) < maxEntrySliceLeasesPerBucket {
		entrySliceLeases[idx] = append(entrySliceLeases[idx], entries)
		entrySliceLeaseMu.Unlock()
		entrySlicePutLeaseTotal.Add(1)
		entrySlicePutLeaseBytesTotal.Add(uint64(leaseBytes))
		return
	}
	entrySliceLeaseMu.Unlock()
	entrySlicePools[idx].Put(entries)
	entrySlicePutPoolTotal.Add(1)
	entrySlicePutPoolBytesTotal.Add(uint64(leaseBytes))
}

func drainEntrySlicePoolsToTargetBytes(target int64) int64 {
	if target < 0 {
		target = 0
	}
	if entrySlicePoolBytes.Load() <= target {
		return 0
	}
	var dropped int64
	for classIdx := len(entrySlicePools) - 1; classIdx >= 0 && entrySlicePoolBytes.Load() > target; classIdx-- {
		for entrySlicePoolBytes.Load() > target {
			v := entrySlicePools[classIdx].Get()
			if v == nil {
				break
			}
			entries, ok := v.([]batch.Entry)
			if !ok {
				continue
			}
			size := int64(cap(entries)) * entrySliceEntrySizeBytes
			releaseEntrySlicePoolBytes(size)
			dropped += size
		}
	}
	return dropped
}

func getEntryRuns(capacity int) [][]batch.Entry {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxEntryRunsPoolCap {
		return make([][]batch.Entry, 0, capacity)
	}
	if v := entryRunsPool.Get(); v != nil {
		if runs, ok := v.([][]batch.Entry); ok && cap(runs) >= capacity {
			return runs[:0]
		}
	}
	return make([][]batch.Entry, 0, capacity)
}

func putEntryRuns(runs [][]batch.Entry) {
	if runs == nil || cap(runs) > maxEntryRunsPoolCap {
		return
	}
	clear(runs)
	entryRunsPool.Put(runs[:0])
}

func getUnitRuns(length int) [][][]batch.Entry {
	if length < 0 {
		length = 0
	}
	if length > maxUnitRunsPoolCap {
		return make([][][]batch.Entry, length)
	}
	if v := unitRunsPool.Get(); v != nil {
		if runs, ok := v.([][][]batch.Entry); ok && cap(runs) >= length {
			return runs[:length]
		}
	}
	return make([][][]batch.Entry, length)
}

func putUnitRuns(runs [][][]batch.Entry) {
	if runs == nil || cap(runs) > maxUnitRunsPoolCap {
		return
	}
	clear(runs)
	unitRunsPool.Put(runs[:0])
}

func getOpMergeHeap(capacity int) opMergeHeap {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxOpMergeHeapCap {
		return make(opMergeHeap, 0, capacity)
	}
	if v := opMergeHeapPool.Get(); v != nil {
		if h, ok := v.(opMergeHeap); ok && cap(h) >= capacity {
			return h[:0]
		}
	}
	return make(opMergeHeap, 0, capacity)
}

func putOpMergeHeap(h opMergeHeap) {
	if h == nil || cap(h) > maxOpMergeHeapCap {
		return
	}
	full := h[:cap(h)]
	clear(full)
	opMergeHeapPool.Put(full[:0])
}

func estimateUnitRunEntries(unitRuns [][][]batch.Entry, floor int) int {
	if floor < 0 {
		floor = 0
	}
	total := floor
	maxInt := int(^uint(0) >> 1)
	for i := range unitRuns {
		for _, run := range unitRuns[i] {
			if len(run) <= 0 {
				continue
			}
			if total > maxInt-len(run) {
				return maxInt
			}
			total += len(run)
		}
	}
	return total
}

func collectOpsInto(mem memtable.Table, dst []batch.Entry) (int, error) {
	if mem == nil {
		return 0, errors.New("cachingdb: nil memtable")
	}
	iter := mem.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()

	i := 0
	for iter.Valid() {
		if i >= len(dst) {
			return 0, fmt.Errorf("cachingdb: collectOpsInto overflow (have=%d need>=%d)", len(dst), i+1)
		}
		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			dst[i] = batch.Entry{
				Type: batch.OpDelete,
				Key:  iter.UnsafeKey(),
			}
		} else if flags&node.FlagPointer != 0 {
			dst[i] = batch.Entry{
				Type:     batch.OpPut,
				Key:      iter.UnsafeKey(),
				ValuePtr: ptr,
				IsPtr:    true,
			}
		} else {
			dst[i] = batch.Entry{
				Type:  batch.OpPut,
				Key:   iter.UnsafeKey(),
				Value: val,
			}
		}
		i++
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	return i, nil
}

type opRunIter struct {
	runs   [][]batch.Entry
	runIdx int
	idx    int
	valid  bool
}

func newOpRunIter(runs [][]batch.Entry) *opRunIter {
	it := &opRunIter{runs: runs}
	it.advanceToValid()
	return it
}

func (it *opRunIter) advanceToValid() {
	for it.runIdx < len(it.runs) {
		if it.idx < len(it.runs[it.runIdx]) {
			it.valid = true
			return
		}
		it.runIdx++
		it.idx = 0
	}
	it.valid = false
}

func (it *opRunIter) Valid() bool {
	return it.valid
}

func (it *opRunIter) Next() {
	if !it.valid {
		return
	}
	it.idx++
	it.advanceToValid()
}

func (it *opRunIter) Entry() batch.Entry {
	if !it.valid {
		return batch.Entry{}
	}
	return it.runs[it.runIdx][it.idx]
}

func (it *opRunIter) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.runs[it.runIdx][it.idx].Key
}

type opMergeItem struct {
	iter     *opRunIter
	priority int
	key      []byte
}

type opMergeHeap []opMergeItem

func (h opMergeHeap) Len() int { return len(h) }

func (h opMergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h opMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *opMergeHeap) push(x opMergeItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *opMergeHeap) pop() opMergeItem {
	old := *h
	n := len(old)
	if n == 0 {
		return opMergeItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h opMergeHeap) peek() *opMergeItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *opMergeHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *opMergeHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

func buildOpRuns(mem memtable.Table, chunkCap int) ([][]batch.Entry, int, error) {
	if mem == nil {
		return nil, 0, errors.New("cachingdb: nil memtable")
	}
	if chunkCap <= 0 {
		chunkCap = 8192
	}
	iter := mem.NewIterator(nil, nil)
	runsCap := mem.Len()/chunkCap + 1
	runs := getEntryRuns(runsCap)
	deleteOps := 0
	ops := getEntrySlice(chunkCap)
	ops = ops[:0]
	stableUnsafe := false
	if stable, ok := mem.(memtable.StableUnsafeIteratorTable); ok {
		stableUnsafe = stable.StableUnsafeIteratorSlices()
	}
	for iter.Valid() {
		val, ptr, flags := iter.UnsafeEntry()
		key := iter.UnsafeKey()
		if !stableUnsafe {
			key = append([]byte(nil), key...)
		}
		if flags&node.FlagTombstone != 0 {
			ops = append(ops, batch.Entry{Type: batch.OpDelete, Key: key})
			deleteOps++
		} else if flags&node.FlagPointer != 0 {
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true})
		} else {
			if !stableUnsafe && val != nil {
				val = append([]byte(nil), val...)
			}
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, Value: val})
		}
		iter.Next()
		if len(ops) >= cap(ops) {
			runs = append(runs, ops)
			ops = getEntrySlice(chunkCap)
			ops = ops[:0]
		}
	}
	err := iter.Error()
	cerr := iter.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		putEntrySlice(ops)
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
		return nil, 0, err
	}
	if len(ops) > 0 {
		runs = append(runs, ops)
	} else {
		putEntrySlice(ops)
	}
	return runs, deleteOps, nil
}

type walFastItem struct {
	record logRecord
	ack    *walAck
}

const (
	walWriteBuffer                = 4096
	walWriteBatchMax              = 512
	walFastBatchMax               = 2048
	walFastQueueMax               = 16384
	defaultDomainIngressQueueSize = 1024
	domainIngressBatchMax         = 128
)

const (
	vlogWriteBuffer    = 4096
	vlogWriteBatchMax  = 512
	vlogDictPrepBuffer = 1024
)

// Always queue values at or above this size to avoid blocking callers on large
// appends and to improve value-log batching efficiency.
const vlogQueueMinValueSize = 1 << 10

// Linger briefly to coalesce micro-batches for small/medium queued writes.
const vlogWriteLinger = 75 * time.Microsecond

func defaultJournalLaneCount(procs int) int {
	if procs <= 2 {
		return 1
	}
	// Keep defaults conservative on low/mid core hosts.
	lanes := procs / 4
	if lanes < 1 {
		lanes = 1
	}
	// On high-core hosts, increase lane fanout to unlock journal/value-log
	// parallelism, but avoid the most aggressive split to limit queue overhead.
	if procs >= 16 {
		highCoreLanes := (procs * 3) / 8
		if highCoreLanes > lanes {
			lanes = highCoreLanes
		}
	}
	if lanes > 8 {
		lanes = 8
	}
	return lanes
}

func (db *DB) startDomainIngressWorkers() {
	if db == nil || db.domainIngressWorkers <= 0 {
		return
	}
	queueSize := db.domainIngressQueueSize
	if queueSize <= 0 {
		queueSize = defaultDomainIngressQueueSize
	}
	db.domainIngressMu.Lock()
	defer db.domainIngressMu.Unlock()
	if len(db.domainIngressCh) > 0 {
		return
	}
	workers := db.domainIngressWorkers
	if workers < 1 {
		return
	}
	db.domainIngressQueueSize = queueSize
	db.domainIngressCh = make([]chan domainIngressRequest, workers)
	for workerID := 0; workerID < workers; workerID++ {
		ch := make(chan domainIngressRequest, queueSize)
		db.domainIngressCh[workerID] = ch
		db.wg.Add(1)
		go db.domainIngressLoop(ch)
	}
}

func (db *DB) stopDomainIngressWorkers() {
	if db == nil {
		return
	}
	db.domainIngressMu.Lock()
	queues := db.domainIngressCh
	db.domainIngressCh = nil
	db.domainIngressMu.Unlock()
	for _, ch := range queues {
		close(ch)
	}
}

func (db *DB) domainIngressLoop(ch <-chan domainIngressRequest) {
	defer db.wg.Done()
	batchReqs := make([]domainIngressRequest, 0, domainIngressBatchMax)
	for {
		req, ok := <-ch
		if !ok {
			return
		}
		batchReqs = append(batchReqs[:0], req)
	drain:
		for len(batchReqs) < domainIngressBatchMax {
			select {
			case req, ok = <-ch:
				if !ok {
					db.processDomainIngressBatch(batchReqs)
					return
				}
				batchReqs = append(batchReqs, req)
			default:
				break drain
			}
		}
		db.processDomainIngressBatch(batchReqs)
	}
}

func (db *DB) processDomainIngressBatch(reqs []domainIngressRequest) {
	if len(reqs) == 0 {
		return
	}
	if len(reqs) == 1 {
		req := reqs[0]
		var err error
		switch req.op {
		case domainIngressOpSet:
			err = db.setDirect(req.key, req.value, false)
		case domainIngressOpDelete:
			err = db.deleteDirect(req.key, false)
		default:
			err = fmt.Errorf("cachingdb: unknown ingress op %d", req.op)
		}
		db.domainIngressProcessed.Add(1)
		if req.done != nil {
			req.done <- err
			close(req.done)
		}
		return
	}

	b := db.NewBatchWithSize(len(reqs))
	var err error
	for i := range reqs {
		req := reqs[i]
		switch req.op {
		case domainIngressOpSet:
			err = b.Set(req.key, req.value)
		case domainIngressOpDelete:
			err = b.Delete(req.key)
		default:
			err = fmt.Errorf("cachingdb: unknown ingress op %d", req.op)
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = b.Write()
	}
	if closeErr := b.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	db.domainIngressProcessed.Add(uint64(len(reqs)))
	for i := range reqs {
		req := reqs[i]
		if req.done != nil {
			req.done <- err
			close(req.done)
		}
	}
}

func (db *DB) observeDomainIngressDepth(depth int) {
	if depth < 0 {
		return
	}
	depthU := uint64(depth)
	for {
		prev := db.domainIngressDepthMax.Load()
		if depthU <= prev {
			return
		}
		if db.domainIngressDepthMax.CompareAndSwap(prev, depthU) {
			return
		}
	}
}

func (db *DB) enqueueDomainIngress(op domainIngressOp, key, value []byte, sync bool) (bool, error) {
	if db == nil {
		return false, nil
	}
	if db.domainIngressWorkers <= 0 {
		return false, nil
	}
	// Preserve legacy sync behavior until ingress batching has explicit sync-fence
	// handling and per-request durable completion accounting.
	if sync {
		return false, nil
	}
	if db.closing.Load() {
		return true, errDBClosing
	}

	db.domainIngressMu.Lock()
	if len(db.domainIngressCh) == 0 {
		db.domainIngressMu.Unlock()
		return false, nil
	}
	req := domainIngressRequest{
		op:    op,
		key:   key,
		value: value,
		sync:  sync,
		done:  make(chan error, 1),
	}
	shardID := db.shardIndex(key)
	workerID := shardID % len(db.domainIngressCh)
	ch := db.domainIngressCh[workerID]
	select {
	case ch <- req:
		db.domainIngressEnqueued.Add(1)
		db.observeDomainIngressDepth(len(ch))
		db.domainIngressMu.Unlock()
	default:
		db.domainIngressFallback.Add(1)
		db.domainIngressMu.Unlock()
		return false, nil
	}

	err, ok := <-req.done
	if !ok {
		return true, errDBClosing
	}
	return true, err
}

func (db *DB) startWALWriter(l *lane) {
	if l == nil {
		return
	}
	l.walCh = make(chan walWriteRequest, walWriteBuffer)
	l.walFastCond = sync.NewCond(&l.walFastMu)
	db.wg.Add(1)
	go db.walWriteLoop(l)
	db.wg.Add(1)
	go db.walFastLoop(l)
}

func (db *DB) startVlogWriter(l *lane) {
	if l == nil {
		return
	}
	l.vlogCh = make(chan vlogWriteRequest, vlogWriteBuffer)
	l.vlogDictBytes = make(map[uint64][]byte)
	db.startVlogDictPreparer(l)
	workers := db.vlogWriteWorkerCount()
	l.vlogWorkers = workers
	for i := 0; i < workers; i++ {
		db.wg.Add(1)
		go db.vlogWriteLoop(l)
	}
}

func (db *DB) vlogWriteWorkerCount() int {
	procs := runtime.GOMAXPROCS(0)
	if procs <= 1 {
		return 1
	}
	lanes := len(db.lanes)
	if lanes < 1 {
		lanes = 1
	}
	workers := procs / lanes
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	return workers
}

func (db *DB) vlogDictPrepWorkerCount() int {
	procs := runtime.GOMAXPROCS(0)
	if procs <= 1 {
		return 0
	}
	lanes := len(db.lanes)
	if lanes < 1 {
		lanes = 1
	}
	workers := procs / lanes
	if workers < 2 {
		workers = 2
	}
	if workers > procs {
		workers = procs
	}
	if workers < 2 {
		return 0
	}
	return workers
}

func (db *DB) startVlogDictPreparer(l *lane) {
	if l == nil {
		return
	}
	maxWorkers := db.vlogDictPrepWorkerCount()
	if maxWorkers <= 1 {
		return
	}
	l.vlogPrepWorkers = 0
	l.vlogPrepMaxWorkers = maxWorkers
	l.vlogPrepCh = make(chan vlogDictPrepareTask, vlogDictPrepBuffer)
}

func (db *DB) ensureVlogDictPrepWorkers(l *lane, wanted int) {
	if l == nil || l.vlogPrepCh == nil || l.vlogPrepMaxWorkers <= 1 {
		return
	}
	if wanted <= 0 {
		wanted = 1
	}
	if wanted > l.vlogPrepMaxWorkers {
		wanted = l.vlogPrepMaxWorkers
	}
	l.vlogPrepMu.Lock()
	start := l.vlogPrepWorkers
	if start >= wanted {
		l.vlogPrepMu.Unlock()
		return
	}
	l.vlogPrepWorkers = wanted
	l.vlogPrepMu.Unlock()
	for i := start; i < wanted; i++ {
		db.wg.Add(1)
		go db.vlogDictPrepareLoop(l)
	}
}

func (db *DB) vlogDictPrepareLoop(l *lane) {
	defer db.wg.Done()
	if l == nil || l.vlogPrepCh == nil {
		return
	}
	preparer := valuelog.NewFramePreparer()
	processTask := func(task vlogDictPrepareTask) {
		preparer.SetDictFrameEncoderOptions(task.level, task.enableEntropy)
		preparer.SetKeepPolicy(task.ioNsPerStored, task.encodeNsPerRaw, task.safetyMargin)
		if task.measureEncode {
			preparer.SetEncodeSampleStride(1)
		} else {
			preparer.SetEncodeSampleStride(0)
		}
		bodyBuf := getVlogPreparedFrameBody()
		body, stats, err := preparer.PrepareFrameInto(bodyBuf.buf[:0], task.dictID, task.dict, task.records)
		if err != nil {
			putVlogPreparedFrameBody(bodyBuf)
			db.publishVlogDictPrepareResult(task, vlogDictPrepareResult{
				fi:  task.fi,
				err: err,
			})
			return
		}
		bodyBuf.buf = body
		db.publishVlogDictPrepareResult(task, vlogDictPrepareResult{
			fi:      task.fi,
			body:    body,
			bodyBuf: bodyBuf,
			stats:   stats,
		})
	}
	for {
		// Prefer queued work, even during close, so enqueued tasks are not stranded.
		select {
		case task := <-l.vlogPrepCh:
			processTask(task)
			continue
		default:
		}
		select {
		case task := <-l.vlogPrepCh:
			processTask(task)
		case <-db.closeCh:
			for {
				select {
				case task := <-l.vlogPrepCh:
					db.publishVlogDictPrepareResult(task, vlogDictPrepareResult{
						fi:  task.fi,
						err: errWALClosed,
					})
				default:
					return
				}
			}
		}
	}
}

func (db *DB) walWriteLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]walWriteRequest, 0, walWriteBatchMax)
	for {
		batch = batch[:0]

		var req walWriteRequest
		select {
		case <-db.closeCh:
			db.drainWALWriter(l, batch)
			return
		case req = <-l.walCh:
		}
		batch = append(batch, req)

	drain:
		for len(batch) < walWriteBatchMax {
			select {
			case req = <-l.walCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		db.walAckMu.Lock()
		walErr := db.walErr
		db.walAckMu.Unlock()
		if walErr != nil {
			db.finishWALRequests(batch, walErr)
			continue
		}

		err := db.flushWALRequests(l, batch)
		if err != nil {
			db.walAckMu.Lock()
			if db.walErr == nil {
				db.walErr = err
			}
			walErr = db.walErr
			db.walAckMu.Unlock()
			db.finishWALRequests(batch, walErr)
			continue
		}

		db.finishWALRequests(batch, nil)
	}
}

func (db *DB) vlogWriteLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]vlogWriteRequest, 0, vlogWriteBatchMax)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()
	for {
		batch = batch[:0]

		var req vlogWriteRequest
		select {
		case <-db.closeCh:
			db.drainVlogWriter(l, batch)
			return
		case req = <-l.vlogCh:
		}
		batch = append(batch, req)
		backlog := len(l.vlogCh)
		observeLaneVlogQueueDepthSample(l, backlog)
		lingerAllowed := backlog < (vlogWriteBatchMax/4) && !l.vlogQueueing.Load()
		if len(batch) < vlogWriteBatchMax && len(req.value) < vlogQueueMinValueSize && lingerAllowed {
			timer.Reset(vlogWriteLinger)
			lingerDone := false
			for len(batch) < vlogWriteBatchMax && !lingerDone {
				select {
				case req = <-l.vlogCh:
					batch = append(batch, req)
				case <-timer.C:
					lingerDone = true
				case <-db.closeCh:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					db.drainVlogWriter(l, batch)
					return
				}
			}
			if !timer.Stop() && !lingerDone {
				select {
				case <-timer.C:
				default:
				}
			}
		}

	drain:
		for len(batch) < vlogWriteBatchMax {
			select {
			case req = <-l.vlogCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}
		observeLaneVlogQueueDepthSample(l, len(l.vlogCh))

		db.flushVlogRequests(l, batch)
		if len(l.vlogCh) == 0 {
			l.vlogQueueing.Store(false)
		}
	}
}

func (db *DB) walFastLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]walFastItem, 0, walFastBatchMax)
	records := make([]logRecord, 0, walFastBatchMax)

	for {
		l.walFastMu.Lock()
		for !l.walFastClosed && len(l.walFastQueue)-l.walFastHead == 0 {
			l.walFastCond.Wait()
		}

		if l.walFastClosed {
			batch = append(batch[:0], l.walFastQueue[l.walFastHead:]...)
			l.walFastQueue = nil
			l.walFastHead = 0
			l.walFastMu.Unlock()

			for i := range batch {
				ack := batch[i].ack
				ack.err = errWALClosed
				ack.wg.Done()
			}
			return
		}

		available := len(l.walFastQueue) - l.walFastHead
		n := available
		if n > walFastBatchMax {
			n = walFastBatchMax
		}
		batch = append(batch[:0], l.walFastQueue[l.walFastHead:l.walFastHead+n]...)
		l.walFastHead += n

		if l.walFastHead == len(l.walFastQueue) {
			l.walFastQueue = l.walFastQueue[:0]
			l.walFastHead = 0
		} else if l.walFastHead > 1024 && l.walFastHead*2 >= len(l.walFastQueue) {
			copy(l.walFastQueue, l.walFastQueue[l.walFastHead:])
			l.walFastQueue = l.walFastQueue[:len(l.walFastQueue)-l.walFastHead]
			l.walFastHead = 0
		}
		l.walFastCond.Broadcast()
		l.walFastMu.Unlock()

		records = records[:0]
		for i := range batch {
			records = append(records, batch[i].record)
		}
		err := db.appendWALDirect(l, records, false)
		for i := range batch {
			ack := batch[i].ack
			ack.err = err
			ack.wg.Done()
		}
	}
}

func (db *DB) drainWALWriter(l *lane, batch []walWriteRequest) {
	for {
		select {
		case req := <-l.walCh:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < walWriteBatchMax {
				select {
				case req = <-l.walCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			db.walAckMu.Lock()
			walErr := db.walErr
			db.walAckMu.Unlock()
			if walErr != nil {
				db.finishWALRequests(batch, walErr)
				continue
			}

			err := db.flushWALRequests(l, batch)
			if err != nil {
				db.walAckMu.Lock()
				if db.walErr == nil {
					db.walErr = err
				}
				walErr = db.walErr
				db.walAckMu.Unlock()
				db.finishWALRequests(batch, walErr)
				continue
			}
			db.finishWALRequests(batch, nil)
		default:
			return
		}
	}
}

func (db *DB) drainVlogWriter(l *lane, batch []vlogWriteRequest) {
	for {
		select {
		case req := <-l.vlogCh:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < vlogWriteBatchMax {
				select {
				case req = <-l.vlogCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			observeLaneVlogQueueDepthSample(l, len(l.vlogCh))
			db.flushVlogRequests(l, batch)
		default:
			return
		}
	}
}

func (db *DB) finishWALRequests(requests []walWriteRequest, err error) {
	for i := range requests {
		ack := requests[i].ack
		if ack == nil {
			continue
		}
		ack.err = err
		ack.wg.Done()
	}
}

func (db *DB) flushWALRequests(l *lane, requests []walWriteRequest) error {
	if len(requests) == 0 {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}

	var (
		totalBytes int64
		needSync   bool
	)

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	for i := range requests {
		req := &requests[i]
		if len(req.records) == 1 {
			rec := req.records[0]
			err := w.Append(rec)
			if err != nil {
				l.walMu.Unlock()
				return err
			}
			totalBytes += db.logRecordSize(rec.Key, rec.Value)
		} else {
			err := w.AppendBatch(req.records)
			if err != nil {
				l.walMu.Unlock()
				return err
			}
			totalBytes += db.logBatchSize(req.records)
		}
		if req.sync {
			needSync = true
		}
	}
	if needSync {
		if err := w.Sync(); err != nil {
			l.walMu.Unlock()
			return err
		}
	}
	l.walMu.Unlock()

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) flushVlogRequests(l *lane, requests []vlogWriteRequest) {
	if len(requests) == 0 {
		return
	}
	if l == nil {
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = errWALUnavailable
			ack.wg.Done()
		}
		return
	}

	var (
		needFlush       bool
		needSync        bool
		rawPayloadBytes int
		singleDictID    uint64
		sawDictID       bool
		multipleDictIDs bool
	)
	records := getValueLogRecordsCap(len(requests))
	records = records[:len(requests)]
	defer putValueLogRecordsNoClear(records)
	for i := range requests {
		req := &requests[i]
		if !req.enqueuedAt.IsZero() {
			observeLaneVlogQueueLag(l, time.Since(req.enqueuedAt))
		}
		records[i] = valuelog.Record{RID: req.rid, Value: req.value}
		if req.durability == journalDurabilitySync {
			needSync = true
		} else if req.durability == journalDurabilityFlush {
			needFlush = true
		}
		if req.dictID != 0 {
			if !sawDictID {
				sawDictID = true
				singleDictID = req.dictID
			} else if req.dictID != singleDictID {
				multipleDictIDs = true
			}
		}
		rawPayloadBytes += len(req.value)
	}

	var (
		singleDict []byte
		dictByID   map[uint64][]byte
	)
	if sawDictID {
		if !multipleDictIDs {
			dictBytes, err := db.dictBytesForLane(context.Background(), l, singleDictID)
			if err == nil && len(dictBytes) > 0 {
				singleDict = dictBytes
			}
		} else {
			dictNeeded := make(map[uint64]struct{})
			for i := range requests {
				if dictID := requests[i].dictID; dictID != 0 {
					dictNeeded[dictID] = struct{}{}
				}
			}
			dictByID = make(map[uint64][]byte, len(dictNeeded))
			for dictID := range dictNeeded {
				dictBytes, err := db.dictBytesForLane(context.Background(), l, dictID)
				if err == nil && len(dictBytes) > 0 {
					dictByID[dictID] = dictBytes
				}
			}
		}
	}

	type vlogBatchPlan struct {
		start       int
		end         int
		writeMode   vlogCompressionWriteMode
		blockCodec  valuelog.BlockCodec
		dictID      uint64
		dict        []byte
		k           int
		probe       bool
		rawBytes    int
		frames      []preparedDictFrame
		storedBytes int
		wallNs      int64
	}
	rawPaused := db.valueLogDictPauseRemaining.Load() > 0
	var planScratch [16]vlogBatchPlan
	plans := planScratch[:0]
	if len(requests) > len(planScratch) {
		plans = make([]vlogBatchPlan, 0, len(requests))
	}
	for i := 0; i < len(requests); {
		writeMode := requests[i].writeMode
		blockCodec := normalizeSelectorBlockCodec(requests[i].blockCodec)
		dictID := requests[i].dictID
		if writeMode != vlogWriteDict {
			dictID = 0
		}
		dict := lookupVlogDictBytes(dictID, singleDictID, singleDict, dictByID)
		if dictID == 0 || len(dict) == 0 || writeMode != vlogWriteDict {
			dictID = 0
			dict = nil
			if writeMode == vlogWriteDict {
				writeMode = vlogWriteOff
			}
		}
		probe := requests[i].probeCompression
		maxValLen := len(requests[i].value)
		rawBytes := len(requests[i].value)
		end := i + 1
		for end < len(requests) {
			nextMode := requests[end].writeMode
			nextCodec := normalizeSelectorBlockCodec(requests[end].blockCodec)
			nextDictID := requests[end].dictID
			nextDict := lookupVlogDictBytes(nextDictID, singleDictID, singleDict, dictByID)
			if nextMode != vlogWriteDict {
				nextDictID = 0
			}
			if nextDictID == 0 || len(nextDict) == 0 || nextMode != vlogWriteDict {
				nextDictID = 0
			}
			if nextMode == vlogWriteDict && nextDictID == 0 {
				nextMode = vlogWriteOff
			}
			if nextMode != writeMode {
				break
			}
			if nextMode == vlogWriteBlock && nextCodec != blockCodec {
				break
			}
			if nextDictID != dictID {
				break
			}
			probe = probe || requests[end].probeCompression
			if n := len(requests[end].value); n > maxValLen {
				maxValLen = n
			}
			rawBytes += len(requests[end].value)
			end++
		}

		k := 1
		if end-i > 1 {
			if writeMode == vlogWriteDict && dictID != 0 {
				k = db.valueLogDictK(dictID)
				k = db.chooseValueLogDictWriteK(k, end-i, rawBytes)
				if db.disableJournal {
					k = valuelog.MaxFrameK
				}
			} else if writeMode == vlogWriteBlock {
				k = db.chooseValueLogBlockWriteK(l, end-i, rawBytes, blockCodec)
			} else if rawPaused && db.disableJournal {
				k = valuelog.MaxFrameK
			} else if cur := int(db.valueLogDictCurrentK.Load()); cur > 1 {
				k = cur
			} else {
				k = 8
				if db.disableJournal && db.forceValueLogPointers {
					k = 16
				}
			}
		}
		if limits.MaxRecordSize > 0 && maxValLen > 0 {
			maxKBySize := int(limits.MaxRecordSize) / maxValLen
			if maxKBySize < 1 {
				maxKBySize = 1
			}
			if k > maxKBySize {
				k = maxKBySize
			}
		}
		k = db.clampValueLogDictK(k)

		plans = append(plans, vlogBatchPlan{
			start:      i,
			end:        end,
			writeMode:  writeMode,
			blockCodec: blockCodec,
			dictID:     dictID,
			dict:       dict,
			k:          k,
			probe:      probe,
			rawBytes:   rawBytes,
		})
		i = end
	}

	ioNsPerStored, encodeNsPerRaw, safetyMargin := db.valueLogKeepPolicy()
	preparedErr := error(nil)
	for pi := range plans {
		plan := &plans[pi]
		if plan.writeMode != vlogWriteDict || plan.dictID == 0 || len(plan.dict) == 0 {
			continue
		}
		keepIoNs := ioNsPerStored
		keepEncodeNs := encodeNsPerRaw
		if plan.probe {
			// Probe writes should always attempt dict compression.
			keepIoNs = 0
			keepEncodeNs = 0
		}
		prepared, _, prepErr := db.prepareAppendDictFrames(
			l,
			plan.dictID,
			plan.dict,
			records[plan.start:plan.end],
			plan.k,
			plan.rawBytes,
			keepIoNs,
			keepEncodeNs,
			safetyMargin,
			time.Time{},
		)
		if prepErr != nil {
			preparedErr = prepErr
			break
		}
		plan.frames = prepared
		for fi := range plan.frames {
			plan.frames[fi].start += plan.start
			plan.frames[fi].end += plan.start
		}
	}
	if preparedErr != nil {
		for i := range plans {
			releasePreparedDictFrames(plans[i].frames)
			putVlogPreparedFrames(plans[i].frames)
			plans[i].frames = nil
		}
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = preparedErr
			ack.wg.Done()
		}
		return
	}
	for i := range plans {
		if len(plans[i].frames) == 0 {
			continue
		}
		defer func(pi int) {
			releasePreparedDictFrames(plans[pi].frames)
			putVlogPreparedFrames(plans[pi].frames)
			plans[pi].frames = nil
		}(i)
	}

	var (
		ptrs        []page.ValuePtr
		startSize   int64
		totalBytes  int64
		framesTotal int
		framesTried int
		framesKept  int
		retainPath  string
		probeKept   bool
		dictRaw     int
		dictStored  int
		dictRecords int
		err         error
	)

	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = errWALUnavailable
			ack.wg.Done()
		}
		return
	}
	firstPath := l.vlogPath
	var retainPaths []string
	noteRotatePath := func(path string) {
		if path == "" {
			return
		}
		if retainPaths == nil {
			if firstPath != "" {
				retainPaths = append(retainPaths, firstPath)
			} else {
				retainPaths = make([]string, 0, 2)
			}
		}
		if len(retainPaths) == 0 || retainPaths[len(retainPaths)-1] != path {
			retainPaths = append(retainPaths, path)
		}
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = rotateErr
			ack.wg.Done()
		}
		return
	}
	// Rotation may replace the writer; reload it so subsequent appends use the
	// correct segment.
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = errWALUnavailable
			ack.wg.Done()
		}
		return
	}
	if maxBytes := db.valueLogMaxSegmentBytesForLane(l); maxBytes > 0 {
		// Pre-rotate to ensure this batch never produces pointers with offsets
		// outside the packed-offset cap.
		est := int64(rawPayloadBytes) + int64(len(records))*64
		if est < 0 {
			est = 0
		}
		if est > 0 && w.Size() > maxBytes-est {
			if rotateErr := db.rotateValueLogMuHeld(l); rotateErr != nil {
				l.vlogMu.Unlock()
				for i := range requests {
					ack := requests[i].ack
					if ack == nil {
						continue
					}
					ack.ptr = page.ValuePtr{}
					ack.retainPath = ""
					ack.err = rotateErr
					ack.wg.Done()
				}
				return
			}
			// Reload writer after rotation to ensure subsequent operations use the
			// new segment.
			w = l.vlog
			if w == nil {
				l.vlogMu.Unlock()
				for i := range requests {
					ack := requests[i].ack
					if ack == nil {
						continue
					}
					ack.ptr = page.ValuePtr{}
					ack.retainPath = ""
					ack.err = errWALUnavailable
					ack.wg.Done()
				}
				return
			}
		}
	}
	if l.vlogPath != firstPath {
		noteRotatePath(l.vlogPath)
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	caps := l.vlogCaps
	rawWriterInto := caps.rawInto
	rawBufferedInto := caps.rawBuf
	policySetter := caps.keep
	preparedAppender := caps.prepared
	startSize = w.Size()

	baseIoNsPerStored := 0.0
	baseEncodeNsPerRaw := 0.0
	keepSafetyMargin := db.valueLogAutotuneSafetyMargin()
	if policySetter != nil {
		snap := db.valueLogAutotuneMetrics.snapshot()
		baseIoNsPerStored = snap.IoNsPerStoredByte
		baseEncodeNsPerRaw = snap.EncodeNsPerRawByte
		if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
			baseIoNsPerStored = 0
			baseEncodeNsPerRaw = 0
		}
	}
	autoSelectorMode := normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionAuto

	ptrs = getValueLogPtrsCap(len(records))
	ptrs = ptrs[:len(records)]
	defer putValueLogPtrsNoClear(ptrs)
	statsWriter := caps.stats
	statsWriterInto := caps.statsInto
	for pi := range plans {
		plan := &plans[pi]
		if err != nil {
			break
		}
		if policySetter != nil {
			ioNsPerStored := baseIoNsPerStored
			encodeNsPerRaw := baseEncodeNsPerRaw
			// In auto mode, block candidate evaluation must observe real compressed
			// output (not keep-policy short-circuits), same as explicit probes.
			if plan.probe || (autoSelectorMode && plan.writeMode == vlogWriteBlock) {
				ioNsPerStored = 0
				encodeNsPerRaw = 0
			}
			policySetter.SetKeepPolicy(ioNsPerStored, encodeNsPerRaw, keepSafetyMargin)
		}
		db.setVlogWriterMode(l, w, plan.writeMode, plan.blockCodec)
		planStart := time.Now()
		beforePlanSize := w.Size()
		planStoredBytes := 0
		segment := records[plan.start:plan.end]
		if plan.writeMode != vlogWriteDict || plan.dictID == 0 {
			if len(segment) == 1 {
				ptrs[plan.start], err = w.Append(0, nil, segment[0].RID, segment[0].Value)
				if err == nil {
					framesTotal++
				}
			} else if plan.writeMode == vlogWriteOff && (rawWriterInto != nil || rawBufferedInto != nil) {
				useBufferedRaw := rawBufferedInto != nil
				var (
					stats    valuelog.FrameStats
					batchErr error
				)
				if useBufferedRaw {
					_, stats, batchErr = rawBufferedInto.AppendRawFramesBufferedInto(segment, plan.k, ptrs[plan.start:plan.end])
				} else if rawWriterInto != nil {
					_, stats, batchErr = rawWriterInto.AppendRawFramesWritevInto(segment, plan.k, ptrs[plan.start:plan.end])
				} else {
					batchErr = errors.New("cachingdb: raw grouped append writer unavailable")
				}
				err = batchErr
				if err == nil && plan.k > 0 {
					framesTotal += (len(segment) + plan.k - 1) / plan.k
					planStoredBytes += stats.StoredPayloadBytes
				}
			} else {
				for i := plan.start; i < plan.end; i += plan.k {
					end := i + plan.k
					if end > plan.end {
						end = plan.end
					}
					frame := records[i:end]
					if statsWriterInto != nil {
						dst := ptrs[i:end]
						_, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(0, nil, frame, dst)
						if frameErr != nil {
							err = frameErr
							break
						}
						framesTotal++
						planStoredBytes += stats.StoredPayloadBytes
						continue
					}
					if statsWriter != nil {
						framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(0, nil, frame)
						if frameErr != nil {
							err = frameErr
							break
						}
						copy(ptrs[i:end], framePtrs)
						framesTotal++
						planStoredBytes += stats.StoredPayloadBytes
						continue
					}
					framePtrs, frameErr := w.AppendFrame(0, nil, frame)
					if frameErr != nil {
						err = frameErr
						break
					}
					copy(ptrs[i:end], framePtrs)
					framesTotal++
				}
			}
			if err == nil {
				if planStoredBytes <= 0 {
					if delta := w.Size() - beforePlanSize; delta > 0 {
						planStoredBytes = int(delta)
					}
				}
				plan.storedBytes = planStoredBytes
				plan.wallNs = time.Since(planStart).Nanoseconds()
			}
			continue
		}

		dictRaw += plan.rawBytes
		dictRecords += len(segment)
		if preparedAppender != nil && len(plan.frames) > 0 {
			for fi := range plan.frames {
				pf := &plan.frames[fi]
				dst := ptrs[pf.start:pf.end]
				if _, frameErr := preparedAppender.AppendEncodedFrameInto(pf.body, pf.stats.Records, dst); frameErr != nil {
					err = frameErr
					break
				}
				releasePreparedDictFrame(pf)
				framesTotal++
				if pf.stats.Attempted {
					framesTried++
				}
				if pf.stats.Kept {
					framesKept++
					if plan.probe {
						probeKept = true
					}
				}
				if pf.stats.StoredPayloadBytes > 0 {
					dictStored += pf.stats.StoredPayloadBytes
					planStoredBytes += pf.stats.StoredPayloadBytes
				}
			}
		} else {
			for i := plan.start; i < plan.end; i += plan.k {
				end := i + plan.k
				if end > plan.end {
					end = plan.end
				}
				frame := records[i:end]
				if statsWriterInto != nil {
					dst := ptrs[i:end]
					_, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(plan.dictID, plan.dict, frame, dst)
					if frameErr != nil {
						err = frameErr
						break
					}
					framesTotal++
					if stats.Attempted {
						framesTried++
					}
					if stats.Kept {
						framesKept++
						if plan.probe {
							probeKept = true
						}
					}
					if stats.StoredPayloadBytes > 0 {
						dictStored += stats.StoredPayloadBytes
						planStoredBytes += stats.StoredPayloadBytes
					}
					continue
				}
				if statsWriter != nil {
					framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(plan.dictID, plan.dict, frame)
					if frameErr != nil {
						err = frameErr
						break
					}
					copy(ptrs[i:end], framePtrs)
					framesTotal++
					if stats.Attempted {
						framesTried++
					}
					if stats.Kept {
						framesKept++
						if plan.probe {
							probeKept = true
						}
					}
					if stats.StoredPayloadBytes > 0 {
						dictStored += stats.StoredPayloadBytes
						planStoredBytes += stats.StoredPayloadBytes
					}
					continue
				}
				framePtrs, frameErr := w.AppendFrame(plan.dictID, plan.dict, frame)
				if frameErr != nil {
					err = frameErr
					break
				}
				copy(ptrs[i:end], framePtrs)
				framesTotal++
			}
		}
		if err == nil {
			if planStoredBytes <= 0 {
				if delta := w.Size() - beforePlanSize; delta > 0 {
					planStoredBytes = int(delta)
				}
			}
			plan.storedBytes = planStoredBytes
			plan.wallNs = time.Since(planStart).Nanoseconds()
		}
	}

	deferredFlushed := false
	if err == nil {
		switch {
		case needSync:
			err = w.Sync()
		case needFlush:
			err = w.Flush()
		default:
			if db.shouldFlushDeferredValueLog(vlogWriteOff, records) {
				err = w.Flush()
				deferredFlushed = err == nil
			}
		}
	}
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	if err == nil {
		if needSync || needFlush || deferredFlushed {
			l.vlogDirty.Store(false)
		} else if totalBytes > 0 {
			l.vlogDirty.Store(true)
		}
	}
	if err == nil && totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	if err == nil && l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
		l.vlogRetainedPath = l.vlogPath
		retainPath = l.vlogPath
	}
	l.vlogMu.Unlock()
	if len(retainPaths) > 0 {
		for _, path := range retainPaths {
			db.markValueLogRetain(path)
		}
	}

	if err == nil && framesTotal > 0 {
		db.valueLogDictFrames.total.Add(uint64(framesTotal))
		if framesTried > 0 {
			db.valueLogDictFrames.attempted.Add(uint64(framesTried))
		}
		if framesKept > 0 {
			db.valueLogDictFrames.kept.Add(uint64(framesKept))
		}
	}

	if dictRaw > 0 {
		if dictStored <= 0 {
			dictStored = dictRaw
		}
		db.valueLogDictObservePayload(uint64(dictRaw), uint64(dictStored), dictRecords)
	}
	if probeKept {
		db.valueLogDictPauseRemaining.Store(0)
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
		db.valueLogDictIncompressibleHoldRemaining.Store(0)
		db.valueLogDictIncompressibleHitStreak.Store(0)
		if db.valueLogDictIncompressibleProbeBytes > 0 {
			db.valueLogDictIncompressibleProbeRemaining.Store(db.valueLogDictIncompressibleProbeBytes)
		}
	}
	if err == nil {
		for i := range plans {
			plan := &plans[i]
			rawForSelector := plan.rawBytes
			if rawForSelector <= 0 {
				continue
			}
			storedForSelector := plan.storedBytes
			if storedForSelector <= 0 {
				if plan.writeMode == vlogWriteOff {
					storedForSelector = rawForSelector
				} else {
					storedForSelector = rawForSelector
				}
			}
			codec := plan.blockCodec
			if plan.writeMode != vlogWriteBlock {
				codec = db.valueLogBlockCodec
			}
			unitForSelector := rawForSelector
			if recordsInPlan := plan.end - plan.start; recordsInPlan > 0 {
				unitForSelector = rawForSelector / recordsInPlan
			}
			db.observeVlogWriteMode(l, plan.writeMode, codec, rawForSelector, unitForSelector, storedForSelector, plan.probe, plan.wallNs)
		}
	}

	if err != nil {
		for i := range requests {
			ack := requests[i].ack
			if ack == nil {
				continue
			}
			ack.ptr = page.ValuePtr{}
			ack.retainPath = ""
			ack.err = err
			ack.wg.Done()
		}
		return
	}

	for i := range requests {
		ack := requests[i].ack
		if ack == nil {
			continue
		}
		ack.ptr = ptrs[i]
		if i == 0 {
			ack.retainPath = retainPath
		} else {
			ack.retainPath = ""
		}
		ack.err = nil
		ack.wg.Done()
	}
}

// journalDurability represents the durability boundary for journal writes.
// When WAL is enabled, payload durability must complete before the
// commit-intent durability for sync writes.
type journalDurability uint8

const (
	journalDurabilityNone journalDurability = iota
	journalDurabilityFlush
	journalDurabilitySync
)

func (db *DB) appendWAL(l *lane, records []logRecord, durability journalDurability) error {
	if db.disableJournal {
		return nil
	}
	if len(records) == 0 {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return errWALClosed
	default:
	}
	db.walAckMu.Lock()
	if db.walErr != nil {
		err := db.walErr
		db.walAckMu.Unlock()
		return err
	}
	db.walAckMu.Unlock()

	if len(records) == 1 {
		return db.appendWALOneChecked(l, records[0], durability)
	}

	db.assignCommitSeq(records)

	switch durability {
	case journalDurabilitySync:
		return db.appendWALDirect(l, records, true)
	case journalDurabilityFlush:
		return db.appendWALInline(l, records, true)
	default:
		return db.appendWALInline(l, records, false)
	}
}

func (db *DB) appendWALOne(l *lane, record logRecord, durability journalDurability) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return errWALClosed
	default:
	}
	db.walAckMu.Lock()
	if db.walErr != nil {
		err := db.walErr
		db.walAckMu.Unlock()
		return err
	}
	db.walAckMu.Unlock()
	return db.appendWALOneChecked(l, record, durability)
}

func (db *DB) appendWALOneChecked(l *lane, record logRecord, durability journalDurability) error {
	record.Seq = db.nextCommitSeq.Add(1)
	switch durability {
	case journalDurabilitySync:
		return db.appendWALDirect(l, []logRecord{record}, true)
	case journalDurabilityFlush:
		return db.appendWALInlineOne(l, record, true)
	default:
		return db.appendWALInlineOne(l, record, false)
	}
}

type preparedDictFrame struct {
	start   int
	end     int
	body    []byte
	bodyBuf *vlogPreparedFrameBody
	stats   valuelog.FrameStats
}

func releasePreparedDictFrame(frame *preparedDictFrame) {
	if frame == nil || frame.bodyBuf == nil {
		return
	}
	putVlogPreparedFrameBody(frame.bodyBuf)
	frame.bodyBuf = nil
	frame.body = nil
}

func releasePreparedDictFrames(frames []preparedDictFrame) {
	for i := range frames {
		releasePreparedDictFrame(&frames[i])
	}
}

func (db *DB) valueLogKeepPolicy() (ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64) {
	safetyMargin = db.valueLogAutotuneSafetyMargin()
	if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
		return 0, 0, safetyMargin
	}
	snap := db.valueLogAutotuneMetrics.snapshot()
	return snap.IoNsPerStoredByte, snap.EncodeNsPerRawByte, safetyMargin
}

func (db *DB) shouldUseVlogDictPrepWorkers(l *lane, frameCount, rawPayloadBytes int) bool {
	if l == nil || l.vlogPrepCh == nil || l.vlogPrepMaxWorkers < 2 {
		return false
	}
	if frameCount <= 0 {
		return false
	}
	if frameCount < 2 {
		return false
	}
	if rawPayloadBytes <= 0 {
		return false
	}
	if rawPayloadBytes < 128<<10 {
		return false
	}
	return true
}

func (db *DB) shouldQueueValueLogOne(l *lane, dictID uint64, valueLen int, durability journalDurability, writeMode vlogCompressionWriteMode, wallStart time.Time) bool {
	if l == nil || l.vlogCh == nil {
		return false
	}
	if durability == journalDurabilitySync {
		return false
	}
	// Preserve direct timing mode for autotune/profile callers.
	if !wallStart.IsZero() {
		return false
	}
	// Block mode requires direct append so per-frame codec metadata and grouping
	// stay on the caller path.
	if writeMode == vlogWriteBlock {
		return false
	}
	// Force-pointer profiles prefer queue coalescing; appendValueLogOne still
	// takes an uncontended direct fast path before enqueueing.
	if db.forceValueLogPointers {
		return true
	}
	// Dict path benefits from queue coalescing even for small values.
	if writeMode == vlogWriteDict && dictID != 0 {
		return true
	}
	// Always queue large values.
	if valueLen >= vlogQueueMinValueSize {
		return true
	}
	// Adaptive path: queue when contention/backlog is visible.
	if l.vlogQueueing.Load() {
		return true
	}
	if len(l.vlogCh) > 0 {
		return true
	}
	return false
}

func (db *DB) prepareAppendDictFrames(
	l *lane,
	dictID uint64,
	dict []byte,
	records []valuelog.Record,
	k int,
	rawPayloadBytes int,
	ioNsPerStoredByte float64,
	encodeNsPerRawByte float64,
	safetyMargin float64,
	wallStart time.Time,
) ([]preparedDictFrame, int64, error) {
	if dictID == 0 || len(dict) == 0 || len(records) == 0 {
		return nil, 0, nil
	}
	if k <= 0 {
		k = 1
	}
	frameCount := (len(records) + k - 1) / k
	if frameCount <= 0 {
		return nil, 0, nil
	}
	useWorkers := db.shouldUseVlogDictPrepWorkers(l, frameCount, rawPayloadBytes)
	prepStart := time.Now()
	prepared := getVlogPreparedFrames(frameCount)
	clear(prepared)
	if !useWorkers {
		// Keep dict frame encode work out of vlogMu even when worker threads are
		// unavailable. This reduces lock hold time on small and medium batches.
		preparer := valuelog.NewFramePreparer()
		preparer.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		preparer.SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin)
		preparer.SetEncodeSampleStride(0)
		for fi := 0; fi < frameCount; fi++ {
			start := fi * k
			end := start + k
			if end > len(records) {
				end = len(records)
			}
			bodyBuf := getVlogPreparedFrameBody()
			body, stats, err := preparer.PrepareFrameInto(bodyBuf.buf[:0], dictID, dict, records[start:end])
			if err != nil {
				putVlogPreparedFrameBody(bodyBuf)
				releasePreparedDictFrames(prepared)
				putVlogPreparedFrames(prepared)
				return nil, time.Since(prepStart).Nanoseconds(), err
			}
			bodyBuf.buf = body
			prepared[fi] = preparedDictFrame{
				start:   start,
				end:     end,
				body:    body,
				bodyBuf: bodyBuf,
				stats:   stats,
			}
		}
		return prepared, time.Since(prepStart).Nanoseconds(), nil
	}
	wantedWorkers := frameCount/8 + 1
	if rawPayloadBytes >= 1<<20 {
		wantedWorkers++
	}
	db.ensureVlogDictPrepWorkers(l, wantedWorkers)
	// Parallel prep frames can execute across multiple goroutines, so summing
	// per-frame encode times would overcount relative to write-path wall time.
	// Leave encodeNs unset for worker-prepared frames to avoid poisoning
	// autotune keep-policy estimates.
	measureEncode := false
	results := getVlogDictPrepareResults(frameCount)
	for fi := 0; fi < frameCount; fi++ {
		start := fi * k
		end := start + k
		if end > len(records) {
			end = len(records)
		}
		task := vlogDictPrepareTask{
			fi:             fi,
			dictID:         dictID,
			dict:           dict,
			records:        records[start:end],
			level:          db.valueLogDictFrameEncodeLevel,
			enableEntropy:  db.valueLogDictFrameEnableEntropy,
			ioNsPerStored:  ioNsPerStoredByte,
			encodeNsPerRaw: encodeNsPerRawByte,
			safetyMargin:   safetyMargin,
			measureEncode:  measureEncode,
			out:            results,
		}
		select {
		case l.vlogPrepCh <- task:
		case <-db.closeCh:
			releasePreparedDictFrames(prepared)
			putVlogPreparedFrames(prepared)
			// Workers may still publish into results after close is observed; do
			// not pool this channel on early return.
			return nil, 0, errWALClosed
		}
	}

	var firstErr error
	for collected := 0; collected < frameCount; collected++ {
		select {
		case res := <-results:
			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
				}
				continue
			}
			start := res.fi * k
			end := start + k
			if end > len(records) {
				end = len(records)
			}
			releasePreparedDictFrame(&prepared[res.fi])
			prepared[res.fi] = preparedDictFrame{
				start:   start,
				end:     end,
				body:    res.body,
				bodyBuf: res.bodyBuf,
				stats:   res.stats,
			}
		case <-db.closeCh:
			releasePreparedDictFrames(prepared)
			putVlogPreparedFrames(prepared)
			// Workers may still publish into results after close is observed; do
			// not pool this channel on early return.
			return nil, time.Since(prepStart).Nanoseconds(), errWALClosed
		}
	}
	putVlogDictPrepareResults(results)
	if firstErr != nil {
		releasePreparedDictFrames(prepared)
		putVlogPreparedFrames(prepared)
		return nil, time.Since(prepStart).Nanoseconds(), firstErr
	}
	return prepared, time.Since(prepStart).Nanoseconds(), nil
}

func (db *DB) appendValueLog(l *lane, dictID uint64, dict []byte, records []valuelog.Record, durability journalDurability) ([]page.ValuePtr, error) {
	if !db.splitValueLogEnabled() {
		return nil, errWALUnavailable
	}
	if len(records) == 0 {
		return nil, nil
	}
	if l == nil {
		return nil, errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return nil, errWALClosed
	default:
	}
	wallStart := time.Time{}
	if db.needsVlogAutotuneTiming() {
		wallStart = db.valueLogAutotuneMetrics.now()
	}
	selectorStart := time.Now()

	var (
		bytesWrittenTotal int64
		bytesWrittenLive  int64
		ptrs              []page.ValuePtr
		err               error
	)

	rawPayloadBytes := 0
	for i := range records {
		rawPayloadBytes += len(records[i].Value)
	}
	templatePrepass := false
	if db.valueLogTemplateEnabled && db.valueLogTemplateMode != template.TemplateOff {
		if db.valueLogTemplateMode == template.TemplateOnly {
			dictID = 0
			dict = nil
		} else if db.valueLogTemplateMode == template.TemplatePrepass {
			templatePrepass = true
		}
	}

	if dictID == 0 || templatePrepass {
		records, _ = db.valueLogTemplateEncodeRecords(records)
		rawPayloadBytes = 0
		for i := range records {
			rawPayloadBytes += len(records[i].Value)
		}
	}

	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	selectorPayloadBytes := rawPayloadBytes
	selectorUnitPayloadBytes := rawPayloadBytes
	if n := len(records); n > 0 {
		selectorUnitPayloadBytes = rawPayloadBytes / n
	}
	writeMode, blockCodec, selectorProbe := db.resolveVlogWriteMode(l, dictID, selectorPayloadBytes, selectorUnitPayloadBytes)
	blockMode := writeMode == vlogWriteBlock
	probeCompression := selectorProbe
	paused := false
	if writeMode != vlogWriteDict {
		dictID = 0
		dict = nil
	}

	if dictID != 0 {
		attemptCompression, dictProbe, dictPaused := db.valueLogDictShouldAttemptCompression(rawPayloadBytes)
		probeCompression = probeCompression || dictProbe
		paused = dictPaused
		if !attemptCompression {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
			blockMode = writeMode == vlogWriteBlock
		}
	}
	if dictID != 0 && db.shouldBypassValueLogDictForRecords(records, probeCompression) {
		dictID = 0
		dict = nil
		writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
		blockMode = writeMode == vlogWriteBlock
	}
	if dictID != 0 && db.valueLogAutotuneOptions.DisableBelowValueBytes > 0 {
		avg := rawPayloadBytes / len(records)
		if avg < db.valueLogAutotuneOptions.DisableBelowValueBytes {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
			blockMode = writeMode == vlogWriteBlock
		}
	}
	if dictID != 0 && len(dict) == 0 {
		if b, dictErr := db.dictBytes(context.Background(), dictID); dictErr == nil {
			dict = b
		} else {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
			blockMode = writeMode == vlogWriteBlock
		}
	}
	switch mode {
	case vlogCompressionDefault, vlogCompressionDict:
		db.valueLogDictCollectSamples(records)
	case vlogCompressionAuto:
		if db.allowAutoDictSampling(l, writeMode, selectorUnitPayloadBytes) {
			db.valueLogDictCollectSamples(records)
		}
	}

	k := 1
	if dictID != 0 && len(dict) > 0 {
		k = db.valueLogDictK(dictID)
		k = db.chooseValueLogDictWriteK(k, len(records), rawPayloadBytes)
		if mode == vlogCompressionAuto && normalizeVlogAutoPolicy(db.valueLogAutoPolicy) != vlogAutoSize && k > 16 {
			k = 16
		}
	} else if blockMode && len(records) > 1 {
		k = db.chooseValueLogBlockWriteK(l, len(records), rawPayloadBytes, blockCodec)
	} else if len(records) > 1 {
		// Even when dictionary compression is disabled/paused, grouping records into
		// frames reduces per-record overhead (CRC/header writes) on append-heavy
		// workloads.
		//
		// When no dict is available, we write raw frames (uncompressed) and still
		// benefit from fewer syscalls and less framing work.
		if paused && db.disableJournal {
			k = valuelog.MaxFrameK
		} else if cur := int(db.valueLogDictCurrentK.Load()); cur > 1 {
			k = cur
		} else {
			k = 8
			if db.disableJournal && db.forceValueLogPointers {
				k = 16
			}
		}
	}
	if dictID != 0 && len(dict) > 0 && db.disableJournal {
		// When the redo/journal log is disabled (ingest-mode), favor maximum frame
		// grouping for throughput. This reduces per-record framing overhead and
		// syscall pressure, and is typically safe for write-heavy workloads where
		// random point reads are not the dominant cost.
		k = valuelog.MaxFrameK
	}
	if dictID != 0 && len(dict) > 0 {
		k = db.clampValueLogDictK(k)
	} else {
		if k < 1 {
			k = 1
		}
		if k > valuelog.MaxFrameK {
			k = valuelog.MaxFrameK
		}
	}

	ioNsPerStored, encodeNsPerRaw, safetyMargin := db.valueLogKeepPolicy()
	if probeCompression {
		// Probe writes must actually attempt compression to detect recovery from a
		// paused/degraded stream. Keep-policy gating can short-circuit probes when
		// historical encode-cost estimates are stale or pessimistic.
		ioNsPerStored = 0
		encodeNsPerRaw = 0
	}
	preparedDictFrames, prepEncodeWallNs, prepareErr := db.prepareAppendDictFrames(
		l,
		dictID,
		dict,
		records,
		k,
		rawPayloadBytes,
		ioNsPerStored,
		encodeNsPerRaw,
		safetyMargin,
		wallStart,
	)
	if prepareErr != nil {
		return nil, prepareErr
	}
	if len(preparedDictFrames) > 0 {
		defer func() {
			releasePreparedDictFrames(preparedDictFrames)
			putVlogPreparedFrames(preparedDictFrames)
		}()
	}

	finalWriteMode := vlogWriteOff
	switch {
	case dictID != 0 && len(dict) > 0:
		finalWriteMode = vlogWriteDict
	case blockMode:
		finalWriteMode = vlogWriteBlock
	default:
		finalWriteMode = vlogWriteOff
	}
	finalBlockCodec := blockCodec
	if finalWriteMode != vlogWriteBlock {
		finalBlockCodec = db.valueLogBlockCodec
	}
	ioNsPerStoredForWriter := ioNsPerStored
	encodeNsPerRawForWriter := encodeNsPerRaw
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionAuto && finalWriteMode == vlogWriteBlock {
		// Keep-policy bypass is required for fair auto-mode block evaluation; the
		// selector should decide whether block stays active based on real outcomes.
		ioNsPerStoredForWriter = 0
		encodeNsPerRawForWriter = 0
	}

	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return nil, errWALUnavailable
	}
	firstPath := l.vlogPath
	var retainPaths []string
	noteRotatePath := func(path string) {
		if path == "" {
			return
		}
		if retainPaths == nil {
			if firstPath != "" {
				retainPaths = append(retainPaths, firstPath)
			} else {
				retainPaths = make([]string, 0, 2)
			}
		}
		if len(retainPaths) == 0 || retainPaths[len(retainPaths)-1] != path {
			retainPaths = append(retainPaths, path)
		}
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		return nil, rotateErr
	}
	if l.vlogPath != firstPath {
		noteRotatePath(l.vlogPath)
	}
	// Rotation may replace the writer; reload it before appending.
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return nil, errWALUnavailable
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	caps := l.vlogCaps
	db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
	policySetter := caps.keep
	statsWriter := caps.stats
	statsWriterInto := caps.statsInto
	rawWriterInto := caps.rawInto
	rawBufferedInto := caps.rawBuf
	preparedAppender := caps.prepared
	hasStats := statsWriter != nil
	hasInto := statsWriterInto != nil
	hasRawInto := rawWriterInto != nil
	hasRawBufferedInto := rawBufferedInto != nil
	usePreparedDictFrames := dictID != 0 && len(dict) > 0 && preparedAppender != nil && len(preparedDictFrames) > 0
	segmentStartSize := w.Size()

	if policySetter != nil && !usePreparedDictFrames {
		policySetter.SetKeepPolicy(ioNsPerStoredForWriter, encodeNsPerRawForWriter, safetyMargin)
	}

	storedPayloadBytes := 0
	rawFrameBytes := 0
	frameRecords := 0
	framesTotal := 0
	framesAttempted := 0
	framesKept := 0
	encodeNsTotal := int64(0)
	encodeRawBytes := 0
	rawBatchUsed := false
	durableBoundary := false
	if dictID == 0 && (hasRawInto || hasRawBufferedInto) && finalWriteMode != vlogWriteBlock && len(records) > 1 {
		if maxBytes := db.valueLogMaxSegmentBytesForLane(l); maxBytes > 0 {
			// Ensure the entire raw batch fits within the packed-offset cap so
			// AppendRawFramesWritevInto never returns pointers with out-of-range
			// offsets.
			est := int64(rawPayloadBytes) + int64(len(records))*64
			if est < 0 {
				est = 0
			}
			if est > 0 && w.Size() > maxBytes-est {
				if rotateErr := db.rotateValueLogMuHeld(l); rotateErr != nil {
					l.vlogMu.Unlock()
					return nil, rotateErr
				}
				noteRotatePath(l.vlogPath)
				// Reload writer after rotation so subsequent appends go to the new
				// segment and capabilities match the writer instance.
				w = l.vlog
				if w == nil {
					l.vlogMu.Unlock()
					return nil, errWALUnavailable
				}
				if l.vlogCaps.writer != w {
					l.vlogCaps = computeVlogWriterCaps(w)
				}
				caps = l.vlogCaps
				db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
				statsWriter = caps.stats
				statsWriterInto = caps.statsInto
				rawWriterInto = caps.rawInto
				rawBufferedInto = caps.rawBuf
				hasStats = statsWriter != nil
				hasInto = statsWriterInto != nil
				hasRawInto = rawWriterInto != nil
				hasRawBufferedInto = rawBufferedInto != nil
				segmentStartSize = w.Size()
			}
		}
	}
	if usePreparedDictFrames {
		rawBatchUsed = true
		ptrs = getValueLogPtrs(len(records))
		for fi := range preparedDictFrames {
			pf := &preparedDictFrames[fi]
			dst := ptrs[pf.start:pf.end]
			if _, frameErr := preparedAppender.AppendEncodedFrameInto(pf.body, pf.stats.Records, dst); frameErr != nil {
				err = frameErr
				break
			}
			releasePreparedDictFrame(pf)
			rawFrameBytes += pf.stats.RawPayloadBytes
			storedPayloadBytes += pf.stats.StoredPayloadBytes
			frameRecords += pf.stats.Records
			framesTotal++
			if pf.stats.Attempted {
				framesAttempted++
			}
			if pf.stats.Kept {
				framesKept++
			}
			if pf.stats.EncodeNs > 0 && pf.stats.RawPayloadBytes > 0 {
				encodeNsTotal += pf.stats.EncodeNs
				encodeRawBytes += pf.stats.RawPayloadBytes
			}
		}
	}

	if err == nil && !rawBatchUsed {
		useRawBatch := dictID == 0 && finalWriteMode != vlogWriteBlock && len(records) > 1
		preferBufferedRaw := useRawBatch && hasRawBufferedInto
		if useRawBatch && (preferBufferedRaw || hasRawInto) {
			ptrs = getValueLogPtrs(len(records))
			var (
				stats    valuelog.FrameStats
				batchErr error
			)
			if preferBufferedRaw {
				_, stats, batchErr = rawBufferedInto.AppendRawFramesBufferedInto(records, k, ptrs)
			} else {
				_, stats, batchErr = rawWriterInto.AppendRawFramesWritevInto(records, k, ptrs)
			}
			if batchErr != nil {
				err = batchErr
				putValueLogPtrs(ptrs)
				ptrs = nil
			} else {
				rawFrameBytes = stats.RawPayloadBytes
				storedPayloadBytes = stats.StoredPayloadBytes
				frameRecords = stats.Records
				rawBatchUsed = true
				if k > 0 {
					framesTotal = (len(records) + k - 1) / k
				}
			}
		} else {
			ptrs = getValueLogPtrs(len(records))
		}
	}
	if err == nil && !rawBatchUsed {
		for i := 0; i < len(records); i += k {
			if i > 0 && i%4096 == 0 {
				l.vlogMu.Unlock()
				runtime.Gosched()
				l.vlogMu.Lock()
				w = l.vlog
				if w == nil {
					l.vlogMu.Unlock()
					putValueLogPtrs(ptrs)
					return nil, errWALUnavailable
				}
				if l.vlogCaps.writer != w {
					l.vlogCaps = computeVlogWriterCaps(w)
				}
				caps = l.vlogCaps
				statsWriter = caps.stats
				statsWriterInto = caps.statsInto
				hasStats = statsWriter != nil
				hasInto = statsWriterInto != nil
			}

			if err == nil {
				if maxBytes := db.valueLogMaxSegmentBytesForLane(l); maxBytes > 0 && w.Size() > maxBytes {
					if delta := w.Size() - segmentStartSize; delta > 0 {
						bytesWrittenTotal += delta
					}
					if rotateErr := db.rotateValueLogMuHeld(l); rotateErr != nil {
						err = rotateErr
						break
					}
					noteRotatePath(l.vlogPath)
					// Rotation may replace the writer; reload it and refresh capabilities
					// for subsequent appends.
					w = l.vlog
					if w == nil {
						err = errWALUnavailable
						break
					}
					if l.vlogCaps.writer != w {
						l.vlogCaps = computeVlogWriterCaps(w)
					}
					caps = l.vlogCaps
					db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
					statsWriter = caps.stats
					statsWriterInto = caps.statsInto
					hasStats = statsWriter != nil
					hasInto = statsWriterInto != nil
					segmentStartSize = w.Size()
				}
			}

			end := i + k
			if end > len(records) {
				end = len(records)
			}
			if hasInto {
				dst := ptrs[i:end]
				_, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(dictID, dict, records[i:end], dst)
				if frameErr != nil {
					err = frameErr
					break
				}
				rawFrameBytes += stats.RawPayloadBytes
				storedPayloadBytes += stats.StoredPayloadBytes
				frameRecords += stats.Records
				framesTotal++
				if stats.Attempted {
					framesAttempted++
				}
				if stats.Kept {
					framesKept++
				}
				if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
					encodeNsTotal += stats.EncodeNs
					encodeRawBytes += stats.RawPayloadBytes
				}
				continue
			}
			if hasStats {
				framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(dictID, dict, records[i:end])
				if frameErr != nil {
					err = frameErr
					break
				}
				copy(ptrs[i:end], framePtrs)
				rawFrameBytes += stats.RawPayloadBytes
				storedPayloadBytes += stats.StoredPayloadBytes
				frameRecords += stats.Records
				framesTotal++
				if stats.Attempted {
					framesAttempted++
				}
				if stats.Kept {
					framesKept++
				}
				if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
					encodeNsTotal += stats.EncodeNs
					encodeRawBytes += stats.RawPayloadBytes
				}
				continue
			}

			framePtrs, frameErr := w.AppendFrame(dictID, dict, records[i:end])
			if frameErr != nil {
				err = frameErr
				break
			}
			copy(ptrs[i:end], framePtrs)
			framesTotal++
		}
	}
	if err == nil {
		switch durability {
		case journalDurabilityFlush:
			err = w.Flush()
			durableBoundary = err == nil
		case journalDurabilitySync:
			err = w.Sync()
			durableBoundary = err == nil
		default:
			if db.shouldFlushDeferredValueLog(finalWriteMode, records) {
				// In deferred value-log mode, the index will publish pointers to
				// value-log records during the flush/commit path. Ensure the value-log
				// bytes are visible to readers even when durability is "none".
				err = w.Flush()
				durableBoundary = err == nil
			}
		}
	}
	if err == nil {
		bytesWrittenLive = w.Size() - segmentStartSize
		if bytesWrittenLive > 0 {
			bytesWrittenTotal += bytesWrittenLive
		}
	}
	if err == nil {
		if durableBoundary {
			l.vlogDirty.Store(false)
		} else if bytesWrittenLive > 0 {
			l.vlogDirty.Store(true)
		}
	}
	if db.testBeforeVlogUnlock != nil {
		db.testBeforeVlogUnlock(int(l.id))
	}
	l.vlogMu.Unlock()
	if len(retainPaths) > 0 {
		for _, path := range retainPaths {
			db.markValueLogRetain(path)
		}
	}
	if err != nil {
		putValueLogPtrs(ptrs)
		return nil, err
	}
	for i := range ptrs {
		if !page.IsValueLogFileID(ptrs[i].FileID) {
			putValueLogPtrs(ptrs)
			return nil, fmt.Errorf("cachingdb: appendValueLog produced invalid pointer idx=%d ptr=%+v", i, ptrs[i])
		}
	}
	if framesTotal > 0 {
		db.valueLogDictFrames.total.Add(uint64(framesTotal))
		if framesAttempted > 0 {
			db.valueLogDictFrames.attempted.Add(uint64(framesAttempted))
		}
		if framesKept > 0 {
			db.valueLogDictFrames.kept.Add(uint64(framesKept))
		}
	}
	if dictID != 0 && len(dict) > 0 {
		if rawFrameBytes == 0 {
			rawFrameBytes = rawPayloadBytes
		}
		if storedPayloadBytes == 0 {
			// Best-effort fallback when writer stats are unavailable.
			storedPayloadBytes = int(bytesWrittenTotal)
		}
		if frameRecords == 0 {
			frameRecords = len(records)
		}
		db.valueLogDictObservePayload(uint64(rawFrameBytes), uint64(storedPayloadBytes), frameRecords)
	}
	if probeCompression && framesKept > 0 {
		// A successful probe indicates compressibility returned; immediately
		// clear the pause so subsequent frames can use dictionaries again.
		db.valueLogDictPauseRemaining.Store(0)
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
		db.valueLogDictIncompressibleHoldRemaining.Store(0)
		db.valueLogDictIncompressibleHitStreak.Store(0)
		if db.valueLogDictIncompressibleProbeBytes > 0 {
			db.valueLogDictIncompressibleProbeRemaining.Store(db.valueLogDictIncompressibleProbeBytes)
		}
	}
	rawForSelector := rawFrameBytes
	if rawForSelector == 0 {
		rawForSelector = rawPayloadBytes
	}
	unitForSelector := rawForSelector
	if n := len(records); n > 0 {
		unitForSelector = rawForSelector / n
	}
	storedForSelector := storedPayloadBytes
	if storedForSelector <= 0 {
		switch finalWriteMode {
		case vlogWriteOff:
			storedForSelector = rawForSelector
		default:
			if bytesWrittenTotal > 0 {
				storedForSelector = int(bytesWrittenTotal)
			} else {
				storedForSelector = rawForSelector
			}
		}
	}
	selectorWallNs := time.Since(selectorStart).Nanoseconds()
	db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, rawForSelector, unitForSelector, storedForSelector, probeCompression, selectorWallNs)

	if bytesWrittenLive > 0 {
		l.vlogLiveBytes.Add(bytesWrittenLive)
		db.vlogGenerationChurnBytes.Add(uint64(bytesWrittenLive))
	}
	if usePreparedDictFrames && prepEncodeWallNs > 0 && rawFrameBytes > 0 && encodeRawBytes == 0 {
		// Prepared frames are encoded before taking vlogMu; account prep wall-time
		// once per batch so autotune keep-policy sees non-zero encode cost.
		encodeEstimateNs := prepEncodeWallNs
		if encodeEstimateNs < 0 {
			encodeEstimateNs = 0
		}
		if ioNsPerStored > 0 && rawFrameBytes > storedPayloadBytes {
			// Bound accounting to a fraction of observed IO savings so encode cost
			// estimates stay stable instead of oscillating into "always skip".
			maxBySavings := int64(float64(rawFrameBytes-storedPayloadBytes) * ioNsPerStored * 0.5)
			if maxBySavings > 0 && encodeEstimateNs > maxBySavings {
				encodeEstimateNs = maxBySavings
			}
		}
		if encodeNsPerRaw > 0 {
			// Bound wall-time accounting to a multiple of the current encode model
			// so unrelated scheduler stalls do not dominate encode estimates.
			maxNs := int64(float64(rawFrameBytes) * encodeNsPerRaw * 4)
			if maxNs > 0 && encodeEstimateNs > maxNs {
				encodeEstimateNs = maxNs
			}
		} else {
			const maxPrepEncodeNsPerRawByte = 8.0
			maxNs := int64(float64(rawFrameBytes) * maxPrepEncodeNsPerRawByte)
			if maxNs > 0 && encodeEstimateNs > maxNs {
				encodeEstimateNs = maxNs
			}
		}
		if encodeEstimateNs > 0 {
			encodeNsTotal += encodeEstimateNs
			encodeRawBytes += rawFrameBytes
		}
	}
	if !wallStart.IsZero() {
		storedForMetrics := storedPayloadBytes
		if storedForMetrics == 0 && bytesWrittenTotal > 0 {
			storedForMetrics = int(bytesWrittenTotal)
		}
		db.valueLogAutotuneMetrics.observe(wallStart, rawPayloadBytes, storedForMetrics, encodeNsTotal, encodeRawBytes)
	}
	return ptrs, nil
}

func (db *DB) appendValueLogOne(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) (page.ValuePtr, string, error) {
	return db.appendValueLogOneInternal(l, dictID, dict, rid, value, durability, true)
}

func (db *DB) appendValueLogOneRaw(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) (page.ValuePtr, string, error) {
	return db.appendValueLogOneInternal(l, dictID, dict, rid, value, durability, true)
}

func (db *DB) appendValueLogOneInternal(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability, allowQueue bool) (page.ValuePtr, string, error) {
	if !db.splitValueLogEnabled() {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l == nil {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return page.ValuePtr{}, "", errWALClosed
	default:
	}
	wallStart := time.Time{}
	if db.needsVlogAutotuneTiming() {
		wallStart = db.valueLogAutotuneMetrics.now()
	}
	selectorStart := time.Now()

	var (
		totalBytes int64
		ptr        page.ValuePtr
		err        error
	)

	templatePrepass := false
	if db.valueLogTemplateEnabled && db.valueLogTemplateMode != template.TemplateOff {
		if db.valueLogTemplateMode == template.TemplateOnly {
			dictID = 0
			dict = nil
		} else if db.valueLogTemplateMode == template.TemplatePrepass {
			templatePrepass = true
		}
	}

	if (dictID == 0 || templatePrepass) && db.templateCompressionEnabled() {
		if payload, ok := db.valueLogTemplateEngine.Encode(nil, value, db.templateStore); ok {
			value = payload
		}
	}

	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	writeMode, blockCodec, selectorProbe := db.resolveVlogWriteMode(l, dictID, len(value), len(value))
	probeCompression := selectorProbe
	if writeMode != vlogWriteDict {
		dictID = 0
		dict = nil
	}
	if dictID != 0 {
		attemptCompression, dictProbe, _ := db.valueLogDictShouldAttemptCompression(len(value))
		probeCompression = probeCompression || dictProbe
		if !attemptCompression {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
		}
	}
	if dictID != 0 && db.shouldBypassValueLogDictForValue(value, probeCompression) {
		dictID = 0
		dict = nil
		writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
	}
	if dictID != 0 && db.valueLogAutotuneOptions.DisableBelowValueBytes > 0 && len(value) < db.valueLogAutotuneOptions.DisableBelowValueBytes {
		dictID = 0
		dict = nil
		writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
	}
	if dictID != 0 && len(dict) == 0 {
		if b, dictErr := db.dictBytes(context.Background(), dictID); dictErr == nil {
			dict = b
		} else {
			dictID = 0
			dict = nil
			writeMode = fallbackAutoVlogWriteMode(mode, writeMode)
		}
	}
	finalWriteMode := vlogWriteOff
	switch {
	case dictID != 0:
		finalWriteMode = vlogWriteDict
	case writeMode == vlogWriteBlock:
		finalWriteMode = vlogWriteBlock
	default:
		finalWriteMode = vlogWriteOff
	}
	finalBlockCodec := blockCodec
	if finalWriteMode != vlogWriteBlock {
		finalBlockCodec = db.valueLogBlockCodec
	}
	switch mode {
	case vlogCompressionDefault, vlogCompressionDict:
		db.valueLogDictCollectSample(value)
	case vlogCompressionAuto:
		if db.allowAutoDictSampling(l, writeMode, len(value)) {
			db.valueLogDictCollectSample(value)
		}
	}

	if allowQueue && db.shouldQueueValueLogOne(l, dictID, len(value), durability, finalWriteMode, wallStart) {
		if dictID == 0 && !l.vlogQueueing.Load() && l.vlogMu.TryLock() {
			w := l.vlog
			if w == nil {
				l.vlogMu.Unlock()
				return page.ValuePtr{}, "", errWALUnavailable
			}
			if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
				l.vlogMu.Unlock()
				return page.ValuePtr{}, "", rotateErr
			}
			// Reload writer in case rotation replaced l.vlog.
			w = l.vlog
			if w == nil {
				l.vlogMu.Unlock()
				return page.ValuePtr{}, "", errWALUnavailable
			}
			if l.vlogCaps.writer != w {
				l.vlogCaps = computeVlogWriterCaps(w)
			}
			caps := l.vlogCaps
			db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
			policySetter := caps.keep
			startSize := w.Size()

			if policySetter != nil {
				snap := db.valueLogAutotuneMetrics.snapshot()
				ioNsPerStored := snap.IoNsPerStoredByte
				encodeNsPerRaw := snap.EncodeNsPerRawByte
				if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
					ioNsPerStored = 0
					encodeNsPerRaw = 0
				}
				policySetter.SetKeepPolicy(ioNsPerStored, encodeNsPerRaw, db.valueLogAutotuneSafetyMargin())
			}

			stats := valuelog.FrameStats{Records: 1, RawPayloadBytes: len(value), StoredPayloadBytes: len(value)}
			durableBoundary := false
			if finalWriteMode == vlogWriteBlock {
				if concrete, ok := w.(*valuelog.Writer); ok {
					ptr, stats, err = concrete.AppendOneFrameWithStats(0, nil, rid, value)
				} else {
					var (
						rec        [1]valuelog.Record
						ptrScratch [1]page.ValuePtr
						ptrs       []page.ValuePtr
					)
					rec[0] = valuelog.Record{RID: rid, Value: value}
					switch {
					case caps.statsInto != nil:
						ptrs, stats, err = caps.statsInto.AppendFrameWithStatsInto(0, nil, rec[:], ptrScratch[:])
					case caps.stats != nil:
						ptrs, stats, err = caps.stats.AppendFrameWithStats(0, nil, rec[:])
					default:
						ptr, err = w.Append(0, nil, rid, value)
					}
					if err == nil && ptr == (page.ValuePtr{}) {
						if len(ptrs) != 1 {
							err = fmt.Errorf("cachingdb: value-log wrote %d ptrs for 1 record", len(ptrs))
						} else {
							ptr = ptrs[0]
						}
					}
				}
			} else {
				ptr, err = w.Append(0, nil, rid, value)
			}
			if err == nil {
				switch durability {
				case journalDurabilityFlush:
					err = w.Flush()
					durableBoundary = err == nil
				default:
					if db.shouldFlushDeferredValueLogValue(finalWriteMode, value) {
						err = w.Flush()
						durableBoundary = err == nil
					}
				}
			}
			if err == nil {
				totalBytes = w.Size() - startSize
			}
			retainPath := ""
			if l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
				l.vlogRetainedPath = l.vlogPath
				retainPath = l.vlogPath
			}
			if err == nil {
				if durableBoundary {
					l.vlogDirty.Store(false)
				} else if totalBytes > 0 {
					l.vlogDirty.Store(true)
				}
			}
			if db.testBeforeVlogUnlock != nil {
				db.testBeforeVlogUnlock(int(l.id))
			}
			l.vlogMu.Unlock()
			if err != nil {
				return page.ValuePtr{}, "", err
			}
			db.valueLogDictFrames.total.Add(1)
			if stats.Attempted {
				db.valueLogDictFrames.attempted.Add(1)
			}
			if stats.Kept {
				db.valueLogDictFrames.kept.Add(1)
			}
			if totalBytes > 0 {
				l.vlogLiveBytes.Add(totalBytes)
			}
			storedForSelector := stats.StoredPayloadBytes
			if storedForSelector <= 0 || (finalWriteMode == vlogWriteBlock && !stats.Attempted && storedForSelector == len(value) && totalBytes > 0) {
				if totalBytes > 0 {
					storedForSelector = int(totalBytes)
				} else {
					storedForSelector = len(value)
				}
			}
			selectorWallNs := time.Since(selectorStart).Nanoseconds()
			db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, len(value), len(value), storedForSelector, probeCompression, selectorWallNs)
			return ptr, retainPath, nil
		}

		l.vlogQueueing.Store(true)
		ack := vlogAckPool.Get().(*vlogAck)
		ack.ptr = page.ValuePtr{}
		ack.retainPath = ""
		ack.err = nil
		ack.wg.Add(1)

		req := vlogWriteRequest{
			rid:              rid,
			value:            value,
			dictID:           dictID,
			writeMode:        finalWriteMode,
			blockCodec:       finalBlockCodec,
			probeCompression: probeCompression,
			durability:       durability,
			enqueuedAt:       time.Now(),
			ack:              ack,
		}
		select {
		case l.vlogCh <- req:
			observeLaneVlogQueueEnqueue(l, len(l.vlogCh))
		case <-db.closeCh:
			ack.err = errWALClosed
			ack.wg.Done()
		}

		ack.wg.Wait()
		ptr := ack.ptr
		retainPath := ack.retainPath
		err := ack.err
		vlogAckPool.Put(ack)
		return ptr, retainPath, err
	}

	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", rotateErr
	}
	// Reload writer in case rotation replaced l.vlog.
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	caps := l.vlogCaps
	db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
	policySetter := caps.keep
	statsWriter := caps.stats
	statsWriterInto := caps.statsInto
	startSize := w.Size()
	durableBoundary := false

	if policySetter != nil {
		snap := db.valueLogAutotuneMetrics.snapshot()
		ioNsPerStored := snap.IoNsPerStoredByte
		encodeNsPerRaw := snap.EncodeNsPerRawByte
		if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
			ioNsPerStored = 0
			encodeNsPerRaw = 0
		}
		policySetter.SetKeepPolicy(ioNsPerStored, encodeNsPerRaw, db.valueLogAutotuneSafetyMargin())
	}

	stats := valuelog.FrameStats{Records: 1, RawPayloadBytes: len(value), StoredPayloadBytes: len(value)}
	if finalWriteMode == vlogWriteBlock {
		if concrete, ok := w.(*valuelog.Writer); ok {
			ptr, stats, err = concrete.AppendOneFrameWithStats(0, nil, rid, value)
		} else {
			var ptrScratch [1]page.ValuePtr
			var rec [1]valuelog.Record
			rec[0] = valuelog.Record{RID: rid, Value: value}
			var ptrs []page.ValuePtr
			var frameErr error
			if statsWriterInto != nil {
				ptrs, stats, frameErr = statsWriterInto.AppendFrameWithStatsInto(0, nil, rec[:], ptrScratch[:])
			} else if statsWriter != nil {
				ptrs, stats, frameErr = statsWriter.AppendFrameWithStats(0, nil, rec[:])
			} else {
				ptr, err = w.Append(0, nil, rid, value)
			}
			if frameErr != nil {
				err = frameErr
			} else if err == nil && ptr == (page.ValuePtr{}) {
				if len(ptrs) != 1 {
					err = fmt.Errorf("cachingdb: value-log wrote %d ptrs for 1 record", len(ptrs))
				} else {
					ptr = ptrs[0]
				}
			}
		}
	} else if dictID == 0 {
		ptr, err = w.Append(0, nil, rid, value)
	} else if len(dict) == 0 {
		ptr, err = w.Append(0, nil, rid, value)
	} else {
		if concrete, ok := w.(*valuelog.Writer); ok {
			ptr, stats, err = concrete.AppendOneFrameWithStats(dictID, dict, rid, value)
		} else {
			var ptrScratch [1]page.ValuePtr
			var rec [1]valuelog.Record
			rec[0] = valuelog.Record{RID: rid, Value: value}
			var ptrs []page.ValuePtr
			var frameErr error
			if statsWriterInto != nil {
				ptrs, stats, frameErr = statsWriterInto.AppendFrameWithStatsInto(dictID, dict, rec[:], ptrScratch[:])
			} else if statsWriter != nil {
				ptrs, stats, frameErr = statsWriter.AppendFrameWithStats(dictID, dict, rec[:])
			} else {
				ptrs, frameErr = w.AppendFrame(dictID, dict, rec[:])
			}
			if frameErr != nil {
				err = frameErr
			} else if len(ptrs) != 1 {
				err = fmt.Errorf("cachingdb: value-log wrote %d ptrs for 1 record", len(ptrs))
			} else {
				ptr = ptrs[0]
			}
		}
	}
	if err == nil {
		switch durability {
		case journalDurabilityFlush:
			err = w.Flush()
			durableBoundary = err == nil
		case journalDurabilitySync:
			err = w.Sync()
			durableBoundary = err == nil
		default:
			if db.shouldFlushDeferredValueLogValue(finalWriteMode, value) {
				err = w.Flush()
				durableBoundary = err == nil
			}
		}
	}
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	retainPath := ""
	if l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
		l.vlogRetainedPath = l.vlogPath
		retainPath = l.vlogPath
	}
	if err == nil {
		if durableBoundary {
			l.vlogDirty.Store(false)
		} else if totalBytes > 0 {
			l.vlogDirty.Store(true)
		}
	}
	if db.testBeforeVlogUnlock != nil {
		db.testBeforeVlogUnlock(int(l.id))
	}
	l.vlogMu.Unlock()
	if err != nil {
		return page.ValuePtr{}, "", err
	}
	db.valueLogDictFrames.total.Add(1)
	if stats.Attempted {
		db.valueLogDictFrames.attempted.Add(1)
	}
	if stats.Kept {
		db.valueLogDictFrames.kept.Add(1)
	}
	if dictID != 0 && len(dict) > 0 {
		db.valueLogDictObservePayload(uint64(stats.RawPayloadBytes), uint64(stats.StoredPayloadBytes), stats.Records)
	}
	if probeCompression && stats.Kept {
		db.valueLogDictPauseRemaining.Store(0)
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
		db.valueLogDictIncompressibleHoldRemaining.Store(0)
		db.valueLogDictIncompressibleHitStreak.Store(0)
		if db.valueLogDictIncompressibleProbeBytes > 0 {
			db.valueLogDictIncompressibleProbeRemaining.Store(db.valueLogDictIncompressibleProbeBytes)
		}
	}
	storedForSelector := stats.StoredPayloadBytes
	if storedForSelector <= 0 || (finalWriteMode == vlogWriteBlock && !stats.Attempted && storedForSelector == len(value) && totalBytes > 0) {
		if totalBytes > 0 {
			storedForSelector = int(totalBytes)
		} else {
			storedForSelector = len(value)
		}
	}
	selectorWallNs := time.Since(selectorStart).Nanoseconds()
	db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, len(value), len(value), storedForSelector, probeCompression, selectorWallNs)
	if totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	encodeNsTotal := int64(0)
	encodeRawBytes := 0
	if stats.EncodeNs > 0 && stats.RawPayloadBytes > 0 {
		encodeNsTotal = stats.EncodeNs
		encodeRawBytes = stats.RawPayloadBytes
	}
	if !wallStart.IsZero() {
		storedForMetrics := stats.StoredPayloadBytes
		if storedForMetrics == 0 && totalBytes > 0 {
			storedForMetrics = int(totalBytes)
		}
		db.valueLogAutotuneMetrics.observe(wallStart, len(value), storedForMetrics, encodeNsTotal, encodeRawBytes)
	}
	return ptr, retainPath, nil
}

func (db *DB) appendWALInline(l *lane, records []logRecord, flush bool) error {
	if l == nil {
		return errWALUnavailable
	}
	if len(records) == 1 {
		return db.appendWALInlineOne(l, records[0], flush)
	}

	var (
		totalBytes int64
		err        error
	)

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	if len(records) == 1 {
		rec := records[0]
		err = w.Append(rec)
		totalBytes = db.logRecordSize(rec.Key, rec.Value)
	} else {
		err = w.AppendBatch(records)
		totalBytes = db.logBatchSize(records)
	}
	if err == nil && flush {
		err = w.Flush()
	}
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) appendWALInlineOne(l *lane, record logRecord, flush bool) error {
	if l == nil {
		return errWALUnavailable
	}

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	err := w.Append(record)
	totalBytes := db.logRecordSize(record.Key, record.Value)
	if err == nil && flush {
		err = w.Flush()
	}
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) flushWALLane(l *lane) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	err := w.Flush()
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}
	return nil
}

func (db *DB) appendWALDirect(l *lane, records []logRecord, sync bool) error {
	if l == nil {
		return errWALUnavailable
	}
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.wg.Add(1)

	req := walWriteRequest{records: records, sync: sync, ack: ack}
	select {
	case l.walCh <- req:
		// wait for ack
	case <-db.closeCh:
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALClosed
	}

	ack.wg.Wait()
	err := ack.err
	walAckPool.Put(ack)
	return err
}

func (db *DB) appendWALFast(l *lane, record logRecord) error {
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.wg.Add(1)

	if l == nil {
		ack.err = errWALUnavailable
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALUnavailable
	}

	record.Seq = db.nextCommitSeq.Add(1)

	l.walFastMu.Lock()
	for !l.walFastClosed && len(l.walFastQueue)-l.walFastHead >= walFastQueueMax {
		l.walFastCond.Wait()
	}
	if l.walFastClosed {
		l.walFastMu.Unlock()
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALClosed
	}
	l.walFastQueue = append(l.walFastQueue, walFastItem{record: record, ack: ack})
	l.walFastCond.Signal()
	l.walFastMu.Unlock()

	ack.wg.Wait()
	err := ack.err
	walAckPool.Put(ack)
	return err
}

func (db *DB) autoCheckpointLoop(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) {
	defer db.wg.Done()

	var intervalTicker *time.Ticker
	intervalCh := (<-chan time.Time)(nil)
	if interval > 0 {
		intervalTicker = time.NewTicker(interval)
		intervalCh = intervalTicker.C
		defer intervalTicker.Stop()
	}

	var idleTimer *time.Timer
	idleCh := (<-chan time.Time)(nil)
	if idleInterval > 0 {
		idleTimer = time.NewTimer(idleInterval)
		if !idleTimer.Stop() {
			<-idleTimer.C
		}
		idleCh = idleTimer.C
	}

	for {
		select {
		case <-db.closeCh:
			return
		case <-intervalCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeInterval)
		case <-db.autoCheckpointOnceCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeForce)
		case <-db.autoCheckpointWriteCh:
			if maxWALBytes > 0 {
				db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeSize)
			}
			if idleTimer != nil {
				resetTimer(idleTimer, idleInterval)
			}
		case <-idleCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeIdle)
		}
	}
}

func (db *DB) maybeAutoCheckpoint(maxWALBytes int64, mode autoCheckpointMode) {
	effectiveBytes := db.effectiveWALBytes()
	if effectiveBytes <= 0 {
		return
	}
	reclaimableBytes := db.reclaimableWALBytes()

	// Avoid thrashing the checkpoint path when workloads are mostly idle but
	// produce tiny write bursts.
	if mode == autoCheckpointModeIdle {
		if effectiveBytes < db.minIdleCheckpointWALBytes() {
			return
		}
		last := db.autoCheckpointLastUnixNano.Load()
		if last > 0 && time.Since(time.Unix(0, last)) < autoCheckpointMinIdleInterval {
			return
		}
	}

	switch mode {
	case autoCheckpointModeInterval, autoCheckpointModeIdle, autoCheckpointModeForce:
		// proceed
	case autoCheckpointModeSize:
		if maxWALBytes <= 0 || reclaimableBytes < maxWALBytes {
			return
		}
		// Avoid repeatedly checkpointing when WAL bytes cannot be reduced (e.g.
		// value-log segments retained for pointers). Rearm once reclaimable bytes
		// drop below maxWALBytes/2.
		if !db.autoCheckpointSizeArmed.CompareAndSwap(true, false) {
			return
		}
	default:
		// Unknown mode: be conservative and do nothing.
		return
	}

	before := effectiveBytes
	beforeReclaimable := reclaimableBytes
	start := time.Now()
	err := db.Checkpoint()
	dur := time.Since(start)
	after := db.effectiveWALBytes()
	afterReclaimable := db.reclaimableWALBytes()
	trimmed := before - after
	if trimmed < 0 {
		trimmed = 0
	}
	if maxWALBytes > 0 && afterReclaimable < maxWALBytes/2 {
		db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
	}

	// Best-effort: failures here should be surfaced via normal write paths or
	// explicit maintenance calls. Avoid printing from background maintenance.
	if err != nil {
		if mode == autoCheckpointModeSize && maxWALBytes > 0 {
			db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
		}
		return
	}

	db.autoCheckpointCount.Add(1)
	db.autoCheckpointLastReason.Store(uint32(mode))
	db.autoCheckpointLastUnixNano.Store(time.Now().UnixNano())
	db.autoCheckpointLastDurNanos.Store(dur.Nanoseconds())
	db.autoCheckpointLastWALBefore.Store(before)
	db.autoCheckpointLastWALAfter.Store(after)
	db.autoCheckpointLastWALReclaimableBefore.Store(beforeReclaimable)
	db.autoCheckpointLastWALReclaimableAfter.Store(afterReclaimable)
	db.autoCheckpointLastWALTrimmed.Store(trimmed)
}

func (db *DB) startVlogGenerationLoop() {
	if db == nil {
		return
	}
	if envBool(envDisableVlogGenerationLoop) {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
		return
	}
	if db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
		return
	}
	if _, ok := db.backend.(backendValueLogRewriter); !ok {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerDisabled)
		return
	}
	// GC integration is optional in this phase; rewrite is required.
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
	db.wg.Add(1)
	go db.vlogGenerationLoop()
}

func (db *DB) vlogGenerationLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(vlogGenerationLoopInterval)
	defer ticker.Stop()
	ticks := 0
	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			ticks++
			db.maybeRunVlogGenerationMaintenance(ticks%vlogGenerationGCEvery == 0)
		}
	}
}

func (db *DB) vlogGenerationRewriteBudgetCapBytes() int64 {
	if db == nil {
		return 0
	}
	// Allow rewrite bursts up to one "generation footprint" (hot+warm+cold) and
	// at least up to the configured retained-bytes trigger when set. These values
	// come from caller configuration/profile defaults and avoid hard-coded burst
	// ceilings.
	capBytes := int64(0)
	if db.valueLogRewriteTriggerBytes > 0 {
		capBytes = db.valueLogRewriteTriggerBytes
	}
	targets := int64(0)
	if db.valueLogGenerationHotTarget > 0 {
		targets = addClampInt64(targets, db.valueLogGenerationHotTarget, maxPositiveInt64)
	}
	if db.valueLogGenerationWarmTarget > 0 {
		targets = addClampInt64(targets, db.valueLogGenerationWarmTarget, maxPositiveInt64)
	}
	if db.valueLogGenerationColdTarget > 0 {
		targets = addClampInt64(targets, db.valueLogGenerationColdTarget, maxPositiveInt64)
	}
	if targets > capBytes {
		capBytes = targets
	}
	if capBytes < 1 {
		capBytes = 1
	}
	return capBytes
}

func (db *DB) vlogGenerationAccrueRewriteBudget(now time.Time) {
	if db == nil {
		return
	}
	budgetBps := db.valueLogRewriteBudgetBytes
	if budgetBps <= 0 {
		return
	}
	nowNs := now.UnixNano()
	var prevNs int64
	for {
		prevNs = db.vlogGenerationRewriteBudgetLastUnixNano.Load()
		if prevNs > 0 && nowNs <= prevNs {
			return
		}
		if db.vlogGenerationRewriteBudgetLastUnixNano.CompareAndSwap(prevNs, nowNs) {
			break
		}
	}
	if prevNs <= 0 {
		return
	}
	deltaNs := nowNs - prevNs
	capBytes := db.vlogGenerationRewriteBudgetCapBytes()
	add := mulDivClampInt64(budgetBps, deltaNs, int64(time.Second), capBytes)
	if add <= 0 {
		return
	}
	for {
		cur := db.vlogGenerationRewriteBudgetTokensBytes.Load()
		next := addClampInt64(cur, add, capBytes)
		if db.vlogGenerationRewriteBudgetTokensBytes.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (db *DB) vlogGenerationRewriteBudgetEnabled() bool {
	return db != nil && db.valueLogRewriteBudgetBytes > 0
}

func (db *DB) vlogGenerationRewriteMaxSegmentsForRun(queueLen int, budgetTokens int64, opts vlogGenerationMaintenanceOptions) int {
	maxSegments := vlogGenerationRewriteResumeMaxSegments
	if db == nil || queueLen <= 1 || !opts.rewriteDebtDrain {
		return maxSegments
	}
	// Checkpoint-kick retries should keep each debt-drain run small to reduce
	// write amplification when foreground ingest is still active.
	if opts.bypassQuiet && !opts.skipCheckpoint {
		return 1
	}
	if queueLen < maxSegments {
		maxSegments = queueLen
	}
	if queueLen > maxSegments {
		maxSegments = queueLen
	}
	if maxSegments > vlogGenerationRewriteDebtDrainMaxSegments {
		maxSegments = vlogGenerationRewriteDebtDrainMaxSegments
	}
	if maxSegments < 1 {
		maxSegments = 1
	}
	if !db.vlogGenerationRewriteBudgetEnabled() {
		return maxSegments
	}
	if budgetTokens <= 0 {
		return 1
	}
	perSegmentBudget := db.valueLogGenerationWarmTarget
	if perSegmentBudget <= 0 {
		perSegmentBudget = defaultVlogGenerationWarmTargetBytes
	}
	if perSegmentBudget <= 0 {
		return 1
	}
	byBudget := int(budgetTokens / perSegmentBudget)
	if byBudget < 1 {
		byBudget = 1
	}
	if byBudget < maxSegments {
		maxSegments = byBudget
	}
	if maxSegments < 1 {
		maxSegments = 1
	}
	return maxSegments
}

const maxPositiveInt64 = int64(^uint64(0) >> 1)

func addClampInt64(cur, add, limit int64) int64 {
	if limit <= 0 {
		return 0
	}
	if cur <= 0 {
		cur = 0
	}
	if add <= 0 {
		if cur > limit {
			return limit
		}
		return cur
	}
	if cur >= limit || cur > limit-add {
		return limit
	}
	return cur + add
}

func mulDivClampInt64(a, b, div, cap int64) int64 {
	if a <= 0 || b <= 0 || div <= 0 || cap <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	divU := uint64(div)
	var q uint64
	if hi == 0 {
		q = lo / divU
	} else {
		// bits.Div64 requires div > hi. If that contract is not met, the exact
		// quotient is at least 2^64 and will be clamped to cap anyway.
		if divU <= hi {
			return cap
		}
		q, _ = bits.Div64(hi, lo, divU)
	}
	if q > uint64(cap) {
		return cap
	}
	return int64(q)
}
func mulDivClampPositiveInt64(x, y, div, capValue int64) int64 {
	if x <= 0 || y <= 0 || div <= 0 || capValue <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(x), uint64(y))
	capHi, capLo := bits.Mul64(uint64(capValue), uint64(div))
	if hi > capHi || (hi == capHi && lo >= capLo) {
		return capValue
	}
	if hi == 0 {
		q := lo / uint64(div)
		if q > uint64(capValue) {
			return capValue
		}
		return int64(q)
	}
	q, _ := bits.Div64(hi, lo, uint64(div))
	if q > uint64(capValue) {
		return capValue
	}
	return int64(q)
}

func (db *DB) vlogGenerationConsumeRewriteBudgetBytes(n int64) {
	if db == nil || n <= 0 {
		return
	}
	for {
		cur := db.vlogGenerationRewriteBudgetTokensBytes.Load()
		next := cur - n
		if next < 0 {
			next = 0
		}
		if db.vlogGenerationRewriteBudgetTokensBytes.CompareAndSwap(cur, next) {
			return
		}
	}
}

func sumVlogRewritePlanBytes(segments []backenddb.ValueLogRewritePlanSegment, ids []uint32) (total, live, stale int64, ok bool) {
	if len(segments) == 0 || len(ids) == 0 {
		return 0, 0, 0, false
	}
	byID := make(map[uint32]backenddb.ValueLogRewritePlanSegment, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		byID[seg.FileID] = seg
	}
	for _, id := range ids {
		seg, found := byID[id]
		if !found {
			continue
		}
		ok = true
		total += seg.BytesTotal
		live += seg.BytesLive
		stale += seg.BytesStale
	}
	return total, live, stale, ok
}

func sumVlogRewritePlanLiveBytes(segments []backenddb.ValueLogRewritePlanSegment, ids []uint32) (sum int64, ok bool) {
	_, sum, _, ok = sumVlogRewritePlanBytes(segments, ids)
	return sum, ok
}

func vlogGenerationRewriteLedgerIDs(segments []backenddb.ValueLogRewritePlanSegment) []uint32 {
	if len(segments) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		ids = append(ids, seg.FileID)
	}
	return ids
}

func (db *DB) observeVlogGenerationRewritePlanOutcome(plan backenddb.ValueLogRewritePlan, err error) {
	if db == nil {
		return
	}
	db.vlogGenerationRewritePlanRuns.Add(1)
	if err != nil {
		if isVlogGenerationPlannerCanceled(err) {
			db.vlogGenerationRewritePlanCanceled.Add(1)
			db.vlogGenerationRewritePlanCanceledLastNS.Store(time.Now().UnixNano())
		} else {
			db.vlogGenerationRewritePlanErrors.Add(1)
		}
		return
	}
	if len(plan.SourceFileIDs) > 0 || len(plan.SelectedSegments) > 0 || plan.SegmentsSelected > 0 {
		db.vlogGenerationRewritePlanSelected.Add(1)
		return
	}
	db.vlogGenerationRewritePlanEmpty.Add(1)
}

func isVlogGenerationPlannerCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (db *DB) vlogGenerationRewritePlanBackoffActive(now time.Time) bool {
	if db == nil {
		return false
	}
	lastCanceled := db.vlogGenerationRewritePlanCanceledLastNS.Load()
	if lastCanceled <= 0 {
		return false
	}
	return now.Sub(time.Unix(0, lastCanceled)) < vlogGenerationRewritePlanCancelBackoff
}

func (db *DB) observeVlogGenerationRewriteCanceled() {
	if db == nil {
		return
	}
	db.vlogGenerationRewriteCanceledRuns.Add(1)
	db.vlogGenerationRewriteCanceledLastNS.Store(time.Now().UnixNano())
}

func (db *DB) observeVlogGenerationRewriteQueuePrune(dropped int) {
	if db == nil || dropped <= 0 {
		return
	}
	db.vlogGenerationRewriteQueuePruneRuns.Add(1)
	db.vlogGenerationRewriteQueuePruneIDs.Add(uint64(dropped))
}

func (db *DB) vlogGenerationRewriteCancelBackoffActive(now time.Time) bool {
	if db == nil {
		return false
	}
	lastCanceled := db.vlogGenerationRewriteCanceledLastNS.Load()
	if lastCanceled <= 0 {
		return false
	}
	return now.Sub(time.Unix(0, lastCanceled)) < vlogGenerationRewriteCancelBackoff
}

func (db *DB) vlogGenerationRewriteIneffectiveBackoffActive(now time.Time) bool {
	if db == nil {
		return false
	}
	lastIneffective := db.vlogGenerationRewriteIneffectiveLastNS.Load()
	if lastIneffective <= 0 {
		return false
	}
	return now.Sub(time.Unix(0, lastIneffective)) < vlogGenerationRewriteIneffectiveBackoff
}

func filterVlogGenerationRewritePlanByPenalty(plan backenddb.ValueLogRewritePlan, penalties map[uint32]valueLogGenerationRewritePenalty, now time.Time) backenddb.ValueLogRewritePlan {
	if len(plan.SourceFileIDs) == 0 || len(penalties) == 0 {
		return plan
	}
	allowed := make(map[uint32]struct{}, len(plan.SourceFileIDs))
	filteredIDs := make([]uint32, 0, len(plan.SourceFileIDs))
	for _, id := range plan.SourceFileIDs {
		if id == 0 {
			continue
		}
		penalty, ok := penalties[id]
		if ok && penalty.CooldownUntilUnixNano > now.UnixNano() {
			continue
		}
		allowed[id] = struct{}{}
		filteredIDs = append(filteredIDs, id)
	}
	if len(filteredIDs) == len(plan.SourceFileIDs) {
		return plan
	}
	plan.SourceFileIDs = filteredIDs
	plan.SegmentsSelected = len(filteredIDs)
	plan.SelectedBytesTotal = 0
	plan.SelectedBytesLive = 0
	plan.SelectedBytesStale = 0
	if len(plan.SelectedSegments) == 0 {
		return plan
	}
	filteredSegments := make([]backenddb.ValueLogRewritePlanSegment, 0, len(plan.SelectedSegments))
	for _, seg := range plan.SelectedSegments {
		if _, ok := allowed[seg.FileID]; !ok {
			continue
		}
		filteredSegments = append(filteredSegments, seg)
		plan.SelectedBytesTotal += seg.BytesTotal
		plan.SelectedBytesLive += seg.BytesLive
		plan.SelectedBytesStale += seg.BytesStale
	}
	plan.SelectedSegments = filteredSegments
	plan.SegmentsSelected = len(filteredSegments)
	return plan
}

func (db *DB) filterVlogGenerationRewritePlanPenalties(plan backenddb.ValueLogRewritePlan, now time.Time) (backenddb.ValueLogRewritePlan, error) {
	if db == nil || len(plan.SourceFileIDs) == 0 {
		return plan, nil
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		return backenddb.ValueLogRewritePlan{}, err
	}
	return filterVlogGenerationRewritePlanByPenalty(plan, penalties, now), nil
}

func filterVlogGenerationRewritePlanToSegments(plan backenddb.ValueLogRewritePlan, segments []backenddb.ValueLogRewritePlanSegment) backenddb.ValueLogRewritePlan {
	if len(segments) == 0 {
		plan.SourceFileIDs = nil
		plan.SelectedSegments = nil
		plan.SegmentsSelected = 0
		plan.SelectedBytesTotal = 0
		plan.SelectedBytesLive = 0
		plan.SelectedBytesStale = 0
		return plan
	}
	filtered := append([]backenddb.ValueLogRewritePlanSegment(nil), segments...)
	ids := make([]uint32, 0, len(filtered))
	var total, live, stale int64
	for _, seg := range filtered {
		if seg.FileID == 0 {
			continue
		}
		ids = append(ids, seg.FileID)
		total += seg.BytesTotal
		live += seg.BytesLive
		stale += seg.BytesStale
	}
	plan.SourceFileIDs = ids
	plan.SelectedSegments = filtered
	plan.SegmentsSelected = len(ids)
	plan.SelectedBytesTotal = total
	plan.SelectedBytesLive = live
	plan.SelectedBytesStale = stale
	return plan
}

func filterVlogGenerationRewriteLedgerByQuality(segments []backenddb.ValueLogRewritePlanSegment, minStaleRatio float64, minStaleBytes int64) []backenddb.ValueLogRewritePlanSegment {
	if len(segments) == 0 {
		return nil
	}
	if minStaleRatio <= 0 && minStaleBytes <= 0 {
		return append([]backenddb.ValueLogRewritePlanSegment(nil), segments...)
	}
	filtered := make([]backenddb.ValueLogRewritePlanSegment, 0, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		if minStaleBytes > 0 && seg.BytesStale < minStaleBytes {
			continue
		}
		ratio := seg.StaleRatio
		if ratio <= 0 && seg.BytesTotal > 0 {
			ratio = float64(seg.BytesStale) / float64(seg.BytesTotal)
		}
		if minStaleRatio > 0 && ratio < minStaleRatio {
			continue
		}
		filtered = append(filtered, seg)
	}
	return filtered
}

func filterVlogGenerationRewriteLedgerToExistingSegments(segments []backenddb.ValueLogRewritePlanSegment, segmentPresence backendValueLogSegmentPresence) []backenddb.ValueLogRewritePlanSegment {
	if len(segments) == 0 {
		return nil
	}
	if segmentPresence == nil {
		return append([]backenddb.ValueLogRewritePlanSegment(nil), segments...)
	}
	filtered := make([]backenddb.ValueLogRewritePlanSegment, 0, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		if !segmentPresence.ValueLogHasSegment(seg.FileID) {
			continue
		}
		filtered = append(filtered, seg)
	}
	return filtered
}

func vlogGenerationRewritePlanHasSelectionSignal(plan backenddb.ValueLogRewritePlan) bool {
	if len(plan.SelectedSegments) > 0 || len(plan.SourceFileIDs) > 0 {
		return true
	}
	return plan.SegmentsTotal > 0 ||
		plan.BytesTotal > 0 ||
		plan.BytesLive > 0 ||
		plan.BytesStale > 0 ||
		plan.AgeBlockedSegments > 0 ||
		plan.AgeBlockedBytesTotal > 0 ||
		plan.AgeBlockedBytesLive > 0 ||
		plan.AgeBlockedBytesStale > 0
}

func (db *DB) refreshQueuedVlogGenerationRewritePlan(
	planner backendValueLogRewritePlanner,
	sourceIDs []uint32,
	minStaleRatio float64,
	opts vlogGenerationMaintenanceOptions,
) (backenddb.ValueLogRewritePlan, error) {
	if db == nil || planner == nil || len(sourceIDs) == 0 {
		return backenddb.ValueLogRewritePlan{}, nil
	}
	ctx, cancel := db.vlogGenerationRewritePlanContext(30*time.Second, opts)
	defer cancel()
	planStart := time.Now()
	plan, err := planner.ValueLogRewritePlan(ctx, backenddb.ValueLogRewriteOnlineOptions{
		SourceFileIDs:        append([]uint32(nil), sourceIDs...),
		MinSegmentStaleRatio: minStaleRatio,
		MinSegmentStaleBytes: vlogGenerationRewriteMinSegmentStaleBytes,
		ProtectedPaths:       db.valueLogInUsePaths(),
	})
	db.debugVlogMaintf(
		"rewrite_queue_refresh min_ratio=%.6f min_stale_bytes=%d queued=%d selected=%d selected_bytes_total=%d selected_bytes_live=%d selected_bytes_stale=%d dur_ms=%.3f err=%v",
		minStaleRatio,
		vlogGenerationRewriteMinSegmentStaleBytes,
		len(sourceIDs),
		plan.SegmentsSelected,
		plan.SelectedBytesTotal,
		plan.SelectedBytesLive,
		plan.SelectedBytesStale,
		float64(time.Since(planStart).Microseconds())/1000,
		err,
	)
	db.observeVlogGenerationRewritePlanOutcome(plan, err)
	if err != nil {
		return backenddb.ValueLogRewritePlan{}, err
	}
	return db.filterVlogGenerationRewritePlanPenalties(plan, time.Now())
}

func (db *DB) shouldRefreshQueuedVlogGenerationRewriteLedger(
	opts vlogGenerationMaintenanceOptions,
	stagePending bool,
	queue []uint32,
	ledger []backenddb.ValueLogRewritePlanSegment,
) bool {
	if db == nil || stagePending || opts.bypassQuiet {
		return false
	}
	if len(queue) <= 1 || len(ledger) <= 1 {
		return false
	}
	return db.vlogGenerationLastRewriteUnixNano.Load() > 0
}

func shouldRefreshStagedVlogGenerationRewriteLedgerForConfirm(
	opts vlogGenerationMaintenanceOptions,
	stagePending bool,
	stageConfirmDue bool,
	queue []uint32,
) bool {
	if !stagePending || !stageConfirmDue || !vlogGenerationIsStageConfirmSource(opts) {
		return false
	}
	return len(queue) > 0
}

func shouldUsePersistedVlogGenerationRewriteLedgerForConfirm(
	opts vlogGenerationMaintenanceOptions,
	stagePending bool,
	stageConfirmDue bool,
	queue []uint32,
	ledger []backenddb.ValueLogRewritePlanSegment,
) bool {
	if !stagePending || !stageConfirmDue || !vlogGenerationIsStageConfirmSource(opts) {
		return false
	}
	return len(queue) > 0 && len(ledger) > 0
}

func shouldDeferVlogGenerationRewritePlanForAge(plan backenddb.ValueLogRewritePlan, minSegmentAge time.Duration) bool {
	if minSegmentAge <= 0 {
		return false
	}
	if len(plan.SourceFileIDs) > 0 {
		return false
	}
	if plan.AgeBlockedSegments <= 0 || plan.AgeBlockedMinRemainingAge <= 0 {
		return false
	}
	return plan.BytesStale > 0
}

func (db *DB) setVlogGenerationRewriteAgeBlockedUntil(deadline time.Time) {
	if db == nil {
		return
	}
	until := deadline.UnixNano()
	db.vlogGenerationRewriteAgeBlockedUntilNS.Store(until)
	wait := time.Until(deadline)
	if wait < 0 {
		wait = 0
	}
	if envBool(envDisableVlogGenerationDeferred) {
		return
	}
	db.wg.Add(1)
	go func(expectedUntil int64, delay time.Duration) {
		defer db.wg.Done()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-db.closeCh:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
			timer.Stop()
		}
		if db.closing.Load() {
			return
		}
		if db.vlogGenerationRewriteAgeBlockedUntilNS.Load() != expectedUntil {
			return
		}
		db.debugVlogMaintf(
			"rewrite_plan age_blocked_retry_due retry_after_ms=%d",
			delay.Milliseconds(),
		)
		db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        false,
			rewriteDebtDrain:      true,
			debugSource:           "rewrite_age_blocked",
		})
	}(until, wait)
}

func (db *DB) clearVlogGenerationRewriteAgeBlockedUntil() {
	if db == nil {
		return
	}
	db.vlogGenerationRewriteAgeBlockedUntilNS.Store(0)
}

func (db *DB) clearVlogGenerationRewriteStageConfirmation() {
	if db == nil {
		return
	}
	db.vlogGenerationRewriteStageWakeObservedNS.Store(0)
}

func (db *DB) scheduleVlogGenerationRewriteStageConfirmation(observedAt int64) {
	if db == nil || observedAt <= 0 || db.closing.Load() {
		return
	}
	if envBool(envDisableVlogGenerationDeferred) {
		return
	}
	if db.vlogGenerationRewriteStageWakeObservedNS.Load() == observedAt {
		return
	}
	db.vlogGenerationRewriteStageWakeObservedNS.Store(observedAt)
	dueAt := time.Unix(0, observedAt).Add(vlogGenerationRewriteMinInterval)
	delay := time.Until(dueAt)
	if delay < 0 {
		delay = 0
	}
	db.wg.Add(1)
	go func(expectedObservedAt int64, wait time.Duration) {
		defer db.wg.Done()
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-db.closeCh:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
			timer.Stop()
		}
		if db.closing.Load() {
			return
		}
		if db.vlogGenerationRewriteStageWakeObservedNS.Load() != expectedObservedAt {
			return
		}
		stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
		if err != nil || !stagePending || stageObservedAt != expectedObservedAt {
			return
		}
		db.debugVlogMaintf(
			"rewrite_plan stage_confirm_due observed_age_ms=%d",
			time.Since(time.Unix(0, expectedObservedAt)).Milliseconds(),
		)
		db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        false,
			rewriteDebtDrain:      true,
			debugSource:           "rewrite_stage_confirm",
		})
	}(observedAt, delay)
}

func (db *DB) vlogGenerationRewriteAgeBlockedDue(now time.Time) bool {
	if db == nil {
		return false
	}
	until := db.vlogGenerationRewriteAgeBlockedUntilNS.Load()
	return until > 0 && !now.Before(time.Unix(0, until))
}

type vlogGenerationMaintenanceOptions struct {
	bypassQuiet           bool
	skipRetainedPruneWait bool
	skipCheckpoint        bool
	rewriteDebtDrain      bool
	debugSource           string
}

func vlogGenerationMaintenanceDebugSource(opts vlogGenerationMaintenanceOptions) string {
	if opts.debugSource != "" {
		return opts.debugSource
	}
	if opts.bypassQuiet {
		return "bypass"
	}
	return "periodic"
}

func vlogGenerationIsStageConfirmSource(opts vlogGenerationMaintenanceOptions) bool {
	return opts.debugSource == "rewrite_stage_confirm" || opts.debugSource == "rewrite_stage_confirm_exit"
}

func vlogGenerationIsAgeBlockedSource(opts vlogGenerationMaintenanceOptions) bool {
	return opts.debugSource == "rewrite_age_blocked" || opts.debugSource == "rewrite_age_blocked_exit"
}

func (db *DB) maybeRunVlogGenerationMaintenance(runGC bool) {
	db.maybeRunVlogGenerationMaintenanceWithOptions(runGC, vlogGenerationMaintenanceOptions{})
}

func (db *DB) vlogGenerationRewriteStageConfirmDue(now time.Time) bool {
	if db == nil || db.closing.Load() {
		return false
	}
	stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
	if err != nil || !stagePending || stageObservedAt <= 0 {
		return false
	}
	return !now.Before(time.Unix(0, stageObservedAt).Add(vlogGenerationRewriteMinInterval))
}

func (db *DB) vlogGenerationDeferredMaintenanceDue(now time.Time) bool {
	if db == nil || db.closing.Load() {
		return false
	}
	if db.vlogGenerationRewriteAgeBlockedDue(now) {
		return true
	}
	return db.vlogGenerationRewriteStageConfirmDue(now)
}

func (db *DB) schedulePendingVlogGenerationCheckpointKick() {
	if db == nil || db.closing.Load() {
		return
	}
	if db.vlogGenerationDeferredMaintenanceDue(time.Now()) {
		db.scheduleDueVlogGenerationDeferredMaintenance()
		return
	}
	if !db.vlogGenerationCheckpointKickPending.CompareAndSwap(true, false) {
		return
	}
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		if db.closing.Load() {
			return
		}
		db.runVlogGenerationCheckpointKickRetries(vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			// This path retries a checkpoint-triggered kick that collided with an
			// already-active maintenance pass.
			skipCheckpoint:   false,
			rewriteDebtDrain: true,
			debugSource:      "checkpoint_pending",
		})
	}()
}

func (db *DB) startVlogGenerationDeferredMaintenance(opts vlogGenerationMaintenanceOptions) {
	if db == nil || db.closing.Load() {
		return
	}
	db.vlogGenerationDeferredMaintenancePending.Store(true)
	if !db.vlogGenerationDeferredMaintenanceRunning.CompareAndSwap(false, true) {
		return
	}
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		defer db.vlogGenerationDeferredMaintenanceRunning.Store(false)
		if db.closing.Load() {
			return
		}
		for !db.closing.Load() {
			if !db.vlogGenerationDeferredMaintenancePending.CompareAndSwap(true, false) {
				return
			}
			db.runVlogGenerationMaintenanceRetries(opts, vlogGenerationDeferredRetryWindow, true)
		}
	}()
}

func (db *DB) scheduleDueVlogGenerationDeferredMaintenance() {
	if db == nil || db.closing.Load() {
		return
	}
	now := time.Now()
	if db.vlogGenerationRewriteAgeBlockedDue(now) {
		db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        false,
			rewriteDebtDrain:      true,
			debugSource:           "rewrite_age_blocked_exit",
		})
		return
	}
	stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
	if err != nil || !stagePending || stageObservedAt <= 0 {
		return
	}
	if now.Before(time.Unix(0, stageObservedAt).Add(vlogGenerationRewriteMinInterval)) {
		return
	}
	db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm_exit",
	})
}

func (db *DB) runVlogGenerationCheckpointKickRetries(opts vlogGenerationMaintenanceOptions) {
	db.runVlogGenerationMaintenanceRetries(opts, vlogGenerationCheckpointKickRetryWindow, false)
}

func (db *DB) runVlogGenerationMaintenanceRetries(opts vlogGenerationMaintenanceOptions, retryWindow time.Duration, stopWhenAcquired bool) {
	if db == nil || db.closing.Load() {
		return
	}
	if retryWindow <= 0 {
		retryWindow = vlogGenerationCheckpointKickRetryWindow
	}
	attempt := 0
	if opts.debugSource != "" {
		db.debugVlogMaintf(
			"maintenance_retry_start source=%s retry_window_ms=%d checkpoint_pending=%t deferred_pending=%t active=%t",
			opts.debugSource,
			retryWindow.Milliseconds(),
			db.vlogGenerationCheckpointKickPending.Load(),
			db.vlogGenerationDeferredMaintenancePending.Load(),
			db.vlogGenerationMaintenanceActive.Load(),
		)
	}
	deadline := time.Now().Add(retryWindow)
	for !db.closing.Load() {
		attempt++
		if opts.debugSource != "" {
			db.debugVlogMaintf(
				"maintenance_retry_attempt source=%s attempt=%d checkpoint_pending=%t deferred_pending=%t active=%t",
				opts.debugSource,
				attempt,
				db.vlogGenerationCheckpointKickPending.Load(),
				db.vlogGenerationDeferredMaintenancePending.Load(),
				db.vlogGenerationMaintenanceActive.Load(),
			)
		}
		ran := db.maybeRunVlogGenerationMaintenanceWithOptions(true, opts)
		if stopWhenAcquired && ran {
			if opts.debugSource != "" {
				db.debugVlogMaintf(
					"maintenance_retry_done source=%s attempt=%d checkpoint_pending=%t deferred_pending=%t active=%t acquired=true",
					opts.debugSource,
					attempt,
					db.vlogGenerationCheckpointKickPending.Load(),
					db.vlogGenerationDeferredMaintenancePending.Load(),
					db.vlogGenerationMaintenanceActive.Load(),
				)
			}
			return
		}
		if !db.vlogGenerationCheckpointKickPending.Load() {
			if opts.debugSource != "" {
				db.debugVlogMaintf(
					"maintenance_retry_done source=%s attempt=%d checkpoint_pending=%t deferred_pending=%t active=%t",
					opts.debugSource,
					attempt,
					db.vlogGenerationCheckpointKickPending.Load(),
					db.vlogGenerationDeferredMaintenancePending.Load(),
					db.vlogGenerationMaintenanceActive.Load(),
				)
			}
			return
		}
		if time.Now().After(deadline) {
			if opts.debugSource != "" {
				db.debugVlogMaintf(
					"maintenance_retry_deadline source=%s attempt=%d checkpoint_pending=%t deferred_pending=%t active=%t",
					opts.debugSource,
					attempt,
					db.vlogGenerationCheckpointKickPending.Load(),
					db.vlogGenerationDeferredMaintenancePending.Load(),
					db.vlogGenerationMaintenanceActive.Load(),
				)
			}
			return
		}
		timer := time.NewTimer(10 * time.Millisecond)
		<-timer.C
		timer.Stop()
	}
}

func (db *DB) maybeRunVlogGenerationMaintenanceWithOptions(runGC bool, opts vlogGenerationMaintenanceOptions) (acquired bool) {
	if db == nil || db.closing.Load() || db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) {
		return
	}
	// In WAL-on mode, the periodic "runGC" tick must not enter the maintenance
	// engine at all. Checkpoint-coupled work belongs to the explicit
	// checkpoint-kick/deferred paths; letting the periodic GC tick even acquire
	// maintenanceActive can strand that slot behind hot restore-time locks.
	if runGC && !db.disableJournal && !opts.bypassQuiet {
		return
	}
	// Serialize generation maintenance passes. The periodic loop and
	// checkpoint-kick path can race otherwise, which causes overlapping rewrite
	// runs to compete on the same resume queue.
	if !db.vlogGenerationMaintenanceActive.CompareAndSwap(false, true) {
		// Checkpoint-kick retries are high-priority and quiet-window-bypassed by
		// design. If they collide with an active pass, queue exactly one retry to
		// run right after the active pass exits.
		if opts.bypassQuiet && !opts.skipCheckpoint {
			db.vlogGenerationCheckpointKickPending.Store(true)
		}
		if opts.debugSource != "" {
			db.debugVlogMaintf(
				"maintenance_retry_collision source=%s checkpoint_pending=%t deferred_pending=%t active=%t",
				opts.debugSource,
				db.vlogGenerationCheckpointKickPending.Load(),
				db.vlogGenerationDeferredMaintenancePending.Load(),
				db.vlogGenerationMaintenanceActive.Load(),
			)
		}
		return
	}
	acquired = true
	activeSource := vlogGenerationMaintenanceDebugSource(opts)
	activeStart := time.Now()
	db.debugVlogMaintf(
		"maintenance_active_acquire source=%s run_gc=%t bypass_quiet=%t skip_checkpoint=%t checkpoint_pending=%t deferred_pending=%t",
		activeSource,
		runGC,
		opts.bypassQuiet,
		opts.skipCheckpoint,
		db.vlogGenerationCheckpointKickPending.Load(),
		db.vlogGenerationDeferredMaintenancePending.Load(),
	)
	defer func() {
		db.debugVlogMaintf(
			"maintenance_active_release source=%s dur_ms=%d checkpoint_pending=%t deferred_pending=%t",
			activeSource,
			time.Since(activeStart).Milliseconds(),
			db.vlogGenerationCheckpointKickPending.Load(),
			db.vlogGenerationDeferredMaintenancePending.Load(),
		)
		db.vlogGenerationMaintenanceActive.Store(false)
		// If a deferred confirmation/age wake became due while this pass held the
		// scheduler active, requeue it immediately on exit instead of relying on
		// the original retry goroutine to still be alive.
		db.scheduleDueVlogGenerationDeferredMaintenance()
		db.schedulePendingVlogGenerationCheckpointKick()
	}()
	now := time.Now()
	quiet := db.foregroundActivityQuietFor(now, vlogGenerationMaintenanceQuietWindow, vlogForegroundReadQuietWindow)
	rewriteQueue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
		if db.notifyError != nil {
			db.notifyError(fmt.Errorf("cachingdb: load generational rewrite queue: %w", err))
		}
		return
	}
	if len(rewriteQueue) > 0 {
		prunedQueue, dropped, pruneErr := db.pruneVlogGenerationRewriteLedgerNonPositiveLive()
		if pruneErr != nil {
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
			if db.notifyError != nil {
				db.notifyError(fmt.Errorf("cachingdb: prune generational rewrite queue: %w", pruneErr))
			}
			return
		}
		if dropped > 0 {
			rewriteQueue = prunedQueue
			db.observeVlogGenerationRewriteQueuePrune(dropped)
			db.debugVlogMaintf("rewrite_queue_prune dropped=%d remaining=%d", dropped, len(rewriteQueue))
		}
	}
	stagePending, stageObservedAt, stageErr := db.currentVlogGenerationRewriteStage()
	if stageErr != nil {
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
		if db.notifyError != nil {
			db.notifyError(fmt.Errorf("cachingdb: load generational rewrite stage: %w", stageErr))
		}
		return
	}
	ageBlockedDue := db.vlogGenerationRewriteAgeBlockedDue(now)
	stageConfirmDue := db.vlogGenerationRewriteStageConfirmDue(now)
	if stagePending {
		if !stageConfirmDue {
			// Once debt is staged, do not let generic periodic or checkpoint-kick
			// passes spend long maintenance windows before the confirmation delay
			// has elapsed. The only valid next step is to wait for confirmation.
			if !vlogGenerationIsStageConfirmSource(opts) {
				return
			}
		} else if !vlogGenerationIsStageConfirmSource(opts) {
			// When confirmation becomes due, reserve the maintenance slot for the
			// explicit stage-confirm wake instead of letting generic retries or
			// periodic passes reacquire it first.
			return
		}
	}
	if !stagePending && ageBlockedDue && !vlogGenerationIsAgeBlockedSource(opts) {
		return
	}
	// Checkpoint-collision retries and timer-driven confirmation wakes should run
	// ahead of generic periodic passes. This avoids repeated active-pass
	// collisions where periodic maintenance keeps reacquiring the scheduler while
	// the higher-priority retry is still trying to run.
	if !opts.bypassQuiet && (db.vlogGenerationCheckpointKickPending.Load() || db.vlogGenerationDeferredMaintenancePending.Load()) {
		return
	}
	// Explicit GC runs bypass the foreground quiet-window gate so callers can
	// force a safety/cleanup pass even while foreground activity is ongoing.
	if !runGC && !opts.bypassQuiet && !quiet {
		return
	}
	// In WAL-off mode, do not start rewrite/GC planning before the first
	// explicit checkpoint. During snapshot restore, the app can issue large
	// volumes of writes while local height is still 0. Background value-log
	// maintenance at that stage competes with import/publication work and has
	// caused real restore stalls. Keep WAL-on profiles eligible for maintenance
	// before the first checkpoint; starving that path causes the main value-log
	// lane to grow unchecked during restore.
	if db.disableJournal && db.checkpointRuns.Load() == 0 && !runGC && len(rewriteQueue) == 0 && !opts.skipCheckpoint {
		return
	}
	// Retained-prune and generation maintenance use the same foreground quiet-window gate.
	// That means a scheduled-but-not-yet-running prune can only delay this path by at most
	// foregroundMaintenancePollInterval(), not by the full quiet window. Waiting here keeps
	// active retained-prune scans serialized with rewrite/GC planning once the system has gone quiet.
	if !opts.skipRetainedPruneWait {
		db.waitForRetainedValueLogPrune()
	}
	if db.checkpointing.Load() {
		if opts.bypassQuiet && !opts.skipCheckpoint {
			db.checkpointMu.Lock()
			for db.checkpointing.Load() && !db.closing.Load() {
				db.checkpointCond.Wait()
			}
			db.checkpointMu.Unlock()
			if db.closing.Load() {
				return
			}
		} else {
			return
		}
	}
	if db.checkpointing.Load() {
		return
	}
	now = time.Now()
	db.vlogGenerationAccrueRewriteBudget(now)
	queueLen := 0
	if view := db.memtables.Load(); view != nil {
		queueLen = len(view.queue)
	}

	retained := db.valueLogRetainedStatsDetailed()
	totalBytes := retained.BytesTotal
	if totalBytes < 0 {
		totalBytes = 0
	}
	reclaimable := db.reclaimableWALBytes()
	if reclaimable < 0 {
		reclaimable = 0
	}
	staleRatioPPM := uint32(0)
	if totalBytes > 0 {
		staleRatioPPM = uint32((reclaimable * 1_000_000) / totalBytes)
	}
	churnBps := db.vlogGenerationEstimateChurnBps()

	var rewriteLedger []backenddb.ValueLogRewritePlanSegment
	if len(rewriteQueue) > 1 {
		rewriteLedger, _ = db.currentVlogGenerationRewriteLedger()
	}
	shouldRewrite, reason := db.shouldRunVlogGenerationRewrite(totalBytes, staleRatioPPM, churnBps)
	var rewritePlan backenddb.ValueLogRewritePlan
	haveRewritePlan := false
	planner, hasPlanner := db.backend.(backendValueLogRewritePlanner)
	segmentPresence, hasSegmentPresence := db.backend.(backendValueLogSegmentPresence)
	if hasSegmentPresence && len(rewriteLedger) > 0 {
		filteredLedger := filterVlogGenerationRewriteLedgerToExistingSegments(rewriteLedger, segmentPresence)
		if dropped := len(rewriteLedger) - len(filteredLedger); dropped > 0 {
			nextStagePending := stagePending
			nextStageObservedAt := stageObservedAt
			if len(filteredLedger) == 0 {
				nextStagePending = false
				nextStageObservedAt = 0
			}
			if err := db.setVlogGenerationRewriteLedgerWithStage(filteredLedger, nextStagePending, nextStageObservedAt); err != nil {
				db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("cachingdb: prune missing generational rewrite queue segments: %w", err))
				}
				return
			}
			db.observeVlogGenerationRewriteQueuePrune(dropped)
			rewriteLedger = filteredLedger
			rewriteQueue = vlogGenerationRewriteLedgerIDs(filteredLedger)
			stagePending = nextStagePending
			stageObservedAt = nextStageObservedAt
			db.debugVlogMaintf(
				"rewrite_queue_missing_prune dropped=%d remaining=%d stage_pending=%t",
				dropped,
				len(rewriteQueue),
				stagePending,
			)
		}
	}
	stageConfirmUsePersistedLedger := shouldUsePersistedVlogGenerationRewriteLedgerForConfirm(opts, stagePending, stageConfirmDue, rewriteQueue, rewriteLedger)
	stageConfirmRefresh := hasPlanner && !stageConfirmUsePersistedLedger && shouldRefreshStagedVlogGenerationRewriteLedgerForConfirm(opts, stagePending, stageConfirmDue, rewriteQueue)
	skipStageConfirmSparsePlan := false
	stageConfirmReady := false
	if stageConfirmUsePersistedLedger {
		stageConfirmReady = true
		skipStageConfirmSparsePlan = true
		db.debugVlogMaintf(
			"rewrite_stage_confirm_use_persisted queued=%d ledger=%d",
			len(rewriteQueue),
			len(rewriteLedger),
		)
	}
	if hasPlanner && (db.shouldRefreshQueuedVlogGenerationRewriteLedger(opts, stagePending, rewriteQueue, rewriteLedger) || stageConfirmRefresh) {
		queueMinStaleRatio := db.vlogGenerationRewriteMinStaleRatioForQueuedDebt(totalBytes, vlogGenerationReasonRewriteResume)
		refreshedPlan, refreshErr := db.refreshQueuedVlogGenerationRewritePlan(planner, rewriteQueue, queueMinStaleRatio, opts)
		switch {
		case refreshErr != nil:
			if stageConfirmRefresh {
				skipStageConfirmSparsePlan = true
				shouldRewrite = false
				reason = vlogGenerationReasonNone
				haveRewritePlan = false
			}
			if !isVlogGenerationPlannerCanceled(refreshErr) && db.notifyError != nil {
				db.notifyError(fmt.Errorf("cachingdb: refresh generational rewrite queue: %w", refreshErr))
			}
		case !vlogGenerationRewritePlanHasSelectionSignal(refreshedPlan):
			// No useful signal; keep the persisted queue/ledger as-is.
		case len(refreshedPlan.SelectedSegments) == 0:
			if stageConfirmRefresh {
				// Stage-confirm passes are allowed to observe that staged debt is
				// still not executable (for example an empty rewrite budget) without
				// consuming or dropping the staged queue. Keep the persisted
				// queue/ledger intact so a later confirmation can retry.
				skipStageConfirmSparsePlan = true
				db.debugVlogMaintf(
					"rewrite_queue_refresh_stage_keep queued=%d min_ratio=%.6f min_stale_bytes=%d",
					len(rewriteQueue),
					queueMinStaleRatio,
					vlogGenerationRewriteMinSegmentStaleBytes,
				)
				break
			}
			dropped := len(rewriteQueue)
			if err := db.setVlogGenerationRewriteLedger(nil); err != nil {
				db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("cachingdb: clear refreshed generational rewrite ledger: %w", err))
				}
				return
			}
			if dropped > 0 {
				db.observeVlogGenerationRewriteQueuePrune(dropped)
			}
			rewriteQueue = nil
			rewriteLedger = nil
			if stageConfirmRefresh {
				stagePending = false
				skipStageConfirmSparsePlan = true
			}
			db.debugVlogMaintf(
				"rewrite_queue_refresh_prune dropped=%d remaining=0 min_ratio=%.6f min_stale_bytes=%d",
				dropped,
				queueMinStaleRatio,
				vlogGenerationRewriteMinSegmentStaleBytes,
			)
		default:
			persistStagePending := false
			persistStageObservedAt := int64(0)
			if stageConfirmRefresh {
				// Keep the persisted staged state until this pass actually executes;
				// confirmation finding an executable chunk is not the same thing as
				// successfully spending it.
				persistStagePending = true
				persistStageObservedAt = stageObservedAt
				if persistStageObservedAt <= 0 {
					persistStageObservedAt = now.UnixNano()
				}
			}
			if err := db.setVlogGenerationRewriteLedgerWithStage(refreshedPlan.SelectedSegments, persistStagePending, persistStageObservedAt); err != nil {
				db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("cachingdb: persist refreshed generational rewrite ledger: %w", err))
				}
				return
			}
			if dropped := len(rewriteQueue) - len(refreshedPlan.SourceFileIDs); dropped > 0 {
				db.observeVlogGenerationRewriteQueuePrune(dropped)
				db.debugVlogMaintf(
					"rewrite_queue_refresh_prune dropped=%d remaining=%d min_ratio=%.6f min_stale_bytes=%d",
					dropped,
					len(refreshedPlan.SourceFileIDs),
					queueMinStaleRatio,
					vlogGenerationRewriteMinSegmentStaleBytes,
				)
			}
			rewriteQueue = append(rewriteQueue[:0], refreshedPlan.SourceFileIDs...)
			rewriteLedger = refreshedPlan.SelectedSegments
			if stageConfirmRefresh {
				skipStageConfirmSparsePlan = true
				stageConfirmReady = true
			}
		}
	}
	allowCheckpointKickBypass := opts.bypassQuiet && !opts.skipCheckpoint
	hasExecutableRewriteQueue := len(rewriteQueue) > 0 && (!stagePending || stageConfirmReady)
	allowCheckpointKickRetry := allowCheckpointKickBypass && hasExecutableRewriteQueue
	ageBlockedRetryDue := len(rewriteQueue) == 0 && db.vlogGenerationRewriteAgeBlockedDue(now)
	if hasExecutableRewriteQueue {
		shouldRewrite = true
		reason = vlogGenerationReasonRewriteResume
	}
	rewriteCancelBackoff := hasExecutableRewriteQueue && db.vlogGenerationRewriteCancelBackoffActive(now) && !allowCheckpointKickRetry
	if rewriteCancelBackoff {
		shouldRewrite = false
		reason = vlogGenerationReasonNone
	}
	planBackoff := !hasExecutableRewriteQueue && db.vlogGenerationRewritePlanBackoffActive(now) && !allowCheckpointKickBypass && !ageBlockedRetryDue
	if planBackoff {
		shouldRewrite = false
		reason = vlogGenerationReasonNone
	}
	ineffectiveBackoff := !hasExecutableRewriteQueue && db.vlogGenerationRewriteIneffectiveBackoffActive(now) && !allowCheckpointKickBypass && !ageBlockedRetryDue
	if ineffectiveBackoff {
		shouldRewrite = false
		reason = vlogGenerationReasonNone
	}
	// Periodic "runGC" passes can run during foreground activity, but rewrite
	// planning/execution should remain quiet-window-bound unless explicitly
	// bypassed (checkpoint-kick).
	if !quiet && !opts.bypassQuiet {
		shouldRewrite = false
		reason = vlogGenerationReasonNone
	}
	// Stale-ratio trigger: use a sparse rewrite plan (live-byte estimate) to
	// detect when any segments are meaningfully stale. This avoids relying on
	// reclaimable-WAL heuristics (which can be 0 in split-value-log mode).
	if (len(rewriteQueue) == 0 || stagePending) && !shouldRewrite && hasPlanner && db.valueLogRewriteTriggerRatioPPM > 0 && !skipStageConfirmSparsePlan {
		if planBackoff {
			goto planned
		}
		if ineffectiveBackoff {
			goto planned
		}
		if !quiet && !opts.bypassQuiet && !ageBlockedRetryDue {
			goto planned
		}
		lastPlan := db.vlogGenerationLastRewritePlanUnixNano.Load()
		if lastPlan > 0 && !ageBlockedRetryDue {
			lastAt := time.Unix(0, lastPlan)
			if now.Sub(lastAt) < vlogGenerationRewriteMinInterval {
				goto planned
			}
		}
		// Avoid planning work before we have at least one full hot segment worth
		// of data; early on this is almost always pure inserts with no stale
		// bytes, and planning would just scan the tree.
		if db.valueLogGenerationHotTarget > 0 && totalBytes < db.valueLogGenerationHotTarget && !ageBlockedRetryDue {
			goto planned
		}
		maxSourceBytes := db.vlogGenerationRewriteBudgetTokensBytes.Load()
		if maxSourceBytes < 0 {
			maxSourceBytes = 0
		}
		// ValueLogRewriteOnline treats MaxSourceBytes=0 as "unbounded". When the
		// configured rewrite token bucket is enabled and currently empty, skip the
		// sparse planning pass instead of issuing an unbounded plan. Keep staged
		// debt eligible for a confirmation pass even with an empty bucket: the
		// planner pass is cheap compared with execution, and otherwise staged debt
		// can get stuck pending forever.
		if maxSourceBytes == 0 && db.vlogGenerationRewriteBudgetEnabled() && !stagePending {
			goto planned
		}
		if maxSourceBytes > 0 && totalBytes > 0 && maxSourceBytes > totalBytes {
			maxSourceBytes = totalBytes
		}
		ctx, cancel := db.vlogGenerationRewritePlanContext(30*time.Second, opts)
		minStaleRatio := db.vlogGenerationRewriteMinStaleRatioForStaleRatioTrigger(totalBytes)
		if minStaleRatio <= 0 {
			cancel()
			goto planned
		}
		planOpts := backenddb.ValueLogRewriteOnlineOptions{
			MaxSourceSegments:    0,
			MaxSourceBytes:       maxSourceBytes,
			MinSegmentStaleRatio: minStaleRatio,
			MinSegmentStaleBytes: 1,
			MinSegmentAge:        vlogGenerationRewriteMinSegmentAge,
		}
		planStart := time.Now()
		plan, err := planner.ValueLogRewritePlan(ctx, planOpts)
		cancel()
		db.debugVlogMaintf(
			"rewrite_plan stale_ratio_trigger min_ratio=%.6f max_source_bytes=%d selected=%d/%d selected_bytes_total=%d selected_bytes_live=%d selected_bytes_stale=%d total_bytes=%d live_bytes=%d stale_bytes=%d dur_ms=%.3f err=%v",
			minStaleRatio,
			maxSourceBytes,
			plan.SegmentsSelected,
			plan.SegmentsTotal,
			plan.SelectedBytesTotal,
			plan.SelectedBytesLive,
			plan.SelectedBytesStale,
			plan.BytesTotal,
			plan.BytesLive,
			plan.BytesStale,
			float64(time.Since(planStart).Microseconds())/1000,
			err,
		)
		db.observeVlogGenerationRewritePlanOutcome(plan, err)
		updatePlanTimestamp := false
		if err != nil {
			db.clearVlogGenerationRewriteAgeBlockedUntil()
			if isVlogGenerationPlannerCanceled(err) {
				db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
				// Foreground activity resumed while planning. Skip rewrite this
				// cycle, but continue to the GC path below instead of aborting
				// maintenance entirely.
				shouldRewrite = false
				haveRewritePlan = false
			}
			if !isVlogGenerationPlannerCanceled(err) {
				updatePlanTimestamp = true
				// Best-effort planning: keep legacy triggers (total-bytes/churn) alive,
				// but surface the failure for observability.
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("cachingdb: generational rewrite plan: %w", err))
				}
			}
		} else if len(plan.SourceFileIDs) > 0 {
			db.clearVlogGenerationRewriteAgeBlockedUntil()
			plan, err = db.filterVlogGenerationRewritePlanPenalties(plan, now)
			if err != nil {
				db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("cachingdb: filter generational rewrite penalties: %w", err))
				}
				return
			}
			updatePlanTimestamp = true
			if len(plan.SourceFileIDs) > 0 {
				if stagePending {
					stagedLedger, ledgerErr := db.currentVlogGenerationRewriteLedger()
					if ledgerErr != nil {
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
						if db.notifyError != nil {
							db.notifyError(fmt.Errorf("cachingdb: load staged generational rewrite ledger: %w", ledgerErr))
						}
						return
					}
					confirmed := stableVlogGenerationRewriteLedgerSegments(stagedLedger, plan.SelectedSegments)
					if len(confirmed) > 0 {
						plan = filterVlogGenerationRewritePlanToSegments(plan, confirmed)
						shouldRewrite = true
						reason = vlogGenerationReasonRewriteResume
					} else {
						reason = vlogGenerationReasonStaleRatio
					}
				} else {
					shouldRewrite = true
					reason = vlogGenerationReasonStaleRatio
				}
				rewritePlan = plan
				haveRewritePlan = true
			}
		} else if shouldDeferVlogGenerationRewritePlanForAge(plan, planOpts.MinSegmentAge) {
			db.setVlogGenerationRewriteAgeBlockedUntil(now.Add(plan.AgeBlockedMinRemainingAge))
			db.debugVlogMaintf(
				"rewrite_plan stale_ratio_trigger age_blocked segments=%d stale_bytes=%d retry_after_ms=%d min_age_ms=%d",
				plan.AgeBlockedSegments,
				plan.AgeBlockedBytesStale,
				plan.AgeBlockedMinRemainingAge.Milliseconds(),
				planOpts.MinSegmentAge.Milliseconds(),
			)
		} else {
			db.clearVlogGenerationRewriteAgeBlockedUntil()
			updatePlanTimestamp = true
		}
		if updatePlanTimestamp {
			db.vlogGenerationLastRewritePlanUnixNano.Store(now.UnixNano())
		}
	}
planned:
	if shouldRewrite && envBool(envDisableVlogGenerationRewrite) {
		shouldRewrite = false
	}
	rewriter, hasRewriter := db.backend.(backendValueLogRewriter)
	rewriteMinIntervalBlocked := false
	if shouldRewrite && hasRewriter {
		last := db.vlogGenerationLastRewriteUnixNano.Load()
		minInterval := vlogGenerationRewriteMinInterval
		if len(rewriteQueue) > 0 {
			minInterval = vlogGenerationRewriteResumeMinInterval
		}
		if last > 0 {
			lastAt := time.Unix(0, last)
			if now.Sub(lastAt) < minInterval && !allowCheckpointKickRetry {
				shouldRewrite = false
				rewriteMinIntervalBlocked = true
			}
		}
	}
	if len(rewriteQueue) == 0 && shouldRewrite && hasRewriter && !haveRewritePlan && hasPlanner {
		maxSourceBytes := db.vlogGenerationRewriteBudgetTokensBytes.Load()
		if maxSourceBytes < 0 {
			maxSourceBytes = 0
		}
		// ValueLogRewriteOnline interprets MaxSourceBytes=0 as "no limit". In
		// token-bucket mode, an empty bucket means "wait", not "run an unbounded
		// rewrite".
		if maxSourceBytes == 0 && db.vlogGenerationRewriteBudgetEnabled() {
			shouldRewrite = false
		} else {
			if maxSourceBytes > 0 && totalBytes > 0 && maxSourceBytes > totalBytes {
				maxSourceBytes = totalBytes
			}
			if maxSourceBytes > 0 {
				ctx, cancel := db.vlogGenerationRewritePlanContext(30*time.Second, opts)
				planStart := time.Now()
				minStaleRatio := db.vlogGenerationRewriteMinStaleRatioForGenericPass(totalBytes)
				plan, err := planner.ValueLogRewritePlan(ctx, backenddb.ValueLogRewriteOnlineOptions{
					MaxSourceSegments:    0,
					MaxSourceBytes:       maxSourceBytes,
					MinSegmentStaleRatio: minStaleRatio,
					MinSegmentStaleBytes: vlogGenerationRewriteMinSegmentStaleBytes,
					MinSegmentAge:        vlogGenerationRewriteMinSegmentAge,
				})
				cancel()
				db.debugVlogMaintf(
					"rewrite_plan pre_rewrite max_source_bytes=%d min_ratio=%.6f min_stale_bytes=%d selected=%d/%d selected_bytes_total=%d selected_bytes_live=%d selected_bytes_stale=%d total_bytes=%d live_bytes=%d stale_bytes=%d dur_ms=%.3f err=%v",
					maxSourceBytes,
					minStaleRatio,
					vlogGenerationRewriteMinSegmentStaleBytes,
					plan.SegmentsSelected,
					plan.SegmentsTotal,
					plan.SelectedBytesTotal,
					plan.SelectedBytesLive,
					plan.SelectedBytesStale,
					plan.BytesTotal,
					plan.BytesLive,
					plan.BytesStale,
					float64(time.Since(planStart).Microseconds())/1000,
					err,
				)
				db.observeVlogGenerationRewritePlanOutcome(plan, err)
				if err != nil {
					db.clearVlogGenerationRewriteAgeBlockedUntil()
					if isVlogGenerationPlannerCanceled(err) {
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
						// Foreground activity resumed while planning. Skip rewrite
						// this cycle, but still allow GC to run below.
						shouldRewrite = false
						haveRewritePlan = false
					}
					if !isVlogGenerationPlannerCanceled(err) {
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
						db.vlogGenerationRemapFailures.Add(1)
						if db.notifyError != nil {
							db.notifyError(fmt.Errorf("cachingdb: generational rewrite plan: %w", err))
						}
						return
					}
				}
				if len(plan.SourceFileIDs) > 0 {
					db.clearVlogGenerationRewriteAgeBlockedUntil()
					plan, err = db.filterVlogGenerationRewritePlanPenalties(plan, now)
					if err != nil {
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
						db.vlogGenerationRemapFailures.Add(1)
						if db.notifyError != nil {
							db.notifyError(fmt.Errorf("cachingdb: filter generational rewrite penalties: %w", err))
						}
						return
					}
				}
				if len(plan.SourceFileIDs) == 0 {
					if shouldDeferVlogGenerationRewritePlanForAge(plan, vlogGenerationRewriteMinSegmentAge) {
						db.setVlogGenerationRewriteAgeBlockedUntil(now.Add(plan.AgeBlockedMinRemainingAge))
						db.debugVlogMaintf(
							"rewrite_plan pre_rewrite age_blocked segments=%d stale_bytes=%d retry_after_ms=%d min_age_ms=%d",
							plan.AgeBlockedSegments,
							plan.AgeBlockedBytesStale,
							plan.AgeBlockedMinRemainingAge.Milliseconds(),
							vlogGenerationRewriteMinSegmentAge.Milliseconds(),
						)
					} else {
						db.clearVlogGenerationRewriteAgeBlockedUntil()
					}
					shouldRewrite = false
				} else {
					rewritePlan = plan
					haveRewritePlan = true
				}
			}
		}
	}
	if !shouldRewrite && hasRewriter {
		db.debugVlogMaintf(
			"rewrite_skip reason=%s quiet=%t queue_len=%d plan_backoff=%t cancel_backoff=%t ineffective_backoff=%t min_interval_blocked=%t run_gc=%t bypass_quiet=%t total_bytes=%d stale_ratio_ppm=%d churn_bps=%d checkpoint_runs=%d disable_journal=%t",
			vlogGenerationReasonString(reason),
			quiet,
			len(rewriteQueue),
			planBackoff,
			rewriteCancelBackoff,
			ineffectiveBackoff,
			rewriteMinIntervalBlocked,
			runGC,
			opts.bypassQuiet,
			totalBytes,
			staleRatioPPM,
			churnBps,
			db.checkpointRuns.Load(),
			db.disableJournal,
		)
	}
	if shouldRewrite && hasRewriter {
		db.debugVlogMaintf(
			"rewrite_start reason=%s total_bytes=%d stale_ratio_ppm=%d churn_bps=%d rewrite_queue=%d checkpoint_runs=%d disable_journal=%t have_plan=%t",
			vlogGenerationReasonString(reason),
			totalBytes,
			staleRatioPPM,
			churnBps,
			len(rewriteQueue),
			db.checkpointRuns.Load(),
			db.disableJournal,
			haveRewritePlan,
		)
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerRunning)
		db.vlogGenerationLastReason.Store(reason)
		err := db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{
			skipCheckpoint:        opts.skipCheckpoint,
			skipRetainedPruneWait: opts.skipRetainedPruneWait,
		}, func() error {
			now := time.Now()
			db.vlogGenerationLastRewriteUnixNano.Store(now.UnixNano())
			maxSourceBytes := int64(0)
			if len(rewriteQueue) == 0 && !haveRewritePlan {
				maxSourceBytes = db.vlogGenerationRewriteBudgetTokensBytes.Load()
				if maxSourceBytes < 0 {
					maxSourceBytes = 0
				}
				// ValueLogRewriteOnline interprets MaxSourceBytes=0 as "no limit". In
				// token-bucket mode, an empty bucket means "wait", not "run an
				// unbounded rewrite".
				if maxSourceBytes == 0 && db.vlogGenerationRewriteBudgetEnabled() {
					db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
					return nil
				}
				if maxSourceBytes > 0 && totalBytes > 0 && maxSourceBytes > totalBytes {
					maxSourceBytes = totalBytes
				}
			}
			rewriteOpts := backenddb.ValueLogRewriteOnlineOptions{
				BatchSize:       db.valueLogRewriteBatchSize(),
				SyncEachBatch:   false,
				MaxSegmentBytes: db.valueLogGenerationWarmTarget,
				ProtectedPaths:  db.valueLogProtectedPaths(),
				ReserveRIDs: func(count int) (uint64, error) {
					if count <= 0 {
						return 0, nil
					}
					end := db.nextRID.Add(uint64(count))
					start := end - uint64(count) + 1
					if start == 0 || end < start {
						return 0, fmt.Errorf("value-log rid space exhausted")
					}
					return start, nil
				},
			}
			processedRewriteIDs := []uint32(nil)
			processedLedgerTotalBytes := int64(0)
			processedLedgerLiveBytes := int64(0)
			processedLedgerStaleBytes := int64(0)
			processedLedgerOK := false
			budgetTokens := int64(0)
			if db.vlogGenerationRewriteBudgetEnabled() {
				budgetTokens = db.vlogGenerationRewriteBudgetTokensBytes.Load()
				// If a queued rewrite is pending, do not run it while the bucket is
				// empty; that defeats the whole point of a bounded executor.
				if budgetTokens <= 0 && len(rewriteQueue) > 0 {
					db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
					return nil
				}
			}
			hadRewriteQueue := len(rewriteQueue) > 0
			rewriteMaxSegments := vlogGenerationRewriteResumeMaxSegments
			if hadRewriteQueue {
				rewriteMaxSegments = db.vlogGenerationRewriteMaxSegmentsForRun(len(rewriteQueue), budgetTokens, opts)
			}
			if len(rewriteQueue) > 0 {
				ledger, _ := db.currentVlogGenerationRewriteLedger()
				if len(ledger) > 0 {
					queueMinStaleRatio := db.vlogGenerationRewriteMinStaleRatioForQueuedDebt(totalBytes, reason)
					filteredLedger := filterVlogGenerationRewriteLedgerByQuality(ledger, queueMinStaleRatio, vlogGenerationRewriteMinSegmentStaleBytes)
					if dropped := len(ledger) - len(filteredLedger); dropped > 0 {
						if err := db.setVlogGenerationRewriteLedger(filteredLedger); err != nil {
							return fmt.Errorf("persist filtered generational rewrite ledger: %w", err)
						}
						db.observeVlogGenerationRewriteQueuePrune(dropped)
						rewriteQueue = append(rewriteQueue[:0], vlogGenerationRewriteLedgerIDs(filteredLedger)...)
						ledger = filteredLedger
						db.debugVlogMaintf(
							"rewrite_queue_quality_prune dropped=%d remaining=%d min_ratio=%.6f min_stale_bytes=%d",
							dropped,
							len(ledger),
							queueMinStaleRatio,
							vlogGenerationRewriteMinSegmentStaleBytes,
						)
					}
					processedRewriteIDs = vlogGenerationRewriteLedgerChunk(ledger, rewriteMaxSegments, budgetTokens)
					if len(processedRewriteIDs) == 0 {
						prunedQueue, dropped, pruneErr := db.pruneVlogGenerationRewriteLedgerNonPositiveLive()
						if pruneErr != nil {
							return fmt.Errorf("prune generational rewrite ledger: %w", pruneErr)
						}
						if dropped > 0 {
							db.observeVlogGenerationRewriteQueuePrune(dropped)
							rewriteQueue = prunedQueue
							db.debugVlogMaintf("rewrite_skip reason=%s dropped_zero_live=%d queue_len=%d", vlogGenerationReasonString(reason), dropped, len(rewriteQueue))
						}
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
						return nil
					}
				} else {
					processedRewriteIDs = vlogGenerationRewriteQueueChunk(rewriteQueue, rewriteMaxSegments)
				}
			} else if haveRewritePlan {
				plannedLedgerForExec := rewritePlan.SelectedSegments
				if reason == vlogGenerationReasonStaleRatio && len(rewritePlan.SelectedSegments) > 0 {
					prevLedger, _ := db.currentVlogGenerationRewriteLedger()
					plannedLedgerForExec = stableVlogGenerationRewriteLedgerSegments(prevLedger, rewritePlan.SelectedSegments)
				}
				if len(rewritePlan.SelectedSegments) > 0 {
					stageLedger := reason == vlogGenerationReasonStaleRatio && len(plannedLedgerForExec) == 0
					stageObservedAt := int64(0)
					if stageLedger {
						stageObservedAt = now.UnixNano()
					}
					if err := db.setVlogGenerationRewriteLedgerWithStage(rewritePlan.SelectedSegments, stageLedger, stageObservedAt); err != nil {
						return fmt.Errorf("persist generational rewrite ledger: %w", err)
					}
				} else {
					if err := db.setVlogGenerationRewriteQueue(rewritePlan.SourceFileIDs); err != nil {
						return fmt.Errorf("persist generational rewrite queue: %w", err)
					}
				}
				rewriteQueue = append([]uint32(nil), rewritePlan.SourceFileIDs...)
				// Do not debt-drain freshly planned work in the same pass; only apply
				// multi-segment debt-drain to explicit resume queues.
				rewriteMaxSegments = vlogGenerationRewriteResumeMaxSegments
				// If the token bucket is enabled and empty, persist the plan/ledger but
				// skip running the rewrite until we have budget to spend.
				if db.vlogGenerationRewriteBudgetEnabled() && budgetTokens <= 0 {
					db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
					return nil
				}
				if len(rewritePlan.SelectedSegments) > 0 {
					if reason == vlogGenerationReasonStaleRatio && len(plannedLedgerForExec) == 0 {
						db.debugVlogMaintf(
							"rewrite_plan staged reason=%s selected=%d observed_once_only=1 queue_len=%d",
							vlogGenerationReasonString(reason),
							len(rewritePlan.SelectedSegments),
							len(rewriteQueue),
						)
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
						return nil
					}
					processedRewriteIDs = vlogGenerationRewriteLedgerChunk(plannedLedgerForExec, rewriteMaxSegments, budgetTokens)
					if len(processedRewriteIDs) == 0 {
						prunedQueue, dropped, pruneErr := db.pruneVlogGenerationRewriteLedgerNonPositiveLive()
						if pruneErr != nil {
							return fmt.Errorf("prune generational rewrite plan ledger: %w", pruneErr)
						}
						if dropped > 0 {
							db.observeVlogGenerationRewriteQueuePrune(dropped)
							rewriteQueue = prunedQueue
							db.debugVlogMaintf("rewrite_skip reason=%s planned_zero_live=%d queue_len=%d", vlogGenerationReasonString(reason), dropped, len(rewriteQueue))
						}
						db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
						return nil
					}
				} else {
					processedRewriteIDs = vlogGenerationRewriteQueueChunk(rewriteQueue, rewriteMaxSegments)
				}
			}
			if len(processedRewriteIDs) > 0 {
				ledger, _ := db.currentVlogGenerationRewriteLedger()
				processedLedgerTotalBytes, processedLedgerLiveBytes, processedLedgerStaleBytes, processedLedgerOK = sumVlogRewritePlanBytes(ledger, processedRewriteIDs)
				rewriteOpts.SourceFileIDs = processedRewriteIDs
			} else {
				rewriteOpts.MaxSourceSegments = 0
				rewriteOpts.MaxSourceBytes = maxSourceBytes
				rewriteOpts.MinSegmentStaleRatio = db.vlogGenerationRewriteMinStaleRatioForGenericPass(totalBytes)
				rewriteOpts.MinSegmentStaleBytes = vlogGenerationRewriteMinSegmentStaleBytes
				rewriteOpts.MinSegmentAge = vlogGenerationRewriteMinSegmentAge
			}
			var ctx context.Context
			var cancel context.CancelFunc
			if hadRewriteQueue && len(processedRewriteIDs) > 0 {
				ctx, cancel = context.WithTimeout(context.Background(), vlogGenerationRewriteBoundedExecTimeout)
			} else {
				ctx, cancel = db.foregroundMaintenanceContext(2 * time.Minute)
			}
			db.debugVlogMaintf(
				"rewrite_exec reason=%s source_ids=%d max_segments=%d budget_tokens=%d max_source_bytes=%d min_stale_ratio=%.6f queue_len=%d ledger_live_bytes=%d",
				vlogGenerationReasonString(reason),
				len(rewriteOpts.SourceFileIDs),
				rewriteMaxSegments,
				budgetTokens,
				maxSourceBytes,
				rewriteOpts.MinSegmentStaleRatio,
				len(rewriteQueue),
				processedLedgerLiveBytes,
			)
			rewriteStart := time.Now()
			stats, err := rewriter.ValueLogRewriteOnline(ctx, rewriteOpts)
			cancel()
			if err != nil {
				db.debugVlogMaintf("rewrite_err reason=%s err=%v dur_ms=%.3f", vlogGenerationReasonString(reason), err, float64(time.Since(rewriteStart).Microseconds())/1000)
				if errors.Is(err, context.Canceled) {
					db.observeVlogGenerationRewriteCanceled()
					if len(processedRewriteIDs) > 0 {
						// A canceled rewrite that already selected a queued chunk should
						// immediately queue a checkpoint-kick retry. The retry executes
						// as resumable debt with bounded non-cancel semantics.
						db.vlogGenerationCheckpointKickPending.Store(true)
					}
				}
				return fmt.Errorf("generational rewrite: %w", err)
			}
			db.debugVlogMaintf(
				"rewrite_done reason=%s segments_before=%d segments_after=%d bytes_before=%d bytes_after=%d records=%d dur_ms=%.3f",
				vlogGenerationReasonString(reason),
				stats.SegmentsBefore,
				stats.SegmentsAfter,
				stats.BytesBefore,
				stats.BytesAfter,
				stats.RecordsCopied,
				float64(time.Since(rewriteStart).Microseconds())/1000,
			)
			effectiveBytesBefore := int64(stats.BytesBefore)
			effectiveBytesAfter := int64(stats.BytesAfter)
			gcBytesDeleted := int64(0)
			restagedRemainingQueue := false
			restageRemainingQueueAfterPass := false
			if len(processedRewriteIDs) > 0 {
				if err := db.consumeVlogGenerationRewriteQueueChunk(processedRewriteIDs); err != nil {
					return fmt.Errorf("consume generational rewrite queue: %w", err)
				}
				// Checkpoint-coupled resumable work should consume one chunk, then
				// re-confirm the remaining tail later instead of draining multiple
				// queued segments back-to-back in the same hot ingest window.
				if hadRewriteQueue && !opts.skipCheckpoint {
					restageRemainingQueueAfterPass = true
				}
			}
			if gcer, ok := db.backend.(backendValueLogGCer); ok {
				gcCtx, gcCancel := context.WithTimeout(context.Background(), 30*time.Second)
				gcStart := time.Now()
				gcStats, gcErr := gcer.ValueLogGC(gcCtx, backenddb.ValueLogGCOptions{
					ProtectedPaths: db.valueLogProtectedPaths(),
				})
				gcCancel()
				if gcErr != nil {
					db.debugVlogMaintf("gc_after_rewrite_err reason=%s err=%v dur_ms=%.3f", vlogGenerationReasonString(reason), gcErr, float64(time.Since(gcStart).Microseconds())/1000)
					return fmt.Errorf("generational gc after rewrite: %w", gcErr)
				}
				if gcStats.BytesDeleted > 0 {
					gcBytesDeleted = int64(gcStats.BytesDeleted)
					effectiveBytesAfter -= gcBytesDeleted
					if effectiveBytesAfter < 0 {
						effectiveBytesAfter = 0
					}
				}
				db.debugVlogMaintf("gc_after_rewrite_done reason=%s dur_ms=%.3f", vlogGenerationReasonString(reason), float64(time.Since(gcStart).Microseconds())/1000)
			}
			db.cleanupProcessedRetainedRewriteSources(reason, processedRewriteIDs)
			locallyEffectiveProcessedDebt := len(processedRewriteIDs) > 0 && processedLedgerOK && processedLedgerStaleBytes > 0 && stats.RecordsCopied > 0
			db.debugVlogGenerationProcessedSourceState(reason, processedRewriteIDs)
			if effectiveBytesBefore > 0 && effectiveBytesAfter >= effectiveBytesBefore && !locallyEffectiveProcessedDebt {
				db.vlogGenerationRewriteIneffectiveRuns.Add(1)
				db.vlogGenerationRewriteIneffectiveBytesIn.Add(uint64(effectiveBytesBefore))
				db.vlogGenerationRewriteIneffectiveBytesOut.Add(uint64(effectiveBytesAfter))
				db.debugVlogMaintf(
					"rewrite_ineffective reason=%s source_ids=%d bytes_before=%d bytes_after=%d gc_bytes_deleted=%d queue_len=%d",
					vlogGenerationReasonString(reason),
					len(rewriteOpts.SourceFileIDs),
					effectiveBytesBefore,
					effectiveBytesAfter,
					gcBytesDeleted,
					len(rewriteQueue),
				)
			}
			penalizeProcessedRewriteDebt := false
			penaltyReason := ""
			if len(processedRewriteIDs) > 0 && effectiveBytesAfter >= effectiveBytesBefore && !locallyEffectiveProcessedDebt {
				growth := effectiveBytesAfter - effectiveBytesBefore
				if growth >= vlogGenerationRewriteIneffectiveGrowthMinBytes {
					penalizeProcessedRewriteDebt = true
					penaltyReason = "material_growth"
				} else if growth == 0 && stats.RecordsCopied == 0 {
					// A queued rewrite that copies nothing and does not reduce bytes is
					// effectively a no-op resume loop; cool down the processed segment
					// and let the remaining debt continue instead of discarding it.
					penalizeProcessedRewriteDebt = true
					penaltyReason = "no_progress"
				}
			}
			if locallyEffectiveProcessedDebt {
				db.debugVlogMaintf(
					"rewrite_effective_local reason=%s processed_ids=%d planned_total=%d planned_live=%d planned_stale=%d global_bytes_before=%d global_bytes_after=%d gc_bytes_deleted=%d records=%d",
					vlogGenerationReasonString(reason),
					len(processedRewriteIDs),
					processedLedgerTotalBytes,
					processedLedgerLiveBytes,
					processedLedgerStaleBytes,
					effectiveBytesBefore,
					effectiveBytesAfter,
					gcBytesDeleted,
					stats.RecordsCopied,
				)
			}
			if restageRemainingQueueAfterPass {
				if remaining, restaged, err := db.restageVlogGenerationRewriteQueueRemaining(time.Now().UnixNano()); err != nil {
					return fmt.Errorf("restage remaining generational rewrite queue: %w", err)
				} else if restaged {
					restagedRemainingQueue = true
					// A newly-restaged tail should wait for its confirmation delay,
					// even if checkpoint kicks piled up during the rewrite/GC tail of
					// the just-finished maintenance pass.
					db.vlogGenerationCheckpointKickPending.Store(false)
					db.debugVlogMaintf(
						"rewrite_queue_restage remaining=%d confirm_delay_ms=%d",
						remaining,
						vlogGenerationRewriteMinInterval.Milliseconds(),
					)
				}
			}
			if penalizeProcessedRewriteDebt {
				db.vlogGenerationRewriteIneffectiveLastNS.Store(time.Now().UnixNano())
				if err := db.recordVlogGenerationRewritePenalty(
					processedRewriteIDs,
					time.Now().Add(vlogGenerationRewriteIneffectiveCooldown),
					effectiveBytesAfter-effectiveBytesBefore,
				); err != nil {
					return fmt.Errorf("record generational rewrite penalty: %w", err)
				}
				remainingQueue, queueErr := db.currentVlogGenerationRewriteQueue()
				if queueErr != nil {
					return fmt.Errorf("load generational rewrite queue after ineffective rewrite penalty: %w", queueErr)
				}
				db.debugVlogMaintf(
					"rewrite_ineffective_penalty reason=%s processed_ids=%d remaining_ids=%d growth=%d threshold=%d penalty_reason=%s cooldown_ms=%d gc_bytes_deleted=%d",
					vlogGenerationReasonString(reason),
					len(processedRewriteIDs),
					len(remainingQueue),
					effectiveBytesAfter-effectiveBytesBefore,
					vlogGenerationRewriteIneffectiveGrowthMinBytes,
					penaltyReason,
					vlogGenerationRewriteIneffectiveCooldown.Milliseconds(),
					gcBytesDeleted,
				)
			}
			if restagedRemainingQueue {
				db.debugVlogMaintf(
					"rewrite_queue_restaged reason=%s processed_ids=%d",
					vlogGenerationReasonString(reason),
					len(processedRewriteIDs),
				)
			}
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
			db.vlogGenerationRewriteRuns.Add(1)
			rewriteBytesIn := int64(0)
			if processedLedgerOK {
				rewriteBytesIn = processedLedgerLiveBytes
			} else if len(processedRewriteIDs) > 0 && stats.BytesBefore > 0 {
				rewriteBytesIn = int64(stats.BytesBefore)
			} else if haveRewritePlan && rewritePlan.SelectedBytesLive > 0 {
				rewriteBytesIn = rewritePlan.SelectedBytesLive
			} else if maxSourceBytes > 0 {
				rewriteBytesIn = maxSourceBytes
			} else if stats.BytesBefore > 0 {
				rewriteBytesIn = int64(stats.BytesBefore)
			}
			if rewriteBytesIn > 0 {
				db.vlogGenerationRewriteBytesIn.Add(uint64(rewriteBytesIn))
			}
			if stats.BytesAfter > 0 {
				db.vlogGenerationRewriteBytesOut.Add(uint64(stats.BytesAfter))
			}
			consumed := int64(0)
			if processedLedgerOK {
				consumed = processedLedgerLiveBytes
			} else if len(processedRewriteIDs) > 0 && stats.BytesBefore > 0 {
				consumed = int64(stats.BytesBefore)
			} else if haveRewritePlan && rewritePlan.SelectedBytesLive > 0 {
				consumed = rewritePlan.SelectedBytesLive
			} else if maxSourceBytes > 0 {
				consumed = maxSourceBytes
			} else if stats.BytesBefore > 0 {
				consumed = int64(stats.BytesBefore)
			}
			if stats.RecordsCopied > 0 {
				db.vlogGenerationRemapSuccesses.Add(uint64(stats.RecordsCopied))
			}
			if consumed > 0 {
				db.vlogGenerationConsumeRewriteBudgetBytes(consumed)
			}
			db.maybeRunVlogGenerationIndexVacuum(int64(stats.BytesBefore))
			return nil
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
				return
			}
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
			db.vlogGenerationRemapFailures.Add(1)
			if db.notifyError != nil {
				db.notifyError(fmt.Errorf("cachingdb: %w", err))
			}
		}
	}

	// If rewrite debt is already staged or waiting on segment age, do not spend
	// the active maintenance slot on generic periodic GC. Let the deferred
	// rewrite confirmation/age retry get the next opportunity instead.
	if !opts.bypassQuiet && !shouldRewrite && (stagePending || db.vlogGenerationRewriteAgeBlockedUntilNS.Load() > 0) {
		return
	}

	if envBool(envDisableVlogGenerationGC) {
		return
	}
	// GC is a best-effort background maintenance task. It requires a checkpoint
	// barrier to be safe, and that barrier can be very expensive during sustained
	// ingest/restore when the flush queue is non-empty. Avoid introducing long
	// stalls by only running the GC path when the cached write queue is drained.
	if queueLen != 0 {
		return
	}
	gcer, ok := db.backend.(backendValueLogGCer)
	if !ok {
		return
	}
	needEligibilityEstimate := !runGC && !db.shouldRunVlogGenerationGC(retained, reclaimable, churnBps)
	now = time.Now()
	lastGC := db.vlogGenerationLastGCUnixNano.Load()
	if lastGC > 0 {
		lastAt := time.Unix(0, lastGC)
		if now.Sub(lastAt) < vlogGenerationGCMinInterval {
			return
		}
	}
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerRunning)
	db.vlogGenerationLastReason.Store(vlogGenerationReasonPeriodicGC)
	err = db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{
		skipCheckpoint:        opts.skipCheckpoint,
		skipRetainedPruneWait: opts.skipRetainedPruneWait,
	}, func() error {
		if needEligibilityEstimate {
			gcStats, err := db.estimateVlogGenerationGCEligible(gcer)
			if err != nil {
				return fmt.Errorf("generational gc dry-run: %w", err)
			}
			if gcStats.BytesEligible < vlogGenerationGCMinBytes && gcStats.SegmentsEligible == 0 {
				return nil
			}
		}
		now := time.Now()
		db.vlogGenerationLastGCUnixNano.Store(now.UnixNano())
		ctx, cancel := db.foregroundMaintenanceContext(30 * time.Second)
		gcOpts := backenddb.ValueLogGCOptions{ProtectedPaths: db.valueLogProtectedPaths()}
		gcStats, err := gcer.ValueLogGC(ctx, gcOpts)
		cancel()
		if err != nil {
			return fmt.Errorf("generational gc: %w", err)
		}
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
		db.vlogGenerationGCRuns.Add(1)
		if gcStats.SegmentsDeleted > 0 {
			db.vlogGenerationGCSegmentsDeleted.Add(uint64(gcStats.SegmentsDeleted))
		}
		if gcStats.BytesDeleted > 0 {
			db.vlogGenerationGCBytesDeleted.Add(uint64(gcStats.BytesDeleted))
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
			return
		}
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
		if db.notifyError != nil {
			db.notifyError(fmt.Errorf("cachingdb: %w", err))
		}
		return
	}
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
	return
}

func (db *DB) maybeKickVlogGenerationMaintenanceAfterCheckpoint() {
	if db == nil || db.closing.Load() {
		if db != nil {
			db.debugVlogMaintf("checkpoint_kick_skip reason=closing")
		}
		return
	}
	if envBool(envDisableVlogGenerationCheckpointKick) {
		db.debugVlogMaintf("checkpoint_kick_skip reason=disabled_env")
		return
	}
	if db.testSkipVlogCheckpointKick {
		db.debugVlogMaintf("checkpoint_kick_skip reason=test_skip")
		return
	}
	if db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) {
		db.debugVlogMaintf("checkpoint_kick_skip reason=policy_off policy=%d", db.valueLogGenerationPolicy)
		return
	}
	now := time.Now()
	last := db.vlogGenerationLastCheckpointKickUnixNano.Load()
	if last > 0 && now.Sub(time.Unix(0, last)) < vlogGenerationCheckpointKickMinInterval {
		db.debugVlogMaintf(
			"checkpoint_kick_skip reason=min_interval since_ms=%.3f min_ms=%.3f",
			float64(now.Sub(time.Unix(0, last)).Microseconds())/1000,
			float64(vlogGenerationCheckpointKickMinInterval.Microseconds())/1000,
		)
		return
	}
	// Avoid forcing extra checkpoint boundaries when rewrite is clearly ineligible.
	// Skip this fast-path when rewrite is disabled so GC-only kicks still run.
	if !envBool(envDisableVlogGenerationRewrite) {
		rewriteQueue, qerr := db.currentVlogGenerationRewriteQueue()
		if qerr != nil {
			db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
			if db.notifyError != nil {
				db.notifyError(fmt.Errorf("cachingdb: load generational rewrite queue for checkpoint kick: %w", qerr))
			}
			return
		}
		if len(rewriteQueue) == 0 {
			if trigger := db.valueLogRewriteTriggerBytes; trigger > 0 {
				retained, bytes := db.valueLogRetainedStats()
				if bytes < trigger && retained < 2 {
					db.debugVlogMaintf(
						"checkpoint_kick_skip reason=trigger_floor bytes=%d trigger=%d retained=%d queue_len=0",
						bytes,
						trigger,
						retained,
					)
					return
				}
			}
		}
	}
	if !db.vlogGenerationCheckpointKickActive.CompareAndSwap(false, true) {
		db.debugVlogMaintf("checkpoint_kick_skip reason=already_active")
		return
	}
	db.vlogGenerationLastCheckpointKickUnixNano.Store(now.UnixNano())
	db.vlogGenerationCheckpointKickRuns.Add(1)
	db.debugVlogMaintf("checkpoint_kick_start")
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		defer db.vlogGenerationCheckpointKickActive.Store(false)
		db.checkpointMu.Lock()
		for db.checkpointing.Load() {
			db.checkpointCond.Wait()
		}
		db.checkpointMu.Unlock()
		if db.closing.Load() {
			return
		}
		rewriteRunsBefore := db.vlogGenerationRewriteRuns.Load()
		gcRunsBefore := db.vlogGenerationGCRuns.Load()
		db.runVlogGenerationCheckpointKickRetries(vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			// Checkpoint-triggered maintenance still needs a fresh serialized
			// backend view before iterator-based rewrite/GC scans run. Re-entering
			// Checkpoint here is safe: the just-finished caller has already cleared
			// checkpointing, and the kick-active guard prevents recursive kicks.
			skipCheckpoint:   false,
			rewriteDebtDrain: true,
		})
		if db.vlogGenerationRewriteRuns.Load() > rewriteRunsBefore {
			db.vlogGenerationCheckpointKickRewriteRuns.Add(1)
		}
		if db.vlogGenerationGCRuns.Load() > gcRunsBefore {
			db.vlogGenerationCheckpointKickGCRuns.Add(1)
		}
		db.debugVlogMaintf(
			"checkpoint_kick_done rewrite_runs_before=%d rewrite_runs_after=%d gc_runs_before=%d gc_runs_after=%d pending=%t",
			rewriteRunsBefore,
			db.vlogGenerationRewriteRuns.Load(),
			gcRunsBefore,
			db.vlogGenerationGCRuns.Load(),
			db.vlogGenerationCheckpointKickPending.Load(),
		)
	}()
}

func (db *DB) maybeRunVlogGenerationIndexVacuum(rewriteBytesIn int64) {
	if db == nil || db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) {
		return
	}
	if envBool(envDisableVlogGenerationVacuum) {
		return
	}
	vacuumer, ok := db.backend.(backendIndexVacuumer)
	if !ok {
		return
	}
	now := time.Now()
	if !db.shouldRunVlogGenerationIndexVacuum(rewriteBytesIn, now) {
		return
	}
	db.vlogGenerationLastReason.Store(vlogGenerationReasonPostRewriteVacuum)
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerRunning)
	runVacuum := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := vacuumer.VacuumIndexOnline(ctx)
		cancel()
		return err
	}
	var err error
	if db.maintenanceActive.Load() {
		err = runVacuum()
	} else {
		err = db.runWithBackendMaintenance(runVacuum)
	}
	if err != nil {
		db.vlogGenerationVacuumFailures.Add(1)
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerError)
		if db.notifyError != nil {
			db.notifyError(fmt.Errorf("cachingdb: generational index vacuum: %w", err))
		}
		return
	}
	db.vlogGenerationVacuumRuns.Add(1)
	db.vlogGenerationLastVacuumUnixNano.Store(now.UnixNano())
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
}

func (db *DB) shouldRunVlogGenerationIndexVacuum(rewriteBytesIn int64, now time.Time) bool {
	if db == nil || db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) {
		return false
	}
	if rewriteBytesIn < vlogGenerationVacuumTriggerRewriteBytes {
		return false
	}
	last := db.vlogGenerationLastVacuumUnixNano.Load()
	if last > 0 {
		lastAt := time.Unix(0, last)
		if now.Sub(lastAt) < vlogGenerationVacuumMinInterval {
			return false
		}
	}
	return true
}

func (db *DB) shouldRunVlogGenerationGC(retained valueLogRetainedGenerationStats, reclaimable int64, churnBps int64) bool {
	if db == nil || db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) {
		return false
	}
	if reclaimable >= vlogGenerationGCMinBytes {
		return true
	}
	if retained.SegmentsHot >= 2 && retained.SegmentsTotal >= 3 {
		return true
	}
	if db.valueLogRewriteTriggerChurn > 0 && churnBps >= db.valueLogRewriteTriggerChurn/2 {
		return true
	}
	return false
}

func (db *DB) estimateVlogGenerationGCEligible(gcer backendValueLogGCer) (backenddb.ValueLogGCStats, error) {
	if db == nil || gcer == nil {
		return backenddb.ValueLogGCStats{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := gcer.ValueLogGC(ctx, backenddb.ValueLogGCOptions{
		DryRun:         true,
		ProtectedPaths: db.valueLogProtectedPaths(),
	})
	if err == nil {
		db.vlogGenerationLastGCDryRunUnixNano.Store(time.Now().UnixNano())
		db.vlogGenerationLastGCDryRunBytesEligible.Store(stats.BytesEligible)
		db.vlogGenerationLastGCDryRunSegsEligible.Store(int64(stats.SegmentsEligible))
	}
	return stats, err
}

func (db *DB) vlogGenerationEstimateChurnBps() int64 {
	if db == nil {
		return 0
	}
	now := time.Now().UnixNano()
	cur := db.vlogGenerationChurnBytes.Load()
	prevBytes := db.vlogGenerationLastChurnSampleBytes.Swap(cur)
	prevNs := db.vlogGenerationLastChurnSampleNS.Swap(now)
	if prevNs <= 0 || now <= prevNs || cur < prevBytes {
		db.vlogGenerationLastChurnBps.Store(0)
		return 0
	}
	deltaBytes := cur - prevBytes
	deltaNs := now - prevNs
	if deltaNs <= 0 {
		db.vlogGenerationLastChurnBps.Store(0)
		return 0
	}
	churn := int64((float64(deltaBytes) * float64(time.Second)) / float64(deltaNs))
	if churn < 0 {
		churn = 0
	}
	db.vlogGenerationLastChurnBps.Store(churn)
	return churn
}

func (db *DB) shouldRunVlogGenerationRewrite(totalBytes int64, staleRatioPPM uint32, churnBps int64) (bool, uint32) {
	if db == nil {
		return false, vlogGenerationReasonNone
	}
	if db.valueLogRewriteTriggerBytes > 0 && totalBytes >= db.valueLogRewriteTriggerBytes {
		return true, vlogGenerationReasonTotalBytes
	}
	if db.valueLogRewriteTriggerRatioPPM > 0 && staleRatioPPM >= db.valueLogRewriteTriggerRatioPPM {
		return true, vlogGenerationReasonStaleRatio
	}
	if db.valueLogRewriteTriggerChurn > 0 && churnBps >= db.valueLogRewriteTriggerChurn {
		return true, vlogGenerationReasonChurn
	}
	return false, vlogGenerationReasonNone
}

func (db *DB) vlogGenerationRewriteMinStaleRatioForGenericPass(totalBytes int64) float64 {
	if totalBytes < vlogGenerationRewriteEfficacyMinTotalBytes {
		return 0
	}
	ratio := vlogGenerationRewriteGenericMinSegmentStaleRatio
	if configured := db.vlogGenerationRewriteMinStaleRatioForStaleRatioTrigger(totalBytes); configured > ratio {
		ratio = configured
	}
	return ratio
}

func (db *DB) vlogGenerationRewriteMinStaleRatioForQueuedDebt(totalBytes int64, reason uint32) float64 {
	if reason == vlogGenerationReasonTotalBytes || reason == vlogGenerationReasonChurn {
		if ratio := db.vlogGenerationRewriteMinStaleRatioForGenericPass(totalBytes); ratio > 0 {
			return ratio
		}
	}
	return vlogGenerationRewriteMinSegmentStaleRatio
}

func (db *DB) vlogGenerationRewriteMinStaleRatioForStaleRatioTrigger(totalBytes int64) float64 {
	if db == nil || db.valueLogRewriteTriggerRatioPPM <= 0 {
		return 0
	}
	ratio := float64(db.valueLogRewriteTriggerRatioPPM) / 1_000_000.0
	if ratio > 1 {
		return 1
	}
	if totalBytes >= vlogGenerationRewriteEfficacyMinTotalBytes && ratio < vlogGenerationRewriteMinSegmentStaleRatio {
		ratio = vlogGenerationRewriteMinSegmentStaleRatio
	}
	return ratio
}

func (db *DB) valueLogRewriteBatchSize() int {
	if db == nil {
		return 256
	}
	if db.valueLogRewriteBudgetRecords > 0 {
		if db.valueLogRewriteBudgetRecords < 1 {
			return 1
		}
		return db.valueLogRewriteBudgetRecords
	}
	return 256
}

func (db *DB) ensureBackendRange() error {
	if db == nil {
		return nil
	}
	db.backendRangeInit.Do(func() {
		r, known, err := db.computeBackendRange()
		db.mu.Lock()
		defer db.mu.Unlock()
		if err != nil {
			db.backendRangeErr = err
			db.backendRangeKnown = false
			return
		}
		if r.valid {
			db.backendRange.add(r.min)
			db.backendRange.add(r.max)
		}
		db.backendRangeKnown = known
	})

	db.mu.RLock()
	err := db.backendRangeErr
	db.mu.RUnlock()
	return err
}

func (db *DB) computeBackendRange() (keyRange, bool, error) {
	minIter, err := db.backend.Iterator(nil, nil)
	if err != nil {
		return keyRange{}, false, err
	}
	defer minIter.Close()
	minIter.Seek(nil)

	r := keyRange{}
	if minIter.Valid() && !minIter.IsDeleted() {
		r.add(minIter.UnsafeKey())
	}

	maxIter, err := db.backend.ReverseIterator(nil, nil)
	if err != nil {
		// Backend doesn't support reverse iteration; disable backend-range-dependent optimizations.
		return r, false, nil
	}
	defer maxIter.Close()

	if maxIter.Valid() && !maxIter.IsDeleted() {
		r.add(maxIter.UnsafeKey())
	}

	return r, true, nil
}

const stopResumeFraction = 0.70
const stopBackpressureStallLimit = 16

func (db *DB) adaptiveBackpressureEnabled() bool {
	return db.slowdownBacklogSeconds > 0 || db.stopBacklogSeconds > 0 || db.maxBacklogBytes > 0
}

func (db *DB) thresholdsLocked() (slowdownBytes, stopBytes, resumeBytes int64) {
	flushBps := db.flushBpsEWMA
	if flushBps <= 0 && db.flushThreshold > 0 {
		// Fallback until we have real measurements: assume ~1 memtable/sec.
		flushBps = float64(db.flushThreshold)
	}
	return computeBackpressureThresholds(backpressureParams{
		flushBps:               flushBps,
		flushThreshold:         db.flushThreshold,
		slowdownBacklogSeconds: db.slowdownBacklogSeconds,
		stopBacklogSeconds:     db.stopBacklogSeconds,
		maxBacklogBytes:        db.maxBacklogBytes,
		stopResumeFraction:     stopResumeFraction,
	})
}

func (db *DB) waitForCheckpoint() {
	if !db.checkpointing.Load() && !db.maintenanceActive.Load() {
		return
	}
	db.checkpointMu.Lock()
	for db.checkpointing.Load() || db.maintenanceActive.Load() {
		db.checkpointCond.Wait()
	}
	db.checkpointMu.Unlock()
}

func (db *DB) recordCheckpointCutover(d time.Duration) {
	if db == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	ns := d.Nanoseconds()
	db.checkpointCutoverLastNanos.Store(ns)
	db.checkpointCutoverLastUnixNano.Store(time.Now().UnixNano())
	db.checkpointCutoverTotalNanos.Add(ns)
	db.checkpointCutoverSamples.Add(1)
	for {
		cur := db.checkpointCutoverMaxNanos.Load()
		if ns <= cur || db.checkpointCutoverMaxNanos.CompareAndSwap(cur, ns) {
			break
		}
	}
}

const (
	checkpointSparseIndexCheckEveryNoops       = 8
	checkpointSparseIndexMinPages              = 128
	checkpointSparseIndexMaxInternalFillP50PPM = 200_000
	checkpointSparseIndexMaxInternalFillAvgPPM = 350_000
	checkpointAutoVacuumTimeout                = 10 * time.Second
)

func parseCheckpointFragUint(report map[string]string, key string) (uint64, bool) {
	if len(report) == 0 {
		return 0, false
	}
	raw := report[key]
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (db *DB) maybeVacuumSparseIndexOnCheckpoint() error {
	if db == nil || !db.disableJournal {
		return nil
	}
	// When outer leaves already live in the value log, checkpoint-time online
	// vacuum is too expensive for the fast profile. Even with leaf-ref-preserving
	// vacuum, swapping index.db during every catch-up window materially hurts live
	// progress, and the outer-leaf tree does not need this automatic path to stay
	// correct. Keep manual/public vacuum available; skip the automatic checkpoint
	// trigger for this layout.
	if db.indexOuterLeavesInValueLog {
		return nil
	}
	if envBool(envDisableCheckpointAutoVacuum) {
		return nil
	}
	if db.testSkipCheckpointAutoVacuum {
		return nil
	}
	frag, ok := db.backend.(backendIndexFragmenter)
	if !ok {
		return nil
	}
	vacuumer, ok := db.backend.(backendIndexVacuumer)
	if !ok {
		return nil
	}

	runs := db.checkpointRuns.Load() + 1
	last := db.checkpointAutoVacuumLastCheckRun.Load()
	if runs-last < checkpointSparseIndexCheckEveryNoops {
		return nil
	}

	report, err := frag.FragmentationReport()
	if err != nil {
		return err
	}
	pages, ok := parseCheckpointFragUint(report, "treedb.user.pages")
	if !ok || pages < checkpointSparseIndexMinPages {
		return nil
	}
	p50, ok := parseCheckpointFragUint(report, "treedb.user.internal_fill_ppm_p50")
	if !ok {
		return nil
	}
	avg, ok := parseCheckpointFragUint(report, "treedb.user.internal_fill_ppm_avg")
	if !ok {
		return nil
	}
	for {
		last = db.checkpointAutoVacuumLastCheckRun.Load()
		if runs-last < checkpointSparseIndexCheckEveryNoops {
			return nil
		}
		if db.checkpointAutoVacuumLastCheckRun.CompareAndSwap(last, runs) {
			break
		}
	}

	db.checkpointAutoVacuumLastPages.Store(pages)
	db.checkpointAutoVacuumLastInternalP50.Store(p50)
	db.checkpointAutoVacuumLastInternalAvg.Store(avg)

	if p50 >= checkpointSparseIndexMaxInternalFillP50PPM && avg >= checkpointSparseIndexMaxInternalFillAvgPPM {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkpointAutoVacuumTimeout)
	defer cancel()
	if err := vacuumer.VacuumIndexOnline(ctx); err != nil {
		if errors.Is(err, backenddb.ErrVacuumUnsupported) {
			return nil
		}
		return err
	}
	db.checkpointAutoVacuumRuns.Add(1)
	return nil
}

// checkpointRotateCapacity returns the memtable capacity used when checkpoint
// rotates mutable shards. We intentionally cap checkpoint-time preallocation to
// keep write-locked cutover latency bounded; normal growth resumes as writers
// repopulate the fresh mutable shard.
func (db *DB) checkpointRotateCapacity() int {
	if db == nil {
		return -1
	}
	capacity := db.memtableCap
	if capacity <= 0 {
		return -1
	}
	// Checkpoint cutover uses a bounded preallocation to keep lock hold time
	// predictable. append_only entry slices, however, pay a high regrowth/copy
	// tax when this cap is too small (notably in settled batch write workloads).
	// Use a higher cap there while still avoiding full-threshold prealloc.
	checkpointRotateCapMax := 256 * 1024
	if db.currentMemtableMode() == memtable.ModeAppendOnly {
		checkpointRotateCapMax = 4 * 1024 * 1024
	}
	if capacity > checkpointRotateCapMax {
		return checkpointRotateCapMax
	}
	return capacity
}

func (db *DB) observePublishWatermarkLagDrift(backlogBytes int64, now time.Time) float64 {
	if db == nil {
		return 0
	}
	nowNS := now.UnixNano()
	db.publishWatermarkLagMu.Lock()
	defer db.publishWatermarkLagMu.Unlock()
	prevNS := db.publishWatermarkLastUnixNano
	prevBacklog := db.publishWatermarkLastBacklogBytes
	db.publishWatermarkLastUnixNano = nowNS
	db.publishWatermarkLastBacklogBytes = backlogBytes
	if prevNS <= 0 || nowNS <= prevNS {
		return 0
	}
	dt := float64(nowNS-prevNS) / float64(time.Second)
	if dt <= 0 {
		return 0
	}
	return float64(backlogBytes-prevBacklog) / dt
}

// Checkpoint forces a durable backend boundary and trims the WAL so long-running
// cached-mode runs do not accumulate unbounded `wal/` growth.
//
// It blocks writers while it:
//   - rotates the current mutable memtable (if non-empty),
//   - rotates to a fresh WAL segment,
//   - flushes all queued memtables with backend sync,
//   - forces a backend sync boundary (even if the queue is empty),
//   - removes all older WAL segments (keeping only the currently-open one).
func (db *DB) Checkpoint() error {
	if db == nil {
		return nil
	}
	if db.closing.Load() {
		return errDBClosing
	}
	start := time.Now()
	defer func() {
		dur := uint64(time.Since(start))
		db.checkpointRuns.Add(1)
		db.checkpointTotalNs.Add(dur)
		updateAtomicMaxUint64(&db.checkpointMaxNs, dur)
	}()
	// Note: Any code path that takes both flushMu and checkpointMu must acquire
	// flushMu first to avoid deadlocks.
	flushMuWaitStart := time.Now()
	db.flushMu.Lock()
	flushMuWait := uint64(time.Since(flushMuWaitStart))
	db.checkpointFlushMuWaitNs.Add(flushMuWait)
	updateAtomicMaxUint64(&db.checkpointFlushMuWaitMaxNs, flushMuWait)
	defer db.flushMu.Unlock() // Ensure it's released

	db.checkpointMu.Lock()
	for db.checkpointing.Load() {
		db.checkpointCond.Wait()
	}
	db.checkpointing.Store(true) // Set flag only after acquiring flushMu
	db.checkpointMu.Unlock()

	defer func() { // This defer runs when db.Checkpoint() returns
		db.checkpointMu.Lock()
		db.checkpointing.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}()

	db.writeMu.Lock()
	cutoverStart := time.Now()
	releaseWriteMu := func() {
		db.writeMu.Unlock()
		db.recordCheckpointCutover(time.Since(cutoverStart))
	}

	// Rotate mutable into the flush queue and ensure future writes land in a fresh
	// WAL segment (so all older segments can be trimmed after the sync boundary).
	db.mu.Lock()
	hasMutable := db.mutableBytes.Load() > 0
	hasQueue := len(db.queue) > 0
	hasDirtyVLog := db.hasDirtyValueLogLanes()
	if hasMutable {
		if err := db.rotateMutableShardsLocked(db.checkpointRotateCapacity(), false); err != nil {
			db.mu.Unlock()
			releaseWriteMu()
			return err
		}
		hasQueue = len(db.queue) > 0
	} else if db.disableJournal && !hasQueue && !hasDirtyVLog {
		db.mu.Unlock()
		releaseWriteMu()
		db.checkpointNoopSkips.Add(1)
		if err := db.maybeVacuumSparseIndexOnCheckpoint(); err != nil {
			return err
		}
		db.checkValueLogRetention()
		db.scheduleRetainedValueLogPrune()
		return nil
	}
	walDir := db.dir
	preRotateWALPaths := db.currentWALPaths()
	ridBeforeWALRotate := db.nextRID.Load()
	db.mu.Unlock()
	rotateLaneIDs := make([]int, 0, len(db.lanes))
	for i := range db.lanes {
		if db.lanes[i].walLiveBytes.Load() > 0 {
			rotateLaneIDs = append(rotateLaneIDs, i)
		}
	}
	if len(rotateLaneIDs) == 0 {
		for i := range db.lanes {
			rotateLaneIDs = append(rotateLaneIDs, i)
		}
	}
	releaseWriteMu()

	errCh := make(chan error, len(rotateLaneIDs))
	var rotateWG sync.WaitGroup
	for _, laneID := range rotateLaneIDs {
		rotateWG.Add(1)
		go func(id int) {
			defer rotateWG.Done()
			if err := db.rotateWALCheckpointLocked(&db.lanes[id]); err != nil {
				errCh <- err
			}
		}(laneID)
	}
	rotateWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	wroteDuringWALRotate := db.nextRID.Load() != ridBeforeWALRotate
	if err := db.checkpointFlushValueLogLanes(); err != nil {
		return err
	}

	// Flush all queued memtables with backend sync.
	db.flushAllLocked(true)

	segments, nonEmptyBytes := listNonEmptyLogSegments(walDir)
	if len(segments) > 0 {
		filtered := segments[:0]
		nonEmptyBytes = 0
		for _, seg := range segments {
			if seg.valueLog != db.walUsesValueLog() {
				continue
			}
			filtered = append(filtered, seg)
			if seg.size > 0 {
				nonEmptyBytes += seg.size
			}
		}
		segments = filtered
	}
	// New logic: perform sync write only if not relaxedSync
	var commitErr error
	if nonEmptyBytes > 0 {
		backendBatch := db.backend.NewBatch()
		if db.relaxedSync {
			// If relaxed sync, just write the batch without forcing sync
			commitErr = backendBatch.Write()
		} else {
			// Otherwise, force sync
			commitErr = backendBatch.WriteSync()
		}
		cerr := backendBatch.Close()
		if commitErr == nil {
			commitErr = cerr
		}
		if commitErr != nil {
			return commitErr
		}
	}

	currentWALs := make(map[string]struct{})
	for _, path := range db.currentWALPaths() {
		currentWALs[path] = struct{}{}
	}
	unsafeWALDeletes := make(map[string]struct{})
	if wroteDuringWALRotate {
		for _, path := range preRotateWALPaths {
			if path == "" {
				continue
			}
			unsafeWALDeletes[path] = struct{}{}
		}
	}

	removed := false
	for _, seg := range segments {
		path := seg.path
		if _, ok := currentWALs[path]; ok {
			continue
		}
		if _, ok := unsafeWALDeletes[path]; ok {
			continue
		}
		if db.valueLogRetained(path) {
			continue
		}
		db.dropValueLogSegment(path)
		if err := db.removeFileRetry(path); err != nil {
			// Best effort cleanup; ignore errors to prevent flakiness on Windows
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
		db.forgetValueLogRetain(path)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	if err := db.maybeVacuumSparseIndexOnCheckpoint(); err != nil {
		return err
	}
	db.checkValueLogRetention()
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()
	db.scheduleRetainedValueLogPrune()
	db.trimRetainedArenasAfterFlush(true)

	return nil
}

func (db *DB) waitForStop() {
	if !db.adaptiveBackpressureEnabled() {
		return
	}

	for {
		db.bpMu.Lock()
		_, stopBytes, resumeBytes := db.thresholdsLocked()
		if stopBytes <= 0 {
			db.bpMu.Unlock()
			return
		}
		// Self-heal: backlog bytes should never remain positive when the queue is empty.
		// If this happens, stop backpressure would block forever.
		// Use the lock-free memtable view to avoid deadlock with db.mu.
		queueLen := 0
		if view := db.memtables.Load(); view != nil {
			queueLen = len(view.queue)
		}
		if queueLen == 0 {
			db.queueBacklogBytes.Store(0)
			db.bpMu.Unlock()
			return
		}

		backlog := db.queueBacklogBytes.Load()
		if backlog < stopBytes {
			db.bpMu.Unlock()
			return
		}
		db.bpMu.Unlock()

		// Ensure a background flush pass is scheduled in case backlog was created
		// without a flush trigger (e.g. iterator-driven rotations).
		db.TriggerFlush()

		// Stop backpressure means we are already blocking the caller. Actively
		// flush a bounded amount of work, then return. We avoid looping/sleeping
		// here to prevent per-write stalls when flush progress is slow.
		target := stopBytes
		if resumeBytes > 0 {
			target = resumeBytes
		}
		stalls := 0
		for db.queueBacklogBytes.Load() >= target {
			maxMemtables := db.writerFlushMaxMemtables
			if maxMemtables <= 0 {
				maxMemtables = 1
			}
			// Under stop-backpressure, flush more aggressively to avoid repeated stalls.
			if maxMemtables < 8 {
				maxMemtables = 8
			}
			if maxMemtables > flushCombineMaxMemtables {
				maxMemtables = flushCombineMaxMemtables
			}

			before := db.queueBacklogBytes.Load()
			db.flushSomeBlocking(false, maxMemtables, db.writerFlushMaxDuration)
			after := db.queueBacklogBytes.Load()
			if after < target {
				break
			}
			if after >= before {
				stalls++
				if stalls >= stopBackpressureStallLimit {
					break
				}
			} else {
				stalls = 0
			}
		}
		return
	}
}

func (db *DB) shouldWaitForStop() bool {
	if !db.adaptiveBackpressureEnabled() {
		return false
	}
	backlog := db.queueBacklogBytes.Load()
	if backlog <= 0 {
		return false
	}
	// Self-heal stale backlog accounting when the queue is already empty.
	if view := db.memtables.Load(); view == nil || len(view.queue) == 0 {
		db.queueBacklogBytes.Store(0)
		return false
	}
	db.bpMu.Lock()
	_, stopBytes, _ := db.thresholdsLocked()
	db.bpMu.Unlock()
	if stopBytes <= 0 {
		return false
	}
	return backlog >= stopBytes
}

func (db *DB) maybeWaitForStop() {
	if db.shouldWaitForStop() {
		db.waitForStop()
	}
}

func (db *DB) maybeAssistFlush() {
	if db.writerFlushMaxMemtables <= 0 && db.writerFlushMaxDuration <= 0 {
		return
	}

	// Adaptive policy: thresholds based on queued backlog bytes.
	if db.adaptiveBackpressureEnabled() {
		backlog := db.queueBacklogBytes.Load()
		// Self-heal stale backlog accounting when the queue is empty.
		if backlog > 0 {
			if view := db.memtables.Load(); view == nil || len(view.queue) == 0 {
				db.queueBacklogBytes.Store(0)
				return
			}
		}

		db.bpMu.Lock()
		slowdownBytes, stopBytes, _ := db.thresholdsLocked()
		db.bpMu.Unlock()

		backlog = db.queueBacklogBytes.Load()
		if stopBytes > 0 && backlog >= stopBytes {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if slowdownBytes > 0 && backlog > slowdownBytes {
			db.TriggerFlush()
			return
		}
		return
	}

	// Legacy policy: thresholds based on queue length.
	if db.maxQueuedMemtables >= 0 {
		queueLen := 0
		if view := db.memtables.Load(); view != nil {
			queueLen = len(view.queue)
		}
		if queueLen > db.maxQueuedMemtables {
			db.TriggerFlush()
		}
	}
}

func (db *DB) flushSome(sync bool, maxMemtables int, maxDuration time.Duration) int {
	if maxMemtables <= 0 && maxDuration <= 0 {
		return 0
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	sync = db.flushSyncRequested(sync)
	start := time.Now()

	flushed := 0
	for {
		if maxMemtables > 0 && flushed >= maxMemtables {
			return flushed
		}
		if maxDuration > 0 && time.Since(start) >= maxDuration {
			return flushed
		}
		laneID, ok := db.pickFlushLane()
		if !ok {
			return flushed
		}
		if laneID < len(db.flushLaneMu) {
			if !db.flushLaneMu[laneID].TryLock() {
				return flushed
			}
		}
		okFlush := db.flushLaneOnce(sync, laneID)
		if laneID < len(db.flushLaneMu) {
			db.flushLaneMu[laneID].Unlock()
		}
		if !okFlush {
			return flushed
		}
		flushed++
	}
}

// flushSomeBlocking is like flushSome, but it blocks on lane locks. This is used
// by stop-backpressure to guarantee forward progress instead of spinning.
func (db *DB) flushSomeBlocking(sync bool, maxMemtables int, maxDuration time.Duration) {
	if maxMemtables <= 0 && maxDuration <= 0 {
		return
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	sync = db.flushSyncRequested(sync)
	start := time.Now()

	flushed := 0
	for {
		if maxMemtables > 0 && flushed >= maxMemtables {
			return
		}
		if maxDuration > 0 && time.Since(start) >= maxDuration {
			return
		}
		laneID, ok := db.pickFlushLane()
		if !ok {
			return
		}
		okFlush := db.flushLaneOnce(sync, laneID)
		if !okFlush {
			return
		}
		flushed++
	}
}

func (db *DB) Close() error {
	var errs []error
	hadMemtables := false
	db.closing.Store(true)
	unregisterTreeDBExpvarStatsDB(db)
	db.stopDomainIngressWorkers()
	db.waitForRetainedValueLogPrune()

	// Lock order must match Checkpoint (flushMu -> writeMu) to avoid a deadlock
	// with the auto-checkpoint goroutine:
	// - Checkpoint takes flushMu, then writeMu.
	// - Close historically took writeMu, then flushMu via flushAll().
	// If auto-checkpoint is in progress (holding flushMu, waiting for writeMu)
	// and Close starts (holding writeMu, waiting for flushMu), the process can
	// deadlock and tests will time out.
	db.flushMu.Lock()
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		hadMemtables = true
		_ = db.rotateMemtableLocked(true)
	} else if len(db.queue) > 0 {
		hadMemtables = true
	}
	db.mu.Unlock()

	for i := range db.lanes {
		l := &db.lanes[i]
		l.walFastMu.Lock()
		l.walFastClosed = true
		if l.walFastCond != nil {
			l.walFastCond.Broadcast()
		}
		l.walFastMu.Unlock()
	}

	// Flush while closeCh is still open so commit/append paths remain available.
	// This avoids dropping pending memtables on close.
	if hadMemtables {
		// flushMu is already held by Close.
		db.flushAllLocked(true)
	}

	close(db.closeCh)
	db.writeMu.Unlock()
	db.flushMu.Unlock()
	db.wg.Wait()
	// Retained-prune scans use the live value-log reader and backend state.
	// Wait for any in-flight prune before tearing down readers or removing
	// lane files so Close cannot race a background live-ID walk.
	db.waitForRetainedValueLogPrune()
	db.valueLogDictTrainerMu.Lock()
	trainer := db.valueLogDictTrainer
	db.valueLogDictTrainer = nil
	db.valueLogDictTrainerMu.Unlock()
	if trainer != nil {
		trainer.Close()
	}
	if db.valueLogTemplateEngine != nil {
		db.valueLogTemplateEngine.Close()
		db.valueLogTemplateEngine = nil
	}
	if db.hashSortedIndexer != nil {
		db.hashSortedIndexer.Close()
		db.hashSortedIndexer = nil
	}
	if db.valueLogReader != nil {
		if err := db.valueLogReader.Close(); err != nil {
			errs = append(errs, err)
		}
		db.valueLogReader = nil
	}

	var walBytes int64
	var walPaths []string
	walPaths = make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		walBytes += l.walClosedBytes.Load()
		for path := range l.walClosedSizes {
			walPaths = append(walPaths, path)
		}
		if l.wal != nil {
			walBytes += l.wal.Size()
			l.walLiveBytes.Store(0)
			if l.walPath != "" {
				walPaths = append(walPaths, l.walPath)
			}
			_ = l.wal.Close()
			l.wal = nil
		} else if l.walPath != "" {
			walPaths = append(walPaths, l.walPath)
		}
		l.walMu.Unlock()

		l.vlogMu.Lock()
		if l.vlog != nil {
			_ = l.vlog.Close()
			l.vlog = nil
			l.vlogCaps = vlogWriterCaps{}
			l.vlogLiveBytes.Store(0)
		}
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		l.vlogMu.Unlock()
	}

	if walBytes > 0 && !hadMemtables {
		backendBatch := db.backend.NewBatch()
		db.flushMu.Lock()
		err := backendBatch.WriteSync()
		db.flushMu.Unlock()
		if cerr := backendBatch.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	seen := make(map[string]struct{}, len(walPaths))
	removed := false
	for _, path := range walPaths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		retain := db.valueLogRetained(path)
		if retain {
			continue
		}
		db.dropValueLogSegment(path)
		if err := db.removeFileRetry(path); err != nil {
			// Best effort cleanup; ignore errors to prevent flakiness on Windows
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
		db.forgetValueLogRetain(path)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}

	db.waitForRetainedValueLogPrune()
	if err := db.backend.Close(); err != nil {
		errs = append(errs, err)
	}
	if db.dictStore != nil {
		if closer, ok := db.dictStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if bgErr := db.backgroundError(); bgErr != nil {
		errs = append(errs, bgErr)
	}
	return errors.Join(errs...)
}
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	return db.set(key, value, false)
}

func (db *DB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	return db.set(key, value, true)
}

func (db *DB) flushAllMemtablesForSync(sync bool) error {
	db.writeMu.Lock()

	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()

	db.writeMu.Unlock()

	db.flushMu.Lock()
	db.flushAllLocked(sync)
	db.flushMu.Unlock()
	return db.backgroundError()
}

func (db *DB) syncBarrierAfterWrite(sync bool) error {
	if !sync {
		return nil
	}
	if !db.disableJournal {
		// Journal durability is handled by appendValueLog + appendWAL:
		// - strict: fsync
		// - relaxed: flush-to-kernel (no fsync)
		return nil
	}
	if db.relaxedSync {
		if db.indexOuterLeavesInValueLog {
			// ProfileFast-style workloads rely on WriteSync for immediate
			// read-after-write visibility, not per-batch backend publication.
			// Forcing a backend flush boundary on every sync write with WAL off and
			// outer leaves in the value log turns live catch-up into thousands of
			// tiny backend commits, which explodes page count and sparse internal
			// nodes. Keep these writes visible in memory and let checkpoint/rotation
			// establish the backend boundary.
			return nil
		}
		// Journal disabled: enforce a backend flush boundary without fsync.
		return db.flushAllMemtablesForSync(false)
	}
	// Journal disabled: enforce a durable backend boundary.
	return db.Checkpoint()
}

func (db *DB) set(key, value []byte, sync bool) error {
	if handled, err := db.enqueueDomainIngress(domainIngressOpSet, key, value, sync); handled {
		return err
	}
	return db.setDirect(key, value, sync)
}

func (db *DB) setDirect(key, value []byte, sync bool) error {
	db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false
	var ptr page.ValuePtr
	var retainPath string
	usePointer := false
	debugPtr := db.debugFlushPointers

	shard := db.shardForKey(key)

	durability := journalDurabilityNone
	if sync {
		if db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	}
	eligible := db.shouldWriteViaValueLogForKeyValue(key, value)
	valueLogEnabled := db.valueLogEnabled()
	allowPointers := eligible && valueLogEnabled && db.allowValueLogPointers()
	if allowPointers && db.disableJournal && !db.memtableValueLogPointers {
		// WAL-off: when the journal is disabled, defer value-log appends to the flush boundary
		// so repeated overwrites can coalesce in the memtable before hitting disk.
		allowPointers = false
	}
	addBytesForLimit := int64(len(key) + len(value))
	if allowPointers && db.memtableValueLogPointers {
		// Pointer-in-memtable mode stores only the key plus packed pointer payload.
		addBytesForLimit = int64(len(key) + page.ValuePtrSize)
	}
	if maxMemtableBytesPerShard > 0 {
		if addBytesForLimit > maxMemtableBytesPerShard {
			db.writeMu.RUnlock()
			return ErrMemtableFull
		}
		shard.mu.Lock()
		exceedsLimit := db.shardExceedsLimit(shard, addBytesForLimit)
		shard.mu.Unlock()
		if exceedsLimit {
			db.writeMu.RUnlock()
			return ErrMemtableFull
		}
	}
	if debugPtr && eligible {
		db.debugPtrEligible.Add(1)
	}

	var lane *lane
	if allowPointers || !db.disableJournal {
		l, err := db.pickLane(durability == journalDurabilitySync, db.laneForShardIndex(db.shardIndex(key)))
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		lane = l
		if durability == journalDurabilitySync {
			defer db.releaseLaneSync(lane)
		}
	}

	if allowPointers {
		dictID := uint64(0)
		if db.valueLogDictTrain.TrainBytes > 0 {
			id, err := db.currentDictID(context.Background())
			if err != nil {
				db.writeMu.RUnlock()
				return err
			}
			dictID = id
		} else {
			dictID = db.dictCurrentCached.Load()
		}

		rid := db.nextRID.Add(1)
		appendPtr, retain, appendErr := db.appendValueLogOneRaw(lane, dictID, nil, rid, value, durability)
		if appendErr != nil {
			db.writeMu.RUnlock()
			return appendErr
		}
		ptr = appendPtr
		walRID := rid
		retainPath = retain
		usePointer = true
		if debugPtr {
			db.debugPtrUsed.Add(1)
		}

		if !db.disableJournal {
			rec := logRecord{Op: logOpSetRID, Key: key, RID: walRID}
			if err := db.appendWALOne(lane, rec, durability); err != nil {
				db.writeMu.RUnlock()
				return err
			}
		}
	} else if !db.disableJournal {
		if debugPtr && eligible {
			if !valueLogEnabled {
				db.debugPtrDisabled.Add(1)
			} else {
				db.debugPtrDenied.Add(1)
			}
		}
		rec := logRecord{Op: logOpSetInline, Key: key, Value: value}
		if err := db.appendWALOne(lane, rec, durability); err != nil {
			db.writeMu.RUnlock()
			return err
		}
	} else if debugPtr && eligible {
		if !valueLogEnabled {
			db.debugPtrDisabled.Add(1)
		} else {
			db.debugPtrDenied.Add(1)
		}
	}

	shard.mu.Lock()
	if usePointer {
		memVal := []byte(nil)
		if !db.memtableValueLogPointers {
			memVal = value
		}
		if borrower, ok := shard.mem.(memtable.ValueBorrower); ok && len(memVal) > 0 {
			owned := shard.appendOnlyDirectValueArena.alloc(len(memVal))
			copy(owned, memVal)
			borrower.SetEntryBorrowValue(key, owned, ptr, node.FlagPointer)
		} else {
			shard.mem.SetEntry(key, memVal, ptr, node.FlagPointer)
		}
	} else {
		if borrower, ok := shard.mem.(memtable.ValueBorrower); ok && len(value) > 0 {
			owned := shard.appendOnlyDirectValueArena.alloc(len(value))
			copy(owned, value)
			borrower.SetEntryBorrowValue(key, owned, page.ValuePtr{}, node.FlagInline)
		} else {
			shard.mem.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
		}
	}
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()
	db.noteWriteKey(key)

	// 3. Check Threshold
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && db.disableJournal {
		needSyncBarrier = true
	}
	db.writeMu.RUnlock()

	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}

	if needRotate {
		if err := db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	return db.delete(key, false)
}

// DeleteRange deletes all keys in the range [start, end).
//
// When WAL is disabled and the backend is empty, a full-range delete can be
// satisfied by clearing the in-memory layers without enumerating keys.
func (db *DB) DeleteRange(start, end []byte) error {
	if db == nil {
		return nil
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	db.waitForCheckpoint()

	// Journal-enabled mode: do a snapshot scan and apply per-key deletes directly.
	// Append journal records one-by-one to preserve batch atomicity and to avoid
	// post-journal apply divergence on partial batch failure.
	if !db.disableJournal {
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		it, err := db.Iterator(start, end)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		applyDelete := func(key []byte) error {
			if maxMemtableBytesPerShard > 0 && int64(len(key)) > maxMemtableBytesPerShard {
				return ErrMemtableFull
			}
			for {
				shard := db.shardForKey(key)
				shard.mu.Lock()
				if db.shardExceedsLimit(shard, int64(len(key))) {
					shard.mu.Unlock()
					db.mu.Lock()
					err := db.rotateMemtableLocked(true)
					db.mu.Unlock()
					if err != nil {
						return err
					}
					continue
				}
				if err := shard.mem.DeleteWithCallback(key, nil); err != nil {
					shard.mu.Unlock()
					return err
				}
				shard.rng.add(key)
				newBytes := shard.mem.Size()
				delta := newBytes - shard.bytes
				shard.bytes = newBytes
				db.mutableBytes.Add(delta)
				shard.mu.Unlock()
				db.noteWriteKey(key)
				if db.mutableBytes.Load() > db.mutableFlushThreshold() {
					db.mu.Lock()
					if db.mutableBytes.Load() > db.mutableFlushThreshold() {
						if err := db.rotateMemtableLocked(true); err != nil {
							db.mu.Unlock()
							return err
						}
					}
					db.mu.Unlock()
				}
				return nil
			}
		}

		poisonApply := func(err error) error {
			if err == nil {
				return nil
			}
			db.reportError(fmt.Errorf("cachingdb: WAL apply failed: %w", err))
			db.walAckMu.Lock()
			if db.walErr == nil {
				db.walErr = err
			}
			db.walAckMu.Unlock()
			return err
		}

		preRotate := func(key []byte) error {
			if maxMemtableBytesPerShard > 0 && int64(len(key)) > maxMemtableBytesPerShard {
				return ErrMemtableFull
			}
			if db.mutableBytes.Load() > db.mutableFlushThreshold() {
				db.mu.Lock()
				if db.mutableBytes.Load() > db.mutableFlushThreshold() {
					err := db.rotateMemtableLocked(true)
					db.mu.Unlock()
					if err != nil {
						return err
					}
				} else {
					db.mu.Unlock()
				}
			}
			shard := db.shardForKey(key)
			shard.mu.Lock()
			exceeds := db.shardExceedsLimit(shard, int64(len(key)))
			shard.mu.Unlock()
			if exceeds {
				db.mu.Lock()
				err := db.rotateMemtableLocked(true)
				db.mu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil
		}

		lane, err := db.pickLane(false, -1)
		if err != nil {
			return err
		}
		deletedAny := false
		for it.Valid() {
			key := it.Key()
			if err := preRotate(key); err != nil {
				return err
			}
			if err := db.appendWALOne(lane, logRecord{Op: logOpDelete, Key: key}, journalDurabilityNone); err != nil {
				return err
			}
			if err := applyDelete(key); err != nil {
				return poisonApply(err)
			}
			deletedAny = true
			it.Next()
		}
		if err := it.Error(); err != nil {
			return err
		}
		if deletedAny {
			// DeleteRange can emit many non-sync WAL records; flush once so process
			// crashes do not lose buffered tombstones before a later sync op.
			if err := db.flushWALLane(lane); err != nil {
				return err
			}
		}

		db.noteWrite()
		db.maybeAssistFlush()
		return nil
	}

	// Ensure we know whether the backend currently contains any keys so that we
	// can safely take the "clear memtables" fast path on empty backends.
	if err := db.ensureBackendRange(); err != nil {
		return err
	}

	// Fast path: when the journal is disabled and there is no in-memory state to merge,
	// avoid snapshot isolation/merge iterators and delete directly from the
	// backend in a single commit.
	//
	// This is safe only when we have no queued memtables and the mutable memtable
	// is empty; otherwise we'd violate "newest wins" semantics.
	if db.disableJournal {
		db.mu.Lock()
		backendOnly := len(db.queue) == 0 && db.mutableBytes.Load() == 0
		db.mu.Unlock()
		if backendOnly {
			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			defer func() { _ = it.Close() }()

			b := db.backend.NewBatch()
			defer func() { _ = b.Close() }()
			for it.Valid() {
				if err := b.Delete(it.Key()); err != nil {
					return err
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
			if err := b.Write(); err != nil {
				return err
			}
			// Best-effort: backend range can shrink; force recompute later.
			db.mu.Lock()
			db.backendRangeKnown = false
			db.backendRange = keyRange{}
			db.mu.Unlock()
			return nil
		}
	}

	// Serialize against flushers while we inspect/clear the in-memory layers.
	db.flushMu.Lock()
	db.mu.Lock()

	// Compute overall min/max across in-memory layers.
	var (
		haveAny bool
		minKey  []byte
		maxKey  []byte
	)
	addRange := func(r keyRange) {
		if !r.valid {
			return
		}
		if !haveAny {
			haveAny = true
			minKey = r.min
			maxKey = r.max
			return
		}
		if bytes.Compare(r.min, minKey) < 0 {
			minKey = r.min
		}
		if bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}

	mutableRange := db.snapshotMutableRange()
	addRange(mutableRange)
	for _, r := range db.queueRanges {
		addRange(r)
	}

	backendEmpty := db.backendRangeKnown && !db.backendRange.valid
	if !backendEmpty {
		addRange(db.backendRange)
	}

	if haveAny {
		coversAll := true
		if start != nil && bytes.Compare(start, minKey) > 0 {
			coversAll = false
		}
		if end != nil && bytes.Compare(end, maxKey) <= 0 {
			// end is exclusive; to cover maxKey it must be strictly greater.
			coversAll = false
		}

		// Fast path: if the backend is empty and the delete range covers all keys we
		// currently have buffered in memory, just drop the in-memory state. This
		// avoids iterator creation, merges, and per-key tombstones.
		if coversAll && db.disableJournal && backendEmpty {
			curMode := db.currentMemtableMode()
			nextMode := curMode
			if db.memtableAdaptive {
				nextMode = db.applyAdaptiveMemtableModeLocked()
				db.memtableWarmupActive = false
				db.updateAdaptiveObservationLocked()
			}

			db.queueRetiredMemtablesLocked(db.queue)
			db.queue = nil
			db.queueShardIDs = nil
			db.queueLaneIDs = nil
			db.queueIDs = nil
			db.queueEnqueueNS = nil
			db.queueRanges = nil
			db.queueWALPaths = nil
			db.queueValueLogPaths = nil
			db.queueBacklogBytes.Store(0)
			db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
			if err := db.resetMutableShardsLocked(nextMode, nextMode == curMode); err != nil {
				db.mu.Unlock()
				db.flushMu.Unlock()
				return err
			}

			db.mu.Unlock()
			db.flushMu.Unlock()
			return nil
		}
	}

	db.mu.Unlock()
	db.flushMu.Unlock()

	// When the journal is disabled (op-geth style "unsafe" mode), avoid snapshot
	// isolation and MergingIterator overhead. DeleteRange doesn't require sorted
	// enumeration across sources; we can scan each source independently and write
	// tombstones into the current mutable memtable.
	if db.disableJournal {
		// Serialize writers and flushers while we enumerate keys to delete and
		// apply tombstones. This keeps snapshot semantics simple and avoids
		// rotating the memtable (which can allocate large arenas).
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		db.mu.Lock()
		mutableRange := db.snapshotMutableRange()
		coversInMemory := queryCoversRange(start, end, mutableRange)
		for _, r := range db.queueRanges {
			if !queryCoversRange(start, end, r) {
				coversInMemory = false
				break
			}
		}

		// Fast path (DisableWAL only): if the delete range covers all buffered
		// in-memory keys, clear the in-memory layers and delete directly from the
		// backend. This avoids building large tombstone sets and avoids per-key
		// copies into an intermediate slice.
		if coversInMemory {
			curMode := db.currentMemtableMode()
			nextMode := curMode
			if db.memtableAdaptive {
				nextMode = db.applyAdaptiveMemtableModeLocked()
				db.memtableWarmupActive = false
				db.updateAdaptiveObservationLocked()
			}

			db.queueRetiredMemtablesLocked(db.queue)
			db.queue = nil
			db.queueShardIDs = nil
			db.queueLaneIDs = nil
			db.queueIDs = nil
			db.queueEnqueueNS = nil
			db.queueRanges = nil
			db.queueWALPaths = nil
			db.queueValueLogPaths = nil
			db.queueBacklogBytes.Store(0)
			db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
			if err := db.resetMutableShardsLocked(nextMode, nextMode == curMode); err != nil {
				db.mu.Unlock()
				return err
			}
			db.mu.Unlock()

			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			defer func() { _ = it.Close() }()

			b := db.backend.NewBatch()
			defer func() { _ = b.Close() }()

			for it.Valid() {
				if err := b.Delete(it.Key()); err != nil {
					return err
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
			if err := b.Write(); err != nil {
				return err
			}

			// Best-effort: backend range can shrink; force recompute later.
			db.mu.Lock()
			db.backendRangeKnown = false
			db.backendRange = keyRange{}
			db.mu.Unlock()

			db.noteWrite()
			return nil
		}

		backendRange := db.backendRange

		// If we need to enumerate keys from the current mutable memtable, rotate it
		// first so we never mutate a memtable while iterating it.
		if overlapsQuery(start, end, mutableRange) && db.mutableBytes.Load() > 0 {
			if queryCoversRange(start, end, mutableRange) {
				if err := db.resetMutableShardsLocked(db.currentMemtableMode(), true); err != nil {
					db.mu.Unlock()
					return err
				}
			} else {
				if err := db.rotateMemtableLocked(false); err != nil {
					db.mu.Unlock()
					return err
				}
			}
			mutableRange = db.snapshotMutableRange()
		}

		// Drop fully-covered queued memtables without enumerating their keys.
		if len(db.queue) > 0 {
			db.ensureQueueLaneIDsLocked()
			dstQueue := db.queue[:0]
			dstShardIDs := db.queueShardIDs[:0]
			dstLaneIDs := db.queueLaneIDs[:0]
			dstIDs := db.queueIDs[:0]
			dstEnqueueNS := db.queueEnqueueNS[:0]
			dstRanges := db.queueRanges[:0]
			dstWALPaths := db.queueWALPaths[:0]
			dstValueLogPaths := db.queueValueLogPaths[:0]
			for i, mem := range db.queue {
				r := keyRange{}
				if i < len(db.queueRanges) {
					r = db.queueRanges[i]
				}
				if queryCoversRange(start, end, r) {
					db.queueRetiredMemtableLocked(mem)
					db.queueBacklogBytes.Add(-mem.Size())
					continue
				}
				dstQueue = append(dstQueue, mem)
				if i < len(db.queueShardIDs) {
					dstShardIDs = append(dstShardIDs, db.queueShardIDs[i])
				} else {
					dstShardIDs = append(dstShardIDs, 0)
				}
				if i < len(db.queueLaneIDs) {
					dstLaneIDs = append(dstLaneIDs, db.queueLaneIDs[i])
				} else {
					dstLaneIDs = append(dstLaneIDs, 0)
				}
				if i < len(db.queueIDs) {
					dstIDs = append(dstIDs, db.queueIDs[i])
				} else {
					dstIDs = append(dstIDs, 0)
				}
				if i < len(db.queueEnqueueNS) {
					dstEnqueueNS = append(dstEnqueueNS, db.queueEnqueueNS[i])
				} else {
					dstEnqueueNS = append(dstEnqueueNS, 0)
				}
				dstRanges = append(dstRanges, r)
				if i < len(db.queueWALPaths) {
					dstWALPaths = append(dstWALPaths, db.queueWALPaths[i])
				} else {
					dstWALPaths = append(dstWALPaths, nil)
				}
				if i < len(db.queueValueLogPaths) {
					dstValueLogPaths = append(dstValueLogPaths, db.queueValueLogPaths[i])
				} else {
					dstValueLogPaths = append(dstValueLogPaths, nil)
				}
			}
			db.queue = dstQueue
			db.queueShardIDs = dstShardIDs
			db.queueLaneIDs = dstLaneIDs
			db.queueIDs = dstIDs
			db.queueEnqueueNS = dstEnqueueNS
			db.queueRanges = dstRanges
			db.queueWALPaths = dstWALPaths
			db.queueValueLogPaths = dstValueLogPaths
			if len(db.queue) == 0 {
				db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
			}
		}
		db.publishMemtablesLocked()

		// Snapshot sources after any rotations/drops.
		mutableHasData := db.mutableBytes.Load() > 0
		mutableRanges := make([]keyRange, len(db.mutableShards))
		mutables := make([]memtable.Table, len(db.mutableShards))
		for i := range db.mutableShards {
			shard := &db.mutableShards[i]
			shard.mu.Lock()
			mutables[i] = shard.mem
			mutableRanges[i] = cloneRange(shard.rng)
			shard.mu.Unlock()
		}
		queue := append([]memtable.Table(nil), db.queue...)
		queueRanges := append([]keyRange(nil), db.queueRanges...)
		db.mu.Unlock()

		var (
			backendIter  iterator.UnsafeIterator
			queueIters   []iterator.UnsafeIterator
			mutableIters []iterator.UnsafeIterator
		)

		if overlapsQuery(start, end, backendRange) {
			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			backendIter = it
			defer func() { _ = backendIter.Close() }()
		}

		if mutableHasData {
			for i, mem := range mutables {
				if mem == nil {
					continue
				}
				if i < len(mutableRanges) && !overlapsQuery(start, end, mutableRanges[i]) {
					continue
				}
				it := mem.NewIterator(start, end)
				it.Seek(start)
				mutableIters = append(mutableIters, it)
				defer func(it iterator.UnsafeIterator) { _ = it.Close() }(it)
			}
		}

		for i, mem := range queue {
			if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
				continue
			}
			it := mem.NewIterator(start, end)
			it.Seek(start)
			queueIters = append(queueIters, it)
			defer func(it iterator.UnsafeIterator) { _ = it.Close() }(it)
		}

		db.mu.Lock()
		applyDelete := func(key []byte) error {
			shard := db.shardForKey(key)
			shard.mu.Lock()
			if db.shardExceedsLimit(shard, int64(len(key))) {
				shard.mu.Unlock()
				return ErrMemtableFull
			}
			shard.mem.Delete(key)
			shard.rng.add(key)
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			db.mutableBytes.Add(delta)
			shard.mu.Unlock()
			if db.mutableBytes.Load() > db.mutableFlushThreshold() {
				if err := db.rotateMemtableLocked(true); err != nil {
					return err
				}
			}
			return nil
		}

		for _, it := range mutableIters {
			for it.Valid() {
				if !it.IsDeleted() {
					if err := applyDelete(it.UnsafeKey()); err != nil {
						db.mu.Unlock()
						return err
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		if backendIter != nil {
			for backendIter.Valid() {
				if err := applyDelete(backendIter.Key()); err != nil {
					db.mu.Unlock()
					return err
				}
				backendIter.Next()
			}
			if err := backendIter.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		for _, it := range queueIters {
			for it.Valid() {
				if !it.IsDeleted() {
					if err := applyDelete(it.UnsafeKey()); err != nil {
						db.mu.Unlock()
						return err
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}
		db.mu.Unlock()

		db.noteWrite()
		db.maybeAssistFlush()
		return nil
	}

	// Fallback: enumerate keys via a snapshot iterator.
	it, err := db.Iterator(start, end)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	b := db.NewBatch()
	defer b.Close()
	for it.Valid() {
		if err := b.Delete(it.Key()); err != nil {
			return err
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

func (db *DB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	return db.delete(key, true)
}

func (db *DB) delete(key []byte, sync bool) error {
	if handled, err := db.enqueueDomainIngress(domainIngressOpDelete, key, nil, sync); handled {
		return err
	}
	return db.deleteDirect(key, sync)
}

func (db *DB) deleteDirect(key []byte, sync bool) error {
	db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false

	shard := db.shardForKey(key)
	shard.mu.Lock()
	if db.shardExceedsLimit(shard, int64(len(key))) {
		shard.mu.Unlock()
		db.writeMu.RUnlock()
		return ErrMemtableFull
	}
	shard.mu.Unlock()

	if !db.disableJournal {
		durability := journalDurabilityNone
		if sync {
			if db.relaxedSync {
				durability = journalDurabilityFlush
			} else {
				durability = journalDurabilitySync
			}
		}
		lane, err := db.pickLane(durability == journalDurabilitySync, db.laneForShardIndex(db.shardIndex(key)))
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		if durability == journalDurabilitySync {
			defer db.releaseLaneSync(lane)
		}
		rec := logRecord{Op: logOpDelete, Key: key}
		if err := db.appendWALOne(lane, rec, durability); err != nil {
			db.writeMu.RUnlock()
			return err
		}
	}

	shard.mu.Lock()
	shard.mem.Delete(key)
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()
	db.noteWriteKey(key)

	// 3. Threshold
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && db.disableJournal {
		needSyncBarrier = true
	}
	db.writeMu.RUnlock()

	if needRotate {
		if err := db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) canReuseWALSegments() bool {
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		w := l.wal
		live := l.walLiveBytes.Load()
		l.walMu.Unlock()
		if w == nil {
			return false
		}
		if live >= 10*1024*1024 {
			return false
		}
	}
	return true
}

func (db *DB) rotateMemtableLockedWithCapacity(triggerFlush bool, newCapacity int) error {
	var walPaths []string
	var valueLogPaths []string
	debugRotate := debugMemtableRotateOn()
	retiredMems := make([]memtable.Table, 0, len(db.mutableShards))
	if !db.disableJournal {
		walPaths = db.currentWALPaths()
	}
	if db.valueLogEnabled() {
		valueLogPaths = db.currentValueLogPaths()
	}
	if newCapacity < 0 {
		newCapacity = db.memtableCap
	}
	if db.memtableAdaptive {
		db.applyAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
		db.updateAdaptiveObservationLocked()
	}
	if debugRotate {
		db.debugMemtableRotatef(
			"event=start path=with_capacity trigger_flush=%t new_capacity=%d mode=%s mutable_bytes=%d queue_len=%d queue_backlog_bytes=%d",
			triggerFlush,
			newCapacity,
			db.currentMemtableMode().String(),
			db.mutableBytes.Load(),
			len(db.queue),
			db.queueBacklogBytes.Load(),
		)
	}
	db.mutableBytes.Store(0)
	enqueueNS := time.Now().UnixNano()
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		oldMode := ""
		oldLen := shard.mem.Len()
		if debugRotate {
			oldMode = debugMemtableModeLabel(shard.mem)
		}
		oldShardBytes := shard.bytes
		db.retainMutableShardAppendOnlyArenaLocked(i, shard)
		shard.mem.Freeze()
		memBytes := shard.mem.Size()
		queueLaneID := db.laneForShardIndex(i)
		enqueueShard := memBytes > 0 || oldLen > 0
		if enqueueShard {
			db.queue = append(db.queue, shard.mem)
			db.queueShardIDs = append(db.queueShardIDs, uint16(i))
			db.queueLaneIDs = append(db.queueLaneIDs, uint16(queueLaneID))
			db.queueIDs = append(db.queueIDs, db.nextQueueID.Add(1))
			db.queueEnqueueNS = append(db.queueEnqueueNS, enqueueNS)
			db.queueBacklogBytes.Add(memBytes)
			db.queueRanges = append(db.queueRanges, shard.rng)
			db.queueWALPaths = append(db.queueWALPaths, walPaths)
			db.queueValueLogPaths = append(db.queueValueLogPaths, valueLogPaths)
		} else {
			retiredMems = append(retiredMems, shard.mem)
		}

		mt, err := db.newMutableMemtableWithCapacityMode(newCapacity, db.currentMemtableMode())
		if err != nil {
			shard.mu.Unlock()
			return err
		}
		shard.mem = mt
		shard.rng = keyRange{}
		shard.bytes = 0
		if debugRotate {
			db.debugMemtableRotatef(
				"event=shard path=with_capacity shard=%d lane=%d old_mode=%s old_len=%d old_size=%d old_bytes=%d new_mode=%s new_capacity=%d queue_len=%d queue_backlog_bytes=%d",
				i,
				queueLaneID,
				oldMode,
				oldLen,
				memBytes,
				oldShardBytes,
				debugMemtableModeLabel(mt),
				newCapacity,
				len(db.queue),
				db.queueBacklogBytes.Load(),
			)
		}
		shard.mu.Unlock()
	}
	if len(retiredMems) > 0 {
		db.pendingRetiredMems = append(db.pendingRetiredMems, retiredMems...)
	}
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	if debugRotate {
		db.debugMemtableRotatef(
			"event=done path=with_capacity trigger_flush=%t queue_len=%d queue_backlog_bytes=%d mutable_threshold=%d",
			triggerFlush,
			len(db.queue),
			db.queueBacklogBytes.Load(),
			db.mutableFlushThreshold(),
		)
	}

	// Optimization: Reuse WAL if small (e.g. < 10MB) to avoid syscall overhead
	// on frequent rotations (e.g. caused by frequent Iterator creation).
	if !db.disableJournal {
		if db.canReuseWALSegments() {
			if triggerFlush {
				select {
				case db.flushCh <- struct{}{}:
				default:
				}
			}
			return nil
		}
		for i := range db.lanes {
			if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
				return err
			}
		}
	}

	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	db.bpMu.Lock()
	db.bpCond.Broadcast()
	db.bpMu.Unlock()
	return nil
}

// rotateMemtableLockedForIterator rotates the current mutable memtable into the
// immutable queue for snapshot iteration without rotating the WAL segment.
//
// This is important for concurrency: iterator creation should not need to
// serialize behind writeMu just to protect WAL rotation.
//
// Caller must hold db.mu.
func (db *DB) rotateMemtableLockedForIterator(newCapacity int) error {
	// Iterator snapshot rotation can be invoked repeatedly under read-heavy
	// traffic. If we already have queued immutable shards, request a background
	// flush so repeated snapshot rotations do not let queue depth grow
	// unboundedly while still preserving snapshot semantics.
	triggerFlush := len(db.queue) > 0
	return db.rotateMutableShardsLocked(newCapacity, triggerFlush)
}

func (db *DB) rotateMemtableLocked(triggerFlush bool) error {
	return db.rotateMemtableLockedWithCapacity(triggerFlush, -1)
}

func (db *DB) rotateMemtableIfNeeded(triggerFlush bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.mutableBytes.Load() <= db.mutableFlushThreshold() {
		return nil
	}
	return db.rotateMutableShardsLocked(-1, triggerFlush)
}

func (db *DB) maybeRotateMemtable(triggerFlush bool) error {
	if db.mutableBytes.Load() <= db.mutableFlushThreshold() {
		return nil
	}
	if !db.rotatePending.CompareAndSwap(false, true) {
		return nil
	}
	defer db.rotatePending.Store(false)
	return db.rotateMemtableIfNeeded(triggerFlush)
}

// rotateMutableShardsLocked rotates the current mutable shards into the queue
// while holding db.mu (write) and the affected shard locks.
//
// It intentionally does not rotate the WAL segment; checkpoint is responsible
// for establishing durable boundaries and trimming old segments. This avoids
// requiring a global writer barrier around WAL rotation.
func (db *DB) rotateMutableShardsLocked(newCapacity int, triggerFlush bool) error {
	debugRotate := debugMemtableRotateOn()
	retiredMems := make([]memtable.Table, 0, len(db.mutableShards))
	if newCapacity < 0 {
		newCapacity = db.memtableCap
	}
	if db.memtableAdaptive {
		db.applyAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
		db.updateAdaptiveObservationLocked()
	}
	var walPaths []string
	if !db.disableJournal {
		walPaths = db.currentWALPaths()
	}
	var valueLogPaths []string
	if db.valueLogEnabled() {
		valueLogPaths = db.currentValueLogPaths()
	}
	if debugRotate {
		db.debugMemtableRotatef(
			"event=start path=mutable_shards trigger_flush=%t new_capacity=%d mode=%s mutable_bytes=%d queue_len=%d queue_backlog_bytes=%d",
			triggerFlush,
			newCapacity,
			db.currentMemtableMode().String(),
			db.mutableBytes.Load(),
			len(db.queue),
			db.queueBacklogBytes.Load(),
		)
	}

	locked := make([]*memShard, 0, len(db.mutableShards))
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].mu.Unlock()
		}
	}()
	enqueueNS := time.Now().UnixNano()

	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		locked = append(locked, shard)
		oldMode := ""
		oldLen := shard.mem.Len()
		if debugRotate {
			oldMode = debugMemtableModeLabel(shard.mem)
		}
		if _, ok := shard.mem.(*memtable.AppendOnly); ok {
			db.observeAppendOnlyMutableEntries(oldLen)
		}
		oldShardBytes := shard.bytes

		// Remove this shard's contribution from the global byte counter before
		// resetting it, since writers may still be updating other shards.
		if shard.bytes != 0 {
			db.mutableBytes.Add(-shard.bytes)
		}

		// Freeze and enqueue the old mutable shard.
		db.retainMutableShardAppendOnlyArenaLocked(i, shard)
		shard.mem.Freeze()
		memBytes := shard.mem.Size()
		queueLaneID := db.laneForShardIndex(i)
		enqueueShard := memBytes > 0 || oldLen > 0
		if enqueueShard {
			db.queue = append(db.queue, shard.mem)
			db.queueShardIDs = append(db.queueShardIDs, uint16(i))
			db.queueLaneIDs = append(db.queueLaneIDs, uint16(queueLaneID))
			db.queueIDs = append(db.queueIDs, db.nextQueueID.Add(1))
			db.queueEnqueueNS = append(db.queueEnqueueNS, enqueueNS)
			db.queueBacklogBytes.Add(memBytes)
			db.queueRanges = append(db.queueRanges, shard.rng)
			db.queueWALPaths = append(db.queueWALPaths, walPaths)
			db.queueValueLogPaths = append(db.queueValueLogPaths, valueLogPaths)
		} else {
			retiredMems = append(retiredMems, shard.mem)
		}

		mt, err := db.newMutableMemtableWithCapacityMode(newCapacity, db.currentMemtableMode())
		if err != nil {
			return err
		}
		shard.mem = mt
		shard.rng = keyRange{}
		shard.bytes = 0
		if debugRotate {
			db.debugMemtableRotatef(
				"event=shard path=mutable_shards shard=%d lane=%d old_mode=%s old_len=%d old_size=%d old_bytes=%d new_mode=%s new_capacity=%d queue_len=%d queue_backlog_bytes=%d",
				i,
				queueLaneID,
				oldMode,
				oldLen,
				memBytes,
				oldShardBytes,
				debugMemtableModeLabel(mt),
				newCapacity,
				len(db.queue),
				db.queueBacklogBytes.Load(),
			)
		}
	}
	if len(retiredMems) > 0 {
		db.pendingRetiredMems = append(db.pendingRetiredMems, retiredMems...)
	}

	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	if debugRotate {
		db.debugMemtableRotatef(
			"event=done path=mutable_shards trigger_flush=%t queue_len=%d queue_backlog_bytes=%d mutable_threshold=%d",
			triggerFlush,
			len(db.queue),
			db.queueBacklogBytes.Load(),
			db.mutableFlushThreshold(),
		)
	}

	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	db.bpMu.Lock()
	db.bpCond.Broadcast()
	db.bpMu.Unlock()
	return nil
}

func (db *DB) cleanupLaneWALWriters(l *lane) {
	if l == nil {
		return
	}
	l.walMu.Lock()
	if l.wal != nil {
		_ = l.wal.Close()
		l.wal = nil
	}
	l.walMu.Unlock()
	l.vlogMu.Lock()
	if l.vlog != nil {
		_ = l.vlog.Close()
		l.vlog = nil
		l.vlogCaps = vlogWriterCaps{}
	}
	l.vlogModeSet = false
	l.vlogModeWriter = nil
	l.vlogMu.Unlock()
}

func (db *DB) defaultVlogWriteMode() vlogCompressionWriteMode {
	if db == nil {
		return vlogWriteOff
	}
	switch normalizeVlogCompressionMode(db.valueLogCompressionMode) {
	case vlogCompressionBlock:
		return vlogWriteBlock
	case vlogCompressionDict:
		return vlogWriteDict
	default:
		return vlogWriteOff
	}
}

func (db *DB) setVlogWriterMode(l *lane, w valueWriter, mode vlogCompressionWriteMode, codec valuelog.BlockCodec) {
	if db == nil || w == nil {
		return
	}
	if l != nil && l.vlogModeSet && l.vlogModeWriter == w && l.vlogMode == mode && l.vlogBlockCodec == codec {
		return
	}
	setter, ok := any(w).(blockCompressionSetter)
	if !ok {
		return
	}
	setter.SetBlockCompression(codec, mode == vlogWriteBlock)
	if l != nil {
		l.vlogModeWriter = w
		l.vlogModeSet = true
		l.vlogMode = mode
		l.vlogBlockCodec = codec
	}
}

func (db *DB) rotateWALLocked(l *lane) error {
	return db.rotateWALLockedWithOptions(l, true)
}

func (db *DB) rotateWALCheckpointLocked(l *lane) error {
	return db.rotateWALLockedWithOptions(l, false)
}

func (db *DB) rotateWALLockedWithOptions(l *lane, rotateValueLog bool) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	l.walMu.Lock()
	defer l.walMu.Unlock()
	nextSeq := l.walSeq + 1
	name := commitLogName(l.id, nextSeq)
	path := filepath.Join(db.dir, name)

	if l.wal != nil {
		oldPath := l.walPath
		oldSize := l.wal.Size()
		if err := l.wal.RotateTo(path); err != nil {
			return err
		}
		l.walSeq = nextSeq
		l.walLiveBytes.Store(0)
		if oldPath != "" {
			if l.walClosedSizes == nil {
				l.walClosedSizes = make(map[string]int64)
			}
			prev := l.walClosedSizes[oldPath]
			l.walClosedSizes[oldPath] = oldSize
			l.walClosedBytes.Add(oldSize - prev)
		}
	} else {
		w, err := commitlog.NewWriterWithOptions(path, commitlog.Options{MaxSegmentSize: db.walMaxSegmentBytes, Compress: db.journalCompression})
		if err != nil {
			return err
		}
		l.wal = w
		l.walSeq = nextSeq
		l.walLiveBytes.Store(0)
	}
	l.walPath = path
	l.walLiveBytes.Store(0)
	if rotateValueLog && db.splitValueLogEnabled() {
		if err := db.rotateValueLogLocked(l); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) rotateValueLogLocked(l *lane) error {
	if !db.splitValueLogEnabled() {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	return db.rotateValueLogMuHeld(l)
}

func (db *DB) rotateValueLogMuHeld(l *lane) error {
	nextSeq := l.vlogSeq + 1
	oldSeq := l.vlogSeq
	oldPath := l.vlogPath
	oldLiveBytes := l.vlogLiveBytes.Load()
	var (
		closedPrev    int64
		hadClosedPrev bool
	)
	name := valueLogName(l.id, nextSeq)
	path := filepath.Join(db.dir, name)
	fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(nextSeq))
	if err != nil {
		return err
	}

	if l.vlog != nil {
		oldSize := l.vlog.Size()
		if err := l.vlog.RotateTo(path, fileID); err != nil {
			return err
		}
		l.vlogRotateTotal.Add(1)
		// Avoid counting an "idle rotation" when there was no previous segment.
		if oldPath != "" && oldLiveBytes <= 0 {
			l.vlogRotateIdleTotal.Add(1)
		}
		l.vlogSeq = nextSeq
		l.vlog.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		// Rotation can reset writer internals; force mode reapply.
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		db.setVlogWriterMode(l, l.vlog, db.defaultVlogWriteMode(), db.valueLogBlockCodec)
		if setter, ok := any(l.vlog).(rawWritevStrategySetter); ok {
			setter.SetRawWritevStrategy(db.valueLogRawWritevMinAvgBytes, db.valueLogRawWritevMinRecords)
		}
		l.vlogLiveBytes.Store(0)
		if oldPath != "" {
			if l.vlogClosedSizes == nil {
				l.vlogClosedSizes = make(map[string]int64)
			}
			prev, ok := l.vlogClosedSizes[oldPath]
			closedPrev = prev
			hadClosedPrev = ok
			l.vlogClosedSizes[oldPath] = oldSize
			l.vlogClosedBytes.Add(oldSize - prev)
			if oldPath == l.vlogRetainedPath {
				db.valueLogRetainedClosedBytes.Add(oldSize - prev)
			}
		}
	} else {
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			return err
		}
		// Segment creation is treated as a rotation from "no current segment" for observability.
		l.vlogRotateTotal.Add(1)
		// Avoid counting an "idle rotation" when there was no previous segment.
		if oldPath != "" && oldLiveBytes <= 0 {
			l.vlogRotateIdleTotal.Add(1)
		}
		w.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		db.setVlogWriterMode(l, w, db.defaultVlogWriteMode(), db.valueLogBlockCodec)
		w.SetRawWritevStrategy(db.valueLogRawWritevMinAvgBytes, db.valueLogRawWritevMinRecords)
		l.vlog = w
		l.vlogSeq = nextSeq
		l.vlogLiveBytes.Store(0)
	}
	if err := db.registerValueLogSegment(path, fileID); err != nil {
		if oldPath != "" {
			if hadClosedPrev {
				l.vlogClosedSizes[oldPath] = closedPrev
			} else {
				delete(l.vlogClosedSizes, oldPath)
			}
			curClosed := int64(0)
			if l.vlog != nil {
				curClosed = l.vlog.Size()
			}
			l.vlogClosedBytes.Add(closedPrev - curClosed)
			if oldPath == l.vlogRetainedPath {
				db.valueLogRetainedClosedBytes.Add(closedPrev - curClosed)
			}
		}
		if rollbackErr := db.restoreValueLogWriterMuHeld(l, oldPath, oldSeq); rollbackErr != nil {
			l.vlogSeq = oldSeq
			l.vlogPath = oldPath
			l.vlogLiveBytes.Store(0)
			return errors.Join(err, rollbackErr)
		}
		l.vlogSeq = oldSeq
		l.vlogPath = oldPath
		l.vlogLiveBytes.Store(0)
		return err
	}
	l.vlogPath = path
	l.vlogLiveBytes.Store(0)
	return nil
}

func (db *DB) restoreValueLogWriterMuHeld(l *lane, path string, seq int) error {
	if l == nil {
		return nil
	}
	if l.vlog != nil {
		_ = l.vlog.Close()
		l.vlog = nil
	}
	l.vlogCaps = vlogWriterCaps{}
	l.vlogModeSet = false
	l.vlogModeWriter = nil
	if path == "" || seq == 0 {
		return nil
	}
	fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(seq))
	if err != nil {
		return err
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		return err
	}
	w.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
	db.setVlogWriterMode(l, w, db.defaultVlogWriteMode(), db.valueLogBlockCodec)
	w.SetRawWritevStrategy(db.valueLogRawWritevMinAvgBytes, db.valueLogRawWritevMinRecords)
	l.vlog = w
	l.vlogCaps = computeVlogWriterCaps(w)
	return nil
}

func (db *DB) registerValueLogSegment(path string, fileID uint32) error {
	if db == nil {
		return nil
	}
	if path == "" || fileID == 0 {
		return fmt.Errorf("invalid value-log segment registration: path=%q file_id=%d", path, fileID)
	}
	if db.valueLogReader != nil {
		if err := db.valueLogReader.RegisterSegment(path, fileID); err != nil {
			return err
		}
		if err := db.valueLogReader.PromoteCurrentWritable(fileID); err != nil {
			_ = db.valueLogReader.RemoveSegment(fileID)
			return err
		}
	}
	if registrar, ok := db.backend.(interface {
		RegisterValueLogSegment(path string, fileID uint32) error
	}); ok {
		if err := registrar.RegisterValueLogSegment(path, fileID); err != nil {
			if db.valueLogReader != nil {
				_ = db.valueLogReader.RemoveSegment(fileID)
			}
			return err
		}
	}
	return nil
}

func (db *DB) rotateValueLogForMaxSegmentMuHeld(l *lane, w valueWriter) error {
	if db == nil || l == nil || w == nil {
		return nil
	}
	maxBytes := db.valueLogMaxSegmentBytesForLane(l)
	if maxBytes <= 0 {
		return nil
	}
	if w.Size() <= maxBytes {
		return nil
	}
	return db.rotateValueLogMuHeld(l)
}

func (db *DB) untrackWALSegmentLocked(path string) {
	laneID, _, _, ok := parseLogSeq(filepath.Base(path))
	if !ok || laneID < 0 || laneID >= len(db.lanes) {
		return
	}
	l := &db.lanes[laneID]
	l.walMu.Lock()
	defer l.walMu.Unlock()
	if l.walClosedSizes == nil || path == "" {
		return
	}
	size, ok := l.walClosedSizes[path]
	if !ok {
		return
	}
	delete(l.walClosedSizes, path)
	for {
		cur := l.walClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if l.walClosedBytes.CompareAndSwap(cur, next) {
			break
		}
	}
}

func (db *DB) untrackValueLogSegmentLocked(path string) {
	laneID, _, _, ok := parseLogSeq(filepath.Base(path))
	if !ok || laneID < 0 || laneID >= len(db.lanes) {
		return
	}
	l := &db.lanes[laneID]
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	if l.vlogClosedSizes == nil || path == "" {
		return
	}
	size, ok := l.vlogClosedSizes[path]
	if !ok {
		return
	}
	delete(l.vlogClosedSizes, path)
	db.valueLogRetainedClosedBytes.Add(-size)
	for {
		cur := l.vlogClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if l.vlogClosedBytes.CompareAndSwap(cur, next) {
			break
		}
	}
}

func (db *DB) flushLoop() {
	defer db.wg.Done()

	for {
		select {
		case <-db.closeCh:
			// Flush all remaining with sync on close.
			db.flushAll(true)
			return
		case <-db.flushCh:
			// Background flush is async when WAL is enabled. Without a WAL, we
			// upgrade to a synced flush unless RelaxedSync is set.
			db.flushAll(false)
		}
	}
}

func (db *DB) flushSyncRequested(sync bool) bool {
	return sync && !db.relaxedSync
}

func (db *DB) pickFlushLane() (int, bool) {
	db.mu.RLock()
	if len(db.queue) == 0 {
		db.mu.RUnlock()
		return 0, false
	}
	laneCount := len(db.lanes)
	if laneCount == 0 {
		laneCount = 1
	}
	counts := make([]int, laneCount)
	for i := range db.queue {
		laneID := 0
		if i < len(db.queueLaneIDs) {
			laneID = int(db.queueLaneIDs[i])
		}
		if laneID < 0 || laneID >= laneCount {
			laneID = 0
		}
		counts[laneID]++
	}
	bestLane := 0
	bestCount := 0
	for laneID, count := range counts {
		if count > bestCount {
			bestCount = count
			bestLane = laneID
		}
	}
	db.mu.RUnlock()
	return bestLane, true
}

func (db *DB) flushAll(reqSync bool) {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.flushAllLocked(reqSync)
}

func (db *DB) flushAllLocked(reqSync bool) {
	origSync := reqSync
	syncFlag := db.flushSyncRequested(reqSync)
	if !origSync && syncFlag && db.disableJournal && !db.relaxedSync {
		db.debugVlogEvent("flushAll_upgraded_sync", -1, "flushMu")
	}
	lanes := len(db.lanes)
	if lanes == 0 {
		lanes = 1
	}

	// Only spawn flush workers for lanes that actually have queued memtables.
	// Otherwise each lane does an O(queueLen) scan in collectFlushUnitsLocked to
	// discover there's nothing to do, which can be extremely expensive when the
	// queue is large and lanes > 1.
	active := make([]bool, lanes)
	db.mu.RLock()
	queueLen := len(db.queue)
	for i := 0; i < queueLen; i++ {
		laneID := 0
		if i < len(db.queueLaneIDs) {
			laneID = int(db.queueLaneIDs[i])
		}
		if laneID < 0 || laneID >= lanes {
			laneID = 0
		}
		active[laneID] = true
	}
	db.mu.RUnlock()

	activeCount := 0
	for i := range active {
		if active[i] {
			activeCount++
		}
	}
	if activeCount == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(activeCount)
	for i := 0; i < lanes; i++ {
		laneID := i
		if !active[laneID] {
			continue
		}
		go func() {
			if laneID < len(db.flushLaneMu) {
				db.flushLaneMu[laneID].Lock()
				defer db.flushLaneMu[laneID].Unlock()
			}
			for db.flushLaneOnce(syncFlag, laneID) {
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if !db.checkpointing.Load() {
		db.mu.RLock()
		queueEmpty := len(db.queue) == 0
		db.mu.RUnlock()
		if queueEmpty {
			db.trimRetainedArenasAfterFlush(false)
		}
	}
}

func (db *DB) flushOne() bool {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	laneID, ok := db.pickFlushLane()
	if !ok {
		return false
	}
	if laneID < len(db.flushLaneMu) {
		db.flushLaneMu[laneID].Lock()
		defer db.flushLaneMu[laneID].Unlock()
	}
	flushed := db.flushLaneOnce(true, laneID)
	if flushed && !db.checkpointing.Load() {
		db.mu.RLock()
		queueEmpty := len(db.queue) == 0
		db.mu.RUnlock()
		if queueEmpty {
			db.trimRetainedArenasAfterFlush(false)
		}
	}
	return flushed
}

const (
	flushCombineTargetBytes    int64 = 64 * 1024 * 1024  // 64MiB
	flushCombineTargetBytesMax       = 256 * 1024 * 1024 // 256MiB
	flushCombineMaxMemtables         = 32
	// flushBackendBatchMaxEntries caps how many operations we buffer into a single
	// backend batch before committing it and continuing with a fresh batch.
	//
	// This avoids large one-shot allocations (and their GC / page-fault overhead)
	// when flushing very large immutable memtables (e.g. when value-log pointers
	// are forced).
	flushBackendBatchMaxEntries = 32 * 1024

	// flushBackendBatchInitEntries is a small "reserve hint" used for backend batch
	// creation. It intentionally stays below flushBackendBatchMaxEntries to avoid
	// spending large CPU time zeroing an oversized []batch.Entry on batch creation.
	flushBackendBatchInitEntries = 8 * 1024
)

type flushUnit struct {
	mem      memtable.Table
	memBytes int64
	memLen   int
	memRange keyRange
	walPaths []string
	id       uint64
	laneID   int
}

func (db *DB) collectFlushUnitsLocked(laneID int, maxMemtables int, targetBytes int64) ([]flushUnit, []uint64, int64, int) {
	queueLen := len(db.queue)
	if queueLen == 0 {
		return nil, nil, 0, 0
	}
	if maxMemtables <= 0 || maxMemtables > flushCombineMaxMemtables {
		maxMemtables = flushCombineMaxMemtables
	}
	units := make([]flushUnit, 0, maxMemtables)
	ids := make([]uint64, 0, maxMemtables)
	var totalBytes int64
	var totalLen int
	for i := 0; i < queueLen && len(units) < maxMemtables; i++ {
		if laneID >= 0 {
			unitLaneID := 0
			if i < len(db.queueLaneIDs) {
				unitLaneID = int(db.queueLaneIDs[i])
			}
			if unitLaneID != laneID {
				continue
			}
		} else if i >= maxMemtables {
			break
		}
		mem := db.queue[i]
		memBytes := mem.Size()
		memLen := mem.Len()
		if len(units) > 0 && targetBytes > 0 && totalBytes >= targetBytes {
			break
		}
		var walPaths []string
		if i < len(db.queueWALPaths) {
			walPaths = db.queueWALPaths[i]
		}
		var rng keyRange
		if i < len(db.queueRanges) {
			rng = db.queueRanges[i]
		}
		var id uint64
		if i < len(db.queueIDs) {
			id = db.queueIDs[i]
		}
		unitLaneID := 0
		if i < len(db.queueLaneIDs) {
			unitLaneID = int(db.queueLaneIDs[i])
		}
		units = append(units, flushUnit{
			mem:      mem,
			memBytes: memBytes,
			memLen:   memLen,
			memRange: rng,
			walPaths: walPaths,
			id:       id,
			laneID:   unitLaneID,
		})
		ids = append(ids, id)
		totalBytes += memBytes
		totalLen += memLen
	}
	return units, ids, totalBytes, totalLen
}

func (db *DB) removeQueuedUnitsLocked(removeIDs map[uint64]struct{}, units []flushUnit, totalBytes int64) {
	for _, unit := range units {
		if unit.memRange.valid {
			db.backendRange.add(unit.memRange.min)
			db.backendRange.add(unit.memRange.max)
		}
		db.queueRetiredMemtableLocked(unit.mem)
	}

	dstQueue := db.queue[:0]
	dstShardIDs := db.queueShardIDs[:0]
	dstLaneIDs := db.queueLaneIDs[:0]
	dstIDs := db.queueIDs[:0]
	dstEnqueueNS := db.queueEnqueueNS[:0]
	dstRanges := db.queueRanges[:0]
	dstWALPaths := db.queueWALPaths[:0]
	dstValueLogPaths := db.queueValueLogPaths[:0]

	db.ensureQueueLaneIDsLocked()
	for i, mem := range db.queue {
		var id uint64
		if i < len(db.queueIDs) {
			id = db.queueIDs[i]
		}
		if _, ok := removeIDs[id]; ok {
			continue
		}
		dstQueue = append(dstQueue, mem)
		if i < len(db.queueShardIDs) {
			dstShardIDs = append(dstShardIDs, db.queueShardIDs[i])
		}
		if i < len(db.queueLaneIDs) {
			dstLaneIDs = append(dstLaneIDs, db.queueLaneIDs[i])
		} else {
			dstLaneIDs = append(dstLaneIDs, 0)
		}
		if i < len(db.queueIDs) {
			dstIDs = append(dstIDs, db.queueIDs[i])
		}
		if i < len(db.queueEnqueueNS) {
			dstEnqueueNS = append(dstEnqueueNS, db.queueEnqueueNS[i])
		} else {
			dstEnqueueNS = append(dstEnqueueNS, 0)
		}
		if i < len(db.queueRanges) {
			dstRanges = append(dstRanges, db.queueRanges[i])
		}
		if i < len(db.queueWALPaths) {
			dstWALPaths = append(dstWALPaths, db.queueWALPaths[i])
		}
		if i < len(db.queueValueLogPaths) {
			dstValueLogPaths = append(dstValueLogPaths, db.queueValueLogPaths[i])
		}
	}

	db.queue = dstQueue
	db.queueShardIDs = dstShardIDs
	db.queueLaneIDs = dstLaneIDs
	db.queueIDs = dstIDs
	db.queueEnqueueNS = dstEnqueueNS
	db.queueRanges = dstRanges
	db.queueWALPaths = dstWALPaths
	db.queueValueLogPaths = dstValueLogPaths
	db.queueBacklogBytes.Add(-totalBytes)
	if len(db.queue) == 0 {
		db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
	}
	db.publishMemtablesLocked()
}

func (db *DB) flushLaneOnce(sync bool, laneID int) bool {
	db.mu.Lock()
	queueLen := len(db.queue)
	if queueLen == 0 {
		db.mu.Unlock()
		return false
	}
	maxMemtables := 1
	targetBytes := int64(0)
	if db.flushBuildConcurrency > 1 || db.deferredValueLogEnabled() {
		maxMemtables = flushCombineMaxMemtables
		targetBytes = flushCombineTargetBytes
		// When FlushThreshold ~= flushCombineTargetBytes (the common default),
		// combining is effectively disabled and large churny workloads are forced
		// through multiple full apply passes. Allow combining several memtables per
		// flush (bounded) to reduce repeated rewrite work.
		desired := db.flushThreshold * 4
		if desired > flushCombineTargetBytesMax {
			desired = flushCombineTargetBytesMax
		}
		if desired > targetBytes {
			targetBytes = desired
		}
	}
	units, ids, totalBytes, totalLen := db.collectFlushUnitsLocked(laneID, maxMemtables, targetBytes)
	db.mu.Unlock()
	if len(units) == 0 {
		return false
	}

	if totalLen == 0 {
		db.mu.Lock()
		removeIDs := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			removeIDs[id] = struct{}{}
		}
		db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)
		db.mu.Unlock()
		return true
	}

	backendEntriesCap := db.flushBackendEntriesCap(totalLen, sync)

	useParallel := db.flushBuildConcurrency > 1 &&
		totalLen >= db.flushBuildMinEntries &&
		len(units) >= db.flushBuildMinUnits &&
		runtime.GOMAXPROCS(0) > 1

	if useParallel && !db.deferredValueLogEnabled() {
		chunkCap := db.flushBuildChunkCap
		if chunkCap < 0 {
			chunkCap = 8192
		}

		type buildResult struct {
			idx       int
			runs      [][]batch.Entry
			deleteOps int
			err       error
		}

		jobs := make(chan int, len(units))
		results := make(chan buildResult, len(units))
		closeCh := db.closeCh

		for i := range units {
			jobs <- i
		}
		close(jobs)

		workers := db.flushBuildConcurrency
		if workers <= 0 {
			workers = 1
		}
		if db.flushBuildAutoConcurrency && totalLen > 0 {
			// Small inline-heavy entries are typically memory-copy bound; high
			// worker counts can over-parallelize and add scheduler overhead.
			// Keep wider concurrency for larger entries where per-entry work is
			// heavier (pointer/value encoding and compression).
			bytesPerEntry := totalBytes / int64(totalLen)
			switch {
			case bytesPerEntry <= 64:
				if workers > 4 {
					workers = 4
				}
			case bytesPerEntry <= 256:
				if workers > 6 {
					workers = 6
				}
			}
		}
		if workers > len(units) {
			workers = len(units)
		}

		done := make(chan struct{}, workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer func() { done <- struct{}{} }()
				for idx := range jobs {
					select {
					case <-closeCh:
						results <- buildResult{idx: idx, err: errDBClosing}
						continue
					default:
					}
					runs, deleteOps, err := buildOpRuns(units[idx].mem, chunkCap)
					results <- buildResult{idx: idx, runs: runs, deleteOps: deleteOps, err: err}
				}
			}()
		}

		go func() {
			for i := 0; i < workers; i++ {
				<-done
			}
			close(results)
		}()

		unitRuns := getUnitRuns(len(units))
		defer func() {
			for i := range unitRuns {
				for _, run := range unitRuns[i] {
					putEntrySlice(run)
				}
				putEntryRuns(unitRuns[i])
			}
			putUnitRuns(unitRuns)
		}()
		unitDeleteOps := make([]int, len(units))
		failed := false
		for res := range results {
			if res.err != nil {
				if !failed {
					db.reportError(fmt.Errorf("cachingdb: flush build failed: %w", res.err))
				}
				failed = true
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			if failed {
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			unitRuns[res.idx] = res.runs
			unitDeleteOps[res.idx] = res.deleteOps
		}
		if failed {
			return false
		}

		// Adaptive micro-batching: delete-heavy flushes are expensive to apply in
		// many intermediate commits (each commit re-writes leaf pages, copying
		// surviving values). Count deletes and tighten the commit cap in that case.
		deleteOps := 0
		for _, n := range unitDeleteOps {
			deleteOps += n
		}
		backendEntriesCap = db.flushBackendEntriesCapForOps(totalLen, deleteOps, sync)

		reserveChunkOps := totalLen
		if reserveChunkOps > backendEntriesCap {
			reserveChunkOps = backendEntriesCap
		}
		sizeHint := reserveChunkOps
		backendBatch := db.newBackendBatchWithSize(sizeHint)
		reserveBackendBatchOps(backendBatch, reserveChunkOps)
		flushStart := time.Now()
		vlogFlushed := false
		backendPendingOps := 0
		chunkBackend := totalLen > backendEntriesCap
		emittedChunk := false

		type ptrSetterView interface {
			SetPointerView(key []byte, ptr page.ValuePtr) error
		}
		type ptrSetter interface {
			SetPointer(key []byte, ptr page.ValuePtr) error
		}
		psv, _ := backendBatch.(ptrSetterView)
		ps, _ := backendBatch.(ptrSetter)
		var single [1]batch.Entry

		// Best-effort: ensure value-log bytes are flushed before we start committing
		// pointers into the index when we expect to emit multiple backend commits.
		if chunkBackend && db.valueLogEnabled() && !vlogFlushed {
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
				_ = backendBatch.Close()
				return false
			}
			vlogFlushed = true
		}

		flushBackendChunk := func() error {
			if !chunkBackend || backendPendingOps < backendEntriesCap {
				return nil
			}
			emittedChunk = true
			db.backendWriteBatchesTotal.Add(1)
			// If sync==true, we only need a single durability boundary at the end of
			// the flush. Write the intermediate chunks without fsync to avoid
			// repeated pager sync work.
			err := backendBatch.Write()
			cerr := backendBatch.Close()
			if err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
			backendBatch = db.newBackendBatchWithSize(sizeHint)
			reserveBackendBatchOps(backendBatch, reserveChunkOps)
			psv, _ = backendBatch.(ptrSetterView)
			ps, _ = backendBatch.(ptrSetter)
			backendPendingOps = 0
			return nil
		}

		heap := getOpMergeHeap(len(unitRuns))
		defer func() { putOpMergeHeap(heap) }()
		for i := range unitRuns {
			if len(unitRuns[i]) == 0 {
				continue
			}
			it := newOpRunIter(unitRuns[i])
			if it.Valid() {
				priority := len(unitRuns) - 1 - i
				heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
			}
		}
		for i := len(heap)/2 - 1; i >= 0; i-- {
			(&heap).down(i, len(heap))
		}

		shadowedOps := 0
		appliedOps := 0
		for len(heap) > 0 {
			top := heap.pop()
			currentKey := top.key

			for len(heap) > 0 {
				next := heap.peek()
				if next != nil && bytes.Equal(next.key, currentKey) {
					shadowed := heap.pop()
					shadowedOps++
					shadowed.iter.Next()
					if shadowed.iter.Valid() {
						shadowed.key = shadowed.iter.Key()
						heap.push(shadowed)
					}
					continue
				}
				break
			}

			entry := top.iter.Entry()
			var (
				err     error
				applied bool
			)
			switch {
			case entry.Type == batch.OpDelete:
				err = backendBatch.Delete(entry.Key)
				applied = true
			case entry.IsPtr:
				if psv != nil {
					err = psv.SetPointerView(entry.Key, entry.ValuePtr)
				} else if ps != nil {
					err = ps.SetPointer(entry.Key, entry.ValuePtr)
				} else {
					single[0] = batch.Entry{Type: batch.OpPut, Key: entry.Key, ValuePtr: entry.ValuePtr, IsPtr: true}
					err = backendBatch.SetOps(single[:])
				}
				applied = true
			default:
				err = backendBatch.Set(entry.Key, entry.Value)
				applied = true
			}
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
				_ = backendBatch.Close()
				return false
			}
			if applied {
				backendPendingOps++
				appliedOps++
				if err := flushBackendChunk(); err != nil {
					db.reportError(err)
					_ = backendBatch.Close()
					return false
				}
			}

			top.iter.Next()
			if top.iter.Valid() {
				top.key = top.iter.Key()
				heap.push(top)
			}
		}
		if shadowedOps > 0 {
			flushMergeShadowedOpsTotal.Add(uint64(shadowedOps))
			flushMergeParallelShadowedOpsTotal.Add(uint64(shadowedOps))
		}
		if appliedOps > 0 {
			flushMergeAppliedOpsTotal.Add(uint64(appliedOps))
			flushMergeParallelAppliedOpsTotal.Add(uint64(appliedOps))
		}

		if db.valueLogEnabled() {
			if !vlogFlushed {
				if err := db.flushValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
					_ = backendBatch.Close()
					return false
				}
				vlogFlushed = true
			}
			if sync && !db.relaxedSync {
				if err := db.syncValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
					_ = backendBatch.Close()
					return false
				}
			}
		}

		var err error
		if backendPendingOps > 0 {
			db.backendWriteBatchesTotal.Add(1)
			if sync {
				err = backendBatch.WriteSync()
			} else {
				err = backendBatch.Write()
			}
		} else if sync && emittedChunk {
			// If we emitted intermediate chunks and happened to land exactly on a
			// chunk boundary, force a single durability boundary at the end.
			db.backendWriteBatchesTotal.Add(1)
			err = backendBatch.WriteSync()
		}
		cerr := backendBatch.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			db.reportError(err)
			return false
		}

		db.mu.Lock()
		removeIDs := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			removeIDs[id] = struct{}{}
		}
		db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)

		deletable := make([]string, 0, len(units))
		if sync {
			inUse := make(map[string]struct{})
			for _, path := range db.currentWALPaths() {
				inUse[path] = struct{}{}
			}
			for _, paths := range db.queueWALPaths {
				for _, path := range paths {
					inUse[path] = struct{}{}
				}
			}
			seen := make(map[string]struct{})
			for _, unit := range units {
				for _, walPath := range unit.walPaths {
					if walPath == "" {
						continue
					}
					if _, ok := inUse[walPath]; ok {
						continue
					}
					if _, ok := seen[walPath]; ok {
						continue
					}
					if db.valueLogRetained(walPath) {
						continue
					}
					seen[walPath] = struct{}{}
					deletable = append(deletable, walPath)
				}
			}
		}
		db.mu.Unlock()

		removed := false
		for _, walPath := range deletable {
			db.dropValueLogSegment(walPath)
			if err := db.removeFileRetry(walPath); err != nil {
				continue
			}
			removed = true
			db.mu.Lock()
			db.untrackWALSegmentLocked(walPath)
			db.mu.Unlock()
			db.forgetValueLogRetain(walPath)
		}
		if removed {
			db.syncDirBestEffort(db.dir)
		}
		db.checkValueLogRetention()

		flushDur := time.Since(flushStart)
		if flushDur > 0 && totalBytes > 0 {
			sample := float64(totalBytes) / flushDur.Seconds()
			db.bpMu.Lock()
			if db.flushBpsEWMA <= 0 {
				db.flushBpsEWMA = sample
			} else {
				db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
			}
			db.bpCond.Broadcast()
			db.bpMu.Unlock()
		}
		return true
	}

	reserveChunkOps := totalLen
	if reserveChunkOps > backendEntriesCap {
		reserveChunkOps = backendEntriesCap
	}
	sizeHint := reserveChunkOps
	backendBatch := db.newBackendBatchWithSize(sizeHint)
	reserveBackendBatchOps(backendBatch, reserveChunkOps)
	flushStart := time.Now()
	vlogFlushed := false
	backendPendingOps := 0
	// When flushing a large combined batch, commit intermediate backend batches
	// to reduce peak allocator demand (and thus index.db high-watermark growth)
	// under small KeepRecent windows.
	chunkBackend := totalLen > backendEntriesCap

	// backendBatch := db.backend.NewBatch() // Original line, now replaced
	if db.deferredValueLogEnabled() {
		pendingOps, err := db.flushDeferredValueLogUnits(units, backendBatch, sync, laneID)
		if err != nil {
			db.reportError(err)
			_ = backendBatch.Close()
			return false
		}
		backendPendingOps = pendingOps
	} else {
		type (
			ptrSetter interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			}
			ptrSetterView interface {
				SetPointerView(key []byte, ptr page.ValuePtr) error
			}
		)
		psv, _ := backendBatch.(ptrSetterView)
		ps, _ := backendBatch.(ptrSetter)
		var single [1]batch.Entry

		// Best-effort: ensure value-log bytes are flushed before we start committing
		// pointers into the index when we expect to emit multiple backend commits.
		// This preserves the relative ordering while still allowing us to amortize
		// the durability boundary to the final commit when sync==true.
		if chunkBackend && db.valueLogEnabled() && !vlogFlushed {
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
				_ = backendBatch.Close()
				return false
			}
			vlogFlushed = true
		}

		flushBackendChunk := func() error {
			if !chunkBackend || backendPendingOps < backendEntriesCap {
				return nil
			}

			db.backendWriteBatchesTotal.Add(1)
			// If sync==true, we only need a single durability boundary at the end of
			// the flush. Write the intermediate chunks without fsync to avoid
			// repeated pager sync work.
			err := backendBatch.Write()
			cerr := backendBatch.Close()
			if err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
			backendBatch = db.newBackendBatchWithSize(sizeHint)
			reserveBackendBatchOps(backendBatch, reserveChunkOps)
			psv, _ = backendBatch.(ptrSetterView)
			ps, _ = backendBatch.(ptrSetter)
			backendPendingOps = 0
			return nil
		}
		for _, unit := range units {
			iter := unit.mem.NewIterator(nil, nil)
			for iter.Valid() {
				key := iter.UnsafeKey()
				val, ptr, flags := iter.UnsafeEntry()
				if flags&node.FlagTombstone != 0 {
					if err := backendBatch.Delete(key); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (delete): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else if flags&node.FlagPointer != 0 {
					if psv != nil {
						if err := psv.SetPointerView(key, ptr); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					} else if ps != nil {
						if err := ps.SetPointer(key, ptr); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					} else {
						type ptrSetterLegacy interface {
							SetPointer(key []byte, ptr page.ValuePtr) error
						}
						if psl, ok := backendBatch.(ptrSetterLegacy); ok {
							if err := psl.SetPointer(key, ptr); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						} else {
							single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
							if err := backendBatch.SetOps(single[:]); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (setops ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
						}
					}
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else {
					if err := backendBatch.Set(key, val); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (set): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				}
				iter.Next()
			}
			if err := iter.Error(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (iter): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			if err := iter.Close(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (iter close): %w", err))
				_ = backendBatch.Close()
				return false
			}
		}
	}

	if db.valueLogEnabled() {
		if !vlogFlushed {
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
				_ = backendBatch.Close()
				return false
			}
			vlogFlushed = true
		}
		if sync && !db.relaxedSync {
			if err := db.syncValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
				_ = backendBatch.Close()
				return false
			}
		}
	}

	var err error
	if backendPendingOps > 0 {
		db.backendWriteBatchesTotal.Add(1)
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		cerr := backendBatch.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			db.reportError(err)
			return false
		}
	} else {
		if err := backendBatch.Close(); err != nil {
			db.reportError(err)
			return false
		}
	}

	// Remove from queue and delete old WAL segments.
	db.mu.Lock()
	removeIDs := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		removeIDs[id] = struct{}{}
	}
	db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)

	deletable := make([]string, 0, len(units))
	if sync {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, unit := range units {
			for _, walPath := range unit.walPaths {
				if walPath == "" {
					continue
				}
				if _, ok := inUse[walPath]; ok {
					continue
				}
				if _, ok := seen[walPath]; ok {
					continue
				}
				if db.valueLogRetained(walPath) {
					continue
				}
				seen[walPath] = struct{}{}
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()

	removed := false
	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			// Best effort cleanup
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()

	flushDur := time.Since(flushStart)
	if flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

func (db *DB) allowAutoDictSampling(l *lane, writeMode vlogCompressionWriteMode, unitPayloadBytes int) bool {
	if writeMode == vlogWriteOff {
		return false
	}
	bootstrap := db.valueLogDictLastAppliedDictID.Load() == 0
	allow := true
	if !bootstrap && l != nil && l.vlogCompressionSelector != nil {
		allow = l.vlogCompressionSelector.allowDictSampling(writeMode)
	}
	if !allow || bootstrap || writeMode == vlogWriteDict {
		return allow
	}
	if unitPayloadBytes <= 256 {
		return false
	}
	return db.valueLogDictShouldCollectPaused()
}

func (db *DB) finalizeFlushStats(totalLen int, totalBytes int64, flushDur, durPreVlog, durBuild, durSet, durPostVlog, durPostVlogSync, durBackendWrite time.Duration) error {
	return nil
}

func (db *DB) flushOneLocked(sync bool) bool {
	db.mu.Lock()
	if len(db.queue) == 0 {
		db.mu.Unlock()
		return false
	}
	mem := db.queue[0]
	memBytes := mem.Size()
	memLen := mem.Len()
	memRange := keyRange{}
	if len(db.queueRanges) > 0 {
		memRange = db.queueRanges[0]
	}
	var walPaths []string
	if len(db.queueWALPaths) > 0 {
		walPaths = db.queueWALPaths[0]
	}
	laneID := 0
	if len(db.queueLaneIDs) > 0 {
		laneID = int(db.queueLaneIDs[0])
	}
	db.mu.Unlock()

	debugTiming := db.debugFlushTiming
	var (
		durPreVlogFlush time.Duration
		durBuildOps     time.Duration
		durSetOps       time.Duration
		durPostVlog     time.Duration
		durPostVlogSync time.Duration
		durBackendWrite time.Duration
	)

	// Optimization: Skip flush for empty memtables (e.g. from frequent Iterator creation)
	flushStart := time.Time{}
	flushed := false
	if memLen > 0 {
		flushStart = time.Now()

		// Flush 'mem' to backend
		backendEntriesCap := db.flushBackendEntriesCap(memLen, sync)
		reserveChunkOps := memLen
		if reserveChunkOps > backendEntriesCap {
			reserveChunkOps = backendEntriesCap
		}
		sizeHint := reserveChunkOps
		backendBatch := db.newBackendBatchWithSize(sizeHint)
		reserveBackendBatchOps(backendBatch, reserveChunkOps)
		vlogFlushed := false
		backendPendingOps := 0
		iter := mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator

		if db.deferredValueLogEnabled() {
			t0 := time.Now()
			var err error
			backendPendingOps, err = db.flushDeferredValueLogMemtable(iter, backendBatch, memLen, sync, laneID)
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (defer vlog): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			if backendPendingOps <= 0 {
				db.reportError(fmt.Errorf("cachingdb: flush deferred produced no backend ops for memtable batch=%d", memLen))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			durBuildOps = time.Since(t0)
		} else {
			// When flushing very large memtables, avoid building an unbounded backend batch
			// (which can allocate / zero very large buffers and appear "hung").
			chunkBackend := memLen > backendEntriesCap

			// Best-effort: ensure value-log data is durable before committing pointers
			// to the backend. This keeps the relative durability ordering intact when
			// we later commit the backend in chunks.
			if chunkBackend && sync && db.valueLogEnabled() {
				t0 := time.Now()
				if err := db.flushValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
					_ = iter.Close()
					_ = backendBatch.Close()
					return false
				}
				durPostVlog = time.Since(t0)
				vlogFlushed = true
				if sync && !db.relaxedSync {
					t1 := time.Now()
					if err := db.syncValueLog(laneID); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					durPostVlogSync = time.Since(t1)
				}
			}

			t0 := time.Now()
			type (
				setViewer interface {
					SetView(key, value []byte) error
				}
				deleteViewer interface {
					DeleteView(key []byte) error
				}
				ptrSetter interface {
					SetPointer(key []byte, ptr page.ValuePtr) error
				}
				ptrSetterView interface {
					SetPointerView(key []byte, ptr page.ValuePtr) error
				}
			)
			sv, _ := backendBatch.(setViewer)
			dv, _ := backendBatch.(deleteViewer)
			psv, _ := backendBatch.(ptrSetterView)
			ps, _ := backendBatch.(ptrSetter)
			var single [1]batch.Entry

			flushBackendChunk := func() error {
				if !chunkBackend || backendPendingOps < backendEntriesCap {
					return nil
				}
				tw := time.Now()
				// If sync==true, we only need a single durability boundary at the end
				// of the flush. Write intermediate chunks without fsync to avoid
				// repeated pager sync work.
				db.backendWriteBatchesTotal.Add(1)
				err := backendBatch.Write()
				cerr := backendBatch.Close()
				if err == nil {
					err = cerr
				}
				durBackendWrite += time.Since(tw)
				if err != nil {
					return err
				}
				backendBatch = db.newBackendBatchWithSize(sizeHint)
				reserveBackendBatchOps(backendBatch, reserveChunkOps)
				sv, _ = backendBatch.(setViewer)
				dv, _ = backendBatch.(deleteViewer)
				psv, _ = backendBatch.(ptrSetterView)
				ps, _ = backendBatch.(ptrSetter)
				backendPendingOps = 0
				return nil
			}

			for iter.Valid() {
				key := iter.UnsafeKey()
				val, ptr, flags := iter.UnsafeEntry()
				if flags&node.FlagTombstone != 0 {
					var err error
					if dv != nil {
						err = dv.DeleteView(key)
					} else {
						err = backendBatch.Delete(key)
					}
					if err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (delete): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else if flags&node.FlagPointer != 0 {
					if psv != nil {
						if err := psv.SetPointerView(key, ptr); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					} else if ps != nil {
						if err := ps.SetPointer(key, ptr); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					} else {
						single[0] = batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}
						if err := backendBatch.SetOps(single[:]); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (setops ptr): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else {
					var err error
					if sv != nil {
						err = sv.SetView(key, val)
					} else {
						err = backendBatch.Set(key, val)
					}
					if err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (set): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					backendPendingOps++
					if err := flushBackendChunk(); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				}
				iter.Next()
			}
			if err := iter.Error(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (iter): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
			durBuildOps = time.Since(t0)
		}
		if err := iter.Close(); err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed (iter close): %w", err))
			_ = backendBatch.Close()
			return false
		}
		// Commit to backend
		if db.valueLogEnabled() && !vlogFlushed {
			t0 := time.Now()
			if err := db.flushValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
				_ = backendBatch.Close()
				return false
			}
			durPostVlog = time.Since(t0)
			if sync && !db.relaxedSync {
				t1 := time.Now()
				if err := db.syncValueLog(laneID); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
					_ = backendBatch.Close()
					return false
				}
				durPostVlogSync = time.Since(t1)
			}
		}
		if backendPendingOps > 0 {
			tw := time.Now()
			var err error
			if sync {
				err = backendBatch.WriteSync()
			} else {
				err = backendBatch.Write()
			}
			cerr := backendBatch.Close()
			if err == nil {
				err = cerr
			}
			durBackendWrite += time.Since(tw)
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
				return false
			}
			backendBatch = nil
		} else {
			if err := backendBatch.Close(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (close): %w", err))
				return false
			}
		}
		flushed = true
	}
	flushDur := time.Duration(0)
	if flushed {
		flushDur = time.Since(flushStart)
	}
	if debugTiming && flushed {
		fmt.Fprintf(os.Stderr, "treedb: flush_timing combined=0 units=1 entries=%d bytes=%d pre_vlog=%s build=%s setops=%s post_vlog=%s post_vlog_sync=%s backend_write=%s total=%s\n",
			memLen,
			memBytes,
			durPreVlogFlush,
			durBuildOps,
			durSetOps,
			durPostVlog,
			durPostVlogSync,
			durBackendWrite,
			flushDur,
		)
	}

	// Remove from queue and delete old WAL
	db.mu.Lock()
	if memRange.valid {
		db.backendRange.add(memRange.min)
		db.backendRange.add(memRange.max)
	}
	if len(db.queue) > 0 {
		db.queueRetiredMemtableLocked(db.queue[0])
		db.queue = db.queue[1:]
	}
	if len(db.queueShardIDs) > 0 {
		db.queueShardIDs = db.queueShardIDs[1:]
	}
	if len(db.queueLaneIDs) > 0 {
		db.queueLaneIDs = db.queueLaneIDs[1:]
	}
	if len(db.queueIDs) > 0 {
		db.queueIDs = db.queueIDs[1:]
	}
	if len(db.queueEnqueueNS) > 0 {
		db.queueEnqueueNS = db.queueEnqueueNS[1:]
	}
	if len(db.queueRanges) > 0 {
		db.queueRanges = db.queueRanges[1:]
	}
	if len(db.queueWALPaths) > 0 {
		db.queueWALPaths = db.queueWALPaths[1:]
	}
	if len(db.queueValueLogPaths) > 0 {
		db.queueValueLogPaths = db.queueValueLogPaths[1:]
	}
	db.queueBacklogBytes.Add(-memBytes)
	if len(db.queue) == 0 {
		db.materializationLastDrainUnixNano.Store(time.Now().UnixNano())
	}
	db.publishMemtablesLocked()

	deletable := make([]string, 0, len(walPaths))
	if sync {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, walPath := range walPaths {
			if walPath == "" {
				continue
			}
			if _, ok := inUse[walPath]; ok {
				continue
			}
			if _, ok := seen[walPath]; ok {
				continue
			}
			if db.valueLogRetained(walPath) {
				continue
			}
			seen[walPath] = struct{}{}
			deletable = append(deletable, walPath)
		}
	}
	db.mu.Unlock()

	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			// Best effort cleanup
			continue
		}
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()

	if flushed && flushDur > 0 && memBytes > 0 {
		sample := float64(memBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	} else {
		db.bpMu.Lock()
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

// canBypassMemtableRead reports whether point-lookups can skip memtable probes
// and go directly to backend lookups for this key.
//
// Safety notes:
//   - We require an empty immutable queue.
//   - We require global mutable bytes to be zero.
//   - We additionally check the target mutable shard length to avoid races where
//     mutableBytes is transiently zero while an old view still has entries.
func (db *DB) canBypassMemtableRead(view *memtableView, key []byte) bool {
	if view == nil || len(view.queue) != 0 || db.mutableBytes.Load() != 0 {
		return false
	}
	if len(view.mutables) == 0 {
		return true
	}
	idx := db.shardIndex(key)
	if idx >= len(view.mutables) {
		return true
	}
	mt := view.mutables[idx]
	if mt == nil {
		return true
	}
	return mt.Len() == 0
}

// canBypassMemtableReadMany is the multi-key equivalent of
// canBypassMemtableRead. It only bypasses memtables when every touched mutable
// shard is observably empty.
func (db *DB) canBypassMemtableReadMany(view *memtableView, keys [][]byte) bool {
	if len(keys) == 0 {
		return true
	}
	if view == nil || len(view.queue) != 0 || db.mutableBytes.Load() != 0 {
		return false
	}
	n := len(view.mutables)
	if n == 0 {
		return true
	}
	// Fast path: common shard counts are small; use a stack bitset to avoid
	// per-call allocations in read-heavy GetMany paths.
	if n <= 64 {
		var checkedBits uint64
		for _, key := range keys {
			idx := db.shardIndex(key)
			if idx >= n {
				continue
			}
			bit := uint64(1) << uint(idx)
			if checkedBits&bit != 0 {
				continue
			}
			checkedBits |= bit
			mt := view.mutables[idx]
			if mt != nil && mt.Len() != 0 {
				return false
			}
		}
		return true
	}

	checked := make([]bool, n)
	for _, key := range keys {
		idx := db.shardIndex(key)
		if idx >= n || checked[idx] {
			continue
		}
		checked[idx] = true
		mt := view.mutables[idx]
		if mt != nil && mt.Len() != 0 {
			return false
		}
	}
	return true
}

func (db *DB) getMemtable(key []byte) ([]byte, bool, error) {
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}
	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
	} else {
		// Defensive fallback: should not happen after Open(), but keep safe
		// behavior for zero-value DBs and tests.
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queueShardIDs = append([]uint16(nil), db.queueShardIDs...)
		db.mu.RUnlock()
	}

	if db.canBypassMemtableRead(view, key) {
		return nil, false, nil
	}

	// check mutable
	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			val, ptr, flags, found := mutables[idx].GetEntry(key)
			if found {
				if flags&node.FlagTombstone != 0 {
					return nil, true, nil
				}
				if flags&node.FlagPointer != 0 {
					if val == nil {
						readVal, err := db.readValueLog(key, ptr)
						if err != nil {
							return nil, true, err
						}
						return readVal, true, nil
					}
					return val, true, nil
				}
				if val == nil {
					return []byte{}, true, nil
				}
				return val, true, nil
			}
		}
	}

	// check queue backwards (newest first)
	shardIdx := 0
	if len(mutables) > 0 {
		shardIdx = db.shardIndex(key)
	}
	for i := len(queue) - 1; i >= 0; i-- {
		if len(queueShardIDs) > i && int(queueShardIDs[i]) != shardIdx {
			continue
		}
		val, ptr, flags, found := queue[i].GetEntry(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				return nil, true, nil
			}
			if flags&node.FlagPointer != 0 {
				if val == nil {
					readVal, err := db.readValueLog(key, ptr)
					if err != nil {
						return nil, true, err
					}
					return readVal, true, nil
				}
				return val, true, nil
			}
			if val == nil {
				return []byte{}, true, nil
			}
			return val, true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) getMemtableAppend(key, dst []byte) ([]byte, bool, error) {
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}
	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
	} else {
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queueShardIDs = append([]uint16(nil), db.queueShardIDs...)
		db.mu.RUnlock()
	}

	if db.canBypassMemtableRead(view, key) {
		return dst, false, nil
	}

	// check mutable
	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			val, ptr, flags, found := mutables[idx].GetEntry(key)
			if found {
				if flags&node.FlagTombstone != 0 {
					return dst, true, tree.ErrKeyNotFound
				}
				if flags&node.FlagPointer != 0 {
					if val == nil {
						out, err := db.readValueLogAppend(key, ptr, dst)
						if err != nil {
							return dst, true, err
						}
						return out, true, nil
					}
					return append(dst, val...), true, nil
				}
				if val == nil {
					return dst, true, nil
				}
				return append(dst, val...), true, nil
			}
		}
	}

	// check queue backwards (newest first)
	shardIdx := 0
	if len(mutables) > 0 {
		shardIdx = db.shardIndex(key)
	}
	for i := len(queue) - 1; i >= 0; i-- {
		if len(queueShardIDs) > i && int(queueShardIDs[i]) != shardIdx {
			continue
		}
		val, ptr, flags, found := queue[i].GetEntry(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				return dst, true, tree.ErrKeyNotFound
			}
			if flags&node.FlagPointer != 0 {
				if val == nil {
					out, err := db.readValueLogAppend(key, ptr, dst)
					if err != nil {
						return dst, true, err
					}
					return out, true, nil
				}
				return append(dst, val...), true, nil
			}
			if val == nil {
				return dst, true, nil
			}
			return append(dst, val...), true, nil
		}
	}
	return dst, false, nil
}

type backendManyGetter interface {
	GetMany(keys [][]byte) ([][]byte, error)
}

type backendManyPlanner interface {
	GetManyParallelPlan(keyCount int) (workers int, parallel bool)
}

// GetManyParallelPlan reports a safe upper-bound scheduling plan for GetMany.
//
// When cache hit-rates are high, backend calls may involve fewer keys than the
// provided key count. This method intentionally reports based on the input
// upper-bound to stay conservative for concurrency budgeting at callers.
func (db *DB) GetManyParallelPlan(keyCount int) (workers int, parallel bool) {
	if keyCount <= 0 {
		return 1, false
	}
	if planner, ok := db.backend.(backendManyPlanner); ok {
		return planner.GetManyParallelPlan(keyCount)
	}
	workers = runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > keyCount {
		workers = keyCount
	}
	return workers, workers > 1
}

func (db *DB) backendGetMany(keys [][]byte) ([][]byte, error) {
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}
	if mg, ok := db.backend.(backendManyGetter); ok {
		return mg.GetMany(keys)
	}
	out := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.backend.Get(key)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

// GetUnsafe returns a safe copy of the value.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// Get returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.noteRead()
	view := db.retainMemtableView()
	bypass := db.canBypassMemtableRead(view, key)
	if view != nil {
		db.releaseMemtableView(view)
	}
	if bypass {
		if err := db.flushDeferredValueLogForBackendRead(); err != nil {
			return nil, err
		}
		return db.backend.Get(key)
	}
	val, found, err := db.getMemtable(key)
	if err != nil {
		return nil, err
	}
	if found {
		if val == nil {
			return nil, nil
		}
		cpy := make([]byte, len(val))
		copy(cpy, val)
		return cpy, nil
	}
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}
	return db.backend.Get(key)
}

// GetMany returns safe copies of values for keys.
//
// Missing keys are returned as nil entries with no error.
func (db *DB) GetMany(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return make([][]byte, 0), nil
	}
	db.noteRead()

	// Fast path: no mutable/queued state and all touched mutable shards are
	// observably empty, so we can delegate to backend single-snapshot GetMany.
	view := db.retainMemtableView()
	bypass := db.canBypassMemtableReadMany(view, keys)
	if view != nil {
		db.releaseMemtableView(view)
	}
	if bypass {
		return db.backendGetMany(keys)
	}

	out := make([][]byte, len(keys))
	backendIdx := make([]int, 0, len(keys))
	backendKeys := make([][]byte, 0, len(keys))

	// Copy all found values into a shared arena-style buffer to avoid one
	// allocation per key. Each returned slice is capacity-capped to preserve
	// safe-copy semantics.
	//
	// The cache layer may need to resolve value-log pointers for memtable hits;
	// by using the append path, those decodes can write directly into this arena
	// instead of allocating per key. The limit below bounds only the initial
	// arena capacity; subsequent appends may still grow the backing array, so
	// multiple underlying allocations may be retained.
	const (
		getManyValueGuessBytes         = 128
		getManyMaxArenaInitialCapBytes = 1 << 20
	)
	arenaCap := len(keys) * getManyValueGuessBytes
	if arenaCap < 0 {
		arenaCap = 0
	}
	if arenaCap > getManyMaxArenaInitialCapBytes {
		arenaCap = getManyMaxArenaInitialCapBytes
	}
	arena := make([]byte, 0, arenaCap)
	emptyValue := []byte{}
	for i, key := range keys {
		start := len(arena)
		nextArena, found, err := db.getMemtableAppend(key, arena)
		if err != nil {
			if err == tree.ErrKeyNotFound {
				// Tombstone in cache layers: treat as a missing key and do not fall
				// through to backend.
				if found {
					continue
				}
			} else {
				// Defensive: if getMemtableAppend ever returns errors with found=false,
				// do not silently route those keys to backend reads.
				return nil, err
			}
		}
		if found {
			arena = nextArena
			if len(arena) == start {
				out[i] = emptyValue
				continue
			}
			out[i] = arena[start:len(arena):len(arena)]
			continue
		}
		backendIdx = append(backendIdx, i)
		backendKeys = append(backendKeys, key)
	}
	if len(backendKeys) == 0 {
		return out, nil
	}

	backendVals, err := db.backendGetMany(backendKeys)
	if err != nil {
		return nil, err
	}
	if len(backendVals) != len(backendKeys) {
		return nil, fmt.Errorf("cachingdb: backend GetMany returned %d values for %d keys", len(backendVals), len(backendKeys))
	}
	for i, outIdx := range backendIdx {
		out[outIdx] = backendVals[i]
	}
	return out, nil
}

// GetAppend appends the value for the key to dst and returns the new slice.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	// 1. Memtable
	out, found, err := db.getMemtableAppend(key, dst)
	if err != nil {
		return dst, err
	}
	if found {
		return out, nil
	}

	// 2. Backend
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return dst, err
	}
	return db.backend.GetAppend(key, dst)
}

func (db *DB) Has(key []byte) (bool, error) {
	db.noteRead()
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}
	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
	} else {
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queueShardIDs = append([]uint16(nil), db.queueShardIDs...)
		db.mu.RUnlock()
	}

	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			_, deleted, found := mutables[idx].Get(key)
			if found {
				return !deleted, nil
			}
		}
	}

	idx := 0
	if len(mutables) > 0 {
		idx = db.shardIndex(key)
	}
	for i := len(queue) - 1; i >= 0; i-- {
		if len(queueShardIDs) > i && int(queueShardIDs[i]) != idx {
			continue
		}
		_, deleted, found := queue[i].Get(key)
		if found {
			return !deleted, nil
		}
	}

	return db.backend.Has(key)
}

func (db *DB) Stats() map[string]string {
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	db.mu.RLock()
	queueLen := len(db.queue)
	flushThreshold := db.flushThreshold
	mutableThresholdBase := db.mutableThreshold.Load()
	memtableMode := db.currentMemtableMode()
	memtableAdaptive := db.memtableAdaptive
	memtableWarmupActive := db.memtableWarmupActive
	maxQueued := db.maxQueuedMemtables
	vlogAutotuneMode := db.valueLogAutotuneOptions.Mode
	oldestQueueEnqueueNS := int64(0)
	for i := range db.queueEnqueueNS {
		ts := db.queueEnqueueNS[i]
		if ts <= 0 {
			continue
		}
		if oldestQueueEnqueueNS == 0 || ts < oldestQueueEnqueueNS {
			oldestQueueEnqueueNS = ts
		}
	}
	db.mu.RUnlock()
	var walCurrentBytes int64
	var walClosedBytes int64
	var queueLagBuckets [vlogQueueLagBucketCount]uint64
	var queueLagCount uint64
	var queueLagTotalNs uint64
	var queueLagMaxNs uint64
	var queueDepthEnqueued uint64
	var queueDepthSamples uint64
	var queueDepthSum uint64
	var queueDepthMax uint64
	var queueDepthLast uint64
	var queueDepthPositiveRunMaxNs uint64
	var rawWritevSyscalls uint64
	var rawWritevBytes uint64
	var rawWritevIovecs uint64
	var rawWritevFlushes uint64
	var rawWriteSyscalls uint64
	var rawWriteBytes uint64
	var rawWriteCalls uint64
	var vlogShapeSegmentsTotal int64
	var vlogShapeBytesTotal int64
	var vlogShapeL0Segments int64
	var vlogShapeL0Bytes int64
	splitValueLog := db.splitValueLogEnabled()
	valueLogOn := db.valueLogEnabled()
	for i := range db.lanes {
		l := &db.lanes[i]
		walCurrentBytes += l.walLiveBytes.Load()
		walClosedBytes += l.walClosedBytes.Load()

		lagSnap := snapshotLaneVlogQueueLag(l)
		depthSnap := snapshotLaneVlogQueueDepth(l)
		queueLagCount += lagSnap.Count
		queueLagTotalNs += lagSnap.TotalNs
		if lagSnap.MaxNs > queueLagMaxNs {
			queueLagMaxNs = lagSnap.MaxNs
		}
		for bucket := 0; bucket < vlogQueueLagBucketCount; bucket++ {
			queueLagBuckets[bucket] += lagSnap.Buckets[bucket]
		}
		queueDepthEnqueued += depthSnap.Enqueued
		queueDepthSamples += depthSnap.Samples
		queueDepthSum += depthSnap.Sum
		if depthSnap.Max > queueDepthMax {
			queueDepthMax = depthSnap.Max
		}
		queueDepthLast += depthSnap.Last
		if depthSnap.PositiveRunMaxNs > queueDepthPositiveRunMaxNs {
			queueDepthPositiveRunMaxNs = depthSnap.PositiveRunMaxNs
		}
		var (
			vlogWriter  valueWriter
			vlogPath    string
			closedSegs  int
			rotTotal    uint64
			rotIdle     uint64
			hasCurrent  bool
			laneID      = l.id
			liveBytes   = l.vlogLiveBytes.Load()
			closedBytes = l.vlogClosedBytes.Load()
		)
		l.vlogMu.Lock()
		vlogWriter = l.vlog
		vlogPath = l.vlogPath
		if l.vlogClosedSizes != nil {
			closedSegs = len(l.vlogClosedSizes)
		}
		rotTotal = l.vlogRotateTotal.Load()
		rotIdle = l.vlogRotateIdleTotal.Load()
		l.vlogMu.Unlock()
		hasCurrent = vlogPath != ""

		if splitValueLog && valueLogOn {
			segs := closedSegs
			if hasCurrent {
				segs++
			}
			bytes := closedBytes + liveBytes
			if bytes < 0 {
				bytes = 0
			}
			vlogShapeSegmentsTotal += int64(segs)
			vlogShapeBytesTotal += bytes
			if laneID == 0 {
				vlogShapeL0Segments = int64(segs)
				vlogShapeL0Bytes = bytes
			}
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.segments_total", laneID)] = fmt.Sprintf("%d", segs)
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.segments_closed", laneID)] = fmt.Sprintf("%d", closedSegs)
			if hasCurrent {
				stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.segment_current", laneID)] = "1"
			} else {
				stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.segment_current", laneID)] = "0"
			}
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.bytes_total", laneID)] = fmt.Sprintf("%d", bytes)
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.bytes_closed", laneID)] = fmt.Sprintf("%d", closedBytes)
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.bytes_live", laneID)] = fmt.Sprintf("%d", liveBytes)
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.rotations_total", laneID)] = fmt.Sprintf("%d", rotTotal)
			stats[fmt.Sprintf("treedb.cache.vlog_shape.lane.%d.rotations_idle_total", laneID)] = fmt.Sprintf("%d", rotIdle)
		}
		if snapper, ok := any(vlogWriter).(interface {
			RawWritevStats() valuelog.RawWritevStats
		}); ok {
			snap := snapper.RawWritevStats()
			rawWritevSyscalls += snap.Syscalls
			rawWritevBytes += snap.Bytes
			rawWritevIovecs += snap.Iovecs
			rawWritevFlushes += snap.Flushes
		}
		if snapper, ok := any(vlogWriter).(interface {
			RawWriteStats() valuelog.RawWriteStats
		}); ok {
			snap := snapper.RawWriteStats()
			rawWriteSyscalls += snap.Syscalls
			rawWriteBytes += snap.Bytes
			rawWriteCalls += snap.Calls
		}

		laneLagP99 := estimateVlogQueueLagPercentile(lagSnap.Buckets, lagSnap.Count, 0.99)
		laneLagP999 := estimateVlogQueueLagPercentile(lagSnap.Buckets, lagSnap.Count, 0.999)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.enqueued", i)] = fmt.Sprintf("%d", depthSnap.Enqueued)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_samples", i)] = fmt.Sprintf("%d", depthSnap.Samples)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_last", i)] = fmt.Sprintf("%d", depthSnap.Last)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_max", i)] = fmt.Sprintf("%d", depthSnap.Max)
		if depthSnap.Samples > 0 {
			stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.depth_avg", i)] = fmt.Sprintf("%.3f", float64(depthSnap.Sum)/float64(depthSnap.Samples))
		}
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.positive_drift_run_max_ms", i)] = fmt.Sprintf("%.3f", float64(depthSnap.PositiveRunMaxNs)/float64(time.Millisecond))
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_samples", i)] = fmt.Sprintf("%d", lagSnap.Count)
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_max_ms", i)] = fmt.Sprintf("%.3f", float64(lagSnap.MaxNs)/float64(time.Millisecond))
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_p99_ms", i)] = fmt.Sprintf("%.3f", float64(laneLagP99)/float64(time.Millisecond))
		stats[fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.lag_p999_ms", i)] = fmt.Sprintf("%.3f", float64(laneLagP999)/float64(time.Millisecond))
	}
	if splitValueLog && valueLogOn {
		stats["treedb.cache.vlog_shape.segments_total"] = fmt.Sprintf("%d", vlogShapeSegmentsTotal)
		stats["treedb.cache.vlog_shape.bytes_total"] = fmt.Sprintf("%d", vlogShapeBytesTotal)
		stats["treedb.cache.vlog_shape.l0.segments_total"] = fmt.Sprintf("%d", vlogShapeL0Segments)
		stats["treedb.cache.vlog_shape.l0.bytes_total"] = fmt.Sprintf("%d", vlogShapeL0Bytes)
	} else {
		stats["treedb.cache.vlog_shape.segments_total"] = "0"
		stats["treedb.cache.vlog_shape.bytes_total"] = "0"
		stats["treedb.cache.vlog_shape.l0.segments_total"] = "0"
		stats["treedb.cache.vlog_shape.l0.bytes_total"] = "0"
	}

	stats["treedb.cache.queue_len"] = fmt.Sprintf("%d", queueLen)
	stats["treedb.cache.mutable_bytes"] = fmt.Sprintf("%d", db.mutableBytes.Load())
	stats["treedb.cache.flush_threshold_bytes"] = fmt.Sprintf("%d", flushThreshold)
	stats["treedb.cache.mutable_flush_threshold_base_bytes"] = fmt.Sprintf("%d", mutableThresholdBase)
	stats["treedb.cache.mutable_flush_threshold_effective_bytes"] = fmt.Sprintf("%d", db.mutableFlushThreshold())
	stats["treedb.cache.checkpoint.runs"] = fmt.Sprintf("%d", db.checkpointRuns.Load())
	stats["treedb.cache.checkpoint.total_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointTotalNs.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.max_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointMaxNs.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.flushmu_wait_total_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointFlushMuWaitNs.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.flushmu_wait_max_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointFlushMuWaitMaxNs.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.noop_skips"] = fmt.Sprintf("%d", db.checkpointNoopSkips.Load())
	stats["treedb.cache.checkpoint.auto_vacuum_runs"] = fmt.Sprintf("%d", db.checkpointAutoVacuumRuns.Load())
	stats["treedb.cache.checkpoint.auto_vacuum_last_pages"] = fmt.Sprintf("%d", db.checkpointAutoVacuumLastPages.Load())
	stats["treedb.cache.checkpoint.auto_vacuum_last_internal_fill_p50_ppm"] = fmt.Sprintf("%d", db.checkpointAutoVacuumLastInternalP50.Load())
	stats["treedb.cache.checkpoint.auto_vacuum_last_internal_fill_avg_ppm"] = fmt.Sprintf("%d", db.checkpointAutoVacuumLastInternalAvg.Load())
	stats["treedb.cache.memtable_mode"] = memtableMode.String()
	if memtableAdaptive {
		stats["treedb.cache.memtable_mode_config"] = "adaptive"
	} else {
		stats["treedb.cache.memtable_mode_config"] = "fixed"
	}
	memWrites := db.memtableStats.writes.Load()
	memSeqWrites := db.memtableStats.seqWrites.Load()
	memOverwriteWrites := db.memtableStats.overwriteWrites.Load()
	memIters := db.memtableStats.iterators.Load()
	memRangeIters := db.memtableStats.rangeIters.Load()
	memAdaptiveDecisionTotal := db.memtableAdaptiveDecisionTotal.Load()
	memAdaptiveDecisionReason := db.memtableAdaptiveDecisionReason.Load()
	memAdaptiveDecisionMode := db.memtableAdaptiveDecisionMode.Load()
	memAdaptiveDecisionWrites := db.memtableAdaptiveDecisionWrites.Load()
	memAdaptiveDecisionSeqWrites := db.memtableAdaptiveDecisionSeqWrites.Load()
	memAdaptiveDecisionOverwriteWrites := db.memtableAdaptiveDecisionOverwriteWrites.Load()
	memAdaptiveDecisionIters := db.memtableAdaptiveDecisionIters.Load()
	memAdaptiveDecisionRangeIters := db.memtableAdaptiveDecisionRangeIters.Load()
	memAdaptiveDecisionRangePctPPM := db.memtableAdaptiveDecisionRangePctPPM.Load()
	memViewRetainTotal := db.memtableViewTelemetry.retainTotal.Load()
	memViewReleaseTotal := db.memtableViewTelemetry.releaseTotal.Load()
	memViewLeasesInFlight := db.memtableViewTelemetry.leasesInFlight.Load()
	memViewLeasesInFlightMax := db.memtableViewTelemetry.leasesInFlightMax.Load()
	memViewDeferredViewsCurrent := db.memtableViewTelemetry.deferredViewsCurrent.Load()
	memViewDeferredViewsMax := db.memtableViewTelemetry.deferredViewsMax.Load()
	memViewDeferredViewsTotal := db.memtableViewTelemetry.deferredViewsTotal.Load()
	memViewDeferredMemtablesCurrent := db.memtableViewTelemetry.deferredMemtablesCurrent.Load()
	memViewDeferredMemtablesMax := db.memtableViewTelemetry.deferredMemtablesMax.Load()
	memViewDeferredMemtablesTotal := db.memtableViewTelemetry.deferredMemtablesTotal.Load()
	memViewDeferredBytesCurrent := db.memtableViewTelemetry.deferredBytesCurrent.Load()
	memViewDeferredBytesMax := db.memtableViewTelemetry.deferredBytesMax.Load()
	memViewDeferredBytesTotal := db.memtableViewTelemetry.deferredBytesTotal.Load()
	memViewOldestDeferredUnixNano := db.memtableViewTelemetry.oldestDeferredUnixNano.Load()
	memViewOldestDeferredAgeMS := 0.0
	if memViewOldestDeferredUnixNano > 0 {
		if ageNS := time.Now().UnixNano() - memViewOldestDeferredUnixNano; ageNS > 0 {
			memViewOldestDeferredAgeMS = float64(ageNS) / float64(time.Millisecond)
		}
	}
	stats["treedb.cache.memtable_stats.writes"] = fmt.Sprintf("%d", memWrites)
	stats["treedb.cache.memtable_stats.seq_writes"] = fmt.Sprintf("%d", memSeqWrites)
	stats["treedb.cache.memtable_stats.overwrite_writes"] = fmt.Sprintf("%d", memOverwriteWrites)
	if memWrites > 0 {
		stats["treedb.cache.memtable_stats.seq_write_pct"] = fmt.Sprintf("%.4f", float64(memSeqWrites)/float64(memWrites))
		stats["treedb.cache.memtable_stats.overwrite_write_pct"] = fmt.Sprintf("%.4f", float64(memOverwriteWrites)/float64(memWrites))
	}
	stats["treedb.cache.memtable_stats.iterators"] = fmt.Sprintf("%d", memIters)
	stats["treedb.cache.memtable_stats.range_iterators"] = fmt.Sprintf("%d", memRangeIters)
	if memIters > 0 {
		stats["treedb.cache.memtable_stats.range_iter_pct"] = fmt.Sprintf("%.4f", float64(memRangeIters)/float64(memIters))
	}
	stats["treedb.cache.memtable_adaptive.btree_min_iterator_samples_effective"] = fmt.Sprintf("%d", db.adaptiveBTreeMinIteratorSamples())
	stats["treedb.cache.memtable_adaptive.decision_total"] = fmt.Sprintf("%d", memAdaptiveDecisionTotal)
	stats["treedb.cache.memtable_adaptive.last_reason"] = adaptiveDecisionReasonString(memAdaptiveDecisionReason)
	stats["treedb.cache.memtable_adaptive.last_mode"] = memtable.Mode(memAdaptiveDecisionMode).String()
	stats["treedb.cache.memtable_adaptive.last_writes"] = fmt.Sprintf("%d", memAdaptiveDecisionWrites)
	stats["treedb.cache.memtable_adaptive.last_seq_writes"] = fmt.Sprintf("%d", memAdaptiveDecisionSeqWrites)
	stats["treedb.cache.memtable_adaptive.last_overwrite_writes"] = fmt.Sprintf("%d", memAdaptiveDecisionOverwriteWrites)
	stats["treedb.cache.memtable_adaptive.last_iterators"] = fmt.Sprintf("%d", memAdaptiveDecisionIters)
	stats["treedb.cache.memtable_adaptive.last_range_iterators"] = fmt.Sprintf("%d", memAdaptiveDecisionRangeIters)
	stats["treedb.cache.memtable_adaptive.last_range_iter_pct"] = fmt.Sprintf("%.4f", float64(memAdaptiveDecisionRangePctPPM)/1_000_000.0)
	stats["treedb.cache.memtable_adaptive.reason_low_data_total"] = fmt.Sprintf("%d", db.memtableAdaptiveDecisionLowDataTotal.Load())
	stats["treedb.cache.memtable_adaptive.reason_btree_range_total"] = fmt.Sprintf("%d", db.memtableAdaptiveDecisionBTreeTotal.Load())
	stats["treedb.cache.memtable_adaptive.reason_btree_blocked_min_iters_total"] = fmt.Sprintf("%d", db.memtableAdaptiveDecisionBTreeBlockedMinItersTotal.Load())
	stats["treedb.cache.memtable_adaptive.reason_append_sequential_total"] = fmt.Sprintf("%d", db.memtableAdaptiveDecisionAppendTotal.Load())
	stats["treedb.cache.memtable_adaptive.reason_hash_mixed_total"] = fmt.Sprintf("%d", db.memtableAdaptiveDecisionHashTotal.Load())
	stats["treedb.cache.memtable_view.retain_total"] = fmt.Sprintf("%d", memViewRetainTotal)
	stats["treedb.cache.memtable_view.release_total"] = fmt.Sprintf("%d", memViewReleaseTotal)
	stats["treedb.cache.memtable_view.leases_inflight"] = fmt.Sprintf("%d", memViewLeasesInFlight)
	stats["treedb.cache.memtable_view.leases_inflight_max"] = fmt.Sprintf("%d", memViewLeasesInFlightMax)
	stats["treedb.cache.memtable_view.deferred_views_current"] = fmt.Sprintf("%d", memViewDeferredViewsCurrent)
	stats["treedb.cache.memtable_view.deferred_views_max"] = fmt.Sprintf("%d", memViewDeferredViewsMax)
	stats["treedb.cache.memtable_view.deferred_views_total"] = fmt.Sprintf("%d", memViewDeferredViewsTotal)
	stats["treedb.cache.memtable_view.deferred_memtables_current"] = fmt.Sprintf("%d", memViewDeferredMemtablesCurrent)
	stats["treedb.cache.memtable_view.deferred_memtables_max"] = fmt.Sprintf("%d", memViewDeferredMemtablesMax)
	stats["treedb.cache.memtable_view.deferred_memtables_total"] = fmt.Sprintf("%d", memViewDeferredMemtablesTotal)
	stats["treedb.cache.memtable_view.deferred_bytes_current"] = fmt.Sprintf("%d", memViewDeferredBytesCurrent)
	stats["treedb.cache.memtable_view.deferred_bytes_max"] = fmt.Sprintf("%d", memViewDeferredBytesMax)
	stats["treedb.cache.memtable_view.deferred_bytes_total"] = fmt.Sprintf("%d", memViewDeferredBytesTotal)
	stats["treedb.cache.memtable_view.deferred_oldest_age_ms"] = fmt.Sprintf("%.3f", memViewOldestDeferredAgeMS)
	stats["treedb.cache.memtable_warmup_active"] = fmt.Sprintf("%t", memtableWarmupActive)
	stats["treedb.cache.max_queued_memtables"] = fmt.Sprintf("%d", maxQueued)
	poolPressure := currentPoolPressureSnapshot()
	arenaBaseBudget := currentBatchArenaPoolBudgetBytes()
	arenaEffectiveBudget := scalePoolBudgetForPressure(arenaBaseBudget, poolPressure.level)
	entrySliceBaseBudget := entrySlicePoolBudgetBytes
	entrySliceEffectiveBudget := scalePoolBudgetForPressure(entrySliceBaseBudget, poolPressure.level)
	arenaPoolBytes := batchArenaPoolBytes.Load()
	arenaPoolBytesMax := batchArenaPoolBytesMaxGlobal.Load()
	arenaInFlightBytes := batchArenaInFlightBytes.Load()
	arenaInFlightBytesMax := batchArenaInFlightBytesMaxGlobal.Load()
	arenaLeasedBytes := db.batchArenaLeaseBytes.Load()
	arenaGlobalLeasedBytes := batchArenaLeasedBytesGlobal.Load()
	arenaGlobalLeasedBytesMax := batchArenaLeasedBytesMaxGlobal.Load()
	arenaRetainedHardCapBytes := currentBatchArenaRetainedHardCapBytes()
	arenaRetainedHardCapEffectiveBytes := db.currentBatchArenaRetainedHardCapEffectiveBytes()
	arenaRetainedEstimate := currentBatchArenaRetainedBytesEstimate()
	arenaRetainedMaxEstimate := batchArenaRetainedBytesMaxGlobal.Load()
	arenaAllocRequestedBytes := db.batchArenaAllocRequestedBytes.Load()
	arenaAllocClassBytes := db.batchArenaAllocClassBytes.Load()
	arenaUsedBytes := db.batchArenaUsedBytes.Load()
	arenaTailWasteBytes := db.batchArenaTailWasteBytes.Load()
	arenaTailCompactRuns := db.batchArenaTailCompactRuns.Load()
	arenaTailCompactCopied := db.batchArenaTailCompactCopied.Load()
	arenaTailCompactSaved := db.batchArenaTailCompactSaved.Load()
	arenaPoolBudget := fmt.Sprintf("%d", arenaBaseBudget)
	arenaPoolBudgetEffective := fmt.Sprintf("%d", arenaEffectiveBudget)
	arenaPoolEstimate := fmt.Sprintf("%d", arenaPoolBytes)
	arenaLeased := fmt.Sprintf("%d", arenaLeasedBytes)
	arenaLeasedMax := fmt.Sprintf("%d", db.batchArenaLeaseBytesMax.Load())
	stats["treedb.cache.batch_arena.pool_budget_bytes"] = arenaPoolBudget
	stats["treedb.cache.batch_arena.pool_budget_effective_bytes"] = arenaPoolBudgetEffective
	stats["treedb.cache.batch_arena.pool_bytes_estimate"] = arenaPoolEstimate
	stats["treedb.cache.batch_arena.pool_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaPoolBytesMax)
	stats["treedb.cache.batch_arena.in_flight_bytes_estimate"] = fmt.Sprintf("%d", arenaInFlightBytes)
	stats["treedb.cache.batch_arena.in_flight_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaInFlightBytesMax)
	stats["treedb.cache.batch_arena.leased_bytes"] = arenaLeased
	stats["treedb.cache.batch_arena.leased_bytes_global_estimate"] = fmt.Sprintf("%d", arenaGlobalLeasedBytes)
	stats["treedb.cache.batch_arena.leased_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaGlobalLeasedBytesMax)
	stats["treedb.cache.batch_arena.leased_bytes_max"] = arenaLeasedMax
	stats["treedb.cache.batch_arena.retained_hard_cap_bytes"] = fmt.Sprintf("%d", arenaRetainedHardCapBytes)
	stats["treedb.cache.batch_arena.retained_hard_cap_effective_bytes"] = fmt.Sprintf("%d", arenaRetainedHardCapEffectiveBytes)
	stats["treedb.cache.batch_arena.deferred_pressure_active"] = fmt.Sprintf("%t", db.batchArenaDeferredPressureActive())
	stats["treedb.cache.batch_arena.retained_bytes_global_estimate"] = fmt.Sprintf("%d", arenaRetainedEstimate)
	stats["treedb.cache.batch_arena.retained_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaRetainedMaxEstimate)
	stats["treedb.cache.batch_arena.pool_plus_db_leases_bytes_estimate"] = fmt.Sprintf("%d", arenaPoolBytes+arenaLeasedBytes)
	stats["treedb.cache.batch_arena.alloc_requested_bytes_total"] = fmt.Sprintf("%d", arenaAllocRequestedBytes)
	stats["treedb.cache.batch_arena.alloc_class_bytes_total"] = fmt.Sprintf("%d", arenaAllocClassBytes)
	stats["treedb.cache.batch_arena.used_bytes_total"] = fmt.Sprintf("%d", arenaUsedBytes)
	stats["treedb.cache.batch_arena.tail_waste_bytes_total"] = fmt.Sprintf("%d", arenaTailWasteBytes)
	stats["treedb.cache.batch_arena.tail_compact_runs_total"] = fmt.Sprintf("%d", arenaTailCompactRuns)
	stats["treedb.cache.batch_arena.tail_compact_copied_bytes_total"] = fmt.Sprintf("%d", arenaTailCompactCopied)
	stats["treedb.cache.batch_arena.tail_compact_saved_bytes_total"] = fmt.Sprintf("%d", arenaTailCompactSaved)
	stats["treedb.cache.batch_arena.pool_skip_zero_budget_total"] = fmt.Sprintf("%d", batchArenaPoolSkipZeroBudgetTotal.Load())
	stats["treedb.cache.batch_arena.pool_drop_bytes_total"] = fmt.Sprintf("%d", batchArenaPoolDropBytesTotal.Load())
	stats["treedb.cache.batch_arena.pool_drop_hard_cap_bytes_total"] = fmt.Sprintf("%d", batchArenaPoolDropHardCapBytesTotal.Load())
	stats["treedb.cache.batch_arena.borrow_blocked_total"] = fmt.Sprintf("%d", batchArenaBorrowBlockedTotal.Load())
	stats["treedb.cache.batch_arena.borrow_preflight_blocked_total"] = fmt.Sprintf("%d", batchArenaBorrowPreflightBlockedTotal.Load())
	stats["treedb.cache.batch_arena.borrow_preflight_blocked_bytes_total"] = fmt.Sprintf("%d", batchArenaBorrowPreflightBlockedBytesTotal.Load())
	stats["treedb.cache.batch_arena.steal_suppressed_deferred_total"] = fmt.Sprintf("%d", batchArenaStealSuppressedDeferredTotal.Load())
	stats["treedb.cache.batch_arena.steal_suppressed_deferred_entries_total"] = fmt.Sprintf("%d", batchArenaStealSuppressedDeferredEntriesTotal.Load())
	stats["treedb.process.batch_arena.pool_budget_bytes"] = arenaPoolBudget
	stats["treedb.process.batch_arena.pool_budget_effective_bytes"] = arenaPoolBudgetEffective
	stats["treedb.process.batch_arena.pool_bytes_estimate"] = arenaPoolEstimate
	stats["treedb.process.batch_arena.pool_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaPoolBytesMax)
	stats["treedb.process.batch_arena.in_flight_bytes_estimate"] = fmt.Sprintf("%d", arenaInFlightBytes)
	stats["treedb.process.batch_arena.in_flight_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaInFlightBytesMax)
	stats["treedb.process.batch_arena.leased_bytes"] = arenaLeased
	stats["treedb.process.batch_arena.leased_bytes_global_estimate"] = fmt.Sprintf("%d", arenaGlobalLeasedBytes)
	stats["treedb.process.batch_arena.leased_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaGlobalLeasedBytesMax)
	stats["treedb.process.batch_arena.leased_bytes_max"] = arenaLeasedMax
	stats["treedb.process.batch_arena.retained_hard_cap_bytes"] = fmt.Sprintf("%d", arenaRetainedHardCapBytes)
	stats["treedb.process.batch_arena.retained_hard_cap_effective_bytes"] = fmt.Sprintf("%d", arenaRetainedHardCapEffectiveBytes)
	stats["treedb.process.batch_arena.deferred_pressure_active"] = fmt.Sprintf("%t", db.batchArenaDeferredPressureActive())
	stats["treedb.process.batch_arena.retained_bytes_estimate"] = fmt.Sprintf("%d", arenaRetainedEstimate)
	stats["treedb.process.batch_arena.retained_bytes_global_max_estimate"] = fmt.Sprintf("%d", arenaRetainedMaxEstimate)
	stats["treedb.process.batch_arena.alloc_requested_bytes_total"] = fmt.Sprintf("%d", arenaAllocRequestedBytes)
	stats["treedb.process.batch_arena.alloc_class_bytes_total"] = fmt.Sprintf("%d", arenaAllocClassBytes)
	stats["treedb.process.batch_arena.used_bytes_total"] = fmt.Sprintf("%d", arenaUsedBytes)
	stats["treedb.process.batch_arena.tail_waste_bytes_total"] = fmt.Sprintf("%d", arenaTailWasteBytes)
	stats["treedb.process.batch_arena.tail_compact_runs_total"] = fmt.Sprintf("%d", arenaTailCompactRuns)
	stats["treedb.process.batch_arena.tail_compact_copied_bytes_total"] = fmt.Sprintf("%d", arenaTailCompactCopied)
	stats["treedb.process.batch_arena.tail_compact_saved_bytes_total"] = fmt.Sprintf("%d", arenaTailCompactSaved)
	stats["treedb.process.batch_arena.pool_skip_zero_budget_total"] = fmt.Sprintf("%d", batchArenaPoolSkipZeroBudgetTotal.Load())
	stats["treedb.process.batch_arena.pool_drop_bytes_total"] = fmt.Sprintf("%d", batchArenaPoolDropBytesTotal.Load())
	stats["treedb.process.batch_arena.pool_drop_hard_cap_bytes_total"] = fmt.Sprintf("%d", batchArenaPoolDropHardCapBytesTotal.Load())
	stats["treedb.process.batch_arena.borrow_blocked_total"] = fmt.Sprintf("%d", batchArenaBorrowBlockedTotal.Load())
	stats["treedb.process.batch_arena.borrow_preflight_blocked_total"] = fmt.Sprintf("%d", batchArenaBorrowPreflightBlockedTotal.Load())
	stats["treedb.process.batch_arena.borrow_preflight_blocked_bytes_total"] = fmt.Sprintf("%d", batchArenaBorrowPreflightBlockedBytesTotal.Load())
	stats["treedb.process.batch_arena.steal_suppressed_deferred_total"] = fmt.Sprintf("%d", batchArenaStealSuppressedDeferredTotal.Load())
	stats["treedb.process.batch_arena.steal_suppressed_deferred_entries_total"] = fmt.Sprintf("%d", batchArenaStealSuppressedDeferredEntriesTotal.Load())
	stats["treedb.cache.entry_slice.pool_budget_bytes"] = fmt.Sprintf("%d", entrySliceBaseBudget)
	stats["treedb.cache.entry_slice.pool_budget_effective_bytes"] = fmt.Sprintf("%d", entrySliceEffectiveBudget)
	stats["treedb.cache.entry_slice.retained_bytes_estimate"] = fmt.Sprintf("%d", entrySlicePoolBytes.Load())
	stats["treedb.cache.entry_slice.trim_runs_total"] = fmt.Sprintf("%d", entrySlicePoolTrimRunsTotal.Load())
	stats["treedb.cache.entry_slice.trim_drop_bytes_total"] = fmt.Sprintf("%d", entrySlicePoolTrimDropBytesTotal.Load())
	stats["treedb.cache.entry_slice.get.lease_hits_total"] = fmt.Sprintf("%d", entrySliceLeaseHitTotal.Load())
	stats["treedb.cache.entry_slice.get.lease_hit_bytes_total"] = fmt.Sprintf("%d", entrySliceLeaseHitBytesTotal.Load())
	stats["treedb.cache.entry_slice.get.pool_hits_total"] = fmt.Sprintf("%d", entrySlicePoolHitTotal.Load())
	stats["treedb.cache.entry_slice.get.pool_hit_bytes_total"] = fmt.Sprintf("%d", entrySlicePoolHitBytesTotal.Load())
	stats["treedb.cache.entry_slice.get.fresh_alloc_total"] = fmt.Sprintf("%d", entrySliceFreshAllocTotal.Load())
	stats["treedb.cache.entry_slice.get.fresh_alloc_bytes_total"] = fmt.Sprintf("%d", entrySliceFreshAllocBytesTotal.Load())
	stats["treedb.cache.entry_slice.put.lease_total"] = fmt.Sprintf("%d", entrySlicePutLeaseTotal.Load())
	stats["treedb.cache.entry_slice.put.lease_bytes_total"] = fmt.Sprintf("%d", entrySlicePutLeaseBytesTotal.Load())
	stats["treedb.cache.entry_slice.put.pool_total"] = fmt.Sprintf("%d", entrySlicePutPoolTotal.Load())
	stats["treedb.cache.entry_slice.put.pool_bytes_total"] = fmt.Sprintf("%d", entrySlicePutPoolBytesTotal.Load())
	stats["treedb.cache.entry_slice.put.drop_budget_total"] = fmt.Sprintf("%d", entrySlicePutDropBudgetTotal.Load())
	stats["treedb.cache.entry_slice.put.drop_budget_bytes_total"] = fmt.Sprintf("%d", entrySlicePutDropBudgetBytesTotal.Load())
	mergeShadowedOpsTotal := flushMergeShadowedOpsTotal.Load()
	mergeAppliedOpsTotal := flushMergeAppliedOpsTotal.Load()
	mergeDeferredShadowedOpsTotal := flushMergeDeferredShadowedOpsTotal.Load()
	mergeDeferredAppliedOpsTotal := flushMergeDeferredAppliedOpsTotal.Load()
	mergeParallelShadowedOpsTotal := flushMergeParallelShadowedOpsTotal.Load()
	mergeParallelAppliedOpsTotal := flushMergeParallelAppliedOpsTotal.Load()
	stats["treedb.cache.flush_merge.shadowed_ops_total"] = fmt.Sprintf("%d", mergeShadowedOpsTotal)
	stats["treedb.cache.flush_merge.applied_ops_total"] = fmt.Sprintf("%d", mergeAppliedOpsTotal)
	if mergeAppliedOpsTotal > 0 {
		stats["treedb.cache.flush_merge.shadowed_per_applied"] = fmt.Sprintf("%.6f", float64(mergeShadowedOpsTotal)/float64(mergeAppliedOpsTotal))
	}
	stats["treedb.cache.flush_merge.deferred.shadowed_ops_total"] = fmt.Sprintf("%d", mergeDeferredShadowedOpsTotal)
	stats["treedb.cache.flush_merge.deferred.applied_ops_total"] = fmt.Sprintf("%d", mergeDeferredAppliedOpsTotal)
	if mergeDeferredAppliedOpsTotal > 0 {
		stats["treedb.cache.flush_merge.deferred.shadowed_per_applied"] = fmt.Sprintf("%.6f", float64(mergeDeferredShadowedOpsTotal)/float64(mergeDeferredAppliedOpsTotal))
	}
	stats["treedb.cache.flush_merge.parallel.shadowed_ops_total"] = fmt.Sprintf("%d", mergeParallelShadowedOpsTotal)
	stats["treedb.cache.flush_merge.parallel.applied_ops_total"] = fmt.Sprintf("%d", mergeParallelAppliedOpsTotal)
	if mergeParallelAppliedOpsTotal > 0 {
		stats["treedb.cache.flush_merge.parallel.shadowed_per_applied"] = fmt.Sprintf("%.6f", float64(mergeParallelShadowedOpsTotal)/float64(mergeParallelAppliedOpsTotal))
	}
	stats["treedb.process.entry_slice.pool_budget_bytes"] = fmt.Sprintf("%d", entrySliceBaseBudget)
	stats["treedb.process.entry_slice.pool_budget_effective_bytes"] = fmt.Sprintf("%d", entrySliceEffectiveBudget)
	stats["treedb.process.entry_slice.retained_bytes_estimate"] = fmt.Sprintf("%d", entrySlicePoolBytes.Load())
	stats["treedb.process.entry_slice.trim_runs_total"] = fmt.Sprintf("%d", entrySlicePoolTrimRunsTotal.Load())
	stats["treedb.process.entry_slice.trim_drop_bytes_total"] = fmt.Sprintf("%d", entrySlicePoolTrimDropBytesTotal.Load())
	stats["treedb.process.entry_slice.get.lease_hits_total"] = fmt.Sprintf("%d", entrySliceLeaseHitTotal.Load())
	stats["treedb.process.entry_slice.get.lease_hit_bytes_total"] = fmt.Sprintf("%d", entrySliceLeaseHitBytesTotal.Load())
	stats["treedb.process.entry_slice.get.pool_hits_total"] = fmt.Sprintf("%d", entrySlicePoolHitTotal.Load())
	stats["treedb.process.entry_slice.get.pool_hit_bytes_total"] = fmt.Sprintf("%d", entrySlicePoolHitBytesTotal.Load())
	stats["treedb.process.entry_slice.get.fresh_alloc_total"] = fmt.Sprintf("%d", entrySliceFreshAllocTotal.Load())
	stats["treedb.process.entry_slice.get.fresh_alloc_bytes_total"] = fmt.Sprintf("%d", entrySliceFreshAllocBytesTotal.Load())
	stats["treedb.process.entry_slice.put.lease_total"] = fmt.Sprintf("%d", entrySlicePutLeaseTotal.Load())
	stats["treedb.process.entry_slice.put.lease_bytes_total"] = fmt.Sprintf("%d", entrySlicePutLeaseBytesTotal.Load())
	stats["treedb.process.entry_slice.put.pool_total"] = fmt.Sprintf("%d", entrySlicePutPoolTotal.Load())
	stats["treedb.process.entry_slice.put.pool_bytes_total"] = fmt.Sprintf("%d", entrySlicePutPoolBytesTotal.Load())
	stats["treedb.process.entry_slice.put.drop_budget_total"] = fmt.Sprintf("%d", entrySlicePutDropBudgetTotal.Load())
	stats["treedb.process.entry_slice.put.drop_budget_bytes_total"] = fmt.Sprintf("%d", entrySlicePutDropBudgetBytesTotal.Load())
	stats["treedb.process.flush_merge.shadowed_ops_total"] = fmt.Sprintf("%d", mergeShadowedOpsTotal)
	stats["treedb.process.flush_merge.applied_ops_total"] = fmt.Sprintf("%d", mergeAppliedOpsTotal)
	if mergeAppliedOpsTotal > 0 {
		stats["treedb.process.flush_merge.shadowed_per_applied"] = fmt.Sprintf("%.6f", float64(mergeShadowedOpsTotal)/float64(mergeAppliedOpsTotal))
	}
	stats["treedb.process.flush_merge.deferred.shadowed_ops_total"] = fmt.Sprintf("%d", mergeDeferredShadowedOpsTotal)
	stats["treedb.process.flush_merge.deferred.applied_ops_total"] = fmt.Sprintf("%d", mergeDeferredAppliedOpsTotal)
	if mergeDeferredAppliedOpsTotal > 0 {
		stats["treedb.process.flush_merge.deferred.shadowed_per_applied"] = fmt.Sprintf("%.6f", float64(mergeDeferredShadowedOpsTotal)/float64(mergeDeferredAppliedOpsTotal))
	}
	stats["treedb.process.flush_merge.parallel.shadowed_ops_total"] = fmt.Sprintf("%d", mergeParallelShadowedOpsTotal)
	stats["treedb.process.flush_merge.parallel.applied_ops_total"] = fmt.Sprintf("%d", mergeParallelAppliedOpsTotal)
	if mergeParallelAppliedOpsTotal > 0 {
		stats["treedb.process.flush_merge.parallel.shadowed_per_applied"] = fmt.Sprintf("%.6f", float64(mergeParallelShadowedOpsTotal)/float64(mergeParallelAppliedOpsTotal))
	}
	appendOnlyEntryHint := int(db.appendOnlyEntryHint.Load())
	appendOnlyHintCapacity := appendOnlyEntriesToCapacity(appendOnlyEntryHint, appendOnlyEstimatedBytesPerEntryDefault)
	appendOnlyMemLeaseHitTotal := db.appendOnlyMemLeaseHitTotal.Load()
	appendOnlyMemPoolHitTotal := db.appendOnlyMemPoolHitTotal.Load()
	appendOnlyMemNewAllocTotal := db.appendOnlyMemNewAllocTotal.Load()
	appendOnlyMemNewAllocWithQueue := db.appendOnlyMemNewAllocWithQueue.Load()
	appendOnlyMemNewAllocQueueBytes := db.appendOnlyMemNewAllocQueueBytes.Load()
	stats["treedb.cache.append_only.entry_hint_entries"] = fmt.Sprintf("%d", appendOnlyEntryHint)
	stats["treedb.cache.append_only.entry_hint_capacity_bytes"] = fmt.Sprintf("%d", appendOnlyHintCapacity)
	stats["treedb.cache.append_only.mutable_from_lease_total"] = fmt.Sprintf("%d", appendOnlyMemLeaseHitTotal)
	stats["treedb.cache.append_only.mutable_from_pool_total"] = fmt.Sprintf("%d", appendOnlyMemPoolHitTotal)
	stats["treedb.cache.append_only.mutable_new_alloc_total"] = fmt.Sprintf("%d", appendOnlyMemNewAllocTotal)
	stats["treedb.cache.append_only.mutable_new_alloc_with_queue_total"] = fmt.Sprintf("%d", appendOnlyMemNewAllocWithQueue)
	stats["treedb.cache.append_only.mutable_new_alloc_queue_bytes_sum"] = fmt.Sprintf("%d", appendOnlyMemNewAllocQueueBytes)
	stats["treedb.process.append_only.entry_hint_entries"] = fmt.Sprintf("%d", appendOnlyEntryHint)
	stats["treedb.process.append_only.entry_hint_capacity_bytes"] = fmt.Sprintf("%d", appendOnlyHintCapacity)
	stats["treedb.process.append_only.mutable_from_lease_total"] = fmt.Sprintf("%d", appendOnlyMemLeaseHitTotal)
	stats["treedb.process.append_only.mutable_from_pool_total"] = fmt.Sprintf("%d", appendOnlyMemPoolHitTotal)
	stats["treedb.process.append_only.mutable_new_alloc_total"] = fmt.Sprintf("%d", appendOnlyMemNewAllocTotal)
	stats["treedb.process.append_only.mutable_new_alloc_with_queue_total"] = fmt.Sprintf("%d", appendOnlyMemNewAllocWithQueueTotal.Load())
	stats["treedb.process.append_only.mutable_new_alloc_queue_bytes_sum"] = fmt.Sprintf("%d", appendOnlyMemNewAllocQueueBytesSum.Load())
	appendOnlyRetainChunks, appendOnlyRetainBytes := appendOnlyDirectArenaRetentionLimitsForPressure(poolPressure.level)
	stats["treedb.cache.append_only_direct_arena.retain_max_bytes_effective"] = fmt.Sprintf("%d", appendOnlyRetainBytes)
	stats["treedb.cache.append_only_direct_arena.retain_max_chunks_effective"] = fmt.Sprintf("%d", appendOnlyRetainChunks)
	stats["treedb.cache.append_only_direct_arena.pool_hit_chunks_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaPoolHitChunksTotal.Load())
	stats["treedb.cache.append_only_direct_arena.pool_hit_bytes_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaPoolHitBytesTotal.Load())
	stats["treedb.cache.append_only_direct_arena.retained_hit_chunks_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaRetainedHitChunksTotal.Load())
	stats["treedb.cache.append_only_direct_arena.retained_hit_bytes_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaRetainedHitBytesTotal.Load())
	stats["treedb.cache.append_only_direct_arena.fresh_alloc_chunks_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaFreshAllocChunksTotal.Load())
	stats["treedb.cache.append_only_direct_arena.fresh_alloc_bytes_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaFreshAllocBytesTotal.Load())
	stats["treedb.cache.batch_pool.drop_under_pressure.entries_total"] = fmt.Sprintf("%d", batchEntriesPoolDropUnderPressureTotal.Load())
	stats["treedb.cache.batch_pool.drop_under_pressure.shard_entries_total"] = fmt.Sprintf("%d", batchShardEntriesPoolDropUnderPressureTotal.Load())
	stats["treedb.cache.batch_pool.drop_under_pressure.int_slices_total"] = fmt.Sprintf("%d", batchIntPoolDropUnderPressureTotal.Load())
	stats["treedb.process.append_only_direct_arena.retain_max_bytes_effective"] = fmt.Sprintf("%d", appendOnlyRetainBytes)
	stats["treedb.process.append_only_direct_arena.retain_max_chunks_effective"] = fmt.Sprintf("%d", appendOnlyRetainChunks)
	stats["treedb.process.append_only_direct_arena.pool_hit_chunks_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaPoolHitChunksTotal.Load())
	stats["treedb.process.append_only_direct_arena.pool_hit_bytes_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaPoolHitBytesTotal.Load())
	stats["treedb.process.append_only_direct_arena.retained_hit_chunks_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaRetainedHitChunksTotal.Load())
	stats["treedb.process.append_only_direct_arena.retained_hit_bytes_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaRetainedHitBytesTotal.Load())
	stats["treedb.process.append_only_direct_arena.fresh_alloc_chunks_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaFreshAllocChunksTotal.Load())
	stats["treedb.process.append_only_direct_arena.fresh_alloc_bytes_total"] = fmt.Sprintf("%d", appendOnlyDirectArenaFreshAllocBytesTotal.Load())
	stats["treedb.process.batch_pool.drop_under_pressure.entries_total"] = fmt.Sprintf("%d", batchEntriesPoolDropUnderPressureTotal.Load())
	stats["treedb.process.batch_pool.drop_under_pressure.shard_entries_total"] = fmt.Sprintf("%d", batchShardEntriesPoolDropUnderPressureTotal.Load())
	stats["treedb.process.batch_pool.drop_under_pressure.int_slices_total"] = fmt.Sprintf("%d", batchIntPoolDropUnderPressureTotal.Load())
	stats["treedb.process.memory.pool_pressure_level"] = poolPressureLevelString(poolPressure.level)
	stats["treedb.process.memory.pool_pressure_used_bytes"] = fmt.Sprintf("%d", poolPressure.usedBytes)
	stats["treedb.process.memory.heap_alloc_bytes"] = fmt.Sprintf("%d", poolPressure.heapAllocBytes)
	stats["treedb.process.memory.heap_inuse_bytes"] = fmt.Sprintf("%d", poolPressure.heapInuseBytes)
	stats["treedb.process.memory.heap_sys_bytes"] = fmt.Sprintf("%d", poolPressure.heapSysBytes)
	stats["treedb.process.memory.heap_idle_bytes"] = fmt.Sprintf("%d", poolPressure.heapIdleBytes)
	stats["treedb.process.memory.heap_released_bytes"] = fmt.Sprintf("%d", poolPressure.heapReleasedBytes)
	stats["treedb.process.memory.heap_idle_unreleased_bytes"] = fmt.Sprintf("%d", poolPressure.heapIdleUnreleased)
	stats["treedb.process.memory.stack_inuse_bytes"] = fmt.Sprintf("%d", poolPressure.stackInuseBytes)
	stats["treedb.process.memory.stack_sys_bytes"] = fmt.Sprintf("%d", poolPressure.stackSysBytes)
	stats["treedb.process.memory.total_sys_bytes"] = fmt.Sprintf("%d", poolPressure.totalSysBytes)
	stats["treedb.process.memory.non_heap_sys_bytes"] = fmt.Sprintf("%d", poolPressure.nonHeapSysBytes)
	stats["treedb.process.memory.next_gc_bytes"] = fmt.Sprintf("%d", poolPressure.nextGCBytes)
	stats["treedb.process.memory.num_gc"] = fmt.Sprintf("%d", poolPressure.numGC)
	stats["treedb.process.memory.gc_cpu_fraction"] = fmt.Sprintf("%.6f", poolPressure.gcCPUFraction)
	stats["treedb.process.memory.gomemlimit_bytes"] = fmt.Sprintf("%d", poolPressure.memoryLimitBytes)
	stats["treedb.process.memory.mutable_bytes"] = fmt.Sprintf("%d", db.mutableBytes.Load())
	stats["treedb.process.memory.memtable_view_deferred_bytes_current"] = fmt.Sprintf("%d", memViewDeferredBytesCurrent)
	stats["treedb.process.memory.memtable_view_deferred_bytes_max"] = fmt.Sprintf("%d", memViewDeferredBytesMax)
	stats["treedb.process.memory.memtable_view_deferred_memtables_current"] = fmt.Sprintf("%d", memViewDeferredMemtablesCurrent)
	stats["treedb.process.memory.memtable_view_deferred_memtables_max"] = fmt.Sprintf("%d", memViewDeferredMemtablesMax)
	stats["treedb.process.memory.memtable_view_deferred_oldest_age_ms"] = fmt.Sprintf("%.3f", memViewOldestDeferredAgeMS)
	stats["treedb.process.memory.pool_pressure_normal_samples_total"] = fmt.Sprintf("%d", poolPressureNormalSamplesTotal.Load())
	stats["treedb.process.memory.pool_pressure_high_samples_total"] = fmt.Sprintf("%d", poolPressureHighSamplesTotal.Load())
	stats["treedb.process.memory.pool_pressure_critical_samples_total"] = fmt.Sprintf("%d", poolPressureCriticalSamplesTotal.Load())
	db.domainIngressMu.Lock()
	ingressWorkers := len(db.domainIngressCh)
	ingressQueueSize := db.domainIngressQueueSize
	ingressDepth := 0
	for _, ch := range db.domainIngressCh {
		ingressDepth += len(ch)
	}
	db.domainIngressMu.Unlock()
	stats["treedb.cache.domain_ingress.enabled"] = fmt.Sprintf("%t", ingressWorkers > 0)
	stats["treedb.cache.domain_ingress.workers"] = fmt.Sprintf("%d", ingressWorkers)
	stats["treedb.cache.domain_ingress.queue_size"] = fmt.Sprintf("%d", ingressQueueSize)
	stats["treedb.cache.domain_ingress.queue_depth"] = fmt.Sprintf("%d", ingressDepth)
	stats["treedb.cache.domain_ingress.queue_depth_max"] = fmt.Sprintf("%d", db.domainIngressDepthMax.Load())
	stats["treedb.cache.domain_ingress.enqueued"] = fmt.Sprintf("%d", db.domainIngressEnqueued.Load())
	stats["treedb.cache.domain_ingress.processed"] = fmt.Sprintf("%d", db.domainIngressProcessed.Load())
	stats["treedb.cache.domain_ingress.fallback_direct"] = fmt.Sprintf("%d", db.domainIngressFallback.Load())
	stats["treedb.cache.wal_bytes_estimate"] = fmt.Sprintf("%d", walClosedBytes+walCurrentBytes)
	stats["treedb.cache.wal_closed_bytes_estimate"] = fmt.Sprintf("%d", walClosedBytes)
	stats["treedb.cache.wal_current_bytes_estimate"] = fmt.Sprintf("%d", walCurrentBytes)
	stats["treedb.cache.vlog_queue.enqueued_total"] = fmt.Sprintf("%d", queueDepthEnqueued)
	stats["treedb.cache.vlog_queue.depth_samples"] = fmt.Sprintf("%d", queueDepthSamples)
	stats["treedb.cache.vlog_queue.depth_last_sum"] = fmt.Sprintf("%d", queueDepthLast)
	stats["treedb.cache.vlog_queue.depth_max"] = fmt.Sprintf("%d", queueDepthMax)
	if queueDepthSamples > 0 {
		stats["treedb.cache.vlog_queue.depth_avg"] = fmt.Sprintf("%.3f", float64(queueDepthSum)/float64(queueDepthSamples))
	}
	stats["treedb.cache.vlog_queue.positive_drift_run_max_ms"] = fmt.Sprintf("%.3f", float64(queueDepthPositiveRunMaxNs)/float64(time.Millisecond))
	stats["treedb.cache.vlog_queue.lag_samples"] = fmt.Sprintf("%d", queueLagCount)
	stats["treedb.cache.vlog_queue.lag_max_ms"] = fmt.Sprintf("%.3f", float64(queueLagMaxNs)/float64(time.Millisecond))
	if queueLagCount > 0 {
		stats["treedb.cache.vlog_queue.lag_avg_ms"] = fmt.Sprintf("%.3f", (float64(queueLagTotalNs)/float64(queueLagCount))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p50_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.50))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p95_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.95))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p99_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.99))/float64(time.Millisecond))
		stats["treedb.cache.vlog_queue.lag_p999_ms"] = fmt.Sprintf("%.3f", float64(estimateVlogQueueLagPercentile(queueLagBuckets, queueLagCount, 0.999))/float64(time.Millisecond))
	}
	for bucket := 0; bucket < vlogQueueLagBucketCount; bucket++ {
		upperUS := vlogQueueLagBucketUpperBounds[bucket].Microseconds()
		key := fmt.Sprintf("treedb.cache.vlog_queue.lag_bucket.le_us.%d", upperUS)
		stats[key] = fmt.Sprintf("%d", queueLagBuckets[bucket])
	}
	retained := db.valueLogRetainedStatsDetailed()
	vlogSegments, vlogBytes := retained.SegmentsTotal, retained.BytesTotal
	db.vlogGenerationRewriteQueueMu.Lock()
	rewriteQueueLen := len(db.vlogGenerationRewriteQueue)
	rewriteQueueLoaded := db.vlogGenerationRewriteQueueLoaded
	db.vlogGenerationRewriteQueueMu.Unlock()
	stats["treedb.cache.vlog_retained_segments"] = fmt.Sprintf("%d", vlogSegments)
	stats["treedb.cache.vlog_retained_bytes_estimate"] = fmt.Sprintf("%d", vlogBytes)
	stats["treedb.process.memory.vlog_retained_bytes_estimate"] = fmt.Sprintf("%d", vlogBytes)
	stats["treedb.cache.vlog_generation.policy"] = fmt.Sprintf("%d", db.valueLogGenerationPolicy)
	stats["treedb.cache.vlog_generation.enabled"] = fmt.Sprintf("%t", db.valueLogGenerationPolicy == uint8(backenddb.ValueLogGenerationHotWarmCold))
	stats["treedb.cache.vlog_generation.scheduler_state"] = vlogGenerationSchedulerStateString(db.vlogGenerationSchedulerState.Load())
	stats["treedb.cache.vlog_generation.scheduler_last_reason"] = vlogGenerationReasonString(db.vlogGenerationLastReason.Load())
	stats["treedb.cache.vlog_generation.checkpoint_kick.active"] = fmt.Sprintf("%t", db.vlogGenerationCheckpointKickActive.Load())
	stats["treedb.cache.vlog_generation.checkpoint_kick.pending"] = fmt.Sprintf("%t", db.vlogGenerationCheckpointKickPending.Load())
	stats["treedb.cache.vlog_generation.checkpoint_kick.last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastCheckpointKickUnixNano.Load())
	stats["treedb.cache.vlog_generation.checkpoint_kick.runs"] = fmt.Sprintf("%d", db.vlogGenerationCheckpointKickRuns.Load())
	stats["treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs"] = fmt.Sprintf("%d", db.vlogGenerationCheckpointKickRewriteRuns.Load())
	stats["treedb.cache.vlog_generation.checkpoint_kick.gc_runs"] = fmt.Sprintf("%d", db.vlogGenerationCheckpointKickGCRuns.Load())
	stats["treedb.cache.vlog_generation.churn_bytes_total"] = fmt.Sprintf("%d", db.vlogGenerationChurnBytes.Load())
	stats["treedb.cache.vlog_generation.churn_bytes_per_sec"] = fmt.Sprintf("%d", db.vlogGenerationLastChurnBps.Load())
	stats["treedb.cache.vlog_generation.rewrite.queue_len"] = fmt.Sprintf("%d", rewriteQueueLen)
	stats["treedb.cache.vlog_generation.rewrite.queue_loaded"] = fmt.Sprintf("%t", rewriteQueueLoaded)
	stats["treedb.cache.vlog_generation.hot.segment_target_bytes"] = fmt.Sprintf("%d", db.valueLogGenerationHotTarget)
	stats["treedb.cache.vlog_generation.warm.segment_target_bytes"] = fmt.Sprintf("%d", db.valueLogGenerationWarmTarget)
	stats["treedb.cache.vlog_generation.cold.segment_target_bytes"] = fmt.Sprintf("%d", db.valueLogGenerationColdTarget)
	stats["treedb.cache.vlog_generation.rewrite_budget.bytes_per_sec"] = fmt.Sprintf("%d", db.valueLogRewriteBudgetBytes)
	stats["treedb.cache.vlog_generation.rewrite_budget.records_per_sec"] = fmt.Sprintf("%d", db.valueLogRewriteBudgetRecords)
	stats["treedb.cache.vlog_generation.rewrite_trigger.stale_ratio_ppm"] = fmt.Sprintf("%d", db.valueLogRewriteTriggerRatioPPM)
	stats["treedb.cache.vlog_generation.rewrite_trigger.total_bytes"] = fmt.Sprintf("%d", db.valueLogRewriteTriggerBytes)
	stats["treedb.cache.vlog_generation.rewrite_trigger.churn_per_sec"] = fmt.Sprintf("%d", db.valueLogRewriteTriggerChurn)
	// PR1 scaffolding: legacy allocator still owns placement; report retained
	// totals under hot generation until generation-aware allocator lands.
	stats["treedb.cache.vlog_generation.bytes.live.total"] = fmt.Sprintf("%d", retained.BytesTotal)
	stats["treedb.cache.vlog_generation.bytes.live.hot"] = fmt.Sprintf("%d", retained.BytesHot)
	stats["treedb.cache.vlog_generation.bytes.live.warm"] = fmt.Sprintf("%d", retained.BytesWarm)
	stats["treedb.cache.vlog_generation.bytes.live.cold"] = fmt.Sprintf("%d", retained.BytesCold)
	stats["treedb.cache.vlog_generation.bytes.stale.total"] = "0"
	stats["treedb.cache.vlog_generation.bytes.stale.hot"] = "0"
	stats["treedb.cache.vlog_generation.bytes.stale.warm"] = "0"
	stats["treedb.cache.vlog_generation.bytes.stale.cold"] = "0"
	stats["treedb.cache.vlog_generation.bytes.total.total"] = fmt.Sprintf("%d", retained.BytesTotal)
	stats["treedb.cache.vlog_generation.bytes.total.hot"] = fmt.Sprintf("%d", retained.BytesHot)
	stats["treedb.cache.vlog_generation.bytes.total.warm"] = fmt.Sprintf("%d", retained.BytesWarm)
	stats["treedb.cache.vlog_generation.bytes.total.cold"] = fmt.Sprintf("%d", retained.BytesCold)
	stats["treedb.cache.vlog_generation.segments.total"] = fmt.Sprintf("%d", retained.SegmentsTotal)
	stats["treedb.cache.vlog_generation.segments.hot"] = fmt.Sprintf("%d", retained.SegmentsHot)
	stats["treedb.cache.vlog_generation.segments.warm"] = fmt.Sprintf("%d", retained.SegmentsWarm)
	stats["treedb.cache.vlog_generation.segments.cold"] = fmt.Sprintf("%d", retained.SegmentsCold)
	stats["treedb.cache.vlog_generation.rewrite.bytes_in"] = fmt.Sprintf("%d", db.vlogGenerationRewriteBytesIn.Load())
	stats["treedb.cache.vlog_generation.rewrite.bytes_out"] = fmt.Sprintf("%d", db.vlogGenerationRewriteBytesOut.Load())
	stats["treedb.cache.vlog_generation.rewrite.runs"] = fmt.Sprintf("%d", db.vlogGenerationRewriteRuns.Load())
	stats["treedb.cache.vlog_generation.rewrite.plan_runs"] = fmt.Sprintf("%d", db.vlogGenerationRewritePlanRuns.Load())
	stats["treedb.cache.vlog_generation.rewrite.plan_canceled"] = fmt.Sprintf("%d", db.vlogGenerationRewritePlanCanceled.Load())
	stats["treedb.cache.vlog_generation.rewrite.plan_canceled_last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationRewritePlanCanceledLastNS.Load())
	stats["treedb.cache.vlog_generation.rewrite.plan_errors"] = fmt.Sprintf("%d", db.vlogGenerationRewritePlanErrors.Load())
	stats["treedb.cache.vlog_generation.rewrite.plan_empty"] = fmt.Sprintf("%d", db.vlogGenerationRewritePlanEmpty.Load())
	stats["treedb.cache.vlog_generation.rewrite.plan_selected"] = fmt.Sprintf("%d", db.vlogGenerationRewritePlanSelected.Load())
	stats["treedb.cache.vlog_generation.rewrite.canceled_runs"] = fmt.Sprintf("%d", db.vlogGenerationRewriteCanceledRuns.Load())
	stats["treedb.cache.vlog_generation.rewrite.canceled_last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationRewriteCanceledLastNS.Load())
	stats["treedb.cache.vlog_generation.rewrite.queue_prune_runs"] = fmt.Sprintf("%d", db.vlogGenerationRewriteQueuePruneRuns.Load())
	stats["treedb.cache.vlog_generation.rewrite.queue_prune_ids"] = fmt.Sprintf("%d", db.vlogGenerationRewriteQueuePruneIDs.Load())
	stats["treedb.cache.vlog_generation.rewrite.ineffective_runs"] = fmt.Sprintf("%d", db.vlogGenerationRewriteIneffectiveRuns.Load())
	stats["treedb.cache.vlog_generation.rewrite.ineffective_bytes_in"] = fmt.Sprintf("%d", db.vlogGenerationRewriteIneffectiveBytesIn.Load())
	stats["treedb.cache.vlog_generation.rewrite.ineffective_bytes_out"] = fmt.Sprintf("%d", db.vlogGenerationRewriteIneffectiveBytesOut.Load())
	stats["treedb.cache.vlog_generation.rewrite.ineffective_last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationRewriteIneffectiveLastNS.Load())
	stats["treedb.cache.vlog_generation.rewrite.ineffective_backoff_seconds"] = fmt.Sprintf("%.0f", vlogGenerationRewriteIneffectiveBackoff.Seconds())
	stats["treedb.cache.vlog_generation.rewrite.plan_last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastRewritePlanUnixNano.Load())
	stats["treedb.cache.vlog_generation.rewrite.last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastRewriteUnixNano.Load())
	stats["treedb.cache.vlog_generation.gc.deleted_segments"] = fmt.Sprintf("%d", db.vlogGenerationGCSegmentsDeleted.Load())
	stats["treedb.cache.vlog_generation.gc.deleted_bytes"] = fmt.Sprintf("%d", db.vlogGenerationGCBytesDeleted.Load())
	stats["treedb.cache.vlog_generation.gc.runs"] = fmt.Sprintf("%d", db.vlogGenerationGCRuns.Load())
	stats["treedb.cache.vlog_generation.gc.last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastGCUnixNano.Load())
	stats["treedb.cache.vlog_generation.gc.dry_run.last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastGCDryRunUnixNano.Load())
	stats["treedb.cache.vlog_generation.gc.dry_run.last_eligible_bytes"] = fmt.Sprintf("%d", db.vlogGenerationLastGCDryRunBytesEligible.Load())
	stats["treedb.cache.vlog_generation.gc.dry_run.last_eligible_segments"] = fmt.Sprintf("%d", db.vlogGenerationLastGCDryRunSegsEligible.Load())
	stats["treedb.cache.vlog_generation.vacuum.runs"] = fmt.Sprintf("%d", db.vlogGenerationVacuumRuns.Load())
	stats["treedb.cache.vlog_generation.vacuum.failures"] = fmt.Sprintf("%d", db.vlogGenerationVacuumFailures.Load())
	stats["treedb.cache.vlog_generation.vacuum.last_unix_nano"] = fmt.Sprintf("%d", db.vlogGenerationLastVacuumUnixNano.Load())
	stats["treedb.cache.vlog_generation.remap.successes"] = fmt.Sprintf("%d", db.vlogGenerationRemapSuccesses.Load())
	stats["treedb.cache.vlog_generation.remap.failures"] = fmt.Sprintf("%d", db.vlogGenerationRemapFailures.Load())
	stats["treedb.cache.vlog_writev.syscalls"] = fmt.Sprintf("%d", rawWritevSyscalls)
	stats["treedb.cache.vlog_writev.bytes"] = fmt.Sprintf("%d", rawWritevBytes)
	stats["treedb.cache.vlog_writev.iovecs"] = fmt.Sprintf("%d", rawWritevIovecs)
	stats["treedb.cache.vlog_writev.flushes"] = fmt.Sprintf("%d", rawWritevFlushes)
	if rawWritevSyscalls > 0 {
		stats["treedb.cache.vlog_writev.bytes_per_syscall"] = fmt.Sprintf("%.1f", float64(rawWritevBytes)/float64(rawWritevSyscalls))
		stats["treedb.cache.vlog_writev.iovecs_per_syscall"] = fmt.Sprintf("%.2f", float64(rawWritevIovecs)/float64(rawWritevSyscalls))
	}
	if rawWritevFlushes > 0 {
		stats["treedb.cache.vlog_writev.bytes_per_flush"] = fmt.Sprintf("%.1f", float64(rawWritevBytes)/float64(rawWritevFlushes))
		stats["treedb.cache.vlog_writev.syscalls_per_flush"] = fmt.Sprintf("%.2f", float64(rawWritevSyscalls)/float64(rawWritevFlushes))
	}
	stats["treedb.cache.vlog_write.syscalls"] = fmt.Sprintf("%d", rawWriteSyscalls)
	stats["treedb.cache.vlog_write.bytes"] = fmt.Sprintf("%d", rawWriteBytes)
	stats["treedb.cache.vlog_write.calls"] = fmt.Sprintf("%d", rawWriteCalls)
	if rawWriteSyscalls > 0 {
		stats["treedb.cache.vlog_write.bytes_per_syscall"] = fmt.Sprintf("%.1f", float64(rawWriteBytes)/float64(rawWriteSyscalls))
	}
	if rawWriteCalls > 0 {
		stats["treedb.cache.vlog_write.bytes_per_call"] = fmt.Sprintf("%.1f", float64(rawWriteBytes)/float64(rawWriteCalls))
		stats["treedb.cache.vlog_write.syscalls_per_call"] = fmt.Sprintf("%.2f", float64(rawWriteSyscalls)/float64(rawWriteCalls))
	}
	totalVlogSyscalls := rawWritevSyscalls + rawWriteSyscalls
	totalVlogBytes := rawWritevBytes + rawWriteBytes
	stats["treedb.cache.vlog_io.syscalls"] = fmt.Sprintf("%d", totalVlogSyscalls)
	stats["treedb.cache.vlog_io.bytes"] = fmt.Sprintf("%d", totalVlogBytes)
	if totalVlogSyscalls > 0 {
		stats["treedb.cache.vlog_io.bytes_per_syscall"] = fmt.Sprintf("%.1f", float64(totalVlogBytes)/float64(totalVlogSyscalls))
	}
	if db.adaptiveBackpressureEnabled() {
		stats["treedb.cache.backpressure_mode"] = "adaptive"
	} else {
		stats["treedb.cache.backpressure_mode"] = "queue_len"
	}
	now := time.Now()
	backlogBytes := db.queueBacklogBytes.Load()
	if backlogBytes <= 0 {
		db.materializationLastDrainUnixNano.Store(now.UnixNano())
	}
	stats["treedb.cache.queue_backlog_bytes"] = fmt.Sprintf("%d", backlogBytes)
	stats["treedb.process.memory.queue_backlog_bytes"] = fmt.Sprintf("%d", backlogBytes)
	stats["treedb.cache.queue_laneid_misses"] = fmt.Sprintf("%d", db.queueLaneIDMisses.Load())
	stats["treedb.cache.stats.backend_write_batches_total"] = fmt.Sprintf("%d", db.backendWriteBatchesTotal.Load())
	watermarkLagDriftBps := db.observePublishWatermarkLagDrift(backlogBytes, now)
	stats["treedb.publish.watermark.lag_drift_bytes_per_sec"] = fmt.Sprintf("%.3f", watermarkLagDriftBps)
	if _, ok := stats["treedb.publish.watermark.lock_delay_share_pct"]; !ok {
		stats["treedb.publish.watermark.lock_delay_share_pct"] = "0.000"
	}
	if _, ok := stats["treedb.publish.watermark.latency_p99_ms"]; !ok {
		stats["treedb.publish.watermark.latency_p99_ms"] = "0.000"
	}
	materializationMarkNS := db.materializationLastDrainUnixNano.Load()
	if backlogBytes > 0 && oldestQueueEnqueueNS > 0 {
		materializationMarkNS = oldestQueueEnqueueNS
	}
	materializationLagAge := time.Duration(0)
	if materializationMarkNS > 0 {
		if ageNS := now.UnixNano() - materializationMarkNS; ageNS > 0 {
			materializationLagAge = time.Duration(ageNS)
		}
	}
	stats["treedb.cache.materialization.last_unix_nano"] = fmt.Sprintf("%d", materializationMarkNS)
	stats["treedb.cache.materialization.oldest_enqueue_unix_nano"] = fmt.Sprintf("%d", oldestQueueEnqueueNS)
	stats["treedb.cache.materialization.lag_age_ms"] = fmt.Sprintf("%.3f", float64(materializationLagAge)/float64(time.Millisecond))
	db.bpMu.Lock()
	stats["treedb.cache.flush_bps_ewma"] = fmt.Sprintf("%.0f", db.flushBpsEWMA)
	db.bpMu.Unlock()

	stats["treedb.cache.auto_checkpoint.count"] = fmt.Sprintf("%d", db.autoCheckpointCount.Load())
	stats["treedb.cache.auto_checkpoint.last_reason"] = autoCheckpointReasonString(db.autoCheckpointLastReason.Load())
	stats["treedb.cache.auto_checkpoint.last_duration_ms"] = fmt.Sprintf("%.3f", float64(db.autoCheckpointLastDurNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALAfter.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_reclaimable_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALReclaimableBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_reclaimable_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALReclaimableAfter.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_trimmed"] = fmt.Sprintf("%d", db.autoCheckpointLastWALTrimmed.Load())
	stats["treedb.cache.auto_checkpoint.last_unix_nano"] = fmt.Sprintf("%d", db.autoCheckpointLastUnixNano.Load())
	cutoverSamples := db.checkpointCutoverSamples.Load()
	cutoverTotalNS := db.checkpointCutoverTotalNanos.Load()
	cutoverAvgMS := 0.0
	if cutoverSamples > 0 {
		cutoverAvgMS = (float64(cutoverTotalNS) / float64(cutoverSamples)) / float64(time.Millisecond)
	}
	stats["treedb.cache.checkpoint.cutover_samples"] = fmt.Sprintf("%d", cutoverSamples)
	stats["treedb.cache.checkpoint.cutover_last_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointCutoverLastNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.cutover_max_ms"] = fmt.Sprintf("%.3f", float64(db.checkpointCutoverMaxNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.checkpoint.cutover_avg_ms"] = fmt.Sprintf("%.3f", cutoverAvgMS)
	stats["treedb.cache.checkpoint.cutover_last_unix_nano"] = fmt.Sprintf("%d", db.checkpointCutoverLastUnixNano.Load())

	vlogFramesTotal := db.valueLogDictFrames.total.Load()
	vlogFramesAttempted := db.valueLogDictFrames.attempted.Load()
	vlogFramesKept := db.valueLogDictFrames.kept.Load()
	stats["treedb.cache.vlog_dict.frames_total"] = fmt.Sprintf("%d", vlogFramesTotal)
	stats["treedb.cache.vlog_dict.frames_attempted"] = fmt.Sprintf("%d", vlogFramesAttempted)
	stats["treedb.cache.vlog_dict.frames_kept"] = fmt.Sprintf("%d", vlogFramesKept)
	if vlogFramesTotal > 0 {
		stats["treedb.cache.vlog_dict.attempted_frac"] = fmt.Sprintf("%.6f", float64(vlogFramesAttempted)/float64(vlogFramesTotal))
		stats["treedb.cache.vlog_dict.kept_frac"] = fmt.Sprintf("%.6f", float64(vlogFramesKept)/float64(vlogFramesTotal))
	}
	classifySampled := db.valueLogDictClassifySampled.Load()
	classifySkipped := db.valueLogDictClassifySkipped.Load()
	stats["treedb.cache.vlog_dict.classifier.sampled"] = fmt.Sprintf("%d", classifySampled)
	stats["treedb.cache.vlog_dict.classifier.skipped"] = fmt.Sprintf("%d", classifySkipped)
	if classifySampled > 0 {
		stats["treedb.cache.vlog_dict.classifier.skip_frac"] = fmt.Sprintf("%.6f", float64(classifySkipped)/float64(classifySampled))
	}
	if db.valueLogTemplateEngine != nil {
		for k, v := range db.valueLogTemplateEngine.StatsSnapshot() {
			stats["treedb.cache.vlog_template."+k] = v
		}
	}
	growStats := valuelog.GrowBufferStatsSnapshot()
	stats["treedb.cache.vlog_decode_buffer_grow.calls_total"] = fmt.Sprintf("%d", growStats.CallsTotal)
	stats["treedb.cache.vlog_decode_buffer_grow.realloc_calls_total"] = fmt.Sprintf("%d", growStats.ReallocCallsTotal)
	stats["treedb.cache.vlog_decode_buffer_grow.requested_bytes_total"] = fmt.Sprintf("%d", growStats.RequestedBytesTotal)
	stats["treedb.cache.vlog_decode_buffer_grow.allocated_bytes_total"] = fmt.Sprintf("%d", growStats.AllocatedBytesTotal)
	stats["treedb.cache.vlog_decode_buffer_grow.copied_bytes_total"] = fmt.Sprintf("%d", growStats.CopiedBytesTotal)
	stats["treedb.cache.vlog_decode_buffer_grow.capacity_waste_bytes_total"] = fmt.Sprintf("%d", growStats.CapacityWasteBytesTotal)
	if growStats.CallsTotal > 0 {
		stats["treedb.cache.vlog_decode_buffer_grow.realloc_rate"] = fmt.Sprintf("%.6f", float64(growStats.ReallocCallsTotal)/float64(growStats.CallsTotal))
	}
	if growStats.ReallocCallsTotal > 0 {
		stats["treedb.cache.vlog_decode_buffer_grow.avg_allocated_bytes_per_realloc"] = fmt.Sprintf("%.3f", float64(growStats.AllocatedBytesTotal)/float64(growStats.ReallocCallsTotal))
		stats["treedb.cache.vlog_decode_buffer_grow.avg_copied_bytes_per_realloc"] = fmt.Sprintf("%.3f", float64(growStats.CopiedBytesTotal)/float64(growStats.ReallocCallsTotal))
	}
	if growStats.RequestedBytesTotal > 0 {
		stats["treedb.cache.vlog_decode_buffer_grow.overalloc_ratio"] = fmt.Sprintf("%.6f", float64(growStats.AllocatedBytesTotal)/float64(growStats.RequestedBytesTotal))
	}
	if db.valueLogReader != nil {
		remaps, deadMappings := db.valueLogReader.RemapStats()
		stats["treedb.cache.vlog_mmap.remaps"] = fmt.Sprintf("%d", remaps)
		stats["treedb.cache.vlog_mmap.dead_mappings"] = fmt.Sprintf("%d", deadMappings)
		stats["treedb.cache.vlog_mmap.dead_mappings.cap_base"] = fmt.Sprintf("%d", valuelog.MaxDeadMappings)
		stats["treedb.cache.vlog_mmap.max_mapped_sealed_segments"] = fmt.Sprintf("%d", valuelog.MaxMappedSealedSegments)
		stats["treedb.cache.vlog_mmap.max_mapped_sealed_bytes"] = fmt.Sprintf("%d", valuelog.MaxMappedSealedBytes)
		currentSegments, currentBytes, sealedSegments, sealedBytes, _, deadBytes := db.valueLogReader.MmapResidencyStats()
		stats["treedb.cache.vlog_mmap.active_segments"] = fmt.Sprintf("%d", currentSegments+sealedSegments)
		stats["treedb.cache.vlog_mmap.active_bytes"] = fmt.Sprintf("%d", currentBytes+sealedBytes)
		stats["treedb.cache.vlog_mmap.current_segments"] = fmt.Sprintf("%d", currentSegments)
		stats["treedb.cache.vlog_mmap.current_bytes"] = fmt.Sprintf("%d", currentBytes)
		stats["treedb.cache.vlog_mmap.sealed_segments"] = fmt.Sprintf("%d", sealedSegments)
		stats["treedb.cache.vlog_mmap.sealed_bytes"] = fmt.Sprintf("%d", sealedBytes)
		stats["treedb.cache.vlog_mmap.dead_bytes"] = fmt.Sprintf("%d", deadBytes)
		stats["treedb.process.memory.vlog_mmap_active_bytes"] = fmt.Sprintf("%d", currentBytes+sealedBytes)
		stats["treedb.process.memory.vlog_mmap_current_bytes"] = fmt.Sprintf("%d", currentBytes)
		stats["treedb.process.memory.vlog_mmap_sealed_bytes"] = fmt.Sprintf("%d", sealedBytes)
		stats["treedb.process.memory.vlog_mmap_dead_bytes"] = fmt.Sprintf("%d", deadBytes)
		sealedDeniedCountCap, sealedDeniedBytesCap := db.valueLogReader.SealedMapDeniedByReasonStats()
		stats["treedb.cache.vlog_mmap.sealed_map_denied.count_cap"] = fmt.Sprintf("%d", sealedDeniedCountCap)
		stats["treedb.cache.vlog_mmap.sealed_map_denied.bytes_cap"] = fmt.Sprintf("%d", sealedDeniedBytesCap)
		stats["treedb.cache.vlog_mmap.sealed_map_denied"] = fmt.Sprintf("%d", sealedDeniedCountCap+sealedDeniedBytesCap)

		mmapHits, mmapMissOutOfRange, mmapMissNoMapping, mmapMissDeadCap, mmapFallbackReadAt := db.valueLogReader.MmapReadStats()
		stats["treedb.cache.vlog_mmap.read.hits"] = fmt.Sprintf("%d", mmapHits)
		stats["treedb.cache.vlog_mmap.read.miss_out_of_range"] = fmt.Sprintf("%d", mmapMissOutOfRange)
		stats["treedb.cache.vlog_mmap.read.miss_no_mapping"] = fmt.Sprintf("%d", mmapMissNoMapping)
		stats["treedb.cache.vlog_mmap.read.miss_dead_mapping_cap"] = fmt.Sprintf("%d", mmapMissDeadCap)
		stats["treedb.cache.vlog_mmap.read.fallback_readat"] = fmt.Sprintf("%d", mmapFallbackReadAt)
		if total := mmapHits + mmapFallbackReadAt; total > 0 {
			stats["treedb.cache.vlog_mmap.read.hit_ratio"] = fmt.Sprintf("%.6f", float64(mmapHits)/float64(total))
		}

		hits, misses, entries, capacity := db.valueLogReader.TemplateDefCacheStats()
		stats["treedb.cache.vlog_template_def_cache.hits"] = fmt.Sprintf("%d", hits)
		stats["treedb.cache.vlog_template_def_cache.misses"] = fmt.Sprintf("%d", misses)
		stats["treedb.cache.vlog_template_def_cache.entries"] = fmt.Sprintf("%d", entries)
		stats["treedb.cache.vlog_template_def_cache.capacity"] = fmt.Sprintf("%d", capacity)
		if total := hits + misses; total > 0 {
			stats["treedb.cache.vlog_template_def_cache.hit_ratio"] = fmt.Sprintf("%.6f", float64(hits)/float64(total))
		}
	}
	stats["treedb.cache.vlog_dict.pause_remaining_bytes"] = fmt.Sprintf("%d", db.valueLogDictPauseRemaining.Load())
	stats["treedb.cache.vlog_dict.incompressible_hold_remaining_bytes"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleHoldRemaining.Load())
	stats["treedb.cache.vlog_dict.incompressible_hits"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleHits.Load())
	stats["treedb.cache.vlog_dict.incompressible_holds"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleHolds.Load())
	stats["treedb.cache.vlog_dict.incompressible_bypass_bytes"] = fmt.Sprintf("%d", db.valueLogDictIncompressibleBypassBytes.Load())
	stats["treedb.cache.vlog_dict.last_applied_dict_id"] = fmt.Sprintf("%d", db.valueLogDictLastAppliedDictID.Load())
	stats["treedb.cache.vlog_dict.last_applied_dict_hash"] = fmt.Sprintf("%x", db.valueLogDictLastAppliedDictHash.Load())
	stats["treedb.cache.vlog_dict.last_publish_unix_nano"] = fmt.Sprintf("%d", db.valueLogDictLastPublishUnixNano.Load())
	stats["treedb.cache.vlog_dict.last_k_update_unix_nano"] = fmt.Sprintf("%d", db.valueLogDictLastKUpdateUnixNano.Load())
	stats["treedb.cache.vlog_dict.current_k"] = fmt.Sprintf("%d", db.valueLogDictCurrentK.Load())
	db.valueLogDictBytesMu.Lock()
	stats["treedb.cache.vlog_dict.cached_dict_id"] = fmt.Sprintf("%d", db.valueLogDictBytesID)
	stats["treedb.cache.vlog_dict.cached_dict_bytes"] = fmt.Sprintf("%d", len(db.valueLogDictBytes))
	db.valueLogDictBytesMu.Unlock()
	var blockKSnap vlogBlockKSnapshot
	var blockRatioWeighted [vlogBlockCodecCount]float64
	var blockRatioSamples [vlogBlockCodecCount]uint64
	for i := range db.lanes {
		kSnap := snapshotLaneVlogBlockK(&db.lanes[i])
		rSnap := snapshotLaneVlogBlockRatio(&db.lanes[i])
		for codecIdx := 0; codecIdx < vlogBlockCodecCount; codecIdx++ {
			blockKSnap.Count[codecIdx] += kSnap.Count[codecIdx]
			blockKSnap.Sum[codecIdx] += kSnap.Sum[codecIdx]
			if kSnap.Max[codecIdx] > blockKSnap.Max[codecIdx] {
				blockKSnap.Max[codecIdx] = kSnap.Max[codecIdx]
			}
			for bucket := 0; bucket < vlogBlockKBucketCount; bucket++ {
				blockKSnap.Buckets[codecIdx][bucket] += kSnap.Buckets[codecIdx][bucket]
			}
			samples := rSnap.Samples[codecIdx]
			if samples == 0 {
				continue
			}
			blockRatioSamples[codecIdx] += samples
			blockRatioWeighted[codecIdx] += rSnap.Ratio[codecIdx] * float64(samples)
		}
	}
	for codecIdx := 0; codecIdx < vlogBlockCodecCount; codecIdx++ {
		suffix := vlogBlockCodecSuffix(codecIdx)
		count := blockKSnap.Count[codecIdx]
		sum := blockKSnap.Sum[codecIdx]
		stats["treedb.cache.vlog_block.k.count."+suffix] = fmt.Sprintf("%d", count)
		stats["treedb.cache.vlog_block.k.max."+suffix] = fmt.Sprintf("%d", blockKSnap.Max[codecIdx])
		if count > 0 {
			stats["treedb.cache.vlog_block.k.avg."+suffix] = fmt.Sprintf("%.3f", float64(sum)/float64(count))
		}
		for bucket := 0; bucket < vlogBlockKBucketCount; bucket++ {
			key := fmt.Sprintf("treedb.cache.vlog_block.k.bucket.%s.le_%d", suffix, vlogBlockKBucketUpperBounds[bucket])
			stats[key] = fmt.Sprintf("%d", blockKSnap.Buckets[codecIdx][bucket])
		}
		if blockRatioSamples[codecIdx] > 0 {
			ratio := blockRatioWeighted[codecIdx] / float64(blockRatioSamples[codecIdx])
			stats["treedb.cache.vlog_block.ratio."+suffix] = fmt.Sprintf("%.6f", ratio)
		}
		stats["treedb.cache.vlog_block.ratio.samples."+suffix] = fmt.Sprintf("%d", blockRatioSamples[codecIdx])
	}
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionAuto {
		var autoSnap vlogCompressionSelectorStats
		for i := range db.lanes {
			selector := db.lanes[i].vlogCompressionSelector
			if selector == nil {
				continue
			}
			snap := selector.snapshot()
			for c := 0; c < vlogAutoCandidateCount; c++ {
				autoSnap.bytesByCandidate[c] += snap.bytesByCandidate[c]
				autoSnap.framesByCandidate[c] += snap.framesByCandidate[c]
			}
			for from := 0; from < vlogAutoCandidateCount; from++ {
				for to := 0; to < vlogAutoCandidateCount; to++ {
					autoSnap.switches[from][to] += snap.switches[from][to]
				}
			}
			autoSnap.probeAttempts += snap.probeAttempts
			autoSnap.probeSuccesses += snap.probeSuccesses
			autoSnap.holdEnters += snap.holdEnters
			autoSnap.holdExits += snap.holdExits
			autoSnap.bypassBytes += snap.bypassBytes
		}
		var totalAutoFrames uint64
		for c := 0; c < vlogAutoCandidateCount; c++ {
			name := vlogAutoCandidate(c).suffix()
			bytes := autoSnap.bytesByCandidate[c]
			frames := autoSnap.framesByCandidate[c]
			totalAutoFrames += frames
			stats["treedb.cache.vlog_auto.bytes."+name] = fmt.Sprintf("%d", bytes)
			stats["treedb.cache.vlog_auto.frames."+name] = fmt.Sprintf("%d", frames)
		}
		if totalAutoFrames > 0 {
			for c := 0; c < vlogAutoCandidateCount; c++ {
				name := vlogAutoCandidate(c).suffix()
				stats["treedb.cache.vlog_auto.frames_frac."+name] = fmt.Sprintf("%.6f", float64(autoSnap.framesByCandidate[c])/float64(totalAutoFrames))
			}
		}
		stats["treedb.cache.vlog_auto.probe_attempts"] = fmt.Sprintf("%d", autoSnap.probeAttempts)
		stats["treedb.cache.vlog_auto.probe_successes"] = fmt.Sprintf("%d", autoSnap.probeSuccesses)
		if autoSnap.probeAttempts > 0 {
			stats["treedb.cache.vlog_auto.probe_success_frac"] = fmt.Sprintf("%.6f", float64(autoSnap.probeSuccesses)/float64(autoSnap.probeAttempts))
		}
		stats["treedb.cache.vlog_auto.hold_enters"] = fmt.Sprintf("%d", autoSnap.holdEnters)
		stats["treedb.cache.vlog_auto.hold_exits"] = fmt.Sprintf("%d", autoSnap.holdExits)
		stats["treedb.cache.vlog_auto.bypass_bytes"] = fmt.Sprintf("%d", autoSnap.bypassBytes)
		for from := 0; from < vlogAutoCandidateCount; from++ {
			for to := 0; to < vlogAutoCandidateCount; to++ {
				if from == to {
					continue
				}
				n := autoSnap.switches[from][to]
				if n == 0 {
					continue
				}
				key := fmt.Sprintf("treedb.cache.vlog_auto.switches.%s_to_%s", vlogAutoCandidate(from).suffix(), vlogAutoCandidate(to).suffix())
				stats[key] = fmt.Sprintf("%d", n)
			}
		}
	}
	db.valueLogDictTrainerMu.Lock()
	tr := db.valueLogDictTrainer
	db.valueLogDictTrainerMu.Unlock()
	if tr != nil {
		snap := tr.Stats()
		stats["treedb.cache.vlog_dict.trainer.profile_attempts"] = fmt.Sprintf("%d", snap.ProfileAttempts)
		stats["treedb.cache.vlog_dict.trainer.profile_accepts"] = fmt.Sprintf("%d", snap.ProfileAccepts)
		stats["treedb.cache.vlog_dict.trainer.profile_rejects"] = fmt.Sprintf("%d", snap.ProfileRejects)
		stats["treedb.cache.vlog_dict.trainer.profile_reject_reason"] = snap.ProfileRejectReason
		if !snap.LastAcceptTimestamp.IsZero() {
			stats["treedb.cache.vlog_dict.trainer.last_accept_unix_nano"] = fmt.Sprintf("%d", snap.LastAcceptTimestamp.UnixNano())
		} else {
			stats["treedb.cache.vlog_dict.trainer.last_accept_unix_nano"] = "0"
		}
	}
	switch vlogAutotuneMode {
	case valuelog.AutotuneOff:
		stats["treedb.cache.vlog_compression_autotune.mode"] = "off"
	case valuelog.AutotuneMedium:
		stats["treedb.cache.vlog_compression_autotune.mode"] = "medium"
	case valuelog.AutotuneAggressive:
		stats["treedb.cache.vlog_compression_autotune.mode"] = "aggressive"
	default:
		stats["treedb.cache.vlog_compression_autotune.mode"] = fmt.Sprintf("%d", vlogAutotuneMode)
	}
	if snap := db.valueLogAutotuneMetrics.snapshot(); snap.hasData() {
		stats["treedb.cache.vlog_autotune.encode_ns_per_raw_byte"] = fmt.Sprintf("%.3f", snap.EncodeNsPerRawByte)
		stats["treedb.cache.vlog_autotune.io_ns_per_stored_byte"] = fmt.Sprintf("%.3f", snap.IoNsPerStoredByte)
		stats["treedb.cache.vlog_autotune.throughput_raw_MBps"] = fmt.Sprintf("%.3f", snap.ThroughputRawMBps)
		stats["treedb.cache.vlog_autotune.observed_ratio"] = fmt.Sprintf("%.6f", snap.ObservedRatio)
	}
	return stats
}

// TriggerFlush schedules a background flush pass (best-effort).
func (db *DB) TriggerFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

// QueueBacklogBytes returns the current queued memtable backlog in bytes.
func (db *DB) QueueBacklogBytes() int64 {
	return db.queueBacklogBytes.Load()
}

// CompactionAssist performs bounded flush work when backpressure triggers. It is
// intended to be called by background maintenance (e.g. index compaction) so that
// flush debt does not grow unbounded in the absence of foreground writes.
func (db *DB) CompactionAssist() {
	// Ensure the background flusher is scheduled even if this call ends up doing
	// no synchronous work (e.g. due to low backlog).
	db.TriggerFlush()

	// Adaptive policy: thresholds based on queued backlog bytes.
	if db.adaptiveBackpressureEnabled() {
		db.bpMu.Lock()
		slowdownBytes, stopBytes, _ := db.thresholdsLocked()
		db.bpMu.Unlock()

		backlog := db.queueBacklogBytes.Load()
		if stopBytes > 0 && backlog >= stopBytes {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if slowdownBytes > 0 && backlog > slowdownBytes {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		return
	}

	// Legacy policy: thresholds based on queue length.
	if db.maxQueuedMemtables >= 0 {
		db.mu.RLock()
		needs := len(db.queue) > db.maxQueuedMemtables
		db.mu.RUnlock()
		if needs {
			_ = db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
		}
	}
}

func (db *DB) Print() error {
	return db.backend.Print()
}

// Drain flushes all currently buffered writes (mutable + queued memtables) to the
// backend. It is intended for maintenance operations that require a fully
// materialized backend state (e.g. index vacuum).
//
// Drain does not provide mutual exclusion against concurrent writers; callers
// should ensure no writes occur concurrently if they require a fully drained
// state.
func (db *DB) Drain() error {
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()
	db.writeMu.Unlock()

	db.flushAll(false)
	return nil
}

// Iterator implements DB.Iterator
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	db.noteRead()
	if err := db.ensureBackendRange(); err != nil {
		return nil, err
	}
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}

	var view *memtableView
	db.mu.Lock()
	db.noteIterator(start, end)

	// Snapshot Isolation:
	// To ensure the iterator sees a consistent point-in-time view, we rotate the
	// mutable memtable into the immutable queue. The iterator then consumes
	// only the queue and the backend. Any subsequent writes will go to a new
	// mutable memtable which this iterator ignores.
	rotate := db.mutableBytes.Load() > 0
	if !rotate {
		for i := range db.mutableShards {
			mt := db.mutableShards[i].mem
			if mt != nil && mt.Len() != 0 {
				rotate = true
				break
			}
		}
	}
	if rotate {
		// Rotating is required for snapshot semantics, but allocating a large arena
		// for the *new* mutable memtable is often wasted (iterator-heavy paths may
		// not write concurrently). Use a small initial capacity and allow it to grow
		// if/when writes resume.
		if err := db.rotateMemtableLockedForIterator(minMemtablePrealloc); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}

	backendRangeKnown := db.backendRangeKnown
	backendRange := db.backendRange
	queueLenLocked := len(db.queue)
	if queueLenLocked > 0 {
		view = db.retainMemtableView()
	}

	db.mu.Unlock()

	// Backend-only fast path: with an empty immutable queue after rotation there
	// are no in-memory iterator sources to merge. Avoid memtable view retain/release
	// and construct a backend iterator directly.
	if queueLenLocked == 0 {
		decorate := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
			if iteratorDebugEnabled.Load() {
				it = &debugIterator{Iterator: it, queueLen: 0, sourcesUsed: sourcesUsed}
			}
			return db.wrapForegroundIterator(it)
		}
		if backendRangeKnown && !overlapsQuery(start, end, backendRange) {
			out := merging.Iterator(&emptyIterator{start: start, end: end})
			return decorate(out, 0), nil
		}
		if start == nil && end == nil && backendRangeKnown && backendRange.valid {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return decorate(diskIter, 1), nil
		}
		diskIter, err := db.backend.Iterator(start, end)
		if err != nil {
			return nil, err
		}
		return decorate(diskIter, 1), nil
	}

	releaseView := true
	defer func() {
		if releaseView && view != nil {
			db.releaseMemtableView(view)
		}
	}()
	var queue []memtable.Table
	var queueRanges []keyRange
	if view != nil {
		queue = view.queue
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open(), but keeps Iterator safe
		// for zero-value DBs and tests.
		db.mu.RLock()
		queue = append([]memtable.Table(nil), db.queue...)
		queueRanges = append([]keyRange(nil), db.queueRanges...)
		db.mu.RUnlock()
	}
	queueLen := len(queue)
	hasMemSource := false
	decorateIterator := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
		if iteratorDebugEnabled.Load() {
			it = &debugIterator{Iterator: it, queueLen: queueLen, sourcesUsed: sourcesUsed}
		}
		if hasMemSource && view != nil {
			leasedView := view
			view = nil
			releaseView = false
			return db.wrapForegroundIterator(&leasedMergingIterator{
				Iterator: it,
				release: func() {
					db.releaseMemtableView(leasedView)
				},
			})
		}
		if view != nil {
			db.releaseMemtableView(view)
			view = nil
			releaseView = false
		}
		return db.wrapForegroundIterator(it)
	}

	// Fast path for full scans: if the in-memory key ranges are disjoint from the
	// backend key range, we can concatenate iterators instead of merging.
	if start == nil && end == nil {
		// Only do this when the queue is empty; queued memtables imply the backend
		// might not yet include older keys, making disjoint-range checks unreliable.
		if backendRangeKnown && len(queue) == 0 && backendRange.valid {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return decorateIterator(diskIter, 1), nil
		}
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	// Note: We skip mutable shards because we just rotated them (so they're empty) or they were already empty.
	prio := 0
	for i := len(queue) - 1; i >= 0; i-- {
		if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
			prio++
			continue
		}
		qIter := queue[i].NewIterator(start, end)
		if db.memtableValueLogPointers {
			qIter = newValueLogIterator(qIter, func(key []byte, ptr page.ValuePtr) ([]byte, error) {
				return db.readValueLog(key, ptr)
			})
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     qIter,
			Priority: prio,
		})
		hasMemSource = true
		prio++
	}

	// Disk Iterator
	// Only skip if we definitively know the range and it doesn't overlap.
	if !backendRangeKnown || overlapsQuery(start, end, backendRange) {
		diskIter, err := db.backend.Iterator(start, end)
		if err != nil {
			for i := range sources {
				if sources[i].Iter != nil {
					_ = sources[i].Iter.Close()
				}
			}
			return nil, err
		}

		sources = append(sources, merging.IteratorSource{
			Iter:     diskIter,
			Priority: prio,
		})
	}

	if len(sources) == 0 {
		out := merging.Iterator(&emptyIterator{start: start, end: end})
		return decorateIterator(out, 0), nil
	}

	if len(sources) == 1 {
		out := newSingleSourceIterator(sources[0].Iter, start, end)
		return decorateIterator(out, 1), nil
	}

	out := merging.NewMergingIterator(sources, start, end)
	return decorateIterator(out, len(sources)), nil
}

type debugIterator struct {
	merging.Iterator
	queueLen    int
	sourcesUsed int
}

func (it *debugIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return it.queueLen, it.sourcesUsed
}

type leasedMergingIterator struct {
	merging.Iterator
	closeOnce sync.Once
	closeErr  error
	release   func()
}

func (it *leasedMergingIterator) Close() error {
	it.closeOnce.Do(func() {
		it.closeErr = it.Iterator.Close()
		if it.release != nil {
			it.release()
		}
	})
	return it.closeErr
}

type foregroundTrackedIterator struct {
	merging.Iterator
	db        *DB
	closeOnce sync.Once
	closeErr  error
}

func (it *foregroundTrackedIterator) Close() error {
	it.closeOnce.Do(func() {
		it.closeErr = it.Iterator.Close()
		if it.db != nil {
			it.db.activeForegroundIterators.Add(-1)
		}
	})
	return it.closeErr
}

func (db *DB) wrapForegroundIterator(it merging.Iterator) merging.Iterator {
	if db == nil || it == nil {
		return it
	}
	if tracked, ok := it.(*foregroundTrackedIterator); ok {
		return tracked
	}
	db.activeForegroundIterators.Add(1)
	return &foregroundTrackedIterator{Iterator: it, db: db}
}

type concatUnsafeIterator struct {
	first  iterator.UnsafeIterator
	second iterator.UnsafeIterator

	cur        iterator.UnsafeIterator
	usingFirst bool
	valid      bool
	err        error
}

func newConcatUnsafeIterator(first, second iterator.UnsafeIterator) merging.Iterator {
	it := &concatUnsafeIterator{
		first:      first,
		second:     second,
		cur:        first,
		usingFirst: true,
	}
	it.advance()
	return it
}

func (it *concatUnsafeIterator) advance() {
	it.valid = false

	for {
		if it.cur == nil {
			return
		}

		// Switch to second iterator when first is exhausted.
		if !it.cur.Valid() {
			if it.usingFirst {
				it.cur = it.second
				it.usingFirst = false
				continue
			}
			return
		}

		if it.cur.IsDeleted() {
			it.cur.Next()
			continue
		}

		it.valid = true
		return
	}
}

func (it *concatUnsafeIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.cur.Next()
	it.advance()
}

func (it *concatUnsafeIterator) Valid() bool { return it.valid }

func (it *concatUnsafeIterator) Key() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Key()
}

func (it *concatUnsafeIterator) Value() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Value()
}

func (it *concatUnsafeIterator) KeyCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.KeyCopy(dst)
}

func (it *concatUnsafeIterator) ValueCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.ValueCopy(dst)
}

func (it *concatUnsafeIterator) Close() error {
	var firstErr error
	if it.first != nil {
		if err := it.first.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if it.second != nil {
		if err := it.second.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (it *concatUnsafeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.first != nil && it.first.Error() != nil {
		return it.first.Error()
	}
	if it.second != nil && it.second.Error() != nil {
		return it.second.Error()
	}
	return nil
}

func (it *concatUnsafeIterator) Domain() (start, end []byte) { return nil, nil }

func (db *DB) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	db.noteRead()
	if err := db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}

	var view *memtableView
	db.mu.Lock()
	db.noteIterator(start, end)

	// Snapshot Isolation:
	// Mirror Iterator() semantics: rotate mutable memtables into the immutable
	// queue so the reverse iterator sees a stable point-in-time view (queue +
	// backend). Subsequent writes land in a new mutable memtable and are ignored.
	rotate := db.mutableBytes.Load() > 0
	if !rotate {
		for i := range db.mutableShards {
			mt := db.mutableShards[i].mem
			if mt != nil && mt.Len() != 0 {
				rotate = true
				break
			}
		}
	}
	if rotate {
		if err := db.rotateMemtableLockedForIterator(minMemtablePrealloc); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}

	queueLenLocked := len(db.queue)
	if queueLenLocked > 0 {
		view = db.retainMemtableView()
	}

	db.mu.Unlock()

	if err := db.ensureBackendRange(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	backendRangeKnown := db.backendRangeKnown
	backendRange := db.backendRange
	db.mu.RUnlock()

	// Backend-only fast path.
	if queueLenLocked == 0 {
		decorate := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
			if iteratorDebugEnabled.Load() {
				it = &debugIterator{Iterator: it, queueLen: 0, sourcesUsed: sourcesUsed}
			}
			return db.wrapForegroundIterator(it)
		}
		if backendRangeKnown && !overlapsQuery(start, end, backendRange) {
			out := merging.Iterator(&emptyIterator{start: start, end: end})
			return decorate(out, 0), nil
		}
		if start == nil && end == nil && backendRangeKnown && backendRange.valid {
			diskIter, err := db.backend.ReverseIterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return decorate(diskIter, 1), nil
		}
		diskIter, err := db.backend.ReverseIterator(start, end)
		if err != nil {
			return nil, err
		}
		return decorate(diskIter, 1), nil
	}

	releaseView := true
	defer func() {
		if releaseView && view != nil {
			db.releaseMemtableView(view)
		}
	}()
	var queue []memtable.Table
	var queueRanges []keyRange
	if view != nil {
		queue = view.queue
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open().
		db.mu.RLock()
		queue = append([]memtable.Table(nil), db.queue...)
		queueRanges = append([]keyRange(nil), db.queueRanges...)
		db.mu.RUnlock()
	}
	queueLen := len(queue)
	hasMemSource := false
	decorateIterator := func(it merging.Iterator, sourcesUsed int) merging.Iterator {
		if iteratorDebugEnabled.Load() {
			it = &debugIterator{Iterator: it, queueLen: queueLen, sourcesUsed: sourcesUsed}
		}
		if hasMemSource && view != nil {
			leasedView := view
			view = nil
			releaseView = false
			return db.wrapForegroundIterator(&leasedMergingIterator{
				Iterator: it,
				release: func() {
					db.releaseMemtableView(leasedView)
				},
			})
		}
		if view != nil {
			db.releaseMemtableView(view)
			view = nil
			releaseView = false
		}
		return db.wrapForegroundIterator(it)
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	prio := 0
	for i := len(queue) - 1; i >= 0; i-- {
		if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
			prio++
			continue
		}
		qIter := queue[i].NewReverseIterator(start, end)
		if db.memtableValueLogPointers {
			qIter = newValueLogIterator(qIter, func(key []byte, ptr page.ValuePtr) ([]byte, error) {
				return db.readValueLog(key, ptr)
			})
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     qIter,
			Priority: prio,
		})
		hasMemSource = true
		prio++
	}

	// Disk Iterator
	if !backendRangeKnown || overlapsQuery(start, end, backendRange) {
		diskIter, err := db.backend.ReverseIterator(start, end)
		if err != nil {
			for i := range sources {
				if sources[i].Iter != nil {
					_ = sources[i].Iter.Close()
				}
			}
			return nil, err
		}

		sources = append(sources, merging.IteratorSource{
			Iter:     diskIter,
			Priority: prio,
		})
	}

	if len(sources) == 0 {
		out := merging.Iterator(&emptyIterator{start: start, end: end})
		return decorateIterator(out, 0), nil
	}

	if len(sources) == 1 {
		out := newSingleSourceIterator(sources[0].Iter, start, end)
		return decorateIterator(out, 1), nil
	}

	out := merging.NewReverseMergingIterator(sources, start, end)
	return decorateIterator(out, len(sources)), nil
}

// NewBatch implementation for CachingDB
// batchOp removed, using batch.Entry directly

type Batch struct {
	db                 *DB
	entries            []batch.Entry
	backend            batch.Interface
	size               int
	copyArena          []byte
	copyArenaChunks    [][]byte
	copyArenaCap       int
	copyBytes          int
	ptrCopyArena       []byte
	ptrCopyArenaChunks [][]byte
	ptrCopyBytes       int
	arenaInFlightBytes int64
	ptrValueIdxs       []int
	walBuf             []logRecord
	shardIdxs          []int
	eligibleIdxs       []int
	shardAdds          []int64
	shardCnts          []int
	shardEntries       [][]batch.Entry
	shardIdxSets       [][]int
	maxEntries         int

	closed         bool
	streamEligible bool
	streamTried    bool
	firstKey       []byte
	lastKey        []byte
	batchRange     keyRange
	dictID         uint64
	dictIDValid    bool
	dictBytes      []byte
	dictBytesValid bool
}

func (db *DB) NewBatch() *Batch {
	capHint := batchDefaultEntriesCap
	if db != nil {
		capHint = db.batchEntriesCapHint(capHint)
	}
	return &Batch{
		db:             db,
		entries:        db.getBatchEntries(capHint),
		copyArenaCap:   db.batchCopyArenaInitCap(0),
		streamEligible: true,
	}
}

func (db *DB) NewBatchWithSize(size int) *Batch {
	reserveHint := backenddb.NormalizePublicBatchReserveHint(size)
	return &Batch{
		db:             db,
		entries:        db.getBatchEntries(reserveHint),
		copyArenaCap:   db.batchCopyArenaInitCap(reserveHint),
		streamEligible: true,
	}
}

func clampAppendOnlyEntryHint(entries int) int {
	if entries < appendOnlyEntryHintMinEntries {
		return appendOnlyEntryHintMinEntries
	}
	if entries > appendOnlyEntryHintMaxEntries {
		return appendOnlyEntryHintMaxEntries
	}
	return entries
}

func (db *DB) observeAppendOnlyMutableEntries(entries int) {
	if db == nil {
		return
	}
	n := clampAppendOnlyEntryHint(entries)
	for {
		old := int(db.appendOnlyEntryHint.Load())
		next := n
		if old > 0 {
			if n > old {
				// Grow faster so sustained larger mutables avoid repeated regrowth.
				next = (old*3 + n + 1) / 4
			} else {
				// Decay to recent smaller mutables to avoid pinning high-water entry
				// slices through rotate/checkpoint-heavy workloads.
				next = (old*7 + n + 3) / 8
			}
		}
		next = clampAppendOnlyEntryHint(next)
		if next == old {
			return
		}
		if db.appendOnlyEntryHint.CompareAndSwap(int32(old), int32(next)) {
			return
		}
	}
}

func appendOnlyEntriesToCapacity(entries, estimatedBytesPerEntry int) int {
	if entries <= 0 {
		return 0
	}
	if estimatedBytesPerEntry <= 0 {
		estimatedBytesPerEntry = appendOnlyEstimatedBytesPerEntryDefault
	}
	maxInt := int(^uint(0) >> 1)
	if entries > maxInt/estimatedBytesPerEntry {
		return maxInt
	}
	return entries * estimatedBytesPerEntry
}

func (db *DB) appendOnlyMemtableCapacityHint(capacity, estimatedBytesPerEntry int) int {
	if db == nil || capacity <= 0 {
		return capacity
	}
	// Keep append-only preallocation aligned with the *effective* mutable flush
	// threshold under process pressure. Without this, we can still preallocate
	// near static memtableCap even after pressure logic has lowered rotation
	// thresholds, inflating peak RSS during restore workloads.
	if threshold := db.mutableFlushThreshold(); threshold > 0 {
		if effectiveCap := memtableCapacity(threshold); effectiveCap > 0 && effectiveCap < capacity {
			capacity = effectiveCap
		}
	}
	hintEntries := int(db.appendOnlyEntryHint.Load())
	if hintEntries <= 0 {
		return capacity
	}
	hintEntries = clampAppendOnlyEntryHint(hintEntries)
	hintCapacity := appendOnlyEntriesToCapacity(hintEntries, estimatedBytesPerEntry)
	if hintCapacity < minMemtablePrealloc {
		hintCapacity = minMemtablePrealloc
	}
	if hintCapacity > capacity {
		hintCapacity = capacity
	}
	if hintCapacity <= 0 {
		return capacity
	}
	return hintCapacity
}

func (db *DB) batchEntriesCapHint(minCap int) int {
	if minCap < batchDefaultEntriesCap {
		minCap = batchDefaultEntriesCap
	}
	if db == nil {
		return minCap
	}
	hint := int(db.batchEntryHint.Load())
	if hint > minCap {
		minCap = hint
	}
	if minCap > batchHintEntriesMax {
		minCap = batchHintEntriesMax
	}
	return minCap
}

func (db *DB) observeBatchEntries(n int) {
	if db == nil || n <= 0 {
		return
	}
	if n < batchDefaultEntriesCap {
		n = batchDefaultEntriesCap
	}
	if n > batchHintEntriesMax {
		n = batchHintEntriesMax
	}
	for {
		old := int(db.batchEntryHint.Load())
		next := n
		if old > 0 {
			// EWMA keeps the hint adaptive: rises quickly for larger batches and
			// decays after sustained smaller batches.
			next = (old*7 + n + 3) / 8
		}
		if next < batchDefaultEntriesCap {
			next = batchDefaultEntriesCap
		}
		if next > batchHintEntriesMax {
			next = batchHintEntriesMax
		}
		if next == old {
			return
		}
		if db.batchEntryHint.CompareAndSwap(int32(old), int32(next)) {
			return
		}
	}
}

func (db *DB) batchCopyArenaInitCap(sizeHint int) int {
	base := batchCopyArenaInitCapForEntries(sizeHint)
	if db == nil {
		return base
	}
	if hint := int(db.batchCopyBytesHint.Load()); hint > base {
		base = hint
	}
	if base < batchCopyArenaMinChunk {
		base = batchCopyArenaMinChunk
	}
	if base > batchCopyArenaInitMax {
		base = batchCopyArenaInitMax
	}
	if maxChunk := currentBatchCopyArenaMaxChunk(); maxChunk > 0 && base > maxChunk {
		base = maxChunk
	}
	return base
}

func (db *DB) observeBatchCopyBytes(n int) {
	if db == nil || n <= 0 {
		return
	}
	if n < batchCopyArenaMinChunk {
		n = batchCopyArenaMinChunk
	}
	if n > batchCopyArenaInitMax {
		n = batchCopyArenaInitMax
	}
	if maxChunk := currentBatchCopyArenaMaxChunk(); maxChunk > 0 && n > maxChunk {
		n = maxChunk
	}
	for {
		old := int(db.batchCopyBytesHint.Load())
		next := n
		if old > 0 {
			if n > old {
				// Increase quickly so large batches avoid repeated growth.
				next = (old*3 + n + 1) / 4
			} else {
				// Decay quickly so high-water marks do not poison small batches.
				next = (old + n + 1) / 2
			}
		}
		if next < batchCopyArenaMinChunk {
			next = batchCopyArenaMinChunk
		}
		if next > batchCopyArenaInitMax {
			next = batchCopyArenaInitMax
		}
		if next == old {
			return
		}
		if db.batchCopyBytesHint.CompareAndSwap(int32(old), int32(next)) {
			return
		}
	}
}

func (db *DB) noteBatchArenaChunkAlloc(requestedCap, classCap int) {
	if db == nil {
		return
	}
	if requestedCap > 0 {
		db.batchArenaAllocRequestedBytes.Add(uint64(requestedCap))
	}
	if classCap > 0 {
		db.batchArenaAllocClassBytes.Add(uint64(classCap))
	}
}

func (db *DB) noteBatchArenaChunkFinalize(used, classCap int) {
	if db == nil || classCap <= 0 {
		return
	}
	if used < 0 {
		used = 0
	}
	if used > classCap {
		used = classCap
	}
	db.batchArenaUsedBytes.Add(uint64(used))
	db.batchArenaTailWasteBytes.Add(uint64(classCap - used))
}

func (db *DB) noteBatchArenaTailCompact(copiedBytes, classCap int) {
	if db == nil || classCap <= 0 {
		return
	}
	if copiedBytes < 0 {
		copiedBytes = 0
	}
	if copiedBytes > classCap {
		copiedBytes = classCap
	}
	saved := classCap - copiedBytes
	if saved < 0 {
		saved = 0
	}
	db.batchArenaTailCompactRuns.Add(1)
	if copiedBytes > 0 {
		db.batchArenaTailCompactCopied.Add(uint64(copiedBytes))
	}
	if saved > 0 {
		db.batchArenaTailCompactSaved.Add(uint64(saved))
	}
}

func (db *DB) getBatchEntries(minCap int) []batch.Entry {
	if minCap < 0 {
		minCap = 0
	}
	if db != nil {
		if pooled := db.batchEntriesPool.Get(); pooled != nil {
			switch v := pooled.(type) {
			case *batchEntrySliceRef:
				entries := v.entries
				putBatchEntrySliceRef(v)
				if cap(entries) >= minCap {
					return entries[:0]
				}
				if c := cap(entries); c > 0 && c <= batchEntriesPoolMaxRetain {
					db.batchEntriesPool.Put(getBatchEntrySliceRef(entries[:0]))
				}
			case []batch.Entry:
				// Backward-compatible fallback for any legacy pooled shape.
				if cap(v) >= minCap {
					return v[:0]
				}
				if c := cap(v); c > 0 && c <= batchEntriesPoolMaxRetain {
					db.batchEntriesPool.Put(getBatchEntrySliceRef(v[:0]))
				}
			}
		}
	}
	return make([]batch.Entry, 0, minCap)
}

func (db *DB) putBatchEntries(entries []batch.Entry) {
	if db == nil || cap(entries) == 0 {
		return
	}
	if cap(entries) > batchEntriesPoolMaxRetain {
		return
	}
	if !shouldRetainBatchAuxPoolEntries(currentPoolPressureSnapshot().level) {
		batchEntriesPoolDropUnderPressureTotal.Add(1)
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	db.batchEntriesPool.Put(getBatchEntrySliceRef(full[:0]))
}

func (db *DB) getBatchShardEntries(minCap int) []batch.Entry {
	if minCap < 0 {
		minCap = 0
	}
	if db != nil {
		if pooled := db.batchShardEntriesPool.Get(); pooled != nil {
			switch v := pooled.(type) {
			case *batchEntrySliceRef:
				entries := v.entries
				putBatchEntrySliceRef(v)
				if cap(entries) >= minCap {
					return entries[:0]
				}
			case []batch.Entry:
				// Backward-compatible fallback for any legacy pooled shape.
				if cap(v) >= minCap {
					return v[:0]
				}
			}
		}
	}
	return make([]batch.Entry, 0, minCap)
}

func (db *DB) putBatchShardEntries(entries []batch.Entry) {
	if db == nil || cap(entries) == 0 {
		return
	}
	if cap(entries) > batchEntriesPoolMaxRetain {
		return
	}
	if !shouldRetainBatchAuxPoolEntries(currentPoolPressureSnapshot().level) {
		batchShardEntriesPoolDropUnderPressureTotal.Add(1)
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	db.batchShardEntriesPool.Put(getBatchEntrySliceRef(full[:0]))
}

func (db *DB) getBatchIntSlice(minCap int) []int {
	if minCap < 0 {
		minCap = 0
	}
	if db != nil {
		if pooled := db.batchIntPool.Get(); pooled != nil {
			if idxs, ok := pooled.([]int); ok {
				if cap(idxs) >= minCap {
					return idxs[:0]
				}
				if c := cap(idxs); c > 0 && c <= batchIntSlicePoolMaxRetain {
					db.batchIntPool.Put(idxs[:0])
				}
			}
		}
	}
	return make([]int, 0, minCap)
}

func (db *DB) putBatchIntSlice(idxs []int) {
	if db == nil || cap(idxs) == 0 {
		return
	}
	if cap(idxs) > batchIntSlicePoolMaxRetain {
		return
	}
	if !shouldRetainBatchAuxPoolEntries(currentPoolPressureSnapshot().level) {
		batchIntPoolDropUnderPressureTotal.Add(1)
		return
	}
	db.batchIntPool.Put(idxs[:0])
}

// Reset clears the batch for reuse without closing it.
//
// This intentionally keeps internal buffers to avoid per-batch allocations in
// callers that frequently reset (e.g. geth benchmarks).
func (b *Batch) Reset() {
	if b == nil {
		return
	}
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	if b.entries != nil {
		b.entries = b.entries[:0]
	}
	if b.shardIdxs != nil {
		b.shardIdxs = b.shardIdxs[:0]
	}
	if b.shardAdds != nil {
		b.shardAdds = b.shardAdds[:0]
	}
	if b.eligibleIdxs != nil {
		b.eligibleIdxs = b.eligibleIdxs[:0]
	}
	if b.shardCnts != nil {
		b.shardCnts = b.shardCnts[:0]
	}
	if b.shardEntries != nil {
		b.shardEntries = b.shardEntries[:0]
	}
	if b.shardIdxSets != nil {
		b.shardIdxSets = b.shardIdxSets[:0]
	}
	b.recycleCopyArenaChunks()
	b.recyclePtrCopyArenaChunks()
	if b.arenaInFlightBytes > 0 {
		b.subArenaInFlightBytes(b.arenaInFlightBytes)
	}
	if b.db != nil {
		b.copyArenaCap = b.db.batchCopyArenaInitCap(0)
	}
	b.size = 0
	b.walBuf = b.walBuf[:0]
	b.streamEligible = true
	b.streamTried = false
	b.firstKey = nil
	b.lastKey = nil
	b.batchRange = keyRange{}
	b.dictID = 0
	b.dictIDValid = false
	b.dictBytes = nil
	b.dictBytesValid = false
	b.maxEntries = 0
	if b.ptrValueIdxs != nil {
		b.ptrValueIdxs = b.ptrValueIdxs[:0]
	}
}

func (b *Batch) noteEntryAppend() {
	if b == nil {
		return
	}
	if n := len(b.entries); n > b.maxEntries {
		b.maxEntries = n
	}
}

func (b *Batch) updateBatchEntryHint() {
	if b == nil || b.db == nil {
		return
	}
	n := b.maxEntries
	if n <= 0 {
		n = len(b.entries)
	}
	if n > 0 {
		b.db.observeBatchEntries(n)
	}
}

func (b *Batch) drainCopyArenaChunks() [][]byte {
	if b == nil {
		return nil
	}
	if b.db != nil && cap(b.copyArena) > 0 {
		b.db.noteBatchArenaChunkFinalize(len(b.copyArena), cap(b.copyArena))
	}
	chunks := b.copyArenaChunks
	if bytes := batchArenaChunksCapBytes(chunks); bytes > 0 {
		b.subArenaInFlightBytes(bytes)
	}
	b.copyArenaChunks = nil
	b.copyArena = nil
	b.copyBytes = 0
	return chunks
}

func (b *Batch) recycleCopyArenaChunks() {
	if b == nil {
		return
	}
	chunks := b.drainCopyArenaChunks()
	if len(chunks) == 0 {
		return
	}
	putBatchArenas(chunks)
}

func (b *Batch) drainPtrCopyArenaChunks() [][]byte {
	if b == nil {
		return nil
	}
	if b.db != nil && cap(b.ptrCopyArena) > 0 {
		b.db.noteBatchArenaChunkFinalize(len(b.ptrCopyArena), cap(b.ptrCopyArena))
	}
	chunks := b.ptrCopyArenaChunks
	if bytes := batchArenaChunksCapBytes(chunks); bytes > 0 {
		b.subArenaInFlightBytes(bytes)
	}
	b.ptrCopyArenaChunks = nil
	b.ptrCopyArena = nil
	b.ptrCopyBytes = 0
	return chunks
}

func (b *Batch) recyclePtrCopyArenaChunks() {
	if b == nil {
		return
	}
	chunks := b.drainPtrCopyArenaChunks()
	if len(chunks) == 0 {
		return
	}
	putBatchArenas(chunks)
}

func (b *Batch) updateBatchCopyHint() {
	if b == nil || b.db == nil {
		return
	}
	if n := b.copyBytes; n > 0 {
		b.db.observeBatchCopyBytes(n)
	}
}

func (b *Batch) addArenaInFlightBytes(n int) {
	if b == nil || n <= 0 {
		return
	}
	b.arenaInFlightBytes += int64(n)
	cur := batchArenaInFlightBytes.Add(int64(n))
	noteBatchArenaInFlightBytesMax(cur)
}

func (b *Batch) subArenaInFlightBytes(n int64) {
	if b == nil || n <= 0 {
		return
	}
	if n > b.arenaInFlightBytes {
		n = b.arenaInFlightBytes
	}
	if n <= 0 {
		return
	}
	b.arenaInFlightBytes -= n
	if next := batchArenaInFlightBytes.Add(-n); next < 0 {
		batchArenaInFlightBytes.Store(0)
	}
}

func (b *Batch) arenaCopy(n int) []byte {
	return b.arenaCopyInto(&b.copyArena, &b.copyArenaChunks, &b.copyBytes, n, b.copyArenaCap)
}

func (b *Batch) arenaCopyPtr(n int) []byte {
	return b.arenaCopyInto(&b.ptrCopyArena, &b.ptrCopyArenaChunks, &b.ptrCopyBytes, n, 0)
}

func (b *Batch) arenaCopyInto(arena *[]byte, chunks *[][]byte, copyBytes *int, n int, initialCap int) []byte {
	if n == 0 {
		return nil
	}
	if cap(*arena)-len(*arena) < n {
		if b != nil && b.db != nil && cap(*arena) > 0 {
			b.db.noteBatchArenaChunkFinalize(len(*arena), cap(*arena))
		}
		chunkCap := cap(*arena) * 2
		if chunkCap < batchCopyArenaMinChunk {
			chunkCap = batchCopyArenaMinChunk
		}
		if cap(*arena) == 0 {
			// Keep unsized batches conservative. Entry-slice capacity can come from
			// pooled high-water marks and is not a reliable signal for copy payload.
			if initialCap > chunkCap {
				chunkCap = initialCap
			}
		}
		if chunkCap < n {
			chunkCap = n
		}
		// Avoid unbounded exponential growth once we reach the pooling limit. Large
		// restore batches can otherwise allocate a huge tail chunk (e.g. 8/16/32MB)
		// that is mostly unused, inflating RSS. Only allow larger chunks when a
		// *single* copy needs it.
		if n <= batchCopyArenaMaxRetain && chunkCap > batchCopyArenaMaxRetain {
			chunkCap = batchCopyArenaMaxRetain
		}
		if maxChunk := currentBatchCopyArenaMaxChunk(); maxChunk > 0 && chunkCap > maxChunk && n <= maxChunk {
			chunkCap = maxChunk
		}
		// Switch to a fresh chunk when exhausted so existing entry slices keep
		// their backing arrays without per-op allocations.
		chunk := getBatchArena(chunkCap)
		if cap(chunk) > 0 {
			b.addArenaInFlightBytes(cap(chunk))
		}
		if b != nil && b.db != nil {
			b.db.noteBatchArenaChunkAlloc(chunkCap, cap(chunk))
		}
		*arena = chunk[:0]
		*chunks = append(*chunks, *arena)
	}
	start := len(*arena)
	*arena = (*arena)[:start+n]
	*copyBytes += n
	return (*arena)[start : start+n : start+n]
}

func (b *Batch) cloneKey(key []byte) []byte {
	dst := b.arenaCopy(len(key))
	copy(dst, key)
	return dst
}

func (b *Batch) cloneValue(value []byte) []byte {
	dst := b.arenaCopy(len(value))
	copy(dst, value)
	return dst
}

func (b *Batch) cloneKeyValue(key, value []byte) ([]byte, []byte) {
	buf := b.arenaCopy(len(key) + len(value))
	keyCopy := buf[:len(key):len(key)]
	copy(keyCopy, key)
	valCopy := buf[len(key):]
	copy(valCopy, value)
	return keyCopy, valCopy
}

func (b *Batch) clonePtrValue(value []byte) []byte {
	dst := b.arenaCopyPtr(len(value))
	copy(dst, value)
	return dst
}

func shouldCompactBatchArenaTailWithPolicy(used, classCap, minWaste, maxFillNumerator, maxFillDenominator int) bool {
	if classCap < batchArenaTailCompactMinCap || used <= 0 || used >= classCap {
		return false
	}
	waste := classCap - used
	if waste < minWaste {
		return false
	}
	return used*maxFillDenominator <= classCap*maxFillNumerator
}

func shouldCompactBatchArenaTail(used, classCap int) bool {
	return shouldCompactBatchArenaTailWithPolicy(
		used,
		classCap,
		batchArenaTailCompactMinWaste,
		batchArenaTailCompactMaxFillNumerator,
		batchArenaTailCompactMaxFillDenominator,
	)
}

func (b *Batch) shouldCompactArenaTail(used, classCap int) bool {
	minWaste := batchArenaTailCompactMinWaste
	maxFillNumerator := batchArenaTailCompactMaxFillNumerator
	maxFillDenominator := batchArenaTailCompactMaxFillDenominator
	if b != nil && b.db != nil && b.db.memtableViewTelemetry.deferredViewsCurrent.Load() > 0 {
		if minWaste > batchArenaTailCompactPinnedMinWaste {
			minWaste = batchArenaTailCompactPinnedMinWaste
		}
		maxFillNumerator = batchArenaTailCompactPinnedMaxFillNumerator
		maxFillDenominator = batchArenaTailCompactPinnedMaxFillDenominator
	}
	return shouldCompactBatchArenaTailWithPolicy(used, classCap, minWaste, maxFillNumerator, maxFillDenominator)
}

func sliceAliasesArenaTail(s, tail []byte) bool {
	if len(s) == 0 || len(tail) == 0 {
		return false
	}
	sPtr := uintptr(unsafe.Pointer(unsafe.SliceData(s)))
	tPtr := uintptr(unsafe.Pointer(unsafe.SliceData(tail)))
	if sPtr < tPtr {
		return false
	}
	offset := sPtr - tPtr
	if offset > uintptr(len(tail)) {
		return false
	}
	return uintptr(len(s)) <= uintptr(len(tail))-offset
}

func (b *Batch) releaseCompactedBatchArenaTail(chunks *[][]byte, arena *[]byte, copiedBytes int) {
	if b == nil || chunks == nil || arena == nil || len(*chunks) == 0 || cap(*arena) == 0 {
		return
	}
	used := len(*arena)
	classCap := cap(*arena)
	last := len(*chunks) - 1
	tailChunk := (*chunks)[last]
	if tailChunk != nil {
		b.subArenaInFlightBytes(int64(cap(tailChunk)))
		putBatchArena(tailChunk)
	}
	(*chunks)[last] = nil
	*chunks = (*chunks)[:last]
	if b.db != nil {
		b.db.noteBatchArenaChunkFinalize(used, classCap)
		b.db.noteBatchArenaTailCompact(copiedBytes, classCap)
	}
	*arena = nil
}

func (b *Batch) compactUnderfilledMainArenaTail() {
	if b == nil || len(b.entries) == 0 || cap(b.copyArena) == 0 || len(b.copyArenaChunks) == 0 {
		return
	}
	tail := b.copyArena
	if !b.shouldCompactArenaTail(len(tail), cap(tail)) {
		return
	}
	copiedBytes := 0
	for i := range b.entries {
		op := &b.entries[i]
		if len(op.Key) > 0 && sliceAliasesArenaTail(op.Key, tail) {
			op.Key = bytes.Clone(op.Key)
			copiedBytes += len(op.Key)
		}
		if len(op.Value) > 0 && sliceAliasesArenaTail(op.Value, tail) {
			op.Value = bytes.Clone(op.Value)
			copiedBytes += len(op.Value)
		}
	}
	if copiedBytes == 0 {
		return
	}
	b.releaseCompactedBatchArenaTail(&b.copyArenaChunks, &b.copyArena, copiedBytes)
}

func (b *Batch) compactUnderfilledPtrArenaTail() {
	if b == nil || len(b.entries) == 0 || cap(b.ptrCopyArena) == 0 || len(b.ptrCopyArenaChunks) == 0 {
		return
	}
	tail := b.ptrCopyArena
	if !b.shouldCompactArenaTail(len(tail), cap(tail)) {
		return
	}
	copiedBytes := 0
	for i := range b.entries {
		op := &b.entries[i]
		if len(op.Value) == 0 || !sliceAliasesArenaTail(op.Value, tail) {
			continue
		}
		op.Value = bytes.Clone(op.Value)
		copiedBytes += len(op.Value)
	}
	if copiedBytes == 0 {
		return
	}
	b.releaseCompactedBatchArenaTail(&b.ptrCopyArenaChunks, &b.ptrCopyArena, copiedBytes)
}

func (b *Batch) maybeCompactUnderfilledArenaTails() {
	b.compactUnderfilledMainArenaTail()
	b.compactUnderfilledPtrArenaTail()
}

func (b *Batch) shouldCopyValueToPtrArena(key, value []byte) bool {
	if b == nil || b.db == nil || b.backend != nil {
		return false
	}
	// Only split when pointer-in-memtable mode is enabled, meaning the inline
	// value bytes will be discarded once the value-log pointer is assigned.
	if !b.db.memtableValueLogPointers || !b.db.valueLogEnabled() {
		return false
	}
	return b.db.shouldWriteViaValueLogForKeyValue(key, value)
}

func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	idx := len(b.entries)
	var keyCopy, valCopy []byte
	if b.shouldCopyValueToPtrArena(key, value) {
		keyCopy = b.cloneKey(key)
		valCopy = b.clonePtrValue(value)
		b.ptrValueIdxs = append(b.ptrValueIdxs, idx)
	} else {
		keyCopy, valCopy = b.cloneKeyValue(key, value)
	}
	if b.backend != nil {
		b.batchRange.add(keyCopy)
		b.size += len(keyCopy) + len(valCopy)
		// Use the backend's view method with owned copies to avoid aliasing.
		if sv, ok := b.backend.(interface{ SetView(key, value []byte) error }); ok {
			return sv.SetView(keyCopy, valCopy)
		}
		return b.backend.Set(keyCopy, valCopy)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = keyCopy
			b.lastKey = keyCopy
		} else {
			if bytes.Compare(keyCopy, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = keyCopy
		}
	}
	// We don't know about value-log thresholds here, so we just store inline.
	// The backend will handle promotion to the value log if needed during
	// writeBypass, or standard write will handle it via the journal/memtable.
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   keyCopy,
		Value: valCopy,
	})
	b.noteEntryAppend()
	b.size += len(keyCopy) + len(valCopy)

	b.maybeSwitchToStreaming()
	return nil
}

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
func (b *Batch) SetView(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	if b.backend != nil {
		b.batchRange.add(key)
		b.size += len(key) + len(value)
		if sv, ok := b.backend.(interface{ SetView(key, value []byte) error }); ok {
			return sv.SetView(key, value)
		}
		return b.backend.Set(key, value)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = key
			b.lastKey = key
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = key
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	})
	b.noteEntryAppend()
	b.size += len(key) + len(value)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	keyCopy := b.cloneKey(key)
	if b.backend != nil {
		b.batchRange.add(keyCopy)
		b.size += len(keyCopy)
		if dv, ok := b.backend.(interface{ DeleteView(key []byte) error }); ok {
			return dv.DeleteView(keyCopy)
		}
		return b.backend.Delete(keyCopy)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = keyCopy
			b.lastKey = keyCopy
		} else {
			if bytes.Compare(keyCopy, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = keyCopy
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  keyCopy,
	})
	b.noteEntryAppend()
	b.size += len(keyCopy)

	b.maybeSwitchToStreaming()
	return nil
}

// DeleteView records a Delete without copying key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	if b.backend != nil {
		b.batchRange.add(key)
		b.size += len(key)
		if dv, ok := b.backend.(interface{ DeleteView(key []byte) error }); ok {
			return dv.DeleteView(key)
		}
		return b.backend.Delete(key)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = key
			b.lastKey = key
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = key
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	})
	b.noteEntryAppend()
	b.size += len(key)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		copied := make([]batch.Entry, len(ops))
		for i, op := range ops {
			copiedOp := op
			copiedOp.Key = b.cloneKey(op.Key)
			if op.Value != nil {
				copiedOp.Value = b.cloneValue(op.Value)
			}
			copied[i] = copiedOp
			b.size += len(copiedOp.Key) + len(copiedOp.Value)
			b.batchRange.add(copiedOp.Key)
		}
		return b.backend.SetOps(copied)
	}
	for _, op := range ops {
		copied := op
		if op.Value != nil {
			copied.Key, copied.Value = b.cloneKeyValue(op.Key, op.Value)
		} else {
			copied.Key = b.cloneKey(op.Key)
			copied.Value = nil
		}
		if b.streamEligible {
			if b.firstKey == nil {
				b.firstKey = copied.Key
				b.lastKey = copied.Key
			} else {
				if bytes.Compare(copied.Key, b.lastKey) <= 0 {
					b.streamEligible = false
				}
				b.lastKey = copied.Key
			}
		}
		b.entries = append(b.entries, copied)
		b.noteEntryAppend()
		b.size += len(copied.Key) + len(copied.Value)
	}
	return nil
}

const (
	batchDefaultEntriesCap = 16
	// Keep the adaptive NewBatch reserve bounded so one huge batch cannot
	// permanently over-provision typical batch allocations.
	batchHintEntriesMax    = 8192
	streamSwitchMinEntries = 4096
	streamSwitchMinBytes   = 1 << 20 // 1MiB
	// Only fan out value-log appends across multiple lanes when a batch is large
	// enough to amortize per-lane setup and goroutine overhead.
	multiLaneValueLogMinRecords = 1024
	batchCopyArenaMinChunk      = 4 << 10
	batchCopyArenaUnsizedInit   = 8 << 10
	batchCopyArenaBytesPerEntry = 192
	batchCopyArenaInitMax       = 2 << 20
	// Keep retained batch-copy chunks bounded to 1MiB. Larger chunks are often
	// underfilled in restore-heavy traffic and can disproportionately inflate
	// peak RSS when multiple memtable views pin retired arenas.
	batchCopyArenaMaxRetain     = 1 << 20
	batchArenaTailCompactMinCap = 256 << 10
	// Only compact tails with meaningful waste so we avoid churn on tiny chunks.
	batchArenaTailCompactMinWaste = 256 << 10
	// Under deferred-view pressure, compact earlier so retired memtable leases
	// don't pin large half-used tail chunks for iterator lifetimes.
	batchArenaTailCompactPinnedMinWaste = 128 << 10
	// Require at least 50% slack in the tail chunk before compacting borrowed
	// slices. This keeps the path focused on pathological underfill.
	batchArenaTailCompactMaxFillNumerator   = 1
	batchArenaTailCompactMaxFillDenominator = 2
	// Under deferred-view pressure, compact tails once fill is at most 75%.
	batchArenaTailCompactPinnedMaxFillNumerator   = 3
	batchArenaTailCompactPinnedMaxFillDenominator = 4
	batchEntriesPoolMaxRetain                     = 16 << 10
	batchIntSlicePoolMaxRetain                    = 16 << 10
	// When deferred iterator views pin retired memtables, reduce retained
	// batch-arena headroom to limit extra lease growth under that pressure.
	batchArenaDeferredPressureThresholdBytes = int64(512 << 20)
	batchArenaDeferredPressureHardCapDivisor = int64(2)
)

func batchCopyArenaInitCapForEntries(entries int) int {
	if entries <= 0 {
		return batchCopyArenaUnsizedInit
	}
	if entries > batchCopyArenaInitMax/batchCopyArenaBytesPerEntry {
		return batchCopyArenaInitMax
	}
	capHint := entries * batchCopyArenaBytesPerEntry
	if capHint < batchCopyArenaMinChunk {
		return batchCopyArenaMinChunk
	}
	if capHint > batchCopyArenaInitMax {
		return batchCopyArenaInitMax
	}
	return capHint
}

func (b *Batch) maybeSwitchToStreaming() {
	if b.streamTried || !b.streamEligible || b.backend != nil {
		return
	}
	// Streaming writes directly to the backend batch and therefore bypasses the
	// journal/value-log orchestration. Only enable it when the journal is
	// disabled and value-log pointers are disabled.
	if !b.db.disableJournal || b.db.valueLogEnabled() {
		return
	}
	// Switch to streaming (direct-to-backend batch) once a batch is "big enough"
	// that keeping all entries in memory provides little benefit.
	//
	// Why this exists:
	// - The cached/memtable path is great for small/random batches because it
	//   aggregates updates and reduces backend write amplification.
	// - For large strictly-increasing batches that start beyond the max key in
	//   the in-memory layers, we don't benefit from memtable aggregation; we just
	//   pay extra overhead storing the batch entries slice until Write().
	//
	// We intentionally use a small threshold (rather than flushThreshold) so
	// "BatchWrite1M" style workloads can switch early and avoid materializing the
	// entire batch in memory before Write().
	// Require both entry-count and byte-size thresholds to avoid switching tiny-value
	// batches to backend streaming solely due to key count.
	if len(b.entries) < streamSwitchMinEntries || b.size < streamSwitchMinBytes {
		return
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	b.db.mu.RUnlock()

	var maxKey []byte
	mutableRange := b.db.snapshotMutableRange()
	if mutableRange.valid {
		maxKey = mutableRange.max
	}
	for _, r := range queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}

	b.streamTried = true
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return
	}

	backendBatch := b.db.backend.NewBatch()
	if b.firstKey != nil && b.lastKey != nil {
		// Streaming is strictly increasing and starts beyond the max key in memory,
		// so the batch range is simply [first,last]. Keep copies for backend range
		// tracking after commit.
		b.batchRange.valid = true
		b.batchRange.min = append([]byte(nil), b.firstKey...)
		b.batchRange.max = append([]byte(nil), b.lastKey...)
	}
	if err := backendBatch.SetOps(b.entries); err != nil {
		_ = backendBatch.Close()
		return
	}
	b.backend = backendBatch
	b.entries = b.entries[:0]
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) freezeDictID(ctx context.Context) error {
	if b.dictIDValid {
		return nil
	}
	if b.db == nil {
		return nil
	}
	dictID, err := b.db.currentDictID(ctx)
	if err != nil {
		return err
	}
	b.dictID = dictID
	b.dictIDValid = true
	return nil
}

func (b *Batch) ensureDictBytes(ctx context.Context) ([]byte, error) {
	if b.dictBytesValid {
		return b.dictBytes, nil
	}
	if b.db == nil {
		return nil, nil
	}
	dictBytes, err := b.db.dictBytes(ctx, b.dictID)
	if err != nil {
		return nil, err
	}
	b.dictBytes = dictBytes
	b.dictBytesValid = true
	return dictBytes, nil
}

func (b *Batch) write(sync bool) error {
	if b.closed {
		return ErrBatchClosed
	}
	b.db.waitForCheckpoint()

	if b.backend != nil {
		var err error
		if sync && !b.db.relaxedSync {
			b.db.flushMu.Lock()
			err = b.backend.WriteSync()
			b.db.flushMu.Unlock()
		} else {
			b.db.flushMu.Lock()
			err = b.backend.Write()
			b.db.flushMu.Unlock()
		}
		if cerr := b.backend.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err == nil && b.batchRange.valid {
			b.db.mu.Lock()
			b.db.backendRange.add(b.batchRange.min)
			b.db.backendRange.add(b.batchRange.max)
			b.db.mu.Unlock()
		}
		b.backend = nil
		if err == nil && b.size > 0 {
			b.db.noteWrite()
		}
		b.updateBatchEntryHint()
		b.updateBatchCopyHint()
		b.Reset()
		return err
	}

	if ok, err := b.tryWriteWALOffStreamBypass(sync); err != nil {
		return err
	} else if ok {
		return nil
	}

	// WAL-off sync writes are common on versioned append-heavy workloads such as
	// IAVL commit. Use the direct backend bypass path for those publish
	// boundaries so we avoid rotating/flushing unrelated mutable state.
	if sync && b.db.disableJournal {
		return b.writeBypass(sync)
	}

	// Optimization: Bypass for Large Batches
	// Generalization: Only bypass if the batch is large enough to be comparable
	// to a memtable flush. Small/Medium random batches cause high write amplification
	// if written directly to the COW backend.
	if b.size >= int(b.db.flushThreshold) {
		return b.writeBypass(sync)
	}
	return b.writeRegular(sync)
}

func (b *Batch) tryWriteWALOffStreamBypass(sync bool) (bool, error) {
	if b == nil || b.db == nil {
		return false, nil
	}
	if !b.db.deferredValueLogEnabled() {
		return false, nil
	}
	if b.backend != nil || !b.streamEligible {
		return false, nil
	}
	if b.firstKey == nil || b.lastKey == nil {
		return false, nil
	}
	// Mirror maybeSwitchToStreaming: bypass only when both dimensions are large.
	if len(b.entries) < streamSwitchMinEntries || b.size < streamSwitchMinBytes {
		return false, nil
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	queueLen := len(b.db.queue)
	b.db.mu.RUnlock()
	if queueLen > 0 && len(queueRanges) == 0 {
		// Cannot reason about overlap without queue range tracking.
		return false, nil
	}

	var maxKey []byte
	mutableRange := b.db.snapshotMutableRange()
	if mutableRange.valid {
		maxKey = mutableRange.max
	}
	for _, r := range queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return false, nil
	}

	// WAL-off streaming bypass: append large values to the value log and commit
	// backend pointers directly, avoiding memtable ingestion costs for append-only
	// workloads.
	ops := getEntrySlice(len(b.entries))
	ops = append(ops, b.entries...)
	defer putEntrySlice(ops)

	ops, err := b.db.deferValueLogOps(ops, sync)
	if err != nil {
		return false, err
	}

	backendBatch := b.db.backend.NewBatch()
	if err := backendBatch.SetOps(ops); err != nil {
		_ = backendBatch.Close()
		return false, err
	}

	if sync && !b.db.relaxedSync {
		b.db.flushMu.Lock()
		err = backendBatch.WriteSync()
		b.db.flushMu.Unlock()
	} else {
		b.db.flushMu.Lock()
		err = backendBatch.Write()
		b.db.flushMu.Unlock()
	}
	if cerr := backendBatch.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return false, err
	}

	b.db.mu.Lock()
	b.db.backendRange.add(b.firstKey)
	b.db.backendRange.add(b.lastKey)
	b.db.mu.Unlock()

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	b.Reset()
	return true, nil
}

func (b *Batch) writeRegular(syncWrite bool) error {
	b.db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false

	// 1. Memtable capacity pre-check
	shardCount := len(b.db.mutableShards)
	shardAdds := b.shardAdds
	if cap(shardAdds) < shardCount {
		shardAdds = make([]int64, shardCount)
	} else {
		shardAdds = shardAdds[:shardCount]
		clear(shardAdds)
	}
	b.shardAdds = shardAdds

	shardCounts := b.shardCnts
	if cap(shardCounts) < shardCount {
		shardCounts = make([]int, shardCount)
	} else {
		shardCounts = shardCounts[:shardCount]
		clear(shardCounts)
	}
	b.shardCnts = shardCounts

	shardIdxs := b.shardIdxs
	if cap(shardIdxs) < len(b.entries) {
		shardIdxs = b.db.getBatchIntSlice(len(b.entries))
		shardIdxs = shardIdxs[:len(b.entries)]
	} else {
		shardIdxs = shardIdxs[:len(b.entries)]
	}
	b.shardIdxs = shardIdxs
	allDeletes := true
	for i := range b.entries {
		op := &b.entries[i]
		if op.Type != batch.OpDelete {
			allDeletes = false
		}
		idx := b.db.shardIndex(op.Key)
		shardIdxs[i] = idx
		shardCounts[idx]++
		add := int64(len(op.Key))
		if op.Type == batch.OpPut {
			add += int64(len(op.Value))
		}
		shardAdds[idx] += add
	}
	retainMainMems := make([]memtable.Table, 0, shardCount)
	retainMainSeen := make(map[memtable.Table]struct{}, shardCount)
	for i, add := range shardAdds {
		if add == 0 {
			continue
		}
		shard := &b.db.mutableShards[i]
		shard.mu.Lock()
		over := b.db.shardExceedsLimit(shard, add)
		shard.mu.Unlock()
		if over {
			b.db.writeMu.RUnlock()
			return ErrMemtableFull
		}
	}
	prospectiveRetainBytes := batchArenaChunksCapBytes(b.copyArenaChunks) + batchArenaChunksCapBytes(b.ptrCopyArenaChunks)
	allowBatchArenaBorrow, preflightBlocked := shouldBorrowBatchArenaBytesForWriteWithHardCap(
		prospectiveRetainBytes,
		b.db.currentBatchArenaRetainedHardCapEffectiveBytes(),
	)
	if !allowBatchArenaBorrow {
		batchArenaBorrowBlockedTotal.Add(1)
		if preflightBlocked {
			batchArenaBorrowPreflightBlockedTotal.Add(1)
			if prospectiveRetainBytes > 0 {
				batchArenaBorrowPreflightBlockedBytesTotal.Add(uint64(prospectiveRetainBytes))
			}
		}
	}

	// 2. Optional value-log + journal append loop.
	//
	// The value log (value store) and journal (redo log) are decoupled:
	// - When DisableWAL=true, we still append large values to the value log
	//   and store pointers in memory; there is simply no redo log for crash replay.
	// - When DisableWAL=false, we also append commit-intent records that can
	//   be replayed to recover unflushed writes.
	durability := journalDurabilityNone
	if syncWrite {
		if b.db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	} else if b.db.disableJournal && b.db.relaxedSync && b.db.indexOuterLeavesInValueLog {
		// In WAL-off fast profiles, staged importer batches can publish pointer
		// metadata via later sync writes before a fresh logical reader reloads the
		// version. Keep unsynced batch commits cheap, but flush value-log writer
		// buffers so pointer-backed payload bytes are visible to subsequent readers.
		durability = journalDurabilityFlush
	}
	debugPtr := b.db.debugFlushPointers
	valueLogEnabled := b.db.valueLogEnabled()

	var (
		eligibleIdxs       []int
		eligibleCountTotal int
		allowPointers      bool
		eligibleCount      int
		multiLanePointers  bool
	)
	if !allDeletes {
		eligibleIdxs = b.eligibleIdxs
		if cap(eligibleIdxs) < len(b.entries) {
			eligibleIdxs = make([]int, 0, len(b.entries))
		} else {
			eligibleIdxs = eligibleIdxs[:0]
		}
		b.eligibleIdxs = eligibleIdxs
		if valueLogEnabled || debugPtr {
			for i := range b.entries {
				op := &b.entries[i]
				if op.Type != batch.OpPut || !b.db.shouldWriteViaValueLogForKeyValue(op.Key, op.Value) {
					continue
				}
				eligibleIdxs = append(eligibleIdxs, i)
			}
		}
		eligibleCountTotal = len(eligibleIdxs)
		allowPointers = eligibleCountTotal > 0 && valueLogEnabled && b.db.allowValueLogPointers()
		if allowPointers && b.db.disableJournal && !b.db.memtableValueLogPointers {
			// WAL-off: when the journal is disabled, defer value-log appends to the flush boundary
			// so repeated overwrites can coalesce in the memtable before hitting disk.
			allowPointers = false
		}
		eligibleCount = len(eligibleIdxs)
		if debugPtr && eligibleCountTotal > 0 {
			b.db.debugPtrEligible.Add(int64(eligibleCountTotal))
			if !valueLogEnabled {
				b.db.debugPtrDisabled.Add(int64(eligibleCountTotal))
			} else if !allowPointers {
				b.db.debugPtrDenied.Add(int64(eligibleCountTotal))
			}
		}
		multiLanePointers = allowPointers &&
			b.db.disableJournal &&
			// journalDurabilityFlush is still an unsynced fast-path write. We widen
			// multi-lane pointer batching to include it so unsynced importer batches
			// can flush pointer payload bytes without collapsing back to a single lane.
			(durability == journalDurabilityNone || durability == journalDurabilityFlush) &&
			len(b.db.lanes) > 1 &&
			eligibleCount >= multiLaneValueLogMinRecords
	}

	var (
		lane *lane
		rids []uint64
	)
	needLaneForPointers := !allDeletes && !multiLanePointers && allowPointers
	needLaneForJournal := !b.db.disableJournal
	if needLaneForPointers || needLaneForJournal {
		l, err := b.db.pickLane(durability == journalDurabilitySync, -1)
		if err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		lane = l
		if durability == journalDurabilitySync {
			defer b.db.releaseLaneSync(lane)
		}
		if !b.db.disableJournal {
			rids = make([]uint64, len(b.entries))
		}
	}

	if !allDeletes && allowPointers && eligibleCount > 0 {
		if err := b.freezeDictID(context.Background()); err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		if multiLanePointers {
			type laneValueLogBatch struct {
				laneID      int
				idxs        []int
				records     []valuelog.Record
				safeNoClear bool
				ptrs        []page.ValuePtr
				err         error
			}

			laneCounts := make([]int, len(b.db.lanes))
			for _, idx := range eligibleIdxs {
				laneID := b.db.laneForShardIndex(shardIdxs[idx])
				if laneID < 0 || laneID >= len(b.db.lanes) {
					laneID = 0
				}
				laneCounts[laneID]++
			}

			laneBatches := make([]laneValueLogBatch, len(b.db.lanes))
			activeLaneIDs := make([]int, 0, len(b.db.lanes))
			for laneID, count := range laneCounts {
				if count == 0 {
					continue
				}
				lb := &laneBatches[laneID]
				lb.laneID = laneID
				lb.idxs = make([]int, 0, count)
				lb.records = getValueLogRecordsCap(count)
				lb.safeNoClear = true
				activeLaneIDs = append(activeLaneIDs, laneID)
			}

			defer func() {
				for _, laneID := range activeLaneIDs {
					lb := &laneBatches[laneID]
					if lb.records != nil {
						if lb.safeNoClear {
							putValueLogRecordsNoClear(lb.records)
						} else {
							putValueLogRecords(lb.records)
						}
						lb.records = nil
					}
					if lb.ptrs != nil {
						putValueLogPtrs(lb.ptrs)
						lb.ptrs = nil
					}
				}
			}()

			for _, idx := range eligibleIdxs {
				op := &b.entries[idx]
				rid := b.db.nextRID.Add(1)
				if rids != nil {
					rids[idx] = rid
				}
				laneID := b.db.laneForShardIndex(shardIdxs[idx])
				if laneID < 0 || laneID >= len(b.db.lanes) {
					laneID = 0
				}
				lb := &laneBatches[laneID]
				payload := op.Value
				if lb.safeNoClear && (len(payload) > 64 || cap(payload) > 64) {
					lb.safeNoClear = false
				}
				lb.idxs = append(lb.idxs, idx)
				lb.records = append(lb.records, valuelog.Record{RID: rid, Value: payload})
			}

			var wg sync.WaitGroup
			for _, laneID := range activeLaneIDs {
				lb := &laneBatches[laneID]
				wg.Add(1)
				go func(batch *laneValueLogBatch) {
					defer wg.Done()
					batch.ptrs, batch.err = b.db.appendValueLog(&b.db.lanes[batch.laneID], b.dictID, nil, batch.records, durability)
				}(lb)
			}
			wg.Wait()

			for _, laneID := range activeLaneIDs {
				lb := &laneBatches[laneID]
				if lb.err != nil {
					b.db.writeMu.RUnlock()
					return lb.err
				}
			}

			for _, laneID := range activeLaneIDs {
				lb := &laneBatches[laneID]
				if len(lb.ptrs) != len(lb.idxs) {
					if debugPtr {
						b.db.debugPtrNoPtr.Add(int64(len(lb.idxs)))
					}
					continue
				}
				for i, idx := range lb.idxs {
					op := &b.entries[idx]
					op.ValuePtr = lb.ptrs[i]
					op.IsPtr = true
					if b.db.memtableValueLogPointers {
						op.Value = nil
					}
				}
				if debugPtr {
					b.db.debugPtrUsed.Add(int64(len(lb.idxs)))
				}
				retainPath := b.db.currentValueLogPath(&b.db.lanes[lb.laneID])
				if retainPath != "" {
					b.db.markValueLogRetain(retainPath)
				}
				putValueLogPtrs(lb.ptrs)
				lb.ptrs = nil
			}
		} else {
			keys := getValueLogKeys(eligibleCount)
			defer putValueLogKeys(keys)
			values := getValueLogKeys(eligibleCount)
			defer putValueLogKeys(values)
			for _, idx := range eligibleIdxs {
				op := &b.entries[idx]
				keys = append(keys, op.Key)
				values = append(values, op.Value)
			}
			var (
				ptrs         []page.ValuePtr
				groups       []outerLeafRecordGroup
				valueRecords []valuelog.Record
				outerArena   []byte
			)
			valueRecords, groups, outerArena, buildErr := b.db.buildOuterLeafValueRecords(keys, values)
			if buildErr != nil {
				b.db.writeMu.RUnlock()
				return buildErr
			}
			if len(valueRecords) == 0 {
				putValueLogRecordsNoClear(valueRecords)
				putOuterLeafArena(outerArena)
				b.db.writeMu.RUnlock()
				return fmt.Errorf("cachingdb: empty value-log record set for %d eligible ops", eligibleCount)
			}
			defer putValueLogRecordsNoClear(valueRecords)
			defer putOuterLeafArena(outerArena)
			startRID := b.db.nextRID.Add(uint64(len(valueRecords))) - uint64(len(valueRecords)) + 1
			for i := range valueRecords {
				rid := startRID + uint64(i)
				valueRecords[i].RID = rid
				if rids != nil {
					group := groups[i]
					if group.start < 0 || group.end < group.start || group.end > len(eligibleIdxs) {
						b.db.writeMu.RUnlock()
						return fmt.Errorf("cachingdb: value-log group out of range [%d,%d) len=%d", group.start, group.end, len(eligibleIdxs))
					}
					for srcPos := group.start; srcPos < group.end; srcPos++ {
						idx := eligibleIdxs[srcPos]
						rids[idx] = rid
					}
				}
			}
			ptrs, buildErr = b.db.appendValueLog(lane, b.dictID, nil, valueRecords, durability)
			if buildErr != nil {
				b.db.writeMu.RUnlock()
				return buildErr
			}
			if len(ptrs) != len(groups) {
				putValueLogPtrs(ptrs)
				b.db.writeMu.RUnlock()
				return fmt.Errorf("cachingdb: value-log pointer group count mismatch expected=%d got=%d", len(groups), len(ptrs))
			}
			defer putValueLogPtrs(ptrs)

			used := 0
			for i := range groups {
				ptr := ptrs[i]
				group := groups[i]
				if group.start < 0 || group.end < group.start || group.end > len(eligibleIdxs) {
					b.db.writeMu.RUnlock()
					return fmt.Errorf("cachingdb: value-log pointer group out of range [%d,%d) len=%d", group.start, group.end, len(eligibleIdxs))
				}
				for srcPos := group.start; srcPos < group.end; srcPos++ {
					idx := eligibleIdxs[srcPos]
					op := &b.entries[idx]
					op.ValuePtr = ptr
					op.IsPtr = true
					if b.db.memtableValueLogPointers {
						op.Value = nil
					}
					used++
				}
			}
			if debugPtr && used > 0 {
				b.db.debugPtrUsed.Add(int64(used))
			}
			retainPath := b.db.currentValueLogPath(lane)
			if retainPath != "" {
				b.db.markValueLogRetain(retainPath)
			}
		}
	}

	if !b.db.disableJournal {
		records := b.walBuf[:0]
		if cap(records) < len(b.entries) {
			records = make([]logRecord, 0, len(b.entries))
		}
		for i := range b.entries {
			op := &b.entries[i]
			switch op.Type {
			case batch.OpDelete:
				records = append(records, logRecord{Op: logOpDelete, Key: op.Key})
			case batch.OpPut:
				if rids != nil && rids[i] != 0 {
					records = append(records, logRecord{Op: logOpSetRID, Key: op.Key, RID: rids[i]})
				} else {
					records = append(records, logRecord{Op: logOpSetInline, Key: op.Key, Value: op.Value})
				}
			}
		}
		b.walBuf = records
		if err := b.db.appendWAL(lane, records, durability); err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
	}

	if !allDeletes {
		// Before borrowing batch arena chunks into memtables, compact
		// pathologically underfilled tail chunks by cloning only the aliased
		// entry slices. This converts a large retained tail into tight slices.
		b.maybeCompactUnderfilledArenaTails()
	}

	if allDeletes {
		shardIdxSets := b.shardIdxSets
		if cap(shardIdxSets) < shardCount {
			shardIdxSets = make([][]int, shardCount)
		} else {
			shardIdxSets = shardIdxSets[:shardCount]
			for i := range shardIdxSets {
				shardIdxSets[i] = shardIdxSets[i][:0]
			}
		}
		b.shardIdxSets = shardIdxSets
		for i, count := range shardCounts {
			if count <= 0 {
				continue
			}
			idxs := shardIdxSets[i]
			if cap(idxs) < count {
				idxs = b.db.getBatchIntSlice(count)
			} else {
				idxs = idxs[:0]
			}
			shardIdxSets[i] = idxs
		}
		for i := range b.entries {
			idx := shardIdxs[i]
			shardIdxSets[idx] = append(shardIdxSets[idx], i)
		}

		// 3. Memtable Update (delete-only fast path)
		for i := range shardIdxSets {
			idxs := shardIdxSets[i]
			if len(idxs) == 0 {
				continue
			}
			shard := &b.db.mutableShards[i]
			shard.mu.Lock()
			useSteal, stealSuppressedDeferred := cachedBatchWriteUseSteal(b.db, shard.mem)
			useSteal = allowBatchArenaBorrow && useSteal
			if stealSuppressedDeferred && allowBatchArenaBorrow {
				batchArenaStealSuppressedDeferredTotal.Add(1)
				batchArenaStealSuppressedDeferredEntriesTotal.Add(uint64(len(idxs)))
			}
			if useSteal && cachedBatchWriteNeedsBatchArenaRetention(shard.mem) {
				if _, ok := retainMainSeen[shard.mem]; !ok {
					retainMainSeen[shard.mem] = struct{}{}
					retainMainMems = append(retainMainMems, shard.mem)
				}
			}
			if b.streamEligible {
				first := b.entries[idxs[0]].Key
				last := first
				for _, entryIdx := range idxs {
					key := b.entries[entryIdx].Key
					last = key
					memtableBatchDelete(shard.mem, useSteal, key)
				}
				shard.rng.add(first)
				if len(idxs) > 1 {
					shard.rng.add(last)
				}
				b.db.noteWriteSortedRun(first, last, len(idxs))
			} else {
				for _, entryIdx := range idxs {
					key := b.entries[entryIdx].Key
					memtableBatchDelete(shard.mem, useSteal, key)
					shard.rng.add(key)
					b.db.noteWriteKey(key)
				}
			}
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			b.db.mutableBytes.Add(delta)
			shard.mu.Unlock()
		}
	} else {
		shardEntries := b.shardEntries
		if cap(shardEntries) < shardCount {
			shardEntries = make([][]batch.Entry, shardCount)
		} else {
			shardEntries = shardEntries[:shardCount]
			for i := range shardEntries {
				shardEntries[i] = shardEntries[i][:0]
			}
		}
		b.shardEntries = shardEntries
		for i, count := range shardCounts {
			if count > 0 {
				entries := shardEntries[i]
				if cap(entries) < count {
					entries = b.db.getBatchShardEntries(count)
				} else {
					entries = entries[:0]
				}
				shardEntries[i] = entries
			}
		}
		for i, op := range b.entries {
			idx := shardIdxs[i]
			shardEntries[idx] = append(shardEntries[idx], op)
		}

		// 3. Memtable Update
		for i := range shardEntries {
			entries := shardEntries[i]
			if len(entries) == 0 {
				continue
			}
			shard := &b.db.mutableShards[i]
			shard.mu.Lock()
			useStream := b.streamEligible
			useSteal, stealSuppressedDeferred := cachedBatchWriteUseSteal(b.db, shard.mem)
			useSteal = allowBatchArenaBorrow && useSteal
			if stealSuppressedDeferred && allowBatchArenaBorrow {
				batchArenaStealSuppressedDeferredTotal.Add(1)
				batchArenaStealSuppressedDeferredEntriesTotal.Add(uint64(len(entries)))
			}
			if useSteal && cachedBatchWriteNeedsBatchArenaRetention(shard.mem) {
				if _, ok := retainMainSeen[shard.mem]; !ok {
					retainMainSeen[shard.mem] = struct{}{}
					retainMainMems = append(retainMainMems, shard.mem)
				}
			}
			storeInlinePtrValues := !b.db.memtableValueLogPointers
			if useStream && useSteal {
				if applier, ok := shard.mem.(memtable.TrustedSortedBatchApplier); ok {
					applier.ApplyStealSortedBatchTrusted(entries, nil)
				} else if applier, ok := shard.mem.(memtable.SortedBatchApplier); ok {
					applier.ApplyStealSortedBatch(entries, nil)
				} else {
					for _, op := range entries {
						if op.Type == batch.OpDelete {
							memtableBatchDelete(shard.mem, true, op.Key)
						} else {
							memtableBatchSet(shard.mem, true, allowBatchArenaBorrow, storeInlinePtrValues, op)
						}
					}
				}
				first := entries[0].Key
				last := entries[len(entries)-1].Key
				shard.rng.add(first)
				if len(entries) > 1 {
					shard.rng.add(last)
				}
				b.db.noteWriteSortedRun(first, last, len(entries))
			} else {
				for _, op := range entries {
					if op.Type == batch.OpDelete {
						memtableBatchDelete(shard.mem, useSteal, op.Key)
					} else {
						borrowed := false
						if allowBatchArenaBorrow && !useSteal {
							if _, ok := shard.mem.(memtable.ValueBorrower); ok {
								if op.IsPtr {
									borrowed = storeInlinePtrValues && len(op.Value) > 0
								} else {
									borrowed = len(op.Value) > 0
								}
							}
						}
						memtableBatchSet(shard.mem, useSteal, allowBatchArenaBorrow, storeInlinePtrValues, op)
						if borrowed {
							if _, ok := retainMainSeen[shard.mem]; !ok {
								retainMainSeen[shard.mem] = struct{}{}
								retainMainMems = append(retainMainMems, shard.mem)
							}
						}
					}
					if useStream {
						// Preserve sorted-run accounting even when we avoid Steal.
						continue
					}
					shard.rng.add(op.Key)
					b.db.noteWriteKey(op.Key)
				}
				if useStream {
					first := entries[0].Key
					last := entries[len(entries)-1].Key
					shard.rng.add(first)
					if len(entries) > 1 {
						shard.rng.add(last)
					}
					b.db.noteWriteSortedRun(first, last, len(entries))
				}
			}
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			b.db.mutableBytes.Add(delta)
			shard.mu.Unlock()
		}
	}

	// 3. Threshold Check
	if b.db.mutableBytes.Load() > b.db.mutableFlushThreshold() {
		needRotate = true
	}
	if syncWrite && b.db.disableJournal {
		needSyncBarrier = true
	}
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	mainChunks := b.drainCopyArenaChunks()
	retainPtrArena := false
	ptrTouchedMems := make([]memtable.Table, 0, len(b.ptrValueIdxs))
	if allowBatchArenaBorrow && len(b.ptrValueIdxs) > 0 {
		seenPtrMems := make(map[memtable.Table]struct{}, len(b.ptrValueIdxs))
		for _, idx := range b.ptrValueIdxs {
			if idx < 0 || idx >= len(b.entries) || idx >= len(shardIdxs) {
				b.db.writeMu.RUnlock()
				return fmt.Errorf("cachingdb: ptr value entry index %d out of range", idx)
			}
			if b.entries[idx].Value == nil {
				continue
			}
			retainPtrArena = true
			shardIdx := shardIdxs[idx]
			if shardIdx < 0 || shardIdx >= len(b.db.mutableShards) {
				b.db.writeMu.RUnlock()
				return fmt.Errorf("cachingdb: ptr value shard index %d out of range for entry %d", shardIdx, idx)
			}
			mt := b.db.mutableShards[shardIdx].mem
			if mt == nil {
				continue
			}
			if _, ok := seenPtrMems[mt]; ok {
				continue
			}
			seenPtrMems[mt] = struct{}{}
			ptrTouchedMems = append(ptrTouchedMems, mt)
		}
	}
	if b.ptrValueIdxs != nil {
		b.ptrValueIdxs = b.ptrValueIdxs[:0]
	}
	ptrChunks := b.drainPtrCopyArenaChunks()
	b.db.retainBatchArenaChunksForMemtables(mainChunks, retainMainMems)
	if retainPtrArena {
		b.db.retainBatchArenaChunksForMemtables(ptrChunks, ptrTouchedMems)
	} else {
		putBatchArenas(ptrChunks)
	}
	b.db.writeMu.RUnlock()

	if needRotate {
		if err := b.db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := b.db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.db.maybeAssistFlush()
	b.Reset()
	return nil
}

type logSegmentInfo struct {
	path     string
	size     int64
	seq      int
	lane     int
	valueLog bool
}

func listNonEmptyLogSegments(walDir string) (segments []logSegmentInfo, nonEmptyBytes int64) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, 0
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		lane, seq, valueLog, ok := parseLogSeq(name)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(walDir, name)
		segments = append(segments, logSegmentInfo{path: path, size: info.Size(), seq: seq, lane: lane, valueLog: valueLog})
		if info.Size() > 0 {
			nonEmptyBytes += info.Size()
		}
	}
	return segments, nonEmptyBytes
}

func maxValueLogRIDFromSegments(segments []logSegmentInfo) (uint64, error) {
	var maxRID uint64
	for _, seg := range segments {
		if !seg.valueLog || seg.size <= 0 || seg.lane < 0 || seg.seq < 0 {
			continue
		}
		fileID, err := valuelog.EncodeFileID(uint32(seg.lane), uint32(seg.seq))
		if err != nil {
			return 0, err
		}
		reader, err := valuelog.NewReader(seg.path, fileID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, err
		}
		reader.DisableValueDecode()
		for {
			rid, _, err := reader.ReadNextMeta()
			if err == nil {
				if rid > maxRID {
					maxRID = rid
				}
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			_ = reader.Close()
			return 0, err
		}
		if err := reader.Close(); err != nil {
			return 0, err
		}
	}
	return maxRID, nil
}

func tailValueLogSegmentsByLane(segments []logSegmentInfo) []logSegmentInfo {
	if len(segments) == 0 {
		return nil
	}
	tailByLane := make(map[int]logSegmentInfo)
	for _, seg := range segments {
		if !seg.valueLog || seg.size <= 0 || seg.lane < 0 || seg.seq < 0 {
			continue
		}
		prev, ok := tailByLane[seg.lane]
		if !ok || seg.seq > prev.seq {
			tailByLane[seg.lane] = seg
		}
	}
	if len(tailByLane) == 0 {
		return nil
	}
	tails := make([]logSegmentInfo, 0, len(tailByLane))
	for _, seg := range tailByLane {
		tails = append(tails, seg)
	}
	sort.Slice(tails, func(i, j int) bool {
		if tails[i].lane != tails[j].lane {
			return tails[i].lane < tails[j].lane
		}
		return tails[i].seq < tails[j].seq
	})
	return tails
}

func parseLogSeq(name string) (int, int, bool, bool) {
	const (
		commitPrefix = "commit-"
		valuePrefix  = "value-"
		walPrefix    = "wal-"
		vlogPrefix   = "vlog-"
	)
	if filepath.Ext(name) != ".log" {
		return 0, 0, false, false
	}
	base := strings.TrimSuffix(name, ".log")

	parseLaneSeq := func(rest string) (int, int, bool) {
		parts := strings.SplitN(rest, "-", 2)
		if len(parts) != 2 {
			return 0, 0, false
		}
		lane, err := strconv.Atoi(parts[0])
		if err != nil || lane < 0 {
			return 0, 0, false
		}
		seq, err := strconv.Atoi(parts[1])
		if err != nil || seq < 0 {
			return 0, 0, false
		}
		return lane, seq, true
	}

	if strings.HasPrefix(base, "commit-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "commit-l"))
		return lane, seq, false, ok
	}
	if strings.HasPrefix(base, "value-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "value-l"))
		return lane, seq, true, ok
	}
	if strings.HasPrefix(base, commitPrefix) {
		core := strings.TrimPrefix(base, commitPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, valuePrefix) {
		core := strings.TrimPrefix(base, valuePrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	if strings.HasPrefix(base, walPrefix) {
		core := strings.TrimPrefix(base, walPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, vlogPrefix) {
		core := strings.TrimPrefix(base, vlogPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	return 0, 0, false, false
}

func (b *Batch) writeBypass(sync bool) (err error) {
	// WAL-off + outer-leaf-in-vlog workloads are highly sensitive to write
	// coalescing. Direct backend sync writes can preserve correctness but still
	// explode the live leaf-page count because each batch mutates the backend tree
	// incrementally instead of letting the cached flush path rebuild denser pages.
	// Keep the direct bypass for other cases, but route this profile through the
	// regular cached path.
	if sync && b.db.disableJournal && b.db.indexOuterLeavesInValueLog {
		return b.writeRegular(sync)
	}
	// Fast path: if none of these keys exist in mutable/queue, we can write directly
	// to the backend without flushing (no in-memory shadowing possible).
	// Cheap append-only check: if the batch key range does not overlap with any
	// in-memory key ranges, it cannot be shadowed.
	batchRange := keyRange{}
	for _, op := range b.entries {
		key := op.Key
		batchRange.add(key)
	}

	mutableRange := b.db.snapshotMutableRange()

	var (
		mutables      []memtable.Table
		queue         []memtable.Table
		queueShardIDs []uint16
		queueRanges   []keyRange
		overlaps      bool
	)
	view := b.db.retainMemtableView()
	if view != nil {
		defer b.db.releaseMemtableView(view)
	}

	b.db.mu.RLock()
	overlaps = rangesOverlap(batchRange, mutableRange)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueShardIDs = view.queueShardIDs
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open(), but keep safe
		// behavior for zero-value DBs and tests.
		if len(b.db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(b.db.mutableShards))
			for i := range b.db.mutableShards {
				mutables[i] = b.db.mutableShards[i].mem
			}
		}
		if len(b.db.queue) > 0 {
			queue = append([]memtable.Table(nil), b.db.queue...)
		}
		if len(b.db.queueShardIDs) > 0 {
			queueShardIDs = append([]uint16(nil), b.db.queueShardIDs...)
		}
		if len(b.db.queueRanges) > 0 {
			queueRanges = append([]keyRange(nil), b.db.queueRanges...)
		}
	}
	b.db.mu.RUnlock()

	if !overlaps {
		if len(queueRanges) == 0 && len(queue) > 0 {
			overlaps = true
		} else {
			for _, r := range queueRanges {
				if rangesOverlap(batchRange, r) {
					overlaps = true
					break
				}
			}
		}
	}

	if overlaps {
		// Slow path: verify no individual key exists in memory (handles sparse overlap).
		for _, op := range b.entries {
			key := op.Key
			if len(mutables) > 0 {
				idx := b.db.shardIndex(key)
				if idx < len(mutables) && mutables[idx] != nil {
					if _, _, found := mutables[idx].Get(key); found {
						return b.writeRegular(sync)
					}
				}
			}
			for i := len(queue) - 1; i >= 0; i-- {
				if len(queueShardIDs) > i && len(mutables) > 0 {
					idx := b.db.shardIndex(key)
					if int(queueShardIDs[i]) != idx {
						continue
					}
				}
				if _, _, found := queue[i].Get(key); found {
					return b.writeRegular(sync)
				}
			}
		}
	}

	// Write directly to backend
	backendBatch := b.db.backend.NewBatch()
	defer func() {
		if backendBatch != nil {
			if cerr := backendBatch.Close(); err == nil {
				err = cerr
			}
		}
	}()

	ops := getEntrySlice(len(b.entries))
	ops = append(ops, b.entries...)
	releaseOps := true
	defer func() {
		if releaseOps {
			putEntrySlice(ops)
		}
	}()

	if rewritten, err := b.db.prepareBypassValueLogOps(ops, sync); err != nil {
		return err
	} else {
		ops = rewritten
	}

	// Use SetOps for bulk transfer (backend will resolve value-log pointers).
	if err := backendBatch.SetOps(ops); err != nil {
		if errors.Is(err, batch.ErrValueTooLarge) {
			origEntries := b.entries
			b.entries = ops
			err = b.writeRegular(sync)
			if err != nil {
				b.entries = origEntries
				return err
			}
			if origEntries != nil {
				b.entries = origEntries[:0]
			} else {
				b.entries = nil
			}
			return nil
		}
		return err
	}

	if sync && !b.db.relaxedSync {
		b.db.flushMu.Lock()
		err = backendBatch.WriteSync()
		b.db.flushMu.Unlock()
	} else {
		b.db.flushMu.Lock()
		err = backendBatch.Write()
		b.db.flushMu.Unlock()
	}

	if err != nil {
		return err
	}
	backendBatch = nil

	b.db.mu.Lock()
	if batchRange.valid {
		b.db.backendRange.add(batchRange.min)
		b.db.backendRange.add(batchRange.max)
	}
	b.db.mu.Unlock()

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	b.Reset()
	return nil
}

func (b *Batch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	if b.db != nil && b.entries != nil {
		b.db.putBatchEntries(b.entries)
	}
	if b.db != nil && b.shardIdxs != nil {
		b.db.putBatchIntSlice(b.shardIdxs)
	}
	if b.db != nil && b.shardIdxSets != nil && cap(b.shardIdxSets) > 0 {
		full := b.shardIdxSets[:cap(b.shardIdxSets)]
		for i := range full {
			if full[i] != nil {
				b.db.putBatchIntSlice(full[i])
				full[i] = nil
			}
		}
	}
	if b.db != nil && b.shardEntries != nil && cap(b.shardEntries) > 0 {
		full := b.shardEntries[:cap(b.shardEntries)]
		for i := range full {
			if full[i] != nil {
				b.db.putBatchShardEntries(full[i])
				full[i] = nil
			}
		}
	}
	b.recycleCopyArenaChunks()
	b.recyclePtrCopyArenaChunks()
	b.entries = nil
	b.walBuf = nil
	b.shardIdxs = nil
	b.eligibleIdxs = nil
	b.shardAdds = nil
	b.shardCnts = nil
	b.shardEntries = nil
	b.shardIdxSets = nil
	b.firstKey = nil
	b.lastKey = nil
	b.ptrValueIdxs = nil
	return nil
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		return b.backend.Replay(fn)
	}

	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	if b.closed {
		return 0, ErrBatchClosed
	}
	return b.size, nil
}

// singleSourceIterator wraps a single UnsafeIterator to satisfy merging.Iterator,
// skipping tombstones.
type singleSourceIterator struct {
	iter  iterator.UnsafeIterator
	valid bool
	start []byte
	end   []byte
}

func newSingleSourceIterator(iter iterator.UnsafeIterator, start, end []byte) merging.Iterator {
	it := &singleSourceIterator{
		iter:  iter,
		start: start,
		end:   end,
	}
	// Iterator is already sought to start by the caller
	it.advance()
	return it
}

func (it *singleSourceIterator) advance() {
	it.valid = false
	for it.iter.Valid() {
		if it.end != nil && bytes.Compare(it.iter.UnsafeKey(), it.end) >= 0 {
			return
		}
		if it.iter.IsDeleted() {
			it.iter.Next()
			continue
		}
		it.valid = true
		return
	}
}

func (it *singleSourceIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.iter.Next()
	it.advance()
}

func (it *singleSourceIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.UnsafeKey()
}

func (it *singleSourceIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.UnsafeValue()
}

func (it *singleSourceIterator) Valid() bool               { return it.valid }
func (it *singleSourceIterator) Key() []byte               { return it.iter.Key() }
func (it *singleSourceIterator) Value() []byte             { return it.iter.Value() }
func (it *singleSourceIterator) KeyCopy(dst []byte) []byte { return it.iter.KeyCopy(dst) }
func (it *singleSourceIterator) ValueCopy(dst []byte) []byte {
	return it.iter.ValueCopy(dst)
}
func (it *singleSourceIterator) Close() error             { return it.iter.Close() }
func (it *singleSourceIterator) Error() error             { return it.iter.Error() }
func (it *singleSourceIterator) Domain() ([]byte, []byte) { return it.start, it.end }

// emptyIterator represents an iterator with no elements.
type emptyIterator struct {
	start, end []byte
}

func (it *emptyIterator) Next()                     { panic("iterator invalid") }
func (it *emptyIterator) Valid() bool               { return false }
func (it *emptyIterator) Key() []byte               { panic("iterator invalid") }
func (it *emptyIterator) Value() []byte             { panic("iterator invalid") }
func (it *emptyIterator) KeyCopy(_ []byte) []byte   { panic("iterator invalid") }
func (it *emptyIterator) ValueCopy(_ []byte) []byte { panic("iterator invalid") }
func (it *emptyIterator) Close() error              { return nil }
func (it *emptyIterator) Error() error              { return nil }
func (it *emptyIterator) Domain() ([]byte, []byte)  { return it.start, it.end }
