package db

import (
	"time"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
)

const maxTimeDurationNs = uint64(1<<63 - 1)

var publishWatermarkLatencyBucketUpperBounds = [...]time.Duration{
	50 * time.Microsecond,
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	40 * time.Millisecond,
	80 * time.Millisecond,
	160 * time.Millisecond,
	320 * time.Millisecond,
	640 * time.Millisecond,
	1 * time.Second,
}

const publishWatermarkLatencyBucketCount = len(publishWatermarkLatencyBucketUpperBounds) + 1

func publishWatermarkLatencyBucketIndex(latency time.Duration) int {
	if latency <= 0 {
		return 0
	}
	for i, upper := range publishWatermarkLatencyBucketUpperBounds {
		if latency <= upper {
			return i
		}
	}
	return len(publishWatermarkLatencyBucketUpperBounds)
}

func estimatePublishWatermarkPercentile(buckets [publishWatermarkLatencyBucketCount]uint64, total uint64, q float64) time.Duration {
	if total == 0 {
		return 0
	}
	if q <= 0 {
		return publishWatermarkLatencyBucketUpperBounds[0]
	}
	if q >= 1 {
		return publishWatermarkLatencyBucketUpperBounds[len(publishWatermarkLatencyBucketUpperBounds)-1]
	}
	target := uint64(float64(total)*q + 0.999999)
	if target < 1 {
		target = 1
	}
	var seen uint64
	for i, n := range buckets {
		seen += n
		if seen >= target {
			if i < len(publishWatermarkLatencyBucketUpperBounds) {
				return publishWatermarkLatencyBucketUpperBounds[i]
			}
			// Overflow bucket (> last configured bound).
			return publishWatermarkLatencyBucketUpperBounds[len(publishWatermarkLatencyBucketUpperBounds)-1] + time.Nanosecond
		}
	}
	// Shouldn't happen for a well-formed histogram, but treat as overflow.
	return publishWatermarkLatencyBucketUpperBounds[len(publishWatermarkLatencyBucketUpperBounds)-1] + time.Nanosecond
}

func durationFromUint64Ns(ns uint64) time.Duration {
	if ns > maxTimeDurationNs {
		return time.Duration(maxTimeDurationNs)
	}
	return time.Duration(ns)
}

func (db *DB) observePublishWatermark(wait, hold, latency time.Duration) {
	if db == nil {
		return
	}
	if wait < 0 {
		wait = 0
	}
	if hold < 0 {
		hold = 0
	}
	if latency < 0 {
		latency = 0
	}

	waitNs := uint64(wait.Nanoseconds())
	holdNs := uint64(hold.Nanoseconds())
	latNs := uint64(latency.Nanoseconds())

	db.publishWatermarkWaitTotalNs.Add(waitNs)
	db.publishWatermarkHoldTotalNs.Add(holdNs)
	db.publishWatermarkLatencySamples.Add(1)
	for {
		cur := db.publishWatermarkLatencyMaxNs.Load()
		if latNs <= cur || db.publishWatermarkLatencyMaxNs.CompareAndSwap(cur, latNs) {
			break
		}
	}
	bucket := publishWatermarkLatencyBucketIndex(latency)
	db.publishWatermarkLatencyBuckets[bucket].Add(1)
}

func (db *DB) publishWatermarkStats() (lockDelaySharePct float64, latencyP99Ms float64) {
	if db == nil {
		return 0, 0
	}
	waitNs := db.publishWatermarkWaitTotalNs.Load()
	holdNs := db.publishWatermarkHoldTotalNs.Load()
	if denom := waitNs + holdNs; denom > 0 {
		lockDelaySharePct = 100 * float64(waitNs) / float64(denom)
	}

	samples := db.publishWatermarkLatencySamples.Load()
	if samples == 0 {
		return lockDelaySharePct, 0
	}

	var buckets [publishWatermarkLatencyBucketCount]uint64
	for i := range buckets {
		buckets[i] = db.publishWatermarkLatencyBuckets[i].Load()
	}
	p99 := estimatePublishWatermarkPercentile(buckets, samples, 0.99)
	if p99 <= 0 {
		if maxNs := db.publishWatermarkLatencyMaxNs.Load(); maxNs > 0 {
			p99 = durationFromUint64Ns(maxNs)
		}
	}
	latencyP99Ms = float64(p99) / float64(time.Millisecond)
	return lockDelaySharePct, latencyP99Ms
}

type orderedRootDeltaGroupPublishStats struct {
	calls                                  uint64
	errors                                 uint64
	roots                                  uint64
	waitTotalNs                            uint64
	holdTotalNs                            uint64
	preflightNs                            uint64
	rootApplyNs                            uint64
	rootApplyCalls                         uint64
	rootApplyParallelGroups                uint64
	rootApplyParallelRoots                 uint64
	rootApplyOps                           uint64
	rootApplyNodeLoads                     uint64
	rootApplyPagerNodeLoads                uint64
	rootApplyLeafLogNodeLoads              uint64
	rootApplyLeafLogCacheHits              uint64
	rootApplyLeafLogReaderCalls            uint64
	rootApplyLeafLogViewReads              uint64
	rootApplyLeafLogScratchReads           uint64
	rootApplyPagerNodeBytesRead            uint64
	rootApplyLeafLogNodeBytesRead          uint64
	rootApplyLeafLogRecordHintBytesRead    uint64
	rootApplyLeafMerges                    uint64
	rootApplyInternalMerges                uint64
	rootApplyInternalParallelMerges        uint64
	rootApplyInternalParallelChildren      uint64
	rootApplyInternalParallelWorkers       uint64
	rootApplyInternalParallelOps           uint64
	rootApplyLeafPagesWritten              uint64
	rootApplyPagerLeafPagesWritten         uint64
	rootApplyLeafLogPagesWritten           uint64
	rootApplyLeafPageBytesWritten          uint64
	rootApplyPagerLeafPageBytesWritten     uint64
	rootApplyLeafLogPageBytesWritten       uint64
	rootApplyLeafLogRecordHintBytesWritten uint64
	rootApplyInternalPagesWritten          uint64
	rootApplyInternalPageBytesWritten      uint64
	rootApplyInternalChildRefs             uint64
	rootApplyInternalPageChildRefs         uint64
	rootApplyInternalLeafLogRefs           uint64
	rootApplyInternalLeafLogRefCopies      uint64
	rootApplyRootSplitLevels               uint64
	rootApplyReadOnlyPrepareNs             uint64
	rootApplyReadOnlyPrepareCalls          uint64
	rootApplyReadOnlyPrepareErrors         uint64
	rootApplyReadOnlyPrepareValidationFail uint64
	rootApplyReadOnlyPrepareRequested      uint64
	rootApplyReadOnlyPrepareRequestedMax   uint64
	rootApplyReadOnlyPrepareSpans          uint64
	rootApplyReadOnlyPrepareSpanOps        uint64
	rootApplyReadOnlyPrepareSpanBytes      uint64
	rootApplyReadOnlyPrepareWorkerRanges   uint64
	systemBuildNs                          uint64
	systemApplyNs                          uint64
	systemApplyCalls                       uint64
	systemApplyOps                         uint64
	systemApplyNodeLoads                   uint64
	publishPrepareNs                       uint64
	publishPrepareCalls                    uint64
	publishPrepareErrors                   uint64
	finalizeNs                             uint64
	finalizeCalls                          uint64
	latencyP99                             time.Duration
	latencyMax                             time.Duration
	writeLockWaitShare                     float64
	avgRootsPerCall                        float64
}

type orderedRootDeltaGroupPublishPhaseStats struct {
	preflightNs                            uint64
	rootApplyNs                            uint64
	rootApplyCalls                         uint64
	rootApplyParallelGroups                uint64
	rootApplyParallelRoots                 uint64
	rootApplyMetrics                       orderedRootDeltaGroupZipperStats
	rootApplyReadOnlyPrepareNs             uint64
	rootApplyReadOnlyPrepareCalls          uint64
	rootApplyReadOnlyPrepareErrors         uint64
	rootApplyReadOnlyPrepareValidationFail uint64
	rootApplyReadOnlyPrepareRequested      uint64
	rootApplyReadOnlyPrepareRequestedMax   uint64
	rootApplyReadOnlyPrepareSpans          uint64
	rootApplyReadOnlyPrepareSpanOps        uint64
	rootApplyReadOnlyPrepareSpanBytes      uint64
	rootApplyReadOnlyPrepareWorkerRanges   uint64
	systemBuildNs                          uint64
	systemApplyNs                          uint64
	systemApplyCalls                       uint64
	systemApplyMetrics                     orderedRootDeltaGroupZipperStats
	publishPrepareNs                       uint64
	publishPrepareCalls                    uint64
	publishPrepareErrors                   uint64
	finalizeNs                             uint64
	finalizeCalls                          uint64
}

type orderedRootDeltaGroupZipperStats struct {
	ZipperApplyOps                      int
	ZipperNodeLoads                     int
	ZipperPagerNodeLoads                int
	ZipperLeafLogNodeLoads              int
	ZipperLeafLogCacheHits              int
	ZipperLeafLogReaderCalls            int
	ZipperLeafLogViewReads              int
	ZipperLeafLogScratchReads           int
	ZipperPagerNodeBytesRead            int
	ZipperLeafLogNodeBytesRead          int
	ZipperLeafLogRecordHintBytesRead    int
	ZipperLeafMerges                    int
	ZipperInternalMerges                int
	ZipperInternalParallelMerges        int
	ZipperInternalParallelChildren      int
	ZipperInternalParallelWorkers       int
	ZipperInternalParallelOps           int
	ZipperLeafPagesWritten              int
	ZipperPagerLeafPagesWritten         int
	ZipperLeafLogPagesWritten           int
	ZipperLeafPageBytesWritten          int
	ZipperPagerLeafPageBytesWritten     int
	ZipperLeafLogPageBytesWritten       int
	ZipperLeafLogRecordHintBytesWritten int
	ZipperInternalPagesWritten          int
	ZipperInternalPageBytesWritten      int
	ZipperInternalChildRefs             int
	ZipperInternalPageChildRefs         int
	ZipperInternalLeafLogRefs           int
	ZipperInternalLeafLogRefCopies      int
	ZipperRootSplitLevels               int
}

func (dst *orderedRootDeltaGroupZipperStats) add(src adaptive.Metrics) {
	if dst == nil {
		return
	}
	dst.ZipperApplyOps += src.ZipperApplyOps
	dst.ZipperNodeLoads += src.ZipperNodeLoads
	dst.ZipperPagerNodeLoads += src.ZipperPagerNodeLoads
	dst.ZipperLeafLogNodeLoads += src.ZipperLeafLogNodeLoads
	dst.ZipperLeafLogCacheHits += src.ZipperLeafLogCacheHits
	dst.ZipperLeafLogReaderCalls += src.ZipperLeafLogReaderCalls
	dst.ZipperLeafLogViewReads += src.ZipperLeafLogViewReads
	dst.ZipperLeafLogScratchReads += src.ZipperLeafLogScratchReads
	dst.ZipperPagerNodeBytesRead += src.ZipperPagerNodeBytesRead
	dst.ZipperLeafLogNodeBytesRead += src.ZipperLeafLogNodeBytesRead
	dst.ZipperLeafLogRecordHintBytesRead += src.ZipperLeafLogRecordHintBytesRead
	dst.ZipperLeafMerges += src.ZipperLeafMerges
	dst.ZipperInternalMerges += src.ZipperInternalMerges
	dst.ZipperInternalParallelMerges += src.ZipperInternalParallelMerges
	dst.ZipperInternalParallelChildren += src.ZipperInternalParallelChildren
	dst.ZipperInternalParallelWorkers += src.ZipperInternalParallelWorkers
	dst.ZipperInternalParallelOps += src.ZipperInternalParallelOps
	dst.ZipperLeafPagesWritten += src.ZipperLeafPagesWritten
	dst.ZipperPagerLeafPagesWritten += src.ZipperPagerLeafPagesWritten
	dst.ZipperLeafLogPagesWritten += src.ZipperLeafLogPagesWritten
	dst.ZipperLeafPageBytesWritten += src.ZipperLeafPageBytesWritten
	dst.ZipperPagerLeafPageBytesWritten += src.ZipperPagerLeafPageBytesWritten
	dst.ZipperLeafLogPageBytesWritten += src.ZipperLeafLogPageBytesWritten
	dst.ZipperLeafLogRecordHintBytesWritten += src.ZipperLeafLogRecordHintBytesWritten
	dst.ZipperInternalPagesWritten += src.ZipperInternalPagesWritten
	dst.ZipperInternalPageBytesWritten += src.ZipperInternalPageBytesWritten
	dst.ZipperInternalChildRefs += src.ZipperInternalChildRefs
	dst.ZipperInternalPageChildRefs += src.ZipperInternalPageChildRefs
	dst.ZipperInternalLeafLogRefs += src.ZipperInternalLeafLogRefs
	dst.ZipperInternalLeafLogRefCopies += src.ZipperInternalLeafLogRefCopies
	dst.ZipperRootSplitLevels += src.ZipperRootSplitLevels
}

func orderedRootDeltaGroupMetricUint(v int) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

func orderedRootDeltaGroupPhaseDurationNs(start time.Time) uint64 {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed.Nanoseconds())
}

func (db *DB) observeOrderedRootDeltaGroupPublish(wait, hold time.Duration, roots int, phases orderedRootDeltaGroupPublishPhaseStats, err error) {
	if db == nil {
		return
	}
	if wait < 0 {
		wait = 0
	}
	if hold < 0 {
		hold = 0
	}
	if roots < 0 {
		roots = 0
	}

	waitNs := uint64(wait.Nanoseconds())
	holdNs := uint64(hold.Nanoseconds())
	latNs := waitNs + holdNs
	if waitNs > maxTimeDurationNs-holdNs {
		latNs = maxTimeDurationNs
	}
	latency := durationFromUint64Ns(latNs)

	db.orderedRootDeltaGroupCalls.Add(1)
	if err != nil {
		db.orderedRootDeltaGroupErrors.Add(1)
	} else {
		db.orderedRootDeltaGroupRoots.Add(uint64(roots))
	}
	db.orderedRootDeltaGroupWaitTotalNs.Add(waitNs)
	db.orderedRootDeltaGroupHoldTotalNs.Add(holdNs)
	db.orderedRootDeltaGroupPreflightNs.Add(phases.preflightNs)
	db.orderedRootDeltaGroupRootApplyNs.Add(phases.rootApplyNs)
	db.orderedRootDeltaGroupRootApplyCalls.Add(phases.rootApplyCalls)
	db.orderedRootDeltaGroupRootApplyParallelGroups.Add(phases.rootApplyParallelGroups)
	db.orderedRootDeltaGroupRootApplyParallelRoots.Add(phases.rootApplyParallelRoots)
	db.orderedRootDeltaGroupRootApplyOps.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperApplyOps))
	db.orderedRootDeltaGroupRootApplyNodeLoads.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperNodeLoads))
	db.orderedRootDeltaGroupRootApplyPagerNodeLoads.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperPagerNodeLoads))
	db.orderedRootDeltaGroupRootApplyLeafLogNodeLoads.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogNodeLoads))
	db.orderedRootDeltaGroupRootApplyLeafLogCacheHits.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogCacheHits))
	db.orderedRootDeltaGroupRootApplyLeafLogReaderCalls.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogReaderCalls))
	db.orderedRootDeltaGroupRootApplyLeafLogViewReads.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogViewReads))
	db.orderedRootDeltaGroupRootApplyLeafLogScratchReads.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogScratchReads))
	db.orderedRootDeltaGroupRootApplyPagerNodeBytesRead.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperPagerNodeBytesRead))
	db.orderedRootDeltaGroupRootApplyLeafLogNodeBytesRead.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogNodeBytesRead))
	db.orderedRootDeltaGroupRootApplyLeafLogRecordHintBytesRead.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogRecordHintBytesRead))
	db.orderedRootDeltaGroupRootApplyLeafMerges.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafMerges))
	db.orderedRootDeltaGroupRootApplyInternalMerges.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalMerges))
	db.orderedRootDeltaGroupRootApplyInternalParallelMerges.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalParallelMerges))
	db.orderedRootDeltaGroupRootApplyInternalParallelChildren.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalParallelChildren))
	db.orderedRootDeltaGroupRootApplyInternalParallelWorkers.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalParallelWorkers))
	db.orderedRootDeltaGroupRootApplyInternalParallelOps.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalParallelOps))
	db.orderedRootDeltaGroupRootApplyLeafPagesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafPagesWritten))
	db.orderedRootDeltaGroupRootApplyPagerLeafPagesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperPagerLeafPagesWritten))
	db.orderedRootDeltaGroupRootApplyLeafLogPagesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogPagesWritten))
	db.orderedRootDeltaGroupRootApplyLeafPageBytesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafPageBytesWritten))
	db.orderedRootDeltaGroupRootApplyPagerLeafPageBytesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperPagerLeafPageBytesWritten))
	db.orderedRootDeltaGroupRootApplyLeafLogPageBytesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogPageBytesWritten))
	db.orderedRootDeltaGroupRootApplyLeafLogRecordHintBytesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperLeafLogRecordHintBytesWritten))
	db.orderedRootDeltaGroupRootApplyInternalPagesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalPagesWritten))
	db.orderedRootDeltaGroupRootApplyInternalPageBytesWritten.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalPageBytesWritten))
	db.orderedRootDeltaGroupRootApplyInternalChildRefs.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalChildRefs))
	db.orderedRootDeltaGroupRootApplyInternalPageChildRefs.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalPageChildRefs))
	db.orderedRootDeltaGroupRootApplyInternalLeafLogRefs.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalLeafLogRefs))
	db.orderedRootDeltaGroupRootApplyInternalLeafLogRefCopies.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperInternalLeafLogRefCopies))
	db.orderedRootDeltaGroupRootApplyRootSplitLevels.Add(orderedRootDeltaGroupMetricUint(phases.rootApplyMetrics.ZipperRootSplitLevels))
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareNs.Add(phases.rootApplyReadOnlyPrepareNs)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareCalls.Add(phases.rootApplyReadOnlyPrepareCalls)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareErrors.Add(phases.rootApplyReadOnlyPrepareErrors)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareValidationFail.Add(phases.rootApplyReadOnlyPrepareValidationFail)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareRequested.Add(phases.rootApplyReadOnlyPrepareRequested)
	storeUint64Max(&db.orderedRootDeltaGroupRootApplyReadOnlyPrepareRequestedMax, phases.rootApplyReadOnlyPrepareRequestedMax)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareSpans.Add(phases.rootApplyReadOnlyPrepareSpans)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareSpanOps.Add(phases.rootApplyReadOnlyPrepareSpanOps)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareSpanBytes.Add(phases.rootApplyReadOnlyPrepareSpanBytes)
	db.orderedRootDeltaGroupRootApplyReadOnlyPrepareWorkerRanges.Add(phases.rootApplyReadOnlyPrepareWorkerRanges)
	db.orderedRootDeltaGroupSystemBuildNs.Add(phases.systemBuildNs)
	db.orderedRootDeltaGroupSystemApplyNs.Add(phases.systemApplyNs)
	db.orderedRootDeltaGroupSystemApplyCalls.Add(phases.systemApplyCalls)
	db.orderedRootDeltaGroupSystemApplyOps.Add(orderedRootDeltaGroupMetricUint(phases.systemApplyMetrics.ZipperApplyOps))
	db.orderedRootDeltaGroupSystemApplyNodeLoads.Add(orderedRootDeltaGroupMetricUint(phases.systemApplyMetrics.ZipperNodeLoads))
	db.orderedRootDeltaGroupPublishPrepareNs.Add(phases.publishPrepareNs)
	db.orderedRootDeltaGroupPublishPrepareCalls.Add(phases.publishPrepareCalls)
	db.orderedRootDeltaGroupPublishPrepareErrors.Add(phases.publishPrepareErrors)
	db.orderedRootDeltaGroupFinalizeNs.Add(phases.finalizeNs)
	db.orderedRootDeltaGroupFinalizeCalls.Add(phases.finalizeCalls)
	for {
		cur := db.orderedRootDeltaGroupLatencyMaxNs.Load()
		if latNs <= cur || db.orderedRootDeltaGroupLatencyMaxNs.CompareAndSwap(cur, latNs) {
			break
		}
	}
	bucket := publishWatermarkLatencyBucketIndex(latency)
	db.orderedRootDeltaGroupLatencyBuckets[bucket].Add(1)
}

func (db *DB) orderedRootDeltaGroupPublishStats() orderedRootDeltaGroupPublishStats {
	if db == nil {
		return orderedRootDeltaGroupPublishStats{}
	}
	calls := db.orderedRootDeltaGroupCalls.Load()
	waitNs := db.orderedRootDeltaGroupWaitTotalNs.Load()
	holdNs := db.orderedRootDeltaGroupHoldTotalNs.Load()
	roots := db.orderedRootDeltaGroupRoots.Load()
	stats := orderedRootDeltaGroupPublishStats{
		calls:                                  calls,
		errors:                                 db.orderedRootDeltaGroupErrors.Load(),
		roots:                                  roots,
		waitTotalNs:                            waitNs,
		holdTotalNs:                            holdNs,
		latencyMax:                             durationFromUint64Ns(db.orderedRootDeltaGroupLatencyMaxNs.Load()),
		preflightNs:                            db.orderedRootDeltaGroupPreflightNs.Load(),
		rootApplyNs:                            db.orderedRootDeltaGroupRootApplyNs.Load(),
		rootApplyCalls:                         db.orderedRootDeltaGroupRootApplyCalls.Load(),
		rootApplyParallelGroups:                db.orderedRootDeltaGroupRootApplyParallelGroups.Load(),
		rootApplyParallelRoots:                 db.orderedRootDeltaGroupRootApplyParallelRoots.Load(),
		rootApplyOps:                           db.orderedRootDeltaGroupRootApplyOps.Load(),
		rootApplyNodeLoads:                     db.orderedRootDeltaGroupRootApplyNodeLoads.Load(),
		rootApplyPagerNodeLoads:                db.orderedRootDeltaGroupRootApplyPagerNodeLoads.Load(),
		rootApplyLeafLogNodeLoads:              db.orderedRootDeltaGroupRootApplyLeafLogNodeLoads.Load(),
		rootApplyLeafLogCacheHits:              db.orderedRootDeltaGroupRootApplyLeafLogCacheHits.Load(),
		rootApplyLeafLogReaderCalls:            db.orderedRootDeltaGroupRootApplyLeafLogReaderCalls.Load(),
		rootApplyLeafLogViewReads:              db.orderedRootDeltaGroupRootApplyLeafLogViewReads.Load(),
		rootApplyLeafLogScratchReads:           db.orderedRootDeltaGroupRootApplyLeafLogScratchReads.Load(),
		rootApplyPagerNodeBytesRead:            db.orderedRootDeltaGroupRootApplyPagerNodeBytesRead.Load(),
		rootApplyLeafLogNodeBytesRead:          db.orderedRootDeltaGroupRootApplyLeafLogNodeBytesRead.Load(),
		rootApplyLeafLogRecordHintBytesRead:    db.orderedRootDeltaGroupRootApplyLeafLogRecordHintBytesRead.Load(),
		rootApplyLeafMerges:                    db.orderedRootDeltaGroupRootApplyLeafMerges.Load(),
		rootApplyInternalMerges:                db.orderedRootDeltaGroupRootApplyInternalMerges.Load(),
		rootApplyInternalParallelMerges:        db.orderedRootDeltaGroupRootApplyInternalParallelMerges.Load(),
		rootApplyInternalParallelChildren:      db.orderedRootDeltaGroupRootApplyInternalParallelChildren.Load(),
		rootApplyInternalParallelWorkers:       db.orderedRootDeltaGroupRootApplyInternalParallelWorkers.Load(),
		rootApplyInternalParallelOps:           db.orderedRootDeltaGroupRootApplyInternalParallelOps.Load(),
		rootApplyLeafPagesWritten:              db.orderedRootDeltaGroupRootApplyLeafPagesWritten.Load(),
		rootApplyPagerLeafPagesWritten:         db.orderedRootDeltaGroupRootApplyPagerLeafPagesWritten.Load(),
		rootApplyLeafLogPagesWritten:           db.orderedRootDeltaGroupRootApplyLeafLogPagesWritten.Load(),
		rootApplyLeafPageBytesWritten:          db.orderedRootDeltaGroupRootApplyLeafPageBytesWritten.Load(),
		rootApplyPagerLeafPageBytesWritten:     db.orderedRootDeltaGroupRootApplyPagerLeafPageBytesWritten.Load(),
		rootApplyLeafLogPageBytesWritten:       db.orderedRootDeltaGroupRootApplyLeafLogPageBytesWritten.Load(),
		rootApplyLeafLogRecordHintBytesWritten: db.orderedRootDeltaGroupRootApplyLeafLogRecordHintBytesWritten.Load(),
		rootApplyInternalPagesWritten:          db.orderedRootDeltaGroupRootApplyInternalPagesWritten.Load(),
		rootApplyInternalPageBytesWritten:      db.orderedRootDeltaGroupRootApplyInternalPageBytesWritten.Load(),
		rootApplyInternalChildRefs:             db.orderedRootDeltaGroupRootApplyInternalChildRefs.Load(),
		rootApplyInternalPageChildRefs:         db.orderedRootDeltaGroupRootApplyInternalPageChildRefs.Load(),
		rootApplyInternalLeafLogRefs:           db.orderedRootDeltaGroupRootApplyInternalLeafLogRefs.Load(),
		rootApplyInternalLeafLogRefCopies:      db.orderedRootDeltaGroupRootApplyInternalLeafLogRefCopies.Load(),
		rootApplyRootSplitLevels:               db.orderedRootDeltaGroupRootApplyRootSplitLevels.Load(),
		rootApplyReadOnlyPrepareNs:             db.orderedRootDeltaGroupRootApplyReadOnlyPrepareNs.Load(),
		rootApplyReadOnlyPrepareCalls:          db.orderedRootDeltaGroupRootApplyReadOnlyPrepareCalls.Load(),
		rootApplyReadOnlyPrepareErrors:         db.orderedRootDeltaGroupRootApplyReadOnlyPrepareErrors.Load(),
		rootApplyReadOnlyPrepareValidationFail: db.orderedRootDeltaGroupRootApplyReadOnlyPrepareValidationFail.Load(),
		rootApplyReadOnlyPrepareRequested:      db.orderedRootDeltaGroupRootApplyReadOnlyPrepareRequested.Load(),
		rootApplyReadOnlyPrepareRequestedMax:   db.orderedRootDeltaGroupRootApplyReadOnlyPrepareRequestedMax.Load(),
		rootApplyReadOnlyPrepareSpans:          db.orderedRootDeltaGroupRootApplyReadOnlyPrepareSpans.Load(),
		rootApplyReadOnlyPrepareSpanOps:        db.orderedRootDeltaGroupRootApplyReadOnlyPrepareSpanOps.Load(),
		rootApplyReadOnlyPrepareSpanBytes:      db.orderedRootDeltaGroupRootApplyReadOnlyPrepareSpanBytes.Load(),
		rootApplyReadOnlyPrepareWorkerRanges:   db.orderedRootDeltaGroupRootApplyReadOnlyPrepareWorkerRanges.Load(),
		systemBuildNs:                          db.orderedRootDeltaGroupSystemBuildNs.Load(),
		systemApplyNs:                          db.orderedRootDeltaGroupSystemApplyNs.Load(),
		systemApplyCalls:                       db.orderedRootDeltaGroupSystemApplyCalls.Load(),
		systemApplyOps:                         db.orderedRootDeltaGroupSystemApplyOps.Load(),
		systemApplyNodeLoads:                   db.orderedRootDeltaGroupSystemApplyNodeLoads.Load(),
		publishPrepareNs:                       db.orderedRootDeltaGroupPublishPrepareNs.Load(),
		publishPrepareCalls:                    db.orderedRootDeltaGroupPublishPrepareCalls.Load(),
		publishPrepareErrors:                   db.orderedRootDeltaGroupPublishPrepareErrors.Load(),
		finalizeNs:                             db.orderedRootDeltaGroupFinalizeNs.Load(),
		finalizeCalls:                          db.orderedRootDeltaGroupFinalizeCalls.Load(),
	}
	if calls > 0 {
		stats.avgRootsPerCall = float64(roots) / float64(calls)
	}
	if denom := float64(waitNs) + float64(holdNs); denom > 0 {
		stats.writeLockWaitShare = 100 * float64(waitNs) / denom
	}
	if calls == 0 {
		return stats
	}
	var buckets [publishWatermarkLatencyBucketCount]uint64
	var bucketSamples uint64
	for i := range buckets {
		buckets[i] = db.orderedRootDeltaGroupLatencyBuckets[i].Load()
		bucketSamples += buckets[i]
	}
	if bucketSamples == 0 {
		if stats.latencyMax > 0 {
			stats.latencyP99 = stats.latencyMax
		}
		return stats
	}
	stats.latencyP99 = estimatePublishWatermarkPercentile(buckets, bucketSamples, 0.99)
	if stats.latencyP99 <= 0 && stats.latencyMax > 0 {
		stats.latencyP99 = stats.latencyMax
	}
	return stats
}
