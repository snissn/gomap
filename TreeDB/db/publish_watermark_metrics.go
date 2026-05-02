package db

import "time"

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
	calls              uint64
	errors             uint64
	roots              uint64
	waitTotalNs        uint64
	holdTotalNs        uint64
	preflightNs        uint64
	rootApplyNs        uint64
	rootApplyCalls     uint64
	systemBuildNs      uint64
	systemApplyNs      uint64
	systemApplyCalls   uint64
	finalizeNs         uint64
	finalizeCalls      uint64
	latencyP99         time.Duration
	latencyMax         time.Duration
	writeLockWaitShare float64
	avgRootsPerCall    float64
}

type orderedRootDeltaGroupPublishPhaseStats struct {
	preflightNs      uint64
	rootApplyNs      uint64
	rootApplyCalls   uint64
	systemBuildNs    uint64
	systemApplyNs    uint64
	systemApplyCalls uint64
	finalizeNs       uint64
	finalizeCalls    uint64
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
	db.orderedRootDeltaGroupSystemBuildNs.Add(phases.systemBuildNs)
	db.orderedRootDeltaGroupSystemApplyNs.Add(phases.systemApplyNs)
	db.orderedRootDeltaGroupSystemApplyCalls.Add(phases.systemApplyCalls)
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
		calls:            calls,
		errors:           db.orderedRootDeltaGroupErrors.Load(),
		roots:            roots,
		waitTotalNs:      waitNs,
		holdTotalNs:      holdNs,
		latencyMax:       durationFromUint64Ns(db.orderedRootDeltaGroupLatencyMaxNs.Load()),
		preflightNs:      db.orderedRootDeltaGroupPreflightNs.Load(),
		rootApplyNs:      db.orderedRootDeltaGroupRootApplyNs.Load(),
		rootApplyCalls:   db.orderedRootDeltaGroupRootApplyCalls.Load(),
		systemBuildNs:    db.orderedRootDeltaGroupSystemBuildNs.Load(),
		systemApplyNs:    db.orderedRootDeltaGroupSystemApplyNs.Load(),
		systemApplyCalls: db.orderedRootDeltaGroupSystemApplyCalls.Load(),
		finalizeNs:       db.orderedRootDeltaGroupFinalizeNs.Load(),
		finalizeCalls:    db.orderedRootDeltaGroupFinalizeCalls.Load(),
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
