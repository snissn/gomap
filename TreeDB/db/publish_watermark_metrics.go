package db

import "time"

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
			p99 = time.Duration(maxNs)
		}
	}
	latencyP99Ms = float64(p99) / float64(time.Millisecond)
	return lockDelaySharePct, latencyP99Ms
}
