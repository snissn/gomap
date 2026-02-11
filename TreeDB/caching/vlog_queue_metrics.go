package caching

import (
	"math"
	"sync/atomic"
	"time"
)

const vlogQueueLagBucketCount = 13

var vlogQueueLagBucketUpperBounds = [vlogQueueLagBucketCount]time.Duration{
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

type vlogQueueLagSnapshot struct {
	Count   uint64
	TotalNs uint64
	MaxNs   uint64
	Buckets [vlogQueueLagBucketCount]uint64
}

type vlogQueueDepthSnapshot struct {
	Enqueued         uint64
	Samples          uint64
	Sum              uint64
	Max              uint64
	Last             uint64
	PositiveRunMaxNs uint64
}

func vlogQueueLagBucketIndex(lag time.Duration) int {
	if lag <= 0 {
		return 0
	}
	for i, upper := range vlogQueueLagBucketUpperBounds {
		if lag <= upper {
			return i
		}
	}
	return vlogQueueLagBucketCount - 1
}

func observeLaneVlogQueueEnqueue(l *lane, depth int) {
	if l == nil {
		return
	}
	l.vlogQueueEnqueued.Add(1)
	observeLaneVlogQueueDepthAtomic(l, depth)
}

func observeLaneVlogQueueDepthSample(l *lane, depth int) {
	if l == nil {
		return
	}
	observeLaneVlogQueueDepthAtomic(l, depth)

	now := time.Now().UnixNano()
	if last := l.vlogQueueDriftLastAtNs; last > 0 {
		if depth > l.vlogQueueDriftLastDepth {
			delta := now - last
			if delta > 0 {
				l.vlogQueueDriftCurrentNs += uint64(delta)
				updateAtomicMax(&l.vlogQueuePositiveRunMax, l.vlogQueueDriftCurrentNs)
			}
		} else {
			l.vlogQueueDriftCurrentNs = 0
		}
	}
	l.vlogQueueDriftLastDepth = depth
	l.vlogQueueDriftLastAtNs = now
}

func observeLaneVlogQueueDepthAtomic(l *lane, depth int) {
	if l == nil {
		return
	}
	if depth < 0 {
		depth = 0
	}
	du := uint64(depth)
	l.vlogQueueDepthLast.Store(du)
	l.vlogQueueDepthSamples.Add(1)
	l.vlogQueueDepthSum.Add(du)
	updateAtomicMax(&l.vlogQueueDepthMax, du)
}

func observeLaneVlogQueueLag(l *lane, lag time.Duration) {
	if l == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	ns := uint64(lag.Nanoseconds())
	l.vlogQueueLagCount.Add(1)
	l.vlogQueueLagTotalNs.Add(ns)
	updateAtomicMax(&l.vlogQueueLagMaxNs, ns)
	bucket := vlogQueueLagBucketIndex(lag)
	l.vlogQueueLagBuckets[bucket].Add(1)
}

func updateAtomicMax(dst *atomic.Uint64, candidate uint64) {
	if dst == nil {
		return
	}
	for {
		cur := dst.Load()
		if candidate <= cur {
			return
		}
		if dst.CompareAndSwap(cur, candidate) {
			return
		}
	}
}

func snapshotLaneVlogQueueLag(l *lane) vlogQueueLagSnapshot {
	out := vlogQueueLagSnapshot{}
	if l == nil {
		return out
	}
	out.Count = l.vlogQueueLagCount.Load()
	out.TotalNs = l.vlogQueueLagTotalNs.Load()
	out.MaxNs = l.vlogQueueLagMaxNs.Load()
	for i := 0; i < vlogQueueLagBucketCount; i++ {
		out.Buckets[i] = l.vlogQueueLagBuckets[i].Load()
	}
	return out
}

func snapshotLaneVlogQueueDepth(l *lane) vlogQueueDepthSnapshot {
	out := vlogQueueDepthSnapshot{}
	if l == nil {
		return out
	}
	out.Enqueued = l.vlogQueueEnqueued.Load()
	out.Samples = l.vlogQueueDepthSamples.Load()
	out.Sum = l.vlogQueueDepthSum.Load()
	out.Max = l.vlogQueueDepthMax.Load()
	out.Last = l.vlogQueueDepthLast.Load()
	out.PositiveRunMaxNs = l.vlogQueuePositiveRunMax.Load()
	return out
}

func estimateVlogQueueLagPercentile(buckets [vlogQueueLagBucketCount]uint64, total uint64, p float64) time.Duration {
	if total == 0 {
		return 0
	}
	if p <= 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	target := uint64(math.Ceil(float64(total) * p))
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i := 0; i < vlogQueueLagBucketCount; i++ {
		seen += buckets[i]
		if seen >= target {
			return vlogQueueLagBucketUpperBounds[i]
		}
	}
	return vlogQueueLagBucketUpperBounds[vlogQueueLagBucketCount-1]
}
