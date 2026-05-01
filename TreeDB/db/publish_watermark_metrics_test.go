package db

import (
	"testing"
	"time"
)

func TestEstimatePublishWatermarkPercentile_OverflowBucket(t *testing.T) {
	var buckets [publishWatermarkLatencyBucketCount]uint64
	buckets[publishWatermarkLatencyBucketCount-1] = 10
	got := estimatePublishWatermarkPercentile(buckets, 10, 0.99)
	last := publishWatermarkLatencyBucketUpperBounds[len(publishWatermarkLatencyBucketUpperBounds)-1]
	if got <= last {
		t.Fatalf("expected overflow percentile > %v, got %v", last, got)
	}
}

func TestEstimatePublishWatermarkPercentile_NonOverflow(t *testing.T) {
	var buckets [publishWatermarkLatencyBucketCount]uint64
	buckets[0] = 5
	buckets[1] = 5
	got := estimatePublishWatermarkPercentile(buckets, 10, 0.5)
	if got != 50*time.Microsecond {
		t.Fatalf("expected first bucket bound, got %v", got)
	}
}

func TestObserveOrderedRootDeltaGroupPublishStats(t *testing.T) {
	db := &DB{}
	db.observeOrderedRootDeltaGroupPublish(2*time.Millisecond, 3*time.Millisecond, 4, nil)
	db.observeOrderedRootDeltaGroupPublish(time.Millisecond, time.Millisecond, 2, errTestFinalizeCommitFailpoint)

	stats := db.orderedRootDeltaGroupPublishStats()
	if stats.calls != 2 {
		t.Fatalf("calls=%d want 2", stats.calls)
	}
	if stats.errors != 1 {
		t.Fatalf("errors=%d want 1", stats.errors)
	}
	if stats.roots != 6 {
		t.Fatalf("roots=%d want 6", stats.roots)
	}
	if stats.waitTotalNs != uint64((3 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("waitTotalNs=%d", stats.waitTotalNs)
	}
	if stats.holdTotalNs != uint64((4 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("holdTotalNs=%d", stats.holdTotalNs)
	}
	if stats.avgRootsPerCall != 3 {
		t.Fatalf("avgRootsPerCall=%f want 3", stats.avgRootsPerCall)
	}
	if stats.writeLockWaitShare <= 0 || stats.writeLockWaitShare >= 100 {
		t.Fatalf("writeLockWaitShare=%f want between 0 and 100", stats.writeLockWaitShare)
	}
	if stats.latencyP99 <= 0 {
		t.Fatalf("latencyP99=%v want >0", stats.latencyP99)
	}
}
