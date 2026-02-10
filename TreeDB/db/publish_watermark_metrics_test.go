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
