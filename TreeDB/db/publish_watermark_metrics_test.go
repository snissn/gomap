package db

import (
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
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
	db.observeOrderedRootDeltaGroupPublish(2*time.Millisecond, 3*time.Millisecond, 4, orderedRootDeltaGroupPublishPhaseStats{
		rootApplyNs:      uint64((7 * time.Millisecond).Nanoseconds()),
		rootApplyCalls:   4,
		systemBuildNs:    uint64((200 * time.Microsecond).Nanoseconds()),
		systemApplyNs:    uint64((800 * time.Microsecond).Nanoseconds()),
		systemApplyCalls: 1,
		finalizeNs:       uint64((300 * time.Microsecond).Nanoseconds()),
		finalizeCalls:    1,
	}, nil)
	db.observeOrderedRootDeltaGroupPublish(time.Millisecond, time.Millisecond, 2, orderedRootDeltaGroupPublishPhaseStats{
		preflightNs:    uint64((100 * time.Microsecond).Nanoseconds()),
		rootApplyNs:    uint64((2 * time.Millisecond).Nanoseconds()),
		rootApplyCalls: 1,
	}, errTestFinalizeCommitFailpoint)

	stats := db.orderedRootDeltaGroupPublishStats()
	if stats.calls != 2 {
		t.Fatalf("calls=%d want 2", stats.calls)
	}
	if stats.errors != 1 {
		t.Fatalf("errors=%d want 1", stats.errors)
	}
	if stats.roots != 4 {
		t.Fatalf("roots=%d want 4", stats.roots)
	}
	if stats.waitTotalNs != uint64((3 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("waitTotalNs=%d", stats.waitTotalNs)
	}
	if stats.holdTotalNs != uint64((4 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("holdTotalNs=%d", stats.holdTotalNs)
	}
	if stats.avgRootsPerCall != 2 {
		t.Fatalf("avgRootsPerCall=%f want 2", stats.avgRootsPerCall)
	}
	if stats.writeLockWaitShare <= 0 || stats.writeLockWaitShare >= 100 {
		t.Fatalf("writeLockWaitShare=%f want between 0 and 100", stats.writeLockWaitShare)
	}
	if stats.latencyP99 <= 0 {
		t.Fatalf("latencyP99=%v want >0", stats.latencyP99)
	}
	if stats.preflightNs != uint64((100 * time.Microsecond).Nanoseconds()) {
		t.Fatalf("preflightNs=%d", stats.preflightNs)
	}
	if stats.rootApplyNs != uint64((9 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("rootApplyNs=%d", stats.rootApplyNs)
	}
	if stats.rootApplyCalls != 5 {
		t.Fatalf("rootApplyCalls=%d want 5", stats.rootApplyCalls)
	}
	if stats.systemBuildNs != uint64((200 * time.Microsecond).Nanoseconds()) {
		t.Fatalf("systemBuildNs=%d", stats.systemBuildNs)
	}
	if stats.systemApplyNs != uint64((800 * time.Microsecond).Nanoseconds()) {
		t.Fatalf("systemApplyNs=%d", stats.systemApplyNs)
	}
	if stats.systemApplyCalls != 1 {
		t.Fatalf("systemApplyCalls=%d want 1", stats.systemApplyCalls)
	}
	if stats.finalizeNs != uint64((300 * time.Microsecond).Nanoseconds()) {
		t.Fatalf("finalizeNs=%d", stats.finalizeNs)
	}
	if stats.finalizeCalls != 1 {
		t.Fatalf("finalizeCalls=%d want 1", stats.finalizeCalls)
	}
}

func TestMergeOrderedRootPublishMetricsIncludesLeafLogAttribution(t *testing.T) {
	src := adaptive.Metrics{
		ZipperApplyOps:                      1,
		ZipperNodeLoads:                     2,
		ZipperPagerNodeLoads:                3,
		ZipperLeafLogNodeLoads:              4,
		ZipperLeafLogCacheHits:              5,
		ZipperLeafLogReaderCalls:            6,
		ZipperLeafLogViewReads:              7,
		ZipperLeafLogScratchReads:           8,
		ZipperPagerNodeBytesRead:            9,
		ZipperLeafLogNodeBytesRead:          10,
		ZipperLeafLogRecordHintBytesRead:    11,
		ZipperLeafMerges:                    12,
		ZipperInternalMerges:                13,
		ZipperLeafPagesWritten:              14,
		ZipperPagerLeafPagesWritten:         15,
		ZipperLeafLogPagesWritten:           16,
		ZipperLeafPageBytesWritten:          17,
		ZipperPagerLeafPageBytesWritten:     18,
		ZipperLeafLogPageBytesWritten:       19,
		ZipperLeafLogRecordHintBytesWritten: 20,
		ZipperInternalPagesWritten:          21,
		ZipperInternalPageBytesWritten:      22,
		ZipperInternalChildRefs:             23,
		ZipperInternalPageChildRefs:         24,
		ZipperInternalLeafLogRefs:           25,
		ZipperInternalLeafLogRefCopies:      26,
		ZipperRootSplitLevels:               27,
	}

	var dst adaptive.Metrics
	mergeOrderedRootPublishMetrics(&dst, src)
	mergeOrderedRootPublishMetrics(&dst, src)

	want := src
	want.ZipperApplyOps *= 2
	want.ZipperNodeLoads *= 2
	want.ZipperPagerNodeLoads *= 2
	want.ZipperLeafLogNodeLoads *= 2
	want.ZipperLeafLogCacheHits *= 2
	want.ZipperLeafLogReaderCalls *= 2
	want.ZipperLeafLogViewReads *= 2
	want.ZipperLeafLogScratchReads *= 2
	want.ZipperPagerNodeBytesRead *= 2
	want.ZipperLeafLogNodeBytesRead *= 2
	want.ZipperLeafLogRecordHintBytesRead *= 2
	want.ZipperLeafMerges *= 2
	want.ZipperInternalMerges *= 2
	want.ZipperLeafPagesWritten *= 2
	want.ZipperPagerLeafPagesWritten *= 2
	want.ZipperLeafLogPagesWritten *= 2
	want.ZipperLeafPageBytesWritten *= 2
	want.ZipperPagerLeafPageBytesWritten *= 2
	want.ZipperLeafLogPageBytesWritten *= 2
	want.ZipperLeafLogRecordHintBytesWritten *= 2
	want.ZipperInternalPagesWritten *= 2
	want.ZipperInternalPageBytesWritten *= 2
	want.ZipperInternalChildRefs *= 2
	want.ZipperInternalPageChildRefs *= 2
	want.ZipperInternalLeafLogRefs *= 2
	want.ZipperInternalLeafLogRefCopies *= 2
	want.ZipperRootSplitLevels *= 2

	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("merged metrics mismatch\n got: %+v\nwant: %+v", dst, want)
	}
}

func TestOrderedRootDeltaGroupPublishStatsUsesBucketSnapshotForPercentile(t *testing.T) {
	db := &DB{}
	db.orderedRootDeltaGroupCalls.Store(100)
	db.orderedRootDeltaGroupLatencyMaxNs.Store(uint64(time.Millisecond))
	db.orderedRootDeltaGroupLatencyBuckets[0].Store(1)

	stats := db.orderedRootDeltaGroupPublishStats()
	if stats.latencyP99 != 50*time.Microsecond {
		t.Fatalf("latencyP99=%v want first bucket bound", stats.latencyP99)
	}
}

func TestDurationFromUint64NsClampsOverflow(t *testing.T) {
	for _, ns := range []uint64{maxTimeDurationNs + 1, ^uint64(0)} {
		if got := durationFromUint64Ns(ns); got != time.Duration(maxTimeDurationNs) {
			t.Fatalf("durationFromUint64Ns(%d)=%v want %v", ns, got, time.Duration(maxTimeDurationNs))
		}
	}
}
