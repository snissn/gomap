package tree

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestOuterLeafReadStatsRecentEstimator(t *testing.T) {
	savedSampleMod := outerLeafReadSampleMod
	savedEstimator := outerLeafRecentReadEstimator
	outerLeafLoadsTotal.Store(0)
	outerLeafPointLoadsTotal.Store(0)
	outerLeafIteratorLoadsTotal.Store(0)
	outerLeafBytesTotal.Store(0)
	outerLeafSamplesTotal.Store(0)
	outerLeafRecent64HitsTotal.Store(0)
	outerLeafRecent256HitsTotal.Store(0)
	outerLeafRecent1KHitsTotal.Store(0)
	outerLeafRecent4KHitsTotal.Store(0)
	outerLeafChecksumVerifiedTotal.Store(0)
	outerLeafChecksumSkippedTotal.Store(0)
	outerLeafReadSampleMod = 1
	outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()
	defer func() {
		outerLeafReadSampleMod = savedSampleMod
		outerLeafRecentReadEstimator = savedEstimator
	}()

	ptrA := page.ValuePtr{FileID: 7, Offset: 128}
	ptrB := page.ValuePtr{FileID: 7, Offset: 256}

	noteOuterLeafLoad(ptrA, page.PageSize, false)
	noteOuterLeafLoad(ptrB, page.PageSize, true)
	noteOuterLeafLoad(ptrA, page.PageSize, false)

	stats := OuterLeafReadStatsSnapshot()
	if stats.LoadsTotal != 3 {
		t.Fatalf("LoadsTotal=%d want 3", stats.LoadsTotal)
	}
	if stats.PointLoadsTotal != 2 {
		t.Fatalf("PointLoadsTotal=%d want 2", stats.PointLoadsTotal)
	}
	if stats.IteratorLoadsTotal != 1 {
		t.Fatalf("IteratorLoadsTotal=%d want 1", stats.IteratorLoadsTotal)
	}
	if stats.BytesTotal != uint64(3*page.PageSize) {
		t.Fatalf("BytesTotal=%d want %d", stats.BytesTotal, uint64(3*page.PageSize))
	}
	if stats.SamplesTotal != 3 {
		t.Fatalf("SamplesTotal=%d want 3", stats.SamplesTotal)
	}
	if stats.Recent64HitsTotal != 1 {
		t.Fatalf("Recent64HitsTotal=%d want 1", stats.Recent64HitsTotal)
	}
	if stats.Recent256HitsTotal != 1 {
		t.Fatalf("Recent256HitsTotal=%d want 1", stats.Recent256HitsTotal)
	}
	if stats.Recent1KHitsTotal != 1 {
		t.Fatalf("Recent1KHitsTotal=%d want 1", stats.Recent1KHitsTotal)
	}
	if stats.Recent4KHitsTotal != 1 {
		t.Fatalf("Recent4KHitsTotal=%d want 1", stats.Recent4KHitsTotal)
	}
}

func TestOuterLeafReadStatsEstimatorKeyAvoidsHighOffsetCollision(t *testing.T) {
	savedSampleMod := outerLeafReadSampleMod
	savedEstimator := outerLeafRecentReadEstimator
	outerLeafLoadsTotal.Store(0)
	outerLeafPointLoadsTotal.Store(0)
	outerLeafIteratorLoadsTotal.Store(0)
	outerLeafBytesTotal.Store(0)
	outerLeafSamplesTotal.Store(0)
	outerLeafRecent64HitsTotal.Store(0)
	outerLeafRecent256HitsTotal.Store(0)
	outerLeafRecent1KHitsTotal.Store(0)
	outerLeafRecent4KHitsTotal.Store(0)
	outerLeafChecksumVerifiedTotal.Store(0)
	outerLeafChecksumSkippedTotal.Store(0)
	outerLeafReadSampleMod = 1
	outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()
	defer func() {
		outerLeafReadSampleMod = savedSampleMod
		outerLeafRecentReadEstimator = savedEstimator
	}()

	ptrA := page.ValuePtr{FileID: 1, Offset: 0}
	ptrB := page.ValuePtr{FileID: 0, Offset: 1 << 32}

	noteOuterLeafLoad(ptrA, page.PageSize, false)
	noteOuterLeafLoad(ptrB, page.PageSize, false)

	stats := OuterLeafReadStatsSnapshot()
	if stats.Recent64HitsTotal != 0 {
		t.Fatalf("Recent64HitsTotal=%d want 0", stats.Recent64HitsTotal)
	}
	if stats.Recent256HitsTotal != 0 {
		t.Fatalf("Recent256HitsTotal=%d want 0", stats.Recent256HitsTotal)
	}
	if stats.Recent1KHitsTotal != 0 {
		t.Fatalf("Recent1KHitsTotal=%d want 0", stats.Recent1KHitsTotal)
	}
	if stats.Recent4KHitsTotal != 0 {
		t.Fatalf("Recent4KHitsTotal=%d want 0", stats.Recent4KHitsTotal)
	}
}
