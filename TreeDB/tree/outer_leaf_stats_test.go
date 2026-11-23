package tree

import (
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestOuterLeafReadStatsRecentEstimator(t *testing.T) {
	savedSampleMod := outerLeafReadSampleMod
	savedEstimator := outerLeafRecentReadEstimator
	resetOuterLeafReadStatsForTest()
	outerLeafReadSampleMod = 1
	outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()
	defer func() {
		outerLeafReadSampleMod = savedSampleMod
		outerLeafRecentReadEstimator = savedEstimator
	}()

	ptrA := page.ValuePtr{FileID: 7, Offset: 128}
	ptrB := page.ValuePtr{FileID: 7, Offset: 256}

	noteOuterLeafLoad(ptrA, false)
	noteOuterLeafLoad(ptrB, true)
	noteOuterLeafLoad(ptrA, false)

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
	resetOuterLeafReadStatsForTest()
	outerLeafReadSampleMod = 1
	outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()
	defer func() {
		outerLeafReadSampleMod = savedSampleMod
		outerLeafRecentReadEstimator = savedEstimator
	}()

	ptrA := page.ValuePtr{FileID: 1, Offset: 0}
	ptrB := page.ValuePtr{FileID: 0, Offset: 1 << 32}

	noteOuterLeafLoad(ptrA, false)
	noteOuterLeafLoad(ptrB, false)

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

func TestOuterLeafRecentEstimatorExactEquivalenceAndBoundedMap(t *testing.T) {
	type refRing struct {
		buf  []uint64
		next int
		used int
	}
	contains := func(r *refRing, k uint64) bool {
		for i := 0; i < r.used; i++ {
			if r.buf[i] == k {
				return true
			}
		}
		return false
	}
	push := func(r *refRing, k uint64) {
		r.buf[r.next] = k
		r.next = (r.next + 1) % len(r.buf)
		if r.used < len(r.buf) {
			r.used++
		}
	}

	r64 := &refRing{buf: make([]uint64, 64)}
	r256 := &refRing{buf: make([]uint64, 256)}
	r1K := &refRing{buf: make([]uint64, 1024)}
	r4K := &refRing{buf: make([]uint64, 4096)}
	est := newOuterLeafRecentReadEstimator()

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 12000; i++ {
		var key uint64
		switch i % 3 {
		case 0:
			key = uint64(rng.Intn(80))
		case 1:
			key = uint64(rng.Intn(1500))
		default:
			key = uint64(rng.Intn(9000))
		}
		w64 := contains(r64, key)
		w256 := contains(r256, key)
		w1K := contains(r1K, key)
		w4K := contains(r4K, key)
		push(r64, key)
		push(r256, key)
		push(r1K, key)
		push(r4K, key)

		g64, g256, g1K, g4K := est.observe(key)
		if g64 != w64 || g256 != w256 || g1K != w1K || g4K != w4K {
			t.Fatalf("step %d key=%d got=(%t,%t,%t,%t) want=(%t,%t,%t,%t)", i, key, g64, g256, g1K, g4K, w64, w256, w1K, w4K)
		}
		if len(est.lastSeen) > 4096 {
			t.Fatalf("step %d lastSeen exceeded 4096: %d", i, len(est.lastSeen))
		}
	}
}

func BenchmarkOuterLeafNoteLoadParallel(b *testing.B) {
	savedSampleMod := outerLeafReadSampleMod
	savedEstimator := outerLeafRecentReadEstimator
	resetOuterLeafReadStatsForTest()
	outerLeafReadSampleMod = 64
	outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()
	defer func() {
		outerLeafReadSampleMod = savedSampleMod
		outerLeafRecentReadEstimator = savedEstimator
	}()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			i++
			ptr := page.ValuePtr{FileID: uint32((i & 7) + 1), Offset: (i & 8191) * 4096}
			noteOuterLeafChecksumSkippedAt(ptr)
			noteOuterLeafLoad(ptr, false)
		}
	})
}
