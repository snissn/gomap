package tree

import (
	"math/bits"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/page"
)

var (
	outerLeafLoadsTotal            atomic.Uint64
	outerLeafPointLoadsTotal       atomic.Uint64
	outerLeafIteratorLoadsTotal    atomic.Uint64
	outerLeafBytesTotal            atomic.Uint64
	outerLeafSamplesTotal          atomic.Uint64
	outerLeafRecent64HitsTotal     atomic.Uint64
	outerLeafRecent256HitsTotal    atomic.Uint64
	outerLeafRecent1KHitsTotal     atomic.Uint64
	outerLeafRecent4KHitsTotal     atomic.Uint64
	outerLeafChecksumVerifiedTotal atomic.Uint64
	outerLeafChecksumSkippedTotal  atomic.Uint64
)

var outerLeafReadSampleMod = loadUintEnvDefault("TREEDB_OUTER_LEAF_READ_SAMPLE_MOD", 64)

var outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()

type OuterLeafReadStats struct {
	LoadsTotal            uint64
	PointLoadsTotal       uint64
	IteratorLoadsTotal    uint64
	BytesTotal            uint64
	SampleMod             uint64
	SamplesTotal          uint64
	Recent64HitsTotal     uint64
	Recent256HitsTotal    uint64
	Recent1KHitsTotal     uint64
	Recent4KHitsTotal     uint64
	ChecksumVerifiedTotal uint64
	ChecksumSkippedTotal  uint64
}

func OuterLeafReadStatsSnapshot() OuterLeafReadStats {
	return OuterLeafReadStats{
		LoadsTotal:            outerLeafLoadsTotal.Load(),
		PointLoadsTotal:       outerLeafPointLoadsTotal.Load(),
		IteratorLoadsTotal:    outerLeafIteratorLoadsTotal.Load(),
		BytesTotal:            outerLeafBytesTotal.Load(),
		SampleMod:             outerLeafReadSampleMod,
		SamplesTotal:          outerLeafSamplesTotal.Load(),
		Recent64HitsTotal:     outerLeafRecent64HitsTotal.Load(),
		Recent256HitsTotal:    outerLeafRecent256HitsTotal.Load(),
		Recent1KHitsTotal:     outerLeafRecent1KHitsTotal.Load(),
		Recent4KHitsTotal:     outerLeafRecent4KHitsTotal.Load(),
		ChecksumVerifiedTotal: outerLeafChecksumVerifiedTotal.Load(),
		ChecksumSkippedTotal:  outerLeafChecksumSkippedTotal.Load(),
	}
}

type outerLeafRecentEstimatorState struct {
	mu      sync.Mutex
	ring64  [64]uint64
	next64  int
	used64  int
	ring256 [256]uint64
	next256 int
	used256 int
	ring1K  [1024]uint64
	next1K  int
	used1K  int
	ring4K  [4096]uint64
	next4K  int
	used4K  int
}

func newOuterLeafRecentReadEstimator() *outerLeafRecentEstimatorState {
	return &outerLeafRecentEstimatorState{}
}

func (e *outerLeafRecentEstimatorState) observe(key uint64) (hit64, hit256, hit1K, hit4K bool) {
	if e == nil {
		return false, false, false, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	hit64 = outerLeafRecentRingContains(e.ring64[:], e.used64, key)
	hit256 = outerLeafRecentRingContains(e.ring256[:], e.used256, key)
	hit1K = outerLeafRecentRingContains(e.ring1K[:], e.used1K, key)
	hit4K = outerLeafRecentRingContains(e.ring4K[:], e.used4K, key)

	e.ring64[e.next64] = key
	e.next64 = (e.next64 + 1) % len(e.ring64)
	if e.used64 < len(e.ring64) {
		e.used64++
	}

	e.ring256[e.next256] = key
	e.next256 = (e.next256 + 1) % len(e.ring256)
	if e.used256 < len(e.ring256) {
		e.used256++
	}

	e.ring1K[e.next1K] = key
	e.next1K = (e.next1K + 1) % len(e.ring1K)
	if e.used1K < len(e.ring1K) {
		e.used1K++
	}

	e.ring4K[e.next4K] = key
	e.next4K = (e.next4K + 1) % len(e.ring4K)
	if e.used4K < len(e.ring4K) {
		e.used4K++
	}

	return hit64, hit256, hit1K, hit4K
}

func outerLeafRecentRingContains(ring []uint64, used int, key uint64) bool {
	for i := 0; i < used; i++ {
		if ring[i] == key {
			return true
		}
	}
	return false
}

func noteOuterLeafChecksumVerified() {
	outerLeafChecksumVerifiedTotal.Add(1)
}

func noteOuterLeafChecksumSkipped() {
	outerLeafChecksumSkippedTotal.Add(1)
}

func noteOuterLeafLoad(ptr page.ValuePtr, bytes int, iterator bool) {
	seq := outerLeafLoadsTotal.Add(1)
	if iterator {
		outerLeafIteratorLoadsTotal.Add(1)
	} else {
		outerLeafPointLoadsTotal.Add(1)
	}
	if bytes > 0 {
		outerLeafBytesTotal.Add(uint64(bytes))
	}

	if outerLeafReadSampleMod == 0 {
		return
	}
	if seq%outerLeafReadSampleMod != 0 {
		return
	}

	outerLeafSamplesTotal.Add(1)
	key := makeOuterLeafEstimatorKey(ptr)
	hit64, hit256, hit1K, hit4K := outerLeafRecentReadEstimator.observe(key)
	if hit64 {
		outerLeafRecent64HitsTotal.Add(1)
	}
	if hit256 {
		outerLeafRecent256HitsTotal.Add(1)
	}
	if hit1K {
		outerLeafRecent1KHitsTotal.Add(1)
	}
	if hit4K {
		outerLeafRecent4KHitsTotal.Add(1)
	}
}

func makeOuterLeafEstimatorKey(ptr page.ValuePtr) uint64 {
	return mixOuterLeafEstimatorUint64(uint64(ptr.FileID)) ^ bits.RotateLeft64(mixOuterLeafEstimatorUint64(ptr.Offset), 32)
}

func mixOuterLeafEstimatorUint64(v uint64) uint64 {
	v += 0x9e3779b97f4a7c15
	v = (v ^ (v >> 30)) * 0xbf58476d1ce4e5b9
	v = (v ^ (v >> 27)) * 0x94d049bb133111eb
	return v ^ (v >> 31)
}

func loadUintEnvDefault(key string, def uint64) uint64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return def
	}
	return v
}
