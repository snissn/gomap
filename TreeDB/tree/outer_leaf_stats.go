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

const outerLeafCounterStripeCount = 16

type outerLeafCounterStripe struct {
	loads            atomic.Uint64
	pointLoads       atomic.Uint64
	iteratorLoads    atomic.Uint64
	checksumVerified atomic.Uint64
	checksumSkipped  atomic.Uint64
	_                [24]byte // pad to 64-byte cache line
}

var (
	outerLeafCounterStripes     [outerLeafCounterStripeCount]outerLeafCounterStripe
	outerLeafSamplesTotal       atomic.Uint64
	outerLeafRecent64HitsTotal  atomic.Uint64
	outerLeafRecent256HitsTotal atomic.Uint64
	outerLeafRecent1KHitsTotal  atomic.Uint64
	outerLeafRecent4KHitsTotal  atomic.Uint64
)

func outerLeafStripeIndex(ptr page.ValuePtr) uint64 {
	h := (uint64(ptr.FileID) << 32) ^ ptr.Offset ^ uint64(ptr.Length)
	h ^= h >> 17
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 31
	return h & (outerLeafCounterStripeCount - 1)
}

func resetOuterLeafReadStatsForTest() {
	for i := range outerLeafCounterStripes {
		outerLeafCounterStripes[i].loads.Store(0)
		outerLeafCounterStripes[i].pointLoads.Store(0)
		outerLeafCounterStripes[i].iteratorLoads.Store(0)
		outerLeafCounterStripes[i].checksumVerified.Store(0)
		outerLeafCounterStripes[i].checksumSkipped.Store(0)
	}
	outerLeafSamplesTotal.Store(0)
	outerLeafRecent64HitsTotal.Store(0)
	outerLeafRecent256HitsTotal.Store(0)
	outerLeafRecent1KHitsTotal.Store(0)
	outerLeafRecent4KHitsTotal.Store(0)
}

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
	var loads, pointLoads, iteratorLoads, checksumVerified, checksumSkipped uint64
	for i := range outerLeafCounterStripes {
		loads += outerLeafCounterStripes[i].loads.Load()
		pointLoads += outerLeafCounterStripes[i].pointLoads.Load()
		iteratorLoads += outerLeafCounterStripes[i].iteratorLoads.Load()
		checksumVerified += outerLeafCounterStripes[i].checksumVerified.Load()
		checksumSkipped += outerLeafCounterStripes[i].checksumSkipped.Load()
	}
	return OuterLeafReadStats{
		LoadsTotal:            loads,
		PointLoadsTotal:       pointLoads,
		IteratorLoadsTotal:    iteratorLoads,
		BytesTotal:            loads * uint64(page.PageSize),
		SampleMod:             outerLeafReadSampleMod,
		SamplesTotal:          outerLeafSamplesTotal.Load(),
		Recent64HitsTotal:     outerLeafRecent64HitsTotal.Load(),
		Recent256HitsTotal:    outerLeafRecent256HitsTotal.Load(),
		Recent1KHitsTotal:     outerLeafRecent1KHitsTotal.Load(),
		Recent4KHitsTotal:     outerLeafRecent4KHitsTotal.Load(),
		ChecksumVerifiedTotal: checksumVerified,
		ChecksumSkippedTotal:  checksumSkipped,
	}
}

type outerLeafRingEntry struct {
	key uint64
	seq uint64
}

type outerLeafRecentEstimatorState struct {
	mu       sync.Mutex
	seq      uint64
	ring4K   [4096]outerLeafRingEntry
	next4K   int
	lastSeen map[uint64]uint64
}

func newOuterLeafRecentReadEstimator() *outerLeafRecentEstimatorState {
	return &outerLeafRecentEstimatorState{
		lastSeen: make(map[uint64]uint64, 256),
	}
}

func (e *outerLeafRecentEstimatorState) observe(key uint64) (hit64, hit256, hit1K, hit4K bool) {
	if e == nil {
		return false, false, false, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	nextSeq := e.seq + 1
	if prev, ok := e.lastSeen[key]; ok {
		age := nextSeq - prev
		hit64 = age <= 64
		hit256 = age <= 256
		hit1K = age <= 1024
		hit4K = age <= 4096
	}

	old := e.ring4K[e.next4K]
	if old.seq != 0 && e.lastSeen[old.key] == old.seq {
		delete(e.lastSeen, old.key)
	}
	e.ring4K[e.next4K] = outerLeafRingEntry{key: key, seq: nextSeq}
	e.next4K = (e.next4K + 1) & (len(e.ring4K) - 1)
	if e.lastSeen == nil {
		e.lastSeen = make(map[uint64]uint64, 256)
	}
	e.lastSeen[key] = nextSeq
	e.seq = nextSeq

	return hit64, hit256, hit1K, hit4K
}

func noteOuterLeafChecksumVerified() {
	outerLeafCounterStripes[0].checksumVerified.Add(1)
}

func noteOuterLeafChecksumSkipped() {
	outerLeafCounterStripes[0].checksumSkipped.Add(1)
}

func noteOuterLeafChecksumVerifiedAt(ptr page.ValuePtr) {
	outerLeafCounterStripes[outerLeafStripeIndex(ptr)].checksumVerified.Add(1)
}

func noteOuterLeafChecksumSkippedAt(ptr page.ValuePtr) {
	outerLeafCounterStripes[outerLeafStripeIndex(ptr)].checksumSkipped.Add(1)
}

// Every logged leaf load records one validated page of page.PageSize bytes.
func noteOuterLeafLoad(ptr page.ValuePtr, iterator bool) {
	stripe := &outerLeafCounterStripes[outerLeafStripeIndex(ptr)]
	seq := stripe.loads.Add(1)
	if iterator {
		stripe.iteratorLoads.Add(1)
	} else {
		stripe.pointLoads.Add(1)
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
