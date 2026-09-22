package valuelog

import (
	"fmt"
	"sync/atomic"
)

// DecodeScratchStats reports process-global decode scratch pool activity plus
// optional per-manager file stash retention filled by Manager.DecodeScratchStats.
type DecodeScratchStats struct {
	SmallPoolMaxEntries      int
	SmallPoolEntries         int
	SmallPoolRetainedBuffers uint64
	SmallPoolRetainedBytes   uint64
	SmallPoolGetsTotal       uint64
	SmallPoolHitsTotal       uint64
	SmallPoolMissesTotal     uint64
	SmallPoolTooSmallTotal   uint64
	SmallPoolPutsTotal       uint64
	SmallPoolDropsTotal      uint64
	SmallPoolDroppedBytes    uint64
	SmallAllocCallsTotal     uint64
	SmallAllocatedBytesTotal uint64

	LargePoolMaxEntries      int
	LargePoolEntries         int
	LargePoolRetainedBuffers uint64
	LargePoolRetainedBytes   uint64
	LargePoolGetsTotal       uint64
	LargePoolHitsTotal       uint64
	LargePoolMissesTotal     uint64
	LargePoolTooSmallTotal   uint64
	LargePoolPutsTotal       uint64
	LargePoolDropsTotal      uint64
	LargePoolDroppedBytes    uint64
	LargeAllocCallsTotal     uint64
	LargeAllocatedBytesTotal uint64

	OversizeAllocCallsTotal     uint64
	OversizeAllocatedBytesTotal uint64
	OversizeDropsTotal          uint64
	OversizeDroppedBytesTotal   uint64

	FileRetainMaxBytes  uint64
	FileSpareMaxCount   uint64
	FileRetainedFiles   uint64
	FileRetainedBuffers uint64
	FileRetainedBytes   uint64
}

var decodeScratchSmallPoolRetainedBytes atomic.Uint64
var decodeScratchSmallPoolRetainedBuffers atomic.Uint64
var decodeScratchSmallPoolGetsTotal atomic.Uint64
var decodeScratchSmallPoolHitsTotal atomic.Uint64
var decodeScratchSmallPoolMissesTotal atomic.Uint64
var decodeScratchSmallPoolTooSmallTotal atomic.Uint64
var decodeScratchSmallPoolPutsTotal atomic.Uint64
var decodeScratchSmallPoolDropsTotal atomic.Uint64
var decodeScratchSmallPoolDroppedBytesTotal atomic.Uint64
var decodeScratchSmallAllocCallsTotal atomic.Uint64
var decodeScratchSmallAllocatedBytesTotal atomic.Uint64

var decodeScratchLargePoolRetainedBytes atomic.Uint64
var decodeScratchLargePoolRetainedBuffers atomic.Uint64
var decodeScratchLargePoolGetsTotal atomic.Uint64
var decodeScratchLargePoolHitsTotal atomic.Uint64
var decodeScratchLargePoolMissesTotal atomic.Uint64
var decodeScratchLargePoolTooSmallTotal atomic.Uint64
var decodeScratchLargePoolPutsTotal atomic.Uint64
var decodeScratchLargePoolDropsTotal atomic.Uint64
var decodeScratchLargePoolDroppedBytesTotal atomic.Uint64
var decodeScratchLargeAllocCallsTotal atomic.Uint64
var decodeScratchLargeAllocatedBytesTotal atomic.Uint64

var decodeScratchOversizeAllocCallsTotal atomic.Uint64
var decodeScratchOversizeAllocatedBytesTotal atomic.Uint64
var decodeScratchOversizeDropsTotal atomic.Uint64
var decodeScratchOversizeDroppedBytesTotal atomic.Uint64

func DecodeScratchStatsSnapshot() DecodeScratchStats {
	return DecodeScratchStats{
		SmallPoolMaxEntries:      smallDecodeScratchPoolEntries,
		SmallPoolEntries:         len(smallDecodeScratchPool),
		SmallPoolRetainedBuffers: decodeScratchSmallPoolRetainedBuffers.Load(),
		SmallPoolRetainedBytes:   decodeScratchSmallPoolRetainedBytes.Load(),
		SmallPoolGetsTotal:       decodeScratchSmallPoolGetsTotal.Load(),
		SmallPoolHitsTotal:       decodeScratchSmallPoolHitsTotal.Load(),
		SmallPoolMissesTotal:     decodeScratchSmallPoolMissesTotal.Load(),
		SmallPoolTooSmallTotal:   decodeScratchSmallPoolTooSmallTotal.Load(),
		SmallPoolPutsTotal:       decodeScratchSmallPoolPutsTotal.Load(),
		SmallPoolDropsTotal:      decodeScratchSmallPoolDropsTotal.Load(),
		SmallPoolDroppedBytes:    decodeScratchSmallPoolDroppedBytesTotal.Load(),
		SmallAllocCallsTotal:     decodeScratchSmallAllocCallsTotal.Load(),
		SmallAllocatedBytesTotal: decodeScratchSmallAllocatedBytesTotal.Load(),

		LargePoolMaxEntries:      largeDecodeScratchPoolEntries,
		LargePoolEntries:         len(largeDecodeScratchPool),
		LargePoolRetainedBuffers: decodeScratchLargePoolRetainedBuffers.Load(),
		LargePoolRetainedBytes:   decodeScratchLargePoolRetainedBytes.Load(),
		LargePoolGetsTotal:       decodeScratchLargePoolGetsTotal.Load(),
		LargePoolHitsTotal:       decodeScratchLargePoolHitsTotal.Load(),
		LargePoolMissesTotal:     decodeScratchLargePoolMissesTotal.Load(),
		LargePoolTooSmallTotal:   decodeScratchLargePoolTooSmallTotal.Load(),
		LargePoolPutsTotal:       decodeScratchLargePoolPutsTotal.Load(),
		LargePoolDropsTotal:      decodeScratchLargePoolDropsTotal.Load(),
		LargePoolDroppedBytes:    decodeScratchLargePoolDroppedBytesTotal.Load(),
		LargeAllocCallsTotal:     decodeScratchLargeAllocCallsTotal.Load(),
		LargeAllocatedBytesTotal: decodeScratchLargeAllocatedBytesTotal.Load(),

		OversizeAllocCallsTotal:     decodeScratchOversizeAllocCallsTotal.Load(),
		OversizeAllocatedBytesTotal: decodeScratchOversizeAllocatedBytesTotal.Load(),
		OversizeDropsTotal:          decodeScratchOversizeDropsTotal.Load(),
		OversizeDroppedBytesTotal:   decodeScratchOversizeDroppedBytesTotal.Load(),

		FileRetainMaxBytes: uint64(fileDecodeScratchRetainMaxBytes),
		FileSpareMaxCount:  uint64(fileDecodeScratchSpareKeep),
	}
}

func AppendDecodeScratchStats(out map[string]string, prefix string, stats DecodeScratchStats) {
	if out == nil || prefix == "" {
		return
	}
	out[prefix+".small_pool.max_entries"] = fmt.Sprintf("%d", stats.SmallPoolMaxEntries)
	out[prefix+".small_pool.entries"] = fmt.Sprintf("%d", stats.SmallPoolEntries)
	out[prefix+".small_pool.retained_buffers"] = fmt.Sprintf("%d", stats.SmallPoolRetainedBuffers)
	out[prefix+".small_pool.retained_bytes"] = fmt.Sprintf("%d", stats.SmallPoolRetainedBytes)
	out[prefix+".small_pool.gets_total"] = fmt.Sprintf("%d", stats.SmallPoolGetsTotal)
	out[prefix+".small_pool.hits_total"] = fmt.Sprintf("%d", stats.SmallPoolHitsTotal)
	out[prefix+".small_pool.misses_total"] = fmt.Sprintf("%d", stats.SmallPoolMissesTotal)
	out[prefix+".small_pool.too_small_total"] = fmt.Sprintf("%d", stats.SmallPoolTooSmallTotal)
	out[prefix+".small_pool.puts_total"] = fmt.Sprintf("%d", stats.SmallPoolPutsTotal)
	out[prefix+".small_pool.drops_total"] = fmt.Sprintf("%d", stats.SmallPoolDropsTotal)
	out[prefix+".small_pool.dropped_bytes_total"] = fmt.Sprintf("%d", stats.SmallPoolDroppedBytes)
	out[prefix+".small_pool.alloc_calls_total"] = fmt.Sprintf("%d", stats.SmallAllocCallsTotal)
	out[prefix+".small_pool.allocated_bytes_total"] = fmt.Sprintf("%d", stats.SmallAllocatedBytesTotal)
	out[prefix+".large_pool.max_entries"] = fmt.Sprintf("%d", stats.LargePoolMaxEntries)
	out[prefix+".large_pool.entries"] = fmt.Sprintf("%d", stats.LargePoolEntries)
	out[prefix+".large_pool.retained_buffers"] = fmt.Sprintf("%d", stats.LargePoolRetainedBuffers)
	out[prefix+".large_pool.retained_bytes"] = fmt.Sprintf("%d", stats.LargePoolRetainedBytes)
	out[prefix+".large_pool.gets_total"] = fmt.Sprintf("%d", stats.LargePoolGetsTotal)
	out[prefix+".large_pool.hits_total"] = fmt.Sprintf("%d", stats.LargePoolHitsTotal)
	out[prefix+".large_pool.misses_total"] = fmt.Sprintf("%d", stats.LargePoolMissesTotal)
	out[prefix+".large_pool.too_small_total"] = fmt.Sprintf("%d", stats.LargePoolTooSmallTotal)
	out[prefix+".large_pool.puts_total"] = fmt.Sprintf("%d", stats.LargePoolPutsTotal)
	out[prefix+".large_pool.drops_total"] = fmt.Sprintf("%d", stats.LargePoolDropsTotal)
	out[prefix+".large_pool.dropped_bytes_total"] = fmt.Sprintf("%d", stats.LargePoolDroppedBytes)
	out[prefix+".large_pool.alloc_calls_total"] = fmt.Sprintf("%d", stats.LargeAllocCallsTotal)
	out[prefix+".large_pool.allocated_bytes_total"] = fmt.Sprintf("%d", stats.LargeAllocatedBytesTotal)
	out[prefix+".oversize.alloc_calls_total"] = fmt.Sprintf("%d", stats.OversizeAllocCallsTotal)
	out[prefix+".oversize.allocated_bytes_total"] = fmt.Sprintf("%d", stats.OversizeAllocatedBytesTotal)
	out[prefix+".oversize.drops_total"] = fmt.Sprintf("%d", stats.OversizeDropsTotal)
	out[prefix+".oversize.dropped_bytes_total"] = fmt.Sprintf("%d", stats.OversizeDroppedBytesTotal)
	out[prefix+".file_stash.retain_max_bytes"] = fmt.Sprintf("%d", stats.FileRetainMaxBytes)
	out[prefix+".file_stash.spare_max_count"] = fmt.Sprintf("%d", stats.FileSpareMaxCount)
	out[prefix+".file_stash.retained_files"] = fmt.Sprintf("%d", stats.FileRetainedFiles)
	out[prefix+".file_stash.retained_buffers"] = fmt.Sprintf("%d", stats.FileRetainedBuffers)
	out[prefix+".file_stash.retained_bytes"] = fmt.Sprintf("%d", stats.FileRetainedBytes)
}

func noteDecodeScratchSmallPoolPut(capacity int) {
	if capacity <= 0 {
		return
	}
	decodeScratchSmallPoolRetainedBuffers.Add(1)
	decodeScratchSmallPoolRetainedBytes.Add(uint64(capacity))
}

func noteDecodeScratchSmallPoolTake(capacity int) {
	if capacity <= 0 {
		return
	}
	decodeScratchSmallPoolRetainedBuffers.Add(^uint64(0))
	decodeScratchSmallPoolRetainedBytes.Add(^uint64(capacity - 1))
}

func noteDecodeScratchLargePoolPut(capacity int) {
	if capacity <= 0 {
		return
	}
	decodeScratchLargePoolRetainedBuffers.Add(1)
	decodeScratchLargePoolRetainedBytes.Add(uint64(capacity))
}

func noteDecodeScratchLargePoolTake(capacity int) {
	if capacity <= 0 {
		return
	}
	decodeScratchLargePoolRetainedBuffers.Add(^uint64(0))
	decodeScratchLargePoolRetainedBytes.Add(^uint64(capacity - 1))
}
