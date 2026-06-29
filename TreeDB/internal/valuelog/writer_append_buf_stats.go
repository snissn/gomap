package valuelog

import (
	"fmt"
	"sync/atomic"
)

// WriterAppendBufferStats reports process-global writer append-buffer pool
// activity. Live Writer.appendBuf ownership is intentionally not inferred here;
// pprof plus pool/drop counters distinguish pooled retention from live writers.
type WriterAppendBufferStats struct {
	PoolMaxEntries      int
	PoolEntries         int
	PoolRetainedBuffers uint64
	PoolRetainedBytes   uint64
	GetsTotal           uint64
	HitsTotal           uint64
	MissesTotal         uint64
	PutsTotal           uint64
	DropsTotal          uint64
	DroppedBytesTotal   uint64
	AllocCallsTotal     uint64
	AllocatedBytesTotal uint64
}

var writerAppendBufPoolRetainedBytes atomic.Uint64
var writerAppendBufPoolRetainedBuffers atomic.Uint64
var writerAppendBufGetsTotal atomic.Uint64
var writerAppendBufHitsTotal atomic.Uint64
var writerAppendBufMissesTotal atomic.Uint64
var writerAppendBufPutsTotal atomic.Uint64
var writerAppendBufDropsTotal atomic.Uint64
var writerAppendBufDroppedBytesTotal atomic.Uint64
var writerAppendBufAllocCallsTotal atomic.Uint64
var writerAppendBufAllocatedBytesTotal atomic.Uint64

func WriterAppendBufferStatsSnapshot() WriterAppendBufferStats {
	return WriterAppendBufferStats{
		PoolMaxEntries:      writerAppendBufPoolEntries,
		PoolEntries:         len(writerAppendBufPool),
		PoolRetainedBuffers: writerAppendBufPoolRetainedBuffers.Load(),
		PoolRetainedBytes:   writerAppendBufPoolRetainedBytes.Load(),
		GetsTotal:           writerAppendBufGetsTotal.Load(),
		HitsTotal:           writerAppendBufHitsTotal.Load(),
		MissesTotal:         writerAppendBufMissesTotal.Load(),
		PutsTotal:           writerAppendBufPutsTotal.Load(),
		DropsTotal:          writerAppendBufDropsTotal.Load(),
		DroppedBytesTotal:   writerAppendBufDroppedBytesTotal.Load(),
		AllocCallsTotal:     writerAppendBufAllocCallsTotal.Load(),
		AllocatedBytesTotal: writerAppendBufAllocatedBytesTotal.Load(),
	}
}

func AppendWriterAppendBufferStats(out map[string]string, prefix string, stats WriterAppendBufferStats) {
	if out == nil || prefix == "" {
		return
	}
	out[prefix+".pool.max_entries"] = fmt.Sprintf("%d", stats.PoolMaxEntries)
	out[prefix+".pool.entries"] = fmt.Sprintf("%d", stats.PoolEntries)
	out[prefix+".pool.retained_buffers"] = fmt.Sprintf("%d", stats.PoolRetainedBuffers)
	out[prefix+".pool.retained_bytes"] = fmt.Sprintf("%d", stats.PoolRetainedBytes)
	out[prefix+".gets_total"] = fmt.Sprintf("%d", stats.GetsTotal)
	out[prefix+".hits_total"] = fmt.Sprintf("%d", stats.HitsTotal)
	out[prefix+".misses_total"] = fmt.Sprintf("%d", stats.MissesTotal)
	out[prefix+".puts_total"] = fmt.Sprintf("%d", stats.PutsTotal)
	out[prefix+".drops_total"] = fmt.Sprintf("%d", stats.DropsTotal)
	out[prefix+".dropped_bytes_total"] = fmt.Sprintf("%d", stats.DroppedBytesTotal)
	out[prefix+".alloc_calls_total"] = fmt.Sprintf("%d", stats.AllocCallsTotal)
	out[prefix+".allocated_bytes_total"] = fmt.Sprintf("%d", stats.AllocatedBytesTotal)
}

func noteWriterAppendBufPoolPut(capacity int) {
	if capacity <= 0 {
		return
	}
	writerAppendBufPoolRetainedBuffers.Add(1)
	writerAppendBufPoolRetainedBytes.Add(uint64(capacity))
}

func noteWriterAppendBufPoolTake(capacity int) {
	if capacity <= 0 {
		return
	}
	writerAppendBufPoolRetainedBuffers.Add(^uint64(0))
	writerAppendBufPoolRetainedBytes.Add(^uint64(capacity - 1))
}
