package caching

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// Keep one bounded scalar snapshot without boxing an immutable heap object for
// every sample. Writers serialize complete publications. Readers copy atomic
// fields between equal even sequence values, without a contended reader count.
// Every concurrently accessed field is atomic; retrying never excuses a data race.
// Typed 64-bit atomics provide their required alignment on 32-bit targets too.
type poolPressureSnapshotCache struct {
	writer             sync.Mutex
	sequence           atomic.Uint64
	sampledUnixNano    atomic.Int64
	level              atomic.Uint32
	usedBytes          atomic.Uint64
	heapAllocBytes     atomic.Uint64
	heapInuseBytes     atomic.Uint64
	heapSysBytes       atomic.Uint64
	heapIdleBytes      atomic.Uint64
	heapReleasedBytes  atomic.Uint64
	heapIdleUnreleased atomic.Uint64
	stackInuseBytes    atomic.Uint64
	stackSysBytes      atomic.Uint64
	nextGCBytes        atomic.Uint64
	numGC              atomic.Uint32
	gcCPUFractionBits  atomic.Uint64
	totalSysBytes      atomic.Uint64
	nonHeapSysBytes    atomic.Uint64
	memoryLimitBytes   atomic.Int64
}

func (c *poolPressureSnapshotCache) Store(snapshot poolPressureSnapshot) {
	c.writer.Lock()
	c.sequence.Add(1)
	c.sampledUnixNano.Store(snapshot.sampledUnixNano)
	c.level.Store(uint32(snapshot.level))
	c.usedBytes.Store(snapshot.usedBytes)
	c.heapAllocBytes.Store(snapshot.heapAllocBytes)
	c.heapInuseBytes.Store(snapshot.heapInuseBytes)
	c.heapSysBytes.Store(snapshot.heapSysBytes)
	c.heapIdleBytes.Store(snapshot.heapIdleBytes)
	c.heapReleasedBytes.Store(snapshot.heapReleasedBytes)
	c.heapIdleUnreleased.Store(snapshot.heapIdleUnreleased)
	c.stackInuseBytes.Store(snapshot.stackInuseBytes)
	c.stackSysBytes.Store(snapshot.stackSysBytes)
	c.nextGCBytes.Store(snapshot.nextGCBytes)
	c.numGC.Store(snapshot.numGC)
	c.gcCPUFractionBits.Store(math.Float64bits(snapshot.gcCPUFraction))
	c.totalSysBytes.Store(snapshot.totalSysBytes)
	c.nonHeapSysBytes.Store(snapshot.nonHeapSysBytes)
	c.memoryLimitBytes.Store(snapshot.memoryLimitBytes)
	c.sequence.Add(1)
	c.writer.Unlock()
}

func (c *poolPressureSnapshotCache) Load() poolPressureSnapshot {
	for {
		sequence := c.sequence.Load()
		if sequence&1 != 0 {
			// A sampler can be preempted while publishing. Let it finish rather
			// than occupying its processor with an unbounded tight spin.
			runtime.Gosched()
			continue
		}
		snapshot := poolPressureSnapshot{
			sampledUnixNano:    c.sampledUnixNano.Load(),
			level:              poolPressureLevel(c.level.Load()),
			usedBytes:          c.usedBytes.Load(),
			heapAllocBytes:     c.heapAllocBytes.Load(),
			heapInuseBytes:     c.heapInuseBytes.Load(),
			heapSysBytes:       c.heapSysBytes.Load(),
			heapIdleBytes:      c.heapIdleBytes.Load(),
			heapReleasedBytes:  c.heapReleasedBytes.Load(),
			heapIdleUnreleased: c.heapIdleUnreleased.Load(),
			stackInuseBytes:    c.stackInuseBytes.Load(),
			stackSysBytes:      c.stackSysBytes.Load(),
			nextGCBytes:        c.nextGCBytes.Load(),
			numGC:              c.numGC.Load(),
			gcCPUFraction:      math.Float64frombits(c.gcCPUFractionBits.Load()),
			totalSysBytes:      c.totalSysBytes.Load(),
			nonHeapSysBytes:    c.nonHeapSysBytes.Load(),
			memoryLimitBytes:   c.memoryLimitBytes.Load(),
		}
		if c.sequence.Load() == sequence {
			return snapshot
		}
	}
}

// LoadPressure reads only the policy fields, bound to one complete publication.
// The full diagnostic reader uses the same sequence and immutable-copy contract.
func (c *poolPressureSnapshotCache) LoadPressure() (sampledUnixNano int64, level poolPressureLevel) {
	for {
		sequence := c.sequence.Load()
		if sequence&1 != 0 {
			runtime.Gosched()
			continue
		}
		sampledUnixNano = c.sampledUnixNano.Load()
		level = poolPressureLevel(c.level.Load())
		if c.sequence.Load() == sequence {
			return sampledUnixNano, level
		}
	}
}
