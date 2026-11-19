package caching

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Requests contain endpoints only. The installed COW cut keeps all source and
// retained-reader ownership. The channel merely wakes the existing worker.
type cowGenerationSealRequest struct {
	through  rootpublication.SealedPrefix
	deadline time.Time
}

type cowGenerationSealQueue struct {
	// Fixed before publishing the running worker's captured size threshold.
	idleInterval atomic.Int64
	mu           sync.Mutex
	pending      [2]cowGenerationSealRequest
}

func (q *cowGenerationSealQueue) full() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending[1].through != (rootpublication.SealedPrefix{})
}

// Only the producer holding nativePressureMu can add a request. The worker
// only removes requests, so the capacity check precedes the real rollover.
func (q *cowGenerationSealQueue) add(request cowGenerationSealRequest) bool {
	if request.through == (rootpublication.SealedPrefix{}) {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending[0].through == (rootpublication.SealedPrefix{}) {
		q.pending[0] = request
	} else {
		q.pending[1] = request
	}
	return true
}

func (q *cowGenerationSealQueue) next(now time.Time) (cowGenerationSealRequest, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending[1].through != (rootpublication.SealedPrefix{}) {
		return q.pending[1], 2
	}
	first := q.pending[0]
	if first.through == (rootpublication.SealedPrefix{}) || now.Before(first.deadline) {
		return first, 0
	}
	return first, 1
}

// Keep a second rotation that arrived while a single request was in flight.
func (q *cowGenerationSealQueue) complete(count int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if count == 1 {
		q.pending[0] = q.pending[1]
	} else {
		q.pending[0] = cowGenerationSealRequest{}
	}
	q.pending[1] = cowGenerationSealRequest{}
}

// Require space for another generation in the parent budget. The original
// per-write admission still checks the actual operation and every other limit.
func cowGenerationSealHasHeadroom(limits memtable.COWLimits, stats memtable.COWStats) bool {
	fits := func(limit, used uint64) bool {
		return used <= limit && limits.MaxGenerationBytes <= limit-used
	}
	early := limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
	retained := stats.HistoryBytes + stats.ReservedBytes + stats.DeferredBytes
	return fits(early, stats.ReservedBytes) && fits(limits.MaxTotalBytes, stats.TotalBytes) &&
		fits(limits.MaxRetiredBytes, retained) && stats.Sources < limits.MaxSources && stats.Generations < limits.MaxGenerations
}
