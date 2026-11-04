package caching

// DIAGNOSTIC OVERLAY ONLY. Not a production API or an acceptance instrument.
// A single exclusive observer counts this process's existing checkpoint/list
// seam. It retains no DB, backend, path string, directory entry or request.

import (
	"errors"
	"sync/atomic"
)

var primaryAllocationObservation atomic.Pointer[PrimaryAllocationObservation]

type PrimaryAllocationObservation struct {
	checkpoints atomic.Uint64
	automatic atomic.Uint64
	readDirs atomic.Uint64
	readDirFinished atomic.Uint64
	readDirErrors atomic.Uint64
	pathBytes atomic.Uint64
	minPathBytes atomic.Uint64
	maxPathBytes atomic.Uint64
}

type PrimaryAllocationSeamCounters struct {
	CheckpointCalls uint64
	AutomaticCheckpointCalls uint64
	ReadDirCalls uint64
	ReadDirFinished uint64
	ReadDirErrors uint64
	PathBytes uint64
	MinPathBytes uint64
	MaxPathBytes uint64
}

// BeginPrimaryAllocationObservation rejects overlapping owners. Counters are
// process-wide, not proof that a particular DB or CommitAt caused an event.
func BeginPrimaryAllocationObservation() (*PrimaryAllocationObservation, error) {
	o := new(PrimaryAllocationObservation)
	if !primaryAllocationObservation.CompareAndSwap(nil, o) {
		return nil, errors.New("PRIMARY diagnostic observer already owned")
	}
	return o, nil
}

func (o *PrimaryAllocationObservation) Close() error {
	if o == nil || !primaryAllocationObservation.CompareAndSwap(o, nil) {
		return errors.New("PRIMARY diagnostic observer owner mismatch")
	}
	// A worker which loaded o before this CAS may finish against o. It remains
	// GC-owned, is never reset/reused, and cannot update a successor observer.
	return nil
}

func (o *PrimaryAllocationObservation) Snapshot() PrimaryAllocationSeamCounters {
	return PrimaryAllocationSeamCounters{
		CheckpointCalls: o.checkpoints.Load(),
		AutomaticCheckpointCalls: o.automatic.Load(),
		ReadDirCalls: o.readDirs.Load(),
		ReadDirFinished: o.readDirFinished.Load(),
		ReadDirErrors: o.readDirErrors.Load(),
		PathBytes: o.pathBytes.Load(),
		MinPathBytes: o.minPathBytes.Load(),
		MaxPathBytes: o.maxPathBytes.Load(),
	}
}

func primaryAllocationCheckpointEntered(automatic bool) {
	if o := primaryAllocationObservation.Load(); o != nil {
		o.checkpoints.Add(1)
		if automatic {
			o.automatic.Add(1)
		}
	}
}

func primaryAllocationReadDirStarted(pathBytes int) *PrimaryAllocationObservation {
	o := primaryAllocationObservation.Load()
	if o == nil {
		return nil
	}
	n := uint64(pathBytes)
	o.pathBytes.Add(n)
	for old := o.minPathBytes.Load(); old == 0 || n < old; old = o.minPathBytes.Load() {
		if o.minPathBytes.CompareAndSwap(old, n) {
			break
		}
	}
	for old := o.maxPathBytes.Load(); n > old; old = o.maxPathBytes.Load() {
		if o.maxPathBytes.CompareAndSwap(old, n) {
			break
		}
	}
	o.readDirs.Add(1)
	return o
}

func (o *PrimaryAllocationObservation) readDirCompleted(failed bool) {
	if failed {
		o.readDirErrors.Add(1)
	}
	o.readDirFinished.Add(1)
}
