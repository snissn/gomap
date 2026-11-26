package db

import (
	"math"
	"runtime"
	"sync"
)

var minPinnedSnapshotCommitSeqAfterScanForTesting func()
var minPinnedSnapshotCommitSeqAfterScanForTestingMu sync.RWMutex

const minPinnedSnapshotCommitSeqMaxAttempts = 64

// MinPinnedSnapshotCommitSeq returns the oldest sequence retained across the
// current and still-tracked retired index generations, including directory-only
// page retention. Reuse and pruning must include every purpose. In-flight
// snapshot acquisitions conservatively return 0 because they may have loaded a
// view that is not registered yet. math.MaxUint64 means no pin is retained.
func (db *DB) MinPinnedSnapshotCommitSeq() uint64 {
	return db.minPinnedCommitSeq(false)
}

// MinPinnedReaderCommitSeq returns the conservative minimum of reader-capable
// pins. It excludes directory-only page retention, but includes snapshots,
// captured recoverable roots and operations that can still read an older basis.
// Reuse and pruning must use MinPinnedSnapshotCommitSeq, which includes ALL pins.
func (db *DB) MinPinnedReaderCommitSeq() uint64 {
	return db.minPinnedCommitSeq(true)
}

func (db *DB) minPinnedCommitSeq(readersOnly bool) uint64 {
	if db == nil {
		return math.MaxUint64
	}
	for attempts := 0; attempts < minPinnedSnapshotCommitSeqMaxAttempts; attempts++ {
		if db.snapshotAcquireInFlight() > 0 {
			return 0
		}
		acquireEpoch := db.snapshotAcquireEpoch.Load()

		// idxMu is a Mutex rather than an RWMutex, so this read-only walk must take
		// the exclusive generation-list lock.
		db.idxMu.Lock()
		min := uint64(math.MaxUint64)
		for _, gen := range db.idxAll {
			min = minPinnedCommitSeqForIndexGen(min, gen, readersOnly)
		}
		min = minPinnedCommitSeqForIndexGen(min, db.idx.Load(), readersOnly)
		db.idxMu.Unlock()

		minPinnedSnapshotCommitSeqAfterScanForTestingMu.RLock()
		hook := minPinnedSnapshotCommitSeqAfterScanForTesting
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.RUnlock()
		if hook != nil {
			hook()
		}
		if db.snapshotAcquireInFlight() > 0 {
			return 0
		}
		if db.snapshotAcquireEpoch.Load() == acquireEpoch {
			return min
		}
		runtime.Gosched()
	}
	return 0
}

// minPinnedSnapshotCommitSeqForDurablePublish computes a conservative lower
// bound on all registered and in-flight snapshot pins without acquiring
// rootReuseMu. Callers must hold durablePublishMu. Because every snapshotViewRO
// publication installs its state while holding rootReuseMu.Lock() (draining all
// prior in-flight captures), every in-flight capture after that unlock can only
// register a retention commit >= visibleRetentionFloor. Therefore clamping the
// scanned registered minimum to visibleRetentionFloor is exact for all pins
// below visibleRetentionFloor while never blocking concurrent readers.
func (db *DB) minPinnedSnapshotCommitSeqForDurablePublish(visibleRetentionFloor uint64) uint64 {
	if db == nil {
		return math.MaxUint64
	}
	if visibleRetentionFloor == 0 {
		db.rootReuseMu.Lock()
		min := db.MinPinnedSnapshotCommitSeq()
		db.rootReuseMu.Unlock()
		return min
	}
	db.idxMu.Lock()
	min := visibleRetentionFloor
	for _, gen := range db.idxAll {
		min = minPinnedCommitSeqForIndexGen(min, gen, false)
	}
	min = minPinnedCommitSeqForIndexGen(min, db.idx.Load(), false)
	db.idxMu.Unlock()
	return min
}

func minPinnedCommitSeqForIndexGen(min uint64, gen *indexGen, readersOnly bool) uint64 {
	if gen == nil || gen.registry == nil {
		return min
	}
	var seq uint64
	if readersOnly {
		seq = gen.registry.MinReaderPinnedSeq()
	} else {
		seq = gen.registry.MinPinnedSeq()
	}
	if seq < min {
		return seq
	}
	return min
}
