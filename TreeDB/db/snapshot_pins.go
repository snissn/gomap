package db

import "math"

// MinPinnedSnapshotCommitSeq returns the oldest commit sequence currently held
// by any active snapshot reader across the current and still-tracked retired
// index generations. In-flight snapshot acquisitions conservatively return 0
// because they may have loaded a view that is not registered yet.
// math.MaxUint64 means no snapshot reader is pinned.
func (db *DB) MinPinnedSnapshotCommitSeq() uint64 {
	if db == nil {
		return math.MaxUint64
	}
	if db.snapshotAcquireInFlight() > 0 {
		return 0
	}

	// idxMu is a Mutex rather than an RWMutex, so this read-only walk must take
	// the exclusive generation-list lock.
	db.idxMu.Lock()
	defer db.idxMu.Unlock()

	min := uint64(math.MaxUint64)
	for _, gen := range db.idxAll {
		min = minPinnedSnapshotCommitSeqForIndexGen(min, gen)
	}
	min = minPinnedSnapshotCommitSeqForIndexGen(min, db.idx.Load())
	if db.snapshotAcquireInFlight() > 0 {
		return 0
	}
	return min
}

func minPinnedSnapshotCommitSeqForIndexGen(min uint64, gen *indexGen) uint64 {
	if gen == nil || gen.registry == nil {
		return min
	}
	if seq := gen.registry.MinPinnedSeq(); seq < min {
		return seq
	}
	return min
}
