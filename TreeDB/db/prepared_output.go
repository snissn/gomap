package db

import "github.com/snissn/gomap/TreeDB/freelist"

type preparedOutputID uint64

type preparedOutputState uint8

const (
	preparedOutputStateNone preparedOutputState = iota
	preparedOutputStatePrepared
	preparedOutputStateInstalled
	preparedOutputStateAbandoned
)

type preparedOutputSnapshot struct {
	ID    preparedOutputID
	State preparedOutputState
	Pages []uint64
}

func (db *DB) nextPreparedOutputID() preparedOutputID {
	if db == nil {
		return 0
	}
	return preparedOutputID(db.preparedOutputNextID.Add(1))
}

func (db *DB) newPreparedOutputAllocTracker(alloc *freelist.Allocator) *allocTracker {
	return newPreparedOutputAllocTracker(alloc, db.nextPreparedOutputID())
}
