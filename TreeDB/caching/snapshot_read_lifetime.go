package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

const snapshotReadClosedBit uint64 = 1 << 63

// beginRead pins the snapshot's readable state against concurrent Close.
// Callers must pair a nil result with endRead.
func (s *Snapshot) beginRead() error {
	if s == nil {
		return backenddb.ErrClosed
	}
	for {
		state := s.readState.Load()
		if state&snapshotReadClosedBit != 0 {
			return backenddb.ErrClosed
		}
		if s.readState.CompareAndSwap(state, state+1) {
			return nil
		}
	}
}

func (s *Snapshot) endRead() {
	if s == nil {
		return
	}
	if s.readState.Add(^uint64(0)) == snapshotReadClosedBit {
		_ = s.finalizeCloseIfUnreferenced()
	}
}
