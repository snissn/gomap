package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

const snapshotReadClosedBit uint64 = 1 << 63

// beginRead pins the snapshot's readable state against concurrent Close.
// Callers must pair a nil result with endRead.
func (s *Snapshot) beginRead() error {
	return s.beginReadStripe(0)
}

func (s *Snapshot) endRead() {
	s.endReadStripe(0)
}

func (s *Snapshot) beginReadStripe(stripe uint32) error {
	if s == nil || s.closed.Load() {
		return backenddb.ErrClosed
	}
	if s.readState.Add(1)&snapshotReadClosedBit != 0 {
		if s.readState.Add(^uint64(0)) == snapshotReadClosedBit && !s.finalized.Load() {
			_ = s.finalizeCloseIfUnreferenced()
		}
		return backenddb.ErrClosed
	}
	if s.cowCache != nil && !s.cowCache.beginReadStripe(stripe) {
		if s.readState.Add(^uint64(0)) == snapshotReadClosedBit && !s.finalized.Load() {
			_ = s.finalizeCloseIfUnreferenced()
		}
		return backenddb.ErrClosed
	}
	return nil
}

func (s *Snapshot) endReadStripe(stripe uint32) {
	if s == nil {
		return
	}
	if s.cowCache != nil {
		s.cowCache.endReadStripe(stripe)
	}
	if s.readState.Add(^uint64(0)) == snapshotReadClosedBit && !s.finalized.Load() {
		_ = s.finalizeCloseIfUnreferenced()
	}
}
