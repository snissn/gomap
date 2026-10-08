package db

const snapshotReadClosedBit uint64 = 1 << 63

// beginRead pins the snapshot's readable state against concurrent Close.
// Callers must pair a nil result with endRead.
func (s *Snapshot) beginRead() error {
	if s == nil {
		return ErrClosed
	}
	for {
		state := s.readState.Load()
		if state&snapshotReadClosedBit != 0 {
			return ErrClosed
		}
		if s.readState.CompareAndSwap(state, state+1) {
			return nil
		}
	}
}

func (s *Snapshot) endRead() { _ = s.endReadChecked() }

// Selected synchronous consumers propagate finalization failure. The real
// Snapshot retains controlled terminal debt for its checked public Close retry.
func (s *Snapshot) endReadChecked() error {
	if s == nil {
		return nil
	}
	s.iteratorMu.Lock()
	c := s.originalCleanup
	if c != nil {
		if err := c.RetainOriginalCleanupV1(); err != nil {
			s.iteratorMu.Unlock()
			return err
		}
	}
	s.iteratorMu.Unlock()
	defer func() {
		s = nil
		if c != nil {
			c.ReleaseOriginalCleanupV1()
			c = nil
		}
	}()
	if s.readState.Add(^uint64(0)) == snapshotReadClosedBit {
		return s.finalizeCloseIfUnreferenced()
	}
	return nil
}
