package commitlog

import (
	"errors"
	"sync"
	"sync/atomic"
)

type journalFileSyncFailure struct{ cause error }

// journalFileSync protects physical syncs of the active writer and its
// retained duplicates, including the ones that have rotated out. On Linux
// duplicates share the open-file description's writeback error cursor. Do
// not let another sync consume a successful result after that cursor has
// advanced past a failed write. This lock does not protect append or flush.
type journalFileSync struct {
	mu      sync.Mutex
	failure atomic.Pointer[journalFileSyncFailure]
	onError func()
}

func (s *journalFileSync) err() error {
	if failure := s.failure.Load(); failure != nil {
		return failure.cause
	}
	return nil
}

// fail publishes the first error from a data-file or namespace owner. The
// namespace has its own descriptor and may fail during an older file sync.
func (s *journalFileSync) fail(err error) error {
	if s == nil || err == nil {
		return err
	}
	if s.failure.CompareAndSwap(nil, &journalFileSyncFailure{cause: err}) {
		if s.onError != nil {
			s.onError()
		}
		return err
	}
	first := s.err()
	if errors.Is(err, first) {
		return err
	}
	return errors.Join(err, first)
}

func (s *journalFileSync) run(syncFile func() error) error {
	if s == nil { // A low-level Writer with no CommandJournal owner.
		return syncFile()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.err(); err != nil {
		return err
	}
	if err := syncFile(); err != nil {
		return s.fail(err)
	}
	return s.err()
}
