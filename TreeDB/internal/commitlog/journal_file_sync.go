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
// advanced past a failed write. It also retains errors that leave the
// authoritative segment ambiguous to recovery. This lock does not protect
// append or flush.
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
		return s.recordFailureLocked(err)
	}
	return nil
}

// recordFailure retains an error from an operation that already ran, even
// when a captured sync has independently failed.
func (s *journalFileSync) recordFailure(cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recordFailureLocked(cause)
}

func (s *journalFileSync) recordFailureLocked(cause error) error {
	prior := s.err()
	if prior != nil {
		cause = errors.Join(prior, cause)
	}
	s.failure.Store(&journalFileSyncFailure{cause: cause})
	if prior == nil && s.onError != nil {
		s.onError()
	}
	return cause
}
