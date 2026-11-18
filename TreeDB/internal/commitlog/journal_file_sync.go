package commitlog

import (
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
		s.failure.Store(&journalFileSyncFailure{cause: err})
		if s.onError != nil {
			s.onError()
		}
		return err
	}
	return nil
}
