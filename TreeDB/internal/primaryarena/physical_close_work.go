package primaryarena

import "sync"

type physicalCloseState struct {
	mu                 sync.Mutex
	phase, chunk, cell int
	failure            error
}

func (a *Arena) closePhysical() (bool, error) {
	if a == nil {
		return true, nil
	}
	s := &a.closeWork
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return false, s.failure
	}
	if s.phase == 5 {
		return true, nil
	}
	for {
		switch s.phase {
		case 0:
			s.phase = 1
		case 1:
			if err := a.pager.Close(); err != nil {
				s.failure = err
				a.metadata.CleanupFailed()
				return false, err
			}
			s.phase = 2
		case 2:
			done, err := a.pager.ClearPrimaryReadRootsWorkV6(nil)
			if err != nil {
				s.failure = err
				a.metadata.CleanupFailed()
				return false, err
			}
			if !done {
				return false, nil
			}
			s.phase = 3
		case 3:
			a.mu.Lock()
			if s.chunk >= len(a.chunks) {
				a.mu.Unlock()
				s.phase = 4
				continue
			}
			c := a.chunks[s.chunk]
			if c == nil {
				s.chunk++
				a.mu.Unlock()
				continue
			}
			if s.cell < len(c) {
				c[s.cell] = bank{}
				s.cell++
				a.mu.Unlock()
				continue
			}
			a.chunks[s.chunk] = nil
			s.cell = 0
			s.chunk++
			a.mu.Unlock()
		case 4:
			a.mu.Lock()
			a.ordinaryRelease = ReleaseQueue{}
			a.metadataDecoder = nil
			a.ownedMetadataDecoder = nil
			a.mu.Unlock()
			if err := a.metadata.Close(); err != nil {
				s.failure = err
				return false, err
			}
			s.phase = 5
			return true, nil
		}
	}
}
func (a *Arena) closeOrdinaryPhysical() error {
	for {
		done, err := a.closePhysical()
		if err != nil || done {
			return err
		}
	}
}
