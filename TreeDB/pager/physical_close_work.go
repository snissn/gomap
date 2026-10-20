package pager

import (
	"runtime"
	"sync"
)

type physicalCloseState struct {
	mu              sync.Mutex
	phase, position int
	failure         error
	view            *chunkList
}

// closePhysical preserves the original mapping/file teardown and failure custody.
func (p *Pager) closePhysical() (bool, error) {
	if p == nil {
		return true, nil
	}
	s := &p.closeWork
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return false, s.failure
	}
	if s.phase == 7 {
		return true, nil
	}
	for {
		switch s.phase {
		case 0:
			p.mu.Lock()
			p.closing = true
			p.ownedFileCloseAttempted = true
			s.view = p.atomicChunks.Swap(nil)
			if p.growStop != nil {
				p.growStopOnce.Do(func() { close(p.growStop) })
			}
			p.mu.Unlock()
			s.phase = 1
		case 1:
			if p.growDone != nil {
				select {
				case <-p.growDone:
				default:
					return false, nil
				}
			}
			s.phase = 2
		case 2:
			p.growMu.Lock()
			p.mu.Lock()
			if s.position >= len(p.chunks) {
				p.mu.Unlock()
				p.growMu.Unlock()
				s.position = 0
				s.phase = 3
				continue
			}
			data := p.chunks[s.position]
			if data == nil || p.memoryOnly {
				p.chunks[s.position] = nil
				if s.position < len(p.mappings) {
					p.mappings[s.position] = mappingOwner{}
				}
				s.position++
				p.mu.Unlock()
				p.growMu.Unlock()
				continue
			}
			var err error
			if s.position < len(p.mappings) && p.mappings[s.position].issued {
				m := &p.mappings[s.position]
				if m.offset == 0 && !m.matches(data) {
					err = ErrMappingAuthority
				} else {
					err = unmapConstructor(m, m.remaining())
					if err == nil && m.remaining() == 0 {
						p.chunks[s.position] = nil
						*m = mappingOwner{}
						s.position++
					}
				}
			} else {
				err = munmapFile(data)
				if err == nil {
					p.chunks[s.position] = nil
					s.position++
				}
			}
			p.mu.Unlock()
			p.growMu.Unlock()
			if err != nil {
				return false, p.failPhysicalClose(err)
			}
		case 3:
			// Read descriptors must stop aliasing unmapped backing before its carrier ends.
			if s.view != nil && s.position < len(s.view.data) {
				s.view.data[s.position] = nil
				s.position++
				continue
			}
			p.mu.Lock()
			p.chunks = nil
			p.mappings = nil
			p.verified.Store(nil)
			p.prefetched.Store(nil)
			p.dirtyChunks = dirtyChunkBits{revision: p.dirtyChunks.revision + 1}
			p.fallback = nil
			p.mu.Unlock()
			s.view = nil
			s.phase = 4
		case 4:
			p.mu.Lock()
			if !p.memoryOnly && p.file != nil {
				if err := p.file.Close(); err != nil {
					p.mu.Unlock()
					return false, p.failPhysicalClose(err)
				}
			}
			p.file = nil
			p.mu.Unlock()
			s.phase = 5
		case 5:
			p.mu.Lock()
			owner, charge := p.ownedFileMetadata, p.ownedFileCharge
			p.path = ""
			p.ownedFileMetadata = nil
			p.ownedFileCharge = 0
			p.mu.Unlock()
			if owner != nil {
				owner.RemovePending(charge)
			}
			s.phase = 7
			return true, nil
		}
	}
}
func (p *Pager) failPhysicalClose(err error) error {
	if p.ownedFileMetadata != nil {
		p.closeWork.failure = err
	}
	p.ownedFileCloseErr = err
	if p.ownedFileMetadata != nil {
		p.ownedFileMetadata.CleanupFailed()
	}
	return err
}
func (p *Pager) closeOrdinaryPhysical() error {
	for {
		ready, err := p.closePhysical()
		if err != nil || ready {
			return err
		}
		runtime.Gosched()
	}
}
