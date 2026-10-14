package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

var ErrCloseWorkRequired = errors.New("pager: bounded physical close requires original work")

type physicalCloseState struct {
	mu              sync.Mutex
	phase, position int
	offset          uintptr
	native          bool
	failure         error
	view            *chunkList
}

// CloseWork continues this original Pager's physical close. It never waits for
// the grower across a bounded return or creates another disposal owner.
func (p *Pager) CloseWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrCloseWorkRequired
	}
	if p != nil && p.ownedFileMetadata != nil {
		return false, ErrCloseWorkRequired
	}
	return p.closePhysicalWork(true, w, nil)
}

// CloseWorkInto keeps the original producer refund cursor in the parent's
// preborn admitted receipt after this Pager's containing aliases are scrubbed.
func (p *Pager) CloseWorkInto(w *iterator.OrdinalScanWork, refund *retainedalloc.RemovalWork) (bool, error) {
	if w == nil || refund == nil {
		return false, ErrCloseWorkRequired
	}
	return p.closePhysicalWork(true, w, refund)
}
func (p *Pager) closePhysicalWork(native bool, w *iterator.OrdinalScanWork, refund *retainedalloc.RemovalWork) (bool, error) {
	if p == nil {
		return true, nil
	}
	s := &p.closeWork
	s.mu.Lock()
	defer s.mu.Unlock()
	if !native && s.native && s.phase != 7 {
		return false, ErrCloseWorkRequired
	}
	if s.failure != nil {
		return false, s.failure
	}
	if s.phase == 7 {
		return true, nil
	}
	if w != nil && (w.RecordLimit == 0 || w.ByteLimit == 0) {
		return false, nil
	}
	if native && !p.memoryOnly && !nativePartialMappingSupported() {
		return false, ErrMappingAuthority
	}
	for {
		switch s.phase {
		case 0:
			if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(Pager{}))+128); !ok || e != nil {
				return false, e
			}
			p.mu.Lock()
			// An old manual slice may use ordinary whole-slice disposal; it cannot be
			// silently promoted to pointer-unmap authority by its address.
			if native && !p.memoryOnly {
				if len(p.mappings) != len(p.chunks) {
					p.mu.Unlock()
					return false, ErrMappingAuthority
				}
			}
			p.closing = true
			s.native = native
			p.ownedFileCloseAttempted = true
			s.view = p.atomicChunks.Swap(nil)
			if p.growStop != nil {
				p.growStopOnce.Do(func() { close(p.growStop) })
			}
			p.mu.Unlock()
			s.phase = 1
		case 1:
			if ok, e := pagerReserve(w, 1, 128); !ok || e != nil {
				return false, e
			}
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
				if ok, e := pagerReserve(w, 1, 128); !ok || e != nil {
					p.mu.Unlock()
					p.growMu.Unlock()
					return false, e
				}
				p.chunks[s.position] = nil
				if s.position < len(p.mappings) {
					p.mappings[s.position] = mappingOwner{}
				}
				s.position++
				p.mu.Unlock()
				p.growMu.Unlock()
				continue
			}
			control := 2*uint64(unsafe.Sizeof(mappingOwner{})) + 128
			var e error
			if s.position < len(p.mappings) && p.mappings[s.position].issued {
				m := &p.mappings[s.position]
				if m.offset == 0 && !m.matches(data) {
					e = ErrMappingAuthority
				} else {
					amount := uint64(m.remaining())
					if w != nil {
						available := uint64(0)
						if w.Bytes < w.ByteLimit && w.ByteLimit-w.Bytes > control {
							available = w.ByteLimit - w.Bytes - control
						}
						if amount > available {
							amount = available
						}
					}
					amount = amount / uint64(os.Getpagesize()) * uint64(os.Getpagesize())
					if amount == 0 {
						p.mu.Unlock()
						p.growMu.Unlock()
						return false, nil
					}
					if ok, err := pagerReserve(w, 1, control+amount); !ok || err != nil {
						p.mu.Unlock()
						p.growMu.Unlock()
						return false, err
					}
					e = unmapConstructor(m, uintptr(amount))
					s.offset = m.offset
					if e == nil && m.remaining() == 0 {
						p.chunks[s.position] = nil
						*m = mappingOwner{}
						s.position++
						s.offset = 0
					}
				}
			} else if native {
				e = ErrMappingAuthority
			} else {
				if ok, err := pagerReserve(w, 1, control+uint64(len(data))); !ok || err != nil {
					p.mu.Unlock()
					p.growMu.Unlock()
					return false, err
				}
				e = munmapFile(data)
				if e == nil {
					p.chunks[s.position] = nil
					s.position++
				}
			}
			p.mu.Unlock()
			p.growMu.Unlock()
			if e != nil {
				return false, p.failPhysicalClose(e)
			}
		case 3:
			// Growth maintains an independent read descriptor backing. Clear each cell
			// before dropping the carrier, including an alias to an already unmapped view.
			if s.view != nil && s.position < len(s.view.data) {
				if ok, e := pagerReserve(w, 1, 128); !ok || e != nil {
					return false, e
				}
				s.view.data[s.position] = nil
				s.position++
				continue
			}
			if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(Pager{}))+128); !ok || e != nil {
				return false, e
			}
			p.mu.Lock()
			p.chunks = nil
			p.mappings = nil
			p.verified.Store(nil)
			p.prefetched.Store(nil)
			p.dirtyChunks.words = nil
			p.fallback = nil
			p.primaryBanks.Store(nil)
			p.mu.Unlock()
			s.view = nil
			s.phase = 4
		case 4:
			if ok, e := pagerReserve(w, 1, 512); !ok || e != nil {
				return false, e
			}
			p.mu.Lock()
			if !p.memoryOnly && p.file != nil {
				if e := p.file.Close(); e != nil {
					p.mu.Unlock()
					return false, p.failPhysicalClose(e)
				}
			}
			p.file = nil
			p.mu.Unlock()
			s.phase = 5
		case 5:
			p.mu.Lock()
			owner, charge := p.ownedFileMetadata, p.ownedFileCharge
			p.mu.Unlock()
			if native && owner != nil {
				if refund == nil {
					return false, ErrCloseWorkRequired
				}
				ready, e := refund.BindPendingWork(owner, charge, w)
				if e != nil || !ready {
					return false, e
				}
			}
			if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(Pager{}))+128); !ok || e != nil {
				return false, e
			}
			p.mu.Lock()
			p.path = ""
			p.ownedFileMetadata = nil
			p.ownedFileCharge = 0
			p.mu.Unlock()
			if !native && owner != nil {
				owner.RemovePending(charge)
			}
			if native && owner != nil {
				s.phase = 6
			} else {
				s.phase = 7
				return true, nil
			}
		case 6:
			if refund == nil {
				return false, ErrCloseWorkRequired
			}
			ready, e := refund.ContinueWork(w)
			if e != nil || !ready {
				return false, e
			}
			s.phase = 7
			return true, nil
		}
	}
}
func (p *Pager) failPhysicalClose(e error) error {
	if p.closeWork.native || p.ownedFileMetadata != nil {
		p.closeWork.failure = e
	}
	p.ownedFileCloseErr = e
	if p.ownedFileMetadata != nil {
		p.ownedFileMetadata.CleanupFailed()
	}
	return e
}
func (p *Pager) closeOrdinaryPhysical() error {
	for {
		ready, e := p.closePhysicalWork(false, nil, nil)
		if e != nil || ready {
			return e
		}
		runtime.Gosched()
	}
}

// SupportsBoundedPhysicalClose is a constructor/platform property. Callers
// check it before acquiring native custody; it never discovers mapping owners.
func (p *Pager) SupportsBoundedPhysicalClose() bool {
	return p != nil && (p.memoryOnly || nativePartialMappingSupported())
}
