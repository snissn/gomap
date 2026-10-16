package primaryarena

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"sync"
	"unsafe"
)

var ErrCloseWorkRequired = errors.New("primary arena: bounded physical close requires original work")

type physicalCloseState struct {
	mu                 sync.Mutex
	native             bool
	phase, chunk, cell int
	failure            error
	pagerRefund        retainedalloc.RemovalWork
}

func (a *Arena) CloseWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrCloseWorkRequired
	}
	return a.closePhysicalWork(true, w)
}
func (a *Arena) closePhysicalWork(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if a == nil {
		return true, nil
	}
	s := &a.closeWork
	s.mu.Lock()
	defer s.mu.Unlock()
	if !native && s.native && s.phase != 5 {
		return false, ErrCloseWorkRequired
	}
	if s.failure != nil {
		return false, s.failure
	}
	if s.phase == 5 {
		return true, nil
	}
	if w != nil && (w.RecordLimit == 0 || w.ByteLimit == 0) {
		return false, nil
	}
	for {
		switch s.phase {
		case 0:
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(physicalCloseState{}))+128) {
				return false, nil
			}
			s.native = native
			s.phase = 1
		case 1:
			var done bool
			var e error
			if native {
				done, e = a.pager.CloseWorkInto(w, &s.pagerRefund)
			} else {
				e = a.pager.Close()
				done = e == nil
			}
			if e != nil {
				s.failure = e
				a.metadata.CleanupFailed()
				return false, e
			}
			if !done {
				return false, nil
			}
			s.phase = 2
		case 2:
			done, e := a.pager.ClearPrimaryReadRootsWorkV6(w)
			if e != nil {
				s.failure = e
				a.metadata.CleanupFailed()
				return false, e
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
				if !reserve(w, 1, 2*uint64(unsafe.Sizeof(c))+64) {
					a.mu.Unlock()
					return false, nil
				}
				s.chunk++
				a.mu.Unlock()
				continue
			}
			if s.cell < len(c) {
				if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))+64) {
					a.mu.Unlock()
					return false, nil
				}
				c[s.cell] = bank{}
				s.cell++
				a.mu.Unlock()
				continue
			}
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(c))+64) {
				a.mu.Unlock()
				return false, nil
			}
			a.chunks[s.chunk] = nil
			s.cell = 0
			s.chunk++
			a.mu.Unlock()
		case 4:
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(Arena{}))+128) {
				return false, nil
			}
			a.mu.Lock()
			a.ordinaryRelease = ReleaseQueue{}
			a.metadataDecoder = nil
			a.ownedMetadataDecoder = nil
			a.borrowedMetadataDecoder = nil
			clear(a.metadataDecodeScratch[:])
			a.mu.Unlock()
			var done bool
			var e error
			if native {
				done, e = a.metadata.CloseWork(w)
			} else {
				e = a.metadata.Close()
				done = e == nil
			}
			if e != nil {
				s.failure = e
				return false, e
			}
			if !done {
				return false, nil
			}
			s.phase = 5
			return true, nil
		}
	}
}
func (a *Arena) closeOrdinaryPhysical() error {
	for {
		done, e := a.closePhysicalWork(false, nil)
		if e != nil || done {
			return e
		}
	}
}
