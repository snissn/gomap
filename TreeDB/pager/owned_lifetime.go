package pager

import (
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
)

var ErrClosing = errors.New("pager: closing or closed")
var ErrOwnedCoverage = errors.New("pager: constructor backing coverage unavailable")
var ErrRawExport = errors.New("pager: raw export unavailable during selected loan")

// constructorStamp is intrinsic and resident-only. File internals, runtime
// channels/goroutines, maps and platform mmap controls are not certified here.
// Consequently complete stays false, independently of known class census.
type constructorStamp struct {
	creator       *residentcredit.Scope
	knownBytes    atomic.Uint64
	complete      bool
	escaped       bool
	selectedLoans uint64
}

type pagerLifetime struct {
	mu      sync.Mutex // admission/Add and closing are one transition
	calls   sync.WaitGroup
	closeMu sync.Mutex // only terminal callers; operations never need this lock
	closing bool
	closed  bool
}

func (p *Pager) beginOperation() error {
	p.lifetime.mu.Lock()
	defer p.lifetime.mu.Unlock()
	if p.lifetime.closing {
		return ErrClosing
	}
	p.lifetime.calls.Add(1)
	return nil
}
func (p *Pager) endOperation() { p.lifetime.calls.Done() }

func (p *Pager) reserveKnown(n uint64, scan bool) error {
	if p.stamp.creator == nil || n == 0 {
		return nil
	}
	class, err := allocclass.ClassBytes(n, scan)
	if err != nil {
		// Ordinary unsupported platforms retain a raw census and incomplete stamp.
		class = n
	}
	if err := p.stamp.creator.ReserveStableMetadata(class); err != nil {
		return err
	}
	p.stamp.knownBytes.Add(class)
	return nil
}

// RetainConstructorLifetime retains the original actual creator before a
// Snapshot/token holder is born or registered. It grants no finite coverage.
func (p *Pager) RetainConstructorLifetime() (*residentcredit.Scope, error) {
	p.lifetime.mu.Lock()
	defer p.lifetime.mu.Unlock()
	if p.lifetime.closing {
		return nil, ErrClosing
	}
	creator := p.stamp.creator
	if creator == nil {
		return nil, nil
	} // legacy/unowned ordinary constructor
	if err := creator.RetainStableMetadata(); err != nil {
		return nil, err
	}
	return creator, nil
}

// MarkRawExport runs before exposing a Pager or exact raw handle. A selected
// loan and export are mutually exclusive under the same admission lock.
func (p *Pager) MarkRawExport() bool {
	if p == nil {
		return false
	}
	p.lifetime.mu.Lock()
	defer p.lifetime.mu.Unlock()
	if p.stamp.selectedLoans != 0 {
		return false
	}
	p.stamp.escaped = true
	return true
}

// RequireOwnedCapacity is an eligibility check, never an activation switch.
// The current constructor stamp intentionally lacks complete platform/control
// coverage, so every selected loan refuses before any request debit or exposure.
func (p *Pager) RequireOwnedCapacity(owner *residentcredit.Owner) error {
	if p == nil || owner == nil {
		return ErrOwnedCoverage
	}
	p.lifetime.mu.Lock()
	defer p.lifetime.mu.Unlock()
	if p.lifetime.closing || p.stamp.escaped || !p.stamp.complete || !owner.OwnsScope(p.stamp.creator) {
		return ErrOwnedCoverage
	}
	return nil
}

// Package-local faults exercise actual constructor and terminal custody, not
// a synthetic finite admission path. Tests must restore hooks and run serially.
var testOwnedOpenAfterFile func(*Pager) error
var testOwnedMunmap func([]byte) error
var testOwnedFileClose func(*os.File) error

func unmapOwnedChunk(b []byte) error {
	if testOwnedMunmap != nil {
		return testOwnedMunmap(b)
	}
	return munmapFile(b)
}
func closeOwnedFile(f *os.File) error {
	if testOwnedFileClose != nil {
		return testOwnedFileClose(f)
	}
	return f.Close()
}

// failedOpen returns nil only after checked cleanup completed. On cleanup
// failure the actual Pager, mappings, file and creator remain retryable.
func (p *Pager) failedOpen(cause error) (*Pager, error) {
	if err := p.Close(); err != nil {
		return p, errors.Join(cause, err)
	}
	return nil, cause
}

func newConstructorPager(path string, chunkSize int64, opts OpenOptions, readOnly bool) (*Pager, error) {
	var creator *residentcredit.Scope
	if opts.ResidentOwner != nil {
		var err error
		creator, err = opts.ResidentOwner.NewOrdinaryScope()
		if err != nil {
			return nil, err
		}
		// Every known allocation is rounded separately BEFORE its actual birth.
		for _, raw := range []struct {
			n    uint64
			scan bool
		}{
			{uint64(unsafe.Sizeof(Pager{})), true}, {uint64(len(path)), false},
			{uint64(unsafe.Sizeof(os.File{})), true},
		} {
			class, err := allocclass.ClassBytes(raw.n, raw.scan)
			if err != nil {
				class = raw.n
			}
			if err := creator.ReserveStableMetadata(class); err != nil {
				creator.ReleaseStableMetadata()
				return nil, err
			}
		}
	}
	p := &Pager{chunkSize: chunkSize, path: strings.Clone(path), readOnly: readOnly,
		mmapPopulate: opts.MmapPopulate, prefetchOnRead: opts.PrefetchOnRead,
		stamp: constructorStamp{creator: creator}}
	p.syncConcurrency.Store(1)
	// dirtyChunks runtime backing has no deterministic capacity certificate.
	// Its unknown family keeps complete=false, including on ordinary platforms.
	if !readOnly {
		p.dirtyChunks = make(map[int]struct{})
	}
	return p, nil
}
