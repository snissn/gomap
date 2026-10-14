package freelist

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
	"math"
	"unsafe"
)

var ErrAppendCapacityContinuation = errors.New("freelist: append backing needs admitted capacity continuation")
var ErrAppendRetireContinuation = errors.New("freelist: installed append needs retained trie retirement continuation")
var ErrAppendRangeContinuation = errors.New("freelist: append reservation gap needs retained range continuation")

// AppendWork is embedded in its producer's admitted containing control. It uses
// the original allocator transaction and Pager Growth; it issues no second
// reservation authority. Until installed, ordinary writers can still use the
// allocator. A changed transaction/count invalidates this private operation.
type AppendWork struct {
	allocator   *Allocator
	transaction *FreelistTxn
	growth      *pager.Growth
	id, count   uint64
	phase       uint8
}

func appendReserve(w *iterator.OrdinalScanWork, r, b uint64) (bool, error) {
	if w == nil {
		return true, nil
	}
	if r > w.RecordLimit || b > w.ByteLimit {
		return false, &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
	}
	return w.Reserve(r, b), nil
}
func (s *AppendWork) Step(a *Allocator, w *iterator.OrdinalScanWork) (uint64, bool, error) {
	if a == nil || a.pager == nil {
		return 0, false, ErrGenerationFormat
	}
	if s.phase == 2 {
		if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
			return 0, false, e
		}
		if s.allocator != a {
			return 0, false, ErrGenerationParent
		}
		return s.id, true, nil
	}
	if ok, e := appendReserve(w, 1, 2*uint64(unsafe.Sizeof(*s))+256); !ok || e != nil {
		return 0, false, e
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.txn == nil {
		return 0, false, ErrGenerationFormat
	}
	if a.cow.prepared != nil {
		return 0, false, nil
	}
	t := a.cow.txn
	if e := t.valid(); e != nil {
		return 0, false, e
	}
	// Recheck on EVERY resumed step under the original lock: a reused-page
	// allocation can consume backing capacity without moving highWater/count.
	if len(t.allocated) == cap(t.allocated) {
		return 0, false, ErrAppendCapacityContinuation
	}
	if s.phase == 0 {
		// No Go append allocation is permitted in the guarded install callback.
		if t.highWater == math.MaxUint64 || t.highWater < a.pager.PageCount() {
			return 0, false, ErrGenerationFormat
		}
	} else if s.allocator != a || s.transaction != t || s.id != t.highWater || s.count != a.pager.PageCount() {
		return 0, false, ErrGenerationParent
	}
	ledger := t.ledger
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	// These are the actual existing map lookup / range and candidate probes.
	// The conservative complete bound is reserved before reservedLocked reads.
	if ok, e := appendReserve(w, 1+uint64(len(ledger.burnedTails)+len(ledger.candidates)), 64+uint64(len(ledger.burnedTails))*16+uint64(len(ledger.candidates))*uint64(unsafe.Sizeof(reservation{}))); !ok || e != nil {
		return 0, false, e
	}
	if ledger.reservedLocked(t.highWater) {
		return 0, false, ErrAppendRangeContinuation
	}
	if s.phase == 0 {
		if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
			return 0, false, e
		}
		s.allocator, s.transaction, s.id, s.count = a, t, t.highWater, a.pager.PageCount()
		s.phase = 1
	}
	if s.growth == nil {
		g, ready, e := pager.NewGrowthForPager(a.pager, s.id+1, w)
		if !ready || e != nil {
			return 0, false, e
		}
		s.growth = g
	}
	// No allocations or locks occur in this guarded callback. Appending uses
	// already existing original capacity; the same install is used ordinarily.
	ready, e := s.growth.StepWithInstallAtCount(a.pager, s.count, w, 2, 64, func() {
		t.installAppend(s.id)
		a.stats.AllocPages++
		a.stats.AppendAllocPages++
		a.lastAlloc = s.id
	})
	if ready && e == nil {
		s.growth = nil
		s.phase = 2
	}
	return s.id, ready, e
}

// Cancel preserves original custody on every refusal. The original synchronous
// trie retirement is NOT a bounded algorithm. Its proven summary-only lower
// bound already exceeds Q32; larger policies also refuse until the actual
// original trie mutation has a retained continuation. Nil is diagnostic teardown.
func (s *AppendWork) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if s.allocator == nil {
		return true, nil
	}
	a := s.allocator
	if s.phase == 2 {
		if w != nil {
			r := uint64(chunkTrieDepth*16 + freelistChunkSize + chunkTrieDepth + 2)
			b := uint64((chunkTrieDepth+1))*uint64(unsafe.Sizeof(stateNode{})) + uint64(unsafe.Sizeof(stateChunk{}))
			if r > w.RecordLimit || b > w.ByteLimit {
				return false, &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
			}
			return false, ErrAppendRetireContinuation
		}
		a.mu.Lock()
		if a.cow == nil || a.cow.txn == nil || a.cow.prepared != nil {
			a.mu.Unlock()
			return false, ErrGenerationParent
		}
		e := a.retireCOWLocked([]uint64{s.id}, a.cow.generation.commitSeq+1)
		a.mu.Unlock()
		if e != nil {
			return false, e
		}
		s.phase = 3
	} else if s.growth != nil {
		ready, e := s.growth.Cancel(w)
		if !ready || e != nil {
			return false, e
		}
		s.growth = nil
		s.phase = 3
	}
	if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
		return false, e
	}
	*s = AppendWork{}
	return true, nil
}
func (s *AppendWork) Installed() bool { return s != nil && s.phase == 2 }
