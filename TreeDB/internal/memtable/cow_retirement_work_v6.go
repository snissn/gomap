package memtable

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

var ErrCOWResourceWork = errors.New("immutable generation requires checked resource work")

// The SAME generation slot owns the producer. Completion is only its original
// accounting lease, returned after the producer detached physical aliases and
// unlocked. The slot clears its provider before closing that lease.
type COWResourceReleaseProviderV6 interface {
	ReleaseCOWResourceWorkV6(*iterator.OrdinalScanWork) (consumed, ready bool, completion retainedalloc.Lease, err error)
}
type cowRetirementCompletionV6 struct {
	budget                    *COWBudget
	charge                    uint64
	frozen, retired, prepared bool
}

func (x cowRetirementCompletionV6) consume() {
	if x.budget == nil {
		return
	}
	b := x.budget
	b.mu.Lock()
	b.stats.TotalBytes -= x.charge
	if x.prepared {
		b.stats.DeferredBytes -= x.charge
		b.stats.RetiredBytes -= x.charge
	} else {
		b.stats.HistoryBytes -= x.charge
		b.stats.Generations--
		if x.frozen {
			b.stats.Sources--
		}
		if x.retired {
			b.stats.RetiredBytes -= x.charge
		}
	}
	b.releaseControlLocked()
	b.mu.Unlock()
}
func cowRetirementReserveV6(w *iterator.OrdinalScanWork, n, b uint64) bool {
	return w == nil || w.Reserve(n, b)
}

func releaseCOWOwnerV6(slot *cowResourceOwner, w *iterator.OrdinalScanWork) (bool, error) {
	if !cowRetirementReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*slot))) {
		return false, nil
	}
	if slot.provider != nil {
		producer := slot.provider
		consumed, ready, completion, err := producer.ReleaseCOWResourceWorkV6(w)
		producer = nil
		if !ready || err != nil {
			return false, err
		}
		if !consumed {
			return false, ErrCOWResourceWork
		}
		slot.provider = nil
		if completion != nil {
			completion.Close()
			completion = nil
		}
		return true, nil
	}
	if slot.release != nil {
		// Legacy callbacks cannot become one bounded unit merely by wrapping them.
		if w != nil {
			return false, ErrCOWResourceWork
		}
		callback := slot.release
		callback()
		callback = nil
		slot.release = nil
	}
	return true, nil
}

// All state remains in the original admitted generation. In-flight is distinct
// from complete; duplicate native callers yield, ordinary callers join through
// its existing budget lock and embedded condition. Callbacks run outside locks.
func (g *cowGeneration) releaseCompletionV6(w *iterator.OrdinalScanWork) (bool, cowRetirementCompletionV6, error) {
	b := g.budget
	b.mu.Lock()
	for g.drainClaimed && !g.drainDone && g.drainError == nil {
		if w != nil {
			b.mu.Unlock()
			return false, cowRetirementCompletionV6{}, nil
		}
		g.drainCondition.Wait()
	}
	if g.drainError != nil {
		err := g.drainError
		b.mu.Unlock()
		return false, cowRetirementCompletionV6{}, err
	}
	if g.drainDone {
		b.mu.Unlock()
		return true, cowRetirementCompletionV6{}, nil
	}
	g.drainClaimed = true
	b.mu.Unlock()
	for g.drainIndex < len(g.owners) {
		i := len(g.owners) - 1 - g.drainIndex
		ready, err := releaseCOWOwnerV6(&g.owners[i], w)
		if !ready || err != nil {
			hard := err != nil && !errors.Is(err, ErrCOWResourceWork) && !errors.Is(err, retainedalloc.ErrWorkRequired)
			b.mu.Lock()
			g.drainClaimed = false
			if hard {
				g.drainError = err
			}
			g.drainCondition.Broadcast()
			b.mu.Unlock()
			return false, cowRetirementCompletionV6{}, err
		}
		g.drainIndex++
	}
	if !cowRetirementReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*g))+2*uint64(unsafe.Sizeof(*b))) {
		b.mu.Lock()
		g.drainClaimed = false
		g.drainCondition.Broadcast()
		b.mu.Unlock()
		return false, cowRetirementCompletionV6{}, nil
	}
	b.mu.Lock()
	completion := cowRetirementCompletionV6{budget: b, charge: g.history, frozen: g.frozen, retired: g.retired}
	g.owners, g.ids, g.history = nil, nil, 0
	g.drainDone = true
	g.drainClaimed = false
	g.drainCondition.Broadcast()
	b.mu.Unlock()
	return true, completion, nil
}
func (p *COWPrepared) releaseCompletionV6(w *iterator.OrdinalScanWork) (bool, cowRetirementCompletionV6, error) {
	b := p.deferredBudget
	b.mu.Lock()
	for p.drainClaimed && !p.drainDone && p.drainError == nil {
		if w != nil {
			b.mu.Unlock()
			return false, cowRetirementCompletionV6{}, nil
		}
		p.drainCondition.Wait()
	}
	if p.drainError != nil {
		err := p.drainError
		b.mu.Unlock()
		return false, cowRetirementCompletionV6{}, err
	}
	if p.drainDone {
		b.mu.Unlock()
		return true, cowRetirementCompletionV6{}, nil
	}
	p.drainClaimed = true
	b.mu.Unlock()
	for p.drainIndex < len(p.owners) {
		i := len(p.owners) - 1 - p.drainIndex
		ready, err := releaseCOWOwnerV6(&p.owners[i], w)
		if !ready || err != nil {
			hard := err != nil && !errors.Is(err, ErrCOWResourceWork) && !errors.Is(err, retainedalloc.ErrWorkRequired)
			b.mu.Lock()
			p.drainClaimed = false
			if hard {
				p.drainError = err
			}
			p.drainCondition.Broadcast()
			b.mu.Unlock()
			return false, cowRetirementCompletionV6{}, err
		}
		p.drainIndex++
	}
	if !cowRetirementReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*p))+2*uint64(unsafe.Sizeof(*b))) {
		b.mu.Lock()
		p.drainClaimed = false
		p.drainCondition.Broadcast()
		b.mu.Unlock()
		return false, cowRetirementCompletionV6{}, nil
	}
	b.mu.Lock()
	completion := cowRetirementCompletionV6{budget: b, charge: p.deferredCharge, prepared: true}
	p.owners, p.deferredCharge = nil, 0
	p.drainDone = true
	p.drainClaimed = false
	p.drainCondition.Broadcast()
	b.mu.Unlock()
	return true, completion, nil
}

// DrainWithWork drives the SAME retirement. Each original alias is cleared
// after actual producer consumption, BEFORE its containing accounting refund.
func (r *COWRetirement) DrainWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if r == nil {
		return true, nil
	}
	if r.generation != nil {
		ready, completion, err := r.generation.releaseCompletionV6(w)
		if !ready || err != nil {
			return false, err
		}
		r.generation = nil
		completion.consume()
	}
	if r.prepared != nil {
		ready, completion, err := r.prepared.releaseCompletionV6(w)
		if !ready || err != nil {
			return false, err
		}
		r.prepared = nil
		completion.consume()
	}
	if r.viewLease != nil {
		if !cowRetirementReserveV6(w, 1, 2*uint64(unsafe.Sizeof(cowViewReleaseLease{}))+2*uint64(unsafe.Sizeof(COWBudget{}))) {
			return false, nil
		}
		lease := r.viewLease
		r.viewLease = nil
		lease.Close()
		lease = nil
	}
	return true, nil
}
func (r *COWRetirement) DrainWithError() error { _, err := r.DrainWithWork(nil); return err }
