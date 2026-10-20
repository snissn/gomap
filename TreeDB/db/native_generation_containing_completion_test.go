package db

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

type generationCompletionBudget struct {
	actual  *memtable.COWBudget
	observe func()
}
type generationCompletionLease struct {
	actual  retainedalloc.Lease
	bytes   uint64
	observe func()
}

func (b *generationCompletionBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	l, err := b.actual.AcquireRetention(n)
	if err != nil {
		return nil, err
	}
	return &generationCompletionLease{actual: l, bytes: n, observe: b.observe}, nil
}
func (l *generationCompletionLease) Resize(n uint64) error {
	if n < l.bytes {
		l.observe()
	}
	err := l.actual.Resize(n)
	if err == nil {
		l.bytes = n
	}
	return err
}
func (l *generationCompletionLease) Close() { l.actual.Close() }

// Drives the actual DB discovery-withdrawal caller. Its retired generation
// header must remain charged through original Close/once/lock completion.
func TestNativeGenerationContainingCompletionAfterOriginalClose(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	g, err := newIndexGenWithMetadata(1, nil, nil, nil, &owner)
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{idxAll: map[uint64]*indexGen{g.id: g}}
	db.idx.Store(g)
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	observed, locked, discoverable := false, false, false
	capture, err := owner.Enroll(&generationCompletionBudget{actual: budget, observe: func() {
		if g.metadata != nil {
			return
		}
		observed = true
		if !g.wrapperMu.TryLock() {
			locked = true
		} else {
			g.wrapperMu.Unlock()
		}
		discoverable = db.idx.Load() != nil || len(db.idxAll) != 0 || g.registry != nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Exercise retained fallback capacity under the SAME original owner.
	var pins [20]int64
	for i := range pins {
		pins[i], err = g.registry.Register(uint64(i + 1))
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range pins {
		g.registry.Unregister(id)
	}
	before := owner.Bytes()
	if err = db.closeAllIndexes(); err != nil {
		t.Fatal(err)
	}
	if !observed || locked || discoverable {
		t.Fatalf("actual containing refund: observed=%t locked=%t discoverable=%t", observed, locked, discoverable)
	}
	if owner.Bytes() >= before {
		t.Fatal("completed generation retained containing charge")
	}
	capture.Close()
	budget.Close()
}

func TestNativeGhostContainingCompletionAfterOriginalDBAliasDetach(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	m, err := newIndexGhostManager(&owner)
	if err != nil {
		t.Fatal(err)
	}
	g, err := newIndexGenWithMetadata(1, nil, nil, nil, &owner)
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{ghostManager: m}
	m.db = db
	if metadata, charge, err := m.add(g); err != nil || metadata != nil || charge != 0 {
		t.Fatalf("ghost enqueue=(%p,%d,%v)", metadata, charge, err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	observed, locked, aliased := false, false, false
	capture, err := owner.Enroll(&generationCompletionBudget{actual: budget, observe: func() {
		if m.metadata != nil {
			return
		}
		observed = true
		if !m.retirementMu.TryLock() {
			locked = true
		} else {
			m.retirementMu.Unlock()
		}
		aliased = db.ghostManager != nil || m.db != nil || m.ghosts != nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	db.closeGhostManagerCompletionV6(true)
	if !observed || locked || aliased {
		t.Fatalf("ghost containing refund: observed=%t locked=%t aliased=%t", observed, locked, aliased)
	}
	capture.Close()
	budget.Close()
	if owner.Bytes() != 0 {
		t.Fatalf("original ghost+generation containing charge=%d", owner.Bytes())
	}
}
