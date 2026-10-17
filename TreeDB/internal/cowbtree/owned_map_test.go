package cowbtree

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

type ownedTestValue struct {
	refs     int
	destroys int
	fail     error
}
type ownedTestLifetime struct{}

func (ownedTestLifetime) Retain(v *ownedTestValue) {
	if v != nil {
		v.refs++
	}
}
func (ownedTestLifetime) Release(v *ownedTestValue) (OwnedMapCompletion, error) {
	if v == nil {
		return OwnedMapCompletion{}, nil
	}
	v.refs--
	if v.refs < 0 {
		panic("value edge double release")
	}
	if v.refs == 0 {
		v.destroys++
		return OwnedMapCompletion{}, v.fail
	}
	return OwnedMapCompletion{}, nil
}

type ownedTestLease struct {
	closed  int
	charge  uint64
	observe func()
}

func (l *ownedTestLease) Close() {
	if l.observe != nil {
		l.observe()
	}
	l.closed++
}
func ownedTestBuilder(t *testing.T, base *OwnedMap[int, *ownedTestValue], n int) (*OwnedMapBuilder[int, *ownedTestValue], *ownedTestLease) {
	t.Helper()
	lease := new(ownedTestLease)
	b, err := NewOwnedMapBuilder(base, n, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) { lease.charge = bytes; return lease, nil })
	if err != nil {
		t.Fatal(err)
	}
	return b, lease
}
func finishOwnedTest(t *testing.T, slot **OwnedMap[int, *ownedTestValue]) {
	m := *slot
	t.Helper()
	if !m.Release() {
		t.Fatal("unexpected extra map reference")
	}
	completion, err := m.Finish()
	*slot, m = nil, nil
	completion.Complete()
	if err != nil {
		t.Fatal(err)
	}
}
func TestOwnedMapSharesUntouchedNodesAndReleasesOriginalValues(t *testing.T) {
	b, firstLease := ownedTestBuilder(t, nil, 4096)
	values := make([]*ownedTestValue, 4096)
	for i := range values {
		values[i] = new(ownedTestValue)
		if err := b.Set(i, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	old := b.Seal()
	b, secondLease := ownedTestBuilder(t, old, 2)
	newValue := new(ownedTestValue)
	if err := b.Set(100, newValue); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(200); err != nil {
		t.Fatal(err)
	}
	next := b.Seal()
	if v, ok := old.Get(100); !ok || v != values[100] {
		t.Fatal("old value changed")
	}
	if _, ok := old.Get(200); !ok {
		t.Fatal("old delete leaked")
	}
	if v, ok := next.Get(100); !ok || v != newValue {
		t.Fatal("new value missing")
	}
	if _, ok := next.Get(200); ok {
		t.Fatal("delete missing")
	}
	// Root paths differ; a far untouched leaf remains the same physical node.
	oldLeaf, nextLeaf := old.tree.root, next.tree.root
	for !oldLeaf.leaf() {
		oldLeaf = (*oldLeaf.children)[len(*oldLeaf.children)-1]
	}
	for !nextLeaf.leaf() {
		nextLeaf = (*nextLeaf.children)[len(*nextLeaf.children)-1]
	}
	if oldLeaf != nextLeaf || oldLeaf.ownership.refs.Load() <= 0 {
		t.Fatal("untouched subtree copied or ownership lost")
	}
	// A shared internal subtree retains one incoming edge, without walking
	// down to increment each descendant again.
	shared := (*old.tree.root.children)[len(*old.tree.root.children)-1]
	if shared != (*next.tree.root.children)[len(*next.tree.root.children)-1] || shared.ownership.refs.Load() != 2 {
		t.Fatal("shared subtree edge missing")
	}
	finishOwnedTest(t, &old)
	if values[100].destroys != 1 || values[200].destroys != 1 || newValue.destroys != 0 || firstLease.closed != 0 {
		t.Fatal("old resource lifetime not sparse")
	}
	finishOwnedTest(t, &next)
	for i, v := range values {
		if v.refs != 0 || v.destroys != 1 {
			t.Fatalf("value%d refs%d destroys%d", i, v.refs, v.destroys)
		}
	}
	if newValue.destroys != 1 || firstLease.closed != 1 || secondLease.closed != 1 {
		t.Fatal("original containing receipts did not close exactly once")
	}
}
func TestOwnedMapDeleteRebalanceAndRootContractionKeepOldViews(t *testing.T) {
	b, _ := ownedTestBuilder(t, nil, 1024)
	values := make([]*ownedTestValue, 1024)
	for i := range values {
		values[i] = new(ownedTestValue)
		_ = b.Set(i, values[i])
	}
	old := b.Seal()
	b, _ = ownedTestBuilder(t, old, 1024)
	for i := range values {
		if err := b.Delete(i); err != nil {
			t.Fatal(err)
		}
	}
	next := b.Seal()
	if next.Len() != 0 || old.Len() != 1024 {
		t.Fatal("root contraction changed old map")
	}
	finishOwnedTest(t, &next)
	for _, v := range values {
		if v.destroys != 0 {
			t.Fatal("private removal released old retained resource")
		}
	}
	finishOwnedTest(t, &old)
	for _, v := range values {
		if v.destroys != 1 {
			t.Fatal("old resource leaked")
		}
	}
}
func TestOwnedMapCanceledOrRefusedPreparationKeepsOriginalEdges(t *testing.T) {
	b, _ := ownedTestBuilder(t, nil, 1)
	v := new(ownedTestValue)
	_ = b.Set(1, v)
	old := b.Seal()
	denied := errors.New("admission denied")
	if _, err := NewOwnedMapBuilder(old, 1, ownedTestLifetime{}, func(uint64) (OwnedMapLease, error) { return nil, denied }); !errors.Is(err, denied) {
		t.Fatal(err)
	}
	b, lease := ownedTestBuilder(t, old, 1)
	private := new(ownedTestValue)
	_ = b.Set(1, private)
	completion := b.Cancel()
	b = nil
	completion.Complete()
	if lease.closed != 1 || private.refs != 0 || private.destroys != 0 || v.refs != 1 {
		t.Fatal("canceled private value acquired published ownership")
	}
	finishOwnedTest(t, &old)
}
func TestOwnedMapFinalReleaseIsConstantAndSealedHeaderCannotMutate(t *testing.T) {
	b, _ := ownedTestBuilder(t, nil, 2048)
	for i := 0; i < 2048; i++ {
		_ = b.Set(i, new(ownedTestValue))
	}
	m := b.Seal()
	if allocs := testing.AllocsPerRun(100, func() {
		m.Retain()
		if m.Release() {
			panic("last edge")
		}
	}); allocs != 0 {
		t.Fatalf("retain/release allocated %g", allocs)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("sealed map mutated")
			}
		}()
		m.tree.Set(100, new(ownedTestValue))
	}()
	finishOwnedTest(t, &m)
}

func TestOwnedMapSealedOriginalRejectsEveryMutationEntrypoint(t *testing.T) {
	for _, n := range []int{0, 1} {
		b, _ := ownedTestBuilder(t, nil, n)
		if n != 0 {
			_ = b.Set(1, new(ownedTestValue))
		}
		m := b.Seal()
		tr := m.tree
		visit := func(int, *ownedTestValue) bool { return true }
		calls := map[string]func(){
			"Copy": func() { tr.Copy() }, "IsoCopy": func() { tr.IsoCopy() },
			"Set": func() { tr.Set(1, nil) }, "SetWithWork": func() { tr.SetWithWork(1, nil, nil) },
			"Delete": func() { tr.Delete(1) }, "Load": func() { tr.Load(1, nil) }, "Clear": func() { tr.Clear() },
			"ScanMut": func() { tr.ScanMut(visit) }, "GetMut": func() { tr.GetMut(1) },
			"AscendMut": func() { tr.AscendMut(1, visit) }, "ReverseMut": func() { tr.ReverseMut(visit) },
			"DescendMut": func() { tr.DescendMut(1, visit) }, "MinMut": func() { tr.MinMut() },
			"MaxMut": func() { tr.MaxMut() }, "PopMin": func() { tr.PopMin() }, "PopMax": func() { tr.PopMax() },
			"GetAtMut": func() { tr.GetAtMut(0) }, "DeleteAt": func() { tr.DeleteAt(0) },
			"IterMut": func() { tr.IterMut() }, "ValuesMut": func() { tr.ValuesMut() }, "KeyValuesMut": func() { tr.KeyValuesMut() },
		}
		for name, call := range calls {
			t.Run(name, func(t *testing.T) {
				defer func() {
					if recover() == nil {
						t.Fatal("sealed original accepted mutation capability")
					}
				}()
				call()
			})
		}
		if tr.Len() != n {
			t.Fatal("sealed original changed")
		}
		finishOwnedTest(t, &m)
	}
}
func TestOwnedMapFailureParksOriginalValueAndReceiptWithoutReplay(t *testing.T) {
	b, lease := ownedTestBuilder(t, nil, 1)
	failure := errors.New("original asset unlink failed")
	v := &ownedTestValue{fail: failure}
	_ = b.Set(1, v)
	m := b.Seal()
	if !m.Release() {
		t.Fatal("last release")
	}
	if err := attemptOwnedFinish(m); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := attemptOwnedFinish(m); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if lease.closed != 0 || v.destroys != 1 || m.tree.root.items[0].value != v || m.tree.root.ownership == nil {
		t.Fatal("failed original custody lost or replayed")
	}
}

func TestOwnedMapFailureReleasesIndependentSiblingsOnce(t *testing.T) {
	b, lease := ownedTestBuilder(t, nil, 256)
	failure := errors.New("failed original child asset")
	values := make([]*ownedTestValue, 256)
	for i := range values {
		values[i] = new(ownedTestValue)
		_ = b.Set(i, values[i])
	}
	values[0].fail = failure
	m := b.Seal()
	m.Release()
	if err := attemptOwnedFinish(m); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := attemptOwnedFinish(m); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	for i, v := range values {
		if v.destroys != 1 {
			t.Fatalf("independent value %d cleanup calls %d", i, v.destroys)
		}
	}
	if lease.closed != 0 {
		t.Fatal("containing receipt released while failed graph retained")
	}
}

func TestOwnedMapRefundWaitsForCallerAndParentAliasDetachment(t *testing.T) {
	b, first := ownedTestBuilder(t, nil, 256)
	for i := range 256 {
		_ = b.Set(i, new(ownedTestValue))
	}
	old := b.Seal()
	b, last := ownedTestBuilder(t, old, 1)
	_ = b.Delete(1)
	next := b.Seal()
	b = nil
	old.Release()
	completion, err := old.Finish()
	if err != nil {
		t.Fatal(err)
	}
	old = nil
	completion.Complete()
	if first.closed != 0 {
		t.Fatal("shared child-only cohort refunded early")
	}
	first.observe = func() {
		if old != nil || next != nil || b != nil {
			t.Error("child cohort refund preceded containing alias detachment")
		}
		if last.closed != 0 {
			t.Error("final containing header refunded before child cohort")
		}
	}
	last.observe = func() {
		if next != nil || b != nil {
			t.Error("final containing header alias survived refund")
		}
		if first.closed != 1 {
			t.Error("final containing receipt preceded child receipt")
		}
	}
	next.Release()
	completion, err = next.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if first.closed != 0 || last.closed != 0 {
		t.Fatal("Finish refunded before returning original completion")
	}
	next = nil
	completion.Complete()
	completion.Complete()
	if first.closed != 1 || last.closed != 1 {
		t.Fatal("original receipts not once-only")
	}
}

type atomicOwnedLifetime struct{}

func (atomicOwnedLifetime) Retain(v *atomic.Int64) { v.Add(1) }
func (atomicOwnedLifetime) Release(v *atomic.Int64) (OwnedMapCompletion, error) {
	v.Add(-1)
	return OwnedMapCompletion{}, nil
}
func TestOwnedMapConcurrentRetainAndIndependentLastNodeRelease(t *testing.T) {
	var leases atomic.Int64
	admit := func(uint64) (OwnedMapLease, error) { return atomicOwnedLease{closed: &leases}, nil }
	b, err := NewOwnedMapBuilder[int, *atomic.Int64](nil, 100, atomicOwnedLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]atomic.Int64, 100)
	for i := range values {
		_ = b.Set(i, &values[i])
	}
	old := b.Seal()
	b, err = NewOwnedMapBuilder(old, 1, atomicOwnedLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Delete(1)
	next := b.Seal()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				next.Retain()
				next.Release()
			}
		}()
	}
	if !old.Release() {
		t.Fatal("last old edge")
	}
	if err := finishAtomicOwned(&old); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !next.Release() {
		t.Fatal("last next edge")
	}
	if err := finishAtomicOwned(&next); err != nil {
		t.Fatal(err)
	}
	for i := range values {
		if values[i].Load() != 0 {
			t.Fatal("edge retained")
		}
	}
	if leases.Load() != 2 {
		t.Fatal("receipt count")
	}
}

func TestOwnedMapSealRetainsSharedChildDuringOlderViewCleanup(t *testing.T) {
	var closed atomic.Int64
	admit := func(uint64) (OwnedMapLease, error) { return atomicOwnedLease{closed: &closed}, nil }
	b, err := NewOwnedMapBuilder[int, *atomic.Int64](nil, 2048, atomicOwnedLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]atomic.Int64, 2048)
	for i := range values {
		_ = b.Set(i, &values[i])
	}
	old := b.Seal()
	b, _ = NewOwnedMapBuilder(old, 1, atomicOwnedLifetime{}, admit)
	_ = b.Delete(1)
	basis := b.Seal()
	b, _ = NewOwnedMapBuilder(basis, 1, atomicOwnedLifetime{}, admit)
	_ = b.Delete(2)
	done := make(chan error, 1)
	go func() {
		old.Release()
		done <- finishAtomicOwned(&old)
	}()
	next := b.Seal() // basis's original opening edge remains live
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	basis.Release()
	if err := finishAtomicOwned(&basis); err != nil {
		t.Fatal(err)
	}
	next.Release()
	if err := finishAtomicOwned(&next); err != nil {
		t.Fatal(err)
	}
	for i := range values {
		if values[i].Load() != 0 {
			t.Fatal("shared descriptor edge lost")
		}
	}
	if closed.Load() != 3 {
		t.Fatal("original receipt leaked")
	}
}

type atomicOwnedLease struct{ closed *atomic.Int64 }

func (l atomicOwnedLease) Close() { l.closed.Add(1) }

func attemptOwnedFinish(m *OwnedMap[int, *ownedTestValue]) error {
	completion, err := m.Finish()
	completion.Complete() // failed containing map is still charged
	return err
}
func finishAtomicOwned(slot **OwnedMap[int, *atomic.Int64]) error {
	completion, err := (*slot).Finish()
	*slot = nil
	completion.Complete()
	return err
}
