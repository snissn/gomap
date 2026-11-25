package cowbtree

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type perfOwnedValue struct {
	refs atomic.Int64
}

type perfOwnedLifetime struct{}

func (perfOwnedLifetime) Retain(v *perfOwnedValue) {
	if v != nil {
		v.refs.Add(1)
	}
}

func (perfOwnedLifetime) Release(v *perfOwnedValue) (OwnedMapCompletion, error) {
	if v != nil {
		if v.refs.Add(-1) < 0 {
			panic("perf value double release")
		}
	}
	return OwnedMapCompletion{}, nil
}

type perfNoopLease struct{}

func (perfNoopLease) Close() {}

func perfNoopAdmit(uint64) (OwnedMapLease, error) {
	return perfNoopLease{}, nil
}

func TestOwnedMapFusedLifecycleAndPartialFailure(t *testing.T) {
	const count = 256 // degree 32 => height 2 (internal root + leaves)

	// 1. Initial build, builder Get/Set/Delete, and Seal.
	b, firstLease := ownedTestBuilder(t, nil, count+2)
	values := make([]*ownedTestValue, count)
	for i := range values {
		values[i] = new(ownedTestValue)
		if err := b.Set(i, values[i]); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if got, ok := b.Get(42); !ok || got != values[42] {
		t.Fatalf("builder Get(42) = (%v, %v), want (%v, true)", got, ok, values[42])
	}
	// Set and delete an extra transient key before sealing.
	transient := new(ownedTestValue)
	if err := b.Set(count+10, transient); err != nil {
		t.Fatalf("Set(transient): %v", err)
	}
	if err := b.Delete(count + 10); err != nil {
		t.Fatalf("Delete(transient): %v", err)
	}
	v1 := b.Seal()
	if v1 == nil || v1.Len() != count {
		t.Fatalf("v1 Len = %d, want %d", v1.Len(), count)
	}
	if v1.tree.Height() < 2 {
		t.Fatalf("expected height >= 2 for %d items, got %d", count, v1.tree.Height())
	}
	if transient.refs != 0 || transient.destroys != 0 {
		t.Fatalf("transient value retained before seal: refs=%d destroys=%d", transient.refs, transient.destroys)
	}
	// Second Seal on an already-sealed builder is a safe nil no-op.
	if b.Seal() != nil {
		t.Fatal("second Seal() returned non-nil map")
	}

	// 2. Structural sharing across versions + Cancel isolation.
	cancelB, cancelLease := ownedTestBuilder(t, v1, 2)
	canceledVal := new(ownedTestValue)
	if err := cancelB.Set(5, canceledVal); err != nil {
		t.Fatalf("cancelB.Set: %v", err)
	}
	if err := cancelB.Delete(6); err != nil {
		t.Fatalf("cancelB.Delete: %v", err)
	}
	cancelComp := cancelB.Cancel()
	cancelComp.Complete()
	if cancelLease.closed != 1 {
		t.Fatalf("cancelLease.closed = %d, want 1", cancelLease.closed)
	}
	if canceledVal.refs != 0 || canceledVal.destroys != 0 {
		t.Fatalf("canceled value mutated lifetime: refs=%d destroys=%d", canceledVal.refs, canceledVal.destroys)
	}
	// Second Cancel is a safe no-op.
	cancelB.Cancel().Complete()
	if cancelLease.closed != 1 {
		t.Fatalf("second Cancel closed lease again: %d", cancelLease.closed)
	}

	b2, secondLease := ownedTestBuilder(t, v1, 2)
	updatedVal := new(ownedTestValue)
	if err := b2.Set(10, updatedVal); err != nil {
		t.Fatalf("b2.Set: %v", err)
	}
	if err := b2.Delete(20); err != nil {
		t.Fatalf("b2.Delete: %v", err)
	}
	v2 := b2.Seal()
	if v2.Len() != count-1 || v1.Len() != count {
		t.Fatalf("lens mismatch: v1=%d v2=%d", v1.Len(), v2.Len())
	}
	if got, ok := v1.Get(10); !ok || got != values[10] {
		t.Fatal("v1 modified by v2 Set")
	}
	if got, ok := v2.Get(10); !ok || got != updatedVal {
		t.Fatal("v2 missing updated value")
	}
	if _, ok := v2.Get(20); ok {
		t.Fatal("v2 still has deleted key")
	}
	// Untouched rightmost child is physically shared with refcount 2.
	sharedChild := (*v1.tree.root.children)[len(*v1.tree.root.children)-1]
	if sharedChild != (*v2.tree.root.children)[len(*v2.tree.root.children)-1] {
		t.Fatal("expected rightmost child to be physically shared")
	}
	if got := sharedChild.ownership.refs.Load(); got != 2 {
		t.Fatalf("shared child refs = %d, want 2", got)
	}

	// 3. Verify clean Finish scrubs root.ownership == nil and refunds leases accurately.
	v1Root := v1.tree.root
	v2Root := v2.tree.root
	if !v1.Release() {
		t.Fatal("v1 unexpected extra ref")
	}
	comp1, err := v1.Finish()
	if err != nil {
		t.Fatalf("v1.Finish: %v", err)
	}
	comp1.Complete()
	if v1Root.ownership != nil || v1Root.items != nil || v1Root.children != nil {
		t.Fatal("v1Root was not scrubbed after clean Finish")
	}
	if v1.tree.root != nil || v1.tree.sealed {
		t.Fatal("v1 header tree not scrubbed after clean Finish")
	}
	if _, err := NewOwnedMapBuilder(v1, 1, ownedTestLifetime{}, perfNoopAdmit); !errors.Is(err, ErrOwnedMap) {
		t.Fatalf("forking finished map returned %v, want ErrOwnedMap", err)
	}
	if firstLease.closed != 0 {
		t.Fatalf("firstLease closed early while shared by v2: %d", firstLease.closed)
	}

	if !v2.Release() {
		t.Fatal("v2 unexpected extra ref")
	}
	comp2, err := v2.Finish()
	if err != nil {
		t.Fatalf("v2.Finish: %v", err)
	}
	comp2.Complete()
	if v2Root.ownership != nil || v2Root.items != nil || v2Root.children != nil {
		t.Fatal("v2Root was not scrubbed after clean Finish")
	}
	if sharedChild.ownership != nil || sharedChild.items != nil {
		t.Fatal("sharedChild was not scrubbed after last owner Finish")
	}
	if firstLease.closed != 1 || secondLease.closed != 1 {
		t.Fatalf("lease close counts = (%d, %d), want (1, 1)", firstLease.closed, secondLease.closed)
	}
	for i, v := range values {
		if v.refs != 0 || v.destroys != 1 {
			t.Fatalf("value[%d] refs=%d destroys=%d", i, v.refs, v.destroys)
		}
	}
	if updatedVal.refs != 0 || updatedVal.destroys != 1 {
		t.Fatalf("updatedVal refs=%d destroys=%d", updatedVal.refs, updatedVal.destroys)
	}

	// 4. Verify Seal performs 0 heap allocations on fused headers & composite nodes.
	const sealRuns = 32
	seedB, err := NewOwnedMapBuilder[uint64, *perfOwnedValue](nil, count, perfOwnedLifetime{}, perfNoopAdmit)
	if err != nil {
		t.Fatal(err)
	}
	seedVals := make([]perfOwnedValue, count)
	for i := range seedVals {
		if err := seedB.Set(uint64(i), &seedVals[i]); err != nil {
			t.Fatal(err)
		}
	}
	seed := seedB.Seal()
	var repl perfOwnedValue
	builders := make([]*OwnedMapBuilder[uint64, *perfOwnedValue], sealRuns+1)
	sealed := make([]*OwnedMap[uint64, *perfOwnedValue], sealRuns+1)
	for i := range builders {
		builders[i], err = NewOwnedMapBuilder(seed, 1, perfOwnedLifetime{}, perfNoopAdmit)
		if err != nil {
			t.Fatal(err)
		}
		if err := builders[i].Set(uint64(i), &repl); err != nil {
			t.Fatal(err)
		}
	}
	idx := 0
	if allocs := testing.AllocsPerRun(sealRuns, func() {
		sealed[idx] = builders[idx].Seal()
		idx++
	}); allocs != 0 {
		t.Fatalf("OwnedMapBuilder.Seal allocated %g times, want 0", allocs)
	}
	for i := 0; i < idx; i++ {
		if !sealed[i].Release() {
			t.Fatal("unexpected extra ref on sealed map")
		}
		c, err := sealed[i].Finish()
		if err != nil {
			t.Fatal(err)
		}
		c.Complete()
	}
	if !seed.Release() {
		t.Fatal("unexpected extra ref on seed")
	}
	seedComp, err := seed.Finish()
	if err != nil {
		t.Fatal(err)
	}
	seedComp.Complete()
	for i := range seedVals {
		if seedVals[i].refs.Load() != 0 {
			t.Fatalf("seedVals[%d] leaked refs=%d", i, seedVals[i].refs.Load())
		}
	}
	if repl.refs.Load() != 0 {
		t.Fatalf("repl leaked refs=%d", repl.refs.Load())
	}

	// 5. Partial release failure parks exact graph, re-observes error idempotently, and retains lease.
	failErr := errors.New("partial release failure")
	fb, failLease := ownedTestBuilder(t, nil, count)
	failVals := make([]*ownedTestValue, count)
	for i := range failVals {
		failVals[i] = new(ownedTestValue)
		if err := fb.Set(i, failVals[i]); err != nil {
			t.Fatal(err)
		}
	}
	failVals[count/2].fail = failErr
	fm := fb.Seal()
	if !fm.Release() {
		t.Fatal("fm unexpected extra ref")
	}
	c1, err1 := fm.Finish()
	c1.Complete()
	if !errors.Is(err1, failErr) {
		t.Fatalf("first Finish err = %v, want %v", err1, failErr)
	}
	c2, err2 := fm.Finish()
	c2.Complete()
	if !errors.Is(err2, failErr) {
		t.Fatalf("second Finish err = %v, want %v", err2, failErr)
	}
	if failLease.closed != 0 {
		t.Fatalf("failed map closed lease: %d", failLease.closed)
	}
	if fm.tree.root == nil || fm.tree.root.ownership == nil {
		t.Fatal("failed map lost root ownership custody")
	}
	for i, v := range failVals {
		if v.destroys != 1 {
			t.Fatalf("failVals[%d] destroys = %d, want 1", i, v.destroys)
		}
	}

	// 6. Unsealed composite node cleanup must panic.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("releaseOwnedNode on unsealed composite node did not panic")
			}
		}()
		var tr Map[int, *ownedTestValue]
		tr.init(32)
		tr.ownedNodes = true
		unsealed := tr.newNode(true)
		var comp OwnedMapCompletion
		_ = releaseOwnedNode(unsealed, &comp)
	}()
}

func TestOwnedMapConcurrentReadersAndBuilderFork(t *testing.T) {
	const (
		keyCount   = 256
		writerOps  = 200
		numReaders = 4
	)

	var leasesClosed atomic.Int64
	admit := func(uint64) (OwnedMapLease, error) {
		return atomicOwnedLease{closed: &leasesClosed}, nil
	}

	keys := make([]string, keyCount)
	values := make([]atomic.Int64, keyCount)
	b, err := NewOwnedMapBuilder[string, *atomic.Int64](nil, keyCount, atomicOwnedLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		keys[i] = fmt.Sprintf("item-%04d", i)
		if err := b.Set(keys[i], &values[i]); err != nil {
			t.Fatal(err)
		}
	}
	initial := b.Seal()

	var (
		mu      sync.Mutex
		current = initial
		stop    atomic.Bool
		cleanWg sync.WaitGroup
		readWg  sync.WaitGroup
	)
	finishAsync := func(m *OwnedMap[string, *atomic.Int64]) {
		cleanWg.Add(1)
		go func() {
			defer cleanWg.Done()
			comp, err := m.Finish()
			if err != nil {
				t.Errorf("Finish failed: %v", err)
			}
			comp.Complete()
		}()
	}

	for r := 0; r < numReaders; r++ {
		readWg.Add(1)
		go func(id int) {
			defer readWg.Done()
			iter := 0
			for !stop.Load() {
				mu.Lock()
				snap := current
				snap.Retain()
				mu.Unlock()

				if snap.Len() <= 0 {
					t.Errorf("reader %d observed empty map", id)
				}
				k := keys[(iter+id)%keyCount]
				_, _ = snap.Get(k)
				visited := 0
				snap.Ascend(k, func(string, *atomic.Int64) bool {
					visited++
					return visited < 8
				})
				if iter%16 == 0 {
					seen := 0
					snap.Scan(func(string, *atomic.Int64) bool {
						seen++
						return seen < 16
					})
				}
				if snap.Release() {
					finishAsync(snap)
				}
				iter++
			}
		}(r)
	}

	replacements := make([]atomic.Int64, writerOps)
	for i := 0; i < writerOps; i++ {
		mu.Lock()
		base := current
		base.Retain()
		mu.Unlock()

		wb, err := NewOwnedMapBuilder(base, 1, atomicOwnedLifetime{}, admit)
		if err != nil {
			t.Fatalf("writer iter %d: %v", i, err)
		}
		if err := wb.Set(keys[i%keyCount], &replacements[i]); err != nil {
			t.Fatalf("writer Set %d: %v", i, err)
		}
		next := wb.Seal()

		mu.Lock()
		prev := current
		current = next
		mu.Unlock()

		if base.Release() {
			finishAsync(base)
		}
		if prev.Release() {
			finishAsync(prev)
		}
	}

	stop.Store(true)
	readWg.Wait()

	mu.Lock()
	finalMap := current
	current = nil
	mu.Unlock()
	if finalMap.Release() {
		finishAsync(finalMap)
	}
	cleanWg.Wait()

	for i := range values {
		if got := values[i].Load(); got != 0 {
			t.Fatalf("initial value[%d] leaked refs=%d", i, got)
		}
	}
	for i := range replacements {
		if got := replacements[i].Load(); got != 0 {
			t.Fatalf("replacement[%d] leaked refs=%d", i, got)
		}
	}
	wantLeases := int64(1 + writerOps)
	if got := leasesClosed.Load(); got != wantLeases {
		t.Fatalf("leasesClosed = %d, want %d", got, wantLeases)
	}
}

func BenchmarkOwnedMapSingleKeyUpdateString(b *testing.B) {
	const count = 256
	keys := make([]string, count)
	vals := make([]perfOwnedValue, count)
	builder, err := NewOwnedMapBuilder[string, *perfOwnedValue](nil, count, perfOwnedLifetime{}, perfNoopAdmit)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("ns/collection-%04d", i)
		if err := builder.Set(keys[i], &vals[i]); err != nil {
			b.Fatal(err)
		}
	}
	base := builder.Seal()
	defer func() {
		base.Release()
		comp, err := base.Finish()
		if err != nil {
			b.Fatal(err)
		}
		comp.Complete()
	}()

	var replacement perfOwnedValue
	lifetime := perfOwnedLifetime{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ub, err := NewOwnedMapBuilder(base, 1, lifetime, perfNoopAdmit)
		if err != nil {
			b.Fatal(err)
		}
		if err := ub.Set(keys[i&(count-1)], &replacement); err != nil {
			b.Fatal(err)
		}
		next := ub.Seal()
		if !next.Release() {
			b.Fatal("unexpected extra ref")
		}
		comp, err := next.Finish()
		if err != nil {
			b.Fatal(err)
		}
		comp.Complete()
	}
}

func BenchmarkOwnedMapSingleKeyUpdateUint64(b *testing.B) {
	const count = 256
	vals := make([]perfOwnedValue, count)
	builder, err := NewOwnedMapBuilder[uint64, *perfOwnedValue](nil, count, perfOwnedLifetime{}, perfNoopAdmit)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := builder.Set(uint64(i), &vals[i]); err != nil {
			b.Fatal(err)
		}
	}
	base := builder.Seal()
	defer func() {
		base.Release()
		comp, err := base.Finish()
		if err != nil {
			b.Fatal(err)
		}
		comp.Complete()
	}()

	var replacement perfOwnedValue
	lifetime := perfOwnedLifetime{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ub, err := NewOwnedMapBuilder(base, 1, lifetime, perfNoopAdmit)
		if err != nil {
			b.Fatal(err)
		}
		if err := ub.Set(uint64(i&(count-1)), &replacement); err != nil {
			b.Fatal(err)
		}
		next := ub.Seal()
		if !next.Release() {
			b.Fatal("unexpected extra ref")
		}
		comp, err := next.Finish()
		if err != nil {
			b.Fatal(err)
		}
		comp.Complete()
	}
}
