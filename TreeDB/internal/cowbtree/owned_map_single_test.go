package cowbtree

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

type copyOwnedValue struct {
	id     int
	copies int
}

func (v *copyOwnedValue) Copy() *copyOwnedValue {
	return &copyOwnedValue{id: v.id, copies: v.copies + 1}
}

type copyOwnedLifetime struct{}

func (copyOwnedLifetime) Retain(*copyOwnedValue) {}
func (copyOwnedLifetime) Release(*copyOwnedValue) (OwnedMapCompletion, error) {
	return OwnedMapCompletion{}, nil
}

func collectOwnedMapEntries[K ordered, V any](m *OwnedMap[K, V]) ([]K, []V) {
	var keys []K
	var vals []V
	m.Scan(func(k K, v V) bool {
		keys = append(keys, k)
		vals = append(vals, v)
		return true
	})
	return keys, vals
}

func finishGenericOwnedMap[K ordered, V any](t *testing.T, m *OwnedMap[K, V]) {
	t.Helper()
	if !m.Release() {
		t.Fatal("unexpected extra map reference")
	}
	comp, err := m.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	comp.Complete()
}

func assertOwnedMapAllocationIsolation[K ordered, V any](t *testing.T, m *OwnedMap[K, V]) {
	t.Helper()
	if m == nil || m.allocation == nil || m.tree.root == nil || m.tree.root.ownership == nil {
		t.Fatal("expected populated owned map, allocation, root, and ownership")
	}
	allocAddr := uintptr(unsafe.Pointer(m.allocation))
	mapStart := uintptr(unsafe.Pointer(m))
	mapEnd := mapStart + unsafe.Sizeof(*m)
	if allocAddr >= mapStart && allocAddr < mapEnd {
		t.Fatalf("ownedMapAllocation (%#x) overlaps OwnedMap [%#x, %#x)", allocAddr, mapStart, mapEnd)
	}
	nodeStart := uintptr(unsafe.Pointer(m.tree.root))
	nodeEnd := nodeStart + unsafe.Sizeof(mapOwnedLeafNode[K, V]{})
	if allocAddr >= nodeStart && allocAddr < nodeEnd {
		t.Fatalf("ownedMapAllocation (%#x) overlaps mapOwnedLeafNode [%#x, %#x)", allocAddr, nodeStart, nodeEnd)
	}
	allocType := reflect.TypeOf(ownedMapAllocation{})
	for i := 0; i < allocType.NumField(); i++ {
		ft := allocType.Field(i).Type
		if ft.Kind() == reflect.Pointer {
			t.Fatalf("ownedMapAllocation field %q must not hold pointer backreference: %v", allocType.Field(i).Name, ft)
		}
	}
}

func TestOwnedMapSetSingleParityWithBuilder(t *testing.T) {
	type scenario struct {
		name         string
		baseKeys     []int
		setKey       int
		expectFast   bool
		expectHeight int
	}

	fullLeafKeys := make([]int, 63)
	for i := range fullLeafKeys {
		fullLeafKeys[i] = i * 2
	}
	multiLevelKeys := make([]int, 256)
	for i := range multiLevelKeys {
		multiLevelKeys[i] = i * 2
	}

	scenarios := []scenario{
		{name: "NilBase", baseKeys: nil, setKey: 10, expectFast: false, expectHeight: 1},
		{name: "EmptyBase", baseKeys: []int{}, setKey: 10, expectFast: false, expectHeight: 1},
		{name: "SingleLeafReplaceOnlyItem", baseKeys: []int{10}, setKey: 10, expectFast: true, expectHeight: 1},
		{name: "SingleLeafInsertFirst", baseKeys: []int{10, 20, 30}, setKey: 5, expectFast: true, expectHeight: 1},
		{name: "SingleLeafInsertMiddle", baseKeys: []int{10, 20, 30}, setKey: 15, expectFast: true, expectHeight: 1},
		{name: "SingleLeafInsertLast", baseKeys: []int{10, 20, 30}, setKey: 35, expectFast: true, expectHeight: 1},
		{name: "SingleLeafReplaceFirst", baseKeys: []int{10, 20, 30}, setKey: 10, expectFast: true, expectHeight: 1},
		{name: "SingleLeafReplaceMiddle", baseKeys: []int{10, 20, 30}, setKey: 20, expectFast: true, expectHeight: 1},
		{name: "SingleLeafReplaceLast", baseKeys: []int{10, 20, 30}, setKey: 30, expectFast: true, expectHeight: 1},
		{name: "FullLeafReplaceFastPath", baseKeys: fullLeafKeys, setKey: 60, expectFast: true, expectHeight: 1},
		{name: "FullLeafInsertSplits", baseKeys: fullLeafKeys, setKey: 61, expectFast: false, expectHeight: 2},
		{name: "MultiLevelReplace", baseKeys: multiLevelKeys, setKey: 120, expectFast: false, expectHeight: 2},
		{name: "MultiLevelInsert", baseKeys: multiLevelKeys, setKey: 121, expectFast: false, expectHeight: 2},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			buildBase := func() (*OwnedMap[int, *ownedTestValue], []*ownedTestValue, *ownedTestLease) {
				if sc.baseKeys == nil {
					return nil, nil, nil
				}
				b, lease := ownedTestBuilder(t, nil, len(sc.baseKeys))
				vals := make([]*ownedTestValue, len(sc.baseKeys))
				for i, k := range sc.baseKeys {
					vals[i] = new(ownedTestValue)
					if err := b.Set(k, vals[i]); err != nil {
						t.Fatal(err)
					}
				}
				return b.Seal(), vals, lease
			}

			baseA, valsA, baseLeaseA := buildBase()
			baseB, valsB, baseLeaseB := buildBase()
			newValA := new(ownedTestValue)
			newValB := new(ownedTestValue)

			singleLease := new(ownedTestLease)
			gotSingle, err := baseA.SetSingle(sc.setKey, newValA, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
				singleLease.charge = bytes
				return singleLease, nil
			})
			if err != nil {
				t.Fatalf("SetSingle failed: %v", err)
			}

			builderLease := new(ownedTestLease)
			ub, err := NewOwnedMapBuilder(baseB, 1, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
				builderLease.charge = bytes
				return builderLease, nil
			})
			if err != nil {
				t.Fatalf("NewOwnedMapBuilder failed: %v", err)
			}
			if err := ub.Set(sc.setKey, newValB); err != nil {
				t.Fatalf("builder Set failed: %v", err)
			}
			gotBuilder := ub.Seal()

			if gotSingle.Len() != gotBuilder.Len() {
				t.Fatalf("Len mismatch: SetSingle=%d Builder=%d", gotSingle.Len(), gotBuilder.Len())
			}
			if gotSingle.tree.Height() != sc.expectHeight || gotBuilder.tree.Height() != sc.expectHeight {
				t.Fatalf("Height mismatch: SetSingle=%d Builder=%d want=%d", gotSingle.tree.Height(), gotBuilder.tree.Height(), sc.expectHeight)
			}
			if sc.expectFast {
				if singleLease.charge >= builderLease.charge {
					t.Fatalf("expected fast-path single-leaf charge (%d) < builder charge (%d)", singleLease.charge, builderLease.charge)
				}
				assertOwnedMapAllocationIsolation(t, gotSingle)
				if gotSingle.allocation.refs.Load() != 2 {
					t.Fatalf("fast-path alloc refs = %d, want 2", gotSingle.allocation.refs.Load())
				}
				if gotSingle.tree.root.ownership.refs.Load() != 1 {
					t.Fatalf("fast-path root ownership refs = %d, want 1", gotSingle.tree.root.ownership.refs.Load())
				}
			} else if singleLease.charge != builderLease.charge {
				t.Fatalf("expected fallback charge (%d) == builder charge (%d)", singleLease.charge, builderLease.charge)
			}

			keysS, valsS := collectOwnedMapEntries(gotSingle)
			keysB, valsBOut := collectOwnedMapEntries(gotBuilder)
			if !reflect.DeepEqual(keysS, keysB) {
				t.Fatalf("keys mismatch:\nSetSingle=%v\nBuilder=  %v", keysS, keysB)
			}
			if len(valsS) != len(valsBOut) {
				t.Fatalf("value len mismatch: %d vs %d", len(valsS), len(valsBOut))
			}
			if v, ok := gotSingle.Get(sc.setKey); !ok || v != newValA {
				t.Fatalf("SetSingle Get(%d) = (%v, %t), want (%v, true)", sc.setKey, v, ok, newValA)
			}
			if !gotSingle.tree.sealed || !gotSingle.tree.ownedNodes {
				t.Fatalf("unexpected tree flags: sealed=%t ownedNodes=%t", gotSingle.tree.sealed, gotSingle.tree.ownedNodes)
			}
			if sc.expectFast && gotSingle.tree.allocationCharge != 0 {
				t.Fatalf("fast-path allocationCharge = %d, want 0", gotSingle.tree.allocationCharge)
			}
			if !sc.expectFast && gotSingle.tree.allocationCharge != gotBuilder.tree.allocationCharge {
				t.Fatalf("fallback allocationCharge = %d, want builder %d", gotSingle.tree.allocationCharge, gotBuilder.tree.allocationCharge)
			}
			if baseA != nil {
				if baseA.Len() != len(sc.baseKeys) {
					t.Fatalf("baseA mutated: len=%d want=%d", baseA.Len(), len(sc.baseKeys))
				}
				if gotSingle.tree.isoid == baseA.tree.isoid {
					t.Fatal("SetSingle reused base isoid")
				}
				if gotSingle.tree.root == baseA.tree.root {
					t.Fatal("SetSingle aliased base root node")
				}
			}

			// Verify subsequent fork from SetSingle result works cleanly (both 0-mutation sharing and 1-mutation fork).
			forkB, forkLease := ownedTestBuilder(t, gotSingle, 0)
			forked := forkB.Seal()
			if forked.Len() != gotSingle.Len() {
				t.Fatalf("zero-mutation fork len mismatch: %d vs %d", forked.Len(), gotSingle.Len())
			}

			finishOwnedTest(t, &gotSingle)
			// Forked still keeps gotSingle's root and allocation alive.
			if singleLease.closed != 0 {
				t.Fatal("singleLease closed while zero-mutation fork still held shared root")
			}
			finishOwnedTest(t, &forked)
			if singleLease.closed != 1 || forkLease.closed != 1 {
				t.Fatalf("leases closed = (single:%d, fork:%d), want (1, 1)", singleLease.closed, forkLease.closed)
			}
			if baseA != nil {
				finishOwnedTest(t, &baseA)
				if baseLeaseA.closed != 1 {
					t.Fatalf("baseLeaseA closed = %d, want 1", baseLeaseA.closed)
				}
			}
			for i, v := range valsA {
				if v.refs != 0 || v.destroys != 1 {
					t.Fatalf("valsA[%d] refs=%d destroys=%d", i, v.refs, v.destroys)
				}
			}
			if newValA.refs != 0 || newValA.destroys != 1 {
				t.Fatalf("newValA refs=%d destroys=%d", newValA.refs, newValA.destroys)
			}

			finishOwnedTest(t, &gotBuilder)
			if baseB != nil {
				finishOwnedTest(t, &baseB)
				if baseLeaseB.closed != 1 {
					t.Fatalf("baseLeaseB closed = %d, want 1", baseLeaseB.closed)
				}
			}
			if builderLease.closed != 1 {
				t.Fatalf("builderLease closed = %d, want 1", builderLease.closed)
			}
			for i, v := range valsB {
				if v.refs != 0 || v.destroys != 1 {
					t.Fatalf("valsB[%d] refs=%d destroys=%d", i, v.refs, v.destroys)
				}
			}
			if newValB.refs != 0 || newValB.destroys != 1 {
				t.Fatalf("newValB refs=%d destroys=%d", newValB.refs, newValB.destroys)
			}
		})
	}
}

func TestOwnedMapSetSingleErrorsAdmitRefusalAndPartialReleaseFailure(t *testing.T) {
	// 1. Invalid arguments and invalid receiver states.
	b, baseLease := ownedTestBuilder(t, nil, 3)
	vals := []*ownedTestValue{new(ownedTestValue), new(ownedTestValue), new(ownedTestValue)}
	for i, v := range vals {
		if err := b.Set(i*10, v); err != nil {
			t.Fatal(err)
		}
	}
	base := b.Seal()

	if _, err := base.SetSingle(0, new(ownedTestValue), nil, perfNoopAdmit); !errors.Is(err, ErrOwnedMap) {
		t.Fatalf("nil lifetime err=%v, want ErrOwnedMap", err)
	}
	if _, err := base.SetSingle(0, new(ownedTestValue), ownedTestLifetime{}, nil); !errors.Is(err, ErrOwnedMap) {
		t.Fatalf("nil admit err=%v, want ErrOwnedMap", err)
	}
	var nilMap *OwnedMap[int, *ownedTestValue]
	if _, err := nilMap.SetSingle(0, new(ownedTestValue), nil, perfNoopAdmit); !errors.Is(err, ErrOwnedMap) {
		t.Fatalf("nil map + nil lifetime err=%v, want ErrOwnedMap", err)
	}

	// Unsealed receiver must fail with ErrOwnedMap before touching tree or admit.
	unsealed := &OwnedMap[int, *ownedTestValue]{}
	unsealed.refs.Store(1)
	admitCalled := false
	if _, err := unsealed.SetSingle(1, new(ownedTestValue), ownedTestLifetime{}, func(uint64) (OwnedMapLease, error) {
		admitCalled = true
		return perfNoopLease{}, nil
	}); !errors.Is(err, ErrOwnedMap) || admitCalled {
		t.Fatalf("unsealed receiver err=%v admitCalled=%t, want ErrOwnedMap and false", err, admitCalled)
	}

	// Released receiver (refs <= 0) must fail with ErrOwnedMap before touching tree or admit.
	released := &OwnedMap[int, *ownedTestValue]{}
	released.tree.sealed = true
	released.refs.Store(0)
	if _, err := released.SetSingle(1, new(ownedTestValue), ownedTestLifetime{}, func(uint64) (OwnedMapLease, error) {
		admitCalled = true
		return perfNoopLease{}, nil
	}); !errors.Is(err, ErrOwnedMap) || admitCalled {
		t.Fatalf("refs==0 receiver err=%v admitCalled=%t, want ErrOwnedMap and false", err, admitCalled)
	}

	// 2. Admit refusal on fast path must leak zero refs and zero leases.
	denied := errors.New("admit denied")
	candidate := new(ownedTestValue)
	if out, err := base.SetSingle(10, candidate, ownedTestLifetime{}, func(uint64) (OwnedMapLease, error) {
		return nil, denied
	}); !errors.Is(err, denied) || out != nil {
		t.Fatalf("admit refusal returned (%v, %v), want (nil, %v)", out, err, denied)
	}
	if candidate.refs != 0 || candidate.destroys != 0 {
		t.Fatalf("candidate modified on admit refusal: refs=%d destroys=%d", candidate.refs, candidate.destroys)
	}
	for i, v := range vals {
		if v.refs != 1 || v.destroys != 0 {
			t.Fatalf("base value %d modified on admit refusal: refs=%d destroys=%d", i, v.refs, v.destroys)
		}
	}

	// 3. Admit refusal on fallback path (nil base) must also leak zero refs.
	if out, err := nilMap.SetSingle(10, candidate, ownedTestLifetime{}, func(uint64) (OwnedMapLease, error) {
		return nil, denied
	}); !errors.Is(err, denied) || out != nil {
		t.Fatalf("nil-base admit refusal returned (%v, %v), want (nil, %v)", out, err, denied)
	}
	if candidate.refs != 0 || candidate.destroys != 0 {
		t.Fatalf("candidate modified on nil-base admit refusal: refs=%d destroys=%d", candidate.refs, candidate.destroys)
	}

	// 4. Partial Release failure on fast-path OwnedMap parks exact failed value,
	// releases independent siblings once, keeps lease open, and replays error idempotently.
	failErr := errors.New("single-leaf release failure")
	failingVal := &ownedTestValue{fail: failErr}
	singleLease := new(ownedTestLease)
	next, err := base.SetSingle(10, failingVal, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
		singleLease.charge = bytes
		return singleLease, nil
	})
	if err != nil {
		t.Fatalf("SetSingle failed: %v", err)
	}
	// Finish base first so next is the sole owner of vals[0], vals[2], and failingVal.
	finishOwnedTest(t, &base)
	if baseLease.closed != 1 {
		t.Fatalf("baseLease.closed = %d, want 1", baseLease.closed)
	}
	if vals[1].refs != 0 || vals[1].destroys != 1 {
		t.Fatalf("replaced vals[1] not destroyed on base finish: refs=%d destroys=%d", vals[1].refs, vals[1].destroys)
	}
	if vals[0].refs != 1 || vals[2].refs != 1 || failingVal.refs != 1 {
		t.Fatalf("unexpected live refs before next finish: v0=%d v2=%d fail=%d", vals[0].refs, vals[2].refs, failingVal.refs)
	}

	if !next.Release() {
		t.Fatal("expected last ref on next")
	}
	c1, err1 := next.Finish()
	c1.Complete()
	if !errors.Is(err1, failErr) {
		t.Fatalf("first Finish err=%v, want %v", err1, failErr)
	}
	c2, err2 := next.Finish()
	c2.Complete()
	if !errors.Is(err2, failErr) {
		t.Fatalf("second Finish err=%v, want %v", err2, failErr)
	}
	if singleLease.closed != 0 {
		t.Fatalf("singleLease closed on partial release failure: %d", singleLease.closed)
	}
	if vals[0].refs != 0 || vals[0].destroys != 1 || vals[2].refs != 0 || vals[2].destroys != 1 {
		t.Fatalf("siblings not released exactly once: v0=(%d,%d) v2=(%d,%d)", vals[0].refs, vals[0].destroys, vals[2].refs, vals[2].destroys)
	}
	if failingVal.destroys != 1 || next.tree.root == nil || next.tree.root.ownership == nil || next.tree.root.items[1].value != failingVal {
		t.Fatal("failed value not parked cleanly on fast-path node")
	}
	if next.tree.root.items[0].value != nil || next.tree.root.items[2].value != nil {
		t.Fatal("released sibling slots were not detached")
	}
}

func TestOwnedMapSetSingleChargeDominatedByBuilderAndBudgetBounds(t *testing.T) {
	if _, err := ownedMapSingleLeafCharge[string, *ownedTestValue](-1); !errors.Is(err, ErrOwnedMap) {
		t.Fatalf("negative newLen err=%v, want ErrOwnedMap", err)
	}
	if _, err := ownedMapSingleLeafCharge[string, *ownedTestValue](math.MaxInt); !errors.Is(err, ErrOwnedMap) {
		t.Fatalf("overflow newLen err=%v, want ErrOwnedMap", err)
	}
	for newLen := 0; newLen <= 63; newLen++ {
		strSingle, err := ownedMapSingleLeafCharge[string, *ownedTestValue](newLen)
		if err != nil {
			t.Fatalf("string single charge(%d): %v", newLen, err)
		}
		strBuilder, err := ownedMapBuilderCharge[string, *ownedTestValue](newLen, 63, 1)
		if err != nil {
			t.Fatalf("string builder charge(%d): %v", newLen, err)
		}
		strBudget, err := OwnedMapBuilderBudgetCharge[string, *ownedTestValue](0, 1)
		if err != nil {
			t.Fatalf("string budget charge: %v", err)
		}
		if strSingle >= strBuilder || strSingle > strBudget {
			t.Fatalf("string newLen=%d: single=%d builder=%d budget=%d", newLen, strSingle, strBuilder, strBudget)
		}

		u64Single, err := ownedMapSingleLeafCharge[uint64, *ownedTestValue](newLen)
		if err != nil {
			t.Fatalf("uint64 single charge(%d): %v", newLen, err)
		}
		u64Builder, err := ownedMapBuilderCharge[uint64, *ownedTestValue](newLen, 63, 1)
		if err != nil {
			t.Fatalf("uint64 builder charge(%d): %v", newLen, err)
		}
		u64Budget, err := OwnedMapBuilderBudgetCharge[uint64, *ownedTestValue](0, 1)
		if err != nil {
			t.Fatalf("uint64 budget charge: %v", err)
		}
		if u64Single >= u64Builder || u64Single > u64Budget {
			t.Fatalf("uint64 newLen=%d: single=%d builder=%d budget=%d", newLen, u64Single, u64Builder, u64Budget)
		}
	}
}

func TestOwnedMapSetSingleCopyValuesFallsBack(t *testing.T) {
	b, err := NewOwnedMapBuilder[int, *copyOwnedValue](nil, 2, copyOwnedLifetime{}, perfNoopAdmit)
	if err != nil {
		t.Fatal(err)
	}
	v1 := &copyOwnedValue{id: 1}
	v2 := &copyOwnedValue{id: 2}
	if err := b.Set(1, v1); err != nil {
		t.Fatal(err)
	}
	if err := b.Set(2, v2); err != nil {
		t.Fatal(err)
	}
	base := b.Seal()
	if !base.tree.copyValues {
		t.Fatal("expected copyValues == true for *copyOwnedValue")
	}

	v3 := &copyOwnedValue{id: 3}
	next, err := base.SetSingle(2, v3, copyOwnedLifetime{}, perfNoopAdmit)
	if err != nil {
		t.Fatalf("SetSingle with copyValues failed: %v", err)
	}
	got1, ok := next.Get(1)
	if !ok || got1 == v1 || got1.id != 1 || got1.copies != 1 {
		t.Fatalf("expected Copy() on untouched item via fallback, got %+v (samePtr=%t)", got1, got1 == v1)
	}
	got2, ok := next.Get(2)
	if !ok || got2 != v3 {
		t.Fatalf("expected replaced item v3, got %+v", got2)
	}
	finishGenericOwnedMap(t, next)
	finishGenericOwnedMap(t, base)
}

func TestOwnedMapSetSingleConcurrentReadersAndWriter(t *testing.T) {
	const (
		keyCount   = 8
		writerOps  = 250
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
		keys[i] = fmt.Sprintf("domain-%02d", i)
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

				if snap.Len() != keyCount {
					t.Errorf("reader %d observed len %d, want %d", id, snap.Len(), keyCount)
				}
				if _, ok := snap.Get(keys[(iter+id)%keyCount]); !ok {
					t.Errorf("reader %d missed key", id)
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

		next, err := base.SetSingle(keys[i%keyCount], &replacements[i], atomicOwnedLifetime{}, admit)
		if err != nil {
			t.Fatalf("SetSingle iter %d: %v", i, err)
		}

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
	if want := int64(1 + writerOps); leasesClosed.Load() != want {
		t.Fatalf("leasesClosed = %d, want %d", leasesClosed.Load(), want)
	}
}

func BenchmarkOwnedMapSetSingleString(b *testing.B) {
	for _, count := range []int{1, 3} {
		keys := make([]string, count)
		vals := make([]perfOwnedValue, count)
		builder, err := NewOwnedMapBuilder[string, *perfOwnedValue](nil, count, perfOwnedLifetime{}, perfNoopAdmit)
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < count; i++ {
			keys[i] = fmt.Sprintf("domain-%02d", i)
			if err := builder.Set(keys[i], &vals[i]); err != nil {
				b.Fatal(err)
			}
		}
		base := builder.Seal()
		b.Cleanup(func() {
			base.Release()
			comp, err := base.Finish()
			if err != nil {
				b.Fatal(err)
			}
			comp.Complete()
		})

		var replacement perfOwnedValue
		lifetime := perfOwnedLifetime{}
		targetKey := keys[0]

		b.Run(fmt.Sprintf("Len%d/Builder", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ub, err := NewOwnedMapBuilder(base, 1, lifetime, perfNoopAdmit)
				if err != nil {
					b.Fatal(err)
				}
				if err := ub.Set(targetKey, &replacement); err != nil {
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
		})

		b.Run(fmt.Sprintf("Len%d/SetSingle", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				next, err := base.SetSingle(targetKey, &replacement, lifetime, perfNoopAdmit)
				if err != nil {
					b.Fatal(err)
				}
				if !next.Release() {
					b.Fatal("unexpected extra ref")
				}
				comp, err := next.Finish()
				if err != nil {
					b.Fatal(err)
				}
				comp.Complete()
			}
		})
	}
}

func BenchmarkOwnedMapSetSingleUint64(b *testing.B) {
	for _, count := range []int{1, 3} {
		vals := make([]perfOwnedValue, count)
		builder, err := NewOwnedMapBuilder[uint64, *perfOwnedValue](nil, count, perfOwnedLifetime{}, perfNoopAdmit)
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < count; i++ {
			if err := builder.Set(uint64(i+1), &vals[i]); err != nil {
				b.Fatal(err)
			}
		}
		base := builder.Seal()
		b.Cleanup(func() {
			base.Release()
			comp, err := base.Finish()
			if err != nil {
				b.Fatal(err)
			}
			comp.Complete()
		})

		var replacement perfOwnedValue
		lifetime := perfOwnedLifetime{}

		b.Run(fmt.Sprintf("Len%d/Builder", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ub, err := NewOwnedMapBuilder(base, 1, lifetime, perfNoopAdmit)
				if err != nil {
					b.Fatal(err)
				}
				if err := ub.Set(uint64(1), &replacement); err != nil {
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
		})

		b.Run(fmt.Sprintf("Len%d/SetSingle", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				next, err := base.SetSingle(uint64(1), &replacement, lifetime, perfNoopAdmit)
				if err != nil {
					b.Fatal(err)
				}
				if !next.Release() {
					b.Fatal("unexpected extra ref")
				}
				comp, err := next.Finish()
				if err != nil {
					b.Fatal(err)
				}
				comp.Complete()
			}
		})
	}
}
