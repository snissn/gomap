package cowbtree

import (
	"errors"
	"runtime"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOwnedMapBuilderTinyLeafAdmission(t *testing.T) {
	const ceiling = 1024
	lifetime := ownedTestLifetime{}
	original, replacement := new(ownedTestValue), new(ownedTestValue)
	openingLease := new(ownedTestLease)
	builder, err := NewOwnedMapBuilder[string, *ownedTestValue](nil, 1, lifetime, func(uint64) (OwnedMapLease, error) {
		return openingLease, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Set("primary", original); err != nil {
		t.Fatal(err)
	}
	opening := builder.Seal()
	lease := new(ownedTestLease)
	refused := errors.New("test admission ceiling exceeded")
	admissions := 0
	builder, err = NewOwnedMapBuilder(opening, 1, lifetime, func(bytes uint64) (OwnedMapLease, error) {
		admissions++
		lease.charge = bytes
		if bytes > ceiling {
			return nil, refused
		}
		return lease, nil
	})
	if err != nil {
		t.Fatalf("eligible singleton replacement rejected: requested=%d ceiling=%d admissions=%d: %v", lease.charge, ceiling, admissions, err)
	}
	if err := builder.Set("primary", replacement); err != nil {
		t.Fatal(err)
	}
	next := builder.Seal()
	if got, _ := opening.Get("primary"); got != original {
		t.Fatal("opening value changed")
	}
	if got, _ := next.Get("primary"); got != replacement || admissions != 1 {
		t.Fatalf("replacement=%p want=%p admissions=%d", got, replacement, admissions)
	}
	next.Retain()
	if next.Release() || lease.closed != 0 {
		t.Fatal("held map refunded on an intermediate release")
	}
	finishTinyLeafMap(t, &opening)
	finishTinyLeafMap(t, &next)
	if openingLease.closed != 1 || lease.closed != 1 || original.refs != 0 || replacement.refs != 0 ||
		original.destroys != 1 || replacement.destroys != 1 {
		t.Fatal("original leases and values must finish exactly once")
	}
	t.Logf("eligible replacement reserved %d B in the original admit callback", lease.charge)
}

func TestOwnedMapBuilderTinyLeafMutations(t *testing.T) {
	const nodeBytes = uint64(unsafe.Sizeof(mapNode[string, *ownedTestValue]{}))
	const pairBytes = uint64(unsafe.Sizeof(mapPair[string, *ownedTestValue]{}))
	// The private mutation receipt records node/backing allocations. Add the
	// separately allocated builder, headers and ownership metadata to it.
	metadata := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(OwnedMapBuilder[string, *ownedTestValue]{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(OwnedMap[string, *ownedTestValue]{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Map[string, *ownedTestValue]{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ownedMapAllocation{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ownedMapNode[string, *ownedTestValue]{})))
	bound := metadata + retainedalloc.AllocationCharge(nodeBytes) +
		retainedalloc.AllocationCharge(pairBytes) + retainedalloc.AllocationCharge(2*pairBytes)
	coarse, err := ownedMapBuilderCharge[string, *ownedTestValue](2, 63, 1)
	if err != nil || bound >= coarse {
		t.Fatalf("tiny bound=%d coarse=%d err=%v", bound, coarse, err)
	}
	maximum, err := OwnedMapBuilderMaximumCharge[string, *ownedTestValue](1)
	if err != nil || bound > maximum {
		t.Fatalf("maximum bound=%d tiny=%d err=%v", maximum, bound, err)
	}
	budgetBound, err := OwnedMapBuilderBudgetCharge[string, *ownedTestValue](bound, 1)
	if err != nil || bound > budgetBound {
		t.Fatalf("budget bound=%d tiny=%d err=%v", budgetBound, bound, err)
	}
	if runtime.GOARCH == "amd64" {
		// The native allocator rounds the cohort sum again. Its separately
		// charged external lease is outside the builder's admit argument.
		if outer := retainedalloc.AllocationCharge(bound); bound != 544 || outer != 1024 {
			t.Fatalf("amd64 fixture structural bound=%d outer cohort=%d", bound, outer)
		}
	}

	for _, tc := range []struct {
		name         string
		openingItems int
		key          string
		deleting     bool
		wantKeys     int
		backingSlots []uint64
	}{
		{"create", -1, "primary", false, 1, []uint64{1}},
		{"empty", 0, "primary", false, 1, []uint64{1}},
		{"replace", 1, "primary", false, 1, []uint64{1}},
		{"grow", 1, "secondary", false, 2, []uint64{1, 2}},
		{"delete", 1, "primary", true, 0, []uint64{1}},
		{"delete miss", 1, "secondary", true, 1, []uint64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original, incoming := new(ownedTestValue), new(ownedTestValue)
			opening, openingLease := tinyLeafTestOpening(t, tc.openingItems, 32, original)
			lease := new(ownedTestLease)
			calls := 0
			builder, err := NewOwnedMapBuilder(opening, 1, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
				calls++
				lease.charge = bytes
				if bytes > bound {
					return nil, errors.New("tiny structural ceiling exceeded")
				}
				return lease, nil
			})
			if err != nil || calls != 1 || lease.charge != bound {
				t.Fatalf("charge=%d want=%d admissions=%d err=%v", lease.charge, bound, calls, err)
			}
			before := builder.tree.AllocationCharge()
			if tc.deleting {
				err = builder.Delete(tc.key)
			} else {
				err = builder.Set(tc.key, incoming)
			}
			if err != nil || builder.Delete(tc.key) != ErrOwnedMap {
				t.Fatalf("one-mutation bound lost: first err=%v", err)
			}
			want := retainedalloc.AllocationCharge(nodeBytes)
			for _, slots := range tc.backingSlots {
				want += retainedalloc.AllocationCharge(slots * pairBytes)
			}
			if actual := builder.tree.AllocationCharge() - before; actual != want || actual+metadata > lease.charge {
				t.Fatalf("actual structural sites=%d want=%d metadata=%d reserve=%d", actual, want, metadata, lease.charge)
			}
			next := builder.Seal()
			if next.Len() != tc.wantKeys {
				t.Fatalf("result count=%d want=%d", next.Len(), tc.wantKeys)
			}
			if got, ok := next.Get(tc.key); (!tc.deleting && (!ok || got != incoming)) || (tc.deleting && ok) {
				t.Fatalf("result key=%q value=%p present=%v", tc.key, got, ok)
			}
			if opening != nil {
				if got, ok := opening.Get("primary"); opening.Len() != tc.openingItems || (tc.openingItems == 1 && (!ok || got != original)) {
					t.Fatal("opening map changed")
				}
				finishTinyLeafMap(t, &opening)
				if openingLease.closed != 1 {
					t.Fatal("opening lease did not refund")
				}
			}
			finishTinyLeafMap(t, &next)
			wantOriginal, wantIncoming := 0, 0
			if tc.openingItems == 1 {
				wantOriginal = 1
			}
			if !tc.deleting {
				wantIncoming = 1
			}
			if lease.closed != 1 || original.refs != 0 || incoming.refs != 0 ||
				original.destroys != wantOriginal || incoming.destroys != wantIncoming {
				t.Fatal("result and opening resources did not finish exactly once")
			}
		})
	}
}

func TestOwnedMapBuilderTinyLeafRefusalAndCancellation(t *testing.T) {
	original := new(ownedTestValue)
	opening, _ := tinyLeafTestOpening(t, 1, 32, original)
	denied := errors.New("admission denied")
	calls := 0
	var exact uint64
	builder, err := NewOwnedMapBuilder(opening, 1, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
		calls++
		exact = bytes
		return nil, denied
	})
	if builder != nil || err != denied || calls != 1 || exact == 0 {
		t.Fatalf("failed to capture bound: requested=%d calls=%d err=%v", exact, calls, err)
	}
	builder, err = NewOwnedMapBuilder(opening, 1, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
		calls++
		if bytes <= exact-1 {
			t.Fatalf("admitted below exact=%d with charge=%d", exact, bytes)
		}
		return nil, denied
	})
	if builder != nil || err != denied || calls != 2 || original.refs != 1 {
		t.Fatalf("refused preparation changed opening value custody: calls=%d refs=%d err=%v", calls, original.refs, err)
	}
	lease := new(ownedTestLease)
	builder, err = NewOwnedMapBuilder(opening, 1, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
		calls++
		if bytes != exact {
			t.Fatalf("changed bound: requested=%d exact=%d", bytes, exact)
		}
		return lease, nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("exact bound rejected: admissions=%d err=%v", calls, err)
	}
	incoming := new(ownedTestValue)
	if err := builder.Set("primary", incoming); err != nil {
		t.Fatal(err)
	}
	completion := builder.Cancel()
	if lease.closed != 0 || builder.tree != nil || incoming.refs != 0 || original.refs != 1 {
		t.Fatal("Cancel did not detach and defer refund of private storage")
	}
	builder = nil
	completion.Complete()
	completion.Complete()
	if lease.closed != 1 {
		t.Fatal("private canceled receipt did not refund exactly once")
	}
	finishTinyLeafMap(t, &opening)
}

func TestOwnedMapBuilderTinyLeafFallback(t *testing.T) {
	for _, tc := range []struct {
		name                string
		items, degree, work int
	}{
		{"no mutations", 1, 32, 0},
		{"two mutations", 1, 32, 2},
		{"two items", 2, 32, 1},
		{"internal root", 66, 32, 1},
		{"different degree", 1, 4, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opening, _ := tinyLeafTestOpening(t, tc.items, tc.degree, new(ownedTestValue))
			want, err := ownedMapBuilderCharge[string, *ownedTestValue](tc.items+tc.work, 63, tc.work)
			if err != nil {
				t.Fatal(err)
			}
			lease := new(ownedTestLease)
			builder, err := NewOwnedMapBuilder(opening, tc.work, ownedTestLifetime{}, func(bytes uint64) (OwnedMapLease, error) {
				lease.charge = bytes
				return lease, nil
			})
			if err != nil || lease.charge != want {
				t.Fatalf("fallback charge=%d want=%d err=%v", lease.charge, want, err)
			}
			completion := builder.Cancel()
			builder = nil
			completion.Complete()
			finishTinyLeafMap(t, &opening)
		})
	}
	t.Run("overflow rejected before admit", func(t *testing.T) {
		opening, _ := tinyLeafTestOpening(t, 1, 32, new(ownedTestValue))
		never := func(uint64) (OwnedMapLease, error) {
			t.Fatal("overflow reached admit")
			return nil, nil
		}
		maxInt := int(^uint(0) >> 1)
		bases := []*OwnedMap[string, *ownedTestValue]{opening}
		if unsafe.Sizeof(uintptr(0)) == 8 {
			bases = append(bases, nil)
		}
		for _, base := range bases {
			if builder, err := NewOwnedMapBuilder(base, maxInt, ownedTestLifetime{}, never); builder != nil || err != ErrOwnedMap {
				t.Fatalf("overflow: got builder=%p err=%v", builder, err)
			}
		}
		finishTinyLeafMap(t, &opening)
	})
}

func tinyLeafTestOpening(t *testing.T, items, degree int, primary *ownedTestValue) (*OwnedMap[string, *ownedTestValue], *ownedTestLease) {
	t.Helper()
	if items < 0 {
		return nil, nil
	}
	lease := new(ownedTestLease)
	builder, err := NewOwnedMapBuilder[string, *ownedTestValue](nil, items, ownedTestLifetime{}, func(uint64) (OwnedMapLease, error) {
		return lease, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if degree != 32 {
		builder.tree = NewMap[string, *ownedTestValue](degree)
	}
	for i := range items {
		key, value := "primary", primary
		if i != 0 {
			key, value = string(rune('A'+i)), new(ownedTestValue)
		}
		if err := builder.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	return builder.Seal(), lease
}

func finishTinyLeafMap[V any](t *testing.T, slot **OwnedMap[string, V]) {
	t.Helper()
	if !(*slot).Release() {
		t.Fatal("unexpected extra map reference")
	}
	completion, err := (*slot).Finish()
	*slot = nil
	completion.Complete()
	if err != nil {
		t.Fatal(err)
	}
}
