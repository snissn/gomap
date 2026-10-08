package residentcredit

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func privateSnapshotTestLayoutV1() PrivateSnapshotLayoutV1 {
	return PrivateSnapshotLayoutV1{Holder: 672, Point: 8}
}
func TestPrivateSnapshotBackingSerialChurnAndLateOriginalClose(t *testing.T) {
	owner, err := NewOrdinary(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := owner.NewOrdinaryScope()
	if err != nil {
		t.Fatal(err)
	}
	base := owner.Stats()
	for i := 0; i < 1024; i++ {
		b, err := scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1())
		if err != nil {
			t.Fatal(err)
		}
		if owner.Stats().Live <= base.Live {
			t.Fatal("private backing was not charged before birth")
		}
		b.ReleasePrivateSnapshotBackingV1()
		b.ReleasePrivateSnapshotBackingV1()
		if got := owner.Stats(); got.Live != base.Live || got.Births <= base.Births {
			t.Fatalf("live/birth census after churn: %+v", got)
		}
	}
	b, err := scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1())
	if err != nil {
		t.Fatal(err)
	}
	owner.Close()
	scope.ReleaseStableMetadata()
	if owner.Stats().Live == 0 {
		t.Fatal("original owner died before private final edge")
	}
	b.ReleasePrivateSnapshotBackingV1()
	if got := owner.Stats(); got.Live != 0 || got.Refs != 0 {
		t.Fatalf("late last edge: %+v", got)
	}
	if _, err = scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1()); !errors.Is(err, ErrLimit) {
		t.Fatalf("dead original recreated backing: %v", err)
	}
}
func TestPrivateSnapshotBackingConcurrentAliasesAndStrictOverlap(t *testing.T) {
	owner, err := NewOrdinary(4096)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := owner.NewOrdinaryScope()
	if err != nil {
		t.Fatal(err)
	}
	strict, err := owner.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strict.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1()); !errors.Is(err, ErrLimit) {
		t.Fatal("strict scope entered ordinary private constructor")
	}
	b, err := scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1())
	if err != nil {
		t.Fatal(err)
	}
	after := owner.Stats()
	remaining := after.Limit - after.Live
	if err := strict.ReserveStableMetadata(remaining); err != nil {
		t.Fatal(err)
	}
	before := owner.Stats()
	if _, err := scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1()); !errors.Is(err, ErrLimit) {
		t.Fatalf("overlap cap bypass: %v", err)
	}
	if owner.Stats() != before {
		t.Fatal("denied private birth changed census")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); b.ReleasePrivateSnapshotBackingV1() }()
	}
	wg.Wait()
	if got := owner.Stats(); got.Births != before.Births || got.Live >= before.Live {
		t.Fatalf("duplicate final claim: %+v", got)
	}
	strict.ReleaseStableMetadata()
	scope.ReleaseStableMetadata()
	owner.Close()
}
func TestPrivateSnapshotBackingMalformedAndOverflowRefusal(t *testing.T) {
	owner, _ := NewOrdinary(1 << 20)
	scope, _ := owner.NewOrdinaryScope()
	defer owner.Close()
	defer scope.ReleaseStableMetadata()
	before := owner.Stats()
	for _, raw := range []PrivateSnapshotLayoutV1{{}, {Holder: 512, Completion: 160, Point: 8, State: 16}, {Holder: math.MaxUint64, Completion: 160, Point: 8}} {
		if _, err := scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, raw); !errors.Is(err, ErrLimit) {
			t.Fatalf("malformed private stamp: %v", err)
		}
		if got := owner.Stats(); got != before {
			t.Fatal("malformed stamp changed original scope")
		}
	}
	owner.mu.Lock()
	owner.births = math.MaxUint64
	owner.mu.Unlock()
	if _, err := scope.ReservePrivateSnapshotBackingV1(PrivatePointReadV1, privateSnapshotTestLayoutV1()); !errors.Is(err, ErrLimit) {
		t.Fatalf("cumulative overflow bypass: %v", err)
	}
}
