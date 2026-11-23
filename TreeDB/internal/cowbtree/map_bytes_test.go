package cowbtree

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSetBytesParityAllocationChargeAndZeroAllocReplace(t *testing.T) {
	base := NewMap[string, string](32)
	for i := 0; i < 400; i++ {
		base.Set(fmt.Sprintf("k%04d", i*2), fmt.Sprintf("v0-%04d", i))
	}
	base = base.SealOwned()

	// Fork two identical maps: apply identical mixed inserts/replaces using
	// Set on m1 and SetBytes on m2, verifying exact logical parity AND exact
	// AllocationCharge parity.
	m1 := base.ForkImmutable()
	m2 := base.ForkImmutable()
	rng := rand.New(rand.NewSource(7))
	for step := 0; step < 600; step++ {
		var k []byte
		if rng.Intn(2) == 0 {
			k = []byte(fmt.Sprintf("k%04d", rng.Intn(400)*2))
		} else {
			k = []byte(fmt.Sprintf("k%04d", rng.Intn(800)*2+1))
		}
		v := fmt.Sprintf("v-%d", step)
		p1, r1 := m1.Set(string(k), v)
		p2, r2 := SetBytes(m2, k, v)
		if p1 != p2 || r1 != r2 {
			t.Fatalf("step %d key=%q: Set=(%q,%v) SetBytes=(%q,%v)", step, k, p1, r1, p2, r2)
		}
	}
	m1 = m1.SealOwned()
	m2 = m2.SealOwned()
	if m1.Len() != m2.Len() {
		t.Fatalf("Len mismatch: m1=%d m2=%d", m1.Len(), m2.Len())
	}
	if m1.AllocationCharge() != m2.AllocationCharge() {
		t.Fatalf("AllocationCharge mismatch: Set=%d SetBytes=%d", m1.AllocationCharge(), m2.AllocationCharge())
	}
	it1, it2 := m1.Iter(), m2.Iter()
	for ok1, ok2 := it1.First(), it2.First(); ok1 || ok2; ok1, ok2 = it1.Next(), it2.Next() {
		if ok1 != ok2 || it1.Key() != it2.Key() || it1.Value() != it2.Value() {
			t.Fatalf("iter mismatch: (%v,%q,%q) vs (%v,%q,%q)", ok1, it1.Key(), it1.Value(), ok2, it2.Key(), it2.Value())
		}
	}

	// Verify 0 allocations when replacing an existing key on a private map
	// (including under -tags treedb_safe).
	priv := m2.ForkImmutable()
	key := []byte("k0100")
	SetBytes(priv, key, "warm")
	allocs := testing.AllocsPerRun(100, func() {
		_, replaced := SetBytes(priv, key, "updated")
		if !replaced {
			t.Fatal("expected replace")
		}
	})
	if allocs != 0 {
		t.Fatalf("SetBytes replace allocated %.2f times/op, want 0", allocs)
	}
}
