package freelist

import (
	"errors"
	"math/rand"
	"testing"
	"unsafe"
)

type radixCredit5105 struct {
	bytes, limit       uint64
	retained, released int
}

func (c *radixCredit5105) ReserveAllocation(bytes uint64) error {
	if bytes > c.limit-c.bytes {
		return ErrAllocationCertificateIncompleteV1
	}
	c.bytes += bytes
	return nil
}
func (c *radixCredit5105) RetainAllocationCredit() error { c.retained++; return nil }
func (c *radixCredit5105) ReleaseAllocationCredit()      { c.released++ }

func TestNumericRadixGlobalMapOracle5105(t *testing.T) {
	radix := newPageRadixV1[uint64]()
	oracle := map[uint64]uint64{}
	rng := rand.New(rand.NewSource(5105))
	for step := 0; step < 10000; step++ {
		key := rng.Uint64()
		if step%4 == 0 {
			key = uint64(step % 1024)
		}
		switch step % 3 {
		case 0:
			radix.Set(key, uint64(step))
			oracle[key] = uint64(step)
		case 1:
			radix.Delete(key)
			delete(oracle, key)
		case 2:
			got, ok := radix.Get(key)
			want, exists := oracle[key]
			if ok != exists || got != want {
				t.Fatal("lookup mismatch")
			}
		}
	}
	if radix.Len() != len(oracle) {
		t.Fatal("length mismatch")
	}
	var last uint64
	first := true
	radix.Range(func(key, value uint64) bool {
		if !first && key <= last {
			t.Fatal("numeric order")
		}
		first = false
		last = key
		if oracle[key] != value {
			t.Fatal("range mismatch")
		}
		return true
	})
	clone := radix.Clone()
	if !radix.Equal(clone) {
		t.Fatal("clone mismatch")
	}
	radix.Clear()
	if radix.Len() != 0 || clone.Len() != len(oracle) {
		t.Fatal("clone ownership")
	}
	clone.Clear()
}

func TestNumericRadixPredebitAndIntrinsicLifetime5105(t *testing.T) {
	a, lease := buildCreditLease5105(t)
	radix := newPageRadixV1[CandidateIDV1]()
	first := candidateIDFromString("first")
	for _, k := range []uint64{10, 20} {
		if err := radix.PutWithCredit(requestForCreator5108(lease), k, first, lease); err != nil {
			t.Fatal(err)
		}
	}
	chunk := radix.head
	if a.bytes != 32+numericRadixChunkClassV1[uint64, CandidateIDV1]() || lease.refs != 2 {
		t.Fatal("whole creating chunk not debited exactly once", a.bytes, lease.refs)
	}
	lease.release()
	b, next := buildCreditLease5105(t)
	radix.Delete(10)
	charged := a.bytes
	if a.released != 0 || chunk.used != 1 {
		t.Fatal("partly empty chunk released")
	}
	if err := radix.PutWithCredit(requestForCreator5108(next), 1, first, next); err != nil {
		t.Fatal(err)
	}
	next.release()
	if b.released != 1 || b.bytes != 32 || radix.head != chunk || chunk.credit != lease {
		t.Fatal("slot reuse transferred a retired creating owner")
	}
	radix.Delete(20)
	if a.released != 0 || a.bytes != charged {
		t.Fatal("retained reused slot lost whole backing")
	}
	radix.Delete(1)
	if a.released != 1 || a.bytes != charged || radix.head != nil || radix.available != nil || radix.freeSlots != 0 || *chunk != (numericRadixChunkV1[uint64, CandidateIDV1]{}) {
		t.Fatal("last actual chunk did not scrub and release owner")
	}
	c := &radixCredit5105{limit: 32}
	limited, err := newComponentAllocationCreator5108(&componentBorrowedRequest5108{}, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := radix.PutWithCredit(requestForCreator5108(limited), 99, first, limited); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal(err)
	}
	if radix.Len() != 0 || radix.root.chunk != nil || radix.head != nil || radix.freeSlots != 0 {
		t.Fatal("failed debit birthed or mutated tree")
	}
	limited.release()
}

func TestNumericRadixActualAllocationClasses5105(t *testing.T) {
	classes := []struct {
		name                   string
		raw, expectedRaw, want uint64
	}{
		{"setchunk", uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, struct{}]{})), 1584, 1792},
		{"ownerchunk", uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, CandidateIDV1]{})), 2096, 2304},
		{"candidatechunk", uint64(unsafe.Sizeof(numericRadixChunkV1[CandidateIDV1, *reservation]{})), 2096, 2304},
		{"candidate-set-chunk", uint64(unsafe.Sizeof(numericRadixChunkV1[CandidateIDV1, struct{}]{})), 1840, 2048},
		{"numeric-value-chunk", uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, uint64]{})), 1840, 2048},
		{"wrapper", uint64(unsafe.Sizeof(numericRadixV1[uint64, CandidateIDV1]{})), 72, 80},
		{"creditlease", uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), 32, 32},
	}
	for _, class := range classes {
		got := allocationClassV1(class.raw, true)
		if class.raw != class.expectedRaw || got != class.want {
			t.Fatalf("%s raw=%d class=%d want raw=%d class=%d", class.name, class.raw, got, class.expectedRaw, class.want)
		}
		t.Logf("%s raw=%d class=%d", class.name, class.raw, got)
	}
}

// Check tree slots and both intrinsic lists against the actual whole backing.
// Maps here are a test oracle only; the implementation has no directory.
func assertRadixChunks5105[K comparable, V comparable](t *testing.T, m *numericRadixV1[K, V]) {
	t.Helper()
	live := map[*numericRadixChunkV1[K, V]]bool{}
	available := map[*numericRadixChunkV1[K, V]]bool{}
	free, used := 0, 0
	var previous *numericRadixChunkV1[K, V]
	for c := m.head; c != nil; c = c.next {
		if live[c] || c.prev != previous || c.used == 0 || c.used > numericRadixChunkSlotsV1 {
			t.Fatal("invalid live chunk list")
		}
		live[c] = true
		seen := map[uint16]bool{}
		for slot := c.free; slot != 0; slot = c.nodes[slot-1].nextFree {
			if slot > numericRadixChunkSlotsV1 || seen[slot] {
				t.Fatal("invalid free-slot chain")
			}
			seen[slot] = true
			n := c.nodes[slot-1]
			n.nextFree = 0
			if n != (numericRadixNodeV1[K, V]{}) {
				t.Fatal("deleted slot retained key/value/child aliases")
			}
		}
		if len(seen)+int(c.used) != numericRadixChunkSlotsV1 {
			t.Fatal("unaccounted chunk capacity")
		}
		free += len(seen)
		used += int(c.used)
		previous = c
	}
	previous = nil
	for c := m.available; c != nil; c = c.availableNext {
		if available[c] || !live[c] || c.free == 0 || c.availablePrev != previous {
			t.Fatal("invalid available list")
		}
		available[c] = true
		previous = c
	}
	for c := range live {
		if (c.free != 0) != available[c] {
			t.Fatal("available chunk missing")
		}
	}
	seenNodes := map[numericRadixRefV1[K, V]]bool{}
	var walk func(numericRadixRefV1[K, V])
	walk = func(ref numericRadixRefV1[K, V]) {
		if ref.chunk == nil {
			return
		}
		if !live[ref.chunk] || seenNodes[ref] || ref.slot == 0 || ref.slot > numericRadixChunkSlotsV1 {
			t.Fatal("dangling/shared tree slot")
		}
		seenNodes[ref] = true
		n := ref.node()
		if n.nextFree != 0 {
			t.Fatal("tree references free slot")
		}
		if !n.leaf {
			if n.child[0].chunk == nil || n.child[1].chunk == nil {
				t.Fatal("missing branch child")
			}
			for _, child := range n.child {
				if !child.node().leaf && child.node().bit <= n.bit {
					t.Fatal("nonincreasing radix bit")
				}
				walk(child)
			}
		}
	}
	walk(m.root)
	wantNodes := 0
	if m.count != 0 {
		wantNodes = 2*m.count - 1
	}
	if used != wantNodes || len(seenNodes) != used || free != m.freeSlots {
		t.Fatal("slot/tree/capacity mismatch", used, wantNodes, free, m.freeSlots)
	}
}

func TestNumericRadixWholeChunkPlanAndBulkClone5105(t *testing.T) {
	a, lease := buildCreditLease5105(t)
	radix := newPageRadixV1[CandidateIDV1]()
	bytes, refs := radix.insertionCapacityV1(100)
	if refs != 7 || bytes != refs*numericRadixChunkClassV1[uint64, CandidateIDV1]() {
		t.Fatal("full prospective capacities omitted")
	}
	operation, err := admitAllocationOperationV1(requestForCreator5108(lease), lease, bytes, refs)
	if err != nil {
		t.Fatal(err)
	}
	value := candidateIDFromString("bulk")
	for i := uint64(0); i < 100; i++ {
		if err = radix.putAdmittedV1(requestForCreator5108(lease), i*19, value, lease, &operation); err != nil {
			t.Fatal(err)
		}
	}
	if operation.bytes != 0 || operation.refs != 0 {
		t.Fatal("planner and births differ")
	}
	operation.close()
	assertRadixChunks5105(t, radix)
	lease.release()
	// Poison comparison while the source has many branches: Range/Insert
	// reconstruction would invoke it; structural bulk copying must not.
	radix.diff = func(uint64, uint64) int { panic("clone reconstructed through Insert") }
	b, cloneOwner := buildCreditLease5105(t)
	clone, err := radix.CloneWithCredit(requestForCreator5108(cloneOwner), cloneOwner)
	if err != nil {
		t.Fatal(err)
	}
	if b.bytes != 32+80+7*numericRadixChunkClassV1[uint64, CandidateIDV1]() {
		t.Fatal("bulk clone whole charge", b.bytes)
	}
	cloneOwner.release()
	if clone.head == radix.head || !radix.Equal(clone) {
		t.Fatal("clone shares backing or changes search")
	}
	for i := uint64(0); i < 99; i++ {
		radix.Delete(i * 19)
		assertRadixChunks5105(t, radix)
	}
	if radix.freeSlots > numericRadixChunkSlotsV1-1 || a.released != 0 {
		t.Fatal("historical empty capacity retained")
	}
	radix.Clear()
	if a.released != 1 || b.released != 0 || clone.Len() != 100 {
		t.Fatal("clone creator lifetime crossed original")
	}
	assertRadixChunks5105(t, clone)
	clone.Clear()
	if b.released != 1 {
		t.Fatal("independent clone backing not released")
	}
}

func TestNumericRadixChunkGrowthDeniedBeforeBirth5105(t *testing.T) {
	a, lease := buildCreditLease5105(t)
	radix := newPageRadixV1[uint64]()
	for i := uint64(0); i < 16; i++ {
		if err := radix.PutWithCredit(requestForCreator5108(lease), i, i, lease); err != nil {
			t.Fatal(err)
		}
	}
	oldHead, oldRoot, oldFree, oldRefs := radix.head, radix.root, radix.freeSlots, lease.refs
	before := a.bytes
	a.limit = before
	if err := radix.PutWithCredit(requestForCreator5108(lease), 16, 16, lease); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal(err)
	}
	if radix.Len() != 16 || radix.head != oldHead || radix.root != oldRoot || radix.freeSlots != oldFree || a.bytes != before || lease.refs != oldRefs {
		t.Fatal("denied growth changed actual backing")
	}
	assertRadixChunks5105(t, radix)
	// Clone admission also rejects before its header or any chunk births.
	if clone, err := radix.CloneWithCredit(requestForCreator5108(lease), lease); clone != nil || !errors.Is(err, ErrAllocationCertificateIncompleteV1) || a.bytes != before || lease.refs != oldRefs {
		t.Fatal("clone partially admitted", err)
	}
	radix.Clear()
	lease.release()
	if a.released != 1 {
		t.Fatal("denied birth leaked creator")
	}
}

func TestNumericRadixCandidateOrderAndDeleteChurn5105(t *testing.T) {
	radix := newCandidateRadixV1[uint64]()
	oracle := map[CandidateIDV1]uint64{}
	rng := rand.New(rand.NewSource(5109))
	for i := 0; i < 2000; i++ {
		var key CandidateIDV1
		rng.Read(key[:])
		radix.Set(key, uint64(i))
		oracle[key] = uint64(i)
	}
	var last CandidateIDV1
	first, visits := true, 0
	radix.Range(func(key CandidateIDV1, value uint64) bool {
		if !first && string(key[:]) <= string(last[:]) {
			t.Fatal("128-bit lexical order")
		}
		if oracle[key] != value {
			t.Fatal("candidate range mismatch")
		}
		first = false
		last = key
		visits++
		return true
	})
	if visits != len(oracle) {
		t.Fatal("range coverage")
	}
	assertRadixChunks5105(t, radix)
	for key := range oracle {
		radix.Delete(key)
		assertRadixChunks5105(t, radix)
	}
	if radix.head != nil || radix.available != nil || radix.freeSlots != 0 {
		t.Fatal("deleted candidate history retained")
	}
}
