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
	a := &radixCredit5105{limit: ^uint64(0)}
	lease, err := newAllocationCreditLeaseV1(a)
	if err != nil {
		t.Fatal(err)
	}
	radix := newPageRadixV1[CandidateIDV1]()
	first := candidateIDFromString("first")
	if err := radix.PutWithCredit(10, first, lease); err != nil {
		t.Fatal(err)
	}
	if err := radix.PutWithCredit(20, first, lease); err != nil {
		t.Fatal(err)
	}
	lease.release()
	if a.retained != 1 || a.released != 0 {
		t.Fatal("candidate release closed live shared nodes")
	}
	// A third leaf inserts above the existing two-key subtree. Deleting A's
	// leaf and its adjacent branch must preserve A's remaining shared branch.
	b := &radixCredit5105{limit: ^uint64(0)}
	next, err := newAllocationCreditLeaseV1(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := radix.PutWithCredit(1, first, next); err != nil {
		t.Fatal(err)
	}
	next.release()
	charged := a.bytes
	radix.Delete(10)
	if a.released != 0 || a.bytes != charged {
		t.Fatal("erase refunded debit or released remaining owner")
	}
	radix.Delete(20)
	if a.released != 1 || a.bytes != charged {
		t.Fatal("last actual node did not release owner")
	}
	radix.Delete(1)
	if b.released != 1 {
		t.Fatal("second owner retained")
	}
	// Admission failure leaves the collection byte-for-byte logically intact.
	c := &radixCredit5105{limit: 32}
	limited, err := newAllocationCreditLeaseV1(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := radix.PutWithCredit(99, first, limited); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal(err)
	}
	if radix.Len() != 0 || radix.root != nil {
		t.Fatal("failed debit mutated tree")
	}
	limited.release()
}

func TestNumericRadixActualAllocationClasses5105(t *testing.T) {
	classes := []struct {
		name      string
		raw, want uint64
	}{
		{"branch", uint64(unsafe.Sizeof(numericRadixBranchV1{})), 48},
		{"ownerleaf", uint64(unsafe.Sizeof(numericRadixLeafV1[uint64, CandidateIDV1]{})), 32},
		{"setleaf", uint64(unsafe.Sizeof(numericRadixLeafV1[uint64, struct{}]{})), 16},
		{"candidateleaf", uint64(unsafe.Sizeof(numericRadixLeafV1[CandidateIDV1, *reservation]{})), 32},
		{"wrapper", uint64(unsafe.Sizeof(numericRadixV1[uint64, CandidateIDV1]{})), 48},
		{"creditlease", uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), 32},
	}
	for _, class := range classes {
		got := allocationClassV1(class.raw, true)
		if got != class.want {
			t.Fatalf("%s raw=%d class=%d want=%d", class.name, class.raw, got, class.want)
		}
		t.Logf("%s raw=%d class=%d", class.name, class.raw, got)
	}
}
