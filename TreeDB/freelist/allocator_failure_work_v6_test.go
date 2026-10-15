package freelist

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestNativeFiniteAllocatorContainingSizesV6(t *testing.T) {
	for _, row := range []struct {
		name string
		size uintptr
	}{
		{"PreparedCOWCandidateV1", unsafe.Sizeof(PreparedCOWCandidateV1{})},
		{"COWPreparationWorkV6", unsafe.Sizeof(COWPreparationWorkV6{})},
		{"AppendWork", unsafe.Sizeof(AppendWork{})},
		{"allocatorCOWStateV1", unsafe.Sizeof(allocatorCOWStateV1{})},
	} {
		t.Logf("layout sourceSymbol=%s size=%d allocationCharge=%d", row.name, row.size, retainedalloc.AllocationCharge(uint64(row.size)))
	}
}

// These controls construct only in-memory membership states. They perform no
// allocator activation, durable publication, pager or filesystem operation.
func TestNativeCOWFailureMembershipV6(t *testing.T) {
	a := &Allocator{}
	state := &allocatorCOWStateV1{ready: sync.NewCond(&a.mu)}
	a.cow = state
	old := &PreparedCOWCandidateV1{activated: true, activationOwner: state}
	newer := &PreparedCOWCandidateV1{activated: true, activationOwner: state}
	state.activated = []*PreparedCOWCandidateV1{old, newer}
	first, later := errors.New("first ambiguous write"), errors.New("later failure")
	w := iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20}
	if ready, err := AdmitCOWFailureV6(&w); !ready || err != nil {
		t.Fatalf("fixed admission: ready=%v err=%v", ready, err)
	}
	if err := a.FailCOWCandidateAdmittedV6(old, first); err != nil {
		t.Fatal(err)
	}
	if err := a.FailCOWCandidateAdmittedV6(newer, later); err != nil || state.waitErr != first {
		t.Fatalf("newer activation changed first cause: %v, %v", err, state.waitErr)
	}
	if err := a.FailCOWCandidateAdmittedV6(old, later); err != nil || state.waitErr != first {
		t.Fatalf("duplicate failure: %v, %v", err, state.waitErr)
	}
	for _, published := range []*PreparedCOWCandidateV1{
		{published: true},                                          // direct publication
		{activated: true, published: true},                         // removed activated prefix
		{activated: true, published: true, activationOwner: state}, // publication bit wins even before alias cleanup
	} {
		if err := a.FailCOWCandidateAdmittedV6(published, later); err != ErrCandidateConsumed || state.waitErr != first {
			t.Fatalf("published candidate accepted: %v", err)
		}
	}
	foreign := &Allocator{cow: &allocatorCOWStateV1{}}
	foreign.cow.ready = sync.NewCond(&foreign.mu)
	if err := foreign.FailCOWCandidateAdmittedV6(old, later); err != ErrCandidateConsumed || foreign.cow.waitErr != nil {
		t.Fatalf("foreign allocator accepted original membership: %v", err)
	}
	a.cow = &allocatorCOWStateV1{ready: sync.NewCond(&a.mu)}
	if err := a.FailCOWCandidateAdmittedV6(old, later); err != ErrCandidateConsumed || a.cow.waitErr != nil {
		t.Fatalf("replacement state accepted stale membership: %v", err)
	}
	private := &PreparedCOWCandidateV1{}
	a.cow.prepared = private
	if err := a.FailCOWCandidateAdmittedV6(private, first); err != nil || a.cow.waitErr != first {
		t.Fatalf("original private prepared slot: %v", err)
	}
}

func TestNativeCOWFailureAdmissionRefusesBeforeEffectsV6(t *testing.T) {
	w := iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	if ready, err := AdmitCOWFailureV6(&w); ready || err == nil || w.Records != 0 || w.Bytes != 0 {
		t.Fatalf("oversized fixed failure admission: ready=%v err=%v work=%+v", ready, err, w)
	}
}

func TestNativeInstalledAppendValidationRetainsRollbackV6(t *testing.T) {
	a := &Allocator{}
	live := &FreelistTxn{allocated: []allocatedPage{{id: 17, kind: ReservationAppendedData}}}
	a.cow = &allocatorCOWStateV1{txn: live}
	s := COWPreparationWorkV6{allocator: a, private: &FreelistTxn{allocated: []allocatedPage{{id: 17, kind: ReservationAppendedData}}}, commitSeq: 9,
		retirements: []COWRetirementV1{{PageIDs: []uint64{12}, LastReachableCommitSeq: 8}}}
	appendWork := AppendWork{allocator: a, id: 17, phase: 2}
	before := appendWork
	refusal := iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ready, err := s.ValidateInstalledPageV6(&appendWork, 17, 12, &refusal); ready || err != nil || !reflect.DeepEqual(appendWork, before) {
		t.Fatalf("partial admission consumed rollback: ready=%v err=%v", ready, err)
	}
	w := iterator.OrdinalScanWork{RecordLimit: 6, ByteLimit: 1 << 20}
	if ready, err := s.ValidateInstalledPageV6(&appendWork, 17, 12, &w); !ready || err != nil || !reflect.DeepEqual(appendWork, before) || s.adopted {
		t.Fatalf("read-only validation: ready=%v err=%v", ready, err)
	}
	s.private.allocated[0].id = 18
	w = iterator.OrdinalScanWork{RecordLimit: 6, ByteLimit: 1 << 20}
	if ready, err := s.ValidateInstalledPageV6(&appendWork, 17, 12, &w); ready || err != ErrGenerationParent || !reflect.DeepEqual(appendWork, before) {
		t.Fatalf("foreign private allocation: ready=%v err=%v", ready, err)
	}
}
