package freelist

import (
	"errors"
	"runtime"
	"testing"
	"unsafe"
)

// Historical fixtures exercise synthetic component accounting on both platforms.
// The current request and resident are independently debited; no request is
// retained in creator backing. These fixtures grant no installed provenance or
// Linux class, whole-caller fit, or finite admission certificate.
// componentBorrowedRequest5108 is owned by one synchronous test operation.
// Tests which need epoch retirement use orderedSplitRequest5108 below.
type componentBorrowedRequest5108 struct{ bytes, calls uint64 }

func (request *componentBorrowedRequest5108) ReserveAllocation(n uint64) error {
	if n > ^uint64(0)-request.bytes {
		return ErrNoAllocatablePage
	}
	request.calls++
	request.bytes += n
	return nil
}

// This test-only bootstrap deliberately does not call the platform-certified
// constructor. It prepays the synthetic creator class in request/resident order
// before its intrinsic birth, using independent facets and exact last-edge release.
// The real public constructor refusal remains a separately asserted boundary.
func newComponentAllocationCreator5108(request AllocationRequestCreditV1, resident AllocationResidentCreditV1) (*allocationCreditLeaseV1, error) {
	if request == nil || resident == nil {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	bytes := allocationClassV1(uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), true)
	if err := request.ReserveAllocation(bytes); err != nil {
		return nil, err
	}
	if err := resident.ReserveAllocation(bytes); err != nil {
		return nil, err
	}
	if err := resident.RetainAllocationCredit(); err != nil {
		return nil, err
	}
	return &allocationCreditLeaseV1{resident: resident, refs: 1}, nil
}

type syntheticBorrowedRequest5108 struct{}

func (syntheticBorrowedRequest5108) ReserveAllocation(uint64) error { return nil }

func requestForCreator5108(creator *allocationCreditLeaseV1) AllocationRequestCreditV1 {
	if creator == nil {
		return nil
	}
	return &componentBorrowedRequest5108{}
}

func requestForAllocator5108(allocator *Allocator) AllocationRequestCreditV1 {
	if allocator == nil {
		return nil
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.cow == nil || allocator.cow.txn == nil {
		return nil
	}
	return requestForCreator5108(allocator.cow.txn.buildCreator)
}

// Historical class tests keep their one synthetic resident counter. This
// distinct test-only scratch proxy grants no installed-owner provenance; real
// split tests below use independently debited scopes instead.
type syntheticScratchResident5108 struct{ resident AllocationResidentCreditV1 }

func (scope syntheticScratchResident5108) ReserveAllocation(n uint64) error {
	return scope.resident.ReserveAllocation(n)
}
func (syntheticScratchResident5108) RetainAllocationCredit() error { return nil }
func (syntheticScratchResident5108) ReleaseAllocationCredit()      {}
func scratchForCreator5108(creator *allocationCreditLeaseV1) *AllocationCreatorV1 {
	if creator == nil {
		return nil
	}
	return &AllocationCreatorV1{resident: syntheticScratchResident5108{creator.resident}, refs: 1}
}
func scratchForAllocator5108(allocator *Allocator) *AllocationCreatorV1 {
	if allocator == nil {
		return nil
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.cow == nil || allocator.cow.txn == nil {
		return nil
	}
	return scratchForCreator5108(allocator.cow.txn.buildCreator)
}

type orderedSplitRequest5108 struct {
	events          *[]string
	bytes, calls    uint64
	reject, retired bool
}

func (request *orderedSplitRequest5108) ReserveAllocation(n uint64) error {
	request.calls++
	*request.events = append(*request.events, "request")
	if request.retired || request.reject {
		return ErrAllocationCertificateIncompleteV1
	}
	request.bytes += n
	return nil
}

type orderedSplitResident5108 struct {
	events                 *[]string
	bytes, calls, releases uint64
	reject                 bool
}

func (resident *orderedSplitResident5108) ReserveAllocation(n uint64) error {
	resident.calls++
	*resident.events = append(*resident.events, "resident")
	if resident.reject {
		return ErrAllocationCertificateIncompleteV1
	}
	resident.bytes += n
	return nil
}
func (*orderedSplitResident5108) RetainAllocationCredit() error     { return nil }
func (resident *orderedSplitResident5108) ReleaseAllocationCredit() { resident.releases++ }

func TestAllocationRequestResidentOrderAndRetirement5108(t *testing.T) {
	t.Run("constructor-platform-and-component-bootstrap", func(t *testing.T) {
		var events []string
		request := &orderedSplitRequest5108{events: &events}
		resident := &orderedSplitResident5108{events: &events}
		creator, err := newComponentAllocationCreator5108(request, resident)
		want := allocationClassV1(uint64(unsafe.Sizeof(AllocationCreatorV1{})), true)
		if err != nil || creator == nil || request.bytes != want || resident.bytes != want || len(events) != 2 || events[0] != "request" || events[1] != "resident" {
			t.Fatal("synthetic component birth was not fully paid in order", err, events)
		}
		creator.release()
		if resident.releases != 1 {
			t.Fatal("synthetic creator last edge leaked")
		}
		events = nil
		request = &orderedSplitRequest5108{events: &events}
		resident = &orderedSplitResident5108{events: &events}
		for _, missing := range []bool{false, true} {
			var candidate *AllocationCreatorV1
			if missing {
				candidate, err = NewAllocationCreatorV1(nil, resident)
			} else {
				candidate, err = NewAllocationCreatorV1(request, nil)
			}
			if candidate != nil || !errors.Is(err, ErrAllocationCertificateIncompleteV1) || request.calls != 0 || resident.calls != 0 {
				t.Fatal("missing facet crossed constructor debit", err)
			}
		}
		creator, err = NewAllocationCreatorV1(request, resident)
		if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || runtime.Version() != "go1.26.3" {
			if creator != nil || !errors.Is(err, ErrAllocationCertificateIncompleteV1) || request.calls != 0 || resident.calls != 0 {
				t.Fatal("unsupported production constructor crossed debit", err)
			}
			return
		}
		if err != nil || creator == nil || request.bytes != want || resident.bytes != want || len(events) != 2 || events[0] != "request" || events[1] != "resident" {
			t.Fatal("supported constructor lost split debit", err, events)
		}
		creator.release()
		if resident.releases != 1 {
			t.Fatal("supported creator last edge leaked")
		}
	})

	var events []string
	request := &orderedSplitRequest5108{events: &events, reject: true}
	resident := &orderedSplitResident5108{events: &events}
	creator := &AllocationCreatorV1{resident: resident, refs: 1}
	if err := creator.reserve(request, 352, 1); !errors.Is(err, ErrAllocationCertificateIncompleteV1) || resident.calls != 0 || creator.refs != 1 {
		t.Fatalf("request refusal mutated resident: %v %+v", err, resident)
	}
	request.reject = false
	resident.reject = true
	events = nil
	if err := creator.reserve(request, 352, 1); !errors.Is(err, ErrAllocationCertificateIncompleteV1) || request.bytes != 352 || resident.bytes != 0 || creator.refs != 1 || len(events) != 2 || events[0] != "request" || events[1] != "resident" {
		t.Fatalf("resident refusal refunded/reordered: %v events=%v", err, events)
	}
	// Closing the old request does not affect the actual resident creator.
	request.retired = true
	resident.reject = false
	nextRequest := &orderedSplitRequest5108{events: &events}
	if err := creator.reserve(nextRequest, 2304, 1); err != nil {
		t.Fatal(err)
	}
	if request.bytes != 352 || nextRequest.bytes != 2304 || resident.bytes != 2304 || creator.refs != 2 {
		t.Fatal("old request retained/rebound")
	}
	creator.release()
	if resident.releases != 0 {
		t.Fatal("creator released before last edge")
	}
	creator.release()
	if resident.releases != 1 {
		t.Fatal("last creator edge did not release")
	}
}

func TestAllocationLoanPreservesHistoricalCreators5108(t *testing.T) {
	var events []string
	request := &orderedSplitRequest5108{events: &events}
	oldResident := &orderedSplitResident5108{events: &events}
	newResident := &orderedSplitResident5108{events: &events}
	old := &AllocationCreatorV1{resident: oldResident, refs: 1}
	destination := &AllocationCreatorV1{resident: newResident, refs: 1}
	operation, err := admitAllocationLoanOperationV1(request, destination, 8192, 384, 1)
	if err != nil {
		t.Fatal(err)
	}
	if request.bytes != 8192 || newResident.bytes != 384 || oldResident.bytes != 0 || old.resident != oldResident || old.refs != 1 {
		t.Fatal("loan recharged/rebound historical creator")
	}
	operation.close()
	destination.release()
	old.release()
	if oldResident.releases != 1 || newResident.releases != 1 {
		t.Fatal("creator lifetime lost")
	}
}

func TestAllocationScratchRefusalAndIndependentLifetime5108(t *testing.T) {
	var events []string
	request := &orderedSplitRequest5108{events: &events}
	resident := &orderedSplitResident5108{events: &events}
	short := &orderedSplitResident5108{events: &events}
	permanent := &AllocationCreatorV1{resident: resident, refs: 1}
	scratch := &AllocationCreatorV1{resident: short, refs: 1}
	for _, invalid := range []*AllocationCreatorV1{nil, permanent} {
		if err := reserveAllocationScratchV1(request, invalid, permanent, 16, 8, false); !errors.Is(err, ErrAllocationCertificateIncompleteV1) || request.calls != 0 || resident.calls != 0 {
			t.Fatal("missing/distinct scratch did not refuse before debit", err)
		}
	}
	if err := reserveAllocationScratchV1(request, scratch, permanent, ^uint64(0), 8, false); !errors.Is(err, ErrNoAllocatablePage) || request.calls != 0 || short.calls != 0 {
		t.Fatal("overflow crossed debit", err)
	}
	if err := reserveAllocationScratchV1(request, scratch, permanent, 16, 8, false); err != nil {
		t.Fatal(err)
	}
	backing := make([]uint64, 16)
	backing[15] = 7
	clear(backing[:cap(backing)])
	scratch.release()
	if short.releases != 1 || resident.releases != 0 || permanent.refs != 1 || request.bytes != 128 || short.bytes != 128 {
		t.Fatal("scratch lifetime leaked into permanent creator")
	}
	permanent.release()
}

// Epoch completion revokes birth permission, never the intrinsic historical
// creator. A new request alone and a new resident edge alone are insufficient.
func TestAllocationEpochRequiresCurrentRequestAndDestination5108(t *testing.T) {
	ordinary := buildCreditTxn5105(t, nil)
	if _, err := ordinary.AllocateAppend(); err != nil {
		t.Fatal("ordinary wrapper changed", err)
	}
	releaseTxnV1(ordinary)
	txn := buildCreditTxn5105(t, nil)
	var events []string
	oldRequest := &orderedSplitRequest5108{events: &events}
	oldResident := &orderedSplitResident5108{events: &events}
	oldCreator := &AllocationCreatorV1{resident: oldResident, refs: 1}
	if err := txn.bindBuildCreatorV1(oldCreator); err != nil {
		t.Fatal(err)
	}
	if err := txn.ReservePageWithAllocationRequestV1(oldRequest, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.AllocateAppendWithAllocationRequestV1(oldRequest); err != nil {
		t.Fatal(err)
	}
	historicalChunk := lookupChunk(txn.root, 0)
	if historicalChunk == nil || historicalChunk.creator != oldCreator {
		t.Fatal("historical creator missing")
	}
	if err := txn.endBuildCreatorV1(oldCreator); err != nil {
		t.Fatal(err)
	}
	oldRequest.retired = true
	high, stats, root, capacity := txn.highWater, txn.stats, txn.root, cap(txn.allocated)
	oldBytes, oldCalls, oldRefs := oldResident.bytes, oldRequest.calls, oldCreator.refs
	nextRequest := &orderedSplitRequest5108{events: &events}
	for _, request := range []AllocationRequestCreditV1{nil, nextRequest} {
		if _, err := txn.AllocateAppendWithAllocationRequestV1(request); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
			t.Fatal("ended epoch admitted birth", err)
		}
		if _, err := txn.cloneForAllocatorPrepare(request); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
			t.Fatal("ended epoch admitted clone", err)
		}
	}
	if txn.highWater != high || txn.stats != stats || txn.root != root || cap(txn.allocated) != capacity || oldResident.bytes != oldBytes || oldRequest.calls != oldCalls || oldCreator.refs != oldRefs || nextRequest.calls != 0 {
		t.Fatal("refusal changed backing, counters, or debit")
	}
	nextResident := &orderedSplitResident5108{events: &events}
	nextCreator := &AllocationCreatorV1{resident: nextResident, refs: 1}
	if err := txn.bindBuildCreatorV1(nextCreator); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.AllocateAppend(); !errors.Is(err, ErrAllocationCertificateIncompleteV1) || nextResident.calls != 0 {
		t.Fatal("new destination admitted absent request", err)
	}
	// Force actual replacement birth, not an in-capacity append, to prove both
	// independent debits and the old/new retained creator overlap.
	if err := txn.growAppendVectorsV1(nextRequest, cap(txn.allocated)+1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.AllocateAppendWithAllocationRequestV1(nextRequest); err != nil {
		t.Fatal(err)
	}
	if nextRequest.bytes == 0 || nextResident.bytes == 0 || nextRequest.bytes != nextResident.bytes || txn.allocatedCreator != nextCreator || historicalChunk.creator != oldCreator || oldResident.bytes != oldBytes || oldRequest.calls != oldCalls {
		t.Fatal("new epoch rebound historical creator or missed current debits")
	}
	if err := txn.endBuildCreatorV1(nextCreator); err != nil {
		t.Fatal(err)
	}
	releaseTxnV1(txn)
	oldCreator.release()
	nextCreator.release()
	if oldResident.releases != 1 || nextResident.releases != 1 {
		t.Fatal("actual last-edge lifetime leaked")
	}
}
