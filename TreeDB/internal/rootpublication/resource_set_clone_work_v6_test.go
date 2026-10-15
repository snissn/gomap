package rootpublication

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestAdmittedResourceCloneWithOrdinalWorkV6(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "native-clone.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	baseline := owner.Bytes()
	views := source.kindViews
	if clone, ready, err := CloneAdmittedStableResourceSetWithOrdinalWorkV6(source, &iterator.OrdinalScanWork{}); clone != nil || ready || err != nil || owner.Bytes() != baseline {
		t.Fatal("refusal changed original custody", ready, err)
	}
	work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	clone, ready, err := CloneAdmittedStableResourceSetWithOrdinalWorkV6(source, &work)
	if !ready || err != nil || clone == nil || clone == source || clone.kindViews != views || work.Records != 1 {
		t.Fatal("clone did not share actual immutable backing", ready, err)
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSet{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceAllocation{})))
	if owner.Bytes() != baseline+charge {
		t.Fatal("clone wrapper charge mismatch", owner.Bytes(), baseline, charge)
	}
	// Original release cannot destroy physical backing borrowed by the clone.
	if err := source.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if clone.kindViews != views || clone.Len() != 1 {
		t.Fatal("source disposal invalidated clone")
	}
	if err := clone.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != 0 {
		t.Fatal("clone retained admitted storage", owner.Bytes())
	}
	if _, _, err := CloneAdmittedStableResourceSetWithOrdinalWorkV6(source, &work); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("closed source cloned", err)
	}
	legacy := &StableResourceSet{}
	if _, _, err := CloneAdmittedStableResourceSetWithOrdinalWorkV6(legacy, &work); !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) {
		t.Fatal("legacy geometry silently traversed", err)
	}
}
