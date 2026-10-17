package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestPrimaryRuntimeOrdinaryDisposalPreservesAccountingV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ref, ready, err := a.PrepareReadRootV6(nil)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	r := &primaryDurableRuntimeV5{}
	charge := primaryRuntimeDescriptorChargeV5()
	if err = a.MetadataOwner().AddPending(charge); err != nil {
		t.Fatal(err)
	}
	bindReservedPrimaryRuntimeV5(r, a)
	r.records[0] = ref
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(a.MetadataOwner())
	if err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	r.slotResources[0] = set
	drops := a.Counters().RootDrops
	completion, ready, err := releasePrimaryRuntimeOriginalV6(a, r)
	if !ready || err != nil || set.Owner() != rootpublication.ResourceOwnerReleased || r.records[0] != (primaryarena.Ref{}) || r.slotResources[0] != nil || r.metadata != nil || r.metadataCharge != 0 || a.Counters().RootDrops != drops+1 {
		t.Fatalf("original completion: ready=%t err=%v", ready, err)
	}
	if !r.disposalMu.TryLock() {
		t.Fatal("runtime disposal returned with containing lock held")
	}
	r.disposalMu.Unlock()
	// The physical driver returned and the owner ends its runtime alias BEFORE
	// the accounting-only value refunds the containing allocation.
	owner := a.MetadataOwner()
	pending := owner.Bytes()
	if pending < charge || completion.charge != charge {
		t.Fatal("containing charge refunded within physical callback")
	}
	r = nil
	completion.complete()
	if owner.Bytes() != pending-charge {
		t.Fatal("accounting did not follow actual physical return")
	}
	completion.complete()
	if owner.Bytes() != pending-charge {
		t.Fatal("accounting replayed")
	}
}

func TestPrimaryRuntimeDisposalPhysicalFailureIsOriginalAndStickyV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	r := &primaryDurableRuntimeV5{}
	charge := primaryRuntimeDescriptorChargeV5()
	if err = a.MetadataOwner().AddPending(charge); err != nil {
		t.Fatal(err)
	}
	bindReservedPrimaryRuntimeV5(r, a)
	set := originalRecoverySetWithPhysicalCloseFault(t)
	r.slotResources[0] = set
	_, ready, first := releasePrimaryRuntimeOriginalV6(a, r)
	if ready || !errors.Is(first, os.ErrClosed) || r.disposalErr != first || r.slotResources[0] != set || r.metadataCharge != charge {
		t.Fatalf("physical refusal lost original custody: %t %v", ready, first)
	}
	_, ready, again := releasePrimaryRuntimeOriginalV6(a, r)
	if ready || again != first || r.slotResources[0] != set {
		t.Fatal("original physical callback replayed or first receipt replaced")
	}
}
