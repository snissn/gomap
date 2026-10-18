package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativePublicationResourcesCapturedMetadataConstructor(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	var carrier NativePublicationResources
	if err = carrier.InitializeWithMetadata(&owner, nil); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() == 0 || budget.Stats().TotalBytes == 0 {
		t.Fatal("selected builder escaped original admission")
	}
	before := budget.Stats()
	if err := carrier.InitializeWithMetadata(&owner, nil); !errors.Is(err, rootpublication.ErrResourceOwnership) || budget.Stats() != before {
		t.Fatal("duplicate constructor changed original ownership", err)
	}
	ctx, err := carrier.context(1)
	if err != nil || ctx.AppliedCommandLSN != 1 {
		t.Fatal(err)
	}
	if err = carrier.finish(); err != nil {
		t.Fatal(err)
	}
	selected, _, err := carrier.CloneResources()
	if err != nil || selected.MetadataOwner() != &owner {
		t.Fatal("carrier lost captured metadata authority", err)
	}
	if err = selected.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	budget.Close()
	if err = carrier.ReleaseNativeResources(); err != nil {
		t.Fatal(err)
	}
	enrollment.Close()
	if owner.Bytes() != 0 || budget.Stats().TotalBytes != 0 {
		t.Fatal("original carrier builder leaked after Close", owner.Bytes(), budget.Stats())
	}
}

func TestNativePublicationResourcesMetadataRefusalBeforeEffects(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	budget.Close()
	before := budget.Stats()
	baseline := owner.Bytes()
	var carrier NativePublicationResources
	if err = carrier.InitializeWithMetadata(nil, nil); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal(err)
	}
	if err = carrier.InitializeWithMetadata(&owner, nil); err == nil {
		t.Fatal("closed governing budget admitted builder")
	}
	if carrier.builder != nil || carrier.resources != nil || carrier.selector != nil || owner.Bytes() != baseline || budget.Stats() != before {
		t.Fatal("preallocation refusal retained effects", owner.Bytes(), budget.Stats())
	}
	if err = carrier.ReleaseNativeResources(); err != nil {
		t.Fatal(err)
	}
	// The generic constructor remains usable by its existing lower callers.
	if err = carrier.Initialize(nil); err != nil {
		t.Fatal(err)
	}
	if err = carrier.ReleaseNativeResources(); err != nil {
		t.Fatal(err)
	}
}
