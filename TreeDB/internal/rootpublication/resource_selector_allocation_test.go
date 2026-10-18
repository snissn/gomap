package rootpublication

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func selectedSelectorFixture(t *testing.T, owner *retainedalloc.Owner, registry *IdentityPinRegistry, n int, operations StableResourceOwnedOperations) (*StableResourceSet, StableResourceSelector) {
	t.Helper()
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Abandon()
	var selector StableResourceSelector
	dir := t.TempDir()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("resource-%04d", i)
		file := writeStableResourceFixture(t, dir, id, "payload")
		identity, err := StableIdentityFromFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err = registry.Observe(identity); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := registry.Unobserve(identity); err != nil {
				t.Error(err)
			}
		})
		obligation := appendMutationTestObligation(uint64(i + 1))
		obligation.Reachability = ReachabilityIndexFile
		token, err := NewStableResourceToken(StableResourceSpec{
			MetadataOwner: owner, OwnedOperations: operations, PinRegistry: registry,
			Kind: ResourceIndex, LogicalLane: "lane", ResourceID: id,
			DiagnosticPath: id, Generation: 1, File: file,
			Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile,
			LogicalObligations: []StableLogicalObligation{obligation},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = builder.Add(token); err != nil {
			t.Fatal(err)
		}
		if i == n/2 {
			selector = StableResourceSelector{Kind: ResourceIndex, LogicalLane: "lane", ResourceID: id, PhysicalGeneration: 1, Obligation: obligation}
		}
	}
	result, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	return result, selector
}

func TestOwnedResourceSelectorSparseExactLookup(t *testing.T) {
	var clonedCharge uint64
	for _, n := range []int{1, 128, 256} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var owner retainedalloc.Owner
			owner.Initialize(0)
			budget := &registryBudget{cap: 64 << 20}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer enrollment.Close()
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			source, selector := selectedSelectorFixture(t, &owner, registry, n, nil)
			before := owner.Bytes()
			var work selectedResourceSelectorWork
			selected, err := cloneSelectedStableResourceForSelector(source, selector, &work)
			if err != nil {
				t.Fatal(err)
			}
			if selected.MetadataOwner() != &owner || work.EntriesCloned != 1 || work.LogicalIndexNodes == 0 || work.LogicalIndexNodes > 20 {
				t.Fatalf("non-sparse selection: %+v owner=%p", work, selected.MetadataOwner())
			}
			delta := owner.Bytes() - before
			if clonedCharge == 0 {
				clonedCharge = delta
			} else if delta != clonedCharge {
				t.Fatalf("selector charge scales with inventory: n=%d bytes=%d want=%d", n, delta, clonedCharge)
			}
			if err = source.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			// The independent selected entry, including its exact obligation, survives
			// disposal of the original inventory and producer admission closure.
			budget.closed = true
			view := selected.kindViews.get(ResourceIndex)
			entry := findStableResourceLogical(view.logical, stableLogicalResourceKey{kind: selector.Kind, lane: selector.LogicalLane, resourceID: selector.ResourceID, generation: selector.PhysicalGeneration})
			if entry == nil || view.count != 1 || view.logicalObligationCount != 1 {
				t.Fatal("selected identity/obligation lost")
			}
			if err = selected.FlushThrough(); err != nil {
				t.Fatal(err)
			}
			if err = selected.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			enrollment.Close()
			if owner.Bytes() != 0 || budget.bytes != 0 {
				t.Fatalf("metadata leak: owner=%d budget=%d", owner.Bytes(), budget.bytes)
			}
		})
	}
}

func TestOwnedResourceSelectorCapacityUnwindsEveryPrivateStage(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	source, selector := selectedSelectorFixture(t, &owner, registry, 1, nil)
	baseline := owner.Bytes()
	refusals := 0
	succeeded := false
	for allowance := uint64(0); allowance < 1<<20; allowance += 64 {
		budget.cap = baseline + allowance
		selected, err := CloneStableResourceForSelector(source, selector)
		if err == nil {
			if selected == nil || !selected.hasSelectedMetadata() {
				t.Fatal("selection escaped selected allocation authority")
			}
			if err = selected.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			succeeded = true
			break
		}
		if selected != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
			t.Fatalf("refusal changed ownership: %v", err)
		}
		refusals++
		if owner.Bytes() != baseline || budget.bytes != baseline {
			t.Fatalf("partial allocation leaked at %d: owner=%d budget=%d baseline=%d", allowance, owner.Bytes(), budget.bytes, baseline)
		}
	}
	if !succeeded {
		t.Fatal("selection never fit finite allowance")
	}
	if refusals < 8 {
		t.Fatalf("insufficient private-stage controls: %d", refusals)
	}
	budget.cap = 1 << 20
	for _, change := range []func(*StableResourceSelector){
		func(s *StableResourceSelector) { s.ResourceID = "absent" },
		func(s *StableResourceSelector) { s.PhysicalGeneration++ },
		func(s *StableResourceSelector) { s.Obligation.Reachability = ReachabilityColumnManifest },
	} {
		wrong := selector
		change(&wrong)
		if selected, err := CloneStableResourceForSelector(source, wrong); selected != nil || err == nil {
			t.Fatal("inexact selector accepted")
		}
		if owner.Bytes() != baseline {
			t.Fatal("rejected selector retained private metadata")
		}
	}
	if err = source.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 {
		t.Fatal("last inventory edge leaked")
	}
	if selected, err := CloneStableResourceForSelector(source, selector); selected != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released inventory accepted: %v", err)
	}
}

type refusingSelectorOperations struct {
	ownedTokenOperationsFixture
	source    *StableResourceSet
	retainErr error
	armed     bool
}

func (o *refusingSelectorOperations) Retain() error {
	if o.armed {
		if err := o.source.ReleaseWithError(); err != nil {
			return err
		}
		o.source = nil
		return o.retainErr
	}
	return o.ownedTokenOperationsFixture.Retain()
}

func TestOwnedResourceSelectorOriginalCleanupFailureCustody(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(refusingSelectorOperations{})))
	if err = owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	physicalErr, preparationErr := errors.New("original physical release refused"), errors.New("original selected retain refused")
	ops := &refusingSelectorOperations{ownedTokenOperationsFixture: ownedTokenOperationsFixture{owner: &owner, charge: charge}, retainErr: preparationErr}
	ops.refs.Store(1)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	source, selector := selectedSelectorFixture(t, &owner, registry, 1, ops)
	if _, err = ops.Release(); err != nil {
		t.Fatal(err)
	}
	ops.source, ops.armed, ops.releaseErr = source, true, physicalErr
	selected, first := CloneStableResourceForSelector(source, selector)
	if selected != nil || !errors.Is(first, physicalErr) || !errors.Is(first, preparationErr) {
		t.Fatalf("original causes lost: result=%p err=%v", selected, first)
	}
	var receipt *originalReleaseFailure
	if !errors.As(first, &receipt) {
		t.Fatal("missing original operation receipt")
	}
	op, ok := receipt.original.(*selectedResourceSelectorOperation)
	if !ok || op.allocation == nil || op.allocation.cleanupResult() != first {
		t.Fatal("failure replaced original operation custody")
	}
	held, calls := owner.Bytes(), ops.releaseCalls.Load()
	budget.closed = true
	if held == 0 || ops.disposed.Load() || ops.refs.Load() != 1 {
		t.Fatal("failed physical owner refunded")
	}
	for i := 0; i < 3; i++ {
		if err := source.ReleaseWithError(); err != nil {
			t.Fatal(err)
		}
		if op.allocation.cleanupResult() != first || owner.Bytes() != held || ops.releaseCalls.Load() != calls {
			t.Fatal("failure observation replayed or refunded original operands")
		}
	}
}

// Keep the exact fixed operations signature checked by the compiler.
var _ interface {
	FlushThrough(*os.File, DurableFrontier) error
} = (*refusingSelectorOperations)(nil)
