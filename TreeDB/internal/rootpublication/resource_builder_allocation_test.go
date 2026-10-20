package rootpublication

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Exercise the original builder, independently of the specialized selector
// constructor. A refused Add must leave the same token available for retry.
func TestOwnedBuilderCapacityUnwindsEveryPrivateStage(t *testing.T) {
	for _, withParent := range []bool{false, true} {
		name := "empty"
		if withParent {
			name = "retained_parent"
		}
		t.Run(name, func(t *testing.T) {
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
			parentBuilder, err := NewStableResourceSetBuilderWithMetadata(&owner)
			if err != nil {
				t.Fatal(err)
			}
			if err = parentBuilder.Add(builderUnwindToken(t, &owner, registry, "parent", 1)); err != nil {
				t.Fatal(err)
			}
			parent, err := parentBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Abandon()
			if withParent {
				copy, err := CloneStableResourceSetExcludingKinds(parent)
				if err != nil {
					t.Fatal(err)
				}
				if err = builder.Merge(copy); err != nil {
					t.Fatal(err)
				}
			}
			token := builderUnwindToken(t, &owner, registry, "incoming", 2)
			baseline, pins := owner.Bytes(), registry.Stats().ActivePins
			views, tokenRefs := builder.kindViews, token.metadata.refs.Load()
			refusals, succeeded := 0, false
			for allowance := uint64(0); allowance < 1<<20; allowance += 64 {
				budget.cap = baseline + allowance
				err = builder.Add(token)
				if err == nil {
					succeeded = true
					break
				}
				if !errors.Is(err, retainedalloc.ErrCapacity) {
					t.Fatalf("Add at allowance %d: %v", allowance, err)
				}
				refusals++
				if owner.Bytes() != baseline || budget.bytes != baseline || builder.kindViews != views || token.metadata.refs.Load() != tokenRefs || ResourceOwnerState(token.owner.Load()) != ResourceOwnerToken || token.released.Load() || registry.Stats().ActivePins != pins || parent.Len() != 1 {
					t.Fatalf("partial Add leaked or changed custody at %d: owner=%d budget=%d baseline=%d token=%v", allowance, owner.Bytes(), budget.bytes, baseline, ResourceOwnerState(token.owner.Load()))
				}
			}
			if !succeeded || refusals < 8 {
				t.Fatalf("finite retry did not cover private stages: success=%v refusals=%d", succeeded, refusals)
			}
			beforeFreeze := owner.Bytes()
			budget.cap = beforeFreeze
			if result, err := builder.Freeze(); result != nil || !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != beforeFreeze || budget.bytes != beforeFreeze {
				t.Fatalf("Freeze refusal changed custody: result=%p err=%v", result, err)
			}
			budget.cap = 1 << 20
			result, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if withParent {
				want++
			}
			if result.Len() != want || ResourceOwnerState(token.owner.Load()) != ResourceOwnerShared {
				t.Fatal("retry did not transfer the complete original token once")
			}
			if err = result.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			held := owner.Bytes()
			if parent.Len() != 1 || held == 0 || registry.Stats().ActivePins != 1 {
				t.Fatal("output disposal lost independent parent")
			}
			if err = result.ReleaseWithError(); err != nil || owner.Bytes() != held {
				t.Fatal("repeated disposal refunded twice")
			}
			if err = parent.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			enrollment.Close()
			if owner.Bytes() != 0 || budget.bytes != 0 || registry.Stats().ActivePins != 0 {
				t.Fatalf("final owner=%d budget=%d pins=%d", owner.Bytes(), budget.bytes, registry.Stats().ActivePins)
			}
		})
	}
}

func builderUnwindToken(t *testing.T, owner *retainedalloc.Owner, registry *IdentityPinRegistry, name string, revision uint64) *StableResourceToken {
	t.Helper()
	file := writeStableResourceFixture(t, t.TempDir(), name, "payload")
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
	obligation := appendMutationTestObligation(revision)
	obligation.Reachability = ReachabilityIndexFile
	token, err := NewStableResourceToken(StableResourceSpec{
		MetadataOwner: owner, PinRegistry: registry, Kind: ResourceIndex,
		LogicalLane: "lane", ResourceID: name, DiagnosticPath: name,
		Generation: 1, File: file, Frontier: DurableFrontier{Bytes: 7},
		Reachability: ReachabilityIndexFile, LogicalObligations: []StableLogicalObligation{obligation},
	})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// The existing admission interface can overlap another legitimate token
// claimant after Add's initial ownership check, without a production test hook.
type claimingBuilderBudget struct {
	registryBudget
	token *StableResourceToken
}

type claimingBuilderLease struct {
	retainedalloc.Lease
	budget *claimingBuilderBudget
}

func (b *claimingBuilderBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	lease, err := b.registryBudget.AcquireRetention(n)
	if err != nil {
		return nil, err
	}
	return &claimingBuilderLease{Lease: lease, budget: b}, nil
}

func (l *claimingBuilderLease) Resize(n uint64) error {
	if token := l.budget.token; token != nil {
		l.budget.token = nil
		if err := token.claim(ResourceOwnerBuilder); err != nil {
			return err
		}
	}
	return l.Lease.Resize(n)
}

func TestOwnedBuilderFailedClaimUnwindsPrivateMetadata(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &claimingBuilderBudget{registryBudget: registryBudget{cap: 1 << 20}}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	token := builderUnwindToken(t, &owner, registry, "incoming", 2)
	baseline, refs := owner.Bytes(), token.metadata.refs.Load()
	budget.token = token
	first := builder.Add(token)
	if !errors.Is(first, ErrResourceOwnership) || owner.Bytes() != baseline || budget.bytes != baseline || token.metadata.refs.Load() != refs || token.released.Load() || builder.kindViews != nil || ResourceOwnerState(token.owner.Load()) != ResourceOwnerBuilder {
		t.Fatalf("failed claim changed original custody: err=%v owner=%d budget=%d baseline=%d", first, owner.Bytes(), budget.bytes, baseline)
	}
	if err = token.releaseFrom(ResourceOwnerBuilder); err != nil {
		t.Fatal(err)
	}
	if err = builder.AbandonWithError(); err != nil {
		t.Fatal(err)
	}
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 || registry.Stats().ActivePins != 0 {
		t.Fatal("failed claim cleanup retained original allocations")
	}
}
