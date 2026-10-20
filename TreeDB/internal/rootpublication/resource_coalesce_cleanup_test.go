package rootpublication

import (
	"errors"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Refuse the next real admission after this original producer is retained by
// a private clone. The producer's real Release independently refuses cleanup.
type collisionCleanupOperations struct {
	ownedTokenOperationsFixture
	budget      *registryBudget
	refuseClone bool
}

func (o *collisionCleanupOperations) Retain() error {
	if err := o.ownedTokenOperationsFixture.Retain(); err != nil {
		return err
	}
	if o.refuseClone {
		o.refuseClone = false
		o.budget.cap = o.owner.Bytes()
	}
	return nil
}

func TestOwnedResourceCollisionCleanupPreservesOriginalErrors(t *testing.T) {
	for _, refuseClone := range []bool{true, false} {
		name := "replaced_clone"
		if refuseClone {
			name = "clone_admission_and_accumulated_entries"
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
			newOperations := func() *collisionCleanupOperations {
				charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(collisionCleanupOperations{})))
				if err := owner.AddPending(charge); err != nil {
					t.Fatal(err)
				}
				o := &collisionCleanupOperations{ownedTokenOperationsFixture: ownedTokenOperationsFixture{owner: &owner, charge: charge}, budget: budget}
				o.refs.Store(1)
				return o
			}
			a, b := newOperations(), newOperations()
			fileA := writeStableResourceFixture(t, t.TempDir(), "a", "payload")
			fileB := writeStableResourceFixture(t, t.TempDir(), "b", "payload")
			add := func(builder *StableResourceSetBuilder, file *os.File, lane string, ops *collisionCleanupOperations) {
				identity, e := StableIdentityFromFile(file)
				if e != nil {
					t.Fatal(e)
				}
				if e = registry.Observe(identity); e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					if e := registry.Unobserve(identity); e != nil {
						t.Error(e)
					}
				})
				token, e := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, OwnedOperations: ops, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: lane, ResourceID: lane, DiagnosticPath: lane, Generation: 1, File: file, Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile})
				if e != nil {
					t.Fatal(e)
				}
				if e = builder.Add(token); e != nil {
					t.Fatal(e)
				}
			}
			parentBuilder, err := NewStableResourceSetBuilderWithMetadata(&owner)
			if err != nil {
				t.Fatal(err)
			}
			add(parentBuilder, fileA, "a", a)
			if refuseClone {
				add(parentBuilder, fileB, "b", b)
			}
			parent, err := parentBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			held, err := CloneStableResourceSetExcludingKinds(parent)
			if err != nil {
				t.Fatal(err)
			}
			incomingBuilder, err := NewStableResourceSetBuilderWithMetadata(&owner)
			if err != nil {
				t.Fatal(err)
			}
			add(incomingBuilder, fileA, "z", b)
			incoming, err := incomingBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			if _, e := a.Release(); e != nil {
				t.Fatal(e)
			}
			if _, e := b.Release(); e != nil {
				t.Fatal(e)
			}
			builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
			if err != nil {
				t.Fatal(err)
			}
			if err = builder.Merge(parent); err != nil {
				t.Fatal(err)
			}
			old, source, beforePins := builder.kindViews, incoming.kindViews, registry.Stats().ActivePins
			physicalA, physicalB := errors.New("original producer A release refusal"), errors.New("original producer B release refusal")
			a.releaseErr, b.releaseErr = physicalA, physicalB
			b.refuseClone = refuseClone
			first := builder.Merge(incoming)
			if first == nil || !errors.Is(first, physicalA) || refuseClone && (!errors.Is(first, retainedalloc.ErrCapacity) || !errors.Is(first, physicalB)) {
				t.Fatalf("constructor dropped actual original cleanup outcome: %v; A releases=%d B releases=%d", first, a.releaseCalls.Load(), b.releaseCalls.Load())
			}
			var receipt *originalReleaseFailure
			if !errors.As(first, &receipt) || receipt.original == nil || builder.kindViews != old || incoming.kindViews != source || ResourceOwnerState(incoming.owner.Load()) != ResourceOwnerBuilder || held.Len() < 1 || registry.Stats().ActivePins != beforePins || registry.failedPinned != nil {
				t.Fatal("refusal lost original receipt, independently retained inputs, or physical custody")
			}
			// Seeing the callback's sentinel is insufficient: its refused clone
			// and actual charged producer edge must remain reachable from first.
			seen := map[*originalReleaseFailure]bool{}
			retained := map[StableResourceOwnedOperations]bool{}
			var visit func(error)
			visit = func(err error) {
				r, ok := err.(*originalReleaseFailure)
				if !ok || seen[r] {
					return
				}
				seen[r] = true
				var token *StableResourceToken
				switch original := r.original.(type) {
				case *StableResourceToken:
					token = original
					if r != &original.cleanupFailure {
						t.Fatal("substituted token receipt")
					}
				case *resourceEntryBacking:
					if r != &original.allocation.failure {
						t.Fatal("substituted backing receipt")
					}
					// The original graph transfers the entry's token into its
					// intrusive child before clearing the containing alias.
					if current := original.allocation.release.current; current != nil {
						if metadata, ok := current.value.(*resourceTokenMetadataReleaseV6); ok {
							token = metadata.token
							if token == nil || current.allocation != token.metadata {
								t.Fatal("lost original metadata child")
							}
						}
					}
				}
				if token != nil && token.metadata != nil && token.metadata.owner == &owner && token.ownedOperations != nil {
					retained[token.ownedOperations] = true
				}
				for _, cause := range r.causes {
					visit(cause)
				}
			}
			visit(first)
			if !retained[a] || refuseClone && !retained[b] {
				t.Fatal("caller received error text without original refused clone custody")
			}
			before, callsA, callsB := owner.Bytes(), a.releaseCalls.Load(), b.releaseCalls.Load()
			for i := 0; i < 3; i++ {
				_ = errors.Is(first, physicalA)
				_ = errors.Is(first, physicalB)
			}
			if owner.Bytes() != before || a.releaseCalls.Load() != callsA || b.releaseCalls.Load() != callsB {
				t.Fatal("observing original failure retried cleanup/refund")
			}
			budget.cap = 1 << 20
			a.releaseErr, b.releaseErr = nil, nil
			if err = builder.AbandonWithError(); err != nil {
				t.Fatal(err)
			}
			if err = incoming.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if held.Len() < 1 || registry.Stats().ActivePins == 0 {
				t.Fatal("failed private cleanup consumed held parent")
			}
			if err = held.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			enrollment.Close()
			if owner.Bytes() == 0 || owner.Bytes() >= before || budget.bytes != owner.Bytes() || registry.Stats().ActivePins != 0 || !errors.Is(first, physicalA) {
				t.Fatalf("failed original debt or independent refunds lost: before=%d after=%d budget=%d pins=%d", before, owner.Bytes(), budget.bytes, registry.Stats().ActivePins)
			}
		})
	}
}
