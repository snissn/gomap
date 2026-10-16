package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestOwnedResourceEntryKeepsDirectoryAndExactSummary(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "entry.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	obligation := appendMutationTestObligation(1)
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "db/index", ResourceID: "index.db", Generation: 1, DiagnosticPath: "index.db", File: file, Frontier: DurableFrontier{Bytes: 7}, Reachability: obligation.Reachability, LogicalObligations: []StableLogicalObligation{obligation}})
	if err != nil {
		t.Fatal(err)
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DependencyDirectoryV2{})))
	if err = owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	var released atomic.Int32
	directory := &DependencyDirectoryV2{metadata: &owner, metadataCharge: charge, ownedRelease: func() (bool, error) { released.Add(1); return true, nil }}
	directory.refs.Store(1)
	clone, err := token.cloneSharedPinnedDirectory(token.logicalLane, token.resourceID, token.diagnosticPath, token.frontier, token.reachability, token.logicalObligations, directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
	directory.Release()
	backing, err := newOwnedResourceEntry(&owner, clone)
	if err != nil {
		t.Fatal(err)
	}
	clone.Release()
	if released.Load() != 0 || directory.refs.Load() != 1 {
		t.Fatalf("physical release stole entry directory: released=%d refs=%d", released.Load(), directory.refs.Load())
	}
	entry := &backing.entries[0]
	if entry.token.ResourceID() != "index.db" || entry.logicalObligations.count != 1 || entry.logicalObligations.deltaSlice()[0] != obligation {
		t.Fatal("entry metadata changed after physical release")
	}
	expected := stableLogicalObligationCommitment{}
	expected.addObligation(obligation)
	if entry.reachability.commitment(obligation.Reachability) != expected {
		t.Fatal("exact summary missing from owned entry")
	}
	backing.release()
	if released.Load() != 1 || owner.Bytes() != 0 {
		t.Fatalf("last entry release leaked: releases=%d bytes=%d", released.Load(), owner.Bytes())
	}
}
