package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

func TestOwnedImportedTokenPreservesForeignOperationsAndLastCustody(t *testing.T) {
	var release, flush, syncs int
	token := stableTokenFixture(t, t.TempDir(), "foreign", 1, 4, ReachabilityIndexFile, "index.db", func(spec *StableResourceSpec) {
		spec.OnRelease = func() { release++ }
		spec.FlushThrough = func(*os.File, DurableFrontier) error { flush++; return nil }
		spec.SyncThrough = func(*os.File, DurableFrontier) error { syncs++; return nil }
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	var root *stableResourceEntryNode
	for _, v := range source.kindViews.all() {
		root = v.root
	}
	owner := new(retainedalloc.Owner)
	owner.Initialize(0)
	imported, err := newImportedResourceToken(owner, token, root, token.logicalLane, token.resourceID, token.diagnosticPath, token.frontier, token.reachability, token.logicalObligations, nil)
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := imported.cloneOwnedPinnedDirectory(imported.logicalLane, imported.resourceID, imported.diagnosticPath, DurableFrontier{Bytes: 8}, imported.reachability, imported.logicalObligations, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	if release != 0 || token.released.Load() {
		t.Fatal("foreign callback consumed while admitted aliases survive")
	}
	if err = imported.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err = cloned.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if flush != 1 || syncs != 1 {
		t.Fatalf("forwarded operations %d/%d", flush, syncs)
	}
	imported.Release()
	if release != 0 {
		t.Fatal("sibling release consumed original callback")
	}
	cloned.Release()
	if release != 1 || owner.Bytes() != 0 {
		t.Fatalf("last custody release=%d bytes=%d", release, owner.Bytes())
	}
	cloned.Release()
	if release != 1 {
		t.Fatal("callback replay")
	}
	if !errors.Is(cloned.FlushThrough(), ErrResourceOwnership) {
		t.Fatal("released operation remained callable")
	}
}

func TestOwnedImportedSetAliasAndRefusal(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "cloned", true: "direct"}[direct], func(t *testing.T) {
			dir := t.TempDir()
			callbacks := 0
			o := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "main", Generation: 1, FileID: 1, Length: 4, Reachability: ReachabilityColumnManifest, Digest: [32]byte{1}}
			token := stableTokenFixture(t, dir, "source", 1, 20, ReachabilityColumnManifest, "column", func(spec *StableResourceSpec) {
				spec.Kind = ResourceColumnAsset
				spec.LogicalObligations = []StableLogicalObligation{o}
				spec.OnRelease = func() { callbacks++ }
			})
			b := NewStableResourceSetBuilder()
			if err := b.Add(token); err != nil {
				t.Fatal(err)
			}
			source, err := b.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			owner := new(retainedalloc.Owner)
			owner.Initialize(0)
			imported, err := ImportStableResourceSetMetadata(owner, source)
			if err != nil {
				t.Fatal(err)
			}
			clone := imported
			if !direct {
				clone, err = CloneStableResourceSetExcludingKinds(imported)
			}
			if err != nil {
				t.Fatal(err)
			}
			source.Release()
			if !direct {
				imported.Release()
			}
			if callbacks != 0 || owner.Bytes() == 0 {
				t.Fatalf("lost surviving alias callbacks=%d bytes=%d", callbacks, owner.Bytes())
			}
			seen := 0
			if err = clone.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, got StableLogicalObligation) error {
				seen++
				if got != o {
					t.Fatalf("obligation %#v", got)
				}
				return nil
			}); err != nil || seen != 1 {
				t.Fatalf("walk=%d err=%v", seen, err)
			}
			clone.Release()
			if callbacks != 1 || owner.Bytes() != 0 {
				t.Fatalf("last alias callbacks=%d bytes=%d", callbacks, owner.Bytes())
			}
			if _, err = ImportStableResourceSetMetadata(owner, source); !errors.Is(err, ErrResourceOwnership) {
				t.Fatal("released source imported")
			}
			live := stableTokenFixture(t, dir, "live", 1, 20, ReachabilityColumnManifest, "column")
			bb := NewStableResourceSetBuilder()
			if err = bb.Add(live); err != nil {
				t.Fatal(err)
			}
			ss, err := bb.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer ss.Release()
			closed := new(retainedalloc.Owner)
			closed.Initialize(0)
			closed.Close()
			if _, err = ImportStableResourceSetMetadata(closed, ss); !errors.Is(err, retainedalloc.ErrClosed) || closed.Bytes() != 0 || live.released.Load() {
				t.Fatalf("refusal err=%v bytes=%d released=%v", err, closed.Bytes(), live.released.Load())
			}
		})
	}
}

func TestOwnedImportedSetSharedIdentityCoalescingAndBoundedRefusal(t *testing.T) {
	callbacks := 0
	value := appendMutationTestObligation(1)
	value.Reachability = ReachabilityIndexFile
	token := stableTokenFixture(t, t.TempDir(), "shared", 1, 20, ReachabilityIndexFile, "shared", func(spec *StableResourceSpec) {
		spec.Kind = ResourceIndex
		spec.LogicalObligations = []StableLogicalObligation{value}
		spec.Frontier = NewRIDFrontier([]uint64{0, 7})
		spec.Frontier.Bytes = 20
		spec.OnRelease = func() { callbacks++ }
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.cap = baseline + 1024
	if _, err = ImportStableResourceSetMetadata(&owner, source); !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != baseline || callbacks != 0 || token.released.Load() {
		t.Fatalf("partial refusal err=%v bytes=%d baseline=%d callbacks=%d", err, owner.Bytes(), baseline, callbacks)
	}
	budget.cap = 1 << 20
	first, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ImportStableResourceSetMetadata(&owner, first)
	if err != nil {
		t.Fatal(err)
	}
	if second.kindViews != first.kindViews {
		t.Fatal("same-owner import rebuilt immutable metadata")
	}
	merge, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	defer merge.Abandon()
	if err = merge.Merge(first); err != nil {
		t.Fatal(err)
	}
	if err = merge.Merge(second); err != nil {
		t.Fatal(err)
	}
	result, err := merge.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	view := result.kindViews.get(ResourceIndex)
	if view.count != 1 || view.logicalObligationCount != 1 {
		t.Fatalf("duplicate union %+v", view)
	}
	view.root.rangeEntries(func(e *stableResourceEntry) bool {
		if !reflect.DeepEqual(e.frontier.RIDs(), []uint64{0, 7}) {
			t.Fatalf("RID union %+v", e.frontier)
		}
		return true
	})
	source.Release()
	first.Release()
	second.Release()
	if callbacks != 0 {
		t.Fatal("surviving result lost foreign custody")
	}
	result.Release()
	if callbacks != 1 || owner.Bytes() != baseline {
		t.Fatalf("completion callbacks=%d bytes=%d baseline=%d", callbacks, owner.Bytes(), baseline)
	}
}

// PRIMARY proof custody can transfer the imported Builder-owned set directly.
// Both selected-owner cases must retain the original foreign operations and
// logical obligations after every predecessor descriptor has been released.
func TestOwnedImportedProofDirectSelectedOwnerCustody(t *testing.T) {
	for _, sameOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "foreign-owner", true: "same-owner"}[sameOwner], func(t *testing.T) {
			releases := 0
			obligation := appendMutationTestObligation(1)
			token := stableTokenFixture(t, t.TempDir(), "proof", 1, 20, ReachabilityColumnManifest, "column", func(spec *StableResourceSpec) {
				spec.Kind = ResourceColumnAsset
				spec.LogicalObligations = []StableLogicalObligation{obligation}
				spec.OnRelease = func() { releases++ }
			})
			b := NewStableResourceSetBuilder()
			if err := b.Add(token); err != nil {
				t.Fatal(err)
			}
			generic, err := b.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			var originalOwner, foreignOwner retainedalloc.Owner
			selected, err := ImportStableResourceSetMetadata(&originalOwner, generic)
			if err != nil {
				t.Fatal(err)
			}
			proofOwner := &foreignOwner
			if sameOwner {
				proofOwner = &originalOwner
			}
			proof, err := ImportStableResourceSetMetadata(proofOwner, selected)
			if err != nil {
				t.Fatal(err)
			}
			if proof == selected || proof.Owner() != ResourceOwnerBuilder || proof.MetadataOwner() != proofOwner {
				t.Fatal("import failed to acquire independent selected ownership")
			}
			if err = generic.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if err = selected.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if releases != 0 || proofOwner.Bytes() == 0 {
				t.Fatal("predecessor release consumed proof custody")
			}
			seen := 0
			if err = proof.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, got StableLogicalObligation) error {
				seen++
				if got != obligation {
					t.Fatalf("changed obligation %#v", got)
				}
				return nil
			}); err != nil || seen != 1 {
				t.Fatalf("proof walk count=%d err=%v", seen, err)
			}
			if err = proof.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if releases != 1 || originalOwner.Bytes() != 0 || foreignOwner.Bytes() != 0 {
				t.Fatalf("last proof release callbacks=%d original=%d foreign=%d", releases, originalOwner.Bytes(), foreignOwner.Bytes())
			}
			if err = proof.ReleaseWithError(); err != nil || releases != 1 {
				t.Fatal("proof cleanup replayed")
			}
		})
	}
}

func TestOwnedImportedProofFitsOneWrapperBudgetAndRefusesSecond(t *testing.T) {
	token := stableTokenFixture(t, t.TempDir(), "budget-proof", 1, 20, ReachabilityIndexFile, "index")
	b := NewStableResourceSetBuilder()
	if err := b.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var owner retainedalloc.Owner
	budget := &registryBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	selected, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	// Only the independent set descriptor and its allocation header are new on
	// same-owner import. Its immutable kind backing remains independently held.
	wrapper := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSet{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceAllocation{})))
	before := owner.Bytes()
	budget.cap = budget.bytes + wrapper
	proof, err := ImportStableResourceSetMetadata(&owner, selected)
	if err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != before+wrapper || budget.bytes > budget.cap {
		t.Fatal("proof exceeded admitted containing storage")
	}
	if clone, err := CloneStableResourceSetExcludingKinds(proof); clone != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("second wrapper escaped cap: clone=%p err=%v", clone, err)
	}
	if owner.Bytes() != before+wrapper || proof.Owner() != ResourceOwnerBuilder {
		t.Fatal("refusal changed existing proof ownership/accounting")
	}
	if err = selected.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if err = proof.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != baseline {
		t.Fatalf("proof storage not refunded: got=%d baseline=%d", owner.Bytes(), baseline)
	}
}
