package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"reflect"
	"testing"
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
	imported, err := newImportedResourceToken(owner, token, root, token.logicalLane, token.resourceID, token.diagnosticPath, token.frontier, token.reachability, token.logicalObligations)
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
	clone, err := CloneStableResourceSetExcludingKinds(imported)
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	imported.Release()
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
