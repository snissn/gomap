package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDependencyDirectoryV2COWRemovalKeepsOldRoot(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	resources, selector := dbSelectorFixture(t)
	defer resources.Release()
	idx := database.idx.Load()
	database.rootReuseMu.Lock()
	bound, retired, err := database.prepareDependencyDirectoryV2(idx, 2, nil, resources)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	if len(retired) != 0 {
		t.Fatalf("initial root retired pages: %v", retired)
	}
	old, err := bound.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	selected, err := rootpublication.CloneStableResourceForSelector(bound, selector)
	if err != nil {
		t.Fatal(err)
	}
	selected.Release()
	removed, _, err := rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(bound, rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{selector.Obligation.Reachability},
		Removed:      []rootpublication.StableLogicalObligation{selector.Obligation},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer removed.Release()
	database.rootReuseMu.Lock()
	empty, retired, err := database.prepareDependencyDirectoryV2(idx, 3, bound, removed)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Release()
	current, err := empty.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	if current.Reference().LogicalCount != 0 || current.Reference().PhysicalCount != 0 || current.Reference().RootPageID == old.Reference().RootPageID || len(retired) == 0 {
		t.Fatalf("removal did not produce distinct empty COW root: old=%+v new=%+v retired=%v", old.Reference(), current.Reference(), retired)
	}
	if _, _, found, err := current.LookupLogical(selector.Obligation); err != nil || found {
		t.Fatalf("removed record survives: %t %v", found, err)
	}
	if _, got, found, err := old.LookupLogical(selector.Obligation); err != nil || !found || got != selector.Obligation {
		t.Fatalf("old root lost record: %t %v", found, err)
	}
	if err := current.Walk(func(_, _ []byte) error { t.Fatal("empty root contains a record"); return nil }); err != nil {
		t.Fatal(err)
	}
	if min := idx.registry.MinPinnedSeq(); min > 2 {
		t.Fatalf("old directory lease not registered: %d", min)
	}
}

func TestDependencyDirectoryV2InitialEmptyAndNoop(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	builder := rootpublication.NewStableResourceSetBuilder()
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	idx := database.idx.Load()
	database.rootReuseMu.Lock()
	bound, _, err := database.prepareDependencyDirectoryV2(idx, 2, nil, resources)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	first, err := bound.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Walk(func(_, _ []byte) error { t.Fatal("empty root contains a record"); return nil }); err != nil {
		t.Fatal(err)
	}
	database.rootReuseMu.Lock()
	next, retired, err := database.prepareDependencyDirectoryV2(idx, 3, bound, bound)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	second, err := next.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	if first.Reference() != second.Reference() || len(retired) != 0 {
		t.Fatalf("no-op rebuilt tree: %+v %+v retired=%v", first.Reference(), second.Reference(), retired)
	}
}
