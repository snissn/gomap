package db

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestDependencyDirectoryV2SealRecoveryBothSlotsAndCorruptFallback(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	idx := database.idx.Load()
	publish := func(first bool) {
		t.Helper()
		database.durablePublishMu.Lock()
		defer database.durablePublishMu.Unlock()
		database.rootReuseMu.Lock()
		defer database.rootReuseMu.Unlock()
		next := database.meta
		next.CommitSeq = database.durableRoot.record.CommitSeq + 1
		var resources *rootpublication.StableResourceSet
		if first {
			resources, _, err = database.prepareDependencyDirectoryV2(idx, next.CommitSeq, nil, nil)
		} else {
			resources, err = rootpublication.CloneStableResourceSetExcludingKinds(database.durableRoot.slotResources[database.durableRoot.slot])
		}
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := database.prepareDurableRootCandidateV1(idx, next, nil, resources, false)
		if err != nil {
			t.Fatal(err)
		}
		published, err := database.executeDurableRootCandidateV1(candidate)
		if err != nil {
			t.Fatal(err)
		}
		database.meta = published
	}
	publish(true)
	publish(false)
	selectRoot := func() (durableRootSelectionV1, error) {
		return selectDurableRootV1(idx.pager, idx.pager.PageCount(), database.validateDurableDependencyManifestV1, database.dependencyDirectoryValidatorV2(idx))
	}
	selected, err := selectRoot()
	if err != nil {
		t.Fatal(err)
	}
	for slot, resources := range selected.SlotResources {
		directory, err := resources.DependencyDirectoryV2()
		if err != nil || directory == nil || selected.SlotRecords[slot].Directory != directory.Reference() {
			t.Fatalf("slot %d directory lease: %v %v", slot, directory, err)
		}
		resources.Release()
	}
	if _, err := selectDurableRootV1(idx.pager, idx.pager.PageCount(), nil); err == nil {
		t.Fatal("directory recovered without validator")
	}
	newest := database.durableRoot.meta.RootRecordPageID
	image, err := idx.pager.ReadPage(newest)
	if err != nil {
		t.Fatal(err)
	}
	image = bytes.Clone(image)
	broken := bytes.Clone(image)
	broken[len(broken)-1] ^= 1
	if err := idx.pager.Write(newest, broken); err != nil {
		t.Fatal(err)
	}
	fallback, err := selectRoot()
	if err != nil {
		t.Fatal(err)
	}
	if fallback.Record.CommitSeq != database.durableRoot.record.CommitSeq-1 {
		t.Fatalf("fallback selected %d", fallback.Record.CommitSeq)
	}
	for _, resources := range fallback.SlotResources {
		resources.Release()
	}
	if err := idx.pager.Write(newest, image); err != nil {
		t.Fatal(err)
	}
	directoryPage := database.durableRoot.record.Directory.RootPageID
	directoryImage, err := idx.pager.ReadPage(directoryPage)
	if err != nil {
		t.Fatal(err)
	}
	directoryImage = bytes.Clone(directoryImage)
	broken = bytes.Clone(directoryImage)
	broken[len(broken)-1] ^= 1
	if err := idx.pager.Write(directoryPage, broken); err != nil {
		t.Fatal(err)
	}
	if _, err := selectRoot(); err == nil {
		t.Fatal("both slots admitted corrupt shared directory")
	}
	if err := idx.pager.Write(directoryPage, directoryImage); err != nil {
		t.Fatal(err)
	}
}

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
	beforeBytes, beforePages := database.durableRootDirectoryBytesEncoded.Load(), database.durableRootDirectoryPagesWritten.Load()
	next, retired, err := database.prepareDependencyDirectoryV2(idx, 3, bound, bound)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	if database.durableRootDirectoryBytesEncoded.Load() != beforeBytes || database.durableRootDirectoryPagesWritten.Load() != beforePages {
		t.Fatal("directory no-op charged changed records or COW pages")
	}
	second, err := next.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	if first.Reference() != second.Reference() || len(retired) != 0 {
		t.Fatalf("no-op rebuilt tree: %+v %+v retired=%v", first.Reference(), second.Reference(), retired)
	}
}

func TestDependencyDirectoryV2RebuildPreservesBothSlots(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.rootReuseMu.Lock()
	bound, _, err := database.prepareDependencyDirectoryV2(database.idx.Load(), 2, nil, nil)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	p, err := pager.Open(path, 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.GrowTo(4); err != nil {
		t.Fatal(err)
	}
	for id := uint64(2); id < 4; id++ {
		image := make([]byte, page.PageSize)
		n := node.NewNode(image)
		n.SetPageID(id)
		n.SetType(page.PageTypeLeaf)
		n.UpdateChecksum()
		if err := p.Write(id, image); err != nil {
			t.Fatal(err)
		}
	}
	older := page.MetaPageBody{CommitSeq: 2, UserRootPageID: 2, SystemRootPageID: 3}
	newer := older
	newer.CommitSeq = 3
	if err := writeRebuiltDurableRootsV1(dir, path, p, []rebuiltDurableRootV1{{meta: older, resources: bound}, {meta: newer, resources: bound}}); err != nil {
		t.Fatal(err)
	}
	selected, err := selectDurableRootV1(p, p.PageCount(), nil, dependencyDirectoryStructureValidatorV2(p))
	if err != nil {
		t.Fatal(err)
	}
	if selected.Record.CommitSeq != 3 || selected.Record.ParentCommitSeq != 2 {
		t.Fatalf("rebuilt lineage=%+v", selected.Record)
	}
	for slot, record := range selected.SlotRecords {
		if record.CommitSeq == 0 || record.Directory.RootPageID == 0 || record.Manifest != (rootpublication.DependencyManifestRefV1{}) {
			t.Fatalf("rebuilt slot %d lost directory: %+v", slot, record)
		}
	}
	if selected.SlotRecords[0].Directory.RootPageID == selected.SlotRecords[1].Directory.RootPageID {
		t.Fatal("rebuilt slots did not independently own copied directory roots")
	}
}
