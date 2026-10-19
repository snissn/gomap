package db

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDependencyDirectoryV2OldReaderSlotsAndSharedSubtreeReclamation(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	idx := database.idx.Load()
	registry := database.StableResourceIdentityPinRegistry()
	if idx == nil || idx.primary != nil || idx.metadata == nil || idx.metadata != registry.MetadataOwner() {
		t.Fatal("not original DATA metadata owner")
	}
	file, err := os.CreateTemp(t.TempDir(), "directory-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	obligations := make([]rootpublication.StableLogicalObligation, 128)
	for i := range obligations {
		obligations[i] = rootpublication.StableLogicalObligation{Class: "asset", Kind: "part", Namespace: "directory-lifetime", Digest: [32]byte{1}, Generation: 1, PartID: uint64(i + 1), FileID: 1, Length: 8, Reachability: rootpublication.ReachabilityColumnManifest}
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{MetadataOwner: idx.metadata, PinRegistry: registry, OriginalObservation: registry, Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", Generation: 1, DiagnosticPath: "columns/1", File: file, Frontier: rootpublication.DurableFrontier{Bytes: 4096}, Reachability: rootpublication.ReachabilityColumnManifest, LogicalObligations: obligations, ContentSynced: true})
	if err != nil {
		t.Fatal(errors.Join(err, registry.Unobserve(identity)))
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(idx.metadata)
	if err != nil {
		t.Fatal(errors.Join(err, token.ReleaseWithError()))
	}
	if err := builder.Add(token); err != nil {
		t.Fatal(errors.Join(err, token.ReleaseWithError(), builder.AbandonWithError()))
	}
	initial, err := builder.Freeze()
	if err != nil {
		t.Fatal(errors.Join(err, builder.AbandonWithError()))
	}
	builder, token = nil, nil
	publish := func(resources *rootpublication.StableResourceSet, first bool) {
		t.Helper()
		database.durablePublishMu.Lock()
		defer database.durablePublishMu.Unlock()
		database.rootReuseMu.Lock()
		defer database.rootReuseMu.Unlock()
		next := database.meta
		next.CommitSeq++
		if first {
			bound, _, err := database.prepareDependencyDirectoryV2(idx, next.CommitSeq, nil, resources)
			resources.Release()
			if err != nil {
				t.Fatal(err)
			}
			resources = bound
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
	publish(initial, true)
	initial = nil
	old, err := rootpublication.CloneStableResourceSetExcludingKinds(database.durableRoot.slotResources[database.durableRoot.slot])
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if old != nil {
			if err := old.ReleaseWithError(); err != nil {
				t.Error(err)
			}
		}
	}()
	oldDirectory, err := old.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	oldPages := collectRootPageIDs(t, database, oldDirectory.Reference().RootPageID)
	removed, _, err := rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(old, rootpublication.StableLogicalObligationMutation{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest}, Removed: obligations[:1]})
	if err != nil {
		t.Fatal(err)
	}
	publish(removed, false)
	currentDirectory, err := database.durableRoot.slotResources[database.durableRoot.slot].DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	currentPages := make(map[uint64]bool)
	for _, id := range collectRootPageIDs(t, database, currentDirectory.Reference().RootPageID) {
		currentPages[id] = true
	}
	var retired, shared []uint64
	for _, id := range oldPages {
		if currentPages[id] {
			shared = append(shared, id)
		} else {
			retired = append(retired, id)
		}
	}
	if len(retired) == 0 || len(shared) == 0 {
		t.Fatalf("fixture lacks changed/shared paths: retired=%v shared=%v", retired, shared)
	}
	advance := func() {
		t.Helper()
		resources, err := rootpublication.CloneStableResourceSetExcludingKinds(database.durableRoot.slotResources[database.durableRoot.slot])
		if err != nil {
			t.Fatal(err)
		}
		publish(resources, false)
	}
	for i := 0; i < 8; i++ {
		advance()
		generation := idx.allocator.COWGenerationV1()
		for _, id := range oldPages {
			if generation.Allocatable(id) {
				t.Fatalf("old reader page %d became allocatable", id)
			}
		}
		if err := oldDirectory.Walk(nil); err != nil {
			t.Fatal(err)
		}
		for slot, resources := range database.durableRoot.slotResources {
			directory, err := resources.DependencyDirectoryV2()
			if err != nil || directory == nil {
				t.Fatalf("slot %d lost directory: %v", slot, err)
			}
			if err := directory.Walk(nil); err != nil {
				t.Fatalf("slot %d: %v", slot, err)
			}
		}
	}
	oldDirectory = nil
	err = old.ReleaseWithError()
	old = nil
	if err != nil {
		t.Fatal(err)
	}
	reclaimed := make(map[uint64]bool)
	for i := 0; i < 16; i++ {
		advance()
		generation := idx.allocator.COWGenerationV1()
		for _, id := range retired {
			if generation.Allocatable(id) {
				reclaimed[id] = true
			}
		}
		for _, extent := range generation.ReservationRecord().Entries() {
			if extent.Kind != freelist.ReservationTargetMetadata && extent.Kind != freelist.ReservationReusedData {
				continue
			}
			for _, id := range retired {
				if id >= extent.StartPageID && id-extent.StartPageID < uint64(extent.Count) {
					reclaimed[id] = true
				}
			}
		}
		for _, id := range shared {
			if generation.Allocatable(id) {
				t.Fatalf("shared live subtree page %d became allocatable", id)
			}
		}
		directory, err := database.durableRoot.slotResources[database.durableRoot.slot].DependencyDirectoryV2()
		if err != nil {
			t.Fatal(err)
		}
		if err := directory.Walk(nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range retired {
		if !reclaimed[id] {
			t.Fatalf("retired directory page %d never reclaimed after old reader and slots released", id)
		}
	}
}
