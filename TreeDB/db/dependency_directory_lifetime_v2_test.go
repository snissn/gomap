package db

import (
	"errors"
	"math"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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
	// A distinct physical producer can append to a retained owned directory.
	// Removing its last pending member before binding must not resurrect it
	// through kind membership, logical walking, or directory serialization.
	t.Run("RemoveLastDistinctPhysicalPendingMember", func(t *testing.T) {
		pendingFile, err := os.CreateTemp(t.TempDir(), "pending-owner")
		if err != nil {
			t.Fatal(err)
		}
		defer pendingFile.Close()
		if err := pendingFile.Truncate(4096); err != nil {
			t.Fatal(err)
		}
		added := obligations[0]
		added.PartID, added.FileID = 129, 2
		identity, err := rootpublication.StableIdentityFromFile(pendingFile)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Observe(identity); err != nil {
			t.Fatal(err)
		}
		token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{MetadataOwner: idx.metadata, PinRegistry: registry, OriginalObservation: registry, Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "2", Generation: 1, DiagnosticPath: "columns/2", File: pendingFile, Frontier: rootpublication.DurableFrontier{Bytes: 4096}, Reachability: rootpublication.ReachabilityColumnManifest, LogicalObligations: []rootpublication.StableLogicalObligation{added}, ContentSynced: true})
		if err != nil {
			t.Fatal(errors.Join(err, registry.Unobserve(identity)))
		}
		defer func() {
			if token != nil {
				if err := token.ReleaseWithError(); err != nil {
					t.Error(err)
				}
			}
		}()
		producerBuilder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(idx.metadata)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if producerBuilder != nil {
				if err := producerBuilder.AbandonWithError(); err != nil {
					t.Error(err)
				}
			}
		}()
		if err := producerBuilder.Add(token); err != nil {
			t.Fatal(err)
		}
		token = nil
		producer, err := producerBuilder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		producerBuilder = nil
		inherited, err := rootpublication.CloneStableResourceSetExcludingKinds(old)
		if err != nil {
			t.Fatal(errors.Join(err, producer.ReleaseWithError()))
		}
		var mixed, restored *rootpublication.StableResourceSet
		defer func() {
			for _, resources := range []*rootpublication.StableResourceSet{producer, inherited, mixed, restored} {
				if resources != nil {
					if err := resources.ReleaseWithError(); err != nil {
						t.Error(err)
					}
				}
			}
		}()
		merge, err := rootpublication.NewStableResourceSetBuilderWithMetadata(idx.metadata)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if merge != nil {
				if err := merge.AbandonWithError(); err != nil {
					t.Error(err)
				}
			}
		}()
		if err := merge.Merge(inherited); err != nil {
			t.Fatal(err)
		}
		inherited = nil
		mutation := rootpublication.StableLogicalObligationMutation{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest}, Added: []rootpublication.StableLogicalObligation{added}}
		if _, err := merge.MergeAppendOnlyLogicalObligations(producer, mutation); err != nil {
			t.Fatal(err)
		}
		producer = nil
		mixed, err = merge.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		merge = nil
		seen := 0
		if err := mixed.WalkLogicalObligations(func(_ rootpublication.StableResourcePhysicalDescriptor, o rootpublication.StableLogicalObligation) error {
			if o == added {
				seen++
			}
			return nil
		}); err != nil || seen != 1 {
			t.Fatalf("pending producer absent before removal: seen=%d err=%v", seen, err)
		}
		mutation.Added, mutation.Removed = nil, []rootpublication.StableLogicalObligation{added}
		restored, _, err = rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(mixed, mutation)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		if err := restored.WalkLogicalObligations(func(_ rootpublication.StableResourcePhysicalDescriptor, o rootpublication.StableLogicalObligation) error {
			if o == added {
				t.Error("removed pending member reappeared in walk")
			}
			count++
			return nil
		}); err != nil || count != len(obligations) {
			t.Fatalf("restored logical membership: count=%d err=%v", count, err)
		}
		missing, _, err := rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(restored, mutation)
		if missing != nil || !errors.Is(err, rootpublication.ErrUnresolvedResource) {
			if missing != nil {
				err = errors.Join(err, missing.ReleaseWithError())
			}
			t.Fatalf("removed pending member remained in membership: %v", err)
		}
		changes := 0
		physical, logical, err := rootpublication.WalkDependencyDirectoryChangesV2(restored, oldDirectory, func(_, _ []byte, _ bool) error { changes++; return nil })
		if err != nil || physical != 1 || logical != uint64(len(obligations)) || changes != 0 {
			t.Fatalf("removed pending member reached serializer: physical=%d logical=%d changes=%d err=%v", physical, logical, changes, err)
		}
		_, got, found, err := oldDirectory.LookupLogical(obligations[0])
		if err != nil || !found || got != obligations[0] {
			t.Fatalf("original root changed: found=%t err=%v", found, err)
		}
		_, _, found, err = oldDirectory.LookupLogical(added)
		if err != nil || found {
			t.Fatalf("pending member entered original root: found=%t err=%v", found, err)
		}
	})
	mutation := rootpublication.StableLogicalObligationMutation{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest}, Removed: obligations[:1]}
	refusal := &originalClosureAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := idx.metadata.Enroll(refusal)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before, pins, oldOwner := idx.metadata.Bytes(), registry.ActivePins(), old.Owner()
	refusal.setLimit(before)
	refused, _, refusalErr := rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(old, mutation)
	refusal.setLimit(math.MaxUint64)
	if refused != nil || !errors.Is(refusalErr, retainedalloc.ErrCapacity) || idx.metadata.Bytes() != before || registry.ActivePins() != pins || old.Owner() != oldOwner {
		t.Fatalf("removal refusal changed original source: result=%p err=%v bytes=%d want=%d pins=%d want=%d", refused, refusalErr, idx.metadata.Bytes(), before, registry.ActivePins(), pins)
	}
	removed, work, err := rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(old, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if work.SourceObligationsInspected != 0 || work.CopiedObligations != 0 || work.AggregateMembershipProbes != 1 || work.DroppedObligations != 1 {
		t.Fatalf("fixed removal inspected retained history: %+v", work)
	}
	// A second private removal must retain the first delta without flattening
	// either the old directory or its 127 still-visible records.
	pending, pendingWork, err := rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(removed, rootpublication.StableLogicalObligationMutation{ScopedFields: mutation.ScopedFields, Removed: obligations[1:2]})
	if err != nil {
		t.Fatal(err)
	}
	if pendingWork.SourceObligationsInspected != 0 || pendingWork.AggregateMembershipProbes != 1 || pendingWork.DroppedObligations != 1 {
		t.Fatalf("pending removal scanned retained history: %+v", pendingWork)
	}
	if err := pending.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	pending = nil
	publish(removed, false)
	removed = nil
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
