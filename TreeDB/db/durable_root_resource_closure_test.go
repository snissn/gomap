package db

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func durableRootClosureObligation3928(partID uint64) rootpublication.StableLogicalObligation {
	obligation := rootpublication.StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
		Generation: 1, PartID: partID, FileID: 1, Offset: int64(partID * 16), Length: 16,
		Checksum: uint32(partID), Reachability: rootpublication.ReachabilityColumnManifest,
	}
	obligation.Digest = sha256.Sum256([]byte{byte(partID)})
	return obligation
}

func durableRootClosureRequirements3928(t *testing.T, obligations ...rootpublication.StableLogicalObligation) rootpublication.StableLogicalObligationRequirements {
	t.Helper()
	requirements, err := rootpublication.NormalizeStableLogicalObligationRequirements(rootpublication.StableLogicalObligationRequirements{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Obligations:  obligations,
	})
	if err != nil {
		t.Fatal(err)
	}
	return requirements
}

func durableRootClosureSet3928(t *testing.T, path string, obligations ...rootpublication.StableLogicalObligation) *rootpublication.StableResourceSet {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "shared-segment",
		Generation: 1, DiagnosticPath: filepath.Base(path), File: file, Frontier: rootpublication.DurableFrontier{Bytes: 4096},
		Digest: sha256.Sum256([]byte("stable-shared-column-segment")), Reachability: rootpublication.ReachabilityColumnManifest,
		LogicalObligations: obligations, ContentSynced: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	return set
}

func openDurableRootClosureDB3928(t *testing.T) (*DB, string) {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	path := filepath.Join(t.TempDir(), "shared-column-segment.bin")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(4096); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return database, path
}

func TestCaptureDurableRootAppendMutationDiscardRetryPreservesBase3928(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	baseObligation := durableRootClosureObligation3928(1)
	added := durableRootClosureObligation3928(2)
	base := durableRootClosureSet3928(t, path, baseObligation)
	defer base.Release()
	requirements := durableRootClosureRequirements3928(t, baseObligation, added)
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{added},
	}

	for attempt := 1; attempt <= 2; attempt++ {
		producer := durableRootClosureSet3928(t, path, added)
		var timing CommandWALPublishTiming
		candidate, err := database.captureDurableRootResourcesFromBaseV1(
			database.idx.Load(), database.meta, nil, base, producer, requirements, mutation, false, &timing,
		)
		if err != nil {
			t.Fatalf("capture attempt %d: %v", attempt, err)
		}
		work := timing.FinalizeCandidateResourceWork
		if work.AppendOnlyFastPath != 1 || work.AppendOnlyFallbacks != 0 || work.FullClosureValidations != 0 {
			candidate.Release()
			t.Fatalf("capture attempt %d work=%+v want certified append-only path", attempt, work)
		}
		descriptors := candidate.Descriptors()
		if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{baseObligation, added}) {
			candidate.Release()
			t.Fatalf("capture attempt %d closure=%+v want exact base+addition", attempt, descriptors)
		}
		// Discard the first prepared closure as a stale/pre-publication failure;
		// the second capture must be legal from the unchanged visible base.
		candidate.Release()
		baseDescriptors := base.Descriptors()
		if len(baseDescriptors) != 1 || !slices.Equal(baseDescriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{baseObligation}) {
			t.Fatalf("base changed after discarded attempt %d: %+v", attempt, baseDescriptors)
		}
	}
}

func TestCaptureDurableRootDestructiveMutationUsesFullFallback3928(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	keep := durableRootClosureObligation3928(1)
	remove := durableRootClosureObligation3928(2)
	base := durableRootClosureSet3928(t, path, keep, remove)
	defer base.Release()
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, nil,
		durableRootClosureRequirements3928(t, keep),
		rootpublication.StableLogicalObligationMutation{
			ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
			Removed:      []rootpublication.StableLogicalObligation{remove},
		}, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	work := timing.FinalizeCandidateResourceWork
	if work.AppendOnlyFastPath != 0 || work.DestructiveFallbacks != 1 || work.FullClosureValidations != 1 || work.RemovedObligations != 1 || work.SourceObligationsInspected == 0 {
		t.Fatalf("destructive work=%+v want measured full fallback", work)
	}
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{keep}) {
		t.Fatalf("destructive closure=%+v want retained obligation only", descriptors)
	}
}

func TestCaptureDurableRootMixedProducerFallsBackAndValidates3928(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	baseObligation := durableRootClosureObligation3928(1)
	announced := durableRootClosureObligation3928(2)
	unannounced := durableRootClosureObligation3928(3)
	base := durableRootClosureSet3928(t, path, baseObligation)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, announced, unannounced)
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		durableRootClosureRequirements3928(t, baseObligation, announced, unannounced),
		rootpublication.StableLogicalObligationMutation{
			ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
			Added:        []rootpublication.StableLogicalObligation{announced},
		}, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	work := timing.FinalizeCandidateResourceWork
	if work.AppendOnlyFastPath != 0 || work.AppendOnlyFallbacks != 1 || work.FullClosureValidations != 1 {
		t.Fatalf("mixed producer work=%+v want fail-closed generic fallback", work)
	}
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{baseObligation, announced, unannounced}) {
		t.Fatalf("mixed producer closure=%+v want exact validated union", descriptors)
	}
}

func TestCaptureDurableRootAppendMutationOmittedRemovalFallsBack3928(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	keep := durableRootClosureObligation3928(1)
	omittedRemoval := durableRootClosureObligation3928(2)
	added := durableRootClosureObligation3928(3)
	base := durableRootClosureSet3928(t, path, keep, omittedRemoval)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, added)
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		durableRootClosureRequirements3928(t, keep, added),
		rootpublication.StableLogicalObligationMutation{
			ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
			Added:        []rootpublication.StableLogicalObligation{added},
			// omittedRemoval is intentionally absent. The mutation must not
			// authorize retaining a stale pin that the final requirements omit.
		}, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{keep, added}) {
		t.Fatalf("incomplete append mutation closure=%+v want exact fallback result", descriptors)
	}
	work := timing.FinalizeCandidateResourceWork
	if work.AppendOnlyFastPath != 0 || work.AppendOnlyFallbacks != 1 || work.FullClosureValidations != 1 {
		t.Fatalf("incomplete append mutation work=%+v want exact validated fallback", work)
	}
}

func TestCaptureDurableRootEmptyMutationCannotRetainStaleObligation3928(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	keep := durableRootClosureObligation3928(1)
	omittedRemoval := durableRootClosureObligation3928(2)
	base := durableRootClosureSet3928(t, path, keep, omittedRemoval)
	defer base.Release()
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, nil,
		durableRootClosureRequirements3928(t, keep),
		rootpublication.StableLogicalObligationMutation{
			ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		}, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{keep}) {
		t.Fatalf("empty mutation closure=%+v want stale obligation filtered", descriptors)
	}
	work := timing.FinalizeCandidateResourceWork
	if work.AppendOnlyFastPath != 0 || work.AppendOnlyFallbacks != 1 || work.FullClosureValidations != 1 {
		t.Fatalf("empty incomplete mutation work=%+v want exact validated fallback", work)
	}
}
