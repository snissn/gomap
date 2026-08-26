package db

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureDurableRootAppendRequirementsStayMutationLocal4366(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	retained := make([]rootpublication.StableLogicalObligation, 4096)
	for i := range retained {
		retained[i] = durableRootClosureObligation3928(uint64(i + 1))
	}
	added := durableRootClosureObligation3928(4097)
	base := durableRootClosureSet3928(t, path, retained...)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, added)
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{added},
	}
	fallbackCalls := 0
	fallback := func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		fallbackCalls++
		return rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableResourceClosureWork{}, fmt.Errorf("unexpected exact fallback")
	}
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		mutation, rootpublication.StableResourceClosureWork{FinalRequirementRecordsDecoded: 1}, fallback, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	if fallbackCalls != 0 {
		t.Fatalf("exact fallback calls=%d want 0", fallbackCalls)
	}
	work := timing.FinalizeCandidateResourceWork
	if work.FinalRequirementProofFastPath != 1 || work.FinalRequirementProofFallbacks != 0 || work.FinalRequirementRecordsDecoded != 1 || work.FinalRequirementObligationsMaterialized != 0 {
		t.Fatalf("capture work=%+v want mutation-local final requirement proof", work)
	}
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 {
		t.Fatalf("candidate descriptors=%d want 1", len(descriptors))
	}
	if len(descriptors[0].LogicalObligations()) != len(retained)+1 {
		t.Fatalf("candidate obligations=%d want %d", len(descriptors[0].LogicalObligations()), len(retained)+1)
	}
	baseDescriptors := base.Descriptors()
	if len(baseDescriptors) != 1 {
		t.Fatalf("visible base descriptors=%d want 1", len(baseDescriptors))
	}
	if len(baseDescriptors[0].LogicalObligations()) != len(retained) {
		t.Fatalf("visible base obligations=%d want %d", len(baseDescriptors[0].LogicalObligations()), len(retained))
	}
}

func TestCaptureDurableRootAppendRequirementsFallbackIsLazyAndOneShot4366(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	baseObligation := durableRootClosureObligation3928(1)
	added := durableRootClosureObligation3928(2)
	base := durableRootClosureSet3928(t, path, baseObligation)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, added)
	requirements := durableRootClosureRequirements3928(t, baseObligation, added)
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{added},
	}
	fallbackCalls := 0
	fallback := func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		fallbackCalls++
		return requirements, rootpublication.StableResourceClosureWork{
			FinalRequirementRecordsDecoded:          2,
			FinalRequirementObligationsMaterialized: 2,
		}, nil
	}
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		// Any generic registration makes the specialized proof non-authoritative
		// and forces the exact oracle. Duplicate exact requirements are harmless.
		requirements, rootpublication.StableLogicalObligationMutation{},
		mutation, rootpublication.StableResourceClosureWork{FinalRequirementRecordsDecoded: 1}, fallback, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	if fallbackCalls != 1 {
		t.Fatalf("exact fallback calls=%d want 1", fallbackCalls)
	}
	work := timing.FinalizeCandidateResourceWork
	if work.FinalRequirementProofFastPath != 0 || work.FinalRequirementProofFallbacks != 1 || work.FinalRequirementRecordsDecoded != 3 || work.FinalRequirementObligationsMaterialized != 2 {
		t.Fatalf("capture work=%+v want one exact fallback", work)
	}
}

func TestCaptureDurableRootAppendRequirementsFallbackErrorPreservesOwnership4366(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	baseObligation := durableRootClosureObligation3928(1)
	added := durableRootClosureObligation3928(2)
	base := durableRootClosureSet3928(t, path, baseObligation)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, added)
	defer producer.Release()
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{added},
	}
	fallbackCalls := 0
	fallback := func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		fallbackCalls++
		return rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableResourceClosureWork{}, errors.New("decode failed")
	}
	_, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		durableRootClosureRequirements3928(t, baseObligation, added), rootpublication.StableLogicalObligationMutation{},
		mutation, rootpublication.StableResourceClosureWork{}, fallback, false, nil,
	)
	if err == nil || !strings.Contains(err.Error(), "decode failed") {
		t.Fatalf("capture error=%v want fallback decode failure", err)
	}
	if fallbackCalls != 1 {
		t.Fatalf("exact fallback calls=%d want 1", fallbackCalls)
	}
	if got := len(base.Descriptors()); got != 1 {
		t.Fatalf("base ownership changed after fallback failure: descriptors=%d", got)
	}
	if got := len(producer.Descriptors()); got != 1 {
		t.Fatalf("producer ownership changed after fallback failure: descriptors=%d", got)
	}
}

func TestCaptureDurableRootAppendRequirementsUseVisiblePredecessor4366(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	planTime := durableRootClosureObligation3928(1)
	queuedPredecessor := durableRootClosureObligation3928(2)
	added := durableRootClosureObligation3928(3)
	planBase := durableRootClosureSet3928(t, path, planTime)
	defer planBase.Release()
	predecessorProducer := durableRootClosureSet3928(t, path, queuedPredecessor)
	predecessorMutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{queuedPredecessor},
	}
	visibleBase, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, planBase, predecessorProducer,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		predecessorMutation, rootpublication.StableResourceClosureWork{}, func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
			return rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableResourceClosureWork{}, errors.New("unexpected predecessor fallback")
		}, false, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer visibleBase.Release()
	// The second capture base now includes an obligation published after this
	// append plan's earlier logical base. Certification must read the actual
	// visible predecessor, including its path-copied aggregate commitment.
	producer := durableRootClosureSet3928(t, path, added)
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{added},
	}
	fallbackCalls := 0
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, visibleBase, producer,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		mutation, rootpublication.StableResourceClosureWork{}, func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
			fallbackCalls++
			return rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableResourceClosureWork{}, errors.New("unexpected fallback")
		}, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	if fallbackCalls != 0 || timing.FinalizeCandidateResourceWork.FinalRequirementProofFastPath != 1 {
		t.Fatalf("visible predecessor proof fallback=%d work=%+v", fallbackCalls, timing.FinalizeCandidateResourceWork)
	}
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{planTime, queuedPredecessor, added}) {
		t.Fatalf("visible predecessor closure=%+v want exact queued base+addition", descriptors)
	}
}

func TestCaptureDurableRootAppendRequirementsProducerMismatchFallsBack4366(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	baseObligation := durableRootClosureObligation3928(1)
	announced := durableRootClosureObligation3928(2)
	unannounced := durableRootClosureObligation3928(3)
	base := durableRootClosureSet3928(t, path, baseObligation)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, announced, unannounced)
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{announced},
	}
	fallbackCalls := 0
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		mutation, rootpublication.StableResourceClosureWork{}, func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
			fallbackCalls++
			return durableRootClosureRequirements3928(t, baseObligation, announced, unannounced), rootpublication.StableResourceClosureWork{
				FinalRequirementRecordsDecoded:          3,
				FinalRequirementObligationsMaterialized: 3,
			}, nil
		}, false, &timing,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	work := timing.FinalizeCandidateResourceWork
	if fallbackCalls != 1 || work.FinalRequirementProofFastPath != 0 || work.FinalRequirementProofFallbacks != 1 || work.FullClosureValidations != 1 {
		t.Fatalf("producer mismatch fallback=%d work=%+v", fallbackCalls, work)
	}
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{baseObligation, announced, unannounced}) {
		t.Fatalf("producer mismatch closure=%+v want exact fallback result", descriptors)
	}
}

func TestCaptureDurableRootAppendRequirementAlreadyRetainedIsIdempotent4366(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	retained := durableRootClosureObligation3928(1)
	base := durableRootClosureSet3928(t, path, retained)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, retained)
	mutation := rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{retained},
	}
	fallbackCalls := 0
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, producer,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		mutation, rootpublication.StableResourceClosureWork{}, func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
			fallbackCalls++
			return rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableResourceClosureWork{}, errors.New("unexpected exact requirements fallback")
		}, false, &timing,
	)
	if err != nil {
		producer.Release()
		t.Fatal(err)
	}
	work := timing.FinalizeCandidateResourceWork
	if fallbackCalls != 0 || work.FinalRequirementProofFastPath != 1 || work.FinalRequirementProofFallbacks != 0 || work.FullClosureValidations != 0 {
		candidate.Release()
		t.Fatalf("retained addition fallback=%d work=%+v want certified set union", fallbackCalls, work)
	}
	if producer.Owner() != rootpublication.ResourceOwnerTransferred || base.Owner() != rootpublication.ResourceOwnerBuilder {
		candidate.Release()
		t.Fatalf("ownership producer=%v base=%v", producer.Owner(), base.Owner())
	}
	descriptors := candidate.Descriptors()
	if len(descriptors) != 1 || !slices.Equal(descriptors[0].LogicalObligations(), []rootpublication.StableLogicalObligation{retained}) {
		candidate.Release()
		t.Fatalf("retained addition closure=%+v want exact set union", descriptors)
	}
	candidate.Release()
	producer.Release()
	if base.Owner() != rootpublication.ResourceOwnerBuilder || len(base.Descriptors()) != 1 {
		t.Fatalf("candidate release changed visible base: owner=%v descriptors=%d", base.Owner(), len(base.Descriptors()))
	}
}

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

func TestCommandWALAppendRequirementRegistrationStaysSeparate4366(t *testing.T) {
	var genericMutation rootpublication.StableLogicalObligationMutation
	var appendMutation rootpublication.StableLogicalObligationMutation
	var work rootpublication.StableResourceClosureWork
	var fallback func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error)
	ctx := CommandWALPublishContext{
		durableResourceMutation:             &genericMutation,
		durableResourceAppendMutation:       &appendMutation,
		durableResourceRequirementWork:      &work,
		durableResourceRequirementsFallback: &fallback,
	}
	added := durableRootClosureObligation3928(1)
	registeredFallback := func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		return rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableResourceClosureWork{}, nil
	}
	if err := ctx.RegisterDurableLogicalObligationAppendMutation(rootpublication.StableLogicalObligationMutation{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
		Added:        []rootpublication.StableLogicalObligation{added},
	}, rootpublication.StableResourceClosureWork{FinalRequirementRecordsDecoded: 1}, registeredFallback); err != nil {
		t.Fatal(err)
	}
	if len(genericMutation.ScopedFields) != 0 || len(appendMutation.Added) != 1 || fallback == nil || work.FinalRequirementRecordsDecoded != 1 {
		t.Fatalf("registration generic=%+v append=%+v fallback_nil=%t work=%+v", genericMutation, appendMutation, fallback == nil, work)
	}
	if err := ctx.RegisterDurableLogicalObligationAppendMutation(appendMutation, rootpublication.StableResourceClosureWork{}, registeredFallback); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("duplicate specialized registration error=%v", err)
	}
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
			database.idx.Load(), database.meta, nil, base, producer, requirements, mutation,
			rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, &timing,
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
		}, rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, &timing,
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
		}, rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, &timing,
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
		}, rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, &timing,
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
		}, rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, &timing,
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
