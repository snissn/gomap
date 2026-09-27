package nativewire

import (
	"errors"
	"strings"
	"testing"
)

func TestOwnerGenerationSourceOpensOnlyBoundLocalDomainV2(t *testing.T) {
	fixture := newVectorPartitionLiveNativewireFixtureV1(t)
	defer fixture.database.Close()
	authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}
	source, err := NewCollectionVectorPartitionGenerationSourceForOwnerReplicatedLifecycleV2(fixture.collection, fixture.placement.Collection, authority, "group-a", fixture.manifest.IntegrityDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for attempt := 0; attempt < 2; attempt++ {
		pin, err := source.PinVectorPartitionGenerationV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation)
		if err != nil {
			t.Fatal(err)
		}
		local, err := pin.OpenPartition(t.Context(), 0)
		if err != nil {
			pin.Close()
			t.Fatalf("local domain open: %v", err)
		}
		if local.CacheHit != (attempt != 0) {
			t.Errorf("attempt %d cache hit=%v", attempt, local.CacheHit)
		}
		if err := local.Close(); err != nil {
			t.Error(err)
		}
		if remote, err := pin.OpenPartition(t.Context(), 2); !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) {
			if remote != nil {
				remote.Close()
			}
			t.Errorf("remote owner open err=%v", err)
		}
		if err := pin.Close(); err != nil {
			t.Error(err)
		}
	}
	if authority.calls != 2 {
		t.Errorf("cold/warm lifecycle checks=%d want 2", authority.calls)
	}
	key := collectionVectorPartitionGenerationKeyV1{index: fixture.definition.Name, generation: fixture.manifest.Generation}
	if source.entries[key].ownerPlan == nil || source.entries[key].openPlan != nil {
		t.Fatal("public source did not select the owner plan")
	}
}

func TestOwnerGenerationSourceRejectsDifferentStoredRootV2(t *testing.T) {
	fixture := newVectorPartitionLiveNativewireFixtureV1(t)
	defer fixture.database.Close()
	authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}
	wrongDigest := strings.Repeat("0", 64)
	if wrongDigest == fixture.manifest.IntegrityDigest {
		t.Fatal("fixture unexpectedly has zero root digest")
	}
	source, err := NewCollectionVectorPartitionGenerationSourceForOwnerReplicatedLifecycleV2(fixture.collection, fixture.placement.Collection, authority, "group-a", wrongDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if pin, err := source.PinVectorPartitionGenerationV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		if pin != nil {
			pin.Close()
		}
		t.Fatalf("different stored root err=%v", err)
	}
	if authority.calls != 0 {
		t.Errorf("different stored root reached lifecycle admission: %d", authority.calls)
	}
}

func TestOwnerGenerationSourcePreservesColdSourceVerificationV2(t *testing.T) {
	fixture := newVectorPartitionLiveNativewireFixtureV1(t)
	defer fixture.database.Close()
	insertVectorPartitionLiveDocumentV1(t, fixture.collection, "new-source-row", []float32{.4, .6})
	if _, err := fixture.collection.RebuildVectorIndex(fixture.definition.Name); err != nil {
		t.Fatal(err)
	}
	// This deliberately stale authority stub isolates local source verification.
	// Real replicated admission must invalidate before a relevant mutation.
	authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}
	source, err := NewCollectionVectorPartitionGenerationSourceForOwnerReplicatedLifecycleV2(fixture.collection, fixture.placement.Collection, authority, "group-a", fixture.manifest.IntegrityDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	pin, err := source.PinVectorPartitionGenerationV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation)
	if err != nil {
		// A refusal at the prepared-generation boundary is also fail-closed.
		if !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
			t.Fatal(err)
		}
		return
	}
	defer pin.Close()
	if local, err := pin.OpenPartition(t.Context(), 0); !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) {
		if local != nil {
			local.Close()
		}
		t.Fatalf("source drift was admitted by owner path: %v", err)
	}
}
