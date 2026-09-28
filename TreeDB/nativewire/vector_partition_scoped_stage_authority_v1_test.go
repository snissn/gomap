package nativewire

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestFixedPeerVectorScopedStageAuthorityFencesCatalogAndRouterOnlyV1(t *testing.T) {
	authority, provider := newNativewireCatalogMetaAuthorityWithLifecycle(t, true)
	proof, err := authority.CurrentCatalogProof(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	manifest := collections.VectorPartitionManifestV1{
		State: "ready", Collection: "users", IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 1),
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, Generation: 7,
		PartitionCount: 2, Placements: []collections.VectorPartitionPlacementV1{
			{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"},
		},
	}
	placement, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	scope := collections.VectorPartitionLocalScopeV1{
		HostedGroup: "meta", Router: true,
		ManifestDigest: fmt.Sprintf("%064x", 2), PlacementDigest: placement,
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection:            raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"},
			CollectionIncarnation: 1, IndexName: manifest.IndexName, IndexDefinitionDigest: manifest.IndexDefinitionDigest,
			IndexEpoch: 1, CatalogEpoch: proof.Epoch, CatalogDigest: proof.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum,
			SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount,
		},
		Generation: manifest.Generation,
		Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
			ManifestDigest: scope.ManifestDigest, PlacementDigest: scope.PlacementDigest,
		},
	}
	coordinator := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: provider}
	coordinator.PrepareImmutableV1 = func(context.Context, raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error) {
		return identity.Immutable, []raftcluster.GroupID{"group-a", "group-b"}, nil
	}
	mutationEpoch, err := coordinator.BuildSourceMutationEpochV1(identity.Index.Collection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-a", "group-b"}, 0, mutationEpoch); err != nil {
		t.Fatal(err)
	}
	status, ok := authority.Status()
	if !ok {
		t.Fatal("catalog status unavailable")
	}
	runtime := &FixedPeerTCPRuntimeV1{
		authority: authority, meta: provider,
		config: FixedPeerTCPConfigV1{
			Catalog: FixedPeerTCPGroupV1{ID: "meta"},
			Vector:  &FixedPeerTCPVectorConfigV1{Identity: identity}, RequestTimeout: time.Second,
		},
	}
	newAdapter := func(fence func(context.Context) (raftplacement.CatalogMetaStatusV1, error)) *fixedPeerVectorScopedStageAuthorityV1 {
		adapter, err := newFixedPeerVectorScopedStageAuthorityV1(runtime, "meta")
		if err != nil || !adapter.routerOnly {
			t.Fatalf("catalog-only constructor adapter=%+v err=%v", adapter, err)
		}
		adapter.fence = fence
		return adapter
	}
	stableFence := func(context.Context) (raftplacement.CatalogMetaStatusV1, error) { return status, nil }
	if err := newAdapter(stableFence).ValidateVectorPartitionScopedStageV1(t.Context(), manifest, scope); err != nil {
		t.Fatalf("catalog-only router preparation: %v", err)
	}
	withoutRouter := scope
	withoutRouter.Router = false
	if err := newAdapter(stableFence).ValidateVectorPartitionScopedStageV1(t.Context(), manifest, withoutRouter); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("catalog-only host claimed no-router inventory: %v", err)
	}
	foreignClaim := manifest
	foreignClaim.Placements = append([]collections.VectorPartitionPlacementV1(nil), manifest.Placements...)
	foreignClaim.Placements[0].GroupID = "meta"
	if err := newAdapter(stableFence).ValidateVectorPartitionScopedStageV1(t.Context(), foreignClaim, scope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("catalog-only host claimed data placement: %v", err)
	}
	calls := 0
	advancingFence := func(context.Context) (raftplacement.CatalogMetaStatusV1, error) {
		calls++
		moved := status
		if calls == 2 {
			moved.AppliedIndex++
		}
		return moved, nil
	}
	if err := newAdapter(advancingFence).ValidateVectorPartitionScopedStageV1(t.Context(), manifest, scope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) || calls != 2 {
		t.Fatalf("moved applied index err=%v calls=%d", err, calls)
	}
	calls = 0
	crossCallFence := func(context.Context) (raftplacement.CatalogMetaStatusV1, error) {
		calls++
		moved := status
		if calls > 2 {
			moved.AppliedIndex++
		}
		return moved, nil
	}
	stage := newAdapter(crossCallFence)
	for i := 0; i < 2; i++ {
		if err := stage.ValidateVectorPartitionScopedStageV1(t.Context(), manifest, scope); err != nil {
			t.Fatalf("unrelated catalog apply during Stage check %d: %v", i+1, err)
		}
	}
	if calls != 4 {
		t.Fatalf("cross-call fences=%d want 4", calls)
	}
	for _, group := range []raftcluster.GroupID{"group-a", "group-b"} {
		if _, err := coordinator.RecordGroupReadyV1(t.Context(), identity, raftplacement.VectorPartitionLifecycleGroupReadyV1{
			GroupID: group, AppliedIndex: 1, AssetSetDigest: fmt.Sprintf("%064x", 3),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coordinator.PrepareV1(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.ActivateV1(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	status, ok = authority.Status()
	if !ok {
		t.Fatal("active catalog status unavailable")
	}
	if err := newAdapter(stableFence).ValidateVectorPartitionScopedStageV1(t.Context(), manifest, scope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("late Stage after ACTIVE err=%v", err)
	}
	ownerScope := scope
	ownerScope.HostedGroup = "group-a"
	ownerScope.Router = false
	owner := &fixedPeerVectorScopedOwnerAuthorityV1{
		authority: authority, fence: stableFence, identity: identity, hosted: "group-a",
		owners: []raftcluster.GroupID{"group-a", "group-b"}, readyDigest: fmt.Sprintf("%064x", 3),
	}
	if err := owner.ValidateVectorPartitionScopedOwnerPairV1(t.Context(), ownerScope); err != nil {
		t.Fatalf("cached ACTIVE owner pair: %v", err)
	}
	owner.readyDigest = fmt.Sprintf("%064x", 4)
	if err := owner.ValidateVectorPartitionScopedOwnerPairV1(t.Context(), ownerScope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("changed ready receipt accepted: %v", err)
	}
	owner.readyDigest = fmt.Sprintf("%064x", 3)
	owner.owners = []raftcluster.GroupID{"group-a"}
	if err := owner.ValidateVectorPartitionScopedOwnerPairV1(t.Context(), ownerScope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("changed owner set accepted: %v", err)
	}
	owner.owners = []raftcluster.GroupID{"group-a", "group-b"}
	owner.fence = advancingFence
	calls = 0
	if err := owner.ValidateVectorPartitionScopedOwnerPairV1(t.Context(), ownerScope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) || calls != 2 {
		t.Fatalf("changed catalog index accepted: err=%v calls=%d", err, calls)
	}
	if _, err := coordinator.InvalidateGenerationBeforeRelevantMutationV1(t.Context(), identity, "owner generation changed"); err != nil {
		t.Fatal(err)
	}
	status, ok = authority.Status()
	if !ok {
		t.Fatal("invalidated catalog status unavailable")
	}
	owner.fence = stableFence
	if err := owner.ValidateVectorPartitionScopedOwnerPairV1(t.Context(), ownerScope); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("invalidated owner remained cached: %v", err)
	}
}
