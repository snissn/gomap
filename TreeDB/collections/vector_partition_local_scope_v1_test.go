package collections

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func scopedLifecycleManifestsV1(t *testing.T) (VectorPartitionManifestV1, VectorPartitionManifestV1, VectorPartitionLocalScopeV1) {
	t.Helper()
	ready := testVectorPartitionManifestV1()
	ready.DomainCount = 2
	ready.DomainPacks = []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}}
	ready.Placements[1].GroupID = "raft-b"
	ready.Canonicalize()
	readyRaw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	building := cloneVectorPartitionManifestForCheckpointV1(ready)
	building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	building.Canonicalize()
	if _, err := EncodeVectorPartitionManifestV1(building); err != nil {
		t.Fatal(err)
	}
	scope := VectorPartitionLocalScopeV1{
		HostedGroup: "raft-a", Router: true,
		ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(readyRaw)), PlacementDigest: strings.Repeat("b", 64),
	}
	return building, ready, scope
}

func TestVectorPartitionLocalScopeV1PreservesLegacyCheckpointBytes(t *testing.T) {
	checkpoint := lifecycleCheckpointV1(t, lifecycleLegalChainV1(t)[:5], 7)
	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if version := binary.BigEndian.Uint32(raw[4:8]); version != vectorPartitionLifecycleCheckpointVersionV1 {
		t.Fatalf("ordinary checkpoint version=%d want VCP1", version)
	}
	// Golden captured from the unchanged 20afb73 collections encoder with
	// the same lifecycleLegalChainV1 fixture, before the scoped VCP2 change.
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != "9e541c3b55b9f0428709ed0fc0af96fd391383658dc0888e9e478cfb4533346d" {
		t.Fatalf("ordinary VCP1 bytes changed: %s", got)
	}
}

func TestVectorPartitionLocalScopeV1ScopedBuildReopenAndCheckpoint(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	building, ready, scope := scopedLifecycleManifestsV1(t)
	assets, err := scope.localAssetsV1(ready)
	if err != nil || len(assets) != 2 || assets[0].PartitionID != 0 || assets[1].ID != "router" {
		t.Fatalf("local assets=%+v err=%v", assets, err)
	}
	foreign := scope
	foreign.HostedGroup, foreign.Router = "raft-b", false
	foreignAssets, err := foreign.localAssetsV1(ready)
	if err != nil || len(foreignAssets) != 1 || foreignAssets[0].PartitionID != 1 {
		t.Fatalf("foreign owner assets=%+v err=%v", foreignAssets, err)
	}
	payload, err := encodeVectorPartitionScopedBuildPayloadV1(building, scope)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleScopedBuildV1, building.Generation, payload); errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Skipf("checkpoint publication unsupported: %v", err)
	} else if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.state.Generations[building.Generation].Scope; got == nil || *got != scope {
		t.Fatalf("scoped build lost descriptor: %+v", got)
	}
	checkpointRaw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(loaded.checkpoint)
	if err != nil || binary.BigEndian.Uint32(checkpointRaw[4:8]) != vectorPartitionLifecycleCheckpointScopedVersionV1 {
		t.Fatalf("scoped checkpoint version=%d err=%v", binary.BigEndian.Uint32(checkpointRaw[4:8]), err)
	}
	promotion, err := makeVectorPartitionReadyPromotionPayloadV1(building, ready)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, ready.Generation, promotion); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	entry := again.state.Generations[ready.Generation]
	if entry.Manifest == nil || entry.Manifest.State != "ready" || entry.Scope == nil || *entry.Scope != scope {
		t.Fatalf("reopened scoped generation=%+v", entry)
	}
	refs, err := vectorPartitionGenerationRefsV1(entry)
	if err != nil || len(refs) != 2 {
		t.Fatalf("local reclaim refs=%+v err=%v", refs, err)
	}
	if err := ValidateVectorPartitionSnapshotNamespaceV1(root); !errors.Is(err, ErrVectorPartitionManifestInvalid) || !strings.Contains(err.Error(), "partition/0") {
		t.Fatalf("snapshot accepted missing hosted asset or checked wrong asset: %v", err)
	}
}

func TestVectorPartitionLocalScopeV1RejectsChangedManifestAndForeignSegment(t *testing.T) {
	building, ready, scope := scopedLifecycleManifestsV1(t)
	if _, err := scope.localAssetsV1(testVectorPartitionPagedRootV2(t)); !errors.Is(err, ErrVectorPartitionPagedRuntimeUnsupportedV2) {
		t.Fatalf("paged root local assets err=%v", err)
	}
	scopeRaw, err := encodeVectorPartitionLocalScopeV1(scope)
	if err != nil {
		t.Fatal(err)
	}
	broken := append([]byte(nil), scopeRaw...)
	broken[6+len(scope.HostedGroup)] = 2
	if _, err := decodeVectorPartitionLocalScopeV1(broken); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("invalid scope flag err=%v", err)
	}
	changed := cloneVectorPartitionManifestForCheckpointV1(ready)
	changed.Placements[1].GroupID = "raft-a"
	changed.Canonicalize()
	if err := scope.validateManifestV1(changed); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("changed ready manifest err=%v", err)
	}
	building.Assets[1].Ref.FileID = building.Assets[0].Ref.FileID
	building.Canonicalize()
	if _, err := scope.localAssetsV1(building); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("shared foreign segment err=%v", err)
	}
}

func TestVectorPartitionLocalScopeV1DeleteOnlyLocalRefs(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	building, _, scope := scopedLifecycleManifestsV1(t)
	payload, err := encodeVectorPartitionScopedBuildPayloadV1(building, scope)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleScopedBuildV1, building.Generation, payload); err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleDeleteCompleteV1, building.Generation, nil); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("direct complete with hosted refs err=%v", err)
	}
	if err := deleteVectorPartitionStoreForTest(store, "docs", "embedding", building.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	reclaim := loaded.state.Generations[building.Generation].Reclaim
	if reclaim == nil || len(reclaim.OriginalRefs) != 1 || reclaim.OriginalRefs[0] != building.Assets[0].Ref {
		t.Fatalf("scoped reclaim includes foreign refs: %+v", reclaim)
	}
}

func TestVectorPartitionLocalScopeV1EmptyRouterBuildDeleteSurvivesReopen(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	building, _, scope := scopedLifecycleManifestsV1(t)
	scope.HostedGroup = "ingress"
	if assets, err := scope.localAssetsV1(building); err != nil || len(assets) != 0 {
		t.Fatalf("router-only build assets=%+v err=%v", assets, err)
	}
	payload, err := encodeVectorPartitionScopedBuildPayloadV1(building, scope)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleScopedBuildV1, building.Generation, payload); err != nil {
		t.Fatal(err)
	}
	if err := deleteVectorPartitionStoreForTest(store, "docs", "embedding", building.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := reopened.vectorPartitionLifecycleGenerationCompleteV1("docs", "embedding", building.Generation)
	if err != nil || !complete {
		t.Fatalf("reopened completion=%v err=%v", complete, err)
	}
	if err := reopened.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleDeleteCompleteV1, building.Generation, nil); err != nil {
		t.Fatalf("ambiguous completion replay: %v", err)
	}
	if err := reopened.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleScopedBuildV1, building.Generation, payload); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("deleted generation resurrection err=%v", err)
	}
}

func TestVectorPartitionLocalScopeV1UnscopedCannotDeleteCompleteWithoutPrepare(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	building, _, _ := scopedLifecycleManifestsV1(t)
	raw, err := EncodeVectorPartitionManifestV1(building)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, building.Generation, raw); err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleDeleteCompleteV1, building.Generation, nil); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("unscoped direct complete err=%v", err)
	}
}
