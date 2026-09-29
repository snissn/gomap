package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

// This authority binds a TEST manifest exactly. It is not a committed catalog
// adapter and grants no public TCP serving or source-holder publication trust.
type ownerRelocationDiagnosticAuthorityV1 struct {
	raw   []byte
	scope collections.VectorPartitionLocalScopeV1
}

func (a ownerRelocationDiagnosticAuthorityV1) ValidateVectorPartitionScopedStageV1(ctx context.Context, m collections.VectorPartitionManifestV1, scope collections.VectorPartitionLocalScopeV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := collections.EncodeVectorPartitionManifestV1(m)
	if err != nil || !bytes.Equal(raw, a.raw) || scope != a.scope {
		return errors.New("diagnostic authority: wrong exact manifest or scope")
	}
	return nil
}

func (a ownerRelocationDiagnosticAuthorityV1) ValidateVectorPartitionScopedOwnerV1(ctx context.Context, m collections.VectorPartitionManifestV1, scope collections.VectorPartitionLocalScopeV1) error {
	return a.ValidateVectorPartitionScopedStageV1(ctx, m, scope)
}

func (a ownerRelocationDiagnosticAuthorityV1) ValidateVectorPartitionScopedOwnerPairV1(ctx context.Context, scope collections.VectorPartitionLocalScopeV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope != a.scope {
		return errors.New("diagnostic authority: wrong exact scope")
	}
	return nil
}

func ownerRelocationDiagnosticAssetPathV1(root string, ref collections.ColumnAssetRef) string {
	return filepath.Join(backenddb.ColumnAssetRootDirPath(root), filepath.FromSlash(ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", ref.FileID))
}

func ownerRelocationDiagnosticPayloadV1(t *testing.T, root string, asset collections.VectorPartitionAssetV1) []byte {
	t.Helper()
	// ponytail: tiny diagnostic only, refuse assets over 1MiB; use streaming
	// producer support before attempting a retained large-asset relocation.
	if asset.Ref.Offset < 0 || asset.Ref.Length <= 0 || asset.Ref.Length > 1<<20 || uint64(asset.Ref.Length) != asset.Bytes {
		t.Fatalf("invalid or oversized diagnostic asset: %+v", asset)
	}
	f, err := os.Open(ownerRelocationDiagnosticAssetPathV1(root, asset.Ref))
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, int(asset.Ref.Length))
	_, readErr := io.ReadFull(io.NewSectionReader(f, asset.Ref.Offset, asset.Ref.Length), payload)
	if err := errors.Join(readErr, f.Close()); err != nil {
		t.Fatal(err)
	}
	if page.Checksum(payload) != asset.Ref.Checksum || fmt.Sprintf("%x", sha256.Sum256(payload)) != asset.Checksum {
		t.Fatalf("source checksum mismatch: %q", asset.ID)
	}
	return payload
}

func ownerRelocationDiagnosticTreeSealV1(t *testing.T, root string) map[string]string {
	t.Helper()
	seal := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected nonregular source entry: %s", path)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, readErr := io.Copy(h, f)
		if err := errors.Join(readErr, f.Close()); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		seal[rel] = fmt.Sprintf("%x", h.Sum(nil))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return seal
}

func TestVectorPartitionOwnerRelocationDiagnosticV1(t *testing.T) {
	if os.Getenv("GOMAP_P3_OWNER_RELOCATION_DIAGNOSTIC") != "1" {
		t.Skip("set GOMAP_P3_OWNER_RELOCATION_DIAGNOSTIC=1 for the tiny relocation diagnostic")
	}
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0, overlap: true},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-a", "group-b"}, false, false)
	seedClosed := false
	t.Cleanup(func() {
		if !seedClosed {
			if err := seed.database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	originalRaw, err := collections.EncodeVectorPartitionManifestV1(seed.manifest)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := collections.VectorPartitionPlacementDigestV1(seed.manifest)
	if err != nil {
		t.Fatal(err)
	}
	originalScope := collections.VectorPartitionLocalScopeV1{HostedGroup: "group-a", ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(originalRaw)), PlacementDigest: placementDigest}
	resources, err := seed.collection.CaptureVectorPartitionScopedExistingAssetsV1(seed.manifest, originalScope)
	if resources != nil {
		resources.Release()
	}
	if !errors.Is(err, collections.ErrVectorPartitionManifestInvalid) || !strings.Contains(err.Error(), "shared foreign segment") {
		t.Fatalf("shared original segment did not hit the unchanged owner guard: %v", err)
	}
	query := []float32{.7, .7}
	baseline := make(map[uint32][]collections.VectorPartitionSearchResultV1)
	for _, partition := range []uint32{0, 2} { // One graph anchor per logical domain.
		searcher, err := seed.collection.OpenVectorPartitionLocalSearcherForGenerationWithContextV1(t.Context(), seed.manifest.IndexName, seed.manifest.Generation, partition)
		if err != nil {
			t.Fatal(err)
		}
		results, metrics, searchErr := searcher.SearchWithMetrics(query, 4)
		if err := errors.Join(searchErr, searcher.Close()); err != nil {
			t.Fatal(err)
		}
		if metrics.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || len(results) == 0 {
			t.Fatalf("original native graph was not searched: %+v", metrics)
		}
		baseline[partition] = results
	}
	metaBytes, err := json.Marshal(seed.collection.MetaView())
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}
	seedClosed = true
	sourceSeal := ownerRelocationDiagnosticTreeSealV1(t, seed.dir)
	derived, err := collections.DecodeVectorPartitionManifestV1(originalRaw, collections.DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	// The tiny fixture's generation is source-derived; retained accepted assets
	// use gen1. In either case native payload bindings require keeping it unchanged.
	// Each fresh DB supplies a NEW vector_partitions authority namespace.
	type ownerDB struct {
		group      string
		dir        string
		db         *backenddb.DB
		collection *collections.Collection
	}
	owners := make([]ownerDB, 0, 2)
	t.Cleanup(func() {
		for i := range owners {
			if owners[i].db != nil {
				if err := owners[i].db.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	groups := make(map[uint32]string)
	for _, placement := range derived.Placements {
		groups[placement.PartitionID] = placement.GroupID
	}
	for ownerIndex, group := range []string{"group-a", "group-b"} {
		var meta collections.CollectionMeta
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			t.Fatal(err)
		}
		meta.Options.ColumnStore.ActiveManifest = nil
		meta.Options.ColumnStore.RecoveryAuthoritativeManifest = nil
		meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN = 0
		meta.Options.ColumnStore.AssetManager = &collections.ColumnAssetManagerConfig{Kind: collections.ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "p3-owner-relocation-v1"}
		dir := t.TempDir()
		database, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		owners = append(owners, ownerDB{group: group, dir: dir, db: database})
		manager := collections.NewCollectionManager(database)
		if _, err := manager.CreateCollection(&meta); err != nil {
			t.Fatal(err)
		}
		collection, err := manager.OpenCollection(meta.Name)
		if err != nil {
			t.Fatal(err)
		}
		owners[ownerIndex].collection = collection
		items := make([]collections.StableColumnPhysicalAssetAppend, 0)
		assetIndexes := make([]int, 0)
		var batchBytes int
		for i, asset := range seed.manifest.Assets {
			if groups[asset.PartitionID] != group {
				continue
			}
			payload := ownerRelocationDiagnosticPayloadV1(t, seed.dir, asset)
			batchBytes += len(payload)
			if batchBytes > 4<<20 {
				t.Fatal("owner batch exceeds the tiny diagnostic's 4MiB bound")
			}
			items = append(items, collections.StableColumnPhysicalAssetAppend{Payload: payload, Kind: asset.Ref.Kind, Generation: asset.Ref.Generation, PartID: asset.Ref.PartID, FileID: uint32(51001 + ownerIndex)})
			assetIndexes = append(assetIndexes, i)
		}
		if ownerIndex == 0 {
			asset := seed.manifest.RouterAsset
			payload := ownerRelocationDiagnosticPayloadV1(t, seed.dir, asset)
			batchBytes += len(payload)
			if batchBytes > 4<<20 {
				t.Fatal("owner/router batch exceeds the tiny diagnostic's 4MiB bound")
			}
			items = append(items, collections.StableColumnPhysicalAssetAppend{Payload: payload, Kind: asset.Ref.Kind, Generation: asset.Ref.Generation, PartID: asset.Ref.PartID, FileID: 51003})
		}
		lease, err := database.AcquireStableResourceCaptureLease()
		if err != nil {
			t.Fatal(err)
		}
		refs, appended, err := collections.AppendColumnPhysicalAssetsWithStableResources(database.ColumnAssetRootDir(), *collection.MetaView().Options.ColumnStore, 0, items, database.StableResourceIdentityPinRegistry(), lease)
		lease.Release()
		if appended != nil {
			appended.Release() // Owner startup recaptures; append-time pins are not transferred.
		}
		if err != nil || len(refs) != len(items) {
			t.Fatalf("owner append refs=%d items=%d err=%v", len(refs), len(items), err)
		}
		for j, i := range assetIndexes {
			derived.Assets[i].Ref = refs[j]
		}
		if ownerIndex == 0 {
			derived.RouterAsset.Ref = refs[len(refs)-1]
		}
	}
	derived.Canonicalize()
	if err := derived.Validate(collections.DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatal(err)
	}
	derivedRaw, err := collections.EncodeVectorPartitionManifestV1(derived)
	if err != nil {
		t.Fatal(err)
	}
	derivedPlacement, err := collections.VectorPartitionPlacementDigestV1(derived)
	if err != nil || derivedPlacement != placementDigest || bytes.Equal(originalRaw, derivedRaw) || derived.ReadySetDigest == seed.manifest.ReadySetDigest {
		t.Fatalf("new physical identity changed placement or failed to reseal: %v", err)
	}
	normalized, err := collections.DecodeVectorPartitionManifestV1(derivedRaw, collections.DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	for i := range normalized.Assets {
		oldRef, newRef := seed.manifest.Assets[i].Ref, normalized.Assets[i].Ref
		oldRef.Namespace, oldRef.FileID, oldRef.Offset = newRef.Namespace, newRef.FileID, newRef.Offset
		if oldRef != newRef {
			t.Fatalf("relocation changed payload ref identity: %q", normalized.Assets[i].ID)
		}
		normalized.Assets[i].Ref = seed.manifest.Assets[i].Ref
	}
	oldRouterRef, newRouterRef := seed.manifest.RouterAsset.Ref, normalized.RouterAsset.Ref
	oldRouterRef.Namespace, oldRouterRef.FileID, oldRouterRef.Offset = newRouterRef.Namespace, newRouterRef.FileID, newRouterRef.Offset
	if oldRouterRef != newRouterRef {
		t.Fatal("relocation changed router payload ref identity")
	}
	normalized.RouterAsset.Ref = seed.manifest.RouterAsset.Ref
	normalized.Canonicalize()
	normalizedRaw, err := collections.EncodeVectorPartitionManifestV1(normalized)
	if err != nil || !bytes.Equal(originalRaw, normalizedRaw) {
		t.Fatalf("relocation changed logical/source/router/membership manifest fields: %v", err)
	}
	for i := range owners {
		owner := &owners[i]
		scope := collections.VectorPartitionLocalScopeV1{HostedGroup: owner.group, Router: i == 0, ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(derivedRaw)), PlacementDigest: placementDigest}
		authority := ownerRelocationDiagnosticAuthorityV1{raw: derivedRaw, scope: scope}
		resources, err := owner.collection.CaptureVectorPartitionScopedExistingAssetsV1(derived, scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.collection.StageVectorPartitionScopedManifestWithContextV1(t.Context(), derived, scope, resources, authority); err != nil {
			t.Fatal(err)
		}
		if err := owner.db.Close(); err != nil {
			t.Fatal(err)
		}
		owner.db = nil
		owner.db, err = backenddb.Open(backenddb.Options{Dir: owner.dir, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		owner.collection, err = collections.NewCollectionManager(owner.db).OpenCollection(derived.Collection)
		if err != nil {
			t.Fatal(err)
		}
		got, gotScope, err := owner.collection.PreparedVectorPartitionScopedManifestWithContextV1(t.Context(), derived.IndexName, derived.Generation)
		gotRaw, encodeErr := collections.EncodeVectorPartitionManifestV1(got)
		if err != nil || encodeErr != nil || !bytes.Equal(gotRaw, derivedRaw) || gotScope != scope {
			t.Fatalf("scoped reopen changed identity: %v %v", err, encodeErr)
		}
		if _, err := owner.collection.ScanDocumentIDsFunc(1, func([]byte) (bool, error) { t.Fatal("owner contains a source document"); return false, nil }); err != nil {
			t.Fatal(err)
		}
		allowed := make(map[string]bool)
		for j, asset := range derived.Assets {
			path := ownerRelocationDiagnosticAssetPathV1(owner.dir, asset.Ref)
			if groups[asset.PartitionID] != owner.group {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("owner received foreign segment: %s %v", path, err)
				}
				continue
			}
			if !bytes.Equal(ownerRelocationDiagnosticPayloadV1(t, owner.dir, asset), ownerRelocationDiagnosticPayloadV1(t, seed.dir, seed.manifest.Assets[j])) {
				t.Fatalf("owned payload differs after reopen: %q", asset.ID)
			}
			rel, err := filepath.Rel(backenddb.ColumnAssetRootDirPath(owner.dir), path)
			if err != nil {
				t.Fatal(err)
			}
			allowed[rel] = true
		}
		if i == 0 {
			if !bytes.Equal(ownerRelocationDiagnosticPayloadV1(t, owner.dir, derived.RouterAsset), ownerRelocationDiagnosticPayloadV1(t, seed.dir, seed.manifest.RouterAsset)) {
				t.Fatal("router payload changed")
			}
			rel, err := filepath.Rel(backenddb.ColumnAssetRootDirPath(owner.dir), ownerRelocationDiagnosticAssetPathV1(owner.dir, derived.RouterAsset.Ref))
			if err != nil {
				t.Fatal(err)
			}
			allowed[rel] = true
		}
		fixedPeerAssertHostedVectorFilesV1(t, owner.dir, allowed, false)
		_, _, pin, err := owner.collection.AcquireVectorPartitionScopedOwnerPinWithContextV1(t.Context(), derived.IndexName, derived.Generation, owner.group, authority)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := collections.NewVectorPartitionScopedOwnerSearchOpenPlanWithContextV1(t.Context(), derived, owner.group)
		if err != nil {
			pin.Release()
			t.Fatal(err)
		}
		partition := uint32(i * 2)
		searcher, err := owner.collection.OpenVectorPartitionScopedLocalSearcherForOwnerPlanWithContextV1(t.Context(), derived.IndexName, derived.Generation, partition, owner.group, scope, plan, pin, authority)
		pin.Release()
		if err != nil {
			t.Fatal(err)
		}
		results, metrics, searchErr := searcher.SearchWithMetrics(query, 4)
		if err := errors.Join(searchErr, searcher.Close()); err != nil {
			t.Fatal(err)
		}
		if metrics.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || !reflect.DeepEqual(results, baseline[partition]) {
			t.Fatalf("relocated native search differs: partition=%d got=%+v want=%+v route=%s", partition, results, baseline[partition], metrics.Route)
		}
		foreignScope := scope
		foreignScope.HostedGroup = owners[1-i].group
		foreignScope.Router = false
		foreignResources, err := owner.collection.CaptureVectorPartitionScopedExistingAssetsV1(derived, foreignScope)
		if foreignResources != nil {
			foreignResources.Release()
		}
		if err == nil {
			t.Fatal("foreign owner's missing assets were captured")
		}
	}
	// Corruption and missing-file controls use only the disposable owner-b root.
	owner := &owners[1]
	var owned collections.VectorPartitionAssetV1
	for _, asset := range derived.Assets {
		if groups[asset.PartitionID] == owner.group {
			owned = asset
			break
		}
	}
	f, err := os.OpenFile(ownerRelocationDiagnosticAssetPathV1(owner.dir, owned.Ref), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := ownerRelocationDiagnosticPayloadV1(t, owner.dir, owned)
	_, writeErr := f.WriteAt([]byte{payload[0] ^ 1}, owned.Ref.Offset)
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := owner.collection.PreparedVectorPartitionScopedManifestWithContextV1(t.Context(), derived.IndexName, derived.Generation); err == nil {
		t.Fatal("scoped prepared read accepted corrupt hosted bytes")
	}
	if err := os.Remove(ownerRelocationDiagnosticAssetPathV1(owner.dir, owned.Ref)); err != nil {
		t.Fatal(err)
	}
	scope := collections.VectorPartitionLocalScopeV1{HostedGroup: owner.group, ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(derivedRaw)), PlacementDigest: placementDigest}
	missingResources, err := owner.collection.CaptureVectorPartitionScopedExistingAssetsV1(derived, scope)
	if missingResources != nil {
		missingResources.Release()
	}
	if err == nil {
		t.Fatal("missing hosted segment was captured")
	}
	if !reflect.DeepEqual(sourceSeal, ownerRelocationDiagnosticTreeSealV1(t, seed.dir)) {
		t.Fatal("original source DB or accepted fixture authority changed")
	}
	t.Logf("diagnostic only: new manifest=%x unchanged placement=%s owners=2 native parity anchors=2; no public TCP/source-holder/performance qualification", sha256.Sum256(derivedRaw), placementDigest)
}
