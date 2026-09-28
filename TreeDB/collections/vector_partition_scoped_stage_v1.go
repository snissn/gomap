package collections

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// VectorPartitionScopedStageAuthorityV1 is supplied by the trusted catalog
// adapter. Its check must fence a fresh committed BUILD record and compare the
// exact immutable manifest, placement, source, definition, and owner set.
// Local inventory or a caller's scope digest alone is never stage authority.
type VectorPartitionScopedStageAuthorityV1 interface {
	ValidateVectorPartitionScopedStageV1(context.Context, VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error
}

// StageVectorPartitionScopedManifestWithContextV1 persists only this node's
// immutable prepared inventory. It never activates a local serving pointer.
// The catalog check brackets local verification and publication; a changed
// catalog may leave an unservable local generation, which reopen must reject.
func (c *Collection) StageVectorPartitionScopedManifestWithContextV1(
	ctx context.Context,
	ready VectorPartitionManifestV1,
	scope VectorPartitionLocalScopeV1,
	resources *rootpublication.StableResourceSet,
	authority VectorPartitionScopedStageAuthorityV1,
) error {
	if resources != nil {
		defer resources.Release()
	}
	if c == nil || c.db == nil {
		return errors.New("collections: closed collection")
	}
	if resources == nil || authority == nil {
		return fmt.Errorf("%w: scoped stage requires stable resources and catalog authority", ErrVectorPartitionManifestInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	if ready.State != "ready" || ready.Collection != c.name {
		return fmt.Errorf("%w: scoped stage requires this collection's ready manifest", ErrVectorPartitionManifestInvalid)
	}
	if err := preflightVectorPartitionManifestV1(ready, DefaultVectorPartitionManifestLimits()); err != nil {
		return err
	}
	ready.Canonicalize()
	if err := ready.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return err
	}
	assets, err := scope.localAssetsV1(ready)
	if err != nil {
		return err
	}
	prepared := make([]ColumnPreparedAsset, 0, len(assets))
	for _, asset := range assets {
		prepared = append(prepared, ColumnPreparedAsset{Ref: asset.Ref, Bytes: int64(asset.Bytes)})
	}
	if err := validateStableColumnResourcesMatchPrepared(prepared, resources); err != nil {
		return fmt.Errorf("collections: scoped stage stable resources: %w", err)
	}
	return c.withVectorPartitionStorageMutationV1(vectorPartitionMutationOperationPublishV1, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var def *VectorIndexDefinition
		for i := range c.meta.VectorIndexes {
			if c.meta.VectorIndexes[i].Name == ready.IndexName {
				def = &c.meta.VectorIndexes[i]
				break
			}
		}
		if def == nil || ready.IndexDefinitionDigest != VectorIndexDefinitionDigestV1(*def) {
			return fmt.Errorf("%w: scoped stage index definition", ErrVectorPartitionManifestInvalid)
		}
		if err := authority.ValidateVectorPartitionScopedStageV1(ctx, ready, scope); err != nil {
			return err
		}
		if c.meta.Options.ColumnStore == nil || c.meta.Options.ColumnStore.AssetManager == nil {
			return fmt.Errorf("%w: missing local asset manager", ErrVectorPartitionManifestInvalid)
		}
		if err := verifyVectorPartitionAssetsWithContextV1(ctx, c.db.ColumnAssetRootDir(), c.meta.Options.ColumnStore.AssetManager.Namespace, assets); err != nil {
			return err
		}
		if err := authority.ValidateVectorPartitionScopedStageV1(ctx, ready, scope); err != nil {
			return err
		}
		store, err := OpenVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		if err := store.stageVectorPartitionScopedManifestLifecycleV1(ready, scope); err != nil {
			return err
		}
		return authority.ValidateVectorPartitionScopedStageV1(ctx, ready, scope)
	})
}

func (s *VectorPartitionStoreV1) stageVectorPartitionScopedManifestLifecycleV1(ready VectorPartitionManifestV1, scope VectorPartitionLocalScopeV1) error {
	loaded, present, err := s.loadVectorPartitionLifecycleAuthorityV1(ready.Collection, ready.IndexName)
	if err != nil {
		return err
	}
	entry, exists := loaded.state.Generations[ready.Generation]
	if !exists {
		if present && ready.Generation <= loaded.state.GenerationHighWater {
			return fmt.Errorf("%w: scoped generation %d already completed", ErrVectorPartitionManifestInvalid, ready.Generation)
		}
		building := cloneVectorPartitionManifestForCheckpointV1(ready)
		building.State, building.RouterGeneration = "building", 0
		building.RouterAsset, building.ReadySetDigest = VectorPartitionAssetV1{}, ""
		building.Canonicalize()
		payload, err := encodeVectorPartitionScopedBuildPayloadV1(building, scope)
		if err != nil {
			return err
		}
		if err := s.persistVectorPartitionLifecycleOperationV1(ready.Collection, ready.IndexName, vectorPartitionLifecycleScopedBuildV1, ready.Generation, payload); err != nil {
			return err
		}
		entry = vectorPartitionLifecycleGenerationStateV1{Manifest: &building, Scope: &scope}
	}
	if entry.Manifest == nil || entry.Scope == nil || *entry.Scope != scope || entry.Deleting {
		return fmt.Errorf("%w: scoped generation identity conflict", ErrVectorPartitionManifestInvalid)
	}
	if entry.Manifest.State == "building" {
		payload, err := makeVectorPartitionReadyPromotionPayloadV1(*entry.Manifest, ready)
		if err != nil {
			return err
		}
		return s.persistVectorPartitionLifecycleOperationV1(ready.Collection, ready.IndexName, vectorPartitionLifecycleReadyV1, ready.Generation, payload)
	}
	if entry.Manifest.State != "ready" || !vectorPartitionManifestCanonicalEqualV1(*entry.Manifest, ready) {
		return fmt.Errorf("%w: scoped generation already published with different bytes", ErrVectorPartitionManifestInvalid)
	}
	return nil
}
