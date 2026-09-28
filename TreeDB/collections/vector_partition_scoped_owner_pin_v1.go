package collections

import (
	"context"
	"errors"
	"fmt"
)

// VectorPartitionScopedOwnerAuthorityV1 checks the exact ACTIVE catalog
// manifest/placement pair and this node's hosted group. An implementation
// must reject a changed catalog status between successive checks.
type VectorPartitionScopedOwnerAuthorityV1 interface {
	ValidateVectorPartitionScopedOwnerV1(context.Context, VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error
	ValidateVectorPartitionScopedOwnerPairV1(context.Context, VectorPartitionLocalScopeV1) error
}

// AcquireVectorPartitionScopedOwnerPinWithContextV1 retains a prepared
// immutable owner generation without requiring a local source graph. The
// catalog checks bracket the local storage barrier so a network read cannot
// hold the barrier needed by local publication or snapshot work.
func (c *Collection) AcquireVectorPartitionScopedOwnerPinWithContextV1(
	ctx context.Context, index string, generation uint64, owner string, authority VectorPartitionScopedOwnerAuthorityV1,
) (VectorPartitionManifestV1, VectorPartitionLocalScopeV1, *VectorPartitionReaderPinV1, error) {
	var zeroManifest VectorPartitionManifestV1
	var zeroScope VectorPartitionLocalScopeV1
	if c == nil || c.db == nil || index == "" || generation == 0 || owner == "" || authority == nil {
		return zeroManifest, zeroScope, nil, fmt.Errorf("%w: scoped owner pin identity or authority", ErrVectorPartitionSearchUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	manifest, scope, err := c.PreparedVectorPartitionScopedManifestWithContextV1(ctx, index, generation)
	if err != nil {
		return zeroManifest, zeroScope, nil, err
	}
	if scope.HostedGroup != owner {
		return zeroManifest, zeroScope, nil, fmt.Errorf("%w: scoped owner mismatch", ErrVectorPartitionSearchUnavailable)
	}
	hostsPartition := false
	for _, placement := range manifest.Placements {
		if placement.GroupID == owner {
			hostsPartition = true
			break
		}
	}
	if !hostsPartition {
		return zeroManifest, zeroScope, nil, fmt.Errorf("%w: scoped owner has no partitions", ErrVectorPartitionSearchUnavailable)
	}
	if err := authority.ValidateVectorPartitionScopedOwnerV1(ctx, manifest, scope); err != nil {
		return zeroManifest, zeroScope, nil, err
	}
	var pin *VectorPartitionReaderPinV1
	err = WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		loaded, present, err := store.loadVectorPartitionLifecycleAuthorityWithContextV1(ctx, c.name, index)
		if err != nil {
			return err
		}
		entry, ok := loaded.state.Generations[generation]
		if !present || !ok || entry.Manifest == nil || entry.Scope == nil || entry.Deleting || entry.Manifest.State != "ready" ||
			*entry.Scope != scope || !vectorPartitionManifestCanonicalEqualV1(*entry.Manifest, manifest) {
			return fmt.Errorf("%w: scoped owner generation changed", ErrVectorPartitionSearchUnavailable)
		}
		assets, err := scope.localAssetsV1(manifest)
		if err != nil {
			return err
		}
		if c.meta.Options.ColumnStore == nil || c.meta.Options.ColumnStore.AssetManager == nil {
			return fmt.Errorf("%w: missing local asset manager", ErrVectorPartitionSearchUnavailable)
		}
		if err := verifyVectorPartitionAssetsWithContextV1(ctx, c.db.ColumnAssetRootDir(), c.meta.Options.ColumnStore.AssetManager.Namespace, assets); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		key := vectorPartitionReaderPinKeyV1(c.db.Dir(), c.name, index, generation)
		if key == "" {
			return errors.New("collections: invalid scoped owner pin root")
		}
		vectorPartitionReaderPinsV1.Lock()
		vectorPartitionReaderPinsV1.counts[key]++
		vectorPartitionReaderPinsV1.Unlock()
		pin = &VectorPartitionReaderPinV1{
			key: key, scopedManifestDigest: scope.ManifestDigest,
			scopedPlacementDigest: scope.PlacementDigest, scopedHostedGroup: owner,
		}
		return nil
	})
	if err != nil {
		return zeroManifest, zeroScope, nil, err
	}
	if err := authority.ValidateVectorPartitionScopedOwnerPairV1(ctx, scope); err != nil {
		pin.Release()
		return zeroManifest, zeroScope, nil, err
	}
	return manifest, scope, pin, nil
}

// OpenVectorPartitionScopedLocalSearcherForOwnerPlanWithContextV1 is the only
// production path that can open an immutable owner pack without a local source
// graph. The scoped pin and ACTIVE catalog pair are checked again around the
// mapped open. The pack decoder still verifies its base identity, membership
// digest, every section chunk, and the canonical graph variant.
func (c *Collection) OpenVectorPartitionScopedLocalSearcherForOwnerPlanWithContextV1(
	ctx context.Context, index string, generation uint64, partition uint32, owner string,
	scope VectorPartitionLocalScopeV1,
	plan *VectorPartitionGenerationOwnerSearchOpenPlanV2, generationPin *VectorPartitionReaderPinV1,
	authority VectorPartitionScopedOwnerAuthorityV1,
) (*VectorPartitionLocalSearcherV1, error) {
	if c == nil || c.db == nil || plan == nil || authority == nil || generationPin == nil ||
		plan.collection != c.name || plan.indexName != index || plan.generation != generation || plan.owner != owner ||
		scope.HostedGroup != owner ||
		plan.scopedManifestDigest != scope.ManifestDigest ||
		generationPin.scopedHostedGroup != owner ||
		generationPin.scopedManifestDigest != scope.ManifestDigest ||
		generationPin.scopedPlacementDigest != scope.PlacementDigest {
		return nil, fmt.Errorf("%w: stale scoped owner plan", ErrVectorPartitionSearchUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := authority.ValidateVectorPartitionScopedOwnerPairV1(ctx, scope); err != nil {
		return nil, err
	}
	asset, members, home, overlap, err := plan.partition(partition)
	if err != nil {
		return nil, err
	}
	pin, err := generationPin.cloneForKey(vectorPartitionReaderPinKeyV1(c.db.Dir(), c.name, index, generation))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	searcher, err := c.openVectorPartitionLocalSearcherForPreparedAssetsWithSourceModeV1(
		ctx, index, generation, partition,
		plan.indexDefinitionDigest, plan.source.Generation, plan.source.Checksum, plan.source.SchemaHash, plan.source.RowCount,
		asset, plan.partitionAssets(partition), members, home, overlap, false, "", false, false, true,
	)
	if err != nil {
		pin.Release()
		return nil, err
	}
	searcher.partitionPin = pin
	if err := authority.ValidateVectorPartitionScopedOwnerPairV1(ctx, scope); err != nil {
		_ = searcher.Close()
		return nil, err
	}
	return searcher, nil
}
