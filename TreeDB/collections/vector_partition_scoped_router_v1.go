package collections

import (
	"context"
	"errors"
	"fmt"
)

// VectorPartitionScopedRouterAuthorityV1 is an ACTIVE catalog capability. The
// implementation must compare the complete immutable manifest/placement pair,
// hosted group, and router flag against fresh replicated authority on every
// check. Local inventory alone never grants serving authority.
type VectorPartitionScopedRouterAuthorityV1 interface {
	ValidateVectorPartitionScopedRouterV1(context.Context, VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error
}

// OpenPreparedVectorPartitionScopedRouterWithContextV1 opens only a locally
// hosted router. It verifies the local inventory and assets under the storage
// barrier, with ACTIVE catalog checks outside it so a network read cannot
// block local publication or snapshot work.
// The ordinary full-local opener retains its source-identity requirement.
func (c *Collection) OpenPreparedVectorPartitionScopedRouterWithContextV1(
	ctx context.Context, index string, generation uint64, authority VectorPartitionScopedRouterAuthorityV1,
) (*VectorPartitionRouterV1, VectorPartitionRouterOpenStatusV1, error) {
	if c == nil || c.db == nil || index == "" || generation == 0 || authority == nil {
		err := fmt.Errorf("%w: scoped router requires collection, generation, and catalog authority", ErrVectorPartitionManifestInvalid)
		return nil, VectorPartitionRouterOpenStatusV1{FailureReason: err.Error()}, err
	}
	manifest, scope, err := c.PreparedVectorPartitionScopedManifestWithContextV1(ctx, index, generation)
	if err != nil {
		return nil, VectorPartitionRouterOpenStatusV1{FailureReason: err.Error()}, err
	}
	if !scope.Router {
		err := fmt.Errorf("%w: scoped router is not hosted", ErrVectorPartitionManifestInvalid)
		return nil, VectorPartitionRouterOpenStatusV1{FailureReason: err.Error()}, err
	}
	if err := authority.ValidateVectorPartitionScopedRouterV1(ctx, manifest, scope); err != nil {
		return nil, VectorPartitionRouterOpenStatusV1{FailureReason: err.Error()}, err
	}
	router, status, err := c.openVectorPartitionRouterWithContextV1(ctx, index, false,
		func(ctx context.Context, store *VectorPartitionStoreV1) (VectorPartitionManifestV1, error) {
			loaded, present, err := store.loadVectorPartitionLifecycleAuthorityWithContextV1(ctx, c.name, index)
			if err != nil {
				return VectorPartitionManifestV1{}, err
			}
			entry, ok := loaded.state.Generations[generation]
			if !present || !ok || entry.Manifest == nil || entry.Scope == nil || entry.Deleting || entry.Manifest.State != "ready" || !entry.Scope.Router {
				return VectorPartitionManifestV1{}, fmt.Errorf("%w: generation %d has no prepared scoped router", ErrVectorPartitionManifestInvalid, generation)
			}
			current, err := vectorPartitionLifecycleManifestWithContextV1(ctx, loaded.state, generation, false)
			if err != nil {
				return VectorPartitionManifestV1{}, err
			}
			if *entry.Scope != scope || !vectorPartitionManifestCanonicalEqualV1(current, manifest) {
				return VectorPartitionManifestV1{}, fmt.Errorf("%w: scoped router generation changed", ErrVectorPartitionManifestInvalid)
			}
			assets, err := scope.localAssetsV1(current)
			if err != nil {
				return VectorPartitionManifestV1{}, err
			}
			if c.meta.Options.ColumnStore == nil || c.meta.Options.ColumnStore.AssetManager == nil {
				return VectorPartitionManifestV1{}, fmt.Errorf("%w: missing local asset manager", ErrVectorPartitionManifestInvalid)
			}
			if err := verifyVectorPartitionAssetsWithContextV1(ctx, c.db.ColumnAssetRootDir(), c.meta.Options.ColumnStore.AssetManager.Namespace, assets); err != nil {
				return VectorPartitionManifestV1{}, err
			}
			return current, nil
		}, true)
	if err != nil {
		return nil, status, err
	}
	if err := authority.ValidateVectorPartitionScopedRouterV1(ctx, manifest, scope); err != nil {
		failure := errors.Join(err, router.Close())
		status.FailureReason = failure.Error()
		return nil, status, failure
	}
	return router, status, nil
}
