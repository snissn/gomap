package nativewire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// fixedPeerVectorScopedStageAuthorityV1 belongs to one bounded owner-local
// Stage call. The authenticated leader read is followed by a local applied
// catch-up; no leader-only proof is copied to an arbitrary owner follower.
type fixedPeerVectorScopedStageAuthorityV1 struct {
	mu         sync.Mutex
	authority  *raftplacement.CatalogMetaAuthorityV1
	fence      func(context.Context) (raftplacement.CatalogMetaStatusV1, error)
	identity   raftplacement.VectorPartitionLifecycleIdentityV1
	hosted     raftcluster.GroupID
	routerOnly bool
	timeout    time.Duration
	expected   *raftplacement.CatalogMetaStatusV1
}

func newFixedPeerVectorScopedStageAuthorityV1(r *FixedPeerTCPRuntimeV1, hosted raftcluster.GroupID) (*fixedPeerVectorScopedStageAuthorityV1, error) {
	if r == nil || r.authority == nil || r.meta == nil || r.config.Vector == nil ||
		r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) ||
		hosted == "" || (r.data[hosted] == nil && (hosted != r.config.Catalog.ID || r.meta == nil)) {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return &fixedPeerVectorScopedStageAuthorityV1{
		authority: r.authority, fence: r.catalogFence, identity: r.config.Vector.Identity, hosted: hosted,
		routerOnly: r.data[hosted] == nil, timeout: r.config.RequestTimeout,
	}, nil
}

func (a *fixedPeerVectorScopedStageAuthorityV1) ValidateVectorPartitionScopedStageV1(
	ctx context.Context, manifest collections.VectorPartitionManifestV1, scope collections.VectorPartitionLocalScopeV1,
) error {
	if a == nil || a.authority == nil || a.fence == nil || ctx == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}
	before, err := a.fence(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if before.AppliedIndex == 0 || before.Epoch != a.identity.Index.CatalogEpoch || before.Digest != a.identity.Index.CatalogDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	record, ok := a.authority.VectorPartitionLifecycleRecordV1(a.identity)
	after, err := a.fence(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if !sameScopedStageCatalogStatusV1(before, after) ||
		(a.expected != nil && (before.Epoch != a.expected.Epoch || before.Digest != a.expected.Digest)) {
		return ErrFixedPeerVectorProofStaleV1
	}
	if !ok || record.Identity != a.identity || record.Aborted ||
		(record.State != raftplacement.VectorPartitionLifecycleBuildingV1 &&
			record.State != raftplacement.VectorPartitionLifecycleStagedV1 &&
			record.State != raftplacement.VectorPartitionLifecyclePreparedV1 &&
			record.State != raftplacement.VectorPartitionLifecycleActiveV1) {
		return ErrFixedPeerVectorProofStaleV1
	}
	identity := a.identity
	placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil || placementDigest != scope.PlacementDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if identity.Immutable.ManifestDigest == "" || identity.Immutable.PlacementDigest == "" ||
		scope.HostedGroup != string(a.hosted) ||
		scope.ManifestDigest != identity.Immutable.ManifestDigest || scope.PlacementDigest != identity.Immutable.PlacementDigest ||
		manifest.Collection != identity.Index.Collection.Collection || manifest.IndexName != identity.Index.IndexName ||
		manifest.IndexDefinitionDigest != identity.Index.IndexDefinitionDigest || manifest.Generation != identity.Generation ||
		manifest.SourceGeneration != identity.Source.Generation || manifest.SourceChecksum != identity.Source.Checksum ||
		manifest.SourceSchemaHash != identity.Source.SchemaHash || manifest.SourceRowCount != identity.Source.RowCount {
		return ErrFixedPeerVectorProofStaleV1
	}
	owners := make([]raftcluster.GroupID, 0, len(manifest.Placements))
	for _, placement := range manifest.Placements {
		owners = append(owners, raftcluster.GroupID(placement.GroupID))
	}
	slices.Sort(owners)
	owners = slices.Compact(owners)
	if !slices.Equal(owners, record.RequiredGroups) {
		return fmt.Errorf("%w: scoped stage owner set differs from catalog", ErrFixedPeerVectorProofStaleV1)
	}
	if record.State == raftplacement.VectorPartitionLifecycleActiveV1 {
		// Recovery may stage a newly elected peer after ACTIVE, but only for
		// an owner whose exact assets already have a committed READY receipt.
		// The router has no READY vote, so require the complete owner set.
		if len(record.ReadyGroups) != len(owners) {
			return ErrFixedPeerVectorProofStaleV1
		}
		for _, owner := range owners {
			found := false
			for _, ready := range record.ReadyGroups {
				if ready.GroupID == owner && ready.AppliedIndex != 0 &&
					ready.AssetSetDigest == vectorPartitionM8GroupAssetSetDigestV1(string(owner), manifest) {
					found = true
				}
			}
			if !found {
				return ErrFixedPeerVectorProofStaleV1
			}
		}
	}
	if a.routerOnly {
		// A catalog-only ingress may prepare the router, but it cannot claim
		// data ownership through a scope with no opened data group.
		if !scope.Router || slices.Contains(owners, a.hosted) {
			return ErrFixedPeerVectorProofStaleV1
		}
	}
	if a.expected == nil {
		a.expected = &before
	}
	return nil
}

func sameScopedStageCatalogStatusV1(a, b raftplacement.CatalogMetaStatusV1) bool {
	return a.AppliedIndex == b.AppliedIndex && a.Epoch == b.Epoch && a.Digest == b.Digest
}

var _ collections.VectorPartitionScopedStageAuthorityV1 = (*fixedPeerVectorScopedStageAuthorityV1)(nil)
