package nativewire

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// fixedPeerVectorScopedOwnerAuthorityV1 binds local immutable pack admission
// to an ACTIVE replicated lifecycle record. The two fences bracket the cloned
// record without carrying a leader-only read proof to this owner follower.
type fixedPeerVectorScopedOwnerAuthorityV1 struct {
	authority *raftplacement.CatalogMetaAuthorityV1
	fence     func(context.Context) (raftplacement.CatalogMetaStatusV1, error)
	identity  raftplacement.VectorPartitionLifecycleIdentityV1
	hosted    raftcluster.GroupID

	mu          sync.RWMutex
	owners      []raftcluster.GroupID
	readyDigest string
}

func newFixedPeerVectorScopedOwnerAuthorityV1(r *FixedPeerTCPRuntimeV1, hosted raftcluster.GroupID) (*fixedPeerVectorScopedOwnerAuthorityV1, error) {
	if r == nil || r.authority == nil || r.meta == nil || r.config.Vector == nil || r.data[hosted] == nil ||
		r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || hosted == "" {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return &fixedPeerVectorScopedOwnerAuthorityV1{
		authority: r.authority, fence: r.catalogFence, identity: r.config.Vector.Identity, hosted: hosted,
	}, nil
}

func (a *fixedPeerVectorScopedOwnerAuthorityV1) ValidateVectorPartitionScopedOwnerV1(
	ctx context.Context, manifest collections.VectorPartitionManifestV1, scope collections.VectorPartitionLocalScopeV1,
) error {
	if a == nil || a.authority == nil || a.fence == nil || ctx == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope.HostedGroup != string(a.hosted) || a.hosted == "" || a.identity.Immutable.ManifestDigest == "" ||
		a.identity.Immutable.PlacementDigest == "" {
		return ErrFixedPeerVectorProofStaleV1
	}
	raw, err := collections.EncodeVectorPartitionManifestV1(manifest)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != scope.ManifestDigest ||
		scope.ManifestDigest != a.identity.Immutable.ManifestDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil || placementDigest != scope.PlacementDigest || placementDigest != a.identity.Immutable.PlacementDigest ||
		manifest.Collection != a.identity.Index.Collection.Collection || manifest.IndexName != a.identity.Index.IndexName ||
		manifest.IndexDefinitionDigest != a.identity.Index.IndexDefinitionDigest || manifest.Generation != a.identity.Generation ||
		manifest.SourceGeneration != a.identity.Source.Generation || manifest.SourceChecksum != a.identity.Source.Checksum ||
		manifest.SourceSchemaHash != a.identity.Source.SchemaHash || manifest.SourceRowCount != a.identity.Source.RowCount {
		return ErrFixedPeerVectorProofStaleV1
	}
	owners := make([]raftcluster.GroupID, 0, len(manifest.Placements))
	for _, placement := range manifest.Placements {
		owners = append(owners, raftcluster.GroupID(placement.GroupID))
	}
	slices.Sort(owners)
	owners = slices.Compact(owners)
	if !slices.Contains(owners, a.hosted) {
		return ErrFixedPeerVectorProofStaleV1
	}
	readyDigest := vectorPartitionM8GroupAssetSetDigestV1(string(a.hosted), manifest)
	if err := a.validatePair(ctx, scope, owners, readyDigest); err != nil {
		return err
	}
	a.mu.Lock()
	a.owners = slices.Clone(owners)
	a.readyDigest = readyDigest
	a.mu.Unlock()
	return nil
}

// ValidateVectorPartitionScopedOwnerPairV1 is the hot-path catalog check. The
// full manifest, hosted bytes, and ready digest were verified at cold pin
// admission; every request still observes a fresh ACTIVE catalog fence.
func (a *fixedPeerVectorScopedOwnerAuthorityV1) ValidateVectorPartitionScopedOwnerPairV1(
	ctx context.Context, scope collections.VectorPartitionLocalScopeV1,
) error {
	if a == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	a.mu.RLock()
	owners := a.owners
	readyDigest := a.readyDigest
	a.mu.RUnlock()
	if len(owners) == 0 || readyDigest == "" {
		return ErrFixedPeerVectorProofStaleV1
	}
	return a.validatePair(ctx, scope, owners, readyDigest)
}

func (a *fixedPeerVectorScopedOwnerAuthorityV1) validatePair(
	ctx context.Context, scope collections.VectorPartitionLocalScopeV1,
	owners []raftcluster.GroupID, readyDigest string,
) error {
	if a == nil || a.authority == nil || a.fence == nil || ctx == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.hosted == "" || scope.HostedGroup != string(a.hosted) ||
		scope.ManifestDigest != a.identity.Immutable.ManifestDigest ||
		scope.PlacementDigest != a.identity.Immutable.PlacementDigest {
		return ErrFixedPeerVectorProofStaleV1
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
	if !sameScopedStageCatalogStatusV1(before, after) || !ok || record.Aborted || record.Identity != a.identity ||
		record.State != raftplacement.VectorPartitionLifecycleActiveV1 || record.ReadySetDigest == "" ||
		!slices.Equal(record.RequiredGroups, owners) {
		return ErrFixedPeerVectorProofStaleV1
	}
	ready := false
	for _, group := range record.ReadyGroups {
		if group.GroupID == a.hosted && group.AppliedIndex != 0 && group.AssetSetDigest == readyDigest {
			ready = true
		}
	}
	if !ready {
		return ErrFixedPeerVectorProofStaleV1
	}
	return nil
}

var _ collections.VectorPartitionScopedOwnerAuthorityV1 = (*fixedPeerVectorScopedOwnerAuthorityV1)(nil)
