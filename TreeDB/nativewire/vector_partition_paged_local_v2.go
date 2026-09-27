package nativewire

import (
	"cmp"
	"context"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// preparedVectorPartitionInputV2 reads the applied BUILD authority. Neither a
// caller-supplied manifest nor a self-consistent source checksum substitutes for
// a live catalog lifecycle record.
func preparedVectorPartitionInputV2(ctx context.Context, authority *raftplacement.CatalogMetaAuthorityV1, identity raftplacement.VectorPartitionLifecycleIdentityV1, node raftcluster.NodeID) (collections.VectorPartitionPreparedInputV2, error) {
	record, localSources, localANN, err := authority.VectorPartitionLocalPreparationV2(ctx, identity, node)
	if err != nil {
		return collections.VectorPartitionPreparedInputV2{}, err
	}
	owners := make([]source.SourceOwnerCommitmentV2, len(record.SourceOwners))
	for i, o := range record.SourceOwners {
		owners[i] = source.SourceOwnerCommitmentV2{GroupID: string(o.GroupID), ShardCount: o.ShardCount, SnapshotSetDigest: o.SnapshotSetDigest}
	}
	return collections.VectorPartitionPreparedInputV2{Generation: identity.Generation, Collection: identity.Index.Collection.Collection, IndexName: identity.Index.IndexName, IndexDefinitionDigest: identity.Index.IndexDefinitionDigest, SourceMapEpoch: identity.SourceV2.SourceMapEpoch, SourceMapDigest: identity.SourceV2.SourceMapDigest, SnapshotSetDigest: identity.SourceV2.SnapshotSetDigest, GraphProfileDigest: identity.SourceV2.GraphProfileDigest, PlacementDigest: identity.SourceV2.PlacementDigest, Owners: owners, LocalSourceOwners: localSources, LocalANNOwners: localANN}, nil
}

// OpenPreparedVectorPartitionSourceV2 opens pinned local source inputs for
// prepared construction. It does not activate the generation or admit native
// distributed Search; those lifecycle transitions remain separately refused.
func OpenPreparedVectorPartitionSourceV2(ctx context.Context, authority *raftplacement.CatalogMetaAuthorityV1, identity raftplacement.VectorPartitionLifecycleIdentityV1, node raftcluster.NodeID, ownership raftplacement.ResolvedSourceShardMapV2, collection *collections.Collection) (*collections.VectorPartitionPagedSourceSessionV2, error) {
	input, err := preparedVectorPartitionInputV2(ctx, authority, identity, node)
	if err != nil {
		return nil, err
	}
	if collection == nil || ownership.Collection() != identity.Index.Collection {
		return nil, raftplacement.ErrVectorPartitionLifecycleIdentity
	}
	return collection.OpenVectorPartitionPagedSourceSessionV2(ctx, input, ownership.SourceMapV2())
}

// BuildAndStagePreparedVectorPartitionSourceV2 stages the complete source-only
// local projection selected by an applied BUILD. Inputs select local imports;
// the collection producer verifies their persisted seals and complete owner
// semantic commitments before installing the local lifecycle roots.
func BuildAndStagePreparedVectorPartitionSourceV2(ctx context.Context, authority *raftplacement.CatalogMetaAuthorityV1, identity raftplacement.VectorPartitionLifecycleIdentityV1, node raftcluster.NodeID, ownership raftplacement.ResolvedSourceShardMapV2, owners []VectorPartitionOwnerSourceInputV2) (collections.VectorPartitionManifestV1, error) {
	input, err := preparedVectorPartitionInputV2(ctx, authority, identity, node)
	if err != nil {
		return collections.VectorPartitionManifestV1{}, err
	}
	if ownership.Collection() != identity.Index.Collection || len(input.LocalANNOwners) != 0 || len(owners) == 0 || len(owners) != len(input.LocalSourceOwners) {
		return collections.VectorPartitionManifestV1{}, raftplacement.ErrVectorPartitionLifecycleGuard
	}
	owners = slices.Clone(owners)
	slices.SortFunc(owners, func(a, b VectorPartitionOwnerSourceInputV2) int { return cmp.Compare(a.GroupID, b.GroupID) })
	collection := owners[0].Collection
	for i, owner := range owners {
		if owner.Collection == nil || owner.Collection != collection || owner.Walk == nil || string(owner.GroupID) != input.LocalSourceOwners[i] {
			return collections.VectorPartitionManifestV1{}, raftplacement.ErrVectorPartitionLifecycleGuard
		}
	}
	return collection.BuildAndStageVectorPartitionSourceProjectionV2(ctx, input, ownership.SourceMapV2(), func(ctx context.Context, visit func(string, collections.VectorPartitionSourceSnapshotV2) error) error {
		for _, owner := range owners {
			if err := owner.Walk(ctx, func(snapshot collections.VectorPartitionSourceSnapshotV2) error {
				return visit(string(owner.GroupID), snapshot)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
