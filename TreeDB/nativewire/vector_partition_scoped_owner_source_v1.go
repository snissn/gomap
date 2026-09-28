package nativewire

import (
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// NewCollectionVectorPartitionGenerationSourceForScopedOwnerV1 serves only
// immutable owner-local assets. The lifecycle authority checks the active
// ready-set, while scopedAuthority binds the full manifest and placement to
// the same ACTIVE catalog record before each load, cache hit, and pack open.
func NewCollectionVectorPartitionGenerationSourceForScopedOwnerV1(
	collection *collections.Collection,
	collectionRef raftplacement.CollectionRefV1,
	lifecycle VectorPartitionReplicatedLifecycleAuthorityV1,
	scopedAuthority collections.VectorPartitionScopedOwnerAuthorityV1,
	owner raftcluster.GroupID,
) (*CollectionVectorPartitionGenerationSourceV1, error) {
	if scopedAuthority == nil || owner == "" {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	source, err := NewCollectionVectorPartitionGenerationSourceForReplicatedLifecycleV1(collection, collectionRef, lifecycle)
	if err != nil {
		return nil, err
	}
	source.scopedOwnerAuthority = scopedAuthority
	source.ownerGroupID = owner
	return source, nil
}
