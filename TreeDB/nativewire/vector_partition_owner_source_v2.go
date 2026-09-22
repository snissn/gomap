package nativewire

import (
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// NewCollectionVectorPartitionGenerationSourceForOwnerReplicatedLifecycleV2
// retains search metadata for one owner while preserving replicated lifecycle
// admission and ordinary source verification. expectedManifestDigest must name
// the exact stored manifest admitted by the caller's placement/asset authority;
// the constructor does not grant that authority. It refuses a different stored
// root instead of interpreting a caller's placement overlay as stored metadata.
//
// This initial V2 path still reads the complete V1 manifest on a cold load and
// uses the existing source reader. It is not yet the paged-root/sharded-source
// scalability contract.
func NewCollectionVectorPartitionGenerationSourceForOwnerReplicatedLifecycleV2(
	collection *collections.Collection,
	collectionRef raftplacement.CollectionRefV1,
	authority VectorPartitionReplicatedLifecycleAuthorityV1,
	owner raftcluster.GroupID,
	expectedManifestDigest string,
) (*CollectionVectorPartitionGenerationSourceV1, error) {
	if owner == "" || len(owner) > collections.DefaultVectorPartitionManifestLimits().MaxStringBytes || !isVectorPartitionShardSearchDigestV1(expectedManifestDigest) {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	source, err := NewCollectionVectorPartitionGenerationSourceForReplicatedLifecycleV1(collection, collectionRef, authority)
	if err != nil {
		return nil, err
	}
	source.ownerGroupID = owner
	source.ownerManifestDigest = expectedManifestDigest
	return source, nil
}
