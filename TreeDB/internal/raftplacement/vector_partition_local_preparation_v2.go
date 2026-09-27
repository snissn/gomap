package raftplacement

import (
	"context"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// VectorPartitionLocalPreparationV2 reads the applied BUILD and all of this
// node's hosted source/ANN groups under one catalog proof and read lock. Source
// ownership and ANN placement are independent. Callers cannot omit a hosted
// group when constructing the generation's one immutable local projection.
func (a *CatalogMetaAuthorityV1) VectorPartitionLocalPreparationV2(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, node raftcluster.NodeID) (VectorPartitionLifecycleRecordV1, []string, []string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionLifecycleRecordV1{}, nil, nil, err
	}
	if a == nil || node == "" || identity.SourceFormat != 2 {
		return VectorPartitionLifecycleRecordV1{}, nil, nil, ErrVectorPartitionLifecycleIdentity
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.admitLocked(CatalogProofV1{Epoch: identity.Index.CatalogEpoch, Digest: identity.Index.CatalogDigest}); err != nil {
		return VectorPartitionLifecycleRecordV1{}, nil, nil, err
	}
	record, ok := a.lifecycle[identity]
	if !ok || record.Identity != identity {
		return VectorPartitionLifecycleRecordV1{}, nil, nil, ErrVectorPartitionLifecycleGuard
	}
	switch record.State {
	case VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1, VectorPartitionLifecyclePreparedV1:
	default:
		return VectorPartitionLifecycleRecordV1{}, nil, nil, ErrVectorPartitionLifecycleState
	}
	var sources, ann []string
	for _, owner := range record.SourceOwners {
		group, ok := a.resolved.groups[owner.GroupID]
		if !ok {
			return VectorPartitionLifecycleRecordV1{}, nil, nil, ErrUnknownVectorPartitionGroup
		}
		if slices.Contains(group.Members, node) {
			sources = append(sources, string(owner.GroupID))
		}
	}
	for _, id := range record.RequiredGroups {
		group, ok := a.resolved.groups[id]
		if !ok {
			return VectorPartitionLifecycleRecordV1{}, nil, nil, ErrUnknownVectorPartitionGroup
		}
		if slices.Contains(group.Members, node) {
			ann = append(ann, string(id))
		}
	}
	if len(sources)+len(ann) == 0 {
		return VectorPartitionLifecycleRecordV1{}, nil, nil, ErrVectorPartitionLifecycleGuard
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionLifecycleRecordV1{}, nil, nil, err
	}
	return cloneVectorPartitionLifecycleRecordV1(record), sources, ann, nil
}
