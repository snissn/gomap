package nativewire

import (
	"context"
	"errors"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

const (
	fixedPeerVectorCatalogStageV1  fixedPeerVectorLifecycleActionV1 = "read-immutable-stage"
	fixedPeerVectorCatalogActiveV1 fixedPeerVectorLifecycleActionV1 = "read-immutable-active"
)

// A consumer decision is a fresh authenticated read for one configured
// immutable owner. It is never installed as local catalog authority or reused
// as a serving lease. BUILD/source capture and the router remain catalog voters.
func (r *FixedPeerTCPRuntimeV1) immutableVectorCatalogConsumerV1() bool {
	if r == nil || r.meta != nil || r.authority != nil || r.client == nil || r.client.security == nil ||
		r.vector == nil || r.config.Vector == nil || r.config.NodeID == r.config.Vector.RouterNodeID ||
		r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return false
	}
	return slices.Contains(fixedPeerVectorOwnerGroupsV1(r.config.Vector.Placement), r.vector.dataGroup) && r.data[r.vector.dataGroup] != nil
}

func (r *FixedPeerTCPRuntimeV1) consumerImmutableVectorCatalogV1(ctx context.Context, action fixedPeerVectorLifecycleActionV1) (raftplacement.CatalogMetaStatusV1, raftplacement.VectorPartitionLifecycleRecordV1, error) {
	var zero raftplacement.VectorPartitionLifecycleRecordV1
	if !r.immutableVectorCatalogConsumerV1() {
		return raftplacement.CatalogMetaStatusV1{}, zero, ErrFixedPeerVectorUnavailableV1
	}
	reply, err := r.catalogConsumerCall(ctx, "vector-catalog-read", fixedPeerRequestV1{VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: action}})
	if err != nil {
		return reply.Catalog, zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if reply.VectorCatalog == nil {
		return reply.Catalog, zero, ErrFixedPeerVectorProofStaleV1
	}
	if err := r.validateImmutableVectorCatalogDecisionV1(reply.Catalog, *reply.VectorCatalog, action); err != nil {
		return reply.Catalog, zero, err
	}
	return reply.Catalog, *reply.VectorCatalog, nil
}

func (r *FixedPeerTCPRuntimeV1) validateImmutableVectorCatalogDecisionV1(status raftplacement.CatalogMetaStatusV1, record raftplacement.VectorPartitionLifecycleRecordV1, action fixedPeerVectorLifecycleActionV1) error {
	if r == nil || r.config.Vector == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	identity := r.config.Vector.Identity
	owners := fixedPeerVectorOwnerGroupsV1(r.config.Vector.Placement)
	if identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || status.AppliedIndex == 0 ||
		status.Epoch != identity.Index.CatalogEpoch || status.Digest != identity.Index.CatalogDigest ||
		record.Identity != identity || record.Aborted || record.InvalidationEpoch != 0 ||
		!slices.Equal(record.RequiredGroups, owners) || len(record.ReadyGroups) > len(owners) {
		return ErrFixedPeerVectorProofStaleV1
	}
	switch action {
	case fixedPeerVectorCatalogActiveV1:
		return r.validateImmutableActiveVectorRecordV1(record, owners)
	case fixedPeerVectorCatalogStageV1:
		switch record.State {
		case raftplacement.VectorPartitionLifecycleBuildingV1, raftplacement.VectorPartitionLifecycleStagedV1, raftplacement.VectorPartitionLifecyclePreparedV1:
			return nil
		case raftplacement.VectorPartitionLifecycleActiveV1:
			return r.validateImmutableActiveVectorRecordV1(record, owners)
		}
	}
	return ErrFixedPeerVectorProofStaleV1
}

// This handler uses only local quorum/apply fences: no nested control RPC can
// consume the bounded read pool held by its caller.
func (r *FixedPeerTCPRuntimeV1) localImmutableVectorCatalogV1(ctx context.Context, action fixedPeerVectorLifecycleActionV1) (raftplacement.CatalogMetaStatusV1, raftplacement.VectorPartitionLifecycleRecordV1, error) {
	var zero raftplacement.VectorPartitionLifecycleRecordV1
	if r == nil || r.config.Vector == nil || r.meta == nil || r.authority == nil ||
		r.client == nil || r.client.security == nil || (action != fixedPeerVectorCatalogStageV1 && action != fixedPeerVectorCatalogActiveV1) {
		return raftplacement.CatalogMetaStatusV1{}, zero, ErrFixedPeerVectorUnavailableV1
	}
	before, err := r.localCatalogFence(ctx)
	if err != nil {
		return before, zero, err
	}
	identity := r.config.Vector.Identity
	var record raftplacement.VectorPartitionLifecycleRecordV1
	if action == fixedPeerVectorCatalogActiveV1 {
		snapshot, err := r.authority.VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(ctx, before.AppliedIndex,
			identity.Index.Collection, identity.Index.IndexName, identity.Generation, identity.Index.IndexDefinitionDigest,
			identity.Source.Generation, identity.Source.Checksum, identity.Source.SchemaHash, identity.Source.RowCount)
		if err != nil || !sameScopedStageCatalogStatusV1(before, snapshot.Catalog) || snapshot.Identity != identity {
			return before, zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		record = snapshot.Record
	} else {
		var ok bool
		record, ok = r.authority.VectorPartitionLifecycleRecordV1(identity)
		after, err := r.localCatalogFence(ctx)
		if err != nil || !ok || !sameScopedStageCatalogStatusV1(before, after) {
			return before, zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
	}
	if err := r.validateImmutableVectorCatalogDecisionV1(before, record, action); err != nil {
		return before, zero, err
	}
	// Reply only with the immutable lifecycle fields needed by stage, serving,
	// and status. Mutable source-owner/planner/cleanup arrays are not authority
	// for this operation and must not inflate its bounded reply.
	return before, raftplacement.VectorPartitionLifecycleRecordV1{
		Identity: record.Identity, State: record.State, Revision: record.Revision, MutationEpoch: record.MutationEpoch,
		RequiredGroups: record.RequiredGroups, ReadyGroups: record.ReadyGroups, ReadySetDigest: record.ReadySetDigest,
		InvalidationEpoch: record.InvalidationEpoch, Aborted: record.Aborted,
	}, nil
}

func (r *FixedPeerTCPRuntimeV1) ValidateVectorPartitionGenerationSearchV1(ctx context.Context, collection raftplacement.CollectionRefV1, index string, generation uint64, definition string, sourceGeneration, sourceChecksum, sourceSchemaHash, sourceRowCount uint64) (string, error) {
	if r == nil || r.config.Vector == nil {
		return "", ErrFixedPeerVectorUnavailableV1
	}
	i := r.config.Vector.Identity
	if collection != i.Index.Collection || index != i.Index.IndexName || generation != i.Generation || definition != i.Index.IndexDefinitionDigest ||
		sourceGeneration != i.Source.Generation || sourceChecksum != i.Source.Checksum || sourceSchemaHash != i.Source.SchemaHash || sourceRowCount != i.Source.RowCount {
		return "", ErrFixedPeerVectorProofStaleV1
	}
	record, err := r.immutableActiveVectorRecordV1(ctx, fixedPeerVectorOwnerGroupsV1(r.config.Vector.Placement))
	return record.ReadySetDigest, err
}

var _ VectorPartitionReplicatedLifecycleAuthorityV1 = (*FixedPeerTCPRuntimeV1)(nil)
