package raftplacement

import (
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// The identity in BEGIN is not a READY grant. It permanently caps this exact
// operation at nonvoter preparation, including cold restore with compacted data.
func validateReplicaReplacementOwnerPreparationPhaseV1(record CatalogMetaRecordV1, lifecycle map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, state ReplicaReplacementStateV1) error {
	identity := state.Begin.OwnerPreparation
	if identity != nil {
		if replacementPhaseOrdinalV1(state.Phase) > replacementPhaseOrdinalV1(ReplicaReplacementAddIntentV1) {
			return ErrVectorPartitionLifecycleGuard
		}
		active, ok := lifecycle[*identity]
		if !ok || active.Identity != *identity || active.State != VectorPartitionLifecycleActiveV1 || identity.Immutable == (VectorPartitionLifecycleImmutableAuthorityV1{}) || identity.Index.CatalogEpoch != record.Epoch || identity.Index.CatalogDigest != record.Digest || state.Begin.ExpectedEpoch != record.Epoch || state.Begin.CatalogDigest != record.Digest || !slices.Contains(active.RequiredGroups, state.Begin.GroupID) {
			return ErrVectorPartitionLifecycleGuard
		}
		for _, existing := range lifecycle {
			if existing.State != VectorPartitionLifecycleAbsentV1 && (existing.State != VectorPartitionLifecycleActiveV1 || existing.Identity.Immutable == (VectorPartitionLifecycleImmutableAuthorityV1{})) {
				return ErrVectorPartitionLifecycleGuard
			}
		}
		for _, placement := range record.Catalog.Placements {
			if placement.GroupID == state.Begin.GroupID {
				return ErrVectorPartitionLifecycleGuard
			}
		}
	}
	// Existing owner operations without the explicit preparation cap remain closed.
	for _, active := range lifecycle {
		if slices.Contains(active.RequiredGroups, state.Begin.GroupID) && (identity == nil || *identity != active.Identity) {
			return ErrVectorPartitionLifecycleGuard
		}
	}
	return nil
}

func validateReplicaReplacementOwnerPreparationPhasesV1(record CatalogMetaRecordV1, lifecycle map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, replacements map[raftcluster.GroupID][]byte, fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1, barriers map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1) error {
	for _, raw := range replacements {
		state, err := decodeReplicaReplacementCurrentV1(raw)
		if err != nil {
			return err
		}
		if state.Phase != ReplicaReplacementCompletedV1 && catalogMetaFeatureEnabledV1(record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
			active := false
			for _, existing := range lifecycle {
				if existing.State == VectorPartitionLifecycleActiveV1 && existing.Identity.Immutable != (VectorPartitionLifecycleImmutableAuthorityV1{}) {
					active = true
					break
				}
			}
			if !active {
				return ErrVectorPartitionLifecycleGuard
			}
		}
		if state.Begin.OwnerPreparation != nil {
			for _, fence := range fences {
				if fence.Pending {
					return ErrVectorPartitionLifecycleGuard
				}
			}
			for _, barrier := range barriers {
				if barrier.Pending {
					return ErrVectorPartitionLifecycleGuard
				}
			}
		}
		if err := validateReplicaReplacementOwnerPreparationPhaseV1(record, lifecycle, state); err != nil {
			return err
		}
	}
	return nil
}
