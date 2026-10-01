package raftplacement

import (
	"errors"
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
		if receipt := state.OwnerQualification; receipt != nil {
			if receipt.ReadySetDigest != active.ReadySetDigest {
				return ErrVectorPartitionLifecycleGuard
			}
			issuer, tailLeader := false, false
			if len(state.Peers) != 0 {
				for _, peer := range state.Peers {
					issuer = issuer || peer.ID == receipt.IssuerNode
					tailLeader = tailLeader || peer.ID == receipt.Tail.LeaderID
				}
			} else {
				for _, group := range record.Catalog.Groups {
					if group.ID == state.Begin.GroupID {
						for _, member := range group.Members {
							issuer = issuer || member == receipt.IssuerNode
							tailLeader = tailLeader || member == receipt.Tail.LeaderID
						}
					}
				}
			}
			if !issuer || !tailLeader {
				return ErrVectorPartitionLifecycleGuard
			}
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
	// Ordinary completed history predates later owner bindings; it grants no
	// authority to prepare that owner. Marked operations retain the cap above.
	if identity == nil && state.Phase == ReplicaReplacementCompletedV1 {
		return nil
	}
	// Existing owner operations without the explicit preparation cap remain closed.
	for _, active := range lifecycle {
		if slices.Contains(active.RequiredGroups, state.Begin.GroupID) && (identity == nil || *identity != active.Identity) {
			return ErrVectorPartitionLifecycleGuard
		}
	}
	return nil
}

func validateReplicaReplacementOwnerPreparationPhasesV1(record CatalogMetaRecordV1, resolved ResolvedCatalogV1, lifecycle map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, replacements map[raftcluster.GroupID][]byte, fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1, barriers map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1) error {
	for _, raw := range replacements {
		state, err := decodeReplicaReplacementCurrentV1(raw)
		if err != nil {
			return err
		}
		if state.Phase != ReplicaReplacementCompletedV1 && catalogMetaFeatureEnabledV1(record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
			if err := validateImmutableReplicaReplacementLifecycleV1(resolved, lifecycle, fences, barriers, state.Begin.GroupID); err != nil {
				return errors.Join(ErrVectorPartitionLifecycleGuard, err)
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
