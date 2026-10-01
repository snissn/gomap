package nativewire

import (
	"context"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// Catalog commit binds only evidence read back from the authorized production
// owner. A caller's self-consistent seed or installed boolean is not evidence.
func (r *FixedPeerTCPRuntimeV1) advanceReplacementV1(ctx context.Context, raw []byte, reply *fixedPeerReplyV1) error {
	next, err := raftplacement.DecodeReplicaReplacementStateV1(raw)
	if err != nil {
		return err
	}
	if next.OwnerQualification != nil {
		return raftplacement.ErrCatalogMetaConflict
	}
	if _, err := r.validateReplacementBeginV1(next.Begin); err != nil {
		return err
	}
	if r.meta == nil {
		return raftplacement.ErrCatalogMetaUnavailable
	}
	if _, err := r.replacementReadV1(ctx, next.Begin); err != nil {
		return err
	}
	current, err := r.authority.ReplicaReplacementStateV1(next.Begin.GroupID)
	if err != nil {
		return err
	}
	if next.Phase != raftplacement.ReplicaReplacementSeededV1 && next.Phase != raftplacement.ReplicaReplacementInstalledV1 && next.Phase != raftplacement.ReplicaReplacementAddIntentV1 {
		return raftplacement.ErrCatalogMetaConflict
	}
	if current.Phase == next.Phase && current.Seed != nil && raftcluster.SameReplacementSnapshotSeedV1(*current.Seed, *next.Seed) {
		reply.ReplacementState = &current
		return nil
	}
	beginRaw, err := raftplacement.EncodeReplicaReplacementBeginV1(next.Begin)
	if err != nil {
		return err
	}
	switch next.Phase {
	case raftplacement.ReplicaReplacementSeededV1:
		if current.Phase != raftplacement.ReplicaReplacementBegunV1 {
			return raftplacement.ErrCatalogMetaConflict
		}
		observed, err := r.client.call(ctx, next.Seed.SourceNodeID, "replacement-seed", fixedPeerRequestV1{Entry: beginRaw}, true)
		if err != nil {
			return err
		}
		if observed.ReplacementPending || observed.ReplacementSeed == nil || !raftcluster.SameReplacementSnapshotSeedV1(*observed.ReplacementSeed, *next.Seed) {
			return raftcluster.ErrAdmissionUnavailable
		}
	case raftplacement.ReplicaReplacementInstalledV1, raftplacement.ReplicaReplacementAddIntentV1:
		expected := raftplacement.ReplicaReplacementSeededV1
		if next.Phase == raftplacement.ReplicaReplacementAddIntentV1 {
			expected = raftplacement.ReplicaReplacementInstalledV1
		}
		if current.Phase != expected || current.Seed == nil || !raftcluster.SameReplacementSnapshotSeedV1(*current.Seed, *next.Seed) {
			return raftplacement.ErrCatalogMetaConflict
		}
		observed, err := r.client.call(ctx, next.Begin.NewPeer.ID, "replacement-receiver", fixedPeerRequestV1{Entry: beginRaw}, true)
		if err != nil {
			return err
		}
		if observed.ReplacementPending || !observed.ReplacementInstalled || observed.ReplacementSeed == nil || !raftcluster.SameReplacementSnapshotSeedV1(*observed.ReplacementSeed, *next.Seed) {
			return raftcluster.ErrAdmissionUnavailable
		}
	default:
		return raftplacement.ErrCatalogMetaConflict
	}
	if err := r.submitCatalogCommandV1(ctx, raw); err != nil {
		return err
	}
	current, err = r.authority.ReplicaReplacementStateV1(next.Begin.GroupID)
	if err == nil {
		reply.ReplacementState = &current
	}
	return err
}
