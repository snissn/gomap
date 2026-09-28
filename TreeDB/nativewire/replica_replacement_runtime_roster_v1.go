package nativewire

import (
	"context"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// The immutable manifest anchors global node identities/features. The bounded
// committed current roster supplies addresses after a completed replacement.
func (r *FixedPeerTCPRuntimeV1) replacementGroupV1(state raftplacement.ReplicaReplacementStateV1, includeTarget bool) (FixedPeerTCPGroupV1, error) {
	group, err := r.validateReplacementBeginV1(state.Begin)
	if err != nil {
		return group, err
	}
	group, err = replacementGroupPeersV1(group, state, includeTarget)
	if err != nil {
		return group, err
	}
	for _, peer := range group.Peers {
		if r.client.addresses[peer.ID] == "" {
			return group, raftcluster.ErrInvalidConfig
		}
	}
	return group, nil
}

func replacementGroupPeersV1(group FixedPeerTCPGroupV1, state raftplacement.ReplicaReplacementStateV1, includeTarget bool) (FixedPeerTCPGroupV1, error) {
	if group.ID != state.Begin.GroupID {
		return group, raftcluster.ErrInvalidConfig
	}
	if len(state.Peers) > 0 {
		group.Peers = slices.Clone(state.Peers)
	} else {
		group.Peers = slices.Clone(group.Peers)
	}
	old, target := false, false
	for _, peer := range group.Peers {
		old = old || peer.ID == state.Begin.OldNodeID
		if peer.ID == state.Begin.NewPeer.ID {
			if peer.Address != state.Begin.NewPeer.Address {
				return group, raftcluster.ErrInvalidConfig
			}
			target = true
		} else if peer.Address == state.Begin.NewPeer.Address {
			return group, raftcluster.ErrInvalidConfig
		}
	}
	if state.Phase == raftplacement.ReplicaReplacementCompletedV1 {
		if old || !target {
			return group, raftcluster.ErrInvalidConfig
		}
	} else {
		if !old || target {
			return group, raftcluster.ErrInvalidConfig
		}
		if includeTarget {
			group.Peers = append(group.Peers, state.Begin.NewPeer)
		}
	}
	if len(group.Peers) > fixedPeerMaxPeersV1 {
		return group, raftcluster.ErrInvalidConfig
	}
	return group, nil
}

func (r *FixedPeerTCPRuntimeV1) currentReplicaGroupReplyV1(id raftcluster.GroupID, reply *fixedPeerReplyV1) error {
	current, state, status, err := r.authority.CurrentReplicaGroupV1(id)
	if err != nil {
		return err
	}
	var group FixedPeerTCPGroupV1
	for _, fixed := range r.config.Groups {
		if fixed.ID == id {
			group = fixed
			break
		}
	}
	if group.ID == "" {
		return raftcluster.ErrRouteTargetUnknown
	}
	if state != nil && len(state.Peers) > 0 {
		group.Peers = slices.Clone(state.Peers)
	}
	ids := make([]raftcluster.NodeID, len(group.Peers))
	for i, peer := range group.Peers {
		if r.client.addresses[peer.ID] == "" {
			return raftcluster.ErrInvalidConfig
		}
		ids[i] = peer.ID
	}
	slices.Sort(ids)
	expected := slices.Clone(current.Members)
	slices.Sort(expected)
	if !slices.Equal(ids, expected) {
		return raftcluster.ErrInvalidConfig
	}
	reply.Catalog, reply.CurrentGroup, reply.ReplacementState = status, &group, state
	return nil
}

func (r *FixedPeerTCPRuntimeV1) currentReplicaGroupV1(ctx context.Context, id raftcluster.GroupID) (FixedPeerTCPGroupV1, *raftplacement.ReplicaReplacementStateV1, error) {
	reply, err := r.catalogConsumerCall(ctx, "catalog-read", fixedPeerRequestV1{Metadata: raftentry.RequestMetadataV1{ClusterRouteGroupID: string(id)}})
	if err != nil {
		return FixedPeerTCPGroupV1{}, nil, err
	}
	if reply.CurrentGroup == nil || reply.CurrentGroup.ID != id {
		return FixedPeerTCPGroupV1{}, nil, raftcluster.ErrInvalidConfig
	}
	return *reply.CurrentGroup, reply.ReplacementState, nil
}
