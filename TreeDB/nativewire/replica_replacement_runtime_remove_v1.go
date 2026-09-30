package nativewire

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func replacementFinalGroupV1(group FixedPeerTCPGroupV1, command raftplacement.ReplicaReplacementBeginV1) FixedPeerTCPGroupV1 {
	group.Peers = slices.Clone(group.Peers)
	group.Peers = slices.DeleteFunc(group.Peers, func(p raftcluster.Peer) bool { return p.ID == command.OldNodeID })
	slices.SortFunc(group.Peers, func(a, b raftcluster.Peer) int { return strings.Compare(string(a.ID), string(b.ID)) })
	return group
}
func validateReplacementFinalMembershipV1(group FixedPeerTCPGroupV1, configuration raftcluster.CommittedRaftConfigurationV1, state raftplacement.ReplicaReplacementStateV1) error {
	if configuration.ConfigurationIndex <= state.RemovalIndex || state.Result != nil && configuration.ConfigurationIndex != state.Result.ConfigurationIndex {
		return raftcluster.ErrInvalidConfig
	}
	if configuration.GroupID != group.ID || len(configuration.Members) != len(group.Peers) {
		return raftcluster.ErrInvalidConfig
	}
	for _, peer := range group.Peers {
		found := false
		for _, member := range configuration.Members {
			if member.ID == peer.ID && member.Address == peer.Address && member.Voter {
				found = true
				break
			}
		}
		if !found {
			return raftcluster.ErrInvalidConfig
		}
	}
	return nil
}

// Prove the surviving target's durable prefix again immediately before removal.
// Native configuration commitment remains the authority for quorum feasibility.
func (r *FixedPeerTCPRuntimeV1) replacementRemovalProofV1(ctx context.Context, state raftplacement.ReplicaReplacementStateV1) (raftcluster.CommittedRaftConfigurationV1, error) {
	if state.Begin.OwnerPreparation != nil || r.config.Vector != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrUnsupportedFeature
	}
	var empty raftcluster.CommittedRaftConfigurationV1
	if state.Phase != raftplacement.ReplicaReplacementPromotedV1 && state.Phase != raftplacement.ReplicaReplacementRemoveIntentV1 {
		return empty, raftcluster.ErrAdmissionUnavailable
	}
	d := r.localDataV1(state.Begin.GroupID)
	if d == nil || d.startErr != nil {
		return empty, raftcluster.ErrRouteTargetUnknown
	}
	configuration, progress, err := d.provider.ReplacementReadFenceV1(ctx)
	if err != nil {
		return configuration, err
	}
	voter, err := r.validateReplacementMembershipV1(state, configuration)
	if err != nil || !voter {
		return configuration, errors.Join(err, raftcluster.ErrReadBarrierNotSatisfied)
	}
	proof, err := d.fsm.ReplacementTailProgressV1(ctx, raftentry.ApplyEntryID{Term: progress.Term, Index: progress.Index})
	if err != nil {
		return configuration, err
	}
	candidate := state
	candidate.Phase, candidate.RemovalIndex, candidate.Result = raftplacement.ReplicaReplacementPromoteIntentV1, 0, nil
	candidate.Tail = &raftcluster.ReplacementTailV1{GroupID: state.Begin.GroupID, LeaderID: r.config.NodeID, LeaderTerm: configuration.Term, CommitIndex: configuration.CommitIndex, ConfigurationIndex: configuration.ConfigurationIndex, Progress: proof}
	if err := r.checkReplacementTargetTailV1(ctx, candidate); err != nil {
		return configuration, err
	}
	return configuration, nil
}

func (r *FixedPeerTCPRuntimeV1) replacementRemovalIntentV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	if command.OwnerPreparation != nil || r.config.Vector != nil {
		return raftcluster.ErrUnsupportedFeature
	}
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	state, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	switch state.Phase {
	case raftplacement.ReplicaReplacementRemoveIntentV1, raftplacement.ReplicaReplacementRemovedV1, raftplacement.ReplicaReplacementCompletedV1:
		reply.ReplacementState = &state
		return nil
	case raftplacement.ReplicaReplacementPromotedV1:
	default:
		return raftcluster.ErrAdmissionUnavailable
	}
	group, err := r.replacementGroupV1(state, true)
	if err != nil {
		return err
	}
	leader, err := r.client.leader(ctx, group)
	if err != nil {
		return err
	}
	raw, _ := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err := r.client.prepareReplacementSelectedLeaderV1(ctx, leader, raw); err != nil {
		return err
	}
	observed, err := r.client.call(ctx, leader, "replacement-removal-proof", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil {
		return err
	}
	if observed.Membership == nil {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	voter, err := r.validateReplacementMembershipV1(state, *observed.Membership)
	if err != nil || !voter || observed.Membership.ConfigurationIndex <= state.Tail.ConfigurationIndex {
		return errors.Join(err, raftcluster.ErrReadBarrierNotSatisfied)
	}
	state.Phase, state.RemovalIndex = raftplacement.ReplicaReplacementRemoveIntentV1, observed.Membership.ConfigurationIndex
	encoded, err := raftplacement.EncodeReplicaReplacementStateV1(state)
	if err != nil {
		return err
	}
	if err := r.submitCatalogCommandV1(ctx, encoded); err != nil {
		return err
	}
	reply.ReplacementState = &state
	return nil
}

func (r *FixedPeerTCPRuntimeV1) removeReplacementV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	if command.OwnerPreparation != nil || r.config.Vector != nil {
		return raftcluster.ErrUnsupportedFeature
	}
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	if state.Phase != raftplacement.ReplicaReplacementRemoveIntentV1 && state.Phase != raftplacement.ReplicaReplacementRemovedV1 && state.Phase != raftplacement.ReplicaReplacementCompletedV1 {
		return raftcluster.ErrAdmissionUnavailable
	}
	d := r.localDataV1(command.GroupID)
	if d == nil || d.startErr != nil {
		return raftcluster.ErrRouteTargetUnknown
	}
	group, err := r.replacementGroupV1(state, true)
	if err != nil {
		return err
	}
	final := replacementFinalGroupV1(group, command)
	configuration, err := d.provider.CommittedConfigurationV1(ctx)
	if err != nil {
		return err
	}
	if validateReplacementFinalMembershipV1(final, configuration, state) == nil {
		reply.Membership = &configuration
		return nil
	}
	if state.Phase != raftplacement.ReplicaReplacementRemoveIntentV1 || configuration.ConfigurationIndex != state.RemovalIndex {
		return raftcluster.ErrInvalidConfig
	}
	if _, err := r.replacementRemovalProofV1(ctx, state); err != nil {
		return err
	}
	removed, err := d.provider.RemoveReplacementV1(ctx, command.OldNodeID, command.NewPeer, state.RemovalIndex)
	if err != nil {
		return err
	}
	if err := validateReplacementFinalMembershipV1(final, removed, state); err != nil {
		return err
	}
	reply.Membership = &removed
	return nil
}

func (r *FixedPeerTCPRuntimeV1) completeReplacementV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	if command.OwnerPreparation != nil || r.config.Vector != nil {
		return raftcluster.ErrUnsupportedFeature
	}
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	state, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	if state.Phase != raftplacement.ReplicaReplacementRemoveIntentV1 && state.Phase != raftplacement.ReplicaReplacementRemovedV1 && state.Phase != raftplacement.ReplicaReplacementCompletedV1 {
		return raftcluster.ErrAdmissionUnavailable
	}
	group, err := r.replacementGroupV1(state, true)
	if err != nil {
		return err
	}
	leader, err := r.client.leader(ctx, group)
	if err != nil {
		return err
	}
	raw, _ := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err := r.client.prepareReplacementSelectedLeaderV1(ctx, leader, raw); err != nil {
		return err
	}
	observed, err := r.client.call(ctx, leader, "replacement-remove", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return err
	}
	final := replacementFinalGroupV1(group, command)
	if observed.Membership == nil {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	if err := validateReplacementFinalMembershipV1(final, *observed.Membership, state); err != nil {
		return err
	}
	if state.Phase == raftplacement.ReplicaReplacementRemoveIntentV1 {
		state.Phase = raftplacement.ReplicaReplacementRemovedV1
		state.Result = &raftplacement.ReplicaReplacementResultV1{ConfigurationIndex: observed.Membership.ConfigurationIndex}
		encoded, err := raftplacement.EncodeReplicaReplacementStateV1(state)
		if err != nil {
			return err
		}
		if err := r.submitCatalogCommandV1(ctx, encoded); err != nil {
			return err
		}
	}
	if state.Phase == raftplacement.ReplicaReplacementRemovedV1 {
		if observed.Membership.ConfigurationIndex != state.Result.ConfigurationIndex {
			return raftcluster.ErrInvalidConfig
		}
		// The native runtime carries normalized capabilities; catalog completion
		// records only the identity/address roster after the default-capability
		// admission check. Never relax the durable state's strict decoder.
		catalogPeers := make([]raftcluster.Peer, len(final.Peers))
		for i, peer := range final.Peers {
			catalogPeers[i] = raftcluster.Peer{ID: peer.ID, Address: peer.Address}
		}
		completion, err := r.authority.ReplacementCompletionV1(command.GroupID, catalogPeers)
		if err != nil {
			return err
		}
		encoded, err := raftplacement.EncodeReplicaReplacementCompleteV1(completion)
		if err != nil {
			return err
		}
		if err := r.submitCatalogCommandV1(ctx, encoded); err != nil {
			return err
		}
		state = completion.State
	}
	reply.ReplacementState, reply.Membership = &state, observed.Membership
	return nil
}

// CompleteReplicaReplacementV1 removes only after committed promotion and a
// fresh surviving target proof, then atomically substitutes catalog membership.
// Placement never changes. The original operation remains the exact retry key.
// A caller must rediscover the catalog leader and retry that exact operation
// after ErrNotLeader; this method cannot safely retry a fixed leader address.
func (c *FixedPeerTCPClientV1) CompleteReplicaReplacementV1(ctx context.Context, catalogLeader raftcluster.NodeID, command raftplacement.ReplicaReplacementBeginV1) (raftcluster.CommittedRaftConfigurationV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	read, err := c.call(ctx, catalogLeader, "replacement-read", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil || read.ReplacementState == nil {
		return raftcluster.CommittedRaftConfigurationV1{}, errors.Join(err, raftcluster.ErrAdmissionUnavailable)
	}
	var group FixedPeerTCPGroupV1
	for _, g := range c.config.Groups {
		if g.ID == command.GroupID {
			group = g
			break
		}
	}
	group, err = replacementGroupPeersV1(group, *read.ReplacementState, false)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if err := c.prepareReplacementPeersV1(ctx, group, command, raw); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if _, err := c.call(ctx, catalogLeader, "replacement-removal-intent", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	for {
		reply, err := c.call(ctx, catalogLeader, "replacement-complete", fixedPeerRequestV1{Entry: raw}, true)
		if err == nil {
			if reply.Membership == nil || reply.ReplacementState == nil || reply.ReplacementState.Phase != raftplacement.ReplicaReplacementCompletedV1 {
				return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
			}
			// Reconciliation is independently fenced at each node. An offline removed
			// member is not a completion dependency; its restart remains unready.
			for _, peer := range append(slices.Clone(reply.ReplacementState.Peers), raftcluster.Peer{ID: command.OldNodeID}) {
				_, reconcileErr := c.call(ctx, peer.ID, "replacement-reconcile", fixedPeerRequestV1{Entry: raw}, true)
				if reconcileErr != nil && peer.ID != command.OldNodeID {
					return *reply.Membership, reconcileErr
				}
			}
			return *reply.Membership, nil
		}
		if errors.Is(err, raftcluster.ErrNotLeader) {
			return raftcluster.CommittedRaftConfigurationV1{}, err
		}
		if !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) && !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
			return raftcluster.CommittedRaftConfigurationV1{}, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return raftcluster.CommittedRaftConfigurationV1{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// Completion is a quorum-fenced current-roster fact. Removed processes remain
// inspectable but cannot accept group traffic; readiness and route fences also
// refuse them after an offline restart from pre-removal native state.
func (r *FixedPeerTCPRuntimeV1) reconcileReplacementV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	if command.OwnerPreparation != nil || r.config.Vector != nil {
		return raftcluster.ErrUnsupportedFeature
	}
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	if state.Phase != raftplacement.ReplicaReplacementCompletedV1 {
		return raftcluster.ErrAdmissionUnavailable
	}
	group, err := r.replacementGroupV1(state, false)
	if err != nil {
		return err
	}
	r.groupsMu.RLock()
	defer r.groupsMu.RUnlock()
	if r.closed.Load() || r.draining.Load() {
		return raftcluster.ErrAdmissionUnavailable
	}
	d := r.data[group.ID]
	if d == nil || d.startErr != nil {
		return raftcluster.ErrRouteTargetUnknown
	}
	if err := d.stream.replaceAuthorizedPeersV1(group.Peers); err != nil {
		return err
	}
	if closer, ok := d.transport.(interface{ CloseStreams() }); ok {
		closer.CloseStreams()
	}
	if r.config.NodeID == command.OldNodeID {
		return d.stream.Close()
	}
	return nil
}
