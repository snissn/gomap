package nativewire

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func replacementHasEnrollmentV1(phase raftplacement.ReplicaReplacementPhaseV1) bool {
	return phase == raftplacement.ReplicaReplacementAddIntentV1 || phase == raftplacement.ReplicaReplacementPromoteIntentV1 || phase == raftplacement.ReplicaReplacementPromotedV1 || phase == raftplacement.ReplicaReplacementRemoveIntentV1 || phase == raftplacement.ReplicaReplacementRemovedV1 || phase == raftplacement.ReplicaReplacementCompletedV1
}

// Every original voter is retained in this milestone. No unrelated membership
// transition, changed address, or unexpected extra voter can reconcile as ours.
func (r *FixedPeerTCPRuntimeV1) validateReplacementMembershipV1(state raftplacement.ReplicaReplacementStateV1, configuration raftcluster.CommittedRaftConfigurationV1) (bool, error) {
	group, err := r.replacementGroupV1(state, true)
	if err != nil {
		return false, err
	}
	if configuration.GroupID != group.ID || len(configuration.Members) != len(group.Peers) {
		return false, raftcluster.ErrInvalidConfig
	}
	targetVoter := false
	for _, peer := range group.Peers {
		found := false
		for _, member := range configuration.Members {
			if member.ID != peer.ID {
				continue
			}
			if member.Address != peer.Address || peer.ID != state.Begin.NewPeer.ID && !member.Voter {
				return false, raftcluster.ErrInvalidConfig
			}
			found = true
			if peer.ID == state.Begin.NewPeer.ID {
				targetVoter = member.Voter
			}
		}
		if !found {
			return false, raftcluster.ErrInvalidConfig
		}
	}
	return targetVoter, nil
}

func (r *FixedPeerTCPRuntimeV1) replacementTailV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) (raftcluster.ReplacementTailV1, error) {
	var tail raftcluster.ReplacementTailV1
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return tail, err
	}
	if !replacementHasEnrollmentV1(state.Phase) || state.Seed == nil {
		return tail, raftcluster.ErrAdmissionUnavailable
	}
	d := r.localDataV1(command.GroupID)
	if d == nil || d.startErr != nil {
		return tail, raftcluster.ErrRouteTargetUnknown
	}
	configuration, progress, err := d.provider.ReplacementReadFenceV1(ctx)
	if err != nil {
		return tail, err
	}
	voter, err := r.validateReplacementMembershipV1(state, configuration)
	if err != nil || voter {
		return tail, errors.Join(err, raftcluster.ErrAdmissionUnavailable)
	}
	proof, err := d.fsm.ReplacementTailProgressV1(ctx, raftentry.ApplyEntryID{Term: progress.Term, Index: progress.Index})
	if err != nil {
		return tail, err
	}
	tail = raftcluster.ReplacementTailV1{GroupID: command.GroupID, LeaderID: r.config.NodeID, LeaderTerm: configuration.Term, CommitIndex: configuration.CommitIndex, ConfigurationIndex: configuration.ConfigurationIndex, Progress: proof}
	if err := tail.Validate(); err != nil {
		return tail, err
	}
	candidate := state
	candidate.Phase, candidate.Tail = raftplacement.ReplicaReplacementPromoteIntentV1, &tail
	if err := r.checkReplacementTargetTailV1(ctx, candidate); err != nil {
		return tail, err
	}
	return tail, nil
}

func (r *FixedPeerTCPRuntimeV1) checkReplacementTargetTailV1(ctx context.Context, candidate raftplacement.ReplicaReplacementStateV1) error {
	raw, err := raftplacement.EncodeReplicaReplacementStateV1(candidate)
	if err != nil {
		return err
	}
	reply, err := r.client.call(ctx, candidate.Begin.NewPeer.ID, "replacement-tail-check", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil {
		return err
	}
	if reply.ReplacementTail == nil || *reply.ReplacementTail != *candidate.Tail {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	return nil
}

// Pure target inspection: an authenticated request can ask about a semantic
// boundary but cannot advance authority, reinstall a seed, or create a voter.
func (r *FixedPeerTCPRuntimeV1) verifyReplacementTargetTailV1(ctx context.Context, raw []byte, reply *fixedPeerReplyV1) error {
	candidate, err := raftplacement.DecodeReplicaReplacementStateV1(raw)
	if err != nil || candidate.Tail == nil {
		return errors.Join(err, raftcluster.ErrInvalidConfig)
	}
	command := candidate.Begin
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	if !replacementHasEnrollmentV1(state.Phase) || state.Seed == nil || !raftcluster.SameReplacementSnapshotSeedV1(*state.Seed, *candidate.Seed) {
		return raftcluster.ErrAdmissionUnavailable
	}
	if err := r.replacementReceiverCutoffV1(ctx, command, reply); err != nil {
		return err
	}
	d := r.localDataV1(command.GroupID)
	status, err := d.fsm.RecoveryStatusV1(ctx, raftfsm.RecoveryStatusOptionsV1{SnapshotManifest: &state.Seed.Manifest, TailTargetIndex: candidate.Tail.Progress.EntryID.Index})
	if err != nil {
		return err
	}
	if status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 || status.TailState != raftcluster.RecoveryTailStateCompleteV1 {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	proof, err := d.fsm.ReplacementTailProgressV1(ctx, candidate.Tail.Progress.EntryID)
	if err != nil {
		return err
	}
	if proof != candidate.Tail.Progress {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	reply.ReplacementTail = candidate.Tail
	return nil
}

// The catalog coordinator derives the complete tail itself; generic advance
// cannot turn a caller's self-consistent tail into promotion authority.
func (r *FixedPeerTCPRuntimeV1) replacementPromotionIntentV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	if r.meta == nil {
		return raftplacement.ErrCatalogMetaUnavailable
	}
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	state, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	if state.Phase == raftplacement.ReplicaReplacementPromoteIntentV1 || state.Phase == raftplacement.ReplicaReplacementPromotedV1 {
		reply.ReplacementState = &state
		return nil
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 {
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
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	observed, err := r.client.call(ctx, leader, "replacement-tail", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return err
	}
	if observed.ReplacementTail == nil || observed.ReplacementTail.LeaderID != leader {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	state.Phase, state.Tail = raftplacement.ReplicaReplacementPromoteIntentV1, observed.ReplacementTail
	encoded, err := raftplacement.EncodeReplicaReplacementStateV1(state)
	if err != nil {
		return err
	}
	if err := r.submitCatalogCommandV1(ctx, encoded); err != nil {
		return err
	}
	committed, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	reply.ReplacementState = &committed
	return err
}

func (r *FixedPeerTCPRuntimeV1) promoteReplacementV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	if (state.Phase != raftplacement.ReplicaReplacementPromoteIntentV1 && state.Phase != raftplacement.ReplicaReplacementPromotedV1) || state.Tail == nil {
		return raftcluster.ErrAdmissionUnavailable
	}
	d := r.localDataV1(command.GroupID)
	if d == nil || d.startErr != nil {
		return raftcluster.ErrRouteTargetUnknown
	}
	configuration, err := d.provider.CommittedConfigurationV1(ctx)
	if err != nil {
		return err
	}
	voter, err := r.validateReplacementMembershipV1(state, configuration)
	if err != nil {
		return err
	}
	if voter {
		if configuration.ConfigurationIndex <= state.Tail.ConfigurationIndex {
			return raftcluster.ErrInvalidConfig
		}
		reply.Membership = &configuration
		return nil
	}
	if state.Phase == raftplacement.ReplicaReplacementPromotedV1 || configuration.ConfigurationIndex != state.Tail.ConfigurationIndex {
		return raftcluster.ErrInvalidConfig
	}
	// On retry/restart, do not mistake an old committed intent for a fresh tail.
	// Re-prove the current leader prefix and target readiness before the native CAS.
	fresh, err := r.replacementTailV1(ctx, command)
	if err != nil {
		return err
	}
	if fresh.ConfigurationIndex != state.Tail.ConfigurationIndex || fresh.Progress.EntryID.Index < state.Tail.Progress.EntryID.Index {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	promoted, err := d.provider.PromoteReplacementV1(ctx, command.OldNodeID, command.NewPeer, state.Tail.ConfigurationIndex)
	if err != nil {
		return err
	}
	voter, err = r.validateReplacementMembershipV1(state, promoted)
	if err != nil || !voter {
		return errors.Join(err, raftcluster.ErrReadBarrierNotSatisfied)
	}
	reply.Membership = &promoted
	return nil
}

func (r *FixedPeerTCPRuntimeV1) completeReplacementPromotionV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	if r.meta == nil {
		return raftplacement.ErrCatalogMetaUnavailable
	}
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	state, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	if state.Phase != raftplacement.ReplicaReplacementPromoteIntentV1 && state.Phase != raftplacement.ReplicaReplacementPromotedV1 {
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
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	observed, err := r.client.call(ctx, leader, "replacement-promote", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return err
	}
	if observed.Membership == nil {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	voter, err := r.validateReplacementMembershipV1(state, *observed.Membership)
	if err != nil || !voter {
		return errors.Join(err, raftcluster.ErrReadBarrierNotSatisfied)
	}
	if state.Phase != raftplacement.ReplicaReplacementPromotedV1 {
		state.Phase = raftplacement.ReplicaReplacementPromotedV1
		encoded, err := raftplacement.EncodeReplicaReplacementStateV1(state)
		if err != nil {
			return err
		}
		if err := r.submitCatalogCommandV1(ctx, encoded); err != nil {
			return err
		}
	}
	reply.Membership = observed.Membership
	return nil
}

// PromoteReplicaReplacementV1 verifies durable snapshot+tail, commits intent,
// then reconciles native promotion. It deliberately retains the old voter and
// does not alter catalog placement or claim completed replica replacement.
func (c *FixedPeerTCPClientV1) PromoteReplicaReplacementV1(ctx context.Context, catalogLeader raftcluster.NodeID, command raftplacement.ReplicaReplacementBeginV1) (raftcluster.CommittedRaftConfigurationV1, error) {
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if command.ConfigDigest != c.digest {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
	}
	read, err := c.call(ctx, catalogLeader, "replacement-read", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if read.ReplacementState == nil || !replacementHasEnrollmentV1(read.ReplacementState.Phase) {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrAdmissionUnavailable
	}
	var group FixedPeerTCPGroupV1
	for _, candidate := range c.config.Groups {
		if candidate.ID == command.GroupID {
			group = candidate
			break
		}
	}
	if group.ID == "" {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrRouteTargetUnknown
	}
	group, err = replacementGroupPeersV1(group, *read.ReplacementState, false)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if err := c.prepareReplacementPeersV1(ctx, group, command, raw); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if _, err := c.call(ctx, catalogLeader, "replacement-promotion-intent", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	reply, err := c.call(ctx, catalogLeader, "replacement-complete-promotion", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if reply.Membership == nil {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
	}
	return *reply.Membership, nil
}
