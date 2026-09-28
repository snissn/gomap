package raftcluster

import (
	"context"
	"errors"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// ReplacementTailProgressV1 is replica-independent durable command identity.
// Local command-WAL offsets are validated by each FSM and never compared across
// replicas. ProgressDigest describes this log position, including idempotent
// replay, which can differ from the original operation's ResultDigest.
type ReplacementTailProgressV1 struct {
	EntryID        raftentry.ApplyEntryID    `json:"entry_id"`
	CommandDigest  raftentry.CommandDigestV1 `json:"command_digest"`
	ProgressDigest raftentry.CommandDigestV1 `json:"progress_digest"`
	Result         raftentry.ApplyResultV1   `json:"result"`
}

func (p ReplacementTailProgressV1) Validate() error {
	if p.EntryID.Term == 0 || p.EntryID.Index == 0 || p.CommandDigest == (raftentry.CommandDigestV1{}) || p.ProgressDigest == (raftentry.CommandDigestV1{}) || p.Result.CommandDigest != p.CommandDigest || p.Result.ResultDigest == (raftentry.CommandDigestV1{}) {
		return ErrInvalidConfig
	}
	return nil
}

// ReplacementTailV1 binds a durable command to a current-term committed native
// prefix. CommitIndex includes configuration/no-op entries; Progress does not.
type ReplacementTailV1 struct {
	GroupID            GroupID                   `json:"group_id"`
	LeaderID           NodeID                    `json:"leader_id"`
	LeaderTerm         uint64                    `json:"leader_term"`
	CommitIndex        uint64                    `json:"commit_index"`
	ConfigurationIndex uint64                    `json:"configuration_index"`
	Progress           ReplacementTailProgressV1 `json:"progress"`
}

func (t ReplacementTailV1) Validate() error {
	if t.GroupID == "" || t.LeaderID == "" || t.LeaderTerm == 0 || t.ConfigurationIndex == 0 || t.ConfigurationIndex > t.CommitIndex || t.Progress.EntryID.Index > t.CommitIndex || t.Progress.EntryID.Term > t.LeaderTerm {
		return ErrInvalidConfig
	}
	return t.Progress.Validate()
}

// ReplacementReadFenceV1 reuses the ReadIndex command-free-gap proof while
// preserving the raw committed/configuration positions needed for membership.
func (p *HashicorpRaftProvider) ReplacementReadFenceV1(ctx context.Context) (CommittedRaftConfigurationV1, AppliedProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	configuration, err := p.CommittedConfigurationV1(ctx)
	if err != nil {
		return configuration, AppliedProgress{}, err
	}
	progress, err := p.waitTreeDBAppliedReadPrefix(ctx, configuration.CommitIndex)
	if err != nil {
		return configuration, progress, err
	}
	// The FSM may advance beyond the captured commit while waiting. That progress
	// is committed too; keep the raw fence large enough to describe the evidence.
	if progress.Index > configuration.CommitIndex {
		configuration.CommitIndex = progress.Index
	}
	if err := p.requireHashicorpReadIndexLeaderTerm(configuration.Term); err != nil {
		return configuration, progress, err
	}
	current := p.raft.GetConfiguration()
	if err := waitHashicorpRaftFuture(ctx, current); err != nil {
		return configuration, progress, err
	}
	if !replacementConfigurationMatchesV1(configuration, current.Configuration()) {
		return configuration, progress, ErrReadBarrierNotSatisfied
	}
	return configuration, progress, nil
}

func replacementConfigurationMatchesV1(expected CommittedRaftConfigurationV1, actual hraft.Configuration) bool {
	if len(expected.Members) != len(actual.Servers) {
		return false
	}
	for _, server := range actual.Servers {
		found := false
		for _, member := range expected.Members {
			if member.ID == NodeID(server.ID) && member.Address == string(server.Address) && (server.Suffrage == hraft.Voter || server.Suffrage == hraft.Nonvoter) && member.Voter == (server.Suffrage == hraft.Voter) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// PromoteReplacementV1 is reachable only after the runtime independently reads
// committed promotion intent and verifies the target's durable tail. A timeout
// is ambiguous; callers reconcile committed configuration and never demote.
func (p *HashicorpRaftProvider) PromoteReplacementV1(ctx context.Context, old NodeID, peer Peer, expectedConfigurationIndex uint64) (CommittedRaftConfigurationV1, error) {
	if old == "" || old == peer.ID || peer.ID == "" || peer.Address == "" || expectedConfigurationIndex == 0 {
		return CommittedRaftConfigurationV1{}, ErrInvalidConfig
	}
	configuration, err := p.CommittedConfigurationV1(ctx)
	if err != nil {
		return configuration, err
	}
	oldVoter, targetFound, targetVoter := false, false, false
	for _, member := range configuration.Members {
		if member.ID == old {
			oldVoter = member.Voter
		}
		if member.ID == peer.ID {
			if member.Address != peer.Address {
				return configuration, ErrInvalidConfig
			}
			targetFound, targetVoter = true, member.Voter
		} else if member.Address == peer.Address {
			return configuration, ErrInvalidConfig
		}
	}
	if !oldVoter || !targetFound {
		return configuration, ErrInvalidConfig
	}
	if targetVoter {
		if configuration.ConfigurationIndex <= expectedConfigurationIndex {
			return configuration, ErrInvalidConfig
		}
		return configuration, nil
	}
	if configuration.ConfigurationIndex != expectedConfigurationIndex {
		return configuration, ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return configuration, err
	}
	if err := waitHashicorpRaftFuture(ctx, p.raft.AddVoter(hraft.ServerID(peer.ID), hraft.ServerAddress(peer.Address), expectedConfigurationIndex, p.applyTimeout)); err != nil {
		return configuration, errors.Join(ErrHashicorpRaftUnavailable, err)
	}
	return p.CommittedConfigurationV1(ctx)
}
