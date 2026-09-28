package raftcluster

import (
	"context"
	"errors"

	hraft "github.com/hashicorp/raft"
)

// RemoveReplacementV1 uses a committed configuration CAS after promotion. Native
// Raft must commit the changed quorum; timeout is ambiguous, never rollback.
func (p *HashicorpRaftProvider) RemoveReplacementV1(ctx context.Context, old NodeID, target Peer, expected uint64) (CommittedRaftConfigurationV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CommittedRaftConfigurationV1{}, err
	}
	if old == "" || target.ID == "" || old == target.ID || expected == 0 {
		return CommittedRaftConfigurationV1{}, ErrInvalidConfig
	}
	configuration, err := p.CommittedConfigurationV1(ctx)
	if err != nil {
		return configuration, err
	}
	oldFound, targetFound := false, false
	for _, member := range configuration.Members {
		if !member.Voter {
			return configuration, ErrInvalidConfig
		}
		oldFound = oldFound || member.ID == old
		if member.ID == target.ID {
			if member.Address != target.Address {
				return configuration, ErrInvalidConfig
			}
			targetFound = true
		}
	}
	if !targetFound {
		return configuration, ErrInvalidConfig
	}
	if !oldFound {
		if configuration.ConfigurationIndex <= expected {
			return configuration, ErrInvalidConfig
		}
		return configuration, nil
	}
	if configuration.ConfigurationIndex != expected || len(configuration.Members) < 2 {
		return configuration, ErrInvalidConfig
	}
	if p.cluster.NodeID == old {
		if err := waitHashicorpRaftFuture(ctx, p.raft.LeadershipTransferToServer(hraft.ServerID(target.ID), hraft.ServerAddress(target.Address))); err != nil {
			return configuration, errors.Join(ErrHashicorpRaftUnavailable, err)
		}
		// The caller resolves the new leader and re-proves current membership/tail.
		return configuration, ErrReadBarrierNotSatisfied
	}
	if err := ctx.Err(); err != nil {
		return configuration, err
	}
	if err := waitHashicorpRaftFuture(ctx, p.raft.RemoveServer(hraft.ServerID(old), expected, p.applyTimeout)); err != nil {
		return configuration, errors.Join(ErrHashicorpRaftUnavailable, err)
	}
	return p.CommittedConfigurationV1(ctx)
}
