package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	hraft "github.com/hashicorp/raft"
)

// RaftMemberV1 reports actual Raft suffrage, not the configured routing list.
// Staging and unknown suffrage fail closed at the replacement boundary.
type RaftMemberV1 struct {
	ID      NodeID
	Address string
	Voter   bool
}

// CommittedRaftConfigurationV1 binds a latest configuration to a quorum-proved
// current-term committed prefix. GetConfiguration alone can report an entry
// which has not committed yet. CommandIndex is deliberately not substituted
// for CommitIndex: configuration/no-op entries need not call the TreeDB FSM.
type CommittedRaftConfigurationV1 struct {
	GroupID            GroupID
	LeaderID           NodeID
	Term               uint64
	CommitIndex        uint64
	ConfigurationIndex uint64
	Members            []RaftMemberV1
}

func (p *HashicorpRaftProvider) CommittedConfigurationV1(ctx context.Context) (CommittedRaftConfigurationV1, error) {
	if p == nil || p.raft == nil {
		return CommittedRaftConfigurationV1{}, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := p.applyTimeout
	if timeout <= 0 {
		timeout = hashicorpRaftDefaultApplyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := p.requireHashicorpReadIndexLeader(); err != nil {
		return CommittedRaftConfigurationV1{}, err
	}
	if err := waitHashicorpRaftFuture(ctx, p.raft.VerifyLeader()); err != nil {
		return CommittedRaftConfigurationV1{}, p.mapHashicorpRaftReadIndexError(err)
	}
	term, _, err := p.waitHashicorpReadIndexCurrentTermCommit(ctx)
	if err != nil {
		return CommittedRaftConfigurationV1{}, err
	}
	for {
		if err := p.requireHashicorpReadIndexLeaderTerm(term); err != nil {
			return CommittedRaftConfigurationV1{}, err
		}
		index, encoded, last, err := p.persistedConfigurationV1(ctx)
		if err != nil && !errors.Is(err, hraft.ErrLogNotFound) {
			return CommittedRaftConfigurationV1{}, err
		}
		future := p.raft.GetConfiguration()
		if err := waitHashicorpRaftFuture(ctx, future); err != nil {
			return CommittedRaftConfigurationV1{}, err
		}
		commit := p.raft.CommitIndex()
		lastNow, lastErr := p.logStore.LastIndex()
		if lastErr != nil {
			return CommittedRaftConfigurationV1{}, lastErr
		}
		if err == nil && last == lastNow && index != 0 && index <= commit && bytes.Equal(encoded, hraft.EncodeConfiguration(future.Configuration())) {
			configuration := future.Configuration()
			result := CommittedRaftConfigurationV1{GroupID: p.cluster.GroupID, LeaderID: p.cluster.NodeID, Term: term, CommitIndex: commit, ConfigurationIndex: index}
			for _, server := range configuration.Servers {
				if server.Suffrage != hraft.Voter && server.Suffrage != hraft.Nonvoter {
					return CommittedRaftConfigurationV1{}, fmt.Errorf("%w: unsupported replacement suffrage", ErrInvalidHashicorpRaftProvider)
				}
				result.Members = append(result.Members, RaftMemberV1{ID: NodeID(server.ID), Address: string(server.Address), Voter: server.Suffrage == hraft.Voter})
			}
			sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].ID < result.Members[j].ID })
			if err := p.requireHashicorpReadIndexLeaderTerm(term); err != nil {
				return CommittedRaftConfigurationV1{}, err
			}
			return result, nil
		}
		timer := time.NewTimer(hashicorpRaftReadIndexInitialPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return CommittedRaftConfigurationV1{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// AddReplacementNonvoterV1 is the internal consensus mutation used after a
// committed catalog BEGIN has authorized this exact old/new identity. It does
// not expose AddVoter: snapshot installation and durable tail verification are
// separate prerequisites. A pre-existing voter is rejected, since HashiCorp's
// AddNonvoter preserves voter suffrage rather than demoting it.
func (p *HashicorpRaftProvider) AddReplacementNonvoterV1(ctx context.Context, old NodeID, peer Peer, expectedConfigurationIndex uint64) (CommittedRaftConfigurationV1, error) {
	if old == "" || peer.ID == "" || old == peer.ID || peer.Address == "" || expectedConfigurationIndex == 0 {
		return CommittedRaftConfigurationV1{}, ErrInvalidConfig
	}
	configuration, err := p.CommittedConfigurationV1(ctx)
	if err != nil {
		return CommittedRaftConfigurationV1{}, err
	}
	if configuration.ConfigurationIndex != expectedConfigurationIndex {
		return CommittedRaftConfigurationV1{}, errors.Join(ErrInvalidConfig, fmt.Errorf("replacement configuration changed"))
	}
	foundOld, foundNew := false, false
	for _, member := range configuration.Members {
		if member.ID == old {
			foundOld = member.Voter
		}
		if member.ID == peer.ID {
			if member.Voter || member.Address != peer.Address {
				return CommittedRaftConfigurationV1{}, errors.Join(ErrInvalidConfig, fmt.Errorf("replacement target already exists with different identity/suffrage"))
			}
			foundNew = true
		} else if member.Address == peer.Address {
			return CommittedRaftConfigurationV1{}, errors.Join(ErrInvalidConfig, fmt.Errorf("replacement address already belongs to another member"))
		}
	}
	if !foundOld {
		return CommittedRaftConfigurationV1{}, errors.Join(ErrInvalidConfig, fmt.Errorf("old replica is not a voter"))
	}
	if foundNew {
		return configuration, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CommittedRaftConfigurationV1{}, err
	}
	future := p.raft.AddNonvoter(hraft.ServerID(peer.ID), hraft.ServerAddress(peer.Address), expectedConfigurationIndex, p.applyTimeout)
	if err := waitHashicorpRaftFuture(ctx, future); err != nil {
		return CommittedRaftConfigurationV1{}, errors.Join(ErrHashicorpRaftUnavailable, err)
	}
	return p.CommittedConfigurationV1(ctx)
}

// The pinned Raft 1.7.3 GetConfiguration future omits latestIndex (Stats uses
// the same future). Read the position from native persisted authority instead.
// This bounds inspection work, not supported database size: an older config
// outside this tail requires a native snapshot before replacement can proceed.
const replacementConfigurationScanLimitV1 = 65536

func (p *HashicorpRaftProvider) persistedConfigurationV1(ctx context.Context) (index uint64, encoded []byte, last uint64, err error) {
	if p == nil || p.logStore == nil || p.snapshotStore == nil {
		return 0, nil, 0, ErrInvalidHashicorpRaftProvider
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, 0, err
	}
	metas, err := p.snapshotStore.List()
	if err != nil {
		return 0, nil, 0, err
	}
	var newest *hraft.SnapshotMeta
	for _, meta := range metas {
		if meta == nil || meta.ConfigurationIndex == 0 || meta.ConfigurationIndex > meta.Index {
			return 0, nil, 0, ErrInvalidSnapshotManifest
		}
		if newest == nil || meta.Index > newest.Index {
			newest = meta
		}
	}
	last, err = p.logStore.LastIndex()
	if err != nil {
		return 0, nil, 0, err
	}
	var floor uint64
	if newest != nil {
		floor = newest.Index
	}
	var entry hraft.Log
	for current, scanned := last, 0; current > floor; current, scanned = current-1, scanned+1 {
		if err := ctx.Err(); err != nil {
			return 0, nil, last, err
		}
		if scanned == replacementConfigurationScanLimitV1 {
			return 0, nil, last, fmt.Errorf("%w: replacement configuration tail exceeds %d entries; native snapshot required", ErrHashicorpRaftUnavailable, replacementConfigurationScanLimitV1)
		}
		entry = hraft.Log{}
		if err := p.logStore.GetLog(current, &entry); err != nil {
			return 0, nil, last, err
		}
		if entry.Index != current {
			return 0, nil, last, ErrInvalidHashicorpRaftProvider
		}
		if entry.Type == hraft.LogConfiguration {
			return current, entry.Data, last, nil
		}
	}
	if newest != nil {
		return newest.ConfigurationIndex, hraft.EncodeConfiguration(newest.Configuration), last, nil
	}
	return 0, nil, last, fmt.Errorf("%w: no persisted configuration", ErrHashicorpRaftUnavailable)
}
