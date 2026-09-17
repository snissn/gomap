package raftcluster

import (
	"context"
	"slices"

	hraft "github.com/hashicorp/raft"
)

// RuntimeStatusV1 is an observation, not a quorum or read-safety proof. Members
// and Features describe the configured fixed peers, not a membership-change API.
type RuntimeStatusV1 struct {
	NodeID                                         NodeID
	GroupID                                        GroupID
	LeaderID                                       NodeID
	State                                          string
	Term, CommitIndex, RaftAppliedIndex, LastIndex uint64
	Members                                        []Peer
	Features                                       FeatureSet
	Applied                                        AppliedProgress
}

func runtimeStatusV1(r *hraft.Raft, c ResolvedConfig) RuntimeStatusV1 {
	_, leader := r.LeaderWithID()
	c.Peers = slices.Clone(c.Peers)
	for i := range c.Peers {
		c.Peers[i].Capabilities.Required = slices.Clone(c.Peers[i].Capabilities.Required)
	}
	c.Features.Required = slices.Clone(c.Features.Required)
	return RuntimeStatusV1{NodeID: c.NodeID, GroupID: c.GroupID, LeaderID: NodeID(leader), State: r.State().String(), Term: r.CurrentTerm(), CommitIndex: r.CommitIndex(), RaftAppliedIndex: r.AppliedIndex(), LastIndex: r.LastIndex(), Members: c.Peers, Features: c.Features}
}

func (p *HashicorpRaftProvider) RuntimeStatusV1(ctx context.Context) (RuntimeStatusV1, error) {
	s := runtimeStatusV1(p.raft, p.cluster)
	var err error
	if p.appliedProgress != nil {
		s.Applied, err = p.appliedProgress.AppliedProgress(ctx)
	}
	return s, err
}

func (p *CatalogMetaRaftProviderV1) RuntimeStatusV1() RuntimeStatusV1 {
	return runtimeStatusV1(p.raft, p.cluster)
}
