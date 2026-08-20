package raftcluster

import (
	"context"
	"errors"
	"strconv"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// HashicorpRaftTestLeaderReady performs the live quorum verification and term
// read needed by the external-package integration harness. Keeping this in a
// _test.go file avoids extending the production provider API for test
// readiness checks.
func HashicorpRaftTestLeaderReady(ctx context.Context, provider *HashicorpRaftProvider) (uint64, error) {
	if provider == nil || provider.raft == nil {
		return 0, ErrHashicorpRaftUnavailable
	}
	if err := waitHashicorpRaftFuture(ctx, provider.raft.VerifyLeader()); err != nil {
		return 0, err
	}
	term, err := strconv.ParseUint(provider.raft.Stats()["term"], 10, 64)
	if err != nil || term == 0 || provider.raft.State() != hraft.Leader {
		return 0, errors.New("leader term is no longer current")
	}
	return term, nil
}

// HashicorpRaftTestFrontier exposes Raft's diagnostic frontier to the
// external-package integration harness without expanding the provider API.
type HashicorpRaftTestFrontierV1 struct {
	State    hraft.RaftState
	Term     uint64
	LeaderID NodeID
	LastLog  uint64
	Commit   uint64
	Applied  uint64
}

func HashicorpRaftTestFrontier(provider *HashicorpRaftProvider) (HashicorpRaftTestFrontierV1, error) {
	if provider == nil || provider.raft == nil {
		return HashicorpRaftTestFrontierV1{}, ErrHashicorpRaftUnavailable
	}
	_, leaderID := provider.raft.LeaderWithID()
	return HashicorpRaftTestFrontierV1{
		State:    provider.raft.State(),
		Term:     provider.raft.CurrentTerm(),
		LeaderID: NodeID(leaderID),
		LastLog:  provider.raft.LastIndex(),
		Commit:   provider.raft.CommitIndex(),
		Applied:  provider.raft.AppliedIndex(),
	}, nil
}

type blockingHashicorpRaftFuture struct {
	release <-chan struct{}
}

func (f blockingHashicorpRaftFuture) Error() error {
	<-f.release
	return nil
}

func TestWaitHashicorpRaftFutureHonorsCanceledContext(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitHashicorpRaftFuture(ctx, blockingHashicorpRaftFuture{release: release}); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitHashicorpRaftFuture error=%v want context cancellation", err)
	}
}
