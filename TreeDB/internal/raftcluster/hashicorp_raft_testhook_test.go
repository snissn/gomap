package raftcluster

import (
	"strconv"

	hraft "github.com/hashicorp/raft"
)

// HashicorpRaftTestLeaderReady performs the live quorum verification and term
// read needed by the external-package integration harness. Keeping this in a
// _test.go file avoids extending the production provider API for test
// readiness checks.
func HashicorpRaftTestLeaderReady(provider *HashicorpRaftProvider) (uint64, bool) {
	if provider == nil || provider.raft == nil {
		return 0, false
	}
	if err := provider.raft.VerifyLeader().Error(); err != nil {
		return 0, false
	}
	term, err := strconv.ParseUint(provider.raft.Stats()["term"], 10, 64)
	return term, err == nil && term != 0 && provider.raft.State() == hraft.Leader
}
