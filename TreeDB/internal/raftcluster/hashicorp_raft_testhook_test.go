package raftcluster

import "strconv"

// HashicorpRaftTestLeaderTerm exposes only the live current term needed by the
// external-package integration harness. Keeping this in a _test.go file avoids
// extending the production provider API for test readiness checks.
func HashicorpRaftTestLeaderTerm(provider *HashicorpRaftProvider) (uint64, bool) {
	if provider == nil || provider.raft == nil {
		return 0, false
	}
	term, err := strconv.ParseUint(provider.raft.Stats()["term"], 10, 64)
	return term, err == nil && term != 0
}
