package raftfsm

import backenddb "github.com/snissn/gomap/TreeDB/db"

func wireRaftSnapshotSideStoreLookupsV1(_ string, _ *backenddb.Options) (func() error, error) {
	// Dictionaries and templates live directly inside maindb's SystemRootPageID
	// and are captured/restored automatically with maindb.
	return func() error { return nil }, nil
}
