package db_test

import (
	"github.com/snissn/gomap/TreeDB/caching"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"testing"
)

func TestCachedCheckpointRetainsRealPendingJournalOwnership(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	cached, err := caching.Open(dir, backend, caching.Options{ExternalCommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cached.Close() }()
	// A publish hook activates the existing no-op command-WAL checkpoint route;
	// it publishes no synthetic LSN or coverage.
	cached.SetCommandWALCheckpointPublishHook(func(bool) (uint64, []backenddb.CommandWALLSNRange, error) { return 0, nil, nil })
	cleanup := backenddb.PendingCommandWALCleanupCheckpointForTesting(t)
	calls := 0
	cached.SetCommandWALCheckpointCleanupHook(func(sync bool) error { calls++; return cleanup(sync) })
	if err = cached.Checkpoint(); err != nil {
		t.Fatalf("cached checkpoint propagated temporary ownership refusal: %v", err)
	}
	cached.SetCommandWALCheckpointCleanupHook(nil)
	if calls != 1 {
		t.Fatalf("cleanup calls=%d want1", calls)
	}
}
