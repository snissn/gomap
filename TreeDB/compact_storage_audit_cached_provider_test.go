package treedb

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompactStorageAudit_PublicCachedProviderForwardingRejectsDrift(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true
	opts.JournalLanes = 1
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("provider/before"), bytes.Repeat([]byte("a"), 256)); err != nil {
		t.Fatalf("Set before: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint before: %v", err)
	}
	beforeRoots, beforeSystemRoots := db.cached.ProtectedLeafGenerationRootIDPair()

	var calls atomic.Uint64
	var mutationMu sync.Mutex
	var mutationErr error
	var rootsAfterMutation []uint64
	var systemRootsAfterMutation []uint64
	planDone := make(chan struct{})
	var stats CompactStorageStats
	go func() {
		stats, err = db.CompactStoragePlan(context.Background(), CompactStorageOptions{
			LeafGenerationProtectedRootIDPairFunc: func() ([]uint64, []uint64) {
				if calls.Add(1) == 2 {
					if setErr := db.Set([]byte("provider/after"), bytes.Repeat([]byte("b"), 256)); setErr != nil {
						mutationMu.Lock()
						mutationErr = setErr
						mutationMu.Unlock()
						return nil, nil
					}
					if checkpointErr := db.Checkpoint(); checkpointErr != nil {
						mutationMu.Lock()
						mutationErr = checkpointErr
						mutationMu.Unlock()
						return nil, nil
					}
					rootsAfterMutation, systemRootsAfterMutation = db.cached.ProtectedLeafGenerationRootIDPair()
				}
				return nil, nil
			},
		})
		close(planDone)
	}()
	select {
	case <-planDone:
	case <-time.After(5 * time.Second):
		t.Fatal("public CompactStoragePlan deadlocked while cached roots advanced")
	}
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	mutationMu.Lock()
	defer mutationMu.Unlock()
	if mutationErr != nil {
		t.Fatalf("advance cached provider: %v", mutationErr)
	}
	if !reflect.DeepEqual(beforeRoots, rootsAfterMutation) || !reflect.DeepEqual(beforeSystemRoots, systemRootsAfterMutation) {
		t.Fatalf("public same-ID provider mutation changed root IDs: ordinary %v -> %v, system %v -> %v", beforeRoots, rootsAfterMutation, beforeSystemRoots, systemRootsAfterMutation)
	}
	if calls.Load() != 6 || stats.Audit.SharedScans != 1 || stats.Audit.RevalidationRetries != 1 {
		t.Fatalf("mixed public cached basis was accepted: calls=%d audit=%+v", calls.Load(), stats.Audit)
	}
}
