package treedb

import (
	"fmt"
	"sync"
	"testing"
)

func TestOpenBackendWithCachedLeafLogStatsSnapshotConcurrentWritesAndCheckpoint(t *testing.T) {
	backend, cleanup, snapshot, err := OpenBackendWithCachedLeafLogStats(OptionsFor(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if stats := snapshot(); stats == nil || stats["treedb.durability_mode"] == "" {
		t.Fatalf("incomplete initial stats snapshot: %v", stats)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			stats := snapshot()
			if stats != nil && stats["treedb.durability_mode"] == "" {
				t.Errorf("incomplete stats snapshot: %v", stats)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		if err := backend.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("value")); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	wg.Wait()
	if err := cleanup(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
