package treedb

import (
	"context"
	"strconv"

	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
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

func TestOpenBackendWithCachedLeafLogPublicGenerationHandoff(t *testing.T) {
	backend, cleanup, stats, err := OpenBackendWithCachedLeafLogStats(OptionsFor(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	for i := 0; i < 256; i++ {
		if err := backend.Set([]byte(fmt.Sprintf("handoff-%03d", i)), []byte("durable-row")); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held := backend.AcquireSnapshot()
	if held == nil {
		t.Fatal("held snapshot unavailable")
	}
	defer held.Close()
	counter := func(name string) uint64 {
		t.Helper()
		value, err := strconv.ParseUint(stats()["treedb.cache.leaf_log_lanes.lane.00."+name], 10, 64)
		if err != nil {
			t.Fatalf("counter %s: %v", name, err)
		}
		return value
	}
	oldID, oldSeq := counter("current_file_id"), counter("current_sequence")
	if oldID == 0 || oldSeq == 0 {
		t.Fatal("public raw producer did not open physical leaf writer")
	}
	if err := backend.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if counter("current_file_id") == oldID || counter("current_sequence") <= oldSeq+1 || counter("maintenance_handoffs_total") != 1 || counter("threshold_rotations_total") != 0 {
		t.Fatalf("public installed handoff did not transfer physical writer: %v", stats())
	}
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("handoff-%03d", i))
		current, err := backend.Get(key)
		if err != nil || string(current) != "durable-row" {
			t.Fatalf("current %q: %q %v", key, current, err)
		}
		old, err := held.Get(key)
		if err != nil || string(old) != "durable-row" {
			t.Fatalf("held %q: %q %v", key, old, err)
		}
	}
	// A dry-run GC and repeated no-work maintenance preserve the new frontier.
	if _, err := backend.LeafGenerationGC(context.Background(), backenddb.LeafGenerationGCOptions{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.LeafGenerationPackRunOnce(context.Background(), backenddb.LeafGenerationPackFromPlanOptions{}); err != nil {
		t.Fatal(err)
	}
	if counter("maintenance_handoffs_total") != 1 {
		t.Fatal("no-work maintenance rotated an empty producer")
	}
}
