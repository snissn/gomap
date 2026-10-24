package db

import (
	"context"
	"os"
	"testing"
)

func TestCommandWALFixedCapacityCleanupSeparatesLogicalDebtAndPhysicalBytes(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir: dir, CommandWAL: true, Durability: DurabilityWALOnRelaxed,
		CommandWALSegmentTargetBytes: 256, DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.Set([]byte("covered"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := database.MaintainCommandWALCoveredPrefix(); err != nil {
		t.Fatal(err)
	}
	state := database.State()
	if err := database.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, state.AppliedCommandLSN, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	active, _ := database.commandWALActiveSegmentSnapshot()
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	var closedLogical, closedPhysical int64
	for _, segment := range segments {
		if !isCommandWALLaneSegment(segment) || samePath(segment.path, active) {
			continue
		}
		info, err := os.Stat(segment.path)
		if err != nil {
			t.Fatal(err)
		}
		closedLogical += segment.size
		closedPhysical += info.Size()
	}
	if closedLogical <= 0 || closedPhysical <= closedLogical {
		t.Fatalf("fixture logical=%d physical=%d", closedLogical, closedPhysical)
	}
	if got := database.commandWALClosedBytes.Load(); got != closedLogical {
		t.Fatalf("closed logical debt=%d want %d", got, closedLogical)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("prepare cleanup pending=%v err=%v", pending, err)
	}
	if err := database.Set([]byte("post-cut"), []byte("retained")); err != nil {
		t.Fatal(err)
	}
	removedBefore := database.commandWALCleanupBytes.Load()
	if complete, err := database.CleanupCommandWALCoveredPrefix(); err != nil || !complete {
		t.Fatalf("cleanup complete=%v err=%v", complete, err)
	}
	if got := database.commandWALClosedBytes.Load(); got != 0 {
		t.Fatalf("closed logical debt after unlink=%d want 0", got)
	}
	if got := database.commandWALCleanupBytes.Load() - removedBefore; got != uint64(closedPhysical) {
		t.Fatalf("physical bytes reclaimed=%d want %d", got, closedPhysical)
	}
	if got, want := database.CommandWALBytes(), database.CommandWALActiveBytes(); got != want || got <= 0 {
		t.Fatalf("remaining logical pressure=%d active=%d", got, want)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(Options{Dir: dir, CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := database.Get([]byte("post-cut")); err != nil || string(got) != "retained" {
		t.Fatalf("post-cut value=%q err=%v", got, err)
	}
	if database.commandWALClosedBytes.Load() < 0 {
		t.Fatal("reopened logical debt is negative")
	}
}
