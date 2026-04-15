package treedb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestLeafGenerationPlan_FastProfileTracksLeafFileRollsAcrossCheckpoints(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 64 << 10
	opts.ValueLog.Generational.WarmSegmentTargetBytes = 64 << 10
	opts.ValueLog.Generational.ColdSegmentTargetBytes = 64 << 10

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaintenancePhase(treedb.MaintenancePhaseRestore)
	writeLeafGenerationChurnWorkload(t, db, 20000, 5000, 4, 96)
	if err := db.Close(); err != nil {
		t.Fatalf("close initial: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened: %v", err)
		}
	}()
	reopened.SetMaintenancePhase(treedb.MaintenancePhaseRestore)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	plan, err := reopened.LeafGenerationPlan(ctx, treedb.LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan after reopen: %v", err)
	}
	if len(plan.Generations) < 2 {
		t.Fatalf("len(Generations)=%d, want >= 2; plan=%+v", len(plan.Generations), plan)
	}
	if got := reopened.Stats()["treedb.leaf_generation.generations.total"]; got == "" || got == "0" || got == "1" {
		t.Fatalf("leaf generation stats total=%q, want > 1", got)
	}
}

func writeLeafGenerationChurnWorkload(t *testing.T, db *treedb.DB, keyCount, hotKeyCount, rounds, valueBytes int) {
	t.Helper()
	if keyCount <= 0 || hotKeyCount <= 0 || hotKeyCount > keyCount || rounds <= 0 || valueBytes <= 16 {
		t.Fatalf("invalid churn workload parameters keyCount=%d hotKeyCount=%d rounds=%d valueBytes=%d", keyCount, hotKeyCount, rounds, valueBytes)
	}
	base := strings.Repeat("a", valueBytes)
	hot := strings.Repeat("h", valueBytes)
	cold := strings.Repeat("c", valueBytes)
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		val := []byte(fmt.Sprintf("%s-%08d", base[:valueBytes-9], i))
		if err := db.Set(key, val[:valueBytes]); err != nil {
			t.Fatalf("initial set %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint initial: %v", err)
	}
	for round := 0; round < rounds; round++ {
		src := hot
		if round%2 == 1 {
			src = cold
		}
		for i := 0; i < hotKeyCount; i++ {
			key := []byte(fmt.Sprintf("k%08d", i))
			val := []byte(fmt.Sprintf("%s-r%02d-k%08d", src[:valueBytes-14], round, i))
			if err := db.Set(key, val[:valueBytes]); err != nil {
				t.Fatalf("round %d set %d: %v", round, i, err)
			}
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("checkpoint round %d: %v", round, err)
		}
	}
}
