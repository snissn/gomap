package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCompactStorageReclamationPreservesJoinedFailures(t *testing.T) {
	ioFailure := errors.New("storage cleanup failed")
	for _, tc := range []struct {
		name     string
		err      error
		deferred bool
	}{
		{"stale", ErrRecoverableRootSetStale, true},
		{"wrapped-stale", fmt.Errorf("capture: %w", ErrRecoverableRootSetStale), true},
		{"joined-stale", errors.Join(ErrRecoverableRootSetStale, ErrRecoverableRootSetStale), true},
		{"joined-cleanup", errors.Join(ErrRecoverableRootSetStale, ioFailure), false},
		{"wrapped-joined-cleanup", fmt.Errorf("capture: %w", errors.Join(ErrRecoverableRootSetStale, ioFailure)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var database DB
			var stats CompactStorageStats
			err := database.runCompactStorageReclamationPhase(&stats, "gc", func() error { return tc.err })
			if tc.deferred {
				if err != nil || len(stats.Phases) != 1 || stats.Phases[0].Status != CompactStoragePhaseStatusDeferred {
					t.Fatalf("pure contention not deferred: err=%v phases=%+v", err, stats.Phases)
				}
			} else if !errors.Is(err, ioFailure) || stats.Phases[0].Status == CompactStoragePhaseStatusDeferred {
				t.Fatalf("mixed cleanup failure hidden: err=%v phases=%+v", err, stats.Phases)
			}
		})
	}
}

func TestCompactStorageDefersContendedValueLogGC(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var orphan string
	for seq := uint32(1); seq <= 3; seq++ {
		id, err := valuelog.EncodeFileID(0, seq)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(ValueLogDirPath(dir), fmt.Sprintf("value-l0-%06d.log", seq))
		writer, err := valuelog.NewWriter(path, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(0, nil, uint64(seq), []byte("unreferenced value")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		registerTestValueLogProducer(t, dir, path, id)
		if seq == 1 {
			orphan = path
		}
	}
	epoch := database.systemRootPublishEpoch.Load()
	defer database.systemRootPublishEpoch.Store(epoch)
	var gcPhase, refused, refusalPreserved bool
	database.compactStorageBeforePhase = func(phase string) {
		if refused && !refusalPreserved {
			// Check the refused pass before a later phase may capture a fresh
			// capability and legitimately reclaim the same candidate.
			if _, err := os.Stat(orphan); err != nil {
				t.Fatalf("refused GC lost original candidate: %v", err)
			}
			refusalPreserved = true
		}
		gcPhase = phase == "value-log-gc"
	}
	database.testValueLogGCBeforeRevalidateHook = func() {
		if gcPhase {
			refused = true
			database.systemRootPublishEpoch.Add(1)
		}
	}
	defer func() {
		database.compactStorageBeforePhase = nil
		database.testValueLogGCBeforeRevalidateHook = nil
	}()
	stats, err := database.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageQuick})
	if err != nil {
		t.Fatalf("normal publisher contention failed compaction: %v", err)
	}
	if !refused || !refusalPreserved {
		t.Fatal("fixture did not reach destructive GC revalidation")
	}
	for _, phase := range stats.Phases {
		if phase.Name != "value-log-gc" {
			continue
		}
		if phase.Status != CompactStoragePhaseStatusDeferred || !phase.Skipped || phase.SkipReason == "" {
			t.Fatalf("refused GC was not reported as deferred: %+v", phase)
		}
		database.compactStorageBeforePhase = nil
		database.testValueLogGCBeforeRevalidateHook = nil
		database.systemRootPublishEpoch.Store(epoch)
		if _, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("later uncontended GC did not reclaim candidate: %v", err)
		}
		return
	}
	t.Fatal("missing value-log-gc phase")
}
