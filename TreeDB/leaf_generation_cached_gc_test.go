package treedb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func listLeafVlogSegmentInfos(t *testing.T, dir string) []os.FileInfo {
	t.Helper()
	leafDir := filepath.Join(dir, "leaf_vlog")
	entries, err := os.ReadDir(leafDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("ReadDir(%s): %v", leafDir, err)
	}
	var out []os.FileInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "value-l255-") || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("entry.Info(%s): %v", entry.Name(), err)
		}
		out = append(out, info)
	}
	return out
}

func TestOpenBackendWithCachedLeafLog_ReclaimsSealedDeadGenerationsAndHandlesCompactStorage(t *testing.T) {
	profiles := []struct {
		name    string
		profile Profile
	}{
		{name: "no_wal_fast", profile: ProfileNoWALFast},
		{name: "command_wal_durable", profile: ProfileCommandWALDurable},
	}

	for _, tc := range profiles {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := OptionsFor(tc.profile, dir)
			opts.IndexOuterLeavesInValueLog = true
			opts.KeepRecent = 1
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.DisableBackgroundPrune = true
			opts.WALMaxSegmentBytes = 128 << 10

			backend, cleanup, err := OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = cleanup()
				}
			}()

			// Write enough distinct keys across multiple rounds and batches so
			// multiple sealed leaf_vlog segments exist, then overwrite all keys so
			// earlier sealed generations become 100% dead while cached worker append
			// lanes may still be parked on them.
			const numKeys = 800
			const batchSize = 100
			writeRound := func(round int, fill byte) {
				val := bytes.Repeat([]byte{fill}, 200)
				for start := 0; start < numKeys; start += batchSize {
					b := backend.NewBatch()
					end := start + batchSize
					if end > numKeys {
						end = numKeys
					}
					for i := start; i < end; i++ {
						key := []byte(fmt.Sprintf("k/%06d", i))
						if err := b.Set(key, val); err != nil {
							t.Fatalf("round %d Set(%d): %v", round, i, err)
						}
					}
					if err := b.Write(); err != nil {
						t.Fatalf("round %d Write: %v", round, err)
					}
					if err := b.Close(); err != nil {
						t.Fatalf("round %d Close: %v", round, err)
					}
				}
			}

			for round := 0; round < 6; round++ {
				writeRound(round, byte('a'+round))
			}
			if err := backend.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint before compact: %v", err)
			}

			beforeSegs := listLeafVlogSegmentInfos(t, dir)
			if len(beforeSegs) < 2 {
				t.Fatalf("expected >= 2 leaf_vlog segments before compact, got %d", len(beforeSegs))
			}

			planBefore, err := backend.LeafGenerationPlan(context.Background(), db.LeafGenerationPlanOptions{Force: true})
			if err != nil {
				t.Fatalf("LeafGenerationPlan before: %v", err)
			}
			deadSealedBefore := 0
			for _, g := range planBefore.Generations {
				if g.State == "sealed" && g.BytesLive == 0 {
					deadSealedBefore++
				}
			}
			if deadSealedBefore == 0 {
				t.Fatalf("expected >= 1 dead sealed generation before compact, got plan=%+v", planBefore)
			}

			stats, err := backend.CompactStorage(context.Background(), db.CompactStorageOptions{
				Mode: db.CompactStorageFull,
			})
			if err != nil {
				t.Fatalf("CompactStorage(Full) failed: %v", err)
			}

			packSupported := rootpublication.StableRelativeNamespaceSupported() && rootpublication.StableCrossParentMoveNoReplaceSupported()
			if !packSupported {
				for _, p := range stats.Phases {
					if strings.HasPrefix(p.Name, "leaf-generation-pack-") && p.Status != "" && p.Status != db.CompactStoragePhaseStatusUnsupported {
						t.Fatalf("unexpected non-unsupported status on %s when pack unsupported: %+v", p.Name, p)
					}
				}
			}

			// Run one more checkpoint + GC if fallback root slot still pinned anything.
			if err := backend.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint after compact: %v", err)
			}
			gcAfter, err := backend.LeafGenerationGC(context.Background(), db.LeafGenerationGCOptions{
				ConvergeFallback:           true,
				DetachSealedCurrentWriters: true,
			})
			if err != nil {
				t.Fatalf("LeafGenerationGC after compact: %v", err)
			}
			totalDeleted := stats.LeafGenerationGC.GenerationsDeleted + gcAfter.GenerationsDeleted
			if totalDeleted == 0 {
				planAfter, _ := backend.LeafGenerationPlan(context.Background(), db.LeafGenerationPlanOptions{Force: true})
				t.Fatalf("expected dead sealed leaf generations to be deleted; compactGC=%+v gcAfter=%+v planAfter=%+v", stats.LeafGenerationGC, gcAfter, planAfter)
			}

			afterSegs := listLeafVlogSegmentInfos(t, dir)
			if len(afterSegs) >= len(beforeSegs) {
				t.Fatalf("expected leaf_vlog segment count to decrease: before=%d after=%d", len(beforeSegs), len(afterSegs))
			}
			for _, info := range afterSegs {
				if info.Size() == 0 {
					t.Fatalf("unexpected zero-byte leaf_vlog segment left after detach/compact: %s", info.Name())
				}
			}

			// Verify detached worker lanes lazily allocate fresh segments on subsequent writes.
			writeRound(99, 'z')
			if err := backend.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint after post-compact write: %v", err)
			}
			if err := cleanup(); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			closed = true

			// Reopen and verify all keys are intact.
			reopened, cleanup2, err := OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = cleanup2() }()
			wantVal := bytes.Repeat([]byte{'z'}, 200)
			for i := 0; i < numKeys; i++ {
				got, err := reopened.Get([]byte(fmt.Sprintf("k/%06d", i)))
				if err != nil {
					t.Fatalf("reopen Get(%d): %v", i, err)
				}
				if !bytes.Equal(got, wantVal) {
					t.Fatalf("reopen value mismatch at %d: len=%d", i, len(got))
				}
			}
		})
	}
}
