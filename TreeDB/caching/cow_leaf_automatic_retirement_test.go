package caching

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestPrivateCOWLeafAutomaticSourceRetirement(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	cache, backend := privateCOWPackFixture(t)
	// Ordinary writes and checkpoints create partly-live active sources. No setup
	// rotation closes a selected writer, and no external reader pins the old tree.
	privateCOWPackWrite(t, cache, backend.DB, 1, 4096)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for round := 2; round <= 4; round++ {
		privateCOWPackWrite(t, cache, backend.DB, round, 2048)
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := backend.DB.LeafGenerationPlan(context.Background(), backenddb.LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sources := make(map[uint64][]string)
	identities := make(map[string]os.FileInfo)
	cache.leafLog.vlogMu.Lock()
	leafDir := filepath.Dir(cache.leafLog.vlogPath)
	cache.leafLog.vlogMu.Unlock()
	for _, gen := range plan.Generations {
		for _, id := range gen.FileIDs {
			path := valuelog.SegmentPath(leafDir, id)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			identities[path] = info
			sources[gen.GenerationID] = append(sources[gen.GenerationID], path)
		}
	}
	before, _ := backend.DB.StateToken()
	next, checkpoints := backend.DB.CommandWALNextLSN(), cache.checkpointRuns.Load()
	backend.published = make(chan backenddb.LeafGenerationPackRunOnceStats, 1)
	backend.release = make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(backend.release) }) }
	t.Cleanup(release)
	backend.packPacket = make(chan (<-chan struct{}), 1)
	backend.gcCompleted = make(chan privateCOWLeafGCResult, 16)
	// The ordinary ticker also checks general value-log GC eligibility. Its
	// independent checkpoint must not supply progress to this leaf pack.
	// Put only that eligibility check in its normal cooldown.
	cache.vlogGenerationLastGCUnixNano.Store(time.Now().UnixNano())
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envDisableVlogGenerationLoop, "0")
	cache.startVlogGenerationLoop()
	var packed backenddb.LeafGenerationPackRunOnceStats
	select {
	case packed = <-backend.published:
	case <-time.After(10 * time.Second):
		t.Fatal("ordinary scheduler did not pack")
	}
	if !packed.Ran || len(packed.Selection.GenerationIDs) == 0 {
		t.Fatalf("automatic selection: %+v", packed)
	}
	var selected []string
	for _, id := range packed.Selection.GenerationIDs {
		selected = append(selected, sources[id]...)
	}
	if len(selected) == 0 {
		t.Fatal("missing original selected source")
	}
	for _, path := range selected {
		info, err := os.Stat(path)
		if err != nil || !os.SameFile(identities[path], info) {
			t.Fatalf("selected source identity changed before automatic GC: %s err=%v", path, err)
		}
	}
	packet := <-backend.packPacket
	release()
	select {
	case result := <-backend.gcCompleted:
		if packet == nil || result.packet != packet {
			t.Fatal("GC did not belong to the selected periodic leaf pack")
		}
		gc := result.stats
		t.Logf("first automatic GC: %+v selected=%v", gc, selected)
		if gc.GenerationsRetiring != 0 || gc.GenerationsEligible < len(packed.Selection.GenerationIDs) {
			t.Fatalf("automatic packet retained sources without an external reader: %+v", gc)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("automatic pack did not reach GC")
	}
	// The adapter reports the backend's GC return while the owning slice
	// still holds flushMu. Join its cache adoption and GC accounting first.
	cache.flushMu.Lock()
	cache.flushMu.Unlock()
	deadline := time.Now().Add(8 * time.Second)
	for {
		remaining := 0
		for _, path := range selected {
			if _, err := os.Stat(path); err == nil {
				remaining++
			} else if !os.IsNotExist(err) {
				t.Fatal(err)
			}
		}
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("automatic source still named without external reader: remaining=%d selected=%v GC=%d", remaining, selected, cache.vlogGenerationLeafPackGCRuns.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	after, _ := backend.DB.StateToken()
	afterNext, afterCheckpoints := backend.DB.CommandWALNextLSN(), cache.checkpointRuns.Load()
	if after.AppliedCommandLSN != before.AppliedCommandLSN || after.MaxEntryRevision != before.MaxEntryRevision || afterNext != next || afterCheckpoints != checkpoints {
		t.Fatalf("automatic retirement changed logical frontier: before=%+v after=%+v nextLSN=%d/%d checkpoints=%d/%d", before, after, next, afterNext, checkpoints, afterCheckpoints)
	}
	if calls := cache.vlogGenerationLeafPackGCRuns.Load(); calls != 1 {
		t.Fatalf("source unlink required %d automatic GC passes, want original packet only", calls)
	}
	t.Logf("ordinary automatic source unlink: %v", selected)
	dir := backend.DB.Dir()
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for key, want := range map[string]byte{"pack-00000": 4, "pack-03000": 1} {
		got, err := reopened.Get([]byte(key))
		if err != nil || len(got) != 128 || got[0] != want {
			t.Fatalf("reopened %s: %x err=%v", key, got, err)
		}
	}
}
