package caching

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestCachingCheckpointPiggybacksStagedDictOnEmptyAndDirtyMemtables(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		ChunkSize:  64 << 10,
		Durability: backenddb.DurabilityDurable,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		FlushThreshold: 4 << 20,
		DisableWAL:     false,
		RelaxedSync:    false,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open caching: %v", err)
	}
	defer func() { _ = cached.Close() }()

	store := dictdb.New(backend)
	cached.SetDictStore(store)

	// Case 1: Dirty memtable checkpoint piggybacks the staged dict in one commit.
	if err := cached.Set([]byte("dirty/k1"), []byte("value-1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	dict1 := bytes.Repeat([]byte("cached-dirty-piggyback-dict|"), 32)
	var gotID1 atomic.Uint64
	id1, err := store.StagePublishDict(context.Background(), dict1, 8, "single_value", true, func(id uint64) {
		gotID1.Store(id)
	})
	if err != nil {
		t.Fatalf("StagePublishDict(1): %v", err)
	}
	if gotID1.Load() != 0 {
		t.Fatal("staged dict activated before Checkpoint")
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(dirty): %v", err)
	}
	if gotID1.Load() != id1 {
		t.Fatalf("expected dirty Checkpoint to activate dict %d, got %d", id1, gotID1.Load())
	}
	if store.HasStagedOps() {
		t.Fatal("expected HasStagedOps=false after dirty Checkpoint")
	}

	// Case 2: Empty-memtable checkpoint also flushes a staged dict cleanly
	// without deadlocking on flushMu or leaving staged ops behind.
	dict2 := bytes.Repeat([]byte("cached-empty-piggyback-dict|"), 32)
	var gotID2 atomic.Uint64
	id2, err := store.StagePublishDict(context.Background(), dict2, 16, "single_value", true, func(id uint64) {
		gotID2.Store(id)
	})
	if err != nil {
		t.Fatalf("StagePublishDict(2): %v", err)
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(empty): %v", err)
	}
	if gotID2.Load() != id2 {
		t.Fatalf("expected empty-memtable Checkpoint to activate dict %d, got %d", id2, gotID2.Load())
	}
	if store.HasStagedOps() {
		t.Fatal("expected HasStagedOps=false after empty-memtable Checkpoint")
	}
	cur, err := store.GetCurrentForClass(context.Background(), "single_value")
	if err != nil || cur != id2 {
		t.Fatalf("GetCurrentForClass: got=%d want=%d err=%v", cur, id2, err)
	}
}

func TestCachingIdleLoopFlushesStagedDict(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		ChunkSize:  64 << 10,
		Durability: backenddb.DurabilityDurable,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		FlushThreshold: 4 << 20,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open caching: %v", err)
	}
	defer func() { _ = cached.Close() }()

	store := dictdb.New(backend)
	cached.SetDictStore(store)
	// Start background dict loop for idle-flush test.
	cached.wg.Add(1)
	go cached.valueLogDictLoop()

	dictBytes := bytes.Repeat([]byte("cached-idle-flush-dict|"), 32)
	var gotID atomic.Uint64
	id, err := store.StagePublishDict(context.Background(), dictBytes, 4, "single_value", true, func(id uint64) {
		gotID.Store(id)
	})
	if err != nil {
		t.Fatalf("StagePublishDict: %v", err)
	}
	cached.valueLogDictKick()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if gotID.Load() == id {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if gotID.Load() != id {
		t.Fatalf("timed out waiting for idle valueLogDictLoop to flush staged dict %d", id)
	}
}
