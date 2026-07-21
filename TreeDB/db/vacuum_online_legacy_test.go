package db

import (
	"bytes"
	"context"
	"runtime"
	"testing"
)

type legacyOnlineVacuumTestCapabilityV1 struct{}

func (legacyOnlineVacuumTestCapabilityV1) allowLegacyOnlineVacuumV1() {}

func (db *DB) vacuumIndexOnlineLegacyForTest(ctx context.Context) error {
	return db.vacuumIndexOnlineLegacyV1(ctx, true, legacyOnlineVacuumTestCapabilityV1{})
}

func skipLegacyOnlineVacuumRuntimeIntegration(t *testing.T) {
	t.Helper()
	t.Skip("deferred to #3681: post-swap writes require recoverable-root-set runtime rebinding")
}

func TestVacuumIndexOnlineUsesProductionRecoverableRootSetFence(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	oldIndex := db.idx.Load()
	if err := db.SetSync([]byte("before"), []byte("vacuum")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("VacuumIndexOnline: %v", err)
	}
	newIndex := db.idx.Load()
	if newIndex == nil || newIndex == oldIndex {
		t.Fatalf("published index=%p want replacement distinct from %p", newIndex, oldIndex)
	}
	if db.rootPublication == nil || db.rootPublication.idx != newIndex {
		t.Fatalf("root-publication index=%p want replacement %p", db.rootPublication.idx, newIndex)
	}
}

func TestVacuumIndexOnlinePostSwapWriteCheckpointAndReopenUseReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("before"), bytes.Repeat([]byte("a"), 64)); err != nil {
		_ = db.Close()
		t.Fatalf("seed: %v", err)
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("VacuumIndexOnline: %v", err)
	}
	replacement := db.idx.Load()
	if err := db.Set([]byte("after"), bytes.Repeat([]byte("b"), 64)); err != nil {
		_ = db.Close()
		t.Fatalf("post-swap Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("post-swap Checkpoint: %v", err)
	}
	if db.idx.Load() != replacement || db.rootPublication == nil || db.rootPublication.idx != replacement {
		_ = db.Close()
		t.Fatal("post-swap publication escaped the replacement generation")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for key, want := range map[string]byte{"before": 'a', "after": 'b'} {
		got, err := reopened.Get([]byte(key))
		if err != nil || len(got) != 64 || got[0] != want {
			t.Fatalf("reopened Get(%q)=%q err=%v", key, got, err)
		}
	}
}
