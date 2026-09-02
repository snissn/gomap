package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVlogGenerationCheckpointKick_SkipsWhenWALOn(t *testing.T) {
	t.Setenv(envDisableVlogGenerationCheckpointKick, "0")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db, err := Open(dir, backend, Options{
		DisableWAL:               false,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer db.Close()

	db.testSkipVlogCheckpointKick = false
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint(false)

	if got := db.vlogGenerationCheckpointKickRuns.Load(); got != 0 {
		t.Fatalf("checkpoint kick runs=%d want 0", got)
	}
	if db.vlogGenerationCheckpointKickActive.Load() {
		t.Fatal("checkpoint kick unexpectedly active")
	}
	if db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatal("checkpoint kick unexpectedly pending")
	}
}
