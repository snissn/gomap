//go:build windows

package treedb

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVacuumIndexOnlineUnsupportedDoesNotCheckpointCachedWrites(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), BackgroundIndexVacuumInterval: -1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()
	control := &DeferredVectorBuildMaintenance{db: database}
	reservation := control.AdmitInsert(context.Background(), "docs", 1)
	if reservation == 0 || !control.CommitInsert(reservation) {
		t.Fatal("begin deferred maintenance")
	}
	if err := database.Set([]byte("dirty"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	checkpointCalls := 0
	database.cached.SetCommandWALCheckpointPublishHook(func(bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		checkpointCalls++
		return 0, nil, errors.New("unexpected checkpoint")
	})

	if err := database.VacuumIndexOnline(context.Background()); !errors.Is(err, backenddb.ErrVacuumUnsupported) {
		t.Fatalf("VacuumIndexOnline error=%v want ErrVacuumUnsupported", err)
	}
	if checkpointCalls != 0 {
		t.Fatalf("unsupported vacuum checkpoint calls=%d want 0", checkpointCalls)
	}
	if !database.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("unsupported vacuum invalidated deferred maintenance")
	}
}
