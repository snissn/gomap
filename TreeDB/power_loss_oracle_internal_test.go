package treedb

import (
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage retains
// its stable witness name while asserting the #3680 fail-closed behavior. A
// command-WAL source whose AppliedCommandLSN coverage has not reached stable
// metadata must remain present; #3681/#3682 own eventual cleanup convergence.
func TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                    dir,
		CommandWAL:             true,
		Durability:             DurabilityDurable,
		DisableBackgroundPrune: true,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open command-WAL fixture: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()
	key := []byte("cleanup/acknowledged")
	value := []byte("durable-command-value")
	var sourcePath string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
			sourcePath = event.Path
		}
		return nil
	})
	lsn, err := d.backend.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{
		Op:    commitlog.RawKVOpSet,
		Key:   key,
		Value: value,
	}, true)
	if err != nil || lsn == 0 {
		restore()
		t.Fatalf("append synced command-WAL frame: lsn=%d err=%v", lsn, err)
	}
	if err := d.backend.RotateCommandWALActiveSegment(true); err != nil {
		restore()
		t.Fatalf("rotate synced command-WAL segment: %v", err)
	}
	restore()
	if sourcePath == "" {
		t.Fatal("synced command-WAL append emitted no exact source path")
	}

	deletedSource := false
	restore = durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterWALOrAssetUnlink &&
			event.Resource == durabilitycut.ResourceCommandWAL && event.Path == sourcePath {
			deletedSource = true
		}
		return nil
	})
	if err := d.backend.PublishCommandWALAppliedLSN(lsn, []backenddb.CommandWALLSNRange{{First: lsn, Last: lsn}}, false); err != nil {
		restore()
		t.Fatalf("publish unsynced AppliedCommandLSN: %v", err)
	}
	err = d.backend.CleanupCommandWALCoveredSegments(true)
	restore()
	if err != nil {
		t.Fatalf("cleanup with unstable coverage: %v", err)
	}
	if deletedSource {
		t.Fatalf("cleanup deleted command-WAL source before stable AppliedCommandLSN coverage: %s", sourcePath)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("command-WAL source not retained before stable coverage: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close source fixture: %v", err)
	}
	closed = true
}
