package treedb

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

// TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage is the
// stable witness for deleting a command-WAL source before its AppliedCommandLSN
// coverage reaches stable metadata. Package-local access is used only to drive
// the backend publication and cleanup sequence; the crash image is reopened
// through the actual public TreeDB Open/read path.
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
	lsn, err := d.backend.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{
		Op:    commitlog.RawKVOpSet,
		Key:   key,
		Value: value,
	}, true)
	if err != nil || lsn == 0 {
		t.Fatalf("append synced command-WAL frame: lsn=%d err=%v", lsn, err)
	}
	if err := d.backend.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatalf("rotate synced command-WAL segment: %v", err)
	}
	baseline := d.backend.State()
	if baseline == nil {
		t.Fatal("missing baseline state")
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("capture stable command-WAL image: %v", err)
	}

	cutErr := errors.New("power-loss-oracle: stop after actual deletion directory sync")
	var snapshot *powerlossoracle.Model
	var deletionSync durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDeletionDirectorySync && event.Resource == durabilitycut.ResourceCommandWAL {
			deletionSync = event
			snapshot = model.Clone()
			return cutErr
		}
		return nil
	})
	if err := d.backend.PublishCommandWALAppliedLSN(lsn, []backenddb.CommandWALLSNRange{{First: lsn, Last: lsn}}, false); err != nil {
		restore()
		t.Fatalf("publish unsynced AppliedCommandLSN: %v", err)
	}
	err = d.backend.CleanupCommandWALCoveredSegments(true)
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil || deletionSync.Path == "" {
		t.Fatalf("actual deletion-directory cut err=%v path=%q", err, deletionSync.Path)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close source fixture: %v", err)
	}
	closed = true

	crashDir := t.TempDir()
	if err := snapshot.MaterializeStable(crashDir); err != nil {
		t.Fatalf("materialize stable-only image: %v", err)
	}
	reopenOpts := opts
	reopenOpts.Dir = crashDir
	reopenOpts.ReadOnly = true
	reopened, openErr := Open(reopenOpts)
	if openErr != nil {
		t.Logf("public TreeDB.Open rejected image after early actual WAL deletion: %v", openErr)
		return
	}
	defer reopened.Close()
	got, getErr := reopened.Get(key)
	if getErr == nil && bytes.Equal(got, value) {
		t.Fatalf("public TreeDB.Open/read recovered acknowledged command after its only stable source was deleted")
	}
	stats := reopened.Stats()
	openedSequence, err := strconv.ParseUint(stats["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse reopened commit sequence: %v", err)
	}
	openedApplied, err := strconv.ParseUint(stats["treedb.applied_command_lsn"], 10, 64)
	if err != nil {
		t.Fatalf("parse reopened applied LSN: %v", err)
	}
	generation := powerLossCompleteGeneration(baseline.CommitSeq, baseline.AppliedCommandLSN)
	for i := range generation.Resources {
		if generation.Resources[i].Kind == powerlossoracle.ResourceCommandWAL {
			generation.Resources[i].ID = deletionSync.Path
		}
	}
	scenario := powerlossoracle.Scenario{
		Name:                 "actual-source-deletion-before-stable-coverage",
		Cut:                  powerlossoracle.AfterDeletionDirectorySync,
		Generations:          []powerlossoracle.Generation{generation},
		LatestSealedSequence: baseline.CommitSeq,
		SelectedSequence:     openedSequence,
		OpenedSequence:       openedSequence,
		OpenedAppliedLSN:     openedApplied,
		RemovedResources: []powerlossoracle.Resource{{
			Kind: powerlossoracle.ResourceCommandWAL,
			ID:   deletionSync.Path,
		}},
		ReopenAttempted: true,
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantEarlySourceDeletion); err != nil {
		t.Fatalf("successful public Open did not produce early-source-deletion diagnosis: %v (value=%q get=%v)", err, got, getErr)
	}
}

func powerLossCompleteGeneration(sequence, applied uint64) powerlossoracle.Generation {
	kinds := []powerlossoracle.ResourceKind{
		powerlossoracle.ResourceIndex,
		powerlossoracle.ResourceFreelist,
		powerlossoracle.ResourceValueLog,
		powerlossoracle.ResourceOuterLeaf,
		powerlossoracle.ResourceAuxiliary,
		powerlossoracle.ResourceDirectory,
		powerlossoracle.ResourceSeal,
		powerlossoracle.ResourceCommandWAL,
	}
	resources := make([]powerlossoracle.Resource, 0, len(kinds))
	for _, kind := range kinds {
		resources = append(resources, powerlossoracle.Resource{Kind: kind, ID: string(kind), Stable: true, Live: true})
	}
	return powerlossoracle.Generation{Sequence: sequence, Recoverable: true, Resources: resources, AppliedLSN: applied}
}
