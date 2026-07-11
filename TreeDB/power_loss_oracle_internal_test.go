package treedb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// TestPowerLossOracleSourceDeletionWaitsForStableCoverage is the converted
// witness for the former source-deletion counterexample. Cleanup must consume
// durable AppliedCommandLSN coverage, never a merely visible candidate.
func TestPowerLossOracleSourceDeletionWaitsForStableCoverage(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                    dir,
		CommandWAL:             true,
		Durability:             DurabilityDurable,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	lsn, err := d.backend.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{
		Op: commitlog.RawKVOpSet, Key: []byte("cleanup/acknowledged"), Value: []byte("durable-command-value"),
	}, true)
	if err != nil || lsn == 0 {
		t.Fatalf("append synced command frame: lsn=%d err=%v", lsn, err)
	}
	if err := d.backend.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "*", "wal", "commit-*.log"))
	if err != nil || len(segments) < 2 {
		t.Fatalf("rotated command-WAL segments=%v err=%v", segments, err)
	}
	source := segments[0]

	cutErr := errors.New("power-loss-oracle: pre-meta publication stall")
	var unlinks int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink && event.Resource == durabilitycut.ResourceCommandWAL {
			unlinks++
		}
		if event.Point == durabilitycut.BeforeMetaWrite {
			return cutErr
		}
		return nil
	})
	if err := d.backend.PublishCommandWALAppliedLSN(lsn, []backenddb.CommandWALLSNRange{{First: lsn, Last: lsn}}, false); err != nil {
		restore()
		t.Fatalf("publish visible AppliedCommandLSN: %v", err)
	}
	if err := d.backend.Checkpoint(); !errors.Is(err, backenddb.ErrPublicationStalled) {
		restore()
		t.Fatalf("checkpoint during pre-meta stall err=%v", err)
	}
	if err := d.backend.CleanupCommandWALCoveredSegments(true); err != nil {
		restore()
		t.Fatalf("cleanup against durable coverage: %v", err)
	}
	if unlinks != 0 {
		restore()
		t.Fatalf("cleanup unlinked %d command-WAL sources before stable AppliedCommandLSN coverage", unlinks)
	}
	if _, err := os.Stat(source); err != nil {
		restore()
		t.Fatalf("stable command-WAL source removed before meta coverage: %v", err)
	}
	restore()

	// A later explicit boundary retries the pre-meta stall. Once coverage is in
	// stable metadata, ordinary cleanup may remove the covered source.
	if err := d.backend.Checkpoint(); err != nil {
		t.Fatalf("retry publication: %v", err)
	}
	if err := d.backend.CleanupCommandWALCoveredSegments(true); err != nil {
		t.Fatalf("cleanup after stable coverage: %v", err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("covered command-WAL source still present after stable coverage: %v", err)
	}
}
