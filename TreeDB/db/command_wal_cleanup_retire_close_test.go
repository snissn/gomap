package db

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALCleanupRefusalChecksDecisionClose(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		stale, failClose, failSync bool
	}{
		{name: "pin/close_failure=false"},
		{name: "pin/close_failure=true", failClose: true},
		{name: "stale/close_failure=false", stale: true},
		{name: "stale/close_failure=true", stale: true, failClose: true},
		{name: "pin/close_and_directory_sync_failures", failClose: true, failSync: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, opts, closed, active := seedCommandWALCleanupFixedPrefix(t)
			files := make(map[string]*os.File)
			var pin *rootpublication.IdentityPin
			var afterScan bool
			frontier := database.commandWALDurableLSN.Load()
			database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
				openPrefix: func(path string) (*os.File, error) {
					file, err := os.Open(path)
					files[path] = file
					return file, err
				},
				afterScan: func() {
					afterScan = true
					for _, path := range closed {
						if _, err := files[path].Stat(); err != nil {
							t.Fatalf("scan did not transfer a live decision reader for %s: %v", path, err)
						}
					}
					if tc.stale {
						database.commandWALDurableLSN.Store(0)
					} else {
						pin = pinCommandWALCleanupFile(t, database.commandJournal, files[closed[0]])
					}
					if tc.failClose {
						// Invalidate the other transferred reader after successful validation.
						if err := files[closed[1]].Close(); err != nil {
							t.Fatal(err)
						}
					}
				},
			}
			removed := database.commandWALCleanupRemoved.Load()
			pinned := database.commandWALCleanupRetainedPinned.Load()
			dirSyncErr := errors.New("prior WAL namespace debt cannot sync")
			complete, err := func() (bool, error) {
				if tc.failSync {
					// Model sync debt from a previous deletion; this pass cannot unlink.
					database.commandWALCleanupNamespaceDirty.Store(true)
					restore := durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDeletionDirectorySync {
							return dirSyncErr
						}
						return nil
					})
					defer restore()
				}
				return database.CleanupCommandWALCoveredPrefix()
			}()
			database.testCommandWALCleanupHooks = nil
			database.commandWALDurableLSN.Store(frontier)
			pin.Release()
			if !afterScan || complete || database.commandWALCleanupRemoved.Load() != removed || database.commandWALCleanupNamespaceDirty.Load() != tc.failSync {
				t.Fatalf("refused cleanup: scanned=%t complete=%t removed=%d dirty=%t err=%v", afterScan, complete, database.commandWALCleanupRemoved.Load()-removed, database.commandWALCleanupNamespaceDirty.Load(), err)
			}
			if !tc.stale && database.commandWALCleanupRetainedPinned.Load() != pinned+1 {
				t.Fatal("expected the identity pin to prevent deletion during lease acquisition")
			}
			for path, file := range files {
				if _, statErr := file.Stat(); !errors.Is(statErr, os.ErrClosed) {
					t.Errorf("refusal retained a decision reader for %s: %v", path, statErr)
				}
			}
			if tc.failSync && (!errors.Is(err, dirSyncErr) || !errors.Is(err, ErrRecoveryRequired)) {
				t.Errorf("failed directory sync lost its independent cause: %v", err)
			}
			if tc.failClose {
				if !errors.Is(err, os.ErrClosed) || errors.Is(err, ErrDurableWALCleanupProofStale) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
					t.Errorf("failed decision Close must remain a hard cleanup failure: %v", err)
				}
				if !tc.stale && !errors.Is(err, rootpublication.ErrResourcePinned) {
					t.Errorf("failed decision Close lost independent pin refusal: %v", err)
				}
			} else if err != nil {
				t.Errorf("ordinary refusal should leave cleanup pending: %v", err)
			}
			for _, path := range append(closed, active) {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("refusal lost %s: %v", path, err)
				}
			}
			if err := cleanupDeferredCommandWALPrefixForTest(database); err != nil {
				t.Fatal(err)
			}
			requireCommandWALCleanupFixedPrefixReplay(t, database, opts, closed, active)
		})
	}
}

func TestCommandWALCleanupHardErrorPreservesIndependentStatAndClose(t *testing.T) {
	statErr := &os.PathError{Op: "stat", Path: "wal", Err: os.ErrPermission}
	closeErr := &os.PathError{Op: "close", Path: "sealed", Err: os.ErrClosed}
	prior := fmt.Errorf("journal snapshot: %w", errors.Join(commitlog.ErrCommandWALCleanupSnapshotStale, statErr))
	err := commandWALCleanupWithHardError(prior, closeErr)
	if !errors.Is(err, statErr) || !errors.Is(err, closeErr) || errors.Is(err, commitlog.ErrCommandWALCleanupSnapshotStale) {
		t.Fatalf("combined error must retain the stat and close failures without a deferrable stale marker: %v", err)
	}
}

func pinCommandWALCleanupFile(t *testing.T, journal *commitlog.CommandJournal, file *os.File) *rootpublication.IdentityPin {
	t.Helper()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var pin *rootpublication.IdentityPin
	err = journal.WithCleanupSnapshot(snapshot, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
		if err := registry.Observe(identity); err != nil {
			return false, err
		}
		var err error
		pin, err = registry.Pin(identity)
		return false, errors.Join(err, registry.Unobserve(identity))
	})
	if err != nil {
		t.Fatal(err)
	}
	return pin
}
