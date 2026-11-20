package db

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

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

func TestCommandWALCleanupClosesUnlinkedStreamAfterUnlock(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux ext4")
	}
	for _, tc := range []struct {
		name                        string
		failClose, failSecondUnlink bool
	}{
		{name: "append_during_close"},
		{name: "close_failure_retires_remaining_handle", failClose: true},
		{name: "partial_unlink_and_close_failure", failClose: true, failSecondUnlink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true,
				ResolvedProfile: ProfileCommandWALRelaxed, Durability: DurabilityWALOnRelaxed,
				MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
			}
			database, err := OpenForNativeSemanticOwner(opts)
			if err != nil {
				t.Fatal(err)
			}
			open := true
			defer func() {
				if open {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			if stats := database.commandJournal.WriterBufferStats(); stats.Container != "stream" || stats.ContainerReason != "ext4_stream" {
				t.Skipf("requires ext4 stream selection, got %s/%s", stats.Container, stats.ContainerReason)
			}

			keys := []string{"covered-first", "covered-second", "concurrent"}
			value := []byte("WAL close handoff")
			var paths [2]string
			for i := range paths {
				paths[i], _ = database.commandWALActiveSegmentSnapshot()
				if err := database.SetSync([]byte(keys[i]), value); err != nil {
					t.Fatal(err)
				}
				if err := database.RotateCommandWALActiveSegment(true); err != nil {
					t.Fatal(err)
				}
			}
			if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
				t.Fatal(err)
			}
			if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
				t.Fatalf("expected deferred WAL cleanup: pending=%t error=%v", pending, err)
			}
			for _, path := range paths {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("deferred cleanup lost its fixture before execution: %v", err)
				}
			}

			const wait = 5 * time.Second
			type cleanupResult struct {
				complete bool
				err      error
			}
			entered := make(chan *os.File, 1)
			resume := make(chan struct{})
			released := false
			defer func() {
				if !released {
					close(resume)
				}
			}()
			var retired []*os.File
			database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
				beforeRetiredClose: func(file *os.File) {
					retired = append(retired, file)
					if len(retired) == 1 {
						entered <- file
						<-resume
						if tc.failClose {
							if err := file.Close(); err != nil {
								t.Errorf("inject close error: %v", err)
							}
						}
					}
				},
			}
			unlinkErr := errors.New("injected refusal before second unlink")
			if tc.failSecondUnlink {
				fired := false
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if !fired && event.Resource == durabilitycut.ResourceCommandWAL &&
						event.Point == durabilitycut.BeforeWALOrAssetUnlink && event.Path == paths[1] {
						fired = true
						return unlinkErr
					}
					return nil
				})
				defer restore()
			}

			before := database.commandWALCleanupRemoved.Load()
			beforeSync := database.commandWALCleanupNamespaceSyncs.Load()
			nextLSN := database.CommandWALNextLSN()
			done := make(chan cleanupResult, 1)
			go func() {
				complete, err := database.CleanupCommandWALCoveredPrefix()
				done <- cleanupResult{complete, err}
			}()
			select {
			case file := <-entered:
				if _, err := file.Stat(); err != nil {
					t.Errorf("final verification handle closed too soon: %v", err)
				}
				if _, err := os.Stat(file.Name()); !os.IsNotExist(err) {
					t.Errorf("retired name still present during close: %v", err)
				}
			case result := <-done:
				t.Fatalf("cleanup returned before the retired descriptor reached the close handoff: %+v", result)
			case <-time.After(wait):
				t.Fatal("cleanup did not reach the close handoff")
			}
			if database.durablePublishMu.TryLock() {
				database.durablePublishMu.Unlock()
			} else {
				t.Error("close still holds the durable publication lock")
			}
			if got := database.commandWALCleanupRemoved.Load(); got != before {
				t.Errorf("durable removal counted while close is held: %d -> %d", before, got)
			}
			if got := database.commandWALCleanupNamespaceSyncs.Load(); got != beforeSync {
				t.Errorf("deletion directory sync started while close is held: %d -> %d", beforeSync, got)
			}
			write := make(chan error, 1)
			go func() { write <- database.Set([]byte(keys[2]), value) }()
			select {
			case err := <-write:
				if err != nil {
					t.Error(err)
				}
				if got := database.CommandWALNextLSN(); got != nextLSN+1 {
					t.Errorf("concurrent write did not append to live WAL: next LSN %d -> %d", nextLSN, got)
				}
			case <-time.After(wait):
				t.Error("foreground WAL append waited for the retired close")
			}
			close(resume)
			released = true
			var result cleanupResult
			select {
			case result = <-done:
			case <-time.After(wait):
				t.Fatal("cleanup stalled after releasing the close")
			}
			database.testCommandWALCleanupHooks = nil
			wantRetired := len(paths)
			if tc.failSecondUnlink {
				wantRetired--
				if _, err := os.Stat(paths[1]); err != nil {
					t.Errorf("refused segment no longer has its original name: %v", err)
				}
			}
			if len(retired) != wantRetired {
				t.Errorf("closed descriptor count %d, want %d", len(retired), wantRetired)
			}
			for _, file := range retired {
				if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Errorf("cleanup retained descriptor for %q: %v", file.Name(), err)
				}
			}
			if tc.failClose {
				if result.complete || !errors.Is(result.err, os.ErrClosed) || !errors.Is(result.err, ErrRecoveryRequired) {
					t.Fatalf("cleanup must report hard close failure and incomplete removal: %+v", result)
				}
				if tc.failSecondUnlink && !errors.Is(result.err, unlinkErr) {
					t.Errorf("cleanup lost independent unlink failure: %v", result.err)
				}
				if !database.commandWALCleanupNamespaceDirty.Load() ||
					database.commandWALCleanupRemoved.Load() != before ||
					database.commandWALCleanupUnlinkedPending.Load() != uint64(wantRetired) {
					t.Fatal("failed close lost pending deletion debt or reported durable removal")
				}
				if complete, err := database.CleanupCommandWALCoveredPrefix(); err != nil || !complete {
					t.Fatalf("cleanup retry: complete=%t error=%v", complete, err)
				}
			} else if result.err != nil || !result.complete {
				t.Fatalf("cleanup failed: %+v", result)
			}
			if database.commandWALCleanupNamespaceDirty.Load() ||
				database.commandWALCleanupUnlinkedPending.Load() != 0 ||
				database.commandWALCleanupRemoved.Load() != before+uint64(len(paths)) {
				t.Fatal("cleanup did not finish durable deletion of both original targets")
			}
			for _, path := range paths {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("covered target path remains after checked cleanup: %v", err)
				}
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			open = false
			database, err = OpenForNativeSemanticOwner(opts)
			if err != nil {
				t.Fatal(err)
			}
			open = true
			for _, key := range keys {
				if got, err := database.Get([]byte(key)); err != nil || string(got) != string(value) {
					t.Errorf("fresh read of %q: value=%q error=%v", key, got, err)
				}
			}
		})
	}
}
