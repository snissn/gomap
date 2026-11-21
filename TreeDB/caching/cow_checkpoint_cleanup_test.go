package caching

import (
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCOWExplicitCheckpointScansCommandWALOnce(t *testing.T) {
	for _, name := range []string{"success", "namespace-sync-error"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(envDisableVlogGenerationLoop, "1")
			cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
			cache.SetCommandWALCheckpointCutoverHook(func() {})
			cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)

			writeCOWAutoFallbackRaw(t, cache, backend, "first")
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			writeCOWAutoFallbackRaw(t, cache, backend, "captured")
			captured := backend.CommandWALNextLSN() - 1
			cache.testAfterCheckpointFrontierCapture = func() {
				writeCOWAutoFallbackRaw(t, cache, backend, "post-cut")
				if backend.CommandWALActiveBytes() == 0 {
					t.Fatal("post-cut append did not populate the successor")
				}
			}
			scanCount := func() uint64 {
				t.Helper()
				count, err := strconv.ParseUint(backend.Stats()["treedb.command_wal.cleanup.scan.count_total"], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return count
			}
			var syncErr error
			if name == "namespace-sync-error" {
				syncErr = errors.New("checkpoint WAL directory sync refused")
			}
			unlinks, barriers := 0, 0
			before := scanCount()
			restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				if event.Point == durabilitycut.AfterWALOrAssetUnlink && filepath.Dir(event.Path) == backenddb.WALDirPath(backend.Dir()) {
					unlinks++
				}
				if event.Point == durabilitycut.BeforeDeletionDirectorySync && event.Root == backend.Dir() {
					barriers++
					return syncErr
				}
				return nil
			}))
			defer restore()
			err := cache.Checkpoint()
			restore()
			cache.testAfterCheckpointFrontierCapture = nil
			if !errors.Is(err, syncErr) || (syncErr != nil && !errors.Is(err, backenddb.ErrRecoveryRequired)) {
				t.Fatalf("checkpoint error=%v, directory-sync cause=%v", err, syncErr)
			}
			if got := scanCount() - before; got != 1 {
				t.Fatalf("one cached checkpoint scanned the WAL %d times, want 1", got)
			}
			if unlinks == 0 || barriers != 1 || backend.State().AppliedCommandLSN != captured || backend.CommandWALNextLSN() != captured+2 || backend.CommandWALActiveBytes() != 0 {
				t.Fatalf("checkpoint cut: unlinks=%d barriers=%d applied=%d next=%d active bytes=%d captured=%d", unlinks, barriers, backend.State().AppliedCommandLSN, backend.CommandWALNextLSN(), backend.CommandWALActiveBytes(), captured)
			}
			if got := backend.Stats()["treedb.command_wal.cleanup.namespace_sync.pending"]; got != strconv.FormatBool(syncErr != nil) {
				t.Fatalf("namespace sync pending=%s, sync error=%v", got, syncErr)
			}
			if syncErr != nil {
				if err := cache.Checkpoint(); err != nil {
					t.Fatalf("retry checkpoint: %v", err)
				}
				if got := backend.Stats()["treedb.command_wal.cleanup.namespace_sync.pending"]; got != "false" {
					t.Fatalf("retry left namespace sync pending=%s", got)
				}
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := backenddb.Open(backenddb.Options{Dir: backend.Dir(), CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			value, readErr := reopened.Get([]byte("key"))
			if closeErr := reopened.Close(); string(value) != "post-cut" || readErr != nil || closeErr != nil {
				t.Fatalf("typed reopen read=%q error=%v close=%v", value, readErr, closeErr)
			}
		})
	}
}
