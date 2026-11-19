package treedb

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Actual public raw owner paths stay usable after explicit semantic activation,
// including ordinary durability boundaries and a recovered nonzero WAL basis.
func TestPublicCOWSemanticActivationRawOwnerAndReopen(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile)
			if err := database.backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("relaxed"), []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("sync"), []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("reopened active owner Close=%v", err)
				}
			}()
			// Canonical recovery/open binds raw authority only; activation is explicit.
			if err := reopened.backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatalf("recovered activation=%v", err)
			}
			for key, want := range map[string]string{"relaxed": "one", "sync": "two"} {
				got, err := reopened.Get([]byte(key))
				if err != nil || string(got) != want {
					t.Fatalf("%s=%q %v", key, got, err)
				}
			}
			if err := reopened.SetSync([]byte("after-reopen"), []byte("three")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Exercise the exact interval between the completed cache checkpoint and the
// optional fallback's maintenance admission. A later writer belongs to a new
// boundary; the optional convergence may not consume or reject that suffix.
func TestPublicCOWSemanticIdleFallbackDefersNewerWrite(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			defer func() {
				if err := database.Close(); err != nil {
					t.Errorf("Close=%v", err)
				}
			}()
			if err := database.backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("prefix"), []byte("sealed")); err != nil {
				t.Fatal(err)
			}
			if err := database.cached.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			basis, ok := database.backend.StateToken()
			if !ok {
				t.Fatal("missing sealed basis")
			}
			if database.commandWALCached {
				needed, err := database.backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
				if err != nil || !needed {
					t.Fatalf("fixture needs real fallback convergence=%v,%v", needed, err)
				}
			}
			if err := database.Set([]byte("suffix"), []byte("newer")); err != nil {
				t.Fatal(err)
			}
			next := database.backend.CommandWALNextLSN()
			if err := database.refreshPublicCommandWALCheckpointFallback(); err != nil {
				t.Fatalf("optional idle refresh after newer write=%v", err)
			}
			after, ok := database.backend.StateToken()
			if !ok || after != basis || database.backend.CommandWALNextLSN() != next {
				t.Fatalf("optional refresh consumed newer boundary: %+v -> %+v", basis, after)
			}
			if found, err := database.backend.Has([]byte("suffix")); err != nil || found {
				t.Fatalf("unsealed suffix physically installed=%v,%v", found, err)
			}
			if value, err := database.Get([]byte("suffix")); err != nil || string(value) != "newer" {
				t.Fatalf("newer logical suffix=%q,%v", value, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("next explicit checkpoint=%v", err)
			}
		})
	}
}

// The ordinary checkpoint already sealed these commands. Refreshing the other
// DATA slot must issue a real meta sync without syncing the active WAL again.
func TestPublicCOWSealedFallbackDoesNotResyncActiveWAL(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile)
			if err := database.Set([]byte("key"), []byte("sealed")); err != nil {
				t.Fatal(err)
			}
			if err := database.cached.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			basis, ok := database.backend.StateToken()
			if !ok || basis.AppliedCommandLSN == 0 {
				t.Fatalf("checkpoint lacks applied command: %+v", basis)
			}
			if needed, err := database.backend.CommandWALCheckpointFallbackNeedsRefresh(basis); err != nil || !needed {
				t.Fatalf("fallback needs refresh=%v, err=%v", needed, err)
			}

			next := database.backend.CommandWALNextLSN()
			var walSyncs, metaSyncs atomic.Int32
			mainDir := filepath.Join(opts.Dir, "maindb")
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == durabilitycut.AfterDependencyFileSync && e.Resource == durabilitycut.ResourceCommandWAL && filepath.Dir(e.Path) == filepath.Join(mainDir, "wal") {
					walSyncs.Add(1)
				}
				if e.Point == durabilitycut.AfterMetaSync && e.Resource == durabilitycut.ResourceMeta && e.Path == filepath.Join(mainDir, "index.db") {
					metaSyncs.Add(1)
				}
				return nil
			})
			err := database.refreshPublicCommandWALCheckpointFallback()
			restore()
			if err != nil {
				t.Fatal(err)
			}
			after, ok := database.backend.StateToken()
			if !ok || after.AppliedCommandLSN != basis.AppliedCommandLSN || after.CommitSeq != basis.CommitSeq+1 {
				t.Fatalf("fallback changed coverage: before=%+v after=%+v", basis, after)
			}
			if needed, err := database.backend.CommandWALCheckpointFallbackNeedsRefresh(after); err != nil || needed {
				t.Fatalf("slots did not converge: needed=%v err=%v", needed, err)
			}
			if got := database.backend.CommandWALNextLSN(); got != next {
				t.Fatalf("fallback changed next command LSN: %d to %d", next, got)
			}
			if walSyncs.Load() != 0 || metaSyncs.Load() != 1 {
				t.Fatalf("fallback physical syncs: WAL=%d meta=%d, want 0 and 1", walSyncs.Load(), metaSyncs.Load())
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			value, readErr := reopened.Get([]byte("key"))
			if closeErr := reopened.Close(); readErr != nil || closeErr != nil || string(value) != "sealed" {
				t.Fatalf("reopened value=%q read=%v close=%v", value, readErr, closeErr)
			}
		})
	}
}

func TestPublicCOWSealedFallbackPropagatesJournalFlushFailure(t *testing.T) {
	database, opts, _ := cowPublicContractOpen(t, ProfileCommandWALRelaxed)
	if err := database.Set([]byte("key"), []byte("sealed")); err != nil {
		t.Fatal(err)
	}
	if err := database.cached.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis, ok := database.backend.StateToken()
	if !ok {
		t.Fatal("missing checkpoint basis")
	}
	walDir := filepath.Join(opts.Dir, "maindb", "wal")
	failure := errors.New("fallback command WAL buffer drain failed")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeUserspaceFlush && e.Resource == durabilitycut.ResourceCommandWAL && filepath.Dir(e.Path) == walDir {
			return failure
		}
		return nil
	})
	err := database.refreshPublicCommandWALCheckpointFallback()
	restore()
	if !errors.Is(err, failure) {
		t.Fatalf("fallback error=%v, want original flush failure", err)
	}
	if after, ok := database.backend.StateToken(); !ok || after != basis {
		t.Fatalf("fallback published after failed WAL flush: before=%+v after=%+v", basis, after)
	}
	if err := database.backend.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("WAL health after flush failure=%v", err)
	}
}
