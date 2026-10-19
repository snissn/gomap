package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestNativeJSONPublicRawTwoCollectionsAtomicTxn(t *testing.T) {
	const crashExitCode = 97
	childDir := os.Getenv("GOMAP_NATIVE_COMPOSITE_CRASH_DIR")
	profiles := []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast}
	if childDir != "" {
		// Keep the exact unapplied replay basis; periodic GC may checkpoint.
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
		profiles = profiles[:1]
	}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			if childDir != "" {
				dir = childDir
			}
			opts := OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Errorf("Close: %v", err)
					}
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				if _, err = manager.CreateCollection(collections.CollectionMeta{Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			if err = database.Set([]byte("raw"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			// Recovery child starts with a fully applied catalog/raw basis; only the
			// following single composite command remains unapplied on its abrupt exit.
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			diskApplied := func() uint64 {
				snapshot := handle.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("missing physical Snapshot")
				}
				token, ok := snapshot.StateToken()
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("missing physical StateToken")
				}
				return token.AppliedCommandLSN
			}
			initialApplied := diskApplied()
			held, err := manager.Begin("a", "b")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := held.Close(); err != nil {
					t.Error(err)
				}
			})
			tx, err := manager.Begin("a", "b")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			})
			if value, err := tx.GetRaw([]byte("raw")); err != nil || string(value) != "old" {
				t.Fatalf("opening raw %q: %v", value, err)
			}
			for _, name := range []string{"a", "b"} {
				if _, err = tx.GetFrom(name, []byte("x")); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("opening %s: %v", name, err)
				}
				if err = tx.InsertBatchInto(name, [][]byte{[]byte("x")}, [][]byte{[]byte(`{"v":"` + name + `"}`)}); err != nil {
					t.Fatal(err)
				}
				if value, err := tx.GetFrom(name, []byte("x")); err != nil || string(value) != `{"v":"`+name+`"}` {
					t.Fatalf("own %s %q: %v", name, value, err)
				}
			}
			if err = tx.SetRaw([]byte("raw"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			if value, err := tx.GetRaw([]byte("raw")); err != nil || string(value) != "new" {
				t.Fatalf("own raw %q: %v", value, err)
			}
			assigned := func() uint64 {
				if profile == ProfileNoWALFast {
					return 0
				}
				n, err := strconv.ParseUint(handle.Stats()["treedb.command_wal.live_accepted_max_lsn"], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			before := assigned()
			if err = tx.Commit(); err != nil {
				t.Fatalf("FIRST PUBLIC COMPOSITE COMMIT: %v", err)
			}
			if profile != ProfileNoWALFast && assigned() != before+1 {
				t.Fatalf("composite assigned %d -> %d", before, assigned())
			}
			t.Logf("PUBLIC COMPOSITE ACK profile=%s lsn=%d", profile, assigned())
			if childDir != "" {
				applied := diskApplied()
				if applied != initialApplied || applied >= assigned() {
					t.Fatalf("child composite not unapplied: initial=%d disk=%d accepted=%d", initialApplied, applied, assigned())
				}
				t.Logf("CRASH CUT initial=%d disk=%d accepted=%d", initialApplied, applied, assigned())
				os.Exit(crashExitCode)
			}
			for _, name := range []string{"a", "b"} {
				if _, err = held.GetFrom(name, []byte("x")); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("old %s: %v", name, err)
				}
			}
			if value, err := held.GetRaw([]byte("raw")); err != nil || string(value) != "old" {
				t.Fatalf("old raw %q: %v", value, err)
			}
			if err = held.Close(); err != nil {
				t.Fatal(err)
			}
			// Native read -> concurrent native write, with a raw write in the losing body.
			loser, err := manager.Begin("a", "b")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = loser.GetFrom("b", []byte("conflict")); !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatal(err)
			}
			if err = manager.InsertBatch("b", [][]byte{[]byte("conflict")}, [][]byte{[]byte(`{"n":1}`)}); err != nil {
				t.Fatal(err)
			}
			if err = loser.SetRaw([]byte("loser"), []byte("no")); err != nil {
				t.Fatal(err)
			}
			if err = loser.InsertBatchInto("a", [][]byte{[]byte("lost")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
				t.Fatal(err)
			}
			before = assigned()
			if err = loser.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
				t.Fatalf("native read conflict: %v", err)
			}
			if assigned() != before {
				t.Fatal("conflict assigned")
			}
			if err = loser.Close(); err != nil {
				t.Fatal(err)
			}
			// Raw read -> concurrent raw write, with native writes in the losing body.
			loser, err = manager.Begin("a", "b")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = loser.GetRaw([]byte("raw")); err != nil {
				t.Fatal(err)
			}
			if err = database.Set([]byte("raw"), []byte("newer")); err != nil {
				t.Fatal(err)
			}
			if err = loser.InsertBatchInto("a", [][]byte{[]byte("lost")}, [][]byte{[]byte(`{"n":3}`)}); err != nil {
				t.Fatal(err)
			}
			before = assigned()
			if err = loser.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
				t.Fatalf("raw read conflict: %v", err)
			}
			if assigned() != before {
				t.Fatal("raw conflict assigned")
			}
			if err = loser.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			registry, cache := handle.ValueLogIdentityPinRegistry(), database.cached
			closed = true
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.ExternalLeases != 0 || got.Views != 0 || got.Generations != 0 {
				t.Fatalf("final resources %+v", got)
			}
			if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || registry.CleanupError() != nil {
				t.Fatalf("pins %+v %v", got, registry.CleanupError())
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			reopenedClosed := false
			t.Cleanup(func() {
				if !reopenedClosed {
					if err := reopened.Close(); err != nil {
						t.Errorf("reopened cleanup Close: %v", err)
					}
				}
			})
			h, err := reopened.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			m, err := collections.NewNativeJSONManager(h)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				if value, err := m.Get(name, []byte("x")); err != nil || string(value) != `{"v":"`+name+`"}` {
					t.Fatalf("reopen %s %q: %v", name, value, err)
				}
			}
			if value, err := reopened.Get([]byte("raw")); err != nil || string(value) != "newer" {
				t.Fatalf("reopen raw %q: %v", value, err)
			}
			reopenedClosed = true
			if err = reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if childDir != "" {
		return
	}
	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestNativeJSONPublicRawTwoCollectionsAtomicTxn$", "-test.v")
	child.Env = append(os.Environ(), "GOMAP_NATIVE_COMPOSITE_CRASH_DIR="+dir)
	output, childErr := child.CombinedOutput()
	t.Logf("crash child output:\n%s", output)
	var exitErr *exec.ExitError
	if !errors.As(childErr, &exitErr) || exitErr.ExitCode() != crashExitCode || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("crash child must exit %d without race warning: %v\n%s", crashExitCode, childErr, output)
	}
	// Diagnostic runs retain the complete postdeath image BEFORE Open can
	// classify/replay/repair it. The optional directory is externally owned.
	if retainDir := os.Getenv("GOMAP_NATIVE_COMPOSITE_RETAIN_DIR"); retainDir != "" {
		if err := os.MkdirAll(retainDir, 0755); err != nil {
			t.Fatal(err)
		}
		saved, err := os.MkdirTemp(retainDir, "pre-reopen-")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.CopyFS(saved, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		t.Logf("RETAINED PRE-REOPEN DB %s", saved)
	}
	before := workstats.Read().Replay
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := Open(opts)
	after := workstats.Read().Replay
	attempted, applied, replayErrors := after.FramesAttempted-before.FramesAttempted, after.FramesApplied-before.FramesApplied, after.FrameErrors-before.FrameErrors
	t.Logf("REPLAY attempted=%d applied=%d errors=%d open=%v", attempted, applied, replayErrors, err)
	if err != nil {
		t.Fatalf("COMPOSITE REPLAY OPEN: %v", err)
	}
	replayClosed := false
	t.Cleanup(func() {
		if !replayClosed {
			if err := database.Close(); err != nil {
				t.Errorf("replay cleanup Close: %v", err)
			}
		}
	})
	physical := database.backend.AcquireSnapshot()
	if physical == nil {
		t.Fatal("missing reopened physical Snapshot")
	}
	token, tokenOK := physical.StateToken()
	if err := physical.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("REOPEN PHYSICAL appliedLSN=%d token=%v", token.AppliedCommandLSN, tokenOK)
	if !tokenOK {
		t.Error("missing reopened physical token")
	}
	h, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	m, err := collections.NewNativeJSONManager(h)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		value, err := m.Get(name, []byte("x"))
		t.Logf("REPLAY EFFECT %s=%q error=%v", name, value, err)
		if err != nil || string(value) != `{"v":"`+name+`"}` {
			t.Errorf("replay %s %q: %v", name, value, err)
		}
	}
	value, err := database.Get([]byte("raw"))
	t.Logf("REPLAY EFFECT raw=%q error=%v", value, err)
	if err != nil || string(value) != "new" {
		t.Errorf("replay raw %q: %v", value, err)
	}
	if applied != 1 {
		t.Errorf("unapplied composite frames=%d want1", applied)
	}
	cache, registry := database.cached, h.ValueLogIdentityPinRegistry()
	replayClosed = true
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.ExternalLeases != 0 || got.Views != 0 || got.Generations != 0 {
		t.Fatalf("replay resources %+v", got)
	}
	if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || registry.CleanupError() != nil {
		t.Fatalf("replay pins %+v", got)
	}
}
