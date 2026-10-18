package treedb

import (
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
	childDir := os.Getenv("GOMAP_NATIVE_COMPOSITE_CRASH_DIR")
	profiles := []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast}
	if childDir != "" {
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
				os.Exit(0)
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
			if err = reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if childDir != "" {
		return
	}
	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestNativeJSONPublicRawTwoCollectionsAtomicTxn$")
	child.Env = append(os.Environ(), "GOMAP_NATIVE_COMPOSITE_CRASH_DIR="+dir)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	before := workstats.Read().Replay.FramesApplied
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("COMPOSITE REPLAY OPEN: %v", err)
	}
	if n := workstats.Read().Replay.FramesApplied - before; n != 1 {
		t.Fatalf("unapplied composite frames=%d want1", n)
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
		if value, err := m.Get(name, []byte("x")); err != nil || string(value) != `{"v":"`+name+`"}` {
			t.Fatalf("replay %s %q: %v", name, value, err)
		}
	}
	if value, err := database.Get([]byte("raw")); err != nil || string(value) != "new" {
		t.Fatalf("replay raw %q: %v", value, err)
	}
	cache, registry := database.cached, h.ValueLogIdentityPinRegistry()
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
