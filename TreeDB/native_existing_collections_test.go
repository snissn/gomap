package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestNativeExistingCollectionPublicLifecycle(t *testing.T) {
	const sentinel = 97
	child := os.Getenv("GOMAP_EXISTING_COLLECTION_CRASH_DIR")
	profiles := []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast}
	if child != "" {
		profiles = profiles[:1]
	}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			if child != "" {
				dir = child
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
					if e := database.Close(); e != nil {
						t.Errorf("cleanup Close: %v", e)
					}
				}
			})
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "rows"}); err != nil {
				t.Fatalf("existing public Create: %v", err)
			}
			collection, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			// Unmigrated producers refuse before callbacks, metadata or WAL effects.
			assignedBefore := backend.CommandWALNextLSN()
			opening, e := backend.CaptureNativeSemanticState()
			if e != nil {
				t.Fatal(e)
			}
			beforeState, beforeSequence := opening.State(), opening.LogicalSequence()
			if e = opening.Close(); e != nil {
				t.Fatal(e)
			}

			called := false
			if _, _, e := collection.Update([]byte("a"), func(current []byte) ([]byte, bool, error) {
				called = true
				return []byte(`{"changed":true}`), true, nil
			}); e == nil || called {
				t.Fatalf("unsupported update: callback=%v error=%v", called, e)
			}
			if e := collection.Delete([]byte("a")); e == nil {
				t.Fatal("unsupported delete accepted")
			}
			if _, e := collection.CreateIndex(collections.IndexDefinition{Name: "blocked"}); e == nil {
				t.Fatal("unsupported index accepted")
			}
			if _, e := manager.CreateCollection(&collections.CollectionMeta{Name: "blocked", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); e == nil {
				t.Fatal("unsupported typed collection accepted")
			}
			if backend.CommandWALNextLSN() != assignedBefore {
				t.Fatal("unsupported operation assigned WAL")
			}
			afterRefusal, e := backend.CaptureNativeSemanticState()
			if e != nil {
				t.Fatal(e)
			}
			if afterRefusal.State() != beforeState || afterRefusal.LogicalSequence() != beforeSequence {
				t.Fatal("unsupported operation changed native state")
			}
			if e = afterRefusal.Close(); e != nil {
				t.Fatal(e)
			}

			manager.SetUpdateBatchDetailedStatsEnabled(true)
			manager.SetVectorIndexConstructionDecisionObserverEnabled(true)
			if _, e := manager.ListCollections(); e == nil {
				t.Fatal("unmigrated list accepted")
			}
			if _, e := manager.SyncForStandaloneWriteConcern(); e == nil {
				t.Fatal("unmigrated sync accepted")
			}

			duplicateLSN := backend.CommandWALNextLSN()
			if _, e := collection.InsertBatch([][]byte{[]byte("dup"), []byte("dup")}, [][]byte{[]byte(`{"n":0}`), []byte(`{"n":0}`)}); e == nil {
				t.Fatal("duplicate IDs within batch accepted")
			}
			if backend.CommandWALNextLSN() != duplicateLSN {
				t.Fatal("duplicate batch assigned WAL")
			}
			ids := [][]byte{[]byte("b"), []byte("a")}
			docs := [][]byte{[]byte(`{"n":2}`), []byte(`{"n":1}`)}
			hookFree := collections.NewCommandWALReplayCollectionManager(backend)
			hookHandle, e := hookFree.OpenCollection("rows")
			if e != nil {
				t.Fatal(e)
			}
			if e = hookFree.Close(); e != nil {
				t.Fatal(e)
			}
			if _, e = hookHandle.Get([]byte("a")); e == nil {
				t.Fatal("closed hook-free manager remained readable")
			}
			got, err := collection.InsertBatch(ids, docs)
			if err != nil {
				t.Fatalf("existing public InsertBatch: %v", err)
			}
			if len(got) != 2 || !bytes.Equal(got[0], ids[0]) || !bytes.Equal(got[1], ids[1]) {
				t.Fatalf("returned IDs: %q", got)
			}
			for i, id := range ids {
				v, e := collection.Get(id)
				if e != nil || !bytes.Equal(v, docs[i]) {
					t.Fatalf("Get %q: %q %v", id, v, e)
				}
			}
			if _, err = collection.InsertBatch(ids, docs); err == nil {
				t.Fatal("duplicate accepted")
			}
			if child != "" {
				snap := backend.AcquireSnapshot()
				token, _ := snap.StateToken()
				if err = snap.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("PRECRASH applied=%d accepted=%d", token.AppliedCommandLSN, backend.CommandWALNextLSN()-1)
				if token.AppliedCommandLSN >= backend.CommandWALNextLSN()-1 {
					t.Fatal("no unapplied WAL")
				}
				os.Exit(sentinel)
			}
			held, err := collection.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := held.Close(); e != nil {
					t.Errorf("held Close: %v", e)
				}
			})
			if _, err = collection.InsertBatch([][]byte{[]byte("c")}, [][]byte{[]byte(`{"n":3}`)}); err != nil {
				t.Fatal(err)
			}
			old, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("c")}, collections.DocumentFetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(old.Results) != 2 || !old.Results[0].Found || string(old.Results[0].Document) != `{"n":1}` || old.Results[1].Found {
				t.Fatalf("old complete view: %+v", old.Results)
			}
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if v, e := collection.Get([]byte("c")); e != nil || string(v) != `{"n":3}` {
				t.Fatalf("new Get: %s %v", v, e)
			}
			if err = held.Close(); err != nil {
				t.Fatal(err)
			}
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			registry, cache := backend.ValueLogIdentityPinRegistry(), database.cached
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("pins: %+v", s)
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed = false
			backend, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(backend)
			collection, err = manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			for i, id := range []string{"a", "b", "c"} {
				v, e := collection.Get([]byte(id))
				if e != nil || string(v) != []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}[i] {
					t.Fatalf("reopen %s: %s %v", id, v, e)
				}
			}
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			cache = database.cached
			registry = backend.ValueLogIdentityPinRegistry()
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("reopen pins: %+v", s)
			}
		})
	}
	if child != "" {
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingCollectionPublicLifecycle$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_EXISTING_COLLECTION_CRASH_DIR="+dir)
	output, err := cmd.CombinedOutput()
	t.Logf("child: %s", output)
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != sentinel || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("crash boundary: %v", err)
	}
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	before := workstats.Read().Replay
	database, err := Open(opts)
	after := workstats.Read().Replay
	t.Logf("replay delta: %+v -> %+v", before, after)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if e := database.Close(); e != nil {
				t.Errorf("replay cleanup: %v", e)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	collection, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b"} {
		v, e := collection.Get([]byte(id))
		if e != nil || string(v) != []string{`{"n":1}`, `{"n":2}`}[i] {
			t.Fatalf("replayed %s: %s %v", id, v, e)
		}
	}
	if after.FramesApplied-before.FramesApplied != 2 {
		t.Fatalf("replayed frames: %d", after.FramesApplied-before.FramesApplied)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	cache, registry := database.cached, backend.ValueLogIdentityPinRegistry()
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("replay pins: %+v", s)
	}
}

func requirePublicCOWFinalZero(t *testing.T, cache *caching.DB) {
	t.Helper()
	if s := cache.COWMemoryStats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
		t.Fatalf("final resources: %+v", s)
	}
}
