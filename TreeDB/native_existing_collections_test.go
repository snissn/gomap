package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestNativeExistingCollectionPublicLifecycle(t *testing.T) {
	const sentinel = 97
	child := os.Getenv("GOMAP_EXISTING_COLLECTION_CRASH_DIR")
	profiles := []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast}
	if child != "" {
		// Keep the exact unapplied replay basis; periodic GC may checkpoint.
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
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
			// Missing single-document mutations are no-ops; unsupported metadata
			// producers still refuse before metadata or WAL effects.
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
			}); e != nil || called {
				t.Fatalf("missing update: callback=%v error=%v", called, e)
			}
			if e := collection.Delete([]byte("a")); e != nil {
				t.Fatalf("missing delete: %v", e)
			}
			if _, e := collection.CreateIndex(collections.IndexDefinition{Name: "blocked"}); e == nil {
				t.Fatal("unsupported index accepted")
			}
			// Scalar compound indexes are supported; text metadata remains ineligible.
			if _, e := manager.CreateCollection(&collections.CollectionMeta{Name: "blocked", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "compound", ValueType: collections.IndexValueBSONOrderedV2, Components: []collections.IndexComponent{{Field: "x", Direction: collections.IndexDirectionAscending}, {Field: "y", Direction: collections.IndexDirectionAscending}}}}, TextIndexes: []collections.TextIndexDefinition{{Name: "lexical", Version: collections.TextIndexVersionV1, Analyzer: collections.TextAnalyzerSimple, Fields: []collections.TextIndexField{{Field: "body"}}}}}); !errors.Is(e, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("unsupported text metadata admission: %v", e)
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
			if metas, e := manager.ListCollections(); e != nil || len(metas) != 1 || metas[0].Name != "rows" {
				t.Fatalf("native collection listing=%+v err=%v", metas, e)
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
			checkProjection := func() {
				t.Helper()
				projected, e := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("c")}, collections.DocumentFetchOptions{ExcludePaths: []string{"n"}})
				if e != nil || len(projected.Results) != 2 || !projected.Results[0].Found || string(projected.Results[0].Document) != `{}` || projected.Results[1].Found {
					t.Fatalf("held public JSON projection: %+v %v", projected, e)
				}
				if projected.Stats.FieldsSkipped != 1 || projected.Stats.FieldsReconstructed != 0 || projected.Stats.OutputBytes != 2 || projected.Stats.DocumentsMissing != 1 {
					t.Fatalf("projection counters: %+v", projected.Stats)
				}
				included, e := held.FetchDocumentsByID([][]byte{[]byte("a")}, collections.DocumentFetchOptions{IncludePaths: []string{"n"}})
				if e != nil || string(included.Results[0].Document) != `{"n":1}` || included.Stats.FieldsReconstructed != 1 {
					t.Fatalf("held include projection: %+v %v", included, e)
				}
				for _, invalid := range []collections.DocumentFetchOptions{
					{IncludePaths: []string{"n.x"}}, {IncludePaths: []string{"n", "n"}}, {Format: collections.DocumentFormatBSON},
				} {
					if _, e := held.FetchDocumentsByID([][]byte{[]byte("a")}, invalid); e == nil {
						t.Fatalf("invalid projection options accepted: %+v", invalid)
					}
				}
			}
			checkProjection()
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkProjection()
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

// Sync is the only durability request before the child exits. In particular,
// neither an explicit public Checkpoint nor Close can repair a missed boundary.
func TestNativeExistingCollectionJournalSyncCrashReopen(t *testing.T) {
	const sentinel = 97
	const childDirEnv = "GOMAP_NATIVE_JOURNAL_SYNC_CRASH_DIR"
	const childProfileEnv = "GOMAP_NATIVE_JOURNAL_SYNC_CRASH_PROFILE"
	childDir := os.Getenv(childDirEnv)
	if childDir != "" {
		// Preserve the journal-only boundary without a periodic GC checkpoint.
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	}
	options := func(profile Profile, dir string) Options {
		opts := OptionsFor(profile, dir)
		opts.MemtableMode = "cow_btree"
		opts.FlushThreshold = 1 << 30
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		opts.MaxWALBytes = -1
		return opts
	}
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		if childDir != "" && string(profile) != os.Getenv(childProfileEnv) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := childDir
			if dir == "" {
				dir = t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingCollectionJournalSyncCrashReopen$", "-test.v")
				cmd.Env = append(os.Environ(), childDirEnv+"="+dir, childProfileEnv+"="+string(profile))
				output, err := cmd.CombinedOutput()
				t.Logf("sync child: %s", output)
				var exited *exec.ExitError
				if !errors.As(err, &exited) || exited.ExitCode() != sentinel ||
					!bytes.Contains(output, []byte("SYNC COMPLETE")) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("sync crash boundary: %v", err)
				}
			}
			database, err := Open(options(profile, dir))
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Errorf("cleanup Close: %v", err)
					}
				}
			})
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			if childDir != "" {
				if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows"}); err != nil {
					t.Fatal(err)
				}
			}
			collection, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			if childDir != "" {
				if _, err := collection.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"n":1}`)}); err != nil {
					t.Fatal(err)
				}
				held, err := collection.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := held.Close(); err != nil {
						t.Errorf("held Close: %v", err)
					}
				})
				if _, err := collection.InsertBatch([][]byte{[]byte("b")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
					t.Fatal(err)
				}
				if err := database.Set([]byte("raw-sync"), []byte("raw-value")); err != nil {
					t.Fatal(err)
				}
				accepted := backend.CommandWALNextLSN()
				for i := 0; i < 2; i++ {
					if physical, err := manager.SyncForStandaloneWriteConcern(); err != nil || !physical {
						t.Fatalf("sync %d: physical=%v error=%v", i, physical, err)
					}
					if backend.CommandWALNextLSN() != accepted {
						t.Fatal("sync assigned a new canonical command")
					}
				}
				if backend.CommandWALEnabled() {
					snapshot := backend.AcquireSnapshot()
					token, ok := snapshot.StateToken()
					if err := snapshot.Close(); err != nil {
						t.Fatal(err)
					}
					if !ok || accepted == 0 || token.AppliedCommandLSN != accepted-1 {
						t.Fatalf("sync prefix: token=%+v valid=%v next=%d", token, ok, accepted)
					}
				}
				old, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b")}, collections.DocumentFetchOptions{})
				if err != nil || len(old.Results) != 2 || !old.Results[0].Found ||
					string(old.Results[0].Document) != `{"n":1}` || old.Results[1].Found {
					t.Fatalf("old view after sync: %+v error=%v", old.Results, err)
				}
				if err := held.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for i, id := range []string{"a", "b"} {
				value, err := collection.Get([]byte(id))
				if err != nil || string(value) != []string{`{"n":1}`, `{"n":2}`}[i] {
					t.Fatalf("current/reopen %s: %s error=%v", id, value, err)
				}
			}
			if value, err := database.Get([]byte("raw-sync")); err != nil || string(value) != "raw-value" {
				t.Fatalf("current/reopen raw: %s error=%v", value, err)
			}
			if childDir != "" {
				t.Log("SYNC COMPLETE; WAL profiles covered the accepted prefix; NoWAL covered its checkpoint")
				os.Exit(sentinel)
			}
			if physical, err := manager.SyncForStandaloneWriteConcern(); err != nil || !physical {
				t.Fatalf("reopened sync: physical=%v error=%v", physical, err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if physical, err := manager.SyncForStandaloneWriteConcern(); physical || !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("closed manager sync: physical=%v error=%v", physical, err)
			}
			cache, registry := database.cached, backend.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
				t.Fatalf("reopen pins: %+v", stats)
			}
		})
	}
}
