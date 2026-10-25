package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// This supported insert/read slice complements, and does not replace, the full
// indexed CRUD lifecycle. WAL children leave a real singleton plus composite
// suffix unapplied; NoWAL proves checkpoint/reopen without claiming replay.
func TestNativeExistingIndexedJSONInsertCapturedReplay(t *testing.T) {
	const sentinel = 97
	childDir, childProfile := os.Getenv("GOMAP_INDEXED_SLICE_CRASH_DIR"), os.Getenv("GOMAP_INDEXED_SLICE_PROFILE")
	// Parent and crash-child cases both require an exact unapplied suffix.
	// The generation scheduler may checkpoint even with automatic checkpoints disabled.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		if childDir != "" && string(profile) != childProfile {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			if childDir != "" {
				dir = childDir
			}
			opts := indexedSliceOptions(profile, dir)
			database, err := Open(opts)
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
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() {
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Errorf("manager Close: %v", err)
					}
				}
			})
			for _, name := range []string{"users", "other"} {
				if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name, Indexes: []collections.IndexDefinition{
					{Name: "city", Field: "city", ValueType: collections.IndexValueString},
					{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
				}}); err != nil {
					t.Fatal(err)
				}
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			seed := []byte(`{"email":"seed@example.com","city":"hnl"}`)
			if _, err := users.InsertBatch([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			physicalApplied := func() uint64 {
				snapshot := handle.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("missing physical Snapshot")
				}
				token, ok := snapshot.StateToken()
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("missing physical state")
				}
				return token.AppliedCommandLSN
			}
			basisLSN := physicalApplied()
			held, err := users.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := held.Close(); err != nil {
					t.Error(err)
				}
			})
			if ids, err := users.FindByIndexValue("missing", "x"); err != nil || ids != nil {
				t.Fatalf("Collection missing index: %q %v", ids, err)
			}
			called := false
			if err := held.VisitIndexValueIDs("missing", "x", func([]byte) error { called = true; return nil }); !errors.Is(err, collections.ErrHybridSearchIndexUnavailable) || called {
				t.Fatalf("captured missing index: called=%v error=%v", called, err)
			}
			// Reentrant fetch uses the same read scratch. Retained callback IDs and
			// returned document buffers remain independent of that scratch.
			var callbackID []byte
			if err := held.VisitIndexValueIDs("city", "hnl", func(id []byte) error {
				callbackID = id
				fetched, err := held.FetchDocumentsByID([][]byte{[]byte("seed")}, collections.DocumentFetchOptions{})
				if err != nil || len(fetched.Results) != 1 || !bytes.Equal(fetched.Results[0].Document, seed) {
					return fmt.Errorf("reentrant fetch: %+v %v", fetched.Results, err)
				}
				fetched.Results[0].Document[0] = '!'
				return nil
			}); err != nil || string(callbackID) != "seed" {
				t.Fatalf("reentrant visitor ID=%q error=%v", callbackID, err)
			}
			callbackID[0] = '!'
			closingView, err := users.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			closeCalls := 0
			if err := closingView.VisitIndexValueIDs("city", "hnl", func([]byte) error { closeCalls++; return closingView.Close() }); !errors.Is(err, backenddb.ErrClosed) || closeCalls != 1 {
				t.Fatalf("reentrant Close calls=%d error=%v", closeCalls, err)
			}
			if err := closingView.Close(); err != nil {
				t.Fatal(err)
			}
			one := []byte(`{"email":"one@example.com","city":"hnl"}`)
			ids, err := users.InsertBatch([][]byte{[]byte("one")}, [][]byte{one})
			if err != nil || len(ids) != 1 || string(ids[0]) != "one" {
				t.Fatalf("singleton Insert: %q %v", ids, err)
			}
			ids[0][0] = '!'
			native, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := native.Begin("users", "other")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			})
			for _, name := range []string{"users", "other"} {
				if err := tx.InsertBatchInto(name, [][]byte{[]byte("two")}, [][]byte{[]byte(`{"email":"two@example.com","city":"sea"}`)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.SetRaw([]byte("indexed-composite"), []byte("complete")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			if profile != ProfileNoWALFast {
				accepted := handle.CommandWALNextLSN() - 1
				if accepted != basisLSN+2 || physicalApplied() != basisLSN {
					t.Fatalf("suffix basis=%d physical=%d accepted=%d", basisLSN, physicalApplied(), accepted)
				}
				if childDir != "" {
					// Explicitly persist both relaxed frames without applying them.
					if err := handle.FlushCommandWAL(true); err != nil {
						t.Fatal(err)
					}
					if physicalApplied() != basisLSN {
						t.Fatal("flush applied suffix")
					}
					t.Logf("INDEXED UNAPPLIED SUFFIX basis=%d accepted=%d", basisLSN, accepted)
					os.Exit(sentinel)
				}
			}
			checkIndexedInsertSlice(t, manager, database)
			checkOld := func() {
				t.Helper()
				var got []string
				if err := held.VisitIndexValueIDs("city", "hnl", func(id []byte) error { got = append(got, string(id)); return nil }); err != nil || fmt.Sprint(got) != "[seed]" {
					t.Fatalf("old index=%q %v", got, err)
				}
				rows, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("one"), []byte("two")}, collections.DocumentFetchOptions{})
				if err != nil || len(rows.Results) != 3 || !rows.Results[0].Found || !bytes.Equal(rows.Results[0].Document, seed) || rows.Results[1].Found || rows.Results[2].Found {
					t.Fatalf("old rows=%+v %v", rows.Results, err)
				}
			}
			checkOld()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkIndexedInsertSlice(t, manager, database)
			checkOld()
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("pins %+v", s)
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed = false
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			checkIndexedInsertSlice(t, manager, database)
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
			cache, registry = database.cached, handle.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("reopened pins %+v", s)
			}
		})
		if childDir == "" && profile != ProfileNoWALFast {
			t.Run(string(profile)+"/unapplied-replay", func(t *testing.T) {
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingIndexedJSONInsertCapturedReplay$", "-test.v")
				cmd.Env = append(os.Environ(), "GOMAP_INDEXED_SLICE_CRASH_DIR="+dir, "GOMAP_INDEXED_SLICE_PROFILE="+string(profile))
				output, err := cmd.CombinedOutput()
				t.Logf("child:\n%s", output)
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != sentinel || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("child %v\n%s", err, output)
				}
				before := workstats.Read().Replay
				database, err := Open(indexedSliceOptions(profile, dir))
				if err != nil {
					t.Fatal(err)
				}
				after := workstats.Read().Replay
				closed := false
				t.Cleanup(func() {
					if !closed {
						if err := database.Close(); err != nil {
							t.Error(err)
						}
					}
				})
				if after.FramesApplied-before.FramesApplied != 2 || after.FrameErrors != before.FrameErrors {
					t.Fatalf("replay %+v -> %+v", before, after)
				}
				handle, err := database.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager := collections.NewCollectionManager(handle)
				t.Cleanup(func() {
					if manager != nil {
						if err := manager.Close(); err != nil {
							t.Error(err)
						}
					}
				})
				checkIndexedInsertSlice(t, manager, database)
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				manager = nil
				cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				closed = true
				requirePublicCOWFinalZero(t, cache)
				if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
					t.Fatalf("replay pins %+v", s)
				}
			})
		}
	}
}

func indexedSliceOptions(profile Profile, dir string) Options {
	opts := OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	return opts
}

func checkIndexedInsertSlice(t *testing.T, manager *collections.CollectionManager, database *DB) {
	t.Helper()
	for _, name := range []string{"users", "other"} {
		collection, err := manager.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		ids := []string{"two"}
		if name == "users" {
			ids = []string{"seed", "one", "two"}
		}
		for _, id := range ids {
			city := "hnl"
			if id == "two" {
				city = "sea"
			}
			want := fmt.Sprintf(`{"email":"%s@example.com","city":"%s"}`, id, city)
			got, err := collection.Get([]byte(id))
			if err != nil || string(got) != want {
				t.Fatalf("%s/%s=%s want=%s error=%v", name, id, got, want, err)
			}
			got[0] = '!'
			found, err := collection.FindByIndexValue("email", id+"@example.com")
			if err != nil || len(found) != 1 || string(found[0]) != id {
				t.Fatalf("%s email/%s=%q %v", name, id, found, err)
			}
			found[0][0] = '!'
			found, err = collection.FindByIndexValue("email", id+"@example.com")
			if err != nil || len(found) != 1 || string(found[0]) != id {
				t.Fatalf("owned index %s/%s=%q %v", name, id, found, err)
			}
		}
		city := []string{"one", "seed"}
		if name == "other" {
			city = nil
		}
		found, err := collection.FindByIndexValue("city", "hnl")
		gotCity := make([]string, len(found))
		for i := range found {
			gotCity[i] = string(found[i])
		}
		sort.Strings(gotCity)
		if err != nil || fmt.Sprint(gotCity) != fmt.Sprint(city) {
			t.Fatalf("%s city hnl=%q want=%q error=%v", name, gotCity, city, err)
		}
		found, err = collection.FindByIndexValue("city", "sea")
		if err != nil || len(found) != 1 || string(found[0]) != "two" {
			t.Fatalf("%s city sea=%q %v", name, found, err)
		}
	}
	value, err := database.Get([]byte("indexed-composite"))
	if err != nil || string(value) != "complete" {
		t.Fatalf("raw=%q %v", value, err)
	}
}
