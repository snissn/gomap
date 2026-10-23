package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// WAL children leave exactly two fsynced, unapplied singleton commands. NoWAL
// exercises explicit checkpoint/reopen only; it does not claim crash recovery.
func TestNativeExistingIndexedJSONCRUDSingletonReplay(t *testing.T) {
	const sentinel = 97
	childDir, childProfile := os.Getenv("GOMAP_INDEXED_CRUD_CRASH_DIR"), os.Getenv("GOMAP_INDEXED_CRUD_PROFILE")
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
						t.Error(err)
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
						t.Error(err)
					}
				}
			})
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users", Indexes: []collections.IndexDefinition{
				{Name: "city", Field: "city", ValueType: collections.IndexValueString},
				{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			}}); err != nil {
				t.Fatal(err)
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			ids := [][]byte{[]byte("u1"), []byte("u2")}
			seed := [][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`), []byte(`{"email":"b@example.com","city":"sea"}`)}
			final := [][]byte{[]byte(`{"email":"c@example.com","city":"nyc"}`), []byte(`{"email":"a@example.com","city":"nyc"}`)}
			if got, err := users.InsertBatch(ids, seed); err != nil || len(got) != 2 {
				t.Fatalf("seed IDs=%q: %v", got, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			basis := indexedCRUDAppliedLSN(t, handle)
			if profile != ProfileNoWALFast && basis != 2 {
				t.Fatalf("seed physical LSN=%d want 2", basis)
			}
			held, err := users.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if held != nil {
					if err := held.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			checkHeld := func() {
				t.Helper()
				fetched, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
				if err != nil || len(fetched.Results) != 2 {
					t.Fatalf("held primary: %+v %v", fetched.Results, err)
				}
				for i := range seed {
					if !fetched.Results[i].Found || !bytes.Equal(fetched.Results[i].Document, seed[i]) {
						t.Fatalf("held row %d: %+v", i, fetched.Results[i])
					}
				}
				for _, q := range []struct{ index, value, id string }{{"email", "a@example.com", "u1"}, {"email", "b@example.com", "u2"}, {"city", "hnl", "u1"}, {"city", "sea", "u2"}} {
					var got []string
					err := held.VisitIndexValueIDs(q.index, q.value, func(id []byte) error { got = append(got, string(id)); return nil })
					if err != nil || len(got) != 1 || got[0] != q.id {
						t.Fatalf("held index %+v: %q %v", q, got, err)
					}
				}
			}
			calls := [2]int{}
			items := make([]collections.UpdateBatchItem, 2)
			for i := range items {
				i := i
				items[i] = collections.UpdateBatchItem{DocumentID: ids[i], Update: func(current []byte) ([]byte, bool, error) {
					calls[i]++
					if !bytes.Equal(current, seed[i]) {
						return nil, false, fmt.Errorf("update %d read %s", i, current)
					}
					return bytes.Clone(final[i]), true, nil
				}}
			}
			updated, err := users.UpdateBatch(items)
			if err != nil || len(updated) != 2 || !updated[0].Modified || !updated[1].Modified || calls != [2]int{1, 1} {
				t.Fatalf("update results=%+v calls=%v: %v", updated, calls, err)
			}
			if n, err := users.DeleteBatch([][]byte{ids[0]}); err != nil || n != 1 {
				t.Fatalf("delete count=%d: %v", n, err)
			}
			checkIndexedCRUDSingletonFinal(t, manager)
			checkHeld()
			if profile != ProfileNoWALFast {
				if accepted := handle.CommandWALNextLSN() - 1; accepted != basis+2 {
					t.Fatalf("accepted LSN=%d basis=%d", accepted, basis)
				}
				if applied := indexedCRUDAppliedLSN(t, handle); applied != basis {
					t.Fatalf("suffix applied before checkpoint: %d basis=%d", applied, basis)
				}
				if childDir != "" {
					if err := handle.FlushCommandWAL(true); err != nil {
						t.Fatal(err)
					}
					if applied := indexedCRUDAppliedLSN(t, handle); applied != basis {
						t.Fatalf("WAL fsync applied suffix: %d", applied)
					}
					t.Logf("CRUD UNAPPLIED SUFFIX basis=%d accepted=%d callbacks=%v", basis, handle.CommandWALNextLSN()-1, calls)
					os.Exit(sentinel)
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkHeld()
			checkIndexedCRUDSingletonFinal(t, manager)
			if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != basis+2 {
				t.Fatal("checkpoint did not seal exact singleton suffix")
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
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
				t.Fatalf("closed pins %+v", s)
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
			checkIndexedCRUDSingletonFinal(t, manager)
			if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != basis+2 {
				t.Fatal("reopened checkpoint LSN changed")
			}
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
		if childDir != "" || profile == ProfileNoWALFast {
			continue
		}
		t.Run(string(profile)+"/unapplied-replay", func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingIndexedJSONCRUDSingletonReplay$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_INDEXED_CRUD_CRASH_DIR="+dir, "GOMAP_INDEXED_CRUD_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			t.Logf("child:\n%s", output)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != sentinel || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("child %v\n%s", err, output)
			}
			checkIndexedCRUDSingletonFrames(t, indexedSliceOptions(profile, dir))
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
			if after.FramesAttempted-before.FramesAttempted != 2 || after.FramesApplied-before.FramesApplied != 2 || after.FrameErrors != before.FrameErrors {
				t.Fatalf("actual replay %+v -> %+v", before, after)
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
			if applied := indexedCRUDAppliedLSN(t, handle); applied != 4 {
				t.Fatalf("replayed physical LSN=%d want 4", applied)
			}
			checkIndexedCRUDSingletonFinal(t, manager)
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
				t.Fatalf("replayed pins %+v", s)
			}
		})
	}
}

func indexedCRUDAppliedLSN(t *testing.T, handle *backenddb.DB) uint64 {
	t.Helper()
	snapshot := handle.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("missing physical Snapshot")
	}
	token, ok := snapshot.StateToken()
	snapshot.Close()
	if !ok {
		t.Fatal("missing physical StateToken")
	}
	return token.AppliedCommandLSN
}

func checkIndexedCRUDSingletonFinal(t *testing.T, manager *collections.CollectionManager) {
	t.Helper()
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := users.GetInto([]byte("u1"), nil); err != nil || found {
		t.Fatalf("deleted u1 found=%t: %v", found, err)
	}
	want := []byte(`{"email":"a@example.com","city":"nyc"}`)
	if got, err := users.Get([]byte("u2")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("u2=%s want=%s: %v", got, want, err)
	}
	for _, q := range []struct {
		index, value string
		want         []string
	}{
		{"email", "a@example.com", []string{"u2"}}, {"email", "b@example.com", nil}, {"email", "c@example.com", nil},
		{"city", "hnl", nil}, {"city", "sea", nil}, {"city", "nyc", []string{"u2"}},
	} {
		ids, err := users.FindByIndexValue(q.index, q.value)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, len(ids))
		for i := range ids {
			got[i] = string(ids[i])
		}
		sort.Strings(got)
		if fmt.Sprint(got) != fmt.Sprint(q.want) {
			t.Fatalf("%s=%s IDs=%q want=%q", q.index, q.value, got, q.want)
		}
	}
}

func checkIndexedCRUDSingletonFrames(t *testing.T, opts Options) {
	t.Helper()
	// Resolve the same public root/side-store mapping as Open, rather than
	// treating its configured root as a backend database directory.
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("canonical WAL paths=%q: %v", paths, err)
	}
	var suffix []commitlog.CommandEnvelope
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			env, readErr := reader.ReadCommandFrame()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = reader.Close()
				t.Fatalf("canonical frame %s: %v", path, readErr)
			}
			if env.LSN > 2 {
				suffix = append(suffix, env)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(suffix) != 2 || suffix[0].LSN != 3 || suffix[1].LSN != 4 || suffix[0].Kind != commitlog.CommandKindCollectionUpdateBatchByID || suffix[1].Kind != commitlog.CommandKindCollectionDeleteBatchByID {
		t.Fatalf("actual singleton suffix %+v", suffix)
	}
	update, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(suffix[0].Payload)
	if err != nil || update.Collection != "users" || len(update.Documents) != 2 {
		t.Fatalf("canonical update %+v: %v", update, err)
	}
	for i, want := range []string{`{"email":"c@example.com","city":"nyc"}`, `{"email":"a@example.com","city":"nyc"}`} {
		if string(update.Documents[i].ID) != fmt.Sprintf("u%d", i+1) || string(update.Documents[i].Document) != want {
			t.Fatalf("canonical final document %d: %+v", i, update.Documents[i])
		}
	}
	deleted, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(suffix[1].Payload)
	if err != nil || deleted.Collection != "users" || len(deleted.IDs) != 1 || string(deleted.IDs[0]) != "u1" {
		t.Fatalf("canonical delete %+v: %v", deleted, err)
	}
}
