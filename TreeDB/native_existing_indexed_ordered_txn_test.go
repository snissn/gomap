package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This uses the ordinary exported entry and one original conditional transaction.
// WAL children leave ONE actual fsynced composite frame unapplied; NoWAL covers
// explicit checkpoint/reopen only. Same-collection order is part of the oracle.
func TestNativeExistingIndexedJSONOrderedRowsAtomicReplay(t *testing.T) {
	const sentinel = 97
	childDir, childProfile := os.Getenv("GOMAP_ORDERED_ROWS_DIR"), os.Getenv("GOMAP_ORDERED_ROWS_PROFILE")
	// Parent and crash-child cases both require an exact unapplied suffix.
	// The generation scheduler may checkpoint even with automatic checkpoints disabled.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		if childDir != "" && childProfile != string(profile) {
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
			if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "users", Indexes: []collections.IndexDefinition{{Name: "city", Field: "city", ValueType: collections.IndexValueString}, {Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}}}); err != nil {
				t.Fatal(err)
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			ids := [][]byte{[]byte("u1"), []byte("u2")}
			seed := [][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`), []byte(`{"email":"b@example.com","city":"sea"}`)}
			final := [][]byte{[]byte(`{"email":"b@example.com","city":"nyc"}`), []byte(`{"email":"a@example.com","city":"nyc"}`)}
			if _, err = users.InsertBatch(ids, seed); err != nil {
				t.Fatal(err)
			}
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			basis := indexedCRUDAppliedLSN(t, handle)
			if profile != ProfileNoWALFast && basis != 2 {
				t.Fatalf("basis=%d want2", basis)
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
			native, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := native.Begin("users")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			})
			calls := [2]int{}
			items := make([]collections.UpdateBatchItem, 2)
			for i := range items {
				i := i
				items[i] = collections.UpdateBatchItem{DocumentID: ids[i], Update: func(current []byte) ([]byte, bool, error) {
					calls[i]++
					if !bytes.Equal(current, seed[i]) {
						return nil, false, fmt.Errorf("callback %d read %s", i, current)
					}
					return bytes.Clone(final[i]), true, nil
				}}
			}
			if result, err := tx.UpdateBatch(items); err != nil || len(result) != 2 || !result[0].Modified || !result[1].Modified || calls != [2]int{1, 1} {
				t.Fatalf("staged update %+v calls%v: %v", result, calls, err)
			}
			if got, err := tx.Get(ids[0]); err != nil || !bytes.Equal(got, final[0]) {
				t.Fatalf("updated Get=%s: %v", got, err)
			}
			if n, err := tx.DeleteBatch([][]byte{ids[0]}); err != nil || n != 1 {
				t.Fatalf("staged delete%d: %v", n, err)
			}
			if _, err := tx.Get(ids[0]); !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("deleted Get: %v", err)
			}
			if err := tx.InsertBatch([][]byte{ids[0]}, [][]byte{final[0]}); err != nil {
				t.Fatal(err)
			}
			got, err := tx.Get(ids[0])
			if err != nil || !bytes.Equal(got, final[0]) {
				t.Fatalf("recreated Get=%s: %v", got, err)
			}
			got[0] = '!'
			if again, err := tx.Get(ids[0]); err != nil || !bytes.Equal(again, final[0]) {
				t.Fatalf("staged output alias: %s %v", again, err)
			}
			// An intermediate unique collision that is subsequently removed is not a
			// final conflict. Insert→update callbacks observe the staged document once.
			transient := []byte(`{"email":"a@example.com","city":"tmp"}`)
			if err := tx.InsertBatch([][]byte{[]byte("transient")}, [][]byte{transient}); err != nil {
				t.Fatal(err)
			}
			transientCalls := 0
			if _, err := tx.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("transient"), Update: func(current []byte) ([]byte, bool, error) {
				transientCalls++
				if !bytes.Equal(current, transient) {
					return nil, false, errors.New("callback missed staged insert")
				}
				return []byte(`{"email":"z@example.com","city":"tmp"}`), true, nil
			}}}); err != nil || transientCalls != 1 {
				t.Fatalf("staged insert callback calls%d: %v", transientCalls, err)
			}
			if n, err := tx.DeleteBatch([][]byte{[]byte("transient")}); err != nil || n != 1 {
				t.Fatalf("net-canceled delete%d %v", n, err)
			}
			if err := tx.SetRaw([]byte("ordered/raw"), []byte("committed")); err != nil {
				t.Fatal(err)
			}
			if got, err := tx.GetRaw([]byte("ordered/raw")); err != nil || string(got) != "committed" {
				t.Fatalf("staged raw=%s %v", got, err)
			}
			before := handle.CommandWALNextLSN()
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if profile != ProfileNoWALFast && handle.CommandWALNextLSN() != before+1 {
				t.Fatal("ordered transaction did not assign exactly one LSN")
			}
			checkOrderedRowsFinal(t, database, manager)
			old, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
			if err != nil || len(old.Results) != 2 {
				t.Fatalf("held rows %+v %v", old, err)
			}
			for i := range seed {
				if !old.Results[i].Found || !bytes.Equal(old.Results[i].Document, seed[i]) {
					t.Fatalf("held%d %+v", i, old.Results[i])
				}
			}
			for _, q := range []struct{ value, id string }{{"a@example.com", "u1"}, {"b@example.com", "u2"}} {
				var got []string
				if err := held.VisitIndexValueIDs("email", q.value, func(id []byte) error { got = append(got, string(id)); return nil }); err != nil || len(got) != 1 || got[0] != q.id {
					t.Fatalf("held unique %+v %q %v", q, got, err)
				}
			}
			rejected, err := native.Begin("users")
			if err != nil {
				t.Fatal(err)
			}
			if err := rejected.InsertBatch([][]byte{[]byte("u3")}, [][]byte{final[0]}); err != nil {
				t.Fatal(err)
			}
			before = handle.CommandWALNextLSN()
			if err := rejected.Commit(); !errors.Is(err, collections.ErrUniqueIndexConflict) {
				t.Fatalf("final duplicate: %v", err)
			}
			if handle.CommandWALNextLSN() != before {
				t.Fatal("duplicate consumed LSN")
			}
			if err := rejected.Close(); err != nil {
				t.Fatal(err)
			}
			if profile != ProfileNoWALFast {
				if handle.CommandWALNextLSN()-1 != basis+1 || indexedCRUDAppliedLSN(t, handle) != basis {
					t.Fatalf("basis%d accepted%d applied%d", basis, handle.CommandWALNextLSN()-1, indexedCRUDAppliedLSN(t, handle))
				}
				if childDir != "" {
					if err := handle.FlushCommandWAL(true); err != nil {
						t.Fatal(err)
					}
					if indexedCRUDAppliedLSN(t, handle) != basis {
						t.Fatal("fsync applied suffix")
					}
					t.Logf("ORDERED UNAPPLIED basis=%d accepted=%d calls=%v", basis, handle.CommandWALNextLSN()-1, calls)
					os.Exit(sentinel)
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
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
			checkOrderedRowsFinal(t, database, manager)
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
				t.Fatalf("reopen pins %+v", s)
			}
		})
		if childDir != "" || profile == ProfileNoWALFast {
			continue
		}
		t.Run(string(profile)+"/unapplied-replay", func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingIndexedJSONOrderedRowsAtomicReplay$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_ORDERED_ROWS_DIR="+dir, "GOMAP_ORDERED_ROWS_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			t.Logf("child:\n%s", output)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != sentinel || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("child %v\n%s", err, output)
			}
			checkOrderedRowsFrame(t, indexedSliceOptions(profile, dir))
			before := workstats.Read().Replay
			database, err := Open(indexedSliceOptions(profile, dir))
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
			after := workstats.Read().Replay
			if after.FramesAttempted-before.FramesAttempted != 1 || after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
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
			if indexedCRUDAppliedLSN(t, handle) != 3 {
				t.Fatal("replay exact applied LSN !=3")
			}
			checkOrderedRowsFinal(t, database, manager)
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

func checkOrderedRowsFinal(t *testing.T, database *DB, manager *collections.CollectionManager) {
	t.Helper()
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{`{"email":"b@example.com","city":"nyc"}`, `{"email":"a@example.com","city":"nyc"}`} {
		id := []byte(fmt.Sprintf("u%d", i+1))
		got, err := users.Get(id)
		if err != nil || string(got) != want {
			t.Fatalf("final%s=%s want%s: %v", id, got, want, err)
		}
	}
	if _, found, err := users.GetInto([]byte("transient"), nil); err != nil || found {
		t.Fatalf("net canceled row found=%t %v", found, err)
	}
	if _, found, err := users.GetInto([]byte("u3"), nil); err != nil || found {
		t.Fatalf("rejected row found=%t %v", found, err)
	}
	for _, q := range []struct{ value, id string }{{"a@example.com", "u2"}, {"b@example.com", "u1"}} {
		ids, err := users.FindByIndexValue("email", q.value)
		if err != nil || len(ids) != 1 || string(ids[0]) != q.id {
			t.Fatalf("final unique %+v %q %v", q, ids, err)
		}
	}
	for _, value := range []string{"hnl", "sea", "tmp"} {
		ids, err := users.FindByIndexValue("city", value)
		if err != nil || len(ids) != 0 {
			t.Fatalf("stale scalar%s=%q %v", value, ids, err)
		}
	}
	if ids, err := users.FindByIndexValue("email", "z@example.com"); err != nil || len(ids) != 0 {
		t.Fatalf("net canceled index=%q %v", ids, err)
	}
	ids, err := users.FindByIndexValue("city", "nyc")
	if err != nil || len(ids) != 2 {
		t.Fatalf("final scalar %q %v", ids, err)
	}
	if got, err := database.Get([]byte("ordered/raw")); err != nil || string(got) != "committed" {
		t.Fatalf("final raw=%s %v", got, err)
	}
}
func checkOrderedRowsFrame(t *testing.T, opts Options) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("canonical paths%q %v", paths, err)
	}
	var suffix []commitlog.CommandEnvelope
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			env, err := reader.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if env.LSN > 2 {
				suffix = append(suffix, env)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(suffix) != 1 || suffix[0].LSN != 3 || suffix[0].Kind != commitlog.CommandKindCompositeMutation {
		t.Fatalf("outer suffix %+v", suffix)
	}
	var kinds []commitlog.CommandKind
	if err := commitlog.ScanCompositeMutationPayload(suffix[0].Payload, func(m commitlog.CompositeMutationMember) error {
		kinds = append(kinds, m.Kind)
		if m.Kind != commitlog.CommandKindRawKVBatch && m.Incarnation == 0 {
			return errors.New("missing captured incarnation")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []commitlog.CommandKind{commitlog.CommandKindRawKVBatch, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionDeleteBatchByID}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("ordered members %v want%v", kinds, want)
	}
}
