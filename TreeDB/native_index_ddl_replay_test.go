package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// FlushCommandWAL syncs only the canonical log. The child exits without Close
// or checkpoint, leaving each schema transition and its dependent CRUD for the
// original startup replay path. NoWAL checkpoint/reopen is covered separately.
func TestNativeExistingJSONIndexDDLUnappliedReplay(t *testing.T) {
	runNativeExistingIndexDDLUnappliedReplay(t, collections.DocumentFormatJSON, "TestNativeExistingJSONIndexDDLUnappliedReplay")
}
func TestNativeExistingBSONIndexDDLUnappliedReplay(t *testing.T) {
	runNativeExistingIndexDDLUnappliedReplay(t, collections.DocumentFormatBSON, "TestNativeExistingBSONIndexDDLUnappliedReplay")
}
func runNativeExistingIndexDDLUnappliedReplay(t *testing.T, format collections.DocumentFormat, selector string) {
	const sentinel = 97
	childDir, childProfile := os.Getenv("GOMAP_INDEX_DDL_CRASH_DIR"), os.Getenv("GOMAP_INDEX_DDL_PROFILE")
	if childDir != "" {
		// Keep the exact unapplied basis; periodic value-log GC may checkpoint.
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	}
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		if childDir != "" && childProfile != string(profile) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			if childDir != "" {
				dir = childDir
			} else {
				cmd := exec.Command(os.Args[0], "-test.run=^"+selector+"$", "-test.v")
				cmd.Env = append(os.Environ(), "GOMAP_INDEX_DDL_CRASH_DIR="+dir, "GOMAP_INDEX_DDL_PROFILE="+string(profile))
				output, err := cmd.CombinedOutput()
				t.Logf("uncheckpointed child:\n%s", output)
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != sentinel || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("child must exit %d without race warning: %v\n%s", sentinel, err, output)
				}
			}
			before := workstats.Read().Replay
			database, err := Open(indexedSliceOptions(profile, dir))
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = database.Close()
				}
			})
			after := workstats.Read().Replay
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() {
				if !closed {
					_ = manager.Close()
				}
			})
			if childDir != "" {
				if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: format}}); err != nil {
					t.Fatal(err)
				}
				users, err := manager.OpenCollection("users")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := users.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
					nativeIndexDDLDocument(t, format, `{"email":"a@example.com","city":"hnl"}`), nativeIndexDDLDocument(t, format, `{"email":"b@example.com","city":"sea"}`),
				}); err != nil {
					t.Fatal(err)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 {
					t.Fatalf("seed physical LSN=%d want 2", applied)
				}
				if _, err := users.InsertBatch([][]byte{[]byte("u3")}, [][]byte{nativeIndexDDLDocument(t, format, `{"email":"c@example.com","city":"hnl"}`)}); err != nil {
					t.Fatal(err)
				}
				city := collections.IndexDefinition{Name: "city", Field: "city", ValueType: collections.IndexValueString}
				email := collections.IndexDefinition{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}
				for _, index := range []collections.IndexDefinition{city, email} {
					if _, err := users.CreateIndex(index); err != nil {
						t.Fatal(err)
					}
				}
				update := func(id, document string) {
					t.Helper()
					if _, err := users.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte(id), Update: func([]byte) ([]byte, bool, error) {
						return nativeIndexDDLDocument(t, format, document), true, nil
					}}}); err != nil {
						t.Fatal(err)
					}
				}
				update("u1", `{"email":"a@example.com","city":"bos"}`)
				if _, err := users.DropIndex("city"); err != nil {
					t.Fatal(err)
				}
				if _, err := users.CreateIndex(city); err != nil {
					t.Fatal(err)
				}
				if _, err := users.DropAllIndexes(); err != nil {
					t.Fatal(err)
				}
				if _, err := users.CreateIndex(email); err != nil {
					t.Fatal(err)
				}
				update("u3", `{"email":"d@example.com","city":"hnl"}`)
				if n, err := users.DeleteBatch([][]byte{[]byte("u2")}); err != nil || n != 1 {
					t.Fatalf("delete=%d err=%v", n, err)
				}
				if err := handle.FlushCommandWAL(true); err != nil {
					t.Fatal(err)
				}
				if applied, accepted := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN()-1; applied != 2 || accepted != 12 {
					t.Fatalf("exact unapplied suffix: applied=%d accepted=%d want 2/12", applied, accepted)
				}
				t.Log("DDL UNAPPLIED SUFFIX applied=2 accepted=12: six schema transitions and four CRUD commands")
				os.Exit(sentinel)
			}
			if after.FramesAttempted-before.FramesAttempted != 10 || after.FramesApplied-before.FramesApplied != 10 || after.FrameErrors != before.FrameErrors {
				t.Fatalf("actual ten-command replay %+v -> %+v", before, after)
			}
			if applied := indexedCRUDAppliedLSN(t, handle); applied != 12 {
				t.Fatalf("replayed physical LSN=%d want 12", applied)
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			if indexes := users.Meta().Indexes; len(indexes) != 1 || indexes[0].Name != "email" || !indexes[0].Unique {
				t.Fatalf("replayed schema=%+v", indexes)
			}
			for _, row := range []struct{ id, email, document string }{
				{"u1", "a@example.com", `{"email":"a@example.com","city":"bos"}`},
				{"u3", "d@example.com", `{"email":"d@example.com","city":"hnl"}`},
			} {
				if got, err := users.Get([]byte(row.id)); err != nil || !bytes.Equal(got, nativeIndexDDLDocument(t, format, row.document)) {
					t.Fatalf("replayed primary %s=%s err=%v", row.id, got, err)
				}
				if ids, err := users.FindByIndexValue("email", row.email); err != nil || len(ids) != 1 || string(ids[0]) != row.id {
					t.Fatalf("replayed index %s=%q err=%v", row.email, ids, err)
				}
			}
			if got, err := users.Get([]byte("u2")); err != nil || got != nil {
				t.Fatalf("replayed deletion=%s err=%v", got, err)
			}
			if ids, err := users.FindByIndexValue("city", "hnl"); err != nil || ids != nil {
				t.Fatalf("replayed drop IDs=%q err=%v", ids, err)
			}
			if _, err := users.InsertBatch([][]byte{[]byte("duplicate")}, [][]byte{nativeIndexDDLDocument(t, format, `{"email":"a@example.com","city":"other"}`)}); !errors.Is(err, collections.ErrUniqueIndexConflict) {
				t.Fatalf("replayed unique index did not enforce ordinary writes: %v", err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if pins := registry.Stats(); pins.ActivePins != 0 || pins.ActiveIdentities != 0 {
				t.Fatalf("replayed close pins=%+v", pins)
			}
		})
	}
}
