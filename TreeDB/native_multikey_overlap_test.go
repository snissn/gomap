package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// A changed multikey set still contains "shared". Its delete and set have the
// same physical key, so the maintained final-effect order must survive sorting.
func TestNativeMultikeyUpdatePreservesOverlappingMemberships(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			runNativeMultikeyOverlap(t, profile, t.TempDir(), true, false)
		})
	}
}

func TestNativeMultikeyOverlapUnappliedReplay(t *testing.T) {
	const selector = "TestNativeMultikeyOverlapUnappliedReplay"
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	childDir, childProfile := os.Getenv("GOMAP_MULTIKEY_OVERLAP_DIR"), os.Getenv("GOMAP_MULTIKEY_OVERLAP_PROFILE")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		if childDir != "" && childProfile != string(profile) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			if childDir != "" {
				runNativeMultikeyOverlap(t, profile, childDir, true, true)
				return
			}
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^"+selector+"$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_MULTIKEY_OVERLAP_DIR="+dir, "GOMAP_MULTIKEY_OVERLAP_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("OVERLAP UNAPPLIED basis=2 accepted=3")) || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("unapplied child: %v\n%s", err, output)
			}
			before := workstats.Read().Replay
			runNativeMultikeyOverlap(t, profile, dir, false, false)
			after := workstats.Read().Replay
			if after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
				t.Fatalf("one actual original update frame: %+v -> %+v", before, after)
			}
		})
	}
}

func runNativeMultikeyOverlap(t *testing.T, profile Profile, dir string, stage, crash bool) {
	t.Helper()
	const count = 32
	ids := make([][]byte, count)
	var documents [2][][]byte
	for phase := range documents {
		documents[phase] = make([][]byte, count)
		for i := range ids {
			ids[i] = []byte(fmt.Sprintf("u%03d", i))
			documents[phase][i] = []byte(fmt.Sprintf(`{"tags":["phase%d","shared","phase%d"],"aliases":["phase%d-u%03d","phase%d-u%03d-alt","phase%d-u%03d"]}`, phase, phase, phase, i, phase, i, phase, i))
		}
	}
	opts := indexedSliceOptions(profile, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var old *collections.CollectionReadView
	t.Cleanup(func() {
		if old != nil {
			_ = old.Close()
		}
		if manager != nil {
			_ = manager.Close()
		}
		if database != nil {
			_ = database.Close()
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	if stage {
		if _, err = manager.CreateCollection(&collections.CollectionMeta{
			Name: "overlap", Indexes: []collections.IndexDefinition{
				{Name: "tags", Field: "tags", ValueType: collections.IndexValueString, MultiKey: true},
				{Name: "aliases", Field: "aliases", ValueType: collections.IndexValueString, MultiKey: true, Unique: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := manager.OpenCollection("overlap")
	if err != nil {
		t.Fatal(err)
	}
	checkOwners := func(index, value string, want [][]byte) {
		t.Helper()
		got, err := collection.FindByIndexValue(index, value)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s/%s=%q want=%q: %v", index, value, got, want, err)
		}
	}
	checkCurrent := func() {
		t.Helper()
		checkOwners("tags", "shared", ids)
		checkOwners("tags", "phase1", ids)
		checkOwners("tags", "phase0", nil)
		for i, id := range ids {
			got, err := collection.Get(id)
			if err != nil || !bytes.Equal(got, documents[1][i]) {
				t.Fatalf("current %s=%s want=%s: %v", id, got, documents[1][i], err)
			}
			for _, suffix := range []string{"", "-alt"} {
				checkOwners("aliases", fmt.Sprintf("phase1-u%03d%s", i, suffix), ids[i:i+1])
				checkOwners("aliases", fmt.Sprintf("phase0-u%03d%s", i, suffix), nil)
			}
		}
	}
	checkOld := func() {
		t.Helper()
		for _, value := range []string{"shared", "phase0"} {
			var got [][]byte
			err := old.VisitIndexValueIDs("tags", value, func(id []byte) error { got = append(got, bytes.Clone(id)); return nil })
			if err != nil || !reflect.DeepEqual(got, ids) {
				t.Fatalf("old tags/%s=%q want=%q: %v", value, got, ids, err)
			}
		}
		response, err := old.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != count {
			t.Fatalf("old documents=%d: %v", len(response.Results), err)
		}
		for i, result := range response.Results {
			if !result.Found || !bytes.Equal(result.Document, documents[0][i]) {
				t.Fatalf("old %s=%s", ids[i], result.Document)
			}
		}
	}
	if stage {
		if returned, err := collection.InsertBatch(ids, documents[0]); err != nil || !reflect.DeepEqual(returned, ids) {
			t.Fatalf("seed IDs=%q: %v", returned, err)
		}
		checkOwners("tags", "shared", ids)
		if err = database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		old, err = collection.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		updates := make([]collections.UpdateBatchItem, count)
		for i, id := range ids {
			document := documents[1][i]
			updates[i] = collections.UpdateBatchItem{DocumentID: id, Update: func([]byte) ([]byte, bool, error) { return document, true, nil }}
		}
		results, err := collection.UpdateBatch(updates)
		if err != nil || len(results) != count {
			t.Fatalf("update results=%d: %v", len(results), err)
		}
		for _, result := range results {
			if !result.Matched || !result.Modified {
				t.Fatalf("update result=%+v", result)
			}
		}
		checkCurrent()
		checkOld()
		if crash {
			if err = handle.FlushCommandWAL(true); err != nil {
				t.Fatal(err)
			}
			if applied, accepted := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN()-1; applied != 2 || accepted != 3 {
				t.Fatalf("unapplied basis=%d accepted=%d", applied, accepted)
			}
			t.Log("OVERLAP UNAPPLIED basis=2 accepted=3")
			os.Exit(97)
		}
		if err = database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkCurrent()
		checkOld()
		if err = old.Close(); err != nil {
			t.Fatal(err)
		}
		old = nil
	} else {
		checkCurrent()
		if applied := indexedCRUDAppliedLSN(t, handle); applied != 3 {
			t.Fatalf("replayed applied=%d want3", applied)
		}
	}
	closeCurrent := func() {
		t.Helper()
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err = manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		if err = database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("original registry: %+v", stats)
		}
	}
	closeCurrent()
	if stage {
		database, err = Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		handle, err = database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(handle)
		collection, err = manager.OpenCollection("overlap")
		if err != nil {
			t.Fatal(err)
		}
		checkCurrent()
		closeCurrent()
	}
}
