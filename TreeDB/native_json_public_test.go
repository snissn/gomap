package treedb

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This discriminator proves Create activation and checkpoint/Close continuity.
// The selected larger proof
// still requires InsertBatch/Get, held old reads, newer suffix handoff, canonical
// WAL crash recovery, and recovered native catalog bootstrap.
func TestNativeJSONPublicCreateActivation(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			closed = true
			if err := database.Close(); err != nil {
				t.Errorf("cleanup PUBLIC CLOSE: %v", err)
			}
		}
	})
	manager, err := collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatal(err)
	}
	refused := collections.NewCollectionManager(database.backend)
	if _, err := refused.CreateCollection(&collections.CollectionMeta{Name: "legacy"}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy admission: %v", err)
	}
	if _, _, err := refused.ListCollectionsBounded(1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy list: %v", err)
	}
	if err := refused.Close(); err != nil {
		t.Fatal(err)
	}
	created, err := manager.CreateCollection(collections.CollectionMeta{Name: "native-json"})
	if err != nil {
		t.Fatalf("FIRST PUBLIC CREATE: %v", err)
	}
	if created.Name != "native-json" {
		t.Fatal(created)
	}
	t.Log("FIRST PUBLIC CREATE installed")
	captured, err := database.backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if captured.State() == nil || captured.LogicalSequence() == 0 {
		t.Fatal("Create did not install native engine at a logical sequence")
	}
	createdSequence := captured.LogicalSequence()
	if err := captured.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("CAPTURED NATIVE STATE retained and released")
	for pass := 0; pass < 3; pass++ {
		// A real ordinary write forces another physical build while the native
		// catalog is unchanged. Idle checkpoints alone can skip that build.
		if pass == 1 {
			if err := database.Set([]byte("mixed-raw"), []byte("after-native-create")); err != nil {
				t.Fatalf("PUBLIC RAW SET: %v", err)
			}
			t.Log("PUBLIC RAW SET installed")
		}
		wantSequence, wantLSN := createdSequence, uint64(1)
		if pass > 0 {
			wantSequence++
			wantLSN++
			value, err := database.Get([]byte("mixed-raw"))
			if err != nil || string(value) != "after-native-create" {
				t.Fatalf("PUBLIC RAW GET pass %d: value %q, error %v", pass, value, err)
			}
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("PUBLIC CHECKPOINT pass %d: %v", pass, err)
		}
		current, err := database.backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		sequence, hasState := current.LogicalSequence(), current.State() != nil
		basis, ok := current.Snapshot().StateToken()
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		if !hasState || sequence != wantSequence || !ok || basis.AppliedCommandLSN != wantLSN {
			t.Fatalf("PUBLIC CHECKPOINT pass %d: native=%v logical=%d want=%d basis=%+v wantLSN=%d", pass, hasState, sequence, wantSequence, basis, wantLSN)
		}
		t.Logf("PUBLIC CHECKPOINT pass %d completed", pass)
	}
	cache := database.cached // retain the original diagnostic owner through Close
	closed = true
	if err := database.Close(); err != nil {
		t.Fatalf("PUBLIC CLOSE: %v", err)
	}
	t.Log("PUBLIC CLOSE completed")
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
		t.Fatalf("original final accounting: %+v", got)
	}
	t.Log("ORIGINAL FINAL ACCOUNTING zero")
}

// This stage exercises real row publication and old captured reads. Crash replay
// and complete recovered catalog import remain separate required assertions.
func TestNativeJSONPublicRowsCapturedCheckpoint(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 2
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
				t.Errorf("cleanup Close: %v", err)
			}
		}
	})
	manager, err := collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.CreateCollection(collections.CollectionMeta{Name: "rows"}); err != nil {
		t.Fatal(err)
	}
	if err = manager.InsertBatch("rows", [][]byte{[]byte("a")}, [][]byte{[]byte(`{"n":1}`)}); err != nil {
		t.Fatalf("FIRST INSERT: %v", err)
	}
	t.Log("FIRST PUBLIC INSERT installed")
	if got, err := manager.Get("rows", []byte("a")); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("first Get %q: %v", got, err)
	}
	held, err := manager.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	secondManager, err := collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatalf("repeat same-owner activation: %v", err)
	}
	if got, err := secondManager.Get("rows", []byte("a")); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("second restricted manager Get %q: %v", got, err)
	}
	heldClosed := false
	t.Cleanup(func() {
		if !heldClosed {
			if err := held.Close(); err != nil {
				t.Errorf("held Close: %v", err)
			}
		}
	})
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	database.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		once.Do(func() { close(reached) })
		<-release
		return database.preparePublicCommandWALPendingPublish(sync)
	})
	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- database.Checkpoint() }()
	<-reached
	if err = manager.InsertBatch("rows", [][]byte{[]byte("b")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
		close(release)
		<-checkpointDone
		t.Fatalf("NEWER INSERT: %v", err)
	}
	if got, err := held.Get([]byte("a")); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("held old Get %q: %v", got, err)
	}
	if _, err := held.Get([]byte("b")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("held old absence: %v", err)
	}
	close(release)
	if err = <-checkpointDone; err != nil {
		t.Fatalf("CHECKPOINT with newer suffix: %v", err)
	}
	database.cached.SetCommandWALCheckpointPublishHook(database.preparePublicCommandWALPendingPublish)
	t.Log("REAL FROZEN CUT handed off while newer native suffix survives")
	for _, id := range []string{"a", "b"} {
		want := `{"n":1}`
		if id == "b" {
			want = `{"n":2}`
		}
		if got, err := manager.Get("rows", []byte(id)); err != nil || string(got) != want {
			t.Fatalf("post-checkpoint Get %s %q: %v", id, got, err)
		}
	}
	if got, err := held.Get([]byte("a")); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("held across checkpoint %q: %v", got, err)
	}
	heldClosed = true
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	cache := database.cached
	closed = true
	if err = database.Close(); err != nil {
		t.Fatalf("PUBLIC CLOSE: %v", err)
	}
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
		t.Fatalf("final accounting: %+v", got)
	}
}

// This complete selected proof includes an actual synced-but-unapplied WAL
// recovery. The child deliberately exits without checkpoint or Close.
func TestNativeJSONPublicCreateInsertCapturedViewCheckpointReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_NATIVE_JSON_CRASH_DIR"); dir != "" {
		opts := OptionsFor(ProfileCommandWALDurable, dir)
		opts.MemtableMode = "cow_btree"
		opts.FlushThreshold = 1 << 30
		opts.BackgroundCheckpointInterval = -1
		database, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		manager, err := collections.NewNativeJSONManager(database.backend)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"rows", "other"} {
			if _, err := manager.CreateCollection(collections.CollectionMeta{Name: name}); err != nil {
				t.Fatal(err)
			}
		}
		for _, row := range []struct{ name, id, value string }{{"rows", "a", `{"n":1}`}, {"other", "x", `{"n":7}`}} {
			if err := manager.InsertBatch(row.name, [][]byte{[]byte(row.id)}, [][]byte{[]byte(row.value)}); err != nil {
				t.Fatal(err)
			}
		}
		// ProfileCommandWALDurable sets intent.syncOnPublish; ordinary native ACK
		// appended and synced the original canonical singleton frame.
		os.Exit(0)
	}
	t.Run("captured-cut-and-newer-suffix", TestNativeJSONPublicRowsCapturedCheckpoint)
	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestNativeJSONPublicCreateInsertCapturedViewCheckpointReplay$")
	child.Env = append(os.Environ(), "GOMAP_NATIVE_JSON_CRASH_DIR="+dir)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	before := workstats.Read().Replay.FramesApplied
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("CANONICAL REPLAY OPEN: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Errorf("cleanup Close: %v", err)
			}
		}
	})
	after := workstats.Read().Replay.FramesApplied
	if after-before < 4 {
		t.Fatalf("unapplied singleton frames: before=%d after=%d", before, after)
	}
	t.Logf("ACTUAL UNAPPLIED CANONICAL REPLAY frames=%d", after-before)
	manager, err := collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatalf("recovered activation: %v", err)
	}
	for _, row := range []struct{ name, id, value string }{{"rows", "a", `{"n":1}`}, {"other", "x", `{"n":7}`}} {
		got, err := manager.Get(row.name, []byte(row.id))
		if err != nil || string(got) != row.value {
			t.Fatalf("recovered Get %s %q: %v", row.name, got, err)
		}
	}
	if err := manager.InsertBatch("rows", [][]byte{[]byte("b")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
		t.Fatalf("FIRST RECOVERED MUTATION: %v", err)
	}
	for _, row := range []struct{ name, id, value string }{{"rows", "a", `{"n":1}`}, {"rows", "b", `{"n":2}`}, {"other", "x", `{"n":7}`}} {
		got, err := manager.Get(row.name, []byte(row.id))
		if err != nil || string(got) != row.value {
			t.Fatalf("complete imported catalog Get %s/%s %q: %v", row.name, row.id, got, err)
		}
	}
	if _, err := manager.CreateCollection(collections.CollectionMeta{Name: "after-recovery"}); err != nil {
		t.Fatalf("recovered identity counter: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("recovered checkpoint: %v", err)
	}
	cache := database.cached
	closed = true
	if err := database.Close(); err != nil {
		t.Fatalf("recovered Close: %v", err)
	}
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
		t.Fatalf("recovered original accounting: %+v", got)
	}
	t.Log("RECOVERED COMPLETE CATALOG mutation/checkpoint/Close original zero")
}
