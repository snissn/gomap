package treedb

import (
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"os/exec"
	"strings"
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
	if _, err := refused.CreateCollection(&collections.CollectionMeta{Name: "legacy", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy admission: %v", err)
	}
	if metas, truncated, err := refused.ListCollectionsBounded(1); err != nil || len(metas) != 0 || truncated {
		t.Fatalf("empty native list=%+v truncated=%v err=%v", metas, truncated, err)
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
	resume := startNativeJSONPausedCheckpoint(t, database, opts.Dir)
	if err = manager.InsertBatch("rows", [][]byte{[]byte("b")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
		if checkpointErr := resume(); checkpointErr != nil {
			t.Errorf("checkpoint after suffix error: %v", checkpointErr)
		}
		t.Fatalf("NEWER INSERT: %v", err)
	}
	if got, err := held.Get([]byte("a")); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("held old Get %q: %v", got, err)
	}
	if _, err := held.Get([]byte("b")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("held old absence: %v", err)
	}
	if err = resume(); err != nil {
		t.Fatalf("CHECKPOINT with newer suffix: %v", err)
	}
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

// Recovered physical pointer rows remain in their original roots. First mutation
// imports descriptors, not all row contents, and reads resolve via its exact cut.
func TestNativeJSONPublicRecoveredPointersDescriptorImport(t *testing.T) {
	dir := os.Getenv("GOMAP_NATIVE_JSON_POINTER_CRASH_DIR")
	childMode := dir != ""
	if !childMode {
		dir = t.TempDir()
	}
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	value := `{"body":"` + strings.Repeat("pointer-row", 128) + `"}`
	if !childMode {
		child := exec.Command(os.Args[0], "-test.run=^TestNativeJSONPublicRecoveredPointersDescriptorImport$")
		child.Env = append(os.Environ(), "GOMAP_NATIVE_JSON_POINTER_CRASH_DIR="+dir)
		if output, err := child.CombinedOutput(); err != nil {
			t.Fatalf("pointer crash child: %v\n%s", err, output)
		}
	}
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
	if childMode {
		manager, err := collections.NewNativeJSONManager(database.backend)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"rows", "other"} {
			if _, err := manager.CreateCollection(collections.CollectionMeta{Name: name}); err != nil {
				t.Fatal(err)
			}
			if err := manager.InsertBatch(name, [][]byte{[]byte("old")}, [][]byte{[]byte(value)}); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(0)
	}
	// Verify actual recovered pointer representation before native activation.
	snapshot := database.backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("nil verification snapshot")
	}
	token, ok := snapshot.StateToken()
	if !ok {
		t.Fatal("nil verification token")
	}
	originalRoots := make(map[string]uint64, 2)
	originalPointers := make(map[string]page.ValuePtr, 2)
	for _, name := range []string{"rows", "other"} {
		root, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
		if err != nil || len(root) != 8 {
			t.Fatalf("recovered root %s: %x %v", name, root, err)
		}
		entry, err := snapshot.GetEntryAtRoot(binary.BigEndian.Uint64(root), []byte("old"))
		if err != nil || entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("REAL POINTER required %s: %+v %v", name, entry, err)
		}
		originalRoots[name] = binary.BigEndian.Uint64(root)
		originalPointers[name] = entry.ValuePtr
		t.Logf("VERIFIED recovered pointer %s FileID=%d Offset=%d", name, entry.ValuePtr.FileID, entry.ValuePtr.Offset)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot = nil
	checkOriginalPointers := func(database *DB) {
		snapshot := database.backend.AcquireSnapshot()
		if snapshot == nil {
			t.Fatal("nil physical pointer check Snapshot")
		}
		token, ok := snapshot.StateToken()
		if !ok {
			t.Fatal("nil physical pointer check token")
		}
		for _, name := range []string{"rows", "other"} {
			raw, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
			if err != nil || len(raw) != 8 {
				t.Fatalf("physical root %s: %x %v", name, raw, err)
			}
			root := binary.BigEndian.Uint64(raw)
			if name == "other" && root != originalRoots[name] {
				t.Fatalf("untouched root changed: before=%d after=%d", originalRoots[name], root)
			}
			entry, err := snapshot.GetEntryAtRoot(root, []byte("old"))
			if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != originalPointers[name] {
				t.Fatalf("original pointer changed %s: %+v %v", name, entry, err)
			}
		}
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}
	manager, err := collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatal(err)
	}
	held, err := manager.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	heldClosed := false
	t.Cleanup(func() {
		if !heldClosed {
			if err := held.Close(); err != nil {
				t.Errorf("held cleanup: %v", err)
			}
		}
	})
	if err := manager.InsertBatch("rows", [][]byte{[]byte("new")}, [][]byte{[]byte(`{"n":1}`)}); err != nil {
		t.Fatalf("FIRST DESCRIPTOR-ONLY MUTATION: %v", err)
	}
	if got, err := held.Get([]byte("old")); err != nil || string(got) != value {
		t.Fatalf("held recovered pointer %q: %v", got, err)
	}
	if _, err := held.Get([]byte("new")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("held new absence: %v", err)
	}
	before := database.backend.Stats()["treedb.command_wal.live_accepted_max_lsn"]
	if err := manager.InsertBatch("rows", [][]byte{[]byte("old")}, [][]byte{[]byte(`{"bad":1}`)}); !errors.Is(err, collections.ErrDocumentExists) {
		t.Fatalf("duplicate pointer refusal: %v", err)
	}
	if after := database.backend.Stats()["treedb.command_wal.live_accepted_max_lsn"]; before == "" || before != after {
		t.Fatalf("duplicate appended: before=%s after=%s", before, after)
	}
	resume := startNativeJSONPausedCheckpoint(t, database, opts.Dir)
	if err := manager.InsertBatch("rows", [][]byte{[]byte("suffix")}, [][]byte{[]byte(`{"n":2}`)}); err != nil {
		if checkpointErr := resume(); checkpointErr != nil {
			t.Errorf("checkpoint after suffix error: %v", checkpointErr)
		}
		t.Fatal(err)
	}
	if err := resume(); err != nil {
		t.Fatalf("pointer suffix handoff: %v", err)
	}
	checkOriginalPointers(database)
	for _, name := range []string{"rows", "other"} {
		if got, err := manager.Get(name, []byte("old")); err != nil || string(got) != value {
			t.Fatalf("untouched pointer %s %q: %v", name, got, err)
		}
	}
	if got, err := held.Get([]byte("old")); err != nil || string(got) != value {
		t.Fatalf("held pointer after handoff %q: %v", got, err)
	}
	heldClosed = true
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	cache := database.cached
	registry := database.backend.ValueLogIdentityPinRegistry()
	closed = true
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
		t.Fatalf("pointer final accounting: %+v", got)
	}
	if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
		t.Fatalf("pointer final pin accounting: %+v cleanup=%v", got, registry.CleanupError())
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatalf("pointer final reopen: %v", err)
	}
	closed = false
	checkOriginalPointers(database)
	manager, err = collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"rows", "other"} {
		if got, err := manager.Get(name, []byte("old")); err != nil || string(got) != value {
			t.Fatalf("reopened pointer %s %q: %v", name, got, err)
		}
	}
	for _, row := range []struct{ id, value string }{{"new", `{"n":1}`}, {"suffix", `{"n":2}`}} {
		if got, err := manager.Get("rows", []byte(row.id)); err != nil || string(got) != row.value {
			t.Fatalf("reopened native row %s %q: %v", row.id, got, err)
		}
	}
	cache = database.cached
	registry = database.backend.ValueLogIdentityPinRegistry()
	closed = true
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
		t.Fatalf("reopened final accounting: %+v", got)
	}
	if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
		t.Fatalf("reopened final pin accounting: %+v cleanup=%v", got, registry.CleanupError())
	}
	t.Log("DESCRIPTOR-ONLY IMPORT and lazy pointer reads; original source/pin/byte zero after handoff and reopen")
}
