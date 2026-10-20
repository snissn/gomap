package treedb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	slashpath "path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// Full is the maintained ordinary JSON compatibility policy. The declared
// columns still publish real assets; direct typed-batch APIs are not used here.
func TestNativeFullRetainedPublicLifecycle(t *testing.T) {
	dir := t.TempDir()
	crash := os.Getenv("GOMAP_FULL_RETAINED_CRASH_DIR")
	if crash != "" {
		dir = crash
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	}
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var held *collections.CollectionReadView
	t.Cleanup(func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	meta := &collections.CollectionMeta{Name: "events", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull,
		RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		Columns: []collections.ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart},
		},
	}}}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("ordinary native Full-retained Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	docs := nativeFullRetainedDocuments()
	insert := func(id string, doc []byte) {
		t.Helper()
		next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		input := bytes.Clone(doc)
		ids, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{input})
		if err != nil || len(ids) != 1 || string(ids[0]) != id {
			t.Fatalf("ordinary Full-retained insert %s: ids=%q err=%v", id, ids, err)
		}
		requireNativeTypedOpsAdvance(t, database, handle, next, seq)
		input[0] = '!'
	}
	insert("a", docs[0])
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 {
		t.Fatalf("seed applied=%d want2", applied)
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	insert("b", docs[1])
	checkNativeFullRetainedDocuments(t, col, 2)
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 2 {
			t.Fatalf("Full held read: %+v %v", got, err)
		}
		if !got.Results[0].Found || got.Results[1].Found {
			t.Fatalf("Full held visibility: %+v", got.Results)
		}
		requireNativeColumnJSON(t, got.Results[0].Document, docs[0])
		got.Results[0].Document[0] = '!'
	}
	checkHeld()
	if crash != "" {
		if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 || handle.CommandWALNextLSN() != 4 {
			t.Fatalf("Full child basis=%d next=%d", applied, handle.CommandWALNextLSN())
		}
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		checkNativeFullRetainedInsertFrame(t, opts, docs[1])
		if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 {
			t.Fatalf("WAL flush applied Full insert: %d", applied)
		}
		t.Log("FULL RETAINED UNAPPLIED basis=2 accepted=3")
		os.Exit(97)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if applied := indexedCRUDAppliedLSN(t, handle); applied != 3 {
		t.Fatalf("Full applied=%d want3", applied)
	}
	checkHeld()
	checkNativeFullRetainedDocuments(t, col, 2)
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
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if st := registry.Stats(); st.ActivePins != 0 || st.ActiveIdentities != 0 {
		t.Fatalf("Full final pins: %+v", st)
	}

	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	// Import the opening root through its original enrollment before opening a
	// manager. No manifest decoder or independently manufactured asset is used.
	path, offset := nativeFullRetainedTypedAsset(t, handle)
	manager = collections.NewCollectionManager(handle)
	col, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeFullRetainedDocuments(t, col, 2)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var original [1]byte
	if _, err := file.ReadAt(original[:], offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	mutated := [1]byte{original[0] ^ 0xff}
	if _, err := file.WriteAt(mutated[:], offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	var observed [1]byte
	if _, err := file.ReadAt(observed[:], offset); err != nil || observed != mutated || observed == original {
		_ = file.Close()
		t.Fatalf("Full typed asset mutation was not persisted: original=%x mutated=%x observed=%x err=%v", original, mutated, observed, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	// Full reads the complete retained JSON, independently of the declared
	// typed payload just corrupted, matching the maintained policy fixture.
	checkNativeFullRetainedDocuments(t, col, 2)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, registry = database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if st := registry.Stats(); st.ActivePins != 0 || st.ActiveIdentities != 0 {
		t.Fatalf("Full reopened final pins: %+v", st)
	}
}

func nativeFullRetainedDocuments() [][]byte {
	return [][]byte{[]byte(`{"row_id":1,"kind":"alpha","payload":{"complete":[true,1,"seed"]}}`), []byte(`{"row_id":2,"kind":"beta","payload":{"complete":[false,2,"later"]}}`)}
}

func checkNativeFullRetainedDocuments(t *testing.T, col *collections.Collection, count int) {
	t.Helper()
	for i, doc := range nativeFullRetainedDocuments()[:count] {
		id := []byte{byte('a' + i)}
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("Full Get %s: %v", id, err)
		}
		requireNativeColumnJSON(t, got, doc)
		got[0] = '!'
		got, err = col.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, doc)
	}
}

func nativeFullRetainedTypedAsset(t *testing.T, handle *backenddb.DB) (string, int64) {
	t.Helper()
	capture, err := handle.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	cleanupFailed := false
	defer func() {
		if !cleanupFailed {
			if err := capture.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	state, ok := capture.Snapshot().StateToken()
	if !ok {
		t.Fatal("Full opening capture has no state")
	}
	roots, err := handle.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owner := roots.NativeResourceMetadataOwner()
	if owner == nil {
		if err := roots.ReleaseWithError(); err != nil {
			cleanupFailed = true
			t.Error(err)
		}
		t.Fatal("Full original resource metadata owner missing")
	}
	enrollment, err := owner.Enroll(capture.NativeAllocator())
	if err != nil {
		if releaseErr := roots.ReleaseWithError(); releaseErr != nil {
			cleanupFailed = true
			t.Error(releaseErr)
		}
		t.Fatal(err)
	}
	defer func() {
		if !cleanupFailed {
			enrollment.Close()
		}
	}()
	defer func() {
		if roots != nil {
			if err := roots.ReleaseWithError(); err != nil {
				cleanupFailed = true
				t.Error(err)
			}
		}
	}()
	resources, err := roots.ImportNativeResourcesForRoot(backenddb.RecoverableRoot{CommitSeq: state.CommitSeq, UserRootPageID: state.RootPageID, SystemRootPageID: state.SystemRootPageID, AppliedCommandLSN: state.AppliedCommandLSN, MaxEntryRevision: uint64(state.MaxEntryRevision)})
	if resources != nil {
		defer func() {
			if err := resources.ReleaseWithError(); err != nil {
				cleanupFailed = true
				t.Error(err)
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	if resources == nil {
		t.Fatal("Full opening resources missing")
	}
	if err := roots.ReleaseWithError(); err != nil {
		cleanupFailed = true
		t.Fatal(err)
	}
	roots = nil
	var path string
	var offset int64
	if err := resources.WalkLogicalObligations(func(physical rootpublication.StableResourcePhysicalDescriptor, logical rootpublication.StableLogicalObligation) error {
		if path == "" && physical.Kind == rootpublication.ResourceTypedColumnAsset && logical.Kind == string(collections.ColumnAssetKindTCS1TypedColumnPart) && logical.Length > 0 {
			path, offset = strings.Clone(physical.DiagnosticPath()), logical.Offset
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Resource diagnostics use slash-separated canonical paths on every OS.
	if path == "" || filepath.IsAbs(path) || slashpath.Clean(path) != path || strings.HasPrefix(path, "..") {
		t.Fatalf("Full root has no canonical typed part: path=%q offset=%d", path, offset)
	}
	return filepath.Join(handle.Dir(), filepath.FromSlash(path)), offset
}

func TestNativeFullRetainedUnappliedInsertReplay(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeFullRetainedPublicLifecycle$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_FULL_RETAINED_CRASH_DIR="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("FULL RETAINED UNAPPLIED basis=2 accepted=3")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("Full abrupt child: %v\n%s", err, output)
	}
	t.Logf("child:\n%s", output)
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	for reopen := 0; reopen < 2; reopen++ {
		before := workstats.Read().Replay
		database, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		after := workstats.Read().Replay
		if after.FramesApplied-before.FramesApplied != uint64(1-reopen) || after.FrameErrors != before.FrameErrors {
			_ = database.Close()
			t.Fatalf("Full actual replay %d: %+v -> %+v", reopen, before, after)
		}
		handle, err := database.NativeSemanticBackend()
		if err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(handle)
		col, err := manager.OpenCollection("events")
		if err != nil {
			_ = manager.Close()
			_ = database.Close()
			t.Fatal(err)
		}
		checkNativeFullRetainedDocuments(t, col, 2)
		if indexedCRUDAppliedLSN(t, handle) != 3 || handle.CommandWALNextLSN() != 4 {
			t.Fatal("Full replay lost/duplicated canonical identity")
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkNativeFullRetainedDocuments(t, col, 2)
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		requirePublicCOWFinalZero(t, cache)
		if st := registry.Stats(); st.ActivePins != 0 || st.ActiveIdentities != 0 {
			t.Fatalf("Full replay final pins: %+v", st)
		}
	}
}

func checkNativeFullRetainedInsertFrame(t *testing.T, opts Options, expected []byte) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	frames := 0
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			frame, err := reader.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if frame.LSN < 3 {
				continue
			}
			frames++
			if frame.LSN != 3 || frame.Kind != commitlog.CommandKindCompositeMutation || frame.PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
				t.Fatalf("Full canonical frame: %+v", frame)
			}
			members := 0
			if err := commitlog.ScanCompositeMutationPayload(frame.Payload, func(member commitlog.CompositeMutationMember) error {
				members++
				if member.Incarnation == 0 || member.Kind != commitlog.CommandKindCollectionInsertBatchByID || member.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
					t.Fatalf("Full ordinary canonical member: %+v", member)
				}
				insert, err := commitlog.DecodeCollectionInsertBatchByIDPayload(member.Payload)
				if err != nil {
					return err
				}
				if insert.Collection != "events" || len(insert.Documents) != 1 || string(insert.Documents[0].ID) != "b" || !bytes.Equal(insert.Documents[0].Document, expected) {
					t.Fatalf("Full canonical documents: %+v", insert)
				}
				return nil
			}); err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if members != 1 {
				t.Fatalf("Full canonical members=%d", members)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if frames != 1 {
		t.Fatalf("Full unapplied frames=%d want1", frames)
	}
}
