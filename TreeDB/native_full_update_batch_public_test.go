package treedb

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// Full and ordinary callback UPDATE meet at the maintained declared parser and
// retained-document policy. Exercise both the physical and pending input cuts.
func TestNativeFullRetainedUpdateBatchPublicLifecycle(t *testing.T) {
	dir := t.TempDir()
	crash := os.Getenv("GOMAP_FULL_UPDATE_CRASH_DIR")
	if crash != "" {
		dir = crash
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	}
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var held, current *collections.CollectionReadView
	t.Cleanup(func() {
		for _, view := range []*collections.CollectionReadView{current, held} {
			if view != nil {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
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
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "events", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull,
		RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		Columns: []collections.ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart},
		},
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	before := nativeFullRetainedDocuments()
	// Expected whole documents are independent of callbacks and typed decoding.
	after := nativeFullRetainedUpdateDocuments()
	insert := func(id string, expected []byte) {
		t.Helper()
		input := bytes.Clone(expected)
		ids, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{input})
		if err != nil || len(ids) != 1 || string(ids[0]) != id {
			t.Fatalf("Full seed %s: ids=%q err=%v", id, ids, err)
		}
		clear(input)
	}
	insert("a", before[0])
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis, valid := handle.StateToken()
	if !valid || basis.AppliedCommandLSN != 2 || handle.CommandWALNextLSN() != 3 {
		t.Fatalf("Full physical A basis=%+v valid=%v next=%d", basis, valid, handle.CommandWALNextLSN())
	}
	insert("b", before[1])
	if physical, valid := handle.StateToken(); !valid || physical != basis || handle.CommandWALNextLSN() != 4 {
		t.Fatalf("Full pending B changed physical basis=%+v valid=%v next=%d", physical, valid, handle.CommandWALNextLSN())
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkView := func(view *collections.CollectionReadView, expected [][]byte) {
		t.Helper()
		got, err := view.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 2 {
			t.Fatalf("Full held read: %+v %v", got.Results, err)
		}
		for i := range expected {
			if !got.Results[i].Found {
				t.Fatalf("Full view row %d missing", i)
			}
			requireNativeColumnJSON(t, got.Results[i].Document, expected[i])
			clear(got.Results[i].Document)
		}
	}
	checkRows := func(expected [][]byte) {
		t.Helper()
		for i, id := range []string{"a", "b"} {
			for pass := 0; pass < 2; pass++ {
				got, err := col.Get([]byte(id))
				if err != nil {
					t.Fatal(err)
				}
				requireNativeColumnJSON(t, got, expected[i])
				clear(got)
			}
		}
	}
	checkRows(before)
	checkView(held, before)
	shared := make([]byte, 0, max(len(after[0]), len(after[1])))
	items := make([]collections.UpdateBatchItem, 2)
	calls := [2]int{}
	for i, id := range []string{"a", "b"} {
		items[i] = collections.UpdateBatchItem{DocumentID: []byte(id), Update: func(document []byte) ([]byte, bool, error) {
			calls[i]++
			requireNativeColumnJSON(t, document, before[i])
			clear(document) // Callback input cannot mutate either retained cut.
			shared = append(shared[:0], after[i]...)
			return shared, true, nil
		}}
	}
	next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	results, err := col.UpdateBatch(items)
	if err != nil {
		t.Fatalf("Full UpdateBatch physical A + pending B: %v", err)
	}
	if calls != [2]int{1, 1} || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("Full UPDATE results=%+v calls=%v", results, calls)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	clear(shared) // Both replacements used this backing; the first was overwritten.
	for i := range items {
		clear(items[i].DocumentID)
	}
	checkRows(after)
	checkView(held, before)
	current, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkView(current, after)
	if physical, valid := handle.StateToken(); !valid || physical != basis || handle.CommandWALNextLSN() != 5 {
		t.Fatalf("Full UPDATE must remain unapplied until checkpoint: %+v valid=%v next=%d", physical, valid, handle.CommandWALNextLSN())
	}
	if crash != "" {
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		checkNativeFullRetainedUpdateFrame(t, opts, after)
		if physical, valid := handle.StateToken(); !valid || physical != basis || handle.CommandWALNextLSN() != 5 {
			t.Fatalf("Full UPDATE WAL sync changed physical basis=%+v valid=%v next=%d", physical, valid, handle.CommandWALNextLSN())
		}
		t.Log("FULL UPDATE UNAPPLIED basis=2 accepted=4")
		os.Exit(97)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != 4 {
		t.Fatal("Full UPDATE was not checkpointed at its original command identity")
	}
	checkRows(after)
	checkView(held, before)
	checkView(current, after)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	current = nil
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	closeOriginal := func() {
		t.Helper()
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
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("Full UPDATE final registry=%+v", stats)
		}
	}
	closeOriginal()
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	col, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	checkRows(after)
	if indexedCRUDAppliedLSN(t, handle) != 4 || handle.CommandWALNextLSN() != 5 {
		t.Fatal("Full UPDATE reopen omitted or duplicated command identity")
	}
	closeOriginal()
}

func nativeFullRetainedUpdateDocuments() [][]byte {
	return [][]byte{
		[]byte(`{"row_id":11,"kind":"changed-alpha","payload":{"complete":[false,11,"updated-a"],"extra":{"a":true}}}`),
		[]byte(`{"row_id":22,"kind":"changed-beta","payload":{"complete":[true,22,"updated-b"],"extra":{"b":false}}}`),
	}
}

// The child uses the same public callback UPDATE as the lifecycle control, then
// exits without applying INSERT@3 or UPDATE@4 to the physical basis at LSN 2.
func TestNativeFullRetainedUnappliedUpdateReplay(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeFullRetainedUpdateBatchPublicLifecycle$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_FULL_UPDATE_CRASH_DIR="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("FULL UPDATE UNAPPLIED basis=2 accepted=4")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("Full UPDATE abrupt child: %v\n%s", err, output)
	}
	t.Logf("child:\n%s", output)
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	for reopen := 0; reopen < 2; reopen++ {
		before := workstats.Read().Replay
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
		after := workstats.Read().Replay
		if after.FramesApplied-before.FramesApplied != uint64(2*(1-reopen)) || after.FrameErrors != before.FrameErrors {
			t.Fatalf("Full UPDATE actual replay %d: %+v -> %+v", reopen, before, after)
		}
		handle, err := database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(handle)
		col, err := manager.OpenCollection("events")
		if err != nil {
			t.Fatal(err)
		}
		expected := nativeFullRetainedUpdateDocuments()
		checkRows := func() {
			t.Helper()
			for i, id := range []string{"a", "b"} {
				for pass := 0; pass < 2; pass++ {
					got, err := col.Get([]byte(id))
					if err != nil {
						t.Fatal(err)
					}
					requireNativeColumnJSON(t, got, expected[i])
					clear(got)
				}
			}
		}
		checkIdentity := func() {
			t.Helper()
			if indexedCRUDAppliedLSN(t, handle) != 4 || handle.CommandWALNextLSN() != 5 {
				t.Fatal("Full UPDATE replay lost or duplicated canonical identity")
			}
		}
		checkRows()
		checkIdentity()
		held, err = col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkRows()
		checkIdentity()
		for pass := 0; pass < 2; pass++ {
			got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b")}, collections.DocumentFetchOptions{})
			if err != nil || len(got.Results) != 2 {
				t.Fatalf("Full UPDATE replay held read: %+v %v", got.Results, err)
			}
			for i := range expected {
				if !got.Results[i].Found {
					t.Fatalf("Full UPDATE replay held row %d missing", i)
				}
				requireNativeColumnJSON(t, got.Results[i].Document, expected[i])
				clear(got.Results[i].Document)
			}
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
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if st := registry.Stats(); st.ActivePins != 0 || st.ActiveIdentities != 0 || st.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
			t.Fatalf("Full UPDATE replay final custody: %+v cleanup=%v", st, registry.CleanupError())
		}
	}
}

func checkNativeFullRetainedUpdateFrame(t *testing.T, opts Options, expected [][]byte) {
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
			if frame.LSN < 4 {
				continue
			}
			frames++
			if frame.LSN != 4 || frame.BaseAppliedLSN != 2 || frame.Kind != commitlog.CommandKindCompositeMutation || frame.PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
				t.Fatalf("Full UPDATE canonical frame: %+v", frame)
			}
			members := 0
			if err := commitlog.ScanCompositeMutationPayload(frame.Payload, func(member commitlog.CompositeMutationMember) error {
				members++
				if member.Incarnation == 0 || member.Kind != commitlog.CommandKindCollectionUpdateBatchByID || member.PayloadFormat != commitlog.PayloadFormatCollectionUpdateBatchByIDV1 {
					t.Fatalf("Full UPDATE canonical member: %+v", member)
				}
				update, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(member.Payload)
				if err != nil {
					return err
				}
				if update.Collection != "events" || update.Colocated != nil || len(update.Documents) != 2 {
					t.Fatalf("Full UPDATE canonical documents: %+v", update)
				}
				for i, id := range []string{"a", "b"} {
					if string(update.Documents[i].ID) != id || !bytes.Equal(update.Documents[i].Document, expected[i]) {
						t.Fatalf("Full UPDATE canonical document %d: %+v", i, update.Documents[i])
					}
				}
				return nil
			}); err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if members != 1 {
				t.Fatalf("Full UPDATE canonical members=%d", members)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if frames != 1 {
		t.Fatalf("Full UPDATE unapplied frames=%d want1", frames)
	}
}
