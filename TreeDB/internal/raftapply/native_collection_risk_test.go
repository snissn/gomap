package raftapply

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func nativeApplyPublicOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	return opts
}

func TestNativeCollectionDeterministicApplyNoEffectFrames(t *testing.T) {
	dir := t.TempDir()
	database, err := treedb.Open(nativeApplyPublicOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	budget, ok := capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
	if !ok {
		t.Fatal("original COW budget does not expose its maintained stats")
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	registry := backend.ValueLogIdentityPinRegistry()
	progress, results := NewMemoryApplyProgressStore(16, 16), NewMemoryApplyResultStore(16)
	options := Options{ProgressStore: progress, ResultStore: results}
	apply := func(index uint64, raw []byte, status raftentry.ApplyStatusV1, affected, matched int64) {
		t.Helper()
		before, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		sequence := before.LogicalSequence()
		if err = before.Close(); err != nil {
			t.Fatal(err)
		}
		next := backend.CommandWALNextLSN()
		meta := applyMeta(1, index)
		meta.SyncLocalCommandWAL = true
		result, err := ApplyCommittedEntryV1(backend, raw, meta, options)
		if err != nil {
			t.Fatalf("apply %d: %v result=%+v", index, err, result)
		}
		assertApplied(t, result, status, affected)
		if result.MatchedCount != matched || backend.CommandWALNextLSN() != next+1 || backend.State().AppliedCommandLSN != next {
			t.Fatalf("apply %d result=%+v next/applied=%d/%d", index, result, backend.CommandWALNextLSN(), backend.State().AppliedCommandLSN)
		}
		after, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		gotSequence := after.LogicalSequence()
		if err = after.Close(); err != nil {
			t.Fatal(err)
		}
		if gotSequence != sequence+1 {
			t.Fatalf("apply %d sequence=%d want %d", index, gotSequence, sequence+1)
		}
	}
	apply(1, deterministicCreateCollectionEntry(t, "users", "native-noop:create", testCreateCollectionMetaOptions{}), raftentry.ApplyStatusApplied, 1, 0)
	doc := []byte(`{"id":"u1","city":"hnl"}`)
	apply(2, deterministicInsertBatchEntry(t, "users", "native-noop:insert", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{doc}), raftentry.ApplyStatusApplied, 1, 0)
	apply(3, deterministicCreateCollectionEntry(t, "users", "native-noop:create-again", testCreateCollectionMetaOptions{}), raftentry.ApplyStatusAlreadyApplied, 0, 0)
	apply(4, deterministicReplaceBatchEntry(t, "users", "native-noop:replace", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1"), []byte("missing")}, [][]byte{doc, []byte(`{"id":"missing"}`)}), raftentry.ApplyStatusApplied, 0, 1)
	apply(5, deterministicDeleteBatchEntry(t, "users", "native-noop:absent-delete", [][]byte{[]byte("missing")}), raftentry.ApplyStatusApplied, 0, 0)
	apply(6, deterministicDeleteBatchEntry(t, "users", "native-noop:delete", [][]byte{[]byte("u1"), []byte("missing")}), raftentry.ApplyStatusApplied, 1, 0)
	frames := readCommandWALFrames(t, backend.Dir())
	if len(frames) != 6 {
		t.Fatalf("frames=%d want 6", len(frames))
	}
	assertCatalogCreateFrame(t, frames[2], "users")
	assertCollectionUpdateFrame(t, frames[3], "users", nil)
	assertCollectionDeleteFrame(t, frames[4], "users", [][]byte{[]byte("missing")})
	assertCollectionDeleteFrame(t, frames[5], "users", [][]byte{[]byte("u1"), []byte("missing")})
	if progress.Len() != 6 || results.Len() != 6 {
		t.Fatalf("result/progress=%d/%d", results.Len(), progress.Len())
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if s := budget.Stats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
		t.Fatalf("original final COW resources=%+v", s)
	}
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("original final identity registry=%+v", s)
	}
}

// The real fault seam runs AFTER assigned append, BEFORE visibility. Process
// death preserves that fsynced frame without allowing Close to repair the gap.
func TestNativeCollectionDeterministicApplyAssignedFailureReplay(t *testing.T) {
	const env = "GOMAP_NATIVE_APPLY_FAILURE_DIR"
	const sentinel = 97
	if dir := os.Getenv(env); dir != "" {
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
		database, err := treedb.Open(nativeApplyPublicOptions(filepath.Join(dir, "db")))
		if err != nil {
			t.Fatal(err)
		}
		backend, err := database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		progress, results := openDurableApplyStoresForTest(t, filepath.Join(dir, "apply"), DurableApplyStoreOptions{})
		opts := Options{ProgressStore: progress, ResultStore: results}
		create := deterministicCreateCollectionEntry(t, "users", "native-failure:create", testCreateCollectionMetaOptions{})
		seed := deterministicInsertBatchEntry(t, "users", "native-failure:seed", nativewire.DocumentFormatJSON, [][]byte{[]byte("seed")}, [][]byte{[]byte(`{"id":"seed","v":1}`)})
		for i, raw := range [][]byte{create, seed} {
			meta := applyMeta(1, uint64(i+1))
			meta.SyncLocalCommandWAL = true
			result, err := ApplyCommittedEntryV1(backend, raw, meta, opts)
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
		}
		manager := collections.NewCollectionManager(backend)
		collection, err := manager.OpenCollection("users")
		if err != nil {
			t.Fatal(err)
		}
		held, err := collection.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		token, ok := backend.StateToken()
		if !ok {
			t.Fatal("missing original physical state")
		}
		next := backend.CommandWALNextLSN()
		raw := deterministicInsertBatchEntry(t, "users", "native-failure:assigned", nativewire.DocumentFormatJSON, [][]byte{[]byte("new")}, [][]byte{[]byte(`{"id":"new","v":2}`)})
		meta := applyMeta(1, 3)
		meta.SyncLocalCommandWAL = true
		opts.FaultInjector = singlePointFaultInjector{point: FaultAfterLocalWALAppendBeforeVisibleV1}
		result, err := ApplyCommittedEntryV1(backend, raw, meta, opts)
		assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
		after, ok := backend.StateToken()
		if !ok || after != token || backend.CommandWALNextLSN() != next+1 {
			t.Fatalf("assigned failure moved physical cut: before=%+v after=%+v next=%d", token, after, backend.CommandWALNextLSN())
		}
		if _, found, err := results.LookupApplyResult(meta.EntryID); err != nil || found {
			t.Fatalf("failed result recorded: found=%v err=%v", found, err)
		}
		if _, found, err := progress.LookupApplyProgress(meta.EntryID); err != nil || found {
			t.Fatalf("failed progress recorded: found=%v err=%v", found, err)
		}
		if got, err := collection.Get([]byte("new")); err != nil || got != nil {
			t.Fatalf("failed row visible=%q err=%v", got, err)
		}
		response, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("new")}, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 || !response.Results[0].Found || response.Results[1].Found {
			t.Fatalf("held old cut=%+v err=%v", response.Results, err)
		}
		if err = backend.CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
			t.Fatalf("assigned failure did not poison original owner: %v", err)
		}
		frames := readCommandWALFrames(t, backend.Dir())
		if len(frames) != 3 {
			t.Fatalf("frames=%d want 3", len(frames))
		}
		assertCollectionInsertFrame(t, frames[2], "users", map[string][]byte{"new": []byte(`{"id":"new","v":2}`)})
		fmt.Println("NATIVE APPLY ASSIGNED FAILURE basis=2 accepted=3")
		os.Exit(sentinel)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeCollectionDeterministicApplyAssignedFailureReplay$", "-test.v")
	cmd.Env = append(os.Environ(), env+"="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != sentinel || !bytes.Contains(output, []byte("NATIVE APPLY ASSIGNED FAILURE basis=2 accepted=3")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("assigned-failure child: %v\n%s", err, output)
	}
	database, err := treedb.Open(nativeApplyPublicOptions(filepath.Join(dir, "db")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	if backend.State().AppliedCommandLSN != 3 || backend.CommandWALNextLSN() != 4 {
		t.Fatalf("replay applied/next=%d/%d want 3/4", backend.State().AppliedCommandLSN, backend.CommandWALNextLSN())
	}
	manager := collections.NewCollectionManager(backend)
	defer manager.Close()
	collection, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"seed", "new"} {
		got, err := collection.Get([]byte(id))
		if err != nil {
			t.Fatalf("recovered %s: %v", id, err)
		}
		var document struct {
			ID string `json:"id"`
			V  int    `json:"v"`
		}
		if err = json.Unmarshal(got, &document); err != nil || document.ID != id || document.V != i+1 {
			t.Fatalf("recovered %s=%q err=%v", id, got, err)
		}
	}
	progress, results := openDurableApplyStoresForTest(t, filepath.Join(dir, "apply"), DurableApplyStoreOptions{})
	defer closeDurableApplyStoresForTest(t, progress, results)
	if _, found, err := results.LookupApplyResult(applyMeta(1, 3).EntryID); err != nil || found {
		t.Fatalf("recovery fabricated deterministic result: found=%v err=%v", found, err)
	}
	if _, found, err := progress.LookupApplyProgress(applyMeta(1, 3).EntryID); err != nil || found {
		t.Fatalf("recovery fabricated apply progress: found=%v err=%v", found, err)
	}
}

// Public scans use one original cut even when a callback publishes another cut.
// Returned IDs/documents remain owned after traversal and its capture close.
func TestNativeCollectionPublicScanCapturedCut(t *testing.T) {
	database, err := treedb.Open(nativeApplyPublicOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "scan"}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("scan")
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("c"), []byte("a"), []byte("b")}
	docs := [][]byte{[]byte(`{"id":"c"}`), []byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	if _, err = collection.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var records []collections.DocumentRecord
	truncated, err := collection.ScanDocumentsFunc(3, func(record collections.DocumentRecord) (bool, error) {
		records = append(records, record)
		if len(records) == 1 {
			if _, err := collection.InsertBatch([][]byte{[]byte("d")}, [][]byte{[]byte(`{"id":"d"}`)}); err != nil {
				return false, err
			}
			if _, err := collection.DeleteBatch([][]byte{[]byte("c")}); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil || truncated || len(records) != 3 {
		t.Fatalf("captured scan count=%d truncated=%v err=%v", len(records), truncated, err)
	}
	for i, want := range []string{"a", "b", "c"} {
		if string(records[i].ID) != want || string(records[i].Document) != `{"id":"`+want+`"}` {
			t.Fatalf("record %d=%+v", i, records[i])
		}
		records[i].ID[0] = '!'
		records[i].Document[0] = '!'
	}
	var got []string
	truncated, err = collection.ScanDocumentIDsFunc(2, func(id []byte) (bool, error) {
		got = append(got, string(id))
		id[0] = '!'
		return true, nil
	})
	if err != nil || !truncated || fmt.Sprint(got) != "[a b]" {
		t.Fatalf("bounded ID scan=%v truncated=%v err=%v", got, truncated, err)
	}
	got = nil
	truncated, err = collection.ScanDocumentsAfterFunc([]byte("b"), 1, func(record collections.DocumentRecord) (bool, error) {
		got = append(got, string(record.ID))
		return true, nil
	})
	if err != nil || truncated || fmt.Sprint(got) != "[d]" {
		t.Fatalf("after scan=%v truncated=%v err=%v", got, truncated, err)
	}
	truncated, err = collection.ScanDocumentIDsFunc(2, func([]byte) (bool, error) { return false, nil })
	if err != nil || truncated {
		t.Fatalf("callback stop truncated=%v err=%v", truncated, err)
	}
	stop := errors.New("scan callback failure")
	_, err = collection.ScanDocumentsFunc(2, func(collections.DocumentRecord) (bool, error) { return false, stop })
	if !errors.Is(err, stop) {
		t.Fatalf("callback error=%v", err)
	}
	if value, err := collection.Get([]byte("a")); err != nil || string(value) != `{"id":"a"}` {
		t.Fatalf("copy ownership value=%s err=%v", value, err)
	}
	materializer, err := collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"id":"a"}`)
	owned, err := materializer.StoredDocumentJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	owned[0] = '!'
	if raw[0] != '{' {
		t.Fatal("materializer returned borrowed JSON")
	}
	if err = materializer.Close(); err != nil {
		t.Fatal(err)
	}
}
