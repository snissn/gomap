package raftapply

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// The same deterministic apply producer must participate in the public COW
// owner's complete cut. This preserves its local physical-seal completion,
// canonical singleton frames and durable result/progress contracts.
func TestNativeCollectionDeterministicApplyPublicLifecycle(t *testing.T) {
	root := t.TempDir()
	dir, storesDir := filepath.Join(root, "db"), filepath.Join(root, "apply")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := treedb.Open(opts)
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
	progress, results := openDurableApplyStoresForTest(t, storesDir, DurableApplyStoreOptions{})
	t.Cleanup(func() {
		if progress != nil {
			closeDurableApplyStoresForTest(t, progress, results)
		}
	})
	apply := func(index uint64, raw []byte, status raftentry.ApplyStatusV1, affected int64) raftentry.ApplyResultV1 {
		t.Helper()
		next := backend.CommandWALNextLSN()
		opening, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		seq := opening.LogicalSequence()
		if err := opening.Close(); err != nil {
			t.Fatal(err)
		}
		meta := applyMeta(1, index)
		meta.SyncLocalCommandWAL = true
		result, err := ApplyCommittedEntryV1(backend, raw, meta, Options{ProgressStore: progress, ResultStore: results})
		if err != nil {
			t.Fatalf("public native deterministic apply index %d: %v result=%+v", index, err, result)
		}
		assertApplied(t, result, status, affected)
		if backend.CommandWALNextLSN() != next+1 || backend.State().AppliedCommandLSN != next {
			t.Fatalf("apply %d did not finish its exact local physical prefix: next=%d applied=%d want %d/%d", index, backend.CommandWALNextLSN(), backend.State().AppliedCommandLSN, next+1, next)
		}
		current, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		gotSeq := current.LogicalSequence()
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		if gotSeq != seq+1 {
			t.Fatalf("apply %d logical sequence=%d want %d", index, gotSeq, seq+1)
		}
		record, found, err := results.LookupApplyResult(meta.EntryID)
		if err != nil || !found || record.Result != result || record.AppliedCommandLSN != next {
			t.Fatalf("apply %d durable result=%+v found=%v err=%v", index, record, found, err)
		}
		p, found, err := progress.LookupApplyProgress(meta.EntryID)
		if err != nil || !found || p.AppliedCommandLSN != next || p.CommandDigest != result.CommandDigest {
			t.Fatalf("apply %d durable progress=%+v found=%v err=%v", index, p, found, err)
		}
		return result
	}
	create := deterministicCreateCollectionEntry(t, "users", "native-apply:create", testCreateCollectionMetaOptions{})
	apply(1, create, raftentry.ApplyStatusApplied, 1)
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	collection, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	doc1, doc2 := []byte(`{"id":"u1","city":"hnl"}`), []byte(`{"id":"u2","city":"sea"}`)
	replacement := []byte(`{"id":"u1","city":"sfo"}`)
	insert := deterministicInsertBatchEntry(t, "users", "native-apply:insert", nativewire.DocumentFormatJSON, [][]byte{[]byte("u2"), []byte("u1")}, [][]byte{doc2, doc1})
	insertResult := apply(2, insert, raftentry.ApplyStatusApplied, 2)
	held, err := collection.OpenCollectionReadView()
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
	assertJSON := func(got, want []byte) {
		t.Helper()
		var actual, expected any
		if err := json.Unmarshal(got, &actual); err != nil {
			t.Fatalf("read document %q: %v", got, err)
		}
		if err := json.Unmarshal(want, &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("document=%s want %s", got, want)
		}
	}
	checkHeld := func() {
		t.Helper()
		response, err := held.FetchDocumentsByID([][]byte{[]byte("u1"), []byte("u2")}, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 || !response.Results[0].Found || !response.Results[1].Found {
			t.Fatalf("held insert cut: %+v err=%v", response.Results, err)
		}
		assertJSON(response.Results[0].Document, doc1)
		assertJSON(response.Results[1].Document, doc2)
		response.Results[0].Document[0] = '!'
	}
	checkHeld()
	replace := deterministicReplaceBatchEntry(t, "users", "native-apply:replace", nativewire.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("missing")}, [][]byte{replacement, []byte(`{"id":"missing","city":"nyc"}`)})
	replaceResult := apply(3, replace, raftentry.ApplyStatusApplied, 1)
	if replaceResult.MatchedCount != 1 {
		t.Fatalf("replace matched=%d want 1", replaceResult.MatchedCount)
	}
	deleteEntry := deterministicDeleteBatchEntry(t, "users", "native-apply:delete", [][]byte{[]byte("u2")})
	deleteResult := apply(4, deleteEntry, raftentry.ApplyStatusApplied, 1)
	if insertResult.ResultDigest == replaceResult.ResultDigest || replaceResult.ResultDigest == deleteResult.ResultDigest {
		t.Fatal("apply logical digest did not change with document effects")
	}
	checkCurrent := func() {
		t.Helper()
		got, err := collection.Get([]byte("u1"))
		if err != nil {
			t.Fatal(err)
		}
		assertJSON(got, replacement)
		got[0] = '!'
		for _, id := range []string{"u2", "missing"} {
			if got, err := collection.Get([]byte(id)); err != nil || got != nil {
				t.Fatalf("missing %s=%q err=%v", id, got, err)
			}
		}
	}
	checkCurrent()
	checkHeld()
	frames := readCommandWALFrames(t, backend.Dir())
	if len(frames) != 4 {
		t.Fatalf("canonical frames=%d want 4", len(frames))
	}
	assertCatalogCreateFrame(t, frames[0], "users")
	assertCollectionInsertFrame(t, frames[1], "users", map[string][]byte{"u1": doc1, "u2": doc2})
	assertCollectionUpdateFrame(t, frames[2], "users", map[string][]byte{"u1": replacement})
	assertCollectionDeleteFrame(t, frames[3], "users", [][]byte{[]byte("u2")})
	for i := range frames {
		if frames[i].LSN != uint64(i+1) {
			t.Fatalf("frame %d LSN=%d want %d", i, frames[i].LSN, i+1)
		}
	}
	// The same committed entry returns its original result without another WAL
	// identity, including after reopening the original result/progress stores.
	checkDuplicate := func() {
		t.Helper()
		next := backend.CommandWALNextLSN()
		meta := applyMeta(1, 4)
		meta.SyncLocalCommandWAL = true
		got, err := ApplyCommittedEntryV1(backend, deleteEntry, meta, Options{ProgressStore: progress, ResultStore: results})
		if err != nil || got != deleteResult || backend.CommandWALNextLSN() != next {
			t.Fatalf("stored duplicate=%+v err=%v next=%d want %+v/%d", got, err, backend.CommandWALNextLSN(), deleteResult, next)
		}
		assertDurableLastApplied(t, progress, meta.EntryID)
	}
	checkDuplicate()
	// Reusing that entry identity for a different command must fail before append
	// and preserve the original durable result.
	next := backend.CommandWALNextLSN()
	conflict, err := ApplyCommittedEntryV1(backend, insert, applyMeta(1, 4), Options{ProgressStore: progress, ResultStore: results})
	assertRejected(t, conflict, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if backend.CommandWALNextLSN() != next {
		t.Fatalf("entry identity conflict appended: next=%d want %d", backend.CommandWALNextLSN(), next)
	}
	checkDuplicate()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkCurrent()
	checkHeld()
	digest, err := LogicalDigestV1ForDB(backend, LogicalDigestOptionsV1{})
	if err != nil || raftentry.CommandDigestV1(digest) != deleteResult.ResultDigest {
		t.Fatalf("final logical digest=%x err=%v want %s", digest, err, deleteResult.ResultDigest.Hex())
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	closeDurableApplyStoresForTest(t, progress, results)
	progress, results = nil, nil
	database, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	collection, err = manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	progress, results = openDurableApplyStoresForTest(t, storesDir, DurableApplyStoreOptions{})
	checkCurrent()
	checkDuplicate()
	reopenedDigest, err := LogicalDigestV1ForDB(backend, LogicalDigestOptionsV1{})
	if err != nil || reopenedDigest != digest {
		t.Fatalf("reopened digest=%x err=%v want %x", reopenedDigest, err, digest)
	}
}
