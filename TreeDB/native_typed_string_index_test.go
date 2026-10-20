package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// The maintained typed API supports scalar string indexes on declared String
// RowAsset columns. Exercise that actual producer and its physical readback,
// rather than placing a second full document in the residual JSON.
func TestNativeTypedStringIndexesLifecycle(t *testing.T) {
	dir := t.TempDir()
	crashDir := os.Getenv("GOMAP_TYPED_INDEX_CRASH_DIR")
	if crashDir != "" {
		dir = crashDir
		// Preserve this dedicated abrupt child's real unapplied physical basis.
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
	fixture := nativeColumnPendingCaseFor(t, "string")
	fixture.typedSeed = &collections.TypedColumnBatch{} // Existing non-column JSON carrier.
	fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	meta := nativeColumnPendingMeta(fixture)
	meta.Indexes = []collections.IndexDefinition{
		{Name: "label_lookup", Field: "label", ValueType: collections.IndexValueString},
		{Name: "label_unique", Field: "label", ValueType: collections.IndexValueString, Unique: true},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("ordinary native typed indexed Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := nativeTypedOpsRows("string")
	check := func(rows []nativeTypedOpsRow, absent []string) {
		t.Helper()
		checkNativeTypedStringIndexes(t, col, rows, absent)
	}
	next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	ids, residual, columns := nativeTypedOpsInput("string", a, b)
	inserted, stats, err := col.InsertTypedBatchWithStats(ids, residual, columns)
	if err != nil || len(inserted) != 2 || stats.Documents != 2 {
		t.Fatalf("actual typed indexed insert: ids=%q stats=%+v err=%v", inserted, stats, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	ids[0][0], residual[0][0], columns[0].Strings[0] = '!', '!', "caller overwrote input"
	check([]nativeTypedOpsRow{a, b}, []string{"c"})
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check([]nativeTypedOpsRow{a, b}, []string{"c"})
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 3 {
			t.Fatalf("held typed indexed view: results=%d err=%v", len(got.Results), err)
		}
		for i, row := range []nativeTypedOpsRow{a, b} {
			if !got.Results[i].Found {
				t.Fatalf("held row %s missing", row.id)
			}
			checkNativeTypedOpsDocument(t, "string", got.Results[i].Document, row)
			got.Results[i].Document[0] = '!'
		}
		if got.Results[2].Found {
			t.Fatal("held cut contains later row c")
		}
	}
	// Admission must validate the final unique owners, allowing an atomic swap.
	afterA, afterB := a, b
	afterA.text, afterB.text = b.text, a.text
	ids, residual, columns = nativeTypedOpsInput("string", afterA, afterB)
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	results, err := col.ReplaceTypedBatch(ids, residual, columns)
	if err != nil || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("typed unique handoff: results=%+v err=%v", results, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	ids[0][0], residual[0][0], columns[0].Strings[0] = '!', '!', "caller overwrote replacement"
	check([]nativeTypedOpsRow{afterA, afterB}, []string{"c"})
	checkHeld()
	if crashDir != "" {
		checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionTypedBatchByIDV1, "string", []nativeTypedOpsRow{afterA, afterB}, nil)
		if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 || next != 3 || handle.CommandWALNextLSN() != 4 {
			t.Fatalf("indexed child basis=%d assigned=%d next=%d", applied, next, handle.CommandWALNextLSN())
		}
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 {
			t.Fatalf("WAL flush advanced physical basis: %d", applied)
		}
		t.Log("TYPED INDEX UNAPPLIED basis=2 accepted=3")
		os.Exit(97)
	}
	refuse := func(label string, operation func() error) {
		t.Helper()
		next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		state, ok := handle.StateToken()
		if !ok {
			t.Fatal("missing physical basis before refusal")
		}
		if err := operation(); !errors.Is(err, collections.ErrUniqueIndexConflict) {
			t.Fatalf("%s unique refusal: %v", label, err)
		}
		after, ok := handle.StateToken()
		if !ok || after != state || handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
			t.Fatalf("%s refusal changed physical/canonical/logical identity", label)
		}
		check([]nativeTypedOpsRow{afterA, afterB}, []string{"c"})
		checkHeld()
	}
	conflict := c
	conflict.text = afterA.text
	refuse("insert", func() error {
		ids, residual, columns := nativeTypedOpsInput("string", conflict)
		_, _, err := col.InsertTypedBatchWithStats(ids, residual, columns)
		return err
	})
	conflict = afterB
	conflict.text = afterA.text
	refuse("replace", func() error {
		ids, residual, columns := nativeTypedOpsInput("string", conflict)
		_, err := col.ReplaceTypedBatch(ids, residual, columns)
		return err
	})
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check([]nativeTypedOpsRow{afterA, afterB}, []string{"c"})
	checkHeld()
	// Deleting an owner must remove both secondary memberships and allow reuse.
	if count, err := col.DeleteBatch([][]byte{[]byte("b")}); err != nil || count != 1 {
		t.Fatalf("delete typed indexed owner: count=%d err=%v", count, err)
	}
	for _, index := range []string{"label_lookup", "label_unique"} {
		if ids, err := col.FindByIndex(index, afterB.text); err != nil || len(ids) != 0 {
			t.Fatalf("deleted %s owner=%q err=%v", index, ids, err)
		}
	}
	c.text = afterB.text
	ids, residual, columns = nativeTypedOpsInput("string", c)
	if inserted, _, err := col.InsertTypedBatchWithStats(ids, residual, columns); err != nil || len(inserted) != 1 {
		t.Fatalf("reuse deleted unique value: ids=%q err=%v", inserted, err)
	}
	check([]nativeTypedOpsRow{afterA, c}, []string{"b"})
	checkHeld()
	accepted := handle.CommandWALNextLSN() - 1
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := indexedCRUDAppliedLSN(t, handle); got != accepted {
		t.Fatalf("checkpoint applied=%d want=%d", got, accepted)
	}
	check([]nativeTypedOpsRow{afterA, c}, []string{"b"})
	checkHeld()
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
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("typed indexed final registry: %+v", stats)
	}
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
	check([]nativeTypedOpsRow{afterA, c}, []string{"b"})
	if indexedCRUDAppliedLSN(t, handle) != accepted || handle.CommandWALNextLSN() != accepted+1 {
		t.Fatal("reopen changed canonical/applied identity")
	}
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
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("typed indexed reopen final registry: %+v", stats)
	}
}

func checkNativeTypedStringIndexes(t *testing.T, col *collections.Collection, rows []nativeTypedOpsRow, absent []string) {
	t.Helper()
	checkNativeTypedOpsCurrent(t, col, "string", rows, absent)
	for _, row := range rows {
		for _, index := range []string{"label_lookup", "label_unique"} {
			for attempt := 0; attempt < 2; attempt++ {
				ids, err := col.FindByIndex(index, row.text)
				if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], []byte(row.id)) {
					t.Fatalf("%s/%q owners=%q want=%q err=%v", index, row.text, ids, row.id, err)
				}
				ids[0][0] = '!' // Returned IDs cannot alias retained index state.
			}
		}
	}
}

func TestNativeTypedStringIndexesUnappliedReplay(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeTypedStringIndexesLifecycle$", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAP_TYPED_INDEX_CRASH_DIR="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("TYPED INDEX UNAPPLIED basis=2 accepted=3")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("indexed typed child: %v\n%s", err, output)
	}
	t.Logf("child:\n%s", output)
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	a, b, _ := nativeTypedOpsRows("string")
	a.text, b.text = b.text, a.text
	for reopen := 0; reopen < 2; reopen++ {
		before := workstats.Read().Replay
		database, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		after := workstats.Read().Replay
		if after.FramesApplied-before.FramesApplied != uint64(1-reopen) || after.FrameErrors != before.FrameErrors {
			_ = database.Close()
			t.Fatalf("indexed typed replay=%d: %+v -> %+v", reopen, before, after)
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
		checkNativeTypedStringIndexes(t, col, []nativeTypedOpsRow{a, b}, []string{"c"})
		if indexedCRUDAppliedLSN(t, handle) != 3 || handle.CommandWALNextLSN() != 4 {
			t.Fatal("indexed replay omitted/duplicated canonical identity")
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkNativeTypedStringIndexes(t, col, []nativeTypedOpsRow{a, b}, []string{"c"})
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("indexed replay final registry: %+v", stats)
		}
	}
}
