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

// Ordinary callbacks must produce the same typed rows, residuals and index
// changes as the maintained typed replacement API. This starts with an eligible
// NonColumn JSON collection; Full is a separate eligibility dependency.
func TestNativeColumnStoreUpdateBatchPublicLifecycle(t *testing.T) {
	const childEnv = "GOMAP_NATIVE_COLUMN_UPDATE_CRASH_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		// Preserve the real unapplied basis through the abrupt child exit.
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
		runNativeColumnUpdateBatch(t, dir, true)
		return
	}
	if !t.Run("lifecycle", func(t *testing.T) {
		runNativeColumnUpdateBatch(t, t.TempDir(), false)
	}) {
		return
	}
	t.Run("unapplied-replay", func(t *testing.T) {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeColumnStoreUpdateBatchPublicLifecycle$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"="+dir)
		output, err := cmd.CombinedOutput()
		t.Logf("ordinary typed update child:\n%s", output)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("COLUMN UPDATE UNAPPLIED basis=2 accepted=3")) || bytes.Contains(output, []byte("DATA RACE")) {
			t.Fatalf("real unapplied update child: %v\n%s", err, output)
		}
		a, b := nativeColumnUpdateFinalRows()
		for reopen := 0; reopen < 2; reopen++ {
			before := workstats.Read().Replay
			database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
			if err != nil {
				t.Fatal(err)
			}
			after := workstats.Read().Replay
			if after.FramesApplied-before.FramesApplied != uint64(1-reopen) || after.FrameErrors != before.FrameErrors {
				_ = database.Close()
				t.Fatalf("ordinary typed update replay=%d: %+v -> %+v", reopen, before, after)
			}
			checkNativeColumnUpdateReopened(t, database, a, b)
		}
	})
}

func nativeColumnUpdateFinalRows() (nativeTypedOpsRow, nativeTypedOpsRow) {
	a, b, _ := nativeTypedOpsRows("")
	a, b = nativeTypedOpsChanged(a), nativeTypedOpsChanged(b)
	a.text, b.text = "beta", "alpha" // Final whole-batch unique-owner handoff.
	a.payload, b.payload = "updated-a", "updated-b"
	return a, b
}

func checkNativeColumnUpdateRows(t *testing.T, col *collections.Collection, rows ...nativeTypedOpsRow) {
	t.Helper()
	for _, row := range rows {
		for pass := 0; pass < 2; pass++ {
			got, err := col.Get([]byte(row.id))
			if err != nil {
				t.Fatal(err)
			}
			checkNativeTypedMultiColumnDocument(t, got, row)
			got[0] = '!' // Public output cannot alias the retained typed cut.
		}
		for _, index := range []string{"label_lookup", "label_unique"} {
			ids, err := col.FindByIndex(index, row.text)
			if err != nil || len(ids) != 1 || string(ids[0]) != row.id {
				t.Fatalf("%s/%q owner=%q want=%s: %v", index, row.text, ids, row.id, err)
			}
		}
	}
	if got, err := col.Get([]byte("missing")); err != nil || got != nil {
		t.Fatalf("missing update became a row: %s %v", got, err)
	}
}

func runNativeColumnUpdateBatch(t *testing.T, dir string, abrupt bool) {
	t.Helper()
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
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
			Enabled: true,
			Columns: []collections.ColumnStoreColumn{
				{Name: "label", Path: "label", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset},
				{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 3, Owner: collections.TypedStorageOwnerColumnPart},
			},
			RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		}},
		Indexes: []collections.IndexDefinition{
			{Name: "label_lookup", Field: "label", ValueType: collections.IndexValueString},
			{Name: "label_unique", Field: "label", ValueType: collections.IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("eligible ordinary native column Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, _ := nativeTypedOpsRows("")
	if ids, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{nativeTypedMultiColumnDocument(t, a), nativeTypedMultiColumnDocument(t, b)}); err != nil || len(ids) != 2 {
		t.Fatalf("ordinary native typed seed insert: ids=%q %v", ids, err)
	}
	checkNativeColumnUpdateRows(t, col, a, b)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis, ok := handle.StateToken()
	if !ok || basis.AppliedCommandLSN != 2 || handle.CommandWALNextLSN() != 3 {
		t.Fatalf("actual seed basis=%+v valid=%v next=%d", basis, ok, handle.CommandWALNextLSN())
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("missing")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 3 || !got.Results[0].Found || !got.Results[1].Found || got.Results[2].Found {
			t.Fatalf("held original column cut: %+v %v", got.Results, err)
		}
		for i, row := range []nativeTypedOpsRow{a, b} {
			checkNativeTypedMultiColumnDocument(t, got.Results[i].Document, row)
			got.Results[i].Document[0] = '!'
		}
	}
	checkHeld()
	afterA, afterB := nativeColumnUpdateFinalRows()
	replacements := [][]byte{nativeTypedMultiColumnDocument(t, afterA), nativeTypedMultiColumnDocument(t, afterB)}
	shared := make([]byte, 0, max(len(replacements[0]), len(replacements[1])))
	called := [3]int{}
	items := make([]collections.UpdateBatchItem, 0, 3)
	for i, row := range []nativeTypedOpsRow{a, b} {
		items = append(items, collections.UpdateBatchItem{DocumentID: []byte(row.id), Update: func(current []byte) ([]byte, bool, error) {
			called[i]++
			checkNativeTypedMultiColumnDocument(t, current, row)
			current[0] = '!'
			shared = append(shared[:0], replacements[i]...)
			return shared, true, nil
		}})
	}
	items = append(items, collections.UpdateBatchItem{DocumentID: []byte("missing"), Update: func([]byte) ([]byte, bool, error) {
		called[2]++
		return nil, false, errors.New("missing callback must not run")
	}})
	next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	results, err := col.UpdateBatch(items)
	if err != nil {
		t.Fatalf("ordinary native typed UpdateBatch after successful seed insert: %v", err)
	}
	if len(results) != 3 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified || results[2].Matched || results[2].Modified || called != [3]int{1, 1, 0} {
		t.Fatalf("callback/results contract: results=%+v calls=%v", results, called)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	clear(shared)
	checkNativeColumnUpdateRows(t, col, afterA, afterB)
	checkHeld()
	unchanged := func(label string, operation func() error, want error) {
		t.Helper()
		next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		before, valid := handle.StateToken()
		if !valid {
			t.Fatal("missing original physical state")
		}
		if err := operation(); !errors.Is(err, want) {
			t.Fatalf("%s: %v want=%v", label, err, want)
		}
		after, valid := handle.StateToken()
		if !valid || after != before || handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
			t.Fatalf("%s consumed physical/canonical/logical identity", label)
		}
		checkNativeColumnUpdateRows(t, col, afterA, afterB)
		checkHeld()
	}
	unchanged("no-op callback input isolation", func() error {
		calls := 0
		got, err := col.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("a"), Update: func(current []byte) ([]byte, bool, error) {
			calls++
			checkNativeTypedMultiColumnDocument(t, current, afterA)
			current[0] = '!'
			return current, false, nil
		}}})
		if err == nil && (calls != 1 || len(got) != 1 || !got[0].Matched || got[0].Modified) {
			t.Fatalf("no-op result=%+v calls=%d", got, calls)
		}
		return err
	}, nil)
	sentinel := errors.New("later callback refused whole batch")
	unchanged("later item error is atomic", func() error {
		calls := [2]int{}
		_, err := col.UpdateBatch([]collections.UpdateBatchItem{
			{DocumentID: []byte("a"), Update: func(current []byte) ([]byte, bool, error) {
				calls[0]++
				checkNativeTypedMultiColumnDocument(t, current, afterA)
				refused := afterA
				refused.payload = "must not publish"
				return nativeTypedMultiColumnDocument(t, refused), true, nil
			}},
			{DocumentID: []byte("b"), Update: func(current []byte) ([]byte, bool, error) {
				calls[1]++
				checkNativeTypedMultiColumnDocument(t, current, afterB)
				return nil, false, sentinel
			}},
		})
		if calls != [2]int{1, 1} {
			t.Fatalf("atomic callback calls=%v", calls)
		}
		return err
	}, sentinel)
	unchanged("unique owner conflict is atomic", func() error {
		_, err := col.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) {
			conflict := afterA
			conflict.text = afterB.text
			return nativeTypedMultiColumnDocument(t, conflict), true, nil
		}}})
		return err
	}, collections.ErrUniqueIndexConflict)
	if err := handle.FlushCommandWAL(true); err != nil {
		t.Fatal(err)
	}
	physical, valid := handle.StateToken()
	if !valid || physical != basis || indexedCRUDAppliedLSN(t, handle) != 2 || handle.CommandWALNextLSN() != 4 {
		t.Fatalf("actual unapplied update basis=%+v physical=%+v next=%d", basis, physical, handle.CommandWALNextLSN())
	}
	if abrupt {
		t.Log("COLUMN UPDATE UNAPPLIED basis=2 accepted=3")
		os.Exit(97)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeColumnUpdateRows(t, col, afterA, afterB)
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
		t.Fatalf("ordinary update final registry=%+v", stats)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	checkNativeColumnUpdateReopened(t, reopened, afterA, afterB)
}

func checkNativeColumnUpdateReopened(t *testing.T, database *DB, rows ...nativeTypedOpsRow) {
	t.Helper()
	t.Cleanup(func() { _ = database.Close() })
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() { _ = manager.Close() })
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeColumnUpdateRows(t, col, rows...)
	if indexedCRUDAppliedLSN(t, handle) != 3 || handle.CommandWALNextLSN() != 4 {
		t.Fatal("ordinary update reopen omitted or duplicated canonical identity")
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeColumnUpdateRows(t, col, rows...)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("ordinary update reopened registry=%+v", stats)
	}
}
