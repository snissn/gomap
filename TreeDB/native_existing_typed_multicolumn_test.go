package treedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestNativeExistingTypedMultiColumnLifecycle(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_MULTICOLUMN_CRASH_DIR"); dir != "" {
		// Keep the exact unapplied basis; periodic value-log GC may checkpoint.
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
		runNativeTypedMultiColumn(t, dir, true)
		return
	}
	t.Run("lifecycle", func(t *testing.T) { runNativeTypedMultiColumn(t, t.TempDir(), false) })
	t.Run("unapplied-typed-replay", func(t *testing.T) {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingTypedMultiColumnLifecycle$", "-test.v")
		cmd.Env = append(os.Environ(), "GOMAP_TYPED_MULTICOLUMN_CRASH_DIR="+dir)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("TYPED MULTICOLUMN UNAPPLIED basis=4 accepted=6")) || bytes.Contains(output, []byte("DATA RACE")) {
			t.Fatalf("real multicolumn typed child: %v\n%s", err, output)
		}
		t.Logf("child:\n%s", output)
		before := workstats.Read().Replay
		database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
		if err != nil {
			t.Fatal(err)
		}
		after := workstats.Read().Replay
		if after.FramesApplied-before.FramesApplied != 2 || after.FrameErrors != before.FrameErrors {
			_ = database.Close()
			t.Fatalf("one actual multicolumn typed replay: %+v -> %+v", before, after)
		}
		a, b, c := nativeTypedOpsRows("")
		checkNativeTypedMultiColumnReopened(t, database, []nativeTypedOpsRow{nativeTypedOpsChanged(a), b, c}, false, 6)
	})
}

func nativeTypedMultiColumnInput(rows ...nativeTypedOpsRow) ([][]byte, [][]byte, []collections.TypedColumnBatch) {
	ids, residual, text := nativeTypedOpsInput("string", rows...)
	_, _, vector := nativeTypedOpsInput("float32_vector", rows...)
	return ids, residual, append(text, vector...)
}

func nativeTypedMultiColumnDocument(t *testing.T, row nativeTypedOpsRow) []byte {
	t.Helper()
	value, err := json.Marshal(map[string]any{"id": row.id, "payload": row.payload, "label": row.text, "embedding": row.vector})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func checkNativeTypedMultiColumnDocument(t *testing.T, got []byte, row nativeTypedOpsRow) {
	t.Helper()
	requireNativeColumnJSON(t, got, nativeTypedMultiColumnDocument(t, row))
	var document struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(got, &document); err != nil || len(document.Embedding) != len(row.vector) {
		t.Fatalf("row %s vector reconstruction: %v", row.id, err)
	}
	for i, value := range row.vector {
		if math.Float32bits(document.Embedding[i]) != math.Float32bits(value) {
			t.Fatalf("row %s vector[%d] bits=%08x want=%08x", row.id, i, math.Float32bits(document.Embedding[i]), math.Float32bits(value))
		}
	}
}

func pauseNativeTypedMultiColumnMaintenance(t *testing.T) func() {
	t.Helper()
	const key = "TREEDB_VLOG_MAINTENANCE_PAUSE_FILE"
	previous, wasSet := os.LookupEnv(key)
	pauseFile := filepath.Join(t.TempDir(), "typed-multicolumn-maintenance.pause")
	if err := os.WriteFile(pauseFile, []byte("pause"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(key, pauseFile)
	released := false
	resume := func() {
		t.Helper()
		if released {
			return
		}
		released = true
		if err := os.Remove(pauseFile); err != nil {
			t.Fatal(err)
		}
		var err error
		if wasSet {
			err = os.Setenv(key, previous)
		} else {
			err = os.Unsetenv(key)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(resume)
	return resume
}

func runNativeTypedMultiColumn(t *testing.T, dir string, crash bool) {
	t.Helper()
	// Maintenance may checkpoint before GC. Fence its admission before Open
	// until the exact unapplied physical token and command frame are checked.
	resumeMaintenance := pauseNativeTypedMultiColumnMaintenance(t)
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
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "events", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{
			{Name: "label", Path: "label", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset},
			{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 3, Owner: collections.TypedStorageOwnerColumnPart},
		},
		RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
	}}}); err != nil {
		t.Fatalf("ordinary public multicolumn typed Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := nativeTypedOpsRows("")
	ids, residual, columns := nativeTypedMultiColumnInput(a, b)
	next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	inserted, stats, err := col.InsertTypedBatchWithStats(ids, residual, columns)
	if err != nil || len(inserted) != 2 || !bytes.Equal(inserted[0], []byte("a")) || !bytes.Equal(inserted[1], []byte("b")) || stats.Documents != 2 {
		t.Fatalf("actual two-column typed seed ids=%q documents=%d: %v", inserted, stats.Documents, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis, ok := handle.StateToken()
	if !ok || basis.AppliedCommandLSN != 4 || indexedCRUDAppliedLSN(t, handle) != 4 || handle.CommandWALNextLSN() != 5 {
		t.Fatalf("real seeded multicolumn basis=%+v valid=%v next=%d", basis, ok, handle.CommandWALNextLSN())
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		result, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(result.Results) != 3 || !result.Results[0].Found || !result.Results[1].Found || result.Results[2].Found {
			t.Fatalf("held multicolumn rows=%+v: %v", result.Results, err)
		}
		for i, row := range []nativeTypedOpsRow{a, b} {
			checkNativeTypedMultiColumnDocument(t, result.Results[i].Document, row)
			result.Results[i].Document[0] = '!'
		}
	}
	checkCurrent := func(rows ...nativeTypedOpsRow) {
		t.Helper()
		for _, row := range rows {
			for pass := 0; pass < 2; pass++ {
				got, err := col.Get([]byte(row.id))
				if err != nil {
					t.Fatal(err)
				}
				checkNativeTypedMultiColumnDocument(t, got, row)
				got[0] = '!'
			}
		}
	}
	checkHeld()
	changedA := nativeTypedOpsChanged(a)
	ids, residual, columns = nativeTypedMultiColumnInput(changedA, c)
	next, sequence = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	matched, err := col.UpsertTypedBatch(ids, residual, columns)
	if err != nil || matched != 1 {
		t.Fatalf("actual multicolumn mixed upsert matched=%d: %v", matched, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
	// All original carriers may be reused, independently for both columns.
	ids[0][0], residual[0][0] = '!', '!'
	columns[0].Strings[0], columns[1].Float32Vectors[0][0] = "caller mutated", 999
	checkCurrent(changedA, b, c)
	checkHeld()
	if err := handle.FlushCommandWAL(true); err != nil {
		t.Fatal(err)
	}
	after, valid := handle.StateToken()
	if !valid || after != basis || indexedCRUDAppliedLSN(t, handle) != 4 || handle.CommandWALNextLSN() != 7 {
		t.Fatalf("typed suffix applied before checkpoint: basis=%+v after=%+v valid=%v", basis, after, valid)
	}
	checkNativeTypedMultiColumnFrame(t, opts, []nativeTypedOpsRow{changedA, c})
	if crash {
		t.Log("TYPED MULTICOLUMN UNAPPLIED basis=4 accepted=6")
		os.Exit(97)
	}
	resumeMaintenance()
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("events")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	next, sequence = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	if deleted, err := tx.DeleteBatch([][]byte{[]byte("c")}); err != nil || deleted != 1 {
		t.Fatalf("private two-column delete=%d: %v", deleted, err)
	}
	if got, err := tx.Get([]byte("c")); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
		t.Fatalf("private deleted row=%s: %v", got, err)
	}
	finalC := nativeTypedOpsChanged(c)
	id, document := []byte("c"), nativeTypedMultiColumnDocument(t, finalC)
	if err := tx.InsertBatch([][]byte{id}, [][]byte{document}); err != nil {
		t.Fatal(err)
	}
	id[0], document[0] = '!', '!'
	for pass := 0; pass < 2; pass++ {
		got, err := tx.Get([]byte("c"))
		if err != nil {
			t.Fatal(err)
		}
		checkNativeTypedMultiColumnDocument(t, got, finalC)
		got[0] = '!'
	}
	if err := tx.SetRaw([]byte("multicolumn/raw"), []byte("complete")); err != nil {
		t.Fatal(err)
	}
	checkCurrent(changedA, b, c)
	if got, err := database.Get([]byte("multicolumn/raw")); err != nil || got != nil {
		t.Fatalf("uncommitted raw=%s: %v", got, err)
	}
	if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
		t.Fatal("private multicolumn mutations consumed canonical authority")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
	checkCurrent(changedA, b, finalC)
	checkHeld()
	if got, err := database.Get([]byte("multicolumn/raw")); err != nil || string(got) != "complete" {
		t.Fatalf("committed multicolumn raw=%s: %v", got, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != 8 {
		t.Fatal("checkpoint omitted a multicolumn command")
	}
	checkCurrent(changedA, b, finalC)
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
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("multicolumn final registry=%+v", s)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	checkNativeTypedMultiColumnReopened(t, reopened, []nativeTypedOpsRow{changedA, b, finalC}, true, 8)
}

func checkNativeTypedMultiColumnReopened(t *testing.T, database *DB, rows []nativeTypedOpsRow, raw bool, applied uint64) {
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
	for _, row := range rows {
		got, err := col.Get([]byte(row.id))
		if err != nil {
			t.Fatal(err)
		}
		checkNativeTypedMultiColumnDocument(t, got, row)
	}
	value, err := database.Get([]byte("multicolumn/raw"))
	if err != nil || (raw && string(value) != "complete") || (!raw && value != nil) {
		t.Fatalf("reopened raw=%s expected=%v: %v", value, raw, err)
	}
	if indexedCRUDAppliedLSN(t, handle) != applied || handle.CommandWALNextLSN() != applied+1 {
		t.Fatalf("reopened multicolumn applied=%d next=%d want=%d", indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN(), applied)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopened multicolumn registry=%+v", s)
	}
}

func checkNativeTypedMultiColumnFrame(t *testing.T, opts Options, rows []nativeTypedOpsRow) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var frames []commitlog.CommandEnvelope
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
			if frame.LSN >= 5 && frame.Kind != commitlog.CommandKindDurablePrefixBarrier {
				frames = append(frames, frame)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(frames) != 1 || frames[0].LSN != 5 || frames[0].Kind != commitlog.CommandKindCompositeMutation || frames[0].PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
		t.Fatalf("one multicolumn typed aggregate at LSN5: %+v", frames)
	}
	var members []commitlog.CompositeMutationMember
	if err := commitlog.ScanCompositeMutationPayload(frames[0].Payload, func(member commitlog.CompositeMutationMember) error {
		members = append(members, member)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Incarnation == 0 || members[0].Kind != commitlog.CommandKindCollectionReplaceSourceByID || members[0].PayloadFormat != commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
		t.Fatalf("original typed source member=%+v", members)
	}
	source, err := commitlog.DecodeCollectionTypedSourcePayload(members[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	payload := source.Inserted
	if payload.Collection != "events" || payload.SchemaHash == 0 || len(payload.Columns) != 2 || len(payload.Documents) != len(rows) || len(source.DeleteIDs) != len(rows) {
		t.Fatalf("two authoritative columns in typed source: collection=%q columns=%d rows=%d deletes=%q", payload.Collection, len(payload.Columns), len(payload.Documents), source.DeleteIDs)
	}
	for rowIndex, row := range rows {
		document := payload.Documents[rowIndex]
		if string(document.ID) != row.id || string(source.DeleteIDs[rowIndex]) != row.id || !bytes.Equal(document.Retained, bytes.TrimSpace(row.retained)) || len(document.Values) != 2 {
			t.Fatalf("typed source row=%q residual=%q deletes=%q", document.ID, document.Retained, source.DeleteIDs)
		}
		seenText, seenVector := false, false
		for i, column := range payload.Columns {
			switch column.Name {
			case "label":
				seenText = true
				if column.Type != commitlog.CollectionTypedString || column.Dimensions != 0 || document.Values[i].String != row.text {
					t.Fatalf("typed string column differs for %s", row.id)
				}
			case "embedding":
				seenVector = true
				if column.Type != commitlog.CollectionTypedFloat32Vector || column.Dimensions != 3 || len(document.Values[i].Vector) != len(row.vector) {
					t.Fatalf("typed vector column differs for %s", row.id)
				}
				for j, value := range row.vector {
					if math.Float32bits(document.Values[i].Vector[j]) != math.Float32bits(value) {
						t.Fatalf("typed vector payload differs for %s[%d]", row.id, j)
					}
				}
			default:
				t.Fatalf("unexpected canonical column %q", column.Name)
			}
		}
		if !seenText || !seenVector {
			t.Fatal("canonical typed member omitted a declared column")
		}
	}
}
