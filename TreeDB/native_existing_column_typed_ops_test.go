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
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// Exercise the existing typed APIs, including their different no-op contracts,
// through ordinary activated collections. The child cases leave real typed
// payloads unapplied; neither a copied WAL nor a JSON insert stands in for them.
func TestNativeExistingTypedOperations(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_OPS_CRASH_DIR"); dir != "" {
		runNativeTypedOperations(t, dir, os.Getenv("GOMAP_TYPED_OPS_FAMILY"), os.Getenv("GOMAP_TYPED_OPS_MODE"))
		return
	}
	for _, family := range []string{"string", "float32_vector"} {
		t.Run(family, func(t *testing.T) {
			t.Run("lifecycle", func(t *testing.T) { runNativeTypedOperations(t, t.TempDir(), family, "lifecycle") })
			for _, mode := range []string{"typed-insert", "typed-source"} {
				t.Run("unapplied-"+mode, func(t *testing.T) {
					dir := t.TempDir()
					cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingTypedOperations$", "-test.v")
					cmd.Env = append(os.Environ(), "GOMAP_TYPED_OPS_CRASH_DIR="+dir, "GOMAP_TYPED_OPS_FAMILY="+family, "GOMAP_TYPED_OPS_MODE="+mode)
					output, err := cmd.CombinedOutput()
					var exit *exec.ExitError
					marker := []byte("TYPED OPS UNAPPLIED " + mode)
					if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, marker) || bytes.Contains(output, []byte("DATA RACE")) {
						t.Fatalf("typed child: %v\n%s", err, output)
					}
					t.Logf("child:\n%s", output)
					before := workstats.Read().Replay
					database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
					if err != nil {
						t.Fatal(err)
					}
					after := workstats.Read().Replay
					if after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
						_ = database.Close()
						t.Fatalf("one actual typed replay: %+v -> %+v", before, after)
					}
					a, b, c := nativeTypedOpsRows(family)
					want, applied := []nativeTypedOpsRow{a, b}, uint64(2)
					if mode == "typed-source" {
						b = nativeTypedOpsChanged(b)
						want, applied = []nativeTypedOpsRow{a, b, c}, 3
					}
					checkNativeTypedOpsReopened(t, database, family, want, nil, applied)
				})
			}
		})
	}
}

type nativeTypedOpsRow struct {
	id, payload, text string
	vector            []float32
	retained          []byte
}

func nativeTypedOpsRows(family string) (nativeTypedOpsRow, nativeTypedOpsRow, nativeTypedOpsRow) {
	rows := []nativeTypedOpsRow{
		{id: "a", payload: "seed-a", text: "alpha", vector: []float32{1, 0, 0.5}},
		{id: "b", payload: "seed-b", text: "beta", vector: []float32{-2, 3.25, 4}},
		{id: "c", payload: "new-c", text: "quote\" slash\\ tab\t \u263a", vector: []float32{8.125, -1.5, 2}},
	}
	for i := range rows {
		rows[i].retained = []byte(`{"id":"` + rows[i].id + `", "payload":"` + rows[i].payload + `"}`)
	}
	return rows[0], rows[1], rows[2]
}

func nativeTypedOpsChanged(row nativeTypedOpsRow) nativeTypedOpsRow {
	row.text += " changed\"\\\t"
	row.vector = append([]float32(nil), row.vector...)
	row.vector[1] = math.Float32frombits(0x80000000)
	return row
}

func nativeTypedOpsInput(family string, rows ...nativeTypedOpsRow) ([][]byte, [][]byte, []collections.TypedColumnBatch) {
	ids, retained := make([][]byte, len(rows)), make([][]byte, len(rows))
	column := collections.TypedColumnBatch{Name: "label"}
	if family == "float32_vector" {
		column.Name = "embedding"
	}
	for i, row := range rows {
		ids[i], retained[i] = []byte(row.id), bytes.Clone(row.retained)
		if family == "string" {
			column.Strings = append(column.Strings, row.text)
		} else {
			column.Float32Vectors = append(column.Float32Vectors, append([]float32(nil), row.vector...))
		}
	}
	return ids, retained, []collections.TypedColumnBatch{column}
}

func nativeTypedOpsDocument(t *testing.T, family string, row nativeTypedOpsRow) []byte {
	t.Helper()
	// The oracle comes from the original test values, never from a reconstructed
	// document, a typed command payload, or the input residual parser.
	doc := map[string]any{"id": row.id, "payload": row.payload}
	if family == "string" {
		doc["label"] = row.text
	} else {
		doc["embedding"] = row.vector
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func checkNativeTypedOpsDocument(t *testing.T, family string, got []byte, row nativeTypedOpsRow) {
	t.Helper()
	requireNativeColumnJSON(t, got, nativeTypedOpsDocument(t, family, row))
	if family == "float32_vector" {
		var doc struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(got, &doc); err != nil || len(doc.Embedding) != len(row.vector) {
			t.Fatalf("vector readback: %v", err)
		}
		for i, value := range row.vector {
			if math.Float32bits(doc.Embedding[i]) != math.Float32bits(value) {
				t.Fatalf("row %s vector[%d] bits=%08x want=%08x", row.id, i, math.Float32bits(doc.Embedding[i]), math.Float32bits(value))
			}
		}
	}
}

func checkNativeTypedOpsCurrent(t *testing.T, col *collections.Collection, family string, rows []nativeTypedOpsRow, absent []string) {
	t.Helper()
	for _, row := range rows {
		for attempt := 0; attempt < 2; attempt++ {
			got, err := col.Get([]byte(row.id))
			if err != nil {
				t.Fatal(err)
			}
			checkNativeTypedOpsDocument(t, family, got, row)
			got[0] = '!' // Caller ownership must survive the next read.
		}
	}
	for _, id := range absent {
		if got, err := col.Get([]byte(id)); err != nil || got != nil {
			t.Fatalf("absent %s=%q: %v", id, got, err)
		}
	}
}

func requireNativeTypedOpsAdvance(t *testing.T, database *DB, handle *backenddb.DB, next uint64, sequence string) {
	t.Helper()
	n, err := strconv.ParseUint(sequence, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if handle.CommandWALNextLSN() != next+1 || database.Stats()["treedb.cache.cow.txn.logical_seq"] != strconv.FormatUint(n+1, 10) {
		t.Fatalf("one canonical publication: next=%d want=%d sequence=%s prior=%s", handle.CommandWALNextLSN(), next+1, database.Stats()["treedb.cache.cow.txn.logical_seq"], sequence)
	}
}

func runNativeTypedOperations(t *testing.T, dir, family, mode string) {
	t.Helper()
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	fixture := nativeColumnPendingCaseFor(t, family)
	fixture.typedSeed = &collections.TypedColumnBatch{} // Select the maintained retained-JSON contract.
	if family == "string" {
		fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	}
	if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
		t.Fatalf("ordinary typed Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := nativeTypedOpsRows(family)
	if mode == "typed-insert" {
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	// The empty held cut in the insert child must stay empty after acceptance.
	var held *collections.CollectionReadView
	t.Cleanup(func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if mode == "typed-insert" {
		held, err = col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
	}
	ids, residual, columns := nativeTypedOpsInput(family, a, b)
	next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	inserted, stats, err := col.InsertTypedBatchWithStats(ids, residual, columns)
	if err != nil || len(inserted) != 2 || !bytes.Equal(inserted[0], []byte("a")) || !bytes.Equal(inserted[1], []byte("b")) || stats.Documents != 2 {
		t.Fatalf("actual typed insert ids=%q documents=%d: %v", inserted, stats.Documents, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkNativeTypedOpsFrame(t, opts, 2, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionTypedBatchByIDV1, family, []nativeTypedOpsRow{a, b}, nil)
	// The service promises inputs may be reused immediately after return.
	ids[0][0], residual[0][0] = '!', '!'
	if family == "string" {
		columns[0].Strings[0] = "mutated caller"
	} else {
		columns[0].Float32Vectors[0][0] = 999
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{a, b}, []string{"c"})
	// Invalid typed inputs and duplicates cannot consume canonical authority.
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	duplicateIDs, duplicateResidual, duplicateColumns := nativeTypedOpsInput(family, a, a)
	if _, err := col.UpsertTypedBatch(duplicateIDs, duplicateResidual, duplicateColumns); err == nil {
		t.Fatal("duplicate typed upsert accepted")
	}
	conflictIDs, conflictResidual, conflictColumns := nativeTypedOpsInput(family, a)
	if _, err := col.ReplaceTypedSourceByID(nil, conflictIDs, conflictResidual, conflictColumns); !errors.Is(err, collections.ErrDocumentExists) {
		t.Fatalf("source insert outside explicit replacement set: %v", err)
	}
	invalid := a
	invalid.id, invalid.retained = "invalid", []byte(`{"id":"invalid", "payload":"seed-a"}`)
	invalid.text, invalid.vector = string([]byte{0xff}), []float32{1, 2}
	invalidIDs, invalidResidual, invalidColumns := nativeTypedOpsInput(family, invalid)
	if _, _, err := col.InsertTypedBatchWithStats(invalidIDs, invalidResidual, invalidColumns); err == nil {
		t.Fatal("invalid typed carrier accepted")
	}
	if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
		t.Fatal("invalid typed input consumed canonical authority")
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{a, b}, []string{"invalid", "c"})
	if mode != "typed-insert" {
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		held, err = col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
	}
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 3 {
			t.Fatalf("held typed view: %v", err)
		}
		for i, row := range []nativeTypedOpsRow{a, b, c} {
			want := mode != "typed-insert" && i < 2
			if got.Results[i].Found != want {
				t.Fatalf("held row %s found=%v want=%v", row.id, got.Results[i].Found, want)
			}
			if want {
				checkNativeTypedOpsDocument(t, family, got.Results[i].Document, row)
				got.Results[i].Document[0] = '!'
			}
		}
	}
	checkHeld()
	if mode == "typed-insert" {
		if indexedCRUDAppliedLSN(t, handle) != 1 {
			t.Fatal("insert child did not keep basis1")
		}
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if indexedCRUDAppliedLSN(t, handle) != 1 {
			t.Fatal("WAL flush applied typed insert")
		}
		t.Log("TYPED OPS UNAPPLIED typed-insert basis=1 accepted=2")
		os.Exit(97)
	}
	changedA, changedB := nativeTypedOpsChanged(a), nativeTypedOpsChanged(b)
	if mode == "lifecycle" {
		missing := nativeTypedOpsChanged(c)
		missing.id = "missing"
		missing.retained = []byte(`{"id":"missing", "payload":"new-c"}`)
		ids, residual, columns = nativeTypedOpsInput(family, changedA, missing)
		next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		results, err := col.ReplaceTypedBatch(ids, residual, columns)
		if err != nil || len(results) != 2 || !results[0].Matched || !results[0].Modified || results[1].Matched || results[1].Modified {
			t.Fatalf("replace missing ignored: %+v %v", results, err)
		}
		requireNativeTypedOpsAdvance(t, database, handle, next, seq)
		checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionTypedBatchByIDV1, family, []nativeTypedOpsRow{changedA}, nil)
		checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{changedA, b}, []string{"missing"})
		// Surrounding whitespace is ignored; bitwise-equal values are no-op.
		ids, residual, columns = nativeTypedOpsInput(family, changedA)
		residual[0] = append(append([]byte(" \n"), residual[0]...), '\t')
		next = handle.CommandWALNextLSN()
		results, err = col.ReplaceTypedBatch(ids, residual, columns)
		if err != nil || len(results) != 1 || !results[0].Matched || results[0].Modified || handle.CommandWALNextLSN() != next+1 {
			t.Fatalf("noop replace: %+v %v", results, err)
		}
		checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, family, nil, nil)
		// Interior bytes are significant despite identical logical JSON.
		changedA.retained = []byte(`{"id":"a",  "payload":"seed-a"}`)
		ids, residual, columns = nativeTypedOpsInput(family, changedA)
		next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		results, err = col.ReplaceTypedBatch(ids, residual, columns)
		if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
			t.Fatalf("interior-byte replace: %+v %v", results, err)
		}
		requireNativeTypedOpsAdvance(t, database, handle, next, seq)
		checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionTypedBatchByIDV1, family, []nativeTypedOpsRow{changedA}, nil)
	}
	currentA := a
	if mode == "lifecycle" {
		currentA = changedA
	}
	ids, residual, columns = nativeTypedOpsInput(family, currentA, changedB, c)
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	matched, err := col.UpsertTypedBatch(ids, residual, columns)
	if err != nil || matched != 2 {
		t.Fatalf("mixed upsert counts unchanged existing: %d %v", matched, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionReplaceSourceByID, commitlog.PayloadFormatCollectionTypedSourceByIDV1, family, []nativeTypedOpsRow{changedB, c}, []string{"b", "c"})
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{currentA, changedB, c}, nil)
	checkHeld()
	if mode == "typed-source" {
		if indexedCRUDAppliedLSN(t, handle) != 2 {
			t.Fatal("source child did not keep basis2")
		}
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if indexedCRUDAppliedLSN(t, handle) != 2 {
			t.Fatal("WAL flush applied typed source")
		}
		t.Log("TYPED OPS UNAPPLIED typed-source basis=2 accepted=3")
		os.Exit(97)
	}
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	matched, err = col.UpsertTypedBatch(ids, residual, columns)
	if err != nil || matched != 3 || handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
		t.Fatalf("noop upsert changed authority: matched=%d %v", matched, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkHeld()
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{currentA, changedB, c}, nil)
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	matched, err = col.UpsertTypedBatch(ids, residual, columns)
	if err != nil || matched != 3 || handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
		t.Fatalf("checkpointed noop upsert changed authority: matched=%d %v", matched, err)
	}
	// Explicit source replacement demonstrates insert-wins-delete separately
	// from the upsert optimization, which omits unchanged rows from its frame.
	finalC := nativeTypedOpsChanged(c)
	ids, residual, columns = nativeTypedOpsInput(family, finalC)
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	deleted, err := col.ReplaceTypedSourceByID([][]byte{[]byte("b"), []byte("c")}, ids, residual, columns)
	if err != nil || deleted != 2 {
		t.Fatalf("source insert wins delete: %d %v", deleted, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionReplaceSourceByID, commitlog.PayloadFormatCollectionTypedSourceByIDV1, family, []nativeTypedOpsRow{finalC}, []string{"b", "c"})
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{currentA, finalC}, []string{"b"})
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	deleted, err = col.DeleteBatch([][]byte{[]byte("a"), []byte("b"), []byte("missing")})
	if err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch existing count: %d %v", deleted, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, family, nil, []string{"a"})
	checkHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkHeld()
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{finalC}, []string{"a", "b", "missing"})
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	deleted, err = col.ReplaceTypedSourceByID([][]byte{[]byte("c")}, nil, nil, nil)
	if err != nil || deleted != 1 {
		t.Fatalf("delete-only source replacement: %d %v", deleted, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkNativeTypedOpsFrame(t, opts, next, commitlog.CommandKindCollectionReplaceSourceByID, commitlog.PayloadFormatCollectionReplaceSourceByIDV1, family, nil, []string{"c"})
	checkNativeTypedOpsCurrent(t, col, family, nil, []string{"a", "b", "c", "missing"})
	checkHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkHeld()
	checkNativeTypedOpsCurrent(t, col, family, nil, []string{"a", "b", "c", "missing"})
	applied := handle.CommandWALNextLSN() - 1
	if indexedCRUDAppliedLSN(t, handle) != applied {
		t.Fatal("checkpoint omitted typed operations")
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
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("typed ops final registry %+v", s)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	checkNativeTypedOpsReopened(t, reopened, family, nil, []string{"a", "b", "c", "missing"}, applied)
}

func checkNativeTypedOpsReopened(t *testing.T, database *DB, family string, rows []nativeTypedOpsRow, absent []string, applied uint64) {
	t.Helper()
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeTypedOpsCurrent(t, col, family, rows, absent)
	if indexedCRUDAppliedLSN(t, handle) != applied || handle.CommandWALNextLSN() != applied+1 {
		t.Fatalf("reopen frontiers applied=%d next=%d want=%d", indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN(), applied)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopen final registry %+v", s)
	}
}

func checkNativeTypedOpsFrame(t *testing.T, opts Options, lsn uint64, kind commitlog.CommandKind, format commitlog.PayloadFormat, family string, rows []nativeTypedOpsRow, deleted []string) {
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
			if frame.LSN >= lsn {
				frames = append(frames, frame)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(frames) != 1 || frames[0].LSN != lsn {
		t.Fatalf("one exact source frame from LSN%d: %+v", lsn, frames)
	}
	frame := frames[0]
	// The native owner routes one original aggregate during startup too; the
	// original member retains the typed format and captured incarnation.
	if frame.Kind != commitlog.CommandKindCompositeMutation || frame.PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
		t.Fatalf("native typed canonical frame kind=%d format=%d", frame.Kind, frame.PayloadFormat)
	}
	var members []commitlog.CompositeMutationMember
	if err := commitlog.ScanCompositeMutationPayload(frame.Payload, func(member commitlog.CompositeMutationMember) error {
		members = append(members, member)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Incarnation == 0 || members[0].Kind != kind || members[0].PayloadFormat != format {
		t.Fatalf("one exact typed canonical member: %+v", members)
	}
	frame.Kind, frame.PayloadFormat, frame.Payload = members[0].Kind, members[0].PayloadFormat, members[0].Payload
	var typed commitlog.CollectionTypedBatchPayload
	var deleteIDs [][]byte
	switch format {
	case commitlog.PayloadFormatCollectionTypedBatchByIDV1:
		typed, err = commitlog.DecodeCollectionTypedBatchPayload(frame.Payload)
	case commitlog.PayloadFormatCollectionTypedSourceByIDV1:
		var source commitlog.CollectionTypedSourcePayload
		source, err = commitlog.DecodeCollectionTypedSourcePayload(frame.Payload)
		typed, deleteIDs = source.Inserted, source.DeleteIDs
	case commitlog.PayloadFormatCollectionUpdateBatchByIDV1:
		update, decodeErr := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
		if decodeErr != nil || len(update.Documents) != 0 {
			t.Fatalf("noop replace ordinary empty update: %+v %v", update, decodeErr)
		}
		return
	case commitlog.PayloadFormatCollectionDeleteBatchByIDV1:
		deletion, decodeErr := commitlog.DecodeCollectionDeleteBatchByIDPayload(frame.Payload)
		err, deleteIDs = decodeErr, deletion.IDs
	case commitlog.PayloadFormatCollectionReplaceSourceByIDV1:
		source, decodeErr := commitlog.DecodeCollectionReplaceSourceByIDPayload(frame.Payload)
		if decodeErr != nil || source.Collection != "events" || len(source.Documents) != 0 {
			t.Fatalf("delete-only ordinary source: %+v %v", source, decodeErr)
		}
		deleteIDs = source.DeleteIDs
	default:
		t.Fatalf("unexpected fixture format %d", format)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(deleteIDs) != len(deleted) {
		t.Fatalf("deleted IDs=%q want=%v", deleteIDs, deleted)
	}
	for i, id := range deleted {
		if string(deleteIDs[i]) != id {
			t.Fatalf("deleted IDs=%q want=%v", deleteIDs, deleted)
		}
	}
	if len(rows) == 0 {
		return
	}
	if typed.Collection != "events" || typed.SchemaHash == 0 || len(typed.Columns) != 1 || len(typed.Documents) != len(rows) {
		t.Fatalf("typed payload collection=%q columns=%d documents=%d", typed.Collection, len(typed.Columns), len(typed.Documents))
	}
	if family == "string" && (typed.Columns[0].Name != "label" || typed.Columns[0].Dimensions != 0) {
		t.Fatal("typed string schema differs")
	}
	if family == "float32_vector" && (typed.Columns[0].Name != "embedding" || typed.Columns[0].Dimensions != 3) {
		t.Fatal("typed vector schema differs")
	}
	for i, row := range rows {
		doc := typed.Documents[i]
		if string(doc.ID) != row.id || !bytes.Equal(doc.Retained, bytes.TrimSpace(row.retained)) || len(doc.Values) != 1 {
			t.Fatalf("typed payload row %d id=%q residual=%q", i, doc.ID, doc.Retained)
		}
		if family == "string" {
			if typed.Columns[0].Type != commitlog.CollectionTypedString || doc.Values[0].String != row.text {
				t.Fatalf("typed string payload differs for %s", row.id)
			}
		} else {
			if typed.Columns[0].Type != commitlog.CollectionTypedFloat32Vector || len(doc.Values[0].Vector) != len(row.vector) {
				t.Fatalf("typed vector payload differs for %s", row.id)
			}
			for j, value := range row.vector {
				if math.Float32bits(doc.Values[0].Vector[j]) != math.Float32bits(value) {
					t.Fatalf("typed vector payload bits differ for %s[%d]", row.id, j)
				}
			}
		}
	}
}
