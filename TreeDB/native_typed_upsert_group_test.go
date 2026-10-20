package treedb

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// The public Try method must preserve its existing per-request source frames
// and decline contract on an already eligible native schema. Whether two public
// callers reach the same cohort is scheduling-dependent; deterministic cohort
// and installed-before-ack controls remain in typed_upsert_group_test.go.
func TestNativeTypedUpsertGroupPublicLifecycle(t *testing.T) {
	dir := t.TempDir()
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
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
		t.Fatalf("eligible group collection: %v", err)
	}
	first, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.OpenCollection("events")
	if err != nil || first == second {
		t.Fatalf("two independent handles: same=%v err=%v", first == second, err)
	}
	a, b, c := nativeTypedOpsRows("")
	if _, _, err := first.InsertTypedBatchWithStats(nativeTypedMultiColumnInput(a)); err != nil {
		t.Fatalf("qualified typed seed: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held, err = first.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte(a.id), []byte(b.id), []byte(c.id)}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 3 || !got.Results[0].Found || got.Results[1].Found || got.Results[2].Found {
			t.Fatalf("original held cut: %+v err=%v", got.Results, err)
		}
		checkNativeTypedMultiColumnDocument(t, got.Results[0].Document, a)
	}
	checkHeld()
	beforeState, valid := handle.StateToken()
	if !valid {
		t.Fatal("missing original physical basis")
	}
	beforeNext, beforeStats := handle.CommandWALNextLSN(), database.Stats()
	t.Logf("eligible create/typed seed/checkpoint/held cut/two handles ready: physical=%+v next=%d", beforeState, beforeNext)
	type result struct {
		updated int
		handled bool
		stats   collections.CollectionInsertStats
		err     error
	}
	start := make(chan struct{})
	done := make(chan result, 2)
	for i, col := range []*collections.Collection{first, second} {
		row := []nativeTypedOpsRow{b, c}[i]
		ids, retained, columns := nativeTypedMultiColumnInput(row)
		go func() {
			<-start
			updated, handled, stats, err := col.TryUpsertTypedBatchGroup(ids, retained, columns)
			// Completion ends input borrowing for each actual public request.
			clear(ids[0])
			clear(retained[0])
			clear(columns[0].Strings)
			clear(columns[1].Float32Vectors[0])
			done <- result{updated, handled, stats, err}
		}()
	}
	close(start)
	results := []result{<-done, <-done}
	if results[0].err != nil || results[1].err != nil {
		// The current native c.db=nil refusal must not acquire an identity,
		// publish either member, or change the selected physical basis.
		afterState, ok := handle.StateToken()
		if !ok || afterState != beforeState || handle.CommandWALNextLSN() != beforeNext || database.Stats()["treedb.cache.cow.txn.logical_seq"] != beforeStats["treedb.cache.cow.txn.logical_seq"] {
			t.Fatalf("group refusal changed original authority: state=%+v next=%d", afterState, handle.CommandWALNextLSN())
		}
		for _, id := range []string{b.id, c.id} {
			if got, err := first.Get([]byte(id)); err != nil || got != nil {
				t.Fatalf("refused request %s visible=%q err=%v", id, got, err)
			}
		}
		checkHeld()
		t.Fatalf("native public typed grouping after successful create/seed/checkpoint: results=%+v; no acceptance side effects", results)
	}
	for _, got := range results {
		if !got.handled || got.updated != 0 || got.stats.Documents != 1 {
			t.Fatalf("fresh disjoint participant result=%+v", got)
		}
	}
	for _, row := range []nativeTypedOpsRow{a, b, c} {
		got, err := first.Get([]byte(row.id))
		if err != nil {
			t.Fatal(err)
		}
		checkNativeTypedMultiColumnDocument(t, got, row)
		got[0] = '!'
	}
	checkHeld()
	barriers := checkNativeTypedGroupFrames(t, opts, beforeNext, b, c)
	// A shared cohort has two participant frames, one barrier and one install;
	// independent cohorts have two participant frames and two installs. Do not
	// turn a scheduler observation into an unconditional shared-cohort promise.
	publications := uint64(2 - barriers)
	requirePublicStatDelta(t, beforeStats, database.Stats(), "treedb.command_wal.file_sync.calls_total", publications)
	beforeSequence, err := strconv.ParseUint(beforeStats["treedb.cache.cow.txn.logical_seq"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	afterSequence, err := strconv.ParseUint(database.Stats()["treedb.cache.cow.txn.logical_seq"], 10, 64)
	if err != nil || afterSequence != beforeSequence+publications || handle.CommandWALNextLSN() != beforeNext+2+uint64(barriers) {
		t.Fatalf("participant/barrier/install accounting: sequence=%d -> %d publications=%d next=%d err=%v", beforeSequence, afterSequence, publications, handle.CommandWALNextLSN(), err)
	}
	// A request now known to be an update declines before queueing/assignment.
	next, sequence := handle.CommandWALNextLSN(), afterSequence
	changed := nativeTypedOpsChanged(b)
	updated, handled, _, err := second.TryUpsertTypedBatchGroup(nativeTypedMultiColumnInput(changed))
	if err != nil || handled || updated != 0 || handle.CommandWALNextLSN() != next {
		t.Fatalf("persisted/private existing-ID decline: updated=%d handled=%v next=%d err=%v", updated, handled, handle.CommandWALNextLSN(), err)
	}
	if got, err := strconv.ParseUint(database.Stats()["treedb.cache.cow.txn.logical_seq"], 10, 64); err != nil || got != sequence {
		t.Fatalf("decline published: sequence=%d err=%v", got, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
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
		t.Fatalf("group final pins=%+v", stats)
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
	first, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []nativeTypedOpsRow{a, b, c} {
		got, err := first.Get([]byte(row.id))
		if err != nil {
			t.Fatal(err)
		}
		checkNativeTypedMultiColumnDocument(t, got, row)
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
		t.Fatalf("reopened group final pins=%+v", stats)
	}
}

func checkNativeTypedGroupFrames(t *testing.T, opts Options, firstLSN uint64, rows ...nativeTypedOpsRow) int {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]nativeTypedOpsRow, len(rows))
	for _, row := range rows {
		want[row.id] = row
	}
	barriers, frames := 0, 0
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
			if frame.LSN < firstLSN {
				continue
			}
			if frame.LSN != firstLSN+uint64(frames) {
				t.Fatalf("noncontiguous actual participant frame: %+v", frame)
			}
			frames++
			if frame.Kind == commitlog.CommandKindDurablePrefixBarrier {
				barriers++
				if len(want) != 0 || frame.PayloadFormat != commitlog.PayloadFormatDurablePrefixBarrierV1 || len(frame.Payload) != 0 {
					t.Fatalf("barrier before complete participants or invalid grammar: %+v", frame)
				}
				continue
			}
			// Each native participant may retain its original singleton aggregate
			// envelope, but two requests must never become one composite command.
			kind, format, payload := frame.Kind, frame.PayloadFormat, frame.Payload
			if kind == commitlog.CommandKindCompositeMutation {
				if format != commitlog.PayloadFormatCompositeMutationV1 {
					t.Fatalf("participant aggregate format=%d", format)
				}
				members := 0
				if err := commitlog.ScanCompositeMutationPayload(payload, func(member commitlog.CompositeMutationMember) error {
					members++
					if member.Incarnation == 0 {
						t.Fatal("participant lost captured collection incarnation")
					}
					kind, format, payload = member.Kind, member.PayloadFormat, member.Payload
					return nil
				}); err != nil || members != 1 {
					t.Fatalf("one canonical participant per frame: members=%d err=%v", members, err)
				}
			}
			if kind != commitlog.CommandKindCollectionReplaceSourceByID || format != commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
				t.Fatalf("original typed source grammar: kind=%d format=%d", kind, format)
			}
			source, err := commitlog.DecodeCollectionTypedSourcePayload(payload)
			if err != nil || source.Inserted.Collection != "events" || source.Inserted.SchemaHash == 0 || len(source.Inserted.Documents) != 1 || len(source.Inserted.Columns) != 2 || len(source.DeleteIDs) != 1 {
				t.Fatalf("participant typed source: %+v err=%v", source, err)
			}
			doc := source.Inserted.Documents[0]
			row, ok := want[string(doc.ID)]
			if !ok || !bytes.Equal(source.DeleteIDs[0], doc.ID) || !bytes.Equal(doc.Retained, bytes.TrimSpace(row.retained)) || len(doc.Values) != 2 {
				t.Fatalf("participant canonical ownership: id=%q residual=%q", doc.ID, doc.Retained)
			}
			for i, column := range source.Inserted.Columns {
				if column.Name == "label" {
					if column.Type != commitlog.CollectionTypedString || doc.Values[i].String != row.text {
						t.Fatal("participant label differs")
					}
				} else if column.Name == "embedding" {
					if column.Type != commitlog.CollectionTypedFloat32Vector || column.Dimensions != 3 || len(doc.Values[i].Vector) != 3 {
						t.Fatal("participant vector shape differs")
					}
					for j, value := range row.vector {
						if doc.Values[i].Vector[j] != value {
							t.Fatal("participant vector differs")
						}
					}
				} else {
					t.Fatalf("unexpected participant column %q", column.Name)
				}
			}
			delete(want, row.id)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(want) != 0 || barriers > 1 || frames != len(rows)+barriers {
		t.Fatalf("separate participant frames=%d barriers=%d missing=%d", frames, barriers, len(want))
	}
	return barriers
}
