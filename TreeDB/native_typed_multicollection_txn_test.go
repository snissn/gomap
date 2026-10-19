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
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestNativeTypedMultiCollectionTxn(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_MULTI_CRASH_DIR"); dir != "" {
		runNativeTypedMultiCollectionTxn(t, dir, true)
		return
	}
	t.Run("lifecycle", func(t *testing.T) { runNativeTypedMultiCollectionTxn(t, t.TempDir(), false) })
	t.Run("unapplied-replay", func(t *testing.T) {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeTypedMultiCollectionTxn$", "-test.v")
		cmd.Env = append(os.Environ(), "GOMAP_TYPED_MULTI_CRASH_DIR="+dir)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("TYPED MULTI UNAPPLIED basis=7 accepted=8")) || bytes.Contains(output, []byte("DATA RACE")) {
			t.Fatalf("real aggregate child: %v\n%s", err, output)
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
			t.Fatalf("one complete aggregate replay: %+v -> %+v", before, after)
		}
		checkNativeTypedMultiReopened(t, database)
	})
}

// These original literals are the oracle for pending, physical, and replayed
// values. The first labels/row value is deliberately erased inside the command.
type nativeTypedMultiCase struct {
	name, family       string
	seed, first, final []byte
}

func nativeTypedMultiCases() []nativeTypedMultiCase {
	return []nativeTypedMultiCase{
		{name: "labels", family: "string", seed: []byte(`{"id":"seed","label":"old label","payload":"old string"}`), first: []byte(`{"id":"row","label":"temporary","payload":"erased"}`), final: []byte(`{"id":"row","label":"final \"quoted\" \\ label","payload":"new string"}`)},
		{name: "vectors", family: "float32_vector", seed: []byte(`{"id":"seed","embedding":[1,-2,0.5],"payload":"old vector"}`), final: []byte(`{"id":"row","embedding":[8.125,-1.5,2],"payload":"new vector"}`)},
		{name: "docs", seed: []byte(`{"id":"seed","payload":"old JSON"}`), final: []byte(`{"id":"row","payload":"new JSON","n":9}`)},
	}
}

func runNativeTypedMultiCollectionTxn(t *testing.T, dir string, crash bool) {
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
	cases := nativeTypedMultiCases()
	cols := make([]*collections.Collection, len(cases))
	for i, fixture := range cases {
		meta := &collections.CollectionMeta{Name: fixture.name}
		if fixture.family != "" {
			config := nativeColumnPendingCaseFor(t, fixture.family)
			config.typedSeed = &collections.TypedColumnBatch{} // Existing JSON residual layout.
			if fixture.family == "string" {
				config.column.Owner = collections.TypedStorageOwnerRowAsset
			}
			meta = nativeColumnPendingMeta(config)
			meta.Name = fixture.name
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			t.Fatalf("Create %s: %v", fixture.name, err)
		}
		cols[i], err = manager.OpenCollection(fixture.name)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, fixture := range cases {
		if _, err := cols[i].InsertBatch([][]byte{[]byte("seed")}, [][]byte{fixture.seed}); err != nil {
			t.Fatalf("seed %s: %v", fixture.name, err)
		}
		// Seed each typed collection separately, so setup does not require the
		// multi-collection build behavior whose real transaction is under test.
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("seed checkpoint %s: %v", fixture.name, err)
		}
	}
	if err := database.Set([]byte("typed-multi/raw"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis, ok := handle.StateToken()
	if !ok || basis.AppliedCommandLSN != 7 || indexedCRUDAppliedLSN(t, handle) != 7 || handle.CommandWALNextLSN() != 8 {
		t.Fatalf("actual seeded basis=%+v valid=%v applied=%d next=%d", basis, ok, indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN())
	}
	held := make([]*collections.CollectionReadView, len(cases))
	t.Cleanup(func() {
		for i, view := range held {
			if view != nil {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
				held[i] = nil
			}
		}
	})
	for i := range cases {
		held[i], err = cols[i].OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
	}
	checkHeld := func() {
		t.Helper()
		for i, fixture := range cases {
			response, err := held[i].FetchDocumentsByID([][]byte{[]byte("seed"), []byte("row")}, collections.DocumentFetchOptions{})
			if err != nil || len(response.Results) != 2 || !response.Results[0].Found || response.Results[1].Found {
				t.Fatalf("held %s visibility: %+v %v", fixture.name, response.Results, err)
			}
			requireNativeColumnJSON(t, response.Results[0].Document, fixture.seed)
			response.Results[0].Document[0] = '!'
		}
	}
	checkHeld()
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("labels", "vectors", "docs")
	if err != nil {
		t.Fatalf("Begin two typed collections and JSON: %v", err)
	}
	t.Cleanup(func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	})
	next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	stage := func(name string, original []byte) {
		t.Helper()
		id, doc := []byte("row"), bytes.Clone(original)
		if err := tx.InsertBatchInto(name, [][]byte{id}, [][]byte{doc}); err != nil {
			t.Fatalf("stage %s: %v", name, err)
		}
		id[0], doc[0] = '!', '!' // Both original stage inputs may be reused.
	}
	checkPrivate := func(name string, want []byte) {
		t.Helper()
		for pass := 0; pass < 2; pass++ {
			got, err := tx.GetFrom(name, []byte("row"))
			if err != nil {
				t.Fatalf("private %s: %v", name, err)
			}
			requireNativeColumnJSON(t, got, want)
			got[0] = '!' // Copy-out must not damage this or the next delta.
		}
	}
	stage("labels", cases[0].first)
	checkPrivate("labels", cases[0].first)
	checkNativeTypedMultiCurrent(t, database, manager, false)
	deleteID := []byte("row")
	deleted, err := tx.DeleteBatchFrom("labels", [][]byte{deleteID})
	if err != nil || deleted != 1 {
		t.Fatalf("private delete count=%d: %v", deleted, err)
	}
	deleteID[0] = '!'
	if got, err := tx.GetFrom("labels", []byte("row")); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
		t.Fatalf("private deleted row=%q: %v", got, err)
	}
	checkNativeTypedMultiCurrent(t, database, manager, false)
	stage("labels", cases[0].final)
	checkPrivate("labels", cases[0].final)
	stage("vectors", cases[1].final)
	checkPrivate("vectors", cases[1].final)
	stage("docs", cases[2].final)
	checkPrivate("docs", cases[2].final)
	rawKey, rawValue := []byte("typed-multi/raw"), []byte("new")
	if err := tx.SetRaw(rawKey, rawValue); err != nil {
		t.Fatal(err)
	}
	rawKey[0], rawValue[0] = '!', '!'
	if got, err := tx.GetRaw([]byte("typed-multi/raw")); err != nil || string(got) != "new" {
		t.Fatalf("private raw=%q: %v", got, err)
	}
	checkNativeTypedMultiCurrent(t, database, manager, false)
	checkHeld()
	if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
		t.Fatal("private stages consumed canonical authority")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("one multi-collection commit: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
	checkNativeTypedMultiCurrent(t, database, manager, true)
	checkHeld()
	if err := handle.FlushCommandWAL(true); err != nil {
		t.Fatal(err)
	}
	after, valid := handle.StateToken()
	if !valid || after != basis || indexedCRUDAppliedLSN(t, handle) != 7 || handle.CommandWALNextLSN() != 9 {
		t.Fatalf("aggregate applied before checkpoint: basis=%+v after=%+v valid=%v", basis, after, valid)
	}
	checkNativeTypedMultiFrame(t, opts, cases)
	if crash {
		t.Log("TYPED MULTI UNAPPLIED basis=7 accepted=8")
		os.Exit(97) // Deliberately retain the whole original uncheckpointed cut.
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("multi-collection checkpoint: %v", err)
	}
	if indexedCRUDAppliedLSN(t, handle) != 8 {
		t.Fatal("checkpoint omitted aggregate")
	}
	checkNativeTypedMultiCurrent(t, database, manager, true)
	checkHeld()
	for i, view := range held {
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		held[i] = nil
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
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("multi-collection final registry: %+v", stats)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	checkNativeTypedMultiReopened(t, reopened)
}

func checkNativeTypedMultiCurrent(t *testing.T, database *DB, manager *collections.CollectionManager, final bool) {
	t.Helper()
	for _, fixture := range nativeTypedMultiCases() {
		col, err := manager.OpenCollection(fixture.name)
		if err != nil {
			t.Fatal(err)
		}
		for pass := 0; pass < 2; pass++ {
			seed, err := col.Get([]byte("seed"))
			if err != nil {
				t.Fatal(err)
			}
			requireNativeColumnJSON(t, seed, fixture.seed)
			seed[0] = '!'
			got, err := col.Get([]byte("row"))
			if err != nil {
				t.Fatal(err)
			}
			if !final {
				if got != nil {
					t.Fatalf("uncommitted %s row visible: %s", fixture.name, got)
				}
				continue
			}
			requireNativeColumnJSON(t, got, fixture.final)
			got[0] = '!'
		}
	}
	want := "old"
	if final {
		want = "new"
	}
	if got, err := database.Get([]byte("typed-multi/raw")); err != nil || string(got) != want {
		t.Fatalf("raw=%q want=%q: %v", got, want, err)
	}
}

func checkNativeTypedMultiReopened(t *testing.T, database *DB) {
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
	checkNativeTypedMultiCurrent(t, database, manager, true)
	if indexedCRUDAppliedLSN(t, handle) != 8 || handle.CommandWALNextLSN() != 9 {
		t.Fatalf("reopened aggregate applied=%d next=%d", indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN())
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
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("reopened multi registry: %+v", stats)
	}
}

func checkNativeTypedMultiFrame(t *testing.T, opts Options, cases []nativeTypedMultiCase) {
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
			if frame.LSN > 7 {
				frames = append(frames, frame)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(frames) != 1 || frames[0].LSN != 8 || frames[0].Kind != commitlog.CommandKindCompositeMutation || frames[0].PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
		t.Fatalf("one LSN8 aggregate: %+v", frames)
	}
	var members []commitlog.CompositeMutationMember
	if err := commitlog.ScanCompositeMutationPayload(frames[0].Payload, func(member commitlog.CompositeMutationMember) error { members = append(members, member); return nil }); err != nil {
		t.Fatal(err)
	}
	wantKinds := []commitlog.CommandKind{commitlog.CommandKindRawKVBatch, commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionInsertBatchByID}
	wantFormats := []commitlog.PayloadFormat{commitlog.PayloadFormatRawKVBatchV1, commitlog.PayloadFormatCollectionInsertBatchByIDV1, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, commitlog.PayloadFormatCollectionInsertBatchByIDV1, commitlog.PayloadFormatCollectionInsertBatchByIDV1, commitlog.PayloadFormatCollectionInsertBatchByIDV1}
	if len(members) != len(wantKinds) {
		t.Fatalf("aggregate members=%d want=%d", len(members), len(wantKinds))
	}
	for i, member := range members {
		if member.Kind != wantKinds[i] || member.PayloadFormat != wantFormats[i] {
			t.Fatalf("member%d kind=%d format=%d", i, member.Kind, member.PayloadFormat)
		}
	}
	// SetRaw uses ordinary ConditionalTxn.Set, whose entry revision is legacy
	// zero. The assigned outer command LSN does not assign an entry revision.
	raw, err := commitlog.DecodeRawKVBatchPayload(members[0].Payload)
	if err != nil || members[0].Incarnation != 0 || len(raw) != 1 || raw[0].Op != commitlog.RawKVOpSet || string(raw[0].Key) != "typed-multi/raw" || string(raw[0].Value) != "new" || raw[0].Revision != 0 {
		t.Fatalf("one original raw binding: %+v %v", raw, err)
	}
	for _, i := range []int{1, 2, 3, 4, 5} {
		if members[i].Incarnation == 0 {
			t.Fatalf("member%d lost original incarnation", i)
		}
	}
	if members[1].Incarnation != members[2].Incarnation || members[1].Incarnation != members[3].Incarnation || members[1].Incarnation == members[4].Incarnation || members[1].Incarnation == members[5].Incarnation || members[4].Incarnation == members[5].Incarnation {
		t.Fatal("aggregate collection identities do not preserve ordered bindings")
	}
	deletion, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(members[2].Payload)
	if err != nil || deletion.Collection != "labels" || len(deletion.IDs) != 1 || string(deletion.IDs[0]) != "row" {
		t.Fatalf("original private delete payload: %+v %v", deletion, err)
	}
	for j, i := range []int{1, 3, 4, 5} {
		name, want := cases[0].name, cases[0].first
		if j > 0 {
			name, want = cases[j-1].name, cases[j-1].final
		}
		payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(members[i].Payload)
		if err != nil || payload.Collection != name || len(payload.Documents) != 1 || string(payload.Documents[0].ID) != "row" || !bytes.Equal(payload.Documents[0].Document, want) {
			t.Fatalf("original member%d insert: %+v %v", i, payload, err)
		}
	}
}
