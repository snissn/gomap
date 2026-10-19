package treedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// The ordinary manager and the original conditional transaction must share the
// typed pending projection, canonical raw+collection frame, and captured assets.
// Create is intentionally a hard failure while native typed storage is absent.
// The collections-package fault fixture separately exercises actual post-close
// closure failure, original accepted-build custody, poison and fresh replay.
func TestNativeExistingColumnPendingCapturedReplay(t *testing.T) {
	testNativeColumnPendingFamily(t, "int64")
}

func TestNativeExistingColumnStringVectorPendingCapturedReplay(t *testing.T) {
	if family := os.Getenv("GOMAP_COLUMN_PENDING_FAMILY"); family != "" {
		testNativeColumnPendingFamily(t, family)
		return
	}
	for _, family := range []string{"string", "float32_vector"} {
		t.Run(family, func(t *testing.T) { testNativeColumnPendingFamily(t, family) })
	}
}

// The existing typed producer has a different physical contract from ordinary
// JSON inserts: retained JSON and, for strings, row-asset ownership.
func TestNativeExistingTypedBatchPendingCapturedReplay(t *testing.T) {
	families := []string{"string", "float32_vector"}
	if family := os.Getenv("GOMAP_COLUMN_PENDING_FAMILY"); family != "" {
		families = []string{family}
	}
	for _, family := range families {
		t.Run(family, func(t *testing.T) {
			fixture := nativeColumnPendingCaseFor(t, family)
			seed := collections.TypedColumnBatch{Name: fixture.column.Name}
			switch family {
			case "string":
				fixture.column.Owner = collections.TypedStorageOwnerRowAsset
				seed.Strings = []string{"repeat"}
			case "float32_vector":
				seed.Float32Vectors = [][]float32{{1, -2, 0.5}}
			default:
				t.Fatalf("unsupported typed producer family %q", family)
			}
			fixture.typedSeed = &seed
			for i, id := range []string{"seed", "one", "two"} {
				fixture.docs[i] = append([]byte(`{"id":"`+id+`",`), fixture.docs[i][1:]...)
			}
			testNativeColumnPendingCase(t, family, fixture)
		})
	}
}

type nativeColumnPendingCase struct {
	column collections.ColumnStoreColumn
	docs   [3][]byte // seed, one, two; independent public result oracle
	// Non-nil selects the actual typed seed producer and its JSON residual
	// contract; nil preserves the original ordinary JSON family fixtures.
	typedSeed *collections.TypedColumnBatch
}

func nativeColumnPendingCaseFor(t *testing.T, family string) nativeColumnPendingCase {
	t.Helper()
	switch family {
	case "int64":
		return nativeColumnPendingCase{
			column: collections.ColumnStoreColumn{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
			docs:   [3][]byte{[]byte(`{"time_us":7,"payload":"seed"}`), []byte(`{"time_us":11,"payload":"one"}`), []byte(`{"time_us":19,"payload":"two"}`)},
		}
	case "string":
		return nativeColumnPendingCase{
			column: collections.ColumnStoreColumn{Name: "label", Path: "label", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart},
			docs:   [3][]byte{[]byte(`{"label":"repeat","payload":"seed"}`), []byte(`{"label":"repeat","payload":"one"}`), []byte(`{"label":"quote\" slash\\ tab\t unicode \u263a","payload":"two"}`)},
		}
	case "float32_vector":
		return nativeColumnPendingCase{
			column: collections.ColumnStoreColumn{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 3, Owner: collections.TypedStorageOwnerColumnPart},
			docs:   [3][]byte{[]byte(`{"embedding":[1,-2,0.5],"payload":"seed"}`), []byte(`{"embedding":[0,3.25,-4.5],"payload":"one"}`), []byte(`{"embedding":[-1.5,2,8.125],"payload":"two"}`)},
		}
	default:
		t.Fatalf("unknown typed lifecycle family %q", family)
		return nativeColumnPendingCase{}
	}
}

func testNativeColumnPendingFamily(t *testing.T, family string) {
	t.Helper()
	testNativeColumnPendingCase(t, family, nativeColumnPendingCaseFor(t, family))
}

func testNativeColumnPendingCase(t *testing.T, family string, fixture nativeColumnPendingCase) {
	t.Helper()
	const sentinel = 97
	if dir := os.Getenv("GOMAP_COLUMN_PENDING_CRASH_DIR"); dir != "" {
		runNativeColumnPending(t, dir, true, fixture)
		return
	}
	t.Run("durable", func(t *testing.T) { runNativeColumnPending(t, t.TempDir(), false, fixture) })
	t.Run("unapplied-replay", func(t *testing.T) {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^"+strings.Split(t.Name(), "/")[0]+"$", "-test.v")
		cmd.Env = append(os.Environ(), "GOMAP_COLUMN_PENDING_CRASH_DIR="+dir, "GOMAP_COLUMN_PENDING_FAMILY="+family)
		output, err := cmd.CombinedOutput()
		t.Logf("child:\n%s", output)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != sentinel || !bytes.Contains(output, []byte("COLUMN UNAPPLIED basis=2 accepted=3")) || bytes.Contains(output, []byte("DATA RACE")) {
			t.Fatalf("unapplied child: %v\n%s", err, output)
		}
		checkNativeColumnFrame(t, indexedSliceOptions(ProfileCommandWALDurable, dir), 2)
		before := workstats.Read().Replay
		database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
		if err != nil {
			t.Fatal(err)
		}
		after := workstats.Read().Replay
		if after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
			_ = database.Close()
			t.Fatalf("one aggregate replay: %+v -> %+v", before, after)
		}
		checkNativeColumnReopened(t, database, fixture)
	})
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile)+"/refuse-before-admission", func(t *testing.T) {
			database, err := Open(indexedSliceOptions(profile, t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			defer func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			}()
			lsn, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			before, pins := database.cached.COWMemoryStats(), handle.ValueLogIdentityPinRegistry().Stats()
			if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("unsupported typed profile admitted Create: %v", err)
			}
			after := database.cached.COWMemoryStats()
			if handle.CommandWALNextLSN() != lsn || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || indexedCRUDAppliedLSN(t, handle) != 0 {
				t.Fatal("unsupported profile changed a publication frontier")
			}
			// Include cumulative peak: native profile eligibility must reject
			// before admitting even a temporary private construction receipt.
			if after != before || handle.ValueLogIdentityPinRegistry().Stats() != pins {
				t.Fatalf("unsupported profile retained admission: memory %+v -> %+v pins %+v -> %+v", before, after, pins, handle.ValueLogIdentityPinRegistry().Stats())
			}
		})
	}
}

func nativeColumnPendingMeta(fixture nativeColumnPendingCase) *collections.CollectionMeta {
	var encoding collections.ColumnRetainedPayloadEncoding = collections.ColumnRetainedPayloadEncodingSemanticStreamV1
	if fixture.typedSeed != nil {
		encoding = collections.ColumnRetainedPayloadEncodingJSON
	}
	return &collections.CollectionMeta{Name: "events", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
		Enabled:                 true,
		Columns:                 []collections.ColumnStoreColumn{fixture.column},
		RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: encoding,
	}}}
}

func runNativeColumnPending(t *testing.T, dir string, crash bool, fixture nativeColumnPendingCase) {
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
	if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
		t.Fatalf("ordinary native typed Create: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if fixture.column.ValueType == collections.ColumnStoreValueFloat32Vector {
		lsn, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		basis, pins := indexedCRUDAppliedLSN(t, handle), handle.ValueLogIdentityPinRegistry().Stats()
		for _, invalid := range []string{
			`{"embedding":[1,2],"payload":"short"}`,
			`{"embedding":[1,2,3,4],"payload":"long"}`,
			`{"embedding":[1,"two",3],"payload":"type"}`,
			`{"embedding":[1,2,1e100],"payload":"float32 overflow"}`,
			`{"embedding":null,"payload":"null"}`,
			`{"payload":"missing"}`,
		} {
			if _, err := collection.InsertBatch([][]byte{[]byte("invalid")}, [][]byte{[]byte(invalid)}); err == nil {
				t.Fatalf("invalid vector accepted: %s", invalid)
			}
			if handle.CommandWALNextLSN() != lsn || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || indexedCRUDAppliedLSN(t, handle) != basis || handle.ValueLogIdentityPinRegistry().Stats() != pins {
				t.Fatal("invalid vector changed canonical publication or physical resources")
			}
			if got, err := collection.Get([]byte("invalid")); err != nil || got != nil {
				t.Fatalf("invalid vector became visible: %s %v", got, err)
			}
		}
	}
	seed := fixture.docs[0]
	if fixture.typedSeed != nil {
		// The declared value exists only in the authoritative typed carrier;
		// the independently specified complete document is solely the oracle.
		retained := [][]byte{[]byte(`{"id":"seed","payload":"seed"}`)}
		ids, _, err := collection.InsertTypedBatchWithStats([][]byte{[]byte("seed")}, retained, []collections.TypedColumnBatch{*fixture.typedSeed})
		if err != nil || len(ids) != 1 || string(ids[0]) != "seed" {
			t.Fatalf("ordinary InsertTypedBatchWithStats: %q %v", ids, err)
		}
		got, err := collection.Get([]byte("seed"))
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, seed)
	} else {
		if ids, err := collection.InsertBatch([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil || len(ids) != 1 || string(ids[0]) != "seed" {
			t.Fatalf("ordinary typed InsertBatch: %q %v", ids, err)
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis := indexedCRUDAppliedLSN(t, handle)
	if basis != 2 || handle.CommandWALNextLSN() != 3 {
		t.Fatalf("typed seed basis=%d next=%d", basis, handle.CommandWALNextLSN())
	}
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
	checkOld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("one"), []byte("two")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 3 || !got.Results[0].Found || got.Results[1].Found || got.Results[2].Found {
			t.Fatalf("held typed rows %+v: %v", got.Results, err)
		}
		requireNativeColumnJSON(t, got.Results[0].Document, seed)
		got.Results[0].Document[0] = '!'
	}
	checkOld() // Materialize the captured old typed assets before publication.
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	openingSequence := database.Stats()["treedb.cache.cow.txn.logical_seq"]
	tx, err := native.Begin("events")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	})
	docs := fixture.docs[1:]
	if err := tx.InsertBatch([][]byte{[]byte("one"), []byte("two")}, docs); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"one", "two"} {
		got, err := tx.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, docs[i])
		got[0] = '!'
		got, err = tx.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, docs[i])
		if got, err := collection.Get([]byte(id)); err != nil || got != nil {
			t.Fatalf("uncommitted typed row %s visible: %s %v", id, got, err)
		}
	}
	if err := tx.SetRaw([]byte("column/raw"), []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.GetRaw([]byte("column/raw")); err != nil || string(got) != "complete" {
		t.Fatalf("pending raw %q %v", got, err)
	}
	if got, err := database.Get([]byte("column/raw")); err != nil || got != nil {
		t.Fatalf("uncommitted raw visible: %q %v", got, err)
	}
	// A real fresh output session may already hold physical identities here.
	// Source review must establish the absence of provisional generation/LSN
	// fields or a JSON shadow; registry counts cannot prove that property.
	if handle.CommandWALNextLSN() != basis+1 || indexedCRUDAppliedLSN(t, handle) != basis || database.Stats()["treedb.cache.cow.txn.logical_seq"] != openingSequence {
		t.Fatal("pending typed projection assigned or published a canonical frontier")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	sequence, err := strconv.ParseUint(openingSequence, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if handle.CommandWALNextLSN() != basis+2 || indexedCRUDAppliedLSN(t, handle) != basis || database.Stats()["treedb.cache.cow.txn.logical_seq"] != strconv.FormatUint(sequence+1, 10) {
		t.Fatal("raw+typed commit did not install exactly one logical publication and one pending frame")
	}
	checkNativeColumnCurrent(t, manager, database, fixture)
	checkOld()
	if err := handle.FlushCommandWAL(true); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != basis {
		t.Fatal("WAL flush applied typed suffix")
	}
	checkNativeColumnFrame(t, opts, basis)
	if crash {
		t.Logf("COLUMN UNAPPLIED basis=%d accepted=%d", basis, handle.CommandWALNextLSN()-1)
		os.Exit(97) // Deliberately bypass manager/view/DB Close and checkpoint.
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != basis+1 {
		t.Fatal("checkpoint omitted aggregate typed suffix")
	}
	checkNativeColumnCurrent(t, manager, database, fixture)
	checkOld()
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
		t.Fatalf("typed final pins %+v", s)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	checkNativeColumnReopened(t, reopened, fixture)
}

func requireNativeColumnJSON(t *testing.T, got, want []byte) {
	t.Helper()
	var actual, expected any
	if err := json.Unmarshal(got, &actual); err != nil {
		t.Fatalf("typed JSON %q: %v", got, err)
	}
	if err := json.Unmarshal(want, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("typed document=%s want=%s", got, want)
	}
}

func checkNativeColumnCurrent(t *testing.T, manager *collections.CollectionManager, database *DB, fixture nativeColumnPendingCase) {
	t.Helper()
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"seed", "one", "two"} {
		got, err := collection.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, fixture.docs[i])
	}
	if got, err := database.Get([]byte("column/raw")); err != nil || string(got) != "complete" {
		t.Fatalf("aggregate raw %q %v", got, err)
	}
}

func checkNativeColumnReopened(t *testing.T, database *DB, fixture nativeColumnPendingCase) {
	t.Helper()
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	}()
	checkNativeColumnCurrent(t, manager, database, fixture)
	if indexedCRUDAppliedLSN(t, handle) != 3 {
		t.Fatal("reopened typed LSN is not the single aggregate frame")
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
		t.Fatalf("reopened typed pins %+v", s)
	}
}

func checkNativeColumnFrame(t *testing.T, opts Options, basis uint64) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("typed WAL paths %q: %v", paths, err)
	}
	var suffix []commitlog.CommandEnvelope
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			envelope, err := reader.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if envelope.LSN > basis {
				suffix = append(suffix, envelope)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(suffix) != 1 || suffix[0].LSN != basis+1 || suffix[0].Kind != commitlog.CommandKindCompositeMutation {
		t.Fatalf("typed aggregate frame %+v", suffix)
	}
	var kinds []commitlog.CommandKind
	if err := commitlog.ScanCompositeMutationPayload(suffix[0].Payload, func(member commitlog.CompositeMutationMember) error {
		kinds = append(kinds, member.Kind)
		if member.Kind != commitlog.CommandKindRawKVBatch && member.Incarnation == 0 {
			return errors.New("typed member lost captured incarnation")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []commitlog.CommandKind{commitlog.CommandKindRawKVBatch, commitlog.CommandKindCollectionInsertBatchByID}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("typed aggregate members %v want %v", kinds, want)
	}
}
