package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The ordinary public constructor must activate BSON on the same native owner.
// WAL children leave a real raw/BSON aggregate fsynced above physical basis4;
// NoWAL asserts only its explicit checkpoint/Close/reopen contract.
func TestNativeExistingBSONPublicLifecycle(t *testing.T) {
	childDir, childProfile := os.Getenv("GOMAP_NATIVE_BSON_CRASH_DIR"), os.Getenv("GOMAP_NATIVE_BSON_PROFILE")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		if childDir != "" && string(profile) != childProfile {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := childDir
			if dir == "" {
				dir = t.TempDir()
			}
			runNativeBSONLifecycle(t, profile, dir, childDir != "")
		})
		if childDir != "" || profile == ProfileNoWALFast {
			continue
		}
		t.Run(string(profile)+"/unapplied-aggregate", func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingBSONPublicLifecycle$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_NATIVE_BSON_CRASH_DIR="+dir, "GOMAP_NATIVE_BSON_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("BSON UNAPPLIED basis=4 accepted=5")) || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("unapplied child: %v\n%s", err, output)
			}
			opts := indexedSliceOptions(profile, dir)
			checkNativeBSONAggregate(t, opts)
			before := workstats.Read().Replay
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			after := workstats.Read().Replay
			t.Cleanup(func() {
				if database != nil {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			if after.FramesAttempted-before.FramesAttempted != 1 || after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
				t.Fatalf("actual one aggregate replay: %+v -> %+v", before, after)
			}
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
			checkNativeBSONFinal(t, database, manager)
			if got := indexedCRUDAppliedLSN(t, handle); got != 5 || handle.CommandWALNextLSN() != 6 {
				t.Fatalf("replayed physical=%d next=%d", got, handle.CommandWALNextLSN())
			}
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
				t.Fatalf("replayed registry: %+v", s)
			}
		})
	}
}

func runNativeBSONLifecycle(t *testing.T, profile Profile, dir string, crash bool) {
	t.Helper()
	// Exclude asynchronous maintenance from the deliberately unapplied suffix;
	// explicit checkpoints and their ordinary WAL retirement remain enabled.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(profile, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var handle *backenddb.DB
	var held *collections.CollectionReadView
	closeOriginal := func() {
		t.Helper()
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
			held = nil
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if database != nil {
			cache := database.cached
			if err := database.Close(); err != nil {
				t.Error(err)
			}
			database = nil
			requirePublicCOWFinalZero(t, cache)
			if handle != nil {
				if s := handle.ValueLogIdentityPinRegistry().Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
					t.Errorf("closed registry: %+v", s)
				}
			}
		}
	}
	t.Cleanup(closeOriginal)
	if got := database.Stats()["treedb.cache.memtable_mode"]; got != "cow_btree" {
		t.Fatalf("direct BSON owner mode=%q, want cow_btree", got)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "users", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
		Indexes: []collections.IndexDefinition{
			{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: collections.IndexValueString},
			{Name: "age", Field: "age", ValueType: collections.IndexValueInt64},
			{Name: "tag", Field: "tag", ValueType: collections.IndexValueBSONOrderedV2},
		},
	}); err != nil {
		t.Fatalf("ordinary activated BSON Create: %v", err)
	}
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("u1"), []byte("u2")}
	seed := nativeBSONSeed(t)
	input := [][]byte{bytes.Clone(seed[0]), bytes.Clone(seed[1])}
	returned, err := users.InsertBatch(ids, input)
	if err != nil || len(returned) != 2 || string(returned[0]) != "u1" || string(returned[1]) != "u2" {
		t.Fatalf("ordinary BSON insert IDs=%q: %v", returned, err)
	}
	input[0][0] ^= 0x80
	returned[0][0] = '!'
	checkNativeBSONDocument(t, users, "u1", seed[0])
	checkNativeBSONDocument(t, users, "u2", seed[1])
	checkNativeBSONIndex(t, users, "age", int64(37), "u1")
	checkNativeBSONIndex(t, users, "tag", bson.RawValue{}, "u1")
	checkNativeBSONIndex(t, users, "tag", nativeBSONQuery(t, nil), "u2")
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 2 {
		t.Fatalf("seed basis=%d want2", indexedCRUDAppliedLSN(t, handle))
	}
	held, err = users.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		response, err := held.FetchDocumentsByID([][]byte{[]byte("u1"), []byte("u2")}, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 {
			t.Fatalf("held BSON fetch: %+v %v", response, err)
		}
		for i, row := range response.Results {
			if !row.Found || !bytes.Equal(row.Document, seed[i]) || bson.Raw(row.Document).Validate() != nil {
				t.Fatalf("held original BSON row%d=%x want%x", i, row.Document, seed[i])
			}
			row.Document[0] ^= 0x80
		}
		var got []string
		if err := held.VisitIndexValueIDs("email", "a@example.com", func(id []byte) error { got = append(got, string(id)); return nil }); err != nil || !reflect.DeepEqual(got, []string{"u1"}) {
			t.Fatalf("held unique owner=%q: %v", got, err)
		}
	}
	checkHeld()
	sequence := func() uint64 {
		t.Helper()
		capture, err := handle.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		seq := capture.LogicalSequence()
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		return seq
	}
	next, seq := handle.CommandWALNextLSN(), sequence()
	state, ok := handle.StateToken()
	if !ok {
		t.Fatal("missing original physical state")
	}
	duplicate := nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u9"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(99)}})
	if _, err := users.InsertBatch([][]byte{[]byte("u9")}, [][]byte{duplicate}); !collections.IsDuplicateKeyError(err) {
		t.Fatalf("unique refusal: %v", err)
	}
	after, afterOK := handle.StateToken()
	if handle.CommandWALNextLSN() != next || sequence() != seq || !afterOK || after != state {
		t.Fatal("unique refusal consumed canonical identity or changed a cut")
	}
	if got, err := users.Get([]byte("u9")); err != nil || got != nil {
		t.Fatalf("refused BSON row visible=%x: %v", got, err)
	}
	// Original BSON bytes remain authoritative. The ordinary batch moves both
	// unique owners together, with each callback invoked once on its opening row.
	swapped := [][]byte{
		nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "b@example.com"}, {Key: "city", Value: "nyc"}, {Key: "age", Value: int64(37)}}),
		nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "nyc"}, {Key: "age", Value: int32(42)}, {Key: "tag", Value: nil}}),
	}
	calls := [2]int{}
	items := make([]collections.UpdateBatchItem, 2)
	for i := range items {
		i := i
		items[i] = collections.UpdateBatchItem{DocumentID: []byte(fmt.Sprintf("u%d", i+1)), Update: func(current []byte) ([]byte, bool, error) {
			calls[i]++
			if !bytes.Equal(current, seed[i]) {
				return nil, false, fmt.Errorf("BSON update%d opening bytes differ", i)
			}
			return bytes.Clone(swapped[i]), true, nil
		}}
	}
	updated, err := users.UpdateBatch(items)
	if err != nil || len(updated) != 2 || !updated[0].Matched || !updated[1].Matched || !updated[0].Modified || !updated[1].Modified || calls != [2]int{1, 1} {
		t.Fatalf("ordinary BSON update=%+v calls=%v: %v", updated, calls, err)
	}
	checkNativeBSONIndex(t, users, "email", "b@example.com", "u1")
	checkNativeBSONIndex(t, users, "email", "a@example.com", "u2")
	if n, err := users.DeleteBatch([][]byte{[]byte("u2"), []byte("missing")}); err != nil || n != 1 {
		t.Fatalf("ordinary BSON delete=%d: %v", n, err)
	}
	checkNativeBSONIndex(t, users, "email", "a@example.com")
	checkHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 4 {
		t.Fatalf("pre-aggregate basis=%d want4", indexedCRUDAppliedLSN(t, handle))
	}
	checkHeld()
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("users")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	})
	final := nativeBSONFinal(t)
	if result, err := tx.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
		if !bytes.Equal(current, swapped[0]) {
			return nil, false, errors.New("aggregate read wrong BSON cut")
		}
		return bytes.Clone(final[0]), true, nil
	}}}); err != nil || len(result) != 1 || !result[0].Modified {
		t.Fatalf("BSON aggregate update=%+v: %v", result, err)
	}
	if err := tx.InsertBatch([][]byte{[]byte("u3")}, [][]byte{final[1]}); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetRaw([]byte("bson/raw"), []byte("accepted")); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.Get([]byte("u3")); err != nil || !bytes.Equal(got, final[1]) {
		t.Fatalf("BSON private read=%x: %v", got, err)
	} else {
		got[0] ^= 0x80
	}
	if got, err := tx.Get([]byte("u3")); err != nil || !bytes.Equal(got, final[1]) {
		t.Fatalf("private copy ownership: %x %v", got, err)
	}
	checkNativeBSONDocument(t, users, "u1", swapped[0])
	if got, err := users.Get([]byte("u3")); err != nil || got != nil {
		t.Fatalf("uncommitted BSON visible=%x: %v", got, err)
	}
	seq = sequence()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if sequence() != seq+1 {
		t.Fatal("aggregate did not install exactly one logical successor")
	}
	checkNativeBSONFinal(t, database, manager)
	checkHeld()
	if profile != ProfileNoWALFast {
		if handle.CommandWALNextLSN() != 6 || indexedCRUDAppliedLSN(t, handle) != 4 {
			t.Fatalf("aggregate next=%d physical=%d", handle.CommandWALNextLSN(), indexedCRUDAppliedLSN(t, handle))
		}
		if crash {
			if err := handle.FlushCommandWAL(true); err != nil {
				t.Fatal(err)
			}
			if indexedCRUDAppliedLSN(t, handle) != 4 {
				t.Fatal("WAL sync applied the aggregate")
			}
			t.Log("BSON UNAPPLIED basis=4 accepted=5")
			os.Exit(97)
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeBSONFinal(t, database, manager)
	checkHeld()
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 5 {
		t.Fatal("checkpoint lost aggregate prefix")
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
	checkNativeBSONFinal(t, database, manager)
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 5 {
		t.Fatal("reopen lost aggregate prefix")
	}
	closeOriginal()
}

func nativeBSONDocument(t *testing.T, document bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func nativeBSONSeed(t *testing.T) [][]byte {
	return [][]byte{
		nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int32(37)}}),
		nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}, {Key: "city", Value: "sea"}, {Key: "age", Value: int64(42)}, {Key: "tag", Value: nil}}),
	}
}

func nativeBSONFinal(t *testing.T) [][]byte {
	return [][]byte{
		nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "nyc"}, {Key: "age", Value: int64(37)}, {Key: "tag", Value: nil}}),
		nativeBSONDocument(t, bson.D{{Key: "_id", Value: "u3"}, {Key: "email", Value: "c@example.com"}, {Key: "city", Value: "nyc"}, {Key: "age", Value: int32(37)}, {Key: "tag", Value: int32(7)}}),
	}
}

func nativeBSONQuery(t *testing.T, value any) bson.RawValue {
	return bson.Raw(nativeBSONDocument(t, bson.D{{Key: "tag", Value: value}})).Lookup("tag")
}

func checkNativeBSONDocument(t *testing.T, users *collections.Collection, id string, want []byte) {
	t.Helper()
	for pass := 0; pass < 2; pass++ {
		got, err := users.Get([]byte(id))
		if err != nil || !bytes.Equal(got, want) || bson.Raw(got).Validate() != nil {
			t.Fatalf("BSON %s=%x want%x: %v", id, got, want, err)
		}
		got[0] ^= 0x80
	}
}

func checkNativeBSONIndex(t *testing.T, users *collections.Collection, index string, value any, want ...string) {
	t.Helper()
	ids, err := users.FindByIndexValue(index, value)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(ids))
	for i, id := range ids {
		got[i] = string(id)
	}
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("BSON index%s value%v IDs=%q want%q", index, value, got, want)
	}
}

func checkNativeBSONFinal(t *testing.T, database *DB, manager *collections.CollectionManager) {
	t.Helper()
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	final := nativeBSONFinal(t)
	checkNativeBSONDocument(t, users, "u1", final[0])
	checkNativeBSONDocument(t, users, "u3", final[1])
	if _, found, err := users.GetInto([]byte("u2"), nil); err != nil || found {
		t.Fatalf("deleted BSON found=%t: %v", found, err)
	}
	checkNativeBSONIndex(t, users, "email", "a@example.com", "u1")
	checkNativeBSONIndex(t, users, "email", "b@example.com")
	checkNativeBSONIndex(t, users, "email", "c@example.com", "u3")
	checkNativeBSONIndex(t, users, "city", "hnl")
	checkNativeBSONIndex(t, users, "city", "sea")
	checkNativeBSONIndex(t, users, "city", "nyc", "u1", "u3")
	checkNativeBSONIndex(t, users, "age", int64(37), "u1", "u3")
	checkNativeBSONIndex(t, users, "tag", bson.RawValue{})
	checkNativeBSONIndex(t, users, "tag", nativeBSONQuery(t, nil), "u1")
	checkNativeBSONIndex(t, users, "tag", nativeBSONQuery(t, int64(7)), "u3")
	if got, err := database.Get([]byte("bson/raw")); err != nil || string(got) != "accepted" {
		t.Fatalf("aggregate raw=%q: %v", got, err)
	}
}

func checkNativeBSONAggregate(t *testing.T, opts Options) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var suffix []commitlog.CommandEnvelope
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			env, err := reader.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if env.LSN > 4 {
				suffix = append(suffix, env)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(suffix) != 1 || suffix[0].LSN != 5 || suffix[0].BaseAppliedLSN != 4 || suffix[0].Kind != commitlog.CommandKindCompositeMutation || suffix[0].PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
		t.Fatalf("one actual unapplied BSON aggregate: %+v", suffix)
	}
	members, err := commitlog.DecodeCompositeMutationPayloadView(suffix[0].Payload)
	if err != nil || len(members) != 3 {
		t.Fatalf("BSON aggregate members=%+v: %v", members, err)
	}
	if members[0].Kind != commitlog.CommandKindRawKVBatch || members[0].PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 || members[0].Incarnation != 0 {
		t.Fatal("raw member binding changed")
	}
	raw, err := commitlog.DecodeRawKVBatchPayload(members[0].Payload)
	if err != nil || len(raw) != 1 || raw[0].Op != commitlog.RawKVOpSet || string(raw[0].Key) != "bson/raw" || string(raw[0].Value) != "accepted" || raw[0].Revision != 0 {
		t.Fatalf("original raw member=%+v: %v", raw, err)
	}
	if members[1].Kind != commitlog.CommandKindCollectionUpdateBatchByID || members[1].PayloadFormat != commitlog.PayloadFormatCollectionUpdateBatchByIDV1 || members[1].Incarnation == 0 || members[2].Kind != commitlog.CommandKindCollectionInsertBatchByID || members[2].PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 || members[2].Incarnation != members[1].Incarnation {
		t.Fatal("original BSON update/insert member authority changed")
	}
	updated, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(members[1].Payload)
	if err != nil {
		t.Fatal(err)
	}
	inserted, err := commitlog.DecodeCollectionInsertBatchByIDPayload(members[2].Payload)
	if err != nil {
		t.Fatal(err)
	}
	final := nativeBSONFinal(t)
	for i, p := range []struct {
		Collection string
		Documents  []commitlog.CollectionDocument
	}{{updated.Collection, updated.Documents}, {inserted.Collection, inserted.Documents}} {
		wantID := "u1"
		if i == 1 {
			wantID = "u3"
		}
		if p.Collection != "users" || len(p.Documents) != 1 || string(p.Documents[0].ID) != wantID || !bytes.Equal(p.Documents[0].Document, final[i]) {
			t.Fatalf("canonical BSON member%d lost original bytes: %+v", i, p)
		}
	}
}
