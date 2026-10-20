package treedb

import (
	"bytes"
	"errors"
	"math"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// One maintained planner must preserve effective array sets through pending
// CRUD, index backfill, physical publication and original canonical replay.
func TestNativeExistingMultikeyLifecycle(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
			t.Run(string(format)+"/"+string(profile), func(t *testing.T) { runNativeMultikeyLifecycle(t, profile, format, t.TempDir(), false) })
		}
	}
}

func TestNativeExistingMultikeyUnappliedReplay(t *testing.T) {
	const selector = "TestNativeExistingMultikeyUnappliedReplay"
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	childDir, childFormat, childProfile := os.Getenv("GOMAP_MULTIKEY_DIR"), os.Getenv("GOMAP_MULTIKEY_FORMAT"), os.Getenv("GOMAP_MULTIKEY_PROFILE")
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
			if childDir != "" && (childFormat != string(format) || childProfile != string(profile)) {
				continue
			}
			t.Run(string(format)+"/"+string(profile), func(t *testing.T) {
				if childDir != "" {
					runNativeMultikeyLifecycle(t, profile, format, childDir, true)
					return
				}
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^"+selector+"$", "-test.v")
				cmd.Env = append(os.Environ(), "GOMAP_MULTIKEY_DIR="+dir, "GOMAP_MULTIKEY_FORMAT="+string(format), "GOMAP_MULTIKEY_PROFILE="+string(profile))
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("MULTIKEY UNAPPLIED basis=2 accepted=7")) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("unapplied child: %v\n%s", err, output)
				}
				before := workstats.Read().Replay
				database, err := Open(indexedSliceOptions(profile, dir))
				if err != nil {
					t.Fatal(err)
				}
				after := workstats.Read().Replay
				if after.FramesApplied-before.FramesApplied != 5 || after.FrameErrors != before.FrameErrors {
					_ = database.Close()
					t.Fatalf("five original frames: %+v -> %+v", before, after)
				}
				handle, err := database.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager := collections.NewCollectionManager(handle)
				collection, err := manager.OpenCollection("arrays")
				if err != nil {
					t.Fatal(err)
				}
				checkNativeMultikeyFinal(t, collection, format)
				checkNativeMultikeyRaw(t, database)
				if applied := indexedCRUDAppliedLSN(t, handle); applied != 7 {
					t.Fatalf("replay applied=%d want7", applied)
				}
				cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
				if err = manager.Close(); err != nil {
					t.Fatal(err)
				}
				if err = database.Close(); err != nil {
					t.Fatal(err)
				}
				requirePublicCOWFinalZero(t, cache)
				if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
					t.Fatalf("replay registry: %+v", stats)
				}
			})
		}
	}
}

func nativeMultikeyDocument(t *testing.T, format collections.DocumentFormat, source string) []byte {
	t.Helper()
	return nativeIndexDDLDocument(t, format, source)
}

func runNativeMultikeyLifecycle(t *testing.T, profile Profile, format collections.DocumentFormat, dir string, crash bool) {
	t.Helper()
	opts := indexedSliceOptions(profile, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	t.Cleanup(func() {
		if manager != nil {
			_ = manager.Close()
		}
		if database != nil {
			_ = database.Close()
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	indexes := []collections.IndexDefinition{
		{Name: "tags", Field: "tags", ValueType: collections.IndexValueString, MultiKey: true},
		{Name: "aliases", Field: "aliases", ValueType: collections.IndexValueString, MultiKey: true, Unique: true},
		{Name: "nested", Field: "profile.tags", ValueType: collections.IndexValueString, MultiKey: true},
	}
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "arrays", Options: collections.CollectionOptions{DocumentFormat: format}, Indexes: indexes}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("arrays")
	if err != nil {
		t.Fatal(err)
	}
	doc := func(source string) []byte { return nativeMultikeyDocument(t, format, source) }
	original := doc(`{"tags":["b","a","a",null],"aliases":["ua","ua"],"profile":{"tags":["x","x"]},"note":"seed"}`)
	second := doc(`{"tags":["c"],"aliases":["ub"],"profile":{"tags":["y"]}}`)
	if _, err = collection.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{original, second}); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 2 {
		t.Fatal("seed basis must be actual LSN2")
	}
	old, err := collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if old != nil {
			_ = old.Close()
		}
	}()
	pending := doc(`{"tags":["a","d"],"aliases":["uc"],"profile":{"tags":["z"]}}`)
	if _, err = collection.InsertBatch([][]byte{[]byte("c")}, [][]byte{pending}); err != nil {
		t.Fatal(err)
	}
	// Reorder and duplicate/null changes preserve the same effective index set.
	reordered := doc(`{"tags":["a",null,"b","a"],"aliases":["ua"],"profile":{"tags":["x"]},"note":"reordered"}`)
	if _, err = collection.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return reordered, true, nil }}}); err != nil {
		t.Fatal(err)
	}
	if _, err = collection.CreateIndex(collections.IndexDefinition{Name: "backfill", Field: "tags", ValueType: collections.IndexValueString, MultiKey: true}); err != nil {
		t.Fatal(err)
	}
	if ids, err := collection.FindByIndexValue("backfill", "a"); err != nil || !reflect.DeepEqual(ids, [][]byte{[]byte("a"), []byte("c")}) {
		t.Fatalf("complete-cut backfill=%q %v", ids, err)
	}
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("arrays")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Close() }()
	finalA := doc(`{"tags":["d","b","b"],"aliases":["ub","ub"],"profile":{"tags":["x","q"]},"note":"final"}`)
	finalB := doc(`{"tags":["c"],"aliases":["ua"],"profile":{"tags":["y"]}}`)
	if _, err = tx.UpdateBatch([]collections.UpdateBatchItem{
		{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return finalA, true, nil }},
		{DocumentID: []byte("b"), Update: func([]byte) ([]byte, bool, error) { return finalB, true, nil }},
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := tx.DeleteBatch([][]byte{[]byte("c")}); err != nil || n != 1 {
		t.Fatalf("private delete=%d %v", n, err)
	}
	finalC := doc(`{"tags":["c","d","d",null],"aliases":["uc"],"profile":{"tags":["z"]}}`)
	if err = tx.InsertBatch([][]byte{[]byte("c")}, [][]byte{finalC}); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.Get([]byte("c")); err != nil || !bytes.Equal(got, finalC) {
		t.Fatalf("RYOW=%s %v", got, err)
	} else {
		got[0] ^= 1
	}
	if got, err := tx.Get([]byte("c")); err != nil || !bytes.Equal(got, finalC) {
		t.Fatal("RYOW copy escaped")
	}
	if err = tx.SetRaw([]byte("multikey/raw"), []byte("one cut")); err != nil {
		t.Fatal(err)
	}
	next := handle.CommandWALNextLSN()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && handle.CommandWALNextLSN() != next+1 {
		t.Fatal("aggregate did not use one canonical identity")
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}
	// Final unique ownership is a handoff, not an intermediate duplicate error.
	for value, id := range map[string]string{"ua": "b", "ub": "a", "uc": "c"} {
		ids, err := collection.FindByIndexValue("aliases", value)
		if err != nil || !reflect.DeepEqual(ids, [][]byte{[]byte(id)}) {
			t.Fatalf("unique %s=%q %v", value, ids, err)
		}
	}
	next = handle.CommandWALNextLSN()
	capture, err := handle.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	seq := capture.LogicalSequence()
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = collection.InsertBatch([][]byte{[]byte("conflict")}, [][]byte{doc(`{"tags":["q"],"aliases":["ua"]}`)}); !errors.Is(err, collections.ErrUniqueIndexConflict) {
		t.Fatalf("unique conflict=%v", err)
	}
	capture, err = handle.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	afterSeq := capture.LogicalSequence()
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	if handle.CommandWALNextLSN() != next || afterSeq != seq {
		t.Fatal("conflict claimed identity")
	}
	if _, err = collection.DropIndex("backfill"); err != nil {
		t.Fatal(err)
	}
	checkNativeMultikeyFinal(t, collection, format)
	checkNativeMultikeyRaw(t, database)
	checkOld := func() {
		t.Helper()
		response, err := old.FetchDocumentsByID([][]byte{[]byte("a"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 || !response.Results[0].Found || response.Results[1].Found || !bytes.Equal(response.Results[0].Document, original) {
			t.Fatalf("held old visibility: %+v %v", response.Results, err)
		}
		response.Results[0].Document[0] ^= 1
	}
	checkOld()
	if crash {
		if err = handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if applied, accepted := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN()-1; applied != 2 || accepted != 7 {
			t.Fatalf("unapplied basis=%d accepted=%d", applied, accepted)
		}
		t.Log("MULTIKEY UNAPPLIED basis=2 accepted=7")
		os.Exit(97)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeMultikeyFinal(t, collection, format)
	checkNativeMultikeyRaw(t, database)
	checkOld()
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("registry: %+v", stats)
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
	collection, err = manager.OpenCollection("arrays")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeMultikeyFinal(t, collection, format)
	checkNativeMultikeyRaw(t, database)
	cache, registry = database.cached, handle.ValueLogIdentityPinRegistry()
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("reopened registry: %+v", stats)
	}
}

func checkNativeMultikeyFinal(t *testing.T, collection *collections.Collection, format collections.DocumentFormat) {
	t.Helper()
	for id, source := range map[string]string{
		"a": `{"tags":["d","b","b"],"aliases":["ub","ub"],"profile":{"tags":["x","q"]},"note":"final"}`,
		"b": `{"tags":["c"],"aliases":["ua"],"profile":{"tags":["y"]}}`,
		"c": `{"tags":["c","d","d",null],"aliases":["uc"],"profile":{"tags":["z"]}}`,
	} {
		want := nativeMultikeyDocument(t, format, source)
		got, err := collection.Get([]byte(id))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("primary %s=%s %v", id, got, err)
		}
		got[0] ^= 1
	}
	for _, query := range []struct {
		index, value string
		ids          []string
	}{{"tags", "b", []string{"a"}}, {"tags", "c", []string{"b", "c"}}, {"tags", "d", []string{"a", "c"}}, {"nested", "x", []string{"a"}}, {"backfill", "b", nil}} {
		ids, err := collection.FindByIndexValue(query.index, query.value)
		var got []string
		for _, id := range ids {
			got = append(got, string(id))
		}
		if err != nil || !reflect.DeepEqual(got, query.ids) {
			t.Fatalf("%s/%s=%q %v want%q", query.index, query.value, got, err, query.ids)
		}
	}
	for _, limit := range []int{0, 2, 3} {
		opts := collections.IndexRangeOptions{Lower: collections.IndexRangeBound{Unbounded: true}, Upper: collections.IndexRangeBound{Unbounded: true}, Limit: limit}
		expected := []string{"a", "b", "c"}
		if limit == 2 {
			expected = expected[:2]
		}
		check := func(ids [][]byte, truncated bool, err error) {
			t.Helper()
			var got []string
			for _, id := range ids {
				got = append(got, string(id))
			}
			if err != nil || !reflect.DeepEqual(got, expected) || truncated != (limit == 2) {
				t.Fatalf("range limit%d=%q truncated%v err%v", limit, got, truncated, err)
			}
		}
		ids, truncated, err := collection.FindByIndexRange("tags", opts)
		check(ids, truncated, err)
		var visited [][]byte
		truncated, err = collection.ScanIndexRange("tags", opts, func(id []byte) (bool, error) {
			visited = append(visited, bytes.Clone(id))
			id[0] ^= 1
			return true, nil
		})
		check(visited, truncated, err)
		if opts.Limit == 0 {
			opts.Limit = 8
		}
		records, truncated, err := collection.FindDocumentsByIndexRange("tags", opts)
		ids = nil
		for _, record := range records {
			ids = append(ids, record.ID)
			want, readErr := collection.Get(record.ID)
			if readErr != nil || !bytes.Equal(record.Document, want) {
				t.Fatal("secondary/primary cut mismatch")
			}
		}
		check(ids, truncated, err)
	}
}

// The legacy array option and BSON dotted arrays share the same extraction
// route. Exact numeric expansion can be much larger than raw BSON doubles.
func TestNativeExistingMultikeyArrayOptionAndNumericExpansion(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() { _ = manager.Close() })
			valueType := collections.IndexValueInt64
			var document []byte
			var query any = int64(1)
			indexes := []collections.IndexDefinition{{Name: "values", Field: "values", ValueType: valueType}}
			if format == collections.DocumentFormatJSON {
				// Repeated values normalize to one key; both distinct matches
				// still produce one result document and no false truncation.
				document = []byte(`{"values":[1,1,2,null]}`)
			} else {
				indexes[0].ValueType = collections.IndexValueBSONOrderedV2
				indexes = append(indexes, collections.IndexDefinition{Name: "nested", Field: "groups.children.values", ValueType: collections.IndexValueBSONOrderedV2, MultiKey: true})
				values := make(bson.A, 64)
				for i := range values {
					values[i] = math.SmallestNonzeroFloat64
				}
				values = append(values, int32(1), int64(1), nil)
				document = nativeBSONDocument(t, bson.D{
					{Key: "values", Value: values},
					{Key: "groups", Value: bson.A{
						bson.D{{Key: "children", Value: bson.A{bson.D{{Key: "values", Value: bson.A{int32(1), int64(1)}}}}}},
						bson.D{{Key: "children", Value: bson.A{bson.D{{Key: "values", Value: bson.A{int64(2)}}}}}},
					}},
				})
				query = nativeBSONQuery(t, math.SmallestNonzeroFloat64)
			}
			if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "option", Options: collections.CollectionOptions{DocumentFormat: format, AllowArrayValuesInIndex: true}, Indexes: indexes}); err != nil {
				t.Fatal(err)
			}
			collection, err := manager.OpenCollection("option")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = collection.InsertBatch([][]byte{[]byte("only")}, [][]byte{document}); err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				ids, err := collection.FindByIndexValue("values", query)
				if err != nil || !reflect.DeepEqual(ids, [][]byte{[]byte("only")}) {
					t.Fatalf("array option equality=%q %v", ids, err)
				}
				if format == collections.DocumentFormatBSON {
					for _, value := range []any{int32(1), int64(1), int64(2)} {
						ids, err = collection.FindByIndexValue("nested", nativeBSONQuery(t, value))
						if err != nil || !reflect.DeepEqual(ids, [][]byte{[]byte("only")}) {
							t.Fatalf("dotted array equality=%q %v", ids, err)
						}
					}
				}
				ids, truncated, err := collection.FindByIndexRange("values", collections.IndexRangeOptions{Lower: collections.IndexRangeBound{Unbounded: true}, Upper: collections.IndexRangeBound{Unbounded: true}, Limit: 1})
				if err != nil || truncated || !reflect.DeepEqual(ids, [][]byte{[]byte("only")}) {
					t.Fatalf("distinct limit=%q %v %v", ids, truncated, err)
				}
			}
			check()
			next := handle.CommandWALNextLSN()
			invalid := nativeMultikeyDocument(t, format, `{"values":[{"unsupported":"object"}]}`)
			if _, err = collection.InsertBatch([][]byte{[]byte("invalid")}, [][]byte{invalid}); err == nil || handle.CommandWALNextLSN() != next {
				t.Fatalf("invalid array assigned identity: %v", err)
			}
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check()
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
				t.Fatalf("array option registry: %+v", stats)
			}
		})
	}
}

func checkNativeMultikeyRaw(t *testing.T, database *DB) {
	t.Helper()
	if got, err := database.Get([]byte("multikey/raw")); err != nil || !bytes.Equal(got, []byte("one cut")) {
		t.Fatalf("same aggregate raw=%q %v", got, err)
	}
}
