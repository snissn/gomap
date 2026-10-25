package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Compound keys must use the original BSON tuple representation through the
// same public owner, including pending sources and genuine reverse traversal.
func TestNativeExistingBSONCompoundLifecycle(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) { runNativeBSONCompound(t, profile, t.TempDir(), false) })
	}
}

func TestNativeExistingBSONCompoundUnappliedReplay(t *testing.T) {
	const selector = "TestNativeExistingBSONCompoundUnappliedReplay"
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	childDir, childProfile := os.Getenv("GOMAP_COMPOUND_DIR"), os.Getenv("GOMAP_COMPOUND_PROFILE")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		if childDir != "" && childProfile != string(profile) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			if childDir != "" {
				runNativeBSONCompound(t, profile, childDir, true)
				return
			}
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^"+selector+"$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_COMPOUND_DIR="+dir, "GOMAP_COMPOUND_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("COMPOUND UNAPPLIED basis=2 accepted=7")) || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("unapplied child: %v\n%s", err, output)
			}
			before := workstats.Read().Replay
			database, err := Open(indexedSliceOptions(profile, dir))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			after := workstats.Read().Replay
			if after.FramesAttempted-before.FramesAttempted != 5 || after.FramesApplied-before.FramesApplied != 5 || after.FrameErrors != before.FrameErrors {
				t.Fatalf("five original CRUD/DDL/aggregate frames: %+v -> %+v", before, after)
			}
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() { _ = manager.Close() })
			collection, err := manager.OpenCollection("tuples")
			if err != nil {
				t.Fatal(err)
			}
			checkNativeBSONCompoundFinal(t, database, collection)
			if applied := indexedCRUDAppliedLSN(t, handle); applied != 7 || handle.CommandWALNextLSN() != 8 {
				t.Fatalf("replay applied=%d next=%d", applied, handle.CommandWALNextLSN())
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
				t.Fatalf("replayed registry: %+v", stats)
			}
		})
	}
}

func nativeBSONCompoundIndex(name, field string, descending, unique bool) collections.IndexDefinition {
	direction := collections.IndexDirectionAscending
	if descending {
		direction = collections.IndexDirectionDescending
	}
	return collections.IndexDefinition{Name: name, ValueType: collections.IndexValueBSONOrderedV2, Unique: unique, Components: []collections.IndexComponent{
		{Field: "tenant", Direction: collections.IndexDirectionAscending}, {Field: field, Direction: direction},
	}}
}

func nativeBSONCompoundDoc(t *testing.T, id, tenant string, rank any, alias string, value ...any) []byte {
	t.Helper()
	doc := bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: tenant}, {Key: "rank", Value: rank}, {Key: "alias", Value: alias}}
	if len(value) != 0 {
		doc = append(doc, bson.E{Key: "detail", Value: bson.D{{Key: "value", Value: value[0]}}})
	}
	return nativeBSONDocument(t, doc)
}

func checkNativeBSONCompoundRange(t *testing.T, collection *collections.Collection, index string, opts collections.CompoundIndexRangeOptions, truncated bool, want ...string) {
	t.Helper()
	for pass := 0; pass < 2; pass++ {
		ids, cut, err := collection.FindByCompoundIndexRange(index, opts)
		got := make([]string, len(ids))
		for i, id := range ids {
			got[i] = string(id)
		}
		if err != nil || cut != truncated || !reflect.DeepEqual(got, want) {
			t.Fatalf("compound %s desc=%v got=%q truncated=%v want=%q truncated=%v: %v", index, opts.Desc, got, cut, want, truncated, err)
		}
		for _, id := range ids {
			if len(id) != 0 {
				id[0] ^= 1 // Caller mutation must not corrupt the next seek.
			}
		}
	}
}

func runNativeBSONCompound(t *testing.T, profile Profile, dir string, crash bool) {
	opts := indexedSliceOptions(profile, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var old *collections.CollectionReadView
	t.Cleanup(func() {
		if old != nil {
			_ = old.Close()
		}
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
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "tuples", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{
		nativeBSONCompoundIndex("rank", "rank", true, false), nativeBSONCompoundIndex("alias", "alias", false, true), nativeBSONCompoundIndex("value", "detail.value", false, false),
	}}); err != nil {
		t.Fatalf("ordinary activated BSON compound Create: %v", err)
	}
	collection, err := manager.OpenCollection("tuples")
	if err != nil {
		t.Fatal(err)
	}
	seed := [][]byte{
		nativeBSONCompoundDoc(t, "a", "acme", int32(1), "ua"),
		nativeBSONCompoundDoc(t, "b", "acme", int32(3), "ub", nil),
		nativeBSONCompoundDoc(t, "c", "acme", int32(2), "uc", int32(2)),
		nativeBSONCompoundDoc(t, "d", "acme", int32(3), "ud", int64(2)),
		nativeBSONCompoundDoc(t, "e", "other", int32(4), "ue"),
	}
	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	if _, err = collection.InsertBatch(ids, seed); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 2 {
		t.Fatal("seed physical basis is not actual LSN2")
	}
	old, err = collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkOld := func() {
		t.Helper()
		response, err := old.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != len(seed) {
			t.Fatalf("held BSON cut: %+v %v", response, err)
		}
		for i, row := range response.Results {
			if !row.Found || !bytes.Equal(row.Document, seed[i]) {
				t.Fatalf("held row%d changed: %x %v", i, row.Document, err)
			}
			row.Document[0] ^= 1
		}
	}
	prefix := []bson.RawValue{nativeBSONQuery(t, "acme")}
	query := collections.CompoundIndexRangeOptions{Prefix: prefix, Limit: 16}
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "b", "d", "c", "a")
	query.Desc = true
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "a", "c", "d", "b")
	query.StableDocumentIDTies = true
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "a", "c", "b", "d")
	query.Limit = 3
	checkNativeBSONCompoundRange(t, collection, "rank", query, true, "a", "c", "b")
	query.Limit = 16
	query.DocumentIDLess = func(a, b []byte) bool { return bytes.Compare(a, b) > 0 }
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "a", "c", "d", "b")
	query = collections.CompoundIndexRangeOptions{Prefix: prefix, Lower: collections.IndexRangeBound{Value: nativeBSONQuery(t, int32(2)), Inclusive: true}, Upper: collections.IndexRangeBound{Value: nativeBSONQuery(t, int64(3)), Inclusive: false}, Limit: 16}
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "c")
	for _, desc := range []bool{false, true} {
		query = collections.CompoundIndexRangeOptions{Prefix: prefix, Limit: 16, Desc: desc, StableDocumentIDTies: true}
		want := []string{"a", "b", "c", "d"}
		if desc {
			want = []string{"c", "d", "a", "b"}
		}
		checkNativeBSONCompoundRange(t, collection, "value", query, false, want...)
	}
	updatedA := nativeBSONCompoundDoc(t, "a", "acme", int64(4), "ua")
	if result, err := collection.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return bytes.Clone(updatedA), true, nil }}}); err != nil || len(result) != 1 || !result[0].Modified {
		t.Fatalf("pending compound update=%+v %v", result, err)
	}
	if matched, modified, err := collection.UpdateBSONSet([]byte("d"), []collections.BSONSetField{{Key: "rank", Value: nativeBSONQuery(t, int64(5))}}); err != nil || !matched || !modified {
		t.Fatalf("nonleading BSONSet matched=%v modified=%v: %v", matched, modified, err)
	}
	if _, err = collection.CreateIndex(nativeBSONCompoundIndex("backfill", "rank", true, false)); err != nil {
		t.Fatal(err)
	}
	query = collections.CompoundIndexRangeOptions{Prefix: prefix, Limit: 16}
	checkNativeBSONCompoundRange(t, collection, "backfill", query, false, "d", "a", "b", "c")
	for _, desc := range []bool{false, true} {
		inspected := -1
		got, truncated, err := collection.FindByCompoundIndexRange("rank", collections.CompoundIndexRangeOptions{Prefix: prefix, Limit: 16, Desc: desc, MaxInspected: 2, Inspected: &inspected})
		if err != nil || !truncated || inspected < 0 || inspected > 2 || len(got) > 2 {
			t.Fatalf("bounded physical scan desc=%v ids=%q inspected=%d truncated=%v: %v", desc, got, inspected, truncated, err)
		}
		got, truncated, err = collection.FindByCompoundIndexRange("value", collections.CompoundIndexRangeOptions{Prefix: prefix, Limit: 16, Desc: desc, StableDocumentIDTies: true, MaxRetainedIDBytes: 1})
		if err != nil || !truncated || len(got) != 0 {
			t.Fatalf("bounded atomic tie desc=%v ids=%q truncated=%v: %v", desc, got, truncated, err)
		}
	}
	sequence := func() uint64 {
		t.Helper()
		capture, err := handle.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		seq := capture.LogicalSequence()
		if err = capture.Close(); err != nil {
			t.Fatal(err)
		}
		return seq
	}
	// Both unique conflict and unsupported array components fail before identity.
	for i, rejected := range [][]byte{
		nativeBSONCompoundDoc(t, "rejected", "acme", int32(9), "ua"),
		nativeBSONCompoundDoc(t, "rejected", "acme", bson.A{int32(9)}, "new"),
	} {
		next, seq := handle.CommandWALNextLSN(), sequence()
		state, ok := handle.StateToken()
		if !ok {
			t.Fatal("missing physical state")
		}
		if _, err = collection.InsertBatch([][]byte{[]byte("rejected")}, [][]byte{rejected}); err == nil || (i == 0 && !collections.IsDuplicateKeyError(err)) {
			t.Fatalf("compound refusal%d: %v", i, err)
		}
		after, ok := handle.StateToken()
		if !ok || after != state || sequence() != seq || handle.CommandWALNextLSN() != next {
			t.Fatal("compound refusal consumed identity or changed a cut")
		}
		if got, err := collection.Get([]byte("rejected")); err != nil || got != nil {
			t.Fatalf("refused row visible=%x: %v", got, err)
		}
	}
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("tuples")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Close() }()
	finalA := nativeBSONCompoundDoc(t, "a", "acme", int64(2), "ub")
	finalB := nativeBSONCompoundDoc(t, "b", "acme", int64(6), "ua", nil)
	if _, err = tx.UpdateBatch([]collections.UpdateBatchItem{
		{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return bytes.Clone(finalA), true, nil }},
		{DocumentID: []byte("b"), Update: func([]byte) ([]byte, bool, error) { return bytes.Clone(finalB), true, nil }},
	}); err != nil {
		t.Fatalf("final unique handoff: %v", err)
	}
	if count, err := tx.DeleteBatch([][]byte{[]byte("c")}); err != nil || count != 1 {
		t.Fatalf("private compound delete=%d: %v", count, err)
	}
	finalC := nativeBSONCompoundDoc(t, "c", "acme", int64(7), "uc", int64(2))
	if err = tx.InsertBatch([][]byte{[]byte("c")}, [][]byte{bytes.Clone(finalC)}); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		got, err := tx.Get([]byte("c"))
		if err != nil || !bytes.Equal(got, finalC) {
			t.Fatalf("owned private BSON=%x: %v", got, err)
		}
		got[0] ^= 1
	}
	checkNativeBSONDocument(t, collection, "c", seed[2])
	if err = tx.SetRaw([]byte("compound/raw"), []byte("one cut")); err != nil {
		t.Fatal(err)
	}
	next, seq := handle.CommandWALNextLSN(), sequence()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if sequence() != seq+1 || (profile != ProfileNoWALFast && handle.CommandWALNextLSN() != next+1) {
		t.Fatal("compound aggregate did not use one logical/canonical successor")
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}
	checkNativeBSONCompoundRange(t, collection, "backfill", query, false, "c", "b", "d", "a")
	if _, err = collection.DropIndex("backfill"); err != nil {
		t.Fatal(err)
	}
	checkNativeBSONCompoundFinal(t, database, collection)
	checkOld()
	if crash {
		if err = handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if applied, accepted := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN()-1; applied != 2 || accepted != 7 {
			t.Fatalf("unapplied physical=%d accepted=%d", applied, accepted)
		}
		t.Log("COMPOUND UNAPPLIED basis=2 accepted=7")
		os.Exit(97)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeBSONCompoundFinal(t, database, collection)
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
		t.Fatalf("original registry: %+v", stats)
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
	collection, err = manager.OpenCollection("tuples")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeBSONCompoundFinal(t, database, collection)
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

func checkNativeBSONCompoundFinal(t *testing.T, database *DB, collection *collections.Collection) {
	t.Helper()
	prefix := []bson.RawValue{nativeBSONQuery(t, "acme")}
	query := collections.CompoundIndexRangeOptions{Prefix: prefix, Limit: 16}
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "c", "b", "d", "a")
	query.Desc = true
	checkNativeBSONCompoundRange(t, collection, "rank", query, false, "a", "d", "b", "c")
	checkNativeBSONCompoundRange(t, collection, "rank", collections.CompoundIndexRangeOptions{Limit: 16}, false, "c", "b", "d", "a", "e")
	checkNativeBSONCompoundRange(t, collection, "rank", collections.CompoundIndexRangeOptions{Limit: 16, Desc: true}, false, "e", "a", "d", "b", "c")
	query = collections.CompoundIndexRangeOptions{Prefix: []bson.RawValue{nativeBSONQuery(t, "acme"), nativeBSONQuery(t, "ua")}, Limit: 16}
	checkNativeBSONCompoundRange(t, collection, "alias", query, false, "b")
	query.Prefix[1] = nativeBSONQuery(t, "ub")
	checkNativeBSONCompoundRange(t, collection, "alias", query, false, "a")
	if ids, truncated, err := collection.FindByCompoundIndexRange("backfill", collections.CompoundIndexRangeOptions{Limit: 16}); err != nil || truncated || len(ids) != 0 {
		t.Fatalf("dropped compound index=%q truncated=%v: %v", ids, truncated, err)
	}
	checkNativeBSONDocument(t, collection, "a", nativeBSONCompoundDoc(t, "a", "acme", int64(2), "ub"))
	checkNativeBSONDocument(t, collection, "b", nativeBSONCompoundDoc(t, "b", "acme", int64(6), "ua", nil))
	checkNativeBSONDocument(t, collection, "c", nativeBSONCompoundDoc(t, "c", "acme", int64(7), "uc", int64(2)))
	// BSONSet preserves the original BSON field order and untouched int64 value.
	checkNativeBSONDocument(t, collection, "d", nativeBSONCompoundDoc(t, "d", "acme", int64(5), "ud", int64(2)))
	checkNativeBSONDocument(t, collection, "e", nativeBSONCompoundDoc(t, "e", "other", int32(4), "ue"))
	if got, err := database.Get([]byte("compound/raw")); err != nil || !bytes.Equal(got, []byte("one cut")) {
		t.Fatalf("aggregate raw=%q: %v", got, err)
	}
}

// A scalar BSON multikey index still uses the compound range API. Each ID must
// count once even when its matching values span several logical tie groups.
func TestNativeBSONMultikeyCompoundRangeUniqueIDs(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
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
	_, err = manager.CreateCollection(&collections.CollectionMeta{Name: "multikey_ranges", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "tags", Field: "tags", ValueType: collections.IndexValueBSONOrderedV2, MultiKey: true}}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("multikey_ranges")
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	docs := make([][]byte, len(ids))
	for i, tags := range []bson.A{{int32(1), int32(2), int32(4)}, {int32(1), int32(3), int32(5)}, {int32(2), int32(6)}} {
		docs[i] = nativeBSONDocument(t, bson.D{{Key: "_id", Value: string(ids[i])}, {Key: "tags", Value: tags}})
	}
	if _, err = col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		for _, stable := range []bool{false, true} {
			for _, desc := range []bool{false, true} {
				want := []string{"a", "b", "c"}
				if desc {
					want = []string{"c", "b", "a"}
				}
				for _, limit := range []int{2, 3, 10} {
					query := collections.CompoundIndexRangeOptions{Limit: limit, Desc: desc, StableDocumentIDTies: stable}
					checkNativeBSONCompoundRange(t, col, "tags", query, limit < 3, want[:min(limit, 3)]...)
				}
				query := collections.CompoundIndexRangeOptions{Limit: 10, Desc: desc, StableDocumentIDTies: stable, MaxRetainedIDBytes: 1}
				got, truncated, err := col.FindByCompoundIndexRange("tags", query)
				if err != nil || !truncated || len(got) != 0 {
					t.Fatalf("retained cap stable=%v desc=%v: ids=%q truncated=%v err=%v", stable, desc, got, truncated, err)
				}
			}
		}
	}
	check() // Genuine pending secondary roots.
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check() // Exact physical roots.
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
		t.Fatalf("registry=%+v", stats)
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
	col, err = manager.OpenCollection("multikey_ranges")
	if err != nil {
		t.Fatal(err)
	}
	check()
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
		t.Fatalf("reopened registry=%+v", stats)
	}
}
