package treedb

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// Dropping predeclared typed indexes is an existing public operation. This
// does not extend CreateIndex to declared NonColumn retained-payload fields.
func TestNativeTypedStringIndexDropLifecycle(t *testing.T) {
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
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
	fixture.typedSeed = &collections.TypedColumnBatch{}
	fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	meta := nativeColumnPendingMeta(fixture)
	meta.Indexes = []collections.IndexDefinition{
		{Name: "label_lookup", Field: "label", ValueType: collections.IndexValueString},
		{Name: "label_unique", Field: "label", ValueType: collections.IndexValueString, Unique: true},
		{Name: "label_keep", Field: "label", ValueType: collections.IndexValueString},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("create predeclared typed indexes: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := nativeTypedOpsRows("string")
	insert := func(rows ...nativeTypedOpsRow) {
		t.Helper()
		ids, residual, columns := nativeTypedOpsInput("string", rows...)
		got, stats, err := col.InsertTypedBatchWithStats(ids, residual, columns)
		if err != nil || len(got) != len(rows) || stats.Documents != len(rows) {
			t.Fatalf("typed insert: ids=%q stats=%+v err=%v", got, stats, err)
		}
		ids[0][0], residual[0][0], columns[0].Strings[0] = '!', '!', "caller-owned input"
	}
	replace := func(row nativeTypedOpsRow) {
		t.Helper()
		ids, residual, columns := nativeTypedOpsInput("string", row)
		got, err := col.ReplaceTypedBatch(ids, residual, columns)
		if err != nil || len(got) != 1 || !got[0].Matched || !got[0].Modified {
			t.Fatalf("typed replacement after drop: results=%+v err=%v", got, err)
		}
	}
	checkIndex := func(name, value string, want ...string) {
		t.Helper()
		ids, err := col.FindByIndex(name, value)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, 0, len(ids))
		for _, id := range ids {
			got = append(got, string(id))
		}
		sort.Strings(got)
		sort.Strings(want)
		if len(got) != len(want) || len(want) != 0 && !reflect.DeepEqual(got, want) {
			t.Fatalf("index %s value=%q ids=%q want=%q", name, value, got, want)
		}
	}
	checkMeta := func(want ...string) {
		t.Helper()
		sort.Strings(want)
		for _, opened := range []*collections.Collection{col, peer} {
			for _, current := range []collections.CollectionMeta{opened.Meta(), opened.MetaView()} {
				got := make([]string, 0, len(current.Indexes))
				for _, index := range current.Indexes {
					got = append(got, index.Name)
				}
				sort.Strings(got)
				if len(got) != len(want) || len(want) != 0 && !reflect.DeepEqual(got, want) {
					t.Fatalf("current typed index metadata=%q want=%q", got, want)
				}
			}
		}
	}
	insert(a, b)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		for attempt := 0; attempt < 2; attempt++ {
			got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("c")}, collections.DocumentFetchOptions{})
			if err != nil || len(got.Results) != 3 {
				t.Fatalf("held typed documents: results=%d err=%v", len(got.Results), err)
			}
			for i, row := range []nativeTypedOpsRow{a, b} {
				if !got.Results[i].Found {
					t.Fatalf("held document %s missing", row.id)
				}
				checkNativeTypedOpsDocument(t, "string", got.Results[i].Document, row)
				got.Results[i].Document[0] = '!'
			}
			if got.Results[2].Found {
				t.Fatal("held cut contains later insert")
			}
		}
		for _, name := range []string{"label_lookup", "label_unique", "label_keep"} {
			var got []string
			if err := held.VisitIndexValueIDs(name, a.text, func(id []byte) error {
				got = append(got, string(id)) // The callback ID is borrowed.
				return nil
			}); err != nil || !reflect.DeepEqual(got, []string{"a"}) {
				t.Fatalf("held index %s ids=%q err=%v", name, got, err)
			}
		}
	}
	insert(c) // Drop must preserve surviving pending as well as persisted domains.
	next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	if _, err := col.DropIndex("label_lookup"); err != nil {
		t.Fatalf("ordinary typed DropIndex: %v", err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkMeta("label_unique", "label_keep")
	checkIndex("label_lookup", a.text) // Public Collection queries return empty for missing indexes.
	checkIndex("label_keep", c.text, "c")
	checkIndex("label_unique", c.text, "c")
	afterA := nativeTypedOpsChanged(a)
	replace(afterA)
	checkIndex("label_keep", a.text)
	checkIndex("label_unique", afterA.text, "a")
	checkNativeTypedOpsCurrent(t, col, "string", []nativeTypedOpsRow{afterA, b, c}, []string{"d"})
	checkHeld()
	conflict := c
	conflict.id, conflict.text = "d", afterA.text
	conflict.payload, conflict.retained = "new-d", []byte(`{"id":"d", "payload":"new-d"}`)
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	state, ok := handle.StateToken()
	if !ok {
		t.Fatal("missing physical state before unique refusal")
	}
	ids, residual, columns := nativeTypedOpsInput("string", conflict)
	if _, _, err := col.InsertTypedBatchWithStats(ids, residual, columns); !errors.Is(err, collections.ErrUniqueIndexConflict) {
		t.Fatalf("surviving unique index refusal: %v", err)
	}
	after, ok := handle.StateToken()
	if !ok || after != state || handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
		t.Fatal("unique refusal changed physical/canonical/logical identity")
	}
	checkNativeTypedOpsCurrent(t, col, "string", []nativeTypedOpsRow{afterA, b, c}, []string{"d"})
	if _, err := col.DropIndexes([]string{"label_unique"}); err != nil {
		t.Fatal(err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkMeta("label_keep")
	afterB := b
	afterB.text = afterA.text
	replace(afterB)
	checkIndex("label_keep", afterA.text, "a", "b")
	checkIndex("label_unique", afterA.text)
	checkNativeTypedOpsCurrent(t, col, "string", []nativeTypedOpsRow{afterA, afterB, c}, []string{"d"})
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkIndex("label_keep", afterA.text, "a", "b")
	checkHeld()
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	if _, err := col.DropAllIndexes(); err != nil {
		t.Fatal(err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	checkMeta()
	checkIndex("label_keep", afterA.text)
	if count, err := col.DeleteBatch([][]byte{[]byte("c")}); err != nil || count != 1 {
		t.Fatalf("typed delete after final drop: count=%d err=%v", count, err)
	}
	d := conflict
	insert(d) // Duplicate scalar values are legal after the unique index is gone.
	check := func() {
		t.Helper()
		checkNativeTypedOpsCurrent(t, col, "string", []nativeTypedOpsRow{afterA, afterB, d}, []string{"c"})
		checkMeta()
	}
	check()
	accepted := handle.CommandWALNextLSN() - 1
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := indexedCRUDAppliedLSN(t, handle); got != accepted {
		t.Fatalf("drop checkpoint applied=%d want=%d", got, accepted)
	}
	check()
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
		t.Fatalf("typed drop final registry: %+v", stats)
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
	peer = col
	check()
	checkIndex("label_keep", afterA.text)
	if indexedCRUDAppliedLSN(t, handle) != accepted || handle.CommandWALNextLSN() != accepted+1 {
		t.Fatal("reopen changed applied/canonical identity")
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
		t.Fatalf("typed drop reopen final registry: %+v", stats)
	}
}
