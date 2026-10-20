package treedb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// The maintained typed API supports scalar string indexes on declared String
// RowAsset columns. Exercise that actual producer and its physical readback,
// rather than placing a second full document in the residual JSON.
func TestNativeTypedStringIndexesLifecycle(t *testing.T) {
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
	fixture.typedSeed = &collections.TypedColumnBatch{} // Existing non-column JSON carrier.
	fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	meta := nativeColumnPendingMeta(fixture)
	meta.Indexes = []collections.IndexDefinition{
		{Name: "label_lookup", Field: "label", ValueType: collections.IndexValueString},
		{Name: "label_unique", Field: "label", ValueType: collections.IndexValueString, Unique: true},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("ordinary native typed indexed Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := nativeTypedOpsRows("string")
	check := func(rows []nativeTypedOpsRow, absent []string) {
		t.Helper()
		checkNativeTypedOpsCurrent(t, col, "string", rows, absent)
		for _, row := range rows {
			for _, index := range []string{"label_lookup", "label_unique"} {
				for attempt := 0; attempt < 2; attempt++ {
					ids, err := col.FindByIndex(index, row.text)
					if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], []byte(row.id)) {
						t.Fatalf("%s/%q owners=%q want=%q err=%v", index, row.text, ids, row.id, err)
					}
					ids[0][0] = '!' // Returned IDs cannot alias retained index state.
				}
			}
		}
	}
	next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	ids, residual, columns := nativeTypedOpsInput("string", a, b)
	inserted, stats, err := col.InsertTypedBatchWithStats(ids, residual, columns)
	if err != nil || len(inserted) != 2 || stats.Documents != 2 {
		t.Fatalf("actual typed indexed insert: ids=%q stats=%+v err=%v", inserted, stats, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	ids[0][0], residual[0][0], columns[0].Strings[0] = '!', '!', "caller overwrote input"
	check([]nativeTypedOpsRow{a, b}, []string{"c"})
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check([]nativeTypedOpsRow{a, b}, []string{"c"})
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		for _, row := range []nativeTypedOpsRow{a, b} {
			got, err := held.Get([]byte(row.id))
			if err != nil {
				t.Fatal(err)
			}
			checkNativeTypedOpsDocument(t, "string", got, row)
			got[0] = '!'
		}
		if got, err := held.Get([]byte("c")); err != nil || got != nil {
			t.Fatalf("held cut contains later row: %s err=%v", got, err)
		}
	}
	// Admission must validate the final unique owners, allowing an atomic swap.
	afterA, afterB := a, b
	afterA.text, afterB.text = b.text, a.text
	ids, residual, columns = nativeTypedOpsInput("string", afterA, afterB)
	next, seq = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	results, err := col.ReplaceTypedBatch(ids, residual, columns)
	if err != nil || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("typed unique handoff: results=%+v err=%v", results, err)
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, seq)
	ids[0][0], residual[0][0], columns[0].Strings[0] = '!', '!', "caller overwrote replacement"
	check([]nativeTypedOpsRow{afterA, afterB}, []string{"c"})
	checkHeld()
	refuse := func(label string, operation func() error) {
		t.Helper()
		next, seq := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		state, ok := handle.StateToken()
		if !ok {
			t.Fatal("missing physical basis before refusal")
		}
		if err := operation(); !errors.Is(err, collections.ErrUniqueIndexConflict) {
			t.Fatalf("%s unique refusal: %v", label, err)
		}
		after, ok := handle.StateToken()
		if !ok || after != state || handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != seq {
			t.Fatalf("%s refusal changed physical/canonical/logical identity", label)
		}
		check([]nativeTypedOpsRow{afterA, afterB}, []string{"c"})
		checkHeld()
	}
	conflict := c
	conflict.text = afterA.text
	refuse("insert", func() error {
		ids, residual, columns := nativeTypedOpsInput("string", conflict)
		_, _, err := col.InsertTypedBatchWithStats(ids, residual, columns)
		return err
	})
	conflict = afterB
	conflict.text = afterA.text
	refuse("replace", func() error {
		ids, residual, columns := nativeTypedOpsInput("string", conflict)
		_, err := col.ReplaceTypedBatch(ids, residual, columns)
		return err
	})
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check([]nativeTypedOpsRow{afterA, afterB}, []string{"c"})
	checkHeld()
	// Deleting an owner must remove both secondary memberships and allow reuse.
	if count, err := col.DeleteBatch([][]byte{[]byte("b")}); err != nil || count != 1 {
		t.Fatalf("delete typed indexed owner: count=%d err=%v", count, err)
	}
	for _, index := range []string{"label_lookup", "label_unique"} {
		if ids, err := col.FindByIndex(index, afterB.text); err != nil || len(ids) != 0 {
			t.Fatalf("deleted %s owner=%q err=%v", index, ids, err)
		}
	}
	c.text = afterB.text
	ids, residual, columns = nativeTypedOpsInput("string", c)
	if inserted, _, err := col.InsertTypedBatchWithStats(ids, residual, columns); err != nil || len(inserted) != 1 {
		t.Fatalf("reuse deleted unique value: ids=%q err=%v", inserted, err)
	}
	check([]nativeTypedOpsRow{afterA, c}, []string{"b"})
	checkHeld()
	accepted := handle.CommandWALNextLSN() - 1
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := indexedCRUDAppliedLSN(t, handle); got != accepted {
		t.Fatalf("checkpoint applied=%d want=%d", got, accepted)
	}
	check([]nativeTypedOpsRow{afterA, c}, []string{"b"})
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
		t.Fatalf("typed indexed final registry: %+v", stats)
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
	check([]nativeTypedOpsRow{afterA, c}, []string{"b"})
	if indexedCRUDAppliedLSN(t, handle) != accepted || handle.CommandWALNextLSN() != accepted+1 {
		t.Fatal("reopen changed canonical/applied identity")
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
		t.Fatalf("typed indexed reopen final registry: %+v", stats)
	}
}
