package treedb

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// Preserve the maintained producer from
// collections/column_retained_vlog_placement_test.go's
// createColumnRetainedSemanticStreamCollection and the prepared insertion in
// TestPreparedInsertSortedValuesAndReopen. This is the same legacy schema and
// owned-input API, rather than broader BSON/template preparation eligibility.
func TestNativePreparedInsertTwoColumnLifecycle(t *testing.T) {
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	database, err := Open(opts)
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
	meta := &collections.CollectionMeta{Name: "events", Options: collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
		ColumnStore: &collections.ColumnStoreConfig{
			Enabled: true,
			Columns: []collections.ColumnStoreColumn{
				{Name: "row_id", Path: "row_id", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerRowAsset},
				{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart, Dictionary: true},
			},
			RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
			RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingSemanticStreamV1,
			Reconstruction:          collections.ColumnReconstructionRetainedPayloadAndColumns,
		},
	}}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("maintained two-column prepared producer: native CreateCollection: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	seed := []byte(`{"row_id":7,"kind":"seed","extra":{"old":true}}`)
	if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	// These complete, independent backing allocations remain unchanged until
	// Commit returns, as required by PrepareInsertBatchOwned's public contract.
	ids := [][]byte{[]byte("z"), []byte("a"), []byte("m")}
	docs := [][]byte{
		[]byte(`{"row_id":3,"kind":"z","extra":null}`),
		[]byte(`{"row_id":1,"kind":"a"}`),
		[]byte(`{"row_id":2,"kind":"m","extra":{"nested":true}}`),
	}
	wantIDs, wantDocs := make([][]byte, len(ids)), make([][]byte, len(docs))
	for i := range ids {
		wantIDs[i], wantDocs[i] = bytes.Clone(ids[i]), bytes.Clone(docs[i])
	}
	next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	physical := indexedCRUDAppliedLSN(t, handle)
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, nativePreparedRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prepared.Abandon)
	if prepared.OwnedBytes() <= 0 || prepared.ReservedBytes() <= prepared.OwnedBytes() || prepared.ReservedBytes() > nativePreparedRequestLimit {
		t.Fatalf("request bytes owned=%d reserved=%d", prepared.OwnedBytes(), prepared.ReservedBytes())
	}
	for _, id := range wantIDs {
		if got, err := col.Get(id); err != nil || got != nil {
			t.Fatalf("private prepared row %q visible=%s: %v", id, got, err)
		}
	}
	if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || indexedCRUDAppliedLSN(t, handle) != physical {
		t.Fatal("preparation published or assigned an identity")
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || indexedCRUDAppliedLSN(t, handle) != physical {
		t.Fatal("preparation/checkpoint published or assigned a prepared identity")
	}
	for _, id := range wantIDs {
		if got, err := col.Get(id); err != nil || got != nil {
			t.Fatalf("checkpoint exposed private prepared row %q=%s: %v", id, got, err)
		}
	}
	result, err := prepared.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != len(wantIDs) {
		t.Fatalf("result IDs=%q", result)
	}
	for i := range result {
		if !bytes.Equal(result[i], wantIDs[i]) {
			t.Fatalf("result input order %d=%q want=%q", i, result[i], wantIDs[i])
		}
		for j := range ids[i] {
			ids[i][j] = '!'
		}
		for j := range docs[i] {
			docs[i][j] = '!'
		}
		if !bytes.Equal(result[i], wantIDs[i]) {
			t.Fatal("result aliases consumed caller input")
		}
	}
	requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
	if indexedCRUDAppliedLSN(t, handle) != next {
		t.Fatal("prepared Commit acknowledged without exact physical completion")
	}
	check := func() {
		t.Helper()
		got, err := col.Get([]byte("seed"))
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, seed)
		current, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		fresh, fetchErr := current.FetchDocumentsByID(wantIDs, collections.DocumentFetchOptions{})
		closeErr := current.Close()
		if fetchErr != nil || closeErr != nil || len(fresh.Results) != len(wantIDs) {
			t.Fatalf("current complete opening=%+v: %v %v", fresh, fetchErr, closeErr)
		}
		for i, row := range fresh.Results {
			if !row.Found {
				t.Fatalf("current row %q absent", wantIDs[i])
			}
			requireNativeColumnJSON(t, row.Document, wantDocs[i])
		}
	}
	checkHeld := func() {
		t.Helper()
		old, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), wantIDs[0], wantIDs[1], wantIDs[2]}, collections.DocumentFetchOptions{})
		if err != nil || len(old.Results) != 4 || !old.Results[0].Found || old.Results[1].Found || old.Results[2].Found || old.Results[3].Found {
			t.Fatalf("held original opening=%+v: %v", old, err)
		}
		requireNativeColumnJSON(t, old.Results[0].Document, seed)
	}
	check()
	checkHeld()
	if _, err := prepared.Commit(); err == nil {
		t.Fatal("prepared batch committed twice")
	}
	prepared.Abandon()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check()
	checkHeld()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	cache, pins := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("original resources: %+v", s)
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
	check()
	cache, pins = database.cached, handle.ValueLogIdentityPinRegistry()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopened resources: %+v", s)
	}
}
