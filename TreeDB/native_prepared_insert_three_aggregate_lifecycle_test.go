package treedb

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// Preserve the exact schema and prepared document from collections/
// prepared_insert_test.go's TestPreparedInsertThreeAggregateSpecsRemainEligible.
// The original five-column producer includes nested paths and three aggregate
// specs; this proof covers storage and prepared lifecycle, not aggregate queries.
func TestNativePreparedInsertThreeAggregateLifecycle(t *testing.T) {
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
	postPredicates := []collections.ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
		{Column: "event", Value: "app.bsky.feed.post"},
	}
	q3Predicates := append([]collections.ColumnPhysicalQueryPredicate(nil), postPredicates...)
	q3Predicates[2] = collections.ColumnPhysicalQueryPredicate{Column: "event", Kind: collections.ColumnPhysicalQueryPredicateInList,
		Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}}
	meta := &collections.CollectionMeta{Name: "events", Options: collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
		ColumnStore: &collections.ColumnStoreConfig{
			Enabled: true,
			Columns: []collections.ColumnStoreColumn{
				{Name: "event", Path: "commit.collection", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
				{Name: "did", Path: "did", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
				{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
				{Name: "operation", Path: "commit.operation", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
				{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
			},
			SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
			AggregateMetadata: []collections.ColumnAggregateMetadata{
				{Name: "q1", GroupColumn: "event", Kind: collections.ColumnAggregateCount},
				{Name: "q3", Column: "time_us", GroupColumn: "event", Kind: collections.ColumnAggregateGroupHourCount, Predicates: q3Predicates},
				{Name: "q5", Column: "time_us", GroupColumn: "did", Kind: collections.ColumnAggregateMin, Predicates: postPredicates},
			},
			RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
			RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingSemanticStreamV1,
			Reconstruction:          collections.ColumnReconstructionRetainedPayloadAndColumns,
		},
	}}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("maintained three-aggregate prepared producer: native CreateCollection: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	seed := []byte(`{"commit":{"collection":"app.bsky.feed.like","operation":"create","residual":"old"},"did":"did:seed","kind":"commit","time_us":7,"extra":{"old":true}}`)
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
	ids := [][]byte{[]byte("a")}
	docs := [][]byte{
		[]byte(`{"commit":{"collection":"app.bsky.feed.post","operation":"create"},"did":"did:one","kind":"commit","time_us":123}`),
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
		old, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), wantIDs[0]}, collections.DocumentFetchOptions{})
		if err != nil || len(old.Results) != 2 || !old.Results[0].Found || old.Results[1].Found {
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
