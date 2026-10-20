package treedb

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// Preserve the maintained five-column/three-aggregate producer from
// TestPreparedInsertThreeAggregateSpecsRemainEligible. These are physical
// queries: checkpoints are explicit, and old prepared runners retain their
// original physical source while ordinary writes publish a later manifest.
func TestNativeColumnPhysicalQueryPublicLifecycle(t *testing.T) {
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var held *collections.CollectionReadView
	var old []*collections.ColumnPhysicalQueryRunner
	t.Cleanup(func() {
		for _, runner := range old {
			if err := runner.Close(); err != nil {
				t.Error(err)
			}
		}
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
	post := []collections.ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"}, {Column: "operation", Value: "create"},
		{Column: "event", Value: "app.bsky.feed.post"},
	}
	feed := append([]collections.ColumnPhysicalQueryPredicate(nil), post...)
	feed[2] = collections.ColumnPhysicalQueryPredicate{Column: "event", Kind: collections.ColumnPhysicalQueryPredicateInList,
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
				{Name: "q3", Column: "time_us", GroupColumn: "event", Kind: collections.ColumnAggregateGroupHourCount, Predicates: feed},
				{Name: "q5", Column: "time_us", GroupColumn: "did", Kind: collections.ColumnAggregateMin, Predicates: post},
			},
			RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
			RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingSemanticStreamV1,
			Reconstruction:          collections.ColumnReconstructionRetainedPayloadAndColumns,
		},
	}}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("maintained physical-query producer Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	const hour = int64(3600 * 1000000)
	document := func(event, did, kind, operation string, timeUS int64) []byte {
		return []byte(fmt.Sprintf(`{"commit":{"collection":%q,"operation":%q},"did":%q,"kind":%q,"time_us":%d,"residual":{"kept":true}}`, event, operation, did, kind, timeUS))
	}
	seed := [][]byte{
		document("app.bsky.feed.post", "did:a", "commit", "create", hour+11),
		document("app.bsky.feed.post", "did:a", "commit", "create", 2*hour+22),
		document("app.bsky.feed.like", "did:b", "commit", "create", hour+33),
		document("app.bsky.feed.post", "did:c", "account", "create", 3*hour),
	}
	insert := func(ids [][]byte, docs [][]byte) {
		t.Helper()
		next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		got, err := col.InsertBatch(ids, docs)
		if err != nil || !reflect.DeepEqual(got, ids) {
			t.Fatalf("physical-query ordinary insert IDs=%q err=%v", got, err)
		}
		requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
	}
	checkpoint := func() {
		t.Helper()
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if got, want := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN()-1; got != want {
			t.Fatalf("physical query checkpoint applied=%d want=%d", got, want)
		}
	}
	insert([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}, seed)
	checkpoint()
	requests := []collections.ColumnPhysicalQueryRequest{
		{Kind: collections.ColumnPhysicalQueryGroupCount, GroupColumn: "event"},
		{Kind: collections.ColumnPhysicalQueryGroupHourCount, GroupColumn: "event", ValueColumn: "time_us", Predicates: feed},
		{Kind: collections.ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", Predicates: post},
	}
	// Expected results are independent of manifest/asset decoders and reducers.
	initial := [][]collections.ColumnPhysicalQueryGroup{
		{{Key: "app.bsky.feed.like", Count: 1}, {Key: "app.bsky.feed.post", Count: 3}},
		{{Key: "app.bsky.feed.like", Hour: 1, Count: 1}, {Key: "app.bsky.feed.post", Hour: 1, Count: 1}, {Key: "app.bsky.feed.post", Hour: 2, Count: 1}},
		{{Key: "did:a", Int64: hour + 11}},
	}
	final := [][]collections.ColumnPhysicalQueryGroup{
		{{Key: "app.bsky.feed.like", Count: 1}, {Key: "app.bsky.feed.post", Count: 5}, {Key: "app.bsky.feed.repost", Count: 1}},
		{{Key: "app.bsky.feed.like", Hour: 1, Count: 1}, {Key: "app.bsky.feed.post", Hour: 0, Count: 1}, {Key: "app.bsky.feed.post", Hour: 1, Count: 1}, {Key: "app.bsky.feed.post", Hour: 2, Count: 1}, {Key: "app.bsky.feed.repost", Hour: 3, Count: 1}},
		{{Key: "did:a", Int64: 5}},
	}
	var originalGeneration uint64
	check := func(want [][]collections.ColumnPhysicalQueryGroup) {
		t.Helper()
		for i, req := range requests {
			direct, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				t.Fatalf("q%d direct physical query: %v", i, err)
			}
			requireNativePhysicalQueryGroups(t, direct.Groups, want[i])
			if direct.Diagnostics.ManifestRoot == 0 || direct.Diagnostics.ActiveManifestChecksum == 0 || direct.Diagnostics.ActiveManifestChecksum != direct.Diagnostics.RecoveryManifestChecksum {
				t.Fatalf("q%d not recovery-authoritative physical manifest: %+v", i, direct.Diagnostics)
			}
			if originalGeneration == 0 {
				originalGeneration = direct.Diagnostics.ManifestGeneration
			}
			req.AggregateMetadataName = []string{"q1", "q3", "q5"}[i]
			metadata, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				t.Fatalf("%s metadata physical query: %v", req.AggregateMetadataName, err)
			}
			requireNativePhysicalQueryGroups(t, metadata.Groups, want[i])
			if metadata.Diagnostics.StorageSource != collections.ColumnPhysicalQueryStorageSourceAggregateMetadata || metadata.Diagnostics.MetadataHits == 0 {
				t.Fatalf("%s did not consume aggregate assets: %+v", req.AggregateMetadataName, metadata.Diagnostics)
			}
		}
	}
	check(initial)
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	for i, req := range requests {
		runner, err := col.PrepareColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("q%d prepare old physical query: %v", i, err)
		}
		old = append(old, runner)
		result, err := runner.Run()
		if err != nil {
			t.Fatal(err)
		}
		requireNativePhysicalQueryGroups(t, result.Groups, initial[i])
	}
	insert([][]byte{[]byte("e"), []byte("f"), []byte("g")}, [][]byte{
		document("app.bsky.feed.post", "did:a", "commit", "create", 5),
		document("app.bsky.feed.repost", "did:d", "commit", "create", 3*hour+44),
		document("app.bsky.feed.post", "did:b", "commit", "update", 4*hour),
	})
	checkpoint()
	check(final)
	for i, runner := range old {
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("old physical runner after later checkpoint: %v", err)
		}
		requireNativePhysicalQueryGroups(t, result.Groups, initial[i])
		if result.Diagnostics.ManifestGeneration != originalGeneration {
			t.Fatalf("old physical generation=%d want=%d", result.Diagnostics.ManifestGeneration, originalGeneration)
		}
	}
	fetched, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("e")}, collections.DocumentFetchOptions{})
	if err != nil || len(fetched.Results) != 2 || !fetched.Results[0].Found || fetched.Results[1].Found {
		t.Fatalf("held document cut=%+v err=%v", fetched, err)
	}
	requireNativeColumnJSON(t, fetched.Results[0].Document, seed[0])
	for _, runner := range old {
		if err := runner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	old = nil
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	checkpoint()
	closeDatabase := func() {
		t.Helper()
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		cache, pins := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if stats := pins.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("physical query original final pins: %+v", stats)
		}
	}
	closeDatabase()
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
	check(final)
	closeDatabase()
}

func requireNativePhysicalQueryGroups(t *testing.T, got, want []collections.ColumnPhysicalQueryGroup) {
	t.Helper()
	order := func(a, b collections.ColumnPhysicalQueryGroup) int {
		if c := strings.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		if a.Hour < b.Hour {
			return -1
		}
		if a.Hour > b.Hour {
			return 1
		}
		return 0
	}
	actual := append([]collections.ColumnPhysicalQueryGroup(nil), got...)
	expected := append([]collections.ColumnPhysicalQueryGroup(nil), want...)
	slices.SortFunc(actual, order)
	slices.SortFunc(expected, order)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("physical query groups=%+v want=%+v", actual, expected)
	}
}
