package collections

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnPhysicalQ1DenseLatestVisibleMutation1953(t *testing.T) {
	batches := columnPhysicalQ1DenseEventBatches1950([][]string{
		{"app.m", "app.z", "app.m", "app.z"},
		{"app.a", "app.m", "app.a", "app.m"},
	})
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer closeFn()

	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)
	updateID := events[0].ID
	deleteID := events[len(events)-1].ID
	updated := latest[updateID]
	updated.Collection = "app.updated"
	updated.TimeUS += 77
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, deleteID)
	delete(latest, deleteID)

	want := collectionCounts1953(latestEvents1953(latest))
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q1 latest-visible): %v", err)
	}
	if got := columnPhysicalGroupCountMap1953(result.Groups); !reflect.DeepEqual(got, want) {
		t.Fatalf("q1 latest-visible groups=%v want %v full=%+v", got, want, result.Groups)
	}
	assertLatestVisibleDenseDiagnostics1953(t, "q1", result.Diagnostics, len(latest), -1, "q1")
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ3DenseLatestVisibleMutationReopen1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	promoteID := pickEventID1953(t, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
		return event.Kind == "identity" && event.Operation == "create" && event.Collection == "app.bsky.feed.post"
	})
	promoted := latest[promoteID]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.post"
	promoted.TimeUS += 11
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	demoteID := pickEventID1953(t, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
		return event.ID != promoteID && columnPhysicalJSONBenchReferenceMatchP0("q3", event) && event.Collection == "app.bsky.feed.post"
	})
	demoted := latest[demoteID]
	demoted.Operation = "delete"
	demoted.TimeUS += 13
	updateTypedColumnEvent1953(t, col, demoted)
	latest[demoted.ID] = demoted

	deleteID := pickEventID1953(t, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
		return event.ID != promoteID && event.ID != demoteID && columnPhysicalJSONBenchReferenceMatchP0("q3", event)
	})
	deleteTypedColumnEvent1953(t, col, deleteID)
	delete(latest, deleteID)

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	defer closeFn()

	live := latestEvents1953(latest)
	want := columnPhysicalQ3DenseReferenceGroups1950(live)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", live)
	req := columnPhysicalQ3DenseRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q3 latest-visible): %v", err)
	}
	if !reflect.DeepEqual(result.Groups, want) {
		t.Fatalf("q3 latest-visible groups=%+v want %+v", result.Groups, want)
	}
	assertLatestVisibleDenseDiagnostics1953(t, "q3", result.Diagnostics, len(live), matchedRows, "q3")
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ5DenseLatestVisibleMutation1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer closeFn()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	promoteID := pickEventID1953(t, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
		return event.Kind == "identity" && event.Collection == "app.bsky.feed.post"
	})
	promoted := latest[promoteID]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.post"
	promoted.Did = "did:beta"
	promoted.TimeUS += 500
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	moveID := pickEventID1953(t, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
		return event.ID != promoteID && event.Kind == "commit" && event.Operation == "create" && event.Collection != "app.bsky.feed.post"
	})
	moved := latest[moveID]
	moved.Collection = "app.bsky.feed.post"
	moved.Did = "did:epsilon"
	moved.TimeUS += 300
	updateTypedColumnEvent1953(t, col, moved)
	latest[moved.ID] = moved

	deleteID := pickEventID1953(t, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
		return event.ID != promoteID && event.ID != moveID && columnPhysicalJSONBenchReferenceMatchP0("q5", event)
	})
	deleteTypedColumnEvent1953(t, col, deleteID)
	delete(latest, deleteID)

	live := latestEvents1953(latest)
	req := columnPhysicalQ5DenseRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	want := columnPhysicalQ5DenseReferenceGroups1950(live, req.TopK)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", live)
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q5 latest-visible): %v", err)
	}
	if !reflect.DeepEqual(result.Groups, want) {
		t.Fatalf("q5 latest-visible groups=%+v want %+v", result.Groups, want)
	}
	assertLatestVisibleDenseDiagnostics1953(t, "q5", result.Diagnostics, len(live), matchedRows, "q5")
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalNonDenseNoPredicateMutationUsesTypedLatestVisible1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{{
		{ID: "a1", TimeUS: 100, Kind: "commit", Operation: "create", Collection: "app.test", Did: "did:a"},
		{ID: "b1", TimeUS: 200, Kind: "commit", Operation: "create", Collection: "app.test", Did: "did:b"},
		{ID: "c1", TimeUS: 50, Kind: "commit", Operation: "create", Collection: "app.test", Did: "did:c"},
	}, {
		{ID: "a2", TimeUS: 90, Kind: "commit", Operation: "create", Collection: "app.test", Did: "did:a"},
		{ID: "b2", TimeUS: 210, Kind: "commit", Operation: "create", Collection: "app.test", Did: "did:b"},
	}}
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer closeFn()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)
	updated := latest["a1"]
	updated.TimeUS = 80
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, "c1")
	delete(latest, "c1")

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"}
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(non-dense no-predicate latest-visible): %v", err)
	}
	if got, want := columnPhysicalGroupInt64Map1953(result.Groups), didMinTimes1953(latestEvents1953(latest)); !reflect.DeepEqual(got, want) {
		t.Fatalf("non-dense visibility overlay groups=%v want %v full=%+v", got, want, result.Groups)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone {
		t.Fatalf("non-dense diagnostics=%+v want typed-column latest-visible path", result.Diagnostics)
	}
	if result.Diagnostics.MutationParts == 0 || result.Diagnostics.VisibilityRows != len(latest) || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.DocumentMaterializations != 0 {
		t.Fatalf("non-dense visibility diagnostics=%+v want latest-visible typed-column merge without document materialization", result.Diagnostics)
	}
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ2NoSortLatestVisibleNoPredicateUsesGeneric1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{{
		{ID: "a1", TimeUS: 100, Kind: "commit", Operation: "create", Collection: "app.alpha", Did: "did:one"},
		{ID: "a2", TimeUS: 110, Kind: "commit", Operation: "create", Collection: "app.alpha", Did: "did:two"},
		{ID: "b1", TimeUS: 120, Kind: "commit", Operation: "create", Collection: "app.beta", Did: "did:one"},
	}, {
		{ID: "c1", TimeUS: 130, Kind: "identity", Operation: "update", Collection: "app.gamma", Did: "did:three"},
		{ID: "a3", TimeUS: 140, Kind: "commit", Operation: "delete", Collection: "app.alpha", Did: "did:one"},
	}}
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer closeFn()
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	updated := latest["a1"]
	updated.Collection = "app.beta"
	updated.Did = "did:two"
	updated.TimeUS += 77
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, "c1")
	delete(latest, "c1")

	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
	}
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 no-sort no-predicate latest-visible): %v diagnostics=%+v", err, result.Diagnostics)
	}
	live := latestEvents1953(latest)
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	want := collectionDidCountDistinct1953(live)
	if !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 no-sort latest-visible counts=%v want %v groups=%+v", got, want, result.Groups)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone {
		t.Fatalf("q2 no-sort diagnostics=%+v want typed-column latest-visible path", result.Diagnostics)
	}
	if result.Diagnostics.DenseGroupCountDistinctUsed || result.Diagnostics.SortedGroupedDistinctUsed {
		t.Fatalf("q2 no-sort diagnostics=%+v want generic latest-visible count-distinct path", result.Diagnostics)
	}
	if result.Diagnostics.MutationParts == 0 || result.Diagnostics.VisibilityRows != len(live) || result.Diagnostics.DeletedRows == 0 {
		t.Fatalf("q2 no-sort visibility diagnostics=%+v want mutation parts, visible=%d, deleted rows", result.Diagnostics, len(live))
	}
	if result.Diagnostics.PredicateCount != 0 || result.Diagnostics.RowsMatched != 0 || result.Diagnostics.ReduceRows != len(live) {
		t.Fatalf("q2 no-sort reduce diagnostics=%+v want no predicates and reduce rows=%d", result.Diagnostics, len(live))
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.DocumentMaterializations != 0 {
		t.Fatalf("q2 no-sort materialization diagnostics=%+v want no row/document materialization", result.Diagnostics)
	}
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ2SortedLatestVisibleMutationReopen1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)

	promoted := latest["a-kind-guard"]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.like"
	promoted.Did = "did:promoted"
	promoted.TimeUS += 101
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	demoted := latest["a-post-a-1"]
	demoted.Operation = "delete"
	demoted.TimeUS += 102
	updateTypedColumnEvent1953(t, col, demoted)
	latest[demoted.ID] = demoted

	moved := latest["a-post-shared"]
	moved.Collection = "app.bsky.feed.like"
	moved.TimeUS += 103
	updateTypedColumnEvent1953(t, col, moved)
	latest[moved.ID] = moved

	deleteTypedColumnEvent1953(t, col, "b-repost")
	delete(latest, "b-repost")

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	defer closeFn()

	live := latestEvents1953(latest)
	req := typedColumnQ2Request1950()
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 sorted latest-visible): %v diagnostics=%+v", err, result.Diagnostics)
	}
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	want := columnPhysicalJSONBenchQ2ReferenceCountsP0(live)
	if !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 latest-visible counts=%v want %v groups=%+v", got, want, result.Groups)
	}
	if _, ok := got[""]; ok {
		t.Fatalf("q2 latest-visible produced sentinel empty collection group: %v", got)
	}
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", live)
	assertLatestVisibleSortedQ2Diagnostics1953(t, result.Diagnostics, len(live), matchedRows)
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ4BTopKSortedLatestVisibleMutation1953(t *testing.T) {
	events := typedColumnQ4BTopKTieBreakEvents1950()
	batches := splitColumnPhysicalEvents1953(events)
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, typedColumnQ4BTopKClickHouseSortKey1950(), batches)
	defer closeFn()
	latest := latestEventMap1953(events)

	moved := latest["post-0"]
	moved.Did = "did:post:aaa"
	moved.TimeUS = 1_999_990
	updateTypedColumnEvent1953(t, col, moved)
	latest[moved.ID] = moved

	demoted := latest["post-1"]
	demoted.Collection = "app.bsky.feed.like"
	demoted.TimeUS += 201
	updateTypedColumnEvent1953(t, col, demoted)
	latest[demoted.ID] = demoted

	promoted := latest["identity-0"]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.post"
	promoted.Did = "did:post:tie-z"
	promoted.TimeUS = moved.TimeUS
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	deleteTypedColumnEvent1953(t, col, "post-2")
	delete(latest, "post-2")

	live := latestEvents1953(latest)
	req := typedColumnQ4BTopKRequest1950()
	want := typedColumnQ4BTopKReferenceGroups1950(live, req.TopK)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q4b", live)
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q4b sorted latest-visible): %v diagnostics=%+v", err, result.Diagnostics)
	}
	assertTypedColumnQ4BTopKGroups1950(t, "latest-visible q4b", result.Groups, want)
	assertLatestVisibleQ4BTopKDiagnostics1953(t, result.Diagnostics, len(live), matchedRows, req.TopK, len(want), typedColumnQ4BTopKReferenceCandidateCount1950(live))
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ4ATimeOrderLatestVisibleMutationReopen1953(t *testing.T) {
	const base = int64(1_930_000_000_000_000)
	batches := [][]columnPhysicalJSONBenchParityEventP0{
		{
			{ID: "move-new", TimeUS: base + 100, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:new"},
			{ID: "move-old", TimeUS: base + 6, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:old"},
			{ID: "promote-predicate", TimeUS: base + 7, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:promote"},
			{ID: "demote-predicate", TimeUS: base + 9, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:demote"},
		},
		{
			{ID: "tie-b", TimeUS: base + 8, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:tie:b"},
			{ID: "tie-a", TimeUS: base + 8, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:tie:a"},
			{ID: "delete-me", TimeUS: base + 4, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:delete"},
			{ID: "stable", TimeUS: base + 12, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:stable"},
		},
	}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, []ColumnSortKey{{Column: "time_us"}}, batches)
	events := flattenColumnPhysicalEvents1950(batches)
	latest := latestEventMap1953(events)
	updateRows := 0

	movedNew := latest["move-new"]
	movedNew.TimeUS = base + 5
	updateTypedColumnEvent1953(t, col, movedNew)
	updateRows++
	latest[movedNew.ID] = movedNew

	movedOld := latest["move-old"]
	movedOld.TimeUS = base + 1_000
	updateTypedColumnEvent1953(t, col, movedOld)
	updateRows++
	latest[movedOld.ID] = movedOld

	promoted := latest["promote-predicate"]
	promoted.Kind = "commit"
	promoted.TimeUS = base + 8
	updateTypedColumnEvent1953(t, col, promoted)
	updateRows++
	latest[promoted.ID] = promoted

	demoted := latest["demote-predicate"]
	demoted.Operation = "delete"
	demoted.TimeUS = base + 3
	updateTypedColumnEvent1953(t, col, demoted)
	updateRows++
	latest[demoted.ID] = demoted

	deleteTypedColumnEvent1953(t, col, "delete-me")
	delete(latest, "delete-me")

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	defer closeFn()

	live := latestEvents1953(latest)
	req := columnPhysicalQ4ATimeOrderRequest1950()
	want := columnPhysicalQ4ATimeOrderReferenceGroups1950(live, req.TopK)
	wantExact := []ColumnPhysicalQueryGroup{
		{Key: "did:new", Int64: base + 5},
		{Key: "did:promote", Int64: base + 8},
		{Key: "did:tie:a", Int64: base + 8},
	}
	if !reflect.DeepEqual(want, wantExact) {
		t.Fatalf("q4a latest-visible reference=%+v want exact %+v", want, wantExact)
	}
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q4a", live)
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q4a time-order latest-visible): %v diagnostics=%+v", err, result.Diagnostics)
	}
	assertTypedColumnQ4BTopKGroups1950(t, "latest-visible q4a", result.Groups, want)
	assertLatestVisibleQ4ATimeOrderDiagnostics1953(t, result.Diagnostics, len(live), matchedRows, req.TopK, len(want), columnPhysicalQ4AMatchingGroupCandidateCount1953(live), len(events)+updateRows)
	assertPreparedMutationFailsClosed1953(t, col, req)
}

func TestColumnPhysicalQ4ATimeOrderMutationUnsupportedSortKey1953(t *testing.T) {
	batches := columnPhysicalQ4ATimeOrderBatches1950(8)
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, []ColumnSortKey{{Column: "did"}}, batches)
	defer closeFn()
	updated := batches[0][0]
	updated.Kind = "commit"
	updated.TimeUS += 19
	updateTypedColumnEvent1953(t, col, updated)

	result, err := col.RunColumnPhysicalQuery(columnPhysicalQ4ATimeOrderRequest1950())
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "mutation visibility") {
		t.Fatalf("q4a unsupported-sort mutation err=%v diagnostics=%+v want explicit mutation visibility fail-closed", err, result.Diagnostics)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackMutationVisibilityUnsupported || result.Diagnostics.MutationParts == 0 || result.Diagnostics.VisibilityRows == 0 {
		t.Fatalf("q4a unsupported-sort mutation diagnostics=%+v want typed-column unsupported visibility", result.Diagnostics)
	}
}

func BenchmarkColumnPhysicalSortedLatestVisible1953(b *testing.B) {
	q2Batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	q4bEvents := typedColumnQ4BTopKTieBreakEvents1950()
	q4bEventsByID := eventsByID1953(q4bEvents)
	q4bBatches := splitColumnPhysicalEvents1953(q4bEvents)
	q4aBatches := columnPhysicalQ4ATimeOrderBatches1950(1024)
	q4aEventsByID := eventsByID1953(flattenColumnPhysicalEvents1950(q4aBatches))
	cases := []struct {
		name    string
		sortKey []ColumnSortKey
		batches [][]columnPhysicalJSONBenchParityEventP0
		req     ColumnPhysicalQueryRequest
		mutate  func(testing.TB, *Collection)
	}{
		{
			name:    "q2_insert_only",
			sortKey: typedColumnQ2ClickHouseSortKey1950(),
			batches: q2Batches,
			req:     typedColumnQ2Request1950(),
		},
		{
			name:    "q2_latest_visible",
			sortKey: typedColumnQ2ClickHouseSortKey1950(),
			batches: q2Batches,
			req:     typedColumnQ2Request1950(),
			mutate: func(tb testing.TB, col *Collection) {
				updated := typedColumnQ2SortedBatchA1950()[0]
				updated.Collection = "app.bsky.feed.like"
				updated.TimeUS += 10
				updateTypedColumnEvent1953(tb, col, updated)
				deleteTypedColumnEvent1953(tb, col, "b-repost")
			},
		},
		{
			name:    "q4b_insert_only",
			sortKey: typedColumnQ4BTopKClickHouseSortKey1950(),
			batches: q4bBatches,
			req:     typedColumnQ4BTopKRequest1950(),
		},
		{
			name:    "q4b_latest_visible",
			sortKey: typedColumnQ4BTopKClickHouseSortKey1950(),
			batches: q4bBatches,
			req:     typedColumnQ4BTopKRequest1950(),
			mutate: func(tb testing.TB, col *Collection) {
				updated := q4bEventsByID["post-0"]
				updated.Did = "did:post:aaa"
				updated.TimeUS = 1_999_990
				updateTypedColumnEvent1953(tb, col, updated)
				deleteTypedColumnEvent1953(tb, col, "post-2")
			},
		},
		{
			name:    "q4a_insert_only",
			sortKey: []ColumnSortKey{{Column: "time_us"}},
			batches: q4aBatches,
			req:     columnPhysicalQ4ATimeOrderRequest1950(),
		},
		{
			name:    "q4a_latest_visible",
			sortKey: []ColumnSortKey{{Column: "time_us"}},
			batches: q4aBatches,
			req:     columnPhysicalQ4ATimeOrderRequest1950(),
			mutate: func(tb testing.TB, col *Collection) {
				updated := q4aEventsByID["a-m"]
				updated.TimeUS -= 25
				updateTypedColumnEvent1953(tb, col, updated)
				promoted := q4aEventsByID["a-kind-guard"]
				promoted.Kind = "commit"
				promoted.TimeUS += 3
				updateTypedColumnEvent1953(tb, col, promoted)
				deleteTypedColumnEvent1953(tb, col, "b-b")
			},
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(b, tc.sortKey, tc.batches)
			defer closeFn()
			if tc.mutate != nil {
				tc.mutate(b, col)
			}
			preview, err := col.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			var groups int
			for i := 0; i < b.N; i++ {
				result, err := col.RunColumnPhysicalQuery(tc.req)
				if err != nil {
					b.Fatalf("RunColumnPhysicalQuery: %v", err)
				}
				groups += len(result.Groups)
				last = result.Diagnostics
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no groups")
			}
			b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
			b.ReportMetric(float64(last.RowsMatched), "rows_matched/op")
			b.ReportMetric(float64(last.ReduceRows), "reduce_rows/op")
			b.ReportMetric(float64(last.VisibilityRows), "visibility_rows/op")
			b.ReportMetric(float64(last.DeletedRows), "deleted_rows/op")
			b.ReportMetric(float64(last.TypedColumnPartSections), "typed_sections/op")
			b.ReportMetric(float64(last.SortKeyMarkChecks), "mark_checks/op")
			b.ReportMetric(float64(last.SortKeyMarkSkips), "mark_skips/op")
			b.ReportMetric(float64(last.TopKCandidates), "topk_candidates/op")
			if last.TimeOrderTopKUsed {
				b.ReportMetric(1, "time_order_topk_used/op")
			} else {
				b.ReportMetric(0, "time_order_topk_used/op")
			}
		})
	}
}

func BenchmarkColumnPhysicalDenseLatestVisible1953(b *testing.B) {
	cases := []struct {
		name    string
		batches [][]columnPhysicalJSONBenchParityEventP0
		req     ColumnPhysicalQueryRequest
		mutate  func(testing.TB, *Collection, [][]columnPhysicalJSONBenchParityEventP0)
	}{
		{
			name:    "q1_insert_only",
			batches: columnPhysicalQ1DenseEventBatches1950([][]string{{"app.m", "app.z", "app.m", "app.z"}, {"app.a", "app.m", "app.a", "app.m"}}),
			req:     ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"},
		},
		{
			name:    "q1_latest_visible",
			batches: columnPhysicalQ1DenseEventBatches1950([][]string{{"app.m", "app.z", "app.m", "app.z"}, {"app.a", "app.m", "app.a", "app.m"}}),
			req:     ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"},
			mutate: func(tb testing.TB, col *Collection, batches [][]columnPhysicalJSONBenchParityEventP0) {
				events := flattenColumnPhysicalEvents1950(batches)
				updated := events[0]
				updated.Collection = "app.updated"
				updateTypedColumnEvent1953(tb, col, updated)
				deleteTypedColumnEvent1953(tb, col, events[len(events)-1].ID)
			},
		},
		{
			name:    "q3_insert_only",
			batches: [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBenchmarkEvents1950(2048)},
			req:     columnPhysicalQ3DenseRequest1950(),
		},
		{
			name:    "q3_latest_visible",
			batches: [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBenchmarkEvents1950(2048)},
			req:     columnPhysicalQ3DenseRequest1950(),
			mutate: func(tb testing.TB, col *Collection, batches [][]columnPhysicalJSONBenchParityEventP0) {
				events := flattenColumnPhysicalEvents1950(batches)
				byID := eventsByID1953(events)
				updateID := pickEventID1953(tb, events, func(event columnPhysicalJSONBenchParityEventP0) bool { return event.Kind == "identity" })
				updated := byID[updateID]
				updated.Kind = "commit"
				updated.Operation = "create"
				updated.Collection = "app.bsky.feed.post"
				updateTypedColumnEvent1953(tb, col, updated)
				deleteID := pickEventID1953(tb, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
					return event.ID != updateID && columnPhysicalJSONBenchReferenceMatchP0("q3", event)
				})
				deleteTypedColumnEvent1953(tb, col, deleteID)
			},
		},
		{
			name:    "q5_insert_only",
			batches: [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBenchmarkEvents1950(2048)},
			req:     columnPhysicalQ5DenseRequest1950(),
		},
		{
			name:    "q5_latest_visible",
			batches: [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBenchmarkEvents1950(2048)},
			req:     columnPhysicalQ5DenseRequest1950(),
			mutate: func(tb testing.TB, col *Collection, batches [][]columnPhysicalJSONBenchParityEventP0) {
				events := flattenColumnPhysicalEvents1950(batches)
				byID := eventsByID1953(events)
				updateID := pickEventID1953(tb, events, func(event columnPhysicalJSONBenchParityEventP0) bool { return event.Kind == "identity" })
				updated := byID[updateID]
				updated.Kind = "commit"
				updated.Operation = "create"
				updated.Collection = "app.bsky.feed.post"
				updated.TimeUS += 999_999
				updateTypedColumnEvent1953(tb, col, updated)
				deleteID := pickEventID1953(tb, events, func(event columnPhysicalJSONBenchParityEventP0) bool {
					return event.ID != updateID && columnPhysicalJSONBenchReferenceMatchP0("q5", event)
				})
				deleteTypedColumnEvent1953(tb, col, deleteID)
			},
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(b, nil, tc.batches)
			defer closeFn()
			if tc.mutate != nil {
				tc.mutate(b, col, tc.batches)
			}
			preview, err := col.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			var groups int
			for i := 0; i < b.N; i++ {
				result, err := col.RunColumnPhysicalQuery(tc.req)
				if err != nil {
					b.Fatalf("RunColumnPhysicalQuery: %v", err)
				}
				groups += len(result.Groups)
				last = result.Diagnostics
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no groups")
			}
			b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
			b.ReportMetric(float64(last.RowsMatched), "rows_matched/op")
			b.ReportMetric(float64(last.ReduceRows), "reduce_rows/op")
			b.ReportMetric(float64(last.VisibilityRows), "visibility_rows/op")
			b.ReportMetric(float64(last.DeletedRows), "deleted_rows/op")
			b.ReportMetric(float64(last.TypedColumnPartSections), "typed_sections/op")
		})
	}
}

func openTypedColumnLatestVisibleFixture1953(tb testing.TB, sortKey []ColumnSortKey, batches [][]columnPhysicalJSONBenchParityEventP0) (string, *backenddb.DB, *Collection, func()) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: typedColumnSortKeyConfig1948(sortKey)}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	for batchIdx, batch := range batches {
		ids := make([][]byte, len(batch))
		docs := make([][]byte, len(batch))
		for i, event := range batch {
			ids[i] = []byte(event.ID)
			docs[i] = typedColumnEventDocument1953(event)
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch[%d]: %v", batchIdx, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint setup: %v", err)
	}
	return dir, d, col, func() { _ = d.Close() }
}

func checkpointAndReopenTypedColumnLatestVisibleFixture1953(tb testing.TB, dir string, d *backenddb.DB, closeFn func()) (*backenddb.DB, *Collection, func()) {
	tb.Helper()
	if err := d.Checkpoint(); err != nil {
		tb.Fatalf("Checkpoint before reopen: %v", err)
	}
	if closeFn != nil {
		closeFn()
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open reopened DB: %v", err)
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		tb.Fatalf("OpenCollection reopened: %v", err)
	}
	return reopen, reopened, func() { _ = reopen.Close() }
}

func typedColumnEventDocument1953(event columnPhysicalJSONBenchParityEventP0) []byte {
	return []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
}

func updateTypedColumnEvent1953(tb testing.TB, col *Collection, event columnPhysicalJSONBenchParityEventP0) {
	tb.Helper()
	matched, modified, err := col.Update([]byte(event.ID), func([]byte) ([]byte, bool, error) {
		return typedColumnEventDocument1953(event), true, nil
	})
	if err != nil || !modified {
		tb.Fatalf("Update %s matched=%t modified=%t err=%v stats=%+v", event.ID, matched, modified, err, col.LastUpdateStats())
	}
}

func deleteTypedColumnEvent1953(tb testing.TB, col *Collection, id string) {
	tb.Helper()
	deleted, err := col.DeleteBatch([][]byte{[]byte(id)})
	if err != nil || deleted != 1 {
		tb.Fatalf("DeleteBatch %s deleted=%d err=%v", id, deleted, err)
	}
}

func latestEventMap1953(events []columnPhysicalJSONBenchParityEventP0) map[string]columnPhysicalJSONBenchParityEventP0 {
	return eventsByID1953(events)
}

func eventsByID1953(events []columnPhysicalJSONBenchParityEventP0) map[string]columnPhysicalJSONBenchParityEventP0 {
	out := make(map[string]columnPhysicalJSONBenchParityEventP0, len(events))
	for _, event := range events {
		out[event.ID] = event
	}
	return out
}

func pickEventID1953(tb testing.TB, events []columnPhysicalJSONBenchParityEventP0, match func(columnPhysicalJSONBenchParityEventP0) bool) string {
	tb.Helper()
	for _, event := range events {
		if match(event) {
			return event.ID
		}
	}
	tb.Fatalf("no generated fixture event matched predicate")
	return ""
}

func latestEvents1953(latest map[string]columnPhysicalJSONBenchParityEventP0) []columnPhysicalJSONBenchParityEventP0 {
	out := make([]columnPhysicalJSONBenchParityEventP0, 0, len(latest))
	for _, event := range latest {
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func collectionDidCountDistinct1953(events []columnPhysicalJSONBenchParityEventP0) map[string]columnPhysicalJSONBenchQ2CountP0 {
	counts := make(map[string]int64)
	distinct := make(map[string]map[string]struct{})
	for _, event := range events {
		counts[event.Collection]++
		if distinct[event.Collection] == nil {
			distinct[event.Collection] = make(map[string]struct{})
		}
		distinct[event.Collection][event.Did] = struct{}{}
	}
	out := make(map[string]columnPhysicalJSONBenchQ2CountP0, len(counts))
	for collection, count := range counts {
		out[collection] = columnPhysicalJSONBenchQ2CountP0{Count: count, Distinct: int64(len(distinct[collection]))}
	}
	return out
}

func collectionCounts1953(events []columnPhysicalJSONBenchParityEventP0) map[string]int {
	counts := make(map[string]int)
	for _, event := range events {
		counts[event.Collection]++
	}
	return counts
}

func columnPhysicalGroupCountMap1953(groups []ColumnPhysicalQueryGroup) map[string]int {
	out := make(map[string]int, len(groups))
	for _, group := range groups {
		out[group.Key] = group.Count
	}
	return out
}

func columnPhysicalGroupInt64Map1953(groups []ColumnPhysicalQueryGroup) map[string]int64 {
	out := make(map[string]int64, len(groups))
	for _, group := range groups {
		out[group.Key] = group.Int64
	}
	return out
}

func didMinTimes1953(events []columnPhysicalJSONBenchParityEventP0) map[string]int64 {
	out := make(map[string]int64)
	for _, event := range events {
		cur, ok := out[event.Did]
		if !ok || event.TimeUS < cur {
			out[event.Did] = event.TimeUS
		}
	}
	return out
}

func splitColumnPhysicalEvents1953(events []columnPhysicalJSONBenchParityEventP0) [][]columnPhysicalJSONBenchParityEventP0 {
	mid := len(events) / 2
	if mid == 0 {
		return [][]columnPhysicalJSONBenchParityEventP0{events}
	}
	return [][]columnPhysicalJSONBenchParityEventP0{events[:mid], events[mid:]}
}

func assertLatestVisibleSortedQ2Diagnostics1953(tb testing.TB, diag ColumnPhysicalQueryDiagnostics, visibleRows int, matchedRows int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("q2 diagnostics storage/fallback=%+v want latest-visible typed-column path", diag)
	}
	if diag.MutationParts == 0 || diag.VisibilityRows != visibleRows || diag.DeletedRows == 0 {
		tb.Fatalf("q2 visibility diagnostics=%+v want mutation parts, visible=%d, deleted rows", diag, visibleRows)
	}
	if diag.PredicateCount != 2 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("q2 predicate diagnostics=%+v want predicates=2 matched/reduced=%d", diag, matchedRows)
	}
	if !diag.SortedGroupedDistinctUsed || !diag.SortedGroupedDistinctReady || diag.SortedGroupedDistinctFallbackReason != columnSortedGroupedDistinctFallbackNone {
		tb.Fatalf("q2 grouped-distinct diagnostics=%+v want sorted latest-visible grouped distinct", diag)
	}
	if !diag.SortKeyPrefixPlanned || diag.SortKeyPrefixLiterals != 2 || !equalStrings1949(diag.SortKeyPrefixColumns, []string{"kind", "operation"}) || diag.SortKeyMarkChecks == 0 {
		tb.Fatalf("q2 sort diagnostics=%+v want kind/operation sorted-prefix marks", diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedBlocks == 0 || diag.PhysicalBytesScanned == 0 {
		tb.Fatalf("q2 typed-column diagnostics=%+v want section/decode/byte counters", diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.ReconstructionRows != 0 {
		tb.Fatalf("q2 materialization diagnostics=%+v want zero row/document reconstruction", diag)
	}
}

func assertLatestVisibleQ4BTopKDiagnostics1953(tb testing.TB, diag ColumnPhysicalQueryDiagnostics, visibleRows int, matchedRows int, wantTopK int, wantGroups int, wantCandidates int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("q4b diagnostics storage/fallback=%+v want latest-visible typed-column path", diag)
	}
	if diag.MutationParts == 0 || diag.VisibilityRows != visibleRows || diag.DeletedRows == 0 {
		tb.Fatalf("q4b visibility diagnostics=%+v want mutation parts, visible=%d, deleted rows", diag, visibleRows)
	}
	if diag.PredicateCount != 3 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("q4b predicate diagnostics=%+v want predicates=3 matched/reduced=%d", diag, matchedRows)
	}
	if !diag.SortKeyPrefixPlanned || diag.SortKeyPrefixLiterals != 3 || !equalStrings1949(diag.SortKeyPrefixColumns, []string{"kind", "operation", "collection"}) {
		tb.Fatalf("q4b prefix diagnostics=%+v want kind/operation/collection sorted prefix", diag)
	}
	if diag.SortKeyMarkChecks == 0 || diag.SortKeyMarkSkips == 0 || diag.SkippedGranules == 0 {
		tb.Fatalf("q4b mark diagnostics=%+v want checked and skipped sort-key marks", diag)
	}
	if diag.RowsScanned <= 0 || diag.RowsScanned >= visibleRows || diag.DecodedPayloadBytes == 0 {
		tb.Fatalf("q4b scan diagnostics=%+v want mark-pruned typed-column section decode", diag)
	}
	if diag.TopKLimit != wantTopK || diag.TopKOrder != string(ColumnPhysicalQueryTopKInt64Asc) || diag.TopKCandidates != wantCandidates || diag.ResultGroups != wantGroups {
		tb.Fatalf("q4b topK diagnostics=%+v want limit/order/candidates/result groups", diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.ReconstructionRows != 0 {
		tb.Fatalf("q4b materialization diagnostics=%+v want zero row/document reconstruction", diag)
	}
}

func assertLatestVisibleQ4ATimeOrderDiagnostics1953(tb testing.TB, diag ColumnPhysicalQueryDiagnostics, visibleRows int, matchedRows int, wantTopK int, wantGroups int, wantCandidates int, wantRowsScanned int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("q4a diagnostics storage/fallback=%+v want latest-visible typed-column path", diag)
	}
	if diag.MutationParts == 0 || diag.VisibilityRows != visibleRows || diag.DeletedRows == 0 {
		tb.Fatalf("q4a visibility diagnostics=%+v want mutation parts, visible=%d, deleted rows", diag, visibleRows)
	}
	if !diag.TimeOrderTopKUsed {
		tb.Fatalf("q4a diagnostics=%+v want time-order topK latest-visible path", diag)
	}
	if diag.PredicateCount != 3 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("q4a predicate diagnostics=%+v want predicates=3 matched/reduced=%d", diag, matchedRows)
	}
	if diag.RowsScanned != wantRowsScanned || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("q4a scan diagnostics=%+v want full multipart latest-visible typed-column decode over %d non-tombstone candidates", diag, wantRowsScanned)
	}
	if diag.SortKeyMarkChecks == 0 || diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.PhysicalBytesScanned == 0 {
		tb.Fatalf("q4a typed-column diagnostics=%+v want sorted typed-column section counters", diag)
	}
	if diag.TopKLimit != wantTopK || diag.TopKOrder != string(ColumnPhysicalQueryTopKInt64Asc) || diag.TopKCandidates != wantCandidates || diag.ResultGroups != wantGroups {
		tb.Fatalf("q4a topK diagnostics=%+v want limit=%d candidates=%d groups=%d", diag, wantTopK, wantCandidates, wantGroups)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.ReconstructionRows != 0 {
		tb.Fatalf("q4a materialization diagnostics=%+v want zero row/document reconstruction", diag)
	}
}

func columnPhysicalQ4AMatchingGroupCandidateCount1953(events []columnPhysicalJSONBenchParityEventP0) int {
	seen := make(map[string]struct{})
	for _, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q4a", event) {
			continue
		}
		seen[event.Did] = struct{}{}
	}
	return len(seen)
}

func assertLatestVisibleDenseDiagnostics1953(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, visibleRows int, matchedRows int, shape string) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics storage/fallback=%+v want latest-visible typed-column path", label, diag)
	}
	if diag.MutationParts == 0 || diag.VisibilityRows != visibleRows || diag.DeletedRows == 0 {
		tb.Fatalf("%s visibility diagnostics=%+v want mutation parts, visible=%d, deleted rows", label, diag, visibleRows)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.ReconstructionRows != 0 {
		tb.Fatalf("%s materialization diagnostics=%+v want zero row/document reconstruction", label, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedBlocks == 0 || diag.PhysicalBytesScanned == 0 {
		tb.Fatalf("%s typed-column diagnostics=%+v want section/decode/byte counters", label, diag)
	}
	if diag.RowsScanned < visibleRows {
		tb.Fatalf("%s rows scanned=%d visible=%d diagnostics=%+v", label, diag.RowsScanned, visibleRows, diag)
	}
	switch shape {
	case "q1":
		if !diag.DenseGroupCountUsed || diag.RowsMatched != 0 || diag.ReduceRows != visibleRows || diag.PredicateCount != 0 {
			tb.Fatalf("%s q1 diagnostics=%+v want dense group-count over visible rows", label, diag)
		}
	case "q3":
		if !diag.DenseGroupHourCountUsed || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows || diag.PredicateCount != 3 {
			tb.Fatalf("%s q3 diagnostics=%+v want dense grouped-hour matched=%d", label, diag, matchedRows)
		}
	case "q5":
		if !diag.DenseInt64SpanUsed || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows || diag.PredicateCount != 3 || diag.TopKLimit == 0 || diag.TopKCandidates == 0 {
			tb.Fatalf("%s q5 diagnostics=%+v want dense int64-span topK matched=%d", label, diag, matchedRows)
		}
	default:
		tb.Fatalf("unknown shape %q", shape)
	}
}

func assertSortedMutationUnsupported1953(tb testing.TB, label string, col *Collection, req ColumnPhysicalQueryRequest) {
	tb.Helper()
	result, err := col.RunColumnPhysicalQuery(req)
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "mutation visibility") {
		tb.Fatalf("%s sorted mutation err=%v diagnostics=%+v want mutation visibility fail-closed", label, err, result.Diagnostics)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackMutationVisibilityUnsupported || result.Diagnostics.VisibilityRows == 0 || result.Diagnostics.MutationParts == 0 {
		tb.Fatalf("%s sorted mutation diagnostics=%+v want explicit typed-column unsupported visibility", label, result.Diagnostics)
	}
}

func assertPreparedMutationFailsClosed1953(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest) {
	tb.Helper()
	_, err := col.PrepareColumnPhysicalQuery(req)
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "insert-only") || !strings.Contains(err.Error(), "lifecycle") {
		tb.Fatalf("PrepareColumnPhysicalQuery mutation err=%v want insert-only/lifecycle fail-closed", err)
	}
}
