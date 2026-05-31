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

	latest := latestEventMap1953(flattenColumnPhysicalEvents1950(batches))
	updated := latest["doc_00_000000"]
	updated.Collection = "app.updated"
	updated.TimeUS += 77
	updateTypedColumnEvent1953(t, col, updated)
	latest[updated.ID] = updated
	deleteTypedColumnEvent1953(t, col, "doc_01_000001")
	delete(latest, "doc_01_000001")

	want := collectionCounts1953(latestEvents1953(latest))
	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q1 latest-visible): %v", err)
	}
	if got := columnPhysicalGroupCountMap1953(result.Groups); !reflect.DeepEqual(got, want) {
		t.Fatalf("q1 latest-visible groups=%v want %v full=%+v", got, want, result.Groups)
	}
	assertLatestVisibleDenseDiagnostics1953(t, "q1", result.Diagnostics, len(latest), -1, "q1")
	assertPreparedMutationFailsClosed1953(t, col, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
}

func TestColumnPhysicalQ3DenseLatestVisibleMutationReopen1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
	dir, d, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	latest := latestEventMap1953(flattenColumnPhysicalEvents1950(batches))

	promoted := latest["a-kind-guard"]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.post"
	promoted.TimeUS += 11
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	demoted := latest["b-post-1"]
	demoted.Operation = "delete"
	demoted.TimeUS += 13
	updateTypedColumnEvent1953(t, col, demoted)
	latest[demoted.ID] = demoted

	deleteTypedColumnEvent1953(t, col, "a-like-1")
	delete(latest, "a-like-1")

	_, col, closeFn = checkpointAndReopenTypedColumnLatestVisibleFixture1953(t, dir, d, closeFn)
	defer closeFn()

	live := latestEvents1953(latest)
	want := columnPhysicalQ3DenseReferenceGroups1950(live)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", live)
	result, err := col.RunColumnPhysicalQuery(columnPhysicalQ3DenseRequest1950())
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q3 latest-visible): %v", err)
	}
	if !reflect.DeepEqual(result.Groups, want) {
		t.Fatalf("q3 latest-visible groups=%+v want %+v", result.Groups, want)
	}
	assertLatestVisibleDenseDiagnostics1953(t, "q3", result.Diagnostics, len(live), matchedRows, "q3")
	assertPreparedMutationFailsClosed1953(t, col, columnPhysicalQ3DenseRequest1950())
}

func TestColumnPhysicalQ5DenseLatestVisibleMutation1953(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, nil, batches)
	defer closeFn()
	latest := latestEventMap1953(flattenColumnPhysicalEvents1950(batches))

	promoted := latest["a-kind-guard"]
	promoted.Kind = "commit"
	promoted.Operation = "create"
	promoted.Collection = "app.bsky.feed.post"
	promoted.Did = "did:beta"
	promoted.TimeUS += 500
	updateTypedColumnEvent1953(t, col, promoted)
	latest[promoted.ID] = promoted

	moved := latest["a-collection-guard"]
	moved.Collection = "app.bsky.feed.post"
	moved.Did = "did:epsilon"
	moved.TimeUS += 300
	updateTypedColumnEvent1953(t, col, moved)
	latest[moved.ID] = moved

	deleteTypedColumnEvent1953(t, col, "b-m-3")
	delete(latest, "b-m-3")

	live := latestEvents1953(latest)
	req := columnPhysicalQ5DenseRequest1950()
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

func TestColumnPhysicalSortedMutationShapesStillFailClosed1953(t *testing.T) {
	t.Run("q2", func(t *testing.T) {
		batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
		_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, typedColumnQ2ClickHouseSortKey1950(), batches)
		defer closeFn()
		updated := typedColumnQ2SortedBatchA1950()[0]
		updated.Collection = "app.bsky.feed.like"
		updateTypedColumnEvent1953(t, col, updated)
		assertSortedMutationUnsupported1953(t, "q2", col, typedColumnQ2Request1950())
	})

	t.Run("q4b", func(t *testing.T) {
		events := typedColumnQ4BTopKTieBreakEvents1950()
		_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, typedColumnQ4BTopKClickHouseSortKey1950(), [][]columnPhysicalJSONBenchParityEventP0{events})
		defer closeFn()
		updated := events[0]
		updated.TimeUS += 17
		updateTypedColumnEvent1953(t, col, updated)
		assertSortedMutationUnsupported1953(t, "q4b", col, typedColumnQ4BTopKRequest1950())
	})

	t.Run("q4a", func(t *testing.T) {
		batches := columnPhysicalQ4ATimeOrderBatches1950(8)
		_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(t, []ColumnSortKey{{Column: "time_us"}}, batches)
		defer closeFn()
		updated := batches[0][0]
		updated.Kind = "commit"
		updated.TimeUS += 19
		updateTypedColumnEvent1953(t, col, updated)
		assertSortedMutationUnsupported1953(t, "q4a", col, columnPhysicalQ4ATimeOrderRequest1950())
	})
}

func BenchmarkColumnPhysicalDenseLatestVisible1953(b *testing.B) {
	cases := []struct {
		name    string
		batches [][]columnPhysicalJSONBenchParityEventP0
		req     ColumnPhysicalQueryRequest
		mutate  func(testing.TB, *Collection)
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
			mutate: func(tb testing.TB, col *Collection) {
				updateTypedColumnEvent1953(tb, col, columnPhysicalJSONBenchParityEventP0{ID: "doc_00_000000", TimeUS: 1_900_000_000_000_000, Kind: "commit", Operation: "create", Collection: "app.updated", Did: "did:q1:000000"})
				deleteTypedColumnEvent1953(tb, col, "doc_01_000001")
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
			mutate: func(tb testing.TB, col *Collection) {
				updateTypedColumnEvent1953(tb, col, columnPhysicalJSONBenchParityEventP0{ID: "q3-bench-000019", TimeUS: 1_960_000_000_000_000, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:q3:0019"})
				deleteTypedColumnEvent1953(tb, col, "q3-bench-000001")
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
			mutate: func(tb testing.TB, col *Collection) {
				updateTypedColumnEvent1953(tb, col, columnPhysicalJSONBenchParityEventP0{ID: "q5-bench-000023", TimeUS: 1_950_000_000_999_999, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:q5:0023"})
				deleteTypedColumnEvent1953(tb, col, "q5-bench-000001")
			},
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, _, col, closeFn := openTypedColumnLatestVisibleFixture1953(b, nil, tc.batches)
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
	_, modified, err := col.Update([]byte(event.ID), func([]byte) ([]byte, bool, error) {
		return typedColumnEventDocument1953(event), true, nil
	})
	if err != nil || !modified {
		tb.Fatalf("Update %s modified=%t err=%v", event.ID, modified, err)
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
	latest := make(map[string]columnPhysicalJSONBenchParityEventP0, len(events))
	for _, event := range events {
		latest[event.ID] = event
	}
	return latest
}

func latestEvents1953(latest map[string]columnPhysicalJSONBenchParityEventP0) []columnPhysicalJSONBenchParityEventP0 {
	out := make([]columnPhysicalJSONBenchParityEventP0, 0, len(latest))
	for _, event := range latest {
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
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
