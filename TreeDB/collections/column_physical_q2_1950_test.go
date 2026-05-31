package collections

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedColumnQ2SortedGroupedDistinctStreaming1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer closeFn()

	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	wantCounts := map[string]columnPhysicalJSONBenchQ2CountP0{
		"app.bsky.feed.like":    {Count: 3, Distinct: 2},
		"app.bsky.feed.post":    {Count: 4, Distinct: 2},
		"app.bsky.feed.repost":  {Count: 1, Distinct: 1},
		"app.bsky.graph.follow": {Count: 1, Distinct: 1},
	}
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events)
	req := typedColumnQ2Request1950()

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2): %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "direct", direct, rowHash, wantCounts)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "direct", direct.Diagnostics, len(events), matchedRows, true)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q2): %v", err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared q2: %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "prepared", prepared, rowHash, wantCounts)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "prepared", prepared.Diagnostics, len(events), matchedRows, true)
}

func TestTypedColumnQ2MutationVisibilityLatestVisibleC3(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer closeFn()

	req := typedColumnQ2Request1950()
	insertOnly, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("insert-only RunColumnPhysicalQuery(q2): %v", err)
	}
	if insertOnly.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || !insertOnly.Diagnostics.SortedGroupedDistinctUsed || insertOnly.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone {
		t.Fatalf("insert-only diagnostics=%+v want optimized typed-column q2 path", insertOnly.Diagnostics)
	}

	updated := typedColumnQ2SortedBatchA1950()[0]
	updated.Collection = "app.bsky.feed.like"
	updated.TimeUS += 10
	updateTypedColumnEvent1953(t, col, updated)

	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("mutation q2 RunColumnPhysicalQuery: %v diagnostics=%+v", err, result.Diagnostics)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone || !result.Diagnostics.SortedGroupedDistinctUsed {
		t.Fatalf("mutation q2 diagnostics=%+v want latest-visible sorted grouped-distinct path", result.Diagnostics)
	}
	if result.Diagnostics.MutationParts == 0 || result.Diagnostics.VisibilityRows == 0 || result.Diagnostics.DocumentMaterializations != 0 || result.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("mutation q2 diagnostics=%+v want latest-visible physical state without document fallback", result.Diagnostics)
	}
	if _, err := col.PrepareColumnPhysicalQuery(req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "insert-only") {
		t.Fatalf("prepared mutation q2 err=%v want fail-closed prepared insert-only guard", err)
	}
}

func TestTypedColumnQ2SortedGroupedDistinctFallback1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	result, err := col.RunColumnPhysicalQuery(typedColumnQ2Request1950())
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 fallback): %v", err)
	}
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "fallback", result, rowHash, columnPhysicalJSONBenchQ2ReferenceCountsP0(events))
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "fallback", result.Diagnostics, len(events), columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events), false)
	if result.Diagnostics.SortedGroupedDistinctFallbackReason == columnSortedGroupedDistinctFallbackNone {
		t.Fatalf("fallback diagnostics=%+v want explicit sorted grouped-distinct fallback reason", result.Diagnostics)
	}
}

func BenchmarkTypedColumnQ2SortedGroupedDistinct1950(b *testing.B) {
	events := typedColumnQ2BenchmarkEvents1950(65_536)
	cases := []struct {
		name     string
		sortKey  []ColumnSortKey
		prepared bool
		wantUsed bool
	}{
		{name: "direct/sorted_prefix", sortKey: typedColumnQ2ClickHouseSortKey1950(), wantUsed: true},
		{name: "prepared/sorted_prefix", sortKey: typedColumnQ2ClickHouseSortKey1950(), prepared: true, wantUsed: true},
		{name: "direct/primary_id_fallback", sortKey: nil},
		{name: "prepared/primary_id_fallback", sortKey: nil, prepared: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, tc.sortKey, [][]columnPhysicalJSONBenchParityEventP0{events})
			defer closeFn()
			req := typedColumnQ2Request1950()
			preview, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			if preview.Diagnostics.SortedGroupedDistinctUsed != tc.wantUsed {
				b.Fatalf("preview sorted grouped-distinct used=%t want %t diagnostics=%+v", preview.Diagnostics.SortedGroupedDistinctUsed, tc.wantUsed, preview.Diagnostics)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			var runner *ColumnPhysicalQueryRunner
			if tc.prepared {
				runner, err = col.PrepareColumnPhysicalQuery(req)
				if err != nil {
					b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
				}
				defer func() { _ = runner.Close() }()
			}
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			var groups int
			for i := 0; i < b.N; i++ {
				var result ColumnPhysicalQueryResult
				if tc.prepared {
					result, err = runner.Run()
				} else {
					result, err = col.RunColumnPhysicalQuery(req)
				}
				if err != nil {
					b.Fatalf("q2 run: %v", err)
				}
				last = result.Diagnostics
				groups += len(result.Groups)
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no result groups")
			}
			b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
			b.ReportMetric(float64(last.RowsMatched), "rows_matched/op")
			b.ReportMetric(float64(last.ReduceRows), "reduce_rows/op")
			b.ReportMetric(float64(last.SortKeyMarkChecks), "mark_checks/op")
			b.ReportMetric(float64(last.SortKeyMarkSkips), "mark_skips/op")
			b.ReportMetric(float64(last.DecodedPayloadBytes), "decoded_bytes/op")
			if last.ScanNanos > 0 {
				b.ReportMetric(1e9/float64(last.ScanNanos), "diag_ops_per_sec")
				b.ReportMetric(float64(last.RowsScanned)*1e9/float64(last.ScanNanos), "diag_rows_per_sec")
			}
		})
	}
}

func typedColumnQ2Request1950() ColumnPhysicalQueryRequest {
	return ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
}

func typedColumnQ2ClickHouseSortKey1950() []ColumnSortKey {
	return []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
}

func typedColumnQ2SortedBatchA1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_800_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "a-post-shared", TimeUS: base + 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:shared"},
		{ID: "a-like-shared", TimeUS: base + 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:shared"},
		{ID: "a-post-a-1", TimeUS: base + 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:post-a"},
		{ID: "a-post-a-2", TimeUS: base + 21, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:post-a"},
		{ID: "a-kind-guard", TimeUS: base + 22, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:kind-guard"},
		{ID: "a-op-guard", TimeUS: base + 23, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.like", Did: "did:op-guard"},
	}
}

func typedColumnQ2SortedBatchB1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_800_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "b-post-shared", TimeUS: base + 31, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:shared"},
		{ID: "b-like-b", TimeUS: base + 32, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:like-b"},
		{ID: "b-like-shared", TimeUS: base + 33, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:shared"},
		{ID: "b-repost", TimeUS: base + 34, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.repost", Did: "did:repost"},
		{ID: "b-graph", TimeUS: base + 35, Kind: "commit", Operation: "create", Collection: "app.bsky.graph.follow", Did: "did:graph"},
	}
}

func typedColumnQ2BenchmarkEvents1950(rows int) []columnPhysicalJSONBenchParityEventP0 {
	collections := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.graph.follow"}
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		kind := "commit"
		operation := "create"
		if i%17 == 0 {
			kind = "identity"
		}
		if i%29 == 0 {
			operation = "delete"
		}
		events[i] = columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("bench-%06d", i),
			TimeUS:     1_900_000_000_000_000 + int64(i),
			Kind:       kind,
			Operation:  operation,
			Collection: collections[i%len(collections)],
			Did:        fmt.Sprintf("did:%06d", i%8192),
		}
	}
	return events
}

func flattenColumnPhysicalEvents1950(batches [][]columnPhysicalJSONBenchParityEventP0) []columnPhysicalJSONBenchParityEventP0 {
	var total int
	for _, batch := range batches {
		total += len(batch)
	}
	events := make([]columnPhysicalJSONBenchParityEventP0, 0, total)
	for _, batch := range batches {
		events = append(events, batch...)
	}
	return events
}

func openTypedColumnSortKeyFixtureBatches1950(tb testing.TB, sortKey []ColumnSortKey, batches [][]columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func()) {
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
			docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch[%d]: %v", batchIdx, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint before reopen: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("Close before reopen: %v", err)
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

func assertTypedColumnQ2SortedGroupedDistinctResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, rowHash uint64, want map[string]columnPhysicalJSONBenchQ2CountP0) {
	tb.Helper()
	gotHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q2", result.Groups))
	if gotHash != rowHash {
		tb.Fatalf("%s q2 hash=%016x want row-scan %016x groups=%+v", label, gotHash, rowHash, result.Groups)
	}
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	if !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		tb.Fatalf("%s q2 counts=%v want %v groups=%+v", label, got, want, result.Groups)
	}
}

func assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, totalRows, matchedRows int, wantSortedUsed bool) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics=%+v want typed-column source without storage fallback", label, diag)
	}
	if diag.PredicateCount != 2 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s predicate diagnostics=%+v want predicates=2 matched/reduced=%d", label, diag, matchedRows)
	}
	if diag.RowsScanned <= 0 || diag.RowsScanned > totalRows {
		tb.Fatalf("%s rows scanned diagnostics=%+v want 1..%d", label, diag, totalRows)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialization diagnostics=%+v want no row/document materialization", label, diag)
	}
	if diag.SortedGroupedDistinctUsed != wantSortedUsed {
		tb.Fatalf("%s sorted grouped-distinct used=%t want %t diagnostics=%+v", label, diag.SortedGroupedDistinctUsed, wantSortedUsed, diag)
	}
	if wantSortedUsed {
		if !diag.SortKeyPrefixPlanned || diag.SortKeyPrefixLiterals != 2 || !equalStrings1949(diag.SortKeyPrefixColumns, []string{"kind", "operation"}) {
			tb.Fatalf("%s prefix diagnostics=%+v want kind/operation sorted prefix", label, diag)
		}
		if !diag.SortedGroupedDistinctReady || diag.SortedGroupedDistinctFallbackReason != columnSortedGroupedDistinctFallbackNone {
			tb.Fatalf("%s grouped-distinct diagnostics=%+v want ready/no fallback", label, diag)
		}
		if diag.SortKeyMarkChecks == 0 {
			tb.Fatalf("%s mark diagnostics=%+v want sorted-prefix mark checks", label, diag)
		}
	}
}
