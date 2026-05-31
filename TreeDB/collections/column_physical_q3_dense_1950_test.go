package collections

import (
	"fmt"
	"testing"
)

func TestColumnPhysicalQ3DenseTypedColumnDirectPreparedParity1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	d, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	codesByGeneration := typedColumnQ1DictionaryCodeByGeneration1950(t, d, col, "collection", "app.bsky.feed.post")
	if len(codesByGeneration) < 2 {
		t.Fatalf("post collection dictionary codes by generation=%v want at least two", codesByGeneration)
	}
	seenCodes := make(map[int64]struct{}, len(codesByGeneration))
	for _, code := range codesByGeneration {
		seenCodes[code] = struct{}{}
	}
	if len(seenCodes) < 2 {
		t.Fatalf("post collection local dictionary codes=%v want differing local dictionary orders", codesByGeneration)
	}

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	assertColumnPhysicalJSONBenchRowScanPreservesFullPredicateColumnsP0(t, events, scanned)
	req := columnPhysicalQ3DenseRequest1950()
	want := columnPhysicalQ3DenseReferenceGroups1950(scanned)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", scanned)

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q3 dense): %v", err)
	}
	assertColumnPhysicalQ3DenseResult1950(t, "direct", direct, want, len(events), matchedRows)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q3 dense): %v", err)
	}
	defer func() { _ = runner.Close() }()
	for run := 0; run < 2; run++ {
		prepared, err := runner.Run()
		if err != nil {
			t.Fatalf("prepared q3 dense run %d: %v", run, err)
		}
		assertColumnPhysicalQ3DenseResult1950(t, fmt.Sprintf("prepared run %d", run), prepared, want, len(events), matchedRows)
	}

	noPredicateReq := req
	noPredicateReq.Predicates = nil
	noPredicate, err := col.RunColumnPhysicalQuery(noPredicateReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q3 dense no predicates): %v", err)
	}
	if !noPredicate.Diagnostics.DenseGroupHourCountUsed || noPredicate.Diagnostics.RowsScanned != len(events) || noPredicate.Diagnostics.RowsMatched != 0 || noPredicate.Diagnostics.ReduceRows != len(events) {
		t.Fatalf("no-predicate diagnostics=%+v want dense group-hour with RowsMatched=0 and ReduceRows=%d", noPredicate.Diagnostics, len(events))
	}
}

func BenchmarkColumnPhysicalQ3DenseTypedColumn1950(b *testing.B) {
	events := columnPhysicalQ3DenseBenchmarkEvents1950(16_384)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, [][]columnPhysicalJSONBenchParityEventP0{events})
	defer closeFn()
	req := columnPhysicalQ3DenseRequest1950()
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", events)

	b.Run("direct_RunColumnPhysicalQuery", func(b *testing.B) {
		preview, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
		}
		assertColumnPhysicalQ3DenseDiagnostics1950(b, "preview direct", preview.Diagnostics, len(events), matchedRows, len(preview.Groups))
		b.SetBytes(int64(preview.Diagnostics.DecodedPayloadBytes))
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		var groups int
		for i := 0; i < b.N; i++ {
			result, err := col.RunColumnPhysicalQuery(req)
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
		reportColumnPhysicalQ3DenseBenchMetrics1950(b, last)
	})

	b.Run("prepared_runner_Run", func(b *testing.B) {
		runner, err := col.PrepareColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
		}
		defer func() { _ = runner.Close() }()
		preview, err := runner.Run()
		if err != nil {
			b.Fatalf("preview runner.Run: %v", err)
		}
		assertColumnPhysicalQ3DenseDiagnostics1950(b, "preview prepared", preview.Diagnostics, len(events), matchedRows, len(preview.Groups))
		b.SetBytes(int64(preview.Diagnostics.DecodedPayloadBytes))
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		var groups int
		for i := 0; i < b.N; i++ {
			result, err := runner.Run()
			if err != nil {
				b.Fatalf("runner.Run: %v", err)
			}
			groups += len(result.Groups)
			last = result.Diagnostics
		}
		b.StopTimer()
		if groups == 0 {
			b.Fatal("benchmark produced no groups")
		}
		reportColumnPhysicalQ3DenseBenchMetrics1950(b, last)
	})
}

func columnPhysicalQ3DenseRequest1950() ColumnPhysicalQueryRequest {
	return ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupHourCount,
		GroupColumn: "collection",
		ValueColumn: "time_us",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "collection", Kind: ColumnPhysicalQueryPredicateInList, Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}},
		},
	}
}

func columnPhysicalQ3DenseBatchA1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_910_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "a-post-1", TimeUS: base + 1*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:q3:a"},
		{ID: "a-like-1", TimeUS: base + 2*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:q3:b"},
		{ID: "a-post-2", TimeUS: base + 1*columnPhysicalQueryHourUS + 7, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:q3:c"},
		{ID: "a-repost-1", TimeUS: base + 3*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.repost", Did: "did:q3:d"},
		{ID: "a-kind-guard", TimeUS: base + 4*columnPhysicalQueryHourUS, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:q3:e"},
	}
}

func columnPhysicalQ3DenseBatchB1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_910_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "b-sort-guard", TimeUS: base, Kind: "identity", Operation: "create", Collection: "app.aaa.guard", Did: "did:q3:guard"},
		{ID: "b-post-1", TimeUS: base + 5*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:q3:f"},
		{ID: "b-like-1", TimeUS: base + 2*columnPhysicalQueryHourUS + 9, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:q3:g"},
		{ID: "b-graph-guard", TimeUS: base + 6*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.graph.follow", Did: "did:q3:h"},
		{ID: "b-operation-guard", TimeUS: base + 7*columnPhysicalQueryHourUS, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.repost", Did: "did:q3:i"},
	}
}

func columnPhysicalQ3DenseBenchmarkEvents1950(rows int) []columnPhysicalJSONBenchParityEventP0 {
	collections := []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like", "app.bsky.graph.follow"}
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		kind := "commit"
		operation := "create"
		if i%19 == 0 {
			kind = "identity"
		}
		if i%31 == 0 {
			operation = "delete"
		}
		events[i] = columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("q3-bench-%06d", i),
			TimeUS:     1_960_000_000_000_000 + int64(i%72)*columnPhysicalQueryHourUS + int64(i%997),
			Kind:       kind,
			Operation:  operation,
			Collection: collections[i%len(collections)],
			Did:        fmt.Sprintf("did:q3:%04d", i%2048),
		}
	}
	return events
}

func columnPhysicalQ3DenseReferenceGroups1950(events []columnPhysicalJSONBenchParityEventP0) []ColumnPhysicalQueryGroup {
	type key struct {
		collection string
		hour       int
	}
	counts := make(map[key]int)
	for _, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q3", event) {
			continue
		}
		counts[key{collection: event.Collection, hour: columnPhysicalQueryUTCHour(event.TimeUS)}]++
	}
	groups := make([]ColumnPhysicalQueryGroup, 0, len(counts))
	for key, count := range counts {
		groups = append(groups, ColumnPhysicalQueryGroup{Key: key.collection, Hour: key.hour, Count: count})
	}
	sortColumnPhysicalQueryGroupsByKeyHour(groups)
	return groups
}

func assertColumnPhysicalQ3DenseResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, want []ColumnPhysicalQueryGroup, rows, matchedRows int) {
	tb.Helper()
	if len(result.Groups) != len(want) {
		tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
	}
	for i := range want {
		if result.Groups[i].Key != want[i].Key || result.Groups[i].Hour != want[i].Hour || result.Groups[i].Count != want[i].Count {
			tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
		}
	}
	assertColumnPhysicalQ3DenseDiagnostics1950(tb, label, result.Diagnostics, rows, matchedRows, len(want))
}

func assertColumnPhysicalQ3DenseDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, rows, matchedRows, resultGroups int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics storage/fallback=%+v", label, diag)
	}
	if !diag.DenseGroupHourCountUsed {
		tb.Fatalf("%s diagnostics did not mark dense group-hour use: %+v", label, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialized rows/documents: %+v", label, diag)
	}
	if diag.RowsScanned != rows || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s rows scanned/matched/reduced=%d/%d/%d want %d/%d diagnostics=%+v", label, diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, rows, matchedRows, diag)
	}
	if diag.PredicateCount != 3 || diag.PredicateLiterals != 5 {
		tb.Fatalf("%s predicate diagnostics=%+v want two equal predicates plus three-value IN", label, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s missing typed-column section/decode diagnostics: %+v", label, diag)
	}
	if diag.ResultGroups != resultGroups {
		tb.Fatalf("%s result groups diagnostics=%+v want %d", label, diag, resultGroups)
	}
}

func reportColumnPhysicalQ3DenseBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(diag.ReduceRows), "reduce_rows/op")
	b.ReportMetric(float64(diag.DecodedPayloadBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPartSections), "typed_sections/op")
}
