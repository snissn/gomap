package collections

import (
	"fmt"
	"slices"
	"sort"
	"testing"
)

func TestColumnPhysicalQ5DenseTypedColumnDirectPreparedParity1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	d, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	codesByGeneration := typedColumnQ1DictionaryCodeByGeneration1950(t, d, col, "did", "did:m")
	if len(codesByGeneration) < 2 {
		t.Fatalf("did:m dictionary codes by generation=%v want at least two", codesByGeneration)
	}
	seenCodes := make(map[int64]struct{}, len(codesByGeneration))
	for _, code := range codesByGeneration {
		seenCodes[code] = struct{}{}
	}
	if len(seenCodes) < 2 {
		t.Fatalf("did:m local dictionary codes=%v want differing local dictionary orders", codesByGeneration)
	}

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	assertColumnPhysicalJSONBenchRowScanPreservesFullPredicateColumnsP0(t, events, scanned)
	req := columnPhysicalQ5DenseRequest1950()
	want := columnPhysicalQ5DenseReferenceGroups1950(scanned, req.TopK)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned)

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q5 dense): %v", err)
	}
	assertColumnPhysicalQ5DenseResult1950(t, "direct", direct, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerLocalMap)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q5 dense): %v", err)
	}
	defer func() { _ = runner.Close() }()
	for run := 0; run < 2; run++ {
		prepared, err := runner.Run()
		if err != nil {
			t.Fatalf("prepared q5 dense run %d: %v", run, err)
		}
		assertColumnPhysicalQ5DenseResult1950(t, fmt.Sprintf("prepared run %d", run), prepared, want, len(events), matchedRows, columnTypedColumnDenseInt64SpanReducerGlobalCodes)
	}

	noPredicateReq := req
	noPredicateReq.Predicates = nil
	noPredicateReq.TopK = 0
	noPredicateReq.TopKOrder = ""
	noPredicate, err := col.RunColumnPhysicalQuery(noPredicateReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q5 dense no predicates): %v", err)
	}
	if !noPredicate.Diagnostics.DenseInt64SpanUsed || noPredicate.Diagnostics.DenseInt64SpanReducer != columnTypedColumnDenseInt64SpanReducerLocalMap || noPredicate.Diagnostics.RowsScanned != len(events) || noPredicate.Diagnostics.RowsMatched != 0 || noPredicate.Diagnostics.ReduceRows != len(events) {
		t.Fatalf("no-predicate diagnostics=%+v want dense span with RowsMatched=0 and ReduceRows=%d", noPredicate.Diagnostics, len(events))
	}
}

func TestColumnTypedColumnDenseInt64SpanSingleCodeTriplePreapply3175(t *testing.T) {
	dense := &columnTypedColumnDenseInt64SpanPart{
		Predicates: []columnTypedColumnDensePredicatePart{
			{
				Codes:             []uint32{1, 1, 2, 1, 1, 1},
				Valid:             []bool{true, true, true, true, false, true},
				SingleCode:        1,
				SingleCodeAllowed: true,
			},
			{
				Codes:             []uint32{2, 2, 2, 2, 2, 3},
				Valid:             []bool{true, true, false, true, true, true},
				SingleCode:        2,
				SingleCodeAllowed: true,
			},
			{
				Codes:             []uint32{3, 4, 3, 3, 3, 3},
				SingleCode:        3,
				SingleCodeAllowed: true,
			},
		},
	}
	if !preapplyColumnTypedColumnDenseInt64SpanSingleCodeTriplePredicates(dense, 6) {
		t.Fatal("single-code triple preapply did not handle q5-shaped predicates")
	}
	if want := []uint32{0, 3}; !slices.Equal(dense.PredicateRows, want) {
		t.Fatalf("predicate rows=%v want %v", dense.PredicateRows, want)
	}
}

func TestColumnTypedColumnDenseInt64SpanLocalMapCapacity3175(t *testing.T) {
	tests := []struct {
		name  string
		parts []columnTypedColumnPhysicalQueryPart
		want  int
	}{
		{
			name: "empty uses initial capacity",
			want: columnTypedColumnDenseInt64SpanLocalMapInitialCapacity,
		},
		{
			name: "nil and zero cardinality parts use initial capacity",
			parts: []columnTypedColumnPhysicalQueryPart{
				{},
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{}},
			},
			want: columnTypedColumnDenseInt64SpanLocalMapInitialCapacity,
		},
		{
			name: "small sum keeps initial floor",
			parts: []columnTypedColumnPhysicalQueryPart{
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{Cardinality: 3}},
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{Cardinality: 5}},
			},
			want: columnTypedColumnDenseInt64SpanLocalMapInitialCapacity,
		},
		{
			name: "sums part cardinalities",
			parts: []columnTypedColumnPhysicalQueryPart{
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{Cardinality: 20}},
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{Cardinality: 21}},
			},
			want: 41,
		},
		{
			name: "caps large capacity",
			parts: []columnTypedColumnPhysicalQueryPart{
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{Cardinality: columnTypedColumnDenseInt64SpanLocalMapMaxCapacity - 1}},
				{DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{Cardinality: 2}},
			},
			want: columnTypedColumnDenseInt64SpanLocalMapMaxCapacity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := columnTypedColumnDenseInt64SpanLocalMapCapacity(tc.parts); got != tc.want {
				t.Fatalf("capacity=%d want %d", got, tc.want)
			}
		})
	}
}

func BenchmarkColumnPhysicalQ5DenseTypedColumn1950(b *testing.B) {
	events := columnPhysicalQ5DenseBenchmarkEvents1950(16_384)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, [][]columnPhysicalJSONBenchParityEventP0{events})
	defer closeFn()
	req := columnPhysicalQ5DenseRequest1950()
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", events)

	b.Run("direct_RunColumnPhysicalQuery", func(b *testing.B) {
		preview, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
		}
		assertColumnPhysicalQ5DenseDiagnostics1950(b, "preview direct", preview.Diagnostics, len(events), matchedRows, req.TopK, len(preview.Groups), columnTypedColumnDenseInt64SpanReducerLocalMap)
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
		reportColumnPhysicalQ5DenseBenchMetrics1950(b, last)
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
		assertColumnPhysicalQ5DenseDiagnostics1950(b, "preview prepared", preview.Diagnostics, len(events), matchedRows, req.TopK, len(preview.Groups), columnTypedColumnDenseInt64SpanReducerGlobalCodes)
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
		reportColumnPhysicalQ5DenseBenchMetrics1950(b, last)
	})
}

func columnPhysicalQ5DenseRequest1950() ColumnPhysicalQueryRequest {
	return ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupInt64Span,
		GroupColumn: "did",
		ValueColumn: "time_us",
		TopK:        3,
		TopKOrder:   ColumnPhysicalQueryTopKInt64Desc,
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "collection", Value: "app.bsky.feed.post"},
		},
	}
}

func columnPhysicalQ5DenseBatchA1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_900_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "a-m-1", TimeUS: base + 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
		{ID: "a-beta-1", TimeUS: base + 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:beta"},
		{ID: "a-m-2", TimeUS: base + 90, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
		{ID: "a-gamma-1", TimeUS: base + 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:gamma"},
		{ID: "a-kind-guard", TimeUS: base + 200, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:guard-kind"},
		{ID: "a-collection-guard", TimeUS: base + 210, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:guard-collection"},
	}
}

func columnPhysicalQ5DenseBatchB1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_900_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "b-delta-1", TimeUS: base + 40, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:delta"},
		{ID: "b-m-3", TimeUS: base + 160, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
		{ID: "b-beta-2", TimeUS: base + 120, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:beta"},
		{ID: "b-gamma-2", TimeUS: base + 130, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:gamma"},
		{ID: "b-epsilon-1", TimeUS: base + 70, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:epsilon"},
		{ID: "b-operation-guard", TimeUS: base + 300, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.post", Did: "did:guard-operation"},
	}
}

func columnPhysicalQ5DenseBenchmarkEvents1950(rows int) []columnPhysicalJSONBenchParityEventP0 {
	collections := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"}
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		kind := "commit"
		operation := "create"
		collection := collections[i%len(collections)]
		if i%23 == 0 {
			kind = "identity"
		}
		if i%29 == 0 {
			operation = "delete"
		}
		events[i] = columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("q5-bench-%06d", i),
			TimeUS:     1_950_000_000_000_000 + int64((i*37)%100_000),
			Kind:       kind,
			Operation:  operation,
			Collection: collection,
			Did:        fmt.Sprintf("did:q5:%04d", i%2048),
		}
	}
	return events
}

func columnPhysicalQ5DenseReferenceGroups1950(events []columnPhysicalJSONBenchParityEventP0, topK int) []ColumnPhysicalQueryGroup {
	type span struct{ min, max int64 }
	spans := make(map[string]span)
	for _, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q5", event) {
			continue
		}
		cur, ok := spans[event.Did]
		if !ok {
			spans[event.Did] = span{min: event.TimeUS, max: event.TimeUS}
			continue
		}
		if event.TimeUS < cur.min {
			cur.min = event.TimeUS
		}
		if event.TimeUS > cur.max {
			cur.max = event.TimeUS
		}
		spans[event.Did] = cur
	}
	groups := make([]ColumnPhysicalQueryGroup, 0, len(spans))
	for did, span := range spans {
		groups = append(groups, ColumnPhysicalQueryGroup{Key: did, Int64: span.max - span.min})
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Int64 != groups[j].Int64 {
			return groups[i].Int64 > groups[j].Int64
		}
		return groups[i].Key < groups[j].Key
	})
	if topK > 0 && len(groups) > topK {
		groups = groups[:topK]
	}
	return groups
}

func assertColumnPhysicalQ5DenseResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, want []ColumnPhysicalQueryGroup, rows, matchedRows int, wantReducer string) {
	tb.Helper()
	if len(result.Groups) != len(want) {
		tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
	}
	for i := range want {
		if result.Groups[i].Key != want[i].Key || result.Groups[i].Int64 != want[i].Int64 {
			tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
		}
	}
	assertColumnPhysicalQ5DenseDiagnostics1950(tb, label, result.Diagnostics, rows, matchedRows, 3, len(want), wantReducer)
}

func assertColumnPhysicalQ5DenseDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, rows, matchedRows, topK, resultGroups int, wantReducer string) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics storage/fallback=%+v", label, diag)
	}
	if !diag.DenseInt64SpanUsed {
		tb.Fatalf("%s diagnostics did not mark dense int64-span use: %+v", label, diag)
	}
	if diag.DenseInt64SpanReducer != wantReducer {
		tb.Fatalf("%s dense int64-span reducer=%q want %q diagnostics=%+v", label, diag.DenseInt64SpanReducer, wantReducer, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialized rows/documents: %+v", label, diag)
	}
	if diag.RowsScanned != rows || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s rows scanned/matched/reduced=%d/%d/%d want %d/%d diagnostics=%+v", label, diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, rows, matchedRows, diag)
	}
	if diag.PredicateCount != 3 || diag.PredicateLiterals != 3 {
		tb.Fatalf("%s predicate diagnostics=%+v want three equality predicates", label, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s missing typed-column section/decode diagnostics: %+v", label, diag)
	}
	if diag.TopKLimit != topK || diag.TopKOrder != string(ColumnPhysicalQueryTopKInt64Desc) || diag.TopKCandidates < resultGroups || diag.ResultGroups != resultGroups {
		tb.Fatalf("%s topK diagnostics=%+v want limit/order/result groups", label, diag)
	}
}

func reportColumnPhysicalQ5DenseBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(diag.ReduceRows), "reduce_rows/op")
	b.ReportMetric(float64(diag.DecodedPayloadBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPartSections), "typed_sections/op")
	b.ReportMetric(float64(diag.TopKCandidates), "topk_candidates/op")
}
