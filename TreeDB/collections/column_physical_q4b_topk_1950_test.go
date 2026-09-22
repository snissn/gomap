package collections

import (
	"sort"
	"testing"
)

func TestTypedColumnQ4BTopKMarkPrunedParity1950(t *testing.T) {
	sortKey := typedColumnQ4BTopKClickHouseSortKey1950()
	events := typedColumnQ4BTopKTieBreakEvents1950()
	_, col, closeFn := openTypedColumnSortKeyFixture1948(t, sortKey, events)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	assertColumnPhysicalJSONBenchRowScanPreservesFullPredicateColumnsP0(t, events, scanned)

	req := typedColumnQ4BTopKRequest1950()
	want := typedColumnQ4BTopKReferenceGroups1950(scanned, req.TopK)
	wantCandidates := typedColumnQ4BTopKReferenceCandidateCount1950(scanned)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q4b", scanned)

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q4b topK): %v", err)
	}
	assertTypedColumnQ4BTopKGroups1950(t, "direct", direct.Groups, want)
	assertTypedColumnQ4BTopKDiagnostics1950(t, "direct", direct.Diagnostics, len(events), matchedRows, req.TopK, len(want), wantCandidates)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q4b topK): %v", err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared q4b topK: %v", err)
	}
	assertTypedColumnQ4BTopKGroups1950(t, "prepared", prepared.Groups, want)
	assertTypedColumnQ4BTopKDiagnostics1950(t, "prepared", prepared.Diagnostics, len(events), matchedRows, req.TopK, len(want), wantCandidates)
}

func TestColumnPhysicalNonMetadataTopKShaping1950(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	col, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	req := typedColumnQ4BTopKRequest1950()
	req.TopK = 1
	want := typedColumnQ4BTopKReferenceGroups1950(scanned, req.TopK)

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(non-metadata topK): %v", err)
	}
	assertTypedColumnQ4BTopKGroups1950(t, "direct compatibility", direct.Groups, want)
	if direct.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset || direct.Diagnostics.TopKLimit != req.TopK || direct.Diagnostics.TopKCandidates <= req.TopK || direct.Diagnostics.ResultGroups != len(want) {
		t.Fatalf("direct diagnostics=%+v want non-metadata TopK-shaped compatibility path", direct.Diagnostics)
	}

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(non-metadata topK): %v", err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared non-metadata topK: %v", err)
	}
	assertTypedColumnQ4BTopKGroups1950(t, "prepared compatibility", prepared.Groups, want)
	if prepared.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset || prepared.Diagnostics.TopKLimit != req.TopK || prepared.Diagnostics.TopKCandidates <= req.TopK || prepared.Diagnostics.ResultGroups != len(want) {
		t.Fatalf("prepared diagnostics=%+v want non-metadata TopK-shaped compatibility path", prepared.Diagnostics)
	}
}

func BenchmarkTypedColumnQ4BTopK1950(b *testing.B) {
	events := typedColumnSortKeyPruningEvents1949()
	clickHouseSortKey := typedColumnQ4BTopKClickHouseSortKey1950()
	cases := []struct {
		name     string
		sortKey  []ColumnSortKey
		events   []columnPhysicalJSONBenchParityEventP0
		prepared bool
	}{
		{name: "direct/primary_id_fallback", events: events},
		{name: "prepared/primary_id_fallback", events: events, prepared: true},
		{name: "direct/clickhouse_full_scan", sortKey: clickHouseSortKey, events: typedColumnSortKeyAllPostEvents1949(len(events))},
		{name: "prepared/clickhouse_full_scan", sortKey: clickHouseSortKey, events: typedColumnSortKeyAllPostEvents1949(len(events)), prepared: true},
		{name: "direct/clickhouse_mark_pruned", sortKey: clickHouseSortKey, events: events},
		{name: "prepared/clickhouse_mark_pruned", sortKey: clickHouseSortKey, events: events, prepared: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, col, closeFn := openTypedColumnSortKeyFixture1948(b, tc.sortKey, tc.events)
			defer closeFn()
			req := typedColumnQ4BTopKRequest1950()
			preview, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			if len(preview.Groups) != req.TopK {
				b.Fatalf("preview groups=%d want topK=%d diagnostics=%+v", len(preview.Groups), req.TopK, preview.Diagnostics)
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
					b.Fatalf("q4b topK run: %v", err)
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
			b.ReportMetric(float64(last.ScheduledGranules), "granules_considered/op")
			b.ReportMetric(float64(last.SkippedGranules), "granules_skipped/op")
			b.ReportMetric(float64(last.DecodedGranules), "granules_decoded/op")
			b.ReportMetric(float64(last.SortKeyMarkChecks), "mark_checks/op")
			b.ReportMetric(float64(last.SortKeyMarkSkips), "mark_skips/op")
			b.ReportMetric(float64(last.DecodedPayloadBytes), "decoded_bytes/op")
			b.ReportMetric(float64(last.TopKCandidates), "topk_candidates/op")
			if last.ScanNanos > 0 {
				b.ReportMetric(1e9/float64(last.ScanNanos), "diag_ops_per_sec")
			}
		})
	}
}

func typedColumnQ4BTopKTieBreakEvents1950() []columnPhysicalJSONBenchParityEventP0 {
	events := typedColumnSortKeyPruningEvents1949()
	type didMin struct {
		did  string
		time int64
		idx  int
	}
	minByDID := make(map[string]didMin)
	for i, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q4b", event) {
			continue
		}
		cur, ok := minByDID[event.Did]
		if !ok || event.TimeUS < cur.time {
			minByDID[event.Did] = didMin{did: event.Did, time: event.TimeUS, idx: i}
		}
	}
	mins := make([]didMin, 0, len(minByDID))
	for _, min := range minByDID {
		mins = append(mins, min)
	}
	if len(mins) < 2 {
		panic("typedColumnQ4BTopKTieBreakEvents1950: fixture needs at least two q4b groups")
	}
	sort.Slice(mins, func(i, j int) bool {
		if mins[i].time != mins[j].time {
			return mins[i].time < mins[j].time
		}
		return mins[i].did < mins[j].did
	})
	// Force an equal min(time_us) across two groups so the top-K assertion
	// exercises deterministic Key tie-break parity with the row-scan reference.
	events[mins[1].idx].TimeUS = mins[0].time
	return events
}

func typedColumnQ4BTopKRequest1950() ColumnPhysicalQueryRequest {
	return ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupMinInt64,
		GroupColumn: "did",
		ValueColumn: "time_us",
		TopK:        3,
		TopKOrder:   ColumnPhysicalQueryTopKInt64Asc,
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "collection", Value: "app.bsky.feed.post"},
		},
	}
}

func typedColumnQ4BTopKClickHouseSortKey1950() []ColumnSortKey {
	return []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
}

func typedColumnQ4BTopKReferenceGroups1950(events []columnPhysicalJSONBenchParityEventP0, topK int) []ColumnPhysicalQueryGroup {
	mins := typedColumnQ4BTopKReferenceMins1950(events)
	groups := make([]ColumnPhysicalQueryGroup, 0, len(mins))
	for key, value := range mins {
		groups = append(groups, ColumnPhysicalQueryGroup{Key: key, Int64: value})
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Int64 != groups[j].Int64 {
			return groups[i].Int64 < groups[j].Int64
		}
		return groups[i].Key < groups[j].Key
	})
	if len(groups) > topK {
		groups = groups[:topK]
	}
	return groups
}

func typedColumnQ4BTopKReferenceCandidateCount1950(events []columnPhysicalJSONBenchParityEventP0) int {
	return len(typedColumnQ4BTopKReferenceMins1950(events))
}

func typedColumnQ4BTopKReferenceMins1950(events []columnPhysicalJSONBenchParityEventP0) map[string]int64 {
	mins := make(map[string]int64)
	for _, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q4b", event) {
			continue
		}
		if cur, ok := mins[event.Did]; !ok || event.TimeUS < cur {
			mins[event.Did] = event.TimeUS
		}
	}
	return mins
}

func assertTypedColumnQ4BTopKGroups1950(tb testing.TB, label string, got, want []ColumnPhysicalQueryGroup) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("%s groups=%+v want %+v", label, got, want)
	}
	for i := range want {
		if got[i].Key != want[i].Key || got[i].Int64 != want[i].Int64 {
			tb.Fatalf("%s groups=%+v want %+v", label, got, want)
		}
	}
}

func assertTypedColumnQ4BTopKDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, totalRows, matchedRows, wantTopK, wantGroups, wantCandidates int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics=%+v want typed-column source without storage fallback", label, diag)
	}
	if diag.PredicateCount != 3 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s predicate diagnostics=%+v want predicates=3 matched/reduced=%d", label, diag, matchedRows)
	}
	if !diag.SortKeyPrefixPlanned || diag.SortKeyPrefixLiterals != 3 || !equalStrings1949(diag.SortKeyPrefixColumns, []string{"kind", "operation", "collection"}) {
		tb.Fatalf("%s prefix diagnostics=%+v want kind/operation/collection sorted prefix", label, diag)
	}
	if diag.SortKeyMarkChecks == 0 || diag.SortKeyMarkSkips == 0 || diag.SkippedGranules == 0 {
		tb.Fatalf("%s mark diagnostics=%+v want checked and skipped sort-key marks", label, diag)
	}
	if diag.RowsScanned <= 0 || diag.RowsScanned >= totalRows || diag.DecodedPayloadBytes == 0 {
		tb.Fatalf("%s scan diagnostics=%+v want mark-pruned typed-column section decode", label, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialization diagnostics=%+v want no row/document materialization", label, diag)
	}
	if diag.TopKLimit != wantTopK || diag.TopKOrder != string(ColumnPhysicalQueryTopKInt64Asc) || diag.TopKCandidates != wantCandidates || diag.ResultGroups != wantGroups {
		tb.Fatalf("%s topK diagnostics=%+v want limit/order/candidates/result groups", label, diag)
	}
}
