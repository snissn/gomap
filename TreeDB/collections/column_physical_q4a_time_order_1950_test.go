package collections

import (
	"fmt"
	"runtime"
	"sort"
	"testing"
)

func TestColumnPhysicalQ4ATimeOrderTopKDirectPreparedParity1950(t *testing.T) {
	batches := columnPhysicalQ4ATimeOrderBatches1950(9_000)
	events := flattenColumnPhysicalEvents1950(batches)
	d, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, []ColumnSortKey{{Column: "time_us"}}, batches)
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
	req := columnPhysicalQ4ATimeOrderRequest1950()
	want := columnPhysicalQ4ATimeOrderReferenceGroups1950(scanned, req.TopK)
	wantCandidates := columnPhysicalQ4ATimeOrderReferenceEarlyCandidates1950(scanned, req.TopK)

	preview, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("preview RunColumnPhysicalQuery(q4a time-order topK): %v", err)
	}
	assertColumnPhysicalQ4ATimeOrderResult1950(t, "direct preview", preview, want, len(events), wantCandidates)
	for run := 0; run < 2; run++ {
		direct, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			t.Fatalf("RunColumnPhysicalQuery(q4a time-order topK run %d): %v", run, err)
		}
		assertColumnPhysicalQ4ATimeOrderResult1950(t, fmt.Sprintf("direct run %d", run), direct, want, len(events), wantCandidates)
	}

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q4a time-order topK): %v", err)
	}
	defer func() { _ = runner.Close() }()
	for run := 0; run < 2; run++ {
		prepared, err := runner.Run()
		if err != nil {
			t.Fatalf("prepared q4a time-order topK run %d: %v", run, err)
		}
		assertColumnPhysicalQ4ATimeOrderResult1950(t, fmt.Sprintf("prepared run %d", run), prepared, want, len(events), wantCandidates)
	}
}

func TestColumnPhysicalQ4ATimeOrderTopKOneShotCache3085(t *testing.T) {
	batches := columnPhysicalQ4ATimeOrderBatches1950(9_000)
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, []ColumnSortKey{{Column: "time_us"}}, batches)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	req := columnPhysicalQ4ATimeOrderRequest1950()
	want := columnPhysicalQ4ATimeOrderReferenceGroups1950(scanned, req.TopK)
	wantCandidates := columnPhysicalQ4ATimeOrderReferenceEarlyCandidates1950(scanned, req.TopK)

	first, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q4a time-order topK): %v", err)
	}
	assertColumnPhysicalQ4ATimeOrderResult1950(t, "first one-shot", first, want, len(events), wantCandidates)
	snapshot := col.typedColumnOneShotCacheSnapshotForTest()
	if snapshot.Entries != 1 || snapshot.CacheMisses != 1 || snapshot.CacheBuilds != 1 || snapshot.CacheHits != 0 {
		t.Fatalf("cache after first run=%+v want one miss/build entry", snapshot)
	}

	second, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q4a time-order topK): %v", err)
	}
	assertColumnPhysicalQ4ATimeOrderResult1950(t, "second one-shot", second, want, len(events), wantCandidates)
	snapshot = col.typedColumnOneShotCacheSnapshotForTest()
	if snapshot.Entries != 1 || snapshot.CacheMisses != 1 || snapshot.CacheBuilds != 1 || snapshot.CacheHits != 1 {
		t.Fatalf("cache after second run=%+v want one cache hit", snapshot)
	}

	smallerTopK := req
	smallerTopK.TopK = 2
	smallerWant := columnPhysicalQ4ATimeOrderReferenceGroups1950(scanned, smallerTopK.TopK)
	smallerCandidates := columnPhysicalQ4ATimeOrderReferenceEarlyCandidates1950(scanned, smallerTopK.TopK)
	third, err := col.RunColumnPhysicalQuery(smallerTopK)
	if err != nil {
		t.Fatalf("third RunColumnPhysicalQuery(q4a time-order topK=2): %v", err)
	}
	if len(third.Groups) != len(smallerWant) {
		t.Fatalf("third one-shot topK=2 groups=%+v want %+v", third.Groups, smallerWant)
	}
	for i := range smallerWant {
		if third.Groups[i].Key != smallerWant[i].Key || third.Groups[i].Int64 != smallerWant[i].Int64 {
			t.Fatalf("third one-shot topK=2 groups=%+v want %+v", third.Groups, smallerWant)
		}
	}
	if !third.Diagnostics.TimeOrderTopKUsed || third.Diagnostics.TopKLimit != smallerTopK.TopK || third.Diagnostics.TopKCandidates != smallerCandidates || third.Diagnostics.ResultGroups != len(smallerWant) {
		t.Fatalf("third one-shot topK=2 diagnostics=%+v want time-order topK=%d candidates=%d groups=%d", third.Diagnostics, smallerTopK.TopK, smallerCandidates, len(smallerWant))
	}
	snapshot = col.typedColumnOneShotCacheSnapshotForTest()
	if snapshot.Entries != 2 || snapshot.CacheMisses != 2 || snapshot.CacheBuilds != 2 || snapshot.CacheHits != 1 || snapshot.Invalidations != 0 {
		t.Fatalf("cache after topK change=%+v want retained prior entry and new build", snapshot)
	}
}

func TestColumnPhysicalQ4ATimeOrderTopKParallelEagerPayloads3158(t *testing.T) {
	if oldProcs := runtime.GOMAXPROCS(0); oldProcs < 2 {
		runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(oldProcs)
	}

	batches := columnPhysicalQ4ATimeOrderBatches1950(9_000)
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, []ColumnSortKey{{Column: "time_us"}}, batches)
	defer closeFn()

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, col, len(events))
	req := columnPhysicalQ4ATimeOrderRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegrityCachedVerify
	want := columnPhysicalQ4ATimeOrderReferenceGroups1950(scanned, req.TopK)
	wantCandidates := columnPhysicalQ4ATimeOrderReferenceEarlyCandidates1950(scanned, req.TopK)

	first, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("first RunColumnPhysicalQuery(q4a time-order topK cached-verify): %v", err)
	}
	assertColumnPhysicalQ4ATimeOrderResult1950(t, "parallel eager first one-shot", first, want, len(events), wantCandidates)
	assertColumnPhysicalQ4ATimeOrderOneShotEagerPayloads3158(t, col, req)

	second, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("second RunColumnPhysicalQuery(q4a time-order topK cached-verify): %v", err)
	}
	assertColumnPhysicalQ4ATimeOrderResult1950(t, "parallel eager second one-shot", second, want, len(events), wantCandidates)
	assertTypedColumnOneShotCacheSnapshot3088(t, col, "after q4a parallel eager second", 1, 1, 1, 1, 0)
}

func BenchmarkColumnPhysicalQ4ATimeOrderTopK1950(b *testing.B) {
	batches := columnPhysicalQ4ATimeOrderBatches1950(9_000)
	events := flattenColumnPhysicalEvents1950(batches)
	req := columnPhysicalQ4ATimeOrderRequest1950()
	cases := []struct {
		name     string
		sortKey  []ColumnSortKey
		prepared bool
		wantFast bool
	}{
		{name: "direct/primary_id_full_scan"},
		{name: "prepared/primary_id_full_scan", prepared: true},
		{name: "direct/time_order_early_stop", sortKey: []ColumnSortKey{{Column: "time_us"}}, wantFast: true},
		{name: "prepared/time_order_early_stop", sortKey: []ColumnSortKey{{Column: "time_us"}}, prepared: true, wantFast: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, tc.sortKey, batches)
			defer closeFn()
			preview, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			if preview.Diagnostics.TimeOrderTopKUsed != tc.wantFast {
				b.Fatalf("preview time-order used=%t want %t diagnostics=%+v", preview.Diagnostics.TimeOrderTopKUsed, tc.wantFast, preview.Diagnostics)
			}
			if len(preview.Groups) != req.TopK {
				b.Fatalf("preview groups=%d want topK=%d diagnostics=%+v", len(preview.Groups), req.TopK, preview.Diagnostics)
			}
			b.SetBytes(int64(preview.Diagnostics.DecodedPayloadBytes))
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
					b.Fatalf("q4a time-order run %d: %v", i, err)
				}
				last = result.Diagnostics
				groups += len(result.Groups)
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no result groups")
			}
			reportColumnPhysicalQ4ATimeOrderBenchMetrics1950(b, last, len(events))
		})
	}
}

func assertColumnPhysicalQ4ATimeOrderOneShotEagerPayloads3158(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest) {
	tb.Helper()
	if col == nil {
		tb.Fatalf("nil collection")
	}
	wantPredicates := collectionTypedColumnOneShotPredicateKey(req)
	var entry *collectionTypedColumnOneShotCacheEntry
	col.typedColumnOneShotMu.Lock()
	for slot, current := range col.typedColumnOneShot {
		if slot.kind == req.Kind &&
			slot.groupColumn == req.GroupColumn &&
			slot.valueColumn == req.ValueColumn &&
			slot.topK == req.TopK &&
			slot.topKOrder == req.TopKOrder &&
			slot.readIntegrity == req.ColumnAssetReadIntegrity &&
			slot.predicates == wantPredicates {
			entry = current
			break
		}
	}
	col.typedColumnOneShotMu.Unlock()
	if entry == nil {
		tb.Fatalf("q4a typed-column one-shot cache entry not found")
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.runner == nil {
		tb.Fatalf("q4a typed-column one-shot runner not found")
	}
	if got := len(entry.runner.parts); got < 2 {
		tb.Fatalf("q4a typed-column one-shot runner parts=%d want at least 2 for parallel decode", got)
	}
	for partIdx, part := range entry.runner.parts {
		if part.TimeOrderTopK == nil {
			tb.Fatalf("q4a runner part %d missing time-order topK state", partIdx)
		}
		if part.TimeOrderTopK.PayloadLoader != nil {
			tb.Fatalf("q4a runner part %d retained lazy payload loader after parallel decode", partIdx)
		}
	}
}

func columnPhysicalQ4ATimeOrderRequest1950() ColumnPhysicalQueryRequest {
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

func columnPhysicalQ4ATimeOrderBatches1950(lateRowsPerBatch int) [][]columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_920_000_000_000_000)
	batchA := []columnPhysicalJSONBenchParityEventP0{
		{ID: "a-kind-guard", TimeUS: base + 1, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:guard-kind"},
		{ID: "a-z", TimeUS: base + 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:z"},
		{ID: "a-m", TimeUS: base + 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
	}
	batchB := []columnPhysicalJSONBenchParityEventP0{
		{ID: "b-operation-guard", TimeUS: base + 2, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.post", Did: "did:guard-operation"},
		{ID: "b-a", TimeUS: base + 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:a"},
		{ID: "b-b", TimeUS: base + 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:b"},
		{ID: "b-m-late", TimeUS: base + 40, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:m"},
	}
	for i := 0; i < lateRowsPerBatch; i++ {
		batchA = append(batchA, columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("a-late-%05d", i),
			TimeUS:     base + 1_000 + int64(i*2),
			Kind:       "commit",
			Operation:  "create",
			Collection: "app.bsky.feed.post",
			Did:        fmt.Sprintf("did:late:a:%05d", i),
		})
		batchB = append(batchB, columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("b-late-%05d", i),
			TimeUS:     base + 1_001 + int64(i*2),
			Kind:       "commit",
			Operation:  "create",
			Collection: "app.bsky.feed.post",
			Did:        fmt.Sprintf("did:late:b:%05d", i),
		})
	}
	return [][]columnPhysicalJSONBenchParityEventP0{batchA, batchB}
}

func columnPhysicalQ4ATimeOrderReferenceGroups1950(events []columnPhysicalJSONBenchParityEventP0, topK int) []ColumnPhysicalQueryGroup {
	mins := make(map[string]int64)
	for _, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q4a", event) {
			continue
		}
		if cur, ok := mins[event.Did]; !ok || event.TimeUS < cur {
			mins[event.Did] = event.TimeUS
		}
	}
	groups := make([]ColumnPhysicalQueryGroup, 0, len(mins))
	for did, value := range mins {
		groups = append(groups, ColumnPhysicalQueryGroup{Key: did, Int64: value})
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

func columnPhysicalQ4ATimeOrderReferenceEarlyCandidates1950(events []columnPhysicalJSONBenchParityEventP0, topK int) int {
	sortedEvents := append([]columnPhysicalJSONBenchParityEventP0(nil), events...)
	sort.Slice(sortedEvents, func(i, j int) bool {
		if sortedEvents[i].TimeUS != sortedEvents[j].TimeUS {
			return sortedEvents[i].TimeUS < sortedEvents[j].TimeUS
		}
		return sortedEvents[i].ID < sortedEvents[j].ID
	})
	mins := make(map[string]int64)
	top := make([]ColumnPhysicalQueryGroup, 0, topK)
	haveKth := false
	kthTime := int64(0)
	for _, event := range sortedEvents {
		if haveKth && event.TimeUS > kthTime {
			break
		}
		if !columnPhysicalJSONBenchReferenceMatchP0("q4a", event) {
			continue
		}
		if _, exists := mins[event.Did]; exists {
			continue
		}
		mins[event.Did] = event.TimeUS
		top = top[:0]
		for key, value := range mins {
			insertColumnPhysicalTopKGroup(&top, ColumnPhysicalQueryGroup{Key: key, Int64: value}, topK, ColumnPhysicalQueryTopKInt64Asc)
		}
		if len(top) >= topK {
			haveKth = true
			kthTime = top[len(top)-1].Int64
		}
	}
	return len(mins)
}

func assertColumnPhysicalQ4ATimeOrderResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, want []ColumnPhysicalQueryGroup, totalRows, wantCandidates int) {
	tb.Helper()
	if len(result.Groups) != len(want) {
		tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
	}
	for i := range want {
		if result.Groups[i].Key != want[i].Key || result.Groups[i].Int64 != want[i].Int64 {
			tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
		}
	}
	assertColumnPhysicalQ4ATimeOrderDiagnostics1950(tb, label, result.Diagnostics, totalRows, wantCandidates, len(want))
}

func assertColumnPhysicalQ4ATimeOrderDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, totalRows, wantCandidates, wantGroups int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics=%+v want typed-column source without storage fallback", label, diag)
	}
	if !diag.TimeOrderTopKUsed {
		tb.Fatalf("%s diagnostics did not mark time-order topK use: %+v", label, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialization diagnostics=%+v want no row/document materialization", label, diag)
	}
	if diag.PredicateCount != 3 || diag.PredicateLiterals != 3 || diag.RowsMatched != diag.ReduceRows || diag.RowsMatched == 0 {
		tb.Fatalf("%s predicate diagnostics=%+v want three predicates and matched rows reduced", label, diag)
	}
	if diag.RowsScanned <= 0 || diag.RowsScanned >= totalRows {
		tb.Fatalf("%s rows scanned=%d want early stop below total rows=%d diagnostics=%+v", label, diag.RowsScanned, totalRows, diag)
	}
	if diag.ScheduledGranules == 0 || diag.DecodedGranules == 0 || diag.DecodedGranules >= diag.ScheduledGranules || diag.SkippedGranules == 0 {
		tb.Fatalf("%s granule diagnostics=%+v want decoded granules below scheduled", label, diag)
	}
	if diag.DecodedPayloadBytes == 0 || diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s decode diagnostics=%+v want typed-column section payload decode", label, diag)
	}
	if diag.TopKLimit != 3 || diag.TopKOrder != string(ColumnPhysicalQueryTopKInt64Asc) || diag.TopKCandidates != wantCandidates || diag.ResultGroups != wantGroups {
		tb.Fatalf("%s topK diagnostics=%+v want candidates=%d groups=%d", label, diag, wantCandidates, wantGroups)
	}
}

func reportColumnPhysicalQ4ATimeOrderBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics, totalRows int) {
	b.Helper()
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(diag.ReduceRows), "reduce_rows/op")
	b.ReportMetric(float64(totalRows), "total_rows/op")
	b.ReportMetric(float64(diag.ScheduledGranules), "granules_scheduled/op")
	b.ReportMetric(float64(diag.SkippedGranules), "granules_skipped/op")
	b.ReportMetric(float64(diag.DecodedGranules), "granules_decoded/op")
	b.ReportMetric(float64(diag.DecodedPayloadBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.TopKCandidates), "topk_candidates/op")
}
