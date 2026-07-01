package collections

import (
	"fmt"
	"testing"
	"time"
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
	assertColumnPhysicalQ3DenseResult1950(t, "direct", direct, want, len(events), matchedRows, matchedRows)

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
		assertColumnPhysicalQ3DenseResult1950(t, fmt.Sprintf("prepared run %d", run), prepared, want, len(events), matchedRows, matchedRows)
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
		assertColumnPhysicalQ3DenseDiagnostics1950(b, "preview direct", preview.Diagnostics, len(events), matchedRows, matchedRows, len(preview.Groups))
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
		assertColumnPhysicalQ3DenseDiagnostics1950(b, "preview prepared", preview.Diagnostics, len(events), matchedRows, matchedRows, len(preview.Groups))
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

func TestColumnPhysicalQ3DenseTypedColumnOneShotSetupDiagnostics1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := columnPhysicalQ3DenseRequest1950()
	view, plan, closeView := openColumnPhysicalQ3DenseOneShotSetupView1950(t, col, req)
	defer closeView()

	diag := prepareColumnPhysicalQ3DenseOneShotSetup1950(t, view, req, plan)
	assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(t, "q3 one-shot setup", diag)
}

func TestColumnPhysicalQ3DenseTypedColumnOneShotTargetedSetupDiagnostics1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ3DenseBatchA1950(), columnPhysicalQ3DenseBatchB1950()}
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := columnPhysicalQ3DenseRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	view, plan, closeView := openColumnPhysicalQ3DenseOneShotSetupView1950(t, col, req)
	defer closeView()

	diag := prepareColumnPhysicalQ3DenseOneShotSetup1950(t, view, req, plan)
	assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(t, "q3 targeted one-shot setup", diag)
}

func BenchmarkColumnPhysicalQ3DenseTypedColumnOneShotSetup1950(b *testing.B) {
	events := columnPhysicalQ3DenseBenchmarkEvents1950(16_384)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, [][]columnPhysicalJSONBenchParityEventP0{events})
	defer closeFn()

	req := columnPhysicalQ3DenseRequest1950()
	view, plan, closeView := openColumnPhysicalQ3DenseOneShotSetupView1950(b, col, req)
	defer closeView()

	preview := prepareColumnPhysicalQ3DenseOneShotSetup1950(b, view, req, plan)
	assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(b, "preview q3 one-shot setup", preview)
	b.SetBytes(int64(preview.DecodedPayloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	var last ColumnPhysicalQueryDiagnostics
	for i := 0; i < b.N; i++ {
		last = prepareColumnPhysicalQ3DenseOneShotSetup1950(b, view, req, plan)
	}
	b.StopTimer()
	assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(b, "last q3 one-shot setup", last)
	reportColumnPhysicalQ3DenseOneShotSetupBenchMetrics1950(b, last)
}

func BenchmarkColumnPhysicalQ3DenseTypedColumnOneShotTargetedSetup1950(b *testing.B) {
	events := columnPhysicalQ3DenseBenchmarkEvents1950(16_384)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, [][]columnPhysicalJSONBenchParityEventP0{events})
	defer closeFn()

	req := columnPhysicalQ3DenseRequest1950()
	req.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	view, plan, closeView := openColumnPhysicalQ3DenseOneShotSetupView1950(b, col, req)
	defer closeView()

	preview := prepareColumnPhysicalQ3DenseOneShotSetup1950(b, view, req, plan)
	assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(b, "preview targeted q3 one-shot setup", preview)
	b.SetBytes(int64(preview.DecodedPayloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	var last ColumnPhysicalQueryDiagnostics
	for i := 0; i < b.N; i++ {
		last = prepareColumnPhysicalQ3DenseOneShotSetup1950(b, view, req, plan)
	}
	b.StopTimer()
	assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(b, "last targeted q3 one-shot setup", last)
	reportColumnPhysicalQ3DenseOneShotSetupBenchMetrics1950(b, last)
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

func openColumnPhysicalQ3DenseOneShotSetupView1950(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest) (columnPhysicalScanSnapshotView, columnTypedColumnPhysicalQueryPlan, func()) {
	tb.Helper()
	if req.AggregateMetadataName != "" || !columnTypedColumnOneShotCacheRequestCandidate(req) {
		tb.Fatalf("q3 setup request is not a no-metadata one-shot candidate: %+v", req)
	}
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if closeView == nil {
		closeView = func() {}
	}
	if err != nil {
		closeView()
		tb.Fatalf("prepare q3 setup snapshot view: %v", err)
	}
	if view.MutationParts != 0 {
		closeView()
		tb.Fatalf("q3 setup view has mutation parts=%d; not a production one-shot candidate", view.MutationParts)
	}
	plan, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req)
	if err != nil {
		closeView()
		tb.Fatalf("plan q3 setup typed-column query: %v", err)
	}
	if !candidate || !columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req) {
		closeView()
		tb.Fatalf("q3 setup plan candidate=%t dense_group_hour=%t plan=%+v", candidate, columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req), plan)
	}
	return view, plan, closeView
}

func prepareColumnPhysicalQ3DenseOneShotSetup1950(tb testing.TB, view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, plan columnTypedColumnPhysicalQueryPlan) ColumnPhysicalQueryDiagnostics {
	tb.Helper()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		tb.Fatalf("open q3 setup read cache: %v", err)
	}
	readCache.returnViews = true
	// Mirror the one-shot cache build boundary without running the reducer or storing a cache entry.
	buildStart := time.Now()
	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunnerWithOptions(view, req, &readCache, columnTypedColumnPhysicalQueryRunnerPrepareOptions{
		prepareDenseGroupCountDistinctGlobalRanks: columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req),
	})
	buildNanos := time.Since(buildStart).Nanoseconds()
	if err != nil {
		if runner != nil {
			runner.close()
		}
		_ = readCache.close()
		tb.Fatalf("prepare q3 one-shot setup runner: %v", err)
	}
	if !candidate || runner == nil {
		_ = readCache.close()
		tb.Fatalf("prepare q3 one-shot setup candidate=%t runner=%p", candidate, runner)
	}
	diag := columnPhysicalQ3DenseOneShotSetupDiagnostics1950(runner, req, buildNanos)
	runner.close()
	if err := readCache.close(); err != nil {
		tb.Fatalf("close q3 setup read cache: %v", err)
	}
	return diag
}

func columnPhysicalQ3DenseOneShotSetupDiagnostics1950(runner *columnTypedColumnPhysicalQueryRunner, req ColumnPhysicalQueryRequest, buildNanos int64) ColumnPhysicalQueryDiagnostics {
	var diag ColumnPhysicalQueryDiagnostics
	diag.TypedColumnOneShotCacheMiss = true
	diag.TypedColumnOneShotCacheBuild = true
	diag.TypedColumnOneShotBuildNanos = buildNanos
	if runner == nil {
		return diag
	}
	runner.prepareDiagnostics.applyTo(&diag)
	diag.ProjectedColumns = len(runner.plan.ProjectedColumns)
	diag.ScheduledGranules = runner.granulesConsidered
	diag.SkippedGranules = runner.granulesSkipped
	diag.DecodedGranules = runner.granulesDecoded
	diag.DecodedBlocks = runner.decodedBlocks
	diag.TypedColumnPartSections = runner.sections
	diag.TypedColumnPartSectionBytes = runner.sectionBytes
	diag.PhysicalBytesScanned = runner.assetBytes
	diag.DecodedPayloadBytes = runner.decodedPayloadBytes
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.StorageSource = ColumnPhysicalQueryStorageSourceTypedColumnPartSection
	diag.FallbackReason = ColumnPhysicalQueryFallbackNone
	diag.SegmentFileCacheHits = runner.segmentFileCacheHits
	diag.SegmentFileCacheMisses = runner.segmentFileCacheMisses
	diag.DenseGroupHourCountUsed = columnTypedColumnPhysicalQueryUseDenseGroupHourCount(runner.plan, req)
	applyColumnPhysicalQueryPredicateDiagnostics(&diag, runner.plan.PredicateDiagnostics, 0, 0)
	return diag
}

func assertColumnPhysicalQ3DenseResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, want []ColumnPhysicalQueryGroup, wantRowsScanned, wantRowsMatched, wantReduceRows int) {
	tb.Helper()
	if len(result.Groups) != len(want) {
		tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
	}
	for i := range want {
		if result.Groups[i].Key != want[i].Key || result.Groups[i].Hour != want[i].Hour || result.Groups[i].Count != want[i].Count {
			tb.Fatalf("%s groups=%+v want %+v", label, result.Groups, want)
		}
	}
	assertColumnPhysicalQ3DenseDiagnostics1950(tb, label, result.Diagnostics, wantRowsScanned, wantRowsMatched, wantReduceRows, len(want))
}

func assertColumnPhysicalQ3DenseDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, wantRowsScanned, wantRowsMatched, wantReduceRows, resultGroups int) {
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
	if diag.RowsScanned != wantRowsScanned || diag.RowsMatched != wantRowsMatched || diag.ReduceRows != wantReduceRows {
		tb.Fatalf("%s rows scanned/matched/reduced=%d/%d/%d want %d/%d/%d diagnostics=%+v", label, diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, wantRowsScanned, wantRowsMatched, wantReduceRows, diag)
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

func assertColumnPhysicalQ3DenseOneShotSetupDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s setup diagnostics storage/fallback=%+v", label, diag)
	}
	if !diag.DenseGroupHourCountUsed || diag.DenseInt64SpanUsed || diag.DenseGroupCountUsed || diag.DenseGroupCountDistinctUsed {
		tb.Fatalf("%s setup dense path diagnostics=%+v want only dense group-hour", label, diag)
	}
	if diag.RowsScanned != 0 || diag.RowsMatched != 0 || diag.ReduceRows != 0 || diag.ResultGroups != 0 {
		tb.Fatalf("%s setup ran query rows scanned/matched/reduced/groups=%d/%d/%d/%d diagnostics=%+v", label, diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, diag.ResultGroups, diag)
	}
	if diag.PredicateCount != 3 || diag.PredicateLiterals != 5 {
		tb.Fatalf("%s setup predicate diagnostics=%+v want two equal predicates plus three-value IN", label, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s setup missing typed-column section/decode diagnostics: %+v", label, diag)
	}
	if diag.TypedColumnPrepareWorkerCount <= 0 {
		tb.Fatalf("%s setup missing prepare worker diagnostics workers=%d diagnostics=%+v", label, diag.TypedColumnPrepareWorkerCount, diag)
	}
	if diag.TypedColumnOneShotBuildNanos < 0 || diag.TypedColumnPreparePartDecodeNanos < 0 {
		tb.Fatalf("%s setup negative build/decode diagnostics build=%d part_decode=%d diagnostics=%+v",
			label,
			diag.TypedColumnOneShotBuildNanos,
			diag.TypedColumnPreparePartDecodeNanos,
			diag)
	}
	if diag.TypedColumnPrepareSummaryNanos != 0 {
		tb.Fatalf("%s setup summary nanos=%d want 0 for no-metadata one-shot setup diagnostics=%+v", label, diag.TypedColumnPrepareSummaryNanos, diag)
	}
	targetedRanges := typedColumnStringUseTargetedRanges(ColumnAssetReadIntegrity(diag.ColumnAssetReadIntegrity))
	if !targetedRanges && (diag.TypedColumnPrepareRangeReadNanos != 0 || diag.TypedColumnPrepareRangeReadBytes != 0) {
		tb.Fatalf("%s setup range-read diagnostics=%d/%d want 0 for raw q3 setup diagnostics=%+v",
			label,
			diag.TypedColumnPrepareRangeReadNanos,
			diag.TypedColumnPrepareRangeReadBytes,
			diag)
	}
	if targetedRanges && diag.TypedColumnPrepareRangeReadBytes == 0 {
		tb.Fatalf("%s setup missing targeted range-read bytes diagnostics=%+v", label, diag)
	}
	denseNanos := diag.TypedColumnPrepareDenseGroupNanos +
		diag.TypedColumnPrepareDenseValueNanos +
		diag.TypedColumnPrepareDensePredicateNanos +
		diag.TypedColumnPrepareDensePreapplyNanos
	if diag.TypedColumnPrepareDenseGroupNanos < 0 ||
		diag.TypedColumnPrepareDenseValueNanos < 0 ||
		diag.TypedColumnPrepareDensePredicateNanos < 0 ||
		diag.TypedColumnPrepareDensePreapplyNanos < 0 {
		tb.Fatalf("%s setup negative dense split diagnostics group=%d value=%d predicate=%d preapply=%d diagnostics=%+v",
			label,
			diag.TypedColumnPrepareDenseGroupNanos,
			diag.TypedColumnPrepareDenseValueNanos,
			diag.TypedColumnPrepareDensePredicateNanos,
			diag.TypedColumnPrepareDensePreapplyNanos,
			diag)
	}
	if !targetedRanges && denseNanos != 0 && (diag.TypedColumnPrepareDenseGroupNanos <= 0 || diag.TypedColumnPrepareDenseValueNanos <= 0 || diag.TypedColumnPrepareDensePredicateNanos <= 0) {
		tb.Fatalf("%s setup dense split diagnostics group=%d value=%d predicate=%d preapply=%d diagnostics=%+v",
			label,
			diag.TypedColumnPrepareDenseGroupNanos,
			diag.TypedColumnPrepareDenseValueNanos,
			diag.TypedColumnPrepareDensePredicateNanos,
			diag.TypedColumnPrepareDensePreapplyNanos,
			diag)
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

func reportColumnPhysicalQ3DenseOneShotSetupBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	b.ReportMetric(float64(diag.DecodedBlocks), "decoded_blocks/op")
	b.ReportMetric(float64(diag.DecodedPayloadBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPartSections), "typed_sections/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareWorkerCount), "typed_column_prepare_workers/op")
	b.ReportMetric(float64(diag.TypedColumnOneShotBuildNanos), "typed_column_one_shot_build_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPreparePlanNanos), "typed_column_prepare_plan_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareRefsNanos), "typed_column_prepare_refs_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPreparePairingNanos), "typed_column_prepare_pairing_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPreparePartDecodeNanos), "typed_column_prepare_part_decode_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPreparePostPrepareNanos), "typed_column_prepare_post_prepare_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareReadImageNanos), "typed_column_prepare_read_image_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareStateBuildNanos), "typed_column_prepare_state_build_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareDictionaryNanos), "typed_column_prepare_dictionary_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPreparePruningNanos), "typed_column_prepare_pruning_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareSortKeyNanos), "typed_column_prepare_sort_key_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareStatsNanos), "typed_column_prepare_stats_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareRangeReadNanos), "typed_column_prepare_range_read_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareRangeReadBytes), "typed_column_prepare_range_read_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareAdapterNanos), "typed_column_prepare_adapter_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareDenseGroupNanos), "typed_column_prepare_dense_group_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareDenseValueNanos), "typed_column_prepare_dense_value_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareDensePredicateNanos), "typed_column_prepare_dense_predicate_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareDensePreapplyNanos), "typed_column_prepare_dense_preapply_nanos/op")
}
