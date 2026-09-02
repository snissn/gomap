package collections

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestColumnPhysicalQ1DenseTypedColumnDirectPreparedParity1950(t *testing.T) {
	batches := columnPhysicalQ1DenseEventBatches1950([][]string{
		{"app.m", "app.z", "app.m", "app.z"},
		{"app.a", "app.m", "app.a", "app.m"},
	})
	d, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) < 2 {
		t.Fatalf("typed-column part refs=%d want at least two insert-only batches/parts: %+v", len(refs), refs)
	}
	codesByGeneration := typedColumnQ1DictionaryCodeByGeneration1950(t, d, col, "collection", "app.m")
	if len(codesByGeneration) < 2 {
		t.Fatalf("app.m dictionary codes by generation=%v want at least two", codesByGeneration)
	}
	seenCodes := make(map[int64]struct{}, len(codesByGeneration))
	for _, code := range codesByGeneration {
		seenCodes[code] = struct{}{}
	}
	if len(seenCodes) < 2 {
		t.Fatalf("app.m local dictionary codes=%v want differing local dictionary orders", codesByGeneration)
	}

	totalRows := totalQ1DenseRows1950(batches)
	want := rowScanCollectionCounts1950(t, col, totalRows)
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q1 dense): %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "direct", direct, want, totalRows, totalRows)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q1 dense): %v", err)
	}
	defer func() { _ = runner.Close() }()
	for run := 0; run < 2; run++ {
		prepared, err := runner.Run()
		if err != nil {
			t.Fatalf("prepared q1 dense run %d: %v", run, err)
		}
		assertColumnPhysicalQ1DenseResult1950(t, fmt.Sprintf("prepared run %d", run), prepared, want, totalRows, totalRows)
	}
}

func TestColumnPhysicalQ1DenseTypedColumnOneShotSummaryUsesCounts1950(t *testing.T) {
	batches := columnPhysicalQ1DenseEventBatches1950([][]string{
		{"app.m", "app.z", "app.m", "app.z"},
		{"app.a", "app.m", "app.a", "app.m"},
	})
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}
	view, plan, closeView := openColumnPhysicalQ1DenseOneShotSetupView1950(t, col, req)
	defer closeView()

	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		t.Fatalf("open q1 summary read cache: %v", err)
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunnerWithOptions(view, req, &readCache, columnTypedColumnPhysicalQueryRunnerPrepareOptions{
		prepareAggregateSummaries: columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req),
		plannedQuery:              plan,
		hasPlannedQuery:           true,
	})
	if err != nil {
		if runner != nil {
			runner.close()
		}
		t.Fatalf("prepare q1 summary runner: %v", err)
	}
	if !candidate || runner == nil {
		t.Fatalf("prepare q1 summary candidate=%t runner=%p", candidate, runner)
	}
	defer runner.close()
	if runner.aggregateSummary == nil || !runner.aggregateSummary.denseGroupCount {
		t.Fatalf("q1 runner missing dense group-count aggregate summary: %#v", runner.aggregateSummary)
	}
	totalRows := totalQ1DenseRows1950(batches)
	if runner.aggregateSummary.rowsScanned != totalRows || runner.aggregateSummary.reduceRows != totalRows {
		t.Fatalf("q1 aggregate summary rows scanned/reduced=%d/%d want %d/%d", runner.aggregateSummary.rowsScanned, runner.aggregateSummary.reduceRows, totalRows, totalRows)
	}
	for partIdx := range runner.parts {
		dense := runner.parts[partIdx].DenseGroupCount
		if dense == nil {
			t.Fatalf("q1 part %d missing dense group-count", partIdx)
		}
		if len(dense.Codes) != 0 || dense.Valid != nil {
			t.Fatalf("q1 part %d retained row codes/valid codes=%d valid=%d dense=%+v", partIdx, len(dense.Codes), len(dense.Valid), dense)
		}
		if len(dense.Counts) != dense.Cardinality || dense.Rows != runner.parts[partIdx].Rows {
			t.Fatalf("q1 part %d counts/cardinality/rows=%d/%d/%d want rows=%d dense=%+v", partIdx, len(dense.Counts), dense.Cardinality, dense.Rows, runner.parts[partIdx].Rows, dense)
		}
		counted := dense.Missing
		for _, count := range dense.Counts {
			counted += count
		}
		if counted != dense.Rows {
			t.Fatalf("q1 part %d counted rows=%d want %d dense=%+v", partIdx, counted, dense.Rows, dense)
		}
	}
	result, err := runner.run(view, req)
	if err != nil {
		t.Fatalf("q1 summary runner run: %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "q1 summary runner", result, rowScanCollectionCounts1950(t, col, totalRows), totalRows, totalRows)
}

func BenchmarkColumnPhysicalQ1DenseTypedColumn1950(b *testing.B) {
	batches := columnPhysicalQ1DenseBenchmarkBatches1950(4096)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, batches)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	totalRows := totalQ1DenseRows1950(batches)

	b.Run("direct_RunColumnPhysicalQuery", func(b *testing.B) {
		preview, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
		}
		assertColumnPhysicalQ1DenseDiagnostics1950(b, "preview direct", preview.Diagnostics, totalRows, totalRows)
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
		reportColumnPhysicalQ1DenseBenchMetrics1950(b, last)
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
		assertColumnPhysicalQ1DenseDiagnostics1950(b, "preview prepared", preview.Diagnostics, totalRows, totalRows)
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
		reportColumnPhysicalQ1DenseBenchMetrics1950(b, last)
	})
}

func TestColumnPhysicalQ1DenseTypedColumnOneShotSetupDiagnostics1950(t *testing.T) {
	batches := columnPhysicalQ1DenseEventBatches1950([][]string{
		{"app.m", "app.z", "app.m", "app.z"},
		{"app.a", "app.m", "app.a", "app.m"},
	})
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	view, plan, closeView := openColumnPhysicalQ1DenseOneShotSetupView1950(t, col, req)
	defer closeView()

	diag := prepareColumnPhysicalQ1DenseOneShotSetup1950(t, view, req, plan)
	assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(t, "q1 one-shot setup", diag)
}

func TestColumnPhysicalQ1DenseOneShotSetupDiagnosticsToleratesCoarseClock1950(t *testing.T) {
	diag := ColumnPhysicalQueryDiagnostics{TypedColumnOneShotBuildNanos: 753_200}
	if columnPhysicalQ1DenseOneShotSetupMissingSplitDiagnostics1950(diag, false) {
		t.Fatal("outer-only timing must be accepted when the clock cannot resolve any nested phase")
	}

	diag.TypedColumnPreparePartDecodeNanos = 1
	if !columnPhysicalQ1DenseOneShotSetupMissingSplitDiagnostics1950(diag, false) {
		t.Fatal("a resolved nested phase must retain the missing-split diagnostic gate")
	}
}

func TestColumnPhysicalQ1DenseTypedColumnOneShotReusesPlannedQuery1950(t *testing.T) {
	batches := columnPhysicalQ1DenseEventBatches1950([][]string{
		{"app.m", "app.z", "app.m", "app.z"},
		{"app.a", "app.m", "app.a", "app.m"},
	})
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	totalRows := totalQ1DenseRows1950(batches)
	want := rowScanCollectionCounts1950(t, col, totalRows)
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q1 dense one-shot): %v", err)
	}
	assertColumnPhysicalQ1DenseResult1950(t, "q1 one-shot planned-query reuse", result, want, totalRows, totalRows)
	if !result.Diagnostics.TypedColumnOneShotCacheMiss || !result.Diagnostics.TypedColumnOneShotCacheBuild || result.Diagnostics.TypedColumnOneShotCacheHit {
		t.Fatalf("q1 one-shot cache diagnostics hit/miss/build=%t/%t/%t want false/true/true diagnostics=%+v",
			result.Diagnostics.TypedColumnOneShotCacheHit,
			result.Diagnostics.TypedColumnOneShotCacheMiss,
			result.Diagnostics.TypedColumnOneShotCacheBuild,
			result.Diagnostics)
	}
	if result.Diagnostics.TypedColumnPreparePlanNanos != 0 {
		t.Fatalf("q1 one-shot prepare plan nanos=%d want 0 when reusing already planned query diagnostics=%+v",
			result.Diagnostics.TypedColumnPreparePlanNanos,
			result.Diagnostics)
	}
}

func TestColumnPhysicalQ1DenseTypedColumnOneShotTargetedSetupDiagnostics1950(t *testing.T) {
	batches := columnPhysicalQ1DenseEventBatches1950([][]string{
		{"app.m", "app.z", "app.m", "app.z"},
		{"app.a", "app.m", "app.a", "app.m"},
	})
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}
	view, plan, closeView := openColumnPhysicalQ1DenseOneShotSetupView1950(t, col, req)
	defer closeView()

	diag := prepareColumnPhysicalQ1DenseOneShotSetup1950(t, view, req, plan)
	assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(t, "q1 targeted one-shot setup", diag)
}

func BenchmarkColumnPhysicalQ1DenseTypedColumnOneShotSetup1950(b *testing.B) {
	batches := columnPhysicalQ1DenseBenchmarkBatches1950(4096)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, batches)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	view, plan, closeView := openColumnPhysicalQ1DenseOneShotSetupView1950(b, col, req)
	defer closeView()

	preview := prepareColumnPhysicalQ1DenseOneShotSetup1950(b, view, req, plan)
	assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(b, "preview q1 one-shot setup", preview)
	b.SetBytes(int64(preview.DecodedPayloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	var last ColumnPhysicalQueryDiagnostics
	for i := 0; i < b.N; i++ {
		last = prepareColumnPhysicalQ1DenseOneShotSetup1950(b, view, req, plan)
	}
	b.StopTimer()
	assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(b, "last q1 one-shot setup", last)
	reportColumnPhysicalQ1DenseOneShotSetupBenchMetrics1950(b, last)
}

func BenchmarkColumnPhysicalQ1DenseTypedColumnOneShotTargetedSetup1950(b *testing.B) {
	batches := columnPhysicalQ1DenseBenchmarkBatches1950(4096)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, batches)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection", ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}
	view, plan, closeView := openColumnPhysicalQ1DenseOneShotSetupView1950(b, col, req)
	defer closeView()

	preview := prepareColumnPhysicalQ1DenseOneShotSetup1950(b, view, req, plan)
	assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(b, "preview targeted q1 one-shot setup", preview)
	b.SetBytes(int64(preview.DecodedPayloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	var last ColumnPhysicalQueryDiagnostics
	for i := 0; i < b.N; i++ {
		last = prepareColumnPhysicalQ1DenseOneShotSetup1950(b, view, req, plan)
	}
	b.StopTimer()
	assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(b, "last targeted q1 one-shot setup", last)
	reportColumnPhysicalQ1DenseOneShotSetupBenchMetrics1950(b, last)
}

func columnPhysicalQ1DenseBenchmarkBatches1950(rowsPerBatch int) [][]columnPhysicalJSONBenchParityEventP0 {
	collections := make([][]string, 4)
	patterns := [][]string{
		{"app.m", "app.z", "app.feed", "app.m"},
		{"app.a", "app.m", "app.graph", "app.a"},
		{"app.chat", "app.z", "app.m", "app.chat"},
		{"app.a", "app.video", "app.m", "app.video"},
	}
	for batch := range collections {
		collections[batch] = make([]string, rowsPerBatch)
		for i := range collections[batch] {
			collections[batch][i] = patterns[batch][i%len(patterns[batch])]
		}
	}
	return columnPhysicalQ1DenseEventBatches1950(collections)
}

func columnPhysicalQ1DenseEventBatches1950(collections [][]string) [][]columnPhysicalJSONBenchParityEventP0 {
	batches := make([][]columnPhysicalJSONBenchParityEventP0, len(collections))
	seq := 0
	for batchIdx, batchCollections := range collections {
		batches[batchIdx] = make([]columnPhysicalJSONBenchParityEventP0, len(batchCollections))
		for i, collection := range batchCollections {
			batches[batchIdx][i] = columnPhysicalJSONBenchParityEventP0{
				ID:         fmt.Sprintf("doc_%02d_%06d", batchIdx, i),
				TimeUS:     int64(1_900_000_000_000_000 + seq),
				Kind:       "commit",
				Operation:  "create",
				Collection: collection,
				Did:        fmt.Sprintf("did:q1:%06d", seq%1024),
			}
			seq++
		}
	}
	return batches
}

func rowScanCollectionCounts1950(tb testing.TB, col *Collection, rows int) map[string]int {
	tb.Helper()
	records, truncated, err := col.ScanDocuments(rows)
	if err != nil {
		tb.Fatalf("ScanDocuments: %v", err)
	}
	if truncated || len(records) != rows {
		tb.Fatalf("ScanDocuments rows=%d truncated=%v want rows=%d", len(records), truncated, rows)
	}
	counts := make(map[string]int)
	for _, record := range records {
		var doc struct {
			Collection string `json:"collection"`
		}
		if err := json.Unmarshal(record.Document, &doc); err != nil {
			tb.Fatalf("json.Unmarshal(%s): %v", record.Document, err)
		}
		counts[doc.Collection]++
	}
	return counts
}

func typedColumnQ1DictionaryCodeByGeneration1950(tb testing.TB, d *backenddb.DB, col *Collection, column, value string) map[uint64]int64 {
	tb.Helper()
	out := make(map[uint64]int64)
	for _, ref := range typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col)) {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
		if err != nil {
			tb.Fatalf("read typed-column part generation=%d: %v", ref.Generation, err)
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			tb.Fatalf("ParseColumnPartImage generation=%d: %v", ref.Generation, err)
		}
		dicts, err := image.Dictionaries()
		if err != nil {
			tb.Fatalf("Dictionaries generation=%d: %v", ref.Generation, err)
		}
		code, ok := dicts[column][value]
		if ok {
			out[ref.Generation] = code
		}
	}
	return out
}

func assertColumnPhysicalQ1DenseResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, want map[string]int, wantRowsScanned, wantReduceRows int) {
	tb.Helper()
	got := make(map[string]int, len(result.Groups))
	for _, group := range result.Groups {
		got[group.Key] = group.Count
	}
	if len(got) != len(want) {
		tb.Fatalf("%s groups=%v want %v", label, got, want)
	}
	for key, wantCount := range want {
		if got[key] != wantCount {
			tb.Fatalf("%s group %q count=%d want %d all=%v", label, key, got[key], wantCount, got)
		}
	}
	assertColumnPhysicalQ1DenseDiagnostics1950(tb, label, result.Diagnostics, wantRowsScanned, wantReduceRows)
}

func openColumnPhysicalQ1DenseOneShotSetupView1950(tb testing.TB, col *Collection, req ColumnPhysicalQueryRequest) (columnPhysicalScanSnapshotView, columnTypedColumnPhysicalQueryPlan, func()) {
	tb.Helper()
	if req.AggregateMetadataName != "" || !columnTypedColumnOneShotCacheRequestCandidate(req) {
		tb.Fatalf("q1 setup request is not a no-metadata one-shot candidate: %+v", req)
	}
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if closeView == nil {
		closeView = func() {}
	}
	if err != nil {
		closeView()
		tb.Fatalf("prepare q1 setup snapshot view: %v", err)
	}
	if view.MutationParts != 0 {
		closeView()
		tb.Fatalf("q1 setup view has mutation parts=%d; not a production one-shot candidate", view.MutationParts)
	}
	plan, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req)
	if err != nil {
		closeView()
		tb.Fatalf("plan q1 setup typed-column query: %v", err)
	}
	if !candidate || !columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req) {
		closeView()
		tb.Fatalf("q1 setup plan candidate=%t dense_group_count=%t plan=%+v", candidate, columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req), plan)
	}
	return view, plan, closeView
}

func prepareColumnPhysicalQ1DenseOneShotSetup1950(tb testing.TB, view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, plan columnTypedColumnPhysicalQueryPlan) ColumnPhysicalQueryDiagnostics {
	tb.Helper()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		tb.Fatalf("open q1 setup read cache: %v", err)
	}
	readCache.returnViews = true
	buildStart := time.Now()
	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunnerWithOptions(view, req, &readCache, columnTypedColumnPhysicalQueryRunnerPrepareOptions{
		prepareDenseGroupCountDistinctGlobalRanks: columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req),
		plannedQuery:    plan,
		hasPlannedQuery: true,
	})
	buildNanos := time.Since(buildStart).Nanoseconds()
	if err != nil {
		if runner != nil {
			runner.close()
		}
		_ = readCache.close()
		tb.Fatalf("prepare q1 one-shot setup runner: %v", err)
	}
	if !candidate || runner == nil {
		_ = readCache.close()
		tb.Fatalf("prepare q1 one-shot setup candidate=%t runner=%p", candidate, runner)
	}
	diag := columnPhysicalQ1DenseOneShotSetupDiagnostics1950(runner, req, buildNanos)
	runner.close()
	if err := readCache.close(); err != nil {
		tb.Fatalf("close q1 setup read cache: %v", err)
	}
	return diag
}

func columnPhysicalQ1DenseOneShotSetupDiagnostics1950(runner *columnTypedColumnPhysicalQueryRunner, req ColumnPhysicalQueryRequest, buildNanos int64) ColumnPhysicalQueryDiagnostics {
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
	diag.DenseGroupCountUsed = columnTypedColumnPhysicalQueryUseDenseGroupCount(runner.plan, req)
	applyColumnPhysicalQueryPredicateDiagnostics(&diag, runner.plan.PredicateDiagnostics, 0, 0)
	return diag
}

func assertColumnPhysicalQ1DenseDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, wantRowsScanned, wantReduceRows int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics storage/fallback=%+v", label, diag)
	}
	if !diag.DenseGroupCountUsed {
		tb.Fatalf("%s diagnostics did not mark dense group-count use: %+v", label, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialized rows/documents: %+v", label, diag)
	}
	if diag.RowsScanned != wantRowsScanned || diag.ReduceRows != wantReduceRows {
		tb.Fatalf("%s rows scanned/reduced=%d/%d want %d/%d diagnostics=%+v", label, diag.RowsScanned, diag.ReduceRows, wantRowsScanned, wantReduceRows, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s missing typed-column section/decode diagnostics: %+v", label, diag)
	}
	if diag.PredicateCount != 0 || diag.RowsMatched != 0 {
		tb.Fatalf("%s q1 dense should not report predicates: %+v", label, diag)
	}
}

func assertColumnPhysicalQ1DenseOneShotSetupDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s setup diagnostics storage/fallback=%+v", label, diag)
	}
	if !diag.DenseGroupCountUsed || diag.DenseGroupCountDistinctUsed || diag.DenseGroupHourCountUsed || diag.DenseInt64SpanUsed {
		tb.Fatalf("%s setup dense path diagnostics=%+v want only dense group-count", label, diag)
	}
	if diag.RowsScanned != 0 || diag.RowsMatched != 0 || diag.ReduceRows != 0 || diag.ResultGroups != 0 {
		tb.Fatalf("%s setup ran query rows scanned/matched/reduced/groups=%d/%d/%d/%d diagnostics=%+v", label, diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, diag.ResultGroups, diag)
	}
	if diag.PredicateCount != 0 || diag.PredicateLiterals != 0 {
		tb.Fatalf("%s setup q1 should not report predicates: %+v", label, diag)
	}
	if diag.TypedColumnOneShotCacheHit || !diag.TypedColumnOneShotCacheMiss || !diag.TypedColumnOneShotCacheBuild {
		tb.Fatalf("%s setup one-shot cache diagnostics hit/miss/build=%t/%t/%t want false/true/true diagnostics=%+v",
			label,
			diag.TypedColumnOneShotCacheHit,
			diag.TypedColumnOneShotCacheMiss,
			diag.TypedColumnOneShotCacheBuild,
			diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s setup missing typed-column section/decode diagnostics: %+v", label, diag)
	}
	if diag.TypedColumnPrepareWorkerCount <= 0 {
		tb.Fatalf("%s setup missing prepare worker diagnostics: %+v", label, diag)
	}
	if diag.TypedColumnOneShotBuildNanos < 0 || diag.TypedColumnPreparePartDecodeNanos < 0 {
		tb.Fatalf("%s setup negative build/decode diagnostics build=%d part_decode=%d diagnostics=%+v",
			label,
			diag.TypedColumnOneShotBuildNanos,
			diag.TypedColumnPreparePartDecodeNanos,
			diag)
	}
	if diag.TypedColumnPreparePlanNanos != 0 {
		tb.Fatalf("%s setup prepare plan nanos=%d want 0 for reused typed-column query plan diagnostics=%+v",
			label,
			diag.TypedColumnPreparePlanNanos,
			diag)
	}
	if diag.TypedColumnPrepareSummaryNanos != 0 {
		tb.Fatalf("%s setup summary nanos=%d want 0 for no-metadata one-shot setup diagnostics=%+v", label, diag.TypedColumnPrepareSummaryNanos, diag)
	}
	if diag.TypedColumnPrepareDenseValueNanos != 0 || diag.TypedColumnPrepareDensePredicateNanos != 0 || diag.TypedColumnPrepareDensePreapplyNanos != 0 {
		tb.Fatalf("%s setup dense value/predicate/preapply diagnostics=%d/%d/%d want 0 for q1 group-count setup diagnostics=%+v",
			label,
			diag.TypedColumnPrepareDenseValueNanos,
			diag.TypedColumnPrepareDensePredicateNanos,
			diag.TypedColumnPrepareDensePreapplyNanos,
			diag)
	}
	if diag.TypedColumnPrepareDenseGroupNanos < 0 {
		tb.Fatalf("%s setup negative dense group diagnostics=%d diagnostics=%+v", label, diag.TypedColumnPrepareDenseGroupNanos, diag)
	}
	targetedRanges := typedColumnStringUseTargetedRanges(ColumnAssetReadIntegrity(diag.ColumnAssetReadIntegrity))
	if !targetedRanges && (diag.TypedColumnPrepareRangeReadNanos != 0 || diag.TypedColumnPrepareRangeReadBytes != 0) {
		tb.Fatalf("%s setup range-read diagnostics=%d/%d want 0 for raw q1 setup diagnostics=%+v",
			label,
			diag.TypedColumnPrepareRangeReadNanos,
			diag.TypedColumnPrepareRangeReadBytes,
			diag)
	}
	if targetedRanges && diag.TypedColumnPrepareRangeReadBytes == 0 {
		tb.Fatalf("%s setup missing targeted range-read bytes diagnostics=%+v", label, diag)
	}
	unexplainedSetupNanos := diag.TypedColumnOneShotBuildNanos - diag.TypedColumnPreparePartDecodeNanos
	if columnPhysicalQ1DenseOneShotSetupMissingSplitDiagnostics1950(diag, targetedRanges) {
		tb.Fatalf("%s setup missing split diagnostics unexplained_setup=%d read_image=%d state_build=%d dictionary=%d adapter=%d dense_group=%d diagnostics=%+v",
			label,
			unexplainedSetupNanos,
			diag.TypedColumnPrepareReadImageNanos,
			diag.TypedColumnPrepareStateBuildNanos,
			diag.TypedColumnPrepareDictionaryNanos,
			diag.TypedColumnPrepareAdapterNanos,
			diag.TypedColumnPrepareDenseGroupNanos,
			diag)
	}
}

func columnPhysicalQ1DenseOneShotSetupMissingSplitDiagnostics1950(diag ColumnPhysicalQueryDiagnostics, targetedRanges bool) bool {
	rawSetupNanos := diag.TypedColumnPrepareReadImageNanos +
		diag.TypedColumnPrepareStateBuildNanos +
		diag.TypedColumnPrepareDictionaryNanos +
		diag.TypedColumnPrepareAdapterNanos
	unexplainedSetupNanos := diag.TypedColumnOneShotBuildNanos - diag.TypedColumnPreparePartDecodeNanos
	return unexplainedSetupNanos > 0 &&
		diag.TypedColumnPreparePartDecodeNanos > 0 &&
		rawSetupNanos == 0 &&
		diag.TypedColumnPrepareDenseGroupNanos == 0 &&
		!targetedRanges
}

func reportColumnPhysicalQ1DenseBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.DecodedPayloadBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPartSections), "typed_sections/op")
}

func reportColumnPhysicalQ1DenseOneShotSetupBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
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
	b.ReportMetric(float64(diag.TypedColumnPrepareRangeReadNanos), "typed_column_prepare_range_read_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareRangeReadBytes), "typed_column_prepare_range_read_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareAdapterNanos), "typed_column_prepare_adapter_nanos/op")
	b.ReportMetric(float64(diag.TypedColumnPrepareDenseGroupNanos), "typed_column_prepare_dense_group_nanos/op")
}

func totalQ1DenseRows1950(batches [][]columnPhysicalJSONBenchParityEventP0) int {
	total := 0
	for _, batch := range batches {
		total += len(batch)
	}
	return total
}
