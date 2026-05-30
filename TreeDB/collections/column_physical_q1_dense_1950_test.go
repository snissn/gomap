package collections

import (
	"encoding/json"
	"fmt"
	"testing"

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
	assertColumnPhysicalQ1DenseResult1950(t, "direct", direct, want, totalRows)

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
		assertColumnPhysicalQ1DenseResult1950(t, fmt.Sprintf("prepared run %d", run), prepared, want, totalRows)
	}
}

func BenchmarkColumnPhysicalQ1DenseTypedColumn1950(b *testing.B) {
	collections := make([][]string, 4)
	patterns := [][]string{
		{"app.m", "app.z", "app.feed", "app.m"},
		{"app.a", "app.m", "app.graph", "app.a"},
		{"app.chat", "app.z", "app.m", "app.chat"},
		{"app.a", "app.video", "app.m", "app.video"},
	}
	for batch := range collections {
		collections[batch] = make([]string, 4096)
		for i := range collections[batch] {
			collections[batch][i] = patterns[batch][i%len(patterns[batch])]
		}
	}
	batches := columnPhysicalQ1DenseEventBatches1950(collections)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, nil, batches)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	totalRows := totalQ1DenseRows1950(batches)

	b.Run("direct_RunColumnPhysicalQuery", func(b *testing.B) {
		preview, err := col.RunColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
		}
		assertColumnPhysicalQ1DenseDiagnostics1950(b, "preview direct", preview.Diagnostics, totalRows)
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
		assertColumnPhysicalQ1DenseDiagnostics1950(b, "preview prepared", preview.Diagnostics, totalRows)
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

func assertColumnPhysicalQ1DenseResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, want map[string]int, rows int) {
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
	assertColumnPhysicalQ1DenseDiagnostics1950(tb, label, result.Diagnostics, rows)
}

func assertColumnPhysicalQ1DenseDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, rows int) {
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
	if diag.RowsScanned != rows || diag.ReduceRows != rows {
		tb.Fatalf("%s rows scanned/reduced=%d/%d want %d diagnostics=%+v", label, diag.RowsScanned, diag.ReduceRows, rows, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 || diag.DecodedPayloadBytes == 0 || diag.DecodedBlocks == 0 {
		tb.Fatalf("%s missing typed-column section/decode diagnostics: %+v", label, diag)
	}
	if diag.PredicateCount != 0 || diag.RowsMatched != 0 {
		tb.Fatalf("%s q1 dense should not report predicates: %+v", label, diag)
	}
}

func reportColumnPhysicalQ1DenseBenchMetrics1950(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.DecodedPayloadBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.TypedColumnPartSections), "typed_sections/op")
}

func totalQ1DenseRows1950(batches [][]columnPhysicalJSONBenchParityEventP0) int {
	total := 0
	for _, batch := range batches {
		total += len(batch)
	}
	return total
}
