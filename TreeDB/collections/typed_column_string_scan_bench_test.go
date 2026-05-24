package collections

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

var typedColumnStringPredicateBenchSink int

func TestTypedColumnStringPredicateBenchDirectMatchesFullScan(t *testing.T) {
	const rowsPerPart = 1024
	target := "kind_003"
	d, col := setupTypedColumnStringPredicateBenchCollection(t, true)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringPredicateBenchRows(t, col, 0, typedColumnStringPredicateBenchKinds(rowsPerPart, target, "kind_007", 4))
	insertTypedColumnStringPredicateBenchRows(t, col, rowsPerPart, typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart))

	runner := prepareTypedColumnStringPredicateBenchRunner(t, d, col, target)
	var scratch typedColumnStringPredicateBenchScratch
	scratch.matches = make([]int, 0, rowsPerPart/4)
	direct, err := runner.scan(&scratch)
	if err != nil {
		t.Fatalf("direct string predicate scan: %v", err)
	}
	fallback, err := runTypedColumnStringPredicateBenchDocumentFallback(col, rowsPerPart*2, target)
	if err != nil {
		t.Fatalf("document fallback string predicate scan: %v", err)
	}
	if direct.RowsMatched != fallback.RowsMatched || direct.CodesMatched != fallback.RowsMatched || direct.RowsMatched != rowsPerPart/4 {
		t.Fatalf("direct diagnostics=%+v fallback diagnostics=%+v", direct, fallback)
	}
	if direct.PartsPruned == 0 || direct.RowMaterializations != 0 || direct.DocumentMaterializations != 0 || direct.DocumentReconstructions != 0 {
		t.Fatalf("direct diagnostics=%+v want typed-column pruning without row/document materialization", direct)
	}
	if fallback.DocumentMaterializations == 0 || fallback.DocumentReconstructions == 0 {
		t.Fatalf("fallback diagnostics=%+v want full document materialization/reconstruction evidence", fallback)
	}
}

func BenchmarkTypedColumnStringPredicateScan(b *testing.B) {
	rowsPerPart := typedColumnStringPredicateBenchRowsPerPart(b)
	target := "kind_003"
	b.Run(fmt.Sprintf("rows_%d/path_typed_column_part/equality", rowsPerPart*2), func(b *testing.B) {
		d, col := setupTypedColumnStringPredicateBenchCollection(b, true)
		defer func() { _ = d.Close() }()
		insertTypedColumnStringPredicateBenchRows(b, col, 0, typedColumnStringPredicateBenchKinds(rowsPerPart, target, "kind_007", maxIntForTypedColumnStringPredicateBench(1, rowsPerPart/16)))
		insertTypedColumnStringPredicateBenchRows(b, col, rowsPerPart, typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart))
		runner := prepareTypedColumnStringPredicateBenchRunner(b, d, col, target)
		var scratch typedColumnStringPredicateBenchScratch
		scratch.matches = make([]int, 0, runner.expectedMatches)
		warm, err := runner.scan(&scratch)
		if err != nil {
			b.Fatalf("warm typed-column string scan: %v", err)
		}
		if warm.RowsMatched != runner.expectedMatches {
			b.Fatalf("warm rows_matched=%d want %d diagnostics=%+v", warm.RowsMatched, runner.expectedMatches, warm)
		}

		b.ReportAllocs()
		b.ResetTimer()
		benchStart := time.Now()
		var diag typedColumnStringPredicateBenchDiagnostics
		for i := 0; i < b.N; i++ {
			scratch.matches = scratch.matches[:0]
			diag, err = runner.scan(&scratch)
			if err != nil {
				b.Fatalf("typed-column string scan: %v", err)
			}
		}
		b.StopTimer()
		if diag.RowsMatched != runner.expectedMatches {
			b.Fatalf("rows_matched=%d want %d diagnostics=%+v", diag.RowsMatched, runner.expectedMatches, diag)
		}
		typedColumnStringPredicateBenchSink = len(scratch.matches)
		reportTypedColumnStringPredicateBenchMetrics(b, diag, time.Since(benchStart), b.N)
	})

	b.Run(fmt.Sprintf("rows_%d/path_document_full_scan_fallback/equality", rowsPerPart*2), func(b *testing.B) {
		d, col := setupTypedColumnStringPredicateBenchCollection(b, true)
		defer func() { _ = d.Close() }()
		insertTypedColumnStringPredicateBenchRows(b, col, 0, typedColumnStringPredicateBenchKinds(rowsPerPart, target, "kind_007", maxIntForTypedColumnStringPredicateBench(1, rowsPerPart/16)))
		insertTypedColumnStringPredicateBenchRows(b, col, rowsPerPart, typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart))
		warm, err := runTypedColumnStringPredicateBenchDocumentFallback(col, rowsPerPart*2, target)
		if err != nil {
			b.Fatalf("warm document fallback string scan: %v", err)
		}
		expected := rowsPerPart / maxIntForTypedColumnStringPredicateBench(1, rowsPerPart/16)
		if warm.RowsMatched != expected {
			b.Fatalf("warm rows_matched=%d want %d diagnostics=%+v", warm.RowsMatched, expected, warm)
		}

		b.ReportAllocs()
		b.ResetTimer()
		benchStart := time.Now()
		var diag typedColumnStringPredicateBenchDiagnostics
		for i := 0; i < b.N; i++ {
			diag, err = runTypedColumnStringPredicateBenchDocumentFallback(col, rowsPerPart*2, target)
			if err != nil {
				b.Fatalf("document fallback string scan: %v", err)
			}
		}
		b.StopTimer()
		if diag.RowsMatched != expected {
			b.Fatalf("rows_matched=%d want %d diagnostics=%+v", diag.RowsMatched, expected, diag)
		}
		typedColumnStringPredicateBenchSink = diag.RowsMatched
		reportTypedColumnStringPredicateBenchMetrics(b, diag, time.Since(benchStart), b.N)
	})
}

func BenchmarkTypedColumnStringPredicateScanCore(b *testing.B) {
	rowsPerPart := typedColumnStringPredicateBenchRowsPerPart(b)
	target := "kind_003"
	d, col := setupTypedColumnStringPredicateBenchCollection(b, true)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringPredicateBenchRows(b, col, 0, typedColumnStringPredicateBenchKinds(rowsPerPart, target, "kind_007", maxIntForTypedColumnStringPredicateBench(1, rowsPerPart/16)))
	insertTypedColumnStringPredicateBenchRows(b, col, rowsPerPart, typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart))
	runner := prepareTypedColumnStringPredicateBenchRunner(b, d, col, target)
	var scratch typedColumnStringPredicateBenchScratch
	scratch.matches = make([]int, 0, runner.expectedMatches)
	warm, err := runner.scan(&scratch)
	if err != nil {
		b.Fatalf("warm core string scan: %v", err)
	}
	if warm.RowsMatched != runner.expectedMatches {
		b.Fatalf("warm rows_matched=%d want %d diagnostics=%+v", warm.RowsMatched, runner.expectedMatches, warm)
	}
	// The timed loop below reuses the decoded part metadata, target dictionary
	// code, GranuleReader scratch, and result row-index buffer. Any allocs/op
	// reported here point at the core dictionary-code scan loop rather than DB
	// open, asset read, dictionary decoding, or result slice growth.
	b.ReportAllocs()
	b.ResetTimer()
	benchStart := time.Now()
	var diag typedColumnStringPredicateBenchDiagnostics
	for i := 0; i < b.N; i++ {
		scratch.matches = scratch.matches[:0]
		diag, err = runner.scan(&scratch)
		if err != nil {
			b.Fatalf("core string scan: %v", err)
		}
	}
	b.StopTimer()
	if diag.RowsMatched != runner.expectedMatches {
		b.Fatalf("rows_matched=%d want %d diagnostics=%+v", diag.RowsMatched, runner.expectedMatches, diag)
	}
	typedColumnStringPredicateBenchSink = len(scratch.matches)
	reportTypedColumnStringPredicateBenchMetrics(b, diag, time.Since(benchStart), b.N)
}

type typedColumnStringPredicateBenchDiagnostics struct {
	RowsScanned              int
	RowsMatched              int
	PartsConsidered          int
	PartsPruned              int
	PartsDecoded             int
	BlocksConsidered         int
	BlocksPruned             int
	BlocksDecoded            int
	CodesMatched             int
	DictionaryBytesDecoded   uint64
	MappedBytes              uint64
	HeapCopyBytes            uint64
	DecodedHeapCopyBytes     uint64
	PhysicalBytesScanned     int64
	RowMaterializations      int
	DocumentMaterializations int
	DocumentReconstructions  int
}

type typedColumnStringPredicateBenchRunner struct {
	parts           []typedColumnStringPredicateBenchPart
	expectedMatches int
	setup           typedColumnStringPredicateBenchDiagnostics
}

type typedColumnStringPredicateBenchPart struct {
	part              *typedcolumn.ColumnPart
	column            string
	targetCode        uint32
	targetPresent     bool
	rows              int
	dictionaryBytes   uint64
	manifestBytes     uint64
	heapCopyBytes     uint64
	physicalBytes     int64
	decodedBlockBytes uint64
}

type typedColumnStringPredicateBenchScratch struct {
	reader  typedcolumn.GranuleReader
	codes   []uint32
	matches []int
}

func (r *typedColumnStringPredicateBenchRunner) scan(scratch *typedColumnStringPredicateBenchScratch) (typedColumnStringPredicateBenchDiagnostics, error) {
	if scratch == nil {
		return typedColumnStringPredicateBenchDiagnostics{}, fmt.Errorf("collections: nil string predicate benchmark scratch")
	}
	diag := r.setup
	for i := range r.parts {
		part := &r.parts[i]
		diag.PartsConsidered++
		if !part.targetPresent {
			diag.PartsPruned++
			continue
		}
		decodedBlocksBefore := diag.BlocksDecoded
		if err := scanTypedColumnStringPredicateBenchPart(part, scratch, &diag); err != nil {
			return diag, err
		}
		if diag.BlocksDecoded == decodedBlocksBefore {
			diag.PartsPruned++
		} else {
			diag.PartsDecoded++
		}
	}
	return diag, nil
}

func scanTypedColumnStringPredicateBenchPart(part *typedColumnStringPredicateBenchPart, scratch *typedColumnStringPredicateBenchScratch, diag *typedColumnStringPredicateBenchDiagnostics) error {
	valueCol, ok := part.part.Columns[part.column]
	if !ok {
		return fmt.Errorf("collections: string predicate benchmark missing column %q", part.column)
	}
	if valueCol.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || valueCol.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return fmt.Errorf("collections: string predicate benchmark column %q type=%s encoding=%s", part.column, valueCol.Definition.Type, valueCol.Definition.Encoding)
	}
	for _, block := range valueCol.Blocks {
		diag.BlocksConsidered++
		g := block.Granule
		if g.HasMinMax && (int64(part.targetCode) < g.Min || int64(part.targetCode) > g.Max) {
			diag.BlocksPruned++
			continue
		}
		codes, err := scratch.reader.DecodeUint32CodesInto(scratch.codes[:0], g)
		if err != nil {
			return err
		}
		scratch.codes = codes
		if len(codes) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: string predicate benchmark decoded rows=%d want %d", len(codes), block.Descriptor.RowCount)
		}
		diag.BlocksDecoded++
		diag.DecodedHeapCopyBytes += uint64(g.RawBytes)
		diag.RowsScanned += len(codes)
		for rowOffset, code := range codes {
			if code != part.targetCode {
				continue
			}
			scratch.matches = append(scratch.matches, block.Descriptor.FirstRow+rowOffset)
			diag.RowsMatched++
			diag.CodesMatched++
		}
	}
	return nil
}

func prepareTypedColumnStringPredicateBenchRunner(tb testing.TB, d *backenddb.DB, col *Collection, target string) *typedColumnStringPredicateBenchRunner {
	tb.Helper()
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col))
	if len(refs) == 0 {
		tb.Fatalf("missing typed_column_part refs")
	}
	fields := []TypedStorageField{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart}}
	runner := &typedColumnStringPredicateBenchRunner{parts: make([]typedColumnStringPredicateBenchPart, 0, len(refs))}
	for _, ref := range refs {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
		if err != nil {
			tb.Fatalf("read typed_column_part generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			tb.Fatalf("ParseColumnPartImage generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		if image.PartID != ref.PartID {
			tb.Fatalf("typed_column_part image part_id=%d ref part_id=%d", image.PartID, ref.PartID)
		}
		adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields}, image)
		if err != nil {
			tb.Fatalf("typedColumnAdapterPartFromImageWithoutRowLocators generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		adapterColumn, ok := adapterPart.columnByName("kind")
		if !ok {
			tb.Fatalf("missing kind adapter column")
		}
		code, present := adapterColumn.Dictionary[target]
		if code < 0 || code > int64(^uint32(0)) {
			tb.Fatalf("target code=%d outside uint32", code)
		}
		part := typedColumnStringPredicateBenchPart{
			part:            adapterPart.Part,
			column:          adapterColumn.Definition.Name,
			targetCode:      uint32(code),
			targetPresent:   present,
			rows:            image.Rows,
			dictionaryBytes: typedColumnStringPredicateBenchDictionaryBytes(adapterColumn.Dictionary),
			manifestBytes:   uint64(image.ManifestBytes),
			heapCopyBytes:   uint64(len(raw)),
			physicalBytes:   int64(len(raw)),
		}
		if present {
			part.decodedBlockBytes = typedColumnStringPredicateBenchDecodedBytes(adapterPart.Part.Columns[adapterColumn.Definition.Name])
			runner.expectedMatches += typedColumnStringPredicateBenchExpectedMatches(adapterPart.Part.Columns[adapterColumn.Definition.Name], part.targetCode)
		}
		runner.parts = append(runner.parts, part)
		runner.setup.DictionaryBytesDecoded += part.dictionaryBytes
		runner.setup.HeapCopyBytes += part.heapCopyBytes
		runner.setup.PhysicalBytesScanned += part.physicalBytes
	}
	return runner
}

func typedColumnStringPredicateBenchExpectedMatches(column typedcolumn.ColumnPartColumn, target uint32) int {
	var reader typedcolumn.GranuleReader
	var codes []uint32
	matches := 0
	for _, block := range column.Blocks {
		if block.Granule.HasMinMax && (int64(target) < block.Granule.Min || int64(target) > block.Granule.Max) {
			continue
		}
		var err error
		codes, err = reader.DecodeUint32CodesInto(codes[:0], block.Granule)
		if err != nil {
			panic(err)
		}
		for _, code := range codes {
			if code == target {
				matches++
			}
		}
	}
	return matches
}

func typedColumnStringPredicateBenchDecodedBytes(column typedcolumn.ColumnPartColumn) uint64 {
	var total uint64
	for _, block := range column.Blocks {
		total += uint64(block.Granule.RawBytes)
	}
	return total
}

func typedColumnStringPredicateBenchDictionaryBytes(dict map[string]int64) uint64 {
	var total uint64
	for value := range dict {
		total += uint64(len(value))
	}
	return total
}

func runTypedColumnStringPredicateBenchDocumentFallback(col *Collection, rows int, target string) (typedColumnStringPredicateBenchDiagnostics, error) {
	needle := []byte(`"kind":"` + target + `"`)
	diag := typedColumnStringPredicateBenchDiagnostics{}
	truncated, err := col.ScanDocumentsFunc(rows, func(record DocumentRecord) (bool, error) {
		diag.RowsScanned++
		diag.RowMaterializations++
		diag.DocumentMaterializations++
		diag.DocumentReconstructions++
		diag.PhysicalBytesScanned += int64(len(record.Document))
		if bytes.Contains(record.Document, needle) {
			diag.RowsMatched++
		}
		return true, nil
	})
	if err != nil {
		return diag, err
	}
	if truncated {
		return diag, fmt.Errorf("collections: string predicate benchmark fallback truncated at rows=%d", rows)
	}
	return diag, nil
}

func reportTypedColumnStringPredicateBenchMetrics(b *testing.B, diag typedColumnStringPredicateBenchDiagnostics, elapsed time.Duration, iterations int) {
	b.Helper()
	if elapsed > 0 && iterations > 0 {
		b.ReportMetric(float64(iterations)/elapsed.Seconds(), "ops/sec")
		b.ReportMetric(float64(diag.RowsScanned*iterations)/elapsed.Seconds(), "rows/sec")
		b.ReportMetric(float64(diag.RowsMatched*iterations)/elapsed.Seconds(), "matches/sec")
	}
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(diag.PartsConsidered), "parts_considered/op")
	b.ReportMetric(float64(diag.PartsPruned), "parts_pruned/op")
	b.ReportMetric(float64(diag.PartsDecoded), "parts_decoded/op")
	b.ReportMetric(float64(diag.BlocksConsidered), "blocks_considered/op")
	b.ReportMetric(float64(diag.BlocksPruned), "blocks_pruned/op")
	b.ReportMetric(float64(diag.BlocksDecoded), "blocks_decoded/op")
	b.ReportMetric(float64(diag.CodesMatched), "codes_matched/op")
	b.ReportMetric(float64(diag.DictionaryBytesDecoded), "dictionary_bytes_decoded/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.DecodedHeapCopyBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.RowMaterializations), "row_materializations/op")
	b.ReportMetric(float64(diag.DocumentMaterializations), "document_materializations/op")
	b.ReportMetric(float64(diag.DocumentReconstructions), "document_reconstructions/op")
}

func setupTypedColumnStringPredicateBenchCollection(tb testing.TB, typedPath bool) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	cfg := testColumnStoreConfig(nil)
	kindOwner := TypedStorageOwnerRowAsset
	if typedPath {
		kindOwner = TypedStorageOwnerColumnPart
	}
	cfg.Columns = []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: kindOwner, Dictionary: true},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col
}

func insertTypedColumnStringPredicateBenchRows(tb testing.TB, col *Collection, start int, kinds []string) {
	tb.Helper()
	ids := make([][]byte, len(kinds))
	docs := make([][]byte, len(kinds))
	for i, kind := range kinds {
		row := start + i
		ids[i] = []byte(fmt.Sprintf("s%09d", row))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"%s","payload":"payload_%04d"}`, row, kind, row%4096))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
}

func typedColumnStringPredicateBenchKinds(rows int, matchKind, otherKind string, matchEvery int) []string {
	if matchEvery <= 0 {
		matchEvery = rows + 1
	}
	out := make([]string, rows)
	for i := range out {
		if i%matchEvery == 0 {
			out[i] = matchKind
		} else {
			out[i] = otherKind
		}
	}
	return out
}

func typedColumnStringPredicateBenchRowsPerPart(b *testing.B) int {
	b.Helper()
	env := strings.TrimSpace(os.Getenv("TREEDB_TYPED_COLUMN_STRING_BENCH_ROWS_PER_PART"))
	if env == "" {
		return 4096
	}
	rows, err := strconv.Atoi(env)
	if err != nil || rows <= 0 {
		b.Fatalf("TREEDB_TYPED_COLUMN_STRING_BENCH_ROWS_PER_PART=%q must be positive integer", env)
	}
	return rows
}

func maxIntForTypedColumnStringPredicateBench(a, b int) int {
	if a > b {
		return a
	}
	return b
}
