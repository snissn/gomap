package collections

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
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

func BenchmarkTypedColumnStringPredicatePreparedHot(b *testing.B) {
	rowsPerPart := typedColumnStringPredicateBenchRowsPerPart(b)
	cases := []struct {
		name    string
		kind    TypedColumnStringPredicateScanKind
		targets []string
		part0   []string
		part1   []string
	}{
		{
			name:    "equality_selective",
			targets: []string{"kind_003"},
			part0:   typedColumnStringPredicateBenchKinds(rowsPerPart, "kind_003", "kind_007", maxIntForTypedColumnStringPredicateBench(1, rowsPerPart/16)),
			part1:   typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart),
		},
		{
			name:    "equality_all_match",
			targets: []string{"kind_003"},
			part0:   typedColumnStringPredicateBenchKinds(rowsPerPart, "kind_003", "kind_003", 1),
			part1:   typedColumnStringPredicateBenchKinds(rowsPerPart, "kind_003", "kind_003", 1),
		},
		{
			name:    "equality_all_pruned",
			targets: []string{"kind_003"},
			part0:   typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart),
			part1:   typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_003", "cold_004", rowsPerPart),
		},
		{
			name:    "in_list_category",
			kind:    TypedColumnStringPredicateCategory,
			targets: []string{"kind_003", "kind_007"},
			part0:   typedColumnStringPredicateBenchAlternatingKinds(rowsPerPart, "kind_003", "kind_007", "kind_011"),
			part1:   typedColumnStringPredicateBenchKinds(rowsPerPart, "cold_001", "cold_002", rowsPerPart),
		},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(fmt.Sprintf("rows_%d/path_prepared_dictionary_kernel/%s", rowsPerPart*2, tc.name), func(b *testing.B) {
			d, col := setupTypedColumnStringPredicateBenchCollection(b, true)
			defer func() { _ = d.Close() }()
			insertTypedColumnStringPredicateBenchRows(b, col, 0, tc.part0)
			insertTypedColumnStringPredicateBenchRows(b, col, rowsPerPart, tc.part1)
			runner := prepareTypedColumnStringPredicatePreparedHotRunnerForTargets(b, d, col, tc.kind, tc.targets)
			var scratch typedColumnStringPredicatePreparedHotScratch
			scratch.rows = make([]TypedColumnStringPredicateScanRow, 0, runner.expectedMatches)
			warm, err := runner.scan(&scratch)
			if err != nil {
				b.Fatalf("warm prepared-hot string scan: %v", err)
			}
			if warm.RowsMatched != runner.expectedMatches {
				b.Fatalf("warm rows_matched=%d want %d diagnostics=%+v", warm.RowsMatched, runner.expectedMatches, warm)
			}
			stopHotProfile := startTypedColumnStringPredicateHotProfile(b)
			profileStopped := false
			stopProfile := func() {
				if profileStopped {
					return
				}
				profileStopped = true
				stopHotProfile()
			}
			defer stopProfile()
			b.ReportAllocs()
			b.ResetTimer()
			benchStart := time.Now()
			var diag typedColumnStringPredicateBenchDiagnostics
			for i := 0; i < b.N; i++ {
				diag, err = runner.scan(&scratch)
				if err != nil {
					b.Fatalf("prepared-hot string scan: %v", err)
				}
			}
			b.StopTimer()
			stopProfile()
			if diag.RowsMatched != runner.expectedMatches {
				b.Fatalf("rows_matched=%d want %d diagnostics=%+v", diag.RowsMatched, runner.expectedMatches, diag)
			}
			typedColumnStringPredicateBenchSink = len(scratch.rows)
			reportTypedColumnStringPredicateBenchMetrics(b, diag, time.Since(benchStart), b.N)
		})
	}
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
	SectionBytesRead         uint64
	RangeBytesRead           uint64
	DecodedMetadataBytes     uint64
	KernelBlocks             int
	KernelSelectedBlocks     int
	SelectionEmptyBlocks     int
	SelectionAllBlocks       int
	SelectionRangeBlocks     int
	SelectionRangesBlocks    int
	SelectionBitmapBlocks    int
	SelectionSparseBlocks    int
	SelectionCompositions    int
	SetupDictionaryBytes     uint64
	SetupHeapCopyBytes       uint64
	SetupPhysicalBytes       int64
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
	targetCodes       map[uint32]struct{}
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
		if !typedColumnStringPredicateBenchBlockMayMatch(part, g) {
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
			if !typedColumnStringPredicateBenchCodeMatches(part, code) {
				continue
			}
			scratch.matches = append(scratch.matches, block.Descriptor.FirstRow+rowOffset)
			diag.RowsMatched++
			diag.CodesMatched++
		}
	}
	return nil
}

type typedColumnStringPredicatePreparedHotRunner struct {
	parts           []typedColumnStringPredicatePreparedHotPart
	expectedMatches int
	setup           typedColumnStringPredicateBenchDiagnostics
}

type typedColumnStringPredicatePreparedHotPart struct {
	raw         []byte
	prepared    *typedColumnPreparedPartState
	valueColumn string
	codes       []uint32
	valueByCode map[uint32]string
	found       bool
	generation  uint64
	partID      uint64
}

type typedColumnStringPredicatePreparedHotScratch struct {
	scan typedColumnStringPredicateScanScratch
	rows []TypedColumnStringPredicateScanRow
}

func (r *typedColumnStringPredicatePreparedHotRunner) scan(scratch *typedColumnStringPredicatePreparedHotScratch) (typedColumnStringPredicateBenchDiagnostics, error) {
	if scratch == nil {
		scratch = &typedColumnStringPredicatePreparedHotScratch{}
	}
	result := TypedColumnStringPredicateScanResult{Rows: scratch.rows[:0]}
	for i := range r.parts {
		part := &r.parts[i]
		result.Diagnostics.PartsConsidered++
		if !part.found {
			if preparedColumn := part.prepared.Columns[part.valueColumn]; preparedColumn != nil {
				result.Diagnostics.BlocksConsidered += len(preparedColumn.BlockPlans)
				result.Diagnostics.BlocksPruned += len(preparedColumn.BlockPlans)
			}
			result.Diagnostics.PartsPruned++
			continue
		}
		partPruned, err := scanTypedColumnStringPreparedPartWithVisibility(part.prepared, part.valueColumn, part.codes, part.valueByCode, part.generation, part.partID, &result, nil, part.readRange, &scratch.scan)
		if err != nil {
			return typedColumnStringPredicateBenchDiagnostics{}, err
		}
		if partPruned {
			result.Diagnostics.PartsPruned++
		} else {
			result.Diagnostics.PartsDecoded++
		}
	}
	scratch.rows = result.Rows
	diag := typedColumnStringPredicateBenchDiagnosticsFromStringScan(result.Diagnostics)
	diag.RangeBytesRead = diag.DecodedHeapCopyBytes
	diag.HeapCopyBytes = diag.DecodedHeapCopyBytes
	diag.PhysicalBytesScanned = int64(diag.DecodedHeapCopyBytes)
	diag.SetupDictionaryBytes = r.setup.SetupDictionaryBytes
	diag.SetupHeapCopyBytes = r.setup.SetupHeapCopyBytes
	diag.SetupPhysicalBytes = r.setup.SetupPhysicalBytes
	return diag, nil
}

func (p *typedColumnStringPredicatePreparedHotPart) readRange(offset int, length int, _ bool) ([]byte, error) {
	if offset < 0 || length <= 0 || offset > len(p.raw) || length > len(p.raw)-offset {
		return nil, fmt.Errorf("collections: prepared-hot string benchmark range offset=%d length=%d raw=%d", offset, length, len(p.raw))
	}
	return p.raw[offset : offset+length], nil
}

func prepareTypedColumnStringPredicatePreparedHotRunnerForTargets(tb testing.TB, d *backenddb.DB, col *Collection, kind TypedColumnStringPredicateScanKind, targets []string) *typedColumnStringPredicatePreparedHotRunner {
	tb.Helper()
	if len(targets) == 0 {
		tb.Fatalf("prepared-hot string benchmark requires at least one target")
	}
	meta := col.Meta()
	if meta.Options.ColumnStore == nil {
		tb.Fatalf("prepared-hot string benchmark missing column store config")
	}
	cfg := *meta.Options.ColumnStore
	fields := columnStoreTypedColumnPartFields(cfg)
	adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(fields, "kind")
	if err != nil {
		tb.Fatalf("typedColumnStringPredicateAdapterColumn: %v", err)
	}
	if !ok {
		tb.Fatalf("prepared-hot string benchmark missing kind adapter column")
	}
	op := typedColumnStringPredicateSemanticOperation(TypedColumnStringPredicateScanRequest{Column: "kind", Kind: kind})
	requests := typedColumnStringPreparedColumnRequests(adapterColumn, op)
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col))
	if len(refs) == 0 {
		tb.Fatalf("missing typed_column_part refs")
	}
	runner := &typedColumnStringPredicatePreparedHotRunner{parts: make([]typedColumnStringPredicatePreparedHotPart, 0, len(refs))}
	for _, ref := range refs {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
		if err != nil {
			tb.Fatalf("read typed_column_part generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			tb.Fatalf("ParseColumnPartImage generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		part := typedColumnStringPredicatePreparedHotPart{raw: raw, valueColumn: adapterColumn.Definition.Name, generation: ref.Generation, partID: ref.PartID}
		prepared, partDiag, err := typedColumnPreparePartStateFromRanges(ref, ColumnAssetRef{}, image.Rows, 0, fields, cfg.SchemaHash, requests, part.readRange, nil)
		if err != nil {
			tb.Fatalf("typedColumnPreparePartStateFromRanges generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		if partDiag.Fallback {
			tb.Fatalf("typedColumnPreparePartStateFromRanges generation=%d part_id=%d fallback: %s", ref.Generation, ref.PartID, partDiag.FallbackReason)
		}
		part.prepared = prepared
		preparedColumn := prepared.Columns[adapterColumn.Definition.Name]
		part.codes, part.valueByCode, part.found, err = typedColumnStringResolvePreparedCodes(preparedColumn, targets)
		if err != nil {
			tb.Fatalf("typedColumnStringResolvePreparedCodes generation=%d part_id=%d: %v", ref.Generation, ref.PartID, err)
		}
		runner.parts = append(runner.parts, part)
		runner.setup.SetupDictionaryBytes += uint64(typedColumnPreparedPartDictionaryBytes(prepared))
		runner.setup.SetupHeapCopyBytes += uint64(len(raw))
		runner.setup.SetupPhysicalBytes += int64(len(raw))
	}
	var scratch typedColumnStringPredicatePreparedHotScratch
	warm, err := runner.scan(&scratch)
	if err != nil {
		tb.Fatalf("warm prepared-hot string benchmark scan: %v", err)
	}
	runner.expectedMatches = warm.RowsMatched
	return runner
}

func typedColumnStringPredicateBenchDiagnosticsFromStringScan(src TypedColumnStringPredicateScanDiagnostics) typedColumnStringPredicateBenchDiagnostics {
	return typedColumnStringPredicateBenchDiagnostics{
		RowsScanned:              src.RowsScanned,
		RowsMatched:              src.RowsMatched,
		PartsConsidered:          src.PartsConsidered,
		PartsPruned:              src.PartsPruned,
		PartsDecoded:             src.PartsDecoded,
		BlocksConsidered:         src.BlocksConsidered,
		BlocksPruned:             src.BlocksPruned,
		BlocksDecoded:            src.BlocksDecoded,
		CodesMatched:             src.CodesMatched,
		DictionaryBytesDecoded:   src.DictionaryBytesDecoded,
		MappedBytes:              src.MappedBytes,
		HeapCopyBytes:            src.HeapCopyBytes,
		DecodedHeapCopyBytes:     src.DecodedHeapCopyBytes,
		PhysicalBytesScanned:     src.PhysicalBytesScanned,
		RowMaterializations:      src.RowMaterializations,
		DocumentMaterializations: src.DocumentMaterializations,
		DocumentReconstructions:  src.DocumentReconstructions,
		SectionBytesRead:         src.SectionBytesRead,
		RangeBytesRead:           src.RangeBytesRead,
		DecodedMetadataBytes:     src.DecodedMetadataBytes,
		KernelBlocks:             src.KernelBlocks,
		KernelSelectedBlocks:     src.KernelSelectedBlocks,
		SelectionEmptyBlocks:     src.SelectionEmptyBlocks,
		SelectionAllBlocks:       src.SelectionAllBlocks,
		SelectionRangeBlocks:     src.SelectionRangeBlocks,
		SelectionRangesBlocks:    src.SelectionRangesBlocks,
		SelectionBitmapBlocks:    src.SelectionBitmapBlocks,
		SelectionSparseBlocks:    src.SelectionSparseBlocks,
		SelectionCompositions:    src.SelectionCompositions,
	}
}

func prepareTypedColumnStringPredicateBenchRunner(tb testing.TB, d *backenddb.DB, col *Collection, target string) *typedColumnStringPredicateBenchRunner {
	return prepareTypedColumnStringPredicateBenchRunnerForTargets(tb, d, col, []string{target})
}

func prepareTypedColumnStringPredicateBenchRunnerForTargets(tb testing.TB, d *backenddb.DB, col *Collection, targets []string) *typedColumnStringPredicateBenchRunner {
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
		part := typedColumnStringPredicateBenchPart{
			part:            adapterPart.Part,
			column:          adapterColumn.Definition.Name,
			rows:            image.Rows,
			dictionaryBytes: typedColumnStringPredicateBenchDictionaryBytes(adapterColumn.Dictionary),
			manifestBytes:   uint64(image.ManifestBytes),
			heapCopyBytes:   uint64(len(raw)),
			physicalBytes:   int64(len(raw)),
		}
		for _, target := range targets {
			code, present := adapterColumn.Dictionary[target]
			if !present {
				continue
			}
			if code < 0 || code > int64(^uint32(0)) {
				tb.Fatalf("target code=%d outside uint32", code)
			}
			if !part.targetPresent {
				part.targetCode = uint32(code)
				if len(targets) > 1 {
					part.targetCodes = make(map[uint32]struct{}, len(targets))
				}
			}
			if part.targetCodes != nil {
				part.targetCodes[uint32(code)] = struct{}{}
			}
			part.targetPresent = true
		}
		if part.targetPresent {
			part.decodedBlockBytes = typedColumnStringPredicateBenchDecodedBytes(adapterPart.Part.Columns[adapterColumn.Definition.Name])
			if part.targetCodes == nil {
				runner.expectedMatches += typedColumnStringPredicateBenchExpectedMatches(adapterPart.Part.Columns[adapterColumn.Definition.Name], part.targetCode)
			} else {
				runner.expectedMatches += typedColumnStringPredicateBenchExpectedMatchesForCodes(adapterPart.Part.Columns[adapterColumn.Definition.Name], part.targetCodes)
			}
		}
		runner.parts = append(runner.parts, part)
		runner.setup.DictionaryBytesDecoded += part.dictionaryBytes
		runner.setup.HeapCopyBytes += part.heapCopyBytes
		runner.setup.PhysicalBytesScanned += part.physicalBytes
	}
	return runner
}

func typedColumnStringPredicateBenchExpectedMatches(column typedcolumn.ColumnPartColumn, target uint32) int {
	return typedColumnStringPredicateBenchExpectedMatchesForCodes(column, map[uint32]struct{}{target: {}})
}

func typedColumnStringPredicateBenchExpectedMatchesForCodes(column typedcolumn.ColumnPartColumn, targets map[uint32]struct{}) int {
	var reader typedcolumn.GranuleReader
	var codes []uint32
	matches := 0
	for _, block := range column.Blocks {
		if !typedColumnStringPredicateBenchCodesMayMatch(targets, block.Granule) {
			continue
		}
		var err error
		codes, err = reader.DecodeUint32CodesInto(codes[:0], block.Granule)
		if err != nil {
			panic(err)
		}
		for _, code := range codes {
			if _, ok := targets[code]; ok {
				matches++
			}
		}
	}
	return matches
}

func typedColumnStringPredicateBenchBlockMayMatch(part *typedColumnStringPredicateBenchPart, g typedcolumn.EncodedGranule) bool {
	if !g.HasMinMax {
		return true
	}
	if part.targetCodes == nil {
		return int64(part.targetCode) >= g.Min && int64(part.targetCode) <= g.Max
	}
	return typedColumnStringPredicateBenchCodesMayMatch(part.targetCodes, g)
}

func typedColumnStringPredicateBenchCodesMayMatch(targets map[uint32]struct{}, g typedcolumn.EncodedGranule) bool {
	if !g.HasMinMax {
		return true
	}
	for code := range targets {
		if int64(code) >= g.Min && int64(code) <= g.Max {
			return true
		}
	}
	return false
}

func typedColumnStringPredicateBenchCodeMatches(part *typedColumnStringPredicateBenchPart, code uint32) bool {
	if part.targetCodes == nil {
		return code == part.targetCode
	}
	_, ok := part.targetCodes[code]
	return ok
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

func startTypedColumnStringPredicateHotProfile(b *testing.B) func() {
	b.Helper()
	prefix := strings.TrimSpace(os.Getenv("TREEDB_TYPED_COLUMN_STRING_HOT_PROFILE_PREFIX"))
	if prefix == "" {
		return func() {}
	}
	match := strings.TrimSpace(os.Getenv("TREEDB_TYPED_COLUMN_STRING_HOT_PROFILE_MATCH"))
	if match != "" && !strings.Contains(b.Name(), match) {
		return func() {}
	}
	cpu, err := os.Create(prefix + "_cpu.pprof")
	if err != nil {
		b.Fatalf("create hot cpu profile: %v", err)
	}
	oldRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	runtime.GC()
	if err := pprof.StartCPUProfile(cpu); err != nil {
		runtime.MemProfileRate = oldRate
		_ = cpu.Close()
		b.Fatalf("start hot cpu profile: %v", err)
	}
	return func() {
		defer func() {
			runtime.MemProfileRate = oldRate
		}()
		pprof.StopCPUProfile()
		if err := cpu.Close(); err != nil {
			b.Fatalf("close hot cpu profile: %v", err)
		}
		runtime.GC()
		allocs, err := os.Create(prefix + "_allocs.pprof")
		if err != nil {
			b.Fatalf("create hot allocs profile: %v", err)
		}
		if err := pprof.Lookup("allocs").WriteTo(allocs, 0); err != nil {
			_ = allocs.Close()
			b.Fatalf("write hot allocs profile: %v", err)
		}
		if err := allocs.Close(); err != nil {
			b.Fatalf("close hot allocs profile: %v", err)
		}
	}
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
	b.ReportMetric(float64(diag.DecodedMetadataBytes), "decoded_metadata_bytes/op")
	b.ReportMetric(float64(diag.SectionBytesRead), "section_bytes_read/op")
	b.ReportMetric(float64(diag.RangeBytesRead), "range_bytes_read/op")
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.KernelBlocks), "kernel_blocks/op")
	b.ReportMetric(float64(diag.KernelSelectedBlocks), "kernel_selected_blocks/op")
	b.ReportMetric(float64(diag.SelectionEmptyBlocks), "selection_empty_blocks/op")
	b.ReportMetric(float64(diag.SelectionAllBlocks), "selection_all_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangeBlocks), "selection_range_blocks/op")
	b.ReportMetric(float64(diag.SelectionRangesBlocks), "selection_ranges_blocks/op")
	b.ReportMetric(float64(diag.SelectionBitmapBlocks), "selection_bitmap_blocks/op")
	b.ReportMetric(float64(diag.SelectionSparseBlocks), "selection_sparse_blocks/op")
	b.ReportMetric(float64(diag.SelectionCompositions), "selection_compositions/op")
	b.ReportMetric(float64(diag.SetupDictionaryBytes), "dictionary_bytes/session")
	b.ReportMetric(float64(diag.SetupHeapCopyBytes), "heap_copy_bytes/session")
	b.ReportMetric(float64(diag.SetupPhysicalBytes), "physical_bytes/session")
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

func typedColumnStringPredicateBenchAlternatingKinds(rows int, a, b, other string) []string {
	out := make([]string, rows)
	for i := range out {
		switch i % 4 {
		case 0:
			out[i] = a
		case 1:
			out[i] = b
		default:
			out[i] = other
		}
	}
	return out
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
