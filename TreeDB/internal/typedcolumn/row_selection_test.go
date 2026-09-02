package typedcolumn

import (
	"reflect"
	"strings"
	"testing"
)

func TestRowSelectionShapesCountIterateAndRanges(t *testing.T) {
	all := mustAllSelection(t, 8)
	assertSelectionRows(t, all, []int{0, 1, 2, 3, 4, 5, 6, 7})
	if shape := all.shape(); shape.Kind != "all" || shape.Count != 8 || shape.BitmapWords != 0 {
		t.Fatalf("all shape=%+v", shape)
	}

	empty := mustEmptySelection(t, 8)
	assertSelectionRows(t, empty, nil)

	rangeSel := mustRangeSelection(t, 8, 2, 6)
	assertSelectionRows(t, rangeSel, []int{2, 3, 4, 5})
	assertSelectionRanges(t, rangeSel, []rowRange{{Start: 2, End: 6}})
	if shape := rangeSel.shape(); shape.Kind != "range" || shape.Ranges != 1 || shape.Count != 4 {
		t.Fatalf("range shape=%+v", shape)
	}

	ranges := mustRangesSelection(t, 10, []rowRange{{Start: 1, End: 3}, {Start: 5, End: 7}, {Start: 8, End: 9}})
	assertSelectionRows(t, ranges, []int{1, 2, 5, 6, 8})
	assertSelectionRanges(t, ranges, []rowRange{{Start: 1, End: 3}, {Start: 5, End: 7}, {Start: 8, End: 9}})
	if shape := ranges.shape(); shape.Kind != "ranges" || shape.Ranges != 3 || shape.Count != 5 {
		t.Fatalf("ranges shape=%+v", shape)
	}
	coalesced := mustRangesSelection(t, 10, []rowRange{{Start: 6, End: 8}, {Start: 1, End: 4}, {Start: 3, End: 6}})
	assertSelectionRows(t, coalesced, []int{1, 2, 3, 4, 5, 6, 7})
	assertSelectionRanges(t, coalesced, []rowRange{{Start: 1, End: 8}})

	bitmap := mustBitmapSelection(t, 130, []int{0, 2, 64, 129})
	assertSelectionRows(t, bitmap, []int{0, 2, 64, 129})
	if shape := bitmap.shape(); shape.Kind != "bitmap" || shape.BitmapWords != 3 || shape.Count != 4 {
		t.Fatalf("bitmap shape=%+v", shape)
	}

	sparse := mustSparseSelection(t, 20, []int{1, 4, 9, 19})
	assertSelectionRows(t, sparse, []int{1, 4, 9, 19})
	if shape := sparse.shape(); shape.Kind != "sparse" || shape.SparseRows != 4 || shape.Count != 4 {
		t.Fatalf("sparse shape=%+v", shape)
	}
}

func TestRowSelectionSetOperationsAndComposition(t *testing.T) {
	predicate := mustRangesSelection(t, 12, []rowRange{{Start: 2, End: 10}})
	visibility := mustRangesSelection(t, 12, []rowRange{{Start: 0, End: 6}, {Start: 8, End: 12}})
	deletes := mustSparseSelection(t, 12, []int{3, 8})
	nulls := mustRangeSelection(t, 12, 5, 6)
	defaults := mustEmptySelection(t, 12)

	andSel, err := andRowSelections(predicate, visibility)
	if err != nil {
		t.Fatalf("andRowSelections: %v", err)
	}
	assertSelectionRows(t, andSel, []int{2, 3, 4, 5, 8, 9})

	orSel, err := orRowSelections(mustRangeSelection(t, 12, 0, 2), mustRangeSelection(t, 12, 2, 4))
	if err != nil {
		t.Fatalf("orRowSelections adjacent: %v", err)
	}
	if shape := orSel.shape(); shape.Kind != "range" {
		t.Fatalf("adjacent OR shape=%+v want range", shape)
	}
	assertSelectionRows(t, orSel, []int{0, 1, 2, 3})
	overlapOr, err := orRowSelections(mustRangeSelection(t, 12, 0, 6), mustRangeSelection(t, 12, 3, 9))
	if err != nil {
		t.Fatalf("orRowSelections overlap: %v", err)
	}
	if shape := overlapOr.shape(); shape.Kind != "range" {
		t.Fatalf("overlap OR shape=%+v want range", shape)
	}
	assertSelectionRows(t, overlapOr, []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

	notSel, err := notRowSelection(mustRangesSelection(t, 8, []rowRange{{Start: 2, End: 4}, {Start: 6, End: 7}}))
	if err != nil {
		t.Fatalf("notRowSelection: %v", err)
	}
	assertSelectionRows(t, notSel, []int{0, 1, 4, 5, 7})

	composed, err := composeRowSelections(12, rowSelectionComponents{
		Predicate:  &predicate,
		Visibility: &visibility,
		Deletes:    &deletes,
		Nulls:      &nulls,
		Defaults:   &defaults,
	})
	if err != nil {
		t.Fatalf("composeRowSelections: %v", err)
	}
	assertSelectionRows(t, composed, []int{2, 4, 9})
}

func TestRowSelectionScratchCompositionMatchesAllocatingComposition(t *testing.T) {
	predicate := mustRangesSelection(t, 12, []rowRange{{Start: 0, End: 8}, {Start: 9, End: 12}})
	visibility := mustRangesSelection(t, 12, []rowRange{{Start: 0, End: 12}})
	deletes := mustSparseSelection(t, 12, []int{8})
	nulls := mustRangeSelection(t, 12, 9, 10)
	defaults := mustRangeSelection(t, 12, 10, 11)
	components := rowSelectionComponents{Predicate: &predicate, Visibility: &visibility, Deletes: &deletes, Nulls: &nulls, Defaults: &defaults}
	want, err := ComposeRowSelections(12, components)
	if err != nil {
		t.Fatalf("ComposeRowSelections: %v", err)
	}
	assertSelectionRows(t, want, []int{0, 1, 2, 3, 4, 5, 6, 7, 11})
	var scratch RowSelectionScratch
	for i := 0; i < 3; i++ {
		got, err := ComposeRowSelectionsInto(12, components, &scratch)
		if err != nil {
			t.Fatalf("ComposeRowSelectionsInto iter %d: %v", i, err)
		}
		if !reflect.DeepEqual(got.appendRows(nil), want.appendRows(nil)) {
			t.Fatalf("iter %d rows=%v want %v shape=%+v", i, got.appendRows(nil), want.appendRows(nil), got.shape())
		}
	}
}

func TestRowSelectionFailClosedOnMaskRowMismatch(t *testing.T) {
	predicate := mustAllSelection(t, 5)
	visibility := mustAllSelection(t, 6)
	got, err := composeRowSelections(5, rowSelectionComponents{Predicate: &predicate, Visibility: &visibility})
	if err == nil || !strings.Contains(err.Error(), "visibility mask row mismatch") {
		t.Fatalf("compose mismatch err=%v", err)
	}
	if got.countRows() != 0 || got.shape().Kind != "empty" || got.rowsCount() != 5 {
		t.Fatalf("fail-closed selection=%+v shape=%+v", got, got.shape())
	}

	other := mustAllSelection(t, 4)
	got, err = andRowSelections(predicate, other)
	if err == nil || !strings.Contains(err.Error(), "rows mismatch") {
		t.Fatalf("and mismatch err=%v", err)
	}
	if got.countRows() != 0 {
		t.Fatalf("and fail-closed count=%d", got.countRows())
	}
}

func TestRowSelectionDoesNotMaterializeBitmapForAllOrRange(t *testing.T) {
	allWords := []uint64{^uint64(0), (uint64(1) << 6) - 1}
	all, err := makeBitmapRowSelection(70, allWords)
	if err != nil {
		t.Fatalf("makeBitmapRowSelection all: %v", err)
	}
	if shape := all.shape(); shape.Kind != "all" || shape.BitmapWords != 0 {
		t.Fatalf("all bitmap shape=%+v", shape)
	}

	words := bitmapWordsForRows(70, []int{10, 11, 12, 13})
	rangeSel, err := makeBitmapRowSelection(70, words)
	if err != nil {
		t.Fatalf("makeBitmapRowSelection range: %v", err)
	}
	if shape := rangeSel.shape(); shape.Kind != "range" || shape.BitmapWords != 0 {
		t.Fatalf("range bitmap shape=%+v", shape)
	}
}

func TestRowSelectionIterationAllocationEvidence(t *testing.T) {
	selection := mustRangeSelection(t, 1024, 128, 896)
	var sum int
	allocs := testing.AllocsPerRun(1000, func() {
		sum = 0
		selection.forEach(func(row int) {
			sum += row
		})
	})
	if allocs != 0 {
		t.Fatalf("range iteration allocs/run=%v want 0", allocs)
	}
	if sum == 0 {
		t.Fatalf("unexpected zero sum")
	}

	rows := make([]int, 0, selection.countRows())
	allocs = testing.AllocsPerRun(1000, func() {
		rows = selection.appendRows(rows[:0])
	})
	if allocs != 0 {
		t.Fatalf("appendRows with preallocated dst allocs/run=%v want 0", allocs)
	}
	if len(rows) != selection.countRows() {
		t.Fatalf("appendRows len=%d count=%d", len(rows), selection.countRows())
	}
}

func TestColumnRowAlignmentMatchesAndMismatchesFailClosed(t *testing.T) {
	span := mustRowSpan(t, 100, 50)
	got, ok, err := alignColumnRowSpans([]columnRowDescriptor{
		{Column: "predicate", Type: ColumnTypeBool, Span: span},
		{Column: "measure", Type: ColumnTypeInt64, Span: span},
		{Column: "projection", Type: ColumnTypeLowCardinalityCode, Span: span},
	})
	if err != nil || !ok || got != span {
		t.Fatalf("align ok got=%+v ok=%v err=%v", got, ok, err)
	}

	mismatch := mustRowSpan(t, 101, 50)
	got, ok, err = alignColumnRowSpans([]columnRowDescriptor{
		{Column: "predicate", Type: ColumnTypeBool, Span: span},
		{Column: "measure", Type: ColumnTypeInt64, Span: mismatch},
	})
	if err == nil || ok || got.RowCount != 0 {
		t.Fatalf("align mismatch got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestSectionDependencyDescriptorsCoverRolesAndAlign(t *testing.T) {
	span := mustRowSpan(t, 0, 128)
	deps := []sectionDependencyDescriptor{
		mustSectionDependency(t, sectionDependencyPredicate, "active", ColumnTypeBool, span),
		mustSectionDependency(t, sectionDependencyMeasure, "amount", ColumnTypeInt64, span),
		mustSectionDependency(t, sectionDependencyProjection, "kind", ColumnTypeLowCardinalityCode, span),
		mustSectionDependency(t, sectionDependencyVisibility, "visible", ColumnTypeBool, span),
		mustSectionDependency(t, sectionDependencyNull, "amount.null", ColumnTypeBool, span),
		mustSectionDependency(t, sectionDependencyDefault, "amount.default", ColumnTypeBool, span),
	}
	got, ok, err := validateSectionDependencies(deps)
	if err != nil || !ok || got != span {
		t.Fatalf("validateSectionDependencies got=%+v ok=%v err=%v", got, ok, err)
	}

	deps[2].Span = mustRowSpan(t, 1, 128)
	got, ok, err = validateSectionDependencies(deps)
	if err == nil || ok || got.RowCount != 0 {
		t.Fatalf("validateSectionDependencies mismatch got=%+v ok=%v err=%v", got, ok, err)
	}

	predicateDeps, err := PredicateColumnDependencies("active", ColumnTypeBool, span, true)
	if err != nil {
		t.Fatalf("PredicateColumnDependencies: %v", err)
	}
	seenKinds := make(map[SectionDependencyKind]bool)
	for _, dep := range predicateDeps {
		seenKinds[dep.Kind] = true
	}
	for _, kind := range []SectionDependencyKind{SectionDependencyValues, SectionDependencyPruningMetadata, SectionDependencyNullMask, SectionDependencyDefaultMask} {
		if !seenKinds[kind] {
			t.Fatalf("predicate dependency kinds=%v missing %s", seenKinds, kind)
		}
	}
}

func TestColumnRowAlignmentRejectsAssetIdentityMismatches(t *testing.T) {
	span := mustRowSpan(t, 0, 16)
	_, ok, err := alignColumnRowSpans([]columnRowDescriptor{
		{Column: "predicate", Type: ColumnTypeInt64, Span: span, SnapshotGeneration: 7, AssetGeneration: 10, PartID: 2, SchemaVersion: 99, AlignmentKey: "asset-a"},
		{Column: "measure", Type: ColumnTypeInt64, Span: span, SnapshotGeneration: 7, AssetGeneration: 10, PartID: 2, SchemaVersion: 99, AlignmentKey: "asset-b"},
	})
	if err == nil || ok || !strings.Contains(err.Error(), "asset ref") {
		t.Fatalf("asset mismatch ok=%v err=%v", ok, err)
	}
	_, ok, err = alignColumnRowSpans([]columnRowDescriptor{
		{Column: "predicate", Type: ColumnTypeInt64, Span: span, SnapshotGeneration: 7, AssetGeneration: 10, PartID: 2, SchemaVersion: 99},
		{Column: "measure", Type: ColumnTypeInt64, Span: span, SnapshotGeneration: 8, AssetGeneration: 10, PartID: 2, SchemaVersion: 99},
	})
	if err == nil || ok || !strings.Contains(err.Error(), "snapshot generation") {
		t.Fatalf("snapshot mismatch ok=%v err=%v", ok, err)
	}
}

func TestRowSelectionNonInt64SmokeConsumption(t *testing.T) {
	selection := mustRangesSelection(t, 6, []rowRange{{Start: 1, End: 3}, {Start: 5, End: 6}})

	bools, err := appendSelectedBools(nil, []bool{false, true, true, false, false, true}, selection)
	if err != nil {
		t.Fatalf("appendSelectedBools: %v", err)
	}
	if !reflect.DeepEqual(bools, []bool{true, true, true}) {
		t.Fatalf("bools=%v", bools)
	}

	codes, err := appendSelectedUint32s(nil, []uint32{10, 11, 12, 13, 14, 15}, selection)
	if err != nil {
		t.Fatalf("appendSelectedUint32s: %v", err)
	}
	if !reflect.DeepEqual(codes, []uint32{11, 12, 15}) {
		t.Fatalf("codes=%v", codes)
	}

	_, _, err = validateSectionDependencies([]sectionDependencyDescriptor{
		mustSectionDependency(t, sectionDependencyProjection, "code", ColumnTypeLowCardinalityCode, mustRowSpan(t, 0, 6)),
		mustSectionDependency(t, sectionDependencyPredicate, "flag", ColumnTypeBool, mustRowSpan(t, 0, 6)),
	})
	if err != nil {
		t.Fatalf("non-int64 deps: %v", err)
	}
}

func BenchmarkRowSelectionIterateAll(b *testing.B) {
	selection := mustAllSelection(b, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		selection.forEach(func(row int) {
			count += row & 1
		})
		if count == -1 {
			b.Fatal(count)
		}
	}
}

func BenchmarkRowSelectionIterateRange(b *testing.B) {
	selection := mustRangeSelection(b, 4096, 256, 3840)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		selection.forEach(func(row int) {
			count += row & 1
		})
		if count == -1 {
			b.Fatal(count)
		}
	}
}

func BenchmarkRowSelectionIterateBitmap(b *testing.B) {
	setRows := make([]int, 0, 512)
	for row := 0; row < 4096; row += 8 {
		setRows = append(setRows, row)
	}
	selection := mustBitmapSelection(b, 4096, setRows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		selection.forEach(func(row int) {
			count += row & 1
		})
		if count != 0 {
			b.Fatal(count)
		}
	}
}

func BenchmarkRowSelectionIterateSparse(b *testing.B) {
	setRows := make([]int, 0, 512)
	for row := 0; row < 4096; row += 8 {
		setRows = append(setRows, row)
	}
	selection := mustSparseSelection(b, 4096, setRows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		selection.forEach(func(row int) {
			count += row & 1
		})
		if count != 0 {
			b.Fatal(count)
		}
	}
}

func BenchmarkRowSelectionAppendRowsPreallocated(b *testing.B) {
	selection := mustRangesSelection(b, 4096, []rowRange{{Start: 10, End: 1024}, {Start: 2048, End: 3072}})
	rows := make([]int, 0, selection.countRows())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows = selection.appendRows(rows[:0])
		if len(rows) != selection.countRows() {
			b.Fatal(len(rows))
		}
	}
}

func BenchmarkRowSelectionAndRanges(b *testing.B) {
	left := mustRangesSelection(b, 8192, []rowRange{{Start: 0, End: 2048}, {Start: 4096, End: 8192}})
	right := mustRangesSelection(b, 8192, []rowRange{{Start: 1024, End: 3072}, {Start: 6144, End: 7168}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := andRowSelections(left, right)
		if err != nil || got.countRows() != 2048 {
			b.Fatalf("got count=%d err=%v", got.countRows(), err)
		}
	}
}

func BenchmarkRowSelectionComposeScratchNoAlloc(b *testing.B) {
	predicate := mustRangesSelection(b, 8192, []rowRange{{Start: 0, End: 4096}, {Start: 6144, End: 8192}})
	visibility := mustRangesSelection(b, 8192, []rowRange{{Start: 1024, End: 7168}})
	deletes := mustSparseSelection(b, 8192, []int{2048, 2049, 4096, 4097})
	components := rowSelectionComponents{Predicate: &predicate, Visibility: &visibility, Deletes: &deletes}
	var scratch RowSelectionScratch
	warm, err := ComposeRowSelectionsInto(8192, components, &scratch)
	if err != nil || warm.countRows() == 0 {
		b.Fatalf("warm count=%d err=%v", warm.countRows(), err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := ComposeRowSelectionsInto(8192, components, &scratch)
		if err != nil || got.countRows() != warm.countRows() {
			b.Fatalf("got count=%d warm=%d err=%v", got.countRows(), warm.countRows(), err)
		}
	}
}

func assertSelectionRows(t testing.TB, selection rowSelection, want []int) {
	t.Helper()
	got := selection.appendRows(nil)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows=%v want %v shape=%+v", got, want, selection.shape())
	}
	if selection.countRows() != len(want) {
		t.Fatalf("count=%d want %d", selection.countRows(), len(want))
	}
	for row := 0; row < selection.rowsCount(); row++ {
		wantContains := false
		for _, w := range want {
			if w == row {
				wantContains = true
				break
			}
		}
		if selection.contains(row) != wantContains {
			t.Fatalf("contains(%d)=%v want %v", row, selection.contains(row), wantContains)
		}
	}
}

func assertSelectionRanges(t testing.TB, selection rowSelection, want []rowRange) {
	t.Helper()
	got := selection.appendRanges(nil)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ranges=%v want %v", got, want)
	}
}

func mustEmptySelection(t testing.TB, rows int) rowSelection {
	t.Helper()
	sel, err := makeEmptyRowSelection(rows)
	if err != nil {
		t.Fatalf("makeEmptyRowSelection: %v", err)
	}
	return sel
}

func mustAllSelection(t testing.TB, rows int) rowSelection {
	t.Helper()
	sel, err := makeAllRowSelection(rows)
	if err != nil {
		t.Fatalf("makeAllRowSelection: %v", err)
	}
	return sel
}

func mustRangeSelection(t testing.TB, rows int, start int, end int) rowSelection {
	t.Helper()
	sel, err := makeRangeRowSelection(rows, start, end)
	if err != nil {
		t.Fatalf("makeRangeRowSelection: %v", err)
	}
	return sel
}

func mustRangesSelection(t testing.TB, rows int, ranges []rowRange) rowSelection {
	t.Helper()
	sel, err := makeRangesRowSelection(rows, ranges)
	if err != nil {
		t.Fatalf("makeRangesRowSelection: %v", err)
	}
	return sel
}

func mustSparseSelection(t testing.TB, rows int, sparse []int) rowSelection {
	t.Helper()
	sel, err := makeSparseRowSelection(rows, sparse)
	if err != nil {
		t.Fatalf("makeSparseRowSelection: %v", err)
	}
	return sel
}

func mustBitmapSelection(t testing.TB, rows int, setRows []int) rowSelection {
	t.Helper()
	sel, err := makeBitmapRowSelection(rows, bitmapWordsForRows(rows, setRows))
	if err != nil {
		t.Fatalf("makeBitmapRowSelection: %v", err)
	}
	return sel
}

func mustRowSpan(t testing.TB, firstRow int, rowCount int) rowSpan {
	t.Helper()
	span, err := makeRowSpan(firstRow, rowCount)
	if err != nil {
		t.Fatalf("makeRowSpan: %v", err)
	}
	return span
}

func mustSectionDependency(t testing.TB, role sectionDependencyRole, column string, columnType ColumnType, span rowSpan) sectionDependencyDescriptor {
	t.Helper()
	dep, err := makeValuesSectionDependency(role, column, columnType, ColumnPartImageSectionColumnData, span, true)
	if err != nil {
		t.Fatalf("makeValuesSectionDependency: %v", err)
	}
	return dep
}

func bitmapWordsForRows(rows int, setRows []int) []uint64 {
	words := make([]uint64, rowSelectionBitmapWords(rows))
	for _, row := range setRows {
		words[row/64] |= uint64(1) << uint(row%64)
	}
	return words
}
