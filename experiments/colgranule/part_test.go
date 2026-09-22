package colgranule

import (
	"math"
	"slices"
	"strings"
	"testing"
)

func TestColumnPartBuildPrimaryEqualsSortKey(t *testing.T) {
	batch := ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}}
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if part.Descriptor.RowCount != 5 || part.Descriptor.VisibleRowCount != 5 {
		t.Fatalf("rows/visible=(%d,%d) want (5,5)", part.Descriptor.RowCount, part.Descriptor.VisibleRowCount)
	}
	if len(part.Descriptor.Granules) != 3 || len(part.Marks) != 3 {
		t.Fatalf("granules/marks=(%d,%d) want (3,3)", len(part.Descriptor.Granules), len(part.Marks))
	}
	assertGranulesAligned(t, part, []int{2, 2, 1})

	scan, err := part.NewScanner().ScanProjected([]string{"id", "value", "kind_code", "has_reply"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5})
	assertInt64s(t, "value", scan.Columns["value"], []int64{100, 200, 300, 400, 500})
	assertInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{0, 1, 1, 0, 2})
	assertInt64s(t, "has_reply", scan.Columns["has_reply"], []int64{0, 1, 1, 1, 0})
	if scan.Diagnostics.RowsScanned != 5 || scan.Diagnostics.GranulesConsidered != 3 || scan.Diagnostics.GranulesDecoded != 3 || scan.Diagnostics.BlocksDecoded != 12 {
		t.Fatalf("diagnostics=%+v want rows=5 granules=3 decoded=3 blocks=12", scan.Diagnostics)
	}

	scanner := part.NewScanner()
	for id, wantValue := range map[int64]int64{1: 100, 2: 200, 3: 300, 4: 400, 5: 500} {
		locator, ok := part.LocatePrimaryID(id)
		if !ok {
			t.Fatalf("missing locator for id %d", id)
		}
		got, err := scanner.ValueAt(locator, "value")
		if err != nil {
			t.Fatalf("ValueAt(id=%d): %v", id, err)
		}
		if got != wantValue {
			t.Fatalf("ValueAt(id=%d)=%d want %d locator=%+v", id, got, wantValue, locator)
		}
	}
}

func TestColumnPartBuildPrimaryDiffersFromSortKey(t *testing.T) {
	batch := ColumnBatch{Columns: map[string][]int64{
		"id":        {100, 101, 102, 103, 104},
		"time_us":   {50, 20, 20, 10, 50},
		"value":     {5, 2, 3, 1, 4},
		"kind_code": {0, 1, 2, 1, 0},
		"has_reply": {0, 1, 0, 1, 0},
	}}
	part, err := BuildColumnPart(9, partTestOptions([]SortKeyColumn{{Column: "time_us"}}), batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	scan, err := part.NewScanner().ScanProjected([]string{"id", "time_us", "value"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{103, 101, 102, 100, 104})
	assertInt64s(t, "time_us", scan.Columns["time_us"], []int64{10, 20, 20, 50, 50})
	assertInt64s(t, "value", scan.Columns["value"], []int64{1, 2, 3, 5, 4})

	locator, ok := part.LocatePrimaryID(100)
	if !ok {
		t.Fatal("missing locator for id 100")
	}
	if locator.PartRow != 3 || locator.GranuleOrdinal != 1 || locator.RowInGranule != 1 {
		t.Fatalf("locator=%+v want part row 3 granule 1 row 1", locator)
	}
	mayContain, constrained, err := part.Marks[0].MayContainRanges([]Int64RangePredicate{{Column: "time_us", Low: 45, High: 55}})
	if err != nil {
		t.Fatalf("MayContainRanges: %v", err)
	}
	if !constrained || mayContain {
		t.Fatalf("first mark constrained/mayContain=(%v,%v) want (true,false)", constrained, mayContain)
	}
}

func TestColumnPartCodecBlocksCanSplitIndependently(t *testing.T) {
	batch := ColumnBatch{Columns: map[string][]int64{
		"id":        {10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		"time_us":   {10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		"value":     {100, 90, 80, 70, 60, 50, 40, 30, 20, 10},
		"kind_code": {1, 0, 1, 2, 1, 0, 2, 1, 0, 2},
		"has_reply": {0, 1, 0, 1, 0, 1, 0, 1, 0, 1},
	}}
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.PartPolicy.RowsPerGranule = 3
	opts.Columns[0].CodecBlockRows = 2
	opts.Columns[1].CodecBlockRows = 4
	opts.Columns[2].CodecBlockRows = 5
	opts.Columns[3].CodecBlockRows = 6
	opts.Columns[4].CodecBlockRows = 7
	part, err := BuildColumnPart(11, opts, batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	assertColumnBlockRows(t, part, "id", []int{2, 2, 2, 2, 2})
	assertColumnBlockRows(t, part, "time_us", []int{4, 4, 2})
	assertColumnBlockRows(t, part, "value", []int{5, 5})
	assertColumnBlockRows(t, part, "kind_code", []int{6, 4})
	assertColumnBlockRows(t, part, "has_reply", []int{7, 3})

	scan, err := part.NewScanner().ScanProjected([]string{"id", "time_us", "value", "kind_code", "has_reply"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	assertInt64s(t, "time_us", scan.Columns["time_us"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	assertInt64s(t, "value", scan.Columns["value"], []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
	assertInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{2, 0, 1, 2, 0, 1, 2, 1, 0, 1})
	assertInt64s(t, "has_reply", scan.Columns["has_reply"], []int64{1, 0, 1, 0, 1, 0, 1, 0, 1, 0})
	if scan.Diagnostics.GranulesDecoded != 4 || scan.Diagnostics.BlocksDecoded != 14 {
		t.Fatalf("diagnostics=%+v want decoded=4 blocks=14", scan.Diagnostics)
	}
}

func TestColumnPartPreservesExplicitCompressionNoneOverride(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.Compression.Default = CompressionLZ4
	opts.Columns[2].Compression = CompressionNone
	opts.Columns[2].CompressionSet = true
	part, err := BuildColumnPart(13, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2, 3},
		"time_us":   {10, 20, 30},
		"value":     {100, 200, 300},
		"kind_code": {0, 1, 2},
		"has_reply": {0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if got := part.Columns["id"].Definition.Compression; got != CompressionLZ4 {
		t.Fatalf("id compression=%s want default %s", got, CompressionLZ4)
	}
	if got := part.Columns["value"].Definition.Compression; got != CompressionNone {
		t.Fatalf("value compression=%s want explicit %s", got, CompressionNone)
	}
}

func TestScanProjectedIntoDropsStaleColumns(t *testing.T) {
	part, err := BuildColumnPart(14, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2, 3},
		"time_us":   {10, 20, 30},
		"value":     {100, 200, 300},
		"kind_code": {0, 1, 2},
		"has_reply": {0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	scanner := part.NewScanner()
	dst := map[string][]int64{"stale": {999}}
	scan, err := scanner.ScanProjectedInto(dst, []string{"id", "value"})
	if err != nil {
		t.Fatalf("ScanProjectedInto(id,value): %v", err)
	}
	if _, ok := scan.Columns["stale"]; ok {
		t.Fatalf("stale projection key remained after first scan: %v", scan.Columns)
	}
	if _, ok := scan.Columns["value"]; !ok {
		t.Fatalf("value projection missing after first scan: %v", scan.Columns)
	}
	scan, err = scanner.ScanProjectedInto(dst, []string{"id"})
	if err != nil {
		t.Fatalf("ScanProjectedInto(id): %v", err)
	}
	if _, ok := scan.Columns["value"]; ok {
		t.Fatalf("narrow projection retained stale value column: %v", scan.Columns)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3})
}

func TestScanProjectedIntoValidatesProjectionBeforeMutatingDestination(t *testing.T) {
	part, err := BuildColumnPart(15, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2, 3},
		"time_us":   {10, 20, 30},
		"value":     {100, 200, 300},
		"kind_code": {0, 1, 2},
		"has_reply": {0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	scanner := part.NewScanner()
	dst := map[string][]int64{"stale": {999}}
	if _, err := scanner.ScanProjectedInto(dst, []string{"id", "missing"}); err == nil || !strings.Contains(err.Error(), "missing column") {
		t.Fatalf("ScanProjectedInto missing column err=%v want missing column", err)
	}
	if got, ok := dst["stale"]; !ok || !slices.Equal(got, []int64{999}) || len(dst) != 1 {
		t.Fatalf("dst mutated after missing projection: %v", dst)
	}
	if _, err := scanner.ScanProjectedInto(dst, []string{"id", "id"}); err == nil || !strings.Contains(err.Error(), "duplicate projection column") {
		t.Fatalf("ScanProjectedInto duplicate column err=%v want duplicate projection column", err)
	}
	if got, ok := dst["stale"]; !ok || !slices.Equal(got, []int64{999}) || len(dst) != 1 {
		t.Fatalf("dst mutated after duplicate projection: %v", dst)
	}
}

func TestColumnPartOptionsAreNotMutatedByNormalization(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.Compression.Default = CompressionLZ4
	part, err := BuildColumnPart(16, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2, 3},
		"time_us":   {10, 20, 30},
		"value":     {100, 200, 300},
		"kind_code": {0, 1, 2},
		"has_reply": {0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if got := part.Columns["id"].Definition.Compression; got != CompressionLZ4 {
		t.Fatalf("normalized id compression=%s want %s", got, CompressionLZ4)
	}
	if opts.Columns[0].Compression != CompressionNone {
		t.Fatalf("caller options column compression mutated to %s", opts.Columns[0].Compression)
	}
	if opts.SortKey.Columns[0].Direction != "" {
		t.Fatalf("caller sort key direction mutated to %s", opts.SortKey.Columns[0].Direction)
	}
}

func TestColumnPartBuilderFailsClosedOnInvalidShape(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	_, err := BuildColumnPart(1, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 1},
		"time_us":   {10, 20},
		"value":     {100, 200},
		"kind_code": {0, 1},
		"has_reply": {0, 1},
	}})
	if err == nil || !strings.Contains(err.Error(), "duplicate primary id") {
		t.Fatalf("duplicate primary err=%v want duplicate primary id", err)
	}
	_, err = BuildColumnPart(1, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2},
		"time_us":   {10},
		"value":     {100, 200},
		"kind_code": {0, 1},
		"has_reply": {0, 1},
	}})
	if err == nil || !strings.Contains(err.Error(), "rows=1 want=2") {
		t.Fatalf("row mismatch err=%v want rows mismatch", err)
	}
	_, err = BuildColumnPart(1, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2},
		"time_us":   {10, 20},
		"value":     {100, 200},
		"kind_code": {0, 9},
		"has_reply": {0, 1},
	}})
	if err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("cardinality err=%v want outside cardinality", err)
	}
	_, err = BuildColumnPart(1, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2},
		"time_us":   {10, 20},
		"value":     {100, 200},
		"kind_code": {0, 1},
		"has_reply": {0, 2},
	}})
	if err == nil || !strings.Contains(err.Error(), "bool value") {
		t.Fatalf("bool err=%v want bool value", err)
	}
	_, err = BuildColumnPart(1, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {math.MaxInt64},
		"time_us":   {10},
		"value":     {100},
		"kind_code": {0},
		"has_reply": {0},
	}})
	if err == nil || !strings.Contains(err.Error(), "cannot form exclusive upper bound") {
		t.Fatalf("max primary id err=%v want exclusive upper bound failure", err)
	}

	invalidDefault := opts
	invalidDefault.Compression.Default = Compression(255)
	if _, err := NewColumnPartBuilder(invalidDefault); err == nil || !strings.Contains(err.Error(), "unsupported default compression") {
		t.Fatalf("invalid default compression err=%v want unsupported default compression", err)
	}

	invalidColumn := opts
	invalidColumn.Columns[0].Compression = Compression(255)
	invalidColumn.Columns[0].CompressionSet = true
	if _, err := NewColumnPartBuilder(invalidColumn); err == nil || !strings.Contains(err.Error(), "unsupported compression") {
		t.Fatalf("invalid column compression err=%v want unsupported compression", err)
	}
}

func partTestOptions(sortKey []SortKeyColumn) ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "time_us", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Compression: CompressionNone, Cardinality: 3},
			{Name: "has_reply", Type: ColumnTypeBool, Compression: CompressionNone},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: sortKey},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
	}
}

func assertGranulesAligned(t *testing.T, part *ColumnPart, rowCounts []int) {
	t.Helper()
	for i, wantRows := range rowCounts {
		g := part.Descriptor.Granules[i]
		if g.Ordinal != i || g.MarkOrdinal != i || g.RowCount != wantRows {
			t.Fatalf("granule[%d]=%+v want ordinal=%d mark=%d rows=%d", i, g, i, i, wantRows)
		}
	}
	for _, column := range part.Columns {
		for _, block := range column.Blocks {
			if block.Descriptor.FirstGranule < 0 || block.Descriptor.LastGranule >= len(rowCounts) || block.Descriptor.FirstGranule > block.Descriptor.LastGranule {
				t.Fatalf("block descriptor=%+v has invalid granule range", block.Descriptor)
			}
		}
	}
}

func assertColumnBlockRows(t *testing.T, part *ColumnPart, columnName string, want []int) {
	t.Helper()
	column := part.Columns[columnName]
	got := make([]int, len(column.Blocks))
	for i, block := range column.Blocks {
		got[i] = block.Descriptor.RowCount
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s block rows=%v want %v", columnName, got, want)
	}
}

func assertInt64s(t *testing.T, name string, got []int64, want []int64) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s=%v want %v", name, got, want)
	}
}
