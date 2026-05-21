package colgranule

import (
	"slices"
	"strings"
	"testing"
)

func TestSortKeyMarkPrefixSummaries(t *testing.T) {
	mark, err := BuildSortKeyMark([]SortKeyColumnValues{
		{Name: "collection", Values: []int64{1, 1, 1, 2, 2}},
		{Name: "time_us", Values: []int64{10, 20, 30, 5, 15}},
	})
	if err != nil {
		t.Fatalf("BuildSortKeyMark: %v", err)
	}
	if mark.Rows != 5 || !slices.Equal(mark.Columns, []string{"collection", "time_us"}) {
		t.Fatalf("mark rows/columns=(%d,%v) want (5,[collection time_us])", mark.Rows, mark.Columns)
	}
	if !slices.Equal(mark.Prefixes[0].Lower.Values, []int64{1}) || !slices.Equal(mark.Prefixes[0].UpperExclusive.Values, []int64{3}) {
		t.Fatalf("prefix 1 bounds=%v/%v want [1]/[3]", mark.Prefixes[0].Lower.Values, mark.Prefixes[0].UpperExclusive.Values)
	}
	if !slices.Equal(mark.Prefixes[1].Lower.Values, []int64{1, 10}) || !slices.Equal(mark.Prefixes[1].UpperExclusive.Values, []int64{2, 16}) {
		t.Fatalf("prefix 2 bounds=%v/%v want [1 10]/[2 16]", mark.Prefixes[1].Lower.Values, mark.Prefixes[1].UpperExclusive.Values)
	}
	mayContain, constrained, err := mark.MayContainRanges([]Int64RangePredicate{{Column: "collection", Low: 3, High: 3}})
	if err != nil {
		t.Fatalf("MayContainRanges: %v", err)
	}
	if mayContain || !constrained {
		t.Fatalf("collection=3 mayContain/constrained=(%v,%v) want (false,true)", mayContain, constrained)
	}
	mayContain, constrained, err = mark.MayContainRanges([]Int64RangePredicate{{Column: "time_us", Low: 12, High: 14}})
	if err != nil {
		t.Fatalf("MayContainRanges(time only): %v", err)
	}
	if !mayContain || constrained {
		t.Fatalf("time-only mayContain/constrained=(%v,%v) want (true,false)", mayContain, constrained)
	}
}

func TestSortKeyMarkRejectsDuplicateOrUnsortedColumns(t *testing.T) {
	if _, err := BuildSortKeyMark([]SortKeyColumnValues{
		{Name: "collection", Values: []int64{1, 1, 1}},
		{Name: "collection", Values: []int64{10, 20, 30}},
	}); err == nil {
		t.Fatal("BuildSortKeyMark duplicate columns succeeded, want error")
	}
	if _, err := BuildSortKeyMark([]SortKeyColumnValues{
		{Name: "collection", Values: []int64{1, 1, 1}},
		{Name: "time_us", Values: []int64{10, 9, 11}},
	}); err == nil {
		t.Fatal("BuildSortKeyMark unsorted rows succeeded, want error")
	}
}

func TestEmptySortKeyPredicateIsConstrainedEmpty(t *testing.T) {
	mark, err := BuildSortKeyMark([]SortKeyColumnValues{{Name: "collection", Values: []int64{1, 1, 1}}})
	if err != nil {
		t.Fatalf("BuildSortKeyMark: %v", err)
	}
	mayContain, constrained, err := mark.MayContainRanges([]Int64RangePredicate{{Column: "collection", Low: 2, High: 1}})
	if err != nil {
		t.Fatalf("MayContainRanges: %v", err)
	}
	if mayContain || !constrained {
		t.Fatalf("empty predicate mayContain/constrained=(%v,%v), want (false,true)", mayContain, constrained)
	}
}

func TestCountInt64RangeDiagnostics(t *testing.T) {
	granules, err := buildInt64GranulesForTest(makeSequentialInt64(100), 10)
	if err != nil {
		t.Fatalf("build granules: %v", err)
	}
	var reader GranuleReader
	count, diagnostics, err := reader.CountInt64RangeWithDiagnostics(granules, nil, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 20, High: 29},
	})
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics(partial): %v", err)
	}
	if count != 10 || diagnostics.Considered != 10 || diagnostics.SkippedByMinMax != 9 || diagnostics.Decoded != 1 || diagnostics.Matched != 10 {
		t.Fatalf("partial count=%d diagnostics=%+v", count, diagnostics)
	}

	reader = GranuleReader{}
	count, diagnostics, err = reader.CountInt64RangeWithDiagnostics(granules, nil, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: -10, High: 1000},
	})
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics(no prune): %v", err)
	}
	if count != 100 || diagnostics.SkippedByMinMax != 0 || diagnostics.Decoded != 10 {
		t.Fatalf("no-prune count=%d diagnostics=%+v", count, diagnostics)
	}

	reader = GranuleReader{}
	count, diagnostics, err = reader.CountInt64RangeWithDiagnostics(granules, nil, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 1000, High: 2000},
	})
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics(full prune): %v", err)
	}
	if count != 0 || diagnostics.SkippedByMinMax != 10 || diagnostics.Decoded != 0 {
		t.Fatalf("full-prune count=%d diagnostics=%+v", count, diagnostics)
	}
}

func TestCountInt64RangeRejectsMismatchedMarkMetadata(t *testing.T) {
	granules, err := buildInt64GranulesForTest([]int64{1, 2, 3}, 3)
	if err != nil {
		t.Fatalf("build granules: %v", err)
	}
	var reader GranuleReader
	_, _, err = reader.CountInt64RangeWithDiagnostics(granules, nil, PredicatePlan{
		Filter:        Int64RangePredicate{Column: "time_us", Low: 1, High: 3},
		SortKeyRanges: []Int64RangePredicate{{Column: "collection", Low: 1, High: 1}},
	})
	if err == nil || !strings.Contains(err.Error(), "sort key ranges require marks") {
		t.Fatalf("missing marks err=%v want sort key ranges require marks", err)
	}

	mark, err := BuildSortKeyMark([]SortKeyColumnValues{{Name: "time_us", Values: []int64{1, 2}}})
	if err != nil {
		t.Fatalf("BuildSortKeyMark: %v", err)
	}
	_, _, err = reader.CountInt64RangeWithDiagnostics(granules, []SortKeyMark{mark}, PredicatePlan{
		Filter:        Int64RangePredicate{Column: "time_us", Low: 1, High: 3},
		SortKeyRanges: []Int64RangePredicate{{Column: "time_us", Low: 1, High: 3}},
	})
	if err == nil {
		t.Fatal("CountInt64RangeWithDiagnostics mismatched mark rows succeeded, want error")
	}

	matchingMark, err := BuildSortKeyMark([]SortKeyColumnValues{{Name: "time_us", Values: []int64{1, 2, 3}}})
	if err != nil {
		t.Fatalf("BuildSortKeyMark matching: %v", err)
	}
	_, _, err = reader.CountInt64RangeWithDiagnostics(granules, []SortKeyMark{matchingMark}, PredicatePlan{
		Filter:        Int64RangePredicate{Column: "other_column", Low: 1, High: 3},
		SortKeyRanges: []Int64RangePredicate{{Column: "other_column", Low: 1, High: 3}},
	})
	if err == nil || !strings.Contains(err.Error(), "not present in mark") {
		t.Fatalf("unknown sort key range err=%v want not present in mark", err)
	}

	_, _, err = reader.CountInt64RangeWithDiagnostics(granules, []SortKeyMark{matchingMark}, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 1, High: 3},
		SortKeyRanges: []Int64RangePredicate{
			{Column: "time_us", Low: 1, High: 2},
			{Column: "time_us", Low: 2, High: 3},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate sort key range column") {
		t.Fatalf("duplicate sort key range err=%v want duplicate sort key range column", err)
	}

	count, _, err := reader.CountInt64RangeWithDiagnostics(granules, []SortKeyMark{matchingMark}, PredicatePlan{
		Filter:        Int64RangePredicate{Column: "value", Low: 2, High: 3},
		SortKeyRanges: []Int64RangePredicate{{Column: "time_us", Low: 2, High: 3}},
	})
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics non-sort filter: %v", err)
	}
	if count != 2 {
		t.Fatalf("non-sort filter count=%d want 2", count)
	}
}

func TestSortKeyMarkPruningMatchesRawReference(t *testing.T) {
	granules, marks, collections, times, err := buildCompositeSortFixtureForTest()
	if err != nil {
		t.Fatalf("build fixture: %v", err)
	}
	plan := PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 110, High: 120},
		SortKeyRanges: []Int64RangePredicate{
			{Column: "collection", Low: 1, High: 1},
			{Column: "time_us", Low: 110, High: 120},
		},
	}
	var reader GranuleReader
	count, diagnostics, err := reader.CountInt64RangeWithDiagnostics(granules, marks, plan)
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics: %v", err)
	}
	want := rawCompositeCount(collections, times, 1, 110, 120)
	if count != want {
		t.Fatalf("count=%d want raw reference %d", count, want)
	}
	if diagnostics.SkippedByMark != 3 || diagnostics.Decoded != 1 || diagnostics.Matched != want {
		t.Fatalf("diagnostics=%+v want skipped_by_mark=3 decoded=1 matched=%d", diagnostics, want)
	}
}

func TestSortKeyRangeIsAppliedWithinMixedGranule(t *testing.T) {
	collections := []int64{1, 1, 2, 2}
	times := []int64{110, 111, 110, 111}
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	g, err := builder.BuildInt64(times)
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	mark, err := BuildSortKeyMark([]SortKeyColumnValues{
		{Name: "collection", Values: collections},
		{Name: "time_us", Values: times},
	})
	if err != nil {
		t.Fatalf("BuildSortKeyMark: %v", err)
	}
	var reader GranuleReader
	count, diagnostics, err := reader.CountInt64RangeWithDiagnostics([]EncodedGranule{g}, []SortKeyMark{mark}, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 110, High: 111},
		SortKeyRanges: []Int64RangePredicate{
			{Column: "collection", Low: 1, High: 1},
			{Column: "time_us", Low: 110, High: 111},
		},
	})
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics: %v", err)
	}
	if count != 2 || diagnostics.Decoded != 1 || diagnostics.Matched != 2 {
		t.Fatalf("count=%d diagnostics=%+v want count=2 decoded=1 matched=2", count, diagnostics)
	}
}

func TestTimeOnlyPredicateDoesNotSortKeyPruneWhenNotLeftPrefix(t *testing.T) {
	granules, marks, _, _, err := buildCompositeSortFixtureForTest()
	if err != nil {
		t.Fatalf("build fixture: %v", err)
	}
	var reader GranuleReader
	_, diagnostics, err := reader.CountInt64RangeWithDiagnostics(granules, marks, PredicatePlan{
		Filter:        Int64RangePredicate{Column: "time_us", Low: 110, High: 120},
		SortKeyRanges: []Int64RangePredicate{{Column: "time_us", Low: 110, High: 120}},
	})
	if err != nil {
		t.Fatalf("CountInt64RangeWithDiagnostics: %v", err)
	}
	if diagnostics.SkippedByMark != 0 {
		t.Fatalf("time-only predicate skipped by mark=%d want 0", diagnostics.SkippedByMark)
	}
}

func TestMarkAndMinMaxSkipsDoNotDecodeCorruptPayload(t *testing.T) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	g, err := builder.BuildInt64([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	g.Payload = []byte{0xff}
	g.StoredBytes = len(g.Payload)
	g.PayloadRef.Length = len(g.Payload)
	g.RawBytes = len(g.Payload)
	mark, err := BuildSortKeyMark([]SortKeyColumnValues{
		{Name: "collection", Values: []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{Name: "time_us", Values: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
	})
	if err != nil {
		t.Fatalf("BuildSortKeyMark: %v", err)
	}
	var reader GranuleReader
	count, diagnostics, err := reader.CountInt64RangeWithDiagnostics([]EncodedGranule{g}, []SortKeyMark{mark}, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 0, High: 9},
		SortKeyRanges: []Int64RangePredicate{
			{Column: "collection", Low: 2, High: 2},
			{Column: "time_us", Low: 0, High: 9},
		},
	})
	if err != nil {
		t.Fatalf("mark-pruned corrupt payload returned error: %v", err)
	}
	if count != 0 || diagnostics.SkippedByMark != 1 || diagnostics.Decoded != 0 {
		t.Fatalf("mark-pruned count=%d diagnostics=%+v", count, diagnostics)
	}

	reader = GranuleReader{}
	count, diagnostics, err = reader.CountInt64RangeWithDiagnostics([]EncodedGranule{g}, nil, PredicatePlan{
		Filter: Int64RangePredicate{Column: "time_us", Low: 100, High: 200},
	})
	if err != nil {
		t.Fatalf("minmax-pruned corrupt payload returned error: %v", err)
	}
	if count != 0 || diagnostics.SkippedByMinMax != 1 || diagnostics.Decoded != 0 {
		t.Fatalf("minmax-pruned count=%d diagnostics=%+v", count, diagnostics)
	}
}

func buildInt64GranulesForTest(values []int64, rowsPerGranule int) ([]EncodedGranule, error) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	var granules []EncodedGranule
	for start := 0; start < len(values); start += rowsPerGranule {
		end := start + rowsPerGranule
		if end > len(values) {
			end = len(values)
		}
		g, err := builder.BuildInt64(values[start:end])
		if err != nil {
			return nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		granules = append(granules, owned)
	}
	return granules, nil
}

func makeSequentialInt64(n int) []int64 {
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(i)
	}
	return values
}

func buildCompositeSortFixtureForTest() ([]EncodedGranule, []SortKeyMark, []int64, []int64, error) {
	const rowsPerGranule = 100
	var granules []EncodedGranule
	var marks []SortKeyMark
	var allCollections []int64
	var allTimes []int64
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	for _, collection := range []int64{1, 2} {
		for block := 0; block < 2; block++ {
			collections := make([]int64, rowsPerGranule)
			times := make([]int64, rowsPerGranule)
			for i := 0; i < rowsPerGranule; i++ {
				collections[i] = collection
				times[i] = int64(block*rowsPerGranule + i)
			}
			g, err := builder.BuildInt64(times)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			owned := g
			owned.Payload = append([]byte(nil), g.Payload...)
			mark, err := BuildSortKeyMark([]SortKeyColumnValues{
				{Name: "collection", Values: collections},
				{Name: "time_us", Values: times},
			})
			if err != nil {
				return nil, nil, nil, nil, err
			}
			granules = append(granules, owned)
			marks = append(marks, mark)
			allCollections = append(allCollections, collections...)
			allTimes = append(allTimes, times...)
		}
	}
	return granules, marks, allCollections, allTimes, nil
}

func rawCompositeCount(collections []int64, times []int64, collection int64, low int64, high int64) int {
	count := 0
	for i, gotCollection := range collections {
		if gotCollection == collection && times[i] >= low && times[i] <= high {
			count++
		}
	}
	return count
}
