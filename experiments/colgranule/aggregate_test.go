package colgranule

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestGroupedCountCodesMatchesRawAndEmptyGroups(t *testing.T) {
	codeSets := [][]uint32{
		{0, 1, 1, 2, 4, 4},
		{2, 2, 3, 4, 4, 4},
	}
	granules, err := buildCodeGranulesForTest(codeSets, 6)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	var arena AggregateArena
	counts, err := arena.GroupedCountCodes(granules, 6)
	if err != nil {
		t.Fatalf("GroupedCountCodes: %v", err)
	}
	want := []uint64{1, 2, 3, 1, 5, 0}
	if !slices.Equal(counts, want) {
		t.Fatalf("counts=%v want %v", counts, want)
	}
}

func TestFilteredGroupedCountCodesMatchesRaw(t *testing.T) {
	codeSets := [][]uint32{
		{0, 1, 1, 2, 3, 3, 0, 1, 2, 2},
		{1, 2, 3, 3, 0, 0, 1, 2, 2, 3},
	}
	timeSets := [][]int64{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
	}
	codeGranules, err := buildCodeGranulesForTest(codeSets, 4)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	timeGranules, err := buildInt64GranulesFromSetsForTest(timeSets)
	if err != nil {
		t.Fatalf("build time granules: %v", err)
	}
	filter := Int64RangePredicate{Column: "time_us", Low: 5, High: 14}
	var arena AggregateArena
	counts, diagnostics, err := arena.FilteredGroupedCountCodes(codeGranules, timeGranules, filter, 4)
	if err != nil {
		t.Fatalf("FilteredGroupedCountCodes: %v", err)
	}
	want := rawFilteredCodeCounts(codeSets, timeSets, filter, 4)
	if !slices.Equal(counts, want) {
		t.Fatalf("counts=%v want %v", counts, want)
	}
	if diagnostics.Considered != 2 || diagnostics.Decoded != 2 || diagnostics.Matched != 10 {
		t.Fatalf("diagnostics=%+v want considered=2 decoded=2 matched=10", diagnostics)
	}
}

func TestAggregateKernelsFailClosedOnShapeMismatch(t *testing.T) {
	codeGranules, err := buildCodeGranulesForTest([][]uint32{{0, 1, 2, 3}}, 4)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	var arena AggregateArena
	if _, err := arena.GroupedCountCodes(codeGranules, 2); err == nil || !strings.Contains(err.Error(), "outside counts") {
		t.Fatalf("GroupedCountCodes small cardinality err=%v want outside counts", err)
	}
	if _, err := arena.ExactDistinctCodes(codeGranules, 2); err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("ExactDistinctCodes small cardinality err=%v want outside cardinality", err)
	}

	filterGranules, err := buildInt64GranulesFromSetsForTest([][]int64{{0, 1, 2}})
	if err != nil {
		t.Fatalf("build filter granules: %v", err)
	}
	codeGranules, err = buildCodeGranulesForTest([][]uint32{{0, 1}}, 4)
	if err != nil {
		t.Fatalf("build shorter code granules: %v", err)
	}
	filter := Int64RangePredicate{Column: "time_us", Low: 0, High: 10}
	if _, _, err := arena.FilteredGroupedCountCodes(codeGranules, filterGranules, filter, 4); err == nil || !strings.Contains(err.Error(), "row mismatch") {
		t.Fatalf("FilteredGroupedCountCodes row mismatch err=%v want row mismatch", err)
	}

	filterGranules, err = buildInt64GranulesFromSetsForTest([][]int64{{100, 101, 102}})
	if err != nil {
		t.Fatalf("build pruned filter granules: %v", err)
	}
	if _, _, err := arena.FilteredGroupedCountCodes(codeGranules, filterGranules, filter, 4); err == nil || !strings.Contains(err.Error(), "row mismatch") {
		t.Fatalf("FilteredGroupedCountCodes pruned row mismatch err=%v want row mismatch", err)
	}
}

func TestMinMaxInt64Kernels(t *testing.T) {
	granules, err := buildInt64GranulesFromSetsForTest([][]int64{
		{-5, -1, 0},
		{7, 7, 7},
	})
	if err != nil {
		t.Fatalf("build int64 granules: %v", err)
	}
	var arena AggregateArena
	min, max, ok, err := arena.MinMaxInt64(granules, nil)
	if err != nil {
		t.Fatalf("MinMaxInt64: %v", err)
	}
	if !ok || min != -5 || max != 7 {
		t.Fatalf("min/max/ok=(%d,%d,%v) want (-5,7,true)", min, max, ok)
	}
	filter := Int64RangePredicate{Column: "v", Low: 100, High: 200}
	_, _, ok, err = arena.MinMaxInt64(granules, &filter)
	if err != nil {
		t.Fatalf("MinMaxInt64(empty filter): %v", err)
	}
	if ok {
		t.Fatal("MinMaxInt64(empty filter) ok=true want false")
	}
	equalFilter := Int64RangePredicate{Column: "v", Low: 7, High: 7}
	min, max, ok, err = arena.MinMaxInt64(granules, &equalFilter)
	if err != nil {
		t.Fatalf("MinMaxInt64(equal filter): %v", err)
	}
	if !ok || min != 7 || max != 7 {
		t.Fatalf("equal min/max/ok=(%d,%d,%v) want (7,7,true)", min, max, ok)
	}
}

func TestExactDistinctKernels(t *testing.T) {
	codeSets := [][]uint32{
		makeUint32CodeRange(512, 0),
		makeUint32CodeRange(512, 512),
	}
	codeGranules, err := buildCodeGranulesForTest(codeSets, 1024)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	var arena AggregateArena
	distinct, err := arena.ExactDistinctCodes(codeGranules, 1024)
	if err != nil {
		t.Fatalf("ExactDistinctCodes: %v", err)
	}
	if distinct != 1024 {
		t.Fatalf("ExactDistinctCodes=%d want 1024", distinct)
	}

	idGranules, err := buildInt64GranulesFromSetsForTest([][]int64{
		{10, 11, 10, 12},
		{12, 13, 13, 10},
	})
	if err != nil {
		t.Fatalf("build id granules: %v", err)
	}
	distinct, err = arena.ExactDistinctInt64(idGranules)
	if err != nil {
		t.Fatalf("ExactDistinctInt64: %v", err)
	}
	if distinct != 4 {
		t.Fatalf("ExactDistinctInt64=%d want 4", distinct)
	}
}

func TestAggregateKernelsInferCardinalityFromHeaders(t *testing.T) {
	codeGranules, err := buildCodeGranulesForTest([][]uint32{
		{0, 1, 1},
		{2, 4, 4},
	}, 5)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	var arena AggregateArena
	counts, err := arena.GroupedCountCodes(codeGranules, 0)
	if err != nil {
		t.Fatalf("GroupedCountCodes inferred cardinality: %v", err)
	}
	want := []uint64{1, 2, 1, 0, 2}
	if !slices.Equal(counts, want) {
		t.Fatalf("inferred counts=%v want %v", counts, want)
	}
	distinct, err := arena.ExactDistinctCodes(codeGranules, 0)
	if err != nil {
		t.Fatalf("ExactDistinctCodes inferred cardinality: %v", err)
	}
	if distinct != 4 {
		t.Fatalf("inferred distinct=%d want 4", distinct)
	}
}

func TestAggregateKernelsInferMaxCardinalityFromMixedHeaders(t *testing.T) {
	codeGranules, err := buildCodeGranulesWithCardinalitiesForTest(
		[][]uint32{{0, 1}, {0, 2, 3}},
		[]uint32{2, 4},
	)
	if err != nil {
		t.Fatalf("build mixed-cardinality code granules: %v", err)
	}
	timeGranules, err := buildInt64GranulesFromSetsForTest([][]int64{{0, 1}, {10, 11, 12}})
	if err != nil {
		t.Fatalf("build time granules: %v", err)
	}
	var arena AggregateArena
	counts, err := arena.GroupedCountCodes(codeGranules, 0)
	if err != nil {
		t.Fatalf("GroupedCountCodes mixed inferred cardinality: %v", err)
	}
	want := []uint64{2, 1, 1, 1}
	if !slices.Equal(counts, want) {
		t.Fatalf("mixed inferred counts=%v want %v", counts, want)
	}
	got, err := arena.TimeBucketedCountCodes(codeGranules, timeGranules, 10, 0)
	if err != nil {
		t.Fatalf("TimeBucketedCountCodes mixed inferred cardinality: %v", err)
	}
	if got.Cardinality != 4 || got.Buckets != 2 {
		t.Fatalf("bucketed inferred cardinality/buckets=(%d,%d) want (4,2)", got.Cardinality, got.Buckets)
	}
	if got.Count(0, 0) != 1 || got.Count(0, 1) != 1 || got.Count(1, 0) != 1 || got.Count(1, 2) != 1 || got.Count(1, 3) != 1 {
		t.Fatalf("bucketed inferred counts=%+v", got)
	}
}

func TestAggregateKernelsInferCardinalityRejectsEmptyInputs(t *testing.T) {
	var arena AggregateArena
	if _, err := arena.GroupedCountCodes(nil, 0); err == nil || !strings.Contains(err.Error(), "empty code cardinality") {
		t.Fatalf("GroupedCountCodes empty inferred cardinality err=%v want empty code cardinality", err)
	}
	if _, err := arena.ExactDistinctCodes(nil, 0); err == nil || !strings.Contains(err.Error(), "empty code cardinality") {
		t.Fatalf("ExactDistinctCodes empty inferred cardinality err=%v want empty code cardinality", err)
	}
	if _, err := arena.TimeBucketedCountCodes(nil, nil, 10, 0); err == nil || !strings.Contains(err.Error(), "empty code cardinality") {
		t.Fatalf("TimeBucketedCountCodes empty inferred cardinality err=%v want empty code cardinality", err)
	}
}

func TestTimeBucketedGroupedCountsMatchRaw(t *testing.T) {
	codes := [][]uint32{{0, 1, 2, 0, 1, 2, 0, 1, 2, 0}, {1, 2, 0, 1, 2, 0, 1, 2, 0, 1}}
	times := [][]int64{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {10, 11, 12, 13, 14, 15, 16, 17, 18, 19}}
	codeGranules, err := buildCodeGranulesForTest(codes, 3)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	timeGranules, err := buildInt64GranulesFromSetsForTest(times)
	if err != nil {
		t.Fatalf("build time granules: %v", err)
	}
	var arena AggregateArena
	got, err := arena.TimeBucketedCountCodes(codeGranules, timeGranules, 10, 3)
	if err != nil {
		t.Fatalf("TimeBucketedCountCodes: %v", err)
	}
	want := rawTimeBucketedCounts(codes, times, 10, 3)
	if got.Buckets != want.Buckets || got.Cardinality != want.Cardinality || !slices.Equal(got.Counts, want.Counts) {
		t.Fatalf("bucketed counts=%+v want %+v", got, want)
	}
	if got.Count(0, 0) != 4 || got.Count(1, 1) != 4 {
		t.Fatalf("selected bucket counts got bucket0/code0=%d bucket1/code1=%d", got.Count(0, 0), got.Count(1, 1))
	}
}

func TestTimeBucketedCountCodesRejectsHugeBucketSpan(t *testing.T) {
	codeGranules, err := buildCodeGranulesForTest([][]uint32{{0, 0}}, 1)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	const minInt64 = -1 << 63
	const maxInt64 = 1<<63 - 1
	timeBuilder := NewGranuleBuilder(Config{Encoding: EncodingRawInt64, Compression: CompressionNone})
	timeGranule, err := timeBuilder.BuildInt64([]int64{minInt64, maxInt64})
	if err != nil {
		t.Fatalf("BuildInt64 times: %v", err)
	}
	timeGranule.Payload = append([]byte(nil), timeGranule.Payload...)
	var arena AggregateArena
	_, err = arena.TimeBucketedCountCodes(codeGranules, []EncodedGranule{timeGranule}, 1, 1)
	if err == nil || !strings.Contains(err.Error(), "aggregate buckets exceed cap") {
		t.Fatalf("TimeBucketedCountCodes huge span err=%v want aggregate bucket cap", err)
	}
}

func TestTimeBucketedCountCodesRejectsStaleMinMaxMetadata(t *testing.T) {
	codeGranules, err := buildCodeGranulesForTest([][]uint32{{0, 0}}, 1)
	if err != nil {
		t.Fatalf("build code granules: %v", err)
	}
	timeBuilder := NewGranuleBuilder(Config{Encoding: EncodingRawInt64, Compression: CompressionNone})
	timeGranule, err := timeBuilder.BuildInt64([]int64{0, 10})
	if err != nil {
		t.Fatalf("BuildInt64 times: %v", err)
	}
	timeGranule.Payload = append([]byte(nil), timeGranule.Payload...)
	timeGranule.Max = 0
	var arena AggregateArena
	_, err = arena.TimeBucketedCountCodes(codeGranules, []EncodedGranule{timeGranule}, 10, 1)
	if err == nil || !strings.Contains(err.Error(), "outside bucket range") {
		t.Fatalf("TimeBucketedCountCodes stale min/max err=%v want outside bucket range", err)
	}
}

func buildCodeGranulesForTest(codeSets [][]uint32, cardinality uint32) ([]EncodedGranule, error) {
	builder := NewGranuleBuilder(Config{Compression: CompressionNone})
	granules := make([]EncodedGranule, 0, len(codeSets))
	for _, codes := range codeSets {
		g, err := builder.BuildUint32Codes(codes, cardinality)
		if err != nil {
			return nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		granules = append(granules, owned)
	}
	return granules, nil
}

func buildCodeGranulesWithCardinalitiesForTest(codeSets [][]uint32, cardinalities []uint32) ([]EncodedGranule, error) {
	if len(codeSets) != len(cardinalities) {
		return nil, errors.New("test: code/cardinality length mismatch")
	}
	builder := NewGranuleBuilder(Config{Compression: CompressionNone})
	granules := make([]EncodedGranule, 0, len(codeSets))
	for i, codes := range codeSets {
		g, err := builder.BuildUint32Codes(codes, cardinalities[i])
		if err != nil {
			return nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		granules = append(granules, owned)
	}
	return granules, nil
}

func buildInt64GranulesFromSetsForTest(sets [][]int64) ([]EncodedGranule, error) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	granules := make([]EncodedGranule, 0, len(sets))
	for _, values := range sets {
		g, err := builder.BuildInt64(values)
		if err != nil {
			return nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		granules = append(granules, owned)
	}
	return granules, nil
}

func rawFilteredCodeCounts(codeSets [][]uint32, timeSets [][]int64, filter Int64RangePredicate, cardinality int) []uint64 {
	counts := make([]uint64, cardinality)
	for granuleIndex, codes := range codeSets {
		for row, code := range codes {
			timestamp := timeSets[granuleIndex][row]
			if timestamp >= filter.Low && timestamp <= filter.High {
				counts[code]++
			}
		}
	}
	return counts
}

func makeUint32CodeRange(n int, offset uint32) []uint32 {
	values := make([]uint32, n)
	for i := range values {
		values[i] = offset + uint32(i)
	}
	return values
}

func rawTimeBucketedCounts(codeSets [][]uint32, timeSets [][]int64, bucketWidth int64, cardinality uint32) TimeBucketedCounts {
	minTime, maxTime := timeSets[0][0], timeSets[0][0]
	for _, times := range timeSets {
		for _, timestamp := range times {
			if timestamp < minTime {
				minTime = timestamp
			}
			if timestamp > maxTime {
				maxTime = timestamp
			}
		}
	}
	minBucket := floorDiv(minTime, bucketWidth)
	maxBucket := floorDiv(maxTime, bucketWidth)
	buckets := int(maxBucket - minBucket + 1)
	counts := make([]uint64, buckets*int(cardinality))
	for granuleIndex, codes := range codeSets {
		for row, code := range codes {
			bucket := floorDiv(timeSets[granuleIndex][row], bucketWidth)
			counts[int(bucket-minBucket)*int(cardinality)+int(code)]++
		}
	}
	return TimeBucketedCounts{
		BucketWidth: bucketWidth,
		MinBucket:   minBucket,
		Buckets:     buckets,
		Cardinality: cardinality,
		Counts:      counts,
	}
}
