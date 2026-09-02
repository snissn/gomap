package typedcolumn

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestQueryReadyGenerationOpenBuildsExecutionDomainsExactlyOnce(t *testing.T) {
	requireQueryReadyGenerationFileOpen(t)
	image := queryReadyJSONBenchImage(t, 8999,
		[]int64{1, 2, 3},
		[]string{"post", "like", "post"},
		[]string{"did:1", "did:2", "did:1"},
		[]string{"commit", "commit", "identity"},
		[]string{"create", "delete", "create"},
		[]int64{11, 22, 33},
	)
	identity := queryReadyBaseTestIdentity(1)
	built, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "base.qrbg")
	if err := os.WriteFile(path, built.Bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	key := QueryReadyGenerationOpenKey{Identity: identity, ManifestHash: sha256.Sum256([]byte("open-execution-domains"))}
	cache := NewQueryReadyGenerationOpenCache(key)
	t.Cleanup(func() { _ = cache.Close() })
	prepared, err := cache.Open(QueryReadyGenerationOpenFiles{
		Key:                key,
		Base:               QueryReadyGenerationFile{Path: path, Identity: identity, Kind: QueryReadyGenerationBase},
		SnapshotGeneration: identity.Generation,
		Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 1, MaxAccumulatedDeltaParts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := cache.Stats()
	if before.PartsDecoded != 1 || before.DomainsConstructed != 4 || before.PayloadBytesDecoded == 0 || before.WholePartDecodesDuringOpen != 0 {
		t.Fatalf("open stats=%+v", before)
	}
	requests := []QueryReadyOperatorRequest{
		{Kind: QueryReadyOperatorGroupCount, GroupColumn: "event"},
		{Kind: QueryReadyOperatorSumSecondOfDaySquare, ValueColumn: "time_us"},
	}
	for repeat := 0; repeat < 3; repeat++ {
		for _, request := range requests {
			runner, err := prepared.PrepareOperator(request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runner.Run(); err != nil {
				t.Fatal(err)
			}
		}
	}
	after := cache.Stats()
	if after.PartsDecoded != before.PartsDecoded || after.PayloadBytesDecoded != before.PayloadBytesDecoded || after.DomainsConstructed != before.DomainsConstructed || after.WholePartDecodesAfterOpen != 0 {
		t.Fatalf("query preparation rebuilt open state: before=%+v after=%+v", before, after)
	}
}

func TestQueryReadyBaseDeltaJSONBenchQ1ToQ5Parity(t *testing.T) {
	reader := queryReadyJSONBenchReader(t)
	base, delta := queryReadyJSONBenchBase(t), queryReadyJSONBenchDelta(t)
	consolidatedBuild, err := ConsolidateQueryReadyBaseDelta(base, []*QueryReadyDeltaGeneration{delta}, 2)
	if err != nil {
		t.Fatal(err)
	}
	consolidated, err := OpenQueryReadyConsolidatedBaseGeneration(consolidatedBuild.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatal(err)
	}
	consolidatedReader, err := NewQueryReadyConsolidatedBaseDeltaReader(consolidated, nil, QueryReadyBaseDeltaOptions{
		SnapshotGeneration: 2,
		Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 4, MaxRows: 64, MaxBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		req  QueryReadyOperatorRequest
		want []QueryReadyOperatorGroup
	}{
		{
			name: "q1",
			req:  QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "event"},
			want: []QueryReadyOperatorGroup{{Key: "app.bsky.feed.follow", Count: 1}, {Key: "app.bsky.feed.post", Count: 4}, {Key: "app.bsky.feed.repost", Count: 1}},
		},
		{
			name: "q2",
			req: QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: []QueryReadyStringPredicate{
				{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}},
			}},
			want: []QueryReadyOperatorGroup{{Key: "app.bsky.feed.post", Count: 4, DistinctCount: 2}, {Key: "app.bsky.feed.repost", Count: 1, DistinctCount: 1}},
		},
		{
			name: "q2-distinct-only",
			req: QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCountDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: []QueryReadyStringPredicate{
				{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}},
			}},
			want: []QueryReadyOperatorGroup{{Key: "app.bsky.feed.post", Count: 2}, {Key: "app.bsky.feed.repost", Count: 1}},
		},
		{
			name: "q3",
			req: QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupHourCount, GroupColumn: "event", ValueColumn: "time_us", Predicates: []QueryReadyStringPredicate{
				{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}},
				{Column: "event", Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}},
			}},
			want: []QueryReadyOperatorGroup{
				{Key: "app.bsky.feed.post", Hour: 1, Count: 1},
				{Key: "app.bsky.feed.post", Hour: 7, Count: 1},
				{Key: "app.bsky.feed.post", Hour: 8, Count: 1},
				{Key: "app.bsky.feed.post", Hour: 9, Count: 1},
				{Key: "app.bsky.feed.repost", Hour: 6, Count: 1},
			},
		},
		{
			name: "q3-hour-only",
			req:  QueryReadyOperatorRequest{Kind: QueryReadyOperatorHourCount, ValueColumn: "time_us"},
			want: []QueryReadyOperatorGroup{
				{Hour: 1, Count: 1}, {Hour: 5, Count: 1}, {Hour: 6, Count: 1},
				{Hour: 7, Count: 1}, {Hour: 8, Count: 1}, {Hour: 9, Count: 1},
			},
		},
		{
			name: "q4",
			req:  QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", TopK: 3, Order: QueryReadyOrderInt64Asc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
			want: []QueryReadyOperatorGroup{{Key: "alice", Int64: 3_600_000_000}, {Key: "dave", Int64: 28_800_000_000}},
		},
		{
			name: "q4b",
			req:  QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", TopK: 3, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
			want: []QueryReadyOperatorGroup{{Key: "alice", Int64: 32_400_000_000}, {Key: "dave", Int64: 28_800_000_000}},
		},
		{
			name: "q5",
			req:  QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", TopK: 3, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
			want: []QueryReadyOperatorGroup{{Key: "alice", Int64: 28_800_000_000}, {Key: "dave", Int64: 0}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, prepared := range []struct {
				name   string
				reader *QueryReadyBaseDeltaReader
			}{{"base-plus-delta", reader}, {"post-consolidation", consolidatedReader}} {
				runner, err := PrepareQueryReadyOperator(prepared.reader, tc.req)
				if err != nil {
					t.Fatalf("%s prepare: %v", prepared.name, err)
				}
				got, err := runner.Run()
				if err != nil {
					t.Fatalf("%s run: %v", prepared.name, err)
				}
				if !slices.Equal(got.Groups, tc.want) {
					t.Fatalf("%s groups=%+v want %+v", prepared.name, got.Groups, tc.want)
				}
				if got.Stats.EncodedBaseDeltaExecutions != 1 || got.Stats.DocumentMaterializations != 0 || got.Stats.LegacyScanFallbacks != 0 || got.Stats.RowsVisible != 6 {
					t.Fatalf("%s stats=%+v", prepared.name, got.Stats)
				}
				wantFused := tc.name == "q4" || tc.name == "q4b" || tc.name == "q5"
				if gotFused := got.Stats.FusedPredicateReductionExecutions == 1; gotFused != wantFused {
					t.Fatalf("%s fused predicate/reduction=%v want %v stats=%+v", prepared.name, gotFused, wantFused, got.Stats)
				}
				// Sub-millisecond fixtures can legitimately measure as zero on
				// Windows' coarser monotonic clock. Route and work counters are
				// the deterministic proof that fused execution ran.
				if wantFused && (got.Stats.FusedPredicateReductionWorkers != 1 || got.Stats.DecodedBlocks == 0) {
					t.Fatalf("%s fused predicate/reduction work counters missing stats=%+v", prepared.name, got.Stats)
				}
				if wantFused && got.Stats.FusedPredicateReductionNanos > 0 &&
					got.Stats.BaseScanNanos+got.Stats.DeltaMergeNanos < got.Stats.FusedPredicateReductionNanos {
					t.Fatalf("%s public scan timing omits decoded fused interval stats=%+v", prepared.name, got.Stats)
				}
			}
		})
	}
}

func TestQueryReadyExecutionTreatsMissingDictionaryAsEmptyForAllNullNullableColumn(t *testing.T) {
	opts := Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "group", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: 0},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
	}
	part, err := BuildColumnPart(9051, opts, Batch{
		Rows:     2,
		Columns:  map[string][]int64{"id": {1, 2}, "group": {0, 0}},
		Nulls:    map[string][]bool{"group": {true, true}},
		Defaults: map[string][]bool{"group": {false, false}},
	})
	if err != nil {
		t.Fatal(err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{}})
	if err != nil {
		t.Fatal(err)
	}
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		req  QueryReadyOperatorRequest
	}{
		{name: "group", req: QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "group"}},
		{name: "predicate", req: QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "group", Predicates: []QueryReadyStringPredicate{{Column: "group", Values: []string{""}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner, err := PrepareQueryReadyOperator(reader, test.req)
			if err != nil {
				t.Fatal(err)
			}
			result, err := runner.Run()
			if err != nil {
				t.Fatal(err)
			}
			want := []QueryReadyOperatorGroup{{Key: "", Count: 2}}
			if !slices.Equal(result.Groups, want) {
				t.Fatalf("groups=%+v want %+v", result.Groups, want)
			}
		})
	}
}

func TestQueryReadyExecutionPreservesPhysicalOrderingWithoutTopK(t *testing.T) {
	tests := []struct {
		name string
		req  QueryReadyOperatorRequest
		want []QueryReadyOperatorGroup
	}{
		{
			name: "group count by key",
			req:  QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "event"},
			want: []QueryReadyOperatorGroup{
				{Key: "app.bsky.feed.follow", Count: 1},
				{Key: "app.bsky.feed.post", Count: 4},
				{Key: "app.bsky.feed.repost", Count: 1},
			},
		},
		{
			name: "group hour by key then hour",
			req: QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupHourCount, GroupColumn: "event", ValueColumn: "time_us", Predicates: []QueryReadyStringPredicate{
				{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}},
				{Column: "event", Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}},
			}},
			want: []QueryReadyOperatorGroup{
				{Key: "app.bsky.feed.post", Hour: 1, Count: 1},
				{Key: "app.bsky.feed.post", Hour: 7, Count: 1},
				{Key: "app.bsky.feed.post", Hour: 8, Count: 1},
				{Key: "app.bsky.feed.post", Hour: 9, Count: 1},
				{Key: "app.bsky.feed.repost", Hour: 6, Count: 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner, err := PrepareQueryReadyOperator(queryReadyJSONBenchReader(t), tc.req)
			if err != nil {
				t.Fatal(err)
			}
			result, err := runner.Run()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(result.Groups, tc.want) {
				t.Fatalf("groups=%+v want %+v", result.Groups, tc.want)
			}
		})
	}
}

func TestQueryReadyBaseDeltaQExprParityWithoutPrecomputedAnswer(t *testing.T) {
	runner, err := PrepareQueryReadyOperator(queryReadyJSONBenchReader(t), QueryReadyOperatorRequest{Kind: QueryReadyOperatorSumSecondOfDaySquare, ValueColumn: "time_us"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	want := int64(3_600*3_600 + 21_600*21_600 + 25_200*25_200 + 18_000*18_000 + 28_800*28_800 + 32_400*32_400)
	if !slices.Equal(result.Groups, []QueryReadyOperatorGroup{{Count: 6, Int64: want}}) {
		t.Fatalf("groups=%+v want sum=%d", result.Groups, want)
	}
	if result.Stats.PrecomputedAnswers != 0 || result.Stats.RowsMatched != 6 {
		t.Fatalf("stats=%+v", result.Stats)
	}
}

func TestQueryReadySumSecondOfDaySquareOmitsEmptyResult(t *testing.T) {
	runner, err := PrepareQueryReadyOperator(queryReadyJSONBenchReader(t), QueryReadyOperatorRequest{
		Kind:        QueryReadyOperatorSumSecondOfDaySquare,
		ValueColumn: "time_us",
		Predicates:  []QueryReadyStringPredicate{{Column: "kind", Values: []string{"no-such-kind"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Groups) != 0 || result.Stats.RowsMatched != 0 {
		t.Fatalf("empty expression result=%+v", result)
	}
}

func TestQueryReadySumSecondOfDaySquareChecksOverflow(t *testing.T) {
	const secondOfDay = int64(86_399)
	term := secondOfDay * secondOfDay
	sum := int64(math.MaxInt64) - term + 1
	wantSum := sum
	runner := &QueryReadyOperator{request: QueryReadyOperatorRequest{Kind: QueryReadyOperatorSumSecondOfDaySquare}, valueProjected: 0}
	part := &queryReadyExecutionPart{values: [][]int64{{secondOfDay * 1_000_000}}}
	if err := runner.reduceRow(0, part, 0, &sum, &QueryReadyExecutionStats{}); err == nil {
		t.Fatal("expected expression sum overflow")
	}
	if sum != wantSum {
		t.Fatalf("overflow mutated sum=%d want %d", sum, wantSum)
	}
}

func TestQueryReadyGroupHourNormalizesNegativeTimestamps(t *testing.T) {
	image := queryReadyJSONBenchImage(t, 9003,
		[]int64{1, 2, 3, 4},
		[]string{"event", "event", "event", "event"},
		[]string{"did", "did", "did", "did"},
		[]string{"commit", "commit", "commit", "commit"},
		[]string{"create", "create", "create", "create"},
		[]int64{-1, -1_000_000, -3_599_000_000, -86_400_000_000})
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{
		Kind: QueryReadyOperatorGroupHourCount, GroupColumn: "event", ValueColumn: "time_us",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	want := []QueryReadyOperatorGroup{{Key: "event", Hour: 0, Count: 1}, {Key: "event", Hour: 23, Count: 3}}
	if !slices.Equal(result.Groups, want) {
		t.Fatalf("groups=%+v want %+v", result.Groups, want)
	}
	hourRunner, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{Kind: QueryReadyOperatorHourCount, ValueColumn: "time_us"})
	if err != nil {
		t.Fatal(err)
	}
	hourResult, err := hourRunner.Run()
	if err != nil {
		t.Fatal(err)
	}
	hourWant := []QueryReadyOperatorGroup{{Hour: 0, Count: 1}, {Hour: 23, Count: 3}}
	if !slices.Equal(hourResult.Groups, hourWant) {
		t.Fatalf("hour groups=%+v want %+v", hourResult.Groups, hourWant)
	}
}

func TestQueryReadyGroupInt64SpanRejectsOverflow(t *testing.T) {
	image := queryReadyJSONBenchImage(t, 9004,
		[]int64{1, 2},
		[]string{"event", "event"},
		[]string{"did", "did"},
		[]string{"commit", "commit"},
		[]string{"create", "create"},
		[]int64{math.MinInt64, math.MaxInt64})
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(); err == nil || !strings.Contains(err.Error(), "span overflow") {
		t.Fatalf("span overflow err=%v", err)
	}
}

func TestQueryReadyExecutionAppliesUpdatesDeletesAndTombstones(t *testing.T) {
	runner, err := PrepareQueryReadyOperator(queryReadyJSONBenchReader(t), QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "event"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.RowsCandidate != 9 || result.Stats.RowsVisible != 6 || result.Stats.RowsSuperseded != 2 || result.Stats.RowsDeleted != 1 {
		t.Fatalf("visibility stats=%+v", result.Stats)
	}
}

func TestQueryReadyExecutionUsesNoDocumentOrLegacyScanFallback(t *testing.T) {
	runner, err := PrepareQueryReadyOperator(queryReadyJSONBenchReader(t), QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "event"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.DocumentMaterializations != 0 || result.Stats.LegacyScanFallbacks != 0 || result.Stats.Fallbacks != 0 {
		t.Fatalf("fallback stats=%+v", result.Stats)
	}
}

func TestQueryReadyHotExecutionAllocationBound(t *testing.T) {
	runner, err := PrepareQueryReadyOperator(queryReadyJSONBenchReader(t), QueryReadyOperatorRequest{
		Kind: QueryReadyOperatorGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "did",
		Predicates: []QueryReadyStringPredicate{{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		result, runErr := runner.Run()
		if runErr != nil || len(result.Groups) == 0 {
			panic("query-ready allocation probe failed")
		}
	})
	if allocs > 1 {
		t.Fatalf("hot execution allocations/run=%f want <=1", allocs)
	}
}

func TestQueryReadyNullableStringsPreserveEmptyNullAndMissingSemantics(t *testing.T) {
	opts := Options{SchemaVersion: 1, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Name: "event", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: 2},
		{Name: "did", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: 2},
		{Name: "kind", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone, Cardinality: 1},
		{Name: "operation", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone, Cardinality: 1},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: 2}}
	part, err := BuildColumnPart(9301, opts, Batch{Rows: 4,
		Columns:  map[string][]int64{"id": {1, 2, 3, 4}, "event": {0, 0, 0, 1}, "did": {0, 0, 0, 1}, "kind": {0, 0, 0, 0}, "operation": {0, 0, 0, 0}},
		Nulls:    map[string][]bool{"event": {false, false, true, false}, "did": {false, false, true, false}},
		Defaults: map[string][]bool{"event": {false, true, false, false}, "did": {false, true, false, false}},
	})
	if err != nil {
		t.Fatal(err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
		"event": {"post": 0, "": 1}, "did": {"alice": 0, "": 1}, "kind": {"commit": 0}, "operation": {"create": 0},
	}})
	if err != nil {
		t.Fatal(err)
	}
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 1, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 1, MaxAccumulatedDeltaParts: 1, MaxRows: 4, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	q1, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "event"})
	if err != nil {
		t.Fatal(err)
	}
	q1Result, err := q1.Run()
	if err != nil {
		t.Fatal(err)
	}
	if want := []QueryReadyOperatorGroup{{Key: "", Count: 3}, {Key: "post", Count: 1}}; !slices.Equal(q1Result.Groups, want) {
		t.Fatalf("q1 groups=%+v want %+v", q1Result.Groups, want)
	}
	q2, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: []QueryReadyStringPredicate{{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}}}})
	if err != nil {
		t.Fatal(err)
	}
	q2Result, err := q2.Run()
	if err != nil {
		t.Fatal(err)
	}
	if want := []QueryReadyOperatorGroup{{Key: "", Count: 3, DistinctCount: 1}, {Key: "post", Count: 1, DistinctCount: 1}}; !slices.Equal(q2Result.Groups, want) {
		t.Fatalf("q2 groups=%+v want %+v", q2Result.Groups, want)
	}
}

func TestQueryReadyEncodedPredicatesGroupingRandomizedDifferential(t *testing.T) {
	type row struct {
		event, did, kind, operation string
		timeUS                      int64
	}
	rng := rand.New(rand.NewSource(3699))
	events := []string{"", "post", "like", "repost", "follow"}
	dids := []string{"", "alice", "bob", "carol", "dave", "eve"}
	kinds := []string{"commit", "identity"}
	operations := []string{"create", "delete"}
	baseRows := make(map[int64]row)
	for id := int64(1); id <= 40; id++ {
		baseRows[id] = row{events[rng.Intn(len(events))], dids[rng.Intn(len(dids))], kinds[rng.Intn(len(kinds))], operations[rng.Intn(len(operations))], int64(rng.Intn(86_400)) * 1_000_000}
	}
	deltaRows := make(map[int64]row)
	for id := int64(1); id <= 10; id++ {
		deltaRows[id] = row{events[rng.Intn(len(events))], dids[rng.Intn(len(dids))], kinds[rng.Intn(len(kinds))], operations[rng.Intn(len(operations))], int64(rng.Intn(86_400)) * 1_000_000}
	}
	for id := int64(41); id <= 50; id++ {
		deltaRows[id] = row{events[rng.Intn(len(events))], dids[rng.Intn(len(dids))], kinds[rng.Intn(len(kinds))], operations[rng.Intn(len(operations))], int64(rng.Intn(86_400)) * 1_000_000}
	}
	buildImage := func(partID uint64, rows map[int64]row) ColumnPartImage {
		ids := make([]int64, 0, len(rows))
		for id := range rows {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		event, did, kind, operation := make([]string, len(ids)), make([]string, len(ids)), make([]string, len(ids)), make([]string, len(ids))
		times := make([]int64, len(ids))
		for i, id := range ids {
			current := rows[id]
			event[i], did[i], kind[i], operation[i], times[i] = current.event, current.did, current.kind, current.operation, current.timeUS
		}
		return queryReadyJSONBenchImage(t, partID, ids, event, did, kind, operation, times)
	}
	baseBuilt, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: buildImage(9101, baseRows)}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(baseBuilt.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	tombstones := []Tombstone{{PrimaryID: 11, GenerationID: 2}, {PrimaryID: 12, GenerationID: 2}, {PrimaryID: 13, GenerationID: 2}, {PrimaryID: 14, GenerationID: 2}, {PrimaryID: 15, GenerationID: 2}}
	deltaBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: buildImage(9102, deltaRows)}}, tombstones)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(deltaBuilt.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 4, MaxRows: 100, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: []QueryReadyStringPredicate{{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	latest := make(map[int64]row, len(baseRows)+len(deltaRows))
	for id, current := range baseRows {
		latest[id] = current
	}
	for id, current := range deltaRows {
		latest[id] = current
	}
	for _, tombstone := range tombstones {
		delete(latest, tombstone.PrimaryID)
	}
	type aggregate struct {
		count    int
		distinct map[string]struct{}
	}
	aggregates := map[string]*aggregate{}
	for _, current := range latest {
		if current.kind != "commit" || current.operation != "create" {
			continue
		}
		agg := aggregates[current.event]
		if agg == nil {
			agg = &aggregate{distinct: map[string]struct{}{}}
			aggregates[current.event] = agg
		}
		agg.count++
		agg.distinct[current.did] = struct{}{}
	}
	want := make([]QueryReadyOperatorGroup, 0, len(aggregates))
	for key, aggregate := range aggregates {
		want = append(want, QueryReadyOperatorGroup{Key: key, Count: aggregate.count, DistinctCount: len(aggregate.distinct)})
	}
	slices.SortFunc(want, func(left, right QueryReadyOperatorGroup) int {
		if left.Key < right.Key {
			return -1
		}
		if left.Key > right.Key {
			return 1
		}
		return 0
	})
	if !slices.Equal(result.Groups, want) {
		t.Fatalf("groups=%+v want %+v", result.Groups, want)
	}
}

func queryReadyPostPredicates() []QueryReadyStringPredicate {
	return []QueryReadyStringPredicate{
		{Column: "kind", Values: []string{"commit"}},
		{Column: "operation", Values: []string{"create"}},
		{Column: "event", Values: []string{"app.bsky.feed.post"}},
	}
}

func queryReadyJSONBenchReader(t testing.TB) *QueryReadyBaseDeltaReader {
	t.Helper()
	base := queryReadyJSONBenchBase(t)
	delta := queryReadyJSONBenchDelta(t)
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{
		SnapshotGeneration: 2,
		Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 4, MaxRows: 64, MaxBytes: 1 << 20},
	})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	return reader
}

func queryReadyJSONBenchBase(t testing.TB) *QueryReadyBaseGeneration {
	t.Helper()
	image := queryReadyJSONBenchImage(t, 9001,
		[]int64{1, 2, 3, 4, 5},
		[]string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.feed.post", "app.bsky.feed.follow"},
		[]string{"alice", "bob", "alice", "", "carol"},
		[]string{"commit", "commit", "commit", "commit", "identity"},
		[]string{"create", "create", "delete", "create", "create"},
		[]int64{3_600_000_000, 7_200_000_000, 10_800_000_000, 14_400_000_000, 18_000_000_000})
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func queryReadyJSONBenchDelta(t testing.TB) *QueryReadyDeltaGeneration {
	t.Helper()
	image := queryReadyJSONBenchImage(t, 9002,
		[]int64{2, 3, 6, 7},
		[]string{"app.bsky.feed.repost", "app.bsky.feed.post", "app.bsky.feed.post", "app.bsky.feed.post"},
		[]string{"bob", "alice", "dave", "alice"},
		[]string{"commit", "commit", "commit", "commit"},
		[]string{"create", "create", "create", "create"},
		[]int64{21_600_000_000, 25_200_000_000, 28_800_000_000, 32_400_000_000})
	built, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: image}}, []Tombstone{{PrimaryID: 4, GenerationID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(built.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatal(err)
	}
	return delta
}

func queryReadyJSONBenchImage(t testing.TB, partID uint64, ids []int64, event, did, kind, operation []string, timeUS []int64) ColumnPartImage {
	return queryReadyJSONBenchImageWithGranules(t, partID, ids, event, did, kind, operation, timeUS, 3)
}

func queryReadyJSONBenchImageWithGranules(t testing.TB, partID uint64, ids []int64, event, did, kind, operation []string, timeUS []int64, rowsPerGranule int) ColumnPartImage {
	t.Helper()
	dictionaries := map[string]map[string]int64{}
	columns := map[string][]int64{"id": ids, "time_us": timeUS}
	nulls, defaults := map[string][]bool{}, map[string][]bool{}
	for name, values := range map[string][]string{"event": event, "did": did, "kind": kind, "operation": operation} {
		dict := map[string]int64{}
		codes := make([]int64, len(values))
		for i, value := range values {
			code, ok := dict[value]
			if !ok {
				code = int64(len(dict))
				dict[value] = code
			}
			codes[i] = code
		}
		dictionaries[name] = dict
		columns[name] = codes
		nulls[name], defaults[name] = make([]bool, len(values)), make([]bool, len(values))
	}
	opts := Options{SchemaVersion: 1, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Name: "event", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: uint32(len(dictionaries["event"]))},
		{Name: "did", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: uint32(len(dictionaries["did"]))},
		{Name: "kind", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: uint32(len(dictionaries["kind"]))},
		{Name: "operation", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Compression: CompressionNone, Cardinality: uint32(len(dictionaries["operation"]))},
		{Name: "time_us", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: rowsPerGranule}}
	part, err := BuildColumnPart(partID, opts, Batch{Rows: len(ids), Columns: columns, Nulls: nulls, Defaults: defaults})
	if err != nil {
		t.Fatal(err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: dictionaries})
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func TestQueryReadyBaseGenerationPersistsQueryIndependentExecutionColumns(t *testing.T) {
	image := queryReadyJSONBenchImage(t, 9901,
		[]int64{0, 1, 2, 3},
		[]string{"post", "like", "post", "repost"},
		[]string{"did:1", "did:2", "did:1", "did:3"},
		[]string{"commit", "commit", "identity", "commit"},
		[]string{"create", "delete", "", "create"},
		[]int64{11, 22, 33, 44},
	)
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{
		SourceGeneration: 1,
		Image:            image,
	}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Parts) != 1 || base.Parts[0].Execution.Rows() != 4 {
		t.Fatalf("execution view parts=%d rows=%d", len(base.Parts), base.Parts[0].Execution.Rows())
	}
	event, ok := base.Parts[0].Execution.Column("event")
	if !ok || event.Kind() != QueryReadyExecutionColumnCode || event.CodeWidth() != 1 {
		t.Fatalf("event execution column=%+v ok=%v", event, ok)
	}
	if got, absent, err := event.CodeAt(2); err != nil || absent || got != 0 {
		t.Fatalf("event row 2 code=%d absent=%v err=%v", got, absent, err)
	}
	timeUS, ok := base.Parts[0].Execution.Column("time_us")
	if !ok || timeUS.Kind() != QueryReadyExecutionColumnInt64 {
		t.Fatalf("time_us execution column=%+v ok=%v", timeUS, ok)
	}
	if got, absent, err := timeUS.Int64At(3); err != nil || absent || got != 44 {
		t.Fatalf("time_us row 3 value=%d absent=%v err=%v", got, absent, err)
	}
}

func TestQueryReadyExecutionImageUpperBoundCoversBuiltSidecarWithoutAllocating(t *testing.T) {
	image := queryReadyJSONBenchImage(t, 9902,
		[]int64{0, 1, 2, 3},
		[]string{"post", "like", "post", "repost"},
		[]string{"did:1", "did:2", "did:1", "did:3"},
		[]string{"commit", "commit", "identity", "commit"},
		[]string{"create", "delete", "", "create"},
		[]int64{11, 22, 33, 44},
	)
	upper, err := EstimateQueryReadyExecutionImageUpperBound(image)
	if err != nil {
		t.Fatal(err)
	}
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: image}})
	if err != nil {
		t.Fatal(err)
	}
	if built.Stats.ExecutionBytes <= 0 || built.Stats.ExecutionBytes > upper {
		t.Fatalf("execution bytes=%d upper=%d", built.Stats.ExecutionBytes, upper)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := EstimateQueryReadyExecutionImageUpperBound(image); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("upper-bound estimate allocated %.1f times", allocs)
	}
}

func TestQueryReadyExecutionImageRejectsOverflowingVectorRange(t *testing.T) {
	image := queryReadyJSONBenchImage(t, 9903,
		[]int64{0, 1, 2, 3},
		[]string{"post", "like", "post", "repost"},
		[]string{"did:1", "did:2", "did:1", "did:3"},
		[]string{"commit", "commit", "identity", "commit"},
		[]string{"create", "delete", "", "create"},
		[]int64{11, 22, 33, 44},
	)
	part, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := buildQueryReadyExecutionImage(part)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), execution...)
	descriptorOffset := queryReadyExecutionImageHeaderBytes + 4*queryReadyExecutionImageColumnBytes
	descriptor := corrupt[descriptorOffset : descriptorOffset+queryReadyExecutionImageColumnBytes]
	binary.LittleEndian.PutUint64(descriptor[16:24], math.MaxUint64-31)
	if _, err := parseQueryReadyExecutionImage(corrupt, part.Descriptor.RowCount); err == nil {
		t.Fatal("overflowing vector range was accepted")
	}
}

func TestQueryReadyExecutionImageRejectsNonCanonicalRangesAndPadding(t *testing.T) {
	image := queryReadyJSONBenchImage(t, 9904,
		[]int64{0, 1, 2, 3},
		[]string{"post", "like", "post", "repost"},
		[]string{"did:1", "did:2", "did:1", "did:3"},
		[]string{"commit", "commit", "identity", "commit"},
		[]string{"create", "delete", "", "create"},
		[]int64{11, 22, 33, 44},
	)
	part, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := buildQueryReadyExecutionImage(part)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := func(data []byte, index int) []byte {
		start := queryReadyExecutionImageHeaderBytes + index*queryReadyExecutionImageColumnBytes
		return data[start : start+queryReadyExecutionImageColumnBytes]
	}
	tests := []struct {
		name    string
		corrupt func([]byte)
	}{
		{
			name: "name gap",
			corrupt: func(data []byte) {
				first := descriptor(data, 0)
				binary.LittleEndian.PutUint32(first[4:8], binary.LittleEndian.Uint32(first[4:8])-1)
			},
		},
		{
			name: "nonzero name padding",
			corrupt: func(data []byte) {
				last := descriptor(data, int(binary.LittleEndian.Uint32(data[16:20]))-1)
				nameEnd := binary.LittleEndian.Uint32(last[0:4]) + binary.LittleEndian.Uint32(last[4:8])
				data[nameEnd] = 1
			},
		},
		{
			name: "overlapping values",
			corrupt: func(data []byte) {
				first, second := descriptor(data, 0), descriptor(data, 1)
				copy(second[16:24], first[16:24])
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), execution...)
			tc.corrupt(corrupt)
			if _, err := parseQueryReadyExecutionImage(corrupt, part.Descriptor.RowCount); err == nil {
				t.Fatal("non-canonical execution image was accepted")
			}
		})
	}
}

var queryReadyBenchmarkResult QueryReadyOperatorResult

func TestQueryReadyEncodedFusedParallelGroupedInt64Parity(t *testing.T) {
	serialReader := queryReadyJSONBenchBenchmarkReaderParts(t, 32_768, 0, 32_768)
	parallelReader := queryReadyJSONBenchBenchmarkReaderParts(t, 32_768, 0, 4_096)
	requests := []QueryReadyOperatorRequest{
		{Kind: QueryReadyOperatorGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", TopK: 10, Order: QueryReadyOrderInt64Asc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
		{Kind: QueryReadyOperatorGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", TopK: 10, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
		{Kind: QueryReadyOperatorGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", TopK: 10, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
	}
	for _, request := range requests {
		serial, err := PrepareQueryReadyOperator(serialReader, request)
		if err != nil {
			t.Fatal(err)
		}
		want, err := serial.Run()
		if err != nil {
			t.Fatal(err)
		}
		parallel, err := PrepareQueryReadyOperator(parallelReader, request)
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			got, err := parallel.Run()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Groups, want.Groups) {
				t.Fatalf("kind=%s attempt=%d groups=%+v want %+v", request.Kind, attempt, got.Groups, want.Groups)
			}
			wantFused := runtime.GOMAXPROCS(0) > 1
			if (got.Stats.FusedPredicateReductionExecutions == 1) != wantFused || (wantFused && got.Stats.FusedPredicateReductionWorkers <= 1) ||
				got.Stats.RowsScanned != want.Stats.RowsScanned || got.Stats.RowsMatched != want.Stats.RowsMatched ||
				got.Stats.DecodedBytes != want.Stats.DecodedBytes ||
				got.Stats.EncodedBaseDeltaExecutions != want.Stats.EncodedBaseDeltaExecutions ||
				got.Stats.ScratchBytes <= 0 ||
				got.Stats.DocumentMaterializations != 0 || got.Stats.LegacyScanFallbacks != 0 || got.Stats.PrecomputedAnswers != 0 {
				t.Fatalf("kind=%s attempt=%d stats=%+v want parity with %+v", request.Kind, attempt, got.Stats, want.Stats)
			}
		}
	}
}

func TestQueryReadyEncodedFusedParallelMixedBaseDeltaTiming(t *testing.T) {
	reader := queryReadyJSONBenchBenchmarkReaderParts(t, 32_768, 1, 4_096)
	operator, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{
		Kind: QueryReadyOperatorGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us",
		TopK: 10, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true,
		Predicates: queryReadyPostPredicates(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := operator.Run()
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOMAXPROCS(0) <= 1 {
		return
	}
	stats := result.Stats
	if stats.FusedPredicateReductionExecutions != 1 || stats.BaseRowsScanned == 0 || stats.DeltaRowsScanned == 0 {
		t.Fatalf("mixed fused route stats=%+v", stats)
	}
	if got := stats.BaseScanNanos + stats.DeltaMergeNanos; got != stats.FusedPredicateReductionNanos {
		t.Fatalf("public scan timing=%d want fused interval %d", got, stats.FusedPredicateReductionNanos)
	}
}

func BenchmarkQueryReadyEncodedJSONBenchOperators(b *testing.B) {
	reader := queryReadyJSONBenchBenchmarkReader(b, 100_000, 20_000)
	requests := map[string]QueryReadyOperatorRequest{
		"q2":            {Kind: QueryReadyOperatorGroupCountAndDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: []QueryReadyStringPredicate{{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}}}},
		"q3":            {Kind: QueryReadyOperatorGroupHourCount, GroupColumn: "event", ValueColumn: "time_us", Predicates: []QueryReadyStringPredicate{{Column: "kind", Values: []string{"commit"}}, {Column: "operation", Values: []string{"create"}}, {Column: "event", Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}}}},
		"q5":            {Kind: QueryReadyOperatorGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", TopK: 3, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
		"canonical-q2":  {Kind: QueryReadyOperatorGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"},
		"canonical-q3":  {Kind: QueryReadyOperatorHourCount, ValueColumn: "time_us"},
		"canonical-q4b": {Kind: QueryReadyOperatorGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"},
	}
	for _, name := range []string{"q2", "q3", "q5", "canonical-q2", "canonical-q3", "canonical-q4b"} {
		b.Run(name, func(b *testing.B) {
			runner, err := PrepareQueryReadyOperator(reader, requests[name])
			if err != nil {
				b.Fatal(err)
			}
			warm, err := runner.Run()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(warm.Stats.RowsScanned))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queryReadyBenchmarkResult, err = runner.Run()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(warm.Stats.ScratchBytes), "scratch_B")
		})
	}
}

func BenchmarkQueryReadyEncodedFusedGroupedInt64(b *testing.B) {
	if runtime.GOMAXPROCS(0) < 2 {
		b.Skip("parallel fused benchmark requires at least two runnable workers")
	}
	reader := queryReadyJSONBenchBenchmarkReaderParts(b, 1_000_000, 0, 16_000)
	requests := map[string]QueryReadyOperatorRequest{
		"q4": {Kind: QueryReadyOperatorGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", TopK: 10, Order: QueryReadyOrderInt64Asc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
		"q5": {Kind: QueryReadyOperatorGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", TopK: 10, Order: QueryReadyOrderInt64Desc, SkipEmptyGroupKey: true, Predicates: queryReadyPostPredicates()},
	}
	for _, name := range []string{"q4", "q5"} {
		b.Run(name, func(b *testing.B) {
			runner, err := PrepareQueryReadyOperator(reader, requests[name])
			if err != nil {
				b.Fatal(err)
			}
			warm, err := runner.Run()
			if err != nil {
				b.Fatal(err)
			}
			if warm.Stats.FusedPredicateReductionExecutions != 1 {
				b.Fatalf("fused predicate/reduction executions=%d want 1", warm.Stats.FusedPredicateReductionExecutions)
			}
			b.ReportAllocs()
			b.SetBytes(int64(warm.Stats.RowsScanned))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queryReadyBenchmarkResult, err = runner.Run()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(warm.Stats.DecodedBytes), "decoded_B/op")
			b.ReportMetric(float64(warm.Stats.ScratchBytes), "scratch_B")
		})
	}
}

func BenchmarkQueryReadyEncodedDeltaDepthCurve(b *testing.B) {
	base, deltas := queryReadyDeltaBenchmarkFixture(b, 4, "low_cardinality")
	for _, depth := range []int{0, 1, 2, 4} {
		b.Run(fmt.Sprintf("N=%d", depth), func(b *testing.B) {
			reader, err := NewQueryReadyBaseDeltaReader(base, deltas[:depth], QueryReadyBaseDeltaOptions{
				SnapshotGeneration: uint64(depth + 1),
				Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8},
			})
			if err != nil {
				b.Fatal(err)
			}
			runner, err := PrepareQueryReadyOperator(reader, QueryReadyOperatorRequest{Kind: QueryReadyOperatorGroupCount, GroupColumn: "kind_code"})
			if err != nil {
				b.Fatal(err)
			}
			warm, err := runner.Run()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(warm.Stats.DecodedBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queryReadyBenchmarkResult, err = runner.Run()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(warm.Stats.RowsScanned), "rows/op")
			b.ReportMetric(float64(warm.Stats.ScratchBytes), "scratch_B")
		})
	}
}

func queryReadyJSONBenchBenchmarkReader(b testing.TB, baseRows, deltaRows int) *QueryReadyBaseDeltaReader {
	return queryReadyJSONBenchBenchmarkReaderParts(b, baseRows, deltaRows, baseRows)
}

func queryReadyJSONBenchBenchmarkReaderParts(b testing.TB, baseRows, deltaRows, basePartRows int) *QueryReadyBaseDeltaReader {
	b.Helper()
	buildRows := func(firstID int64, rows int) ([]int64, []string, []string, []string, []string, []int64) {
		ids := make([]int64, rows)
		event, did, kind, operation := make([]string, rows), make([]string, rows), make([]string, rows), make([]string, rows)
		times := make([]int64, rows)
		events := [...]string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.feed.follow", "app.bsky.graph.block"}
		for i := 0; i < rows; i++ {
			ids[i] = firstID + int64(i)
			event[i] = events[i%len(events)]
			did[i] = "did:plc:" + string(rune(0x1000+i%2048))
			if i%5 == 0 {
				kind[i] = "identity"
			} else {
				kind[i] = "commit"
			}
			if i%7 == 0 {
				operation[i] = "delete"
			} else {
				operation[i] = "create"
			}
			times[i] = int64(i%86_400) * 1_000_000
		}
		return ids, event, did, kind, operation, times
	}
	ids, event, did, kind, operation, times := buildRows(1, baseRows)
	if basePartRows <= 0 {
		b.Fatal("base part rows must be positive")
	}
	baseParts := make([]QueryReadyBasePartInput, 0, (baseRows+basePartRows-1)/basePartRows)
	for first, partID := 0, uint64(9201); first < baseRows; first, partID = first+basePartRows, partID+1 {
		limit := min(first+basePartRows, baseRows)
		baseParts = append(baseParts, QueryReadyBasePartInput{
			SourceGeneration: 1,
			Image: queryReadyJSONBenchImageWithGranules(b, partID,
				ids[first:limit], event[first:limit], did[first:limit], kind[first:limit], operation[first:limit], times[first:limit], 8_192),
		})
	}
	baseBuilt, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), baseParts)
	if err != nil {
		b.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(baseBuilt.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		b.Fatal(err)
	}
	if deltaRows == 0 {
		reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{
			SnapshotGeneration: 1,
			Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 1, MaxAccumulatedDeltaParts: 1, MaxRows: int64(baseRows), MaxBytes: 1 << 30},
		})
		if err != nil {
			b.Fatal(err)
		}
		return reader
	}
	updates := deltaRows / 2
	ids, event, did, kind, operation, times = buildRows(1, updates)
	newIDs, newEvent, newDid, newKind, newOperation, newTimes := buildRows(int64(baseRows)+1, deltaRows-updates)
	ids, event, did, kind, operation, times = append(ids, newIDs...), append(event, newEvent...), append(did, newDid...), append(kind, newKind...), append(operation, newOperation...), append(times, newTimes...)
	deltaImage := queryReadyJSONBenchImageWithGranules(b, 9202, ids, event, did, kind, operation, times, 8_192)
	deltaBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: deltaImage}}, nil)
	if err != nil {
		b.Fatal(err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(deltaBuilt.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		b.Fatal(err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 4, MaxRows: int64(baseRows + deltaRows), MaxBytes: 1 << 30}})
	if err != nil {
		b.Fatal(err)
	}
	return reader
}
