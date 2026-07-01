package collections

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedColumnQ2SortedGroupedDistinctStreaming1950(t *testing.T) {
	batches := typedColumnQ2SortedDiagnosticsBatches1950(256)
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer closeFn()

	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	wantCounts := columnPhysicalJSONBenchQ2ReferenceCountsP0(events)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events)
	req := typedColumnQ2Request1950()

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2): %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "direct", direct, rowHash, wantCounts)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "direct", direct.Diagnostics, len(events), matchedRows, true)
	assertTypedColumnQ2SortedGroupedDistinctPostPrepareDiagnostics3324(t, "direct", direct.Diagnostics, true)

	skipChecksumsReq := req
	skipChecksumsReq.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
	skipChecksums, err := col.RunColumnPhysicalQuery(skipChecksumsReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 skip checksums): %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "skip checksums", skipChecksums, rowHash, wantCounts)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "skip checksums", skipChecksums.Diagnostics, len(events), matchedRows, true)
	assertTypedColumnQ2SortedGroupedDistinctPostPrepareDiagnostics3324(t, "skip checksums", skipChecksums.Diagnostics, true)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q2): %v", err)
	}
	defer func() { _ = runner.Close() }()
	assertTypedColumnQ2SortedGroupedDistinctPostPrepareDiagnostics3324(t, "prepared setup", runner.PrepareDiagnostics(), true)
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared q2: %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "prepared", prepared, rowHash, wantCounts)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "prepared", prepared.Diagnostics, len(events), matchedRows, true)
}

func TestTypedColumnQ2MutationVisibilityLatestVisibleC3(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer closeFn()

	req := typedColumnQ2Request1950()
	insertOnly, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("insert-only RunColumnPhysicalQuery(q2): %v", err)
	}
	if insertOnly.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || !insertOnly.Diagnostics.SortedGroupedDistinctUsed || insertOnly.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone {
		t.Fatalf("insert-only diagnostics=%+v want optimized typed-column q2 path", insertOnly.Diagnostics)
	}

	updated := typedColumnQ2SortedBatchA1950()[0]
	updated.Collection = "app.bsky.feed.like"
	updated.TimeUS += 10
	updateTypedColumnEvent1953(t, col, updated)

	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("mutation q2 RunColumnPhysicalQuery: %v diagnostics=%+v", err, result.Diagnostics)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone || !result.Diagnostics.SortedGroupedDistinctUsed {
		t.Fatalf("mutation q2 diagnostics=%+v want latest-visible sorted grouped-distinct path", result.Diagnostics)
	}
	if result.Diagnostics.MutationParts == 0 || result.Diagnostics.VisibilityRows == 0 || result.Diagnostics.DocumentMaterializations != 0 || result.Diagnostics.RowMaterializations != 0 {
		t.Fatalf("mutation q2 diagnostics=%+v want latest-visible physical state without document fallback", result.Diagnostics)
	}
	if _, err := col.PrepareColumnPhysicalQuery(req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "insert-only") {
		t.Fatalf("prepared mutation q2 err=%v want fail-closed prepared insert-only guard", err)
	}
}

func TestTypedColumnQ2SortedGroupedDistinctFallback1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	result, err := col.RunColumnPhysicalQuery(typedColumnQ2Request1950())
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 fallback): %v", err)
	}
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "fallback", result, rowHash, columnPhysicalJSONBenchQ2ReferenceCountsP0(events))
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "fallback", result.Diagnostics, len(events), columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events), false)
	if result.Diagnostics.SortedGroupedDistinctFallbackReason == columnSortedGroupedDistinctFallbackNone {
		t.Fatalf("fallback diagnostics=%+v want explicit sorted grouped-distinct fallback reason", result.Diagnostics)
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctNoSortLocalDictionaries1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2LocalDictBatchA1950(), typedColumnQ2LocalDictBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	d, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, nil, batches)
	defer closeFn()

	codesByGeneration := typedColumnQ1DictionaryCodeByGeneration1950(t, d, col, "did", "did:m")
	if len(codesByGeneration) < 2 {
		t.Fatalf("did:m dictionary codes by generation=%v want at least two", codesByGeneration)
	}
	seenCodes := make(map[int64]struct{}, len(codesByGeneration))
	for _, code := range codesByGeneration {
		seenCodes[code] = struct{}{}
	}
	if len(seenCodes) < 2 {
		t.Fatalf("did:m local dictionary codes=%v want differing local dictionary orders", codesByGeneration)
	}

	req := typedColumnQ2Request1950()
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	want := columnPhysicalJSONBenchQ2ReferenceCountsP0(events)
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events)

	direct, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 dense no-sort): %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "dense no-sort", direct, rowHash, want)
	assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(t, "dense no-sort", direct.Diagnostics, len(events), matchedRows, columnTypedColumnDenseGroupCountDistinctReducerPairBitset)

	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q2 dense no-sort): %v", err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared q2 dense no-sort: %v", err)
	}
	assertTypedColumnQ2DensePreparedGlobalCodes1950(t, runner)
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "prepared dense no-sort", prepared, rowHash, want)
	assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(t, "prepared dense no-sort", prepared.Diagnostics, len(events), matchedRows, columnTypedColumnDenseGroupCountDistinctReducerPairBitset)

	got := columnPhysicalJSONBenchQ2CountsP0(prepared.Groups)
	if got["app.z"].Count != 4 || got["app.z"].Distinct != 2 {
		t.Fatalf("app.z counts=%+v want duplicate did:m counted once across different local dictionaries", got["app.z"])
	}
	if got[""].Count != 1 || got[""].Distinct != 1 {
		t.Fatalf("empty event counts=%+v want real empty group value", got[""])
	}
	if got["app.emptydid"].Count != 2 || got["app.emptydid"].Distinct != 1 {
		t.Fatalf("empty did counts=%+v want empty distinct value counted once", got["app.emptydid"])
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctLocalCodesNullableValues3080(t *testing.T) {
	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	predicateCodes := []uint32{1, 1, 1, 1, 0}
	allowed := []uint64{2}
	runner := &columnTypedColumnPhysicalQueryRunner{
		plan: columnTypedColumnPhysicalQueryPlan{
			ProjectedColumns:        []string{"collection", "did", "kind", "operation"},
			PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
			DenseGroupCountDistinct: true,
		},
		parts: []columnTypedColumnPhysicalQueryPart{
			{
				Rows: len(predicateCodes),
				DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
					Rows: len(predicateCodes),
					Group: columnTypedColumnDenseStringCodeColumn{
						Codes:      []uint32{0, 0, 0, 1, 0},
						Valid:      []bool{true, false, true, true, true},
						Dictionary: []string{"app.a", "app.b"},
					},
					Distinct: columnTypedColumnDenseStringCodeColumn{
						Codes:      []uint32{0, 1, 0, 0, 1},
						Valid:      []bool{true, true, false, true, true},
						Dictionary: []string{"did:x", "did:y"},
					},
					Predicates: []columnTypedColumnDensePredicatePart{
						{Codes: predicateCodes, Allowed: allowed},
						{Codes: predicateCodes, Allowed: allowed},
					},
				},
			},
		},
	}

	result, err := runner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
	if err != nil {
		t.Fatalf("runDenseGroupCountDistinct local nullable: %v", err)
	}
	if result.Diagnostics.DenseGroupCountDistinctReducer != columnTypedColumnDenseGroupCountDistinctReducerLocalBitset {
		t.Fatalf("reducer=%q want %q diagnostics=%+v", result.Diagnostics.DenseGroupCountDistinctReducer, columnTypedColumnDenseGroupCountDistinctReducerLocalBitset, result.Diagnostics)
	}
	if result.Diagnostics.RowsScanned != len(predicateCodes) || result.Diagnostics.RowsMatched != 4 || result.Diagnostics.ReduceRows != 4 {
		t.Fatalf("row diagnostics=%+v want scanned=%d matched/reduced=4", result.Diagnostics, len(predicateCodes))
	}
	if len(runner.parts[0].DenseGroupCountDistinct.Group.GlobalCodes) != 0 || len(runner.parts[0].DenseGroupCountDistinct.Distinct.GlobalCodes) != 0 {
		t.Fatalf("local reducer populated global codes: group=%d distinct=%d", len(runner.parts[0].DenseGroupCountDistinct.Group.GlobalCodes), len(runner.parts[0].DenseGroupCountDistinct.Distinct.GlobalCodes))
	}
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	want := map[string]columnPhysicalJSONBenchQ2CountP0{
		"":      {Count: 1, Distinct: 1},
		"app.a": {Count: 2, Distinct: 2},
		"app.b": {Count: 1, Distinct: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("groups=%v want %v raw=%+v", got, want, result.Groups)
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctBitsetLayout1950(t *testing.T) {
	wordsPerGroup, totalWords, ok := columnTypedColumnDenseGroupCountDistinctBitsetLayout(13, 1_000_000)
	if !ok {
		t.Fatalf("layout ok=false want true")
	}
	if wordsPerGroup != 15_625 || totalWords != 203_125 {
		t.Fatalf("layout words_per_group=%d total_words=%d want 15625/203125", wordsPerGroup, totalWords)
	}

	wordsPerGroup, totalWords, ok = columnTypedColumnDenseGroupCountDistinctBitsetLayout(0, 1_000_000)
	if !ok || wordsPerGroup != 0 || totalWords != 0 {
		t.Fatalf("zero-group layout words_per_group=%d total_words=%d ok=%t want 0/0/true", wordsPerGroup, totalWords, ok)
	}

	_, _, ok = columnTypedColumnDenseGroupCountDistinctBitsetLayout(1, columnTypedColumnDenseGroupCountDistinctMaxBitsetWords*64+1)
	if ok {
		t.Fatalf("oversized layout ok=true want false")
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctRankMapCapacity3158(t *testing.T) {
	if got, want := columnTypedColumnDenseGroupCountDistinctRankMapCapacity((16<<10)-1), (16<<10)-1; got != want {
		t.Fatalf("small rank map capacity=%d want %d", got, want)
	}
	if got, want := columnTypedColumnDenseGroupCountDistinctRankMapCapacity(16<<10), 16<<10; got != want {
		t.Fatalf("threshold rank map capacity=%d want %d", got, want)
	}
	if got, want := columnTypedColumnDenseGroupCountDistinctRankMapCapacity(627_647), 209_215; got != want {
		t.Fatalf("large rank map capacity=%d want %d", got, want)
	}

	parts := []columnTypedColumnPhysicalQueryPart{
		{
			DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
				Group: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"app.a", "app.c"},
					Valid:      []bool{true, false},
				},
				Distinct: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"did:a", "did:c"},
				},
			},
		},
		{
			DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
				Group: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"app.a", "app.b"},
				},
				Distinct: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"did:b", "did:c"},
				},
			},
		},
	}
	groupDictionary, groupRanks, err := columnTypedColumnDenseGroupCountDistinctGlobalDictionary(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Group
	})
	if err != nil {
		t.Fatalf("global group dictionary: %v", err)
	}
	if want := []string{"", "app.a", "app.b", "app.c"}; !reflect.DeepEqual(groupDictionary, want) {
		t.Fatalf("global group dictionary=%v want %v", groupDictionary, want)
	}
	for rank, value := range groupDictionary {
		if groupRanks[value] != uint32(rank) {
			t.Fatalf("group rank[%q]=%d want %d ranks=%v", value, groupRanks[value], rank, groupRanks)
		}
	}
	distinctRanks, distinctCardinality, err := columnTypedColumnDenseGroupCountDistinctGlobalRanks(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Distinct
	})
	if err != nil {
		t.Fatalf("global distinct ranks: %v", err)
	}
	if distinctCardinality != 3 {
		t.Fatalf("distinct cardinality=%d want 3 ranks=%v", distinctCardinality, distinctRanks)
	}
	for value, want := range map[string]uint32{"did:a": 0, "did:b": 1, "did:c": 2} {
		if got := distinctRanks[value]; got != want {
			t.Fatalf("distinct rank[%q]=%d want %d ranks=%v", value, got, want, distinctRanks)
		}
	}

	var prepareDiagnostics columnTypedColumnPhysicalQueryPrepareDiagnostics
	if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics(parts, &prepareDiagnostics); err != nil {
		t.Fatalf("prepare sorted global rank maps: %v", err)
	}
	if prepareDiagnostics.Q2DenseGroupGlobalRankNanos != prepareDiagnostics.Q2GroupRankNanos ||
		prepareDiagnostics.Q2DenseDistinctGlobalRankNanos != prepareDiagnostics.Q2DistinctRankNanos ||
		prepareDiagnostics.Q2DensePartLocalRankNanos != prepareDiagnostics.Q2LocalRankNanos {
		t.Fatalf("dense q2 rank diagnostics=%+v want explicit dense split to mirror legacy phases", prepareDiagnostics)
	}
	group0 := parts[0].DenseGroupCountDistinct.Group
	group1 := parts[1].DenseGroupCountDistinct.Group
	if !reflect.DeepEqual(group0.GlobalDictionary, []string{"", "app.a", "app.b", "app.c"}) {
		t.Fatalf("part 0 group global dictionary=%v", group0.GlobalDictionary)
	}
	if !reflect.DeepEqual(group1.GlobalDictionary, group0.GlobalDictionary) {
		t.Fatalf("part 1 group global dictionary=%v want %v", group1.GlobalDictionary, group0.GlobalDictionary)
	}
	if !reflect.DeepEqual(group0.GlobalLocalRanks, []uint32{1, 3}) {
		t.Fatalf("part 0 group local ranks=%v want [1 3]", group0.GlobalLocalRanks)
	}
	if !reflect.DeepEqual(group1.GlobalLocalRanks, []uint32{1, 2}) {
		t.Fatalf("part 1 group local ranks=%v want [1 2]", group1.GlobalLocalRanks)
	}
	if !group0.GlobalEmptyRankOK || group0.GlobalEmptyRank != 0 || !group1.GlobalEmptyRankOK || group1.GlobalEmptyRank != 0 {
		t.Fatalf("group empty ranks part0=(%d,%t) part1=(%d,%t) want (0,true)", group0.GlobalEmptyRank, group0.GlobalEmptyRankOK, group1.GlobalEmptyRank, group1.GlobalEmptyRankOK)
	}
	distinct0 := parts[0].DenseGroupCountDistinct.Distinct
	distinct1 := parts[1].DenseGroupCountDistinct.Distinct
	if distinct0.GlobalDictionary != nil || distinct1.GlobalDictionary != nil {
		t.Fatalf("distinct global dictionary allocated part0=%v part1=%v", distinct0.GlobalDictionary, distinct1.GlobalDictionary)
	}
	if distinct0.GlobalCardinality != 3 || distinct1.GlobalCardinality != 3 || !distinct0.GlobalCardinalityOK || !distinct1.GlobalCardinalityOK {
		t.Fatalf("distinct cardinality part0=(%d,%t) part1=(%d,%t) want (3,true)", distinct0.GlobalCardinality, distinct0.GlobalCardinalityOK, distinct1.GlobalCardinality, distinct1.GlobalCardinalityOK)
	}
	if !reflect.DeepEqual(distinct0.GlobalLocalRanks, []uint32{0, 2}) {
		t.Fatalf("part 0 distinct local ranks=%v want [0 2]", distinct0.GlobalLocalRanks)
	}
	if !reflect.DeepEqual(distinct1.GlobalLocalRanks, []uint32{1, 2}) {
		t.Fatalf("part 1 distinct local ranks=%v want [1 2]", distinct1.GlobalLocalRanks)
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctActiveGroupBitset1950(t *testing.T) {
	const (
		groupCardinality    = 4096
		distinctCardinality = 65536
		groupA              = 100
		groupB              = 200
		groupRejected       = 300
	)
	groupDictionary := make([]string, groupCardinality)
	groupDictionary[groupA] = "app.active.a"
	groupDictionary[groupB] = "app.active.b"
	groupDictionary[groupRejected] = "app.rejected"
	groupCodes := []uint32{groupA, groupA, groupA, groupB, groupB, groupB, groupRejected}
	distinctCodes := []uint32{10, 10, 11, 20, 21, 20, 30}
	predicateCodes := []uint32{1, 1, 1, 1, 1, 1, 0}
	allowed := []uint64{2}
	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	runner := &columnTypedColumnPhysicalQueryRunner{
		plan: columnTypedColumnPhysicalQueryPlan{
			ProjectedColumns:        []string{"collection", "did", "kind", "operation"},
			PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
			DenseGroupCountDistinct: true,
		},
		parts: []columnTypedColumnPhysicalQueryPart{
			{
				Rows: len(groupCodes),
				DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
					Rows: len(groupCodes),
					Group: columnTypedColumnDenseStringCodeColumn{
						Codes:               groupCodes,
						GlobalCodes:         groupCodes,
						GlobalDictionary:    groupDictionary,
						GlobalCardinality:   groupCardinality,
						GlobalCardinalityOK: true,
					},
					Distinct: columnTypedColumnDenseStringCodeColumn{
						Codes:               distinctCodes,
						GlobalCodes:         distinctCodes,
						GlobalCardinality:   distinctCardinality,
						GlobalCardinalityOK: true,
					},
					Predicates: []columnTypedColumnDensePredicatePart{
						{Codes: predicateCodes, Allowed: allowed},
						{Codes: predicateCodes, Allowed: allowed},
					},
				},
			},
		},
	}

	result, err := runner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
	if err != nil {
		t.Fatalf("runDenseGroupCountDistinct: %v", err)
	}
	if result.Diagnostics.DenseGroupCountDistinctReducer != columnTypedColumnDenseGroupCountDistinctReducerActiveBitset {
		t.Fatalf("reducer=%q want %q diagnostics=%+v", result.Diagnostics.DenseGroupCountDistinctReducer, columnTypedColumnDenseGroupCountDistinctReducerActiveBitset, result.Diagnostics)
	}
	if result.Diagnostics.DenseGroupCountDistinctGroups != groupCardinality || result.Diagnostics.DenseGroupCountDistinctValues != distinctCardinality {
		t.Fatalf("cardinality diagnostics=%+v want groups=%d values=%d", result.Diagnostics, groupCardinality, distinctCardinality)
	}
	if got, want := result.Diagnostics.DenseGroupCountDistinctPairBitWords, 2*((distinctCardinality+63)/64); got != want {
		t.Fatalf("pair bit words=%d want active group words=%d diagnostics=%+v", got, want, result.Diagnostics)
	}
	if result.Diagnostics.RowsScanned != len(groupCodes) || result.Diagnostics.RowsMatched != 6 || result.Diagnostics.ReduceRows != 6 {
		t.Fatalf("row diagnostics=%+v want scanned=%d matched/reduced=6", result.Diagnostics, len(groupCodes))
	}
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	want := map[string]columnPhysicalJSONBenchQ2CountP0{
		"app.active.a": {Count: 3, Distinct: 2},
		"app.active.b": {Count: 3, Distinct: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("groups=%v want %v raw=%+v", got, want, result.Groups)
	}
}

func BenchmarkTypedColumnQ2DenseGroupCountDistinctActiveGroups1950(b *testing.B) {
	const (
		rows                = 262144
		activeGroups        = 13
		groupCardinality    = 4096
		distinctCardinality = 65536
	)
	groupDictionary := make([]string, groupCardinality)
	activeGroupCodes := make([]uint32, activeGroups)
	for groupIdx := 0; groupIdx < activeGroups; groupIdx++ {
		code := uint32(100 + groupIdx)
		activeGroupCodes[groupIdx] = code
		groupDictionary[code] = fmt.Sprintf("app.active.%02d", groupIdx)
	}
	groupCodes := make([]uint32, rows)
	distinctCodes := make([]uint32, rows)
	predicateCodes := make([]uint32, rows)
	for row := 0; row < rows; row++ {
		groupCodes[row] = activeGroupCodes[row%activeGroups]
		distinctCodes[row] = uint32(row % distinctCardinality)
		predicateCodes[row] = 1
	}
	allowed := []uint64{2}
	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	runner := &columnTypedColumnPhysicalQueryRunner{
		plan: columnTypedColumnPhysicalQueryPlan{
			ProjectedColumns:        []string{"collection", "did", "kind", "operation"},
			PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
			DenseGroupCountDistinct: true,
		},
		parts: []columnTypedColumnPhysicalQueryPart{
			{
				Rows: rows,
				DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
					Rows: rows,
					Group: columnTypedColumnDenseStringCodeColumn{
						Codes:               groupCodes,
						GlobalCodes:         groupCodes,
						GlobalDictionary:    groupDictionary,
						GlobalCardinality:   groupCardinality,
						GlobalCardinalityOK: true,
					},
					Distinct: columnTypedColumnDenseStringCodeColumn{
						Codes:               distinctCodes,
						GlobalCodes:         distinctCodes,
						GlobalCardinality:   distinctCardinality,
						GlobalCardinalityOK: true,
					},
					Predicates: []columnTypedColumnDensePredicatePart{
						{Codes: predicateCodes, Allowed: allowed},
						{Codes: predicateCodes, Allowed: allowed},
					},
				},
			},
		},
	}
	preview, err := runner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
	if err != nil {
		b.Fatalf("preview runDenseGroupCountDistinct: %v", err)
	}
	if len(preview.Groups) != activeGroups || preview.Diagnostics.ReduceRows != rows {
		b.Fatalf("preview groups=%d reduce=%d diagnostics=%+v", len(preview.Groups), preview.Diagnostics.ReduceRows, preview.Diagnostics)
	}
	b.SetBytes(rows)
	b.ReportAllocs()
	b.ResetTimer()
	var last ColumnPhysicalQueryDiagnostics
	for i := 0; i < b.N; i++ {
		result, err := runner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
		if err != nil {
			b.Fatalf("runDenseGroupCountDistinct: %v", err)
		}
		last = result.Diagnostics
	}
	b.StopTimer()
	b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(last.ReduceRows), "reduce_rows/op")
	b.ReportMetric(float64(last.DenseGroupCountDistinctPairBitWords), "pair_bit_words/op")
	if last.ScanNanos > 0 {
		b.ReportMetric(float64(last.RowsScanned)*1e9/float64(last.ScanNanos), "diag_rows_per_sec")
	}
}

func TestTypedColumnQ2SortedGroupedDistinctLocalDictionariesAndEmptyValues1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2LocalDictBatchA1950(), typedColumnQ2LocalDictBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	d, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, typedColumnQ2ClickHouseSortKey1950(), batches)
	defer closeFn()

	codesByGeneration := typedColumnQ1DictionaryCodeByGeneration1950(t, d, col, "did", "did:m")
	if len(codesByGeneration) < 2 {
		t.Fatalf("did:m dictionary codes by generation=%v want at least two", codesByGeneration)
	}
	seenCodes := make(map[int64]struct{}, len(codesByGeneration))
	for _, code := range codesByGeneration {
		seenCodes[code] = struct{}{}
	}
	if len(seenCodes) < 2 {
		t.Fatalf("did:m local dictionary codes=%v want differing local dictionary orders", codesByGeneration)
	}

	result, err := col.RunColumnPhysicalQuery(typedColumnQ2Request1950())
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 local dictionaries): %v", err)
	}
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	want := columnPhysicalJSONBenchQ2ReferenceCountsP0(events)
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "local dictionaries", result, rowHash, want)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "local dictionaries", result.Diagnostics, len(events), columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events), true)
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	if got["app.z"].Count != 4 || got["app.z"].Distinct != 2 {
		t.Fatalf("app.z counts=%+v want duplicate did:m counted once across different local dictionaries", got["app.z"])
	}
	if got[""].Count != 1 || got[""].Distinct != 1 {
		t.Fatalf("empty event counts=%+v want real empty group value", got[""])
	}
	if got["app.emptydid"].Count != 2 || got["app.emptydid"].Distinct != 1 {
		t.Fatalf("empty did counts=%+v want empty distinct value counted once", got["app.emptydid"])
	}

	runner, err := col.PrepareColumnPhysicalQuery(typedColumnQ2Request1950())
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q2 local dictionaries): %v", err)
	}
	defer func() { _ = runner.Close() }()
	assertTypedColumnQ2PreparedGlobalCodes1950(t, runner)
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared q2 local dictionaries: %v", err)
	}
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "prepared local dictionaries", prepared, rowHash, want)
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "prepared local dictionaries", prepared.Diagnostics, len(events), columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events), true)
}

func TestTypedColumnQ2SortedGroupedDistinctPrefixMismatchFallback1950(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{typedColumnQ2SortedBatchA1950(), typedColumnQ2SortedBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	mismatchSortKey := []ColumnSortKey{{Column: "kind"}, {Column: "collection"}, {Column: "operation"}, {Column: "did"}, {Column: "time_us"}}
	_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(t, mismatchSortKey, batches)
	defer closeFn()

	result, err := col.RunColumnPhysicalQuery(typedColumnQ2Request1950())
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2 prefix mismatch): %v", err)
	}
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	assertTypedColumnQ2SortedGroupedDistinctResult1950(t, "prefix mismatch", result, rowHash, columnPhysicalJSONBenchQ2ReferenceCountsP0(events))
	assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(t, "prefix mismatch", result.Diagnostics, len(events), columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", events), false)
	if result.Diagnostics.SortedGroupedDistinctFallbackReason != columnSortedGroupedDistinctFallbackSortKeyLayout {
		t.Fatalf("prefix mismatch diagnostics=%+v want sort-key-layout fallback", result.Diagnostics)
	}
}

func BenchmarkTypedColumnQ2SortedGroupedDistinct1950(b *testing.B) {
	events := typedColumnQ2BenchmarkEvents1950(65_536)
	cases := []struct {
		name     string
		sortKey  []ColumnSortKey
		prepared bool
		wantUsed bool
	}{
		{name: "direct/sorted_prefix", sortKey: typedColumnQ2ClickHouseSortKey1950(), wantUsed: true},
		{name: "prepared/sorted_prefix", sortKey: typedColumnQ2ClickHouseSortKey1950(), prepared: true, wantUsed: true},
		{name: "direct/primary_id_fallback", sortKey: nil},
		{name: "prepared/primary_id_fallback", sortKey: nil, prepared: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, col, closeFn := openTypedColumnSortKeyFixtureBatches1950(b, tc.sortKey, [][]columnPhysicalJSONBenchParityEventP0{events})
			defer closeFn()
			req := typedColumnQ2Request1950()
			preview, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			if preview.Diagnostics.SortedGroupedDistinctUsed != tc.wantUsed {
				b.Fatalf("preview sorted grouped-distinct used=%t want %t diagnostics=%+v", preview.Diagnostics.SortedGroupedDistinctUsed, tc.wantUsed, preview.Diagnostics)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			var runner *ColumnPhysicalQueryRunner
			if tc.prepared {
				runner, err = col.PrepareColumnPhysicalQuery(req)
				if err != nil {
					b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
				}
				defer func() { _ = runner.Close() }()
			}
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			var groups int
			for i := 0; i < b.N; i++ {
				var result ColumnPhysicalQueryResult
				if tc.prepared {
					result, err = runner.Run()
				} else {
					result, err = col.RunColumnPhysicalQuery(req)
				}
				if err != nil {
					b.Fatalf("q2 run: %v", err)
				}
				last = result.Diagnostics
				groups += len(result.Groups)
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no result groups")
			}
			b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
			b.ReportMetric(float64(last.RowsMatched), "rows_matched/op")
			b.ReportMetric(float64(last.ReduceRows), "reduce_rows/op")
			b.ReportMetric(float64(last.SortKeyMarkChecks), "mark_checks/op")
			b.ReportMetric(float64(last.SortKeyMarkSkips), "mark_skips/op")
			b.ReportMetric(float64(last.DecodedPayloadBytes), "decoded_bytes/op")
			if last.ScanNanos > 0 {
				b.ReportMetric(1e9/float64(last.ScanNanos), "diag_ops_per_sec")
				b.ReportMetric(float64(last.RowsScanned)*1e9/float64(last.ScanNanos), "diag_rows_per_sec")
			}
		})
	}
}

func typedColumnQ2Request1950() ColumnPhysicalQueryRequest {
	return ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
}

func typedColumnQ2ClickHouseSortKey1950() []ColumnSortKey {
	return []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
}

func typedColumnQ2SortedBatchA1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_800_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "a-post-shared", TimeUS: base + 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:shared"},
		{ID: "a-like-shared", TimeUS: base + 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:shared"},
		{ID: "a-post-a-1", TimeUS: base + 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:post-a"},
		{ID: "a-post-a-2", TimeUS: base + 21, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:post-a"},
		{ID: "a-kind-guard", TimeUS: base + 22, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:kind-guard"},
		{ID: "a-op-guard", TimeUS: base + 23, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.like", Did: "did:op-guard"},
	}
}

func typedColumnQ2SortedBatchB1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_800_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "b-post-shared", TimeUS: base + 31, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:shared"},
		{ID: "b-like-b", TimeUS: base + 32, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:like-b"},
		{ID: "b-like-shared", TimeUS: base + 33, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:shared"},
		{ID: "b-repost", TimeUS: base + 34, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.repost", Did: "did:repost"},
		{ID: "b-graph", TimeUS: base + 35, Kind: "commit", Operation: "create", Collection: "app.bsky.graph.follow", Did: "did:graph"},
	}
}

func typedColumnQ2SortedDiagnosticsBatches1950(repeats int) [][]columnPhysicalJSONBenchParityEventP0 {
	return [][]columnPhysicalJSONBenchParityEventP0{
		typedColumnQ2RepeatSortedBatch1950(typedColumnQ2SortedBatchA1950(), repeats),
		typedColumnQ2RepeatSortedBatch1950(typedColumnQ2SortedBatchB1950(), repeats),
	}
}

func typedColumnQ2RepeatSortedBatch1950(base []columnPhysicalJSONBenchParityEventP0, repeats int) []columnPhysicalJSONBenchParityEventP0 {
	if repeats <= 0 {
		repeats = 1
	}
	out := make([]columnPhysicalJSONBenchParityEventP0, 0, len(base)*repeats)
	for i := 0; i < repeats; i++ {
		suffix := fmt.Sprintf(".r%04d", i)
		for _, event := range base {
			event.ID = fmt.Sprintf("%s%s", event.ID, suffix)
			event.TimeUS += int64(i) * 1_000
			if event.Kind == "commit" && event.Operation == "create" {
				event.Collection += suffix
				event.Did += suffix
			}
			out = append(out, event)
		}
	}
	return out
}

func typedColumnQ2LocalDictBatchA1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_810_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "a-z-m-1", TimeUS: base + 1, Kind: "commit", Operation: "create", Collection: "app.z", Did: "did:m"},
		{ID: "a-z-m-2", TimeUS: base + 2, Kind: "commit", Operation: "create", Collection: "app.z", Did: "did:m"},
		{ID: "a-a-x", TimeUS: base + 3, Kind: "commit", Operation: "create", Collection: "app.a", Did: "did:x"},
		{ID: "a-empty-event", TimeUS: base + 4, Kind: "commit", Operation: "create", Collection: "", Did: "did:empty-event"},
		{ID: "a-kind-guard", TimeUS: base + 5, Kind: "identity", Operation: "create", Collection: "app.z", Did: "did:guard"},
	}
}

func typedColumnQ2LocalDictBatchB1950() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_810_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "b-z-m", TimeUS: base + 6, Kind: "commit", Operation: "create", Collection: "app.z", Did: "did:m"},
		{ID: "b-z-n", TimeUS: base + 7, Kind: "commit", Operation: "create", Collection: "app.z", Did: "did:n"},
		{ID: "b-a-x", TimeUS: base + 8, Kind: "commit", Operation: "create", Collection: "app.a", Did: "did:x"},
		{ID: "b-empty-did-1", TimeUS: base + 9, Kind: "commit", Operation: "create", Collection: "app.emptydid", Did: ""},
		{ID: "b-empty-did-2", TimeUS: base + 10, Kind: "commit", Operation: "create", Collection: "app.emptydid", Did: ""},
		{ID: "b-op-guard", TimeUS: base + 11, Kind: "commit", Operation: "delete", Collection: "app.z", Did: "did:a"},
		{ID: "b-kind-guard", TimeUS: base + 12, Kind: "identity", Operation: "create", Collection: "app.z", Did: "did:b"},
	}
}

func typedColumnQ2BenchmarkEvents1950(rows int) []columnPhysicalJSONBenchParityEventP0 {
	collections := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.graph.follow"}
	events := make([]columnPhysicalJSONBenchParityEventP0, rows)
	for i := range events {
		kind := "commit"
		operation := "create"
		if i%17 == 0 {
			kind = "identity"
		}
		if i%29 == 0 {
			operation = "delete"
		}
		events[i] = columnPhysicalJSONBenchParityEventP0{
			ID:         fmt.Sprintf("bench-%06d", i),
			TimeUS:     1_900_000_000_000_000 + int64(i),
			Kind:       kind,
			Operation:  operation,
			Collection: collections[i%len(collections)],
			Did:        fmt.Sprintf("did:%06d", i%8192),
		}
	}
	return events
}

func flattenColumnPhysicalEvents1950(batches [][]columnPhysicalJSONBenchParityEventP0) []columnPhysicalJSONBenchParityEventP0 {
	var total int
	for _, batch := range batches {
		total += len(batch)
	}
	events := make([]columnPhysicalJSONBenchParityEventP0, 0, total)
	for _, batch := range batches {
		events = append(events, batch...)
	}
	return events
}

func assertTypedColumnQ2PreparedGlobalCodes1950(tb testing.TB, runner *ColumnPhysicalQueryRunner) {
	tb.Helper()
	if runner == nil || runner.typedColumn == nil {
		tb.Fatalf("prepared q2 runner missing typed-column state")
	}
	if len(runner.typedColumn.parts) < 2 {
		tb.Fatalf("prepared q2 parts=%d want multiple local dictionaries", len(runner.typedColumn.parts))
	}
	didMRank := -1
	didMParts := 0
	for partIdx := range runner.typedColumn.parts {
		part := runner.typedColumn.parts[partIdx].SortedGroupedDistinct
		if part == nil {
			tb.Fatalf("part %d missing sorted grouped-distinct state", partIdx)
		}
		if len(part.Group.GlobalCodes) != part.Rows || len(part.Distinct.GlobalCodes) != part.Rows {
			tb.Fatalf("part %d global code rows group=%d distinct=%d want %d", partIdx, len(part.Group.GlobalCodes), len(part.Distinct.GlobalCodes), part.Rows)
		}
		if !sort.StringsAreSorted(part.Group.GlobalDictionary) || !sort.StringsAreSorted(part.Distinct.GlobalDictionary) {
			tb.Fatalf("part %d global dictionaries not lexicographically sorted group=%v distinct=%v", partIdx, part.Group.GlobalDictionary, part.Distinct.GlobalDictionary)
		}
		for row, code := range part.Group.GlobalCodes {
			if int(code) >= len(part.Group.GlobalDictionary) {
				tb.Fatalf("part %d group global code row=%d code=%d outside cardinality=%d", partIdx, row, code, len(part.Group.GlobalDictionary))
			}
		}
		for row, code := range part.Distinct.GlobalCodes {
			if int(code) >= len(part.Distinct.GlobalDictionary) {
				tb.Fatalf("part %d distinct global code row=%d code=%d outside cardinality=%d", partIdx, row, code, len(part.Distinct.GlobalDictionary))
			}
		}
		localDidM := -1
		for code, value := range part.Distinct.Dictionary {
			if value == "did:m" {
				localDidM = code
				break
			}
		}
		if localDidM < 0 {
			continue
		}
		partDidMRank := -1
		for rank, value := range part.Distinct.GlobalDictionary {
			if value == "did:m" {
				partDidMRank = rank
				break
			}
		}
		if partDidMRank < 0 {
			tb.Fatalf("part %d distinct global dictionary missing did:m", partIdx)
		}
		if didMRank < 0 {
			didMRank = partDidMRank
		} else if didMRank != partDidMRank {
			tb.Fatalf("did:m global rank part %d=%d want %d", partIdx, partDidMRank, didMRank)
		}
		seenDidMRow := false
		for row, localCode := range part.Distinct.Codes {
			if localCode == int64(localDidM) {
				seenDidMRow = true
				if part.Distinct.GlobalCodes[row] != uint32(partDidMRank) {
					tb.Fatalf("part %d did:m row=%d global code=%d want %d", partIdx, row, part.Distinct.GlobalCodes[row], partDidMRank)
				}
			}
		}
		if !seenDidMRow {
			tb.Fatalf("part %d local did:m code=%d not referenced by any row", partIdx, localDidM)
		}
		didMParts++
	}
	if didMParts < 2 {
		tb.Fatalf("did:m appeared in %d parts want at least two to prove cross-part global rank reuse", didMParts)
	}
}

func assertTypedColumnQ2DensePreparedGlobalCodes1950(tb testing.TB, runner *ColumnPhysicalQueryRunner) {
	tb.Helper()
	if runner == nil || runner.typedColumn == nil {
		tb.Fatalf("prepared q2 runner missing typed-column state")
	}
	if len(runner.typedColumn.parts) < 2 {
		tb.Fatalf("prepared q2 parts=%d want multiple local dictionaries", len(runner.typedColumn.parts))
	}
	didMRank := -1
	didMParts := 0
	for partIdx := range runner.typedColumn.parts {
		part := runner.typedColumn.parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			tb.Fatalf("part %d missing dense grouped count-distinct state", partIdx)
		}
		if len(part.Group.GlobalCodes) != part.Rows || len(part.Distinct.GlobalCodes) != part.Rows {
			tb.Fatalf("part %d global code rows group=%d distinct=%d want %d", partIdx, len(part.Group.GlobalCodes), len(part.Distinct.GlobalCodes), part.Rows)
		}
		if !sort.StringsAreSorted(part.Group.GlobalDictionary) {
			tb.Fatalf("part %d group global dictionary not lexicographically sorted group=%v", partIdx, part.Group.GlobalDictionary)
		}
		if !part.Group.GlobalCardinalityOK || part.Group.GlobalCardinality != len(part.Group.GlobalDictionary) {
			tb.Fatalf("part %d group global cardinality=%d ok=%t want dictionary=%d", partIdx, part.Group.GlobalCardinality, part.Group.GlobalCardinalityOK, len(part.Group.GlobalDictionary))
		}
		if part.Distinct.GlobalDictionary != nil {
			tb.Fatalf("part %d distinct global dictionary allocated=%d want nil", partIdx, len(part.Distinct.GlobalDictionary))
		}
		if !part.Distinct.GlobalCardinalityOK {
			tb.Fatalf("part %d distinct global cardinality not prepared", partIdx)
		}
		if len(part.Distinct.GlobalLocalRanks) != len(part.Distinct.Dictionary) {
			tb.Fatalf("part %d distinct global local ranks=%d want dictionary=%d", partIdx, len(part.Distinct.GlobalLocalRanks), len(part.Distinct.Dictionary))
		}
		localDidM := -1
		for code, value := range part.Distinct.Dictionary {
			if value == "did:m" {
				localDidM = code
				break
			}
		}
		if localDidM < 0 {
			continue
		}
		if localDidM >= len(part.Distinct.GlobalLocalRanks) {
			tb.Fatalf("part %d local did:m code=%d outside distinct global local ranks=%d", partIdx, localDidM, len(part.Distinct.GlobalLocalRanks))
		}
		partDidMRank := int(part.Distinct.GlobalLocalRanks[localDidM])
		if partDidMRank >= part.Distinct.GlobalCardinality {
			tb.Fatalf("part %d did:m global rank=%d outside distinct cardinality=%d", partIdx, partDidMRank, part.Distinct.GlobalCardinality)
		}
		if didMRank < 0 {
			didMRank = partDidMRank
		} else if didMRank != partDidMRank {
			tb.Fatalf("did:m global rank part %d=%d want %d", partIdx, partDidMRank, didMRank)
		}
		seenDidMRow := false
		for row, localCode := range part.Distinct.Codes {
			if localCode == uint32(localDidM) && columnTypedColumnDenseCodeValid(part.Distinct.Valid, row) {
				seenDidMRow = true
				if part.Distinct.GlobalCodes[row] != uint32(partDidMRank) {
					tb.Fatalf("part %d did:m row=%d global code=%d want %d", partIdx, row, part.Distinct.GlobalCodes[row], partDidMRank)
				}
			}
		}
		if !seenDidMRow {
			tb.Fatalf("part %d local did:m code=%d not referenced by any row", partIdx, localDidM)
		}
		didMParts++
	}
	if didMParts < 2 {
		tb.Fatalf("did:m appeared in %d parts want at least two to prove cross-part global rank reuse", didMParts)
	}
}

func openTypedColumnSortKeyFixtureBatches1950(tb testing.TB, sortKey []ColumnSortKey, batches [][]columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func()) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: typedColumnSortKeyConfig1948(sortKey)}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	for batchIdx, batch := range batches {
		ids := make([][]byte, len(batch))
		docs := make([][]byte, len(batch))
		for i, event := range batch {
			ids[i] = []byte(event.ID)
			docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch[%d]: %v", batchIdx, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint before reopen: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("Close before reopen: %v", err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open reopened DB: %v", err)
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		tb.Fatalf("OpenCollection reopened: %v", err)
	}
	return reopen, reopened, func() { _ = reopen.Close() }
}

func assertTypedColumnQ2SortedGroupedDistinctResult1950(tb testing.TB, label string, result ColumnPhysicalQueryResult, rowHash uint64, want map[string]columnPhysicalJSONBenchQ2CountP0) {
	tb.Helper()
	gotHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q2", result.Groups))
	if gotHash != rowHash {
		tb.Fatalf("%s q2 hash=%016x want row-scan %016x groups=%+v", label, gotHash, rowHash, result.Groups)
	}
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	if !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		tb.Fatalf("%s q2 counts=%v want %v groups=%+v", label, got, want, result.Groups)
	}
}

func assertTypedColumnQ2SortedGroupedDistinctDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, totalRows, matchedRows int, wantSortedUsed bool) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics=%+v want typed-column source without storage fallback", label, diag)
	}
	if diag.PredicateCount != 2 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s predicate diagnostics=%+v want predicates=2 matched/reduced=%d", label, diag, matchedRows)
	}
	if diag.RowsScanned <= 0 || diag.RowsScanned > totalRows {
		tb.Fatalf("%s rows scanned diagnostics=%+v want 1..%d", label, diag, totalRows)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialization diagnostics=%+v want no row/document materialization", label, diag)
	}
	if diag.SortedGroupedDistinctUsed != wantSortedUsed {
		tb.Fatalf("%s sorted grouped-distinct used=%t want %t diagnostics=%+v", label, diag.SortedGroupedDistinctUsed, wantSortedUsed, diag)
	}
	if wantSortedUsed {
		if !diag.SortKeyPrefixPlanned || diag.SortKeyPrefixLiterals != 2 || !equalStrings1949(diag.SortKeyPrefixColumns, []string{"kind", "operation"}) {
			tb.Fatalf("%s prefix diagnostics=%+v want kind/operation sorted prefix", label, diag)
		}
		if !diag.SortedGroupedDistinctReady || diag.SortedGroupedDistinctFallbackReason != columnSortedGroupedDistinctFallbackNone {
			tb.Fatalf("%s grouped-distinct diagnostics=%+v want ready/no fallback", label, diag)
		}
		if diag.SortKeyMarkChecks == 0 {
			tb.Fatalf("%s mark diagnostics=%+v want sorted-prefix mark checks", label, diag)
		}
	}
}

func assertTypedColumnQ2DenseGroupCountDistinctDiagnostics1950(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, totalRows, matchedRows int, wantReducer string) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics=%+v want typed-column source without storage fallback", label, diag)
	}
	if diag.PredicateCount != 2 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s predicate diagnostics=%+v want predicates=2 matched/reduced=%d", label, diag, matchedRows)
	}
	if diag.RowsScanned != totalRows {
		tb.Fatalf("%s rows scanned diagnostics=%+v want full no-sort scan %d", label, diag, totalRows)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialization diagnostics=%+v want no row/document materialization", label, diag)
	}
	if !diag.DenseGroupCountDistinctUsed || diag.SortedGroupedDistinctUsed || diag.DenseGroupCountUsed || diag.DenseGroupHourCountUsed || diag.DenseInt64SpanUsed || diag.TimeOrderTopKUsed {
		tb.Fatalf("%s diagnostics=%+v want only dense grouped count-distinct path", label, diag)
	}
	if diag.DenseGroupCountDistinctReducer != wantReducer || diag.DenseGroupCountDistinctGroups == 0 || diag.DenseGroupCountDistinctValues == 0 || diag.DenseGroupCountDistinctPairBitWords == 0 {
		tb.Fatalf("%s dense grouped count-distinct reducer diagnostics=%+v want %s with cardinalities", label, diag, wantReducer)
	}
	if diag.SortKeyMarkChecks != 0 || diag.SortKeyMarkSkips != 0 {
		tb.Fatalf("%s mark diagnostics=%+v want no sort-key pruning in dense no-sort path", label, diag)
	}
}

func assertTypedColumnQ2SortedGroupedDistinctPostPrepareDiagnostics3324(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, want bool) {
	tb.Helper()
	total := diag.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos +
		diag.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos +
		diag.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos +
		diag.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos
	if !want {
		if total != 0 {
			tb.Fatalf("%s sorted grouped-distinct post-prepare split nanos=%d want 0 diagnostics=%+v", label, total, diag)
		}
		return
	}
	// Tiny fixture phases can round to zero on coarse platform timers. This live
	// path gate checks that split accounting is bounded; the merge additivity
	// test covers all four individual fields structurally.
	if diag.TypedColumnPreparePostPrepareNanos < total {
		tb.Fatalf("%s typed-column post-prepare nanos=%d want >= split total %d diagnostics=%+v", label, diag.TypedColumnPreparePostPrepareNanos, total, diag)
	}
}
