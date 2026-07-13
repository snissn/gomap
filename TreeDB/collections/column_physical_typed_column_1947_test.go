package collections

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnPhysicalJSONBenchTypedColumnPartDirectPreparedReopen1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	d, collection, closeFn, preCloseTypedRefs := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	postReopenTypedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, collection))
	assertColumnAssetRefsEqual1755(t, preCloseTypedRefs, postReopenTypedRefs)
	assertTypedColumnManifestShape1755(t, d, collection, 1, 1)

	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, collection, len(events))
	assertColumnPhysicalJSONBenchRowScanPreservesFullPredicateColumnsP0(t, events, scanned)

	commitCreate := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
	}
	postCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{Column: "collection", Value: "app.bsky.feed.post"})
	feedCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{
		Column: "collection",
		Kind:   ColumnPhysicalQueryPredicateInList,
		Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"},
	})

	cases := []struct {
		name            string
		req             ColumnPhysicalQueryRequest
		wantPredicates  int
		wantMatchedRows int
		wantReduceRows  int
	}{
		{
			name:            "q1",
			req:             ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"},
			wantPredicates:  0,
			wantMatchedRows: 0,
			wantReduceRows:  len(scanned),
		},
		{
			name: "q2",
			req: ColumnPhysicalQueryRequest{
				Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
				GroupColumn:    "collection",
				DistinctColumn: "did",
				Predicates:     commitCreate,
			},
			wantPredicates:  len(commitCreate),
			wantMatchedRows: columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", scanned),
			wantReduceRows:  columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", scanned),
		},
		{
			name: "q3",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupHourCount,
				GroupColumn: "collection",
				ValueColumn: "time_us",
				Predicates:  feedCreate,
			},
			wantPredicates:  len(feedCreate),
			wantMatchedRows: columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", scanned),
			wantReduceRows:  columnPhysicalJSONBenchReferenceMatchedRowsP0("q3", scanned),
		},
		{
			name: "q4a",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupMinInt64,
				GroupColumn: "did",
				ValueColumn: "time_us",
				Predicates:  postCreate,
			},
			wantPredicates:  len(postCreate),
			wantMatchedRows: columnPhysicalJSONBenchReferenceMatchedRowsP0("q4a", scanned),
			wantReduceRows:  columnPhysicalJSONBenchReferenceMatchedRowsP0("q4a", scanned),
		},
		{
			name: "q4b",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupMaxInt64,
				GroupColumn: "did",
				ValueColumn: "time_us",
				Predicates:  postCreate,
			},
			wantPredicates:  len(postCreate),
			wantMatchedRows: columnPhysicalJSONBenchReferenceMatchedRowsP0("q4b", scanned),
			wantReduceRows:  columnPhysicalJSONBenchReferenceMatchedRowsP0("q4b", scanned),
		},
		{
			name: "q5",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQueryGroupInt64Span,
				GroupColumn: "did",
				ValueColumn: "time_us",
				Predicates:  postCreate,
			},
			wantPredicates:  len(postCreate),
			wantMatchedRows: columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned),
			wantReduceRows:  columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", scanned),
		},
		{
			name: "sum_time_second_of_day_square",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQuerySumSecondOfDaySquare,
				ValueColumn: "time_us",
			},
			wantPredicates:  0,
			wantMatchedRows: 0,
			wantReduceRows:  len(scanned),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0(tc.name, scanned))

			direct, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery(%s): %v", tc.name, err)
			}
			assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(t, tc.name, direct.Diagnostics, len(scanned), tc.wantPredicates, tc.wantMatchedRows, tc.wantReduceRows)
			directHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0(tc.name, direct.Groups))
			if directHash != rowHash {
				t.Fatalf("%s direct hash=%016x want row scan %016x groups=%+v", tc.name, directHash, rowHash, direct.Groups)
			}

			runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery(%s): %v", tc.name, err)
			}
			defer func() { _ = runner.Close() }()
			for run := 0; run < 2; run++ {
				prepared, err := runner.Run()
				if err != nil {
					t.Fatalf("prepared %s run %d: %v", tc.name, run, err)
				}
				assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(t, tc.name, prepared.Diagnostics, len(scanned), tc.wantPredicates, tc.wantMatchedRows, tc.wantReduceRows)
				preparedHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0(tc.name, prepared.Groups))
				if preparedHash != rowHash {
					t.Fatalf("%s prepared run %d hash=%016x want row scan %016x groups=%+v", tc.name, run, preparedHash, rowHash, prepared.Groups)
				}
			}
		})
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartStoresFullQ2Columns1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	d, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	rows := typedColumnPartRowsForGeneration1778(t, d, collection, 1)
	if len(rows) != len(events) {
		t.Fatalf("typed rows=%d want events=%d", len(rows), len(events))
	}
	for i, event := range events {
		row := rows[i]
		if row.Values["kind"].String != event.Kind || row.Values["operation"].String != event.Operation || row.Values["collection"].String != event.Collection || row.Values["did"].String != event.Did {
			t.Fatalf("row %d typed q2 columns kind=%q operation=%q collection=%q did=%q want %+v", i, row.Values["kind"].String, row.Values["operation"].String, row.Values["collection"].String, row.Values["did"].String, event)
		}
		if (event.Kind != "commit" || event.Operation != "create") && (row.Values["collection"].String == "" || row.Values["did"].String == "") {
			t.Fatalf("row %d used q2 masking/sentinel payload: %+v", i, row.Values)
		}
	}

	q2 := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	result, err := collection.RunColumnPhysicalQuery(q2)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2): %v", err)
	}
	got := columnPhysicalJSONBenchQ2CountsP0(result.Groups)
	want := columnPhysicalJSONBenchQ2ReferenceCountsP0(events)
	if !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 counts/distinct=%v want %v groups=%+v", got, want, result.Groups)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.PredicateCount != len(q2.Predicates) {
		t.Fatalf("q2 diagnostics=%+v want typed-column source and real predicates", result.Diagnostics)
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartNullableFullDataMissingStrings2165(t *testing.T) {
	docs := []string{
		`{"time_us":1000,"kind":"commit","did":"did_a","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":2000,"kind":"commit","did":"did_b","commit":{"operation":"create"}}`,
		`{"time_us":3000,"did":"did_c"}`,
		`{"time_us":4000,"kind":"commit","did":"did_a","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":5000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":6000,"kind":"commit","did":"did_d","commit":{"operation":"delete","collection":"app.bsky.feed.post"}}`,
	}
	_, collection, closeFn := openColumnPhysicalJSONBenchNullableFullDataFixture2165(t, docs)
	defer closeFn()

	q1 := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "event"}
	q1Result := runNullableFullDataDenseQ1Query3075(t, collection, "q1", q1, len(docs), 0, 0, len(docs))
	if got, want := columnPhysicalGroupCountMap2165(q1Result.Groups), map[string]int{"": 2, "app.bsky.feed.post": 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("q1 groups=%v want %v raw=%+v", got, want, q1Result.Groups)
	}

	commitCreate := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
	}
	q2 := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "event",
		DistinctColumn: "did",
		Predicates:     commitCreate,
	}
	q2Result := runNullableFullDataDenseQ2Query2165(t, collection, "q2", q2, len(docs), len(commitCreate), 4, 4)
	wantQ2 := map[string]columnPhysicalCountDistinct2165{
		"":                   {Count: 1, Distinct: 1},
		"app.bsky.feed.post": {Count: 3, Distinct: 2},
	}
	if got := columnPhysicalCountDistinctMap2165(q2Result.Groups); !reflect.DeepEqual(got, wantQ2) {
		t.Fatalf("q2 groups=%v want %v raw=%+v", got, wantQ2, q2Result.Groups)
	}

	feedCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{
		Column: "event",
		Kind:   ColumnPhysicalQueryPredicateInList,
		Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"},
	})
	q3 := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupHourCount,
		GroupColumn: "event",
		ValueColumn: "time_us",
		Predicates:  feedCreate,
	}
	q3Result := runNullableFullDataDenseQ3Query3078(t, collection, "q3", q3, len(docs), len(feedCreate), 3, 3)
	if got, want := columnPhysicalGroupHourMap3078(q3Result.Groups), map[string]map[int]int{"app.bsky.feed.post": {0: 3}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("q3 groups=%v want %v raw=%+v", got, want, q3Result.Groups)
	}

	postCreate := append(append([]ColumnPhysicalQueryPredicate(nil), commitCreate...), ColumnPhysicalQueryPredicate{Column: "event", Value: "app.bsky.feed.post"})
	q4 := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupMinInt64,
		GroupColumn: "did",
		ValueColumn: "time_us",
		Predicates:  postCreate,
		TopK:        2,
		TopKOrder:   ColumnPhysicalQueryTopKInt64Asc,
	}
	q4Result := runNullableFullDataQuery2165(t, collection, "q4a", q4, len(docs), len(postCreate), 3, 3)
	if got, want := columnPhysicalInt64Map2165(q4Result.Groups), map[string]int64{"did_a": 1000, "": 5000}; !reflect.DeepEqual(got, want) {
		t.Fatalf("q4 groups=%v want %v raw=%+v", got, want, q4Result.Groups)
	}
	if q4Result.Diagnostics.TimeOrderTopKUsed {
		t.Fatalf("q4 diagnostics used nullable-unsafe time-order topK: %+v", q4Result.Diagnostics)
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartNullableFullDataAllMissingQ13075(t *testing.T) {
	docs := []string{
		`{"time_us":1000,"kind":"commit","did":"did_a","commit":{"operation":"create"}}`,
		`{"time_us":2000}`,
	}
	_, collection, closeFn := openColumnPhysicalJSONBenchNullableFullDataFixture2165(t, docs)
	defer closeFn()

	q1 := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "event"}
	q1Result := runNullableFullDataDenseQ1Query3075(t, collection, "q1 all missing", q1, len(docs), 0, 0, len(docs))
	if got, want := columnPhysicalGroupCountMap2165(q1Result.Groups), map[string]int{"": len(docs)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("q1 all-missing groups=%v want %v raw=%+v", got, want, q1Result.Groups)
	}

	q3 := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, GroupColumn: "event", ValueColumn: "time_us"}
	q3Result := runNullableFullDataDenseQ3Query3078(t, collection, "q3 all missing", q3, len(docs), 0, 0, len(docs))
	if got, want := columnPhysicalGroupHourMap3078(q3Result.Groups), map[string]map[int]int{"": {0: len(docs)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("q3 all-missing groups=%v want %v raw=%+v", got, want, q3Result.Groups)
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartNullableFullDataTopKFastPaths2878(t *testing.T) {
	docs := []string{
		`{"time_us":1000,"kind":"commit","did":"did_a","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":1050,"kind":"commit","did":"did_x","commit":{"collection":"app.bsky.feed.post"}}`,
		`{"time_us":1100,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":1200,"kind":"commit","did":"did_b","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":2000,"kind":"commit","did":"did_a","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":2200,"kind":"commit","did":"did_delete","commit":{"operation":"delete","collection":"app.bsky.feed.post"}}`,
		`{"time_us":2500,"kind":"commit","did":"did_c","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"time_us":3000,"kind":"commit","did":"did_c","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
	}
	_, collection, closeFn := openColumnPhysicalJSONBenchNullableFullDataFixture2165(t, docs)
	defer closeFn()

	postCreate := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
		{Column: "event", Value: "app.bsky.feed.post"},
	}
	q4 := ColumnPhysicalQueryRequest{
		Kind:              ColumnPhysicalQueryGroupMinInt64,
		GroupColumn:       "did",
		ValueColumn:       "time_us",
		Predicates:        postCreate,
		TopK:              2,
		TopKOrder:         ColumnPhysicalQueryTopKInt64Asc,
		SkipEmptyGroupKey: true,
	}
	q4Want := []ColumnPhysicalQueryGroup{{Key: "did_a", Int64: 1000}, {Key: "did_b", Int64: 1200}}
	runNullableFullDataTopKFastPath2878(t, collection, "q4", q4, q4Want, func(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
		tb.Helper()
		assertNullableFullDataTopKBaseDiagnostics2878(tb, label, diag, len(postCreate), q4.TopK, string(q4.TopKOrder), len(q4Want))
		if !diag.TimeOrderTopKUsed || diag.DenseInt64SpanUsed {
			tb.Fatalf("%s diagnostics=%+v want time-order topK only", label, diag)
		}
		if diag.RowsScanned != 4 || diag.RowsMatched != 3 || diag.ReduceRows != 3 {
			tb.Fatalf("%s diagnostics=%+v want early q4 scan/match/reduce 4/3/3", label, diag)
		}
		if diag.TopKCandidates != len(q4Want) {
			tb.Fatalf("%s diagnostics=%+v want topK candidates=%d", label, diag, len(q4Want))
		}
	})

	q5 := ColumnPhysicalQueryRequest{
		Kind:              ColumnPhysicalQueryGroupInt64Span,
		GroupColumn:       "did",
		ValueColumn:       "time_us",
		Predicates:        postCreate,
		TopK:              2,
		TopKOrder:         ColumnPhysicalQueryTopKInt64Desc,
		SkipEmptyGroupKey: true,
	}
	q5Want := []ColumnPhysicalQueryGroup{{Key: "did_a", Int64: 1000}, {Key: "did_c", Int64: 500}}
	runNullableFullDataTopKFastPath2878(t, collection, "q5", q5, q5Want, func(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
		tb.Helper()
		assertNullableFullDataTopKBaseDiagnostics2878(tb, label, diag, len(postCreate), q5.TopK, string(q5.TopKOrder), len(q5Want))
		if !diag.DenseInt64SpanUsed || diag.TimeOrderTopKUsed {
			tb.Fatalf("%s diagnostics=%+v want dense int64-span only", label, diag)
		}
		if diag.RowsScanned != len(docs) || diag.RowsMatched != 6 || diag.ReduceRows != 6 {
			tb.Fatalf("%s diagnostics=%+v want q5 scan/match/reduce %d/6/6", label, diag, len(docs))
		}
		if diag.TopKCandidates != 3 {
			tb.Fatalf("%s diagnostics=%+v want three non-empty topK candidates", label, diag)
		}
	})
	assertTypedColumnQ5OneShotDenseDictionaryValuesByCode3175(t, collection, q5, false)

	missingOperation := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: ""},
		{Column: "event", Value: "app.bsky.feed.post"},
	}
	q4MissingOperation := ColumnPhysicalQueryRequest{
		Kind:              ColumnPhysicalQueryGroupMinInt64,
		GroupColumn:       "did",
		ValueColumn:       "time_us",
		Predicates:        missingOperation,
		TopK:              1,
		TopKOrder:         ColumnPhysicalQueryTopKInt64Asc,
		SkipEmptyGroupKey: true,
	}
	q4MissingOperationWant := []ColumnPhysicalQueryGroup{{Key: "did_x", Int64: 1050}}
	runNullableFullDataTopKFastPath2878(t, collection, "q4 missing operation", q4MissingOperation, q4MissingOperationWant, func(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
		tb.Helper()
		assertNullableFullDataTopKBaseDiagnostics2878(tb, label, diag, len(missingOperation), q4MissingOperation.TopK, string(q4MissingOperation.TopKOrder), len(q4MissingOperationWant))
		if !diag.TimeOrderTopKUsed || diag.DenseInt64SpanUsed {
			tb.Fatalf("%s diagnostics=%+v want time-order topK only", label, diag)
		}
		if diag.RowsMatched != 1 || diag.ReduceRows != 1 {
			tb.Fatalf("%s diagnostics=%+v want nullable missing operation to match exactly one row", label, diag)
		}
	})

	q5MissingOperation := ColumnPhysicalQueryRequest{
		Kind:              ColumnPhysicalQueryGroupInt64Span,
		GroupColumn:       "did",
		ValueColumn:       "time_us",
		Predicates:        missingOperation,
		TopK:              1,
		TopKOrder:         ColumnPhysicalQueryTopKInt64Desc,
		SkipEmptyGroupKey: true,
	}
	q5MissingOperationWant := []ColumnPhysicalQueryGroup{{Key: "did_x", Int64: 0}}
	runNullableFullDataTopKFastPath2878(t, collection, "q5 missing operation", q5MissingOperation, q5MissingOperationWant, func(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
		tb.Helper()
		assertNullableFullDataTopKBaseDiagnostics2878(tb, label, diag, len(missingOperation), q5MissingOperation.TopK, string(q5MissingOperation.TopKOrder), len(q5MissingOperationWant))
		if !diag.DenseInt64SpanUsed || diag.TimeOrderTopKUsed {
			tb.Fatalf("%s diagnostics=%+v want dense int64-span only", label, diag)
		}
		if diag.RowsMatched != 1 || diag.ReduceRows != 1 {
			tb.Fatalf("%s diagnostics=%+v want nullable missing operation to match exactly one row", label, diag)
		}
	})
}

func TestColumnPhysicalJSONBenchTypedColumnPartEdgeShapes1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	sameColumn, err := collection.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "collection", DistinctColumn: "collection"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery same-column count-distinct: %v", err)
	}
	if sameColumn.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || len(sameColumn.Groups) == 0 {
		t.Fatalf("same-column count-distinct result=%+v diagnostics=%+v want typed source groups", sameColumn.Groups, sameColumn.Diagnostics)
	}
	for _, group := range sameColumn.Groups {
		if group.Count != 1 {
			t.Fatalf("same-column count-distinct group=%+v want distinct count 1", group)
		}
	}

	hourCount, err := collection.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery hour-count: %v", err)
	}
	if hourCount.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || len(hourCount.Groups) == 0 {
		t.Fatalf("hour-count result=%+v diagnostics=%+v want typed source groups", hourCount.Groups, hourCount.Diagnostics)
	}
	for _, group := range hourCount.Groups {
		if group.Key == "" || group.Count == 0 || group.Hour != 0 {
			t.Fatalf("hour-count group=%+v want public key/count shape without group-hour metadata", group)
		}
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartAssetFailureFailClosed1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	cases := []struct {
		name string
		fail func(testing.TB, *backenddb.DB, *Collection, ColumnAssetRef)
	}{
		{name: "missing_truncated_payload", fail: removeTypedColumnAssetPayload1755},
		{name: "corrupt_checksum", fail: func(tb testing.TB, d *backenddb.DB, _ *Collection, ref ColumnAssetRef) {
			corruptTypedColumnAssetPayload1755(tb, d, ref)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, collection, closeFn, typedRefs := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
			defer closeFn()
			if len(typedRefs) != 1 {
				t.Fatalf("typed refs=%+v want one", typedRefs)
			}
			tc.fail(t, d, collection, typedRefs[0])

			_, err := collection.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
			if err == nil {
				t.Fatalf("RunColumnPhysicalQuery after %s typed_column_part succeeded; want fail-closed error", tc.name)
			}
			errText := err.Error()
			if !strings.Contains(errText, "typed-column") || (!strings.Contains(errText, "checksum") && !strings.Contains(errText, "short")) {
				t.Fatalf("%s typed_column_part error=%v want typed-column context plus checksum/short-read context", tc.name, err)
			}
		})
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartMultipartRefFailsClosed1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	d, collection, closeFn, typedRefs := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()
	if len(typedRefs) != 1 {
		t.Fatalf("typed refs=%+v want one primary typed_column_part", typedRefs)
	}
	extraRef := publishTypedColumnMultipartPartRef1787(t, d, collection, 3)

	_, err := collection.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"})
	if err == nil {
		t.Fatalf("RunColumnPhysicalQuery with multipart typed_column_part ref succeeded; want fail-closed error")
	}
	errText := err.Error()
	if !strings.Contains(errText, fmt.Sprintf("generation=%d", extraRef.Generation)) ||
		!strings.Contains(errText, fmt.Sprintf("part_id=%d", extraRef.PartID)) ||
		!strings.Contains(errText, "multipart/non-primary typed_column_part refs are unsupported by this physical query path") {
		t.Fatalf("multipart typed_column_part error=%v want generation, part_id, and unsupported physical-query-path context", err)
	}
}

func TestColumnPhysicalJSONBenchTypedColumnPartDecodeMismatchesFailClosed1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	d, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotViewWithSidecars: %v", err)
	}
	if len(view.TypedColumnPartRefs) != 1 || len(view.AssetRefs) != 1 {
		t.Fatalf("typed refs=%+v physical refs=%+v want one paired generation", view.TypedColumnPartRefs, view.AssetRefs)
	}
	plan, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req)
	if err != nil || !candidate {
		t.Fatalf("planColumnTypedColumnPhysicalQuery candidate=%v err=%v", candidate, err)
	}
	typedRef := view.TypedColumnPartRefs[0]
	physicalRef := view.AssetRefs[0]
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), typedRef.Ref)
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}

	t.Run("typed_ref_row_count_mismatch", func(t *testing.T) {
		staleTypedRef := typedRef
		staleTypedRef.Rows++
		_, err := decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash, staleTypedRef, physicalRef, raw, false)
		if err == nil || !strings.Contains(err.Error(), "image/ref mismatch") {
			t.Fatalf("decode row-count mismatch err=%v want image/ref mismatch", err)
		}
	})
	t.Run("physical_ref_row_count_mismatch", func(t *testing.T) {
		stalePhysicalRef := physicalRef
		stalePhysicalRef.Rows++
		_, err := decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash, typedRef, stalePhysicalRef, raw, false)
		if err == nil || !strings.Contains(err.Error(), "do not match physical rows") {
			t.Fatalf("decode physical row-count mismatch err=%v want physical row mismatch", err)
		}
	})
	t.Run("schema_hash_mismatch", func(t *testing.T) {
		_, err := decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash+1, typedRef, physicalRef, raw, false)
		if err == nil || !strings.Contains(err.Error(), "schema_version") {
			t.Fatalf("decode schema mismatch err=%v want schema_version mismatch", err)
		}
	})
}

func TestColumnPhysicalJSONBenchTypedColumnPartPreparedRunnerPinsSnapshot1947(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}
	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	first, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared initial Run: %v", err)
	}
	firstHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q1", first.Groups))
	if first.Diagnostics.RowsScanned != len(events) || first.Diagnostics.ReduceRows != len(events) || first.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection {
		t.Fatalf("initial prepared diagnostics=%+v want typed-column summary over snapshot rows=%d", first.Diagnostics, len(events))
	}

	newEvent := columnPhysicalJSONBenchParityEventP0{ID: "e99", TimeUS: events[0].TimeUS + 99*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.reply", Did: "did_new"}
	if _, err := collection.InsertBatch([][]byte{[]byte(newEvent.ID)}, [][]byte{[]byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, newEvent.TimeUS, newEvent.Kind, newEvent.Operation, newEvent.Collection, newEvent.Did))}); err != nil {
		t.Fatalf("InsertBatch after prepare: %v", err)
	}
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("direct RunColumnPhysicalQuery after insert: %v", err)
	}
	directHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q1", direct.Groups))
	if direct.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || direct.Diagnostics.RowsScanned != len(events)+1 || directHash == firstHash {
		t.Fatalf("direct after insert diagnostics=%+v hash=%016x first=%016x want latest typed-column rows=%d and changed hash", direct.Diagnostics, directHash, firstHash, len(events)+1)
	}

	pinned, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared pinned Run after insert: %v", err)
	}
	pinnedHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q1", pinned.Groups))
	if pinned.Diagnostics.RowsScanned != len(events) || pinned.Diagnostics.ReduceRows != len(events) || pinnedHash != firstHash {
		t.Fatalf("prepared pinned diagnostics=%+v hash=%016x want original summary rows=%d hash=%016x", pinned.Diagnostics, pinnedHash, len(events), firstHash)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("runner Close: %v", err)
	}
	if _, err := runner.Run(); err == nil {
		t.Fatalf("runner Run after Close succeeded; want closed-runner error")
	}
}

func BenchmarkColumnPhysicalJSONBenchTypedColumnPartDirectSmoke1947(b *testing.B) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(b, events)
	defer closeFn()

	cases := []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{
			name: "q1_typed_column_part_section_no_fallback",
			req:  ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"},
		},
		{
			name: "q2_typed_column_part_section_no_fallback",
			req: ColumnPhysicalQueryRequest{
				Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
				GroupColumn:    "collection",
				DistinctColumn: "did",
				Predicates: []ColumnPhysicalQueryPredicate{
					{Column: "kind", Value: "commit"},
					{Column: "operation", Value: "create"},
				},
			},
		},
		{
			name: "qexpr_typed_column_part_section_no_fallback",
			req: ColumnPhysicalQueryRequest{
				Kind:        ColumnPhysicalQuerySumSecondOfDaySquare,
				ValueColumn: "time_us",
			},
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			preview, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			if preview.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || preview.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone {
				b.Fatalf("preview diagnostics=%+v want typed_column_part_section/no fallback", preview.Diagnostics)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var groups int
			var last ColumnPhysicalQueryDiagnostics
			for i := 0; i < b.N; i++ {
				result, err := collection.RunColumnPhysicalQuery(tc.req)
				if err != nil {
					b.Fatalf("RunColumnPhysicalQuery: %v", err)
				}
				groups += len(result.Groups)
				last = result.Diagnostics
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no result groups")
			}
			b.ReportMetric(float64(last.PhysicalBytesScanned), "physical_bytes/op")
			b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
			b.ReportMetric(float64(last.TypedColumnPartSections), "typed_sections/op")
		})
	}
}

func openColumnPhysicalJSONBenchTypedColumnPartFixture1947(tb testing.TB, events []columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func(), []ColumnAssetRef) {
	return openColumnPhysicalJSONBenchTypedColumnPartBatches1947(tb, [][]columnPhysicalJSONBenchParityEventP0{events})
}

func openColumnPhysicalJSONBenchTypedColumnPartBatches1947(tb testing.TB, batches [][]columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func(), []ColumnAssetRef) {
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
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "operation", Path: "operation", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
	}}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint setup DB: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	wantTypedRefs := 0
	for batchIndex, events := range batches {
		if len(events) == 0 {
			continue
		}
		ids := make([][]byte, len(events))
		docs := make([][]byte, len(events))
		for i, event := range events {
			ids[i] = []byte(event.ID)
			docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch[%d]: %v", batchIndex, err)
		}
		wantTypedRefs++
	}
	preCloseTypedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col))
	if len(preCloseTypedRefs) != wantTypedRefs {
		_ = d.Close()
		tb.Fatalf("typed refs=%+v want %d", preCloseTypedRefs, wantTypedRefs)
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
	return reopen, reopened, func() { _ = reopen.Close() }, preCloseTypedRefs
}

func openColumnPhysicalJSONBenchNullableFullDataFixture2165(tb testing.TB, docs []string) (*backenddb.DB, *Collection, func()) {
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
	cfg := &ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "operation", Path: "commit.operation", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "event", Path: "commit.collection", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("Checkpoint setup DB: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection setup: %v", err)
	}
	ids := make([][]byte, len(docs))
	payloads := make([][]byte, len(docs))
	for i, doc := range docs {
		ids[i] = []byte(fmt.Sprintf("event-%02d", i))
		payloads[i] = []byte(doc)
	}
	if _, err := col.InsertBatch(ids, payloads); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch: %v", err)
	}
	if refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col)); len(refs) != 1 {
		_ = d.Close()
		tb.Fatalf("typed refs=%+v want one", refs)
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

func runNullableFullDataQuery2165(tb testing.TB, collection *Collection, name string, req ColumnPhysicalQueryRequest, wantRows, wantPredicates, wantMatched, wantReduce int) ColumnPhysicalQueryResult {
	tb.Helper()
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("RunColumnPhysicalQuery(%s): %v", name, err)
	}
	assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, name+" direct", direct.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
	assertNullableFullDataGenericDiagnostics2165(tb, name+" direct", direct.Diagnostics)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("PrepareColumnPhysicalQuery(%s): %v", name, err)
	}
	defer func() { _ = runner.Close() }()
	var prepared ColumnPhysicalQueryResult
	for run := 0; run < 2; run++ {
		prepared, err = runner.Run()
		if err != nil {
			tb.Fatalf("prepared %s run %d: %v", name, run, err)
		}
		assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
		assertNullableFullDataGenericDiagnostics2165(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics)
		if !reflect.DeepEqual(prepared.Groups, direct.Groups) {
			tb.Fatalf("%s prepared run %d groups=%+v want direct %+v", name, run, prepared.Groups, direct.Groups)
		}
	}
	return direct
}

func runNullableFullDataDenseQ1Query3075(tb testing.TB, collection *Collection, name string, req ColumnPhysicalQueryRequest, wantRows, wantPredicates, wantMatched, wantReduce int) ColumnPhysicalQueryResult {
	tb.Helper()
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("RunColumnPhysicalQuery(%s): %v", name, err)
	}
	assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, name+" direct", direct.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
	assertNullableFullDataDenseQ1Diagnostics3075(tb, name+" direct", direct.Diagnostics)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("PrepareColumnPhysicalQuery(%s): %v", name, err)
	}
	defer func() { _ = runner.Close() }()
	var prepared ColumnPhysicalQueryResult
	for run := 0; run < 2; run++ {
		prepared, err = runner.Run()
		if err != nil {
			tb.Fatalf("prepared %s run %d: %v", name, run, err)
		}
		assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
		assertNullableFullDataDenseQ1Diagnostics3075(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics)
		if !reflect.DeepEqual(prepared.Groups, direct.Groups) {
			tb.Fatalf("%s prepared run %d groups=%+v want direct %+v", name, run, prepared.Groups, direct.Groups)
		}
	}
	return direct
}

func runNullableFullDataDenseQ2Query2165(tb testing.TB, collection *Collection, name string, req ColumnPhysicalQueryRequest, wantRows, wantPredicates, wantMatched, wantReduce int) ColumnPhysicalQueryResult {
	tb.Helper()
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("RunColumnPhysicalQuery(%s): %v", name, err)
	}
	assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, name+" direct", direct.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
	assertNullableFullDataDenseQ2Diagnostics2165(tb, name+" direct", direct.Diagnostics)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("PrepareColumnPhysicalQuery(%s): %v", name, err)
	}
	defer func() { _ = runner.Close() }()
	var prepared ColumnPhysicalQueryResult
	for run := 0; run < 2; run++ {
		prepared, err = runner.Run()
		if err != nil {
			tb.Fatalf("prepared %s run %d: %v", name, run, err)
		}
		assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
		assertNullableFullDataDenseQ2Diagnostics2165(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics)
		if !reflect.DeepEqual(prepared.Groups, direct.Groups) {
			tb.Fatalf("%s prepared run %d groups=%+v want direct %+v", name, run, prepared.Groups, direct.Groups)
		}
	}
	return direct
}

func runNullableFullDataDenseQ3Query3078(tb testing.TB, collection *Collection, name string, req ColumnPhysicalQueryRequest, wantRows, wantPredicates, wantMatched, wantReduce int) ColumnPhysicalQueryResult {
	tb.Helper()
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("RunColumnPhysicalQuery(%s): %v", name, err)
	}
	assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, name+" direct", direct.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
	assertNullableFullDataDenseQ3Diagnostics3078(tb, name+" direct", direct.Diagnostics)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("PrepareColumnPhysicalQuery(%s): %v", name, err)
	}
	defer func() { _ = runner.Close() }()
	var prepared ColumnPhysicalQueryResult
	for run := 0; run < 2; run++ {
		prepared, err = runner.Run()
		if err != nil {
			tb.Fatalf("prepared %s run %d: %v", name, run, err)
		}
		assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics, wantRows, wantPredicates, wantMatched, wantReduce)
		assertNullableFullDataDenseQ3Diagnostics3078(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics)
		if !reflect.DeepEqual(prepared.Groups, direct.Groups) {
			tb.Fatalf("%s prepared run %d groups=%+v want direct %+v", name, run, prepared.Groups, direct.Groups)
		}
	}
	return direct
}

func runNullableFullDataTopKFastPath2878(tb testing.TB, collection *Collection, name string, req ColumnPhysicalQueryRequest, want []ColumnPhysicalQueryGroup, assertDiag func(testing.TB, string, ColumnPhysicalQueryDiagnostics)) {
	tb.Helper()
	direct, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("RunColumnPhysicalQuery(%s): %v", name, err)
	}
	if !reflect.DeepEqual(direct.Groups, want) {
		tb.Fatalf("%s direct groups=%+v want %+v", name, direct.Groups, want)
	}
	assertDiag(tb, name+" direct", direct.Diagnostics)

	runner, err := collection.PrepareColumnPhysicalQuery(req)
	if err != nil {
		tb.Fatalf("PrepareColumnPhysicalQuery(%s): %v", name, err)
	}
	defer func() { _ = runner.Close() }()
	for run := 0; run < 2; run++ {
		prepared, err := runner.Run()
		if err != nil {
			tb.Fatalf("prepared %s run %d: %v", name, run, err)
		}
		if !reflect.DeepEqual(prepared.Groups, want) {
			tb.Fatalf("%s prepared run %d groups=%+v want %+v", name, run, prepared.Groups, want)
		}
		assertDiag(tb, fmt.Sprintf("%s prepared %d", name, run), prepared.Diagnostics)
	}
}

func assertNullableFullDataTopKBaseDiagnostics2878(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, predicates, topK int, order string, resultGroups int) {
	tb.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics storage/fallback=%+v want typed-column source without fallback", label, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialized row/document on typed-column physical path: %+v", label, diag)
	}
	if diag.PredicateCount != predicates || diag.PredicateLiterals != predicates {
		tb.Fatalf("%s predicate diagnostics=%+v want %d equality predicates", label, diag, predicates)
	}
	if diag.DecodedBlocks == 0 || diag.DecodedPayloadBytes == 0 || diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 {
		tb.Fatalf("%s typed-column decode diagnostics missing: %+v", label, diag)
	}
	if diag.TopKLimit != topK || diag.TopKOrder != order || diag.ResultGroups != resultGroups {
		tb.Fatalf("%s topK diagnostics=%+v want limit=%d order=%q groups=%d", label, diag, topK, order, resultGroups)
	}
}

func assertNullableFullDataGenericDiagnostics2165(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if diag.DenseGroupCountUsed || diag.DenseGroupCountDistinctUsed || diag.DenseGroupHourCountUsed || diag.DenseInt64SpanUsed || diag.SortedGroupedDistinctUsed || diag.TimeOrderTopKUsed {
		tb.Fatalf("%s nullable full-data query used nullable-unsafe fast path: %+v", label, diag)
	}
	if diag.SortKeyPrefixPlanned || diag.SortKeyMarkChecks != 0 || diag.SortKeyMarkSkips != 0 {
		tb.Fatalf("%s nullable full-data query used sort-key pruning: %+v", label, diag)
	}
	if diag.DecodedBlocks == 0 || diag.DecodedPayloadBytes == 0 {
		tb.Fatalf("%s nullable full-data query did not report typed-column decode work: %+v", label, diag)
	}
}

func assertNullableFullDataDenseQ1Diagnostics3075(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if !diag.DenseGroupCountUsed || diag.DenseGroupCountDistinctUsed || diag.DenseGroupHourCountUsed || diag.DenseInt64SpanUsed || diag.SortedGroupedDistinctUsed || diag.TimeOrderTopKUsed {
		tb.Fatalf("%s diagnostics=%+v want only dense q1 group-count fast path", label, diag)
	}
	if diag.SortKeyPrefixPlanned || diag.SortKeyMarkChecks != 0 || diag.SortKeyMarkSkips != 0 {
		tb.Fatalf("%s dense q1 diagnostics used sort-key pruning: %+v", label, diag)
	}
	if diag.DecodedBlocks == 0 || diag.DecodedPayloadBytes == 0 {
		tb.Fatalf("%s dense q1 diagnostics did not report typed-column decode work: %+v", label, diag)
	}
}

func assertNullableFullDataDenseQ2Diagnostics2165(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if !diag.DenseGroupCountDistinctUsed || diag.DenseGroupCountUsed || diag.DenseGroupHourCountUsed || diag.DenseInt64SpanUsed || diag.SortedGroupedDistinctUsed || diag.TimeOrderTopKUsed {
		tb.Fatalf("%s diagnostics=%+v want only dense q2 count-distinct fast path", label, diag)
	}
	if diag.SortKeyPrefixPlanned || diag.SortKeyMarkChecks != 0 || diag.SortKeyMarkSkips != 0 {
		tb.Fatalf("%s dense q2 diagnostics used sort-key pruning: %+v", label, diag)
	}
	if diag.DecodedBlocks == 0 || diag.DecodedPayloadBytes == 0 {
		tb.Fatalf("%s dense q2 diagnostics did not report typed-column decode work: %+v", label, diag)
	}
}

func assertNullableFullDataDenseQ3Diagnostics3078(tb testing.TB, label string, diag ColumnPhysicalQueryDiagnostics) {
	tb.Helper()
	if !diag.DenseGroupHourCountUsed || diag.DenseGroupCountUsed || diag.DenseGroupCountDistinctUsed || diag.DenseInt64SpanUsed || diag.SortedGroupedDistinctUsed || diag.TimeOrderTopKUsed {
		tb.Fatalf("%s diagnostics=%+v want only dense q3 group-hour fast path", label, diag)
	}
	if diag.SortKeyPrefixPlanned || diag.SortKeyMarkChecks != 0 || diag.SortKeyMarkSkips != 0 {
		tb.Fatalf("%s dense q3 diagnostics used sort-key pruning: %+v", label, diag)
	}
	if diag.DecodedBlocks == 0 || diag.DecodedPayloadBytes == 0 {
		tb.Fatalf("%s dense q3 diagnostics did not report typed-column decode work: %+v", label, diag)
	}
}

func columnPhysicalGroupCountMap2165(groups []ColumnPhysicalQueryGroup) map[string]int {
	out := make(map[string]int, len(groups))
	for _, group := range groups {
		out[group.Key] = group.Count
	}
	return out
}

func columnPhysicalGroupHourMap3078(groups []ColumnPhysicalQueryGroup) map[string]map[int]int {
	out := make(map[string]map[int]int, len(groups))
	for _, group := range groups {
		byHour := out[group.Key]
		if byHour == nil {
			byHour = make(map[int]int, 1)
			out[group.Key] = byHour
		}
		byHour[group.Hour] = group.Count
	}
	return out
}

type columnPhysicalCountDistinct2165 struct {
	Count    int
	Distinct int
}

func columnPhysicalCountDistinctMap2165(groups []ColumnPhysicalQueryGroup) map[string]columnPhysicalCountDistinct2165 {
	out := make(map[string]columnPhysicalCountDistinct2165, len(groups))
	for _, group := range groups {
		out[group.Key] = columnPhysicalCountDistinct2165{Count: group.Count, Distinct: group.DistinctCount}
	}
	return out
}

func columnPhysicalInt64Map2165(groups []ColumnPhysicalQueryGroup) map[string]int64 {
	out := make(map[string]int64, len(groups))
	for _, group := range groups {
		out[group.Key] = group.Int64
	}
	return out
}

func assertColumnPhysicalJSONBenchTypedColumnDiagnostics1947(tb testing.TB, query string, diag ColumnPhysicalQueryDiagnostics, wantRowsScanned, wantPredicates, wantMatchedRows, wantReduceRows int) {
	tb.Helper()
	if diag.ManifestRootName != "events/column/manifest" || diag.ManifestRoot == 0 {
		tb.Fatalf("%s manifest root name/id missing: %+v", query, diag)
	}
	if diag.ManifestGeneration == 0 || diag.ActiveManifestChecksum == 0 {
		tb.Fatalf("%s active manifest generation/checksum missing: %+v", query, diag)
	}
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection {
		tb.Fatalf("%s storage source=%q want %q diagnostics=%+v", query, diag.StorageSource, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, diag)
	}
	if diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s fallback reason=%q want none diagnostics=%+v", query, diag.FallbackReason, diag)
	}
	if diag.TypedColumnPartSections == 0 || diag.TypedColumnPartSectionBytes == 0 {
		tb.Fatalf("%s typed-column section diagnostics missing: %+v", query, diag)
	}
	if diag.PhysicalBytesScanned <= 0 {
		tb.Fatalf("%s physical bytes scanned missing: %+v", query, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialized row/document on typed-column physical path: %+v", query, diag)
	}
	if diag.RowsScanned != wantRowsScanned || diag.PredicateCount != wantPredicates || diag.RowsMatched != wantMatchedRows || diag.ReduceRows != wantReduceRows {
		tb.Fatalf("%s row/predicate diagnostics=%+v want scanned=%d predicates=%d matched=%d reduced=%d", query, diag, wantRowsScanned, wantPredicates, wantMatchedRows, wantReduceRows)
	}
}
