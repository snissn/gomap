package collections

import (
	"reflect"
	"testing"
)

func TestColumnDictionaryPredicateFastPath1927(t *testing.T) {
	for _, tc := range []struct {
		name  string
		asset columnDictionaryPredicateAsset
	}{
		{
			name: "one predicate",
			asset: columnDictionaryPredicateAsset{
				rowCount: 4,
				codes:    [][]uint32{{0, 1, 2, 3}},
				allowed:  [][]uint64{{(uint64(1) << 1) | (uint64(1) << 3)}},
				fastSafe: true,
			},
		},
		{
			name: "two predicates",
			asset: columnDictionaryPredicateAsset{
				rowCount: 4,
				codes: [][]uint32{
					{0, 1, 2, 3},
					{3, 2, 1, 0},
				},
				allowed: [][]uint64{
					{(uint64(1) << 0) | (uint64(1) << 2)},
					{(uint64(1) << 1) | (uint64(1) << 3)},
				},
				fastSafe: true,
			},
		},
		{
			name: "generic predicates",
			asset: columnDictionaryPredicateAsset{
				rowCount: 4,
				codes: [][]uint32{
					{0, 1, 2, 3},
					{1, 1, 0, 1},
					{3, 3, 3, 2},
				},
				allowed: [][]uint64{
					{(uint64(1) << 0) | (uint64(1) << 2) | (uint64(1) << 3)},
					{uint64(1) << 1},
					{uint64(1) << 3},
				},
				fastSafe: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast, ok := tc.asset.fastPath(tc.asset.rowCount)
			if !ok {
				t.Fatalf("fastPath rejected eligible asset: %+v", tc.asset)
			}
			for rowIdx := 0; rowIdx < tc.asset.rowCount; rowIdx++ {
				want := tc.asset.matches(rowIdx)
				var got bool
				switch fast.predicateCount() {
				case 1:
					got = fast.matches1(rowIdx)
				case 2:
					got = fast.matches2(rowIdx)
				default:
					got = fast.matches(rowIdx)
				}
				if got != want {
					t.Fatalf("row %d fast match=%v want %v", rowIdx, got, want)
				}
			}
		})
	}
}

func TestColumnDictionaryPredicateFastPathRejectsUnsafeState1927(t *testing.T) {
	eligible := columnDictionaryPredicateAsset{
		rowCount: 2,
		codes:    [][]uint32{{0, 1}},
		allowed:  [][]uint64{{uint64(1) << 1}},
		fastSafe: true,
	}
	if _, ok := eligible.fastPath(2); !ok {
		t.Fatalf("eligible fastPath rejected")
	}
	if got := eligible.matches(0); got {
		t.Fatalf("row 0 matches=%v want false", got)
	}
	if got := eligible.matches(1); !got {
		t.Fatalf("row 1 matches=%v want true", got)
	}

	for _, tc := range []struct {
		name  string
		asset columnDictionaryPredicateAsset
		rows  int
	}{
		{name: "not marked fast safe", asset: columnDictionaryPredicateAsset{rowCount: 2, codes: [][]uint32{{0, 1}}, allowed: [][]uint64{{3}}}, rows: 2},
		{name: "row count mismatch", asset: eligible, rows: 1},
		{name: "code allowed length mismatch", asset: columnDictionaryPredicateAsset{rowCount: 2, codes: [][]uint32{{0, 1}}, allowed: nil, fastSafe: true}, rows: 2},
		{name: "short code slice", asset: columnDictionaryPredicateAsset{rowCount: 2, codes: [][]uint32{{0}}, allowed: [][]uint64{{3}}, fastSafe: true}, rows: 2},
		{name: "empty allowed", asset: columnDictionaryPredicateAsset{rowCount: 2, codes: [][]uint32{{0, 1}}, allowed: [][]uint64{{}}, fastSafe: true}, rows: 2},
		{name: "rejects all", asset: columnDictionaryPredicateAsset{rowCount: 2, codes: [][]uint32{{0, 1}}, allowed: [][]uint64{{3}}, rejectsAll: true, fastSafe: true}, rows: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := tc.asset.fastPath(tc.rows); ok {
				t.Fatalf("fastPath accepted unsafe asset: %+v rows=%d", tc.asset, tc.rows)
			}
		})
	}
}

func TestColumnDictionaryPreparedPredicateFastPathsPreserveDiagnostics1927(t *testing.T) {
	base := int64(1_700_000_000_000_000)
	events := []columnPhysicalPredicateEvent1869{
		{ID: "e1", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: base + 0*columnPhysicalQueryHourUS},
		{ID: "e2", Kind: "commit", Operation: "create", Event: "post", Did: "did_b", TimeUS: base + 1*columnPhysicalQueryHourUS},
		{ID: "e3", Kind: "commit", Operation: "delete", Event: "post", Did: "did_a", TimeUS: base + 2*columnPhysicalQueryHourUS},
		{ID: "e4", Kind: "identity", Operation: "create", Event: "post", Did: "did_c", TimeUS: base + 3*columnPhysicalQueryHourUS},
		{ID: "e5", Kind: "commit", Operation: "create", Event: "like", Did: "did_a", TimeUS: base + 4*columnPhysicalQueryHourUS},
		{ID: "e6", Kind: "commit", Operation: "create", Event: "repost", Did: "did_d", TimeUS: base + 5*columnPhysicalQueryHourUS},
		{ID: "e7", Kind: "commit", Operation: "create", Event: "post", Did: "did_a", TimeUS: base + 6*columnPhysicalQueryHourUS},
		{ID: "e8", Kind: "identity", Operation: "delete", Event: "like", Did: "did_e", TimeUS: base + 7*columnPhysicalQueryHourUS},
	}
	collection, closeFn := openColumnPhysicalPredicateFixture1869(t, events)
	defer closeFn()

	commit := []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}}
	commitCreate := []ColumnPhysicalQueryPredicate{{Column: "kind", Value: "commit"}, {Column: "operation", Value: "create"}}
	postOrLikeCreate := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
		{Column: "event", Kind: ColumnPhysicalQueryPredicateInList, Values: []string{"post", "like"}},
	}

	for _, tc := range []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{name: "group_count_one_predicate", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "event", Predicates: commit}},
		{name: "group_count_two_predicates", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "event", Predicates: commitCreate}},
		{name: "distinct_two_predicates", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "event", DistinctColumn: "did", Predicates: commitCreate}},
		{name: "hour_generic_predicates", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupHourCount, GroupColumn: "event", ValueColumn: "time_us", Predicates: postOrLikeCreate}},
		{name: "int64_one_predicate", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", Predicates: commit}},
		{name: "int64_generic_predicates", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", Predicates: postOrLikeCreate}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			direct, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			runner, err := collection.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			defer func() { _ = runner.Close() }()
			prepared, err := runner.Run()
			if err != nil {
				t.Fatalf("prepared Run: %v", err)
			}
			if !reflect.DeepEqual(prepared.Groups, direct.Groups) {
				t.Fatalf("prepared groups=%+v want direct %+v", prepared.Groups, direct.Groups)
			}
			wantMatched := columnPhysicalPredicateMatchedRows1927(events, tc.req.Predicates)
			diag := prepared.Diagnostics
			if diag.RowsScanned != len(events) || diag.RowsMatched != wantMatched || diag.ReduceRows != wantMatched {
				t.Fatalf("diagnostics rows scanned/matched/reduced=%d/%d/%d want %d/%d/%d full=%+v", diag.RowsScanned, diag.RowsMatched, diag.ReduceRows, len(events), wantMatched, wantMatched, diag)
			}
			// Each prepared predicate uses one dictionary-code sidecar per scheduled granule.
			if diag.PredicateDictionaryCodeHits != diag.ScheduledGranules*len(tc.req.Predicates) {
				t.Fatalf("predicate code hits=%d want scheduled(%d)*predicates(%d) diagnostics=%+v", diag.PredicateDictionaryCodeHits, diag.ScheduledGranules, len(tc.req.Predicates), diag)
			}
		})
	}
}

func columnPhysicalPredicateMatchedRows1927(events []columnPhysicalPredicateEvent1869, predicates []ColumnPhysicalQueryPredicate) int {
	matched := 0
	for _, event := range events {
		if columnPhysicalPredicateEventMatches1927(event, predicates) {
			matched++
		}
	}
	return matched
}

func columnPhysicalPredicateEventMatches1927(event columnPhysicalPredicateEvent1869, predicates []ColumnPhysicalQueryPredicate) bool {
	for _, predicate := range predicates {
		value := columnPhysicalPredicateEventValue1927(event, predicate.Column)
		switch columnPhysicalQueryPredicateKindOrDefault(predicate.Kind) {
		case ColumnPhysicalQueryPredicateEqual:
			if value != predicate.Value {
				return false
			}
		case ColumnPhysicalQueryPredicateInList:
			matched := false
			for _, allowed := range predicate.Values {
				if value == allowed {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func columnPhysicalPredicateEventValue1927(event columnPhysicalPredicateEvent1869, column string) string {
	switch column {
	case "kind":
		return event.Kind
	case "operation":
		return event.Operation
	case "event":
		return event.Event
	case "did":
		return event.Did
	default:
		return ""
	}
}
