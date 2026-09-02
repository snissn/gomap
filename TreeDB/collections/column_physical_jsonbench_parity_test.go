package collections

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type columnPhysicalJSONBenchParityEventP0 struct {
	ID         string `json:"-"`
	TimeUS     int64  `json:"time_us"`
	Kind       string `json:"kind"`
	Operation  string `json:"operation"`
	Collection string `json:"collection"`
	Did        string `json:"did"`
}

func TestColumnPhysicalJSONBenchParityQ1Q5P0(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	collection, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()

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
		wantSource      ColumnPhysicalQueryStorageSource
	}{
		{
			name:            "q1",
			req:             ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "collection"},
			wantPredicates:  0,
			wantMatchedRows: 0,
			wantReduceRows:  len(scanned),
			wantSource:      ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset,
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
			wantSource:      ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset,
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
			wantSource:      ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset,
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
			wantSource:      ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset,
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
			wantSource:      ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset,
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
			wantSource:      ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset,
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
			wantSource:      ColumnPhysicalQueryStorageSourceTypedRowAsset,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rowLines := columnPhysicalJSONBenchReferenceLinesP0(tc.name, scanned)
			rowHash := columnPhysicalJSONBenchHashLinesP0(rowLines)

			direct, err := collection.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery(%s): %v", tc.name, err)
			}
			assertColumnPhysicalJSONBenchDiagnosticsP0(t, tc.name, direct.Diagnostics, len(scanned), tc.wantPredicates, tc.wantMatchedRows, tc.wantReduceRows, tc.wantSource)
			directHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0(tc.name, direct.Groups))
			if directHash != rowHash {
				t.Fatalf("%s direct hash=%016x want row scan %016x row=%v direct=%v", tc.name, directHash, rowHash, rowLines, columnPhysicalJSONBenchPhysicalLinesP0(tc.name, direct.Groups))
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
				assertColumnPhysicalJSONBenchDiagnosticsP0(t, tc.name, prepared.Diagnostics, len(scanned), tc.wantPredicates, tc.wantMatchedRows, tc.wantReduceRows, tc.wantSource)
				preparedHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0(tc.name, prepared.Groups))
				if preparedHash != rowHash {
					t.Fatalf("%s prepared run %d hash=%016x want row scan %016x row=%v prepared=%v", tc.name, run, preparedHash, rowHash, rowLines, columnPhysicalJSONBenchPhysicalLinesP0(tc.name, prepared.Groups))
				}
			}
		})
	}
}

func TestColumnPhysicalJSONBenchQ2NoMaskingGuardP0(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	collection, closeFn := openColumnPhysicalJSONBenchParityFixtureP0(t, events)
	defer closeFn()
	scanned := scanColumnPhysicalJSONBenchParityEventsP0(t, collection, len(events))

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
	want := columnPhysicalJSONBenchQ2ReferenceCountsP0(scanned)
	if !columnPhysicalJSONBenchQ2CountsEqualP0(got, want) {
		t.Fatalf("q2 counts/distinct=%v want %v groups=%+v", got, want, result.Groups)
	}
	if _, ok := got[""]; ok {
		t.Fatalf("q2 produced sentinel empty collection group: %v", got)
	}
	if result.Diagnostics.PredicateCount != 2 || result.Diagnostics.RowsMatched != columnPhysicalJSONBenchReferenceMatchedRowsP0("q2", scanned) {
		t.Fatalf("q2 diagnostics=%+v want two real predicates and matched source rows", result.Diagnostics)
	}
	assertColumnPhysicalJSONBenchRowScanPreservesFullPredicateColumnsP0(t, events, scanned)
}

func columnPhysicalJSONBenchParityEventsP0() []columnPhysicalJSONBenchParityEventP0 {
	const base = int64(1_700_000_000_000_000)
	return []columnPhysicalJSONBenchParityEventP0{
		{ID: "e01", TimeUS: base + 1*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did_a"},
		{ID: "e02", TimeUS: base + 3*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did_a"},
		{ID: "e03", TimeUS: base + 2*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did_b"},
		{ID: "e04", TimeUS: base + 4*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did_a"},
		{ID: "e05", TimeUS: base + 5*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.repost", Did: "did_c"},
		{ID: "e06", TimeUS: base + 6*columnPhysicalQueryHourUS, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did_kind_guard"},
		{ID: "e07", TimeUS: base + 7*columnPhysicalQueryHourUS, Kind: "commit", Operation: "delete", Collection: "app.bsky.feed.post", Did: "did_operation_guard"},
		{ID: "e08", TimeUS: base + 8*columnPhysicalQueryHourUS, Kind: "commit", Operation: "create", Collection: "app.bsky.graph.follow", Did: "did_d"},
	}
}

func openColumnPhysicalJSONBenchParityFixtureP0(tb testing.TB, events []columnPhysicalJSONBenchParityEventP0) (*Collection, func()) {
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
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "operation", Path: "operation", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "collection", Path: "collection", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true},
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
	ids := make([][]byte, len(events))
	docs := make([][]byte, len(events))
	for i, event := range events {
		ids[i] = []byte(event.ID)
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":%q,"operation":%q,"collection":%q,"did":%q}`, event.TimeUS, event.Kind, event.Operation, event.Collection, event.Did))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch: %v", err)
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
	return reopened, func() { _ = reopen.Close() }
}

func scanColumnPhysicalJSONBenchParityEventsP0(tb testing.TB, collection *Collection, rows int) []columnPhysicalJSONBenchParityEventP0 {
	tb.Helper()
	out := make([]columnPhysicalJSONBenchParityEventP0, 0, rows)
	truncated, err := collection.ScanDocumentsFunc(rows+1, func(record DocumentRecord) (bool, error) {
		var event columnPhysicalJSONBenchParityEventP0
		if err := json.Unmarshal(record.Document, &event); err != nil {
			return false, err
		}
		event.ID = string(record.ID)
		out = append(out, event)
		return true, nil
	})
	if err != nil {
		tb.Fatalf("ScanDocumentsFunc: %v", err)
	}
	if truncated || len(out) != rows {
		tb.Fatalf("row scan truncated=%t rows=%d want %d", truncated, len(out), rows)
	}
	return out
}

func assertColumnPhysicalJSONBenchRowScanPreservesFullPredicateColumnsP0(tb testing.TB, want, got []columnPhysicalJSONBenchParityEventP0) {
	tb.Helper()
	byID := make(map[string]columnPhysicalJSONBenchParityEventP0, len(got))
	for _, event := range got {
		byID[event.ID] = event
	}
	for _, expected := range want {
		actual, ok := byID[expected.ID]
		if !ok {
			tb.Fatalf("row scan missing id %s", expected.ID)
		}
		if actual != expected {
			tb.Fatalf("row scan id %s=%+v want full unmasked columns %+v", expected.ID, actual, expected)
		}
		if expected.Kind != "commit" || expected.Operation != "create" {
			if actual.Collection == "" || actual.Did == "" {
				tb.Fatalf("row scan id %s has sentinel empty collection/did for non-q2 row: %+v", expected.ID, actual)
			}
		}
	}
}

func assertColumnPhysicalJSONBenchDiagnosticsP0(tb testing.TB, query string, diag ColumnPhysicalQueryDiagnostics, wantRowsScanned, wantPredicates, wantMatchedRows, wantReduceRows int, wantSource ColumnPhysicalQueryStorageSource) {
	tb.Helper()
	if diag.ManifestRootName != "events/column/manifest" || diag.ManifestRoot == 0 {
		tb.Fatalf("%s manifest root name/id missing: %+v", query, diag)
	}
	if diag.ManifestGeneration == 0 || diag.ActiveManifestChecksum == 0 {
		tb.Fatalf("%s active manifest generation/checksum missing: %+v", query, diag)
	}
	if diag.StorageSource != wantSource {
		tb.Fatalf("%s storage source=%q want %q diagnostics=%+v", query, diag.StorageSource, wantSource, diag)
	}
	if diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s fallback reason=%q want none diagnostics=%+v", query, diag.FallbackReason, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s materialized row/document on physical path: %+v", query, diag)
	}
	if diag.RowsScanned != wantRowsScanned || diag.PredicateCount != wantPredicates || diag.RowsMatched != wantMatchedRows || diag.ReduceRows != wantReduceRows {
		tb.Fatalf("%s row/predicate diagnostics=%+v want scanned=%d predicates=%d matched=%d reduced=%d", query, diag, wantRowsScanned, wantPredicates, wantMatchedRows, wantReduceRows)
	}
}

func columnPhysicalJSONBenchReferenceMatchedRowsP0(query string, events []columnPhysicalJSONBenchParityEventP0) int {
	matched := 0
	for _, event := range events {
		if columnPhysicalJSONBenchReferenceMatchP0(query, event) {
			matched++
		}
	}
	return matched
}

func columnPhysicalJSONBenchReferenceMatchP0(query string, event columnPhysicalJSONBenchParityEventP0) bool {
	switch query {
	case "q1":
		return true
	case "q2":
		return event.Kind == "commit" && event.Operation == "create"
	case "q3":
		return event.Kind == "commit" && event.Operation == "create" && (event.Collection == "app.bsky.feed.post" || event.Collection == "app.bsky.feed.repost" || event.Collection == "app.bsky.feed.like")
	case "q4a", "q4b", "q5":
		return event.Kind == "commit" && event.Operation == "create" && event.Collection == "app.bsky.feed.post"
	case "sum_time_second_of_day_square":
		return true
	default:
		panic(query)
	}
}

func columnPhysicalJSONBenchReferenceLinesP0(query string, events []columnPhysicalJSONBenchParityEventP0) []string {
	switch query {
	case "q1":
		counts := make(map[string]int64)
		for _, event := range events {
			counts[event.Collection]++
		}
		return columnPhysicalJSONBenchIntLinesP0(query, counts)
	case "q2":
		return columnPhysicalJSONBenchQ2LinesP0(columnPhysicalJSONBenchQ2ReferenceCountsP0(events))
	case "q3":
		counts := make(map[string]int64)
		for _, event := range events {
			if !columnPhysicalJSONBenchReferenceMatchP0(query, event) {
				continue
			}
			key := fmt.Sprintf("%s:%s", event.Collection, columnPhysicalQueryHourKey(columnPhysicalQueryUTCHour(event.TimeUS)))
			counts[key]++
		}
		return columnPhysicalJSONBenchIntLinesP0(query, counts)
	case "q4a", "q4b":
		values := make(map[string]int64)
		for _, event := range events {
			if !columnPhysicalJSONBenchReferenceMatchP0(query, event) {
				continue
			}
			cur, ok := values[event.Did]
			if !ok || (query == "q4a" && event.TimeUS < cur) || (query == "q4b" && event.TimeUS > cur) {
				values[event.Did] = event.TimeUS
			}
		}
		return columnPhysicalJSONBenchIntLinesP0(query, values)
	case "q5":
		type span struct{ min, max int64 }
		spans := make(map[string]span)
		for _, event := range events {
			if !columnPhysicalJSONBenchReferenceMatchP0(query, event) {
				continue
			}
			cur, ok := spans[event.Did]
			if !ok {
				spans[event.Did] = span{min: event.TimeUS, max: event.TimeUS}
				continue
			}
			if event.TimeUS < cur.min {
				cur.min = event.TimeUS
			}
			if event.TimeUS > cur.max {
				cur.max = event.TimeUS
			}
			spans[event.Did] = cur
		}
		values := make(map[string]int64, len(spans))
		for did, span := range spans {
			values[did] = span.max - span.min
		}
		return columnPhysicalJSONBenchIntLinesP0(query, values)
	case "sum_time_second_of_day_square":
		sum := int64(0)
		for _, event := range events {
			value, err := typedColumnInt64AggregateExpressionValue(TypedColumnInt64AggregateSecondOfDaySquare, event.TimeUS)
			if err != nil {
				panic(err)
			}
			sum += value
		}
		return columnPhysicalJSONBenchIntLinesP0(query, map[string]int64{"time_us_second_of_day_square": sum})
	default:
		panic(query)
	}
}

func columnPhysicalJSONBenchPhysicalLinesP0(query string, groups []ColumnPhysicalQueryGroup) []string {
	switch query {
	case "q1":
		values := make(map[string]int64, len(groups))
		for _, group := range groups {
			values[group.Key] = int64(group.Count)
		}
		return columnPhysicalJSONBenchIntLinesP0(query, values)
	case "q2":
		return columnPhysicalJSONBenchQ2LinesP0(columnPhysicalJSONBenchQ2CountsP0(groups))
	case "q3":
		values := make(map[string]int64, len(groups))
		for _, group := range groups {
			key := fmt.Sprintf("%s:%s", group.Key, columnPhysicalQueryHourKey(group.Hour))
			values[key] = int64(group.Count)
		}
		return columnPhysicalJSONBenchIntLinesP0(query, values)
	case "q4a", "q4b", "q5", "sum_time_second_of_day_square":
		values := make(map[string]int64, len(groups))
		for _, group := range groups {
			values[group.Key] = group.Int64
		}
		return columnPhysicalJSONBenchIntLinesP0(query, values)
	default:
		panic(query)
	}
}

type columnPhysicalJSONBenchQ2CountP0 struct {
	Count    int64
	Distinct int64
}

func columnPhysicalJSONBenchQ2ReferenceCountsP0(events []columnPhysicalJSONBenchParityEventP0) map[string]columnPhysicalJSONBenchQ2CountP0 {
	counts := make(map[string]int64)
	distinct := make(map[string]map[string]struct{})
	for _, event := range events {
		if !columnPhysicalJSONBenchReferenceMatchP0("q2", event) {
			continue
		}
		counts[event.Collection]++
		if distinct[event.Collection] == nil {
			distinct[event.Collection] = make(map[string]struct{})
		}
		distinct[event.Collection][event.Did] = struct{}{}
	}
	out := make(map[string]columnPhysicalJSONBenchQ2CountP0, len(counts))
	for collection, count := range counts {
		out[collection] = columnPhysicalJSONBenchQ2CountP0{Count: count, Distinct: int64(len(distinct[collection]))}
	}
	return out
}

func columnPhysicalJSONBenchQ2CountsP0(groups []ColumnPhysicalQueryGroup) map[string]columnPhysicalJSONBenchQ2CountP0 {
	out := make(map[string]columnPhysicalJSONBenchQ2CountP0, len(groups))
	for _, group := range groups {
		out[group.Key] = columnPhysicalJSONBenchQ2CountP0{Count: int64(group.Count), Distinct: int64(group.DistinctCount)}
	}
	return out
}

func columnPhysicalJSONBenchQ2CountsEqualP0(left, right map[string]columnPhysicalJSONBenchQ2CountP0) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func columnPhysicalJSONBenchQ2LinesP0(values map[string]columnPhysicalJSONBenchQ2CountP0) []string {
	lines := make([]string, 0, len(values)*2)
	for key, value := range values {
		lines = append(lines, fmt.Sprintf("q2:%s:count=%d", key, value.Count))
		lines = append(lines, fmt.Sprintf("q2:%s:distinct=%d", key, value.Distinct))
	}
	return lines
}

func columnPhysicalJSONBenchIntLinesP0(query string, values map[string]int64) []string {
	lines := make([]string, 0, len(values))
	for key, value := range values {
		lines = append(lines, fmt.Sprintf("%s:%s=%d", query, key, value))
	}
	return lines
}

func columnPhysicalJSONBenchHashLinesP0(lines []string) uint64 {
	ordered := append([]string(nil), lines...)
	sort.Strings(ordered)
	h := fnv.New64a()
	for _, line := range ordered {
		_, _ = h.Write([]byte(line))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}
