package collections

import (
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnSortKeyPrefixPlannerFallbacks1949(t *testing.T) {
	sortKey := []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
	req := columnPhysicalQ4BPostCreateRequest1949()
	part := buildTypedColumnSortKeyPlannerPart1949(t, sortKey, []columnPhysicalJSONBenchParityEventP0{
		{ID: "e0", TimeUS: 10, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:a"},
		{ID: "e1", TimeUS: 20, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.like", Did: "did:b"},
		{ID: "e2", TimeUS: 30, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:c"},
		{ID: "e3", TimeUS: 40, Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:d"},
		{ID: "e4", TimeUS: 50, Kind: "commit", Operation: "create", Collection: "app.bsky.graph.follow", Did: "did:e"},
		{ID: "e5", TimeUS: 60, Kind: "identity", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:f"},
	})
	cfg := typedColumnSortKeyConfig1948(sortKey)
	plan := planColumnTypedColumnSortKeyPrefix(*cfg, sortKey, req)
	if !plan.Planned || plan.FallbackReason != columnSortKeyMarkFallbackNone {
		t.Fatalf("plan=%+v want planned q4b prefix", plan)
	}
	pruned, err := plan.prunePartRows(part)
	if err != nil {
		t.Fatalf("prunePartRows: %v", err)
	}
	if pruned.FallbackReason != columnSortKeyMarkFallbackNone || pruned.Checks != 3 || pruned.Skips != 2 || pruned.Matches != 1 {
		t.Fatalf("pruned=%+v want one matching granule and two skipped", pruned)
	}
	if got, want := pruned.Rows, []int{2, 3}; !equalInts1949(got, want) {
		t.Fatalf("pruned rows=%v want %v", got, want)
	}

	partialReq := columnPhysicalQ4BPostCreateRequest1949()
	partialReq.Predicates[1] = ColumnPhysicalQueryPredicate{Column: "operation", Kind: ColumnPhysicalQueryPredicateInList, Values: []string{"create"}}
	partial := planColumnTypedColumnSortKeyPrefix(*cfg, sortKey, partialReq)
	if !partial.Planned || partial.PrefixLen != 1 || !equalStrings1949(partial.prefixColumns(), []string{"kind"}) {
		t.Fatalf("partial prefix plan=%+v want usable kind equality prefix", partial)
	}

	missingMarks := cloneTypedColumnAdapterPartForPlanner1949(part)
	missingMarks.Part.Marks = nil
	missing, err := plan.prunePartRows(missingMarks)
	if err != nil {
		t.Fatalf("missing marks prune: %v", err)
	}
	if missing.FallbackReason != columnSortKeyMarkFallbackMissingMarks || !missing.AllRows {
		t.Fatalf("missing marks result=%+v want explicit full-scan fallback", missing)
	}

	staleMarks := cloneTypedColumnAdapterPartForPlanner1949(part)
	staleMarks.Part.Marks = staleMarks.Part.Marks[:len(staleMarks.Part.Marks)-1]
	stale, err := plan.prunePartRows(staleMarks)
	if err != nil {
		t.Fatalf("stale marks prune: %v", err)
	}
	if stale.FallbackReason != columnSortKeyMarkFallbackStaleMarks || !stale.AllRows {
		t.Fatalf("stale marks result=%+v want explicit full-scan fallback", stale)
	}

	corruptMarks := cloneTypedColumnAdapterPartForPlanner1949(part)
	corruptMarks.Part.Marks[0].Prefixes = nil
	if _, err := plan.prunePartRows(corruptMarks); err == nil {
		t.Fatal("corrupt sort-key marks were used silently; want fail-closed error")
	}

	descending := planColumnTypedColumnSortKeyPrefix(*cfg, []ColumnSortKey{{Column: "kind", Direction: ColumnSortDescending}}, req)
	if descending.Planned || descending.FallbackReason != columnSortKeyMarkFallbackUnsupportedDescending {
		t.Fatalf("descending plan=%+v want unsupported-descending fallback", descending)
	}

	uncertified := cloneTypedColumnAdapterPartForPlanner1949(part)
	kindColumn, ok := uncertified.columnByName("kind")
	if !ok {
		t.Fatal("missing kind column")
	}
	kindColumn.Dictionary = map[string]int64{"commit": 1, "identity": 0}
	for i := range uncertified.Columns {
		if uncertified.Columns[i].Definition.Name == kindColumn.Definition.Name {
			uncertified.Columns[i] = kindColumn
		}
	}
	uncertifiedResult, err := plan.prunePartRows(uncertified)
	if err != nil {
		t.Fatalf("uncertified dictionary prune: %v", err)
	}
	if uncertifiedResult.FallbackReason != columnSortKeyMarkFallbackUncertifiedDictionaryOrdering || !uncertifiedResult.AllRows {
		t.Fatalf("uncertified dictionary result=%+v want explicit full-scan fallback", uncertifiedResult)
	}
}

func TestTypedColumnSortKeyPrefixPlannerCapsAndScalarLiterals1949(t *testing.T) {
	var cfg ColumnStoreConfig
	var sortKey []ColumnSortKey
	var predicates []ColumnPhysicalQueryPredicate
	for i := 0; i < typedColumnPartSortKeyMaxColumns+1; i++ {
		name := "c" + strconv.Itoa(i)
		cfg.Columns = append(cfg.Columns, ColumnStoreColumn{Name: name, ValueType: ColumnStoreValueString})
		sortKey = append(sortKey, ColumnSortKey{Column: name})
		predicates = append(predicates, ColumnPhysicalQueryPredicate{Column: name, Value: "v"})
	}
	plan := planColumnTypedColumnSortKeyPrefix(cfg, sortKey, ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "c0", Predicates: predicates})
	if plan.Planned || plan.FallbackReason != columnSortKeyMarkFallbackUnsupportedSortKeyWidth {
		t.Fatalf("wide prefix plan=%+v want unsupported sort-key width fallback", plan)
	}

	boolColumn := typedColumnAdapterColumn{Field: TypedStorageField{ValueType: ColumnStoreValueBool}}
	encoded, present, fallback, err := encodeSortKeyPrefixPredicateValue(boolColumn, "true")
	if err != nil || !present || fallback != columnSortKeyMarkFallbackNone || encoded != 1 {
		t.Fatalf("bool true encoded=%d present=%v fallback=%q err=%v", encoded, present, fallback, err)
	}
	encoded, present, fallback, err = encodeSortKeyPrefixPredicateValue(boolColumn, "false")
	if err != nil || !present || fallback != columnSortKeyMarkFallbackNone || encoded != 0 {
		t.Fatalf("bool false encoded=%d present=%v fallback=%q err=%v", encoded, present, fallback, err)
	}
	_, present, fallback, err = encodeSortKeyPrefixPredicateValue(boolColumn, "not-bool")
	if err != nil || present || fallback != columnSortKeyMarkFallbackUnsupportedPredicate {
		t.Fatalf("bool invalid present=%v fallback=%q err=%v", present, fallback, err)
	}

	intColumn := typedColumnAdapterColumn{Field: TypedStorageField{ValueType: ColumnStoreValueInt64}}
	encoded, present, fallback, err = encodeSortKeyPrefixPredicateValue(intColumn, "-42")
	if err != nil || !present || fallback != columnSortKeyMarkFallbackNone || encoded != -42 {
		t.Fatalf("int64 encoded=%d present=%v fallback=%q err=%v", encoded, present, fallback, err)
	}
	_, present, fallback, err = encodeSortKeyPrefixPredicateValue(intColumn, "not-int")
	if err != nil || present || fallback != columnSortKeyMarkFallbackUnsupportedPredicate {
		t.Fatalf("int64 invalid present=%v fallback=%q err=%v", present, fallback, err)
	}
}

func TestTypedColumnSortKeyQ4BMarkPrunedParity1949(t *testing.T) {
	sortKey := []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
	events := typedColumnSortKeyPruningEvents1949()
	_, col, closeFn := openTypedColumnSortKeyFixture1948(t, sortKey, events)
	defer closeFn()

	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q4b", events))
	direct, err := col.RunColumnPhysicalQuery(columnPhysicalQ4BPostCreateRequest1949())
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q4b): %v", err)
	}
	directHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q4b", direct.Groups))
	if directHash != rowHash {
		t.Fatalf("q4b direct hash=%016x want row-scan %016x", directHash, rowHash)
	}
	assertQ4BMarkPrunedDiagnostics1949(t, "direct", direct.Diagnostics, len(events))

	runner, err := col.PrepareColumnPhysicalQuery(columnPhysicalQ4BPostCreateRequest1949())
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery(q4b): %v", err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		t.Fatalf("prepared q4b: %v", err)
	}
	preparedHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q4b", prepared.Groups))
	if preparedHash != rowHash {
		t.Fatalf("q4b prepared hash=%016x want row-scan %016x", preparedHash, rowHash)
	}
	assertQ4BMarkPrunedDiagnostics1949(t, "prepared", prepared.Diagnostics, len(events))
}

func TestTypedColumnSortKeyLiteralAbsentSkipsAllRows1949(t *testing.T) {
	sortKey := []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
	events := typedColumnSortKeyPruningEvents1949()
	_, col, closeFn := openTypedColumnSortKeyFixture1948(t, sortKey, events)
	defer closeFn()

	req := columnPhysicalQ4BPostCreateRequest1949()
	req.Predicates[2].Value = "app.bsky.feed.repost"
	result, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(absent q4b): %v", err)
	}
	if len(result.Groups) != 0 {
		t.Fatalf("absent q4b groups=%+v want none", result.Groups)
	}
	diag := result.Diagnostics
	if !diag.SortKeyPrefixPlanned || diag.SortKeyMarkFallbackReason != columnSortKeyMarkFallbackLiteralAbsent {
		t.Fatalf("absent q4b diagnostics=%+v want literal-absent prefix pruning", diag)
	}
	if diag.SortKeyMarkSkips != diag.ScheduledGranules || diag.ScheduledGranules == 0 {
		t.Fatalf("absent q4b diagnostics=%+v want all granules skipped by marks", diag)
	}
	if diag.RowsScanned != 0 || diag.DecodedGranules != 0 || diag.DecodedPayloadBytes != 0 {
		t.Fatalf("absent q4b scan diagnostics=%+v want no row/block decode", diag)
	}
}

func TestTypedColumnSortKeyQ2StreamingReadinessDiagnostics1949(t *testing.T) {
	sortKey := []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}
	events := typedColumnSortKeyPruningEvents1949()
	_, col, closeFn := openTypedColumnSortKeyFixture1948(t, sortKey, events)
	defer closeFn()

	q2 := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
		},
	}
	result, err := col.RunColumnPhysicalQuery(q2)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery(q2): %v", err)
	}
	rowHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchReferenceLinesP0("q2", events))
	gotHash := columnPhysicalJSONBenchHashLinesP0(columnPhysicalJSONBenchPhysicalLinesP0("q2", result.Groups))
	if gotHash != rowHash {
		t.Fatalf("q2 hash=%016x want row-scan %016x", gotHash, rowHash)
	}
	if !result.Diagnostics.SortKeyPrefixPlanned || result.Diagnostics.SortKeyPrefixLiterals != 2 || !equalStrings1949(result.Diagnostics.SortKeyPrefixColumns, []string{"kind", "operation"}) {
		t.Fatalf("q2 prefix diagnostics=%+v want kind/operation prefix plan", result.Diagnostics)
	}
	if !result.Diagnostics.SortedGroupedDistinctReady || !result.Diagnostics.SortedGroupedDistinctUsed || result.Diagnostics.SortedGroupedDistinctFallbackReason != columnSortedGroupedDistinctFallbackNone {
		t.Fatalf("q2 grouped-distinct diagnostics=%+v want #1950 streaming consumer used", result.Diagnostics)
	}
	if result.Diagnostics.SortKeyMarkChecks == 0 || result.Diagnostics.RowsScanned != len(events) {
		t.Fatalf("q2 diagnostics=%+v want checked sorted-prefix marks and exact grouped distinct full execution", result.Diagnostics)
	}
}

func BenchmarkTypedColumnSortKeyQ4BDirect1949(b *testing.B) {
	events := typedColumnSortKeyPruningEvents1949()
	cases := []struct {
		name    string
		sortKey []ColumnSortKey
		events  []columnPhysicalJSONBenchParityEventP0
	}{
		{name: "primary_id_control", sortKey: nil, events: events},
		{name: "clickhouse_full_scan", sortKey: []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}, events: typedColumnSortKeyAllPostEvents1949(len(events))},
		{name: "clickhouse_mark_pruning", sortKey: []ColumnSortKey{{Column: "kind"}, {Column: "operation"}, {Column: "collection"}, {Column: "did"}, {Column: "time_us"}}, events: events},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, col, closeFn := openTypedColumnSortKeyFixture1948(b, tc.sortKey, tc.events)
			defer closeFn()
			req := columnPhysicalQ4BPostCreateRequest1949()
			preview, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			for i := 0; i < b.N; i++ {
				result, err := col.RunColumnPhysicalQuery(req)
				if err != nil {
					b.Fatalf("RunColumnPhysicalQuery: %v", err)
				}
				last = result.Diagnostics
			}
			b.StopTimer()
			if b.N == 0 {
				last = preview.Diagnostics
			}
			b.ReportMetric(float64(last.SortKeyMarkChecks), "mark_checks/op")
			b.ReportMetric(float64(last.SortKeyMarkSkips), "mark_skips/op")
			b.ReportMetric(float64(last.ScheduledGranules), "granules_considered/op")
			b.ReportMetric(float64(last.SkippedGranules), "granules_skipped/op")
			b.ReportMetric(float64(last.DecodedGranules), "granules_decoded/op")
			b.ReportMetric(float64(last.RowsScanned), "rows_scanned/op")
			b.ReportMetric(float64(last.DecodedPayloadBytes), "decoded_bytes/op")
			if last.ScanNanos > 0 {
				b.ReportMetric(1e9/float64(last.ScanNanos), "diag_ops_per_sec")
			}
			if last.RowsScanned > 0 && last.ScanNanos > 0 {
				b.ReportMetric(float64(last.RowsScanned)*1e9/float64(last.ScanNanos), "diag_rows_per_sec")
			}
		})
	}
}

func buildTypedColumnSortKeyPlannerPart1949(t testing.TB, sortKey []ColumnSortKey, events []columnPhysicalJSONBenchParityEventP0) *typedColumnAdapterPart {
	t.Helper()
	cfg := typedColumnSortKeyConfig1948(sortKey)
	fields := columnStoreTypedColumnPartFields(*cfg)
	declared := typedColumnDeclaredRows1948(events)
	adapterRows, err := typedColumnAdapterRowsFromDeclaredRows(cfg.Columns, fields, declared)
	if err != nil {
		t.Fatalf("typedColumnAdapterRowsFromDeclaredRows: %v", err)
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: typedColumnPartAssetPartID, RowsPerGranule: 2, Fields: fields, SortKey: sortKey}, adapterRows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	return part
}

func cloneTypedColumnAdapterPartForPlanner1949(part *typedColumnAdapterPart) *typedColumnAdapterPart {
	clone := *part
	clone.Columns = append([]typedColumnAdapterColumn(nil), part.Columns...)
	partClone := *part.Part
	partClone.Marks = make([]typedcolumnSortKeyMarkAlias1949, len(part.Part.Marks))
	copy(partClone.Marks, part.Part.Marks)
	clone.Part = &partClone
	return &clone
}

type typedcolumnSortKeyMarkAlias1949 = typedcolumn.SortKeyMark

func columnPhysicalQ4BPostCreateRequest1949() ColumnPhysicalQueryRequest {
	return ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupMaxInt64,
		GroupColumn: "did",
		ValueColumn: "time_us",
		Predicates: []ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "collection", Value: "app.bsky.feed.post"},
		},
	}
}

func typedColumnSortKeyPruningEvents1949() []columnPhysicalJSONBenchParityEventP0 {
	var events []columnPhysicalJSONBenchParityEventP0
	appendSegment := func(prefix string, count int, kind string, operation string, collection string, didPrefix string, base int64) {
		for i := 0; i < count; i++ {
			events = append(events, columnPhysicalJSONBenchParityEventP0{
				ID:         prefix + intString1949(i),
				TimeUS:     base + int64(i),
				Kind:       kind,
				Operation:  operation,
				Collection: collection,
				Did:        didPrefix + intString1949(i%64),
			})
		}
	}
	appendSegment("like-", 9000, "commit", "create", "app.bsky.feed.like", "did:like:", 1_000_000)
	appendSegment("post-", 2000, "commit", "create", "app.bsky.feed.post", "did:post:", 2_000_000)
	appendSegment("graph-", 9000, "commit", "create", "app.bsky.graph.follow", "did:graph:", 3_000_000)
	appendSegment("identity-", 1000, "identity", "create", "app.bsky.feed.post", "did:identity:", 4_000_000)
	return events
}

func typedColumnSortKeyAllPostEvents1949(count int) []columnPhysicalJSONBenchParityEventP0 {
	events := make([]columnPhysicalJSONBenchParityEventP0, count)
	for i := range events {
		events[i] = columnPhysicalJSONBenchParityEventP0{ID: "post-all-" + intString1949(i), TimeUS: int64(5_000_000 + i), Kind: "commit", Operation: "create", Collection: "app.bsky.feed.post", Did: "did:post:" + intString1949(i%64)}
	}
	return events
}

func assertQ4BMarkPrunedDiagnostics1949(t testing.TB, label string, diag ColumnPhysicalQueryDiagnostics, totalRows int) {
	t.Helper()
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		t.Fatalf("%s diagnostics=%+v want typed-column source without storage fallback", label, diag)
	}
	if !diag.SortKeyPrefixPlanned || diag.SortKeyPrefixLiterals != 3 || !equalStrings1949(diag.SortKeyPrefixColumns, []string{"kind", "operation", "collection"}) {
		t.Fatalf("%s prefix diagnostics=%+v want full q4b prefix", label, diag)
	}
	if diag.SortKeyMarkChecks == 0 || diag.SortKeyMarkSkips == 0 || diag.SkippedGranules == 0 {
		t.Fatalf("%s mark diagnostics=%+v want checked and skipped granules", label, diag)
	}
	if diag.RowsScanned >= totalRows || diag.RowsScanned == 0 || diag.DecodedPayloadBytes == 0 {
		t.Fatalf("%s scan diagnostics=%+v want fewer scanned rows and decoded bytes", label, diag)
	}
	if diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		t.Fatalf("%s materialization diagnostics=%+v want no document materialization", label, diag)
	}
}

func intString1949(v int) string {
	return strconv.Itoa(v)
}

func equalInts1949(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalStrings1949(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
