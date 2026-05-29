package collections

import (
	"fmt"
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
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection || result.Diagnostics.PredicateCount != 2 {
		t.Fatalf("q2 diagnostics=%+v want typed-column source and real predicates", result.Diagnostics)
	}
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
		if group.Key == "" || group.Count == 0 || group.Key != columnPhysicalQueryHourKey(group.Hour) {
			t.Fatalf("hour-count group=%+v want key/count/hour populated consistently", group)
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
		_, err := decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash, staleTypedRef, physicalRef, raw)
		if err == nil || !strings.Contains(err.Error(), "image/ref mismatch") {
			t.Fatalf("decode row-count mismatch err=%v want image/ref mismatch", err)
		}
	})
	t.Run("physical_ref_row_count_mismatch", func(t *testing.T) {
		stalePhysicalRef := physicalRef
		stalePhysicalRef.Rows++
		_, err := decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash, typedRef, stalePhysicalRef, raw)
		if err == nil || !strings.Contains(err.Error(), "do not match physical rows") {
			t.Fatalf("decode physical row-count mismatch err=%v want physical row mismatch", err)
		}
	})
	t.Run("schema_hash_mismatch", func(t *testing.T) {
		_, err := decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash+1, typedRef, physicalRef, raw)
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
	if first.Diagnostics.RowsScanned != len(events) || first.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection {
		t.Fatalf("initial prepared diagnostics=%+v want typed-column snapshot rows=%d", first.Diagnostics, len(events))
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
	if pinned.Diagnostics.RowsScanned != len(events) || pinnedHash != firstHash {
		t.Fatalf("prepared pinned diagnostics=%+v hash=%016x want original rows=%d hash=%016x", pinned.Diagnostics, pinnedHash, len(events), firstHash)
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
	if len(events) != 0 {
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
	}
	preCloseTypedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col))
	wantTypedRefs := 0
	if len(events) != 0 {
		wantTypedRefs = 1
	}
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
