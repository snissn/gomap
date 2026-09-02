package collections

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestAggregateMetadataTypedColumnPartMinMaxSpan1786(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(256)
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, events)
	defer func() { _ = d.Close() }()

	refs := aggregateMetadataRefs1786(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if got, want := len(refs), 2; got != want {
		t.Fatalf("aggregate metadata refs=%d want %d refs=%+v", got, want, refs)
	}
	for _, ref := range refs {
		if ref.PartID != typedColumnPartAssetPartID {
			t.Fatalf("aggregate metadata ref part_id=%d want typed_column_part part_id=%d ref=%+v", ref.PartID, typedColumnPartAssetPartID, ref)
		}
	}

	tests := []struct {
		name     string
		hashName string
		req      ColumnPhysicalQueryRequest
	}{
		{name: "q4b", hashName: "q4b", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "max_time_us"}},
		{name: "q5_metadata", hashName: "q5", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := col.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			gotHash := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.name, result.Groups))
			wantHash := columnPhysicalQueryReferenceHashM13B(tc.hashName, events)
			if gotHash != wantHash {
				t.Fatalf("hash=%016x want %016x groups=%+v", gotHash, wantHash, result.Groups)
			}
			assertAggregateMetadataTypedColumnDiagnostics1786(t, result.Diagnostics, len(events))
		})
	}
}

func TestAggregateMetadataTypedColumnPartGroupCount3000(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	d := openTypedColumnInt64ScanDB(t)
	col := createAggregateMetadataCollection1786(t, d, aggregateMetadataCountTypedColumnPartConfig3000())
	insertAggregateMetadataEvents1786(t, col, events)
	collectionName := col.Meta().Name

	refs := aggregateMetadataRefs1786(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if got, want := len(refs), 1; got != want {
		t.Fatalf("aggregate metadata refs=%d want %d refs=%+v", got, want, refs)
	}
	if refs[0].PartID != typedColumnPartAssetPartID {
		t.Fatalf("aggregate metadata ref part_id=%d want typed_column_part part_id=%d ref=%+v", refs[0].PartID, typedColumnPartAssetPartID, refs[0])
	}

	scanReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "did"}
	scan, err := col.RunColumnPhysicalQuery(scanReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery scan: %v", err)
	}
	if scan.Diagnostics.StorageSource == ColumnPhysicalQueryStorageSourceAggregateMetadata || scan.Diagnostics.RowsScanned != len(events) {
		t.Fatalf("scan diagnostics=%+v want typed-column data scan over %d rows", scan.Diagnostics, len(events))
	}
	wantCounts := make(map[string]int)
	for _, ev := range events {
		wantCounts[ev.Did]++
	}

	metaReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "did", AggregateMetadataName: "count_did"}
	metadata, err := col.RunColumnPhysicalQuery(metaReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery metadata: %v", err)
	}
	assertAggregateMetadataGroupCount3000(t, "direct metadata", metadata, wantCounts, len(events))

	runner, err := col.PrepareColumnPhysicalQuery(metaReq)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery metadata: %v", err)
	}
	defer func() { _ = runner.Close() }()
	for run := 0; run < 2; run++ {
		prepared, err := runner.Run()
		if err != nil {
			t.Fatalf("prepared metadata run %d: %v", run, err)
		}
		assertAggregateMetadataGroupCount3000(t, fmt.Sprintf("prepared metadata run %d", run), prepared, wantCounts, len(events))
	}

	dir := d.Dir()
	if err := d.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open reopened DB: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedResult, err := reopenedCol.RunColumnPhysicalQuery(metaReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery reopened metadata: %v", err)
	}
	assertAggregateMetadataGroupCount3000(t, "reopened metadata", reopenedResult, wantCounts, len(events))
}

func TestAggregateMetadataTypedColumnPartReopen1786(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, events)
	dir := d.Dir()
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open reopened DB: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection(col.Meta().Name)
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	result, err := reopenedCol.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery reopened: %v", err)
	}
	gotHash := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q5_metadata", result.Groups))
	wantHash := columnPhysicalQueryReferenceHashM13B("q5", events)
	if gotHash != wantHash {
		t.Fatalf("reopened hash=%016x want %016x groups=%+v", gotHash, wantHash, result.Groups)
	}
	assertAggregateMetadataTypedColumnDiagnostics1786(t, result.Diagnostics, len(events))
}

func TestAggregateMetadataTypedColumnPartCorruptFailsClosed1786(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(64)
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, events)
	defer func() { _ = d.Close() }()
	refs := aggregateMetadataRefs1786(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) == 0 {
		t.Fatal("missing aggregate metadata refs")
	}
	for _, ref := range refs {
		corruptTypedColumnAssetPayload1755(t, d, ref)
	}
	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"})
	if err == nil {
		t.Fatalf("RunColumnPhysicalQuery corrupt metadata unexpectedly succeeded: %+v", result)
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("corrupt metadata diagnostics materialized rows: %+v", result.Diagnostics)
	}
}

func TestAggregateMetadataMixedOwnersMissingAssetFailsClosed1786(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(32)
	d := openTypedColumnInt64ScanDB(t)
	defer func() { _ = d.Close() }()
	cfg := aggregateMetadataTypedColumnPartConfig1786()
	for i := range cfg.Columns {
		if cfg.Columns[i].Name == "time_us" {
			cfg.Columns[i].Owner = TypedStorageOwnerRowAsset
		}
	}
	col := createAggregateMetadataCollection1786(t, d, cfg)
	insertAggregateMetadataEvents1786(t, col, events)
	if refs := aggregateMetadataRefs1786(columnManifestAssetRefsForCollectionM12A(t, d, col)); len(refs) != 0 {
		t.Fatalf("mixed-owner aggregate metadata refs=%+v want none", refs)
	}
	_, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"})
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "no physical asset refs") {
		t.Fatalf("mixed-owner metadata err=%v want fail-closed no physical asset refs", err)
	}
}

func TestAggregateMetadataTypedColumnPartRejectsStaleSchemaOrGeneration1786(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(16)
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, events)
	defer func() { _ = d.Close() }()
	refs := aggregateMetadataRefs1786(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) == 0 {
		t.Fatal("missing aggregate metadata refs")
	}
	cfg := *col.Meta().Options.ColumnStore
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity: %v", err)
	}
	defer func() { _ = cache.close() }()
	raw, err := cache.read(refs[0], nil)
	if err != nil {
		t.Fatalf("read aggregate metadata: %v", err)
	}
	staleRef := refs[0]
	staleRef.Generation++
	if _, err := decodeColumnAggregateMetadataAsset(raw, staleRef, cfg, col.Meta().Name, "min_time_us"); err == nil || !strings.Contains(err.Error(), "generation/part mismatch") {
		t.Fatalf("decode stale generation err=%v want generation/part mismatch", err)
	}
	staleCfg := cfg
	staleCfg.SchemaHash++
	if _, err := decodeColumnAggregateMetadataAsset(raw, refs[0], staleCfg, col.Meta().Name, "min_time_us"); err == nil || !strings.Contains(err.Error(), "schema_hash") {
		t.Fatalf("decode stale schema err=%v want schema_hash mismatch", err)
	}
}

func TestAggregateMetadataTypedColumnPartPreparedRunnerAllocation1786(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(1024)
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, events)
	defer func() { _ = d.Close() }()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}
	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	defer func() { _ = runner.Close() }()
	for i := 0; i < 3; i++ {
		result, err := runner.Run()
		if err != nil {
			t.Fatalf("runner warmup: %v", err)
		}
		gotHash := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B("q5_metadata", result.Groups))
		wantHash := columnPhysicalQueryReferenceHashM13B("q5", events)
		if gotHash != wantHash {
			t.Fatalf("runner hash=%016x want %016x", gotHash, wantHash)
		}
		assertAggregateMetadataTypedColumnDiagnostics1786(t, result.Diagnostics, len(events))
	}
	allocs := testing.AllocsPerRun(20, func() {
		result, err := runner.Run()
		if err != nil {
			panic(fmt.Sprintf("runner Run: %v", err))
		}
		if len(result.Groups) == 0 {
			panic("empty metadata result")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed typed-column aggregate metadata runner allocs/run=%.2f want 0", allocs)
	}
}

func TestAggregateMetadataPreparedTopK1871(t *testing.T) {
	events := []columnPhysicalQueryEventM13B{
		{ID: "e00", Did: "", TimeUS: 0, Kind: "post"},
		{ID: "e01", Did: "", TimeUS: 100, Kind: "post"},
		{ID: "e02", Did: "did:a", TimeUS: 10, Kind: "post"},
		{ID: "e03", Did: "did:a", TimeUS: 20, Kind: "post"},
		{ID: "e04", Did: "did:b", TimeUS: 5, Kind: "post"},
		{ID: "e05", Did: "did:b", TimeUS: 15, Kind: "post"},
		{ID: "e06", Did: "did:c", TimeUS: 5, Kind: "post"},
		{ID: "e07", Did: "did:c", TimeUS: 13, Kind: "post"},
		{ID: "e08", Did: "did:d", TimeUS: 1, Kind: "post"},
		{ID: "e09", Did: "did:d", TimeUS: 10, Kind: "post"},
		{ID: "e10", Did: "did:e", TimeUS: 7, Kind: "post"},
		{ID: "e11", Did: "did:e", TimeUS: 14, Kind: "post"},
	}
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, events)
	defer func() { _ = d.Close() }()

	tests := []struct {
		name  string
		req   ColumnPhysicalQueryRequest
		want  []ColumnPhysicalQueryGroup
		order ColumnPhysicalQueryTopKOrder
	}{
		{
			name:  "q4 asc min skips empty",
			req:   ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: 3, TopKOrder: ColumnPhysicalQueryTopKInt64Asc, SkipEmptyGroupKey: true},
			want:  []ColumnPhysicalQueryGroup{{Key: "did:d", Int64: 1}, {Key: "did:b", Int64: 5}, {Key: "did:c", Int64: 5}},
			order: ColumnPhysicalQueryTopKInt64Asc,
		},
		{
			name:  "q5 desc span skips empty",
			req:   ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: 3, TopKOrder: ColumnPhysicalQueryTopKInt64Desc, SkipEmptyGroupKey: true},
			want:  []ColumnPhysicalQueryGroup{{Key: "did:a", Int64: 10}, {Key: "did:b", Int64: 10}, {Key: "did:d", Int64: 9}},
			order: ColumnPhysicalQueryTopKInt64Desc,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oneShot, err := col.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			if !equalColumnPhysicalGroups1871(oneShot.Groups, tc.want) {
				t.Fatalf("one-shot groups=%+v want %+v", oneShot.Groups, tc.want)
			}
			runner, err := col.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			defer func() { _ = runner.Close() }()
			for i := 0; i < 2; i++ {
				result, err := runner.Run()
				if err != nil {
					t.Fatalf("runner Run %d: %v", i, err)
				}
				if !equalColumnPhysicalGroups1871(result.Groups, tc.want) {
					t.Fatalf("groups=%+v want %+v", result.Groups, tc.want)
				}
				if result.Diagnostics.RowsScanned != 0 || result.Diagnostics.ReduceRows != len(events) {
					t.Fatalf("diagnostics=%+v want zero scanned and reduce rows=%d", result.Diagnostics, len(events))
				}
				if result.Diagnostics.TopKLimit != 3 || result.Diagnostics.TopKOrder != string(tc.order) || result.Diagnostics.TopKCandidates != 5 || result.Diagnostics.ResultGroups != 3 || result.Diagnostics.ResultShapeNanos < 0 {
					t.Fatalf("top-K diagnostics=%+v", result.Diagnostics)
				}
			}
		})
	}

	allReq := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}
	all, err := col.RunColumnPhysicalQuery(allReq)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery all groups: %v", err)
	}
	if len(all.Groups) != 6 || all.Groups[0].Key != "" || all.Diagnostics.TopKLimit != 0 {
		t.Fatalf("all-groups result=%+v diagnostics=%+v", all.Groups, all.Diagnostics)
	}
}

func TestAggregateMetadataTopKValidation1871(t *testing.T) {
	d, col := openAggregateMetadataTypedColumnPartFixture1786(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer func() { _ = d.Close() }()
	tests := []struct {
		name string
		req  ColumnPhysicalQueryRequest
		want string
	}{
		{name: "negative", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: -1}, want: "non-negative"},
		{name: "missing order", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: 3}, want: "order is required"},
		{name: "order without limit", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopKOrder: ColumnPhysicalQueryTopKInt64Asc}, want: "requires a positive limit"},
		{name: "unsupported kind", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: 3, TopKOrder: ColumnPhysicalQueryTopKInt64Asc}, want: "requires an int64 aggregate kind"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := col.PrepareColumnPhysicalQuery(tc.req); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("PrepareColumnPhysicalQuery err=%v want ErrColumnQueryPlanUnsupported containing %q", err, tc.want)
			}
		})
	}
	if _, err := col.RunColumnPhysicalQueryParallel(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}, 4); !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "parallel aggregate metadata") {
		t.Fatalf("RunColumnPhysicalQueryParallel aggregate metadata err=%v want fail-closed", err)
	}
}

func equalColumnPhysicalGroups1871(a, b []ColumnPhysicalQueryGroup) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func openAggregateMetadataTypedColumnPartFixture1786(tb testing.TB, events []columnPhysicalQueryEventM13B) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	col := createAggregateMetadataCollection1786(tb, d, aggregateMetadataTypedColumnPartConfig1786())
	insertAggregateMetadataEvents1786(tb, col, events)
	return d, col
}

func aggregateMetadataTypedColumnPartConfig1786() *ColumnStoreConfig {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset, Dictionary: true},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = []ColumnAggregateMetadata{
		{Name: "min_time_us", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
		{Name: "max_time_us", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMax},
	}
	return cfg
}

func aggregateMetadataCountTypedColumnPartConfig3000() *ColumnStoreConfig {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset, Dictionary: true},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = []ColumnAggregateMetadata{
		{Name: "count_did", GroupColumn: "did", Kind: ColumnAggregateCount},
	}
	return cfg
}

func createAggregateMetadataCollection1786(tb testing.TB, d *backenddb.DB, cfg *ColumnStoreConfig) *Collection {
	tb.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func insertAggregateMetadataEvents1786(tb testing.TB, col *Collection, events []columnPhysicalQueryEventM13B) {
	tb.Helper()
	ids := make([][]byte, len(events))
	docs := make([][]byte, len(events))
	for i, event := range events {
		ids[i] = []byte(event.ID)
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"%s","did":"%s","payload":"ignored_%d"}`, event.TimeUS, event.Kind, event.Did, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
}

func aggregateMetadataRefs1786(refs []ColumnAssetRef) []ColumnAssetRef {
	out := make([]ColumnAssetRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind == ColumnAssetKindTCS1AggregateMetadata {
			out = append(out, ref)
		}
	}
	return out
}

func BenchmarkAggregateMetadataTypedColumnPart1786(b *testing.B) {
	const rows = 4096
	events := columnPhysicalQueryFixtureEventsM13B(rows)
	reqMetadata := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
	reqMetadataTopK := reqMetadata
	reqMetadataTopK.TopK = 3
	reqMetadataTopK.TopKOrder = ColumnPhysicalQueryTopKInt64Desc
	reqMetadataTopK.SkipEmptyGroupKey = true
	reqScan := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
	reqCountMetadata := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "did", AggregateMetadataName: "count_did", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
	reqCountScan := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "did", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}

	b.Run("typed_column_metadata", func(b *testing.B) {
		d, col := openAggregateMetadataTypedColumnPartFixture1786(b, events)
		defer func() { _ = d.Close() }()
		preview, err := col.RunColumnPhysicalQuery(reqMetadata)
		if err != nil {
			b.Fatalf("preview metadata: %v", err)
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := col.RunColumnPhysicalQuery(reqMetadata)
			if err != nil {
				b.Fatalf("RunColumnPhysicalQuery metadata: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("typed_column_metadata_prepared", func(b *testing.B) {
		d, col := openAggregateMetadataTypedColumnPartFixture1786(b, events)
		defer func() { _ = d.Close() }()
		runner, err := col.PrepareColumnPhysicalQuery(reqMetadata)
		if err != nil {
			b.Fatalf("PrepareColumnPhysicalQuery metadata: %v", err)
		}
		defer func() { _ = runner.Close() }()
		preview, err := runner.Run()
		if err != nil {
			b.Fatalf("preview prepared metadata: %v", err)
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := runner.Run()
			if err != nil {
				b.Fatalf("runner Run metadata: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("typed_column_metadata_prepared_topk", func(b *testing.B) {
		d, col := openAggregateMetadataTypedColumnPartFixture1786(b, events)
		defer func() { _ = d.Close() }()
		runner, err := col.PrepareColumnPhysicalQuery(reqMetadataTopK)
		if err != nil {
			b.Fatalf("PrepareColumnPhysicalQuery top-K metadata: %v", err)
		}
		defer func() { _ = runner.Close() }()
		preview, err := runner.Run()
		if err != nil {
			b.Fatalf("preview prepared top-K metadata: %v", err)
		}
		if got, want := len(preview.Groups), 3; got != want {
			b.Fatalf("preview top-K groups=%d want %d", got, want)
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := runner.Run()
			if err != nil {
				b.Fatalf("runner Run top-K metadata: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("typed_column_group_count_metadata", func(b *testing.B) {
		d, col := openAggregateMetadataCountTypedColumnPartFixture3000(b, events)
		defer func() { _ = d.Close() }()
		preview, err := col.RunColumnPhysicalQuery(reqCountMetadata)
		if err != nil {
			b.Fatalf("preview group-count metadata: %v", err)
		}
		if preview.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceAggregateMetadata || len(preview.Groups) == 0 {
			b.Fatalf("preview group-count metadata diagnostics=%+v groups=%d", preview.Diagnostics, len(preview.Groups))
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := col.RunColumnPhysicalQuery(reqCountMetadata)
			if err != nil {
				b.Fatalf("RunColumnPhysicalQuery group-count metadata: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("typed_column_group_count_metadata_prepared", func(b *testing.B) {
		d, col := openAggregateMetadataCountTypedColumnPartFixture3000(b, events)
		defer func() { _ = d.Close() }()
		runner, err := col.PrepareColumnPhysicalQuery(reqCountMetadata)
		if err != nil {
			b.Fatalf("PrepareColumnPhysicalQuery group-count metadata: %v", err)
		}
		defer func() { _ = runner.Close() }()
		preview, err := runner.Run()
		if err != nil {
			b.Fatalf("preview prepared group-count metadata: %v", err)
		}
		if preview.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceAggregateMetadata || len(preview.Groups) == 0 {
			b.Fatalf("preview prepared group-count metadata diagnostics=%+v groups=%d", preview.Diagnostics, len(preview.Groups))
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := runner.Run()
			if err != nil {
				b.Fatalf("runner Run group-count metadata: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("typed_column_group_count_direct_scan", func(b *testing.B) {
		d, col := openAggregateMetadataCountTypedColumnPartFixture3000(b, events)
		defer func() { _ = d.Close() }()
		preview, err := col.RunColumnPhysicalQuery(reqCountScan)
		if err != nil {
			b.Fatalf("preview group-count scan: %v", err)
		}
		if preview.Diagnostics.StorageSource == ColumnPhysicalQueryStorageSourceAggregateMetadata || len(preview.Groups) == 0 {
			b.Fatalf("preview group-count scan diagnostics=%+v groups=%d", preview.Diagnostics, len(preview.Groups))
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := col.RunColumnPhysicalQuery(reqCountScan)
			if err != nil {
				b.Fatalf("RunColumnPhysicalQuery group-count scan: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("typed_row_asset_direct_scan", func(b *testing.B) {
		col, closeFn := openColumnPhysicalQueryFixtureM13B(b, events)
		defer closeFn()
		preview, err := col.RunColumnPhysicalQuery(reqScan)
		if err != nil {
			b.Fatalf("preview scan: %v", err)
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := col.RunColumnPhysicalQuery(reqScan)
			if err != nil {
				b.Fatalf("RunColumnPhysicalQuery scan: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
	b.Run("document_full_scan_reconstruction", func(b *testing.B) {
		d, col := openAggregateMetadataDocumentFixture1786(b, events)
		defer func() { _ = d.Close() }()
		preview, err := runAggregateMetadataDocumentFullScan1786(col)
		if err != nil {
			b.Fatalf("preview document scan: %v", err)
		}
		b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
		b.ReportAllocs()
		b.ResetTimer()
		var last ColumnPhysicalQueryDiagnostics
		for i := 0; i < b.N; i++ {
			result, err := runAggregateMetadataDocumentFullScan1786(col)
			if err != nil {
				b.Fatalf("document scan: %v", err)
			}
			last = result.Diagnostics
		}
		reportAggregateMetadataPhysicalQueryBench1786(b, last, rows)
	})
}

func openAggregateMetadataCountTypedColumnPartFixture3000(tb testing.TB, events []columnPhysicalQueryEventM13B) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	col := createAggregateMetadataCollection1786(tb, d, aggregateMetadataCountTypedColumnPartConfig3000())
	insertAggregateMetadataEvents1786(tb, col, events)
	return d, col
}

func openAggregateMetadataDocumentFixture1786(tb testing.TB, events []columnPhysicalQueryEventM13B) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events"}); err != nil {
		tb.Fatalf("CreateCollection document fixture: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		tb.Fatalf("OpenCollection document fixture: %v", err)
	}
	insertAggregateMetadataEvents1786(tb, col, events)
	return d, col
}

func runAggregateMetadataDocumentFullScan1786(col *Collection) (ColumnPhysicalQueryResult, error) {
	fallbackCfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
	}}
	spans := make(map[string]columnPhysicalQuerySpan)
	result := ColumnPhysicalQueryResult{}
	_, err := col.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		result.Diagnostics.RowMaterializations++
		result.Diagnostics.DocumentMaterializations++
		result.Diagnostics.FallbackReads++
		result.Diagnostics.PhysicalBytesScanned += int64(len(record.Document))
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(fallbackCfg, []columnWriteDocument{{ID: record.ID, Document: record.Document}})
		if err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0].Values) != 2 {
			return false, errors.New("collections: document fallback failed to extract aggregate columns")
		}
		groupValue := rows[0].Values[0]
		valueValue := rows[0].Values[1]
		if groupValue.Type != ColumnStoreValueString || valueValue.Type != ColumnStoreValueInt64 || groupValue.Null || valueValue.Null {
			return false, fmt.Errorf("%w: document aggregate fallback encountered incompatible values", ErrColumnQueryPlanUnsupported)
		}
		group := groupValue.String
		if group == "" && groupValue.StringBytes != nil {
			group = string(groupValue.StringBytes)
		}
		cur, ok := spans[group]
		if !ok {
			spans[group] = columnPhysicalQuerySpan{min: valueValue.Int64, max: valueValue.Int64}
		} else {
			if valueValue.Int64 < cur.min {
				cur.min = valueValue.Int64
			}
			if valueValue.Int64 > cur.max {
				cur.max = valueValue.Int64
			}
			spans[group] = cur
		}
		result.Diagnostics.RowsScanned++
		result.Diagnostics.ReduceRows++
		return true, nil
	})
	if err != nil {
		return result, err
	}
	result.Groups = make([]ColumnPhysicalQueryGroup, 0, len(spans))
	for key, span := range spans {
		result.Groups = append(result.Groups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
	}
	sortColumnPhysicalQueryGroupsByKey(result.Groups)
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, nil
}

func reportAggregateMetadataPhysicalQueryBench1786(b *testing.B, diag ColumnPhysicalQueryDiagnostics, rows int) {
	b.Helper()
	elapsed := b.Elapsed()
	if elapsed > 0 && b.N > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
		b.ReportMetric(float64(rows*b.N)/elapsed.Seconds(), "rows/sec")
		if diag.ResultGroups > 0 {
			b.ReportMetric(float64(diag.ResultGroups*b.N)/elapsed.Seconds(), "groups/sec")
		}
	}
	b.ReportMetric(float64(diag.MetadataHits), "metadata_hits/op")
	b.ReportMetric(float64(diag.MetadataMisses), "metadata_misses/op")
	b.ReportMetric(float64(diag.ScheduledGranules), "scheduled_granules/op")
	b.ReportMetric(float64(diag.SkippedGranules), "skipped_granules/op")
	b.ReportMetric(float64(diag.DecodedMetadataBytes), "metadata_decoded_bytes/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.FallbackReads), "fallback_reads/op")
	b.ReportMetric(float64(diag.DecodedBlocks), "decoded_blocks/op")
	b.ReportMetric(float64(diag.RowMaterializations), "row_materializations/op")
	b.ReportMetric(float64(diag.DocumentMaterializations), "document_materializations/op")
	b.ReportMetric(float64(diag.ReconstructionRows), "reconstruction_rows/op")
	b.ReportMetric(float64(diag.TopKLimit), "topk_limit/op")
	b.ReportMetric(float64(diag.TopKCandidates), "topk_candidates/op")
	b.ReportMetric(float64(diag.ResultShapeNanos), "result_shape_ns/op")
}

func assertAggregateMetadataTypedColumnDiagnostics1786(t testing.TB, diag ColumnPhysicalQueryDiagnostics, wantRows int) {
	t.Helper()
	if diag.MetadataHits == 0 || diag.MetadataMisses != 0 || diag.DecodedMetadataBytes == 0 {
		t.Fatalf("metadata diagnostics=%+v want hits and decoded bytes", diag)
	}
	if diag.RowsScanned != 0 || diag.DecodedBlocks != 0 || diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 || diag.ReconstructionRows != 0 {
		t.Fatalf("typed-column aggregate metadata materialized/scanned rows: %+v", diag)
	}
	if diag.ReduceRows != wantRows || diag.ResultGroups == 0 {
		t.Fatalf("typed-column aggregate metadata reduce diagnostics=%+v want rows=%d and groups", diag, wantRows)
	}
	if diag.MappedBytes+diag.HeapCopyBytes == 0 || diag.PhysicalBytesScanned <= 0 {
		t.Fatalf("typed-column aggregate metadata bytes diagnostics=%+v", diag)
	}
	if diag.ScheduledGranules == 0 || diag.SkippedGranules != 0 {
		t.Fatalf("typed-column aggregate metadata skip diagnostics=%+v", diag)
	}
}

func assertAggregateMetadataGroupCount3000(t testing.TB, label string, result ColumnPhysicalQueryResult, wantCounts map[string]int, wantRows int) {
	t.Helper()
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceAggregateMetadata || result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone {
		t.Fatalf("%s diagnostics storage/fallback=%+v", label, result.Diagnostics)
	}
	assertAggregateMetadataTypedColumnDiagnostics1786(t, result.Diagnostics, wantRows)
	if result.Diagnostics.ProjectedColumns != 1 || result.Diagnostics.PredicateCount != 0 {
		t.Fatalf("%s projected/predicate diagnostics=%+v want one projected group column and no predicates", label, result.Diagnostics)
	}
	gotCounts := columnPhysicalQueryGroupCountsM14B(result.Groups)
	if !reflect.DeepEqual(gotCounts, wantCounts) {
		t.Fatalf("%s counts=%v want %v groups=%+v", label, gotCounts, wantCounts, result.Groups)
	}
}
