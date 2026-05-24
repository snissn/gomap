package collections

import (
	"errors"
	"fmt"
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
	reqScan := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}

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
	b.ReportMetric(float64(diag.DecodedBlocks), "decoded_blocks/op")
	b.ReportMetric(float64(diag.RowMaterializations), "row_materializations/op")
	b.ReportMetric(float64(diag.ReconstructionRows), "reconstruction_rows/op")
}

func assertAggregateMetadataTypedColumnDiagnostics1786(t testing.TB, diag ColumnPhysicalQueryDiagnostics, wantRows int) {
	t.Helper()
	if diag.MetadataHits == 0 || diag.MetadataMisses != 0 || diag.DecodedMetadataBytes == 0 {
		t.Fatalf("metadata diagnostics=%+v want hits and decoded bytes", diag)
	}
	if diag.RowsScanned != 0 || diag.DecodedBlocks != 0 || diag.RowMaterializations != 0 || diag.ReconstructionRows != 0 {
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
