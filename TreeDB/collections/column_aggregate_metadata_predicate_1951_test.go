package collections

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPredicateQualifiedAggregateMetadataBuildsPerTypedGranule1951(t *testing.T) {
	cfg := *predicateAggregateMetadataConfig1951()
	rows := make([]columnDeclaredRow, typedColumnDefaultRowsPerGranule()+1)
	for idx := range rows {
		rows[idx] = columnDeclaredRow{ID: []byte(fmt.Sprintf("row-%d", idx)), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: int64(idx)},
			{Type: ColumnStoreValueString, Present: true, String: "commit"},
			{Type: ColumnStoreValueString, Present: true, String: "create"},
			{Type: ColumnStoreValueString, Present: true, String: "app.bsky.feed.post"},
			{Type: ColumnStoreValueString, Present: true, String: "did:one"},
		}}
	}
	asset, ok, err := buildColumnAggregateMetadataAsset(cfg, rows, cfg.AggregateMetadata[0], "events", "events/column-assets", 1, typedColumnPartAssetPartID, 1)
	if err != nil || !ok {
		t.Fatalf("buildColumnAggregateMetadataAsset ok=%v err=%v", ok, err)
	}
	if got, want := asset.Rows, len(rows); got != want {
		t.Fatalf("asset rows=%d want %d", got, want)
	}
	if got, want := len(asset.Entries), 2; got != want {
		t.Fatalf("asset entries=%+v want one summary per typed-column granule", asset.Entries)
	}
	if asset.Entries[0].Group != "did:one" || asset.Entries[0].Count != typedColumnDefaultRowsPerGranule() || asset.Entries[1].Group != "did:one" || asset.Entries[1].Count != 1 {
		t.Fatalf("asset entries=%+v want split counts across typed-column granules", asset.Entries)
	}
	if got, want := len(asset.Predicates), 3; got != want {
		t.Fatalf("asset predicate coverage=%+v want %d predicates", asset.Predicates, want)
	}
}

func TestPredicateQualifiedAggregateMetadataQ4Q5DirectPrepared1951(t *testing.T) {
	batches := [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()}
	events := flattenColumnPhysicalEvents1950(batches)
	_, col, closeFn := openPredicateAggregateMetadataFixture1951(t, batches)
	defer closeFn()

	cases := []struct {
		name string
		full ColumnPhysicalQueryRequest
		meta ColumnPhysicalQueryRequest
	}{
		{
			name: "q4a",
			full: columnPredicateAggregateMetadataQ4Request1951(false),
			meta: columnPredicateAggregateMetadataQ4Request1951(true),
		},
		{
			name: "q5",
			full: columnPredicateAggregateMetadataQ5Request1951(false),
			meta: columnPredicateAggregateMetadataQ5Request1951(true),
		},
	}
	matchedRows := columnPhysicalJSONBenchReferenceMatchedRowsP0("q5", events)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full, err := col.RunColumnPhysicalQuery(tc.full)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery full %s: %v", tc.name, err)
			}
			if full.Diagnostics.StorageSource == ColumnPhysicalQueryStorageSourceAggregateMetadata || full.Diagnostics.RowsScanned != len(events) || full.Diagnostics.RowsMatched != matchedRows {
				t.Fatalf("full %s diagnostics=%+v want data scan over real predicates", tc.name, full.Diagnostics)
			}
			wantHash := hashPredicateAggregateMetadataGroups1951(full.Groups)

			metadata, err := col.RunColumnPhysicalQuery(tc.meta)
			if err != nil {
				t.Fatalf("RunColumnPhysicalQuery metadata %s: %v", tc.name, err)
			}
			assertPredicateAggregateMetadataResult1951(t, "direct metadata "+tc.name, metadata, wantHash, matchedRows, len(events))

			runner, err := col.PrepareColumnPhysicalQuery(tc.meta)
			if err != nil {
				t.Fatalf("PrepareColumnPhysicalQuery metadata %s: %v", tc.name, err)
			}
			defer func() { _ = runner.Close() }()
			for run := 0; run < 2; run++ {
				prepared, err := runner.Run()
				if err != nil {
					t.Fatalf("prepared metadata %s run %d: %v", tc.name, run, err)
				}
				assertPredicateAggregateMetadataResult1951(t, fmt.Sprintf("prepared metadata %s run %d", tc.name, run), prepared, wantHash, matchedRows, len(events))
			}
		})
	}
}

func TestPredicateQualifiedAggregateMetadataExactPredicateCoverage1951(t *testing.T) {
	_, col, closeFn := openPredicateAggregateMetadataFixture1951(t, [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950()})
	defer closeFn()

	mismatched := columnPredicateAggregateMetadataQ5Request1951(true)
	mismatched.Predicates = []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
		{Column: "collection", Value: "app.bsky.feed.like"},
	}
	if _, err := col.RunColumnPhysicalQuery(mismatched); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery mismatched predicates err=%v want unsupported fail-closed", err)
	}
	if _, err := col.PrepareColumnPhysicalQuery(mismatched); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("PrepareColumnPhysicalQuery mismatched predicates err=%v want unsupported fail-closed", err)
	}
}

func TestPredicateQualifiedAggregateMetadataColumnAssetRewriteRoundTrip1951(t *testing.T) {
	d, col, closeFn := openPredicateAggregateMetadataFixture1951(t, [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950(), columnPhysicalQ5DenseBatchB1950()})
	defer closeFn()

	req := columnPredicateAggregateMetadataQ5Request1951(true)
	before, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery before rewrite: %v", err)
	}
	beforeHash := hashPredicateAggregateMetadataGroups1951(before.Groups)
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, rewrite test requires live column assets")
	}
	candidate := writePredicateAggregateMetadataRewriteCandidate1951(t, d, col, 1951, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{Detailed: true, CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if stats.RefsRemapped == 0 || stats.SegmentsRewritten == 0 {
		t.Fatalf("rewrite stats=%+v want remapped metadata/typed refs", stats)
	}
	after, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery after rewrite: %v", err)
	}
	assertPredicateAggregateMetadataResult1951(t, "metadata after column asset rewrite", after, beforeHash, before.Diagnostics.RowsMatched, len(columnPhysicalQ5DenseBatchA1950())+len(columnPhysicalQ5DenseBatchB1950()))
}

func TestPredicateQualifiedAggregateMetadataRejectsMutationParts1951(t *testing.T) {
	_, col, closeFn := openPredicateAggregateMetadataFixture1951(t, [][]columnPhysicalJSONBenchParityEventP0{columnPhysicalQ5DenseBatchA1950()})
	defer closeFn()

	if _, _, err := col.Update([]byte("a-m-1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"time_us":1900000000000999,"kind":"commit","operation":"create","collection":"app.bsky.feed.post","did":"did:m"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, err := col.RunColumnPhysicalQuery(columnPredicateAggregateMetadataQ5Request1951(true)); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery metadata after mutation err=%v want unsupported", err)
	}
	if _, err := col.PrepareColumnPhysicalQuery(columnPredicateAggregateMetadataQ5Request1951(true)); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("PrepareColumnPhysicalQuery metadata after mutation err=%v want unsupported", err)
	}
}

func BenchmarkPredicateQualifiedAggregateMetadataQ4Q5DirectPrepared1951(b *testing.B) {
	events := columnPhysicalQ5DenseBenchmarkEvents1950(16_384)
	_, col, closeFn := openPredicateAggregateMetadataFixture1951(b, [][]columnPhysicalJSONBenchParityEventP0{events})
	defer closeFn()

	for _, tc := range []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{name: "q4a_direct_metadata", req: columnPredicateAggregateMetadataQ4Request1951(true)},
		{name: "q5_direct_metadata", req: columnPredicateAggregateMetadataQ5Request1951(true)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			preview, err := col.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			var groups int
			for i := 0; i < b.N; i++ {
				result, err := col.RunColumnPhysicalQuery(tc.req)
				if err != nil {
					b.Fatalf("RunColumnPhysicalQuery: %v", err)
				}
				groups += len(result.Groups)
				last = result.Diagnostics
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no groups")
			}
			reportPredicateAggregateMetadataBenchMetrics1951(b, last)
		})
	}

	for _, tc := range []struct {
		name string
		req  ColumnPhysicalQueryRequest
	}{
		{name: "q4a_prepared_metadata", req: columnPredicateAggregateMetadataQ4Request1951(true)},
		{name: "q5_prepared_metadata", req: columnPredicateAggregateMetadataQ5Request1951(true)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runner, err := col.PrepareColumnPhysicalQuery(tc.req)
			if err != nil {
				b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			defer func() { _ = runner.Close() }()
			preview, err := runner.Run()
			if err != nil {
				b.Fatalf("preview runner.Run: %v", err)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var last ColumnPhysicalQueryDiagnostics
			var groups int
			for i := 0; i < b.N; i++ {
				result, err := runner.Run()
				if err != nil {
					b.Fatalf("runner.Run: %v", err)
				}
				groups += len(result.Groups)
				last = result.Diagnostics
			}
			b.StopTimer()
			if groups == 0 {
				b.Fatal("benchmark produced no groups")
			}
			reportPredicateAggregateMetadataBenchMetrics1951(b, last)
		})
	}
}

func openPredicateAggregateMetadataFixture1951(tb testing.TB, batches [][]columnPhysicalJSONBenchParityEventP0) (*backenddb.DB, *Collection, func()) {
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
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: predicateAggregateMetadataConfig1951()}}); err != nil {
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

func writePredicateAggregateMetadataRewriteCandidate1951(tb testing.TB, d *backenddb.DB, col *Collection, generation, partID uint64) ColumnAssetRef {
	tb.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil {
		tb.Fatal("missing column store config")
	}
	rows := []columnDeclaredRow{{
		ID: []byte("rewrite-candidate"),
		Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: int64(generation)},
			{Type: ColumnStoreValueString, Present: true, String: "commit"},
			{Type: ColumnStoreValueString, Present: true, String: "create"},
			{Type: ColumnStoreValueString, Present: true, String: "app.bsky.feed.post"},
			{Type: ColumnStoreValueString, Present: true, String: "did:rewrite-candidate"},
		},
	}}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: d.State().AppliedCommandLSN + 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows:              rows,
	})
	if err != nil {
		tb.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	ref, err := writeColumnPhysicalAssetToManager(d.ColumnAssetRootDir(), *cfg, encoded, generation, partID)
	if err != nil {
		tb.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	return ref
}

func predicateAggregateMetadataConfig1951() *ColumnStoreConfig {
	cfg := typedColumnSortKeyConfig1948(nil)
	cfg.AggregateMetadata = []ColumnAggregateMetadata{
		{
			Name:        "post_time_us_minmax",
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        ColumnAggregateMin,
			Predicates:  columnPredicateAggregateMetadataPostPredicates1951(),
		},
	}
	return cfg
}

func columnPredicateAggregateMetadataPostPredicates1951() []ColumnPhysicalQueryPredicate {
	return []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
		{Column: "collection", Value: "app.bsky.feed.post"},
	}
}

func columnPredicateAggregateMetadataQ4Request1951(metadata bool) ColumnPhysicalQueryRequest {
	req := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupMinInt64,
		GroupColumn: "did",
		ValueColumn: "time_us",
		TopK:        3,
		TopKOrder:   ColumnPhysicalQueryTopKInt64Asc,
		Predicates:  columnPredicateAggregateMetadataPostPredicates1951(),
	}
	if metadata {
		req.AggregateMetadataName = "post_time_us_minmax"
	}
	return req
}

func columnPredicateAggregateMetadataQ5Request1951(metadata bool) ColumnPhysicalQueryRequest {
	req := ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupInt64Span,
		GroupColumn: "did",
		ValueColumn: "time_us",
		TopK:        3,
		TopKOrder:   ColumnPhysicalQueryTopKInt64Desc,
		Predicates:  columnPredicateAggregateMetadataPostPredicates1951(),
	}
	if metadata {
		req.AggregateMetadataName = "post_time_us_minmax"
	}
	return req
}

func assertPredicateAggregateMetadataResult1951(tb testing.TB, label string, result ColumnPhysicalQueryResult, wantHash uint64, matchedRows, totalRows int) {
	tb.Helper()
	if got := hashPredicateAggregateMetadataGroups1951(result.Groups); got != wantHash {
		tb.Fatalf("%s hash=%016x want %016x groups=%+v", label, got, wantHash, result.Groups)
	}
	diag := result.Diagnostics
	if diag.StorageSource != ColumnPhysicalQueryStorageSourceAggregateMetadata || diag.FallbackReason != ColumnPhysicalQueryFallbackNone {
		tb.Fatalf("%s diagnostics storage/fallback=%+v", label, diag)
	}
	if diag.RowsScanned != 0 || diag.DecodedBlocks != 0 || diag.RowMaterializations != 0 || diag.DocumentMaterializations != 0 {
		tb.Fatalf("%s metadata path scanned/materialized data rows: %+v", label, diag)
	}
	if diag.PredicateCount != 3 || diag.PredicateLiterals != 3 || diag.RowsMatched != matchedRows || diag.ReduceRows != matchedRows {
		tb.Fatalf("%s predicate/source-row diagnostics=%+v want matched/reduced=%d", label, diag, matchedRows)
	}
	if diag.MetadataHits == 0 || diag.MetadataEntries == 0 || diag.DecodedMetadataBytes == 0 || diag.PhysicalBytesScanned <= 0 {
		tb.Fatalf("%s metadata diagnostics=%+v want metadata entries/hits/bytes", label, diag)
	}
	if diag.MetadataEntries > matchedRows || matchedRows <= 0 || totalRows <= matchedRows {
		tb.Fatalf("%s unexpected metadata/source counts diagnostics=%+v matched=%d total=%d", label, diag, matchedRows, totalRows)
	}
}

func hashPredicateAggregateMetadataGroups1951(groups []ColumnPhysicalQueryGroup) uint64 {
	h := fnv.New64a()
	for _, group := range groups {
		_, _ = fmt.Fprintf(h, "%s=%d\n", group.Key, group.Int64)
	}
	return h.Sum64()
}

func reportPredicateAggregateMetadataBenchMetrics1951(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	b.ReportMetric(float64(diag.MetadataHits), "metadata_hits/op")
	b.ReportMetric(float64(diag.MetadataEntries), "metadata_entries/op")
	b.ReportMetric(float64(diag.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(diag.ReduceRows), "reduce_rows/op")
	b.ReportMetric(float64(diag.DecodedMetadataBytes), "metadata_decoded_bytes/op")
	b.ReportMetric(float64(diag.ResultShapeNanos), "result_shape_ns/op")
}
