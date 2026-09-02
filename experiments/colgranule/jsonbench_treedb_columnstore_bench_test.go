package colgranule

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const defaultJSONBenchColumnStoreCompareBatchRows = 32768

// BenchmarkJSONBenchColumnStoreCompare keeps the historical colgranule JSONBench
// kernels and the current durable TreeDB column-store physical query path in one
// benchmark matrix. The fixture is intentionally synthetic and filter-degenerate:
// every row is kind=commit, operation=create, collection=app.bsky.feed.post.
// That makes the current TreeDB physical query API (which does not yet expose
// JSONBench's separate filter columns) comparable to the older colgranule
// Q1/Q2/Q3-hour/Q4/Q5 shapes.
func BenchmarkJSONBenchColumnStoreCompare(b *testing.B) {
	rows := jsonBenchColumnStoreCompareRows(b)
	ds := syntheticJSONBenchPostOnlyCompareDataset(rows)
	b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
		benchmarkJSONBenchColumnStoreCompareLegacy(b, ds)
		benchmarkJSONBenchColumnStoreCompareTreeDB(b, ds)
	})
}

func jsonBenchColumnStoreCompareRows(b *testing.B) int {
	b.Helper()
	env := os.Getenv("TREEDB_JSONBENCH_COMPARE_ROWS")
	if env == "" {
		return DefaultRowsPerGranule * 100
	}
	rows, err := strconv.Atoi(env)
	if err != nil || rows <= 0 {
		b.Fatalf("TREEDB_JSONBENCH_COMPARE_ROWS=%q must be positive integer", env)
	}
	return rows
}

func benchmarkJSONBenchColumnStoreCompareLegacy(b *testing.B, ds JSONBenchDataset) {
	timePart, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout(time_us): %v", err)
	}
	clickHousePart, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout(clickhouse): %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		part *ColumnPart
		run  jsonBenchPartQueryRunner
	}{
		{name: "q1_grouped_count", part: timePart, run: runJSONBenchPartQ1},
		{name: "q2_group_count_distinct", part: timePart, run: runJSONBenchPartQ2},
		{name: "q3_hourly_grouped_count", part: timePart, run: runJSONBenchPartQ3},
		{name: "q4_time_order_early_stop_min_by_user", part: timePart, run: runJSONBenchPartQ4},
		{name: "q4_full_scan_min_by_user", part: clickHousePart, run: runJSONBenchPartQ4ClickHouseOrder},
		{name: "q5_span_by_user", part: timePart, run: runJSONBenchPartQ5},
		{name: "q4_metadata_min_by_user", part: clickHousePart, run: runJSONBenchPartQ4ClickHouseOrderAggregateMetadata},
		{name: "q5_metadata_span_by_user", part: timePart, run: runJSONBenchPartQ5AggregateMetadata},
	}
	for _, q := range queries {
		q := q
		b.Run("legacy_colgranule_encoded/"+q.name, func(b *testing.B) {
			scratch := &jsonBenchPartQueryScratch{scanner: q.part.NewScanner(), projected: make(map[string][]int64, 6)}
			rows, digest, diagnostics, err := q.run(q.part, codes, scratch)
			if err != nil {
				b.Fatalf("legacy preview %s: %v", q.name, err)
			}
			if rows == 0 {
				b.Fatalf("legacy preview %s returned zero rows", q.name)
			}
			benchSink += int64(rows) + int64(digest)
			b.ReportAllocs()
			b.ResetTimer()
			var measuredRows int64
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = q.run(q.part, codes, scratch)
				if err != nil {
					b.Fatalf("legacy run %s: %v", q.name, err)
				}
				benchSink += int64(rows) + int64(digest)
				measuredRows += int64(jsonBenchCompareMeasuredRows(ds.Rows, diagnostics))
			}
			reportJSONBenchColumnStoreCompareMetrics(b, measuredRows, diagnostics.BytesDecoded, ds.Rows)
		})
	}
}

func benchmarkJSONBenchColumnStoreCompareTreeDB(b *testing.B, ds JSONBenchDataset) {
	col, closeFn, setup := openJSONBenchTreeDBColumnStoreCompareCollection(b, ds)
	defer closeFn()
	queries := []struct {
		name string
		req  collections.ColumnPhysicalQueryRequest
	}{
		{name: "q1_grouped_count", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupCount, GroupColumn: "collection"}},
		{name: "q2_group_count_distinct", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "collection", DistinctColumn: "did"}},
		{name: "q3_hourly_count", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}},
		{name: "q3_group_hour_count", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupHourCount, GroupColumn: "collection", ValueColumn: "time_us", Predicates: []collections.ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "collection", Kind: collections.ColumnPhysicalQueryPredicateInList, Values: []string{"app.bsky.feed.post"}},
		}}},
		{name: "q4_min_by_user", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"}},
		{name: "q5_span_by_user", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}},
		{name: "q4_metadata_min_by_user", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}},
		{name: "q4_metadata_top3_min_by_user", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: 3, TopKOrder: collections.ColumnPhysicalQueryTopKInt64Asc, SkipEmptyGroupKey: true}},
		{name: "q5_metadata_span_by_user", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us"}},
		{name: "q5_metadata_top3_span_by_user", req: collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us", AggregateMetadataName: "min_time_us", TopK: 3, TopKOrder: collections.ColumnPhysicalQueryTopKInt64Desc, SkipEmptyGroupKey: true}},
	}
	for _, q := range queries {
		q := q
		req := q.req
		req.ColumnAssetReadIntegrity = collections.ColumnAssetReadIntegrityCachedVerify
		previewDigest := verifyTreeDBColumnStoreComparePreviewDigest(b, col, q.name, req)
		b.Run("treedb_column_store_one_shot/"+q.name, func(b *testing.B) {
			reportTreeDBJSONBenchColumnStoreSetupMetrics(b, setup)
			preview, err := col.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("TreeDB preview %s: %v", q.name, err)
			}
			if digest := jsonBenchColumnPhysicalResultDigest(preview.Groups); digest != previewDigest {
				b.Fatalf("TreeDB preview %s digest=%s want %s", q.name, digest, previewDigest)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var reducedRows int64
			var last collections.ColumnPhysicalQueryDiagnostics
			for i := 0; i < b.N; i++ {
				result, err := col.RunColumnPhysicalQuery(req)
				if err != nil {
					b.Fatalf("TreeDB run %s: %v", q.name, err)
				}
				if len(result.Groups) == 0 {
					b.Fatalf("TreeDB run %s returned zero groups", q.name)
				}
				last = result.Diagnostics
				reducedRows += int64(result.Diagnostics.ReduceRows)
			}
			b.StopTimer()
			reportTreeDBJSONBenchColumnStoreCompareMetrics(b, reducedRows, last, ds.Rows)
		})
		b.Run("treedb_column_store_prepared/"+q.name, func(b *testing.B) {
			reportTreeDBJSONBenchColumnStoreSetupMetrics(b, setup)
			runner, err := col.PrepareColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("PrepareColumnPhysicalQuery %s: %v", q.name, err)
			}
			defer func() { _ = runner.Close() }()
			preview, err := runner.Run()
			if err != nil {
				b.Fatalf("TreeDB prepared preview %s: %v", q.name, err)
			}
			if digest := jsonBenchColumnPhysicalResultDigest(preview.Groups); digest != previewDigest {
				b.Fatalf("TreeDB prepared preview %s digest=%s want %s", q.name, digest, previewDigest)
			}
			b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
			b.ReportAllocs()
			b.ResetTimer()
			var reducedRows int64
			var last collections.ColumnPhysicalQueryDiagnostics
			for i := 0; i < b.N; i++ {
				result, err := runner.Run()
				if err != nil {
					b.Fatalf("TreeDB prepared run %s: %v", q.name, err)
				}
				if len(result.Groups) == 0 {
					b.Fatalf("TreeDB prepared run %s returned zero groups", q.name)
				}
				last = result.Diagnostics
				reducedRows += int64(result.Diagnostics.ReduceRows)
			}
			b.StopTimer()
			reportTreeDBJSONBenchColumnStoreCompareMetrics(b, reducedRows, last, ds.Rows)
		})
	}
}

func verifyTreeDBColumnStoreComparePreviewDigest(b *testing.B, col *collections.Collection, name string, req collections.ColumnPhysicalQueryRequest) string {
	b.Helper()
	oneShot, err := col.RunColumnPhysicalQuery(req)
	if err != nil {
		b.Fatalf("TreeDB one-shot preview %s: %v", name, err)
	}
	oneShotDigest := jsonBenchColumnPhysicalResultDigest(oneShot.Groups)
	if len(oneShot.Groups) == 0 {
		b.Fatalf("TreeDB one-shot preview %s returned zero groups digest=%s", name, oneShotDigest)
	}
	runner, err := col.PrepareColumnPhysicalQuery(req)
	if err != nil {
		b.Fatalf("PrepareColumnPhysicalQuery preview %s: %v", name, err)
	}
	defer func() { _ = runner.Close() }()
	prepared, err := runner.Run()
	if err != nil {
		b.Fatalf("TreeDB prepared preview %s: %v", name, err)
	}
	preparedDigest := jsonBenchColumnPhysicalResultDigest(prepared.Groups)
	if len(prepared.Groups) == 0 {
		b.Fatalf("TreeDB prepared preview %s returned zero groups digest=%s", name, preparedDigest)
	}
	if oneShotDigest != preparedDigest {
		b.Fatalf("TreeDB preview digest mismatch %s: one-shot=%s prepared=%s", name, oneShotDigest, preparedDigest)
	}
	return oneShotDigest
}

func jsonBenchColumnPhysicalResultDigest(groups []collections.ColumnPhysicalQueryGroup) string {
	ordered := append([]collections.ColumnPhysicalQueryGroup(nil), groups...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Key != ordered[j].Key {
			return ordered[i].Key < ordered[j].Key
		}
		if ordered[i].Hour != ordered[j].Hour {
			return ordered[i].Hour < ordered[j].Hour
		}
		if ordered[i].Count != ordered[j].Count {
			return ordered[i].Count < ordered[j].Count
		}
		return ordered[i].Int64 < ordered[j].Int64
	})
	h := sha256.New()
	for _, group := range ordered {
		_, _ = fmt.Fprintf(h, "%s\x00%d\x00%d\x00%d\x00", group.Key, group.Hour, group.Count, group.Int64)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type jsonBenchTreeDBColumnStoreSetup struct {
	setupNanos       int64
	dbDirBytes       int64
	columnAssetBytes int64
}

func openJSONBenchTreeDBColumnStoreCompareCollection(b *testing.B, ds JSONBenchDataset) (*collections.Collection, func(), jsonBenchTreeDBColumnStoreSetup) {
	b.Helper()
	dir := b.TempDir()
	start := time.Now()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		b.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatalf("Open setup DB: %v", err)
	}
	mgr := collections.NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "jsonbench", Options: collections.CollectionOptions{ColumnStore: jsonBenchTreeDBColumnStoreCompareConfig()}}); err != nil {
		_ = d.Close()
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("jsonbench")
	if err != nil {
		_ = d.Close()
		b.Fatalf("OpenCollection setup: %v", err)
	}
	insertJSONBenchTreeDBColumnStoreRows(b, col, ds)
	setupDuration := time.Since(start)
	setup := jsonBenchTreeDBColumnStoreSetup{
		setupNanos:       setupDuration.Nanoseconds(),
		dbDirBytes:       jsonBenchCompareDirSize(d.Dir()),
		columnAssetBytes: jsonBenchCompareDirSize(d.ColumnAssetRootDir()),
	}
	if err := d.Close(); err != nil {
		b.Fatalf("Close setup DB: %v", err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatalf("Open reopened DB: %v", err)
	}
	reopened, err := collections.NewCollectionManager(reopen).OpenCollection("jsonbench")
	if err != nil {
		_ = reopen.Close()
		b.Fatalf("OpenCollection reopened: %v", err)
	}
	return reopened, func() { _ = reopen.Close() }, setup
}

func jsonBenchTreeDBColumnStoreCompareConfig() *collections.ColumnStoreConfig {
	return &collections.ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
		Columns: []collections.ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "operation", Path: "operation", ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "collection", Path: "collection", ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "did", Path: "did", ValueType: collections.ColumnStoreValueString, Dictionary: true},
		},
		SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
		AggregateMetadata: []collections.ColumnAggregateMetadata{
			{Name: "min_time_us", Column: "time_us", GroupColumn: "did", Kind: collections.ColumnAggregateMin},
			{Name: "max_time_us", Column: "time_us", GroupColumn: "did", Kind: collections.ColumnAggregateMax},
		},
	}
}

func insertJSONBenchTreeDBColumnStoreRows(b *testing.B, col *collections.Collection, ds JSONBenchDataset) {
	b.Helper()
	timeValues := ds.Columns["time_us"]
	didCodes := ds.Columns["did_code"]
	if len(timeValues) != ds.Rows || len(didCodes) != ds.Rows {
		b.Fatalf("invalid JSONBench compare dataset rows=%d time=%d did=%d", ds.Rows, len(timeValues), len(didCodes))
	}
	for start := 0; start < ds.Rows; start += defaultJSONBenchColumnStoreCompareBatchRows {
		end := start + defaultJSONBenchColumnStoreCompareBatchRows
		if end > ds.Rows {
			end = ds.Rows
		}
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i := start; i < end; i++ {
			local := i - start
			ids[local] = []byte(fmt.Sprintf("jb%012d", i))
			docs[local] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"commit","operation":"create","collection":"app.bsky.feed.post","did":"%d"}`, timeValues[i], didCodes[i]))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			b.Fatalf("InsertBatch rows %d..%d: %v", start, end, err)
		}
	}
}

func syntheticJSONBenchPostOnlyCompareDataset(rows int) JSONBenchDataset {
	ds := syntheticJSONBenchDataset(rows)
	collection := ds.Columns["commit_collection_code"]
	for i := range collection {
		collection[i] = 1
	}
	ds.Dictionaries["commit_collection_code"] = map[string]int64{"app.bsky.feed.post": 1}
	return ds
}

func jsonBenchCompareMeasuredRows(totalRows int, diagnostics JSONBenchPartQueryDiagnostics) int {
	if diagnostics.RowsScanned != 0 {
		return diagnostics.RowsScanned
	}
	if diagnostics.AggregateMetadataEntries != 0 {
		return diagnostics.AggregateMetadataEntries
	}
	return totalRows
}

func reportJSONBenchColumnStoreCompareMetrics(b *testing.B, measuredRows int64, decodedBytes int, datasetRows int) {
	b.Helper()
	elapsed := b.Elapsed()
	if elapsed > 0 && measuredRows > 0 {
		b.ReportMetric(float64(measuredRows)/elapsed.Seconds(), "rows/s")
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(measuredRows), "ns/row")
	}
	if elapsed > 0 && datasetRows > 0 && b.N > 0 {
		b.ReportMetric(float64(datasetRows*b.N)/elapsed.Seconds(), "dataset_rows/s")
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(datasetRows*b.N), "dataset_ns/row")
	}
	b.ReportMetric(float64(decodedBytes), "decoded_bytes/op")
}

func reportTreeDBJSONBenchColumnStoreSetupMetrics(b *testing.B, setup jsonBenchTreeDBColumnStoreSetup) {
	b.Helper()
	b.ReportMetric(float64(setup.setupNanos), "setup_ns")
	b.ReportMetric(float64(setup.dbDirBytes), "db_dir_bytes")
	b.ReportMetric(float64(setup.columnAssetBytes), "column_asset_bytes")
}

func reportTreeDBJSONBenchColumnStoreCompareMetrics(b *testing.B, reducedRows int64, diag collections.ColumnPhysicalQueryDiagnostics, datasetRows int) {
	b.Helper()
	elapsed := b.Elapsed()
	if elapsed > 0 && reducedRows > 0 {
		b.ReportMetric(float64(reducedRows)/elapsed.Seconds(), "rows/s")
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(reducedRows), "ns/row")
	}
	if elapsed > 0 && datasetRows > 0 && b.N > 0 {
		b.ReportMetric(float64(datasetRows*b.N)/elapsed.Seconds(), "dataset_rows/s")
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(datasetRows*b.N), "dataset_ns/row")
	}
	b.ReportMetric(float64(diag.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(diag.ReduceRows), "reduce_rows/op")
	b.ReportMetric(float64(diag.ResultGroups), "result_groups/op")
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.HeapCopyBytes), "heap_copy_bytes/op")
	b.ReportMetric(float64(diag.DecodedMetadataBytes), "decoded_metadata_bytes/op")
	b.ReportMetric(float64(diag.RowMaterializations), "row_materializations/op")
	b.ReportMetric(float64(diag.DocumentMaterializations), "document_materializations/op")
	b.ReportMetric(float64(diag.TopKLimit), "topk_limit/op")
	b.ReportMetric(float64(diag.TopKCandidates), "topk_candidates/op")
	b.ReportMetric(float64(diag.ResultShapeNanos), "result_shape_ns/op")
	if diag.StorageSource != "" {
		b.ReportMetric(1, "storage_source_"+string(diag.StorageSource))
	}
	if diag.FallbackReason != "" {
		b.ReportMetric(1, "fallback_"+string(diag.FallbackReason))
	}
	reportTreeDBJSONBenchUint64Metric(b, diag.ManifestRoot, "manifest_root_id")
	reportTreeDBJSONBenchUint64Metric(b, diag.ManifestGeneration, "manifest_generation")
	reportTreeDBJSONBenchUint64Metric(b, diag.ActiveManifestChecksum, "active_manifest_checksum")
}

func reportTreeDBJSONBenchUint64Metric(b *testing.B, value uint64, name string) {
	b.Helper()
	b.ReportMetric(float64(uint32(value>>32)), name+"_hi")
	b.ReportMetric(float64(uint32(value)), name+"_lo")
}

func jsonBenchCompareDirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}
