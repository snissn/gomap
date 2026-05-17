package colgranule

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

func TestColumnPartSetDeltaTombstoneVisibilityAndCompaction(t *testing.T) {
	ds := syntheticJSONBenchDataset(192)
	opts, err := JSONBenchColumnPartOptions(ds, 24)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()
	baseEntry := publishJSONBenchPartRows(t, workspace, opts, ds, 301, 0, ds.Rows)

	updates := map[int64]map[string]int64{
		5: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][40],
			"time_us":                ds.Columns["time_us"][5] - 10_000_000,
		},
		25: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][41],
			"time_us":                ds.Columns["time_us"][25] + 20_000_000,
		},
	}
	for _, update := range updates {
		update["hour_of_day"] = unixMicroHour(update["time_us"])
	}
	deletes := map[int64]bool{10: true, 11: true}
	deltaBatch := jsonBenchDeltaBatch(ds, updates)
	deltaPart, err := BuildColumnDeltaPart(302, opts, deltaBatch)
	if err != nil {
		t.Fatalf("BuildColumnDeltaPart: %v", err)
	}
	deltaEntry, err := workspace.PublishPart(deltaPart, ds.Dictionaries)
	if err != nil {
		t.Fatalf("PublishPart(delta): %v", err)
	}
	tombstones := []ColumnTombstone{
		{PrimaryID: 10, GenerationID: 3, Reason: "delete"},
		{PrimaryID: 11, GenerationID: 3, Reason: "delete"},
	}
	manifest, err := NewColumnCollectionManifest(
		"jsonbench",
		opts,
		[]ColumnManifestPartRef{NewColumnManifestPartRef(ColumnPartRoleBase, 1, baseEntry)},
		[]ColumnManifestPartRef{NewColumnManifestPartRef(ColumnPartRoleDelta, 2, deltaEntry)},
		tombstones,
	)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	if err := workspace.SaveCollectionManifest(manifest); err != nil {
		t.Fatalf("SaveCollectionManifest: %v", err)
	}
	reader, err := OpenColumnPartSetReader(workspace, manifest, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("OpenColumnPartSetReader: %v", err)
	}
	expected := applyJSONBenchMutations(ds, updates, deletes)
	if stats := reader.VisibilityStats(); stats.VisibleRows != expected.Rows || stats.SupersededRows != len(updates) || stats.DeletedRows != len(deletes) {
		t.Fatalf("visibility stats=%+v want visible=%d superseded=%d deleted=%d", stats, expected.Rows, len(updates), len(deletes))
	}
	locator, ok := reader.LatestLocator(5)
	if !ok {
		t.Fatal("missing latest locator for updated id 5")
	}
	if locator.PartID != deltaEntry.PartID {
		t.Fatalf("updated id 5 locator part=%d want delta part=%d", locator.PartID, deltaEntry.PartID)
	}
	if _, ok := reader.LatestLocator(10); ok {
		t.Fatal("deleted id 10 still has latest locator")
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, reader, expected)
	plan, err := PlanColumnPartSetCompaction(manifest, reader.VisibilityStats(), ColumnPartSetCompactionPolicy{
		MaxDeltaParts:             1,
		MaxTombstones:             1,
		MaxReadAmplificationParts: 2,
		MaxStaleBytes:             1,
		MinExpectedReclaimPPM:     1,
		MinVisibleRowsPPM:         990_000,
	})
	if err != nil {
		t.Fatalf("PlanColumnPartSetCompaction: %v", err)
	}
	for _, reason := range []string{"delta_part_count", "tombstone_count", "read_amplification", "column_asset_stale_bytes", "expected_reclaim_ratio", "sparse_visible_rows", "aggregate_metadata_invalidation"} {
		if !containsString(plan.Reasons, reason) {
			t.Fatalf("compaction reasons=%v missing %s", plan.Reasons, reason)
		}
	}
	if !plan.ShouldCompact || plan.SelectedParts != 2 || plan.SkippedParts != 0 || plan.StaleBytes == 0 || plan.LiveBytes == 0 {
		t.Fatalf("bad compaction plan=%+v", plan)
	}

	compacted, err := CompactColumnPartSet(workspace, reader, opts, ds.Dictionaries, 399)
	if err != nil {
		t.Fatalf("CompactColumnPartSet: %v", err)
	}
	if compacted.VisibleRows != expected.Rows || compacted.DroppedRows != len(updates)+len(deletes) {
		t.Fatalf("compaction rows visible=%d dropped=%d want visible=%d dropped=%d", compacted.VisibleRows, compacted.DroppedRows, expected.Rows, len(updates)+len(deletes))
	}
	if compacted.OldAssetBytes != manifest.ByteAccounting.TotalAssetBytes || compacted.NewAssetBytes == 0 {
		t.Fatalf("compaction bytes=%+v manifest bytes=%d", compacted, manifest.ByteAccounting.TotalAssetBytes)
	}
	if compacted.ReclaimableBytes != 0 || compacted.RewriteDebtBytes != manifest.ByteAccounting.TotalAssetBytes {
		t.Fatalf("compaction reachability reclaimable/rewrite=(%d,%d) want (0,%d)", compacted.ReclaimableBytes, compacted.RewriteDebtBytes, manifest.ByteAccounting.TotalAssetBytes)
	}
	if compacted.PrePublishReachability.ReclaimableBytes != 0 || compacted.PrePublishReachability.RewriteDebtBytes != 0 {
		t.Fatalf("pre-publish reachability should not expose old bytes to GC: %+v", compacted.PrePublishReachability)
	}
	if !compacted.SelectionPlan.ShouldCompact || compacted.SelectionPlan.SelectedParts != 2 {
		t.Fatalf("bad compacted selection plan=%+v", compacted.SelectionPlan)
	}
	if compacted.PostPublishReachability.Stats.MixedLiveDeadSegments != 1 {
		t.Fatalf("post-publish reachability mixed segments=%d want 1", compacted.PostPublishReachability.Stats.MixedLiveDeadSegments)
	}
	if len(compacted.Manifest.PartSet.BaseParts) != 1 || len(compacted.Manifest.PartSet.DeltaParts) != 0 || len(compacted.Manifest.PartSet.Tombstones) != 0 {
		t.Fatalf("bad compacted manifest part set=%+v", compacted.Manifest.PartSet)
	}
	coverage := compacted.Manifest.PartSet.BaseParts[0].Coverage
	if coverage.CompactionLevel != 1 || len(coverage.SourceParts) != 2 || coverage.SourceParts[0].PartID != baseEntry.PartID || coverage.SourceParts[1].PartID != deltaEntry.PartID {
		t.Fatalf("bad compacted coverage=%+v base=%d delta=%d", coverage, baseEntry.PartID, deltaEntry.PartID)
	}
	if err := workspace.SaveCollectionManifest(compacted.Manifest); err != nil {
		t.Fatalf("SaveCollectionManifest(compacted): %v", err)
	}
	reopenedManifest, err := workspace.LoadCollectionManifest()
	if err != nil {
		t.Fatalf("LoadCollectionManifest(compacted): %v", err)
	}
	compactedReader, err := OpenColumnPartSetReader(workspace, reopenedManifest, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("OpenColumnPartSetReader(compacted): %v", err)
	}
	if stats := compactedReader.VisibilityStats(); stats.VisibleRows != expected.Rows || stats.SupersededRows != 0 || stats.DeletedRows != 0 {
		t.Fatalf("compacted visibility stats=%+v want visible=%d no hidden rows", stats, expected.Rows)
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, compactedReader, expected)
}

func TestColumnPartSetQ4FairnessKernelsMatchRaw(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rawRows, rawDigest := runJSONBenchQ4(ds, codes)
	timeOpts, err := JSONBenchColumnPartOptionsForLayout(ds, 32, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsForLayout(time): %v", err)
	}
	clickHouseOpts, err := JSONBenchColumnPartOptionsForLayout(ds, 32, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsForLayout(clickhouse): %v", err)
	}
	timeWorkspace, timeReader := testColumnPartSetReader(t, ds, timeOpts, ColumnPartImageReadOptions{})
	defer timeWorkspace.Close()
	clickHouseWorkspace, clickHouseReader := testColumnPartSetReader(t, ds, clickHouseOpts, ColumnPartImageReadOptions{})
	defer clickHouseWorkspace.Close()

	q4aRows, q4aDigest, q4aDiagnostics, err := runJSONBenchPartSetQ4TimeOrdered(timeReader, codes, &jsonBenchPartQueryScratch{})
	if err != nil {
		t.Fatalf("runJSONBenchPartSetQ4TimeOrdered: %v", err)
	}
	if q4aRows != rawRows || q4aDigest != rawDigest {
		t.Fatalf("q4a rows/digest=(%d,%d) raw=(%d,%d)", q4aRows, q4aDigest, rawRows, rawDigest)
	}
	if q4aDiagnostics.AggregateKernel != "multipart_sort_key_early_stop_min_by_user" || !q4aDiagnostics.EarlyStopAvailable {
		t.Fatalf("q4a diagnostics=%+v want multipart early-stop", q4aDiagnostics)
	}
	assertStringSliceEqual(t, q4aDiagnostics.SortKey, []string{"time_us"})

	q4bRows, q4bDigest, q4bDiagnostics, err := runJSONBenchPartSetQ4ClickHouseOrder(clickHouseReader, codes, &jsonBenchPartQueryScratch{})
	if err != nil {
		t.Fatalf("runJSONBenchPartSetQ4ClickHouseOrder: %v", err)
	}
	if q4bRows != rawRows || q4bDigest != rawDigest {
		t.Fatalf("q4b rows/digest=(%d,%d) raw=(%d,%d)", q4bRows, q4bDigest, rawRows, rawDigest)
	}
	if q4bDiagnostics.AggregateKernel != "multipart_clickhouse_order_prefix_scan_min_by_user" || q4bDiagnostics.EarlyStopAvailable {
		t.Fatalf("q4b diagnostics=%+v want ClickHouse-order prefix scan only", q4bDiagnostics)
	}
	if q4bDiagnostics.GranulesSkipped == 0 {
		t.Fatalf("q4b diagnostics skipped no granules: %+v", q4bDiagnostics)
	}
	assertStringSliceEqual(t, q4bDiagnostics.SortKey, []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"})
}

func TestColumnPartSetAggregateMetadataKernelsMatchRaw(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rawQ4Rows, rawQ4Digest := runJSONBenchQ4(ds, codes)
	rawQ5Rows, rawQ5Digest := runJSONBenchQ5(ds, codes)
	timeOpts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, 32, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(time): %v", err)
	}
	clickHouseOpts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, 32, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(clickhouse): %v", err)
	}
	readOpts := ColumnPartImageReadOptions{IncludeAggregateMetadata: true}
	timeWorkspace, timeReader := testColumnPartSetReader(t, ds, timeOpts, readOpts)
	defer timeWorkspace.Close()
	clickHouseWorkspace, clickHouseReader := testColumnPartSetReader(t, ds, clickHouseOpts, readOpts)
	defer clickHouseWorkspace.Close()

	q4Rows, q4Digest, q4Diagnostics, err := runJSONBenchPartSetQ4AggregateMetadata(clickHouseReader, codes, &jsonBenchPartQueryScratch{})
	if err != nil {
		t.Fatalf("runJSONBenchPartSetQ4AggregateMetadata: %v", err)
	}
	if q4Rows != rawQ4Rows || q4Digest != rawQ4Digest {
		t.Fatalf("q4 metadata rows/digest=(%d,%d) raw=(%d,%d)", q4Rows, q4Digest, rawQ4Rows, rawQ4Digest)
	}
	if !q4Diagnostics.AggregateMetadataUsed || q4Diagnostics.RowsScanned != 0 || q4Diagnostics.BlocksDecoded != 0 {
		t.Fatalf("q4 metadata diagnostics=%+v want metadata-only", q4Diagnostics)
	}
	assertStringSliceEqual(t, q4Diagnostics.SortKey, []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"})

	q5Rows, q5Digest, q5Diagnostics, err := runJSONBenchPartSetQ5AggregateMetadata(timeReader, codes, &jsonBenchPartQueryScratch{})
	if err != nil {
		t.Fatalf("runJSONBenchPartSetQ5AggregateMetadata: %v", err)
	}
	if q5Rows != rawQ5Rows || q5Digest != rawQ5Digest {
		t.Fatalf("q5 metadata rows/digest=(%d,%d) raw=(%d,%d)", q5Rows, q5Digest, rawQ5Rows, rawQ5Digest)
	}
	if !q5Diagnostics.AggregateMetadataUsed || q5Diagnostics.RowsScanned != 0 || q5Diagnostics.BlocksDecoded != 0 {
		t.Fatalf("q5 metadata diagnostics=%+v want metadata-only", q5Diagnostics)
	}
	assertStringSliceEqual(t, q5Diagnostics.SortKey, []string{"time_us"})
}

func jsonBenchDeltaBatch(ds JSONBenchDataset, updates map[int64]map[string]int64) ColumnBatch {
	ids := make([]int64, 0, len(updates))
	for id := range updates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	columns := make(map[string][]int64, len(ds.Columns))
	for _, id := range ids {
		row := findJSONBenchPrimaryRow(ds, id)
		if row < 0 {
			panic("missing update row")
		}
		update := updates[id]
		for name, values := range ds.Columns {
			value := values[row]
			if updated, ok := update[name]; ok {
				value = updated
			}
			columns[name] = append(columns[name], value)
		}
	}
	return ColumnBatch{Rows: len(ids), Columns: columns}
}

func testColumnPartSetReader(t *testing.T, ds JSONBenchDataset, opts ColumnStoreOptions, readOpts ColumnPartImageReadOptions) (*ColumnWorkspace, *ColumnPartSetReader) {
	t.Helper()
	workspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry1 := publishJSONBenchPartRows(t, workspace, opts, ds, 701, 0, ds.Rows/2)
	entry2 := publishJSONBenchPartRows(t, workspace, opts, ds, 702, ds.Rows/2, ds.Rows)
	manifest, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{
		NewColumnManifestPartRef(ColumnPartRoleBase, 1, entry1),
		NewColumnManifestPartRef(ColumnPartRoleBase, 2, entry2),
	}, nil, nil)
	if err != nil {
		_ = workspace.Close()
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	reader, err := OpenColumnPartSetReader(workspace, manifest, readOpts)
	if err != nil {
		_ = workspace.Close()
		t.Fatalf("OpenColumnPartSetReader: %v", err)
	}
	return workspace, reader
}

func applyJSONBenchMutations(ds JSONBenchDataset, updates map[int64]map[string]int64, deletes map[int64]bool) JSONBenchDataset {
	out := JSONBenchDataset{
		Files:        append([]string(nil), ds.Files...),
		Columns:      make(map[string][]int64, len(ds.Columns)),
		Dictionaries: ds.Dictionaries,
	}
	for row := 0; row < ds.Rows; row++ {
		primaryID := ds.Columns["row_index"][row]
		if deletes[primaryID] {
			continue
		}
		update := updates[primaryID]
		for name, values := range ds.Columns {
			value := values[row]
			if updated, ok := update[name]; ok {
				value = updated
			}
			out.Columns[name] = append(out.Columns[name], value)
		}
		out.Rows++
	}
	return out
}

func findJSONBenchPrimaryRow(ds JSONBenchDataset, primaryID int64) int {
	for row, id := range ds.Columns["row_index"] {
		if id == primaryID {
			return row
		}
	}
	return -1
}

func BenchmarkJSONBenchColumnPartSetQueriesM4(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	opts, err := JSONBenchColumnPartOptions(ds, DefaultRowsPerGranule)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	reader, cleanup := benchmarkColumnPartSetReader(b, ds, opts)
	defer cleanup()
	b.ReportAllocs()
	b.SetBytes(int64(reader.VisibilityStats().VisibleRows))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timings, err := RunJSONBenchColumnPartSetQueries(reader, ds, 1)
		if err != nil {
			b.Fatal(err)
		}
		var digest uint64
		for _, timing := range timings {
			digest ^= timing.ResultDigest
		}
		benchSink += int64(digest)
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(reader.VisibilityStats().VisibleRows*b.N)/seconds, "rows/s")
	}
}

func BenchmarkJSONBenchColumnPartSetQueriesM4PerQuery(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	benchmarkJSONBenchColumnPartSetQueriesM4PerQuery(b, ds)
}

func BenchmarkJSONBenchLocalColumnPartSetQueriesM4PerQuery(b *testing.B) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		path = DefaultJSONBenchDir
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("JSONBench data not present; set JSONBENCH_DATA or install %s", DefaultJSONBenchDir)
	}
	ds, err := LoadJSONBenchColumns(path, 1_000_000)
	if err != nil {
		b.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	if ds.Rows < 1_000_000 {
		b.Skipf("local JSONBench fixture has rows=%d; set JSONBENCH_DATA to a 1M-row fixture for the local part-set gate", ds.Rows)
	}
	benchmarkJSONBenchColumnPartSetQueriesM4PerQuery(b, ds)
}

func BenchmarkJSONBenchColumnPartSetQ4FairnessM4(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	benchmarkJSONBenchColumnPartSetQ4FairnessM4(b, ds)
}

func BenchmarkJSONBenchLocalColumnPartSetQ4FairnessM4(b *testing.B) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		path = DefaultJSONBenchDir
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("JSONBench data not present; set JSONBENCH_DATA or install %s", DefaultJSONBenchDir)
	}
	ds, err := LoadJSONBenchColumns(path, 1_000_000)
	if err != nil {
		b.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	if ds.Rows < 1_000_000 {
		b.Skipf("local JSONBench fixture has rows=%d; set JSONBENCH_DATA to a 1M-row fixture for the local q4 fairness gate", ds.Rows)
	}
	benchmarkJSONBenchColumnPartSetQ4FairnessM4(b, ds)
}

func BenchmarkJSONBenchColumnPartSetAggregateMetadataM4(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	benchmarkJSONBenchColumnPartSetAggregateMetadataM4(b, ds)
}

func BenchmarkJSONBenchLocalColumnPartSetAggregateMetadataM4(b *testing.B) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		path = DefaultJSONBenchDir
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("JSONBench data not present; set JSONBENCH_DATA or install %s", DefaultJSONBenchDir)
	}
	ds, err := LoadJSONBenchColumns(path, 1_000_000)
	if err != nil {
		b.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	if ds.Rows < 1_000_000 {
		b.Skipf("local JSONBench fixture has rows=%d; set JSONBENCH_DATA to a 1M-row fixture for the local metadata gate", ds.Rows)
	}
	benchmarkJSONBenchColumnPartSetAggregateMetadataM4(b, ds)
}

func benchmarkJSONBenchColumnPartSetQueriesM4PerQuery(b *testing.B, ds JSONBenchDataset) {
	opts, err := JSONBenchColumnPartOptions(ds, DefaultRowsPerGranule)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	reader, cleanup := benchmarkColumnPartSetReader(b, ds, opts)
	defer cleanup()
	queries := []struct {
		name    string
		columns int
		run     func(*ColumnPartSetReader, queryCodeSet, *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error)
	}{
		{"Q1_grouped_count", 1, runJSONBenchPartSetQ1},
		{"Q2_group_count_distinct", len(jsonBenchPartQ2Columns), runJSONBenchPartSetQ2},
		{"Q3_hourly_grouped_count", len(jsonBenchPartQ3Columns), runJSONBenchPartSetQ3},
		{"Q4_min_by_user", len(jsonBenchPartQ4Columns), runJSONBenchPartSetQ4},
		{"Q5_span_by_user", len(jsonBenchPartQ5Columns), runJSONBenchPartSetQ5},
	}
	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{}
			rows, digest, diagnostics, err := query.run(reader, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			valueBytes := diagnostics.RowsScanned * query.columns * 8
			if valueBytes == 0 {
				valueBytes = reader.VisibilityStats().VisibleRows * query.columns * 8
			}
			b.SetBytes(int64(valueBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = query.run(reader, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			reportGranuleBenchMetrics(b, diagnostics.RowsScanned, valueBytes, diagnostics.BytesDecoded)
		})
	}
}

func benchmarkJSONBenchColumnPartSetQ4FairnessM4(b *testing.B, ds JSONBenchDataset) {
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	timeOpts, err := JSONBenchColumnPartOptionsForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptionsForLayout(time): %v", err)
	}
	clickHouseOpts, err := JSONBenchColumnPartOptionsForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptionsForLayout(clickhouse): %v", err)
	}
	timeReader, timeCleanup := benchmarkColumnPartSetReader(b, ds, timeOpts)
	defer timeCleanup()
	clickHouseReader, clickHouseCleanup := benchmarkColumnPartSetReader(b, ds, clickHouseOpts)
	defer clickHouseCleanup()
	queries := []struct {
		name    string
		columns int
		reader  *ColumnPartSetReader
		run     func(*ColumnPartSetReader, queryCodeSet, *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error)
	}{
		{"Q4a_time_ordered", len(jsonBenchPartQ4Columns), timeReader, runJSONBenchPartSetQ4TimeOrdered},
		{"Q4b_clickhouse_order", len(jsonBenchPartQ4Columns), clickHouseReader, runJSONBenchPartSetQ4ClickHouseOrder},
	}
	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{}
			rows, digest, diagnostics, err := query.run(query.reader, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			valueBytes := diagnostics.RowsScanned * query.columns * 8
			if valueBytes == 0 {
				valueBytes = query.reader.VisibilityStats().VisibleRows * query.columns * 8
			}
			b.SetBytes(int64(valueBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = query.run(query.reader, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			reportGranuleBenchMetrics(b, diagnostics.RowsScanned, valueBytes, diagnostics.BytesDecoded)
		})
	}
}

func benchmarkJSONBenchColumnPartSetAggregateMetadataM4(b *testing.B, ds JSONBenchDataset) {
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	timeOpts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(time): %v", err)
	}
	clickHouseOpts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(clickhouse): %v", err)
	}
	readOpts := ColumnPartImageReadOptions{IncludeAggregateMetadata: true}
	timeReader, timeCleanup := benchmarkColumnPartSetReaderWithReadOptions(b, ds, timeOpts, readOpts)
	defer timeCleanup()
	clickHouseReader, clickHouseCleanup := benchmarkColumnPartSetReaderWithReadOptions(b, ds, clickHouseOpts, readOpts)
	defer clickHouseCleanup()
	queries := []struct {
		name    string
		columns int
		reader  *ColumnPartSetReader
		run     func(*ColumnPartSetReader, queryCodeSet, *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error)
	}{
		{"Q4b_metadata_min_by_user", len(jsonBenchPartQ4Columns), clickHouseReader, runJSONBenchPartSetQ4AggregateMetadata},
		{"Q5_metadata_span_by_user", len(jsonBenchPartQ5Columns), timeReader, runJSONBenchPartSetQ5AggregateMetadata},
		{"Q4b_row_scan_min_by_user", len(jsonBenchPartQ4Columns), clickHouseReader, runJSONBenchPartSetQ4ClickHouseOrder},
		{"Q5_row_scan_span_by_user", len(jsonBenchPartQ5Columns), timeReader, runJSONBenchPartSetQ5},
	}
	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{}
			rows, digest, diagnostics, err := query.run(query.reader, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			rowsMeasured := diagnostics.RowsScanned
			if rowsMeasured == 0 {
				rowsMeasured = diagnostics.AggregateMetadataEntries
			}
			if rowsMeasured == 0 {
				rowsMeasured = query.reader.VisibilityStats().VisibleRows
			}
			valueBytes := rowsMeasured * query.columns * 8
			b.SetBytes(int64(valueBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = query.run(query.reader, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			storedBytes := diagnostics.BytesDecoded
			if diagnostics.RowsScanned == 0 {
				storedBytes = diagnostics.AggregateMetadataBytes
			}
			reportGranuleBenchMetrics(b, rowsMeasured, valueBytes, storedBytes)
		})
	}
}

func BenchmarkColumnPartSetCompactionPlan10K(b *testing.B) {
	manifest := syntheticColumnCollectionManifestForBenchmark(10_000)
	manifest.PartSet.Tombstones = make([]ColumnTombstone, 1000)
	for i := range manifest.PartSet.Tombstones {
		manifest.PartSet.Tombstones[i] = ColumnTombstone{
			PrimaryID:    int64(i + 1),
			GenerationID: manifest.ActiveGeneration,
			Reason:       "benchmark delete",
		}
	}
	manifest.ByteAccounting = columnManifestByteAccounting(manifest)
	stats := ColumnPartSetVisibilityStats{
		Parts:          len(manifest.PartSet.BaseParts),
		BaseParts:      len(manifest.PartSet.BaseParts),
		InputRows:      manifest.ByteAccounting.Rows,
		VisibleRows:    manifest.ByteAccounting.Rows - 2000,
		SupersededRows: 1000,
		DeletedRows:    1000,
		Tombstones:     len(manifest.PartSet.Tombstones),
	}
	policy := ColumnPartSetCompactionPolicy{
		MaxTombstones:             1000,
		MaxReadAmplificationParts: 1024,
		MinExpectedReclaimPPM:     100_000,
		MinVisibleRowsPPM:         900_000,
	}
	plan, err := PlanColumnPartSetCompaction(manifest, stats, policy)
	if err != nil {
		b.Fatalf("PlanColumnPartSetCompaction: %v", err)
	}
	b.ReportMetric(float64(len(manifest.PartSet.BaseParts)), "parts")
	b.ReportMetric(float64(plan.StaleBytes), "stale_bytes")
	b.ReportMetric(float64(plan.ExpectedReclaimPPM), "expected_reclaim_ppm")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := PlanColumnPartSetCompaction(manifest, stats, policy)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(plan.SelectedParts + plan.StaleBytes)
	}
}

func BenchmarkColumnPartSetCompactionM5(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	opts, err := JSONBenchColumnPartOptions(ds, DefaultRowsPerGranule)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	updates := map[int64]map[string]int64{
		5: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][40],
			"time_us":                ds.Columns["time_us"][5] - 10_000_000,
		},
		25: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][41],
			"time_us":                ds.Columns["time_us"][25] + 20_000_000,
		},
	}
	for _, update := range updates {
		update["hour_of_day"] = unixMicroHour(update["time_us"])
	}
	deletes := map[int64]bool{10: true, 11: true}
	rootDir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		workspace, reader := benchmarkMutableColumnPartSetReader(b, filepath.Join(rootDir, strconv.Itoa(i)), ds, opts, updates, deletes)
		b.StartTimer()
		result, err := CompactColumnPartSet(workspace, reader, opts, ds.Dictionaries, uint64(10_000+i))
		b.StopTimer()
		if err != nil {
			_ = workspace.Close()
			b.Fatalf("CompactColumnPartSet: %v", err)
		}
		benchSink += int64(result.NewAssetBytes)
		if err := workspace.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(ds.Rows*b.N)/seconds, "input_rows/s")
	}
}

func BenchmarkColumnPartSetCompactionM5Breakdown(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	opts, err := JSONBenchColumnPartOptions(ds, DefaultRowsPerGranule)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	updates, deletes := benchmarkJSONBenchMutations(b, ds)
	columnNames := make([]string, 0, len(opts.Columns))
	for _, def := range opts.Columns {
		columnNames = append(columnNames, def.Name)
	}

	b.Run("visible_scan_declared_columns", func(b *testing.B) {
		workspace, reader := benchmarkMutableColumnPartSetReader(b, b.TempDir(), ds, opts, updates, deletes)
		defer workspace.Close()
		projected := make(map[string][]int64, len(columnNames))
		b.ReportAllocs()
		b.SetBytes(int64(ds.Rows * len(columnNames) * 8))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			scan, err := reader.ScanProjectedInto(projected, columnNames)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(scan.Rows)
		}
		seconds := b.Elapsed().Seconds()
		if seconds > 0 {
			b.ReportMetric(float64(ds.Rows*b.N)/seconds, "input_rows/s")
		}
	})

	b.Run("build_compacted_part", func(b *testing.B) {
		workspace, reader := benchmarkMutableColumnPartSetReader(b, b.TempDir(), ds, opts, updates, deletes)
		defer workspace.Close()
		scan, err := reader.ScanProjected(columnNames)
		if err != nil {
			b.Fatalf("ScanProjected: %v", err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(scan.Rows * len(columnNames) * 8))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			part, err := BuildColumnPart(uint64(20_000+i), opts, ColumnBatch{Rows: scan.Rows, Columns: scan.Columns})
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(part.Descriptor.RowCount)
		}
		seconds := b.Elapsed().Seconds()
		if seconds > 0 {
			b.ReportMetric(float64(scan.Rows*b.N)/seconds, "visible_rows/s")
		}
	})

	b.Run("build_part_image", func(b *testing.B) {
		workspace, reader := benchmarkMutableColumnPartSetReader(b, b.TempDir(), ds, opts, updates, deletes)
		defer workspace.Close()
		scan, err := reader.ScanProjected(columnNames)
		if err != nil {
			b.Fatalf("ScanProjected: %v", err)
		}
		part, err := BuildColumnPart(30_000, opts, ColumnBatch{Rows: scan.Rows, Columns: scan.Columns})
		if err != nil {
			b.Fatalf("BuildColumnPart: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: ds.Dictionaries})
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(image.TotalBytes())
		}
	})

	b.Run("publish_compacted_part", func(b *testing.B) {
		workspace, reader := benchmarkMutableColumnPartSetReader(b, b.TempDir(), ds, opts, updates, deletes)
		defer workspace.Close()
		scan, err := reader.ScanProjected(columnNames)
		if err != nil {
			b.Fatalf("ScanProjected: %v", err)
		}
		part, err := BuildColumnPart(40_000, opts, ColumnBatch{Rows: scan.Rows, Columns: scan.Columns})
		if err != nil {
			b.Fatalf("BuildColumnPart: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			entry, err := workspace.PublishPart(part, ds.Dictionaries)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(entry.AssetBytes)
		}
	})
}

func benchmarkColumnPartSetReader(b *testing.B, ds JSONBenchDataset, opts ColumnStoreOptions) (*ColumnPartSetReader, func()) {
	return benchmarkColumnPartSetReaderWithReadOptions(b, ds, opts, ColumnPartImageReadOptions{})
}

func benchmarkColumnPartSetReaderWithReadOptions(b *testing.B, ds JSONBenchDataset, opts ColumnStoreOptions, readOpts ColumnPartImageReadOptions) (*ColumnPartSetReader, func()) {
	b.Helper()
	dir := b.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		b.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry1 := benchmarkPublishJSONBenchPartRows(b, workspace, opts, ds, 401, 0, ds.Rows/2)
	entry2 := benchmarkPublishJSONBenchPartRows(b, workspace, opts, ds, 402, ds.Rows/2, ds.Rows)
	manifest, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{
		NewColumnManifestPartRef(ColumnPartRoleBase, 1, entry1),
		NewColumnManifestPartRef(ColumnPartRoleBase, 2, entry2),
	}, nil, nil)
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	reader, err := OpenColumnPartSetReader(workspace, manifest, readOpts)
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("OpenColumnPartSetReader: %v", err)
	}
	return reader, func() {
		_ = workspace.Close()
	}
}

func benchmarkMutableColumnPartSetReader(b *testing.B, dir string, ds JSONBenchDataset, opts ColumnStoreOptions, updates map[int64]map[string]int64, deletes map[int64]bool) (*ColumnWorkspace, *ColumnPartSetReader) {
	b.Helper()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		b.Fatalf("OpenColumnWorkspace: %v", err)
	}
	baseEntry := benchmarkPublishJSONBenchPartRows(b, workspace, opts, ds, 501, 0, ds.Rows)
	deltaBatch := jsonBenchDeltaBatch(ds, updates)
	deltaPart, err := BuildColumnDeltaPart(502, opts, deltaBatch)
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("BuildColumnDeltaPart: %v", err)
	}
	deltaEntry, err := workspace.PublishPart(deltaPart, ds.Dictionaries)
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("PublishPart(delta): %v", err)
	}
	tombstones := make([]ColumnTombstone, 0, len(deletes))
	for id := range deletes {
		tombstones = append(tombstones, ColumnTombstone{PrimaryID: id, GenerationID: 3, Reason: "delete"})
	}
	sort.Slice(tombstones, func(i, j int) bool { return tombstones[i].PrimaryID < tombstones[j].PrimaryID })
	manifest, err := NewColumnCollectionManifest(
		"jsonbench",
		opts,
		[]ColumnManifestPartRef{NewColumnManifestPartRef(ColumnPartRoleBase, 1, baseEntry)},
		[]ColumnManifestPartRef{NewColumnManifestPartRef(ColumnPartRoleDelta, 2, deltaEntry)},
		tombstones,
	)
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	reader, err := OpenColumnPartSetReader(workspace, manifest, ColumnPartImageReadOptions{})
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("OpenColumnPartSetReader: %v", err)
	}
	return workspace, reader
}

func benchmarkJSONBenchMutations(b *testing.B, ds JSONBenchDataset) (map[int64]map[string]int64, map[int64]bool) {
	b.Helper()
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	updates := map[int64]map[string]int64{
		5: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][40],
			"time_us":                ds.Columns["time_us"][5] - 10_000_000,
		},
		25: {
			"kind_code":              codes.kindCommit,
			"commit_operation_code":  codes.operationCreate,
			"commit_collection_code": codes.collectionPost,
			"did_code":               ds.Columns["did_code"][41],
			"time_us":                ds.Columns["time_us"][25] + 20_000_000,
		},
	}
	for _, update := range updates {
		update["hour_of_day"] = unixMicroHour(update["time_us"])
	}
	return updates, map[int64]bool{10: true, 11: true}
}

func benchmarkPublishJSONBenchPartRows(b *testing.B, workspace *ColumnWorkspace, opts ColumnStoreOptions, ds JSONBenchDataset, partID uint64, start int, end int) ColumnWorkspacePartManifest {
	b.Helper()
	part, err := BuildColumnPart(partID, opts, ColumnBatch{Rows: end - start, Columns: sliceJSONBenchColumns(ds, start, end)})
	if err != nil {
		b.Fatalf("BuildColumnPart(%d,%d:%d): %v", partID, start, end, err)
	}
	entry, err := workspace.PublishPart(part, ds.Dictionaries)
	if err != nil {
		b.Fatalf("PublishPart(%d): %v", partID, err)
	}
	return entry
}
