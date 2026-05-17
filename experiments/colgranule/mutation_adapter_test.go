package colgranule

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestColumnMutationAdapterReplayAndJSONBenchParity(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	opts, err := JSONBenchColumnPartOptions(ds, 32)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
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
	deletes := []int64{10, 11}

	dir := t.TempDir()
	workspace, adapter := testColumnMutationAdapter(t, dir, opts, ds.Dictionaries)
	defer workspace.Close()
	baseResult, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{
		SourceRowRootGeneration: 1,
		SourceRowVersionUpper:   uint64(ds.Rows),
	})
	if err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	if baseResult.GenerationID != 1 || baseResult.InsertedRows != ds.Rows {
		t.Fatalf("base result=%+v want generation=1 inserted=%d", baseResult, ds.Rows)
	}
	result, err := adapter.Apply(ColumnMutationBatch{
		Updates:                 jsonBenchDeltaBatch(ds, updates),
		Deletes:                 deletes,
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + len(updates) + len(deletes)),
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.GenerationID != 2 || result.UpdatedRows != len(updates) || result.DeletedRows != len(deletes) {
		t.Fatalf("mutation result=%+v", result)
	}
	if len(result.Manifest.PartSet.BaseParts) != 1 || len(result.Manifest.PartSet.DeltaParts) != 1 || len(result.Manifest.PartSet.Tombstones) != len(deletes) {
		t.Fatalf("manifest part set=%+v", result.Manifest.PartSet)
	}
	coverage := result.Manifest.PartSet.DeltaParts[0].Coverage
	if coverage.SourceRowRootGeneration != 2 || coverage.SourceRowVersionLower != uint64(ds.Rows) || coverage.SourceRowVersionUpper != uint64(ds.Rows+len(updates)+len(deletes)) {
		t.Fatalf("delta coverage=%+v", coverage)
	}

	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	expected := applyJSONBenchMutations(ds, updates, map[int64]bool{10: true, 11: true})
	if stats := reader.VisibilityStats(); stats.VisibleRows != expected.Rows || stats.SupersededRows != len(updates) || stats.DeletedRows != len(deletes) {
		t.Fatalf("visibility stats=%+v want visible=%d superseded=%d deleted=%d", stats, expected.Rows, len(updates), len(deletes))
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, reader, expected)

	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench", ValidationMode: ColumnWorkspaceValidateTCS1Header})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace(reopen): %v", err)
	}
	defer reopened.Close()
	reopenedManifest, err := reopened.LoadCollectionManifest()
	if err != nil {
		t.Fatalf("LoadCollectionManifest(reopen): %v", err)
	}
	reopenedReader, err := OpenColumnPartSetReader(reopened, reopenedManifest, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("OpenColumnPartSetReader(reopen): %v", err)
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, reopenedReader, expected)

	replayWorkspace, replayAdapter := testColumnMutationAdapter(t, t.TempDir(), opts, ds.Dictionaries)
	defer replayWorkspace.Close()
	if _, err := replayAdapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		t.Fatalf("replay PublishBaseBatch: %v", err)
	}
	if _, err := replayAdapter.Apply(ColumnMutationBatch{
		Updates:                 jsonBenchDeltaBatch(ds, updates),
		Deletes:                 deletes,
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + len(updates) + len(deletes)),
	}); err != nil {
		t.Fatalf("replay Apply: %v", err)
	}
	replayReader, err := replayAdapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("replay Reader: %v", err)
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, replayReader, expected)
}

func TestColumnMutationAdapterDeleteOnlyBatch(t *testing.T) {
	ds := syntheticJSONBenchDataset(96)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	workspace, adapter := testColumnMutationAdapter(t, t.TempDir(), opts, ds.Dictionaries)
	defer workspace.Close()
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	result, err := adapter.Apply(ColumnMutationBatch{
		Deletes:                 []int64{3, 5, 5, 7},
		SourceRowRootGeneration: 2,
		SourceRowVersionLower:   uint64(ds.Rows),
		SourceRowVersionUpper:   uint64(ds.Rows + 1),
	})
	if err != nil {
		t.Fatalf("Apply(delete-only): %v", err)
	}
	if result.GenerationID != 2 || result.DeletedRows != 3 || result.Part.PartID != 0 {
		t.Fatalf("delete-only result=%+v", result)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if stats := reader.VisibilityStats(); stats.VisibleRows != ds.Rows-3 || stats.DeletedRows != 3 || stats.DeltaParts != 0 {
		t.Fatalf("visibility stats=%+v", stats)
	}
	for _, id := range []int64{3, 5, 7} {
		if _, ok := reader.LatestLocator(id); ok {
			t.Fatalf("deleted id %d has latest locator", id)
		}
	}
}

func TestColumnPartSetLocatorDecisionPathsMatch(t *testing.T) {
	ds := syntheticJSONBenchDataset(128)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	workspace, adapter := testColumnMutationAdapter(t, t.TempDir(), opts, ds.Dictionaries)
	defer workspace.Close()
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	for i := 0; i < 12; i++ {
		id := int64(i * 3)
		row := int(id)
		nextTime := ds.Columns["time_us"][row] + int64(i+1)*1_000_000
		updates := jsonBenchDeltaBatch(ds, map[int64]map[string]int64{
			id: {
				"time_us":     nextTime,
				"hour_of_day": unixMicroHour(nextTime),
			},
		})
		if _, err := adapter.Apply(ColumnMutationBatch{
			Updates:                 updates,
			SourceRowRootGeneration: uint64(i + 2),
			SourceRowVersionLower:   uint64(ds.Rows + i),
			SourceRowVersionUpper:   uint64(ds.Rows + i + 1),
		}); err != nil {
			t.Fatalf("Apply update %d: %v", i, err)
		}
	}
	if _, err := adapter.Apply(ColumnMutationBatch{
		Deletes:                 []int64{9},
		SourceRowRootGeneration: 20,
		SourceRowVersionLower:   uint64(ds.Rows + 12),
		SourceRowVersionUpper:   uint64(ds.Rows + 13),
	}); err != nil {
		t.Fatalf("Apply delete: %v", err)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	for _, id := range []int64{0, 3, 12, 45, 127} {
		side, sideOK := reader.LatestLocator(id)
		scan, scanOK := reader.ScanLatestLocator(id)
		if sideOK != scanOK || side != scan {
			t.Fatalf("locator id=%d side=(%+v,%v) scan=(%+v,%v)", id, side, sideOK, scan, scanOK)
		}
		sideValue, sideValueOK, err := reader.ValueAtLatest(id, "time_us")
		if err != nil {
			t.Fatalf("ValueAtLatest(%d): %v", id, err)
		}
		scanValue, scanValueOK, err := reader.ScanValueAtLatest(id, "time_us")
		if err != nil {
			t.Fatalf("ScanValueAtLatest(%d): %v", id, err)
		}
		if sideValueOK != scanValueOK || sideValue != scanValue {
			t.Fatalf("value id=%d side=(%d,%v) scan=(%d,%v)", id, sideValue, sideValueOK, scanValue, scanValueOK)
		}
	}
	if _, ok := reader.LatestLocator(9); ok {
		t.Fatal("deleted id 9 has side-index locator")
	}
	if _, ok := reader.ScanLatestLocator(9); ok {
		t.Fatal("deleted id 9 has scan locator")
	}
}

func BenchmarkColumnLocatorDecisionM8A(b *testing.B) {
	for _, deltaParts := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("delta_parts_%03d", deltaParts), func(b *testing.B) {
			reader, target := benchmarkColumnMutationLocatorReader(b, deltaParts)
			b.ReportMetric(float64(deltaParts+1), "parts")
			b.ReportMetric(float64(reader.VisibilityStats().VisibleRows), "visible_rows")
			b.Run("side_index_locator", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					locator, ok := reader.LatestLocator(target)
					if !ok {
						b.Fatal("missing side-index locator")
					}
					benchSink += int64(locator.PartRow)
				}
			})
			b.Run("part_local_scan_locator", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					locator, ok := reader.ScanLatestLocator(target)
					if !ok {
						b.Fatal("missing scan locator")
					}
					benchSink += int64(locator.PartRow)
				}
			})
			b.Run("side_index_point_value", func(b *testing.B) {
				var scratch ColumnPartSetPointLookupScratch
				if _, _, err := reader.ValueAtLatestWithScratch(target, "time_us", &scratch); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, ok, err := reader.ValueAtLatestWithScratch(target, "time_us", &scratch)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						b.Fatal("missing side-index value")
					}
					benchSink += value
				}
			})
			b.Run("part_local_scan_point_value", func(b *testing.B) {
				var scratch ColumnPartSetPointLookupScratch
				if _, _, err := reader.ScanValueAtLatestWithScratch(target, "time_us", &scratch); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, ok, err := reader.ScanValueAtLatestWithScratch(target, "time_us", &scratch)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						b.Fatal("missing scan value")
					}
					benchSink += value
				}
			})
		})
	}
}

func BenchmarkColumnMutationAdapterApplyM8A(b *testing.B) {
	ds := syntheticJSONBenchDataset(4096)
	opts, err := JSONBenchColumnPartOptions(ds, 128)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	updates := make(map[int64]map[string]int64, 128)
	for i := int64(0); i < 128; i++ {
		id := i * 7
		nextTime := ds.Columns["time_us"][int(id)] + 1_000_000
		updates[id] = map[string]int64{
			"time_us":     nextTime,
			"hour_of_day": unixMicroHour(nextTime),
		}
	}
	deletes := make([]int64, 0, 32)
	for i := int64(0); i < 32; i++ {
		deletes = append(deletes, 2048+i*3)
	}
	updateBatch := jsonBenchDeltaBatch(ds, updates)
	root := b.TempDir()
	b.ReportMetric(float64(updateBatch.Rows), "updated_rows")
	b.ReportMetric(float64(len(deletes)), "deleted_rows")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		workspace, adapter := benchmarkColumnMutationAdapter(b, filepath.Join(root, fmt.Sprintf("iter-%06d", i)), opts, ds.Dictionaries)
		if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
			_ = workspace.Close()
			b.Fatalf("PublishBaseBatch: %v", err)
		}
		b.StartTimer()
		_, err := adapter.Apply(ColumnMutationBatch{
			Updates:                 updateBatch,
			Deletes:                 deletes,
			SourceRowRootGeneration: 2,
			SourceRowVersionLower:   uint64(ds.Rows),
			SourceRowVersionUpper:   uint64(ds.Rows + updateBatch.Rows + len(deletes)),
		})
		b.StopTimer()
		if err != nil {
			_ = workspace.Close()
			b.Fatalf("Apply: %v", err)
		}
		_ = workspace.Close()
	}
}

func testColumnMutationAdapter(t testing.TB, dir string, opts ColumnStoreOptions, dictionaries map[string]map[string]int64) (*ColumnWorkspace, *ColumnMutationAdapter) {
	t.Helper()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:   "jsonbench",
		StoreOptions: opts,
		Dictionaries: dictionaries,
	})
	if err != nil {
		_ = workspace.Close()
		t.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	return workspace, adapter
}

func benchmarkColumnMutationAdapter(b *testing.B, dir string, opts ColumnStoreOptions, dictionaries map[string]map[string]int64) (*ColumnWorkspace, *ColumnMutationAdapter) {
	b.Helper()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		b.Fatalf("OpenColumnWorkspace: %v", err)
	}
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:   "jsonbench",
		StoreOptions: opts,
		Dictionaries: dictionaries,
	})
	if err != nil {
		_ = workspace.Close()
		b.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	return workspace, adapter
}

func benchmarkColumnMutationLocatorReader(b *testing.B, deltaParts int) (*ColumnPartSetReader, int64) {
	b.Helper()
	ds := syntheticJSONBenchDataset(8192)
	opts, err := JSONBenchColumnPartOptions(ds, 128)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	workspace, adapter := benchmarkColumnMutationAdapter(b, b.TempDir(), opts, ds.Dictionaries)
	b.Cleanup(func() { _ = workspace.Close() })
	if _, err := adapter.PublishBaseBatch(ColumnBatch{Rows: ds.Rows, Columns: ds.Columns}, ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(ds.Rows)}); err != nil {
		b.Fatalf("PublishBaseBatch: %v", err)
	}
	target := int64(4096)
	for i := 0; i < deltaParts; i++ {
		id := target
		if i%4 != 0 {
			id = int64((i * 97) % ds.Rows)
		}
		nextTime := ds.Columns["time_us"][int(id)] + int64(i+1)*1_000_000
		updates := jsonBenchDeltaBatch(ds, map[int64]map[string]int64{
			id: {
				"time_us":     nextTime,
				"hour_of_day": unixMicroHour(nextTime),
			},
		})
		if _, err := adapter.Apply(ColumnMutationBatch{
			Updates:                 updates,
			SourceRowRootGeneration: uint64(i + 2),
			SourceRowVersionLower:   uint64(ds.Rows + i),
			SourceRowVersionUpper:   uint64(ds.Rows + i + 1),
		}); err != nil {
			b.Fatalf("Apply(%d): %v", i, err)
		}
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		b.Fatalf("Reader: %v", err)
	}
	return reader, target
}
