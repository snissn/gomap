package colgranule

import (
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

	compacted, err := CompactColumnPartSet(workspace, reader, opts, ds.Dictionaries, 399)
	if err != nil {
		t.Fatalf("CompactColumnPartSet: %v", err)
	}
	if compacted.VisibleRows != expected.Rows || compacted.DroppedRows != len(updates)+len(deletes) {
		t.Fatalf("compaction rows visible=%d dropped=%d want visible=%d dropped=%d", compacted.VisibleRows, compacted.DroppedRows, expected.Rows, len(updates)+len(deletes))
	}
	if compacted.ReclaimableBytes != manifest.ByteAccounting.TotalAssetBytes || compacted.NewAssetBytes == 0 {
		t.Fatalf("compaction bytes=%+v manifest bytes=%d", compacted, manifest.ByteAccounting.TotalAssetBytes)
	}
	if len(compacted.Manifest.PartSet.BaseParts) != 1 || len(compacted.Manifest.PartSet.DeltaParts) != 0 || len(compacted.Manifest.PartSet.Tombstones) != 0 {
		t.Fatalf("bad compacted manifest part set=%+v", compacted.Manifest.PartSet)
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

func benchmarkColumnPartSetReader(b *testing.B, ds JSONBenchDataset, opts ColumnStoreOptions) (*ColumnPartSetReader, func()) {
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
	reader, err := OpenColumnPartSetReader(workspace, manifest, ColumnPartImageReadOptions{})
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
