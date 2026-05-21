package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnPhysicalRowReaderFetchesRowsWithBoundedCacheV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	leftRows := testColumnPhysicalRowReaderRowsV1(0, 3, normalized)
	rightRows := testColumnPhysicalRowReaderRowsV1(3, 3, normalized)
	left := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 7, 1, leftRows)
	right := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 7, 2, rightRows)
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, left, right), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"embedding", "neighbors"},
		MaxDecodedBlocks:  1,
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if got, want := reader.RowCount(), 6; got != want {
		t.Fatalf("RowCount=%d want %d", got, want)
	}
	var scratch columnPhysicalRowReaderScratch
	scratch.Values = make([]columnDeclaredValue, 0, 2)
	scratch.Float32Values = make([]float32, 0, normalized.Columns[1].VectorDims)
	scratch.Uint32Values = make([]uint32, 0, 4)

	row, err := reader.FetchRow(1, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(1): %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-001", 1, []float32{1, 1.25, 1.5, 1.75}, []uint32{1, 2, 3})
	row, err = reader.FetchRow(2, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(2): %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-002", 2, []float32{2, 2.25, 2.5, 2.75}, []uint32{2, 3, 4})
	row, err = reader.FetchRow(4, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(4): %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-004", 4, []float32{4, 4.25, 4.5, 4.75}, []uint32{4, 5, 6})

	stats := reader.Stats()
	if stats.OpenGranulesRead != 2 || stats.Rows != 6 || stats.Granules != 2 {
		t.Fatalf("open stats=%+v want two granules and six rows", stats)
	}
	if stats.CacheMisses != 2 || stats.CacheHits != 1 || stats.BlockEvictions != 1 {
		t.Fatalf("cache stats=%+v want misses=2 hits=1 evictions=1", stats)
	}
	if stats.ResidentBytes <= 0 || stats.MaxResidentBytes <= 0 || stats.ResidentBytes > stats.MaxResidentBytes {
		t.Fatalf("resident stats=%+v", stats)
	}
}

func TestColumnPhysicalRowReaderBatchFetchReusesCachedBlockV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	left := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 9, 1, testColumnPhysicalRowReaderRowsV1(0, 4, normalized))
	right := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 9, 2, testColumnPhysicalRowReaderRowsV1(4, 4, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, left, right), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"doc_ordinal"},
		MaxDecodedBlocks:  2,
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnPhysicalRowReaderScratch
	scratch.Values = make([]columnDeclaredValue, 0, 1)
	var got []int64
	if err := reader.FetchBatch([]int{0, 1, 5, 6}, &scratch, func(row columnPhysicalRowReaderRow) error {
		if len(row.Values) != 1 || row.Values[0].Type != ColumnStoreValueInt64 {
			return fmt.Errorf("unexpected row values=%+v", row.Values)
		}
		got = append(got, row.Values[0].Int64)
		return nil
	}); err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	want := []int64{0, 1, 5, 6}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("batch got=%v want=%v", got, want)
	}
	stats := reader.Stats()
	if stats.BatchFetches != 1 || stats.RowsFetched != 4 {
		t.Fatalf("stats=%+v want one batch and four rows", stats)
	}
	if stats.CacheMisses != 2 || stats.CacheHits != 2 || stats.BlockEvictions != 0 {
		t.Fatalf("cache stats=%+v want two block misses, two hits, no evictions", stats)
	}
}

func TestColumnPhysicalRowReaderRejectsMutationPartsV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 11, 1, testColumnPhysicalRowReaderRowsV1(0, 1, normalized))
	view := columnPhysicalRowReaderViewForTestV1(root, normalized, ref)
	view.MutationParts = 1
	if _, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{RequireInsertOnly: true}); err == nil || err != errColumnPhysicalQueryNeedsVisibility {
		t.Fatalf("new reader err=%v want mutation visibility failure", err)
	}
}

func TestColumnPhysicalRowReaderRejectsNegativeMaxDecodedBlocksV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	view := columnPhysicalRowReaderViewForTestV1(root, normalized)
	_, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{MaxDecodedBlocks: -1})
	if err == nil || !strings.Contains(err.Error(), "max decoded blocks cannot be negative") {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView err=%v want negative max decoded blocks failure", err)
	}
}

func TestColumnPhysicalRowReaderOwnsSnapshotCloseV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 12, 1, testColumnPhysicalRowReaderRowsV1(0, 1, normalized))
	closed := 0
	reader, err := newColumnPhysicalRowReaderFromSnapshotViewWithClose(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"embedding", "neighbors"},
		RequireInsertOnly: true,
	}, func() {
		closed++
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotViewWithClose: %v", err)
	}
	if closed != 0 {
		t.Fatalf("snapshot close called during open: %d", closed)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if closed != 1 {
		t.Fatalf("snapshot close calls=%d want 1", closed)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if closed != 1 {
		t.Fatalf("snapshot close calls after second close=%d want 1", closed)
	}
}

func TestColumnPhysicalRowReaderClosesSnapshotOnOpenErrorV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	view := columnPhysicalRowReaderViewForTestV1(backenddb.ColumnAssetRootDirPath(t.TempDir()), normalized)
	view.ColumnStoreEnabled = false
	closed := 0
	_, err := newColumnPhysicalRowReaderFromSnapshotViewWithClose(view, columnPhysicalRowReaderOptions{}, func() {
		closed++
	})
	if err == nil {
		t.Fatal("newColumnPhysicalRowReaderFromSnapshotViewWithClose succeeded, want error")
	}
	if closed != 1 {
		t.Fatalf("snapshot close calls=%d want 1", closed)
	}
}

func TestColumnPhysicalRowReaderClosesSnapshotOnceOnRangeBuildErrorV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 12, 1, testColumnPhysicalRowReaderRowsV1(0, 1, normalized))
	ref.FileID++
	closed := 0
	_, err := newColumnPhysicalRowReaderFromSnapshotViewWithClose(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"embedding", "neighbors"},
		RequireInsertOnly: true,
	}, func() {
		closed++
	})
	if err == nil {
		t.Fatal("newColumnPhysicalRowReaderFromSnapshotViewWithClose succeeded, want range-build error")
	}
	if closed != 1 {
		t.Fatalf("snapshot close calls=%d want 1", closed)
	}
}

func TestColumnPhysicalRowReaderRejectsOutOfRangeOrdinalV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 13, 1, testColumnPhysicalRowReaderRowsV1(0, 2, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"doc_ordinal"},
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnPhysicalRowReaderScratch
	if _, err := reader.FetchRow(2, &scratch); err == nil {
		t.Fatal("FetchRow accepted ordinal equal to RowCount")
	}
	if _, err := reader.FetchRow(-1, &scratch); err == nil {
		t.Fatal("FetchRow accepted negative ordinal")
	}
}

func TestColumnPhysicalRowReaderRejectsUseAfterCloseV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 14, 1, testColumnPhysicalRowReaderRowsV1(0, 1, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"doc_ordinal"},
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var scratch columnPhysicalRowReaderScratch
	if _, err := reader.FetchRow(0, &scratch); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("FetchRow after Close err=%v want closed failure", err)
	}
	if err := reader.FetchBatch([]int{0}, &scratch, func(columnPhysicalRowReaderRow) error { return nil }); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("FetchBatch after Close err=%v want closed failure", err)
	}
}

func TestColumnPhysicalRowReaderNilReceiverGuardsV1(t *testing.T) {
	var reader *columnPhysicalRowReader
	if got := reader.RowCount(); got != 0 {
		t.Fatalf("nil RowCount=%d want 0", got)
	}
	if got := reader.Stats(); got != (columnPhysicalRowReaderStats{}) {
		t.Fatalf("nil Stats=%+v want zero", got)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("nil Close err=%v want nil", err)
	}
	var scratch columnPhysicalRowReaderScratch
	if _, err := reader.FetchRow(0, &scratch); err == nil || !strings.Contains(err.Error(), "nil physical column row reader") {
		t.Fatalf("nil FetchRow err=%v want nil-reader failure", err)
	}
	if err := reader.FetchBatch([]int{0}, &scratch, func(columnPhysicalRowReaderRow) error { return nil }); err == nil || !strings.Contains(err.Error(), "nil physical column row reader") {
		t.Fatalf("nil FetchBatch err=%v want nil-reader failure", err)
	}
}

func TestColumnPhysicalRowReaderBatchVisitorErrorV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 16, 1, testColumnPhysicalRowReaderRowsV1(0, 2, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"doc_ordinal"},
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	sentinel := errors.New("stop visitor")
	var scratch columnPhysicalRowReaderScratch
	err = reader.FetchBatch([]int{0, 1}, &scratch, func(row columnPhysicalRowReaderRow) error {
		if row.Ordinal != 0 {
			t.Fatalf("visitor reached ordinal=%d after sentinel", row.Ordinal)
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("FetchBatch err=%v want sentinel", err)
	}
	if stats := reader.Stats(); stats.RowsFetched != 1 {
		t.Fatalf("stats=%+v want exactly one fetched row before visitor error", stats)
	}
}

func TestColumnPhysicalRowReaderWarmScratchHotFetchZeroAllocsV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 15, 1, testColumnPhysicalRowReaderRowsV1(0, 8, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"embedding", "neighbors"},
		MaxDecodedBlocks:  1,
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	scratch := columnPhysicalRowReaderScratch{
		Values:        make([]columnDeclaredValue, 0, 2),
		Float32Values: make([]float32, 0, 4),
		Uint32Values:  make([]uint32, 0, 4),
	}
	if _, err := reader.FetchRow(3, &scratch); err != nil {
		t.Fatalf("warm FetchRow: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		row, err := reader.FetchRow(3, &scratch)
		if err != nil {
			t.Fatalf("FetchRow: %v", err)
		}
		columnPhysicalScanBenchSum += int64(row.Values[1].AdjacencyList[0])
	})
	if allocs != 0 {
		t.Fatalf("hot FetchRow allocs=%v want zero", allocs)
	}
}

func TestColumnPhysicalRowReaderScratchAppendRejectsWrappedFixedWidthSliceLengthsV1(t *testing.T) {
	wrappedElementCount := uint64(maxCollectionInt)/2 + 2
	var b bytes.Buffer
	writeManifestUint64(&b, wrappedElementCount)
	raw := b.Bytes()

	t.Run("float32_vector", func(t *testing.T) {
		cur := manifestCursor{raw: raw}
		got, err := cur.appendFloat32SliceWithExpectedLength(nil, int(wrappedElementCount))
		if err == nil || !strings.Contains(err.Error(), "short column binary float32_vector") {
			t.Fatalf("appendFloat32SliceWithExpectedLength err=%v want short binary error", err)
		}
		if got != nil {
			t.Fatalf("appendFloat32SliceWithExpectedLength returned %d values", len(got))
		}
	})

	t.Run("uint32_slice", func(t *testing.T) {
		cur := manifestCursor{raw: raw}
		got, err := cur.appendUint32Slice(nil)
		if err == nil || !strings.Contains(err.Error(), "short column binary uint32 slice") {
			t.Fatalf("appendUint32Slice err=%v want short binary error", err)
		}
		if got != nil {
			t.Fatalf("appendUint32Slice returned %d values", len(got))
		}
	})
}

func BenchmarkColumnPhysicalRowReaderFetchV1(b *testing.B) {
	normalized := testColumnPhysicalRowReaderConfigForBenchV1(b, 128)
	root := backenddb.ColumnAssetRootDirPath(b.TempDir())
	const rowsPerAsset = 1024
	const assets = 8
	refs := make([]ColumnAssetRef, 0, assets)
	for asset := 0; asset < assets; asset++ {
		refs = append(refs, writeColumnPhysicalRowReaderAssetForTestV1(b, root, normalized, 17, uint64(asset+1), testColumnPhysicalRowReaderRowsV1(asset*rowsPerAsset, rowsPerAsset, normalized)))
	}
	b.Run("row_warmed_vector_adjacency", func(b *testing.B) {
		reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, refs...), columnPhysicalRowReaderOptions{
			ProjectedColumns:  []string{"embedding", "neighbors"},
			MaxDecodedBlocks:  assets,
			RequireInsertOnly: true,
		})
		if err != nil {
			b.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
		}
		defer func() { _ = reader.Close() }()
		scratch := columnPhysicalRowReaderScratch{
			Values:        make([]columnDeclaredValue, 0, 2),
			Float32Values: make([]float32, 0, normalized.Columns[1].VectorDims),
			Uint32Values:  make([]uint32, 0, 16),
		}
		for asset := 0; asset < assets; asset++ {
			if _, err := reader.FetchRow(asset*rowsPerAsset, &scratch); err != nil {
				b.Fatalf("warm FetchRow asset=%d: %v", asset, err)
			}
		}
		b.ReportAllocs()
		baseStats := reader.Stats()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ordinal := (i * 131) & (assets*rowsPerAsset - 1)
			row, err := reader.FetchRow(ordinal, &scratch)
			if err != nil {
				b.Fatalf("FetchRow(%d): %v", ordinal, err)
			}
			columnPhysicalScanBenchSum += int64(row.Values[1].AdjacencyList[0])
		}
		b.StopTimer()
		stats := reader.Stats()
		b.ReportMetric(float64(stats.CacheHits-baseStats.CacheHits)/float64(b.N), "cache_hits/op")
		b.ReportMetric(float64(stats.CacheMisses-baseStats.CacheMisses)/float64(b.N), "cache_misses/op")
		b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
	})
	b.Run("batch_warmed_vector_adjacency", func(b *testing.B) {
		reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, refs...), columnPhysicalRowReaderOptions{
			ProjectedColumns:  []string{"embedding", "neighbors"},
			MaxDecodedBlocks:  assets,
			RequireInsertOnly: true,
		})
		if err != nil {
			b.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
		}
		defer func() { _ = reader.Close() }()
		scratch := columnPhysicalRowReaderScratch{
			Values:        make([]columnDeclaredValue, 0, 2),
			Float32Values: make([]float32, 0, normalized.Columns[1].VectorDims),
			Uint32Values:  make([]uint32, 0, 16),
		}
		for asset := 0; asset < assets; asset++ {
			if _, err := reader.FetchRow(asset*rowsPerAsset, &scratch); err != nil {
				b.Fatalf("warm FetchRow asset=%d: %v", asset, err)
			}
		}
		ordinals := []int{7, 11, 19, 23, 31, 43, 59, 71}
		b.ReportAllocs()
		baseStats := reader.Stats()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			base := (i * 97) & (assets*rowsPerAsset - 1)
			for j := range ordinals {
				ordinals[j] = (base + j*3) & (assets*rowsPerAsset - 1)
			}
			if err := reader.FetchBatch(ordinals, &scratch, func(row columnPhysicalRowReaderRow) error {
				columnPhysicalScanBenchSum += int64(row.Values[1].AdjacencyList[0])
				return nil
			}); err != nil {
				b.Fatalf("FetchBatch: %v", err)
			}
		}
		b.StopTimer()
		stats := reader.Stats()
		rowsFetched := stats.RowsFetched - baseStats.RowsFetched
		b.ReportMetric(float64(rowsFetched)/float64(b.N), "rows/op")
		b.ReportMetric(float64(stats.CacheHits-baseStats.CacheHits)/float64(rowsFetched), "cache_hits/row")
		b.ReportMetric(float64(stats.CacheMisses-baseStats.CacheMisses)/float64(rowsFetched), "cache_misses/row")
		b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
	})
}

func testColumnPhysicalRowReaderConfigV1(t testing.TB) *ColumnStoreConfig {
	t.Helper()
	return testColumnPhysicalRowReaderConfigForBenchV1(t, 4)
}

func testColumnPhysicalRowReaderConfigForBenchV1(t testing.TB, dims int) *ColumnStoreConfig {
	t.Helper()
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "doc_ordinal", Path: "doc_ordinal", ValueType: ColumnStoreValueInt64},
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: dims},
			{Name: "neighbors", Path: "neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
		ProfileSupport: ColumnStoreProfileBenchmarkRelaxed,
	})
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.ActiveManifest = &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1}
	cfg.RecoveryAuthoritativeManifest = &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	return cfg
}

func testColumnPhysicalRowReaderRowsV1(start, count int, cfg *ColumnStoreConfig) []columnDeclaredRow {
	rows := make([]columnDeclaredRow, count)
	dims := cfg.Columns[1].VectorDims
	for i := range rows {
		ordinal := start + i
		vector := make([]float32, dims)
		for dim := range vector {
			vector[dim] = float32(ordinal) + float32(dim)*0.25
		}
		neighbors := make([]uint32, 16)
		if dims == 4 {
			neighbors = neighbors[:3]
		}
		for j := range neighbors {
			neighbors[j] = uint32(ordinal + j)
		}
		rows[i] = columnDeclaredRow{
			ID: []byte(fmt.Sprintf("doc-%03d", ordinal)),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueInt64, Present: true, Int64: int64(ordinal)},
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vector},
				{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: neighbors},
			},
		}
	}
	return rows
}

func writeColumnPhysicalRowReaderAssetForTestV1(t testing.TB, root string, cfg *ColumnStoreConfig, generation, partID uint64, rows []columnDeclaredRow) ColumnAssetRef {
	t.Helper()
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, *cfg)
	if err != nil {
		t.Fatalf("newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	ref, err := appender.append(encoded, generation, partID)
	if err != nil {
		_ = cleanupColumnAssetRewriteOpenAppender(appender)
		t.Fatalf("append: %v", err)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("close appender: %v", err)
	}
	return ref
}

func columnPhysicalRowReaderViewForTestV1(root string, cfg *ColumnStoreConfig, refs ...ColumnAssetRef) columnPhysicalScanSnapshotView {
	assetRefs := make([]columnManifestAssetRefForScan, len(refs))
	for i, ref := range refs {
		assetRefs[i] = columnManifestAssetRefForScan{Ref: ref, Reason: ColumnPublishOperationInsert}
	}
	return columnPhysicalScanSnapshotView{
		CollectionName:     "events",
		Config:             *cfg,
		ColumnStoreEnabled: true,
		CommitSeq:          1,
		AssetRefs:          assetRefs,
		Diagnostics:        columnPhysicalScanDiagnostics{AssetRefs: len(refs)},
		ColumnAssetRootDir: root,
		AssetNamespace:     cfg.AssetManager.Namespace,
	}
}

func assertColumnPhysicalRowReaderVectorRowV1(t testing.TB, row columnPhysicalRowReaderRow, id string, ordinal int64, vector []float32, neighbors []uint32) {
	t.Helper()
	if string(row.ID) != id || row.Ordinal != int(ordinal) || row.RowIndex < 0 || row.Deleted {
		t.Fatalf("row identity=%+v id=%q want id=%q ordinal=%d not deleted", row, string(row.ID), id, ordinal)
	}
	if len(row.Values) != 2 {
		t.Fatalf("row values=%d want 2", len(row.Values))
	}
	gotVector := row.Values[0].Float32Vector
	if len(gotVector) != len(vector) {
		t.Fatalf("vector len=%d want %d", len(gotVector), len(vector))
	}
	for i := range vector {
		if math.Abs(float64(gotVector[i]-vector[i])) > 1e-6 {
			t.Fatalf("vector[%d]=%v want %v", i, gotVector[i], vector[i])
		}
	}
	gotNeighbors := row.Values[1].AdjacencyList
	if fmt.Sprint(gotNeighbors) != fmt.Sprint(neighbors) {
		t.Fatalf("neighbors=%v want %v", gotNeighbors, neighbors)
	}
}
