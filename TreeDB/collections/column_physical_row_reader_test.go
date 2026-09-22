package collections

import (
	"bytes"
	"encoding/binary"
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

func TestColumnPhysicalRowReaderDenseVectorScratchResets1930(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{Name: "codes", Path: "codes", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueUint16Vector, ElementsPerRow: 3}},
	})
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.ActiveManifest = &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1}
	cfg.RecoveryAuthoritativeManifest = &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	rows := []columnDeclaredRow{
		{ID: []byte("doc-000"), Values: []columnDeclaredValue{{Type: ColumnStoreValueUint16Vector, Present: true, DenseNumericVector: []byte{1, 0, 2, 0, 3, 0}}}},
		{ID: []byte("doc-001"), Values: []columnDeclaredValue{{Type: ColumnStoreValueUint16Vector, Present: true, DenseNumericVector: []byte{4, 0, 5, 0, 6, 0}}}},
	}
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, cfg, 7, 1, rows)
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, cfg, ref), columnPhysicalRowReaderOptions{ProjectedColumns: []string{"codes"}, RequireInsertOnly: true})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnPhysicalRowReaderScratch
	row, err := reader.FetchRow(0, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(0): %v", err)
	}
	if got := row.Values[0].DenseNumericVector; !bytes.Equal(got, rows[0].Values[0].DenseNumericVector) || len(scratch.Bytes) != 6 {
		t.Fatalf("row0 dense=%x scratch_bytes=%d", got, len(scratch.Bytes))
	}
	row, err = reader.FetchRow(1, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(1): %v", err)
	}
	if got := row.Values[0].DenseNumericVector; !bytes.Equal(got, rows[1].Values[0].DenseNumericVector) || len(scratch.Bytes) != 6 {
		t.Fatalf("row1 dense=%x scratch_bytes=%d want per-row scratch reset", got, len(scratch.Bytes))
	}
}

func TestColumnPhysicalRowReaderFetchesLittleEndianFixedWidthRowsV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	normalized.Columns[1].FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	normalized.SchemaHash = hashColumnStoreSchema(normalized)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 7, 1, testColumnPhysicalRowReaderRowsV1(0, 3, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, ref), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"embedding", "neighbors"},
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var scratch columnPhysicalRowReaderScratch
	row, err := reader.FetchRow(2, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(2): %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-002", 2, []float32{2, 2.25, 2.5, 2.75}, []uint32{2, 3, 4})

	if _, err := reader.FetchRow(2, &scratch); err != nil {
		t.Fatalf("warm FetchRow(2): %v", err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := reader.FetchRow(2, &scratch); err != nil {
			t.Fatalf("FetchRow(2): %v", err)
		}
	}); allocs != 0 {
		t.Fatalf("little-endian hot FetchRow allocs=%v want zero", allocs)
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

func TestColumnPhysicalRowReaderFetchesV7FixedIDRows(t *testing.T) {
	cfg := testColumnPhysicalRowReaderFixedIDConfigV7(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	rows := []columnDeclaredRow{
		{ID: []byte("doc-0000")},
		{ID: []byte("doc-0002")},
		{ID: []byte("doc-0004")},
		{ID: []byte("doc-0006")},
	}
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, cfg, 19, 1, rows)
	raw, err := readColumnPhysicalAssetFromManager(root, ref)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	if _, version, _, err := parseColumnPhysicalAssetScanHeader(raw, ref, "events", cfg, ColumnPublishOperationInsert); err != nil {
		t.Fatalf("parseColumnPhysicalAssetScanHeader: %v", err)
	} else if version != columnPhysicalAssetVersionV7 {
		t.Fatalf("asset version=%d want V7", version)
	}

	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, cfg, ref), columnPhysicalRowReaderOptions{
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if got, want := reader.RowCount(), len(rows); got != want {
		t.Fatalf("RowCount=%d want %d", got, want)
	}

	var scratch columnPhysicalRowReaderScratch
	row, err := reader.FetchRow(2, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(2): %v", err)
	}
	if string(row.ID) != "doc-0004" || row.Ordinal != 2 || row.RowIndex != 2 || row.Deleted || len(row.Values) != 0 {
		t.Fatalf("row=%+v id=%q want fixed-id doc-0004 without values", row, string(row.ID))
	}

	var got []string
	if err := reader.FetchBatch([]int{3, 1}, &scratch, func(row columnPhysicalRowReaderRow) error {
		got = append(got, string(row.ID))
		if row.Deleted || len(row.Values) != 0 {
			return fmt.Errorf("row=%+v want non-deleted fixed-id row without values", row)
		}
		return nil
	}); err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if fmt.Sprint(got) != "[doc-0006 doc-0002]" {
		t.Fatalf("batch ids=%v", got)
	}
}

func TestColumnPhysicalRowReaderFetchesV8DenseIDRangeRows(t *testing.T) {
	cfg := testColumnPhysicalRowReaderFixedIDConfigV7(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	rows := []columnDeclaredRow{
		{ID: columnPhysicalAssetBigEndianUint64ID(900)},
		{ID: columnPhysicalAssetBigEndianUint64ID(901)},
		{ID: columnPhysicalAssetBigEndianUint64ID(902)},
		{ID: columnPhysicalAssetBigEndianUint64ID(903)},
	}
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, cfg, 19, 1, rows)
	raw, err := readColumnPhysicalAssetFromManager(root, ref)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	if _, version, _, err := parseColumnPhysicalAssetScanHeader(raw, ref, "events", cfg, ColumnPublishOperationInsert); err != nil {
		t.Fatalf("parseColumnPhysicalAssetScanHeader: %v", err)
	} else if version != columnPhysicalAssetVersionV8 {
		t.Fatalf("asset version=%d want V8", version)
	}

	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, cfg, ref), columnPhysicalRowReaderOptions{
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var scratch columnPhysicalRowReaderScratch
	row, err := reader.FetchRow(2, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(2): %v", err)
	}
	if got := binary.BigEndian.Uint64(row.ID); got != 902 || row.Ordinal != 2 || row.RowIndex != 2 || row.Deleted || len(row.Values) != 0 {
		t.Fatalf("row=%+v id=%d want dense id 902 without values", row, got)
	}

	var got []uint64
	if err := reader.FetchBatch([]int{3, 1}, &scratch, func(row columnPhysicalRowReaderRow) error {
		got = append(got, binary.BigEndian.Uint64(row.ID))
		if row.Deleted || len(row.Values) != 0 {
			return fmt.Errorf("row=%+v want non-deleted dense-id row without values", row)
		}
		return nil
	}); err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if fmt.Sprint(got) != "[903 901]" {
		t.Fatalf("batch ids=%v", got)
	}
}

func TestColumnPhysicalRowReaderCachedBlocksOwnRawBytesV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	left := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 1, testColumnPhysicalRowReaderRowsV1(0, 4, normalized))
	right := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 2, testColumnPhysicalRowReaderRowsV1(4, 4, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, left, right), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"embedding", "neighbors"},
		MaxDecodedBlocks:  2,
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	scratch := columnPhysicalRowReaderScratch{
		Values:        make([]columnDeclaredValue, 0, 2),
		Float32Values: make([]float32, 0, normalized.Columns[1].VectorDims),
		Uint32Values:  make([]uint32, 0, 4),
	}
	if _, err := reader.FetchRow(0, &scratch); err != nil {
		t.Fatalf("warm left block: %v", err)
	}
	if _, err := reader.FetchRow(4, &scratch); err != nil {
		t.Fatalf("warm right block: %v", err)
	}
	row, err := reader.FetchRow(1, &scratch)
	if err != nil {
		t.Fatalf("refetch left block: %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-001", 1, []float32{1, 1.25, 1.5, 1.75}, []uint32{1, 2, 3})
	if stats := reader.Stats(); stats.CacheMisses != 2 || stats.CacheHits != 1 || stats.BlockEvictions != 0 {
		t.Fatalf("cache stats=%+v want cached left block to survive right-block load", stats)
	}
}

func TestColumnPhysicalRowReaderHotBlockFastPathInvalidatesOnEvictionV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	left := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 1, testColumnPhysicalRowReaderRowsV1(0, 4, normalized))
	right := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 2, testColumnPhysicalRowReaderRowsV1(4, 4, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, left, right), columnPhysicalRowReaderOptions{
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
		Float32Values: make([]float32, 0, normalized.Columns[1].VectorDims),
		Uint32Values:  make([]uint32, 0, 4),
	}
	row, err := reader.FetchRow(0, &scratch)
	if err != nil {
		t.Fatalf("warm left block: %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-000", 0, []float32{0, 0.25, 0.5, 0.75}, []uint32{0, 1, 2})
	row, err = reader.FetchRow(4, &scratch)
	if err != nil {
		t.Fatalf("fetch right block: %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-004", 4, []float32{4, 4.25, 4.5, 4.75}, []uint32{4, 5, 6})
	row, err = reader.FetchRow(1, &scratch)
	if err != nil {
		t.Fatalf("refetch evicted left block: %v", err)
	}
	assertColumnPhysicalRowReaderVectorRowV1(t, row, "doc-001", 1, []float32{1, 1.25, 1.5, 1.75}, []uint32{1, 2, 3})
	stats := reader.Stats()
	if stats.CacheMisses != 3 || stats.CacheHits != 0 || stats.BlockEvictions != 2 {
		t.Fatalf("cache stats=%+v want misses=3 hits=0 evictions=2", stats)
	}
}

func TestColumnPhysicalRowReaderHotBlockFastPathPreservesInterleavedLRUV1(t *testing.T) {
	normalized := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	left := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 1, testColumnPhysicalRowReaderRowsV1(0, 3, normalized))
	middle := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 2, testColumnPhysicalRowReaderRowsV1(3, 3, normalized))
	right := writeColumnPhysicalRowReaderAssetForTestV1(t, root, normalized, 10, 3, testColumnPhysicalRowReaderRowsV1(6, 3, normalized))
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, normalized, left, middle, right), columnPhysicalRowReaderOptions{
		ProjectedColumns:  []string{"doc_ordinal"},
		MaxDecodedBlocks:  2,
		RequireInsertOnly: true,
	})
	if err != nil {
		t.Fatalf("newColumnPhysicalRowReaderFromSnapshotView: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnPhysicalRowReaderScratch
	for _, ordinal := range []int{0, 3, 4, 1, 4, 6} {
		if _, err := reader.FetchRow(ordinal, &scratch); err != nil {
			t.Fatalf("FetchRow(%d): %v", ordinal, err)
		}
	}
	row, err := reader.FetchRow(5, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(5): %v", err)
	}
	if len(row.Values) != 1 || row.Values[0].Int64 != 5 {
		t.Fatalf("row=%+v want doc_ordinal 5", row)
	}
	stats := reader.Stats()
	if stats.CacheMisses != 3 || stats.CacheHits != 4 || stats.BlockEvictions != 1 {
		t.Fatalf("cache stats=%+v want misses=3 hits=4 evictions=1", stats)
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

func TestColumnPhysicalRowReaderClonesRangeHeaderBytesV1(t *testing.T) {
	original := columnPhysicalAssetScanHeader{
		Collection: []byte("events"),
		Namespace:  []byte("events/column-assets"),
	}
	cloned := cloneColumnPhysicalAssetScanHeader(original)
	original.Collection[0] = 'x'
	original.Namespace[0] = 'x'

	if got := string(cloned.Collection); got != "events" {
		t.Fatalf("cloned collection=%q aliases original", got)
	}
	if got := string(cloned.Namespace); got != "events/column-assets" {
		t.Fatalf("cloned namespace=%q aliases original", got)
	}
}

func TestColumnPhysicalRowReaderRejectsCorruptLRUV1(t *testing.T) {
	reader := &columnPhysicalRowReader{
		maxBlocks: 1,
		blocks: map[int]*columnPhysicalRowReaderBlock{
			2: {assetOrdinal: 2, residentBytes: 128},
		},
		lru: []int{1},
	}
	err := reader.evictBlocksForInsert()
	if err == nil || !strings.Contains(err.Error(), "LRU references missing cached asset ordinal=1") {
		t.Fatalf("evictBlocksForInsert err=%v want corrupt LRU failure", err)
	}
	if _, ok := reader.blocks[2]; !ok {
		t.Fatal("evictBlocksForInsert deleted unrelated cached block after corrupt LRU")
	}
	if got := fmt.Sprint(reader.lru); got != "[1]" {
		t.Fatalf("evictBlocksForInsert lru=%s want unchanged [1]", got)
	}
}

func TestColumnPhysicalRowReaderRejectsEmptyLRUV1(t *testing.T) {
	reader := &columnPhysicalRowReader{
		maxBlocks: 1,
		blocks: map[int]*columnPhysicalRowReaderBlock{
			2: {assetOrdinal: 2, residentBytes: 128},
		},
	}
	err := reader.evictBlocksForInsert()
	if err == nil || !strings.Contains(err.Error(), "LRU empty with 1 cached blocks and max=1") {
		t.Fatalf("evictBlocksForInsert err=%v want empty LRU failure", err)
	}
	if _, ok := reader.blocks[2]; !ok {
		t.Fatal("evictBlocksForInsert deleted cached block after empty LRU")
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

func TestColumnPhysicalRowReaderScratchAppendFixedWidthSlicesV1(t *testing.T) {
	t.Run("float32_vector_empty", func(t *testing.T) {
		var b bytes.Buffer
		writeManifestFloat32Slice(&b, nil)
		cur := manifestCursor{raw: b.Bytes()}
		dst := []float32{42}
		got, err := cur.appendFloat32SliceWithExpectedLength(dst, 0)
		if err != nil {
			t.Fatalf("appendFloat32SliceWithExpectedLength: %v", err)
		}
		if len(got) != 1 || got[0] != 42 || cur.pos != len(cur.raw) {
			t.Fatalf("got=%v pos=%d len=%d", got, cur.pos, len(cur.raw))
		}
	})

	t.Run("float32_vector_exact", func(t *testing.T) {
		values := []float32{1.25, -2.5, 0.75, 8.5, -16.25}
		var b bytes.Buffer
		writeManifestFloat32Slice(&b, values)
		cur := manifestCursor{raw: b.Bytes()}
		dst := []float32{99}
		got, err := cur.appendFloat32SliceWithExpectedLength(dst, len(values))
		if err != nil {
			t.Fatalf("appendFloat32SliceWithExpectedLength: %v", err)
		}
		if len(got) != 1+len(values) || got[0] != 99 || cur.pos != len(cur.raw) {
			t.Fatalf("got=%v pos=%d len=%d", got, cur.pos, len(cur.raw))
		}
		for i, want := range values {
			if got[i+1] != want {
				t.Fatalf("got[%d]=%v want %v", i+1, got[i+1], want)
			}
		}
	})

	t.Run("float32_vector_little_endian_exact", func(t *testing.T) {
		values := []float32{math.Float32frombits(0x3f800000), math.Float32frombits(0x7fc12345), math.Float32frombits(0xc0200000)}
		var b bytes.Buffer
		writeManifestFloat32SliceWithEncoding(&b, values, ColumnFixedWidthEncodingLittleEndian)
		cur := manifestCursor{raw: b.Bytes()}
		got, err := cur.appendFloat32SliceWithExpectedLengthAndEncoding([]float32{99}, len(values), ColumnFixedWidthEncodingLittleEndian)
		if err != nil {
			t.Fatalf("appendFloat32SliceWithExpectedLengthAndEncoding: %v", err)
		}
		if len(got) != 1+len(values) || got[0] != 99 || cur.pos != len(cur.raw) {
			t.Fatalf("got=%v pos=%d len=%d", got, cur.pos, len(cur.raw))
		}
		for i, want := range values {
			if math.Float32bits(got[i+1]) != math.Float32bits(want) {
				t.Fatalf("got[%d] bits=0x%08x want 0x%08x", i+1, math.Float32bits(got[i+1]), math.Float32bits(want))
			}
		}
	})

	t.Run("float32_vector_unsupported_encoding", func(t *testing.T) {
		var b bytes.Buffer
		writeManifestFloat32Slice(&b, []float32{1})
		cur := manifestCursor{raw: b.Bytes()}
		got, err := cur.appendFloat32SliceWithExpectedLengthAndEncoding([]float32{7}, 1, ColumnFixedWidthEncoding("future"))
		if err == nil || !strings.Contains(err.Error(), "unsupported fixed_width_encoding") {
			t.Fatalf("appendFloat32SliceWithExpectedLengthAndEncoding err=%v want unsupported fixed_width_encoding", err)
		}
		if fmt.Sprint(got) != "[7]" {
			t.Fatalf("got=%v want original dst", got)
		}
	})

	t.Run("float32_vector_wrong_length", func(t *testing.T) {
		var b bytes.Buffer
		writeManifestFloat32Slice(&b, []float32{1, 2})
		cur := manifestCursor{raw: b.Bytes()}
		got, err := cur.appendFloat32SliceWithExpectedLength([]float32{7}, 3)
		if err == nil || !strings.Contains(err.Error(), "length=2 want vector_dims=3") {
			t.Fatalf("appendFloat32SliceWithExpectedLength err=%v want length mismatch", err)
		}
		if fmt.Sprint(got) != "[7]" {
			t.Fatalf("got=%v want original dst", got)
		}
	})

	t.Run("float32_vector_truncated", func(t *testing.T) {
		var b bytes.Buffer
		writeManifestUint64(&b, 2)
		writeManifestUint32(&b, math.Float32bits(1.5))
		cur := manifestCursor{raw: b.Bytes()}
		got, err := cur.appendFloat32SliceWithExpectedLength([]float32{7}, 2)
		if err == nil || !strings.Contains(err.Error(), "short column binary float32_vector") {
			t.Fatalf("appendFloat32SliceWithExpectedLength err=%v want short binary", err)
		}
		if fmt.Sprint(got) != "[7]" {
			t.Fatalf("got=%v want original dst", got)
		}
	})

	t.Run("uint32_slice_empty", func(t *testing.T) {
		var b bytes.Buffer
		writeManifestUint32Slice(&b, nil)
		cur := manifestCursor{raw: b.Bytes()}
		dst := []uint32{42}
		got, err := cur.appendUint32Slice(dst)
		if err != nil {
			t.Fatalf("appendUint32Slice: %v", err)
		}
		if len(got) != 1 || got[0] != 42 || cur.pos != len(cur.raw) {
			t.Fatalf("got=%v pos=%d len=%d", got, cur.pos, len(cur.raw))
		}
	})

	t.Run("uint32_slice_exact", func(t *testing.T) {
		values := []uint32{1, 17, 1024, math.MaxUint16 + 1, math.MaxUint32}
		var b bytes.Buffer
		writeManifestUint32Slice(&b, values)
		cur := manifestCursor{raw: b.Bytes()}
		dst := []uint32{99}
		got, err := cur.appendUint32Slice(dst)
		if err != nil {
			t.Fatalf("appendUint32Slice: %v", err)
		}
		if len(got) != 1+len(values) || got[0] != 99 || cur.pos != len(cur.raw) {
			t.Fatalf("got=%v pos=%d len=%d", got, cur.pos, len(cur.raw))
		}
		for i, want := range values {
			if got[i+1] != want {
				t.Fatalf("got[%d]=%v want %v", i+1, got[i+1], want)
			}
		}
	})

	t.Run("uint32_slice_truncated", func(t *testing.T) {
		var b bytes.Buffer
		writeManifestUint64(&b, 2)
		writeManifestUint32(&b, 1)
		cur := manifestCursor{raw: b.Bytes()}
		got, err := cur.appendUint32Slice([]uint32{7})
		if err == nil || !strings.Contains(err.Error(), "short column binary uint32 slice") {
			t.Fatalf("appendUint32Slice err=%v want short binary", err)
		}
		if fmt.Sprint(got) != "[7]" {
			t.Fatalf("got=%v want original dst", got)
		}
	})
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

func testColumnPhysicalRowReaderFixedIDConfigV7(t testing.TB) *ColumnStoreConfig {
	t.Helper()
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		ProfileSupport: ColumnStoreProfileBenchmarkRelaxed,
	})
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.ActiveManifest = &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1}
	cfg.RecoveryAuthoritativeManifest = &ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 1}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	rowAssetConfig := columnStoreRowAssetConfig(*cfg)
	if len(rowAssetConfig.Columns) != 0 {
		t.Fatalf("row asset columns=%d want zero", len(rowAssetConfig.Columns))
	}
	return &rowAssetConfig
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
