package collections

import (
	"fmt"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnVectorGraphPhysicalRowReaderFetchesPublishedGraphRowsV2B(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	reader, err := reopenedCol.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if got, want := reader.RowCount(), len(rows); got != want {
		t.Fatalf("RowCount=%d want %d", got, want)
	}
	scratch := columnPhysicalRowReaderScratch{
		Values:        make([]columnDeclaredValue, 0, 3),
		Float32Values: make([]float32, 0, def.Dimensions),
		Uint32Values:  make([]uint32, 0, def.M),
	}
	row, err := reader.FetchRow(1, &scratch)
	if err != nil {
		t.Fatalf("FetchRow(1): %v", err)
	}
	assertColumnVectorGraphPhysicalRowV2B(t, row, "doc-b", 1, 1, []float32{0, 1, 0}, 1, []uint32{0, 2})

	var batchIDs []string
	if err := reader.FetchBatch([]int{2, 0}, &scratch, func(row columnVectorGraphPhysicalRow) error {
		batchIDs = append(batchIDs, string(row.ID))
		return nil
	}); err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if got, want := strings.Join(batchIDs, ","), "doc-c,doc-a"; got != want {
		t.Fatalf("batch IDs=%q want %q", got, want)
	}
	stats := reader.Stats()
	if stats.RowFetches != 1 || stats.BatchFetches != 1 || stats.RowsFetched != 3 {
		t.Fatalf("stats=%+v want one row fetch, one batch fetch, three fetched rows", stats)
	}
	if stats.CacheMisses != 1 || stats.CacheHits != 2 || stats.BlockEvictions != 0 {
		t.Fatalf("cache stats=%+v want one graph block miss, two hits, no evictions", stats)
	}
}

func TestColumnVectorGraphPhysicalRowReaderOpensEmptyPublishedGraphV2B(t *testing.T) {
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, nil)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex empty collection: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if got := reader.RowCount(); got != 0 {
		t.Fatalf("RowCount=%d want 0", got)
	}
	var scratch columnPhysicalRowReaderScratch
	_, err = reader.FetchRow(0, &scratch)
	if err == nil || !strings.Contains(err.Error(), "outside row_count=0") {
		t.Fatalf("FetchRow empty err=%v want row_count bounds", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderRejectsBadAdjacencyOrdinalV2B(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{2}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnPhysicalRowReaderScratch
	_, err = reader.FetchRow(0, &scratch)
	if err == nil || !strings.Contains(err.Error(), "adjacency[0]=2 outside row_count=2") {
		t.Fatalf("FetchRow err=%v want adjacency bounds failure", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderRejectsDefinitionMismatchV2B(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithMetaV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	}, func(meta CollectionMeta) CollectionMeta {
		mismatched := columnGraphRebuildVectorIndexDefinitionV2A(3, 2)
		mismatched.Dimensions = 4
		meta.VectorIndexes = []VectorIndexDefinition{mismatched}
		return meta
	})
	defer func() { _ = d.Close() }()

	_, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err == nil || !strings.Contains(err.Error(), "graph manifest does not match vector index definition") {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want manifest mismatch", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderRejectsStaleGraphAfterMutationV2B(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{{id: "doc-c", vector: []float32{0, 0, 1}}})

	status, err = col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded {
		t.Fatalf("status=%+v want stale graph mismatch", status)
	}
	_, err = col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err == nil || !strings.Contains(err.Error(), "graph manifest does not match vector index definition") {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want stale graph mismatch", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderWarmScratchHotFetchZeroAllocsV2B(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	scratch := columnPhysicalRowReaderScratch{
		Values:        make([]columnDeclaredValue, 0, 3),
		Float32Values: make([]float32, 0, def.Dimensions),
		Uint32Values:  make([]uint32, 0, def.M),
	}
	if _, err := reader.FetchRow(1, &scratch); err != nil {
		t.Fatalf("warm FetchRow: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		row, err := reader.FetchRow(1, &scratch)
		if err != nil {
			t.Fatalf("FetchRow: %v", err)
		}
		columnPhysicalScanBenchSum += int64(row.Adjacency[0])
	})
	if allocs != 0 {
		t.Fatalf("hot FetchRow allocs=%v want zero", allocs)
	}
}

func BenchmarkColumnVectorGraphPhysicalRowReaderFetchV2B(b *testing.B) {
	const (
		rows = 1024
		dims = 128
		m    = 16
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(b, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	scratch := columnPhysicalRowReaderScratch{
		Values:        make([]columnDeclaredValue, 0, 3),
		Float32Values: make([]float32, 0, dims),
		Uint32Values:  make([]uint32, 0, m),
	}
	if _, err := reader.FetchRow(0, &scratch); err != nil {
		b.Fatalf("warm FetchRow: %v", err)
	}
	b.ReportAllocs()
	baseStats := reader.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ordinal := (i * 131) & (rows - 1)
		row, err := reader.FetchRow(ordinal, &scratch)
		if err != nil {
			b.Fatalf("FetchRow(%d): %v", ordinal, err)
		}
		columnPhysicalScanBenchSum += int64(row.Adjacency[0])
	}
	b.StopTimer()
	stats := reader.Stats()
	b.ReportMetric(float64(baseStats.Rows), "graph_rows")
	b.ReportMetric(float64(baseStats.Granules), "graph_granules")
	b.ReportMetric(float64(baseStats.OpenPhysicalBytesRead), "open_physical_B")
	if baseStats.Rows > 0 {
		b.ReportMetric(float64(baseStats.OpenPhysicalBytesRead)/float64(baseStats.Rows), "asset_B/row")
	}
	b.ReportMetric(float64(stats.CacheHits-baseStats.CacheHits)/float64(b.N), "cache_hits/op")
	b.ReportMetric(float64(stats.CacheMisses-baseStats.CacheMisses)/float64(b.N), "cache_misses/op")
	b.ReportMetric(float64(stats.DecodedBlocks-baseStats.DecodedBlocks)/float64(b.N), "decoded_blocks/op")
	b.ReportMetric(float64(stats.GranulesTouched-baseStats.GranulesTouched)/float64(b.N), "granules_touched/op")
	b.ReportMetric(float64(stats.PhysicalBytesRead-baseStats.PhysicalBytesRead)/float64(b.N), "physical_B/op")
	b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
}

func publishColumnVectorGraphPhysicalReaderTestAssetV2B(tb testing.TB, rows []columnVectorGraphAssetRow) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return publishColumnVectorGraphPhysicalReaderTestAssetWithMetaV2B(tb, rows, nil)
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithMetaV2B(tb testing.TB, rows []columnVectorGraphAssetRow, mutateMeta func(CollectionMeta) CollectionMeta) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	baseCfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(3))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 2)
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, rows)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsV2A(tb, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(3),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if mutateMeta != nil {
		meta = mutateMeta(meta)
	}
	publishColumnGraphCatalogForTestV2A(tb, d, meta, identity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col, def
}

func assertColumnVectorGraphPhysicalRowV2B(tb testing.TB, row columnVectorGraphPhysicalRow, id string, ordinal, rowIndex int, vector []float32, invNorm float32, adjacency []uint32) {
	tb.Helper()
	if string(row.ID) != id || row.Ordinal != ordinal {
		tb.Fatalf("row id=%q ordinal=%d want id=%q ordinal=%d", string(row.ID), row.Ordinal, id, ordinal)
	}
	if row.RowIndex != rowIndex {
		tb.Fatalf("row index=%d want %d", row.RowIndex, rowIndex)
	}
	if len(row.Vector) != len(vector) {
		tb.Fatalf("vector len=%d want %d", len(row.Vector), len(vector))
	}
	for i := range vector {
		if math.Abs(float64(row.Vector[i]-vector[i])) > 1e-6 {
			tb.Fatalf("vector[%d]=%v want %v", i, row.Vector[i], vector[i])
		}
	}
	if math.Abs(float64(row.InvNorm-invNorm)) > 1e-6 {
		tb.Fatalf("invNorm=%v want %v", row.InvNorm, invNorm)
	}
	if got, want := fmt.Sprint(row.Adjacency), fmt.Sprint(adjacency); got != want {
		tb.Fatalf("adjacency=%s want %s", got, want)
	}
}
