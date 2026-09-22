package collections

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnVectorGraphPhysicalRowReaderFetchesPublishedGraphRowsV2B(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{2, 0}},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1, Adjacency: []uint32{1}},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
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
	assertColumnVectorGraphPhysicalRowV2B(t, row, "doc-b", 1, 1, rows[1].Vector, 1, rows[1].Adjacency)

	var batchIDs []string
	if err := reader.FetchBatch([]int{2, 0}, &scratch, func(row columnVectorGraphPhysicalRow) error {
		batchIDs = append(batchIDs, string(row.ID))
		return nil
	}); err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if got, want := strings.Join(batchIDs, ","), strings.Join([]string{"doc-c", "doc-a"}, ","); got != want {
		t.Fatalf("batch IDs=%q want %q", got, want)
	}
	stats := reader.Stats()
	if stats.RowFetches != 1 || stats.BatchFetches != 1 || stats.RowsFetched != 3 {
		t.Fatalf("stats=%+v want one row fetch, one batch fetch, three fetched rows", stats)
	}
}

func columnGraphRebuildInputByIDV2B(tb testing.TB, rows []columnGraphRebuildInputRowV2A, id string) columnGraphRebuildInputRowV2A {
	tb.Helper()
	for _, row := range rows {
		if row.id == id {
			return row
		}
	}
	tb.Fatalf("missing input row id=%q", id)
	return columnGraphRebuildInputRowV2A{}
}

func columnVectorGraphAssetRowsFromSyntheticV2B(rows []columnGraphRebuildInputRowV2A) []columnVectorGraphAssetRow {
	out := make([]columnVectorGraphAssetRow, len(rows))
	for i, row := range rows {
		invNorm, err := columnVectorGraphInvNorm(row.vector)
		if err != nil {
			panic(err)
		}
		var adjacency []uint32
		if len(rows) > 1 {
			adjacency = []uint32{uint32((i + 1) % len(rows))}
		}
		out[i] = columnVectorGraphAssetRow{ID: []byte(row.id), Vector: append([]float32(nil), row.vector...), InvNorm: invNorm, Adjacency: adjacency}
	}
	return out
}

func TestColumnVectorGraphPhysicalRowReaderOpensEmptyPublishedGraphV2B(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, nil)
	defer func() { _ = d.Close() }()

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
	if !errors.Is(err, errColumnPhysicalRowOrdinalOutOfBounds) {
		t.Fatalf("FetchRow empty err=%v want row_count bounds", err)
	}
}

func TestColumnVectorGraphAdjacencyBoundsRejectsBadOrdinalV2B(t *testing.T) {
	err := validateColumnVectorGraphAdjacencyOrdinal("embedding_graph", 7, 0, 2, 2)
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("validateColumnVectorGraphAdjacencyOrdinal err=%v want adjacency bounds sentinel", err)
	}
	if err := validateColumnVectorGraphAdjacencyOrdinal("embedding_graph", 7, 0, 1, 2); err != nil {
		t.Fatalf("validateColumnVectorGraphAdjacencyOrdinal valid edge err=%v", err)
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
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("FetchRow err=%v want adjacency bounds sentinel", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderUncheckedSkipsAdjacencyBoundsV2B(t *testing.T) {
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
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("FetchRow err=%v want adjacency bounds sentinel", err)
	}
	row, err := reader.fetchRowUnchecked(0, &scratch)
	if err != nil {
		t.Fatalf("fetchRowUnchecked: %v", err)
	}
	if !slices.Equal(row.Adjacency, []uint32{2}) {
		t.Fatalf("unchecked adjacency=%v want invalid edge retained for search-time validation", row.Adjacency)
	}
	var batchRows int
	err = reader.fetchBatchUnchecked([]int{0}, &scratch, func(row columnVectorGraphPhysicalRow) error {
		if !slices.Equal(row.Adjacency, []uint32{2}) {
			t.Fatalf("unchecked batch adjacency=%v want invalid edge retained", row.Adjacency)
		}
		batchRows++
		return nil
	})
	if err != nil {
		t.Fatalf("fetchBatchUnchecked: %v", err)
	}
	if batchRows != 1 {
		t.Fatalf("unchecked batch rows=%d want 1", batchRows)
	}
}

func TestColumnVectorGraphPhysicalRowReaderUncheckedPreservesValidationV2B(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 0, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnPhysicalRowReaderScratch
	_, err = reader.fetchRowUnchecked(0, &scratch)
	if err == nil || !strings.Contains(err.Error(), "invalid inv_norm") {
		t.Fatalf("fetchRowUnchecked err=%v want inv_norm validation failure", err)
	}
	err = reader.fetchBatchUnchecked([]int{0}, &scratch, func(columnVectorGraphPhysicalRow) error {
		t.Fatal("visitor should not run for invalid graph row")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid inv_norm") {
		t.Fatalf("fetchBatchUnchecked err=%v want inv_norm validation failure", err)
	}
	err = reader.fetchBatchUnchecked([]int{0}, &scratch, nil)
	if !errors.Is(err, errColumnVectorGraphPhysicalRowReaderBatchVisitorNil) {
		t.Fatalf("fetchBatchUnchecked nil visitor err=%v want sentinel", err)
	}
	err = reader.FetchBatch([]int{0}, &scratch, nil)
	if !errors.Is(err, errColumnVectorGraphPhysicalRowReaderBatchVisitorNil) {
		t.Fatalf("FetchBatch nil visitor err=%v want sentinel", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderRejectsMalformedGraphRowsV2B(t *testing.T) {
	reader := &columnVectorGraphPhysicalRowReader{
		def:    VectorIndexDefinition{Name: "embedding_graph", Dimensions: 3},
		reader: &columnPhysicalRowReader{totalRows: 8},
	}
	row := func(vector []float32, invNorm float32) columnPhysicalRowReaderRow {
		return columnPhysicalRowReaderRow{
			Ordinal: 7,
			ID:      []byte("doc-bad"),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vector},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: invNorm},
				{Type: ColumnStoreValueAdjacencyList, Present: true},
			},
		}
	}
	t.Run("vector dims", func(t *testing.T) {
		_, err := reader.graphRowFromPhysicalRow(row([]float32{1, 0}, 1), reader.RowCount())
		if err == nil || !strings.Contains(err.Error(), "vector dims=2 want 3") {
			t.Fatalf("graphRowFromPhysicalRow err=%v want vector dims failure", err)
		}
	})
	t.Run("missing projected value", func(t *testing.T) {
		missing := row([]float32{1, 0, 0}, 1)
		missing.Values[columnVectorGraphPhysicalRowValueVector].Present = false
		_, err := reader.graphRowFromPhysicalRow(missing, reader.RowCount())
		if err == nil || !strings.Contains(err.Error(), "missing graph value") {
			t.Fatalf("graphRowFromPhysicalRow err=%v want missing value failure", err)
		}
	})
	for _, tc := range []struct {
		name string
		inv  float32
	}{
		{name: "zero", inv: 0},
		{name: "nan", inv: float32(math.NaN())},
		{name: "positive infinity", inv: float32(math.Inf(1))},
		{name: "negative infinity", inv: float32(math.Inf(-1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reader.graphRowFromPhysicalRow(row([]float32{1, 0, 0}, tc.inv), reader.RowCount())
			if err == nil || !strings.Contains(err.Error(), "invalid inv_norm") {
				t.Fatalf("graphRowFromPhysicalRow err=%v want invalid inv_norm failure", err)
			}
		})
	}
	t.Run("nil underlying reader", func(t *testing.T) {
		nilReader := &columnVectorGraphPhysicalRowReader{
			def: VectorIndexDefinition{Name: "embedding_graph", Dimensions: 3},
		}
		_, err := nilReader.FetchRow(0, nil)
		if !errors.Is(err, errNilColumnVectorGraphPhysicalRowReader) {
			t.Fatalf("FetchRow err=%v want nil-reader sentinel", err)
		}
	})
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
	if !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want manifest mismatch", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderRejectsManifestRowCountMismatchV2B(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithManifestRowsV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	}, 3)
	defer func() { _ = d.Close() }()

	_, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if !errors.Is(err, errColumnVectorGraphManifestMismatch) || !strings.Contains(err.Error(), "manifest row_count=3 physical_row_count=2") {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want row_count manifest mismatch", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderAllowsBaseMutationManifestPartsV2B(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithMutationPartV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()

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
	row, err := reader.FetchRow(0, &scratch)
	if err != nil {
		t.Fatalf("FetchRow: %v", err)
	}
	assertColumnVectorGraphPhysicalRowV2B(t, row, "doc-a", 0, 0, []float32{1, 0, 0}, 1, []uint32{1})
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
	if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphUnsupportedVisibility || !status.RebuildNeeded {
		t.Fatalf("status=%+v want stale graph unsupported visibility", status)
	}
	_, err = col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want stale graph mismatch", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderRejectsNonRecoveryAuthoritativeManifestV2B(t *testing.T) {
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

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	badCatalog := catalog.copy()
	badMeta := copyCollectionMeta(badCatalog.meta)
	badCfg := badMeta.Options.ColumnStore.copy()
	recovery := *badCfg.RecoveryAuthoritativeManifest
	recovery.Checksum++
	badCfg.RecoveryAuthoritativeManifest = &recovery
	badMeta.Options.ColumnStore = &badCfg
	badCatalog.meta = badMeta
	systemRoot := snapshotSystemRoot(snap)
	commitSeq := snapshotCommitSeq(snap)
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	col.catalogMu.Lock()
	col.catalog = badCatalog
	col.catalogSystemRoot = systemRoot
	col.catalogCommitSeq = commitSeq
	col.catalogMu.Unlock()

	_, err = col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err == nil || !strings.Contains(err.Error(), "active recovery-authoritative column manifest") {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want recovery-authoritative manifest failure", err)
	}
}

func TestColumnVectorGraphPhysicalRowReaderWarmScratchHotFetchZeroAllocsV2B(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{2, 0}},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1, Adjacency: []uint32{1}},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
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
	var fetchErr error
	allocs := testing.AllocsPerRun(1000, func() {
		if fetchErr != nil {
			return
		}
		row, err := reader.FetchRow(1, &scratch)
		if err != nil {
			fetchErr = err
			return
		}
		columnPhysicalScanBenchSum += int64(row.Adjacency[0])
	})
	if fetchErr != nil {
		t.Fatalf("FetchRow: %v", fetchErr)
	}
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
	input := columnVectorGraphAssetRowsFromSyntheticV2B(columnGraphRebuildSyntheticRowsV2A(rows, dims))
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(b, dims, m, input)
	defer func() { _ = d.Close() }()
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
		ordinal := (i * 131) % rows
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
	return publishColumnVectorGraphPhysicalReaderTestAssetWithMetaAndManifestRowsV2B(tb, rows, len(rows), mutateMeta)
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithManifestRowsV2B(tb testing.TB, rows []columnVectorGraphAssetRow, manifestRows int) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return publishColumnVectorGraphPhysicalReaderTestAssetWithMetaAndManifestRowsV2B(tb, rows, manifestRows, nil)
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithMetaAndManifestRowsV2B(tb testing.TB, rows []columnVectorGraphAssetRow, manifestRows int, mutateMeta func(CollectionMeta) CollectionMeta) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return publishColumnVectorGraphPhysicalReaderTestAssetWithMetaManifestRowsAndBaseAssetsV2B(tb, rows, manifestRows, mutateMeta, nil)
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(tb testing.TB, dims, m int, rows []columnVectorGraphAssetRow) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return publishColumnVectorGraphPhysicalReaderTestAssetWithShapeMetaManifestRowsAndBaseAssetsV2B(tb, dims, m, rows, len(rows), nil, nil)
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithMutationPartV2B(tb testing.TB, rows []columnVectorGraphAssetRow) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return publishColumnVectorGraphPhysicalReaderTestAssetWithMetaManifestRowsAndBaseAssetsV2B(tb, rows, len(rows), nil, func(baseCfg ColumnStoreConfig) []ColumnPreparedAsset {
		return []ColumnPreparedAsset{{
			Ref: ColumnAssetRef{
				Kind:       ColumnAssetKindTCS1PartImage,
				Namespace:  baseCfg.AssetManager.Namespace,
				Generation: 2,
				PartID:     99,
				FileID:     99,
				Offset:     0,
				Length:     1,
				Checksum:   99,
			},
			Bytes:        1,
			PublishID:    99,
			GenerationID: 2,
			Reason:       string(ColumnPublishOperationUpdate),
		}}
	})
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithMetaManifestRowsAndBaseAssetsV2B(tb testing.TB, rows []columnVectorGraphAssetRow, manifestRows int, mutateMeta func(CollectionMeta) CollectionMeta, baseAssets func(ColumnStoreConfig) []ColumnPreparedAsset) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return publishColumnVectorGraphPhysicalReaderTestAssetWithShapeMetaManifestRowsAndBaseAssetsV2B(tb, 3, 2, rows, manifestRows, mutateMeta, baseAssets)
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithShapeMetaManifestRowsAndBaseAssetsV2B(tb testing.TB, dims, m int, rows []columnVectorGraphAssetRow, manifestRows int, mutateMeta func(CollectionMeta) CollectionMeta, baseAssets func(ColumnStoreConfig) []ColumnPreparedAsset) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	baseCfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(dims))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(dims, m)
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, rows)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	var assets []ColumnPreparedAsset
	if baseAssets != nil {
		assets = baseAssets(*baseCfg)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, manifestIdentity := testColumnGraphManifestRecordsWithBaseAssetsV2B(tb, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, manifestRows, assets)
	graph := graphManifestFromRecords1918(tb, records, def)
	operation := ColumnPublishOperationInsert
	if len(assets) != 0 {
		operation = ColumnPublishOperationUpdate
	}
	var payloadBytes int64
	for _, asset := range assets {
		payloadBytes += asset.Bytes
	}
	stateRows := columnVectorGraphStateRowsForTest1987(rows, manifestRows, dims, columnVectorGraphExpectedAdjacencyLayerCountFromAssetRows1989(tb, rows))
	records, manifestIdentity = appendCompleteVectorIndexStateForGraphTest1987(tb, d, "docs", *baseCfg, def, graph, records, manifestIdentity, ColumnPublishManifestEncodeInput{
		Collection:        "docs",
		ColumnStore:       *baseCfg,
		Operation:         operation,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             append([]ColumnPreparedAsset(nil), assets...),
			RowCount:           manifestRows,
			ColumnPayloadBytes: payloadBytes,
		},
	}, stateRows)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    baseCfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if mutateMeta != nil {
		meta = mutateMeta(meta)
	}
	publishColumnGraphCatalogForTestV2A(tb, d, meta, manifestIdentity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col, def
}

func testColumnGraphManifestRecordsWithBaseAssetsV2B(tb testing.TB, baseCfg ColumnStoreConfig, def VectorIndexDefinition, identity ColumnManifestIdentity, ref ColumnAssetRef, assetBytes int64, rows int, assets []ColumnPreparedAsset) ([]columnManifestRecord, ColumnManifestIdentity) {
	tb.Helper()
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", baseCfg, def)
	if err != nil {
		tb.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	operation := ColumnPublishOperationInsert
	if len(assets) != 0 {
		operation = ColumnPublishOperationUpdate
	}
	var payloadBytes int64
	for _, asset := range assets {
		payloadBytes += asset.Bytes
	}
	input := ColumnPublishManifestEncodeInput{
		Collection:        "docs",
		ColumnStore:       baseCfg,
		Operation:         operation,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             append([]ColumnPreparedAsset(nil), assets...),
			RowCount:           rows,
			ColumnPayloadBytes: payloadBytes,
		},
	}
	header, err := encodeColumnManifestHeaderRecord(input, identity.Generation)
	if err != nil {
		tb.Fatalf("encodeColumnManifestHeaderRecord: %v", err)
	}
	baseRecords := []columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: header},
	}
	for _, asset := range assets {
		value, err := encodeColumnManifestPartRecord(asset)
		if err != nil {
			tb.Fatalf("encodeColumnManifestPartRecord: %v", err)
		}
		baseRecords = append(baseRecords, columnManifestRecord{
			key:   columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID),
			value: value,
		})
	}
	sortColumnManifestRecords(baseRecords)
	baseChecksum := checksumColumnManifestRecords(input, identity.Generation, baseRecords)
	record := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: identity.Generation,
		BaseManifestChecksum:   baseChecksum,
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               rows,
		AssetRef:               ref,
		AssetBytes:             assetBytes,
	}
	encoded, err := encodeColumnVectorGraphManifestRecord(record)
	if err != nil {
		tb.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	records := append(baseRecords, columnManifestRecord{key: columnVectorGraphManifestRecordKey(def.Name), value: encoded})
	sortColumnManifestRecords(records)
	identity.Checksum = checksumColumnManifestRecords(input, identity.Generation, records)
	return records, identity
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
	if !slices.Equal(row.Adjacency, adjacency) {
		tb.Fatalf("adjacency=%v want %v", row.Adjacency, adjacency)
	}
}
