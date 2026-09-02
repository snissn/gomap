package collections

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type columnVectorGraphSearchTopologyParityMode2091 string

type columnVectorGraphSearchTopologyParityBoundary2091 string

const (
	columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091 columnVectorGraphSearchTopologyParityMode2091 = "legacy_graph_row_direct"
	columnVectorGraphSearchTopologyParityModeCurrentPrepared2091      columnVectorGraphSearchTopologyParityMode2091 = "current_prepared_typed_column"

	columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091 columnVectorGraphSearchTopologyParityBoundary2091 = "graph_only"
	columnVectorGraphSearchTopologyParityBoundaryResultID2091  columnVectorGraphSearchTopologyParityBoundary2091 = "result_id"
)

type columnVectorGraphSearchTopologyParityShape2091 struct {
	rows         int
	dims         int
	degree       int
	topK         int
	efSearch     int
	queryOrdinal int
}

func columnVectorGraphSearchTopologyParityTestShape2091() columnVectorGraphSearchTopologyParityShape2091 {
	return columnVectorGraphSearchTopologyParityShape2091{rows: 192, dims: 32, degree: 8, topK: 6, efSearch: 48, queryOrdinal: 17}
}

func columnVectorGraphSearchTopologyParityProductionShape2091() columnVectorGraphSearchTopologyParityShape2091 {
	return columnVectorGraphSearchTopologyParityShape2091{rows: 8192, dims: 128, degree: 16, topK: 10, efSearch: 128, queryOrdinal: 4096}
}

func TestColumnVectorGraphSearchTopologyParity2091(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("topology-parity prepared current-format search requires mmap_direct prepared views")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	legacyClose, legacy, legacyQuery := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091)
	defer legacyClose()
	currentClose, current, currentQuery := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer currentClose()
	if !float32SlicesEqual1782(legacyQuery, currentQuery) {
		t.Fatalf("query mismatch legacy=%v current=%v", legacyQuery, currentQuery)
	}
	assertColumnVectorGraphSearchTopologyFixtureParity2091(t, rows, legacy, current)

	for _, boundary := range []columnVectorGraphSearchTopologyParityBoundary2091{
		columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091,
		columnVectorGraphSearchTopologyParityBoundaryResultID2091,
	} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			legacyResults, legacyStats := runColumnVectorGraphSearchTopologyParitySearch2091(t, legacy, legacyQuery, shape, boundary)
			currentResults, currentStats := runColumnVectorGraphSearchTopologyParitySearch2091(t, current, currentQuery, shape, boundary)
			if mismatch := columnGraphNativeSearchResultsMismatchV3(currentResults, legacyResults); mismatch != "" {
				t.Fatalf("%s results mismatch: %s", boundary, mismatch)
			}
			assertColumnVectorGraphSearchTopologyParityWork2091(t, boundary, legacyStats, currentStats)
			assertColumnVectorGraphSearchTopologyParityLegacyStats2091(t, boundary, legacyStats)
			assertColumnVectorGraphSearchTopologyParityCurrentStats2091(t, boundary, currentStats)
		})
	}
}

func BenchmarkColumnVectorGraphSearchTopologyParity2091(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("topology-parity prepared current-format search requires mmap_direct prepared views")
	}
	shape := columnVectorGraphSearchTopologyParityProductionShape2091()
	for _, boundary := range []columnVectorGraphSearchTopologyParityBoundary2091{
		columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091,
		columnVectorGraphSearchTopologyParityBoundaryResultID2091,
	} {
		boundary := boundary
		b.Run("boundary="+string(boundary), func(b *testing.B) {
			for _, mode := range []columnVectorGraphSearchTopologyParityMode2091{
				columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091,
				columnVectorGraphSearchTopologyParityModeCurrentPrepared2091,
			} {
				mode := mode
				b.Run("mode="+string(mode), func(b *testing.B) {
					benchmarkColumnVectorGraphSearchTopologyParity2091(b, shape, boundary, mode)
				})
			}
		})
	}
}

func benchmarkColumnVectorGraphSearchTopologyParity2091(b *testing.B, shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091, mode columnVectorGraphSearchTopologyParityMode2091) {
	b.Helper()
	rows := columnVectorGraphSearchTopologyParityRows2091(b, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(b, shape, rows, mode)
	defer closeFn()
	var scratch columnVectorGraphNativeSearchScratch
	opts := columnVectorGraphSearchTopologyParityOptions2091(shape, boundary)
	warm, warmStats, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	if len(warm) == 0 {
		b.Fatalf("warm SearchCosine returned no results")
	}
	assertColumnVectorGraphSearchTopologyParityModeStats2091(b, boundary, mode, warmStats)
	baseReaderStats := reader.Stats()
	var totals columnVectorGraphNativeSearchStats
	var checksum int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, stats, err := reader.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		if len(got) == 0 {
			b.Fatalf("SearchCosine returned no results")
		}
		checksum += int64(got[0].Ordinal)
		addColumnVectorGraphSearchTopologyParityStats2091(&totals, stats)
	}
	b.StopTimer()
	columnPhysicalScanBenchSum += checksum
	reportColumnVectorGraphSearchTopologyParityMetrics2091(b, shape, boundary, mode, b.N, baseReaderStats, reader.Stats(), totals)
}

func columnVectorGraphSearchTopologyParityRows2091(tb testing.TB, shape columnVectorGraphSearchTopologyParityShape2091) []columnVectorGraphAssetRow {
	tb.Helper()
	if shape.queryOrdinal < 0 || shape.queryOrdinal >= shape.rows {
		tb.Fatalf("query ordinal=%d out of range rows=%d", shape.queryOrdinal, shape.rows)
	}
	return columnVectorGraphNativeSearchBenchAssetRowsV3(tb, shape.rows, shape.dims, shape.degree)
}

func openColumnVectorGraphSearchTopologyParityReader2091(tb testing.TB, shape columnVectorGraphSearchTopologyParityShape2091, rows []columnVectorGraphAssetRow, mode columnVectorGraphSearchTopologyParityMode2091) (func(), *columnVectorGraphPhysicalRowReader, []float32) {
	tb.Helper()
	var d *backenddb.DB
	var col *Collection
	var def VectorIndexDefinition
	switch mode {
	case columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091:
		d, col, def = publishColumnVectorGraphPhysicalReaderTestAssetWithShapeAndAdjacencyState1989(tb, shape.dims, shape.degree, cloneColumnVectorGraphTopologyParityRows2091(rows))
	case columnVectorGraphSearchTopologyParityModeCurrentPrepared2091:
		d, col, def = publishColumnVectorGraphCurrentPreparedTopologyParityCollection2091(tb, shape.dims, shape.degree, cloneColumnVectorGraphTopologyParityRows2091(rows))
	default:
		tb.Fatalf("unsupported topology-parity mode %q", mode)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("openColumnVectorGraphPhysicalRowReader mode=%s: %v", mode, err)
	}
	if mode == columnVectorGraphSearchTopologyParityModeCurrentPrepared2091 {
		if reader.preparedSearch == nil || !reader.preparedSearch.ready() {
			_ = reader.Close()
			_ = d.Close()
			tb.Fatalf("current prepared reader preparedSearch=%v ready=%v", reader.preparedSearch != nil, reader.preparedSearch != nil && reader.preparedSearch.ready())
		}
		if stats := reader.Stats(); stats.Rows != 0 {
			_ = reader.Close()
			_ = d.Close()
			tb.Fatalf("current prepared reader graph rows=%d want 0", stats.Rows)
		}
	}
	query := append([]float32(nil), rows[shape.queryOrdinal].Vector...)
	closeFn := func() {
		_ = reader.Close()
		_ = d.Close()
	}
	return closeFn, reader, query
}

func publishColumnVectorGraphCurrentPreparedTopologyParityCollection2091(tb testing.TB, dims, m int, rows []columnVectorGraphAssetRow) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	input := make([]columnGraphRebuildInputRowV2A, len(rows))
	for i := range rows {
		input[i] = columnGraphRebuildInputRowV2A{id: string(rows[i].ID), vector: append([]float32(nil), rows[i].Vector...)}
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(tb, dims, m, input)

	snap := d.AcquireSnapshot()
	if snap == nil {
		_ = d.Close()
		tb.Fatal("AcquireSnapshot returned nil")
	}
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatal("collection docs missing")
	}
	baseMeta := catalog.meta
	cfg := baseMeta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatalf("column store metadata missing active manifest: %+v", cfg)
	}
	rootName := collectionColumnManifestRootName(baseMeta.Name)
	baseManifestRootID := catalog.rootID(rootName)
	if baseManifestRootID == 0 {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatalf("missing base manifest root %q", rootName)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, baseManifestRootID)
	if err != nil {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	state := snap.State()
	if state == nil {
		_ = snap.Close()
		_ = d.Close()
		tb.Fatal("snapshot state is nil")
	}
	baseCommitSeq := state.CommitSeq
	baseSystemRoot := state.SystemRootPageID
	_ = snap.Close()

	stateRows := cloneColumnVectorGraphTopologyParityRows2091(rows)
	if err := col.assignColumnVectorGraphRowRefsFromBaseManifest(baseMeta.Name, *cfg, records, manifest.Generation, stateRows); err != nil {
		_ = d.Close()
		tb.Fatalf("assignColumnVectorGraphRowRefsFromBaseManifest: %v", err)
	}
	intent, err := col.newCollectionRebuildVectorIndexCommandWALIntent(def.Name, nil)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("newCollectionRebuildVectorIndexCommandWALIntent: %v", err)
	}
	rootNames := []string{rootName}
	baseRootIDs := map[string]uint64{rootName: baseManifestRootID}
	var updatedMeta CollectionMeta
	buildContextDeltas := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
		_, nextRecords, identity, prepareErr := prepareColumnVectorGraphRebuildManifest(baseMeta.Name, *cfg, baseMeta.VectorIndexes, def, manifest, records, ctx.AppliedCommandLSN, stateRows, d.ColumnAssetRootDir())
		if prepareErr != nil {
			return nil, prepareErr
		}
		delta := ColumnManifestRootDelta{
			RootName:       rootName,
			BaseRootID:     baseManifestRootID,
			StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
			Identity:       identity,
			IdentityRecord: encodeColumnManifestIdentityRecordArray(identity),
			Records:        nextRecords,
		}
		ordered, orderedErr := delta.OrderedRootDeltaPublishInput()
		if orderedErr != nil {
			return nil, orderedErr
		}
		var metaErr error
		updatedMeta, metaErr = columnGraphRebuildUpdatedMeta(baseMeta, identity, ctx.AppliedCommandLSN)
		if metaErr != nil {
			return nil, metaErr
		}
		return []backenddb.OrderedRootDeltaPublishInput{ordered}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if updatedMeta.Name == "" {
			return nil, fmt.Errorf("collections: topology-parity prepared manifest did not prepare updated metadata at applied_lsn=%d", ctx.AppliedCommandLSN)
		}
		return col.buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	}
	if _, _, err := d.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(nil, intent, buildContextDeltas, buildSystemDelta); err != nil {
		_ = d.Close()
		tb.Fatalf("publish topology-parity prepared manifest: %v", err)
	}
	opened, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection current prepared topology parity: %v", err)
	}
	return d, opened, def
}

func cloneColumnVectorGraphTopologyParityRows2091(rows []columnVectorGraphAssetRow) []columnVectorGraphAssetRow {
	out := make([]columnVectorGraphAssetRow, len(rows))
	for i := range rows {
		out[i] = rows[i]
		out[i].ID = append([]byte(nil), rows[i].ID...)
		out[i].Vector = append([]float32(nil), rows[i].Vector...)
		out[i].Adjacency = append([]uint32(nil), rows[i].Adjacency...)
	}
	return out
}

func columnVectorGraphSearchTopologyParityOptions2091(shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091) columnVectorGraphNativeSearchOptions {
	opts := columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, ScoreBatchMode: columnVectorGraphScoreBatchModeScalar}
	if boundary == columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091 {
		opts.OmitResultMaterialization = true
	}
	return opts
}

func runColumnVectorGraphSearchTopologyParitySearch2091(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, query []float32, shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats) {
	tb.Helper()
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphSearchTopologyParityOptions2091(shape, boundary), &scratch)
	if err != nil {
		tb.Fatalf("SearchCosine boundary=%s: %v", boundary, err)
	}
	if len(got) == 0 {
		tb.Fatalf("SearchCosine boundary=%s returned no results", boundary)
	}
	return cloneColumnVectorGraphPreparedResults2045(got), stats
}

func assertColumnVectorGraphSearchTopologyFixtureParity2091(tb testing.TB, rows []columnVectorGraphAssetRow, legacy, current *columnVectorGraphPhysicalRowReader) {
	tb.Helper()
	if legacy == nil || current == nil || current.preparedSearch == nil {
		tb.Fatalf("legacy/current readers not ready legacy=%v current=%v prepared=%v", legacy != nil, current != nil, current != nil && current.preparedSearch != nil)
	}
	legacyPlan, err := newColumnVectorGraphSearchPlan(legacy)
	if err != nil {
		tb.Fatalf("new legacy search plan: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	for ordinal, row := range rows {
		legacyAdjacency, _, err := legacy.rawCandidateAdjacencyWithDirectView(legacyPlan, nil, ordinal, &scratch)
		if err != nil {
			tb.Fatalf("legacy raw adjacency ordinal=%d: %v", ordinal, err)
		}
		legacyLayer0, err := columnVectorGraphAdjacencyLayer(legacyAdjacency, 0)
		if err != nil {
			tb.Fatalf("legacy layer0 ordinal=%d: %v", ordinal, err)
		}
		wantLayer0, err := columnVectorGraphAdjacencyLayer(row.Adjacency, 0)
		if err != nil {
			tb.Fatalf("fixture layer0 ordinal=%d: %v", ordinal, err)
		}
		currentLayer0, outcome, err := current.preparedSearch.adjacencyLayerForOrdinal(ordinal, 0)
		if err != nil {
			tb.Fatalf("current prepared adjacency ordinal=%d: %v", ordinal, err)
		}
		if outcome != columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect {
			tb.Fatalf("current adjacency ordinal=%d outcome=%s want prepared CSR mmap direct", ordinal, outcome)
		}
		if !columnVectorGraphUint32SlicesEqual(legacyLayer0, wantLayer0) || !columnVectorGraphUint32SlicesEqual(currentLayer0, wantLayer0) {
			tb.Fatalf("ordinal=%d topology mismatch legacy=%v current=%v want=%v", ordinal, legacyLayer0, currentLayer0, wantLayer0)
		}
		currentVector, reason, ok := current.preparedSearch.vector.vectorForOrdinal(ordinal)
		if !ok {
			tb.Fatalf("current prepared vector ordinal=%d unavailable reason=%s", ordinal, reason)
		}
		if !float32SlicesEqual1782(currentVector, row.Vector) {
			tb.Fatalf("ordinal=%d vector mismatch", ordinal)
		}
		currentID, ok := current.preparedSearch.documentIDForOrdinal(ordinal)
		if !ok || !bytes.Equal(currentID, row.ID) {
			tb.Fatalf("ordinal=%d document id=%q ok=%v want %q", ordinal, string(currentID), ok, string(row.ID))
		}
	}
}

func assertColumnVectorGraphSearchTopologyParityWork2091(tb testing.TB, boundary columnVectorGraphSearchTopologyParityBoundary2091, legacy, current columnVectorGraphNativeSearchStats) {
	tb.Helper()
	checks := []struct {
		name         string
		legacyValue  uint64
		currentValue uint64
	}{
		{name: "candidate_rows", legacyValue: legacy.CandidateRows, currentValue: current.CandidateRows},
		{name: "candidates", legacyValue: legacy.Candidates, currentValue: current.Candidates},
		{name: "edges", legacyValue: legacy.Edges, currentValue: current.Edges},
		{name: "visited_nodes", legacyValue: legacy.VisitedNodes, currentValue: current.VisitedNodes},
		{name: "visited_edges", legacyValue: legacy.VisitedEdges, currentValue: current.VisitedEdges},
		{name: "candidate_fetches", legacyValue: legacy.CandidateFetches, currentValue: current.CandidateFetches},
		{name: "expansion_fetches", legacyValue: legacy.ExpansionFetches, currentValue: current.ExpansionFetches},
		{name: "result_fetches", legacyValue: legacy.ResultFetches, currentValue: current.ResultFetches},
		{name: "score_batch_candidates", legacyValue: legacy.ScoreBatchCandidates, currentValue: current.ScoreBatchCandidates},
		{name: "vector_bytes", legacyValue: legacy.VectorBytesRead, currentValue: current.VectorBytesRead},
		{name: "norm_bytes", legacyValue: legacy.NormBytesRead, currentValue: current.NormBytesRead},
		{name: "adjacency_bytes", legacyValue: legacy.AdjacencyBytesRead, currentValue: current.AdjacencyBytesRead},
		{name: "adjacency_expansions", legacyValue: legacy.AdjacencyExpansions, currentValue: current.AdjacencyExpansions},
		{name: "adjacency_prepared_csr_direct_views", legacyValue: legacy.AdjacencyPreparedCSRDirectViews, currentValue: current.AdjacencyPreparedCSRDirectViews},
		{name: "adjacency_prepared_csr_mmap_direct", legacyValue: legacy.AdjacencyPreparedCSRMmapDirectViews, currentValue: current.AdjacencyPreparedCSRMmapDirectViews},
	}
	for _, check := range checks {
		if check.legacyValue != check.currentValue {
			tb.Fatalf("%s %s legacy=%d current=%d legacyStats=%+v currentStats=%+v", boundary, check.name, check.legacyValue, check.currentValue, legacy, current)
		}
	}
	if current.ScoreBatchCalls == 0 || current.ScoreBatchCalls > legacy.ScoreBatchCalls || current.ScoreBatchMaxTileSize == 0 {
		tb.Fatalf("%s current score batches calls=%d max_tile=%d legacy_calls=%d currentStats=%+v legacyStats=%+v want prepared path to preserve candidates while allowing scalar neighbor-tile grouping", boundary, current.ScoreBatchCalls, current.ScoreBatchMaxTileSize, legacy.ScoreBatchCalls, current, legacy)
	}
}

func assertColumnVectorGraphSearchTopologyParityLegacyStats2091(tb testing.TB, boundary columnVectorGraphSearchTopologyParityBoundary2091, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.PreparedGraphSearchViews != 0 || stats.GraphRowFallbacks == 0 || stats.TypedColumnFallbacks == 0 || stats.VectorScratchDecodes == 0 {
		tb.Fatalf("legacy %s stats=%+v want graph-row/direct compatibility path", boundary, stats)
	}
	if stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencySourceFallbacks != 0 {
		tb.Fatalf("legacy %s stats=%+v want shared direct prepared-CSR adjacency state without adjacency graph-row fallback", boundary, stats)
	}
	if boundary == columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091 {
		if stats.ResultFetches != 0 || stats.ResultIDGraphFallbacks != 0 {
			tb.Fatalf("legacy graph-only stats=%+v want no result materialization", stats)
		}
	} else if stats.ResultFetches == 0 || stats.ResultIDGraphFallbacks != stats.ResultFetches {
		tb.Fatalf("legacy result-ID stats=%+v want graph-row result-ID compatibility fallback for each result", stats)
	}
}

func assertColumnVectorGraphSearchTopologyParityCurrentStats2091(tb testing.TB, boundary columnVectorGraphSearchTopologyParityBoundary2091, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 || stats.ResultIDGraphFallbacks != 0 {
		tb.Fatalf("current %s stats=%+v want prepared current-format path without graph rows/fallback", boundary, stats)
	}
	if stats.PreparedScoreCalls != stats.CandidateFetches || stats.VectorPreparedDirectViews != stats.CandidateFetches || stats.NormPreparedDirectViews != stats.CandidateFetches {
		tb.Fatalf("current %s stats=%+v want prepared scoring to cover all candidates", boundary, stats)
	}
	if stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencySourceFallbacks != 0 || stats.AdjacencyTypedListMmapDirectViews != 0 || stats.AdjacencyTypedListScratchDecodes != 0 {
		tb.Fatalf("current %s stats=%+v want prepared CSR adjacency with no fallback", boundary, stats)
	}
	if boundary == columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091 {
		if stats.ResultFetches != 0 || stats.ResultIDTypedBytesState != 0 {
			tb.Fatalf("current graph-only stats=%+v want no result materialization", stats)
		}
	} else if stats.ResultFetches == 0 || stats.ResultIDPreparedBytesViews != 1 || stats.ResultIDTypedBytesState != stats.ResultFetches || stats.RowRefStateResultRefs != stats.ResultFetches {
		tb.Fatalf("current result-ID stats=%+v want prepared result-ID and row-ref state", stats)
	}
}

func assertColumnVectorGraphSearchTopologyParityModeStats2091(tb testing.TB, boundary columnVectorGraphSearchTopologyParityBoundary2091, mode columnVectorGraphSearchTopologyParityMode2091, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	switch mode {
	case columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091:
		assertColumnVectorGraphSearchTopologyParityLegacyStats2091(tb, boundary, stats)
	case columnVectorGraphSearchTopologyParityModeCurrentPrepared2091:
		assertColumnVectorGraphSearchTopologyParityCurrentStats2091(tb, boundary, stats)
	default:
		tb.Fatalf("unsupported topology-parity mode %q", mode)
	}
}

func addColumnVectorGraphSearchTopologyParityStats2091(dst *columnVectorGraphNativeSearchStats, src columnVectorGraphNativeSearchStats) {
	if dst == nil {
		return
	}
	dst.CandidateRows += src.CandidateRows
	dst.Candidates += src.Candidates
	dst.Edges += src.Edges
	dst.VisitedNodes += src.VisitedNodes
	dst.VisitedEdges += src.VisitedEdges
	dst.VectorBytesRead += src.VectorBytesRead
	dst.NormBytesRead += src.NormBytesRead
	dst.AdjacencyBytesRead += src.AdjacencyBytesRead
	dst.CandidateFetches += src.CandidateFetches
	dst.ExpansionFetches += src.ExpansionFetches
	dst.ResultFetches += src.ResultFetches
	dst.ScoreBatches += src.ScoreBatches
	dst.OrdinalsGrouped += src.OrdinalsGrouped
	dst.ScoreBatchCalls += src.ScoreBatchCalls
	dst.ScoreBatchCandidates += src.ScoreBatchCandidates
	if src.ScoreBatchMaxTileSize > dst.ScoreBatchMaxTileSize {
		dst.ScoreBatchMaxTileSize = src.ScoreBatchMaxTileSize
	}
	dst.ScoreBatchOptimizedCalls += src.ScoreBatchOptimizedCalls
	dst.ScoreBatchScalarFallbackCalls += src.ScoreBatchScalarFallbackCalls
	dst.PreparedScoreCalls += src.PreparedScoreCalls
	dst.ScoreFloat64Fallbacks += src.ScoreFloat64Fallbacks
	dst.AdjacencyExpansions += src.AdjacencyExpansions
	dst.AdjacencyScratchDecodes += src.AdjacencyScratchDecodes
	dst.AdjacencyDirectViews += src.AdjacencyDirectViews
	dst.AdjacencyMmapDirectViews += src.AdjacencyMmapDirectViews
	dst.AdjacencyHeapCopyTypedViews += src.AdjacencyHeapCopyTypedViews
	dst.AdjacencyPreparedCSRDirectViews += src.AdjacencyPreparedCSRDirectViews
	dst.AdjacencyPreparedCSRMmapDirectViews += src.AdjacencyPreparedCSRMmapDirectViews
	dst.AdjacencyTypedListDirectViews += src.AdjacencyTypedListDirectViews
	dst.AdjacencyTypedListMmapDirectViews += src.AdjacencyTypedListMmapDirectViews
	dst.AdjacencyTypedListHeapCopyTypedViews += src.AdjacencyTypedListHeapCopyTypedViews
	dst.AdjacencyTypedListScratchDecodes += src.AdjacencyTypedListScratchDecodes
	dst.AdjacencyLegacyFallbacks += src.AdjacencyLegacyFallbacks
	dst.AdjacencySourceUnavailable += src.AdjacencySourceUnavailable
	dst.AdjacencySourceFallbacks += src.AdjacencySourceFallbacks
	dst.AdjacencyCertificationFailures += src.AdjacencyCertificationFailures
	dst.AdjacencyValidationFailures += src.AdjacencyValidationFailures
	dst.NormDirectViews += src.NormDirectViews
	dst.NormMmapDirectViews += src.NormMmapDirectViews
	dst.NormHeapCopyTypedViews += src.NormHeapCopyTypedViews
	dst.NormScratchDecodes += src.NormScratchDecodes
	dst.NormPreparedDirectViews += src.NormPreparedDirectViews
	dst.NormSourceUnavailable += src.NormSourceUnavailable
	dst.NormSourceFallbacks += src.NormSourceFallbacks
	dst.NormValidationFailures += src.NormValidationFailures
	dst.VectorDirectViews += src.VectorDirectViews
	dst.VectorMmapDirectViews += src.VectorMmapDirectViews
	dst.VectorHeapCopyTypedViews += src.VectorHeapCopyTypedViews
	dst.VectorScratchDecodes += src.VectorScratchDecodes
	dst.VectorPreparedDirectViews += src.VectorPreparedDirectViews
	dst.VectorPreparedIdentityMappings += src.VectorPreparedIdentityMappings
	dst.VectorPreparedRowRefMappings += src.VectorPreparedRowRefMappings
	dst.VectorCertificationFailures += src.VectorCertificationFailures
	dst.TypedColumnFallbacks += src.TypedColumnFallbacks
	dst.RowRefVectorSourceState += src.RowRefVectorSourceState
	dst.RowRefVectorSourceLegacyGraphIDs += src.RowRefVectorSourceLegacyGraphIDs
	dst.RowRefStatePreparedViews += src.RowRefStatePreparedViews
	dst.RowRefStateMmapDirectFields += src.RowRefStateMmapDirectFields
	dst.RowRefStateResultRefs += src.RowRefStateResultRefs
	dst.RowRefStateSourceUnavailable += src.RowRefStateSourceUnavailable
	dst.RowRefStateSourceFallbacks += src.RowRefStateSourceFallbacks
	dst.ResultIDPreparedBytesViews += src.ResultIDPreparedBytesViews
	dst.ResultIDTypedBytesState += src.ResultIDTypedBytesState
	dst.ResultIDGraphFallbacks += src.ResultIDGraphFallbacks
	dst.ResultIDStateValidationFailures += src.ResultIDStateValidationFailures
	dst.PreparedGraphSearchViews += src.PreparedGraphSearchViews
	dst.GraphRowFallbacks += src.GraphRowFallbacks
	dst.WavefrontSearches += src.WavefrontSearches
	if src.WavefrontWidth > dst.WavefrontWidth {
		dst.WavefrontWidth = src.WavefrontWidth
	}
	dst.WavefrontRounds += src.WavefrontRounds
	dst.WavefrontCandidatePops += src.WavefrontCandidatePops
	dst.WavefrontStagedNeighbors += src.WavefrontStagedNeighbors
	if src.WavefrontMaxTileSize > dst.WavefrontMaxTileSize {
		dst.WavefrontMaxTileSize = src.WavefrontMaxTileSize
	}
	addColumnVectorGraphNativeSearchDebugStats1979(dst, src)
	dst.TypedColumnMappedBytes = src.TypedColumnMappedBytes
	dst.TypedColumnHeapCopyBytes = src.TypedColumnHeapCopyBytes
	dst.TypedColumnDecodedBytes = src.TypedColumnDecodedBytes
	dst.TypedColumnActiveHandles = src.TypedColumnActiveHandles
	dst.NormMappedBytes = src.NormMappedBytes
	dst.NormHeapCopyBytes = src.NormHeapCopyBytes
	dst.NormDecodedBytes = src.NormDecodedBytes
	dst.NormActiveHandles = src.NormActiveHandles
}

func reportColumnVectorGraphSearchTopologyParityMetrics2091(b *testing.B, shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091, mode columnVectorGraphSearchTopologyParityMode2091, n int, baseStats, readerStats columnPhysicalRowReaderStats, searchStats columnVectorGraphNativeSearchStats) {
	b.Helper()
	if n <= 0 {
		return
	}
	denom := float64(n)
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/sec")
	}
	b.ReportMetric(2091, "topology_parity_issue")
	b.ReportMetric(1, "topology_parity_mode_"+string(mode))
	b.ReportMetric(1, "topology_parity_boundary_"+string(boundary))
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.degree), "degree")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
	b.ReportMetric(float64(shape.queryOrdinal), "query_ordinal")
	b.ReportMetric(float64(readerStats.Rows), "graph_rows")
	b.ReportMetric(float64(searchStats.CandidateRows)/denom, "candidate_rows/search")
	b.ReportMetric(float64(searchStats.Candidates)/denom, "candidates/search")
	b.ReportMetric(float64(searchStats.Edges)/denom, "edges/search")
	b.ReportMetric(float64(searchStats.VisitedNodes)/denom, "visited_nodes/search")
	b.ReportMetric(float64(searchStats.VisitedEdges)/denom, "visited_edges/search")
	b.ReportMetric(float64(searchStats.CandidateFetches)/denom, "candidate_fetches/search")
	b.ReportMetric(float64(searchStats.ExpansionFetches)/denom, "expansion_fetches/search")
	b.ReportMetric(float64(searchStats.ResultFetches)/denom, "result_fetches/search")
	b.ReportMetric(float64(searchStats.VectorBytesRead)/denom, "vector_B/search")
	b.ReportMetric(float64(searchStats.NormBytesRead)/denom, "norm_B/search")
	b.ReportMetric(float64(searchStats.AdjacencyBytesRead)/denom, "adjacency_B/search")
	b.ReportMetric(float64(searchStats.ScoreBatches)/denom, "score_batches/search")
	b.ReportMetric(float64(searchStats.ScoreBatchCalls)/denom, "score_batch_calls/search")
	b.ReportMetric(float64(searchStats.ScoreBatchCandidates)/denom, "score_batch_candidates/search")
	b.ReportMetric(float64(searchStats.ScoreBatchMaxTileSize), "score_batch_max_tile_size")
	b.ReportMetric(float64(searchStats.ScoreBatchOptimizedCalls)/denom, "score_batch_optimized/search")
	b.ReportMetric(float64(searchStats.ScoreBatchScalarFallbackCalls)/denom, "score_batch_fallback/search")
	b.ReportMetric(float64(searchStats.PreparedScoreCalls)/denom, "prepared_score_calls/search")
	b.ReportMetric(float64(searchStats.ScoreFloat64Fallbacks)/denom, "score_float64_fallbacks/search")
	reportColumnVectorGraphNativeSearchDebugMetrics1979(b, n, searchStats)
	b.ReportMetric(float64(searchStats.VectorDirectViews)/denom, "vector_direct_views/search")
	b.ReportMetric(float64(searchStats.VectorMmapDirectViews)/denom, "vector_mmap_direct/search")
	b.ReportMetric(float64(searchStats.VectorHeapCopyTypedViews)/denom, "vector_heap_copy_typed_view/search")
	b.ReportMetric(float64(searchStats.VectorScratchDecodes)/denom, "vector_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.VectorPreparedDirectViews)/denom, "vector_prepared_direct/search")
	b.ReportMetric(float64(searchStats.VectorPreparedIdentityMappings)/denom, "vector_prepared_identity_mapping/search")
	b.ReportMetric(float64(searchStats.VectorPreparedRowRefMappings)/denom, "vector_prepared_row_ref_mapping/search")
	b.ReportMetric(float64(searchStats.TypedColumnFallbacks)/denom, "typed_column_vector_fallbacks/search")
	b.ReportMetric(float64(searchStats.NormDirectViews)/denom, "norm_direct_views/search")
	b.ReportMetric(float64(searchStats.NormMmapDirectViews)/denom, "norm_mmap_direct/search")
	b.ReportMetric(float64(searchStats.NormHeapCopyTypedViews)/denom, "norm_heap_copy_typed_view/search")
	b.ReportMetric(float64(searchStats.NormScratchDecodes)/denom, "norm_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.NormPreparedDirectViews)/denom, "norm_prepared_direct/search")
	b.ReportMetric(float64(searchStats.NormSourceFallbacks)/denom, "norm_source_fallbacks/search")
	b.ReportMetric(float64(searchStats.AdjacencyExpansions)/denom, "adjacency_expansions/search")
	b.ReportMetric(float64(searchStats.AdjacencyDirectViews)/denom, "adjacency_direct_views/search")
	b.ReportMetric(float64(searchStats.AdjacencyMmapDirectViews)/denom, "adjacency_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyScratchDecodes)/denom, "adjacency_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.AdjacencyPreparedCSRDirectViews)/denom, "adjacency_prepared_csr_direct_views/search")
	b.ReportMetric(float64(searchStats.AdjacencyPreparedCSRMmapDirectViews)/denom, "adjacency_prepared_csr_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyTypedListMmapDirectViews)/denom, "adjacency_typed_list_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyTypedListScratchDecodes)/denom, "adjacency_typed_list_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.AdjacencyLegacyFallbacks)/denom, "adjacency_legacy_fallbacks/search")
	b.ReportMetric(float64(searchStats.AdjacencySourceFallbacks)/denom, "adjacency_source_fallbacks/search")
	b.ReportMetric(float64(searchStats.RowRefVectorSourceState)/denom, "row_ref_vector_source_state/search")
	b.ReportMetric(float64(searchStats.RowRefVectorSourceLegacyGraphIDs)/denom, "row_ref_vector_source_legacy_graph_ids/search")
	b.ReportMetric(float64(searchStats.RowRefStatePreparedViews)/denom, "row_ref_state_prepared_views/search")
	b.ReportMetric(float64(searchStats.RowRefStateMmapDirectFields)/denom, "row_ref_state_mmap_direct_fields/search")
	b.ReportMetric(float64(searchStats.RowRefStateResultRefs)/denom, "row_ref_state_result_refs/search")
	b.ReportMetric(float64(searchStats.RowRefStateSourceFallbacks)/denom, "row_ref_state_source_fallbacks/search")
	b.ReportMetric(float64(searchStats.ResultIDPreparedBytesViews)/denom, "result_id_prepared_bytes_views/search")
	b.ReportMetric(float64(searchStats.ResultIDTypedBytesState)/denom, "result_id_typed_bytes_state/search")
	b.ReportMetric(float64(searchStats.ResultIDGraphFallbacks)/denom, "result_id_graph_fallbacks/search")
	b.ReportMetric(float64(searchStats.PreparedGraphSearchViews)/denom, "prepared_graph_search_views/search")
	b.ReportMetric(float64(searchStats.GraphRowFallbacks)/denom, "graph_row_fallbacks/search")
	b.ReportMetric(float64(searchStats.WavefrontSearches)/denom, "wavefront_searches/search")
	b.ReportMetric(float64(searchStats.WavefrontWidth), "wavefront_width")
	b.ReportMetric(float64(searchStats.WavefrontRounds)/denom, "wavefront_rounds/search")
	b.ReportMetric(float64(searchStats.WavefrontCandidatePops)/denom, "wavefront_candidate_pops/search")
	b.ReportMetric(float64(searchStats.WavefrontStagedNeighbors)/denom, "wavefront_staged_neighbors/search")
	b.ReportMetric(float64(searchStats.WavefrontMaxTileSize), "wavefront_max_tile_size")
	if searchStats.WavefrontRounds > 0 {
		b.ReportMetric(float64(searchStats.WavefrontStagedNeighbors)/float64(searchStats.WavefrontRounds), "wavefront_avg_tile_size")
	}
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(readerStats.CacheHits, baseStats.CacheHits))/denom, "cache_hits/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(readerStats.CacheMisses, baseStats.CacheMisses))/denom, "cache_misses/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(readerStats.DecodedBlocks, baseStats.DecodedBlocks))/denom, "decoded_blocks/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchBytesV3(readerStats.PhysicalBytesRead, baseStats.PhysicalBytesRead))/denom, "physical_B/search")
}

func (m columnVectorGraphSearchTopologyParityMode2091) String() string { return string(m) }

func (b columnVectorGraphSearchTopologyParityBoundary2091) String() string { return string(b) }

func (s columnVectorGraphSearchTopologyParityShape2091) String() string {
	return fmt.Sprintf("rows=%d/dims=%d/degree=%d/topK=%d/efSearch=%d/queryOrdinal=%d", s.rows, s.dims, s.degree, s.topK, s.efSearch, s.queryOrdinal)
}
