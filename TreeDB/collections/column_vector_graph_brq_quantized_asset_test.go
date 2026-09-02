package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/brq"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const columnGraphBRQQuantizedIndexName2481 = "embedding.brq_1bit.fast"

func TestVectorIndexQuantizedDefinitionNormalizationBRQ2481(t *testing.T) {
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 3)
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphBRQQuantizedIndexName2481, Codec: brq.CodecName}}
	normalized, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition brq_1bit: %v", err)
	}
	if got := normalized.QuantizedIndexes[0]; got.Codec != brq.CodecName || got.Version != brq.CodecVersion {
		t.Fatalf("normalized brq quantized index=%+v", got)
	}

	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphBRQQuantizedIndexName2481, Codec: brq.CodecName, Version: brq.CodecVersion + 1}}
	if _, err := normalizeVectorIndexDefinition(def); err == nil || !strings.Contains(err.Error(), "brq_1bit version") {
		t.Fatalf("normalize unsupported brq version err=%v want brq_1bit version failure", err)
	}
}

func TestColumnGraphBRQQuantizedAssetRebuildPrepareReopen2481(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0.125}},
	}
	dir, d, col, def := openColumnGraphBRQQuantizedTestCollection2481(t, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	cfg, graph, state, asset := loadColumnGraphBRQQuantizedState2481(t, d, def)
	q := def.QuantizedIndexes[0]
	wantAssetID := "quantized/" + q.Name + "/brq_1bit/packed_codes"
	if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes || asset.AssetID != wantAssetID || asset.RowCount != len(rows) || asset.AssetBytes <= 0 {
		t.Fatalf("brq quantized asset snapshot=%+v want asset_id=%q", asset, wantAssetID)
	}
	if asset.LogicalType != columnVectorIndexStateLogicalTypePackedBitVector || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawPackedBitVector {
		t.Fatalf("brq state type/encoding=(%q,%q)", asset.LogicalType, asset.PhysicalEncoding)
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, state, graph); err != nil {
		t.Fatalf("validate state assets with brq asset: %v", err)
	}

	prepared, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, asset)
	if err != nil {
		t.Fatalf("loadColumnVectorGraphQuantizedAsset brq: %v", err)
	}
	_, scannedRows := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	assertPreparedBRQRows2481(t, prepared, def, scannedRows)
	if fp := prepared.Footprint(); fp.AssetBytes != asset.AssetBytes || fp.BytesPerVector <= 0 {
		t.Fatalf("brq footprint=%+v assetBytes=%d", fp, asset.AssetBytes)
	}

	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	query := []float32{0.2, 0.9, 0.1}
	var buffer VectorIndexSearchBuffer
	quantizedOnly, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		_ = searcher.Close()
		t.Fatalf("brq quantized_only SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, brqQuantizedTopKForTest2481(t, rows, query, 2), false)
	assertBRQQuantizedOnlyStats2481(t, quantizedOnly.Stats, prepared)
	if err := searcher.Close(); err != nil {
		t.Fatalf("SearchWithBuffer searcher Close: %v", err)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	reopenedSearcher, err := reopenedCol.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher reopen: %v", err)
	}
	defer func() { _ = reopenedSearcher.Close() }()
	reopenedStatus := reopenedSearcher.reader.quantizedAssetStatus[q.Name]
	if reopenedStatus.Prepared == nil || reopenedStatus.BRQPlan == nil || reopenedStatus.Err != nil {
		t.Fatalf("reopened reader brq status=%+v", reopenedStatus)
	}
	assertPreparedBRQRows2481(t, reopenedStatus.Prepared, def, scannedRows)
	reopenedOnly, err := reopenedSearcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatalf("reopened brq quantized_only SearchWithBuffer: %v", err)
	}
	assertBRQQuantizedOnlyStats2481(t, reopenedOnly.Stats, reopenedStatus.Prepared)
}

func TestVectorIndexSearcherBRQQuantizedSearchWithBuffer2481(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.5, 0.5, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{-0.25, 0.75, 0.125}},
	}
	query := []float32{0.2, 0.9, 0.1}
	_, d, col, def := openColumnGraphBRQQuantizedTestCollection2481(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	q := def.QuantizedIndexes[0]
	prepared := searcher.reader.quantizedAssetStatus[q.Name].Prepared
	if prepared == nil {
		t.Fatalf("brq prepared status=%+v", searcher.reader.quantizedAssetStatus[q.Name])
	}
	plan, err := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
	if err != nil {
		t.Fatalf("brq.NewPlan: %v", err)
	}
	var buffer VectorIndexSearchBuffer

	if _, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows)}, &buffer); err == nil || !strings.Contains(err.Error(), "exact") {
		t.Fatalf("exact SearchWithBuffer with brq index err=%v want exact quantized-option rejection", err)
	}

	quantizedOnlyOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 3, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	quantizedOnly, err := searcher.SearchWithBuffer(quantizedOnlyOpts, &buffer)
	if err != nil {
		t.Fatalf("brq quantized_only SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, brqQuantizedTopKForTest2481(t, rows, query, 3), false)
	assertBRQQuantizedOnlyStats2481(t, quantizedOnly.Stats, prepared)

	rerankedAllOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: q.Name, QuantizedRerankCandidates: len(rows), TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	rerankedAll, err := searcher.SearchWithBuffer(rerankedAllOpts, &buffer)
	if err != nil {
		t.Fatalf("brq quantized_rerank all SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, rerankedAll.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	assertBRQQuantizedRerankStats2481(t, rerankedAll.Stats, len(rows), def.Dimensions, plan.BytesPerCode())

	const shortlist = 3
	rerankedShortOpts := rerankedAllOpts
	rerankedShortOpts.QuantizedRerankCandidates = shortlist
	rerankedShort, err := searcher.SearchWithBuffer(rerankedShortOpts, &buffer)
	if err != nil {
		t.Fatalf("brq quantized_rerank short SearchWithBuffer: %v", err)
	}
	if len(rerankedShort.Results) != 2 {
		t.Fatalf("brq quantized_rerank short results=%d want 2", len(rerankedShort.Results))
	}
	assertBRQQuantizedRerankStats2481(t, rerankedShort.Stats, shortlist, def.Dimensions, plan.BytesPerCode())

	if collectionsRaceEnabled {
		t.Skip("exact allocation counts are unstable under race instrumentation")
	}
	for i := 0; i < 8; i++ {
		if _, err := searcher.SearchWithBuffer(quantizedOnlyOpts, &buffer); err != nil {
			t.Fatalf("warm brq quantized_only iteration %d: %v", i, err)
		}
		if _, err := searcher.SearchWithBuffer(rerankedShortOpts, &buffer); err != nil {
			t.Fatalf("warm brq quantized_rerank iteration %d: %v", i, err)
		}
	}
	quantizedOnlyAllocs := testing.AllocsPerRun(100, func() {
		got, err := searcher.SearchWithBuffer(quantizedOnlyOpts, &buffer)
		if err != nil || len(got.Results) != quantizedOnlyOpts.TopK {
			panic("unexpected brq quantized_only SearchWithBuffer allocation probe result")
		}
	})
	if quantizedOnlyAllocs != 0 {
		t.Fatalf("brq quantized_only SearchWithBuffer steady-state allocs/run=%v want 0", quantizedOnlyAllocs)
	}
	rerankAllocs := testing.AllocsPerRun(100, func() {
		got, err := searcher.SearchWithBuffer(rerankedShortOpts, &buffer)
		if err != nil || len(got.Results) != rerankedShortOpts.TopK {
			panic("unexpected brq quantized_rerank SearchWithBuffer allocation probe result")
		}
	})
	if rerankAllocs != 0 {
		t.Fatalf("brq quantized_rerank SearchWithBuffer steady-state allocs/run=%v want 0", rerankAllocs)
	}
}

func TestColumnGraphBRQQuantizedScorerMatchesOracle2481(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0.125}},
		{id: "doc-d", vector: []float32{0.1, 0.2, 0.95}},
	}
	query := []float32{0.15, -0.35, 0.75}
	_, d, col, def := openColumnGraphBRQQuantizedTestCollection2481(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	q := def.QuantizedIndexes[0]
	status := reader.quantizedAssetStatus[q.Name]
	if status.Prepared == nil || status.BRQPlan == nil || status.Err != nil {
		t.Fatalf("brq prepared status=%+v", status)
	}
	var scratch columnVectorGraphNativeSearchScratch
	scorer, err := reader.prepareBRQQuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, q.Name, query, &scratch)
	if err != nil {
		t.Fatalf("prepareBRQQuantizedScorer: %v", err)
	}
	ordinals := []int{0, 1, 2, 3}
	var stats columnVectorGraphNativeSearchStats
	scores, err := scorer.scoreOrdinals(ordinals, nil, &scratch, &stats)
	if err != nil {
		t.Fatalf("scoreOrdinals: %v", err)
	}
	oracleQuery, err := status.BRQPlan.EncodeQuery(query, &brq.Workspace{})
	if err != nil {
		t.Fatalf("oracle EncodeQuery: %v", err)
	}
	view, ok := status.Prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok {
		t.Fatal("prepared packed code view unavailable")
	}
	for i, ordinal := range ordinals {
		code, ok := view.RowBytes(ordinal)
		if !ok {
			t.Fatalf("row %d code unavailable", ordinal)
		}
		codeCount, ok := status.Prepared.Uint32(quantizedasset.RoleCodeCount, ordinal)
		if !ok {
			t.Fatalf("row %d code_count unavailable", ordinal)
		}
		qdpInv, ok := status.Prepared.Float32(quantizedasset.RoleQuantizedDotProductInv, ordinal)
		if !ok {
			t.Fatalf("row %d qdp unavailable", ordinal)
		}
		want, err := status.BRQPlan.ScoreCosine(oracleQuery, code, codeCount, qdpInv)
		if err != nil {
			t.Fatalf("oracle ScoreCosine ordinal=%d: %v", ordinal, err)
		}
		if math.Abs(scores[i]-want) > 1e-9 {
			t.Fatalf("ordinal=%d score=%v want oracle=%v", ordinal, scores[i], want)
		}
	}
	if stats.QuantizedScoreCalls != uint64(len(ordinals)) || stats.QuantizedCodeBytesRead != uint64(len(ordinals)*status.BRQPlan.BytesPerCode()) || stats.BRQ1BitBitProductPasses != uint64(len(ordinals)*2) || stats.BRQ1BitQueryWeightBits != brq.QueryWeightBits || stats.QuantizedScoreCodecBRQ1Bit != 1 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 {
		t.Fatalf("brq scorer stats=%+v want packed code reads and brq counters only", stats)
	}
	_, err = scorer.scoreOrdinals([]int{0, len(rows)}, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "ordinal=4") {
		t.Fatalf("invalid ordinal err=%v want fail-closed unavailable", err)
	}
	if _, err = scorer.scoreOrdinals(ordinals, scores[:0], nil, &columnVectorGraphNativeSearchStats{}); !errors.Is(err, errColumnVectorGraphNativeSearchScratchRequired) {
		t.Fatalf("nil scratch err=%v want scratch required", err)
	}

	_, err = scorer.scoreOrdinals(ordinals, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
	if err != nil {
		t.Fatalf("warm scoreOrdinals: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, err := scorer.scoreOrdinals(ordinals, scores[:0], &scratch, &columnVectorGraphNativeSearchStats{})
		if err != nil {
			panic(err)
		}
		columnPhysicalScanBenchSum += int64(got[0] * 1_000_000)
	})
	if allocs != 0 {
		t.Fatalf("steady-state brq scoreOrdinals allocs/run=%v want 0", allocs)
	}
}

func TestVectorIndexSearcherBRQQuantizedFailClosedStats2481(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	query := []float32{1, 0, 0}
	_, d, col, def := openColumnGraphBRQQuantizedTestCollection2481(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	q := def.QuantizedIndexes[0]
	cases := []struct {
		name   string
		status *columnVectorGraphQuantizedAssetLoadStatus
		health columnVectorGraphQuantizedAssetHealth
	}{
		{name: "missing_status", health: columnVectorGraphQuantizedAssetHealthMissing},
		{name: "missing_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetMissing}, health: columnVectorGraphQuantizedAssetHealthMissing},
		{name: "invalid_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetInvalid}, health: columnVectorGraphQuantizedAssetHealthInvalid},
		{name: "stale_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetStale}, health: columnVectorGraphQuantizedAssetHealthStale},
		{name: "closed_asset", status: &columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Err: errColumnVectorGraphQuantizedAssetClosed}, health: columnVectorGraphQuantizedAssetHealthClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				t.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			if tc.status == nil {
				delete(searcher.reader.quantizedAssetStatus, q.Name)
			} else {
				searcher.reader.quantizedAssetStatus[q.Name] = *tc.status
			}
			var buffer VectorIndexSearchBuffer
			got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: q.Name, TopK: 1, EfSearch: len(rows)}, &buffer)
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) || len(got.Results) != 0 {
				t.Fatalf("SearchWithBuffer response=%+v err=%v want fail-closed unavailable", got, err)
			}
			assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, tc.health)
			if got.Stats.QuantizedScoreCalls != 0 || got.Stats.PreparedScoreCalls != 0 || got.Stats.VectorBytesRead != 0 || got.Stats.NormBytesRead != 0 || got.Stats.SearchRouteHNSWSearchPack != 0 {
				t.Fatalf("fail-closed stats=%+v want no scoring or exact fallback", got.Stats)
			}
		})
	}
}

func TestColumnGraphBRQQuantizedAssetFailClosed2481(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0.125}},
	}
	_, d, col, def := openColumnGraphBRQQuantizedTestCollection2481(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	cfg, graph, state, asset := loadColumnGraphBRQQuantizedState2481(t, d, def)
	q := def.QuantizedIndexes[0]

	missing := state
	missing.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	for i := range missing.Assets {
		if missing.Assets[i].Role == columnVectorIndexStateAssetRoleQuantizedCodes {
			missing.Assets = append(missing.Assets[:i], missing.Assets[i+1:]...)
			break
		}
	}
	if err := validateColumnVectorIndexStateAssetsForStatus(d.ColumnAssetRootDir(), "docs", *cfg, def, missing, graph); err == nil || !strings.Contains(err.Error(), "missing quantized asset") {
		t.Fatalf("missing brq asset err=%v want missing quantized asset", err)
	}

	staleSchema := asset
	staleSchema.SourceSchemaHash++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleSchema); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "schema_hash") {
		t.Fatalf("stale schema err=%v want stale schema_hash", err)
	}
	staleType := asset
	staleType.LogicalType = columnVectorIndexStateLogicalTypeByteVector
	staleType.PhysicalEncoding = columnVectorIndexStateEncodingRawFixedBytes
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleType); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "type/encoding") {
		t.Fatalf("stale type err=%v want stale type/encoding", err)
	}
	staleRowCount := asset
	staleRowCount.RowCount++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, staleRowCount); !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) || !strings.Contains(err.Error(), "row_count") {
		t.Fatalf("stale row_count err=%v want stale row_count", err)
	}
	badVersion := q
	badVersion.Version++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, badVersion, asset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "version") {
		t.Fatalf("bad codec version err=%v want invalid version", err)
	}
	badChecksum := asset
	badChecksum.Ref.Checksum++
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, badChecksum); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("bad checksum err=%v want invalid checksum", err)
	}
	paddingAsset := writeUncheckedRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+101, func(raw []byte, image typedcolumn.ColumnPartImage) {
		section := mustColumnDataSection2450(t, image, columnVectorGraphQuantizedPackedCodesColumnName)
		raw[section.Offset+section.Length-1] |= 0xf0
	})
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, paddingAsset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || (!strings.Contains(err.Error(), "non-zero padding") && !strings.Contains(err.Error(), "checksum")) {
		t.Fatalf("non-zero padding err=%v want invalid padding/checksum fail-closed", err)
	}
	countAsset := writeUncheckedRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+102, func(raw []byte, image typedcolumn.ColumnPartImage) {
		section := mustColumnDataSection2450(t, image, columnVectorGraphQuantizedCodeCountColumnName)
		raw[section.Offset]++
	})
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, countAsset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "code_count") {
		t.Fatalf("code_count mismatch err=%v want invalid code_count", err)
	}
	qdpRangeAsset := writeOutOfRangeQDPRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+103)
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, qdpRangeAsset); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "quantized_dot_product_inv") || !strings.Contains(err.Error(), "outside valid range") {
		t.Fatalf("qdp range err=%v want invalid quantized_dot_product_inv range", err)
	}
	wrongShape := writeWrongShapeRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, asset.Ref.PartID+104, len(rows))
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, wrongShape); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !strings.Contains(err.Error(), "elements_per_row") {
		t.Fatalf("wrong packed shape err=%v want elements_per_row invalid", err)
	}
	missingSide := writeMissingSideRabitQQuantizedAsset2450(t, d, *cfg, def, graph, q, rows, asset.Ref.PartID+105)
	if _, err := loadColumnVectorGraphQuantizedAsset(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, q, missingSide); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || (!strings.Contains(err.Error(), "missing required role") && !strings.Contains(err.Error(), "missing typed-column column")) {
		t.Fatalf("missing side-array err=%v want missing role/column", err)
	}

	schema, err := columnVectorGraphQuantizedAssetSchema(def, graph, q, asset, columnVectorGraphQuantizedAssetRefIdentity(asset.Ref))
	if err != nil {
		t.Fatalf("columnVectorGraphQuantizedAssetSchema: %v", err)
	}
	expected := quantizedasset.ExpectedSchema{Metric: schema.Metric, VectorDimensions: schema.VectorDimensions, CodeDimensions: schema.CodeDimensions, CodeWidthBits: schema.CodeWidthBits, RowCount: schema.RowCount, OrdinalOrder: schema.OrdinalOrder, Codec: schema.Codec, BaseGraph: schema.BaseGraph, RequiredRoles: columnVectorGraphQuantizedRequiredRoles(q)}
	expected.Codec.ConfigHash++
	if err := quantizedasset.Validate(schema, expected); err == nil || !strings.Contains(err.Error(), "config_hash") {
		t.Fatalf("config hash mismatch err=%v want config_hash", err)
	}
	expected = quantizedasset.ExpectedSchema{Metric: schema.Metric, VectorDimensions: schema.VectorDimensions, CodeDimensions: schema.CodeDimensions, CodeWidthBits: schema.CodeWidthBits, RowCount: schema.RowCount, OrdinalOrder: schema.OrdinalOrder, Codec: schema.Codec, BaseGraph: schema.BaseGraph, RequiredRoles: columnVectorGraphQuantizedRequiredRoles(q)}
	expected.Codec.Config = append([]byte(nil), expected.Codec.Config...)
	expected.Codec.Config[len(expected.Codec.Config)-1] ^= 1
	if err := quantizedasset.Validate(schema, expected); err == nil || !strings.Contains(err.Error(), "config bytes") {
		t.Fatalf("config bytes mismatch err=%v want config bytes", err)
	}
	expected = quantizedasset.ExpectedSchema{Metric: schema.Metric, VectorDimensions: schema.VectorDimensions, CodeDimensions: schema.CodeDimensions, CodeWidthBits: schema.CodeWidthBits, RowCount: schema.RowCount, OrdinalOrder: schema.OrdinalOrder, Codec: schema.Codec, BaseGraph: schema.BaseGraph, RequiredRoles: columnVectorGraphQuantizedRequiredRoles(q)}
	expected.BaseGraph.BaseManifestChecksum++
	if err := quantizedasset.Validate(schema, expected); err == nil || !strings.Contains(err.Error(), "base graph identity mismatch") {
		t.Fatalf("base graph mismatch err=%v want identity mismatch", err)
	}
	mismatchedGraph := graph
	mismatchedGraph.BaseManifestChecksum++
	if columnVectorIndexStateMatchesGraph(state, mismatchedGraph) {
		t.Fatalf("state matched graph with mismatched base manifest checksum")
	}
}

func openColumnGraphBRQQuantizedTestCollection2481(tb testing.TB, rows []columnGraphRebuildInputRowV2A) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	return openColumnGraphQuantizedTestCollection1926(tb, rows, []QuantizedVectorIndexDefinition{{Name: columnGraphBRQQuantizedIndexName2481, Codec: brq.CodecName}})
}

func loadColumnGraphBRQQuantizedState2481(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition) (*ColumnStoreConfig, columnVectorGraphManifestSnapshot, columnVectorIndexStateSnapshot, columnVectorIndexStateAssetSnapshot) {
	tb.Helper()
	cfg, graph, state, asset := loadColumnGraphRabitQQuantizedState2450(tb, d, def)
	return cfg, graph, state, asset
}

func assertPreparedBRQRows2481(tb testing.TB, prepared *quantizedasset.Prepared, def VectorIndexDefinition, rows []columnGraphRebuildScannedRowV2A) {
	tb.Helper()
	if prepared == nil {
		tb.Fatal("prepared brq asset is nil")
	}
	if prepared.Rows() != len(rows) {
		tb.Fatalf("prepared rows=%d want %d", prepared.Rows(), len(rows))
	}
	plan, err := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
	if err != nil {
		tb.Fatalf("brq.NewPlan: %v", err)
	}
	view, ok := prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok || view.Rows() != len(rows) || view.BytesPerRow() != plan.BytesPerCode() || view.ElementsPerRow() != plan.CodeDimensions() {
		tb.Fatalf("brq code view=%+v ok=%v", view, ok)
	}
	var ws brq.Workspace
	var codeScratch []byte
	for ordinal, row := range rows {
		encoded, err := plan.Encode(codeScratch, row.vector, &ws)
		if err != nil {
			tb.Fatalf("brq Encode row %d: %v", ordinal, err)
		}
		codeScratch = encoded.Code
		gotCode, ok := view.RowBytes(ordinal)
		if !ok || !bytes.Equal(gotCode, encoded.Code) {
			tb.Fatalf("row %d code=%v ok=%v want %v", ordinal, gotCode, ok, encoded.Code)
		}
		gotCount, ok := prepared.Uint32(quantizedasset.RoleCodeCount, ordinal)
		if !ok || gotCount != encoded.CodeCount {
			tb.Fatalf("row %d code_count=%d ok=%v want %d", ordinal, gotCount, ok, encoded.CodeCount)
		}
		gotQDP, ok := prepared.Float32(quantizedasset.RoleQuantizedDotProductInv, ordinal)
		if !ok || gotQDP != encoded.QuantizedDotProductInv {
			tb.Fatalf("row %d qdp_inv=%v ok=%v want %v", ordinal, gotQDP, ok, encoded.QuantizedDotProductInv)
		}
	}
}

func brqQuantizedTopKForTest2481(tb testing.TB, rows []columnGraphRebuildInputRowV2A, query []float32, topK int) []columnVectorGraphNativeSearchResult {
	tb.Helper()
	if len(rows) == 0 || topK <= 0 {
		return nil
	}
	plan, err := brq.NewPlan(len(query), brq.DefaultConfig())
	if err != nil {
		tb.Fatalf("brq.NewPlan: %v", err)
	}
	var ws brq.Workspace
	encodedQuery, err := plan.EncodeQuery(query, &ws)
	if err != nil {
		tb.Fatalf("brq EncodeQuery: %v", err)
	}
	var top []columnVectorGraphSearchCandidate
	var codeScratch []byte
	for ordinal, row := range rows {
		encoded, err := plan.Encode(codeScratch, row.vector, &ws)
		if err != nil {
			tb.Fatalf("brq Encode row %d: %v", ordinal, err)
		}
		codeScratch = encoded.Code
		score, err := plan.ScoreEncoded(encodedQuery, encoded)
		if err != nil {
			tb.Fatalf("brq ScoreEncoded row %d: %v", ordinal, err)
		}
		top = insertColumnGraphTopForTest(top, topK, columnVectorGraphSearchCandidate{ordinal: ordinal, score: score})
	}
	out := make([]columnVectorGraphNativeSearchResult, len(top))
	for i, candidate := range top {
		out[i] = columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, ID: []byte(rows[candidate.ordinal].id), Score: candidate.score}
	}
	return out
}

func assertBRQQuantizedOnlyStats2481(tb testing.TB, stats VectorIndexSearchStats, prepared *quantizedasset.Prepared) {
	tb.Helper()
	bytesPerCode, ok := prepared.BytesPerRow(quantizedasset.RolePackedCodes)
	if !ok || bytesPerCode <= 0 {
		tb.Fatalf("brq prepared bytes_per_code=%d ok=%v", bytesPerCode, ok)
	}
	if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("brq quantized_only route stats=%+v", stats)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(bytesPerCode) {
		tb.Fatalf("brq quantized_only code stats=%+v bytes_per_code=%d", stats, bytesPerCode)
	}
	if stats.QuantizedScoreCodecBRQ1Bit != 1 || stats.BRQ1BitQueryWeightBits != brq.QueryWeightBits || stats.BRQ1BitBitProductPasses != stats.QuantizedScoreCalls*2 || stats.BRQ1BitQueryWeightScale <= 0 {
		tb.Fatalf("brq quantized_only codec stats=%+v", stats)
	}
	if stats.PreparedScoreCalls != 0 || stats.QuantizedRerankCandidates != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 || stats.DocumentsFetched != 0 {
		tb.Fatalf("brq quantized_only exact/doc stats=%+v want none", stats)
	}
}

func assertBRQQuantizedRerankStats2481(tb testing.TB, stats VectorIndexSearchStats, shortlist int, dims int, bytesPerCode int) {
	tb.Helper()
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("brq quantized_rerank route stats=%+v", stats)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(bytesPerCode) {
		tb.Fatalf("brq quantized_rerank code stats=%+v bytes_per_code=%d", stats, bytesPerCode)
	}
	if stats.QuantizedScoreCodecBRQ1Bit != 1 || stats.BRQ1BitQueryWeightBits != brq.QueryWeightBits || stats.BRQ1BitBitProductPasses != stats.QuantizedScoreCalls*2 || stats.BRQ1BitQueryWeightScale <= 0 {
		tb.Fatalf("brq quantized_rerank codec stats=%+v", stats)
	}
	if stats.QuantizedRerankCandidates != uint64(shortlist) || stats.QuantizedRerankExactScoreCalls != uint64(shortlist) {
		tb.Fatalf("brq quantized_rerank exact calls stats=%+v shortlist=%d", stats, shortlist)
	}
	if stats.VectorBytesRead != uint64(shortlist*dims*4) || stats.NormBytesRead != uint64(shortlist*4) {
		tb.Fatalf("brq quantized_rerank exact bytes stats=%+v want vector=%d norm=%d", stats, shortlist*dims*4, shortlist*4)
	}
	if stats.DocumentsFetched != 0 {
		tb.Fatalf("brq quantized_rerank stats=%+v want no documents", stats)
	}
}

func TestCollectionVectorIndexPreparedQuantizedSearchCacheKeyBRQIdentity2481(t *testing.T) {
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 3)
	def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphBRQQuantizedIndexName2481, Codec: brq.CodecName}}
	def, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	q := def.QuantizedIndexes[0]
	graphRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "ns", Generation: 7, PartID: 11, FileID: 13, Offset: 17, Length: 19, Checksum: 23}
	graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: 29, BaseManifestChecksum: 31, BaseSchemaHash: 37, GraphSchemaHash: 41, RowCount: 5, AssetRef: graphRef, AssetBytes: graphRef.Length}
	logicalType, physicalEncoding := columnVectorGraphQuantizedAssetStateType(q)
	asset := columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleQuantizedCodes, AssetID: columnVectorGraphQuantizedAssetID(q), LogicalType: logicalType, PhysicalEncoding: physicalEncoding, RowCount: graph.RowCount, SourceSchemaHash: 43, AssetBytes: 47, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "ns", Generation: graph.BaseManifestGeneration, PartID: 53, FileID: 59, Offset: 61, Length: 67, Checksum: 71}}
	state := columnVectorIndexStateSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, RowCount: graph.RowCount, BaseManifestGeneration: graph.BaseManifestGeneration, BaseManifestChecksum: graph.BaseManifestChecksum, BaseSchemaHash: graph.BaseSchemaHash, AdjacencyLayerCount: graph.AdjacencyLayerCount, Assets: []columnVectorIndexStateAssetSnapshot{asset}}
	key, err := collectionVectorIndexPreparedQuantizedSearchCacheKey("docs", "ns", def, graph, state, q.Name, 1)
	if err != nil {
		t.Fatalf("collectionVectorIndexPreparedQuantizedSearchCacheKey brq: %v", err)
	}
	wantIdentity := fmt.Sprintf("codec=%s|version=%d|codec_config_hash=%d|codec_config=%x|code_dimensions=4|code_width_bits=%d|", brq.CodecName, brq.CodecVersion, brq.DefaultConfig().Hash64(), brq.DefaultConfig().CanonicalBytes(), brq.CodeWidthBits)
	if !strings.Contains(key, wantIdentity) || !strings.Contains(key, "asset_id=quantized/embedding.brq_1bit.fast/brq_1bit/packed_codes") {
		t.Fatalf("brq cache key missing codec/version/config/asset identity: %s want identity %s", key, wantIdentity)
	}
}
