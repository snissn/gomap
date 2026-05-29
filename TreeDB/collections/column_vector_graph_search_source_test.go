package collections

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

func TestColumnVectorGraphSearchSourceConstructionHealthyTypedColumn1968(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(16, 8)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 8, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	plan, _, _ := prepareColumnVectorGraphScoreSourceTestPlan1968(t, reader, rows[0].vector)
	if plan.scoreSource.dims != def.Dimensions || plan.scoreSource.rowCount != len(rows) {
		t.Fatalf("score source dims/rows=(%d,%d) want (%d,%d)", plan.scoreSource.dims, plan.scoreSource.rowCount, def.Dimensions, len(rows))
	}
	if plan.scoreSource.vectorKind != columnVectorGraphSearchVectorSourceTypedColumn || plan.scoreSource.typedVectorSource == nil || len(plan.scoreSource.typedVectorLocations) != len(rows) {
		t.Fatalf("vector source kind=%s typed=%v locations=%d want prebound typed_column row mapping", plan.scoreSource.vectorKind, plan.scoreSource.typedVectorSource != nil, len(plan.scoreSource.typedVectorLocations))
	}
	if plan.scoreSource.normKind != columnVectorGraphSearchNormSourceInvNormByOrdinal || len(plan.scoreSource.invNormByOrdinal) != len(rows) {
		t.Fatalf("norm source kind=%s values=%d want inv_norm_by_ordinal rows=%d", plan.scoreSource.normKind, len(plan.scoreSource.invNormByOrdinal), len(rows))
	}
	if len(plan.ordinalRefs) != 0 || !plan.singleOrdinalRange || !plan.ordinalRefsReady {
		t.Fatalf("ordinal map refs=%d single=%v ready=%v want single-range prebound map", len(plan.ordinalRefs), plan.singleOrdinalRange, plan.ordinalRefsReady)
	}
}

func TestColumnVectorGraphSearchSourceDisablesUnsupportedTypedVector1968(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(12, 6)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 6, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.typedVectorSource == nil {
		t.Fatal("test requires typed vector source")
	}
	reader.typedVectorSource.dims = def.Dimensions + 1

	plan, scratch, queryInvNorm := prepareColumnVectorGraphScoreSourceTestPlan1968(t, reader, rows[3].vector)
	if plan.scoreSource.vectorKind != columnVectorGraphSearchVectorSourceGraphRows || plan.scoreSource.vectorFallbackReason != typeddecode.ReasonDimensionMismatch {
		t.Fatalf("vector source kind=%s reason=%s want graph-row fallback from dimension mismatch", plan.scoreSource.vectorKind, plan.scoreSource.vectorFallbackReason)
	}
	var stats columnVectorGraphNativeSearchStats
	got, err := plan.scoreSource.scoreOrdinal(plan, nil, rows[3].vector, queryInvNorm, 3, scratch, &stats)
	if err != nil {
		t.Fatalf("score source fallback: %v", err)
	}
	if stats.TypedColumnFallbacks != 1 || stats.VectorScratchDecodes != 1 || stats.VectorCertificationFailures == 0 {
		t.Fatalf("stats=%+v want typed-column fallback, graph-row scratch decode, and certification failure", stats)
	}
	legacyReader := *reader
	legacyReader.typedVectorSource = nil
	legacy, err := legacyReader.scoreOrdinalLegacy(plan, nil, rows[3].vector, queryInvNorm, 3, scratch, nil)
	if err != nil {
		t.Fatalf("legacy graph-row score: %v", err)
	}
	if math.Abs(got-legacy) > 1e-6 {
		t.Fatalf("fallback score=%g legacy=%g", got, legacy)
	}
}

func TestColumnVectorGraphSearchSourceScoresMatchLegacyContiguousAndScattered1968(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(32, 10)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 10, 6, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	query := rows[11].vector
	plan, scratch, queryInvNorm := prepareColumnVectorGraphScoreSourceTestPlan1968(t, reader, query)
	if plan.scoreSource.vectorKind != columnVectorGraphSearchVectorSourceTypedColumn || plan.scoreSource.normKind != columnVectorGraphSearchNormSourceInvNormByOrdinal {
		t.Fatalf("source vector=%s norm=%s want typed vector and inv_norm state", plan.scoreSource.vectorKind, plan.scoreSource.normKind)
	}

	for _, tc := range []struct {
		name     string
		ordinals []int
	}{
		{name: "contiguous", ordinals: []int{8, 9, 10, 11, 12, 13}},
		{name: "scattered", ordinals: []int{31, 2, 17, 5, 23, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, ordinal := range tc.ordinals {
				var sourceStats columnVectorGraphNativeSearchStats
				sourceScore, err := plan.scoreSource.scoreOrdinal(plan, nil, query, queryInvNorm, ordinal, scratch, &sourceStats)
				if err != nil {
					t.Fatalf("source ordinal %d: %v", ordinal, err)
				}
				legacyScore, err := reader.scoreOrdinalLegacy(plan, nil, query, queryInvNorm, ordinal, scratch, nil)
				if err != nil {
					t.Fatalf("legacy ordinal %d: %v", ordinal, err)
				}
				if math.Abs(sourceScore-legacyScore) > 1e-6 {
					t.Fatalf("ordinal %d source=%g legacy=%g", ordinal, sourceScore, legacyScore)
				}
				if sourceStats.VectorSourceCounters().MmapDirectViews+sourceStats.VectorSourceCounters().HeapCopyTypedViews+sourceStats.VectorSourceCounters().ScratchDecodes != 1 {
					t.Fatalf("source stats=%+v want one vector source outcome", sourceStats)
				}
				if sourceStats.NormSourceCounters().MmapDirectViews+sourceStats.NormSourceCounters().HeapCopyTypedViews+sourceStats.NormSourceCounters().ScratchDecodes != 1 {
					t.Fatalf("source stats=%+v want one norm source outcome", sourceStats)
				}
			}
		})
	}
}

func TestColumnVectorGraphSearchSourcePolicyMatchesLegacy1968(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(8, 4)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 4, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plan, scratch, _ := prepareColumnVectorGraphScoreSourceTestPlan1968(t, reader, rows[0].vector)

	t.Run("dimension_mismatch", func(t *testing.T) {
		query := []float32{1, 0, 0}
		queryInvNorm, err := columnVectorGraphInvNorm(query)
		if err != nil {
			t.Fatalf("query inv norm: %v", err)
		}
		_, sourceErr := plan.scoreSource.scoreOrdinal(plan, nil, query, queryInvNorm, 0, scratch, nil)
		_, legacyErr := reader.scoreOrdinalLegacy(plan, nil, query, queryInvNorm, 0, scratch, nil)
		if !errors.Is(sourceErr, errColumnVectorGraphNativeSearchCandidateDimensionMismatch) || !errors.Is(legacyErr, errColumnVectorGraphNativeSearchCandidateDimensionMismatch) {
			t.Fatalf("sourceErr=%v legacyErr=%v want candidate dimension mismatch", sourceErr, legacyErr)
		}
	})

	t.Run("nan_inf_score", func(t *testing.T) {
		badDB, badCol, badDef := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
			{ID: []byte("doc-inf"), Vector: []float32{float32(math.Inf(1)), 0, 0}, InvNorm: 1, Adjacency: []uint32{0}},
		})
		defer func() { _ = badDB.Close() }()
		badReader, err := badCol.openColumnVectorGraphPhysicalRowReader(badDef.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("open bad reader: %v", err)
		}
		defer func() { _ = badReader.Close() }()
		query := []float32{1, 0, 0}
		queryInvNorm, err := columnVectorGraphInvNorm(query)
		if err != nil {
			t.Fatalf("query inv norm: %v", err)
		}
		badScratch := &columnVectorGraphNativeSearchScratch{}
		if err := badScratch.prepare(badReader.RowCount(), badReader.def.Dimensions, badReader.def.M, 1, 1); err != nil {
			t.Fatalf("bad scratch prepare: %v", err)
		}
		badPlan, err := badScratch.prepareSearchPlan(badReader)
		if err != nil {
			t.Fatalf("bad prepareSearchPlan: %v", err)
		}
		if _, err := badPlan.blockViewForAssetOrdinal(0); err != nil {
			t.Fatalf("bad warm block view: %v", err)
		}
		_, sourceErr := badPlan.scoreSource.scoreOrdinal(badPlan, nil, query, queryInvNorm, 0, badScratch, nil)
		_, legacyErr := badReader.scoreOrdinalLegacy(badPlan, nil, query, queryInvNorm, 0, badScratch, nil)
		if sourceErr == nil || legacyErr == nil || !strings.Contains(sourceErr.Error(), "cosine score is not finite") || !strings.Contains(legacyErr.Error(), "cosine score is not finite") {
			t.Fatalf("sourceErr=%v legacyErr=%v want non-finite score policy", sourceErr, legacyErr)
		}
	})

	t.Run("norm_state_fallback", func(t *testing.T) {
		fallbackReader := *reader
		fallbackReader.invNormSource = nil
		fallbackReader.invNormStateUnavailable = true
		fallbackPlan := *plan
		fallbackPlan.reader = &fallbackReader
		if err := fallbackPlan.scoreSource.prepare(&fallbackPlan); err != nil {
			t.Fatalf("prepare fallback source: %v", err)
		}
		query := rows[2].vector
		queryInvNorm, err := columnVectorGraphInvNorm(query)
		if err != nil {
			t.Fatalf("query inv norm: %v", err)
		}
		var stats columnVectorGraphNativeSearchStats
		sourceScore, err := fallbackPlan.scoreSource.scoreOrdinal(&fallbackPlan, nil, query, queryInvNorm, 2, scratch, &stats)
		if err != nil {
			t.Fatalf("source fallback score: %v", err)
		}
		legacyScore, err := fallbackReader.scoreOrdinalLegacy(&fallbackPlan, nil, query, queryInvNorm, 2, scratch, nil)
		if err != nil {
			t.Fatalf("legacy fallback score: %v", err)
		}
		if math.Abs(sourceScore-legacyScore) > 1e-6 || stats.NormMmapDirectViews+stats.NormHeapCopyTypedViews+stats.NormScratchDecodes != 0 {
			t.Fatalf("source=%g legacy=%g stats=%+v want graph-row norm fallback parity", sourceScore, legacyScore, stats)
		}
	})
}

func TestColumnVectorGraphSearchSourceAggregateCounters1968(t *testing.T) {
	stats := columnVectorGraphNativeSearchStats{
		VectorDirectViews:                2,
		VectorMmapDirectViews:            2,
		VectorHeapCopyTypedViews:         3,
		VectorScratchDecodes:             5,
		TypedColumnFallbacks:             7,
		VectorCertificationFailures:      11,
		NormDirectViews:                  13,
		NormMmapDirectViews:              13,
		NormHeapCopyTypedViews:           17,
		NormScratchDecodes:               19,
		NormSourceFallbacks:              23,
		NormValidationFailures:           29,
		AdjacencyDirectViews:             31,
		AdjacencyMmapDirectViews:         31,
		AdjacencyHeapCopyTypedViews:      37,
		AdjacencyScratchDecodes:          41,
		AdjacencySourceFallbacks:         43,
		AdjacencyCertificationFailures:   47,
		AdjacencyValidationFailures:      53,
		AdjacencyAbsoluteOffsetUnaligned: 59,
		AdjacencyActualPointerUnaligned:  61,
		AdjacencyStaleHandles:            67,
	}
	vector := stats.VectorSourceCounters()
	if vector.DirectViews != 2 || vector.HeapCopyTypedViews != 3 || vector.ScratchDecodes != 5 || vector.Fallbacks != 7 || vector.CertificationFailures != 11 {
		t.Fatalf("vector counters=%+v", vector)
	}
	agg := stats.AggregateSourceCounters()
	if agg.DirectViews != 46 || agg.HeapCopyTypedViews != 57 || agg.ScratchDecodes != 65 || agg.Fallbacks != 73 || agg.CertificationFailures != 58 || agg.ValidationFailures != 82 {
		t.Fatalf("aggregate counters=%+v", agg)
	}
}

func TestColumnVectorGraphSearchSourceAllocationFreeAfterWarmup1968(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(64, 16)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 16, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	query := rows[5].vector
	plan, scratch, queryInvNorm := prepareColumnVectorGraphScoreSourceTestPlan1968(t, reader, query)
	ordinals := []int{0, 3, 7, 11, 19, 31, 43, 63}
	scores := make([]float64, len(ordinals))
	var warmStats columnVectorGraphNativeSearchStats
	if _, err := plan.scoreSource.scoreOrdinalsScalar(plan, nil, query, queryInvNorm, ordinals, scores, scratch, &warmStats); err != nil {
		t.Fatalf("warm score tile: %v", err)
	}
	var runErr error
	allocs := testing.AllocsPerRun(1000, func() {
		var stats columnVectorGraphNativeSearchStats
		_, runErr = plan.scoreSource.scoreOrdinalsScalar(plan, nil, query, queryInvNorm, ordinals, scores, scratch, &stats)
	})
	if runErr != nil {
		t.Fatalf("score tile: %v", runErr)
	}
	if allocs != 0 {
		t.Fatalf("allocs/run=%g want 0 after warmup", allocs)
	}
}

func BenchmarkColumnVectorGraphScoreSourceScalarTile1968(b *testing.B) {
	for _, tile := range []int{1, 2, 4, 8, 13, 16, 32} {
		for _, scattered := range []bool{false, true} {
			name := "contiguous"
			if scattered {
				name = "scattered"
			}
			b.Run(name+"/tile="+itoaColumnVectorGraph1968(tile), func(b *testing.B) {
				benchmarkColumnVectorGraphScoreSourceScalarTile1968(b, tile, scattered)
			})
		}
	}
}

func benchmarkColumnVectorGraphScoreSourceScalarTile1968(b *testing.B, tile int, scattered bool) {
	shape := columnVectorGraphNativeSearchBenchShapeV3{rows: 2048, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 17, typedColumnVector: true}
	closeFn, col, def, query := openColumnVectorGraphNativeSearchBenchFixtureV3(b, shape)
	defer closeFn()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plan, scratch, queryInvNorm := prepareColumnVectorGraphScoreSourceTestPlan1968(b, reader, query)
	if plan.scoreSource.vectorKind != columnVectorGraphSearchVectorSourceTypedColumn || plan.scoreSource.normKind != columnVectorGraphSearchNormSourceInvNormByOrdinal {
		b.Fatalf("source vector=%s norm=%s want typed/inv_norm", plan.scoreSource.vectorKind, plan.scoreSource.normKind)
	}
	ordinals := make([]int, tile)
	for i := range ordinals {
		if scattered {
			ordinals[i] = (17 + i*257) % reader.RowCount()
		} else {
			ordinals[i] = 128 + i
		}
	}
	scores := make([]float64, tile)
	var warmStats columnVectorGraphNativeSearchStats
	if _, err := plan.scoreSource.scoreOrdinalsScalar(plan, nil, query, queryInvNorm, ordinals, scores, scratch, &warmStats); err != nil {
		b.Fatalf("warm score tile: %v", err)
	}
	var stats columnVectorGraphNativeSearchStats
	var sink float64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := plan.scoreSource.scoreOrdinalsScalar(plan, nil, query, queryInvNorm, ordinals, scores, scratch, &stats)
		if err != nil {
			b.Fatalf("score tile: %v", err)
		}
		sink += got[0]
	}
	b.StopTimer()
	columnPhysicalScanBenchSum += int64(sink)
	b.ReportMetric(float64(def.Dimensions), "dims")
	b.ReportMetric(float64(tile), "tile_size")
	if scattered {
		b.ReportMetric(1, "scattered_ordinals")
	} else {
		b.ReportMetric(1, "contiguous_ordinals")
	}
	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N*tile)/elapsed, "ops/sec")
	}
	counters := stats.AggregateSourceCounters()
	if b.N > 0 {
		b.ReportMetric(float64(counters.DirectViews)/float64(b.N), "source_direct_views/op")
		b.ReportMetric(float64(counters.MmapDirectViews)/float64(b.N), "source_mmap_direct/op")
		b.ReportMetric(float64(counters.HeapCopyTypedViews)/float64(b.N), "source_heap_copy_typed_views/op")
		b.ReportMetric(float64(counters.ScratchDecodes)/float64(b.N), "source_scratch_decodes/op")
		b.ReportMetric(float64(counters.Fallbacks)/float64(b.N), "source_fallbacks/op")
		b.ReportMetric(float64(counters.CertificationFailures)/float64(b.N), "source_certification_failures/op")
	}
}

func prepareColumnVectorGraphScoreSourceTestPlan1968(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, query []float32) (*columnVectorGraphSearchPlan, *columnVectorGraphNativeSearchScratch, float32) {
	tb.Helper()
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	scratch := &columnVectorGraphNativeSearchScratch{}
	if err := scratch.prepare(reader.RowCount(), reader.def.Dimensions, reader.def.M, 8, 8); err != nil {
		tb.Fatalf("scratch prepare: %v", err)
	}
	plan, err := scratch.prepareSearchPlan(reader)
	if err != nil {
		tb.Fatalf("prepareSearchPlan: %v", err)
	}
	if _, err := plan.blockViewForAssetOrdinal(0); err != nil {
		tb.Fatalf("warm block view: %v", err)
	}
	var stats columnVectorGraphNativeSearchStats
	if _, err := plan.scoreSource.scoreOrdinal(plan, nil, query, queryInvNorm, 0, scratch, &stats); err != nil {
		tb.Fatalf("warm score source: %v", err)
	}
	return plan, scratch, queryInvNorm
}

func itoaColumnVectorGraph1968(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
