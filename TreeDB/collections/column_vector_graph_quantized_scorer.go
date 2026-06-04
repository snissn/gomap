package collections

import (
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

const columnVectorGraphScalarU8CodeScale = 255.0 * 255.0

type columnVectorGraphScalarU8QuantizedScorer struct {
	indexName string
	dims      int
	codeRows  quantizedasset.CodeRowView
	// queryCode and centeredQuery alias caller-owned search scratch.
	queryCode     []byte
	centeredQuery vectorops.ScalarU8CenteredQuery
}

func (r *columnVectorGraphPhysicalRowReader) prepareScalarU8QuantizedScorer(mode columnVectorGraphNativeSearchQueryMode, indexName string, query []float32, queryInvNorm float32, scratch *columnVectorGraphNativeSearchScratch) (columnVectorGraphScalarU8QuantizedScorer, error) {
	if r == nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, errNilColumnVectorGraphPhysicalRowReader
	}
	status, ok := r.quantizedAssetStatus[indexName]
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName)
	}
	if status.Err != nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q score-plane asset unavailable: %w", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, status.Err)
	}
	if status.Prepared == nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName)
	}
	qdef, ok := findQuantizedVectorIndex(r.def, indexName)
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, r.def.Name, indexName)
	}
	if status.Definition != qdef {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q prepared definition mismatch", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName)
	}
	if qdef.Codec != QuantizedVectorCodecScalarU8 || qdef.Version != 1 {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q codec/version=(%q,%d) is not scalar_u8 v1", ErrVectorIndexSearchUnavailable, r.def.Name, indexName, qdef.Codec, qdef.Version)
	}
	if r.def.Metric != VectorMetricCosine {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q metric %q is unsupported for scalar_u8 scorer", ErrVectorIndexSearchUnavailable, r.def.Name, indexName, r.def.Metric)
	}
	if len(query) != r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	if scratch == nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	prepared := status.Prepared
	if prepared.Rows() != r.RowCount() {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q prepared rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, prepared.Rows(), r.RowCount())
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q code row view unavailable", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName)
	}
	if codeRows.Rows() != r.RowCount() {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q code row view rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, codeRows.Rows(), r.RowCount())
	}
	if bytesPerRow := codeRows.BytesPerRow(); bytesPerRow != r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q code bytes_per_row=%d want dimensions=%d", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, bytesPerRow, r.def.Dimensions)
	}
	if elements := codeRows.ElementsPerRow(); elements != r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q code elements_per_row=%d want dimensions=%d", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, elements, r.def.Dimensions)
	}
	queryCode := resizeColumnVectorGraphNativeByteScratch(scratch.quantizedQueryCodes, r.def.Dimensions)
	for _, value := range query {
		queryCode = append(queryCode, columnVectorGraphScalarU8Code(value*queryInvNorm))
	}
	scratch.quantizedQueryCodes = queryCode
	centeredScratch := resizeColumnVectorGraphNativeScalarU8CenteredScratch(scratch.quantizedQueryCentered, r.def.Dimensions)
	centeredQuery, centeredScratch, ok := vectorops.PrepareScalarU8CenteredQuery(centeredScratch, queryCode, r.def.Dimensions)
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q centered scalar_u8 query unavailable", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName)
	}
	scratch.quantizedQueryCentered = centeredScratch
	return columnVectorGraphScalarU8QuantizedScorer{indexName: indexName, dims: r.def.Dimensions, codeRows: codeRows, queryCode: queryCode, centeredQuery: centeredQuery}, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreOrdinal(ordinal int, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if s == nil || !s.codeRows.Valid() || s.dims <= 0 || len(s.queryCode) != s.dims || !s.centeredQuery.ValidForDims(s.dims) {
		return 0, fmt.Errorf("%w: column_graph quantized scalar_u8 scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	row, ok := s.codeRows.RowBytes(ordinal)
	if !ok || len(row) != s.dims {
		return 0, fmt.Errorf("%w: column_graph quantized index %q code row ordinal=%d unavailable len=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, len(row), ok, s.dims)
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		s.recordScoreStats(stats, 1)
	}
	score, ok := scalarU8CenteredQuantizedCosineScore(s.centeredQuery, row)
	if !ok || math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph quantized index %q candidate ordinal=%d scalar_u8 score is not finite", s.indexName, ordinal)
	}
	return score, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreOrdinals(ordinals []int, dst []float64, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	if len(ordinals) == 0 {
		return dst, nil
	}
	if s == nil || !s.codeRows.Valid() || s.dims <= 0 || len(s.queryCode) != s.dims || !s.centeredQuery.ValidForDims(s.dims) {
		return dst[:0], fmt.Errorf("%w: column_graph quantized scalar_u8 scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, len(ordinals), false, true)
	}
	successCount := 0
	for i, ordinal := range ordinals {
		row, ok := s.codeRows.RowBytes(ordinal)
		if !ok || len(row) != s.dims {
			s.recordScoreStats(stats, successCount)
			return dst[:i], fmt.Errorf("%w: column_graph quantized index %q code row ordinal=%d unavailable len=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, len(row), ok, s.dims)
		}
		score, ok := scalarU8CenteredQuantizedCosineScore(s.centeredQuery, row)
		if !ok || math.IsNaN(score) || math.IsInf(score, 0) {
			s.recordScoreStats(stats, successCount)
			return dst[:i], fmt.Errorf("collections: column_graph quantized index %q candidate ordinal=%d scalar_u8 score is not finite", s.indexName, ordinal)
		}
		dst[i] = score
		successCount++
	}
	s.recordScoreStats(stats, successCount)
	return dst, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) recordScoreStats(stats *columnVectorGraphNativeSearchStats, count int) {
	if stats == nil || count <= 0 {
		return
	}
	count64 := uint64(count)
	stats.VisitedNodes += count64
	stats.CandidateFetches += count64
	stats.QuantizedScoreCalls += count64
	stats.QuantizedCodeBytesRead += count64 * uint64(s.dims)
}

func scalarU8CenteredQuantizedCosineScore(query vectorops.ScalarU8CenteredQuery, row []byte) (float64, bool) {
	dot, ok := vectorops.ScalarU8CenteredDot(query, row)
	if !ok {
		return 0, false
	}
	return float64(dot) / columnVectorGraphScalarU8CodeScale, true
}
