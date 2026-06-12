package collections

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/snissn/gomap/TreeDB/internal/brq"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/rabitq"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

const columnVectorGraphScalarU8CodeScale = 255.0 * 255.0

type columnVectorGraphQuantizedScorerKind uint8

const (
	columnVectorGraphQuantizedScorerKindNone columnVectorGraphQuantizedScorerKind = iota
	columnVectorGraphQuantizedScorerKindScalarU8
	columnVectorGraphQuantizedScorerKindRabitQ1Bit
	columnVectorGraphQuantizedScorerKindBRQ1Bit
)

type columnVectorGraphQuantizedScorer struct {
	kind     columnVectorGraphQuantizedScorerKind
	scalarU8 columnVectorGraphScalarU8QuantizedScorer
	rabitq   columnVectorGraphRabitQQuantizedScorer
	brq      columnVectorGraphBRQQuantizedScorer
}

func (r *columnVectorGraphPhysicalRowReader) prepareQuantizedScorer(mode columnVectorGraphNativeSearchQueryMode, indexName string, query []float32, queryInvNorm float32, scratch *columnVectorGraphNativeSearchScratch) (columnVectorGraphQuantizedScorer, error) {
	if r == nil {
		return columnVectorGraphQuantizedScorer{}, errNilColumnVectorGraphPhysicalRowReader
	}
	qdef, ok := findQuantizedVectorIndex(r.def, indexName)
	if !ok {
		return columnVectorGraphQuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, r.def.Name, indexName)
	}
	switch qdef.Codec {
	case QuantizedVectorCodecScalarU8:
		scorer, err := r.prepareScalarU8QuantizedScorer(mode, indexName, query, queryInvNorm, scratch)
		if err != nil {
			return columnVectorGraphQuantizedScorer{}, err
		}
		return columnVectorGraphQuantizedScorer{kind: columnVectorGraphQuantizedScorerKindScalarU8, scalarU8: scorer}, nil
	case rabitq.CodecName:
		scorer, err := r.prepareRabitQQuantizedScorer(mode, indexName, query, scratch)
		if err != nil {
			return columnVectorGraphQuantizedScorer{}, err
		}
		return columnVectorGraphQuantizedScorer{kind: columnVectorGraphQuantizedScorerKindRabitQ1Bit, rabitq: scorer}, nil
	case brq.CodecName:
		scorer, err := r.prepareBRQQuantizedScorer(mode, indexName, query, scratch)
		if err != nil {
			return columnVectorGraphQuantizedScorer{}, err
		}
		return columnVectorGraphQuantizedScorer{kind: columnVectorGraphQuantizedScorerKindBRQ1Bit, brq: scorer}, nil
	default:
		return columnVectorGraphQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q codec/version=(%q,%d) is unsupported", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, qdef.Codec, qdef.Version)
	}
}

func (s *columnVectorGraphQuantizedScorer) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: column_graph quantized scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	switch s.kind {
	case columnVectorGraphQuantizedScorerKindScalarU8:
		return s.scalarU8.scoreOrdinal(ordinal, scratch, stats)
	case columnVectorGraphQuantizedScorerKindRabitQ1Bit:
		return s.rabitq.scoreOrdinal(ordinal, scratch, stats)
	case columnVectorGraphQuantizedScorerKindBRQ1Bit:
		return s.brq.scoreOrdinal(ordinal, scratch, stats)
	default:
		return 0, fmt.Errorf("%w: column_graph quantized scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
}

func (s *columnVectorGraphQuantizedScorer) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if s == nil {
		return dst[:0], fmt.Errorf("%w: column_graph quantized scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	switch s.kind {
	case columnVectorGraphQuantizedScorerKindScalarU8:
		return s.scalarU8.scoreOrdinals(ordinals, dst, scratch, stats)
	case columnVectorGraphQuantizedScorerKindRabitQ1Bit:
		return s.rabitq.scoreOrdinals(ordinals, dst, scratch, stats)
	case columnVectorGraphQuantizedScorerKindBRQ1Bit:
		return s.brq.scoreOrdinals(ordinals, dst, scratch, stats)
	default:
		return dst[:0], fmt.Errorf("%w: column_graph quantized scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
}

type columnVectorGraphScalarU8QuantizedScorer struct {
	indexName   string
	dims        int
	codeRows    quantizedasset.CodeRowView
	codePayload []byte
	// centeredQuery aliases caller-owned search scratch.
	centeredQuery vectorops.ScalarU8CenteredQuery
}

func (r *columnVectorGraphPhysicalRowReader) prepareScalarU8QuantizedScorer(mode columnVectorGraphNativeSearchQueryMode, indexName string, query []float32, queryInvNorm float32, scratch *columnVectorGraphNativeSearchScratch) (columnVectorGraphScalarU8QuantizedScorer, error) {
	if r == nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, errNilColumnVectorGraphPhysicalRowReader
	}
	status, ok := r.quantizedAssetStatus[indexName]
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, r.def.Name, mode.String(), indexName)
	}
	if status.Err != nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q score-plane asset unavailable: %w", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, status.Err)
	}
	if status.Prepared == nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, r.def.Name, mode.String(), indexName)
	}
	qdef, ok := findQuantizedVectorIndex(r.def, indexName)
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, r.def.Name, indexName)
	}
	if status.Definition != qdef {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q prepared definition mismatch", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName)
	}
	if qdef.Codec != QuantizedVectorCodecScalarU8 || qdef.Version != 1 {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q codec/version=(%q,%d) is not scalar_u8 v1", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, qdef.Codec, qdef.Version)
	}
	if r.def.Metric != VectorMetricCosine {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q metric %q is unsupported for scalar_u8 scorer", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, r.def.Metric)
	}
	if len(query) != r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	if scratch == nil {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	prepared := status.Prepared
	if prepared.Rows() != r.RowCount() {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q prepared rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName, prepared.Rows(), r.RowCount())
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code row view unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	if codeRows.Rows() != r.RowCount() {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code row view rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName, codeRows.Rows(), r.RowCount())
	}
	if bytesPerRow := codeRows.BytesPerRow(); bytesPerRow != r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code bytes_per_row=%d want dimensions=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, bytesPerRow, r.def.Dimensions)
	}
	if elements := codeRows.ElementsPerRow(); elements != r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code elements_per_row=%d want dimensions=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, elements, r.def.Dimensions)
	}
	codePayload, ok := codeRows.PayloadBytes()
	if !ok || len(codePayload) != r.RowCount()*r.def.Dimensions {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code row payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(codePayload), ok, r.RowCount()*r.def.Dimensions)
	}
	centeredScratch := resizeColumnVectorGraphNativeScalarU8CenteredScratch(scratch.quantizedQueryCentered, r.def.Dimensions)[:r.def.Dimensions]
	var centeredSum int64
	for i, value := range query {
		code := columnVectorGraphScalarU8Code(value * queryInvNorm)
		centered := vectorops.ScalarU8CenteredValue(code)
		centeredScratch[i] = centered
		centeredSum += int64(centered)
	}
	centeredQuery, centeredScratch, ok := vectorops.PrepareScalarU8CenteredQueryFromCentered(centeredScratch, r.def.Dimensions, centeredSum)
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q centered scalar_u8 query unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	scratch.quantizedQueryCentered = centeredScratch
	return columnVectorGraphScalarU8QuantizedScorer{indexName: indexName, dims: r.def.Dimensions, codeRows: codeRows, codePayload: codePayload, centeredQuery: centeredQuery}, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if err := s.validatePrepared(); err != nil {
		return 0, err
	}
	rows := s.codeRows.Rows()
	if ordinal < 0 || ordinal >= rows || uint64(ordinal) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q code row ordinal=%d unavailable len=0 ok=false want %d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, s.dims)
	}
	return s.scoreRowIDPrepared(uint32(ordinal), scratch, stats)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	if len(ordinals) == 0 {
		return dst, nil
	}
	if err := s.validatePrepared(); err != nil {
		return dst[:0], err
	}
	if scratch == nil {
		return dst[:0], fmt.Errorf("collections: column_graph quantized scalar_u8 scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(ordinals))
	rowIDs := scratch.scoreTileRowIDs[:len(ordinals)]
	rows := s.codeRows.Rows()
	for i, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= rows || uint64(ordinal) > uint64(^uint32(0)) {
			return dst[:0], fmt.Errorf("%w: column_graph quantized index %q code row ordinal=%d unavailable len=0 ok=false want %d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, s.dims)
		}
		rowIDs[i] = uint32(ordinal)
	}
	return s.scoreRowIDsPrepared(rowIDs, dst, scratch, stats)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) validatePrepared() error {
	if s == nil || !s.codeRows.Valid() || s.dims <= 0 || len(s.codePayload) != s.codeRows.Rows()*s.dims || !s.centeredQuery.ValidForDims(s.dims) {
		return fmt.Errorf("%w: column_graph quantized scalar_u8 scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	return nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRowIDPrevalidated(rowID uint32, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if err := s.validatePrepared(); err != nil {
		return 0, err
	}
	return s.scoreRowIDPrepared(rowID, scratch, stats)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRowIDPrepared(rowID uint32, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if scratch == nil {
		return 0, fmt.Errorf("collections: column_graph quantized scalar_u8 scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, 1)
	rowIDs := scratch.scoreTileRowIDs[:1]
	rowIDs[0] = rowID
	scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, 1)
	dots := scratch.scoreTileQuantizedDots[:1]
	status := vectorops.DotScalarU8CenteredIndexedPrevalidated(dots, s.codePayload, s.centeredQuery, rowIDs, s.dims)
	if status.Invalid || status.Rows != 1 {
		return 0, fmt.Errorf("%w: column_graph quantized index %q scalar_u8 score invalid status=%+v rows=%d want 1", ErrVectorIndexSearchUnavailable, s.indexName, status, status.Rows)
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, status.Rows, status.Optimized, status.Fallback)
		s.recordScoreStats(stats, status.Rows)
	}
	return scalarU8QuantizedCosineScoreFromDot(dots[0]), nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(rowIDs) {
		dst = make([]float64, len(rowIDs))
	} else {
		dst = dst[:len(rowIDs)]
	}
	if len(rowIDs) == 0 {
		return dst, nil
	}
	if err := s.validatePrepared(); err != nil {
		return dst[:0], err
	}
	return s.scoreRowIDsPrepared(rowIDs, dst, scratch, stats)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRowIDsPrepared(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if scratch == nil {
		return dst[:0], fmt.Errorf("collections: column_graph quantized scalar_u8 scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, len(rowIDs))
	dots := scratch.scoreTileQuantizedDots[:len(rowIDs)]
	status := vectorops.DotScalarU8CenteredIndexedPrevalidated(dots, s.codePayload, s.centeredQuery, rowIDs, s.dims)
	if status.Invalid || status.Rows != len(rowIDs) {
		return dst[:0], fmt.Errorf("%w: column_graph quantized index %q scalar_u8 batch score invalid status=%+v rows=%d want %d", ErrVectorIndexSearchUnavailable, s.indexName, status, status.Rows, len(rowIDs))
	}
	for i, dot := range dots {
		dst[i] = scalarU8QuantizedCosineScoreFromDot(dot)
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, status.Rows, status.Optimized, status.Fallback)
	}
	s.recordScoreStats(stats, status.Rows)
	return dst, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if len(rowIDs) == 0 {
		return 0, nil
	}
	if err := s.validatePrepared(); err != nil {
		return 0, err
	}
	if scratch == nil {
		return 0, fmt.Errorf("collections: column_graph quantized scalar_u8 scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, len(rowIDs))
	dots := scratch.scoreTileQuantizedDots[:len(rowIDs)]
	status := vectorops.DotScalarU8CenteredIndexedPrevalidated(dots, s.codePayload, s.centeredQuery, rowIDs, s.dims)
	if status.Invalid || status.Rows != len(rowIDs) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q scalar_u8 batch score invalid status=%+v rows=%d want %d", ErrVectorIndexSearchUnavailable, s.indexName, status, status.Rows, len(rowIDs))
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, status.Rows, status.Optimized, status.Fallback)
	}
	s.recordScoreStats(stats, status.Rows)
	for i, rowID := range rowIDs {
		candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: scalarU8QuantizedCosineScoreFromDot(dots[i])}
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontier(candidate)
		}
	}
	return len(rowIDs), nil
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

type columnVectorGraphRabitQQuantizedScorer struct {
	indexName                     string
	plan                          *rabitq.Plan
	query                         rabitq.Query
	queryWeightSum                float64
	queryByteMismatchWeights      []float64
	codeRows                      quantizedasset.CodeRowView
	codePayload                   []byte
	codeCountPayload              []byte
	quantizedDotProductInvPayload []byte
	bytesPerCode                  int
	codeDimensions                int
}

func (r *columnVectorGraphPhysicalRowReader) prepareRabitQQuantizedScorer(mode columnVectorGraphNativeSearchQueryMode, indexName string, query []float32, scratch *columnVectorGraphNativeSearchScratch) (columnVectorGraphRabitQQuantizedScorer, error) {
	if r == nil {
		return columnVectorGraphRabitQQuantizedScorer{}, errNilColumnVectorGraphPhysicalRowReader
	}
	status, ok := r.quantizedAssetStatus[indexName]
	if !ok {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, r.def.Name, mode.String(), indexName)
	}
	if status.Err != nil {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q score-plane asset unavailable: %w", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, status.Err)
	}
	if status.Prepared == nil {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, r.def.Name, mode.String(), indexName)
	}
	qdef, ok := findQuantizedVectorIndex(r.def, indexName)
	if !ok {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, r.def.Name, indexName)
	}
	if status.Definition != qdef {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q prepared definition mismatch", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName)
	}
	if qdef.Codec != rabitq.CodecName || qdef.Version != rabitq.CodecVersion {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q codec/version=(%q,%d) is not rabitq_1bit v%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, qdef.Codec, qdef.Version, rabitq.CodecVersion)
	}
	if r.def.Metric != VectorMetricCosine {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q metric %q is unsupported for rabitq_1bit scorer", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, r.def.Metric)
	}
	if len(query) != r.def.Dimensions {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	if scratch == nil {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	prepared := status.Prepared
	if prepared.Rows() != r.RowCount() {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q prepared rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName, prepared.Rows(), r.RowCount())
	}
	plan := status.RabitQPlan
	if plan == nil || plan.VectorDimensions() != r.def.Dimensions {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q rabitq_1bit plan unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code row view unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	if codeRows.Rows() != r.RowCount() {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code row view rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName, codeRows.Rows(), r.RowCount())
	}
	if bytesPerRow := codeRows.BytesPerRow(); bytesPerRow != plan.BytesPerCode() {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code bytes_per_row=%d want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, bytesPerRow, plan.BytesPerCode())
	}
	if elements := codeRows.ElementsPerRow(); elements != plan.CodeDimensions() {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code elements_per_row=%d want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, elements, plan.CodeDimensions())
	}
	codePayload, ok := codeRows.PayloadBytes()
	if !ok || len(codePayload) != r.RowCount()*plan.BytesPerCode() {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(codePayload), ok, r.RowCount()*plan.BytesPerCode())
	}
	codeCountPayload, ok := prepared.Uint32Payload(quantizedasset.RoleCodeCount)
	if !ok || len(codeCountPayload) != r.RowCount()*4 {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code_count payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(codeCountPayload), ok, r.RowCount()*4)
	}
	qdpInvPayload, ok := prepared.Float32Payload(quantizedasset.RoleQuantizedDotProductInv)
	if !ok || len(qdpInvPayload) != r.RowCount()*4 {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q quantized_dot_product_inv payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(qdpInvPayload), ok, r.RowCount()*4)
	}
	encodedQuery, err := plan.EncodeQuery(query, &scratch.quantizedRabitQWorkspace)
	if err != nil {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q rabitq_1bit query encode: %v", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, err)
	}
	queryByteMismatchWeights, queryWeightSum, ok := rabitq.PrepareQueryByteMismatchWeights(encodedQuery, &scratch.quantizedRabitQWorkspace)
	if !ok {
		return columnVectorGraphRabitQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q rabitq_1bit query byte tables unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	return columnVectorGraphRabitQQuantizedScorer{indexName: indexName, plan: plan, query: encodedQuery, queryWeightSum: queryWeightSum, queryByteMismatchWeights: queryByteMismatchWeights, codeRows: codeRows, codePayload: codePayload, codeCountPayload: codeCountPayload, quantizedDotProductInvPayload: qdpInvPayload, bytesPerCode: plan.BytesPerCode(), codeDimensions: plan.CodeDimensions()}, nil
}

func (s *columnVectorGraphRabitQQuantizedScorer) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	if scratch == nil {
		return 0, fmt.Errorf("collections: column_graph quantized rabitq_1bit scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	score, err := s.scoreOrdinalUnchecked(ordinal)
	if err != nil {
		return 0, err
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		s.recordScoreStats(stats, 1)
	}
	return score, nil
}

func (s *columnVectorGraphRabitQQuantizedScorer) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	if len(ordinals) == 0 {
		return dst, nil
	}
	if err := s.validate(); err != nil {
		return dst[:0], err
	}
	if scratch == nil {
		return dst[:0], fmt.Errorf("collections: column_graph quantized rabitq_1bit scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	for i, ordinal := range ordinals {
		score, err := s.scoreOrdinalUnchecked(ordinal)
		if err != nil {
			return dst[:i], err
		}
		dst[i] = score
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, len(ordinals), false, true)
		s.recordScoreStats(stats, len(ordinals))
	}
	return dst, nil
}

func (s *columnVectorGraphRabitQQuantizedScorer) validate() error {
	if s == nil || s.plan == nil || !s.codeRows.Valid() || s.bytesPerCode <= 0 || s.codeDimensions <= 0 || len(s.codePayload) != s.codeRows.Rows()*s.bytesPerCode || len(s.codeCountPayload) != s.codeRows.Rows()*4 || len(s.quantizedDotProductInvPayload) != s.codeRows.Rows()*4 || !rabitqQueryShapeValidForPlan(s.query, s.plan) || !rabitq.QueryByteMismatchWeightsValid(s.query, s.queryByteMismatchWeights, s.queryWeightSum) {
		return fmt.Errorf("%w: column_graph quantized rabitq_1bit scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	return nil
}

func (s *columnVectorGraphRabitQQuantizedScorer) scoreOrdinalUnchecked(ordinal int) (float64, error) {
	rows := s.codeRows.Rows()
	if ordinal < 0 || ordinal >= rows {
		return 0, fmt.Errorf("%w: column_graph quantized index %q rabitq_1bit code row ordinal=%d unavailable want rows=%d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, rows)
	}
	start := ordinal * s.bytesPerCode
	end := start + s.bytesPerCode
	if start < 0 || end < start || end > len(s.codePayload) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q rabitq_1bit code row ordinal=%d range [%d,%d) outside payload=%d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, start, end, len(s.codePayload))
	}
	sideStart := ordinal * 4
	if sideStart < 0 || sideStart > len(s.codeCountPayload)-4 || sideStart > len(s.quantizedDotProductInvPayload)-4 {
		return 0, fmt.Errorf("%w: column_graph quantized index %q rabitq_1bit side row ordinal=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, ordinal)
	}
	code := s.codePayload[start:end]
	codeCount := binary.LittleEndian.Uint32(s.codeCountPayload[sideStart : sideStart+4])
	if codeCount > uint32(s.codeDimensions) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q rabitq_1bit code_count ordinal=%d value=%d exceeds code_dimensions=%d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, codeCount, s.codeDimensions)
	}
	qdpInv := math.Float32frombits(binary.LittleEndian.Uint32(s.quantizedDotProductInvPayload[sideStart : sideStart+4]))
	score, ok := rabitqQuantizedCosineScoreWithByteTablesPrevalidated(s.query, code, qdpInv, s.queryByteMismatchWeights, s.queryWeightSum)
	if !ok {
		return 0, fmt.Errorf("%w: column_graph quantized index %q rabitq_1bit score ordinal=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, ordinal)
	}
	return score, nil
}

func (s *columnVectorGraphRabitQQuantizedScorer) recordScoreStats(stats *columnVectorGraphNativeSearchStats, count int) {
	if stats == nil || count <= 0 {
		return
	}
	count64 := uint64(count)
	stats.VisitedNodes += count64
	stats.CandidateFetches += count64
	stats.QuantizedScoreCalls += count64
	stats.QuantizedCodeBytesRead += count64 * uint64(s.bytesPerCode)
}

type columnVectorGraphBRQQuantizedScorer struct {
	indexName                     string
	plan                          *brq.Plan
	query                         brq.Query
	codeRows                      quantizedasset.CodeRowView
	codePayload                   []byte
	codeCountPayload              []byte
	quantizedDotProductInvPayload []byte
	bytesPerCode                  int
	codeDimensions                int
}

func (r *columnVectorGraphPhysicalRowReader) prepareBRQQuantizedScorer(mode columnVectorGraphNativeSearchQueryMode, indexName string, query []float32, scratch *columnVectorGraphNativeSearchScratch) (columnVectorGraphBRQQuantizedScorer, error) {
	if r == nil {
		return columnVectorGraphBRQQuantizedScorer{}, errNilColumnVectorGraphPhysicalRowReader
	}
	status, ok := r.quantizedAssetStatus[indexName]
	if !ok {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, r.def.Name, mode.String(), indexName)
	}
	if status.Err != nil {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q score-plane asset unavailable: %w", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, status.Err)
	}
	if status.Prepared == nil {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q has no prepared quantized score-plane asset", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, r.def.Name, mode.String(), indexName)
	}
	qdef, ok := findQuantizedVectorIndex(r.def, indexName)
	if !ok {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, r.def.Name, indexName)
	}
	if status.Definition != qdef {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q prepared definition mismatch", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName)
	}
	if qdef.Codec != brq.CodecName || qdef.Version != brq.CodecVersion {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q codec/version=(%q,%d) is not brq_1bit v%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, qdef.Codec, qdef.Version, brq.CodecVersion)
	}
	if r.def.Metric != VectorMetricCosine {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q quantized index %q metric %q is unsupported for brq_1bit scorer", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, indexName, r.def.Metric)
	}
	if len(query) != r.def.Dimensions {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	if scratch == nil {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	prepared := status.Prepared
	if prepared.Rows() != r.RowCount() {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q prepared rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName, prepared.Rows(), r.RowCount())
	}
	plan := status.BRQPlan
	if plan == nil || plan.VectorDimensions() != r.def.Dimensions {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q brq_1bit plan unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code row view unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	if codeRows.Rows() != r.RowCount() {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code row view rows=%d want graph rows=%d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, r.def.Name, mode.String(), indexName, codeRows.Rows(), r.RowCount())
	}
	if bytesPerRow := codeRows.BytesPerRow(); bytesPerRow != plan.BytesPerCode() {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code bytes_per_row=%d want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, bytesPerRow, plan.BytesPerCode())
	}
	if elements := codeRows.ElementsPerRow(); elements != plan.CodeDimensions() {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code elements_per_row=%d want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, elements, plan.CodeDimensions())
	}
	codePayload, ok := codeRows.PayloadBytes()
	if !ok || len(codePayload) != r.RowCount()*plan.BytesPerCode() {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q packed code payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(codePayload), ok, r.RowCount()*plan.BytesPerCode())
	}
	codeCountPayload, ok := prepared.Uint32Payload(quantizedasset.RoleCodeCount)
	if !ok || len(codeCountPayload) != r.RowCount()*4 {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q code_count payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(codeCountPayload), ok, r.RowCount()*4)
	}
	qdpInvPayload, ok := prepared.Float32Payload(quantizedasset.RoleQuantizedDotProductInv)
	if !ok || len(qdpInvPayload) != r.RowCount()*4 {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q quantized_dot_product_inv payload bytes=%d ok=%v want %d", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, len(qdpInvPayload), ok, r.RowCount()*4)
	}
	encodedQuery, err := plan.EncodeQuery(query, &scratch.quantizedBRQWorkspace)
	if err != nil {
		return columnVectorGraphBRQQuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q brq_1bit query encode: %v", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, err)
	}
	return columnVectorGraphBRQQuantizedScorer{indexName: indexName, plan: plan, query: encodedQuery, codeRows: codeRows, codePayload: codePayload, codeCountPayload: codeCountPayload, quantizedDotProductInvPayload: qdpInvPayload, bytesPerCode: plan.BytesPerCode(), codeDimensions: plan.CodeDimensions()}, nil
}

func (s *columnVectorGraphBRQQuantizedScorer) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	if scratch == nil {
		return 0, fmt.Errorf("collections: column_graph quantized brq_1bit scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	score, err := s.scoreOrdinalUnchecked(ordinal)
	if err != nil {
		return 0, err
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		s.recordScoreStats(stats, 1)
	}
	return score, nil
}

func (s *columnVectorGraphBRQQuantizedScorer) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	if len(ordinals) == 0 {
		return dst, nil
	}
	if err := s.validate(); err != nil {
		return dst[:0], err
	}
	if scratch == nil {
		return dst[:0], fmt.Errorf("collections: column_graph quantized brq_1bit scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	for i, ordinal := range ordinals {
		score, err := s.scoreOrdinalUnchecked(ordinal)
		if err != nil {
			return dst[:i], err
		}
		dst[i] = score
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, len(ordinals), false, true)
		s.recordScoreStats(stats, len(ordinals))
	}
	return dst, nil
}

func (s *columnVectorGraphBRQQuantizedScorer) validate() error {
	if s == nil || s.plan == nil || !s.codeRows.Valid() || s.bytesPerCode <= 0 || s.codeDimensions <= 0 || len(s.codePayload) != s.codeRows.Rows()*s.bytesPerCode || len(s.codeCountPayload) != s.codeRows.Rows()*4 || len(s.quantizedDotProductInvPayload) != s.codeRows.Rows()*4 || !brqQueryShapeValidForPlan(s.query, s.plan) {
		return fmt.Errorf("%w: column_graph quantized brq_1bit scorer is unavailable", ErrVectorIndexSearchUnavailable)
	}
	return nil
}

func (s *columnVectorGraphBRQQuantizedScorer) scoreOrdinalUnchecked(ordinal int) (float64, error) {
	rows := s.codeRows.Rows()
	if ordinal < 0 || ordinal >= rows {
		return 0, fmt.Errorf("%w: column_graph quantized index %q brq_1bit code row ordinal=%d unavailable want rows=%d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, rows)
	}
	start := ordinal * s.bytesPerCode
	end := start + s.bytesPerCode
	if start < 0 || end < start || end > len(s.codePayload) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q brq_1bit code row ordinal=%d range [%d,%d) outside payload=%d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, start, end, len(s.codePayload))
	}
	sideStart := ordinal * 4
	if sideStart < 0 || sideStart > len(s.codeCountPayload)-4 || sideStart > len(s.quantizedDotProductInvPayload)-4 {
		return 0, fmt.Errorf("%w: column_graph quantized index %q brq_1bit side row ordinal=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, ordinal)
	}
	code := s.codePayload[start:end]
	codeCount := binary.LittleEndian.Uint32(s.codeCountPayload[sideStart : sideStart+4])
	if codeCount > uint32(s.codeDimensions) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q brq_1bit code_count ordinal=%d value=%d exceeds code_dimensions=%d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, codeCount, s.codeDimensions)
	}
	qdpInv := math.Float32frombits(binary.LittleEndian.Uint32(s.quantizedDotProductInvPayload[sideStart : sideStart+4]))
	score, ok := brqQuantizedCosineScore(s.query, code, qdpInv)
	if !ok {
		return 0, fmt.Errorf("%w: column_graph quantized index %q brq_1bit score ordinal=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, ordinal)
	}
	return score, nil
}

func (s *columnVectorGraphBRQQuantizedScorer) recordScoreStats(stats *columnVectorGraphNativeSearchStats, count int) {
	if stats == nil || count <= 0 {
		return
	}
	count64 := uint64(count)
	stats.VisitedNodes += count64
	stats.CandidateFetches += count64
	stats.QuantizedScoreCalls += count64
	stats.QuantizedCodeBytesRead += count64 * uint64(s.bytesPerCode)
	stats.QuantizedScoreCodecBRQ1Bit = 1
	stats.BRQ1BitQueryWeightBits = brq.QueryWeightBits
	stats.BRQ1BitBitProductPasses += count64 * 2
	stats.BRQ1BitQueryWeightScale = s.query.QueryWeightScale
}

func brqQueryShapeValidForPlan(query brq.Query, plan *brq.Plan) bool {
	return plan != nil && query.CodeDimensions == plan.CodeDimensions() && len(query.SignBits) == plan.BytesPerCode() && len(query.Weights) == plan.CodeDimensions() && len(query.PosQ1) == plan.BytesPerCode() && len(query.PosQ2) == plan.BytesPerCode() && len(query.PosQ4) == plan.BytesPerCode() && len(query.PosQ8) == plan.BytesPerCode() && len(query.NegQ1) == plan.BytesPerCode() && len(query.NegQ2) == plan.BytesPerCode() && len(query.NegQ4) == plan.BytesPerCode() && len(query.NegQ8) == plan.BytesPerCode() && query.QueryWeightScale > 0 && !math.IsNaN(query.QueryWeightScale) && !math.IsInf(query.QueryWeightScale, 0) && query.QueryWeightSumInt > 0
}

func brqQuantizedCosineScore(query brq.Query, code []byte, quantizedDotProductInv float32) (float64, bool) {
	if query.CodeDimensions <= 0 || len(code) != len(query.SignBits) || len(query.Weights) < query.CodeDimensions || quantizedDotProductInv <= 0 || math.IsNaN(float64(quantizedDotProductInv)) || math.IsInf(float64(quantizedDotProductInv), 0) || query.QueryWeightScale <= 0 || math.IsNaN(query.QueryWeightScale) || math.IsInf(query.QueryWeightScale, 0) || query.QueryWeightSumInt == 0 {
		return 0, false
	}
	posSet := brqBitProductNoValidate(code, query.PosQ1, query.PosQ2, query.PosQ4, query.PosQ8)
	negSet := brqBitProductNoValidate(code, query.NegQ1, query.NegQ2, query.NegQ4, query.NegQ8)
	if negSet > query.NegativeWeightSumInt {
		return 0, false
	}
	matchWeight := posSet + (query.NegativeWeightSumInt - negSet)
	signedWeight := int64(2*matchWeight) - int64(query.QueryWeightSumInt)
	score := float64(signedWeight) * query.QueryWeightScale / (float64(quantizedDotProductInv) * float64(query.CodeDimensions))
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, false
	}
	return score, true
}

func brqBitProductNoValidate(code, q1, q2, q4, q8 []byte) uint32 {
	var total uint32
	for i, b := range code {
		total += uint32(bits.OnesCount8(b & q1[i]))
		total += 2 * uint32(bits.OnesCount8(b&q2[i]))
		total += 4 * uint32(bits.OnesCount8(b&q4[i]))
		total += 8 * uint32(bits.OnesCount8(b&q8[i]))
	}
	return total
}

func rabitqQueryShapeValidForPlan(query rabitq.Query, plan *rabitq.Plan) bool {
	return plan != nil && query.CodeDimensions == plan.CodeDimensions() && len(query.SignBits) == plan.BytesPerCode() && len(query.AbsWeights) == plan.CodeDimensions() && query.WeightSum > 0 && !math.IsNaN(float64(query.WeightSum)) && !math.IsInf(float64(query.WeightSum), 0)
}

func rabitqQuantizedCosineScoreWithByteTables(query rabitq.Query, code []byte, quantizedDotProductInv float32, byteMismatchWeights []float64, queryWeightSum float64) (float64, bool) {
	if !rabitq.QueryByteMismatchWeightsValid(query, byteMismatchWeights, queryWeightSum) || len(code) != len(query.SignBits) || quantizedDotProductInv <= 0 || math.IsNaN(float64(quantizedDotProductInv)) || math.IsInf(float64(quantizedDotProductInv), 0) {
		return 0, false
	}
	return rabitqQuantizedCosineScoreWithByteTablesPrevalidated(query, code, quantizedDotProductInv, byteMismatchWeights, queryWeightSum)
}

// rabitqQuantizedCosineScoreWithByteTablesPrevalidated is the hot-loop form for
// callers that already validated the query shape and byte mismatch table. It
// still checks per-row code/qdp side inputs so corrupted row data fails closed.
func rabitqQuantizedCosineScoreWithByteTablesPrevalidated(query rabitq.Query, code []byte, quantizedDotProductInv float32, byteMismatchWeights []float64, queryWeightSum float64) (float64, bool) {
	if len(code) != len(query.SignBits) || quantizedDotProductInv <= 0 || math.IsNaN(float64(quantizedDotProductInv)) || math.IsInf(float64(quantizedDotProductInv), 0) {
		return 0, false
	}
	mismatchWeight := rabitqByteTableMismatchWeight(query.SignBits, code, byteMismatchWeights)
	weightedSignDot := queryWeightSum - 2*mismatchWeight
	score := weightedSignDot / (float64(quantizedDotProductInv) * float64(query.CodeDimensions))
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, false
	}
	return score, true
}

func rabitqQuantizedCosineScore(query rabitq.Query, code []byte, quantizedDotProductInv float32) (float64, bool) {
	if query.CodeDimensions <= 0 || len(query.AbsWeights) < query.CodeDimensions || len(query.SignBits) == 0 || len(code) != len(query.SignBits) || quantizedDotProductInv <= 0 || math.IsNaN(float64(quantizedDotProductInv)) || math.IsInf(float64(quantizedDotProductInv), 0) {
		return 0, false
	}
	var weightedSignDot float64
	for i, weight32 := range query.AbsWeights[:query.CodeDimensions] {
		weight := float64(weight32)
		if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
			return 0, false
		}
		mask := byte(1 << uint(i&7))
		if (code[i>>3]&mask != 0) == (query.SignBits[i>>3]&mask != 0) {
			weightedSignDot += weight
		} else {
			weightedSignDot -= weight
		}
	}
	score := weightedSignDot / (float64(quantizedDotProductInv) * float64(query.CodeDimensions))
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, false
	}
	return score, true
}

func scalarU8CenteredQuantizedCosineScore(query vectorops.ScalarU8CenteredQuery, row []byte) (float64, bool) {
	dot, ok := vectorops.ScalarU8CenteredDot(query, row)
	if !ok {
		return 0, false
	}
	return scalarU8QuantizedCosineScoreFromDot(dot), true
}

func scalarU8QuantizedCosineScoreFromDot(dot int64) float64 {
	return float64(dot) / columnVectorGraphScalarU8CodeScale
}
