package collections

import (
	"encoding/binary"
	"errors"
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
	codeSums    []uint32
	// centeredQuery aliases caller-owned search scratch.
	centeredQuery vectorops.ScalarU8CenteredQuery
	// alphaLookup is non-nil only for scalar_u8 per_granule_alpha scoring.
	alphaLookup *columnVectorGraphScalarU8AlphaLookup
	queryAlpha  float32
	// alphaScoreScales stores queryAlpha*granuleAlpha/(255*255), indexed by
	// alpha granule. It aliases caller-owned scratch and is valid for the
	// lifetime of the prepared search using that scratch.
	alphaScoreScales []float64
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
	if !quantizedVectorIndexDefinitionValuesEqual(status.Definition, qdef) {
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
	queryAlpha := float32(1)
	var alphaLookup *columnVectorGraphScalarU8AlphaLookup
	if !scalarU8CalibrationIsLegacy(qdef) {
		alphaLookup = status.ScalarU8Alpha
		if err := alphaLookup.validateForScoring(r.RowCount()); err != nil {
			return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q scalar_u8 alpha metadata unavailable: %w", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), indexName, err)
		}
		var alphaErr error
		queryAlpha, alphaErr = columnVectorGraphScalarU8QueryAlpha(qdef, query, queryInvNorm, scratch)
		if alphaErr != nil {
			return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q scalar_u8 query alpha: %v", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName, alphaErr)
		}
		scratch.quantizedScalarU8AlphaScales = ensureColumnVectorGraphNativeFloat64Scratch(scratch.quantizedScalarU8AlphaScales, alphaLookup.granules)
		if !prepareColumnVectorGraphScalarU8AlphaScoreScales(scratch.quantizedScalarU8AlphaScales[:alphaLookup.granules], alphaLookup, queryAlpha) {
			return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q scalar_u8 alpha score scales unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
		}
	}
	centeredScratch := resizeColumnVectorGraphNativeScalarU8CenteredScratch(scratch.quantizedQueryCentered, r.def.Dimensions)[:r.def.Dimensions]
	var halfScratch []int8
	if len(status.ScalarU8CodeSums) == r.RowCount() && vectorops.DotScalarU8CenteredIndexedPreparedByteEligible(r.def.Dimensions) {
		halfScratch = resizeColumnVectorGraphNativeInt8Scratch(scratch.quantizedQueryHalf, r.def.Dimensions)[:r.def.Dimensions]
	}
	queryCodeScale := queryInvNorm
	if alphaLookup != nil {
		queryCodeScale *= 1 / queryAlpha
	}
	var centeredSum int64
	for i, value := range query {
		code := columnVectorGraphScalarU8Code(value * queryCodeScale)
		centered := vectorops.ScalarU8CenteredValue(code)
		centeredScratch[i] = centered
		if halfScratch != nil {
			halfScratch[i] = int8(int(code) - 128)
		}
		centeredSum += int64(centered)
	}
	var centeredQuery vectorops.ScalarU8CenteredQuery
	if halfScratch != nil {
		centeredQuery, centeredScratch, ok = vectorops.PrepareScalarU8CenteredQueryFromCenteredWithHalf(centeredScratch, halfScratch, r.def.Dimensions, centeredSum)
		scratch.quantizedQueryHalf = halfScratch
	} else {
		centeredQuery, centeredScratch, ok = vectorops.PrepareScalarU8CenteredQueryFromCentered(centeredScratch, r.def.Dimensions, centeredSum)
	}
	if !ok {
		return columnVectorGraphScalarU8QuantizedScorer{}, fmt.Errorf("%w: %w: column_graph %q query_mode=%s quantized index %q centered scalar_u8 query unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, r.def.Name, mode.String(), indexName)
	}
	scratch.quantizedQueryCentered = centeredScratch
	var alphaScoreScales []float64
	if alphaLookup != nil {
		alphaScoreScales = scratch.quantizedScalarU8AlphaScales[:alphaLookup.granules]
	}
	return columnVectorGraphScalarU8QuantizedScorer{indexName: indexName, dims: r.def.Dimensions, codeRows: codeRows, codePayload: codePayload, codeSums: status.ScalarU8CodeSums, centeredQuery: centeredQuery, alphaLookup: alphaLookup, queryAlpha: queryAlpha, alphaScoreScales: alphaScoreScales}, nil
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
	if len(s.codeSums) != 0 && len(s.codeSums) != s.codeRows.Rows() {
		return fmt.Errorf("%w: column_graph quantized scalar_u8 scorer code sums are unavailable", ErrVectorIndexSearchUnavailable)
	}
	if s.alphaLookup != nil {
		if !validColumnVectorGraphScalarU8Alpha(s.queryAlpha) || !s.alphaLookup.validShapeForRows(s.codeRows.Rows()) {
			return fmt.Errorf("%w: column_graph quantized scalar_u8 alpha scorer is unavailable", ErrVectorIndexSearchUnavailable)
		}
		if len(s.alphaScoreScales) > 0 && len(s.alphaScoreScales) != s.alphaLookup.granules {
			return fmt.Errorf("%w: column_graph quantized scalar_u8 alpha scorer is unavailable", ErrVectorIndexSearchUnavailable)
		}
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
	dot, err := s.scoreRawDotRowIDPrepared(rowID, scratch, stats)
	if err != nil {
		return 0, err
	}
	score, ok := s.scoreFromDotRowID(dot, rowID)
	if !ok {
		return 0, fmt.Errorf("%w: column_graph quantized index %q scalar_u8 alpha row_id=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, rowID)
	}
	return score, nil
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
	var dotScratch []int64
	if scratch != nil {
		dotScratch = scratch.scoreTileQuantizedDots
	}
	dots, err := s.scoreRawDotRowIDsPrepared(rowIDs, dotScratch, scratch, stats)
	if err != nil {
		return dst[:0], err
	}
	if s.alphaLookup == nil {
		for i, dot := range dots {
			dst[i] = scalarU8QuantizedCosineScoreFromDot(dot)
		}
		return dst, nil
	}
	for i, dot := range dots {
		score, ok := s.scoreAlphaFromDotRowID(dot, rowIDs[i])
		if !ok {
			return dst[:0], fmt.Errorf("%w: column_graph quantized index %q scalar_u8 alpha row_id=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, rowIDs[i])
		}
		dst[i] = score
	}
	return dst, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreFromDotRowID(dot int64, rowID uint32) (float64, bool) {
	if s == nil || s.alphaLookup == nil {
		return scalarU8QuantizedCosineScoreFromDot(dot), true
	}
	return s.scoreAlphaFromDotRowID(dot, rowID)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreAlphaFromDotRowID(dot int64, rowID uint32) (float64, bool) {
	if s == nil || s.alphaLookup == nil {
		return 0, false
	}
	row := int(rowID)
	if len(s.alphaScoreScales) == s.alphaLookup.granules {
		if row < 0 || row >= s.alphaLookup.rows {
			return 0, false
		}
		if uniform := s.alphaLookup.uniformGranuleRows; uniform > 0 {
			granule := row / uniform
			if granule >= len(s.alphaScoreScales) {
				granule = len(s.alphaScoreScales) - 1
			}
			return float64(dot) * s.alphaScoreScales[granule], true
		}
		granule, ok := s.alphaLookup.granuleForRow(row)
		if !ok {
			return 0, false
		}
		return float64(dot) * s.alphaScoreScales[granule], true
	}
	alpha, _, ok := s.alphaLookup.AlphaForRow(row)
	if !ok {
		return 0, false
	}
	return scalarU8QuantizedCosineScoreFromDot(dot) * float64(s.queryAlpha) * float64(alpha), true
}

func prepareColumnVectorGraphScalarU8AlphaScoreScales(dst []float64, lookup *columnVectorGraphScalarU8AlphaLookup, queryAlpha float32) bool {
	if lookup == nil || len(dst) != lookup.granules || len(lookup.alphaPayload) != lookup.granules*4 || !validColumnVectorGraphScalarU8Alpha(queryAlpha) {
		return false
	}
	queryScale := float64(queryAlpha) / columnVectorGraphScalarU8CodeScale
	for granule := 0; granule < lookup.granules; granule++ {
		alpha := math.Float32frombits(binary.LittleEndian.Uint32(lookup.alphaPayload[granule*4 : granule*4+4]))
		if !validColumnVectorGraphScalarU8Alpha(alpha) {
			return false
		}
		dst[granule] = queryScale * float64(alpha)
	}
	return true
}

func columnVectorGraphScalarU8QueryAlpha(q QuantizedVectorIndexDefinition, query []float32, queryInvNorm float32, scratch *columnVectorGraphNativeSearchScratch) (float32, error) {
	if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 || scalarU8CalibrationIsLegacy(q) {
		return 1, nil
	}
	cfg := q.ScalarU8Calibration
	if cfg == nil || cfg.Mode != ScalarU8CalibrationModePerGranuleAlpha {
		return 0, fmt.Errorf("mode %q is not per_granule_alpha", scalarU8CalibrationMode(q))
	}
	if len(query) == 0 || queryInvNorm <= 0 || math.IsNaN(float64(queryInvNorm)) || math.IsInf(float64(queryInvNorm), 0) {
		return 0, errors.New("query norm is invalid")
	}
	policy := cfg.AlphaPolicy
	switch policy.Name {
	case "", ScalarU8AlphaPolicyMaxAbs:
		alpha := columnVectorGraphScalarU8MaxAbsQueryAlpha(query, queryInvNorm)
		if !validColumnVectorGraphScalarU8Alpha(alpha) {
			return 0, fmt.Errorf("computed query alpha=%v is not positive finite", alpha)
		}
		return alpha, nil
	case ScalarU8AlphaPolicyAbsQuantile:
		if policy.QuantilePPM != ScalarU8AlphaPolicyAbsQuantilePPM999 {
			return 0, fmt.Errorf("abs_quantile quantile_ppm=%d is unsupported", policy.QuantilePPM)
		}
		if scratch == nil {
			return 0, errColumnVectorGraphNativeSearchScratchRequired
		}
		alpha := columnVectorGraphScalarU8AbsQuantileQueryAlpha(query, queryInvNorm, policy.QuantilePPM, scratch)
		if !validColumnVectorGraphScalarU8Alpha(alpha) {
			return 0, fmt.Errorf("computed query alpha=%v is not positive finite", alpha)
		}
		return alpha, nil
	default:
		return 0, fmt.Errorf("alpha_policy name %q is unsupported", policy.Name)
	}
}

func columnVectorGraphScalarU8MaxAbsQueryAlpha(query []float32, queryInvNorm float32) float32 {
	maxAbs := float32(0)
	for _, value := range query {
		normalized := value * queryInvNorm
		if normalized < 0 {
			normalized = -normalized
		}
		if normalized > maxAbs {
			maxAbs = normalized
		}
	}
	return maxAbs
}

func columnVectorGraphScalarU8AbsQuantileQueryAlpha(query []float32, queryInvNorm float32, quantilePPM uint32, scratch *columnVectorGraphNativeSearchScratch) float32 {
	if len(query) == 0 || scratch == nil {
		return 0
	}
	values := scratch.scoreScratch.Float32Values
	if cap(values) < len(query) {
		values = ensureColumnVectorGraphNativeFloat32Scratch(values, len(query))
	}
	values = values[:len(query)]
	maxAbs := float32(0)
	for i, value := range query {
		normalized := value * queryInvNorm
		if normalized < 0 {
			normalized = -normalized
		}
		if normalized > maxAbs {
			maxAbs = normalized
		}
		values[i] = normalized
	}
	scratch.scoreScratch.Float32Values = values
	idx := int((uint64(len(values))*uint64(quantilePPM) + 999999) / 1000000)
	if idx <= 0 {
		idx = 1
	}
	if idx > len(values) {
		idx = len(values)
	}
	alpha := columnVectorGraphSelectKthFloat32(values, idx-1)
	if validColumnVectorGraphScalarU8Alpha(alpha) {
		return alpha
	}
	// Match build-time abs_quantile's sparse-granule positive fallback: if
	// the requested quantile lands on zero but the query has a positive finite
	// coordinate, use the smallest positive finite alpha instead of failing the
	// otherwise valid query as unavailable.
	if maxAbs > 0 {
		fallback := float32(0)
		for _, candidate := range values {
			if validColumnVectorGraphScalarU8Alpha(candidate) && (fallback == 0 || candidate < fallback) {
				fallback = candidate
			}
		}
		if validColumnVectorGraphScalarU8Alpha(fallback) {
			return fallback
		}
		if validColumnVectorGraphScalarU8Alpha(maxAbs) {
			return maxAbs
		}
	}
	return alpha
}

func columnVectorGraphSelectKthFloat32(values []float32, k int) float32 {
	if len(values) == 0 || k < 0 || k >= len(values) {
		return 0
	}
	left, right := 0, len(values)-1
	for left < right {
		pivot := values[(left+right)>>1]
		i, j := left, right
		for i <= j {
			for values[i] < pivot {
				i++
			}
			for values[j] > pivot {
				j--
			}
			if i <= j {
				values[i], values[j] = values[j], values[i]
				i++
				j--
			}
		}
		if k <= j {
			right = j
		} else if k >= i {
			left = i
		} else {
			return values[k]
		}
	}
	return values[left]
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRawDotOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int64, error) {
	if err := s.validatePrepared(); err != nil {
		return 0, err
	}
	rows := s.codeRows.Rows()
	if ordinal < 0 || ordinal >= rows || uint64(ordinal) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("%w: column_graph quantized index %q code row ordinal=%d unavailable len=0 ok=false want %d", ErrVectorIndexSearchUnavailable, s.indexName, ordinal, s.dims)
	}
	return s.scoreRawDotRowIDPrepared(uint32(ordinal), scratch, stats)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRawDotRowIDPrepared(rowID uint32, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int64, error) {
	if scratch == nil {
		return 0, fmt.Errorf("collections: column_graph quantized scalar_u8 scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, 1)
	rowIDs := scratch.scoreTileRowIDs[:1]
	rowIDs[0] = rowID
	scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, 1)
	dots, err := s.scoreRawDotRowIDsPrepared(rowIDs, scratch.scoreTileQuantizedDots, scratch, stats)
	if err != nil {
		return 0, err
	}
	return dots[0], nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRawDotRowIDsPrevalidated(rowIDs []uint32, dst []int64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]int64, error) {
	if cap(dst) < len(rowIDs) {
		if scratch != nil {
			scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, len(rowIDs))
			dst = scratch.scoreTileQuantizedDots[:len(rowIDs)]
		} else {
			dst = make([]int64, len(rowIDs))
		}
	} else {
		dst = dst[:len(rowIDs)]
	}
	if len(rowIDs) == 0 {
		return dst, nil
	}
	if err := s.validatePrepared(); err != nil {
		return dst[:0], err
	}
	return s.scoreRawDotRowIDsPrepared(rowIDs, dst, scratch, stats)
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreRawDotRowIDsPrepared(rowIDs []uint32, dst []int64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]int64, error) {
	if scratch == nil {
		return dst[:0], fmt.Errorf("collections: column_graph quantized scalar_u8 scorer: %w", errColumnVectorGraphNativeSearchScratchRequired)
	}
	if cap(dst) < len(rowIDs) {
		scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, len(rowIDs))
		dst = scratch.scoreTileQuantizedDots[:len(rowIDs)]
	} else {
		dst = dst[:len(rowIDs)]
	}
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	var status vectorops.ScalarU8DotBatchStatus
	if len(s.codeSums) == s.codeRows.Rows() {
		status = vectorops.DotScalarU8CenteredIndexedPrevalidatedWithByteSums(dst, s.codePayload, s.codeSums, s.centeredQuery, rowIDs, s.dims)
	} else {
		status = vectorops.DotScalarU8CenteredIndexedPrevalidated(dst, s.codePayload, s.centeredQuery, rowIDs, s.dims)
	}
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
	if status.Invalid || status.Rows != len(rowIDs) {
		return dst[:0], fmt.Errorf("%w: column_graph quantized index %q scalar_u8 batch score invalid status=%+v rows=%d want %d", ErrVectorIndexSearchUnavailable, s.indexName, status, status.Rows, len(rowIDs))
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, status.Rows, status.Optimized, status.Fallback)
	}
	s.recordScoreStats(stats, status.Rows)
	return dst, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreGreedyBestRowIDsPrevalidated(rowIDs []uint32, best int, bestScore float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, float64, bool, error) {
	if len(rowIDs) == 0 {
		return best, bestScore, false, nil
	}
	var dotScratch []int64
	if scratch != nil {
		dotScratch = scratch.scoreTileQuantizedDots
	}
	dots, err := s.scoreRawDotRowIDsPrevalidated(rowIDs, dotScratch, scratch, stats)
	if err != nil {
		return best, bestScore, false, err
	}
	changed := false
	if s.alphaLookup == nil {
		for i, rowID := range rowIDs {
			neighborOrdinal := int(rowID)
			score := scalarU8QuantizedCosineScoreFromDot(dots[i])
			// Preserve the prepared traversal's public float64 score formula and
			// exact lower-ordinal tie behavior while avoiding the intermediate
			// float64 score tile used by the generic row-ID score seam.
			if score > bestScore || (score == bestScore && neighborOrdinal < best) {
				best = neighborOrdinal
				bestScore = score
				changed = true
			}
		}
		return best, bestScore, changed, nil
	}
	for i, rowID := range rowIDs {
		neighborOrdinal := int(rowID)
		score, ok := s.scoreAlphaFromDotRowID(dots[i], rowID)
		if !ok {
			return best, bestScore, false, fmt.Errorf("%w: column_graph quantized index %q scalar_u8 alpha row_id=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, rowID)
		}
		if score > bestScore || (score == bestScore && neighborOrdinal < best) {
			best = neighborOrdinal
			bestScore = score
			changed = true
		}
	}
	return best, bestScore, changed, nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if len(rowIDs) == 0 {
		return 0, nil
	}
	var dotScratch []int64
	if scratch != nil {
		dotScratch = scratch.scoreTileQuantizedDots
	}
	dots, err := s.scoreRawDotRowIDsPrevalidated(rowIDs, dotScratch, scratch, stats)
	if err != nil {
		return 0, err
	}
	if s.alphaLookup == nil {
		for i, rowID := range rowIDs {
			candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: scalarU8QuantizedCosineScoreFromDot(dots[i])}
			if scratch.insertTop(topK, candidate) {
				scratch.pushFrontierAccounting(candidate, stats)
			}
		}
		return len(rowIDs), nil
	}
	for i, rowID := range rowIDs {
		score, ok := s.scoreAlphaFromDotRowID(dots[i], rowID)
		if !ok {
			return 0, fmt.Errorf("%w: column_graph quantized index %q scalar_u8 alpha row_id=%d unavailable", ErrVectorIndexSearchUnavailable, s.indexName, rowID)
		}
		candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: score}
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontierAccounting(candidate, stats)
		}
	}
	return len(rowIDs), nil
}

func (s *columnVectorGraphScalarU8QuantizedScorer) scoreAndPushRawDotFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if len(rowIDs) == 0 {
		return 0, nil
	}
	var dotScratch []int64
	if scratch != nil {
		dotScratch = scratch.scoreTileQuantizedDots
	}
	dots, err := s.scoreRawDotRowIDsPrevalidated(rowIDs, dotScratch, scratch, stats)
	if err != nil {
		return 0, err
	}
	for i, rowID := range rowIDs {
		candidate := columnVectorGraphRawDotSearchCandidate{ordinal: int(rowID), dot: dots[i]}
		if scratch.insertRawDotTop(topK, candidate) {
			scratch.pushRawDotFrontierAccounting(candidate, stats)
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
	if s.alphaLookup != nil {
		stats.QuantizedScoreCodecScalarU8Alpha = 1
	}
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
	if !quantizedVectorIndexDefinitionValuesEqual(status.Definition, qdef) {
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
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	score, err := s.scoreOrdinalUnchecked(ordinal)
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
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
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	for i, ordinal := range ordinals {
		score, err := s.scoreOrdinalUnchecked(ordinal)
		if err != nil {
			columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
			return dst[:i], err
		}
		dst[i] = score
	}
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
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
	if !quantizedVectorIndexDefinitionValuesEqual(status.Definition, qdef) {
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
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	score, err := s.scoreOrdinalUnchecked(ordinal)
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
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
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	for i, ordinal := range ordinals {
		score, err := s.scoreOrdinalUnchecked(ordinal)
		if err != nil {
			columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
			return dst[:i], err
		}
		dst[i] = score
	}
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
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
