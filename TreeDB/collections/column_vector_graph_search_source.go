package collections

import (
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

type columnVectorGraphSearchVectorSourceKind uint8

const (
	columnVectorGraphSearchVectorSourceGraphRows columnVectorGraphSearchVectorSourceKind = iota
	columnVectorGraphSearchVectorSourceTypedColumn
)

func (k columnVectorGraphSearchVectorSourceKind) String() string {
	switch k {
	case columnVectorGraphSearchVectorSourceTypedColumn:
		return "typed_column"
	default:
		return "graph_rows"
	}
}

type columnVectorGraphSearchNormSourceKind uint8

const (
	columnVectorGraphSearchNormSourceGraphRows columnVectorGraphSearchNormSourceKind = iota
	columnVectorGraphSearchNormSourceInvNormByOrdinal
)

func (k columnVectorGraphSearchNormSourceKind) String() string {
	switch k {
	case columnVectorGraphSearchNormSourceInvNormByOrdinal:
		return "inv_norm_by_ordinal"
	default:
		return "graph_rows"
	}
}

type columnVectorGraphSearchSource struct {
	reader   *columnVectorGraphPhysicalRowReader
	dims     int
	rowCount int

	vectorKind                   columnVectorGraphSearchVectorSourceKind
	typedVectorSource            *columnVectorGraphTypedColumnVectorSource
	typedVectorLocations         []columnVectorGraphTypedColumnVectorLocation
	vectorFallbackReason         typeddecode.Reason
	vectorFallbackDescription    string
	preparedVector               columnVectorGraphPreparedVectorView
	preparedScoreReady           bool
	preparedScoreIdentityMapping bool

	normKind           columnVectorGraphSearchNormSourceKind
	invNormSource      *columnVectorGraphInvNormStateSource
	invNormByOrdinal   []float32
	invNormOutcome     columnVectorGraphInvNormStateOutcome
	invNormFallback    typeddecode.Reason
	preparedNorm       columnVectorGraphPreparedNormView
	normFallbackReason typeddecode.Reason
}

func (s *columnVectorGraphSearchSource) reset() {
	*s = columnVectorGraphSearchSource{}
}

// liveFor reports whether the source bindings prepared for plan can be reused
// for another search. The reader owns these immutable sources for its lifetime;
// an explicitly closed or replaced source must be prepared again so the normal
// source validation and fallback behavior remains in force.
func (s *columnVectorGraphSearchSource) liveFor(plan *columnVectorGraphSearchPlan) bool {
	if s == nil || plan == nil || plan.reader == nil || s.reader != plan.reader || s.dims != plan.reader.def.Dimensions || s.rowCount != plan.reader.RowCount() {
		return false
	}
	if s.vectorKind == columnVectorGraphSearchVectorSourceTypedColumn && (s.typedVectorSource == nil || s.typedVectorSource != plan.reader.typedVectorSource || s.typedVectorSource.closed) {
		return false
	}
	if s.normKind == columnVectorGraphSearchNormSourceInvNormByOrdinal && (s.invNormSource == nil || s.invNormSource != plan.reader.invNormSource || s.invNormSource.closed || (s.invNormSource.handle != nil && s.invNormSource.handle.Released())) {
		return false
	}
	return true
}

func (s *columnVectorGraphSearchSource) prepare(plan *columnVectorGraphSearchPlan) error {
	s.reset()
	if plan == nil || plan.reader == nil {
		return errNilColumnVectorGraphPhysicalRowReader
	}
	reader := plan.reader
	physicalReader := plan.physicalReader
	if physicalReader == nil && columnVectorGraphManifestHasPhysicalAsset(reader.graph) {
		var err error
		physicalReader, err = reader.rowReader()
		if err != nil {
			return err
		}
		plan.physicalReader = physicalReader
	}
	s.reader = reader
	s.dims = reader.def.Dimensions
	s.rowCount = reader.RowCount()
	s.vectorKind = columnVectorGraphSearchVectorSourceGraphRows
	s.normKind = columnVectorGraphSearchNormSourceGraphRows

	if reason, description, ok := columnVectorGraphTypedVectorSourceUsableForSearch(reader.typedVectorSource, s.rowCount, s.dims); ok {
		s.vectorKind = columnVectorGraphSearchVectorSourceTypedColumn
		s.typedVectorSource = reader.typedVectorSource
		s.typedVectorLocations = reader.typedVectorSource.locations
		if reader.typedVectorSource.prepared.ready() {
			s.preparedVector = reader.typedVectorSource.prepared
		}
	} else if reader.typedVectorSource != nil {
		s.vectorFallbackReason = reason
		s.vectorFallbackDescription = description
	}

	if values, outcome, fallbackReason, reason, ok := columnVectorGraphInvNormSourceUsableForSearch(reader.invNormSource, s.rowCount); ok {
		s.normKind = columnVectorGraphSearchNormSourceInvNormByOrdinal
		s.invNormSource = reader.invNormSource
		s.invNormByOrdinal = values
		s.invNormOutcome = outcome
		s.invNormFallback = fallbackReason
		if reader.invNormSource.prepared.ready() {
			s.preparedNorm = reader.invNormSource.prepared
		}
	} else if reader.invNormSource != nil {
		s.normFallbackReason = reason
	}
	if s.preparedVector.ready() && s.preparedNorm.ready() {
		s.preparedScoreReady = true
		s.preparedScoreIdentityMapping = s.preparedVector.identityMapping()
	}
	if physicalReader == nil && s.rowCount > 0 {
		if s.vectorKind != columnVectorGraphSearchVectorSourceTypedColumn {
			return fmt.Errorf("collections: column_graph %q search requires base typed-column vectors when graph row fallback is unavailable", reader.def.Name)
		}
		if s.normKind != columnVectorGraphSearchNormSourceInvNormByOrdinal {
			return fmt.Errorf("collections: column_graph %q search requires vector-index inverse-norm state when graph row fallback is unavailable", reader.def.Name)
		}
	}
	return nil
}

func columnVectorGraphTypedVectorSourceUsableForSearch(source *columnVectorGraphTypedColumnVectorSource, rowCount, dims int) (typeddecode.Reason, string, bool) {
	if source == nil {
		return "", "", false
	}
	if source.closed {
		return typeddecode.ReasonStaleHandle, "typed-column vector source is closed", false
	}
	if dims <= 0 || source.dims != dims {
		return typeddecode.ReasonDimensionMismatch, fmt.Sprintf("typed-column vector source dims=%d want %d", source.dims, dims), false
	}
	if len(source.locations) != rowCount {
		return typeddecode.ReasonRowCountMismatch, fmt.Sprintf("typed-column vector source locations=%d want rows=%d", len(source.locations), rowCount), false
	}
	for partIndex, part := range source.parts {
		if part == nil {
			return typeddecode.ReasonValidationFailed, fmt.Sprintf("typed-column vector source part[%d] is nil", partIndex), false
		}
		if part.rows < 0 {
			return typeddecode.ReasonRowCountMismatch, fmt.Sprintf("typed-column vector source part[%d] rows=%d", partIndex, part.rows), false
		}
		if part.rows > 0 && part.rows > maxCollectionInt/dims {
			return typeddecode.ReasonPayloadLengthMismatch, fmt.Sprintf("typed-column vector source part[%d] rows=%d dims=%d overflows rows*dims", partIndex, part.rows, dims), false
		}
		wantValues := part.rows * dims
		if wantValues < 0 || len(part.values) != wantValues {
			return typeddecode.ReasonPayloadLengthMismatch, fmt.Sprintf("typed-column vector source part[%d] values=%d want rows*dims=%d", partIndex, len(part.values), wantValues), false
		}
	}
	return "", "", true
}

func columnVectorGraphInvNormSourceUsableForSearch(source *columnVectorGraphInvNormStateSource, rowCount int) ([]float32, columnVectorGraphInvNormStateOutcome, typeddecode.Reason, typeddecode.Reason, bool) {
	if source == nil {
		return nil, columnVectorGraphInvNormStateOutcomeUnknown, "", "", false
	}
	if source.closed || (source.handle != nil && source.handle.Released()) {
		return nil, columnVectorGraphInvNormStateOutcomeUnknown, "", typeddecode.ReasonStaleHandle, false
	}
	if source.rows != rowCount || len(source.values) != rowCount {
		return nil, columnVectorGraphInvNormStateOutcomeUnknown, "", typeddecode.ReasonRowCountMismatch, false
	}
	return source.values, source.outcome, source.fallbackReason, "", true
}

func (s *columnVectorGraphSearchSource) scoreOrdinal(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if s == nil || s.reader == nil {
		if plan == nil || plan.reader == nil {
			return 0, errNilColumnVectorGraphPhysicalRowReader
		}
		return plan.reader.scoreOrdinalLegacy(plan, singleBlockView, query, queryInvNorm, ordinal, scratch, stats)
	}
	if s.preparedScoreReady {
		if score, handled, err := s.scorePreparedOrdinal(plan, query, queryInvNorm, ordinal, stats); handled || err != nil {
			return score, err
		}
	}
	var view *columnVectorGraphBlockView
	rowIndex := ordinal
	needsGraphRow := s.vectorKind == columnVectorGraphSearchVectorSourceGraphRows || s.normKind == columnVectorGraphSearchNormSourceGraphRows
	if needsGraphRow {
		view = singleBlockView
		if view == nil {
			refView, ref, err := plan.blockViewForOrdinal(ordinal)
			if err != nil {
				return 0, err
			}
			view = refView
			rowIndex = ref.rowIndex
		}
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		stats.VisitedNodes++
		stats.FP32ScoreCalls++
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
	}

	vector, err := s.vectorForOrdinal(view, rowIndex, ordinal, scratch, stats)
	if err != nil {
		return 0, err
	}
	invNorm, err := s.invNormForOrdinal(view, rowIndex, ordinal, stats)
	if err != nil {
		return 0, err
	}
	if stats != nil {
		stats.CandidateFetches++
		stats.VectorBytesRead += uint64(len(vector)) * 4
		stats.NormBytesRead += 4
	}
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	score, err := columnVectorGraphNativeCosineScoreVector(query, queryInvNorm, ordinal, vector, invNorm)
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is not finite", s.reader.def.Name, ordinal)
	}
	return score, nil
}

func (s *columnVectorGraphSearchSource) scoreOrdinalsScalar(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	for i, ordinal := range ordinals {
		score, err := s.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, ordinal, scratch, stats)
		if err != nil {
			return dst[:i], err
		}
		dst[i] = score
	}
	return dst, nil
}

func (s *columnVectorGraphSearchSource) scoreOrdinals(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if len(ordinals) <= 1 || plan == nil || !plan.scoreBatchMode.indexedEnabled() || scratch == nil {
		return s.scoreOrdinalsScalar(plan, singleBlockView, query, queryInvNorm, ordinals, dst, scratch, stats)
	}
	if got, ok, err := s.scoreOrdinalsIndexed(plan, query, queryInvNorm, ordinals, dst, scratch, stats); ok || err != nil {
		return got, err
	}
	return s.scoreOrdinalsScalar(plan, singleBlockView, query, queryInvNorm, ordinals, dst, scratch, stats)
}

func (s *columnVectorGraphSearchSource) scoreOrdinalsIndexed(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, bool, error) {
	if s == nil || s.reader == nil || s.vectorKind != columnVectorGraphSearchVectorSourceTypedColumn || s.normKind != columnVectorGraphSearchNormSourceInvNormByOrdinal || s.dims <= 0 || len(query) != s.dims || len(ordinals) == 0 || scratch == nil {
		return dst, false, nil
	}
	if s.invNormSource != nil && (s.invNormSource.closed || (s.invNormSource.handle != nil && s.invNormSource.handle.Released())) {
		return dst, false, nil
	}
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	maxRun := 0
	for i := 0; i < len(ordinals); {
		loc, ok := s.indexedVectorLocationForOrdinal(ordinals[i])
		if !ok || ordinals[i] < 0 || ordinals[i] >= len(s.invNormByOrdinal) || uint64(loc.rowIndex) > uint64(^uint32(0)) {
			return dst, false, nil
		}
		part := loc.part
		j := i + 1
		for j < len(ordinals) {
			nextLoc, ok := s.indexedVectorLocationForOrdinal(ordinals[j])
			if !ok || nextLoc.part != part || ordinals[j] < 0 || ordinals[j] >= len(s.invNormByOrdinal) || uint64(nextLoc.rowIndex) > uint64(^uint32(0)) {
				break
			}
			j++
		}
		if runLen := j - i; runLen > maxRun {
			maxRun = runLen
		}
		i = j
	}
	if maxRun == 0 {
		return dst, false, nil
	}
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, maxRun)
	scratch.scoreTileDots = ensureColumnVectorGraphNativeFloat32Scratch(scratch.scoreTileDots, maxRun)
	var optimizedCalls uint64
	var scalarFallbackCalls uint64
	for runStart := 0; runStart < len(ordinals); {
		loc, _ := s.indexedVectorLocationForOrdinal(ordinals[runStart])
		part := loc.part
		runEnd := runStart + 1
		for runEnd < len(ordinals) {
			nextLoc, _ := s.indexedVectorLocationForOrdinal(ordinals[runEnd])
			if nextLoc.part != part {
				break
			}
			runEnd++
		}
		runLen := runEnd - runStart
		rowIDs := scratch.scoreTileRowIDs[:runLen]
		for i, ordinal := range ordinals[runStart:runEnd] {
			loc := s.typedVectorLocations[ordinal]
			rowIDs[i] = uint32(loc.rowIndex)
		}
		dots := scratch.scoreTileDots[:runLen]
		scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
		status := vectorops.DotFloat32Indexed(dots, part.values, query, rowIDs, s.dims)
		columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
		if status.Invalid || status.Rows != runLen {
			return dst, false, nil
		}
		if status.Optimized {
			optimizedCalls++
		} else {
			scalarFallbackCalls++
		}
		for i, ordinal := range ordinals[runStart:runEnd] {
			loc := s.typedVectorLocations[ordinal]
			score, err := columnVectorGraphNativeCosineScoreDot(query, queryInvNorm, ordinal, float64(dots[i]), part.values[loc.rowIndex*s.dims:loc.rowIndex*s.dims+s.dims], s.invNormByOrdinal[ordinal])
			if err != nil {
				return dst, true, err
			}
			dst[runStart+i] = score
		}
		runStart = runEnd
	}
	if stats != nil {
		for runStart := 0; runStart < len(ordinals); {
			loc := s.typedVectorLocations[ordinals[runStart]]
			part := loc.part
			runEnd := runStart + 1
			for runEnd < len(ordinals) && s.typedVectorLocations[ordinals[runEnd]].part == part {
				runEnd++
			}
			recordColumnVectorGraphScoreBatchStats(stats, runEnd-runStart, false, false)
			runStart = runEnd
		}
		stats.ScoreBatchOptimizedCalls += optimizedCalls
		stats.ScoreBatchScalarFallbackCalls += scalarFallbackCalls
		stats.VisitedNodes += uint64(len(ordinals))
		stats.FP32ScoreCalls += uint64(len(ordinals))
		stats.CandidateFetches += uint64(len(ordinals))
		stats.VectorBytesRead += uint64(len(ordinals) * s.dims * 4)
		stats.NormBytesRead += uint64(len(ordinals) * 4)
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
		for _, ordinal := range ordinals {
			loc := s.typedVectorLocations[ordinal]
			recordColumnVectorGraphVectorSourceStats(stats, loc.part.outcome, loc.part.fallbackReason)
			recordColumnVectorGraphInvNormSourceStats(stats, s.invNormOutcome, s.invNormFallback)
		}
	}
	return dst, true, nil
}

func (s *columnVectorGraphSearchSource) indexedVectorLocationForOrdinal(ordinal int) (columnVectorGraphTypedColumnVectorLocation, bool) {
	if s == nil || ordinal < 0 || ordinal >= len(s.typedVectorLocations) || s.dims <= 0 {
		return columnVectorGraphTypedColumnVectorLocation{}, false
	}
	if s.typedVectorSource != nil && s.typedVectorSource.closed {
		return columnVectorGraphTypedColumnVectorLocation{}, false
	}
	loc := s.typedVectorLocations[ordinal]
	if loc.part == nil || loc.rowIndex < 0 || loc.rowIndex >= loc.part.rows {
		return columnVectorGraphTypedColumnVectorLocation{}, false
	}
	if loc.part.handle != nil && loc.part.handle.Released() {
		return columnVectorGraphTypedColumnVectorLocation{}, false
	}
	start := loc.rowIndex * s.dims
	end := start + s.dims
	if start < 0 || end < start || end > len(loc.part.values) {
		return columnVectorGraphTypedColumnVectorLocation{}, false
	}
	return loc, true
}

func (s *columnVectorGraphSearchSource) typedVectorForOrdinal(ordinal int) ([]float32, columnVectorGraphTypedColumnVectorOutcome, typeddecode.Reason, bool) {
	if s == nil || ordinal < 0 || ordinal >= len(s.typedVectorLocations) || s.dims <= 0 {
		return nil, columnVectorGraphTypedColumnVectorOutcomeUnknown, typeddecode.ReasonRowCountMismatch, false
	}
	if s.typedVectorSource != nil && s.typedVectorSource.closed {
		return nil, columnVectorGraphTypedColumnVectorOutcomeUnknown, typeddecode.ReasonStaleHandle, false
	}
	loc := s.typedVectorLocations[ordinal]
	if loc.part == nil || loc.rowIndex < 0 || loc.rowIndex >= loc.part.rows {
		return nil, columnVectorGraphTypedColumnVectorOutcomeUnknown, typeddecode.ReasonRowCountMismatch, false
	}
	if loc.part.handle != nil && loc.part.handle.Released() {
		return nil, columnVectorGraphTypedColumnVectorOutcomeUnknown, typeddecode.ReasonStaleHandle, false
	}
	start := loc.rowIndex * s.dims
	end := start + s.dims
	if start < 0 || end < start || end > len(loc.part.values) {
		return nil, columnVectorGraphTypedColumnVectorOutcomeUnknown, typeddecode.ReasonPayloadLengthMismatch, false
	}
	return loc.part.values[start:end], loc.part.outcome, loc.part.fallbackReason, true
}

func (s *columnVectorGraphSearchSource) vectorForOrdinal(view *columnVectorGraphBlockView, rowIndex int, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float32, error) {
	if s.vectorKind == columnVectorGraphSearchVectorSourceTypedColumn {
		vector, outcome, fallbackReason, ok := s.typedVectorForOrdinal(ordinal)
		if ok {
			recordColumnVectorGraphVectorSourceStats(stats, outcome, fallbackReason)
			return vector, nil
		}
		if fallbackReason == "" {
			fallbackReason = typeddecode.ReasonValidationFailed
		}
		recordColumnVectorGraphVectorFallbackReasonStats(stats, fallbackReason)
		if stats != nil {
			stats.TypedColumnFallbacks = 1
		}
	}
	if s.vectorFallbackReason != "" {
		recordColumnVectorGraphVectorFallbackReasonStats(stats, s.vectorFallbackReason)
		if stats != nil {
			stats.TypedColumnFallbacks = 1
		}
	}
	if view == nil {
		return nil, fmt.Errorf("collections: column_graph %q vector graph-row fallback unavailable for ordinal=%d", s.reader.def.Name, ordinal)
	}
	scratch.scoreScratch.Float32Values = scratch.scoreScratch.Float32Values[:0]
	var vectorScratch []float32
	vector, vectorScratch := view.vectorUnchecked(rowIndex, scratch.scoreScratch.Float32Values)
	scratch.scoreScratch.Float32Values = vectorScratch
	if stats != nil {
		stats.VectorScratchDecodes++
		stats.GraphRowFallbacks++
	}
	return vector, nil
}

func (s *columnVectorGraphSearchSource) invNormForOrdinal(view *columnVectorGraphBlockView, rowIndex int, ordinal int, stats *columnVectorGraphNativeSearchStats) (float32, error) {
	if s.normKind == columnVectorGraphSearchNormSourceInvNormByOrdinal {
		if s.invNormSource != nil && (s.invNormSource.closed || (s.invNormSource.handle != nil && s.invNormSource.handle.Released())) {
			s.normFallbackReason = typeddecode.ReasonStaleHandle
		} else if ordinal >= 0 && ordinal < len(s.invNormByOrdinal) {
			recordColumnVectorGraphInvNormSourceStats(stats, s.invNormOutcome, s.invNormFallback)
			return s.invNormByOrdinal[ordinal], nil
		} else {
			s.normFallbackReason = typeddecode.ReasonRowCountMismatch
		}
	}
	if s.normFallbackReason != "" {
		recordColumnVectorGraphInvNormFallbackReasonStats(stats, s.normFallbackReason)
		if stats != nil {
			stats.NormSourceFallbacks++
		}
	}
	if view == nil {
		return 0, fmt.Errorf("collections: column_graph %q inverse-norm graph-row fallback unavailable for ordinal=%d", s.reader.def.Name, ordinal)
	}
	if stats != nil {
		stats.GraphRowFallbacks++
	}
	return view.legacyInvNorm(rowIndex)
}

func (s *columnVectorGraphSearchSource) populateConstructionStats(stats *columnVectorGraphNativeSearchStats) {
	if s == nil || stats == nil {
		return
	}
	if s.vectorFallbackReason != "" && stats.TypedColumnFallbacks == 0 {
		stats.TypedColumnFallbacks = 1
	}
}

type columnVectorGraphAggregateSourceCounters struct {
	DirectViews             uint64
	MmapDirectViews         uint64
	HeapCopyTypedViews      uint64
	ScratchDecodes          uint64
	Fallbacks               uint64
	CertificationFailures   uint64
	ValidationFailures      uint64
	AbsoluteOffsetUnaligned uint64
	ActualPointerUnaligned  uint64
	StaleHandles            uint64
}

func (s columnVectorGraphNativeSearchStats) VectorSourceCounters() columnVectorGraphAggregateSourceCounters {
	return columnVectorGraphAggregateSourceCounters{
		DirectViews:             s.VectorDirectViews,
		MmapDirectViews:         s.VectorMmapDirectViews,
		HeapCopyTypedViews:      s.VectorHeapCopyTypedViews,
		ScratchDecodes:          s.VectorScratchDecodes,
		Fallbacks:               s.TypedColumnFallbacks,
		CertificationFailures:   s.VectorCertificationFailures,
		AbsoluteOffsetUnaligned: s.VectorAbsoluteOffsetUnaligned,
		ActualPointerUnaligned:  s.VectorActualPointerUnaligned,
		StaleHandles:            s.VectorStaleHandles,
	}
}

func (s columnVectorGraphNativeSearchStats) NormSourceCounters() columnVectorGraphAggregateSourceCounters {
	return columnVectorGraphAggregateSourceCounters{
		DirectViews:             s.NormDirectViews,
		MmapDirectViews:         s.NormMmapDirectViews,
		HeapCopyTypedViews:      s.NormHeapCopyTypedViews,
		ScratchDecodes:          s.NormScratchDecodes,
		Fallbacks:               s.NormSourceFallbacks,
		ValidationFailures:      s.NormValidationFailures,
		AbsoluteOffsetUnaligned: s.NormAbsoluteOffsetUnaligned,
		ActualPointerUnaligned:  s.NormActualPointerUnaligned,
		StaleHandles:            s.NormStaleHandles,
	}
}

func (s columnVectorGraphNativeSearchStats) AdjacencySourceCounters() columnVectorGraphAggregateSourceCounters {
	return columnVectorGraphAggregateSourceCounters{
		DirectViews:             s.AdjacencyDirectViews,
		MmapDirectViews:         s.AdjacencyMmapDirectViews,
		HeapCopyTypedViews:      s.AdjacencyHeapCopyTypedViews,
		ScratchDecodes:          s.AdjacencyScratchDecodes,
		Fallbacks:               s.AdjacencySourceFallbacks,
		CertificationFailures:   s.AdjacencyCertificationFailures,
		ValidationFailures:      s.AdjacencyValidationFailures,
		AbsoluteOffsetUnaligned: s.AdjacencyAbsoluteOffsetUnaligned,
		ActualPointerUnaligned:  s.AdjacencyActualPointerUnaligned,
		StaleHandles:            s.AdjacencyStaleHandles,
	}
}

func (s columnVectorGraphNativeSearchStats) AggregateSourceCounters() columnVectorGraphAggregateSourceCounters {
	out := s.VectorSourceCounters()
	out.add(s.NormSourceCounters())
	out.add(s.AdjacencySourceCounters())
	return out
}

func (c *columnVectorGraphAggregateSourceCounters) add(other columnVectorGraphAggregateSourceCounters) {
	if c == nil {
		return
	}
	c.DirectViews += other.DirectViews
	c.MmapDirectViews += other.MmapDirectViews
	c.HeapCopyTypedViews += other.HeapCopyTypedViews
	c.ScratchDecodes += other.ScratchDecodes
	c.Fallbacks += other.Fallbacks
	c.CertificationFailures += other.CertificationFailures
	c.ValidationFailures += other.ValidationFailures
	c.AbsoluteOffsetUnaligned += other.AbsoluteOffsetUnaligned
	c.ActualPointerUnaligned += other.ActualPointerUnaligned
	c.StaleHandles += other.StaleHandles
}
