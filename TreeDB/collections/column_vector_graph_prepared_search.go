package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

// columnVectorGraphPreparedSearchView is the #2045 combined prepared view for
// healthy current-format column_graph search. It is assembled once at reader
// open from already-admitted #2038/#2040/#2041 state and is then consumed by the
// HNSW search loop without per-candidate source/fallback selection.
type columnVectorGraphPreparedSearchView struct {
	rows int
	dims int

	vector                columnVectorGraphPreparedVectorView
	norm                  columnVectorGraphPreparedNormView
	adjacency             *columnVectorGraphAdjacencyDirectSources
	rowRefs               *columnVectorGraphRowRefStateSource
	documentIDs           *columnVectorGraphDocumentIDStateSource
	documentIDsMmapDirect bool
	vectorIdentityMapping bool
}

// maybePrepareColumnVectorGraphPreparedSearchView admits the #2045 combined
// prepared route only when every required current-format state is already
// mmap_direct. Non-mmap resource/platform fallbacks keep reader.preparedSearch
// nil so the existing counted source/compatibility path remains available;
// nil, stale, or malformed prerequisites intentionally fall through to
// prepareColumnVectorGraphPreparedSearchView so current-format corruption still
// fails closed.
func maybePrepareColumnVectorGraphPreparedSearchView(reader *columnVectorGraphPhysicalRowReader) error {
	if !columnVectorGraphPreparedSearchMmapPrerequisitesPresent(reader) {
		if reader != nil {
			reader.preparedSearch = nil
		}
		return nil
	}
	preparedSearch, err := prepareColumnVectorGraphPreparedSearchView(reader)
	if err != nil {
		return err
	}
	reader.preparedSearch = preparedSearch
	return nil
}

func columnVectorGraphPreparedSearchMmapPrerequisitesPresent(reader *columnVectorGraphPhysicalRowReader) bool {
	if reader == nil || columnVectorGraphManifestHasPhysicalAsset(reader.graph) || reader.RowCount() <= 0 {
		return true
	}
	if !columnVectorGraphPreparedSearchVectorMmapPrerequisitePresent(reader.typedVectorSource) {
		return false
	}
	if !columnVectorGraphPreparedSearchNormMmapPrerequisitePresent(reader.invNormSource) {
		return false
	}
	if !columnVectorGraphPreparedSearchAdjacencyMmapPrerequisitePresent(reader.adjacencyLayerSources) {
		return false
	}
	if reader.rowRefSource != nil && reader.rowRefSource.preparedViewActive() && reader.rowRefSource.baseMmapDirectFieldCount() != 4 {
		return false
	}
	if reader.documentIDSource != nil && reader.documentIDSource.preparedViewActive() && !columnVectorGraphDocumentIDSourceMmapDirect(reader.documentIDSource) {
		return false
	}
	return true
}

func columnVectorGraphPreparedSearchVectorMmapPrerequisitePresent(source *columnVectorGraphTypedColumnVectorSource) bool {
	if source == nil || source.closed {
		return true
	}
	for _, part := range source.parts {
		if part == nil {
			return true
		}
		switch part.outcome {
		case columnVectorGraphTypedColumnVectorOutcomeMmapDirect:
		case columnVectorGraphTypedColumnVectorOutcomeHeapCopyTypedView, columnVectorGraphTypedColumnVectorOutcomeScratchDecode:
			return false
		default:
			return true
		}
		if part.handle == nil || part.handle.Released() {
			return true
		}
	}
	return true
}

func columnVectorGraphPreparedSearchNormMmapPrerequisitePresent(source *columnVectorGraphInvNormStateSource) bool {
	if source == nil || source.closed || (source.handle != nil && source.handle.Released()) {
		return true
	}
	switch source.outcome {
	case columnVectorGraphInvNormStateOutcomeMmapDirect:
		return true
	case columnVectorGraphInvNormStateOutcomeHeapCopyTypedView, columnVectorGraphInvNormStateOutcomeScratchDecode:
		return false
	default:
		return true
	}
}

func columnVectorGraphPreparedSearchAdjacencyMmapPrerequisitePresent(group *columnVectorGraphAdjacencyDirectSources) bool {
	if group == nil || group.closed {
		return true
	}
	for _, source := range group.sources {
		if source == nil || source.closed {
			return true
		}
		switch source.outcome {
		case columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect:
		case columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView, columnVectorGraphLayer0AdjacencySourceOutcomeScratchDecode, columnVectorGraphLayer0AdjacencySourceOutcomeTypedListMmapDirect, columnVectorGraphLayer0AdjacencySourceOutcomeTypedListHeapCopyTypedView, columnVectorGraphLayer0AdjacencySourceOutcomeTypedListScratchDecode:
			return false
		default:
			return true
		}
		if source.offsetsHandle == nil || source.valuesHandle == nil || source.offsetsHandle.Released() || source.valuesHandle.Released() {
			return true
		}
	}
	return true
}

func prepareColumnVectorGraphPreparedSearchView(reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphPreparedSearchView, error) {
	if reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	if columnVectorGraphManifestHasPhysicalAsset(reader.graph) {
		return nil, errors.New("legacy physical graph-row asset is compatibility-only")
	}
	rows := reader.RowCount()
	if rows != reader.graph.RowCount {
		return nil, fmt.Errorf("row_count=%d want graph rows=%d", rows, reader.graph.RowCount)
	}
	if rows <= 0 {
		return &columnVectorGraphPreparedSearchView{rows: rows, dims: reader.def.Dimensions}, nil
	}
	vector, reason, description, ok := prepareColumnVectorGraphPreparedVectorView(reader.typedVectorSource, rows, reader.def.Dimensions)
	if !ok {
		if description != "" {
			return nil, fmt.Errorf("base vector prepared view unavailable reason=%s: %s", reason, description)
		}
		return nil, fmt.Errorf("base vector prepared view unavailable reason=%s", reason)
	}
	norm, normReason, ok := prepareColumnVectorGraphPreparedNormView(reader.invNormSource, rows)
	if !ok {
		return nil, fmt.Errorf("inverse-norm prepared view unavailable reason=%s", normReason)
	}
	view := &columnVectorGraphPreparedSearchView{
		rows:                  rows,
		dims:                  reader.def.Dimensions,
		vector:                vector,
		norm:                  norm,
		adjacency:             reader.adjacencyLayerSources,
		rowRefs:               reader.rowRefSource,
		documentIDs:           reader.documentIDSource,
		documentIDsMmapDirect: columnVectorGraphDocumentIDSourceMmapDirect(reader.documentIDSource),
		vectorIdentityMapping: vector.identityMapping(),
	}
	if err := view.validateLive(); err != nil {
		return nil, err
	}
	return view, nil
}

func (v *columnVectorGraphPreparedSearchView) ready() bool {
	return v != nil && v.rows >= 0 && v.dims > 0 && v.vector.ready() && v.norm.ready() && v.adjacency != nil && v.rowRefs != nil && v.documentIDs != nil
}

func (v *columnVectorGraphPreparedSearchView) indexedScoringDefaultEligible() bool {
	return v != nil && v.ready() && v.vector.singlePart != nil
}

func (v *columnVectorGraphPreparedSearchView) indexedScoreBatchOptimizedEligible(count int) bool {
	return v != nil && v.ready() && v.vector.singlePart != nil && vectorops.DotFloat32IndexedOptimizedEligible(count, v.dims)
}

func (v *columnVectorGraphPreparedSearchView) recordIndexedScoreBatchMinimalCounters(counters *columnVectorGraphPreparedMinimalSearchCounters, ordinals []int) {
	if counters == nil || len(ordinals) == 0 {
		return
	}
	if v == nil || !v.ready() || len(ordinals) <= 1 {
		counters.recordPreparedScores(len(ordinals), false, true)
		return
	}
	if v.vector.singlePart != nil {
		optimized := v.indexedScoreBatchOptimizedEligible(len(ordinals))
		counters.recordPreparedScores(len(ordinals), optimized, !optimized)
		return
	}
	for _, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= len(v.norm.values) {
			counters.recordPreparedScores(len(ordinals), false, true)
			return
		}
		if _, _, ok := v.vector.locationForOrdinal(ordinal); !ok {
			counters.recordPreparedScores(len(ordinals), false, true)
			return
		}
	}
	for runStart := 0; runStart < len(ordinals); {
		part, _, _ := v.vector.locationForOrdinal(ordinals[runStart])
		runEnd := runStart + 1
		for runEnd < len(ordinals) {
			nextPart, _, _ := v.vector.locationForOrdinal(ordinals[runEnd])
			if nextPart != part {
				break
			}
			runEnd++
		}
		runLen := runEnd - runStart
		optimized := vectorops.DotFloat32IndexedOptimizedEligible(runLen, v.dims)
		counters.recordPreparedScores(runLen, optimized, !optimized)
		runStart = runEnd
	}
}

func (v *columnVectorGraphPreparedSearchView) validateLive() error {
	if v == nil {
		return errors.New("nil prepared graph-search view")
	}
	if v.rows < 0 || v.dims <= 0 {
		return fmt.Errorf("invalid rows/dims=(%d,%d)", v.rows, v.dims)
	}
	if v.rows == 0 {
		return nil
	}
	if !v.vector.ready() {
		return errors.New("base vector prepared view is not ready")
	}
	if !v.norm.ready() {
		return errors.New("inverse-norm prepared view is not ready")
	}
	if err := v.validateVectorLive(); err != nil {
		return err
	}
	if err := v.validateNormLive(); err != nil {
		return err
	}
	if err := validateColumnVectorGraphPreparedSearchAdjacency(v.adjacency, v.rows); err != nil {
		return err
	}
	if v.rowRefs == nil || !v.rowRefs.preparedViewActive() {
		return errors.New("row-ref prepared view is not active")
	}
	if got, want := v.rowRefs.baseMmapDirectFieldCount(), uint64(4); got != want {
		return fmt.Errorf("row-ref prepared mmap fields=%d want %d", got, want)
	}
	if v.documentIDs == nil || !v.documentIDs.preparedViewActive() {
		return errors.New("document-id prepared bytes view is not active")
	}
	if !v.documentIDsMmapDirect {
		return errors.New("document-id prepared bytes view is not mmap_direct")
	}
	return nil
}

func (v *columnVectorGraphPreparedSearchView) validateVectorLive() error {
	if v == nil || !v.vector.ready() {
		return errors.New("base vector prepared view is not ready")
	}
	if v.vector.rows != v.rows || v.vector.dims != v.dims {
		return fmt.Errorf("base vector prepared rows/dims=(%d,%d) want (%d,%d)", v.vector.rows, v.vector.dims, v.rows, v.dims)
	}
	if v.vector.singlePart != nil {
		part := v.vector.singlePart
		if part.handle == nil || part.handle.Released() {
			return fmt.Errorf("base vector prepared single-part handle is stale: %w", errColumnVectorGraphManifestMismatch)
		}
		if part.outcome != columnVectorGraphTypedColumnVectorOutcomeMmapDirect {
			return fmt.Errorf("base vector prepared single-part outcome=%s want mmap_direct", part.outcome)
		}
		if part.rows < 0 || part.rows > maxCollectionInt/v.dims {
			return fmt.Errorf("base vector prepared single-part rows=%d dims=%d overflows rows*dims", part.rows, v.dims)
		}
		wantValues := part.rows * v.dims
		if len(v.vector.values) != wantValues {
			return fmt.Errorf("base vector prepared single-part values=%d want rows*dims=%d", len(v.vector.values), wantValues)
		}
		if len(part.values) != wantValues {
			return fmt.Errorf("base vector prepared single-part part values=%d want rows*dims=%d", len(part.values), wantValues)
		}
		rowIndexByOrdinal := v.vector.rowIndexByOrdinal
		if rowIndexByOrdinal == nil {
			if v.rows > part.rows {
				return fmt.Errorf("base vector prepared single-part identity rows=%d exceeds part rows=%d", v.rows, part.rows)
			}
			return nil
		}
		if len(rowIndexByOrdinal) != v.rows {
			return fmt.Errorf("base vector prepared single-part row map rows=%d want %d", len(rowIndexByOrdinal), v.rows)
		}
		return nil
	}
	if len(v.vector.parts) == 0 || len(v.vector.partIndexByOrdinal) != v.rows || len(v.vector.rowIndexByOrdinal) != v.rows {
		return errors.New("base vector prepared multipart mapping is incomplete")
	}
	for i, part := range v.vector.parts {
		if part == nil || part.handle == nil || part.handle.Released() {
			return fmt.Errorf("base vector prepared part[%d] handle is stale", i)
		}
		if part.outcome != columnVectorGraphTypedColumnVectorOutcomeMmapDirect {
			return fmt.Errorf("base vector prepared part[%d] outcome=%s want mmap_direct", i, part.outcome)
		}
		if len(part.values) != part.rows*v.dims {
			return fmt.Errorf("base vector prepared part[%d] values=%d want rows*dims=%d", i, len(part.values), part.rows*v.dims)
		}
	}
	return nil
}

func (v *columnVectorGraphPreparedSearchView) validateNormLive() error {
	if v == nil || !v.norm.ready() {
		return errors.New("inverse-norm prepared view is not ready")
	}
	if v.norm.rows != v.rows || len(v.norm.values) != v.rows {
		return fmt.Errorf("inverse-norm prepared rows/values=(%d,%d) want rows=%d", v.norm.rows, len(v.norm.values), v.rows)
	}
	if v.norm.source == nil || v.norm.source.closed || (v.norm.source.handle != nil && v.norm.source.handle.Released()) {
		return errors.New("inverse-norm prepared handle is stale")
	}
	if v.norm.source.outcome != columnVectorGraphInvNormStateOutcomeMmapDirect {
		return fmt.Errorf("inverse-norm prepared outcome=%s want mmap_direct", v.norm.source.outcome)
	}
	return nil
}

func validateColumnVectorGraphPreparedSearchAdjacency(group *columnVectorGraphAdjacencyDirectSources, rows int) error {
	if group == nil || group.closed || !group.allLayers || len(group.sources) == 0 {
		return errors.New("adjacency prepared CSR view is not active for all layers")
	}
	for layer, source := range group.sources {
		if source == nil || source.closed {
			return fmt.Errorf("adjacency prepared CSR layer %d is not active", layer)
		}
		if source.rows != rows || len(source.offsets) != rows+1 {
			return fmt.Errorf("adjacency prepared CSR layer %d rows/offsets=(%d,%d) want (%d,%d)", layer, source.rows, len(source.offsets), rows, rows+1)
		}
		if source.outcome != columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect {
			return fmt.Errorf("adjacency prepared CSR layer %d outcome=%s want prepared_csr_mmap_direct", layer, source.outcome)
		}
		if source.offsetsHandle == nil || source.valuesHandle == nil || source.offsetsHandle.Released() || source.valuesHandle.Released() {
			return fmt.Errorf("adjacency prepared CSR layer %d handle is stale", layer)
		}
	}
	return nil
}

func columnVectorGraphDocumentIDSourceMmapDirect(source *columnVectorGraphDocumentIDStateSource) bool {
	if source == nil || source.manager == nil || !source.preparedViewActive() {
		return false
	}
	stats := source.manager.Stats()
	return stats.ActiveHandles >= 2 && stats.ActiveMappedBytes > 0 && stats.ActiveHeapCopyBytes == 0
}

func (v *columnVectorGraphPreparedSearchView) scoreOrdinal(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinal int, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if v == nil || !v.ready() {
		return 0, errors.New("collections: column_graph prepared graph-search view is unavailable")
	}
	if len(query) != v.dims {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", ordinal, v.dims, len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	if ordinal < 0 || ordinal >= v.rows || ordinal >= len(v.norm.values) {
		return 0, fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonRowCountMismatch)
	}
	if v.norm.source == nil || v.norm.source.closed || (v.norm.source.handle != nil && v.norm.source.handle.Released()) {
		return 0, fmt.Errorf("collections: column_graph prepared inverse-norm ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
	}
	vector, ok := v.vectorForOrdinalFast(ordinal)
	if !ok {
		return 0, fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
	}
	score, err := columnVectorGraphPreparedCosineScore(query, queryInvNorm, ordinal, vector, v.norm.values[ordinal], stats)
	if err != nil {
		return 0, err
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		v.recordScoreStats(stats, plan, 1)
		v.recordMappingStats(stats, ordinal)
	}
	return score, nil
}

func (v *columnVectorGraphPreparedSearchView) checkScalarScoreInputs(query []float32, ordinal int) error {
	if v == nil || !v.ready() {
		return errors.New("collections: column_graph prepared graph-search view is unavailable")
	}
	if len(query) != v.dims {
		return fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", ordinal, v.dims, len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	if v.norm.source == nil || v.norm.source.closed || (v.norm.source.handle != nil && v.norm.source.handle.Released()) {
		return fmt.Errorf("collections: column_graph prepared inverse-norm ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
	}
	return nil
}

func columnVectorGraphPreparedCosineScore(query []float32, queryInvNorm float32, ordinal int, vector []float32, invNorm float32, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	dot := float64(vectorDotProductFloat32(query, vector))
	if math.IsInf(dot, 0) || math.IsNaN(dot) {
		if stats != nil {
			stats.ScoreFloat64Fallbacks++
		}
		dot = columnVectorGraphNativeDotProductFloat64(query, vector)
	}
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
	score := dot * float64(queryInvNorm) * float64(invNorm)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d cosine score is not finite", ordinal)
	}
	return score, nil
}

func (v *columnVectorGraphPreparedSearchView) vectorForOrdinalFast(ordinal int) ([]float32, bool) {
	vector := &v.vector
	dims := vector.dims
	if vector.singlePart != nil {
		part := vector.singlePart
		if part.handle == nil || part.handle.Released() {
			return nil, false
		}
		row := ordinal
		if vector.rowIndexByOrdinal != nil {
			row = int(vector.rowIndexByOrdinal[ordinal])
		}
		start := row * dims
		end := start + dims
		if row < 0 || row >= part.rows || start < 0 || end < start || end > len(part.values) {
			return nil, false
		}
		return part.values[start:end], true
	}
	if ordinal >= len(vector.partIndexByOrdinal) || ordinal >= len(vector.rowIndexByOrdinal) {
		return nil, false
	}
	partIndex := int(vector.partIndexByOrdinal[ordinal])
	if partIndex < 0 || partIndex >= len(vector.parts) {
		return nil, false
	}
	part := vector.parts[partIndex]
	if part == nil || part.handle == nil || part.handle.Released() {
		return nil, false
	}
	row := int(vector.rowIndexByOrdinal[ordinal])
	start := row * dims
	end := start + dims
	if row < 0 || row >= part.rows || start < 0 || end < start || end > len(part.values) {
		return nil, false
	}
	return part.values[start:end], true
}

func (v *columnVectorGraphPreparedSearchView) scoreOrdinals(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if len(ordinals) <= 1 || plan == nil || !plan.scoreBatchMode.indexedEnabled() || scratch == nil {
		return v.scoreOrdinalsScalar(plan, query, queryInvNorm, ordinals, dst, stats)
	}
	if got, ok, err := v.scoreOrdinalsIndexed(plan, query, queryInvNorm, ordinals, dst, scratch, stats); ok || err != nil {
		return got, err
	}
	return v.scoreOrdinalsScalar(plan, query, queryInvNorm, ordinals, dst, stats)
}

func (v *columnVectorGraphPreparedSearchView) scoreOrdinalsScalar(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinals []int, dst []float64, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	if len(ordinals) == 0 {
		return dst, nil
	}
	if err := v.checkScalarScoreInputs(query, ordinals[0]); err != nil {
		return dst[:0], err
	}
	vectorView := &v.vector
	dims := v.dims
	normValues := v.norm.values
	if part := vectorView.singlePart; part != nil {
		if part.handle == nil || part.handle.Released() {
			return dst[:0], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinals[0], typeddecode.ReasonStaleHandle)
		}
		rowIndexByOrdinal := vectorView.rowIndexByOrdinal
		for i, ordinal := range ordinals {
			if ordinal < 0 || ordinal >= v.rows || ordinal >= len(normValues) {
				return dst[:i], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonRowCountMismatch)
			}
			row := ordinal
			if rowIndexByOrdinal != nil {
				row = int(rowIndexByOrdinal[ordinal])
			}
			start := row * dims
			end := start + dims
			if row < 0 || row >= part.rows || start < 0 || end < start || end > len(part.values) {
				return dst[:i], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
			}
			score, err := columnVectorGraphPreparedCosineScore(query, queryInvNorm, ordinal, part.values[start:end], normValues[ordinal], stats)
			if err != nil {
				return dst[:i], err
			}
			dst[i] = score
		}
	} else {
		parts := vectorView.parts
		partIndexByOrdinal := vectorView.partIndexByOrdinal
		rowIndexByOrdinal := vectorView.rowIndexByOrdinal
		for i, ordinal := range ordinals {
			if ordinal < 0 || ordinal >= v.rows || ordinal >= len(normValues) || ordinal >= len(partIndexByOrdinal) || ordinal >= len(rowIndexByOrdinal) {
				return dst[:i], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonRowCountMismatch)
			}
			partIndex := int(partIndexByOrdinal[ordinal])
			if partIndex < 0 || partIndex >= len(parts) {
				return dst[:i], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
			}
			part := parts[partIndex]
			if part == nil || part.handle == nil || part.handle.Released() {
				return dst[:i], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
			}
			row := int(rowIndexByOrdinal[ordinal])
			start := row * dims
			end := start + dims
			if row < 0 || row >= part.rows || start < 0 || end < start || end > len(part.values) {
				return dst[:i], fmt.Errorf("collections: column_graph prepared vector ordinal=%d unavailable reason=%s", ordinal, typeddecode.ReasonStaleHandle)
			}
			score, err := columnVectorGraphPreparedCosineScore(query, queryInvNorm, ordinal, part.values[start:end], normValues[ordinal], stats)
			if err != nil {
				return dst[:i], err
			}
			dst[i] = score
		}
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, len(ordinals), false, true)
		v.recordScoreStats(stats, plan, len(ordinals))
		v.recordMappingStatsCount(stats, len(ordinals))
	}
	return dst, nil
}

func (v *columnVectorGraphPreparedSearchView) scoreOrdinalsIndexed(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, bool, error) {
	if v == nil || !v.ready() || len(query) != v.dims || len(ordinals) == 0 || scratch == nil {
		return dst, false, nil
	}
	if got, ok, err := v.scoreOrdinalsIndexedSinglePart(plan, query, queryInvNorm, ordinals, dst, scratch, stats); ok || err != nil {
		return got, ok, err
	}
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	maxRun := 0
	for i := 0; i < len(ordinals); {
		part, row, ok := v.vector.locationForOrdinal(ordinals[i])
		if !ok || ordinals[i] < 0 || ordinals[i] >= len(v.norm.values) || uint64(row) > uint64(^uint32(0)) {
			return dst, false, nil
		}
		j := i + 1
		for j < len(ordinals) {
			nextPart, nextRow, ok := v.vector.locationForOrdinal(ordinals[j])
			if !ok || nextPart != part || ordinals[j] < 0 || ordinals[j] >= len(v.norm.values) || uint64(nextRow) > uint64(^uint32(0)) {
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
		part, _, _ := v.vector.locationForOrdinal(ordinals[runStart])
		runEnd := runStart + 1
		for runEnd < len(ordinals) {
			nextPart, _, _ := v.vector.locationForOrdinal(ordinals[runEnd])
			if nextPart != part {
				break
			}
			runEnd++
		}
		runLen := runEnd - runStart
		rowIDs := scratch.scoreTileRowIDs[:runLen]
		for i, ordinal := range ordinals[runStart:runEnd] {
			_, row, _ := v.vector.locationForOrdinal(ordinal)
			rowIDs[i] = uint32(row)
		}
		dots := scratch.scoreTileDots[:runLen]
		scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
		status := vectorops.DotFloat32Indexed(dots, part.values, query, rowIDs, v.dims)
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
			_, row, _ := v.vector.locationForOrdinal(ordinal)
			vector := part.values[row*v.dims : row*v.dims+v.dims]
			score, err := columnVectorGraphNativeCosineScoreDot(query, queryInvNorm, ordinal, float64(dots[i]), vector, v.norm.values[ordinal])
			if err != nil {
				return dst, true, err
			}
			dst[runStart+i] = score
		}
		runStart = runEnd
	}
	if stats != nil {
		for runStart := 0; runStart < len(ordinals); {
			part, _, _ := v.vector.locationForOrdinal(ordinals[runStart])
			runEnd := runStart + 1
			for runEnd < len(ordinals) {
				nextPart, _, _ := v.vector.locationForOrdinal(ordinals[runEnd])
				if nextPart != part {
					break
				}
				runEnd++
			}
			recordColumnVectorGraphScoreBatchStats(stats, runEnd-runStart, false, false)
			runStart = runEnd
		}
		stats.ScoreBatchOptimizedCalls += optimizedCalls
		stats.ScoreBatchScalarFallbackCalls += scalarFallbackCalls
		v.recordScoreStats(stats, plan, len(ordinals))
		v.recordMappingStatsCount(stats, len(ordinals))
	}
	return dst, true, nil
}

func (v *columnVectorGraphPreparedSearchView) scoreOrdinalsIndexedSinglePart(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, bool, error) {
	if v == nil || !v.ready() || v.vector.singlePart == nil || len(query) != v.dims || len(ordinals) == 0 || scratch == nil {
		return dst, false, nil
	}
	part := v.vector.singlePart
	if part == nil || part.handle == nil || part.handle.Released() || part.rows < 0 || v.dims <= 0 || part.rows > maxCollectionInt/v.dims || len(part.values) != part.rows*v.dims {
		return dst, false, nil
	}
	if len(v.norm.values) != v.rows {
		return dst, false, nil
	}
	rowIndexByOrdinal := v.vector.rowIndexByOrdinal
	if rowIndexByOrdinal != nil && len(rowIndexByOrdinal) != v.rows {
		return dst, false, nil
	}
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	dims := v.dims
	values := part.values
	if !vectorops.DotFloat32BatchOptimizedAvailable() {
		if rowIndexByOrdinal == nil {
			for i, ordinal := range ordinals {
				if ordinal < 0 || ordinal >= v.rows || ordinal >= part.rows {
					return dst, false, nil
				}
				start := ordinal * dims
				vector := values[start : start+dims]
				scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
				dot := float64(vectorDotProductFloat32(query, vector))
				columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
				score, err := v.scorePreparedDot(query, queryInvNorm, ordinal, dot, vector, stats)
				if err != nil {
					return dst, true, err
				}
				dst[i] = score
			}
		} else {
			for i, ordinal := range ordinals {
				if ordinal < 0 || ordinal >= v.rows {
					return dst, false, nil
				}
				rowID := rowIndexByOrdinal[ordinal]
				if uint64(rowID) >= uint64(part.rows) {
					return dst, false, nil
				}
				row := int(rowID)
				start := row * dims
				vector := values[start : start+dims]
				scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
				dot := float64(vectorDotProductFloat32(query, vector))
				columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
				score, err := v.scorePreparedDot(query, queryInvNorm, ordinal, dot, vector, stats)
				if err != nil {
					return dst, true, err
				}
				dst[i] = score
			}
		}
		if stats != nil {
			recordColumnVectorGraphScoreBatchStats(stats, len(ordinals), false, true)
			v.recordScoreStats(stats, plan, len(ordinals))
			v.recordMappingStatsCount(stats, len(ordinals))
		}
		return dst, true, nil
	}

	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(ordinals))
	rowIDs := scratch.scoreTileRowIDs[:len(ordinals)]
	if rowIndexByOrdinal == nil {
		for i, ordinal := range ordinals {
			if ordinal < 0 || ordinal >= v.rows || ordinal >= part.rows || uint64(ordinal) > uint64(^uint32(0)) {
				return dst, false, nil
			}
			rowIDs[i] = uint32(ordinal)
		}
	} else {
		for i, ordinal := range ordinals {
			if ordinal < 0 || ordinal >= v.rows {
				return dst, false, nil
			}
			rowID := rowIndexByOrdinal[ordinal]
			if uint64(rowID) >= uint64(part.rows) {
				return dst, false, nil
			}
			rowIDs[i] = rowID
		}
	}
	scratch.scoreTileDots = ensureColumnVectorGraphNativeFloat32Scratch(scratch.scoreTileDots, len(ordinals))
	dots := scratch.scoreTileDots[:len(ordinals)]
	scoreStart := columnVectorGraphNativeSearchStartDistanceKernel(stats)
	status := vectorops.DotFloat32Indexed(dots, values, query, rowIDs, dims)
	columnVectorGraphNativeSearchFinishDistanceKernel(stats, scoreStart)
	if status.Invalid || status.Rows != len(ordinals) {
		return dst, false, nil
	}
	for i, ordinal := range ordinals {
		row := int(rowIDs[i])
		start := row * dims
		vector := values[start : start+dims]
		score, err := v.scorePreparedDot(query, queryInvNorm, ordinal, float64(dots[i]), vector, stats)
		if err != nil {
			return dst, true, err
		}
		dst[i] = score
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, len(ordinals), false, false)
		if status.Optimized {
			stats.ScoreBatchOptimizedCalls++
		} else {
			stats.ScoreBatchScalarFallbackCalls++
		}
		v.recordScoreStats(stats, plan, len(ordinals))
		v.recordMappingStatsCount(stats, len(ordinals))
	}
	return dst, true, nil
}

func (v *columnVectorGraphPreparedSearchView) scorePreparedDot(query []float32, queryInvNorm float32, ordinal int, dot float64, vector []float32, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if math.IsInf(dot, 0) || math.IsNaN(dot) {
		if stats != nil {
			stats.ScoreFloat64Fallbacks++
		}
		dot = columnVectorGraphNativeDotProductFloat64(query, vector)
	}
	score := dot * float64(queryInvNorm) * float64(v.norm.values[ordinal])
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d cosine score is not finite", ordinal)
	}
	return score, nil
}

func (v *columnVectorGraphPreparedSearchView) recordScoreStats(stats *columnVectorGraphNativeSearchStats, plan *columnVectorGraphSearchPlan, count int) {
	if stats == nil || count <= 0 {
		return
	}
	stats.PreparedScoreCalls += uint64(count)
	stats.FP32ScoreCalls += uint64(count)
	stats.VisitedNodes += uint64(count)
	stats.CandidateFetches += uint64(count)
	stats.VectorBytesRead += uint64(count * v.dims * 4)
	stats.NormBytesRead += uint64(count * 4)
	stats.VectorDirectViews += uint64(count)
	stats.VectorMmapDirectViews += uint64(count)
	stats.VectorPreparedDirectViews += uint64(count)
	stats.NormDirectViews += uint64(count)
	stats.NormMmapDirectViews += uint64(count)
	stats.NormPreparedDirectViews += uint64(count)
	if plan != nil {
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
	}
}

func (v *columnVectorGraphPreparedSearchView) recordMappingStats(stats *columnVectorGraphNativeSearchStats, ordinal int) {
	if stats == nil {
		return
	}
	if v.vectorIdentityMapping {
		stats.VectorPreparedIdentityMappings++
		return
	}
	stats.VectorPreparedRowRefMappings++
}

func (v *columnVectorGraphPreparedSearchView) recordMappingStatsCount(stats *columnVectorGraphNativeSearchStats, count int) {
	if stats == nil || count <= 0 {
		return
	}
	if v.vectorIdentityMapping {
		stats.VectorPreparedIdentityMappings += uint64(count)
		return
	}
	stats.VectorPreparedRowRefMappings += uint64(count)
}

func (v *columnVectorGraphPreparedSearchView) maxAdjacencyLayerForOrdinal(ordinal int) (int, columnVectorGraphAdjacencySourceCounterSnapshot, error) {
	if v == nil || v.adjacency == nil {
		return 0, columnVectorGraphAdjacencySourceCounterSnapshot{}, errors.New("collections: column_graph prepared adjacency view is unavailable")
	}
	layer, _, counters, reason, ok := v.adjacency.MaxLayerForOrdinal(ordinal)
	if !ok {
		return 0, counters, fmt.Errorf("collections: column_graph prepared adjacency max-layer ordinal=%d unavailable reason=%s", ordinal, reason)
	}
	return layer, counters, nil
}

func (v *columnVectorGraphPreparedSearchView) adjacencyLayerForOrdinal(ordinal int, layer int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, error) {
	if v == nil || v.adjacency == nil {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, errors.New("collections: column_graph prepared adjacency view is unavailable")
	}
	neighbors, reason, ok := v.adjacency.preparedCSRNeighbors(layer, ordinal)
	if !ok {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, fmt.Errorf("collections: column_graph prepared adjacency ordinal=%d layer=%d unavailable reason=%s", ordinal, layer, reason)
	}
	return neighbors, columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect, nil
}

func (v *columnVectorGraphPreparedSearchView) documentIDForOrdinal(ordinal int) ([]byte, bool) {
	if v == nil || v.documentIDs == nil {
		return nil, false
	}
	return v.documentIDs.documentIDForOrdinal(ordinal)
}

func (v *columnVectorGraphPreparedSearchView) rowRefForOrdinal(ordinal int) (DocumentRowRef, bool) {
	if v == nil || v.rowRefs == nil {
		return DocumentRowRef{}, false
	}
	return v.rowRefs.rowRefForOrdinal(ordinal)
}
