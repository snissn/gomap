package collections

import (
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

const (
	columnVectorGraphPreparedOwnerVectorIndexState = "vector_index_state"
	columnVectorGraphPreparedVectorRoleBaseVectors = "base_vectors"
)

// columnVectorGraphPreparedVectorView is the #2040 search-native vector view
// over already-certified typed-column base vectors. The single-part identity
// shape keeps rowIndexByOrdinal nil so the scoring loop can use ordinal as the
// base row index; non-identity and multipart layouts keep explicit open-time
// certified ordinal maps.
type columnVectorGraphPreparedVectorView struct {
	rows int
	dims int

	values             []float32
	singlePart         *columnVectorGraphTypedColumnVectorPart
	parts              []*columnVectorGraphTypedColumnVectorPart
	partIndexByOrdinal []uint32
	rowIndexByOrdinal  []uint32
}

type columnVectorGraphPreparedNormView struct {
	rows   int
	values []float32
	source *columnVectorGraphInvNormStateSource
}

func prepareColumnVectorGraphPreparedVectorView(source *columnVectorGraphTypedColumnVectorSource, rowCount, dims int) (columnVectorGraphPreparedVectorView, typeddecode.Reason, string, bool) {
	if source == nil {
		return columnVectorGraphPreparedVectorView{}, "", "", false
	}
	if source.closed {
		return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonStaleHandle, "typed-column vector source is closed", false
	}
	if dims <= 0 || source.dims != dims {
		return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonDimensionMismatch, fmt.Sprintf("typed-column vector source dims=%d want %d", source.dims, dims), false
	}
	if len(source.locations) != rowCount {
		return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonRowCountMismatch, fmt.Sprintf("typed-column vector source locations=%d want rows=%d", len(source.locations), rowCount), false
	}
	if rowCount == 0 {
		return columnVectorGraphPreparedVectorView{rows: rowCount, dims: dims}, "", "", true
	}
	partOrdinal := make(map[*columnVectorGraphTypedColumnVectorPart]int, len(source.parts))
	parts := make([]*columnVectorGraphTypedColumnVectorPart, 0, len(source.parts))
	identity := true
	for ordinal, loc := range source.locations {
		part := loc.part
		if part == nil {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonValidationFailed, fmt.Sprintf("typed-column vector source ordinal=%d has nil part", ordinal), false
		}
		if part.outcome != columnVectorGraphTypedColumnVectorOutcomeMmapDirect {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonHandleSourceUnsupported, fmt.Sprintf("typed-column vector source part generation=%d part_id=%d outcome=%s is not mmap_direct", part.generation, part.partID, part.outcome), false
		}
		if part.handle == nil || part.handle.Released() {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonStaleHandle, fmt.Sprintf("typed-column vector source part generation=%d part_id=%d handle is stale", part.generation, part.partID), false
		}
		if part.rows < 0 {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonRowCountMismatch, fmt.Sprintf("typed-column vector source part generation=%d rows=%d", part.generation, part.rows), false
		}
		if part.rows > 0 && part.rows > maxCollectionInt/dims {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonPayloadLengthMismatch, fmt.Sprintf("typed-column vector source part generation=%d rows=%d dims=%d overflows rows*dims", part.generation, part.rows, dims), false
		}
		wantValues := part.rows * dims
		if wantValues < 0 || len(part.values) != wantValues {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonPayloadLengthMismatch, fmt.Sprintf("typed-column vector source part generation=%d values=%d want rows*dims=%d", part.generation, len(part.values), wantValues), false
		}
		if loc.rowIndex < 0 || loc.rowIndex >= part.rows {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonRowCountMismatch, fmt.Sprintf("typed-column vector source ordinal=%d row_index=%d outside part rows=%d", ordinal, loc.rowIndex, part.rows), false
		}
		if uint64(loc.rowIndex) > uint64(^uint32(0)) {
			return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonRowCountMismatch, fmt.Sprintf("typed-column vector source ordinal=%d row_index=%d exceeds uint32 mapping", ordinal, loc.rowIndex), false
		}
		idx, ok := partOrdinal[part]
		if !ok {
			if uint64(len(parts)) > uint64(^uint32(0)) {
				return columnVectorGraphPreparedVectorView{}, typeddecode.ReasonRowCountMismatch, "typed-column vector source part count exceeds uint32 mapping", false
			}
			idx = len(parts)
			partOrdinal[part] = idx
			parts = append(parts, part)
		}
		if idx != 0 || loc.rowIndex != ordinal {
			identity = false
		}
	}
	if len(parts) == 1 {
		view := columnVectorGraphPreparedVectorView{rows: rowCount, dims: dims, values: parts[0].values, singlePart: parts[0]}
		if !identity {
			view.rowIndexByOrdinal = make([]uint32, rowCount)
			for ordinal, loc := range source.locations {
				view.rowIndexByOrdinal[ordinal] = uint32(loc.rowIndex)
			}
		}
		return view, "", "", true
	}
	view := columnVectorGraphPreparedVectorView{
		rows:               rowCount,
		dims:               dims,
		parts:              parts,
		partIndexByOrdinal: make([]uint32, rowCount),
		rowIndexByOrdinal:  make([]uint32, rowCount),
	}
	for ordinal, loc := range source.locations {
		view.partIndexByOrdinal[ordinal] = uint32(partOrdinal[loc.part])
		view.rowIndexByOrdinal[ordinal] = uint32(loc.rowIndex)
	}
	return view, "", "", true
}

func prepareColumnVectorGraphPreparedNormView(source *columnVectorGraphInvNormStateSource, rowCount int) (columnVectorGraphPreparedNormView, typeddecode.Reason, bool) {
	if source == nil {
		return columnVectorGraphPreparedNormView{}, "", false
	}
	if source.closed || (source.handle != nil && source.handle.Released()) {
		return columnVectorGraphPreparedNormView{}, typeddecode.ReasonStaleHandle, false
	}
	if source.outcome != columnVectorGraphInvNormStateOutcomeMmapDirect {
		return columnVectorGraphPreparedNormView{}, typeddecode.ReasonHandleSourceUnsupported, false
	}
	if source.rows != rowCount || len(source.values) != rowCount {
		return columnVectorGraphPreparedNormView{}, typeddecode.ReasonRowCountMismatch, false
	}
	return columnVectorGraphPreparedNormView{rows: rowCount, values: source.values, source: source}, "", true
}

func (v columnVectorGraphPreparedVectorView) ready() bool {
	return v.rows >= 0 && v.dims > 0 && (v.singlePart != nil || len(v.parts) > 0 || v.rows == 0)
}

func (v columnVectorGraphPreparedVectorView) identityMapping() bool {
	return v.ready() && v.singlePart != nil && v.rowIndexByOrdinal == nil
}

func (v columnVectorGraphPreparedVectorView) vectorForOrdinal(ordinal int) ([]float32, typeddecode.Reason, bool) {
	part, row, ok := v.locationForOrdinal(ordinal)
	if !ok {
		if !v.ready() || ordinal < 0 || ordinal >= v.rows {
			return nil, typeddecode.ReasonRowCountMismatch, false
		}
		return nil, typeddecode.ReasonStaleHandle, false
	}
	start := row * v.dims
	return part.values[start : start+v.dims], "", true
}

func (v columnVectorGraphPreparedVectorView) locationForOrdinal(ordinal int) (*columnVectorGraphTypedColumnVectorPart, int, bool) {
	if !v.ready() || ordinal < 0 || ordinal >= v.rows || v.dims <= 0 {
		return nil, 0, false
	}
	if v.singlePart != nil {
		part := v.singlePart
		if part.handle == nil || part.handle.Released() {
			return nil, 0, false
		}
		row := ordinal
		if v.rowIndexByOrdinal != nil {
			row = int(v.rowIndexByOrdinal[ordinal])
		}
		start := row * v.dims
		end := start + v.dims
		if row < 0 || row >= part.rows || start < 0 || end < start || end > len(part.values) {
			return nil, 0, false
		}
		return part, row, true
	}
	if ordinal >= len(v.partIndexByOrdinal) || ordinal >= len(v.rowIndexByOrdinal) {
		return nil, 0, false
	}
	partIndex := int(v.partIndexByOrdinal[ordinal])
	if partIndex < 0 || partIndex >= len(v.parts) {
		return nil, 0, false
	}
	part := v.parts[partIndex]
	if part == nil || part.handle == nil || part.handle.Released() {
		return nil, 0, false
	}
	row := int(v.rowIndexByOrdinal[ordinal])
	start := row * v.dims
	end := start + v.dims
	if row < 0 || row >= part.rows || start < 0 || end < start || end > len(part.values) {
		return nil, 0, false
	}
	return part, row, true
}

func (v columnVectorGraphPreparedNormView) ready() bool {
	return v.rows >= 0 && v.source != nil && len(v.values) == v.rows
}

func (v columnVectorGraphPreparedNormView) valueForOrdinal(ordinal int) (float32, typeddecode.Reason, bool) {
	if !v.ready() || ordinal < 0 || ordinal >= v.rows {
		return 0, typeddecode.ReasonRowCountMismatch, false
	}
	if v.source.closed || (v.source.handle != nil && v.source.handle.Released()) {
		return 0, typeddecode.ReasonStaleHandle, false
	}
	return v.values[ordinal], "", true
}

func (s *columnVectorGraphSearchSource) scorePreparedOrdinal(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinal int, stats *columnVectorGraphNativeSearchStats) (float64, bool, error) {
	if s == nil || s.reader == nil {
		return 0, false, nil
	}
	if !s.preparedScoreReady {
		return 0, false, nil
	}
	vectorView := &s.preparedVector
	normView := &s.preparedNorm
	if len(query) != s.dims {
		return 0, true, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", ordinal, s.dims, len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	if ordinal < 0 || ordinal >= vectorView.rows || ordinal >= normView.rows {
		return 0, false, nil
	}
	if (s.typedVectorSource != nil && s.typedVectorSource.closed) || normView.source == nil || normView.source.closed || (normView.source.handle != nil && normView.source.handle.Released()) {
		return 0, false, nil
	}

	var vector []float32
	identityMapping := false
	if vectorView.singlePart != nil {
		if vectorView.singlePart.handle == nil || vectorView.singlePart.handle.Released() {
			return 0, false, nil
		}
		row := ordinal
		identityMapping = s.preparedScoreIdentityMapping
		if !identityMapping {
			row = int(vectorView.rowIndexByOrdinal[ordinal])
		}
		start := row * vectorView.dims
		vector = vectorView.values[start : start+vectorView.dims]
	} else {
		partIndex := int(vectorView.partIndexByOrdinal[ordinal])
		part := vectorView.parts[partIndex]
		if part == nil || part.handle == nil || part.handle.Released() {
			return 0, false, nil
		}
		row := int(vectorView.rowIndexByOrdinal[ordinal])
		start := row * vectorView.dims
		vector = part.values[start : start+vectorView.dims]
	}
	invNorm := normView.values[ordinal]

	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		stats.PreparedScoreCalls++
		stats.FP32ScoreCalls++
		stats.VisitedNodes++
		stats.CandidateFetches++
		stats.VectorBytesRead += uint64(vectorView.dims) * 4
		stats.NormBytesRead += 4
		stats.VectorDirectViews++
		stats.VectorMmapDirectViews++
		stats.VectorPreparedDirectViews++
		if identityMapping {
			stats.VectorPreparedIdentityMappings++
		} else {
			stats.VectorPreparedRowRefMappings++
		}
		stats.NormDirectViews++
		stats.NormMmapDirectViews++
		stats.NormPreparedDirectViews++
		if plan != nil {
			stats.BlockViewHits = plan.hits
			stats.BlockViewMisses = plan.misses
			stats.BlockViewBuilds = plan.builds
		}
	}
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
		return 0, true, fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is not finite", s.reader.def.Name, ordinal)
	}
	return score, true, nil
}
