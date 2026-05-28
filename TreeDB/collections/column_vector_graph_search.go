package collections

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

// Keep modest scratch overgrowth to avoid realloc churn when callers vary
// TopK/EfSearch slightly, while still releasing oversized scratch after large
// probes.
const columnVectorGraphNativeScratchOversizeSlack = 16

// Default TopK values are small; insertion order avoids sort overhead there.
// Larger result sets switch to sort.Slice so result ordering does not go O(k^2).
const columnVectorGraphNativeResultOrderInsertionSortLimit = 32

var (
	errColumnVectorGraphNativeSearchScratchRequired            = errors.New("collections: column_graph native search requires caller-owned scratch")
	errColumnVectorGraphNativeSearchQueryDimensionMismatch     = errors.New("collections: column_graph native search query dimension mismatch")
	errColumnVectorGraphNativeSearchQueryNormInvalid           = errors.New("collections: column_graph native search query norm invalid")
	errColumnVectorGraphNativeSearchTopKNegative               = errors.New("collections: column_graph native search top_k cannot be negative")
	errColumnVectorGraphNativeSearchEfSearchNegative           = errors.New("collections: column_graph native search ef_search cannot be negative")
	errColumnVectorGraphNativeSearchCandidateDimensionMismatch = errors.New("collections: column_graph native search candidate dimension mismatch")
)

type columnVectorGraphNativeSearchOptions struct {
	TopK     int
	EfSearch int

	// CandidateRows is an optional pre-composed row-domain filter over graph
	// ordinals. It is intentionally internal until public metadata predicate
	// planning is designed; callers that set it should compose predicate and
	// visibility masks through typedcolumn.ComposeRowSelections first.
	CandidateRows    typedcolumn.RowSelection
	HasCandidateRows bool
}

type columnVectorGraphNativeSearchStats struct {
	CandidateRows                    uint64
	Candidates                       uint64
	Edges                            uint64
	VisitedNodes                     uint64
	VisitedEdges                     uint64
	VectorBytesRead                  uint64
	AdjacencyBytesRead               uint64
	CandidateFetches                 uint64
	ExpansionFetches                 uint64
	ResultFetches                    uint64
	ScoreBatches                     uint64
	OrdinalsGrouped                  uint64
	BlockViewHits                    uint64
	BlockViewMisses                  uint64
	BlockViewBuilds                  uint64
	AdjacencyExpansions              uint64
	AdjacencyScratchDecodes          uint64
	AdjacencyDirectViews             uint64
	AdjacencyMmapDirectViews         uint64
	AdjacencyHeapCopyTypedViews      uint64
	AdjacencySourceUnavailable       uint64
	AdjacencySourceFallbacks         uint64
	AdjacencyCertificationFailures   uint64
	AdjacencyAbsoluteOffsetUnaligned uint64
	AdjacencyActualPointerUnaligned  uint64
	AdjacencyStaleHandles            uint64
	VectorDirectViews                uint64
	VectorMmapDirectViews            uint64
	VectorHeapCopyTypedViews         uint64
	VectorScratchDecodes             uint64
	VectorCertificationFailures      uint64
	VectorAbsoluteOffsetUnaligned    uint64
	VectorActualPointerUnaligned     uint64
	VectorStaleHandles               uint64
	TypedColumnMappedBytes           uint64
	TypedColumnHeapCopyBytes         uint64
	TypedColumnDecodedBytes          uint64
	TypedColumnActiveHandles         int64
	TypedColumnDeniedResources       uint64
	TypedColumnFallbacks             uint64
}

// columnVectorGraphNativeSearchResult aliases buffers owned by the search
// scratch. Callers must copy the returned result slice and any retained result
// IDs before the next search with the same scratch.
type columnVectorGraphNativeSearchResult struct {
	Ordinal int
	ID      []byte
	Score   float64
}

type columnVectorGraphSearchCandidate struct {
	ordinal int
	score   float64
}

// columnVectorGraphNativeSearchScratch is caller-owned mutable search state.
// It is not concurrency-safe. Parallel searches over immutable graph assets are
// valid with one reader and one scratch per worker.
type columnVectorGraphNativeSearchScratch struct {
	scoreScratch   columnPhysicalRowReaderScratch
	expandScratch  columnPhysicalRowReaderScratch
	resultScratch  columnPhysicalRowReaderScratch
	visitMarks     []uint64
	visitEpoch     uint64
	frontier       []columnVectorGraphSearchCandidate
	top            []columnVectorGraphSearchCandidate
	results        []columnVectorGraphNativeSearchResult
	idBuffers      [][]byte
	resultOrder    []int
	resultOrdinals []int
	searchPlan     columnVectorGraphSearchPlan
}

func (s *columnVectorGraphNativeSearchScratch) prepare(rowCount, dimensions, degree, topK, efSearch int) error {
	if s == nil {
		return errColumnVectorGraphNativeSearchScratchRequired
	}
	if rowCount < 0 || dimensions < 0 || degree < 0 || topK < 0 || efSearch < 0 {
		return fmt.Errorf("collections: column_graph native search received negative sizing input: rowCount=%d dimensions=%d degree=%d topK=%d efSearch=%d", rowCount, dimensions, degree, topK, efSearch)
	}
	for _, rowScratch := range []*columnPhysicalRowReaderScratch{&s.scoreScratch, &s.expandScratch, &s.resultScratch} {
		prepareColumnVectorGraphNativeRowScratch(rowScratch, dimensions, degree)
	}
	s.visitMarks = resizeColumnVectorGraphNativeUint64Scratch(s.visitMarks, rowCount)
	s.visitEpoch++
	if s.visitEpoch == 0 {
		clear(s.visitMarks)
		s.visitEpoch = 1
	}
	frontierCap := efSearch
	if frontierCap > rowCount {
		frontierCap = rowCount
	}
	if frontierCap < topK {
		frontierCap = topK
	}
	s.frontier = resizeColumnVectorGraphNativeCandidateScratch(s.frontier, frontierCap)
	s.top = resizeColumnVectorGraphNativeCandidateScratch(s.top, topK)
	s.results = resizeColumnVectorGraphNativeResultScratch(s.results, topK)
	s.idBuffers = resizeColumnVectorGraphNativeIDBuffersScratch(s.idBuffers, topK)
	s.resultOrder = resizeColumnVectorGraphNativeIntScratch(s.resultOrder, topK)
	s.resultOrdinals = resizeColumnVectorGraphNativeIntScratch(s.resultOrdinals, topK)
	return nil
}

func prepareColumnVectorGraphNativeRowScratch(s *columnPhysicalRowReaderScratch, dimensions, degree int) {
	if cap(s.Values) < columnVectorGraphPhysicalRowValueCount {
		s.Values = make([]columnDeclaredValue, 0, columnVectorGraphPhysicalRowValueCount)
	} else {
		clear(s.Values)
		s.Values = s.Values[:0]
	}
	if cap(s.Float32Values) < dimensions {
		s.Float32Values = make([]float32, 0, dimensions)
	} else {
		s.Float32Values = s.Float32Values[:0]
	}
	if cap(s.Uint32Values) < degree {
		s.Uint32Values = make([]uint32, 0, degree)
	} else {
		s.Uint32Values = s.Uint32Values[:0]
	}
}

func resizeColumnVectorGraphNativeCandidateScratch(dst []columnVectorGraphSearchCandidate, target int) []columnVectorGraphSearchCandidate {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]columnVectorGraphSearchCandidate, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeResultScratch(dst []columnVectorGraphNativeSearchResult, target int) []columnVectorGraphNativeSearchResult {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		clear(dst)
		return make([]columnVectorGraphNativeSearchResult, 0, target)
	}
	if len(dst) > 0 {
		clear(dst)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeIntScratch(dst []int, target int) []int {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]int, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeUint64Scratch(dst []uint64, target int) []uint64 {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]uint64, target)
	}
	return dst[:target]
}

func resizeColumnVectorGraphNativeIDBuffersScratch(dst [][]byte, target int) [][]byte {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		next := make([][]byte, target)
		copy(next, dst)
		return next
	}
	if len(dst) < target {
		oldLen := len(dst)
		dst = dst[:target]
		clear(dst[oldLen:target])
		return dst
	}
	for i := target; i < len(dst); i++ {
		dst[i] = nil
	}
	return dst[:target]
}

func columnVectorGraphNativeScratchCapOversized(capacity, target int) bool {
	if target < 0 {
		return true
	}
	if target > (math.MaxInt-columnVectorGraphNativeScratchOversizeSlack)/2 {
		return false
	}
	return capacity > target*2+columnVectorGraphNativeScratchOversizeSlack
}

// SearchCosine traverses the persisted column graph through the physical row
// reader. It fetches only graph rows: document materialization stays outside
// this kernel. Returned results and result IDs alias scratch-owned buffers and
// must be copied before the next SearchCosine call with the same scratch.
func (r *columnVectorGraphPhysicalRowReader) SearchCosine(query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) (results []columnVectorGraphNativeSearchResult, stats columnVectorGraphNativeSearchStats, err error) {
	if r == nil || r.reader == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errNilColumnVectorGraphPhysicalRowReader
	}
	if len(query) != r.def.Dimensions {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	rowCount := r.RowCount()
	topK := opts.TopK
	if topK < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search top_k cannot be negative: %w", r.def.Name, errColumnVectorGraphNativeSearchTopKNegative)
	}
	efSearch := opts.EfSearch
	if efSearch < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search ef_search cannot be negative: %w", r.def.Name, errColumnVectorGraphNativeSearchEfSearchNegative)
	}
	if topK == 0 || rowCount == 0 {
		return nil, columnVectorGraphNativeSearchStats{}, nil
	}
	candidateRows, hasCandidateRows, err := columnVectorGraphSearchCandidateRows(opts, rowCount)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q candidate rows: %w", r.def.Name, err)
	}
	candidateRowCount := rowCount
	if hasCandidateRows {
		candidateRowCount = candidateRows.Count()
	}
	stats.CandidateRows = uint64(candidateRowCount)
	if candidateRowCount == 0 {
		return nil, stats, nil
	}
	if scratch == nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query norm: %w: %w", r.def.Name, errColumnVectorGraphNativeSearchQueryNormInvalid, err)
	}
	if topK > candidateRowCount {
		topK = candidateRowCount
	}
	if efSearch == 0 {
		efSearch = r.def.EfSearch
	}
	if efSearch < topK {
		efSearch = topK
	}
	if efSearch > candidateRowCount {
		efSearch = candidateRowCount
	}
	degree := r.def.M
	if degree < 0 {
		degree = 0
	}
	if err := scratch.prepare(rowCount, r.def.Dimensions, degree, topK, efSearch); err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search scratch prepare: %w", r.def.Name, err)
	}

	defer func() {
		r.populateTypedColumnVectorSearchStats(&stats)
		r.populateLayer0AdjacencySourceSearchStats(&stats)
	}()
	plan, err := scratch.prepareSearchPlan(r)
	if err != nil {
		return nil, stats, err
	}
	var singleBlockView *columnVectorGraphBlockView
	if plan.physicalReader != nil && len(plan.physicalReader.ranges) == 1 {
		singleBlockView, err = plan.blockViewForAssetOrdinal(0)
		if err != nil {
			return nil, stats, err
		}
	}
	visitMarks := scratch.visitMarks
	visitEpoch := scratch.visitEpoch
	entryOrdinal, ok := columnVectorGraphNextCandidateSeed(0, rowCount, candidateRows, hasCandidateRows, visitMarks, visitEpoch)
	if !ok {
		return scratch.results, stats, nil
	}
	maxLayer, err := r.maxAdjacencyLayer(plan, singleBlockView, entryOrdinal, scratch, &stats)
	if err != nil {
		return nil, stats, err
	}
	for layer := maxLayer; layer > 0; layer-- {
		entryOrdinal, err = r.greedyNearestAtLayer(plan, singleBlockView, query, queryInvNorm, entryOrdinal, layer, candidateRows, hasCandidateRows, scratch, &stats)
		if err != nil {
			return nil, stats, err
		}
	}
	visitMarks[entryOrdinal] = visitEpoch
	if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, entryOrdinal, topK, scratch, &stats); err != nil {
		return nil, stats, err
	}
	nextSeed := 0
	for stats.Candidates < uint64(efSearch) {
		candidate, ok := scratch.popFrontier()
		if !ok {
			seed, ok := columnVectorGraphNextCandidateSeed(nextSeed, rowCount, candidateRows, hasCandidateRows, visitMarks, visitEpoch)
			if !ok {
				break
			}
			nextSeed = seed + 1
			visitMarks[seed] = visitEpoch
			if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, seed, topK, scratch, &stats); err != nil {
				return nil, stats, err
			}
			continue
		}
		adjacency, err := r.expandCandidateAdjacencyLayer(plan, singleBlockView, candidate.ordinal, 0, scratch, &stats)
		if err != nil {
			return nil, stats, err
		}
		for i, neighbor := range adjacency {
			if stats.Candidates >= uint64(efSearch) {
				break
			}
			stats.Edges++
			stats.VisitedEdges++
			if uint64(neighbor) >= uint64(rowCount) {
				return nil, stats, fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
			neighborOrdinal := int(neighbor)
			if visitMarks[neighborOrdinal] == visitEpoch {
				continue
			}
			if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
				visitMarks[neighborOrdinal] = visitEpoch
				continue
			}
			visitMarks[neighborOrdinal] = visitEpoch
			if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, neighborOrdinal, topK, scratch, &stats); err != nil {
				return nil, stats, err
			}
		}
	}

	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	if err := r.fetchTopSearchResults(plan, singleBlockView, scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func columnVectorGraphSearchCandidateRows(opts columnVectorGraphNativeSearchOptions, rowCount int) (typedcolumn.RowSelection, bool, error) {
	if !opts.HasCandidateRows {
		return typedcolumn.RowSelection{}, false, nil
	}
	if opts.CandidateRows.Rows() != rowCount {
		return typedcolumn.RowSelection{}, false, fmt.Errorf("selection rows=%d want graph rows=%d", opts.CandidateRows.Rows(), rowCount)
	}
	return opts.CandidateRows, true, nil
}

func composeColumnVectorGraphCandidateRowSelection(rowCount int, predicate *typedcolumn.RowSelection, visibility *typedcolumn.RowSelection, scratch *typedcolumn.RowSelectionScratch) (typedcolumn.RowSelection, bool, error) {
	if predicate == nil && visibility == nil {
		selection, err := typedcolumn.NewAllRowSelection(rowCount)
		return selection, false, err
	}
	selection, err := typedcolumn.ComposeRowSelectionsInto(rowCount, typedcolumn.RowSelectionComponents{Predicate: predicate, Visibility: visibility}, scratch)
	return selection, true, err
}

func columnVectorGraphCandidateRowAllowed(selection typedcolumn.RowSelection, hasSelection bool, ordinal int) bool {
	return !hasSelection || selection.Contains(ordinal)
}

func columnVectorGraphNextCandidateSeed(start int, rowCount int, selection typedcolumn.RowSelection, hasSelection bool, visitMarks []uint64, visitEpoch uint64) (int, bool) {
	if start < 0 {
		start = 0
	}
	for ordinal := start; ordinal < rowCount; ordinal++ {
		if len(visitMarks) > ordinal && visitMarks[ordinal] == visitEpoch {
			continue
		}
		if !columnVectorGraphCandidateRowAllowed(selection, hasSelection, ordinal) {
			continue
		}
		return ordinal, true
	}
	return 0, false
}

func (r *columnVectorGraphPhysicalRowReader) maxAdjacencyLayer(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if layer, layerAdjacency, outcome, fallbackReason, ok := r.maxDirectAdjacencyLayerForOrdinal(ordinal); ok {
		recordColumnVectorGraphAdjacencySourceOutcomeStats(stats, len(layerAdjacency), outcome)
		return layer, nil
	} else if fallbackReason != "" {
		recordColumnVectorGraphAdjacencyFallbackReasonStats(stats, fallbackReason)
		if stats != nil {
			stats.AdjacencySourceFallbacks++
		}
	}
	adjacency, direct, err := r.rawCandidateAdjacencyWithDirectView(plan, singleBlockView, ordinal, scratch)
	if err != nil {
		return 0, err
	}
	recordColumnVectorGraphAdjacencySourceStats(stats, len(adjacency), direct)
	return columnVectorGraphAdjacencyMaxLayer(adjacency)
}

func (r *columnVectorGraphPhysicalRowReader) greedyNearestAtLayer(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, entryOrdinal int, layer int, candidateRows typedcolumn.RowSelection, hasCandidateRows bool, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	best := entryOrdinal
	bestScore, err := r.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, best, scratch, stats)
	if err != nil {
		return 0, err
	}
	changed := true
	for changed {
		changed = false
		adjacency, err := r.expandCandidateAdjacencyLayer(plan, singleBlockView, best, layer, scratch, stats)
		if err != nil {
			return 0, err
		}
		for i, neighbor := range adjacency {
			if stats != nil {
				stats.Edges++
				stats.VisitedEdges++
			}
			if uint64(neighbor) >= uint64(r.RowCount()) {
				return 0, fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, best, i, neighbor, r.RowCount(), errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
			neighborOrdinal := int(neighbor)
			if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
				continue
			}
			score, err := r.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, neighborOrdinal, scratch, stats)
			if err != nil {
				return 0, err
			}
			if score > bestScore || (score == bestScore && neighborOrdinal < best) {
				best = neighborOrdinal
				bestScore = score
				changed = true
			}
		}
	}
	return best, nil
}

func (r *columnVectorGraphPhysicalRowReader) fetchTopSearchResults(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	n := len(scratch.top)
	scratch.resultOrder = scratch.resultOrder[:n]
	scratch.resultOrdinals = scratch.resultOrdinals[:n]
	for i := 0; i < n; i++ {
		scratch.resultOrder[i] = i
	}
	sortColumnVectorGraphResultOrderByOrdinal(scratch.resultOrder, scratch.top)
	for fetchPos, topIndex := range scratch.resultOrder {
		scratch.resultOrdinals[fetchPos] = scratch.top[topIndex].ordinal
	}
	for resultPos, topIndex := range scratch.resultOrder {
		ordinal := scratch.resultOrdinals[resultPos]
		view := singleBlockView
		rowIndex := ordinal
		if view == nil {
			refView, ref, err := plan.blockViewForOrdinal(ordinal)
			if err != nil {
				return err
			}
			view = refView
			rowIndex = ref.rowIndex
		}
		id, err := view.id(rowIndex)
		if err != nil {
			return err
		}
		if cap(scratch.idBuffers[topIndex]) < len(id) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		} else if columnVectorGraphNativeScratchCapOversized(cap(scratch.idBuffers[topIndex]), len(id)) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		}
		scratch.idBuffers[topIndex] = scratch.idBuffers[topIndex][:len(id)]
		copy(scratch.idBuffers[topIndex], id)
	}
	stats.ResultFetches += uint64(n)
	stats.BlockViewHits = plan.hits
	stats.BlockViewMisses = plan.misses
	stats.BlockViewBuilds = plan.builds
	for i, candidate := range scratch.top {
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal: candidate.ordinal,
			ID:      scratch.idBuffers[i],
			Score:   candidate.score,
		})
	}
	return nil
}

func sortColumnVectorGraphResultOrderByOrdinal(order []int, top []columnVectorGraphSearchCandidate) {
	if len(order) <= columnVectorGraphNativeResultOrderInsertionSortLimit {
		for i := 1; i < len(order); i++ {
			item := order[i]
			ordinal := top[item].ordinal
			j := i - 1
			for j >= 0 && top[order[j]].ordinal > ordinal {
				order[j+1] = order[j]
				j--
			}
			order[j+1] = item
		}
		return
	}
	sort.Slice(order, func(i, j int) bool {
		return top[order[i]].ordinal < top[order[j]].ordinal
	})
}

func (r *columnVectorGraphPhysicalRowReader) scoreAndPushFrontierVisited(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinal, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	score, err := r.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, ordinal, scratch, stats)
	if err != nil {
		return err
	}
	stats.Candidates++
	candidate := columnVectorGraphSearchCandidate{
		ordinal: ordinal,
		score:   score,
	}
	scratch.insertTop(topK, candidate)
	scratch.pushFrontier(candidate)
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) scoreOrdinal(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	view := singleBlockView
	rowIndex := ordinal
	if view == nil {
		refView, ref, err := plan.blockViewForOrdinal(ordinal)
		if err != nil {
			return 0, err
		}
		view = refView
		rowIndex = ref.rowIndex
	}
	if stats != nil {
		stats.ScoreBatches++
		stats.OrdinalsGrouped++
		stats.VisitedNodes++
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
	}
	var vector []float32
	if typedVector, outcome, fallbackReason, ok := r.typedVectorForOrdinal(ordinal); ok {
		vector = typedVector
		recordColumnVectorGraphVectorSourceStats(stats, outcome, fallbackReason)
	} else {
		scratch.scoreScratch.Float32Values = scratch.scoreScratch.Float32Values[:0]
		var vectorScratch []float32
		vector, vectorScratch = view.vectorUnchecked(rowIndex, scratch.scoreScratch.Float32Values)
		scratch.scoreScratch.Float32Values = vectorScratch
		if stats != nil {
			stats.VectorScratchDecodes++
		}
	}
	invNorm := view.invNormUnchecked(rowIndex)
	if stats != nil {
		stats.CandidateFetches++
		stats.VectorBytesRead += uint64(len(vector)) * 4
	}
	score, err := columnVectorGraphNativeCosineScoreVector(query, queryInvNorm, ordinal, vector, invNorm)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is not finite", r.def.Name, ordinal)
	}
	return score, nil
}

func (r *columnVectorGraphPhysicalRowReader) expandCandidateAdjacencyLayer(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, layer int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]uint32, error) {
	if layerAdjacency, outcome, fallbackReason, ok := r.directAdjacencyLayerForOrdinal(ordinal, layer); ok {
		if stats != nil {
			stats.ExpansionFetches++
			stats.AdjacencyExpansions++
			recordColumnVectorGraphAdjacencySourceOutcomeStats(stats, len(layerAdjacency), outcome)
			stats.BlockViewHits = plan.hits
			stats.BlockViewMisses = plan.misses
			stats.BlockViewBuilds = plan.builds
		}
		return layerAdjacency, nil
	} else if fallbackReason != "" {
		recordColumnVectorGraphAdjacencyFallbackReasonStats(stats, fallbackReason)
		if stats != nil {
			stats.AdjacencySourceFallbacks++
		}
	}
	adjacency, direct, err := r.rawCandidateAdjacencyWithDirectView(plan, singleBlockView, ordinal, scratch)
	if err != nil {
		return nil, err
	}
	layerAdjacency, err := columnVectorGraphAdjacencyLayer(adjacency, layer)
	if err != nil {
		return nil, fmt.Errorf("collections: column_graph %q ordinal=%d malformed adjacency layer=%d: %w", r.def.Name, ordinal, layer, err)
	}
	if stats != nil {
		stats.ExpansionFetches++
		stats.AdjacencyExpansions++
		recordColumnVectorGraphAdjacencySourceStats(stats, len(adjacency), direct)
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
	}
	return layerAdjacency, nil
}

func recordColumnVectorGraphVectorSourceStats(stats *columnVectorGraphNativeSearchStats, outcome columnVectorGraphTypedColumnVectorOutcome, fallbackReason typeddecode.Reason) {
	if stats == nil {
		return
	}
	switch outcome {
	case columnVectorGraphTypedColumnVectorOutcomeMmapDirect:
		stats.VectorMmapDirectViews++
		stats.VectorDirectViews++
	case columnVectorGraphTypedColumnVectorOutcomeHeapCopyTypedView:
		stats.VectorHeapCopyTypedViews++
	case columnVectorGraphTypedColumnVectorOutcomeScratchDecode:
		stats.VectorScratchDecodes++
	default:
		stats.VectorScratchDecodes++
	}
	recordColumnVectorGraphVectorFallbackReasonStats(stats, fallbackReason)
}

func recordColumnVectorGraphVectorFallbackReasonStats(stats *columnVectorGraphNativeSearchStats, reason typeddecode.Reason) {
	if stats == nil || reason == "" || reason == typeddecode.ReasonSupported {
		return
	}
	switch reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonUnaligned:
		stats.VectorAbsoluteOffsetUnaligned++
	case typeddecode.ReasonActualPointerUnaligned:
		stats.VectorActualPointerUnaligned++
	case typeddecode.ReasonStaleHandle:
		stats.VectorStaleHandles++
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonLengthMultipleMismatch, typeddecode.ReasonPayloadLengthMismatch, typeddecode.ReasonRowCountMismatch, typeddecode.ReasonDimensionMismatch, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper, typeddecode.ReasonValidationFailed:
		stats.VectorCertificationFailures++
	}
}

func recordColumnVectorGraphAdjacencySourceStats(stats *columnVectorGraphNativeSearchStats, adjacencyLen int, direct bool) {
	if stats == nil {
		return
	}
	stats.AdjacencyBytesRead += uint64(adjacencyLen) * 4
	if adjacencyLen == 0 {
		return
	}
	if direct {
		stats.AdjacencyDirectViews++
		return
	}
	stats.AdjacencyScratchDecodes++
}

func recordColumnVectorGraphAdjacencySourceOutcomeStats(stats *columnVectorGraphNativeSearchStats, adjacencyLen int, outcome columnVectorGraphLayer0AdjacencySourceOutcome) {
	if stats == nil {
		return
	}
	stats.AdjacencyBytesRead += uint64(adjacencyLen) * 4
	switch outcome {
	case columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect:
		stats.AdjacencyDirectViews++
		stats.AdjacencyMmapDirectViews++
	case columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView:
		stats.AdjacencyHeapCopyTypedViews++
	default:
		if adjacencyLen > 0 {
			stats.AdjacencyScratchDecodes++
		}
	}
}

func recordColumnVectorGraphAdjacencyFallbackReasonStats(stats *columnVectorGraphNativeSearchStats, reason typeddecode.Reason) {
	if stats == nil || reason == "" || reason == typeddecode.ReasonSupported {
		return
	}
	switch reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonUnaligned:
		stats.AdjacencyAbsoluteOffsetUnaligned++
	case typeddecode.ReasonActualPointerUnaligned:
		stats.AdjacencyActualPointerUnaligned++
	case typeddecode.ReasonNilHandle, typeddecode.ReasonStaleHandle:
		stats.AdjacencyStaleHandles++
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonLengthMultipleMismatch, typeddecode.ReasonPayloadLengthMismatch, typeddecode.ReasonRowCountMismatch, typeddecode.ReasonDimensionMismatch, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper, typeddecode.ReasonValidationFailed, typeddecode.ReasonOffsetsCountMismatch, typeddecode.ReasonOffsetsStartMismatch, typeddecode.ReasonOffsetsNonMonotonic, typeddecode.ReasonOffsetsGoIntRange, typeddecode.ReasonValuesLengthMismatch:
		stats.AdjacencyCertificationFailures++
	}
}

func (r *columnVectorGraphPhysicalRowReader) maxDirectAdjacencyLayerForOrdinal(ordinal int) (int, []uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	if r == nil || r.adjacencyLayerSources == nil || !r.adjacencyLayerSources.allLayers {
		return 0, nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
	}
	return r.adjacencyLayerSources.MaxLayerForOrdinal(ordinal)
}

func (r *columnVectorGraphPhysicalRowReader) directAdjacencyLayerForOrdinal(ordinal int, layer int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	if r == nil {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
	}
	if r.adjacencyLayerSources != nil {
		return r.adjacencyLayerSources.Neighbors(layer, ordinal)
	}
	if layer == 0 && r.layer0AdjacencySource != nil {
		return r.layer0AdjacencySource.Neighbors(ordinal)
	}
	return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
}

func (r *columnVectorGraphPhysicalRowReader) layer0AdjacencyForOrdinal(ordinal int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	return r.directAdjacencyLayerForOrdinal(ordinal, 0)
}

func (r *columnVectorGraphPhysicalRowReader) populateLayer0AdjacencySourceSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if r == nil || stats == nil || r.adjacencyLayerSources != nil || r.layer0AdjacencySource != nil {
		return
	}
	stats.AdjacencySourceUnavailable = 1
	if r.layer0AdjacencySourceUnavailable {
		stats.AdjacencySourceFallbacks = 1
		return
	}
	if r.layer0AdjacencySourceFallbackReason != "" {
		stats.AdjacencySourceFallbacks = 1
		recordColumnVectorGraphAdjacencyFallbackReasonStats(stats, r.layer0AdjacencySourceFallbackReason)
	}
}

func (r *columnVectorGraphPhysicalRowReader) rawCandidateAdjacency(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, scratch *columnVectorGraphNativeSearchScratch) ([]uint32, error) {
	adjacency, _, err := r.rawCandidateAdjacencyWithDirectView(plan, singleBlockView, ordinal, scratch)
	return adjacency, err
}

func (r *columnVectorGraphPhysicalRowReader) rawCandidateAdjacencyWithDirectView(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, scratch *columnVectorGraphNativeSearchScratch) ([]uint32, bool, error) {
	view := singleBlockView
	rowIndex := ordinal
	if view == nil {
		refView, ref, err := plan.blockViewForOrdinal(ordinal)
		if err != nil {
			return nil, false, err
		}
		view = refView
		rowIndex = ref.rowIndex
	}
	scratch.expandScratch.Uint32Values = scratch.expandScratch.Uint32Values[:0]
	adjacency, adjacencyScratch, direct, err := view.adjacency(rowIndex, scratch.expandScratch.Uint32Values)
	if err != nil {
		return nil, false, err
	}
	scratch.expandScratch.Uint32Values = adjacencyScratch
	return adjacency, direct, nil
}

func (s *columnVectorGraphNativeSearchScratch) markVisited(ordinal int) bool {
	if ordinal < 0 || ordinal >= len(s.visitMarks) || s.visitMarks[ordinal] == s.visitEpoch {
		return false
	}
	s.visitMarks[ordinal] = s.visitEpoch
	return true
}

func (s *columnVectorGraphNativeSearchScratch) pushFrontier(candidate columnVectorGraphSearchCandidate) {
	s.frontier = append(s.frontier, candidate)
	s.frontierSiftUp(len(s.frontier) - 1)
}

func (s *columnVectorGraphNativeSearchScratch) popFrontier() (columnVectorGraphSearchCandidate, bool) {
	if len(s.frontier) == 0 {
		return columnVectorGraphSearchCandidate{}, false
	}
	lastIdx := len(s.frontier) - 1
	best := s.frontier[0]
	last := s.frontier[lastIdx]
	s.frontier = s.frontier[:lastIdx]
	if len(s.frontier) > 0 {
		s.frontier[0] = last
		s.frontierSiftDown(0)
	}
	return best, true
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !columnVectorGraphSearchCandidateBetter(s.frontier[idx], s.frontier[parent]) {
			return
		}
		s.frontier[idx], s.frontier[parent] = s.frontier[parent], s.frontier[idx]
		idx = parent
	}
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftDown(idx int) {
	for {
		left := idx*2 + 1
		if left >= len(s.frontier) {
			return
		}
		child := left
		if right := left + 1; right < len(s.frontier) && columnVectorGraphSearchCandidateBetter(s.frontier[right], s.frontier[left]) {
			child = right
		}
		if !columnVectorGraphSearchCandidateBetter(s.frontier[child], s.frontier[idx]) {
			return
		}
		s.frontier[idx], s.frontier[child] = s.frontier[child], s.frontier[idx]
		idx = child
	}
}

func (s *columnVectorGraphNativeSearchScratch) insertTop(limit int, candidate columnVectorGraphSearchCandidate) {
	if limit <= 0 {
		return
	}
	pos := len(s.top)
	for pos > 0 && columnVectorGraphSearchCandidateBetter(candidate, s.top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return
	}
	if len(s.top) < limit {
		s.top = append(s.top, columnVectorGraphSearchCandidate{})
	}
	copy(s.top[pos+1:], s.top[pos:len(s.top)-1])
	s.top[pos] = candidate
}

func columnVectorGraphSearchCandidateBetter(left, right columnVectorGraphSearchCandidate) bool {
	if left.score == right.score {
		return left.ordinal < right.ordinal
	}
	return left.score > right.score
}

func columnVectorGraphNativeCosineScore(query []float32, queryInvNorm float32, row columnVectorGraphPhysicalRow) (float64, error) {
	return columnVectorGraphNativeCosineScoreVector(query, queryInvNorm, row.Ordinal, row.Vector, row.InvNorm)
}

func columnVectorGraphNativeCosineScoreVector(query []float32, queryInvNorm float32, ordinal int, vector []float32, invNorm float32) (float64, error) {
	if len(vector) != len(query) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", ordinal, len(vector), len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	dot := float64(vectorDotProductFloat32(query, vector))
	if !math.IsInf(dot, 0) && !math.IsNaN(dot) {
		return dot * float64(queryInvNorm) * float64(invNorm), nil
	}
	dot = columnVectorGraphNativeDotProductFloat64(query, vector)
	return dot * float64(queryInvNorm) * float64(invNorm), nil
}

func columnVectorGraphNativeDotProductFloat64(left, right []float32) float64 {
	var dot float64
	for i, v := range left {
		dot += float64(v) * float64(right[i])
	}
	return dot
}
