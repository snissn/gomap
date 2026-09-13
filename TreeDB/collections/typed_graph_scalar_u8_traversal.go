package collections

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// typedGraphScalarU8TraversalOptions is the private Q1 handoff between typed
// filter preparation and scalar-u8 candidate collection. It intentionally has
// no public query-mode surface: Q2 owns visibility-aware reranking and Q3 owns
// public option exposure.
type typedGraphScalarU8TraversalOptions struct {
	TopK        int
	EfSearch    int
	ScoreBudget int

	QuantizedIndexName string
	ScoreBatchMode     columnVectorGraphScoreBatchMode
	StatsMode          columnVectorGraphNativeSearchStatsMode

	// PreparedFilter is the authoritative typed-filter pin. The private
	// collector derives both base membership and any cached local navigation
	// from it; callers cannot pair a navigation map with a same-sized but
	// different RowSelection.
	PreparedFilter *typedGraphPreparedFilter
}

// typedGraphScalarU8ScorePlane is the smallest typed adapter around the
// prepared legacy scalar-u8 plane. It translates an optional local navigation
// ordinal to the immutable base code row and charges every actual scalar-u8
// invocation against one strict score allowance before dispatching a whole
// batch. The base plane remains the code/kernel owner.
type typedGraphScalarU8ScorePlane struct {
	ctx        context.Context
	base       *columnHNSWPreparedScalarU8ScorePlane
	ordinals   []uint32
	baseRows   int
	scoreLimit uint64
}

func (p *typedGraphScalarU8ScorePlane) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindQuantized
}

func (p *typedGraphScalarU8ScorePlane) prepareForHNSWPreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, _ columnHNSWPreparedTraversalOptions, _ *columnVectorGraphNativeSearchScratch) error {
	if err := p.contextErr(); err != nil {
		return err
	}
	if p == nil || p.base == nil || !p.base.ready || pack == nil || p.baseRows < 0 {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if len(query) != pack.Header.Dimensions || p.base.scorer.dims != pack.Header.Dimensions || p.base.scorer.codeRows.Rows() != p.baseRows {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if p.ordinals != nil && len(p.ordinals) != pack.Header.Rows {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.base.scorer.validatePrepared()
}

func (p *typedGraphScalarU8ScorePlane) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if err := p.contextErr(); err != nil {
		return 0, err
	}
	rowID, err := p.mapOrdinal(ordinal)
	if err != nil {
		return 0, err
	}
	if err := p.preflight(stats, 1); err != nil {
		return 0, err
	}
	score, err := p.base.scorer.scoreRowIDPrevalidated(rowID, scratch, stats)
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (p *typedGraphScalarU8ScorePlane) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	rowIDs, err := p.mapOrdinals(ordinals, scratch)
	if err != nil {
		return dst[:0], err
	}
	return p.scoreMappedRowIDs(rowIDs, dst, scratch, stats)
}

func (p *typedGraphScalarU8ScorePlane) scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	mapped, err := p.mapRowIDs(rowIDs, scratch)
	if err != nil {
		return dst[:0], err
	}
	return p.scoreMappedRowIDs(mapped, dst, scratch, stats)
}

func (p *typedGraphScalarU8ScorePlane) scoreGreedyBestRowIDsPrevalidated(rowIDs []uint32, best int, bestScore float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, float64, bool, error) {
	mapped, err := p.mapRowIDs(rowIDs, scratch)
	if err != nil {
		return best, bestScore, false, err
	}
	if err := p.preflight(stats, len(mapped)); err != nil {
		return best, bestScore, false, err
	}
	if p.ordinals == nil {
		newBest, newBestScore, changed, err := p.base.scoreGreedyBestRowIDsPrevalidated(mapped, best, bestScore, scratch, stats)
		if err != nil {
			return best, bestScore, false, err
		}
		return newBest, newBestScore, changed, nil
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(mapped))
	scores, err := p.base.scoreRowIDsPrevalidated(mapped, scratch.scoreTileScores, scratch, stats)
	if err != nil {
		return best, bestScore, false, err
	}
	changed := false
	for i, rowID := range rowIDs {
		ordinal := int(rowID)
		score := scores[i]
		if score > bestScore || (score == bestScore && ordinal < best) {
			best, bestScore, changed = ordinal, score, true
		}
	}
	return best, bestScore, changed, nil
}

func (p *typedGraphScalarU8ScorePlane) scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	mapped, err := p.mapRowIDs(rowIDs, scratch)
	if err != nil {
		return 0, err
	}
	if err := p.preflight(stats, len(mapped)); err != nil {
		return 0, err
	}
	if p.ordinals == nil {
		n, err := p.base.scoreAndPushFrontierVisitedRowIDsPrevalidated(mapped, topK, scratch, stats)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(mapped))
	scores, err := p.base.scoreRowIDsPrevalidated(mapped, scratch.scoreTileScores, scratch, stats)
	if err != nil {
		return 0, err
	}
	for i, rowID := range rowIDs {
		candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: scores[i]}
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontierAccounting(candidate, stats)
		}
	}
	return len(rowIDs), nil
}

func (p *typedGraphScalarU8ScorePlane) contextErr() error {
	if p == nil {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if p.ctx == nil {
		return nil
	}
	return p.ctx.Err()
}

func (p *typedGraphScalarU8ScorePlane) preflight(stats *columnVectorGraphNativeSearchStats, calls int) error {
	if calls < 0 {
		return errTypedGraphSearchBudget
	}
	if err := p.contextErr(); err != nil {
		return err
	}
	if calls == 0 {
		return nil
	}
	if stats == nil {
		// The strict allowance is deliberately counter-backed. A caller that
		// cannot provide the quantized work counter cannot be allowed to score
		// through this private bounded adapter.
		return errTypedGraphSearchBudget
	}
	used := stats.QuantizedScoreCalls
	if used > p.scoreLimit || uint64(calls) > p.scoreLimit-used {
		return errTypedGraphSearchBudget
	}
	return nil
}

func (p *typedGraphScalarU8ScorePlane) mapOrdinal(ordinal int) (uint32, error) {
	if p == nil || ordinal < 0 {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	if p.ordinals == nil {
		if ordinal >= p.baseRows {
			return 0, ErrVectorIndexSnapshotMismatch
		}
		return uint32(ordinal), nil
	}
	if ordinal >= len(p.ordinals) {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	rowID := p.ordinals[ordinal]
	if uint64(rowID) >= uint64(p.baseRows) {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	return rowID, nil
}

func (p *typedGraphScalarU8ScorePlane) mapOrdinals(ordinals []int, scratch *columnVectorGraphNativeSearchScratch) ([]uint32, error) {
	if scratch == nil {
		return nil, errColumnVectorGraphNativeSearchScratchRequired
	}
	if err := p.contextErr(); err != nil {
		return nil, err
	}
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(ordinals))
	rows := scratch.scoreTileRowIDs[:len(ordinals)]
	for i, ordinal := range ordinals {
		rowID, err := p.mapOrdinal(ordinal)
		if err != nil {
			return nil, err
		}
		rows[i] = rowID
	}
	if err := p.contextErr(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (p *typedGraphScalarU8ScorePlane) mapRowIDs(rowIDs []uint32, scratch *columnVectorGraphNativeSearchScratch) ([]uint32, error) {
	if scratch == nil {
		return nil, errColumnVectorGraphNativeSearchScratchRequired
	}
	if err := p.contextErr(); err != nil {
		return nil, err
	}
	if p.ordinals == nil {
		for _, rowID := range rowIDs {
			if uint64(rowID) >= uint64(p.baseRows) {
				return nil, ErrVectorIndexSnapshotMismatch
			}
		}
		return rowIDs, nil
	}
	scratch.filterNavigationRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.filterNavigationRowIDs, len(rowIDs))
	mapped := scratch.filterNavigationRowIDs[:len(rowIDs)]
	for i, rowID := range rowIDs {
		if uint64(rowID) >= uint64(len(p.ordinals)) {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		baseRowID := p.ordinals[rowID]
		if uint64(baseRowID) >= uint64(p.baseRows) {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		mapped[i] = baseRowID
	}
	if err := p.contextErr(); err != nil {
		return nil, err
	}
	return mapped, nil
}

func (p *typedGraphScalarU8ScorePlane) scoreMappedRowIDs(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if p == nil || p.base == nil || !p.base.ready {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if err := p.preflight(stats, len(rowIDs)); err != nil {
		return dst[:0], err
	}
	scores, err := p.base.scoreRowIDsPrevalidated(rowIDs, dst, scratch, stats)
	if err != nil {
		return dst[:0], err
	}
	return scores, nil
}

func (v *typedGraphOverlaySearch) searchScalarU8PreparedCandidatesWithContext(ctx context.Context, query []float32, opts typedGraphScalarU8TraversalOptions, scratch *columnVectorGraphNativeSearchScratch) (_ []columnVectorGraphNativeSearchResult, stats columnVectorGraphNativeSearchStats, err error) {
	if scratch == nil {
		return nil, stats, errColumnVectorGraphNativeSearchScratchRequired
	}
	// Both scratch fields below borrow request-scoped filter/navigation state.
	// Clear them on every return, including failures before this invocation has
	// installed its own state, so a caller-owned scratch neither retains an old
	// snapshot/context nor makes that state observable to a later request.
	defer func() {
		scratch.typedScalarU8Filter = columnHNSWPreparedTraversalFilter{}
		scratch.typedScalarU8Plane = typedGraphScalarU8ScorePlane{}
	}()
	completed := false
	defer func() {
		if !completed {
			scratch.results = scratch.results[:0]
			scratch.top = scratch.top[:0]
			scratch.frontier = scratch.frontier[:0]
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	if !v.validOpen() || v.pack == nil || v.base.reader == nil {
		return nil, stats, ErrVectorIndexSnapshotMismatch
	}
	if opts.TopK < 0 {
		return nil, stats, errColumnVectorGraphNativeSearchTopKNegative
	}
	if opts.EfSearch < 0 || opts.ScoreBudget < 0 {
		return nil, stats, errTypedGraphSearchBudget
	}
	basePack := v.pack
	baseRows := basePack.Header.Rows
	if baseRows < 0 || basePack.Header.Dimensions != v.base.reader.def.Dimensions {
		return nil, stats, ErrVectorIndexSnapshotMismatch
	}
	plan := opts.PreparedFilter
	var candidateRows typedcolumn.RowSelection
	hasCandidateRows := false
	var excludedBaseRows []int
	eligibleCount := baseRows
	if plan != nil {
		if !plan.validFor(v) {
			return nil, stats, ErrVectorIndexSnapshotMismatch
		}
		candidateRows = plan.base
		hasCandidateRows = true
		excludedBaseRows = plan.excludedBase
		var err error
		eligibleCount, err = typedGraphScalarU8EligibleCount(baseRows, candidateRows, true, excludedBaseRows)
		if err != nil {
			return nil, stats, err
		}
	}
	// This is a private candidate-collection seam. It reports scalar asset and
	// work facts, but deliberately does not claim either public quantized route:
	// Q2 owns the eventual requested/effective query mode and rerank evidence.
	setupStats := columnVectorGraphNativeSearchStats{}
	reader := v.base.reader
	reader.populateQuantizedAssetSearchStats(opts.QuantizedIndexName, &setupStats)
	if reader.RowCount() != baseRows {
		return nil, setupStats, ErrVectorIndexSnapshotMismatch
	}
	qdef, qdefOK := findQuantizedVectorIndex(reader.def, opts.QuantizedIndexName)
	if !qdefOK || qdef.Codec != QuantizedVectorCodecScalarU8 || qdef.Version != 1 || !scalarU8CalibrationIsLegacy(qdef) {
		return nil, setupStats, fmt.Errorf("%w: %w: typed scalar_u8 candidate collection requires legacy scalar_u8 v1 index %q", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, opts.QuantizedIndexName)
	}
	if len(query) != reader.def.Dimensions {
		return nil, setupStats, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, setupStats, err
	}
	// Validate and encode the selected code plane once even for an empty typed
	// domain so a stale/corrupt requested asset cannot be hidden by a shortcut.
	scratch.preparedScalarU8Plane.ready = false
	scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, opts.QuantizedIndexName, query, queryInvNorm, scratch)
	if err != nil {
		recordColumnVectorGraphQuantizedAssetErrorStats(&setupStats, err)
		return nil, setupStats, err
	}
	scratch.preparedScalarU8Plane.scorer = scorer
	scratch.preparedScalarU8Plane.ready = true
	setupStats.QuantizedScorerActive = 1
	if reader.preparedSearch != nil && reader.preparedSearch.ready() {
		setupStats.PreparedGraphSearchViews = 1
	}
	if opts.TopK == 0 || eligibleCount == 0 {
		return nil, setupStats, nil
	}
	if opts.ScoreBudget == 0 {
		return nil, setupStats, errTypedGraphSearchBudget
	}

	traversalPack := basePack
	var filter *columnHNSWPreparedTraversalFilter
	if hasCandidateRows || len(excludedBaseRows) != 0 {
		filter = &scratch.typedScalarU8Filter
		*filter = columnHNSWPreparedTraversalFilter{
			candidateRows:        candidateRows,
			hasCandidateRows:     hasCandidateRows,
			excludedBaseOrdinals: excludedBaseRows,
			baseRows:             baseRows,
			eligibleCount:        eligibleCount,
			eligibleCountSet:     true,
		}
	}
	plane := &scratch.typedScalarU8Plane
	*plane = typedGraphScalarU8ScorePlane{ctx: ctx, base: &scratch.preparedScalarU8Plane, baseRows: baseRows, scoreLimit: uint64(opts.ScoreBudget)}
	var navigation *typedGraphFilterNavigation
	if plan != nil {
		baseFilter := plan.borrowedBaseFilter
		if baseFilter != nil && baseFilter.navigation != nil {
			navigation = baseFilter.navigation
			// A bound plan is a fresh wrapper for every request. Its detached cached
			// origin, rather than wrapper identity, proves that the cached local map
			// was built from this exact immutable base selection. The base pack pin
			// prevents a same-shape graph/code plane from another snapshot slipping
			// through this private seam.
			if plan.cachedBasePlan == nil || plan.cachedBasePlan != baseFilter.plan || baseFilter.plan == nil || baseFilter.plan.overlay != nil || navigation.basePack != basePack || len(navigation.baseOrdinals) != baseFilter.plan.base.Count() || navigation.view.Header.Rows != len(navigation.baseOrdinals) || navigation.view.Header.Dimensions != basePack.Header.Dimensions {
				return nil, setupStats, ErrVectorIndexSnapshotMismatch
			}
			traversalPack = &navigation.view
			filter = &scratch.typedScalarU8Filter
			*filter = columnHNSWPreparedTraversalFilter{
				excludedBaseOrdinals: excludedBaseRows,
				localToBase:          navigation.baseOrdinals,
				baseRows:             baseRows,
				eligibleCount:        eligibleCount,
				eligibleCountSet:     true,
			}
			plane.ordinals = navigation.baseOrdinals
		}
	}

	traversalOpts := columnHNSWPreparedTraversalOptions{
		TopK:                      opts.TopK,
		EfSearch:                  opts.EfSearch,
		ScoreBatchMode:            opts.ScoreBatchMode,
		StatsMode:                 opts.StatsMode,
		OmitResultMaterialization: true,
		// Candidate collection consumes scratch.top directly so neither the base
		// nor navigation form allocates or appends an intermediate result slice.
		SuppressOmittedResultMaterialization: true,
		Context:                              ctx,
		Filter:                               filter,
	}
	_, traversalStats, err := traversalPack.searchCosinePreparedScorePlane(query, traversalOpts, scratch, plane)
	columnVectorGraphApplyQuantizedPreparedTraversalSetupStats(&traversalStats, setupStats)
	reader.populateScalarU8PreparedTraversalSearchStats(&traversalStats)
	stats = traversalStats
	if err != nil {
		return nil, stats, err
	}
	// A positive traversal allowance is strict: finishing exactly on the last
	// permitted scalar-u8 score is exhaustion, not a successful shortlist. The
	// preflight above still permits that final attempted invocation so work
	// counters stay truthful; this terminal check keeps callers from treating a
	// budget-bound prefix as a complete candidate collection.
	if stats.QuantizedScoreCalls >= uint64(opts.ScoreBudget) {
		return nil, stats, errTypedGraphSearchBudget
	}
	resultTopK := min(opts.TopK, eligibleCount)
	scratch.retainTopBestFirst(resultTopK)
	scratch.results = scratch.results[:0]
	for i, candidate := range scratch.top {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, err
			}
		}
		if !filter.admits(candidate.ordinal) {
			return nil, stats, ErrVectorIndexSnapshotMismatch
		}
		ordinal := candidate.ordinal
		if navigation != nil {
			if candidate.ordinal < 0 || candidate.ordinal >= len(plane.ordinals) {
				return nil, stats, ErrVectorIndexSnapshotMismatch
			}
			ordinal = int(plane.ordinals[candidate.ordinal])
		}
		if ordinal < 0 || ordinal >= baseRows {
			return nil, stats, ErrVectorIndexSnapshotMismatch
		}
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{Ordinal: ordinal, Score: candidate.score})
	}
	completed = true
	return scratch.results, stats, nil
}

func typedGraphScalarU8EligibleCount(rows int, selection typedcolumn.RowSelection, hasSelection bool, excluded []int) (int, error) {
	if rows < 0 {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	count := rows
	if hasSelection {
		if selection.Rows() != rows {
			return 0, ErrVectorIndexSnapshotMismatch
		}
		count = selection.Count()
	}
	for i, ordinal := range excluded {
		if ordinal < 0 || ordinal >= rows || (i > 0 && ordinal <= excluded[i-1]) {
			return 0, ErrVectorIndexSnapshotMismatch
		}
		if !hasSelection || selection.Contains(ordinal) {
			count--
		}
	}
	if count < 0 {
		return 0, fmt.Errorf("collections: typed scalar_u8 eligible count underflow")
	}
	return count, nil
}
