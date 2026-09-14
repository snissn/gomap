package collections

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

// typedGraphQuantizedRerankPlan is the request-local arithmetic for Q2's
// selected legacy scalar-u8 route. Its widths intentionally describe the
// unshadowed base traversal domain separately from the eventual live rerank
// shortlist. In particular, candidateWidth is not a work-budget clamp.
type typedGraphQuantizedRerankPlan struct {
	baseDomain      int
	shadowAllowance int
	deltaEligible   int
	effectiveEF     int
	rerankCap       int
	candidateWidth  int
}

func newTypedGraphQuantizedRerankScorePlane(opts VectorIndexSearchOptions, q QuantizedVectorIndexDefinition) ColumnGraphScorePlaneWork {
	configHash, _ := scalarU8CalibrationConfigHashForAssetID(q)
	return ColumnGraphScorePlaneWork{
		Version:                   1,
		Available:                 true,
		RequestedMode:             opts.QueryMode,
		EffectiveMode:             VectorIndexQueryModeQuantizedRerank,
		Route:                     "quantized_rerank",
		QuantizedIndexName:        q.Name,
		QuantizedCodec:            q.Codec,
		QuantizedVersion:          uint16(q.Version),
		QuantizedConfigHash:       configHash,
		RequestedTopK:             uint64(opts.TopK),
		RequestedEFSearch:         uint64(opts.EfSearch),
		RequestedRerankCandidates: uint64(opts.QuantizedRerankCandidates),
	}
}

// normalizeTypedGraphQuantizedRerankPlan deliberately performs only request
// arithmetic. It does not look at the serving score budget while choosing E0,
// Rcap, or C: silently shrinking an explicit recall request would turn a
// capacity failure into a lower-recall success.
func normalizeTypedGraphQuantizedRerankPlan(opts VectorIndexSearchOptions, defaultEF, baseDomain, shadowAllowance, deltaEligible int) (typedGraphQuantizedRerankPlan, error) {
	var plan typedGraphQuantizedRerankPlan
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return plan, err
	}
	if opts.QuantizedRerankCandidates < 0 {
		return plan, errColumnVectorGraphNativeSearchQuantizedRerankLimit
	}
	if opts.QuantizedRerankCandidates != 0 && opts.TopK > 0 && opts.QuantizedRerankCandidates < opts.TopK {
		return plan, errColumnVectorGraphNativeSearchQuantizedRerankLimit
	}
	if defaultEF < 0 || baseDomain < 0 || shadowAllowance < 0 || deltaEligible < 0 {
		return plan, ErrVectorIndexSnapshotMismatch
	}

	efRequest := opts.EfSearch
	if efRequest == 0 {
		efRequest = defaultEF
	}
	plan.baseDomain, plan.shadowAllowance, plan.deltaEligible = baseDomain, shadowAllowance, deltaEligible
	plan.effectiveEF = min(baseDomain, max(opts.TopK, efRequest))
	if opts.QuantizedRerankCandidates == 0 {
		plan.rerankCap = plan.effectiveEF
	} else {
		plan.rerankCap = min(baseDomain, plan.effectiveEF, opts.QuantizedRerankCandidates)
	}
	// Compute min(A, E0+S) without allowing caller-controlled integers to
	// overflow before the cap. This is also the only width Q1 is permitted to
	// allocate/retain for this request.
	if plan.shadowAllowance >= baseDomain-plan.effectiveEF {
		plan.candidateWidth = baseDomain
	} else {
		plan.candidateWidth = plan.effectiveEF + plan.shadowAllowance
	}
	return plan, nil
}

func typedGraphQuantizedRerankLegacyScalarU8Definition(reader *columnVectorGraphPhysicalRowReader, name string) (QuantizedVectorIndexDefinition, error) {
	if reader == nil {
		return QuantizedVectorIndexDefinition{}, ErrVectorIndexSnapshotMismatch
	}
	q, ok := findQuantizedVectorIndex(reader.def, name)
	if !ok {
		return QuantizedVectorIndexDefinition{}, fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, reader.def.Name, name)
	}
	if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 || !scalarU8CalibrationIsLegacy(q) {
		return QuantizedVectorIndexDefinition{}, fmt.Errorf("%w: typed graph quantized rerank requires named legacy scalar_u8 v1 index %q", ErrHybridSearchUnsupported, name)
	}
	if reader.def.Metric != VectorMetricCosine {
		// Q1's scalar scorer is cosine-only, and Q2's authoritative stable
		// rerank is intentionally the same cosine score plane. Check this here
		// as well so a valid empty base cannot bypass Q1's nonempty validation.
		return QuantizedVectorIndexDefinition{}, fmt.Errorf("%w: typed graph quantized rerank requires cosine metric for scalar_u8 index %q", ErrVectorIndexSearchUnavailable, name)
	}
	return q, nil
}

func typedGraphQuantizedRerankDeltaCount(v *typedGraphOverlaySearch, filter *typedGraphPreparedFilter) (int, error) {
	if v == nil {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	if filter == nil {
		count := 0
		for _, row := range v.rows {
			if !row.Deleted {
				count++
			}
		}
		return count, nil
	}
	previous := -1
	for _, ordinal := range filter.delta {
		if ordinal <= previous || ordinal < 0 || ordinal >= len(v.rows) || v.rows[ordinal].Deleted {
			return 0, ErrVectorIndexSnapshotMismatch
		}
		previous = ordinal
	}
	return len(filter.delta), nil
}

func typedGraphQuantizedRerankForEachDelta(ctx context.Context, v *typedGraphOverlaySearch, filter *typedGraphPreparedFilter, visit func(columnPhysicalVisibleRow) error) error {
	if v == nil || visit == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	if filter == nil {
		for ordinal, row := range v.rows {
			if ordinal&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if row.Deleted {
				continue
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	}
	for rank, ordinal := range filter.delta {
		if rank&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if ordinal < 0 || ordinal >= len(v.rows) || v.rows[ordinal].Deleted {
			return ErrVectorIndexSnapshotMismatch
		}
		if err := visit(v.rows[ordinal]); err != nil {
			return err
		}
	}
	return nil
}

// typedGraphStableCosineQueryInvNorm and typedGraphStableCosineScore are the
// selected Q2 score plane. They intentionally do not use the prepared pack's
// stored float32 inverse norms or its raw-dot scorer: final rerank ordering is
// defined by the authoritative typed float32 vectors with FP64 inverse norms.
func typedGraphStableCosineQueryInvNorm(query []float32) (float64, error) {
	if len(query) == 0 {
		return 0, errColumnVectorGraphInvNormEmpty
	}
	var normSquared float64
	for i, value := range query {
		asFloat64 := float64(value)
		if math.IsNaN(asFloat64) || math.IsInf(asFloat64, 0) {
			return 0, fmt.Errorf("query[%d] is not finite: %w", i, errColumnVectorGraphInvNormValueNotFinite)
		}
		normSquared += asFloat64 * asFloat64
	}
	if normSquared == 0 || math.IsNaN(normSquared) || math.IsInf(normSquared, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	return 1 / math.Sqrt(normSquared), nil
}

func typedGraphStableCosineScore(query []float32, queryInvNorm float64, vector []float32) (float64, error) {
	if len(query) != len(vector) {
		return 0, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	if queryInvNorm <= 0 || math.IsNaN(queryInvNorm) || math.IsInf(queryInvNorm, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	var normSquared float64
	for i, value := range vector {
		asFloat64 := float64(value)
		if math.IsNaN(asFloat64) || math.IsInf(asFloat64, 0) {
			return 0, fmt.Errorf("vector[%d] is not finite: %w", i, errColumnVectorGraphInvNormValueNotFinite)
		}
		normSquared += asFloat64 * asFloat64
	}
	if normSquared == 0 || math.IsNaN(normSquared) || math.IsInf(normSquared, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	distance := vectorops.CosineDistanceFloat32Normalized(query, vector, queryInvNorm, 1/math.Sqrt(normSquared))
	score := 1 - float64(distance)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	return score, nil
}

func typedGraphQuantizedRerankCompare(left, right VectorIndexSearchResult) int {
	if vectorIndexSearchResultBefore(left, right) {
		return -1
	}
	if vectorIndexSearchResultBefore(right, left) {
		return 1
	}
	return 0
}

func typedGraphQuantizedRerankAppendDelta(ctx context.Context, v *typedGraphOverlaySearch, filter *typedGraphPreparedFilter, query []float32, queryInvNorm float64, buffer *VectorIndexSearchBuffer, stats *typedGraphOverlaySearchStats, proof *ColumnGraphScorePlaneWork) error {
	return typedGraphQuantizedRerankForEachDelta(ctx, v, filter, func(row columnPhysicalVisibleRow) error {
		if row.Values == nil || v.vectorColumn < 0 || v.vectorColumn >= len(row.Values) {
			return ErrVectorIndexSnapshotMismatch
		}
		score, err := typedGraphStableCosineScore(query, queryInvNorm, row.Values[v.vectorColumn].Float32Vector)
		if err != nil {
			return err
		}
		stats.DeltaScored++
		proof.ExactSuffixScoreCalls++
		buffer.deltaResults = append(buffer.deltaResults, VectorIndexSearchResult{ID: row.ID, Score: score})
		return nil
	})
}

func typedGraphQuantizedRerankAppendExactBase(ctx context.Context, v *typedGraphOverlaySearch, ordinal int, query []float32, queryInvNorm float64, buffer *VectorIndexSearchBuffer, stats *typedGraphOverlaySearchStats, proof *ColumnGraphScorePlaneWork, smallFilter bool) error {
	if ordinal < 0 || ordinal >= v.pack.Header.Rows || v.base.reader.typedVectorSource == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	vector, _, _, ok := v.base.reader.typedVectorSource.vectorForOrdinal(ordinal)
	if !ok {
		return ErrVectorIndexSnapshotMismatch
	}
	id, ok := v.pack.documentIDForOrdinal(ordinal)
	if !ok {
		return ErrVectorIndexSnapshotMismatch
	}
	score, err := typedGraphStableCosineScore(query, queryInvNorm, vector)
	if err != nil {
		return err
	}
	stats.ExactBaseScored++
	stats.BaseResultIDs++
	if smallFilter {
		proof.ExactSmallFilterScoreCalls++
	} else {
		proof.ExactBaseRerankScoreCalls++
	}
	buffer.baseResults = append(buffer.baseResults, VectorIndexSearchResult{ID: id, Score: score})
	return nil
}

func (o *typedGraphReadOwner) validateTypedGraphQuantizedRerankAsset(ctx context.Context, query []float32, q QuantizedVectorIndexDefinition, filter *typedGraphPreparedFilter, buffer *VectorIndexSearchBuffer, stats *typedGraphOverlaySearchStats) error {
	v := o.overlay
	if v.base.reader.RowCount() == 0 {
		// The owner attach seam has already validated the declared scalar-u8 v1
		// identity. An empty base has no code rows and intentionally does not
		// fabricate a status merely to satisfy the Q1 collector.
		return nil
	}
	_, baseStats, err := v.searchScalarU8PreparedCandidatesWithContext(ctx, query, typedGraphScalarU8TraversalOptions{
		TopK:               0,
		EfSearch:           0,
		ScoreBudget:        0,
		QuantizedIndexName: q.Name,
		StatsMode:          columnVectorGraphNativeSearchStatsModeMinimal,
		PreparedFilter:     filter,
	}, &buffer.searchScratch)
	stats.Base = baseStats
	stats.Base.SearchRouteQuantizedRerank = 1
	return err
}

// searchScalarU8QuantizedRerankWithContext is Q2's only selected typed serving
// path. It deliberately reuses the captured owner, Q1 candidate collector,
// existing filter plan, raw typed-vector source, and merge routine; it does not
// open a searcher, reload a code plane, or create an ANN/index lifecycle.
func (o *typedGraphReadOwner) searchScalarU8QuantizedRerankWithContext(ctx context.Context, opts VectorIndexSearchOptions, filter *typedGraphPreparedFilter, candidateLimit int, buffer *VectorIndexSearchBuffer) (_ []VectorIndexSearchResult, stats typedGraphOverlaySearchStats, proof ColumnGraphScorePlaneWork, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer stats.recordWork()
	completed := false
	if buffer != nil {
		buffer.resetView()
		defer func() {
			if !completed {
				buffer.resetView()
			}
		}()
	}
	if o == nil || o.closed || o.overlay == nil || buffer == nil || !o.overlay.validOpen() {
		return nil, stats, proof, ErrVectorIndexSnapshotMismatch
	}
	v := o.overlay
	q, err := typedGraphQuantizedRerankLegacyScalarU8Definition(v.base.reader, opts.QuantizedIndexName)
	if err != nil {
		return nil, stats, proof, err
	}
	proof = newTypedGraphQuantizedRerankScorePlane(opts, q)
	if err := o.attachTypedGraphLegacyScalarU8QuantizedAsset(q.Name); err != nil {
		v.base.reader.populateQuantizedAssetSearchStats(q.Name, &stats.Base)
		return nil, stats, proof, err
	}
	stats.Base.SearchRouteQuantizedRerank = 1
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return nil, stats, proof, err
	}
	if len(opts.Query) != v.base.reader.def.Dimensions {
		return nil, stats, proof, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	queryInvNorm, err := typedGraphStableCosineQueryInvNorm(opts.Query)
	if err != nil {
		return nil, stats, proof, err
	}
	if filter != nil && !filter.validFor(v) {
		return nil, stats, proof, ErrVectorIndexSnapshotMismatch
	}
	switch v.pack.fastStatus("") {
	case columnHNSWSearchPackPreparedStatusDirect:
		stats.PackMmapDirect = true
	case columnHNSWSearchPackPreparedStatusHeap:
		stats.PackHeapCopy = true
	default:
		return nil, stats, proof, errColumnHNSWSearchPackSearchUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, proof, err
	}

	baseDomain := v.pack.Header.Rows
	if filter != nil {
		baseDomain = filter.base.Count()
	}
	shadowAllowance := len(v.rows)
	if filter != nil {
		shadowAllowance = len(filter.excludedBase)
	}
	deltaEligible, err := typedGraphQuantizedRerankDeltaCount(v, filter)
	if err != nil {
		return nil, stats, proof, err
	}
	if candidateLimit <= 0 {
		return nil, stats, proof, errTypedGraphSearchBudget
	}
	plan, err := normalizeTypedGraphQuantizedRerankPlan(opts, v.base.reader.def.EfSearch, baseDomain, shadowAllowance, deltaEligible)
	if err != nil {
		return nil, stats, proof, err
	}
	proof.NormalizedCandidateWidth = uint64(plan.effectiveEF)
	proof.RawCandidateWidth = uint64(plan.candidateWidth)
	proof.RerankCandidateCap = uint64(plan.rerankCap)

	// Q1 validates the selected code plane even when no traversal follows. This
	// catches a stale/corrupt nonempty base before any empty or exact shortcut.
	// An actually empty base is a separate, valid suffix-only contract handled
	// by the owner attach seam above.
	needsValidationOnly := v.base.reader.RowCount() > 0 && (opts.TopK == 0 || baseDomain == 0 || (filter != nil && filter.count <= typedGraphScalarExactLimit))
	if needsValidationOnly {
		if err := o.validateTypedGraphQuantizedRerankAsset(ctx, opts.Query, q, filter, buffer, &stats); err != nil {
			proof.QuantizedScoreCalls = stats.Base.QuantizedScoreCalls
			proof.QuantizedCodeBytesRead = stats.Base.QuantizedCodeBytesRead
			return nil, stats, proof, err
		}
		proof.QuantizedScoreCalls = stats.Base.QuantizedScoreCalls
		proof.QuantizedCodeBytesRead = stats.Base.QuantizedCodeBytesRead
	}

	if opts.TopK == 0 || (filter != nil && filter.count == 0) {
		stats.Route = "typed_empty"
		proof.Route = "typed_empty"
		completed = true
		return nil, stats, proof, nil
	}

	if filter != nil && filter.count <= typedGraphScalarExactLimit {
		stats.FilteredExact = true
		stats.Route = "typed_exact"
		proof.Route = "typed_exact"
		baseLive := 0
		for rank, ordinal := range filter.exactBaseByID {
			if rank&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats, proof, err
				}
			}
			if ordinal < 0 || ordinal >= v.pack.Header.Rows || !filter.base.Contains(ordinal) {
				return nil, stats, proof, ErrVectorIndexSnapshotMismatch
			}
			id, ok := v.pack.documentIDForOrdinal(ordinal)
			if !ok {
				return nil, stats, proof, ErrVectorIndexSnapshotMismatch
			}
			if filter.excludesBaseOrdinal(ordinal) || v.shadows(id) {
				// A bound cached plan normally has already accounted for both
				// conditions. Keep this defensive visibility check explicit rather
				// than letting an old base incarnation reappear on a raced fold.
				stats.BaseShadowed++
				continue
			}
			baseLive++
		}
		if deltaEligible > candidateLimit || baseLive > candidateLimit-deltaEligible {
			return nil, stats, proof, errTypedGraphSearchBudget
		}
		for rank, ordinal := range filter.exactBaseByID {
			if rank&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats, proof, err
				}
			}
			id, ok := v.pack.documentIDForOrdinal(ordinal)
			if !ok {
				return nil, stats, proof, ErrVectorIndexSnapshotMismatch
			}
			if filter.excludesBaseOrdinal(ordinal) || v.shadows(id) {
				continue
			}
			if err := typedGraphQuantizedRerankAppendExactBase(ctx, v, ordinal, opts.Query, queryInvNorm, buffer, &stats, &proof, true); err != nil {
				return nil, stats, proof, err
			}
		}
		if err := typedGraphQuantizedRerankAppendDelta(ctx, v, filter, opts.Query, queryInvNorm, buffer, &stats, &proof); err != nil {
			return nil, stats, proof, err
		}
		slices.SortFunc(buffer.baseResults, typedGraphQuantizedRerankCompare)
		slices.SortFunc(buffer.deltaResults, typedGraphQuantizedRerankCompare)
		results, err := mergeVectorIndexViewResultsWithContext(ctx, buffer.baseResults, buffer.deltaResults, opts.TopK, buffer)
		if err != nil {
			return nil, stats, proof, err
		}
		if len(results) < min(opts.TopK, filter.count) {
			return nil, stats, proof, errTypedGraphSearchBudget
		}
		completed = true
		return results, stats, proof, nil
	}

	if deltaEligible > candidateLimit {
		return nil, stats, proof, errTypedGraphSearchBudget
	}
	if baseDomain == 0 {
		stats.Route = "typed_exact"
		proof.Route = "typed_exact"
		if err := typedGraphQuantizedRerankAppendDelta(ctx, v, filter, opts.Query, queryInvNorm, buffer, &stats, &proof); err != nil {
			return nil, stats, proof, err
		}
		slices.SortFunc(buffer.deltaResults, typedGraphQuantizedRerankCompare)
		results, err := mergeVectorIndexViewResultsWithContext(ctx, nil, buffer.deltaResults, opts.TopK, buffer)
		if err != nil {
			return nil, stats, proof, err
		}
		completed = true
		return results, stats, proof, nil
	}
	if plan.rerankCap > candidateLimit-deltaEligible {
		return nil, stats, proof, errTypedGraphSearchBudget
	}
	baseScoreBudget := candidateLimit - deltaEligible - plan.rerankCap
	if plan.candidateWidth == 0 || baseScoreBudget <= plan.candidateWidth {
		// Q1 deliberately rejects a traversal that consumes its final permitted
		// scalar score. Reserve strict spare work before it allocates/scans so a
		// terminal prefix cannot be mistaken for a completed shortlist.
		return nil, stats, proof, errTypedGraphSearchBudget
	}

	traversalFilter := filter
	if filter != nil && len(filter.excludedBase) != 0 {
		// Q1 applies excludedBase during admission. Q2's C is intentionally raw
		// pre-shadow width, so borrow every other immutable filter component but
		// defer exclusions until after candidate collection. Never mutate the
		// shared request/keeper plan.
		cloned := *filter
		cloned.excludedBase = nil
		traversalFilter = &cloned
	}
	raw, baseStats, err := v.searchScalarU8PreparedCandidatesWithContext(ctx, opts.Query, typedGraphScalarU8TraversalOptions{
		TopK:               plan.candidateWidth,
		EfSearch:           plan.candidateWidth,
		ScoreBudget:        baseScoreBudget,
		QuantizedIndexName: q.Name,
		StatsMode:          columnVectorGraphNativeSearchStatsModeMinimal,
		PreparedFilter:     traversalFilter,
	}, &buffer.searchScratch)
	stats.Base = baseStats
	stats.Route = "typed_hnsw"
	proof.Route = "quantized_rerank"
	proof.QuantizedScoreCalls = baseStats.QuantizedScoreCalls
	proof.QuantizedCodeBytesRead = baseStats.QuantizedCodeBytesRead
	stats.Base.SearchRouteQuantizedRerank = 1
	if err != nil {
		return nil, stats, proof, err
	}
	if len(raw) > plan.candidateWidth {
		return nil, stats, proof, ErrVectorIndexSnapshotMismatch
	}
	proof.RawRetainedCandidates = uint64(len(raw))
	// Q1's returned Ordinal is always an immutable base ordinal. Do not inspect
	// scratch.top here: a cached filtered navigation view leaves that internal
	// heap in its local ordinal domain.
	survivors := resizeColumnVectorGraphNativeIntScratch(buffer.searchScratch.resultOrdinals, plan.effectiveEF)
	for rank, candidate := range raw {
		if rank&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, proof, err
			}
		}
		ordinal := candidate.Ordinal
		if ordinal < 0 || ordinal >= v.pack.Header.Rows {
			return nil, stats, proof, ErrVectorIndexSnapshotMismatch
		}
		if filter != nil && (!filter.base.Contains(ordinal) || filter.excludesBaseOrdinal(ordinal)) {
			stats.BaseShadowed++
			continue
		}
		id, ok := v.pack.documentIDForOrdinal(ordinal)
		if !ok {
			return nil, stats, proof, ErrVectorIndexSnapshotMismatch
		}
		if v.shadows(id) {
			stats.BaseShadowed++
			continue
		}
		if len(survivors) < plan.effectiveEF {
			survivors = append(survivors, ordinal)
		}
	}
	buffer.searchScratch.resultOrdinals = survivors
	proof.LiveShortlistCandidates = uint64(len(survivors))
	rerankCount := min(len(survivors), plan.rerankCap)
	for rank, ordinal := range survivors[:rerankCount] {
		if rank&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, proof, err
			}
		}
		if err := typedGraphQuantizedRerankAppendExactBase(ctx, v, ordinal, opts.Query, queryInvNorm, buffer, &stats, &proof, false); err != nil {
			return nil, stats, proof, err
		}
		// These counts describe completed canonical base reranks, not the
		// requested cap. Keeping them in the loop preserves a truthful prefix
		// if cancellation or a corrupt authoritative vector stops the query.
		proof.ActualRerankCandidates++
		stats.Base.QuantizedRerankCandidates++
		stats.Base.QuantizedRerankExactScoreCalls++
		stats.Base.FP32ScoreCalls++
	}
	if err := typedGraphQuantizedRerankAppendDelta(ctx, v, filter, opts.Query, queryInvNorm, buffer, &stats, &proof); err != nil {
		return nil, stats, proof, err
	}
	if stats.DeltaScored != deltaEligible {
		return nil, stats, proof, ErrVectorIndexSnapshotMismatch
	}
	slices.SortFunc(buffer.baseResults, typedGraphQuantizedRerankCompare)
	slices.SortFunc(buffer.deltaResults, typedGraphQuantizedRerankCompare)
	results, err := mergeVectorIndexViewResultsWithContext(ctx, buffer.baseResults, buffer.deltaResults, opts.TopK, buffer)
	if err != nil {
		return nil, stats, proof, err
	}
	if filter != nil && len(results) < min(opts.TopK, filter.count) {
		return nil, stats, proof, errTypedGraphSearchBudget
	}
	completed = true
	return results, stats, proof, nil
}
