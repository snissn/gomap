package collections

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
)

// This view borrows its two pins; current is the logical query authority and
// base is solely an immutable accelerator. Explicit serving admission installs
// this route without changing the current snapshot's logical authority.
type typedGraphOverlaySearch struct {
	lastMetadataGeneration       uint64
	base                         *VectorIndexSearcher
	pack                         *columnHNSWSearchPackPreparedView
	current                      *CollectionReadView
	rows                         []columnPhysicalVisibleRow
	invNorms                     []float32
	vectorColumn                 int
	sourceRows, sourceTombstones int
	sourceBytes                  int64
}

type typedGraphOverlaySearchStats struct {
	Route                        string
	Base                         columnVectorGraphNativeSearchStats
	DeltaScored                  int
	FilteredExact                bool
	ExactBaseScored              int
	BaseShadowed                 int
	BaseResultIDs                int
	PackMmapDirect, PackHeapCopy bool
}

func (v *typedGraphOverlaySearch) validOpen() bool {
	return v != nil && v.base != nil && !v.base.closed && v.base.reader != nil && v.current != nil && v.current.validateOpen() == nil
}

func prepareTypedGraphOverlaySearch(base *VectorIndexSearcher, current *CollectionReadView, limits typedGraphOverlayLimits) (*typedGraphOverlaySearch, error) {
	suffix, err := prepareTypedGraphOverlaySuffix(base, current, limits)
	if err != nil {
		return nil, err
	}
	pack, _, eligible := base.hnswSearchPackSearchWithBufferRoute(columnVectorGraphNativeSearchQueryModeExact, columnVectorGraphNativeSearchStatsModeMinimal)
	if !eligible {
		return nil, errColumnHNSWSearchPackSearchUnavailable
	}
	var lastMetadataGeneration uint64
	rows, err := suffix.prepareRowsWithAccounting(current, limits.Bytes, 0, nil, &lastMetadataGeneration)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b columnPhysicalVisibleRow) int { return bytes.Compare(a.ID, b.ID) })
	view := &typedGraphOverlaySearch{base: base, pack: pack, current: current, rows: rows, vectorColumn: -1, sourceRows: suffix.rows, sourceTombstones: suffix.tombstones, sourceBytes: suffix.bytes}
	view.lastMetadataGeneration = lastMetadataGeneration
	if !vectorIndexUsesCosineNormalizedF32V1(base.reader.def) {
		view.invNorms = make([]float32, len(rows))
	}
	for i, field := range suffix.view.FullConfig.Columns {
		if field.Path == base.reader.def.Field {
			view.vectorColumn = i
			break
		}
	}
	if view.vectorColumn < 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	for i, row := range rows {
		if row.Deleted {
			continue
		}
		vector := row.Values[view.vectorColumn].Float32Vector
		if len(vector) != base.reader.def.Dimensions {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		if vectorIndexUsesCosineNormalizedF32V1(base.reader.def) {
			if err := validateCosineNormalizedF32V1Canonical(vector, base.reader.def.Dimensions); err != nil {
				return nil, err
			}
		} else {
			view.invNorms[i], err = columnVectorGraphInvNorm(vector)
			if err != nil {
				return nil, err
			}
		}
	}
	return view, nil
}

func (v *typedGraphOverlaySearch) shadows(id []byte) bool {
	i := sort.Search(len(v.rows), func(i int) bool { return bytes.Compare(v.rows[i].ID, id) >= 0 })
	return i < len(v.rows) && bytes.Equal(v.rows[i].ID, id)
}

var errTypedGraphSearchBudget = errors.New("collections: typed graph search work budget exhausted")

const typedGraphScalarExactLimit = 4096

// search is an internal unfiltered slice. Bounded exact suffix work is counted
// separately from prepared-base ANN work. Filtering and public lifecycle
// installation are deliberately not inferred from this primitive.
func (v *typedGraphOverlaySearch) search(query []float32, topK, efSearch, candidateLimit int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, typedGraphOverlaySearchStats, error) {
	return v.searchWithContext(context.Background(), query, topK, efSearch, candidateLimit, buffer)
}

func (v *typedGraphOverlaySearch) searchWithContext(ctx context.Context, query []float32, topK, efSearch, candidateLimit int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, typedGraphOverlaySearchStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var stats typedGraphOverlaySearchStats
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
	if !v.validOpen() || buffer == nil {
		return nil, stats, ErrVectorIndexSnapshotMismatch
	}
	switch v.pack.fastStatus("") {
	case columnHNSWSearchPackPreparedStatusDirect:
		stats.PackMmapDirect = true
	case columnHNSWSearchPackPreparedStatusHeap:
		stats.PackHeapCopy = true
	default:
		return nil, stats, errColumnHNSWSearchPackSearchUnavailable
	}
	if err := validateVectorIndexSearchRequest(topK, efSearch); err != nil {
		return nil, stats, err
	}
	if candidateLimit <= len(v.rows) || topK > candidateLimit-len(v.rows) || efSearch > candidateLimit-len(v.rows) {
		return nil, stats, errTypedGraphSearchBudget
	}
	if len(query) != v.base.reader.def.Dimensions {
		return nil, stats, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	canonicalRepresentation := vectorIndexUsesCosineNormalizedF32V1(v.base.reader.def)
	scoreQuery := query
	queryInvNorm := float32(1)
	var err error
	if canonicalRepresentation {
		scoreQuery, err = buffer.normalizeCosineNormalizedF32V1Query(query, v.base.reader.def.Dimensions)
	} else {
		queryInvNorm, err = columnVectorGraphInvNorm(query)
	}
	if err != nil {
		return nil, stats, err
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	if topK == 0 {
		stats.Route = "typed_empty"
		return nil, stats, nil
	}
	// Retrieving K plus every possible shadow prevents filtering an already
	// truncated base top-K from underfilling. Shadows remain traversable graph
	// nodes; only result admission excludes them. No corpus visibility bitmap.
	baseLimit := candidateLimit - len(v.rows)
	baseTopK := topK + len(v.rows)
	if baseTopK > baseLimit {
		return nil, stats, errTypedGraphSearchBudget
	}
	if efSearch == 0 {
		efSearch = min(v.base.reader.def.EfSearch, baseLimit)
	}
	if v.pack.Header.Rows > 0 {
		stats.Route = "typed_hnsw"
	} else {
		stats.Route = "typed_empty"
	}
	baseResults, baseStats, err := v.pack.searchCosineWithContext(ctx, scoreQuery, columnVectorGraphNativeSearchOptions{TopK: baseTopK, EfSearch: max(efSearch, baseTopK), StrictScoreBudget: true, CandidateLimit: baseLimit, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics, CanonicalNormalizedQuery: canonicalRepresentation}, &buffer.searchScratch)
	stats.Base = baseStats
	stats.BaseResultIDs = len(baseResults)
	if err != nil {
		return nil, stats, err
	}
	// The pack rejects truncated traversal. Check the aggregate allowance in
	// actual score invocations, including upper repeats; suffix rows were
	// reserved from candidateLimit before the base call.
	if baseStats.PreparedScoreCalls > uint64(baseLimit) {
		return nil, stats, errTypedGraphSearchBudget
	}
	for i, result := range baseResults {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, err
			}
		}
		if v.shadows(result.ID) {
			stats.BaseShadowed++
			continue
		}
		buffer.baseResults = append(buffer.baseResults, VectorIndexSearchResult{ID: result.ID, Score: result.Score})
	}
	if canonicalRepresentation {
		var ignoredProof ColumnGraphScorePlaneWork
		if err := typedGraphCanonicalPackedAppendDelta(ctx, v, nil, scoreQuery, buffer, &stats, &ignoredProof); err != nil {
			return nil, stats, err
		}
		if stats.DeltaScored > 0 && stats.Route == "typed_empty" {
			stats.Route = "typed_exact"
		}
	} else {
		for i, row := range v.rows {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats, err
				}
			}
			if row.Deleted {
				continue
			}
			if stats.Route == "typed_empty" {
				stats.Route = "typed_exact"
			}
			score, err := columnVectorGraphNativeCosineScoreVector(scoreQuery, queryInvNorm, i, row.Values[v.vectorColumn].Float32Vector, v.invNorms[i])
			if err != nil {
				return nil, stats, err
			}
			stats.DeltaScored++
			buffer.deltaResults = append(buffer.deltaResults, VectorIndexSearchResult{ID: row.ID, Score: score})
		}
	}
	compare := func(a, b VectorIndexSearchResult) int {
		if vectorIndexSearchResultBefore(a, b) {
			return -1
		}
		if vectorIndexSearchResultBefore(b, a) {
			return 1
		}
		return 0
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	slices.SortFunc(buffer.baseResults, compare)
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	slices.SortFunc(buffer.deltaResults, compare)
	results, err := mergeVectorIndexViewResultsWithContext(ctx, buffer.baseResults, buffer.deltaResults, topK, buffer)
	if err != nil {
		return nil, stats, err
	}
	completed = true
	return results, stats, nil
}
