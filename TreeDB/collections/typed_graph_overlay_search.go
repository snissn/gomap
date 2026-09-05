package collections

import (
	"bytes"
	"errors"
	"slices"
	"sort"
)

// This view borrows its two pins; current is the logical query authority and
// base is solely an immutable accelerator. No collection route installs it.
type typedGraphOverlaySearch struct {
	base         *VectorIndexSearcher
	pack         *columnHNSWSearchPackPreparedView
	current      *CollectionReadView
	rows         []columnPhysicalVisibleRow
	invNorms     []float32
	vectorColumn int
}

type typedGraphOverlaySearchStats struct {
	Base                         columnVectorGraphNativeSearchStats
	DeltaScored                  int
	FilteredExact                bool
	ExactBaseScored              int
	BaseShadowed                 int
	BaseResultIDs                int
	PackMmapDirect, PackHeapCopy bool
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
	rows, err := suffix.prepareRows(current, limits.Bytes)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b columnPhysicalVisibleRow) int { return bytes.Compare(a.ID, b.ID) })
	view := &typedGraphOverlaySearch{base: base, pack: pack, current: current, rows: rows, vectorColumn: -1, invNorms: make([]float32, len(rows))}
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
		view.invNorms[i], err = columnVectorGraphInvNorm(vector)
		if err != nil {
			return nil, err
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
	var stats typedGraphOverlaySearchStats
	completed := false
	if buffer != nil {
		buffer.resetView()
		defer func() {
			if !completed {
				buffer.resetView()
			}
		}()
	}
	if v == nil || v.base == nil || v.base.closed || v.current == nil || v.current.closed || v.current.snapshot == nil || buffer == nil {
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
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, stats, err
	}
	if topK == 0 {
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
	baseResults, baseStats, err := v.pack.searchCosine(query, columnVectorGraphNativeSearchOptions{TopK: baseTopK, EfSearch: max(efSearch, baseTopK), CandidateLimit: baseLimit}, &buffer.searchScratch)
	stats.Base = baseStats
	stats.BaseResultIDs = len(baseResults)
	if err != nil {
		return nil, stats, err
	}
	// The pack currently exposes work counts, not a completion certificate.
	// Conservatively reject a reached cap below corpus size, even if K results
	// happened to be collected. Never silently return cap-truncated output.
	if baseLimit < v.pack.Header.Rows && baseStats.Candidates >= uint64(baseLimit) {
		return nil, stats, errTypedGraphSearchBudget
	}
	for _, result := range baseResults {
		if v.shadows(result.ID) {
			stats.BaseShadowed++
			continue
		}
		buffer.baseResults = append(buffer.baseResults, VectorIndexSearchResult{ID: result.ID, Score: result.Score})
	}
	for i, row := range v.rows {
		if row.Deleted {
			continue
		}
		score, err := columnVectorGraphNativeCosineScoreVector(query, queryInvNorm, i, row.Values[v.vectorColumn].Float32Vector, v.invNorms[i])
		if err != nil {
			return nil, stats, err
		}
		stats.DeltaScored++
		buffer.deltaResults = append(buffer.deltaResults, VectorIndexSearchResult{ID: row.ID, Score: score})
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
	slices.SortFunc(buffer.baseResults, compare)
	slices.SortFunc(buffer.deltaResults, compare)
	results, err := mergeVectorIndexViewResults(buffer.baseResults, buffer.deltaResults, topK, buffer)
	if err != nil {
		return nil, stats, err
	}
	completed = true
	return results, stats, nil
}
