package collections

import (
	"bytes"
	"errors"
	"math/bits"
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
var errTypedGraphFilteredANNRequired = errors.New("collections: typed graph filter requires ANN mapping")

const typedGraphScalarExactLimit = 4096

// searchScalarExact uses only the current pin's persisted postings and locator.
// Incomplete probes never become an exact route. Larger filters require the
// separate filtered ANN primitive; they cannot fall back to a corpus scan.
func (v *typedGraphOverlaySearch) searchScalarExact(query []float32, topK int, filter HybridScalarFilter, mappingWorkLimit int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	completed := false
	if buffer != nil {
		buffer.resetView()
		defer func() {
			if !completed {
				buffer.resetView()
			}
		}()
	}
	if v == nil || v.base == nil || v.base.closed || v.current == nil || v.current.closed || buffer == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if err := validateVectorIndexSearchRequest(topK, 0); err != nil {
		return nil, err
	}
	if len(query) != v.base.reader.def.Dimensions {
		return nil, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	queryNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, err
	}
	if err := validateHybridScalarFilter(filter); err != nil {
		return nil, err
	}
	leaves := filter.And
	if len(leaves) == 0 {
		leaves = []HybridScalarFilter{filter}
	}
	// Borrowed view: nil domain deliberately excludes newer mutable buffers.
	lookup := hybridScalarLookupView{snapshot: v.current.snapshot, catalog: v.current.catalog}
	var allowed hybridScalarAllowSet
	for _, leaf := range leaves {
		set, _, truncated, err := lookup.leafProbe(leaf, typedGraphScalarExactLimit+1)
		if err != nil {
			return nil, err
		}
		if truncated || len(set) > typedGraphScalarExactLimit {
			return nil, errTypedGraphFilteredANNRequired
		}
		if allowed == nil {
			allowed = set
		} else {
			for id := range allowed {
				if _, ok := set[id]; !ok {
					delete(allowed, id)
				}
			}
		}
	}
	inverse := v.base.reader.rowRefSource
	if !inverse.inversePermutationActive() {
		return nil, errTypedGraphInverseRequired
	}
	// Binary lookup is bounded independently of sparse physical row indexes.
	perID := bits.Len(uint(inverse.rows)) + 1
	if mappingWorkLimit <= 0 {
		return nil, errTypedGraphSearchBudget
	}
	if perID > 0 && len(allowed) > mappingWorkLimit/perID {
		return nil, errTypedGraphSearchBudget
	}
	ids := make([][]byte, 0, len(allowed))
	for id := range allowed {
		ids = append(ids, []byte(id))
	}
	refs, err := v.current.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		return nil, err
	}
	for _, result := range refs.Results {
		if !result.Found {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		vector, err := v.vectorForCurrentRow(result.RowRef)
		if err != nil {
			return nil, err
		}
		norm, err := columnVectorGraphInvNorm(vector)
		if err != nil {
			return nil, err
		}
		score, err := columnVectorGraphNativeCosineScoreVector(query, queryNorm, 0, vector, norm)
		if err != nil {
			return nil, err
		}
		buffer.baseResults = append(buffer.baseResults, VectorIndexSearchResult{ID: result.ID, Score: score})
	}
	sort.Slice(buffer.baseResults, func(i, j int) bool {
		return vectorIndexSearchResultBefore(buffer.baseResults[i], buffer.baseResults[j])
	})
	results, err := mergeVectorIndexViewResults(buffer.baseResults, nil, topK, buffer)
	if err != nil {
		return nil, err
	}
	completed = true
	return results, nil
}

func (v *typedGraphOverlaySearch) vectorForCurrentRow(ref DocumentRowRef) ([]float32, error) {
	i := sort.Search(len(v.rows), func(i int) bool { return bytes.Compare(v.rows[i].ID, ref.DocumentID) >= 0 })
	if i < len(v.rows) && bytes.Equal(v.rows[i].ID, ref.DocumentID) {
		row := v.rows[i]
		if row.Deleted || row.Generation != ref.Generation || row.PartID != ref.PartID || row.RowIndex != ref.RowIndex || row.AppliedCommandLSN != ref.AppliedCommandLSN {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		return row.Values[v.vectorColumn].Float32Vector, nil
	}
	ordinal, ok := v.base.reader.rowRefSource.ordinalForPhysicalRow(ref)
	if !ok {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	vector, _, _, ok := v.base.reader.typedVectorSource.vectorForOrdinal(ordinal)
	if !ok {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	return vector, nil
}

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
