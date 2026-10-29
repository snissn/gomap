package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	vectorIndexSearchViewActive uint32 = iota
	vectorIndexSearchViewRetired
	vectorIndexSearchViewSpare
	vectorIndexSearchViewDiscarded
)

// vectorIndexSearchView is the last fully reconciled immutable search
// generation. Node vectors and document IDs are immutable after insertion;
// only changed adjacency rows are copied when a new view is published.
type vectorIndexSearchView struct {
	mu                       sync.RWMutex
	reuseState               atomic.Uint32
	nodes                    []vectorIndexNode
	currentNode              map[string]int
	name                     string
	field                    string
	metric                   VectorMetric
	encoding                 VectorIndexEncoding
	dimensions               int
	m                        int
	efConstruction           int
	efSearch                 int
	schemaGeneration         uint64
	entry                    int
	maxLevel                 int
	liveDocs                 int
	sourceDocumentRootsValid bool
	persisted                atomic.Pointer[vectorIndexSearchPersistedMetadata]
	mutationSeq              uint64
	sourceDocumentGeneration uint64
}

type vectorIndexSearchPersistedMetadata struct {
	epoch     uint64
	bytesDisk int64
}

type vectorIndexNativeSearchState struct {
	mutationSeq              uint64
	sourceDocumentGeneration uint64
}

type vectorIndexNativeSearchWork struct {
	queryPreparations int
	scoreCalls        int
	baseVisited       int
}

func (idx *columnGraphPartitionLiveCarrier) publishSearchView() {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.publishSearchViewLocked(false)
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) acknowledgeSearchViewStateLocked() {
	if !idx.sourceDocumentRootsValid || len(idx.unnotifiedDocumentIDs) != 0 {
		idx.searchViewAcknowledged = false
		return
	}
	idx.searchViewAcknowledged = true
	idx.searchViewAcknowledgedMutationSeq = idx.mutationSeq
	idx.searchViewAcknowledgedGeneration = idx.sourceDocumentGeneration
}

func (idx *columnGraphPartitionLiveCarrier) publishSearchViewLocked(forceFull bool) {
	forceFull = forceFull || idx.searchViewForceFull
	previous := idx.searchView.Load()
	next := idx.searchViewSpare.Swap(nil)
	if next != nil && !next.mu.TryLock() {
		if next.reuseState.CompareAndSwap(vectorIndexSearchViewSpare, vectorIndexSearchViewRetired) {
			idx.recycleSearchView(next)
		}
		next = nil
	}
	if next == nil {
		next = &vectorIndexSearchView{}
		next.mu.Lock()
	}
	// A spare can alias the previous immutable image. Drop its headers before
	// cloning; never clear shared nodes or their neighbor backings in place.

	next.reuseState.Store(vectorIndexSearchViewActive)
	var previousNodes []vectorIndexNode
	if previous != nil {
		previousNodes = previous.nodes
	}
	var nodes []vectorIndexNode
	if previous != nil && !forceFull && len(idx.searchViewDirty) == 0 && len(previousNodes) == len(idx.nodes) {
		nodes = previousNodes
	} else {
		nodes = copyVectorIndexSearchNodes(nil, previousNodes, idx.nodes, idx.searchViewDirty, forceFull)
	}
	epoch := idx.persistedEpoch
	if epoch == 0 {
		epoch = idx.fullSnapshotBaseEpoch
	}
	next.nodes = nodes
	if previous != nil && !forceFull && len(previousNodes) == len(idx.nodes) {
		next.currentNode = previous.currentNode
	} else {
		next.currentNode = vectorIndexNodeOrdinalMap(idx.nodes)
	}
	next.name = idx.name
	next.field = idx.field
	next.metric = idx.metric
	next.encoding = idx.encoding
	next.dimensions = idx.dimensions
	next.m = idx.m
	next.efConstruction = idx.efConstruction
	next.efSearch = idx.efSearch
	next.schemaGeneration = idx.schemaGeneration
	next.entry = idx.entry
	next.maxLevel = idx.maxLevel
	next.liveDocs = len(idx.currentNode)
	next.sourceDocumentRootsValid = idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0
	next.persisted.Store(&vectorIndexSearchPersistedMetadata{epoch: epoch, bytesDisk: idx.persistedBytesDisk})
	next.mutationSeq = idx.mutationSeq
	next.sourceDocumentGeneration = idx.sourceDocumentGeneration
	next.mu.Unlock()
	idx.searchView.Store(next)
	if previous != nil {
		if len(previous.nodes) > len(next.nodes) {
			previous.reuseState.Store(vectorIndexSearchViewDiscarded)
		} else {
			previous.reuseState.Store(vectorIndexSearchViewRetired)
			idx.recycleSearchView(previous)
		}
	}
	clear(idx.searchViewDirty)
	idx.searchViewForceFull = false
	idx.acknowledgeSearchViewStateLocked()
	idx.searchViewCurrent.Store(true)
}

func copyVectorIndexSearchNodes(dst, previous, current []vectorIndexNode, dirty map[int]struct{}, forceFull bool) []vectorIndexNode {
	if cap(dst) > 2*len(current) {
		dst = make([]vectorIndexNode, len(current))
	} else if cap(dst) < len(current) {
		capacity := maxInt(len(current), cap(dst)*2)
		dst = make([]vectorIndexNode, len(current), capacity)
	} else {
		if len(current) < len(dst) {
			clear(dst[len(current):])
		}
		dst = dst[:len(current)]
	}
	if !forceFull && len(previous) <= len(dst) {
		copy(dst, previous)
		for nodeID := len(previous); nodeID < len(dst); nodeID++ {
			dst[nodeID] = cloneVectorIndexSearchNode(current[nodeID])
		}
		for nodeID := range dirty {
			if nodeID >= 0 && nodeID < len(previous) {
				dst[nodeID] = cloneVectorIndexSearchNode(current[nodeID])
			}
		}
	} else {
		for nodeID := range current {
			dst[nodeID] = cloneVectorIndexSearchNode(current[nodeID])
		}
	}
	return dst
}

func (idx *columnGraphPartitionLiveCarrier) releaseSearchView(view *vectorIndexSearchView) {
	view.mu.RUnlock()
	idx.recycleSearchView(view)
}

func (idx *columnGraphPartitionLiveCarrier) recycleSearchView(view *vectorIndexSearchView) {
	if idx == nil || view == nil || view.reuseState.Load() != vectorIndexSearchViewRetired || !view.mu.TryLock() {
		return
	}
	if !view.reuseState.CompareAndSwap(vectorIndexSearchViewRetired, vectorIndexSearchViewSpare) {
		view.mu.Unlock()
		return
	}
	view.mu.Unlock()
	if !idx.searchViewSpare.CompareAndSwap(nil, view) {
		view.reuseState.Store(vectorIndexSearchViewDiscarded)
	}
}

func cloneVectorIndexSearchNode(node vectorIndexNode) vectorIndexNode {
	out := node
	out.neighbors = make([][]vectorIndexNeighbor, len(node.neighbors))
	for layer := range node.neighbors {
		out.neighbors[layer] = append([]vectorIndexNeighbor(nil), node.neighbors[layer]...)
	}
	return out
}

func (idx *columnGraphPartitionLiveCarrier) prepareSearchViewForMutationLocked() {
	if idx == nil || !idx.preserveAcknowledgedView.Load() || idx.searchViewCurrent.Load() {
		return
	}
	if idx.searchViewAcknowledged &&
		idx.searchViewAcknowledgedMutationSeq == idx.mutationSeq &&
		idx.searchViewAcknowledgedGeneration == idx.sourceDocumentGeneration {
		idx.publishSearchViewLocked(false)
	}
}

func (idx *columnGraphPartitionLiveCarrier) acquireSearchView() *vectorIndexSearchView {
	if idx == nil {
		return nil
	}
	for !idx.searchViewCurrent.Load() {
		idx.mu.Lock()
		if idx.searchViewCurrent.Load() {
			idx.mu.Unlock()
			break
		}
		if !idx.sourceDocumentRootsValid || len(idx.unnotifiedDocumentIDs) != 0 || !idx.searchViewAcknowledged {
			view := idx.searchView.Load()
			if view == nil {
				idx.mu.Unlock()
				return nil
			}
			if view.mu.TryRLock() {
				idx.mu.Unlock()
				return view
			}
			idx.mu.Unlock()
			runtime.Gosched()
			continue
		}
		if idx.searchViewAcknowledgedMutationSeq == idx.mutationSeq &&
			idx.searchViewAcknowledgedGeneration == idx.sourceDocumentGeneration {
			idx.publishSearchViewLocked(false)
			idx.mu.Unlock()
			break
		}
		view := idx.searchView.Load()
		if view != nil && view.sourceDocumentRootsValid &&
			view.mutationSeq == idx.searchViewAcknowledgedMutationSeq &&
			view.sourceDocumentGeneration == idx.searchViewAcknowledgedGeneration &&
			view.mu.TryRLock() {
			idx.mu.Unlock()
			return view
		}
		idx.mu.Unlock()
		runtime.Gosched()
	}
	for {
		view := idx.searchView.Load()
		if view == nil {
			return nil
		}
		if !view.mu.TryRLock() {
			runtime.Gosched()
			continue
		}
		if idx.searchView.Load() == view {
			return view
		}
		idx.releaseSearchView(view)
	}
}

func (view *vectorIndexSearchView) matchesDefinition(def VectorIndexDefinition) bool {
	return view != nil && view.sourceDocumentRootsValid &&
		view.name == def.Name && view.field == def.Field && view.metric == def.Metric &&
		view.encoding == def.Encoding && view.dimensions == def.Dimensions &&
		view.m == def.M && view.efConstruction == def.EfConstruction &&
		view.efSearch == def.EfSearch && view.schemaGeneration == def.SchemaGeneration
}

func (idx *columnGraphPartitionLiveCarrier) nativeSearchStateCoversCurrentDocuments(state vectorIndexNativeSearchState) bool {
	if idx == nil || idx.collection == nil || idx.collection.nativeVectorIndexMutationActive() {
		return false
	}
	idx.mu.RLock()
	covers := idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0 && idx.mutationSeq == state.mutationSeq &&
		idx.sourceDocumentGeneration == state.sourceDocumentGeneration
	idx.mu.RUnlock()
	return covers
}

func (c *Collection) nativeVectorIndexMutationActive() bool {
	if c == nil || c.writeDomain == nil {
		return false
	}
	return c.writeDomain.nativeVectorSearchActive.Load()
}

func (view *vectorIndexSearchView) searchGraphOnlyWithBuffer(query []float32, topK, efSearch int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	return view.searchGraphOnlyWithScoreBudget(query, topK, efSearch, 0, buffer)
}

func (view *vectorIndexSearchView) searchGraphOnlyWithScoreBudget(query []float32, topK, efSearch, maxScoreCalls int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	return view.searchGraphOnlyWithCanonicalScoreBudget(query, topK, efSearch, maxScoreCalls, nil, buffer)
}

func (view *vectorIndexSearchView) searchGraphOnlyWithCanonicalScoreBudget(query []float32, topK, efSearch, maxScoreCalls int, canonical *CanonicalVectorPartitionCosineScorerV1, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	if view == nil {
		return nil, errors.New("collections: vector index search view is unavailable")
	}
	if maxScoreCalls < 0 {
		return nil, errors.New("collections: vector index search score budget must not be negative")
	}
	if !view.sourceDocumentRootsValid {
		return nil, fmt.Errorf("%w: ColumnGraph partition carrier %q does not cover current documents", ErrVectorIndexSearchUnavailable, view.name)
	}
	if topK < 0 {
		return nil, errors.New("collections: vector search TopK must be positive")
	}
	queryNorm, preparedQuery, preparedCosine, err := prepareVectorIndexGraphOnlyQuery(query, view.metric, view.dimensions)
	if err != nil {
		return nil, err
	}
	var prepared *preparedFloat32CosineQuery
	if preparedCosine {
		prepared = &preparedQuery
	}
	if buffer.nativeSearchWorkEnabled {
		buffer.nativeSearchWork = vectorIndexNativeSearchWork{}
		buffer.nativeSearchWork.queryPreparations = 1
	}
	buffer.nativeSearchScratch.startScoreTracking(maxScoreCalls)
	defer func() {
		if buffer.nativeSearchWorkEnabled {
			buffer.nativeSearchWork.scoreCalls = buffer.nativeSearchScratch.scoreCalls
		}
		buffer.nativeSearchScratch.stopScoreTracking()
	}()
	results, err := searchVectorIndexViewPlane(query, queryNorm, prepared, topK, efSearch, view.nodes, view.entry, view.maxLevel, view.liveDocs, view, canonical, &buffer.nativeSearchScratch, &buffer.results, &buffer.idBytes)
	if buffer.nativeSearchWorkEnabled {
		buffer.nativeSearchWork.baseVisited = buffer.nativeSearchScratch.explored
	}
	return results, err
}

func searchVectorIndexViewPlane(query []float32, queryNorm float64, prepared *preparedFloat32CosineQuery, topK, efSearch int, nodes []vectorIndexNode, entry, maxLevel, liveDocs int, view *vectorIndexSearchView, canonical *CanonicalVectorPartitionCosineScorerV1, scratch *vectorIndexSearchScratch, results *[]VectorIndexSearchResult, idBytes *[]byte) ([]VectorIndexSearchResult, error) {
	runtimeIndex := columnGraphPartitionLiveCarrier{columnGraphHNSWBuilder: &columnGraphHNSWBuilder{metric: view.metric,
		encoding:   view.encoding,
		dimensions: view.dimensions,
		m:          view.m,

		nodes:    nodes,
		entry:    entry,
		maxLevel: maxLevel}, efSearch: view.efSearch,
	}
	candidates, err := runtimeIndex.searchGraphOnlyCandidatesWithPreparedQueryLocked(query, queryNorm, prepared, topK, efSearch, liveDocs, scratch)
	if err != nil {
		return nil, err
	}
	if canonical != nil {
		// Rerank the bounded ANN candidates before the bounded domain TopK cut.
		// Negative scores preserve the exact FP32 bits; 1-score would round.
		live := candidates[:0]
		for i, candidate := range candidates {
			if i&255 == 0 {
				if err := scratch.finalContextErr(); err != nil {
					return nil, err
				}
			}
			if candidate.nodeID < 0 || candidate.nodeID >= len(nodes) {
				return nil, ErrVectorPartitionSearchUnavailable
			}
			if nodes[candidate.nodeID].deleted {
				continue
			}
			if scratch.scoreLimit > 0 && scratch.scoreCalls >= scratch.scoreLimit {
				scratch.scoreBudgetExceeded = true
				return nil, scratch.finalScoreBudgetErr()
			}
			node := &nodes[candidate.nodeID]
			// The pinned FP32 node caches the same binary64 norm used by the
			// canonical scorer; round its inverse to FP32 before normalizing.
			score, err := canonicalVectorPartitionScoreWithInvNormV1(canonical.normalizedQuery, node.vector, float32(node.cachedInvNorm))
			if err != nil {
				return nil, err
			}
			scratch.scoreCalls++
			candidate.distance = -score
			live = append(live, candidate)
		}
		candidates = live
		runtimeIndex.sortVectorIndexCandidatesByDistanceLocked(candidates)
		if err := scratch.finalContextErr(); err != nil {
			return nil, err
		}
	}
	resultCount := 0
	idByteCount := 0
	for _, candidate := range candidates {
		if resultCount >= topK {
			break
		}
		if candidate.nodeID < 0 || candidate.nodeID >= len(nodes) || nodes[candidate.nodeID].deleted {
			continue
		}
		var err error
		idByteCount, err = addVectorIndexSearchByteTotal(idByteCount, len(nodes[candidate.nodeID].documentID), math.MaxInt, "result id")
		if err != nil {
			return nil, err
		}
		resultCount++
	}
	*results = resizeVectorIndexSearchResultBuffer(*results, resultCount)
	*idBytes = resizeVectorIndexSearchByteBuffer(*idBytes, idByteCount)
	resultIndex, idOffset := 0, 0
	for _, candidate := range candidates {
		if resultIndex >= resultCount {
			break
		}
		node := nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		nextIDOffset := idOffset + len(node.documentID)
		id := (*idBytes)[idOffset:nextIDOffset:nextIDOffset]
		copy(id, node.documentID)
		score := 1 - float64(candidate.distance)
		if canonical != nil {
			score = -float64(candidate.distance)
		}
		(*results)[resultIndex] = VectorIndexSearchResult{ID: id, Score: score}
		resultIndex++
		idOffset = nextIDOffset
	}
	return *results, nil
}

func mergeVectorIndexViewResults(base, delta []VectorIndexSearchResult, topK int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	return mergeVectorIndexViewResultsWithContext(nil, base, delta, topK, buffer)
}

func mergeVectorIndexViewResultsWithContext(ctx context.Context, base, delta []VectorIndexSearchResult, topK int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	resultCount := minInt(topK, len(base)+len(delta))
	buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, resultCount)
	idByteCount := 0
	baseIndex, deltaIndex, resultIndex := 0, 0, 0
	for resultIndex < resultCount && (baseIndex < len(base) || deltaIndex < len(delta)) {
		if ctx != nil && (baseIndex+deltaIndex)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		candidate, nextBase, nextDelta := nextVectorIndexViewResult(base, delta, baseIndex, deltaIndex)
		baseIndex, deltaIndex = nextBase, nextDelta
		if vectorIndexViewResultAlreadySelected(buffer.results[:resultIndex], candidate.ID) {
			continue
		}
		var err error
		idByteCount, err = addVectorIndexSearchByteTotal(idByteCount, len(candidate.ID), math.MaxInt, "result id")
		if err != nil {
			return nil, err
		}
		buffer.results[resultIndex] = candidate
		resultIndex++
	}
	buffer.results = buffer.results[:resultIndex]
	buffer.idBytes = resizeVectorIndexSearchByteBuffer(buffer.idBytes, idByteCount)
	idOffset := 0
	for resultIndex := range buffer.results {
		if ctx != nil && resultIndex&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		nextIDOffset := idOffset + len(buffer.results[resultIndex].ID)
		id := buffer.idBytes[idOffset:nextIDOffset:nextIDOffset]
		copy(id, buffer.results[resultIndex].ID)
		buffer.results[resultIndex].ID = id
		idOffset = nextIDOffset
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return buffer.results, nil
}

func nextVectorIndexViewResult(base, delta []VectorIndexSearchResult, baseIndex, deltaIndex int) (VectorIndexSearchResult, int, int) {
	if deltaIndex >= len(delta) || baseIndex < len(base) && vectorIndexSearchResultBefore(base[baseIndex], delta[deltaIndex]) {
		return base[baseIndex], baseIndex + 1, deltaIndex
	}
	return delta[deltaIndex], baseIndex, deltaIndex + 1
}

func vectorIndexSearchResultBefore(left, right VectorIndexSearchResult) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	return bytes.Compare(left.ID, right.ID) < 0
}

func vectorIndexViewResultAlreadySelected(results []VectorIndexSearchResult, id []byte) bool {
	// ponytail: top-K is small; add reusable ID-set scratch only if merge profiles demand it.
	for resultIndex := range results {
		if bytes.Equal(results[resultIndex].ID, id) {
			return true
		}
	}
	return false
}

func vectorIndexNodeOrdinalMap(nodes []vectorIndexNode) map[string]int {
	if len(nodes) == 0 {
		return nil
	}
	out := make(map[string]int, len(nodes))
	for nodeID := range nodes {
		out[string(nodes[nodeID].documentID)] = nodeID
	}
	return out
}
