package collections

import (
	"bytes"
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
	deltaNodes               []vectorIndexNode
	scalarColumns            map[string]vectorIndexScalarColumn
	deltaScalarColumns       map[string]vectorIndexScalarColumn
	currentNode              map[string]int
	deltaCurrentNode         map[string]int
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
	deltaEntry               int
	deltaMaxLevel            int
	liveDocs                 int
	deltaLiveDocs            int
	sourceDocumentRootsValid bool
	rebuildDeletedRatio      float64
	persisted                atomic.Pointer[vectorIndexSearchPersistedMetadata]
	fullRebuilds             uint64
	mutationSeq              uint64
	sourceDocumentGeneration uint64
}

type vectorIndexSearchPersistedMetadata struct {
	epoch     uint64
	bytesDisk int64
}

type vectorIndexNativeSearchState struct {
	liveDocs                 int
	rebuildNeeded            bool
	fullRebuilds             uint64
	mutationSeq              uint64
	sourceDocumentGeneration uint64
}

type vectorIndexNativeSearchWork struct {
	queryPreparations      int
	baseVisited            int
	deltaPasses            int
	deltaRetries           int
	deltaResumes           int
	deltaVisited           int
	deltaInitialTopK       int
	deltaTerminalTopK      int
	deltaInitialEfSearch   int
	deltaTerminalEfSearch  int
	retryChangedMergedTopK bool
}

func (idx *VectorIndex) publishSearchView() {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.publishSearchViewLocked(false)
	idx.mu.Unlock()
}

func (idx *VectorIndex) publishSearchViewLocked(forceFull bool) {
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
	next.reuseState.Store(vectorIndexSearchViewActive)
	var previousNodes, previousDeltaNodes []vectorIndexNode
	if previous != nil {
		previousNodes = previous.nodes
		previousDeltaNodes = previous.deltaNodes
	}
	var nodes []vectorIndexNode
	if previous != nil && !forceFull && len(idx.searchViewDirty) == 0 && len(previousNodes) == len(idx.nodes) {
		nodes = previousNodes
	} else {
		nodes = copyVectorIndexSearchNodes(nil, previousNodes, idx.nodes, idx.searchViewDirty, forceFull)
	}
	var deltaNodes []vectorIndexNode
	deltaEntry, deltaMaxLevel, deltaLiveDocs := -1, -1, 0
	if idx.liveDelta != nil {
		deltaNodes = copyVectorIndexSearchNodes(next.deltaNodes, previousDeltaNodes, idx.liveDelta.nodes, idx.liveDelta.searchViewDirty, forceFull)
		deltaEntry = idx.liveDelta.entry
		deltaMaxLevel = idx.liveDelta.maxLevel
		deltaLiveDocs = len(idx.liveDelta.currentNode)
	} else {
		deltaNodes = copyVectorIndexSearchNodes(next.deltaNodes, previousDeltaNodes, nil, nil, true)
	}
	epoch := idx.persistedEpoch
	if epoch == 0 {
		epoch = idx.fullSnapshotBaseEpoch
	}
	next.nodes = nodes
	next.deltaNodes = deltaNodes
	next.scalarColumns = nil
	if idx.validateNativeScalarColumnLengthsLocked() == nil {
		if previous != nil && len(previousNodes) == len(idx.nodes) {
			next.scalarColumns = previous.scalarColumns
		} else {
			next.scalarColumns = cloneVectorIndexScalarColumns(idx.scalarColumns)
		}
	}
	next.deltaScalarColumns = nil
	if idx.liveDelta != nil && idx.liveDelta.validateNativeScalarColumnLengthsLocked() == nil {
		if previous != nil && len(previousDeltaNodes) == len(idx.liveDelta.nodes) {
			next.deltaScalarColumns = previous.deltaScalarColumns
		} else {
			next.deltaScalarColumns = cloneVectorIndexScalarColumns(idx.liveDelta.scalarColumns)
		}
	}
	if previous != nil && len(previousNodes) == len(idx.nodes) {
		next.currentNode = previous.currentNode
	} else {
		next.currentNode = vectorIndexNodeOrdinalMap(idx.nodes)
	}
	next.deltaCurrentNode = nil
	if idx.liveDelta != nil {
		if previous != nil && len(previousDeltaNodes) == len(idx.liveDelta.nodes) {
			next.deltaCurrentNode = previous.deltaCurrentNode
		} else {
			next.deltaCurrentNode = vectorIndexNodeOrdinalMap(idx.liveDelta.nodes)
		}
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
	next.deltaEntry = deltaEntry
	next.deltaMaxLevel = deltaMaxLevel
	next.liveDocs = len(idx.currentNode) + deltaLiveDocs
	next.deltaLiveDocs = deltaLiveDocs
	next.sourceDocumentRootsValid = idx.sourceDocumentRootsValid
	next.rebuildDeletedRatio = idx.rebuildDeletedRatio
	next.persisted.Store(&vectorIndexSearchPersistedMetadata{epoch: epoch, bytesDisk: idx.persistedBytesDisk})
	next.fullRebuilds = idx.liveANNFullRebuilds
	next.mutationSeq = idx.mutationSeq
	next.sourceDocumentGeneration = idx.sourceDocumentGeneration
	next.mu.Unlock()
	idx.searchView.Store(next)
	if previous != nil {
		if len(previous.nodes) > len(next.nodes) || len(previous.deltaNodes) > len(next.deltaNodes) {
			previous.reuseState.Store(vectorIndexSearchViewDiscarded)
		} else {
			previous.reuseState.Store(vectorIndexSearchViewRetired)
			idx.recycleSearchView(previous)
		}
	}
	clear(idx.searchViewDirty)
	if idx.liveDelta != nil {
		clear(idx.liveDelta.searchViewDirty)
	}
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

func (idx *VectorIndex) releaseSearchView(view *vectorIndexSearchView) {
	view.mu.RUnlock()
	idx.recycleSearchView(view)
}

func (idx *VectorIndex) recycleSearchView(view *vectorIndexSearchView) {
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

func (idx *VectorIndex) acquireSearchView() *vectorIndexSearchView {
	for idx != nil {
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
	return nil
}

func (view *vectorIndexSearchView) matchesDefinition(def VectorIndexDefinition) bool {
	return view != nil && view.sourceDocumentRootsValid &&
		view.name == def.Name && view.field == def.Field && view.metric == def.Metric &&
		view.encoding == def.Encoding && view.dimensions == def.Dimensions &&
		view.m == def.M && view.efConstruction == def.EfConstruction &&
		view.efSearch == def.EfSearch && view.schemaGeneration == def.SchemaGeneration
}

func (idx *VectorIndex) publishedNativeSearchLoadStatus(def VectorIndexDefinition) (VectorIndexLoadStatus, bool) {
	if idx == nil {
		return VectorIndexLoadStatus{}, false
	}
	view := idx.acquireSearchView()
	if !view.matchesDefinition(def) {
		if view != nil {
			idx.releaseSearchView(view)
		}
		return VectorIndexLoadStatus{}, false
	}
	persisted := view.persisted.Load()
	if persisted == nil {
		idx.releaseSearchView(view)
		return VectorIndexLoadStatus{}, false
	}
	status := VectorIndexLoadStatus{
		Loaded:    true,
		RootName:  idx.nativeRootName,
		RootID:    persisted.epoch,
		Epoch:     persisted.epoch,
		BytesDisk: persisted.bytesDisk,
	}
	idx.releaseSearchView(view)
	return status, true
}

func (view *vectorIndexSearchView) nativeSearchState() vectorIndexNativeSearchState {
	nodes := len(view.nodes) + len(view.deltaNodes)
	deletedDocs := nodes - view.liveDocs
	return vectorIndexNativeSearchState{
		liveDocs:                 view.liveDocs,
		rebuildNeeded:            deletedDocs > 0 && nodes > 0 && float64(deletedDocs)/float64(nodes) >= view.rebuildDeletedRatio,
		fullRebuilds:             view.fullRebuilds,
		mutationSeq:              view.mutationSeq,
		sourceDocumentGeneration: view.sourceDocumentGeneration,
	}
}

func (idx *VectorIndex) nativeSearchStateCoversCurrentDocuments(state vectorIndexNativeSearchState) bool {
	if idx == nil || idx.collection == nil || idx.collection.nativeVectorIndexMutationActive() {
		return false
	}
	idx.mu.RLock()
	covers := idx.sourceDocumentRootsValid && idx.mutationSeq == state.mutationSeq &&
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
	if view == nil {
		return nil, errors.New("collections: vector index search view is unavailable")
	}
	if !view.sourceDocumentRootsValid {
		return nil, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, view.name)
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
	if len(view.deltaNodes) == 0 {
		results, err := searchVectorIndexViewPlane(query, queryNorm, prepared, topK, efSearch, view.nodes, view.entry, view.maxLevel, view.liveDocs, view, &buffer.nativeSearchScratch, &buffer.results, &buffer.idBytes)
		if buffer.nativeSearchWorkEnabled {
			buffer.nativeSearchWork.baseVisited = buffer.nativeSearchScratch.explored
		}
		return results, err
	}
	baseResults, err := searchVectorIndexViewPlane(query, queryNorm, prepared, topK, efSearch, view.nodes, view.entry, view.maxLevel, view.liveDocs-view.deltaLiveDocs, view, &buffer.nativeSearchScratch, &buffer.baseResults, &buffer.baseIDBytes)
	if err != nil {
		return nil, err
	}
	if buffer.nativeSearchWorkEnabled {
		buffer.nativeSearchWork.baseVisited = buffer.nativeSearchScratch.explored
	}
	deltaTopK := vectorIndexLiveDeltaSearchBudget(topK, view.deltaLiveDocs, view.liveDocs)
	deltaEfSearch := efSearch
	if deltaEfSearch <= 0 {
		deltaEfSearch = view.efSearch
	}
	deltaEfSearch = vectorIndexLiveDeltaSearchBudget(deltaEfSearch, view.deltaLiveDocs, view.liveDocs)
	if buffer.nativeSearchWorkEnabled {
		buffer.nativeSearchWork.deltaInitialTopK = deltaTopK
		buffer.nativeSearchWork.deltaInitialEfSearch = deltaEfSearch
	}
	buffer.nativeSearchScratch.startResumableSearch()
	defer buffer.nativeSearchScratch.stopResumableSearch()
	deltaResults, err := searchVectorIndexViewPlane(query, queryNorm, prepared, deltaTopK, deltaEfSearch, view.deltaNodes, view.deltaEntry, view.deltaMaxLevel, view.deltaLiveDocs, view, &buffer.nativeSearchScratch, &buffer.deltaResults, &buffer.deltaIDBytes)
	if err != nil {
		return nil, err
	}
	var initialMergedFingerprint uint64
	if buffer.nativeSearchWorkEnabled {
		buffer.nativeSearchWork.deltaPasses = 1
		buffer.nativeSearchWork.deltaVisited = buffer.nativeSearchScratch.explored
		initialMerged, mergeErr := mergeVectorIndexViewResults(baseResults, deltaResults, topK, buffer)
		if mergeErr != nil {
			return nil, mergeErr
		}
		initialMergedFingerprint = vectorIndexSearchResultsFingerprint(initialMerged)
	}
	for deltaTopK < topK && len(deltaResults) == deltaTopK && (len(baseResults) < topK || vectorIndexSearchResultBefore(deltaResults[len(deltaResults)-1], baseResults[len(baseResults)-1])) {
		buffer.nativeSearchScratch.resumeSearch()
		deltaTopK = minInt(topK, deltaTopK*2)
		deltaEfSearch = maxInt(deltaEfSearch, deltaTopK)
		deltaResults, err = searchVectorIndexViewPlane(query, queryNorm, prepared, deltaTopK, deltaEfSearch, view.deltaNodes, view.deltaEntry, view.deltaMaxLevel, view.deltaLiveDocs, view, &buffer.nativeSearchScratch, &buffer.deltaResults, &buffer.deltaIDBytes)
		if err != nil {
			return nil, err
		}
		if buffer.nativeSearchWorkEnabled {
			buffer.nativeSearchWork.deltaPasses++
			buffer.nativeSearchWork.deltaRetries++
			if buffer.nativeSearchScratch.resumed {
				buffer.nativeSearchWork.deltaResumes++
			}
			buffer.nativeSearchWork.deltaVisited += buffer.nativeSearchScratch.explored
		}
	}
	merged, err := mergeVectorIndexViewResults(baseResults, deltaResults, topK, buffer)
	if buffer.nativeSearchWorkEnabled {
		buffer.nativeSearchWork.deltaTerminalTopK = deltaTopK
		buffer.nativeSearchWork.deltaTerminalEfSearch = deltaEfSearch
		buffer.nativeSearchWork.retryChangedMergedTopK = buffer.nativeSearchWork.deltaRetries > 0 && initialMergedFingerprint != vectorIndexSearchResultsFingerprint(merged)
	}
	return merged, err
}

func vectorIndexSearchResultsFingerprint(results []VectorIndexSearchResult) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for _, result := range results {
		for _, value := range result.ID {
			hash = (hash ^ uint64(value)) * prime
		}
		hash = (hash ^ math.Float64bits(result.Score)) * prime
	}
	return hash
}

func vectorIndexLiveDeltaSearchBudget(requested, deltaDocs, totalDocs int) int {
	if requested <= 0 || deltaDocs <= 0 || totalDocs <= 0 {
		return requested
	}
	budget := (requested*deltaDocs + totalDocs - 1) / totalDocs
	budget = maxInt(budget, minInt(16, requested))
	return minInt(budget, requested)
}

func searchVectorIndexViewPlane(query []float32, queryNorm float64, prepared *preparedFloat32CosineQuery, topK, efSearch int, nodes []vectorIndexNode, entry, maxLevel, liveDocs int, view *vectorIndexSearchView, scratch *vectorIndexSearchScratch, results *[]VectorIndexSearchResult, idBytes *[]byte) ([]VectorIndexSearchResult, error) {
	runtimeIndex := VectorIndex{
		metric:     view.metric,
		encoding:   view.encoding,
		dimensions: view.dimensions,
		m:          view.m,
		efSearch:   view.efSearch,
		nodes:      nodes,
		entry:      entry,
		maxLevel:   maxLevel,
	}
	candidates, err := runtimeIndex.searchGraphOnlyCandidatesWithPreparedQueryLocked(query, queryNorm, prepared, topK, efSearch, liveDocs, scratch)
	if err != nil {
		return nil, err
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
		(*results)[resultIndex] = VectorIndexSearchResult{ID: id, Score: 1 - float64(candidate.distance)}
		resultIndex++
		idOffset = nextIDOffset
	}
	return *results, nil
}

func mergeVectorIndexViewResults(base, delta []VectorIndexSearchResult, topK int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	resultCount := minInt(topK, len(base)+len(delta))
	buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, resultCount)
	idByteCount := 0
	baseIndex, deltaIndex, resultIndex := 0, 0, 0
	for resultIndex < resultCount && (baseIndex < len(base) || deltaIndex < len(delta)) {
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
		nextIDOffset := idOffset + len(buffer.results[resultIndex].ID)
		id := buffer.idBytes[idOffset:nextIDOffset:nextIDOffset]
		copy(id, buffer.results[resultIndex].ID)
		buffer.results[resultIndex].ID = id
		idOffset = nextIDOffset
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
