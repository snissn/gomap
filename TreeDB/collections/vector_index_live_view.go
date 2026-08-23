package collections

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
)

// vectorIndexSearchView is the last fully reconciled native graph. Node vectors
// and document IDs are immutable after insertion; only changed adjacency rows
// are copied when a new view is published.
type vectorIndexSearchView struct {
	mu                       sync.RWMutex
	nodes                    []vectorIndexNode
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
	rebuildDeletedRatio      float64
	epoch                    uint64
	bytesDisk                int64
	fullRebuilds             uint64
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
	next := idx.searchViewSpare
	if next == nil || !next.mu.TryLock() {
		next = &vectorIndexSearchView{}
		next.mu.Lock()
	}
	nodes := next.nodes
	if cap(nodes) < len(idx.nodes) {
		capacity := maxInt(len(idx.nodes), cap(nodes)*2)
		nodes = make([]vectorIndexNode, len(idx.nodes), capacity)
	} else {
		if len(idx.nodes) < len(nodes) {
			clear(nodes[len(idx.nodes):])
		}
		nodes = nodes[:len(idx.nodes)]
	}
	if !forceFull && previous != nil && len(previous.nodes) <= len(nodes) {
		copy(nodes, previous.nodes)
		for nodeID := len(previous.nodes); nodeID < len(nodes); nodeID++ {
			nodes[nodeID] = cloneVectorIndexSearchNode(idx.nodes[nodeID])
		}
		for nodeID := range idx.searchViewDirty {
			if nodeID >= 0 && nodeID < len(previous.nodes) {
				nodes[nodeID] = cloneVectorIndexSearchNode(idx.nodes[nodeID])
			}
		}
	} else {
		for nodeID := range idx.nodes {
			nodes[nodeID] = cloneVectorIndexSearchNode(idx.nodes[nodeID])
		}
	}
	epoch := idx.persistedEpoch
	if epoch == 0 {
		epoch = idx.fullSnapshotBaseEpoch
	}
	next.nodes = nodes
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
	next.sourceDocumentRootsValid = idx.sourceDocumentRootsValid
	next.rebuildDeletedRatio = idx.rebuildDeletedRatio
	next.epoch = epoch
	next.bytesDisk = idx.persistedBytesDisk
	next.fullRebuilds = idx.liveANNFullRebuilds
	next.mu.Unlock()
	idx.searchView.Store(next)
	if previous != nil && len(previous.nodes) > len(next.nodes) {
		idx.searchViewSpare = nil
	} else {
		idx.searchViewSpare = previous
	}
	clear(idx.searchViewDirty)
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
		view.mu.RUnlock()
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
			view.mu.RUnlock()
		}
		return VectorIndexLoadStatus{}, false
	}
	status := VectorIndexLoadStatus{
		Loaded:    true,
		RootName:  idx.nativeRootName,
		RootID:    view.epoch,
		Epoch:     view.epoch,
		BytesDisk: view.bytesDisk,
	}
	view.mu.RUnlock()
	return status, true
}

func (idx *VectorIndex) publishedNativeSearchState() (liveDocs int, rebuildNeeded bool, fullRebuilds uint64) {
	if idx == nil {
		return 0, false, 0
	}
	view := idx.acquireSearchView()
	if view == nil {
		_, _, liveDocs, rebuildNeeded, fullRebuilds = idx.nativeSearchState()
		return liveDocs, rebuildNeeded, fullRebuilds
	}
	deletedDocs := len(view.nodes) - view.liveDocs
	rebuildNeeded = deletedDocs > 0 && len(view.nodes) > 0 && float64(deletedDocs)/float64(len(view.nodes)) >= view.rebuildDeletedRatio
	liveDocs, fullRebuilds = view.liveDocs, view.fullRebuilds
	view.mu.RUnlock()
	return liveDocs, rebuildNeeded, fullRebuilds
}

func (c *Collection) nativeVectorIndexMutationActive() bool {
	if c == nil || c.writeDomain == nil {
		return false
	}
	c.writeDomain.nativeVectorActiveMu.Lock()
	active := c.writeDomain.nativeVectorActive != 0
	c.writeDomain.nativeVectorActiveMu.Unlock()
	return active
}

func (view *vectorIndexSearchView) searchGraphOnlyWithBuffer(query []float32, topK, efSearch int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, error) {
	if view == nil {
		return nil, errors.New("collections: vector index search view is unavailable")
	}
	if !view.sourceDocumentRootsValid {
		return nil, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, view.name)
	}
	runtimeIndex := VectorIndex{
		metric:     view.metric,
		encoding:   view.encoding,
		dimensions: view.dimensions,
		m:          view.m,
		efSearch:   view.efSearch,
		nodes:      view.nodes,
		entry:      view.entry,
		maxLevel:   view.maxLevel,
	}
	candidates, err := runtimeIndex.searchGraphOnlyCandidatesWithLiveDocsLocked(query, topK, efSearch, view.liveDocs, &buffer.nativeSearchScratch)
	if err != nil {
		return nil, err
	}
	resultCount := 0
	idByteCount := 0
	for _, candidate := range candidates {
		if resultCount >= topK {
			break
		}
		if candidate.nodeID < 0 || candidate.nodeID >= len(view.nodes) || view.nodes[candidate.nodeID].deleted {
			continue
		}
		var err error
		idByteCount, err = addVectorIndexSearchByteTotal(idByteCount, len(view.nodes[candidate.nodeID].documentID), math.MaxInt, "result id")
		if err != nil {
			return nil, err
		}
		resultCount++
	}
	buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, resultCount)
	buffer.idBytes = resizeVectorIndexSearchByteBuffer(buffer.idBytes, idByteCount)
	resultIndex, idOffset := 0, 0
	for _, candidate := range candidates {
		if resultIndex >= resultCount {
			break
		}
		node := view.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		nextIDOffset := idOffset + len(node.documentID)
		id := buffer.idBytes[idOffset:nextIDOffset:nextIDOffset]
		copy(id, node.documentID)
		buffer.results[resultIndex] = VectorIndexSearchResult{ID: id, Score: 1 - float64(candidate.distance)}
		resultIndex++
		idOffset = nextIDOffset
	}
	return buffer.results, nil
}
