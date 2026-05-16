package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type vectorIndexNativeRootBackedGraphReader struct {
	snap      *backenddb.Snapshot
	catalog   *collectionCatalog
	rootName  string
	meta      vectorIndexPersistMeta
	nodeCount int

	nodeBuf []byte
	edgeBuf []byte
	docBuf  []byte
	scratch vectorIndexSearchScratch

	nodeGets int64
	edgeGets int64
	docGets  int64
}

func TestVectorIndexNativeRootBackedGraphSearchMatchesLoadedGraph(t *testing.T) {
	const (
		docs = 512
		dims = 32
		topK = 10
		ef   = 64
	)
	d, col, loaded, _ := openLoadedNativeVectorBenchmarkIndex(t, docs, dims, "embedding_graph_root_backed_match")
	defer func() { _ = d.Close() }()
	reader := newVectorIndexNativeRootBackedGraphReader(t, d, col, loaded.name, loaded.Stats().Nodes)
	defer func() { _ = reader.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	loadedResults, err := loaded.searchGraphOnly(query, topK, ef)
	if err != nil {
		t.Fatalf("loaded graph search: %v", err)
	}
	rootResults, err := reader.searchGraphOnly(query, topK, ef)
	if err != nil {
		t.Fatalf("native root-backed graph search: %v", err)
	}
	if len(rootResults) != len(loadedResults) {
		t.Fatalf("result count=%d want %d", len(rootResults), len(loadedResults))
	}
	for i := range loadedResults {
		if string(rootResults[i].DocumentID) != string(loadedResults[i].DocumentID) {
			t.Fatalf("result %d document=%q want %q", i, rootResults[i].DocumentID, loadedResults[i].DocumentID)
		}
		if rootResults[i].Distance != loadedResults[i].Distance {
			t.Fatalf("result %d distance=%g want %g", i, rootResults[i].Distance, loadedResults[i].Distance)
		}
	}
}

func openNativeVectorBenchmarkIndexRoot(tb testing.TB, docs, dims int, name string) (*backenddb.DB, *Collection, VectorIndexLoadStatus, VectorIndexStats) {
	tb.Helper()
	dir := tb.TempDir()
	def := VectorIndexDefinition{
		Name:       name,
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          16,
	}
	d, col := openVectorBenchmarkCollectionWithVectorIndex(tb, dir, docs, dims, def)
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("build native vector index: %v", err)
	}
	stats := index.Stats()
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		_ = d.Close()
		tb.Fatalf("save native vector index: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("close setup db: %v", err)
	}
	d, err = backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("reopen db: %v", err)
	}
	col, err = NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	return d, col, saveStatus, stats
}

func newVectorIndexNativeRootBackedGraphReader(tb testing.TB, d *backenddb.DB, col *Collection, indexName string, nodeCount int) *vectorIndexNativeRootBackedGraphReader {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("acquire snapshot: nil")
	}
	catalog, err := loadCollectionCatalog(snap, col.meta.Name)
	if err != nil {
		_ = snap.Close()
		tb.Fatalf("load collection catalog: %v", err)
	}
	if catalog == nil {
		_ = snap.Close()
		tb.Fatal("load collection catalog: missing catalog")
	}
	rootName := collectionVectorIndexRootName(col.meta.Name, indexName)
	rawMeta, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, []byte(vectorIndexNativeKeyMeta), nil)
	if err != nil {
		_ = snap.Close()
		tb.Fatalf("read native vector meta: %v", err)
	}
	if !ok {
		_ = snap.Close()
		tb.Fatal("read native vector meta: missing graph root entry")
	}
	var meta vectorIndexPersistMeta
	if err := json.Unmarshal(rawMeta, &meta); err != nil {
		_ = snap.Close()
		tb.Fatalf("decode native vector meta: %v", err)
	}
	return &vectorIndexNativeRootBackedGraphReader{
		snap:      snap,
		catalog:   catalog,
		rootName:  rootName,
		meta:      meta,
		nodeCount: nodeCount,
	}
}

func (r *vectorIndexNativeRootBackedGraphReader) Close() error {
	if r == nil || r.snap == nil {
		return nil
	}
	return r.snap.Close()
}

func (r *vectorIndexNativeRootBackedGraphReader) resetCounters() {
	r.nodeGets = 0
	r.edgeGets = 0
	r.docGets = 0
}

func (r *vectorIndexNativeRootBackedGraphReader) searchGraphOnly(query []float32, topK, efSearch int) ([]VectorSearchResult, error) {
	if r == nil {
		return nil, errors.New("collections: native vector root reader is nil")
	}
	if topK <= 0 {
		return nil, errors.New("collections: vector search TopK must be positive")
	}
	if len(query) == 0 {
		return nil, errors.New("collections: vector query cannot be empty")
	}
	if err := validateFloat32Vector(query); err != nil {
		return nil, fmt.Errorf("collections: vector query: %w", err)
	}
	if r.meta.Dimensions != 0 && len(query) != r.meta.Dimensions {
		return nil, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), r.meta.Dimensions)
	}
	queryNorm := float64(-1)
	if r.meta.Metric == VectorMetricCosine {
		queryNorm = vectorNormSquared(query)
		if queryNorm == 0 {
			return nil, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if r.meta.Metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(query, queryNorm)
		if err != nil {
			return nil, err
		}
		prepared = &preparedQuery
	}
	if r.meta.Entry < 0 || r.nodeCount == 0 {
		return nil, nil
	}
	limit := efSearch
	if limit <= 0 {
		limit = r.meta.EfSearch
	}
	if limit < topK {
		limit = topK
	}

	entryPoint := r.meta.Entry
	for layer := r.meta.MaxLevel; layer > 0; layer-- {
		nextEntry, err := r.greedyNearestAtLayer(query, queryNorm, prepared, entryPoint, layer)
		if err != nil {
			return nil, err
		}
		entryPoint = nextEntry
	}
	candidates, err := r.searchLayer(query, queryNorm, prepared, entryPoint, limit, 0)
	if err != nil {
		return nil, err
	}
	results := make([]VectorSearchResult, 0, minInt(topK, len(candidates)))
	for _, candidate := range candidates {
		if len(results) >= topK {
			break
		}
		node, ok, err := r.readNode(candidate.nodeID)
		if err != nil {
			return nil, err
		}
		if !ok || node.deleted {
			continue
		}
		current, err := r.isCurrentNode(candidate.nodeID, string(node.documentID))
		if err != nil {
			return nil, err
		}
		if !current {
			continue
		}
		results = append(results, VectorSearchResult{
			DocumentID: node.documentID,
			Distance:   candidate.distance,
		})
	}
	cloneVectorSearchResultDocumentIDs(results)
	return results, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) greedyNearestAtLayer(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, layer int) (int, error) {
	if entryPoint < 0 {
		return entryPoint, nil
	}
	best := entryPoint
	bestDistance, err := r.distanceToNode(query, queryNormSquared, prepared, best)
	if err != nil {
		return best, err
	}
	changed := true
	for changed {
		changed = false
		edges, ok, err := r.readEdges(best, layer)
		if err != nil {
			return best, err
		}
		if !ok {
			continue
		}
		for _, neighborID := range edges.Neighbor {
			if neighborID < 0 || neighborID >= r.nodeCount {
				continue
			}
			distance, err := r.distanceToNode(query, queryNormSquared, prepared, neighborID)
			if err != nil {
				return best, err
			}
			if distance < bestDistance {
				best = neighborID
				bestDistance = distance
				changed = true
			}
		}
	}
	return best, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) searchLayer(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit int, layer int) ([]vectorIndexCandidate, error) {
	if entryPoint < 0 || entryPoint >= r.nodeCount || limit <= 0 {
		return nil, nil
	}
	entryDistance, err := r.distanceToNode(query, queryNormSquared, prepared, entryPoint)
	if err != nil {
		return nil, err
	}
	if math.IsInf(float64(entryDistance), 1) {
		return nil, nil
	}
	visited, mark := r.scratch.nextVisitedEpoch(r.nodeCount)
	visited[entryPoint] = mark
	entry := vectorIndexCandidate{nodeID: entryPoint, distance: entryDistance}
	queue := r.scratch.queue[:0]
	queue.push(entry)
	best := r.scratch.best[:0]
	best.pushBounded(entry, limit)
	for len(queue) > 0 {
		current := queue.pop()
		if len(best) >= limit && vectorIndexCandidateWorse(current, best[0]) {
			break
		}
		if current.nodeID < 0 || current.nodeID >= r.nodeCount {
			continue
		}
		edges, ok, err := r.readEdges(current.nodeID, layer)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for _, neighborID := range edges.Neighbor {
			if neighborID < 0 || neighborID >= r.nodeCount || visited[neighborID] == mark {
				continue
			}
			visited[neighborID] = mark
			distance, err := r.distanceToNode(query, queryNormSquared, prepared, neighborID)
			if err != nil {
				return nil, err
			}
			if math.IsInf(float64(distance), 1) {
				continue
			}
			candidate := vectorIndexCandidate{nodeID: neighborID, distance: distance}
			if len(best) < limit || vectorIndexCandidateLess(candidate, best[0]) {
				queue.push(candidate)
				best.pushBounded(candidate, limit)
			}
		}
	}
	r.scratch.queue = queue[:0]
	r.scratch.best = best[:0]
	out := append(r.scratch.out[:0], best...)
	r.scratch.out = out
	sortVectorIndexCandidates(out)
	return out, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) distanceToNode(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, nodeID int) (float32, error) {
	node, ok, err := r.readNode(nodeID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return float32(math.Inf(1)), nil
	}
	if prepared != nil && r.meta.Metric == VectorMetricCosine && len(node.vector) > 0 {
		return vectorDistanceToFloat32NodeCosineUnchecked(*prepared, &node), nil
	}
	distance, err := vectorDistanceToStoredNodeWithQueryNorm(query, queryNormSquared, &node, r.meta.Metric)
	if err != nil {
		return float32(math.Inf(1)), nil
	}
	return distance, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) readNode(nodeID int) (vectorIndexNode, bool, error) {
	if nodeID < 0 || nodeID >= r.nodeCount {
		return vectorIndexNode{}, false, nil
	}
	r.nodeGets++
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, vectorIndexNativeNodeKey(nodeID), r.nodeBuf)
	r.nodeBuf = data
	if err != nil || !ok {
		return vectorIndexNode{}, ok, err
	}
	var persisted vectorIndexPersistNode
	if err := json.Unmarshal(data, &persisted); err != nil {
		return vectorIndexNode{}, false, err
	}
	node := vectorIndexNode{
		documentID: []byte(persisted.DocumentID),
		vector:     persisted.Vector,
		quantized:  persisted.Quantized,
		quantScale: persisted.QuantScale,
		level:      persisted.Level,
		deleted:    persisted.Deleted,
	}
	node.cacheVectorNorms()
	return node, true, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) readEdges(nodeID, layer int) (vectorIndexPersistEdges, bool, error) {
	if nodeID < 0 || nodeID >= r.nodeCount || layer < 0 {
		return vectorIndexPersistEdges{}, false, nil
	}
	r.edgeGets++
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, vectorIndexNativeEdgeKey(nodeID, layer), r.edgeBuf)
	r.edgeBuf = data
	if err != nil || !ok {
		return vectorIndexPersistEdges{}, ok, err
	}
	var edges vectorIndexPersistEdges
	if err := json.Unmarshal(data, &edges); err != nil {
		return vectorIndexPersistEdges{}, false, err
	}
	return edges, true, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) isCurrentNode(nodeID int, docID string) (bool, error) {
	r.docGets++
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, vectorIndexNativeDocKey(docID), r.docBuf)
	r.docBuf = data
	if err != nil || !ok {
		return false, err
	}
	var currentNodeID int
	if err := json.Unmarshal(data, &currentNodeID); err != nil {
		return false, err
	}
	return currentNodeID == nodeID, nil
}
