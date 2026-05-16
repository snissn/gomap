package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
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
	nodeKey []byte
	edgeKey []byte
	docKey  []byte
	vector  []float32
	edges   []int
	docIDs  []byte
	results []VectorSearchResult
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
	if !saveStatus.Loaded || saveStatus.RootID == 0 {
		_ = d.Close()
		tb.Fatalf("unexpected native save status: %+v", saveStatus)
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
		return []VectorSearchResult{}, nil
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
	results := r.results[:0]
	if cap(results) < minInt(topK, len(candidates)) {
		results = make([]VectorSearchResult, 0, minInt(topK, len(candidates)))
	}
	r.docIDs = r.docIDs[:0]
	for _, candidate := range candidates {
		if len(results) >= topK {
			break
		}
		docID, deleted, ok, err := r.readNodeDocumentID(candidate.nodeID)
		if err != nil {
			return nil, err
		}
		if !ok || deleted {
			continue
		}
		current, err := r.isCurrentNode(candidate.nodeID, docID)
		if err != nil {
			return nil, err
		}
		if !current {
			continue
		}
		start := len(r.docIDs)
		r.docIDs = append(r.docIDs, docID...)
		results = append(results, VectorSearchResult{
			DocumentID: r.docIDs[start:len(r.docIDs):len(r.docIDs)],
			Distance:   candidate.distance,
		})
	}
	r.results = results
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
		neighbors, ok, err := r.readNeighbors(best, layer)
		if err != nil {
			return best, err
		}
		if !ok {
			continue
		}
		for _, neighborID := range neighbors {
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
		neighbors, ok, err := r.readNeighbors(current.nodeID, layer)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for _, neighborID := range neighbors {
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
	node, ok, err := r.readDistanceNode(nodeID)
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

func (r *vectorIndexNativeRootBackedGraphReader) readDistanceNode(nodeID int) (vectorIndexNode, bool, error) {
	if nodeID < 0 || nodeID >= r.nodeCount {
		return vectorIndexNode{}, false, nil
	}
	r.nodeGets++
	r.nodeKey = appendVectorIndexNativeRootNodeKey(r.nodeKey, nodeID)
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, r.nodeKey, r.nodeBuf)
	r.nodeBuf = data
	if err != nil || !ok {
		return vectorIndexNode{}, ok, err
	}
	vector, normSquared, vectorOK, err := parseVectorIndexNativeRootFloat32Vector(data, r.vector[:0])
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	r.vector = vector
	if vectorOK {
		node := vectorIndexNode{
			vector:      r.vector,
			normSquared: normSquared,
			deleted:     vectorIndexNativeRootJSONBoolTrue(data, "deleted"),
		}
		if normSquared > 0 {
			node.cachedInvNorm = float32(1 / math.Sqrt(normSquared))
		}
		return node, true, nil
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

func (r *vectorIndexNativeRootBackedGraphReader) readNodeDocumentID(nodeID int) ([]byte, bool, bool, error) {
	if nodeID < 0 || nodeID >= r.nodeCount {
		return nil, false, false, nil
	}
	r.nodeGets++
	r.nodeKey = appendVectorIndexNativeRootNodeKey(r.nodeKey, nodeID)
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, r.nodeKey, r.nodeBuf)
	r.nodeBuf = data
	if err != nil || !ok {
		return nil, false, ok, err
	}
	docID, docOK := parseVectorIndexNativeRootJSONStringField(data, "document_id")
	if docOK {
		return docID, vectorIndexNativeRootJSONBoolTrue(data, "deleted"), true, nil
	}
	var persisted vectorIndexPersistNode
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, false, false, err
	}
	return []byte(persisted.DocumentID), persisted.Deleted, true, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) readNeighbors(nodeID, layer int) ([]int, bool, error) {
	if nodeID < 0 || nodeID >= r.nodeCount || layer < 0 {
		return nil, false, nil
	}
	r.edgeGets++
	r.edgeKey = appendVectorIndexNativeRootEdgeKey(r.edgeKey, nodeID, layer)
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, r.edgeKey, r.edgeBuf)
	r.edgeBuf = data
	if err != nil || !ok {
		return nil, ok, err
	}
	neighbors, neighborsOK, err := parseVectorIndexNativeRootIntArrayField(data, "neighbors", r.edges[:0])
	if err != nil {
		return nil, false, err
	}
	r.edges = neighbors
	if neighborsOK {
		return r.edges, true, nil
	}
	var edges vectorIndexPersistEdges
	if err := json.Unmarshal(data, &edges); err != nil {
		return nil, false, err
	}
	r.edges = append(r.edges[:0], edges.Neighbor...)
	return r.edges, true, nil
}

func (r *vectorIndexNativeRootBackedGraphReader) isCurrentNode(nodeID int, docID []byte) (bool, error) {
	r.docGets++
	r.docKey = appendVectorIndexNativeRootDocKey(r.docKey, docID)
	data, ok, err := collectionGetAppendAtCatalogRoot(r.snap, r.catalog, r.rootName, r.docKey, r.docBuf)
	r.docBuf = data
	if err != nil || !ok {
		return false, err
	}
	currentNodeID, err := parseVectorIndexNativeRootJSONInt(data)
	if err != nil {
		return false, err
	}
	return currentNodeID == nodeID, nil
}

func appendVectorIndexNativeRootNodeKey(dst []byte, nodeID int) []byte {
	dst = append(dst[:0], vectorIndexNativeKeyPrefixNode...)
	return appendVectorIndexNativeRootPaddedInt(dst, nodeID, vectorIndexNativeKeyOrdinalWidth)
}

func appendVectorIndexNativeRootEdgeKey(dst []byte, nodeID, layer int) []byte {
	dst = append(dst[:0], vectorIndexNativeKeyPrefixEdge...)
	dst = appendVectorIndexNativeRootPaddedInt(dst, nodeID, vectorIndexNativeKeyOrdinalWidth)
	dst = append(dst, '/')
	return appendVectorIndexNativeRootPaddedInt(dst, layer, vectorIndexNativeKeyEdgeLayerWidth)
}

func appendVectorIndexNativeRootDocKey(dst []byte, docID []byte) []byte {
	dst = append(dst[:0], vectorIndexNativeKeyPrefixDoc...)
	return append(dst, docID...)
}

func appendVectorIndexNativeRootPaddedInt(dst []byte, value, width int) []byte {
	start := len(dst)
	for i := 0; i < width; i++ {
		dst = append(dst, '0')
	}
	for pos := start + width - 1; pos >= start && value > 0; pos-- {
		dst[pos] = byte('0' + value%10)
		value /= 10
	}
	return dst
}

func parseVectorIndexNativeRootFloat32Vector(data []byte, dst []float32) ([]float32, float64, bool, error) {
	start, ok := vectorIndexNativeRootJSONArrayStart(data, "vector")
	if !ok {
		return dst, 0, false, nil
	}
	dst = dst[:0]
	var normSquared float64
	i := start
	for {
		i = vectorIndexNativeRootSkipSpaces(data, i)
		if i >= len(data) {
			return dst, 0, false, errors.New("collections: invalid native vector JSON array")
		}
		if data[i] == ']' {
			return dst, normSquared, true, nil
		}
		valueStart := i
		for i < len(data) && data[i] != ',' && data[i] != ']' {
			i++
		}
		valueEnd := i
		for valueEnd > valueStart && isVectorIndexNativeRootSpace(data[valueEnd-1]) {
			valueEnd--
		}
		value, err := parseVectorIndexNativeRootFloat32Token(data[valueStart:valueEnd])
		if err != nil {
			return dst, 0, false, err
		}
		dst = append(dst, value)
		v := float64(value)
		normSquared += v * v
		i = vectorIndexNativeRootSkipSpaces(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
}

func parseVectorIndexNativeRootIntArrayField(data []byte, field string, dst []int) ([]int, bool, error) {
	start, ok := vectorIndexNativeRootJSONArrayStart(data, field)
	if !ok {
		return dst, false, nil
	}
	dst = dst[:0]
	i := start
	for {
		i = vectorIndexNativeRootSkipSpaces(data, i)
		if i >= len(data) {
			return dst, false, errors.New("collections: invalid native vector JSON array")
		}
		if data[i] == ']' {
			return dst, true, nil
		}
		valueStart := i
		for i < len(data) && data[i] != ',' && data[i] != ']' {
			i++
		}
		valueEnd := i
		for valueEnd > valueStart && isVectorIndexNativeRootSpace(data[valueEnd-1]) {
			valueEnd--
		}
		value, err := parseVectorIndexNativeRootIntToken(data[valueStart:valueEnd])
		if err != nil {
			return dst, false, err
		}
		dst = append(dst, value)
		i = vectorIndexNativeRootSkipSpaces(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
}

func vectorIndexNativeRootJSONArrayStart(data []byte, field string) (int, bool) {
	prefix := []byte(`"` + field + `":[`)
	pos := bytes.Index(data, prefix)
	if pos < 0 {
		return 0, false
	}
	return pos + len(prefix), true
}

func parseVectorIndexNativeRootJSONStringField(data []byte, field string) ([]byte, bool) {
	prefix := []byte(`"` + field + `":"`)
	pos := bytes.Index(data, prefix)
	if pos < 0 {
		return nil, false
	}
	start := pos + len(prefix)
	end := start
	for end < len(data) && data[end] != '"' {
		if data[end] == '\\' {
			return nil, false
		}
		end++
	}
	if end >= len(data) {
		return nil, false
	}
	return data[start:end], true
}

func vectorIndexNativeRootJSONBoolTrue(data []byte, field string) bool {
	return bytes.Contains(data, []byte(`"`+field+`":true`))
}

func parseVectorIndexNativeRootJSONInt(data []byte) (int, error) {
	start := vectorIndexNativeRootSkipSpaces(data, 0)
	end := start
	for end < len(data) && data[end] >= '0' && data[end] <= '9' {
		end++
	}
	if end == start {
		return 0, errors.New("collections: invalid native vector JSON integer")
	}
	return parseVectorIndexNativeRootIntToken(data[start:end])
}

func parseVectorIndexNativeRootFloat32Token(data []byte) (float32, error) {
	if len(data) == 0 {
		return 0, errors.New("collections: invalid native vector JSON float")
	}
	i := 0
	sign := 1.0
	if data[i] == '-' {
		sign = -1
		i++
	} else if data[i] == '+' {
		i++
	}
	if i >= len(data) {
		return 0, errors.New("collections: invalid native vector JSON float")
	}
	var integer uint64
	digits := 0
	for i < len(data) && data[i] >= '0' && data[i] <= '9' {
		integer = integer*10 + uint64(data[i]-'0')
		i++
		digits++
	}
	value := float64(integer)
	if i < len(data) && data[i] == '.' {
		i++
		scale := 1.0
		for i < len(data) && data[i] >= '0' && data[i] <= '9' {
			value = value*10 + float64(data[i]-'0')
			scale *= 10
			i++
			digits++
		}
		value /= scale
	}
	if digits == 0 {
		return 0, errors.New("collections: invalid native vector JSON float")
	}
	if i == len(data) {
		return float32(sign * value), nil
	}
	if data[i] == 'e' || data[i] == 'E' {
		value, err := strconv.ParseFloat(string(data), 32)
		if err != nil {
			return 0, err
		}
		return float32(value), nil
	}
	return 0, errors.New("collections: invalid native vector JSON float")
}

func parseVectorIndexNativeRootIntToken(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("collections: invalid native vector JSON integer")
	}
	i := 0
	sign := 1
	if data[i] == '-' {
		sign = -1
		i++
	}
	if i >= len(data) {
		return 0, errors.New("collections: invalid native vector JSON integer")
	}
	value := 0
	for ; i < len(data); i++ {
		if data[i] < '0' || data[i] > '9' {
			return 0, errors.New("collections: invalid native vector JSON integer")
		}
		value = value*10 + int(data[i]-'0')
	}
	return sign * value, nil
}

func vectorIndexNativeRootSkipSpaces(data []byte, i int) int {
	for i < len(data) && isVectorIndexNativeRootSpace(data[i]) {
		i++
	}
	return i
}

func isVectorIndexNativeRootSpace(c byte) bool {
	return c == ' ' || c == '\n' || c == '\r' || c == '\t'
}
