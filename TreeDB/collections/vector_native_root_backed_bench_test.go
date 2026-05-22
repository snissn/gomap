package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type vectorIndexNativeRootRecordFormat uint8

const (
	vectorIndexNativeRootRecordFormatJSON vectorIndexNativeRootRecordFormat = iota
	vectorIndexNativeRootRecordFormatTemplateV1Raw
)

const (
	vectorIndexTemplateV1NodeTemplateID uint64 = 1
	vectorIndexTemplateV1EdgeTemplateID uint64 = 2
)

type vectorIndexNativeRootBackedGraphReader struct {
	snap      *backenddb.Snapshot
	catalog   *collectionCatalog
	rootName  string
	meta      vectorIndexPersistMeta
	nodeCount int
	format    vectorIndexNativeRootRecordFormat

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
	d, loaded, _ := openLoadedNativeVectorBenchmarkIndex(t, docs, dims, "embedding_graph_root_backed_match")
	defer func() { _ = d.Close() }()
	col := loaded.collection
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
	assertVectorSearchResultsMatch(t, rootResults, loadedResults)
}

func TestVectorIndexNativeRootTemplateV1GraphSearchMatchesLoadedGraph(t *testing.T) {
	const (
		docs = 512
		dims = 32
		topK = 10
		ef   = 64
	)
	d, col, status, setupStats := openTemplateV1NativeVectorBenchmarkIndexRoot(t, docs, dims, "embedding_graph_template_v1_match")
	defer func() { _ = d.Close() }()
	reader := newVectorIndexNativeRootTemplateV1GraphReader(t, d, col, "embedding_graph_template_v1_match", setupStats.Nodes, status.Meta)
	defer func() { _ = reader.Close() }()
	loaded, _, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{
		Name:       "embedding_graph_template_v1_match",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          16,
	})
	if err == nil && loaded != nil {
		t.Fatal("template-v1 raw benchmark root should not load through JSON native snapshot loader")
	}

	referenceD, loadedReference, _ := openLoadedNativeVectorBenchmarkIndex(t, docs, dims, "embedding_graph_template_v1_match")
	defer func() { _ = referenceD.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	loadedResults, err := loadedReference.searchGraphOnly(query, topK, ef)
	if err != nil {
		t.Fatalf("loaded graph search: %v", err)
	}
	rootResults, err := reader.searchGraphOnly(query, topK, ef)
	if err != nil {
		t.Fatalf("template-v1 native root-backed graph search: %v", err)
	}
	if len(rootResults) != len(loadedResults) {
		t.Fatalf("result count=%d want %d", len(rootResults), len(loadedResults))
	}
	assertVectorSearchResultsMatch(t, rootResults, loadedResults)
}

func TestDecodeTemplateV1RawFloat32SliceUsesLittleEndian(t *testing.T) {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint32(raw[0:], math.Float32bits(1.25))
	binary.LittleEndian.PutUint32(raw[4:], math.Float32bits(-2.5))

	dst := []float32{99, 99, 99}
	got, err := decodeTemplateV1RawFloat32Slice(raw, dst[:0])
	if err != nil {
		t.Fatalf("decode raw vector: %v", err)
	}
	if len(got) != 2 || got[0] != 1.25 || got[1] != -2.5 {
		t.Fatalf("decoded vector=%v want [1.25 -2.5]", got)
	}
	if cap(got) != cap(dst) {
		t.Fatalf("decoded vector cap=%d want reused cap %d", cap(got), cap(dst))
	}
}

func assertVectorSearchResultsMatch(t *testing.T, got, want []VectorSearchResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result count=%d want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].DocumentID, want[i].DocumentID) {
			t.Fatalf("result %d document=%q want %q", i, got[i].DocumentID, want[i].DocumentID)
		}
		if math.Abs(float64(got[i].Distance-want[i].Distance)) > 1e-6 {
			t.Fatalf("result %d distance=%g want %g", i, got[i].Distance, want[i].Distance)
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

type vectorIndexTemplateV1BenchmarkStatus struct {
	VectorIndexLoadStatus
	Meta vectorIndexPersistMeta
}

func openTemplateV1NativeVectorBenchmarkIndexRoot(tb testing.TB, docs, dims int, name string) (*backenddb.DB, *Collection, vectorIndexTemplateV1BenchmarkStatus, VectorIndexStats) {
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
	saveStatus, err := saveTemplateV1NativeVectorBenchmarkRoot(index)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("save template-v1 native vector index root: %v", err)
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

func saveTemplateV1NativeVectorBenchmarkRoot(idx *VectorIndex) (vectorIndexTemplateV1BenchmarkStatus, error) {
	status := vectorIndexTemplateV1BenchmarkStatus{}
	c := idx.collection
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return status, backenddb.ErrClosed
	}
	defer func() { _ = pin.Close() }()
	catalog, err := loadCollectionCatalog(pin, c.meta.Name)
	if err != nil {
		return status, err
	}
	rootName := collectionVectorIndexRootName(catalog.meta.Name, idx.name)
	baseRootIDs := map[string]uint64{rootName: catalog.rootID(rootName)}
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	policy, err := collectionRootStoragePolicyForDB(c.db, catalog.meta, rootName)
	if err != nil {
		return status, err
	}
	snapshot, snapshotSeq := idx.persistSnapshot()
	table, bytesDisk, err := buildVectorIndexTemplateV1RawSnapshotTable(snapshot)
	if err != nil {
		return status, err
	}
	table.Freeze()
	publishTable, pointerized, err := pointerizeCollectionRunTableValues(c.db, table)
	if err != nil {
		resetCollectionRunTable(table)
		return status, err
	}
	if pointerized {
		defer resetCollectionRunTable(publishTable)
	}
	iter := publishTable.NewIterator(nil, nil)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      0,
		Iter:          iter,
		StoragePolicy: policy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(catalog.meta, baseCommitSeq, baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
	})
	_ = iter.Close()
	resetCollectionRunTable(table)
	if err != nil {
		return status, err
	}
	if len(rootIDs) != 1 {
		return status, unexpectedOrderedRootCountError(catalog.meta.Name, 1, len(rootIDs))
	}
	status.Loaded = true
	status.RootName = rootName
	status.RootID = rootIDs[0]
	status.Epoch = rootIDs[0]
	status.BytesDisk = bytesDisk
	status.Meta = snapshot.Meta
	idx.setNativePersistent(true)
	idx.recordPersistedSnapshot(status.Epoch, bytesDisk, snapshotSeq)
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, []string{rootName}, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return status, nil
}

func buildVectorIndexTemplateV1RawSnapshotTable(snapshot vectorIndexPersistSnapshot) (memtable.Table, int64, error) {
	entryCount := 1 + len(snapshot.Nodes) + len(snapshot.Edges) + len(snapshot.Tombstones.NodeIDs) + len(snapshot.DocMap.Current)
	table := newCollectionRunTable(entryCount)
	var bytesDisk int64
	addRaw := func(key []byte, data []byte) {
		bytesDisk += int64(len(data))
		table.SetSteal(key, data)
	}
	meta, err := json.Marshal(snapshot.Meta)
	if err != nil {
		resetCollectionRunTable(table)
		return nil, 0, err
	}
	meta = append(meta, '\n')
	addRaw([]byte(vectorIndexNativeKeyMeta), meta)
	for i := range snapshot.Nodes {
		addRaw(vectorIndexNativeNodeKey(i), appendVectorIndexTemplateV1RawNode(nil, snapshot.Nodes[i]))
	}
	for i := range snapshot.Edges {
		edge := snapshot.Edges[i]
		addRaw(vectorIndexNativeEdgeKey(edge.NodeID, edge.Layer), appendVectorIndexTemplateV1RawEdges(nil, edge))
	}
	for _, nodeID := range snapshot.Tombstones.NodeIDs {
		addRaw(vectorIndexNativeTombstoneKey(nodeID), binary.AppendUvarint(nil, uint64(nodeID)))
	}
	for docID, nodeID := range snapshot.DocMap.Current {
		addRaw(vectorIndexNativeDocKey(docID), binary.AppendUvarint(nil, uint64(nodeID)))
	}
	return table, bytesDisk, nil
}

func appendVectorIndexTemplateV1RawNode(dst []byte, node vectorIndexPersistNode) []byte {
	dst = append(dst, templateV1StoredMagic...)
	dst = appendTemplateV1Uvarint(dst, vectorIndexTemplateV1NodeTemplateID)
	if node.Deleted {
		dst = append(dst, templateV1KindTrue)
	} else {
		dst = append(dst, templateV1KindFalse)
	}
	dst = appendTemplateV1RawBytes(dst, []byte(node.DocumentID))
	dst = appendTemplateV1RawFloat64(dst, float64(node.Level))
	dst = appendTemplateV1RawFloat64(dst, vectorNormSquared(node.Vector))
	dst = appendTemplateV1RawFloat32Slice(dst, node.Vector)
	return dst
}

func appendVectorIndexTemplateV1RawEdges(dst []byte, edge vectorIndexPersistEdges) []byte {
	dst = append(dst, templateV1StoredMagic...)
	dst = appendTemplateV1Uvarint(dst, vectorIndexTemplateV1EdgeTemplateID)
	dst = appendTemplateV1RawFloat64(dst, float64(edge.Layer))
	dst = appendTemplateV1RawIntSlice(dst, edge.Neighbor)
	dst = appendTemplateV1RawFloat64(dst, float64(edge.NodeID))
	return dst
}

func appendTemplateV1RawFloat64(dst []byte, value float64) []byte {
	dst = append(dst, templateV1KindFloat64)
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], math.Float64bits(value))
	return append(dst, scratch[:]...)
}

func appendTemplateV1RawBytes(dst []byte, raw []byte) []byte {
	dst = append(dst, templateV1KindString)
	dst = appendTemplateV1Uvarint(dst, uint64(len(raw)))
	return append(dst, raw...)
}

func appendTemplateV1RawFloat32Slice(dst []byte, values []float32) []byte {
	dst = append(dst, templateV1KindString)
	dst = appendTemplateV1Uvarint(dst, uint64(len(values)*4))
	var scratch [4]byte
	for _, value := range values {
		binary.LittleEndian.PutUint32(scratch[:], math.Float32bits(value))
		dst = append(dst, scratch[:]...)
	}
	return dst
}

func appendTemplateV1RawIntSlice(dst []byte, values []int) []byte {
	dst = append(dst, templateV1KindString)
	dst = appendTemplateV1Uvarint(dst, uint64(len(values)*4))
	var scratch [4]byte
	for _, value := range values {
		binary.LittleEndian.PutUint32(scratch[:], uint32(value))
		dst = append(dst, scratch[:]...)
	}
	return dst
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
		format:    vectorIndexNativeRootRecordFormatJSON,
	}
}

func newVectorIndexNativeRootTemplateV1GraphReader(tb testing.TB, d *backenddb.DB, col *Collection, indexName string, nodeCount int, meta vectorIndexPersistMeta) *vectorIndexNativeRootBackedGraphReader {
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
	return &vectorIndexNativeRootBackedGraphReader{
		snap:      snap,
		catalog:   catalog,
		rootName:  collectionVectorIndexRootName(col.meta.Name, indexName),
		meta:      meta,
		nodeCount: nodeCount,
		format:    vectorIndexNativeRootRecordFormatTemplateV1Raw,
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
		return 0, fmt.Errorf("collections: native vector root distance node=%d metric=%s: %w", nodeID, r.meta.Metric, err)
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
	if r.format == vectorIndexNativeRootRecordFormatTemplateV1Raw {
		node, ok, err := parseVectorIndexTemplateV1RawDistanceNode(data, r.vector[:0])
		if err != nil || !ok {
			return vectorIndexNode{}, ok, err
		}
		r.vector = node.vector
		return node, true, nil
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
	if r.format == vectorIndexNativeRootRecordFormatTemplateV1Raw {
		return parseVectorIndexTemplateV1RawNodeDocumentID(data)
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
	if r.format == vectorIndexNativeRootRecordFormatTemplateV1Raw {
		neighbors, err := parseVectorIndexTemplateV1RawNeighbors(data, r.edges[:0])
		if err != nil {
			return nil, false, err
		}
		r.edges = neighbors
		return r.edges, true, nil
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
	if err != nil {
		return false, fmt.Errorf("collections: native vector root doc map node=%d document=%q: %w", nodeID, docID, err)
	}
	if !ok {
		return false, fmt.Errorf("collections: native vector root doc map missing node=%d document=%q", nodeID, docID)
	}
	if r.format == vectorIndexNativeRootRecordFormatTemplateV1Raw {
		currentNodeID, err := parseVectorIndexNativeRootBinaryUint64(data)
		if err != nil {
			return false, err
		}
		return currentNodeID == nodeID, nil
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
	if value < 0 {
		panic(fmt.Sprintf("collections: native vector root key negative ordinal %d", value))
	}
	original := value
	start := len(dst)
	for i := 0; i < width; i++ {
		dst = append(dst, '0')
	}
	for pos := start + width - 1; pos >= start && value > 0; pos-- {
		dst[pos] = byte('0' + value%10)
		value /= 10
	}
	if value > 0 {
		panic(fmt.Sprintf("collections: native vector root key ordinal %d exceeds width %d", original, width))
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

func parseVectorIndexTemplateV1RawDistanceNode(data []byte, dst []float32) (vectorIndexNode, bool, error) {
	pos, err := parseVectorIndexTemplateV1RawHeader(data, vectorIndexTemplateV1NodeTemplateID)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	deleted, pos, err := readTemplateV1RawBool(data, pos)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	_, pos, err = readTemplateV1RawBytes(data, pos)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	_, pos, err = readTemplateV1RawFloat64(data, pos)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	normSquared, pos, err := readTemplateV1RawFloat64(data, pos)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	rawVector, pos, err := readTemplateV1RawBytes(data, pos)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	if pos != len(data) {
		return vectorIndexNode{}, false, errors.New("collections: trailing template-v1 raw node bytes")
	}
	vector, err := decodeTemplateV1RawFloat32Slice(rawVector, dst)
	if err != nil {
		return vectorIndexNode{}, false, err
	}
	node := vectorIndexNode{
		vector:      vector,
		normSquared: normSquared,
		deleted:     deleted,
	}
	if normSquared > 0 {
		node.cachedInvNorm = float32(1 / math.Sqrt(normSquared))
	}
	return node, true, nil
}

func parseVectorIndexTemplateV1RawNodeDocumentID(data []byte) ([]byte, bool, bool, error) {
	pos, err := parseVectorIndexTemplateV1RawHeader(data, vectorIndexTemplateV1NodeTemplateID)
	if err != nil {
		return nil, false, false, err
	}
	deleted, pos, err := readTemplateV1RawBool(data, pos)
	if err != nil {
		return nil, false, false, err
	}
	docID, _, err := readTemplateV1RawBytes(data, pos)
	if err != nil {
		return nil, false, false, err
	}
	return docID, deleted, true, nil
}

func parseVectorIndexTemplateV1RawNeighbors(data []byte, dst []int) ([]int, error) {
	pos, err := parseVectorIndexTemplateV1RawHeader(data, vectorIndexTemplateV1EdgeTemplateID)
	if err != nil {
		return dst, err
	}
	_, pos, err = readTemplateV1RawFloat64(data, pos)
	if err != nil {
		return dst, err
	}
	rawNeighbors, _, err := readTemplateV1RawBytes(data, pos)
	if err != nil {
		return dst, err
	}
	if len(rawNeighbors)%4 != 0 {
		return dst, errors.New("collections: malformed template-v1 raw neighbor bytes")
	}
	dst = dst[:0]
	for i := 0; i < len(rawNeighbors); i += 4 {
		dst = append(dst, int(binary.LittleEndian.Uint32(rawNeighbors[i:])))
	}
	return dst, nil
}

func parseVectorIndexNativeRootBinaryUint64(data []byte) (int, error) {
	value, n := binary.Uvarint(data)
	if n <= 0 || n != len(data) {
		return 0, errors.New("collections: invalid native vector binary integer")
	}
	const maxIntValue = int(^uint(0) >> 1)
	if value > uint64(maxIntValue) {
		return 0, errors.New("collections: native vector binary integer overflows int")
	}
	return int(value), nil
}

func parseVectorIndexTemplateV1RawHeader(data []byte, wantID uint64) (int, error) {
	pos := 0
	if !consumeMagic(data, &pos, templateV1StoredMagic) {
		return 0, errors.New("collections: malformed template-v1 raw stored document")
	}
	id, err := readTemplateV1TemplateID(data, &pos)
	if err != nil {
		return 0, fmt.Errorf("collections: malformed template-v1 raw template id: %w", err)
	}
	if id != wantID {
		return 0, fmt.Errorf("collections: template-v1 raw template id=%d want %d", id, wantID)
	}
	return pos, nil
}

func readTemplateV1RawBool(raw []byte, pos int) (bool, int, error) {
	if pos >= len(raw) {
		return false, pos, errors.New("collections: malformed template-v1 raw bool")
	}
	switch raw[pos] {
	case templateV1KindFalse:
		return false, pos + 1, nil
	case templateV1KindTrue:
		return true, pos + 1, nil
	default:
		return false, pos, errors.New("collections: malformed template-v1 raw bool")
	}
}

func readTemplateV1RawFloat64(raw []byte, pos int) (float64, int, error) {
	if pos >= len(raw) || raw[pos] != templateV1KindFloat64 {
		return 0, pos, errors.New("collections: malformed template-v1 raw float64")
	}
	pos++
	if len(raw)-pos < 8 {
		return 0, pos, errors.New("collections: malformed template-v1 raw float64")
	}
	value := math.Float64frombits(binary.BigEndian.Uint64(raw[pos:]))
	return value, pos + 8, nil
}

func readTemplateV1RawBytes(raw []byte, pos int) ([]byte, int, error) {
	if pos >= len(raw) || raw[pos] != templateV1KindString {
		return nil, pos, errors.New("collections: malformed template-v1 raw bytes")
	}
	pos++
	n, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return nil, pos, err
	}
	if n > uint64(len(raw)-pos) {
		return nil, pos, errors.New("collections: malformed template-v1 raw bytes")
	}
	end := pos + int(n)
	return raw[pos:end], end, nil
}

func decodeTemplateV1RawFloat32Slice(raw []byte, dst []float32) ([]float32, error) {
	if len(raw)%4 != 0 {
		return nil, errors.New("collections: malformed template-v1 raw float32 bytes")
	}
	if len(raw) == 0 {
		return nil, nil
	}
	count := len(raw) / 4
	if cap(dst) < count {
		dst = make([]float32, count)
	} else {
		dst = dst[:count]
	}
	for i := range dst {
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return dst, nil
}
