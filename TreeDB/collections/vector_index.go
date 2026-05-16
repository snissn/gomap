package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"gonum.org/v1/gonum/blas/blas32"
)

const (
	defaultVectorIndexM              = 16
	defaultVectorIndexEfConstruction = 128
	defaultVectorIndexEfSearch       = 64
	defaultVectorIndexFetchMultiple  = 4
	defaultVectorIndexRebuildPPM     = 250_000
	defaultVectorIndexExactFilterMax = 1024
)

// VectorIndexEncoding selects the process-local ANN vector copy format. The
// collection row remains canonical and exact reranking always reads the full
// precision vector from TreeDB.
type VectorIndexEncoding uint8

const (
	VectorIndexEncodingFloat32 VectorIndexEncoding = iota
	VectorIndexEncodingInt8
)

func (e VectorIndexEncoding) String() string {
	switch e {
	case VectorIndexEncodingFloat32:
		return "float32"
	case VectorIndexEncodingInt8:
		return "int8"
	default:
		return fmt.Sprintf("unknown(%d)", e)
	}
}

func (e VectorIndexEncoding) MarshalJSON() ([]byte, error) {
	encoding, err := normalizeVectorIndexEncoding(e)
	if err != nil {
		return nil, err
	}
	return json.Marshal(encoding.String())
}

func (e *VectorIndexEncoding) UnmarshalJSON(raw []byte) error {
	if e == nil {
		return errors.New("collections: nil vector index encoding")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		encoding, err := parseVectorIndexEncoding(s)
		if err != nil {
			return err
		}
		*e = encoding
		return nil
	}
	var n uint8
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	encoding, err := normalizeVectorIndexEncoding(VectorIndexEncoding(n))
	if err != nil {
		return err
	}
	*e = encoding
	return nil
}

// VectorIndexOptions configures an in-memory vector secondary index built from
// collection rows. The index stores stable collection document IDs and vector
// copies for graph search; TreeDB collection rows remain canonical for final
// exact reranking.
type VectorIndexOptions struct {
	Name                string
	Field               string
	Metric              VectorMetric
	Dimensions          int
	M                   int
	EfConstruction      int
	EfSearch            int
	RebuildDeletedRatio float64
	Encoding            VectorIndexEncoding
}

// VectorIndexSearchOptions configures one in-memory vector index search.
type VectorIndexSearchOptions struct {
	TopK                 int
	EfSearch             int
	FetchMultiplier      int
	Filter               func(DocumentRecord) (bool, error)
	IndexRangeFilter     *VectorIndexRangeFilter
	ExactFilterMaxDocs   int
	DisableExactFallback bool
}

// VectorIndexTrace reports how one vector-index search was executed.
type VectorIndexTrace struct {
	Strategy                 string
	EfSearch                 int
	FetchMultiplier          int
	CandidatesExamined       int
	CandidatesAfterTombstone int
	CandidatesAfterFilter    int
	RerankCount              int
	ReturnedCount            int
	ExactFallbackReason      string
}

// VectorIndexStats reports process-local in-memory vector index state.
type VectorIndexStats struct {
	Name                string
	Field               string
	Metric              VectorMetric
	Encoding            VectorIndexEncoding
	Dimensions          int
	M                   int
	EfConstruction      int
	EfSearch            int
	Nodes               int
	LiveDocs            int
	DeletedDocs         int
	DeletedRatio        float64
	BytesMemory         int64
	BytesDisk           int64
	AvgDegree           float64
	MaxLevel            int
	Epoch               uint64
	SnapshotDirty       bool
	LastRebuildDuration time.Duration
	RebuildNeeded       bool
}

// VectorIndexRecall reports ANN overlap with exact search for sampled queries.
type VectorIndexRecall struct {
	Queries      int
	TopK         int
	ExactTotal   int
	ANNTotal     int
	Overlap      int
	Recall       float64
	SearchTraces []VectorIndexTrace
}

// VectorIndex is the process-local runtime graph for collection vector fields.
// Declared collection vector indexes can persist this runtime graph into a
// TreeDB-managed collection root; ad hoc indexes can still be rebuilt from
// primary collection rows.
type VectorIndex struct {
	collection *Collection

	name                string
	field               string
	fieldPath           []string
	metric              VectorMetric
	encoding            VectorIndexEncoding
	dimensions          int
	m                   int
	efConstruction      int
	efSearch            int
	rebuildDeletedRatio float64

	mu            sync.RWMutex
	nodes         []vectorIndexNode
	currentNode   map[string]int
	entry         int
	maxLevel      int
	insertScratch vectorIndexSearchScratch
	searchScratch sync.Pool

	mutationSeq            uint64
	persistedEpoch         uint64
	persistedBytesDisk     int64
	persistedSnapshotDirty bool
	nativePersistent       bool
	dirtyMeta              bool
	dirtyNodes             map[int]struct{}
	dirtyDocs              map[string]struct{}
	lastRebuildDuration    time.Duration
}

type vectorIndexNode struct {
	documentID    []byte
	vector        []float32
	quantized     []int8
	quantScale    float32
	normSquared   float64
	cachedInvNorm float32
	level         int
	neighbors     [][]vectorIndexNeighbor
	deleted       bool
}

type vectorIndexNeighbor struct {
	nodeID   int
	distance float32
}

// BuildVectorIndex builds an in-memory vector secondary index from the current
// live collection rows.
func (c *Collection) BuildVectorIndex(opts VectorIndexOptions) (*VectorIndex, error) {
	return c.buildVectorIndex(opts, true)
}

func (c *Collection) buildVectorIndex(opts VectorIndexOptions, register bool) (*VectorIndex, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	index, err := newVectorIndex(c, opts)
	if err != nil {
		return nil, err
	}
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}
	index.setNativePersistent(collectionMetaDeclaresVectorIndex(c.meta, index.name))
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()

	_, err = c.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		vector, ok, err := vectorFromStoredDocument(materializer, record.Document, index.fieldPath)
		if err != nil {
			return false, fmt.Errorf("collections: vector field %q in document %q: %w", index.field, record.ID, err)
		}
		if !ok {
			return true, nil
		}
		if err := index.insertVectorLocked(record.ID, vector); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if register {
		c.RegisterVectorIndex(index)
	}
	return index, nil
}

func newVectorIndex(c *Collection, opts VectorIndexOptions) (*VectorIndex, error) {
	if opts.Name == "" {
		opts.Name = vectorIndexDefaultName(opts.Field)
	}
	fieldPath, err := parseVectorFieldPath(opts.Field)
	if err != nil {
		return nil, err
	}
	metric, err := normalizeVectorMetric(opts.Metric)
	if err != nil {
		return nil, err
	}
	encoding, err := normalizeVectorIndexEncoding(opts.Encoding)
	if err != nil {
		return nil, err
	}
	if opts.Dimensions < 0 {
		return nil, errors.New("collections: vector index dimensions cannot be negative")
	}
	m := opts.M
	if m <= 0 {
		m = defaultVectorIndexM
	}
	efConstruction := opts.EfConstruction
	if efConstruction <= 0 {
		efConstruction = defaultVectorIndexEfConstruction
	}
	if efConstruction < m {
		efConstruction = m
	}
	efSearch := opts.EfSearch
	if efSearch <= 0 {
		efSearch = defaultVectorIndexEfSearch
	}
	rebuildRatio := opts.RebuildDeletedRatio
	if rebuildRatio <= 0 {
		rebuildRatio = float64(defaultVectorIndexRebuildPPM) / 1_000_000
	}
	if rebuildRatio > 1 {
		return nil, errors.New("collections: vector index rebuild deleted ratio cannot exceed 1")
	}
	return &VectorIndex{
		collection:          c,
		name:                opts.Name,
		field:               opts.Field,
		fieldPath:           fieldPath,
		metric:              metric,
		encoding:            encoding,
		dimensions:          opts.Dimensions,
		m:                   m,
		efConstruction:      efConstruction,
		efSearch:            efSearch,
		rebuildDeletedRatio: rebuildRatio,
		currentNode:         make(map[string]int),
		entry:               -1,
		maxLevel:            -1,
	}, nil
}

func normalizeVectorIndexEncoding(encoding VectorIndexEncoding) (VectorIndexEncoding, error) {
	switch encoding {
	case VectorIndexEncodingFloat32, VectorIndexEncodingInt8:
		return encoding, nil
	default:
		return VectorIndexEncodingFloat32, fmt.Errorf("collections: unsupported vector index encoding %d", encoding)
	}
}

func parseVectorIndexEncoding(value string) (VectorIndexEncoding, error) {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "float32":
		return VectorIndexEncodingFloat32, nil
	case "int8":
		return VectorIndexEncodingInt8, nil
	default:
		return VectorIndexEncodingFloat32, fmt.Errorf("collections: unsupported vector index encoding %q", value)
	}
}

// RegisterVectorIndex attaches an in-memory vector index to this collection so
// successful collection inserts, updates, and deletes keep the index in sync.
func (c *Collection) RegisterVectorIndex(index *VectorIndex) {
	if c == nil || index == nil {
		return
	}
	c.vectorIndexesMu.Lock()
	defer c.vectorIndexesMu.Unlock()
	if c.vectorIndexes == nil {
		c.vectorIndexes = make(map[string]*VectorIndex)
	}
	index.collection = c
	index.setNativePersistent(collectionMetaDeclaresVectorIndex(c.meta, index.name))
	c.vectorIndexes[index.name] = index
}

// UnregisterVectorIndex detaches a registered in-memory vector index.
func (c *Collection) UnregisterVectorIndex(name string) {
	if c == nil {
		return
	}
	c.vectorIndexesMu.Lock()
	defer c.vectorIndexesMu.Unlock()
	delete(c.vectorIndexes, name)
	empty := len(c.vectorIndexes) == 0
	if empty && c.manager != nil {
		c.manager.unregisterCollectionHandle(c)
	}
}

func (c *Collection) registeredVectorIndexes() []*VectorIndex {
	if c == nil {
		return nil
	}
	c.vectorIndexesMu.RLock()
	defer c.vectorIndexesMu.RUnlock()
	if len(c.vectorIndexes) == 0 {
		return nil
	}
	out := make([]*VectorIndex, 0, len(c.vectorIndexes))
	for _, index := range c.vectorIndexes {
		out = append(out, index)
	}
	return out
}

func (c *Collection) hasRegisteredVectorIndex(name string) bool {
	if c == nil || name == "" {
		return false
	}
	c.vectorIndexesMu.RLock()
	defer c.vectorIndexesMu.RUnlock()
	_, ok := c.vectorIndexes[name]
	return ok
}

func (c *Collection) ensureDeclaredNativeVectorIndexesLoaded() error {
	if c == nil || len(c.meta.VectorIndexes) == 0 {
		return nil
	}
	c.vectorIndexLoadMu.Lock()
	defer c.vectorIndexLoadMu.Unlock()
	for _, def := range c.meta.VectorIndexes {
		if c.hasRegisteredVectorIndex(def.Name) {
			continue
		}
		index, status, err := c.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			return err
		}
		if index != nil {
			continue
		}
		if status.ExactFallbackReason == "missing_graph_root" {
			if _, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Collection) notifyVectorIndexesUpsert(documentIDs [][]byte) error {
	if len(documentIDs) == 0 {
		return nil
	}
	if err := c.ensureDeclaredNativeVectorIndexesLoaded(); err != nil {
		return err
	}
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return nil
	}
	if c.manager != nil {
		c.manager.registerCollectionHandle(c)
	}
	for _, index := range indexes {
		for _, documentID := range documentIDs {
			if len(documentID) == 0 {
				continue
			}
			if err := index.InsertDocument(documentID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Collection) notifyVectorIndexesDelete(documentIDs [][]byte) error {
	if len(documentIDs) == 0 {
		return nil
	}
	if err := c.ensureDeclaredNativeVectorIndexesLoaded(); err != nil {
		return err
	}
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return nil
	}
	if c.manager != nil {
		c.manager.registerCollectionHandle(c)
	}
	for _, index := range indexes {
		for _, documentID := range documentIDs {
			index.TombstoneDocumentID(documentID)
		}
	}
	return nil
}

func (c *Collection) notifyVectorIndexesUpdateBatch(items []UpdateBatchItem, results []UpdateBatchResult) error {
	if len(items) == 0 || len(results) == 0 {
		return nil
	}
	var updated [][]byte
	for i := range items {
		if i >= len(results) {
			break
		}
		if results[i].Modified {
			updated = append(updated, items[i].DocumentID)
		}
	}
	return c.notifyVectorIndexesUpsert(updated)
}

func (c *Collection) notifyVectorIndexesBSONSetUpdateBatch(items []BSONSetUpdateBatchItem, results []UpdateBatchResult) error {
	if len(items) == 0 || len(results) == 0 {
		return nil
	}
	var updated [][]byte
	for i := range items {
		if i >= len(results) {
			break
		}
		if results[i].Modified {
			updated = append(updated, items[i].DocumentID)
		}
	}
	return c.notifyVectorIndexesUpsert(updated)
}

func (c *Collection) persistNativeVectorIndexIfDeclared(index *VectorIndex) error {
	if c == nil || index == nil || !index.needsNativeAutoPersist() {
		return nil
	}
	if !collectionMetaDeclaresVectorIndex(c.meta, index.name) {
		declared, err := c.refreshVectorIndexDeclaration(index.name)
		if err != nil || !declared {
			return err
		}
	}
	if !index.isNativePersistent() || !index.hasNativePersistedSnapshot() {
		_, err := index.SaveNativeSnapshot()
		if errors.Is(err, errVectorIndexNotDeclared) {
			return nil
		}
		return err
	}
	_, err := index.SaveNativeDeltaSnapshot()
	if errors.Is(err, errVectorIndexNotDeclared) {
		return nil
	}
	return err
}

func (c *Collection) refreshVectorIndexDeclaration(name string) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil || catalog == nil {
		return false, err
	}
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	return collectionMetaDeclaresVectorIndex(catalog.meta, name), nil
}

func (c *Collection) persistDirtyNativeVectorIndexes() error {
	indexes := c.registeredVectorIndexes()
	for _, index := range indexes {
		if err := c.persistNativeVectorIndexIfDeclared(index); err != nil {
			return err
		}
	}
	if c.manager != nil && !c.hasDirtyNativeVectorIndex() {
		c.manager.unregisterCollectionHandle(c)
	}
	return nil
}

func (c *Collection) hasDirtyNativeVectorIndex() bool {
	for _, index := range c.registeredVectorIndexes() {
		if index.needsNativeAutoPersist() {
			return true
		}
	}
	return false
}

func collectionMetaDeclaresVectorIndex(meta CollectionMeta, name string) bool {
	if name == "" {
		return false
	}
	_, ok := findVectorIndex(meta.VectorIndexes, name)
	return ok
}

// InsertDocument adds or replaces one committed collection document in the
// in-memory index. Missing or null vector fields leave the document unindexed
// and tombstone any previous indexed version.
func (idx *VectorIndex) InsertDocument(documentID []byte) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	document, err := idx.collection.Get(documentID)
	if err != nil {
		return err
	}
	if document == nil {
		idx.TombstoneDocumentID(documentID)
		return nil
	}
	materializer, err := idx.collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return err
	}
	defer func() { _ = materializer.Close() }()
	vector, ok, err := vectorFromStoredDocument(materializer, document, idx.fieldPath)
	if err != nil {
		return fmt.Errorf("collections: vector field %q in document %q: %w", idx.field, documentID, err)
	}
	if !ok {
		idx.TombstoneDocumentID(documentID)
		return nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.insertVectorLocked(documentID, vector)
}

// TombstoneDocumentID marks the current indexed version of documentID deleted.
// Tombstoned nodes remain in the graph until the caller rebuilds the index.
func (idx *VectorIndex) TombstoneDocumentID(documentID []byte) {
	if idx == nil || len(documentID) == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tombstoneDocumentIDLocked(documentID)
}

func (idx *VectorIndex) insertVectorLocked(documentID []byte, vector []float32) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	if len(vector) == 0 {
		return errors.New("collections: vector cannot be empty")
	}
	if err := validateFloat32Vector(vector); err != nil {
		return err
	}
	vectorNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		vectorNorm = vectorNormSquared(vector)
		if vectorNorm == 0 {
			return errors.New("collections: cosine vector cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(vector, vectorNorm)
		if err != nil {
			return err
		}
		prepared = &preparedQuery
	}
	if idx.dimensions == 0 {
		idx.dimensions = len(vector)
		idx.markVectorMetaDirtyLocked()
	} else if len(vector) != idx.dimensions {
		return fmt.Errorf("collections: vector field %q in document %q has dimension %d, want %d", idx.field, documentID, len(vector), idx.dimensions)
	}
	if nodeID, ok := idx.currentNode[string(documentID)]; ok && nodeID >= 0 && nodeID < len(idx.nodes) {
		node := &idx.nodes[nodeID]
		if !node.deleted && node.matchesVector(vector, idx.encoding) {
			return nil
		}
	}
	idx.tombstoneDocumentIDLocked(documentID)

	nodeID := len(idx.nodes)
	level := idx.levelForDocumentID(documentID)
	node := idx.newVectorIndexNode(documentID, vector, level)
	idx.nodes = append(idx.nodes, node)
	idx.markGraphChangedLocked()
	idx.markVectorNodeDirtyLocked(nodeID)
	idx.markVectorDocDirtyLocked(documentID)
	idx.currentNode[string(documentID)] = nodeID
	if idx.entry < 0 {
		idx.entry = nodeID
		idx.maxLevel = level
		idx.markVectorMetaDirtyLocked()
		return nil
	}
	entryPoint := idx.entry
	for layer := idx.maxLevel; layer > level; layer-- {
		entryPoint = idx.greedyNearestAtLayerLocked(vector, vectorNorm, prepared, entryPoint, layer)
	}
	for layer := minInt(level, idx.maxLevel); layer >= 0; layer-- {
		candidates := idx.searchLayerWithScratchLocked(vector, vectorNorm, prepared, entryPoint, idx.efConstruction, layer, &idx.insertScratch)
		neighbors := idx.selectLayerNeighborsLocked(vector, vectorNorm, prepared, candidates, layer, idx.maxNeighborsForLayer(layer), nodeID)
		for _, neighborID := range neighbors {
			idx.linkLayerLocked(nodeID, neighborID, layer)
			idx.linkLayerLocked(neighborID, nodeID, layer)
		}
		if len(neighbors) > 0 {
			entryPoint = neighbors[0]
		}
	}
	if level > idx.maxLevel {
		idx.entry = nodeID
		idx.maxLevel = level
		idx.markVectorMetaDirtyLocked()
	}
	return nil
}

func (node *vectorIndexNode) matchesVector(vector []float32, encoding VectorIndexEncoding) bool {
	if node == nil {
		return false
	}
	switch encoding {
	case VectorIndexEncodingInt8:
		quantized, quantScale := quantizeVectorIndexInt8(vector)
		return node.quantScale == quantScale && slices.Equal(node.quantized, quantized)
	default:
		return slices.Equal(node.vector, vector)
	}
}

func (idx *VectorIndex) newVectorIndexNode(documentID []byte, vector []float32, level int) vectorIndexNode {
	node := vectorIndexNode{
		documentID: bytes.Clone(documentID),
		level:      level,
		neighbors:  make([][]vectorIndexNeighbor, level+1),
	}
	switch idx.encoding {
	case VectorIndexEncodingInt8:
		node.quantized, node.quantScale = quantizeVectorIndexInt8(vector)
	default:
		node.vector = append([]float32(nil), vector...)
	}
	node.cacheVectorNorms()
	return node
}

func quantizeVectorIndexInt8(vector []float32) ([]int8, float32) {
	var maxAbs float32
	for _, value := range vector {
		abs := value
		if abs < 0 {
			abs = -abs
		}
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	scale := maxAbs / 127
	if scale == 0 {
		scale = 1
	}
	out := make([]int8, len(vector))
	for i, value := range vector {
		q := int(math.Round(float64(value / scale)))
		if q > 127 {
			q = 127
		} else if q < -127 {
			q = -127
		}
		out[i] = int8(q)
	}
	return out, scale
}

func (idx *VectorIndex) tombstoneDocumentIDLocked(documentID []byte) {
	nodeID, ok := idx.currentNode[string(documentID)]
	if !ok {
		return
	}
	if nodeID >= 0 && nodeID < len(idx.nodes) {
		idx.nodes[nodeID].deleted = true
		idx.markVectorNodeDirtyLocked(nodeID)
	}
	delete(idx.currentNode, string(documentID))
	idx.markVectorDocDirtyLocked(documentID)
	idx.markGraphChangedLocked()
	if idx.entry == nodeID {
		idx.entry = idx.firstLiveNodeLocked()
		idx.maxLevel = idx.maxLiveLevelLocked()
		if idx.entry >= 0 {
			idx.entry = idx.firstLiveNodeAtLevelLocked(idx.maxLevel)
		}
		idx.markVectorMetaDirtyLocked()
	}
}

func (idx *VectorIndex) firstLiveNodeLocked() int {
	for i := range idx.nodes {
		if !idx.nodes[i].deleted {
			return i
		}
	}
	return -1
}

func (idx *VectorIndex) maxLiveLevelLocked() int {
	maxLevel := -1
	for i := range idx.nodes {
		if !idx.nodes[i].deleted && idx.nodes[i].level > maxLevel {
			maxLevel = idx.nodes[i].level
		}
	}
	return maxLevel
}

func (idx *VectorIndex) firstLiveNodeAtLevelLocked(level int) int {
	for i := range idx.nodes {
		if !idx.nodes[i].deleted && idx.nodes[i].level >= level {
			return i
		}
	}
	return -1
}

func (idx *VectorIndex) levelForDocumentID(documentID []byte) int {
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], xxhash.Sum64(documentID)^xxhash.Sum64String(idx.name))
	hash := xxhash.Sum64(seed[:])
	level := 0
	for level < 32 && hash&0x3 == 0 {
		level++
		hash >>= 2
	}
	return level
}

func (idx *VectorIndex) markGraphChangedLocked() {
	idx.mutationSeq++
	if idx.persistedEpoch != 0 {
		idx.persistedSnapshotDirty = true
	}
}

func (idx *VectorIndex) setNativePersistent(enabled bool) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.nativePersistent = enabled
	idx.mu.Unlock()
}

func (idx *VectorIndex) isNativePersistent() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.nativePersistent
}

func (idx *VectorIndex) hasNativePersistedSnapshot() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.persistedEpoch != 0
}

func (idx *VectorIndex) markVectorMetaDirtyLocked() {
	if !idx.nativePersistent {
		return
	}
	idx.dirtyMeta = true
}

func (idx *VectorIndex) markVectorNodeDirtyLocked(nodeID int) {
	if !idx.nativePersistent || nodeID < 0 {
		return
	}
	if idx.dirtyNodes == nil {
		idx.dirtyNodes = make(map[int]struct{})
	}
	idx.dirtyNodes[nodeID] = struct{}{}
}

func (idx *VectorIndex) markVectorDocDirtyLocked(documentID []byte) {
	if !idx.nativePersistent || len(documentID) == 0 {
		return
	}
	if idx.dirtyDocs == nil {
		idx.dirtyDocs = make(map[string]struct{})
	}
	idx.dirtyDocs[string(documentID)] = struct{}{}
}

func (idx *VectorIndex) maxNeighborsForLayer(layer int) int {
	if layer == 0 {
		return maxInt(idx.m*2, idx.m)
	}
	return idx.m
}

func normalizeVectorIndexEdgeDistance(distance float32) (float32, bool) {
	if math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 1) {
		return 0, false
	}
	if math.IsInf(float64(distance), -1) {
		return -math.MaxFloat32, true
	}
	return distance, true
}

func (idx *VectorIndex) linkLayerLocked(fromNodeID, toNodeID, layer int) {
	if fromNodeID < 0 || fromNodeID >= len(idx.nodes) {
		return
	}
	if layer < 0 || layer > idx.nodes[fromNodeID].level {
		return
	}
	neighbors := idx.nodes[fromNodeID].neighbors[layer]
	for _, existing := range neighbors {
		if existing.nodeID == toNodeID {
			return
		}
	}
	distance := idx.distanceBetweenNodesLocked(fromNodeID, toNodeID)
	var ok bool
	distance, ok = normalizeVectorIndexEdgeDistance(distance)
	if !ok {
		return
	}
	neighbors = append(neighbors, vectorIndexNeighbor{nodeID: toNodeID, distance: distance})
	limit := idx.maxNeighborsForLayer(layer)
	if len(neighbors) > limit {
		neighbors = idx.pruneLayerNeighborsLocked(fromNodeID, neighbors, limit)
	}
	idx.nodes[fromNodeID].neighbors[layer] = neighbors
	idx.markVectorNodeDirtyLocked(fromNodeID)
}

func (idx *VectorIndex) pruneLayerNeighborsLocked(_ int, neighbors []vectorIndexNeighbor, limit int) []vectorIndexNeighbor {
	if limit <= 0 || len(neighbors) == 0 {
		return nil
	}
	if len(neighbors) <= limit {
		return neighbors
	}
	var stack [128]vectorIndexCandidate
	scored := stack[:0]
	if len(neighbors) > len(stack) {
		scored = make([]vectorIndexCandidate, 0, len(neighbors))
	}
	for _, neighbor := range neighbors {
		neighborID := neighbor.nodeID
		if neighborID < 0 || neighborID >= len(idx.nodes) {
			continue
		}
		distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
		if !ok {
			continue
		}
		scored = append(scored, vectorIndexCandidate{nodeID: neighborID, distance: distance})
	}
	slices.SortFunc(scored, func(left, right vectorIndexCandidate) int {
		if left.distance < right.distance {
			return -1
		}
		if left.distance > right.distance {
			return 1
		}
		return bytes.Compare(idx.nodes[left.nodeID].documentID, idx.nodes[right.nodeID].documentID)
	})
	if len(scored) > limit {
		scored = scored[:limit]
	}
	out := neighbors[:0]
	for _, candidate := range scored {
		out = append(out, vectorIndexNeighbor{nodeID: candidate.nodeID, distance: candidate.distance})
	}
	return out
}

// Search returns ANN candidates from the in-memory graph and exact-reranks the
// final result set from canonical collection rows. If graph search underfills
// and DisableExactFallback is false, it falls back to the exact scan API.
func (idx *VectorIndex) Search(query []float32, opts VectorIndexSearchOptions) ([]VectorSearchResult, VectorIndexTrace, error) {
	trace := VectorIndexTrace{Strategy: "ann_graph"}
	if idx == nil {
		return nil, trace, errors.New("collections: vector index is nil")
	}
	if opts.TopK <= 0 {
		return nil, trace, errors.New("collections: vector search TopK must be positive")
	}
	if len(query) == 0 {
		return nil, trace, errors.New("collections: vector query cannot be empty")
	}
	if err := validateFloat32Vector(query); err != nil {
		return nil, trace, fmt.Errorf("collections: vector query: %w", err)
	}
	queryNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		queryNorm = vectorNormSquared(query)
		if queryNorm == 0 {
			return nil, trace, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(query, queryNorm)
		if err != nil {
			return nil, trace, err
		}
		prepared = &preparedQuery
	}
	idx.mu.RLock()
	if idx.dimensions != 0 && len(query) != idx.dimensions {
		dims := idx.dimensions
		idx.mu.RUnlock()
		return nil, trace, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), dims)
	}
	ef := opts.EfSearch
	if ef <= 0 {
		ef = idx.efSearch
	}
	fetchMultiplier := opts.FetchMultiplier
	if fetchMultiplier <= 0 {
		fetchMultiplier = defaultVectorIndexFetchMultiple
	}
	candidateLimit := opts.TopK * fetchMultiplier
	if candidateLimit < ef {
		candidateLimit = ef
	}
	if candidateLimit < opts.TopK {
		candidateLimit = opts.TopK
	}
	trace.EfSearch = ef
	trace.FetchMultiplier = fetchMultiplier
	idx.mu.RUnlock()

	var rangeIDs [][]byte
	var rangeFilter func(DocumentRecord) (bool, error)
	if opts.IndexRangeFilter != nil {
		exactFilterMax := opts.ExactFilterMaxDocs
		if exactFilterMax <= 0 {
			exactFilterMax = defaultVectorIndexExactFilterMax
		}
		probeIDs, truncated, err := idx.collection.vectorSearchIndexRangeDocumentIDs(opts.IndexRangeFilter, exactFilterMax+1)
		if err != nil {
			return nil, trace, err
		}
		if !truncated && len(probeIDs) <= exactFilterMax {
			trace.Strategy = "exact_filtered"
			trace.CandidatesExamined = len(probeIDs)
			trace.CandidatesAfterTombstone = len(probeIDs)
			results, err := idx.rerankCandidates(query, probeIDs, opts.Filter, &trace)
			if err != nil {
				return nil, trace, err
			}
			sortVectorSearchResults(results)
			if len(results) > opts.TopK {
				results = results[:opts.TopK]
			}
			trace.ReturnedCount = len(results)
			return results, trace, nil
		}
		rangeIDs, _, err = idx.collection.vectorSearchIndexRangeDocumentIDs(opts.IndexRangeFilter, 0)
		if err != nil {
			return nil, trace, err
		}
		allowed := vectorDocumentIDSet(rangeIDs)
		rangeFilter = func(record DocumentRecord) (bool, error) {
			if _, ok := allowed[string(record.ID)]; !ok {
				return false, nil
			}
			if opts.Filter == nil {
				return true, nil
			}
			return opts.Filter(record)
		}
		trace.Strategy = "ann_postfilter"
	}
	idx.mu.RLock()
	scratch := idx.getSearchScratch()
	candidates := idx.searchCandidatesLocked(query, queryNorm, prepared, candidateLimit, scratch)
	trace.CandidatesExamined = len(candidates)
	candidateIDs := make([][]byte, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		node := idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		candidateIDs = append(candidateIDs, bytes.Clone(node.documentID))
	}
	idx.putSearchScratch(scratch)
	idx.mu.RUnlock()
	trace.CandidatesAfterTombstone = len(candidateIDs)

	filter := opts.Filter
	if rangeFilter != nil {
		filter = rangeFilter
	}
	results, err := idx.rerankCandidates(query, candidateIDs, filter, &trace)
	if err != nil {
		return nil, trace, err
	}
	sortVectorSearchResults(results)
	if len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	trace.ReturnedCount = len(results)
	if len(results) < opts.TopK && !opts.DisableExactFallback {
		if opts.IndexRangeFilter != nil {
			trace.Strategy = "ann_postfilter_exact_fallback"
		} else {
			trace.Strategy = "ann_graph_exact_fallback"
		}
		trace.ExactFallbackReason = "underfilled_results"
		exact, err := idx.collection.SearchVectorsExact(query, VectorSearchOptions{
			Field:            idx.field,
			Metric:           idx.metric,
			TopK:             opts.TopK,
			Filter:           opts.Filter,
			IndexRangeFilter: opts.IndexRangeFilter,
		})
		if err != nil {
			return nil, trace, err
		}
		trace.ReturnedCount = len(exact)
		return exact, trace, nil
	}
	return results, trace, nil
}

func (idx *VectorIndex) searchGraphOnly(query []float32, topK, efSearch int) ([]VectorSearchResult, error) {
	if idx == nil {
		return nil, errors.New("collections: vector index is nil")
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
	queryNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		queryNorm = vectorNormSquared(query)
		if queryNorm == 0 {
			return nil, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(query, queryNorm)
		if err != nil {
			return nil, err
		}
		prepared = &preparedQuery
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.dimensions != 0 && len(query) != idx.dimensions {
		return nil, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), idx.dimensions)
	}
	limit := efSearch
	if limit <= 0 {
		limit = idx.efSearch
	}
	if limit < topK {
		limit = topK
	}
	scratch := idx.getSearchScratch()
	candidates := idx.searchCandidatesLocked(query, queryNorm, prepared, limit, scratch)
	results := make([]VectorSearchResult, 0, minInt(topK, len(candidates)))
	for _, candidate := range candidates {
		if len(results) >= topK {
			break
		}
		if candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		node := idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		results = append(results, VectorSearchResult{
			DocumentID: bytes.Clone(node.documentID),
			Distance:   candidate.distance,
		})
	}
	idx.putSearchScratch(scratch)
	return results, nil
}

func vectorDocumentIDSet(ids [][]byte) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[string(id)] = struct{}{}
	}
	return out
}

func (idx *VectorIndex) rerankCandidates(query []float32, candidateIDs [][]byte, filter func(DocumentRecord) (bool, error), trace *VectorIndexTrace) ([]VectorSearchResult, error) {
	materializer, err := idx.collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()

	results := make([]VectorSearchResult, 0, len(candidateIDs))
	for _, documentID := range candidateIDs {
		document, err := idx.collection.Get(documentID)
		if err != nil {
			return nil, err
		}
		if document == nil {
			continue
		}
		record := DocumentRecord{ID: bytes.Clone(documentID), Document: document}
		if filter != nil {
			include, err := filter(record)
			if err != nil {
				return nil, err
			}
			if !include {
				continue
			}
		}
		if trace != nil {
			trace.CandidatesAfterFilter++
		}
		vector, ok, err := vectorFromStoredDocument(materializer, document, idx.fieldPath)
		if err != nil {
			return nil, fmt.Errorf("collections: vector field %q in document %q: %w", idx.field, documentID, err)
		}
		if !ok || len(vector) != len(query) {
			continue
		}
		distance, err := exactVectorDistance(query, vector, idx.metric)
		if err != nil {
			return nil, err
		}
		if trace != nil {
			trace.RerankCount++
		}
		results = append(results, VectorSearchResult{
			DocumentID: bytes.Clone(documentID),
			Distance:   distance,
			Document:   bytes.Clone(document),
		})
	}
	return results, nil
}

func (idx *VectorIndex) searchCandidatesLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if idx.entry < 0 || len(idx.nodes) == 0 || limit <= 0 {
		return nil
	}
	entryPoint := idx.entry
	for layer := idx.maxLevel; layer > 0; layer-- {
		entryPoint = idx.greedyNearestAtLayerLocked(query, queryNormSquared, prepared, entryPoint, layer)
	}
	return idx.searchLayerWithScratchLocked(query, queryNormSquared, prepared, entryPoint, limit, 0, scratch)
}

func (idx *VectorIndex) greedyNearestAtLayerLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, layer int) int {
	if entryPoint < 0 {
		return entryPoint
	}
	best := entryPoint
	bestDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, best)
	changed := true
	for changed {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best, layer) {
			neighborID := neighbor.nodeID
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID)
			if distance < bestDistance {
				best = neighborID
				bestDistance = distance
				changed = true
			}
		}
	}
	return best
}

func (idx *VectorIndex) searchLayerLocked(query []float32, queryNormSquared float64, entryPoint int, limit int, layer int) []vectorIndexCandidate {
	return idx.searchLayerWithScratchLocked(query, queryNormSquared, nil, entryPoint, limit, layer, nil)
}

func (idx *VectorIndex) getSearchScratch() *vectorIndexSearchScratch {
	if scratch, ok := idx.searchScratch.Get().(*vectorIndexSearchScratch); ok {
		return scratch
	}
	return &vectorIndexSearchScratch{}
}

func (idx *VectorIndex) putSearchScratch(scratch *vectorIndexSearchScratch) {
	if scratch == nil {
		return
	}
	idx.searchScratch.Put(scratch)
}

func (idx *VectorIndex) searchLayerWithScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if entryPoint < 0 || entryPoint >= len(idx.nodes) || limit <= 0 {
		return nil
	}
	entryDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, entryPoint)
	if math.IsInf(float64(entryDistance), 1) {
		return nil
	}
	if scratch == nil {
		scratch = &vectorIndexSearchScratch{}
	}
	visited, mark := scratch.nextVisitedEpoch(len(idx.nodes))
	visited[entryPoint] = mark
	entry := vectorIndexCandidate{nodeID: entryPoint, distance: entryDistance}
	queue := scratch.queue[:0]
	queue.push(entry)
	best := scratch.best[:0]
	best.pushBounded(entry, limit)
	for len(queue) > 0 {
		current := queue.pop()
		if len(best) >= limit && vectorIndexCandidateWorse(current, best[0]) {
			break
		}
		if current.nodeID < 0 || current.nodeID >= len(idx.nodes) {
			continue
		}
		for _, neighbor := range idx.layerNeighborsLocked(current.nodeID, layer) {
			neighborID := neighbor.nodeID
			if neighborID < 0 || neighborID >= len(idx.nodes) || visited[neighborID] == mark {
				continue
			}
			visited[neighborID] = mark
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID)
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
	scratch.queue = queue[:0]
	scratch.best = best[:0]
	out := append(scratch.out[:0], best...)
	scratch.out = out
	sortVectorIndexCandidates(out)
	return out
}

func (idx *VectorIndex) layerNeighborsLocked(nodeID int, layer int) []vectorIndexNeighbor {
	if nodeID < 0 || nodeID >= len(idx.nodes) {
		return nil
	}
	node := idx.nodes[nodeID]
	if layer < 0 || layer >= len(node.neighbors) {
		return nil
	}
	return node.neighbors[layer]
}

func (idx *VectorIndex) selectLayerNeighborsLocked(vector []float32, vectorNormSquared float64, prepared *preparedFloat32CosineQuery, candidates []vectorIndexCandidate, layer, limit, excludeNodeID int) []int {
	if limit <= 0 {
		return nil
	}
	scored := candidates[:0]
	for _, candidate := range candidates {
		if candidate.nodeID == excludeNodeID || candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		if idx.nodes[candidate.nodeID].level < layer {
			continue
		}
		if math.IsInf(float64(candidate.distance), 1) {
			continue
		}
		scored = append(scored, candidate)
	}
	sortVectorIndexCandidates(scored)
	if len(scored) > limit {
		scored = scored[:limit]
	}
	out := make([]int, len(scored))
	for i := range scored {
		out[i] = scored[i].nodeID
	}
	return out
}

func (idx *VectorIndex) distanceToNodeLocked(query []float32, nodeID int) float32 {
	return idx.distanceToNodeWithQueryNormLocked(query, -1, nodeID)
}

func (idx *VectorIndex) distanceToNodeWithQueryNormLocked(query []float32, queryNormSquared float64, nodeID int) float32 {
	return idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, nil, nodeID)
}

func (idx *VectorIndex) distanceToNodeWithPreparedQueryLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, nodeID int) float32 {
	if nodeID < 0 || nodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	node := &idx.nodes[nodeID]
	if prepared != nil && idx.metric == VectorMetricCosine && len(node.vector) > 0 {
		return vectorDistanceToFloat32NodeCosineUnchecked(*prepared, node)
	}
	distance, err := vectorDistanceToStoredNodeWithQueryNorm(query, queryNormSquared, node, idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func (idx *VectorIndex) distanceBetweenNodesLocked(leftNodeID, rightNodeID int) float32 {
	if leftNodeID < 0 || leftNodeID >= len(idx.nodes) || rightNodeID < 0 || rightNodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	distance, err := vectorDistanceBetweenStoredNodes(&idx.nodes[leftNodeID], &idx.nodes[rightNodeID], idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func vectorDistanceToStoredNode(query []float32, node *vectorIndexNode, metric VectorMetric) (float32, error) {
	return vectorDistanceToStoredNodeWithQueryNorm(query, -1, node, metric)
}

func vectorDistanceToStoredNodeWithQueryNorm(query []float32, queryNormSquared float64, node *vectorIndexNode, metric VectorMetric) (float32, error) {
	dims := node.vectorDimensions()
	if len(query) != dims {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(query), dims)
	}
	switch metric {
	case VectorMetricCosine:
		if len(node.vector) > 0 {
			return vectorDistanceToFloat32NodeCosine(query, queryNormSquared, node)
		}
		var dot float64
		for i, left := range query {
			right := node.vectorValueAt(i)
			dot += float64(left * right)
		}
		leftNorm := queryNormSquared
		if leftNorm < 0 {
			leftNorm = vectorNormSquared(query)
		}
		rightNorm := node.cachedNormSquared()
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i, left := range query {
			diff := float64(left - node.vectorValueAt(i))
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i, left := range query {
			dot += float64(left * node.vectorValueAt(i))
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

func vectorDistanceBetweenStoredNodes(left, right *vectorIndexNode, metric VectorMetric) (float32, error) {
	dims := left.vectorDimensions()
	rightDims := right.vectorDimensions()
	if dims != rightDims {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", dims, rightDims)
	}
	switch metric {
	case VectorMetricCosine:
		if len(left.vector) > 0 && len(right.vector) > 0 {
			return vectorDistanceBetweenFloat32NodesCosine(left, right)
		}
		var dot float64
		for i := 0; i < dims; i++ {
			leftValue := left.vectorValueAt(i)
			rightValue := right.vectorValueAt(i)
			dot += float64(leftValue * rightValue)
		}
		leftNorm := left.cachedNormSquared()
		rightNorm := right.cachedNormSquared()
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i := 0; i < dims; i++ {
			diff := float64(left.vectorValueAt(i) - right.vectorValueAt(i))
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i := 0; i < dims; i++ {
			dot += float64(left.vectorValueAt(i) * right.vectorValueAt(i))
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

type preparedFloat32CosineQuery struct {
	vector  []float32
	invNorm float32
}

func prepareFloat32CosineQuery(query []float32, queryNormSquared float64) (preparedFloat32CosineQuery, error) {
	leftNorm := queryNormSquared
	if leftNorm < 0 {
		leftNorm = vectorNormSquared(query)
	}
	if leftNorm == 0 {
		return preparedFloat32CosineQuery{}, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return preparedFloat32CosineQuery{
		vector:  query,
		invNorm: float32(1 / math.Sqrt(leftNorm)),
	}, nil
}

func vectorDistanceToFloat32NodeCosine(query []float32, queryNormSquared float64, node *vectorIndexNode) (float32, error) {
	prepared, err := prepareFloat32CosineQuery(query, queryNormSquared)
	if err != nil {
		return 0, err
	}
	return vectorDistanceToFloat32NodeCosinePrepared(prepared, node)
}

func vectorDistanceToFloat32NodeCosinePrepared(query preparedFloat32CosineQuery, node *vectorIndexNode) (float32, error) {
	if len(query.vector) != len(node.vector) {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(query.vector), len(node.vector))
	}
	if node.cachedInvNorm == 0 {
		return 0, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return vectorDistanceToFloat32NodeCosineUnchecked(query, node), nil
}

func vectorDistanceToFloat32NodeCosineUnchecked(query preparedFloat32CosineQuery, node *vectorIndexNode) float32 {
	n := len(query.vector)
	if n != len(node.vector) {
		panic(fmt.Sprintf("collections: vector dimensions differ: %d vs %d", n, len(node.vector)))
	}
	dot := blas32.Dot(
		blas32.Vector{N: n, Inc: 1, Data: query.vector},
		blas32.Vector{N: n, Inc: 1, Data: node.vector},
	)
	return 1 - dot*query.invNorm*node.cachedInvNorm
}

func vectorDistanceBetweenFloat32NodesCosine(left, right *vectorIndexNode) (float32, error) {
	if len(left.vector) != len(right.vector) {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(left.vector), len(right.vector))
	}
	if left.cachedInvNorm == 0 || right.cachedInvNorm == 0 {
		return 0, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	n := len(left.vector)
	dot := blas32.Dot(
		blas32.Vector{N: n, Inc: 1, Data: left.vector},
		blas32.Vector{N: n, Inc: 1, Data: right.vector},
	)
	return 1 - dot*left.cachedInvNorm*right.cachedInvNorm, nil
}

func (node *vectorIndexNode) vectorDimensions() int {
	if len(node.vector) > 0 {
		return len(node.vector)
	}
	return len(node.quantized)
}

func (node *vectorIndexNode) vectorValueAt(i int) float32 {
	if len(node.vector) > 0 {
		return node.vector[i]
	}
	return float32(node.quantized[i]) * node.quantScale
}

func (node *vectorIndexNode) cachedNormSquared() float64 {
	if node.normSquared > 0 {
		return node.normSquared
	}
	return node.storedNormSquared()
}

func (node *vectorIndexNode) cacheVectorNorms() {
	node.normSquared = node.storedNormSquared()
	if node.normSquared > 0 {
		node.cachedInvNorm = float32(1 / math.Sqrt(node.normSquared))
	} else {
		node.cachedInvNorm = 0
	}
}

func (node *vectorIndexNode) storedNormSquared() float64 {
	var norm float64
	dims := node.vectorDimensions()
	for i := 0; i < dims; i++ {
		value := node.vectorValueAt(i)
		norm += float64(value * value)
	}
	return norm
}

func (node *vectorIndexNode) vectorBytes() int {
	if len(node.quantized) > 0 {
		return len(node.quantized) + 4
	}
	return len(node.vector) * 4
}

type vectorIndexCandidate struct {
	nodeID   int
	distance float32
}

type vectorIndexSearchScratch struct {
	visitedEpochs []uint32
	visitedEpoch  uint32
	queue         vectorIndexMinCandidateHeap
	best          vectorIndexMaxCandidateHeap
	out           []vectorIndexCandidate
}

func (scratch *vectorIndexSearchScratch) nextVisitedEpoch(nodes int) ([]uint32, uint32) {
	if cap(scratch.visitedEpochs) < nodes {
		scratch.visitedEpochs = make([]uint32, nodes, growVectorIndexScratchCapacity(cap(scratch.visitedEpochs), nodes))
	} else {
		scratch.visitedEpochs = scratch.visitedEpochs[:nodes]
	}
	scratch.visitedEpoch++
	if scratch.visitedEpoch == 0 {
		clear(scratch.visitedEpochs)
		scratch.visitedEpoch = 1
	}
	return scratch.visitedEpochs, scratch.visitedEpoch
}

func growVectorIndexScratchCapacity(current int, required int) int {
	next := current
	if next < 64 {
		next = 64
	}
	for next < required {
		next *= 2
	}
	return next
}

func sortVectorIndexCandidates(candidates []vectorIndexCandidate) {
	slices.SortFunc(candidates, func(left, right vectorIndexCandidate) int {
		if left.distance < right.distance {
			return -1
		}
		if left.distance > right.distance {
			return 1
		}
		if left.nodeID < right.nodeID {
			return -1
		}
		if left.nodeID > right.nodeID {
			return 1
		}
		return 0
	})
}

func vectorIndexCandidateLess(left, right vectorIndexCandidate) bool {
	if left.distance != right.distance {
		return left.distance < right.distance
	}
	return left.nodeID < right.nodeID
}

func vectorIndexCandidateWorse(left, right vectorIndexCandidate) bool {
	return vectorIndexCandidateLess(right, left)
}

type vectorIndexMinCandidateHeap []vectorIndexCandidate

func (h *vectorIndexMinCandidateHeap) push(candidate vectorIndexCandidate) {
	*h = append(*h, candidate)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !vectorIndexCandidateLess((*h)[child], (*h)[parent]) {
			break
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorIndexMinCandidateHeap) pop() vectorIndexCandidate {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	h.down(0)
	return out
}

func (h vectorIndexMinCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && vectorIndexCandidateLess(h[right], h[left]) {
			child = right
		}
		if !vectorIndexCandidateLess(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

type vectorIndexMaxCandidateHeap []vectorIndexCandidate

func (h *vectorIndexMaxCandidateHeap) pushBounded(candidate vectorIndexCandidate, limit int) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(len(*h) - 1)
		return
	}
	if !vectorIndexCandidateLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(0)
}

func (h *vectorIndexMaxCandidateHeap) up(child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !vectorIndexCandidateWorse((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h vectorIndexMaxCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && vectorIndexCandidateWorse(h[right], h[left]) {
			child = right
		}
		if !vectorIndexCandidateWorse(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

// Stats returns a snapshot of in-memory vector index state.
func (idx *VectorIndex) Stats() VectorIndexStats {
	if idx == nil {
		return VectorIndexStats{}
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	stats := VectorIndexStats{
		Name:                idx.name,
		Field:               idx.field,
		Metric:              idx.metric,
		Encoding:            idx.encoding,
		Dimensions:          idx.dimensions,
		M:                   idx.m,
		EfConstruction:      idx.efConstruction,
		EfSearch:            idx.efSearch,
		Nodes:               len(idx.nodes),
		LiveDocs:            len(idx.currentNode),
		MaxLevel:            idx.maxLevel,
		Epoch:               idx.persistedEpoch,
		BytesDisk:           idx.persistedBytesDisk,
		SnapshotDirty:       idx.persistedSnapshotDirty || (idx.nativePersistent && idx.persistedEpoch == 0 && idx.mutationSeq != 0),
		LastRebuildDuration: idx.lastRebuildDuration,
	}
	var edges int
	var vectorBytes int64
	for i := range idx.nodes {
		node := idx.nodes[i]
		if node.deleted {
			stats.DeletedDocs++
		}
		for _, layerNeighbors := range node.neighbors {
			edges += len(layerNeighbors)
		}
		vectorBytes += int64(node.vectorBytes())
		vectorBytes += int64(len(node.documentID))
	}
	if stats.Nodes > 0 {
		stats.DeletedRatio = float64(stats.DeletedDocs) / float64(stats.Nodes)
		stats.AvgDegree = float64(edges) / float64(stats.Nodes)
	}
	// Approximate heap footprint; edge accounting tracks the neighbor struct
	// size but excludes slice headers and spare capacity.
	stats.BytesMemory = vectorBytes + int64(edges)*int64(unsafe.Sizeof(vectorIndexNeighbor{})) + int64(stats.Nodes*32)
	stats.RebuildNeeded = stats.DeletedRatio >= idx.rebuildDeletedRatio && stats.DeletedDocs > 0
	return stats
}

// CheckRecall compares indexed search with exact search for the supplied query
// vectors and reports recall@TopK.
func (idx *VectorIndex) CheckRecall(queries [][]float32, opts VectorIndexSearchOptions) (VectorIndexRecall, error) {
	recall := VectorIndexRecall{Queries: len(queries), TopK: opts.TopK}
	if idx == nil {
		return recall, errors.New("collections: vector index is nil")
	}
	if opts.TopK <= 0 {
		return recall, errors.New("collections: vector search TopK must be positive")
	}
	recall.SearchTraces = make([]VectorIndexTrace, 0, len(queries))
	for _, query := range queries {
		exact, err := idx.collection.SearchVectorsExact(query, VectorSearchOptions{
			Field:            idx.field,
			Metric:           idx.metric,
			TopK:             opts.TopK,
			Filter:           opts.Filter,
			IndexRangeFilter: opts.IndexRangeFilter,
		})
		if err != nil {
			return recall, err
		}
		searchOpts := opts
		searchOpts.DisableExactFallback = true
		ann, trace, err := idx.Search(query, searchOpts)
		if err != nil {
			return recall, err
		}
		recall.SearchTraces = append(recall.SearchTraces, trace)
		recall.ExactTotal += len(exact)
		recall.ANNTotal += len(ann)
		exactSet := make(map[string]struct{}, len(exact))
		for _, result := range exact {
			exactSet[string(result.DocumentID)] = struct{}{}
		}
		for _, result := range ann {
			if _, ok := exactSet[string(result.DocumentID)]; ok {
				recall.Overlap++
			}
		}
	}
	if recall.ExactTotal > 0 {
		recall.Recall = float64(recall.Overlap) / float64(recall.ExactTotal)
	}
	return recall, nil
}

func vectorFromStoredDocument(materializer *StoredDocumentJSONMaterializer, document []byte, fieldPath []string) ([]float32, bool, error) {
	if materializer != nil && materializer.DocumentFormat() == DocumentFormatBSON {
		return vectorFromBSONField(document, fieldPath)
	}
	if materializer == nil {
		return nil, false, errors.New("collections: nil stored document materializer")
	}
	jsonDoc, err := materializer.StoredDocumentJSON(document)
	if err != nil {
		return nil, false, err
	}
	return vectorFromJSONField(jsonDoc, fieldPath)
}

// Rebuild removes tombstoned/superseded graph nodes by rebuilding the index from
// live canonical collection rows. It preserves vector index options and swaps
// the rebuilt graph into the receiver.
func (idx *VectorIndex) Rebuild() error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	c := idx.collection
	if c == nil {
		return errCollectionNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	start := time.Now()
	rebuilt, err := c.buildVectorIndex(VectorIndexOptions{
		Name:                idx.name,
		Field:               idx.field,
		Metric:              idx.metric,
		Encoding:            idx.encoding,
		Dimensions:          idx.dimensions,
		M:                   idx.m,
		EfConstruction:      idx.efConstruction,
		EfSearch:            idx.efSearch,
		RebuildDeletedRatio: idx.rebuildDeletedRatio,
	}, false)
	if err != nil {
		return err
	}
	rebuilt.mu.RLock()
	nodes := cloneVectorIndexNodes(rebuilt.nodes)
	currentNode := cloneVectorIndexCurrentNode(rebuilt.currentNode)
	entry := rebuilt.entry
	maxLevel := rebuilt.maxLevel
	dimensions := rebuilt.dimensions
	rebuilt.mu.RUnlock()

	idx.mu.Lock()
	idx.nodes = nodes
	idx.currentNode = currentNode
	idx.entry = entry
	idx.maxLevel = maxLevel
	idx.dimensions = dimensions
	idx.lastRebuildDuration = collectionObservedElapsedSince(start)
	idx.markGraphChangedLocked()
	idx.mu.Unlock()
	c.RegisterVectorIndex(idx)
	return nil
}

func cloneVectorIndexNodes(in []vectorIndexNode) []vectorIndexNode {
	out := make([]vectorIndexNode, len(in))
	for i := range in {
		out[i] = vectorIndexNode{
			documentID:    bytes.Clone(in[i].documentID),
			vector:        append([]float32(nil), in[i].vector...),
			quantized:     append([]int8(nil), in[i].quantized...),
			quantScale:    in[i].quantScale,
			normSquared:   in[i].normSquared,
			cachedInvNorm: in[i].cachedInvNorm,
			level:         in[i].level,
			deleted:       in[i].deleted,
			neighbors:     make([][]vectorIndexNeighbor, len(in[i].neighbors)),
		}
		for layer := range in[i].neighbors {
			out[i].neighbors[layer] = append([]vectorIndexNeighbor(nil), in[i].neighbors[layer]...)
		}
	}
	return out
}

func cloneVectorIndexCurrentNode(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
