package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	nativeScalarProbeLimit = hybridScalarDefaultLookupLimit
	// 512 is a conservative correctness-first ceiling pending coordinator
	// same-host crossover measurement across dimension and concurrency.
	nativeScalarExactSafetyCap = 512
	nativeScalarANNVisitFactor = 16
	// Broad vector-aligned filters inspect at most the same 4,096 immutable
	// scalar rows as the route-selection probe, spread deterministically across
	// the graph. At most 32 matching rows become graph entries; all vector
	// scoring, including entries, remains inside the ANN visit-factor budget.
	nativeScalarANNSeedProbeLimit = nativeScalarProbeLimit
	nativeScalarANNSeedLimit      = 32

	nativeScalarPlanCacheMaxEntries = 64
	nativeScalarPlanCacheMaxBytes   = 8 << 20
)

// NativeScalarFilterPlan identifies the bounded declared-scalar execution route.
type NativeScalarFilterPlan string

const (
	NativeScalarFilterPlanNone           NativeScalarFilterPlan = "none"
	NativeScalarFilterPlanCompleteExact  NativeScalarFilterPlan = "complete_exact"
	NativeScalarFilterPlanCompleteFinite NativeScalarFilterPlan = "complete_finite_ann"
	NativeScalarFilterPlanMixed          NativeScalarFilterPlan = "mixed_refined"
	NativeScalarFilterPlanVectorAligned  NativeScalarFilterPlan = "vector_aligned_ann"
)

const nativeScalarColumnChunkRows = 256

type vectorIndexScalarColumnChunk struct {
	offsets []uint32
	data    []byte
	present []uint64
}

type vectorIndexScalarColumn struct {
	valueType  IndexValueType
	fullChunks []vectorIndexScalarColumnChunk
	tail       vectorIndexScalarColumnChunk
	rows       int
	tailBytes  uint64
}

func newVectorIndexScalarColumn(valueType IndexValueType) vectorIndexScalarColumn {
	return vectorIndexScalarColumn{
		valueType: valueType,
		tail:      vectorIndexScalarColumnChunk{offsets: []uint32{0}},
	}
}

func (c *vectorIndexScalarColumn) appendPrevalidated(value []byte, present bool) {
	if c.rows > 0 && c.rows%nativeScalarColumnChunkRows == 0 {
		c.fullChunks = append(c.fullChunks, c.tail)
		c.tail = vectorIndexScalarColumnChunk{offsets: []uint32{0}}
		c.tailBytes = 0
	}
	row := c.rows % nativeScalarColumnChunkRows
	if present {
		word := row / 64
		for len(c.tail.present) <= word {
			c.tail.present = append(c.tail.present, 0)
		}
		c.tail.present[word] |= uint64(1) << uint(row%64)
		c.tail.data = append(c.tail.data, value...)
		c.tailBytes += uint64(len(value))
	}
	c.tail.offsets = append(c.tail.offsets, uint32(len(c.tail.data)))
	c.rows++
}

func (c vectorIndexScalarColumn) value(row int) ([]byte, bool) {
	if row < 0 || row >= c.rows {
		return nil, false
	}
	chunkID, chunkRow := row/nativeScalarColumnChunkRows, row%nativeScalarColumnChunkRows
	chunk := c.tail
	if chunkID < len(c.fullChunks) {
		chunk = c.fullChunks[chunkID]
	}
	if chunkRow/64 >= len(chunk.present) || chunk.present[chunkRow/64]&(uint64(1)<<uint(chunkRow%64)) == 0 {
		return nil, false
	}
	start, end := chunk.offsets[chunkRow], chunk.offsets[chunkRow+1]
	if start > end || int(end) > len(chunk.data) {
		return nil, false
	}
	return chunk.data[start:end], true
}

func cloneVectorIndexScalarColumnChunk(chunk vectorIndexScalarColumnChunk) vectorIndexScalarColumnChunk {
	chunk.offsets = append([]uint32(nil), chunk.offsets...)
	chunk.data = append([]byte(nil), chunk.data...)
	chunk.present = append([]uint64(nil), chunk.present...)
	return chunk
}

func cloneVectorIndexScalarColumns(in map[string]vectorIndexScalarColumn) map[string]vectorIndexScalarColumn {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]vectorIndexScalarColumn, len(in))
	for name, column := range in {
		column.fullChunks = append([]vectorIndexScalarColumnChunk(nil), column.fullChunks...)
		for i := range column.fullChunks {
			column.fullChunks[i] = cloneVectorIndexScalarColumnChunk(column.fullChunks[i])
		}
		column.tail = cloneVectorIndexScalarColumnChunk(column.tail)
		out[name] = column
	}
	return out
}

func snapshotVectorIndexScalarColumns(in map[string]vectorIndexScalarColumn) map[string]vectorIndexScalarColumn {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]vectorIndexScalarColumn, len(in))
	for name, column := range in {
		column.tail = cloneVectorIndexScalarColumnChunk(column.tail)
		out[name] = column
	}
	return out
}

func supportedNativeScalarType(valueType IndexValueType) bool {
	switch valueType {
	case IndexValueString, IndexValueBool, IndexValueInt64, IndexValueDouble:
		return true
	default:
		return false
	}
}

func nativeScalarDefinitions(meta CollectionMeta) []IndexDefinition {
	out := make([]IndexDefinition, 0, len(meta.Indexes))
	for _, def := range meta.Indexes {
		if supportedNativeScalarType(def.ValueType) && !def.MultiKey && len(def.Components) <= 1 {
			out = append(out, def)
		}
	}
	return out
}

func newNativeScalarColumns(defs []IndexDefinition) map[string]vectorIndexScalarColumn {
	if len(defs) == 0 {
		return nil
	}
	out := make(map[string]vectorIndexScalarColumn, len(defs))
	for _, def := range defs {
		out[def.Name] = newVectorIndexScalarColumn(def.ValueType)
	}
	return out
}

func nativeScalarRuntimes(defs []IndexDefinition) ([]indexRuntime, error) {
	indexes := make([]indexDefinition, len(defs))
	for i, def := range defs {
		indexes[i] = indexDefinition{name: def.Name, field: def.Field, valueType: def.ValueType, unique: def.Unique, multiKey: def.MultiKey}
	}
	return (insertBatchPlanner{indexes: indexes}).indexRuntimes()
}

func cloneNativeScalarRuntimes(in []indexRuntime) []indexRuntime {
	out := append([]indexRuntime(nil), in...)
	for i := range out {
		out[i].def.components = append([]IndexComponent(nil), in[i].def.components...)
		out[i].path = append([]string(nil), in[i].path...)
		out[i].componentPaths = make([][]string, len(in[i].componentPaths))
		for component := range in[i].componentPaths {
			out[i].componentPaths[component] = append([]string(nil), in[i].componentPaths[component]...)
		}
	}
	return out
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

func (idx *VectorIndex) nativeScalarRow(materializer *StoredDocumentJSONMaterializer, document []byte) (map[string][]byte, error) {
	if idx == nil {
		return nil, nil
	}
	idx.mu.RLock()
	definitions := idx.scalarDefinitions
	runtimes := idx.scalarRuntimes
	idx.mu.RUnlock()
	if len(definitions) == 0 {
		return nil, nil
	}
	scalarDocument := document
	documentFormat := normalizedDocumentFormat(materializer.DocumentFormat())
	if documentFormat != DocumentFormatBSON {
		var err error
		scalarDocument, err = materializer.StoredDocumentJSON(document)
		if err != nil {
			return nil, err
		}
		documentFormat = DocumentFormatJSON
	}
	if len(runtimes) != len(definitions) {
		return nil, errors.New("collections: native scalar runtimes are unavailable")
	}
	state, err := orderedIndexStateForDocument(scalarDocument, runtimes, collectionOptions{documentFormat: documentFormat})
	if err != nil {
		return nil, err
	}
	row := make(map[string][]byte, len(definitions))
	for i, def := range definitions {
		values := state.valuesAt(i)
		if len(values) > 1 {
			return nil, fmt.Errorf("collections: native scalar index %q produced multiple values", def.Name)
		}
		if len(values) == 1 {
			row[def.Name] = bytes.Clone(values[0])
		}
	}
	return row, nil
}

func (idx *VectorIndex) validateNativeScalarRowsAppendLocked(rows ...map[string][]byte) error {
	for _, def := range idx.scalarDefinitions {
		column, ok := idx.scalarColumns[def.Name]
		if !ok {
			return fmt.Errorf("collections: native scalar column %q is unavailable", def.Name)
		}
		chunkRows := column.rows % nativeScalarColumnChunkRows
		if column.rows > 0 && chunkRows == 0 {
			chunkRows = nativeScalarColumnChunkRows
		}
		chunkBytes := column.tailBytes
		for _, row := range rows {
			if chunkRows == nativeScalarColumnChunkRows {
				chunkRows = 0
				chunkBytes = 0
			}
			value, present := row[def.Name]
			if present {
				valueBytes := uint64(len(value))
				if valueBytes > math.MaxUint32 || chunkBytes > math.MaxUint32-valueBytes {
					return errors.New("collections: native scalar column chunk exceeds 4GiB payload limit")
				}
				chunkBytes += valueBytes
			}
			chunkRows++
		}
	}
	return nil
}

func (idx *VectorIndex) appendNativeScalarRowValuesPrevalidatedLocked(row map[string][]byte) {
	for _, def := range idx.scalarDefinitions {
		column := idx.scalarColumns[def.Name]
		value, present := row[def.Name]
		column.appendPrevalidated(value, present)
		idx.scalarColumns[def.Name] = column
	}
}

func (idx *VectorIndex) appendNativeScalarRowValuesLocked(row map[string][]byte) error {
	if err := idx.validateNativeScalarRowsAppendLocked(row); err != nil {
		return err
	}
	idx.appendNativeScalarRowValuesPrevalidatedLocked(row)
	return nil
}

func (idx *VectorIndex) appendNativeScalarRowLocked(row map[string][]byte) error {
	if err := idx.appendNativeScalarRowValuesLocked(row); err != nil {
		return err
	}
	return idx.validateNativeScalarColumnLengthsLocked()
}

func (idx *VectorIndex) validateNativeScalarColumnLengthsLocked() error {
	for _, def := range idx.scalarDefinitions {
		column, ok := idx.scalarColumns[def.Name]
		if !ok || column.rows != len(idx.nodes) {
			return fmt.Errorf("collections: native scalar column %q row count mismatch: rows=%d nodes=%d", def.Name, column.rows, len(idx.nodes))
		}
	}
	return nil
}

type nativeScalarClause struct {
	indexName      string
	valueType      IndexValueType
	lower          []byte
	upper          []byte
	lowerInclusive bool
	upperInclusive bool
}

func (c nativeScalarClause) matches(value []byte, present bool) bool {
	if !present {
		return false
	}
	if c.lower != nil {
		cmp := bytes.Compare(value, c.lower)
		if cmp < 0 || cmp == 0 && !c.lowerInclusive {
			return false
		}
	}
	if c.upper != nil {
		cmp := bytes.Compare(value, c.upper)
		if cmp > 0 || cmp == 0 && !c.upperInclusive {
			return false
		}
	}
	return true
}

func (idx *VectorIndex) nativeScalarRowAtLocked(row int) map[string][]byte {
	if idx == nil || len(idx.scalarDefinitions) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(idx.scalarDefinitions))
	for _, def := range idx.scalarDefinitions {
		column, ok := idx.scalarColumns[def.Name]
		if !ok {
			continue
		}
		if value, present := column.value(row); present {
			out[def.Name] = bytes.Clone(value)
		}
	}
	return out
}

func (idx *VectorIndex) insertVectorWithNativeScalarLocked(documentID []byte, vector []float32, row map[string][]byte) error {
	if len(idx.scalarDefinitions) == 0 {
		if idx.liveDeltaActiveLocked() {
			return idx.insertLiveVectorBatchLocked([][]byte{documentID}, [][]float32{vector})
		}
		return idx.insertVectorLocked(documentID, vector)
	}
	if !idx.liveDeltaActiveLocked() {
		if err := idx.validateNativeScalarColumnLengthsLocked(); err != nil {
			return err
		}
		if err := idx.validateNativeScalarRowsAppendLocked(row); err != nil {
			return err
		}
		idx.tombstoneDocumentIDLocked(documentID)
		before := len(idx.nodes)
		if err := idx.insertVectorLocked(documentID, vector); err != nil {
			return err
		}
		if len(idx.nodes) != before+1 {
			return errors.New("collections: native scalar vector insertion did not append one node")
		}
		idx.appendNativeScalarRowValuesPrevalidatedLocked(row)
		return idx.validateNativeScalarColumnLengthsLocked()
	}
	idx.prepareSearchViewForMutationLocked()
	delta, err := idx.ensureLiveDeltaLocked()
	if err != nil {
		return err
	}
	if len(delta.nodes)+1 >= defaultVectorIndexLiveDeltaRows {
		if err := idx.foldLiveDeltaLocked(); err != nil {
			return err
		}
		delta, err = idx.ensureLiveDeltaLocked()
		if err != nil {
			return err
		}
	}
	if err := delta.validateNativeScalarColumnLengthsLocked(); err != nil {
		return err
	}
	if err := delta.validateNativeScalarRowsAppendLocked(row); err != nil {
		return err
	}
	delta.tombstoneDocumentIDLocked(documentID)
	before := len(delta.nodes)
	if err := delta.insertVectorLocked(documentID, vector); err != nil {
		return err
	}
	if len(delta.nodes) != before+1 {
		return errors.New("collections: native scalar delta insertion did not append one node")
	}
	delta.appendNativeScalarRowValuesPrevalidatedLocked(row)
	if err := delta.validateNativeScalarColumnLengthsLocked(); err != nil {
		return err
	}
	idx.tombstoneDocumentIDLocked(documentID)
	idx.markGraphChangedLocked()
	return nil
}

type nativeScalarFilterExecution struct {
	identity             NativeScalarFilterPlan
	clauses              []nativeScalarClause
	finiteIDs            hybridScalarAllowSet
	seedIDs              []string
	probeIDs             uint64
	probeTruncated       uint64
	candidateIDs         uint64
	retainedCandidateIDs uint64
	refinedCandidateIDs  uint64
	sourceGeneration     uint64
	exactScoring         bool
}

// nativeScalarBoundMatcher holds the immutable columns for one graph plane.
// Keeping the map lookup out of the row loop is material for broad ANN walks.
type nativeScalarBoundMatcher struct {
	plan     *nativeScalarFilterExecution
	columns  [4]vectorIndexScalarColumn
	overflow []vectorIndexScalarColumn
}

type nativeScalarPlanCacheKey struct {
	vectorIndex       *VectorIndex
	sourceGeneration  uint64
	vectorGeneration  uint64
	vectorMutationSeq uint64
	vectorSchema      string
	scalarSchema      string
	filterIdentity    string
	probeLimit        int
	exactSafetyCap    int
	annSeedProbeLimit int
	annSeedLimit      int
}

type nativeScalarPlanCacheEpoch struct {
	sourceGeneration  uint64
	vectorGeneration  uint64
	vectorMutationSeq uint64
	vectorSchema      string
	scalarSchema      string
}

type nativeScalarPlanCacheEpochState struct {
	epoch   nativeScalarPlanCacheEpoch
	entries int
}

func (key nativeScalarPlanCacheKey) epoch() nativeScalarPlanCacheEpoch {
	return nativeScalarPlanCacheEpoch{
		sourceGeneration:  key.sourceGeneration,
		vectorGeneration:  key.vectorGeneration,
		vectorMutationSeq: key.vectorMutationSeq,
		vectorSchema:      key.vectorSchema,
		scalarSchema:      key.scalarSchema,
	}
}

func (idx *VectorIndex) nativeScalarPlanCacheGeneration() (uint64, uint64, bool) {
	if idx == nil {
		return 0, 0, false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.sourceDocumentGeneration, idx.mutationSeq, idx.sourceDocumentRootsValid
}

type nativeScalarPlanCacheEntry struct {
	plan  *nativeScalarFilterExecution
	bytes uint64
}

type nativeScalarPlanCacheStats struct {
	hits               uint64
	misses             uint64
	invalidations      uint64
	generationBypasses uint64
	evictions          uint64
	entries            uint64
	retainedBytes      uint64
}

func appendNativeScalarIdentityBytes(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendNativeScalarIdentityString(dst []byte, value string) []byte {
	return appendNativeScalarIdentityBytes(dst, []byte(value))
}

func nativeScalarFilterIdentity(clauses []nativeScalarClause) string {
	identity := make([]byte, 0, len(clauses)*64)
	identity = binary.BigEndian.AppendUint64(identity, uint64(len(clauses)))
	for _, clause := range clauses {
		identity = appendNativeScalarIdentityString(identity, clause.indexName)
		identity = appendNativeScalarIdentityString(identity, string(clause.valueType))
		if clause.lower != nil {
			identity = append(identity, 1)
			identity = appendNativeScalarIdentityBytes(identity, clause.lower)
		} else {
			identity = append(identity, 0)
		}
		if clause.lowerInclusive {
			identity = append(identity, 1)
		} else {
			identity = append(identity, 0)
		}
		if clause.upper != nil {
			identity = append(identity, 1)
			identity = appendNativeScalarIdentityBytes(identity, clause.upper)
		} else {
			identity = append(identity, 0)
		}
		if clause.upperInclusive {
			identity = append(identity, 1)
		} else {
			identity = append(identity, 0)
		}
	}
	return string(identity)
}

func nativeScalarSchemaIdentity(defs []IndexDefinition) string {
	identity := make([]byte, 0, len(defs)*64)
	identity = binary.BigEndian.AppendUint64(identity, uint64(len(defs)))
	for _, def := range defs {
		identity = appendNativeScalarIdentityString(identity, def.Name)
		identity = appendNativeScalarIdentityString(identity, def.Field)
		identity = appendNativeScalarIdentityString(identity, string(def.ValueType))
		if def.Unique {
			identity = append(identity, 1)
		} else {
			identity = append(identity, 0)
		}
		if def.MultiKey {
			identity = append(identity, 1)
		} else {
			identity = append(identity, 0)
		}
		identity = appendNativeScalarIdentityString(identity, string(def.StoragePolicy))
		identity = binary.BigEndian.AppendUint64(identity, uint64(len(def.Components)))
		for _, component := range def.Components {
			identity = appendNativeScalarIdentityString(identity, component.Field)
			identity = append(identity, byte(component.Direction))
		}
	}
	return string(identity)
}

func nativeScalarVectorSchemaIdentity(def VectorIndexDefinition) string {
	identity := make([]byte, 0, 96)
	identity = appendNativeScalarIdentityString(identity, def.Name)
	identity = appendNativeScalarIdentityString(identity, def.Field)
	identity = append(identity, byte(def.Metric))
	identity = binary.BigEndian.AppendUint64(identity, uint64(def.Dimensions))
	identity = binary.BigEndian.AppendUint64(identity, uint64(def.M))
	identity = binary.BigEndian.AppendUint64(identity, uint64(def.EfConstruction))
	identity = binary.BigEndian.AppendUint64(identity, uint64(def.EfSearch))
	identity = append(identity, byte(def.Encoding))
	identity = appendNativeScalarIdentityString(identity, string(def.Strategy))
	identity = binary.BigEndian.AppendUint64(identity, def.SchemaGeneration)
	return string(identity)
}

func cloneNativeScalarPlanForQuery(plan *nativeScalarFilterExecution) *nativeScalarFilterExecution {
	if plan == nil || plan.identity != NativeScalarFilterPlanMixed {
		return plan
	}
	clone := *plan
	clone.finiteIDs = make(hybridScalarAllowSet, len(plan.finiteIDs))
	for id := range plan.finiteIDs {
		clone.finiteIDs[id] = struct{}{}
	}
	return &clone
}

func nativeScalarPlanCacheEntryBytes(key nativeScalarPlanCacheKey, plan *nativeScalarFilterExecution) uint64 {
	size := uint64(512 + 2*(len(key.vectorSchema)+len(key.scalarSchema)+len(key.filterIdentity)))
	if plan == nil {
		return size
	}
	for _, clause := range plan.clauses {
		size += uint64(64 + len(clause.indexName) + len(clause.lower) + len(clause.upper))
	}
	for id := range plan.finiteIDs {
		size += uint64(64 + len(id))
	}
	for _, id := range plan.seedIDs {
		size += uint64(32 + len(id))
	}
	return size
}

func (c *Collection) nativeScalarPlanCacheInvalidateEpochLocked(key nativeScalarPlanCacheKey, stats *nativeScalarPlanCacheStats) {
	state, ok := c.nativeScalarPlanCacheEpochs[key.vectorIndex]
	if !ok || state.epoch == key.epoch() {
		return
	}
	oldOrder := c.nativeScalarPlanCacheOrder
	nextOrder := oldOrder[:0]
	for _, cachedKey := range oldOrder {
		entry, ok := c.nativeScalarPlanCache[cachedKey]
		if !ok {
			continue
		}
		if cachedKey.vectorIndex == key.vectorIndex {
			delete(c.nativeScalarPlanCache, cachedKey)
			c.nativeScalarPlanCacheBytes -= entry.bytes
			stats.invalidations++
			continue
		}
		nextOrder = append(nextOrder, cachedKey)
	}
	for i := len(nextOrder); i < len(oldOrder); i++ {
		oldOrder[i] = nativeScalarPlanCacheKey{}
	}
	c.nativeScalarPlanCacheOrder = nextOrder
	delete(c.nativeScalarPlanCacheEpochs, key.vectorIndex)
}

func (c *Collection) nativeScalarPlanCacheSnapshotLocked(stats nativeScalarPlanCacheStats) nativeScalarPlanCacheStats {
	stats.entries = uint64(len(c.nativeScalarPlanCache))
	stats.retainedBytes = c.nativeScalarPlanCacheBytes
	return stats
}

func (c *Collection) nativeScalarPlanCacheSnapshot(stats nativeScalarPlanCacheStats) nativeScalarPlanCacheStats {
	c.nativeScalarPlanCacheMu.Lock()
	defer c.nativeScalarPlanCacheMu.Unlock()
	return c.nativeScalarPlanCacheSnapshotLocked(stats)
}

func (c *Collection) nativeScalarPlanCacheGet(key nativeScalarPlanCacheKey) (*nativeScalarFilterExecution, nativeScalarPlanCacheStats) {
	c.nativeScalarPlanCacheMu.Lock()
	defer c.nativeScalarPlanCacheMu.Unlock()

	var stats nativeScalarPlanCacheStats
	if entry, ok := c.nativeScalarPlanCache[key]; ok {
		stats.hits = 1
		return cloneNativeScalarPlanForQuery(entry.plan), c.nativeScalarPlanCacheSnapshotLocked(stats)
	}
	c.nativeScalarPlanCacheInvalidateEpochLocked(key, &stats)
	stats.misses = 1
	return nil, c.nativeScalarPlanCacheSnapshotLocked(stats)
}

func (c *Collection) nativeScalarPlanCachePut(key nativeScalarPlanCacheKey, plan *nativeScalarFilterExecution, stats nativeScalarPlanCacheStats) (*nativeScalarFilterExecution, nativeScalarPlanCacheStats) {
	c.nativeScalarPlanCacheMu.Lock()
	defer c.nativeScalarPlanCacheMu.Unlock()

	c.nativeScalarPlanCacheInvalidateEpochLocked(key, &stats)

	if entry, ok := c.nativeScalarPlanCache[key]; ok {
		return cloneNativeScalarPlanForQuery(entry.plan), c.nativeScalarPlanCacheSnapshotLocked(stats)
	}
	entry := nativeScalarPlanCacheEntry{plan: plan, bytes: nativeScalarPlanCacheEntryBytes(key, plan)}
	if entry.bytes > nativeScalarPlanCacheMaxBytes {
		return cloneNativeScalarPlanForQuery(plan), c.nativeScalarPlanCacheSnapshotLocked(stats)
	}
	if c.nativeScalarPlanCache == nil {
		c.nativeScalarPlanCache = make(map[nativeScalarPlanCacheKey]nativeScalarPlanCacheEntry)
	}
	if c.nativeScalarPlanCacheEpochs == nil {
		c.nativeScalarPlanCacheEpochs = make(map[*VectorIndex]nativeScalarPlanCacheEpochState)
	}
	for len(c.nativeScalarPlanCacheOrder) >= nativeScalarPlanCacheMaxEntries ||
		c.nativeScalarPlanCacheBytes+entry.bytes > nativeScalarPlanCacheMaxBytes {
		victim := c.nativeScalarPlanCacheOrder[0]
		c.nativeScalarPlanCacheOrder[0] = nativeScalarPlanCacheKey{}
		c.nativeScalarPlanCacheOrder = c.nativeScalarPlanCacheOrder[1:]
		if evicted, ok := c.nativeScalarPlanCache[victim]; ok {
			delete(c.nativeScalarPlanCache, victim)
			c.nativeScalarPlanCacheBytes -= evicted.bytes
			stats.evictions++
			if state, tracked := c.nativeScalarPlanCacheEpochs[victim.vectorIndex]; tracked {
				state.entries--
				if state.entries == 0 {
					delete(c.nativeScalarPlanCacheEpochs, victim.vectorIndex)
				} else {
					c.nativeScalarPlanCacheEpochs[victim.vectorIndex] = state
				}
			}
		}
	}
	c.nativeScalarPlanCache[key] = entry
	c.nativeScalarPlanCacheOrder = append(c.nativeScalarPlanCacheOrder, key)
	c.nativeScalarPlanCacheBytes += entry.bytes
	state := c.nativeScalarPlanCacheEpochs[key.vectorIndex]
	if state.entries == 0 || state.epoch != key.epoch() {
		state = nativeScalarPlanCacheEpochState{epoch: key.epoch()}
	}
	state.entries++
	c.nativeScalarPlanCacheEpochs[key.vectorIndex] = state
	return cloneNativeScalarPlanForQuery(plan), c.nativeScalarPlanCacheSnapshotLocked(stats)
}

func (m *nativeScalarBoundMatcher) matches(row int, id []byte) bool {
	if m.plan == nil {
		return true
	}
	if m.plan.finiteIDs != nil {
		if _, ok := m.plan.finiteIDs[string(id)]; !ok {
			return false
		}
	}
	return m.matchesKnownFinite(row)
}

func (m *nativeScalarBoundMatcher) matchesKnownFinite(row int) bool {
	if m.plan == nil {
		return true
	}
	for i, clause := range m.plan.clauses {
		var column vectorIndexScalarColumn
		if i < len(m.columns) {
			column = m.columns[i]
		} else {
			column = m.overflow[i-len(m.columns)]
		}
		if column.rows <= row {
			return false
		}
		if !clause.matches(column.value(row)) {
			return false
		}
	}
	return true
}

func compileNativeScalarClause(def IndexDefinition, filter HybridScalarFilter) (nativeScalarClause, error) {
	if !supportedNativeScalarType(def.ValueType) || def.MultiKey || len(def.Components) > 1 {
		return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q has unsupported type or multikey/compound shape", ErrHybridSearchUnsupported, def.Name)
	}
	clause := nativeScalarClause{indexName: def.Name, valueType: def.ValueType}
	if filter.Range == nil {
		encoded, err := encodeIndexScalar(def.ValueType, filter.Value)
		if err != nil {
			return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q value: %v", ErrHybridSearchUnsupported, def.Name, err)
		}
		clause.lower, clause.upper = encoded, bytes.Clone(encoded)
		clause.lowerInclusive, clause.upperInclusive = true, true
		return clause, nil
	}
	if !filter.Range.Lower.Unbounded {
		encoded, err := encodeIndexScalar(def.ValueType, filter.Range.Lower.Value)
		if err != nil {
			return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q lower bound: %v", ErrHybridSearchUnsupported, def.Name, err)
		}
		clause.lower, clause.lowerInclusive = encoded, filter.Range.Lower.Inclusive
	}
	if !filter.Range.Upper.Unbounded {
		encoded, err := encodeIndexScalar(def.ValueType, filter.Range.Upper.Value)
		if err != nil {
			return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q upper bound: %v", ErrHybridSearchUnsupported, def.Name, err)
		}
		clause.upper, clause.upperInclusive = encoded, filter.Range.Upper.Inclusive
	}
	return clause, nil
}

type nativeScalarLeafProbe struct {
	set       hybridScalarAllowSet
	truncated bool
}

func boundedNativeScalarSeedIDs(set hybridScalarAllowSet, limit int) []string {
	if limit <= 0 || len(set) == 0 {
		return nil
	}
	ids := make([]string, 0, minInt(limit, len(set)))
	for id := range set {
		if len(ids) < limit {
			ids = append(ids, id)
			continue
		}
		largest := 0
		for candidate := 1; candidate < len(ids); candidate++ {
			if ids[candidate] > ids[largest] {
				largest = candidate
			}
		}
		if id < ids[largest] {
			ids[largest] = id
		}
	}
	sort.Strings(ids)
	return ids
}

func (c *Collection) planNativeScalarFilter(filter *HybridScalarFilter, index *VectorIndex, vectorDef VectorIndexDefinition) (*nativeScalarFilterExecution, nativeScalarPlanCacheStats, error) {
	if filter == nil {
		return nil, nativeScalarPlanCacheStats{}, nil
	}
	if err := validateHybridScalarFilter(*filter); err != nil {
		return nil, nativeScalarPlanCacheStats{}, err
	}
	view, err := c.openHybridScalarLookupView()
	if err != nil {
		return nil, nativeScalarPlanCacheStats{}, fmt.Errorf("%w: native scalar lookup view: %v", ErrHybridSearchStaleIndex, err)
	}
	defer view.close()
	generation, err := vectorIndexDocumentGeneration(view.snapshot, view.catalog)
	if err != nil {
		return nil, nativeScalarPlanCacheStats{}, err
	}
	leaves := filter.And
	if len(leaves) == 0 {
		leaves = []HybridScalarFilter{*filter}
	}
	plan := &nativeScalarFilterExecution{
		sourceGeneration: generation,
		clauses:          make([]nativeScalarClause, 0, len(leaves)),
	}
	for _, leaf := range leaves {
		def, ok := findIndex(view.catalog.meta.Indexes, leaf.IndexName)
		if !ok {
			return nil, nativeScalarPlanCacheStats{}, fmt.Errorf("%w: native scalar index %q is unavailable", ErrHybridSearchIndexUnavailable, leaf.IndexName)
		}
		clause, err := compileNativeScalarClause(def, leaf)
		if err != nil {
			return nil, nativeScalarPlanCacheStats{}, err
		}
		plan.clauses = append(plan.clauses, clause)
	}

	vectorGeneration, vectorMutationSeq, vectorGenerationValid := index.nativeScalarPlanCacheGeneration()
	cacheKey := nativeScalarPlanCacheKey{
		vectorIndex:       index,
		sourceGeneration:  generation,
		vectorGeneration:  vectorGeneration,
		vectorMutationSeq: vectorMutationSeq,
		vectorSchema:      nativeScalarVectorSchemaIdentity(vectorDef),
		scalarSchema:      nativeScalarSchemaIdentity(view.catalog.meta.Indexes),
		filterIdentity:    nativeScalarFilterIdentity(plan.clauses),
		probeLimit:        nativeScalarProbeLimit,
		exactSafetyCap:    nativeScalarExactSafetyCap,
		annSeedProbeLimit: nativeScalarANNSeedProbeLimit,
		annSeedLimit:      nativeScalarANNSeedLimit,
	}
	cacheable := vectorGenerationValid && vectorGeneration == generation
	var cacheStats nativeScalarPlanCacheStats
	if cacheable {
		if cached, stats := c.nativeScalarPlanCacheGet(cacheKey); cached != nil {
			return cached, stats, nil
		} else {
			cacheStats = stats
		}
	} else {
		cacheStats = c.nativeScalarPlanCacheSnapshot(nativeScalarPlanCacheStats{generationBypasses: 1})
	}
	finish := func(plan *nativeScalarFilterExecution) (*nativeScalarFilterExecution, nativeScalarPlanCacheStats, error) {
		if !cacheable {
			return plan, cacheStats, nil
		}
		cached, stats := c.nativeScalarPlanCachePut(cacheKey, plan, cacheStats)
		return cached, stats, nil
	}

	probes := make([]nativeScalarLeafProbe, 0, len(leaves))
	for _, leaf := range leaves {
		set, inputIDs, truncated, err := view.leafProbe(leaf, nativeScalarProbeLimit)
		if err != nil {
			return nil, cacheStats, err
		}
		plan.probeIDs += inputIDs
		if truncated {
			plan.probeTruncated++
		}
		probes = append(probes, nativeScalarLeafProbe{set: set, truncated: truncated})
	}
	complete := make([]hybridScalarAllowSet, 0, len(probes))
	allComplete := true
	for _, probe := range probes {
		if probe.truncated {
			allComplete = false
			continue
		}
		complete = append(complete, probe.set)
	}
	if len(complete) == 0 {
		plan.identity = NativeScalarFilterPlanVectorAligned
		plan.seedIDs = boundedNativeScalarSeedIDs(probes[0].set, nativeScalarANNSeedLimit)
		plan.candidateIDs = uint64(len(plan.seedIDs))
		plan.retainedCandidateIDs = uint64(len(plan.seedIDs))
		plan.refinedCandidateIDs = uint64(len(plan.seedIDs))
		return finish(plan)
	}
	sort.SliceStable(complete, func(i, j int) bool { return len(complete[i]) < len(complete[j]) })
	candidates := complete[0]
	if allComplete {
		for _, set := range complete[1:] {
			for id := range candidates {
				if _, ok := set[id]; !ok {
					delete(candidates, id)
				}
			}
		}
		plan.finiteIDs = candidates
		plan.candidateIDs = uint64(len(candidates))
		plan.retainedCandidateIDs = uint64(len(candidates))
		plan.refinedCandidateIDs = uint64(len(candidates))
		plan.exactScoring = len(candidates) <= nativeScalarExactSafetyCap
		if plan.exactScoring {
			plan.identity = NativeScalarFilterPlanCompleteExact
		} else {
			plan.identity = NativeScalarFilterPlanCompleteFinite
		}
		return finish(plan)
	}
	plan.finiteIDs = candidates
	plan.candidateIDs = uint64(len(candidates))
	plan.retainedCandidateIDs = uint64(len(candidates))
	plan.refinedCandidateIDs = uint64(len(candidates))
	plan.identity = NativeScalarFilterPlanMixed
	plan.exactScoring = len(candidates) <= nativeScalarExactSafetyCap
	return finish(plan)
}

func populateNativeScalarColumnsFromSecondaryIndexes(idx *VectorIndex, snap *backenddb.Snapshot, catalog *collectionCatalog) error {
	if idx == nil || len(idx.scalarDefinitions) == 0 {
		return nil
	}
	rows := make(map[string]map[string][]byte, len(idx.currentNode))
	for _, def := range idx.scalarDefinitions {
		it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionSecondaryRootName(catalog.meta.Name, def.Name), nil, nil, true)
		if err != nil {
			return err
		}
		if it == nil {
			continue
		}
		for it.Valid() {
			if it.IsDeleted() {
				it.Next()
				continue
			}
			key := it.Key()
			n, err := indexComponentLength(def.ValueType, key)
			if err != nil {
				_ = it.Close()
				return err
			}
			id, err := indexKeyDocumentID(def.ValueType, key)
			if err != nil {
				_ = it.Close()
				return err
			}
			if _, current := idx.currentNode[string(id)]; current {
				row := rows[string(id)]
				if row == nil {
					row = make(map[string][]byte)
					rows[string(id)] = row
				}
				if _, duplicate := row[def.Name]; duplicate {
					_ = it.Close()
					return fmt.Errorf("collections: native scalar index %q is multikey for document %q", def.Name, id)
				}
				row[def.Name] = bytes.Clone(key[:n])
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			return err
		}
		if err := it.Close(); err != nil {
			return err
		}
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.scalarColumns = newNativeScalarColumns(idx.scalarDefinitions)
	for nodeID := range idx.nodes {
		documentID := string(idx.nodes[nodeID].documentID)
		var row map[string][]byte
		if idx.currentNode[documentID] == nodeID {
			row = rows[documentID]
		}
		if err := idx.appendNativeScalarRowValuesLocked(row); err != nil {
			return err
		}
	}
	if err := idx.validateNativeScalarColumnLengthsLocked(); err != nil {
		return err
	}
	idx.acknowledgeSearchViewStateLocked()
	idx.searchViewForceFull = true
	idx.searchViewCurrent.Store(false)
	return nil
}

type nativeScalarSearchWork struct {
	visited         int
	scored          int
	eligibleSeeds   int
	seedRowsVisited int
	admitted        int
	underfill       bool
}

type nativeScalarSeedBudget struct {
	rows            int
	scores          int
	planesRemaining int
}

func (p *nativeScalarFilterExecution) exact() bool {
	return p != nil && p.exactScoring
}

func (idx *VectorIndex) searchGraphOnlyWithNativeScalarFilterBuffer(query []float32, topK, efSearch int, plan *nativeScalarFilterExecution, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, vectorIndexNativeSearchState, nativeScalarSearchWork, error) {
	if idx == nil || buffer == nil {
		return nil, vectorIndexNativeSearchState{}, nativeScalarSearchWork{}, errors.New("collections: nil native scalar search state")
	}
	buffer.Reset()
	view := idx.acquireSearchView()
	if view == nil {
		return nil, vectorIndexNativeSearchState{}, nativeScalarSearchWork{}, fmt.Errorf("%w: native_runtime vector index %q has no published scalar generation", ErrVectorIndexSearchUnavailable, idx.name)
	}
	defer idx.releaseSearchView(view)
	state := view.nativeSearchState()
	if plan == nil {
		return nil, state, nativeScalarSearchWork{}, fmt.Errorf("%w: native scalar plan is unavailable", ErrHybridSearchUnsupported)
	}
	// Vector-aligned plans evaluate clauses against scalar columns captured in
	// the immutable graph view. Their bounded lookup probe selects the route
	// and supplies diagnostics only, so a newer probe remains valid while an
	// in-flight mutation continues serving the prior published view. Exact and
	// mixed plans carry finite IDs from the probe and remain generation-bound.
	if plan.sourceGeneration != state.sourceDocumentGeneration &&
		plan.identity != NativeScalarFilterPlanVectorAligned {
		return nil, state, nativeScalarSearchWork{}, fmt.Errorf("%w: native scalar probe generation %d does not match vector generation %d", ErrHybridSearchStaleIndex, plan.sourceGeneration, state.sourceDocumentGeneration)
	}
	results, work, err := view.searchWithNativeScalarFilterBuffer(query, topK, efSearch, plan, buffer)
	if err == nil {
		idx.liveDeltaEnabled.Store(true)
	}
	return results, state, work, err
}
func (p *nativeScalarFilterExecution) refineMixed(view *vectorIndexSearchView, base, delta *nativeScalarBoundMatcher) {
	if p == nil || p.identity != NativeScalarFilterPlanMixed {
		return
	}
	for id := range p.finiteIDs {
		matched := false
		if nodeID, ok := view.deltaCurrentNode[id]; ok && nodeID >= 0 && nodeID < len(view.deltaNodes) {
			node := &view.deltaNodes[nodeID]
			matched = !node.deleted && delta.matchesKnownFinite(nodeID)
		}
		if !matched {
			if nodeID, ok := view.currentNode[id]; ok && nodeID >= 0 && nodeID < len(view.nodes) {
				node := &view.nodes[nodeID]
				matched = !node.deleted && base.matchesKnownFinite(nodeID)
			}
		}
		if !matched {
			delete(p.finiteIDs, id)
		}
	}
	p.candidateIDs = uint64(len(p.finiteIDs))
	p.refinedCandidateIDs = uint64(len(p.finiteIDs))
	p.exactScoring = len(p.finiteIDs) <= nativeScalarExactSafetyCap
}

func bindNativeScalarMatcher(plan *nativeScalarFilterExecution, columns map[string]vectorIndexScalarColumn, rows int, plane string) (nativeScalarBoundMatcher, error) {
	matcher := nativeScalarBoundMatcher{plan: plan}
	for i, clause := range plan.clauses {
		column, ok := columns[clause.indexName]
		if !ok || column.rows != rows {
			return nativeScalarBoundMatcher{}, fmt.Errorf("%w: native scalar %s column %q is missing or misaligned", ErrVectorIndexSearchUnavailable, plane, clause.indexName)
		}
		if i < len(matcher.columns) {
			matcher.columns[i] = column
		} else {
			matcher.overflow = append(matcher.overflow, column)
		}
	}
	return matcher, nil
}

func (view *vectorIndexSearchView) bindNativeScalarMatchers(plan *nativeScalarFilterExecution) (nativeScalarBoundMatcher, nativeScalarBoundMatcher, error) {
	base, err := bindNativeScalarMatcher(plan, view.scalarColumns, len(view.nodes), "base")
	if err != nil {
		return nativeScalarBoundMatcher{}, nativeScalarBoundMatcher{}, err
	}
	if len(view.deltaNodes) == 0 {
		return base, nativeScalarBoundMatcher{}, nil
	}
	delta, err := bindNativeScalarMatcher(plan, view.deltaScalarColumns, len(view.deltaNodes), "delta")
	if err != nil {
		return nativeScalarBoundMatcher{}, nativeScalarBoundMatcher{}, err
	}
	return base, delta, nil
}

func (view *vectorIndexSearchView) searchWithNativeScalarFilterBuffer(query []float32, topK, efSearch int, plan *nativeScalarFilterExecution, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, nativeScalarSearchWork, error) {
	if !view.sourceDocumentRootsValid {
		return nil, nativeScalarSearchWork{}, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, view.name)
	}
	baseMatcher, deltaMatcher, err := view.bindNativeScalarMatchers(plan)
	if err != nil {
		return nil, nativeScalarSearchWork{}, err
	}
	queryNorm, preparedQuery, preparedCosine, err := prepareVectorIndexGraphOnlyQuery(query, view.metric, view.dimensions)
	if err != nil {
		return nil, nativeScalarSearchWork{}, err
	}
	plan.refineMixed(view, &baseMatcher, &deltaMatcher)
	var prepared *preparedFloat32CosineQuery
	if preparedCosine {
		prepared = &preparedQuery
	}
	seedBudget := nativeScalarSeedBudget{
		rows: nativeScalarANNSeedProbeLimit, scores: nativeScalarANNSeedLimit, planesRemaining: 1,
	}
	if len(view.deltaNodes) == 0 {
		results, work, err := searchVectorIndexViewPlaneNativeScalar(query, queryNorm, prepared, topK, efSearch, view.nodes, view.entry, view.maxLevel, &baseMatcher, view.currentNode, plan, view, plan.exact(), &seedBudget, &buffer.nativeSearchScratch, &buffer.results, &buffer.idBytes)
		return results, work, err
	}
	seedBudget.planesRemaining = 2
	base, baseWork, err := searchVectorIndexViewPlaneNativeScalar(query, queryNorm, prepared, topK, efSearch, view.nodes, view.entry, view.maxLevel, &baseMatcher, view.currentNode, plan, view, plan.exact(), &seedBudget, &buffer.nativeSearchScratch, &buffer.baseResults, &buffer.baseIDBytes)
	if err != nil {
		return nil, baseWork, err
	}
	delta, deltaWork, err := searchVectorIndexViewPlaneNativeScalar(query, queryNorm, prepared, topK, efSearch, view.deltaNodes, view.deltaEntry, view.deltaMaxLevel, &deltaMatcher, view.deltaCurrentNode, plan, view, plan.exact(), &seedBudget, &buffer.nativeSearchScratch, &buffer.deltaResults, &buffer.deltaIDBytes)
	if err != nil {
		return nil, baseWork, err
	}
	merged, err := mergeVectorIndexViewResults(base, delta, topK, buffer)
	work := nativeScalarSearchWork{
		visited:         baseWork.visited + deltaWork.visited,
		scored:          baseWork.scored + deltaWork.scored,
		eligibleSeeds:   baseWork.eligibleSeeds + deltaWork.eligibleSeeds,
		seedRowsVisited: baseWork.seedRowsVisited + deltaWork.seedRowsVisited,
		admitted:        len(merged),
		underfill:       len(merged) < topK,
	}
	return merged, work, err
}

func searchVectorIndexViewPlaneNativeScalar(query []float32, queryNorm float64, prepared *preparedFloat32CosineQuery, topK, efSearch int, nodes []vectorIndexNode, entry, maxLevel int, matcher *nativeScalarBoundMatcher, currentNode map[string]int, plan *nativeScalarFilterExecution, view *vectorIndexSearchView, exact bool, seedBudget *nativeScalarSeedBudget, scratch *vectorIndexSearchScratch, results *[]VectorIndexSearchResult, resultIDBytes *[]byte) ([]VectorIndexSearchResult, nativeScalarSearchWork, error) {
	runtimeIndex := VectorIndex{metric: view.metric, encoding: view.encoding, dimensions: view.dimensions, m: view.m, efSearch: view.efSearch, nodes: nodes, entry: entry, maxLevel: maxLevel}
	var candidates []vectorIndexCandidate
	work := nativeScalarSearchWork{}
	if exact {
		ids := make([]string, 0, len(plan.finiteIDs))
		for id := range plan.finiteIDs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		candidates = scratch.out[:0]
		for _, id := range ids {
			nodeID, ok := currentNode[id]
			if !ok || nodeID < 0 || nodeID >= len(nodes) {
				continue
			}
			node := &nodes[nodeID]
			if node.deleted || !matcher.matches(nodeID, node.documentID) {
				continue
			}
			work.visited++
			distance := runtimeIndex.distanceToNodeWithPreparedQueryLocked(query, queryNorm, prepared, nodeID)
			work.scored++
			if work.scored&63 == 0 && scratch.context != nil {
				scratch.contextErr = scratch.context.Err()
				if scratch.contextErr != nil {
					return nil, work, scratch.contextErr
				}
			}
			if !math.IsInf(float64(distance), 1) {
				candidates = append(candidates, vectorIndexCandidate{nodeID: nodeID, distance: distance})
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].distance != candidates[j].distance {
				return candidates[i].distance < candidates[j].distance
			}
			return bytes.Compare(nodes[candidates[i].nodeID].documentID, nodes[candidates[j].nodeID].documentID) < 0
		})
		if len(candidates) > topK {
			candidates = candidates[:topK]
		}
		scratch.out = candidates
	} else {
		var err error
		candidates, work, err = runtimeIndex.searchNativeScalarCandidatesLocked(query, queryNorm, prepared, topK, efSearch, matcher, currentNode, plan, seedBudget, scratch)
		if err != nil {
			return nil, work, err
		}
	}
	matched, idBytes := 0, 0
	for _, candidate := range candidates {
		node := nodes[candidate.nodeID]
		if node.deleted || !matcher.matches(candidate.nodeID, node.documentID) {
			continue
		}
		var err error
		idBytes, err = addVectorIndexSearchByteTotal(idBytes, len(node.documentID), math.MaxInt, "result id")
		if err != nil {
			return nil, work, err
		}
		matched++
	}
	*results = resizeVectorIndexSearchResultBuffer(*results, matched)
	*resultIDBytes = resizeVectorIndexSearchByteBuffer(*resultIDBytes, idBytes)
	resultRow, offset := 0, 0
	for _, candidate := range candidates {
		node := nodes[candidate.nodeID]
		if node.deleted || !matcher.matches(candidate.nodeID, node.documentID) {
			continue
		}
		next := offset + len(node.documentID)
		id := (*resultIDBytes)[offset:next:next]
		copy(id, node.documentID)
		(*results)[resultRow] = VectorIndexSearchResult{ID: id, Score: 1 - float64(candidate.distance)}
		resultRow++
		offset = next
	}
	work.admitted = len(*results)
	work.underfill = len(*results) < topK
	if err := scratch.finalContextErr(); err != nil {
		return nil, work, err
	}
	return *results, work, nil
}

func (idx *VectorIndex) searchNativeScalarCandidatesLocked(query []float32, queryNorm float64, prepared *preparedFloat32CosineQuery, topK, efSearch int, matcher *nativeScalarBoundMatcher, currentNode map[string]int, plan *nativeScalarFilterExecution, seedBudget *nativeScalarSeedBudget, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, nativeScalarSearchWork, error) {
	work := nativeScalarSearchWork{}
	if idx.entry < 0 || len(idx.nodes) == 0 || topK <= 0 {
		return nil, work, nil
	}
	if scratch == nil {
		return nil, work, errors.New("collections: native scalar search scratch is nil")
	}
	limit := efSearch
	if limit <= 0 {
		limit = idx.efSearch
	}
	limit = maxInt(limit, topK)
	explorationLimit := len(idx.nodes)
	if limit <= math.MaxInt/nativeScalarANNVisitFactor {
		explorationLimit = minInt(explorationLimit, limit*nativeScalarANNVisitFactor)
	}
	scratch.explorationLimit = explorationLimit
	visited, mark := scratch.nextVisitedEpoch(len(idx.nodes))
	queue := scratch.queue[:0]
	navigation := scratch.best[:0]
	eligible := scratch.liveBest[:0]

	seedEligibleRegion := plan.identity == NativeScalarFilterPlanVectorAligned
	if seedEligibleRegion && seedBudget != nil && seedBudget.rows > 0 &&
		seedBudget.scores > 0 && seedBudget.planesRemaining > 0 {
		planeProbeLimit := (seedBudget.rows + seedBudget.planesRemaining - 1) / seedBudget.planesRemaining
		planeScoreLimit := (seedBudget.scores + seedBudget.planesRemaining - 1) / seedBudget.planesRemaining
		seedBudget.planesRemaining--
		planeRowsRemaining := planeProbeLimit
		probeRows := minInt(len(idx.nodes), planeProbeLimit)
		seedBuckets := minInt(probeRows, nativeScalarANNSeedLimit)
		// Preserve at least three quarters of the plane's score budget for the
		// ordinary entry and graph navigation. Tiny ef/top-k searches must not
		// spend their entire frontier on scalar-derived entries.
		seedScoreLimit := minInt(planeScoreLimit, explorationLimit/4)
		stride := maxInt(1, seedBuckets*3/5)
		for {
			left, right := stride, seedBuckets
			for right != 0 {
				left, right = right, left%right
			}
			if left == 1 {
				break
			}
			stride++
		}
		seedScores := 0
		for _, id := range plan.seedIDs {
			if seedScores >= seedScoreLimit || seedBudget.scores <= 0 {
				break
			}
			nodeID, ok := currentNode[id]
			if !ok || nodeID < 0 || nodeID >= len(idx.nodes) || visited[nodeID] == mark {
				continue
			}
			node := &idx.nodes[nodeID]
			if node.deleted || !matcher.matches(nodeID, node.documentID) {
				continue
			}
			visited[nodeID] = mark
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNorm, prepared, nodeID)
			work.scored++
			if work.scored&63 == 0 && scratch.context != nil {
				scratch.contextErr = scratch.context.Err()
				if scratch.contextErr != nil {
					return nil, work, scratch.contextErr
				}
			}
			seedScores++
			seedBudget.scores--
			if math.IsInf(float64(distance), 1) {
				continue
			}
			candidate := vectorIndexCandidate{nodeID: nodeID, distance: distance}
			queue.push(candidate)
			navigation.pushBounded(candidate, explorationLimit)
			eligible.pushBounded(candidate, topK)
			work.eligibleSeeds++
		}
		for ordinal := 0; ordinal < seedBuckets && seedScores < seedScoreLimit &&
			seedBudget.rows > 0 && planeRowsRemaining > 0; ordinal++ {
			bucket := ordinal * stride % seedBuckets
			start := bucket * probeRows / seedBuckets
			end := (bucket + 1) * probeRows / seedBuckets
			for probe := start; probe < end && seedBudget.rows > 0 && planeRowsRemaining > 0; probe++ {
				// Integer stratification visits every row for small graphs and
				// evenly covers the full immutable ordinal space for large graphs.
				nodeID := int(uint64(probe) * uint64(len(idx.nodes)) / uint64(probeRows))
				work.seedRowsVisited++
				if work.seedRowsVisited&63 == 0 && scratch.context != nil {
					scratch.contextErr = scratch.context.Err()
					if scratch.contextErr != nil {
						return nil, work, scratch.contextErr
					}
				}
				seedBudget.rows--
				planeRowsRemaining--
				node := &idx.nodes[nodeID]
				if node.deleted || !matcher.matches(nodeID, node.documentID) {
					continue
				}
				clusterStart := nodeID
				for clusterStart > 0 && seedBudget.rows > 0 && planeRowsRemaining > 0 {
					previous := clusterStart - 1
					work.seedRowsVisited++
					if work.seedRowsVisited&63 == 0 && scratch.context != nil {
						scratch.contextErr = scratch.context.Err()
						if scratch.contextErr != nil {
							return nil, work, scratch.contextErr
						}
					}
					seedBudget.rows--
					planeRowsRemaining--
					previousNode := &idx.nodes[previous]
					if previousNode.deleted || !matcher.matches(previous, previousNode.documentID) {
						break
					}
					clusterStart = previous
				}
				clusterScoreLimit := minInt(topK, seedScoreLimit-seedScores)
				clusterScores := 0
				for candidateID := clusterStart; candidateID < len(idx.nodes) &&
					clusterScores < clusterScoreLimit; candidateID++ {
					if candidateID > nodeID {
						if seedBudget.rows <= 0 || planeRowsRemaining <= 0 {
							break
						}
						work.seedRowsVisited++
						if work.seedRowsVisited&63 == 0 && scratch.context != nil {
							scratch.contextErr = scratch.context.Err()
							if scratch.contextErr != nil {
								return nil, work, scratch.contextErr
							}
						}
						seedBudget.rows--
						planeRowsRemaining--
					}
					candidateNode := &idx.nodes[candidateID]
					if candidateNode.deleted || !matcher.matches(candidateID, candidateNode.documentID) {
						break
					}
					if visited[candidateID] == mark {
						continue
					}
					visited[candidateID] = mark
					distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNorm, prepared, candidateID)
					work.scored++
					if work.scored&63 == 0 && scratch.context != nil {
						scratch.contextErr = scratch.context.Err()
						if scratch.contextErr != nil {
							return nil, work, scratch.contextErr
						}
					}
					seedScores++
					clusterScores++
					seedBudget.scores--
					if math.IsInf(float64(distance), 1) {
						continue
					}
					candidate := vectorIndexCandidate{nodeID: candidateID, distance: distance}
					queue.push(candidate)
					navigation.pushBounded(candidate, explorationLimit)
					eligible.pushBounded(candidate, topK)
					work.eligibleSeeds++
				}
				break
			}
		}
	}

	// Keep the ordinary HNSW entry beside the eligible-region samples. This
	// preserves the established global route and handles selective filters
	// whose entry happens to match without mistaking one row for an all-match
	// predicate.
	entryPoint := idx.entry
	if seedEligibleRegion {
		upperLimit := maxInt(0, explorationLimit-2)
		for layer := idx.maxLevel; layer > 0 && work.scored < upperLimit; layer-- {
			entryPoint = idx.greedyNearestAtLayerBoundedLocked(query, queryNorm, prepared, entryPoint, layer, upperLimit, &work.scored)
		}
	} else {
		// Preserve the established finite and mixed routes.
		for layer := idx.maxLevel; layer > 0; layer-- {
			entryPoint = idx.greedyNearestAtLayerLocked(query, queryNorm, prepared, entryPoint, layer)
		}
	}
	if work.scored < explorationLimit && visited[entryPoint] != mark {
		entryDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNorm, prepared, entryPoint)
		work.scored++
		if !math.IsInf(float64(entryDistance), 1) {
			visited[entryPoint] = mark
			entryCandidate := vectorIndexCandidate{nodeID: entryPoint, distance: entryDistance}
			queue.push(entryCandidate)
			navigation.pushBounded(entryCandidate, explorationLimit)
			node := &idx.nodes[entryPoint]
			if !node.deleted && matcher.matches(entryPoint, node.documentID) {
				eligible.pushBounded(entryCandidate, topK)
			}
		}
	}

search:
	for len(queue) > 0 {
		current := queue.pop()
		if len(navigation) >= explorationLimit && vectorIndexCandidateWorse(current, navigation[0]) {
			break
		}
		for _, neighbor := range idx.layerNeighborsLocked(current.nodeID, 0) {
			nodeID := neighbor.nodeID
			if nodeID < 0 || nodeID >= len(idx.nodes) || visited[nodeID] == mark {
				continue
			}
			if work.scored >= explorationLimit {
				break search
			}
			visited[nodeID] = mark
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNorm, prepared, nodeID)
			work.scored++
			if work.scored&63 == 0 && scratch.context != nil {
				scratch.contextErr = scratch.context.Err()
				if scratch.contextErr != nil {
					break search
				}
			}
			if math.IsInf(float64(distance), 1) {
				continue
			}
			candidate := vectorIndexCandidate{nodeID: nodeID, distance: distance}
			if len(navigation) < explorationLimit || vectorIndexCandidateLess(candidate, navigation[0]) {
				queue.push(candidate)
				navigation.pushBounded(candidate, explorationLimit)
			}
			node := &idx.nodes[nodeID]
			if !node.deleted && matcher.matches(nodeID, node.documentID) {
				eligible.pushBounded(candidate, topK)
			}
		}
	}
	work.visited = work.seedRowsVisited + work.scored
	scratch.explored = work.scored
	scratch.queue = queue[:0]
	scratch.best = navigation[:0]
	scratch.liveBest = eligible[:0]
	out := scratch.out[:0]
	out = append(out, eligible...)
	scratch.out = out
	sortVectorIndexCandidates(out)
	if err := scratch.finalContextErr(); err != nil {
		return nil, work, err
	}
	return out, work, nil
}
