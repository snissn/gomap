package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

var errColumnVectorGraphSharedPreparedSearchNotEligible = errors.New("collections: column_graph shared prepared search requires an mmap-direct prepared view")

// columnVectorGraphSharedPreparedSearch is the local #1735 seam for immutable
// prepared column_graph state. It is intentionally a collection-scoped,
// ref-counted owner around the existing generic mappedresource handles and
// prepared metadata; it does not define a new vector sidecar lifecycle. The key
// includes graph/vector-index/base-manifest identity so a future DB-wide
// mapped-resource manager can replace the backing owner without changing search
// semantics.
type columnVectorGraphSharedPreparedSearch struct {
	key string

	typedVectorSource     *columnVectorGraphTypedColumnVectorSource
	invNormSource         *columnVectorGraphInvNormStateSource
	rowRefSource          *columnVectorGraphRowRefStateSource
	documentIDSource      *columnVectorGraphDocumentIDStateSource
	hnswSearchPack        *columnHNSWSearchPackPreparedView
	hnswSearchPackStatus  columnHNSWSearchPackPreparedStatus
	hnswSearchPackNanos   uint64
	adjacencyLayerSources *columnVectorGraphAdjacencyDirectSources
	preparedSearch        *columnVectorGraphPreparedSearchView
}

type columnVectorGraphSharedPreparedSearchRef struct {
	collection *Collection
	key        string
	holder     *columnVectorGraphSharedPreparedSearch
	once       sync.Once
}

type columnVectorGraphSharedPreparedSearchCacheEntry struct {
	ready    chan struct{}
	building bool
	holder   *columnVectorGraphSharedPreparedSearch
	err      error
	refs     int
}

type columnVectorGraphSharedPreparedSearchCacheSnapshot struct {
	Entries                    int
	Refs                       int
	BuildingEntries            int
	ActiveHandles              int64
	ActiveMappedBytes          int64
	ActiveHeapCopyBytes        int64
	ActiveDerivedMetadataBytes int64
	TotalAcquires              uint64
	TotalReleases              uint64
	CacheHits                  uint64
	CacheMisses                uint64
	CacheWaits                 uint64
	CacheBuilds                uint64
	Hits                       uint64
	Misses                     uint64
	FallbackReads              uint64
	Opens                      uint64
	Closes                     uint64
	Errors                     uint64
}

func (c *Collection) acquireColumnVectorGraphSharedPreparedSearch(key string, build func() (*columnVectorGraphSharedPreparedSearch, error)) (*columnVectorGraphSharedPreparedSearchRef, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if key == "" {
		return nil, errors.New("collections: column_graph shared prepared search key is empty")
	}
	if build == nil {
		return nil, errors.New("collections: column_graph shared prepared search build function is nil")
	}
	for {
		c.vectorPreparedSearchMu.Lock()
		if c.vectorPreparedSearch == nil {
			c.vectorPreparedSearch = make(map[string]*columnVectorGraphSharedPreparedSearchCacheEntry)
		}
		entry := c.vectorPreparedSearch[key]
		if entry == nil {
			entry = &columnVectorGraphSharedPreparedSearchCacheEntry{ready: make(chan struct{}), building: true}
			c.vectorPreparedSearch[key] = entry
			c.vectorPreparedSearchMisses++
			c.vectorPreparedSearchBuilds++
			c.vectorPreparedSearchMu.Unlock()

			holder, err := build()
			if err == nil && holder == nil {
				err = errors.New("collections: column_graph shared prepared search build returned nil holder")
			}
			if err == nil {
				holder.key = key
			}

			c.vectorPreparedSearchMu.Lock()
			entry.holder = holder
			entry.err = err
			entry.building = false
			if err != nil {
				if !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
					delete(c.vectorPreparedSearch, key)
				}
			} else {
				entry.refs = 1
			}
			close(entry.ready)
			c.vectorPreparedSearchMu.Unlock()
			if err != nil {
				return nil, err
			}
			return &columnVectorGraphSharedPreparedSearchRef{collection: c, key: key, holder: holder}, nil
		}
		ready := entry.ready
		if entry.building {
			c.vectorPreparedSearchWaits++
			c.vectorPreparedSearchMu.Unlock()
			<-ready
			continue
		}
		if entry.err != nil {
			err := entry.err
			if !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
				delete(c.vectorPreparedSearch, key)
			}
			c.vectorPreparedSearchMu.Unlock()
			return nil, err
		}
		if entry.holder == nil || !entry.holder.ready() {
			delete(c.vectorPreparedSearch, key)
			c.vectorPreparedSearchMu.Unlock()
			continue
		}
		entry.refs++
		c.vectorPreparedSearchHits++
		holder := entry.holder
		c.vectorPreparedSearchMu.Unlock()
		return &columnVectorGraphSharedPreparedSearchRef{collection: c, key: key, holder: holder}, nil
	}
}

func (r *columnVectorGraphSharedPreparedSearchRef) release() error {
	if r == nil {
		return nil
	}
	var closeErr error
	r.once.Do(func() {
		c := r.collection
		if c == nil || r.key == "" {
			return
		}
		var holder *columnVectorGraphSharedPreparedSearch
		c.vectorPreparedSearchMu.Lock()
		entry := c.vectorPreparedSearch[r.key]
		if entry != nil && entry.holder == r.holder {
			if entry.refs > 0 {
				entry.refs--
			}
			if entry.refs == 0 && !entry.building {
				holder = entry.holder
				delete(c.vectorPreparedSearch, r.key)
			}
		}
		c.vectorPreparedSearchMu.Unlock()
		if holder != nil {
			closeErr = holder.close()
		}
	})
	return closeErr
}

func newColumnVectorGraphSharedPreparedSearchFromReader(reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphSharedPreparedSearch, error) {
	if reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	if columnVectorGraphManifestHasPhysicalAsset(reader.graph) {
		return nil, errors.New("collections: column_graph shared prepared search is disabled for legacy graph-row assets")
	}
	if reader.preparedSearch == nil {
		return nil, errColumnVectorGraphSharedPreparedSearchNotEligible
	}
	if !reader.preparedSearch.ready() {
		return nil, errors.New("collections: column_graph shared prepared search requires ready combined prepared view")
	}
	holder := &columnVectorGraphSharedPreparedSearch{
		typedVectorSource:     reader.typedVectorSource,
		invNormSource:         reader.invNormSource,
		rowRefSource:          reader.rowRefSource,
		documentIDSource:      reader.documentIDSource,
		hnswSearchPack:        reader.hnswSearchPack,
		hnswSearchPackStatus:  reader.hnswSearchPackStatus,
		hnswSearchPackNanos:   reader.hnswSearchPackOpenNanos,
		adjacencyLayerSources: reader.adjacencyLayerSources,
		preparedSearch:        reader.preparedSearch,
	}
	if !holder.ready() {
		return nil, errors.New("collections: column_graph shared prepared search holder is not ready")
	}
	// Ownership of immutable sources transfers to the shared holder. The build
	// reader is temporary; the returned worker reader attaches via a ref below.
	reader.typedVectorSource = nil
	reader.invNormSource = nil
	reader.rowRefSource = nil
	reader.documentIDSource = nil
	reader.hnswSearchPack = nil
	reader.adjacencyLayerSources = nil
	reader.layer0AdjacencySource = nil
	reader.preparedSearch = nil
	return holder, nil
}

func (h *columnVectorGraphSharedPreparedSearch) ready() bool {
	return h != nil && h.typedVectorSource != nil && h.invNormSource != nil && h.rowRefSource != nil && h.documentIDSource != nil && h.adjacencyLayerSources != nil && h.preparedSearch != nil && h.preparedSearch.ready()
}

func (h *columnVectorGraphSharedPreparedSearch) close() error {
	if h == nil {
		return nil
	}
	var closeErr error
	if h.typedVectorSource != nil {
		if err := h.typedVectorSource.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		h.typedVectorSource = nil
	}
	if h.invNormSource != nil {
		if err := h.invNormSource.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		h.invNormSource = nil
	}
	if h.rowRefSource != nil {
		if err := h.rowRefSource.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		h.rowRefSource = nil
	}
	if h.documentIDSource != nil {
		if err := h.documentIDSource.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		h.documentIDSource = nil
	}
	if h.hnswSearchPack != nil {
		if err := h.hnswSearchPack.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		h.hnswSearchPack = nil
	}
	if h.adjacencyLayerSources != nil {
		if err := h.adjacencyLayerSources.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		h.adjacencyLayerSources = nil
	}
	h.preparedSearch = nil
	return closeErr
}

func (r *columnVectorGraphPhysicalRowReader) attachSharedPreparedSearch(ref *columnVectorGraphSharedPreparedSearchRef) error {
	if r == nil {
		return errNilColumnVectorGraphPhysicalRowReader
	}
	if ref == nil || ref.holder == nil || !ref.holder.ready() {
		return errors.New("collections: column_graph shared prepared search ref is not ready")
	}
	h := ref.holder
	r.sharedPreparedSearch = ref
	r.typedVectorSource = h.typedVectorSource
	r.invNormSource = h.invNormSource
	r.rowRefSource = h.rowRefSource
	r.documentIDSource = h.documentIDSource
	r.hnswSearchPack = h.hnswSearchPack
	r.hnswSearchPackStatus = h.hnswSearchPackStatus
	r.hnswSearchPackOpenNanos = h.hnswSearchPackNanos
	r.adjacencyLayerSources = h.adjacencyLayerSources
	if h.adjacencyLayerSources != nil && len(h.adjacencyLayerSources.sources) > 0 {
		r.layer0AdjacencySource = h.adjacencyLayerSources.sources[0]
	}
	r.preparedSearch = h.preparedSearch
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) releaseSharedPreparedSearch() error {
	if r == nil || r.sharedPreparedSearch == nil {
		return nil
	}
	ref := r.sharedPreparedSearch
	r.sharedPreparedSearch = nil
	r.typedVectorSource = nil
	r.invNormSource = nil
	r.rowRefSource = nil
	r.documentIDSource = nil
	r.hnswSearchPack = nil
	r.adjacencyLayerSources = nil
	r.layer0AdjacencySource = nil
	r.preparedSearch = nil
	return ref.release()
}

func (c *Collection) columnVectorGraphSharedPreparedSearchCacheSnapshot() columnVectorGraphSharedPreparedSearchCacheSnapshot {
	if c == nil {
		return columnVectorGraphSharedPreparedSearchCacheSnapshot{}
	}
	c.vectorPreparedSearchMu.Lock()
	defer c.vectorPreparedSearchMu.Unlock()
	snap := columnVectorGraphSharedPreparedSearchCacheSnapshot{
		CacheHits:   c.vectorPreparedSearchHits,
		CacheMisses: c.vectorPreparedSearchMisses,
		CacheWaits:  c.vectorPreparedSearchWaits,
		CacheBuilds: c.vectorPreparedSearchBuilds,
	}
	for _, entry := range c.vectorPreparedSearch {
		if entry == nil || (entry.err != nil && entry.holder == nil && !entry.building) {
			continue
		}
		snap.Entries++
		snap.Refs += entry.refs
		if entry.building {
			snap.BuildingEntries++
		}
		if entry.holder != nil {
			snap.add(entry.holder.stats())
		}
	}
	return snap
}

func (s *columnVectorGraphSharedPreparedSearchCacheSnapshot) add(stats mappedresource.Stats) {
	if s == nil {
		return
	}
	s.ActiveHandles += stats.ActiveHandles
	s.ActiveMappedBytes += stats.ActiveMappedBytes
	s.ActiveHeapCopyBytes += stats.ActiveHeapCopyBytes
	s.ActiveDerivedMetadataBytes += stats.ActiveDerivedMetadataBytes
	s.TotalAcquires += stats.TotalAcquires
	s.TotalReleases += stats.TotalReleases
	s.Hits += stats.Hits
	s.Misses += stats.Misses
	s.FallbackReads += stats.FallbackReads
	s.Opens += stats.Opens
	s.Closes += stats.Closes
	s.Errors += stats.Errors
}

func (h *columnVectorGraphSharedPreparedSearch) stats() mappedresource.Stats {
	if h == nil {
		return mappedresource.Stats{}
	}
	var out mappedresource.Stats
	add := func(stats mappedresource.Stats) {
		out.ActiveHandles += stats.ActiveHandles
		out.ActiveMappedBytes += stats.ActiveMappedBytes
		out.ActiveHeapCopyBytes += stats.ActiveHeapCopyBytes
		out.ActiveDerivedMetadataBytes += stats.ActiveDerivedMetadataBytes
		out.TotalAcquires += stats.TotalAcquires
		out.TotalReleases += stats.TotalReleases
		out.TotalMappedBytes += stats.TotalMappedBytes
		out.TotalHeapCopyBytes += stats.TotalHeapCopyBytes
		out.TotalDerivedMetadataBytes += stats.TotalDerivedMetadataBytes
		out.Hits += stats.Hits
		out.Misses += stats.Misses
		out.FallbackReads += stats.FallbackReads
		out.Opens += stats.Opens
		out.Closes += stats.Closes
		out.Errors += stats.Errors
		out.DirectViewSuccesses += stats.DirectViewSuccesses
		out.DirectViewFailures += stats.DirectViewFailures
	}
	if h.typedVectorSource != nil && h.typedVectorSource.manager != nil {
		add(h.typedVectorSource.manager.Stats())
	}
	if h.invNormSource != nil && h.invNormSource.manager != nil {
		add(h.invNormSource.manager.Stats())
	}
	if h.rowRefSource != nil && h.rowRefSource.manager != nil {
		add(h.rowRefSource.manager.Stats())
	}
	if h.documentIDSource != nil && h.documentIDSource.manager != nil {
		add(h.documentIDSource.manager.Stats())
	}
	if h.hnswSearchPack != nil && h.hnswSearchPack.manager != nil {
		add(h.hnswSearchPack.manager.Stats())
	}
	if h.adjacencyLayerSources != nil {
		for _, source := range h.adjacencyLayerSources.sources {
			if source != nil && source.manager != nil {
				add(source.manager.Stats())
			}
		}
	}
	return out
}

func columnVectorGraphSharedPreparedSearchCacheKey(collection string, namespace string, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (string, error) {
	if collection == "" || namespace == "" || def.Name == "" || graph.IndexName == "" || state.IndexName == "" {
		return "", errors.New("collections: column_graph shared prepared search key requires collection, namespace, graph, and state identity")
	}
	var b bytes.Buffer
	writeKeyString := func(label, value string) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte('|')
	}
	writeKeyInt := func(label string, value int) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(value))
		b.WriteByte('|')
	}
	writeKeyU64 := func(label string, value uint64) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.FormatUint(value, 10))
		b.WriteByte('|')
	}
	writeKeyI64 := func(label string, value int64) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.FormatInt(value, 10))
		b.WriteByte('|')
	}
	writeKeyU32 := func(label string, value uint32) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.FormatUint(uint64(value), 10))
		b.WriteByte('|')
	}
	writeRef := func(prefix string, ref ColumnAssetRef) {
		writeKeyString(prefix+".kind", string(ref.Kind))
		writeKeyString(prefix+".namespace", ref.Namespace)
		writeKeyU64(prefix+".generation", ref.Generation)
		writeKeyU64(prefix+".part_id", ref.PartID)
		writeKeyU32(prefix+".file_id", ref.FileID)
		writeKeyI64(prefix+".offset", ref.Offset)
		writeKeyI64(prefix+".length", ref.Length)
		writeKeyU32(prefix+".checksum", ref.Checksum)
	}

	writeKeyString("collection", collection)
	writeKeyString("namespace", namespace)
	writeKeyString("index", def.Name)
	writeKeyString("field", def.Field)
	writeKeyString("metric", def.Metric.String())
	writeKeyString("encoding", def.Encoding.String())
	writeKeyInt("dims", def.Dimensions)
	writeKeyInt("m", def.M)
	writeKeyInt("ef_construction", def.EfConstruction)
	writeKeyInt("ef_search", def.EfSearch)

	writeKeyString("graph.index", graph.IndexName)
	writeKeyString("graph.field", graph.Field)
	writeKeyString("graph.metric", graph.Metric.String())
	writeKeyString("graph.encoding", graph.Encoding.String())
	writeKeyInt("graph.dims", graph.Dimensions)
	writeKeyInt("graph.m", graph.M)
	writeKeyInt("graph.ef_construction", graph.EfConstruction)
	writeKeyInt("graph.ef_search", graph.EfSearch)
	writeKeyU64("graph.base_generation", graph.BaseManifestGeneration)
	writeKeyU64("graph.base_checksum", graph.BaseManifestChecksum)
	writeKeyU64("graph.base_schema", graph.BaseSchemaHash)
	writeKeyU64("graph.schema", graph.GraphSchemaHash)
	writeKeyInt("graph.rows", graph.RowCount)
	writeKeyInt("graph.layers", graph.AdjacencyLayerCount)
	writeRef("graph.asset", graph.AssetRef)

	writeKeyString("state.index", state.IndexName)
	writeKeyString("state.field", state.Field)
	writeKeyString("state.metric", state.Metric.String())
	writeKeyString("state.encoding", state.Encoding.String())
	writeKeyInt("state.dims", state.Dimensions)
	writeKeyInt("state.m", state.M)
	writeKeyInt("state.ef_construction", state.EfConstruction)
	writeKeyInt("state.ef_search", state.EfSearch)
	writeKeyInt("state.rows", state.RowCount)
	writeKeyU64("state.base_generation", state.BaseManifestGeneration)
	writeKeyU64("state.base_checksum", state.BaseManifestChecksum)
	writeKeyU64("state.base_schema", state.BaseSchemaHash)
	writeKeyInt("state.layers", state.AdjacencyLayerCount)
	assets := append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	sort.SliceStable(assets, func(i, j int) bool {
		if assets[i].Role != assets[j].Role {
			return assets[i].Role < assets[j].Role
		}
		if assets[i].AssetID != assets[j].AssetID {
			return assets[i].AssetID < assets[j].AssetID
		}
		if assets[i].LogicalType != assets[j].LogicalType {
			return assets[i].LogicalType < assets[j].LogicalType
		}
		return assets[i].PhysicalEncoding < assets[j].PhysicalEncoding
	})
	writeKeyInt("state.assets", len(assets))
	for i, asset := range assets {
		prefix := fmt.Sprintf("state.asset.%d", i)
		writeKeyString(prefix+".role", asset.Role)
		writeKeyString(prefix+".asset_id", asset.AssetID)
		writeKeyString(prefix+".logical", asset.LogicalType)
		writeKeyString(prefix+".physical", asset.PhysicalEncoding)
		writeKeyInt(prefix+".rows", asset.RowCount)
		writeKeyU64(prefix+".source_schema", asset.SourceSchemaHash)
		writeKeyI64(prefix+".bytes", asset.AssetBytes)
		writeRef(prefix+".ref", asset.Ref)
	}
	return b.String(), nil
}
