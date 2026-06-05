package collections

import (
	"errors"
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type collectionVectorIndexPreparedSearchCacheEntry struct {
	ready      chan struct{}
	building   bool
	commitSeq  uint64
	systemRoot uint64
	prepared   *collectionVectorIndexPreparedSearch
	response   VectorIndexSearchResponse
	err        error
}

type collectionVectorIndexPreparedSearch struct {
	mu sync.RWMutex

	key        string
	indexName  string
	commitSeq  uint64
	systemRoot uint64
	response   VectorIndexSearchResponse

	pack       *columnHNSWSearchPackPreparedView
	packStatus columnHNSWSearchPackPreparedStatus
	routeStats vectorIndexSearchRouteStats
	closed     bool
}

type collectionVectorIndexPreparedSearchCacheSnapshot struct {
	Entries                    int
	BuildingEntries            int
	ActiveHandles              int64
	ActiveMappedBytes          int64
	ActiveHeapCopyBytes        int64
	ActiveDerivedMetadataBytes int64
	CacheHits                  uint64
	CacheMisses                uint64
	CacheWaits                 uint64
	CacheBuilds                uint64
	Invalidations              uint64
	Closes                     uint64
	Errors                     uint64
}

type collectionVectorIndexPreparedSearchAcquireStats struct {
	HNSWSearchPackCacheHits   uint64
	HNSWSearchPackCacheMisses uint64
	HNSWSearchPackCacheWaits  uint64
	HNSWSearchPackCacheBuilds uint64
}

func (s collectionVectorIndexPreparedSearchAcquireStats) apply(stats *VectorIndexSearchStats) {
	if stats == nil {
		return
	}
	stats.HNSWSearchPackCacheHits += s.HNSWSearchPackCacheHits
	stats.HNSWSearchPackCacheMisses += s.HNSWSearchPackCacheMisses
	stats.HNSWSearchPackCacheWaits += s.HNSWSearchPackCacheWaits
	stats.HNSWSearchPackCacheBuilds += s.HNSWSearchPackCacheBuilds
}

var collectionVectorIndexPreparedSearchBuildHookForTest struct {
	mu sync.Mutex
	fn func(indexName string)
}

func callCollectionVectorIndexPreparedSearchBuildHookForTest(indexName string) {
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	hook := collectionVectorIndexPreparedSearchBuildHookForTest.fn
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	if hook != nil {
		hook(indexName)
	}
}

func (c *Collection) acquireCollectionVectorIndexPreparedSearch(opts VectorIndexSearchOptions) (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, collectionVectorIndexPreparedSearchAcquireStats, error) {
	var response VectorIndexSearchResponse
	var acquireStats collectionVectorIndexPreparedSearchAcquireStats
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return nil, response, acquireStats, err
	}
	if opts.MaxDecodedBlocks < 0 {
		return nil, response, acquireStats, errors.New("collections: vector index search max_decoded_blocks cannot be negative")
	}
	if c == nil {
		return nil, response, acquireStats, errCollectionNil
	}
	if c.db == nil {
		return nil, response, acquireStats, errCollectionDBNil
	}
	if c.manager != nil && c.manager.isClosing() {
		return nil, response, acquireStats, backenddb.ErrClosed
	}
	commitSeq, systemRoot := dbCommitSeqAndSystemRoot(c.db)
	if commitSeq == 0 || systemRoot == 0 || c.db.IsClosing() {
		return nil, response, acquireStats, backenddb.ErrClosed
	}

	for {
		var oldPrepared *collectionVectorIndexPreparedSearch
		c.vectorBufferedSearchMu.Lock()
		if c.vectorBufferedSearch == nil {
			c.vectorBufferedSearch = make(map[string]*collectionVectorIndexPreparedSearchCacheEntry)
		}
		entry := c.vectorBufferedSearch[opts.IndexName]
		if entry != nil && entry.building {
			ready := entry.ready
			c.vectorBufferedSearchWaits++
			acquireStats.HNSWSearchPackCacheWaits++
			c.vectorBufferedSearchMu.Unlock()
			<-ready
			commitSeq, systemRoot = dbCommitSeqAndSystemRoot(c.db)
			if commitSeq == 0 || systemRoot == 0 || c.db.IsClosing() {
				return nil, response, acquireStats, backenddb.ErrClosed
			}
			continue
		}
		if entry != nil && entry.commitSeq == commitSeq && entry.systemRoot == systemRoot {
			if entry.err != nil {
				delete(c.vectorBufferedSearch, opts.IndexName)
				c.vectorBufferedSearchErrors++
				response = entry.response
				err := entry.err
				c.vectorBufferedSearchMu.Unlock()
				return nil, response, acquireStats, err
			}
			prepared := entry.prepared
			c.vectorBufferedSearchHits++
			acquireStats.HNSWSearchPackCacheHits++
			c.vectorBufferedSearchMu.Unlock()
			if prepared != nil && prepared.readyForCurrentSearch() {
				return prepared, prepared.responseForSearch(), acquireStats, nil
			}
			c.invalidateCollectionVectorIndexPreparedSearch(opts.IndexName, prepared)
			continue
		}
		if entry != nil {
			delete(c.vectorBufferedSearch, opts.IndexName)
			oldPrepared = entry.prepared
			c.vectorBufferedSearchInvalidations++
		}
		entry = &collectionVectorIndexPreparedSearchCacheEntry{
			ready:      make(chan struct{}),
			building:   true,
			commitSeq:  commitSeq,
			systemRoot: systemRoot,
		}
		c.vectorBufferedSearch[opts.IndexName] = entry
		c.vectorBufferedSearchMisses++
		c.vectorBufferedSearchBuilds++
		acquireStats.HNSWSearchPackCacheMisses++
		acquireStats.HNSWSearchPackCacheBuilds++
		c.vectorBufferedSearchMu.Unlock()

		if oldPrepared != nil {
			_ = oldPrepared.Close()
		}

		callCollectionVectorIndexPreparedSearchBuildHookForTest(opts.IndexName)
		prepared, buildResponse, buildErr := c.openCollectionVectorIndexPreparedSearch(opts)
		if prepared != nil {
			entry.commitSeq = prepared.commitSeq
			entry.systemRoot = prepared.systemRoot
		}
		entry.response = buildResponse
		entry.err = buildErr

		if buildErr == nil && c.manager != nil && !c.manager.registerCollectionHandleIfOpen(c) {
			buildErr = backenddb.ErrClosed
		}
		stored := false
		c.vectorBufferedSearchMu.Lock()
		closing := buildErr == nil && ((c.manager != nil && c.manager.isClosing()) || c.db == nil || c.db.IsClosing())
		if closing {
			buildErr = backenddb.ErrClosed
		}
		entry.prepared = prepared
		entry.building = false
		if c.vectorBufferedSearch[opts.IndexName] == entry {
			if buildErr != nil {
				delete(c.vectorBufferedSearch, opts.IndexName)
				c.vectorBufferedSearchErrors++
			} else {
				stored = true
			}
		}
		close(entry.ready)
		c.vectorBufferedSearchMu.Unlock()

		if !stored && prepared != nil {
			_ = prepared.Close()
		}
		if buildErr != nil {
			return nil, buildResponse, acquireStats, buildErr
		}
		return prepared, buildResponse, acquireStats, nil
	}
}

func (c *Collection) openCollectionVectorIndexPreparedSearch(opts VectorIndexSearchOptions) (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	if c == nil {
		return nil, response, errCollectionNil
	}
	if c.db == nil {
		return nil, response, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, response, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()

	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, response, err
	}
	if catalog == nil {
		return nil, response, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, opts.IndexName)
	if !ok {
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires a declared vector index", ErrIndexNotFound, opts.IndexName)
	}
	response.IndexName = def.Name
	response.Strategy = def.Strategy
	switch def.Strategy {
	case VectorIndexStrategyNativeRuntime:
		response.Status = VectorIndexStatus{Name: def.Name, Strategy: def.Strategy, State: VectorIndexStateNativeRuntime, Reason: VectorIndexReasonNativeRuntime}
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires an explicit column_graph index; native_runtime cannot serve the no-document hnsw_search_pack_v1 route", ErrVectorIndexSearchUnavailable, def.Name)
	case VectorIndexStrategyColumnGraph:
	default:
		response.Status = VectorIndexStatus{Name: def.Name, Strategy: def.Strategy, State: VectorIndexStateColumnGraphUnavailable, Reason: VectorIndexReasonUnsupportedStrategy}
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires column_graph strategy; got unsupported strategy %q", ErrVectorIndexSearchUnavailable, def.Name, def.Strategy)
	}
	if def.Metric != VectorMetricCosine {
		response.Status = VectorIndexStatus{Name: def.Name, Strategy: def.Strategy, State: VectorIndexStateColumnGraphUnavailable, Reason: VectorIndexReasonColumnGraphUnsupportedMetric}
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires cosine column_graph state; got metric %q", ErrVectorIndexSearchUnavailable, def.Name, def.Metric)
	}

	def, graph, view, err := c.columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(def.Name, snap)
	if err != nil {
		status, statusErr := c.columnGraphVectorIndexStatusAtSnapshot(def.Name, snap)
		if statusErr != nil {
			if errors.Is(statusErr, ErrIndexNotFound) {
				return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires a declared vector index", ErrIndexNotFound, def.Name)
			}
			return nil, response, statusErr
		}
		status = failClosedColumnGraphReaderOpenStatus(def, status)
		response.Status = status
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires loaded column_graph state: state=%s reason=%s", ErrVectorIndexSearchUnavailable, def.Name, status.State, status.Reason)
	}
	readerCatalog := view.Catalog
	if readerCatalog == nil || readerCatalog.meta.Options.ColumnStore == nil {
		return nil, response, errors.New("collections: column_graph prepared collection search missing snapshot catalog")
	}
	response.IndexName = def.Name
	response.Strategy = def.Strategy
	response.Path = VectorIndexSearchPathColumnGraphNativeReader
	response.Status = VectorIndexStatus{Name: def.Name, Strategy: def.Strategy, State: VectorIndexStateColumnGraphLoaded, Loaded: true}

	key, err := collectionVectorIndexPreparedSearchCacheKey(readerCatalog.meta.Name, view.AssetNamespace, def, graph, view.VectorIndexState)
	if err != nil {
		return nil, response, err
	}
	if !columnVectorGraphDocumentIDStatePresent(view.VectorIndexState) {
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires vector-index document-id state for no-document result IDs; rebuild the vector index", ErrVectorIndexSearchUnavailable, def.Name)
	}
	if err := validateColumnVectorGraphDocumentIDStateAssetPayload(c.db.ColumnAssetRootDir(), readerCatalog.meta.Name, *readerCatalog.meta.Options.ColumnStore, def, graph, view.VectorIndexState); err != nil {
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires valid vector-index document-id state for no-document result IDs; rebuild the vector index", ErrVectorIndexSearchUnavailable, def.Name)
	}
	pack, packStatus, packOpenNanos, packErr := c.openColumnHNSWSearchPackPreparedViewForReader(readerCatalog.meta.Name, *readerCatalog.meta.Options.ColumnStore, def, graph, view.VectorIndexState)
	if packErr != nil {
		response.Stats = VectorIndexSearchStats{}
		vectorIndexSearchRouteStatsForHNSWSearchPackFastStatus(vectorIndexSearchRouteStats{}, columnHNSWSearchPackPreparedStatusInvalid).apply(&response.Stats)
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires valid hnsw_search_pack_v1 state; rebuild the vector index before using the buffered no-document route", ErrVectorIndexSearchUnavailable, def.Name)
	}
	if packStatus != columnHNSWSearchPackPreparedStatusDirect && packStatus != columnHNSWSearchPackPreparedStatusHeap {
		if pack != nil {
			_ = pack.Close()
		}
		response.Stats = VectorIndexSearchStats{}
		vectorIndexSearchRouteStatsForHNSWSearchPackFastStatus(vectorIndexSearchRouteStats{}, packStatus).apply(&response.Stats)
		return nil, response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires healthy hnsw_search_pack_v1 state; rebuild the vector index before using the buffered no-document route", ErrVectorIndexSearchUnavailable, def.Name)
	}
	routeStats := vectorIndexSearchRouteStatsForHNSWSearchPackRoute(pack.routeStats(packStatus, packOpenNanos))
	return &collectionVectorIndexPreparedSearch{
		key:        key,
		indexName:  def.Name,
		commitSeq:  snapshotCommitSeq(snap),
		systemRoot: snapshotSystemRoot(snap),
		response:   response,
		pack:       pack,
		packStatus: packStatus,
		routeStats: routeStats,
	}, response, nil
}

func collectionVectorIndexPreparedSearchCacheKey(collection string, namespace string, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (string, error) {
	key, err := columnVectorGraphSharedPreparedSearchCacheKey(collection, namespace, def, graph, state)
	if err != nil {
		return "", err
	}
	return "collection_buffered_hnsw_search_pack_v1|" + key, nil
}

func (p *collectionVectorIndexPreparedSearch) readyForCurrentSearch() bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.pack == nil {
		return false
	}
	status := p.pack.fastStatus(p.packStatus)
	return status == columnHNSWSearchPackPreparedStatusDirect || status == columnHNSWSearchPackPreparedStatusHeap
}

func (p *collectionVectorIndexPreparedSearch) responseForSearch() VectorIndexSearchResponse {
	if p == nil {
		return VectorIndexSearchResponse{}
	}
	response := p.response
	response.Stats = VectorIndexSearchStats{}
	response.Results = nil
	return response
}

func (p *collectionVectorIndexPreparedSearch) SearchWithBuffer(opts VectorIndexSearchOptions, statsMode columnVectorGraphNativeSearchStatsMode, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	previousResults := buffer.results
	buffer.resetView()
	response := p.responseForSearch()
	if p == nil {
		clear(previousResults)
		return response, errors.New("collections: nil collection vector index prepared search")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.pack == nil {
		clear(previousResults)
		response.Stats = VectorIndexSearchStats{HNSWSearchPackClosed: 1, HNSWSearchPackFallbacks: 1}
		return response, errColumnHNSWSearchPackPreparedViewClosed
	}
	status := p.pack.fastStatus(p.packStatus)
	if status != columnHNSWSearchPackPreparedStatusDirect && status != columnHNSWSearchPackPreparedStatusHeap {
		clear(previousResults)
		routeStats := collectionVectorIndexPreparedSearchRouteStatsForUnavailable(p.routeStats, status)
		routeStats.apply(&response.Stats)
		return response, columnHNSWSearchPackStatusError(status)
	}
	results, searchStats, err := p.pack.searchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:           opts.TopK,
		EfSearch:       opts.EfSearch,
		ScoreBatchMode: opts.scoreBatchMode,
		StatsMode:      statsMode,
		QueryMode:      columnVectorGraphNativeSearchQueryModeExact,
	}, &buffer.searchScratch)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, columnPhysicalRowReaderStats{})
	p.routeStats.apply(&response.Stats)
	if err != nil {
		clear(previousResults)
		return response, err
	}
	response.Results, err = copyVectorIndexSearchResultsToBuffer(results, buffer, previousResults)
	if err != nil {
		return response, err
	}
	return response, nil
}

func collectionVectorIndexPreparedSearchRouteStatsForUnavailable(cached vectorIndexSearchRouteStats, status columnHNSWSearchPackPreparedStatus) vectorIndexSearchRouteStats {
	stats := cached
	stats.SearchRouteHNSWSearchPack = 0
	stats.clearHNSWSearchPackAvailability()
	stats.applyHNSWSearchPackStatus(status)
	return stats
}

func (p *collectionVectorIndexPreparedSearch) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var err error
	if p.pack != nil {
		err = p.pack.Close()
		p.pack = nil
	}
	return err
}

func (p *collectionVectorIndexPreparedSearch) stats() mappedresource.Stats {
	if p == nil {
		return mappedresource.Stats{}
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.pack == nil || p.pack.manager == nil {
		return mappedresource.Stats{}
	}
	return p.pack.manager.Stats()
}

func (c *Collection) invalidateCollectionVectorIndexPreparedSearch(indexName string, prepared *collectionVectorIndexPreparedSearch) {
	if c == nil || indexName == "" {
		return
	}
	for {
		var closePrepared *collectionVectorIndexPreparedSearch
		c.vectorBufferedSearchMu.Lock()
		entry := c.vectorBufferedSearch[indexName]
		if entry != nil && entry.building {
			ready := entry.ready
			c.vectorBufferedSearchWaits++
			c.vectorBufferedSearchMu.Unlock()
			<-ready
			continue
		}
		if entry != nil && (prepared == nil || entry.prepared == prepared) {
			delete(c.vectorBufferedSearch, indexName)
			closePrepared = entry.prepared
			c.vectorBufferedSearchInvalidations++
		}
		c.vectorBufferedSearchMu.Unlock()
		if closePrepared != nil {
			_ = closePrepared.Close()
		}
		return
	}
}

func (c *Collection) hasCollectionVectorIndexPreparedSearchCacheEntries() bool {
	if c == nil {
		return false
	}
	c.vectorBufferedSearchMu.Lock()
	defer c.vectorBufferedSearchMu.Unlock()
	return len(c.vectorBufferedSearch) > 0
}

func (c *Collection) closeCollectionVectorIndexPreparedSearchCache() error {
	if c == nil {
		return nil
	}
	var entries []*collectionVectorIndexPreparedSearchCacheEntry
	for {
		var waits []chan struct{}
		c.vectorBufferedSearchMu.Lock()
		for _, entry := range c.vectorBufferedSearch {
			if entry == nil {
				continue
			}
			if entry.building {
				waits = append(waits, entry.ready)
			}
		}
		if len(waits) == 0 {
			for _, entry := range c.vectorBufferedSearch {
				entries = append(entries, entry)
			}
			c.vectorBufferedSearch = nil
			if len(entries) > 0 {
				c.vectorBufferedSearchCloses += uint64(len(entries))
			}
			c.vectorBufferedSearchMu.Unlock()
			break
		}
		c.vectorBufferedSearchMu.Unlock()
		for _, ready := range waits {
			<-ready
		}
	}
	var closeErr error
	for _, entry := range entries {
		if entry != nil && entry.prepared != nil {
			closeErr = errors.Join(closeErr, entry.prepared.Close())
		}
	}
	if c.manager != nil && !c.hasDirtyNativeVectorIndex() {
		c.manager.unregisterCollectionHandle(c)
	}
	return closeErr
}

func (m *CollectionManager) closeCollectionVectorIndexPreparedSearchCaches() error {
	if m == nil {
		return nil
	}
	m.collectionsMu.RLock()
	collections := make([]*Collection, 0, len(m.collections))
	for collection := range m.collections {
		if collection != nil {
			collections = append(collections, collection)
		}
	}
	m.collectionsMu.RUnlock()
	var closeErr error
	for _, collection := range collections {
		closeErr = errors.Join(closeErr, collection.closeCollectionVectorIndexPreparedSearchCache())
	}
	return closeErr
}

func (c *Collection) collectionVectorIndexPreparedSearchCacheSnapshot() collectionVectorIndexPreparedSearchCacheSnapshot {
	if c == nil {
		return collectionVectorIndexPreparedSearchCacheSnapshot{}
	}
	c.vectorBufferedSearchMu.Lock()
	defer c.vectorBufferedSearchMu.Unlock()
	snap := collectionVectorIndexPreparedSearchCacheSnapshot{
		CacheHits:     c.vectorBufferedSearchHits,
		CacheMisses:   c.vectorBufferedSearchMisses,
		CacheWaits:    c.vectorBufferedSearchWaits,
		CacheBuilds:   c.vectorBufferedSearchBuilds,
		Invalidations: c.vectorBufferedSearchInvalidations,
		Closes:        c.vectorBufferedSearchCloses,
		Errors:        c.vectorBufferedSearchErrors,
	}
	for _, entry := range c.vectorBufferedSearch {
		if entry == nil {
			continue
		}
		snap.Entries++
		if entry.building {
			snap.BuildingEntries++
			continue
		}
		if entry.prepared != nil {
			snap.add(entry.prepared.stats())
		}
	}
	return snap
}

func (s *collectionVectorIndexPreparedSearchCacheSnapshot) add(stats mappedresource.Stats) {
	if s == nil {
		return
	}
	s.ActiveHandles += stats.ActiveHandles
	s.ActiveMappedBytes += stats.ActiveMappedBytes
	s.ActiveHeapCopyBytes += stats.ActiveHeapCopyBytes
	s.ActiveDerivedMetadataBytes += stats.ActiveDerivedMetadataBytes
}
