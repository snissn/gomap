package collections

import (
	"errors"
	"fmt"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ErrVectorIndexSearchUnavailable reports that the requested vector index is
// not currently searchable through the selected product path.
var ErrVectorIndexSearchUnavailable = errors.New("collections: vector index search unavailable")

type VectorIndexSearchPath string

const (
	// VectorIndexSearchPathColumnGraphNativeReader searches the persisted
	// column_graph asset through the physical column row reader. It does not
	// build or query a decoded in-memory ColumnVectorGraph.
	VectorIndexSearchPathColumnGraphNativeReader VectorIndexSearchPath = "column_graph_native_reader"
)

type VectorIndexSearchOptions struct {
	IndexName        string
	Query            []float32
	TopK             int
	EfSearch         int
	IncludeDocuments bool
	// MaxDecodedBlocks bounds the generic physical column reader cache used by
	// the column_graph path. Zero uses the reader default.
	MaxDecodedBlocks int
}

type VectorIndexSearcherOptions struct {
	IndexName string
	// MaxDecodedBlocks bounds the generic physical column reader cache used by
	// the column_graph path. Zero uses the reader default.
	MaxDecodedBlocks int
}

type VectorIndexSearcherSearchOptions struct {
	Query            []float32
	TopK             int
	EfSearch         int
	IncludeDocuments bool
}

type VectorIndexSearchResult struct {
	ID       []byte  `json:"id"`
	Ordinal  int     `json:"ordinal"`
	Score    float64 `json:"score"`
	Document []byte  `json:"document,omitempty"`
}

type VectorIndexSearchStats struct {
	Candidates       uint64 `json:"candidates,omitempty"`
	Edges            uint64 `json:"edges,omitempty"`
	CandidateFetches uint64 `json:"candidate_fetches,omitempty"`
	ExpansionFetches uint64 `json:"expansion_fetches,omitempty"`
	ResultFetches    uint64 `json:"result_fetches,omitempty"`
	DocumentsFetched uint64 `json:"documents_fetched,omitempty"`

	RowFetches        uint64 `json:"row_fetches,omitempty"`
	BatchFetches      uint64 `json:"batch_fetches,omitempty"`
	RowsFetched       uint64 `json:"rows_fetched,omitempty"`
	CacheHits         uint64 `json:"cache_hits,omitempty"`
	CacheMisses       uint64 `json:"cache_misses,omitempty"`
	DecodedBlocks     uint64 `json:"decoded_blocks,omitempty"`
	GranulesTouched   uint64 `json:"granules_touched,omitempty"`
	PhysicalBytesRead int64  `json:"physical_bytes_read,omitempty"`
	MaxResidentBytes  int64  `json:"max_resident_bytes,omitempty"`

	OpenGranulesRead      int   `json:"open_granules_read,omitempty"`
	OpenPhysicalBytesRead int64 `json:"open_physical_bytes_read,omitempty"`
}

type VectorIndexSearchResponse struct {
	IndexName string                    `json:"index_name"`
	Strategy  VectorIndexStrategy       `json:"strategy"`
	Path      VectorIndexSearchPath     `json:"path,omitempty"`
	Status    VectorIndexStatus         `json:"status"`
	Stats     VectorIndexSearchStats    `json:"stats,omitempty"`
	Results   []VectorIndexSearchResult `json:"results,omitempty"`
}

// VectorIndexSearcher is a reusable, snapshot-bound vector index search handle.
// It is not concurrency-safe; parallel query workers should open independent
// searchers. Close and reopen the searcher after writes/rebuilds when callers
// need the newest column_graph generation.
type VectorIndexSearcher struct {
	collection *Collection
	indexName  string
	strategy   VectorIndexStrategy
	path       VectorIndexSearchPath
	status     VectorIndexStatus
	snapshot   *backenddb.Snapshot
	catalog    *collectionCatalog
	reader     *columnVectorGraphPhysicalRowReader
	scratch    columnVectorGraphNativeSearchScratch
	readerLast columnPhysicalRowReaderStats
	closed     bool
}

// SearchVectorIndex searches a collection vector index through the public
// collection lifecycle. V4 wires only explicit column_graph indexes to the
// native physical column reader; native_runtime remains reported as native
// rather than silently falling back or pretending to use column storage.
func (c *Collection) SearchVectorIndex(opts VectorIndexSearchOptions) (VectorIndexSearchResponse, error) {
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return VectorIndexSearchResponse{}, err
	}
	searcher, response, err := c.openVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        opts.IndexName,
		MaxDecodedBlocks: opts.MaxDecodedBlocks,
	})
	if err != nil {
		return response, err
	}
	defer func() { _ = searcher.Close() }()
	return searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            opts.Query,
		TopK:             opts.TopK,
		EfSearch:         opts.EfSearch,
		IncludeDocuments: opts.IncludeDocuments,
	})
}

// OpenVectorIndexSearcher opens a reusable search handle for steady-state
// vector queries. Setup/open/decode cost is paid at open; Search then measures
// graph traversal, vector scoring, top-k production, and optional post-top-k
// document fetch.
func (c *Collection) OpenVectorIndexSearcher(opts VectorIndexSearcherOptions) (*VectorIndexSearcher, error) {
	searcher, _, err := c.openVectorIndexSearcher(opts)
	return searcher, err
}

func (c *Collection) openVectorIndexSearcher(opts VectorIndexSearcherOptions) (*VectorIndexSearcher, VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return nil, response, err
	}
	if opts.MaxDecodedBlocks < 0 {
		return nil, response, errors.New("collections: vector index search max_decoded_blocks cannot be negative")
	}
	if c == nil {
		return nil, response, errCollectionNil
	}
	if c.db == nil {
		return nil, response, errCollectionDBNil
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, opts.IndexName)
	if !ok {
		return nil, response, ErrIndexNotFound
	}
	response.IndexName = def.Name
	response.Strategy = def.Strategy
	switch def.Strategy {
	case VectorIndexStrategyNativeRuntime:
		response.Status = VectorIndexStatus{
			Name:     def.Name,
			Strategy: def.Strategy,
			State:    VectorIndexStateNativeRuntime,
			Reason:   VectorIndexReasonNativeRuntime,
		}
		return nil, response, fmt.Errorf("%w: vector index %q uses native_runtime; SearchVectorIndex currently requires an explicit column_graph index", ErrVectorIndexSearchUnavailable, def.Name)
	case VectorIndexStrategyColumnGraph:
	default:
		return nil, response, fmt.Errorf("collections: unsupported vector index strategy %q", def.Strategy)
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, response, backenddb.ErrClosed
	}
	closeOnErr := true
	defer func() {
		if closeOnErr {
			_ = snap.Close()
		}
	}()
	reader, err := c.openColumnVectorGraphPhysicalRowReaderAtSnapshot(def.Name, snap, columnVectorGraphPhysicalRowReaderOptions{
		MaxDecodedBlocks: opts.MaxDecodedBlocks,
	})
	if err != nil {
		status, statusErr := c.columnGraphVectorIndexStatusAtSnapshot(def.Name, snap)
		if statusErr != nil {
			return nil, response, statusErr
		}
		response.Status = status
		return nil, response, fmt.Errorf("%w: column_graph %q is not loaded: state=%s reason=%s: %v", ErrVectorIndexSearchUnavailable, def.Name, status.State, status.Reason, err)
	}
	catalog := reader.catalog
	if catalog == nil {
		_ = reader.Close()
		return nil, response, errors.New("collections: column_graph physical row reader missing snapshot catalog")
	}

	response.Status = VectorIndexStatus{
		Name:     reader.def.Name,
		Strategy: reader.def.Strategy,
		State:    VectorIndexStateColumnGraphLoaded,
		Loaded:   true,
	}
	response.Path = VectorIndexSearchPathColumnGraphNativeReader
	searcher := &VectorIndexSearcher{
		collection: c,
		indexName:  response.IndexName,
		strategy:   response.Strategy,
		path:       response.Path,
		status:     response.Status,
		snapshot:   snap,
		catalog:    catalog,
		reader:     reader,
		readerLast: reader.Stats(),
	}
	closeOnErr = false
	return searcher, response, nil
}

func (s *VectorIndexSearcher) Search(opts VectorIndexSearcherSearchOptions) (VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	if s == nil || s.reader == nil || s.collection == nil {
		return response, errors.New("collections: nil vector index searcher")
	}
	response.IndexName = s.indexName
	response.Strategy = s.strategy
	response.Path = s.path
	response.Status = s.status
	if s.closed {
		return response, errors.New("collections: vector index searcher is closed")
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return response, err
	}
	readerStatsBefore := s.readerLast
	results, searchStats, err := s.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:     opts.TopK,
		EfSearch: opts.EfSearch,
	}, &s.scratch)
	readerStatsAfter := s.reader.Stats()
	s.readerLast = readerStatsAfter
	readerStats := columnPhysicalRowReaderStatsDelta(readerStatsBefore, readerStatsAfter)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, readerStats)
	if err != nil {
		return response, err
	}
	if len(results) == 0 {
		return response, nil
	}
	var documents [][]byte
	var documentBytes []byte
	if opts.IncludeDocuments {
		var inlineDocuments [16][]byte
		if len(results) <= len(inlineDocuments) {
			documents = inlineDocuments[:len(results)]
		} else {
			documents = make([][]byte, len(results))
		}
		totalDocumentBytes := 0
		for i, result := range results {
			doc, found, err := s.getDocumentAtBoundSnapshot(result.ID)
			if err != nil {
				return response, err
			}
			if !found {
				return response, fmt.Errorf("collections: vector index %q result document %q not found", s.indexName, string(result.ID))
			}
			documents[i] = doc
			totalDocumentBytes, err = addVectorIndexSearchByteTotal(totalDocumentBytes, len(doc), math.MaxInt, "document")
			if err != nil {
				return response, err
			}
			response.Stats.DocumentsFetched++
		}
		if totalDocumentBytes > 0 {
			documentBytes = make([]byte, totalDocumentBytes)
		}
	}
	response.Results = make([]VectorIndexSearchResult, len(results))
	idByteCount, err := vectorIndexSearchResultIDBytes(results)
	if err != nil {
		return response, err
	}
	idBytes := make([]byte, idByteCount)
	idOffset := 0
	docOffset := 0
	for i, result := range results {
		if len(result.ID) > len(idBytes)-idOffset {
			return response, errors.New("collections: vector index search result id byte accounting mismatch")
		}
		nextIDOffset := idOffset + len(result.ID)
		id := idBytes[idOffset:nextIDOffset:nextIDOffset]
		idOffset = nextIDOffset
		copy(id, result.ID)
		response.Results[i] = VectorIndexSearchResult{
			ID:      id,
			Ordinal: result.Ordinal,
			Score:   result.Score,
		}
		if opts.IncludeDocuments {
			doc := documents[i]
			if len(doc) == 0 {
				response.Results[i].Document = []byte{}
				continue
			}
			if len(doc) > len(documentBytes)-docOffset {
				return response, errors.New("collections: vector index search document byte accounting mismatch")
			}
			nextDocOffset := docOffset + len(doc)
			responseDoc := documentBytes[docOffset:nextDocOffset:nextDocOffset]
			copy(responseDoc, doc)
			response.Results[i].Document = responseDoc
			docOffset = nextDocOffset
		}
	}
	return response, nil
}

func validateVectorIndexSearchRequest(topK, efSearch int) error {
	if topK < 0 {
		return errors.New("collections: vector index search top_k cannot be negative")
	}
	if efSearch < 0 {
		return errors.New("collections: vector index search ef_search cannot be negative")
	}
	return nil
}

// getDocumentAtBoundSnapshot returns snapshot-bound document bytes for immediate
// use by Search. Search copies them into a response-owned arena before exposing
// documents to callers, matching Collection.Get retention semantics.
func (s *VectorIndexSearcher) getDocumentAtBoundSnapshot(documentID []byte) ([]byte, bool, error) {
	if s == nil || s.snapshot == nil || s.catalog == nil || s.collection == nil {
		return nil, false, errors.New("collections: nil vector index searcher snapshot")
	}
	value, found, err := collectionGetAppendAtCatalogRoot(s.snapshot, s.catalog, collectionPrimaryRootName(s.collection.meta.Name), documentID, nil)
	if err != nil || !found {
		return nil, found, err
	}
	if !columnStoreCanReconstructDocument(s.catalog.meta) {
		return value, true, nil
	}
	reconstructed, err := s.collection.reconstructColumnDocumentAtSnapshot(s.snapshot, s.catalog, documentID, value)
	if err != nil {
		return nil, false, err
	}
	return reconstructed, true, nil
}

func vectorIndexSearchResultIDBytes(results []columnVectorGraphNativeSearchResult) (int, error) {
	return vectorIndexSearchResultIDBytesLimit(results, math.MaxInt)
}

func vectorIndexSearchResultIDBytesLimit(results []columnVectorGraphNativeSearchResult, limit int) (int, error) {
	var total int
	for _, result := range results {
		next, err := addVectorIndexSearchByteTotal(total, len(result.ID), limit, "result id")
		if err != nil {
			return 0, err
		}
		total = next
	}
	return total, nil
}

func addVectorIndexSearchByteTotal(total, n, limit int, label string) (int, error) {
	if n < 0 || total < 0 || limit < 0 || n > limit-total {
		return 0, fmt.Errorf("collections: vector index search %s bytes overflow", label)
	}
	return total + n, nil
}

func (s *VectorIndexSearcher) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var closeErr error
	if s.reader == nil {
		if s.snapshot != nil {
			closeErr = s.snapshot.Close()
			s.snapshot = nil
		}
		return closeErr
	}
	if err := s.reader.Close(); err != nil {
		closeErr = err
	}
	if s.snapshot != nil {
		if err := s.snapshot.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		s.snapshot = nil
	}
	return closeErr
}

// columnPhysicalRowReaderStatsDelta reports per-search deltas for mutable
// fetch/read counters while carrying open-time and resident-byte telemetry as
// absolute values from the bound reader.
func columnPhysicalRowReaderStatsDelta(before, after columnPhysicalRowReaderStats) columnPhysicalRowReaderStats {
	return columnPhysicalRowReaderStats{
		OpenGranulesRead:      after.OpenGranulesRead,
		OpenPhysicalBytesRead: after.OpenPhysicalBytesRead,
		RowFetches:            deltaUint64(before.RowFetches, after.RowFetches),
		BatchFetches:          deltaUint64(before.BatchFetches, after.BatchFetches),
		RowsFetched:           deltaUint64(before.RowsFetched, after.RowsFetched),
		CacheHits:             deltaUint64(before.CacheHits, after.CacheHits),
		CacheMisses:           deltaUint64(before.CacheMisses, after.CacheMisses),
		DecodedBlocks:         deltaUint64(before.DecodedBlocks, after.DecodedBlocks),
		GranulesTouched:       deltaUint64(before.GranulesTouched, after.GranulesTouched),
		PhysicalBytesRead:     deltaInt64(before.PhysicalBytesRead, after.PhysicalBytesRead),
		MaxResidentBytes:      after.MaxResidentBytes,
	}
}

func deltaUint64(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func deltaInt64(before, after int64) int64 {
	if after < before {
		return 0
	}
	return after - before
}

func vectorIndexSearchStatsFromInternal(searchStats columnVectorGraphNativeSearchStats, readerStats columnPhysicalRowReaderStats) VectorIndexSearchStats {
	return VectorIndexSearchStats{
		Candidates:            searchStats.Candidates,
		Edges:                 searchStats.Edges,
		CandidateFetches:      searchStats.CandidateFetches,
		ExpansionFetches:      searchStats.ExpansionFetches,
		ResultFetches:         searchStats.ResultFetches,
		RowFetches:            readerStats.RowFetches,
		BatchFetches:          readerStats.BatchFetches,
		RowsFetched:           readerStats.RowsFetched,
		CacheHits:             readerStats.CacheHits,
		CacheMisses:           readerStats.CacheMisses,
		DecodedBlocks:         readerStats.DecodedBlocks,
		GranulesTouched:       readerStats.GranulesTouched,
		PhysicalBytesRead:     readerStats.PhysicalBytesRead,
		MaxResidentBytes:      readerStats.MaxResidentBytes,
		OpenGranulesRead:      readerStats.OpenGranulesRead,
		OpenPhysicalBytesRead: readerStats.OpenPhysicalBytesRead,
	}
}
