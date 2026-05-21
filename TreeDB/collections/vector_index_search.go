package collections

import (
	"errors"
	"fmt"
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
	reader     *columnVectorGraphPhysicalRowReader
	scratch    columnVectorGraphNativeSearchScratch
	closed     bool
}

// SearchVectorIndex searches a collection vector index through the public
// collection lifecycle. V4 wires only explicit column_graph indexes to the
// native physical column reader; native_runtime remains reported as native
// rather than silently falling back or pretending to use column storage.
func (c *Collection) SearchVectorIndex(opts VectorIndexSearchOptions) (VectorIndexSearchResponse, error) {
	if opts.TopK < 0 {
		return VectorIndexSearchResponse{}, errors.New("collections: vector index search top_k cannot be negative")
	}
	if opts.EfSearch < 0 {
		return VectorIndexSearchResponse{}, errors.New("collections: vector index search ef_search cannot be negative")
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
		return nil, response, fmt.Errorf("%w: vector index %q uses native_runtime; public column_graph search was requested", ErrVectorIndexSearchUnavailable, def.Name)
	case VectorIndexStrategyColumnGraph:
	default:
		return nil, response, fmt.Errorf("collections: unsupported vector index strategy %q", def.Strategy)
	}

	reader, err := c.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{
		MaxDecodedBlocks: opts.MaxDecodedBlocks,
	})
	if err != nil {
		status, statusErr := c.VectorIndexStatus(def.Name)
		if statusErr != nil {
			return nil, response, statusErr
		}
		response.Status = status
		return nil, response, fmt.Errorf("%w: column_graph %q is not loaded: state=%s reason=%s: %v", ErrVectorIndexSearchUnavailable, def.Name, status.State, status.Reason, err)
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
		reader:     reader,
	}
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
	if opts.TopK < 0 {
		return response, errors.New("collections: vector index search top_k cannot be negative")
	}
	if opts.EfSearch < 0 {
		return response, errors.New("collections: vector index search ef_search cannot be negative")
	}
	results, searchStats, err := s.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:     opts.TopK,
		EfSearch: opts.EfSearch,
	}, &s.scratch)
	readerStats := s.reader.Stats()
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, readerStats)
	if err != nil {
		return response, err
	}
	if len(results) == 0 {
		return response, nil
	}
	response.Results = make([]VectorIndexSearchResult, len(results))
	idBytes := make([]byte, vectorIndexSearchResultIDBytes(results))
	idOffset := 0
	for i, result := range results {
		id := idBytes[idOffset : idOffset+len(result.ID)]
		idOffset += len(result.ID)
		copy(id, result.ID)
		response.Results[i] = VectorIndexSearchResult{
			ID:      id,
			Ordinal: result.Ordinal,
			Score:   result.Score,
		}
		if opts.IncludeDocuments {
			doc, err := s.collection.Get(id)
			if err != nil {
				return response, err
			}
			if doc == nil {
				return response, fmt.Errorf("collections: vector index %q result document %q not found", s.indexName, string(id))
			}
			response.Results[i].Document = doc
			response.Stats.DocumentsFetched++
		}
	}
	return response, nil
}

func vectorIndexSearchResultIDBytes(results []columnVectorGraphNativeSearchResult) int {
	var total int
	for _, result := range results {
		total += len(result.ID)
	}
	return total
}

func (s *VectorIndexSearcher) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	if s.reader == nil {
		return nil
	}
	return s.reader.Close()
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
