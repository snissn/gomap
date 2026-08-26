package documentservice

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const maxServiceScanDocuments = int(^uint(0) >> 1)

// Service maps the document/search contract onto TreeDB collections.
type Service struct {
	manager *collections.CollectionManager
	writeMu sync.Mutex

	benchmarkSearchCacheMu       sync.RWMutex
	benchmarkSearchCache         map[string]*serviceBenchmarkSearchCacheEntry
	benchmarkSearchBufferPool    sync.Pool
	denseVectorNativeAfterSearch func(int, collections.VectorIndexSearchResponse) error
	vectorPartitionOperations    *vectorpartition.OperationsV1
	closed                       bool
}

// RegisterVectorPartitionOperationsV1 installs the optional default-off
// operator boundary. The wrapped service, backend, and live-health function
// remain node-owned and cannot bypass this boundary through documentservice.
func (s *Service) RegisterVectorPartitionOperationsV1(operations *vectorpartition.OperationsV1) error {
	if s == nil || operations == nil || !operations.Enabled() {
		return errors.New("document service: vector partition operations are required")
	}
	s.benchmarkSearchCacheMu.Lock()
	defer s.benchmarkSearchCacheMu.Unlock()
	if s.closed {
		return serviceClosedError()
	}
	if s.vectorPartitionOperations != nil {
		return errors.New("document service: vector partition operations already registered")
	}
	s.vectorPartitionOperations = operations
	return nil
}

func (s *Service) VectorPartitionOperationsV1() (*vectorpartition.OperationsV1, error) {
	if s == nil {
		return nil, errors.New("document service: vector partition operations are unavailable")
	}
	s.benchmarkSearchCacheMu.RLock()
	defer s.benchmarkSearchCacheMu.RUnlock()
	if s.closed || s.vectorPartitionOperations == nil {
		return nil, errors.New("document service: vector partition operations are unavailable")
	}
	return s.vectorPartitionOperations, nil
}

type serviceBenchmarkSearchCacheEntry struct {
	collection *collections.Collection
	info       IndexInfo
}

func (e *serviceBenchmarkSearchCacheEntry) matches(name string) bool {
	return e != nil && e.collection != nil && e.info.Name == name
}

// New returns a document/search service backed by manager.
func New(manager *collections.CollectionManager) *Service {
	return &Service{
		manager: manager,
		benchmarkSearchBufferPool: sync.Pool{New: func() any {
			return &collections.VectorIndexSearchBuffer{}
		}},
	}
}

// Close releases service-owned cached search resources and marks the service
// unavailable. The underlying collection manager and database remain owned by
// the caller.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	var entries []*serviceBenchmarkSearchCacheEntry
	s.benchmarkSearchCacheMu.Lock()
	if s.closed {
		s.benchmarkSearchCacheMu.Unlock()
		return nil
	}
	s.closed = true
	s.vectorPartitionOperations = nil
	s.benchmarkSearchBufferPool = sync.Pool{}
	for _, entry := range s.benchmarkSearchCache {
		if entry != nil {
			entries = append(entries, entry)
		}
	}
	s.benchmarkSearchCache = nil
	s.benchmarkSearchCacheMu.Unlock()

	var closeErr error
	for _, entry := range entries {
		if entry.collection != nil {
			closeErr = errors.Join(closeErr, entry.collection.CloseVectorIndexPreparedSearchCache())
		}
	}
	return closeErr
}

func (s *Service) isClosed() bool {
	if s == nil {
		return false
	}
	s.benchmarkSearchCacheMu.RLock()
	closed := s.closed
	s.benchmarkSearchCacheMu.RUnlock()
	return closed
}

func serviceClosedError() error {
	return serviceError(CodeIndexUnavailable, "document service is closed")
}

// CreateIndex creates or opens a compatible document service index.
func (s *Service) CreateIndex(ctx context.Context, req CreateIndexRequest) (IndexInfo, error) {
	if s == nil {
		return IndexInfo{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.createIndexLocked(ctx, req)
}

func (s *Service) createIndexLocked(ctx context.Context, req CreateIndexRequest) (IndexInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return IndexInfo{}, err
	}
	if s == nil || s.manager == nil {
		return IndexInfo{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	if s.isClosed() {
		return IndexInfo{}, serviceClosedError()
	}
	if err := collections.ValidateCollectionName(req.Name); err != nil {
		return IndexInfo{}, wrapServiceError(CodeInvalidRequest, "invalid index name", err)
	}
	if req.Dimension <= 0 {
		return IndexInfo{}, serviceError(CodeInvalidRequest, "dimension must be positive")
	}
	metric, err := normalizeMetric(req.Metric)
	if err != nil {
		return IndexInfo{}, err
	}
	collectionMetric, err := metricToCollection(metric)
	if err != nil {
		return IndexInfo{}, err
	}
	vectorOptions, err := benchmarkVectorIndexOptionsForCreate(metric, req.VectorIndexOptions)
	if err != nil {
		return IndexInfo{}, err
	}
	scalarDeclarations, err := normalizeScalarFieldDeclarations(req.ScalarFields)
	if err != nil {
		return IndexInfo{}, err
	}
	options := collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}
	if vectorOptions.strategy == collections.VectorIndexStrategyColumnGraph {
		options.ColumnStore = serviceColumnStoreConfig(req.Dimension)
	}
	meta := &collections.CollectionMeta{
		Name:    req.Name,
		Options: options,
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:             defaultVectorIndexName,
			Field:            defaultEmbeddingField,
			Metric:           collectionMetric,
			Dimensions:       req.Dimension,
			M:                vectorOptions.m,
			EfConstruction:   vectorOptions.efConstruction,
			EfSearch:         vectorOptions.efSearch,
			Encoding:         collections.VectorIndexEncodingFloat32,
			Strategy:         vectorOptions.strategy,
			SchemaGeneration: 1,
			QuantizedIndexes: vectorOptions.quantizedIndexes,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name:             defaultTextIndexName,
			Fields:           []collections.TextIndexField{{Field: defaultTextField}},
			Analyzer:         collections.TextAnalyzerSimple,
			StorePositions:   true,
			SchemaGeneration: 1,
		}},
	}
	if len(scalarDeclarations) > 0 {
		indexes := make([]collections.IndexDefinition, len(scalarDeclarations))
		for i, declaration := range scalarDeclarations {
			indexes[i] = collections.IndexDefinition{
				Name:      declaration.indexName,
				Field:     declaration.field,
				ValueType: declaration.collectionTy,
			}
		}
		meta.Indexes = indexes
	}
	created, alreadyExisted, err := s.manager.CreateCollectionWithPreparedCommandWALIntentStatus(*meta, nil)
	if err != nil {
		if strings.Contains(err.Error(), "incompatible") {
			return IndexInfo{}, wrapServiceError(CodeConflict, fmt.Sprintf("index %q already exists with an incompatible schema", req.Name), err)
		}
		return IndexInfo{}, wrapServiceError(CodeInternal, "create index failed", err)
	}
	if !alreadyExisted {
		if err := s.invalidateBenchmarkSearchCache(req.Name); err != nil {
			return IndexInfo{}, wrapServiceError(CodeInternal, "invalidate benchmark vector search cache after create index failed", err)
		}
	}
	info, err := indexInfoFromMeta(*created)
	if err != nil {
		return IndexInfo{}, err
	}
	if info.VectorStrategy == collections.VectorIndexStrategyNativeRuntime {
		s.benchmarkSearchCacheMu.RLock()
		cached := s.benchmarkSearchCache[req.Name].matches(req.Name)
		s.benchmarkSearchCacheMu.RUnlock()
		if cached {
			return info, nil
		}
		col, _, err := s.openIndex(ctx, req.Name, 0)
		if err != nil {
			return IndexInfo{}, err
		}
		opts := collections.VectorIndexOptions{
			Name:           info.VectorIndexName,
			Field:          info.EmbeddingField,
			Metric:         collectionMetric,
			Dimensions:     info.Dimension,
			M:              info.VectorM,
			EfConstruction: info.VectorEfConstruction,
			EfSearch:       info.VectorEfSearch,
			Encoding:       collections.VectorIndexEncodingFloat32,
		}
		index, load, err := col.LoadNativeVectorIndexSnapshot(opts)
		if err != nil {
			return IndexInfo{}, mapCollectionMaintenanceError("load native vector index", err)
		}
		if index == nil && !load.Loaded && load.RootName != "" && load.RootID == 0 {
			empty := true
			if _, err := col.ScanDocumentsFunc(1, func(collections.DocumentRecord) (bool, error) {
				empty = false
				return false, nil
			}); err != nil {
				return IndexInfo{}, mapCollectionMaintenanceError("inspect native vector index", err)
			}
			if empty {
				index, err = col.BuildVectorIndex(opts)
				if err != nil {
					return IndexInfo{}, mapCollectionMaintenanceError("initialize native vector index", err)
				}
			}
		}
		if index != nil {
			if err := s.primeBenchmarkSearchCache(req.Name, col, info); err != nil {
				return IndexInfo{}, wrapServiceError(CodeInternal, "prime native vector search cache after create index failed", err)
			}
		}
	}
	return info, nil
}

// OpenIndex returns metadata for an existing document service index.
func (s *Service) OpenIndex(ctx context.Context, name string) (IndexInfo, error) {
	col, info, err := s.openIndex(ctx, name, 0)
	_ = col
	return info, err
}

// UpsertDocuments writes or replaces documents in index.
func (s *Service) UpsertDocuments(ctx context.Context, index string, req UpsertDocumentsRequest) (UpsertDocumentsResponse, error) {
	if s == nil {
		return UpsertDocumentsResponse{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return UpsertDocumentsResponse{}, err
	}
	if len(req.Documents) == 0 {
		return UpsertDocumentsResponse{}, serviceError(CodeInvalidRequest, "documents must not be empty")
	}
	prepared, err := prepareDocumentsForWrite(req.Documents, info)
	if err != nil {
		return UpsertDocumentsResponse{}, err
	}

	ids := make([]string, 0, len(prepared))
	compactEmbeddings := 0
	insertIDs := make([][]byte, 0, len(prepared))
	insertDocs := make([][]byte, 0, len(prepared))
	inserts := make([]preparedDocument, 0, len(prepared))
	updates := make([]preparedDocument, 0)
	for _, doc := range prepared {
		if err := ctxErr(ctx); err != nil {
			return UpsertDocumentsResponse{}, err
		}
		ids = append(ids, doc.id)
		if doc.compactEmbedding {
			compactEmbeddings++
		}
		current, err := col.Get([]byte(doc.id))
		if err != nil {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "read before upsert failed", err)
		}
		if current == nil {
			insertIDs = append(insertIDs, []byte(doc.id))
			insertDocs = append(insertDocs, doc.raw)
			inserts = append(inserts, doc)
			continue
		}
		updates = append(updates, doc)
	}
	if len(insertIDs) > 0 && len(updates) == 0 && !req.DeferVectorIndexRebuild {
		if err := preflightServiceVectorAutoRebuildSupported(info); err != nil {
			return UpsertDocumentsResponse{}, err
		}
	}
	inserted := 0
	updated := 0
	if len(insertIDs) > 0 {
		if _, err := col.InsertBatch(insertIDs, insertDocs); err == nil {
			inserted = len(insertIDs)
		} else if collections.IsDuplicateKeyError(err) {
			// Upsert is a service contract, while Collection.InsertBatch is an
			// insert-only primitive. If another request inserts one of these IDs
			// between the read preflight and InsertBatch, fall back per item to
			// replace-or-insert semantics instead of leaking ErrDocumentExists.
			for _, doc := range inserts {
				wasInserted, wasUpdated, err := upsertPreparedDocument(ctx, col, doc, true)
				if err != nil {
					return UpsertDocumentsResponse{}, err
				}
				if wasInserted {
					inserted++
				}
				if wasUpdated {
					updated++
				}
			}
		} else {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "insert documents failed", err)
		}
	}
	for _, doc := range updates {
		wasInserted, wasUpdated, err := upsertPreparedDocument(ctx, col, doc, false)
		if err != nil {
			return UpsertDocumentsResponse{}, err
		}
		if wasInserted {
			inserted++
		}
		if wasUpdated {
			updated++
		}
	}
	var rebuildErr error
	if inserted > 0 && updated == 0 && !req.DeferVectorIndexRebuild {
		rebuildErr = rebuildServiceVectorIndex(ctx, col)
	}
	if inserted+updated > 0 {
		if err := s.finishVectorMutation(index, col, info); err != nil && rebuildErr == nil {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "publish vector mutation after upsert failed", err)
		}
	}
	if rebuildErr != nil {
		return UpsertDocumentsResponse{}, rebuildErr
	}
	return UpsertDocumentsResponse{Index: info, Upserted: len(prepared), Inserted: inserted, Updated: updated, IDs: ids, CompactEmbeddings: compactEmbeddings}, nil
}

// DeleteDocuments deletes explicit IDs or documents matching a filter.
func (s *Service) DeleteDocuments(ctx context.Context, index string, req DeleteDocumentsRequest) (DeleteDocumentsResponse, error) {
	if s == nil {
		return DeleteDocumentsResponse{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return DeleteDocumentsResponse{}, err
	}
	if len(req.IDs) > 0 && req.Filter != nil {
		return DeleteDocumentsResponse{}, serviceError(CodeInvalidRequest, "delete accepts either ids or filter, not both")
	}

	var ids []string
	if len(req.IDs) > 0 {
		ids, err = validateDocumentIDs(req.IDs)
		if err != nil {
			return DeleteDocumentsResponse{}, err
		}
	} else if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return DeleteDocumentsResponse{}, err
		}
		ids, err = s.collectMatchingIDs(ctx, col, req.Filter)
		if err != nil {
			return DeleteDocumentsResponse{}, err
		}
	} else {
		return DeleteDocumentsResponse{}, serviceError(CodeInvalidRequest, "delete requires ids or filter")
	}
	if len(ids) == 0 {
		return DeleteDocumentsResponse{Index: info, IDs: []string{}}, nil
	}
	deleteIDs := make([][]byte, len(ids))
	for i, id := range ids {
		deleteIDs[i] = []byte(id)
	}
	deleted, err := col.DeleteBatch(deleteIDs)
	if err != nil {
		return DeleteDocumentsResponse{}, wrapServiceError(CodeInternal, "delete documents failed", err)
	}
	if deleted > 0 {
		if err := s.finishVectorMutation(index, col, info); err != nil {
			return DeleteDocumentsResponse{}, wrapServiceError(CodeInternal, "publish vector mutation after delete failed", err)
		}
	}
	return DeleteDocumentsResponse{Index: info, Deleted: deleted, IDs: ids}, nil
}

// CountDocuments counts documents matching a filter.
func (s *Service) CountDocuments(ctx context.Context, index string, req CountDocumentsRequest) (CountDocumentsResponse, error) {
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return CountDocumentsResponse{}, err
	}
	if err := req.Filter.Validate(); err != nil {
		return CountDocumentsResponse{}, err
	}
	count := 0
	err = s.scanDocuments(ctx, col, func(doc Document) error {
		ok, err := matchFilter(req.Filter, doc)
		if err != nil || !ok {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return CountDocumentsResponse{}, err
	}
	return CountDocumentsResponse{Index: info, Count: count}, nil
}

// FilterDocuments returns documents matching a filter in stable document-ID order.
func (s *Service) FilterDocuments(ctx context.Context, index string, req FilterDocumentsRequest) (FilterDocumentsResponse, error) {
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return FilterDocumentsResponse{}, err
	}
	if req.Limit < 0 || req.Offset < 0 {
		return FilterDocumentsResponse{}, serviceError(CodeInvalidRequest, "limit and offset must be non-negative")
	}
	if err := req.Filter.Validate(); err != nil {
		return FilterDocumentsResponse{}, err
	}
	var docs []Document
	matched := 0
	err = s.scanDocuments(ctx, col, func(doc Document) error {
		ok, err := matchFilter(req.Filter, doc)
		if err != nil || !ok {
			return err
		}
		matched++
		if matched <= req.Offset {
			return nil
		}
		if req.Limit > 0 && len(docs) >= req.Limit {
			return nil
		}
		if !req.ReturnEmbedding {
			doc.Embedding = nil
		}
		docs = append(docs, doc)
		return nil
	})
	if err != nil {
		return FilterDocumentsResponse{}, err
	}
	truncated := req.Limit > 0 && matched > req.Offset+len(docs)
	if docs == nil {
		docs = []Document{}
	}
	return FilterDocumentsResponse{Index: info, Documents: docs, MatchedCount: matched, Truncated: truncated}, nil
}

// SearchDenseVector scores QueryEmbedding over the index. Route=ann uses a
// compatible native_runtime or column_graph vector index; declared scalar
// filters are supported by native_runtime. Route=exact keeps the bounded
// document scan. Route selection is deterministic and echoed in the response;
// there is no silent downgrade.
func (s *Service) SearchDenseVector(ctx context.Context, index string, req DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	if req.TopK <= 0 {
		return DenseVectorSearchResponse{}, serviceError(CodeInvalidRequest, "top_k must be positive")
	}
	if err := validateEmbedding("query_embedding", req.QueryEmbedding, info.Dimension, info.Metric); err != nil {
		return DenseVectorSearchResponse{}, err
	}
	route, err := resolveDenseSearchRoute(req, info)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	if req.EfSearch < 0 {
		return DenseVectorSearchResponse{}, serviceError(CodeInvalidRequest, "ef_search must be non-negative")
	}
	if route == RouteAnn {
		return s.searchDenseVectorAnn(ctx, col, info, req)
	}
	if err := req.Filter.Validate(); err != nil {
		return DenseVectorSearchResponse{}, err
	}
	var candidates []scoredDocument
	candidateCount := 0
	err = s.scanDocuments(ctx, col, func(doc Document) error {
		ok, err := matchFilter(req.Filter, doc)
		if err != nil || !ok {
			return err
		}
		candidateCount++
		if err := validateEmbedding("document "+doc.ID+" embedding", doc.Embedding, info.Dimension, info.Metric); err != nil {
			return err
		}
		score, err := scoreEmbedding(req.QueryEmbedding, doc.Embedding, info.Metric)
		if err != nil {
			return err
		}
		if !req.ReturnEmbedding {
			doc.Embedding = nil
		}
		doc.Score = scorePtr(score)
		candidates = append(candidates, scoredDocument{document: doc, score: score})
		return nil
	})
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].document.ID < candidates[j].document.ID
		}
		return candidates[i].score > candidates[j].score
	})
	if len(candidates) > req.TopK {
		candidates = candidates[:req.TopK]
	}
	docs := make([]Document, len(candidates))
	for i := range candidates {
		docs[i] = candidates[i].document
	}
	if docs == nil {
		docs = []Document{}
	}
	return DenseVectorSearchResponse{Index: info, Documents: docs, Metric: info.Metric, Route: RouteExact, Exact: true, Candidates: candidateCount}, nil
}

// resolveDenseSearchRoute applies the deterministic route defaulting rules:
// explicit route values are validated; an omitted route selects ann when the
// index declares a compatible no-document vector route, including declared
// scalar filters on native_runtime, and exact otherwise.
func resolveDenseSearchRoute(req DenseVectorSearchRequest, info IndexInfo) (Route, error) {
	switch Route(strings.TrimSpace(strings.ToLower(string(req.Route)))) {
	case "":
		if info.Capabilities.NoDocumentVectorSearch &&
			(req.Filter == nil || info.VectorStrategy == collections.VectorIndexStrategyNativeRuntime) {
			return RouteAnn, nil
		}
		return RouteExact, nil
	case RouteExact:
		return RouteExact, nil
	case RouteAnn:
		if req.Filter != nil && info.VectorStrategy != collections.VectorIndexStrategyNativeRuntime {
			return "", serviceError(CodeInvalidRequest, "dense route \"ann\" metadata filters require a native_runtime vector index")
		}
		return RouteAnn, nil
	default:
		return "", serviceErrorf(CodeInvalidRequest, "unsupported dense search route %q; use \"ann\" or \"exact\"", req.Route)
	}
}

// ResetIndex creates a missing benchmark index or clears an existing compatible
// non-column_graph index when drop_old is requested. Column_graph benchmark
// indexes intentionally fail closed for in-place reset: rebuilding those assets
// after delete/reinsert tombstones is not the fresh insert-only load boundary
// VectorDBBench needs. Managed benchmark runs should use a fresh data directory;
// external shared services should use a unique index name per run.
func (s *Service) ResetIndex(ctx context.Context, index string, req ResetIndexRequest) (ResetIndexResponse, error) {
	if s == nil {
		return ResetIndexResponse{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	if err := ctxErr(ctx); err != nil {
		return ResetIndexResponse{}, err
	}
	if err := collections.ValidateCollectionName(index); err != nil {
		return ResetIndexResponse{}, wrapServiceError(CodeInvalidRequest, "invalid index name", err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	existingCol, existingInfo, err := s.openIndex(ctx, index, 0)
	if err != nil {
		if ErrorCodeOf(err) != CodeIndexNotFound {
			return ResetIndexResponse{}, err
		}
		info, err := s.createIndexLocked(ctx, CreateIndexRequest{Name: index, Dimension: req.Dimension, Metric: req.Metric, VectorIndexOptions: req.VectorIndexOptions})
		if err != nil {
			return ResetIndexResponse{}, err
		}
		return ResetIndexResponse{Index: info, Created: true, Reset: false, DropOld: req.DropOld, DroppedDocuments: 0}, nil
	}
	if !req.DropOld {
		return ResetIndexResponse{}, serviceErrorf(CodeConflict, "index %q already exists and drop_old=false", index)
	}
	if req.Dimension == 0 {
		req.Dimension = existingInfo.Dimension
	}
	if req.Metric == "" {
		req.Metric = existingInfo.Metric
	}
	if existingInfo.VectorStrategy == collections.VectorIndexStrategyColumnGraph {
		return ResetIndexResponse{}, serviceErrorf(CodeUnsupported, "drop_old reset for column_graph benchmark index %q requires a fresh data directory or unique index name", index)
	}
	if _, err := s.createIndexLocked(ctx, CreateIndexRequest{Name: index, Dimension: req.Dimension, Metric: req.Metric, VectorIndexOptions: req.VectorIndexOptions}); err != nil {
		return ResetIndexResponse{}, err
	}

	ids, err := s.collectMatchingIDs(ctx, existingCol, nil)
	if err != nil {
		return ResetIndexResponse{}, err
	}
	if len(ids) > 0 {
		deleteIDs := make([][]byte, len(ids))
		for i, id := range ids {
			deleteIDs[i] = []byte(id)
		}
		if _, err := existingCol.DeleteBatch(deleteIDs); err != nil {
			return ResetIndexResponse{}, mapCollectionMaintenanceError("reset index", err)
		}
		if err := s.finishVectorMutation(index, existingCol, existingInfo); err != nil {
			return ResetIndexResponse{}, wrapServiceError(CodeInternal, "publish vector mutation after reset failed", err)
		}
	}
	_, info, err := s.openIndex(ctx, index, 0)
	if err != nil {
		return ResetIndexResponse{}, err
	}
	return ResetIndexResponse{Index: info, Created: false, Reset: true, DropOld: req.DropOld, DroppedDocuments: len(ids)}, nil
}

// OptimizeIndex rebuilds service vector assets after a benchmark load phase.
func (s *Service) OptimizeIndex(ctx context.Context, index string, req OptimizeIndexRequest) (OptimizeIndexResponse, error) {
	if s == nil {
		return OptimizeIndexResponse{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return OptimizeIndexResponse{}, err
	}
	vectorIndexName := strings.TrimSpace(req.VectorIndexName)
	if vectorIndexName == "" {
		vectorIndexName = info.VectorIndexName
	}
	if vectorIndexName != info.VectorIndexName {
		return OptimizeIndexResponse{}, serviceErrorf(CodeInvalidRequest, "unsupported vector_index_name %q", req.VectorIndexName)
	}

	if err := s.invalidateBenchmarkSearchCache(index); err != nil {
		return OptimizeIndexResponse{}, wrapServiceError(CodeInternal, "invalidate benchmark vector search cache before optimize failed", err)
	}
	status, err := col.RebuildVectorIndex(vectorIndexName)
	if err != nil {
		return OptimizeIndexResponse{}, mapCollectionMaintenanceError("optimize vector index", err)
	}
	maintenance := vectorIndexMaintenanceStatus(status)
	if info.Capabilities.NoDocumentVectorSearch && maintenance.Loaded && !maintenance.RebuildNeeded {
		if err := s.primeBenchmarkSearchCache(index, col, info); err != nil {
			return OptimizeIndexResponse{}, wrapServiceError(CodeInternal, "prime benchmark vector search cache after optimize failed", err)
		}
		if info.VectorStrategy == collections.VectorIndexStrategyColumnGraph {
			if err := s.warmBenchmarkSearchCache(ctx, index, info, vectorIndexName); err != nil {
				_ = s.invalidateBenchmarkSearchCache(index)
				return OptimizeIndexResponse{}, err
			}
		}
	}
	return OptimizeIndexResponse{Index: info, VectorIndexName: vectorIndexName, Status: maintenance}, nil
}

// SearchBenchmarkVector runs fail-closed no-document vector-index benchmark
// search. It never materializes documents and never falls back to exact dense
// document scanning.
func (s *Service) SearchBenchmarkVector(ctx context.Context, index string, req BenchmarkVectorSearchRequest) (BenchmarkVectorSearchResponse, error) {
	col, info, release, err := s.acquireBenchmarkSearchIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	defer release()
	if !info.Capabilities.NoDocumentVectorSearch {
		return BenchmarkVectorSearchResponse{}, serviceError(CodeIndexUnavailable, "no-document vector-index search requires a supported cosine float32 index")
	}
	if req.TopK <= 0 {
		return BenchmarkVectorSearchResponse{}, serviceError(CodeInvalidRequest, "top_k must be positive")
	}
	if req.EfSearch < 0 || req.QuantizedRerankCandidates < 0 {
		return BenchmarkVectorSearchResponse{}, serviceError(CodeInvalidRequest, "ef_search and quantized_rerank_candidates must be non-negative")
	}
	if err := validateBenchmarkVectorStatsMode(req.StatsMode); err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	if err := validateBenchmarkVectorResponseFormat(req.ResponseFormat); err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	if err := normalizeBenchmarkVectorQueryEmbedding(&req); err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	if err := validateEmbedding("query_embedding", req.QueryEmbedding, info.Dimension, info.Metric); err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	vectorIndexName := strings.TrimSpace(req.VectorIndexName)
	if vectorIndexName == "" {
		vectorIndexName = info.VectorIndexName
	}
	if vectorIndexName != info.VectorIndexName {
		return BenchmarkVectorSearchResponse{}, serviceErrorf(CodeInvalidRequest, "unsupported vector_index_name %q", req.VectorIndexName)
	}
	queryMode, collectionQueryMode, err := normalizeBenchmarkVectorQueryMode(req.QueryMode)
	if err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	if err := validateBenchmarkVectorSearchRequestShape(queryMode, req); err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	buffer := s.benchmarkSearchBufferPool.Get().(*collections.VectorIndexSearchBuffer)
	defer func() {
		buffer.Reset()
		s.benchmarkSearchBufferPool.Put(buffer)
	}()
	search, err := col.SearchVectorIndexWithBuffer(collections.VectorIndexSearchOptions{
		IndexName:                 vectorIndexName,
		Query:                     req.QueryEmbedding,
		QueryMode:                 collectionQueryMode,
		QuantizedIndexName:        req.QuantizedIndexName,
		QuantizedRerankCandidates: req.QuantizedRerankCandidates,
		TopK:                      req.TopK,
		EfSearch:                  req.EfSearch,
		StatsMode:                 req.StatsMode,
	}, buffer)
	if err != nil {
		return BenchmarkVectorSearchResponse{}, mapVectorIndexSearchError("benchmark vector search", err)
	}
	if err := validateBenchmarkVectorSearchRoute(queryMode, req, search); err != nil {
		return BenchmarkVectorSearchResponse{}, err
	}
	responseStart := time.Time{}
	if search.Stats.WorkAccountingSearches != 0 {
		responseStart = time.Now()
	}
	var results []BenchmarkVectorSearchResult
	var compactIDs []string
	if req.ResponseFormat == BenchmarkVectorResponseFormatIDs {
		compactIDs = benchmarkVectorSearchIDs(search.Results)
	} else {
		results = benchmarkVectorSearchResults(search.Results)
		if results == nil {
			results = []BenchmarkVectorSearchResult{}
		}
	}
	stats := search.Stats
	if !responseStart.IsZero() {
		stats.ServiceResponseNanos = uint64(time.Since(responseStart))
		if stats.ServiceResponseNanos == 0 {
			stats.ServiceResponseNanos = 1
		}
	}
	return BenchmarkVectorSearchResponse{
		Index:                     info,
		Results:                   results,
		Metric:                    info.Metric,
		VectorIndexName:           vectorIndexName,
		QueryMode:                 queryMode,
		QuantizedIndexName:        req.QuantizedIndexName,
		QuantizedRerankCandidates: req.QuantizedRerankCandidates,
		NoDocuments:               true,
		Stats:                     stats,
		Diagnostics:               stats.Diagnostics(),
		compactIDs:                compactIDs,
	}, nil
}

// SearchKeyword runs ranked lexical search over the service content text index.
func (s *Service) SearchKeyword(ctx context.Context, index string, req KeywordSearchRequest) (KeywordSearchResponse, error) {
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return KeywordSearchResponse{}, err
	}
	if req.TopK <= 0 {
		return KeywordSearchResponse{}, serviceError(CodeInvalidRequest, "top_k must be positive")
	}
	operator, err := normalizeKeywordSearchOperator(req.Operator)
	if err != nil {
		return KeywordSearchResponse{}, err
	}
	if req.CandidateLimit < 0 || req.MaxPostingsScanned < 0 {
		return KeywordSearchResponse{}, serviceError(CodeInvalidRequest, "candidate_limit and max_postings_scanned must be non-negative")
	}
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return KeywordSearchResponse{}, err
		}
		if req.MaxPostingsScanned > 0 {
			return KeywordSearchResponse{}, serviceError(CodeUnsupported, "max_postings_scanned with metadata filters is unsupported; the filtered route fails closed rather than ignoring the guardrail")
		}
		return s.searchKeywordWithScalarFilter(ctx, col, info, req, operator)
	}

	textResponse, err := col.SearchText(collections.TextSearchOptions{
		IndexName:            defaultTextIndexName,
		Query:                req.Query,
		Operator:             operator,
		TopK:                 req.TopK,
		CandidateLimit:       req.CandidateLimit,
		MaxPostingsScanned:   req.MaxPostingsScanned,
		IncludeDocuments:     true,
		DocumentFetchOptions: serviceDocumentFetchOptions(req.ReturnEmbedding),
	})
	response := KeywordSearchResponse{Index: info, TextIndex: defaultTextIndexName, Stats: keywordStatsFromCollection(textResponse.Stats)}
	if err != nil {
		return response, mapKeywordSearchError(err)
	}
	docs, err := documentsFromTextSearchResults(textResponse.Results, req.ReturnEmbedding)
	if err != nil {
		return response, err
	}
	response.Documents = docs
	return response, nil
}

// SearchHybrid runs collection-native hybrid retrieval with text and/or vector sources.
func (s *Service) SearchHybrid(ctx context.Context, index string, req HybridSearchRequest) (HybridSearchResponse, error) {
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return HybridSearchResponse{}, err
	}
	if req.TopK <= 0 {
		return HybridSearchResponse{}, serviceError(CodeInvalidRequest, "top_k must be positive")
	}
	hasText := strings.TrimSpace(req.Query) != ""
	hasVector := len(req.QueryEmbedding) > 0
	if !hasText && !hasVector {
		return HybridSearchResponse{}, serviceError(CodeInvalidRequest, "hybrid search requires query, query_embedding, or both")
	}
	switch {
	case hasText && hasVector:
		if !info.Capabilities.HybridSearch {
			return HybridSearchResponse{}, serviceError(CodeIndexUnavailable, "hybrid search requires a cosine column_graph vector index and content text index")
		}
	case hasText:
		if !info.Capabilities.KeywordSearch {
			return HybridSearchResponse{}, serviceError(CodeIndexUnavailable, "hybrid text-only search requires a content text index")
		}
	case hasVector:
		if !info.Capabilities.NoDocumentVectorSearch {
			return HybridSearchResponse{}, serviceError(CodeIndexUnavailable, "hybrid vector-only search requires a cosine column_graph or native_runtime vector index")
		}
	}
	if req.CandidateLimit < 0 || req.TextCandidateLimit < 0 || req.VectorCandidateLimit < 0 || req.EfSearch < 0 {
		return HybridSearchResponse{}, serviceError(CodeInvalidRequest, "candidate limits and ef_search must be non-negative")
	}
	if req.MaxChunksPerParent < 0 {
		return HybridSearchResponse{}, serviceError(CodeInvalidRequest, "max_chunks_per_parent must be non-negative")
	}
	schema := newScalarSchema(info.ScalarFields)
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return HybridSearchResponse{}, err
		}
	}
	scalarFilter, err := translateScalarFilter(req.Filter, schema)
	if err != nil {
		return HybridSearchResponse{}, err
	}


	opts := collections.HybridSearchOptions{
		TopK:                 req.TopK,
		MaxChunksPerParent:   req.MaxChunksPerParent,
		Fusion:               req.Fusion,
		ScalarFilter:         scalarFilter,
		IncludeDocuments:     true,
		DocumentFetchOptions: serviceDocumentFetchOptions(req.ReturnEmbedding),
	}
	response := HybridSearchResponse{Index: info}
	if hasText {
		limit := req.TextCandidateLimit
		if limit == 0 {
			limit = req.CandidateLimit
		}
		opts.Text = &collections.HybridTextQuery{IndexName: defaultTextIndexName, Query: req.Query, CandidateLimit: limit}
		response.TextIndex = defaultTextIndexName
	}
	if hasVector {
		if err := validateEmbedding("query_embedding", req.QueryEmbedding, info.Dimension, info.Metric); err != nil {
			return HybridSearchResponse{}, err
		}
		limit := req.VectorCandidateLimit
		if limit == 0 {
			limit = req.CandidateLimit
		}
		opts.Vector = &collections.HybridVectorQuery{
			IndexName:      defaultVectorIndexName,
			Query:          append([]float32(nil), req.QueryEmbedding...),
			CandidateLimit: limit,
			EfSearch:       req.EfSearch,
			QueryMode:      collections.VectorIndexQueryModeExact,
		}
		response.VectorIndex = defaultVectorIndexName
	}

	hybridResponse, err := col.SearchHybrid(opts)
	response.Plan = hybridResponse.Plan
	response.Snapshot = hybridResponse.Snapshot
	response.Stats = hybridResponse.Stats
	if err != nil {
		return response, mappedHybridSearchError("hybrid search", err, hybridResponse.Stats)
	}
	docs, err := documentsFromHybridSearchResults(hybridResponse.Results, hybridResponse.Plan, req.ReturnEmbedding)
	if err != nil {
		return response, err
	}
	response.Documents = docs
	return response, nil
}

func (s *Service) acquireBenchmarkSearchIndex(ctx context.Context, name string, expectedGeneration uint64) (*collections.Collection, IndexInfo, func(), error) {
	if err := ctxErr(ctx); err != nil {
		return nil, IndexInfo{}, nil, err
	}
	if s == nil || s.manager == nil {
		return nil, IndexInfo{}, nil, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	if err := collections.ValidateCollectionName(name); err != nil {
		return nil, IndexInfo{}, nil, wrapServiceError(CodeInvalidRequest, "invalid index name", err)
	}
	for {
		s.benchmarkSearchCacheMu.RLock()
		if s.closed {
			s.benchmarkSearchCacheMu.RUnlock()
			return nil, IndexInfo{}, nil, serviceClosedError()
		}
		entry := s.benchmarkSearchCache[name]
		if entry.matches(name) {
			info := entry.info
			if expectedGeneration != 0 && expectedGeneration != info.Generation {
				s.benchmarkSearchCacheMu.RUnlock()
				return nil, IndexInfo{}, nil, serviceErrorf(CodeIndexStale, "index %q generation %d does not match expected_generation %d", name, info.Generation, expectedGeneration)
			}
			return entry.collection, info, s.benchmarkSearchCacheMu.RUnlock, nil
		}
		s.benchmarkSearchCacheMu.RUnlock()

		col, info, err := s.openIndex(ctx, name, expectedGeneration)
		if err != nil {
			return nil, IndexInfo{}, nil, err
		}
		entry = &serviceBenchmarkSearchCacheEntry{collection: col, info: info}
		var closeCol *collections.Collection
		s.benchmarkSearchCacheMu.Lock()
		if s.closed {
			closeCol = col
		} else {
			if s.benchmarkSearchCache == nil {
				s.benchmarkSearchCache = make(map[string]*serviceBenchmarkSearchCacheEntry)
			}
			if existing := s.benchmarkSearchCache[name]; existing.matches(name) {
				closeCol = col
			} else {
				s.benchmarkSearchCache[name] = entry
			}
		}
		s.benchmarkSearchCacheMu.Unlock()
		if closeCol != nil {
			_ = closeCol.CloseVectorIndexPreparedSearchCache()
			if s.isClosed() {
				return nil, IndexInfo{}, nil, serviceClosedError()
			}
		}
	}
}

func (s *Service) primeBenchmarkSearchCache(name string, col *collections.Collection, info IndexInfo) error {
	if s == nil || col == nil {
		return nil
	}
	s.benchmarkSearchCacheMu.RLock()
	existing := s.benchmarkSearchCache[name]
	if !s.closed && existing != nil && existing.collection == col {
		s.benchmarkSearchCacheMu.RUnlock()
		return nil
	}
	s.benchmarkSearchCacheMu.RUnlock()
	var closeCol *collections.Collection
	s.benchmarkSearchCacheMu.Lock()
	if s.closed {
		s.benchmarkSearchCacheMu.Unlock()
		return serviceClosedError()
	}
	if s.benchmarkSearchCache == nil {
		s.benchmarkSearchCache = make(map[string]*serviceBenchmarkSearchCacheEntry)
	}
	if existing := s.benchmarkSearchCache[name]; existing != nil && existing.collection != nil && existing.collection != col {
		closeCol = existing.collection
	}
	s.benchmarkSearchCache[name] = &serviceBenchmarkSearchCacheEntry{collection: col, info: info}
	s.benchmarkSearchCacheMu.Unlock()
	if closeCol != nil {
		return closeCol.CloseVectorIndexPreparedSearchCache()
	}
	return nil
}

func (s *Service) finishVectorMutation(name string, col *collections.Collection, info IndexInfo) error {
	if info.VectorStrategy != collections.VectorIndexStrategyNativeRuntime || !info.Capabilities.NoDocumentVectorSearch {
		return s.invalidateBenchmarkSearchCache(name)
	}
	return s.primeBenchmarkSearchCache(name, col, info)
}

func (s *Service) invalidateBenchmarkSearchCache(name string) error {
	if s == nil {
		return nil
	}
	var closeCol *collections.Collection
	s.benchmarkSearchCacheMu.Lock()
	if entry := s.benchmarkSearchCache[name]; entry != nil {
		closeCol = entry.collection
		delete(s.benchmarkSearchCache, name)
	}
	s.benchmarkSearchCacheMu.Unlock()
	if closeCol != nil {
		return closeCol.CloseVectorIndexPreparedSearchCache()
	}
	return nil
}

func (s *Service) warmBenchmarkSearchCache(ctx context.Context, index string, info IndexInfo, vectorIndexName string) error {
	col, _, release, err := s.acquireBenchmarkSearchIndex(ctx, index, info.Generation)
	if err != nil {
		return err
	}
	defer release()
	response, err := col.WarmVectorIndexPreparedSearch(collections.VectorIndexSearchOptions{
		IndexName: vectorIndexName,
		QueryMode: collections.VectorIndexQueryModeExact,
		TopK:      1,
		EfSearch:  info.VectorEfSearch,
		StatsMode: collections.VectorIndexSearchStatsModeProduction,
	})
	if err != nil {
		return mapVectorIndexSearchError("warm benchmark vector search cache", err)
	}
	if err := validateBenchmarkVectorSearchRoute(BenchmarkVectorQueryModeExact, BenchmarkVectorSearchRequest{}, response); err != nil {
		return err
	}
	return nil
}

func (s *Service) benchmarkSearchCacheSizeForTest() int {
	if s == nil {
		return 0
	}
	s.benchmarkSearchCacheMu.RLock()
	defer s.benchmarkSearchCacheMu.RUnlock()
	return len(s.benchmarkSearchCache)
}

func (s *Service) openIndex(ctx context.Context, name string, expectedGeneration uint64) (*collections.Collection, IndexInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, IndexInfo{}, err
	}
	if s == nil || s.manager == nil {
		return nil, IndexInfo{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	if s.isClosed() {
		return nil, IndexInfo{}, serviceClosedError()
	}
	if err := collections.ValidateCollectionName(name); err != nil {
		return nil, IndexInfo{}, wrapServiceError(CodeInvalidRequest, "invalid index name", err)
	}
	s.benchmarkSearchCacheMu.RLock()
	entry := s.benchmarkSearchCache[name]
	if entry.matches(name) {
		col, info := entry.collection, entry.info
		s.benchmarkSearchCacheMu.RUnlock()
		if expectedGeneration != 0 && expectedGeneration != info.Generation {
			return nil, IndexInfo{}, serviceErrorf(CodeIndexStale, "index %q generation %d does not match expected_generation %d", name, info.Generation, expectedGeneration)
		}
		return col, info, nil
	}
	s.benchmarkSearchCacheMu.RUnlock()
	col, err := s.manager.OpenCollection(name)
	if err != nil {
		return nil, IndexInfo{}, serviceErrorFromCollectionOpen(err, name)
	}
	info, err := indexInfoFromMeta(col.Meta())
	if err != nil {
		return nil, IndexInfo{}, err
	}
	if expectedGeneration != 0 && expectedGeneration != info.Generation {
		return nil, IndexInfo{}, serviceErrorf(CodeIndexStale, "index %q generation %d does not match expected_generation %d", name, info.Generation, expectedGeneration)
	}
	return col, info, nil
}

func indexInfoFromMeta(meta collections.CollectionMeta) (IndexInfo, error) {
	format := meta.Options.DocumentFormat
	if format == "" {
		format = collections.DocumentFormatJSON
	}
	if format != collections.DocumentFormatJSON {
		return IndexInfo{}, serviceErrorf(CodeIndexUnavailable, "collection %q is not a document service JSON index", meta.Name)
	}
	vectorDef, err := serviceVectorIndexDefinition(meta)
	if err != nil {
		return IndexInfo{}, err
	}
	textDef, err := serviceTextIndexDefinition(meta)
	if err != nil {
		return IndexInfo{}, err
	}
	metric, err := metricFromCollection(vectorDef.Metric)
	if err != nil {
		return IndexInfo{}, err
	}
	generation := vectorDef.SchemaGeneration
	if textDef.SchemaGeneration > generation {
		generation = textDef.SchemaGeneration
	}
	if generation == 0 {
		generation = 1
	}
	hybridSearch := vectorDef.Strategy == collections.VectorIndexStrategyColumnGraph && vectorDef.Metric == collections.VectorMetricCosine && vectorDef.Encoding == collections.VectorIndexEncodingFloat32
	scalarFields := scalarFieldsFromCollectionIndexes(meta.Indexes)
	capabilities := indexCapabilities(vectorDef, hybridSearch)
	capabilities.KeywordMetadataFilters = len(scalarFields) > 0
	capabilities.HybridMetadataFilters = len(scalarFields) > 0
	return IndexInfo{
		Name:                 meta.Name,
		Dimension:            vectorDef.Dimensions,
		Metric:               metric,
		Generation:           generation,
		ContractVersion:      ContractVersion,
		EmbeddingField:       defaultEmbeddingField,
		VectorIndexName:      vectorDef.Name,
		VectorStrategy:       vectorDef.Strategy,
		VectorM:              vectorDef.M,
		VectorEfConstruction: vectorDef.EfConstruction,
		VectorEfSearch:       vectorDef.EfSearch,
		QuantizedIndexes:     quantizedIndexInfos(vectorDef),
		ScalarFields:         scalarFields,
		TextField:            defaultTextField,
		TextIndexName:        textDef.Name,
		DocumentType:         defaultCollectionDocType,
		Capabilities:         capabilities,
	}, nil
}

func serviceVectorIndexDefinition(meta collections.CollectionMeta) (collections.VectorIndexDefinition, error) {
	for _, candidate := range meta.VectorIndexes {
		if candidate.Name == defaultVectorIndexName {
			if candidate.Field != defaultEmbeddingField {
				return collections.VectorIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q uses unsupported embedding field %q", meta.Name, candidate.Field)
			}
			if candidate.Dimensions <= 0 {
				return collections.VectorIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q has invalid embedding dimensions", meta.Name)
			}
			if candidate.Encoding != collections.VectorIndexEncodingFloat32 {
				return collections.VectorIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q uses unsupported vector index encoding %s", meta.Name, candidate.Encoding.String())
			}
			if candidate.Strategy != collections.VectorIndexStrategyNativeRuntime && candidate.Strategy != collections.VectorIndexStrategyColumnGraph {
				return collections.VectorIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q uses unsupported vector index strategy %q", meta.Name, candidate.Strategy)
			}
			return candidate, nil
		}
	}
	return collections.VectorIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q does not expose the service vector index %q", meta.Name, defaultVectorIndexName)
}

func serviceTextIndexDefinition(meta collections.CollectionMeta) (collections.TextIndexDefinition, error) {
	for _, candidate := range meta.TextIndexes {
		if candidate.Name != defaultTextIndexName {
			continue
		}
		if candidate.Analyzer != collections.TextAnalyzerSimple {
			return collections.TextIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q text index %q uses unsupported analyzer %q", meta.Name, defaultTextIndexName, candidate.Analyzer)
		}
		if len(candidate.Fields) != 1 || candidate.Fields[0].Field != defaultTextField {
			return collections.TextIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q text index %q must cover only field %q", meta.Name, defaultTextIndexName, defaultTextField)
		}
		if candidate.Fields[0].Weight != 1 {
			return collections.TextIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q text index %q uses unsupported content weight %g", meta.Name, defaultTextIndexName, candidate.Fields[0].Weight)
		}
		if !candidate.StorePositions || candidate.StoreOffsets {
			return collections.TextIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q text index %q uses unsupported position/offset storage", meta.Name, defaultTextIndexName)
		}
		return candidate, nil
	}
	return collections.TextIndexDefinition{}, serviceErrorf(CodeIndexUnavailable, "collection %q does not expose the service text index %q", meta.Name, defaultTextIndexName)
}

type normalizedBenchmarkVectorIndexOptions struct {
	strategy         collections.VectorIndexStrategy
	m                int
	efConstruction   int
	efSearch         int
	quantizedIndexes []collections.QuantizedVectorIndexDefinition
}

func benchmarkVectorIndexOptionsForCreate(metric Metric, opts *BenchmarkVectorIndexOptions) (normalizedBenchmarkVectorIndexOptions, error) {
	out := normalizedBenchmarkVectorIndexOptions{strategy: collections.VectorIndexStrategyNativeRuntime}
	if metric == MetricCosine {
		out.strategy = collections.VectorIndexStrategyColumnGraph
	}
	if opts == nil {
		return out, nil
	}
	if opts.M < 0 || opts.EfConstruction < 0 || opts.EfSearch < 0 {
		return normalizedBenchmarkVectorIndexOptions{}, serviceError(CodeInvalidRequest, "vector index m, ef_construction, and ef_search must be non-negative")
	}
	out.m = opts.M
	out.efConstruction = opts.EfConstruction
	out.efSearch = opts.EfSearch
	if opts.Strategy != "" {
		switch opts.Strategy {
		case collections.VectorIndexStrategyNativeRuntime, collections.VectorIndexStrategyColumnGraph:
			out.strategy = opts.Strategy
		default:
			return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "unsupported vector index strategy %q", opts.Strategy)
		}
	} else if len(opts.QuantizedIndexes) > 0 {
		out.strategy = collections.VectorIndexStrategyColumnGraph
	}
	if out.strategy == collections.VectorIndexStrategyColumnGraph && metric != MetricCosine {
		return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "vector index strategy %q requires metric %q", out.strategy, MetricCosine)
	}
	if len(opts.QuantizedIndexes) > 0 && out.strategy != collections.VectorIndexStrategyColumnGraph {
		return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "quantized vector indexes require strategy %q", collections.VectorIndexStrategyColumnGraph)
	}
	if len(opts.QuantizedIndexes) == 0 {
		return out, nil
	}
	out.quantizedIndexes = make([]collections.QuantizedVectorIndexDefinition, len(opts.QuantizedIndexes))
	seen := make(map[string]struct{}, len(opts.QuantizedIndexes))
	for i, q := range opts.QuantizedIndexes {
		if q.Name == "" {
			return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "vector_index_options.quantized_indexes[%d].name is required", i)
		}
		if err := collections.ValidateIndexName(q.Name); err != nil {
			return normalizedBenchmarkVectorIndexOptions{}, wrapServiceError(CodeInvalidRequest, fmt.Sprintf("vector_index_options.quantized_indexes[%d].name is invalid", i), err)
		}
		if _, ok := seen[q.Name]; ok {
			return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "duplicate quantized index %q", q.Name)
		}
		seen[q.Name] = struct{}{}
		switch q.Codec {
		case "":
			q.Codec = collections.QuantizedVectorCodecScalarU8
		case collections.QuantizedVectorCodecScalarU8, "rabitq_1bit":
		case "brq_1bit":
			return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "vector index quantized_indexes[%d].codec %q is not supported by the document service benchmark contract", i, q.Codec)
		default:
			return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "quantized index %q codec %q is unsupported", q.Name, q.Codec)
		}
		if q.Version == 0 {
			q.Version = 1
		}
		if q.Version > 1 {
			return normalizedBenchmarkVectorIndexOptions{}, serviceErrorf(CodeInvalidRequest, "quantized index %q version=%d is unsupported", q.Name, q.Version)
		}
		collectionQ := collections.QuantizedVectorIndexDefinition{Name: q.Name, Codec: q.Codec, Version: q.Version, ScalarU8Calibration: q.ScalarU8Calibration}
		scalarU8Calibration, err := collections.NormalizeScalarU8CalibrationConfig(defaultVectorIndexName, i, collectionQ)
		if err != nil {
			return normalizedBenchmarkVectorIndexOptions{}, wrapServiceError(CodeInvalidRequest, fmt.Sprintf("vector_index_options.quantized_indexes[%d].scalar_u8_calibration is invalid", i), err)
		}
		collectionQ.ScalarU8Calibration = scalarU8Calibration
		out.quantizedIndexes[i] = collectionQ
	}
	return out, nil
}

func serviceColumnStoreConfig(dimension int) *collections.ColumnStoreConfig {
	return &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{{
			Name:       defaultEmbeddingField,
			Path:       defaultEmbeddingField,
			Owner:      collections.TypedStorageOwnerColumnPart,
			ValueType:  collections.ColumnStoreValueFloat32Vector,
			VectorDims: dimension,
		}},
		RetainedPayload:         collections.ColumnRetainedPayloadFull,
		RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
	}
}

type preparedDocument struct {
	id               string
	raw              []byte
	compactEmbedding bool
}

func preflightServiceVectorAutoRebuildSupported(info IndexInfo) error {
	return nil
}

func scalarU8CalibrationInfoIsLegacy(q QuantizedIndexInfo) bool {
	if q.ScalarU8Calibration == nil {
		return true
	}
	return q.ScalarU8Calibration.Mode == "" || q.ScalarU8Calibration.Mode == collections.ScalarU8CalibrationModeLegacy
}

func upsertPreparedDocument(ctx context.Context, col *collections.Collection, doc preparedDocument, preferInsert bool) (inserted bool, updated bool, err error) {
	const maxAttempts = 4
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctxErr(ctx); err != nil {
			return false, false, err
		}
		if preferInsert {
			if _, err := col.InsertBatch([][]byte{[]byte(doc.id)}, [][]byte{doc.raw}); err == nil {
				return true, false, nil
			} else if !collections.IsDuplicateKeyError(err) {
				return false, false, wrapServiceError(CodeInternal, "insert document failed", err)
			}
			preferInsert = false
			continue
		}
		matched, err := col.Replace([]byte(doc.id), doc.raw)
		if err != nil {
			return false, false, wrapServiceError(CodeInternal, "replace document failed", err)
		}
		if matched {
			return false, true, nil
		}
		preferInsert = true
	}
	return false, false, serviceErrorf(CodeConflict, "document %q changed concurrently during upsert", doc.id)
}

func prepareDocumentsForWrite(documents []Document, info IndexInfo) ([]preparedDocument, error) {
	seen := make(map[string]struct{}, len(documents))
	prepared := make([]preparedDocument, len(documents))
	for i, doc := range documents {
		id := strings.TrimSpace(doc.ID)
		if id == "" {
			return nil, serviceErrorf(CodeInvalidRequest, "documents[%d].id must not be empty", i)
		}
		if id != doc.ID {
			return nil, serviceErrorf(CodeInvalidRequest, "documents[%d].id must not have leading or trailing whitespace", i)
		}
		if _, ok := seen[id]; ok {
			return nil, serviceErrorf(CodeInvalidRequest, "duplicate document id %q", id)
		}
		seen[id] = struct{}{}
		compactEmbedding, err := normalizeDocumentEmbedding(&doc, i)
		if err != nil {
			return nil, err
		}
		if err := validateEmbedding(fmt.Sprintf("documents[%d].embedding", i), doc.Embedding, info.Dimension, info.Metric); err != nil {
			return nil, err
		}
		stored := Document{ID: id, Content: doc.Content, Embedding: append([]float32(nil), doc.Embedding...), Meta: cloneMeta(doc.Meta)}
		raw, err := json.Marshal(stored)
		if err != nil {
			return nil, wrapServiceError(CodeInvalidRequest, fmt.Sprintf("documents[%d] is not JSON-serializable", i), err)
		}
		prepared[i] = preparedDocument{id: id, raw: raw, compactEmbedding: compactEmbedding}
	}
	return prepared, nil
}

func normalizeDocumentEmbedding(doc *Document, index int) (bool, error) {
	encoded := strings.TrimSpace(doc.EmbeddingF32LEBase64)
	if doc.Embedding != nil && encoded != "" {
		return false, serviceErrorf(CodeInvalidRequest, "documents[%d] accepts either embedding or embedding_f32_le_b64, not both", index)
	}
	if encoded == "" {
		return false, nil
	}
	embedding, err := decodeFloat32LEBase64String(encoded, fmt.Sprintf("documents[%d].embedding_f32_le_b64", index))
	if err != nil {
		return false, err
	}
	doc.Embedding = embedding
	doc.EmbeddingF32LEBase64 = ""
	return true, nil
}

func validateDocumentIDs(ids []string) ([]string, error) {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, len(ids))
	for i, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return nil, serviceErrorf(CodeInvalidRequest, "ids[%d] must not be empty", i)
		}
		if trimmed != id {
			return nil, serviceErrorf(CodeInvalidRequest, "ids[%d] must not have leading or trailing whitespace", i)
		}
		if _, ok := seen[id]; ok {
			return nil, serviceErrorf(CodeInvalidRequest, "duplicate document id %q", id)
		}
		seen[id] = struct{}{}
		out[i] = id
	}
	return out, nil
}

func (s *Service) collectMatchingIDs(ctx context.Context, col *collections.Collection, filter *Filter) ([]string, error) {
	var ids []string
	err := s.scanDocuments(ctx, col, func(doc Document) error {
		ok, err := matchFilter(filter, doc)
		if err != nil || !ok {
			return err
		}
		ids = append(ids, doc.ID)
		return nil
	})
	return ids, err
}

func (s *Service) scanDocuments(ctx context.Context, col *collections.Collection, fn func(Document) error) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if col == nil {
		return serviceError(CodeIndexUnavailable, "index collection is unavailable")
	}
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return wrapServiceError(CodeIndexUnavailable, "document materializer unavailable", err)
	}
	defer func() { _ = materializer.Close() }()
	_, err = col.ScanDocumentsFunc(maxServiceScanDocuments, func(record collections.DocumentRecord) (bool, error) {
		if err := ctxErr(ctx); err != nil {
			return false, err
		}
		jsonDoc, err := materializer.StoredDocumentJSON(record.Document)
		if err != nil {
			return false, wrapServiceError(CodeInternal, "stored document materialization failed", err)
		}
		doc, err := decodeStoredDocument(record.ID, jsonDoc)
		if err != nil {
			return false, err
		}
		if err := fn(doc); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		var serviceErr *Error
		if errors.As(err, &serviceErr) {
			return err
		}
		if errors.Is(err, backenddb.ErrClosed) {
			return wrapServiceError(CodeIndexUnavailable, "TreeDB backend is closed", err)
		}
		return wrapServiceError(CodeInternal, "document scan failed", err)
	}
	return nil
}

func decodeStoredDocument(id []byte, raw []byte) (Document, error) {
	var doc Document
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return Document{}, wrapServiceError(CodeInternal, "stored document JSON is invalid", err)
	}
	doc.ID = string(id)
	doc.Score = nil
	if doc.Meta == nil {
		doc.Meta = map[string]any{}
	}
	return doc, nil
}

func validateEmbedding(label string, embedding []float32, dimension int, metric Metric) error {
	if len(embedding) == 0 {
		return serviceErrorf(CodeInvalidRequest, "%s is required", label)
	}
	if len(embedding) != dimension {
		return serviceErrorf(CodeInvalidRequest, "%s dimension %d does not match index dimension %d", label, len(embedding), dimension)
	}
	var norm float64
	for i, value := range embedding {
		f := float64(value)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return serviceErrorf(CodeInvalidRequest, "%s[%d] is not finite", label, i)
		}
		norm += f * f
	}
	if metric == MetricCosine && norm == 0 {
		return serviceErrorf(CodeInvalidRequest, "%s must have non-zero magnitude for cosine metric", label)
	}
	return nil
}

func scoreEmbedding(query, document []float32, metric Metric) (float64, error) {
	if len(query) != len(document) {
		return 0, serviceErrorf(CodeInvalidRequest, "embedding dimension %d does not match query dimension %d", len(document), len(query))
	}
	switch metric {
	case MetricCosine:
		var dot, leftNorm, rightNorm float64
		for i := range query {
			q := float64(query[i])
			d := float64(document[i])
			dot += q * d
			leftNorm += q * q
			rightNorm += d * d
		}
		if leftNorm == 0 || rightNorm == 0 {
			return 0, serviceError(CodeInvalidRequest, "cosine embeddings must have non-zero magnitude")
		}
		return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm)), nil
	case MetricL2:
		var sum float64
		for i := range query {
			delta := float64(query[i]) - float64(document[i])
			sum += delta * delta
		}
		return -sum, nil
	case MetricInnerProduct:
		var dot float64
		for i := range query {
			dot += float64(query[i]) * float64(document[i])
		}
		return dot, nil
	default:
		return 0, serviceErrorf(CodeInvalidRequest, "unsupported metric %q", metric)
	}
}

type scoredDocument struct {
	document Document
	score    float64
}

func serviceDocumentFetchOptions(returnEmbedding bool) collections.DocumentFetchOptions {
	opts := collections.DocumentFetchOptions{Format: collections.DocumentFormatJSON}
	if !returnEmbedding {
		opts.ExcludePaths = []string{defaultEmbeddingField}
	}
	return opts
}

func vectorIndexMaintenanceStatus(status collections.VectorIndexStatus) VectorIndexMaintenanceStatus {
	return VectorIndexMaintenanceStatus{
		Name:             status.Name,
		Strategy:         status.Strategy,
		State:            status.State,
		Reason:           status.Reason,
		Loaded:           status.Loaded,
		RebuildNeeded:    status.RebuildNeeded,
		RootID:           status.RootID,
		NativeRootLoaded: status.NativeRootLoaded,
		NativeRootBytes:  status.NativeRootBytes,
		DurationNanos:    status.Duration.Nanoseconds(),
	}
}

func normalizeBenchmarkVectorQueryMode(mode BenchmarkVectorQueryMode) (BenchmarkVectorQueryMode, collections.VectorIndexQueryMode, error) {
	switch mode {
	case "", BenchmarkVectorQueryModeExact:
		return BenchmarkVectorQueryModeExact, collections.VectorIndexQueryModeExact, nil
	case BenchmarkVectorQueryModeQuantizedOnly:
		return BenchmarkVectorQueryModeQuantizedOnly, collections.VectorIndexQueryModeQuantizedOnly, nil
	case BenchmarkVectorQueryModeQuantizedRerank:
		return BenchmarkVectorQueryModeQuantizedRerank, collections.VectorIndexQueryModeQuantizedRerank, nil
	default:
		return "", "", serviceErrorf(CodeInvalidRequest, "unsupported benchmark vector query_mode %q", mode)
	}
}

func benchmarkVectorSearchResults(results []collections.VectorIndexSearchResult) []BenchmarkVectorSearchResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]BenchmarkVectorSearchResult, len(results))
	for i, result := range results {
		out[i] = BenchmarkVectorSearchResult{ID: string(result.ID), Ordinal: result.Ordinal, Score: result.Score}
	}
	return out
}

func benchmarkVectorSearchIDs(results []collections.VectorIndexSearchResult) []string {
	ids := make([]string, len(results))
	for i := range results {
		ids[i] = string(results[i].ID)
	}
	return ids
}

func validateBenchmarkVectorResponseFormat(format BenchmarkVectorResponseFormat) error {
	switch format {
	case BenchmarkVectorResponseFormatFull, BenchmarkVectorResponseFormatIDs:
		return nil
	default:
		return serviceErrorf(CodeInvalidRequest, "unsupported benchmark vector response_format %q", format)
	}
}

func validateBenchmarkVectorStatsMode(mode collections.VectorIndexSearchStatsMode) error {
	switch mode {
	case collections.VectorIndexSearchStatsModeDefault,
		collections.VectorIndexSearchStatsModeMinimal,
		collections.VectorIndexSearchStatsModeProduction,
		collections.VectorIndexSearchStatsModeFullDiagnostics,
		collections.VectorIndexSearchStatsModeWorkAccounting:
		return nil
	case collections.VectorIndexSearchStatsModeBenchmarkDebug:
		return serviceError(CodeInvalidRequest, "benchmark vector search does not support benchmark_debug stats_mode")
	default:
		return serviceErrorf(CodeInvalidRequest, "unsupported benchmark vector stats_mode %q", mode)
	}
}

func normalizeBenchmarkVectorQueryEmbedding(req *BenchmarkVectorSearchRequest) error {
	if req == nil {
		return nil
	}
	encoded := strings.TrimSpace(req.QueryEmbeddingF32LEBase64)
	if encoded == "" {
		return nil
	}
	if req.QueryEmbedding != nil {
		return serviceError(CodeInvalidRequest, "benchmark vector search accepts either query_embedding or query_embedding_f32_le_b64, not both")
	}
	query, err := decodeBenchmarkVectorQueryEmbeddingF32LEBase64String(encoded)
	if err != nil {
		return err
	}
	req.QueryEmbedding = query
	return nil
}

func decodeBenchmarkVectorQueryEmbeddingF32LEBase64String(encoded string) ([]float32, error) {
	return decodeFloat32LEBase64String(encoded, "query_embedding_f32_le_b64")
}

func decodeFloat32LEBase64String(encoded, label string) ([]float32, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, wrapServiceError(CodeInvalidRequest, "decode "+label+" failed", err)
	}
	return decodeBenchmarkVectorQueryEmbeddingF32LERawWithLabel(raw, label+" decoded")
}

func decodeBenchmarkVectorQueryEmbeddingF32LEBase64Bytes(encoded []byte) ([]float32, error) {
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Decode(raw, encoded)
	if err != nil {
		return nil, wrapServiceError(CodeInvalidRequest, "decode query_embedding_f32_le_b64 failed", err)
	}
	return decodeBenchmarkVectorQueryEmbeddingF32LERaw(raw[:n])
}

func decodeBenchmarkVectorQueryEmbeddingF32LERaw(raw []byte) ([]float32, error) {
	return decodeBenchmarkVectorQueryEmbeddingF32LERawWithLabel(raw, "query_embedding_f32_le_b64 decoded")
}

func decodeBenchmarkVectorQueryEmbeddingF32LERawWithLabel(raw []byte, label string) ([]float32, error) {
	if len(raw)%4 != 0 {
		return nil, serviceErrorf(CodeInvalidRequest, "%s byte length %d is not a multiple of 4", label, len(raw))
	}
	query := make([]float32, len(raw)/4)
	for i := range query {
		query[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return query, nil
}

func validateBenchmarkVectorSearchRequestShape(mode BenchmarkVectorQueryMode, req BenchmarkVectorSearchRequest) error {
	switch mode {
	case BenchmarkVectorQueryModeExact:
		if req.QuantizedIndexName != "" || req.QuantizedRerankCandidates != 0 {
			return serviceError(CodeInvalidRequest, "exact benchmark vector search does not accept quantized_index_name or quantized_rerank_candidates")
		}
	case BenchmarkVectorQueryModeQuantizedOnly:
		if req.QuantizedIndexName == "" {
			return serviceError(CodeInvalidRequest, "quantized_only benchmark vector search requires quantized_index_name")
		}
		if req.QuantizedRerankCandidates != 0 {
			return serviceError(CodeInvalidRequest, "quantized_only benchmark vector search does not accept quantized_rerank_candidates")
		}
	case BenchmarkVectorQueryModeQuantizedRerank:
		if req.QuantizedIndexName == "" {
			return serviceError(CodeInvalidRequest, "quantized_rerank benchmark vector search requires quantized_index_name")
		}
	default:
		return serviceErrorf(CodeInvalidRequest, "unsupported benchmark vector query_mode %q", mode)
	}
	return nil
}

func validateBenchmarkVectorSearchRoute(mode BenchmarkVectorQueryMode, req BenchmarkVectorSearchRequest, response collections.VectorIndexSearchResponse) error {
	diag := response.Diagnostics()
	stats := response.Stats
	if !diag.NoDocumentGuardrailsOK || stats.DocumentsFetched != 0 || stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 {
		return serviceErrorf(CodeIndexUnavailable, "benchmark vector search left the no-document route: diagnostics=%+v", diag)
	}
	switch mode {
	case BenchmarkVectorQueryModeExact:
		nativeRoute := response.Strategy == collections.VectorIndexStrategyNativeRuntime && diag.Route == collections.VectorIndexSearchRouteNativeRuntime && diag.LiveANN.Enabled && diag.LiveANN.ExactFallbacks == 0 && diag.LiveANN.FullRebuilds == 0
		packRoute := response.Strategy == collections.VectorIndexStrategyColumnGraph && diag.Route == collections.VectorIndexSearchRouteExactHNSWSearchPackV1 && diag.ExactHNSWSearchPackNoDocRoute
		if (!nativeRoute && !packRoute) || diag.FallbackReason != collections.VectorIndexSearchFallbackReasonNone {
			return serviceErrorf(CodeIndexUnavailable, "exact benchmark vector search did not use the exact no-document hnsw_search_pack_v1 route: diagnostics=%+v", diag)
		}
	case BenchmarkVectorQueryModeQuantizedOnly, BenchmarkVectorQueryModeQuantizedRerank:
		if err := validateBenchmarkQuantizedVectorSearchRoute(mode, req, response); err != nil {
			return err
		}
	default:
		return serviceErrorf(CodeInvalidRequest, "unsupported benchmark vector query_mode %q", mode)
	}
	return nil
}

func validateBenchmarkQuantizedVectorSearchRoute(mode BenchmarkVectorQueryMode, req BenchmarkVectorSearchRequest, response collections.VectorIndexSearchResponse) error {
	diag := response.Diagnostics()
	stats := response.Stats
	emptySearch := len(response.Results) == 0 && stats.CandidateRows == 0
	columnGraphRoute := stats.SearchRouteHNSWSearchPack == 0 &&
		stats.HNSWSearchPackActive == 0 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback == 1
	packRoute := stats.SearchRouteHNSWSearchPack == 1 &&
		stats.HNSWSearchPackActive == 1 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared == 0 &&
		stats.SearchRouteColumnGraphFallback == 0
	if !columnGraphRoute && !packRoute {
		return serviceErrorf(CodeIndexUnavailable, "quantized benchmark vector search did not use a prepared column_graph or quantized hnsw_search_pack route: diagnostics=%+v", diag)
	}
	if stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetStale != 0 || stats.QuantizedAssetClosed != 0 {
		return serviceErrorf(CodeIndexUnavailable, "quantized benchmark vector asset is unavailable: diagnostics=%+v", diag)
	}
	if !emptySearch && stats.QuantizedScorerActive != 1 {
		return serviceErrorf(CodeIndexUnavailable, "quantized benchmark vector search did not activate a quantized scorer: diagnostics=%+v", diag)
	}
	if !emptySearch && stats.QuantizedScoreCalls == 0 {
		return serviceErrorf(CodeIndexUnavailable, "quantized benchmark vector search did not report quantized score calls: diagnostics=%+v", diag)
	}
	switch mode {
	case BenchmarkVectorQueryModeQuantizedOnly:
		if diag.Route != collections.VectorIndexSearchRouteQuantizedOnly || stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedRerankCandidates != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.PreparedScoreCalls != 0 {
			return serviceErrorf(CodeIndexUnavailable, "quantized_only benchmark vector search did not stay on the quantized-only route: diagnostics=%+v", diag)
		}
	case BenchmarkVectorQueryModeQuantizedRerank:
		if diag.Route != collections.VectorIndexSearchRouteQuantizedRerank || stats.SearchRouteQuantizedRerank != 1 || stats.SearchRouteQuantizedOnly != 0 {
			return serviceErrorf(CodeIndexUnavailable, "quantized_rerank benchmark vector search did not use the quantized-rerank route: diagnostics=%+v", diag)
		}
		if !emptySearch {
			if stats.QuantizedRerankCandidates == 0 || stats.QuantizedRerankExactScoreCalls != stats.QuantizedRerankCandidates || stats.VectorBytesRead == 0 || (!packRoute && stats.NormBytesRead == 0) {
				return serviceErrorf(CodeIndexUnavailable, "quantized_rerank benchmark vector search did not report exact rerank counters: diagnostics=%+v", diag)
			}
			if req.QuantizedRerankCandidates > 0 && stats.QuantizedRerankCandidates > uint64(req.QuantizedRerankCandidates) {
				return serviceErrorf(CodeIndexUnavailable, "quantized_rerank benchmark vector search exceeded requested rerank candidates: got %d want <= %d", stats.QuantizedRerankCandidates, req.QuantizedRerankCandidates)
			}
		}
	}
	return nil
}

func normalizeKeywordSearchOperator(op collections.TextSearchOperator) (collections.TextSearchOperator, error) {
	switch strings.TrimSpace(strings.ToLower(string(op))) {
	case "", string(collections.TextSearchOperatorOR):
		return collections.TextSearchOperatorOR, nil
	case string(collections.TextSearchOperatorAND):
		return collections.TextSearchOperatorAND, nil
	default:
		return "", serviceErrorf(CodeInvalidRequest, "unsupported keyword operator %q", op)
	}
}

func keywordStatsFromCollection(stats collections.TextSearchStats) KeywordSearchStats {
	return KeywordSearchStats{
		QueryTerms:                stats.QueryTerms,
		CandidatesRequested:       stats.TextCandidatesRequested,
		CandidatesReturned:        stats.TextCandidatesReturned,
		PostingsScanned:           maxUint64(stats.TextPostingsScanned, stats.PostingsScanned),
		CandidatesScored:          maxUint64(stats.TextCandidatesScored, stats.CandidatesScored),
		DocumentsFetched:          stats.DocumentsFetched,
		DocumentsMissing:          stats.DocumentsMissing,
		FullDocumentScanFallbacks: stats.FullDocumentScanFallbacks,
		PostingsScanNanos:         stats.PostingsScanNanos,
		CandidateScoreNanos:       stats.CandidateScoreNanos,
		DocumentFetchNanos:        stats.DocumentFetchNanos,
		Truncated:                 stats.Truncated,
		FailClosed:                stats.FailClosed,
		FailClosedReason:          stats.FailClosedReason,
		Unavailable:               stats.Unavailable,
		UnavailableReason:         stats.UnavailableReason,
	}
}

func documentsFromTextSearchResults(results []collections.TextSearchResult, returnEmbedding bool) ([]Document, error) {
	docs := make([]Document, 0, len(results))
	for _, result := range results {
		if len(result.Document) == 0 {
			return nil, serviceErrorf(CodeIndexUnavailable, "keyword result document %q was not fetched", string(result.DocumentID))
		}
		doc, err := decodeStoredDocument(result.DocumentID, result.Document)
		if err != nil {
			return nil, err
		}
		if !returnEmbedding {
			doc.Embedding = nil
		}
		doc.Score = scorePtr(result.Score)
		attachSearchMeta(&doc, keywordSearchMeta(result))
		docs = append(docs, doc)
	}
	if docs == nil {
		docs = []Document{}
	}
	return docs, nil
}

func documentsFromHybridSearchResults(results []collections.HybridSearchResult, plan collections.HybridSearchPlan, returnEmbedding bool) ([]Document, error) {
	docs := make([]Document, 0, len(results))
	for _, result := range results {
		if !result.DocumentFound || len(result.Document) == 0 {
			return nil, serviceErrorf(CodeIndexUnavailable, "hybrid result document %q was not fetched", string(result.ID))
		}
		doc, err := decodeStoredDocument(result.ID, result.Document)
		if err != nil {
			return nil, err
		}
		if !returnEmbedding {
			doc.Embedding = nil
		}
		doc.Score = scorePtr(result.FusedScore)
		attachSearchMeta(&doc, hybridSearchMeta(result, plan))
		docs = append(docs, doc)
	}
	if docs == nil {
		docs = []Document{}
	}
	return docs, nil
}

func keywordSearchMeta(result collections.TextSearchResult) map[string]any {
	meta := map[string]any{
		"type":           "keyword",
		"text_index":     result.IndexName,
		"rank":           result.Rank,
		"score_kind":     string(result.ScoreKind),
		"matched_terms":  append([]string(nil), result.MatchedTerms...),
		"matched_fields": append([]string(nil), result.MatchedFields...),
	}
	if len(result.TextMatches) > 0 {
		meta["text_matches"] = textSearchMatchesMeta(result.TextMatches)
	}
	return meta
}

func hybridSearchMeta(result collections.HybridSearchResult, plan collections.HybridSearchPlan) map[string]any {
	meta := map[string]any{
		"type":              "hybrid",
		"rank":              result.Rank,
		"fusion_method":     string(plan.FusionMethod),
		"fusion_tie_policy": string(plan.FusionTiePolicy),
		"fused_score":       result.FusedScore,
	}
	if len(result.Sources) > 0 {
		sources := make([]map[string]any, len(result.Sources))
		for i, source := range result.Sources {
			sourceMeta := map[string]any{
				"source":       string(source.Source),
				"index_name":   source.IndexName,
				"source_rank":  source.SourceRank,
				"score":        source.Score,
				"score_kind":   string(source.ScoreKind),
				"fusion_score": source.FusionScore,
			}
			if len(source.TextMatches) > 0 {
				sourceMeta["text_matches"] = hybridTextMatchesMeta(source.TextMatches)
			}
			sources[i] = sourceMeta
		}
		meta["sources"] = sources
	}
	return meta
}

func textSearchMatchesMeta(matches []collections.TextSearchMatch) []map[string]any {
	out := make([]map[string]any, len(matches))
	for i, match := range matches {
		out[i] = map[string]any{"field": match.Field, "terms": append([]string(nil), match.Terms...)}
	}
	return out
}

func hybridTextMatchesMeta(matches []collections.HybridTextMatch) []map[string]any {
	out := make([]map[string]any, len(matches))
	for i, match := range matches {
		out[i] = map[string]any{"field": match.Field, "terms": append([]string(nil), match.Terms...)}
	}
	return out
}

func attachSearchMeta(doc *Document, meta map[string]any) {
	if doc.Meta == nil {
		doc.Meta = map[string]any{}
	}
	doc.Meta[searchMetaKey] = meta
}

func rebuildServiceVectorIndex(ctx context.Context, col *collections.Collection) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if col == nil {
		return serviceError(CodeIndexUnavailable, "index collection is unavailable")
	}
	def, err := serviceVectorIndexDefinition(col.Meta())
	if err != nil {
		return err
	}
	if def.Strategy != collections.VectorIndexStrategyColumnGraph {
		return nil
	}
	if _, err := col.RebuildVectorIndex(defaultVectorIndexName); err != nil {
		if serviceVectorRebuildUnsupportedAfterMutation(err) {
			// Column-graph vector rebuild is currently insert-only for this row-ref
			// state. Preserve write/upsert semantics and let subsequent hybrid vector
			// searches fail closed as stale/unavailable instead of scanning.
			return nil
		}
		return mapCollectionMaintenanceError("rebuild service vector index", err)
	}
	return nil
}

func serviceVectorRebuildUnsupportedAfterMutation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "requires insert-only base physical refs")
}

func mapKeywordSearchError(err error) error {
	if err == nil {
		return nil
	}
	var serviceErr *Error
	if errors.As(err, &serviceErr) {
		return err
	}
	if errors.Is(err, collections.ErrTextIndexUnavailable) || errors.Is(err, collections.ErrIndexNotFound) {
		return wrapServiceError(CodeIndexUnavailable, "keyword text index is unavailable", err)
	}
	if errors.Is(err, backenddb.ErrClosed) {
		return wrapServiceError(CodeIndexUnavailable, "TreeDB backend is closed", err)
	}
	return wrapServiceError(CodeInvalidRequest, "keyword search request is invalid or unsupported", err)
}

func mapHybridSearchError(err error) error {
	if err == nil {
		return nil
	}
	var serviceErr *Error
	if errors.As(err, &serviceErr) {
		return err
	}
	if errors.Is(err, collections.ErrHybridSearchStaleIndex) {
		return wrapServiceError(CodeIndexStale, "hybrid search index snapshot is stale", err)
	}
	if errors.Is(err, collections.ErrHybridSearchIndexUnavailable) || errors.Is(err, collections.ErrIndexNotFound) {
		return wrapServiceError(CodeIndexUnavailable, "hybrid search index is unavailable", err)
	}
	if errors.Is(err, collections.ErrHybridSearchUnsupported) {
		return wrapServiceError(CodeUnsupported, "hybrid search request is unsupported", err)
	}
	if errors.Is(err, backenddb.ErrClosed) {
		return wrapServiceError(CodeIndexUnavailable, "TreeDB backend is closed", err)
	}
	return wrapServiceError(CodeInternal, "hybrid search failed", err)
}

func mapVectorIndexSearchError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var serviceErr *Error
	if errors.As(err, &serviceErr) {
		return err
	}
	if errors.Is(err, collections.ErrVectorIndexSnapshotMismatch) {
		return wrapServiceError(CodeSnapshotMismatch, operation+" could not establish matching search and document visibility", err)
	}
	if errors.Is(err, collections.ErrVectorIndexSearchUnavailable) || errors.Is(err, collections.ErrIndexNotFound) {
		return wrapServiceError(CodeIndexUnavailable, operation+" failed closed", err)
	}
	if errors.Is(err, backenddb.ErrClosed) {
		return wrapServiceError(CodeIndexUnavailable, "TreeDB backend is closed", err)
	}
	return wrapServiceError(CodeInternal, operation+" failed", err)
}

func mapCollectionMaintenanceError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var serviceErr *Error
	if errors.As(err, &serviceErr) {
		return err
	}
	if errors.Is(err, collections.ErrIndexNotFound) || errors.Is(err, collections.ErrHybridSearchIndexUnavailable) || errors.Is(err, collections.ErrVectorIndexSearchUnavailable) {
		return wrapServiceError(CodeIndexUnavailable, operation+" failed", err)
	}
	if strings.Contains(err.Error(), "scalar_u8 calibration mode") && strings.Contains(err.Error(), "alpha assets are not built") {
		return wrapServiceError(CodeUnsupported, operation+" unsupported", err)
	}
	if errors.Is(err, backenddb.ErrClosed) {
		return wrapServiceError(CodeIndexUnavailable, "TreeDB backend is closed", err)
	}
	return wrapServiceError(CodeInternal, operation+" failed", err)
}

func maxUint64(values ...uint64) uint64 {
	var max uint64
	for _, value := range values {
		if value > max {
			max = value
		}
	}
	return max
}

func cloneMeta(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneJSONValue(value)
	}
	return out
}

func cloneJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneMeta(v)
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = cloneJSONValue(v[i])
		}
		return out
	default:
		return v
	}
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return wrapServiceError(CodeIndexUnavailable, "request context is no longer available", err)
	}
	return nil
}
