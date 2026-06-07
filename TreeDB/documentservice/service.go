package documentservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const maxServiceScanDocuments = int(^uint(0) >> 1)

// Service maps the document/search contract onto TreeDB collections.
type Service struct {
	manager *collections.CollectionManager
}

// New returns a document/search service backed by manager.
func New(manager *collections.CollectionManager) *Service {
	return &Service{manager: manager}
}

// CreateIndex creates or opens a compatible document service index.
func (s *Service) CreateIndex(ctx context.Context, req CreateIndexRequest) (IndexInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return IndexInfo{}, err
	}
	if s == nil || s.manager == nil {
		return IndexInfo{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
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
	meta := &collections.CollectionMeta{
		Name: req.Name,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:             defaultVectorIndexName,
			Field:            defaultEmbeddingField,
			Metric:           collectionMetric,
			Dimensions:       req.Dimension,
			Encoding:         collections.VectorIndexEncodingFloat32,
			Strategy:         collections.VectorIndexStrategyNativeRuntime,
			SchemaGeneration: 1,
		}},
	}
	created, err := s.manager.CreateCollection(meta)
	if err != nil {
		if strings.Contains(err.Error(), "incompatible") {
			return IndexInfo{}, wrapServiceError(CodeConflict, fmt.Sprintf("index %q already exists with an incompatible schema", req.Name), err)
		}
		return IndexInfo{}, wrapServiceError(CodeInternal, "create index failed", err)
	}
	return indexInfoFromMeta(*created)
}

// OpenIndex returns metadata for an existing document service index.
func (s *Service) OpenIndex(ctx context.Context, name string) (IndexInfo, error) {
	col, info, err := s.openIndex(ctx, name, 0)
	_ = col
	return info, err
}

// UpsertDocuments writes or replaces documents in index.
func (s *Service) UpsertDocuments(ctx context.Context, index string, req UpsertDocumentsRequest) (UpsertDocumentsResponse, error) {
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
	insertIDs := make([][]byte, 0, len(prepared))
	insertDocs := make([][]byte, 0, len(prepared))
	updates := make([]preparedDocument, 0)
	for _, doc := range prepared {
		if err := ctxErr(ctx); err != nil {
			return UpsertDocumentsResponse{}, err
		}
		ids = append(ids, doc.id)
		current, err := col.Get([]byte(doc.id))
		if err != nil {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "read before upsert failed", err)
		}
		if current == nil {
			insertIDs = append(insertIDs, []byte(doc.id))
			insertDocs = append(insertDocs, doc.raw)
			continue
		}
		updates = append(updates, doc)
	}
	inserted := 0
	if len(insertIDs) > 0 {
		if _, err := col.InsertBatch(insertIDs, insertDocs); err != nil {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "insert documents failed", err)
		}
		inserted = len(insertIDs)
	}
	updated := 0
	for _, doc := range updates {
		if err := ctxErr(ctx); err != nil {
			return UpsertDocumentsResponse{}, err
		}
		matched, err := col.Replace([]byte(doc.id), doc.raw)
		if err != nil {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "replace document failed", err)
		}
		if !matched {
			if _, err := col.InsertBatch([][]byte{[]byte(doc.id)}, [][]byte{doc.raw}); err != nil {
				return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "insert raced replacement failed", err)
			}
			inserted++
			continue
		}
		updated++
	}
	return UpsertDocumentsResponse{Index: info, Upserted: len(prepared), Inserted: inserted, Updated: updated, IDs: ids}, nil
}

// DeleteDocuments deletes explicit IDs or documents matching a filter.
func (s *Service) DeleteDocuments(ctx context.Context, index string, req DeleteDocumentsRequest) (DeleteDocumentsResponse, error) {
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

// SearchDenseVector runs exact dense scoring with optional metadata filters.
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
	return DenseVectorSearchResponse{Index: info, Documents: docs, Metric: info.Metric, Exact: true, Candidates: candidateCount}, nil
}

// SearchKeyword intentionally fails closed until TreeDB ranked text execution is implemented.
func (s *Service) SearchKeyword(context.Context, string, json.RawMessage) error {
	return serviceError(CodeUnsupported, "keyword search is not implemented; TreeDB text search currently fails closed and the service does not scan documents as a fallback")
}

// SearchHybrid intentionally fails closed until TreeDB hybrid execution is implemented.
func (s *Service) SearchHybrid(context.Context, string, json.RawMessage) error {
	return serviceError(CodeUnsupported, "hybrid search is not implemented; the service does not run text/hybrid document-scan fallbacks")
}

func (s *Service) openIndex(ctx context.Context, name string, expectedGeneration uint64) (*collections.Collection, IndexInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, IndexInfo{}, err
	}
	if s == nil || s.manager == nil {
		return nil, IndexInfo{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	if err := collections.ValidateCollectionName(name); err != nil {
		return nil, IndexInfo{}, wrapServiceError(CodeInvalidRequest, "invalid index name", err)
	}
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
	var def collections.VectorIndexDefinition
	found := false
	for _, candidate := range meta.VectorIndexes {
		if candidate.Name == defaultVectorIndexName || candidate.Field == defaultEmbeddingField {
			def = candidate
			found = true
			break
		}
	}
	if !found {
		return IndexInfo{}, serviceErrorf(CodeIndexUnavailable, "collection %q does not expose the service embedding index", meta.Name)
	}
	if def.Field != defaultEmbeddingField {
		return IndexInfo{}, serviceErrorf(CodeIndexUnavailable, "collection %q uses unsupported embedding field %q", meta.Name, def.Field)
	}
	if def.Dimensions <= 0 {
		return IndexInfo{}, serviceErrorf(CodeIndexUnavailable, "collection %q has invalid embedding dimensions", meta.Name)
	}
	metric, err := metricFromCollection(def.Metric)
	if err != nil {
		return IndexInfo{}, err
	}
	generation := def.SchemaGeneration
	if generation == 0 {
		generation = 1
	}
	return IndexInfo{
		Name:            meta.Name,
		Dimension:       def.Dimensions,
		Metric:          metric,
		Generation:      generation,
		ContractVersion: ContractVersion,
		EmbeddingField:  defaultEmbeddingField,
		DocumentType:    defaultCollectionDocType,
		Capabilities:    indexCapabilities(),
	}, nil
}

type preparedDocument struct {
	id  string
	raw []byte
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
		if err := validateEmbedding(fmt.Sprintf("documents[%d].embedding", i), doc.Embedding, info.Dimension, info.Metric); err != nil {
			return nil, err
		}
		stored := Document{ID: id, Content: doc.Content, Embedding: append([]float32(nil), doc.Embedding...), Meta: cloneMeta(doc.Meta)}
		raw, err := json.Marshal(stored)
		if err != nil {
			return nil, wrapServiceError(CodeInvalidRequest, fmt.Sprintf("documents[%d] is not JSON-serializable", i), err)
		}
		prepared[i] = preparedDocument{id: id, raw: raw}
	}
	return prepared, nil
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
			delta := float64(query[i] - document[i])
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
