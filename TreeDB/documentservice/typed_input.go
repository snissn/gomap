package documentservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

// TypedDocumentsRequest carries declared values separately from residual JSON.
// All slices are borrowed only for the synchronous call; the core planner owns
// published values. ExpectedGeneration is mandatory for this native boundary.
type TypedDocumentsRequest struct {
	ExpectedGeneration uint64
	IDs                [][]byte
	Retained           [][]byte
	Columns            []collections.TypedColumnBatch
}

// UpsertTypedDocuments performs one atomic mixed upsert without reconstructing
// indexed JSON. It has the same durability and unchanged-row counting contract
// as UpsertDocuments; graph build/admission remains explicit.
func (s *Service) UpsertTypedDocuments(ctx context.Context, index string, req TypedDocumentsRequest) (UpsertDocumentsResponse, error) {
	if s == nil {
		return UpsertDocumentsResponse{}, serviceError(CodeInternal, "service is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if req.ExpectedGeneration == 0 {
		return UpsertDocumentsResponse{}, serviceError(CodeInvalidRequest, "typed upsert requires expected generation")
	}
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return UpsertDocumentsResponse{}, err
	}
	if !info.TypedInput {
		return UpsertDocumentsResponse{}, serviceError(CodeUnsupported, "typed upsert requires declared typed input")
	}
	if len(req.IDs) == 0 || len(req.IDs) != len(req.Retained) || len(req.Columns) != 2+len(info.ScalarFields) {
		return UpsertDocumentsResponse{}, serviceError(CodeInvalidRequest, "typed upsert shape does not match schema")
	}
	names := make([]string, len(req.IDs))
	for i, id := range req.IDs {
		names[i] = string(id)
	}
	if _, err := validateDocumentIDs(names); err != nil {
		return UpsertDocumentsResponse{}, err
	}
	seen := make(map[string]bool, len(req.Columns))
	for _, column := range req.Columns {
		if seen[column.Name] {
			return UpsertDocumentsResponse{}, serviceError(CodeInvalidRequest, "duplicate typed column")
		}
		seen[column.Name] = true
		if column.Name == defaultEmbeddingField {
			if len(column.Float32Vectors) != len(req.IDs) || len(column.Strings) != 0 {
				return UpsertDocumentsResponse{}, serviceError(CodeInvalidRequest, "invalid typed vector shape")
			}
			for _, vector := range column.Float32Vectors {
				if err := ctxErr(ctx); err != nil {
					return UpsertDocumentsResponse{}, err
				}
				if err := validateEmbedding("embedding", vector, info.Dimension, info.Metric); err != nil {
					return UpsertDocumentsResponse{}, err
				}
			}
		} else {
			valid := column.Name == defaultTextField
			for _, field := range info.ScalarFields {
				valid = valid || column.Name == field.Field
			}
			if !valid || len(column.Strings) != len(req.IDs) || len(column.Float32Vectors) != 0 {
				return UpsertDocumentsResponse{}, serviceError(CodeInvalidRequest, "invalid typed string column")
			}
		}
	}
	if err := ctxErr(ctx); err != nil {
		return UpsertDocumentsResponse{}, err
	}
	return finishTypedDocuments(col, info, req.IDs, req.Retained, req.Columns, names, 0)
}

func (s *Service) optimizeTypedInput(ctx context.Context, col *collections.Collection, info IndexInfo, req OptimizeIndexRequest) (OptimizeIndexResponse, error) {
	started := time.Now()
	action := req.ColumnGraphAction
	if action == "" {
		action = "build"
	}
	if (action == "build" || action == "ensure") && req.ColumnGraphServing == nil {
		return OptimizeIndexResponse{}, serviceError(CodeInvalidRequest, "typed build/ensure requires positive column_graph_serving limits")
	}
	if action != "build" && action != "ensure" && action != "fold" && action != "renew" {
		return OptimizeIndexResponse{}, serviceError(CodeInvalidRequest, "unknown column_graph_action")
	}
	if (action == "fold" || action == "renew") && req.ColumnGraphServing != nil {
		return OptimizeIndexResponse{}, serviceError(CodeInvalidRequest, "fold/renew uses previously admitted limits")
	}
	var status collections.VectorIndexStatus
	work := func() error {
		var err error
		switch action {
		case "build":
			status, err = col.RebuildVectorIndex(info.VectorIndexName)
			if err == nil {
				err = col.EnsureColumnGraphServing(ctx, info.VectorIndexName, *req.ColumnGraphServing)
			}
		case "ensure":
			err = col.EnsureColumnGraphServing(ctx, info.VectorIndexName, *req.ColumnGraphServing)
		case "fold":
			err = col.FoldColumnGraphServing(ctx, info.VectorIndexName)
		case "renew":
			_, err = col.RenewColumnGraphServing(ctx, info.VectorIndexName)
		}
		return err
	}
	var err error
	if s.deferredVectorBuildMaintenance != nil {
		err = s.deferredVectorBuildMaintenance.Finalize(ctx, info.Name, info.Generation, s.manager.FlushAll, work)
	} else {
		_, err = s.manager.SyncForStandaloneWriteConcern()
		if err == nil {
			err = work()
		}
	}
	if err != nil {
		return OptimizeIndexResponse{}, mapCollectionMaintenanceError("typed graph "+action, err)
	}
	if action != "build" {
		status, err = col.VectorIndexStatus(info.VectorIndexName)
		if err != nil {
			return OptimizeIndexResponse{}, mapCollectionMaintenanceError("typed graph status", err)
		}
	}
	return OptimizeIndexResponse{Index: info, VectorIndexName: info.VectorIndexName, Status: vectorIndexMaintenanceStatus(status), Timing: OptimizeIndexTiming{TotalNanos: time.Since(started).Nanoseconds()}}, nil
}

func (s *Service) upsertTypedDocuments(ctx context.Context, col *collections.Collection, info IndexInfo, req UpsertDocumentsRequest) (UpsertDocumentsResponse, error) {
	names := make([]string, len(req.Documents))
	for i := range req.Documents {
		names[i] = req.Documents[i].ID
	}
	if _, err := validateDocumentIDs(names); err != nil {
		return UpsertDocumentsResponse{}, err
	}
	ids := make([][]byte, len(names))
	retained := make([][]byte, len(names))
	columns := make([]collections.TypedColumnBatch, 2+len(info.ScalarFields))
	columns[0] = collections.TypedColumnBatch{Name: defaultEmbeddingField, Float32Vectors: make([][]float32, len(names))}
	columns[1] = collections.TypedColumnBatch{Name: defaultTextField, Strings: make([]string, len(names))}
	for j, field := range info.ScalarFields {
		columns[j+2] = collections.TypedColumnBatch{Name: field.Field, Strings: make([]string, len(names))}
	}
	compact := 0
	for i, doc := range req.Documents {
		if err := ctxErr(ctx); err != nil {
			return UpsertDocumentsResponse{}, err
		}
		encoded, err := normalizeDocumentEmbedding(&doc, i)
		if err != nil {
			return UpsertDocumentsResponse{}, err
		}
		if encoded {
			compact++
		}
		if err := validateEmbedding(fmt.Sprintf("documents[%d].embedding", i), doc.Embedding, info.Dimension, info.Metric); err != nil {
			return UpsertDocumentsResponse{}, err
		}
		ids[i] = []byte(doc.ID)
		columns[0].Float32Vectors[i] = doc.Embedding
		columns[1].Strings[i] = doc.Content
		residual := cloneMeta(doc.Meta)
		for j, field := range info.ScalarFields {
			value, ok := lookupFilterField(doc, field.Field)
			str, stringOK := value.(string)
			if !ok || !stringOK {
				return UpsertDocumentsResponse{}, serviceErrorf(CodeInvalidRequest, "documents[%d].%s must be a string", i, field.Field)
			}
			columns[j+2].Strings[i] = str
			path := strings.Split(strings.TrimPrefix(field.Field, "meta."), ".")
			parent := residual
			for _, part := range path[:len(path)-1] {
				parent, _ = parent[part].(map[string]any)
			}
			delete(parent, path[len(path)-1])
		}
		// Only flexible residual payload is serialized; indexed values stay in
		// declared carriers and are never extracted from this JSON by the planner.
		retained[i], err = json.Marshal(struct {
			ID   string         `json:"id"`
			Meta map[string]any `json:"meta,omitempty"`
		}{doc.ID, residual})
		if err != nil {
			return UpsertDocumentsResponse{}, wrapServiceError(CodeInvalidRequest, "retained metadata is not JSON-serializable", err)
		}
	}
	return finishTypedDocuments(col, info, ids, retained, columns, names, compact)
}

func finishTypedDocuments(col *collections.Collection, info IndexInfo, ids, retained [][]byte, columns []collections.TypedColumnBatch, names []string, compact int) (UpsertDocumentsResponse, error) {
	updated, err := col.UpsertTypedBatch(ids, retained, columns)
	if err != nil {
		return UpsertDocumentsResponse{}, wrapServiceError(CodeInternal, "typed upsert failed", err)
	}
	return UpsertDocumentsResponse{Index: info, Upserted: len(ids), Inserted: len(ids) - updated, Updated: updated, IDs: names, CompactEmbeddings: compact}, nil
}

// The schema, not a process-local flag, identifies selected typed input after
// reopen. Operational graph admission remains a separate lifecycle action.
func serviceTypedInputConfig(dimension int, fields []normalizedScalarField) (*collections.ColumnStoreConfig, error) {
	cfg := serviceColumnStoreConfig(dimension)
	cfg.Columns = append(cfg.Columns, collections.ColumnStoreColumn{
		Name: defaultTextField, Path: defaultTextField, ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset,
	})
	for _, field := range fields {
		if field.collectionTy != collections.IndexValueString {
			return nil, serviceError(CodeInvalidRequest, "typed input requires declared string scalar fields")
		}
		cfg.Columns = append(cfg.Columns, collections.ColumnStoreColumn{
			Name: field.field, Path: field.field, ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset,
		})
	}
	return cfg, nil
}

func serviceUsesTypedInput(meta collections.CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload != collections.ColumnRetainedPayloadNonColumn ||
		cfg.RetainedPayloadEncoding != collections.ColumnRetainedPayloadEncodingJSON ||
		len(cfg.Columns) != len(meta.Indexes)+2 || len(meta.VectorIndexes) != 1 ||
		meta.VectorIndexes[0].Strategy != collections.VectorIndexStrategyColumnGraph {
		return false
	}
	for _, column := range cfg.Columns {
		if column.Nullable {
			return false
		}
		if column.Path != defaultEmbeddingField && column.Owner != "" && column.Owner != collections.TypedStorageOwnerRowAsset {
			return false
		}
		switch column.Path {
		case defaultEmbeddingField:
			if column.Name != defaultEmbeddingField || column.Path != defaultEmbeddingField ||
				column.ValueType != collections.ColumnStoreValueFloat32Vector ||
				column.Owner != collections.TypedStorageOwnerColumnPart || column.VectorDims != meta.VectorIndexes[0].Dimensions {
				return false
			}
		case defaultTextField:
			if column.Name != defaultTextField || column.Path != defaultTextField || column.ValueType != collections.ColumnStoreValueString {
				return false
			}
		default:
			matched := false
			for _, index := range meta.Indexes {
				if column.Name == index.Field && column.Path == index.Field && column.ValueType == collections.ColumnStoreValueString && index.ValueType == collections.IndexValueString {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	return true
}
