package collections

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections/embedding"
)

// Fail-closed ingest-embedding errors. Callers should test with errors.Is.
var (
	// errCollectionEmbedderUnknownVectorIndex is returned when an embed
	// request names no vector index defined on the collection.
	errCollectionEmbedderUnknownVectorIndex = errors.New("collections: unknown vector index")
)

// EmbedForIngest resolves the configured embedder against the named vector
// index definition and returns one vector per text, index-aligned.
//
// This is the ingest-path dimension gate, mirroring the chunked-ingest
// plan-before-mutation pattern: every failure — unknown vector index,
// dimension mismatch against the declared definition, unknown provider, or
// batch/cancellation failure — happens here, before the caller writes
// anything. A failed call leaves the collection untouched by construction:
// this method performs no mutations of its own.
func (c *Collection) EmbedForIngest(ctx context.Context, vectorIndexName string, texts [][]byte, cfg embedding.Config) ([][]float32, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, vectorIndexName)
	if !ok {
		return nil, fmt.Errorf("collections: ingest embed into %q: %w", vectorIndexName, errCollectionEmbedderUnknownVectorIndex)
	}
	if cfg.Dimensions != def.Dimensions {
		return nil, fmt.Errorf("collections: ingest embed into vector index %q wants %d dims, config declares %d: %w",
			def.Name, def.Dimensions, cfg.Dimensions, embedding.ErrDimensionMismatch)
	}
	emb, err := embedding.DefaultRegistry().Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("collections: ingest embed into vector index %q: %w", def.Name, err)
	}
	vectors, err := emb.EmbedBatch(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("collections: ingest embed into vector index %q: %w", def.Name, err)
	}
	return vectors, nil
}

// ValidateEmbedderForVectorIndex fails closed unless emb declares exactly the
// dimensions of the named vector index definition. Callers that hold their own
// Embedder instance use this gate before writing vectors through any mutation
// path; EmbedForIngest applies the same check automatically.
func (c *Collection) ValidateEmbedderForVectorIndex(vectorIndexName string, emb embedding.Embedder) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	if emb == nil {
		return fmt.Errorf("collections: validate embedder for %q: embedder must be non-nil", vectorIndexName)
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, vectorIndexName)
	if !ok {
		return fmt.Errorf("collections: validate embedder for %q: %w", vectorIndexName, errCollectionEmbedderUnknownVectorIndex)
	}
	if emb.Dimensions() != def.Dimensions {
		return fmt.Errorf("collections: vector index %q declares %d dims, embedder builds %d: %w",
			def.Name, def.Dimensions, emb.Dimensions(), embedding.ErrDimensionMismatch)
	}
	return nil
}
