package collections

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections/embedding"
)

// EmbedForIngest resolves the configured embedder against the named vector
// index definition and returns one vector per text, index-aligned.
//
// This is the ingest-path provider and output gate, mirroring the chunked-
// ingest plan-before-mutation pattern: every failure — unknown vector index,
// dimension mismatch, unknown provider, invalid output, or batch/cancellation
// failure — happens here, before the caller writes anything. A failed call
// leaves the collection untouched by construction:
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
		return nil, fmt.Errorf("collections: ingest embed into %q: %w", vectorIndexName, ErrIndexNotFound)
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
		return nil, fmt.Errorf("collections: provider %q embed into vector index %q: %w", cfg.Provider, def.Name, err)
	}
	if err := validateEmbeddingOutput(vectors, len(texts), def); err != nil {
		return nil, fmt.Errorf("collections: validate provider %q output for vector index %q: %w", cfg.Provider, def.Name, err)
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
		return fmt.Errorf("collections: validate embedder for %q: %w", vectorIndexName, ErrIndexNotFound)
	}
	dimensions := emb.Dimensions()
	if dimensions != def.Dimensions {
		return fmt.Errorf("collections: vector index %q declares %d dims, embedder builds %d: %w",
			def.Name, def.Dimensions, dimensions, embedding.ErrDimensionMismatch)
	}
	return nil
}

// EmbedderForIngest resolves the configured embedder against the named vector
// index definition and returns it for repeated batch use, applying the same
// fail-closed gates as EmbedForIngest — unknown vector index, config/provider
// dimension agreement, and declared-vs-index dimension agreement — without
// embedding anything. Batch composition paths (see IngestSources) call this
// once per batch and then drive EmbedBatch per source through the returned
// embedder.
func (c *Collection) EmbedderForIngest(vectorIndexName string, cfg embedding.Config) (embedding.Embedder, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, vectorIndexName)
	if !ok {
		return nil, fmt.Errorf("collections: ingest embed into %q: %w", vectorIndexName, ErrIndexNotFound)
	}
	if cfg.Dimensions != def.Dimensions {
		return nil, fmt.Errorf("collections: ingest embed into vector index %q wants %d dims, config declares %d: %w",
			def.Name, def.Dimensions, cfg.Dimensions, embedding.ErrDimensionMismatch)
	}
	emb, err := embedding.DefaultRegistry().Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("collections: ingest embed into vector index %q: %w", def.Name, err)
	}
	return emb, nil
}
