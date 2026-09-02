// Package embedding provides the collection-native pluggable embedder seam
// for TreeDB collections: a batch-first interface for turning text into
// fixed-width float32 vectors, a provider registry, and a deterministic
// reference embedder that keeps in-repo tests and benchmarks hermetic.
//
// # Contracts
//
// EmbedBatch is ordered and fail-closed: the returned slice is index-aligned
// to the inputs, an empty batch is a typed error, and any per-text failure —
// including context cancellation — fails the whole batch with no partial
// results. Dimension validation against the target vector index definition
// happens at ingest time (see collections.EmbedForIngest) and fails closed
// before any write lands.
//
// # Determinism
//
// The reference "hashing" embedder is a pure function of (text, config):
// integer feature-hashing accumulation followed by IEEE-754 correctly rounded
// normalization. No randomness, clocks, map iteration, global state, or
// network participate in output construction, so identical inputs plus config
// produce bit-identical vectors on every platform and run. Committed parity
// fixtures under testdata/ pin this guarantee.
package embedding

import (
	"context"
	"errors"
	"fmt"
)

// Embedder turns text into fixed-width vectors.
//
// Implementations must satisfy the package contracts: ordered fail-closed
// batches, deterministic output for deterministic inputs, and no unbounded
// concurrency (use RunBatch or equivalent bounded pooling). Implementations
// need not support concurrent method calls on one instance; callers must
// serialize shared instances or create one instance per worker.
type Embedder interface {
	// Dimensions reports the fixed width of every returned vector.
	Dimensions() int
	// EmbedBatch returns exactly one vector per input text, index-aligned to
	// texts. An empty texts slice fails with ErrEmptyBatch. Any failure — of
	// any position, or of ctx — fails the whole batch and returns nil vectors.
	EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error)
}

// Fail-closed errors. Callers should test with errors.Is; implementations wrap
// these with positional context via fmt.Errorf %w.
var (
	// ErrUnknownProvider is returned when a Config names no registered provider.
	ErrUnknownProvider = errors.New("embedding: unknown provider")
	// ErrProviderAlreadyRegistered is returned by Registry.Register on a
	// duplicate name.
	ErrProviderAlreadyRegistered = errors.New("embedding: provider already registered")
	// ErrInvalidEmbedder is returned when a provider factory returns a nil or
	// typed-nil Embedder.
	ErrInvalidEmbedder = errors.New("embedding: invalid embedder")
	// ErrEmptyBatch is returned by EmbedBatch for zero input texts.
	ErrEmptyBatch = errors.New("embedding: empty batch")
	// ErrDimensionMismatch is returned when declared dimensions disagree —
	// between config and provider, or embedder and target vector index.
	ErrDimensionMismatch = errors.New("embedding: dimension mismatch")
	// ErrInvalidOutput is returned when a provider batch result violates the
	// ordered, fixed-width, finite-vector contract.
	ErrInvalidOutput = errors.New("embedding: invalid output")
)

// Config selects and parameterizes an embedder. Validate must pass before any
// registry Create; all violations are fail-closed errors.
type Config struct {
	// Provider names a registered embedder factory (e.g. "hashing").
	Provider string `json:"provider"`
	// Dimensions is the declared vector width. It must match both the
	// provider's capability and the target vector index definition at
	// ingest time.
	Dimensions int `json:"dimensions"`
}

// Validate fails closed on any configuration that cannot be honored exactly.
func (c Config) Validate() error {
	if c.Provider == "" {
		return fmt.Errorf("embedding: provider must be named")
	}
	if c.Dimensions <= 0 {
		return fmt.Errorf("embedding: dimensions must be positive, got %d", c.Dimensions)
	}
	return nil
}
