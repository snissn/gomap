package embedding

import (
	"context"
	"fmt"
	"math"
	"strings"
	"unicode"
)

// ProviderHashing names the deterministic reference embedder: feature hashing
// over whitespace-delimited tokens into a fixed-width signed integer sketch,
// L2-normalized. It exists to keep in-repo tests and benchmarks hermetic and
// reproducible; real-model providers belong to future work.
const ProviderHashing = "hashing"

// HashingEmbedder is the deterministic reference Embedder. Output is a pure
// function of (text, dimensions): see the package determinism contract.
type HashingEmbedder struct {
	dims int
}

// NewHashingEmbedder validates config and builds the reference embedder.
func NewHashingEmbedder(cfg Config) (Embedder, error) {
	if cfg.Provider != ProviderHashing {
		return nil, fmt.Errorf("embedding: provider %q: want %q", cfg.Provider, ProviderHashing)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &HashingEmbedder{dims: cfg.Dimensions}, nil
}

// Dimensions reports the fixed vector width.
func (h *HashingEmbedder) Dimensions() int { return h.dims }

// EmbedBatch implements the ordered fail-closed batch contract over RunBatch's
// bounded pool. Each text is sketched serially inside its worker, so bucket
// accumulation order is token order regardless of scheduling — output stays
// bit-identical at any concurrency.
func (h *HashingEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, ErrEmptyBatch
	}
	out := make([][]float32, len(texts))
	err := RunBatch(ctx, len(texts), func(_ context.Context, i int) error {
		out[i] = h.embedOne(texts[i])
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// fnvOffsetBasisAlt is an independent FNV-1a offset basis used for sign bits,
// so index and sign derive from uncorrelated hash streams without extra state.
const fnvOffsetBasisAlt uint64 = 0x6c62272e07bb0142 ^ 0x9e3779b97f4a7c15

const fnvOffsetBasis uint64 = 14695981039346656037

const fnvPrime uint64 = 1099511628211

func fnv1a(data string, basis uint64) uint64 {
	h := basis
	for i := 0; i < len(data); i++ {
		h ^= uint64(data[i])
		h *= fnvPrime
	}
	return h
}

// embedOne sketches text into dims signed integer buckets, then normalizes.
//
// Determinism rests on exact arithmetic only: token hashes are FNV-1a over
// bytes (integer math), bucket accumulation is integer addition in token
// order, and normalization uses only IEEE-754 correctly rounded operations
// (integer sum of squares -> Sqrt -> divide), immune to FMA contraction or
// platform rounding drift. A degenerate input with no tokens yields the
// all-zero vector rather than NaN.
func (h *HashingEmbedder) embedOne(text []byte) []float32 {
	counts := make([]int64, h.dims)
	for _, token := range strings.FieldsFunc(string(text), unicode.IsSpace) {
		index := fnv1a(token, fnvOffsetBasis)
		sign := fnv1a(token, fnvOffsetBasisAlt)
		bucket := int(index % uint64(h.dims))
		if sign&1 == 1 {
			counts[bucket]++
		} else {
			counts[bucket]--
		}
	}
	vec := make([]float32, h.dims)
	var normSq int64
	for _, c := range counts {
		normSq += c * c
	}
	if normSq == 0 {
		return vec
	}
	norm := math.Sqrt(float64(normSq))
	for i, c := range counts {
		vec[i] = float32(float64(c) / norm)
	}
	return vec
}

// compile-time interface check
var _ Embedder = (*HashingEmbedder)(nil)
