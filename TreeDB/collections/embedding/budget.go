package embedding

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const defaultMinRunesPerToken = 2.5

// ContextWindowReporter is an optional interface implemented by Embedder
// providers that know their model's maximum input context window in tokens.
type ContextWindowReporter interface {
	MaxContextTokens() int
}

var (
	// ErrContextWindowExceeded is returned when a configured chunk size or input
	// text exceeds the safe rune budget derived from an embedder's context
	// window.
	ErrContextWindowExceeded = errors.New("embedding: input or chunk size exceeds model context window")
	// ErrContextWindowUnknown is returned when RequireKnownLimit is true and
	// neither ContextBudgetOptions.MaxContextTokens nor the embedder reports a
	// positive token limit.
	ErrContextWindowUnknown = errors.New("embedding: model context window is unknown")
)

// ContextBudgetOptions configures fail-closed context-window budget validation
// between a rune-based chunker and a token-limited embedder.
type ContextBudgetOptions struct {
	MaxContextTokens   int     // 0 queries emb.(ContextWindowReporter); negative fails closed
	MinRunesPerToken   float64 // 0 defaults to 2.5; negative/NaN/Inf fails closed
	HeaderReserveRunes int     // non-negative overhead reserved for contextual headers; negative fails closed
	RequireKnownLimit  bool    // when true and resolved maxTokens <= 0, returns ErrContextWindowUnknown
}

func resolveContextBudget(emb Embedder, opts ContextBudgetOptions) (maxTokens int, minRunesPerToken float64, maxSafeRunes int, hasLimit bool, err error) {
	if opts.MaxContextTokens < 0 {
		return 0, 0, 0, false, fmt.Errorf("embedding: MaxContextTokens must be non-negative, got %d", opts.MaxContextTokens)
	}
	if opts.HeaderReserveRunes < 0 {
		return 0, 0, 0, false, fmt.Errorf("embedding: HeaderReserveRunes must be non-negative, got %d", opts.HeaderReserveRunes)
	}
	if math.IsNaN(opts.MinRunesPerToken) || math.IsInf(opts.MinRunesPerToken, 0) || opts.MinRunesPerToken < 0 {
		return 0, 0, 0, false, fmt.Errorf("embedding: MinRunesPerToken must be finite and non-negative, got %v", opts.MinRunesPerToken)
	}
	minRunesPerToken = opts.MinRunesPerToken
	if minRunesPerToken == 0 {
		minRunesPerToken = defaultMinRunesPerToken
	}

	if emb != nil {
		if err := ValidateEmbedder(emb); err != nil {
			return 0, 0, 0, false, err
		}
	}

	maxTokens = opts.MaxContextTokens
	if maxTokens == 0 && emb != nil {
		if reporter, ok := emb.(ContextWindowReporter); ok {
			maxTokens = reporter.MaxContextTokens()
		}
	}
	if maxTokens <= 0 {
		if opts.RequireKnownLimit {
			return 0, minRunesPerToken, 0, false, ErrContextWindowUnknown
		}
		return 0, minRunesPerToken, 0, false, nil
	}

	rawBudget := math.Floor(float64(maxTokens) * minRunesPerToken)
	if math.IsInf(rawBudget, 0) || math.IsNaN(rawBudget) || rawBudget >= float64(math.MaxInt) {
		maxSafeRunes = math.MaxInt - opts.HeaderReserveRunes
	} else {
		maxSafeRunes = int(rawBudget) - opts.HeaderReserveRunes
	}
	return maxTokens, minRunesPerToken, maxSafeRunes, true, nil
}

// ValidateChunkSizeForEmbedder checks that a configured chunk size (in runes)
// fits within the conservative rune budget derived from emb's context window
// (or opts.MaxContextTokens). It fails closed on non-positive chunkSizeRunes,
// invalid options, non-positive effective budget, or chunk sizes that exceed
// the safe budget.
func ValidateChunkSizeForEmbedder(chunkSizeRunes int, emb Embedder, opts ContextBudgetOptions) error {
	if chunkSizeRunes <= 0 {
		return fmt.Errorf("embedding: chunkSizeRunes must be positive, got %d", chunkSizeRunes)
	}
	maxTokens, minRunesPerToken, maxSafeRunes, hasLimit, err := resolveContextBudget(emb, opts)
	if err != nil {
		return err
	}
	if !hasLimit {
		return nil
	}
	if maxSafeRunes <= 0 || chunkSizeRunes > maxSafeRunes {
		return fmt.Errorf(
			"%w: chunk size %d runes exceeds safe budget %d runes (max_context_tokens=%d, min_runes_per_token=%g, header_reserve_runes=%d)",
			ErrContextWindowExceeded, chunkSizeRunes, maxSafeRunes, maxTokens, minRunesPerToken, opts.HeaderReserveRunes,
		)
	}
	return nil
}

// ValidateTextsForContextWindow validates each UTF-8 text entry against the
// conservative rune budget derived from emb's context window (or
// opts.MaxContextTokens). It performs zero heap allocations on the happy path.
//
// Callers passing texts that already include contextual headers should set
// opts.HeaderReserveRunes to 0 to avoid subtracting header overhead twice.
func ValidateTextsForContextWindow(texts [][]byte, emb Embedder, opts ContextBudgetOptions) error {
	maxTokens, minRunesPerToken, maxSafeRunes, hasLimit, err := resolveContextBudget(emb, opts)
	if err != nil {
		return err
	}
	if !hasLimit {
		return nil
	}
	if maxSafeRunes <= 0 {
		return fmt.Errorf(
			"%w: safe budget %d runes is non-positive (max_context_tokens=%d, min_runes_per_token=%g, header_reserve_runes=%d)",
			ErrContextWindowExceeded, maxSafeRunes, maxTokens, minRunesPerToken, opts.HeaderReserveRunes,
		)
	}
	for i, text := range texts {
		if n := utf8.RuneCount(text); n > maxSafeRunes {
			return fmt.Errorf(
				"%w: text[%d] has %d runes, exceeds safe budget %d runes (max_context_tokens=%d, min_runes_per_token=%g, header_reserve_runes=%d)",
				ErrContextWindowExceeded, i, n, maxSafeRunes, maxTokens, minRunesPerToken, opts.HeaderReserveRunes,
			)
		}
	}
	return nil
}
