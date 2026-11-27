package embedding

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"
)

type contextWindowStubEmbedder struct {
	dims             int
	maxContextTokens int
}

func (s contextWindowStubEmbedder) Dimensions() int       { return s.dims }
func (s contextWindowStubEmbedder) MaxContextTokens() int { return s.maxContextTokens }
func (s contextWindowStubEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	return stubEmbedder{dims: s.dims}.EmbedBatch(ctx, texts)
}

var _ ContextWindowReporter = contextWindowStubEmbedder{}

func TestValidateChunkSizeForEmbedder(t *testing.T) {
	emb512 := contextWindowStubEmbedder{dims: 768, maxContextTokens: 512}
	emb8192 := contextWindowStubEmbedder{dims: 1536, maxContextTokens: 8192}
	embUnknown := stubEmbedder{dims: 64}
	embZeroLimit := contextWindowStubEmbedder{dims: 64, maxContextTokens: 0}

	// 512 tokens * default 2.5 runes/token = 1280 max safe runes.
	if err := ValidateChunkSizeForEmbedder(3000, emb512, ContextBudgetOptions{}); !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("3000 runes on 512-token embedder: err=%v want ErrContextWindowExceeded", err)
	}
	if err := ValidateChunkSizeForEmbedder(1281, emb512, ContextBudgetOptions{}); !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("1281 runes on 512-token embedder: err=%v want ErrContextWindowExceeded", err)
	}
	if err := ValidateChunkSizeForEmbedder(1280, emb512, ContextBudgetOptions{}); err != nil {
		t.Fatalf("1280 runes on 512-token embedder: unexpected err=%v", err)
	}

	// 8192 tokens * default 2.5 runes/token = 20480 max safe runes.
	if err := ValidateChunkSizeForEmbedder(3000, emb8192, ContextBudgetOptions{}); err != nil {
		t.Fatalf("3000 runes on 8192-token embedder: unexpected err=%v", err)
	}

	// HeaderReserveRunes reduces max safe runes.
	if err := ValidateChunkSizeForEmbedder(1080, emb512, ContextBudgetOptions{HeaderReserveRunes: 200}); err != nil {
		t.Fatalf("1080 runes with 200 reserve: unexpected err=%v", err)
	}
	if err := ValidateChunkSizeForEmbedder(1081, emb512, ContextBudgetOptions{HeaderReserveRunes: 200}); !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("1081 runes with 200 reserve: err=%v want ErrContextWindowExceeded", err)
	}

	// Custom MinRunesPerToken (e.g. prose 4.0 -> 512*4 = 2048).
	if err := ValidateChunkSizeForEmbedder(2048, emb512, ContextBudgetOptions{MinRunesPerToken: 4.0}); err != nil {
		t.Fatalf("2048 runes at 4.0 runes/token: unexpected err=%v", err)
	}
	if err := ValidateChunkSizeForEmbedder(2049, emb512, ContextBudgetOptions{MinRunesPerToken: 4.0}); !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("2049 runes at 4.0 runes/token: err=%v want ErrContextWindowExceeded", err)
	}

	// Explicit opts.MaxContextTokens overrides embedder reporter.
	if err := ValidateChunkSizeForEmbedder(2000, emb512, ContextBudgetOptions{MaxContextTokens: 1024}); err != nil {
		t.Fatalf("explicit MaxContextTokens override failed: %v", err)
	}
	if err := ValidateChunkSizeForEmbedder(2000, emb8192, ContextBudgetOptions{MaxContextTokens: 512}); !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("explicit lower MaxContextTokens override: err=%v want ErrContextWindowExceeded", err)
	}

	// Unknown limit behavior.
	for _, emb := range []Embedder{nil, embUnknown, embZeroLimit} {
		if err := ValidateChunkSizeForEmbedder(3000, emb, ContextBudgetOptions{RequireKnownLimit: false}); err != nil {
			t.Fatalf("unknown limit with RequireKnownLimit=false: unexpected err=%v", err)
		}
		if err := ValidateChunkSizeForEmbedder(3000, emb, ContextBudgetOptions{RequireKnownLimit: true}); !errors.Is(err, ErrContextWindowUnknown) {
			t.Fatalf("unknown limit with RequireKnownLimit=true: err=%v want ErrContextWindowUnknown", err)
		}
	}

	// Fail-closed invalid chunk sizes and options.
	invalidCases := []struct {
		name      string
		chunkSize int
		emb       Embedder
		opts      ContextBudgetOptions
		wantErr   error
	}{
		{name: "zero chunk size", chunkSize: 0, emb: emb512},
		{name: "negative chunk size", chunkSize: -1, emb: emb512},
		{name: "negative MaxContextTokens", chunkSize: 100, emb: emb512, opts: ContextBudgetOptions{MaxContextTokens: -1}},
		{name: "negative HeaderReserveRunes", chunkSize: 100, emb: emb512, opts: ContextBudgetOptions{HeaderReserveRunes: -1}},
		{name: "negative MinRunesPerToken", chunkSize: 100, emb: emb512, opts: ContextBudgetOptions{MinRunesPerToken: -0.5}},
		{name: "NaN MinRunesPerToken", chunkSize: 100, emb: emb512, opts: ContextBudgetOptions{MinRunesPerToken: math.NaN()}},
		{name: "+Inf MinRunesPerToken", chunkSize: 100, emb: emb512, opts: ContextBudgetOptions{MinRunesPerToken: math.Inf(1)}},
		{name: "-Inf MinRunesPerToken", chunkSize: 100, emb: emb512, opts: ContextBudgetOptions{MinRunesPerToken: math.Inf(-1)}},
		{
			name:      "header reserve exhausts budget (maxSafeRunes == 0)",
			chunkSize: 1,
			emb:       emb512,
			opts:      ContextBudgetOptions{HeaderReserveRunes: 1280},
			wantErr:   ErrContextWindowExceeded,
		},
		{
			name:      "header reserve exceeds budget (maxSafeRunes < 0)",
			chunkSize: 1,
			emb:       emb512,
			opts:      ContextBudgetOptions{HeaderReserveRunes: 2000},
			wantErr:   ErrContextWindowExceeded,
		},
		{
			name:      "tiny MinRunesPerToken floors to 0",
			chunkSize: 1,
			emb:       contextWindowStubEmbedder{dims: 8, maxContextTokens: 1},
			opts:      ContextBudgetOptions{MinRunesPerToken: 0.5},
			wantErr:   ErrContextWindowExceeded,
		},
	}
	for _, tc := range invalidCases {
		err := ValidateChunkSizeForEmbedder(tc.chunkSize, tc.emb, tc.opts)
		if err == nil {
			t.Fatalf("%s: expected fail-closed error, got nil", tc.name)
		}
		if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s: err=%v want %v", tc.name, err, tc.wantErr)
		}
	}

	// Huge context window does not overflow int to negative.
	if err := ValidateChunkSizeForEmbedder(1000, nil, ContextBudgetOptions{MaxContextTokens: math.MaxInt, MinRunesPerToken: 10}); err != nil {
		t.Fatalf("large MaxContextTokens overflowed: %v", err)
	}
}

func TestValidateTextsForContextWindow(t *testing.T) {
	emb512 := contextWindowStubEmbedder{dims: 768, maxContextTokens: 512}
	embUnknown := stubEmbedder{dims: 64}

	// 512 * 2.5 = 1280 runes.
	okBatch := [][]byte{
		[]byte(strings.Repeat("a", 1280)),
		[]byte(strings.Repeat("界", 1280)), // 3840 bytes, 1280 runes — counts runes, not bytes!
		[]byte(strings.Repeat("🚀", 100)),  // 400 bytes, 100 runes
	}
	if err := ValidateTextsForContextWindow(okBatch, emb512, ContextBudgetOptions{}); err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}

	// Entry 2 exceeds budget (1281 runes).
	failBatch := [][]byte{
		[]byte("short ok"),
		[]byte(strings.Repeat("b", 1280)),
		[]byte(strings.Repeat("c", 1281)),
		[]byte(strings.Repeat("d", 5000)),
	}
	err := ValidateTextsForContextWindow(failBatch, emb512, ContextBudgetOptions{})
	if !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("oversized batch err=%v want ErrContextWindowExceeded", err)
	}
	if !strings.Contains(err.Error(), "2") || !strings.Contains(err.Error(), "1281") {
		t.Fatalf("error %q should include failing index 2 and rune count 1281", err.Error())
	}

	// Unknown limit behavior.
	if err := ValidateTextsForContextWindow(failBatch, embUnknown, ContextBudgetOptions{RequireKnownLimit: false}); err != nil {
		t.Fatalf("unknown limit with RequireKnownLimit=false: unexpected err=%v", err)
	}
	if err := ValidateTextsForContextWindow(failBatch, embUnknown, ContextBudgetOptions{RequireKnownLimit: true}); !errors.Is(err, ErrContextWindowUnknown) {
		t.Fatalf("unknown limit with RequireKnownLimit=true: err=%v want ErrContextWindowUnknown", err)
	}

	// Invalid options fail closed even on empty slice.
	for _, opts := range []ContextBudgetOptions{
		{MaxContextTokens: -1},
		{HeaderReserveRunes: -1},
		{MinRunesPerToken: -1},
		{MinRunesPerToken: math.NaN()},
		{MinRunesPerToken: math.Inf(1)},
	} {
		if err := ValidateTextsForContextWindow(nil, emb512, opts); err == nil {
			t.Fatalf("invalid opts %+v accepted on empty texts", opts)
		}
	}

	// maxSafeRunes <= 0 fails closed with ErrContextWindowExceeded even on empty texts.
	if err := ValidateTextsForContextWindow(nil, emb512, ContextBudgetOptions{HeaderReserveRunes: 1280}); !errors.Is(err, ErrContextWindowExceeded) {
		t.Fatalf("maxSafeRunes <= 0 on empty texts: err=%v want ErrContextWindowExceeded", err)
	}
}

func TestValidateTextsForContextWindowZeroAllocHappyPath(t *testing.T) {
	var emb Embedder = &contextWindowStubEmbedder{dims: 768, maxContextTokens: 512}
	texts := make([][]byte, 32)
	for i := range texts {
		texts[i] = bytes.Repeat([]byte("hello world "), 50) // 600 runes < 1280
	}
	opts := ContextBudgetOptions{}
	allocs := testing.AllocsPerRun(100, func() {
		if err := ValidateTextsForContextWindow(texts, emb, opts); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ValidateTextsForContextWindow happy path allocated %.1f allocs/run, want 0", allocs)
	}
	allocsOverride := testing.AllocsPerRun(100, func() {
		if err := ValidateTextsForContextWindow(texts, nil, ContextBudgetOptions{MaxContextTokens: 512}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if allocsOverride != 0 {
		t.Fatalf("ValidateTextsForContextWindow explicit-limit happy path allocated %.1f allocs/run, want 0", allocsOverride)
	}
}

func BenchmarkValidateTextsForContextWindow(b *testing.B) {
	var emb Embedder = &contextWindowStubEmbedder{dims: 768, maxContextTokens: 512}
	texts := make([][]byte, 64)
	for i := range texts {
		texts[i] = bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 20)
	}
	opts := ContextBudgetOptions{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ValidateTextsForContextWindow(texts, emb, opts); err != nil {
			b.Fatalf("ValidateTextsForContextWindow: %v", err)
		}
	}
}
