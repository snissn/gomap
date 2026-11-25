package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenAICompatRegisteredAndBatching(t *testing.T) {
	var calls atomic.Int32
	var sawAuth atomic.Value
	var sawDims atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/embeddings" {
			http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		sawAuth.Store(r.Header.Get("Authorization"))
		var req struct {
			Model      string   `json:"model"`
			Input      []string `json:"input"`
			Dimensions int      `json:"dimensions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Model != "test-embed-v1" {
			http.Error(w, "bad model", http.StatusBadRequest)
			return
		}
		sawDims.Store(int32(req.Dimensions))

		// Return items in reverse order to verify index-aligned reassembly.
		type datum struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		data := make([]datum, len(req.Input))
		for i := range req.Input {
			rev := len(req.Input) - 1 - i
			data[i] = datum{
				Index:     rev,
				Embedding: []float32{float32(len(req.Input[rev])), 3, 4},
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer srv.Close()

	t.Setenv("TEST_TREEDB_EMBED_KEY", "secret-token-123")
	cfg := Config{
		Provider:   ProviderOpenAICompat,
		Dimensions: 3,
		Options: map[string]string{
			"base_url":        srv.URL + "/v1",
			"model":           "test-embed-v1",
			"api_key_env":     "TEST_TREEDB_EMBED_KEY",
			"batch_size":      "2",
			"normalize":       "true",
			"send_dimensions": "true",
		},
	}

	emb, err := DefaultRegistry().Create(cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := ValidateEmbedder(emb); err != nil {
		t.Fatalf("ValidateEmbedder: %v", err)
	}
	if emb.Dimensions() != 3 {
		t.Fatalf("Dimensions()=%d want 3", emb.Dimensions())
	}

	inputs := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("dddd"), []byte("eeeee")}
	vecs, err := emb.EmbedBatch(context.Background(), inputs)
	if err != nil {
		t.Fatalf("EmbedBatch: %v", err)
	}
	if len(vecs) != len(inputs) {
		t.Fatalf("len(vecs)=%d want %d", len(vecs), len(inputs))
	}
	if calls.Load() != 3 {
		t.Fatalf("HTTP calls=%d want 3 (batch_size=2 for 5 inputs)", calls.Load())
	}
	if gotAuth, _ := sawAuth.Load().(string); gotAuth != "Bearer secret-token-123" {
		t.Fatalf("Authorization=%q want Bearer secret-token-123", gotAuth)
	}
	if sawDims.Load() != 3 {
		t.Fatalf("sent dimensions=%d want 3", sawDims.Load())
	}
	for i, v := range vecs {
		if len(v) != 3 {
			t.Fatalf("vec[%d] len=%d want 3", i, len(v))
		}
		var normSq float64
		for _, x := range v {
			normSq += float64(x) * float64(x)
		}
		if math.Abs(math.Sqrt(normSq)-1.0) > 1e-5 {
			t.Fatalf("vec[%d] not unit-normalized: norm=%v vec=%v", i, math.Sqrt(normSq), v)
		}
		wantX := float64(len(inputs[i])) / math.Sqrt(float64(len(inputs[i])*len(inputs[i]))+25.0)
		if math.Abs(float64(v[0])-wantX) > 1e-5 {
			t.Fatalf("vec[%d][0]=%v want %v (misaligned index?)", i, v[0], wantX)
		}
	}
}

func TestOpenAICompatRetriesAndErrors(t *testing.T) {
	t.Run("retries_429_then_succeeds", func(t *testing.T) {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) <= 2 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limited"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,0]}]}`))
		}))
		defer srv.Close()

		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options: map[string]string{
				"base_url":    srv.URL + "/v1/embeddings",
				"model":       "m",
				"max_retries": "2",
			},
		})
		if err != nil {
			t.Fatalf("NewOpenAICompatEmbedder: %v", err)
		}
		vecs, err := emb.EmbedBatch(context.Background(), [][]byte{[]byte("hi")})
		if err != nil || len(vecs) != 1 || vecs[0][0] != 1 {
			t.Fatalf("got vecs=%v err=%v", vecs, err)
		}
		if attempts.Load() != 3 {
			t.Fatalf("attempts=%d want 3", attempts.Load())
		}
	})

	t.Run("dimension_mismatch_fails_closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,2,3]}]}`))
		}))
		defer srv.Close()
		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": srv.URL, "model": "m"},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = emb.EmbedBatch(context.Background(), [][]byte{[]byte("x")})
		if !errors.Is(err, ErrDimensionMismatch) {
			t.Fatalf("err=%v want ErrDimensionMismatch", err)
		}
	})

	t.Run("zero_vector_normalize_fails_closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[0,0]}]}`))
		}))
		defer srv.Close()
		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": srv.URL, "model": "m", "normalize": "true"},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = emb.EmbedBatch(context.Background(), [][]byte{[]byte("x")})
		if !errors.Is(err, ErrInvalidOutput) {
			t.Fatalf("err=%v want ErrInvalidOutput", err)
		}
	})

	t.Run("empty_batch_and_validation_errors", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("server must not be called for empty batch")
		}))
		defer srv.Close()
		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": srv.URL, "model": "m"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := emb.EmbedBatch(context.Background(), nil); !errors.Is(err, ErrEmptyBatch) {
			t.Fatalf("empty batch err=%v want ErrEmptyBatch", err)
		}
		if _, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": srv.URL, "model": "m", "unknown_opt": "1"},
		}); err == nil {
			t.Fatal("expected unknown option to fail closed")
		}
		if _, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": "ftp://example.com", "model": "m"},
		}); err == nil {
			t.Fatal("expected non-http scheme to fail closed")
		}
		if _, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": srv.URL, "model": "m", "api_key_env": "UNSET_TREEDB_TEST_KEY_999"},
		}); err == nil {
			t.Fatal("expected unset api_key_env to fail closed")
		}
	})

	t.Run("context_cancellation_no_goroutine_leak", func(t *testing.T) {
		before := runtime.NumGoroutine()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(250 * time.Millisecond):
			}
		}))
		defer srv.Close()

		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": srv.URL, "model": "m", "max_retries": "0"},
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if _, err := emb.EmbedBatch(ctx, [][]byte{[]byte("slow")}); err == nil {
			t.Fatal("expected cancellation error")
		}
		time.Sleep(50 * time.Millisecond)
		if after := runtime.NumGoroutine(); after > before+6 {
			t.Fatalf("goroutine leak: before=%d after=%d", before, after)
		}
	})
}

func BenchmarkOpenAICompatEmbedBatch(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		type datum struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		vec := make([]float32, 64)
		vec[0] = 1
		data := make([]datum, len(req.Input))
		for i := range req.Input {
			data[i] = datum{Index: i, Embedding: vec}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer srv.Close()

	emb, err := NewOpenAICompatEmbedder(Config{
		Provider:   ProviderOpenAICompat,
		Dimensions: 64,
		Options:    map[string]string{"base_url": srv.URL, "model": "bench-model", "batch_size": "32"},
	})
	if err != nil {
		b.Fatal(err)
	}
	inputs := make([][]byte, 32)
	for i := range inputs {
		inputs[i] = []byte("treedb text search and hybrid vector embedding benchmark document")
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := emb.EmbedBatch(ctx, inputs); err != nil {
			b.Fatal(err)
		}
	}
}
