package embedding

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
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

type cancelOnCloseBody struct {
	body   []byte
	offset int
	cancel context.CancelFunc
}

func (c *cancelOnCloseBody) Read(p []byte) (int, error) {
	if c.offset >= len(c.body) {
		return 0, io.EOF
	}
	n := copy(p, c.body[c.offset:])
	c.offset += n
	return n, nil
}

func (c *cancelOnCloseBody) Close() error {
	c.cancel()
	return nil
}

type cancelOnCloseTransport struct {
	payload []byte
	cancel  context.CancelFunc
}

func (t cancelOnCloseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       &cancelOnCloseBody{body: t.payload, cancel: t.cancel},
		Request:    req,
	}, nil
}

func TestOpenAICompatRejectsNullComponentsAndLateCancel(t *testing.T) {
	t.Run("null_component_fails_closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[null,1]}]}`))
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
		if _, err := emb.EmbedBatch(context.Background(), [][]byte{[]byte("x")}); !errors.Is(err, ErrInvalidOutput) {
			t.Fatalf("null component err=%v want ErrInvalidOutput", err)
		}
	})

	t.Run("missing_index_fails_closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"embedding":[1,2]}]}`))
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
		if _, err := emb.EmbedBatch(context.Background(), [][]byte{[]byte("x")}); !errors.Is(err, ErrInvalidOutput) {
			t.Fatalf("missing index err=%v want ErrInvalidOutput", err)
		}
	})

	t.Run("cancel_after_response_body_close_fails_batch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rawEmb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": "http://127.0.0.1:1/v1", "model": "m"},
		})
		if err != nil {
			t.Fatal(err)
		}
		emb := rawEmb.(*OpenAICompatEmbedder)
		emb.client = &http.Client{
			Transport: cancelOnCloseTransport{
				payload: []byte(`{"data":[{"index":0,"embedding":[1,2]}]}`),
				cancel:  cancel,
			},
		}
		vecs, err := emb.EmbedBatch(ctx, [][]byte{[]byte("x")})
		if !errors.Is(err, context.Canceled) || vecs != nil {
			t.Fatalf("late cancel got vecs=%v err=%v want nil, context.Canceled", vecs, err)
		}
	})
}

func encodeFloat32sBase64(vals []float32) string {
	buf := make([]byte, len(vals)*4)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func TestOpenAICompatEmbedderBase64Encoding(t *testing.T) {
	t.Run("wire_parity_and_base64_decode", func(t *testing.T) {
		var sawEncodingFormat atomic.Value
		var sawHasEncodingField atomic.Bool

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rawBody, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var rawMap map[string]json.RawMessage
			if err := json.Unmarshal(rawBody, &rawMap); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if rawFmt, ok := rawMap["encoding_format"]; ok {
				sawHasEncodingField.Store(true)
				var s string
				_ = json.Unmarshal(rawFmt, &s)
				sawEncodingFormat.Store(s)
			} else {
				sawHasEncodingField.Store(false)
				sawEncodingFormat.Store("")
			}
			var req struct {
				Input []string `json:"input"`
			}
			_ = json.Unmarshal(rawBody, &req)
			if gotFmt, _ := sawEncodingFormat.Load().(string); gotFmt == "base64" {
				type datum struct {
					Index     int    `json:"index"`
					Embedding string `json:"embedding"`
				}
				data := make([]datum, len(req.Input))
				for i := range req.Input {
					rev := len(req.Input) - 1 - i
					vec := []float32{float32(len(req.Input[rev])), -3.5, math.Float32frombits(1 << 31), 4.25}
					data[i] = datum{Index: rev, Embedding: encodeFloat32sBase64(vec)}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
				return
			}
			type datum struct {
				Index     int       `json:"index"`
				Embedding []float32 `json:"embedding"`
			}
			data := make([]datum, len(req.Input))
			for i := range req.Input {
				data[i] = datum{Index: i, Embedding: []float32{1, 2, 3, 4}}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
		}))
		defer srv.Close()

		// 1. Default and explicit "float" must omit "encoding_format" on the wire.
		for _, optVal := range []string{"", "float"} {
			opts := map[string]string{"base_url": srv.URL, "model": "m"}
			if optVal != "" {
				opts["encoding_format"] = optVal
			}
			emb, err := NewOpenAICompatEmbedder(Config{
				Provider:   ProviderOpenAICompat,
				Dimensions: 4,
				Options:    opts,
			})
			if err != nil {
				t.Fatalf("NewOpenAICompatEmbedder(%q): %v", optVal, err)
			}
			vecs, err := emb.EmbedBatch(context.Background(), [][]byte{[]byte("a")})
			if err != nil {
				t.Fatalf("EmbedBatch(%q): %v", optVal, err)
			}
			if sawHasEncodingField.Load() {
				t.Fatalf("encoding_format=%q unexpectedly sent encoding_format on wire", optVal)
			}
			if len(vecs) != 1 || vecs[0][0] != 1 || vecs[0][3] != 4 {
				t.Fatalf("unexpected float vecs: %v", vecs)
			}
		}

		// 2. "base64" sends "encoding_format":"base64" and decodes LE IEEE-754 float32s.
		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 4,
			Options: map[string]string{
				"base_url":        srv.URL,
				"model":           "m",
				"batch_size":      "2",
				"encoding_format": "base64",
			},
		})
		if err != nil {
			t.Fatalf("NewOpenAICompatEmbedder(base64): %v", err)
		}
		inputs := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
		vecs, err := emb.EmbedBatch(context.Background(), inputs)
		if err != nil {
			t.Fatalf("EmbedBatch(base64): %v", err)
		}
		if !sawHasEncodingField.Load() {
			t.Fatal("base64 mode did not send encoding_format on wire")
		}
		if got, _ := sawEncodingFormat.Load().(string); got != "base64" {
			t.Fatalf("wire encoding_format=%q want base64", got)
		}
		if len(vecs) != len(inputs) {
			t.Fatalf("len(vecs)=%d want %d", len(vecs), len(inputs))
		}
		for i, v := range vecs {
			if v[0] != float32(len(inputs[i])) || v[1] != -3.5 || math.Float32bits(v[2]) != (1<<31) || v[3] != 4.25 {
				t.Fatalf("vec[%d]=%v (bits[2]=%#x) mismatch", i, v, math.Float32bits(v[2]))
			}
		}
	})

	t.Run("base64_normalize_and_json_escaped_string", func(t *testing.T) {
		// Encode [3, 4] and inject a JSON-escaped slash if present or unicode escape \u0041.
		rawB64 := encodeFloat32sBase64([]float32{3, 4})
		// Replace first byte with \u00XX JSON escape to exercise JSON string unescaping.
		escapedB64 := `\u00` + string("0123456789abcdef"[rawB64[0]>>4]) + string("0123456789abcdef"[rawB64[0]&0xf]) + rawB64[1:]
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":"` + escapedB64 + `"}]}`))
		}))
		defer srv.Close()

		emb, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options: map[string]string{
				"base_url":        srv.URL,
				"model":           "m",
				"encoding_format": "base64",
				"normalize":       "true",
			},
		})
		if err != nil {
			t.Fatalf("NewOpenAICompatEmbedder: %v", err)
		}
		vecs, err := emb.EmbedBatch(context.Background(), [][]byte{[]byte("norm")})
		if err != nil {
			t.Fatalf("EmbedBatch: %v", err)
		}
		if math.Abs(float64(vecs[0][0])-0.6) > 1e-6 || math.Abs(float64(vecs[0][1])-0.8) > 1e-6 {
			t.Fatalf("normalized vec=%v want [0.6 0.8]", vecs[0])
		}
	})

	t.Run("strict_bidirectional_mismatch_and_error_split", func(t *testing.T) {
		nanB64 := encodeFloat32sBase64([]float32{float32(math.NaN()), 1})
		posInfB64 := encodeFloat32sBase64([]float32{float32(math.Inf(1)), 1})
		negInfB64 := encodeFloat32sBase64([]float32{float32(math.Inf(-1)), 1})
		wrongDimsB64 := encodeFloat32sBase64([]float32{1, 2, 3})
		zeroB64 := encodeFloat32sBase64([]float32{0, 0})
		notDiv4B64 := base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4, 5})
		valid2B64 := encodeFloat32sBase64([]float32{1, 2})

		cases := []struct {
			name           string
			encodingFormat string
			normalize      string
			body           string
			wantErr        error
		}{
			{
				name:           "float_rejects_base64_string",
				encodingFormat: "float",
				body:           `{"data":[{"index":0,"embedding":"` + valid2B64 + `"}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_rejects_float_array",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":[1,2]}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_rejects_null",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":null}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_invalid_encoding",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":"%%%not-base64%%%"}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_len_not_divisible_by_4",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":"` + notDiv4B64 + `"}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_nan",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":"` + nanB64 + `"}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_pos_inf",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":"` + posInfB64 + `"}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_neg_inf",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":"` + negInfB64 + `"}]}`,
				wantErr:        ErrInvalidOutput,
			},
			{
				name:           "base64_dimension_mismatch",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":"` + wrongDimsB64 + `"}]}`,
				wantErr:        ErrDimensionMismatch,
			},
			{
				name:           "base64_empty_string_dimension_mismatch",
				encodingFormat: "base64",
				body:           `{"data":[{"index":0,"embedding":""}]}`,
				wantErr:        ErrDimensionMismatch,
			},
			{
				name:           "base64_zero_vector_normalize",
				encodingFormat: "base64",
				normalize:      "true",
				body:           `{"data":[{"index":0,"embedding":"` + zeroB64 + `"}]}`,
				wantErr:        ErrInvalidOutput,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()

				opts := map[string]string{
					"base_url":        srv.URL,
					"model":           "m",
					"encoding_format": tc.encodingFormat,
				}
				if tc.normalize != "" {
					opts["normalize"] = tc.normalize
				}
				emb, err := NewOpenAICompatEmbedder(Config{
					Provider:   ProviderOpenAICompat,
					Dimensions: 2,
					Options:    opts,
				})
				if err != nil {
					t.Fatalf("NewOpenAICompatEmbedder: %v", err)
				}
				_, err = emb.EmbedBatch(context.Background(), [][]byte{[]byte("x")})
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("got err=%v want %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("option_validation_and_max_context_tokens", func(t *testing.T) {
		type contextWindowReporter interface {
			MaxContextTokens() int
		}
		for _, badFmt := range []string{"", "int8", "BASE64", "binary"} {
			if _, err := NewOpenAICompatEmbedder(Config{
				Provider:   ProviderOpenAICompat,
				Dimensions: 2,
				Options:    map[string]string{"base_url": "http://127.0.0.1:1/v1", "model": "m", "encoding_format": badFmt},
			}); err == nil {
				t.Fatalf("expected invalid encoding_format %q to fail closed", badFmt)
			}
		}
		for _, badCtx := range []string{"", "0", "-10", "abc"} {
			if _, err := NewOpenAICompatEmbedder(Config{
				Provider:   ProviderOpenAICompat,
				Dimensions: 2,
				Options:    map[string]string{"base_url": "http://127.0.0.1:1/v1", "model": "m", "max_context_tokens": badCtx},
			}); err == nil {
				t.Fatalf("expected invalid max_context_tokens %q to fail closed", badCtx)
			}
		}

		embUnset, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": "http://127.0.0.1:1/v1", "model": "m"},
		})
		if err != nil {
			t.Fatalf("unset max_context_tokens: %v", err)
		}
		repUnset, ok := embUnset.(contextWindowReporter)
		if !ok {
			t.Fatalf("OpenAICompatEmbedder does not implement MaxContextTokens() int")
		}
		if got := repUnset.MaxContextTokens(); got != 0 {
			t.Fatalf("unset MaxContextTokens()=%d want 0", got)
		}

		embSet, err := NewOpenAICompatEmbedder(Config{
			Provider:   ProviderOpenAICompat,
			Dimensions: 2,
			Options:    map[string]string{"base_url": "http://127.0.0.1:1/v1", "model": "m", "max_context_tokens": "8192"},
		})
		if err != nil {
			t.Fatalf("set max_context_tokens: %v", err)
		}
		repSet, ok := embSet.(contextWindowReporter)
		if !ok {
			t.Fatalf("OpenAICompatEmbedder does not implement MaxContextTokens() int")
		}
		if got := repSet.MaxContextTokens(); got != 8192 {
			t.Fatalf("set MaxContextTokens()=%d want 8192", got)
		}
	})
}

func TestOpenAICompatConcurrentEmbedBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input          []string `json:"input"`
			EncodingFormat string   `json:"encoding_format"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if req.EncodingFormat == "base64" {
			type datum struct {
				Index     int    `json:"index"`
				Embedding string `json:"embedding"`
			}
			data := make([]datum, len(req.Input))
			for i, s := range req.Input {
				data[i] = datum{Index: i, Embedding: encodeFloat32sBase64([]float32{float32(len(s)), 1, 2, 3})}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
			return
		}
		type datum struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		data := make([]datum, len(req.Input))
		for i, s := range req.Input {
			data[i] = datum{Index: i, Embedding: []float32{float32(len(s)), 1, 2, 3}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer srv.Close()

	for _, format := range []string{"float", "base64"} {
		t.Run(format, func(t *testing.T) {
			emb, err := NewOpenAICompatEmbedder(Config{
				Provider:   ProviderOpenAICompat,
				Dimensions: 4,
				Options: map[string]string{
					"base_url":        srv.URL,
					"model":           "m",
					"batch_size":      "4",
					"encoding_format": format,
				},
			})
			if err != nil {
				t.Fatalf("NewOpenAICompatEmbedder(%s): %v", format, err)
			}
			const workers = 8
			const iters = 10
			var wg sync.WaitGroup
			errCh := make(chan error, workers*iters)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					inputs := make([][]byte, 6)
					for i := range inputs {
						inputs[i] = bytes.Repeat([]byte("a"), workerID+i+1)
					}
					for iter := 0; iter < iters; iter++ {
						vecs, err := emb.EmbedBatch(context.Background(), inputs)
						if err != nil {
							errCh <- err
							return
						}
						if len(vecs) != len(inputs) {
							errCh <- errors.New("unexpected vector count")
							return
						}
						for i, v := range vecs {
							if v[0] != float32(len(inputs[i])) {
								errCh <- errors.New("misaligned vector content")
								return
							}
						}
					}
				}(w)
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Fatalf("concurrent EmbedBatch error: %v", err)
			}
		})
	}
}

type staticPayloadTransport struct {
	payload []byte
}

func (t staticPayloadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(t.payload)),
		Request:    req,
	}, nil
}

func BenchmarkOpenAICompatDecode(b *testing.B) {
	const (
		batchSize = 64
		dims      = 768
	)
	vec := make([]float32, dims)
	for d := range dims {
		vec[d] = float32(((d*37)%199)-99) / 100.0
	}
	vec[0] = 1.0

	type floatDatum struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	}
	floatData := make([]floatDatum, batchSize)
	for i := range batchSize {
		floatData[i] = floatDatum{Index: i, Embedding: vec}
	}
	floatPayload, err := json.Marshal(map[string]any{"data": floatData})
	if err != nil {
		b.Fatalf("marshal float payload: %v", err)
	}

	type b64Datum struct {
		Index     int    `json:"index"`
		Embedding string `json:"embedding"`
	}
	b64Vec := encodeFloat32sBase64(vec)
	b64Data := make([]b64Datum, batchSize)
	for i := range batchSize {
		b64Data[i] = b64Datum{Index: i, Embedding: b64Vec}
	}
	b64Payload, err := json.Marshal(map[string]any{"data": b64Data})
	if err != nil {
		b.Fatalf("marshal base64 payload: %v", err)
	}

	inputs := make([][]byte, batchSize)
	for i := range batchSize {
		inputs[i] = []byte("benchmark chunk text for openai_compat decode")
	}

	for _, tc := range []struct {
		name    string
		format  string
		payload []byte
	}{
		{name: "float", format: "float", payload: floatPayload},
		{name: "base64", format: "base64", payload: b64Payload},
	} {
		b.Run(tc.name, func(b *testing.B) {
			opts := map[string]string{
				"base_url":   "http://127.0.0.1:1/v1",
				"model":      "bench-model",
				"batch_size": "64",
			}
			if tc.format == "base64" {
				opts["encoding_format"] = "base64"
			}
			rawEmb, err := NewOpenAICompatEmbedder(Config{
				Provider:   ProviderOpenAICompat,
				Dimensions: dims,
				Options:    opts,
			})
			if err != nil {
				b.Fatal(err)
			}
			emb := rawEmb.(*OpenAICompatEmbedder)
			emb.client = &http.Client{Transport: staticPayloadTransport{payload: tc.payload}}
			ctx := context.Background()
			b.SetBytes(int64(len(tc.payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := emb.EmbedBatch(ctx, inputs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
