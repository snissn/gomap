package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ProviderOpenAICompat is the registry name for OpenAI-compatible HTTP
// /v1/embeddings endpoints (including OpenAI, Ollama /v1/embeddings, vLLM,
// llama.cpp server, HuggingFace TEI, and compatible gateways).
const ProviderOpenAICompat = "openai_compat"

const (
	defaultOpenAICompatBatchSize        = 64
	maxOpenAICompatBatchSize            = 2048
	defaultOpenAICompatTimeoutMs        = 30000
	defaultOpenAICompatMaxRetries       = 2
	maxOpenAICompatRetries              = 10
	maxOpenAICompatResponseBytes  int64 = 64 << 20
	openAICompatBaseBackoff             = 50 * time.Millisecond
	openAICompatMaxBackoff              = 2 * time.Second
)

// OpenAICompatEmbedder calls an OpenAI-compatible /embeddings HTTP endpoint
// using only the Go standard library.
type OpenAICompatEmbedder struct {
	dims           int
	endpoint       string
	model          string
	apiKey         string
	batchSize      int
	maxRetries     int
	normalize      bool
	sendDimensions bool
	client         *http.Client
}

// NewOpenAICompatEmbedder validates cfg and builds an OpenAICompatEmbedder.
//
// Supported Config.Options keys (unknown keys fail closed):
//   - "base_url" (required): http:// or https:// base URL; "/embeddings" is
//     appended unless the URL path already ends with "/embeddings".
//   - "model" (required): non-empty model identifier.
//   - "api_key_env" (optional): name of an environment variable holding the
//     Bearer token. If set, the variable must be non-empty.
//   - "batch_size" (optional): positive int <= 2048 (default 64).
//   - "timeout_ms" (optional): positive int milliseconds per HTTP request (default 30000).
//   - "max_retries" (optional): int in [0, 10] for HTTP 429/5xx retries (default 2).
//   - "normalize" (optional): "true" or "false" (default "false"); L2-normalizes
//     returned vectors and fails with ErrInvalidOutput on zero vectors.
//   - "send_dimensions" (optional): "true" or "false" (default "false"); includes
//     "dimensions" in the request payload when true.
func NewOpenAICompatEmbedder(cfg Config) (Embedder, error) {
	if cfg.Provider != ProviderOpenAICompat {
		return nil, fmt.Errorf("embedding: provider %q: want %q", cfg.Provider, ProviderOpenAICompat)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	for k := range cfg.Options {
		switch k {
		case "base_url", "model", "api_key_env", "batch_size", "timeout_ms", "max_retries", "normalize", "send_dimensions":
		default:
			return nil, fmt.Errorf("embedding: openai_compat unsupported option %q", k)
		}
	}

	rawURL := strings.TrimSpace(cfg.Options["base_url"])
	if rawURL == "" {
		return nil, fmt.Errorf("embedding: openai_compat requires option \"base_url\"")
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("embedding: openai_compat invalid base_url %q: %w", rawURL, err)
	}
	if (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return nil, fmt.Errorf("embedding: openai_compat base_url %q must use http or https with a host", rawURL)
	}
	trimmedPath := strings.TrimRight(parsedURL.Path, "/")
	if !strings.HasSuffix(trimmedPath, "/embeddings") {
		parsedURL.Path = trimmedPath + "/embeddings"
	} else {
		parsedURL.Path = trimmedPath
	}

	model := strings.TrimSpace(cfg.Options["model"])
	if model == "" {
		return nil, fmt.Errorf("embedding: openai_compat requires option \"model\"")
	}

	var apiKey string
	if envName, ok := cfg.Options["api_key_env"]; ok {
		envName = strings.TrimSpace(envName)
		if envName == "" {
			return nil, fmt.Errorf("embedding: openai_compat api_key_env must be non-empty when set")
		}
		apiKey = os.Getenv(envName)
		if apiKey == "" {
			return nil, fmt.Errorf("embedding: openai_compat environment variable %q is empty or unset", envName)
		}
	}

	batchSize := defaultOpenAICompatBatchSize
	if raw, ok := cfg.Options["batch_size"]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n <= 0 || n > maxOpenAICompatBatchSize {
			return nil, fmt.Errorf("embedding: openai_compat invalid batch_size %q (must be in [1, %d])", raw, maxOpenAICompatBatchSize)
		}
		batchSize = n
	}

	timeoutMs := defaultOpenAICompatTimeoutMs
	if raw, ok := cfg.Options["timeout_ms"]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("embedding: openai_compat invalid timeout_ms %q (must be positive)", raw)
		}
		timeoutMs = n
	}

	maxRetries := defaultOpenAICompatMaxRetries
	if raw, ok := cfg.Options["max_retries"]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 0 || n > maxOpenAICompatRetries {
			return nil, fmt.Errorf("embedding: openai_compat invalid max_retries %q (must be in [0, %d])", raw, maxOpenAICompatRetries)
		}
		maxRetries = n
	}

	normalize, err := parseOptionalBool(cfg.Options, "normalize")
	if err != nil {
		return nil, err
	}
	sendDims, err := parseOptionalBool(cfg.Options, "send_dimensions")
	if err != nil {
		return nil, err
	}

	return &OpenAICompatEmbedder{
		dims:           cfg.Dimensions,
		endpoint:       parsedURL.String(),
		model:          model,
		apiKey:         apiKey,
		batchSize:      batchSize,
		maxRetries:     maxRetries,
		normalize:      normalize,
		sendDimensions: sendDims,
		client: &http.Client{
			Timeout: time.Duration(timeoutMs) * time.Millisecond,
		},
	}, nil
}

func parseOptionalBool(opts map[string]string, key string) (bool, error) {
	raw, ok := opts[key]
	if !ok {
		return false, nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("embedding: openai_compat invalid %s %q: %w", key, raw, err)
	}
	return v, nil
}

// Dimensions reports the fixed vector width.
func (e *OpenAICompatEmbedder) Dimensions() int { return e.dims }

type openAICompatRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions,omitempty"`
}

// strictFloat32Slice decodes a JSON array of non-null finite numbers, rejecting
// JSON null (which standard []float32 unmarshaling silently converts to 0),
// non-numeric elements, and non-finite/overflowing values.
type strictFloat32Slice []float32

func (s *strictFloat32Slice) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return fmt.Errorf("embedding must be a JSON array: %w", ErrInvalidOutput)
	}
	inner := bytes.TrimSpace(trimmed[1 : len(trimmed)-1])
	if len(inner) == 0 {
		*s = (*s)[:0]
		return nil
	}
	// Pre-size from comma count to avoid slice growth allocations.
	count := bytes.Count(inner, []byte(",")) + 1
	vals := make([]float32, 0, count)
	pos := 0
	for pos < len(inner) {
		next := bytes.IndexByte(inner[pos:], ',')
		var tok []byte
		if next < 0 {
			tok = bytes.TrimSpace(inner[pos:])
			pos = len(inner)
		} else {
			tok = bytes.TrimSpace(inner[pos : pos+next])
			pos += next + 1
			if pos >= len(inner) {
				return fmt.Errorf("trailing comma in embedding array: %w", ErrInvalidOutput)
			}
		}
		if len(tok) == 0 {
			return fmt.Errorf("empty element in embedding array: %w", ErrInvalidOutput)
		}
		// Reject JSON null, strings, booleans, objects, or nested arrays explicitly.
		if tok[0] != '-' && (tok[0] < '0' || tok[0] > '9') {
			return fmt.Errorf("non-numeric embedding component %q: %w", string(tok), ErrInvalidOutput)
		}
		f, err := strconv.ParseFloat(string(tok), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) > math.MaxFloat32 {
			return fmt.Errorf("invalid numeric embedding component %q: %w", string(tok), ErrInvalidOutput)
		}
		vals = append(vals, float32(f))
	}
	*s = vals
	return nil
}

type openAICompatResponse struct {
	Data []struct {
		Index     *int               `json:"index"`
		Embedding strictFloat32Slice `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

// EmbedBatch implements Embedder by chunking texts into e.batchSize HTTP
// requests, validating exact alignment, dimension width, and finiteness.
func (e *OpenAICompatEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, ErrEmptyBatch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("embedding: openai_compat canceled before start: %w", err)
	}

	// Single contiguous backing allocation for all output vectors.
	backing := make([]float32, len(texts)*e.dims)
	out := make([][]float32, len(texts))
	inputs := make([]string, 0, min(len(texts), e.batchSize))

	for start := 0; start < len(texts); start += e.batchSize {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("embedding: openai_compat canceled at offset %d: %w", start, err)
		}
		end := min(start+e.batchSize, len(texts))
		inputs = inputs[:0]
		for i := start; i < end; i++ {
			inputs = append(inputs, string(texts[i]))
		}
		if err := e.embedSubBatch(ctx, inputs, start, backing, out); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("embedding: openai_compat canceled before returning batch: %w", err)
	}
	return out, nil
}

func (e *OpenAICompatEmbedder) embedSubBatch(ctx context.Context, inputs []string, globalOffset int, backing []float32, out [][]float32) error {
	reqPayload := openAICompatRequest{
		Model: e.model,
		Input: inputs,
	}
	if e.sendDimensions {
		reqPayload.Dimensions = e.dims
	}
	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return fmt.Errorf("embedding: openai_compat marshal request: %w", err)
	}

	var respBytes []byte
	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := openAICompatBaseBackoff << min(attempt-1, 6)
			if backoff > openAICompatMaxBackoff {
				backoff = openAICompatMaxBackoff
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("embedding: openai_compat canceled during retry backoff: %w", ctx.Err())
			case <-timer.C:
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("embedding: openai_compat build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		if e.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+e.apiKey)
		}

		resp, err := e.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("embedding: openai_compat request canceled: %w", ctx.Err())
			}
			lastErr = fmt.Errorf("embedding: openai_compat request failed: %w", err)
			continue
		}

		data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxOpenAICompatResponseBytes+1))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("embedding: openai_compat read response: %w", readErr)
			continue
		}
		if int64(len(data)) > maxOpenAICompatResponseBytes {
			return fmt.Errorf("embedding: openai_compat response exceeded %d bytes: %w", maxOpenAICompatResponseBytes, ErrInvalidOutput)
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("embedding: openai_compat HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("embedding: openai_compat HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}
		respBytes = data
		lastErr = nil
		break
	}
	if lastErr != nil {
		return lastErr
	}

	var parsed openAICompatResponse
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return fmt.Errorf("embedding: openai_compat decode response: %w", errors.Join(ErrInvalidOutput, err))
	}
	if len(parsed.Data) != len(inputs) {
		return fmt.Errorf("embedding: openai_compat returned %d embeddings for %d inputs: %w", len(parsed.Data), len(inputs), ErrInvalidOutput)
	}

	seen := make([]bool, len(inputs))
	for _, item := range parsed.Data {
		if item.Index == nil {
			return fmt.Errorf("embedding: openai_compat response item missing index: %w", ErrInvalidOutput)
		}
		idx := *item.Index
		if idx < 0 || idx >= len(inputs) {
			return fmt.Errorf("embedding: openai_compat out-of-range index %d (batch size %d): %w", idx, len(inputs), ErrInvalidOutput)
		}
		if seen[idx] {
			return fmt.Errorf("embedding: openai_compat duplicate index %d: %w", idx, ErrInvalidOutput)
		}
		seen[idx] = true

		if len(item.Embedding) != e.dims {
			return fmt.Errorf("embedding: openai_compat item %d has %d dims, want %d: %w", globalOffset+idx, len(item.Embedding), e.dims, ErrDimensionMismatch)
		}
		dstIdx := globalOffset + idx
		dst := backing[dstIdx*e.dims : (dstIdx+1)*e.dims : (dstIdx+1)*e.dims]
		var normSq float64
		for j, v := range item.Embedding {
			f := float64(v)
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return fmt.Errorf("embedding: openai_compat item %d dim %d non-finite value: %w", dstIdx, j, ErrInvalidOutput)
			}
			dst[j] = v
			if e.normalize {
				normSq += f * f
			}
		}
		if e.normalize {
			if normSq == 0 {
				return fmt.Errorf("embedding: openai_compat item %d zero vector cannot be normalized: %w", dstIdx, ErrInvalidOutput)
			}
			invNorm := 1.0 / math.Sqrt(normSq)
			for j := range dst {
				dst[j] = float32(float64(dst[j]) * invNorm)
			}
		}
		out[dstIdx] = dst
	}
	return nil
}

// compile-time interface check
var _ Embedder = (*OpenAICompatEmbedder)(nil)
