package documentservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

var benchmarkVectorSearchBase64FieldNeedle = []byte(`"query_embedding_f32_le_b64"`)

const benchmarkVectorSearchDecodePeekBytes = 512

func (h *Handler) decodeBenchmarkVectorSearchRequest(w http.ResponseWriter, r *http.Request, maxBodyBytes int64) (BenchmarkVectorSearchRequest, bool) {
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = body.Close() }()

	var prefixBuf [benchmarkVectorSearchDecodePeekBytes]byte
	n, readErr := io.ReadFull(body, prefixBuf[:])
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		writeError(w, wrapServiceError(CodeMalformedJSON, "malformed JSON request body", readErr))
		return BenchmarkVectorSearchRequest{}, false
	}
	prefix := prefixBuf[:n]

	var req BenchmarkVectorSearchRequest
	var err error
	if bytes.Contains(prefix, benchmarkVectorSearchBase64FieldNeedle) {
		var rest []byte
		rest, err = io.ReadAll(body)
		if err == nil {
			raw := make([]byte, 0, len(prefix)+len(rest))
			raw = append(raw, prefix...)
			raw = append(raw, rest...)
			req, err = decodeBenchmarkVectorSearchJSON(raw)
		}
	} else {
		req, err = decodeBenchmarkVectorSearchJSONGenericReader(io.MultiReader(bytes.NewReader(prefix), body))
	}
	if err != nil {
		if ErrorCodeOf(err) != CodeInternal {
			writeError(w, err)
		} else {
			writeError(w, wrapServiceError(CodeMalformedJSON, "malformed JSON request body", err))
		}
		return BenchmarkVectorSearchRequest{}, false
	}
	return req, true
}

func decodeBenchmarkVectorSearchJSON(raw []byte) (BenchmarkVectorSearchRequest, error) {
	var req BenchmarkVectorSearchRequest
	i := skipJSONSpaces(raw, 0)
	if i >= len(raw) {
		return req, io.EOF
	}
	if raw[i] != '{' {
		_, err := decodeBenchmarkVectorSearchJSONGeneric(raw)
		if err != nil {
			return req, err
		}
		return req, fmt.Errorf("expected JSON object")
	}
	if !bytes.Contains(raw, benchmarkVectorSearchBase64FieldNeedle) {
		return decodeBenchmarkVectorSearchJSONGeneric(raw)
	}
	i++
	queryFromBase64 := false
	base64Present := false
	for {
		i = skipJSONSpaces(raw, i)
		if i >= len(raw) {
			return req, io.ErrUnexpectedEOF
		}
		if raw[i] == '}' {
			i++
			break
		}
		if raw[i] != '"' {
			return req, fmt.Errorf("expected JSON object key")
		}
		keyEnd, err := scanJSONString(raw, i)
		if err != nil {
			return req, err
		}
		var key string
		if err := json.Unmarshal(raw[i:keyEnd], &key); err != nil {
			return req, err
		}
		i = skipJSONSpaces(raw, keyEnd)
		if i >= len(raw) || raw[i] != ':' {
			return req, fmt.Errorf("expected ':' after JSON object key")
		}
		i = skipJSONSpaces(raw, i+1)
		valueStart := i
		valueEnd, err := scanJSONValue(raw, valueStart)
		if err != nil {
			return req, err
		}
		value := raw[valueStart:valueEnd]
		switch key {
		case "expected_generation":
			err = json.Unmarshal(value, &req.ExpectedGeneration)
		case "vector_index_name":
			err = json.Unmarshal(value, &req.VectorIndexName)
		case "query_embedding":
			var query []float32
			err = json.Unmarshal(value, &query)
			if err == nil && query != nil {
				if base64Present {
					return req, serviceError(CodeInvalidRequest, "benchmark vector search accepts either query_embedding or query_embedding_f32_le_b64, not both")
				}
				req.QueryEmbedding = query
				queryFromBase64 = false
			} else if err == nil && !queryFromBase64 {
				req.QueryEmbedding = nil
			}
		case "query_embedding_f32_le_b64":
			var query []float32
			query, base64Present, err = decodeBenchmarkVectorSearchBase64JSONValue(value)
			if err == nil {
				if base64Present {
					if req.QueryEmbedding != nil && !queryFromBase64 {
						return req, serviceError(CodeInvalidRequest, "benchmark vector search accepts either query_embedding or query_embedding_f32_le_b64, not both")
					}
					req.QueryEmbedding = query
					queryFromBase64 = true
				} else if queryFromBase64 {
					req.QueryEmbedding = nil
					queryFromBase64 = false
				}
			}
		case "top_k":
			err = json.Unmarshal(value, &req.TopK)
		case "ef_search":
			err = json.Unmarshal(value, &req.EfSearch)
		case "query_mode":
			err = json.Unmarshal(value, &req.QueryMode)
		case "quantized_index_name":
			err = json.Unmarshal(value, &req.QuantizedIndexName)
		case "quantized_rerank_candidates":
			err = json.Unmarshal(value, &req.QuantizedRerankCandidates)
		case "stats_mode":
			err = json.Unmarshal(value, &req.StatsMode)
		default:
			return req, fmt.Errorf("json: unknown field %q", key)
		}
		if err != nil {
			return req, err
		}
		i = skipJSONSpaces(raw, valueEnd)
		if i >= len(raw) {
			return req, io.ErrUnexpectedEOF
		}
		switch raw[i] {
		case ',':
			i++
		case '}':
			i++
			if err := rejectTrailingJSONValues(raw[i:]); err != nil {
				return req, err
			}
			return req, nil
		default:
			return req, fmt.Errorf("expected ',' or '}' after JSON object field")
		}
	}
	if err := rejectTrailingJSONValues(raw[i:]); err != nil {
		return req, err
	}
	return req, nil
}

func decodeBenchmarkVectorSearchBase64JSONValue(value []byte) ([]float32, bool, error) {
	trimmed := bytes.TrimSpace(value)
	if bytes.Equal(trimmed, []byte("null")) {
		return nil, false, nil
	}
	if len(trimmed) < 2 || trimmed[0] != '"' || trimmed[len(trimmed)-1] != '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return nil, false, err
		}
		return decodeBenchmarkVectorSearchBase64String(s)
	}
	content := trimmed[1 : len(trimmed)-1]
	if jsonStringNeedsUnquote(content) {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return nil, false, err
		}
		return decodeBenchmarkVectorSearchBase64String(s)
	}
	content = bytes.TrimSpace(content)
	if len(content) == 0 {
		return nil, false, nil
	}
	query, err := decodeBenchmarkVectorQueryEmbeddingF32LEBase64Bytes(content)
	return query, true, err
}

func decodeBenchmarkVectorSearchBase64String(encoded string) ([]float32, bool, error) {
	encoded = trimASCIIWhitespaceString(encoded)
	if encoded == "" {
		return nil, false, nil
	}
	query, err := decodeBenchmarkVectorQueryEmbeddingF32LEBase64String(encoded)
	return query, true, err
}

func decodeBenchmarkVectorSearchJSONGeneric(raw []byte) (BenchmarkVectorSearchRequest, error) {
	return decodeBenchmarkVectorSearchJSONGenericReader(bytes.NewReader(raw))
}

func decodeBenchmarkVectorSearchJSONGenericReader(r io.Reader) (BenchmarkVectorSearchRequest, error) {
	var req BenchmarkVectorSearchRequest
	dec := json.NewDecoder(r)
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return BenchmarkVectorSearchRequest{}, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values in request body")
		}
		return BenchmarkVectorSearchRequest{}, err
	}
	return req, nil
}

func rejectTrailingJSONValues(raw []byte) error {
	i := skipJSONSpaces(raw, 0)
	if i >= len(raw) {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw[i:]))
	dec.UseNumber()
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values in request body")
		}
		return err
	}
	return nil
}

func skipJSONSpaces(raw []byte, i int) int {
	for i < len(raw) {
		switch raw[i] {
		case ' ', '\n', '\r', '\t':
			i++
		default:
			return i
		}
	}
	return i
}

func scanJSONValue(raw []byte, i int) (int, error) {
	i = skipJSONSpaces(raw, i)
	if i >= len(raw) {
		return i, io.ErrUnexpectedEOF
	}
	switch raw[i] {
	case '"':
		return scanJSONString(raw, i)
	case '{':
		return scanJSONComposite(raw, i, '{', '}')
	case '[':
		return scanJSONComposite(raw, i, '[', ']')
	case 't':
		return scanJSONLiteral(raw, i, "true")
	case 'f':
		return scanJSONLiteral(raw, i, "false")
	case 'n':
		return scanJSONLiteral(raw, i, "null")
	default:
		if raw[i] == '-' || (raw[i] >= '0' && raw[i] <= '9') {
			return scanJSONNumber(raw, i)
		}
		return i, fmt.Errorf("invalid JSON value")
	}
}

func scanJSONString(raw []byte, i int) (int, error) {
	if i >= len(raw) || raw[i] != '"' {
		return i, fmt.Errorf("expected JSON string")
	}
	for j := i + 1; j < len(raw); j++ {
		switch raw[j] {
		case '"':
			return j + 1, nil
		case '\\':
			j++
			if j >= len(raw) {
				return j, io.ErrUnexpectedEOF
			}
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31:
			return j, fmt.Errorf("invalid character in JSON string")
		}
	}
	return len(raw), io.ErrUnexpectedEOF
}

func scanJSONComposite(raw []byte, i int, open, close byte) (int, error) {
	depth := 0
	for i < len(raw) {
		switch raw[i] {
		case '"':
			end, err := scanJSONString(raw, i)
			if err != nil {
				return end, err
			}
			i = end
			continue
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
		i++
	}
	return i, io.ErrUnexpectedEOF
}

func scanJSONLiteral(raw []byte, i int, literal string) (int, error) {
	end := i + len(literal)
	if end > len(raw) || string(raw[i:end]) != literal {
		return i, fmt.Errorf("invalid JSON literal")
	}
	return end, nil
}

func scanJSONNumber(raw []byte, i int) (int, error) {
	start := i
	if raw[i] == '-' {
		i++
		if i >= len(raw) {
			return i, io.ErrUnexpectedEOF
		}
	}
	if raw[i] == '0' {
		i++
	} else if raw[i] >= '1' && raw[i] <= '9' {
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
	} else {
		return i, fmt.Errorf("invalid JSON number")
	}
	if i < len(raw) && raw[i] == '.' {
		i++
		if i >= len(raw) || raw[i] < '0' || raw[i] > '9' {
			return i, fmt.Errorf("invalid JSON number")
		}
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
	}
	if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
		i++
		if i < len(raw) && (raw[i] == '+' || raw[i] == '-') {
			i++
		}
		if i >= len(raw) || raw[i] < '0' || raw[i] > '9' {
			return i, fmt.Errorf("invalid JSON number")
		}
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
	}
	if i == start {
		return i, fmt.Errorf("invalid JSON number")
	}
	return i, nil
}

func jsonStringNeedsUnquote(content []byte) bool {
	for _, c := range content {
		if c == '\\' || c < 0x20 {
			return true
		}
	}
	return false
}

func trimASCIIWhitespaceString(s string) string {
	start := 0
	for start < len(s) {
		switch s[start] {
		case ' ', '\n', '\r', '\t':
			start++
		default:
			goto foundStart
		}
	}
	return ""
foundStart:
	end := len(s)
	for end > start {
		switch s[end-1] {
		case ' ', '\n', '\r', '\t':
			end--
		default:
			return s[start:end]
		}
	}
	return ""
}
