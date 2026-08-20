package documentservice

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const defaultMaxRequestBytes = 16 << 20

// Handler serves the pre-alpha HTTP/JSON document service contract.
type Handler struct {
	Service      *Service
	MaxBodyBytes int64
}

// NewHandler returns an HTTP handler for service.
func NewHandler(service *Service) *Handler {
	return &Handler{Service: service, MaxBodyBytes: defaultMaxRequestBytes}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.Service == nil {
		writeError(w, serviceError(CodeIndexUnavailable, "document service is unavailable"))
		return
	}
	maxBodyBytes := h.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultMaxRequestBytes
	}
	parts := splitPath(r.URL.Path)
	if len(parts) == 2 && parts[0] == "v1" && parts[1] == "health" {
		if r.Method != http.MethodGet {
			writeError(w, serviceErrorf(CodeInvalidRequest, "method %s is not allowed for %s", r.Method, r.URL.Path))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "contract_version": ContractVersion})
		return
	}
	if len(parts) == 2 && parts[0] == "v1" && parts[1] == "indexes" {
		if r.Method != http.MethodPost {
			writeError(w, serviceErrorf(CodeInvalidRequest, "method %s is not allowed for %s", r.Method, r.URL.Path))
			return
		}
		var req CreateIndexRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		info, err := h.Service.CreateIndex(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"index": info})
		return
	}
	if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "indexes" {
		index, err := url.PathUnescape(parts[2])
		if err != nil {
			writeError(w, wrapServiceError(CodeInvalidRequest, "invalid index path", err))
			return
		}
		if len(parts) == 3 {
			if r.Method != http.MethodGet {
				writeError(w, serviceErrorf(CodeInvalidRequest, "method %s is not allowed for %s", r.Method, r.URL.Path))
				return
			}
			info, err := h.Service.OpenIndex(r.Context(), index)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"index": info})
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, serviceErrorf(CodeInvalidRequest, "method %s is not allowed for %s", r.Method, r.URL.Path))
			return
		}
		if len(parts) == 4 {
			h.serveIndexOperation(w, r, index, parts[3], maxBodyBytes)
			return
		}
		if len(parts) == 5 && parts[3] == "documents" {
			h.serveDocumentOperation(w, r, index, parts[4], maxBodyBytes)
			return
		}
		if len(parts) == 5 && parts[3] == "search" {
			h.serveSearchOperation(w, r, index, parts[4], maxBodyBytes)
			return
		}
	}
	writeError(w, serviceErrorf(CodeInvalidRequest, "unknown document service route %q", r.URL.Path))
}

func (h *Handler) serveIndexOperation(w http.ResponseWriter, r *http.Request, index, op string, maxBodyBytes int64) {
	switch op {
	case "reset":
		var req ResetIndexRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.ResetIndex(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "optimize":
		var req OptimizeIndexRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.OptimizeIndex(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	default:
		writeError(w, serviceErrorf(CodeInvalidRequest, "unknown index operation %q", op))
	}
}

func writeBenchmarkVectorSearchResponse(w http.ResponseWriter, response BenchmarkVectorSearchResponse, format BenchmarkVectorResponseFormat) {
	if format == BenchmarkVectorResponseFormatIDs {
		writeJSON(w, http.StatusOK, BenchmarkVectorSearchIDsResponse{ResponseFormat: format, IDs: response.compactIDs})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) serveDocumentOperation(w http.ResponseWriter, r *http.Request, index, op string, maxBodyBytes int64) {
	switch op {
	case "upsert":
		var req UpsertDocumentsRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.UpsertDocuments(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "delete":
		var req DeleteDocumentsRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.DeleteDocuments(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "count":
		var req CountDocumentsRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.CountDocuments(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "filter":
		var req FilterDocumentsRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.FilterDocuments(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	default:
		writeError(w, serviceErrorf(CodeInvalidRequest, "unknown document operation %q", op))
	}
}

func (h *Handler) serveSearchOperation(w http.ResponseWriter, r *http.Request, index, op string, maxBodyBytes int64) {
	switch op {
	case "vector":
		var req DenseVectorSearchRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.SearchDenseVector(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "keyword":
		var req KeywordSearchRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.SearchKeyword(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "hybrid":
		var req HybridSearchRequest
		if !h.decodeJSON(w, r, maxBodyBytes, &req) {
			return
		}
		res, err := h.Service.SearchHybrid(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "vector-index":
		req, ok := h.decodeBenchmarkVectorSearchRequest(w, r, maxBodyBytes)
		if !ok {
			return
		}
		res, err := h.Service.SearchBenchmarkVector(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeBenchmarkVectorSearchResponse(w, res, req.ResponseFormat)
	case "vector-index:binary":
		req, ok := h.decodeBenchmarkVectorSearchBinaryRequest(w, r, maxBodyBytes)
		if !ok {
			return
		}
		res, err := h.Service.SearchBenchmarkVector(r.Context(), index, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeBenchmarkVectorSearchResponse(w, res, req.ResponseFormat)
	default:
		writeError(w, serviceErrorf(CodeInvalidRequest, "unknown search operation %q", op))
	}
}

func (h *Handler) decodeJSON(w http.ResponseWriter, r *http.Request, maxBodyBytes int64, dst any) bool {
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = body.Close() }()
	dec := json.NewDecoder(body)
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, wrapServiceError(CodeMalformedJSON, "malformed JSON request body", err))
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values in request body")
		}
		writeError(w, wrapServiceError(CodeMalformedJSON, "malformed JSON request body", err))
		return false
	}
	return true
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"failed to encode response"}}` + "\n"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(payload, '\n'))
}

func writeError(w http.ResponseWriter, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	var out *Error
	if serviceCode := ErrorCodeOf(err); serviceCode != CodeInternal {
		var serviceErr *Error
		if errors.As(err, &serviceErr) {
			out = serviceErr
		}
	}
	if out == nil {
		out = &Error{Code: ErrorCodeOf(err), Message: err.Error()}
	}
	writeJSON(w, httpStatusForError(err), map[string]any{"error": out})
}
