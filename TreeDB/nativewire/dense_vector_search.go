package nativewire

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"strings"

	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const (
	denseFilterMaxDepth  = 16
	denseFilterMaxLeaves = 64

	denseFilterEQ  = 1
	denseFilterGT  = 2
	denseFilterGTE = 3
	denseFilterLT  = 4
	denseFilterLTE = 5

	denseValueString = 1
	denseValueBool   = 2
	denseValueInt64  = 3
	denseValueDouble = 4
)

// DenseVectorSearchRequest is the ANN-only native document-service request.
type DenseVectorSearchRequest struct {
	Index              string
	Query              []float32
	TopK               int
	EfSearch           int
	ExpectedGeneration uint64
	Filter             *documentservice.Filter
	ReturnEmbedding    bool
}

// DenseVectorSearchResult borrows ID and Document from the client's response
// buffer until its next round trip.
type DenseVectorSearchResult struct {
	ID       []byte
	Score    float64
	Document []byte
}

// DenseVectorSearchResponse and its Results borrow from the client until its
// next round trip.
type DenseVectorSearchResponse struct {
	Results                   []DenseVectorSearchResult
	Route                     documentservice.Route
	Candidates                int
	NativeBasePlusLiveDelta   bool
	ExactFallbacks            uint64
	FullDocumentScanFallbacks uint64
}

// DenseVectorSearch executes one filtered native-runtime ANN query.
func (c *Client) DenseVectorSearch(ctx context.Context, request DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	if c == nil {
		return DenseVectorSearchResponse{}, io.ErrClosedPipe
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	payload, err := appendDenseVectorSearchRequest(c.denseRequest[:0], request, c.limits)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	body, err := appendCommandRequestBody(c.requestBody[:0], iwire.CommandDenseVectorSearch, iwire.Section{ID: iwire.SectionDenseSearchRequest, Bytes: payload})
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	_, response, err := c.roundTripLocked(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.denseRequest = payload[:0]
	c.requestBody = body[:0]
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	c.vectorSections, err = iwire.DecodeSectionsInto(c.vectorSections[:0], response, c.limits)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	meta, ok, err := singletonSection(c.vectorSections, iwire.SectionDenseSearchResponse)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "dense search response metadata missing")
		}
		return DenseVectorSearchResponse{}, err
	}
	ids, ok, err := singletonSection(c.vectorSections, iwire.SectionDocumentIDs)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "dense search response ids missing")
		}
		return DenseVectorSearchResponse{}, err
	}
	docs, ok, err := singletonSection(c.vectorSections, iwire.SectionDocuments)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "dense search response documents missing")
		}
		return DenseVectorSearchResponse{}, err
	}
	var out DenseVectorSearchResponse
	out, c.denseIDs, c.denseDocuments, c.denseResults, err = decodeDenseVectorSearchResponse(
		ids, docs, meta, c.limits, c.denseIDs, c.denseDocuments, c.denseResults,
	)
	return out, err
}

func (s *Server) handleDenseVectorSearch(ctx context.Context, state *connState, sections []iwire.Section, dst []byte) ([]byte, error) {
	if s.documentService == nil {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "dense document service is not configured")
	}
	raw, ok, err := singletonSection(sections, iwire.SectionDenseSearchRequest)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrInvalidCommand, "dense search request missing")
		}
		return nil, err
	}
	request, leaves, err := decodeDenseVectorSearchRequest(raw, s.limits, state.vectorQuery[:0], &state.denseFilter, state.denseFilters[:0])
	if err != nil {
		return nil, err
	}
	state.vectorQuery = request.Query[:0]
	state.denseFilters = leaves[:0]
	defer clearDenseVectorSearchScratch(state)
	response, err := s.documentService.SearchDenseVectorNativeRawInto(ctx, request.Index, documentservice.DenseVectorSearchRequest{
		ExpectedGeneration: request.ExpectedGeneration,
		QueryEmbedding:     request.Query,
		TopK:               request.TopK,
		EfSearch:           request.EfSearch,
		Route:              documentservice.RouteAnn,
		Filter:             request.Filter,
		ReturnEmbedding:    request.ReturnEmbedding,
	}, state.denseResults[:0])
	if err != nil {
		return nil, err
	}
	state.denseResults = response.Results[:0]

	state.idsScratch = resizeByteSlices(state.idsScratch, len(response.Results))
	state.docsScratch = resizeByteSlices(state.docsScratch, len(response.Results))
	for i := range response.Results {
		state.idsScratch[i] = response.Results[i].ID
		state.docsScratch[i] = response.Results[i].Document
	}
	state.denseMeta = appendDenseVectorSearchResponse(state.denseMeta[:0], response)
	idLen := iwire.ByteVectorEncodedLen(state.idsScratch)
	docLen := iwire.ByteVectorEncodedLen(state.docsScratch)
	idBytes, err := denseByteVectorPayloadLen(state.idsScratch)
	if err != nil {
		return nil, err
	}
	docBytes, err := denseByteVectorPayloadLen(state.docsScratch)
	if err != nil {
		return nil, err
	}
	if err := s.checkResponseSectionLen("dense ids", idLen); err != nil {
		return nil, err
	}
	if err := s.checkResponseSectionLen("dense documents", docLen); err != nil {
		return nil, err
	}
	if err := s.checkResponseSectionLen("dense metadata", len(state.denseMeta)); err != nil {
		return nil, err
	}
	if err := s.checkResponseByteVectorLen("dense ids", idBytes); err != nil {
		return nil, err
	}
	if err := s.checkResponseByteVectorLen("dense documents", docBytes); err != nil {
		return nil, err
	}
	bodyLen := uint64(0)
	for _, section := range []struct {
		id     iwire.SectionID
		length int
	}{{iwire.SectionDocumentIDs, idLen}, {iwire.SectionDocuments, docLen}, {iwire.SectionDenseSearchResponse, len(state.denseMeta)}} {
		sectionLen, err := responseSectionBodyLen(section.id, section.length)
		if err != nil {
			return nil, err
		}
		bodyLen, err = addResponseLen(bodyLen, sectionLen)
		if err != nil {
			return nil, err
		}
	}
	if err := s.checkResponseBodyLen(bodyLen); err != nil {
		return nil, err
	}
	for _, section := range []struct {
		id     iwire.SectionID
		length int
	}{
		{iwire.SectionDocumentIDs, idLen},
		{iwire.SectionDocuments, docLen},
		{iwire.SectionDenseSearchResponse, len(state.denseMeta)},
	} {
		dst, err = iwire.AppendSectionHeader(dst, section.id, 0, section.length)
		if err != nil {
			return nil, err
		}
		switch section.id {
		case iwire.SectionDocumentIDs:
			dst = iwire.AppendByteVectorWithEncodedLen(dst, idLen, state.idsScratch...)
		case iwire.SectionDocuments:
			dst = iwire.AppendByteVectorWithEncodedLen(dst, docLen, state.docsScratch...)
		default:
			dst = append(dst, state.denseMeta...)
		}
	}
	return dst, nil
}

func clearDenseVectorSearchScratch(state *connState) {
	clear(state.denseResults[:cap(state.denseResults)])
	state.denseResults = state.denseResults[:0]
	clear(state.idsScratch[:cap(state.idsScratch)])
	state.idsScratch = state.idsScratch[:0]
	clear(state.docsScratch[:cap(state.docsScratch)])
	state.docsScratch = state.docsScratch[:0]
}

func denseByteVectorPayloadLen(items [][]byte) (int, error) {
	total := 0
	for _, item := range items {
		var ok bool
		total, ok = addPayloadLen(total, len(item))
		if !ok {
			return 0, protocolError(iwire.ErrResourceExhausted, "dense byte-vector length exceeds int capacity")
		}
	}
	return total, nil
}

func resizeByteSlices(dst [][]byte, count int) [][]byte {
	if count <= cap(dst) {
		if count < len(dst) {
			clear(dst[count:])
		}
		return dst[:count]
	}
	return make([][]byte, count)
}

func appendDenseVectorSearchRequest(dst []byte, request DenseVectorSearchRequest, limits iwire.Limits) ([]byte, error) {
	limits = denseDefaultLimits(limits)
	if request.Index == "" || request.TopK <= 0 || request.EfSearch < 0 || len(request.Query) == 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "dense search requires index, query, positive top_k, and non-negative ef_search")
	}
	if request.TopK > limits.MaxByteVectorItems {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense top_k %d exceeds limit %d", request.TopK, limits.MaxByteVectorItems)
	}
	if uint64(len(request.Index)) > limits.MaxDeterministicNameBytes {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense index name exceeds limit")
	}
	if len(request.Query) > limits.MaxByteVectorItems || uint64(len(request.Query))*4 > limits.MaxSectionLen {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense query exceeds limit")
	}
	leaves, err := countDenseFilterLeaves(request.Filter, 0)
	if err != nil {
		return nil, err
	}

	dst = binary.AppendUvarint(dst, uint64(len(request.Index)))
	dst = append(dst, request.Index...)
	dst = binary.AppendUvarint(dst, uint64(request.TopK))
	dst = binary.AppendUvarint(dst, uint64(request.EfSearch))
	dst = binary.AppendUvarint(dst, request.ExpectedGeneration)
	if request.ReturnEmbedding {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, uint64(len(request.Query)))
	for _, value := range request.Query {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, protocolError(iwire.ErrInvalidCommand, "dense query must be finite")
		}
		dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(value))
	}
	dst = binary.AppendUvarint(dst, uint64(leaves))
	dst, err = appendDenseFilterLeaves(dst, request.Filter, limits)
	if err == nil && uint64(len(dst)) > limits.MaxSectionLen {
		err = protocolError(iwire.ErrResourceExhausted, "dense search request exceeds section limit")
	}
	return dst, err
}

func countDenseFilterLeaves(filter *documentservice.Filter, depth int) (int, error) {
	if filter == nil {
		return 0, nil
	}
	if depth >= denseFilterMaxDepth {
		return 0, protocolError(iwire.ErrResourceExhausted, "dense filter exceeds maximum depth")
	}
	op := strings.ToLower(strings.TrimSpace(filter.Operator))
	if op != "and" {
		if _, ok := denseFilterOperatorCode(op); !ok || strings.TrimSpace(filter.Field) == "" || len(filter.Conditions) != 0 {
			return 0, protocolError(iwire.ErrUnsupportedFeature, "dense filter supports equality/range leaves joined by AND")
		}
		return 1, nil
	}
	if filter.Field != "" || filter.Value != nil || len(filter.Conditions) == 0 {
		return 0, protocolError(iwire.ErrInvalidCommand, "dense AND filter is malformed")
	}
	total := 0
	for i := range filter.Conditions {
		count, err := countDenseFilterLeaves(&filter.Conditions[i], depth+1)
		if err != nil {
			return 0, err
		}
		total += count
		if total > denseFilterMaxLeaves {
			return 0, protocolError(iwire.ErrResourceExhausted, "dense filter exceeds leaf limit")
		}
	}
	return total, nil
}

func appendDenseFilterLeaves(dst []byte, filter *documentservice.Filter, limits iwire.Limits) ([]byte, error) {
	if filter == nil {
		return dst, nil
	}
	op := strings.ToLower(strings.TrimSpace(filter.Operator))
	if op == "and" {
		var err error
		for i := range filter.Conditions {
			dst, err = appendDenseFilterLeaves(dst, &filter.Conditions[i], limits)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	}
	code, _ := denseFilterOperatorCode(op)
	field := strings.TrimSpace(filter.Field)
	if uint64(len(field)) > limits.MaxDeterministicNameBytes {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense filter field exceeds limit")
	}
	dst = binary.AppendUvarint(dst, uint64(len(field)))
	dst = append(dst, field...)
	dst = append(dst, code)
	return appendDenseFilterValue(dst, filter.Value, code, limits)
}

func denseFilterOperatorCode(op string) (byte, bool) {
	switch op {
	case "==":
		return denseFilterEQ, true
	case ">":
		return denseFilterGT, true
	case ">=":
		return denseFilterGTE, true
	case "<":
		return denseFilterLT, true
	case "<=":
		return denseFilterLTE, true
	default:
		return 0, false
	}
}

func denseFilterOperator(code byte) (string, bool) {
	switch code {
	case denseFilterEQ:
		return "==", true
	case denseFilterGT:
		return ">", true
	case denseFilterGTE:
		return ">=", true
	case denseFilterLT:
		return "<", true
	case denseFilterLTE:
		return "<=", true
	default:
		return "", false
	}
}

func appendDenseFilterValue(dst []byte, value any, operator byte, limits iwire.Limits) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		if uint64(len(typed)) > limits.MaxSectionLen {
			return nil, protocolError(iwire.ErrResourceExhausted, "dense filter string exceeds limit")
		}
		dst = append(dst, denseValueString)
		dst = binary.AppendUvarint(dst, uint64(len(typed)))
		return append(dst, typed...), nil
	case bool:
		if operator != denseFilterEQ {
			return nil, protocolError(iwire.ErrInvalidCommand, "boolean dense filters support equality only")
		}
		dst = append(dst, denseValueBool)
		if typed {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case int:
		return appendDenseInt64(dst, int64(typed)), nil
	case int64:
		return appendDenseInt64(dst, typed), nil
	case float64:
		return appendDenseFloat64(dst, typed)
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return appendDenseInt64(dst, integer), nil
		}
		value, err := typed.Float64()
		if err != nil {
			return nil, protocolError(iwire.ErrInvalidCommand, "dense filter number is invalid")
		}
		return appendDenseFloat64(dst, value)
	default:
		return nil, protocolError(iwire.ErrInvalidCommand, "dense filter value type is unsupported")
	}
}

func appendDenseInt64(dst []byte, value int64) []byte {
	dst = append(dst, denseValueInt64)
	return binary.AppendVarint(dst, value)
}

func appendDenseFloat64(dst []byte, value float64) ([]byte, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, protocolError(iwire.ErrInvalidCommand, "dense filter number must be finite")
	}
	dst = append(dst, denseValueDouble)
	return binary.LittleEndian.AppendUint64(dst, math.Float64bits(value)), nil
}

func decodeDenseVectorSearchRequest(src []byte, limits iwire.Limits, query []float32, root *documentservice.Filter, leaves []documentservice.Filter) (DenseVectorSearchRequest, []documentservice.Filter, error) {
	limits = denseDefaultLimits(limits)
	var request DenseVectorSearchRequest
	off := 0
	index, err := readDenseString(src, &off, limits.MaxDeterministicNameBytes, "index")
	if err != nil || index == "" {
		return request, leaves, denseDecodeError(err, "dense index is empty")
	}
	request.Index = index
	topK, err := readDenseInt(src, &off, "top_k")
	if err != nil || topK <= 0 {
		return request, leaves, denseDecodeError(err, "dense top_k must be positive")
	}
	if topK > limits.MaxByteVectorItems {
		return request, leaves, protocolError(iwire.ErrResourceExhausted, "dense top_k %d exceeds limit %d", topK, limits.MaxByteVectorItems)
	}
	request.TopK = topK
	efSearch, err := readDenseInt(src, &off, "ef_search")
	if err != nil {
		return request, leaves, err
	}
	request.EfSearch = efSearch
	request.ExpectedGeneration, err = readUvarintField(src, &off, "expected_generation")
	if err != nil {
		return request, leaves, err
	}
	if off >= len(src) || (src[off] != 0 && src[off] != 1) {
		return request, leaves, protocolError(iwire.ErrMalformedFrame, "dense return_embedding is invalid")
	}
	request.ReturnEmbedding = src[off] == 1
	off++
	queryCount, err := readUvarintField(src, &off, "query_count")
	if err != nil || queryCount == 0 || queryCount > uint64(limits.MaxByteVectorItems) || queryCount > uint64((len(src)-off)/4) {
		return request, leaves, denseDecodeError(err, "dense query length is invalid")
	}
	if int(queryCount) <= cap(query) {
		query = query[:int(queryCount)]
	} else {
		query = make([]float32, int(queryCount))
	}
	for i := range query {
		query[i] = math.Float32frombits(binary.LittleEndian.Uint32(src[off+i*4:]))
		if math.IsNaN(float64(query[i])) || math.IsInf(float64(query[i]), 0) {
			return request, leaves, protocolError(iwire.ErrInvalidCommand, "dense query must be finite")
		}
	}
	off += len(query) * 4
	request.Query = query
	leafCount, err := readUvarintField(src, &off, "filter_count")
	if err != nil || leafCount > denseFilterMaxLeaves || leafCount > uint64(len(src)-off) {
		return request, leaves, denseDecodeError(err, "dense filter count is invalid")
	}
	if int(leafCount) <= cap(leaves) {
		leaves = leaves[:int(leafCount)]
		clear(leaves)
	} else {
		leaves = make([]documentservice.Filter, int(leafCount))
	}
	for i := range leaves {
		field, err := readDenseString(src, &off, limits.MaxDeterministicNameBytes, "filter field")
		if err != nil || field == "" || off >= len(src) {
			return request, leaves, denseDecodeError(err, "dense filter field/operator is invalid")
		}
		op, ok := denseFilterOperator(src[off])
		off++
		if !ok {
			return request, leaves, protocolError(iwire.ErrUnsupportedFeature, "dense filter operator is unsupported")
		}
		value, err := readDenseFilterValue(src, &off, op, limits)
		if err != nil {
			return request, leaves, err
		}
		leaves[i] = documentservice.Filter{Field: field, Operator: op, Value: value}
	}
	if off != len(src) {
		return request, leaves, protocolError(iwire.ErrMalformedFrame, "dense request has trailing bytes")
	}
	if len(leaves) == 1 {
		*root = leaves[0]
		request.Filter = root
	} else if len(leaves) > 1 {
		*root = documentservice.Filter{Operator: "AND", Conditions: leaves}
		request.Filter = root
	} else {
		*root = documentservice.Filter{}
	}
	return request, leaves, nil
}

func readDenseFilterValue(src []byte, off *int, operator string, limits iwire.Limits) (any, error) {
	if *off >= len(src) {
		return nil, protocolError(iwire.ErrMalformedFrame, "dense filter value is missing")
	}
	kind := src[*off]
	*off += 1
	switch kind {
	case denseValueString:
		return readDenseString(src, off, limits.MaxSectionLen, "filter string")
	case denseValueBool:
		if operator != "==" || *off >= len(src) || (src[*off] != 0 && src[*off] != 1) {
			return nil, protocolError(iwire.ErrInvalidCommand, "dense boolean filter value is invalid")
		}
		value := src[*off] == 1
		*off += 1
		return value, nil
	case denseValueInt64:
		return readVarint(src, off)
	case denseValueDouble:
		if len(src)-*off < 8 {
			return nil, protocolError(iwire.ErrMalformedFrame, "dense double filter value is truncated")
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(src[*off:]))
		*off += 8
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, protocolError(iwire.ErrInvalidCommand, "dense filter number must be finite")
		}
		return value, nil
	default:
		return nil, protocolError(iwire.ErrMalformedFrame, "dense filter value type is invalid")
	}
}

func readDenseString(src []byte, off *int, limit uint64, field string) (string, error) {
	length, err := readUvarintField(src, off, field+" length")
	if err != nil {
		return "", err
	}
	if length > limit || length > uint64(len(src)-*off) {
		return "", protocolError(iwire.ErrResourceExhausted, "%s exceeds limit", field)
	}
	value := string(src[*off : *off+int(length)])
	*off += int(length)
	return value, nil
}

func readDenseInt(src []byte, off *int, field string) (int, error) {
	value, err := readUvarintField(src, off, field)
	if err != nil {
		return 0, err
	}
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return 0, protocolError(iwire.ErrResourceExhausted, "%s exceeds int capacity", field)
	}
	return int(value), nil
}

func denseDecodeError(err error, message string) error {
	if err != nil {
		return err
	}
	return protocolError(iwire.ErrMalformedFrame, "%s", message)
}

func appendDenseVectorSearchResponse(dst []byte, response documentservice.RawDenseVectorSearchResponse) []byte {
	if response.NativeBasePlusLiveDelta {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, uint64(response.Candidates))
	dst = binary.AppendUvarint(dst, response.ExactFallbacks)
	dst = binary.AppendUvarint(dst, response.FullDocumentScanFallbacks)
	dst = binary.AppendUvarint(dst, uint64(len(response.Results)))
	for i := range response.Results {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(response.Results[i].Score))
	}
	return dst
}

func decodeDenseVectorSearchResponse(idsRaw, docsRaw, meta []byte, limits iwire.Limits, ids, docs [][]byte, results []DenseVectorSearchResult) (DenseVectorSearchResponse, [][]byte, [][]byte, []DenseVectorSearchResult, error) {
	limits = denseDefaultLimits(limits)
	var response DenseVectorSearchResponse
	var err error
	ids, err = iwire.DecodeByteVectorItemsInto(ids[:0], idsRaw, limits)
	if err != nil {
		return response, ids, docs, results, err
	}
	docs, err = iwire.DecodeByteVectorItemsInto(docs[:0], docsRaw, limits)
	if err != nil {
		return response, ids, docs, results, err
	}
	if len(meta) == 0 || (meta[0] != 0 && meta[0] != 1) {
		return response, ids, docs, results, protocolError(iwire.ErrMalformedFrame, "dense route proof is invalid")
	}
	response.Route = documentservice.RouteAnn
	response.NativeBasePlusLiveDelta = meta[0] == 1
	off := 1
	response.Candidates, err = readDenseInt(meta, &off, "candidates")
	if err != nil {
		return response, ids, docs, results, err
	}
	response.ExactFallbacks, err = readUvarintField(meta, &off, "exact_fallbacks")
	if err != nil {
		return response, ids, docs, results, err
	}
	response.FullDocumentScanFallbacks, err = readUvarintField(meta, &off, "full_document_scan_fallbacks")
	if err != nil {
		return response, ids, docs, results, err
	}
	count, err := readUvarintField(meta, &off, "result_count")
	if err != nil || count != uint64(len(ids)) || len(ids) != len(docs) || count > uint64(limits.MaxByteVectorItems) || count > uint64((len(meta)-off)/8) || len(meta)-off != int(count)*8 {
		return response, ids, docs, results, denseDecodeError(err, "dense response lengths do not match")
	}
	if int(count) <= cap(results) {
		if int(count) < len(results) {
			clear(results[int(count):])
		}
		results = results[:int(count)]
	} else {
		results = make([]DenseVectorSearchResult, int(count))
	}
	for i := range results {
		score := math.Float64frombits(binary.LittleEndian.Uint64(meta[off+i*8:]))
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return response, ids, docs, results, protocolError(iwire.ErrMalformedFrame, "dense response score is not finite")
		}
		results[i] = DenseVectorSearchResult{ID: ids[i], Score: score, Document: docs[i]}
	}
	response.Results = results
	if !response.NativeBasePlusLiveDelta || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 || response.Candidates < len(results) {
		return DenseVectorSearchResponse{}, ids, docs, results, protocolError(iwire.ErrConsistencyUnavailable, "dense response did not prove the native route")
	}
	return response, ids, docs, results, nil
}

func denseDefaultLimits(limits iwire.Limits) iwire.Limits {
	defaults := iwire.DefaultLimits()
	if limits.MaxSectionLen == 0 {
		limits.MaxSectionLen = defaults.MaxSectionLen
	}
	if limits.MaxByteVectorItems <= 0 {
		limits.MaxByteVectorItems = defaults.MaxByteVectorItems
	}
	if limits.MaxDeterministicNameBytes == 0 {
		limits.MaxDeterministicNameBytes = defaults.MaxDeterministicNameBytes
	}
	return limits
}
