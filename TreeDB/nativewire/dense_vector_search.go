package nativewire

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
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
	// TypedColumnGraph requires hello-negotiated dense v2 and selected admission.
	TypedColumnGraph          bool
	Index                     string
	Query                     []float32
	TopK                      int
	EfSearch                  int
	QueryMode                 collections.VectorIndexQueryMode
	QuantizedIndexName        string
	QuantizedRerankCandidates int
	ExpectedGeneration        uint64
	Filter                    *documentservice.Filter
	ReturnEmbedding           bool
}

// DenseVectorSearchResult borrows ID and Document from the client's response
// buffer until its next round trip.
type DenseVectorSearchResult struct {
	ID       []byte
	Score    float64
	Document []byte
}

// DenseVectorSearchResponse and its Results borrow from the client until its
// next round trip. DenseWork and ScorePlane are independently owned and remain
// valid afterward.
type DenseVectorSearchResponse struct {
	DenseWork                 documentservice.DenseSearchWork
	ScorePlane                *collections.ColumnGraphScorePlaneWork
	TypedColumnGraph          bool
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
	if ctx == nil {
		ctx = context.Background()
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return DenseVectorSearchResponse{}, protocolError(iwire.ErrInvalidCommand, "bounded dense search deadline is required")
	}
	mode := request.QueryMode
	if mode == "" {
		mode = collections.VectorIndexQueryModeExact
	}
	if mode == collections.VectorIndexQueryModeExact && (request.QuantizedIndexName != "" || request.QuantizedRerankCandidates != 0) {
		return DenseVectorSearchResponse{}, protocolError(iwire.ErrInvalidCommand, "exact dense search does not accept quantized options")
	}
	if mode == collections.VectorIndexQueryModeQuantizedRerank && request.QuantizedIndexName == "" {
		return DenseVectorSearchResponse{}, protocolError(iwire.ErrInvalidCommand, "quantized dense search requires an index name")
	}
	request.QueryMode = mode
	version := iwire.DenseVectorSearchLegacyVersion
	if request.TypedColumnGraph {
		if request.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
			if !c.denseTypedQuantizedNegotiated {
				return DenseVectorSearchResponse{}, protocolError(iwire.ErrUnsupportedFeature, "hello did not negotiate typed dense quantized search")
			}
			version = iwire.DenseVectorSearchTypedQuantizedVersion
		} else if request.QueryMode != "" && request.QueryMode != collections.VectorIndexQueryModeExact {
			return DenseVectorSearchResponse{}, protocolError(iwire.ErrUnsupportedFeature, "unsupported typed dense query mode")
		} else if !c.denseTypedNegotiated {
			return DenseVectorSearchResponse{}, protocolError(iwire.ErrUnsupportedFeature, "hello did not negotiate typed dense search")
		} else {
			version = iwire.DenseVectorSearchTypedVersion
		}
	} else if request.QueryMode != "" && request.QueryMode != collections.VectorIndexQueryModeExact {
		return DenseVectorSearchResponse{}, protocolError(iwire.ErrUnsupportedFeature, "quantized dense search requires typed column graph")
	}

	payload, err := appendDenseVectorSearchRequest(c.denseRequest[:0], request, c.limits)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	requestSections := []iwire.Section{
		{ID: iwire.SectionDenseSearchRequest, Bytes: payload},
		{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(deadline.UnixNano()))},
	}
	if version == iwire.DenseVectorSearchTypedQuantizedVersion {
		options, optionsErr := appendDenseQuantizedOptions(nil, request, c.limits)
		if optionsErr != nil {
			return DenseVectorSearchResponse{}, optionsErr
		}
		requestSections = append(requestSections, iwire.Section{ID: iwire.SectionDenseSearchQuantizedOptions, Bytes: options})
	}
	body, err := appendVersionedCommandRequestBody(c.requestBody[:0], iwire.CommandDenseVectorSearch, version, requestSections...)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	_, response, err := c.roundTripLockedStreamVersion(ctx, 0, iwire.FrameRequest, body, iwire.FrameResponse, version)
	c.denseRequest = retainSmallPayloadScratch(payload)
	c.requestBody = retainSmallPayloadScratch(body)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	decoded := false
	defer func() {
		if !decoded {
			c.clearBorrowedResponseViews()
			c.readBody = retainSmallPayloadScratch(c.readBody)
		}
	}()
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
	out, c.denseIDs, c.denseDocuments, c.denseResults, err = decodeVersionedDenseVectorSearchResponse(
		version, ids, docs, meta, request.TopK, c.limits, c.denseIDs, c.denseDocuments, c.denseResults,
	)
	if err == nil {
		out.DenseWork, err = decodeDenseWorkSectionVersion(c.vectorSections, version)
	}
	if err == nil && version == iwire.DenseVectorSearchTypedQuantizedVersion {
		out.ScorePlane, err = decodeDenseScorePlaneSection(c.vectorSections, true, c.limits)
		if err == nil {
			err = validateDenseQuantizedScorePlaneResponse(out.DenseWork, out.ScorePlane, request, len(out.Results))
		}
	}
	if err == nil && version >= iwire.DenseVectorSearchTypedVersion {
		err = validateDenseWorkResults(out.DenseWork, out.Results)
	}
	decoded = err == nil
	if err != nil {
		var work *documentservice.DenseSearchWork
		if out.DenseWork.Version != 0 {
			owned := out.DenseWork
			work = &owned
		}
		return DenseVectorSearchResponse{}, &DenseVectorSearchDecodeError{Err: err, DenseWork: work, ScorePlane: out.ScorePlane}
	}
	return out, err
}

func validateDenseQuantizedScorePlaneResponse(work documentservice.DenseSearchWork, proof *collections.ColumnGraphScorePlaneWork, request DenseVectorSearchRequest, resultCount int) error {
	graphRoute := ""
	if work.Graph.Available {
		graphRoute = work.Graph.Route
	}
	expectedGraphRoute := ""
	switch {
	case proof != nil && proof.Route == "typed_empty":
		expectedGraphRoute = "typed_empty"
	case proof != nil && proof.Route == "typed_exact":
		expectedGraphRoute = "typed_exact"
	case proof != nil && proof.Route == "quantized_rerank":
		expectedGraphRoute = "typed_hnsw"
	}
	if proof == nil || !proof.Available || !proof.Completed || !proof.Snapshot.Available ||
		proof.RequestedMode != request.QueryMode || proof.EffectiveMode != collections.VectorIndexQueryModeQuantizedRerank ||
		(proof.Route != "typed_empty" && proof.Route != "typed_exact" && proof.Route != "quantized_rerank") ||
		!work.Completed || !work.Graph.Completed || graphRoute != expectedGraphRoute ||
		proof.Snapshot != work.Graph.Snapshot ||
		!denseScorePlaneRerankCountersMatch(proof) ||
		!denseScorePlaneByteCountersMatch(proof, uint64(len(request.Query))) ||
		!denseScorePlaneCountersMatchWork(work, proof, resultCount) ||
		resultCount > request.TopK ||
		(proof.Route == "typed_empty" && resultCount != 0) ||
		(proof.Route == "quantized_rerank" && (proof.ActualRerankCandidates > proof.RerankCandidateCap ||
			proof.LiveShortlistCandidates > proof.RawRetainedCandidates ||
			proof.RawRetainedCandidates > proof.RawCandidateWidth ||
			proof.ActualRerankCandidates > proof.LiveShortlistCandidates ||
			(request.QuantizedRerankCandidates != 0 && proof.RerankCandidateCap > uint64(request.QuantizedRerankCandidates)))) ||
		proof.QuantizedIndexName != request.QuantizedIndexName || proof.RequestedTopK != uint64(request.TopK) ||
		proof.RequestedEFSearch != uint64(request.EfSearch) || proof.RequestedRerankCandidates != uint64(request.QuantizedRerankCandidates) {
		return protocolError(iwire.ErrConsistencyUnavailable, "dense score-plane proof does not match the request")
	}
	return nil
}

func denseScorePlaneCountersMatchWork(work documentservice.DenseSearchWork, proof *collections.ColumnGraphScorePlaneWork, resultCount int) bool {
	if proof == nil || resultCount < 0 || proof.ExactSmallFilterScoreCalls > ^uint64(0)-proof.ExactBaseRerankScoreCalls {
		return false
	}
	exactBaseScoreCalls := proof.ExactBaseRerankScoreCalls + proof.ExactSmallFilterScoreCalls
	if proof.ExactSuffixScoreCalls > ^uint64(0)-exactBaseScoreCalls {
		return false
	}
	return proof.QuantizedScoreCalls == work.Graph.BaseANNScored &&
		exactBaseScoreCalls == work.Graph.ExactBaseScored &&
		proof.ExactSuffixScoreCalls == work.Graph.DeltaScored &&
		uint64(resultCount) <= exactBaseScoreCalls+proof.ExactSuffixScoreCalls
}

func denseScorePlaneByteCountersMatch(proof *collections.ColumnGraphScorePlaneWork, dimension uint64) bool {
	if proof == nil || dimension == 0 {
		return false
	}
	if proof.QuantizedScoreCalls > ^uint64(0)/dimension {
		return false
	}
	if proof.QuantizedCodeBytesRead != proof.QuantizedScoreCalls*dimension {
		return false
	}
	if dimension > ^uint64(0)/4 {
		return false
	}
	bytesPerExact := dimension * 4
	if proof.ExactSmallFilterScoreCalls > ^uint64(0)-proof.ExactBaseRerankScoreCalls {
		return false
	}
	exactBaseCalls := proof.ExactBaseRerankScoreCalls + proof.ExactSmallFilterScoreCalls
	if exactBaseCalls > ^uint64(0)/bytesPerExact || proof.ExactSuffixScoreCalls > ^uint64(0)/bytesPerExact {
		return false
	}
	return proof.ExactBaseVectorBytesRead == exactBaseCalls*bytesPerExact &&
		proof.ExactSuffixVectorBytesRead == proof.ExactSuffixScoreCalls*bytesPerExact
}

func (s *Server) handleDenseVectorSearch(ctx context.Context, state *connState, sections []iwire.Section, dst []byte) ([]byte, error) {
	return s.handleVersionedDenseVectorSearch(ctx, state, iwire.DenseVectorSearchLegacyVersion, sections, dst)
}

func (s *Server) handleVersionedDenseVectorSearch(ctx context.Context, state *connState, version uint64, sections []iwire.Section, dst []byte) (_ []byte, err error) {
	defer clearDenseVectorSearchScratch(state)
	var proof *documentservice.DenseSearchWork
	var scorePlane *collections.ColumnGraphScorePlaneWork
	defer func() {
		if err != nil && version >= iwire.DenseVectorSearchTypedVersion {
			if proof == nil {
				proof = denseServiceWork(err)
			}
			if version == iwire.DenseVectorSearchTypedQuantizedVersion && scorePlane == nil {
				scorePlane = denseServiceScorePlane(err)
			}
			if proof != nil {
				err = &denseWorkError{error: err, work: *proof, scorePlane: scorePlane, version: version}
			}
		}
	}()
	deadline, err := deadlineUnixNanosFromSections(sections)
	if err != nil {
		return nil, err
	}
	if deadline <= 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "positive dense search deadline is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, deadline))
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.documentService == nil {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "dense document service is not configured")
	}
	sectionCount := 3
	if version >= iwire.DenseVectorSearchTypedVersion {
		sectionCount++
	}
	if version == iwire.DenseVectorSearchTypedQuantizedVersion {
		sectionCount++
	}
	if err := s.checkResponseSectionCount(sectionCount); err != nil {
		return nil, err
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
	if version == iwire.DenseVectorSearchTypedQuantizedVersion {
		optionsRaw, optionsOK, optionsErr := singletonSection(sections, iwire.SectionDenseSearchQuantizedOptions)
		if optionsErr != nil || !optionsOK {
			if optionsErr == nil {
				optionsErr = protocolError(iwire.ErrInvalidCommand, "dense quantized options missing")
			}
			return nil, optionsErr
		}
		options, optionsErr := decodeDenseQuantizedOptions(optionsRaw, s.limits)
		if optionsErr != nil {
			return nil, optionsErr
		}
		request.QueryMode = options.QueryMode
		request.QuantizedIndexName = options.QuantizedIndexName
		request.QuantizedRerankCandidates = options.QuantizedRerankCandidates
	}
	response, err := s.documentService.SearchDenseVectorNativeRawInto(ctx, request.Index, documentservice.DenseVectorSearchRequest{
		ExpectedGeneration:        request.ExpectedGeneration,
		QueryEmbedding:            request.Query,
		TopK:                      request.TopK,
		EfSearch:                  request.EfSearch,
		QueryMode:                 request.QueryMode,
		QuantizedIndexName:        request.QuantizedIndexName,
		QuantizedRerankCandidates: request.QuantizedRerankCandidates,
		Route:                     documentservice.RouteAnn,
		Filter:                    request.Filter,
		ReturnEmbedding:           request.ReturnEmbedding,
	}, state.denseResults[:0])
	if err != nil {
		return nil, err
	}
	proof = response.DenseWork
	scorePlane = response.ScorePlane
	state.denseResults = response.Results[:0]
	if response.TypedColumnGraph != (version >= iwire.DenseVectorSearchTypedVersion) {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "dense command version does not match index strategy")
	}

	state.idsScratch = resizeByteSlices(state.idsScratch, len(response.Results))
	state.docsScratch = resizeByteSlices(state.docsScratch, len(response.Results))
	for i := range response.Results {
		state.idsScratch[i] = response.Results[i].ID
		state.docsScratch[i] = response.Results[i].Document
	}
	state.denseMeta = appendDenseVectorSearchResponse(state.denseMeta[:0], response)
	if version >= iwire.DenseVectorSearchTypedVersion {
		state.denseMeta[0] = iwire.DenseVectorSearchTypedRouteTag
		if version == iwire.DenseVectorSearchTypedQuantizedVersion {
			state.denseMeta[0] = iwire.DenseVectorSearchTypedQuantizedRouteTag
		}
	}
	var proofBytes []byte
	var proofScratch [380]byte
	var scorePlaneBytes []byte
	var scorePlaneScratch [2048]byte
	if version >= iwire.DenseVectorSearchTypedVersion {
		if proof == nil || !proof.Completed {
			return nil, protocolError(iwire.ErrConsistencyUnavailable, "selected dense work is unavailable")
		}
		proofBytes, err = appendDenseWork(proofScratch[:0], *proof)
		if err != nil {
			return nil, err
		}
		if err := s.checkResponseSectionLen("dense work", len(proofBytes)); err != nil {
			return nil, err
		}
	}
	if version == iwire.DenseVectorSearchTypedQuantizedVersion {
		if scorePlane == nil || !scorePlane.Available || !scorePlane.Completed {
			return nil, protocolError(iwire.ErrConsistencyUnavailable, "selected dense score-plane proof is unavailable")
		}
		scorePlaneBytes, err = appendDenseScorePlane(scorePlaneScratch[:0], *scorePlane, s.limits)
		if err != nil {
			return nil, err
		}
		if err := s.checkResponseSectionLen("dense score-plane proof", len(scorePlaneBytes)); err != nil {
			return nil, err
		}
	}
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
	if proofBytes != nil {
		sectionLen, err := responseSectionBodyLen(iwire.SectionDenseSearchWork, len(proofBytes))
		if err != nil {
			return nil, err
		}
		bodyLen, err = addResponseLen(bodyLen, sectionLen)
		if err != nil {
			return nil, err
		}
	}
	if scorePlaneBytes != nil {
		sectionLen, err := responseSectionBodyLen(iwire.SectionDenseSearchScorePlaneProof, len(scorePlaneBytes))
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
	if proofBytes != nil {
		dst, err = iwire.AppendSection(dst, iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: proofBytes})
		if err != nil {
			return nil, err
		}
	}
	if scorePlaneBytes != nil {
		dst, err = iwire.AppendSection(dst, iwire.Section{ID: iwire.SectionDenseSearchScorePlaneProof, Flags: iwire.SectionFlagCritical, Bytes: scorePlaneBytes})
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func clearDenseVectorSearchScratch(state *connState) {
	state.denseMeta = retainSmallPayloadScratch(state.denseMeta)
	clear(state.denseResults[:cap(state.denseResults)])
	if cap(state.denseResults) > maxRetainedGetManyScratchItems {
		state.denseResults = nil
	} else {
		state.denseResults = state.denseResults[:0]
	}
	clear(state.idsScratch[:cap(state.idsScratch)])
	if cap(state.idsScratch) > maxRetainedGetManyScratchItems {
		state.idsScratch = nil
	} else {
		state.idsScratch = state.idsScratch[:0]
	}
	clear(state.docsScratch[:cap(state.docsScratch)])
	if cap(state.docsScratch) > maxRetainedGetManyScratchItems {
		state.docsScratch = nil
	} else {
		state.docsScratch = state.docsScratch[:0]
	}
	if cap(state.vectorQuery) > maxRetainedGetManyScratchItems {
		state.vectorQuery = nil
	} else {
		state.vectorQuery = state.vectorQuery[:0]
	}
	clear(state.denseFilters[:cap(state.denseFilters)])
	state.denseFilters = state.denseFilters[:0]
	state.denseFilter = documentservice.Filter{}
}

func retainSmallPayloadScratch(payload []byte) []byte {
	if cap(payload) > maxRetainedGetManyPayloadBytes {
		return nil
	}
	return payload[:0]
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
	if request.EfSearch > limits.MaxByteVectorItems {
		return nil, protocolError(iwire.ErrResourceExhausted, "dense ef_search %d exceeds limit %d", request.EfSearch, limits.MaxByteVectorItems)
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

const denseQuantizedOptionsVersion = uint64(1)

func appendDenseQuantizedOptions(dst []byte, request DenseVectorSearchRequest, limits iwire.Limits) ([]byte, error) {
	limits = denseDefaultLimits(limits)
	if request.QueryMode != collections.VectorIndexQueryModeQuantizedRerank {
		return nil, protocolError(iwire.ErrInvalidCommand, "typed dense quantized options require query_mode quantized_rerank")
	}
	if request.QuantizedIndexName == "" {
		return nil, protocolError(iwire.ErrInvalidCommand, "typed dense quantized options require quantized_index_name")
	}
	if !utf8.ValidString(request.QuantizedIndexName) {
		return nil, protocolError(iwire.ErrInvalidCommand, "typed dense quantized index name must be valid UTF-8")
	}
	if request.QuantizedRerankCandidates < 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "typed dense quantized rerank candidates must be non-negative")
	}
	if request.QuantizedRerankCandidates != 0 && request.QuantizedRerankCandidates < request.TopK {
		return nil, protocolError(iwire.ErrInvalidCommand, "typed dense quantized rerank candidates must be zero or at least top_k")
	}
	if request.QuantizedRerankCandidates > limits.MaxByteVectorItems {
		return nil, protocolError(iwire.ErrResourceExhausted, "typed dense quantized rerank candidates exceed limit")
	}
	if uint64(len(request.QuantizedIndexName)) > limits.MaxDeterministicNameBytes {
		return nil, protocolError(iwire.ErrResourceExhausted, "typed dense quantized index name exceeds limit")
	}
	dst = binary.AppendUvarint(dst, denseQuantizedOptionsVersion)
	dst = binary.AppendUvarint(dst, 1) // quantized_rerank mode
	dst = binary.AppendUvarint(dst, uint64(len(request.QuantizedIndexName)))
	dst = append(dst, request.QuantizedIndexName...)
	dst = binary.AppendUvarint(dst, uint64(request.QuantizedRerankCandidates))
	if uint64(len(dst)) > limits.MaxSectionLen {
		return nil, protocolError(iwire.ErrResourceExhausted, "typed dense quantized options exceed section limit")
	}
	return dst, nil
}

func decodeDenseQuantizedOptions(raw []byte, limits iwire.Limits) (DenseVectorSearchRequest, error) {
	limits = denseDefaultLimits(limits)
	off := 0
	version, err := readUvarintField(raw, &off, "dense quantized options version")
	if err != nil || version != denseQuantizedOptionsVersion {
		if err != nil {
			return DenseVectorSearchRequest{}, err
		}
		return DenseVectorSearchRequest{}, protocolError(iwire.ErrUnsupportedVersion, "unsupported dense quantized options version %d", version)
	}
	mode, err := readUvarintField(raw, &off, "dense quantized options mode")
	if err != nil || mode != 1 {
		if err != nil {
			return DenseVectorSearchRequest{}, err
		}
		return DenseVectorSearchRequest{}, protocolError(iwire.ErrUnsupportedFeature, "unsupported dense quantized options mode")
	}
	name, err := readDenseString(raw, &off, limits.MaxDeterministicNameBytes, "dense quantized index name")
	if err != nil || name == "" {
		if err != nil {
			return DenseVectorSearchRequest{}, err
		}
		return DenseVectorSearchRequest{}, protocolError(iwire.ErrInvalidCommand, "dense quantized index name is empty")
	}
	if !utf8.ValidString(name) {
		return DenseVectorSearchRequest{}, protocolError(iwire.ErrMalformedFrame, "dense quantized index name must be valid UTF-8")
	}
	r, err := readDenseInt(raw, &off, "dense quantized rerank candidates")
	if err != nil {
		return DenseVectorSearchRequest{}, err
	}
	if r > limits.MaxByteVectorItems {
		return DenseVectorSearchRequest{}, protocolError(iwire.ErrResourceExhausted, "dense quantized rerank candidates exceed limit")
	}
	if off != len(raw) {
		return DenseVectorSearchRequest{}, protocolError(iwire.ErrMalformedFrame, "dense quantized options have trailing bytes")
	}
	return DenseVectorSearchRequest{QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: name, QuantizedRerankCandidates: r}, nil
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
	if efSearch > limits.MaxByteVectorItems {
		return request, leaves, protocolError(iwire.ErrResourceExhausted, "dense ef_search %d exceeds limit %d", efSearch, limits.MaxByteVectorItems)
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

func decodeDenseVectorSearchResponse(idsRaw, docsRaw, meta []byte, topK int, limits iwire.Limits, ids, docs [][]byte, results []DenseVectorSearchResult) (DenseVectorSearchResponse, [][]byte, [][]byte, []DenseVectorSearchResult, error) {
	return decodeVersionedDenseVectorSearchResponse(iwire.DenseVectorSearchLegacyVersion, idsRaw, docsRaw, meta, topK, limits, ids, docs, results)
}

func decodeVersionedDenseVectorSearchResponse(version uint64, idsRaw, docsRaw, meta []byte, topK int, limits iwire.Limits, ids, docs [][]byte, results []DenseVectorSearchResult) (DenseVectorSearchResponse, [][]byte, [][]byte, []DenseVectorSearchResult, error) {
	limits = denseDefaultLimits(limits)
	var response DenseVectorSearchResponse
	var err error
	if version != iwire.DenseVectorSearchLegacyVersion && version != iwire.DenseVectorSearchTypedVersion && version != iwire.DenseVectorSearchTypedQuantizedVersion {
		return response, ids, docs, results, protocolError(iwire.ErrUnsupportedVersion, "unsupported dense search response version %d", version)
	}
	typed := version >= iwire.DenseVectorSearchTypedVersion
	validTypedTag := metaTagForDenseVersion(version)
	if topK <= 0 || len(meta) == 0 || (typed && meta[0] != validTypedTag) || (!typed && (version != iwire.DenseVectorSearchLegacyVersion || meta[0] > 1)) {
		return response, ids, docs, results, protocolError(iwire.ErrMalformedFrame, "dense route proof is invalid")
	}
	response.Route = documentservice.RouteAnn
	response.NativeBasePlusLiveDelta = meta[0] == 1
	response.TypedColumnGraph = typed
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
	if err != nil || count > uint64(topK) || count > uint64(limits.MaxByteVectorItems) || count > uint64((len(meta)-off)/8) || len(meta)-off != int(count)*8 {
		return response, ids, docs, results, denseDecodeError(err, "dense response lengths do not match")
	}
	ids, err = iwire.DecodeByteVectorItemsInto(ids[:0], idsRaw, limits)
	if err != nil {
		return response, ids, docs, results, err
	}
	docs, err = iwire.DecodeByteVectorItemsInto(docs[:0], docsRaw, limits)
	if err != nil {
		return response, ids, docs, results, err
	}
	if count != uint64(len(ids)) || len(ids) != len(docs) {
		return response, ids, docs, results, protocolError(iwire.ErrMalformedFrame, "dense response lengths do not match")
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
	if (!typed && !response.NativeBasePlusLiveDelta) || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 || response.Candidates < len(results) {
		return DenseVectorSearchResponse{}, ids, docs, results, protocolError(iwire.ErrConsistencyUnavailable, "dense response did not prove the native route")
	}
	return response, ids, docs, results, nil
}

func metaTagForDenseVersion(version uint64) byte {
	if version == iwire.DenseVectorSearchTypedQuantizedVersion {
		return iwire.DenseVectorSearchTypedQuantizedRouteTag
	}
	return iwire.DenseVectorSearchTypedRouteTag
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
