package nativewire

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// VectorPartitionRouteV1 identifies the typed vector commands carried by the
// existing TreeDB native-wire connection.
const VectorPartitionRouteV1 = "treedb.nativewire.vector_search_v1"

// VectorPartitionStatusV1 binds live vector readiness to the serving node.
type VectorPartitionStatusV1 struct {
	NodeConfigSHA256 string
	Health           public.OperationsHealthV1
}

// VectorStatusV1 reads live vector serving status from the connected node.
func (c *Client) VectorStatusV1(ctx context.Context) (VectorPartitionStatusV1, error) {
	if c == nil {
		return VectorPartitionStatusV1{}, io.ErrClosedPipe
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sections, err := c.vectorCommandLockedV1(ctx, iwire.CommandVectorStatus, nil, nil, nil)
	if err != nil {
		return VectorPartitionStatusV1{}, err
	}
	raw, ok, err := singletonSection(sections, iwire.SectionVectorStatus)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "vector status response is missing")
		}
		return VectorPartitionStatusV1{}, err
	}
	return decodeVectorPartitionStatusV1(raw)
}

// VectorSearchStrictV1 executes the strict proof-carrying search shape.
func (c *Client) VectorSearchStrictV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	response, _, err := c.vectorSearchCommandV1(ctx, iwire.CommandVectorSearchStrict, request, nil)
	return response, err
}

// VectorSearchFastV1 executes the bounded immutable-snapshot search shape.
func (c *Client) VectorSearchFastV1(ctx context.Context, request public.SearchRequestV1, options public.FastSearchOptionsV1) (public.SearchResponseV1, public.FastSearchEvidenceV1, error) {
	return c.vectorSearchCommandV1(ctx, iwire.CommandVectorSearchFast, request, &options)
}

// VectorPinSearchSnapshotV1 pins one bounded immutable snapshot on this
// connection. A connection owns at most one pin.
func (c *Client) VectorPinSearchSnapshotV1(ctx context.Context, options public.PinSearchSnapshotOptionsV1) (public.FastSearchEvidenceV1, error) {
	if c == nil {
		return public.FastSearchEvidenceV1{}, io.ErrClosedPipe
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sections, err := c.vectorCommandLockedV1(ctx, iwire.CommandVectorPinSearchSnapshot, nil, nil, &options)
	if err != nil {
		return public.FastSearchEvidenceV1{}, err
	}
	raw, ok, err := singletonSection(sections, iwire.SectionVectorFastEvidence)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "vector pin response evidence is missing")
		}
		return public.FastSearchEvidenceV1{}, err
	}
	evidence, err := decodeVectorPartitionFastEvidenceV1(raw)
	if err == nil {
		err = validateVectorPartitionFastEvidenceV1(evidence, public.GenerationIDV1{}, options.FastSearchOptionsV1)
	}
	return evidence, err
}

// VectorSearchPinnedV1 searches the snapshot pinned on this connection.
func (c *Client) VectorSearchPinnedV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	response, _, err := c.vectorSearchCommandV1(ctx, iwire.CommandVectorSearchPinned, request, nil)
	return response, err
}

// VectorClosePinnedSnapshotV1 releases this connection's pinned snapshot.
func (c *Client) VectorClosePinnedSnapshotV1(ctx context.Context) error {
	if c == nil {
		return io.ErrClosedPipe
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.vectorCommandLockedV1(ctx, iwire.CommandVectorClosePinnedSnapshot, nil, nil, nil)
	return err
}

func (c *Client) vectorSearchCommandV1(ctx context.Context, command iwire.CommandID, request public.SearchRequestV1, options *public.FastSearchOptionsV1) (public.SearchResponseV1, public.FastSearchEvidenceV1, error) {
	if c == nil {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, io.ErrClosedPipe
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sections, err := c.vectorCommandLockedV1(ctx, command, &request, options, nil)
	if err != nil {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, err
	}
	rawResponse, ok, err := singletonSection(sections, iwire.SectionVectorSearchResponse)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "vector search response is missing")
		}
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, err
	}
	response, err := decodeVectorPartitionSearchResponseV1(rawResponse, c.limits)
	if err != nil {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, err
	}
	if response.Generation != request.Generation || len(response.Neighbors) > request.TopK {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, protocolError(iwire.ErrMalformedFrame, "vector search response does not match request")
	}
	if command != iwire.CommandVectorSearchFast {
		return response, public.FastSearchEvidenceV1{}, nil
	}
	rawEvidence, ok, err := singletonSection(sections, iwire.SectionVectorFastEvidence)
	if err != nil || !ok {
		if err == nil {
			err = protocolError(iwire.ErrMalformedFrame, "vector fast search evidence is missing")
		}
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, err
	}
	evidence, err := decodeVectorPartitionFastEvidenceV1(rawEvidence)
	if err == nil {
		err = validateVectorPartitionFastEvidenceV1(evidence, request.Generation, *options)
	}
	return response, evidence, err
}

func (c *Client) vectorCommandLockedV1(ctx context.Context, command iwire.CommandID, request *public.SearchRequestV1, fast *public.FastSearchOptionsV1, pin *public.PinSearchSnapshotOptionsV1) ([]iwire.Section, error) {
	var deadline time.Time
	if ctx != nil {
		deadline, _ = ctx.Deadline()
	}
	if request != nil && !request.Deadline.IsZero() && (deadline.IsZero() || request.Deadline.Before(deadline)) {
		deadline = request.Deadline
	}
	if deadline.IsZero() {
		return nil, vectorPartitionClientErrorV1(protocolError(iwire.ErrInvalidCommand, "bounded vector command deadline is required"))
	}
	body, err := appendVectorPartitionCommandBodyV1(c.requestBody[:0], command, request, fast, pin, deadline, c.limits)
	if err != nil {
		return nil, vectorPartitionClientErrorV1(err)
	}
	_, response, err := c.roundTripLocked(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err != nil {
		return nil, vectorPartitionClientErrorV1(err)
	}
	c.vectorSections, err = iwire.DecodeSectionsInto(c.vectorSections[:0], response, c.limits)
	return c.vectorSections, err
}

func validateVectorPartitionFastEvidenceV1(evidence public.FastSearchEvidenceV1, generation public.GenerationIDV1, options public.FastSearchOptionsV1) error {
	if evidence.Generation.Index == "" || evidence.Generation.Generation == 0 || evidence.PublishedAt.IsZero() || evidence.IndexAge < 0 || evidence.TopologyDigest == "" || evidence.AuthorizationOverlayDigest == "" {
		return protocolError(iwire.ErrMalformedFrame, "vector fast evidence is incomplete")
	}
	if generation.Index != "" && evidence.Generation != generation {
		return protocolError(iwire.ErrCatalogChanged, "vector fast evidence generation changed")
	}
	if evidence.IndexedThrough < options.MinIndexedThrough || evidence.IndexAge > options.MaxIndexAge {
		return protocolError(iwire.ErrConsistencyUnavailable, "vector fast evidence is stale")
	}
	return nil
}

func (s *Server) handleVectorPartitionCommandV1(ctx context.Context, state *connState, cmd iwire.ValidatedCommand, dst []byte) ([]byte, error) {
	if s.vectorPartitionOperations == nil || !s.vectorPartitionOperations.Enabled() {
		return nil, protocolError(iwire.ErrConsistencyUnavailable, "vector partition operations are unavailable")
	}
	deadline, err := deadlineUnixNanosFromSections(cmd.Known)
	if err != nil {
		return nil, err
	}
	if deadline <= 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "positive vector command deadline is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithDeadline(ctx, time.Unix(0, deadline))
	defer cancel()
	if state != nil && cmd.Header.ID != iwire.CommandVectorStatus {
		state.mu.Lock()
		defer state.mu.Unlock()
	}
	switch cmd.Header.ID {
	case iwire.CommandVectorStatus:
		health, err := s.vectorPartitionOperations.Status(ctx)
		if err != nil {
			return nil, vectorPartitionServerErrorV1(err)
		}
		return appendVectorPartitionStatusSectionV1(dst, VectorPartitionStatusV1{NodeConfigSHA256: s.vectorPartitionNodeConfigSHA256, Health: health})
	case iwire.CommandVectorSearchStrict, iwire.CommandVectorSearchFast, iwire.CommandVectorSearchPinned:
		raw, ok, err := singletonSection(cmd.Known, iwire.SectionVectorSearchRequest)
		if err != nil || !ok {
			if err == nil {
				err = protocolError(iwire.ErrInvalidCommand, "vector search request is missing")
			}
			return nil, err
		}
		var query []float32
		if state != nil {
			query = state.vectorQuery[:0]
		}
		request, err := decodeVectorPartitionSearchRequestIntoV1(raw, s.limits, query)
		if state != nil && request.Query != nil {
			state.vectorQuery = request.Query[:0]
		}
		if err != nil {
			return nil, err
		}
		var response public.SearchResponseV1
		var evidence public.FastSearchEvidenceV1
		switch cmd.Header.ID {
		case iwire.CommandVectorSearchStrict:
			response, err = s.vectorPartitionOperations.Search(ctx, request)
		case iwire.CommandVectorSearchFast:
			rawOptions, found, findErr := singletonSection(cmd.Known, iwire.SectionVectorFastOptions)
			if findErr != nil || !found {
				if findErr == nil {
					findErr = protocolError(iwire.ErrInvalidCommand, "vector fast options are missing")
				}
				return nil, findErr
			}
			options, decodeErr := decodeVectorPartitionFastOptionsV1(rawOptions)
			if decodeErr != nil {
				return nil, decodeErr
			}
			response, evidence, err = s.vectorPartitionOperations.SearchFast(ctx, request, options)
		case iwire.CommandVectorSearchPinned:
			if state == nil || state.vectorPinned == nil {
				return nil, protocolError(iwire.ErrConsistencyUnavailable, "pinned vector snapshot is unavailable")
			}
			response, err = state.vectorPinned.Search(ctx, request)
		}
		if err != nil {
			return nil, vectorPartitionServerErrorV1(err)
		}
		dst, err = appendVectorPartitionSearchResponseSectionV1(dst, response)
		if err == nil && cmd.Header.ID == iwire.CommandVectorSearchFast {
			dst, err = appendVectorPartitionFastEvidenceSectionV1(dst, evidence)
		}
		return dst, err
	case iwire.CommandVectorPinSearchSnapshot:
		if state == nil || state.vectorPinned != nil {
			return nil, protocolError(iwire.ErrInvalidCommand, "connection already owns a pinned vector snapshot")
		}
		raw, ok, err := singletonSection(cmd.Known, iwire.SectionVectorPinOptions)
		if err != nil || !ok {
			if err == nil {
				err = protocolError(iwire.ErrInvalidCommand, "vector pin options are missing")
			}
			return nil, err
		}
		options, err := decodeVectorPartitionPinOptionsV1(raw)
		if err != nil {
			return nil, err
		}
		pinned, err := s.vectorPartitionOperations.PinSearchSnapshot(ctx, options)
		if err != nil {
			return nil, vectorPartitionServerErrorV1(err)
		}
		response, err := appendVectorPartitionFastEvidenceSectionV1(dst, pinned.Evidence())
		if err != nil {
			_ = pinned.Close()
			return nil, err
		}
		state.vectorPinned = pinned
		return response, nil
	case iwire.CommandVectorClosePinnedSnapshot:
		if state == nil {
			return nil, protocolError(iwire.ErrInvalidCommand, "connection state is unavailable")
		}
		return dst, vectorPartitionServerErrorV1(state.closeVectorPinnedLocked())
	default:
		return nil, protocolError(iwire.ErrUnsupportedFeature, "unsupported vector command")
	}
}

func vectorPartitionServerErrorV1(err error) error {
	if err == nil {
		return nil
	}
	var typed *public.ErrorV1
	if errors.As(err, &typed) {
		code := iwire.ErrInternal
		switch typed.Code {
		case public.ErrorInvalidRequestV1:
			code = iwire.ErrInvalidCommand
		case public.ErrorGenerationMismatchV1:
			code = iwire.ErrCatalogChanged
		case public.ErrorUnavailableV1:
			code = iwire.ErrConsistencyUnavailable
		case public.ErrorCanceledV1:
			code = iwire.ErrCanceled
		case public.ErrorDeadlineExceededV1:
			code = iwire.ErrTimeout
		}
		if code == iwire.ErrInternal {
			logDebug("vector operation failed: %v", err)
			return protocolError(code, "vector operation failed")
		}
		return protocolError(code, "%s", err)
	}
	logDebug("vector operation failed: %v", err)
	return protocolError(iwire.ErrInternal, "vector operation failed")
}

func vectorPartitionClientErrorV1(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return &public.ErrorV1{Code: public.ErrorCanceledV1, Err: context.Canceled}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &public.ErrorV1{Code: public.ErrorDeadlineExceededV1, Err: context.DeadlineExceeded}
	}
	var wire *WireError
	wireCode, cause := iwire.ErrorCode(0), err
	if errors.As(err, &wire) {
		wireCode, cause = wire.Code, errors.New(wire.Message)
	} else if code, ok := iwire.ErrorCodeOf(err); ok {
		wireCode = code
	} else {
		return err
	}
	code := public.ErrorFailedV1
	switch wireCode {
	case iwire.ErrInvalidCommand, iwire.ErrMalformedFrame:
		code = public.ErrorInvalidRequestV1
	case iwire.ErrResourceExhausted:
		code = public.ErrorUnavailableV1
	case iwire.ErrCatalogChanged:
		code = public.ErrorGenerationMismatchV1
	case iwire.ErrConsistencyUnavailable:
		code = public.ErrorUnavailableV1
	case iwire.ErrCanceled:
		code, cause = public.ErrorCanceledV1, context.Canceled
	case iwire.ErrTimeout:
		code, cause = public.ErrorDeadlineExceededV1, context.DeadlineExceeded
	}
	return &public.ErrorV1{Code: code, Err: cause}
}

func appendVectorPartitionCommandBodyV1(dst []byte, command iwire.CommandID, request *public.SearchRequestV1, fast *public.FastSearchOptionsV1, pin *public.PinSearchSnapshotOptionsV1, deadline time.Time, limits iwire.Limits) ([]byte, error) {
	body, err := appendCommandHeaderSection(dst, command)
	if err != nil {
		return nil, err
	}
	if !deadline.IsZero() {
		if deadline.UnixNano() <= 0 {
			return nil, protocolError(iwire.ErrInvalidCommand, "vector command deadline cannot be encoded")
		}
		body, err = iwire.AppendSectionHeader(body, iwire.SectionDeadline, 0, uvarintLen(uint64(deadline.UnixNano())))
		if err == nil {
			body = binary.AppendUvarint(body, uint64(deadline.UnixNano()))
		}
	}
	if request != nil {
		if err == nil {
			body, err = appendVectorPartitionSearchRequestSectionV1(body, *request, limits)
		}
	}
	if err == nil && fast != nil {
		body, err = appendVectorPartitionFastOptionsSectionV1(body, *fast)
	}
	if err == nil && pin != nil {
		body, err = appendVectorPartitionPinOptionsSectionV1(body, *pin)
	}
	return body, err
}

func appendVectorPartitionSearchRequestSectionV1(dst []byte, request public.SearchRequestV1, limits iwire.Limits) ([]byte, error) {
	if request.Version != 1 || len(request.Query) == 0 || len(request.Query) > limits.MaxByteVectorItems || len(request.Query) > maxInt/4 || request.TopK < 0 || request.Probes < 0 || request.EfSearch < 0 || request.Limits.MergeEntries < 0 || request.Metric != public.MetricCosineV1 || request.Consistency != public.ConsistencyGenerationSnapshotV1 {
		return nil, protocolError(iwire.ErrInvalidCommand, "vector search request cannot be encoded")
	}
	deadline := uint64(0)
	if !request.Deadline.IsZero() {
		if request.Deadline.UnixNano() <= 0 {
			return nil, protocolError(iwire.ErrInvalidCommand, "vector search deadline cannot be encoded")
		}
		deadline = uint64(request.Deadline.UnixNano())
	}
	payloadLen := uvarintLen(uint64(request.Version)) + encodedStringLenV1(request.Generation.Index) + uvarintLen(request.Generation.Generation) + uvarintLen(uint64(len(request.Query))) + 4*len(request.Query) +
		uvarintLen(1) + uvarintLen(uint64(request.TopK)) + uvarintLen(uint64(request.Probes)) + uvarintLen(uint64(request.EfSearch)) + uvarintLen(1) +
		uvarintLen(request.Limits.RequestBytes) + uvarintLen(request.Limits.CandidateBytes) + uvarintLen(request.Limits.ResponseBytes) + uvarintLen(uint64(request.Limits.MergeEntries)) + uvarintLen(deadline)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorSearchRequest, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, uint64(request.Version))
	body = appendString(body, request.Generation.Index)
	body = binary.AppendUvarint(body, request.Generation.Generation)
	body = binary.AppendUvarint(body, uint64(len(request.Query)))
	for _, value := range request.Query {
		body = binary.LittleEndian.AppendUint32(body, math.Float32bits(value))
	}
	for _, value := range []uint64{1, uint64(request.TopK), uint64(request.Probes), uint64(request.EfSearch), 1, request.Limits.RequestBytes, request.Limits.CandidateBytes, request.Limits.ResponseBytes, uint64(request.Limits.MergeEntries), deadline} {
		body = binary.AppendUvarint(body, value)
	}
	return body, nil
}

func decodeVectorPartitionSearchRequestV1(src []byte, limits iwire.Limits) (public.SearchRequestV1, error) {
	return decodeVectorPartitionSearchRequestIntoV1(src, limits, nil)
}

func decodeVectorPartitionSearchRequestIntoV1(src []byte, limits iwire.Limits, query []float32) (public.SearchRequestV1, error) {
	r := vectorPartitionWireReaderV1{src: src}
	request := public.SearchRequestV1{}
	version := r.u64()
	if version > math.MaxUint32 && r.err == nil {
		r.err = protocolError(iwire.ErrInvalidCommand, "vector request version overflows uint32")
	} else if version != 1 && r.err == nil {
		r.err = protocolError(iwire.ErrUnsupportedVersion, "unsupported vector request version")
	} else {
		request.Version = uint32(version)
	}
	request.Generation.Index = r.string()
	request.Generation.Generation = r.u64()
	queryCount := r.int()
	if r.err == nil && queryCount <= 0 {
		r.err = protocolError(iwire.ErrInvalidCommand, "non-empty vector query is required")
	} else if r.err == nil && (queryCount > limits.MaxByteVectorItems || queryCount > (len(src)-r.off)/4) {
		r.err = protocolError(iwire.ErrResourceExhausted, "vector query dimension exceeds bound")
	}
	if r.err == nil {
		if cap(query) < queryCount {
			query = make([]float32, queryCount)
		} else {
			query = query[:queryCount]
		}
		request.Query = query
		for i := range request.Query {
			request.Query[i] = r.float32()
		}
	}
	if metric := r.u64(); metric == 1 {
		request.Metric = public.MetricCosineV1
	} else if r.err == nil {
		r.err = protocolError(iwire.ErrInvalidCommand, "unsupported vector metric")
	}
	request.TopK, request.Probes, request.EfSearch = r.int(), r.int(), r.int()
	if consistency := r.u64(); consistency == 1 {
		request.Consistency = public.ConsistencyGenerationSnapshotV1
	} else if r.err == nil {
		r.err = protocolError(iwire.ErrInvalidCommand, "unsupported vector consistency")
	}
	request.Limits.RequestBytes, request.Limits.CandidateBytes, request.Limits.ResponseBytes = r.u64(), r.u64(), r.u64()
	request.Limits.MergeEntries = r.int()
	if deadline := r.u64(); deadline != 0 {
		if deadline > math.MaxInt64 {
			r.err = protocolError(iwire.ErrInvalidCommand, "vector deadline overflows time")
		} else {
			request.Deadline = time.Unix(0, int64(deadline))
		}
	}
	return request, r.done()
}

func appendVectorPartitionFastOptionsSectionV1(dst []byte, options public.FastSearchOptionsV1) ([]byte, error) {
	if options.MaxIndexAge <= 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "positive vector max index age is required")
	}
	payloadLen := uvarintLen(uint64(options.MaxIndexAge)) + uvarintLen(options.MinIndexedThrough)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorFastOptions, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, uint64(options.MaxIndexAge))
	return binary.AppendUvarint(body, options.MinIndexedThrough), nil
}

func decodeVectorPartitionFastOptionsV1(src []byte) (public.FastSearchOptionsV1, error) {
	r := vectorPartitionWireReaderV1{src: src}
	options := public.FastSearchOptionsV1{MaxIndexAge: r.duration(), MinIndexedThrough: r.u64()}
	if r.err == nil && options.MaxIndexAge <= 0 {
		r.err = protocolError(iwire.ErrInvalidCommand, "positive vector max index age is required")
	}
	return options, r.done()
}

func appendVectorPartitionPinOptionsSectionV1(dst []byte, options public.PinSearchSnapshotOptionsV1) ([]byte, error) {
	if options.MaxIndexAge <= 0 || options.MaxSessionAge <= 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "positive vector snapshot ages are required")
	}
	payloadLen := uvarintLen(uint64(options.MaxIndexAge)) + uvarintLen(options.MinIndexedThrough) + uvarintLen(uint64(options.MaxSessionAge))
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorPinOptions, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, uint64(options.MaxIndexAge))
	body = binary.AppendUvarint(body, options.MinIndexedThrough)
	return binary.AppendUvarint(body, uint64(options.MaxSessionAge)), nil
}

func decodeVectorPartitionPinOptionsV1(src []byte) (public.PinSearchSnapshotOptionsV1, error) {
	r := vectorPartitionWireReaderV1{src: src}
	options := public.PinSearchSnapshotOptionsV1{FastSearchOptionsV1: public.FastSearchOptionsV1{MaxIndexAge: r.duration(), MinIndexedThrough: r.u64()}, MaxSessionAge: r.duration()}
	if r.err == nil && (options.MaxIndexAge <= 0 || options.MaxSessionAge <= 0) {
		r.err = protocolError(iwire.ErrInvalidCommand, "positive vector snapshot ages are required")
	}
	return options, r.done()
}

func appendVectorPartitionSearchResponseSectionV1(dst []byte, response public.SearchResponseV1) ([]byte, error) {
	payloadLen := encodedStringLenV1(response.Generation.Index) + uvarintLen(response.Generation.Generation) + uvarintLen(uint64(len(response.Neighbors)))
	for _, neighbor := range response.Neighbors {
		payloadLen += encodedStringLenV1(neighbor.ID) + 4
	}
	counters := vectorPartitionCountersV1(response.Counters)
	for _, value := range counters {
		payloadLen += uvarintLen(value)
	}
	timings, err := vectorPartitionTimingsV1(response.Timing)
	if err != nil {
		return nil, err
	}
	for _, value := range timings {
		payloadLen += uvarintLen(value)
	}
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorSearchResponse, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = appendString(body, response.Generation.Index)
	body = binary.AppendUvarint(body, response.Generation.Generation)
	body = binary.AppendUvarint(body, uint64(len(response.Neighbors)))
	for _, neighbor := range response.Neighbors {
		body = appendString(body, neighbor.ID)
		body = binary.LittleEndian.AppendUint32(body, math.Float32bits(neighbor.Score))
	}
	for _, value := range counters {
		body = binary.AppendUvarint(body, value)
	}
	for _, value := range timings {
		body = binary.AppendUvarint(body, value)
	}
	return body, nil
}

func decodeVectorPartitionSearchResponseV1(src []byte, limits iwire.Limits) (public.SearchResponseV1, error) {
	r := vectorPartitionWireReaderV1{src: src, ownedStrings: string(src)}
	response := public.SearchResponseV1{}
	response.Generation.Index, response.Generation.Generation = r.string(), r.u64()
	count := r.int()
	if r.err == nil && (count < 0 || count > limits.MaxByteVectorItems || count > (len(src)-r.off)/5) {
		r.err = protocolError(iwire.ErrResourceExhausted, "vector neighbor count exceeds bound")
	}
	if r.err == nil {
		response.Neighbors = make([]public.NeighborV1, count)
		for i := range response.Neighbors {
			response.Neighbors[i] = public.NeighborV1{ID: r.string(), Score: r.float32()}
		}
	}
	var counters [16]uint64
	for i := range counters {
		counters[i] = r.u64()
	}
	response.Counters = public.SearchCountersV1{
		SelectedPartitions: counters[0], SelectedGroups: counters[1], Requests: counters[2], RPCs: counters[3], Retries: counters[4], Redirects: counters[5], Candidates: counters[6], Edges: counters[7],
		SnapshotPins: counters[8], ReadProofs: counters[9], GenerationPins: counters[10], PartitionOpens: counters[11], QueryBytes: counters[12], RequestBytes: counters[13], CandidateBytes: counters[14], ResponseBytes: counters[15],
	}
	var timings [20]time.Duration
	for i := range timings {
		timings[i] = r.duration()
	}
	response.Timing = public.SearchTimingV1{
		Admission: timings[0], OperationsHealth: timings[1], ServiceAdapter: timings[2], PublicAdapter: timings[3], RouterOpen: timings[4], RouterSearch: timings[5], Placement: timings[6], CoordinatorLifecycle: timings[7], Dispatch: timings[8], Queue: timings[9], RPC: timings[10], Network: timings[11], ReadIndexApply: timings[12], GenerationOpen: timings[13], ShardSearch: timings[14], Response: timings[15], Dedupe: timings[16], Merge: timings[17], CoordinatorTotal: timings[18], Total: timings[19],
	}
	return response, r.done()
}

func appendVectorPartitionFastEvidenceSectionV1(dst []byte, evidence public.FastSearchEvidenceV1) ([]byte, error) {
	if evidence.PublishedAt.IsZero() || evidence.PublishedAt.UnixNano() <= 0 || evidence.IndexAge < 0 {
		return nil, protocolError(iwire.ErrInternal, "vector fast evidence cannot be encoded")
	}
	payloadLen := encodedStringLenV1(evidence.Generation.Index) + uvarintLen(evidence.Generation.Generation) + uvarintLen(evidence.IndexedThrough) + uvarintLen(uint64(evidence.PublishedAt.UnixNano())) + uvarintLen(uint64(evidence.IndexAge)) + encodedStringLenV1(evidence.TopologyDigest) + encodedStringLenV1(evidence.AuthorizationOverlayDigest)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorFastEvidence, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = appendString(body, evidence.Generation.Index)
	for _, value := range []uint64{evidence.Generation.Generation, evidence.IndexedThrough, uint64(evidence.PublishedAt.UnixNano()), uint64(evidence.IndexAge)} {
		body = binary.AppendUvarint(body, value)
	}
	body = appendString(body, evidence.TopologyDigest)
	return appendString(body, evidence.AuthorizationOverlayDigest), nil
}

func decodeVectorPartitionFastEvidenceV1(src []byte) (public.FastSearchEvidenceV1, error) {
	r := vectorPartitionWireReaderV1{src: src, ownedStrings: string(src)}
	evidence := public.FastSearchEvidenceV1{}
	evidence.Generation.Index, evidence.Generation.Generation = r.string(), r.u64()
	evidence.IndexedThrough = r.u64()
	published := r.u64()
	if published > math.MaxInt64 {
		r.err = protocolError(iwire.ErrMalformedFrame, "vector publication time overflows")
	} else if published != 0 {
		evidence.PublishedAt = time.Unix(0, int64(published))
	}
	evidence.IndexAge = r.duration()
	evidence.TopologyDigest, evidence.AuthorizationOverlayDigest = r.string(), r.string()
	return evidence, r.done()
}

func appendVectorPartitionStatusSectionV1(dst []byte, status VectorPartitionStatusV1) ([]byte, error) {
	ready := uint64(0)
	if status.Health.Ready {
		ready = 1
	}
	payloadLen := encodedStringLenV1(status.NodeConfigSHA256) + uvarintLen(ready) + encodedStringLenV1(string(status.Health.State)) + encodedStringLenV1(status.Health.Generation.Index) + uvarintLen(status.Health.Generation.Generation) + encodedStringLenV1(status.Health.Reason)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorStatus, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = appendString(body, status.NodeConfigSHA256)
	body = binary.AppendUvarint(body, ready)
	body = appendString(body, string(status.Health.State))
	body = appendString(body, status.Health.Generation.Index)
	body = binary.AppendUvarint(body, status.Health.Generation.Generation)
	return appendString(body, status.Health.Reason), nil
}

func decodeVectorPartitionStatusV1(src []byte) (VectorPartitionStatusV1, error) {
	r := vectorPartitionWireReaderV1{src: src, ownedStrings: string(src)}
	status := VectorPartitionStatusV1{NodeConfigSHA256: r.string()}
	ready := r.u64()
	if ready > 1 && r.err == nil {
		r.err = protocolError(iwire.ErrMalformedFrame, "vector ready flag is invalid")
	}
	status.Health.Ready = ready == 1
	status.Health.State = public.GenerationStateV1(r.string())
	status.Health.Generation.Index, status.Health.Generation.Generation = r.string(), r.u64()
	status.Health.Reason = r.string()
	return status, r.done()
}

func vectorPartitionCountersV1(c public.SearchCountersV1) [16]uint64 {
	return [16]uint64{c.SelectedPartitions, c.SelectedGroups, c.Requests, c.RPCs, c.Retries, c.Redirects, c.Candidates, c.Edges, c.SnapshotPins, c.ReadProofs, c.GenerationPins, c.PartitionOpens, c.QueryBytes, c.RequestBytes, c.CandidateBytes, c.ResponseBytes}
}

func vectorPartitionTimingsV1(t public.SearchTimingV1) ([20]uint64, error) {
	durations := [20]time.Duration{t.Admission, t.OperationsHealth, t.ServiceAdapter, t.PublicAdapter, t.RouterOpen, t.RouterSearch, t.Placement, t.CoordinatorLifecycle, t.Dispatch, t.Queue, t.RPC, t.Network, t.ReadIndexApply, t.GenerationOpen, t.ShardSearch, t.Response, t.Dedupe, t.Merge, t.CoordinatorTotal, t.Total}
	var out [20]uint64
	for i, duration := range durations {
		if duration < 0 {
			return out, protocolError(iwire.ErrInternal, "negative vector timing")
		}
		out[i] = uint64(duration)
	}
	return out, nil
}

func encodedStringLenV1(value string) int { return uvarintLen(uint64(len(value))) + len(value) }

type vectorPartitionWireReaderV1 struct {
	src          []byte
	ownedStrings string
	off          int
	err          error
}

func (r *vectorPartitionWireReaderV1) u64() uint64 {
	if r.err != nil {
		return 0
	}
	value, n, err := readUvarint(r.src[r.off:])
	if err != nil {
		r.err = err
		return 0
	}
	r.off += n
	return value
}

func (r *vectorPartitionWireReaderV1) int() int {
	value := r.u64()
	if value > uint64(maxInt) && r.err == nil {
		r.err = protocolError(iwire.ErrResourceExhausted, "vector integer exceeds capacity")
		return 0
	}
	return int(value)
}

func (r *vectorPartitionWireReaderV1) string() string {
	if r.err != nil {
		return ""
	}
	if r.ownedStrings != "" {
		length, read, err := readUvarint(r.src[r.off:])
		if err != nil {
			r.err = err
			return ""
		}
		r.off += read
		if length > uint64(len(r.src)-r.off) {
			r.err = protocolError(iwire.ErrMalformedFrame, "string length exceeds remaining payload")
			return ""
		}
		start := r.off
		r.off += int(length)
		return r.ownedStrings[start:r.off]
	}
	value, err := readString(r.src, &r.off)
	if err != nil {
		r.err = err
	}
	return value
}

func (r *vectorPartitionWireReaderV1) float32() float32 {
	if r.err != nil {
		return 0
	}
	if len(r.src)-r.off < 4 {
		r.err = protocolError(iwire.ErrMalformedFrame, "truncated vector float32")
		return 0
	}
	value := math.Float32frombits(binary.LittleEndian.Uint32(r.src[r.off : r.off+4]))
	r.off += 4
	return value
}

func (r *vectorPartitionWireReaderV1) duration() time.Duration {
	value := r.u64()
	if value > math.MaxInt64 && r.err == nil {
		r.err = protocolError(iwire.ErrMalformedFrame, "vector duration overflows")
		return 0
	}
	return time.Duration(value)
}

func (r *vectorPartitionWireReaderV1) done() error {
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.src) {
		return protocolError(iwire.ErrMalformedFrame, "vector payload has %d trailing bytes", len(r.src)-r.off)
	}
	return nil
}
