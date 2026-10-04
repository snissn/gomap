package nativewire

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func (c *Client) VectorReplaceV1(ctx context.Context, request public.ReplaceRequestV1) (public.MutationResponseV1, error) {
	return c.vectorColocatedMutationCommandV1(ctx, VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: request}})
}
func (c *Client) VectorDeleteV1(ctx context.Context, request public.DeleteRequestV1) (public.MutationResponseV1, error) {
	return c.vectorColocatedMutationCommandV1(ctx, VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{Version: request.Version, Generation: request.Generation, ID: request.ID, IdempotencyKey: request.IdempotencyKey, Deadline: request.Deadline}}, Delete: true})
}
func (c *Client) vectorColocatedMutationCommandV1(ctx context.Context, request VectorPartitionRoutedMutationV1) (public.MutationResponseV1, error) {
	var zero public.MutationResponseV1
	if c == nil {
		return zero, vectorPartitionMutationClientErrorV1(&requestNotSubmittedError{io.ErrClosedPipe})
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := request.Request.Deadline
	if d, ok := ctx.Deadline(); ok && (deadline.IsZero() || d.Before(deadline)) {
		deadline = d
	}
	if deadline.IsZero() {
		return zero, vectorPartitionClientErrorV1(protocolError(iwire.ErrInvalidCommand, "bounded vector mutation deadline is required"))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.local == nil && c.conn == nil {
		return zero, vectorPartitionMutationClientErrorV1(&requestNotSubmittedError{io.ErrClosedPipe})
	}
	body, err := appendVectorPartitionColocatedCommandBodyV1(c.requestBody[:0], request, deadline, c.limits)
	if err != nil {
		return zero, vectorPartitionClientErrorV1(err)
	}
	requestCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := requestCtx.Err(); err != nil {
		return zero, vectorPartitionClientErrorV1(err)
	}
	_, raw, err := c.roundTripLocked(requestCtx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err != nil {
		return zero, vectorPartitionMutationClientErrorV1(err)
	}
	c.vectorSections, err = iwire.DecodeSectionsInto(c.vectorSections[:0], raw, c.limits)
	if err != nil {
		return zero, vectorPartitionMutationClientErrorV1(err)
	}
	encoded, ok, err := singletonSection(c.vectorSections, iwire.SectionVectorMutationResponse)
	if err != nil || !ok {
		return zero, vectorPartitionMutationClientErrorV1(protocolError(iwire.ErrMalformedFrame, "colocated mutation response missing"))
	}
	response, err := decodeVectorPartitionColocatedResponseV1(encoded)
	if err == nil {
		err = public.ValidateMutationResponseV1(request.Request.Generation, request.Delete, response)
	}
	return response, vectorPartitionMutationClientErrorV1(err)
}

func appendVectorPartitionColocatedCommandBodyV1(dst []byte, request VectorPartitionRoutedMutationV1, deadline time.Time, limits iwire.Limits) ([]byte, error) {
	command := iwire.CommandVectorReplace
	if request.Delete {
		command = iwire.CommandVectorDelete
	}
	body, err := appendCommandHeaderSection(dst, command)
	if err != nil {
		return nil, err
	}
	if deadline.UnixNano() <= 0 {
		return nil, protocolError(iwire.ErrInvalidCommand, "invalid mutation deadline")
	}
	body, err = iwire.AppendSectionHeader(body, iwire.SectionDeadline, 0, uvarintLen(uint64(deadline.UnixNano())))
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, uint64(deadline.UnixNano()))
	if !request.Delete {
		if err := public.ValidateReplaceRequestV1(context.Background(), request.Request); err != nil {
			return nil, err
		}
		return appendVectorPartitionInsertRequestSectionIDV1(body, request.Request, limits, iwire.SectionVectorReplaceRequest)
	}
	r := request.Request
	if r.Version != 1 || len(r.Generation.Index) == 0 || r.Generation.Generation == 0 || len(r.ID) == 0 || len(r.ID) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || len(r.IdempotencyKey) == 0 || len(r.IdempotencyKey) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 {
		return nil, protocolError(iwire.ErrInvalidCommand, "invalid exact-ID deletion")
	}
	payloadLen := 1 + encodedStringLenV1(r.Generation.Index) + uvarintLen(r.Generation.Generation) + uvarintLen(uint64(len(r.IdempotencyKey))) + len(r.IdempotencyKey) + uvarintLen(uint64(len(r.ID))) + len(r.ID) + uvarintLen(uint64(deadline.UnixNano()))
	body, err = iwire.AppendSectionHeader(body, iwire.SectionVectorDeleteRequest, 0, payloadLen)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, 1)
	body = appendString(body, r.Generation.Index)
	body = binary.AppendUvarint(body, r.Generation.Generation)
	body = binary.AppendUvarint(body, uint64(len(r.IdempotencyKey)))
	body = append(body, r.IdempotencyKey...)
	body = binary.AppendUvarint(body, uint64(len(r.ID)))
	body = append(body, r.ID...)
	return binary.AppendUvarint(body, uint64(deadline.UnixNano())), nil
}

func decodeVectorPartitionDeleteRequestV1(raw []byte) (public.DeleteRequestV1, error) {
	r := vectorPartitionWireReaderV1{src: raw}
	request := public.DeleteRequestV1{}
	version := r.u64()
	if version != 1 {
		return request, protocolError(iwire.ErrUnsupportedVersion, "unsupported deletion version")
	}
	request.Version = 1
	request.Generation.Index, request.Generation.Generation = r.string(), r.u64()
	request.IdempotencyKey, request.ID = r.bytes(commitlog.ColocatedVectorMutationMaxIdentityBytesV1), r.bytes(commitlog.ColocatedVectorMutationMaxIdentityBytesV1)
	deadline := r.u64()
	if deadline == 0 || deadline > math.MaxInt64 {
		return request, protocolError(iwire.ErrInvalidCommand, "invalid mutation deadline")
	}
	request.Deadline = time.Unix(0, int64(deadline))
	return request, r.done()
}

func (s *Server) handleVectorPartitionColocatedCommandV1(ctx context.Context, cmd iwire.ValidatedCommand, dst []byte) ([]byte, error) {
	deletion := cmd.Header.ID == iwire.CommandVectorDelete
	section := iwire.SectionVectorReplaceRequest
	if deletion {
		section = iwire.SectionVectorDeleteRequest
	}
	raw, ok, err := singletonSection(cmd.Known, section)
	if err != nil || !ok {
		return nil, protocolError(iwire.ErrInvalidCommand, "mutation request missing")
	}
	var response public.MutationResponseV1
	if deletion {
		request, decodeErr := decodeVectorPartitionDeleteRequestV1(raw)
		if decodeErr != nil {
			return nil, decodeErr
		}
		response, err = s.vectorPartitionOperations.Delete(ctx, request)
	} else {
		request, decodeErr := decodeVectorPartitionInsertRequestV1(raw, s.limits)
		if decodeErr != nil {
			return nil, decodeErr
		}
		response, err = s.vectorPartitionOperations.Replace(ctx, request)
	}
	if err != nil {
		return nil, vectorPartitionServerErrorV1(err)
	}
	return appendVectorPartitionColocatedResponseV1(dst, response)
}

func appendVectorPartitionColocatedResponseV1(dst []byte, response public.MutationResponseV1) ([]byte, error) {
	if len(response.VisibilityToken) == 0 || len(response.VisibilityToken) > 8192 {
		return nil, protocolError(iwire.ErrResourceExhausted, "mutation visibility token exceeds bound")
	}
	values := []uint64{response.CommitTerm, response.CommitIndex, response.AppliedIndex, 0, response.Coverage, response.LiveRevision, response.Matched, response.Modified, response.Deleted, response.Counters.Routes, response.Counters.Forwards, response.Counters.Commits, response.Counters.Replications, response.Counters.Applies, response.Counters.VisibilityProofs}
	if response.ProductionConsensus {
		values[3] = 1
	}
	size := 1 + encodedStringLenV1(response.Generation.Index) + uvarintLen(response.Generation.Generation) + encodedStringLenV1(response.OwnerGroup) + uvarintLen(uint64(len(response.VisibilityToken))) + len(response.VisibilityToken)
	for _, v := range values {
		size += uvarintLen(v)
	}
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionVectorMutationResponse, 0, size)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, 1)
	body = appendString(body, response.Generation.Index)
	body = binary.AppendUvarint(body, response.Generation.Generation)
	body = appendString(body, response.OwnerGroup)
	for _, v := range values {
		body = binary.AppendUvarint(body, v)
	}
	body = binary.AppendUvarint(body, uint64(len(response.VisibilityToken)))
	return append(body, response.VisibilityToken...), nil
}
func decodeVectorPartitionColocatedResponseV1(raw []byte) (public.MutationResponseV1, error) {
	r := vectorPartitionWireReaderV1{src: raw}
	response := public.MutationResponseV1{}
	if r.u64() != 1 {
		return response, protocolError(iwire.ErrUnsupportedVersion, "unsupported mutation response")
	}
	response.Generation.Index, response.Generation.Generation, response.OwnerGroup = r.string(), r.u64(), r.string()
	response.CommitTerm, response.CommitIndex, response.AppliedIndex = r.u64(), r.u64(), r.u64()
	consensus := r.u64()
	if consensus > 1 {
		return response, protocolError(iwire.ErrMalformedFrame, "invalid consensus boolean")
	}
	response.ProductionConsensus = consensus == 1
	response.Coverage, response.LiveRevision, response.Matched, response.Modified, response.Deleted = r.u64(), r.u64(), r.u64(), r.u64(), r.u64()
	response.Counters = public.MutationCountersV1{Routes: r.u64(), Forwards: r.u64(), Commits: r.u64(), Replications: r.u64(), Applies: r.u64(), VisibilityProofs: r.u64()}
	response.VisibilityToken = append([]byte(nil), r.bytes(8192)...)
	return response, r.done()
}
