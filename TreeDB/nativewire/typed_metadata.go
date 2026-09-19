package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type typedMetadataUpdateCarrier struct {
	Index string `json:"index"`
	documentservice.UpdateMetadataByIDRequest
}

func decodeTypedMetadataUpdate(raw []byte, limits iwire.Limits) (typedMetadataUpdateCarrier, error) {
	var req typedMetadataUpdateCarrier
	if len(raw) == 0 || uint64(len(raw)) > limits.MaxSectionLen {
		return req, protocolError(iwire.ErrResourceExhausted, "typed metadata request exceeds bounds")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return req, protocolError(iwire.ErrMalformedFrame, "malformed typed metadata request: %v", err)
	}
	for _, name := range []string{"index", "expected_generation", "ids", "set", "unset"} {
		if _, ok := fields[name]; !ok {
			return req, protocolError(iwire.ErrInvalidCommand, "typed metadata request missing %s", name)
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, protocolError(iwire.ErrMalformedFrame, "malformed typed metadata request: %v", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return typedMetadataUpdateCarrier{}, protocolError(iwire.ErrMalformedFrame, "malformed typed metadata request: %v", err)
	}
	if req.Index == "" || req.ExpectedGeneration == 0 {
		return typedMetadataUpdateCarrier{}, protocolError(iwire.ErrInvalidCommand, "typed metadata index and positive generation are required")
	}
	if len(req.IDs) > limits.MaxByteVectorItems || len(req.Set) > limits.MaxByteVectorItems || len(req.Unset) > limits.MaxByteVectorItems || len(req.Set) > limits.MaxByteVectorItems-len(req.Unset) {
		return typedMetadataUpdateCarrier{}, protocolError(iwire.ErrResourceExhausted, "typed metadata item count exceeds limit")
	}
	return req, nil
}

func (s *Server) handleTypedMetadataUpdate(ctx context.Context, sections []iwire.Section) ([]iwire.Section, error) {
	if s.documentService == nil {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "document service is not configured")
	}
	if err := s.checkResponseSectionCount(1); err != nil {
		return nil, err
	}
	deadline, err := deadlineUnixNanosFromSections(sections)
	if err != nil || deadline <= 0 {
		return nil, denseDecodeError(err, "positive typed metadata update deadline required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, deadline))
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, ok, err := singletonSection(sections, iwire.SectionTypedMetadataUpdateRequest)
	if err != nil || !ok {
		return nil, denseDecodeError(err, "missing typed metadata update request")
	}
	carrier, err := decodeTypedMetadataUpdate(raw, s.limits)
	if err != nil {
		return nil, err
	}
	out, err := s.documentService.UpdateMetadataByID(ctx, carrier.Index, carrier.UpdateMetadataByIDRequest)
	if err != nil {
		return nil, err
	}
	response := binary.AppendUvarint(nil, out.Index.Generation)
	response = binary.AppendUvarint(response, uint64(out.MatchedCount))
	response = binary.AppendUvarint(response, uint64(out.ModifiedCount))
	return []iwire.Section{{ID: iwire.SectionTypedMetadataUpdateResponse, Bytes: response}}, nil
}
