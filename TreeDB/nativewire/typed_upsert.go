package nativewire

import (
	"context"
	"encoding/binary"
	"math"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// decodeTypedUpsert borrows residual/ID bytes for the synchronous service call.
// FP32 values occupy one owned flat allocation, with capped per-row views.
func decodeTypedUpsert(raw, idsRaw, residualRaw []byte, limits iwire.Limits) (string, documentservice.TypedDocumentsRequest, error) {
	var req documentservice.TypedDocumentsRequest
	off := 0
	index, err := readDenseString(raw, &off, limits.MaxByteVectorBytes, "index")
	if err != nil {
		return "", req, err
	}
	req.ExpectedGeneration, err = readUvarintField(raw, &off, "generation")
	if err != nil || req.ExpectedGeneration == 0 {
		return "", req, denseDecodeError(err, "positive generation required")
	}
	rows, err := readDenseInt(raw, &off, "rows")
	if err != nil || rows <= 0 || rows > limits.MaxByteVectorItems {
		return "", req, denseDecodeError(err, "invalid row count")
	}
	dims, err := readDenseInt(raw, &off, "dimensions")
	if err != nil || dims <= 0 || dims > 65536 || rows > (len(raw)-off)/4/dims {
		return "", req, denseDecodeError(err, "invalid packed vector dimensions")
	}
	req.IDs, err = iwire.DecodeByteVectorItemsInto(nil, idsRaw, limits)
	if err != nil {
		return "", req, err
	}
	req.Retained, err = iwire.DecodeByteVectorItemsInto(nil, residualRaw, limits)
	if err != nil {
		return "", req, err
	}
	if len(req.IDs) != rows || len(req.Retained) != rows {
		return "", req, protocolError(iwire.ErrMalformedFrame, "typed row count mismatch")
	}
	flat := make([]float32, rows*dims)
	for i := range flat {
		flat[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
	}
	vectors := make([][]float32, rows)
	for i := range vectors {
		vectors[i] = flat[i*dims : (i+1)*dims : (i+1)*dims]
	}
	// The following column count includes content plus all declared scalars.
	count, err := readDenseInt(raw, &off, "string columns")
	if err != nil || count <= 0 || count > limits.MaxByteVectorItems || count > (len(raw)-off)/(rows+1) {
		return "", req, denseDecodeError(err, "invalid string column count")
	}
	req.Columns = make([]collections.TypedColumnBatch, 1, count+1)
	req.Columns[0] = collections.TypedColumnBatch{Name: "embedding", Float32Vectors: vectors}
	for n := 0; n < count; n++ {
		name, err := readDenseString(raw, &off, limits.MaxByteVectorBytes, "column")
		if err != nil {
			return "", req, err
		}
		values := make([]string, rows)
		for i := range values {
			values[i], err = readDenseString(raw, &off, limits.MaxByteVectorBytes, "string value")
			if err != nil {
				return "", req, err
			}
		}
		req.Columns = append(req.Columns, collections.TypedColumnBatch{Name: name, Strings: values})
	}
	if off != len(raw) {
		return "", req, protocolError(iwire.ErrMalformedFrame, "trailing typed request bytes")
	}
	return index, req, nil
}

func (s *Server) handleTypedDocumentUpsert(ctx context.Context, sections []iwire.Section) ([]iwire.Section, error) {
	if s.documentService == nil {
		return nil, protocolError(iwire.ErrUnsupportedFeature, "document service is not configured")
	}
	if err := s.checkResponseSectionCount(1); err != nil {
		return nil, err
	}
	deadline, err := deadlineUnixNanosFromSections(sections)
	if err != nil || deadline <= 0 {
		return nil, denseDecodeError(err, "positive typed upsert deadline required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, deadline))
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var payload [3][]byte
	for i, id := range []iwire.SectionID{iwire.SectionTypedUpsertRequest, iwire.SectionDocumentIDs, iwire.SectionDocuments} {
		data, ok, err := singletonSection(sections, id)
		if err != nil || !ok {
			return nil, denseDecodeError(err, "missing typed upsert section")
		}
		payload[i] = data
	}
	index, req, err := decodeTypedUpsert(payload[0], payload[1], payload[2], s.limits)
	if err != nil {
		return nil, err
	}
	out, err := s.documentService.UpsertTypedDocuments(ctx, index, req)
	if err != nil {
		return nil, err
	}
	meta := binary.AppendUvarint(nil, out.Index.Generation)
	meta = binary.AppendUvarint(meta, uint64(out.Upserted))
	meta = binary.AppendUvarint(meta, uint64(out.Inserted))
	meta = binary.AppendUvarint(meta, uint64(out.Updated))
	return []iwire.Section{{ID: iwire.SectionTypedUpsertResponse, Bytes: meta}}, nil
}
