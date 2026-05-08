package nativewire

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// DocumentsResult is a batched read result. ID and document slices returned by
// client APIs borrow the client's response buffer unless documented otherwise.
type DocumentsResult struct {
	IDs       [][]byte
	Docs      [][]byte
	Present   []bool
	Cursor    CursorMeta
	Truncated bool
}

// GetMany fetches documents by ID. Returned document slices borrow the client's
// response buffer and remain valid until the next round trip on this client.
func (c *Client) GetMany(ctx context.Context, collection string, ids [][]byte) ([][]byte, []bool, error) {
	return c.getMany(ctx, collection, 0, false, ids)
}

// GetManyHandle fetches documents by ID through an open collection handle.
// Returned document slices borrow the client's response buffer and remain valid
// until the next round trip on this client.
func (c *Client) GetManyHandle(ctx context.Context, handle CollectionHandle, ids [][]byte) ([][]byte, []bool, error) {
	return c.getMany(ctx, "", handle, true, ids)
}

func (c *Client) getMany(ctx context.Context, collection string, handle CollectionHandle, useHandle bool, ids [][]byte) ([][]byte, []bool, error) {
	if c == nil {
		return nil, nil, io.ErrClosedPipe
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	body, err := appendGetManyRequestBodyRef(c.requestBody[:0], collection, handle, useHandle, ids)
	if err != nil {
		return nil, nil, err
	}
	_, response, err := c.roundTripLocked(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err != nil {
		return nil, nil, err
	}
	var sectionBuf [4]iwire.Section
	sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, c.limits)
	if err != nil {
		return nil, nil, err
	}
	rawDocs, ok, err := singletonSection(sections, iwire.SectionDocuments)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, protocolError(iwire.ErrMalformedFrame, "get_many missing documents")
	}
	docs, err := decodeByteVectorBorrowed(rawDocs, c.limits)
	if err != nil {
		return nil, nil, err
	}
	if len(docs) != len(ids) {
		return nil, nil, protocolError(iwire.ErrMalformedFrame, "get_many documents length %d does not match requested ids length %d", len(docs), len(ids))
	}
	rawPresence, ok, err := singletonSection(sections, iwire.SectionPresenceBitmap)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, protocolError(iwire.ErrMalformedFrame, "get_many missing presence bitmap")
	}
	present, err := decodePresenceBitmap(rawPresence, len(ids))
	if err != nil {
		return nil, nil, err
	}
	return docs, present, nil
}

func appendGetManyRequestBody(dst []byte, collection string, ids [][]byte) ([]byte, error) {
	return appendGetManyRequestBodyRef(dst, collection, 0, false, ids)
}

func appendGetManyRequestBodyRef(dst []byte, collection string, handle CollectionHandle, useHandle bool, ids [][]byte) ([]byte, error) {
	var commandHeader [16]byte
	commandPayload := iwire.AppendCommandHeader(commandHeader[:0], iwire.CommandHeader{ID: iwire.CommandGetMany, Version: 1})
	var refBuf [1 + binary.MaxVarintLen64]byte
	var refPayload []byte
	refLen := collectionNameRefPayloadLen(collection)
	if useHandle {
		refPayload = appendCollectionHandleRefPayload(refBuf[:0], handle)
		refLen = len(refPayload)
	}
	idsLen := iwire.ByteVectorEncodedLen(ids)
	total := iwire.SectionHeaderEncodedLen(iwire.SectionCommandHeader, 0, len(commandPayload)) + len(commandPayload)
	total += iwire.SectionHeaderEncodedLen(iwire.SectionCollectionRef, 0, refLen) + refLen
	total += iwire.SectionHeaderEncodedLen(iwire.SectionDocumentIDs, 0, idsLen) + idsLen
	if cap(dst)-len(dst) < total {
		next := make([]byte, len(dst), len(dst)+total)
		copy(next, dst)
		dst = next
	}
	body, err := appendRawSection(dst, iwire.SectionCommandHeader, commandPayload)
	if err != nil {
		return nil, err
	}
	if useHandle {
		body, err = appendRawSection(body, iwire.SectionCollectionRef, refPayload)
	} else {
		body, err = appendCollectionNameRefSection(body, collection)
	}
	if err != nil {
		return nil, err
	}
	return appendByteVectorSectionKnownLen(body, iwire.SectionDocumentIDs, idsLen, ids)
}

// IndexLookup returns IDs matching an index value. Returned ID slices borrow the
// client's response buffer and remain valid until the next round trip.
func (c *Client) IndexLookup(ctx context.Context, collection, index string, value any, limits CursorLimits) ([][]byte, bool, error) {
	scalar, err := encodeScalar(value)
	if err != nil {
		return nil, false, err
	}
	req := []iwire.Section{
		collectionNameRef(collection),
		{ID: iwire.SectionIndexName, Bytes: encodeIndexName(index)},
		{ID: iwire.SectionIndexValue, Bytes: scalar},
	}
	if limits.MaxItems > 0 || limits.MaxBytes > 0 {
		req = append(req, iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(limits)})
	}
	sections, err := c.commandSections(ctx, iwire.CommandIndexLookup, req...)
	if err != nil {
		return nil, false, err
	}
	return decodeIDsAndTruncated(sections, c.limits)
}

// IndexRange returns IDs within an index range. Returned ID slices borrow the
// client's response buffer and remain valid until the next round trip.
func (c *Client) IndexRange(ctx context.Context, collection, index string, opts IndexRange) ([][]byte, bool, error) {
	req := []iwire.Section{
		collectionNameRef(collection),
		{ID: iwire.SectionIndexName, Bytes: encodeIndexName(index)},
	}
	if !opts.LowerUnbounded {
		raw, err := encodeIndexBound(opts.Lower.Value, opts.LowerInclusive, false)
		if err != nil {
			return nil, false, err
		}
		req = append(req, iwire.Section{ID: iwire.SectionIndexLowerBound, Bytes: raw})
	}
	if !opts.UpperUnbounded {
		raw, err := encodeIndexBound(opts.Upper.Value, opts.UpperInclusive, false)
		if err != nil {
			return nil, false, err
		}
		req = append(req, iwire.Section{ID: iwire.SectionIndexUpperBound, Bytes: raw})
	}
	if opts.Limit > 0 || opts.MaxBytes > 0 {
		req = append(req, iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{
			MaxItems: opts.Limit,
			MaxBytes: opts.MaxBytes,
		})})
	}
	sections, err := c.commandSections(ctx, iwire.CommandIndexRange, req...)
	if err != nil {
		return nil, false, err
	}
	return decodeIDsAndTruncated(sections, c.limits)
}

// OpenScan starts a collection scan. Returned ID and document slices borrow the
// client's response buffer and remain valid until the next round trip.
func (c *Client) OpenScan(ctx context.Context, collection string, limits CursorLimits) (DocumentsResult, error) {
	req := []iwire.Section{collectionNameRef(collection)}
	if limits.MaxItems > 0 || limits.MaxBytes > 0 {
		req = append(req, iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(limits)})
	}
	sections, err := c.commandSections(ctx, iwire.CommandOpenScan, req...)
	if err != nil {
		return DocumentsResult{}, err
	}
	return decodeDocumentsResult(sections, c.limits)
}

// CursorNext fetches the next cursor batch. Returned ID and document slices
// borrow the client's response buffer and remain valid until the next round trip.
func (c *Client) CursorNext(ctx context.Context, cursorID uint64, limits CursorLimits) (DocumentsResult, error) {
	sections, err := c.commandSectionsOnStream(ctx, cursorID, iwire.CommandCursorNext,
		iwire.Section{ID: iwire.SectionCursorRef, Bytes: encodeCursorRef(cursorID)},
		iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(limits)},
	)
	if err != nil {
		return DocumentsResult{}, err
	}
	return decodeDocumentsResult(sections, c.limits)
}

func (c *Client) CursorClose(ctx context.Context, cursorID uint64) error {
	_, err := c.commandSectionsOnStream(ctx, cursorID, iwire.CommandCursorClose,
		iwire.Section{ID: iwire.SectionCursorRef, Bytes: encodeCursorRef(cursorID)},
	)
	return err
}

func decodeIDsAndTruncated(sections []iwire.Section, limits iwire.Limits) ([][]byte, bool, error) {
	rawIDs, ok, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, protocolError(iwire.ErrMalformedFrame, "missing document_ids")
	}
	ids, err := decodeByteVectorBorrowed(rawIDs, limits)
	if err != nil {
		return nil, false, err
	}
	truncated := false
	if raw, ok, err := singletonSection(sections, iwire.SectionTruncated); err != nil {
		return nil, false, err
	} else if ok {
		truncated, err = decodeBoolPayload(raw, "truncated")
		if err != nil {
			return nil, false, err
		}
	}
	return ids, truncated, nil
}

func decodeDocumentsResult(sections []iwire.Section, limits iwire.Limits) (DocumentsResult, error) {
	var out DocumentsResult
	rawIDs, ok, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return out, err
	}
	if ok {
		out.IDs, err = decodeByteVectorBorrowed(rawIDs, limits)
		if err != nil {
			return out, err
		}
	}
	rawDocs, ok, err := singletonSection(sections, iwire.SectionDocuments)
	if err != nil {
		return out, err
	}
	if ok {
		out.Docs, err = decodeByteVectorBorrowed(rawDocs, limits)
		if err != nil {
			return out, err
		}
	}
	rawMeta, ok, err := singletonSection(sections, iwire.SectionCursorMeta)
	if err != nil {
		return out, err
	}
	if ok {
		out.Cursor, err = decodeCursorMeta(rawMeta)
		if err != nil {
			return out, err
		}
	}
	if raw, ok, err := singletonSection(sections, iwire.SectionTruncated); err != nil {
		return out, err
	} else if ok {
		out.Truncated, err = decodeBoolPayload(raw, "truncated")
		if err != nil {
			return out, err
		}
	}
	if out.IDs != nil && out.Docs != nil && len(out.IDs) != len(out.Docs) {
		return out, protocolError(iwire.ErrMalformedFrame, "document_ids length %d does not match documents length %d", len(out.IDs), len(out.Docs))
	}
	return out, nil
}

func decodeBoolPayload(raw []byte, name string) (bool, error) {
	off := 0
	value, err := readBool(raw, &off)
	if err != nil {
		return false, err
	}
	if off != len(raw) {
		return false, protocolError(iwire.ErrMalformedFrame, "%s bool has %d trailing bytes", name, len(raw)-off)
	}
	return value, nil
}

func scalarFromIndexValueType(valueType collections.IndexValueType, value any) Scalar {
	return Scalar{Type: valueType, Value: value}
}
