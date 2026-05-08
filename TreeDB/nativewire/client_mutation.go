package nativewire

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (c *Client) InsertBatch(ctx context.Context, collection string, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([][]byte, error) {
	return c.insertBatch(ctx, collection, 0, false, format, ids, docs, ack)
}

func (c *Client) InsertBatchHandle(ctx context.Context, handle CollectionHandle, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([][]byte, error) {
	return c.insertBatch(ctx, "", handle, true, format, ids, docs, ack)
}

func (c *Client) InsertBatchNoResult(ctx context.Context, collection string, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) error {
	return c.insertBatchNoResult(ctx, collection, 0, false, format, ids, docs, ack)
}

func (c *Client) InsertBatchHandleNoResult(ctx context.Context, handle CollectionHandle, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) error {
	return c.insertBatchNoResult(ctx, "", handle, true, format, ids, docs, ack)
}

func (c *Client) insertBatch(ctx context.Context, collection string, handle CollectionHandle, useHandle bool, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([][]byte, error) {
	if c == nil {
		return nil, io.ErrClosedPipe
	}
	guard, err := c.replicatedMutationGuard(ctx, "insert_batch")
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	body, err := appendInsertBatchRequestBodyRefFlags(c.requestBody[:0], collection, handle, useHandle, format, ids, docs, ack, 0, guard)
	if err != nil {
		return nil, err
	}
	_, response, err := c.roundTripLocked(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err != nil {
		return nil, err
	}
	c.catalogVersionPlusOne.Store(0)
	var sectionBuf [4]iwire.Section
	sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, c.limits)
	if err != nil {
		return nil, err
	}
	rawIDs, ok, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrMalformedFrame, "insert_batch missing result document_ids")
	}
	return decodeByteVectorBorrowed(rawIDs, c.limits)
}

func (c *Client) insertBatchNoResult(ctx context.Context, collection string, handle CollectionHandle, useHandle bool, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) error {
	if c == nil {
		return io.ErrClosedPipe
	}
	guard, err := c.replicatedMutationGuard(ctx, "insert_batch")
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	body, err := appendInsertBatchRequestBodyRefFlags(c.requestBody[:0], collection, handle, useHandle, format, ids, docs, ack, iwire.CommandFlagOmitResultIDs|iwire.CommandFlagOmitResponseMeta, guard)
	if err != nil {
		return err
	}
	err = c.roundTripLockedDiscardResponse(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err == nil {
		c.catalogVersionPlusOne.Store(0)
	}
	return err
}

func appendInsertBatchRequestBody(dst []byte, collection string, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([]byte, error) {
	return appendInsertBatchRequestBodyRef(dst, collection, 0, false, format, ids, docs, ack)
}

func appendInsertBatchRequestBodyRef(dst []byte, collection string, handle CollectionHandle, useHandle bool, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([]byte, error) {
	return appendInsertBatchRequestBodyRefFlags(dst, collection, handle, useHandle, format, ids, docs, ack, 0, nil)
}

func appendInsertBatchRequestBodyRefFlags(dst []byte, collection string, handle CollectionHandle, useHandle bool, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy, commandFlags uint64, guard []iwire.Section) ([]byte, error) {
	var commandHeader [16]byte
	commandPayload := iwire.AppendCommandHeader(commandHeader[:0], iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1, Flags: commandFlags})
	var refBuf [1 + binary.MaxVarintLen64]byte
	var refPayload []byte
	refLen := collectionNameRefPayloadLen(collection)
	if useHandle {
		refPayload = appendCollectionHandleRefPayload(refBuf[:0], handle)
		refLen = len(refPayload)
	}
	var formatBuf [binary.MaxVarintLen64]byte
	formatPayload := binary.AppendUvarint(formatBuf[:0], uint64(encodeDocumentFormat(format)))
	var ackBuf [binary.MaxVarintLen64]byte
	var ackPayload []byte
	if ack != 0 {
		ackPayload = binary.AppendUvarint(ackBuf[:0], uint64(ack))
	}
	idsLen := iwire.ByteVectorEncodedLen(ids)
	docsLen := iwire.ByteVectorEncodedLen(docs)
	total := iwire.SectionHeaderEncodedLen(iwire.SectionCommandHeader, 0, len(commandPayload)) + len(commandPayload)
	for _, section := range guard {
		total += iwire.SectionEncodedLen(section)
	}
	total += iwire.SectionHeaderEncodedLen(iwire.SectionCollectionRef, 0, refLen) + refLen
	total += iwire.SectionHeaderEncodedLen(iwire.SectionDocumentFormat, 0, len(formatPayload)) + len(formatPayload)
	total += iwire.SectionHeaderEncodedLen(iwire.SectionDocumentIDs, 0, idsLen) + idsLen
	total += iwire.SectionHeaderEncodedLen(iwire.SectionDocuments, 0, docsLen) + docsLen
	if ack != 0 {
		total += iwire.SectionHeaderEncodedLen(iwire.SectionAckPolicy, 0, len(ackPayload)) + len(ackPayload)
	}
	if cap(dst)-len(dst) < total {
		next := make([]byte, len(dst), len(dst)+total)
		copy(next, dst)
		dst = next
	}
	body, err := appendRawSection(dst, iwire.SectionCommandHeader, commandPayload)
	if err != nil {
		return nil, err
	}
	for _, section := range guard {
		body, err = iwire.AppendSection(body, section)
		if err != nil {
			return nil, err
		}
	}
	if useHandle {
		body, err = appendRawSection(body, iwire.SectionCollectionRef, refPayload)
	} else {
		body, err = appendCollectionNameRefSection(body, collection)
	}
	if err != nil {
		return nil, err
	}
	body, err = appendRawSection(body, iwire.SectionDocumentFormat, formatPayload)
	if err != nil {
		return nil, err
	}
	body, err = appendByteVectorSectionKnownLen(body, iwire.SectionDocumentIDs, idsLen, ids)
	if err != nil {
		return nil, err
	}
	body, err = appendByteVectorSectionKnownLen(body, iwire.SectionDocuments, docsLen, docs)
	if err != nil {
		return nil, err
	}
	if ack != 0 {
		body, err = appendRawSection(body, iwire.SectionAckPolicy, ackPayload)
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

func (c *Client) ReplaceBatch(ctx context.Context, collection string, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) (matched, modified int, err error) {
	guard, err := c.replicatedMutationGuard(ctx, "replace_batch")
	if err != nil {
		return 0, 0, err
	}
	req := append(guard,
		collectionNameRef(collection),
		documentFormatSection(format),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
		iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{1}},
	)
	if ack != 0 {
		req = append(req, ackSection(ack))
	}
	sections, err := c.commandSections(ctx, iwire.CommandReplaceBatch, req...)
	if err != nil {
		return 0, 0, err
	}
	c.catalogVersionPlusOne.Store(0)
	matched, err = responseCount(sections, "matched_count")
	if err != nil {
		return 0, 0, err
	}
	modified, err = responseCount(sections, "modified_count")
	return matched, modified, err
}

func (c *Client) DeleteBatch(ctx context.Context, collection string, ids [][]byte, ack AckPolicy) (int, error) {
	guard, err := c.replicatedMutationGuard(ctx, "delete_batch")
	if err != nil {
		return 0, err
	}
	req := append(guard,
		collectionNameRef(collection),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
	)
	if ack != 0 {
		req = append(req, ackSection(ack))
	}
	sections, err := c.commandSections(ctx, iwire.CommandDeleteBatch, req...)
	if err != nil {
		return 0, err
	}
	c.catalogVersionPlusOne.Store(0)
	return responseCount(sections, "deleted_count")
}

func (c *Client) FlushCollection(ctx context.Context, collection string) error {
	_, err := c.commandSections(ctx, iwire.CommandFlushCollection, collectionNameRef(collection))
	return err
}

func (c *Client) FlushAll(ctx context.Context) error {
	_, err := c.commandSections(ctx, iwire.CommandFlushAll)
	return err
}

func (c *Client) Checkpoint(ctx context.Context) error {
	_, err := c.commandSections(ctx, iwire.CommandCheckpoint)
	return err
}
