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
		c.clearCatalogVersionOnMismatch(err)
		return nil, err
	}
	var sectionBuf [4]iwire.Section
	sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, c.limits)
	if err != nil {
		return nil, err
	}
	c.updateCatalogVersionFromMutationResponse(sections)
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
	if err != nil {
		c.clearCatalogVersionOnMismatch(err)
		return err
	}
	return nil
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
		var err error
		total, err = addRequestBodyLen(total, iwire.SectionEncodedLen(section))
		if err != nil {
			return nil, err
		}
	}
	var err error
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionCollectionRef, 0, refLen)+refLen)
	if err != nil {
		return nil, err
	}
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionDocumentFormat, 0, len(formatPayload))+len(formatPayload))
	if err != nil {
		return nil, err
	}
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionDocumentIDs, 0, idsLen)+idsLen)
	if err != nil {
		return nil, err
	}
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionDocuments, 0, docsLen)+docsLen)
	if err != nil {
		return nil, err
	}
	if ack != 0 {
		total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionAckPolicy, 0, len(ackPayload))+len(ackPayload))
		if err != nil {
			return nil, err
		}
	}
	dst, err = growRequestBody(dst, total)
	if err != nil {
		return nil, err
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
		c.clearCatalogVersionOnMismatch(err)
		return 0, 0, err
	}
	return c.replaceBatchCountsFromResponse(sections)
}

func (c *Client) UpdateBSONSet(ctx context.Context, collection string, id []byte, fields []collections.BSONSetField, ack AckPolicy) (matched, modified int, err error) {
	return c.updateBSONSet(ctx, collection, 0, false, id, fields, ack)
}

func (c *Client) UpdateBSONSetHandle(ctx context.Context, handle CollectionHandle, id []byte, fields []collections.BSONSetField, ack AckPolicy) (matched, modified int, err error) {
	return c.updateBSONSet(ctx, "", handle, true, id, fields, ack)
}

func (c *Client) updateBSONSet(ctx context.Context, collection string, handle CollectionHandle, useHandle bool, id []byte, fields []collections.BSONSetField, ack AckPolicy) (matched, modified int, err error) {
	if c == nil {
		return 0, 0, io.ErrClosedPipe
	}
	guard, err := c.replicatedMutationGuard(ctx, "update_bson_set")
	if err != nil {
		return 0, 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	body, err := appendUpdateBSONSetRequestBodyRef(c.requestBody[:0], collection, handle, useHandle, id, fields, ack, guard)
	if err != nil {
		return 0, 0, err
	}
	_, response, err := c.roundTripLocked(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err != nil {
		c.clearCatalogVersionOnMismatch(err)
		return 0, 0, err
	}
	var sectionBuf [2]iwire.Section
	sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, c.limits)
	if err != nil {
		return 0, 0, err
	}
	return c.replaceBatchCountsFromResponse(sections)
}

func appendUpdateBSONSetRequestBodyRef(dst []byte, collection string, handle CollectionHandle, useHandle bool, id []byte, fields []collections.BSONSetField, ack AckPolicy, guard []iwire.Section) ([]byte, error) {
	var commandHeader [16]byte
	commandPayload := iwire.AppendCommandHeader(commandHeader[:0], iwire.CommandHeader{ID: iwire.CommandUpdateBSONSet, Version: 1})
	var refBuf [1 + binary.MaxVarintLen64]byte
	var refPayload []byte
	refLen := collectionNameRefPayloadLen(collection)
	if useHandle {
		refPayload = appendCollectionHandleRefPayload(refBuf[:0], handle)
		refLen = len(refPayload)
	}
	var ackBuf [binary.MaxVarintLen64]byte
	var ackPayload []byte
	if ack != 0 {
		ackPayload = binary.AppendUvarint(ackBuf[:0], uint64(ack))
	}
	idsLen := iwire.ByteVectorEncodedLen([][]byte{id})
	namesLen := bsonSetFieldNamesEncodedLen(fields)
	valuesLen := bsonSetFieldValuesEncodedLen(fields)
	total := iwire.SectionHeaderEncodedLen(iwire.SectionCommandHeader, 0, len(commandPayload)) + len(commandPayload)
	for _, section := range guard {
		var err error
		total, err = addRequestBodyLen(total, iwire.SectionEncodedLen(section))
		if err != nil {
			return nil, err
		}
	}
	var err error
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionCollectionRef, 0, refLen)+refLen)
	if err != nil {
		return nil, err
	}
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionDocumentIDs, 0, idsLen)+idsLen)
	if err != nil {
		return nil, err
	}
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionUpdateFieldNames, 0, namesLen)+namesLen)
	if err != nil {
		return nil, err
	}
	total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionUpdateFieldValues, 0, valuesLen)+valuesLen)
	if err != nil {
		return nil, err
	}
	if ack != 0 {
		total, err = addRequestBodyLen(total, iwire.SectionHeaderEncodedLen(iwire.SectionAckPolicy, 0, len(ackPayload))+len(ackPayload))
		if err != nil {
			return nil, err
		}
	}
	dst, err = growRequestBody(dst, total)
	if err != nil {
		return nil, err
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
	body, err = appendByteVectorSectionKnownLen(body, iwire.SectionDocumentIDs, idsLen, [][]byte{id})
	if err != nil {
		return nil, err
	}
	body, err = appendBSONSetFieldNamesSectionKnownLen(body, namesLen, fields)
	if err != nil {
		return nil, err
	}
	body, err = appendBSONSetFieldValuesSectionKnownLen(body, valuesLen, fields)
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

func bsonSetFieldNamesEncodedLen(fields []collections.BSONSetField) int {
	total := uvarintLen(uint64(len(fields)))
	for _, field := range fields {
		total += uvarintLen(uint64(len(field.Key)))
	}
	for _, field := range fields {
		total += len(field.Key)
	}
	return total
}

func bsonSetFieldValuesEncodedLen(fields []collections.BSONSetField) int {
	total := uvarintLen(uint64(len(fields)))
	for _, field := range fields {
		total += uvarintLen(uint64(1 + len(field.Value.Value)))
	}
	for _, field := range fields {
		total += 1 + len(field.Value.Value)
	}
	return total
}

func appendBSONSetFieldNamesSectionKnownLen(dst []byte, encodedLen int, fields []collections.BSONSetField) ([]byte, error) {
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionUpdateFieldNames, 0, encodedLen)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, uint64(len(fields)))
	for _, field := range fields {
		body = binary.AppendUvarint(body, uint64(len(field.Key)))
	}
	for _, field := range fields {
		body = append(body, field.Key...)
	}
	return body, nil
}

func appendBSONSetFieldValuesSectionKnownLen(dst []byte, encodedLen int, fields []collections.BSONSetField) ([]byte, error) {
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionUpdateFieldValues, 0, encodedLen)
	if err != nil {
		return nil, err
	}
	body = binary.AppendUvarint(body, uint64(len(fields)))
	for _, field := range fields {
		body = binary.AppendUvarint(body, uint64(1+len(field.Value.Value)))
	}
	for _, field := range fields {
		body = append(body, byte(field.Value.Type))
		body = append(body, field.Value.Value...)
	}
	return body, nil
}

func (c *Client) replaceBatchCountsFromResponse(sections []iwire.Section) (int, int, error) {
	fields, err := responseMetaFieldsFromSections(sections, "matched_count", "modified_count")
	if err != nil {
		c.clearCatalogVersionAfterOpaqueMutation()
		return 0, 0, err
	}
	if !fields.hasCount1 {
		c.clearCatalogVersionAfterOpaqueMutation()
		return 0, 0, protocolError(iwire.ErrMalformedFrame, "response_meta missing matched_count")
	}
	if !fields.hasCount2 {
		c.clearCatalogVersionAfterOpaqueMutation()
		return 0, 0, protocolError(iwire.ErrMalformedFrame, "response_meta missing modified_count")
	}
	c.updateCatalogVersionFromResponseMetaFields(fields)
	return fields.count1, fields.count2, nil
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
		c.clearCatalogVersionOnMismatch(err)
		return 0, err
	}
	return c.deleteBatchCountFromResponse(sections)
}

func (c *Client) deleteBatchCountFromResponse(sections []iwire.Section) (int, error) {
	fields, err := responseMetaFieldsFromSections(sections, "deleted_count", "")
	if err != nil {
		c.clearCatalogVersionAfterOpaqueMutation()
		return 0, err
	}
	if !fields.hasCount1 {
		c.clearCatalogVersionAfterOpaqueMutation()
		return 0, protocolError(iwire.ErrMalformedFrame, "response_meta missing deleted_count")
	}
	c.updateCatalogVersionFromResponseMetaFields(fields)
	return fields.count1, nil
}

func (c *Client) FlushCollection(ctx context.Context, collection string) error {
	return c.FlushCollectionWithAck(ctx, collection, 0)
}

func (c *Client) FlushCollectionWithAck(ctx context.Context, collection string, ack AckPolicy) error {
	if err := ensureCollectionName(collection); err != nil {
		return err
	}
	req := []iwire.Section{collectionNameRef(collection)}
	if ack != 0 {
		req = append(req, ackSection(ack))
	}
	_, err := c.commandSections(ctx, iwire.CommandFlushCollection, req...)
	return err
}

func (c *Client) FlushAll(ctx context.Context) error {
	return c.FlushAllWithAck(ctx, 0)
}

func (c *Client) FlushAllWithAck(ctx context.Context, ack AckPolicy) error {
	var req []iwire.Section
	if ack != 0 {
		req = append(req, ackSection(ack))
	}
	_, err := c.commandSections(ctx, iwire.CommandFlushAll, req...)
	return err
}

func (c *Client) Checkpoint(ctx context.Context) error {
	return c.CheckpointWithAck(ctx, 0)
}

func (c *Client) CheckpointWithAck(ctx context.Context, ack AckPolicy) error {
	var req []iwire.Section
	if ack != 0 {
		req = append(req, ackSection(ack))
	}
	_, err := c.commandSections(ctx, iwire.CommandCheckpoint, req...)
	return err
}
