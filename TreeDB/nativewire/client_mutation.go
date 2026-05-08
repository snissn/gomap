package nativewire

import (
	"context"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (c *Client) InsertBatch(ctx context.Context, collection string, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy) ([][]byte, error) {
	guard, err := c.replicatedMutationGuard(ctx, "insert_batch")
	if err != nil {
		return nil, err
	}
	req := append(guard,
		collectionNameRef(collection),
		documentFormatSection(format),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
	)
	if ack != 0 {
		req = append(req, ackSection(ack))
	}
	sections, err := c.commandSections(ctx, iwire.CommandInsertBatch, req...)
	if err != nil {
		c.clearCatalogVersionOnMismatch(err)
		return nil, err
	}
	c.updateCatalogVersionAfterMutation(sections)
	rawIDs, ok, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrMalformedFrame, "insert_batch missing result document_ids")
	}
	return decodeByteVectorCloned(rawIDs, c.limits)
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
	c.updateCatalogVersionAfterMutation(sections)
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
		c.clearCatalogVersionOnMismatch(err)
		return 0, err
	}
	c.updateCatalogVersionAfterMutation(sections)
	return responseCount(sections, "deleted_count")
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
