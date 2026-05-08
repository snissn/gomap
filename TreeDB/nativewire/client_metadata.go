package nativewire

import (
	"context"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (c *Client) CreateCollection(ctx context.Context, meta collections.CollectionMeta) (collections.CollectionMeta, error) {
	meta, err := normalizeClientCollectionMeta(meta)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	sections, err := c.commandSections(ctx, iwire.CommandCreateCollection, iwire.Section{
		ID:    iwire.SectionCollectionMeta,
		Bytes: encodeCollectionMeta(meta),
	})
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	return firstMetaFromResponse(sections)
}

func (c *Client) ListCollections(ctx context.Context) ([]collections.CollectionMeta, error) {
	sections, err := c.commandSections(ctx, iwire.CommandListCollections)
	if err != nil {
		return nil, err
	}
	return firstMetaVectorFromResponse(sections, c.limits)
}

func (c *Client) OpenCollection(ctx context.Context, name string) (CollectionHandle, error) {
	sections, err := c.commandSections(ctx, iwire.CommandOpenCollection, collectionNameRef(name))
	if err != nil {
		return 0, err
	}
	return firstHandleFromResponse(sections)
}

func (c *Client) CloseCollection(ctx context.Context, handle CollectionHandle) error {
	_, err := c.commandSections(ctx, iwire.CommandCloseCollection, collectionHandleRef(handle))
	return err
}

func (c *Client) CreateIndex(ctx context.Context, collection string, def collections.IndexDefinition) (collections.CollectionMeta, error) {
	if err := normalizeClientIndexDefinition(def); err != nil {
		return collections.CollectionMeta{}, err
	}
	sections, err := c.commandSections(ctx, iwire.CommandCreateIndex,
		collectionNameRef(collection),
		iwire.Section{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinition(def)},
	)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	return firstMetaFromResponse(sections)
}

func (c *Client) ListIndexes(ctx context.Context, collection string) ([]collections.IndexDefinition, error) {
	sections, err := c.commandSections(ctx, iwire.CommandListIndexes, collectionNameRef(collection))
	if err != nil {
		return nil, err
	}
	return firstIndexVectorFromResponse(sections, c.limits)
}

func (c *Client) DropIndex(ctx context.Context, collection, index string) (collections.CollectionMeta, error) {
	if err := ensureIndexName(index); err != nil {
		return collections.CollectionMeta{}, err
	}
	sections, err := c.commandSections(ctx, iwire.CommandDropIndex,
		collectionNameRef(collection),
		iwire.Section{ID: iwire.SectionIndexName, Bytes: encodeIndexName(index)},
	)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	return firstMetaFromResponse(sections)
}
