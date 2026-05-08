package nativewire

import (
	"context"
	"encoding/binary"
	"strconv"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (c *Client) CreateCollection(ctx context.Context, meta collections.CollectionMeta) (collections.CollectionMeta, error) {
	meta, err := normalizeClientCollectionMeta(meta)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	guard, err := c.replicatedMetadataGuard(ctx, "create_collection")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	req := append(guard, iwire.Section{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(meta)})
	sections, err := c.commandSections(ctx, iwire.CommandCreateCollection, req...)
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
	guard, err := c.replicatedMetadataGuard(ctx, "create_index")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	req := append(guard,
		collectionNameRef(collection),
		iwire.Section{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinition(def)},
	)
	sections, err := c.commandSections(ctx, iwire.CommandCreateIndex, req...)
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
	guard, err := c.replicatedMetadataGuard(ctx, "drop_index")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	req := append(guard,
		collectionNameRef(collection),
		iwire.Section{ID: iwire.SectionIndexName, Bytes: encodeIndexName(index)},
	)
	sections, err := c.commandSections(ctx, iwire.CommandDropIndex, req...)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	return firstMetaFromResponse(sections)
}

// CurrentCatalogVersion returns the catalog commit sequence advertised by the
// server for guarded metadata mutations.
func (c *Client) CurrentCatalogVersion(ctx context.Context) (uint64, error) {
	stats, err := c.Stats(ctx)
	if err != nil {
		return 0, err
	}
	raw, ok := stats[nativeStatsPrefix+"catalog.version"]
	if !ok {
		return 0, protocolError(iwire.ErrInvalidCommand, "catalog version is unavailable")
	}
	version, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, protocolError(iwire.ErrMalformedFrame, "invalid catalog version %q", raw)
	}
	return version, nil
}

func (c *Client) replicatedMetadataGuard(ctx context.Context, command string) ([]iwire.Section, error) {
	version, err := c.CurrentCatalogVersion(ctx)
	if err != nil {
		return nil, err
	}
	key := []byte("client/" + command + "/" + strconv.FormatUint(c.nextReq.Add(1), 10))
	return []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: key},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
	}, nil
}
