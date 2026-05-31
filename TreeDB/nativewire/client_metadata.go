package nativewire

import (
	"context"
	"encoding/binary"
	"strconv"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type metadataGuardContextKey struct{}

type metadataGuardOptions struct {
	idempotencyKey            []byte
	hasIdempotencyKey         bool
	expectedCatalogVersion    uint64
	hasExpectedCatalogVersion bool
}

// WithIdempotencyKey attaches a caller-controlled idempotency key to replicated
// mutations issued with ctx. This lets callers retry the same logical mutation
// without the client generating a different key on each attempt.
func WithIdempotencyKey(ctx context.Context, key []byte) context.Context {
	opts := metadataGuardOptionsFromContext(ctx)
	opts.idempotencyKey = append([]byte(nil), key...)
	opts.hasIdempotencyKey = true
	return contextWithMetadataGuardOptions(ctx, opts)
}

// WithExpectedCatalogVersion attaches the catalog version guard to replicated
// mutations issued with ctx. When unset, the client reads or reuses the current
// version immediately before sending the mutation.
func WithExpectedCatalogVersion(ctx context.Context, version uint64) context.Context {
	opts := metadataGuardOptionsFromContext(ctx)
	opts.expectedCatalogVersion = version
	opts.hasExpectedCatalogVersion = true
	return contextWithMetadataGuardOptions(ctx, opts)
}

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
		c.clearCatalogVersionOnMismatch(err)
		return collections.CollectionMeta{}, err
	}
	c.catalogVersionPlusOne.Store(0)
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
	if err := ensureCollectionName(name); err != nil {
		return 0, err
	}
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
	if err := ensureCollectionName(collection); err != nil {
		return collections.CollectionMeta{}, err
	}
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
		c.clearCatalogVersionOnMismatch(err)
		return collections.CollectionMeta{}, err
	}
	c.catalogVersionPlusOne.Store(0)
	return firstMetaFromResponse(sections)
}

func (c *Client) ListIndexes(ctx context.Context, collection string) ([]collections.IndexDefinition, error) {
	if err := ensureCollectionName(collection); err != nil {
		return nil, err
	}
	sections, err := c.commandSections(ctx, iwire.CommandListIndexes, collectionNameRef(collection))
	if err != nil {
		return nil, err
	}
	return firstIndexVectorFromResponse(sections, c.limits)
}

func (c *Client) DropIndex(ctx context.Context, collection, index string) (collections.CollectionMeta, error) {
	if err := ensureCollectionName(collection); err != nil {
		return collections.CollectionMeta{}, err
	}
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
		c.clearCatalogVersionOnMismatch(err)
		return collections.CollectionMeta{}, err
	}
	c.catalogVersionPlusOne.Store(0)
	return firstMetaFromResponse(sections)
}

// CurrentCatalogVersion returns the server-advertised catalog/schema version
// used by guarded replicated mutations.
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
	c.catalogVersionPlusOne.Store(version + 1)
	return version, nil
}

func (c *Client) replicatedMetadataGuard(ctx context.Context, command string) ([]iwire.Section, error) {
	opts := metadataGuardOptionsFromContext(ctx)
	version := opts.expectedCatalogVersion
	if !opts.hasExpectedCatalogVersion {
		var err error
		version, err = c.CurrentCatalogVersion(ctx)
		if err != nil {
			return nil, err
		}
	}
	return c.replicatedGuardForVersion(command, version, opts)
}

func (c *Client) replicatedMutationGuard(ctx context.Context, command string) ([]iwire.Section, error) {
	opts := metadataGuardOptionsFromContext(ctx)
	version := opts.expectedCatalogVersion
	if !opts.hasExpectedCatalogVersion {
		versionPlusOne := c.catalogVersionPlusOne.Load()
		if versionPlusOne != 0 {
			version = versionPlusOne - 1
		} else {
			var err error
			version, err = c.CurrentCatalogVersion(ctx)
			if err != nil {
				return nil, err
			}
		}
	}
	return c.replicatedGuardForVersion(command, version, opts)
}

func (c *Client) replicatedGuardForVersion(command string, version uint64, opts metadataGuardOptions) ([]iwire.Section, error) {
	key, err := c.idempotencyKeyForCommand(command, opts)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: key},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
	}, nil
}

func (c *Client) idempotencyKeyForCommand(command string, opts metadataGuardOptions) ([]byte, error) {
	if opts.hasIdempotencyKey {
		if len(opts.idempotencyKey) == 0 {
			return nil, protocolError(iwire.ErrInvalidCommand, "idempotency key cannot be empty")
		}
		return append([]byte(nil), opts.idempotencyKey...), nil
	}
	return []byte("client/" + command + "/" + strconv.FormatUint(c.nextReq.Add(1), 10)), nil
}

func (c *Client) clearCatalogVersionOnMismatch(err error) {
	if isRemoteError(err, iwire.ErrCatalogVersionMismatch) {
		c.catalogVersionPlusOne.Store(0)
	}
}

func (c *Client) updateCatalogVersionFromMutationResponse(sections []iwire.Section) {
	version, ok, err := catalogVersionFromResponseMeta(sections)
	if err != nil || !ok || version == ^uint64(0) {
		c.catalogVersionPlusOne.Store(0)
		return
	}
	c.catalogVersionPlusOne.Store(version + 1)
}

func (c *Client) updateCatalogVersionFromResponseMetaFields(fields responseMetaFields) {
	if !fields.hasCatalogVersion || fields.catalogVersion == ^uint64(0) {
		c.catalogVersionPlusOne.Store(0)
		return
	}
	c.catalogVersionPlusOne.Store(fields.catalogVersion + 1)
}

func (c *Client) clearCatalogVersionAfterOpaqueMutation() {
	c.catalogVersionPlusOne.Store(0)
}

func catalogVersionFromResponseMeta(sections []iwire.Section) (uint64, bool, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil || !ok {
		return 0, ok, err
	}
	fields, err := decodeResponseMetaFields(raw, "", "")
	if err != nil {
		return 0, false, err
	}
	return fields.catalogVersion, fields.hasCatalogVersion, nil
}

func metadataGuardOptionsFromContext(ctx context.Context) metadataGuardOptions {
	if ctx == nil {
		return metadataGuardOptions{}
	}
	opts, _ := ctx.Value(metadataGuardContextKey{}).(metadataGuardOptions)
	return opts
}

func contextWithMetadataGuardOptions(ctx context.Context, opts metadataGuardOptions) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, metadataGuardContextKey{}, opts)
}
