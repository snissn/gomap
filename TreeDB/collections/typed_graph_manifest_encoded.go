package collections

// Binary record sizes only: no encoded assets or invented publication identity.
// Calculate at schema admission so sort-key preparation stays off the write path.
func typedGraphManifestEncodedBounds(meta CollectionMeta) (bounds [3]int64, deletePart int64, err error) {
	cfg := meta.Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		return bounds, 0, ErrHybridSearchUnsupported
	}
	keys, err := typedColumnPartPublicationSortKey(*cfg, columnStoreTypedColumnPartFields(*cfg))
	if err != nil {
		return bounds, 0, err
	}
	partBytes := func(kind ColumnAssetKind, operation ColumnPublishOperation, sortKeys []ColumnSortKey) (int64, error) {
		n := int64(6 + 10*8 + 8) // magic/version, scalar fields, sort-key count
		for _, s := range []string{string(kind), cfg.AssetManager.Namespace, string(operation), string(columnManifestPartRoleForPublish(operation))} {
			if err := addTypedGraphEncodedBytes(&n, int64(manifestStringEncodedSize(s))); err != nil {
				return 0, err
			}
		}
		for _, key := range sortKeys {
			for _, s := range []string{key.Column, string(key.Direction)} {
				if err := addTypedGraphEncodedBytes(&n, int64(manifestStringEncodedSize(s))); err != nil {
					return 0, err
				}
			}
		}
		if err := typedGraphRootEncodedEntry(&n, len(columnManifestPartRecordPrefix)+16, 0); err != nil {
			return 0, err
		}
		return n, nil
	}
	for i, operation := range [...]ColumnPublishOperation{ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete} {
		headerBytes := 6 + 8*8 + manifestStringEncodedSize(meta.Name) + manifestStringEncodedSize(string(operation))
		if err := typedGraphRootEncodedEntry(&bounds[i], len(columnManifestHeaderRecordKey), headerBytes); err != nil {
			return bounds, 0, err
		}
		if err := typedGraphRootEncodedEntry(&bounds[i], len(columnManifestIdentityRecordKey), columnManifestIdentityRecordSize); err != nil {
			return bounds, 0, err
		}
		row, err := partBytes(ColumnAssetKindTCS1PartImage, operation, nil)
		if err != nil {
			return bounds, 0, err
		}
		if err := addTypedGraphEncodedBytes(&bounds[i], row); err != nil {
			return bounds, 0, err
		}
		if operation == ColumnPublishOperationDelete {
			deletePart = row
			continue
		}
		typed, err := partBytes(ColumnAssetKindTCS1TypedColumnPart, operation, keys)
		if err != nil {
			return bounds, 0, err
		}
		if err := addTypedGraphEncodedBytes(&bounds[i], typed); err != nil {
			return bounds, 0, err
		}
	}
	return bounds, deletePart, nil
}
