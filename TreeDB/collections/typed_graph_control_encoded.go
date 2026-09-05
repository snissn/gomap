package collections

func (s *typedGraphPublicationState) prepareEncodedBounds() (err error) {
	if s.limits.EncodedOutputBytes <= 0 {
		return nil
	}
	s.controlEncodedBytes, err = typedGraphControlEncodedBound(s.catalog.meta)
	if err != nil {
		return err
	}
	s.manifestEncodedBytes, s.deletePartEncodedBytes, err = typedGraphManifestEncodedBounds(s.catalog.meta)
	return err
}

func typedGraphWriteEncodedBound(input columnWritePublishInput, state *typedGraphPublicationState) (int64, error) {
	if state == nil || state.controlEncodedBytes <= 0 {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	bound, err := typedGraphColumnEncodedBound(input)
	if err != nil || (len(input.documents) == 0 && len(input.sourceDeleteDocuments) == 0) {
		return bound, err
	}
	if err := addTypedGraphEncodedBytes(&bound, state.controlEncodedBytes); err != nil {
		return 0, err
	}
	index := 0
	switch input.operation {
	case ColumnPublishOperationInsert:
	case ColumnPublishOperationUpdate:
		index = 1
	case ColumnPublishOperationDelete:
		index = 2
	default:
		return 0, ErrHybridSearchUnsupported
	}
	if len(input.documents) == 0 {
		index = 2 // source removal without inserted rows
	}
	if err := addTypedGraphEncodedBytes(&bound, state.manifestEncodedBytes[index]); err != nil {
		return 0, err
	}
	if len(input.sourceDeleteDocuments) != 0 && len(input.documents) != 0 {
		if err := addTypedGraphEncodedBytes(&bound, state.deletePartEncodedBytes); err != nil {
			return 0, err
		}
	}
	return bound, nil
}

// Compute once during lifecycle initialization/reconciliation. Ordinary writes
// retain only this scalar on their existing immutable publication state.
func typedGraphControlEncodedBound(meta CollectionMeta) (int64, error) {
	identity := ColumnManifestIdentity{Generation: ^uint64(0), Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: ^uint64(0)}
	future, err := columnGraphRebuildUpdatedMeta(meta, identity, ^uint64(0))
	if err != nil {
		return 0, err
	}
	future.Options.ColumnStore.PhysicalMutationParts = ^uint64(0)
	raw, err := encodeNormalizedCollectionMeta(future)
	if err != nil {
		return 0, err
	}
	var total int64
	if err := typedGraphRootEncodedEntry(&total, len(systemCollectionMetaKey(meta.Name)), len(raw)); err != nil {
		return 0, err
	}
	// A publisher may update only a subset. Charging all schema roots also
	// covers roots created by the first write without a per-write inventory.
	for _, name := range collectionRootNames(future) {
		if err := typedGraphRootEncodedEntry(&total, len(systemCollectionRootKey(name)), 8); err != nil {
			return 0, err
		}
	}
	if err := typedGraphRootEncodedEntry(&total, len(systemCollectionDocumentGenerationKey(meta.Name)), 8); err != nil {
		return 0, err
	}
	return total, nil
}
