package collections

import "context"

// ProveVectorPartitionColocatedMutationV1 captures one source/live outcome
// while the prepared command owner still holds publication admission. It does
// not prove consensus or retain retry history: the applying FSM must bind this
// proof to its original durable outcome. Unlike insert's overlay-only proof,
// unchanged base content and a genuinely missing target are explicit outcomes.
func (owner *CommandWALAdmittedCollection) ProveVectorPartitionColocatedMutationV1(ctx context.Context, manifest VectorPartitionManifestV1, id, expected []byte, deletion bool, matched, affected int64) (VectorIndexPartitionLiveStatusV1, error) {
	var zero VectorIndexPartitionLiveStatusV1
	if err := owner.validate(); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if len(id) == 0 || matched < 0 || matched > 1 || affected < 0 || affected > 1 || (!deletion && affected > matched) || (deletion && matched != 0) {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	c := owner.collection
	if manifest.Collection != c.collectionName() {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	defer snap.Close()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return zero, err
	}
	if catalog == nil || !VectorPartitionLiveDocumentProofSupportedV1(catalog.meta) {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	generation, err := vectorIndexDocumentGenerationForCollection(snap, c.collectionName())
	if err != nil {
		return zero, err
	}
	state, ok := snap.StateToken()
	if !ok || generation == 0 {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx := c.registeredVectorIndex(manifest.IndexName)
	if idx == nil {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	live := idx.partitionLive
	if live == nil || live.invalid || !live.bindingDurable || live.generation != manifest.Generation || live.indexDefinitionDigest != manifest.IndexDefinitionDigest || live.source != vectorPartitionLiveSourceV1(manifest) || !vectorPartitionLiveRoutingIdentityMatchesV1(live.packDomains, manifest) || !idx.sourceDocumentRootsValid || len(idx.unnotifiedDocumentIDs) != 0 || !idx.sourceDocumentStateValid || idx.sourceDocumentState != state || idx.sourceDocumentGeneration != generation || live.coverage != generation {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	current, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, catalog.primaryRootName, id, nil)
	if err != nil {
		return zero, err
	}
	key := string(id)
	liveOwner, overlaid := live.ownerV1(key)
	if deletion || matched == 0 {
		if found || (overlaid && !liveOwner.deleted) || (affected != 0 && !overlaid) {
			return zero, ErrVectorIndexPartitionLiveMismatchV1
		}
	} else {
		if !found || (overlaid && liveOwner.deleted) || (affected != 0 && !overlaid) {
			return zero, ErrVectorIndexPartitionLiveMismatchV1
		}
		if columnStoreCanReconstructDocument(catalog.meta) {
			current, err = c.reconstructColumnDocumentAtSnapshot(snap, catalog, id, current)
			if err != nil {
				return zero, err
			}
		}
		equal, err := vectorPartitionLiveDocumentEqualV1(catalog.meta, id, expected, current)
		if err != nil {
			return zero, err
		}
		if !equal {
			return zero, ErrVectorIndexPartitionLiveMismatchV1
		}
	}
	// Exact membership checks are bounded by the admitted domain count; no
	// population scan or top-k absence inference is used for deletion.
	for domain, delta := range live.domains {
		delta.mu.RLock()
		_, present := delta.currentNode[key]
		delta.mu.RUnlock()
		want := overlaid && !liveOwner.deleted && liveOwner.domain == domain
		if present != want {
			return zero, ErrVectorIndexPartitionLiveMismatchV1
		}
	}
	if overlaid && !liveOwner.deleted && live.domains[liveOwner.domain] == nil {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	if affected != 0 && live.revision == 0 {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return VectorIndexPartitionLiveStatusV1{Generation: live.generation, Revision: live.revision, Coverage: live.coverage, MutatedIDs: live.ownerCountV1(), Cutovers: live.cutovers}, nil
}
