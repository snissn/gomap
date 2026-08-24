package collections

import "errors"

func (idx *VectorIndex) liveDeltaActiveLocked() bool {
	return idx != nil && idx.nativePersistent && idx.liveDeltaEnabled.Load() && idx.searchView.Load() != nil
}

func (idx *VectorIndex) ensureLiveDeltaLocked() (*VectorIndex, error) {
	if idx.liveDelta != nil {
		return idx.liveDelta, nil
	}
	delta, err := newVectorIndex(nil, VectorIndexOptions{
		Name:                idx.name,
		Field:               idx.field,
		Metric:              idx.metric,
		Encoding:            idx.encoding,
		Dimensions:          idx.dimensions,
		M:                   idx.m,
		EfConstruction:      idx.efConstruction,
		EfSearch:            idx.efSearch,
		RebuildDeletedRatio: idx.rebuildDeletedRatio,
		schemaGeneration:    idx.schemaGeneration,
	})
	if err != nil {
		return nil, err
	}
	delta.parallelReciprocalLinks = idx.parallelReciprocalLinks
	delta.constructionWorkers = idx.constructionWorkers
	delta.nativePersistent = true
	delta.sourceDocumentRootsValid = true
	delta.trackSearchViewDirty = true
	idx.liveDelta = delta
	return delta, nil
}

func (idx *VectorIndex) insertLiveVectorBatchLocked(documentIDs [][]byte, vectors [][]float32) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	delta, err := idx.ensureLiveDeltaLocked()
	if err != nil {
		return err
	}
	beforeMutation := idx.mutationSeq
	beforeDeltaMutation := delta.mutationSeq
	if err := delta.insertVectorBatchLocked(documentIDs, vectors); err != nil {
		return err
	}
	for _, documentID := range documentIDs {
		idx.tombstoneDocumentIDLocked(documentID)
	}
	if delta.mutationSeq != beforeDeltaMutation && idx.mutationSeq == beforeMutation {
		idx.markGraphChangedLocked()
	}
	if len(delta.currentNode) >= defaultVectorIndexLiveDeltaRows {
		// Make the bounded delta visible before folding it into the base. Existing
		// readers keep this immutable generation while construction proceeds.
		idx.publishSearchViewLocked(false)
		if err := idx.foldLiveDeltaLocked(); err != nil {
			return err
		}
		idx.publishSearchViewLocked(false)
	}
	return nil
}

func (idx *VectorIndex) tombstoneLiveDocumentLocked(documentID []byte) {
	idx.tombstoneDocumentIDLocked(documentID)
	if idx.liveDelta != nil {
		beforeMutation := idx.liveDelta.mutationSeq
		idx.liveDelta.tombstoneDocumentIDLocked(documentID)
		if idx.liveDelta.mutationSeq != beforeMutation {
			idx.markGraphChangedLocked()
		}
	}
}

func (idx *VectorIndex) foldLiveDeltaLocked() error {
	delta := idx.liveDelta
	if delta == nil || len(delta.currentNode) == 0 {
		idx.liveDelta = nil
		return nil
	}
	documentIDs := make([][]byte, 0, len(delta.currentNode))
	vectors := make([][]float32, 0, len(delta.currentNode))
	for nodeID := range delta.nodes {
		node := &delta.nodes[nodeID]
		if node.deleted || delta.currentNode[string(node.documentID)] != nodeID {
			continue
		}
		documentIDs = append(documentIDs, node.documentID)
		vector := node.vector
		if len(vector) == 0 {
			vector = make([]float32, node.vectorDimensions())
			for dimension := range vector {
				vector[dimension] = node.vectorValueAt(dimension)
			}
		}
		vectors = append(vectors, vector)
	}
	if err := idx.insertVectorBatchLocked(documentIDs, vectors); err != nil {
		return err
	}
	idx.liveDelta = nil
	idx.liveDeltaCutovers++
	return nil
}

func (idx *VectorIndex) foldLiveDeltaForPersistence() error {
	if idx == nil {
		return nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.liveDelta == nil {
		return nil
	}
	if err := idx.foldLiveDeltaLocked(); err != nil {
		return err
	}
	idx.publishSearchViewLocked(false)
	return nil
}
