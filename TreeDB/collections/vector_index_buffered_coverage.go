package collections

import (
	"bytes"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// bufferedVectorPublication belongs to one publication under vector admission.
// It pins installed identities and the durable baseline before any runs reset.
type bufferedVectorPublication struct {
	generation  uint64
	indexes     []*VectorIndex
	documentIDs [][]byte
}

// The mutation pins only identities that already account for its predecessor.
// Admission can hand off for a foreign drain; that establishes a different
// baseline pointer and cannot reuse this predecessor proof.
type unnotifiedVectorMutation struct {
	indexes           []*VectorIndex
	admissionBaseline *uint64
}

func (c *Collection) captureUnnotifiedVectorMutation() (*unnotifiedVectorMutation, error) {
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return nil, nil
	}
	generation, err := c.currentVectorIndexDocumentGenerationForAdmission()
	if err != nil {
		return nil, err
	}
	proof := &unnotifiedVectorMutation{}
	if coord := c.collectionSchemaCoordinator(); coord != nil {
		proof.admissionBaseline = coord.nativeVectorBaseline.Load()
	}
	for _, index := range indexes {
		if index.hasSourceDocumentReconciliationBaseline(generation) {
			proof.indexes = append(proof.indexes, index)
		}
	}
	return proof, nil
}

func (c *Collection) markVectorIndexDocumentsUnnotified(documentIDs [][]byte, proof *unnotifiedVectorMutation) {
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return
	}
	generation, err := c.currentVectorIndexDocumentGenerationForAdmission()
	sameAdmission := proof != nil
	if coord := c.collectionSchemaCoordinator(); coord != nil && proof != nil {
		sameAdmission = coord.nativeVectorBaseline.Load() == proof.admissionBaseline
	}
	for _, index := range indexes {
		accounted := false
		if sameAdmission && err == nil {
			for _, predecessor := range proof.indexes {
				if predecessor == index {
					accounted = true
					break
				}
			}
		}
		index.mu.Lock()
		if accounted && index.sourceDocumentRootsValid {
			index.sourceDocumentAccountedGeneration = generation
			index.sourceDocumentAccountedGenerationValid = true
		} else {
			index.sourceDocumentRootsValid = false
			index.sourceDocumentStateValid = false
		}
		if index.unnotifiedDocumentIDs == nil {
			index.unnotifiedDocumentIDs = make(map[string]struct{}, len(documentIDs))
		}
		for _, id := range documentIDs {
			if len(id) != 0 {
				index.unnotifiedDocumentIDs[string(id)] = struct{}{}
			}
		}
		index.searchViewAcknowledged = false
		index.searchViewCurrent.Store(false)
		index.mu.Unlock()
	}
}

func (idx *VectorIndex) clearUnnotifiedDocuments(documentIDs [][]byte) {
	idx.mu.Lock()
	for _, id := range documentIDs {
		delete(idx.unnotifiedDocumentIDs, string(id))
	}
	idx.mu.Unlock()
}

func (c *Collection) captureBufferedVectorPublicationLocked(domain *collectionWriteDomain, unit *indexedFlushUnit) (*bufferedVectorPublication, error) {
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return nil, nil
	}
	generation, err := c.currentVectorIndexDocumentGenerationWithWriteDomainLocked()
	if err != nil {
		return nil, err
	}
	proof := &bufferedVectorPublication{generation: generation}
	for _, index := range indexes {
		index.mu.RLock()
		eligible := (index.nativePersistent || index.partitionLiveOnly) && index.sourceDocumentReconciliationBaselineLocked(generation)
		index.mu.RUnlock()
		if eligible {
			proof.indexes = append(proof.indexes, index)
		}
	}
	if len(proof.indexes) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	addID := func(id []byte) {
		if len(id) == 0 {
			return
		}
		if _, exists := seen[string(id)]; exists {
			return
		}
		seen[string(id)] = struct{}{}
		proof.documentIDs = append(proof.documentIDs, bytes.Clone(id))
	}
	addRuns := func(runs []memtable.Table) error {
		for _, run := range runs {
			if run == nil {
				continue
			}
			iter := run.NewIterator(nil, nil)
			for ; iter.Valid(); iter.Next() {
				addID(iter.UnsafeKey())
			}
			err := iter.Error()
			_ = iter.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}
	addOverlay := func(overlay *bufferedPrimaryOverlay) {
		for _, entry := range overlay.appendEntries(nil) {
			addID(entry.key)
		}
	}
	name := collectionPrimaryRootName(bufferedDomainCollectionName(domain, c.meta.Name))
	if unit != nil {
		if err := addRuns(unit.rootRuns[name]); err != nil {
			return nil, err
		}
		addOverlay(unit.primaryOverlay)
	} else {
		if err := addRuns(pendingIndexedRootRunMapLocked(domain)[name]); err != nil {
			return nil, err
		}
		if domain.table != nil {
			if err := addRuns([]memtable.Table{domain.table}); err != nil {
				return nil, err
			}
		}
		addOverlay(domain.primaryOverlay)
		for _, pending := range domain.indexedPublishingUnits {
			addOverlay(pending.primaryOverlay)
		}
		for _, pending := range domain.indexedFlushUnits {
			addOverlay(pending.primaryOverlay)
		}
	}
	return proof, nil
}

// Admission still belongs to the publisher. Read its pending overlay first,
// then its pinned published catalog; an already maintained tail must win over
// an older published prefix for the same ID.
func (c *Collection) reconcileBufferedVectorPublication(proof *bufferedVectorPublication) error {
	return c.reconcileBufferedVectorPublicationWithMutationState(proof, false)
}

// The loader's flush inherits its graph mutex; ordinary publishers acquire it.
func (c *Collection) reconcileBufferedVectorPublicationWithMutationState(proof *bufferedVectorPublication, vectorMutationLocked bool) (repairErr error) {
	if proof == nil || len(proof.documentIDs) == 0 {
		return nil
	}
	defer func() {
		if repairErr != nil {
			for _, index := range proof.indexes {
				if c.isRegisteredVectorIndex(index) {
					index.invalidateSourceDocumentRoots()
				}
			}
			repairErr = commitAmbiguousError("buffered native vector reconciliation", repairErr)
		}
	}()
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshotWithWriteDomainLockState(snap, false)
	if err != nil {
		return err
	}
	generation, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return err
	}
	state, ok := snap.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	documents := make([][]byte, len(proof.documentIDs))
	buffered := make([]bool, len(proof.documentIDs))
	domain := c.writeDomain
	domain.mu.Lock()
	for i, id := range proof.documentIDs {
		var found bool
		documents[i], buffered[i], found, err = c.getBufferedDocumentIntoLocked(domain, id, nil)
		if buffered[i] && !found {
			documents[i] = nil
		}
		if err != nil {
			break
		}
	}
	domain.mu.Unlock()
	if err != nil {
		return err
	}
	for i, id := range proof.documentIDs {
		if buffered[i] {
			continue
		}
		var found bool
		documents[i], found, err = collectionGetAppendAtCatalogRoot(snap, catalog, catalog.primaryRootName, id, nil)
		if err != nil {
			return err
		}
		if !found {
			documents[i] = nil
			continue
		}
		if columnStoreCanReconstructDocument(catalog.meta) {
			documents[i], err = c.reconstructColumnDocumentAtSnapshot(snap, catalog, id, documents[i])
			if err != nil {
				return err
			}
		}
	}
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return err
	}
	defer func() { _ = materializer.Close() }()
	if !vectorMutationLocked {
		unlockMutation := c.lockVectorIndexMutation()
		defer unlockMutation()
	}
	unlockPublication := c.lockNativeVectorIndexPublicationRead()
	defer unlockPublication()
	for _, index := range proof.indexes {
		if !c.isRegisteredVectorIndex(index) {
			continue
		}
		index.mu.RLock()
		eligible := index.sourceDocumentReconciliationBaselineLocked(proof.generation)
		index.mu.RUnlock()
		if !eligible {
			continue
		}
		for i, id := range proof.documentIDs {
			if err := index.reconcileBufferedStoredDocument(materializer, id, documents[i]); err != nil {
				index.invalidateSourceDocumentRoots()
				return err
			}
		}
		index.clearUnnotifiedDocuments(proof.documentIDs)
		// Retain the genuine published baseline even while unrelated raw IDs
		// keep this index unavailable. Never erase those missing-ID proofs.
		index.recordSourceDocumentState(generation, state)
		if err := c.recordReconciledVectorIndexCoverage([]*VectorIndex{index}); err != nil {
			return err
		}
	}
	return nil
}

// Publication repair may replay an already maintained logical row. Compare the
// effective delta before the base and all scalar presence/bytes before mutation.
func (idx *VectorIndex) reconcileBufferedStoredDocument(materializer *StoredDocumentJSONMaterializer, documentID, document []byte) error {
	vector, scalarRow, err := idx.parseStoredVectorRow(materializer, documentID, document)
	if err != nil {
		return err
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.bufferedStoredVectorMatchesLocked(documentID, vector, scalarRow) {
		return nil
	}
	return idx.insertParsedStoredDocumentLocked(documentID, vector, scalarRow, false)
}

func (idx *VectorIndex) bufferedStoredVectorMatchesLocked(documentID []byte, vector []float32, scalarRow map[string][]byte) bool {
	if live := idx.partitionLive; live != nil && !live.invalid {
		owner, exists := live.ownerV1(string(documentID))
		if vector == nil {
			if !exists || !owner.deleted {
				return false
			}
		} else {
			domain, err := idx.partitionLiveRouteLocked(vector)
			if err != nil || !exists || owner.deleted || owner.domain != domain {
				return false
			}
			delta := live.domains[domain]
			if delta == nil {
				return false
			}
			delta.mu.RLock()
			matches := delta.currentVectorMatchesLocked(documentID, vector)
			delta.mu.RUnlock()
			if !matches {
				return false
			}
		}
		if idx.partitionLiveOnly {
			return true
		}
	}
	plane := idx
	key := string(documentID)
	if idx.liveDelta != nil {
		if _, exists := idx.liveDelta.currentNode[key]; exists {
			plane = idx.liveDelta
		}
	}
	nodeID, exists := plane.currentNode[key]
	if !exists || nodeID < 0 || nodeID >= len(plane.nodes) {
		return vector == nil
	}
	node := &plane.nodes[nodeID]
	if vector == nil {
		return node.deleted
	}
	if node.deleted || !plane.vectorIndexNodeMatchesSourceVectorLocked(node, vector) {
		return false
	}
	for _, def := range idx.scalarDefinitions {
		column, exists := plane.scalarColumns[def.Name]
		var value []byte
		var present bool
		if exists {
			value, present = column.value(nodeID)
		}
		expected, expectedPresent := scalarRow[def.Name]
		if present != expectedPresent || !bytes.Equal(value, expected) {
			return false
		}
	}
	return true
}
