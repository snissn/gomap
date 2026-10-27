package collections

import (
	"context"

	"errors"
	"fmt"
	"math"

	"sync"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"

	"github.com/snissn/gomap/TreeDB/node"
)

const (
	vectorIndexFallbackMissingVectorIndexMetadata = "missing_vector_index_metadata"
	vectorIndexFallbackMissingGraphRoot           = "missing_graph_root"
	vectorIndexFallbackMissingGraphRootEntry      = "missing_graph_root_entry"
	vectorIndexFallbackInvalidGraphRootKey        = "invalid_graph_root_key"
	vectorIndexFallbackInvalidGraphRootEntry      = "invalid_graph_root_entry"
	vectorIndexFallbackStaleRuntimeIndex          = "stale_runtime_index_ignored"
	vectorIndexFallbackStaleDocumentRoot          = "stale_document_root"
	vectorIndexFallbackMetaMismatch               = "meta_mismatch"
	vectorIndexFallbackInvalidEncoding            = "invalid_encoding"
	vectorIndexFallbackMetaEncodingMismatch       = "meta_encoding_mismatch"
	vectorIndexFallbackMetaDimensionMismatch      = "meta_dimension_mismatch"
	vectorIndexFallbackInvalidDimensions          = "invalid_dimensions"
	vectorIndexFallbackInvalidEdgeNode            = "invalid_edge_node"
	vectorIndexFallbackInvalidTombstone           = "invalid_tombstone"
	vectorIndexFallbackInvalidDocMapNode          = "invalid_docmap_node"
	vectorIndexFallbackInvalidEntry               = "invalid_entry"
	vectorIndexFallbackMissingManifest            = "missing_manifest"
	vectorIndexFallbackInvalidManifest            = "invalid_manifest"
)

var errVectorIndexStaleRuntime = errors.New("collections: vector index runtime handle is stale")

var nativeVectorIndexBeforeInstallHookForTest struct {
	mu sync.Mutex
	fn func(string)
}

var nativeVectorIndexBeforeAutoPersistSaveHookForTest struct {
	mu sync.Mutex
	fn func(string)
}

func setNativeVectorIndexBeforeInstallHookForTest(fn func(string)) func() {
	nativeVectorIndexBeforeInstallHookForTest.mu.Lock()
	previous := nativeVectorIndexBeforeInstallHookForTest.fn
	nativeVectorIndexBeforeInstallHookForTest.fn = fn
	nativeVectorIndexBeforeInstallHookForTest.mu.Unlock()
	return func() {
		nativeVectorIndexBeforeInstallHookForTest.mu.Lock()
		nativeVectorIndexBeforeInstallHookForTest.fn = previous
		nativeVectorIndexBeforeInstallHookForTest.mu.Unlock()
	}
}

func runNativeVectorIndexBeforeInstallHookForTest(name string) {
	nativeVectorIndexBeforeInstallHookForTest.mu.Lock()
	fn := nativeVectorIndexBeforeInstallHookForTest.fn
	nativeVectorIndexBeforeInstallHookForTest.mu.Unlock()
	if fn != nil {
		fn(name)
	}
}

func setNativeVectorIndexBeforeAutoPersistSaveHookForTest(fn func(string)) func() {
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Lock()
	previous := nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn = fn
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Unlock()
	return func() {
		nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Lock()
		nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn = previous
		nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Unlock()
	}
}

func runNativeVectorIndexBeforeAutoPersistSaveHookForTest(name string) {
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Lock()
	fn := nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Unlock()
	if fn != nil {
		fn(name)
	}
}

// columnGraphPartitionLiveOptions describes one bounded domain graph and its persisted owner.
type columnGraphPartitionLiveOptions struct {
	Name                string
	Field               string
	Metric              VectorMetric
	Dimensions          int
	M                   int
	EfConstruction      int
	EfSearch            int
	RebuildDeletedRatio float64
	Encoding            VectorIndexEncoding
	schemaGeneration    uint64
	ownerDefinition     *VectorIndexDefinition
}

// columnGraphPartitionLiveCarrier owns bounded mutable domains for a declared
// ColumnGraph manifest. It has no ordinary collection search lifecycle.
type columnGraphPartitionLiveCarrier struct {
	*columnGraphHNSWBuilder

	collection *Collection

	nativeRootName string

	fieldPath []string

	efSearch            int
	rebuildDeletedRatio float64
	schemaGeneration    uint64

	nativePublicationMu sync.RWMutex

	searchView           atomic.Pointer[vectorIndexSearchView]
	searchViewSpare      atomic.Pointer[vectorIndexSearchView]
	searchViewCurrent    atomic.Bool
	searchViewDirty      map[int]struct{}
	trackSearchViewDirty bool
	searchViewForceFull  bool

	partitionPreparation      *VectorPartitionPrepareCompletionV1
	partitionLive             *vectorIndexPartitionLiveStateV1
	partitionLiveMutationUndo *vectorIndexPartitionLiveDomainUndoV2
	preserveAcknowledgedView  atomic.Bool
	mutationSeq               uint64
	sourceDocumentRootsValid  bool
	unnotifiedDocumentIDs     map[string]struct{}
	sourceDocumentGeneration  uint64
	// Accounted roots may still contain exact IDs awaiting graph maintenance.
	sourceDocumentAccountedGeneration      uint64
	sourceDocumentAccountedGenerationValid bool
	sourceDocumentState                    backenddb.StateToken
	sourceDocumentStateValid               bool

	searchViewAcknowledged            bool
	searchViewAcknowledgedMutationSeq uint64
	searchViewAcknowledgedGeneration  uint64

	persistedEpoch         uint64
	fullSnapshotBaseEpoch  uint64
	persistedBytesDisk     int64
	persistedSnapshotDirty bool
	nativePersistent       bool
	partitionLiveOnly      bool
	dirtyMeta              bool
	dirtyNodes             map[int]struct{}
	dirtyDocs              map[string]struct{}
	lastRebuildDuration    time.Duration

	// constructionTrace is an offline-only nullable sink installed by the
	// partition-pack diagnostic builder. Normal collection indexes never set it.

	// qualityPostfillCandidates belongs to the temporary builder, never to the
	// evidence sink: graph construction must be identical with tracing off.

}

func (c *Collection) currentVectorIndexDocumentGeneration() (uint64, error) {
	return c.currentVectorIndexDocumentGenerationWithWriteDomainLockState(false)
}

func (c *Collection) currentVectorIndexDocumentGenerationWithWriteDomainLocked() (uint64, error) {
	return c.currentVectorIndexDocumentGenerationWithWriteDomainLockState(true)
}

func (c *Collection) currentVectorIndexDocumentGenerationWithWriteDomainLockState(writeDomainLocked bool) (uint64, error) {
	generation, _, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(writeDomainLocked)
	return generation, err
}

func (c *Collection) currentVectorIndexDocumentStateWithWriteDomainLockState(writeDomainLocked bool) (uint64, backenddb.StateToken, error) {
	if c == nil {
		return 0, backenddb.StateToken{}, errCollectionNil
	}
	if c.db == nil {
		return 0, backenddb.StateToken{}, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshotWithWriteDomainLockState(snap, writeDomainLocked)
	if err != nil {
		return 0, backenddb.StateToken{}, err
	}
	if catalog == nil {
		return 0, backenddb.StateToken{}, errCollectionNotFound
	}
	generation, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return 0, backenddb.StateToken{}, err
	}
	state, ok := snap.StateToken()
	if !ok {
		return 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	return generation, state, nil
}

func vectorIndexDocumentGeneration(snap *backenddb.Snapshot, catalog *collectionCatalog) (uint64, error) {
	if snap == nil || catalog == nil {
		return 0, errCollectionNotFound
	}
	return vectorIndexDocumentGenerationForCollection(snap, catalog.meta.Name)
}

func vectorIndexDocumentGenerationForCollection(snap *backenddb.Snapshot, collection string) (uint64, error) {
	raw, ok, err := getSystemValue(snap, systemCollectionDocumentGenerationKey(collection))
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return decodeRootID(raw)
}

func (c *Collection) currentVectorIndexDocumentGenerationForAdmission() (uint64, error) {
	if c == nil {
		return 0, errCollectionNil
	}
	if c.db == nil {
		return 0, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	collection := c.collectionName()
	if collection == "" && c.writeDomain != nil {
		c.writeDomain.mu.RLock()
		collection = c.writeDomain.meta.Name
		c.writeDomain.mu.RUnlock()
	}
	return vectorIndexDocumentGenerationForCollection(snap, collection)
}

func (c *Collection) isRegisteredVectorIndex(index *columnGraphPartitionLiveCarrier) bool {
	if c == nil || index == nil {
		return false
	}
	return c.registeredColumnGraphPartitionLiveCarrier(index.name) == index
}

func (c *Collection) hasNativePersistentVectorIndex() bool {
	for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
		if index != nil && index.isNativePersistent() {
			return true
		}
	}
	return false
}

func (c *Collection) hasRegisteredVectorIndex(name string) bool {
	return c != nil && name != "" && c.registeredColumnGraphPartitionLiveCarrier(name) != nil
}

func (c *Collection) lockNativeVectorIndexLoad() func() {
	if coord := c.collectionSchemaCoordinator(); coord != nil {
		coord.nativeVectorIndexLoadMu.Lock()
		return coord.nativeVectorIndexLoadMu.Unlock
	}
	if c != nil && c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexLoadMu.Lock()
		return c.writeDomain.nativeVectorIndexLoadMu.Unlock
	}
	c.vectorIndexLoadMu.Lock()
	return c.vectorIndexLoadMu.Unlock
}

func (idx *columnGraphPartitionLiveCarrier) nativePublicationLock() *sync.RWMutex {
	if idx != nil && idx.isPartitionLiveCarrier() && idx.collection != nil {
		if coord := idx.collection.collectionSchemaCoordinator(); coord != nil {
			return &coord.partitionLivePublishMu
		}
	}
	if idx != nil && idx.collection != nil && idx.collection.writeDomain != nil {
		return &idx.collection.writeDomain.nativeVectorPublishMu
	}
	return &idx.nativePublicationMu
}

func (c *Collection) ensureDeclaredNativeVectorIndexesLoaded() (map[string]struct{}, error) {
	unlockAdmission := c.lockVectorIndexSynchronousPublicationAdmission()
	defer unlockAdmission()
	unlockMutation := c.lockVectorIndexMutation()
	defer unlockMutation()
	return c.ensureDeclaredNativeVectorIndexesLoadedWithMutationHeld()
}

func (c *Collection) declaredNativeVectorIndexesLoadedForCurrentCatalog() bool {
	if c == nil || c.db == nil {
		return false
	}
	commitSeq, systemRoot := dbCommitSeqAndSystemRoot(c.db)
	c.catalogMu.RLock()
	catalogCurrent := c.catalog != nil && c.catalogCommitSeq == commitSeq && c.catalogSystemRoot == systemRoot
	var defs []VectorIndexDefinition
	var persistentColumnGraphRoots map[string]struct{}
	if catalogCurrent {
		defs = append([]VectorIndexDefinition(nil), c.catalog.meta.VectorIndexes...)
		for _, def := range defs {
			if def.Strategy == VectorIndexStrategyColumnGraph && c.catalog.rootID(collectionVectorIndexRootName(c.catalog.meta.Name, def.Name)) != 0 {
				if persistentColumnGraphRoots == nil {
					persistentColumnGraphRoots = make(map[string]struct{})
				}
				persistentColumnGraphRoots[def.Name] = struct{}{}
			}
		}
	}
	c.catalogMu.RUnlock()
	if !catalogCurrent {
		return false
	}
	nativeDefs := make([]VectorIndexDefinition, 0, len(defs))
	for _, def := range defs {
		_, persistentColumnGraph := persistentColumnGraphRoots[def.Name]
		if persistentColumnGraph {
			nativeDefs = append(nativeDefs, def)
		}
	}
	if len(nativeDefs) == 0 {
		for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
			if index.isNativePersistent() {
				return false
			}
		}
		return true
	}
	generation, err := c.currentVectorIndexDocumentGenerationForAdmission()
	if err != nil {
		return false
	}
	declared := make(map[string]struct{}, len(nativeDefs))
	for _, def := range nativeDefs {
		declared[def.Name] = struct{}{}
		index := c.registeredColumnGraphPartitionLiveCarrier(def.Name)
		if index == nil || !index.isNativePersistent() || !index.hasSourceDocumentReconciliationBaseline(generation) || index.validateNativeSnapshotDefinition(def) != "" {
			return false
		}
	}
	for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
		if !index.isNativePersistent() {
			continue
		}
		if _, ok := declared[index.name]; !ok {
			return false
		}
	}
	return true
}

func vectorIndexDefinitionUsesPersistentRuntime(def VectorIndexDefinition, index *columnGraphPartitionLiveCarrier) bool {
	return def.Strategy == VectorIndexStrategyColumnGraph && index != nil && index.isPartitionLiveCarrier()
}

func (c *Collection) notifyVectorIndexesUpsert(documentIDs [][]byte) error {
	return c.reconcileVectorIndexes(documentIDs)
}

func (c *Collection) notifyVectorIndexesDelete(documentIDs [][]byte) error {
	return c.reconcileVectorIndexes(documentIDs)
}

func (c *Collection) reconcileVectorIndexes(documentIDs [][]byte) error {
	if len(documentIDs) == 0 {
		return nil
	}
	unlockMutation := c.lockVectorIndexMutation()
	defer unlockMutation()
	// Each installed index must account for this owner's acquisition generation
	// or have already completed maintenance under this same retained admission.
	// This permits genuine Update/Delete notification without letting a newly
	// created raw marker explain an older unknown generation.
	if c.writeDomain != nil {
		if coord := c.collectionSchemaCoordinator(); coord != nil {
			if baseline := coord.nativeVectorBaseline.Load(); baseline != nil {
				generation, err := c.currentVectorIndexDocumentGenerationForAdmission()
				if err != nil {
					c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
					return err
				}
				for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
					c.writeDomain.nativeVectorActiveMu.Lock()
					_, reconciled := c.writeDomain.nativeVectorReconciled[index]
					c.writeDomain.nativeVectorActiveMu.Unlock()
					index.mu.Lock()
					if index.sourceDocumentReconciliationBaselineLocked(*baseline) ||
						reconciled && index.sourceDocumentRootsValid && len(index.unnotifiedDocumentIDs) == 0 {
						index.sourceDocumentAccountedGeneration = generation
						index.sourceDocumentAccountedGenerationValid = true
					}
					index.mu.Unlock()
				}
			}
		}
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorActiveMu.Lock()
		rebuildCurrentDocuments := c.writeDomain.nativeVectorActive != 0 && !c.writeDomain.nativeVectorSearchActive.Load()
		c.writeDomain.nativeVectorActiveMu.Unlock()
		if rebuildCurrentDocuments {
			unlockPublication := c.lockNativeVectorIndexPublicationRead()
			indexes := c.registeredColumnGraphPartitionLiveCarriers()
			if len(indexes) != 0 {
				currentGeneration, err := c.currentVectorIndexDocumentGeneration()
				if err != nil {
					c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
					unlockPublication()
					return err
				}
				for _, index := range indexes {
					// Another invalid index must not erase this installed
					// runtime's genuine baseline or its exact known gaps.
					index.mu.RLock()
					retained := index.sourceDocumentReconciliationBaselineLocked(currentGeneration)
					index.mu.RUnlock()
					if !retained {
						index.invalidateSourceDocumentRoots()
					}
				}
			}
			unlockPublication()
		}
	}
	rebuilt, err := c.ensureDeclaredNativeVectorIndexesLoadedWithMutationHeld()
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverage()
		return err
	}
	unlockPublication := c.lockNativeVectorIndexPublicationRead()
	defer unlockPublication()
	indexes := c.registeredColumnGraphPartitionLiveCarriers()
	if vectorIndexListHasPartitionLiveCarrier(indexes) {
		currentGeneration, err := c.currentVectorIndexDocumentGeneration()
		if err != nil {
			c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
			return err
		}
		write := 0
		for _, index := range indexes {
			if index.isPartitionLiveCarrier() && index.coversSourceDocumentGeneration(currentGeneration) {
				continue
			}
			indexes[write] = index
			write++
		}
		indexes = indexes[:write]
	}
	return c.reconcileLoadedVectorIndexes(documentIDs, indexes, rebuilt, true)
}

func vectorIndexListHasPartitionLiveCarrier(indexes []*columnGraphPartitionLiveCarrier) bool {
	for _, index := range indexes {
		if index.isPartitionLiveCarrier() {
			return true
		}
	}
	return false
}

// reconcileVectorPartitionLiveReplay advances only the already-restored live
// carrier. Command replay must never trigger a collection scan or graph build.
func (c *Collection) reconcileVectorPartitionLiveReplay(documentIDs [][]byte) error {
	if len(documentIDs) == 0 {
		return nil
	}
	indexes := c.registeredVectorPartitionLiveCarriersV1()
	if len(indexes) == 0 {
		return nil
	}
	unlockMutation := c.lockVectorIndexMutation()
	defer unlockMutation()
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	coord.partitionLivePublishMu.Lock()
	defer coord.partitionLivePublishMu.Unlock()
	currentGeneration, err := c.currentVectorIndexDocumentGeneration()
	if err != nil {
		return err
	}
	covered := true
	for _, index := range indexes {
		if !index.coversSourceDocumentGeneration(currentGeneration) {
			covered = false
			break
		}
	}
	if covered {
		return nil
	}
	return c.reconcileLoadedVectorIndexes(documentIDs, indexes, nil, false)
}

func (c *Collection) reconcileLoadedVectorIndexes(documentIDs [][]byte, indexes []*columnGraphPartitionLiveCarrier, rebuilt map[string]struct{}, registerHandle bool) error {
	if len(indexes) == 0 {
		return nil
	}
	if registerHandle && c.manager != nil && vectorIndexListHasNativePersistent(indexes) {
		c.manager.registerCollectionHandle(c)
	}
	needsInsert := false
	for _, index := range indexes {
		if _, ok := rebuilt[index.name]; !ok {
			needsInsert = true
			break
		}
	}
	if !needsInsert {
		return c.recordReconciledVectorIndexCoverage(indexes)
	}
	cachedPrimaryRead := c.snapshotVectorIndexPrimaryCache(documentIDs)
	defer putUpdateBatchBufferedEntries(cachedPrimaryRead.primaryEntries, cachedPrimaryRead.primaryBuffer)
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		return err
	}
	defer func() { _ = materializer.Close() }()
	for _, index := range indexes {
		if _, ok := rebuilt[index.name]; ok {
			continue
		}
		for documentIndex, documentID := range documentIDs {
			if len(documentID) == 0 {
				continue
			}
			var document []byte
			if cachedPrimaryRead.enabled && cachedPrimaryRead.primaryEntries[documentIndex].found {
				entry := cachedPrimaryRead.primaryEntries[documentIndex]
				if entry.flags&node.FlagTombstone == 0 {
					document = entry.value
				}
			} else {
				document, err = c.Get(documentID)
				if err != nil {
					c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
					return err
				}
			}
			if err := index.reconcileBufferedStoredDocument(materializer, documentID, document); err != nil {
				c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
				return err
			}
		}

		index.clearUnnotifiedDocuments(documentIDs)
	}
	return c.recordReconciledVectorIndexCoverage(indexes)
}

func (c *Collection) snapshotVectorIndexPrimaryCache(documentIDs [][]byte) updateBatchBufferedRead {
	if c == nil || c.db == nil || len(documentIDs) == 0 {
		return updateBatchBufferedRead{}
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return updateBatchBufferedRead{}
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil || catalog == nil {
		return updateBatchBufferedRead{}
	}
	items := make([]updateBatchItem, len(documentIDs))
	for i, documentID := range documentIDs {
		items[i].DocumentID = documentID
	}
	return snapshotUpdateBatchPrimaryCache(c.writeDomain, catalog.meta, snapshotSystemRoot(snap), items)
}

func (c *Collection) invalidateRegisteredVectorIndexDocumentCoverage() {
	unlockPublication := c.lockNativeVectorIndexPublicationRead()
	defer unlockPublication()
	c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
}

func (c *Collection) invalidateRegisteredVectorIndexDocumentCoverageLocked() {
	for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
		index.invalidateSourceDocumentRoots()
	}
}

func (c *Collection) invalidateOtherVectorIndexDocumentCoverage() {
	if c == nil || c.writeDomain == nil || c.writeDomain.schemaCoordinator == nil {
		return
	}
	coord := c.writeDomain.schemaCoordinator
	shared := make(map[string]struct{})
	for _, carrier := range coord.partitionLiveCarrierList() {
		shared[carrier.name] = struct{}{}
	}
	for _, domain := range coord.snapshotDomains() {
		if domain == c.writeDomain {
			continue
		}
		domain.nativeVectorMutationMu.Lock()
		domain.nativeVectorSearchActive.Store(false)
		other := &Collection{db: c.db, writeDomain: domain}
		unlockPublication := other.lockNativeVectorIndexPublicationRead()
		domain.nativeVectorIndexesMu.RLock()
		for name, index := range domain.nativeVectorIndexes {
			if _, ok := shared[name]; !ok {
				index.invalidateSourceDocumentRoots()
			}
		}
		domain.nativeVectorIndexesMu.RUnlock()
		unlockPublication()
		domain.nativeVectorMutationMu.Unlock()
	}
}

func (c *Collection) invalidateVectorIndexCoverageOnAcceptedMutation(err error) error {
	if backenddb.CommitPublicationAccepted(err) {
		c.invalidateRegisteredVectorIndexDocumentCoverage()
	}
	return err
}

func (c *Collection) lockVectorIndexMutation() func() {
	if c != nil && c.nativeJSON != nil {
		coord := c.collectionSchemaCoordinator()
		coord.vectorDDLMutationMu.Lock()
		return coord.vectorDDLMutationMu.Unlock
	}
	if c == nil {
		return func() {}
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorMutationMu.Lock()
		return c.writeDomain.nativeVectorMutationMu.Unlock
	}
	c.vectorIndexMutationMu.Lock()
	return c.vectorIndexMutationMu.Unlock
}

func (c *Collection) nativeVectorAdmissionMutex() *sync.RWMutex {
	if c == nil || c.writeDomain == nil {
		return nil
	}
	if coord := c.writeDomain.schemaCoordinator; coord != nil {
		return &coord.nativeVectorAdmissionMu
	}
	return &c.writeDomain.nativeVectorAdmissionMu
}

func collectionMetaHasNativeVectorIndexes(meta CollectionMeta) bool { return false }

func (c *Collection) lockVectorIndexCoverageMutation() func() {
	return c.lockVectorIndexCoverageMutationWithColdCarrier(false)
}

func (c *Collection) lockVectorIndexCoverageMutationWithColdCarrier(forceColdCarrier bool) func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	domain := c.writeDomain
	unlockAdHocAdmission := c.lockAdHocVectorAdmissionRead()
	admissionMu := c.nativeVectorAdmissionMutex()
	coord := domain.schemaCoordinator
	var hasMaintainedVectorIndexes bool
	var exclusiveAdmission bool
	for {
		admissionMu.RLock()
		hasMaintainedVectorIndexes = coord != nil && (coord.hasNativeVectorIndexes.Load() || coord.hasPartitionLiveCarrier())
		domain.mu.RLock()
		hasMaintainedVectorIndexes = hasMaintainedVectorIndexes || collectionMetaHasNativeVectorIndexes(domain.meta)
		if forceColdCarrier {
			// A cold ColumnGraph can have a durable live carrier before serving
			// registers it. Loading requires exclusive admission, never an upgrade.
			for _, def := range domain.meta.VectorIndexes {
				if def.Strategy == VectorIndexStrategyColumnGraph {
					hasMaintainedVectorIndexes = true
					break
				}
			}
		}
		domain.mu.RUnlock()
		domain.nativeVectorIndexesMu.RLock()
		hasMaintainedVectorIndexes = hasMaintainedVectorIndexes || len(domain.nativeVectorIndexes) != 0
		domain.nativeVectorIndexesMu.RUnlock()
		if !hasMaintainedVectorIndexes {
			break
		}
		admissionMu.RUnlock()
		domain.waitIndexedAsyncFlush()
		admissionMu.Lock()
		if !domain.indexedAsyncFlushRunning() {
			exclusiveAdmission = true
			break
		}
		admissionMu.Unlock()
	}
	var baselineGeneration uint64
	var baselineErr error
	if hasMaintainedVectorIndexes {
		baselineGeneration, baselineErr = c.currentVectorIndexDocumentGenerationForAdmission()
	}
	if exclusiveAdmission && coord != nil {
		coord.nativeVectorBaseline.Store(nil)
		if baselineErr == nil {
			coord.nativeVectorBaseline.Store(&baselineGeneration)
		}
	}
	domain.nativeVectorCoverageMu.RLock()
	baselineCurrent := !hasMaintainedVectorIndexes || baselineErr == nil && c.nativeVectorBaselineCovers(baselineGeneration)
	domain.nativeVectorActiveMu.Lock()
	if domain.nativeVectorActive == 0 {
		domain.nativeVectorReconciled = nil
	}
	domain.nativeVectorActive++
	if exclusiveAdmission {
		domain.nativeVectorAsyncAdmissionHeld = true
	}
	domain.nativeVectorSearchActive.Store(hasMaintainedVectorIndexes && baselineCurrent)
	domain.nativeVectorActiveMu.Unlock()
	return func() {
		defer unlockAdHocAdmission()
		domain.mu.RLock()
		domain.nativeVectorActiveMu.Lock()
		domain.nativeVectorActive--
		if exclusiveAdmission {
			domain.nativeVectorAsyncAdmissionHeld = false
		}
		if domain.nativeVectorActive == 0 {
			if baselineCurrent || len(domain.nativeVectorReconciled) != 0 {
				indexes := c.registeredColumnGraphPartitionLiveCarriers()
				if len(indexes) != 0 {
					// An unchanged acquisition baseline carries coverage forward. A
					// new document generation requires completed graph reconciliation.
					if generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(true); err == nil {
						for _, index := range indexes {
							_, reconciled := domain.nativeVectorReconciled[index]
							if index.hasValidSourceDocumentRoots() && index.hasSourceDocumentReconciliationBaseline(generation) && (reconciled || baselineCurrent && generation == baselineGeneration && index.coversSourceDocumentGeneration(baselineGeneration)) {
								index.recordSourceDocumentState(generation, state)
							}
						}
					}
				}
			}
			domain.nativeVectorReconciled = nil
			domain.nativeVectorSearchActive.Store(false)
		}
		domain.nativeVectorCoverageMu.RUnlock()
		domain.nativeVectorActiveMu.Unlock()
		domain.mu.RUnlock()
		if exclusiveAdmission {
			c.startDeferredIndexedAsyncFlush()
			if coord != nil {
				coord.nativeVectorBaseline.Store(nil)
			}
			admissionMu.Unlock()
		} else {
			admissionMu.RUnlock()
		}
	}
}

func (c *Collection) lockVectorIndexPublicationAdmission() func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	domain := c.writeDomain
	admissionMu := c.nativeVectorAdmissionMutex()
	admissionMu.RLock()
	coord := domain.schemaCoordinator
	hasMaintainedVectorIndexes := coord != nil && (coord.hasNativeVectorIndexes.Load() || coord.hasPartitionLiveCarrier())
	domain.mu.RLock()
	hasMaintainedVectorIndexes = hasMaintainedVectorIndexes || collectionMetaHasNativeVectorIndexes(domain.meta)
	domain.mu.RUnlock()
	domain.nativeVectorIndexesMu.RLock()
	hasMaintainedVectorIndexes = hasMaintainedVectorIndexes || len(domain.nativeVectorIndexes) != 0
	domain.nativeVectorIndexesMu.RUnlock()
	if !hasMaintainedVectorIndexes {
		return admissionMu.RUnlock
	}
	admissionMu.RUnlock()
	admissionMu.Lock()
	return admissionMu.Unlock
}

func (c *Collection) lockVectorIndexSynchronousPublicationAdmission() func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	domains := []*collectionWriteDomain{c.writeDomain}
	if coord := c.collectionSchemaCoordinator(); coord != nil {
		domains = coord.snapshotDomains()
	}
	for {
		for _, domain := range domains {
			domain.waitIndexedAsyncFlush()
		}
		unlock := c.lockVectorIndexPublicationAdmission()
		running := false
		for _, domain := range domains {
			running = running || domain.indexedAsyncFlushRunning()
		}
		if !running {
			return unlock
		}
		unlock()
	}
}

func (c *Collection) nativeVectorBaselineCovers(generation uint64) bool {
	carriers := c.registeredVectorPartitionLiveCarriersV1()
	covered := false
	var shared map[string]struct{}
	if len(carriers) != 0 {
		shared = make(map[string]struct{}, len(carriers))
	}
	for _, index := range carriers {
		covered = true
		shared[index.name] = struct{}{}
		if !index.coversSourceDocumentGeneration(generation) {
			return false
		}
	}
	domain := c.writeDomain
	domain.nativeVectorIndexesMu.RLock()
	defer domain.nativeVectorIndexesMu.RUnlock()
	for name, index := range domain.nativeVectorIndexes {
		if _, ok := shared[name]; ok {
			continue
		}
		covered = true
		if !index.coversSourceDocumentGeneration(generation) {
			return false
		}
	}
	return covered
}

func (c *Collection) lockVectorIndexCoveragePersistence() func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	c.writeDomain.nativeVectorCoverageMu.Lock()
	return c.writeDomain.nativeVectorCoverageMu.Unlock
}

func (c *Collection) recordReconciledVectorIndexCoverage(indexes []*columnGraphPartitionLiveCarrier) error {
	return c.recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes, false)
}

func (c *Collection) recordReconciledVectorIndexCoverageWithWriteDomainLocked(indexes []*columnGraphPartitionLiveCarrier) error {
	return c.recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes, true)
}

func (c *Collection) recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes []*columnGraphPartitionLiveCarrier, writeDomainLocked bool) error {
	if c == nil {
		return errCollectionNil
	}
	if c.writeDomain == nil {
		generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(writeDomainLocked)
		if err != nil {
			c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
			return err
		}
		for _, index := range indexes {
			if c.isRegisteredVectorIndex(index) {
				index.recordSourceDocumentState(generation, state)
			}
		}
		return nil
	}

	domain := c.writeDomain
	if !writeDomainLocked {
		domain.mu.RLock()
		defer domain.mu.RUnlock()
	}
	generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(true)
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		return err
	}
	domain.nativeVectorActiveMu.Lock()
	defer domain.nativeVectorActiveMu.Unlock()
	if domain.nativeVectorActive != 0 {
		for _, index := range indexes {
			if c.isRegisteredVectorIndex(index) && index.hasValidSourceDocumentRoots() && index.hasSourceDocumentReconciliationBaseline(generation) {
				if domain.nativeVectorReconciled == nil {
					domain.nativeVectorReconciled = make(map[*columnGraphPartitionLiveCarrier]struct{}, len(indexes))
				}
				domain.nativeVectorReconciled[index] = struct{}{}
			}
		}
		return nil
	}
	domain.nativeVectorSearchActive.Store(c.nativeVectorBaselineCovers(generation))
	defer domain.nativeVectorSearchActive.Store(false)
	for _, index := range indexes {
		if c.isRegisteredVectorIndex(index) && index.hasValidSourceDocumentRoots() && index.hasSourceDocumentReconciliationBaseline(generation) {
			index.recordSourceDocumentState(generation, state)
		}
	}
	return nil
}

func (c *Collection) recordVectorIndexCoverageAfterBufferedDocumentPublish() error {
	return c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLockState(false)
}

func (c *Collection) recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLocked() error {
	return c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLockState(true)
}

func (c *Collection) recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLockState(writeDomainLocked bool) error {
	if c == nil || c.writeDomain == nil {
		return nil
	}
	indexes := c.registeredColumnGraphPartitionLiveCarriers()
	if len(indexes) == 0 {
		return nil
	}
	domain := c.writeDomain
	domain.nativeVectorActiveMu.Lock()
	active := domain.nativeVectorActive != 0
	domain.nativeVectorActiveMu.Unlock()
	if active {
		return nil
	}

	if !domain.nativeVectorCoverageMu.TryLock() {
		// The holder is either a public mutation, whose final unlock certifies
		// coverage, or persistence, whose locked flush does the same.
		return nil
	}
	defer domain.nativeVectorCoverageMu.Unlock()
	domain.nativeVectorActiveMu.Lock()
	active = domain.nativeVectorActive != 0
	domain.nativeVectorActiveMu.Unlock()
	if active {
		return nil
	}
	return c.recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes, writeDomainLocked)
}

func (c *Collection) lockNativeVectorIndexPublicationRead() func() {
	var liveMu *sync.RWMutex
	if coord := c.collectionSchemaCoordinator(); coord != nil && coord.hasPartitionLiveCarrier() {
		liveMu = &coord.partitionLivePublishMu
		// Live reconciliation and invalidation publish a new revision, so they
		// exclude coordinator and shard pins held across a whole request.
		liveMu.Lock()
	}
	if c != nil && c.writeDomain != nil {
		c.writeDomain.nativeVectorPublishMu.RLock()
		return func() {
			c.writeDomain.nativeVectorPublishMu.RUnlock()
			if liveMu != nil {
				liveMu.Unlock()
			}
		}
	}
	if liveMu != nil {
		return liveMu.Unlock
	}
	return func() {}
}

func vectorIndexListHasNativePersistent(indexes []*columnGraphPartitionLiveCarrier) bool {
	for _, index := range indexes {
		if index != nil && index.isNativePersistent() {
			return true
		}
	}
	return false
}

func (c *Collection) notifyVectorIndexesUpdateBatch(items []UpdateBatchItem, results []UpdateBatchResult) error {
	if len(items) == 0 || len(results) == 0 {
		return nil
	}
	var updated [][]byte
	for i := range items {
		if i >= len(results) {
			break
		}
		if results[i].Modified {
			updated = append(updated, items[i].DocumentID)
		}
	}
	return c.notifyVectorIndexesUpsert(updated)
}

func (c *Collection) notifyVectorIndexesBSONSetUpdateBatch(items []BSONSetUpdateBatchItem, results []UpdateBatchResult) error {
	if len(items) == 0 || len(results) == 0 {
		return nil
	}
	var updated [][]byte
	for i := range items {
		if i >= len(results) {
			break
		}
		if results[i].Modified {
			updated = append(updated, items[i].DocumentID)
		}
	}
	return c.notifyVectorIndexesUpsert(updated)
}

func (c *Collection) persistNativeVectorIndexIfDeclared(index *columnGraphPartitionLiveCarrier) error {
	if c == nil || index == nil {
		return nil
	}
	if index.isNativePersistent() {
		runNativeVectorIndexBeforeAutoPersistSaveHookForTest(index.name)
		_, err := index.saveColumnGraphPartitionLiveDelta()
		if errors.Is(err, errVectorIndexNotDeclared) {
			return nil
		}
		return err
	}
	if !index.needsNativeAutoPersist() {
		return nil
	}
	if !collectionMetaDeclaresNativeVectorIndex(c.meta, index.name) {
		declared, err := c.refreshNativeVectorIndexDeclaration(index.name)
		if err != nil || !declared {
			return err
		}
	}
	_, err := index.saveColumnGraphPartitionLiveSnapshot()
	if errors.Is(err, errVectorIndexNotDeclared) {
		return nil
	}
	return err
}

func (c *Collection) refreshNativeVectorIndexDeclaration(name string) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil || catalog == nil {
		return false, err
	}
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	return collectionMetaDeclaresNativeVectorIndex(catalog.meta, name), nil
}

func (c *Collection) persistDirtyNativeVectorIndexes() error {
	indexes := c.registeredColumnGraphPartitionLiveCarriers()
	for _, index := range indexes {
		if err := c.persistNativeVectorIndexIfDeclared(index); err != nil {
			return err
		}
	}
	if c.manager != nil && !c.hasDirtyNativeVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionTypedColumnOneShotCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
	return nil
}

func (c *Collection) hasDirtyNativeVectorIndex() bool {
	for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
		if index == nil || (!index.isNativePersistent() && !collectionMetaDeclaresNativeVectorIndex(c.meta, index.name)) {
			continue
		}
		if index.needsNativeAutoPersist() {
			return true
		}
	}
	return false
}

func collectionMetaDeclaresNativeVectorIndex(meta CollectionMeta, name string) bool {
	if name == "" {
		return false
	}
	def, ok := findVectorIndex(meta.VectorIndexes, name)
	return ok && def.Strategy == VectorIndexStrategyColumnGraph
}

func (idx *columnGraphPartitionLiveCarrier) validateVectorBatch(documentIDs [][]byte, vectors [][]float32) error {
	return idx.validateVectorBatchWithContext(context.Background(), documentIDs, vectors)
}

func (idx *columnGraphPartitionLiveCarrier) markGraphChangedLocked() {
	idx.mutationSeq++
	idx.searchViewCurrent.Store(false)
	if idx.persistedEpoch != 0 {
		idx.persistedSnapshotDirty = true
	}
}

func (idx *columnGraphPartitionLiveCarrier) requireFullNativeSnapshotLocked() {
	if !idx.nativePersistent {
		return
	}
	if idx.persistedEpoch != 0 {
		idx.fullSnapshotBaseEpoch = idx.persistedEpoch
	}
	idx.persistedEpoch = 0
	idx.persistedBytesDisk = 0
	idx.persistedSnapshotDirty = true
	idx.dirtyMeta = false
	clear(idx.dirtyNodes)
	clear(idx.dirtyDocs)
}

func (idx *columnGraphPartitionLiveCarrier) setNativePersistent(enabled bool) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.nativePersistent = enabled
	idx.parallelReciprocalLinks = enabled
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) recordPersistentDefinition(def VectorIndexDefinition) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.nativePersistent = true
	idx.partitionLiveOnly = def.Strategy == VectorIndexStrategyColumnGraph
	idx.parallelReciprocalLinks = !idx.partitionLiveOnly
	idx.schemaGeneration = def.SchemaGeneration
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) isPartitionLiveCarrier() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.partitionLiveOnly
}

func (idx *columnGraphPartitionLiveCarrier) setPartitionLiveCarrier(enabled bool) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.partitionLiveOnly = enabled
	if enabled {
		idx.nativePersistent = true
		idx.parallelReciprocalLinks = false
	}
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) isNativePersistent() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.nativePersistent
}

func (idx *columnGraphPartitionLiveCarrier) nativeSnapshotBaseEpochForFullSave() uint64 {
	if idx == nil {
		return 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.persistedEpoch != 0 {
		return idx.persistedEpoch
	}
	return idx.fullSnapshotBaseEpoch
}

func (idx *columnGraphPartitionLiveCarrier) recordFullSnapshotBaseEpoch(epoch uint64) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.fullSnapshotBaseEpoch = epoch
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) recordSourceDocumentGeneration(generation uint64) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	generationChanged := idx.sourceDocumentGeneration != generation
	changed := !idx.sourceDocumentRootsValid || generationChanged
	idx.sourceDocumentRootsValid = true
	idx.sourceDocumentGeneration = generation
	idx.sourceDocumentAccountedGeneration = generation
	idx.sourceDocumentAccountedGenerationValid = true
	idx.sourceDocumentStateValid = false
	idx.searchViewAcknowledged = false
	if changed {
		idx.searchViewCurrent.Store(false)
		if idx.nativePersistent && generationChanged {
			idx.dirtyMeta = true
		}
	}
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) recordSourceDocumentState(generation uint64, state backenddb.StateToken) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.recordSourceDocumentStateLocked(generation, state)
	idx.mu.Unlock()
}

func (idx *columnGraphPartitionLiveCarrier) recordSourceDocumentStateLocked(generation uint64, state backenddb.StateToken) {
	generationChanged := idx.sourceDocumentGeneration != generation
	changed := !idx.sourceDocumentRootsValid || generationChanged
	idx.sourceDocumentRootsValid = true
	idx.sourceDocumentGeneration = generation
	idx.sourceDocumentAccountedGeneration = generation
	idx.sourceDocumentAccountedGenerationValid = true
	idx.sourceDocumentState = state
	idx.sourceDocumentStateValid = true
	if idx.partitionLive != nil && !idx.partitionLive.invalid {
		if idx.partitionLive.coverage != generation {
			idx.partitionLive.coverage = generation
			idx.mutationSeq++
			idx.markVectorMetaDirtyLocked()
		}
	}
	idx.acknowledgeSearchViewStateLocked()
	if changed {
		idx.searchViewCurrent.Store(false)
		if idx.nativePersistent && generationChanged {
			idx.dirtyMeta = true
		}
	}
}

func (idx *columnGraphPartitionLiveCarrier) invalidateSourceDocumentRoots() {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.sourceDocumentRootsValid = false
	idx.sourceDocumentStateValid = false
	idx.searchViewAcknowledged = false
	idx.searchViewCurrent.Store(false)
	idx.mu.Unlock()
}

// A retained baseline may need exact-ID maintenance. It is safe for the loader
// to reuse, but does not grant search, persistence, or coverage acknowledgement.
func (idx *columnGraphPartitionLiveCarrier) sourceDocumentReconciliationBaselineLocked(generation uint64) bool {
	accounted := idx.sourceDocumentGeneration
	if idx.sourceDocumentAccountedGenerationValid {
		accounted = idx.sourceDocumentAccountedGeneration
	}
	return idx.sourceDocumentRootsValid && accounted == generation
}

func (idx *columnGraphPartitionLiveCarrier) hasSourceDocumentReconciliationBaseline(generation uint64) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	valid := idx.sourceDocumentReconciliationBaselineLocked(generation)
	idx.mu.RUnlock()
	return valid
}

func (idx *columnGraphPartitionLiveCarrier) hasValidSourceDocumentRoots() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	valid := idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0
	idx.mu.RUnlock()
	return valid
}

func (idx *columnGraphPartitionLiveCarrier) coversSourceDocumentGeneration(generation uint64) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	covers := idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0 && idx.sourceDocumentGeneration == generation
	idx.mu.RUnlock()
	return covers
}

func (idx *columnGraphPartitionLiveCarrier) coversSourceDocumentState(state backenddb.StateToken) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	covers := idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0 && idx.sourceDocumentStateValid && idx.sourceDocumentState == state
	idx.mu.RUnlock()
	return covers
}

func (idx *columnGraphPartitionLiveCarrier) publishedSearchViewCoversSourceDocumentState(state backenddb.StateToken) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	view := idx.searchView.Load()
	covers := idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0 && idx.sourceDocumentStateValid && idx.sourceDocumentState == state &&
		view != nil && view.sourceDocumentRootsValid && view.sourceDocumentGeneration == idx.sourceDocumentGeneration &&
		view.mutationSeq == idx.mutationSeq
	idx.mu.RUnlock()
	return covers
}

func (idx *columnGraphPartitionLiveCarrier) publishedSearchViewCoversSourceDocumentGeneration(generation uint64) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	view := idx.searchView.Load()
	covers := idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0 && idx.sourceDocumentGeneration == generation &&
		view != nil && view.sourceDocumentRootsValid && view.sourceDocumentGeneration == generation &&
		view.mutationSeq == idx.mutationSeq
	idx.mu.RUnlock()
	return covers
}

func (idx *columnGraphPartitionLiveCarrier) sourceDocumentCoverage() (uint64, bool) {
	if idx == nil {
		return 0, false
	}
	idx.mu.RLock()
	generation, valid := idx.sourceDocumentGeneration, idx.sourceDocumentRootsValid && len(idx.unnotifiedDocumentIDs) == 0
	idx.mu.RUnlock()
	return generation, valid
}

func (idx *columnGraphPartitionLiveCarrier) markVectorMetaDirtyLocked() {
	if !idx.nativePersistent {
		return
	}
	idx.dirtyMeta = true
}

func (idx *columnGraphPartitionLiveCarrier) markVectorNodeDirtyLocked(nodeID int) {
	if nodeID < 0 {
		return
	}
	if view := idx.searchView.Load(); idx.trackSearchViewDirty || view != nil && nodeID < len(view.nodes) {
		if idx.searchViewDirty == nil {
			idx.searchViewDirty = make(map[int]struct{})
		}
		idx.searchViewDirty[nodeID] = struct{}{}
	}
	if !idx.nativePersistent {
		return
	}
	if idx.dirtyNodes == nil {
		idx.dirtyNodes = make(map[int]struct{})
	}
	idx.dirtyNodes[nodeID] = struct{}{}
}

func (idx *columnGraphPartitionLiveCarrier) markVectorDocDirtyLocked(documentID []byte) {
	if !idx.nativePersistent || len(documentID) == 0 {
		return
	}
	if idx.dirtyDocs == nil {
		idx.dirtyDocs = make(map[string]struct{})
	}
	idx.dirtyDocs[string(documentID)] = struct{}{}
}

func (idx *columnGraphPartitionLiveCarrier) searchGraphOnlyCandidatesLocked(query []float32, topK, efSearch int, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, error) {
	if idx.nativePersistent && (!idx.sourceDocumentRootsValid || len(idx.unnotifiedDocumentIDs) != 0) {
		return nil, fmt.Errorf("%w: ColumnGraph partition carrier %q does not cover current documents", ErrVectorIndexSearchUnavailable, idx.name)
	}
	return idx.searchGraphOnlyCandidatesWithLiveDocsLocked(query, topK, efSearch, len(idx.currentNode), scratch)
}

func (idx *columnGraphPartitionLiveCarrier) searchGraphOnlyCandidatesWithLiveDocsLocked(query []float32, topK, efSearch, liveDocs int, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, error) {
	if topK < 0 {
		return nil, errors.New("collections: vector search TopK must be positive")
	}
	queryNorm, preparedQuery, preparedCosine, err := prepareVectorIndexGraphOnlyQuery(query, idx.metric, idx.dimensions)
	if err != nil {
		return nil, err
	}
	var prepared *preparedFloat32CosineQuery
	if preparedCosine {
		prepared = &preparedQuery
	}
	return idx.searchGraphOnlyCandidatesWithPreparedQueryLocked(query, queryNorm, prepared, topK, efSearch, liveDocs, scratch)
}

func (idx *columnGraphPartitionLiveCarrier) searchGraphOnlyCandidatesWithPreparedQueryLocked(query []float32, queryNorm float64, prepared *preparedFloat32CosineQuery, topK, efSearch, liveDocs int, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, error) {
	if err := scratch.finalContextErr(); err != nil {
		return nil, err
	}
	if topK == 0 {
		return nil, nil
	}
	if liveDocs == 0 {
		return nil, nil
	}
	limit := efSearch
	if limit <= 0 {
		limit = idx.efSearch
	}
	if limit < topK {
		limit = topK
	}
	// Historical nodes remain traversal waypoints. Preserve the requested Ef
	// budget through tombstones while bounding it by the allocated graph.
	if limit > len(idx.nodes) {
		limit = len(idx.nodes)
	}
	var candidates []vectorIndexCandidate
	if scratch != nil && scratch.resumeEnabled && liveDocs == len(idx.nodes) {
		candidates = idx.searchCandidatesResumableLocked(query, queryNorm, prepared, limit, scratch)
	} else {
		candidates = idx.searchCurrentCandidatesWithLiveDocsLocked(query, queryNorm, prepared, limit, liveDocs, scratch)
	}
	if err := scratch.finalScoreBudgetErr(); err != nil {
		return nil, err
	}
	if err := scratch.finalContextErr(); err != nil {
		return nil, err
	}
	if target := minInt(topK, liveDocs); len(candidates) < target {
		return nil, fmt.Errorf("%w: native graph search returned %d of %d live candidates within bounded traversal; rebuild the vector index", ErrVectorIndexSearchUnavailable, len(candidates), target)
	}
	return candidates, nil
}

func (idx *columnGraphPartitionLiveCarrier) searchCandidatesResumableLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if scratch.resumeRequested && scratch.resumeVisitedMark != 0 {
		scratch.resumed = true
		scratch.explored = 0
		// Rebuild the bounded result heap from every score already paid for.
		// Deferred nodes were rejected only by the smaller prior limit and have
		// not been expanded, so adding them to the frontier exactly continues
		// the same immutable layer-0 search at the larger limit.
		scratch.best = scratch.best[:0]
		for _, candidate := range scratch.resumeCandidates {
			scratch.best.pushBounded(candidate, limit)
		}
		for _, candidate := range scratch.resumeDeferred {
			scratch.queue.push(candidate)
		}
		scratch.resumeDeferred = scratch.resumeDeferred[:0]
		return idx.continueResumableCandidatesLocked(query, queryNormSquared, prepared, limit, scratch)
	}
	if idx.entry < 0 || len(idx.nodes) == 0 || limit <= 0 {
		return nil
	}
	entryPoint := idx.entry
	upperExplored := 0
	for layer := idx.maxLevel; layer > 0; layer-- {
		entryPoint = idx.greedyNearestAtLayerSearchBoundedLocked(query, queryNormSquared, prepared, entryPoint, layer, math.MaxInt, &upperExplored, scratch)
		if scratch.scoreBudgetExceeded {
			return nil
		}
	}
	entryDistance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, entryPoint, scratch)
	if !ok {
		return nil
	}
	if math.IsInf(float64(entryDistance), 1) {
		return nil
	}
	visited, mark := scratch.nextVisitedEpoch(len(idx.nodes))
	visited[entryPoint] = mark
	scratch.resumeVisitedMark = mark
	scratch.explored = 1
	entry := vectorIndexCandidate{nodeID: entryPoint, distance: entryDistance}
	scratch.queue = scratch.queue[:0]
	scratch.queue.push(entry)
	scratch.best = scratch.best[:0]
	scratch.best.pushBounded(entry, limit)
	scratch.resumeCandidates = append(scratch.resumeCandidates[:0], entry)
	scratch.resumeDeferred = scratch.resumeDeferred[:0]
	return idx.continueResumableCandidatesLocked(query, queryNormSquared, prepared, limit, scratch)
}

func (idx *columnGraphPartitionLiveCarrier) continueResumableCandidatesLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	visited := scratch.visitedEpochs
	mark := scratch.resumeVisitedMark

search:
	for len(scratch.queue) > 0 {
		current := scratch.queue[0]
		if len(scratch.best) >= limit && vectorIndexCandidateWorse(current, scratch.best[0]) {
			break
		}
		current = scratch.queue.pop()
		if current.nodeID < 0 || current.nodeID >= len(idx.nodes) {
			continue
		}
		for _, neighbor := range idx.layerNeighborsLocked(current.nodeID, 0) {
			neighborID := int(neighbor.nodeID)
			if neighborID < 0 || neighborID >= len(idx.nodes) || visited[neighborID] == mark {
				continue
			}
			visited[neighborID] = mark
			distance, ok := idx.scoreSearchNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID, scratch)
			if !ok {
				break search
			}
			scratch.explored++
			if scratch.explored&63 == 0 && scratch.context != nil {
				scratch.contextErr = scratch.context.Err()
				if scratch.contextErr != nil {
					break search
				}
			}
			if math.IsInf(float64(distance), 1) {
				continue
			}
			candidate := vectorIndexCandidate{nodeID: neighborID, distance: distance}
			scratch.resumeCandidates = append(scratch.resumeCandidates, candidate)
			if len(scratch.best) < limit || vectorIndexCandidateLess(candidate, scratch.best[0]) {
				scratch.queue.push(candidate)
				scratch.best.pushBounded(candidate, limit)
			} else {
				scratch.resumeDeferred = append(scratch.resumeDeferred, candidate)
			}
		}
	}
	scratch.out = append(scratch.out[:0], scratch.best...)
	sortVectorIndexCandidates(scratch.out)
	return scratch.out
}

func (idx *columnGraphPartitionLiveCarrier) greedyNearestAtLayerBoundedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint, layer, scoreLimit int, scored *int) int {
	return idx.greedyNearestAtLayerSearchBoundedLocked(query, queryNormSquared, prepared, entryPoint, layer, scoreLimit, scored, nil)
}

func (idx *columnGraphPartitionLiveCarrier) searchLayerLocked(query []float32, queryNormSquared float64, entryPoint int, limit int, layer int) []vectorIndexCandidate {
	return idx.searchLayerWithScratchLocked(query, queryNormSquared, nil, entryPoint, limit, layer, nil)
}

func (idx *columnGraphPartitionLiveCarrier) searchLayerCurrentWithScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchLayerWithScratchModeLocked(query, queryNormSquared, prepared, entryPoint, limit, explorationLimit, layer, scratch, true)
}

// selectDiverseCandidatesWithOriginsLocked preserves the existing candidate
// order while retaining the causal classification of each selected edge.

func (idx *columnGraphPartitionLiveCarrier) distanceToNodeLocked(query []float32, nodeID int) float32 {
	return idx.distanceToNodeWithQueryNormLocked(query, -1, nodeID)
}

func (idx *columnGraphPartitionLiveCarrier) distanceToNodeWithQueryNormLocked(query []float32, queryNormSquared float64, nodeID int) float32 {
	return idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, nil, nodeID)
}

func (idx *columnGraphPartitionLiveCarrier) persistedCarrierMetadata() (epoch uint64, bytesDisk int64) {
	if idx == nil {
		return 0, 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.persistedEpoch, idx.persistedBytesDisk
}

func (idx *columnGraphPartitionLiveCarrier) nativeMutationSequence() uint64 {
	if idx == nil {
		return 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.mutationSeq
}

func (idx *columnGraphPartitionLiveCarrier) nativePersistedBytesAndMutationSequence() (int64, uint64) {
	if idx == nil {
		return 0, 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.persistedBytesDisk, idx.mutationSeq
}

func newColumnGraphPartitionLiveDomain(c *Collection, opts columnGraphPartitionLiveOptions) (*columnGraphPartitionLiveCarrier, error) {
	// Collection ownership must be captured from an explicit modern definition.
	// Nil-collection instances are bounded domain/codec builders, never registered
	// collection search runtimes.
	if c != nil {
		def := opts.ownerDefinition
		if def == nil || def.Strategy != VectorIndexStrategyColumnGraph ||
			def.Name != opts.Name || def.Field != opts.Field || def.Metric != opts.Metric ||
			def.Encoding != opts.Encoding || def.Dimensions != opts.Dimensions ||
			def.M != opts.M || def.EfConstruction != opts.EfConstruction ||
			def.EfSearch != opts.EfSearch || def.SchemaGeneration != opts.schemaGeneration {
			return nil, fmt.Errorf("%w: explicit ColumnGraph carrier definition required", errVectorIndexNotDeclared)
		}
	}
	builder, err := newColumnGraphHNSWBuilder(columnGraphHNSWBuildOptions{Name: opts.Name, Field: opts.Field, Metric: opts.Metric, Encoding: opts.Encoding, Dimensions: opts.Dimensions, M: opts.M, EfConstruction: opts.EfConstruction})
	if err != nil {
		return nil, err
	}
	fieldPath, err := parseVectorFieldPath(opts.Field)
	if err != nil {
		return nil, err
	}
	ef := opts.EfSearch
	if ef <= 0 {
		ef = defaultVectorIndexEfSearch
	}
	ratio := opts.RebuildDeletedRatio
	if ratio <= 0 {
		ratio = float64(defaultVectorIndexRebuildPPM) / 1000000
	}
	if ratio > 1 {
		return nil, errors.New("collections: invalid carrier rebuild ratio")
	}
	index := &columnGraphPartitionLiveCarrier{columnGraphHNSWBuilder: builder, collection: c, fieldPath: fieldPath, efSearch: ef, rebuildDeletedRatio: ratio, schemaGeneration: opts.schemaGeneration, nativePersistent: true}
	if c != nil {
		index.nativeRootName = collectionVectorIndexRootName(c.collectionName(), opts.Name)
	}
	index.mutationObserver = index
	return index, nil
}

func (c *Collection) registerColumnGraphPartitionLiveCarrier(index *columnGraphPartitionLiveCarrier) {
	if c == nil || index == nil {
		return
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, index.name)
	if !ok || def.Strategy != VectorIndexStrategyColumnGraph {
		return
	}
	index.recordPersistentDefinition(def)
	index.collection = c
	if index.searchView.Load() == nil && index.hasValidSourceDocumentRoots() {
		index.publishSearchView()
	}
	if coord := c.collectionSchemaCoordinator(); coord != nil {
		coord.registerPartitionLiveCarrier(index)
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexesMu.Lock()
		if c.writeDomain.nativeVectorIndexes == nil {
			c.writeDomain.nativeVectorIndexes = make(map[string]*columnGraphPartitionLiveCarrier)
		}
		c.writeDomain.nativeVectorIndexes[index.name] = index
		c.writeDomain.nativeVectorIndexesMu.Unlock()
	}
}

func (c *Collection) removeColumnGraphPartitionLiveCarrier(name string) {
	if c == nil {
		return
	}
	coord := c.collectionSchemaCoordinator()
	if coord != nil {
		coord.partitionLivePublishMu.Lock()
		defer coord.partitionLivePublishMu.Unlock()
	}
	c.removeColumnGraphPartitionLiveCarrierLocked(name, coord)
}

func (c *Collection) removeColumnGraphPartitionLiveCarrierLocked(name string, coord *collectionSchemaCoordinator) {
	old := c.registeredColumnGraphPartitionLiveCarrier(name)
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexesMu.Lock()
		delete(c.writeDomain.nativeVectorIndexes, name)
		c.writeDomain.nativeVectorIndexesMu.Unlock()
	}
	if coord != nil && old != nil {
		coord.unregisterPartitionLiveCarrier(name, old)
	}
	if !c.hasNativePersistentVectorIndex() && c.manager != nil && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
}

func (c *Collection) registeredColumnGraphPartitionLiveCarriers() []*columnGraphPartitionLiveCarrier {
	return c.registeredVectorPartitionLiveCarriersV1()
}

func (c *Collection) ensureDeclaredNativeVectorIndexesLoadedWithMutationHeld() (map[string]struct{}, error) {
	if c == nil {
		return nil, nil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if c.declaredNativeVectorIndexesLoadedForCurrentCatalog() {
		return nil, nil
	}
	unlock := c.lockNativeVectorIndexLoad()
	defer unlock()
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		snap.Close()
		return nil, err
	}
	if catalog == nil {
		snap.Close()
		return nil, errCollectionNotFound
	}
	generation, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		snap.Close()
		return nil, err
	}
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	snap.Close()
	for _, index := range c.registeredColumnGraphPartitionLiveCarriers() {
		def, ok := findVectorIndex(c.meta.VectorIndexes, index.name)
		if !ok || def.Strategy != VectorIndexStrategyColumnGraph || index.validateNativeSnapshotDefinition(def) != "" {
			c.removeColumnGraphPartitionLiveCarrier(index.name)
		}
	}
	for _, def := range c.meta.VectorIndexes {
		if def.Strategy != VectorIndexStrategyColumnGraph || catalog.rootID(collectionVectorIndexRootName(catalog.meta.Name, def.Name)) == 0 {
			continue
		}
		if index := c.registeredColumnGraphPartitionLiveCarrier(def.Name); index != nil {
			if index.validateNativeSnapshotDefinition(def) == "" && index.hasSourceDocumentReconciliationBaseline(generation) {
				continue
			}
			return nil, fmt.Errorf("%w: partition live carrier %q does not cover current documents", ErrVectorIndexPartitionLiveMismatchV1, def.Name)
		}
		index, status, err := c.loadColumnGraphPartitionLiveCarrier(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			return nil, err
		}
		if index == nil {
			return nil, fmt.Errorf("%w: partition live carrier %q load failed: %s", ErrVectorIndexPartitionLiveUnavailableV1, def.Name, status.ExactFallbackReason)
		}
	}
	return nil, nil
}

func (idx *columnGraphPartitionLiveCarrier) parseStoredVectorRow(materializer *StoredDocumentJSONMaterializer, documentID, document []byte) ([]float32, error) {
	if idx == nil {
		return nil, errors.New("collections: nil partition carrier")
	}
	if len(documentID) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	if document == nil {
		return nil, nil
	}
	vector, present, err := vectorFromStoredDocument(materializer, document, idx.fieldPath)
	if err != nil {
		return nil, fmt.Errorf("collections: vector field %q in document %q: %w", idx.field, documentID, err)
	}
	if !present {
		return nil, nil
	}
	return vector, nil
}

func (idx *columnGraphPartitionLiveCarrier) insertParsedStoredDocumentLocked(documentID []byte, vector []float32, acknowledge bool) error {
	if err := idx.preflightVectorPartitionMutationLocked(documentID, vector); err != nil {
		return err
	}
	if err := idx.reconcileVectorPartitionMutationLocked(documentID, vector); err != nil {
		idx.invalidateVectorPartitionLiveLocked()
		return err
	}
	if acknowledge {
		idx.acknowledgeSearchViewStateLocked()
	}
	return nil
}

func (c *Collection) registeredColumnGraphPartitionLiveCarrierIsStale(index *columnGraphPartitionLiveCarrier) (bool, error) {
	if c == nil || index == nil || c.db == nil {
		return false, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil || catalog == nil {
		return false, err
	}
	c.rememberCatalog(snap, catalog)
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, index.name)
	if !ok || !vectorIndexDefinitionUsesPersistentRuntime(def, index) {
		return index.isNativePersistent(), nil
	}
	wasNativePersistent := index.isNativePersistent()
	if reason := index.validateNativeSnapshotDefinition(def); reason != "" {
		if reason == "schema_generation_mismatch" && !wasNativePersistent {
			index.recordPersistentDefinition(def)
			rootName := collectionVectorIndexRootName(catalog.meta.Name, index.name)
			return catalog.rootID(rootName) != index.nativeSnapshotBaseEpochForFullSave(), nil
		}
		return true, nil
	}
	index.recordPersistentDefinition(def)
	rootName := collectionVectorIndexRootName(catalog.meta.Name, index.name)
	return catalog.rootID(rootName) != index.nativeSnapshotBaseEpochForFullSave(), nil
}

// repairPartitionLiveDocument reconciles one known document into an already
// bound carrier. It cannot construct or serve an ordinary collection runtime.
func (idx *columnGraphPartitionLiveCarrier) repairPartitionLiveDocument(documentID []byte) error {
	if idx == nil || idx.collection == nil || len(documentID) == 0 {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	idx.mu.RLock()
	bound := idx.partitionLiveOnly && idx.partitionLive != nil
	idx.mu.RUnlock()
	if !bound {
		return ErrVectorIndexPartitionLiveUnavailableV1
	}
	runVectorPartitionLiveInsertDocumentBeforePublicationHookForTest()
	unlock := idx.collection.lockNativeVectorIndexPublicationRead()
	defer unlock()
	document, err := idx.collection.Get(documentID)
	if err != nil {
		return err
	}
	materializer, err := idx.collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return err
	}
	defer materializer.Close()
	vector, err := idx.parseStoredVectorRow(materializer, documentID, document)
	if err != nil {
		return err
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.insertParsedStoredDocumentLocked(documentID, vector, true)
}
