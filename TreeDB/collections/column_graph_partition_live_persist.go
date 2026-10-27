package collections

import (
	"bytes"

	"encoding/binary"

	"encoding/json"
	"errors"
	"fmt"
	"math"

	"sort"

	"strconv"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const (
	vectorIndexDocumentGenerationVersion = 3

	vectorIndexNativeKeyMeta             = "meta"
	vectorIndexNativeKeyPrefixNode       = "node/"
	vectorIndexNativeKeyPrefixEdge       = "edge/"
	vectorIndexNativeKeyPrefixTomb       = "tomb/"
	vectorIndexNativeKeyPrefixDoc        = "doc/"
	vectorIndexNativeKeyPrefixLiveOwner  = "partition_live/owner/"
	vectorIndexNativeKeyPrefixLiveDomain = "partition_live/domain/"
	vectorIndexNativeKeyOrdinalWidth     = 20
	vectorIndexNativeKeyEdgeLayerWidth   = 3
)

var (
	errVectorIndexNotDeclared             = errors.New("collections: vector index is not declared in collection metadata")
	errVectorIndexStaleNativeRoot         = errors.New("collections: vector index native root changed since load")
	errVectorIndexStaleDocumentGeneration = errors.New("collections: vector index document generation changed since load")
)

// columnGraphPartitionLiveLoadStatus reports a persisted carrier load or refusal.
type columnGraphPartitionLiveLoadStatus struct {
	Loaded              bool
	ExactFallbackReason string
	RootName            string
	RootID              uint64
	Epoch               uint64
	BytesDisk           int64
}

// saveColumnGraphPartitionLiveSnapshot persists the current in-memory vector index as ordinary
// TreeDB collection-root content and publishes that root through the collection
// root descriptor system state.
func (idx *columnGraphPartitionLiveCarrier) saveColumnGraphPartitionLiveSnapshot() (columnGraphPartitionLiveLoadStatus, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	c := idx.collection
	if c == nil {
		return status, errCollectionNil
	}
	if c.db == nil {
		return status, errCollectionDBNil
	}
	unlockAdmission := c.lockVectorIndexSynchronousPublicationAdmission()
	defer unlockAdmission()
	unlockCoverage := c.lockVectorIndexCoveragePersistence()
	defer unlockCoverage()
	return idx.saveNativeSnapshotWithCoverageLocked()
}

func (idx *columnGraphPartitionLiveCarrier) saveNativeSnapshotWithCoverageLocked() (columnGraphPartitionLiveLoadStatus, error) {
	return idx.saveNativeSnapshotWithCoverageLockedAndCommandWALIntent(nil)
}

func (idx *columnGraphPartitionLiveCarrier) saveNativeSnapshotWithCoverageLockedAndCommandWALIntent(replay *backenddb.CommandWALIntent) (columnGraphPartitionLiveLoadStatus, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	c := idx.collection
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWritesWithCoverageLocked(); err != nil {
		return status, err
	}
	if staleStatus, stale, err := staleNativeSnapshotSaveStatus(c, idx); err != nil {
		return staleStatus, err
	} else if stale {
		return staleStatus, nil
	}
	sourceDocumentGeneration, sourceDocumentState, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		return status, err
	}
	idx.recordSourceDocumentState(sourceDocumentGeneration, sourceDocumentState)
	return idx.saveNativeSnapshotPreparedWithCommandWALIntent(replay)
}

func staleNativeSnapshotSaveStatus(c *Collection, idx *columnGraphPartitionLiveCarrier) (columnGraphPartitionLiveLoadStatus, bool, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	if c == nil || idx == nil {
		return status, false, nil
	}
	if idx.isNativePersistent() && !idx.hasValidSourceDocumentRoots() {
		status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
		return status, false, fmt.Errorf("%w: index %q does not cover current documents", errVectorIndexStaleNativeRoot, idx.name)
	}
	if c.isRegisteredVectorIndex(idx) {
		stale, err := c.registeredColumnGraphPartitionLiveCarrierIsStale(idx)
		if err == nil && stale {
			status.ExactFallbackReason = vectorIndexFallbackStaleRuntimeIndex
			if idx.needsNativeAutoPersist() {
				return status, false, fmt.Errorf("%w: index %q has dirty registered stale runtime", errVectorIndexStaleNativeRoot, idx.name)
			}
			return status, true, nil
		}
		return status, false, nil
	}
	if idx.isNativePersistent() || collectionMetaDeclaresNativeVectorIndex(c.meta, idx.name) {
		status.ExactFallbackReason = vectorIndexFallbackStaleRuntimeIndex
		return status, true, nil
	}
	declared, err := c.refreshNativeVectorIndexDeclaration(idx.name)
	if err != nil || !declared {
		return status, false, nil
	}
	status.ExactFallbackReason = vectorIndexFallbackStaleRuntimeIndex
	return status, true, nil
}

// saveNativeSnapshotPrepared publishes the current graph after the caller has
// already acquired the collection mutation barrier and flushed buffered writes.
func (idx *columnGraphPartitionLiveCarrier) saveNativeSnapshotPrepared() (columnGraphPartitionLiveLoadStatus, error) {
	return idx.saveNativeSnapshotPreparedWithCommandWALIntent(nil)
}

func (idx *columnGraphPartitionLiveCarrier) saveNativeSnapshotPreparedWithCommandWALIntent(replay *backenddb.CommandWALIntent) (columnGraphPartitionLiveLoadStatus, error) {
	return idx.saveNativeSnapshotPreparedWithCommandWALIntentAndPublicationHandoff(replay, nil)
}

func (idx *columnGraphPartitionLiveCarrier) saveNativeSnapshotPreparedWithCommandWALIntentAndPublicationHandoff(replay *backenddb.CommandWALIntent, preparePublication func() (bool, error)) (columnGraphPartitionLiveLoadStatus, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	c := idx.collection
	if c == nil {
		return status, errCollectionNil
	}
	if c.db == nil {
		return status, errCollectionDBNil
	}
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return status, backenddb.ErrClosed
	}
	defer func() { _ = pin.Close() }()
	catalog, err := loadCollectionCatalog(pin, c.meta.Name)
	if err != nil {
		return status, err
	}
	if catalog == nil {
		return status, errCollectionNotFound
	}
	c.meta = catalog.meta
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, idx.name)
	if !ok || !vectorIndexDefinitionUsesPersistentRuntime(def, idx) {
		return status, fmt.Errorf("%w: %q", errVectorIndexNotDeclared, idx.name)
	}
	if reason := idx.validateNativeSnapshotDefinition(def); reason != "" {
		return status, fmt.Errorf("collections: vector index %q does not match collection metadata: %s", idx.name, reason)
	}
	rootName := collectionVectorIndexRootName(catalog.meta.Name, idx.name)
	status.RootName = rootName
	baseRoot := catalog.rootID(rootName)
	if baseEpoch := idx.nativeSnapshotBaseEpochForFullSave(); baseRoot != baseEpoch {
		return status, fmt.Errorf("%w: index %q loaded epoch %d current root %d", errVectorIndexStaleNativeRoot, idx.name, baseEpoch, baseRoot)
	}
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	policy, err := collectionRootStoragePolicyForDB(c.db, catalog.meta, rootName)
	if err != nil {
		return status, err
	}

	snapshot, snapshotSeq := idx.persistSnapshot()
	var nativeVersion *vectorDomainRootVersion
	if _, present, e := getSystemValue(pin, nativeCatalogIncarnationPrefix+catalog.meta.Name); e != nil {
		return status, e
	} else if present {
		key := nativeVectorVersionKey(catalog.meta.Name, idx.name)
		version := def.SchemaGeneration
		raw, exists, e := getSystemValue(pin, key)
		if e != nil {
			return status, e
		}
		if exists {
			if len(raw) != 16 {
				return status, backenddb.ErrNativeSemanticUnsupported
			}
			version = binary.BigEndian.Uint64(raw[:8])
		} else if baseRoot != 0 {
			version = baseRoot
		}
		if version == ^uint64(0) {
			return status, memtable.ErrCOWCapacity
		}
		snapshot.Meta.NativeLogicalVersion = version + 1
		nativeVersion = &vectorDomainRootVersion{key: key, value: nativeVectorVersionRecord(version+1, snapshot.Meta.SourceDocumentGeneration)}
	}

	table, bytesDisk, err := buildVectorIndexNativeSnapshotTable(snapshot)
	if err != nil {
		return status, err
	}
	table.Freeze()
	publishTable, pointerized, err := pointerizeCollectionRunTableValues(c.db, table)
	if err != nil {
		resetCollectionRunTable(table)
		return status, err
	}
	if pointerized {
		defer resetCollectionRunTable(publishTable)
	}
	var intent *backenddb.CommandWALIntent
	if idx.isPartitionLiveCarrier() {
		intent, err = c.newCollectionPersistPartitionLiveCommandWALIntent(idx.name, replay)
	} else {
		intent, err = c.newCollectionRebuildVectorIndexCommandWALIntent(idx.name, replay)
	}
	if err != nil {
		resetCollectionRunTable(table)
		return status, err
	}
	iter := publishTable.NewIterator(nil, nil)
	// Full-image encoding and pointer preparation above retain mutation but
	// not global raw ownership. Hand off only when the image is ready to publish.
	rawPublishLocked := false
	if preparePublication != nil {
		rawPublishLocked, err = preparePublication()
		if err != nil {
			_ = iter.Close()
			resetCollectionRunTable(table)
			return status, err
		}
	}
	publicationMu := idx.nativePublicationLock()
	publicationMu.Lock()
	newSystemRoot, rootIDs, err := c.publishRootDeltaGroupWithoutColumn([]backenddb.OrderedRootDeltaPublishInput{{
		// Native vector snapshots are full graph images. Publish a replacement
		// root so keys removed by rebuild/shrink do not survive from prior roots.
		BaseRoot:      0,
		Iter:          iter,
		StoragePolicy: policy,
	}}, columnWritePublishInput{
		vectorDomainVersion: nativeVersion,
		meta:                catalog.meta,
		baseCommitSeq:       baseCommitSeq,
		baseSystemRoot:      baseSystemRoot,
		rootNames:           []string{rootName},
		baseRootIDs:         baseRootIDs,
		commandWALIntent:    intent,
		rawPublishLocked:    rawPublishLocked,
	})
	if err == nil && len(rootIDs) == 1 {
		status.Loaded = true
		status.RootID = rootIDs[0]
		status.Epoch = rootIDs[0]
		status.BytesDisk = bytesDisk
		idx.setNativePersistent(true)
		idx.recordPersistedSnapshot(status.Epoch, bytesDisk, snapshotSeq)
		c.registerColumnGraphPartitionLiveCarrier(idx)
	}
	publicationMu.Unlock()
	_ = iter.Close()
	resetCollectionRunTable(table)
	if err != nil {
		return status, err
	}
	if len(rootIDs) != 1 {
		return status, unexpectedOrderedRootCountError(catalog.meta.Name, 1, len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, []string{rootName}, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if generation, valid := idx.sourceDocumentCoverage(); valid {
		if state, ok := c.db.StateToken(); ok && state.SystemRootPageID == newSystemRoot {
			idx.recordSourceDocumentState(generation, state)
		}
	}
	return status, nil
}

func (c *Collection) currentNativeVectorIndexRootID(name string) (uint64, error) {
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
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return 0, err
	}
	if catalog == nil {
		return 0, errCollectionNotFound
	}
	return catalog.rootID(collectionVectorIndexRootName(catalog.meta.Name, name)), nil
}

func (c *Collection) installColumnGraphPartitionLiveCarrier(candidate *columnGraphPartitionLiveCarrier, expectedRoot uint64, replaceCurrent *columnGraphPartitionLiveCarrier, replaceMutationSeq uint64) (*columnGraphPartitionLiveCarrier, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if candidate == nil {
		return nil, errors.New("collections: vector index is nil")
	}
	candidateGeneration, candidateCoverageValid := candidate.sourceDocumentCoverage()
	runNativeVectorIndexBeforeInstallHookForTest(candidate.name)
	publicationMu := candidate.nativePublicationLock()
	publicationMu.Lock()
	defer publicationMu.Unlock()

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, candidate.name)
	candidate.mu.RLock()
	hasPartitionLive := candidate.partitionLive != nil
	candidate.mu.RUnlock()
	partitionLiveCarrier := ok && def.Strategy == VectorIndexStrategyColumnGraph && hasPartitionLive
	if !ok || !partitionLiveCarrier {
		return nil, fmt.Errorf("%w: %q", errVectorIndexNotDeclared, candidate.name)
	}
	candidate.setPartitionLiveCarrier(partitionLiveCarrier)
	activeRoot := catalog.rootID(collectionVectorIndexRootName(catalog.meta.Name, def.Name))
	documentGeneration, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return nil, err
	}
	current := c.registeredColumnGraphPartitionLiveCarrier(def.Name)
	replaceCurrentUnchanged := current != nil && current == replaceCurrent && current.nativeMutationSequence() == replaceMutationSeq && activeRoot == expectedRoot
	if current != nil && current.validateNativeSnapshotDefinition(def) == "" && current.nativeSnapshotBaseEpochForFullSave() == activeRoot && current.coversSourceDocumentGeneration(documentGeneration) {
		if !replaceCurrentUnchanged {
			return current, nil
		}
	}
	if reason := candidate.validateNativeSnapshotDefinition(def); reason != "" {
		return nil, fmt.Errorf("%w: index %q candidate metadata changed: %s", errVectorIndexStaleNativeRoot, def.Name, reason)
	}
	if activeRoot != expectedRoot {
		return nil, fmt.Errorf("%w: index %q candidate root %d current root %d", errVectorIndexStaleNativeRoot, def.Name, expectedRoot, activeRoot)
	}
	if current != nil && current.needsNativeAutoPersist() && !replaceCurrentUnchanged {
		return nil, fmt.Errorf("%w: refusing to replace dirty registered index %q", errVectorIndexStaleNativeRoot, def.Name)
	}
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	candidate.invalidateSourceDocumentRoots()
	c.registerColumnGraphPartitionLiveCarrier(candidate)
	rollback := func() {
		if c.registeredColumnGraphPartitionLiveCarrier(def.Name) != candidate {
			return
		}
		if current != nil {
			c.registerColumnGraphPartitionLiveCarrier(current)
		} else {
			c.removeColumnGraphPartitionLiveCarrierLocked(def.Name, c.collectionSchemaCoordinator())
		}
	}
	postInstall := c.db.AcquireSnapshot()
	if postInstall == nil {
		rollback()
		return nil, backenddb.ErrClosed
	}
	postCatalog, err := loadCollectionCatalog(postInstall, c.meta.Name)
	if err != nil {
		_ = postInstall.Close()
		rollback()
		return nil, err
	}
	if postCatalog == nil {
		_ = postInstall.Close()
		rollback()
		return nil, errCollectionNotFound
	}
	postDef, ok := findVectorIndex(postCatalog.meta.VectorIndexes, candidate.name)
	postRoot := postCatalog.rootID(collectionVectorIndexRootName(postCatalog.meta.Name, candidate.name))
	if !ok || !vectorIndexDefinitionUsesPersistentRuntime(postDef, candidate) || candidate.validateNativeSnapshotDefinition(postDef) != "" || postRoot != expectedRoot {
		_ = postInstall.Close()
		rollback()
		return nil, fmt.Errorf("%w: index %q changed during install", errVectorIndexStaleNativeRoot, candidate.name)
	}
	postGeneration, err := vectorIndexDocumentGeneration(postInstall, postCatalog)
	postState, stateOK := postInstall.StateToken()
	_ = postInstall.Close()
	if err != nil {
		rollback()
		return nil, err
	}
	if !candidateCoverageValid || candidateGeneration != postGeneration {
		rollback()
		return nil, fmt.Errorf("%w: index %q candidate generation %d current generation %d", errVectorIndexStaleDocumentGeneration, candidate.name, candidateGeneration, postGeneration)
	}
	if !stateOK {
		rollback()
		return nil, backenddb.ErrClosed
	}
	candidate.recordSourceDocumentState(candidateGeneration, postState)
	candidate.materializePartitionLiveStagedOwnersV2()
	return candidate, nil
}

// saveColumnGraphPartitionLiveDelta persists dirty graph records for a declared vector
// index as a collection-root delta. It is used by live write maintenance; full
// rebuild/shrink publication should continue to use saveColumnGraphPartitionLiveSnapshot so
// removed graph keys cannot survive.
func (idx *columnGraphPartitionLiveCarrier) saveColumnGraphPartitionLiveDelta() (columnGraphPartitionLiveLoadStatus, error) {
	return idx.saveNativeDeltaSnapshotWithCommandWALIntent(nil)
}

func (idx *columnGraphPartitionLiveCarrier) saveNativeDeltaSnapshotWithCommandWALIntent(replay *backenddb.CommandWALIntent) (columnGraphPartitionLiveLoadStatus, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	c := idx.collection
	if c == nil {
		return status, errCollectionNil
	}
	if c.db == nil {
		return status, errCollectionDBNil
	}
	unlockAdmission := c.lockVectorIndexSynchronousPublicationAdmission()
	defer unlockAdmission()
	return idx.saveNativeDeltaSnapshotWithAdmissionHeldAndCommandWALIntent(replay)
}

// saveNativeDeltaSnapshotWithAdmissionHeldAndCommandWALIntent persists a
// native delta while its caller owns native-vector publication admission.
// First standalone partition binding uses the stronger write admission lock so
// no document mutation can observe the carrier before its durable root exists.
func (idx *columnGraphPartitionLiveCarrier) saveNativeDeltaSnapshotWithAdmissionHeldAndCommandWALIntent(replay *backenddb.CommandWALIntent) (columnGraphPartitionLiveLoadStatus, error) {
	return idx.saveNativeDeltaSnapshotWithAdmissionHeldAndCommandWALIntentAndOwner(replay, nil)
}
func (idx *columnGraphPartitionLiveCarrier) saveNativeDeltaSnapshotWithAdmissionHeldAndCommandWALIntentAndOwner(replay *backenddb.CommandWALIntent, owner *CommandWALAdmittedCollection) (columnGraphPartitionLiveLoadStatus, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	c := idx.collection
	if owner == nil {
		unlockCoverage := c.lockVectorIndexCoveragePersistence()
		defer unlockCoverage()
	} else {
		if err := owner.validate(); err != nil {
			return status, err
		}
		if owner.collection != c {
			return status, errors.New("collections: foreign partition persistence owner")
		}
	}
	if owner != nil && idx.needsNativeFullSnapshotAutoPersist() {
		generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
		if err != nil {
			return status, err
		}
		idx.recordSourceDocumentState(generation, state)
		return idx.saveNativeSnapshotPreparedWithCommandWALIntent(replay)
	}
	if idx.needsNativeFullSnapshotAutoPersist() {
		return idx.saveNativeSnapshotWithCoverageLockedAndCommandWALIntent(replay)
	}
	if owner == nil {
		unlockMutation := c.lockMutation()
		defer unlockMutation.Unlock()
	}
	flush := c.flushBufferedWritesWithCoverageLocked
	if owner != nil {
		flush = func() error {
			return c.flushBufferedWritesWithRawPublishStateAndCoverage(c.commandWALRawPublishLocked || replay.StagedForPublish(), true, true, false)
		}
	}
	if err := flush(); err != nil {
		return status, err
	}
	if staleStatus, stale, err := staleNativeSnapshotSaveStatus(c, idx); err != nil {
		return staleStatus, err
	} else if stale {
		return staleStatus, nil
	}
	sourceDocumentGeneration, sourceDocumentState, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		return status, err
	}
	idx.recordSourceDocumentState(sourceDocumentGeneration, sourceDocumentState)

	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return status, backenddb.ErrClosed
	}
	defer func() { _ = pin.Close() }()
	catalog, err := loadCollectionCatalog(pin, c.meta.Name)
	if err != nil {
		return status, err
	}
	if catalog == nil {
		return status, errCollectionNotFound
	}
	c.meta = catalog.meta
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, idx.name)
	if !ok || !vectorIndexDefinitionUsesPersistentRuntime(def, idx) {
		return status, fmt.Errorf("%w: %q", errVectorIndexNotDeclared, idx.name)
	}
	if reason := idx.validateNativeSnapshotDefinition(def); reason != "" {
		return status, fmt.Errorf("collections: vector index %q does not match collection metadata: %s", idx.name, reason)
	}
	rootName := collectionVectorIndexRootName(catalog.meta.Name, idx.name)
	status.RootName = rootName
	baseRoot := catalog.rootID(rootName)
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	policy, err := collectionRootStoragePolicyForDB(c.db, catalog.meta, rootName)
	if err != nil {
		return status, err
	}

	table, bytesDisk, snapshotSeq, persistedEpoch, hasWork, err := idx.persistNativeDeltaTable(baseRoot == 0)
	if err != nil {
		return status, err
	}
	if !hasWork {
		return status, nil
	}
	if baseRoot != persistedEpoch {
		resetCollectionRunTable(table)
		return status, fmt.Errorf("%w: index %q loaded epoch %d current root %d", errVectorIndexStaleNativeRoot, idx.name, persistedEpoch, baseRoot)
	}
	table.Freeze()
	publishTable, pointerized, err := pointerizeCollectionRunTableValues(c.db, table)
	if err != nil {
		resetCollectionRunTable(table)
		return status, err
	}
	if pointerized {
		defer resetCollectionRunTable(publishTable)
	}
	var intent *backenddb.CommandWALIntent
	if idx.isPartitionLiveCarrier() {
		intent, err = c.newCollectionPersistPartitionLiveCommandWALIntent(idx.name, replay)
	} else {
		intent, err = c.newCollectionRebuildVectorIndexCommandWALIntent(idx.name, replay)
	}
	if err != nil {
		resetCollectionRunTable(table)
		return status, err
	}
	iter := publishTable.NewIterator(nil, nil)
	publicationMu := idx.nativePublicationLock()
	publicationMu.Lock()
	newSystemRoot, rootIDs, err := c.publishRootDeltaGroupWithoutColumn([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: policy,
	}}, columnWritePublishInput{
		meta:             catalog.meta,
		baseCommitSeq:    baseCommitSeq,
		baseSystemRoot:   baseSystemRoot,
		rootNames:        []string{rootName},
		baseRootIDs:      baseRootIDs,
		commandWALIntent: intent,
	})
	if err == nil && len(rootIDs) == 1 {
		status.Loaded = true
		status.RootID = rootIDs[0]
		status.Epoch = rootIDs[0]
		status.BytesDisk = bytesDisk
		idx.setNativePersistent(true)
		idx.recordPersistedSnapshot(status.Epoch, bytesDisk, snapshotSeq)
	}
	publicationMu.Unlock()
	_ = iter.Close()
	resetCollectionRunTable(table)
	if err != nil {
		return status, err
	}
	if len(rootIDs) != 1 {
		return status, unexpectedOrderedRootCountError(catalog.meta.Name, 1, len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, []string{rootName}, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if generation, valid := idx.sourceDocumentCoverage(); valid {
		if state, ok := c.db.StateToken(); ok && state.SystemRootPageID == newSystemRoot {
			idx.recordSourceDocumentState(generation, state)
		}
	}
	return status, nil
}

// loadColumnGraphPartitionLiveCarrier restores a bound modern carrier from its
// published root. Missing, incomplete or corrupt state returns an unavailable
// status; callers must not rebuild or select a fallback engine here.
func (c *Collection) loadColumnGraphPartitionLiveCarrier(opts columnGraphPartitionLiveOptions) (*columnGraphPartitionLiveCarrier, columnGraphPartitionLiveLoadStatus, error) {
	status := columnGraphPartitionLiveLoadStatus{}
	if c == nil {
		return nil, status, errCollectionNil
	}
	if c.db == nil {
		return nil, status, errCollectionDBNil
	}
	name := vectorIndexOptionName(opts)
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, status, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, status, err
	}
	if catalog == nil {
		return nil, status, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		status.ExactFallbackReason = vectorIndexFallbackMissingVectorIndexMetadata
		return nil, status, nil
	}
	rootName := collectionVectorIndexRootName(catalog.meta.Name, def.Name)
	status.RootName = rootName
	rootID := catalog.rootID(rootName)
	if rootID == 0 && len(catalog.overlayRootIDs(rootName)) == 0 {
		status.ExactFallbackReason = vectorIndexFallbackMissingGraphRoot
		return nil, status, nil
	}
	snapshot, bytesDisk, reason, err := readVectorIndexNativeSnapshot(snap, catalog, rootName)
	if err != nil {
		return nil, status, err
	}
	if reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	if def.Strategy != VectorIndexStrategyColumnGraph || snapshot.Meta.PartitionLive == nil {
		status.ExactFallbackReason = vectorIndexFallbackMetaMismatch
		return nil, status, nil
	}
	matchesDocumentRoots, err := vectorIndexSnapshotMatchesDocumentRoots(snapshot.Meta, catalog, snap)
	if err != nil {
		return nil, status, err
	}
	if !matchesDocumentRoots {
		status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
		return nil, status, nil
	}
	index, err := newColumnGraphPartitionLiveDomain(c, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return nil, status, err
	}
	if reason := index.loadPersistSnapshot(snapshot); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	index.setPartitionLiveCarrier(def.Strategy == VectorIndexStrategyColumnGraph)
	if index.validateNativeSnapshotDefinition(def) != "" {
		status.ExactFallbackReason = vectorIndexFallbackMetaMismatch
		return nil, status, nil
	}
	status.RootID = rootID
	index.recordLoadedSnapshot(rootID, bytesDisk)
	installed, err := c.installColumnGraphPartitionLiveCarrier(index, rootID, nil, 0)
	if err != nil {
		if errors.Is(err, errVectorIndexStaleDocumentGeneration) {
			status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
			return nil, status, nil
		}
		status.ExactFallbackReason = vectorIndexFallbackStaleRuntimeIndex
		return nil, status, err
	}
	status.Loaded = true
	status.Epoch, status.BytesDisk = installed.persistedCarrierMetadata()
	status.RootID = status.Epoch
	return installed, status, nil
}

func vectorIndexSnapshotMatchesDocumentRoots(meta vectorIndexPersistMeta, catalog *collectionCatalog, snap *backenddb.Snapshot) (bool, error) {
	if catalog == nil || meta.SourceDocumentGenerationVersion != vectorIndexDocumentGenerationVersion {
		return false, nil
	}
	generation, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return false, err
	}
	return meta.SourceDocumentGeneration == generation, nil
}

// PruneOldSnapshots removes older immutable epoch directories for this vector
// index while preserving the currently published manifest epoch and the newest
// keep-1 additional epochs. It never removes temp directories.

func vectorIndexDefaultName(field string) string {
	if field == "" {
		return "default"
	}
	return field
}

type vectorIndexPersistSnapshot struct {
	Meta                 vectorIndexPersistMeta
	Nodes                []vectorIndexPersistNode
	Edges                []vectorIndexPersistEdges
	Tombstones           vectorIndexPersistTombstones
	DocMap               vectorIndexPersistDocMap
	PartitionLiveOwners  []vectorIndexPartitionLivePersistOwnerV1
	PartitionLiveDomains []vectorIndexPartitionLivePersistDomainV1
}

type vectorIndexPersistMeta struct {
	NativeLogicalVersion            uint64                              `json:"native_logical_version,omitempty"`
	Name                            string                              `json:"name"`
	Field                           string                              `json:"field"`
	Metric                          VectorMetric                        `json:"metric"`
	Encoding                        VectorIndexEncoding                 `json:"encoding"`
	Dimensions                      int                                 `json:"dimensions"`
	M                               int                                 `json:"m"`
	EfConstruction                  int                                 `json:"ef_construction"`
	EfSearch                        int                                 `json:"ef_search"`
	RebuildDeletedRatio             float64                             `json:"rebuild_deleted_ratio"`
	Entry                           int                                 `json:"entry"`
	MaxLevel                        int                                 `json:"max_level"`
	SourceDocumentGenerationVersion int                                 `json:"source_document_generation_version"`
	SourceDocumentGeneration        uint64                              `json:"source_document_generation"`
	PartitionLive                   *vectorIndexPartitionLivePersistV1  `json:"partition_live,omitempty"`
	Preparation                     *VectorPartitionPrepareCompletionV1 `json:"preparation,omitempty"`
}

type vectorIndexPersistNode struct {
	DocumentID string    `json:"document_id"`
	Vector     []float32 `json:"vector,omitempty"`
	Quantized  []int8    `json:"quantized,omitempty"`
	QuantScale float32   `json:"quant_scale,omitempty"`
	Level      int       `json:"level"`
	Deleted    bool      `json:"deleted,omitempty"`
}

type vectorIndexPersistEdges struct {
	NodeID    int       `json:"node_id"`
	Layer     int       `json:"layer"`
	Neighbor  []int     `json:"neighbors"`
	Distances []float32 `json:"distances,omitempty"`
}

type vectorIndexPersistTombstones struct {
	NodeIDs []int `json:"node_ids"`
}

type vectorIndexPersistDocMap struct {
	Current map[string]int `json:"current"`
}

func (idx *columnGraphPartitionLiveCarrier) persistSnapshot() (vectorIndexPersistSnapshot, uint64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	seq := idx.mutationSeq
	liveMeta := idx.partitionLiveMetaPersistLockedV2()
	liveOwners, liveDomains := idx.partitionLiveSnapshotRecordsLockedV2()
	snapshot := vectorIndexPersistSnapshot{
		Meta: vectorIndexPersistMeta{
			Name:                            idx.name,
			Field:                           idx.field,
			Metric:                          idx.metric,
			Encoding:                        idx.encoding,
			Dimensions:                      idx.dimensions,
			M:                               idx.m,
			EfConstruction:                  idx.efConstruction,
			EfSearch:                        idx.efSearch,
			RebuildDeletedRatio:             idx.rebuildDeletedRatio,
			Entry:                           idx.entry,
			MaxLevel:                        idx.maxLevel,
			SourceDocumentGenerationVersion: vectorIndexDocumentGenerationVersion,
			SourceDocumentGeneration:        idx.sourceDocumentGeneration,
			PartitionLive:                   liveMeta,
			Preparation:                     idx.partitionPreparation,
		},
		Nodes: make([]vectorIndexPersistNode, len(idx.nodes)),
		DocMap: vectorIndexPersistDocMap{
			Current: make(map[string]int, len(idx.currentNode)),
		},
		PartitionLiveOwners: liveOwners, PartitionLiveDomains: liveDomains,
	}
	for i, node := range idx.nodes {
		snapshot.Nodes[i] = vectorIndexPersistNode{
			DocumentID: string(node.documentID),
			Vector:     append([]float32(nil), node.vector...),
			Quantized:  append([]int8(nil), node.quantized...),
			QuantScale: node.quantScale,
			Level:      node.level,
			Deleted:    node.deleted,
		}
		for layer, neighbors := range node.neighbors {
			neighborIDs := make([]int, len(neighbors))
			distances := make([]float32, len(neighbors))
			for j, neighbor := range neighbors {
				neighborIDs[j] = int(neighbor.nodeID)
				distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
				if !ok {
					distance = idx.distanceBetweenNodesLocked(i, int(neighbor.nodeID))
					distance, ok = normalizeVectorIndexEdgeDistance(distance)
					if !ok {
						distance = math.MaxFloat32
					}
				}
				distances[j] = distance
			}
			snapshot.Edges = append(snapshot.Edges, vectorIndexPersistEdges{
				NodeID:    i,
				Layer:     layer,
				Neighbor:  neighborIDs,
				Distances: distances,
			})
		}
		if node.deleted {
			snapshot.Tombstones.NodeIDs = append(snapshot.Tombstones.NodeIDs, i)
		}
	}
	for docID, nodeID := range idx.currentNode {
		snapshot.DocMap.Current[docID] = nodeID
	}
	sort.Ints(snapshot.Tombstones.NodeIDs)
	return snapshot, seq
}

func buildVectorIndexNativeSnapshotTable(snapshot vectorIndexPersistSnapshot) (memtable.Table, int64, error) {
	entryCount := 1 + len(snapshot.Nodes) + len(snapshot.Edges) + len(snapshot.Tombstones.NodeIDs) + len(snapshot.DocMap.Current) + len(snapshot.PartitionLiveOwners)
	table := newCollectionRunTable(entryCount)
	var bytesDisk int64
	add := func(key []byte, payload any) error {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		bytesDisk += int64(len(data))
		table.SetSteal(key, data)
		return nil
	}
	if err := add([]byte(vectorIndexNativeKeyMeta), snapshot.Meta); err != nil {
		return nil, 0, err
	}
	for i := range snapshot.Nodes {
		if err := add(vectorIndexNativeNodeKey(i), snapshot.Nodes[i]); err != nil {
			return nil, 0, err
		}
	}
	for i := range snapshot.Edges {
		edge := snapshot.Edges[i]
		if err := add(vectorIndexNativeEdgeKey(edge.NodeID, edge.Layer), edge); err != nil {
			return nil, 0, err
		}
	}
	for _, nodeID := range snapshot.Tombstones.NodeIDs {
		if err := add(vectorIndexNativeTombstoneKey(nodeID), nodeID); err != nil {
			return nil, 0, err
		}
	}
	for docID, nodeID := range snapshot.DocMap.Current {
		if err := add(vectorIndexNativeDocKey(docID), nodeID); err != nil {
			return nil, 0, err
		}
	}
	if live := snapshot.Meta.PartitionLive; live != nil && live.Version == 3 {
		for _, owner := range snapshot.PartitionLiveOwners {
			if err := add(vectorIndexPartitionLiveOwnerKeyV2(live.OwnerEpoch, owner.ID), owner); err != nil {
				resetCollectionRunTable(table)
				return nil, 0, err
			}
		}
		for _, domain := range snapshot.PartitionLiveDomains {
			domainTable, domainBytes, err := buildVectorIndexNativeSnapshotTable(domain.Snapshot)
			if err != nil {
				resetCollectionRunTable(table)
				return nil, 0, err
			}
			domainTable.Freeze()
			it := domainTable.NewIterator(nil, nil)
			prefix := vectorIndexPartitionLiveDomainPrefixV2(domain.Domain, domain.Epoch)
			for it.Valid() {
				key := append(append([]byte(nil), prefix...), it.KeyCopy(nil)...)
				if it.IsDeleted() {
					table.DeleteSteal(key)
				} else {
					table.SetSteal(key, it.ValueCopy(nil))
				}
				it.Next()
			}
			iterErr := it.Error()
			_ = it.Close()
			resetCollectionRunTable(domainTable)
			if iterErr != nil {
				resetCollectionRunTable(table)
				return nil, 0, iterErr
			}
			bytesDisk += domainBytes
		}
	}
	return table, bytesDisk, nil
}

func vectorIndexPartitionLiveOwnerKeyV2(epoch uint64, id string) []byte {
	key := make([]byte, 0, len(vectorIndexNativeKeyPrefixLiveOwner)+vectorIndexNativeKeyOrdinalWidth+1+len(id))
	key = append(key, vectorIndexNativeKeyPrefixLiveOwner...)
	key = append(key, fmt.Sprintf("%020d/", epoch)...)
	key = append(key, id...)
	return key
}

func vectorIndexPartitionLiveDomainPrefixV2(domain uint32, epoch uint64) []byte {
	return []byte(fmt.Sprintf("%s%020d/%020d/", vectorIndexNativeKeyPrefixLiveDomain, domain, epoch))
}

func (idx *columnGraphPartitionLiveCarrier) persistNativeDeltaTable(includeMeta bool) (memtable.Table, int64, uint64, uint64, bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	seq := idx.mutationSeq
	persistedEpoch := idx.persistedEpoch
	if seq == 0 && !idx.dirtyMeta {
		return nil, 0, seq, persistedEpoch, false, nil
	}
	if includeMeta || idx.dirtyMeta {
		includeMeta = true
	}
	nodeIDs := make([]int, 0, len(idx.dirtyNodes))
	for nodeID := range idx.dirtyNodes {
		if nodeID >= 0 && nodeID < len(idx.nodes) {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	sort.Ints(nodeIDs)
	docIDs := make([]string, 0, len(idx.dirtyDocs))
	for docID := range idx.dirtyDocs {
		docIDs = append(docIDs, docID)
	}
	sort.Strings(docIDs)
	if !includeMeta && len(nodeIDs) == 0 && len(docIDs) == 0 {
		return nil, 0, seq, persistedEpoch, false, nil
	}

	entryCount := len(docIDs)
	if includeMeta {
		entryCount++
	}
	for _, nodeID := range nodeIDs {
		entryCount += 1 + len(idx.nodes[nodeID].neighbors)
		if idx.nodes[nodeID].deleted {
			entryCount++
		}
	}
	table := newCollectionRunTable(entryCount)
	var bytesDisk int64
	add := func(key []byte, payload any) error {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		bytesDisk += int64(len(data))
		table.SetSteal(key, data)
		return nil
	}
	if includeMeta {
		if err := add([]byte(vectorIndexNativeKeyMeta), idx.persistMetaLocked()); err != nil {
			resetCollectionRunTable(table)
			return nil, 0, seq, persistedEpoch, false, err
		}
	}
	var edge vectorIndexPersistEdges
	for _, nodeID := range nodeIDs {
		if err := idx.emitNativeNodeLocked(nodeID, &edge, add); err != nil {
			resetCollectionRunTable(table)
			return nil, 0, seq, persistedEpoch, false, err
		}
	}
	for _, docID := range docIDs {
		nodeID, ok := idx.currentNode[docID]
		if !ok {
			table.DeleteSteal(vectorIndexNativeDocKey(docID))
			continue
		}
		if err := add(vectorIndexNativeDocKey(docID), nodeID); err != nil {
			resetCollectionRunTable(table)
			return nil, 0, seq, persistedEpoch, false, err
		}
	}
	if live := idx.partitionLive; live != nil && !live.invalid {
		ownerIDs := make([]string, 0, len(live.dirtyOwners))
		for id := range live.dirtyOwners {
			ownerIDs = append(ownerIDs, id)
		}
		sort.Strings(ownerIDs)
		for _, id := range ownerIDs {
			owner, ok := live.ownerV1(id)
			if !ok {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, fmt.Errorf("collections: partition-live dirty owner %q is missing", id)
			}
			if err := add(vectorIndexPartitionLiveOwnerKeyV2(live.ownerEpoch, id), vectorIndexPartitionLivePersistOwnerV1{ID: id, Domain: owner.domain, Deleted: owner.deleted}); err != nil {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, err
			}
		}
		domains := make([]uint32, 0, len(live.dirtyDomains))
		for domain := range live.dirtyDomains {
			domains = append(domains, domain)
		}
		sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
		for _, domain := range domains {
			delta := live.domains[domain]
			if delta == nil {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, fmt.Errorf("collections: partition-live dirty domain %d is missing", domain)
			}
			_, full := live.fullDomains[domain]
			domainTable, domainBytes, _, _, domainWork, err := delta.persistNativeDeltaTable(full)
			if err != nil {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, err
			}
			if !domainWork {
				resetCollectionRunTable(domainTable)
				continue
			}
			domainTable.Freeze()
			it := domainTable.NewIterator(nil, nil)
			prefix := vectorIndexPartitionLiveDomainPrefixV2(domain, live.domainEpochs[domain])
			for it.Valid() {
				key := append(append([]byte(nil), prefix...), it.KeyCopy(nil)...)
				if it.IsDeleted() {
					table.DeleteSteal(key)
				} else {
					table.SetSteal(key, it.ValueCopy(nil))
				}
				it.Next()
			}
			iterErr := it.Error()
			_ = it.Close()
			resetCollectionRunTable(domainTable)
			if iterErr != nil {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, iterErr
			}
			bytesDisk += domainBytes
		}
	}
	return table, bytesDisk, seq, persistedEpoch, true, nil
}

func (idx *columnGraphPartitionLiveCarrier) persistMetaLocked() vectorIndexPersistMeta {
	return vectorIndexPersistMeta{
		Name:                            idx.name,
		Field:                           idx.field,
		Metric:                          idx.metric,
		Encoding:                        idx.encoding,
		Dimensions:                      idx.dimensions,
		M:                               idx.m,
		EfConstruction:                  idx.efConstruction,
		EfSearch:                        idx.efSearch,
		RebuildDeletedRatio:             idx.rebuildDeletedRatio,
		Entry:                           idx.entry,
		MaxLevel:                        idx.maxLevel,
		SourceDocumentGenerationVersion: vectorIndexDocumentGenerationVersion,
		SourceDocumentGeneration:        idx.sourceDocumentGeneration,
		PartitionLive:                   idx.partitionLiveMetaPersistLockedV2(),
		Preparation:                     idx.partitionPreparation,
	}
}

// emitNativeNodeLocked uses the maintained record structs and edge normalization.
// Encoding is synchronous under the graph read lock; vectors may be borrowed,
// and one edge-conversion workspace is reused instead of copying a full graph.
func (idx *columnGraphPartitionLiveCarrier) emitNativeNodeLocked(nodeID int, edge *vectorIndexPersistEdges, add func([]byte, any) error) error {
	node := idx.nodes[nodeID]
	if err := add(vectorIndexNativeNodeKey(nodeID), vectorIndexPersistNode{
		DocumentID: string(node.documentID), Vector: node.vector, Quantized: node.quantized,
		QuantScale: node.quantScale, Level: node.level, Deleted: node.deleted,
	}); err != nil {
		return err
	}
	for layer, neighbors := range node.neighbors {
		edge.NodeID, edge.Layer = nodeID, layer
		if cap(edge.Neighbor) < len(neighbors) {
			edge.Neighbor = make([]int, len(neighbors))
			edge.Distances = make([]float32, len(neighbors))
		} else {
			edge.Neighbor = edge.Neighbor[:len(neighbors)]
			edge.Distances = edge.Distances[:len(neighbors)]
		}
		for i, neighbor := range neighbors {
			edge.Neighbor[i] = int(neighbor.nodeID)
			distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
			if !ok {
				distance = idx.distanceBetweenNodesLocked(nodeID, int(neighbor.nodeID))
				distance, ok = normalizeVectorIndexEdgeDistance(distance)
				if !ok {
					distance = math.MaxFloat32
				}
			}
			edge.Distances[i] = distance
		}
		if err := add(vectorIndexNativeEdgeKey(nodeID, layer), *edge); err != nil {
			return err
		}
	}
	if node.deleted {
		return add(vectorIndexNativeTombstoneKey(nodeID), nodeID)
	}
	return nil
}

func readVectorIndexNativeSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string) (vectorIndexPersistSnapshot, int64, string, error) {
	var snapshot vectorIndexPersistSnapshot
	var bytesDisk int64
	rawMeta, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, []byte(vectorIndexNativeKeyMeta), nil)
	if err != nil {
		return snapshot, 0, "", err
	}
	if !ok {
		return snapshot, 0, vectorIndexFallbackMissingGraphRootEntry, nil
	}
	bytesDisk += int64(len(rawMeta))
	if err := json.Unmarshal(rawMeta, &snapshot.Meta); err != nil {
		return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootEntry, nil
	}

	nodes := make(map[int]vectorIndexPersistNode)
	maxNodeID := -1
	snapshot.DocMap.Current = make(map[string]int)
	type liveDomainReadV2 struct {
		snapshot vectorIndexPersistSnapshot
		nodes    map[int]vectorIndexPersistNode
		maxNode  int
		seenMeta bool
	}
	activeDomainEpochs := make(map[uint32]uint64)
	if live := snapshot.Meta.PartitionLive; live != nil && live.Version == 3 {
		for _, domain := range live.DomainEpochs {
			activeDomainEpochs[domain.Domain] = domain.Epoch
		}
	}
	liveDomains := make(map[uint32]*liveDomainReadV2, len(activeDomainEpochs))
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil {
		return snapshot, bytesDisk, "", err
	}
	if it == nil {
		return snapshot, bytesDisk, vectorIndexFallbackMissingGraphRoot, nil
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		key := it.KeyCopy(nil)
		value := it.ValueCopy(nil)
		if err := it.Error(); err != nil {
			return snapshot, bytesDisk, "", err
		}
		if bytes.Equal(key, []byte(vectorIndexNativeKeyMeta)) {
			it.Next()
			continue
		}
		bytesDisk += int64(len(value))
		switch {
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixLiveOwner)):
			live := snapshot.Meta.PartitionLive
			rest := key[len(vectorIndexNativeKeyPrefixLiveOwner):]
			if live == nil || live.Version != 3 || len(rest) <= vectorIndexNativeKeyOrdinalWidth || rest[vectorIndexNativeKeyOrdinalWidth] != '/' {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			epoch, err := strconv.ParseUint(string(rest[:vectorIndexNativeKeyOrdinalWidth]), 10, 64)
			if err != nil || epoch == 0 {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			if epoch == live.OwnerEpoch {
				var owner vectorIndexPartitionLivePersistOwnerV1
				if err := json.Unmarshal(value, &owner); err != nil || owner.ID != string(rest[vectorIndexNativeKeyOrdinalWidth+1:]) {
					return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootEntry, nil
				}
				snapshot.PartitionLiveOwners = append(snapshot.PartitionLiveOwners, owner)
			}
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixLiveDomain)):
			live := snapshot.Meta.PartitionLive
			rest := key[len(vectorIndexNativeKeyPrefixLiveDomain):]
			segment := vectorIndexNativeKeyOrdinalWidth
			if live == nil || live.Version != 3 || len(rest) <= segment*2+2 || rest[segment] != '/' || rest[segment*2+1] != '/' {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			domain64, domainErr := strconv.ParseUint(string(rest[:segment]), 10, 32)
			epoch, epochErr := strconv.ParseUint(string(rest[segment+1:segment*2+1]), 10, 64)
			if domainErr != nil || epochErr != nil || epoch == 0 {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			domain := uint32(domain64)
			if activeDomainEpochs[domain] != epoch {
				break
			}
			state := liveDomains[domain]
			if state == nil {
				state = &liveDomainReadV2{nodes: make(map[int]vectorIndexPersistNode), maxNode: -1}
				state.snapshot.DocMap.Current = make(map[string]int)
				liveDomains[domain] = state
			}
			nativeKey := rest[segment*2+2:]
			if bytes.Equal(nativeKey, []byte(vectorIndexNativeKeyMeta)) {
				if state.seenMeta || json.Unmarshal(value, &state.snapshot.Meta) != nil {
					return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootEntry, nil
				}
				state.seenMeta = true
			} else if reason := appendVectorIndexNativeSnapshotRecordV2(&state.snapshot, state.nodes, &state.maxNode, nativeKey, value); reason != "" {
				return snapshot, bytesDisk, reason, nil
			}
		default:
			if reason := appendVectorIndexNativeSnapshotRecordV2(&snapshot, nodes, &maxNodeID, key, value); reason != "" {
				return snapshot, bytesDisk, reason, nil
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return snapshot, bytesDisk, "", err
	}
	if reason := finalizeVectorIndexNativeSnapshotRecordsV2(&snapshot, nodes, maxNodeID); reason != "" {
		return snapshot, bytesDisk, reason, nil
	}
	if live := snapshot.Meta.PartitionLive; live != nil && live.Version == 3 {
		sort.Slice(snapshot.PartitionLiveOwners, func(i, j int) bool { return snapshot.PartitionLiveOwners[i].ID < snapshot.PartitionLiveOwners[j].ID })
		for _, descriptor := range live.DomainEpochs {
			state := liveDomains[descriptor.Domain]
			if state == nil || !state.seenMeta {
				return snapshot, bytesDisk, vectorIndexFallbackMissingGraphRootEntry, nil
			}
			if reason := finalizeVectorIndexNativeSnapshotRecordsV2(&state.snapshot, state.nodes, state.maxNode); reason != "" {
				return snapshot, bytesDisk, reason, nil
			}
			snapshot.PartitionLiveDomains = append(snapshot.PartitionLiveDomains, vectorIndexPartitionLivePersistDomainV1{Domain: descriptor.Domain, Epoch: descriptor.Epoch, Snapshot: state.snapshot})
		}
	}
	sort.Ints(snapshot.Tombstones.NodeIDs)
	return snapshot, bytesDisk, "", nil
}

func appendVectorIndexNativeSnapshotRecordV2(snapshot *vectorIndexPersistSnapshot, nodes map[int]vectorIndexPersistNode, maxNodeID *int, key, value []byte) string {
	return appendVectorIndexNativeSnapshotSeededRecordV2(snapshot, nodes, maxNodeID, key, value, vectorIndexPersistNode{}, vectorIndexPersistEdges{})
}

// Optional native seeds own admitted array backing. The record codec and its
// duplicate, ordinal and JSON validation remain shared with physical readers.
func appendVectorIndexNativeSnapshotSeededRecordV2(snapshot *vectorIndexPersistSnapshot, nodes map[int]vectorIndexPersistNode, maxNodeID *int, key, value []byte, node vectorIndexPersistNode, edge vectorIndexPersistEdges) string {
	switch {
	case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixNode)):
		nodeID, ok := parseVectorIndexNativeOrdinal(string(key[len(vectorIndexNativeKeyPrefixNode):]))
		if !ok {
			return vectorIndexFallbackInvalidGraphRootKey
		}
		var err error
		if node, err = decodeVectorIndexPersistNode(value, node); err != nil {
			return vectorIndexFallbackInvalidGraphRootEntry
		}
		if _, duplicate := nodes[nodeID]; duplicate {
			return vectorIndexFallbackInvalidGraphRootEntry
		}
		nodes[nodeID] = node
		if nodeID > *maxNodeID {
			*maxNodeID = nodeID
		}
	case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixEdge)):
		var err error
		if edge, err = decodeVectorIndexPersistEdge(value, edge); err != nil {
			return vectorIndexFallbackInvalidGraphRootEntry
		}
		snapshot.Edges = append(snapshot.Edges, edge)
	case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixTomb)):
		nodeID, ok := parseVectorIndexNativeOrdinal(string(key[len(vectorIndexNativeKeyPrefixTomb):]))
		if !ok {
			return vectorIndexFallbackInvalidGraphRootKey
		}
		snapshot.Tombstones.NodeIDs = append(snapshot.Tombstones.NodeIDs, nodeID)
	case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixDoc)):
		var nodeID int
		if err := json.Unmarshal(value, &nodeID); err != nil {
			return vectorIndexFallbackInvalidGraphRootEntry
		}
		docID := string(key[len(vectorIndexNativeKeyPrefixDoc):])
		if _, duplicate := snapshot.DocMap.Current[docID]; duplicate {
			return vectorIndexFallbackInvalidGraphRootEntry
		}
		snapshot.DocMap.Current[docID] = nodeID
	default:
		return vectorIndexFallbackInvalidGraphRootKey
	}
	return ""
}

func decodeVectorIndexPersistNode(value []byte, seed vectorIndexPersistNode) (vectorIndexPersistNode, error) {
	err := json.Unmarshal(value, &seed)
	return seed, err
}

func decodeVectorIndexPersistEdge(value []byte, seed vectorIndexPersistEdges) (vectorIndexPersistEdges, error) {
	err := json.Unmarshal(value, &seed)
	return seed, err
}

func finalizeVectorIndexNativeSnapshotRecordsV2(snapshot *vectorIndexPersistSnapshot, nodes map[int]vectorIndexPersistNode, maxNodeID int) string {
	if maxNodeID >= 0 {
		snapshot.Nodes = make([]vectorIndexPersistNode, maxNodeID+1)
		for nodeID := 0; nodeID <= maxNodeID; nodeID++ {
			node, ok := nodes[nodeID]
			if !ok {
				return vectorIndexFallbackMissingGraphRootEntry
			}
			snapshot.Nodes[nodeID] = node
		}
	}
	sort.Ints(snapshot.Tombstones.NodeIDs)
	return ""
}

func vectorIndexNativeNodeKey(nodeID int) []byte {
	return []byte(vectorIndexNativeKeyPrefixNode + formatVectorIndexNativeOrdinal(nodeID))
}

func vectorIndexNativeEdgeKey(nodeID, layer int) []byte {
	return []byte(vectorIndexNativeKeyPrefixEdge + formatVectorIndexNativeOrdinal(nodeID) + "/" + formatVectorIndexNativeEdgeLayer(layer))
}

func vectorIndexNativeTombstoneKey(nodeID int) []byte {
	return []byte(vectorIndexNativeKeyPrefixTomb + formatVectorIndexNativeOrdinal(nodeID))
}

func vectorIndexNativeDocKey(docID string) []byte {
	key := make([]byte, 0, len(vectorIndexNativeKeyPrefixDoc)+len(docID))
	key = append(key, vectorIndexNativeKeyPrefixDoc...)
	key = append(key, docID...)
	return key
}

func formatVectorIndexNativeOrdinal(value int) string {
	return fmt.Sprintf("%0*d", vectorIndexNativeKeyOrdinalWidth, value)
}

func formatVectorIndexNativeEdgeLayer(value int) string {
	return fmt.Sprintf("%0*d", vectorIndexNativeKeyEdgeLayerWidth, value)
}

func parseVectorIndexNativeOrdinal(value string) (int, bool) {
	if len(value) != vectorIndexNativeKeyOrdinalWidth {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, false
	}
	return parsed, true
}

func vectorIndexOptionName(opts columnGraphPartitionLiveOptions) string {
	if opts.Name != "" {
		return opts.Name
	}
	return vectorIndexDefaultName(opts.Field)
}

func vectorIndexOptionsFromDefinition(def VectorIndexDefinition) columnGraphPartitionLiveOptions {
	return columnGraphPartitionLiveOptions{
		Name:             def.Name,
		Field:            def.Field,
		Metric:           def.Metric,
		Dimensions:       def.Dimensions,
		M:                def.M,
		EfConstruction:   def.EfConstruction,
		EfSearch:         def.EfSearch,
		Encoding:         def.Encoding,
		schemaGeneration: def.SchemaGeneration,
		ownerDefinition:  &def,
	}
}

func (idx *columnGraphPartitionLiveCarrier) validateNativeSnapshotDefinition(def VectorIndexDefinition) string {
	if idx == nil {
		return "nil_index"
	}
	if def.Field != idx.field {
		return "field_mismatch"
	}
	if def.Metric != idx.metric {
		return "metric_mismatch"
	}
	if def.Encoding != idx.encoding {
		return "encoding_mismatch"
	}
	if def.SchemaGeneration != idx.schemaGeneration {
		return "schema_generation_mismatch"
	}
	if def.Dimensions != idx.dimensions {
		return "dimension_mismatch"
	}
	if def.M != idx.m || def.EfConstruction != idx.efConstruction || def.EfSearch != idx.efSearch {
		return "hnsw_param_mismatch"
	}
	return ""
}

func (idx *columnGraphPartitionLiveCarrier) recordPersistedSnapshot(epoch uint64, bytesDisk int64, snapshotSeq uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.persistedEpoch = epoch
	idx.fullSnapshotBaseEpoch = 0
	idx.persistedBytesDisk = bytesDisk
	idx.persistedSnapshotDirty = idx.mutationSeq != snapshotSeq
	if !idx.persistedSnapshotDirty {
		idx.dirtyMeta = false
		clear(idx.dirtyNodes)
		clear(idx.dirtyDocs)
		idx.acknowledgePartitionLivePersistenceLockedV2()
	}
	if view := idx.searchView.Load(); idx.nativePersistent && view != nil {
		view.persisted.Store(&vectorIndexSearchPersistedMetadata{epoch: epoch, bytesDisk: bytesDisk})
	}
}

func (idx *columnGraphPartitionLiveCarrier) recordLoadedSnapshot(epoch uint64, bytesDisk int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.persistedEpoch = epoch
	idx.fullSnapshotBaseEpoch = 0
	idx.persistedBytesDisk = bytesDisk
	idx.persistedSnapshotDirty = false
	idx.mutationSeq = 0
	idx.dirtyMeta = false
	clear(idx.dirtyNodes)
	clear(idx.dirtyDocs)
	idx.publishSearchViewLocked(false)
}

func (idx *columnGraphPartitionLiveCarrier) needsNativeAutoPersist() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.dirtyMeta || (idx.mutationSeq != 0 && (idx.persistedEpoch == 0 || idx.persistedSnapshotDirty))
}

func (idx *columnGraphPartitionLiveCarrier) needsNativeFullSnapshotAutoPersist() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.fullSnapshotBaseEpoch != 0 && idx.mutationSeq != 0
}

func validateVectorIndexPersistNode(node vectorIndexPersistNode, meta vectorIndexPersistMeta) string {
	switch meta.Encoding {
	case VectorIndexEncodingFloat32:
		if len(node.Vector) != meta.Dimensions || len(node.Quantized) != 0 {
			return "node_dimension_mismatch"
		}
		if err := validateFloat32Vector(node.Vector); err != nil {
			return "invalid_node_vector"
		}
		if meta.Metric == VectorMetricCosine && vectorNormSquared(node.Vector) == 0 {
			return "invalid_zero_cosine_vector"
		}
	case VectorIndexEncodingInt8:
		if len(node.Quantized) != meta.Dimensions || len(node.Vector) != 0 {
			return "node_dimension_mismatch"
		}
		if node.QuantScale <= 0 || math.IsNaN(float64(node.QuantScale)) || math.IsInf(float64(node.QuantScale), 0) {
			return "invalid_quant_scale"
		}
		if meta.Metric == VectorMetricCosine {
			var norm float32
			for _, value := range node.Quantized {
				scaled := float32(value) * node.QuantScale
				norm += scaled * scaled
			}
			if norm == 0 {
				return "invalid_zero_cosine_vector"
			}
		}
	default:
		return vectorIndexFallbackInvalidEncoding
	}
	return ""
}

func (idx *columnGraphPartitionLiveCarrier) loadPersistSnapshot(snapshot vectorIndexPersistSnapshot) string {
	if snapshot.Meta.Field != idx.field || snapshot.Meta.Metric != idx.metric {
		return vectorIndexFallbackMetaMismatch
	}
	encoding, err := normalizeVectorIndexEncoding(snapshot.Meta.Encoding)
	if err != nil {
		return vectorIndexFallbackInvalidEncoding
	}
	if encoding != idx.encoding {
		return vectorIndexFallbackMetaEncodingMismatch
	}
	if idx.dimensions != 0 && snapshot.Meta.Dimensions != idx.dimensions {
		return vectorIndexFallbackMetaDimensionMismatch
	}
	if snapshot.Meta.Dimensions <= 0 {
		return vectorIndexFallbackInvalidDimensions
	}
	if len(snapshot.Nodes) == 0 {
		if reason := validateVectorIndexPersistShape(snapshot.Meta, 0, len(snapshot.Edges), len(snapshot.Tombstones.NodeIDs), len(snapshot.DocMap.Current)); reason != "" {
			return reason
		}
		return idx.finishVectorIndexPersistLoadWithPartitions(snapshot.Meta, nil, make(map[string]int), snapshot.PartitionLiveOwners, snapshot.PartitionLiveDomains)
	}
	tombstoned := make(map[int]struct{}, len(snapshot.Tombstones.NodeIDs))
	for _, nodeID := range snapshot.Tombstones.NodeIDs {
		if nodeID < 0 || nodeID >= len(snapshot.Nodes) {
			return vectorIndexFallbackInvalidTombstone
		}
		tombstoned[nodeID] = struct{}{}
	}
	if !vectorIndexNodeOrdinalsFitUint32(0, uint64(len(snapshot.Nodes))) {
		return vectorIndexFallbackInvalidEdgeNode
	}
	nodes := make([]vectorIndexNode, len(snapshot.Nodes))
	for i, node := range snapshot.Nodes {
		if reason := validateVectorIndexPersistNode(node, snapshot.Meta); reason != "" {
			return reason
		}
		_, deletedByTombstone := tombstoned[i]
		if node.Deleted != deletedByTombstone {
			return "tombstone_mismatch"
		}
		converted, reason := idx.convertVectorIndexPersistNode(node, snapshot.Meta)
		if reason != "" {
			return reason
		}
		nodes[i] = converted
	}
	for _, edge := range snapshot.Edges {
		if reason := idx.applyVectorIndexPersistEdge(nodes, edge, snapshot.Meta); reason != "" {
			return reason
		}
	}
	current := make(map[string]int, len(snapshot.DocMap.Current))
	for docID, nodeID := range snapshot.DocMap.Current {
		if reason := validateVectorIndexPersistDocMapNode(nodes, docID, nodeID); reason != "" {
			return reason
		}
		current[docID] = nodeID
	}
	return idx.finishVectorIndexPersistLoadWithPartitions(snapshot.Meta, nodes, current, snapshot.PartitionLiveOwners, snapshot.PartitionLiveDomains)
}

func validateVectorIndexPersistShape(meta vectorIndexPersistMeta, nodes, edges, tombs, docs int) string {
	if nodes == 0 {
		if edges != 0 {
			return vectorIndexFallbackInvalidEdgeNode
		}
		if tombs != 0 {
			return vectorIndexFallbackInvalidTombstone
		}
		if docs != 0 {
			return vectorIndexFallbackInvalidDocMapNode
		}
		if meta.Entry >= 0 {
			return vectorIndexFallbackInvalidEntry
		}
	}
	if !vectorIndexNodeOrdinalsFitUint32(0, uint64(nodes)) {
		return vectorIndexFallbackInvalidEdgeNode
	}
	return ""
}

// convertVectorIndexPersistNode and the edge/doc-map helpers are the maintained
// snapshot-loader contract, also used by native family-at-a-time loading.
func (idx *columnGraphPartitionLiveCarrier) convertVectorIndexPersistNode(node vectorIndexPersistNode, meta vectorIndexPersistMeta) (vectorIndexNode, string) {
	if reason := validateVectorIndexPersistNode(node, meta); reason != "" {
		return vectorIndexNode{}, reason
	}
	if node.Level < 0 {
		return vectorIndexNode{}, "invalid_node_level"
	}
	out := vectorIndexNode{
		documentID: []byte(node.DocumentID), vector: append([]float32(nil), node.Vector...),
		quantized: append([]int8(nil), node.Quantized...), quantScale: node.QuantScale,
		level: node.Level, neighbors: make([][]vectorIndexNeighbor, node.Level+1), deleted: node.Deleted,
	}
	out.cacheVectorNorms()
	return out, ""
}

func (idx *columnGraphPartitionLiveCarrier) applyVectorIndexPersistEdge(nodes []vectorIndexNode, edge vectorIndexPersistEdges, meta vectorIndexPersistMeta) string {
	if edge.NodeID < 0 || edge.NodeID >= len(nodes) {
		return vectorIndexFallbackInvalidEdgeNode
	}
	if edge.Layer < 0 || edge.Layer > nodes[edge.NodeID].level {
		return "invalid_edge_layer"
	}
	for _, neighbor := range edge.Neighbor {
		if neighbor < 0 || neighbor >= len(nodes) {
			return "invalid_edge_neighbor"
		}
		if nodes[neighbor].level < edge.Layer {
			return "edge_neighbor_missing_layer"
		}
	}
	if len(edge.Distances) > len(edge.Neighbor) {
		return "invalid_edge_distance_count"
	}
	var neighbors []vectorIndexNeighbor
	neighbors = make([]vectorIndexNeighbor, len(edge.Neighbor))
	for i, neighbor := range edge.Neighbor {
		if neighbor < 0 || neighbor >= len(nodes) {
			return "invalid_edge_neighbor"
		}
		distance := float32(math.Inf(1))
		if i < len(edge.Distances) {
			distance = edge.Distances[i]
		}
		if normalized, ok := normalizeVectorIndexEdgeDistance(distance); ok {
			distance = normalized
		} else {
			var err error
			distance, err = vectorDistanceBetweenStoredNodes(&nodes[edge.NodeID], &nodes[neighbor], meta.Metric)
			if err != nil {
				return "invalid_edge_distance"
			}
			normalized, ok = normalizeVectorIndexEdgeDistance(distance)
			if !ok {
				return "invalid_edge_distance"
			}
			distance = normalized
		}
		neighbors[i] = vectorIndexNeighbor{nodeID: uint32(neighbor), distance: distance}
	}
	nodes[edge.NodeID].neighbors[edge.Layer] = neighbors
	return ""
}

func validateVectorIndexPersistDocMapNode(nodes []vectorIndexNode, docID string, nodeID int) string {
	if nodeID < 0 || nodeID >= len(nodes) {
		return vectorIndexFallbackInvalidDocMapNode
	}
	if nodes[nodeID].deleted {
		return "docmap_points_to_deleted_node"
	}
	if !bytes.Equal(nodes[nodeID].documentID, []byte(docID)) {
		return "docmap_document_mismatch"
	}
	return ""
}

func (idx *columnGraphPartitionLiveCarrier) finishVectorIndexPersistLoad(meta vectorIndexPersistMeta, nodes []vectorIndexNode, current map[string]int) string {
	return idx.finishVectorIndexPersistLoadWithPartitions(meta, nodes, current, nil, nil)
}

func (idx *columnGraphPartitionLiveCarrier) finishVectorIndexPersistLoadWithPartitions(meta vectorIndexPersistMeta, nodes []vectorIndexNode, current map[string]int, owners []vectorIndexPartitionLivePersistOwnerV1, domains []vectorIndexPartitionLivePersistDomainV1) string {
	entry := meta.Entry
	if entry < 0 || entry >= len(nodes) || nodes[entry].deleted {
		entry = -1
		for i := range nodes {
			if !nodes[i].deleted {
				entry = i
				break
			}
		}
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.name = meta.Name
	idx.encoding, _ = normalizeVectorIndexEncoding(meta.Encoding)
	idx.dimensions = meta.Dimensions
	idx.m = meta.M
	idx.efConstruction = meta.EfConstruction
	idx.efSearch = meta.EfSearch
	idx.rebuildDeletedRatio = meta.RebuildDeletedRatio
	idx.nodes = nodes
	idx.vectorRows = nil
	idx.currentNode = current
	idx.entry = entry
	idx.maxLevel = idx.maxLiveLevelLocked()
	idx.persistedEpoch = 0
	idx.fullSnapshotBaseEpoch = 0
	idx.persistedBytesDisk = 0
	idx.persistedSnapshotDirty = false
	idx.lastRebuildDuration = 0
	idx.mutationSeq = 0
	idx.sourceDocumentGeneration = meta.SourceDocumentGeneration
	idx.sourceDocumentRootsValid = true
	partitionLive, reason := idx.restorePartitionLiveSnapshotV2(meta.PartitionLive, owners, domains, meta.SourceDocumentGeneration)
	if reason != "" {
		return reason
	}
	idx.partitionLive = partitionLive
	idx.partitionPreparation = meta.Preparation
	idx.publishSearchViewLocked(true)
	return ""
}
