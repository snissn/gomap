package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const (
	vectorIndexFormatVersion             = 1
	vectorIndexDocumentGenerationVersion = 3
	vectorIndexDirName                   = "vector_indexes"
	vectorIndexManifestFile              = "manifest.json"
	vectorIndexMetaFile                  = "meta.json"
	vectorIndexNodesFile                 = "nodes.json"
	vectorIndexEdgesFile                 = "edges.json"
	vectorIndexTombstonesFile            = "tombstones.json"
	vectorIndexDocMapFile                = "docmap.json"

	vectorIndexNativeKeyMeta           = "meta"
	vectorIndexNativeKeyPrefixNode     = "node/"
	vectorIndexNativeKeyPrefixEdge     = "edge/"
	vectorIndexNativeKeyPrefixTomb     = "tomb/"
	vectorIndexNativeKeyPrefixDoc      = "doc/"
	vectorIndexNativeKeyOrdinalWidth   = 20
	vectorIndexNativeKeyEdgeLayerWidth = 3
)

var (
	errVectorIndexNotDeclared             = errors.New("collections: vector index is not declared in collection metadata")
	errVectorIndexStaleNativeRoot         = errors.New("collections: vector index native root changed since load")
	errVectorIndexStaleDocumentGeneration = errors.New("collections: vector index document generation changed since load")
)

// VectorIndexLoadStatus reports whether a persisted vector index loaded or why
// callers should use exact search as the safe fallback.
type VectorIndexLoadStatus struct {
	Loaded              bool
	ExactFallbackReason string
	ManifestPath        string
	RootName            string
	RootID              uint64
	Epoch               uint64
	BytesDisk           int64
}

// VectorIndexPruneStatus reports persisted vector-index epoch cleanup.
type VectorIndexPruneStatus struct {
	IndexDir      string
	ActiveEpoch   string
	RemovedEpochs int
	RemovedBytes  int64
}

// SaveSnapshot persists the current in-memory vector index. Declared collection
// vector indexes use a native TreeDB collection root; ad hoc in-memory indexes
// keep using the legacy sidecar snapshot path.
func (idx *VectorIndex) SaveSnapshot() (VectorIndexLoadStatus, error) {
	status, err := idx.SaveNativeSnapshot()
	if err == nil || !errors.Is(err, errVectorIndexNotDeclared) {
		return status, err
	}
	return idx.saveLegacySnapshot()
}

// SaveNativeSnapshot persists the current in-memory vector index as ordinary
// TreeDB collection-root content and publishes that root through the collection
// root descriptor system state.
func (idx *VectorIndex) SaveNativeSnapshot() (VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
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

func (idx *VectorIndex) saveNativeSnapshotWithCoverageLocked() (VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
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
	return idx.saveNativeSnapshotPrepared()
}

func staleNativeSnapshotSaveStatus(c *Collection, idx *VectorIndex) (VectorIndexLoadStatus, bool, error) {
	status := VectorIndexLoadStatus{}
	if c == nil || idx == nil {
		return status, false, nil
	}
	if idx.isNativePersistent() && !idx.hasValidSourceDocumentRoots() {
		status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
		return status, false, fmt.Errorf("%w: index %q does not cover current documents", errVectorIndexStaleNativeRoot, idx.name)
	}
	if c.isRegisteredVectorIndex(idx) {
		stale, err := c.registeredVectorIndexNativeRuntimeIsStale(idx)
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
func (idx *VectorIndex) saveNativeSnapshotPrepared() (VectorIndexLoadStatus, error) {
	return idx.saveNativeSnapshotPreparedWithCommandWALIntent(nil)
}

func (idx *VectorIndex) saveNativeSnapshotPreparedWithCommandWALIntent(replay *backenddb.CommandWALIntent) (VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
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
	if !ok || !vectorIndexDefinitionUsesNativeRuntime(def) {
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

	if err := idx.foldLiveDeltaForPersistence(); err != nil {
		return status, err
	}
	snapshot, snapshotSeq := idx.persistSnapshot()
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
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(idx.name, replay)
	if err != nil {
		resetCollectionRunTable(table)
		return status, err
	}
	iter := publishTable.NewIterator(nil, nil)
	publicationMu := idx.nativePublicationLock()
	publicationMu.Lock()
	newSystemRoot, rootIDs, err := c.publishRootDeltaGroupWithoutColumn([]backenddb.OrderedRootDeltaPublishInput{{
		// Native vector snapshots are full graph images. Publish a replacement
		// root so keys removed by rebuild/shrink do not survive from prior roots.
		BaseRoot:      0,
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
		c.RegisterVectorIndex(idx)
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

func (c *Collection) installNativeVectorIndexCandidate(candidate *VectorIndex, expectedRoot uint64, replaceCurrent *VectorIndex, replaceMutationSeq uint64) (*VectorIndex, error) {
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
	if !ok || !vectorIndexDefinitionUsesNativeRuntime(def) {
		return nil, fmt.Errorf("%w: %q", errVectorIndexNotDeclared, candidate.name)
	}
	activeRoot := catalog.rootID(collectionVectorIndexRootName(catalog.meta.Name, def.Name))
	documentGeneration, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return nil, err
	}
	current := c.registeredVectorIndex(def.Name)
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
	c.registerVectorIndexCurrentCatalog(candidate)
	rollback := func() {
		if c.registeredVectorIndex(def.Name) != candidate {
			return
		}
		if current != nil {
			c.registerVectorIndexCurrentCatalog(current)
		} else {
			c.UnregisterVectorIndex(def.Name)
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
	if !ok || !vectorIndexDefinitionUsesNativeRuntime(postDef) || candidate.validateNativeSnapshotDefinition(postDef) != "" || postRoot != expectedRoot {
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
	return candidate, nil
}

// SaveNativeDeltaSnapshot persists dirty graph records for a declared vector
// index as a collection-root delta. It is used by live write maintenance; full
// rebuild/shrink publication should continue to use SaveNativeSnapshot so
// removed graph keys cannot survive.
func (idx *VectorIndex) SaveNativeDeltaSnapshot() (VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
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
	if idx.needsNativeFullSnapshotAutoPersist() {
		return idx.saveNativeSnapshotWithCoverageLocked()
	}
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
	if !ok || !vectorIndexDefinitionUsesNativeRuntime(def) {
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

	if err := idx.foldLiveDeltaForPersistence(); err != nil {
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
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(idx.name, nil)
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

func (idx *VectorIndex) saveLegacySnapshot() (VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	if idx.collection == nil || idx.collection.db == nil {
		return status, errCollectionDBNil
	}
	unlockSidecar := idx.collection.lockLegacyVectorSidecar()
	defer unlockSidecar()
	indexDir, err := idx.persistDir()
	if err != nil {
		return status, err
	}
	status.ManifestPath = filepath.Join(indexDir, vectorIndexManifestFile)
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return status, err
	}
	epoch := uint64(time.Now().UnixNano())
	epochName := fmt.Sprintf("epoch-%020d", epoch)
	epochDir := filepath.Join(indexDir, epochName)
	tmpDir := filepath.Join(indexDir, ".tmp-"+epochName)
	if err := os.RemoveAll(tmpDir); err != nil {
		return status, err
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return status, err
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	snapshot, snapshotSeq := idx.persistSnapshot()
	files := map[string]any{
		vectorIndexMetaFile:       snapshot.Meta,
		vectorIndexNodesFile:      snapshot.Nodes,
		vectorIndexEdgesFile:      snapshot.Edges,
		vectorIndexTombstonesFile: snapshot.Tombstones,
		vectorIndexDocMapFile:     snapshot.DocMap,
	}
	fileEntries := make([]vectorIndexManifestFileEntry, 0, len(files))
	for name, payload := range files {
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return status, err
		}
		data = append(data, '\n')
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return status, err
		}
		if err := fsyncFile(path); err != nil {
			return status, err
		}
		sum := sha256.Sum256(data)
		fileEntries = append(fileEntries, vectorIndexManifestFileEntry{
			Name:   name,
			Size:   int64(len(data)),
			SHA256: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(fileEntries, func(i, j int) bool {
		return fileEntries[i].Name < fileEntries[j].Name
	})
	if err := fsyncDir(tmpDir); err != nil {
		return status, err
	}
	if err := os.Rename(tmpDir, epochDir); err != nil {
		return status, err
	}
	cleanupTmp = false
	if err := fsyncDir(indexDir); err != nil {
		return status, err
	}
	runLegacyVectorSnapshotPostEpochRenameHookForTest()

	manifest := vectorIndexManifest{
		FormatVersion:  vectorIndexFormatVersion,
		Collection:     idx.collection.meta.Name,
		IndexName:      idx.name,
		Epoch:          epoch,
		EpochDir:       epochName,
		Dims:           snapshot.Meta.Dimensions,
		Metric:         idx.metric,
		Encoding:       snapshot.Meta.Encoding,
		M:              idx.m,
		EfConstruction: idx.efConstruction,
		EfSearch:       idx.efSearch,
		MaxLevel:       snapshot.Meta.MaxLevel,
		NodeCount:      len(snapshot.Nodes),
		LiveDocCount:   len(snapshot.DocMap.Current),
		DeletedCount:   len(snapshot.Tombstones.NodeIDs),
		CreatedAtUnix:  time.Now().Unix(),
		Files:          fileEntries,
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return status, err
	}
	manifestData = append(manifestData, '\n')
	bytesDisk := vectorIndexSnapshotBytes(manifestData, fileEntries)
	tmpManifest := filepath.Join(indexDir, ".manifest.tmp")
	if err := os.WriteFile(tmpManifest, manifestData, 0o644); err != nil {
		return status, err
	}
	if err := fsyncFile(tmpManifest); err != nil {
		return status, err
	}
	if err := os.Rename(tmpManifest, status.ManifestPath); err != nil {
		return status, err
	}
	if err := fsyncDir(indexDir); err != nil {
		return status, err
	}
	status.Loaded = true
	status.Epoch = epoch
	status.BytesDisk = bytesDisk
	idx.recordPersistedSnapshot(epoch, bytesDisk, snapshotSeq)
	return status, nil
}

// LoadVectorIndexSnapshot loads the currently published persisted vector index
// epoch. Missing, incomplete, or corrupt snapshots return a non-loaded status
// with ExactFallbackReason set and no error, so callers can safely use exact
// search as the correctness fallback.
func (c *Collection) LoadVectorIndexSnapshot(opts VectorIndexOptions) (*VectorIndex, VectorIndexLoadStatus, error) {
	index, status, err := c.LoadNativeVectorIndexSnapshot(opts)
	if err != nil {
		return nil, status, err
	}
	if index != nil || status.Loaded || status.ExactFallbackReason != vectorIndexFallbackMissingVectorIndexMetadata {
		return index, status, nil
	}
	return c.loadLegacyVectorIndexSnapshot(opts)
}

// LoadNativeVectorIndexSnapshot loads a declared vector index from its TreeDB
// collection root. Missing or invalid graph roots return a non-loaded status so
// callers can safely fall back to exact search.
func (c *Collection) LoadNativeVectorIndexSnapshot(opts VectorIndexOptions) (*VectorIndex, VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
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
	matchesDocumentRoots, err := vectorIndexSnapshotMatchesDocumentRoots(snapshot.Meta, catalog, snap)
	if err != nil {
		return nil, status, err
	}
	if !matchesDocumentRoots {
		status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
		return nil, status, nil
	}
	index, err := newVectorIndex(c, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return nil, status, err
	}
	if reason := index.loadPersistSnapshot(snapshot); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	if index.validateNativeSnapshotDefinition(def) != "" {
		status.ExactFallbackReason = vectorIndexFallbackMetaMismatch
		return nil, status, nil
	}
	if err := populateNativeScalarColumnsFromSecondaryIndexes(index, snap, catalog); err != nil {
		return nil, status, fmt.Errorf("%w: native scalar columns: %v", ErrVectorIndexSearchUnavailable, err)
	}
	status.RootID = rootID
	index.recordLoadedSnapshot(rootID, bytesDisk)
	installed, err := c.installNativeVectorIndexCandidate(index, rootID, nil, 0)
	if err != nil {
		if errors.Is(err, errVectorIndexStaleDocumentGeneration) {
			status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
			return nil, status, nil
		}
		status.ExactFallbackReason = vectorIndexFallbackStaleRuntimeIndex
		return nil, status, err
	}
	status.Loaded = true
	status.Epoch, status.BytesDisk, _, _, _ = installed.nativeSearchState()
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

func (c *Collection) loadLegacyVectorIndexSnapshot(opts VectorIndexOptions) (*VectorIndex, VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
	if c == nil {
		return nil, status, errCollectionNil
	}
	if c.db == nil {
		return nil, status, errCollectionDBNil
	}
	unlockSidecar := c.lockLegacyVectorSidecar()
	defer unlockSidecar()
	if opts.Name == "" {
		opts.Name = vectorIndexDefaultName(opts.Field)
	}
	indexDir, err := vectorIndexPersistDir(c.db.Dir(), c.meta.Name, opts.Name)
	if err != nil {
		return nil, status, err
	}
	status.ManifestPath = filepath.Join(indexDir, vectorIndexManifestFile)
	index, err := newVectorIndex(c, opts)
	if err != nil {
		return nil, status, err
	}
	manifestData, err := os.ReadFile(status.ManifestPath)
	if errors.Is(err, os.ErrNotExist) {
		status.ExactFallbackReason = vectorIndexFallbackMissingManifest
		return nil, status, nil
	}
	if err != nil {
		return nil, status, err
	}
	var manifest vectorIndexManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		status.ExactFallbackReason = vectorIndexFallbackInvalidManifest
		return nil, status, nil
	}
	if reason := validateVectorIndexManifest(manifest, c.meta.Name, index.name, index.metric, index.encoding, index.dimensions); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	epochDir := filepath.Join(indexDir, manifest.EpochDir)
	files, reason, err := readVectorIndexSnapshotFiles(epochDir, manifest.Files)
	if err != nil {
		return nil, status, err
	}
	if reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	if reason := validateVectorIndexSnapshotManifestCounts(manifest, files); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	if reason := index.loadPersistSnapshot(files); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	status.Loaded = true
	status.Epoch = manifest.Epoch
	status.BytesDisk = vectorIndexSnapshotBytes(manifestData, manifest.Files)
	index.recordLoadedSnapshot(status.Epoch, status.BytesDisk)
	c.RegisterVectorIndex(index)
	return index, status, nil
}

func (idx *VectorIndex) persistDir() (string, error) {
	if idx == nil || idx.collection == nil || idx.collection.db == nil {
		return "", errCollectionDBNil
	}
	return vectorIndexPersistDir(idx.collection.db.Dir(), idx.collection.meta.Name, idx.name)
}

// PruneOldSnapshots removes older immutable epoch directories for this vector
// index while preserving the currently published manifest epoch and the newest
// keep-1 additional epochs. It never removes temp directories.
func (idx *VectorIndex) PruneOldSnapshots(keep int) (VectorIndexPruneStatus, error) {
	status := VectorIndexPruneStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	if keep <= 0 {
		return status, errors.New("collections: vector index snapshot keep count must be positive")
	}
	unlockSidecar := idx.collection.lockLegacyVectorSidecar()
	defer unlockSidecar()
	indexDir, err := idx.persistDir()
	if err != nil {
		return status, err
	}
	status.IndexDir = indexDir
	manifestPath := filepath.Join(indexDir, vectorIndexManifestFile)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return status, err
	}
	var manifest vectorIndexManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return status, err
	}
	if manifest.EpochDir == "" || strings.ContainsAny(manifest.EpochDir, `/\`) {
		return status, errors.New("collections: invalid vector index manifest epoch dir")
	}
	status.ActiveEpoch = manifest.EpochDir

	epochs, err := vectorIndexEpochDirs(indexDir)
	if err != nil {
		return status, err
	}
	preserve := map[string]struct{}{manifest.EpochDir: {}}
	for i := len(epochs) - 1; i >= 0 && len(preserve) < keep; i-- {
		preserve[epochs[i]] = struct{}{}
	}
	for _, epoch := range epochs {
		if _, ok := preserve[epoch]; ok {
			continue
		}
		path := filepath.Join(indexDir, epoch)
		bytes, err := dirSize(path)
		if err != nil {
			return status, err
		}
		if err := os.RemoveAll(path); err != nil {
			return status, err
		}
		status.RemovedEpochs++
		status.RemovedBytes += bytes
	}
	if status.RemovedEpochs > 0 {
		if err := fsyncDir(indexDir); err != nil {
			return status, err
		}
	}
	return status, nil
}

func vectorIndexEpochDirs(indexDir string) ([]string, error) {
	entries, err := os.ReadDir(indexDir)
	if err != nil {
		return nil, err
	}
	var epochs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "epoch-") {
			epochs = append(epochs, name)
		}
	}
	sort.Strings(epochs)
	return epochs, nil
}

func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func vectorIndexPersistDir(dbDir, collection, indexName string) (string, error) {
	if dbDir == "" {
		return "", errors.New("collections: vector index persistence requires a database directory")
	}
	collectionComponent, err := vectorIndexSafePathComponent(collection)
	if err != nil {
		return "", err
	}
	indexComponent, err := vectorIndexSafePathComponent(indexName)
	if err != nil {
		return "", err
	}
	return filepath.Join(dbDir, vectorIndexDirName, collectionComponent, indexComponent), nil
}

func vectorIndexDefaultName(field string) string {
	if field == "" {
		return "default"
	}
	return field
}

func vectorIndexSafePathComponent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, `/\`) {
		return "", fmt.Errorf("collections: invalid vector index path component %q", s)
	}
	return s, nil
}

type vectorIndexManifest struct {
	FormatVersion  int                            `json:"format_version"`
	Collection     string                         `json:"collection"`
	IndexName      string                         `json:"index_name"`
	Epoch          uint64                         `json:"epoch"`
	EpochDir       string                         `json:"epoch_dir"`
	Dims           int                            `json:"dims"`
	Metric         VectorMetric                   `json:"metric"`
	Encoding       VectorIndexEncoding            `json:"encoding"`
	M              int                            `json:"m"`
	EfConstruction int                            `json:"ef_construction"`
	EfSearch       int                            `json:"ef_search"`
	MaxLevel       int                            `json:"max_level"`
	NodeCount      int                            `json:"node_count"`
	LiveDocCount   int                            `json:"live_doc_count"`
	DeletedCount   int                            `json:"deleted_doc_count"`
	CreatedAtUnix  int64                          `json:"created_at_unix"`
	Files          []vectorIndexManifestFileEntry `json:"files"`
}

type vectorIndexManifestFileEntry struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type vectorIndexPersistSnapshot struct {
	Meta       vectorIndexPersistMeta
	Nodes      []vectorIndexPersistNode
	Edges      []vectorIndexPersistEdges
	Tombstones vectorIndexPersistTombstones
	DocMap     vectorIndexPersistDocMap
}

type vectorIndexPersistMeta struct {
	Name                            string              `json:"name"`
	Field                           string              `json:"field"`
	Metric                          VectorMetric        `json:"metric"`
	Encoding                        VectorIndexEncoding `json:"encoding"`
	Dimensions                      int                 `json:"dimensions"`
	M                               int                 `json:"m"`
	EfConstruction                  int                 `json:"ef_construction"`
	EfSearch                        int                 `json:"ef_search"`
	RebuildDeletedRatio             float64             `json:"rebuild_deleted_ratio"`
	Entry                           int                 `json:"entry"`
	MaxLevel                        int                 `json:"max_level"`
	SourceDocumentGenerationVersion int                 `json:"source_document_generation_version"`
	SourceDocumentGeneration        uint64              `json:"source_document_generation"`
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

func (idx *VectorIndex) persistSnapshot() (vectorIndexPersistSnapshot, uint64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	seq := idx.mutationSeq
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
		},
		Nodes: make([]vectorIndexPersistNode, len(idx.nodes)),
		DocMap: vectorIndexPersistDocMap{
			Current: make(map[string]int, len(idx.currentNode)),
		},
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
				neighborIDs[j] = neighbor.nodeID
				distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
				if !ok {
					distance = idx.distanceBetweenNodesLocked(i, neighbor.nodeID)
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

func vectorIndexSnapshotBytes(manifestData []byte, entries []vectorIndexManifestFileEntry) int64 {
	total := int64(len(manifestData))
	for _, entry := range entries {
		total += entry.Size
	}
	return total
}

func buildVectorIndexNativeSnapshotTable(snapshot vectorIndexPersistSnapshot) (memtable.Table, int64, error) {
	entryCount := 1 + len(snapshot.Nodes) + len(snapshot.Edges) + len(snapshot.Tombstones.NodeIDs) + len(snapshot.DocMap.Current)
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
	return table, bytesDisk, nil
}

func (idx *VectorIndex) persistNativeDeltaTable(includeMeta bool) (memtable.Table, int64, uint64, uint64, bool, error) {
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
	for _, nodeID := range nodeIDs {
		node := idx.nodes[nodeID]
		if err := add(vectorIndexNativeNodeKey(nodeID), vectorIndexPersistNodeFromRuntime(node)); err != nil {
			resetCollectionRunTable(table)
			return nil, 0, seq, persistedEpoch, false, err
		}
		for layer, neighbors := range node.neighbors {
			edge := vectorIndexPersistEdges{
				NodeID:    nodeID,
				Layer:     layer,
				Neighbor:  make([]int, len(neighbors)),
				Distances: make([]float32, len(neighbors)),
			}
			for i, neighbor := range neighbors {
				edge.Neighbor[i] = neighbor.nodeID
				distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
				if !ok {
					distance = idx.distanceBetweenNodesLocked(nodeID, neighbor.nodeID)
					distance, ok = normalizeVectorIndexEdgeDistance(distance)
					if !ok {
						distance = math.MaxFloat32
					}
				}
				edge.Distances[i] = distance
			}
			if err := add(vectorIndexNativeEdgeKey(nodeID, layer), edge); err != nil {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, err
			}
		}
		if node.deleted {
			if err := add(vectorIndexNativeTombstoneKey(nodeID), nodeID); err != nil {
				resetCollectionRunTable(table)
				return nil, 0, seq, persistedEpoch, false, err
			}
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
	return table, bytesDisk, seq, persistedEpoch, true, nil
}

func (idx *VectorIndex) persistMetaLocked() vectorIndexPersistMeta {
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
	}
}

func vectorIndexPersistNodeFromRuntime(node vectorIndexNode) vectorIndexPersistNode {
	return vectorIndexPersistNode{
		DocumentID: string(node.documentID),
		Vector:     append([]float32(nil), node.vector...),
		Quantized:  append([]int8(nil), node.quantized...),
		QuantScale: node.quantScale,
		Level:      node.level,
		Deleted:    node.deleted,
	}
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
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixNode)):
			nodeID, ok := parseVectorIndexNativeOrdinal(string(key[len(vectorIndexNativeKeyPrefixNode):]))
			if !ok {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			var node vectorIndexPersistNode
			if err := json.Unmarshal(value, &node); err != nil {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootEntry, nil
			}
			nodes[nodeID] = node
			if nodeID > maxNodeID {
				maxNodeID = nodeID
			}
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixEdge)):
			var edge vectorIndexPersistEdges
			if err := json.Unmarshal(value, &edge); err != nil {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootEntry, nil
			}
			snapshot.Edges = append(snapshot.Edges, edge)
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixTomb)):
			nodeID, ok := parseVectorIndexNativeOrdinal(string(key[len(vectorIndexNativeKeyPrefixTomb):]))
			if !ok {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
			}
			snapshot.Tombstones.NodeIDs = append(snapshot.Tombstones.NodeIDs, nodeID)
		case bytes.HasPrefix(key, []byte(vectorIndexNativeKeyPrefixDoc)):
			var nodeID int
			if err := json.Unmarshal(value, &nodeID); err != nil {
				return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootEntry, nil
			}
			snapshot.DocMap.Current[string(key[len(vectorIndexNativeKeyPrefixDoc):])] = nodeID
		default:
			return snapshot, bytesDisk, vectorIndexFallbackInvalidGraphRootKey, nil
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return snapshot, bytesDisk, "", err
	}
	if maxNodeID >= 0 {
		snapshot.Nodes = make([]vectorIndexPersistNode, maxNodeID+1)
		for nodeID := 0; nodeID <= maxNodeID; nodeID++ {
			node, ok := nodes[nodeID]
			if !ok {
				return snapshot, bytesDisk, vectorIndexFallbackMissingGraphRootEntry, nil
			}
			snapshot.Nodes[nodeID] = node
		}
	}
	sort.Ints(snapshot.Tombstones.NodeIDs)
	return snapshot, bytesDisk, "", nil
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

func vectorIndexOptionName(opts VectorIndexOptions) string {
	if opts.Name != "" {
		return opts.Name
	}
	return vectorIndexDefaultName(opts.Field)
}

func vectorIndexOptionsFromDefinition(def VectorIndexDefinition) VectorIndexOptions {
	return VectorIndexOptions{
		Name:             def.Name,
		Field:            def.Field,
		Metric:           def.Metric,
		Dimensions:       def.Dimensions,
		M:                def.M,
		EfConstruction:   def.EfConstruction,
		EfSearch:         def.EfSearch,
		Encoding:         def.Encoding,
		schemaGeneration: def.SchemaGeneration,
		nativeRuntime:    vectorIndexDefinitionUsesNativeRuntime(def),
	}
}

func (idx *VectorIndex) validateNativeSnapshotDefinition(def VectorIndexDefinition) string {
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

func (idx *VectorIndex) recordPersistedSnapshot(epoch uint64, bytesDisk int64, snapshotSeq uint64) {
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
	}
	if view := idx.searchView.Load(); idx.nativePersistent && view != nil {
		view.persisted.Store(&vectorIndexSearchPersistedMetadata{epoch: epoch, bytesDisk: bytesDisk})
	}
}

func (idx *VectorIndex) recordLoadedSnapshot(epoch uint64, bytesDisk int64) {
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

func (idx *VectorIndex) needsNativeAutoPersist() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.dirtyMeta || (idx.mutationSeq != 0 && (idx.persistedEpoch == 0 || idx.persistedSnapshotDirty))
}

func (idx *VectorIndex) needsNativeFullSnapshotAutoPersist() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.fullSnapshotBaseEpoch != 0 && idx.mutationSeq != 0
}

func validateVectorIndexManifest(manifest vectorIndexManifest, collection, indexName string, metric VectorMetric, encoding VectorIndexEncoding, dimensions int) string {
	if manifest.FormatVersion != vectorIndexFormatVersion {
		return "unsupported_format_version"
	}
	if manifest.Collection != collection || manifest.IndexName != indexName {
		return "manifest_scope_mismatch"
	}
	if manifest.Metric != metric {
		return "manifest_metric_mismatch"
	}
	if manifest.Encoding != encoding {
		return "manifest_encoding_mismatch"
	}
	if dimensions != 0 && manifest.Dims != dimensions {
		return "manifest_dimension_mismatch"
	}
	if manifest.EpochDir == "" || strings.ContainsAny(manifest.EpochDir, `/\`) {
		return "invalid_epoch_dir"
	}
	if manifest.M <= 0 || manifest.EfConstruction <= 0 || manifest.EfSearch <= 0 {
		return "invalid_manifest_hnsw_params"
	}
	if manifest.NodeCount < 0 || manifest.LiveDocCount < 0 || manifest.DeletedCount < 0 {
		return "invalid_manifest_counts"
	}
	if len(manifest.Files) == 0 {
		return "manifest_missing_files"
	}
	return ""
}

func validateVectorIndexSnapshotManifestCounts(manifest vectorIndexManifest, snapshot vectorIndexPersistSnapshot) string {
	if manifest.NodeCount != len(snapshot.Nodes) {
		return "manifest_node_count_mismatch"
	}
	if manifest.LiveDocCount != len(snapshot.DocMap.Current) {
		return "manifest_live_count_mismatch"
	}
	if manifest.DeletedCount != len(snapshot.Tombstones.NodeIDs) {
		return "manifest_deleted_count_mismatch"
	}
	if snapshot.Meta.EfConstruction != manifest.EfConstruction || snapshot.Meta.EfSearch != manifest.EfSearch || snapshot.Meta.M != manifest.M {
		return "manifest_meta_param_mismatch"
	}
	if snapshot.Meta.Encoding != manifest.Encoding {
		return "manifest_meta_encoding_mismatch"
	}
	if snapshot.Meta.MaxLevel != manifest.MaxLevel {
		return "manifest_max_level_mismatch"
	}
	return ""
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

func readVectorIndexSnapshotFiles(epochDir string, entries []vectorIndexManifestFileEntry) (vectorIndexPersistSnapshot, string, error) {
	var snapshot vectorIndexPersistSnapshot
	for _, entry := range entries {
		if entry.Name == "" || strings.ContainsAny(entry.Name, `/\`) {
			return snapshot, "invalid_file_name", nil
		}
		data, err := os.ReadFile(filepath.Join(epochDir, entry.Name))
		if errors.Is(err, os.ErrNotExist) {
			return snapshot, "missing_epoch_file", nil
		}
		if err != nil {
			return snapshot, "", err
		}
		if int64(len(data)) != entry.Size {
			return snapshot, "file_size_mismatch", nil
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != entry.SHA256 {
			return snapshot, "file_checksum_mismatch", nil
		}
		switch entry.Name {
		case vectorIndexMetaFile:
			if err := json.Unmarshal(data, &snapshot.Meta); err != nil {
				return snapshot, "invalid_meta_file", nil
			}
		case vectorIndexNodesFile:
			if err := json.Unmarshal(data, &snapshot.Nodes); err != nil {
				return snapshot, "invalid_nodes_file", nil
			}
		case vectorIndexEdgesFile:
			if err := json.Unmarshal(data, &snapshot.Edges); err != nil {
				return snapshot, "invalid_edges_file", nil
			}
		case vectorIndexTombstonesFile:
			if err := json.Unmarshal(data, &snapshot.Tombstones); err != nil {
				return snapshot, "invalid_tombstones_file", nil
			}
		case vectorIndexDocMapFile:
			if err := json.Unmarshal(data, &snapshot.DocMap); err != nil {
				return snapshot, "invalid_docmap_file", nil
			}
		}
	}
	return snapshot, "", nil
}

func (idx *VectorIndex) loadPersistSnapshot(snapshot vectorIndexPersistSnapshot) string {
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
		if len(snapshot.Edges) != 0 {
			return vectorIndexFallbackInvalidEdgeNode
		}
		if len(snapshot.Tombstones.NodeIDs) != 0 {
			return vectorIndexFallbackInvalidTombstone
		}
		if len(snapshot.DocMap.Current) != 0 {
			return vectorIndexFallbackInvalidDocMapNode
		}
		if snapshot.Meta.Entry >= 0 {
			return vectorIndexFallbackInvalidEntry
		}
		idx.mu.Lock()
		defer idx.mu.Unlock()
		idx.name = snapshot.Meta.Name
		idx.encoding = encoding
		idx.dimensions = snapshot.Meta.Dimensions
		idx.m = snapshot.Meta.M
		idx.efConstruction = snapshot.Meta.EfConstruction
		idx.efSearch = snapshot.Meta.EfSearch
		idx.rebuildDeletedRatio = snapshot.Meta.RebuildDeletedRatio
		idx.nodes = nil
		idx.currentNode = make(map[string]int)
		idx.entry = -1
		idx.maxLevel = -1
		idx.persistedEpoch = 0
		idx.fullSnapshotBaseEpoch = 0
		idx.persistedBytesDisk = 0
		idx.persistedSnapshotDirty = false
		idx.lastRebuildDuration = 0
		idx.mutationSeq = 0
		idx.sourceDocumentGeneration = snapshot.Meta.SourceDocumentGeneration
		idx.sourceDocumentRootsValid = true
		idx.publishSearchViewLocked(true)
		return ""
	}
	tombstoned := make(map[int]struct{}, len(snapshot.Tombstones.NodeIDs))
	for _, nodeID := range snapshot.Tombstones.NodeIDs {
		if nodeID < 0 || nodeID >= len(snapshot.Nodes) {
			return vectorIndexFallbackInvalidTombstone
		}
		tombstoned[nodeID] = struct{}{}
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
		if node.Level < 0 {
			return "invalid_node_level"
		}
		nodes[i] = vectorIndexNode{
			documentID: []byte(node.DocumentID),
			vector:     append([]float32(nil), node.Vector...),
			quantized:  append([]int8(nil), node.Quantized...),
			quantScale: node.QuantScale,
			level:      node.Level,
			neighbors:  make([][]vectorIndexNeighbor, node.Level+1),
			deleted:    node.Deleted,
		}
		nodes[i].cacheVectorNorms()
	}
	for _, edge := range snapshot.Edges {
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
		neighbors := make([]vectorIndexNeighbor, len(edge.Neighbor))
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
				distance, err = vectorDistanceBetweenStoredNodes(&nodes[edge.NodeID], &nodes[neighbor], snapshot.Meta.Metric)
				if err != nil {
					return "invalid_edge_distance"
				}
				normalized, ok = normalizeVectorIndexEdgeDistance(distance)
				if !ok {
					return "invalid_edge_distance"
				}
				distance = normalized
			}
			neighbors[i] = vectorIndexNeighbor{nodeID: neighbor, distance: distance}
		}
		nodes[edge.NodeID].neighbors[edge.Layer] = neighbors
	}
	current := make(map[string]int, len(snapshot.DocMap.Current))
	for docID, nodeID := range snapshot.DocMap.Current {
		if nodeID < 0 || nodeID >= len(nodes) {
			return vectorIndexFallbackInvalidDocMapNode
		}
		if nodes[nodeID].deleted {
			return "docmap_points_to_deleted_node"
		}
		if !bytes.Equal(nodes[nodeID].documentID, []byte(docID)) {
			return "docmap_document_mismatch"
		}
		current[docID] = nodeID
	}
	entry := snapshot.Meta.Entry
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
	idx.name = snapshot.Meta.Name
	idx.encoding = encoding
	idx.dimensions = snapshot.Meta.Dimensions
	idx.m = snapshot.Meta.M
	idx.efConstruction = snapshot.Meta.EfConstruction
	idx.efSearch = snapshot.Meta.EfSearch
	idx.rebuildDeletedRatio = snapshot.Meta.RebuildDeletedRatio
	idx.nodes = nodes
	idx.currentNode = current
	idx.entry = entry
	idx.maxLevel = idx.maxLiveLevelLocked()
	idx.persistedEpoch = 0
	idx.fullSnapshotBaseEpoch = 0
	idx.persistedBytesDisk = 0
	idx.persistedSnapshotDirty = false
	idx.lastRebuildDuration = 0
	idx.mutationSeq = 0
	idx.sourceDocumentGeneration = snapshot.Meta.SourceDocumentGeneration
	idx.sourceDocumentRootsValid = true
	idx.publishSearchViewLocked(true)
	return ""
}

func fsyncFile(path string) error {
	flag := os.O_RDONLY
	if runtime.GOOS == "windows" {
		flag = os.O_RDWR
	}
	f, err := os.OpenFile(path, flag, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

func fsyncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
