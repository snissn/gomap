package collections

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

func (c *Collection) nativeMutateIndexes(create *IndexDefinition, remove map[string]struct{}, all bool) (*CollectionMeta, error) {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	return c.nativeJSON.mutateIndexFamilies(c, c.name, create, remove, all, nil, "")
}

func finishNativeHNSWSchemaNoop(bool) {}

// Preparation and installation share the existing DDL/mutation owner. Queries
// retain their original graph until every replacement is admitted. No canonical
// row rebuild or live-delta fold changes the caller's unsaved graph semantics.
func (c *Collection) prepareNativeHNSWSchemaChange(read *NativeJSONRead, meta CollectionMeta, admit func(uint64) error) (func(bool), error) {
	if c == nil || !nativeOrdinaryHNSWMeta(read.meta) {
		return finishNativeHNSWSchemaNoop, nil
	}
	coord := c.collectionSchemaCoordinator()
	coord.nativeHNSWIndexesMu.RLock()
	count := len(coord.nativeHNSWIndexes)
	coord.nativeHNSWIndexesMu.RUnlock()
	if count == 0 {
		return finishNativeHNSWSchemaNoop, nil
	}
	type replacement struct{ receiver, candidate *VectorIndex }
	// List, cleanup closure and captured owner state precede construction.
	if err := admit(uint64(count)*uint64(unsafe.Sizeof(replacement{})) + 128); err != nil {
		return nil, err
	}
	prepared := make([]replacement, 0, count)
	finished := false
	finish := func(install bool) {
		if finished {
			return
		}
		finished = true
		for _, p := range prepared {
			if install {
				p.receiver.swapNativeHNSW(p.candidate)
			}
			p.candidate.releaseNativeHNSW()
			p.receiver.nativeHNSWUseMu.Unlock()
		}
	}
	// The mutation gate excludes map writers. Never acquire use/admission under
	// the registry lock. Removed or changed declarations retain their own lifecycle.
	coord.nativeHNSWIndexesMu.RLock()
	for _, receiver := range coord.nativeHNSWIndexes {
		before, oldOK := findVectorIndex(read.meta.VectorIndexes, receiver.name)
		after, newOK := findVectorIndex(meta.VectorIndexes, receiver.name)
		if oldOK && newOK && vectorIndexDefinitionValuesEqual(before, after) {
			prepared = append(prepared, replacement{receiver: receiver})
		}
	}
	coord.nativeHNSWIndexesMu.RUnlock()
	for i := range prepared {
		p := &prepared[i]
		p.receiver.nativeHNSWUseMu.Lock()
		err := p.receiver.validateNativeHNSWRead(read)
		if err == nil || errors.Is(err, errVectorIndexStaleDocumentGeneration) {
			p.candidate, err = p.receiver.cloneNativeHNSWForSchema(read, meta)
		}
		if err != nil {
			p.receiver.nativeHNSWUseMu.Unlock()
			prepared = prepared[:i]
			finish(false)
			return nil, fmt.Errorf("collections: native vector schema preparation: %w", err)
		}
	}
	return finish, nil
}

// The source use lock is held. Snapshot/load is the maintained graph codec;
// clone base and delta independently, never fold the original before DDL.
func (idx *VectorIndex) cloneNativeHNSWForSchema(read *NativeJSONRead, meta CollectionMeta) (candidate *VectorIndex, resultErr error) {
	allocator := read.capture.NativeAllocator()
	scratchBytes, err := idx.nativeHNSWPersistenceCharge()
	if err != nil {
		return nil, err
	}
	scratch, err := allocator.AdmitNativeBytes(scratchBytes)
	if err != nil {
		return nil, err
	}
	defer scratch.Close()
	// Snapshot and canonical-row decoder allocation cohorts overlap.
	decoder, err := allocator.AdmitNativeBytes(0)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	peak := nativeHNSWPeakAdmission(decoder)
	def, _ := findVectorIndex(meta.VectorIndexes, idx.name)
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{}))) + 64<<10
	for _, plane := range []*VectorIndex{idx, idx.liveDelta} {
		if plane == nil {
			continue
		}
		for _, row := range plane.nodes {
			n, e := nativeHNSWRowCharge(def, row.documentID)
			if e != nil || n > ^uint64(0)-charge {
				return nil, memtable.ErrCOWCapacity
			}
			charge += n
		}
	}
	if idx.liveDelta != nil {
		n := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{}))) + 64<<10
		if n > ^uint64(0)-charge {
			return nil, memtable.ErrCOWCapacity
		}
		charge += n
	}
	// Reuse the maintained metadata construction bound for retained scalar
	// definitions, compiled path/runtimes and map descriptors in each plane.
	// Recompute it from the proposed schema rather than retaining old charges.
	planes := uint64(1)
	if idx.liveDelta != nil {
		planes++
	}
	if err = nativeIndexSchemaScratch(meta, nil, func(n uint64) error {
		n = memtable.COWAllocationCharge(n)
		if n > (^uint64(0)-charge)/planes {
			return memtable.ErrCOWCapacity
		}
		charge += n * planes
		return nil
	}); err != nil {
		return nil, err
	}
	lease, err := allocator.AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	keep := false
	var owned *VectorIndex
	defer func() {
		if !keep {
			if owned != nil && owned.nativeHNSWLease != nil {
				owned.releaseNativeHNSW()
			} else {
				lease.Close()
			}
		}
	}()
	schema, err := newNativeSchemaState(&meta, func(n uint64) (cowbtree.OwnedMapLease, error) { return allocator.AdmitNativeBytes(n) })
	if err != nil {
		return nil, err
	}
	defer func() { work := schema.release(); work.Complete() }()
	opts := vectorIndexOptionsFromDefinition(def)
	opts.RebuildDeletedRatio = idx.rebuildDeletedRatio
	candidate, err = newVectorIndexWithMeta(idx.collection, opts, *schema.meta)
	if err != nil {
		return nil, err
	}
	owned = candidate
	candidate.nativeHNSWLease, candidate.nativeHNSWCharge = lease, charge
	schema.retain()
	candidate.nativeHNSWSchemaState = schema
	clonePlane := func(source, target *VectorIndex) error {
		snapshot, _ := source.persistSnapshot()
		if reason := target.loadPersistSnapshot(snapshot); reason != "" {
			return errors.New("collections: native vector schema snapshot: " + reason)
		}
		// The snapshot doc-map keys may borrow the source map. Rekey from the
		// cloned nodes so source receipt retirement releases every old alias.
		current := make(map[string]int, len(target.currentNode))
		for _, ordinal := range target.currentNode {
			current[string(target.nodes[ordinal].documentID)] = ordinal
		}
		target.currentNode = current
		if err := populateNativeHNSWScalarRows(target, read, peak, candidate); err != nil {
			return err
		}
		copyNativeHNSWBookkeeping(target, source)
		// The codec view preceded scalar population. Publish the privately owned
		// complete plane; no view/map aliases the original runtime.
		target.publishSearchViewLocked(true)
		return nil
	}
	if err = clonePlane(idx, candidate); err != nil {
		return nil, err
	}
	if idx.liveDelta != nil {
		delta, e := newVectorIndexWithMeta(idx.collection, opts, *schema.meta)
		if e != nil {
			return nil, e
		}
		// Initialization derives the proposed scalar schema from the facade,
		// then the private plane retains its original non-serving role.
		delta.collection = idx.liveDelta.collection
		candidate.liveDelta = delta
		if err = clonePlane(idx.liveDelta, delta); err != nil {
			return nil, err
		}
	}
	candidate.nativeHNSWIncarnation, candidate.nativeHNSWSchema, candidate.nativeHNSWVersion = idx.nativeHNSWIncarnation, idx.nativeHNSWSchema, idx.nativeHNSWVersion
	candidate.nativeHNSWIDBytes = idx.nativeHNSWIDBytes
	candidate.recordNativeDefinition(def)
	candidate.liveDeltaEnabled.Store(idx.liveDeltaEnabled.Load())
	candidate.publishSearchViewLocked(true)
	keep = true
	return candidate, nil
}

// The codec omits runtime-only dirty/coverage bookkeeping. Preserve it so a
// schema change cannot mark an unsaved graph clean. Node charges cover the maps.
func copyNativeHNSWBookkeeping(target, source *VectorIndex) {
	target.entry, target.maxLevel = source.entry, source.maxLevel
	target.mutationSeq = source.mutationSeq
	target.sourceDocumentRootsValid = source.sourceDocumentRootsValid
	target.sourceDocumentGeneration = source.sourceDocumentGeneration
	target.sourceDocumentAccountedGeneration = source.sourceDocumentAccountedGeneration
	target.sourceDocumentAccountedGenerationValid = source.sourceDocumentAccountedGenerationValid
	target.sourceDocumentState, target.sourceDocumentStateValid = source.sourceDocumentState, source.sourceDocumentStateValid
	target.unnotifiedDocumentIDs = cloneNativeHNSWDocumentSet(source.unnotifiedDocumentIDs)
	target.persistedEpoch, target.fullSnapshotBaseEpoch, target.persistedBytesDisk = source.persistedEpoch, source.fullSnapshotBaseEpoch, source.persistedBytesDisk
	target.persistedSnapshotDirty, target.dirtyMeta = source.persistedSnapshotDirty, source.dirtyMeta
	target.dirtyNodes, target.dirtyDocs = maps.Clone(source.dirtyNodes), cloneNativeHNSWDocumentSet(source.dirtyDocs)
	target.lastRebuildDuration = source.lastRebuildDuration
	target.nativePersistent, target.trackSearchViewDirty = source.nativePersistent, source.trackSearchViewDirty
	target.parallelReciprocalLinks, target.constructionWorkers = source.parallelReciprocalLinks, source.constructionWorkers
	target.liveDeltaCutovers, target.liveANNFullRebuilds = source.liveDeltaCutovers, source.liveANNFullRebuilds
	target.searchViewAcknowledged = source.searchViewAcknowledged
	target.searchViewAcknowledgedMutationSeq, target.searchViewAcknowledgedGeneration = source.searchViewAcknowledgedMutationSeq, source.searchViewAcknowledgedGeneration
}

func cloneNativeHNSWDocumentSet(source map[string]struct{}) map[string]struct{} {
	if source == nil {
		return nil
	}
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[strings.Clone(key)] = struct{}{}
	}
	return result
}

// Shared by fresh Load and schema replacement. The runtime owner covers both
// planes' column storage; canonical decoder scratch remains operation-local.
func populateNativeHNSWScalarRows(index *VectorIndex, read *NativeJSONRead, admit func(uint64) error, owner *VectorIndex) error {
	if len(index.scalarDefinitions) == 0 {
		return nil
	}
	materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat)}
	for _, row := range index.nodes {
		doc, err := read.namedDomainGetAdmitted("primary", row.documentID, admit)
		if errors.Is(err, tree.ErrKeyNotFound) && (row.deleted || index.sourceDocumentGeneration != read.collection.documentGeneration) {
			err = nil
		}
		if err != nil {
			return err
		}
		var scalar map[string][]byte
		if doc != nil {
			scalar, err = index.nativeScalarRow(materializer, doc)
			if err != nil {
				return err
			}
		}
		for _, value := range scalar {
			n := memtable.COWAllocationCharge(uint64(len(value)) * 4)
			if n > ^uint64(0)-owner.nativeHNSWCharge {
				return memtable.ErrCOWCapacity
			}
			if err = owner.nativeHNSWLease.Resize(owner.nativeHNSWCharge + n); err != nil {
				return err
			}
			owner.nativeHNSWCharge += n
		}
		if err = index.appendNativeScalarRowValuesLocked(scalar); err != nil {
			return err
		}
	}
	return nil
}
