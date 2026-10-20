package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// A scalar stamp proves only which original complete cut was validated. It
// grants no storage access and retains no refunded domain or capture header.
func (idx *VectorIndex) nativePartitionCutMatches(r *NativeJSONRead) bool {
	token, ok := r.capture.Snapshot().StateToken()
	if !ok {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.nativePartitionCutValid && !idx.dirtyMeta && !idx.persistedSnapshotDirty && len(idx.dirtyNodes) == 0 && len(idx.dirtyDocs) == 0 && idx.nativePartitionSequence == r.capture.LogicalSequence() && idx.sourceDocumentStateValid && idx.sourceDocumentState == token
}

func (idx *VectorIndex) recordNativePartitionCut(r *NativeJSONRead, seq uint64, bytesDisk int64, snapshotSeq uint64) bool {
	token, ok := r.capture.Snapshot().StateToken()
	if !ok {
		return false
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.nativePartitionCutValid = r.capture.LogicalSequence() == seq
	if !idx.nativePartitionCutValid {
		return false
	}
	idx.nativePartitionSequence = seq
	idx.sourceDocumentState, idx.sourceDocumentStateValid = token, true
	idx.persistedBytesDisk = bytesDisk
	idx.persistedSnapshotDirty = idx.mutationSeq != snapshotSeq
	if !idx.persistedSnapshotDirty {
		idx.dirtyMeta = false
		clear(idx.dirtyNodes)
		clear(idx.dirtyDocs)
		idx.acknowledgePartitionLivePersistenceLockedV2()
	}
	return true
}

// The original live node bound already includes JSON expansion and transient
// snapshots. Census the actual retained cohort before its snapshot/table copies.
func (idx *VectorIndex) nativePartitionSnapshotCapacity() (uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	live := idx.partitionLive
	if live == nil || live.invalid {
		return 0, ErrVectorIndexPartitionLiveUnavailableV1
	}
	perNode, ok := idx.vectorPartitionLiveNodeBytesV1()
	if !ok {
		return 0, memtable.ErrCOWCapacity
	}
	total, err := nativeTypedCheckedMul(uint64(len(idx.nodes)+idx.partitionLiveNodeCountLocked()), perNode)
	if err != nil {
		return 0, err
	}
	for _, n := range []uint64{4096, uint64(len(live.packDomains)) * 132, live.nodeIDBytes, live.ownerIDBytes, uint64(live.ownerCountV1()) * 256} {
		if err = nativeTypedCheckedAdd(&total, n); err != nil {
			return 0, err
		}
	}
	for _, rep := range live.representatives {
		if err = nativeTypedCheckedAdd(&total, uint64(len(rep.vector))*4+128); err != nil {
			return 0, err
		}
	}
	for _, n := range idx.nodes {
		if err = nativeTypedCheckedAdd(&total, uint64(len(n.documentID))); err != nil {
			return 0, err
		}
	}
	extra, err := nativeSchemaBackingCharge(reflect.ValueOf(idx.partitionPreparation), 0)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, extra); err != nil {
		return 0, err
	}
	return nativeTypedCheckedMul(total, 16)
}

func (idx *VectorIndex) saveNativePartitionLiveSnapshot(replay *backenddb.CommandWALIntent, owner *CommandWALAdmittedCollection) (status VectorIndexLoadStatus, resultErr error) {
	c := idx.collection
	if replay != nil || owner != nil || !idx.isPartitionLiveCarrier() {
		return status, backenddb.ErrNativeSemanticUnsupported
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return status, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	g := &tx.bindings[0]
	def, ok := findVectorIndex(g.probe.meta.VectorIndexes, idx.name)
	if !ok || def.Strategy != VectorIndexStrategyColumnGraph || idx.validateNativeSnapshotDefinition(def) != "" {
		return status, ErrVectorIndexPartitionLiveMismatchV1
	}
	primary, _ := g.probe.collection.domains.Get("primary")
	manifest, present := g.probe.collection.domains.Get("column/manifest")
	if !present || primary != nil && primary.typed != nil {
		return status, backenddb.ErrNativeSemanticUnsupported
	}
	coverage, err := vectorIndexCoverageAtRoot(g.probe.meta, tx.core.Snapshot(), manifest.baseRoot)
	if err != nil {
		return status, err
	}
	idx.mu.RLock()
	live := idx.partitionLive
	sourceCurrent := live != nil && !live.invalid && live.source == coverage.source
	idx.mu.RUnlock()
	if !sourceCurrent {
		return status, ErrVectorIndexPartitionLiveMismatchV1
	}
	token, ok := tx.core.Snapshot().StateToken()
	if !ok {
		return status, backenddb.ErrClosed
	}
	idx.recordSourceDocumentState(coverage.generation, token)
	if !idx.needsNativeAutoPersist() {
		// A checkpoint/unrelated publication may only invalidate the scalar stamp.
		// Load and validate the actual complete carrier rather than inventing a cut.
		_, status, err = c.loadNativePartitionLiveSnapshot(vectorIndexOptionsFromDefinition(def))
		return status, err
	}
	capacity, err := idx.nativePartitionSnapshotCapacity()
	if err != nil {
		return status, err
	}
	if err = tx.admitStage(g, capacity); err != nil {
		return status, err
	}
	snapshot, snapshotSeq := idx.persistSnapshot()
	table, bytesDisk, err := buildVectorIndexNativeSnapshotTable(snapshot)
	if err != nil {
		return status, err
	}
	defer resetCollectionRunTable(table)
	table.Freeze()
	var count int
	it := table.NewIterator(nil, nil)
	for it.Valid() {
		count++
		it.Next()
	}
	iterErr := it.Error()
	closeErr := it.Close()
	if err = errors.Join(iterErr, closeErr); err != nil {
		return status, err
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONDomainChange{}))) + memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof(memtable.COWMutation{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(txnhistory.Effect{})))
	if err = tx.admitStage(g, charge); err != nil {
		return status, err
	}
	entries := make([]memtable.COWMutation, 0, count)
	it = table.NewIterator(nil, nil)
	for it.Valid() {
		key, value := it.UnsafeKey(), it.UnsafeValue()
		if err = it.Error(); err != nil {
			break
		}
		if err = tx.admitStage(g, memtable.COWAllocationCharge(uint64(len(key)))+memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			break
		}
		entries = append(entries, memtable.COWMutation{Key: bytes.Clone(key), Value: bytes.Clone(value)})
		it.Next()
	}
	err = errors.Join(err, it.Error(), it.Close())
	if err != nil {
		return status, err
	}
	// Canonical collection root naming is preserved; only the complete image's
	// original native domain replaces its former base/captured/pending sources.
	domainName := collectionVectorIndexRootName(c.name, idx.name)[len(c.name)+1:]
	policy, err := collectionRootStoragePolicyForDB(tx.db, g.probe.meta, collectionVectorIndexRootName(c.name, idx.name))
	if err != nil {
		return status, err
	}
	g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, custody: tx.custody, expectedCollection: g.probe.collection, partitionLive: true, additional: []nativeJSONDomainChange{{name: domainName, policy: policy, entries: entries, fullReplacement: true}}}
	g.effects = []txnhistory.Effect{{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(g.name)}}
	if err = tx.admitStage(g, memtable.COWAllocationCharge(uint64(10+len(c.name)+len(idx.name)))); err != nil {
		return status, err
	}
	payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload(c.name, idx.name)
	if err != nil {
		return status, err
	}
	apply := nativeCommandWALApplyCommit{frame: commandwalapply.LoweredFrame{Kind: commitlog.CommandKindCollectionPersistPartitionLive, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionRebuildVectorIndexV1, Payload: payload}, sync: true}
	backend := tx.db
	if err = tx.commitNativeApply(false, &apply); err != nil {
		if apply.result.LogicalInstalled {
			err = commitAmbiguousError("partition live durable completion", err)
		}
		return status, err
	}
	// A successful logical install alone does not establish durable binding.
	// Command WAL uses its actual stable frame receipt; WAL-off uses the
	// original synchronous physical seal already awaited by the bridge.
	durable := apply.result.LogicalInstalled
	if backend.CommandWALEnabled() {
		durable = durable && apply.result.Assigned && apply.result.WALCompleted && apply.result.WALStable
	} else {
		durable = durable && apply.result.Seal.Valid()
	}
	if !durable {
		return status, commitAmbiguousError("partition live durable receipt", backenddb.ErrNativeSemanticUnsupported)
	}
	// Only the actual assigned installation receipt can stamp this runtime.
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return status, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if !idx.recordNativePartitionCut(read, apply.result.LogicalSequence, bytesDisk, snapshotSeq) {
		_, status, err = c.loadNativePartitionLiveSnapshot(vectorIndexOptionsFromDefinition(def))
		return status, err
	}
	status.Loaded, status.RootName, status.BytesDisk = true, collectionVectorIndexRootName(c.name, idx.name), bytesDisk
	return status, nil
}

func (c *Collection) loadNativePartitionLiveSnapshot(opts VectorIndexOptions) (index *VectorIndex, status VectorIndexLoadStatus, resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, status, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	primary, _ := read.collection.domains.Get("primary")
	if primary != nil && primary.typed != nil {
		return nil, status, backenddb.ErrNativeSemanticUnsupported
	}

	name := vectorIndexOptionName(opts)
	def, ok := findVectorIndex(read.meta.VectorIndexes, name)
	if !ok {
		status.ExactFallbackReason = vectorIndexFallbackMissingVectorIndexMetadata
		return nil, status, nil
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		return nil, status, backenddb.ErrNativeSemanticUnsupported
	}
	status.RootName = collectionVectorIndexRootName(c.name, name)
	domain, ok := read.collection.domains.Get(status.RootName[len(c.name)+1:])
	if !ok {
		status.ExactFallbackReason = vectorIndexFallbackMissingGraphRoot
		return nil, status, nil
	}
	// Import retains an empty descriptor for each declared root. Like the
	// physical loader's zero root with no overlays, this permits the original
	// first-carrier creation; an established carrier must still contain meta.
	if domain != nil && domain.baseRoot == 0 && domain.pending == nil && domain.captured == nil && domain.typed == nil {
		status.ExactFallbackReason = vectorIndexFallbackMissingGraphRoot
		return nil, status, nil
	}
	source, err := newNativeDomainRead(domain, read.capture.Snapshot())
	if err != nil {
		return nil, status, err
	}
	scratch, err := read.physicalScratch()
	if err != nil {
		return nil, status, err
	}
	// Preflight every actual record before any JSON/map/runtime allocation.
	var charge uint64
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return nil, status, err
	}
	defer lease.Close()
	var next []byte
	for {
		entry, found, e := source.seekGE(next, nil, scratch, read.readLeaf)
		if e != nil {
			return nil, status, e
		}
		if !found {
			break
		}
		n, e := nativeTypedCheckedMul(uint64(len(entry.Key)+len(entry.Value)), 16)
		if e != nil {
			return nil, status, e
		}
		if e = nativeTypedCheckedAdd(&n, 1024); e != nil {
			return nil, status, e
		}
		if e = nativeTypedCheckedAdd(&charge, memtable.COWAllocationCharge(n)); e != nil {
			return nil, status, e
		}
		if e = lease.Resize(charge); e != nil {
			return nil, status, e
		}
		next = append(append(next[:0], entry.Key...), 0)
		if entry.Flags&node.FlagPointer != 0 && entry.Flags&node.FlagTombstone == 0 {
			value, e := read.readValue(entry.ValuePtr, func(n uint64) error {
				expanded, e := nativeTypedCheckedMul(n, 16)
				if e != nil {
					return e
				}
				if e = nativeTypedCheckedAdd(&charge, expanded); e != nil {
					return e
				}
				return lease.Resize(charge)
			})
			value = nil
			_ = value
			if e != nil {
				return nil, status, e
			}
		}
	}
	rawMeta, err := source.getEntry([]byte(vectorIndexNativeKeyMeta), scratch, read.readLeaf)
	if err != nil {
		return nil, status, err
	}
	if rawMeta.Flags&node.FlagTombstone != 0 {
		return nil, status, backenddb.ErrNativeSemanticUnsupported
	}
	var metadata []byte
	if rawMeta.Flags&node.FlagPointer != 0 {
		metadata, err = read.readValue(rawMeta.ValuePtr, func(uint64) error { return nil })
		if err != nil {
			return nil, status, err
		}
	} else {
		metadata = bytes.Clone(rawMeta.Value)
	}
	next = nil
	snapshot, bytesDisk, reason, err := readVectorIndexNativeSnapshotRecords(metadata, func() ([]byte, []byte, bool, error) {
		for {
			entry, found, e := source.seekGE(next, nil, scratch, read.readLeaf)
			if e != nil || !found {
				return nil, nil, false, e
			}
			key := bytes.Clone(entry.Key)
			next = append(append(next[:0], key...), 0)
			if entry.Flags&node.FlagTombstone != 0 {
				continue
			}
			var value []byte
			if entry.Flags&node.FlagPointer != 0 {
				value, e = read.readValue(entry.ValuePtr, func(uint64) error { return nil })
				if e != nil {
					return nil, nil, false, e
				}
			} else {
				value = bytes.Clone(entry.Value)
			}
			return key, value, true, nil
		}
	})
	if err != nil {
		return nil, status, err
	}
	if reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	manifest, ok := read.collection.domains.Get("column/manifest")
	if !ok {
		return nil, status, backenddb.ErrNativeSemanticUnsupported
	}
	coverage, err := vectorIndexCoverageAtRoot(read.meta, read.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return nil, status, err
	}
	if snapshot.Meta.PartitionLive == nil || !coverage.matches(snapshot.Meta) {
		status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
		return nil, status, nil
	}
	index, err = newVectorIndex(c, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return nil, status, err
	}
	if reason = index.loadPersistSnapshot(snapshot); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	index.setPartitionLiveCarrier(true)
	if reason = index.validateNativeSnapshotDefinition(def); reason != "" {
		return nil, status, fmt.Errorf("%w: %s", ErrVectorIndexPartitionLiveMismatchV1, reason)
	}
	token, ok := read.capture.Snapshot().StateToken()
	if !ok {
		return nil, status, backenddb.ErrClosed
	}
	index.recordSourceDocumentState(coverage.generation, token)
	index.recordNativePartitionCut(read, read.capture.LogicalSequence(), bytesDisk, index.nativeMutationSequence())
	index.materializePartitionLiveStagedOwnersV2()
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, status, err
	}
	unchanged := index.nativePartitionCutMatches(current)
	closeErr := current.Close()
	if closeErr != nil {
		return nil, status, closeErr
	}
	if !unchanged {
		return nil, status, backenddb.ErrConcurrentModification
	}

	c.registerVectorIndexCurrentCatalog(index)
	status.Loaded, status.BytesDisk = true, bytesDisk
	return index, status, nil
}

func (c *Collection) vectorPartitionLiveWarmIndexCurrentV1(name string) bool {
	idx := c.registeredVectorIndex(name)
	if idx == nil {
		return false
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return false
	}
	defer read.Close()
	return idx.nativePartitionCutMatches(read)
}

func (c *Collection) nativePartitionCurrentStateV1(index string) (identity VectorPartitionSourceIdentityV1, generation uint64, token backenddb.StateToken, resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return identity, 0, token, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	manifest, ok := read.collection.domains.Get("column/manifest")
	if !ok {
		return identity, 0, token, backenddb.ErrNativeSemanticUnsupported
	}
	primary, _ := read.collection.domains.Get("primary")
	if primary != nil && primary.typed != nil {
		return identity, 0, token, backenddb.ErrNativeSemanticUnsupported
	}
	coverage, err := vectorIndexCoverageAtRoot(read.meta, read.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return identity, 0, token, err
	}
	identity, generation = coverage.source, coverage.generation
	if index != "" {
		status, e := c.nativeVectorGraphStatusAtRead(read, index)
		if e != nil {
			return identity, 0, token, e
		}
		if !status.Loaded {
			return identity, 0, token, ErrVectorPartitionSearchUnavailable
		}
	}
	token, ok = read.capture.Snapshot().StateToken()
	if !ok {
		return identity, 0, token, backenddb.ErrClosed
	}
	return identity, generation, token, nil
}
