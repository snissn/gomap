package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"sort"
)

// Ordinary HNSW reuses the maintained runtime and graph record codec. Typed
// ColumnGraph and partition-live keep their separately authenticated routes.
func nativeOrdinaryHNSWMeta(meta CollectionMeta) bool {
	if meta.Options.ColumnStore != nil {
		return false
	}
	for _, def := range meta.VectorIndexes {
		if !vectorIndexDefinitionUsesNativeRuntime(def) {
			return false
		}
	}
	return true
}
func nativeVectorDomainRemoved(name string, meta CollectionMeta) bool {
	n, ok := strings.CutPrefix(name, "vector-index/")
	if !ok {
		return false
	}
	_, found := findVectorIndex(meta.VectorIndexes, n)
	return !found
}

const nativeVectorVersionPrefix = "collection-vector-version/"

func nativeVectorVersionKey(collection, index string) string {
	return nativeVectorVersionPrefix + collection + "/" + index
}

func (c *Collection) nativeCreateVectorIndex(def VectorIndexDefinition) (*CollectionMeta, error) {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	meta, err := c.nativeJSON.mutateIndexFamilies(c.name, nil, nil, false, &def, "")
	if err == nil {
		c.collectionSchemaCoordinator().hasNativeVectorIndexes.Store(len(meta.VectorIndexes) != 0)
	}
	return meta, err
}
func (c *Collection) nativeDropVectorIndex(name string) (*CollectionMeta, error) {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	meta, err := c.nativeJSON.mutateIndexFamilies(c.name, nil, nil, false, nil, name)
	if err == nil {
		c.collectionSchemaCoordinator().hasNativeVectorIndexes.Store(len(meta.VectorIndexes) != 0)
		old := c.registeredVectorIndex(name)
		coord := c.collectionSchemaCoordinator()
		coord.nativeHNSWIndexesMu.Lock()
		if coord.nativeHNSWIndexes[name] == old {
			delete(coord.nativeHNSWIndexes, name)
		}
		coord.nativeHNSWIndexesMu.Unlock()
		if old != nil {
			old.releaseNativeHNSW()
		}
	}
	return meta, err
}

// The retained receipt covers runtime backings and maintained construction
// scratch. Decoder, persistence and concurrent query workspaces have separate
// operation-local receipts.
func nativeHNSWRowCharge(def VectorIndexDefinition, id []byte) (uint64, error) {
	// levelForDocumentID is deterministic and capped at32. Capacity is 2*M
	// on layer0 and M on each actual upper layer. Four copies cover growable
	// node/adjacency backings, the maintained immutable search view and scratch.
	if def.M <= 0 || def.Dimensions <= 0 {
		return 0, memtable.ErrCOWCapacity
	}
	probe := VectorIndex{name: def.Name, m: def.M}
	level := probe.levelForDocumentID(id)
	if uint64(def.M) > ^uint64(0)/uint64(level+2) {
		return 0, memtable.ErrCOWCapacity
	}
	neighborCapacity := uint64(level+2) * uint64(def.M)
	total := uint64(512)
	for _, pair := range [][2]uint64{
		{uint64(unsafe.Sizeof(vectorIndexNode{})), 4},
		{uint64(def.Dimensions), 4 * 4},
		{neighborCapacity, 4 * uint64(unsafe.Sizeof(vectorIndexNeighbor{}))},
		{uint64(level + 1), 4 * uint64(unsafe.Sizeof([]vectorIndexNeighbor{}))},
		{uint64(len(id)), 8},
	} {
		if pair[0] > (^uint64(0)-total)/pair[1] {
			return 0, memtable.ErrCOWCapacity
		}
		total += pair[0] * pair[1]
	}
	return memtable.COWAllocationCharge(total), nil
}

// Decoder receipts are operation-local. Retaining one maximum document/record
// workspace avoids charging every historical BSON image to a live HNSW node.
func nativeHNSWPeakAdmission(lease *memtable.COWExternalLease) func(uint64) error {
	var peak uint64
	return func(n uint64) error {
		if n > ^uint64(0)/32 {
			return memtable.ErrCOWCapacity
		}
		n = memtable.COWAllocationCharge(n * 32)
		if n <= peak {
			return nil
		}
		if err := lease.Resize(n); err != nil {
			return err
		}
		peak = n
		return nil
	}
}

func (idx *VectorIndex) releaseNativeHNSW() {
	idx.nativeHNSWUseMu.Lock()
	defer idx.nativeHNSWUseMu.Unlock()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.nativeHNSWLease == nil {
		return
	}
	idx.sourceDocumentRootsValid = false
	idx.nodes = nil
	idx.vectorRows = nil
	idx.currentNode = nil
	idx.searchView.Store(nil)
	idx.searchViewSpare.Store(nil)
	idx.liveDelta = nil
	idx.scalarColumns = nil
	idx.scalarRuntimes = nil
	idx.scalarDefinitions = nil
	idx.insertScratch = vectorIndexSearchScratch{}
	idx.insertQuantScratch = nil
	idx.dirtyNodes = nil
	idx.dirtyDocs = nil
	idx.unnotifiedDocumentIDs = nil
	idx.searchViewDirty = nil
	if idx.nativeHNSWSchemaState != nil {
		completion := idx.nativeHNSWSchemaState.release()
		idx.nativeHNSWSchemaState = nil
		completion.Complete()
	}
	idx.nativeHNSWLease.Close()
	idx.nativeHNSWLease = nil
}
func (coord *collectionSchemaCoordinator) nativeHNSWRuntimeList() []*VectorIndex {
	if coord == nil {
		return nil
	}
	coord.nativeHNSWIndexesMu.RLock()
	defer coord.nativeHNSWIndexesMu.RUnlock()
	out := make([]*VectorIndex, 0, len(coord.nativeHNSWIndexes))
	for _, idx := range coord.nativeHNSWIndexes {
		out = append(out, idx)
	}
	return out
}
func (coord *collectionSchemaCoordinator) closeNativeHNSWForDB() error {
	indexes := coord.nativeHNSWRuntimeList()
	var err error
	if len(indexes) != 0 {
		err = indexes[0].collection.flushNativeHNSW()
	}
	for _, idx := range indexes {
		idx.releaseNativeHNSW()
	}
	coord.nativeHNSWIndexesMu.Lock()
	coord.nativeHNSWIndexes = nil
	coord.nativeHNSWIndexesMu.Unlock()
	return err
}

// walkNativeHNSWRows borrows one complete cut. Admission precedes value-pointer
// resolution, BSON decoding and every caller-owned record copy.
func walkNativeHNSWRows(r *NativeJSONRead, admit func(uint64) error, scratch func(uint64) error, visit func([]byte, []byte) error) error {
	if r.domain.domain == nil {
		return nil
	}
	if err := admit(memtable.COWAllocationCharge(page.PageSize + 1)); err != nil {
		return err
	}
	cursor := make([]byte, page.PageSize+1)
	var start []byte
	for {
		entry, ok, err := nativeJSONSeekLive(&r.domain, r, start, nil, cursor)
		if err != nil || !ok {
			return err
		}
		size := len(entry.Value)
		if entry.Flags&node.FlagPointer != 0 { // readValue accounts exact resolved storage below.
			size = 0
		}
		if err = scratch(uint64(size + len(entry.Key))); err != nil {
			return err
		}
		id := bytes.Clone(entry.Key)
		n := copy(cursor, entry.Key)
		if n >= len(cursor) {
			return memtable.ErrCOWCapacity
		}
		cursor[n] = 0
		start = cursor[:n+1]
		var value []byte
		if entry.Flags&node.FlagPointer != 0 {
			value, err = r.readValue(entry.ValuePtr, scratch)
		} else {
			value = bytes.Clone(entry.Value)
		}
		if err != nil {
			return err
		}
		if err = visit(id, value); err != nil {
			return err
		}
	}
}

func (c *Collection) buildNativeHNSW(opts VectorIndexOptions, register bool) (index *VectorIndex, resultErr error) {
	// Preserve the maintained constructor's finite nonpositive default, while
	// refusing nonfinite tuning before it can poison runtime or saved metadata.
	if math.IsNaN(opts.RebuildDeletedRatio) || math.IsInf(opts.RebuildDeletedRatio, 0) || opts.RebuildDeletedRatio > 1 {
		return nil, errors.New("collections: vector index rebuild deleted ratio must be finite and at most 1")
	}

	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	def, ok := findVectorIndex(read.meta.VectorIndexes, vectorIndexOptionName(opts))
	if !ok || !vectorIndexDefinitionUsesNativeRuntime(def) {
		return nil, errVectorIndexNotDeclared
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{}))) + 64<<10)
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
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{}))) + 64<<10
	admit := func(n uint64) error {
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if e := lease.Resize(charge + n); e != nil {
			return e
		}
		charge += n
		return nil
	}
	scratchLease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return nil, fmt.Errorf("collections: native vector build scratch admission: %w", err)
	}
	defer scratchLease.Close()
	scratch := nativeHNSWPeakAdmission(scratchLease)

	buildOptions := vectorIndexOptionsFromDefinition(def)
	buildOptions.RebuildDeletedRatio = opts.RebuildDeletedRatio
	index, err = newVectorIndexWithMeta(c, buildOptions, read.meta)
	if err != nil {
		return nil, err
	}
	materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat)}
	// Template resolution must borrow this same cut, rather than open another.
	if materializer.documentFormat == DocumentFormatTemplateV1 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	err = walkNativeHNSWRows(read, admit, scratch, func(id, doc []byte) error {
		vector, ok, e := vectorFromStoredDocument(materializer, doc, index.fieldPath)
		if e != nil {
			return e
		}
		if !ok {
			return nil
		}
		rowCharge, e := nativeHNSWRowCharge(def, id)
		if e != nil {
			return e
		}
		if e = admit(rowCharge); e != nil {
			return e
		}
		scalar, e := index.nativeScalarRow(materializer, doc)
		if e != nil {
			return e
		}
		for _, value := range scalar {
			if e = admit(memtable.COWAllocationCharge(uint64(len(value)) * 4)); e != nil {
				return e
			}
		}
		if uint64(len(id)) > ^uint64(0)-index.nativeHNSWIDBytes {
			return memtable.ErrCOWCapacity
		}
		index.nativeHNSWIDBytes += uint64(len(id))
		if e = index.insertVectorLocked(id, vector); e != nil {
			return e
		}
		return index.appendNativeScalarRowLocked(scalar)
	})
	if err != nil {
		return nil, err
	}
	owned = index
	index.nativeHNSWLease = lease
	index.nativeHNSWCharge = charge
	read.collection.schema.retain()
	index.nativeHNSWSchemaState = read.collection.schema
	index.nativeHNSWIncarnation = read.collection.incarnation
	index.nativeHNSWSchema = def.SchemaGeneration
	if domain, ok := read.collection.domains.Get("vector-index/" + def.Name); ok {
		index.nativeHNSWVersion = domain.interpretation
		index.recordFullSnapshotBaseEpoch(domain.baseRoot)
	}
	index.recordNativeDefinition(def)
	index.setNativePersistent(true)
	state, _ := read.capture.Snapshot().StateToken()
	index.recordSourceDocumentState(read.collection.documentGeneration, state)
	if register {
		if err = c.installNativeHNSW(index, read); err != nil {
			return nil, err
		}
	}
	keep = true
	return index, nil
}

func (c *Collection) installNativeHNSW(index *VectorIndex, expected *NativeJSONRead) (resultErr error) {
	runNativeVectorIndexBeforeInstallHookForTest(index.name)
	valid := func(r *NativeJSONRead) bool {
		def, ok := findVectorIndex(r.meta.VectorIndexes, index.name)
		if !ok || !collectionMetaValuesEqual(r.meta, *index.nativeHNSWSchemaState.meta) || def.SchemaGeneration != index.nativeHNSWSchema || r.collection.incarnation != index.nativeHNSWIncarnation || r.collection.documentGeneration != expected.collection.documentGeneration {
			return false
		}
		d, ok := r.collection.domains.Get("vector-index/" + index.name)
		return ok && d.interpretation == index.nativeHNSWVersion
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, current.Close()) }()
	if !valid(current) {
		return backenddb.ErrConcurrentModification
	}
	coord := c.collectionSchemaCoordinator()
	coord.nativeHNSWIndexesMu.Lock()
	if coord.nativeHNSWIndexes == nil {
		coord.nativeHNSWIndexes = make(map[string]*VectorIndex)
	}
	old := coord.nativeHNSWIndexes[index.name]
	coord.nativeHNSWIndexes[index.name] = index
	coord.nativeHNSWIndexesMu.Unlock()
	post, err := c.nativeJSON.Capture(c.name)
	if err == nil {
		if !valid(post) {
			err = backenddb.ErrConcurrentModification
		}
		err = errors.Join(err, post.Close())
	}
	if err == nil && !c.manager.registerCollectionHandleIfOpen(c) {
		err = backenddb.ErrClosed
	}
	if err != nil {
		coord.nativeHNSWIndexesMu.Lock()
		if coord.nativeHNSWIndexes[index.name] == index {
			if old == nil {
				delete(coord.nativeHNSWIndexes, index.name)
			} else {
				coord.nativeHNSWIndexes[index.name] = old
			}
		}
		coord.nativeHNSWIndexesMu.Unlock()
		return err
	}
	if old != nil && old != index {
		old.releaseNativeHNSW()
	}
	return nil
}

func (idx *VectorIndex) validateNativeHNSWRead(read *NativeJSONRead) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	def, ok := findVectorIndex(read.meta.VectorIndexes, idx.name)
	if !ok || idx.nativeHNSWSchemaState == nil || !collectionMetaValuesEqual(read.meta, *idx.nativeHNSWSchemaState.meta) || def.SchemaGeneration != idx.nativeHNSWSchema || read.collection.incarnation != idx.nativeHNSWIncarnation {
		return errVectorIndexStaleNativeRoot
	}
	domain, ok := read.collection.domains.Get("vector-index/" + idx.name)
	if !ok || domain.interpretation != idx.nativeHNSWVersion {
		return errVectorIndexStaleNativeRoot
	}
	if !idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != read.collection.documentGeneration {
		return errVectorIndexStaleDocumentGeneration
	}
	return nil
}

// This is the normal changed-ID maintenance policy, with native captured
// generation/reads instead of pager-root identity. An intervening publication
// invalidates coverage; it cannot bless an incomplete graph.
func (c *Collection) reconcileNativeHNSW(ids [][]byte) error {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	return c.reconcileNativeHNSWWithMutationHeld(ids)
}
func (c *Collection) reconcileNativeHNSWWithMutationHeld(ids [][]byte) (resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if len(read.meta.VectorIndexes) == 0 {
		return nil
	}
	scratchLease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return err
	}
	defer scratchLease.Close()
	scratch := nativeHNSWPeakAdmission(scratchLease)

	for _, def := range read.meta.VectorIndexes {
		idx := c.registeredVectorIndex(def.Name)
		if idx == nil {
			if _, err = c.buildNativeHNSW(vectorIndexOptionsFromDefinition(def), true); err != nil {
				return err
			}
			continue
		}
		// Keep the admitted public query's graph-size bound stable through
		// evaluation; graph mutations wait for its existing use lease.
		idx.nativeHNSWUseMu.Lock()
		defer idx.nativeHNSWUseMu.Unlock()
		generation, valid := idx.sourceDocumentCoverage()
		if valid && generation == read.collection.documentGeneration {
			continue
		}
		if !valid || generation+1 != read.collection.documentGeneration {
			idx.invalidateSourceDocumentRoots()
			return errVectorIndexStaleDocumentGeneration
		}
		idx.mu.Lock()
		for _, id := range ids {
			doc, e := read.namedDomainGetAdmitted("primary", id, scratch)
			if errors.Is(e, tree.ErrKeyNotFound) {
				doc = nil
				e = nil
			}
			if e != nil {
				err = e
				break
			}
			materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat)}
			idx.mu.Unlock()
			vector, scalarRow, e := idx.parseStoredVectorRow(materializer, id, doc)
			idx.mu.Lock()
			if e != nil {
				err = e
				break
			}
			if idx.bufferedStoredVectorMatchesLocked(id, vector, scalarRow) {
				continue
			}
			// Deletions and identical vectors retain no new node. Changed rows
			// append maintained graph storage (including its tombstoned history)
			// until the ordinary rebuild replaces and releases this runtime.
			if vector != nil {
				charge, e := nativeHNSWRowCharge(def, id)
				for _, value := range scalarRow {
					n := memtable.COWAllocationCharge(uint64(len(value)) * 4)
					if n > ^uint64(0)-charge {
						e = memtable.ErrCOWCapacity
						break
					}
					charge += n
				}
				if e != nil || charge > ^uint64(0)-idx.nativeHNSWCharge {
					err = memtable.ErrCOWCapacity
					break
				}
				if e = idx.nativeHNSWLease.Resize(idx.nativeHNSWCharge + charge); e != nil {
					err = e
					break
				}
				if uint64(len(id)) > ^uint64(0)-idx.nativeHNSWIDBytes {
					err = memtable.ErrCOWCapacity
					break
				}
				idx.nativeHNSWIDBytes += uint64(len(id))
				idx.nativeHNSWCharge += charge
			}
			if e = idx.insertParsedStoredDocumentLocked(id, vector, scalarRow, false); e != nil {
				err = e
				break
			}
		}
		idx.mu.Unlock()
		if err != nil {
			idx.invalidateSourceDocumentRoots()
			return err
		}
		state, _ := read.capture.Snapshot().StateToken()
		idx.recordSourceDocumentState(read.collection.documentGeneration, state)
	}
	return nil
}

// A prepared full graph is immutable original work. Only this bounded domain
// CAS runs under publication gates; HNSW construction never does.
type nativeHNSWPublication struct {
	collection *nativeCollectionState
	domainName string
	bootstrap  *nativeEngineVersion
	version    uint64
	rows       []memtable.COWMutation
	custody    *nativeMutationCustody
}

func (p *nativeHNSWPublication) PrepareNativePublication(latest backenddb.NativeSemanticState, _ *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
	base, ok := latest.(*nativeEngineVersion)
	if latest == nil {
		base = p.bootstrap
		ok = base != nil
	}
	if !ok {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: p.collection, sourceGeneration: p.collection.documentGeneration, domains: []nativeNamedDomainChange{{name: p.domainName, entries: p.rows, replace: true, expectedInterpretation: p.version}}}})
	if err == nil {
		c, _ := next.collections.Get(p.collection.incarnation)
		(nativeCollectionLifetime{}).Retain(c)
		p.custody.adoptCollection(c)
	}
	return backenddb.NativeFinalizedPublication{State: next}, err
}

func (idx *VectorIndex) saveNativeHNSW() (status VectorIndexLoadStatus, resultErr error) {
	c := idx.collection
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	idx.nativeHNSWUseMu.Lock()
	defer idx.nativeHNSWUseMu.Unlock()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return status, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if err = idx.validateNativeHNSWRead(read); err != nil {
		return status, err
	}
	domain, _ := read.collection.domains.Get("vector-index/" + idx.name)
	if domain.interpretation == ^uint64(0) {
		return status, memtable.ErrCOWCapacity
	}
	persistCharge, e := idx.nativeHNSWPersistenceCharge()
	if e != nil {
		return status, e
	}
	persistLease, e := read.capture.NativeAllocator().AdmitNativeBytes(persistCharge)
	if e != nil {
		return status, e
	}
	defer persistLease.Close()

	if err = idx.foldLiveDeltaForPersistence(); err != nil {
		return status, err
	}
	// Admit the maintained snapshot and codec workspace before allocating it.
	snapshot, sequence := idx.persistSnapshot()
	snapshot.Meta.NativeLogicalVersion = domain.interpretation + 1
	table, disk, err := buildVectorIndexNativeSnapshotTable(snapshot)
	if err != nil {
		return status, err
	}
	snapshot = vectorIndexPersistSnapshot{}
	defer resetCollectionRunTable(table)
	rows := make([]memtable.COWMutation, 0, table.Len())
	it := table.NewIterator(nil, nil)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		// The immutable run table and iterator remain owned until P1 finishes.
		// Existing COWWriter.Prepare copies these borrowed payloads exactly once.
		rows = append(rows, memtable.COWMutation{Key: it.UnsafeKey(), Value: it.UnsafeValue(), Flags: node.FlagInline})
	}
	if err = it.Error(); err != nil {
		return status, err
	}
	// The snapshot is now unreachable. Keep the encoded image, iterator/table
	// descriptor arrays and outgoing headers admitted, without retaining the
	// obsolete decoder/vector/edge workspace through COW preparation.
	projectionCharge := uint64(64<<10) + uint64(table.Len())*512 + uint64(disk)*2
	for _, row := range rows {
		projectionCharge += uint64(len(row.Key)) * 2
	}
	if err = persistLease.Resize(memtable.COWAllocationCharge(projectionCharge)); err != nil {
		return status, err
	}
	base, _ := read.capture.State().(*nativeEngineVersion)
	if base == nil {
		base = read.privateEngine
	}
	custody, err := newNativeMutationCustody(0, 1, base.allocator)
	if err != nil {
		return status, err
	}
	name := "vector-index/" + idx.name
	plan := &nativeHNSWPublication{collection: read.collection, bootstrap: base, version: domain.interpretation, domainName: name, rows: rows, custody: custody}
	ns := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: read.collection.incarnation, Domain: nativeJSONDomainNumber(read.meta, name), Interpretation: domain.interpretation}
	effect := txnhistory.Effect{Namespace: ns, Kind: txnhistory.RangeEffect}
	pub := &backenddb.NativePreparedPublication{Basis: read.capture.State(), StartSequence: read.capture.LogicalSequence(), Reprepare: plan, Custody: custody, Effects: []txnhistory.Effect{effect}, Dependencies: []txnhistory.Dependency{effect, {Namespace: txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: read.collection.incarnation, Domain: 1, Interpretation: 1}, Kind: txnhistory.RangeEffect}, {Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(c.name)}}}
	payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload(c.name, idx.name)
	if err != nil {
		return status, errors.Join(err, cancelPrivateNativePublication(pub))
	}
	intent, err := c.nativeJSON.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionRebuildVectorIndex, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionRebuildVectorIndexV1, payload)
	if err != nil {
		return status, errors.Join(err, cancelPrivateNativePublication(pub))
	}
	result, err := c.nativeJSON.db.WriteNativePublication(pub, intent, false)
	if err != nil && (result.Assigned || result.LogicalInstalled) {
		err = commitAmbiguousError("native vector snapshot completion", err)
	}
	if !result.Consumed {
		err = errors.Join(err, cancelPrivateNativePublication(pub))
	}
	if err != nil {
		return status, err
	}
	idx.mu.Lock()
	idx.nativeHNSWVersion = domain.interpretation + 1
	idx.mu.Unlock()
	idx.recordPersistedSnapshot(domain.baseRoot, disk, sequence)
	return VectorIndexLoadStatus{Loaded: true, RootName: collectionVectorIndexRootName(c.name, idx.name), RootID: domain.baseRoot, Epoch: domain.baseRoot, LogicalVersion: domain.interpretation + 1, BytesDisk: disk}, nil
}

func (c *Collection) loadNativeHNSW(opts VectorIndexOptions) (index *VectorIndex, status VectorIndexLoadStatus, resultErr error) {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, status, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	def, ok := findVectorIndex(read.meta.VectorIndexes, vectorIndexOptionName(opts))
	if !ok {
		return nil, status, errVectorIndexNotDeclared
	}
	name := "vector-index/" + def.Name
	domain, ok := read.collection.domains.Get(name)
	if !ok {
		return nil, status, backenddb.ErrNativeSemanticUnsupported
	}
	status.RootName = collectionVectorIndexRootName(c.name, def.Name)
	status.RootID = domain.baseRoot
	status.LogicalVersion = domain.interpretation
	// Reuse the maintained runtime only when it is the exact saved image.
	// Primary coverage alone cannot authenticate an unsaved or dirty graph.
	if cached := c.registeredVectorIndex(def.Name); cached != nil && domain.sourceGeneration == read.collection.documentGeneration && cached.validateNativeHNSWRead(read) == nil {
		cached.mu.RLock()
		clean := !cached.dirtyMeta && !cached.persistedSnapshotDirty && cached.nativeHNSWVersion == domain.interpretation
		disk := cached.persistedBytesDisk
		cached.mu.RUnlock()
		if clean && disk > 0 {
			status.Loaded = true
			status.Epoch = domain.baseRoot
			status.BytesDisk = disk
			return cached, status, nil
		}
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(64 << 10)
	if err != nil {
		return nil, status, err
	}
	defer lease.Close()
	charge := uint64(64 << 10)
	admit := func(n uint64) error {
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if e := lease.Resize(charge + n); e != nil {
			return e
		}
		charge += n
		return nil
	}
	snapshot, disk, reason, err := readNativeHNSWSnapshot(read, def, admit)
	if err != nil || reason != "" {
		status.ExactFallbackReason = reason
		if err != nil {
			err = fmt.Errorf("collections: native vector snapshot decode (%d admitted bytes): %w", charge, err)
		}
		return nil, status, err
	}

	runtimeCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{}))) + 64<<10
	var idBytes uint64
	for _, row := range snapshot.Nodes {
		if uint64(len(row.DocumentID)) > ^uint64(0)-idBytes {
			return nil, status, memtable.ErrCOWCapacity
		}
		idBytes += uint64(len(row.DocumentID))
		n, e := nativeHNSWRowCharge(def, []byte(row.DocumentID))
		if e != nil {
			return nil, status, e
		}
		if n > ^uint64(0)-runtimeCharge {
			return nil, status, memtable.ErrCOWCapacity
		}
		runtimeCharge += n
	}
	runtimeLease, err := read.capture.NativeAllocator().AdmitNativeBytes(runtimeCharge)
	if err != nil {
		return nil, status, fmt.Errorf("collections: native vector snapshot runtime (%d decode bytes, %d runtime bytes): %w", charge, runtimeCharge, err)
	}
	retained := false
	var owned *VectorIndex
	defer func() {
		if !retained {
			if owned != nil && owned.nativeHNSWLease != nil {
				owned.releaseNativeHNSW()
			} else {
				runtimeLease.Close()
			}
		}
	}()

	index, err = newVectorIndexWithMeta(c, vectorIndexOptionsFromDefinition(def), read.meta)
	if err != nil {
		return nil, status, err
	}
	if reason := index.loadPersistSnapshot(snapshot); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	owned = index
	index.nativeHNSWLease = runtimeLease
	index.nativeHNSWCharge = runtimeCharge
	index.nativeHNSWIDBytes = idBytes
	read.collection.schema.retain()
	index.nativeHNSWSchemaState = read.collection.schema
	index.nativeHNSWIncarnation = read.collection.incarnation
	index.nativeHNSWSchema = def.SchemaGeneration
	index.nativeHNSWVersion = domain.interpretation
	index.setNativePersistent(true)
	index.recordNativeDefinition(def)
	index.recordLoadedSnapshot(domain.baseRoot, disk)
	if len(index.scalarDefinitions) != 0 {
		materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat)}
		for _, row := range snapshot.Nodes {
			doc, e := read.namedDomainGetAdmitted("primary", []byte(row.DocumentID), admit)
			if errors.Is(e, tree.ErrKeyNotFound) && row.Deleted {
				e = nil
			}
			if e != nil {
				return nil, status, e
			}
			scalar, e := index.nativeScalarRow(materializer, doc)
			if e != nil {
				return nil, status, e
			}
			for _, value := range scalar {
				n := memtable.COWAllocationCharge(uint64(len(value)) * 4)
				if n > ^uint64(0)-index.nativeHNSWCharge {
					return nil, status, memtable.ErrCOWCapacity
				}
				if e = index.nativeHNSWLease.Resize(index.nativeHNSWCharge + n); e != nil {
					return nil, status, e
				}
				index.nativeHNSWCharge += n
			}
			if e = index.appendNativeScalarRowValuesLocked(scalar); e != nil {
				return nil, status, e
			}
		}
	}
	state, _ := read.capture.Snapshot().StateToken()
	index.recordSourceDocumentState(read.collection.documentGeneration, state)
	if err = c.installNativeHNSW(index, read); err != nil {
		return nil, status, err
	}
	retained = true
	status.Loaded = true
	status.Epoch = domain.baseRoot
	status.BytesDisk = disk
	return index, status, nil
}

// The physical projection persists both values with the same graph root.
func nativeVectorVersionRecord(interpretation, generation uint64) [16]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], interpretation)
	binary.BigEndian.PutUint64(b[8:], generation)
	return b
}

// Startup canonical rebuild uses the same physical publisher and one original
// system delta; this optional record preserves logical identity through replay.
type nativeHNSWRootVersion struct {
	key   string
	value [16]byte
}

func appendNativeHNSWVersion(it iterator.UnsafeIterator, version *nativeHNSWRootVersion) (iterator.UnsafeIterator, error) {
	base, ok := it.(*systemTargetIterator)
	if !ok || base.idx != 0 {
		_ = it.Close()
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	base.entries = append(base.entries, systemTargetEntry{key: []byte(version.key), value: bytes.Clone(version.value[:])})
	sort.Slice(base.entries, func(i, j int) bool { return bytes.Compare(base.entries[i].key, base.entries[j].key) < 0 })
	return base, nil
}

func nativeHNSWDecodeCharge(key, value int) (uint64, error) {
	n := uint64(key) + uint64(value)
	if n > (^uint64(0)-512)/8 {
		return 0, memtable.ErrCOWCapacity
	}
	// The largest decoded numeric element is an eight-byte int. JSON needs
	// at least two bytes per element (digit and separator); growable slices
	// can retain twice their length. Eight bytes per encoded byte covers that
	// worst case, while512 covers map/record and duplicate node descriptors.
	return memtable.COWAllocationCharge(512 + n*8), nil
}
func (idx *VectorIndex) nativeHNSWPersistenceCharge() (uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var total uint64 = 64 << 10
	add := func(n, factor uint64) bool {
		if n > (^uint64(0)-total)/factor {
			return false
		}
		total += n * factor
		return true
	}
	// persistSnapshot copies float32 vectors (4 bytes), neighbor IDs (8) and
	// distances (4). The maintained JSON codec emits at most17 bytes/float32
	// and11 bytes/uint32 ID, including commas. Three encoded images cover the
	// encoder buffer, run table and outgoing mutation copy: 4+3*17<=64 per
	// vector element and12+3*(11+17)<=112 per neighbor. Quantized int8 uses
	// at most5 encoded bytes. Fixed node/layer charges cover record structs,
	// growable descriptor arrays, keys, tombstones and hash-table slots; ID
	// bytes allow JSON's six-byte escaping plus owned ID/key copies.
	// Fold may append live-delta rows; include both original planes before it.
	charge := func(plane *VectorIndex) bool {
		for _, row := range plane.nodes {
			if !add(1, 1024) || !add(uint64(len(row.documentID)), 32) || !add(uint64(len(row.vector)), 64) || !add(uint64(len(row.quantized)), 24) || !add(uint64(len(row.neighbors)), 512) {
				return false
			}
			for _, neighbors := range row.neighbors {
				if !add(uint64(len(neighbors)), 112) {
					return false
				}
			}
		}
		return true
	}
	if !charge(idx) {
		return 0, memtable.ErrCOWCapacity
	}
	if idx.liveDelta != nil {
		idx.liveDelta.mu.RLock()
		ok := charge(idx.liveDelta)
		idx.liveDelta.mu.RUnlock()
		if !ok {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return memtable.COWAllocationCharge(total), nil
}

func readNativeHNSWSnapshot(read *NativeJSONRead, def VectorIndexDefinition, admit func(uint64) error) (snapshot vectorIndexPersistSnapshot, disk int64, reason string, resultErr error) {
	normalized, normalizeErr := normalizeVectorIndexDefinition(def)
	if normalizeErr != nil {
		return snapshot, 0, vectorIndexFallbackMetaMismatch, normalizeErr
	}
	def = normalized

	name := "vector-index/" + def.Name
	domain, ok := read.collection.domains.Get(name)
	if !ok {
		return snapshot, 0, vectorIndexFallbackMissingGraphRoot, nil
	}

	raw, err := read.namedDomainGetAdmitted(name, []byte(vectorIndexNativeKeyMeta), admit)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return snapshot, disk, vectorIndexFallbackMissingGraphRoot, nil
	}
	if err != nil {
		return snapshot, disk, "", err
	}
	if e := admit(uint64(len(raw)) * 16); e != nil {
		return snapshot, disk, "", e
	}
	var meta vectorIndexPersistMeta
	if err = json.Unmarshal(raw, &meta); err != nil {
		return snapshot, disk, "", err
	}
	if meta.NativeLogicalVersion != domain.interpretation || meta.SourceDocumentGeneration != read.collection.documentGeneration || meta.SourceDocumentGenerationVersion != vectorIndexDocumentGenerationVersion {
		return snapshot, disk, vectorIndexFallbackStaleDocumentRoot, nil
	}
	// Persisted tuning cannot change the canonical graph's allocation bounds.
	// The maintained decoder adopts these fields, so authenticate them before it
	// allocates any graph storage or installs a runtime.
	if meta.Name != def.Name || meta.Field != def.Field || meta.Metric != def.Metric || meta.Encoding != def.Encoding ||
		meta.M != def.M || meta.EfConstruction != def.EfConstruction || meta.EfSearch != def.EfSearch ||
		meta.Dimensions <= 0 || (def.Dimensions != 0 && meta.Dimensions != def.Dimensions) ||
		!(meta.RebuildDeletedRatio > 0 && meta.RebuildDeletedRatio <= 1) {
		return snapshot, disk, vectorIndexFallbackMetaMismatch, nil
	}
	// Decode incrementally with the maintained graph record codec. Each encoded
	// record, decoder backing and eventual HNSW storage is admitted first.
	snapshot.Meta = meta
	snapshot.DocMap.Current = make(map[string]int)
	nodes := make(map[int]vectorIndexPersistNode)
	maxNode := -1
	var start []byte
	for {
		key, value, found, e := read.namedDomainSeekAdmitted(name, start, nil, false, admit)
		if e != nil {
			return snapshot, disk, "", e
		}
		if !found {
			break
		}
		n, e := nativeHNSWDecodeCharge(len(key), len(value))
		if e != nil {
			return snapshot, disk, "", e
		}
		if e = admit(n); e != nil {
			return snapshot, disk, "", e
		}
		disk += int64(len(value))
		if !bytes.Equal(key, []byte(vectorIndexNativeKeyMeta)) {
			if reason := appendVectorIndexNativeSnapshotRecordV2(&snapshot, nodes, &maxNode, key, value); reason != "" {
				return snapshot, disk, reason, nil
			}
		}
		start = append(key, 0)
	}
	// Refuse a sparse/malicious ordinal before the maintained finalizer sizes
	// its node descriptor array from maxNode.
	if maxNode >= len(nodes) {
		return snapshot, disk, vectorIndexFallbackMissingGraphRootEntry, nil
	}
	if reason := finalizeVectorIndexNativeSnapshotRecordsV2(&snapshot, nodes, maxNode); reason != "" {
		return snapshot, disk, reason, nil
	}
	for _, row := range snapshot.Nodes {
		probe := VectorIndex{name: def.Name, m: def.M}
		if row.Level != probe.levelForDocumentID([]byte(row.DocumentID)) {
			return snapshot, disk, vectorIndexFallbackInvalidGraphRootEntry, nil
		}
	}
	for _, edge := range snapshot.Edges {
		limit := def.M
		if edge.Layer == 0 {
			limit *= 2
		}
		if len(edge.Neighbor) > limit {
			return snapshot, disk, vectorIndexFallbackInvalidGraphRootEntry, nil
		}
	}
	if meta.PartitionLive != nil {
		return snapshot, disk, vectorIndexFallbackMetaMismatch, nil
	}
	return snapshot, disk, "", nil
}

// Explicit Flush repairs graph coverage from the current canonical declaration,
// including handles opened before vector DDL. It uses the maintained rebuild
// when that handle has no current runtime; a stale runtime is never blessed.
func (c *Collection) flushNativeHNSW() error {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	definitions := append([]VectorIndexDefinition(nil), read.meta.VectorIndexes...)
	if err = read.Close(); err != nil {
		return err
	}
	for _, def := range definitions {
		idx := c.registeredVectorIndex(def.Name)
		current, e := c.nativeJSON.Capture(c.name)
		if e != nil {
			return e
		}
		valid := idx != nil && idx.validateNativeHNSWRead(current) == nil
		e = current.Close()
		if e != nil {
			return e
		}
		temporary := false
		if !valid {
			unlock := c.lockVectorIndexMutation()
			idx, e = c.buildNativeHNSW(vectorIndexOptionsFromDefinition(def), false)
			unlock()
			if e != nil {
				return e
			}
			temporary = true
		}
		if temporary || idx.needsNativeAutoPersist() {
			_, e = idx.SaveSnapshot()
		}
		if temporary {
			idx.releaseNativeHNSW()
		}
		if e != nil {
			return e
		}
	}
	return nil
}

func (c *Collection) nativeHNSWMaintenanceNeeded() bool {
	coord := c.collectionSchemaCoordinator()
	return coord != nil && coord.hasNativeVectorIndexes.Load()
}

func (c *Collection) lockNativeHNSWMutationIfNeeded() (func(), bool) {
	if !c.nativeHNSWMaintenanceNeeded() {
		return func() {}, false
	}
	return c.lockVectorIndexMutation(), true
}
func (c *Collection) finishNativeHNSWMutation(ids [][]byte, held bool) error {
	if held {
		return c.reconcileNativeHNSWWithMutationHeld(ids)
	}
	return c.reconcileNativeHNSW(ids)
}
