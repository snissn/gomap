package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
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
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return nil, err
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	meta, err := c.nativeJSON.mutateIndexFamilies(c, c.name, nil, nil, false, &def, "", nil)
	if meta != nil {
		c.collectionSchemaCoordinator().hasNativeVectorIndexes.Store(len(meta.VectorIndexes) != 0)
	}
	return meta, err
}
func (c *Collection) nativeDropVectorIndex(name string) (*CollectionMeta, error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return nil, err
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	meta, err := c.nativeJSON.mutateIndexFamilies(c, c.name, nil, nil, false, nil, name, nil)
	if meta != nil {
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

// A native runtime has one mutable graph and at most two immutable images
// during publication. IDs/vectors alias across those images. Native adjacency
// has one fixed limit+1 backing (the extra reciprocal edge is pruned in place),
// and native node descriptors double explicitly; their complete growth cohorts
// total less than four descriptors per appended node. Legacy growth is unchanged.
func nativeHNSWRowCharge(def VectorIndexDefinition, id []byte) (uint64, error) {
	if def.M <= 0 || def.Dimensions <= 0 {
		return 0, memtable.ErrCOWCapacity
	}
	probe := VectorIndex{name: def.Name, m: def.M}
	level := probe.levelForDocumentID(id)
	var total uint64
	add := func(count, size uint64) bool {
		if count != 0 && size > (^uint64(0)-total)/count {
			return false
		}
		total += count * size
		return true
	}
	allocation := func(count, size uint64) bool {
		if count != 0 && size > ^uint64(0)/count {
			return false
		}
		return add(1, memtable.COWAllocationCharge(count*size))
	}
	// Mutable descriptor growth cohorts plus two exact-length view descriptors.
	if !add(6, uint64(unsafe.Sizeof(vectorIndexNode{}))) ||
		!allocation(uint64(def.Dimensions), 4) ||
		!add(6, memtable.COWAllocationCharge(uint64(len(id)))) ||
		!add(3, memtable.COWAllocationCharge(uint64(level+1)*uint64(unsafe.Sizeof([]vectorIndexNeighbor{})))) {
		return 0, memtable.ErrCOWCapacity
	}
	for layer := 0; layer <= level; layer++ {
		limit := uint64(def.M)
		if layer == 0 {
			if limit > (^uint64(0)-1)/2 {
				return 0, memtable.ErrCOWCapacity
			}
			limit *= 2
		}
		if !allocation(limit+1, uint64(unsafe.Sizeof(vectorIndexNeighbor{}))) ||
			!add(2, memtable.COWAllocationCharge(limit*uint64(unsafe.Sizeof(vectorIndexNeighbor{})))) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	// Current ordinal, dirty node/document, unnotified document and dirty-view
	// maps plus two immutable ordinal maps. Four slots per logical entry bound
	// hash-table occupancy/growth, including control bytes and rounded groups.
	mapSlotBytes := uint64(unsafe.Sizeof(string(""))+unsafe.Sizeof(int(0)))*3 +
		uint64(unsafe.Sizeof(int(0)))*2 + uint64(unsafe.Sizeof(string("")))*2 + 7*8
	if !add(4, mapSlotBytes) {
		return 0, memtable.ErrCOWCapacity
	}
	return total, nil
}

// Construction scratch is one high-water envelope, not one copy per historical
// row. Queue/visited can reach every node; best/output/selection are bounded by
// EF and degree. Eight backing elements per logical element cover append growth
// cohorts; visited uses the maintained doubling policy. No scratch is refunded
// while insertScratch or quantization scratch still refers to it.
func nativeHNSWConstructionCharge(def VectorIndexDefinition, nodes uint64) (uint64, error) {
	return nativeHNSWConstructionChargeForWorkers(def, nodes, 1)
}

func nativeHNSWConstructionChargeForWorkers(def VectorIndexDefinition, nodes, workers uint64) (uint64, error) {
	if def.M <= 0 || def.Dimensions <= 0 || def.EfConstruction <= 0 {
		return 0, memtable.ErrCOWCapacity
	}
	var total uint64
	add := func(count, size uint64) bool {
		if count != 0 && size > (^uint64(0)-total)/count {
			return false
		}
		total += count * size
		return true
	}
	capacity := uint64(64)
	for capacity < nodes {
		if capacity > ^uint64(0)/2 {
			return 0, memtable.ErrCOWCapacity
		}
		capacity *= 2
	}
	degree := uint64(def.M)
	if degree > (^uint64(0)-1)/2 {
		return 0, memtable.ErrCOWCapacity
	}
	degree = degree*2 + 1
	bounded := min(nodes, max(uint64(def.EfConstruction), degree))
	// Queue; visited epochs; best, liveBest, output, scored/rejected/selected
	// candidates; selected neighbor ordinals; quantization/conversion vector.
	if !add(nodes, 8*uint64(unsafe.Sizeof(vectorIndexCandidate{}))) ||
		!add(capacity, 2*uint64(unsafe.Sizeof(uint32(0)))) ||
		!add(bounded, 8*6*uint64(unsafe.Sizeof(vectorIndexCandidate{}))) ||
		!add(degree, 2*uint64(unsafe.Sizeof(int(0)))) || !add(uint64(def.Dimensions), 8) {
		return 0, memtable.ErrCOWCapacity
	}
	// Native float32 rows keep their per-node backing. The maintained int8
	// fold temporarily dequantizes a whole delta before inserting its rows.
	if def.Encoding == VectorIndexEncodingInt8 {
		if nodes != 0 && uint64(def.Dimensions) > ^uint64(0)/nodes {
			return 0, memtable.ErrCOWCapacity
		}
		if !add(nodes*uint64(def.Dimensions), 4) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	// Frozen-prefix planning/reciprocal pruning owns independent worker scratch.
	// The same runtime peak grows at the existing fold boundary before workers
	// start; the serial builder pays for one workspace.
	if degree > ^uint64(0)/degree || !add(degree*degree, 4) {
		return 0, memtable.ErrCOWCapacity
	}
	if workers == 0 || total > ^uint64(0)/workers {
		return 0, memtable.ErrCOWCapacity
	}
	return memtable.COWAllocationCharge(total * workers), nil
}

func nativeHNSWAdmissionDelta(def VectorIndexDefinition, id []byte, nodes, previousPeak uint64) (retained, peak, delta uint64, err error) {
	retained, err = nativeHNSWRowCharge(def, id)
	if err != nil {
		return
	}
	peak, err = nativeHNSWConstructionCharge(def, nodes)
	if err != nil {
		return
	}
	peak = max(peak, previousPeak)
	if retained > ^uint64(0)-(peak-previousPeak) {
		err = memtable.ErrCOWCapacity
		return
	}
	delta = retained + peak - previousPeak
	return
}

// Column offset/presence/chunk descriptors grow even for a missing value.
// Eight payload cohorts bound append growth; two immutable tail images add two
// copies. Full immutable chunks alias their original data rather than copying it.
func nativeHNSWScalarCharge(definitions []IndexDefinition, row map[string][]byte) (uint64, error) {
	var total uint64
	for _, def := range definitions {
		n := uint64(len(row[def.Name]))
		if total > ^uint64(0)-64 || n > (^uint64(0)-total-64)/10 {
			return 0, memtable.ErrCOWCapacity
		}
		total += 64 + n*10
	}
	return total, nil
}

// Called under the graph lock before the maintained live-delta fold allocates
// frozen-prefix plans or starts workers. It extends the same receipt and peak.
func (idx *VectorIndex) admitNativeHNSWFoldLocked() error {
	if !idx.nativeHNSWOwned || idx.liveDelta == nil {
		return nil
	}
	def := VectorIndexDefinition{Name: idx.name, M: idx.m, Dimensions: idx.dimensions, EfConstruction: idx.efConstruction, Encoding: idx.encoding}
	nodes := uint64(len(idx.nodes)) + uint64(len(idx.liveDelta.nodes))
	workers := max(idx.constructionWorkerCount(minInt(nativeVectorFrozenPrefixBatchWidth, len(idx.liveDelta.nodes))), idx.constructionWorkerCount(vectorIndexReciprocalLinkWorkerCount(idx.maxNeighborsForLayer(0))))
	peak, err := nativeHNSWConstructionChargeForWorkers(def, nodes, uint64(maxInt(1, workers)))
	if err != nil {
		return err
	}
	if peak <= idx.nativeHNSWConstructionPeak {
		return nil
	}
	delta := peak - idx.nativeHNSWConstructionPeak
	if idx.nativeHNSWLease == nil || delta > ^uint64(0)-idx.nativeHNSWCharge {
		return memtable.ErrCOWCapacity
	}
	if err = idx.nativeHNSWLease.Resize(idx.nativeHNSWCharge + delta); err != nil {
		return err
	}
	idx.nativeHNSWCharge += delta
	idx.nativeHNSWConstructionPeak = peak
	return nil
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
	idx.nativeHNSWCharge, idx.nativeHNSWConstructionPeak, idx.nativeHNSWIDBytes = 0, 0, 0
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
	// Stored templates resolve through this original cut and its maintained
	// admitted materializer, just as explicit native document insertion does.
	materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat), nativeRead: read}
	err = walkNativeHNSWRows(read, admit, scratch, func(id, doc []byte) error {
		vector, ok, e := vectorFromStoredDocument(materializer, doc, index.fieldPath)
		if e != nil {
			return e
		}
		if !ok {
			return nil
		}
		_, peak, rowCharge, e := nativeHNSWAdmissionDelta(def, id, uint64(len(index.nodes)+1), index.nativeHNSWConstructionPeak)
		if e != nil {
			return e
		}
		if e = admit(rowCharge); e != nil {
			return e
		}
		index.nativeHNSWConstructionPeak = peak
		scalar, e := index.nativeScalarRow(materializer, doc)
		if e != nil {
			return e
		}
		scalarCharge, e := nativeHNSWScalarCharge(index.scalarDefinitions, scalar)
		if e != nil {
			return e
		}
		if e = admit(scalarCharge); e != nil {
			return e
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

func (c *Collection) installNativeHNSW(index *VectorIndex, expected *NativeJSONRead) error {
	return c.installNativeHNSWInto(index, expected, nil)
}

// receiver, when nonnil, is use-locked by the caller. The candidate remains
// privately admitted until validation/manager registration have both succeeded.
func (c *Collection) installNativeHNSWInto(index *VectorIndex, expected *NativeJSONRead, receiver *VectorIndex) (resultErr error) {
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
	installed := index
	if receiver != nil {
		installed = receiver
	}
	coord.nativeHNSWIndexes[index.name] = installed
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
		if coord.nativeHNSWIndexes[index.name] == installed {
			if old == nil {
				delete(coord.nativeHNSWIndexes, index.name)
			} else {
				coord.nativeHNSWIndexes[index.name] = old
			}
		}
		coord.nativeHNSWIndexesMu.Unlock()
		return err
	}
	if receiver != nil {
		receiver.swapNativeHNSW(index)
		index.releaseNativeHNSW() // the candidate now owns the superseded payload
	}
	if old != nil && old != installed {
		old.releaseNativeHNSW()
	}
	return nil
}

func (idx *VectorIndex) validateNativeHNSWRead(read *NativeJSONRead) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	def, ok := findVectorIndex(read.meta.VectorIndexes, idx.name)
	if !ok || idx.nativeHNSWLease == nil || idx.nativeHNSWSchemaState == nil || !collectionMetaValuesEqual(read.meta, *idx.nativeHNSWSchemaState.meta) || def.SchemaGeneration != idx.nativeHNSWSchema || read.collection.incarnation != idx.nativeHNSWIncarnation {
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
	// The primary mutation is already accepted. On any maintenance failure,
	// retire every potentially partial/stale payload, preserving registration
	// and receiver identity for explicit canonical Rebuild recovery. Per-index
	// use locks must be released before this existing retirement boundary.
	defer func() {
		if resultErr != nil {
			for _, idx := range c.collectionSchemaCoordinator().nativeHNSWRuntimeList() {
				idx.releaseNativeHNSW()
			}
		}
	}()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if !collectionMetaHasNativeVectorIndexes(read.meta) {
		return nil
	}
	scratchLease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return err
	}
	defer scratchLease.Close()
	scratch := nativeHNSWPeakAdmission(scratchLease)
	for _, def := range read.meta.VectorIndexes {
		if !vectorIndexDefinitionUsesNativeRuntime(def) {
			continue
		}
		idx := c.registeredVectorIndex(def.Name)
		if idx == nil {
			if _, err = c.buildNativeHNSW(vectorIndexOptionsFromDefinition(def), true); err != nil {
				return err
			}
			continue
		}
		if err = idx.reconcileNativeHNSWRows(read, def, ids, scratch); err != nil {
			return err
		}
	}
	return nil
}

// Keep only this receiver use-locked. The caller's shared mutation gate excludes
// registry replacement; its failure cleanup runs after this lock is released.
func (idx *VectorIndex) reconcileNativeHNSWRows(read *NativeJSONRead, def VectorIndexDefinition, ids [][]byte, scratch func(uint64) error) error {
	idx.nativeHNSWUseMu.Lock()
	defer idx.nativeHNSWUseMu.Unlock()
	generation, valid := idx.sourceDocumentCoverage()
	if valid && generation == read.collection.documentGeneration {
		return nil
	}
	if !valid || generation+1 != read.collection.documentGeneration {
		idx.invalidateSourceDocumentRoots()
		return errVectorIndexStaleDocumentGeneration
	}
	var err error
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
		materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat), nativeRead: read}
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
			if e = idx.admitNativeHNSWRowLocked(def, id, scalarRow); e != nil {
				err = e
				break
			}
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
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return status, err
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	return idx.saveNativeHNSWWithMutationHeld()
}

func (idx *VectorIndex) saveNativeHNSWWithMutationHeld() (status VectorIndexLoadStatus, resultErr error) {
	c := idx.collection
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
	imagePlan, e := idx.nativeHNSWImagePlan()
	if e != nil {
		return status, e
	}
	persistLease, e := read.capture.NativeAllocator().AdmitNativeBytes(imagePlan.charge)
	if e != nil {
		return status, e
	}
	defer persistLease.Close()
	if err = idx.foldLiveDeltaForPersistence(); err != nil {
		return status, err
	}
	rows, disk, sequence, err := idx.encodeNativeHNSWImage(domain.interpretation+1, imagePlan)
	if err != nil {
		return status, err
	}
	// Encoding has returned: its private buffer, edge conversion arrays and
	// record construction aliases have ended. Keep the actual original output
	// capacities and downstream publication controls on the SAME receipt;
	// publication separately admits its COW copy.
	retained, err := nativeHNSWImageRetainedCharge(rows)
	admitted := memtable.COWAllocationCharge(imagePlan.charge)
	if err != nil || imagePlan.control > ^uint64(0)-retained {
		return status, memtable.ErrCOWCapacity
	}
	retained += imagePlan.control
	if retained > admitted {
		return status, memtable.ErrCOWCapacity
	}
	if err = persistLease.ShrinkNativeBytes(admitted-retained, retained); err != nil {
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
	return c.loadNativeHNSWAtRead(opts, read)
}

// Caller owns the existing mutation gate and the borrowed data capture.
func (c *Collection) loadNativeHNSWAtRead(opts VectorIndexOptions, read *NativeJSONRead) (index *VectorIndex, status VectorIndexLoadStatus, resultErr error) {
	def, ok := findVectorIndex(read.meta.VectorIndexes, vectorIndexOptionName(opts))
	if !ok {
		return nil, status, errVectorIndexNotDeclared
	}
	var err error
	def, err = normalizeVectorIndexDefinition(def)
	if err != nil {
		return nil, status, err
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
	meta, disk, reason, err := readNativeHNSWMeta(read, def, admit)
	if err != nil || reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, err
	}
	// Keep the ENTIRE metadata-phase promise: decoded metadata strings and
	// preparation may be retained by the final runtime. Graph-only callbacks and
	// decode temporaries end in the private success-return scope below.
	phaseFloor := charge
	index, graphDisk, reason, err := c.loadNativeHNSWGraphPhase(read, def, meta, &charge, admit)
	if err != nil || reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, err
	}
	def.Dimensions = meta.Dimensions
	retained := false
	owned := index
	defer func() {
		if !retained {
			owned.releaseNativeHNSW()
		}
	}()
	if graphDisk > math.MaxInt64-disk {
		return nil, status, memtable.ErrCOWCapacity
	}
	disk += graphDisk
	if err = completeNativeHNSWGraphPhase(lease, &charge, phaseFloor); err != nil {
		return nil, status, err
	}
	read.collection.schema.retain()
	index.nativeHNSWSchemaState = read.collection.schema
	index.nativeHNSWIncarnation = read.collection.incarnation
	index.nativeHNSWSchema = def.SchemaGeneration
	index.nativeHNSWVersion = domain.interpretation
	index.setNativePersistent(true)
	index.recordNativeDefinition(def)
	index.recordLoadedSnapshot(domain.baseRoot, disk)
	if err = populateNativeHNSWScalarRows(index, read, admit, index); err != nil {
		return nil, status, err
	}
	// The codec published its graph view before scalar reconstruction. Keep
	// that incomplete view private and rebuild both planes before first use.
	index.searchViewForceFull = true
	index.searchViewCurrent.Store(false)
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

func readNativeHNSWMeta(read *NativeJSONRead, def VectorIndexDefinition, admit func(uint64) error) (meta vectorIndexPersistMeta, disk int64, reason string, resultErr error) {
	name := "vector-index/" + def.Name
	domain, ok := read.collection.domains.Get(name)
	if !ok {
		return meta, 0, vectorIndexFallbackMissingGraphRoot, nil
	}
	raw, err := read.namedDomainGetAdmitted(name, []byte(vectorIndexNativeKeyMeta), admit)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return meta, int64(len(raw)), vectorIndexFallbackMissingGraphRoot, nil
	}
	if err != nil {
		return meta, int64(len(raw)), "", err
	}
	var metaPlan nativeHNSWDecodePlan
	if e := metaPlan.allocation(uint64(len(raw)), 16); e != nil {
		return meta, int64(len(raw)), "", e
	}
	if e := admit(metaPlan.charge); e != nil {
		return meta, int64(len(raw)), "", e
	}
	if _, reason, e := planNativeHNSWDecode([]byte(vectorIndexNativeKeyMeta), raw, admit); e != nil || reason != "" {
		return meta, int64(len(raw)), reason, e
	}
	if err = json.Unmarshal(raw, &meta); err != nil {
		return meta, int64(len(raw)), "", err
	}
	if meta.NativeLogicalVersion != domain.interpretation || meta.SourceDocumentGeneration != read.collection.documentGeneration || meta.SourceDocumentGenerationVersion != vectorIndexDocumentGenerationVersion {
		return meta, int64(len(raw)), vectorIndexFallbackStaleDocumentRoot, nil
	}
	// Persisted tuning cannot change the canonical graph's allocation bounds.
	// The maintained decoder adopts these fields, so authenticate them before it
	// allocates any graph storage or installs a runtime.
	if meta.Name != def.Name || meta.Field != def.Field || meta.Metric != def.Metric || meta.Encoding != def.Encoding ||
		meta.M != def.M || meta.EfConstruction != def.EfConstruction || meta.EfSearch != def.EfSearch ||
		meta.Dimensions <= 0 || (def.Dimensions != 0 && meta.Dimensions != def.Dimensions) ||
		!(meta.RebuildDeletedRatio > 0 && meta.RebuildDeletedRatio <= 1) {
		return meta, int64(len(raw)), vectorIndexFallbackMetaMismatch, nil
	}
	if meta.PartitionLive != nil {
		return meta, int64(len(raw)), vectorIndexFallbackMetaMismatch, nil
	}
	return meta, int64(len(raw)), "", nil
}

func readNativeHNSWSnapshot(read *NativeJSONRead, def VectorIndexDefinition, admit func(uint64) error) (snapshot vectorIndexPersistSnapshot, disk int64, reason string, resultErr error) {
	normalized, normalizeErr := normalizeVectorIndexDefinition(def)
	if normalizeErr != nil {
		return snapshot, 0, vectorIndexFallbackMetaMismatch, normalizeErr
	}
	def = normalized

	name := "vector-index/" + def.Name
	_, ok := read.collection.domains.Get(name)
	if !ok {
		return snapshot, 0, vectorIndexFallbackMissingGraphRoot, nil
	}

	meta, _, reason, err := readNativeHNSWMeta(read, def, admit)
	if err != nil || reason != "" {
		return snapshot, disk, reason, err
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
		disk += int64(len(value))
		if !bytes.Equal(key, []byte(vectorIndexNativeKeyMeta)) {
			plan, reason, e := planNativeHNSWDecode(key, value, admit)
			if e != nil || reason != "" {
				return snapshot, disk, reason, e
			}
			node, edge := plan.seeds()
			if reason := appendVectorIndexNativeSnapshotSeededRecordV2(&snapshot, nodes, &maxNode, key, value, node, edge); reason != "" {
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
		if !vectorIndexDefinitionUsesNativeRuntime(def) {
			continue
		}
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

// This flag is deliberately coarse across handle/DDL changes. The captured
// declaration inside reconcile/flush decides which strategy owns maintenance.
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

// Every public native graph mutation uses the established mutation -> use ->
// graph lock order. Captured canonical rows and scratch live until cleanup.
func (idx *VectorIndex) ownedNativeHNSWRead() (*NativeJSONRead, func(), error) {
	c := idx.collection
	unlock := c.lockVectorIndexMutation()
	idx.nativeHNSWUseMu.Lock()
	cleanup := func() { idx.nativeHNSWUseMu.Unlock(); unlock() }
	if c.nativeJSON.db.IsClosing() {
		cleanup()
		return nil, nil, backenddb.ErrClosed
	}
	if c.registeredVectorIndex(idx.name) != idx {
		cleanup()
		return nil, nil, errVectorIndexStaleRuntime
	}
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if err = idx.validateNativeHNSWRead(r); err != nil {
		r.Close()
		cleanup()
		return nil, nil, err
	}
	return r, func() { r.Close(); cleanup() }, nil
}

func (idx *VectorIndex) admitNativeHNSWRowLocked(def VectorIndexDefinition, id []byte, scalar map[string][]byte) error {
	nodes := uint64(len(idx.nodes) + 1)
	if idx.liveDelta != nil {
		nodes += uint64(len(idx.liveDelta.nodes))
	}
	_, peak, charge, err := nativeHNSWAdmissionDelta(def, id, nodes, idx.nativeHNSWConstructionPeak)
	if err != nil {
		return err
	}
	workers := uint64(idx.constructionWorkerCount(vectorIndexReciprocalLinkWorkerCount(idx.maxNeighborsForLayer(0))))
	concurrentPeak, err := nativeHNSWConstructionChargeForWorkers(def, nodes, workers)
	if err != nil {
		return err
	}
	if concurrentPeak > peak {
		if concurrentPeak-peak > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		charge += concurrentPeak - peak
		peak = concurrentPeak
	}
	scalarCharge, err := nativeHNSWScalarCharge(idx.scalarDefinitions, scalar)
	if err != nil || scalarCharge > ^uint64(0)-charge {
		return memtable.ErrCOWCapacity
	}
	charge += scalarCharge
	if err != nil {
		return err
	}
	if idx.nativeHNSWLease == nil || charge > ^uint64(0)-idx.nativeHNSWCharge || uint64(len(id)) > ^uint64(0)-idx.nativeHNSWIDBytes {
		return memtable.ErrCOWCapacity
	}
	if err = idx.nativeHNSWLease.Resize(idx.nativeHNSWCharge + charge); err != nil {
		return fmt.Errorf("collections: native vector row admission: %w", err)
	}
	idx.nativeHNSWCharge += charge
	idx.nativeHNSWConstructionPeak = peak
	idx.nativeHNSWIDBytes += uint64(len(id))
	return nil
}

func (idx *VectorIndex) insertNativeHNSWDocument(id []byte) error {
	read, done, err := idx.ownedNativeHNSWRead()
	if err != nil {
		return err
	}
	defer done()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return err
	}
	defer lease.Close()
	scratch := nativeHNSWPeakAdmission(lease)
	doc, err := read.namedDomainGetAdmitted("primary", id, scratch)
	if err != nil {
		return err
	}
	if err = scratch(uint64(len(doc))); err != nil {
		return err
	}
	materializer := &StoredDocumentJSONMaterializer{documentFormat: normalizedDocumentFormat(read.meta.Options.DocumentFormat), nativeRead: read}
	vector, scalar, err := idx.parseStoredVectorRow(materializer, id, doc)
	if err != nil {
		return err
	}
	def, _ := findVectorIndex(read.meta.VectorIndexes, idx.name)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.bufferedStoredVectorMatchesLocked(id, vector, scalar) {
		return nil
	}
	if vector != nil {
		if err = idx.admitNativeHNSWRowLocked(def, id, scalar); err != nil {
			return err
		}
	}
	return idx.insertParsedStoredDocumentLocked(id, vector, scalar, true)
}

func (idx *VectorIndex) tombstoneNativeHNSWDocument(id []byte) {
	_, done, err := idx.ownedNativeHNSWRead()
	if err != nil {
		return
	}
	defer done()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.liveDeltaActiveLocked() {
		idx.tombstoneLiveDocumentLocked(id)
	} else {
		idx.tombstoneDocumentIDLocked(id)
	}
	idx.acknowledgeSearchViewStateLocked()
}

// Rebuild and explicit re-registration keep the caller's receiver. The fresh
// maintained builder owns all replacement arrays/schema; no mutex is copied and
// no graph backing is cloned after admission. The candidate receives old custody.
func (c *Collection) rebuildNativeHNSWReceiver(idx *VectorIndex, register bool) error {
	if idx.collection == nil || idx.collection.nativeJSON == nil || idx.collection.nativeJSON.db != c.nativeJSON.db || idx.collection.name != c.name {
		return errVectorIndexStaleRuntime
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	idx.nativeHNSWUseMu.Lock()
	defer idx.nativeHNSWUseMu.Unlock()
	current := c.registeredVectorIndex(idx.name)
	if (current != idx && !register) || (current != nil && current != idx) {
		return errVectorIndexStaleRuntime
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	defer read.Close()
	def, ok := findVectorIndex(read.meta.VectorIndexes, idx.name)
	domain, hasDomain := read.collection.domains.Get("vector-index/" + idx.name)
	if !ok || !hasDomain || !vectorIndexDefinitionUsesNativeRuntime(def) || read.collection.incarnation != idx.nativeHNSWIncarnation || def.SchemaGeneration != idx.nativeHNSWSchema || domain.interpretation != idx.nativeHNSWVersion || idx.validateNativeSnapshotDefinition(def) != "" {
		return errVectorIndexStaleRuntime
	}
	if idx.nativeHNSWLease != nil {
		if err = idx.validateNativeHNSWRead(read); err != nil {
			return err
		}
		if register && current == idx {
			return nil
		}
	}
	start := time.Now()
	options := vectorIndexOptionsFromDefinition(def)
	options.RebuildDeletedRatio = idx.rebuildDeletedRatio
	candidate, err := c.buildNativeHNSW(options, false)
	if err != nil {
		return err
	}
	defer candidate.releaseNativeHNSW()
	candidate.lastRebuildDuration = collectionObservedElapsedSince(start)
	return c.installNativeHNSWInto(candidate, read, idx)
}

func (c *Collection) unregisterNativeHNSW(name string) {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	coord := c.collectionSchemaCoordinator()
	coord.nativeHNSWIndexesMu.Lock()
	old := coord.nativeHNSWIndexes[name]
	delete(coord.nativeHNSWIndexes, name)
	coord.nativeHNSWIndexesMu.Unlock()
	if old != nil {
		old.releaseNativeHNSW()
	}
	if c.manager != nil && !c.hasNativePersistentVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
}

// The use lock excludes admitted queries and mutators. Both receipts remain
// live throughout the swap; releasing the temporary then retires exactly old.
func (idx *VectorIndex) swapNativeHNSW(other *VectorIndex) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.nativeHNSWSchemaState, other.nativeHNSWSchemaState = other.nativeHNSWSchemaState, idx.nativeHNSWSchemaState
	idx.nativeHNSWLease, other.nativeHNSWLease = other.nativeHNSWLease, idx.nativeHNSWLease
	idx.nativeHNSWCharge, other.nativeHNSWCharge = other.nativeHNSWCharge, idx.nativeHNSWCharge
	idx.nativeHNSWConstructionPeak, other.nativeHNSWConstructionPeak = other.nativeHNSWConstructionPeak, idx.nativeHNSWConstructionPeak
	idx.nativeHNSWIDBytes, other.nativeHNSWIDBytes = other.nativeHNSWIDBytes, idx.nativeHNSWIDBytes
	idx.nativeHNSWIncarnation, other.nativeHNSWIncarnation = other.nativeHNSWIncarnation, idx.nativeHNSWIncarnation
	idx.nativeHNSWSchema, other.nativeHNSWSchema = other.nativeHNSWSchema, idx.nativeHNSWSchema
	idx.nativeHNSWVersion, other.nativeHNSWVersion = other.nativeHNSWVersion, idx.nativeHNSWVersion
	idx.nodes, other.nodes = other.nodes, idx.nodes
	idx.vectorRows, other.vectorRows = other.vectorRows, idx.vectorRows
	idx.constructionRowsFixed, other.constructionRowsFixed = other.constructionRowsFixed, idx.constructionRowsFixed
	idx.currentNode, other.currentNode = other.currentNode, idx.currentNode
	idx.entry, other.entry = other.entry, idx.entry
	idx.maxLevel, other.maxLevel = other.maxLevel, idx.maxLevel
	idx.insertScratch, other.insertScratch = other.insertScratch, idx.insertScratch
	idx.liveDelta, other.liveDelta = other.liveDelta, idx.liveDelta
	idx.scalarDefinitions, other.scalarDefinitions = other.scalarDefinitions, idx.scalarDefinitions
	idx.scalarRuntimes, other.scalarRuntimes = other.scalarRuntimes, idx.scalarRuntimes
	idx.scalarColumns, other.scalarColumns = other.scalarColumns, idx.scalarColumns
	idx.mutationSeq, other.mutationSeq = other.mutationSeq, idx.mutationSeq
	idx.sourceDocumentRootsValid, other.sourceDocumentRootsValid = other.sourceDocumentRootsValid, idx.sourceDocumentRootsValid
	idx.unnotifiedDocumentIDs, other.unnotifiedDocumentIDs = other.unnotifiedDocumentIDs, idx.unnotifiedDocumentIDs
	idx.sourceDocumentGeneration, other.sourceDocumentGeneration = other.sourceDocumentGeneration, idx.sourceDocumentGeneration
	idx.sourceDocumentAccountedGeneration, other.sourceDocumentAccountedGeneration = other.sourceDocumentAccountedGeneration, idx.sourceDocumentAccountedGeneration
	idx.sourceDocumentAccountedGenerationValid, other.sourceDocumentAccountedGenerationValid = other.sourceDocumentAccountedGenerationValid, idx.sourceDocumentAccountedGenerationValid
	idx.sourceDocumentState, other.sourceDocumentState = other.sourceDocumentState, idx.sourceDocumentState
	idx.sourceDocumentStateValid, other.sourceDocumentStateValid = other.sourceDocumentStateValid, idx.sourceDocumentStateValid
	idx.searchViewDirty, other.searchViewDirty = other.searchViewDirty, idx.searchViewDirty
	idx.trackSearchViewDirty, other.trackSearchViewDirty = other.trackSearchViewDirty, idx.trackSearchViewDirty
	idx.searchViewForceFull, other.searchViewForceFull = other.searchViewForceFull, idx.searchViewForceFull
	idx.searchViewAcknowledged, other.searchViewAcknowledged = other.searchViewAcknowledged, idx.searchViewAcknowledged
	idx.searchViewAcknowledgedMutationSeq, other.searchViewAcknowledgedMutationSeq = other.searchViewAcknowledgedMutationSeq, idx.searchViewAcknowledgedMutationSeq
	idx.searchViewAcknowledgedGeneration, other.searchViewAcknowledgedGeneration = other.searchViewAcknowledgedGeneration, idx.searchViewAcknowledgedGeneration
	idx.persistedEpoch, other.persistedEpoch = other.persistedEpoch, idx.persistedEpoch
	idx.fullSnapshotBaseEpoch, other.fullSnapshotBaseEpoch = other.fullSnapshotBaseEpoch, idx.fullSnapshotBaseEpoch
	idx.persistedBytesDisk, other.persistedBytesDisk = other.persistedBytesDisk, idx.persistedBytesDisk
	idx.persistedSnapshotDirty, other.persistedSnapshotDirty = other.persistedSnapshotDirty, idx.persistedSnapshotDirty
	idx.dirtyMeta, other.dirtyMeta = other.dirtyMeta, idx.dirtyMeta
	idx.dirtyNodes, other.dirtyNodes = other.dirtyNodes, idx.dirtyNodes
	idx.dirtyDocs, other.dirtyDocs = other.dirtyDocs, idx.dirtyDocs
	idx.lastRebuildDuration, other.lastRebuildDuration = other.lastRebuildDuration, idx.lastRebuildDuration
	idx.insertQuantScratch, other.insertQuantScratch = other.insertQuantScratch, idx.insertQuantScratch
	asearchView := idx.searchView.Load()
	idx.searchView.Store(other.searchView.Load())
	other.searchView.Store(asearchView)
	asearchViewSpare := idx.searchViewSpare.Load()
	idx.searchViewSpare.Store(other.searchViewSpare.Load())
	other.searchViewSpare.Store(asearchViewSpare)
	asearchViewCurrent := idx.searchViewCurrent.Load()
	idx.searchViewCurrent.Store(other.searchViewCurrent.Load())
	other.searchViewCurrent.Store(asearchViewCurrent)
	aliveDeltaEnabled := idx.liveDeltaEnabled.Load()
	idx.liveDeltaEnabled.Store(other.liveDeltaEnabled.Load())
	other.liveDeltaEnabled.Store(aliveDeltaEnabled)
}

// Dispatch from a captured declared role, preserving the separate source-import
// graph route. A facade's nil legacy DB does not describe its vector capability.
func (c *Collection) nativeOrdinaryHNSWRole(name string) (bool, error) {
	if err := ValidateIndexName(name); err != nil {
		return false, err
	}
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return false, err
	}
	defer r.Close()
	def, ok := findVectorIndex(r.meta.VectorIndexes, name)
	if !ok {
		return false, ErrIndexNotFound
	}
	return r.meta.Options.ColumnStore == nil && vectorIndexDefinitionUsesNativeRuntime(def), nil
}

func (c *Collection) rebuildNativeOrdinaryHNSW(name string) (VectorIndexStatus, error) {
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return VectorIndexStatus{}, err
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	def, ok := findVectorIndex(r.meta.VectorIndexes, name)
	if !ok || r.meta.Options.ColumnStore != nil || !vectorIndexDefinitionUsesNativeRuntime(def) {
		r.Close()
		return VectorIndexStatus{}, errVectorIndexNotDeclared
	}
	options := vectorIndexOptionsFromDefinition(def)
	r.Close()
	start := time.Now()
	idx, err := c.buildNativeHNSW(options, true)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	saved, err := idx.saveNativeHNSWWithMutationHeld()
	if err != nil {
		return VectorIndexStatus{}, err
	}
	// Save just validated and published this exact runtime under the mutation
	// gate. Assemble its result from a fresh authenticated capture, rather than
	// decoding a second graph beside it. Explicit VectorIndexStatus continues
	// to inspect the persisted image independently.
	out, err := c.nativeOrdinaryHNSWStatusWithMutationHeld(name, false)
	if err != nil {
		return out, err
	}
	idx.nativeHNSWUseMu.RLock()
	idx.mu.RLock()
	clean := saved.Loaded && idx.nativeHNSWLease != nil && idx.nativeHNSWVersion == saved.LogicalVersion &&
		!idx.dirtyMeta && !idx.persistedSnapshotDirty && idx.persistedBytesDisk == saved.BytesDisk
	idx.mu.RUnlock()
	idx.nativeHNSWUseMu.RUnlock()
	if c.registeredVectorIndex(name) != idx || !clean || !out.Loaded || !out.NativeRootLoaded ||
		out.Stats.SnapshotDirty || out.RebuildNeeded || saved.BytesDisk <= 0 || out.Stats.BytesDisk != saved.BytesDisk {
		return out, errVectorIndexStaleNativeRoot
	}
	out.NativeRootBytes = saved.BytesDisk
	out.Duration = collectionObservedElapsedSince(start)
	return out, nil
}

func (c *Collection) nativeOrdinaryHNSWStatus(name string, inspect bool) (VectorIndexStatus, error) {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	return c.nativeOrdinaryHNSWStatusWithMutationHeld(name, inspect)
}

func (c *Collection) nativeOrdinaryHNSWStatusWithMutationHeld(name string, inspect bool) (out VectorIndexStatus, resultErr error) {
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, r.Close()) }()
	def, ok := findVectorIndex(r.meta.VectorIndexes, name)
	if !ok {
		return out, ErrIndexNotFound
	}
	// Metadata returned by this diagnostic is external caller-owned output.
	meta := r.meta.copy()
	def, _ = findVectorIndex(meta.VectorIndexes, name)
	out = VectorIndexStatus{Definition: def, Name: name, Strategy: def.Strategy, State: VectorIndexStateNativeRuntime, Reason: VectorIndexReasonNativeRuntime, RootName: collectionVectorIndexRootName(c.name, name)}
	domain, ok := r.collection.domains.Get("vector-index/" + name)
	if !ok {
		out.ExactFallbackReason = vectorIndexFallbackMissingGraphRoot
		out.RebuildNeeded = true
		return out, nil
	}
	out.RootID = domain.baseRoot
	validRuntime := false
	if idx := c.registeredVectorIndex(name); idx != nil {
		out.Registered = true
		idx.nativeHNSWUseMu.RLock()
		validRuntime = idx.validateNativeHNSWRead(r) == nil
		if validRuntime {
			out.Stats = idx.Stats()
			out.Loaded = true
		}
		idx.nativeHNSWUseMu.RUnlock()
	}
	if !inspect {
		out.NativeRootLoaded = domain.sourceGeneration == r.collection.documentGeneration
		out.RebuildNeeded = out.Stats.RebuildNeeded || out.Stats.SnapshotDirty || !out.NativeRootLoaded
		return out, nil
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(64 << 10)
	if err != nil {
		return out, err
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
	snapshot, disk, reason, err := readNativeHNSWSnapshot(r, def, admit)
	if err != nil {
		return out, err
	}
	if reason != "" {
		out.ExactFallbackReason = reason
		out.RebuildNeeded = true
		return out, nil
	}
	for _, row := range snapshot.Nodes {
		n, e := nativeHNSWRowCharge(def, []byte(row.DocumentID))
		if e != nil {
			return out, e
		}
		if e = admit(n); e != nil {
			return out, e
		}
	}
	peak, err := nativeHNSWConstructionCharge(def, uint64(len(snapshot.Nodes)))
	if err != nil {
		return out, err
	}
	if err = admit(peak); err != nil {
		return out, err
	}
	probe, err := newVectorIndexWithMeta(c, vectorIndexOptionsFromDefinition(def), r.meta)
	if err != nil {
		return out, err
	}
	if reason = probe.loadPersistSnapshot(snapshot); reason != "" {
		out.ExactFallbackReason = reason
		out.RebuildNeeded = true
		return out, nil
	}
	probe.recordLoadedSnapshot(domain.baseRoot, disk)
	out.NativeRootLoaded = true
	out.NativeRootBytes = disk
	out.Loaded = true
	if !validRuntime {
		out.Stats = probe.Stats()
	}
	out.RebuildNeeded = out.Stats.RebuildNeeded || out.Stats.SnapshotDirty
	return out, nil
}
