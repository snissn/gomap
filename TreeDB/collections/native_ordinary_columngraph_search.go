package collections

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// A native prepared pack has the same cache and independent file handles as
// the legacy pack. Only acquisition authority differs: the caller's original
// complete logical cut replaces a new current-catalog snapshot.
func nativeColumnGraphPreparedIdentityAtRead(read *NativeJSONRead) (nativeColumnGraphPreparedIdentity, error) {
	if read == nil || read.collection == nil || read.database == nil || read.capture.Snapshot() == nil || !nativeOrdinaryColumnGraphMeta(read.meta) {
		return nativeColumnGraphPreparedIdentity{}, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := read.collection.domains.Get("column/manifest")
	primary, primaryOK := read.collection.domains.Get("primary")
	// Root-only graph validation cannot substitute an older physical graph for
	// a logical manifest replacement or an unmaterialized typed producer.
	if !ok || !primaryOK || manifest.baseRoot == 0 || manifest.pending != nil || manifest.captured != nil || manifest.typed != nil || manifest.privateSource != nil || primary.typed != nil || primary.privateSource != nil {
		return nativeColumnGraphPreparedIdentity{}, fmt.Errorf("%w: column_graph source has unmaterialized native changes", ErrVectorIndexSearchUnavailable)
	}
	cfg := read.meta.Options.ColumnStore
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return nativeColumnGraphPreparedIdentity{}, ErrVectorIndexSearchUnavailable
	}
	return nativeColumnGraphPreparedIdentity{native: true, incarnation: read.collection.incarnation, documentGeneration: read.collection.documentGeneration, logicalSequence: read.capture.LogicalSequence(), manifestRoot: manifest.baseRoot, manifestVersion: manifest.interpretation, manifestGeneration: cfg.ActiveManifest.Generation, manifestChecksum: cfg.ActiveManifest.Checksum}, nil
}

func (c *Collection) warmNativeColumnGraphPreparedSearch(opts VectorIndexSearchOptions) (response VectorIndexSearchResponse, resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return response, fmt.Errorf("native prepared search capture: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	prepared, response, stats, err := c.acquireNativeColumnGraphPreparedSearchAtRead(opts, read)
	if err != nil {
		return response, err
	}
	prepared.routeStats.apply(&response.Stats)
	stats.apply(&response.Stats)
	return response, nil
}

func (c *Collection) acquireNativeColumnGraphPreparedSearchAtRead(opts VectorIndexSearchOptions, read *NativeJSONRead) (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, collectionVectorIndexPreparedSearchAcquireStats, error) {
	var response VectorIndexSearchResponse
	var stats collectionVectorIndexPreparedSearchAcquireStats
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return nil, response, stats, err
	}
	if opts.MaxDecodedBlocks < 0 {
		return nil, response, stats, errors.New("collections: MaxDecodedBlocks must be >= 0")
	}
	mode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		return nil, response, stats, err
	}
	sm, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		return nil, response, stats, err
	}
	if mode != columnVectorGraphNativeSearchQueryModeExact || !columnHNSWSearchPackStatsModeSupportedForSearch(sm) {
		return nil, response, stats, ErrVectorIndexSearchUnavailable
	}
	identity, err := nativeColumnGraphPreparedIdentityAtRead(read)
	if err != nil {
		return nil, response, stats, err
	}
	def, ok := findVectorIndex(read.meta.VectorIndexes, opts.IndexName)
	if !ok {
		return nil, response, stats, ErrIndexNotFound
	}
	if def.Strategy != VectorIndexStrategyColumnGraph || def.Metric != VectorMetricCosine || def.Representation != "" || len(def.QuantizedIndexes) != 0 {
		return nil, response, stats, ErrVectorIndexSearchUnavailable
	}
	// This independent original-budget receipt covers cache entry/channel/map
	// controls, definition strings and cache-key formatting even on the first miss.
	// On hits the unused candidate receipt ends; no cached owner is refunded.
	control, err := nativeSchemaBackingCharge(reflect.ValueOf(&def), 0)
	if err != nil {
		return nil, response, stats, err
	}
	control, err = nativeTypedCheckedMul(control, 16)
	if err != nil {
		return nil, response, stats, err
	}
	for _, n := range []uint64{uint64(unsafe.Sizeof(collectionVectorIndexPreparedSearch{})), uint64(unsafe.Sizeof(collectionVectorIndexPreparedSearchCacheEntry{})), uint64(unsafe.Sizeof(collectionVectorIndexPreparedSearchCacheSlot{})), uint64(len(read.database.ColumnAssetRootDir())), uint64(len(read.meta.Name))} {
		if err = nativeTypedCheckedAdd(&control, memtable.COWAllocationCharge(n)); err != nil {
			return nil, response, stats, err
		}
	}
	if err = nativeTypedCheckedAdd(&control, 8*preparedSemanticStreamMapEntryReserveBytes); err != nil {
		return nil, response, stats, err
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(control)
	if err != nil {
		return nil, response, stats, fmt.Errorf("native prepared search control admission bytes=%d: %w", control, err)
	}
	transferred := false
	defer func() {
		if !transferred {
			lease.Close()
		}
	}()
	slot := collectionVectorIndexPreparedSearchCacheSlotForOptions(opts, mode)
	prepared, response, stats, err := c.acquireCollectionVectorIndexPreparedSearchAtIdentity(opts, slot, func() (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, error) {
		var p *collectionVectorIndexPreparedSearch
		var out VectorIndexSearchResponse
		e := WithVectorPartitionStorageBarrierWithContextV1(opts.Context, read.database.Dir(), func() error {
			var buildErr error
			p, out, buildErr = c.openNativeColumnGraphPreparedSearch(opts, read, identity, lease, control)
			return buildErr
		})
		if e == nil {
			transferred = true
		}
		return p, out, e
	}, read.database, 0, 0, identity)
	return prepared, response, stats, err
}

func (c *Collection) openNativeColumnGraphPreparedSearch(opts VectorIndexSearchOptions, read *NativeJSONRead, identity nativeColumnGraphPreparedIdentity, retained *memtable.COWExternalLease, retainedCharge uint64) (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	charge, err := read.nativeTypedReadViewCharge()
	if err != nil {
		return nil, response, err
	}
	recordsCharge, err := nativeVectorGraphRecordBackingCapacity(read.capture.Snapshot(), identity.manifestRoot, true)
	if err != nil {
		return nil, response, err
	}
	if err = nativeTypedCheckedAdd(&charge, recordsCharge); err != nil {
		return nil, response, err
	}
	// Serving constructs no source-requirement stream or its two leaf codec
	// slots. Keep the original untrusted TVIS gates and two complete state
	// decode envelopes: the selected-state census and shared view validation
	// both decode before their aliases end. The record copy charge is separate.
	decoded, err := nativeSourceRequirementBasisDecodedLengthV2(read.capture.Snapshot(), identity.manifestRoot)
	if err != nil {
		return nil, response, err
	}
	states, err := nativeTypedCheckedMul(nativeSourceRequirementStateCapacityV2(decoded), 2)
	if err != nil {
		return nil, response, err
	}
	if err = nativeTypedCheckedAdd(&charge, states); err != nil {
		return nil, response, err
	}
	metadata, err := nativeSchemaBackingCharge(reflect.ValueOf(&read.meta), 0)
	if err != nil {
		return nil, response, err
	}
	metadata, err = nativeTypedCheckedMul(metadata, 16)
	if err != nil {
		return nil, response, err
	}
	if err = nativeTypedCheckedAdd(&charge, metadata); err != nil {
		return nil, response, err
	}
	operation, err := read.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, response, fmt.Errorf("native prepared search initial setup admission bytes=%d: %w", charge, err)
	}
	defer operation.Close()
	local := read.nativeTypedReadCollection()
	def, _ := findVectorIndex(read.meta.VectorIndexes, opts.IndexName)
	cfg := read.meta.Options.ColumnStore
	if err = validateColumnManifestIdentityAtRoot(read.capture.Snapshot(), identity.manifestRoot, *cfg.ActiveManifest); err != nil {
		return nil, response, err
	}
	records, err := nativeVectorGraphRecords(read.capture.Snapshot(), identity.manifestRoot, true)
	if err != nil {
		return nil, response, err
	}
	record, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		return nil, response, ErrVectorIndexSearchUnavailable
	}
	state, err := decodeColumnVectorIndexStateRecord(record.value)
	if err != nil {
		return nil, response, err
	}
	// All asset-setup inspections finish under this operation lease. The TCIM
	// census admits stored parse backing before parsing and decoded/dictionary
	// backing before any generic decoder or mapped/heap projection is opened.
	var setup uint64
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleDocumentIDs {
			continue
		}
		n, e := nativeColumnGraphAssetReadCapacity(read.database.ColumnAssetRootDir(), asset, func(n uint64) error {
			if e := nativeTypedCheckedAdd(&n, charge); e != nil {
				return e
			}
			if e := nativeTypedCheckedAdd(&n, setup); e != nil {
				return e
			}
			if e := operation.Resize(n); e != nil {
				return fmt.Errorf("native prepared search asset census admission bytes=%d: %w", n, e)
			}
			return nil
		})
		if e != nil {
			return nil, response, e
		}
		if err = nativeTypedCheckedAdd(&setup, n); err != nil {
			return nil, response, err
		}
	}
	// Norm setup and exact manifest/TVIS/base/schema validation are common with
	// legacy. Document-ID payload validation is a separate synchronous phase.
	normCharge := uint64(0)
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleInverseNorm {
			n, e := nativeTypedCheckedMul(uint64(asset.Ref.Length), 16)
			if e != nil {
				return nil, response, e
			}
			if e = nativeTypedCheckedAdd(&normCharge, n); e != nil {
				return nil, response, e
			}
		}
	}
	if err = nativeTypedCheckedAdd(&charge, normCharge); err != nil {
		return nil, response, err
	}
	if err = operation.Resize(charge); err != nil {
		return nil, response, fmt.Errorf("native prepared search norm setup admission bytes=%d: %w", charge, err)
	}
	def, graph, view, err := local.columnVectorGraphPhysicalRowReaderViewFromRecords(def, local.catalog, records, nil)
	if err != nil {
		return nil, response, err
	}
	var key string
	err = func() error {
		temporary, e := read.capture.NativeAllocator().AdmitNativeBytes(setup)
		if e != nil {
			return fmt.Errorf("native prepared search document-ID setup admission bytes=%d: %w", setup, e)
		}
		defer temporary.Close()
		key, response, e = local.validateCollectionVectorIndexPreparedExactSearchView(opts, def, graph, view, 0, 0)
		return e
	}()
	if err != nil {
		return nil, response, err
	}
	pack, found, err := findColumnHNSWSearchPackStateAsset(view.VectorIndexState)
	if err != nil {
		return nil, response, err
	}
	if !found {
		return nil, response, ErrVectorIndexSearchUnavailable
	}
	packCharge, err := nativeColumnGraphPackCapacity(pack)
	if err != nil {
		return nil, response, err
	}
	stateBacking, err := nativeSchemaBackingCharge(reflect.ValueOf(&state), 0)
	if err != nil {
		return nil, response, err
	}
	graphBacking, err := nativeSchemaBackingCharge(reflect.ValueOf(&graph), 0)
	if err != nil {
		return nil, response, err
	}
	if err = nativeTypedCheckedAdd(&stateBacking, graphBacking); err != nil {
		return nil, response, err
	}
	stateBacking, err = nativeTypedCheckedMul(stateBacking, 16)
	if err != nil {
		return nil, response, err
	}
	if err = nativeTypedCheckedAdd(&packCharge, stateBacking); err != nil {
		return nil, response, err
	}
	if err = nativeTypedCheckedAdd(&packCharge, retainedCharge); err != nil {
		return nil, response, err
	}
	err = retained.Resize(packCharge)
	if errors.Is(err, memtable.ErrCOWCapacity) {
		// The original capture may release idle cache-owned generic scratch.
		// Do not retain a cache backpointer in the allocator or retry unchanged.
		if idle, ok := read.capture.(interface{ TryReleaseIdleNativeReadScratch() bool }); ok && idle.TryReleaseIdleNativeReadScratch() {
			err = retained.Resize(packCharge)
		}
	}
	if err != nil {
		return nil, response, fmt.Errorf("native prepared search retained pack admission bytes=%d setup=%d: %w", packCharge, charge, err)
	}
	// Definition strings in a cached response must not depend on the read's
	// schema receipt. Ordinary definitions contain no quantized option slices.
	def.Name = strings.Clone(def.Name)
	def.Field = strings.Clone(def.Field)
	def.Strategy = VectorIndexStrategy(strings.Clone(string(def.Strategy)))
	response.IndexName = def.Name
	response.Strategy = def.Strategy
	response.Status.Name = def.Name
	response.Status.Strategy = def.Strategy
	key = fmt.Sprintf("%s|native=%+v", key, identity)
	response.Status.Definition = def
	// The mapped resource scope retains the collection name after this catalog
	// and capture end; give that scope its own admitted name backing.
	local.catalog.meta.Name = strings.Clone(local.catalog.meta.Name)
	p, response, err := local.openCollectionVectorIndexPreparedExactSearchPack(def, graph, view, 0, 0, key, response)
	if err != nil {
		return nil, response, err
	}
	// Only the complete maintained opener establishes that validation scratch is
	// dead. Preserve the original control/state/graph floors and refund only the
	// difference on this same receipt; unknown shapes retain the full promise.
	if owned, ok := nativeColumnGraphFlatPackRetainedCharge(p); ok {
		if e := nativeTypedCheckedAdd(&owned, stateBacking); e == nil {
			if e = nativeTypedCheckedAdd(&owned, retainedCharge); e == nil && owned < packCharge {
				if e = retained.ShrinkNativeBytes(packCharge-owned, owned); e != nil {
					return nil, response, errors.Join(e, p.Close())
				}
			}
		}
	}
	p.collection = c // Never retain the operation-local catalog, DB facade or capture.
	p.nativeIdentity = identity
	p.nativeLease = retained
	return p, response, nil
}

// This is a post-success retained census of the maintained flat file view,
// never an input-size estimate or permission to allocate under a smaller peak.
func nativeColumnGraphFlatPackRetainedCharge(p *collectionVectorIndexPreparedSearch) (uint64, bool) {
	if p == nil || p.pack == nil {
		return 0, false
	}
	v := p.pack
	if v.handle == nil || v.manager == nil || v.ephemeralHeap || v.Header.ExternalNormalizedVectors || v.handles != nil || v.sectionBytes != nil || p.searcher != nil || p.capturedBase != nil || p.quantizedReaders != nil || p.sharedQuantizedAssets != nil {
		return 0, false
	}
	n, ok := v.handle.SingleFileRetainedCharge()
	if !ok {
		return 0, false
	}
	addArray := func(count int, size uintptr) bool {
		bytes, err := nativeTypedCheckedMul(uint64(count), uint64(size))
		return err == nil && nativeTypedCheckedAdd(&n, memtable.COWAllocationCharge(bytes)) == nil
	}
	if !addArray(1, unsafe.Sizeof(*v)) || !addArray(cap(v.Sections), unsafe.Sizeof(columnHNSWSearchPackSection{})) || !addArray(cap(v.AdjacencyLayers), unsafe.Sizeof(columnHNSWSearchPackPreparedLayer{})) || !addArray(cap(v.adjacencyOffsetChunks), unsafe.Sizeof(columnHNSWSearchPackPreparedChunks[uint64]{})) || !addArray(cap(v.adjacencyNeighborChunks), unsafe.Sizeof(columnHNSWSearchPackPreparedChunks[uint32]{})) {
		return 0, false
	}
	// All typed values and auxiliary-navigation spans alias the admitted file
	// extent. Only each chunks.values descriptor and chunks.starts is allocated.
	for _, dims := range [...][2]int{
		{cap(v.normalizedVectorChunks.values), cap(v.normalizedVectorChunks.starts)},
		{cap(v.levelChunks.values), cap(v.levelChunks.starts)},
		{cap(v.rowRefGenerationChunks.values), cap(v.rowRefGenerationChunks.starts)},
		{cap(v.rowRefPartIDChunks.values), cap(v.rowRefPartIDChunks.starts)},
		{cap(v.rowRefRowIndexChunks.values), cap(v.rowRefRowIndexChunks.starts)},
		{cap(v.rowRefAppliedLSNChunks.values), cap(v.rowRefAppliedLSNChunks.starts)},
		{cap(v.documentIDOffsetChunks.values), cap(v.documentIDOffsetChunks.starts)},
		{cap(v.documentIDByteChunks.values), cap(v.documentIDByteChunks.starts)},
	} {
		if !addArray(dims[0], unsafe.Sizeof([]byte{})) || !addArray(dims[1], unsafe.Sizeof(uint64(0))) {
			return 0, false
		}
	}
	for i := range v.adjacencyOffsetChunks {
		if i >= len(v.adjacencyNeighborChunks) {
			return 0, false
		}
		a, b := v.adjacencyOffsetChunks[i], v.adjacencyNeighborChunks[i]
		if !addArray(cap(a.values), unsafe.Sizeof([]uint64{})) || !addArray(cap(a.starts), unsafe.Sizeof(uint64(0))) || !addArray(cap(b.values), unsafe.Sizeof([]uint32{})) || !addArray(cap(b.starts), unsafe.Sizeof(uint64(0))) {
			return 0, false
		}
	}
	// The cache key is assembled after the original control charge. Definition
	// and response strings have their unchanged conservative control/state floor.
	if nativeTypedCheckedAdd(&n, memtable.COWAllocationCharge(uint64(len(p.key)))) != nil {
		return 0, false
	}
	return n, true
}

// Document-ID setup does not decode granules or row locators. Its maintained
// TCIM parser only needs a generic decoder for compressed image sections, even
// when selected data is raw. Keep the original full compressed-section envelope
// and malformed dictionary census; entirely raw images need no decoder.
func nativeColumnGraphAssetReadCapacity(root string, asset columnVectorIndexStateAssetSnapshot, admit func(uint64) error) (uint64, error) {
	if asset.Ref.Length <= 0 {
		return 0, ErrVectorIndexSearchUnavailable
	}
	total, err := nativeTypedCheckedMul(uint64(asset.Ref.Length), 16)
	if err != nil {
		return 0, err
	}
	if err = admit(total); err != nil {
		return 0, err
	}
	raw, err := readColumnPhysicalAssetFromManager(root, asset.Ref)
	if err != nil {
		return 0, err
	}
	image, err := typedcolumn.ParseColumnPartImageManifest(raw, len(raw))
	if err != nil {
		return 0, err
	}
	compressed := false
	for _, section := range image.Sections {
		compressed = compressed || section.Compression != typedcolumn.CompressionNone
		n := uint64(max(section.RawBytes, section.Length))
		extra, e := nativeTypedCheckedMul(n, 12)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, extra); e != nil {
			return 0, e
		}
		if section.Kind == typedcolumn.ColumnPartImageSectionDictionaries {
			// The maintained dense dictionary can allocate one slot per 4 raw bytes;
			// malformed counts are checked by the original decoder before allocation.
			slots := n/4 + n/8
			extra, e = nativeTypedCheckedMul(slots, preparedSemanticStreamMapEntryReserveBytes)
			if e != nil {
				return 0, e
			}
			if e = nativeTypedCheckedAdd(&total, extra); e != nil {
				return 0, e
			}
		}
	}
	if compressed {
		fixed, err := nativeTypedDecoderFixedCapacity()
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&total, fixed); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func nativeColumnGraphPackCapacity(asset columnVectorIndexStateAssetSnapshot) (uint64, error) {
	if asset.Ref.Length <= 0 {
		return 0, ErrVectorIndexSearchUnavailable
	}
	// Directory entries are 64 bytes. Sixteen stored-image copies cover one
	// heap fallback (or mapped extent), both directory arrays, range sorting,
	// section maps/layer descriptors and the v6/v7 rows-sized BFS validation.
	// Fixed cache/manager/handle and key controls are independently preadmitted.
	return nativeTypedCheckedMul(memtable.COWAllocationCharge(uint64(asset.Ref.Length)), 16)
}

// Buffer ownership is unchanged: Reset detaches all native backing before
// releasing this receipt. Bounds cover full-row frontiers/tiles even when Ef
// or TopK is small, including growth cohorts and the independent public IDs.
func nativeColumnGraphQueryCapacity(pack *columnHNSWSearchPackPreparedView, opts VectorIndexSearchOptions) (uint64, error) {
	if pack == nil || pack.Header.Rows < 0 || pack.Header.VectorStride < 0 {
		return 0, ErrVectorIndexSearchUnavailable
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return 0, err
	}
	rows := uint64(pack.Header.Rows)
	top := uint64(min(opts.TopK, pack.Header.Rows))
	total := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndexSearchBuffer{})))
	// visit marks; two candidate heaps; row-ID and exact-score tiles; optional
	// allow-set ordinal scan. Eight cohorts cover append growth plus replacement
	// scratch. Each resize's empty/header allocation is included independently.
	units := []uint64{2, uint64(unsafe.Sizeof(columnVectorGraphSearchCandidate{})), uint64(unsafe.Sizeof(columnVectorGraphSearchCandidate{})), 4, 4, 4, 8, 4}
	for _, unit := range units {
		n, e := nativeTypedCheckedMul(rows, unit)
		if e != nil {
			return 0, e
		}
		n, e = nativeTypedCheckedMul(memtable.COWAllocationCharge(n), 8)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, n); e != nil {
			return 0, e
		}
	}
	for _, unit := range []uint64{uint64(unsafe.Sizeof(columnVectorGraphNativeSearchResult{})), uint64(unsafe.Sizeof(VectorIndexSearchResult{})), 24, 24, 8, 8, uint64(unsafe.Sizeof(DocumentRowRef{})), 1} {
		n, e := nativeTypedCheckedMul(top, unit)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); e != nil {
			return 0, e
		}
	}
	n, e := nativeTypedCheckedMul(uint64(pack.Header.VectorStride), 4)
	if e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); e != nil {
		return 0, e
	}
	// Every returned ID is a subspan of the validated pack. Paying its complete
	// ID payload twice bounds both buffer and independent owned-result copies.
	// The source pack itself remains independently admitted by the cache.
	ids := uint64(len(pack.DocumentIDBytes))
	if pack.documentIDByteChunks.length > ids {
		ids = pack.documentIDByteChunks.length
	}
	ids, e = nativeTypedCheckedMul(memtable.COWAllocationCharge(ids), 2)
	if e != nil {
		return 0, e
	}
	if e = nativeTypedCheckedAdd(&total, ids); e != nil {
		return 0, e
	}
	return total, nil
}

// nativeColumnGraphPreparedSearchAcquisition is a borrowed operation-local result,
// not a second prepared owner. The original cache owns prepared and its receipt.
type nativeColumnGraphPreparedSearchAcquisition struct {
	prepared *collectionVectorIndexPreparedSearch
	response VectorIndexSearchResponse
	stats    collectionVectorIndexPreparedSearchAcquireStats
	err      error
}

func (c *Collection) searchNativeColumnGraphAtReadView(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer, view *CollectionReadView, allowSet hybridScalarAllowSet) (response VectorIndexSearchResponse, resultErr error) {
	return c.searchNativeColumnGraphAtReadViewWithPrepared(opts, buffer, view, allowSet, nil)
}

func (c *Collection) searchNativeColumnGraphAtReadViewWithPrepared(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer, view *CollectionReadView, allowSet hybridScalarAllowSet, acquisition *nativeColumnGraphPreparedSearchAcquisition) (response VectorIndexSearchResponse, resultErr error) {
	if err := validateCollectionVectorIndexSearchWithBufferOptions(opts, buffer); err != nil {
		return response, err
	}
	buffer.Reset()
	defer func() {
		if resultErr != nil {
			buffer.Reset()
			response.Results = nil
		}
	}()
	if view == nil || view.closed || view.collection != c || view.nativeRead == nil {
		return response, ErrVectorIndexSnapshotMismatch
	}
	if opts.DeclaredScalarFilter != nil {
		return response, collectionVectorIndexWithBufferUnsupportedOptionError("DeclaredScalarFilter", "column_graph requires the captured scalar allow-set route")
	}
	var prepared *collectionVectorIndexPreparedSearch
	var stats collectionVectorIndexPreparedSearchAcquireStats
	var err error
	if acquisition == nil {
		prepared, response, stats, err = c.acquireNativeColumnGraphPreparedSearchAtRead(opts, view.nativeRead)
	} else {
		prepared, response, stats, err = acquisition.prepared, acquisition.response, acquisition.stats, acquisition.err
	}
	if err != nil {
		return response, err
	}
	sm, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		return response, err
	}
	// Hold the existing prepared-owner lock through admission and traversal so a
	// concurrent cache replacement cannot close the mapped bytes between them.
	prepared.mu.RLock()
	defer prepared.mu.RUnlock()
	if prepared.closed || prepared.pack == nil {
		return response, errColumnHNSWSearchPackPreparedViewClosed
	}
	if prepared.searchStartedForTest != nil {
		prepared.searchStartedForTest()
	}
	charge, err := nativeColumnGraphQueryCapacity(prepared.pack, opts)
	if err != nil {
		return response, err
	}
	buffer.nativeLease, err = view.nativeRead.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return response, err
	}
	if opts.Context != nil {
		if err = opts.Context.Err(); err != nil {
			return response, err
		}
	}
	searchOpts := columnVectorGraphNativeSearchOptions{TopK: opts.TopK, EfSearch: opts.EfSearch, ScoreBatchMode: opts.scoreBatchMode, StatsMode: sm, QueryMode: columnVectorGraphNativeSearchQueryModeExact}
	var results []columnVectorGraphNativeSearchResult
	var work columnVectorGraphNativeSearchStats
	if allowSet != nil {
		results, work, err = prepared.pack.searchCosineAllowSet(opts.Query, searchOpts, allowSet, &buffer.searchScratch)
	} else {
		ctx := opts.Context
		if ctx == nil {
			ctx = context.Background()
		}
		results, work, err = prepared.pack.searchCosineWithContext(ctx, opts.Query, searchOpts, &buffer.searchScratch)
	}
	response.Stats = vectorIndexSearchStatsFromInternal(work, columnPhysicalRowReaderStats{})
	prepared.routeStats.apply(&response.Stats)
	stats.apply(&response.Stats)
	if err != nil {
		return response, err
	}
	if opts.Context != nil {
		if err = opts.Context.Err(); err != nil {
			return response, err
		}
	}
	response.Results, err = copyVectorIndexSearchResultsToBuffer(results, buffer, nil)
	return response, err
}

// Same native capture for strategy selection, graph search and document fetch.
func (c *Collection) searchNativeVectorWithBufferReadView(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (response VectorIndexSearchResponse, view *CollectionReadView, resultErr error) {
	if err := validateCollectionVectorIndexSearchWithBufferOptions(opts, buffer); err != nil {
		return response, nil, err
	}
	view, err := c.OpenCollectionReadView()
	if err != nil {
		buffer.Reset()
		return response, nil, err
	}
	def, ok := findVectorIndex(view.nativeRead.meta.VectorIndexes, opts.IndexName)
	if !ok {
		err = ErrIndexNotFound
	} else if def.Strategy == VectorIndexStrategyColumnGraph {
		response, err = c.searchNativeColumnGraphAtReadView(opts, buffer, view, nil)
	} else {
		response, err = c.searchNativeHNSWAtReadView(opts, buffer, view, nil)
	}
	if err != nil {
		buffer.Reset()
		return response, nil, errors.Join(err, view.Close())
	}
	return response, view, nil
}

func (c *Collection) searchNativeVectorOwned(opts VectorIndexSearchOptions) (response VectorIndexSearchResponse, resultErr error) {
	include := opts.IncludeDocuments
	fetch := opts.DocumentFetchOptions
	if !include && vectorIndexDocumentFetchOptionsNonZero(fetch) {
		return response, errors.New("collections: vector index document projection requires IncludeDocuments")
	}
	opts.IncludeDocuments = false
	opts.DocumentFetchOptions = DocumentFetchOptions{}
	var buffer VectorIndexSearchBuffer
	defer buffer.Reset()
	response, view, err := c.searchNativeVectorWithBufferReadView(opts, &buffer)
	if err != nil {
		return response, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, view.Close())
		if resultErr != nil {
			response.Results = nil
		}
	}()
	// The local buffer never enters the legacy pool. ID/result ownership moves
	// to the public response; scratch is detached when the local scope ends.
	if include {
		n, e := nativeTypedCheckedMul(uint64(len(response.Results)), uint64(unsafe.Sizeof([]byte(nil))))
		if e != nil {
			return response, e
		}
		headers, e := view.nativeRead.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(n))
		if e != nil {
			return response, e
		}
		defer headers.Close()
		docs, e := view.FetchDocumentsForVectorIndexSearchResults(response.Results, fetch)
		if e != nil {
			return response, e
		}
		if len(docs.Results) != len(response.Results) {
			return response, ErrVectorIndexSnapshotMismatch
		}
		for i := range response.Results {
			if !docs.Results[i].Found {
				return response, ErrVectorIndexSnapshotMismatch
			}
			response.Results[i].Document = docs.Results[i].Document
		}
	}
	// Reset clears the same results array; detach response-owned output first.
	buffer.results = nil
	buffer.idBytes = nil
	markVectorIndexSearchResponseOwnedResultAllocs(&response)
	return response, nil
}
