package collections

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This is an operation scope, not a second read authority. The catalog and
// physical view borrow the very same complete-cut capture as ordinary Get.
// Prepared runners keep that capture; one-shot reducers release it on return.
type nativeColumnPhysicalQuery struct {
	read       *NativeJSONRead
	lease      *memtable.COWExternalLease
	collection *Collection
	view       columnPhysicalScanSnapshotView
	cleanupErr error // serialized by read.mu, including parallel decoder caches
}

func (s *nativeColumnPhysicalQuery) observeClose(err error) {
	if err == nil {
		return
	}
	s.read.mu.Lock()
	s.cleanupErr = errors.Join(s.cleanupErr, err)
	s.read.mu.Unlock()
}

func (s *nativeColumnPhysicalQuery) close() error {
	if s == nil {
		return nil
	}
	read, lease, closeErr := s.read, s.lease, s.cleanupErr
	// End every borrowed view/catalog/cache alias before its original receipt.
	s.view = columnPhysicalScanSnapshotView{}
	s.collection, s.read, s.lease, s.cleanupErr = nil, nil, nil, nil
	s = nil
	lease.Close()
	return errors.Join(closeErr, read.Close())
}

func newColumnPhysicalAssetReadCacheForView(view columnPhysicalScanSnapshotView, integrity ColumnAssetReadIntegrity) (columnPhysicalAssetReadCache, error) {
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, integrity)
	if err == nil && view.nativePhysical != nil {
		// The existing mmap reader discards a failed unmap. An admitted
		// operation uses its maintained ReadAt path so no mapping can outlive
		// this scope after an unsuccessful cleanup.
		cache.operationLocal = true
		cache.forceReadAtFallback = true
		cache.serialPartDecode = true
		cache.closeError = view.nativePhysical.observeClose
	}
	return cache, err
}

func nativeColumnPhysicalRequestCharge(req ColumnPhysicalQueryRequest) (uint64, error) {
	headers, err := nativeTypedCheckedMul(uint64(len(req.Predicates)), uint64(unsafe.Sizeof(ColumnPhysicalQueryPredicate{})))
	if err != nil {
		return 0, err
	}
	total := memtable.COWAllocationCharge(headers)
	addString := func(v string) error {
		return nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(uint64(len(v))))
	}
	for _, v := range []string{string(req.Kind), req.GroupColumn, req.ValueColumn, req.DistinctColumn, req.AggregateMetadataName, string(req.TopKOrder)} {
		if err := addString(v); err != nil {
			return 0, err
		}
	}
	for _, p := range req.Predicates {
		for _, v := range []string{p.Column, string(p.Kind), p.Value} {
			if err := addString(v); err != nil {
				return 0, err
			}
		}
		n, err := nativeTypedCheckedMul(uint64(len(p.Values)), uint64(unsafe.Sizeof("")))
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); err != nil {
			return 0, err
		}
		for _, v := range p.Values {
			if err := addString(v); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func cloneNativeColumnPhysicalRequest(req ColumnPhysicalQueryRequest) ColumnPhysicalQueryRequest {
	req.Kind = ColumnPhysicalQueryKind(strings.Clone(string(req.Kind)))
	req.GroupColumn = strings.Clone(req.GroupColumn)
	req.ValueColumn = strings.Clone(req.ValueColumn)
	req.DistinctColumn = strings.Clone(req.DistinctColumn)
	req.AggregateMetadataName = strings.Clone(req.AggregateMetadataName)
	req.TopKOrder = ColumnPhysicalQueryTopKOrder(strings.Clone(string(req.TopKOrder)))
	if req.Predicates != nil {
		input := req.Predicates
		req.Predicates = make([]ColumnPhysicalQueryPredicate, len(input))
		for i, p := range input {
			p.Column = strings.Clone(p.Column)
			p.Kind = ColumnPhysicalQueryPredicateKind(strings.Clone(string(p.Kind)))
			p.Value = strings.Clone(p.Value)
			if p.Values != nil {
				p.Values = make([]string, len(input[i].Values))
				for j, v := range input[i].Values {
					p.Values[j] = strings.Clone(v)
				}
			}
			req.Predicates[i] = p
		}
	}
	return req
}

// Reducer maps use actual dictionary-section cardinality bounds, not the
// already-expanded retained decoder/output allowance. Other decoded sections
// contribute bytes, never fictitious dictionary entries.
func nativeColumnPhysicalWorkspace(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, shape nativeTypedFamilyReadShape) (uint64, error) {
	var rows, refs, stored, metadataSlots, sidecarDictionarySlots uint64
	addRef := func(ref ColumnAssetRef) error {
		if ref.Length < 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if err := nativeTypedCheckedAdd(&stored, uint64(ref.Length)); err != nil {
			return err
		}
		return nativeTypedCheckedAdd(&refs, 1)
	}
	for _, part := range view.AssetRefs {
		if part.Rows < 0 {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		if err := nativeTypedCheckedAdd(&rows, uint64(part.Rows)); err != nil {
			return 0, err
		}
		if err := addRef(part.Ref); err != nil {
			return 0, err
		}
	}
	for _, part := range view.TypedColumnPartRefs {
		// Some maintained layouts have only typed-column parts; paired row and
		// typed parts may conservatively credit the same rows twice.
		if part.Rows < 0 {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		if err := nativeTypedCheckedAdd(&rows, uint64(part.Rows)); err != nil {
			return 0, err
		}
		if err := addRef(part.Ref); err != nil {
			return 0, err
		}
	}
	for _, part := range view.AggregateMetadata {
		if err := addRef(part.AssetRef); err != nil {
			return 0, err
		}
		// The original metadata decoder bounds each entry by at least 32 bytes.
		if err := nativeTypedCheckedAdd(&metadataSlots, uint64(part.AssetRef.Length)/32); err != nil {
			return 0, err
		}
	}
	for _, part := range view.DictionaryCodes {
		if err := addRef(part.AssetRef); err != nil {
			return 0, err
		}
		// Compatibility strings use the original manifest cursor's eight-byte lengths.
		if err := nativeTypedCheckedAdd(&sidecarDictionarySlots, uint64(part.AssetRef.Length)/8); err != nil {
			return 0, err
		}
	}
	for _, part := range view.Int64Values {
		if err := addRef(part.AssetRef); err != nil {
			return 0, err
		}
	}
	for _, ref := range view.GraphAssetRefs {
		if err := addRef(ref); err != nil {
			return 0, err
		}
	}
	if rows > uint64(maxCollectionInt) || refs > uint64(maxCollectionInt) {
		return 0, memtable.ErrCOWCapacity
	}
	dictionary := shape.dictionarySlots
	if err := nativeTypedCheckedAdd(&dictionary, sidecarDictionarySlots); err != nil {
		return 0, err
	}
	cells, err := nativeTypedCheckedMul(rows, uint64(len(view.FullConfig.Columns)+1))
	if err != nil {
		return 0, err
	}
	slots := cells
	for _, n := range []uint64{dictionary, metadataSlots, refs, uint64(len(req.Predicates)), 128} {
		if err = nativeTypedCheckedAdd(&slots, n); err != nil {
			return 0, err
		}
	}
	// At most eight coexisting map/rank/summary families, including old+growing
	// tables: decoder reverse maps, global/local ranks, visibility, reducer and
	// predicate tables. This uses the maintained per-entry map quota.
	total, err := nativeTypedCheckedMul(slots, 8*preparedSemanticStreamMapEntryReserveBytes)
	if err != nil {
		return 0, err
	}
	// Hour counts have 24 cells per row; retain result headers, count/min/max,
	// touched/rank arrays and old+replacement backing together until Close.
	vectors, err := nativeTypedCheckedMul(rows+1, 24*(uint64(unsafe.Sizeof(ColumnPhysicalQueryGroup{}))+8*8)+2*uint64(unsafe.Sizeof(columnPhysicalVisibleRow{})))
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(vectors)); err != nil {
		return 0, err
	}
	// Input/key clones, decoder output and summary strings can coexist. The
	// family receipt separately admits decoded backing from raw section lengths.
	stringsBytes := stored
	if err = nativeTypedCheckedAdd(&stringsBytes, shape.decodedBytes); err != nil {
		return 0, err
	}
	stringsBytes, err = nativeTypedCheckedMul(stringsBytes, 24)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(stringsBytes)); err != nil {
		return 0, err
	}
	if req.Kind == ColumnPhysicalQueryGroupCountDistinct || req.Kind == ColumnPhysicalQueryGroupCountAndDistinct {
		cardinality := dictionary
		if err = nativeTypedCheckedAdd(&cardinality, rows+1); err != nil {
			return 0, err
		}
		if cardinality > uint64(maxCollectionInt)-63 {
			return 0, memtable.ErrCOWCapacity
		}
		words, err := nativeTypedCheckedMul(cardinality, (cardinality+63)/64)
		if err != nil {
			return 0, err
		}
		words = min(words, uint64(columnTypedColumnDenseGroupCountDistinctMaxBitsetWords))
		bits, err := nativeTypedCheckedMul(words, 16)
		if err != nil {
			return 0, err
		} // old+growing uint64 slices
		if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(bits)); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func (c *Collection) captureNativeColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (scope *nativeColumnPhysicalQuery, resultErr error) {
	if err := validateColumnPhysicalQueryRequest(req); err != nil {
		return nil, err
	}
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, err
	}
	var lease *memtable.COWExternalLease
	var wrapper *Collection
	var view columnPhysicalScanSnapshotView
	defer func() {
		if resultErr != nil {
			wrapper = nil
			view = columnPhysicalScanSnapshotView{}
			if lease != nil {
				lease.Close()
			}
			resultErr = errors.Join(resultErr, read.Close())
		}
	}()
	if !nativeTypedMeta(read.meta) || read.collection == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := read.collection.domains.Get("column/manifest")
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	physical, err := nativeTypedReadCapacity(read.meta, read.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return nil, err
	}
	// Prepared runners retain decoded data, not the synchronous decoder. Keep
	// the original fixed codec envelope only during actual Prepare/Run below.
	fixed, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		return nil, err
	}
	if physical < fixed {
		return nil, memtable.ErrCOWCapacity
	}
	physical -= fixed
	header, err := read.nativeTypedReadViewCharge()
	if err != nil {
		return nil, err
	}
	request, err := nativeColumnPhysicalRequestCharge(req)
	if err != nil {
		return nil, err
	}
	for _, n := range []uint64{memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeColumnPhysicalQuery{}))), memtable.COWAllocationCharge(uint64(unsafe.Sizeof(ColumnPhysicalQueryRunner{}))), request} {
		if err = nativeTypedCheckedAdd(&header, n); err != nil {
			return nil, err
		}
	}
	if err = nativeTypedCheckedAdd(&physical, header); err != nil {
		return nil, err
	}
	lease, err = read.capture.NativeAllocator().AdmitNativeBytes(physical)
	if err != nil {
		return nil, fmt.Errorf("native physical query captured view reserve %d bytes: %w", physical, err)
	}
	var shape nativeTypedFamilyReadShape
	extra, _, err := nativeTypedFamilyReadCapacity(read.meta, read.capture.Snapshot(), manifest.baseRoot, read.database.ColumnAssetRootDir(), &shape, func(n uint64) error {
		if e := nativeTypedCheckedAdd(&n, header); e != nil {
			return e
		}
		requested := max(physical, n)
		if e := lease.Resize(requested); e != nil {
			return fmt.Errorf("native physical query preflight reserve %d bytes: %w", requested, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = nativeTypedCheckedAdd(&physical, extra); err != nil {
		return nil, err
	}
	if err = lease.Resize(physical); err != nil {
		return nil, fmt.Errorf("native physical query retained backing reserve %d bytes: %w", physical, err)
	}
	wrapper = read.nativeTypedReadCollection()
	view, err = wrapper.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(read.capture.Snapshot(), wrapper.catalog, read.meta.Name, manifest.baseRoot, *read.meta.Options.ColumnStore, true, columnManifestScanSidecarsForPhysicalQuery(req))
	if err != nil {
		return nil, err
	}
	workspace, err := nativeColumnPhysicalWorkspace(view, req, shape)
	if err != nil {
		return nil, err
	}
	if err = nativeTypedCheckedAdd(&physical, workspace); err != nil {
		return nil, err
	}
	if err = lease.Resize(physical); err != nil {
		return nil, fmt.Errorf("native physical query reducer workspace reserve %d bytes: %w", physical, err)
	}
	scope = &nativeColumnPhysicalQuery{read: read, lease: lease, collection: wrapper, view: view}
	scope.view.snapshot = read.capture.Snapshot()
	scope.view.nativePhysical = scope
	return scope, nil
}

// Every decoder is synchronous and closed before these calls return. The same
// original allocator admits its fixed envelope; no prepared runner retains it.
func (s *nativeColumnPhysicalQuery) admitCodec() (*memtable.COWExternalLease, error) {
	n, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		return nil, err
	}
	lease, err := s.read.capture.NativeAllocator().AdmitNativeBytes(n)
	if err != nil {
		return nil, fmt.Errorf("native physical query codec reserve %d bytes: %w", n, err)
	}
	return lease, nil
}

func (c *Collection) runNativeColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (result ColumnPhysicalQueryResult, resultErr error) {
	scope, err := c.captureNativeColumnPhysicalQuery(req)
	if err != nil {
		return result, err
	}
	view, wrapper := scope.view, scope.collection
	defer func() {
		// Reducer results on failure may still borrow operation scratch.
		if resultErr != nil {
			result = ColumnPhysicalQueryResult{}
		}
		view = columnPhysicalScanSnapshotView{}
		wrapper = nil
		resultErr = errors.Join(resultErr, scope.close())
		scope = nil
	}()
	codec, err := scope.admitCodec()
	if err != nil {
		return result, err
	}
	defer codec.Close()
	if view.MutationParts > 0 {
		if req.AggregateMetadataName != "" {
			return result, fmt.Errorf("%w: aggregate metadata physical query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
		}
		var candidate bool
		result, candidate, err = wrapper.runColumnPhysicalQueryTypedColumnPartInSnapshotView(view, req)
		if !candidate {
			if columnPhysicalQueryHasPredicates(req) {
				return result, fmt.Errorf("%w: physical predicates require insert-only manifest", ErrColumnQueryPlanUnsupported)
			}
			result, err = wrapper.runColumnPhysicalQueryWithVisibilityInView(view, req)
		}
	} else if req.AggregateMetadataName != "" {
		result, err = wrapper.runColumnPhysicalQueryAggregateMetadataInSnapshotView(view, req)
	} else {
		result, err = wrapper.runColumnPhysicalQueryInSnapshotView(view, req)
	}
	if err != nil {
		return result, err
	}
	// One-shot output transfers to the public caller; no reducer/cache backing
	// may escape the scope. Admit its exact independent headers/keys first.
	n, e := nativeTypedCheckedMul(uint64(len(result.Groups)), uint64(unsafe.Sizeof(ColumnPhysicalQueryGroup{})))
	if e != nil {
		return ColumnPhysicalQueryResult{}, e
	}
	charge := memtable.COWAllocationCharge(n)
	if e = nativeColumnPhysicalDiagnosticsCharge(&charge, result.Diagnostics); e != nil {
		return ColumnPhysicalQueryResult{}, e
	}
	for _, group := range result.Groups {
		if e = nativeTypedCheckedAdd(&charge, memtable.COWAllocationCharge(uint64(len(group.Key)))); e != nil {
			return ColumnPhysicalQueryResult{}, e
		}
	}
	output, e := scope.read.capture.NativeAllocator().AdmitNativeBytes(charge)
	if e != nil {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("native physical query owned output reserve %d bytes: %w", charge, e)
	}
	groups := make([]ColumnPhysicalQueryGroup, len(result.Groups))
	for i, group := range result.Groups {
		group.Key = strings.Clone(group.Key)
		groups[i] = group
	}
	result.Groups = groups
	result.Diagnostics = cloneNativeColumnPhysicalDiagnostics(result.Diagnostics)
	groups = nil
	output.Close()
	return result, nil
}

func nativeColumnPhysicalDiagnosticsCharge(total *uint64, d ColumnPhysicalQueryDiagnostics) error {
	for _, value := range []string{d.ManifestRootName, d.SortKeyMarkFallbackReason, d.SortedGroupedDistinctFallbackReason, d.DenseGroupCountDistinctReducer, d.DenseInt64SpanReducer, d.TopKOrder, d.ColumnAssetReadIntegrity, string(d.StorageSource), string(d.FallbackReason)} {
		if err := nativeTypedCheckedAdd(total, memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			return err
		}
	}
	for _, values := range [][]string{d.PredicateColumns, d.PredicateKinds, d.SortKeyPrefixColumns} {
		n, err := nativeTypedCheckedMul(uint64(len(values)), uint64(unsafe.Sizeof("")))
		if err != nil {
			return err
		}
		if err = nativeTypedCheckedAdd(total, memtable.COWAllocationCharge(n)); err != nil {
			return err
		}
		for _, value := range values {
			if err = nativeTypedCheckedAdd(total, memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
				return err
			}
		}
	}
	return nil
}

func cloneNativeColumnPhysicalDiagnostics(d ColumnPhysicalQueryDiagnostics) ColumnPhysicalQueryDiagnostics {
	d.ManifestRootName = strings.Clone(d.ManifestRootName)
	d.SortKeyMarkFallbackReason = strings.Clone(d.SortKeyMarkFallbackReason)
	d.SortedGroupedDistinctFallbackReason = strings.Clone(d.SortedGroupedDistinctFallbackReason)
	d.DenseGroupCountDistinctReducer = strings.Clone(d.DenseGroupCountDistinctReducer)
	d.DenseInt64SpanReducer = strings.Clone(d.DenseInt64SpanReducer)
	d.TopKOrder = strings.Clone(d.TopKOrder)
	d.ColumnAssetReadIntegrity = strings.Clone(d.ColumnAssetReadIntegrity)
	d.StorageSource = ColumnPhysicalQueryStorageSource(strings.Clone(string(d.StorageSource)))
	d.FallbackReason = ColumnPhysicalQueryFallbackReason(strings.Clone(string(d.FallbackReason)))
	clone := func(input []string) []string {
		if input == nil {
			return nil
		}
		out := make([]string, len(input))
		for i, value := range input {
			out[i] = strings.Clone(value)
		}
		return out
	}
	d.PredicateColumns = clone(d.PredicateColumns)
	d.PredicateKinds = clone(d.PredicateKinds)
	d.SortKeyPrefixColumns = clone(d.SortKeyPrefixColumns)
	return d
}

func (c *Collection) prepareNativeColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (*ColumnPhysicalQueryRunner, error) {
	scope, err := c.captureNativeColumnPhysicalQuery(req)
	if err != nil {
		return nil, err
	}
	codec, err := scope.admitCodec()
	if err != nil {
		return nil, errors.Join(err, scope.close())
	}
	defer codec.Close()
	runner, err := scope.collection.prepareColumnPhysicalQueryInView(scope.view, nil, cloneNativeColumnPhysicalRequest(req))
	if err != nil {
		return nil, errors.Join(err, scope.close())
	}
	runner.nativePhysical = scope
	return runner, nil
}

func (c *Collection) runColumnPhysicalQueryWithVisibilityInView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if view.nativePhysical == nil {
		return c.runColumnPhysicalQueryWithVisibility(view.Config, req)
	}
	exec, err := newColumnPhysicalQueryExecutor(view.Config, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	cache, err := newColumnPhysicalAssetReadCacheForView(view, req.ColumnAssetReadIntegrity)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	defer func() { _ = cache.close() }()
	start := time.Now()
	visible, err := c.scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadCache(view.snapshot, view.Catalog, view.CollectionName, view.Diagnostics.ManifestRoot, view.FullConfig, view.ColumnStoreEnabled, nil, exec.projected, req.ColumnAssetReadIntegrity, nil, &cache, 0)
	return reduceColumnPhysicalVisibleRows(exec, req, visible, err, time.Since(start).Nanoseconds())
}
