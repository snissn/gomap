package collections

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
)

// No snapshot, catalog or suffix is retained here. The serving capability owns
// both the immutable holder ref and the exact-base lifecycle pin; they cannot
// be acquired or released independently.
type typedGraphCapturedBaseResources struct {
	capability       *typedGraphServingHolderCapability
	accounting       *typedGraphReadOwnerAccounting
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
	background       sync.WaitGroup
	assetBytes       int64
	descriptorBytes  int64
	backingBytes     int64
	filtersMu        sync.Mutex
	// ponytail: keep the first eight distinct predicates until keeper Close.
	// Overflow uses uncached preparation; add eviction only if this ceiling matters.
	filters [8]typedGraphCachedFilter
}

func (r *typedGraphCapturedBaseResources) Close() error {
	if r == nil {
		return nil
	}
	if r.backgroundCancel != nil {
		r.backgroundCancel()
	}
	r.background.Wait()
	clear(r.filters[:])
	err := r.capability.Close()
	r.capability = nil
	if a := r.accounting; a != nil {
		a.Lock()
		a.baseOwners--
		a.baseAssetBytes -= r.assetBytes
		a.baseDescriptorBytes -= r.descriptorBytes
		a.baseBackingBytes -= r.backingBytes
		if a.owners == 0 && a.baseOwners == 0 {
			a.states = nil
			a.limits = typedGraphReadOwnerLimits{}
		}
		a.Unlock()
		r.accounting = nil
	}
	return err
}

func (r *typedGraphCapturedBaseResources) holderRef() *columnVectorGraphSharedPreparedSearchRef {
	if r == nil || r.capability == nil {
		return nil
	}
	return r.capability.ref
}

func (r *typedGraphCapturedBaseResources) reserve(a *typedGraphReadOwnerAccounting, limits typedGraphReadOwnerLimits) error {
	a.Lock()
	defer a.Unlock()
	if (a.owners != 0 || a.baseOwners != 0) && a.limits != limits {
		return ErrVectorIndexSnapshotMismatch
	}
	remaining := limits.StateBytes - a.stateBytes - a.baseDescriptorBytes - a.baseBackingBytes
	if a.owners+a.baseOwners >= limits.Owners || r.assetBytes > limits.AssetBytes-a.assetBytes-a.baseAssetBytes || r.descriptorBytes > remaining || r.backingBytes > remaining-r.descriptorBytes {
		return errTypedGraphOwnerBudget
	}
	a.limits = limits
	a.baseOwners++
	a.baseAssetBytes += r.assetBytes
	a.baseDescriptorBytes += r.descriptorBytes
	a.baseBackingBytes += r.backingBytes
	r.accounting = a
	return nil
}

// Internal warm-before-owner seam, never called under the storage barrier.
// It is not a pinned-identity handshake: a concurrent base cutover can make the
// next current owner build a different holder. Current authority stays checked
// by openTypedGraphReadOwner; warming cannot authorize stale serving.
func (c *Collection) acquireTypedGraphCapturedBaseCache(index string, limits typedGraphReadOwnerLimits) (*collectionVectorIndexPreparedSearch, error) {
	return c.acquireTypedGraphCapturedBaseCacheWithContext(context.Background(), index, limits)
}

func (c *Collection) acquireTypedGraphCapturedBaseCacheWithContext(ctx context.Context, index string, limits typedGraphReadOwnerLimits) (*collectionVectorIndexPreparedSearch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateIndexName(index); err != nil {
		return nil, err
	}
	if !typedGraphReadOwnerLimitsValid(limits) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	prepared, _, _, err := c.acquireCollectionVectorIndexPreparedSearchSlot(VectorIndexSearchOptions{IndexName: index, Context: ctx}, collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}, func() (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, error) {
		p, err := c.openTypedGraphCapturedBaseCache(ctx, index, limits)
		return p, VectorIndexSearchResponse{}, err
	})
	if err != nil {
		return nil, err
	}
	prepared.mu.RLock()
	defer prepared.mu.RUnlock()
	if prepared.closed || prepared.capturedBase == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	a := prepared.capturedBase.accounting
	a.Lock()
	match := a.limits == limits
	a.Unlock()
	if !match {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	return prepared, nil
}

func (c *Collection) openTypedGraphCapturedBaseCache(ctx context.Context, index string, limits typedGraphReadOwnerLimits) (prepared *collectionVectorIndexPreparedSearch, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.db == nil || c.db.IsClosing() || !typedGraphReadOwnerLimitsValid(limits) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	state := coord.typedPublication.Load()
	policy := coord.typedGraphServing.Load()
	// Initial Ensure warms and physically admits this holder before publishing
	// servingAdmitted. The installed policy plus immutable metadata is the only
	// unadmitted entry point. Once admitted, the installed state is sufficient
	// authority for this package-private seam; a present policy must still match.
	// Public requests independently require both the policy and servingAdmitted.
	if state == nil || state.invalid || state.servingBase == nil ||
		(!state.servingAdmitted && (policy == nil || policy.index != index || policy.options.Owners != limits)) ||
		(policy != nil && (policy.index != index || policy.options.Owners != limits)) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	base := state.servingBase
	if base.preparedKey == "" || base.graph.RowCount <= 0 || base.view.Catalog == nil || len(base.view.Catalog.meta.VectorIndexes) != 1 || base.view.Catalog.meta.VectorIndexes[0].Name != index {
		return nil, errColumnVectorGraphSharedPreparedSearchNotEligible
	}
	graph, refs := base.graph, base.refs
	// Per decoded working term, not a claimed total Go heap bound. Installed
	// metadata already passed the cold manifest decoder; this separately bounds
	// the holder's known retained source shapes before any physical admission.
	for _, term := range [][2]int64{{int64(graph.Dimensions), 4}, {1, int64(reflect.TypeFor[DocumentRowRef]().Size())}, {int64(graph.M), 8}} {
		if term[0] <= 0 || term[0] > limits.Cold.DecodedTermBytes/term[1] || graph.RowCount < 0 || int64(graph.RowCount) > limits.Cold.DecodedTermBytes/(term[0]*term[1]) {
			return nil, errTypedGraphOverlayFoldNeeded
		}
	}
	backgroundCtx, backgroundCancel := context.WithCancel(context.Background())
	r := &typedGraphCapturedBaseResources{backgroundCtx: backgroundCtx, backgroundCancel: backgroundCancel}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.Close())
		}
	}()
	for _, ref := range refs {
		if ref.Length <= 0 || ref.Length > limits.Cold.AssetBytes-r.assetBytes {
			return nil, errTypedGraphOverlayFoldNeeded
		}
		r.assetBytes += ref.Length
		if int64(len(ref.Namespace)) > limits.StateBytes-r.descriptorBytes {
			return nil, errTypedGraphOwnerBudget
		}
		r.descriptorBytes += int64(len(ref.Namespace))
	}
	// Logical StateBytes covers this keeper's capability/ref/pin wrapper. The
	// holder/cache entry, exact guardian, authority copy, and segment table are
	// separately charged once in Physical.InventoryBytes.
	fixed := int64(reflect.TypeFor[typedGraphCapturedBaseResources]().Size() +
		reflect.TypeFor[typedGraphServingHolderCapability]().Size() +
		reflect.TypeFor[columnVectorGraphSharedPreparedSearchRef]().Size() +
		reflect.TypeFor[ColumnAssetLifecyclePinSet]().Size() +
		reflect.TypeFor[collectionVectorIndexPreparedSearch]().Size())
	refSize := int64(reflect.TypeFor[ColumnAssetRef]().Size())
	if fixed > limits.StateBytes-r.descriptorBytes || int64(len(refs)) > (limits.StateBytes-r.descriptorBytes-fixed)/refSize {
		return nil, errTypedGraphOwnerBudget
	}
	r.descriptorBytes += fixed + int64(len(refs))*refSize
	legacyScalarU8Descriptors := columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptors(base.view.Catalog.meta.VectorIndexes[0], base.view.VectorIndexState)
	r.backingBytes, err = typedGraphCapturedBaseBackingBoundWithLegacyScalarU8Assets(graph.RowCount, base.recordCount, graph.AdjacencyLayerCount, legacyScalarU8Descriptors, limits.StateBytes-r.descriptorBytes)
	if err != nil {
		return nil, err
	}
	// A hit retains one key in the wrapper in addition to the cache's key.
	if int64(len(base.preparedKey)) > (limits.StateBytes-r.descriptorBytes)/2 {
		return nil, errTypedGraphOwnerBudget
	}
	r.descriptorBytes += 2 * int64(len(base.preparedKey))
	if err := r.reserve(&coord.typedGraphOwners, limits); err != nil {
		return nil, err
	}
	var commitSeq, systemRoot uint64
	r.capability, commitSeq, systemRoot, err = base.acquireServingHolder(ctx, c, refs, "typed_graph_captured_base_cache", limits.Physical)
	if err != nil {
		return nil, err
	}
	prepared = &collectionVectorIndexPreparedSearch{
		family: collectionVectorIndexPreparedSearchFamilyCapturedBase, collection: c,
		indexName: index, commitSeq: commitSeq, systemRoot: systemRoot, capturedBase: r,
	}
	return prepared, nil
}

// Bounds retained source structs and explicit Go slice backing before mapping.
// Each used vector generation has a manifest record; source and prepared part
// pointer slices have exact capacities. Pack codec validates 8+2*layers sections
// plus two optional navigation sections. This deliberately excludes manager
// bookkeeping/allocator overhead and temporary decoder/validation maps; it is
// not a total process heap ceiling. Shared holders are charged conservatively
// per keeper and per read owner, including owners surviving keeper retirement.
func typedGraphCapturedBaseBackingBound(rows, records, layers int, limit int64) (int64, error) {
	return typedGraphCapturedBaseBackingBoundWithLegacyScalarU8Assets(rows, records, layers, nil, limit)
}

// typedGraphCapturedBaseBackingBoundWithLegacyScalarU8Assets extends the
// existing retained-holder bound with the maximum metadata that a shared
// holder can keep after an owner lazily requests legacy scalar_u8 v1. The code
// file bytes remain covered by typedGraphOwnerRefs/lifecycle pins; this adds
// only holder descriptors, entry/status/resource structs, one-column Prepared
// metadata, and the O(rows) derived code sums for each declared legacy plane.
// A first lazy request also retains the entry's unbuffered ready channel. Go
// 1.26's 64-bit hchan is 112 bytes; this bound covers the channel allocation on
// both known pointer widths without exposing runtime internals here.
const typedGraphLegacyScalarU8EntryReadyChannelBackingBound uintptr = 128

const (
	// The reader starts with an empty quantizedAssetStatus map. A Q2 attachment
	// fills its first group and stores an indirect
	// columnVectorGraphQuantizedAssetLoadStatus value. This includes that map
	// header/group plus its first value allocation on Go 1.26 amd64.
	typedGraphLegacyScalarU8ReaderAttachment64BitBound uintptr = 768

	// Keep a separately conservative future 32-bit bound: the map layout and
	// value indirection threshold differ from amd64.
	typedGraphLegacyScalarU8ReaderAttachment32BitBound uintptr = 2 << 10
)

func typedGraphLegacyScalarU8ReaderAttachmentBoundForPointerBytes(pointerBytes uintptr) (uintptr, bool) {
	switch pointerBytes {
	case 8:
		return typedGraphLegacyScalarU8ReaderAttachment64BitBound, true
	case 4:
		return typedGraphLegacyScalarU8ReaderAttachment32BitBound, true
	default:
		return 0, false
	}
}

func typedGraphLegacyScalarU8ReaderAttachmentBackingBound(legacyScalarU8Assets int, limit int64) (int64, error) {
	if legacyScalarU8Assets < 0 || limit < 0 {
		return 0, errTypedGraphOwnerBudget
	}
	if legacyScalarU8Assets == 0 {
		return 0, nil
	}
	perPlane, bounded := typedGraphLegacyScalarU8ReaderAttachmentBoundForPointerBytes(unsafe.Sizeof(uintptr(0)))
	if !bounded || int64(legacyScalarU8Assets) > limit/int64(perPlane) {
		return 0, errTypedGraphOwnerBudget
	}
	return int64(legacyScalarU8Assets) * int64(perPlane), nil
}

// typedGraphLegacyScalarU8DescriptorPayloadBytes accounts string backing
// transferred from a decoded VectorIndexState into a long-lived shared holder.
// The descriptor and calibration headers themselves are charged separately by
// the captured-base bound; this helper covers only their retained payload.
func typedGraphLegacyScalarU8DescriptorPayloadBytes(descriptors []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor, limit int64) (int64, error) {
	if limit < 0 {
		return 0, errTypedGraphOwnerBudget
	}
	var total int64
	addString := func(value string) bool {
		bytes := int64(len(value))
		if bytes < 0 || total > limit || bytes > limit-total {
			return false
		}
		total += bytes
		return true
	}
	for _, descriptor := range descriptors {
		if !addString(descriptor.definition.Name) ||
			!addString(descriptor.definition.Codec) ||
			!addString(descriptor.assets.Codes.Role) ||
			!addString(descriptor.assets.Codes.AssetID) ||
			!addString(descriptor.assets.Codes.LogicalType) ||
			!addString(descriptor.assets.Codes.PhysicalEncoding) ||
			!addString(string(descriptor.assets.Codes.Ref.Kind)) ||
			!addString(descriptor.assets.Codes.Ref.Namespace) {
			return 0, errTypedGraphOwnerBudget
		}
		if cfg := descriptor.definition.ScalarU8Calibration; cfg != nil &&
			(!addString(string(cfg.Mode)) || !addString(string(cfg.Grouping)) || !addString(string(cfg.AlphaPolicy.Name))) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	return total, nil
}

func typedGraphCapturedBaseBackingBoundWithLegacyScalarU8Assets(rows, records, layers int, legacyScalarU8Assets []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor, limit int64) (int64, error) {
	var total int64
	add := func(count int, size uintptr) bool {
		if count < 0 || size == 0 || int64(count) > (limit-total)/int64(size) {
			return false
		}
		total += int64(count) * int64(size)
		return true
	}
	if limit < 0 || rows < 0 || records < 0 || layers < 0 {
		return 0, errTypedGraphOwnerBudget
	}
	for _, size := range []uintptr{
		reflect.TypeFor[columnVectorGraphSharedPreparedSearch]().Size(),
		reflect.TypeFor[columnVectorGraphTypedColumnVectorSource]().Size(),
		reflect.TypeFor[columnVectorGraphInvNormStateSource]().Size(),
		reflect.TypeFor[columnVectorGraphRowRefStateSource]().Size(),
		reflect.TypeFor[columnVectorGraphDocumentIDStateSource]().Size(),
		reflect.TypeFor[columnVectorGraphAdjacencyDirectSources]().Size(),
		reflect.TypeFor[columnVectorGraphPreparedSearchView]().Size(),
		reflect.TypeFor[columnHNSWSearchPackPreparedView]().Size(),
	} {
		if !add(1, size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	parts := min(rows, records)
	for _, term := range []struct {
		n    int
		size uintptr
	}{
		{rows, reflect.TypeFor[columnVectorGraphTypedColumnVectorLocation]().Size()},
		{rows, 2 * reflect.TypeFor[uint32]().Size()},
		{parts, reflect.TypeFor[columnVectorGraphTypedColumnVectorPart]().Size() + 2*reflect.TypeFor[*columnVectorGraphTypedColumnVectorPart]().Size()},
		{layers, reflect.TypeFor[columnVectorGraphLayer0AdjacencyDirectSource]().Size() + reflect.TypeFor[*columnVectorGraphLayer0AdjacencyDirectSource]().Size() + reflect.TypeFor[columnHNSWSearchPackPreparedLayer]().Size()},
		{layers, 2 * reflect.TypeFor[columnHNSWSearchPackSection]().Size()},
		{10, reflect.TypeFor[columnHNSWSearchPackSection]().Size()},
	} {
		if !add(term.n, term.size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	if len(legacyScalarU8Assets) > 0 {
		preparedMetadataBound, preparedMetadataBounded := quantizedasset.PreparedOneColumnRetainedMetadataBound()
		if !preparedMetadataBounded {
			return 0, errTypedGraphOwnerBudget
		}
		// The descriptor slice and request-entry slice retain exactly one slot per
		// declared legacy plane. Entries are allocated lazily, but a captured keeper
		// can outlive the owner that first populates them, so admit their maximum
		// safely at holder construction time.
		for _, size := range []uintptr{
			reflect.TypeFor[columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor]().Size(),
			reflect.TypeFor[*columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry]().Size(),
			reflect.TypeFor[columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry]().Size(),
			typedGraphLegacyScalarU8EntryReadyChannelBackingBound,
			reflect.TypeFor[columnVectorGraphQuantizedAssetResource]().Size(),
			preparedMetadataBound,
			reflect.TypeFor[ScalarU8CalibrationConfig]().Size(),
		} {
			if !add(len(legacyScalarU8Assets), size) {
				return 0, errTypedGraphOwnerBudget
			}
		}
	}
	descriptorPayloadBytes, err := typedGraphLegacyScalarU8DescriptorPayloadBytes(legacyScalarU8Assets, limit-total)
	if err != nil {
		return 0, err
	}
	total += descriptorPayloadBytes
	// Resource loading derives one uint32 sum per base row. Keep this checked
	// separately so the product cannot overflow and no query can add an
	// unadmitted O(N) slice after its owner was accepted.
	if rows > 0 && len(legacyScalarU8Assets) > 0 {
		remaining := limit - total
		sumSize := int64(reflect.TypeFor[uint32]().Size())
		if sumSize <= 0 || int64(rows) > remaining/sumSize {
			return 0, errTypedGraphOwnerBudget
		}
		perPlane := int64(rows) * sumSize
		if perPlane <= 0 || int64(len(legacyScalarU8Assets)) > remaining/perPlane {
			return 0, errTypedGraphOwnerBudget
		}
		total += int64(len(legacyScalarU8Assets)) * perPlane
	}
	return total, nil
}
