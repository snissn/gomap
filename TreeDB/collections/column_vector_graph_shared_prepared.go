package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

var errColumnVectorGraphSharedPreparedSearchNotEligible = errors.New("collections: column_graph shared prepared search requires an admitted direct prepared view")

// columnVectorGraphSharedPreparedSearch is the local #1735 seam for immutable
// prepared column_graph state. It is intentionally a collection-scoped,
// ref-counted owner around the existing generic mappedresource handles and
// prepared metadata; it does not define a new vector sidecar lifecycle. The key
// includes graph/vector-index/base-manifest identity so a future DB-wide
// mapped-resource manager can replace the backing owner without changing search
// semantics.
type columnVectorGraphSharedPreparedSearchKeyFamily uint8

const (
	columnVectorGraphSharedPreparedSearchKeyGeneric columnVectorGraphSharedPreparedSearchKeyFamily = iota + 1
	columnVectorGraphSharedPreparedSearchKeyServing
)

// The serving family is deliberately typed and comparable. Its closure digest
// is binary, not a concatenated display string, and its DB pointer prevents a
// reopened database at the same path from sharing process-local authority.
type columnVectorGraphSharedPreparedSearchKey struct {
	family     columnVectorGraphSharedPreparedSearchKeyFamily
	db         *backenddb.DB
	assetRoot  string
	collection string
	namespace  string
	logical    string
	refsDigest [32]byte
	refsCount  int
}

func genericColumnVectorGraphSharedPreparedSearchKey(logical string) columnVectorGraphSharedPreparedSearchKey {
	return columnVectorGraphSharedPreparedSearchKey{family: columnVectorGraphSharedPreparedSearchKeyGeneric, logical: logical}
}

func (k columnVectorGraphSharedPreparedSearchKey) valid() bool {
	switch k.family {
	case columnVectorGraphSharedPreparedSearchKeyGeneric:
		return k.logical != "" && k.db == nil && k.assetRoot == "" && k.collection == "" && k.namespace == "" && k.refsDigest == [32]byte{} && k.refsCount == 0
	case columnVectorGraphSharedPreparedSearchKeyServing:
		// A SHA-256 digest may legitimately be all zero bits. Non-empty exact
		// authority is represented independently by refsCount.
		return k.db != nil && k.assetRoot != "" && k.collection != "" && k.namespace != "" && k.logical != "" && k.refsCount > 0
	default:
		return false
	}
}

type columnVectorGraphSharedPreparedSearch struct {
	key columnVectorGraphSharedPreparedSearchKey

	typedVectorSource     *columnVectorGraphTypedColumnVectorSource
	invNormSource         *columnVectorGraphInvNormStateSource
	rowRefSource          *columnVectorGraphRowRefStateSource
	documentIDSource      *columnVectorGraphDocumentIDStateSource
	hnswSearchPack        *columnHNSWSearchPackPreparedView
	hnswSearchPackStatus  columnHNSWSearchPackPreparedStatus
	hnswSearchPackNanos   uint64
	adjacencyLayerSources *columnVectorGraphAdjacencyDirectSources
	preparedSearch        *columnVectorGraphPreparedSearchView
	servingSegments       *columnServingSegmentLeaseSet
	servingSourceAccess   *columnVectorGraphSourceAccess

	// legacyScalarU8Assets is immutable holder identity metadata captured with
	// the same vector-index state that produced key. It is deliberately only a
	// declaration/resource locator: actual code-plane resources are requested
	// below, one legacy scalar_u8 v1 name at a time.
	legacyScalarU8Assets []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor

	legacyScalarU8Mu      sync.Mutex
	legacyScalarU8Closed  bool
	legacyScalarU8Entries []*columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry
}

// columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor records the
// immutable declaration and code-plane identity that a shared holder may serve.
// It never implies that the code plane is ready; a separate request is required
// before a reader can use scalar_u8 scoring.
type columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor struct {
	definition QuantizedVectorIndexDefinition
	assets     columnVectorGraphQuantizedAssetSet
}

// Entries are synchronized independently of the collection prepared-holder
// cache. In particular, I/O for a first scalar-u8 request must never run while
// vectorPreparedSearchMu is held.
type columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry struct {
	descriptor columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor
	ready      chan struct{}
	building   bool
	status     columnVectorGraphQuantizedAssetLoadStatus
	err        error
}

// columnVectorGraphLegacyScalarU8ZeroRowValidationCache keeps the exceptional
// empty-base contract tied to immutable serving metadata. A zero-row graph has
// no exact prepared holder and must not fabricate a resource/status solely to
// run a selected suffix-only request, but its published scalar code image is
// still an authoritative selected asset that must parse and validate once.
//
// Entries are fixed at publication time, keyed by the full copied declaration
// and asset identity rather than by index name. They retain only a successful
// validation bit, never a code image, prepared scorer, or mapped handle.
// Failures are deliberately retried: a transient asset-read failure must not
// poison every later request for an otherwise immutable serving publication.
type columnVectorGraphLegacyScalarU8ZeroRowValidationCache struct {
	descriptors []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor

	mu      sync.Mutex
	cond    *sync.Cond
	entries []columnVectorGraphLegacyScalarU8ZeroRowValidationEntry
}

type columnVectorGraphLegacyScalarU8ZeroRowValidationEntry struct {
	building  bool
	completed bool
}

func newColumnVectorGraphLegacyScalarU8ZeroRowValidationCache(def VectorIndexDefinition, state columnVectorIndexStateSnapshot) *columnVectorGraphLegacyScalarU8ZeroRowValidationCache {
	descriptors := columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptors(def, state)
	if len(descriptors) == 0 {
		return nil
	}
	cache := &columnVectorGraphLegacyScalarU8ZeroRowValidationCache{
		descriptors: descriptors,
		entries:     make([]columnVectorGraphLegacyScalarU8ZeroRowValidationEntry, len(descriptors)),
	}
	// Keep one fixed waiter primitive for every immutable publication. It
	// serializes first validation without growing a name map, a code-plane cache,
	// or a retry-generation allocation after read-owner admission.
	cache.cond = sync.NewCond(&cache.mu)
	return cache
}

func (c *columnVectorGraphLegacyScalarU8ZeroRowValidationCache) waitWithContext(ctx context.Context) error {
	if c == nil || c.cond == nil {
		return errColumnVectorGraphQuantizedAssetInvalid
	}
	var stop func() bool
	if ctx.Done() != nil {
		stop = context.AfterFunc(ctx, func() {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
		})
	}
	c.cond.Wait()
	if stop != nil {
		stop()
	}
	return ctx.Err()
}

// validate serializes the one bounded parse/prepare check for a selected
// zero-row legacy plane. The caller supplies the existing asset loader so this
// cache owns no collection, snapshot, reader, or resource lifetime.
func (c *columnVectorGraphLegacyScalarU8ZeroRowValidationCache) validate(name string, definition QuantizedVectorIndexDefinition, validate func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error) error {
	return c.validateWithContext(context.Background(), name, definition, validate)
}

// validateWithContext keeps a public waiter cancellable while another request
// performs the one bounded zero-row asset validation. The validation itself is
// deliberately still single-flight; cancellation only abandons this caller,
// leaving the immutable publication's result available to later callers.
func (c *columnVectorGraphLegacyScalarU8ZeroRowValidationCache) validateWithContext(ctx context.Context, name string, definition QuantizedVectorIndexDefinition, validate func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error) error {
	if c == nil || name == "" || validate == nil {
		return fmt.Errorf("%w: zero-row legacy scalar_u8 validation request is invalid", errColumnVectorGraphQuantizedAssetInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	index := -1
	for i, descriptor := range c.descriptors {
		if descriptor.definition.Name == name && quantizedVectorIndexDefinitionValuesEqual(descriptor.definition, definition) {
			index = i
			break
		}
	}
	if index < 0 || index >= len(c.entries) {
		return fmt.Errorf("%w: zero-row legacy scalar_u8 asset %q does not match immutable serving metadata", errColumnVectorGraphQuantizedAssetStale, name)
	}
	c.mu.Lock()
	for {
		entry := &c.entries[index]
		if entry.completed {
			c.mu.Unlock()
			return nil
		}
		if entry.building {
			if err := c.waitWithContext(ctx); err != nil {
				c.mu.Unlock()
				return err
			}
			continue
		}
		entry.building = true
		descriptor := c.descriptors[index]
		c.mu.Unlock()

		// Parsing and schema validation may read the immutable asset; never hold
		// metadata synchronization while doing that I/O.
		err := ctx.Err()
		if err == nil {
			err = validate(descriptor)
		}

		c.mu.Lock()
		entry.building = false
		if err == nil {
			entry.completed = true
		}
		c.cond.Broadcast()
		c.mu.Unlock()
		return err
	}
}

func (c *columnVectorGraphLegacyScalarU8ZeroRowValidationCache) retainedCapacities() (descriptors, entries int) {
	if c == nil {
		return 0, 0
	}
	return cap(c.descriptors), cap(c.entries)
}

type columnVectorGraphSharedPreparedSearchRef struct {
	collection *Collection
	key        columnVectorGraphSharedPreparedSearchKey
	holder     *columnVectorGraphSharedPreparedSearch
	once       sync.Once
}

type columnVectorGraphSharedPreparedSearchCacheEntry struct {
	ready         chan struct{}
	building      bool
	serving       bool
	holder        *columnVectorGraphSharedPreparedSearch
	err           error
	refs          int
	waiters       int
	servingLimits typedGraphPhysicalResourceLimits
}

type columnVectorGraphSharedPreparedSearchCacheSnapshot struct {
	Entries                    int
	Refs                       int
	BuildingEntries            int
	ActiveHandles              int64
	ActiveMappedBytes          int64
	ActiveHeapCopyBytes        int64
	ActiveDerivedMetadataBytes int64
	TotalAcquires              uint64
	TotalReleases              uint64
	CacheHits                  uint64
	CacheMisses                uint64
	CacheWaits                 uint64
	CacheBuilds                uint64
	Hits                       uint64
	Misses                     uint64
	FallbackReads              uint64
	Opens                      uint64
	Closes                     uint64
	Errors                     uint64
}

func (c *Collection) acquireColumnVectorGraphSharedPreparedSearch(key string, build func() (*columnVectorGraphSharedPreparedSearch, error)) (*columnVectorGraphSharedPreparedSearchRef, error) {
	return c.acquireColumnVectorGraphSharedPreparedSearchWithContext(context.Background(), genericColumnVectorGraphSharedPreparedSearchKey(key), build)
}

func (c *Collection) acquireColumnVectorGraphSharedPreparedSearchWithContext(ctx context.Context, key columnVectorGraphSharedPreparedSearchKey, build func() (*columnVectorGraphSharedPreparedSearch, error)) (*columnVectorGraphSharedPreparedSearchRef, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if !key.valid() {
		return nil, errors.New("collections: column_graph shared prepared search key is empty")
	}
	if build == nil {
		return nil, errors.New("collections: column_graph shared prepared search build function is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for {
		c.vectorPreparedSearchMu.Lock()
		if c.vectorPreparedSearch == nil {
			c.vectorPreparedSearch = make(map[columnVectorGraphSharedPreparedSearchKey]*columnVectorGraphSharedPreparedSearchCacheEntry)
		}
		entry := c.vectorPreparedSearch[key]
		if entry == nil {
			entry = &columnVectorGraphSharedPreparedSearchCacheEntry{ready: make(chan struct{}), building: true}
			c.vectorPreparedSearch[key] = entry
			c.vectorPreparedSearchMisses++
			c.vectorPreparedSearchBuilds++
			c.vectorPreparedSearchMu.Unlock()

			holder, err := build()
			if err == nil && holder == nil {
				err = errors.New("collections: column_graph shared prepared search build returned nil holder")
			}
			if err == nil {
				holder.key = key
			}

			c.vectorPreparedSearchMu.Lock()
			entry.holder = holder
			entry.err = err
			entry.building = false
			if err != nil {
				if !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
					delete(c.vectorPreparedSearch, key)
				}
			} else {
				entry.refs = 1
			}
			close(entry.ready)
			c.vectorPreparedSearchMu.Unlock()
			if err != nil {
				return nil, err
			}
			return &columnVectorGraphSharedPreparedSearchRef{collection: c, key: key, holder: holder}, nil
		}
		ready := entry.ready
		if entry.building {
			c.vectorPreparedSearchWaits++
			c.vectorPreparedSearchMu.Unlock()
			select {
			case <-ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		if entry.err != nil {
			err := entry.err
			if !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
				delete(c.vectorPreparedSearch, key)
			}
			c.vectorPreparedSearchMu.Unlock()
			return nil, err
		}
		if entry.holder == nil || !entry.holder.ready() {
			delete(c.vectorPreparedSearch, key)
			c.vectorPreparedSearchMu.Unlock()
			continue
		}
		entry.refs++
		c.vectorPreparedSearchHits++
		holder := entry.holder
		c.vectorPreparedSearchMu.Unlock()
		return &columnVectorGraphSharedPreparedSearchRef{collection: c, key: key, holder: holder}, nil
	}
}

func (r *columnVectorGraphSharedPreparedSearchRef) release() error {
	if r == nil {
		return nil
	}
	var closeErr error
	r.once.Do(func() {
		c := r.collection
		if c == nil || !r.key.valid() {
			return
		}
		var holder *columnVectorGraphSharedPreparedSearch
		c.vectorPreparedSearchMu.Lock()
		entry := c.vectorPreparedSearch[r.key]
		if entry != nil && entry.holder == r.holder {
			if entry.refs > 0 {
				entry.refs--
			}
			if entry.refs == 0 && entry.waiters == 0 && !entry.building {
				holder = entry.holder
				delete(c.vectorPreparedSearch, r.key)
			}
		}
		c.vectorPreparedSearchMu.Unlock()
		if holder != nil {
			closeErr = c.closeColumnVectorGraphSharedPreparedSearchHolder(holder)
		}
	})
	return closeErr
}

func newColumnVectorGraphSharedPreparedSearchFromReader(reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphSharedPreparedSearch, error) {
	if reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	if columnVectorGraphManifestHasPhysicalAsset(reader.graph) {
		return nil, errors.New("collections: column_graph shared prepared search is disabled for legacy graph-row assets")
	}
	if reader.preparedSearch == nil {
		return nil, errColumnVectorGraphSharedPreparedSearchNotEligible
	}
	if !reader.preparedSearch.ready() {
		return nil, errors.New("collections: column_graph shared prepared search requires ready combined prepared view")
	}
	if err := reader.preparedSearch.validateLive(); err != nil {
		return nil, err
	}
	holder := &columnVectorGraphSharedPreparedSearch{
		typedVectorSource:     reader.typedVectorSource,
		invNormSource:         reader.invNormSource,
		rowRefSource:          reader.rowRefSource,
		documentIDSource:      reader.documentIDSource,
		hnswSearchPack:        reader.hnswSearchPack,
		hnswSearchPackStatus:  reader.hnswSearchPackStatus,
		hnswSearchPackNanos:   reader.hnswSearchPackOpenNanos,
		adjacencyLayerSources: reader.adjacencyLayerSources,
		preparedSearch:        reader.preparedSearch,
		servingSourceAccess:   reader.sourceAccess,
		legacyScalarU8Assets:  reader.sharedPreparedLegacyScalarU8Assets,
	}
	if !holder.ready() {
		return nil, errors.New("collections: column_graph shared prepared search holder is not ready")
	}
	// Ownership of immutable sources transfers to the shared holder. The build
	// reader is temporary; the returned worker reader attaches via a ref below.
	reader.typedVectorSource = nil
	reader.invNormSource = nil
	reader.rowRefSource = nil
	reader.documentIDSource = nil
	reader.hnswSearchPack = nil
	reader.adjacencyLayerSources = nil
	reader.layer0AdjacencySource = nil
	reader.preparedSearch = nil
	reader.sourceAccess = nil
	reader.sharedPreparedLegacyScalarU8Assets = nil
	return holder, nil
}

func columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptors(def VectorIndexDefinition, state columnVectorIndexStateSnapshot) []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor {
	if len(def.QuantizedIndexes) == 0 {
		return nil
	}
	byName := columnVectorGraphQuantizedAssetSetsByName(state, def)
	out := make([]columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor, 0, columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptorCount(def))
	for _, q := range def.QuantizedIndexes {
		if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 || !scalarU8CalibrationIsLegacy(q) {
			continue
		}
		copied := q
		copied.ScalarU8Calibration = scalarU8CalibrationConfigClone(q.ScalarU8Calibration)
		out = append(out, columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{
			definition: copied,
			assets:     byName[q.Name],
		})
	}
	return out
}

func columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptorCount(def VectorIndexDefinition) int {
	count := 0
	for _, q := range def.QuantizedIndexes {
		if q.Codec == QuantizedVectorCodecScalarU8 && q.Version == 1 && scalarU8CalibrationIsLegacy(q) {
			count++
		}
	}
	return count
}

func (h *columnVectorGraphSharedPreparedSearch) legacyScalarU8AssetDescriptor(name string, definition QuantizedVectorIndexDefinition) (columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor, bool) {
	if h == nil || name == "" {
		return columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{}, false
	}
	for _, descriptor := range h.legacyScalarU8Assets {
		if descriptor.definition.Name == name && quantizedVectorIndexDefinitionValuesEqual(descriptor.definition, definition) {
			return descriptor, true
		}
	}
	return columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{}, false
}

func columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptorEqual(a, b columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) bool {
	return quantizedVectorIndexDefinitionValuesEqual(a.definition, b.definition) && a.assets == b.assets
}

func columnVectorGraphSharedPreparedLegacyScalarU8AssetStatusReady(status columnVectorGraphQuantizedAssetLoadStatus, descriptor columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) bool {
	q := descriptor.definition
	return q.Codec == QuantizedVectorCodecScalarU8 && q.Version == 1 && scalarU8CalibrationIsLegacy(q) &&
		status.Err == nil && status.Prepared != nil && status.resource != nil && status.ownsResource &&
		quantizedVectorIndexDefinitionValuesEqual(status.Definition, q) && status.Asset == descriptor.assets.Codes
}

// removeLegacyScalarU8AssetEntryLocked removes one terminal failed entry while
// preserving the fixed declared-plane backing capacity. legacyScalarU8Mu must
// be held. Clearing the old slot is important: the removed entry may otherwise
// retain its descriptor and ready channel until the holder is released.
func (h *columnVectorGraphSharedPreparedSearch) removeLegacyScalarU8AssetEntryLocked(entry *columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry) {
	if h == nil || entry == nil {
		return
	}
	for i, candidate := range h.legacyScalarU8Entries {
		if candidate != entry {
			continue
		}
		last := len(h.legacyScalarU8Entries) - 1
		copy(h.legacyScalarU8Entries[i:], h.legacyScalarU8Entries[i+1:])
		h.legacyScalarU8Entries[last] = nil
		h.legacyScalarU8Entries = h.legacyScalarU8Entries[:last]
		return
	}
}

// acquireLegacyScalarU8Asset serializes one requested code-plane load per
// holder/name. The holder is already selected by the complete prepared-search
// key, while descriptor equality prevents a same name from being attached to a
// different declaration or physical resource identity.
func (h *columnVectorGraphSharedPreparedSearch) acquireLegacyScalarU8Asset(descriptor columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor, load func() (columnVectorGraphQuantizedAssetLoadStatus, error)) (columnVectorGraphQuantizedAssetLoadStatus, error) {
	return h.acquireLegacyScalarU8AssetWithContext(context.Background(), descriptor, load)
}

// acquireLegacyScalarU8AssetWithContext coalesces immutable code-plane loads
// without making a canceled public waiter retain its already-admitted owner.
// A first loader remains single-flight; only the caller's wait is abandoned.
func (h *columnVectorGraphSharedPreparedSearch) acquireLegacyScalarU8AssetWithContext(ctx context.Context, descriptor columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor, load func() (columnVectorGraphQuantizedAssetLoadStatus, error)) (columnVectorGraphQuantizedAssetLoadStatus, error) {
	if h == nil || descriptor.definition.Name == "" || load == nil {
		return columnVectorGraphQuantizedAssetLoadStatus{}, fmt.Errorf("%w: shared legacy scalar_u8 asset request is invalid", errColumnVectorGraphQuantizedAssetInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return columnVectorGraphQuantizedAssetLoadStatus{}, err
		}
		h.legacyScalarU8Mu.Lock()
		if h.legacyScalarU8Closed {
			h.legacyScalarU8Mu.Unlock()
			return columnVectorGraphQuantizedAssetLoadStatus{}, fmt.Errorf("%w: shared legacy scalar_u8 asset holder is closed", errColumnVectorGraphQuantizedAssetClosed)
		}
		var entry *columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry
		for _, candidate := range h.legacyScalarU8Entries {
			if candidate != nil && candidate.descriptor.definition.Name == descriptor.definition.Name {
				entry = candidate
				break
			}
		}
		if entry == nil {
			if h.legacyScalarU8Entries == nil {
				h.legacyScalarU8Entries = make([]*columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry, 0, len(h.legacyScalarU8Assets))
			}
			entry = &columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry{
				descriptor: descriptor,
				ready:      make(chan struct{}),
				building:   true,
			}
			h.legacyScalarU8Entries = append(h.legacyScalarU8Entries, entry)
			h.legacyScalarU8Mu.Unlock()
			if err := ctx.Err(); err != nil {
				h.legacyScalarU8Mu.Lock()
				entry.status, entry.err, entry.building = columnVectorGraphQuantizedAssetLoadStatus{}, err, false
				close(entry.ready)
				h.removeLegacyScalarU8AssetEntryLocked(entry)
				h.legacyScalarU8Mu.Unlock()
				return columnVectorGraphQuantizedAssetLoadStatus{}, err
			}

			// Do not hold either the collection prepared-holder mutex or this
			// holder mutex while mapping/parsing the requested code plane.
			status, err := load()
			if err == nil {
				err = ctx.Err()
			}
			if err == nil && !columnVectorGraphSharedPreparedLegacyScalarU8AssetStatusReady(status, descriptor) {
				err = fmt.Errorf("%w: shared legacy scalar_u8 asset %q did not produce a ready resource", errColumnVectorGraphQuantizedAssetInvalid, descriptor.definition.Name)
			}
			if err != nil {
				// A normal loader already releases a partially opened resource on
				// error, but retain that invariant at this synchronization seam
				// for every loader. Failed entries are immediately forgettable.
				closeErr := status.close()
				status.ownsResource = false
				err = errors.Join(err, closeErr)
			}

			h.legacyScalarU8Mu.Lock()
			if h.legacyScalarU8Closed {
				h.legacyScalarU8Mu.Unlock()
				// Holder close is waiting on ready. Release the just-created owner
				// before waking it, then publish a non-owning terminal status so it
				// cannot perform a second resource release.
				closeErr := status.close()
				status.ownsResource = false
				closedErr := errors.Join(err, fmt.Errorf("%w: shared legacy scalar_u8 asset holder closed while loading", errColumnVectorGraphQuantizedAssetClosed))
				h.legacyScalarU8Mu.Lock()
				entry.status, entry.err, entry.building = status, closedErr, false
				close(entry.ready)
				h.legacyScalarU8Mu.Unlock()
				return columnVectorGraphQuantizedAssetLoadStatus{}, errors.Join(closedErr, closeErr)
			}
			entry.status, entry.err, entry.building = status, err, false
			if err != nil {
				// Publish first so waiters leave their channel receive, then
				// remove this exact failed generation before they re-lock and
				// coalesce on one fresh retry.
				close(entry.ready)
				h.removeLegacyScalarU8AssetEntryLocked(entry)
				h.legacyScalarU8Mu.Unlock()
				return status, err
			}
			close(entry.ready)
			h.legacyScalarU8Mu.Unlock()
			return status, nil
		}
		if !columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptorEqual(entry.descriptor, descriptor) {
			h.legacyScalarU8Mu.Unlock()
			return columnVectorGraphQuantizedAssetLoadStatus{}, fmt.Errorf("%w: shared legacy scalar_u8 asset %q identity does not match holder", errColumnVectorGraphQuantizedAssetStale, descriptor.definition.Name)
		}
		if entry.building {
			ready := entry.ready
			h.legacyScalarU8Mu.Unlock()
			select {
			case <-ready:
			case <-ctx.Done():
				return columnVectorGraphQuantizedAssetLoadStatus{}, ctx.Err()
			}
			continue
		}
		status, err := entry.status, entry.err
		h.legacyScalarU8Mu.Unlock()
		if err != nil {
			return status, err
		}
		if !columnVectorGraphSharedPreparedLegacyScalarU8AssetStatusReady(status, descriptor) {
			return status, fmt.Errorf("%w: shared legacy scalar_u8 asset %q is not ready", errColumnVectorGraphQuantizedAssetClosed, descriptor.definition.Name)
		}
		return status, nil
	}
}

func (h *columnVectorGraphSharedPreparedSearch) ready() bool {
	// Combined readiness includes any borrowed pack and persisted inverse.
	// Pack presence alone cannot admit the native counted-source fallback.
	return h != nil && h.typedVectorSource != nil && h.invNormSource != nil && h.rowRefSource != nil && h.documentIDSource != nil && h.adjacencyLayerSources != nil && h.preparedSearch != nil && h.preparedSearch.ready() && (h.key.family != columnVectorGraphSharedPreparedSearchKeyServing || (h.servingSegments != nil && h.servingSourceAccess != nil && h.servingSourceAccess.pool == h.servingSegments))
}

func (h *columnVectorGraphSharedPreparedSearch) close() error {
	if h == nil {
		return nil
	}
	var closeErr error
	// A holder reaches close only after its final shared ref releases. Mark the
	// independent scalar-u8 request set closed first, then wait outside its lock
	// for an in-flight first request before closing the one holder-owned
	// resource. Attached readers carry ownsResource=false and therefore cannot
	// double-close this status.
	h.legacyScalarU8Mu.Lock()
	entries := h.legacyScalarU8Entries
	h.legacyScalarU8Entries = nil
	alreadyClosed := h.legacyScalarU8Closed
	h.legacyScalarU8Closed = true
	h.legacyScalarU8Mu.Unlock()
	if !alreadyClosed {
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			h.legacyScalarU8Mu.Lock()
			building, ready := entry.building, entry.ready
			status := entry.status
			h.legacyScalarU8Mu.Unlock()
			if building {
				<-ready
				h.legacyScalarU8Mu.Lock()
				status = entry.status
				h.legacyScalarU8Mu.Unlock()
			}
			closeErr = errors.Join(closeErr, status.close())
		}
	}
	// The prepared view is non-owning and may point into every source below.
	// Make it unreachable before releasing any logical source handle.
	h.preparedSearch = nil
	if h.adjacencyLayerSources != nil {
		closeErr = errors.Join(closeErr, h.adjacencyLayerSources.Close())
		h.adjacencyLayerSources = nil
	}
	if h.documentIDSource != nil {
		closeErr = errors.Join(closeErr, h.documentIDSource.Close())
		h.documentIDSource = nil
	}
	if h.rowRefSource != nil {
		closeErr = errors.Join(closeErr, h.rowRefSource.Close())
		h.rowRefSource = nil
	}
	if h.typedVectorSource != nil {
		closeErr = errors.Join(closeErr, h.typedVectorSource.Close())
		h.typedVectorSource = nil
	}
	if h.invNormSource != nil {
		closeErr = errors.Join(closeErr, h.invNormSource.Close())
		h.invNormSource = nil
	}
	// Pack is an owning source beneath prepared/adjacency borrowers.
	if h.hnswSearchPack != nil {
		closeErr = errors.Join(closeErr, h.hnswSearchPack.Close())
		h.hnswSearchPack = nil
	}
	h.servingSourceAccess = nil
	if h.servingSegments != nil {
		closeErr = errors.Join(closeErr, h.servingSegments.Close())
		if !h.servingSegments.cleanupRetained() {
			h.servingSegments = nil
		}
	}
	return closeErr
}

func (h *columnVectorGraphSharedPreparedSearch) detachRetainedServingSegments() *columnServingSegmentLeaseSet {
	if h == nil || h.servingSegments == nil || !h.servingSegments.cleanupRetained() {
		return nil
	}
	set := h.servingSegments
	h.servingSegments = nil
	return set
}

func (c *Collection) closeColumnVectorGraphSharedPreparedSearchHolder(holder *columnVectorGraphSharedPreparedSearch) error {
	var err error
	if holder != nil {
		err = holder.close()
		if retained := holder.detachRetainedServingSegments(); retained != nil {
			return errors.Join(err, retained.ledger.quarantine(retained, nil, err))
		}
	}
	return err
}

func (r *columnVectorGraphPhysicalRowReader) attachSharedPreparedSearch(ref *columnVectorGraphSharedPreparedSearchRef) error {
	if r == nil {
		return errNilColumnVectorGraphPhysicalRowReader
	}
	if ref == nil || !ref.key.valid() || ref.holder == nil || !ref.holder.ready() {
		return errors.New("collections: column_graph shared prepared search ref is not ready")
	}
	h := ref.holder
	r.sharedPreparedSearch = ref
	r.typedVectorSource = h.typedVectorSource
	r.invNormSource = h.invNormSource
	r.rowRefSource = h.rowRefSource
	r.documentIDSource = h.documentIDSource
	r.hnswSearchPack = h.hnswSearchPack
	r.hnswSearchPackStatus = h.hnswSearchPackStatus
	r.hnswSearchPackOpenNanos = h.hnswSearchPackNanos
	r.adjacencyLayerSources = h.adjacencyLayerSources
	if h.adjacencyLayerSources != nil && len(h.adjacencyLayerSources.sources) > 0 {
		r.layer0AdjacencySource = h.adjacencyLayerSources.sources[0]
	}
	r.preparedSearch = h.preparedSearch
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) attachServingPreparedSearch(capability *typedGraphServingHolderCapability) error {
	if capability == nil || capability.ref == nil || capability.ref.key.family != columnVectorGraphSharedPreparedSearchKeyServing {
		return errors.New("collections: column_graph serving prepared capability is not ready")
	}
	if err := r.attachSharedPreparedSearch(capability.ref); err != nil {
		return err
	}
	r.sharedServingHolder = capability
	return nil
}

// requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8Asset acquires
// exactly one declared legacy scalar_u8 v1 code plane through the reader's
// existing full-identity shared holder, then attaches a non-owning status copy
// to that reader. It is intentionally separate from exact holder readiness:
// unavailable scalar assets must not poison exact search state.
func (c *Collection) requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8Asset(reader *columnVectorGraphPhysicalRowReader, name string) error {
	return c.requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8AssetWithContext(context.Background(), reader, name)
}

func (c *Collection) requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8AssetWithContext(ctx context.Context, reader *columnVectorGraphPhysicalRowReader, name string) error {
	if c == nil || c.db == nil || reader == nil || name == "" {
		return fmt.Errorf("%w: shared legacy scalar_u8 asset request is invalid", ErrVectorIndexSearchUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	q, ok := findQuantizedVectorIndex(reader.def, name)
	if !ok {
		return fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, reader.def.Name, name)
	}
	if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 || !scalarU8CalibrationIsLegacy(q) {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q must be legacy scalar_u8 v1", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetInvalid, reader.def.Name, name)
	}
	if reader.catalog == nil || reader.catalog.meta.Name == "" || reader.catalog.meta.Name != c.collectionName() || reader.catalog.meta.Options.ColumnStore == nil {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q has no matching collection asset authority", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, reader.def.Name, name)
	}
	// A suffix-only typed owner has no exact shared holder, but the zero-row
	// scalar image is still selected authority. Validate its full immutable
	// descriptor and payload before K=0/empty/suffix shortcuts without creating
	// a persistent resource or pretending an empty scorer is ready.
	if reader.RowCount() == 0 {
		cache := reader.zeroRowLegacyScalarU8Validation
		if cache == nil {
			return fmt.Errorf("%w: %w: column_graph %q quantized index %q has no zero-row scalar-u8 validation metadata", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, reader.def.Name, name)
		}
		err := cache.validateWithContext(ctx, name, q, func(descriptor columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error {
			if !descriptor.assets.HasCodes || descriptor.assets.Codes.AssetID != columnVectorGraphQuantizedCodesAssetID(q) {
				return fmt.Errorf("%w: quantized asset %q has no matching zero-row scalar_u8 code-plane identity", errColumnVectorGraphQuantizedAssetMissing, name)
			}
			prepared, loadErr := loadColumnVectorGraphQuantizedAssetSet(c.db.ColumnAssetRootDir(), reader.catalog.meta.Name, *reader.catalog.meta.Options.ColumnStore, reader.def, reader.graph, descriptor.definition, descriptor.assets)
			if loadErr != nil {
				return loadErr
			}
			rows := -1
			if prepared != nil {
				rows = prepared.Rows()
			}
			if rows != 0 {
				return fmt.Errorf("%w: quantized asset %q zero-row prepared rows=%d", errColumnVectorGraphQuantizedAssetStale, name, rows)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%w: column_graph %q quantized index %q zero-row scalar_u8 code plane: %w", ErrVectorIndexSearchUnavailable, reader.def.Name, name, err)
		}
		return nil
	}
	ref := reader.sharedPreparedSearch
	if ref == nil || ref.holder == nil || !ref.key.valid() || ref.key != ref.holder.key || !ref.holder.ready() {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q requires a live shared prepared holder", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetClosed, reader.def.Name, name)
	}
	descriptor, ok := ref.holder.legacyScalarU8AssetDescriptor(name, q)
	if !ok || descriptor.definition.Name != name || descriptor.definition.Codec != QuantizedVectorCodecScalarU8 || descriptor.definition.Version != 1 || !scalarU8CalibrationIsLegacy(descriptor.definition) {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q does not match shared holder declaration", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, reader.def.Name, name)
	}
	wantAssetID := columnVectorGraphQuantizedCodesAssetID(q)
	if !descriptor.assets.HasCodes || descriptor.assets.Codes.Role != columnVectorIndexStateAssetRoleQuantizedCodes || descriptor.assets.Codes.AssetID != wantAssetID {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q has no matching scalar_u8 code-plane identity", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetMissing, reader.def.Name, name)
	}
	access := ref.holder.servingSourceAccess
	if ref.key.family == columnVectorGraphSharedPreparedSearchKeyServing && (access == nil || access.pool != ref.holder.servingSegments) {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q has no serving source capability", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetClosed, reader.def.Name, name)
	}
	status, err := ref.holder.acquireLegacyScalarU8AssetWithContext(ctx, descriptor, func() (columnVectorGraphQuantizedAssetLoadStatus, error) {
		return loadColumnVectorGraphQuantizedAssetResourceStatusWithSourceAccess(ctx, c.db.ColumnAssetRootDir(), reader.catalog.meta.Name, *reader.catalog.meta.Options.ColumnStore, reader.def, reader.graph, descriptor.definition, descriptor.assets, access)
	})
	if err != nil {
		return fmt.Errorf("%w: column_graph %q quantized index %q shared scalar_u8 code plane: %w", ErrVectorIndexSearchUnavailable, reader.def.Name, name, err)
	}
	if !columnVectorGraphSharedPreparedLegacyScalarU8AssetStatusReady(status, descriptor) {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q shared scalar_u8 code plane is unavailable", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetClosed, reader.def.Name, name)
	}
	if reader.quantizedAssetStatus == nil {
		reader.quantizedAssetStatus = make(map[string]columnVectorGraphQuantizedAssetLoadStatus, 1)
	}
	if existing, exists := reader.quantizedAssetStatus[name]; exists && existing.ownsResource && existing.resource != nil && existing.resource != status.resource {
		return fmt.Errorf("%w: %w: column_graph %q quantized index %q already has a reader-owned code plane", ErrVectorIndexSearchUnavailable, errColumnVectorGraphQuantizedAssetStale, reader.def.Name, name)
	}
	attached := status
	attached.ownsResource = false
	reader.quantizedAssetStatus[descriptor.definition.Name] = attached
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) releaseSharedPreparedSearch() error {
	if r != nil && r.sharedServingHolder != nil {
		capability := r.sharedServingHolder
		r.sharedServingHolder = nil
		r.detachSharedPreparedSearchAlias()
		return capability.Close()
	}
	return r.detachSharedPreparedSearch().release()
}

// Transfer only the immutable resource ref, never the reader's catalog/snapshot.
func (r *columnVectorGraphPhysicalRowReader) detachSharedPreparedSearch() *columnVectorGraphSharedPreparedSearchRef {
	if r != nil && r.sharedServingHolder != nil {
		return nil
	}
	return r.detachSharedPreparedSearchAlias()
}

// detachSharedPreparedSearchAlias clears non-owning source aliases. A serving
// caller must separately transfer or close sharedServingHolder; this helper is
// also used by the generic owning-ref path above.
func (r *columnVectorGraphPhysicalRowReader) detachSharedPreparedSearchAlias() *columnVectorGraphSharedPreparedSearchRef {
	if r == nil || r.sharedPreparedSearch == nil {
		return nil
	}
	ref := r.sharedPreparedSearch
	r.sharedPreparedSearch = nil
	r.typedVectorSource = nil
	r.invNormSource = nil
	r.rowRefSource = nil
	r.documentIDSource = nil
	r.hnswSearchPack = nil
	r.adjacencyLayerSources = nil
	r.layer0AdjacencySource = nil
	r.preparedSearch = nil
	return ref
}

func (c *Collection) columnVectorGraphSharedPreparedSearchCacheSnapshot() columnVectorGraphSharedPreparedSearchCacheSnapshot {
	if c == nil {
		return columnVectorGraphSharedPreparedSearchCacheSnapshot{}
	}
	c.vectorPreparedSearchMu.Lock()
	defer c.vectorPreparedSearchMu.Unlock()
	snap := columnVectorGraphSharedPreparedSearchCacheSnapshot{
		CacheHits:   c.vectorPreparedSearchHits,
		CacheMisses: c.vectorPreparedSearchMisses,
		CacheWaits:  c.vectorPreparedSearchWaits,
		CacheBuilds: c.vectorPreparedSearchBuilds,
	}
	for _, entry := range c.vectorPreparedSearch {
		if entry == nil || (entry.err != nil && entry.holder == nil && !entry.building) {
			continue
		}
		snap.Entries++
		snap.Refs += entry.refs
		if entry.building {
			snap.BuildingEntries++
		}
		if entry.holder != nil {
			snap.add(entry.holder.stats())
		}
	}
	return snap
}

func (s *columnVectorGraphSharedPreparedSearchCacheSnapshot) add(stats mappedresource.Stats) {
	if s == nil {
		return
	}
	s.ActiveHandles += stats.ActiveHandles
	s.ActiveMappedBytes += stats.ActiveMappedBytes
	s.ActiveHeapCopyBytes += stats.ActiveHeapCopyBytes
	s.ActiveDerivedMetadataBytes += stats.ActiveDerivedMetadataBytes
	s.TotalAcquires += stats.TotalAcquires
	s.TotalReleases += stats.TotalReleases
	s.Hits += stats.Hits
	s.Misses += stats.Misses
	s.FallbackReads += stats.FallbackReads
	s.Opens += stats.Opens
	s.Closes += stats.Closes
	s.Errors += stats.Errors
}

func (h *columnVectorGraphSharedPreparedSearch) stats() mappedresource.Stats {
	if h == nil {
		return mappedresource.Stats{}
	}
	var out mappedresource.Stats
	add := func(stats mappedresource.Stats) {
		out.ActiveHandles += stats.ActiveHandles
		out.ActiveMappedBytes += stats.ActiveMappedBytes
		out.ActiveHeapCopyBytes += stats.ActiveHeapCopyBytes
		out.ActiveDerivedMetadataBytes += stats.ActiveDerivedMetadataBytes
		out.TotalAcquires += stats.TotalAcquires
		out.TotalReleases += stats.TotalReleases
		out.TotalMappedBytes += stats.TotalMappedBytes
		out.TotalHeapCopyBytes += stats.TotalHeapCopyBytes
		out.TotalDerivedMetadataBytes += stats.TotalDerivedMetadataBytes
		out.Hits += stats.Hits
		out.Misses += stats.Misses
		out.FallbackReads += stats.FallbackReads
		out.Opens += stats.Opens
		out.Closes += stats.Closes
		out.Errors += stats.Errors
		out.DirectViewSuccesses += stats.DirectViewSuccesses
		out.DirectViewFailures += stats.DirectViewFailures
	}
	if h.typedVectorSource != nil && h.typedVectorSource.manager != nil {
		add(h.typedVectorSource.manager.Stats())
	}
	if h.invNormSource != nil && h.invNormSource.manager != nil {
		add(h.invNormSource.manager.Stats())
	}
	if h.rowRefSource != nil && h.rowRefSource.manager != nil {
		add(h.rowRefSource.manager.Stats())
	}
	if h.documentIDSource != nil && h.documentIDSource.manager != nil {
		add(h.documentIDSource.manager.Stats())
	}
	if h.hnswSearchPack != nil && h.hnswSearchPack.manager != nil {
		add(h.hnswSearchPack.manager.Stats())
	}
	if h.adjacencyLayerSources != nil {
		for _, source := range h.adjacencyLayerSources.sources {
			if source != nil && source.manager != nil {
				add(source.manager.Stats())
			}
		}
	}
	h.legacyScalarU8Mu.Lock()
	for _, entry := range h.legacyScalarU8Entries {
		if entry != nil && !entry.building && entry.status.resource != nil && entry.status.resource.manager != nil {
			add(entry.status.resource.manager.Stats())
		}
	}
	h.legacyScalarU8Mu.Unlock()
	return out
}

func columnVectorGraphSharedPreparedSearchCacheKey(collection string, namespace string, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (string, error) {
	if collection == "" || namespace == "" || def.Name == "" || graph.IndexName == "" || state.IndexName == "" {
		return "", errors.New("collections: column_graph shared prepared search key requires collection, namespace, graph, and state identity")
	}
	var b bytes.Buffer
	writeKeyString := func(label, value string) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte('|')
	}
	writeKeyInt := func(label string, value int) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(value))
		b.WriteByte('|')
	}
	writeKeyU64 := func(label string, value uint64) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.FormatUint(value, 10))
		b.WriteByte('|')
	}
	writeKeyI64 := func(label string, value int64) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.FormatInt(value, 10))
		b.WriteByte('|')
	}
	writeKeyU32 := func(label string, value uint32) {
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(strconv.FormatUint(uint64(value), 10))
		b.WriteByte('|')
	}
	writeRef := func(prefix string, ref ColumnAssetRef) {
		writeKeyString(prefix+".kind", string(ref.Kind))
		writeKeyString(prefix+".namespace", ref.Namespace)
		writeKeyU64(prefix+".generation", ref.Generation)
		writeKeyU64(prefix+".part_id", ref.PartID)
		writeKeyU32(prefix+".file_id", ref.FileID)
		writeKeyI64(prefix+".offset", ref.Offset)
		writeKeyI64(prefix+".length", ref.Length)
		writeKeyU32(prefix+".checksum", ref.Checksum)
	}

	writeKeyString("collection", collection)
	writeKeyString("namespace", namespace)
	writeKeyString("index", def.Name)
	writeKeyString("field", def.Field)
	writeKeyString("metric", def.Metric.String())
	writeKeyString("encoding", def.Encoding.String())
	writeKeyInt("dims", def.Dimensions)
	writeKeyInt("m", def.M)
	writeKeyInt("ef_construction", def.EfConstruction)
	writeKeyInt("ef_search", def.EfSearch)

	writeKeyString("graph.index", graph.IndexName)
	writeKeyString("graph.field", graph.Field)
	writeKeyString("graph.metric", graph.Metric.String())
	writeKeyString("graph.encoding", graph.Encoding.String())
	writeKeyInt("graph.dims", graph.Dimensions)
	writeKeyInt("graph.m", graph.M)
	writeKeyInt("graph.ef_construction", graph.EfConstruction)
	writeKeyInt("graph.ef_search", graph.EfSearch)
	writeKeyU64("graph.base_generation", graph.BaseManifestGeneration)
	writeKeyU64("graph.base_checksum", graph.BaseManifestChecksum)
	writeKeyU64("graph.base_schema", graph.BaseSchemaHash)
	writeKeyU64("graph.schema", graph.GraphSchemaHash)
	writeKeyInt("graph.rows", graph.RowCount)
	writeKeyInt("graph.layers", graph.AdjacencyLayerCount)
	writeRef("graph.asset", graph.AssetRef)

	writeKeyString("state.index", state.IndexName)
	writeKeyString("state.field", state.Field)
	writeKeyString("state.metric", state.Metric.String())
	writeKeyString("state.encoding", state.Encoding.String())
	writeKeyInt("state.dims", state.Dimensions)
	writeKeyInt("state.m", state.M)
	writeKeyInt("state.ef_construction", state.EfConstruction)
	writeKeyInt("state.ef_search", state.EfSearch)
	writeKeyInt("state.rows", state.RowCount)
	writeKeyU64("state.base_generation", state.BaseManifestGeneration)
	writeKeyU64("state.base_checksum", state.BaseManifestChecksum)
	writeKeyU64("state.base_schema", state.BaseSchemaHash)
	writeKeyInt("state.layers", state.AdjacencyLayerCount)
	assets := append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	sort.SliceStable(assets, func(i, j int) bool {
		if assets[i].Role != assets[j].Role {
			return assets[i].Role < assets[j].Role
		}
		if assets[i].AssetID != assets[j].AssetID {
			return assets[i].AssetID < assets[j].AssetID
		}
		if assets[i].LogicalType != assets[j].LogicalType {
			return assets[i].LogicalType < assets[j].LogicalType
		}
		return assets[i].PhysicalEncoding < assets[j].PhysicalEncoding
	})
	writeKeyInt("state.assets", len(assets))
	for i, asset := range assets {
		prefix := fmt.Sprintf("state.asset.%d", i)
		writeKeyString(prefix+".role", asset.Role)
		writeKeyString(prefix+".asset_id", asset.AssetID)
		writeKeyString(prefix+".logical", asset.LogicalType)
		writeKeyString(prefix+".physical", asset.PhysicalEncoding)
		writeKeyInt(prefix+".rows", asset.RowCount)
		writeKeyU64(prefix+".source_schema", asset.SourceSchemaHash)
		writeKeyI64(prefix+".bytes", asset.AssetBytes)
		writeRef(prefix+".ref", asset.Ref)
	}
	return b.String(), nil
}
