package collections

import (
	"bytes"
	"errors"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	warmIndexRangeMaxLimit     = 256
	warmRangeScratchMaxSlabCap = 64 << 10
)

var errWarmRangeBorrowFallback = errors.New("collections: warm range borrow fallback")

type nativePhysicalRangeScanner interface {
	NativeScanLiveKeysAtRoot(root uint64, start, end []byte, visit func(key []byte) (bool, error)) error
}

type nativePhysicalBatchEntryReader interface {
	NativeEntriesAtRoot(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error
}

type nativePhysicalBatchEntryIntoReader interface {
	NativeEntriesAtRootInto(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, slab []byte) ([]byte, error)
}

type nativeCutAllocator interface {
	NativeAllocator() backenddb.NativeSemanticAllocator
}

type warmRangeDocBatch struct {
	entries [17]node.LeafEntry
	found   [17]bool
	keys    [17][]byte
}

type warmRangeCollector struct {
	lease              *memtable.COWExternalLease
	charge             uint64
	preAdmitted        uint64
	valueType          IndexValueType
	limit              int
	documents          bool
	dedupe             bool
	borrow             bool
	hasLookahead       bool
	seen               map[string]struct{}
	collectedIDs       [][]byte
	inlineIDs          [16][]byte
	idBuf              []byte
	lookaheadID        []byte
	lookaheadRawKey    []byte
	lookaheadRawKeyBuf []byte
	lastEmittedRawKey  []byte
}

type warmRangeScratch struct {
	collector          warmRangeCollector
	batch              warmRangeDocBatch
	idArena            [512]byte
	lastRawKeyBuf      [256]byte
	lookaheadRawKeyBuf [256]byte
	valSlab            []byte
	ptrValBuf          []byte
	collectFn          func([]byte) (bool, error)
}

var warmRangeScratchPool = sync.Pool{
	New: func() any {
		sc := new(warmRangeScratch)
		sc.collectFn = sc.collector.collect
		return sc
	},
}

func (sc *warmRangeScratch) reset() {
	sc.collector.reset()
	clear(sc.batch.entries[:])
	clear(sc.batch.keys[:])
	if cap(sc.valSlab) > warmRangeScratchMaxSlabCap {
		sc.valSlab = nil
	} else {
		sc.valSlab = sc.valSlab[:0]
	}
	if cap(sc.ptrValBuf) > warmRangeScratchMaxSlabCap {
		sc.ptrValBuf = nil
	} else {
		sc.ptrValBuf = sc.ptrValBuf[:0]
	}
}

func (c *warmRangeCollector) admit(n uint64) error {
	if n <= c.preAdmitted {
		c.preAdmitted -= n
		return nil
	}
	if c.lease == nil {
		return errWarmRangeBorrowFallback
	}
	extra := n - c.preAdmitted
	c.preAdmitted = 0
	if extra > ^uint64(0)-c.charge {
		return memtable.ErrCOWCapacity
	}
	if err := c.lease.Resize(c.charge + extra); err != nil {
		return err
	}
	c.charge += extra
	return nil
}

func (c *warmRangeCollector) copyInternalID(id []byte) []byte {
	if len(id) <= cap(c.idBuf)-len(c.idBuf) {
		off := len(c.idBuf)
		c.idBuf = append(c.idBuf, id...)
		return c.idBuf[off : off+len(id) : off+len(id)]
	}
	return bytes.Clone(id)
}

func (c *warmRangeCollector) collect(key []byte) (bool, error) {
	if len(key) >= page.PageSize+1 {
		return false, memtable.ErrCOWCapacity
	}
	id, err := indexKeyDocumentID(c.valueType, key)
	if err != nil {
		return false, err
	}
	if c.dedupe {
		if _, exists := c.seen[string(id)]; exists {
			return true, nil
		}
	}
	if len(c.collectedIDs) >= c.limit {
		c.hasLookahead = true
		if c.documents {
			idCharge := memtable.COWAllocationCharge(uint64(len(id)))
			keyCharge := memtable.COWAllocationCharge(uint64(len(key)))
			if keyCharge > ^uint64(0)-idCharge {
				return false, memtable.ErrCOWCapacity
			}
			if err := c.admit(idCharge + keyCharge); err != nil {
				return false, err
			}
			c.lookaheadID = c.copyInternalID(id)
			if len(key) <= cap(c.lookaheadRawKeyBuf) {
				c.lookaheadRawKey = append(c.lookaheadRawKeyBuf[:0], key...)
			} else {
				c.lookaheadRawKey = bytes.Clone(key)
			}
		}
		return false, nil
	}
	idCharge := memtable.COWAllocationCharge(uint64(len(id)))
	if c.dedupe {
		dedupeCharge := memtable.COWAllocationCharge(768) + idCharge
		if dedupeCharge > ^uint64(0)-idCharge {
			return false, memtable.ErrCOWCapacity
		}
		if err := c.admit(dedupeCharge + idCharge); err != nil {
			return false, err
		}
		c.seen[string(id)] = struct{}{}
	} else {
		if err := c.admit(idCharge); err != nil {
			return false, err
		}
	}
	if c.borrow {
		c.collectedIDs = append(c.collectedIDs, c.copyInternalID(id))
	} else {
		c.collectedIDs = append(c.collectedIDs, bytes.Clone(id))
	}
	if c.documents {
		if len(key) > cap(c.lastEmittedRawKey) {
			newCap := max(256, len(key))
			if err := c.admit(memtable.COWAllocationCharge(uint64(newCap))); err != nil {
				return false, err
			}
			c.lastEmittedRawKey = make([]byte, len(key), newCap)
			copy(c.lastEmittedRawKey, key)
		} else {
			c.lastEmittedRawKey = append(c.lastEmittedRawKey[:0], key...)
		}
	}
	return true, nil
}

func (c *warmRangeCollector) reset() {
	*c = warmRangeCollector{}
}

// One original capture supplies both secondary winners and primary documents.
// When borrow is true, ID and document slices passed to visit may alias pooled
// buffers valid only for the duration of the callback; callers requiring owned
// slices pass borrow=false.
func (c *Collection) scanNativeIndexRange(name string, opts IndexRangeOptions, documents, borrow bool, visit func([]byte, []byte) (bool, error)) (truncated, indexFound bool, resultErr error) {
	if err := ValidateIndexName(name); err != nil {
		return false, false, err
	}
	if opts.Limit < 0 {
		return false, false, errors.New("collections: index range limit cannot be negative")
	}
	if documents && opts.Limit == 0 {
		return false, false, errors.New("collections: document index range reads require a positive limit")
	}
	if opts.Desc {
		return false, false, errors.New("collections: descending index range scans are not supported")
	}
	if trunc, found, handled, err := c.scanNativeIndexRangeBorrowedCut(name, opts, documents, borrow, visit); handled {
		return trunc, found, err
	}
	err := c.nativeJSON.withOneShotRead(c.name, func(read *NativeJSONRead) error {
		var scanErr error
		truncated, indexFound, scanErr = read.scanIndexRangeWithBorrow(name, opts, documents, borrow, visit)
		return scanErr
	})
	return truncated, indexFound, err
}

func warmIndexRangeInitialCharge(name string, index IndexDefinition, opts IndexRangeOptions, documents, dedupe, primaryBatched bool) (charge, preAdmitted uint64, err error) {
	charge = memtable.COWAllocationCharge(uint64(len(name))+6) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(warmRangeCollector{}))) +
		memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
	if opts.Limit > len(warmRangeCollector{}.inlineIDs) {
		charge += memtable.COWAllocationCharge(uint64(opts.Limit) * uint64(unsafe.Sizeof([]byte{})))
	}
	if index.ValueType == IndexValueBSONOrderedV2 {
		charge += memtable.COWAllocationCharge(page.PageSize)
	}
	if dedupe {
		charge += memtable.COWAllocationCharge(256)
	}
	for _, bound := range [2]IndexRangeBound{opts.Lower, opts.Upper} {
		if bound.Unbounded {
			continue
		}
		amount, boundErr := nativeIndexQueryCharge(index, bound.Value)
		if boundErr != nil {
			return 0, 0, boundErr
		}
		if amount > ^uint64(0)-charge {
			return 0, 0, memtable.ErrCOWCapacity
		}
		charge += amount
	}
	idSlots := uint64(opts.Limit)
	if documents {
		idSlots++
	}
	collectorAllowance := idSlots * memtable.COWAllocationCharge(64)
	if documents {
		collectorAllowance += 2 * memtable.COWAllocationCharge(256)
	}
	if dedupe {
		collectorAllowance += uint64(opts.Limit) * (memtable.COWAllocationCharge(768) + memtable.COWAllocationCharge(64))
	}
	if primaryBatched {
		batchAllowance := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(warmRangeDocBatch{})))
		if batchAllowance > ^uint64(0)-collectorAllowance {
			return 0, 0, memtable.ErrCOWCapacity
		}
		collectorAllowance += batchAllowance
	}
	if collectorAllowance > ^uint64(0)-charge {
		return 0, 0, memtable.ErrCOWCapacity
	}
	charge += collectorAllowance
	return charge, collectorAllowance, nil
}

// scanNativeIndexRangeBorrowedCut executes a bounded warm settled index range
// scan directly against a borrowed live cut without allocating a
// NativeSemanticCapture or NativeJSONRead handle. Any non-settled domain,
// typed schema, oversized batch, orphan/missing primary doc, or budget resize
// falls back cleanly to withOneShotRead before visit is ever invoked.
func (c *Collection) scanNativeIndexRangeBorrowedCut(
	name string,
	opts IndexRangeOptions,
	documents, borrow bool,
	visit func([]byte, []byte) (bool, error),
) (truncated, indexFound, handled bool, resultErr error) {
	if c == nil || c.nativeJSON == nil || c.nativeJSON.db == nil {
		return false, false, false, nil
	}
	if err := ValidateCollectionName(c.name); err != nil {
		return false, false, true, err
	}
	if opts.Limit <= 0 || opts.Limit > warmIndexRangeMaxLimit {
		return false, false, false, nil
	}
	if documents && opts.Limit+1 > len(warmRangeDocBatch{}.entries) {
		return false, false, false, nil
	}
	cut, err := c.nativeJSON.db.BorrowNativeSemanticCut()
	if err != nil {
		if errors.Is(err, backenddb.ErrClosed) {
			return false, false, true, err
		}
		return false, false, false, nil
	}
	releaseCut := func() error {
		if cut == nil {
			return nil
		}
		rel := cut
		cut = nil
		return rel.Release()
	}
	defer func() {
		if cut != nil {
			relErr := releaseCut()
			if handled {
				resultErr = errors.Join(resultErr, relErr)
			}
		}
	}()

	engine, ok := cut.State().(*nativeEngineVersion)
	if !ok || engine == nil {
		return false, false, false, nil
	}
	col, ok := engine.names.Get(c.name)
	if !ok {
		return false, false, true, tree.ErrKeyNotFound
	}
	meta := col.schema.meta
	index, found := findIndex(meta.Indexes, name)
	if !found {
		return false, false, true, nil
	}
	if orderedBSONIndexRequiresCompoundRangeAPI(index) {
		return false, false, true, errors.New("collections: compound or descending BSON v2 indexes require FindByCompoundIndexRange")
	}
	if documents && nativeTypedMeta(*meta) {
		return false, false, false, nil
	}
	idxDomain, hasIdxDomain := col.domains.Get("index/" + name)
	if !hasIdxDomain || idxDomain == nil || idxDomain.refs.Load() <= 0 ||
		idxDomain.baseRoot == 0 || idxDomain.pending != nil || idxDomain.captured != nil {
		return false, false, false, nil
	}
	var primDomain *nativeDomainState
	if documents {
		var hasPrim bool
		primDomain, hasPrim = col.domains.Get("primary")
		if !hasPrim || primDomain == nil || primDomain.refs.Load() <= 0 ||
			primDomain.baseRoot == 0 || primDomain.pending != nil || primDomain.captured != nil {
			return false, false, false, nil
		}
	}
	scanner, hasScanner := cut.(nativePhysicalRangeScanner)
	if !hasScanner {
		return false, false, false, nil
	}
	var batchInto nativePhysicalBatchEntryIntoReader
	if documents {
		var hasBatchInto bool
		batchInto, hasBatchInto = cut.(nativePhysicalBatchEntryIntoReader)
		if !hasBatchInto {
			return false, false, false, nil
		}
	}
	cutAlloc, hasAlloc := cut.(nativeCutAllocator)
	if !hasAlloc {
		return false, false, false, nil
	}
	valAlloc, hasValAlloc := cutAlloc.NativeAllocator().(nativeExternalValueAllocator)
	if !hasValAlloc {
		return false, false, false, nil
	}

	dedupe := shouldDedupeIndexDocumentIDs(index, meta.Options)
	charge, preAdmitted, err := warmIndexRangeInitialCharge(name, index, opts, documents, dedupe, documents)
	if err != nil {
		return false, true, true, err
	}
	leaseVal, err := valAlloc.AcquireExternalValue(charge)
	if err != nil {
		return false, false, false, nil
	}
	defer leaseVal.Close()

	start, end, empty, err := indexRangeScanBounds(index.ValueType, opts)
	if err != nil || empty {
		return false, true, true, err
	}

	sc := warmRangeScratchPool.Get().(*warmRangeScratch)
	defer func() {
		sc.reset()
		warmRangeScratchPool.Put(sc)
	}()

	var seen map[string]struct{}
	if dedupe {
		seen = make(map[string]struct{})
	}
	collector := &sc.collector
	collector.lease = nil
	collector.charge = charge
	collector.preAdmitted = preAdmitted
	collector.valueType = index.ValueType
	collector.limit = opts.Limit
	collector.documents = documents
	collector.dedupe = dedupe
	collector.borrow = borrow
	collector.seen = seen
	collector.idBuf = sc.idArena[:0]
	collector.lookaheadRawKeyBuf = sc.lookaheadRawKeyBuf[:0]
	collector.lastEmittedRawKey = sc.lastRawKeyBuf[:0]
	if opts.Limit <= len(collector.inlineIDs) {
		collector.collectedIDs = collector.inlineIDs[:0]
	} else {
		collector.collectedIDs = make([][]byte, 0, opts.Limit)
	}

	if scanErr := scanner.NativeScanLiveKeysAtRoot(idxDomain.baseRoot, start, end, sc.collectFn); scanErr != nil {
		if errors.Is(scanErr, errWarmRangeBorrowFallback) {
			return false, false, false, nil
		}
		return false, true, true, scanErr
	}

	numDocs := len(collector.collectedIDs)
	if !documents {
		for i := 0; i < numDocs; i++ {
			id := collector.collectedIDs[i]
			collector.collectedIDs[i] = nil
			cont, visitErr := visit(id, nil)
			if visitErr != nil {
				return false, true, true, visitErr
			}
			if !cont {
				return false, true, true, nil
			}
		}
		return collector.hasLookahead, true, true, nil
	}

	if numDocs == 0 {
		return false, true, true, nil
	}
	totalKeys := numDocs
	if collector.hasLookahead {
		totalKeys++
	}
	if err := collector.admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(warmRangeDocBatch{})))); err != nil {
		if errors.Is(err, errWarmRangeBorrowFallback) {
			return false, false, false, nil
		}
		return false, true, true, err
	}
	entries := sc.batch.entries[:totalKeys]
	foundFlags := sc.batch.found[:totalKeys]
	batchKeys := sc.batch.keys[:totalKeys]
	copy(batchKeys, collector.collectedIDs)
	lookIdx := numDocs
	if collector.hasLookahead {
		batchKeys[lookIdx] = collector.lookaheadID
	}

	var batchErr error
	if borrow {
		sc.valSlab, batchErr = batchInto.NativeEntriesAtRootInto(primDomain.baseRoot, batchKeys, entries, foundFlags, sc.valSlab)
	} else {
		_, batchErr = batchInto.NativeEntriesAtRootInto(primDomain.baseRoot, batchKeys, entries, foundFlags, nil)
	}
	if batchErr != nil {
		return false, true, true, batchErr
	}

	// Verify no missing/tombstoned primary docs (including lookahead) BEFORE
	// calling visit so any rare orphan case falls back cleanly to withOneShotRead.
	for i := 0; i < totalKeys; i++ {
		if !foundFlags[i] || entries[i].Flags&node.FlagTombstone != 0 {
			return false, false, false, nil
		}
	}

	for i := 0; i < numDocs; i++ {
		id := collector.collectedIDs[i]
		collector.collectedIDs[i] = nil
		batchKeys[i] = nil
		var document []byte
		if entries[i].Flags&node.FlagPointer != 0 {
			ptr := entries[i].ValuePtr
			entries[i] = node.LeafEntry{}
			var valErr error
			if borrow {
				sc.ptrValBuf, valErr = cut.NativeValueAtPointerInto(ptr, sc.ptrValBuf[:0], nil)
				document = sc.ptrValBuf
			} else {
				document, valErr = cut.NativeValueAtPointer(ptr, nil)
			}
			if valErr != nil {
				return false, true, true, valErr
			}
		} else {
			document = entries[i].Value
			entries[i] = node.LeafEntry{}
		}
		cont, visitErr := visit(id, document)
		document = nil
		if visitErr != nil {
			return false, true, true, visitErr
		}
		if !cont {
			return false, true, true, nil
		}
	}
	if !collector.hasLookahead {
		return false, true, true, nil
	}
	if entries[lookIdx].Flags&node.FlagPointer != 0 {
		lookPtr := entries[lookIdx].ValuePtr
		entries[lookIdx] = node.LeafEntry{}
		batchKeys[lookIdx] = nil
		var lookErr error
		sc.ptrValBuf, lookErr = cut.NativeValueAtPointerInto(lookPtr, sc.ptrValBuf[:0], nil)
		if lookErr != nil {
			return false, true, true, lookErr
		}
	}
	return true, true, true, nil
}

func (r *NativeJSONRead) scanIndexRange(name string, opts IndexRangeOptions, documents bool, visit func([]byte, []byte) (bool, error)) (truncated, indexFound bool, resultErr error) {
	return r.scanIndexRangeWithBorrow(name, opts, documents, false, visit)
}

func (r *NativeJSONRead) scanIndexRangeWithBorrow(name string, opts IndexRangeOptions, documents, borrow bool, visit func([]byte, []byte) (bool, error)) (truncated, indexFound bool, resultErr error) {
	r.mu.Lock()
	if r.capture == nil || r.failure != nil || r.closing || r.visits == ^uint32(0) {
		r.mu.Unlock()
		return false, false, backenddb.ErrClosed
	}
	r.visits++
	defer func() {
		r.visits--
		closeRequested := r.closing && r.visits == 0
		r.mu.Unlock()
		if closeRequested {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	index, found := findIndex(r.meta.Indexes, name)
	if !found {
		return false, false, nil
	}
	if orderedBSONIndexRequiresCompoundRangeAPI(index) {
		return false, false, errors.New("collections: compound or descending BSON v2 indexes require FindByCompoundIndexRange")
	}
	if r.collection == nil {
		return false, true, backenddb.ErrNativeSemanticUnsupported
	}
	domain, hasDomain := r.collection.domains.Get("index/" + name)
	scanner, hasScanner := r.capture.(nativePhysicalRangeScanner)
	canWarmScan := hasScanner &&
		opts.Limit > 0 && opts.Limit <= warmIndexRangeMaxLimit &&
		hasDomain && domain != nil && domain.refs.Load() > 0 &&
		domain.baseRoot != 0 && domain.pending == nil && domain.captured == nil

	// Each bounded scalar can coexist with its cloned/prefix-successor bound.
	// Strings encode at most two bytes per source byte plus a fixed tag/tail;
	// numeric scalars fit the same fixed allowance. Three capacities cover the
	// encoder's growing backing and the independent bound, as on equality reads.
	// Admit before encoding.
	var charge uint64
	if canWarmScan {
		charge = memtable.COWAllocationCharge(uint64(len(name))+6) +
			memtable.COWAllocationCharge(uint64(unsafe.Sizeof(warmRangeCollector{}))) +
			memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
		if opts.Limit > len(warmRangeCollector{}.inlineIDs) {
			charge += memtable.COWAllocationCharge(uint64(opts.Limit) * uint64(unsafe.Sizeof([]byte{})))
		}
	} else {
		charge = memtable.COWAllocationCharge(page.PageSize+1) +
			memtable.COWAllocationCharge(uint64(len(name))+6) +
			memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainCursor{}))) +
			memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
	}
	if index.ValueType == IndexValueBSONOrderedV2 {
		// Escaped NUL IDs use the maintained decoder's temporary owned
		// suffix, independently of the seek cursor and the caller's copy.
		charge += memtable.COWAllocationCharge(page.PageSize)
	}
	dedupe := shouldDedupeIndexDocumentIDs(index, r.meta.Options)
	if dedupe {
		// Original map policy: control backing first, then a conservative
		// bucket/growth allowance plus owned string per distinct emitted ID.
		// Each growth is charged before allocation; the original finite
		// operation budget also bounds an unbounded-result public scan.
		charge += memtable.COWAllocationCharge(256)
	}
	for _, bound := range [2]IndexRangeBound{opts.Lower, opts.Upper} {
		if bound.Unbounded {
			continue
		}
		amount, err := nativeIndexQueryCharge(index, bound.Value)
		if err != nil {
			return false, true, err
		}
		if amount > ^uint64(0)-charge {
			return false, true, memtable.ErrCOWCapacity
		}
		charge += amount
	}
	var preAdmitted uint64
	if canWarmScan {
		// Pre-admit a bounded collector allowance so common-case Limit <= 256
		// scans make zero lease.Resize calls inside NativeScanLiveKeysAtRoot,
		// while oversized IDs/keys still charge strictly before allocation.
		idSlots := uint64(opts.Limit)
		if documents {
			idSlots++
		}
		collectorAllowance := idSlots * memtable.COWAllocationCharge(64)
		if documents {
			collectorAllowance += 2 * memtable.COWAllocationCharge(256)
		}
		if dedupe {
			collectorAllowance += uint64(opts.Limit) * (memtable.COWAllocationCharge(768) + memtable.COWAllocationCharge(64))
		}
		if documents &&
			!nativeTypedMeta(r.meta) &&
			r.domain.hasBase && r.domain.domain != nil && r.domain.domain.refs.Load() > 0 &&
			r.domain.domain.baseRoot != 0 && r.domain.domain.pending == nil && r.domain.domain.captured == nil {
			if _, hasBatch := r.capture.(nativePhysicalBatchEntryReader); hasBatch {
				batchAllowance := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(warmRangeDocBatch{})))
				if batchAllowance > ^uint64(0)-collectorAllowance {
					return false, true, memtable.ErrCOWCapacity
				}
				collectorAllowance += batchAllowance
			}
		}
		if collectorAllowance > ^uint64(0)-charge {
			return false, true, memtable.ErrCOWCapacity
		}
		charge += collectorAllowance
		preAdmitted = collectorAllowance
	} else if hasDomain && domain != nil && domain.baseRoot != 0 {
		projCharge := memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())
		if projCharge > ^uint64(0)-charge {
			return false, true, memtable.ErrCOWCapacity
		}
		charge += projCharge
		preAdmitted = projCharge
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return false, true, err
	}
	defer func() {
		domain = nil
		lease.Close()
	}()
	truncated, err = r.scanIndexRangeAdmitted(index.ValueType, opts, documents, borrow, dedupe, hasDomain, domain, canWarmScan, scanner, lease, charge, preAdmitted, visit)
	return truncated, true, err
}

func (r *NativeJSONRead) scanIndexRangeAdmitted(
	valueType IndexValueType,
	opts IndexRangeOptions,
	documents bool,
	borrow bool,
	dedupe bool,
	hasDomain bool,
	domain *nativeDomainState,
	canWarmScan bool,
	scanner nativePhysicalRangeScanner,
	lease *memtable.COWExternalLease,
	charge uint64,
	preAdmitted uint64,
	visit func([]byte, []byte) (bool, error),
) (truncated bool, operationErr error) {
	var seen map[string]struct{}
	if dedupe {
		seen = make(map[string]struct{})
	}
	start, end, empty, err := indexRangeScanBounds(valueType, opts)
	if err != nil || empty || !hasDomain {
		seen = nil
		domain = nil
		start, end = nil, nil
		return false, err
	}
	defer func() {
		seen = nil
		domain = nil
		start, end = nil, nil
	}()

	if canWarmScan {
		sc := warmRangeScratchPool.Get().(*warmRangeScratch)
		defer func() {
			sc.reset()
			warmRangeScratchPool.Put(sc)
		}()
		collector := &sc.collector
		collector.lease = lease
		collector.charge = charge
		collector.preAdmitted = preAdmitted
		collector.valueType = valueType
		collector.limit = opts.Limit
		collector.documents = documents
		collector.dedupe = dedupe
		collector.borrow = borrow
		collector.seen = seen
		collector.idBuf = sc.idArena[:0]
		collector.lookaheadRawKeyBuf = sc.lookaheadRawKeyBuf[:0]
		collector.lastEmittedRawKey = sc.lastRawKeyBuf[:0]
		if opts.Limit <= len(collector.inlineIDs) {
			collector.collectedIDs = collector.inlineIDs[:0]
		} else {
			collector.collectedIDs = make([][]byte, 0, opts.Limit)
		}

		if scanErr := scanner.NativeScanLiveKeysAtRoot(domain.baseRoot, start, end, sc.collectFn); scanErr != nil {
			return false, scanErr
		}

		emitted := 0
		missing := 0
		primarySettled := documents &&
			!nativeTypedMeta(r.meta) &&
			r.domain.hasBase &&
			r.domain.domain != nil &&
			r.domain.domain.refs.Load() > 0 &&
			r.domain.domain.baseRoot != 0 &&
			r.domain.domain.pending == nil &&
			r.domain.domain.captured == nil
		if batchReader, ok := r.capture.(nativePhysicalBatchEntryReader); ok && primarySettled && len(collector.collectedIDs) > 0 {
			if r.capture == nil || r.failure != nil || r.closing {
				return false, backenddb.ErrClosed
			}
			numDocs := len(collector.collectedIDs)
			totalKeys := numDocs
			if collector.hasLookahead {
				totalKeys++
			}
			var (
				entries   []node.LeafEntry
				found     []bool
				batchKeys [][]byte
			)
			if totalKeys <= len(sc.batch.entries) {
				if err := collector.admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(warmRangeDocBatch{})))); err != nil {
					return false, err
				}
				entries = sc.batch.entries[:totalKeys]
				found = sc.batch.found[:totalKeys]
				batchKeys = sc.batch.keys[:totalKeys]
			} else {
				eCharge := memtable.COWAllocationCharge(uint64(totalKeys) * uint64(unsafe.Sizeof(node.LeafEntry{})))
				fCharge := memtable.COWAllocationCharge(uint64(totalKeys) * uint64(unsafe.Sizeof(bool(false))))
				kCharge := memtable.COWAllocationCharge(uint64(totalKeys) * uint64(unsafe.Sizeof([]byte{})))
				if fCharge > ^uint64(0)-eCharge || kCharge > ^uint64(0)-eCharge-fCharge {
					return false, memtable.ErrCOWCapacity
				}
				if err := collector.admit(eCharge + fCharge + kCharge); err != nil {
					return false, err
				}
				entries = make([]node.LeafEntry, totalKeys)
				found = make([]bool, totalKeys)
				batchKeys = make([][]byte, totalKeys)
			}
			defer func() {
				clear(entries)
				clear(batchKeys)
				entries, found, batchKeys = nil, nil, nil
			}()

			copy(batchKeys, collector.collectedIDs)
			lookIdx := numDocs
			if collector.hasLookahead {
				batchKeys[lookIdx] = collector.lookaheadID
			}
			if batchInto, ok := r.capture.(nativePhysicalBatchEntryIntoReader); ok && borrow {
				var batchErr error
				sc.valSlab, batchErr = batchInto.NativeEntriesAtRootInto(r.domain.domain.baseRoot, batchKeys, entries, found, sc.valSlab)
				if batchErr != nil {
					return false, batchErr
				}
			} else {
				if batchErr := batchReader.NativeEntriesAtRoot(r.domain.domain.baseRoot, batchKeys, entries, found, true); batchErr != nil {
					return false, batchErr
				}
			}

			valInto, hasValInto := r.capture.(nativePhysicalValueIntoReader)
			for i := 0; i < numDocs; i++ {
				id := collector.collectedIDs[i]
				collector.collectedIDs[i] = nil
				batchKeys[i] = nil
				if r.capture == nil || r.failure != nil || r.closing {
					return false, backenddb.ErrClosed
				}
				if !found[i] || entries[i].Flags&node.FlagTombstone != 0 {
					entries[i] = node.LeafEntry{}
					missing++
					continue
				}
				var document []byte
				if entries[i].Flags&node.FlagPointer != 0 {
					ptr := entries[i].ValuePtr
					entries[i] = node.LeafEntry{}
					var valErr error
					if borrow && hasValInto {
						sc.ptrValBuf, valErr = valInto.NativeValueAtPointerInto(ptr, sc.ptrValBuf[:0], nil)
						document = sc.ptrValBuf
					} else {
						document, valErr = r.readValue(ptr, nil)
					}
					if valErr != nil {
						return false, valErr
					}
				} else {
					document = entries[i].Value
					entries[i] = node.LeafEntry{}
				}
				cont, visitErr := func() (bool, error) {
					r.mu.Unlock()
					defer r.mu.Lock()
					return visit(id, document)
				}()
				document = nil
				if visitErr != nil {
					return false, visitErr
				}
				if !cont {
					return false, nil
				}
				emitted++
				if r.capture == nil || r.failure != nil || r.closing {
					return false, backenddb.ErrClosed
				}
			}
			collector.collectedIDs = nil

			if !collector.hasLookahead {
				return false, nil
			}
			var fallbackRawKey []byte
			if missing == 0 {
				lookFound := found[lookIdx]
				lookFlags := entries[lookIdx].Flags
				lookPtr := entries[lookIdx].ValuePtr
				entries[lookIdx] = node.LeafEntry{}
				batchKeys[lookIdx] = nil
				if lookFound && lookFlags&node.FlagTombstone == 0 {
					if lookFlags&node.FlagPointer != 0 {
						if borrow && hasValInto {
							var lookErr error
							sc.ptrValBuf, lookErr = valInto.NativeValueAtPointerInto(lookPtr, sc.ptrValBuf[:0], nil)
							if lookErr != nil {
								return false, lookErr
							}
						} else if _, lookErr := r.readValue(lookPtr, nil); lookErr != nil {
							return false, lookErr
						}
					}
					return true, nil
				}
				// Orphan (Limit+1)-th lookahead key: resume cursor after lookaheadRawKey
				// to verify whether any subsequent live primary document exists.
				fallbackRawKey = collector.lookaheadRawKey
			} else {
				// Missing primary doc(s) within collectedIDs: resume cursor after
				// lastEmittedRawKey so lookahead and subsequent keys are scanned cleanly.
				fallbackRawKey = collector.lastEmittedRawKey
			}
			clear(entries)
			clear(batchKeys)
			entries, found, batchKeys = nil, nil, nil
			fallbackCharge := memtable.COWAllocationCharge(page.PageSize+1) +
				memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainCursor{})))
			if err := collector.admit(fallbackCharge); err != nil {
				return false, err
			}
			return r.scanIndexRangeCursor(valueType, opts, documents, dedupe, domain, start, end, fallbackRawKey, emitted, seen, collector.admit, visit)
		}

		for i, id := range collector.collectedIDs {
			collector.collectedIDs[i] = nil
			if r.capture == nil || r.failure != nil || r.closing {
				return false, backenddb.ErrClosed
			}
			var document []byte
			if documents {
				var getErr error
				document, getErr = r.getAdmittedLocked(id, nil)
				if errors.Is(getErr, tree.ErrKeyNotFound) {
					missing++
					continue
				}
				if getErr != nil {
					return false, getErr
				}
			}
			cont, visitErr := func() (bool, error) {
				r.mu.Unlock()
				defer r.mu.Lock()
				return visit(id, document)
			}()
			document = nil
			if visitErr != nil {
				return false, visitErr
			}
			if !cont {
				return false, nil
			}
			emitted++
			if r.capture == nil || r.failure != nil || r.closing {
				return false, backenddb.ErrClosed
			}
		}
		collector.collectedIDs = nil

		if !documents {
			return collector.hasLookahead, nil
		}
		if !collector.hasLookahead {
			return false, nil
		}
		var fallbackRawKey []byte
		if missing == 0 {
			_, lookErr := r.getAdmittedLocked(collector.lookaheadID, nil)
			if lookErr == nil {
				return true, nil
			}
			if !errors.Is(lookErr, tree.ErrKeyNotFound) {
				return false, lookErr
			}
			// Orphan (Limit+1)-th lookahead key: resume cursor after lookaheadRawKey
			// to verify whether any subsequent live primary document exists.
			fallbackRawKey = collector.lookaheadRawKey
		} else {
			// Missing primary doc(s) within collectedIDs: resume cursor after
			// lastEmittedRawKey so lookahead and subsequent keys are scanned cleanly.
			fallbackRawKey = collector.lastEmittedRawKey
		}
		fallbackCharge := memtable.COWAllocationCharge(page.PageSize+1) +
			memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainCursor{})))
		if err := collector.admit(fallbackCharge); err != nil {
			return false, err
		}
		return r.scanIndexRangeCursor(valueType, opts, documents, dedupe, domain, start, end, fallbackRawKey, emitted, seen, collector.admit, visit)
	}

	admit := func(n uint64) error {
		if n <= preAdmitted {
			preAdmitted -= n
			return nil
		}
		extra := n - preAdmitted
		preAdmitted = 0
		if extra > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charge + extra); err != nil {
			return err
		}
		charge += extra
		return nil
	}
	return r.scanIndexRangeCursor(valueType, opts, documents, dedupe, domain, start, end, nil, 0, seen, admit, visit)
}

func (r *NativeJSONRead) scanIndexRangeCursor(
	valueType IndexValueType,
	opts IndexRangeOptions,
	documents bool,
	dedupe bool,
	domain *nativeDomainState,
	start, end []byte,
	fallbackRawKey []byte,
	emitted int,
	seen map[string]struct{},
	admit func(uint64) error,
	visit func([]byte, []byte) (bool, error),
) (truncated bool, operationErr error) {
	read, err := newNativeDomainRead(domain, r.capture.Snapshot())
	domain = nil
	if err != nil {
		return false, err
	}
	physical := nativeDomainCursor{read: read, start: start, end: end}
	read = nativeDomainRead{}
	cursor := make([]byte, page.PageSize+1)
	curStart := start
	if fallbackRawKey != nil {
		n := copy(cursor, fallbackRawKey)
		cursor[n] = 0
		curStart = cursor[:n+1]
		fallbackRawKey = nil
	}
	defer func() {
		operationErr = errors.Join(operationErr, physical.close())
		cursor, curStart, start, end = nil, nil, nil, nil
	}()
	readLeaf := r.cursorReadLeaf()
	forward := false
	for {
		if r.capture == nil || r.failure != nil || r.closing {
			return false, backenddb.ErrClosed
		}
		entry, found, err := physical.read.seekWithCursor(curStart, end, nil, readLeaf, false, nil, &physical, forward, admit)
		if err != nil || !found {
			return false, err
		}
		if len(entry.Key) >= len(cursor) {
			entry = node.LeafEntry{}
			return false, memtable.ErrCOWCapacity
		}
		n := copy(cursor, entry.Key)
		cursor[n] = 0
		curStart = cursor[:n+1]
		forward = true
		if entry.Flags&node.FlagTombstone != 0 {
			entry = node.LeafEntry{}
			continue
		}
		id, err := indexKeyDocumentID(valueType, entry.Key)
		if err != nil {
			entry = node.LeafEntry{}
			return false, err
		}
		if dedupe {
			if _, exists := seen[string(id)]; exists {
				entry = node.LeafEntry{}
				continue
			}
			// Lookahead certifies truncation but need not retain a new key.
			if opts.Limit == 0 || emitted < opts.Limit {
				delta := memtable.COWAllocationCharge(768) + memtable.COWAllocationCharge(uint64(len(id)))
				if err := admit(delta); err != nil {
					entry = node.LeafEntry{}
					return false, err
				}
				seen[string(id)] = struct{}{}
			}
		}
		// End every borrowed secondary alias before primary lookup or user
		// code can reuse physical scratch. The visitor pins this SAME cut.
		id = bytes.Clone(id)
		entry = node.LeafEntry{}
		var document []byte
		if documents {
			document, err = r.getAdmittedLocked(id, nil)
			if errors.Is(err, tree.ErrKeyNotFound) {
				id = nil
				continue
			}
			if err != nil {
				id = nil
				return false, err
			}
		}
		if opts.Limit > 0 && emitted >= opts.Limit {
			id, document = nil, nil
			return true, nil
		}
		cont, err := func() (bool, error) {
			r.mu.Unlock()
			defer r.mu.Lock()
			return visit(id, document)
		}()
		id, document = nil, nil
		if err != nil {
			return false, err
		}
		if !cont {
			return false, nil
		}
		emitted++
	}
}
