package collections

import (
	"bytes"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const warmIndexRangeMaxLimit = 256

type nativePhysicalRangeScanner interface {
	NativeScanLiveKeysAtRoot(root uint64, start, end []byte, visit func(key []byte) (bool, error)) error
}

type warmRangeCollector struct {
	lease             *memtable.COWExternalLease
	charge            uint64
	preAdmitted       uint64
	valueType         IndexValueType
	limit             int
	documents         bool
	dedupe            bool
	hasLookahead      bool
	seen              map[string]struct{}
	collectedIDs      [][]byte
	inlineIDs         [16][]byte
	lookaheadID       []byte
	lookaheadRawKey   []byte
	lastEmittedRawKey []byte
}

func (c *warmRangeCollector) admit(n uint64) error {
	if n <= c.preAdmitted {
		c.preAdmitted -= n
		return nil
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
			c.lookaheadID = bytes.Clone(id)
			c.lookaheadRawKey = bytes.Clone(key)
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
	c.collectedIDs = append(c.collectedIDs, bytes.Clone(id))
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
// The result wrappers own their copies; callbacks never borrow a physical page.
func (c *Collection) scanNativeIndexRange(name string, opts IndexRangeOptions, documents bool, visit func([]byte, []byte) (bool, error)) (truncated, indexFound bool, resultErr error) {
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
	err := c.nativeJSON.withOneShotRead(c.name, func(read *NativeJSONRead) error {
		var scanErr error
		truncated, indexFound, scanErr = read.scanIndexRange(name, opts, documents, visit)
		return scanErr
	})
	return truncated, indexFound, err
}

func (r *NativeJSONRead) scanIndexRange(name string, opts IndexRangeOptions, documents bool, visit func([]byte, []byte) (bool, error)) (truncated, indexFound bool, resultErr error) {
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
	truncated, err = r.scanIndexRangeAdmitted(index.ValueType, opts, documents, dedupe, hasDomain, domain, canWarmScan, scanner, lease, charge, preAdmitted, visit)
	return truncated, true, err
}

func (r *NativeJSONRead) scanIndexRangeAdmitted(
	valueType IndexValueType,
	opts IndexRangeOptions,
	documents bool,
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
		collector := &warmRangeCollector{
			lease:       lease,
			charge:      charge,
			preAdmitted: preAdmitted,
			valueType:   valueType,
			limit:       opts.Limit,
			documents:   documents,
			dedupe:      dedupe,
			seen:        seen,
		}
		if opts.Limit <= len(collector.inlineIDs) {
			collector.collectedIDs = collector.inlineIDs[:0]
		} else {
			collector.collectedIDs = make([][]byte, 0, opts.Limit)
		}
		defer collector.reset()

		if scanErr := scanner.NativeScanLiveKeysAtRoot(domain.baseRoot, start, end, collector.collect); scanErr != nil {
			return false, scanErr
		}

		emitted := 0
		missing := 0
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
