package collections

import (
	"bytes"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// findNativeCompoundIndexRange borrows ONE captured schema/domain/snapshot.
// It never opens a legacy iterator or materializes/reverses the index range.
func (c *Collection) findNativeCompoundIndexRange(name string, opts CompoundIndexRangeOptions) (ids [][]byte, truncated bool, resultErr error) {
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, r.Close()) }()
	r.mu.Lock()
	if r.capture == nil || r.failure != nil || r.closing || r.visits == ^uint32(0) {
		r.mu.Unlock()
		return nil, false, backenddb.ErrClosed
	}
	r.visits++
	defer func() { r.visits--; r.mu.Unlock() }()
	index, found := findIndex(r.meta.Indexes, name)
	if !found {
		return nil, false, nil
	}
	components := max(1, len(index.Components))
	if opts.StableDocumentIDTies && components-len(opts.Prefix) > 1 {
		return nil, false, errors.New("collections: stable document-ID ties require at most one unfixed compound component")
	}
	if r.collection == nil {
		return nil, false, backenddb.ErrNativeSemanticUnsupported
	}
	// Account the domain name before constructing it; metadata itself remains
	// borrowed from the exact captured schema under this read scope.
	charge := memtable.COWAllocationCharge(page.PageSize+1) + memtable.COWAllocationCharge(page.PageSize) + memtable.COWAllocationCharge(uint64(len(name))+6)
	dedupe := shouldDedupeIndexDocumentIDs(index, r.meta.Options)
	if dedupe {
		// Same original map control and per-entry growth policy as scalar reads.
		charge += memtable.COWAllocationCharge(256)
	}
	scalar := index
	scalar.Components = nil
	addBound := func(value any) error {
		n, err := nativeIndexQueryCharge(scalar, value)
		if err != nil {
			return err
		}
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		charge += n
		return nil
	}
	for _, value := range opts.Prefix {
		if err = addBound(value); err != nil {
			return nil, false, err
		}
	}
	for _, bound := range [2]IndexRangeBound{opts.Lower, opts.Upper} {
		if !bound.Unbounded && bound.Value != nil {
			if err = addBound(bound.Value); err != nil {
				return nil, false, err
			}
		}
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, false, err
	}
	defer lease.Close()
	admit := func(n uint64) error {
		n = memtable.COWAllocationCharge(n)
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charge + n); err != nil {
			return err
		}
		charge += n
		return nil
	}
	domainName := "index/" + name
	domain, ok := r.collection.domains.Get(domainName)
	if !ok {
		return nil, false, nil
	}
	read, err := newNativeDomainRead(domain, r.capture.Snapshot())
	if err != nil {
		return nil, false, err
	}
	start, end, empty, err := compoundIndexRangeScanBounds(index, opts)
	if err != nil || empty {
		return nil, false, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return nil, false, err
	}
	cursor := make([]byte, page.PageSize+1)
	workCap := compoundIndexRangeInspectedCap(opts.Limit)
	if opts.MaxInspected > 0 {
		workCap = opts.MaxInspected
	}
	inspected := 0
	defer func() {
		if opts.Inspected != nil {
			*opts.Inspected = inspected
		}
	}()
	inspect := func() error {
		if inspected >= workCap {
			return errCollectionIndexScanWorkCap
		}
		inspected++
		return nil
	}
	next := func() ([]byte, bool, bool, error) {
		for {
			if r.closing || r.failure != nil {
				return nil, false, false, backenddb.ErrClosed
			}
			entry, found, err := read.seek(start, end, scratch, r.readLeaf, opts.Desc, inspect)
			if errors.Is(err, errCollectionIndexScanWorkCap) {
				return nil, false, true, nil
			}
			if err != nil || !found {
				return nil, false, false, err
			}
			if len(entry.Key) >= len(cursor) {
				return nil, false, false, memtable.ErrCOWCapacity
			}
			n := copy(cursor, entry.Key)
			if opts.Desc {
				end = cursor[:n]
			} else {
				cursor[n] = 0
				start = cursor[:n+1]
			}
			if entry.Flags&node.FlagTombstone != 0 {
				continue
			}
			// Own the source key before another seek can reuse physical scratch.
			// Also admit the maintained logical-prefix copy and escaped-ID decoder.
			if dedupe && opts.StableDocumentIDTies {
				// The maintained stable scanner owns its seen map. Admit its conservative
				// bucket/growth and string allowance before it sees this source key.
				// The key bounds the decoded ID length, including escaped IDs.
				if err = admit(768 + uint64(len(entry.Key))); err != nil {
					return nil, false, false, err
				}
			}
			if err = admit(3 * uint64(len(entry.Key))); err != nil {
				return nil, false, false, err
			}
			return bytes.Clone(entry.Key), true, false, nil
		}
	}
	retained := 0
	emit := func(id []byte) (bool, error) {
		n := len(id) + stableDocumentIDTieEntryOverhead
		if n < len(id) || (opts.MaxRetainedIDBytes > 0 && n > opts.MaxRetainedIDBytes-retained) {
			return false, errCollectionIndexScanRetainedBytesCap
		}
		if len(ids) == cap(ids) {
			capacity := max(1, 2*cap(ids))
			if capacity < 0 || uint64(capacity) > ^uint64(0)/uint64(unsafe.Sizeof([]byte{})) {
				return false, memtable.ErrCOWCapacity
			}
			if err := admit(uint64(capacity) * uint64(unsafe.Sizeof([]byte{}))); err != nil {
				return false, err
			}
			grown := make([][]byte, len(ids), capacity)
			copy(grown, ids)
			ids = grown
		}
		ids = append(ids, id)
		retained += n
		return true, nil
	}
	if opts.StableDocumentIDTies {
		less := opts.DocumentIDLess
		if less != nil {
			original := less
			less = func(left, right []byte) bool {
				r.mu.Unlock()
				defer r.mu.Lock()
				return original(left, right)
			}
		}
		truncated, err = scanCollectionIndexIDsStable(next, index.ValueType, opts.Limit, scanMergedCollectionIndexIDOptions{StableDocumentIDTies: true, DedupeDocumentID: dedupe, DocumentIDLess: less, LogicalIndexKey: BSONIndexKeyStableSortPrefixV2, MaxStableTieBytes: opts.MaxRetainedIDBytes, admitTemporary: admit}, emit)
	} else {
		var seen map[string]struct{}
		if dedupe {
			seen = make(map[string]struct{})
		}
		for {
			key, ok, capped, nextErr := next()
			if nextErr != nil {
				err = nextErr
				break
			}
			if capped {
				truncated = true
				break
			}
			if !ok {
				break
			}
			if !dedupe && len(ids) >= opts.Limit {
				truncated = true
				break
			}
			id, decodeErr := indexKeyDocumentID(index.ValueType, key)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			if dedupe {
				if _, duplicate := seen[string(id)]; duplicate {
					continue
				}
			}
			// Only a distinct lookahead ID can certify truncation. It needs no map
			// entry and must not consume retained-ID capacity.
			if dedupe && len(ids) >= opts.Limit {
				truncated = true
				break
			}
			if dedupe {
				if err = admit(768 + uint64(len(id))); err != nil {
					break
				}
				seen[string(id)] = struct{}{}
			}
			if err = admit(uint64(len(id))); err != nil {
				break
			}
			if _, err = emit(bytes.Clone(id)); err != nil {
				break
			}
		}
	}
	// All stream/group/parser aliases are local and dead before the original
	// temporary receipt ends. Only independent caller-owned result copies escape.
	read = nativeDomainRead{}
	cursor, start, end = nil, nil, nil
	if errors.Is(err, errCollectionIndexScanRetainedBytesCap) {
		return ids, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	return ids, truncated, nil
}
