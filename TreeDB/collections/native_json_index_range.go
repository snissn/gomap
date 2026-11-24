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
	// Each bounded scalar can coexist with its cloned/prefix-successor bound.
	// Strings encode at most two bytes per source byte plus a fixed tag/tail;
	// numeric scalars fit the same fixed allowance. Three capacities cover the
	// encoder's growing backing and the independent bound, as on equality reads.
	// Admit before encoding.
	charge := memtable.COWAllocationCharge(page.PageSize+1) +
		memtable.COWAllocationCharge(uint64(len(name))+6) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainCursor{}))) +
		memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
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
	domain, hasDomain := r.collection.domains.Get("index/" + name)
	var preAdmitted uint64
	if hasDomain && domain != nil && domain.baseRoot != 0 {
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
	var seen map[string]struct{}
	operation := func() (truncated bool, operationErr error) {
		if dedupe {
			seen = make(map[string]struct{})
		}
		start, end, empty, err := indexRangeScanBounds(index.ValueType, opts)
		if err != nil || empty {
			return false, err
		}
		valueType := index.ValueType
		index = IndexDefinition{}
		if !hasDomain {
			return false, nil
		}
		read, err := newNativeDomainRead(domain, r.capture.Snapshot())
		domain = nil
		if err != nil {
			return false, err
		}
		physical := nativeDomainCursor{read: read, start: start, end: end}
		read = nativeDomainRead{}
		cursor := make([]byte, page.PageSize+1)
		curStart := start
		defer func() {
			operationErr = errors.Join(operationErr, physical.close())
			cursor, curStart, start, end = nil, nil, nil, nil
		}()
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
		readLeaf := r.cursorReadLeaf()
		emitted := 0
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
					if delta > ^uint64(0)-charge {
						entry = node.LeafEntry{}
						return false, memtable.ErrCOWCapacity
					}
					if err := lease.Resize(charge + delta); err != nil {
						entry = node.LeafEntry{}
						return false, err
					}
					charge += delta
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
	truncated, err = operation()
	operation = nil
	seen = nil
	lease.Close()
	return truncated, true, err
}
