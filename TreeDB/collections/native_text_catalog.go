package collections

import (
	"bytes"
	"errors"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeTextCatalog borrows one complete cut and its operation's admission.
// It neither owns nor refreshes the read. The enclosing operation closes every
// iterator and drops decoded text state before releasing admission/the capture.
type nativeTextCatalog struct {
	read  *NativeJSONRead
	admit func(uint64) error
	// One admitted bound callback for general record copies on this scope.
	recordAdmit func(uint64) error
	// Optional rewrite-only key-derived map/string/slice cohorts.
	admitKey func(uint64) error
	// Set only after the maintained query analyzer selects one V2 term.
	// Reducers, inspection, rewrite and other serving shapes keep admitRecord.
	singleTermFields uint64
}

func (n *nativeTextCatalog) relative(root string) string {
	return strings.TrimPrefix(root, n.read.meta.Name+"/")
}

// A codec record can contain at most one decoded element per source byte. The
// largest text block entry and its field slices bound docmap/norm/posting/position
// decoding, growth and re-encoding. Source payload/output copies are included;
// this allowance remains live until the complete reducer/search scope ends.
func (n *nativeTextCatalog) admitRecord(size uint64) error {
	unit := uint64(unsafe.Sizeof(textV2DocMapEntry{})) + uint64(unsafe.Sizeof(textV2NormBlockEntry{})) + uint64(unsafe.Sizeof(textV2PostingBlockEntry{})) + uint64(unsafe.Sizeof(textV2PositionFieldValue{})) + 4*uint64(unsafe.Sizeof([]byte{})) + 16
	if size > (^uint64(0)-unit)/(2*unit) {
		return memtable.ErrCOWCapacity
	}
	return n.admit(memtable.COWAllocationCharge((size + 1) * 2 * unit))
}
func (n *nativeTextCatalog) entry(root string, key []byte) (entry node.LeafEntry, resultErr error) {
	r := n.read
	r.mu.Lock()
	defer r.mu.Unlock()
	domain, err := r.borrowedNamedDomain(n.relative(root))
	if err != nil {
		return entry, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return entry, err
	}
	entry, err = domain.getEntry(key, scratch, r.readLeaf)
	if err != nil {
		return node.LeafEntry{}, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return entry, nil
	}
	value, err := n.value(entry, root, key)
	if err != nil {
		return node.LeafEntry{}, err
	}
	return node.LeafEntry{Key: key, Value: value, Flags: node.FlagInline, Revision: entry.Revision}, nil
}
func (n *nativeTextCatalog) get(root string, key, dst []byte) ([]byte, bool, error) {
	entry, err := n.entry(root, key)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], false, nil
	}
	if err != nil {
		return dst[:0], false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return dst[:0], false, nil
	}
	if dst == nil {
		return entry.Value, true, nil
	}
	if err = n.admit(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
		return dst[:0], false, err
	}
	return append(dst[:0], entry.Value...), true, nil
}

// withRecord confines a borrowed inline record to one synchronous private codec
// call under the original read exclusion. consume must neither retain raw aliases
// nor reenter a read, move a cursor, or invoke external callbacks. Pointer records
// retain the unchanged owned value path. A true found also distinguishes a codec
// error from a lookup/admission error for the original lookup counters.
func (n *nativeTextCatalog) withRecord(root string, key []byte, consume func([]byte) error) (bool, error) {
	r := n.read
	r.mu.Lock()
	defer r.mu.Unlock()
	domain, err := r.borrowedNamedDomain(n.relative(root))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return false, err
	}
	entry, err := domain.getEntry(key, scratch, r.readLeaf)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return false, nil
	}
	value, err := n.iteratorValue(entry, root, key)
	if err != nil {
		return false, err
	}
	return true, consume(value)
}

// normBlockSelected is the only synchronous borrowed-record consumer. The
// maintained decoder validates the complete record and retains only its own
// descriptor/field arrays, including when selection excludes every entry.
func (n *nativeTextCatalog) normBlockSelected(root string, key []byte, allowed *textV2SearchOrdinalAllowSet) (decoded textV2SearchNormBlock, found bool, err error) {
	found, err = n.withRecord(root, key, func(raw []byte) error {
		var decodeErr error
		decoded, decodeErr = decodeTextV2SearchNormBlockSelected(raw, allowed)
		return decodeErr
	})
	return decoded, found, err
}

// iteratorValue borrows inline bytes from this iterator's own projection or the
// immutable captured memory sources. Synchronous consumption holds the read
// mutex throughout; iterator consumption holds it during movement and retains
// the existing read visit until all cursor/value aliases are detached.
// Source and decoded-storage promises remain identical to value, including the
// original rounded input to recordAdmit; omitting a copy grants no credit.
func (n *nativeTextCatalog) iteratorValue(entry node.LeafEntry, root string, key []byte) ([]byte, error) {
	if entry.Flags&node.FlagPointer != 0 {
		return n.value(entry, root, key)
	}
	if n.singleTermFields == 0 {
		if err := n.bindRecordAdmission(); err != nil {
			return nil, err
		}
		if err := n.recordAdmit(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
			return nil, err
		}
		return entry.Value, nil
	}
	if err := n.admit(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
		return nil, err
	}
	if err := n.admitServingRecord(root, key, uint64(len(entry.Value))); err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (n *nativeTextCatalog) iterator(root string, start, end []byte, deleted bool, max int, onInspect func(int), inspectionError func() error) (iterator.UnsafeIterator, error) {
	if err := n.admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextIterator{}))) + memtable.COWAllocationCharge(uint64(len(start)+len(end))) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))); err != nil {
		return nil, err
	}
	it := &nativeTextIterator{scope: n, root: root, start: bytes.Clone(start), end: bytes.Clone(end), includeDeleted: deleted, max: max, onInspect: onInspect, inspectionError: inspectionError}
	// General mutation/DDL/rewrite callers retain owned values across movement.
	// Freeze borrowing only for the existing selected serving scope, together
	// with the original read visit that protects that iterator's raw aliases.
	if n.singleTermFields != 0 {
		r := n.read
		r.mu.Lock()
		if r.capture == nil || r.failure != nil || r.closing || r.visits == ^uint32(0) {
			r.mu.Unlock()
			return nil, backenddb.ErrClosed
		}
		r.visits++
		it.heldVisit = true
		r.mu.Unlock()
	}
	// The physical cursor retains this bound method. Construct it once on the
	// admitted wrapper, rather than allocating another escaping closure per row.
	it.readLeaf = n.read.readLeaf
	it.cursor.start, it.cursor.end = it.start, it.end
	it.Seek(start)
	if it.err != nil {
		return nil, errors.Join(it.err, it.Close())
	}
	return it, nil
}

type nativeTextIterator struct {
	cursor                 nativeDomainCursor
	readLeaf               func(page.LeafLogPtr, []byte) ([]byte, error)
	scope                  *nativeTextCatalog
	root                   string
	start, end, key, value []byte
	// Owned key/successor workspace; exposed key is invalidated on movement.
	keyWorkspace    []byte
	flags           byte
	valid           bool
	err             error
	includeDeleted  bool
	heldVisit       bool
	max, inspected  int
	onInspect       func(int)
	inspectionError func() error
}

func (it *nativeTextIterator) inspect() error {
	if it.inspectionError != nil {
		if err := it.inspectionError(); err != nil {
			return err
		}
	}
	if it.max > 0 && it.inspected == it.max {
		return errCollectionIndexScanWorkCap
	}
	it.inspected++
	if it.onInspect != nil {
		it.onInspect(it.inspected)
	}
	return nil
}
func (it *nativeTextIterator) Seek(start []byte) {
	it.seek(start, false)
}
func (it *nativeTextIterator) seek(start []byte, forward bool) {
	it.valid = false
	it.key, it.value = nil, nil
	if it.scope == nil || it.err != nil {
		return
	}
	r := it.scope.read
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capture == nil || r.closing || r.failure != nil {
		it.err = backenddb.ErrClosed
		return
	}
	if it.cursor.read.domain == nil {
		// The cursor borrows one immutable domain for the entire scope. Do not
		// reconstruct and discard its physical root reader on every movement.
		domain, err := r.borrowedNamedDomain(it.scope.relative(it.root))
		if errors.Is(err, tree.ErrKeyNotFound) {
			return
		}
		if err != nil {
			it.err = err
			return
		}
		it.cursor.read = domain
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		it.err = err
		return
	}
	for {
		entry, found, err := it.cursor.read.seekWithCursor(start, it.end, scratch, it.readLeaf, false, it.inspect, &it.cursor, forward, it.scope.admit)
		if err != nil {
			it.err = err
			return
		}
		if !found {
			return
		}
		if len(entry.Key) == int(^uint(0)>>1) {
			it.err = memtable.ErrCOWCapacity
			return
		}
		if cap(it.keyWorkspace) < len(entry.Key)+1 {
			// Keep every growth cohort charged through the original operation.
			// The old backing can still be reachable until GC after movement.
			if err = it.scope.admit(memtable.COWAllocationCharge(uint64(len(entry.Key)) + 1)); err != nil {
				it.err = err
				return
			}
			it.keyWorkspace = make([]byte, len(entry.Key), len(entry.Key)+1)
		}
		if it.scope.admitKey != nil {
			if err = it.scope.admitKey(uint64(len(entry.Key))); err != nil {
				it.err = err
				return
			}
		}
		// Merge has consumed start, which may alias this workspace. Only now
		// overwrite it with the winner, before any source movement reuses keys.
		it.keyWorkspace = it.keyWorkspace[:len(entry.Key)]
		key := it.keyWorkspace
		copy(key, entry.Key)
		if entry.Flags&node.FlagTombstone != 0 && !it.includeDeleted {
			start = append(key, 0)
			forward = true
			continue
		}
		var value []byte
		if entry.Flags&node.FlagTombstone == 0 {
			if it.heldVisit {
				value, err = it.scope.iteratorValue(entry, it.root, entry.Key)
			} else {
				value, err = it.scope.value(entry, it.root, entry.Key)
			}
			if err != nil {
				it.err = err
				return
			}
		}
		it.key, it.value, it.flags, it.valid = key, value, entry.Flags, true
		if it.flags&node.FlagTombstone == 0 {
			it.flags = node.FlagInline
		}
		return
	}
}
func (it *nativeTextIterator) Next() {
	if !it.valid {
		return
	}
	it.seek(append(it.key, 0), true)
}
func (it *nativeTextIterator) Valid() bool         { return it.valid && it.err == nil }
func (it *nativeTextIterator) UnsafeKey() []byte   { return it.key }
func (it *nativeTextIterator) UnsafeValue() []byte { return it.value }
func (it *nativeTextIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.value, page.ValuePtr{}, it.flags
}
func (it *nativeTextIterator) Key() []byte                 { return it.key }
func (it *nativeTextIterator) Value() []byte               { return it.value }
func (it *nativeTextIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], it.key...) }
func (it *nativeTextIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], it.value...) }
func (it *nativeTextIterator) IsDeleted() bool             { return it.flags&node.FlagTombstone != 0 }
func (it *nativeTextIterator) Error() error                { return it.err }
func (it *nativeTextIterator) Close() error {
	if it.scope == nil {
		return nil
	}
	r := it.scope.read
	heldVisit := it.heldVisit
	if heldVisit {
		r.mu.Lock()
	}
	err := it.cursor.close()
	it.valid = false
	it.scope = nil
	it.readLeaf = nil
	it.start, it.end, it.key, it.value, it.keyWorkspace = nil, nil, nil, nil, nil
	if !heldVisit {
		return err
	}
	it.heldVisit = false
	r.visits--
	closeRequested := r.closing && r.visits == 0
	r.mu.Unlock()
	// The original Close acquires r.mu and releases the original capture and
	// receipts. All borrowed aliases must be gone before its final disposal.
	if closeRequested {
		err = errors.Join(err, r.Close())
	}
	return err
}
func (it *nativeTextIterator) Domain() ([]byte, []byte) { return it.start, it.end }

func newNativeTextCatalog(r *NativeJSONRead, admit func(uint64) error) (*collectionCatalog, error) {
	if err := admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(collectionCatalog{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextCatalog{}))) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))); err != nil {
		return nil, err
	}
	scope := &nativeTextCatalog{read: r, admit: admit}
	scope.recordAdmit = scope.admitRecord
	return &collectionCatalog{meta: r.meta, primaryRootName: collectionPrimaryRootName(r.meta.Name), nativeText: scope}, nil
}

var _ iterator.UnsafeIterator = (*nativeTextIterator)(nil)
