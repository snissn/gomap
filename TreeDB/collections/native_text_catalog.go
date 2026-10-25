package collections

import (
	"bytes"
	"errors"
	"strings"
	"unsafe"

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

func (n *nativeTextCatalog) iterator(root string, start, end []byte, deleted bool, max int, onInspect func(int), inspectionError func() error) (iterator.UnsafeIterator, error) {
	if err := n.admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextIterator{}))) + memtable.COWAllocationCharge(uint64(len(start)+len(end)))); err != nil {
		return nil, err
	}
	it := &nativeTextIterator{scope: n, root: root, start: bytes.Clone(start), end: bytes.Clone(end), includeDeleted: deleted, max: max, onInspect: onInspect, inspectionError: inspectionError}
	it.cursor.start, it.cursor.end = it.start, it.end
	it.Seek(start)
	if it.err != nil {
		return nil, errors.Join(it.err, it.Close())
	}
	return it, nil
}

type nativeTextIterator struct {
	cursor                 nativeDomainCursor
	scope                  *nativeTextCatalog
	root                   string
	start, end, key, value []byte
	flags                  byte
	valid                  bool
	err                    error
	includeDeleted         bool
	max, inspected         int
	onInspect              func(int)
	inspectionError        func() error
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
	domain, err := r.borrowedNamedDomain(it.scope.relative(it.root))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return
	}
	if err != nil {
		it.err = err
		return
	}
	if it.cursor.read.domain == nil {
		it.cursor.read = domain
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		it.err = err
		return
	}
	for {
		entry, found, err := it.cursor.read.seekWithCursor(start, it.end, scratch, r.readLeaf, false, it.inspect, &it.cursor, forward, it.scope.admit)
		if err != nil {
			it.err = err
			return
		}
		if !found {
			return
		}
		if err = it.scope.admit(memtable.COWAllocationCharge(uint64(len(entry.Key) + 1))); err != nil {
			it.err = err
			return
		}
		if it.scope.admitKey != nil {
			if err = it.scope.admitKey(uint64(len(entry.Key))); err != nil {
				it.err = err
				return
			}
		}
		key := append([]byte(nil), entry.Key...)
		if entry.Flags&node.FlagTombstone != 0 && !it.includeDeleted {
			start = append(key, 0)
			forward = true
			continue
		}
		var value []byte
		if entry.Flags&node.FlagTombstone == 0 {
			value, err = it.scope.value(entry, it.root, entry.Key)
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
	if err := it.scope.admit(memtable.COWAllocationCharge(uint64(len(it.key) + 1))); err != nil {
		it.err = err
		it.valid = false
		return
	}
	it.seek(append(bytes.Clone(it.key), 0), true)
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
	err := it.cursor.close()
	it.valid = false
	it.scope = nil
	it.start, it.end, it.key, it.value = nil, nil, nil, nil
	return err
}
func (it *nativeTextIterator) Domain() ([]byte, []byte) { return it.start, it.end }

func newNativeTextCatalog(r *NativeJSONRead, admit func(uint64) error) (*collectionCatalog, error) {
	if err := admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(collectionCatalog{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextCatalog{})))); err != nil {
		return nil, err
	}
	return &collectionCatalog{meta: r.meta, primaryRootName: collectionPrimaryRootName(r.meta.Name), nativeText: &nativeTextCatalog{read: r, admit: admit}}, nil
}

var _ iterator.UnsafeIterator = (*nativeTextIterator)(nil)
