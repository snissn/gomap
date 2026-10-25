package collections

import (
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/tidwall/btree"
)

// Layout mirrors are guarded against the actual dependency in the focused
// representation test. No mutable Map.Copy is used. tidwall/btree v1.8.1 has
// degree 32, at most 63 items and at least 31 items in non-root nodes.
type nativeTextBTreeEntry struct {
	value    []byte
	ptr      page.ValuePtr
	revision page.EntryRevision
	flags    byte
}
type nativeTextBTreePair struct {
	value nativeTextBTreeEntry
	key   string
}
type nativeTextBTreeNode struct {
	isoid    uint64
	count    int
	items    []nativeTextBTreePair
	children *[]*nativeTextBTreeNode
}
type nativeTextBTreeArena struct {
	chunkSize int
	chunks    [][]byte
	offset    int
}
type nativeTextBTreeIterator struct {
	iter          btree.MapIter[string, nativeTextBTreeEntry]
	end           string
	hasEnd, valid bool
	cur           nativeTextBTreeEntry
	hasCur        bool
	mu            *sync.RWMutex
}

func nativeTextTable(_ int) memtable.Table { return memtable.NewBTree() }

// Operations bound distinct temporary keys: DeleteSteal is a tombstone Set,
// so no nodes merge/disappear. The node sum across declared roots is bounded
// by roots+operations/31. Every split adds a node and can restart a left growth cohort; charge
// two cohorts per node. Below 256 elements Go slice growth at least doubles;
// a cohort's cumulative rounded backing is bounded by twice its largest rounded
// allocation. Bound item capacity by 126 and children by 128, including the
// extra root preallocation overwritten in Map.Set. This includes obsolete arrays
// until the whole private builder scope ends, rather than final live sizes only.
func nativeTextTableCharge(operations, roots uint64) (uint64, error) {
	charge := memtable.COWAllocationCharge
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	// Reset allocates a fresh empty map, before the entire table is dropped.
	base := charge(uint64(unsafe.Sizeof(memtable.BTree{}))) + 2*charge(uint64(unsafe.Sizeof(btree.Map[string, nativeTextBTreeEntry]{}))) + charge(uint64(unsafe.Sizeof(nativeTextBTreeArena{}))) + charge(uint64(unsafe.Sizeof(nativeTextBTreeIterator{})))
	if roots != 3 && roots != 7 {
		return 0, memtable.ErrCOWCapacity
	}
	nodes := roots + operations/31
	perNode := charge(uint64(unsafe.Sizeof(nativeTextBTreeNode{}))) + charge(uint64(unsafe.Sizeof([]*nativeTextBTreeNode{}))) + 4*charge(126*uint64(unsafe.Sizeof(nativeTextBTreePair{}))) + 4*charge(128*ptr)
	// Every new root may allocate its discarded initial children backing.
	perNode += charge(64 * ptr)
	// An iterator stack is bounded by height <= nodes; use the same geometric
	// growth bound for its (node pointer,index) entries in each root iterator.
	// Check each product and rounded allocation independently: rounding can
	// itself saturate, and an unrepresentable request must refuse admission.
	max := ^uint64(0)
	if nodes > max/perNode || nodes > max/(8*ptr) {
		return 0, memtable.ErrCOWCapacity
	}
	stack := charge(8 * nodes * ptr)
	if stack > max/roots || nodes*perNode > max-roots*base || roots*stack > max-roots*base-nodes*perNode {
		return 0, memtable.ErrCOWCapacity
	}
	return roots*base + nodes*perNode + roots*stack, nil
}
