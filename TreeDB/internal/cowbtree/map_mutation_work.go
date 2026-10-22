package cowbtree

import (
	"reflect"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// AllocationCharge reports only node and backing allocations made through this
// header since creation or its private fork. It includes discarded growth
// buffers, excludes the Map header and opaque value copies, and grants no
// allocation authority. Callers must reserve a proven bound before mutation.
// New headers returned by Copy/IsoCopy/ForkImmutable begin at zero; their opening
// node graph is already owned by the original source. Published headers are
// immutable, so readers never update or need this preparation-only scalar.
func (tr *Map[K, V]) AllocationCharge() uint64 { return tr.allocationCharge }

func (tr *Map[K, V]) recordAllocation(bytes uint64) {
	charge := retainedalloc.AllocationCharge(bytes)
	if charge > ^uint64(0)-tr.allocationCharge {
		tr.allocationCharge = ^uint64(0)
	} else {
		tr.allocationCharge += charge
	}
}

// These append sites use the runtime's growth policy. Charge the concrete new
// capacity after allocation, while the caller still owns the private postimage.
func (tr *Map[K, V]) recordItemsGrowth(oldCapacity int, items []mapPair[K, V]) {
	if cap(items) > oldCapacity {
		tr.recordAllocation(uint64(cap(items)) * uint64(unsafe.Sizeof(mapPair[K, V]{})))
	}
}

func (tr *Map[K, V]) recordChildrenGrowth(oldCapacity int, children []*mapNode[K, V]) {
	if cap(children) > oldCapacity {
		tr.recordAllocation(uint64(cap(children)) * uint64(unsafe.Sizeof((*mapNode[K, V])(nil))))
	}
}

// Mutation admission uses the caller's Work directly. It has no local budget,
// retry journal, speculative logical write, or alternate tree representation.
func mutationReserve(w Work, records, bytes uint64) bool {
	return w == nil || w.Reserve(records, bytes)
}

func (tr *Map[K, V]) admitHeader(w Work) bool {
	return mutationReserve(w, 2, uint64(unsafe.Sizeof(*tr))+uint64(unsafe.Sizeof(tr.count)))
}
func (tr *Map[K, V]) admitLoad(w Work) bool {
	return mutationReserve(w, 1, uint64(unsafe.Sizeof(mapNode[K, V]{}))+2*uint64(unsafe.Sizeof(uintptr(0))))
}
func (tr *Map[K, V]) admitProbe(w Work) bool {
	return mutationReserve(w, 1, uint64(unsafe.Sizeof(mapPair[K, V]{})))
}
func (tr *Map[K, V]) admitNewNode(leaf bool, w Work) bool {
	if w == nil {
		return true
	}
	if leaf {
		return w.Reserve(1, uint64(unsafe.Sizeof(mapNode[K, V]{})))
	}
	return w.Reserve(1, uint64(unsafe.Sizeof(mapInternalNode[K, V]{})))
}
func (tr *Map[K, V]) admitCopy(n *mapNode[K, V], w Work) bool {
	if w == nil {
		return true
	}
	// These callbacks have no cost declaration or checked continuation. Charging
	// their invocation alone cannot bound their effects or internal allocations.
	if tr.copyValues || tr.isoCopyValues {
		return false
	}
	pair := uint64(unsafe.Sizeof(mapPair[K, V]{}))
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	if !tr.admitNewNode(n.leaf(), w) ||
		!w.Reserve(uint64(mapCopyCapacity(len(n.items), cap(n.items))), uint64(mapCopyCapacity(len(n.items), cap(n.items)))*pair) ||
		!w.Reserve(uint64(len(n.items)), 2*uint64(len(n.items))*pair) {
		return false
	}
	if n.leaf() {
		return true
	}
	capacity := uint64(mapCopyCapacity(len(*n.children), tr.max+1))
	return w.Reserve(capacity, capacity*ptr) &&
		w.Reserve(uint64(len(*n.children)), 2*uint64(len(*n.children))*ptr)
}

func mapCopyCapacity(length, maximum int) int {
	if length < maximum {
		return length + 1
	}
	return maximum
}

// Every zeroed capacity slot and copied source/destination slot is admitted
// before make/copy. Explicit growth keeps concrete capacity knowable before
// allocation; ordinary Set drives the same growth without accounting scans.
func (tr *Map[K, V]) growItems(items []mapPair[K, V], extra int, w Work) ([]mapPair[K, V], bool) {
	if cap(items)-len(items) >= extra {
		return items, true
	}
	capacity := cap(items) * 2
	if capacity < len(items)+extra {
		capacity = len(items) + extra
	}
	if capacity > tr.max {
		capacity = tr.max
	}
	size := uint64(unsafe.Sizeof(mapPair[K, V]{}))
	if !mutationReserve(w, uint64(capacity), uint64(capacity)*size) ||
		!mutationReserve(w, uint64(len(items)), 2*uint64(len(items))*size) {
		return nil, false
	}
	grown := make([]mapPair[K, V], len(items), capacity)
	tr.recordAllocation(uint64(cap(grown)) * size)
	copy(grown, items)
	return grown, true
}
func (tr *Map[K, V]) growChildren(children []*mapNode[K, V], extra int, w Work) ([]*mapNode[K, V], bool) {
	if cap(children)-len(children) >= extra {
		return children, true
	}
	capacity := cap(children) * 2
	if capacity < len(children)+extra {
		capacity = len(children) + extra
	}
	if capacity > tr.max+1 {
		capacity = tr.max + 1
	}
	size := uint64(unsafe.Sizeof(uintptr(0)))
	if !mutationReserve(w, uint64(capacity), uint64(capacity)*size) ||
		!mutationReserve(w, uint64(len(children)), 2*uint64(len(children))*size) {
		return nil, false
	}
	grown := make([]*mapNode[K, V], len(children), capacity)
	tr.recordAllocation(uint64(cap(grown)) * size)
	copy(grown, children)
	return grown, true
}
func (tr *Map[K, V]) admitInitial(w Work) bool {
	return mutationReserve(w, 4, uint64(unsafe.Sizeof(mapPair[K, V]{}))+uint64(unsafe.Sizeof(mapNode[K, V]{}))+uint64(unsafe.Sizeof(*tr)))
}
func (tr *Map[K, V]) admitReplacement(w Work) bool {
	return mutationReserve(w, 1, uint64(unsafe.Sizeof(mapPair[K, V]{}))+uint64(unsafe.Sizeof(tr.empty.value)))
}
func (tr *Map[K, V]) admitInsert(shift int, w Work) bool {
	size := uint64(unsafe.Sizeof(mapPair[K, V]{}))
	return mutationReserve(w, uint64(shift), 2*uint64(shift)*size) &&
		mutationReserve(w, 3, 2*size+uint64(unsafe.Sizeof(int(0))))
}
func (tr *Map[K, V]) admitDescent(w Work) bool {
	return mutationReserve(w, 2, 2*uint64(unsafe.Sizeof(uintptr(0)))+uint64(unsafe.Sizeof(int(0))))
}
func (tr *Map[K, V]) admitSplit(n *mapNode[K, V], w Work) bool {
	if w == nil {
		return true
	}
	if !tr.admitNewNode(n.leaf(), w) ||
		!w.Reserve(4, uint64(unsafe.Sizeof(mapNode[K, V]{}))*2+2*uint64(unsafe.Sizeof(mapPair[K, V]{}))) {
		return false
	}
	if n.leaf() {
		return true
	}
	// updateCount visits all children of the left and right nodes once.
	return w.Reserve(uint64(len(*n.children)), uint64(len(*n.children))*(uint64(unsafe.Sizeof(uintptr(0)))+uint64(unsafe.Sizeof(int(0)))))
}
func (tr *Map[K, V]) admitParentSplit(itemShift, childShift int, w Work) bool {
	pair := uint64(unsafe.Sizeof(mapPair[K, V]{}))
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	return mutationReserve(w, uint64(itemShift), 2*uint64(itemShift)*pair) &&
		mutationReserve(w, uint64(childShift), 2*uint64(childShift)*ptr) &&
		mutationReserve(w, 4, 2*pair+2*ptr)
}
func (tr *Map[K, V]) admitRootSplit(w Work) bool {
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	return mutationReserve(w, 6, uint64(unsafe.Sizeof(mapPair[K, V]{}))+5*ptr+3*uint64(unsafe.Sizeof(int(0))))
}

func mutationLess[K ordered](a, b K, w Work) (bool, bool) {
	if w == nil {
		return a < b, true
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(a))) {
		return false, false
	}
	av := reflect.ValueOf(a)
	if av.Kind() != reflect.String {
		return a < b, true
	}
	// reflect preserves aliases of string without copying their contents.
	as, bs := av.String(), reflect.ValueOf(b).String()
	for i := 0; i < len(as) && i < len(bs); i++ {
		if !w.Reserve(0, 2) {
			return false, false
		}
		x, y := as[i], bs[i]
		if x != y {
			return x < y, true
		}
	}
	return len(as) < len(bs), true
}
