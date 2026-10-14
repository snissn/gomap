package pager

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// ClearPrimaryReadRootsWorkV6 removes actual producer table aliases one node at
// a time. Its position lives in the original table, not a disposal registry.
func (p *Pager) ClearPrimaryReadRootsWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.clearPosition < len(t.heads) {
		head := t.heads[t.clearPosition]
		if head == nil {
			if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(head))+64); !ok || e != nil {
				return false, e
			}
			t.clearPosition++
			continue
		}
		if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))+64); !ok || e != nil {
			return false, e
		}
		t.heads[t.clearPosition] = head.next
		head.next = nil
		head.owner = nil
		head.image = nil
		t.count--
	}
	if t.heads != nil {
		if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(primaryReadRootTableV6{}))+64); !ok || e != nil {
			return false, e
		}
		t.heads = nil
		t.retention = nil
	}
	return true, nil
}
