package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// DropReadRootConstructionV6 removes the original transaction alias and its
// root edge together. A surviving immutable reader keeps only the bank and
// closure, so releasing the transaction descriptor cannot refund a live alias.
// An already detached bank belongs to a bound transaction; its original root
// edge may still be dropped by that SAME transaction's abort/finish path.
// Refusal leaves both alias and edge unchanged.
func (a *Arena) DropReadRootConstructionV6(ref Ref, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := 2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{}))
	if !reserve(w, 2, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(ref)
	if b == nil || owner == nil || (b.construction != nil && b.construction != owner) {
		return false, ErrStale
	}
	b.refs--
	b.construction = nil
	if b.refs == 0 {
		a.enqueueOn(&a.ordinaryRelease, Local(ref.PageID), b)
	}
	a.counters.RootDrops++
	a.counters.Bytes += bytes
	return true, nil
}
